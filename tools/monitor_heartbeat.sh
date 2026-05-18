#!/bin/bash
# ============================================================
# XEMM Lead-Lag — periodic heartbeat (called by system cron */5).
#
# Writes ONE line to logs/monitor_heartbeat.log with:
#   - UTC timestamp
#   - status (OK / DOWN / FILL_NEW / ANOMALY)
#   - fill count, anomalies count, log size, bot pid
#
# Format is grep-friendly and ≤ 100 chars so it fits in chat events.
# ============================================================
set -u
cd "$(dirname "$0")/.." || exit 1

LEDGER=logs/xemm_lead_lag/trades.jsonl
# LOG is auto-detected below from the running bot's cmdline (handles SBE/shadow/etc. swaps)
STATE=logs/xemm_lead_lag/state.json
LAST_FILL_FILE=logs/xemm_lead_lag/.heartbeat_last_fill_seq
OUT=logs/monitor_heartbeat.log

TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# --- Bot alive? ---
BOT_PID=$(pgrep -f "hummingbot_quickstart.*conf_xemm_lead_lag" | head -1)
if [ -z "$BOT_PID" ]; then
  echo "$TS DOWN bot_pid=none fills=? anomalies=? log_lines=?" >> "$OUT"
  exit 0
fi

# --- Auto-detect active log from running process cmdline ---
# Robust to SBE/shadow/etc. swaps: reads the actual --v2 config the bot was started with.
ACTIVE_CONF=$(tr '\0' ' ' < /proc/$BOT_PID/cmdline 2>/dev/null | grep -oE 'conf_xemm_lead_lag[^[:space:]]*\.yml' | head -1)
if [ -n "$ACTIVE_CONF" ]; then
  LOG="logs/logs_${ACTIVE_CONF%.yml}.log"
else
  LOG=logs/logs_conf_xemm_lead_lag_shadow.log  # fallback (cmdline parse failed for some reason)
fi

# --- Fill count and seq from ledger / state.json ---
if [ -f "$STATE" ]; then
  CUR_SEQ=$(grep -oE '"seq":[[:space:]]*[0-9]+' "$STATE" | head -1 | grep -oE '[0-9]+')
  FILL_COUNT=$(grep -oE '"trades_total_session":[[:space:]]*[0-9]+' "$STATE" | grep -oE '[0-9]+')
else
  CUR_SEQ=0
  FILL_COUNT=0
fi
CUR_SEQ=${CUR_SEQ:-0}
FILL_COUNT=${FILL_COUNT:-0}

PREV_SEQ=$(cat "$LAST_FILL_FILE" 2>/dev/null || echo 0)
PREV_SEQ=${PREV_SEQ:-0}

# --- Anomalies in last 400 log lines ---
if [ -f "$LOG" ]; then
  LOG_LINES=$(wc -l < "$LOG")
  TAIL=$(tail -n 400 "$LOG" 2>/dev/null)
  ANOMALY_COUNT=$(echo "$TAIL" | grep -cE "CRITICAL|ERROR|Traceback|orphans=[1-9]|REBALANCE_STUCK|DRIFT_STUCK|EVENT_LOOP_LAG|KILL_SWITCH|\[ghost_fill\]" || true)
else
  LOG_LINES=0
  ANOMALY_COUNT=0
fi

# --- Memory guard L1/L2 (2026-05-16 12:22Z OOM incident follow-up) ---
# L1 (financial risk):   RSS > 1800 MB OR growth > 100 MB/min OR swap > 500 MB
#                        → touch pause kill switch (stops trading)
# L2 (server protection): RSS > 2400 MB OR (still rising after L1)
#                        → kill -TERM the bot for clean shutdown
# Why two levels: pausing trading does NOT stop market-data / WS / queue /
# decoder, so if the leak is in the data pipeline, RSS keeps growing and
# we still risk OOM-reboot. L2 is the safety net for that case.
RSS_FILE=logs/xemm_lead_lag/.heartbeat_last_rss
PAUSE_FILE=/tmp/xemm_lead_lag_pause
MEMGUARD_SNAPSHOTS=logs/xemm_lead_lag/memguard_snapshots.log

RSS_KB=$(awk '/^VmRSS:/ {print $2}' /proc/$BOT_PID/status 2>/dev/null || echo 0)
SWAP_KB=$(awk '/^VmSwap:/ {print $2}' /proc/$BOT_PID/status 2>/dev/null || echo 0)
RSS_MB=$((RSS_KB / 1024))
SWAP_MB=$((SWAP_KB / 1024))

PREV_RSS_KB=$(cat "$RSS_FILE" 2>/dev/null || echo "$RSS_KB")
PREV_RSS_MB=$((PREV_RSS_KB / 1024))
# Heartbeat cadence is system cron */5 → divide MB delta by 5 for MB/min.
GROWTH_MB_PER_MIN=$(( (RSS_MB - PREV_RSS_MB) / 5 ))
echo "$RSS_KB" > "$RSS_FILE"

MEMGUARD=""
# L1 — only fire once until pause is cleared (prevents log spam)
if [ ! -f "$PAUSE_FILE" ]; then
  if [ "$RSS_MB" -gt 1800 ] || [ "$GROWTH_MB_PER_MIN" -gt 100 ] || [ "$SWAP_MB" -gt 500 ]; then
    touch "$PAUSE_FILE"
    MEMGUARD="L1"
    {
      echo "=== L1 trigger $TS pid=$BOT_PID rss=${RSS_MB}MB growth=${GROWTH_MB_PER_MIN}MB/min swap=${SWAP_MB}MB ==="
      cat /proc/$BOT_PID/status 2>/dev/null | egrep 'VmRSS|VmHWM|VmSize|VmSwap|Threads|FDSize'
      echo "--- smaps_rollup ---"
      cat /proc/$BOT_PID/smaps_rollup 2>/dev/null | egrep 'Rss|Pss|Private|Shared|Swap'
      echo "--- fd_count ---"
      ls /proc/$BOT_PID/fd 2>/dev/null | wc -l
      echo "--- last 200 log lines ---"
      tail -n 200 "$LOG" 2>/dev/null
      echo "=== end L1 ==="
    } >> "$MEMGUARD_SNAPSHOTS"
  fi
fi
# L2 — independent of L1, protects server even if L1 already fired
if [ "$RSS_MB" -gt 2400 ] || { [ -f "$PAUSE_FILE" ] && [ "$GROWTH_MB_PER_MIN" -gt 50 ]; }; then
  kill -TERM "$BOT_PID" 2>/dev/null && MEMGUARD="L2"
  {
    echo "=== L2 trigger $TS pid=$BOT_PID rss=${RSS_MB}MB growth=${GROWTH_MB_PER_MIN}MB/min swap=${SWAP_MB}MB ==="
    echo "Sent SIGTERM to PID $BOT_PID for clean shutdown (memory leak protection)."
    echo "=== end L2 ==="
  } >> "$MEMGUARD_SNAPSHOTS"
fi

# --- PnL guard (defense in depth — bot has its own gates, this catches
# the case where the bot is unable to apply them: event-loop lag, frozen
# tick, etc.). Reads safety counters from state.json (snapshotted every
# 60s by the controller) and compares against ENV-configurable thresholds.
# Defaults are slightly LOOSER than the bot's own limits so the bot
# normally trips first — monitor is the safety net.
#
# Action: touch $PAUSE_FILE (graceful — bot sees KILL_SWITCH on next tick).
# Escalates to kill -TERM if pause already set AND bot still alive after
# $PNL_KILL_GRACE_SEC (default 300s). This is the PnL analog of L2 in
# the memory guard above.
#
# Thresholds (env, all in QUOTE / BRL):
#   PNL_MAX_DAILY_LOSS       default 120 (bot config: 100)
#   PNL_MAX_SESSION_DRAWDOWN default 60  (bot config: 50)
#   PNL_MAX_HOURLY_BURN      default 60  (bot config: 50)
#   PNL_MAX_MINUTE_BURN      default 20  (bot config: 15)
#   PNL_KILL_GRACE_SEC       default 300 — escalate to SIGTERM after this
PNL_MAX_DAILY_LOSS=${PNL_MAX_DAILY_LOSS:-120}
PNL_MAX_SESSION_DRAWDOWN=${PNL_MAX_SESSION_DRAWDOWN:-60}
PNL_MAX_HOURLY_BURN=${PNL_MAX_HOURLY_BURN:-60}
PNL_MAX_MINUTE_BURN=${PNL_MAX_MINUTE_BURN:-20}
PNL_KILL_GRACE_SEC=${PNL_KILL_GRACE_SEC:-300}
PNL_SNAPSHOTS=logs/xemm_lead_lag/pnl_guard_snapshots.log

PNLGUARD=""
PNL_BREACH=""
if [ -f "$STATE" ]; then
  # Pull safety fields via python (json parsing in bash is awful; python is
  # already used by the bot, so it's always available).
  PNL_READOUT=$(python3 - "$STATE" 2>/dev/null <<'PYEOF'
import json, sys
try:
    with open(sys.argv[1]) as f:
        d = json.load(f)
    s = d.get("safety") or {}
    fields = ["daily_realized_pnl", "session_drawdown",
              "hourly_burn", "minute_burn", "regime", "kill_reason"]
    # Print as space-separated key=val for cheap bash parsing.
    for k in fields:
        v = s.get(k, "")
        # Decimals come through as strings; strip and substitute "" for None.
        print(f"{k}={v}" if v not in (None, "") else f"{k}=NA")
except Exception as e:
    print(f"ERR={type(e).__name__}")
PYEOF
)
  # Parse readout. eval is safe here because the source is our own python.
  if echo "$PNL_READOUT" | grep -q '^ERR='; then
    PNL_READOUT=""
  else
    DAILY_PNL=$(echo "$PNL_READOUT" | grep -oE 'daily_realized_pnl=[^ ]+' | cut -d= -f2)
    DRAWDOWN=$(echo "$PNL_READOUT" | grep -oE 'session_drawdown=[^ ]+' | cut -d= -f2)
    HOURLY_BURN=$(echo "$PNL_READOUT" | grep -oE 'hourly_burn=[^ ]+' | cut -d= -f2)
    MINUTE_BURN=$(echo "$PNL_READOUT" | grep -oE 'minute_burn=[^ ]+' | cut -d= -f2)
    REGIME=$(echo "$PNL_READOUT" | grep -oE 'regime=[^ ]+' | cut -d= -f2)

    # awk for Decimal arithmetic (bash only handles ints).
    # Each gate sets PNL_BREACH on the first match.
    breach() {
      local label="$1" value="$2" limit="$3" cmp="$4"
      awk -v v="$value" -v l="$limit" 'BEGIN{exit !(v+0 '"$cmp"' l+0)}' \
        2>/dev/null && PNL_BREACH="${PNL_BREACH:+$PNL_BREACH }${label}=${value}/${limit}"
    }
    [ "$DAILY_PNL" != "NA" ]  && breach DAILY_LOSS       "$DAILY_PNL"   "-$PNL_MAX_DAILY_LOSS"       "<="
    [ "$DRAWDOWN" != "NA" ]   && breach SESSION_DRAWDOWN "$DRAWDOWN"    "$PNL_MAX_SESSION_DRAWDOWN"  ">="
    [ "$HOURLY_BURN" != "NA" ] && breach HOURLY_BURN     "$HOURLY_BURN" "-$PNL_MAX_HOURLY_BURN"      "<="
    [ "$MINUTE_BURN" != "NA" ] && breach MINUTE_BURN     "$MINUTE_BURN" "-$PNL_MAX_MINUTE_BURN"      "<="
  fi
fi

# L1: graceful pause (only fire once until pause cleared)
if [ -n "$PNL_BREACH" ] && [ ! -f "$PAUSE_FILE" ]; then
  touch "$PAUSE_FILE"
  PNLGUARD="L1"
  {
    echo "=== PnL L1 $TS pid=$BOT_PID breach=$PNL_BREACH ==="
    echo "regime=$REGIME pnl_today=$DAILY_PNL drawdown=$DRAWDOWN hourly_burn=$HOURLY_BURN minute_burn=$MINUTE_BURN"
    echo "limits: daily=$PNL_MAX_DAILY_LOSS drawdown=$PNL_MAX_SESSION_DRAWDOWN hourly=$PNL_MAX_HOURLY_BURN minute=$PNL_MAX_MINUTE_BURN"
    echo "Touched $PAUSE_FILE — bot will see KILL_SWITCH on next tick."
    echo "=== end PnL L1 ==="
  } >> "$PNL_SNAPSHOTS"
fi

# L2: if pause is set AND breach persists AND bot still alive past grace,
# escalate to SIGTERM. Uses pause file mtime for elapsed time (set in L1
# above OR by memory guard).
if [ -n "$PNL_BREACH" ] && [ -f "$PAUSE_FILE" ]; then
  PAUSE_MTIME=$(stat -c %Y "$PAUSE_FILE" 2>/dev/null || echo 0)
  NOW_EPOCH=$(date +%s)
  PAUSE_AGE=$((NOW_EPOCH - PAUSE_MTIME))
  if [ "$PAUSE_AGE" -ge "$PNL_KILL_GRACE_SEC" ]; then
    kill -TERM "$BOT_PID" 2>/dev/null && PNLGUARD="${PNLGUARD:+$PNLGUARD,}L2"
    {
      echo "=== PnL L2 $TS pid=$BOT_PID pause_age=${PAUSE_AGE}s breach=$PNL_BREACH ==="
      echo "Sent SIGTERM — bot still alive ${PAUSE_AGE}s after pause."
      echo "=== end PnL L2 ==="
    } >> "$PNL_SNAPSHOTS"
  fi
fi

# --- Decide status ---
if [ "$CUR_SEQ" != "$PREV_SEQ" ] && [ "$CUR_SEQ" -gt "$PREV_SEQ" ]; then
  STATUS="FILL_NEW seq=$CUR_SEQ"
  echo "$CUR_SEQ" > "$LAST_FILL_FILE"
elif [ "$ANOMALY_COUNT" -gt 0 ]; then
  STATUS="ANOMALY n=$ANOMALY_COUNT"
else
  STATUS="OK"
fi
[ -n "$MEMGUARD" ] && STATUS="$STATUS MEMGUARD=$MEMGUARD"
[ -n "$PNLGUARD" ] && STATUS="$STATUS PNLGUARD=$PNLGUARD breach=$PNL_BREACH"

echo "$TS $STATUS bot_pid=$BOT_PID fills=$FILL_COUNT anomalies=$ANOMALY_COUNT log_lines=$LOG_LINES rss=${RSS_MB}MB swap=${SWAP_MB}MB growth=${GROWTH_MB_PER_MIN}MB/min" >> "$OUT"
