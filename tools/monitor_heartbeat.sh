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
LOG=logs/logs_conf_xemm_lead_lag_shadow.log
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

# --- Decide status ---
if [ "$CUR_SEQ" != "$PREV_SEQ" ] && [ "$CUR_SEQ" -gt "$PREV_SEQ" ]; then
  STATUS="FILL_NEW seq=$CUR_SEQ"
  echo "$CUR_SEQ" > "$LAST_FILL_FILE"
elif [ "$ANOMALY_COUNT" -gt 0 ]; then
  STATUS="ANOMALY n=$ANOMALY_COUNT"
else
  STATUS="OK"
fi

echo "$TS $STATUS bot_pid=$BOT_PID fills=$FILL_COUNT anomalies=$ANOMALY_COUNT log_lines=$LOG_LINES" >> "$OUT"
