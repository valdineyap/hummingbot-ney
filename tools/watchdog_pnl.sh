#!/bin/bash
# ============================================================
# XEMM Lead-Lag — external PnL watchdog (defense in depth).
#
# Designed to be run from system cron every minute, INDEPENDENTLY of the
# bot's own asyncio loop. If the bot's in-process gates fail (bug, hang,
# unexpected race), this watchdog still pauses live trading by touching the
# kill switch file.
#
# Thresholds are intentionally LOOSER than the in-process gates so the
# watchdog only fires when in-process gates have failed. If the watchdog
# trips before the in-process gates, that is a BUG (in the in-process gates)
# to investigate — not normal behaviour.
#
# Reads:
#   logs/xemm_lead_lag/state.json   (pnl_today, pnl_session, updated_at)
#
# Writes:
#   logs/watchdog.log               (1 line per invocation)
#   /tmp/xemm_lead_lag_pause        (touched only when a limit is breached)
#
# Cron line (every minute):
#   * * * * * cd /home/ubuntu/hummingbot-ney && bash tools/watchdog_pnl.sh
# ============================================================
set -u
cd "$(dirname "$0")/.." || exit 1

STATE=logs/xemm_lead_lag/state.json
OUT=logs/watchdog.log
PAUSE_FILE=/tmp/xemm_lead_lag_pause

# === Thresholds (BRL). Looser than in-process gates by design. ===
# In-process gates:
#   max_daily_loss_quote         = 100 BRL
#   max_session_drawdown_quote   = 50 BRL
# Watchdog (this script):
#   WATCHDOG_DAILY_LOSS          = 150 BRL  (50% headroom)
#   WATCHDOG_SESSION_LOSS        = 120 BRL  (catches negative sessions
#                                            independent of peak tracking)
#   WATCHDOG_STATE_STALE_SEC     = 120 s    (state.json not refreshed → bot hung)
WATCHDOG_DAILY_LOSS=${WATCHDOG_DAILY_LOSS:-150}
WATCHDOG_SESSION_LOSS=${WATCHDOG_SESSION_LOSS:-120}

TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

trip() {
    local reason="$1"
    if [ ! -f "$PAUSE_FILE" ]; then
        touch "$PAUSE_FILE"
    fi
    echo "$TS TRIPPED reason=$reason $2" >> "$OUT"
    exit 0
}

# --- Bot alive? ---
BOT_PID=$(pgrep -f "hummingbot_quickstart.*conf_xemm_lead_lag" | head -1)

# --- state.json present and parseable? ---
if [ ! -f "$STATE" ]; then
    # No state file usually means bot has not produced its first fill yet.
    # Don't trip; just log and exit. The heartbeat will flag DOWN if it is
    # actually dead.
    echo "$TS NO_STATE bot_pid=${BOT_PID:-none}" >> "$OUT"
    exit 0
fi

# --- Parse pnl_today, pnl_session, updated_at (no jq dependency — grep only) ---
PNL_TODAY=$(grep -oE '"pnl_today":[[:space:]]*"-?[0-9.]+"' "$STATE" | grep -oE '\-?[0-9.]+$' | head -1)
PNL_SESSION=$(grep -oE '"pnl_session":[[:space:]]*"-?[0-9.]+"' "$STATE" | grep -oE '\-?[0-9.]+$' | head -1)
UPDATED_AT=$(grep -oE '"updated_at":[[:space:]]*"[^"]+"' "$STATE" | grep -oE '"[^"]+"$' | tr -d '"')

PNL_TODAY=${PNL_TODAY:-0}
PNL_SESSION=${PNL_SESSION:-0}

# --- Staleness (logged for visibility, NOT used for tripping) ---
# state.json is written ONLY when the trade ledger sees a fill — markets
# can easily be quiet for hours in BRL pairs off-peak, so this metric is
# useless for liveness. Liveness is the heartbeat script's job (pgrep).
# Kept here as telemetry only.
if [ -n "$UPDATED_AT" ]; then
    STATE_EPOCH=$(date -d "$UPDATED_AT" +%s 2>/dev/null || echo 0)
    NOW_EPOCH=$(date +%s)
    STALE_SEC=$((NOW_EPOCH - STATE_EPOCH))
else
    STALE_SEC=99999
fi

# --- Compare PnL against thresholds (use awk for float comparison) ---
DAILY_BREACH=$(awk -v p="$PNL_TODAY" -v t="$WATCHDOG_DAILY_LOSS" 'BEGIN { print (p+0 <= -t) ? 1 : 0 }')
SESSION_BREACH=$(awk -v p="$PNL_SESSION" -v t="$WATCHDOG_SESSION_LOSS" 'BEGIN { print (p+0 <= -t) ? 1 : 0 }')

if [ "$DAILY_BREACH" = "1" ]; then
    trip "DAILY_LOSS_${PNL_TODAY}" "pnl_today=$PNL_TODAY threshold=-$WATCHDOG_DAILY_LOSS"
fi
if [ "$SESSION_BREACH" = "1" ]; then
    trip "SESSION_LOSS_${PNL_SESSION}" "pnl_session=$PNL_SESSION threshold=-$WATCHDOG_SESSION_LOSS"
fi

# --- Healthy ---
echo "$TS OK bot_pid=${BOT_PID:-none} pnl_today=$PNL_TODAY pnl_session=$PNL_SESSION stale=${STALE_SEC}s" >> "$OUT"
