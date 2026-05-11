#!/bin/bash
# ============================================================
# Start script — XEMM Lead-Lag (BTC-BRL)
# ============================================================
# Usage:
#   ./start_xemm_lead_lag.sh <password>
#   ./start_xemm_lead_lag.sh Senha123
#
# NOTE: Log is auto-named by hummingbot based on the config file:
#   logs/logs_conf_xemm_lead_lag_shadow.log
#
# Monitor:
#   tail -f logs/logs_conf_xemm_lead_lag_shadow.log | grep -E "Created maker|Cancel|ERROR|CRITICAL"
#
# Kill switch (graceful pause):
#   touch /tmp/xemm_lead_lag_pause
#
# Matching command from working runs (kept as reference):
#   conda run -n hummingbot python bin/hummingbot_quickstart.py --headless \
#     --v2 conf_xemm_lead_lag_shadow.yml \
#     --config-password <pass> \
#     2>&1 | tee -a logs/logs_conf_xemm_lead_lag_shadow.log &
# ============================================================

set -e
cd "$(dirname "$0")"

if [ -z "$1" ]; then
    echo "Usage: $0 <password>"
    exit 1
fi

# Kill any running instance gracefully
if pgrep -f "conf_xemm_lead_lag_shadow" > /dev/null; then
    echo "Stopping existing bot..."
    # Step 1: trigger the kill switch so the controller cancels all maker orders
    touch /tmp/xemm_lead_lag_pause

    # Step 2: wait up to 12s for the bot to process the kill switch and cancel orders.
    # With 200ms polling, Phase 1 fires within 200ms; exchange round-trip ~300-500ms;
    # we wait 12s to be safe under slow network conditions.
    for i in $(seq 1 24); do
        sleep 0.5
        # Bot is done when it stops logging "Created maker" lines
        if ! pgrep -f "conf_xemm_lead_lag_shadow" > /dev/null; then
            echo "Bot exited cleanly."
            break
        fi
    done

    # Step 3: send SIGTERM (graceful) then SIGKILL if still alive
    pkill -TERM -f "conf_xemm_lead_lag_shadow" 2>/dev/null || true
    sleep 3
    if pgrep -f "conf_xemm_lead_lag_shadow" > /dev/null; then
        echo "Bot did not exit after SIGTERM, sending SIGKILL..."
        pkill -KILL -f "conf_xemm_lead_lag_shadow" 2>/dev/null || true
        sleep 2
    fi
fi
rm -f /tmp/xemm_lead_lag_pause

# Rotate previous log so each run starts with a clean file
LOG=logs/logs_conf_xemm_lead_lag_shadow.log
if [ -f "$LOG" ]; then
    mv "$LOG" "${LOG%.log}_$(date -u +%Y%m%dT%H%M%S).log"
fi

# ============================================================
# Pre-cleanup: cancel any orphan orders BEFORE the bot starts.
#
# Closes the 5-10s window between process boot and connector.ready=True
# during which orphan orders from a previous crashed session could fill
# without a corresponding hedge (producing a directional position).
#
# Exit codes from precleanup.py:
#   0 = clean (or all cancels OK)
#   1 = auth/config error → ABORT bot startup (would be unsafe)
#   2 = partial failure → continue (in-bot startup_cleanup retries at boot)
# ============================================================
echo "Running pre-cleanup..."
PRECLEAN_LOG="logs/logs_precleanup_$(date -u +%Y%m%dT%H%M%S).log"
conda run -n hummingbot python tools/precleanup.py \
    --controller-config conf/controllers/xemm_lead_lag_btc_brl.yml \
    --password "$1" 2>&1 | tee "$PRECLEAN_LOG"
PRECLEAN_RC=${PIPESTATUS[0]}

if [ "$PRECLEAN_RC" = "1" ]; then
    echo "ERROR: Pre-cleanup failed with auth/config error — refusing to start bot"
    echo "  See $PRECLEAN_LOG"
    exit 1
elif [ "$PRECLEAN_RC" = "2" ]; then
    echo "WARNING: Pre-cleanup had partial failures — continuing (bot will retry)"
fi

echo "Starting XEMM Lead-Lag bot..."

conda run -n hummingbot python bin/hummingbot_quickstart.py --headless \
  --v2 conf_xemm_lead_lag_shadow.yml \
  --config-password "$1" \
  2>&1 | tee -a "$LOG" &

BOT_PID=$!

# Wait up to 60s for the first maker order (proof the bot is live and healthy).
# Prints startup_cleanup lines as they appear, then exits on first "Created maker".
echo "Waiting for first order (up to 60s)..."
WAITED=0
while [ $WAITED -lt 60 ]; do
    sleep 1
    WAITED=$((WAITED + 1))
    # Show any startup_cleanup lines from this second
    tail -n 20 "$LOG" 2>/dev/null | grep "startup_cleanup" | tail -5
    # Exit as soon as we see a Created maker line
    if tail -n 20 "$LOG" 2>/dev/null | grep -q "Created maker order"; then
        LAST=$(tail -n 40 "$LOG" | grep "Created maker order" | tail -1)
        echo ""
        echo "✓ Bot live (${WAITED}s). PID=$(pgrep -f conf_xemm_lead_lag_shadow | head -1)"
        echo "  $LAST"
        exit 0
    fi
done

echo "WARNING: no maker order seen after 60s — check $LOG"
pgrep -f "conf_xemm_lead_lag_shadow" && echo "Process is running" || echo "Process is NOT running"
