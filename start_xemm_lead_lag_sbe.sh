#!/bin/bash
# ============================================================
# Start script — XEMM Lead-Lag (BTC-BRL) — SBE variant
# ============================================================
# Same XEMM strategy as start_xemm_lead_lag.sh but uses the
# `binance_sbe` connector as signal_connector (Binance Spot SBE
# market data) instead of `binance` JSON. Taker stays on the
# REST/JSON `binance` connector — SBE doesn't cover trading.
#
# Fully isolated from the legacy script:
#   - separate script config:  conf_xemm_lead_lag_sbe.yml
#   - separate controller cfg: xemm_lead_lag_btc_brl_sbe.yml
#   - separate log file:       logs_conf_xemm_lead_lag_sbe.log
#   - separate kill switch:    /tmp/xemm_lead_lag_sbe_pause
#   - separate pgrep pattern:  conf_xemm_lead_lag_sbe
#
# The two scripts share the same .env and the same encrypted
# connector configs (conf/connectors/binance.yml, bitpreco.yml,
# binance_sbe.yml) — no duplication of credentials.
#
# Usage:
#   ./start_xemm_lead_lag_sbe.sh <password>
#   ./start_xemm_lead_lag_sbe.sh Senha123
#
# Monitor:
#   tail -f logs/logs_conf_xemm_lead_lag_sbe.log | grep -E "Created maker|Cancel|ERROR|CRITICAL|SBE"
#
# Kill switch (graceful pause — distinct from the legacy bot's):
#   touch /tmp/xemm_lead_lag_sbe_pause
# ============================================================

set -e
cd "$(dirname "$0")"

if [ -z "$1" ]; then
    echo "Usage: $0 <password>"
    exit 1
fi

# ============================================================
# Environment file loading (shared .env with the legacy script)
# ============================================================
# Same .env that start_xemm_lead_lag.sh reads. Currently expected
# variables:
#   BINANCE_SBE_API_KEY  — Ed25519 API key STRING for the SBE WS
#                          (used as a fallback by the connector if
#                          conf/connectors/binance_sbe.yml is absent).
#   BITPRECO_INTERNAL_*  — optional overrides for the internal BitPreco
#                          hosts (have hardcoded defaults below).
# ============================================================
if [ -f "$(dirname "$0")/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$(dirname "$0")/.env"
    set +a
fi

# ============================================================
# BitPreco fast-path routing (BitPreco-owned operators only)
# ============================================================
# Same defaults as the legacy script. Move into .env if you need
# per-host configuration.
# ============================================================
export BITPRECO_INTERNAL_BOOKS="${BITPRECO_INTERNAL_BOOKS:-http://54.232.138.12}"
export BITPRECO_INTERNAL_API="${BITPRECO_INTERNAL_API:-https://backend.bitpreco.com/exchange/exch_api.php}"

# ============================================================
# Kill any running instance — ONLY this script's bot, never the legacy one
# ============================================================
# The pgrep pattern is the SBE script config name; the legacy script
# uses "conf_xemm_lead_lag_shadow", so the two are mutually exclusive
# matchers — neither can accidentally kill the other.
PGREP_PATTERN="conf_xemm_lead_lag_sbe"
PAUSE_FILE="/tmp/xemm_lead_lag_sbe_pause"
LOG=logs/logs_conf_xemm_lead_lag_sbe.log

if pgrep -f "$PGREP_PATTERN" > /dev/null; then
    echo "Stopping existing SBE bot..."
    touch "$PAUSE_FILE"

    # Wait up to 12s for graceful shutdown.
    for i in $(seq 1 24); do
        sleep 0.5
        if ! pgrep -f "$PGREP_PATTERN" > /dev/null; then
            echo "SBE bot exited cleanly."
            break
        fi
    done

    pkill -TERM -f "$PGREP_PATTERN" 2>/dev/null || true
    sleep 3
    if pgrep -f "$PGREP_PATTERN" > /dev/null; then
        echo "SBE bot did not exit after SIGTERM, sending SIGKILL..."
        pkill -KILL -f "$PGREP_PATTERN" 2>/dev/null || true
        sleep 2
    fi
fi
rm -f "$PAUSE_FILE"

# Rotate previous log so each run starts with a clean file
if [ -f "$LOG" ]; then
    mv "$LOG" "${LOG%.log}_$(date -u +%Y%m%dT%H%M%S).log"
fi

# ============================================================
# Pre-cleanup: cancel any orphan orders BEFORE the bot starts.
# Reads the SBE controller config so it cancels on the same exchanges
# this bot will trade on.
# ============================================================
echo "Running pre-cleanup..."
PRECLEAN_LOG="logs/logs_precleanup_sbe_$(date -u +%Y%m%dT%H%M%S).log"
conda run -n hummingbot python tools/precleanup.py \
    --controller-config conf/controllers/xemm_lead_lag_btc_brl_sbe.yml \
    --password "$1" 2>&1 | tee "$PRECLEAN_LOG"
PRECLEAN_RC=${PIPESTATUS[0]}

if [ "$PRECLEAN_RC" = "1" ]; then
    echo "ERROR: Pre-cleanup failed with auth/config error — refusing to start bot"
    echo "  See $PRECLEAN_LOG"
    exit 1
elif [ "$PRECLEAN_RC" = "2" ]; then
    echo "WARNING: Pre-cleanup had partial failures — continuing (bot will retry)"
fi

echo "Starting XEMM Lead-Lag SBE bot..."

conda run -n hummingbot python bin/hummingbot_quickstart.py --headless \
  --v2 conf_xemm_lead_lag_sbe.yml \
  --config-password "$1" \
  2>&1 | tee -a "$LOG" &

BOT_PID=$!

# Wait up to 60s for the first maker order (proof the bot is live and healthy).
echo "Waiting for first order (up to 60s)..."
WAITED=0
while [ $WAITED -lt 60 ]; do
    sleep 1
    WAITED=$((WAITED + 1))
    tail -n 20 "$LOG" 2>/dev/null | grep "startup_cleanup" | tail -5
    if tail -n 20 "$LOG" 2>/dev/null | grep -q "Created maker order"; then
        LAST=$(tail -n 40 "$LOG" | grep "Created maker order" | tail -1)
        echo ""
        echo "✓ SBE bot live (${WAITED}s). PID=$(pgrep -f "$PGREP_PATTERN" | head -1)"
        echo "  $LAST"
        exit 0
    fi
done

echo "WARNING: no maker order seen after 60s — check $LOG"
pgrep -f "$PGREP_PATTERN" && echo "Process is running" || echo "Process is NOT running"
