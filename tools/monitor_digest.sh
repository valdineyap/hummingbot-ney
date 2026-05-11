#!/usr/bin/env bash
# monitor_digest.sh — single-shot health digest for XEMM Lead-Lag bot.
#
# Designed to be called from a Claude monitoring loop every N minutes
# instead of running 5–6 separate commands. Outputs a compact text
# digest covering: process liveness, fills (both via ledger AND grep of
# OrderFilledEvent in master log), session PnL, recent ERRORs, and
# cancel-retry / orphan / partial-fill anomalies.
#
# Usage: bash tools/monitor_digest.sh
#
# Exit codes:
#   0  — healthy (or only minor anomalies)
#   1  — fill detected (caller should analyse + decide)
#   2  — bot not alive
#   3  — master log stale (no writes in > LOG_STALE_SEC)
#   4  — ledger desynchronised (fills in log but absent from trades.jsonl)

set -u

REPO="${REPO:-/home/ubuntu/hummingbot-ney}"
MASTER_LOG="${MASTER_LOG:-$REPO/logs/logs_conf_xemm_lead_lag_shadow.log}"
LEDGER_DIR="${LEDGER_DIR:-$REPO/logs/xemm_lead_lag}"
STATE_JSON="$LEDGER_DIR/state.json"
TOUCH_FILE="$LEDGER_DIR/last_fill.touch"
TRADES_JSONL="$LEDGER_DIR/trades.jsonl"

LOG_STALE_SEC="${LOG_STALE_SEC:-120}"
ALERT_RETRY_THRESHOLD="${ALERT_RETRY_THRESHOLD:-3}"
RECENT_LINES="${RECENT_LINES:-400}"
# When set, scan since this ISO date (e.g. "2026-05-07 16:36"). Empty = whole log.
SESSION_START_TS="${SESSION_START_TS:-}"

EXIT_CODE=0

echo "=== XEMM Lead-Lag digest @ $(date -u +%H:%M:%SZ) ==="

# ----------------------------------------------------------------------
# 1. Bot alive
# ----------------------------------------------------------------------
PIDS=$(pgrep -f hummingbot_quickstart | tr '\n' ' ')
if [[ -z "$PIDS" ]]; then
  echo "BOT: DOWN (no hummingbot_quickstart process)"
  EXIT_CODE=2
else
  echo "BOT: UP (pids=$PIDS)"
fi

# ----------------------------------------------------------------------
# 2. Fills via ledger (the canonical, post-Task-1.2 source)
# ----------------------------------------------------------------------
LEDGER_FILL_COUNT=0
if [[ -f "$TRADES_JSONL" ]]; then
  LEDGER_FILL_COUNT=$(wc -l < "$TRADES_JSONL")
fi
if [[ -f "$STATE_JSON" ]]; then
  echo "LEDGER: state.json present"
  echo "--- state.json (last_trade) ---"
  cat "$STATE_JSON"
  echo
  echo "--- last 3 trades.jsonl ---"
  tail -n 3 "$TRADES_JSONL"
  EXIT_CODE=1
else
  echo "LEDGER: 0 trades recorded (no state.json)"
fi

# ----------------------------------------------------------------------
# 3. Fills via master-log grep — ground truth for catching ledger gaps.
#    OrderFilledEvent is emitted by Hummingbot's event bus for every fill,
#    independent of the ledger writer. If counts diverge, the ledger is
#    losing fills (the exact bug Task 1.2 fixed but worth verifying live).
# ----------------------------------------------------------------------
if [[ -f "$MASTER_LOG" ]]; then
  if [[ -n "$SESSION_START_TS" ]]; then
    LOG_FILL_COUNT=$(awk -v ts="$SESSION_START_TS" '$0 ~ ts,EOF' "$MASTER_LOG" \
                     | grep -c '"event_name": "OrderFilledEvent"' || true)
    SCOPE="since $SESSION_START_TS"
  else
    LOG_FILL_COUNT=$(grep -c '"event_name": "OrderFilledEvent"' "$MASTER_LOG" || true)
    SCOPE="whole log"
  fi
  echo "LOG_FILLS ($SCOPE): $LOG_FILL_COUNT (vs ledger=$LEDGER_FILL_COUNT)"
  # Ledger has both 'kind:fill' (raw) and 'kind:trade' (executor close); raw
  # entries should match LOG_FILL_COUNT 1:1 once Task 1.2 is deployed.
  if (( LOG_FILL_COUNT > 0 )) && (( LEDGER_FILL_COUNT == 0 )); then
    echo "LEDGER_DESYNC: log shows $LOG_FILL_COUNT fills but trades.jsonl is empty/missing"
    [[ $EXIT_CODE -eq 0 ]] && EXIT_CODE=4
  fi
fi

# ----------------------------------------------------------------------
# 4. Approximate realised PnL from master log fills (best-effort).
#    Parsing strategy: every OrderFilledEvent line has a JSON payload after
#    "EVENT_LOG - ". We extract that prefix, jq the fields properly (the
#    nested ``trade_fee.flat_fees[].amount`` confuses naive regex), and
#    aggregate side/price/amount. Reported as cashflow (BRL in − BRL out)
#    plus base-asset deltas, which approximates realised PnL when the
#    session ends delta-flat (inventory_audit enforces that on shutdown).
# ----------------------------------------------------------------------
if [[ -f "$MASTER_LOG" ]] && command -v jq >/dev/null; then
  if [[ -n "$SESSION_START_TS" ]]; then
    PNL_SCAN=$(awk -v ts="$SESSION_START_TS" '$0 ~ ts,EOF' "$MASTER_LOG" \
               | grep '"event_name": "OrderFilledEvent"')
  else
    PNL_SCAN=$(grep '"event_name": "OrderFilledEvent"' "$MASTER_LOG")
  fi
  if [[ -n "$PNL_SCAN" ]]; then
    # Strip leading log prefix (everything up to & including "EVENT_LOG - ")
    # so what remains on each line is parseable JSON.
    PNL_SUMMARY=$(echo "$PNL_SCAN" \
      | sed -E 's/^.*EVENT_LOG - //' \
      | jq -rs '
          [ .[]
            | { src:    .event_source,
                side:   (.trade_type | sub("TradeType\\."; "")),
                ot:     (.order_type | sub("OrderType\\."; "")),
                price:  (.price | tonumber),
                amount: (.amount | tonumber) } ]
          | (length) as $n
          | (map(select(.side=="SELL") | .amount) | add // 0) as $sells
          | (map(select(.side=="BUY")  | .amount) | add // 0) as $buys
          | (map(if .side=="SELL" then .price*.amount else -.price*.amount end) | add // 0) as $cash
          | "fills=\($n) sells_btc=\($sells | tostring) buys_btc=\($buys | tostring) net_cash_brl=\($cash | tostring)"
        ')
    echo "PNL_RAW: $PNL_SUMMARY"
  fi
fi

# ----------------------------------------------------------------------
# 5. Master log freshness
# ----------------------------------------------------------------------
if [[ -f "$MASTER_LOG" ]]; then
  MTIME=$(stat -c %Y "$MASTER_LOG")
  NOW=$(date +%s)
  AGE=$((NOW - MTIME))
  LINES=$(wc -l < "$MASTER_LOG")
  echo "LOG: lines=$LINES last_write=${AGE}s ago"
  if (( AGE > LOG_STALE_SEC )); then
    echo "LOG: STALE (>${LOG_STALE_SEC}s without writes)"
    [[ $EXIT_CODE -eq 0 ]] && EXIT_CODE=3
  fi
else
  echo "LOG: MISSING ($MASTER_LOG)"
  EXIT_CODE=3
fi

RECENT_TAIL=$(tail -n "$RECENT_LINES" "$MASTER_LOG" 2>/dev/null || true)

# ----------------------------------------------------------------------
# 6. Recent ERRORs
# ----------------------------------------------------------------------
ERRORS=$(echo "$RECENT_TAIL" | grep -E " - ERROR - " | tail -n 5 || true)
if [[ -n "$ERRORS" ]]; then
  echo "--- recent ERRORs (last 5 in tail $RECENT_LINES) ---"
  echo "$ERRORS"
fi

# ----------------------------------------------------------------------
# 7. Anomaly counters (cancel_retry, orphans, partial fills, late fills)
# ----------------------------------------------------------------------
ANOMALY=$(echo "$RECENT_TAIL" \
  | grep -oE 'cancel_retry\] Re-issuing cancel for maker_order [A-Za-z0-9]+' \
  | awk '{print $NF}' \
  | sort | uniq -c | sort -rn \
  | awk -v t="$ALERT_RETRY_THRESHOLD" '$1 >= t {print}')
if [[ -n "$ANOMALY" ]]; then
  echo "--- cancel_retry anomaly (>= $ALERT_RETRY_THRESHOLD retries / order in last $RECENT_LINES lines) ---"
  echo "$ANOMALY"
fi

ORPHANS_RECENT=$(echo "$RECENT_TAIL" | grep -c "cancelled orphan exchange_order_id" || true)
PARTIAL_RECENT=$(echo "$RECENT_TAIL" | grep -c "'status': 'PARTIAL'" || true)
LATE_RECENT=$(echo "$RECENT_TAIL"   | grep -c "order fill updates did not arrive" || true)
WS_RECENT=$(echo "$RECENT_TAIL"     | grep -c "websocket connection was closed" || true)
echo "ANOMALIES (last $RECENT_LINES lines): orphans=$ORPHANS_RECENT partial=$PARTIAL_RECENT late_fill=$LATE_RECENT ws_disc=$WS_RECENT"

# ----------------------------------------------------------------------
# 8. Last orphan_check status (single line, gives tracker/exchange counts)
# ----------------------------------------------------------------------
LAST_ORPHAN=$(echo "$RECENT_TAIL" | grep '\[orphan_check\]' | tail -n 1 || true)
if [[ -n "$LAST_ORPHAN" ]]; then
  echo "--- last orphan_check ---"
  echo "$LAST_ORPHAN" | sed 's/.*\[orphan_check\]/[orphan_check]/'
fi

# ----------------------------------------------------------------------
# 9. Last 2 maker order events for liveness sanity
# ----------------------------------------------------------------------
LAST_EVENTS=$(echo "$RECENT_TAIL" \
  | grep -E "Created maker order|profitability .* (>|<) " \
  | tail -n 2 || true)
if [[ -n "$LAST_EVENTS" ]]; then
  echo "--- last 2 executor events ---"
  echo "$LAST_EVENTS" | sed -E 's/.*xemm_lead_lag_executor - INFO - //'
fi

echo "=== exit=$EXIT_CODE ==="
exit $EXIT_CODE
