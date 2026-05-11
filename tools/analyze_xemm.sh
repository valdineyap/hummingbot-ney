#!/usr/bin/env bash
# tools/analyze_xemm.sh - Dashboard de análise do bot XEMM Lead-Lag (BTC-BRL).
#
# Restrições de memória (servidor 1.9GB RAM, 0 swap):
#   - 1 passagem awk por arquivo CSV grande
#   - sempre slicing para /tmp antes de processar
#   - sem grep paralelo em background sobre arquivos grandes
#
# Uso: ./tools/analyze_xemm.sh [--period 30m|1h|4h|session]

set -euo pipefail

PERIOD="${1:-30m}"
if [[ "$PERIOD" == "--period" ]]; then PERIOD="${2:-30m}"; fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

CSV_DIR="logs/xemm_lead_lag"
LOG_FILE="logs/logs_conf_xemm_lead_lag_shadow.log"
CONFIG="conf/controllers/xemm_lead_lag_btc_brl.yml"
TMPDIR_BASE="${TMPDIR:-/tmp}"
WORK="$TMPDIR_BASE/xemm_analyze_$$"
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT

# --- período em segundos --------------------------------------------------
case "$PERIOD" in
    30m)     PERIOD_SEC=1800;  PERIOD_LABEL="últimos 30min" ;;
    1h)      PERIOD_SEC=3600;  PERIOD_LABEL="última 1h" ;;
    4h)      PERIOD_SEC=14400; PERIOD_LABEL="últimas 4h" ;;
    session) PERIOD_SEC=0;     PERIOD_LABEL="sessão completa" ;;
    *) echo "período inválido: $PERIOD (use 30m|1h|4h|session)" >&2; exit 1 ;;
esac

# --- detectar CSV mais recente --------------------------------------------
CSV=$(ls -t "$CSV_DIR"/*.csv 2>/dev/null | head -1 || true)
if [[ -z "$CSV" ]]; then
    echo "ERRO: nenhum CSV encontrado em $CSV_DIR/" >&2
    exit 1
fi
CSV_NAME=$(basename "$CSV")

# --- ler config -----------------------------------------------------------
yaml_get() {
    awk -v key="$1" -F: '
        $1 ~ "^[[:space:]]*"key"[[:space:]]*$" {
            sub(/^[^:]*:[[:space:]]*"?/, "", $0)
            sub(/"?[[:space:]]*(#.*)?$/, "", $0)
            print
            exit
        }
    ' "$CONFIG"
}

MIN_PROF_RAW=$(yaml_get "min_profitability")
MAX_PROF_RAW=$(yaml_get "max_profitability")
TARGET_PROF_RAW=$(yaml_get "target_profitability")
ORDER_AMOUNT_RAW=$(yaml_get "order_amount")
PLACEMENT_BUFFER_RAW=$(yaml_get "placement_profitability_buffer")
SHADOW_MODE_RAW=$(yaml_get "shadow_mode")

# converter decimal → bps
to_bps() { awk -v v="$1" 'BEGIN{ printf "%.2f", v*10000 }'; }
MIN_PROF_BPS=$(to_bps "${MIN_PROF_RAW:-0.0005}")
MAX_PROF_BPS=$(to_bps "${MAX_PROF_RAW:-0.001}")
TARGET_PROF_BPS=$(to_bps "${TARGET_PROF_RAW:-0.0007}")
PLACEMENT_BUFFER_BPS=$(to_bps "${PLACEMENT_BUFFER_RAW:-0.0002}")
ORDER_AMOUNT="${ORDER_AMOUNT_RAW:-0.0002}"

# --- slicing do CSV -------------------------------------------------------
CSV_SLICE="$WORK/csv_slice.csv"
HEADER_FILE="$WORK/csv_header"
head -1 "$CSV" > "$HEADER_FILE"

if [[ "$PERIOD_SEC" -eq 0 ]]; then
    cp "$CSV" "$CSV_SLICE"
else
    N=$(awk -v s="$PERIOD_SEC" 'BEGIN{ printf "%d", s*1.05 + 1 }')
    {
        cat "$HEADER_FILE"
        tail -n "$N" "$CSV"
    } > "$CSV_SLICE"
fi

CSV_LINES=$(wc -l < "$CSV_SLICE")
DATA_LINES=$((CSV_LINES > 0 ? CSV_LINES - 1 : 0))

# --- single-pass awk sobre o CSV ------------------------------------------
STATS_FILE="$WORK/csv_stats.kv"

awk -F',' -v MINP="$MIN_PROF_BPS" -v TGTP="$TARGET_PROF_BPS" -v RES_SIZE=2000 '
function reservoir_add(arr, n_seen, val,    pos) {
    n_seen++
    if (n_seen <= RES_SIZE) {
        arr[n_seen] = val
    } else {
        pos = int(rand() * n_seen) + 1
        if (pos <= RES_SIZE) arr[pos] = val
    }
    return n_seen
}
function pct(arr, n, p,    cnt, i, j, tmp) {
    cnt = (n < RES_SIZE) ? n : RES_SIZE
    if (cnt == 0) return 0
    # cópia local
    for (i=1; i<=cnt; i++) tmp[i] = arr[i]
    # insertion sort (cnt <= 2000)
    for (i=2; i<=cnt; i++) {
        v = tmp[i]
        j = i-1
        while (j>=1 && tmp[j] > v) { tmp[j+1] = tmp[j]; j-- }
        tmp[j+1] = v
    }
    pos = int(p * (cnt-1)) + 1
    if (pos < 1) pos = 1
    if (pos > cnt) pos = cnt
    return tmp[pos]
}

NR==1 {
    for (i=1; i<=NF; i++) col[$i] = i
    next
}
NR > 1 && NF >= col["iso_time"] && $col["timestamp"] != "timestamp" && ($col["timestamp"] + 0) > 0 {
    rows++
    # primeiros / últimos timestamps
    if (rows == 1) {
        first_ts = $col["timestamp"]
        first_iso = $col["iso_time"]
        first_combined_pct = $col["combined_pct"] + 0
        first_basis = $col["basis_bps"] + 0
    }
    last_ts = $col["timestamp"]
    last_iso = $col["iso_time"]
    last_combined_pct = $col["combined_pct"] + 0
    last_basis = $col["basis_bps"] + 0
    last_maker_quote = $col["maker_quote"] + 0
    last_taker_base = $col["taker_base"] + 0
    last_taker_quote = $col["taker_quote"] + 0
    last_local_ask = $col["local_ask"] + 0
    last_taker_local_ask = $col["taker_local_ask"] + 0
    last_fx_mid = $col["fx_mid_ema"] + 0

    # regime
    r = $col["regime"]; regime_count[r]++
    sq = $col["signal_quality"]; sq_count[sq]++

    # n_active_executors
    nex = $col["n_active_executors"] + 0
    sum_nex += nex
    if (nex > max_nex) max_nex = nex

    # spreads
    ls = $col["local_spread_bps"] + 0
    sum_ls += ls; if (ls > max_ls) max_ls = ls
    n_ls = reservoir_add(ls_arr, n_ls, ls)

    mvt = $col["maker_vs_taker_bps"] + 0
    sum_mvt += mvt
    if (rows == 1 || mvt < min_mvt) min_mvt = mvt
    if (mvt > max_mvt) max_mvt = mvt
    n_mvt = reservoir_add(mvt_arr, n_mvt, mvt)
    # buckets
    if (mvt < 0) b_neg++
    else if (mvt < 3) b_0_3++
    else if (mvt < MINP) b_3_min++
    else if (mvt < TGTP) b_min_tgt++
    else b_above_tgt++
    if (mvt >= MINP) ticks_above_min++

    # basis
    bs = $col["basis_bps"] + 0
    sum_bs += bs

    # best_lead_bps
    bl = $col["best_lead_bps"] + 0
    sum_bl += bl
    abl = (bl < 0) ? -bl : bl
    sum_abl += abl
    if (!(bl in lead_unique)) lead_unique[bl] = 1

    # arb
    ant = $col["arb_near_threshold"] + 0
    if (ant > 0) arb_near_count++
    arb_long = $col["arb_long_gross_bps"] + 0
    arb_short = $col["arb_short_gross_bps"] + 0
    if (arb_long > max_arb_long) max_arb_long = arb_long
    if (arb_short > max_arb_short) max_arb_short = arb_short

    # inventory
    cp = $col["combined_pct"] + 0
    if (rows == 1 || cp < min_cp) min_cp = cp
    if (cp > max_cp) max_cp = cp

    isk = $col["inventory_skew"] + 0
    sum_isk += isk

    # target_prof (CSV armazena decimal — multiplicar por 10000 para bps)
    tpb = ($col["target_prof_buy"] + 0) * 10000
    tps = ($col["target_prof_sell"] + 0) * 10000
    sum_tpb += tpb; sum_tps += tps
    if (rows == 1 || tpb < min_tpb) min_tpb = tpb
    if (tpb > max_tpb) max_tpb = tpb
    if (rows == 1 || tps < min_tps) min_tps = tps
    if (tps > max_tps) max_tps = tps

    # audit
    if ($col["audit_drift_active"] == "True" || $col["audit_drift_active"] == "true" || $col["audit_drift_active"] == "1") {
        audit_drift_active_count++
    }

    # gap detection (starvation)
    cur_ts = $col["timestamp"] + 0
    if (rows > 1) {
        gap = cur_ts - prev_ts
        if (gap > 26) {
            gaps_count++
            if (gaps_count <= 30) {
                gap_iso[gaps_count] = $col["iso_time"]
                gap_dur[gaps_count] = gap
            }
        }
    }
    prev_ts = cur_ts
}
END {
    if (rows == 0) {
        print "rows=0"
        exit
    }
    print "rows=" rows
    print "first_ts=" first_ts
    print "last_ts=" last_ts
    print "first_iso=" first_iso
    print "last_iso=" last_iso
    duration = last_ts - first_ts
    printf "duration_sec=%d\n", duration + 0.5

    # regime
    for (r in regime_count) printf "regime_%s=%d\n", r, regime_count[r]
    for (s in sq_count) printf "sq_%s=%d\n", s, sq_count[s]

    printf "avg_nex=%.4f\n", sum_nex/rows
    printf "max_nex=%d\n", max_nex

    printf "avg_local_spread=%.3f\n", sum_ls/rows
    printf "max_local_spread=%.3f\n", max_ls
    printf "p10_local_spread=%.3f\n", pct(ls_arr, n_ls, 0.10)
    printf "p90_local_spread=%.3f\n", pct(ls_arr, n_ls, 0.90)

    printf "avg_mvt=%.3f\n", sum_mvt/rows
    printf "min_mvt=%.3f\n", min_mvt
    printf "max_mvt=%.3f\n", max_mvt
    printf "p50_mvt=%.3f\n", pct(mvt_arr, n_mvt, 0.50)
    printf "p95_mvt=%.3f\n", pct(mvt_arr, n_mvt, 0.95)

    print "b_neg=" b_neg+0
    print "b_0_3=" b_0_3+0
    print "b_3_min=" b_3_min+0
    print "b_min_tgt=" b_min_tgt+0
    print "b_above_tgt=" b_above_tgt+0
    print "ticks_above_min=" ticks_above_min+0

    printf "avg_basis=%.3f\n", sum_bs/rows
    printf "drift_basis=%.3f\n", last_basis - first_basis

    printf "avg_lead=%.3f\n", sum_bl/rows
    printf "abs_avg_lead=%.3f\n", sum_abl/rows
    n_lead = 0; for (k in lead_unique) n_lead++
    print "unique_lead=" n_lead

    print "arb_near_count=" arb_near_count+0
    printf "max_arb_long=%.3f\n", max_arb_long+0
    printf "max_arb_short=%.3f\n", max_arb_short+0

    printf "first_cp=%.6f\n", first_combined_pct
    printf "last_cp=%.6f\n", last_combined_pct
    printf "min_cp=%.6f\n", min_cp
    printf "max_cp=%.6f\n", max_cp
    printf "drift_cp=%.6f\n", last_combined_pct - first_combined_pct

    printf "avg_isk=%.4f\n", sum_isk/rows

    printf "last_maker_quote=%.4f\n", last_maker_quote
    printf "last_taker_base=%.8f\n", last_taker_base
    printf "last_taker_quote=%.4f\n", last_taker_quote
    printf "last_local_ask=%.2f\n", last_local_ask
    printf "last_taker_local_ask=%.2f\n", last_taker_local_ask
    printf "last_fx_mid=%.6f\n", last_fx_mid

    printf "avg_tpb=%.3f\n", sum_tpb/rows
    printf "min_tpb=%.3f\n", min_tpb
    printf "max_tpb=%.3f\n", max_tpb
    printf "avg_tps=%.3f\n", sum_tps/rows
    printf "min_tps=%.3f\n", min_tps
    printf "max_tps=%.3f\n", max_tps

    print "audit_drift_active_count=" audit_drift_active_count+0
    print "gaps_count=" gaps_count+0
    for (i=1; i<=gaps_count && i<=30; i++) {
        printf "gap_%d_iso=%s\n", i, gap_iso[i]
        printf "gap_%d_dur=%.1f\n", i, gap_dur[i]
    }
}
' "$CSV_SLICE" > "$STATS_FILE"

# carregar stats em variáveis bash
declare -A S
while IFS='=' read -r k v; do
    [[ -z "$k" ]] && continue
    S["$k"]="$v"
done < "$STATS_FILE"

if [[ "${S[rows]:-0}" -eq 0 ]]; then
    echo "ERRO: CSV slice vazio (sem dados no período)"
    exit 1
fi

# --- slicing do log por intervalo de tempo --------------------------------
LOG_SLICE="$WORK/log_slice.log"
LOG_AVAILABLE=0
if [[ -f "$LOG_FILE" ]]; then
    LOG_AVAILABLE=1
    # extrair "YYYY-MM-DD HH:MM:SS" do iso_time
    START_DT=$(echo "${S[first_iso]}" | sed -E 's/T/ /; s/\..*//')
    END_DT=$(echo "${S[last_iso]}"  | sed -E 's/T/ /; s/\..*//')
    awk -v start="$START_DT" -v end="$END_DT" '
        {
            # padrão "YYYY-MM-DD HH:MM:SS,mmm - ..."
            ts = substr($0, 1, 19)
            if (ts >= start && ts <= end) print
        }
    ' "$LOG_FILE" > "$LOG_SLICE" || true
fi

# --- estatísticas do log --------------------------------------------------
# grep -c retorna count=0 e exit 1 quando não há match, então usamos
# captura via $() + ||true para evitar "0\n0" e respeitar set -u.
log_count() {
    if [[ ! -f "$LOG_SLICE" ]]; then echo 0; return; fi
    local n
    n=$(grep -cE "$1" "$LOG_SLICE" 2>/dev/null || true)
    echo "${n:-0}"
}
log_pipe_count() {
    # uso: log_pipe_count "primary regex" "filter regex"
    if [[ ! -f "$LOG_SLICE" ]]; then echo 0; return; fi
    local n
    n=$(grep -E "$1" "$LOG_SLICE" 2>/dev/null | grep -ciE "$2" 2>/dev/null || true)
    echo "${n:-0}"
}

CREATED_TOTAL=$(log_count "Created maker order")
CREATED_BUY=$(log_pipe_count  "Created maker order" "BUY")
CREATED_SELL=$(log_pipe_count "Created maker order" "SELL")

MODE_IMPROVE=$(log_count "mode=improve")
MODE_JOIN=$(log_count "mode=join")
MODE_FALLBACK=$(log_count "mode=fallback")

FILLS=$(log_count "has been filled at")
FILLS_BUY=$(log_pipe_count  "has been filled at" "BUY")
FILLS_SELL=$(log_pipe_count "has been filled at" "SELL")

CANCEL_BELOW=$(log_count "below minimum profitability")
CANCEL_ABOVE=$(log_count "above maximum profitability")
CANCEL_KILL=$(log_count "KILL_SWITCH")
CANCEL_TOTAL=$((CANCEL_BELOW + CANCEL_ABOVE + CANCEL_KILL))

# erros (filtrando MQTT/commlib que são ruído conhecido)
ERR_REAL=0
if [[ -f "$LOG_SLICE" ]]; then
    n=$(grep -E "ERROR|CRITICAL" "$LOG_SLICE" 2>/dev/null | grep -vcE "MQTT|commlib|Reconnect" 2>/dev/null || true)
    ERR_REAL="${n:-0}"
fi

# falhas específicas
WS_BOOK_ERR=$(log_count "Unexpected error.*order book streams")
WS_USER_ERR=$(log_count "Unexpected error.*user stream")
RACE_NONE=$(log_count "NoneType.*is_done")
BUDGET_ERR=$(log_count "Not enough budget")
HEDGE_FAIL=$(log_count "hedge.*fail|consecutive_hedge_failures")

# normalizar (qualquer coisa não-numérica vira 0)
for var in CREATED_TOTAL CREATED_BUY CREATED_SELL MODE_IMPROVE MODE_JOIN MODE_FALLBACK \
           FILLS FILLS_BUY FILLS_SELL CANCEL_BELOW CANCEL_ABOVE CANCEL_KILL ERR_REAL \
           WS_BOOK_ERR WS_USER_ERR RACE_NONE BUDGET_ERR HEDGE_FAIL; do
    val="${!var}"
    [[ "$val" =~ ^[0-9]+$ ]] || eval "$var=0"
done

# OTR
if [[ "$FILLS" -gt 0 ]]; then
    OTR=$(awk -v c="$CANCEL_TOTAL" -v f="$FILLS" 'BEGIN{ printf "%.0f", c/f }')
    OTR_DISP="$OTR"
else
    OTR_DISP="-- (no fills)"
    OTR=0
fi

# vida média das ordens + volume criado/fillado + lista de fills (1 passagem awk)
AVG_LIFE="n/a"; MIN_LIFE="n/a"; MAX_LIFE="n/a"
LOG_STATS_FILE="$WORK/log_stats.kv"
FILLS_LIST_FILE="$WORK/fills_list.tsv"
: > "$FILLS_LIST_FILE"

if [[ -f "$LOG_SLICE" ]]; then
    awk -v ORDER_AMOUNT="$ORDER_AMOUNT" -v FILLS_OUT="$FILLS_LIST_FILE" '
        function ts2sec(ts,    a,b,c,h,m,s) {
            split(ts, a, " "); split(a[2], b, ":")
            h=b[1]+0; m=b[2]+0
            split(b[3], c, ","); s=c[1]+0+(c[2]+0)/1000
            return h*3600 + m*60 + s
        }
        /Created maker order/ {
            ts = substr($0, 1, 23)
            ts_str = substr($0, 12, 8)  # HH:MM:SS
            # extrair order_id
            if (match($0, /(LIMIT_MAKER\) |LIMIT\) )[A-Za-z0-9_-]{20,}/)) {
                head = substr($0, RSTART, RLENGTH)
                sub(/^[^)]+\) /, "", head)
                oid = head
                created[oid] = ts2sec(ts)
                created_ts_str[oid] = ts_str
            }
            # extrair price=
            if (match($0, /price=[0-9.]+/)) {
                p = substr($0, RSTART+6, RLENGTH-6) + 0
                vol_created_n++
                vol_created_brl += ORDER_AMOUNT * p
                vol_created_btc += ORDER_AMOUNT
                if (vol_created_n == 1 || p < min_create_price) min_create_price = p
                if (p > max_create_price) max_create_price = p
            }
        }
        /Successfully canceled order/ {
            ts = substr($0, 1, 23)
            if (match($0, /order [A-Za-z0-9_-]{20,}/)) {
                oid = substr($0, RSTART+6, RLENGTH-6)
                if (oid in created) {
                    life = ts2sec(ts) - created[oid]
                    if (life < 0) life += 86400
                    sum_life += life; n_life++
                    if (n_life == 1 || life < min_life) min_life = life
                    if (life > max_life) max_life = life
                }
            }
        }
        /has been filled at/ {
            # "The (BUY|SELL) order ORDER_ID amounting to A/B BTC has been filled at PRICE BRL."
            ts_str = substr($0, 12, 8)
            side = ""
            if (match($0, /The (BUY|SELL) order/)) {
                seg = substr($0, RSTART, RLENGTH)
                if (seg ~ /BUY/) side = "BUY"; else side = "SELL"
            }
            oid = ""
            if (match($0, /order [A-Za-z0-9_-]{20,}/)) {
                oid = substr($0, RSTART+6, RLENGTH-6)
            }
            amt_cum = 0; amt_total = 0
            if (match($0, /amounting to [0-9.]+\/[0-9.]+/)) {
                seg = substr($0, RSTART+13, RLENGTH-13)
                split(seg, parts, "/")
                amt_cum = parts[1] + 0
                amt_total = parts[2] + 0
            }
            price = 0
            if (match($0, /at [0-9.]+ BRL/)) {
                price = substr($0, RSTART+3, RLENGTH-7) + 0
            }
            if (oid != "" && amt_cum > 0 && price > 0) {
                # mantém último cumulativo por order_id
                fill_side[oid] = side
                fill_amt[oid] = amt_cum
                fill_total[oid] = amt_total
                fill_price[oid] = price
                fill_ts[oid] = ts_str
            }
        }
        END {
            print "vol_created_n=" vol_created_n+0
            printf "vol_created_brl=%.2f\n", vol_created_brl+0
            printf "vol_created_btc=%.6f\n", vol_created_btc+0
            printf "min_create_price=%.2f\n", min_create_price+0
            printf "max_create_price=%.2f\n", max_create_price+0
            if (n_life > 0) {
                printf "avg_life=%.1f\n", sum_life/n_life
                printf "min_life=%.1f\n", min_life
                printf "max_life=%.1f\n", max_life
            } else {
                print "avg_life=n/a"
                print "min_life=n/a"
                print "max_life=n/a"
            }
            # consolidar fills
            n_fills = 0; vol_fills_btc = 0; vol_fills_brl = 0
            n_buy = 0; n_sell = 0; vol_buy_btc = 0; vol_sell_btc = 0
            for (oid in fill_side) {
                amt = fill_amt[oid]
                if (amt <= 0) continue
                n_fills++
                vol_fills_btc += amt
                vol_fills_brl += amt * fill_price[oid]
                if (fill_side[oid] == "BUY") {
                    n_buy++; vol_buy_btc += amt; sum_buy_price += amt * fill_price[oid]
                } else {
                    n_sell++; vol_sell_btc += amt; sum_sell_price += amt * fill_price[oid]
                }
                # gravar TSV: ts \t side \t amt \t price \t brl
                printf "%s\t%s\t%.6f\t%.2f\t%.2f\n", \
                    fill_ts[oid], fill_side[oid], amt, fill_price[oid], amt*fill_price[oid] > FILLS_OUT
            }
            print "fills_n=" n_fills
            printf "fills_btc=%.6f\n", vol_fills_btc
            printf "fills_brl=%.2f\n", vol_fills_brl
            print "fills_buy_n=" n_buy
            print "fills_sell_n=" n_sell
            printf "fills_buy_btc=%.6f\n", vol_buy_btc
            printf "fills_sell_btc=%.6f\n", vol_sell_btc
            if (vol_buy_btc > 0) printf "avg_buy_price=%.2f\n", sum_buy_price/vol_buy_btc; else print "avg_buy_price=0"
            if (vol_sell_btc > 0) printf "avg_sell_price=%.2f\n", sum_sell_price/vol_sell_btc; else print "avg_sell_price=0"
        }
    ' "$LOG_SLICE" > "$LOG_STATS_FILE"

    # carregar em S
    while IFS='=' read -r k v; do
        [[ -z "$k" ]] && continue
        S["$k"]="$v"
    done < "$LOG_STATS_FILE"

    AVG_LIFE="${S[avg_life]}"
    MIN_LIFE="${S[min_life]}"
    MAX_LIFE="${S[max_life]}"
fi
AVG_LIFE_UNIT=$([[ "$AVG_LIFE" == "n/a" ]] && echo "" || echo "s")

# --- health snapshot ------------------------------------------------------
BOT_PID=$(ps -eo pid,cmd | grep -E "conf_xemm_lead_lag_shadow|xemm_lead_lag_btc_brl" | grep -v grep | grep -v "analyze_xemm" | awk '{print $1}' | head -1)
BOT_RSS="n/a"
BOT_STATUS="PARADO"
if [[ -n "$BOT_PID" ]]; then
    BOT_STATUS="PID $BOT_PID"
    RSS_KB=$(ps -o rss= -p "$BOT_PID" 2>/dev/null | tr -d ' ' || echo 0)
    if [[ -n "$RSS_KB" && "$RSS_KB" != "0" ]]; then
        BOT_RSS=$(awk -v k="$RSS_KB" 'BEGIN{ printf "%.0fMB RSS", k/1024 }')
        BOT_STATUS="$BOT_STATUS ($BOT_RSS)"
    fi
fi

# último log timestamp
LAST_LOG_AGE_SEC="n/a"
LAST_LOG_OK="?"
if [[ -f "$LOG_FILE" ]]; then
    LAST_LOG_LINE=$(tail -1 "$LOG_FILE" 2>/dev/null || true)
    LAST_LOG_TS=$(echo "$LAST_LOG_LINE" | awk '{ print $1 " " $2 }' | sed 's/,.*//')
    if [[ -n "$LAST_LOG_TS" ]]; then
        LAST_LOG_EPOCH=$(date -d "$LAST_LOG_TS" +%s 2>/dev/null || echo 0)
        NOW_EPOCH=$(date +%s)
        if [[ "$LAST_LOG_EPOCH" -gt 0 ]]; then
            LAST_LOG_AGE_SEC=$((NOW_EPOCH - LAST_LOG_EPOCH))
            if [[ "$LAST_LOG_AGE_SEC" -gt 60 ]]; then LAST_LOG_OK="STALE"; else LAST_LOG_OK="OK"; fi
        fi
    fi
fi

# cobertura CSV
COVERAGE_PCT=$(awk -v r="${S[rows]}" -v d="${S[duration_sec]}" '
    BEGIN { if (d > 0) printf "%.1f", r/(d+1)*100; else print "0" }
')

# regime %
ROWS=${S[rows]}
PCT_OK=$(awk -v n="${S[regime_OK]:-0}" -v r="$ROWS" 'BEGIN{ printf "%.1f", n/r*100 }')
PCT_DEG=$(awk -v n="${S[regime_DEGRADED]:-0}" -v r="$ROWS" 'BEGIN{ printf "%.1f", n/r*100 }')
PCT_WARM=$(awk -v n="${S[regime_WARMUP]:-0}" -v r="$ROWS" 'BEGIN{ printf "%.1f", n/r*100 }')
PCT_PAUSED=$(awk -v n="${S[regime_PAUSED]:-0}" -v r="$ROWS" 'BEGIN{ printf "%.1f", n/r*100 }')

# distribuição maker_vs_taker (em %)
pct_of() { awk -v n="$1" -v r="$ROWS" 'BEGIN{ printf "%.2f", n/r*100 }'; }
PCT_NEG=$(pct_of "${S[b_neg]:-0}")
PCT_0_3=$(pct_of "${S[b_0_3]:-0}")
PCT_3_MIN=$(pct_of "${S[b_3_min]:-0}")
PCT_MIN_TGT=$(pct_of "${S[b_min_tgt]:-0}")
PCT_ABOVE_TGT=$(pct_of "${S[b_above_tgt]:-0}")
PCT_ABOVE_MIN=$(pct_of "${S[ticks_above_min]:-0}")
PCT_ARB_NEAR=$(pct_of "${S[arb_near_count]:-0}")

# cancels %
PCT_CANCEL_BELOW="0"
PCT_CANCEL_ABOVE="0"
if [[ "$CANCEL_TOTAL" -gt 0 ]]; then
    PCT_CANCEL_BELOW=$(awk -v n="$CANCEL_BELOW" -v t="$CANCEL_TOTAL" 'BEGIN{ printf "%.0f", n/t*100 }')
    PCT_CANCEL_ABOVE=$(awk -v n="$CANCEL_ABOVE" -v t="$CANCEL_TOTAL" 'BEGIN{ printf "%.0f", n/t*100 }')
fi

# mode mix
MODE_TOTAL=$((MODE_IMPROVE + MODE_JOIN + MODE_FALLBACK))
PCT_IMPROVE="0"; PCT_JOIN="0"; PCT_FALLBACK="0"
if [[ "$MODE_TOTAL" -gt 0 ]]; then
    PCT_IMPROVE=$(awk -v n="$MODE_IMPROVE" -v t="$MODE_TOTAL" 'BEGIN{ printf "%.0f", n/t*100 }')
    PCT_JOIN=$(awk -v n="$MODE_JOIN"    -v t="$MODE_TOTAL" 'BEGIN{ printf "%.0f", n/t*100 }')
    PCT_FALLBACK=$(awk -v n="$MODE_FALLBACK" -v t="$MODE_TOTAL" 'BEGIN{ printf "%.0f", n/t*100 }')
fi

# capital sufficiency
need_brl=$(awk -v a="$ORDER_AMOUNT" -v p="${S[last_local_ask]}" 'BEGIN{ printf "%.2f", a*p }')
need_btc="$ORDER_AMOUNT"
need_usdt=$(awk -v a="$ORDER_AMOUNT" -v p="${S[last_taker_local_ask]}" -v fx="${S[last_fx_mid]}" 'BEGIN{ if (fx>0) printf "%.2f", a*p/fx; else print "0" }')

cap_brl_ok=$(awk -v c="${S[last_maker_quote]}" -v n="$need_brl" 'BEGIN{ print (c>=n)?"OK":"BAIXO" }')
cap_btc_ok=$(awk -v c="${S[last_taker_base]}" -v n="$need_btc" 'BEGIN{ print (c>=n)?"OK":"BAIXO" }')
cap_usdt_ok=$(awk -v c="${S[last_taker_quote]}" -v n="$need_usdt" 'BEGIN{ print (c>=n)?"OK":"BAIXO" }')

# --- alertas --------------------------------------------------------------
ALERTS=()
add_alert() { ALERTS+=("$1"); }

# Bloco 1
if [[ "$LAST_LOG_AGE_SEC" != "n/a" && "$LAST_LOG_AGE_SEC" -gt 60 ]]; then
    add_alert "[CRÍTICO] Último log foi há ${LAST_LOG_AGE_SEC}s (>60s) — bot pode estar parado/travado"
fi
COV_INT=$(printf "%.0f" "$COVERAGE_PCT")
if (( COV_INT < 50 )); then
    add_alert "[CRÍTICO] Cobertura CSV ${COVERAGE_PCT}% (<50%) — starvation severa"
elif (( COV_INT < 90 )); then
    add_alert "[ALTO] Cobertura CSV ${COVERAGE_PCT}% (50–90%) — investigar gaps"
fi
if awk "BEGIN{exit !($PCT_OK < 95)}"; then
    add_alert "[ALTO] Regime OK em ${PCT_OK}% (<95% do período)"
fi

# Bloco 2
if [[ "$FILLS" -eq 0 && "$CREATED_TOTAL" -gt 0 ]]; then
    if [[ "${S[duration_sec]}" -gt 14400 ]]; then
        add_alert "[ALTO] Zero fills em sessão >${S[duration_sec]}s com $CREATED_TOTAL ordens criadas"
    fi
fi
if [[ "$OTR" -gt 300 ]]; then
    add_alert "[CRÍTICO] OTR=$OTR (>300) — risco de penalidade da exchange"
elif [[ "$OTR" -gt 100 ]]; then
    add_alert "[ALTO] OTR=$OTR (>100)"
fi
if [[ "$MODE_TOTAL" -gt 0 ]] && (( PCT_FALLBACK > 5 )); then
    add_alert "[ALTO] mode=fallback em ${PCT_FALLBACK}% das ordens (>5%)"
fi
if [[ "$CANCEL_TOTAL" -gt 0 ]] && (( PCT_CANCEL_ABOVE > 60 )); then
    add_alert "[MÉDIO] above_max domina ${PCT_CANCEL_ABOVE}% dos cancels — janela [min,max] estreita"
fi
if [[ "$AVG_LIFE" != "n/a" ]] && awk "BEGIN{exit !($AVG_LIFE < 30)}"; then
    add_alert "[MÉDIO] vida média das ordens=${AVG_LIFE}s (<30s)"
fi

# Bloco 3
if [[ "${S[ticks_above_min]:-0}" -eq 0 && "${S[duration_sec]:-0}" -gt 14400 ]]; then
    add_alert "[ALTO] maker_vs_taker_bps nunca atingiu min_profitability (${MIN_PROF_BPS}bps) em >4h"
fi
if awk "BEGIN{exit !($PCT_ABOVE_MIN < 0.1)}"; then
    add_alert "[MÉDIO] maker_vs_taker acima de min_prof apenas ${PCT_ABOVE_MIN}% do tempo"
fi
if awk "BEGIN{exit !($PCT_ARB_NEAR > 10)}"; then
    add_alert "[INFO] arb_near_threshold em ${PCT_ARB_NEAR}% do tempo — considerar enable_pure_arb"
fi
if [[ "${S[unique_lead]:-0}" -lt 5 && "$ROWS" -gt 100 ]]; then
    add_alert "[INFO] best_lead_bps com apenas ${S[unique_lead]} valores únicos — sinal possivelmente travado"
fi

# Bloco 4
if [[ "$cap_brl_ok" == "BAIXO" ]]; then
    add_alert "[ALTO] maker_quote=${S[last_maker_quote]} BRL < necessário ${need_brl} para 1 trade"
fi
if [[ "$cap_btc_ok" == "BAIXO" ]]; then
    add_alert "[ALTO] taker_base=${S[last_taker_base]} BTC < necessário ${need_btc} para 1 hedge"
fi
if [[ "$cap_usdt_ok" == "BAIXO" ]]; then
    add_alert "[ALTO] taker_quote=${S[last_taker_quote]} USDT < necessário ${need_usdt} para 1 hedge"
fi
DRIFT_CP_ABS=$(awk -v d="${S[drift_cp]}" 'BEGIN{ printf "%.6f", (d<0)?-d:d }')
if [[ "${S[duration_sec]}" -ge 3600 ]] && awk "BEGIN{exit !($DRIFT_CP_ABS > 0.05)}"; then
    add_alert "[MÉDIO] drift de combined_pct=${S[drift_cp]} em ${S[duration_sec]}s (>0.05/h)"
fi
if [[ "$FILLS" -gt 0 ]]; then
    side_max=$FILLS_BUY; [[ "$FILLS_SELL" -gt "$side_max" ]] && side_max=$FILLS_SELL
    pct_side=$(awk -v s="$side_max" -v t="$FILLS" 'BEGIN{ printf "%.0f", s/t*100 }')
    if (( pct_side > 80 )); then
        add_alert "[MÉDIO] fills concentrados em 1 lado (${pct_side}%) — risco de drift direcional"
    fi
fi

# Bloco 5
if [[ "${S[gaps_count]:-0}" -gt 0 ]]; then
    add_alert "[ALTO] ${S[gaps_count]} gaps >26s detectados no CSV (event-loop starvation)"
fi
if [[ "$WS_BOOK_ERR" -gt 0 ]]; then
    add_alert "[MÉDIO] $WS_BOOK_ERR erros de WebSocket (order book)"
fi
if [[ "$WS_USER_ERR" -gt 0 ]]; then
    add_alert "[MÉDIO] $WS_USER_ERR erros de WebSocket (user stream)"
fi
if [[ "$RACE_NONE" -gt 3 ]]; then
    add_alert "[ALTO] $RACE_NONE race conditions (NoneType.is_done)"
fi
if [[ "$BUDGET_ERR" -gt 0 ]]; then
    add_alert "[INFO] $BUDGET_ERR ocorrências de 'Not enough budget'"
fi
if [[ "$HEDGE_FAIL" -gt 0 ]]; then
    add_alert "[ALTO] $HEDGE_FAIL falhas de hedge"
fi

# --- estilização (cores ANSI + helpers de box) ----------------------------
if [[ -t 1 ]] && [[ "${NO_COLOR:-}" == "" ]]; then
    R=$'\033[31m'; G=$'\033[32m'; Y=$'\033[33m'; B=$'\033[34m'
    M=$'\033[35m'; C=$'\033[36m'; W=$'\033[37m'
    BOLD=$'\033[1m'; DIM=$'\033[2m'; N=$'\033[0m'
else
    R=''; G=''; Y=''; B=''; M=''; C=''; W=''; BOLD=''; DIM=''; N=''
fi

# repetir char unicode N vezes (tr não funciona com multi-byte)
repeat_char() {
    awk -v n="$1" -v c="$2" 'BEGIN{ for(i=0;i<n;i++) printf "%s", c }'
}

# barra de preenchimento: bar pct [width] [filled_color]
bar() {
    local pct=$1 width=${2:-30} color=${3:-$C}
    local n=$(awk -v p="$pct" -v m="$width" 'BEGIN{ x=p*m/100+0.5; if(x<0)x=0; if(x>m)x=m; printf "%d", x }')
    local r=$((width - n))
    local filled='' empty=''
    [[ $n -gt 0 ]] && filled=$(repeat_char "$n" "█")
    [[ $r -gt 0 ]] && empty=$(repeat_char  "$r" "░")
    printf "%s%s%s%s%s" "$color" "$filled" "$DIM" "$empty" "$N"
}

# linha horizontal de N caracteres "─"
hline() { repeat_char "$1" "─"; }

# colorir status segundo limiar
status_dot() {  # value good_threshold (>= → green)
    awk -v v="$1" -v t="$2" 'BEGIN{ if (v+0 >= t+0) exit 0; exit 1 }' \
        && printf "%s●%s" "$G" "$N" \
        || printf "%s●%s" "$Y" "$N"
}

color_value() {  # value condition (gt|lt) threshold
    local v="$1" cond="$2" t="$3"
    if [[ "$cond" == "gt" ]]; then
        awk -v v="$v" -v t="$t" 'BEGIN{ exit !(v+0 > t+0) }' \
            && printf "%s%s%s" "$G" "$v" "$N" \
            || printf "%s%s%s" "$Y" "$v" "$N"
    else
        awk -v v="$v" -v t="$t" 'BEGIN{ exit !(v+0 < t+0) }' \
            && printf "%s%s%s" "$G" "$v" "$N" \
            || printf "%s%s%s" "$Y" "$v" "$N"
    fi
}

# --- métricas derivadas (volume, throughput, P&L) -------------------------
DUR=${S[duration_sec]:-1}
[[ "$DUR" -lt 1 ]] && DUR=1

# rates
ORDERS_PER_HOUR=$(awk -v c="$CREATED_TOTAL" -v d="$DUR" 'BEGIN{ if(d>0) printf "%.1f", c*3600/d; else print "0" }')
FILLS_PER_HOUR=$(awk -v f="${S[fills_n]:-0}" -v d="$DUR" 'BEGIN{ if(d>0) printf "%.2f", f*3600/d; else print "0" }')
FILL_RATE=$(awk -v f="${S[fills_n]:-0}" -v c="$CREATED_TOTAL" 'BEGIN{ if(c>0) printf "%.1f", f*100/c; else print "0" }')

VOL_CREATED_BRL="${S[vol_created_brl]:-0.00}"
VOL_CREATED_BTC="${S[vol_created_btc]:-0.000000}"
VOL_FILLS_BRL="${S[fills_brl]:-0.00}"
VOL_FILLS_BTC="${S[fills_btc]:-0.000000}"
THROUGHPUT_BRL_H=$(awk -v v="$VOL_FILLS_BRL" -v d="$DUR" 'BEGIN{ if(d>0) printf "%.2f", v*3600/d; else print "0" }')

# P&L estimado: se houve fills BUY e SELL, comparar avg prices
PNL_BRL="0.00"
PNL_BPS="n/a"
if [[ -n "${S[fills_buy_n]:-}" && -n "${S[fills_sell_n]:-}" ]] \
   && [[ "${S[fills_buy_n]}" -gt 0 ]] && [[ "${S[fills_sell_n]}" -gt 0 ]]; then
    PNL_RES=$(awk -v bp="${S[avg_buy_price]:-0}" -v sp="${S[avg_sell_price]:-0}" \
                  -v bb="${S[fills_buy_btc]:-0}" -v sb="${S[fills_sell_btc]:-0}" '
        BEGIN {
            min_btc = (bb < sb) ? bb : sb
            pnl_brl = (sp - bp) * min_btc
            if (bp > 0) printf "%.2f|%.2f", pnl_brl, (sp-bp)/bp*10000
            else printf "%.2f|0", pnl_brl
        }')
    IFS='|' read -r PNL_BRL PNL_BPS <<< "$PNL_RES"
fi

# regime ratio em barra
PCT_OK_INT=$(printf "%.0f" "$PCT_OK")
COV_INT_BAR=$(printf "%.0f" "$COVERAGE_PCT")

# decidir cor do status global do bot
if [[ "$BOT_PID" == "" ]]; then
    STATUS_DOT="${R}●${N}"
    STATUS_TXT="${R}OFFLINE${N}"
elif [[ "$LAST_LOG_OK" == "STALE" ]]; then
    STATUS_DOT="${Y}●${N}"
    STATUS_TXT="${Y}STALE${N}"
else
    STATUS_DOT="${G}●${N}"
    STATUS_TXT="${G}LIVE${N}"
fi

SHADOW_BADGE="${R}LIVE${N}"
[[ "${SHADOW_MODE_RAW:-}" == "true" ]] && SHADOW_BADGE="${Y}SHADOW${N}"

# duração formatada
DUR_FMT=$(awk -v d="$DUR" 'BEGIN{
    h=int(d/3600); m=int((d%3600)/60); s=d%60
    if (h>0) printf "%dh%02dm%02ds", h,m,s
    else if (m>0) printf "%dm%02ds", m,s
    else printf "%ds", s
}')

# --- output ---------------------------------------------------------------
NOW_UTC=$(date -u "+%Y-%m-%d %H:%M UTC")

# helper: section header (─── TÍTULO ──────...───)
section() {
    local title="$1" total=${2:-78}
    local title_len=${#title}
    # "─── " (4) + title + " " (1) + fill + ""  → total = 4 + title_len + 1 + fill
    local fill=$((total - 4 - title_len - 1))
    [[ $fill -lt 3 ]] && fill=3
    echo
    printf "${BOLD}${C}─── %s ${N}${DIM}${C}%s${N}\n" "$title" "$(repeat_char "$fill" "─")"
}

# ── Header card ───────────────────────────────────────────────────────────
echo
printf "${BOLD}${C}╔══════════════════════════════════════════════════════════════════════════╗${N}\n"
printf "${BOLD}${C}║${N}  ${BOLD}XEMM LEAD-LAG${N}  ${DIM}•${N}  ${BOLD}BTC-BRL${N}  ${DIM}•${N}  ${SHADOW_BADGE}  ${DIM}•${N}  ${STATUS_DOT} ${STATUS_TXT}\n"
printf "${BOLD}${C}║${N}  ${DIM}%s   •   %s   •   duração %s${N}\n" \
    "$NOW_UTC" "$PERIOD_LABEL" "${DUR_FMT}"
printf "${BOLD}${C}║${N}  ${DIM}CSV: %s${N}\n" "$CSV_NAME"
printf "${BOLD}${C}╚══════════════════════════════════════════════════════════════════════════╝${N}\n"

# ── KPIs principais (4 colunas alinhadas a 22 chars cada) ─────────────────
section "KPIs PRINCIPAIS"
echo
# Helper: gera célula de exatamente W chars (label + valor)
kpi_cell() {  # label value width
    local label="$1" val="$2" w="${3:-22}"
    local txt="$label $val"
    printf "%-${w}s" "$txt"
}

W=22
# headers + linha de separação
printf "  ${BOLD}${B}%-${W}s%-${W}s%-${W}s%s${N}\n"  "ORDENS"  "FILLS"  "VOLUME"  "P&L"
printf "  ${DIM}%-${W}s%-${W}s%-${W}s%s${N}\n" \
    "$(repeat_char 18 '─')"  "$(repeat_char 18 '─')"  "$(repeat_char 18 '─')"  "$(repeat_char 18 '─')"

# Cada linha: 4 cells de exatamente W chars
echo "$(printf '  '; \
        kpi_cell "Criadas" "$(printf '%6d' "$CREATED_TOTAL")" $W; \
        kpi_cell "Total  " "$(printf '%6d' "${S[fills_n]:-0}")" $W; \
        kpi_cell "Criado" "$(printf '%10s BRL' "$VOL_CREATED_BRL")" $W; \
        kpi_cell "PnL   " "$(printf '%9s BRL' "$PNL_BRL")" $W)"
echo "$(printf '  '; \
        kpi_cell "  BUY  " "$(printf '%6d' "$CREATED_BUY")" $W; \
        kpi_cell "  BUY  " "$(printf '%6d' "${S[fills_buy_n]:-0}")" $W; \
        kpi_cell "      " "$(printf '%10s BTC' "$VOL_CREATED_BTC")" $W; \
        kpi_cell "      " "$(printf '%9s bps' "$PNL_BPS")" $W)"
echo "$(printf '  '; \
        kpi_cell "  SELL " "$(printf '%6d' "$CREATED_SELL")" $W; \
        kpi_cell "  SELL " "$(printf '%6d' "${S[fills_sell_n]:-0}")" $W; \
        kpi_cell "Fillado" "$(printf '%9s BRL' "$VOL_FILLS_BRL")" $W; \
        kpi_cell "fills/h" "$(printf '%13s' "$FILLS_PER_HOUR")" $W)"
echo "$(printf '  '; \
        kpi_cell "Cancel " "$(printf '%6d' "$CANCEL_TOTAL")" $W; \
        kpi_cell "Fill r " "$(printf '%5s%%' "$FILL_RATE")" $W; \
        kpi_cell "      " "$(printf '%10s BTC' "$VOL_FILLS_BTC")" $W; \
        kpi_cell "ord/h " "$(printf '%14s' "$ORDERS_PER_HOUR")" $W)"
echo "$(printf '  '; \
        kpi_cell "OTR   " "$(printf '%14s' "$OTR_DISP")" $W; \
        kpi_cell "v.med " "$(printf '%13ss' "$AVG_LIFE")" $W; \
        kpi_cell "Thrput" "$(printf '%6s BRL/h' "$THROUGHPUT_BRL_H")" $W)"

# ── STATUS ────────────────────────────────────────────────────────────────
section "STATUS"
echo
printf "  ${STATUS_DOT}  ${BOLD}Bot${N}:        %s\n" "$BOT_STATUS"
local_age_color="$G"
[[ "$LAST_LOG_OK" == "STALE" ]] && local_age_color="$Y"
[[ "$LAST_LOG_OK" == "?" ]] && local_age_color="$DIM"
printf "  ●  ${BOLD}Último log${N}: ${local_age_color}%ss atrás [%s]${N}\n" "$LAST_LOG_AGE_SEC" "$LAST_LOG_OK"
COV_COLOR="$G"; (( COV_INT_BAR < 90 )) && COV_COLOR="$Y"; (( COV_INT_BAR < 50 )) && COV_COLOR="$R"
printf "  ●  ${BOLD}Cobertura${N}:  %s ${COV_COLOR}${BOLD}%5s%%${N}   ${DIM}(%s/%s linhas)${N}\n" \
    "$(bar "$COVERAGE_PCT" 24 "$COV_COLOR")" "$COVERAGE_PCT" "${S[rows]}" "$((${S[duration_sec]:-0}+1))"
REG_COLOR="$G"; (( PCT_OK_INT < 95 )) && REG_COLOR="$Y"; (( PCT_OK_INT < 80 )) && REG_COLOR="$R"
printf "  ●  ${BOLD}Regime OK${N}:  %s ${REG_COLOR}${BOLD}%5s%%${N}   ${DIM}DEG %s%%  WARM %s%%  PAUSED %s%%${N}\n" \
    "$(bar "$PCT_OK" 24 "$REG_COLOR")" "$PCT_OK" "$PCT_DEG" "$PCT_WARM" "$PCT_PAUSED"
SQ_DISP=""
for k in "${!S[@]}"; do [[ "$k" == sq_* ]] && SQ_DISP+="${k#sq_}=${S[$k]} "; done
printf "  ●  ${BOLD}signal_quality${N}: %s\n" "$SQ_DISP"
ERR_COLOR="$G"; [[ "$ERR_REAL" -gt 0 ]] && ERR_COLOR="$Y"
printf "  ●  ${BOLD}Executors${N}: média %s   máx %s   ${DIM}erros reais=${ERR_COLOR}%s${N}\n" \
    "${S[avg_nex]}" "${S[max_nex]}" "$ERR_REAL"

# ── SPREAD MARKET-MAKING (com barras de distribuição) ─────────────────────
section "SPREAD MARKET-MAKING"
echo
printf "  ${BOLD}maker_vs_taker${N}   avg ${BOLD}%7s${N}    p50 %7s    p95 %7s    max ${BOLD}%7s${N}  bps\n" \
    "${S[avg_mvt]}" "${S[p50_mvt]}" "${S[p95_mvt]}" "${S[max_mvt]}"
echo
printf "  ${DIM}Distribuição (%% do tempo na faixa de spread):${N}\n"
printf "    %-13s  %s  ${BOLD}%6s%%${N}\n"        "<0bps"                                  "$(bar "$PCT_NEG"      36 "$R")"  "$PCT_NEG"
printf "    %-13s  %s  ${BOLD}%6s%%${N}\n"        "0-3bps"                                 "$(bar "$PCT_0_3"     36 "$Y")"  "$PCT_0_3"
printf "    %-13s  %s  ${BOLD}%6s%%${N}\n"        "3-${MIN_PROF_BPS}bps"                   "$(bar "$PCT_3_MIN"   36 "$Y")"  "$PCT_3_MIN"
printf "    %-13s  %s  ${BOLD}%6s%%${N} ${G}← fill viável${N}\n"  "${MIN_PROF_BPS}-${TARGET_PROF_BPS}bps"  "$(bar "$PCT_MIN_TGT" 36 "$G")"  "$PCT_MIN_TGT"
printf "    %-13s  %s  ${BOLD}%6s%%${N} ${G}← fill ótimo${N}\n"   ">${TARGET_PROF_BPS}bps"                 "$(bar "$PCT_ABOVE_TGT" 36 "$G")"  "$PCT_ABOVE_TGT"
echo
printf "  ${BOLD}Ticks ≥ min_prof${N} (${MIN_PROF_BPS} bps): ${BOLD}%s${N}  (${BOLD}%s%%${N} do período)  ${DIM}← KPI principal${N}\n" \
    "${S[ticks_above_min]:-0}" "$PCT_ABOVE_MIN"
printf "  ${DIM}Config: min=%s  tgt=%s  max=%s  placement_buf=%s bps${N}\n" \
    "$MIN_PROF_BPS" "$TARGET_PROF_BPS" "$MAX_PROF_BPS" "$PLACEMENT_BUFFER_BPS"

# ── SINAL & MARKET ────────────────────────────────────────────────────────
section "SINAL E CONDIÇÕES DE MERCADO"
echo
printf "  ${BOLD}local_spread${N}  ${DIM}(BitPreco book)${N}    avg ${BOLD}%6s${N}   p90 %6s   max %6s  bps\n" \
    "${S[avg_local_spread]}" "${S[p90_local_spread]}" "${S[max_local_spread]}"
printf "  ${BOLD}basis_bps${N}                          avg ${BOLD}%6s${N}   drift %+7s bps\n" \
    "${S[avg_basis]}" "${S[drift_basis]}"
printf "  ${BOLD}best_lead${N}                          avg %6s   abs_avg %6s   ${DIM}unique=%s${N}\n" \
    "${S[avg_lead]}" "${S[abs_avg_lead]}" "${S[unique_lead]}"
printf "  ${BOLD}arb_near${N}                           %6s%% do tempo   ${DIM}long_max %s, short_max %s${N}\n" \
    "$PCT_ARB_NEAR" "${S[max_arb_long]}" "${S[max_arb_short]}"
printf "  ${DIM}target_buy   avg %6s  range [%s, %s] bps${N}\n" \
    "${S[avg_tpb]}" "${S[min_tpb]}" "${S[max_tpb]}"
printf "  ${DIM}target_sell  avg %6s  range [%s, %s] bps${N}\n" \
    "${S[avg_tps]}" "${S[min_tps]}" "${S[max_tps]}"

# ── INVENTÁRIO & CAPITAL ──────────────────────────────────────────────────
section "INVENTÁRIO E CAPITAL"
echo
printf "  ${BOLD}combined_pct${N}     ${BOLD}%s${N} → ${BOLD}%s${N}   drift %+s   ${DIM}range [%s, %s]${N}\n" \
    "${S[first_cp]}" "${S[last_cp]}" "${S[drift_cp]}" "${S[min_cp]}" "${S[max_cp]}"
printf "  ${BOLD}inventory_skew${N}   avg %s   audit_drift_active %s ticks\n" \
    "${S[avg_isk]}" "${S[audit_drift_active_count]}"
echo
printf "  ${DIM}Saldos (último tick):${N}\n"
ico_brl="${G}✓${N}";  [[ "$cap_brl_ok"  != "OK" ]] && ico_brl="${R}✗${N}"
ico_btc="${G}✓${N}";  [[ "$cap_btc_ok"  != "OK" ]] && ico_btc="${R}✗${N}"
ico_usdt="${G}✓${N}"; [[ "$cap_usdt_ok" != "OK" ]] && ico_usdt="${R}✗${N}"
printf "    %s  ${BOLD}%-16s${N} ${BOLD}%14s${N}    ${DIM}(need %s)${N}\n" "$ico_brl"  "BitPreco BRL"  "${S[last_maker_quote]}" "$need_brl"
printf "    %s  ${BOLD}%-16s${N} ${BOLD}%14s${N}    ${DIM}(need %s)${N}\n" "$ico_btc"  "Binance BTC"   "${S[last_taker_base]}"  "$need_btc"
printf "    %s  ${BOLD}%-16s${N} ${BOLD}%14s${N}    ${DIM}(need %s)${N}\n" "$ico_usdt" "Binance USDT"  "${S[last_taker_quote]}" "$need_usdt"
# inventário congelado?
if (( ROWS > 60 )); then
    range_cp=$(awk -v lo="${S[min_cp]}" -v hi="${S[max_cp]}" 'BEGIN{ printf "%.6f", hi-lo }')
    if awk "BEGIN{exit !($range_cp < 0.001 && ${S[duration_sec]} > 3600)}"; then
        printf "  ${DIM}[INFO] Inventário congelado (range=%s) — esperado com zero fills.${N}\n" "$range_cp"
    fi
fi

# ── ÚLTIMOS FILLS ────────────────────────────────────────────────────────
if [[ -s "$FILLS_LIST_FILE" ]]; then
    section "ÚLTIMOS FILLS"
    echo
    printf "  ${DIM}%-10s  %-5s  %-13s  %-15s  %-13s${N}\n" \
        "Hora" "Lado" "BTC" "Preço (BRL)" "Notional BRL"
    sort -r "$FILLS_LIST_FILE" 2>/dev/null | head -10 | while IFS=$'\t' read -r ts side amt price brl; do
        side_color="$G"; [[ "$side" == "SELL" ]] && side_color="$R"
        printf "  %-10s  ${side_color}${BOLD}%-5s${N}  %-13s  %-15s  %-13s\n" \
            "$ts" "$side" "$amt" "$price" "$brl"
    done
fi

# ── DETECÇÃO DE FALHAS ────────────────────────────────────────────────────
section "DETECÇÃO DE FALHAS"
echo
gaps_n=${S[gaps_count]:-0}
ico() { [[ "$1" -eq 0 ]] && printf "${G}✓${N}" || printf "${R}✗${N}"; }
printf "  $(ico "$gaps_n") ${BOLD}Starvation${N}        gaps >26s         %5d\n" "$gaps_n"
if [[ "$gaps_n" -gt 0 ]]; then
    show=$gaps_n; [[ "$show" -gt 3 ]] && show=3
    for i in $(seq 1 "$show"); do
        printf "      ${DIM}- %s (gap %ss)${N}\n" "${S[gap_${i}_iso]}" "${S[gap_${i}_dur]}"
    done
    [[ "$gaps_n" -gt 3 ]] && printf "      ${DIM}... +%d mais${N}\n" $((gaps_n-3))
fi
printf "  $(ico "$WS_BOOK_ERR") ${BOLD}WebSocket${N}         order book        %5d\n" "$WS_BOOK_ERR"
printf "  $(ico "$WS_USER_ERR") ${BOLD}WebSocket${N}         user stream       %5d\n" "$WS_USER_ERR"
printf "  $(ico "$RACE_NONE") ${BOLD}Race condition${N}    NoneType.is_done  %5d\n" "$RACE_NONE"
printf "  $(ico "$BUDGET_ERR") ${BOLD}Budget${N}            errors            %5d\n" "$BUDGET_ERR"
printf "  $(ico "$HEDGE_FAIL") ${BOLD}Hedge${N}             failures          %5d\n" "$HEDGE_FAIL"

# ── DIAGNÓSTICO ───────────────────────────────────────────────────────────
section "DIAGNÓSTICO"
echo

DIAG_PRINTED=0
diag()     { printf "  ${Y}→${N} %s\n"      "$1"; DIAG_PRINTED=1; }
diag_sub() { printf "    ${DIM}%s${N}\n"    "$1"; }

if [[ "$OTR" -gt 100 ]] && (( PCT_CANCEL_ABOVE > 50 )); then
    diag "Janela [min,max] estreita (above_max ${PCT_CANCEL_ABOVE}% dos cancels)"
    diag_sub "Considerar aumentar max_profitability."
fi
if [[ "$OTR" -gt 100 ]] && (( PCT_CANCEL_BELOW > 70 )); then
    diag "Adverse selection (below_min ${PCT_CANCEL_BELOW}% dos cancels)"
    diag_sub "Considerar aumentar min_requote_interval_sec."
fi
if [[ "${S[ticks_above_min]:-0}" -eq 0 && "${S[duration_sec]:-0}" -gt 14400 ]]; then
    diag "Spread cross-exchange nunca atingiu min_prof (${MIN_PROF_BPS} bps) em >4h"
    diag_sub "Mercado fechado neste horário (avg maker_vs_taker=${S[avg_mvt]} bps)."
fi
if awk "BEGIN{exit !($PCT_ARB_NEAR > 10)}"; then
    diag "arb_near em ${PCT_ARB_NEAR}% do tempo — considerar enable_pure_arb"
fi
if [[ "${S[fills_n]:-0}" -gt 0 ]]; then
    side_max=${S[fills_buy_n]:-0}; side="BUY"
    [[ "${S[fills_sell_n]:-0}" -gt "$side_max" ]] && { side_max=${S[fills_sell_n]}; side="SELL"; }
    pct_side=$(awk -v s="$side_max" -v t="${S[fills_n]}" 'BEGIN{ printf "%.0f", s/t*100 }')
    if (( pct_side > 80 )); then
        diag "Fills concentrados em $side (${pct_side}%) — risco de drift direcional"
    fi
fi
if [[ "${S[unique_lead]:-0}" -lt 5 && "$ROWS" -gt 100 ]]; then
    diag "best_lead com baixa variação (${S[unique_lead]} valores únicos)"
    diag_sub "Verificar feed Binance futures."
fi
if (( COV_INT < 90 )); then
    diag "Possível starvation do event loop (cobertura ${COVERAGE_PCT}%)"
    diag_sub "Verificar MQTT e considerar desabilitar commlib."
fi

if (( DIAG_PRINTED == 0 )); then
    printf "  ${G}✓${N} Bot operando dentro dos parâmetros esperados. Nenhuma ação necessária.\n"
fi

# ── ALERTAS ───────────────────────────────────────────────────────────────
if [[ ${#ALERTS[@]} -gt 0 ]]; then
    section "ALERTAS"
    echo
    for a in "${ALERTS[@]}"; do
        a_color="$Y"
        [[ "$a" == *CRÍTICO* ]] && a_color="$R"
        [[ "$a" == *INFO* ]]    && a_color="$DIM"
        printf "  ${a_color}%s${N}\n" "$a"
    done
fi

echo
