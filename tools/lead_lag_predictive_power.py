"""Lead-lag predictive-power analysis.

Question: at time t, does ``lead_bps(window)`` predict the **forward return**
of ``local_mid`` over horizons Δ ∈ {1, 2, 5, 10, 30, 60}s?

Two analyses:

1. **Recorded windows** — uses the lead_5s/lead_10s/lead_15s columns the
   controller already wrote.

2. **Synthetic shorter windows** — recomputes the lead-lag signal for
   W ∈ {1, 2, 3, 5, 10, 15}s directly from leader+local+fx columns,
   using the same log-ratio formula as ``LeadLagSignalProvider.lead_signal_bps``.
   This lets us see whether the shorter windows the SBE cadence enables
   would actually carry predictive signal.

Output: per (window, horizon):
  - Pearson correlation
  - Hit rate (sign agreement) when |lead_bps| > threshold
  - Mean forward return, in bps, conditional on lead_bps > +T vs < -T

Run: python tools/lead_lag_predictive_power.py
"""
from __future__ import annotations

import csv
import glob
import math
import os
from typing import Dict, List, Optional, Tuple

CSV_GLOB = (
    "/home/ubuntu/hummingbot-ney/logs/xemm_lead_lag/"
    "xemm_lead_lag_xemm_lead_lag_btcbrl_v1_2026051[56]_*.csv"
)

WINDOWS_RECORDED = [5, 10, 15]  # columns already in CSV
WINDOWS_SYNTHETIC = [1, 2, 3, 5, 10, 15]  # recomputed
HORIZONS_SEC = [1, 2, 5, 10, 30, 60]
THRESHOLDS_BPS = [2.0, 3.0, 5.0]


def _f(v: str) -> Optional[float]:
    if v is None or v == "" or v == "None":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _load_csv(path: str) -> List[dict]:
    """Read into list of dicts with floats where possible.
    Skips rows where critical fields are missing."""
    rows: List[dict] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            ts = _f(r.get("timestamp"))
            local_mid = _f(r.get("local_mid"))
            leader_mid = _f(r.get("leader_mid"))
            fair_fast = _f(r.get("fair_brl_fast"))
            if ts is None or local_mid is None or local_mid <= 0:
                continue
            rows.append({
                "ts": ts,
                "local_mid": local_mid,
                "leader_mid": leader_mid,
                "fair_brl_fast": fair_fast,
                "lead_5s": _f(r.get("lead_5s")),
                "lead_10s": _f(r.get("lead_10s")),
                "lead_15s": _f(r.get("lead_15s")),
                "signal_quality": r.get("signal_quality"),
            })
    rows.sort(key=lambda x: x["ts"])
    return rows


def _build_time_index(rows: List[dict]) -> List[float]:
    return [r["ts"] for r in rows]


def _bsearch_at_or_before(ts_list: List[float], target: float) -> int:
    """Return largest i such that ts_list[i] <= target, or -1 if none."""
    lo, hi = 0, len(ts_list) - 1
    res = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if ts_list[mid] <= target:
            res = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return res


def _bsearch_at_or_after(ts_list: List[float], target: float) -> int:
    """Return smallest i such that ts_list[i] >= target, or -1 if none."""
    lo, hi = 0, len(ts_list) - 1
    res = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if ts_list[mid] >= target:
            res = mid
            hi = mid - 1
        else:
            lo = mid + 1
    return res


def _synthetic_lead_bps(rows: List[dict], ts_list: List[float],
                       i: int, window_sec: int) -> Optional[float]:
    """Recompute lead_bps using the same formula as LeadLagSignalProvider.

    lead_bps = 10000 * (log(fair_now/fair_past) - log(local_now/local_past))
    """
    cur = rows[i]
    if cur["fair_brl_fast"] is None or cur["fair_brl_fast"] <= 0:
        return None
    past_ts = cur["ts"] - window_sec
    j = _bsearch_at_or_before(ts_list, past_ts)
    if j < 0:
        return None
    past = rows[j]
    if (past["fair_brl_fast"] is None or past["fair_brl_fast"] <= 0
            or past["local_mid"] <= 0):
        return None
    # Don't require exact match — accept any past row within ~2s of target
    if abs(past["ts"] - past_ts) > 2.0:
        return None
    fair_ret = math.log(cur["fair_brl_fast"] / past["fair_brl_fast"])
    local_ret = math.log(cur["local_mid"] / past["local_mid"])
    return 10000.0 * (fair_ret - local_ret)


def _forward_return_bps(rows: List[dict], ts_list: List[float],
                        i: int, horizon_sec: int) -> Optional[float]:
    cur = rows[i]
    fwd_ts = cur["ts"] + horizon_sec
    j = _bsearch_at_or_after(ts_list, fwd_ts)
    if j < 0:
        return None
    fwd = rows[j]
    # Accept up to 2s slop
    if abs(fwd["ts"] - fwd_ts) > 2.0:
        return None
    if fwd["local_mid"] <= 0 or cur["local_mid"] <= 0:
        return None
    return 10000.0 * math.log(fwd["local_mid"] / cur["local_mid"])


def _pearson(xs: List[float], ys: List[float]) -> Tuple[float, int]:
    n = len(xs)
    if n < 30:
        return float("nan"), n
    mx = sum(xs) / n
    my = sum(ys) / n
    sx2 = sum((x - mx) ** 2 for x in xs)
    sy2 = sum((y - my) ** 2 for y in ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    denom = math.sqrt(sx2 * sy2)
    if denom <= 0:
        return float("nan"), n
    return sxy / denom, n


def _hit_rate(xs: List[float], ys: List[float], threshold: float) -> Tuple[float, int]:
    """Among samples where |x| > threshold, fraction where sign(x) == sign(y)."""
    hits = 0
    total = 0
    for x, y in zip(xs, ys):
        if abs(x) <= threshold:
            continue
        if x * y > 0:
            hits += 1
        total += 1
    if total == 0:
        return float("nan"), 0
    return hits / total, total


def _conditional_mean(xs: List[float], ys: List[float],
                      threshold: float, side: str) -> Tuple[float, int]:
    """Mean forward return when lead_bps > +T (side='pos') or < -T (side='neg')."""
    selected = []
    for x, y in zip(xs, ys):
        if side == "pos" and x > threshold:
            selected.append(y)
        elif side == "neg" and x < -threshold:
            selected.append(y)
    if not selected:
        return float("nan"), 0
    return sum(selected) / len(selected), len(selected)


def analyse(rows: List[dict]) -> None:
    if not rows:
        print("No rows loaded.")
        return
    ts_list = _build_time_index(rows)
    span_sec = rows[-1]["ts"] - rows[0]["ts"]
    print(f"Loaded {len(rows)} rows spanning {span_sec/3600:.2f} hours")
    print(f"  cadence p50: {_median_dt(rows):.2f}s")
    print()

    # ---- Pre-compute forward returns once per horizon ----
    fwd_ret_by_horizon: Dict[int, List[Optional[float]]] = {}
    for h in HORIZONS_SEC:
        fwd_ret_by_horizon[h] = [
            _forward_return_bps(rows, ts_list, i, h) for i in range(len(rows))
        ]

    # ---- Pre-compute synthetic lead_bps per window ----
    synth_by_window: Dict[int, List[Optional[float]]] = {}
    for w in WINDOWS_SYNTHETIC:
        synth_by_window[w] = [
            _synthetic_lead_bps(rows, ts_list, i, w) for i in range(len(rows))
        ]

    # ---- Recorded windows ----
    print("=" * 80)
    print("PART 1: RECORDED lead_bps (from CSV columns lead_5s/10s/15s)")
    print("=" * 80)
    _report_correlations(rows, fwd_ret_by_horizon, source="recorded")

    print()
    print("=" * 80)
    print("PART 2: SYNTHETIC lead_bps (recomputed from leader/fx/local series)")
    print("        Validates whether short SBE-enabled windows carry signal")
    print("=" * 80)
    _report_correlations(rows, fwd_ret_by_horizon, source="synthetic",
                         synth=synth_by_window)


def _median_dt(rows: List[dict]) -> float:
    dts = [rows[i + 1]["ts"] - rows[i]["ts"] for i in range(len(rows) - 1)]
    dts = [d for d in dts if 0 < d < 10]
    if not dts:
        return 0.0
    dts.sort()
    return dts[len(dts) // 2]


def _report_correlations(rows, fwd_ret_by_horizon, source: str,
                         synth: Optional[Dict[int, List]] = None) -> None:
    windows = WINDOWS_RECORDED if source == "recorded" else WINDOWS_SYNTHETIC

    # Pearson table
    print(f"\n[{source}] Pearson r ( lead_bps(W)  vs  local_mid forward return at Δ )")
    print(f"{'window↓ / Δ→':<14}" + "".join(f"{h:>10}s" for h in HORIZONS_SEC))
    for w in windows:
        line = f"{w:>3}s          "
        for h in HORIZONS_SEC:
            xs, ys = _aligned_xy(rows, w, fwd_ret_by_horizon[h], source, synth)
            r, n = _pearson(xs, ys)
            line += f"{r:>10.3f}" if not math.isnan(r) else "      n/a "
        print(line)

    # Sample counts (single horizon Δ=2s as reference)
    print(f"\n[{source}] Sample counts (Δ=2s)")
    for w in windows:
        xs, ys = _aligned_xy(rows, w, fwd_ret_by_horizon[2], source, synth)
        n_total = len(xs)
        n_above_3bps = sum(1 for x in xs if abs(x) > 3.0)
        n_above_5bps = sum(1 for x in xs if abs(x) > 5.0)
        print(f"  W={w:>2}s: total={n_total} |lead|>3bps={n_above_3bps} |lead|>5bps={n_above_5bps}")

    # Hit rate, |lead| > 3 bps, vs forward return
    for thr in THRESHOLDS_BPS:
        print(f"\n[{source}] Hit rate ( sign(lead_bps)==sign(fwd_ret) ) when |lead_bps| > {thr} bps")
        print(f"{'window↓ / Δ→':<14}" + "".join(f"{h:>10}s" for h in HORIZONS_SEC))
        for w in windows:
            line = f"{w:>3}s          "
            for h in HORIZONS_SEC:
                xs, ys = _aligned_xy(rows, w, fwd_ret_by_horizon[h], source, synth)
                hr, n = _hit_rate(xs, ys, thr)
                cell = f"{hr*100:>7.1f}%[{n}]" if not math.isnan(hr) else "    n/a    "
                line += cell
            print(line)

    # Conditional mean forward return (informative as edge magnitude)
    print(f"\n[{source}] Mean fwd return (bps) when lead_bps > +3 / < -3")
    print(f"{'window↓ / Δ→':<14}" + "".join(f"{h:>10}s" for h in HORIZONS_SEC))
    for w in windows:
        line_pos = f"{w:>3}s  pos>+3 "
        line_neg = f"{w:>3}s  neg<-3 "
        for h in HORIZONS_SEC:
            xs, ys = _aligned_xy(rows, w, fwd_ret_by_horizon[h], source, synth)
            mp, np_ = _conditional_mean(xs, ys, 3.0, "pos")
            mn, nn_ = _conditional_mean(xs, ys, 3.0, "neg")
            line_pos += f"{mp:>+7.2f}[{np_:>3}]" if not math.isnan(mp) else "    n/a    "
            line_neg += f"{mn:>+7.2f}[{nn_:>3}]" if not math.isnan(mn) else "    n/a    "
        print(line_pos)
        print(line_neg)


def _aligned_xy(rows, w, fwd_list, source, synth):
    xs, ys = [], []
    n = len(rows)
    for i in range(n):
        if source == "recorded":
            x = rows[i].get(f"lead_{w}s")
        else:
            x = synth[w][i]
        y = fwd_list[i]
        if x is None or y is None or math.isnan(x) or math.isnan(y):
            continue
        xs.append(x)
        ys.append(y)
    return xs, ys


def main():
    paths = sorted(glob.glob(CSV_GLOB))
    print(f"Files: {len(paths)}")
    for p in paths:
        print(f"  {os.path.basename(p)}")
    print()
    all_rows: List[dict] = []
    for p in paths:
        all_rows.extend(_load_csv(p))
    all_rows.sort(key=lambda x: x["ts"])
    analyse(all_rows)


if __name__ == "__main__":
    main()
