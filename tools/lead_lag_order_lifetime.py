"""Order-lifetime analysis for XEMM lead-lag maker orders.

Goal: quantify
  1. Distribution of maker-order lifetime by cancel reason (<min vs >max).
  2. lead_bps at moment of create vs at moment of cancel/fill.
  3. Whether the >max cancels are systematically firing during lead-favorable
     conditions (i.e., when the signal predicts the move will continue).

Data sources:
  - logs/logs_conf_xemm_lead_lag_sbe*.log — has create / cancel / fill events
    with order_id and timestamps.
  - logs/xemm_lead_lag/xemm_lead_lag_*.csv — Tier-3 CSV with lead_5s/10s/15s
    at ~1s cadence; used to look up lead_bps at any given time.

Run: python tools/lead_lag_order_lifetime.py
"""
from __future__ import annotations

import csv
import glob
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

LOG_GLOB = "/home/ubuntu/hummingbot-ney/logs/logs_conf_xemm_lead_lag_sbe*.log"
CSV_GLOB = (
    "/home/ubuntu/hummingbot-ney/logs/xemm_lead_lag/"
    "xemm_lead_lag_xemm_lead_lag_btcbrl_v1_2026051[56]_*.csv"
)

# Log patterns
RE_CREATE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?Created maker order \(LIMIT_MAKER\) "
    r"(\S+) side=(BUY|SELL) price=([\d.]+) mode=\S+ lead=(\w+) lead_bps=(-?[\d.]+) "
)
RE_CANCEL_GATE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?Order (\S+) profitability "
    r"(-?[\d.]+) bps ([<>]) (min|max) (-?[\d.]+) bps\. Cancelling order"
)
RE_FILL = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?The (BUY|SELL) order (\S+) "
    r"amounting to ([\d.]+)/([\d.]+).*?filled at ([\d.]+)"
)
RE_CANCEL_CONFIRM = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?BitPreco cancel confirmed "
    r"for exchange_order_id=\S+.*?ORDER_CANCELED"
)
RE_CANCEL_NOFILL = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?Maker order (\S+) cancelled with no fills"
)


def parse_ts(s: str) -> float:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f").timestamp()


def parse_logs() -> Tuple[Dict, Dict, Dict]:
    """Returns dicts keyed by order_id:
      creates[oid] = {ts, side, price, lead_mode, lead_bps}
      cancels[oid] = {ts, profitability_bps, direction (<|>), bound (min|max), threshold_bps}
      fills[oid]   = {ts, side, filled_amount, total_amount, price}
    Multiple fills per oid keep last.
    """
    creates: Dict[str, dict] = {}
    cancels: Dict[str, dict] = {}
    fills: Dict[str, dict] = {}
    log_paths = sorted(glob.glob(LOG_GLOB))
    print(f"Parsing {len(log_paths)} log file(s)")
    for path in log_paths:
        with open(path, errors="ignore") as f:
            for line in f:
                m = RE_CREATE.match(line)
                if m:
                    ts_str, oid, side, price, lead_mode, lead_bps = m.groups()
                    # Only track BitPreco maker order IDs (prefix B/S BCBL...)
                    if not (oid.startswith("BBCBL") or oid.startswith("SBCBL")):
                        continue
                    creates[oid] = {
                        "ts": parse_ts(ts_str),
                        "side": side,
                        "price": float(price),
                        "lead_mode": lead_mode,
                        "lead_bps": float(lead_bps),
                    }
                    continue
                m = RE_CANCEL_GATE.match(line)
                if m:
                    ts_str, oid, prof, direction, bound, thresh = m.groups()
                    if not (oid.startswith("BBCBL") or oid.startswith("SBCBL")):
                        continue
                    cancels[oid] = {
                        "ts": parse_ts(ts_str),
                        "profitability_bps": float(prof),
                        "direction": direction,
                        "bound": bound,
                        "threshold_bps": float(thresh),
                    }
                    continue
                m = RE_FILL.match(line)
                if m:
                    ts_str, side, oid, fa, ta, px = m.groups()
                    if not (oid.startswith("BBCBL") or oid.startswith("SBCBL")):
                        continue
                    fills[oid] = {
                        "ts": parse_ts(ts_str),
                        "side": side,
                        "filled": float(fa),
                        "total": float(ta),
                        "price": float(px),
                    }
    return creates, cancels, fills


def load_csv_lead_index() -> Tuple[List[float], List[Optional[float]],
                                    List[Optional[float]], List[Optional[float]]]:
    """Returns (timestamps, lead_5s, lead_10s, lead_15s) — sorted by ts."""
    paths = sorted(glob.glob(CSV_GLOB))
    rows: List[Tuple[float, Optional[float], Optional[float], Optional[float]]] = []
    for p in paths:
        with open(p, newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                try:
                    ts = float(r["timestamp"])
                except (KeyError, ValueError, TypeError):
                    continue
                def _f(k):
                    v = r.get(k)
                    if v is None or v == "" or v == "None":
                        return None
                    try:
                        return float(v)
                    except ValueError:
                        return None
                rows.append((ts, _f("lead_5s"), _f("lead_10s"), _f("lead_15s")))
    rows.sort(key=lambda x: x[0])
    return ([r[0] for r in rows], [r[1] for r in rows],
            [r[2] for r in rows], [r[3] for r in rows])


def lookup_lead(ts: float, ts_list: List[float],
                lead_lists: List[List[Optional[float]]],
                tol_sec: float = 2.0) -> Tuple[Optional[float], ...]:
    if not ts_list:
        return (None,) * len(lead_lists)
    lo, hi = 0, len(ts_list) - 1
    res = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if ts_list[mid] <= ts:
            res = mid
            lo = mid + 1
        else:
            hi = mid - 1
    if res < 0:
        return (None,) * len(lead_lists)
    if abs(ts_list[res] - ts) > tol_sec:
        return (None,) * len(lead_lists)
    return tuple(lst[res] for lst in lead_lists)


def percentiles(xs: List[float], ps: List[int]) -> Dict[int, float]:
    if not xs:
        return {p: float("nan") for p in ps}
    s = sorted(xs)
    out = {}
    n = len(s)
    for p in ps:
        idx = max(0, min(n - 1, int(round(p / 100.0 * (n - 1)))))
        out[p] = s[idx]
    return out


def main():
    creates, cancels, fills = parse_logs()
    print(f"Parsed: {len(creates)} creates, {len(cancels)} cancels, {len(fills)} fills")

    ts_list, lead5, lead10, lead15 = load_csv_lead_index()
    lead_lists = [lead5, lead10, lead15]
    print(f"CSV lead index: {len(ts_list)} rows")
    print()

    # ---------------------------------------------------------------- #
    # Lifetime distribution by cancel reason                            #
    # ---------------------------------------------------------------- #
    lifetimes_by_bound: Dict[str, List[float]] = defaultdict(list)
    cancel_with_create = 0
    for oid, c in cancels.items():
        if oid in creates:
            life = c["ts"] - creates[oid]["ts"]
            if 0 < life < 600:  # sanity bound (10 min)
                lifetimes_by_bound[c["bound"]].append(life)
                cancel_with_create += 1

    print("=" * 80)
    print("LIFETIME DISTRIBUTION (seconds from create → cancel-decision)")
    print("=" * 80)
    print(f"  Matched pairs: {cancel_with_create}")
    for bound in ("min", "max"):
        xs = lifetimes_by_bound[bound]
        pct = percentiles(xs, [10, 25, 50, 75, 90, 95, 99])
        mean = sum(xs) / len(xs) if xs else float("nan")
        print(f"  cancel-by-{bound}: n={len(xs)} mean={mean:.2f}s "
              f"p10={pct[10]:.2f} p25={pct[25]:.2f} p50={pct[50]:.2f} "
              f"p75={pct[75]:.2f} p90={pct[90]:.2f} p99={pct[99]:.2f}")

    # ---------------------------------------------------------------- #
    # lead_bps at create vs at cancel/fill                              #
    # ---------------------------------------------------------------- #
    print()
    print("=" * 80)
    print("LEAD_BPS @ CREATE  vs  LEAD_BPS @ CANCEL (cross-tab on direction)")
    print("=" * 80)
    print("  Reads lead_10s from CSV (medium window) at the event timestamp.")
    print()

    # Bin by sign and bound
    create_lead_bins = {("BUY", "min"): [], ("BUY", "max"): [],
                        ("SELL", "min"): [], ("SELL", "max"): []}
    cancel_lead_bins = {("BUY", "min"): [], ("BUY", "max"): [],
                        ("SELL", "min"): [], ("SELL", "max"): []}
    for oid, c in cancels.items():
        if oid not in creates:
            continue
        side = creates[oid]["side"]
        bound = c["bound"]
        _, lead10_at_create, _ = lookup_lead(creates[oid]["ts"], ts_list, lead_lists)
        _, lead10_at_cancel, _ = lookup_lead(c["ts"], ts_list, lead_lists)
        if lead10_at_create is not None:
            create_lead_bins[(side, bound)].append(lead10_at_create)
        if lead10_at_cancel is not None:
            cancel_lead_bins[(side, bound)].append(lead10_at_cancel)

    def _stats(xs: List[float]) -> str:
        if not xs:
            return "n=0"
        mean = sum(xs) / len(xs)
        median = sorted(xs)[len(xs) // 2]
        pos = sum(1 for x in xs if x > 3)
        neg = sum(1 for x in xs if x < -3)
        return f"n={len(xs)} mean={mean:+.2f} median={median:+.2f} >+3bps={pos} <-3bps={neg}"

    for side in ("BUY", "SELL"):
        for bound in ("min", "max"):
            print(f"  cancel-{bound} on {side} maker:")
            print(f"    lead_10s @ create: {_stats(create_lead_bins[(side, bound)])}")
            print(f"    lead_10s @ cancel: {_stats(cancel_lead_bins[(side, bound)])}")

    # ---------------------------------------------------------------- #
    # Key cross-tab: were >max cancels happening when lead was FAVORABLE?
    # Favorable for BUY = lead_bps positive (leader up, local lagging up)
    # Favorable for SELL = lead_bps negative
    # ---------------------------------------------------------------- #
    print()
    print("=" * 80)
    print("KEY CROSS-TAB: cancel-by-max ON BUY when lead_bps @ cancel > +3")
    print("                  i.e., we abandoned a BUY about to fill profitably")
    print("=" * 80)
    n_buy_max_favorable = sum(1 for x in cancel_lead_bins[("BUY", "max")] if x > 3)
    n_buy_max_total = len(cancel_lead_bins[("BUY", "max")])
    n_sell_max_favorable = sum(1 for x in cancel_lead_bins[("SELL", "max")] if x < -3)
    n_sell_max_total = len(cancel_lead_bins[("SELL", "max")])
    def pct(a, b):
        return (a / b * 100) if b else 0.0
    print(f"  BUY  >max cancels with lead_bps @ cancel > +3 bps: "
          f"{n_buy_max_favorable}/{n_buy_max_total} ({pct(n_buy_max_favorable, n_buy_max_total):.1f}%)")
    print(f"  SELL >max cancels with lead_bps @ cancel < -3 bps: "
          f"{n_sell_max_favorable}/{n_sell_max_total} ({pct(n_sell_max_favorable, n_sell_max_total):.1f}%)")

    # ---------------------------------------------------------------- #
    # Fills: lead_bps @ fill                                            #
    # ---------------------------------------------------------------- #
    print()
    print("=" * 80)
    print("FILLS: lead_bps at moment of fill (BitPreco maker fills only)")
    print("=" * 80)
    fill_leads_by_side: Dict[str, List[float]] = defaultdict(list)
    for oid, f in fills.items():
        _, lead10_at_fill, _ = lookup_lead(f["ts"], ts_list, lead_lists)
        if lead10_at_fill is not None:
            fill_leads_by_side[f["side"]].append(lead10_at_fill)
    for side in ("BUY", "SELL"):
        print(f"  {side}: {_stats(fill_leads_by_side[side])}")

    # ---------------------------------------------------------------- #
    # Fill rate
    # ---------------------------------------------------------------- #
    print()
    print("=" * 80)
    print("FILL RATE")
    print("=" * 80)
    n_created = len(creates)
    n_filled = len(fills)
    print(f"  Created: {n_created}")
    print(f"  Filled : {n_filled}  ({pct(n_filled, n_created):.2f}%)")
    n_cancel_only = sum(1 for oid in creates if oid in cancels and oid not in fills)
    print(f"  Cancelled (no fill): {n_cancel_only}")

    # Lifetime distribution of FILLED orders (when fill happened on still-live)
    filled_lifetimes = []
    for oid, f in fills.items():
        if oid in creates:
            life = f["ts"] - creates[oid]["ts"]
            if 0 < life < 600:
                filled_lifetimes.append(life)
    if filled_lifetimes:
        pct_ = percentiles(filled_lifetimes, [10, 25, 50, 75, 90, 99])
        mean = sum(filled_lifetimes) / len(filled_lifetimes)
        print(f"  Lifetime of FILLED orders: n={len(filled_lifetimes)} mean={mean:.2f}s "
              f"p10={pct_[10]:.2f} p25={pct_[25]:.2f} p50={pct_[50]:.2f} "
              f"p75={pct_[75]:.2f} p90={pct_[90]:.2f} p99={pct_[99]:.2f}")


if __name__ == "__main__":
    main()
