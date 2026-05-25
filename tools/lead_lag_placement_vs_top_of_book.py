"""Placement vs top-of-book analysis.

Question: how far from BitPreco's best_bid/best_ask are our maker orders
placed? If we're consistently *behind* the queue, that explains the 1,8%
fill rate independent of lead-lag signal quality.

For each maker create event:
  - BUY:  rel_bps = 10000 * (our_price - best_bid) / best_bid
          > 0  → improving the bid (new top-of-book bidder)
          = 0  → at-the-money with best_bid (behind whoever was first)
          < 0  → below best_bid (passive, deep in queue)
  - SELL: rel_bps = 10000 * (best_ask - our_price) / our_price
          > 0  → improving the ask
          = 0  → at-the-money
          < 0  → above best_ask (passive, deep)

Also reports: where in the spread are we sitting (0% = at best_bid for BUY,
100% = at best_ask), and per-mode (`improve` vs `clip_max`) breakdown.

Cross-tab with the fill outcome: does placement aggressiveness correlate
with fill rate?

Run: python tools/lead_lag_placement_vs_top_of_book.py
"""
from __future__ import annotations

import csv
import glob
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple

LOG_GLOB = "/home/ubuntu/hummingbot-ney/logs/logs_conf_xemm_lead_lag_sbe*.log"
CSV_GLOB = (
    "/home/ubuntu/hummingbot-ney/logs/xemm_lead_lag/"
    "xemm_lead_lag_xemm_lead_lag_btcbrl_v1_2026051[56]_*.csv"
)

# Captures: ts, oid, side, price, mode, lead_mode, lead_bps
RE_CREATE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?Created maker order \(LIMIT_MAKER\) "
    r"(\S+) side=(BUY|SELL) price=([\d.]+) mode=(\S+) lead=(\w+) lead_bps=(-?[\d.]+) "
)
RE_FILL = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?The (BUY|SELL) order (\S+) "
    r"amounting to ([\d.]+)/([\d.]+).*?filled at ([\d.]+)"
)


def parse_ts(s: str) -> float:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f").timestamp()


def parse_creates_and_fills():
    creates = {}
    fills = {}
    for path in sorted(glob.glob(LOG_GLOB)):
        with open(path, errors="ignore") as f:
            for line in f:
                m = RE_CREATE.match(line)
                if m:
                    ts_str, oid, side, price, mode, lead_mode, lead_bps = m.groups()
                    if not (oid.startswith("BBCBL") or oid.startswith("SBCBL")):
                        continue
                    creates[oid] = {
                        "ts": parse_ts(ts_str),
                        "side": side,
                        "price": float(price),
                        "mode": mode,
                        "lead_mode": lead_mode,
                        "lead_bps": float(lead_bps),
                    }
                    continue
                m = RE_FILL.match(line)
                if m:
                    ts_str, side, oid, fa, ta, px = m.groups()
                    if oid.startswith("BBCBL") or oid.startswith("SBCBL"):
                        fills[oid] = True
    return creates, fills


def load_csv_book():
    """Returns (ts_list, bid_list, ask_list)."""
    rows = []
    for p in sorted(glob.glob(CSV_GLOB)):
        with open(p, newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                try:
                    ts = float(r["timestamp"])
                    bid = float(r["local_bid"])
                    ask = float(r["local_ask"])
                except (KeyError, ValueError, TypeError):
                    continue
                if bid <= 0 or ask <= 0 or ask < bid:
                    continue
                rows.append((ts, bid, ask))
    rows.sort(key=lambda x: x[0])
    return [r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows]


def lookup_book(ts, ts_list, bids, asks, tol_sec=2.0):
    if not ts_list:
        return None, None
    lo, hi = 0, len(ts_list) - 1
    res = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if ts_list[mid] <= ts:
            res = mid
            lo = mid + 1
        else:
            hi = mid - 1
    if res < 0 or abs(ts_list[res] - ts) > tol_sec:
        return None, None
    return bids[res], asks[res]


def percentiles(xs, ps):
    if not xs:
        return {p: float("nan") for p in ps}
    s = sorted(xs)
    n = len(s)
    return {p: s[max(0, min(n - 1, int(round(p / 100.0 * (n - 1)))))] for p in ps}


def stats_block(xs, label):
    if not xs:
        print(f"  {label}: n=0")
        return
    pct = percentiles(xs, [5, 10, 25, 50, 75, 90, 95])
    mean = sum(xs) / len(xs)
    print(f"  {label}: n={len(xs)} mean={mean:+.2f} "
          f"p5={pct[5]:+.2f} p10={pct[10]:+.2f} p25={pct[25]:+.2f} "
          f"p50={pct[50]:+.2f} p75={pct[75]:+.2f} p90={pct[90]:+.2f} p95={pct[95]:+.2f}")


def main():
    creates, fills = parse_creates_and_fills()
    print(f"Parsed: {len(creates)} creates, {len(fills)} filled order_ids")
    ts_list, bids, asks = load_csv_book()
    print(f"CSV book index: {len(ts_list)} rows")
    print()

    # Compute rel_bps for each create
    rel_by_side: Dict[str, List[float]] = {"BUY": [], "SELL": []}
    rel_by_side_mode: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    rel_by_outcome: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    spread_pct_by_side: Dict[str, List[float]] = {"BUY": [], "SELL": []}
    n_inside = {"BUY": 0, "SELL": 0}
    n_at = {"BUY": 0, "SELL": 0}
    n_behind = {"BUY": 0, "SELL": 0}
    n_cross = {"BUY": 0, "SELL": 0}
    n_total = {"BUY": 0, "SELL": 0}
    missing_book = 0

    for oid, c in creates.items():
        bid, ask = lookup_book(c["ts"], ts_list, bids, asks)
        if bid is None or ask is None:
            missing_book += 1
            continue
        side = c["side"]
        price = c["price"]
        if side == "BUY":
            rel_bps = 10000.0 * (price - bid) / bid
            # spread position: 0% = at bid, 100% = at ask
            spread = ask - bid
            spread_pos_pct = (price - bid) / spread * 100 if spread > 0 else float("nan")
            if price > ask:
                n_cross[side] += 1
            elif price > bid:
                n_inside[side] += 1
            elif price == bid:
                n_at[side] += 1
            else:
                n_behind[side] += 1
        else:  # SELL
            rel_bps = 10000.0 * (ask - price) / price
            spread = ask - bid
            spread_pos_pct = (ask - price) / spread * 100 if spread > 0 else float("nan")
            if price < bid:
                n_cross[side] += 1
            elif price < ask:
                n_inside[side] += 1
            elif price == ask:
                n_at[side] += 1
            else:
                n_behind[side] += 1
        n_total[side] += 1
        rel_by_side[side].append(rel_bps)
        rel_by_side_mode[(side, c["mode"])].append(rel_bps)
        outcome = "FILLED" if oid in fills else "CANCELLED"
        rel_by_outcome[(side, outcome)].append(rel_bps)
        if not (spread_pos_pct != spread_pos_pct):  # not NaN
            spread_pct_by_side[side].append(spread_pos_pct)

    print(f"Missing book snapshots (skipped): {missing_book}")
    print()

    # ----------------------------------------------------------------- #
    # Where are we placing? (relative to top of OUR side)               #
    # rel_bps > 0 = improving the book (new top-of-book on our side)
    # rel_bps = 0 = at-the-money (behind existing queue at same price)
    # rel_bps < 0 = passive (below best_bid for BUY / above best_ask for SELL)
    # ----------------------------------------------------------------- #
    print("=" * 80)
    print("PLACEMENT BPS FROM TOP-OF-OUR-SIDE")
    print("  BUY: rel_bps = 10000 * (our_price - best_bid) / best_bid")
    print("    > 0 = improving the bid (becomes new top-of-book)")
    print("    = 0 = at-the-money with current bid (behind existing queue)")
    print("    < 0 = below current bid (passive, deep in queue)")
    print("  SELL: rel_bps = 10000 * (best_ask - our_price) / our_price")
    print("=" * 80)
    for side in ("BUY", "SELL"):
        stats_block(rel_by_side[side], f"{side} all creates")

    print()
    print("=" * 80)
    print("DISCRETE BREAKDOWN: where on the book are we?")
    print("=" * 80)
    for side in ("BUY", "SELL"):
        tot = n_total[side]
        if tot == 0:
            continue
        def p(x):
            return x / tot * 100
        print(f"  {side}: total={tot}")
        print(f"    crossed/aggressive (would have crossed): {n_cross[side]} ({p(n_cross[side]):.1f}%)")
        print(f"    improving top (rel_bps > 0):              {n_inside[side]} ({p(n_inside[side]):.1f}%)")
        print(f"    at-the-money (rel_bps == 0):              {n_at[side]} ({p(n_at[side]):.1f}%)")
        print(f"    behind top (rel_bps < 0):                 {n_behind[side]} ({p(n_behind[side]):.1f}%)")

    print()
    print("=" * 80)
    print("BY PLACEMENT MODE (improve vs clip_max vs fallback)")
    print("=" * 80)
    for side in ("BUY", "SELL"):
        for (s, mode), xs in sorted(rel_by_side_mode.items()):
            if s != side:
                continue
            stats_block(xs, f"{side} mode={mode:<12}")

    print()
    print("=" * 80)
    print("BY OUTCOME (filled vs cancelled)")
    print("=" * 80)
    for side in ("BUY", "SELL"):
        for outcome in ("FILLED", "CANCELLED"):
            xs = rel_by_outcome[(side, outcome)]
            stats_block(xs, f"{side} {outcome:<10}")

    print()
    print("=" * 80)
    print("SPREAD POSITION (0% = at best_bid for BUY / 100% = at best_ask for BUY)")
    print("  (For BUY: 0% means we're at the bid, 100% means we improved to the ask.")
    print("   For SELL: 0% means we're at the ask, 100% means we improved to the bid.)")
    print("=" * 80)
    for side in ("BUY", "SELL"):
        stats_block(spread_pct_by_side[side], f"{side} spread_pos%")

    # ----------------------------------------------------------------- #
    # Fill rate by placement bucket                                     #
    # ----------------------------------------------------------------- #
    print()
    print("=" * 80)
    print("FILL RATE BY PLACEMENT-AGGRESSIVENESS BUCKET")
    print("=" * 80)
    # Buckets in bps: (-inf, -1), [-1, 0), [0, 0.5), [0.5, 1), [1, 2), [2, inf)
    bucket_defs = [
        ("< -1 bps (deep)", lambda x: x < -1),
        ("[-1, 0)",         lambda x: -1 <= x < 0),
        ("[0, 0.5)",        lambda x: 0 <= x < 0.5),
        ("[0.5, 1)",        lambda x: 0.5 <= x < 1),
        ("[1, 2)",          lambda x: 1 <= x < 2),
        (">= 2 bps (very aggressive)", lambda x: x >= 2),
    ]
    for side in ("BUY", "SELL"):
        print(f"  {side}:")
        for label, fn in bucket_defs:
            tot_in_bucket = 0
            fill_in_bucket = 0
            for oid, c in creates.items():
                if c["side"] != side:
                    continue
                bid, ask = lookup_book(c["ts"], ts_list, bids, asks)
                if bid is None:
                    continue
                if side == "BUY":
                    rel = 10000.0 * (c["price"] - bid) / bid
                else:
                    rel = 10000.0 * (ask - c["price"]) / c["price"]
                if fn(rel):
                    tot_in_bucket += 1
                    if oid in fills:
                        fill_in_bucket += 1
            pct = (fill_in_bucket / tot_in_bucket * 100) if tot_in_bucket else 0.0
            print(f"    {label:<32} total={tot_in_bucket:>5} filled={fill_in_bucket:>4} ({pct:.2f}%)")


if __name__ == "__main__":
    main()
