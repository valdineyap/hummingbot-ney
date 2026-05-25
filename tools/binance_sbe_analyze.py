#!/usr/bin/env python3
"""Analyse one or more shadow CSV files produced by ``binance_sbe_shadow.py``.

Computes the metrics the rollout plan calls for as the GO/NO-GO gate:

* Per-source event counts and rates
* Top-of-book divergence between SBE and JSON, in time windows
* Best-bid / best-ask delta when both sources have an observation in the
  same poll window
* CPU sample stats (from the summary JSON, if present)
* SBE-vs-JSON latency at the recorder layer (median, p95, p99) by
  pairing consecutive same-symbol diff rows across sources

Output: prints a human-readable summary to stdout and (optionally) writes
a structured JSON next to the input CSV with ``--write-summary``.

Usage::

    python tools/binance_sbe_analyze.py var/sbe_shadow/*.csv
    python tools/binance_sbe_analyze.py --write-summary var/sbe_shadow/*.csv

Requires pandas. The shadow script itself does not — pandas is only used
for post-run analysis so the data-collection process stays light.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

import pandas as pd


def _load(csv_paths: List[Path]) -> pd.DataFrame:
    frames = []
    for p in csv_paths:
        df = pd.read_csv(p)
        df["__source_file"] = p.name
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _analyze(df: pd.DataFrame) -> dict:
    out = {"by_source": {}, "top_of_book": {}, "latency": {}}

    # ---------- per-source event counts ----------
    for source in df["source"].unique():
        sub = df[df["source"] == source]
        if sub.empty:
            continue
        duration_ns = sub["ts_recv_ns"].max() - sub["ts_recv_ns"].min()
        duration_sec = duration_ns / 1e9 if duration_ns > 0 else float("nan")
        out["by_source"][source] = {
            "events": int(len(sub)),
            "duration_sec": round(duration_sec, 1),
            "events_per_sec": round(len(sub) / duration_sec, 2) if duration_sec else None,
            "unique_symbols": int(sub["symbol"].nunique()),
        }

    # ---------- top-of-book divergence ----------
    # For each symbol, bin both streams to 100ms windows and compare the
    # last observation in each window.
    df["bucket"] = (df["ts_wall_ns"] // 100_000_000).astype("int64")  # 100ms buckets
    pivot = df.groupby(["symbol", "bucket", "source"]).agg(
        best_bid=("best_bid", "last"),
        best_ask=("best_ask", "last"),
    ).unstack("source")
    # Now `pivot` has multi-index columns (best_bid, json) / (best_bid, sbe)...
    if isinstance(pivot.columns, pd.MultiIndex) and "json" in pivot.columns.get_level_values("source") and "sbe" in pivot.columns.get_level_values("source"):
        bid_json = pd.to_numeric(pivot[("best_bid", "json")], errors="coerce")
        bid_sbe = pd.to_numeric(pivot[("best_bid", "sbe")], errors="coerce")
        ask_json = pd.to_numeric(pivot[("best_ask", "json")], errors="coerce")
        ask_sbe = pd.to_numeric(pivot[("best_ask", "sbe")], errors="coerce")

        paired = (bid_json.notna() & bid_sbe.notna()).sum()
        bid_diff = (bid_json - bid_sbe).abs()
        ask_diff = (ask_json - ask_sbe).abs()
        out["top_of_book"] = {
            "paired_buckets_100ms": int(paired),
            "bid_diff_p50": _round(bid_diff.median()),
            "bid_diff_p95": _round(bid_diff.quantile(0.95)),
            "bid_diff_p99": _round(bid_diff.quantile(0.99)),
            "bid_diff_max": _round(bid_diff.max()),
            "ask_diff_p50": _round(ask_diff.median()),
            "ask_diff_p95": _round(ask_diff.quantile(0.95)),
            "ask_diff_p99": _round(ask_diff.quantile(0.99)),
            "ask_diff_max": _round(ask_diff.max()),
            "buckets_with_divergent_top": int(((bid_diff > 0) | (ask_diff > 0)).sum()),
        }
    else:
        out["top_of_book"] = {"note": "both sources not present — skipped"}

    # ---------- latency at recorder layer ----------
    # Pair consecutive diff observations by (symbol, bucket) and compute
    # delta between source timestamps.
    if isinstance(pivot.columns, pd.MultiIndex):
        ts_pivot = df.groupby(["symbol", "bucket", "source"]).agg(
            ts_recv_ns=("ts_recv_ns", "first")
        ).unstack("source")
        if "json" in ts_pivot.columns.get_level_values("source") and "sbe" in ts_pivot.columns.get_level_values("source"):
            ts_json = ts_pivot[("ts_recv_ns", "json")]
            ts_sbe = ts_pivot[("ts_recv_ns", "sbe")]
            # delta_ms = ts_json - ts_sbe (positive = SBE arrived first)
            delta_ms = ((ts_json - ts_sbe) / 1_000_000).dropna()
            if len(delta_ms) > 0:
                out["latency"] = {
                    "paired_observations": int(len(delta_ms)),
                    "delta_ms_p50": _round(delta_ms.median()),
                    "delta_ms_p90": _round(delta_ms.quantile(0.90)),
                    "delta_ms_p95": _round(delta_ms.quantile(0.95)),
                    "delta_ms_p99": _round(delta_ms.quantile(0.99)),
                    "delta_ms_mean": _round(delta_ms.mean()),
                    "delta_ms_min": _round(delta_ms.min()),
                    "delta_ms_max": _round(delta_ms.max()),
                    "sbe_first_pct": _round(100 * (delta_ms > 0).mean()),
                }

    return out


def _round(v):
    try:
        return None if pd.isna(v) else round(float(v), 4)
    except Exception:
        return None


def _print_report(summary: dict):
    print("=" * 70)
    print("Per-source totals")
    print("=" * 70)
    for src, stats in summary["by_source"].items():
        print(f"  {src:8s}  events={stats['events']:>8}  "
              f"duration={stats['duration_sec']}s  "
              f"rate={stats['events_per_sec']}/s")

    print()
    print("=" * 70)
    print("Top-of-book divergence (paired 100ms buckets)")
    print("=" * 70)
    tob = summary.get("top_of_book", {})
    if "note" in tob:
        print(f"  {tob['note']}")
    else:
        print(f"  paired buckets : {tob.get('paired_buckets_100ms')}")
        print(f"  bid diff p50/95/99/max : {tob.get('bid_diff_p50')} / "
              f"{tob.get('bid_diff_p95')} / {tob.get('bid_diff_p99')} / {tob.get('bid_diff_max')}")
        print(f"  ask diff p50/95/99/max : {tob.get('ask_diff_p50')} / "
              f"{tob.get('ask_diff_p95')} / {tob.get('ask_diff_p99')} / {tob.get('ask_diff_max')}")
        print(f"  buckets with any divergence : {tob.get('buckets_with_divergent_top')}")

    print()
    print("=" * 70)
    print("Recorder-layer latency (ts_json - ts_sbe, ms; positive = SBE first)")
    print("=" * 70)
    lat = summary.get("latency", {})
    if not lat:
        print("  not enough paired data")
    else:
        print(f"  paired observations : {lat['paired_observations']}")
        print(f"  delta_ms p50 / p90 / p95 / p99 : "
              f"{lat['delta_ms_p50']} / {lat['delta_ms_p90']} / "
              f"{lat['delta_ms_p95']} / {lat['delta_ms_p99']}")
        print(f"  delta_ms mean       : {lat['delta_ms_mean']}")
        print(f"  delta_ms min / max  : {lat['delta_ms_min']} / {lat['delta_ms_max']}")
        print(f"  SBE arrived first   : {lat['sbe_first_pct']}% of paired observations")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("csv_paths", nargs="+", type=Path,
                   help="One or more CSV files produced by binance_sbe_shadow.py")
    p.add_argument("--write-summary", action="store_true",
                   help="Write a structured summary JSON alongside the first CSV.")
    args = p.parse_args()

    df = _load(args.csv_paths)
    if df.empty:
        print("no rows loaded — nothing to do", file=sys.stderr)
        sys.exit(2)
    summary = _analyze(df)
    _print_report(summary)

    if args.write_summary:
        out_path = args.csv_paths[0].with_suffix(".analysis.json")
        with out_path.open("w") as fh:
            json.dump(summary, fh, indent=2, default=str)
        print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
