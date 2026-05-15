#!/usr/bin/env python3
"""Phase 0 / step 3 — capture ``orderbook:<market>`` snapshots.

Passive subscriber. Runs in parallel with the legacy bot, just like
``02_capture_update.py``.

Goals
=====

1. Confirm BitPreco's Redis publishes orderbook snapshots for the
   markets we care about.
2. Discover the payload schema. ``bitbots-js`` assumes the same shape
   as the REST ``/orderbook`` response, but we need to verify before
   reusing the parser in production.
3. Measure publish frequency, snapshot size, and field consistency.
4. Identify a usable timestamp field (book-event time vs publish time).

REST comparison
---------------

After the subscribe loop ends, we fetch the REST ``/orderbook``
endpoint once and write both responses side-by-side under
``schema_comparison`` in the report. This makes the "can we reuse
``snapshot_message_from_exchange_rest``?" decision explicit.

Run::

    conda activate hummingbot
    python tools/redis_probe/03_capture_orderbook.py \\
        --duration 900 --market BTC-BRL
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    RedisConfig,
    SchemaDiscovery,
    connect_pubsub,
    deadline,
    open_capture,
    percentile,
    scan_timestamps,
    setup_logging,
    time_left,
)


log = setup_logging("redis_probe.03_capture_orderbook")
SCRIPT = "03_capture_orderbook"


class OrderBookCapture:
    def __init__(self) -> None:
        self.schema = SchemaDiscovery()
        self.message_count: int = 0
        self.first_recv_ts: Optional[float] = None
        self.last_recv_ts: Optional[float] = None
        self.inter_event_ms: list[float] = []
        self.timestamp_field_hits: Dict[str, list[Any]] = {}
        self.byte_sizes: list[int] = []
        self.bid_depth_samples: list[int] = []
        self.ask_depth_samples: list[int] = []
        self._last_recv: Optional[float] = None

    def observe(self, payload: Any, recv_ts: float, raw_size: int) -> None:
        self.message_count += 1
        if self.first_recv_ts is None:
            self.first_recv_ts = recv_ts
        if self._last_recv is not None:
            self.inter_event_ms.append((recv_ts - self._last_recv) * 1000)
        self._last_recv = recv_ts
        self.last_recv_ts = recv_ts
        self.byte_sizes.append(raw_size)

        try:
            self.schema.observe(payload)
        except Exception:
            log.exception("schema observe failed")

        for path, unit, value in scan_timestamps(payload):
            self.timestamp_field_hits.setdefault(path, []).append((unit, value))

        # Depth heuristics — bitbots-js shape: bids/asks at top level
        for side, bucket in (("bids", self.bid_depth_samples),
                             ("asks", self.ask_depth_samples)):
            v = payload.get(side) if isinstance(payload, dict) else None
            if isinstance(v, list):
                bucket.append(len(v))

    def to_report(self, rest_sample: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        ts_summary: Dict[str, Any] = {}
        for path, hits in self.timestamp_field_hits.items():
            units = {u for u, _ in hits}
            ts_summary[path] = {
                "hits": len(hits),
                "units_detected": sorted(units),
                "samples": [v for _, v in hits[:3]],
            }

        # Build a quick schema comparison vs REST if we have one
        comparison: Dict[str, Any] = {}
        if rest_sample is not None:
            rest_schema = SchemaDiscovery()
            rest_schema.observe(rest_sample)
            redis_keys = set(self.schema.report()["fields"].keys())
            rest_keys = set(rest_schema.report()["fields"].keys())
            comparison = {
                "redis_only_fields": sorted(redis_keys - rest_keys),
                "rest_only_fields": sorted(rest_keys - redis_keys),
                "shared_fields": sorted(redis_keys & rest_keys),
                "rest_schema": rest_schema.report(),
                "rest_sample_keys": sorted(rest_sample.keys())
                if isinstance(rest_sample, dict) else None,
            }

        return {
            "script": SCRIPT,
            "message_count": self.message_count,
            "elapsed_s": round(
                (self.last_recv_ts or 0) - (self.first_recv_ts or 0), 3),
            "byte_size": {
                "p50": percentile(self.byte_sizes, 50),
                "p99": percentile(self.byte_sizes, 99),
                "max": max(self.byte_sizes) if self.byte_sizes else None,
            },
            "inter_event_latency_ms": {
                "count": len(self.inter_event_ms),
                "p50": percentile(self.inter_event_ms, 50),
                "p99": percentile(self.inter_event_ms, 99),
                "max": max(self.inter_event_ms) if self.inter_event_ms else None,
            },
            "bid_depth": {
                "p50": percentile(self.bid_depth_samples, 50),
                "max": max(self.bid_depth_samples) if self.bid_depth_samples else None,
            },
            "ask_depth": {
                "p50": percentile(self.ask_depth_samples, 50),
                "max": max(self.ask_depth_samples) if self.ask_depth_samples else None,
            },
            "timestamp_candidates": ts_summary,
            "schema": self.schema.report(),
            "schema_comparison_vs_rest": comparison,
        }


def _fetch_rest_orderbook(market: str, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
    """Best-effort REST fetch. BitPreco public; URL pattern from
    bitpreco_constants.ORDER_BOOK_PATH_URL.

    `market` like ``BTC-BRL`` -> ``btc-brl`` in URL.
    """
    url = f"https://api.bitpreco.com/{market.lower()}/orderbook"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "redis_probe/0.1"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
        log.warning("REST orderbook fetch failed: %s", e)
        return None
    except Exception:
        log.exception("REST orderbook fetch crashed")
        return None


async def run(cfg: RedisConfig, market: str, duration_s: float) -> int:
    channel = f"orderbook:{market}"
    capture_path, writer = open_capture(SCRIPT)
    print(f"Capture file: {capture_path}")
    print(f"Subscribing to {channel!r} for {duration_s:.0f}s "
          "(Ctrl-C to stop early)")

    state = OrderBookCapture()
    end_at = deadline(duration_s)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    rest_sample: Optional[Dict[str, Any]] = None

    try:
        async with connect_pubsub(cfg) as (client, pubsub):  # noqa: F841
            await pubsub.subscribe(channel)
            print("Subscribed.")
            while time_left(end_at) > 0 and not stop_event.is_set():
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=min(1.0, time_left(end_at)),
                )
                if msg is None:
                    continue
                recv_ts = time.time()
                data = msg.get("data")
                raw_size = len(data) if isinstance(data, (bytes, str)) else 0
                payload: Any = data
                if isinstance(data, (bytes, str)):
                    try:
                        payload = json.loads(data)
                    except Exception:
                        log.warning("payload not JSON: %r", data[:200])
                writer.write({
                    "recv_ts": recv_ts,
                    "channel": msg.get("channel"),
                    "payload": payload,
                })
                state.observe(payload, recv_ts, raw_size)
                if state.message_count % 50 == 0:
                    print(f"  [{state.message_count} snapshots] "
                          f"size_p50={percentile(state.byte_sizes, 50):.0f}B "
                          f"inter_p50={percentile(state.inter_event_ms, 50):.0f}ms")
    finally:
        writer.close()
        # One REST sample for schema comparison
        rest_sample = _fetch_rest_orderbook(market)
        report = state.to_report(rest_sample)
        capture_path.with_suffix(".report.json").write_text(
            json.dumps(report, indent=2, default=str))
        print(f"Captured {state.message_count} snapshots. Report: "
              f"{capture_path.with_suffix('.report.json')}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--duration", type=float, default=900.0,
                    help="capture window in seconds (default 900 = 15 min)")
    ap.add_argument("--market", type=str, default="BTC-BRL",
                    help="market pair, e.g. BTC-BRL (default)")
    args = ap.parse_args()

    try:
        cfg = RedisConfig.from_env()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    try:
        return asyncio.run(run(cfg, args.market, args.duration))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
