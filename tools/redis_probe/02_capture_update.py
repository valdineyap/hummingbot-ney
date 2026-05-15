#!/usr/bin/env python3
"""Phase 0 / step 2 — capture ``update:<idBPBot>`` for analysis.

Passive subscriber. Runs **in parallel with the legacy bot in
production** so we observe the exact events that would drive the bot
if it were already on the Redis backend.

Output
======

Two files under ``tools/redis_probe/captures/``:

- ``02_capture_update-<YYYYMMDD-HHMM>.jsonl`` — one JSON object per
  message: ``{"recv_ts": float, "channel": str, "payload": <raw>}``.
  This is sensitive (real ``order_id``, ``user_id``, balances). 0600
  perms, in ``.gitignore``.

- ``02_capture_update-<YYYYMMDD-HHMM>.report.json`` — derived stats
  (schema discovery, timestamp candidates, ``message_cod`` counts,
  monotonicity checks). Safe to share if values are scrubbed.

Run::

    conda activate hummingbot
    python tools/redis_probe/02_capture_update.py --duration 7200

Stop early with Ctrl-C — the report is written on exit either way.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

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


log = setup_logging("redis_probe.02_capture_update")
SCRIPT = "02_capture_update"


class UpdateCapture:
    def __init__(self) -> None:
        self.schema = SchemaDiscovery()
        self.message_cod_counts: Dict[str, int] = defaultdict(int)
        self.payloads_per_order: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.timestamp_field_hits: Dict[str, List[Any]] = defaultdict(list)
        self.inter_event_ms_per_order: Dict[str, List[float]] = defaultdict(list)
        self.first_recv_ts: Optional[float] = None
        self.last_recv_ts: Optional[float] = None
        self.message_count: int = 0
        # For monotonicity check
        self.last_ts_per_order_per_field: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.regressions_per_field: Dict[str, int] = defaultdict(int)

    def observe(self, payload: Any, recv_ts: float) -> None:
        self.message_count += 1
        if self.first_recv_ts is None:
            self.first_recv_ts = recv_ts
        self.last_recv_ts = recv_ts

        # Schema discovery
        try:
            self.schema.observe(payload)
        except Exception:
            log.exception("schema observe failed")

        # message_cod count (path varies; bitbots-js has it at top-level)
        cod = _dig(payload, "message_cod") or _dig(payload, "type") or "<unknown>"
        self.message_cod_counts[str(cod)] += 1

        # Identify the per-order key. Try common shapes:
        # bitbots-js: payload.order.id (top of `update:` payload)
        order_id = (
            _dig(payload, "order", "id")
            or _dig(payload, "id")
            or _dig(payload, "order_id")
        )
        if order_id is not None:
            order_key = str(order_id)
            prev = self.payloads_per_order[order_key]
            if prev:
                last_ts = prev[-1]["recv_ts"]
                self.inter_event_ms_per_order[order_key].append((recv_ts - last_ts) * 1000)
            prev.append({"recv_ts": recv_ts, "cod": cod})

        # Timestamp candidates + monotonicity
        for field_path, unit, value in scan_timestamps(payload):
            self.timestamp_field_hits[field_path].append((unit, value))
            if order_id is None:
                continue
            okey = str(order_id)
            seen = self.last_ts_per_order_per_field[field_path]
            prev = seen.get(okey)
            if prev is not None:
                try:
                    if float(value) < float(prev):
                        self.regressions_per_field[field_path] += 1
                except (TypeError, ValueError):
                    pass
            seen[okey] = value

    def to_report(self) -> Dict[str, Any]:
        elapsed = (self.last_recv_ts or 0) - (self.first_recv_ts or 0)
        ts_summary: Dict[str, Any] = {}
        for path, hits in self.timestamp_field_hits.items():
            units = {u for u, _ in hits}
            ts_summary[path] = {
                "hits": len(hits),
                "units_detected": sorted(units),
                "regressions_per_order": self.regressions_per_field.get(path, 0),
                "samples": [v for _, v in hits[:3]],
            }

        # Per-order inter-event latency
        all_inter = [x for xs in self.inter_event_ms_per_order.values() for x in xs]

        return {
            "script": SCRIPT,
            "message_count": self.message_count,
            "elapsed_s": round(elapsed, 3),
            "first_recv_ts": self.first_recv_ts,
            "last_recv_ts": self.last_recv_ts,
            "message_cod_counts": dict(self.message_cod_counts),
            "unique_orders_seen": len(self.payloads_per_order),
            "inter_event_latency_ms": {
                "count": len(all_inter),
                "p50": percentile(all_inter, 50),
                "p99": percentile(all_inter, 99),
                "max": max(all_inter) if all_inter else None,
            },
            "timestamp_candidates": ts_summary,
            "schema": self.schema.report(),
        }


def _dig(obj: Any, *keys: str) -> Any:
    cur = obj
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur


async def run(cfg: RedisConfig, duration_s: float, channel_override: Optional[str]) -> int:
    if channel_override:
        channel = channel_override
    elif cfg.user_id:
        channel = f"update:{cfg.user_id}"
    else:
        print("BITPRECO_USER_ID not set. Run 04_get_user_id.py first or "
              "pass --channel.", file=sys.stderr)
        return 2

    capture_path, writer = open_capture(SCRIPT)
    print(f"Capture file: {capture_path}")
    print(f"Subscribing to {channel!r} for {duration_s:.0f}s "
          "(Ctrl-C to stop early)")

    state = UpdateCapture()
    end_at = deadline(duration_s)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        async with connect_pubsub(cfg) as (client, pubsub):  # noqa: F841
            await pubsub.subscribe(channel)
            print("Subscribed. ack messages will appear below.")
            while time_left(end_at) > 0 and not stop_event.is_set():
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=min(1.0, time_left(end_at)),
                )
                if msg is None:
                    continue
                recv_ts = time.time()
                data = msg.get("data")
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
                state.observe(payload, recv_ts)
                if state.message_count % 25 == 0:
                    print(f"  [{state.message_count} msgs] "
                          f"cods={dict(state.message_cod_counts)}")
    finally:
        writer.close()
        _write_report(capture_path, state)
        print(f"Captured {state.message_count} messages. Report: "
              f"{capture_path.with_suffix('.report.json')}")
    return 0


def _write_report(capture_path: Path, state: UpdateCapture) -> None:
    report_path = capture_path.with_suffix(".report.json")
    report = state.to_report()
    report_path.write_text(json.dumps(report, indent=2, default=str))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--duration", type=float, default=3600.0,
                    help="capture window in seconds (default 3600 = 1 hour)")
    ap.add_argument("--channel", type=str, default=None,
                    help="override channel (default: update:<BITPRECO_USER_ID>)")
    args = ap.parse_args()

    try:
        cfg = RedisConfig.from_env()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    try:
        return asyncio.run(run(cfg, args.duration, args.channel))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
