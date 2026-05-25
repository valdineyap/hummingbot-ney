#!/usr/bin/env python3
"""Phase 0 / step 5 — measure Redis vs REST coverage and latency.

The decisive script for Phase 0. Runs in parallel with the legacy bot
in production. Two concurrent tasks:

1. **Redis listener** — subscribes to ``update:<idBPBot>``, indexes
   every event by ``order_id`` and ``message_cod`` with its recv
   timestamp.
2. **REST poller** — every ~3s, fetches ``executed_orders`` and
   ``open_orders``; tracks the lifecycle of every order the bot
   touches.

Reconciliation produces three metrics:

- **Fill match rate**: of every fill seen in REST ``executed_orders``,
  how many had a Redis ``ORDER_FULLY_EXECUTED`` /
  ``ORDER_PARTIALLY_EXECUTED`` event within ±30s?
- **Cancel match rate**: of every cancel observed via REST (order
  disappears from ``open_orders``, no fill recorded), how many had a
  Redis ``ORDER_CANCELED`` event within ±30s?
- **Redis vs REST latency**: for each matched fill, the recv_ts delta
  (Redis - REST). Positive means Redis arrived later; negative means
  earlier.

Exit-criterion target (per plan): match rate ≥ 99% on both. Anything
below means Redis suffers the same observability problem Phoenix did,
and the rewrite plan doesn't pay off.

Run::

    conda activate hummingbot
    python tools/redis_probe/05_compare_rest_vs_redis.py --duration 3600
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    RedisConfig,
    RestCreds,
    connect_pubsub,
    deadline,
    open_capture,
    percentile,
    rest_post,
    setup_logging,
    time_left,
)


log = setup_logging("redis_probe.05_compare")
SCRIPT = "05_compare_rest_vs_redis"

# Match window: a Redis event must arrive within ±MATCH_WINDOW_S of
# the REST observation to be considered a match.
MATCH_WINDOW_S = 30.0
REST_POLL_INTERVAL_S = 3.0


@dataclass
class RedisEvent:
    order_id: str
    message_cod: str
    recv_ts: float
    raw: Dict[str, Any]


@dataclass
class State:
    # All Redis events keyed by order_id
    redis_by_order: Dict[str, List[RedisEvent]] = field(default_factory=lambda: defaultdict(list))
    # REST observations
    rest_open_seen: Set[str] = field(default_factory=set)  # order_ids ever seen open
    rest_open_now: Set[str] = field(default_factory=set)   # in latest open_orders snapshot
    # First time an executed_orders row for this order_id appeared
    rest_fill_first_seen: Dict[str, float] = field(default_factory=dict)
    # First time a cancel was inferred (was open, then not open and not in fill list)
    rest_cancel_first_seen: Dict[str, float] = field(default_factory=dict)
    # Per-order: did REST mark as filled? (any exec_amount > 0)
    rest_fill_exec_amount: Dict[str, float] = field(default_factory=dict)
    # Baseline from the very first REST poll — these orders existed
    # before we started watching. Excluded from metrics to avoid
    # contaminating the "Redis never saw this" count with pre-history.
    baseline_open_ids: Set[str] = field(default_factory=set)
    baseline_executed_ids: Set[str] = field(default_factory=set)
    baseline_captured: bool = False


async def redis_listener(cfg: RedisConfig, channel: str, state: State,
                         stop: asyncio.Event, end_at: float) -> None:
    try:
        async with connect_pubsub(cfg) as (client, pubsub):  # noqa: F841
            await pubsub.subscribe(channel)
            log.info("subscribed to %s", channel)
            while time_left(end_at) > 0 and not stop.is_set():
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
                        continue
                if not isinstance(payload, dict):
                    continue
                order = payload.get("order") if isinstance(
                    payload.get("order"), dict) else None
                order_id = None
                if order is not None:
                    order_id = order.get("id")
                order_id = order_id or payload.get("order_id") or payload.get("id")
                if order_id is None:
                    continue
                cod = payload.get("message_cod") or payload.get("type") or "<unknown>"
                state.redis_by_order[str(order_id)].append(
                    RedisEvent(
                        order_id=str(order_id),
                        message_cod=str(cod),
                        recv_ts=recv_ts,
                        raw=payload,
                    )
                )
    except Exception:
        log.exception("redis listener crashed")
        stop.set()


async def rest_poller(creds: RestCreds, state: State, stop: asyncio.Event,
                      end_at: float) -> None:
    while time_left(end_at) > 0 and not stop.is_set():
        t0 = time.time()
        try:
            # open_orders + executed_orders in parallel; sleep until next slot
            open_resp, exec_resp = await asyncio.gather(
                rest_post("open_orders", creds, timeout_s=8.0),
                rest_post("executed_orders", creds, timeout_s=8.0),
                return_exceptions=True,
            )
            now = time.time()
            # First successful pair of responses = baseline. Don't
            # register any of these as "new" fills/cancels — they
            # existed before we started watching.
            if not state.baseline_captured:
                _capture_baseline(open_resp, exec_resp, state)
                state.baseline_captured = True
                log.info(
                    "baseline captured: %d open, %d executed (excluded from metrics)",
                    len(state.baseline_open_ids), len(state.baseline_executed_ids),
                )
                # Seed rest_open_now so the cancel-inference diff
                # against the next poll has the right reference.
                state.rest_open_now = set(state.baseline_open_ids)
            else:
                # Order matters: fills FIRST so cancel-inference in
                # _process_open_orders correctly skips orders that
                # just filled (same-poll race condition).
                _process_executed_orders(exec_resp, state, now)
                _process_open_orders(open_resp, state, now)
        except Exception:
            log.exception("REST poll iteration crashed")
        # Pace polling
        elapsed = time.time() - t0
        await asyncio.sleep(max(0.1, REST_POLL_INTERVAL_S - elapsed))


def _capture_baseline(open_resp: Any, exec_resp: Any, state: State) -> None:
    """Collect order_ids from the first REST poll. These existed before
    our Redis subscription, so metrics must exclude them."""
    for resp, target in ((open_resp, state.baseline_open_ids),
                         (exec_resp, state.baseline_executed_ids)):
        rows = _extract_order_list(resp)
        if rows is None:
            continue
        for o in rows:
            if not isinstance(o, dict):
                continue
            oid = o.get("id") or o.get("order_id")
            if oid is not None:
                target.add(str(oid))


def _extract_order_list(resp: Any) -> Optional[List[Dict[str, Any]]]:
    """BitPreco's open_orders/executed_orders return shape:

    Empirically (Phase 0 capture, 2026-05-15): a bare top-level JSON
    array of order dicts. Some endpoints in the API wrap responses in
    ``{"success": bool, ...}``; accept both for safety.
    """
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for key in ("orders", "open_orders", "executed_orders", "data"):
            v = resp.get(key)
            if isinstance(v, list):
                return v
    return None


def _process_open_orders(resp: Any, state: State, now: float) -> None:
    if isinstance(resp, BaseException):
        log.warning("open_orders failed: %s", resp)
        return
    # BitPreco's `open_orders` returns a bare JSON array at top level
    # (not wrapped in {"orders": [...]}). Accept either shape.
    orders = _extract_order_list(resp)
    if orders is None:
        return
    current_open: Set[str] = set()
    for o in orders:
        if not isinstance(o, dict):
            continue
        oid = o.get("id") or o.get("order_id")
        if oid is None:
            continue
        oid = str(oid)
        current_open.add(oid)
        state.rest_open_seen.add(oid)
    # Cancel inference: any order that was open but isn't anymore AND
    # has no fill record gets flagged as cancel-at-this-poll.
    previously_open = state.rest_open_now
    disappeared = previously_open - current_open
    for oid in disappeared:
        if oid in state.rest_fill_first_seen:
            continue  # it filled, not cancelled
        if oid not in state.rest_cancel_first_seen:
            state.rest_cancel_first_seen[oid] = now
    state.rest_open_now = current_open


def _process_executed_orders(resp: Any, state: State, now: float) -> None:
    if isinstance(resp, BaseException):
        log.warning("executed_orders failed: %s", resp)
        return
    rows = _extract_order_list(resp)
    if rows is None:
        return
    for o in rows:
        if not isinstance(o, dict):
            continue
        oid = o.get("id") or o.get("order_id")
        if oid is None:
            continue
        oid = str(oid)
        # Skip orders that existed in executed history before we
        # started watching — they bias the metric heavily.
        if oid in state.baseline_executed_ids:
            continue
        exec_amount = float(o.get("exec_amount") or o.get("executed") or 0)
        if exec_amount > 0:
            state.rest_fill_first_seen.setdefault(oid, now)
            prev = state.rest_fill_exec_amount.get(oid, 0)
            if exec_amount > prev:
                state.rest_fill_exec_amount[oid] = exec_amount


def reconcile(state: State) -> Dict[str, Any]:
    """Build the match-rate + latency report."""
    fill_matches: List[Dict[str, Any]] = []
    fill_misses: List[str] = []
    cancel_matches: List[Dict[str, Any]] = []
    cancel_misses: List[str] = []
    redis_only: List[str] = []

    # Track which Redis order_ids we matched (to find Redis-only)
    matched_redis_ids: Set[str] = set()

    # --- Fills ---
    for oid, rest_ts in state.rest_fill_first_seen.items():
        events = state.redis_by_order.get(oid, [])
        match = _find_match(events, ("ORDER_FULLY_EXECUTED",
                                     "ORDER_PARTIALLY_EXECUTED"), rest_ts)
        if match:
            fill_matches.append({
                "order_id": oid,
                "rest_ts": rest_ts,
                "redis_ts": match.recv_ts,
                "delta_s": match.recv_ts - rest_ts,
                "cod": match.message_cod,
            })
            matched_redis_ids.add(oid)
        else:
            fill_misses.append(oid)

    # --- Cancels ---
    for oid, rest_ts in state.rest_cancel_first_seen.items():
        events = state.redis_by_order.get(oid, [])
        match = _find_match(events, ("ORDER_CANCELED",), rest_ts)
        if match:
            cancel_matches.append({
                "order_id": oid,
                "rest_ts": rest_ts,
                "redis_ts": match.recv_ts,
                "delta_s": match.recv_ts - rest_ts,
            })
            matched_redis_ids.add(oid)
        else:
            cancel_misses.append(oid)

    # --- Redis-only (events for orders REST never recorded) ---
    for oid in state.redis_by_order:
        if oid not in matched_redis_ids and oid not in state.rest_open_seen:
            redis_only.append(oid)

    fill_deltas = [m["delta_s"] for m in fill_matches]
    cancel_deltas = [m["delta_s"] for m in cancel_matches]

    fill_total = len(fill_matches) + len(fill_misses)
    cancel_total = len(cancel_matches) + len(cancel_misses)

    return {
        "fills": {
            "total_observed_via_rest": fill_total,
            "matched_by_redis": len(fill_matches),
            "match_rate_pct": round(100 * len(fill_matches) / fill_total, 2)
            if fill_total else None,
            "missed_order_ids": fill_misses[:50],
            "delta_redis_minus_rest_s": {
                "count": len(fill_deltas),
                "p50": percentile(fill_deltas, 50),
                "p99": percentile(fill_deltas, 99),
                "min": min(fill_deltas) if fill_deltas else None,
                "max": max(fill_deltas) if fill_deltas else None,
            },
        },
        "cancels": {
            "total_observed_via_rest": cancel_total,
            "matched_by_redis": len(cancel_matches),
            "match_rate_pct": round(100 * len(cancel_matches) / cancel_total, 2)
            if cancel_total else None,
            "missed_order_ids": cancel_misses[:50],
            "delta_redis_minus_rest_s": {
                "count": len(cancel_deltas),
                "p50": percentile(cancel_deltas, 50),
                "p99": percentile(cancel_deltas, 99),
            },
        },
        "redis_only": {
            "count": len(redis_only),
            "sample_order_ids": redis_only[:20],
            "note": ("Redis events for order_ids REST never observed. "
                     "Could be: orders cancelled before next REST poll, "
                     "phantom/test events, or REST poll gap. Investigate "
                     "manually if count is non-trivial."),
        },
    }


def _find_match(events: List[RedisEvent], cods: tuple, rest_ts: float) -> Optional[RedisEvent]:
    best: Optional[RedisEvent] = None
    best_delta = float("inf")
    for ev in events:
        if ev.message_cod not in cods:
            continue
        d = abs(ev.recv_ts - rest_ts)
        if d > MATCH_WINDOW_S:
            continue
        if d < best_delta:
            best_delta = d
            best = ev
    return best


async def run(cfg: RedisConfig, creds: RestCreds, channel_override: Optional[str],
              duration_s: float) -> int:
    if channel_override:
        channel = channel_override
    elif cfg.user_id:
        channel = f"update:{cfg.user_id}"
    else:
        print("BITPRECO_USER_ID not set. Run 04_get_user_id.py first or "
              "pass --channel.", file=sys.stderr)
        return 2

    capture_path, writer = open_capture(SCRIPT)
    state = State()
    print(f"Comparing Redis ({channel}) vs REST for {duration_s:.0f}s "
          "(Ctrl-C to stop early)")
    print(f"Output: {capture_path.with_suffix('.report.json')}")

    end_at = deadline(duration_s)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    redis_task = asyncio.create_task(redis_listener(cfg, channel, state, stop, end_at))
    rest_task = asyncio.create_task(rest_poller(creds, state, stop, end_at))

    # Progress loop
    try:
        while time_left(end_at) > 0 and not stop.is_set():
            await asyncio.sleep(min(60.0, time_left(end_at)))
            interim = reconcile(state)
            print(f"  [t+{int(duration_s - time_left(end_at)):d}s] "
                  f"fills={interim['fills']['matched_by_redis']}/"
                  f"{interim['fills']['total_observed_via_rest']} "
                  f"cancels={interim['cancels']['matched_by_redis']}/"
                  f"{interim['cancels']['total_observed_via_rest']}")
    finally:
        stop.set()
        for t in (redis_task, rest_task):
            try:
                await asyncio.wait_for(t, timeout=2.0)
            except asyncio.TimeoutError:
                t.cancel()
        report = reconcile(state)
        # Persist both the raw state summary and the metrics
        report["meta"] = {
            "duration_s": duration_s,
            "match_window_s": MATCH_WINDOW_S,
            "rest_poll_interval_s": REST_POLL_INTERVAL_S,
        }
        capture_path.with_suffix(".report.json").write_text(
            json.dumps(report, indent=2, default=str))
        # The JSONL is empty here — keep file for symmetry but note this
        writer.write({"note": "this script's primary output is the .report.json sibling"})
        writer.close()
        print(json.dumps(report, indent=2, default=str))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--duration", type=float, default=3600.0,
                    help="comparison window in seconds (default 3600 = 1h)")
    ap.add_argument("--channel", type=str, default=None,
                    help="override channel (default: update:<BITPRECO_USER_ID>)")
    args = ap.parse_args()

    try:
        cfg = RedisConfig.from_env()
        creds = RestCreds.from_env()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    try:
        return asyncio.run(run(cfg, creds, args.channel, args.duration))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
