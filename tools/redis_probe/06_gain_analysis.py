#!/usr/bin/env python3
"""Phase 0 / step 6 — quantify Redis gains vs the legacy bot.

Cross-references the Redis update capture (script 02) with the
legacy bot's own log file to answer:

1. **Detection latency**: for every BitPreco order event the legacy
   bot observed, how much later than Redis did it arrive?
2. **Coverage gap**: how many terminal events (fill / cancel)
   Redis observed but the legacy bot never recognized within its
   60 s ghost-controller window?
3. **Ghost-purge waste**: of the orders the bot ended up
   ghost-purging (giving up after 60 s with "no fill event"),
   how many had a terminal Redis event that arrived *while the
   bot was still waiting*?

Both inputs must cover the same wall-clock window. The script
takes the 02 capture JSONL on the command line and the bot log
path; it self-aligns the comparison window to the JSONL's
recv_ts range.

Usage::

    conda activate hummingbot
    python tools/redis_probe/06_gain_analysis.py \\
        --capture tools/redis_probe/captures/02_capture_update-20260515-1438.jsonl \\
        --bot-log logs/logs_conf_xemm_lead_lag_sbe.log
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import percentile, setup_logging  # noqa: E402


log = setup_logging("redis_probe.06_gain")

# BRT (UTC-3) offset, applied to BitPreco's order/balance timestamps.
BRT_OFFSET_SEC = -3 * 3600


# ---------------------------------------------------------------------------
# Bot log parsing
# ---------------------------------------------------------------------------

# Format: 2026-05-15 14:39:20,184 - PID - ...
LOG_TS_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - \d+ - "
)

# Hummingbot EVENT_LOG records are JSON in the message body. We pull
# the exchange_order_id (BitPreco numeric id) and the event_name.
EVENT_RE = re.compile(
    r"EVENT_LOG - (?P<json>\{.*\})$"
)

# Ghost purge lines look like:
#   ... INFO - [ghost_controller] purged stale entry BBCBL...
GHOST_RE = re.compile(
    r"\[ghost_controller\] purged stale entry (?P<cid>\S+) "
    r"\(no fill event arrived within (?P<wait>[\d.]+)s\)"
)


@dataclass
class BotEvent:
    """A single observed bot-side event keyed by BitPreco exchange order id."""
    exchange_order_id: str
    event_name: str           # "BuyOrderCreatedEvent" | "OrderFilledEvent" | ...
    bot_ts: float             # unix epoch (UTC) — bot's wall clock
    client_order_id: Optional[str] = None


@dataclass
class BotState:
    # All BitPreco events the bot observed, by exchange_order_id
    events_by_xid: Dict[str, List[BotEvent]] = field(default_factory=lambda: defaultdict(list))
    # Reverse map: client_order_id -> exchange_order_id (built from Created events)
    client_to_xid: Dict[str, str] = field(default_factory=dict)
    # Reverse map: exchange_order_id -> bot's Create timestamp (when bot first saw it)
    create_ts_by_xid: Dict[str, float] = field(default_factory=dict)
    # Ghost purges (by client_order_id, with timestamp)
    ghost_purges: List[Tuple[float, str]] = field(default_factory=list)


def _parse_log_ts(ts_str: str) -> float:
    """Bot log timestamps are local wall clock (UTC, this host).
    Parse to unix epoch."""
    # Replace comma with dot for microseconds
    s = ts_str.replace(",", ".")
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f").replace(
        tzinfo=dt.timezone.utc).timestamp()


def parse_bot_log(path: Path, win_start: float, win_end: float) -> BotState:
    """Walk the bot log, keep only entries inside [win_start, win_end]
    (unix epoch UTC) and only those that reference a BitPreco order."""
    state = BotState()
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = LOG_TS_RE.match(line)
            if not m:
                continue
            try:
                bot_ts = _parse_log_ts(m.group("ts"))
            except ValueError:
                continue
            if bot_ts < win_start - 60 or bot_ts > win_end + 60:
                continue  # outside the window of interest (small buffer)

            # Ghost purges
            g = GHOST_RE.search(line)
            if g:
                state.ghost_purges.append((bot_ts, g.group("cid")))
                continue

            # JSON event log
            e = EVENT_RE.search(line)
            if not e:
                continue
            try:
                payload = json.loads(e.group("json"))
            except json.JSONDecodeError:
                continue
            if payload.get("event_source") != "bitpreco":
                continue
            xid = payload.get("exchange_order_id")
            if not xid:
                continue
            ev = BotEvent(
                exchange_order_id=str(xid),
                event_name=str(payload.get("event_name") or ""),
                bot_ts=bot_ts,
                client_order_id=payload.get("order_id"),
            )
            state.events_by_xid[ev.exchange_order_id].append(ev)
            if ev.event_name in ("BuyOrderCreatedEvent", "SellOrderCreatedEvent"):
                if ev.client_order_id:
                    state.client_to_xid[ev.client_order_id] = ev.exchange_order_id
                # Earliest create wins (in case of duplicates)
                state.create_ts_by_xid.setdefault(ev.exchange_order_id, ev.bot_ts)
    return state


# ---------------------------------------------------------------------------
# Redis capture parsing
# ---------------------------------------------------------------------------

@dataclass
class RedisOrderEvent:
    exchange_order_id: str
    message_cod: str
    recv_ts: float                  # unix epoch UTC (when WE saw it)
    event_ts: Optional[float] = None  # parsed from order.time_stamp (BRT->UTC)


def _parse_iso_brt(s: str) -> Optional[float]:
    """BitPreco timestamps are 'YYYY-MM-DD HH:MM:SS[.ffffff]' in BRT (UTC-3)."""
    if not isinstance(s, str):
        return None
    try:
        if "." in s:
            d = dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
        else:
            d = dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    # Treat as BRT (UTC-3): unix epoch = local - offset (i.e. add 3h to get UTC)
    return d.replace(tzinfo=dt.timezone.utc).timestamp() - BRT_OFFSET_SEC


def parse_redis_capture(path: Path) -> List[RedisOrderEvent]:
    out: List[RedisOrderEvent] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            payload = rec.get("payload")
            if not isinstance(payload, dict):
                continue
            order = payload.get("order") if isinstance(payload.get("order"), dict) else None
            xid = (payload.get("order_id")
                   or (order.get("id") if order else None))
            cod = payload.get("message_cod")
            recv_ts = rec.get("recv_ts")
            if not (xid and cod and recv_ts):
                continue
            event_ts = None
            if order and isinstance(order.get("time_stamp"), str):
                event_ts = _parse_iso_brt(order["time_stamp"])
            out.append(RedisOrderEvent(
                exchange_order_id=str(xid),
                message_cod=str(cod),
                recv_ts=float(recv_ts),
                event_ts=event_ts,
            ))
    return out


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

# Map Redis message_cod -> the bot-side event_name that should follow.
# Used to pair them up when measuring detection latency.
_TERMINAL_REDIS_CODS = {"ORDER_FULLY_EXECUTED", "ORDER_CANCELED",
                        "ORDER_PARTIALLY_EXECUTED"}
_REDIS_TO_BOT_NAME = {
    "BUY_ORDER_CREATED":          "BuyOrderCreatedEvent",
    "SELL_ORDER_CREATED":         "SellOrderCreatedEvent",
    "ORDER_FULLY_EXECUTED":       "OrderFilledEvent",
    "ORDER_PARTIALLY_EXECUTED":   "OrderFilledEvent",
    "ORDER_CANCELED":             "OrderCancelledEvent",
}


def analyze(redis_events: List[RedisOrderEvent], bot: BotState,
            win_start: float, win_end: float) -> Dict:
    # Bucket Redis by xid
    redis_by_xid: Dict[str, List[RedisOrderEvent]] = defaultdict(list)
    for ev in redis_events:
        redis_by_xid[ev.exchange_order_id].append(ev)

    # --- 1. Bidirectional detection latency ---
    # Pair each Redis event (create/terminal) with the corresponding
    # bot event for the same xid, REGARDLESS of direction. Positive
    # delta = bot saw it AFTER Redis (Redis would have helped).
    # Negative delta = bot saw it BEFORE Redis (bot's sync path wins,
    # Redis doesn't help here).
    deltas_terminal_ms: List[float] = []
    deltas_create_ms: List[float] = []
    bot_first_terminal = 0
    redis_first_terminal = 0
    redis_terminals_total = 0
    redis_terminals_with_match = 0
    redis_terminals_no_match: List[Tuple[str, str, float]] = []

    for xid, evs in redis_by_xid.items():
        bot_evs = bot.events_by_xid.get(xid, [])
        for r in evs:
            if not (win_start <= r.recv_ts <= win_end):
                continue
            target = _REDIS_TO_BOT_NAME.get(r.message_cod)
            if not target:
                continue
            # Pick the bot event of matching type CLOSEST in time
            candidates = [b for b in bot_evs if b.event_name == target]
            if not candidates:
                if r.message_cod in _TERMINAL_REDIS_CODS:
                    redis_terminals_total += 1
                    redis_terminals_no_match.append(
                        (xid, r.message_cod, r.recv_ts))
                continue
            best = min(candidates, key=lambda b: abs(b.bot_ts - r.recv_ts))
            delta_ms = (best.bot_ts - r.recv_ts) * 1000
            if r.message_cod in _TERMINAL_REDIS_CODS:
                redis_terminals_total += 1
                redis_terminals_with_match += 1
                deltas_terminal_ms.append(delta_ms)
                if delta_ms < 0:
                    bot_first_terminal += 1
                else:
                    redis_first_terminal += 1
            else:
                deltas_create_ms.append(delta_ms)

    # Split positive-only (Redis-faster) vs negative-only (bot-faster)
    redis_faster_terminal = [d for d in deltas_terminal_ms if d > 0]
    bot_faster_terminal = [d for d in deltas_terminal_ms if d < 0]

    # Bot-only terminals (no Redis event for that xid at all)
    bot_terminal_no_redis = 0
    for xid, evs in bot.events_by_xid.items():
        for b in evs:
            if b.event_name not in ("OrderFilledEvent", "OrderCancelledEvent"):
                continue
            if not (win_start <= b.bot_ts <= win_end):
                continue
            if xid not in redis_by_xid:
                bot_terminal_no_redis += 1

    # --- 2. Ghost purges: did Redis know in time? ---
    # Use the REAL create_ts from the bot log instead of approximating
    # `ghost_ts - 60s` (the bot may wait longer than 60s).
    ghost_info = {
        "total": 0, "had_redis_within_wait": 0,
        "had_redis_at_all": 0, "no_create_ts": 0,
        "details": [],
    }
    for ghost_ts, cid in bot.ghost_purges:
        if not (win_start <= ghost_ts <= win_end):
            continue
        ghost_info["total"] += 1
        xid = bot.client_to_xid.get(cid)
        if not xid:
            continue
        create_ts = bot.create_ts_by_xid.get(xid)
        if create_ts is None:
            ghost_info["no_create_ts"] += 1
            continue
        redis_evs = redis_by_xid.get(xid, [])
        terminal = [r for r in redis_evs if r.message_cod in _TERMINAL_REDIS_CODS]
        if not terminal:
            continue
        ghost_info["had_redis_at_all"] += 1
        # Wait window = [create_ts, ghost_ts]. Redis terminal arriving
        # in this window = preventable (bot could have known).
        in_window = [r for r in terminal
                     if create_ts <= r.recv_ts <= ghost_ts]
        if in_window:
            ghost_info["had_redis_within_wait"] += 1
            earliest = min(in_window, key=lambda r: r.recv_ts)
            ghost_info["details"].append({
                "client_order_id": cid,
                "exchange_order_id": xid,
                "create_ts": create_ts,
                "ghost_purge_ts": ghost_ts,
                "redis_terminal_ts": earliest.recv_ts,
                "redis_cod": earliest.message_cod,
                "wait_seconds_total": ghost_ts - create_ts,
                "wasted_seconds": ghost_ts - earliest.recv_ts,
            })

    wasted = [d["wasted_seconds"] for d in ghost_info["details"]]

    return {
        "window": {
            "start_utc": dt.datetime.fromtimestamp(win_start, dt.timezone.utc).isoformat(),
            "end_utc":   dt.datetime.fromtimestamp(win_end, dt.timezone.utc).isoformat(),
            "duration_s": round(win_end - win_start, 1),
        },
        "detection_latency_ms": {
            "terminal_all": {
                "count": len(deltas_terminal_ms),
                "p50": percentile(deltas_terminal_ms, 50),
                "p99": percentile(deltas_terminal_ms, 99),
                "min": min(deltas_terminal_ms) if deltas_terminal_ms else None,
                "max": max(deltas_terminal_ms) if deltas_terminal_ms else None,
                "note": "positive = bot saw event AFTER Redis (Redis would help). "
                        "negative = bot's sync path beat Redis.",
            },
            "terminal_redis_faster": {
                "count": len(redis_faster_terminal),
                "p50": percentile(redis_faster_terminal, 50),
                "p99": percentile(redis_faster_terminal, 99),
                "max": max(redis_faster_terminal) if redis_faster_terminal else None,
            },
            "terminal_bot_faster": {
                "count": len(bot_faster_terminal),
                "p50": percentile(bot_faster_terminal, 50),
                "p99": percentile(bot_faster_terminal, 99),
                "min": min(bot_faster_terminal) if bot_faster_terminal else None,
                "note": "bot's _emit_synchronous_fill / cancel-response path",
            },
            "create_all": {
                "count": len(deltas_create_ms),
                "p50": percentile(deltas_create_ms, 50),
                "p99": percentile(deltas_create_ms, 99),
            },
        },
        "directionality": {
            "redis_first_count": redis_first_terminal,
            "bot_first_count":   bot_first_terminal,
            "redis_first_pct":   round(
                100 * redis_first_terminal / (redis_first_terminal + bot_first_terminal), 2)
                if (redis_first_terminal + bot_first_terminal) else None,
        },
        "coverage": {
            "redis_observed_terminals":       redis_terminals_total,
            "bot_also_observed":              redis_terminals_with_match,
            "bot_missed_terminals":           redis_terminals_total - redis_terminals_with_match,
            "match_rate_pct": round(
                100 * redis_terminals_with_match / redis_terminals_total, 2)
                if redis_terminals_total else None,
            "bot_terminals_with_no_redis":    bot_terminal_no_redis,
            "redis_terminals_no_bot_sample":  redis_terminals_no_match[:10],
        },
        "ghost_purges": {
            "total_in_window": ghost_info["total"],
            "had_redis_terminal_at_all": ghost_info["had_redis_at_all"],
            "had_redis_terminal_during_wait": ghost_info["had_redis_within_wait"],
            "preventable_pct": round(
                100 * ghost_info["had_redis_within_wait"] / ghost_info["total"], 2)
                if ghost_info["total"] else None,
            "no_create_ts_in_log": ghost_info["no_create_ts"],
            "wasted_wait_seconds_per_preventable_purge": {
                "count": len(wasted),
                "p50": percentile(wasted, 50),
                "p99": percentile(wasted, 99),
                "total": round(sum(wasted), 1),
                "mean": round(sum(wasted) / len(wasted), 2) if wasted else None,
            },
            "sample_details": ghost_info["details"][:5],
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--capture", type=Path, required=True,
                    help="JSONL produced by 02_capture_update.py")
    ap.add_argument("--bot-log", type=Path, required=True,
                    help="legacy bot log, e.g. logs/logs_conf_xemm_lead_lag_sbe.log")
    ap.add_argument("--report", type=Path, default=None,
                    help="write JSON report here (default: capture path + .gain.json)")
    args = ap.parse_args()

    redis_events = parse_redis_capture(args.capture)
    if not redis_events:
        print("No Redis events parsed from capture.", file=sys.stderr)
        return 2

    win_start = min(r.recv_ts for r in redis_events)
    win_end = max(r.recv_ts for r in redis_events)
    bot = parse_bot_log(args.bot_log, win_start, win_end)

    report = analyze(redis_events, bot, win_start, win_end)
    report["inputs"] = {
        "capture": str(args.capture),
        "bot_log": str(args.bot_log),
        "redis_events_in_window": len(redis_events),
        "bot_orders_in_window":   len(bot.events_by_xid),
        "bot_ghost_purges_in_window": sum(
            1 for ts, _ in bot.ghost_purges if win_start <= ts <= win_end),
    }

    out_path = args.report or args.capture.with_suffix(".gain.json")
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    print(f"\nReport written to {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
