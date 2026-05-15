#!/usr/bin/env python3
"""Phase 1A shadow-run analyser.

Parses the structured ``[redis_shadow]`` / ``[redis_shadow_ob]``
log lines emitted by the BitPreco connector when
``bitpreco_redis_shadow_mode=True``, cross-references them against
the bot's own OrderFilled/OrderCancelled events and ghost-controller
purges, and reports whether the Phase 2 promotion criteria are met.

The shadow observer runs in parallel with the legacy Phoenix/REST
path. It logs:

- once at startup:
    ``[redis_shadow] enabled — host=... user_id=...``
    ``[redis_shadow] subscribed to update:<id>``
    ``[redis_shadow_ob] subscribed to ['orderbook:<pair>']``

- once per terminal event (FULLY/PARTIALLY_EXECUTED, CANCELED):
    ``[redis_shadow] terminal type=ORDER_CANCELED xid=... decision=APPLY``
    ``                  recv_ts=... event_ts=... redis_minus_event=...``

- every 60 s, periodic counters:
    ``[redis_shadow] metrics recv=N parse_fail=N drop=N applied=N``
    ``                       enrich=N stale=N regress=N ...``
    ``[redis_shadow_ob] metrics counts={...} freshness_s={...} ...``

- on anomaly:
    ``[redis_shadow] REGRESSION rejected xid=...``
    ``[redis_shadow] unknown message_cod ...``
    ``[redis_shadow_ob] silence pair=... elapsed=...``

This script is read-only — it never touches the bot. Safe to run
at any time.

Usage
=====

::

    conda activate hummingbot
    python tools/redis_probe/07_shadow_log_analysis.py \\
        --bot-log logs/logs_conf_xemm_lead_lag_sbe.log

By default the analyser scans the entire log. Use ``--since`` /
``--until`` (ISO datetimes, UTC) to restrict to a window. The
report is printed to stdout and also written to
``<bot-log>.shadow.json`` next to the input.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import percentile  # noqa: E402


# ---------------------------------------------------------------------------
# Regex anchors — keyed to the log strings emitted by the shadow modules.
# ---------------------------------------------------------------------------

# Hummingbot timestamp: 2026-05-15 16:44:48,422
RE_LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3}) - \d+ - ")

RE_US_ENABLED = re.compile(
    r"\[redis_shadow\] enabled — host=(?P<host>\S+) "
    r"user_id=(?P<user_id>\S+) tls=(?P<tls>\S+) "
    r"update_channel=(?P<channel>\S+)"
)

RE_US_SUBSCRIBED = re.compile(r"\[redis_shadow\] subscribed to (\S+)")

RE_OB_SUBSCRIBED = re.compile(r"\[redis_shadow_ob\] subscribed to (\[.*\])")

RE_US_METRICS = re.compile(
    r"\[redis_shadow\] metrics "
    r"recv=(?P<recv>\d+) parse_fail=(?P<parse_fail>\d+) "
    r"drop=(?P<drop>\d+) applied=(?P<applied>\d+) "
    r"enrich=(?P<enrich>\d+) stale=(?P<stale>\d+) "
    r"regress=(?P<regress>\d+) unknown_cod=(?P<unknown>\d+) "
    r"orphans_buf=(?P<orphans_buf>\d+) orphans_swept=(?P<orphans_swept>\d+) "
    r"reconnects=(?P<reconnects>\d+) "
    r"sm_size=(?P<sm_size>\d+) orphan_size=(?P<orphan_size>\d+)"
)

RE_US_TERMINAL = re.compile(
    r"\[redis_shadow\] terminal "
    r"type=(?P<type>\S+) xid=(?P<xid>\S+) decision=(?P<decision>\S+) "
    r"recv_ts=(?P<recv_ts>[\d.]+) event_ts=(?P<event_ts>\S+) "
    r"redis_minus_event=(?P<delta>\S+)"
)

RE_US_REGRESSION = re.compile(
    r"\[redis_shadow\] REGRESSION rejected xid=(?P<xid>\S+) "
    r"current_state=(?P<current>\S+) incoming=(?P<incoming>\S+)"
)

RE_US_UNKNOWN_COD = re.compile(
    r"\[redis_shadow\] unknown message_cod .*raw_cod=(?P<cod>.+?)\s*$"
)

RE_US_NON_JSON = re.compile(r"\[redis_shadow\] non-JSON message dropped:")

RE_OB_METRICS = re.compile(
    r"\[redis_shadow_ob\] metrics "
    r"counts=(?P<counts>\{[^}]*\}) freshness_s=(?P<freshness>\{[^}]*\}) "
    r"parse_fail=(?P<parse_fail>\d+) reconnects=(?P<reconnects>\d+)"
)

RE_OB_SILENCE = re.compile(
    r"\[redis_shadow_ob\] silence pair=(?P<pair>\S+) "
    r"elapsed=(?P<elapsed>[\d.]+)s threshold=(?P<threshold>[\d.]+)s"
)

# Bot side — for cross-reference
RE_EVENT_LOG = re.compile(r"EVENT_LOG - (?P<json>\{.*\})$")
RE_GHOST_PURGE = re.compile(
    r"\[ghost_controller\] purged stale entry (?P<cid>\S+) "
    r"\(no fill event arrived within (?P<wait>[\d.]+)s\)"
)


# ---------------------------------------------------------------------------
# State accumulators
# ---------------------------------------------------------------------------

@dataclass
class TerminalEvent:
    """A single ``[redis_shadow] terminal`` log row."""
    bot_log_ts: float       # wall clock when the line was written
    redis_recv_ts: float    # connector's recv_ts (Redis-side wall clock)
    event_ts: Optional[float]   # server-side event_ts (the canonical one)
    delta: Optional[float]      # recv_ts - event_ts (publish latency)
    event_type: str
    decision: str
    xid: str


@dataclass
class US_MetricsSnapshot:
    bot_log_ts: float
    recv: int
    parse_fail: int
    drop: int
    applied: int
    enrich: int
    stale: int
    regress: int
    unknown: int
    orphans_buf: int
    orphans_swept: int
    reconnects: int
    sm_size: int
    orphan_size: int


@dataclass
class OB_MetricsSnapshot:
    bot_log_ts: float
    counts: Dict[str, int]
    freshness_s: Dict[str, Optional[float]]
    parse_fail: int
    reconnects: int


@dataclass
class BotTerminalEvent:
    """An OrderFilled/Cancelled event the bot itself logged. Used to
    measure bot-side detection latency against Redis."""
    bot_log_ts: float
    event_name: str   # 'OrderFilledEvent' or 'OrderCancelledEvent'
    xid: str          # exchange_order_id (BitPreco numeric)
    client_order_id: Optional[str]


@dataclass
class GhostPurge:
    bot_log_ts: float
    client_order_id: str


@dataclass
class State:
    enabled: Optional[Dict[str, str]] = None
    us_subscribed_to: List[str] = field(default_factory=list)
    ob_subscribed_to: List[str] = field(default_factory=list)

    terminals: List[TerminalEvent] = field(default_factory=list)
    us_metrics_snapshots: List[US_MetricsSnapshot] = field(default_factory=list)
    ob_metrics_snapshots: List[OB_MetricsSnapshot] = field(default_factory=list)

    regressions: List[Dict[str, str]] = field(default_factory=list)
    unknown_cods: Counter = field(default_factory=Counter)
    non_json_drops: int = 0
    silence_warnings: List[Dict[str, Any]] = field(default_factory=list)

    bot_terminals_by_xid: Dict[str, List[BotTerminalEvent]] = \
        field(default_factory=lambda: defaultdict(list))
    bot_create_by_cid: Dict[str, Tuple[float, str]] = field(default_factory=dict)
    # (bot_log_ts, exchange_order_id) for each create event we saw
    ghost_purges: List[GhostPurge] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_log_ts(s: str, ms: str) -> float:
    """Convert Hummingbot's 'YYYY-MM-DD HH:MM:SS,fff' to unix epoch.

    Hummingbot writes wall-clock UTC in this host's timezone-aware sense
    but without TZ marker. We treat as UTC — same convention used by
    ``tools/redis_probe/06_gain_analysis.py``.
    """
    naive = dt.datetime.strptime(f"{s}.{ms}", "%Y-%m-%d %H:%M:%S.%f")
    return naive.replace(tzinfo=dt.timezone.utc).timestamp()


def _maybe_float(s: str) -> Optional[float]:
    if s in ("?", "None"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_dict_like(literal: str) -> Dict[str, Any]:
    """Parse ``{'BTC-BRL': 77, 'ETH-BRL': 12}`` to a real dict.

    The shadow observer formats with Python's repr (single quotes,
    None instead of null), so json.loads won't work. We use a guarded
    ``ast.literal_eval``.
    """
    import ast
    try:
        result = ast.literal_eval(literal)
        return result if isinstance(result, dict) else {}
    except (ValueError, SyntaxError):
        return {}


def parse_log(path: Path, win_start: Optional[float],
              win_end: Optional[float]) -> State:
    state = State()
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = RE_LOG_TS.match(line)
            if not m:
                continue
            ts = _parse_log_ts(m.group(1), m.group(2))
            if win_start is not None and ts < win_start:
                continue
            if win_end is not None and ts > win_end:
                continue
            _dispatch_line(line, ts, state)
    return state


def _dispatch_line(line: str, ts: float, state: State) -> None:
    # Try each pattern in order of expected frequency.
    m = RE_US_TERMINAL.search(line)
    if m:
        state.terminals.append(TerminalEvent(
            bot_log_ts=ts,
            redis_recv_ts=float(m.group("recv_ts")),
            event_ts=_maybe_float(m.group("event_ts")),
            delta=_maybe_float(m.group("delta")),
            event_type=m.group("type"),
            decision=m.group("decision"),
            xid=m.group("xid"),
        ))
        return

    m = RE_US_METRICS.search(line)
    if m:
        state.us_metrics_snapshots.append(US_MetricsSnapshot(
            bot_log_ts=ts,
            recv=int(m.group("recv")),
            parse_fail=int(m.group("parse_fail")),
            drop=int(m.group("drop")),
            applied=int(m.group("applied")),
            enrich=int(m.group("enrich")),
            stale=int(m.group("stale")),
            regress=int(m.group("regress")),
            unknown=int(m.group("unknown")),
            orphans_buf=int(m.group("orphans_buf")),
            orphans_swept=int(m.group("orphans_swept")),
            reconnects=int(m.group("reconnects")),
            sm_size=int(m.group("sm_size")),
            orphan_size=int(m.group("orphan_size")),
        ))
        return

    m = RE_OB_METRICS.search(line)
    if m:
        counts = _parse_dict_like(m.group("counts"))
        freshness = _parse_dict_like(m.group("freshness"))
        state.ob_metrics_snapshots.append(OB_MetricsSnapshot(
            bot_log_ts=ts,
            counts={k: int(v) if isinstance(v, (int, float)) else 0
                    for k, v in counts.items()},
            freshness_s={k: float(v) if isinstance(v, (int, float)) else None
                         for k, v in freshness.items()},
            parse_fail=int(m.group("parse_fail")),
            reconnects=int(m.group("reconnects")),
        ))
        return

    m = RE_EVENT_LOG.search(line)
    if m:
        try:
            payload = json.loads(m.group("json"))
        except json.JSONDecodeError:
            return
        if payload.get("event_source") != "bitpreco":
            return
        evname = payload.get("event_name", "")
        xid = payload.get("exchange_order_id")
        cid = payload.get("order_id")
        if evname in ("BuyOrderCreatedEvent", "SellOrderCreatedEvent") and cid and xid:
            state.bot_create_by_cid[cid] = (ts, str(xid))
        if evname in ("OrderFilledEvent", "OrderCancelledEvent") and xid:
            state.bot_terminals_by_xid[str(xid)].append(BotTerminalEvent(
                bot_log_ts=ts,
                event_name=evname,
                xid=str(xid),
                client_order_id=cid,
            ))
        return

    m = RE_GHOST_PURGE.search(line)
    if m:
        state.ghost_purges.append(GhostPurge(
            bot_log_ts=ts, client_order_id=m.group("cid")))
        return

    m = RE_US_REGRESSION.search(line)
    if m:
        state.regressions.append({
            "bot_log_ts": ts,
            "xid": m.group("xid"),
            "current": m.group("current"),
            "incoming": m.group("incoming"),
        })
        return

    m = RE_US_UNKNOWN_COD.search(line)
    if m:
        state.unknown_cods[m.group("cod").strip()] += 1
        return

    if RE_US_NON_JSON.search(line):
        state.non_json_drops += 1
        return

    m = RE_OB_SILENCE.search(line)
    if m:
        state.silence_warnings.append({
            "bot_log_ts": ts,
            "pair": m.group("pair"),
            "elapsed": float(m.group("elapsed")),
            "threshold": float(m.group("threshold")),
        })
        return

    m = RE_US_ENABLED.search(line)
    if m:
        state.enabled = m.groupdict()
        return

    m = RE_US_SUBSCRIBED.search(line)
    if m:
        state.us_subscribed_to.append(m.group(1))
        return

    m = RE_OB_SUBSCRIBED.search(line)
    if m:
        state.ob_subscribed_to.append(m.group(1))
        return


# ---------------------------------------------------------------------------
# Cross-reference / analysis
# ---------------------------------------------------------------------------

# Redis message_cod → bot's event_name. Both PARTIAL and FULL map to
# OrderFilledEvent on the bot's side.
_TERMINAL_REDIS_TO_BOT = {
    "ORDER_FULLY_EXECUTED":     "OrderFilledEvent",
    "ORDER_PARTIALLY_EXECUTED": "OrderFilledEvent",
    "ORDER_CANCELED":           "OrderCancelledEvent",
}

# How wide a window to look for the bot's matching event around the
# Redis terminal. Empirically the bot is within 1 s; 5 s is generous.
_MATCH_WINDOW_SEC = 5.0


def analyse(state: State) -> Dict[str, Any]:
    # --- Latency (Redis recv_ts → server event_ts: publish delay) ---
    deltas = [t.delta for t in state.terminals if t.delta is not None]

    # --- Terminal coverage: Redis observed vs bot observed ---
    by_type = Counter(t.event_type for t in state.terminals)
    decisions = Counter(t.decision for t in state.terminals)

    # Cross-reference each Redis terminal with bot's terminal events
    # to compute bot detection latency after Redis.
    bot_after_redis_ms: List[float] = []
    redis_after_bot_ms: List[float] = []
    redis_no_bot_match: List[str] = []
    for t in state.terminals:
        target = _TERMINAL_REDIS_TO_BOT.get(t.event_type)
        if target is None:
            continue
        candidates = [b for b in state.bot_terminals_by_xid.get(t.xid, [])
                      if b.event_name == target]
        # Pick the bot event closest to the Redis recv_ts, within window
        nearby = [b for b in candidates
                  if abs(b.bot_log_ts - t.redis_recv_ts) <= _MATCH_WINDOW_SEC]
        if not nearby:
            redis_no_bot_match.append(t.xid)
            continue
        best = min(nearby, key=lambda b: abs(b.bot_log_ts - t.redis_recv_ts))
        delta_ms = (best.bot_log_ts - t.redis_recv_ts) * 1000
        if delta_ms >= 0:
            bot_after_redis_ms.append(delta_ms)
        else:
            redis_after_bot_ms.append(-delta_ms)

    # --- Ghost purges: did Redis terminal arrive in the bot's wait window? ---
    # bot_create_by_cid: cid → (create_ts, xid)
    ghost_total = len(state.ghost_purges)
    ghost_preventable = 0
    ghost_wasted_seconds: List[float] = []
    ghost_no_create = 0
    ghost_no_redis = 0
    redis_xid_to_first_terminal_ts: Dict[str, float] = {}
    for t in state.terminals:
        prev = redis_xid_to_first_terminal_ts.get(t.xid)
        if prev is None or t.redis_recv_ts < prev:
            redis_xid_to_first_terminal_ts[t.xid] = t.redis_recv_ts
    for g in state.ghost_purges:
        info = state.bot_create_by_cid.get(g.client_order_id)
        if info is None:
            ghost_no_create += 1
            continue
        create_ts, xid = info
        redis_terminal_ts = redis_xid_to_first_terminal_ts.get(xid)
        if redis_terminal_ts is None:
            ghost_no_redis += 1
            continue
        if create_ts <= redis_terminal_ts <= g.bot_log_ts:
            ghost_preventable += 1
            ghost_wasted_seconds.append(g.bot_log_ts - redis_terminal_ts)

    # --- Window (first/last meaningful timestamp seen) ---
    timestamps: List[float] = []
    for collection in (state.terminals, state.us_metrics_snapshots,
                       state.ob_metrics_snapshots):
        timestamps.extend(getattr(x, "bot_log_ts") for x in collection)
    win_start = min(timestamps) if timestamps else None
    win_end = max(timestamps) if timestamps else None
    duration = (win_end - win_start) if (win_start and win_end) else 0

    # --- Cumulative counts from the periodic metric snapshots ---
    last_us = state.us_metrics_snapshots[-1] if state.us_metrics_snapshots else None
    last_ob = state.ob_metrics_snapshots[-1] if state.ob_metrics_snapshots else None

    # Aggregate orderbook freshness samples across all snapshots/pairs
    freshness_samples_per_pair: Dict[str, List[float]] = defaultdict(list)
    snapshot_counts_per_pair: Dict[str, int] = defaultdict(int)
    for snap in state.ob_metrics_snapshots:
        for pair, count in snap.counts.items():
            snapshot_counts_per_pair[pair] = max(
                snapshot_counts_per_pair[pair], count)
        for pair, fr in snap.freshness_s.items():
            if fr is not None:
                freshness_samples_per_pair[pair].append(fr)

    # ---------- Phase 2 exit-criteria evaluation ----------
    total_redis_terminals = len(state.terminals)
    bot_matched = total_redis_terminals - len(redis_no_bot_match)
    match_rate = (
        (bot_matched / total_redis_terminals * 100)
        if total_redis_terminals else None
    )

    criteria = []
    criteria.append({
        "name": "≥99% terminal match rate (Redis ↔ bot)",
        "target_pct": 99.0,
        "observed_pct": round(match_rate, 2) if match_rate is not None else None,
        "passed": (match_rate is not None) and (match_rate >= 99.0),
    })
    criteria.append({
        "name": "Zero parse failures",
        "target": 0,
        "observed": last_us.parse_fail if last_us else 0,
        "passed": (last_us is None) or (last_us.parse_fail == 0),
    })
    criteria.append({
        "name": "Zero state regression",
        "target": 0,
        "observed": len(state.regressions),
        "passed": len(state.regressions) == 0,
    })
    criteria.append({
        "name": "Zero unknown message_cod",
        "target": 0,
        "observed": sum(state.unknown_cods.values()),
        "passed": sum(state.unknown_cods.values()) == 0,
    })
    criteria.append({
        "name": "Zero non-JSON drops",
        "target": 0,
        "observed": state.non_json_drops,
        "passed": state.non_json_drops == 0,
    })
    criteria.append({
        "name": "Zero orderbook silence warnings",
        "target": 0,
        "observed": len(state.silence_warnings),
        "passed": len(state.silence_warnings) == 0,
    })
    all_pass = all(c["passed"] for c in criteria)

    return {
        "window": {
            "start_utc": (dt.datetime.fromtimestamp(win_start, dt.timezone.utc)
                          .isoformat() if win_start else None),
            "end_utc": (dt.datetime.fromtimestamp(win_end, dt.timezone.utc)
                        .isoformat() if win_end else None),
            "duration_s": round(duration, 1),
            "duration_human": _fmt_duration(duration),
        },
        "startup": {
            "enabled": state.enabled,
            "us_subscribed_to": state.us_subscribed_to,
            "ob_subscribed_to": state.ob_subscribed_to,
        },
        "user_stream_cumulative": {
            "snapshots_seen": len(state.us_metrics_snapshots),
            "latest": _us_snapshot_to_dict(last_us) if last_us else None,
        },
        "terminal_events": {
            "total": len(state.terminals),
            "by_type": dict(by_type),
            "by_decision": dict(decisions),
            "publish_latency_seconds": {
                "p50": percentile(deltas, 50),
                "p99": percentile(deltas, 99),
                "max": max(deltas) if deltas else None,
                "count": len(deltas),
            },
        },
        "bot_cross_reference": {
            "redis_terminals_observed": total_redis_terminals,
            "bot_terminal_matched": bot_matched,
            "redis_no_bot_match_sample": redis_no_bot_match[:10],
            "match_rate_pct": round(match_rate, 2) if match_rate is not None else None,
            "bot_detection_latency_ms_after_redis": {
                "p50": percentile(bot_after_redis_ms, 50),
                "p99": percentile(bot_after_redis_ms, 99),
                "max": max(bot_after_redis_ms) if bot_after_redis_ms else None,
                "count": len(bot_after_redis_ms),
            },
            "bot_first_count": len(redis_after_bot_ms),
            "bot_first_latency_ms_before_redis": {
                "p50": percentile(redis_after_bot_ms, 50),
                "max": max(redis_after_bot_ms) if redis_after_bot_ms else None,
            },
        },
        "ghost_purges": {
            "total": ghost_total,
            "no_create_in_log": ghost_no_create,
            "no_redis_terminal": ghost_no_redis,
            "preventable_by_redis": ghost_preventable,
            "preventable_pct": (
                round(100 * ghost_preventable / ghost_total, 2)
                if ghost_total else None),
            "wasted_seconds_per_purge": {
                "p50": percentile(ghost_wasted_seconds, 50),
                "mean": (round(sum(ghost_wasted_seconds) / len(ghost_wasted_seconds), 2)
                         if ghost_wasted_seconds else None),
                "total_seconds": round(sum(ghost_wasted_seconds), 1),
                "total_minutes": round(sum(ghost_wasted_seconds) / 60, 1),
            },
        },
        "order_book": {
            "snapshots_seen": dict(snapshot_counts_per_pair),
            "freshness_s": {
                pair: {
                    "p50": percentile(fs, 50),
                    "p99": percentile(fs, 99),
                    "max": max(fs) if fs else None,
                }
                for pair, fs in freshness_samples_per_pair.items()
            },
            "silence_warnings": len(state.silence_warnings),
            "silence_sample": state.silence_warnings[:5],
            "latest": _ob_snapshot_to_dict(last_ob) if last_ob else None,
        },
        "anomalies": {
            "regressions": state.regressions[:10],
            "regression_count": len(state.regressions),
            "unknown_cods": dict(state.unknown_cods),
            "non_json_drops": state.non_json_drops,
        },
        "phase2_exit_criteria": {
            "criteria": criteria,
            "all_passed": all_pass,
            "recommendation": (
                "READY TO PROMOTE — all criteria met. Phase 2 (data_backend=redis) "
                "rollout can proceed in escalation stages."
                if all_pass else
                "NOT READY — at least one criterion failing. Inspect anomalies "
                "before considering Phase 2."
            ),
        },
    }


def _us_snapshot_to_dict(s: US_MetricsSnapshot) -> Dict[str, Any]:
    return {
        "recv": s.recv,
        "applied": s.applied,
        "enrich": s.enrich,
        "stale": s.stale,
        "regress": s.regress,
        "unknown": s.unknown,
        "parse_fail": s.parse_fail,
        "drop": s.drop,
        "orphans_buf": s.orphans_buf,
        "orphans_swept": s.orphans_swept,
        "reconnects": s.reconnects,
        "sm_size": s.sm_size,
        "orphan_size": s.orphan_size,
    }


def _ob_snapshot_to_dict(s: OB_MetricsSnapshot) -> Dict[str, Any]:
    return {
        "counts": s.counts,
        "freshness_s": s.freshness_s,
        "parse_fail": s.parse_fail,
        "reconnects": s.reconnects,
    }


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}min"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


# ---------------------------------------------------------------------------
# Pretty-print
# ---------------------------------------------------------------------------

def render(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    win = report["window"]
    lines.append("=" * 68)
    lines.append(" Phase 1A — Redis shadow run analysis")
    lines.append("=" * 68)
    lines.append(
        f"Window:   {win['start_utc']}  →  {win['end_utc']}  "
        f"({win['duration_human']})"
    )

    su = report["startup"]
    if su["enabled"]:
        e = su["enabled"]
        lines.append(
            f"Enabled:  host={e['host']} user_id={e['user_id']} "
            f"tls={e['tls']} channel={e['channel']}"
        )
    if su["us_subscribed_to"]:
        lines.append(f"US sub:   {', '.join(su['us_subscribed_to'][:3])}"
                     + ("  ..." if len(su["us_subscribed_to"]) > 3 else ""))
    if su["ob_subscribed_to"]:
        lines.append(f"OB sub:   {', '.join(su['ob_subscribed_to'][:3])}"
                     + ("  ..." if len(su["ob_subscribed_to"]) > 3 else ""))

    # User stream
    lines.append("")
    lines.append("USER-STREAM")
    lines.append("-" * 68)
    us = report["user_stream_cumulative"]["latest"]
    if us is not None:
        lines.append(
            f"  Total received:       {us['recv']:>6}    "
            f"Applied:        {us['applied']:>6}   Enrich: {us['enrich']:>6}"
        )
        lines.append(
            f"  Stale (rejected):     {us['stale']:>6}    "
            f"Regression:     {us['regress']:>6}   Unknown: {us['unknown']:>6}"
        )
        lines.append(
            f"  Parse failures:       {us['parse_fail']:>6}    "
            f"Non-JSON drops: {report['anomalies']['non_json_drops']:>6}"
        )
        lines.append(
            f"  Orphan buf (cur):     {us['orphan_size']:>6}    "
            f"Buffered total: {us['orphans_buf']:>6}   "
            f"Swept: {us['orphans_swept']:>6}"
        )
        lines.append(
            f"  State machine size:   {us['sm_size']:>6}    "
            f"Reconnects:     {us['reconnects']:>6}"
        )

    # Terminal events
    te = report["terminal_events"]
    lines.append("")
    lines.append("TERMINAL EVENTS")
    lines.append("-" * 68)
    lines.append(f"  Total:       {te['total']}")
    for typ, count in te["by_type"].items():
        lines.append(f"    {typ:<28} {count}")
    for dec, count in te["by_decision"].items():
        lines.append(f"    decision={dec:<22} {count}")
    pl = te["publish_latency_seconds"]
    if pl["count"]:
        lines.append(
            f"  Publish latency (recv − event_ts): "
            f"p50={_fmt_lat(pl['p50'])}  p99={_fmt_lat(pl['p99'])}  "
            f"max={_fmt_lat(pl['max'])}"
        )

    # Cross-reference
    cr = report["bot_cross_reference"]
    lines.append("")
    lines.append("BOT CROSS-REFERENCE (Redis vs legacy detection)")
    lines.append("-" * 68)
    lines.append(
        f"  Redis terminals: {cr['redis_terminals_observed']}  "
        f"matched by bot: {cr['bot_terminal_matched']}  "
        f"match rate: {cr['match_rate_pct']}%"
    )
    bl = cr["bot_detection_latency_ms_after_redis"]
    if bl["count"]:
        lines.append(
            f"  Bot lags Redis by:  p50={bl['p50']:.0f}ms  "
            f"p99={bl['p99']:.0f}ms  max={bl['max']:.0f}ms  "
            f"(n={bl['count']})"
        )
    if cr["bot_first_count"]:
        bf = cr["bot_first_latency_ms_before_redis"]
        lines.append(
            f"  Bot first (sync path):  p50={bf['p50']:.0f}ms  "
            f"max={bf['max']:.0f}ms  (n={cr['bot_first_count']})"
        )

    # Ghost purges
    gp = report["ghost_purges"]
    lines.append("")
    lines.append("GHOST PURGES")
    lines.append("-" * 68)
    lines.append(f"  Total:                 {gp['total']}")
    lines.append(
        f"  Preventable by Redis:  {gp['preventable_by_redis']}  "
        f"({gp['preventable_pct']}%)"
    )
    if gp["no_create_in_log"]:
        lines.append(
            f"  No create in log:      {gp['no_create_in_log']}  "
            "(pre-existing or not from this strategy)")
    if gp["no_redis_terminal"]:
        lines.append(
            f"  No Redis terminal:     {gp['no_redis_terminal']}  "
            "(Redis didn't see)")
    ws = gp["wasted_seconds_per_purge"]
    if ws["mean"]:
        lines.append(
            f"  Wasted wait per purge: p50={ws['p50']:.1f}s  "
            f"mean={ws['mean']:.1f}s  "
            f"total={ws['total_minutes']:.1f}min"
        )

    # Order book
    ob = report["order_book"]
    lines.append("")
    lines.append("ORDER BOOK")
    lines.append("-" * 68)
    for pair, count in ob["snapshots_seen"].items():
        fs = ob["freshness_s"].get(pair, {})
        line = f"  {pair}: {count} snapshots"
        if fs.get("p50") is not None:
            line += (
                f"  freshness p50={fs['p50']:.1f}s  "
                f"p99={fs['p99']:.1f}s  max={fs['max']:.1f}s"
            )
        lines.append(line)
    lines.append(f"  Silence warnings: {ob['silence_warnings']}")

    # Anomalies (only if any)
    an = report["anomalies"]
    if (an["regression_count"] or an["unknown_cods"] or an["non_json_drops"]):
        lines.append("")
        lines.append("ANOMALIES")
        lines.append("-" * 68)
        if an["regression_count"]:
            lines.append(f"  Regressions rejected:  {an['regression_count']}")
        if an["unknown_cods"]:
            lines.append("  Unknown message_cod:")
            for cod, count in an["unknown_cods"].items():
                lines.append(f"    {cod!r}: {count}")
        if an["non_json_drops"]:
            lines.append(f"  Non-JSON dropped:      {an['non_json_drops']}")

    # Exit criteria
    ec = report["phase2_exit_criteria"]
    lines.append("")
    lines.append("PHASE 2 EXIT CRITERIA")
    lines.append("-" * 68)
    for c in ec["criteria"]:
        mark = "✓" if c["passed"] else "✗"
        target = c.get("target_pct") or c.get("target")
        observed = c.get("observed_pct") or c.get("observed")
        lines.append(
            f"  [{mark}] {c['name']:<55} "
            f"target={target}  got={observed}"
        )
    lines.append("")
    lines.append("  " + ec["recommendation"])
    lines.append("=" * 68)
    return "\n".join(lines)


def _fmt_lat(seconds: Optional[float]) -> str:
    if seconds is None:
        return "?"
    if abs(seconds) < 1:
        return f"{seconds * 1000:.0f}ms"
    return f"{seconds:.2f}s"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_iso_arg(s: str) -> float:
    """Accept either 'YYYY-MM-DD HH:MM:SS' or full ISO with TZ."""
    s = s.strip()
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        d = dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bot-log", type=Path, required=True,
                    help="Hummingbot log file, "
                         "e.g. logs/logs_conf_xemm_lead_lag_sbe.log")
    ap.add_argument("--since", type=str, default=None,
                    help="UTC ISO datetime; ignore log lines before this")
    ap.add_argument("--until", type=str, default=None,
                    help="UTC ISO datetime; ignore log lines after this")
    ap.add_argument("--json-only", action="store_true",
                    help="emit JSON only (no human-readable summary)")
    ap.add_argument("--report", type=Path, default=None,
                    help="JSON report path (default: <bot-log>.shadow.json)")
    args = ap.parse_args()

    if not args.bot_log.exists():
        print(f"ERROR: log not found: {args.bot_log}", file=sys.stderr)
        return 2

    win_start = _parse_iso_arg(args.since) if args.since else None
    win_end = _parse_iso_arg(args.until) if args.until else None

    state = parse_log(args.bot_log, win_start, win_end)
    report = analyse(state)

    out_path = args.report or args.bot_log.with_suffix(args.bot_log.suffix + ".shadow.json")
    out_path.write_text(json.dumps(report, indent=2, default=str))

    if args.json_only:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(render(report))
        print(f"\nFull JSON: {out_path}")
    return 0 if report["phase2_exit_criteria"]["all_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
