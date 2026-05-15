"""Normalised event envelope for BitPreco Redis pub/sub messages.

Why this exists
===============

Phase 0 captured the real shape of BitPreco's `update:<idBPBot>`
payloads (see `docs/BITPRECO_REDIS_PROTOCOL.md`). Phase 1A consumes
those payloads in shadow mode; Phase 2 promotes them to the primary
backend. In both phases the connector needs a single normalised
shape it can reason about — so that the rest of the code is agnostic
of which backend (Phoenix WS, Redis pub/sub, REST poll) produced the
event.

This module:

1. Parses a raw Redis JSON payload into a typed ``EventEnvelope``,
   converting BitPreco's BRT (UTC-3) ISO timestamps to UTC epoch
   floats and coercing the type-inconsistent fields (`limited`,
   `programmed`, `canceled`, `percent_fee` flip between str/int
   between events — see FINDINGS.md).
2. Tracks per-order state in an ``OrderStateMachine`` that rejects
   regression (terminal → open) but accepts terminal enrichment
   (a CANCELED after a PARTIAL preserves the executed amount).
3. Enforces timestamp-based ordering as the primary guard, with the
   state machine as defense-in-depth.

It does NOT touch the connector's order tracker, balances or
trading rules. Wiring those is the responsibility of the data
sources that consume the envelope.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Dict, Optional

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS


# ---------------------------------------------------------------------------
# Event types — the wire-format enum from Redis, exposed as a string enum.
# Names match BitPreco's `message_cod` values verbatim so we can route on
# them with no translation.
# ---------------------------------------------------------------------------

class EventType(str, Enum):
    BUY_ORDER_CREATED = "BUY_ORDER_CREATED"
    SELL_ORDER_CREATED = "SELL_ORDER_CREATED"
    ORDER_PARTIALLY_EXECUTED = "ORDER_PARTIALLY_EXECUTED"
    ORDER_FULLY_EXECUTED = "ORDER_FULLY_EXECUTED"
    ORDER_CANCELED = "ORDER_CANCELED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_str(cls, value: Optional[str]) -> "EventType":
        if not value:
            return cls.UNKNOWN
        try:
            return cls(value)
        except ValueError:
            return cls.UNKNOWN


# ---------------------------------------------------------------------------
# Per-order lifecycle state — modelled separately from EventType because
# multiple events can map to the same state (CREATED → OPEN, both PARTIAL
# and FULL fills land us in different terminal states).
# ---------------------------------------------------------------------------

class OrderLifecycle(str, Enum):
    UNKNOWN = "UNKNOWN"
    OPEN = "OPEN"                # CREATED received, no terminal yet
    PARTIAL = "PARTIAL"          # PARTIALLY_EXECUTED received
    FILLED = "FILLED"            # FULLY_EXECUTED received (terminal)
    CANCELED = "CANCELED"        # CANCELED received (terminal)


_TERMINAL_STATES = {OrderLifecycle.FILLED, OrderLifecycle.CANCELED}


# ---------------------------------------------------------------------------
# Helpers — defensive coercion of BitPreco's loose JSON shapes.
# ---------------------------------------------------------------------------

def _to_decimal(value: Any) -> Optional[Decimal]:
    """Accept str, int, float, None, Decimal. Return Decimal or None
    (never raise — bad data is dropped, not propagated)."""
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        # Cast through str so that float 0.0002 doesn't pick up
        # binary-representation noise.
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _to_int(value: Any) -> Optional[int]:
    """Accept ``"1"`` / ``1`` / ``1.0`` / None. Return int or None."""
    if value is None or value == "":
        return None
    try:
        # bool is an int subclass; explicit cast keeps the semantics.
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        return int(str(value))
    except (ValueError, TypeError):
        return None


def parse_bitpreco_timestamp(ts_str: Optional[str]) -> Optional[float]:
    """BitPreco timestamps are ``YYYY-MM-DD HH:MM:SS[.ffffff]`` in
    BRT (UTC-3). Return a unix epoch float in UTC, or None on
    parse failure.

    Critical: BitPreco does NOT include a timezone marker. We assume
    BRT and apply ``CONSTANTS.BITPRECO_TZ_OFFSET_SEC``. If they ever
    switch their fleet timezone, this needs updating.
    """
    if not isinstance(ts_str, str) or not ts_str:
        return None
    try:
        if "." in ts_str:
            naive = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
        else:
            naive = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    # Treat the naive datetime as if it were UTC, then subtract the
    # BRT offset to get the true UTC instant.
    pseudo_utc = naive.replace(tzinfo=dt.timezone.utc).timestamp()
    return pseudo_utc - CONSTANTS.BITPRECO_TZ_OFFSET_SEC


# ---------------------------------------------------------------------------
# The normalised envelope.
# ---------------------------------------------------------------------------

@dataclass
class OrderInfo:
    """A flattened, type-coerced order record."""
    exchange_order_id: str
    market: str
    side: str                      # "BUY" or "SELL"
    status: str                    # raw BitPreco status ("EMPTY", "FILLED", ...)
    amount: Optional[Decimal]      # base-asset amount requested
    price: Optional[Decimal]
    exec_amount: Optional[Decimal] = None
    cost: Optional[Decimal] = None
    fee: Optional[Decimal] = None
    canceled_flag: Optional[int] = None
    limited_flag: Optional[int] = None
    programmed_flag: Optional[int] = None
    percent_fee: Optional[Decimal] = None
    event_ts: Optional[float] = None  # order.time_stamp in UTC


@dataclass
class BalanceSnapshot:
    """Per-currency available/locked balance from a single payload."""
    available: Dict[str, Decimal]
    locked: Dict[str, Decimal]
    event_ts: Optional[float] = None  # balance.utimestamp in UTC


@dataclass
class EventEnvelope:
    """Normalised wire envelope. Wraps a single Redis pub/sub message.

    Consumers route on ``event_type`` and read ``order`` / ``balance``
    as needed. ``recv_ts`` is our wall clock when we received the
    message (NOT BitPreco's timestamp). ``event_ts`` is the most
    authoritative server-side timestamp we could extract — see
    docstring of ``parse_envelope``.
    """
    event_type: EventType
    order: Optional[OrderInfo]
    balance: Optional[BalanceSnapshot]
    recv_ts: float
    event_ts: Optional[float]
    source: str = "redis"          # for log breadcrumbs
    raw: Optional[Dict[str, Any]] = field(default=None, repr=False)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _parse_balance(payload: Optional[Dict[str, Any]]) -> Optional[BalanceSnapshot]:
    """The ``balance`` block has per-currency `<CCY>` and `<CCY>_locked`
    pairs, plus `utimestamp`, `timestamp`, `success` fields. We treat
    every non-meta key as a currency."""
    if not isinstance(payload, dict):
        return None
    META_KEYS = {"success", "utimestamp", "timestamp"}
    available: Dict[str, Decimal] = {}
    locked: Dict[str, Decimal] = {}
    for k, v in payload.items():
        if k in META_KEYS:
            continue
        if not isinstance(k, str):
            continue
        amount = _to_decimal(v)
        if amount is None:
            continue
        if k.endswith("_locked"):
            ccy = k[: -len("_locked")]
            if ccy:
                locked[ccy] = amount
        else:
            available[k] = amount
    event_ts = parse_bitpreco_timestamp(payload.get("utimestamp")) or \
        parse_bitpreco_timestamp(payload.get("timestamp"))
    return BalanceSnapshot(available=available, locked=locked, event_ts=event_ts)


def _parse_order(payload: Optional[Dict[str, Any]],
                 fallback_xid: Optional[str]) -> Optional[OrderInfo]:
    if not isinstance(payload, dict):
        return None
    xid = payload.get("id") or fallback_xid
    if xid is None:
        return None
    return OrderInfo(
        exchange_order_id=str(xid),
        market=str(payload.get("market") or ""),
        side=str(payload.get("type") or "").upper(),
        status=str(payload.get("status") or ""),
        amount=_to_decimal(payload.get("amount")),
        price=_to_decimal(payload.get("price")),
        exec_amount=_to_decimal(payload.get("exec_amount")),
        cost=_to_decimal(payload.get("cost")),
        fee=_to_decimal(payload.get("fee")),
        canceled_flag=_to_int(payload.get("canceled")),
        limited_flag=_to_int(payload.get("limited")),
        programmed_flag=_to_int(payload.get("programmed")),
        percent_fee=_to_decimal(payload.get("percent_fee")),
        event_ts=parse_bitpreco_timestamp(payload.get("time_stamp")),
    )


def parse_envelope(payload: Dict[str, Any], recv_ts: float,
                   source: str = "redis") -> Optional[EventEnvelope]:
    """Turn a Redis JSON payload into a typed envelope.

    Returns None if the payload is malformed beyond useful recovery
    (no ``message_cod``, no order id at all). The caller logs and
    drops; we do not raise.

    ``event_ts`` priority (most → least authoritative):
      1. ``balance.utimestamp`` (microsecond resolution, always present
         in our 30 min capture, monotonic per order_id)
      2. ``order.time_stamp`` (second resolution)
      3. None — fall back to ``recv_ts`` at the call site
    """
    if not isinstance(payload, dict):
        return None
    cod = payload.get("message_cod")
    event_type = EventType.from_str(cod) if isinstance(cod, str) else EventType.UNKNOWN

    fallback_xid = payload.get("order_id")
    fallback_xid_str = str(fallback_xid) if fallback_xid is not None else None
    order = _parse_order(payload.get("order"), fallback_xid_str)
    balance = _parse_balance(payload.get("balance"))

    # If neither order nor balance parsed, we have nothing to act on.
    if order is None and balance is None and event_type == EventType.UNKNOWN:
        return None

    event_ts: Optional[float] = None
    if balance is not None and balance.event_ts is not None:
        event_ts = balance.event_ts
    elif order is not None and order.event_ts is not None:
        event_ts = order.event_ts

    return EventEnvelope(
        event_type=event_type,
        order=order,
        balance=balance,
        recv_ts=recv_ts,
        event_ts=event_ts,
        source=source,
        raw=payload,
    )


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

_EVENT_TO_TARGET_STATE = {
    EventType.BUY_ORDER_CREATED:        OrderLifecycle.OPEN,
    EventType.SELL_ORDER_CREATED:       OrderLifecycle.OPEN,
    EventType.ORDER_PARTIALLY_EXECUTED: OrderLifecycle.PARTIAL,
    EventType.ORDER_FULLY_EXECUTED:     OrderLifecycle.FILLED,
    EventType.ORDER_CANCELED:           OrderLifecycle.CANCELED,
}


@dataclass
class _OrderRecord:
    state: OrderLifecycle = OrderLifecycle.UNKNOWN
    last_event_ts: Optional[float] = None
    last_exec_amount: Optional[Decimal] = None


class StateDecision(str, Enum):
    """What the state machine recommends for a given event."""
    APPLY = "APPLY"            # apply mutation, advance state
    APPLY_ENRICH = "APPLY_ENRICH"  # in terminal state but accept enrichment
    SKIP_STALE = "SKIP_STALE"  # event_ts <= last_event_ts; drop silently
    SKIP_REGRESSION = "SKIP_REGRESSION"  # would move terminal → non-terminal
    SKIP_UNKNOWN = "SKIP_UNKNOWN"        # unknown event type


class OrderStateMachine:
    """Tracks per-order lifecycle + last-seen timestamp.

    Thread-affinity: NOT thread-safe. Use one instance per asyncio
    consumer (i.e. one per data-source instance). The bot's
    user-stream task is single-threaded so this is fine.

    Behaviour rules (see plan §"Ordering / state machine"):

    1. **Timestamp guard first.** If the new event's `event_ts` is
       strictly less than what we already have for this order, drop
       (SKIP_STALE).
    2. **Terminal regression rejected.** Once an order is FILLED or
       CANCELED, a subsequent non-terminal event (e.g. a stale
       CREATED) is rejected with SKIP_REGRESSION even if its
       timestamp is newer (would imply server bug).
    3. **Terminal enrichment accepted.** A second terminal event for
       a FILLED/CANCELED order (e.g. CANCELED after PARTIAL → records
       final exec_amount) is APPLY_ENRICH — caller may use it to
       enrich fee / exec_amount, but must not regress status.
    4. **Equal timestamps** are not stale — accepted as APPLY. Tie-
       break (recv_ts) lives at the caller.
    """

    def __init__(self) -> None:
        self._records: Dict[str, _OrderRecord] = {}

    def decide(self, envelope: EventEnvelope) -> StateDecision:
        order = envelope.order
        if order is None:
            return StateDecision.SKIP_UNKNOWN
        xid = order.exchange_order_id
        rec = self._records.get(xid)
        target = _EVENT_TO_TARGET_STATE.get(envelope.event_type)
        if target is None:
            return StateDecision.SKIP_UNKNOWN

        # 1. Timestamp guard. We prefer order.event_ts (per-order
        # authoritative) but fall back to envelope.event_ts (balance
        # timestamp) if order doesn't carry one.
        new_ts = order.event_ts if order.event_ts is not None else envelope.event_ts
        if rec is not None and rec.last_event_ts is not None and new_ts is not None:
            if new_ts < rec.last_event_ts:
                return StateDecision.SKIP_STALE

        # 2. Regression guard.
        if rec is not None and rec.state in _TERMINAL_STATES:
            if target not in _TERMINAL_STATES:
                return StateDecision.SKIP_REGRESSION
            # Both terminal: this is enrichment (e.g. CANCELED after
            # a PARTIAL fill, or another FULL event for the same order).
            return StateDecision.APPLY_ENRICH

        return StateDecision.APPLY

    def apply(self, envelope: EventEnvelope, decision: StateDecision) -> None:
        """Update the internal record. Caller must have inspected
        ``decision`` and either dropped (SKIP_*) or proceeded to act
        on the envelope before calling apply()."""
        if decision in (StateDecision.SKIP_STALE,
                        StateDecision.SKIP_REGRESSION,
                        StateDecision.SKIP_UNKNOWN):
            return
        order = envelope.order
        if order is None:
            return
        target = _EVENT_TO_TARGET_STATE.get(envelope.event_type)
        if target is None:
            return
        rec = self._records.setdefault(order.exchange_order_id, _OrderRecord())
        # Don't downgrade state on enrichment.
        if decision != StateDecision.APPLY_ENRICH:
            rec.state = target
        new_ts = order.event_ts if order.event_ts is not None else envelope.event_ts
        if new_ts is not None and (rec.last_event_ts is None or new_ts >= rec.last_event_ts):
            rec.last_event_ts = new_ts
        if order.exec_amount is not None:
            if rec.last_exec_amount is None or order.exec_amount > rec.last_exec_amount:
                rec.last_exec_amount = order.exec_amount

    def state_of(self, exchange_order_id: str) -> OrderLifecycle:
        rec = self._records.get(exchange_order_id)
        return rec.state if rec else OrderLifecycle.UNKNOWN

    def evict_older_than(self, cutoff_ts: float) -> int:
        """Drop records whose last_event_ts is older than ``cutoff_ts``.
        Returns the number of evictions. Caller schedules this on a
        timer; we don't run our own loop."""
        to_drop = [xid for xid, rec in self._records.items()
                   if rec.last_event_ts is not None
                   and rec.last_event_ts < cutoff_ts]
        for xid in to_drop:
            del self._records[xid]
        return len(to_drop)

    def __len__(self) -> int:
        return len(self._records)
