"""TTL buffer for orphan Redis events.

Race condition this solves
==========================

When the bot places an order via REST `cmd=buy`, the sequence is:

  1. Bot calls `cmd=buy` (REST POST)
  2. BitPreco accepts, assigns an exchange_order_id, fires the
     `BUY_ORDER_CREATED` event to Redis pub/sub
  3. REST response returns to the bot, carrying the exchange_order_id
  4. Bot maps client_order_id ↔ exchange_order_id internally
  5. Bot's user-stream listener finally sees the Redis event

In a normal world (1) → (2) → (3) → (4) → (5). On a slow REST round
trip OR a fast Redis broadcaster, (5) can arrive BEFORE (4) — the
listener sees an event for an exchange_order_id the connector hasn't
linked yet. The listener can't know which client_order_id (= which
internal `InFlightOrder`) it belongs to.

Without a buffer, those events get dropped — the bot then waits 60 s
for `ghost_controller` to give up (the very waste we're trying to
eliminate). With a short-TTL buffer keyed on exchange_order_id, we
hold the event and replay it the moment the connector registers the
mapping.

Usage
=====

The data source calls ``observe(event)`` for every Redis event whose
exchange_order_id is not yet known. The connector, after a
successful `place_order` REST call that maps client → exchange,
calls ``replay(exchange_order_id)`` to drain any buffered events.

A periodic ``sweep(now_ts)`` (called from the data source's own
loop, not a separate task) evicts entries past ``ttl_seconds``.

This module is intentionally agnostic of the envelope type — it
buffers ``EventEnvelope`` objects but doesn't introspect them.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import EventEnvelope


@dataclass
class _BufferEntry:
    events: List[EventEnvelope] = field(default_factory=list)
    first_seen_ts: float = 0.0


class OrphanEventBuffer:
    """TTL-bounded multimap from exchange_order_id → list of envelopes.

    Bounded both by TTL and by max entry count to avoid unbounded
    growth under pathological conditions (e.g. an upstream bug
    spraying events for unknown IDs).
    """

    def __init__(
        self,
        ttl_seconds: float = CONSTANTS.REDIS_ORPHAN_BUFFER_TTL_SEC,
        max_entries: int = 1000,
        now_fn: Callable[[], float] = time.time,
    ) -> None:
        self._ttl = ttl_seconds
        self._max = max_entries
        self._now = now_fn
        self._entries: Dict[str, _BufferEntry] = {}

    # ------------------------------------------------------------------
    # Mutators
    # ------------------------------------------------------------------

    def observe(self, envelope: EventEnvelope) -> None:
        """Hold the envelope until the connector calls replay() for
        the same exchange_order_id, or until TTL expires."""
        if envelope.order is None:
            return
        xid = envelope.order.exchange_order_id
        entry = self._entries.get(xid)
        if entry is None:
            # Pre-emptive eviction if we're about to grow past the
            # cap. Drops the oldest entry whatsoever — under steady
            # state this only fires when buffer is genuinely
            # overloaded, which is itself a signal worth logging at
            # the call site.
            if len(self._entries) >= self._max:
                self._evict_oldest()
            entry = _BufferEntry(first_seen_ts=self._now())
            self._entries[xid] = entry
        entry.events.append(envelope)

    def replay(self, exchange_order_id: str) -> List[EventEnvelope]:
        """Drain (and remove) all buffered envelopes for
        ``exchange_order_id``. Returns them in insertion order
        (== arrival order)."""
        entry = self._entries.pop(exchange_order_id, None)
        if entry is None:
            return []
        return list(entry.events)

    def sweep(self, now_ts: Optional[float] = None) -> int:
        """Drop entries older than ``ttl_seconds``. Returns the
        number of buckets evicted (NOT the count of dropped events,
        which is usually more interesting for ops — get it from
        ``len(self)`` deltas if you want it)."""
        cutoff = (now_ts if now_ts is not None else self._now()) - self._ttl
        to_drop = [xid for xid, e in self._entries.items()
                   if e.first_seen_ts < cutoff]
        for xid in to_drop:
            del self._entries[xid]
        return len(to_drop)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._entries)

    def total_events(self) -> int:
        return sum(len(e.events) for e in self._entries.values())

    def contains(self, exchange_order_id: str) -> bool:
        return exchange_order_id in self._entries

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _evict_oldest(self) -> None:
        if not self._entries:
            return
        oldest_xid = min(self._entries.items(),
                         key=lambda kv: kv[1].first_seen_ts)[0]
        del self._entries[oldest_xid]
