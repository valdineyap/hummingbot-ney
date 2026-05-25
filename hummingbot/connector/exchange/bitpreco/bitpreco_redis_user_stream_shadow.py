"""Shadow observer for the BitPreco `update:<idBPBot>` Redis channel.

Phase 1A of the BitPreco Redis migration. This task runs in
**parallel** with the legacy Phoenix/REST user-stream and exercises
the entire production pipeline (subscribe, parse, normalise, state
machine) — except for the final step that would mutate the bot's
`InFlightOrder` tracker.

The point is to:

1. Validate the Redis backend on real production traffic without
   any risk to the running strategy.
2. Generate metrics that compare what Redis sees to what the legacy
   bot sees (logged as structured ``[redis_shadow]`` lines, post-
   processable by ``tools/redis_probe/06_gain_analysis.py``).
3. Surface parse errors, race conditions, orphan events, and
   reconnect handling well before any of it touches order state.

Wiring
======

The connector's ``start_network()`` spawns this observer as a
background task when ``bitpreco_redis_shadow_mode=True``. The task
is cancelled in ``stop_network()``. Cancellation is cooperative —
we catch ``CancelledError`` and exit cleanly.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import (
    EventType,
    OrderStateMachine,
    StateDecision,
    parse_envelope,
)
from hummingbot.connector.exchange.bitpreco.bitpreco_orphan_buffer import OrphanEventBuffer
from hummingbot.connector.exchange.bitpreco.bitpreco_redis_client import (
    RedisBackendConfig,
    RedisConnectionFactory,
)

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange

LOG = logging.getLogger(__name__)


# Reconnect backoff: cap at 30s. The plan calls for graceful
# degradation (REST fallback) on prolonged silence; in shadow mode
# we just log loudly and keep trying.
_RECONNECT_BACKOFF_INITIAL_SEC = 1.0
_RECONNECT_BACKOFF_MAX_SEC = 30.0


class BitprecoRedisUserStreamShadow:
    """Subscribes to ``update:<idBPBot>`` and emits comparison metrics.

    Stateless from the bot's perspective: instances of this class
    don't push to the user-stream queue, don't touch
    ``InFlightOrder``, don't refresh balances. They only:

    - exercise the parse + state-machine + orphan-buffer code paths
    - emit ``[redis_shadow]`` log lines for offline analysis
    - keep the orphan buffer swept on a timer
    """

    def __init__(
        self,
        connector: "BitprecoExchange",
        factory: RedisConnectionFactory,
    ) -> None:
        self._connector = connector
        self._factory = factory
        self._config: RedisBackendConfig = factory.config
        self._state = OrderStateMachine()
        self._orphans = OrphanEventBuffer()
        # Metrics counters — periodically snapshotted to log
        self._counts = {
            "messages_received":   0,
            "parse_failures":      0,
            "envelope_dropped":    0,
            "applied":             0,
            "applied_enrich":      0,
            "skipped_stale":       0,
            "skipped_regression":  0,
            "skipped_unknown":     0,
            "orphans_buffered":    0,
            "orphans_swept":       0,
        }
        self._reconnect_count = 0
        self._last_metric_log_ts = 0.0
        self._metric_log_interval_sec = 60.0

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Outer loop: reconnect on transport-level errors. Each
        iteration tries to establish a fresh pub/sub and consume
        until something throws."""
        backoff = _RECONNECT_BACKOFF_INITIAL_SEC
        while True:
            try:
                await self._connect_and_consume()
                # Clean exit (subscription returned None forever?). Reset
                # backoff and try again rather than spin.
                backoff = _RECONNECT_BACKOFF_INITIAL_SEC
            except asyncio.CancelledError:
                LOG.info("[redis_shadow] user-stream observer cancelled, "
                         "shutting down cleanly")
                return
            except Exception as exc:
                self._reconnect_count += 1
                LOG.warning("[redis_shadow] user-stream observer error: "
                            "%s (reconnect #%d, backoff=%.1fs)",
                            exc, self._reconnect_count, backoff)
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    return
                backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_SEC)

    # ------------------------------------------------------------------
    # Inner consumer loop
    # ------------------------------------------------------------------

    async def _connect_and_consume(self) -> None:
        client = await self._factory.create_pubsub_connection()
        pubsub = client.pubsub()
        channel = self._config.update_channel
        try:
            await pubsub.subscribe(channel)
            LOG.info("[redis_shadow] subscribed to %s", channel)
            await self._consume_loop(pubsub)
        finally:
            try:
                await pubsub.aclose()
            except Exception:
                pass
            try:
                await client.aclose()
            except Exception:
                pass

    async def _consume_loop(self, pubsub: Any) -> None:
        last_sweep = time.time()
        sweep_interval = max(30.0, CONSTANTS.REDIS_ORPHAN_BUFFER_TTL_SEC / 2)
        while True:
            # Tight timeout so cancellation is responsive even when
            # the channel is quiet (silence is normal for update:).
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )
            now = time.time()
            if msg is not None:
                self._handle_message(msg, recv_ts=now)
            if now - last_sweep >= sweep_interval:
                evicted = self._orphans.sweep(now)
                if evicted:
                    self._counts["orphans_swept"] += evicted
                last_sweep = now
            self._maybe_log_metrics(now)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _handle_message(self, msg: Any, recv_ts: float) -> None:
        self._counts["messages_received"] += 1
        data = msg.get("data") if isinstance(msg, dict) else None
        if isinstance(data, (bytes, bytearray)):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                self._counts["parse_failures"] += 1
                return
        if isinstance(data, str):
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                self._counts["parse_failures"] += 1
                LOG.warning("[redis_shadow] non-JSON message dropped: %r",
                            data[:200])
                return
        elif isinstance(data, dict):
            payload = data
        else:
            self._counts["parse_failures"] += 1
            return

        envelope = parse_envelope(payload, recv_ts=recv_ts)
        if envelope is None:
            self._counts["envelope_dropped"] += 1
            return

        decision = self._state.decide(envelope)
        # Track stats
        if decision == StateDecision.APPLY:
            self._counts["applied"] += 1
        elif decision == StateDecision.APPLY_ENRICH:
            self._counts["applied_enrich"] += 1
        elif decision == StateDecision.SKIP_STALE:
            self._counts["skipped_stale"] += 1
        elif decision == StateDecision.SKIP_REGRESSION:
            self._counts["skipped_regression"] += 1
            self._log_regression(envelope)
        elif decision == StateDecision.SKIP_UNKNOWN:
            self._counts["skipped_unknown"] += 1
            if envelope.event_type == EventType.UNKNOWN:
                LOG.warning("[redis_shadow] unknown message_cod for order_id=%s "
                            "raw_cod=%r — gating Phase 2 for this event type",
                            envelope.order.exchange_order_id if envelope.order
                            else "?",
                            payload.get("message_cod"))

        self._state.apply(envelope, decision)

        # Orphan buffer: if connector hasn't mapped this exchange_order_id
        # yet, hold the event. In shadow mode the connector continues
        # using the legacy path so we won't actively replay — this
        # exercises the data path under realistic conditions and
        # validates that the buffer doesn't leak memory.
        if envelope.order is not None and decision == StateDecision.APPLY \
                and envelope.event_type in (EventType.BUY_ORDER_CREATED,
                                            EventType.SELL_ORDER_CREATED):
            xid = envelope.order.exchange_order_id
            if not self._connector_knows(xid):
                self._orphans.observe(envelope)
                self._counts["orphans_buffered"] += 1

        # Latency breadcrumb for downstream gain analysis. Only log
        # terminal events (the ones worth comparing against bot's
        # OrderFilledEvent / OrderCancelledEvent in the offline
        # post-processor). Per-event log is fine at <1 msg/sec.
        if envelope.event_type in (
            EventType.ORDER_FULLY_EXECUTED,
            EventType.ORDER_PARTIALLY_EXECUTED,
            EventType.ORDER_CANCELED,
        ):
            o = envelope.order
            xid = o.exchange_order_id if o else "?"
            event_ts_str = (f"{envelope.event_ts:.3f}"
                            if envelope.event_ts is not None else "?")
            LOG.info(
                "[redis_shadow] terminal "
                "type=%s xid=%s decision=%s recv_ts=%.3f event_ts=%s "
                "redis_minus_event=%s",
                envelope.event_type.value, xid, decision.value, recv_ts,
                event_ts_str,
                f"{recv_ts - envelope.event_ts:.3f}" if envelope.event_ts
                else "?",
            )

    # ------------------------------------------------------------------
    # Bot-state introspection (read-only — never mutates the connector)
    # ------------------------------------------------------------------

    def _connector_knows(self, exchange_order_id: str) -> bool:
        """Has the connector already mapped this exchange_order_id
        to a client_order_id? We use the order tracker's in_flight
        dict — the connector's standard reverse map."""
        tracker = getattr(self._connector, "_order_tracker", None)
        if tracker is None:
            return False
        try:
            for in_flight in tracker.active_orders.values():
                if in_flight.exchange_order_id == exchange_order_id:
                    return True
        except Exception:
            # Connector internals can change; never let lookup
            # failure crash the shadow loop.
            pass
        return False

    # ------------------------------------------------------------------
    # Metrics + diagnostics
    # ------------------------------------------------------------------

    def _maybe_log_metrics(self, now: float) -> None:
        if now - self._last_metric_log_ts < self._metric_log_interval_sec:
            return
        self._last_metric_log_ts = now
        LOG.info(
            "[redis_shadow] metrics recv=%d parse_fail=%d drop=%d "
            "applied=%d enrich=%d stale=%d regress=%d unknown_cod=%d "
            "orphans_buf=%d orphans_swept=%d reconnects=%d "
            "sm_size=%d orphan_size=%d",
            self._counts["messages_received"],
            self._counts["parse_failures"],
            self._counts["envelope_dropped"],
            self._counts["applied"],
            self._counts["applied_enrich"],
            self._counts["skipped_stale"],
            self._counts["skipped_regression"],
            self._counts["skipped_unknown"],
            self._counts["orphans_buffered"],
            self._counts["orphans_swept"],
            self._reconnect_count,
            len(self._state),
            len(self._orphans),
        )

    def _log_regression(self, envelope: Any) -> None:
        order = envelope.order
        xid = order.exchange_order_id if order else "?"
        current = self._state.state_of(xid).value if order else "?"
        LOG.warning(
            "[redis_shadow] REGRESSION rejected xid=%s current_state=%s "
            "incoming=%s recv_ts=%.3f event_ts=%s",
            xid, current, envelope.event_type.value, envelope.recv_ts,
            f"{envelope.event_ts:.3f}" if envelope.event_ts else "?",
        )

    # ------------------------------------------------------------------
    # Test/debug introspection
    # ------------------------------------------------------------------

    def snapshot_counts(self) -> dict:
        """Return a copy of the counts dict. Used by tests."""
        snap = dict(self._counts)
        snap["reconnects"] = self._reconnect_count
        snap["state_machine_size"] = len(self._state)
        snap["orphan_buffer_size"] = len(self._orphans)
        return snap

    @property
    def state_machine(self) -> OrderStateMachine:
        return self._state

    @property
    def orphan_buffer(self) -> OrphanEventBuffer:
        return self._orphans

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    @property
    def metric_log_interval_sec(self) -> float:
        return self._metric_log_interval_sec

    @metric_log_interval_sec.setter
    def metric_log_interval_sec(self, value: float) -> None:
        self._metric_log_interval_sec = float(value)
