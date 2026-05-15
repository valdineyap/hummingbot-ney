"""Primary (non-shadow) BitPreco user-stream data source backed by Redis pub/sub.

Used when ``bitpreco_data_backend=redis``. Replaces the Phoenix WS
data source — events flow into the connector's user-stream queue,
the connector's listener routes them to the order tracker + balance
refresh.

Design contract
===============

- Subscribes to ``update:<idBPBot>`` on a dedicated Redis pub/sub
  connection (separate from orderbook's, so orderbook backpressure
  can't delay fill events).
- Every Redis message is parsed via ``parse_envelope`` (same code
  path Phase 1A shadow validated against real traffic for 90 min
  with zero anomalies).
- Ordering guard: events pass through ``OrderStateMachine`` — stale
  events (event_ts < last seen for that order_id) are dropped;
  terminal regressions (terminal → non-terminal) are dropped.
- Orphan buffer: events for an exchange_order_id the connector
  hasn't mapped yet (race between place_order REST return and the
  Redis publish) are buffered with TTL. When the connector calls
  ``replay_orphans(xid)`` after mapping, the buffered events drain
  in arrival order.
- Output: ``EventEnvelope`` objects pushed to the framework's
  user-stream queue. The connector's
  ``_user_stream_event_listener`` dispatches on
  ``isinstance(msg, EventEnvelope)`` for the Redis path.

Reconnect handling
==================

- Network errors surface as exceptions in the subscribe/get_message
  path; we catch, sleep with exponential backoff (cap 30 s), and
  retry the whole subscribe cycle.
- After reconnect, we trigger a REST catch-up on the connector
  (refresh all in-flight orders + force balance refresh) — Redis
  pub/sub has no replay so messages published during the gap are
  lost, REST is the authoritative recovery path.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any, List, Optional

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import (
    EventEnvelope,
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
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange

LOG = logging.getLogger(__name__)

_RECONNECT_BACKOFF_INITIAL_SEC = 1.0
_RECONNECT_BACKOFF_MAX_SEC = 30.0


class BitprecoRedisUserStream(UserStreamTrackerDataSource):
    """User-stream data source that subscribes to BitPreco's Redis
    ``update:<idBPBot>`` channel and pushes normalised
    :class:`EventEnvelope` objects to the framework queue.

    Lifecycle is owned by the framework's ``UserStreamTracker``,
    same as the legacy Phoenix data source — caller does
    ``listen_for_user_stream(queue)`` and we run until cancelled.
    """

    def __init__(
        self,
        connector: "BitprecoExchange",
        factory: RedisConnectionFactory,
    ) -> None:
        super().__init__()
        self._connector = connector
        self._factory = factory
        self._config: RedisBackendConfig = factory.config
        self._state = OrderStateMachine()
        self._orphans = OrphanEventBuffer()
        self._last_recv_ts: float = 0.0
        self._reconnect_count: int = 0
        # Stashed when listen_for_user_stream starts so replay_orphans
        # can push without taking the queue as a parameter.
        self._output_queue: Optional[asyncio.Queue] = None
        self._last_sweep_ts: float = 0.0
        # Periodic info log to confirm liveness in long-running deployments.
        # Same cadence as the shadow observer for symmetric output.
        self._last_health_log_ts: float = 0.0
        self._health_log_interval_sec: float = 60.0
        # Metrics — same shape as shadow observer for analyser reuse.
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
            "orphans_replayed":    0,
            "orphans_swept":       0,
            "queue_pushes":        0,
        }

    # ------------------------------------------------------------------
    # UserStreamTrackerDataSource contract
    # ------------------------------------------------------------------

    @property
    def last_recv_time(self) -> float:
        """The framework reads this to gauge stream health. Return
        seconds-since-epoch of the last message we received."""
        return self._last_recv_ts

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        """Entry point — runs until cancelled. Outer loop reconnects
        on transport errors with exponential backoff."""
        # Stash the queue so replay_orphans() can use it without the
        # connector having to pass it in.
        self._output_queue = output
        backoff = _RECONNECT_BACKOFF_INITIAL_SEC
        while True:
            try:
                await self._connect_and_consume(output)
                backoff = _RECONNECT_BACKOFF_INITIAL_SEC
            except asyncio.CancelledError:
                LOG.info("[redis_us] cancelled; exiting cleanly")
                raise
            except Exception as exc:
                self._reconnect_count += 1
                LOG.warning(
                    "[redis_us] error: %s (reconnect #%d, backoff=%.1fs)",
                    exc, self._reconnect_count, backoff,
                )
                try:
                    await self._sleep(backoff)
                except asyncio.CancelledError:
                    raise
                backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_SEC)

    # ------------------------------------------------------------------
    # Inner consumer loop
    # ------------------------------------------------------------------

    async def _connect_and_consume(self, output: asyncio.Queue) -> None:
        client = await self._factory.create_pubsub_connection()
        pubsub = client.pubsub()
        channel = self._config.update_channel
        try:
            await pubsub.subscribe(channel)
            LOG.info("[redis_us] subscribed to %s (reconnect=#%d)",
                     channel, self._reconnect_count)
            # Post-(re)connect catch-up: in-flight orders may have
            # changed state during our absence. The connector
            # implements the actual REST sync; we just trigger it.
            self._trigger_post_connect_catch_up()
            await self._consume_loop(pubsub, output)
        finally:
            try:
                await pubsub.aclose()
            except Exception:
                pass
            try:
                await client.aclose()
            except Exception:
                pass

    async def _consume_loop(self, pubsub: Any, output: asyncio.Queue) -> None:
        sweep_interval = max(30.0, CONSTANTS.REDIS_ORPHAN_BUFFER_TTL_SEC / 2)
        self._last_sweep_ts = time.time()
        # Liveness ping: when this data source runs it counts as
        # "subscribed and reachable" — the framework's readiness gate
        # checks ``last_recv_time`` and would mark us stale during
        # quiet stretches (the ``update:<id>`` channel is silent
        # whenever the bot has no order activity, e.g. before the
        # first quote refresh after start). Unlike Phoenix WS, Redis
        # pub/sub has no application-level heartbeat. Treat every
        # successful ``get_message`` round-trip (even a timeout
        # return) as proof the connection is alive.
        self._last_recv_ts = time.time()
        while True:
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )
            now = time.time()
            # See note above — advance even on timeout. If the socket
            # were dead, get_message would raise, not return None.
            self._last_recv_ts = now
            if msg is not None:
                self._counts["messages_received_real"] = (
                    self._counts.get("messages_received_real", 0) + 1)
                self._handle_message(msg, recv_ts=now, output=output)
            # Periodic housekeeping (cheap; runs once per second of
            # idle time at most).
            if now - self._last_sweep_ts >= sweep_interval:
                evicted = self._orphans.sweep(now)
                if evicted:
                    self._counts["orphans_swept"] += evicted
                self._last_sweep_ts = now
            self._maybe_log_health(now)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _handle_message(
        self,
        msg: Any,
        recv_ts: float,
        output: asyncio.Queue,
    ) -> None:
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
                LOG.warning("[redis_us] non-JSON dropped: %r", data[:200])
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
        if decision == StateDecision.APPLY:
            self._counts["applied"] += 1
        elif decision == StateDecision.APPLY_ENRICH:
            self._counts["applied_enrich"] += 1
        elif decision == StateDecision.SKIP_STALE:
            self._counts["skipped_stale"] += 1
            return
        elif decision == StateDecision.SKIP_REGRESSION:
            self._counts["skipped_regression"] += 1
            LOG.warning(
                "[redis_us] REGRESSION rejected xid=%s incoming=%s",
                envelope.order.exchange_order_id if envelope.order else "?",
                envelope.event_type.value,
            )
            return
        elif decision == StateDecision.SKIP_UNKNOWN:
            self._counts["skipped_unknown"] += 1
            if envelope.event_type == EventType.UNKNOWN:
                LOG.warning(
                    "[redis_us] unknown message_cod payload=%r — dropping",
                    payload.get("message_cod"))
            return

        # APPLY or APPLY_ENRICH: advance the state machine, then
        # decide whether to push to the queue immediately or
        # buffer as orphan.
        self._state.apply(envelope, decision)

        if envelope.order is not None and decision == StateDecision.APPLY \
                and envelope.event_type in (EventType.BUY_ORDER_CREATED,
                                            EventType.SELL_ORDER_CREATED):
            xid = envelope.order.exchange_order_id
            if not self._connector_knows(xid):
                # The connector hasn't mapped this xid yet — its
                # place_order REST is still in flight. Hold the
                # event and replay when the connector calls
                # replay_orphans(xid).
                self._orphans.observe(envelope)
                self._counts["orphans_buffered"] += 1
                return

        # Push to the framework queue. The connector's listener
        # dispatches on EventEnvelope and routes accordingly.
        output.put_nowait(envelope)
        self._counts["queue_pushes"] += 1

    # ------------------------------------------------------------------
    # Orphan replay — called by the connector when place_order maps
    # client_order_id ↔ exchange_order_id.
    # ------------------------------------------------------------------

    def replay_orphans(self, exchange_order_id: str) -> List[EventEnvelope]:
        """Drain any buffered envelopes for ``exchange_order_id`` and
        push them to the user-stream queue in arrival order. The queue
        was stashed in :meth:`listen_for_user_stream`. If the data
        source hasn't started yet (e.g. tests), drained events are
        returned but not pushed.

        Returns the drained list.
        """
        drained = self._orphans.replay(exchange_order_id)
        if self._output_queue is not None:
            for envelope in drained:
                self._output_queue.put_nowait(envelope)
                self._counts["queue_pushes"] += 1
                self._counts["orphans_replayed"] += 1
        return drained

    # ------------------------------------------------------------------
    # Bot-state introspection (read-only — never mutates the connector)
    # ------------------------------------------------------------------

    def _connector_knows(self, exchange_order_id: str) -> bool:
        tracker = getattr(self._connector, "_order_tracker", None)
        if tracker is None:
            return False
        try:
            for in_flight in tracker.active_orders.values():
                if in_flight.exchange_order_id == exchange_order_id:
                    return True
        except Exception:
            pass
        return False

    def _trigger_post_connect_catch_up(self) -> None:
        """Ask the connector to REST-sync after a (re)connect.

        Pub/sub has no replay, so we may have missed events while
        disconnected. The connector decides what to fetch (in-flight
        order status + balance). Best-effort — we don't await.
        """
        cb = getattr(self._connector, "_on_redis_reconnect_catchup", None)
        if cb is None:
            return
        try:
            res = cb()
            if asyncio.iscoroutine(res):
                # Connector may return a coroutine; schedule it
                # without blocking the consume loop.
                asyncio.create_task(res)
        except Exception:
            LOG.exception("[redis_us] catch-up callback raised")

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def _maybe_log_health(self, now: float) -> None:
        if now - self._last_health_log_ts < self._health_log_interval_sec:
            return
        self._last_health_log_ts = now
        LOG.info(
            "[redis_us] health recv=%d applied=%d enrich=%d "
            "stale=%d regress=%d unknown_cod=%d parse_fail=%d "
            "orphans_buf=%d replayed=%d swept=%d "
            "queue_pushes=%d sm_size=%d orphan_size=%d reconnects=%d",
            self._counts["messages_received"],
            self._counts["applied"],
            self._counts["applied_enrich"],
            self._counts["skipped_stale"],
            self._counts["skipped_regression"],
            self._counts["skipped_unknown"],
            self._counts["parse_failures"],
            self._counts["orphans_buffered"],
            self._counts["orphans_replayed"],
            self._counts["orphans_swept"],
            self._counts["queue_pushes"],
            len(self._state),
            len(self._orphans),
            self._reconnect_count,
        )

    # ------------------------------------------------------------------
    # Test introspection
    # ------------------------------------------------------------------

    def snapshot_counts(self) -> dict:
        return {
            **self._counts,
            "reconnects": self._reconnect_count,
            "state_machine_size": len(self._state),
            "orphan_buffer_size": len(self._orphans),
        }

    @property
    def state_machine(self) -> OrderStateMachine:
        return self._state

    @property
    def orphan_buffer(self) -> OrphanEventBuffer:
        return self._orphans

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    # ------------------------------------------------------------------
    # Abstract methods we inherit but explicitly don't use — the
    # parent's default implementation assumes WSAssistant which we
    # don't have. Raise to make any accidental invocation loud.
    # ------------------------------------------------------------------

    async def _connected_websocket_assistant(self):  # pragma: no cover
        raise NotImplementedError("Redis data source has no WSAssistant")

    async def _subscribe_channels(self, websocket_assistant):  # pragma: no cover
        raise NotImplementedError("Redis data source has no WSAssistant")
