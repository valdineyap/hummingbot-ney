"""Primary (non-shadow) BitPreco order-book data source backed by Redis pub/sub.

Used when ``bitpreco_data_backend=redis``. Replaces the REST-polling
``BitprecoAPIOrderBookDataSource`` — orderbook snapshots flow from
Redis ``orderbook:<market>`` into the framework's message queue, the
parent class' parser converts them to ``OrderBookMessage`` objects.

Phase 0 verified that the Redis payload is byte-equivalent to the
REST ``/orderbook`` response (11/11 shared fields, 0 redis-only,
0 rest-only — see ``docs/BITPRECO_REDIS_PROTOCOL.md``). We reuse
the parent's ``_parse_order_book_diff_message`` and
``BitprecoOrderBook.diff_message_from_exchange`` verbatim — just
swapping the *source* of the raw dict (Redis vs REST poll).

What we change vs the legacy class:

- ``listen_for_subscriptions`` is overridden to subscribe to
  ``orderbook:<market>`` per trading pair on a dedicated Redis
  connection (separate from the user-stream's, so high-frequency
  snapshots can't delay fill events).
- On each message, construct the same dict shape the legacy class
  puts in ``_message_queue[self._diff_messages_queue_key]``, then
  let the inherited diff parser do its thing.

What we keep from the parent:

- ``_request_order_book_snapshot`` — REST fallback for `force_snapshot`
  paths and reconciliation.
- ``_order_book_snapshot`` — used at startup to seed the book.
- ``_parse_order_book_diff_message`` — the actual parser.
- ``_channel_originating_message``, ``_time``, etc.

Silence handling
================

Redis pub/sub has no replay, so a sustained silence on the orderbook
channel means the bot is operating on a stale book. In Phase 1A
shadow we just logged a warning when silence exceeded the
threshold. For Phase 2 we go further: if no Redis snapshot arrives
within ``REDIS_ORDERBOOK_SILENCE_THRESHOLD_SEC``, we spin up a
REST-poll fallback task that keeps the book fresh until Redis
recovers. Recovery: once Redis publishes consistently for
``REDIS_ORDERBOOK_RECOVERY_STABLE_SEC``, the REST fallback is torn
down.

The fallback is scoped to this class — no surgery in
``BitprecoAPIOrderBookDataSource``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_api_order_book_data_source import (
    BitprecoAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.bitpreco.bitpreco_redis_client import (
    RedisBackendConfig,
    RedisConnectionFactory,
)
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange

LOG = logging.getLogger(__name__)

_RECONNECT_BACKOFF_INITIAL_SEC = 1.0
_RECONNECT_BACKOFF_MAX_SEC = 30.0


class BitprecoRedisOrderBookDataSource(BitprecoAPIOrderBookDataSource):
    """OrderBook data source that streams from Redis ``orderbook:<pair>``.

    Inherits everything from the REST-polling sibling and only
    overrides the subscription mechanism. Snapshots land in the
    same ``_message_queue`` slot the parent's parser already
    consumes, so downstream consumers (book tracker, diff queue)
    don't change.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "BitprecoExchange",
        factory: RedisConnectionFactory,
        api_factory: Optional[WebAssistantsFactory] = None,
        throttler: Optional[AsyncThrottler] = None,
    ) -> None:
        super().__init__(
            trading_pairs=trading_pairs,
            connector=connector,
            api_factory=api_factory,
            throttler=throttler,
        )
        self._factory = factory
        self._config: RedisBackendConfig = factory.config
        # Per-pair freshness tracking
        self._last_recv_per_pair: Dict[str, float] = {}
        self._last_event_ts_per_pair: Dict[str, float] = {}
        self._snapshot_count_per_pair: Dict[str, int] = {p: 0 for p in trading_pairs}
        # Silence-fallback state
        self._rest_fallback_task: Optional[asyncio.Task] = None
        self._rest_fallback_active: bool = False
        self._redis_stable_since: Optional[float] = None
        # Metrics
        self._parse_failures: int = 0
        self._reconnect_count: int = 0
        self._silence_warnings: int = 0
        # Health logging
        self._last_health_log_ts: float = 0.0
        self._health_log_interval_sec: float = 60.0

    # ------------------------------------------------------------------
    # Override: subscribe to Redis instead of polling REST
    # ------------------------------------------------------------------

    async def listen_for_subscriptions(self) -> None:
        """Outer loop: reconnect on transport errors with backoff."""
        backoff = _RECONNECT_BACKOFF_INITIAL_SEC
        while True:
            try:
                await self._connect_and_consume()
                backoff = _RECONNECT_BACKOFF_INITIAL_SEC
            except asyncio.CancelledError:
                LOG.info("[redis_ob] cancelled; exiting cleanly")
                # Cancel any active REST fallback before exit
                await self._stop_rest_fallback()
                raise
            except Exception as exc:
                self._reconnect_count += 1
                LOG.warning(
                    "[redis_ob] error: %s (reconnect #%d, backoff=%.1fs)",
                    exc, self._reconnect_count, backoff,
                )
                # Spin up REST fallback while we're not subscribed,
                # so the book doesn't go cold during reconnect.
                self._start_rest_fallback("redis_disconnected")
                try:
                    await self._sleep(backoff)
                except asyncio.CancelledError:
                    raise
                backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_SEC)

    async def _connect_and_consume(self) -> None:
        if not self._trading_pairs:
            LOG.warning("[redis_ob] no trading_pairs configured; exiting")
            return
        client = await self._factory.create_pubsub_connection()
        pubsub = client.pubsub()
        channels = [self._config.orderbook_channel(p) for p in self._trading_pairs]
        try:
            await pubsub.subscribe(*channels)
            LOG.info(
                "[redis_ob] subscribed to %s (reconnect=#%d)",
                channels, self._reconnect_count,
            )
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
        while True:
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )
            now = time.time()
            if msg is not None:
                self._handle_message(msg, recv_ts=now)
                self._maybe_recover_from_fallback(now)
            self._check_silence(now)
            self._maybe_log_health(now)

    # ------------------------------------------------------------------
    # Message handling — turn Redis JSON into the dict shape the
    # parent's parser expects, then drop on the diff queue.
    # ------------------------------------------------------------------

    def _handle_message(self, msg: Any, recv_ts: float) -> None:
        channel = msg.get("channel") if isinstance(msg, dict) else None
        data = msg.get("data") if isinstance(msg, dict) else None
        if not isinstance(channel, str):
            self._parse_failures += 1
            return
        pair = channel.split(":", 1)[1] if ":" in channel else channel

        if isinstance(data, (bytes, bytearray)):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                self._parse_failures += 1
                return
        if isinstance(data, str):
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                self._parse_failures += 1
                LOG.warning("[redis_ob] non-JSON dropped on %s", pair)
                return
        elif isinstance(data, dict):
            payload = data
        else:
            self._parse_failures += 1
            return

        self._record_snapshot_freshness(pair, recv_ts)

        # Mirror the dict shape the legacy class produces — same
        # parser downstream. Note: we use Redis's `utimestamp` (if
        # present) for `timestamp` to keep per-event ordering, but
        # the parent's parser doesn't strictly require it.
        snapshot = {
            "event": "snapshot",
            "payload": {
                "asks": payload.get("asks"),
                "bids": payload.get("bids"),
            },
            "timestamp": payload.get("utimestamp") or payload.get("timestamp"),
            "topic": f"orderbook:{pair}",
        }
        self._message_queue[self._diff_messages_queue_key].put_nowait(snapshot)

    def _record_snapshot_freshness(self, pair: str, recv_ts: float) -> None:
        self._last_recv_per_pair[pair] = recv_ts
        self._snapshot_count_per_pair[pair] = (
            self._snapshot_count_per_pair.get(pair, 0) + 1)

    # ------------------------------------------------------------------
    # Silence watchdog + REST fallback
    # ------------------------------------------------------------------

    def _check_silence(self, now: float) -> None:
        threshold = CONSTANTS.REDIS_ORDERBOOK_SILENCE_THRESHOLD_SEC
        worst_silence = 0.0
        silent_pair: Optional[str] = None
        for pair in self._trading_pairs:
            last = self._last_recv_per_pair.get(pair)
            if last is None:
                continue
            sil = now - last
            if sil > worst_silence:
                worst_silence = sil
                silent_pair = pair
        if silent_pair is not None and worst_silence > threshold and \
                not self._rest_fallback_active:
            LOG.warning(
                "[redis_ob] silence pair=%s elapsed=%.1fs threshold=%.1fs"
                " → enabling REST fallback",
                silent_pair, worst_silence, threshold,
            )
            self._silence_warnings += 1
            self._start_rest_fallback(f"silence_{silent_pair}_{worst_silence:.0f}s")

    def _maybe_recover_from_fallback(self, now: float) -> None:
        if not self._rest_fallback_active:
            return
        stable_for = CONSTANTS.REDIS_ORDERBOOK_RECOVERY_STABLE_SEC
        if self._redis_stable_since is None:
            self._redis_stable_since = now
            return
        if now - self._redis_stable_since >= stable_for:
            LOG.info(
                "[redis_ob] Redis stable for %.0fs — disabling REST fallback",
                stable_for,
            )
            asyncio.create_task(self._stop_rest_fallback())

    def _start_rest_fallback(self, reason: str) -> None:
        if self._rest_fallback_active and self._rest_fallback_task is not None \
                and not self._rest_fallback_task.done():
            return
        LOG.info("[redis_ob] starting REST fallback (reason=%s)", reason)
        self._rest_fallback_active = True
        self._redis_stable_since = None
        self._rest_fallback_task = asyncio.create_task(self._rest_fallback_loop())

    async def _stop_rest_fallback(self) -> None:
        task = self._rest_fallback_task
        self._rest_fallback_task = None
        self._rest_fallback_active = False
        self._redis_stable_since = None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def _rest_fallback_loop(self) -> None:
        """While active, poll REST every ~500ms (the legacy cadence)
        and drop snapshots into the same queue so the book stays
        fresh during Redis silence/reconnect.
        """
        try:
            while True:
                for pair in self._trading_pairs:
                    try:
                        order_book = await self._request_order_book_snapshot(
                            trading_pair=pair)
                    except Exception as exc:
                        LOG.warning(
                            "[redis_ob] REST fallback fetch failed pair=%s: %s",
                            pair, exc)
                        continue
                    snapshot = {
                        "event": "snapshot",
                        "payload": {
                            "asks": order_book.get("asks"),
                            "bids": order_book.get("bids"),
                        },
                        "timestamp": order_book.get("timestamp"),
                        "topic": f"orderbook:{pair}",
                    }
                    self._message_queue[self._diff_messages_queue_key].put_nowait(snapshot)
                # Mirror the bot's SHORT_POLL_INTERVAL = 3 s but
                # tighter for orderbook (legacy used 500ms).
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            LOG.debug("[redis_ob] REST fallback loop cancelled")
            raise

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def _maybe_log_health(self, now: float) -> None:
        if now - self._last_health_log_ts < self._health_log_interval_sec:
            return
        self._last_health_log_ts = now
        freshness = {}
        for pair in self._trading_pairs:
            last = self._last_recv_per_pair.get(pair)
            freshness[pair] = round(now - last, 2) if last is not None else None
        LOG.info(
            "[redis_ob] health counts=%s freshness_s=%s parse_fail=%d "
            "reconnects=%d silence_warnings=%d rest_fallback=%s",
            dict(self._snapshot_count_per_pair), freshness,
            self._parse_failures, self._reconnect_count,
            self._silence_warnings,
            "active" if self._rest_fallback_active else "off",
        )

    # ------------------------------------------------------------------
    # Test introspection
    # ------------------------------------------------------------------

    def snapshot_counts(self) -> Dict[str, Any]:
        return {
            "by_pair": dict(self._snapshot_count_per_pair),
            "last_recv_per_pair": dict(self._last_recv_per_pair),
            "parse_failures": self._parse_failures,
            "reconnects": self._reconnect_count,
            "silence_warnings": self._silence_warnings,
            "rest_fallback_active": self._rest_fallback_active,
        }
