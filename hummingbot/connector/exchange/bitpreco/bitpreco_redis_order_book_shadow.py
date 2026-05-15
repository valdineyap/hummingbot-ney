"""Shadow observer for the BitPreco `orderbook:<market>` Redis channel.

Phase 1A. Runs in parallel with the legacy REST-polling order book
data source. Subscribes to one channel per trading pair, parses
each snapshot, and emits ``[redis_shadow_ob]`` metrics for
latency / staleness comparison against the REST path.

This observer does NOT push to ``_message_queue`` and does NOT
build ``OrderBookMessage`` objects yet — the legacy order book
tracker continues to drive the bot. Phase 2 promotes this path to
primary; the parser logic lives there.

Phase 0 confirmed that the Redis orderbook payload is byte-
equivalent to the REST ``/orderbook`` response (same 11 fields,
``bids``/``asks`` of 100 levels each, ISO timestamps in BRT). The
production parser in PR 3 will reuse
``BitprecoOrderBook.snapshot_message_from_exchange_rest`` directly;
in PR 2 we only verify continuity and watchdog silence.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import parse_bitpreco_timestamp
from hummingbot.connector.exchange.bitpreco.bitpreco_redis_client import (
    RedisBackendConfig,
    RedisConnectionFactory,
)

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange

LOG = logging.getLogger(__name__)


_RECONNECT_BACKOFF_INITIAL_SEC = 1.0
_RECONNECT_BACKOFF_MAX_SEC = 30.0


class BitprecoRedisOrderBookShadow:
    """Subscribes to ``orderbook:<market>`` for every configured pair.

    Single subscriber + single connection covers N markets via
    ``SUBSCRIBE`` to N channel names — Redis pub/sub multiplexes
    them natively over one socket. We open ONE connection here
    (separate from the user-stream observer's connection, to keep
    backpressure isolated).
    """

    def __init__(
        self,
        connector: "BitprecoExchange",
        factory: RedisConnectionFactory,
        trading_pairs: List[str],
    ) -> None:
        self._connector = connector
        self._factory = factory
        self._config: RedisBackendConfig = factory.config
        self._trading_pairs = list(trading_pairs)
        # Per-pair state
        self._last_recv_ts: Dict[str, float] = {}
        self._last_event_ts: Dict[str, float] = {}  # utimestamp in UTC
        self._snapshot_counts: Dict[str, int] = {p: 0 for p in trading_pairs}
        self._parse_failures = 0
        self._silence_warnings_emitted: Dict[str, int] = {p: 0 for p in trading_pairs}
        self._reconnect_count = 0
        self._last_metric_log_ts = 0.0
        self._metric_log_interval_sec = 60.0

    async def run(self) -> None:
        backoff = _RECONNECT_BACKOFF_INITIAL_SEC
        while True:
            try:
                await self._connect_and_consume()
                backoff = _RECONNECT_BACKOFF_INITIAL_SEC
            except asyncio.CancelledError:
                LOG.info("[redis_shadow_ob] observer cancelled, "
                         "shutting down cleanly")
                return
            except Exception as exc:
                self._reconnect_count += 1
                LOG.warning("[redis_shadow_ob] error: %s "
                            "(reconnect #%d, backoff=%.1fs)",
                            exc, self._reconnect_count, backoff)
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    return
                backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_SEC)

    async def _connect_and_consume(self) -> None:
        if not self._trading_pairs:
            LOG.warning("[redis_shadow_ob] no trading pairs configured, exiting")
            return
        client = await self._factory.create_pubsub_connection()
        pubsub = client.pubsub()
        channels = [self._config.orderbook_channel(p) for p in self._trading_pairs]
        try:
            await pubsub.subscribe(*channels)
            LOG.info("[redis_shadow_ob] subscribed to %s", channels)
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
            self._check_silence(now)
            self._maybe_log_metrics(now)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _handle_message(self, msg: Any, recv_ts: float) -> None:
        channel = msg.get("channel") if isinstance(msg, dict) else None
        data = msg.get("data") if isinstance(msg, dict) else None
        if not isinstance(channel, str):
            self._parse_failures += 1
            return
        # Channel format is "orderbook:<pair>"; extract the pair.
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
                return
        elif isinstance(data, dict):
            payload = data
        else:
            self._parse_failures += 1
            return

        self._record_snapshot(pair, payload, recv_ts)

    def _record_snapshot(self, pair: str, payload: Dict[str, Any],
                         recv_ts: float) -> None:
        self._snapshot_counts[pair] = self._snapshot_counts.get(pair, 0) + 1
        self._last_recv_ts[pair] = recv_ts
        event_ts = parse_bitpreco_timestamp(payload.get("utimestamp"))
        if event_ts is None:
            event_ts = parse_bitpreco_timestamp(payload.get("timestamp"))
        if event_ts is not None:
            prev = self._last_event_ts.get(pair)
            # Stale check: snapshots are absolute, so accept newer-
            # or-equal. Older than last_event_ts → drop, log once
            # per ~100 occurrences to avoid flooding.
            if prev is not None and event_ts < prev:
                # Don't update last_event_ts; this is a regression.
                # In shadow mode just count and continue.
                LOG.debug(
                    "[redis_shadow_ob] stale snapshot pair=%s "
                    "event_ts=%.3f < prev=%.3f", pair, event_ts, prev)
                return
            self._last_event_ts[pair] = event_ts

    # ------------------------------------------------------------------
    # Silence watchdog (shadow-mode logs warning only; no fallback yet)
    # ------------------------------------------------------------------

    def _check_silence(self, now: float) -> None:
        threshold = CONSTANTS.REDIS_ORDERBOOK_SILENCE_THRESHOLD_SEC
        for pair in self._trading_pairs:
            last = self._last_recv_ts.get(pair)
            if last is None:
                # Never received — only warn after we've been up >threshold
                # to avoid noise during startup.
                continue
            silence = now - last
            if silence > threshold:
                # Emit one warning per silence episode, then again every
                # 30 seconds while silence persists.
                emitted = self._silence_warnings_emitted.get(pair, 0)
                if emitted == 0 or silence > threshold + (emitted * 30):
                    LOG.warning(
                        "[redis_shadow_ob] silence pair=%s elapsed=%.1fs "
                        "threshold=%.1fs",
                        pair, silence, threshold)
                    self._silence_warnings_emitted[pair] = emitted + 1
            else:
                # Reset the counter once snapshots resume.
                self._silence_warnings_emitted[pair] = 0

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def _maybe_log_metrics(self, now: float) -> None:
        if now - self._last_metric_log_ts < self._metric_log_interval_sec:
            return
        self._last_metric_log_ts = now
        # Per-pair freshness as a compact dict
        freshness = {}
        for pair in self._trading_pairs:
            last = self._last_recv_ts.get(pair)
            freshness[pair] = round(now - last, 2) if last is not None else None
        LOG.info(
            "[redis_shadow_ob] metrics counts=%s freshness_s=%s "
            "parse_fail=%d reconnects=%d",
            dict(self._snapshot_counts), freshness, self._parse_failures,
            self._reconnect_count,
        )

    # ------------------------------------------------------------------
    # Test introspection
    # ------------------------------------------------------------------

    def snapshot_counts(self) -> Dict[str, Any]:
        return {
            "by_pair": dict(self._snapshot_counts),
            "last_recv_ts": dict(self._last_recv_ts),
            "last_event_ts": dict(self._last_event_ts),
            "parse_failures": self._parse_failures,
            "reconnects": self._reconnect_count,
        }

    @property
    def trading_pairs(self) -> List[str]:
        return list(self._trading_pairs)
