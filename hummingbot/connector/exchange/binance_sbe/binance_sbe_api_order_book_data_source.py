"""Order-book data source for the ``binance_sbe`` connector.

Inherits from :class:`BinanceAPIOrderBookDataSource` and overrides the
WebSocket-side concerns:

* connects to ``stream-sbe.binance.com:9443`` with ``X-MBX-APIKEY`` header;
* subscribes to ``<sym>@trade`` and ``<sym>@depth`` (no ``@100ms`` — SBE
  depth is 25ms by design);
* parses binary frames via :func:`sbe_decoder.decode_frame` rather than
  JSON;
* proactively recycles the connection before Binance's 24h hard cap.

REST snapshot retrieval (``_request_order_book_snapshot``) and the
high-level orchestration in :class:`OrderBookTrackerDataSource` are reused
verbatim — the SBE stream does not provide a 1000-level snapshot, so the
public REST endpoint remains the authoritative source for tracker
initialisation.
"""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.binance.binance_api_order_book_data_source import (
    BinanceAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.binance_sbe import binance_sbe_constants as CONSTANTS
from hummingbot.connector.exchange.binance_sbe import sbe_decoder
from hummingbot.connector.exchange.binance_sbe.sbe_decoder import SbeDecodeError, SbeSchemaMismatchError
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.binance_sbe.binance_sbe_exchange import BinanceSbeExchange


class BinanceSbeAPIOrderBookDataSource(BinanceAPIOrderBookDataSource):
    """SBE variant of the Binance public WS order-book source."""

    def __init__(self,
                 trading_pairs: List[str],
                 connector: "BinanceSbeExchange",
                 api_factory: WebAssistantsFactory,
                 sbe_api_key: str,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(
            trading_pairs=trading_pairs,
            connector=connector,
            api_factory=api_factory,
            domain=domain,
        )
        # The Ed25519 API key STRING that goes verbatim into X-MBX-APIKEY.
        # Stored on the data source (not pulled from the exchange) so this
        # class is fully self-contained for testing.
        self._sbe_api_key = sbe_api_key
        # Monotonic timestamp of the most recent (re)connection; used to
        # decide when to proactively recycle the WS to dodge Binance's
        # 24h connection TTL. See _process_websocket_messages.
        self._connect_monotonic: Optional[float] = None
        # Set to True after the first successful binary-frame decode in
        # the current session. Reset to False on every reconnect so each
        # new session emits a single "pipeline healthy" log line at INFO
        # level — operator's positive signal that decode is working end
        # to end (vs. silent connection that never produces events).
        self._first_frame_logged: bool = False
        # ---- Queue-size telemetry (2026-05-16 memory-leak hunt) ----
        # ``self._message_queue`` is inherited from OrderBookTrackerDataSource
        # as ``defaultdict(asyncio.Queue)`` — unbounded. If a consumer task
        # stalls (backpressure, blocking work in an awaited callback, etc.),
        # the queue grows without limit and is a prime suspect for the OOM
        # incident on 2026-05-16 12:22Z. We emit a ``[sbe_queue]`` line at
        # most every ``_queue_log_interval_sec`` so the runtime backlog
        # appears in the same log feed as ``[mem]`` and the two curves can
        # be correlated by a grep + plot. Time-gated to avoid spam — the
        # frame-processing path runs at ~25 ms cadence.
        self._last_queue_log_time: float = 0.0
        self._queue_log_interval_sec: float = 60.0

    # ------------------------------------------------------------------
    # Stream-name helpers — used by every subscribe/unsubscribe path.
    # Keeping them in one place prevents drift between the initial
    # subscribe and dynamic add/remove of trading pairs.
    # ------------------------------------------------------------------

    @staticmethod
    def _trade_stream(symbol: str) -> str:
        return f"{symbol.lower()}@trade"

    @staticmethod
    def _depth_stream(symbol: str) -> str:
        # SBE depth stream is 25ms by design — no @100ms suffix.
        return f"{symbol.lower()}@depth"

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def _connected_websocket_assistant(self) -> WSAssistant:
        if not self._sbe_api_key:
            raise ValueError(
                "binance_sbe requires a non-empty SBE API key (Ed25519 string). "
                "Set 'binance_sbe_api_key' in the connector config."
            )
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WSS_SBE_URL,
            ws_headers={"X-MBX-APIKEY": self._sbe_api_key},
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
        )
        self._connect_monotonic = time.monotonic()
        # Reset the "first frame seen" flag so each new connection emits
        # one healthy-pipeline log line. Crucial for the 24h reconnect
        # case — without this, post-reconnect there'd be no positive
        # signal that the new session is actually delivering frames.
        self._first_frame_logged = False
        self.logger().info(
            f"[binance_sbe] connected to SBE WS at {CONSTANTS.WSS_SBE_URL} "
            f"(X-MBX-APIKEY header set, key length={len(self._sbe_api_key)})"
        )
        return ws

    # ------------------------------------------------------------------
    # Subscribe — initial bulk subscribe at session start.
    # ------------------------------------------------------------------

    async def _subscribe_channels(self, ws: WSAssistant):
        """Subscribe to trade+depth streams for every initial trading pair.

        Subscription payloads themselves are JSON — only the response
        frames are binary SBE. We reuse :class:`WSJSONRequest` here.
        """
        try:
            trade_params: List[str] = []
            depth_params: List[str] = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trade_params.append(self._trade_stream(symbol))
                depth_params.append(self._depth_stream(symbol))

            await ws.send(WSJSONRequest(payload={
                "method": "SUBSCRIBE", "params": trade_params, "id": 1,
            }))
            await ws.send(WSJSONRequest(payload={
                "method": "SUBSCRIBE", "params": depth_params, "id": 2,
            }))
            self.logger().info("Subscribed to SBE public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(
                "Unexpected error occurred subscribing to SBE order book trading and delta streams..."
            )
            raise

    # ------------------------------------------------------------------
    # Dynamic add/remove of trading pairs over an already-open WS.
    # Override the JSON-side implementations because they use the
    # `@depth@100ms` suffix that SBE doesn't accept.
    # ------------------------------------------------------------------

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot subscribe to {trading_pair}: WebSocket not connected")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

            await self._ws_assistant.send(WSJSONRequest(payload={
                "method": "SUBSCRIBE",
                "params": [self._trade_stream(symbol)],
                "id": self._get_next_subscribe_id(),
            }))
            await self._ws_assistant.send(WSJSONRequest(payload={
                "method": "SUBSCRIBE",
                "params": [self._depth_stream(symbol)],
                "id": self._get_next_subscribe_id(),
            }))

            self.add_trading_pair(trading_pair)
            self.logger().info(f"Subscribed to SBE {trading_pair} order book and trade channels")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error subscribing to SBE {trading_pair} channels")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot unsubscribe from {trading_pair}: WebSocket not connected")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            await self._ws_assistant.send(WSJSONRequest(payload={
                "method": "UNSUBSCRIBE",
                "params": [self._trade_stream(symbol), self._depth_stream(symbol)],
                "id": self._get_next_subscribe_id(),
            }))
            self.remove_trading_pair(trading_pair)
            self.logger().info(f"Unsubscribed from SBE {trading_pair} order book and trade channels")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error unsubscribing from SBE {trading_pair} channels")
            return False

    # ------------------------------------------------------------------
    # Message processing
    # ------------------------------------------------------------------

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        """Read frames off the wire and route them to the right queue.

        For SBE we receive a mix of:
          * binary frames (``ws_response.data`` is ``bytes``) — actual
            market events, decoded by :func:`sbe_decoder.decode_frame`;
          * JSON frames (``ws_response.data`` is ``dict``) — SUBSCRIBE
            and UNSUBSCRIBE acks, e.g. ``{"id": 1, "result": null}``.

        We do not rely on ``WSResponse.type`` because the framework's
        ``_build_resp`` does not preserve the underlying ``aiohttp``
        message type — we type-discriminate on ``data`` itself.

        This override also implements proactive reconnect: when the
        elapsed time on the current connection exceeds
        :data:`CONSTANTS.WS_RECONNECT_INTERVAL_SEC` we raise
        ``ConnectionError`` which the base ``listen_for_subscriptions``
        loop catches and treats as a normal reconnect cycle.
        """
        async for ws_response in websocket_assistant.iter_messages():
            self._maybe_force_reconnect()  # cheap check, runs each message

            data = ws_response.data
            if data is None:
                # iter_messages emits None on disconnect; the loop will exit naturally.
                continue

            if isinstance(data, (bytes, bytearray)):
                await self._process_binary_frame(bytes(data))
            elif isinstance(data, dict):
                self._handle_subscription_ack(data)
            else:
                self.logger().warning(
                    f"[binance_sbe] unexpected WS payload type {type(data).__name__}; ignoring"
                )

    def _maybe_force_reconnect(self) -> None:
        """If the current WS has been open longer than the configured
        interval, raise ``ConnectionError`` so the base reconnect cycle
        runs cleanly before Binance enforces its 24h cap."""
        if self._connect_monotonic is None:
            return
        elapsed = time.monotonic() - self._connect_monotonic
        if elapsed >= CONSTANTS.WS_RECONNECT_INTERVAL_SEC:
            self.logger().info(
                f"[binance_sbe] proactive reconnect after {elapsed:.0f}s "
                f"(threshold={CONSTANTS.WS_RECONNECT_INTERVAL_SEC}s, "
                f"avoids Binance 24h hard cap)"
            )
            # Force reset so we don't re-enter this branch repeatedly if
            # the base loop's reconnect takes a moment.
            self._connect_monotonic = None
            raise ConnectionError("binance_sbe proactive reconnect")

    async def _process_binary_frame(self, frame: bytes) -> None:
        # First successful decode per session is logged at INFO so the
        # operator gets a positive signal that the SBE pipeline is
        # actually delivering events (vs. just a successful TCP handshake
        # with no data). After this fires once, the OrderBookTracker's
        # own ``best_bid``/``best_ask`` updates are the steady-state
        # heartbeat.
        try:
            events = sbe_decoder.decode_frame(frame)
        except SbeSchemaMismatchError:
            # Schema (id or non-additive version) mismatch is systemic —
            # every frame on this stream will fail the same way. Propagate
            # so the base ``listen_for_subscriptions`` exception handler
            # logs and forces a reconnect; if the issue is persistent
            # (Binance bumped the schema), this surfaces as a loud,
            # operator-visible reconnect loop rather than a silently
            # empty queue. Matches the fail-stop policy documented in
            # the implementation plan.
            self.logger().exception("[binance_sbe] schema mismatch while decoding SBE frame")
            raise
        except SbeDecodeError:
            # Truncated buffers and other per-frame anomalies are
            # recoverable — the next frame will succeed. Log + skip.
            self.logger().exception("[binance_sbe] failed to decode SBE frame")
            return

        valid_channels = self._get_messages_queue_keys()
        for event in events:
            channel = self._channel_originating_message(event_message=event)
            if channel in valid_channels:
                self._message_queue[channel].put_nowait(event)
            else:
                # Decoder returned an event we don't know how to route;
                # surface it for diagnostics but don't crash.
                self.logger().debug(
                    f"[binance_sbe] decoded event with unrouted channel "
                    f"(e={event.get('e')}); dropping"
                )

        # One-time positive signal per session: the SBE pipeline is
        # actually decoding frames and the events are well-formed.
        # Runs once after the very first successful decode of each
        # (re)connection; for steady-state runtime visibility we rely
        # on the order book's own update cadence.
        if not self._first_frame_logged and events:
            first = events[0]
            self.logger().info(
                f"[binance_sbe] first SBE frame decoded — pipeline healthy "
                f"(event_type={first.get('e')!r}, "
                f"symbol={first.get('s')!r}, "
                f"events_in_frame={len(events)})"
            )
            self._first_frame_logged = True

        # Time-gated queue-backlog telemetry. Cheap on the common path —
        # one ``time.monotonic`` call and an int comparison — but emits a
        # ``[sbe_queue]`` line every ``_queue_log_interval_sec`` so we can
        # plot backlog vs RSS. See ``_maybe_log_queue_sizes``.
        self._maybe_log_queue_sizes()

    def _maybe_log_queue_sizes(self) -> None:
        """Emit a ``[sbe_queue]`` line with the current per-channel queue
        backlog if the time gate has elapsed.

        Why this lives in the connector and not in the controller: the
        controller's ``[mem]`` line tries to reach the queue via attribute
        walks (``maker._user_stream_tracker.data_source._state_machine``)
        which is fragile and connector-specific. The connector itself
        always has direct access to ``self._message_queue``, so logging
        here is the simplest and most reliable place.

        Format::

            [sbe_queue] total=<sum> max=<max> n_channels=<n> top=<{ch: q}>

        ``top`` shows up to the 3 largest channels so we can tell which
        stream (trade/depth/bookTicker) is backing up. The whole line is
        designed to be one grep-friendly INFO message per minute.
        """
        try:
            now = time.monotonic()
            if (now - self._last_queue_log_time) < self._queue_log_interval_sec:
                return
            self._last_queue_log_time = now
            queues = self._message_queue
            if not queues:
                return
            sizes = {ch: q.qsize() for ch, q in queues.items()}
            total = sum(sizes.values())
            mx = max(sizes.values())
            top3 = sorted(sizes.items(), key=lambda kv: -kv[1])[:3]
            top_str = ",".join(f"{ch}:{q}" for ch, q in top3)
            self.logger().info(
                f"[sbe_queue] total={total} max={mx} "
                f"n_channels={len(sizes)} top={top_str}"
            )
        except Exception as e:
            # Telemetry must never break the data path.
            self.logger().debug(
                f"[sbe_queue] log failed: {type(e).__name__}: {e}"
            )

    def _handle_subscription_ack(self, data: Dict[str, Any]) -> None:
        """Acknowledge a JSON subscribe/unsubscribe response.

        Binance returns ``{"id": <int>, "result": null}`` on success and
        ``{"id": <int>, "error": {...}}`` on failure. We log either way
        but don't block the stream — the next binary frame will arrive
        regardless.
        """
        if "result" in data:
            # Logged at INFO (not DEBUG) because acks are sparse — one
            # per SUBSCRIBE/UNSUBSCRIBE call — and they're the proof
            # the request actually reached the server. Failure mode
            # this guards against: subscribes silently never being
            # acknowledged, with no log of what we sent vs received.
            self.logger().info(
                f"[binance_sbe] subscription ack id={data.get('id')} result={data.get('result')}"
            )
        elif "error" in data:
            self.logger().error(
                f"[binance_sbe] subscription error id={data.get('id')} error={data.get('error')}"
            )
        else:
            self.logger().warning(f"[binance_sbe] unknown JSON ws payload: {data}")
