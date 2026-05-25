"""Persistent WebSocket request router for Binance Spot WS-API.

The router owns one long-lived ``aiohttp`` WebSocket connection to
``wss://ws-api.binance.com:443/ws-api/v3`` and serialises an
asynchronous request/response protocol over it.

  - ``send_public(method, params=None)`` — unsigned method (``ping``,
    ``time``). Goes through the throttler with the appropriate limit id.
  - ``send_signed(method, params)`` — appends ``apiKey`` / ``timestamp``
    / ``signature`` (alphabetically ordered per the WS-API spec) before
    sending; throttler honours method-specific weights.

Each request gets a UUID4 ``id`` and a future placed in ``_pending``.
The dispatcher task reads frames off the socket and discriminates by
shape: a frame with an ``id`` resolves the matching future; a frame
without ``id`` but with ``event.e == "serverShutdown"`` triggers a
graceful reconnect; anything else is logged.

Key invariants:

  1. **Late responses are not silently dropped.** ``asyncio.wait_for``
     would cancel the pending future on timeout, killing the late-
     response handler. We wrap with ``asyncio.shield(future)`` so the
     future survives; if a response arrives after the timeout we log
     ``[bws_late_response]`` with ``id``, ``method``, ``late_by_ms``
     for manual reconciliation of any UNKNOWN orders.

  2. **Disconnect distinguishes CREATED_NOT_SENT vs SENT.** A pending
     request that hasn't yet been written to the socket can be retried
     safely (``BinanceWsDisconnectedError``). One that was already sent
     might have reached the matching engine and must be treated as an
     UNKNOWN execution (``BinanceWsUnknownExecutionError``); the caller
     consults REST to reconcile.

  3. **All sends go through the throttler.** ``self._throttler`` is
     the same instance the parent ``BinanceExchange`` uses for REST,
     so WS and REST counts share the IP-wide REQUEST_WEIGHT / ORDERS
     pools the way Binance bills them.

  4. **FAILED state on reconnect storm.** More than
     ``WS_MAX_RECONNECTS_IN_WINDOW`` reconnects in
     ``WS_RECONNECT_WINDOW_SEC`` flips the router to ``state=FAILED``.
     ``check_network`` on the exchange then reports NOT_CONNECTED, so
     the bot stops quoting instead of staying alive but zombie. The
     router keeps trying once per minute so it recovers on its own
     when the upstream comes back.

The class is loop-aware (background tasks for dispatch + lifecycle) but
testable: the constructor accepts a ``ws_connector`` callable that
defaults to ``aiohttp.ClientSession.ws_connect`` so the unit suite can
inject a fake bidirectional channel without touching the network.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp

from hummingbot.connector.exchange.binance_ws import binance_ws_constants as CONSTANTS
from hummingbot.connector.exchange.binance_ws.binance_ws_auth import sign_request_params
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


# ---------------------------------------------------------------------------
# Exceptions — caller distinguishes UNKNOWN-execution vs clean failures.
# ---------------------------------------------------------------------------

class BinanceWsError(Exception):
    """Base class for router-raised errors."""


class BinanceWsRequestError(BinanceWsError):
    """Server returned status≠200 with an error body. Application-level
    failure (insufficient balance, no-such-order, etc.). NOT execution-
    unknown."""

    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"binance_ws error code={code}: {msg}")


class BinanceWsTimeoutError(BinanceWsError):
    """Local timeout fired. Pending entry transitions to LATE_PENDING
    and is kept for the configured window so an arriving response can
    still be logged."""


class BinanceWsUnknownExecutionError(BinanceWsError):
    """The request *might* have been processed by the matching engine.

    Raised for status ≥500, code -1007 (server timeout), or disconnect
    of a request in ``SENT`` state. Caller MUST treat the operation as
    unknown — placing an order means returning ``("UNKNOWN", time())``;
    cancelling means querying order.status via REST before deciding.
    """


class BinanceWsDisconnectedError(BinanceWsError):
    """Socket dropped BEFORE the request was written. Safe to retry."""


# ---------------------------------------------------------------------------
# Public response envelope
# ---------------------------------------------------------------------------

@dataclass
class BinanceWsResponse:
    """Envelope returned by ``send_public`` / ``send_signed``.

    Preserves ``rate_limits`` (which would otherwise be discarded if we
    returned only ``result``) so the router's sampling helper can pull
    them out and log usage.
    """
    id: str
    status: int
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    rate_limits: Optional[List[Dict[str, Any]]] = None
    rtt_ms: float = 0.0


# ---------------------------------------------------------------------------
# Internal pending-request bookkeeping
# ---------------------------------------------------------------------------

class PendingState(Enum):
    CREATED_NOT_SENT = "created_not_sent"
    SENT = "sent"
    LATE_PENDING = "late_pending"


@dataclass
class _PendingEntry:
    future: asyncio.Future
    method: str
    state: PendingState = PendingState.CREATED_NOT_SENT
    created_at: float = field(default_factory=time.time)
    sent_at: Optional[float] = None
    timed_out_at: Optional[float] = None


class RouterState(Enum):
    STARTING = "starting"
    CONNECTED = "connected"
    DRAINING = "draining"  # serverShutdown received; new conn coming up
    RECONNECTING = "reconnecting"
    FAILED = "failed"
    STOPPED = "stopped"


# Type alias for the WS-connector factory injected for tests.
WsConnectorFactory = Callable[[str], Awaitable[Any]]


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

class BinanceWsRequestRouter:

    _logger: Optional[logging.Logger] = None

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        throttler: AsyncThrottler,
        time_provider: Callable[[], float],
        domain: str = "com",
        ws_url: Optional[str] = None,
        ws_connector: Optional[WsConnectorFactory] = None,
        request_timeout_sec: float = CONSTANTS.WS_REQUEST_TIMEOUT_SEC,
        late_response_window_sec: float = CONSTANTS.WS_LATE_RESPONSE_WINDOW_SEC,
        reconnect_interval_sec: float = CONSTANTS.WS_RECONNECT_INTERVAL_SEC,
    ):
        self._api_key = api_key
        self._api_secret = api_secret
        self._throttler = throttler
        self._time_provider = time_provider
        self._domain = domain
        self._ws_url = ws_url or CONSTANTS.WSS_API_TRADING_URL.format(domain)
        self._ws_connector = ws_connector  # None → use aiohttp directly
        self._request_timeout_sec = request_timeout_sec
        self._late_response_window_sec = late_response_window_sec
        self._reconnect_interval_sec = reconnect_interval_sec

        # Lifecycle
        self._state: RouterState = RouterState.STOPPED
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[Any] = None
        self._dispatch_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._proactive_reconnect_task: Optional[asyncio.Task] = None
        self._connection_started_at: float = 0.0
        self._stop_event: asyncio.Event = asyncio.Event()

        # Pending bookkeeping
        self._pending: Dict[str, _PendingEntry] = {}

        # Reconnect bookkeeping
        self._reconnect_count = 0
        self._reconnect_timestamps: List[float] = []
        self._consecutive_timeouts = 0
        self._last_message_ts: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._state == RouterState.CONNECTED and self._ws is not None and not self._ws.closed

    @property
    def state(self) -> RouterState:
        return self._state

    def health(self) -> Dict[str, Any]:
        """Snapshot of router state. Consumed by ``check_network``
        override on the exchange."""
        return {
            "ws_trading_connected": self.connected,
            "ws_state": self._state.value,
            "ws_reconnect_count": self._reconnect_count,
            "ws_pending_requests": sum(
                1 for e in self._pending.values()
                if e.state != PendingState.LATE_PENDING
            ),
            "ws_last_message_ts": self._last_message_ts,
            "ws_consecutive_timeouts": self._consecutive_timeouts,
        }

    async def start(self) -> None:
        if self._state not in (RouterState.STOPPED, RouterState.FAILED):
            return
        self._stop_event.clear()
        self._state = RouterState.STARTING
        await self._connect_once()
        # Background lifecycle task — handles proactive reconnect at TTL
        # cap and FAILED-mode slow retry.
        self._proactive_reconnect_task = asyncio.create_task(self._proactive_reconnect_loop())

    async def stop(self) -> None:
        self._state = RouterState.STOPPED
        self._stop_event.set()
        for task in (self._dispatch_task, self._proactive_reconnect_task, self._reconnect_task):
            if task is not None and not task.done():
                task.cancel()
        self._dispatch_task = None
        self._proactive_reconnect_task = None
        self._reconnect_task = None
        await self._close_ws()
        # Resolve any remaining pendings as disconnected — caller chose
        # to stop the router, so anything queued is fine to fail-fast.
        for req_id, entry in list(self._pending.items()):
            if not entry.future.done():
                exc = (
                    BinanceWsDisconnectedError("router stopped before send")
                    if entry.state == PendingState.CREATED_NOT_SENT
                    else BinanceWsUnknownExecutionError("router stopped after send")
                )
                entry.future.set_exception(exc)
        self._pending.clear()

    async def send_public(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        limit_id: Optional[str] = None,
    ) -> BinanceWsResponse:
        """Send an unsigned method (e.g. ``ping``, ``time``)."""
        payload_params = dict(params or {})
        return await self._send(method, payload_params, signed=False, limit_id=limit_id)

    async def send_signed(
        self,
        method: str,
        params: Dict[str, Any],
        limit_id: Optional[str] = None,
    ) -> BinanceWsResponse:
        """Sign and send a SIGNED method."""
        signed_params = sign_request_params(
            params or {}, self._api_key, self._api_secret, self._time_provider,
        )
        return await self._send(method, signed_params, signed=True, limit_id=limit_id)

    # ------------------------------------------------------------------
    # Send pipeline
    # ------------------------------------------------------------------

    _DEFAULT_LIMIT_BY_METHOD: Dict[str, str] = {
        CONSTANTS.WS_METHOD_PING: CONSTANTS.WS_LIMIT_PING,
        CONSTANTS.WS_METHOD_TIME: CONSTANTS.WS_LIMIT_PING,
        CONSTANTS.WS_METHOD_ORDER_PLACE: CONSTANTS.WS_LIMIT_ORDER_PLACE,
        CONSTANTS.WS_METHOD_ORDER_CANCEL: CONSTANTS.WS_LIMIT_ORDER_CANCEL,
        CONSTANTS.WS_METHOD_ORDER_TEST: CONSTANTS.WS_LIMIT_ORDER_TEST,
        CONSTANTS.WS_METHOD_ORDER_STATUS: CONSTANTS.WS_LIMIT_ORDER_STATUS,
        CONSTANTS.WS_METHOD_ACCOUNT_RATE_LIMITS: CONSTANTS.WS_LIMIT_ACCOUNT_RATE_LIMITS,
    }

    async def _send(
        self,
        method: str,
        params: Dict[str, Any],
        signed: bool,
        limit_id: Optional[str],
    ) -> BinanceWsResponse:
        if self._state == RouterState.FAILED:
            raise BinanceWsDisconnectedError("router in FAILED state")
        if self._state == RouterState.STOPPED:
            raise BinanceWsDisconnectedError("router stopped")

        effective_limit = limit_id or self._DEFAULT_LIMIT_BY_METHOD.get(method, CONSTANTS.WS_LIMIT_PING)

        req_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        entry = _PendingEntry(future=future, method=method)
        self._pending[req_id] = entry

        envelope = {"id": req_id, "method": method, "params": params}
        send_started = time.monotonic()

        try:
            async with self._throttler.execute_task(limit_id=effective_limit):
                if not self.connected:
                    raise BinanceWsDisconnectedError(
                        f"cannot send {method}: router not connected (state={self._state.value})"
                    )
                await self._ws.send_json(envelope)
                entry.state = PendingState.SENT
                entry.sent_at = time.monotonic()
        except BinanceWsError:
            self._pending.pop(req_id, None)
            raise
        except (aiohttp.ClientError, ConnectionError) as exc:
            self._pending.pop(req_id, None)
            raise BinanceWsDisconnectedError(f"send failed: {exc!r}") from exc

        # asyncio.shield prevents wait_for from cancelling the future on
        # timeout. The future is kept alive in LATE_PENDING so an
        # arriving response can still log [bws_late_response].
        try:
            response = await asyncio.wait_for(
                asyncio.shield(future),
                timeout=self._request_timeout_sec,
            )
        except asyncio.TimeoutError as exc:
            entry.state = PendingState.LATE_PENDING
            entry.timed_out_at = time.monotonic()
            self._consecutive_timeouts += 1
            asyncio.create_task(self._expire_late_pending(req_id))
            raise BinanceWsTimeoutError(
                f"timed out after {self._request_timeout_sec}s waiting for {method} (id={req_id})"
            ) from exc

        self._pending.pop(req_id, None)
        self._consecutive_timeouts = 0
        response.rtt_ms = (time.monotonic() - send_started) * 1000.0
        if response.rtt_ms > CONSTANTS.WS_SLOW_REQUEST_WARN_MS:
            self.logger().warning(
                "%s slow %s rtt_ms=%.1f id=%s",
                CONSTANTS.WS_LOG_TIMING, method, response.rtt_ms, req_id,
            )
        else:
            self.logger().debug(
                "%s %s rtt_ms=%.1f id=%s status=%s",
                CONSTANTS.WS_LOG_TIMING, method, response.rtt_ms, req_id, response.status,
            )

        if response.error is not None:
            err = response.error
            code = int(err.get("code", -1))
            msg = str(err.get("msg", ""))
            if code == -1007 or response.status >= 500:
                raise BinanceWsUnknownExecutionError(
                    f"{method} returned execution-unknown (code={code}, status={response.status}): {msg}"
                )
            raise BinanceWsRequestError(code, msg)
        return response

    async def _expire_late_pending(self, req_id: str) -> None:
        await asyncio.sleep(self._late_response_window_sec)
        entry = self._pending.pop(req_id, None)
        if entry is None:
            return
        if not entry.future.done():
            entry.future.cancel()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def _connect_once(self) -> None:
        await self._close_ws()
        await self._throttler.execute_task(limit_id=CONSTANTS.WS_LIMIT_CONNECT).__aenter__()  # consumes WEIGHT 2

        if self._ws_connector is not None:
            # Test injection — factory returns a ready-to-use ws object.
            self._ws = await self._ws_connector(self._ws_url)
            self._session = None
        else:
            self._session = aiohttp.ClientSession()
            # autoping=True so aiohttp answers the server-initiated pings
            # automatically (server pings every ~20s, expects pong in
            # <60s, otherwise drops). heartbeat=None disables client-
            # initiated pings — dual pings would be wasteful.
            self._ws = await self._session.ws_connect(
                self._ws_url,
                autoping=True,
                heartbeat=None,
            )

        self._connection_started_at = time.monotonic()
        self._last_message_ts = time.time()
        self._state = RouterState.CONNECTED
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())

    async def _close_ws(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None

    # ------------------------------------------------------------------
    # Dispatch task — reads frames and resolves futures
    # ------------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        try:
            async for msg in self._ws:
                self._last_message_ts = time.time()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_text(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSE,
                                  aiohttp.WSMsgType.CLOSING,
                                  aiohttp.WSMsgType.CLOSED,
                                  aiohttp.WSMsgType.ERROR):
                    break
        except (aiohttp.ClientError, ConnectionError) as exc:
            self.logger().warning("%s dispatch loop terminated: %r",
                                  CONSTANTS.WS_LOG_RECONNECT, exc)
        finally:
            if self._state not in (RouterState.STOPPED, RouterState.FAILED):
                self._reconnect_task = asyncio.create_task(self._reconnect())

    def _handle_text(self, raw: str) -> None:
        import json
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            self.logger().warning("%s non-JSON frame: %r",
                                  CONSTANTS.WS_LOG_UNHANDLED_EVENT, raw[:200])
            return

        # Discriminate by shape — frames without ``id`` are events or
        # connection-level errors and MUST NOT resolve _pending[None].
        msg_id = msg.get("id")
        if msg_id is None:
            event = msg.get("event")
            if isinstance(event, dict) and event.get("e") == "serverShutdown":
                self._handle_server_shutdown(event)
                return
            err = msg.get("error")
            if err is not None:
                self.logger().error(
                    "%s connection-level error: %r",
                    CONSTANTS.WS_LOG_UNHANDLED_EVENT, err,
                )
                return
            self.logger().info(
                "%s frame without id: %r",
                CONSTANTS.WS_LOG_UNHANDLED_EVENT, msg,
            )
            return

        self._dispatch_response(msg_id, msg)

    def _dispatch_response(self, msg_id: str, msg: Dict[str, Any]) -> None:
        entry = self._pending.get(msg_id)
        if entry is None:
            self.logger().warning(
                "%s response for unknown id=%s method=%s",
                CONSTANTS.WS_LOG_UNHANDLED_EVENT, msg_id, msg.get("method"),
            )
            return

        status = int(msg.get("status", 200))
        result = msg.get("result")
        error = msg.get("error")
        rate_limits = msg.get("rateLimits")
        response = BinanceWsResponse(
            id=msg_id, status=status, result=result, error=error,
            rate_limits=rate_limits,
        )

        if entry.state == PendingState.LATE_PENDING:
            late_by_ms = (time.monotonic() - (entry.timed_out_at or time.monotonic())) * 1000.0
            self.logger().warning(
                "%s id=%s method=%s late_by_ms=%.1f status=%s",
                CONSTANTS.WS_LOG_LATE_RESPONSE, msg_id, entry.method,
                late_by_ms, status,
            )
            self._pending.pop(msg_id, None)
            return

        if not entry.future.done():
            entry.future.set_result(response)

    def _handle_server_shutdown(self, event: Dict[str, Any]) -> None:
        self.logger().warning(
            "%s scheduled server-side disconnect; will reconnect within drain window",
            CONSTANTS.WS_LOG_SERVER_SHUTDOWN,
        )
        if self._state == RouterState.CONNECTED:
            self._state = RouterState.DRAINING
            # Schedule the reconnect; the existing socket continues
            # accepting late responses for in-flight pendings during
            # the configured drain window.
            self._reconnect_task = asyncio.create_task(self._reconnect())

    # ------------------------------------------------------------------
    # Reconnect
    # ------------------------------------------------------------------

    async def _reconnect(self) -> None:
        # Drain pendings whose state is SENT — those might have hit the
        # matching engine. CREATED_NOT_SENT can be retried by caller.
        await self._drain_pending_on_disconnect()

        attempt = 0
        while not self._stop_event.is_set():
            now = time.monotonic()
            # Window-based FAILED detection
            self._reconnect_timestamps = [
                t for t in self._reconnect_timestamps
                if now - t < CONSTANTS.WS_RECONNECT_WINDOW_SEC
            ]
            self._reconnect_timestamps.append(now)
            if len(self._reconnect_timestamps) > CONSTANTS.WS_MAX_RECONNECTS_IN_WINDOW:
                self.logger().critical(
                    "%s entered FAILED state after %d reconnects in %ss",
                    CONSTANTS.WS_LOG_RECONNECT,
                    len(self._reconnect_timestamps),
                    CONSTANTS.WS_RECONNECT_WINDOW_SEC,
                )
                self._state = RouterState.FAILED
                # Cool-down: keep trying, but slowly.
                try:
                    await asyncio.wait_for(self._stop_event.wait(),
                                           timeout=CONSTANTS.WS_FAILED_RETRY_INTERVAL_SEC)
                    return
                except asyncio.TimeoutError:
                    pass
            else:
                self._state = RouterState.RECONNECTING

            backoff = min(
                CONSTANTS.WS_RECONNECT_BACKOFF_MIN_SEC * (2 ** attempt),
                CONSTANTS.WS_RECONNECT_BACKOFF_MAX_SEC,
            )
            jitter = backoff * 0.2 * random.uniform(-1.0, 1.0)
            wait = max(0.1, backoff + jitter)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait)
                return  # stop requested
            except asyncio.TimeoutError:
                pass

            try:
                await self._connect_once()
                self._reconnect_count += 1
                self.logger().info(
                    "%s reconnected (count=%d)",
                    CONSTANTS.WS_LOG_RECONNECT, self._reconnect_count,
                )
                return
            except Exception as exc:
                self.logger().warning(
                    "%s reconnect attempt %d failed: %r",
                    CONSTANTS.WS_LOG_RECONNECT, attempt, exc,
                )
                attempt += 1

    async def _drain_pending_on_disconnect(self) -> None:
        for req_id, entry in list(self._pending.items()):
            if entry.future.done():
                continue
            if entry.state == PendingState.CREATED_NOT_SENT:
                exc: BinanceWsError = BinanceWsDisconnectedError(
                    f"disconnected before send (id={req_id})"
                )
            elif entry.state == PendingState.SENT:
                exc = BinanceWsUnknownExecutionError(
                    f"disconnected after send (id={req_id} method={entry.method})"
                )
            else:
                # LATE_PENDING — let _expire_late_pending handle it.
                continue
            entry.future.set_exception(exc)
            self._pending.pop(req_id, None)

    async def _proactive_reconnect_loop(self) -> None:
        """Recycle the connection well before Binance's 24h TTL cap."""
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._reconnect_interval_sec,
                    )
                    return
                except asyncio.TimeoutError:
                    pass
                if self._state == RouterState.CONNECTED:
                    self.logger().info(
                        "%s proactive reconnect (uptime=%.0fs)",
                        CONSTANTS.WS_LOG_RECONNECT,
                        time.monotonic() - self._connection_started_at,
                    )
                    self._state = RouterState.DRAINING
                    asyncio.create_task(self._reconnect())
        except asyncio.CancelledError:
            return
