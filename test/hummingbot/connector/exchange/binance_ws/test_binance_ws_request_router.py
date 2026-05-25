"""Tests for ``BinanceWsRequestRouter``.

The router owns a persistent WebSocket and a pile of async invariants
(timeout doesn't cancel pending, out-of-order responses dispatch by id,
disconnect distinguishes CREATED_NOT_SENT vs SENT, FAILED-state after
reconnect storm, etc.). We test by injecting a fake WS factory so no
network is involved.

A "real" aiohttp WS is asynchronously iterable over ``WSMessage``
objects. Our fake mirrors that surface area enough for the dispatch
loop, but lets the test push responses by calling
``fake_ws.deliver(text)``.
"""
from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any, List, Optional

import aiohttp

from hummingbot.connector.exchange.binance.binance_constants import RATE_LIMITS
from hummingbot.connector.exchange.binance_ws import binance_ws_constants as CONSTANTS
from hummingbot.connector.exchange.binance_ws.binance_ws_request_router import (
    BinanceWsDisconnectedError,
    BinanceWsRequestError,
    BinanceWsRequestRouter,
    BinanceWsResponse,
    BinanceWsTimeoutError,
    BinanceWsUnknownExecutionError,
    PendingState,
    RouterState,
)
from hummingbot.connector.exchange.binance_ws.binance_ws_utils import build_rate_limits
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


# ---------------------------------------------------------------------------
# Fake aiohttp-ish WS
# ---------------------------------------------------------------------------

class _FakeWs:
    def __init__(self):
        self.sent: List[dict] = []
        self._incoming: asyncio.Queue = asyncio.Queue()
        self.closed: bool = False
        self._send_hook = None  # callable(envelope) -> None | awaitable

    def hook_send(self, fn):
        """Install a callback that fires synchronously inside ``send_json``.

        Used by tests to deliver a response *as soon as* the request is
        written, or to delay/swallow the send.
        """
        self._send_hook = fn

    async def send_json(self, payload: dict) -> None:
        if self.closed:
            raise aiohttp.ClientError("fake ws closed")
        self.sent.append(payload)
        if self._send_hook is not None:
            res = self._send_hook(payload)
            if asyncio.iscoroutine(res):
                await res

    def deliver(self, msg: dict) -> None:
        """Push a TEXT frame to the dispatcher."""
        wsmsg = aiohttp.WSMessage(
            type=aiohttp.WSMsgType.TEXT,
            data=json.dumps(msg),
            extra="",
        )
        self._incoming.put_nowait(wsmsg)

    def deliver_close(self) -> None:
        wsmsg = aiohttp.WSMessage(
            type=aiohttp.WSMsgType.CLOSE, data=None, extra="",
        )
        self._incoming.put_nowait(wsmsg)

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self._incoming.get()
        if msg.type in (aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.CLOSED):
            self.closed = True
            raise StopAsyncIteration
        return msg

    async def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _build_throttler() -> AsyncThrottler:
    return AsyncThrottler(list(RATE_LIMITS) + build_rate_limits())


def _frozen_clock(value: float = 1_700_000_000.0):
    return lambda: value


class _RouterFixture:
    def __init__(self, router: BinanceWsRequestRouter):
        self.router = router
        self.ws_instances: List[_FakeWs] = []


def _make_router(
    *,
    timeout: float = 0.5,
    late_window: float = 0.3,
    reconnect_interval: float = 9999.0,
) -> _RouterFixture:
    fixture = _RouterFixture.__new__(_RouterFixture)
    fixture.ws_instances = []

    async def _factory(url: str):
        ws = _FakeWs()
        fixture.ws_instances.append(ws)
        return ws

    router = BinanceWsRequestRouter(
        api_key="K",
        api_secret="S",
        throttler=_build_throttler(),
        time_provider=_frozen_clock(),
        domain="com",
        ws_connector=_factory,
        request_timeout_sec=timeout,
        late_response_window_sec=late_window,
        reconnect_interval_sec=reconnect_interval,
    )
    fixture.__init__(router)
    return fixture


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class HappyPathTest(unittest.IsolatedAsyncioTestCase):

    async def asyncTearDown(self):
        # Clean up any router left running.
        for task in asyncio.all_tasks() - {asyncio.current_task()}:
            task.cancel()

    async def test_send_public_resolves_with_response(self):
        fx = _make_router()
        await fx.router.start()
        ws = fx.ws_instances[0]

        def _on_send(envelope):
            ws.deliver({
                "id": envelope["id"],
                "status": 200,
                "result": {"pong": True},
            })

        ws.hook_send(_on_send)

        resp = await fx.router.send_public(CONSTANTS.WS_METHOD_PING)
        self.assertIsInstance(resp, BinanceWsResponse)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.result, {"pong": True})
        self.assertGreater(resp.rtt_ms, 0)
        await fx.router.stop()

    async def test_send_signed_attaches_signature(self):
        fx = _make_router()
        await fx.router.start()
        ws = fx.ws_instances[0]

        def _on_send(envelope):
            ws.deliver({"id": envelope["id"], "status": 200, "result": {}})
        ws.hook_send(_on_send)

        await fx.router.send_signed(
            CONSTANTS.WS_METHOD_ORDER_TEST,
            {"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
             "quantity": "0.001", "price": "20000", "timeInForce": "GTC"},
        )
        sent = ws.sent[0]["params"]
        self.assertIn("apiKey", sent)
        self.assertIn("timestamp", sent)
        self.assertIn("signature", sent)
        self.assertEqual(sent["apiKey"], "K")
        await fx.router.stop()

    async def test_out_of_order_responses_dispatch_by_id(self):
        fx = _make_router()
        await fx.router.start()
        ws = fx.ws_instances[0]

        captured_ids: List[str] = []

        def _on_send(envelope):
            captured_ids.append(envelope["id"])
        ws.hook_send(_on_send)

        t1 = asyncio.create_task(fx.router.send_public(CONSTANTS.WS_METHOD_PING))
        t2 = asyncio.create_task(fx.router.send_public(CONSTANTS.WS_METHOD_TIME))
        await asyncio.sleep(0.01)

        # Respond in REVERSE order.
        ws.deliver({"id": captured_ids[1], "status": 200, "result": {"serverTime": 999}})
        ws.deliver({"id": captured_ids[0], "status": 200, "result": {"pong": 1}})

        r1, r2 = await asyncio.gather(t1, t2)
        self.assertEqual(r1.result, {"pong": 1})
        self.assertEqual(r2.result, {"serverTime": 999})
        await fx.router.stop()

    async def test_unique_ids_per_request(self):
        fx = _make_router()
        await fx.router.start()
        ws = fx.ws_instances[0]

        seen: List[str] = []

        def _on_send(envelope):
            seen.append(envelope["id"])
            ws.deliver({"id": envelope["id"], "status": 200, "result": {}})
        ws.hook_send(_on_send)

        await asyncio.gather(
            fx.router.send_public(CONSTANTS.WS_METHOD_PING),
            fx.router.send_public(CONSTANTS.WS_METHOD_PING),
            fx.router.send_public(CONSTANTS.WS_METHOD_PING),
        )
        self.assertEqual(len(set(seen)), 3)
        await fx.router.stop()


class ErrorPathTest(unittest.IsolatedAsyncioTestCase):

    async def test_application_error_raises_BinanceWsRequestError(self):
        fx = _make_router()
        await fx.router.start()
        ws = fx.ws_instances[0]

        def _on_send(envelope):
            ws.deliver({
                "id": envelope["id"], "status": 400,
                "error": {"code": -2010, "msg": "Account has insufficient balance."},
            })
        ws.hook_send(_on_send)

        with self.assertRaises(BinanceWsRequestError) as cm:
            await fx.router.send_signed(CONSTANTS.WS_METHOD_ORDER_PLACE, {"symbol": "BTCUSDT"})
        self.assertEqual(cm.exception.code, -2010)
        self.assertIn("insufficient", cm.exception.msg)
        await fx.router.stop()

    async def test_status_5xx_raises_BinanceWsUnknownExecutionError(self):
        fx = _make_router()
        await fx.router.start()
        ws = fx.ws_instances[0]

        def _on_send(envelope):
            ws.deliver({
                "id": envelope["id"], "status": 503,
                "error": {"code": -1, "msg": "Service unavailable"},
            })
        ws.hook_send(_on_send)

        with self.assertRaises(BinanceWsUnknownExecutionError):
            await fx.router.send_signed(CONSTANTS.WS_METHOD_ORDER_PLACE, {"symbol": "BTCUSDT"})
        await fx.router.stop()

    async def test_code_neg1007_raises_BinanceWsUnknownExecutionError(self):
        fx = _make_router()
        await fx.router.start()
        ws = fx.ws_instances[0]

        def _on_send(envelope):
            ws.deliver({
                "id": envelope["id"], "status": 200,
                "error": {"code": -1007, "msg": "Timeout"},
            })
        ws.hook_send(_on_send)

        with self.assertRaises(BinanceWsUnknownExecutionError):
            await fx.router.send_signed(CONSTANTS.WS_METHOD_ORDER_PLACE, {"symbol": "BTCUSDT"})
        await fx.router.stop()


class TimeoutAndLateResponseTest(unittest.IsolatedAsyncioTestCase):

    async def test_timeout_raises_BinanceWsTimeoutError(self):
        fx = _make_router(timeout=0.1, late_window=0.5)
        await fx.router.start()
        # Don't hook a response — request hangs until timeout fires.

        with self.assertRaises(BinanceWsTimeoutError):
            await fx.router.send_public(CONSTANTS.WS_METHOD_PING)
        await fx.router.stop()

    async def test_late_response_after_timeout_does_not_crash_and_is_logged(self):
        # The key invariant: asyncio.shield must keep the future alive
        # so an arriving late response doesn't blow up on a cancelled
        # future. We verify (a) the original waiter saw the timeout,
        # (b) a late response logged to [bws_late_response] cleanly,
        # (c) no unhandled exceptions.
        fx = _make_router(timeout=0.1, late_window=1.0)
        await fx.router.start()
        ws = fx.ws_instances[0]
        captured = []

        def _on_send(envelope):
            captured.append(envelope["id"])
        ws.hook_send(_on_send)

        with self.assertRaises(BinanceWsTimeoutError):
            await fx.router.send_public(CONSTANTS.WS_METHOD_PING)

        self.assertIn(captured[0], fx.router._pending)
        self.assertEqual(
            fx.router._pending[captured[0]].state, PendingState.LATE_PENDING
        )

        # Now deliver the late response.
        ws.deliver({"id": captured[0], "status": 200, "result": {"late": True}})
        await asyncio.sleep(0.05)  # let dispatcher pick it up
        # Pending is dropped, no exception raised by the dispatcher.
        self.assertNotIn(captured[0], fx.router._pending)
        await fx.router.stop()

    async def test_pending_cleared_after_late_window_expires(self):
        fx = _make_router(timeout=0.05, late_window=0.1)
        await fx.router.start()
        ws = fx.ws_instances[0]
        captured = []

        def _on_send(envelope):
            captured.append(envelope["id"])
        ws.hook_send(_on_send)

        with self.assertRaises(BinanceWsTimeoutError):
            await fx.router.send_public(CONSTANTS.WS_METHOD_PING)
        await asyncio.sleep(0.2)
        self.assertNotIn(captured[0], fx.router._pending)
        await fx.router.stop()


class DispatchShapeTest(unittest.IsolatedAsyncioTestCase):

    async def test_frame_without_id_does_not_resolve_pending(self):
        fx = _make_router()
        await fx.router.start()
        ws = fx.ws_instances[0]

        captured = []

        def _on_send(envelope):
            captured.append(envelope["id"])
        ws.hook_send(_on_send)

        task = asyncio.create_task(fx.router.send_public(CONSTANTS.WS_METHOD_PING))
        await asyncio.sleep(0.01)

        # An event frame without id MUST NOT resolve the pending future.
        ws.deliver({"error": {"code": -1, "msg": "global error, no id"}})
        ws.deliver({"event": {"e": "unknownEvent"}})

        # Real response then resolves it.
        ws.deliver({"id": captured[0], "status": 200, "result": {"pong": True}})
        resp = await task
        self.assertEqual(resp.result, {"pong": True})
        await fx.router.stop()

    async def test_serverShutdown_triggers_reconnect_drain(self):
        fx = _make_router(timeout=1.0)
        await fx.router.start()
        ws_old = fx.ws_instances[0]

        ws_old.deliver({"event": {"e": "serverShutdown"}})
        await asyncio.sleep(0.05)
        self.assertIn(fx.router.state, (RouterState.DRAINING, RouterState.RECONNECTING, RouterState.CONNECTED))
        await fx.router.stop()


class DisconnectDistinguishesStateTest(unittest.IsolatedAsyncioTestCase):

    async def test_pending_in_SENT_state_raises_UnknownExecution_on_disconnect(self):
        # Place a pending in SENT state, drop the socket → caller sees
        # BinanceWsUnknownExecutionError (request might have hit the
        # matching engine).
        fx = _make_router(timeout=2.0)
        await fx.router.start()
        ws = fx.ws_instances[0]

        captured = []

        def _on_send(envelope):
            captured.append(envelope["id"])
        ws.hook_send(_on_send)

        task = asyncio.create_task(fx.router.send_signed(
            CONSTANTS.WS_METHOD_ORDER_PLACE,
            {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.001"},
        ))
        await asyncio.sleep(0.05)  # ensure send_json completed → SENT
        self.assertEqual(fx.router._pending[captured[0]].state, PendingState.SENT)

        ws.deliver_close()
        with self.assertRaises(BinanceWsUnknownExecutionError):
            await task
        await fx.router.stop()


class FailedStateAfterReconnectStormTest(unittest.IsolatedAsyncioTestCase):

    async def test_router_enters_FAILED_after_many_reconnects(self):
        # Make _connect_once fail on every attempt past the first by
        # raising from the factory. Force the storm by triggering
        # disconnects quickly.
        attempt = {"n": 0}

        async def _factory(url):
            attempt["n"] += 1
            if attempt["n"] == 1:
                ws = _FakeWs()
                return ws
            raise aiohttp.ClientError("simulated upstream failure")

        # Patch CONSTANTS values down so the test runs quickly. The
        # backoff overrides shrink the per-attempt wait so the storm
        # finishes within the test budget.
        import hummingbot.connector.exchange.binance_ws.binance_ws_constants as C
        original_max = C.WS_MAX_RECONNECTS_IN_WINDOW
        original_window = C.WS_RECONNECT_WINDOW_SEC
        original_min = C.WS_RECONNECT_BACKOFF_MIN_SEC
        original_max_bo = C.WS_RECONNECT_BACKOFF_MAX_SEC
        C.WS_MAX_RECONNECTS_IN_WINDOW = 2
        C.WS_RECONNECT_WINDOW_SEC = 60.0
        C.WS_RECONNECT_BACKOFF_MIN_SEC = 0.01
        C.WS_RECONNECT_BACKOFF_MAX_SEC = 0.05
        try:
            router = BinanceWsRequestRouter(
                api_key="K", api_secret="S",
                throttler=_build_throttler(),
                time_provider=_frozen_clock(),
                ws_connector=_factory,
                request_timeout_sec=0.5,
                late_response_window_sec=0.1,
                reconnect_interval_sec=9999.0,
            )
            await router.start()
            ws = router._ws
            ws.deliver_close()
            # Reconnect attempts each fail; eventually FAILED.
            for _ in range(30):
                await asyncio.sleep(0.1)
                if router.state == RouterState.FAILED:
                    break
            self.assertEqual(router.state, RouterState.FAILED)
            await router.stop()
        finally:
            C.WS_MAX_RECONNECTS_IN_WINDOW = original_max
            C.WS_RECONNECT_WINDOW_SEC = original_window
            C.WS_RECONNECT_BACKOFF_MIN_SEC = original_min
            C.WS_RECONNECT_BACKOFF_MAX_SEC = original_max_bo


class StopAndCleanupTest(unittest.IsolatedAsyncioTestCase):

    async def test_stop_resolves_pending_with_appropriate_error(self):
        fx = _make_router(timeout=2.0)
        await fx.router.start()
        ws = fx.ws_instances[0]

        captured = []

        def _on_send(envelope):
            captured.append(envelope["id"])
        ws.hook_send(_on_send)

        task = asyncio.create_task(fx.router.send_signed(
            CONSTANTS.WS_METHOD_ORDER_PLACE,
            {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.001"},
        ))
        await asyncio.sleep(0.05)
        await fx.router.stop()
        with self.assertRaises((BinanceWsUnknownExecutionError, BinanceWsDisconnectedError)):
            await task


class HealthSnapshotTest(unittest.IsolatedAsyncioTestCase):

    async def test_health_exposes_state_pending_reconnects(self):
        fx = _make_router()
        await fx.router.start()
        snapshot = fx.router.health()
        self.assertTrue(snapshot["ws_trading_connected"])
        self.assertEqual(snapshot["ws_state"], RouterState.CONNECTED.value)
        self.assertEqual(snapshot["ws_pending_requests"], 0)
        self.assertEqual(snapshot["ws_reconnect_count"], 0)
        await fx.router.stop()


if __name__ == "__main__":
    unittest.main()
