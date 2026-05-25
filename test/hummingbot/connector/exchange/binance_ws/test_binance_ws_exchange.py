"""Tests for ``BinanceWsExchange``: identity, boot trapdoors, network
status, mixin wiring.

Heavier mixin behaviour (place / cancel / reconciliation) lives in
``test_binance_ws_trading_mixin.py``.
"""
from __future__ import annotations

import asyncio
import unittest

from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange
from hummingbot.connector.exchange.binance_ws.binance_ws_exchange import BinanceWsExchange
from hummingbot.connector.exchange.binance_ws.binance_ws_request_router import (
    BinanceWsRequestRouter,
)
from hummingbot.connector.exchange.binance_ws.binance_ws_trading_mixin import (
    BinanceWsTradingMixin,
)
from hummingbot.core.network_iterator import NetworkStatus


def _ensure_event_loop() -> None:
    # IsolatedAsyncioTestCase closes its loop after each test, which
    # under Python 3.13 leaves asyncio.get_event_loop() raising in
    # sync test classes that run later in the same process. Re-install
    # a fresh loop so BinanceExchange.__init__ (which calls
    # get_event_loop on the OrderBookTracker) doesn't blow up.
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


def _make_exchange(*, trading_required: bool = False, use_ws_trading: bool = True,
                   domain: str = "com") -> BinanceWsExchange:
    _ensure_event_loop()
    return BinanceWsExchange(
        binance_ws_api_key="hmac-key",
        binance_ws_api_secret="hmac-secret",
        use_ws_trading=use_ws_trading,
        trading_pairs=["BTC-USDT"],
        trading_required=trading_required,
        domain=domain,
    )


class IdentityTest(unittest.TestCase):

    def test_name_for_com(self):
        ex = _make_exchange()
        self.assertEqual(ex.name, "binance_ws")

    def test_name_for_non_com(self):
        ex = _make_exchange(domain="us")
        self.assertEqual(ex.name, "binance_ws_us")

    def test_inherits_binance_exchange(self):
        ex = _make_exchange()
        self.assertIsInstance(ex, BinanceExchange)

    def test_uses_trading_mixin(self):
        ex = _make_exchange()
        self.assertIsInstance(ex, BinanceWsTradingMixin)


class BootTrapdoorTest(unittest.TestCase):

    def test_trading_required_with_empty_hmac_raises(self):
        _ensure_event_loop()
        with self.assertRaises(ValueError) as cm:
            BinanceWsExchange(
                binance_ws_api_key="",
                binance_ws_api_secret="",
                trading_pairs=["BTC-USDT"],
                trading_required=True,
            )
        self.assertIn("HMAC", str(cm.exception))
        self.assertIn("trading_required", str(cm.exception))
        self.assertIn("binance_ws_register", str(cm.exception))

    def test_trading_required_with_partial_hmac_raises(self):
        _ensure_event_loop()
        with self.assertRaises(ValueError):
            BinanceWsExchange(
                binance_ws_api_key="hmac-key",
                binance_ws_api_secret="",
                trading_pairs=["BTC-USDT"],
                trading_required=True,
            )

    def test_trading_required_false_allows_empty_hmac(self):
        _ensure_event_loop()
        ex = BinanceWsExchange(
            binance_ws_api_key="",
            binance_ws_api_secret="",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
        )
        self.assertFalse(ex._trading_required)


class RouterWiringTest(unittest.TestCase):

    def test_router_constructed_when_use_ws_trading_true(self):
        ex = _make_exchange(use_ws_trading=True)
        self.assertIsNotNone(ex._router)
        self.assertIsInstance(ex._router, BinanceWsRequestRouter)
        # Router shares the exchange's throttler — same instance.
        self.assertIs(ex._router._throttler, ex._throttler)

    def test_router_absent_when_use_ws_trading_false(self):
        ex = _make_exchange(use_ws_trading=False)
        self.assertIsNone(ex._router)
        self.assertFalse(ex._use_ws_trading)

    def test_env_var_overrides_use_ws_trading_to_false(self):
        import os
        old = os.environ.get("BINANCE_WS_USE_WS_TRADING")
        os.environ["BINANCE_WS_USE_WS_TRADING"] = "false"
        try:
            ex = _make_exchange(use_ws_trading=True)
            self.assertFalse(ex._use_ws_trading)
        finally:
            if old is None:
                os.environ.pop("BINANCE_WS_USE_WS_TRADING", None)
            else:
                os.environ["BINANCE_WS_USE_WS_TRADING"] = old


class RateLimitsTest(unittest.TestCase):

    def test_rate_limits_extend_not_replace_binance(self):
        ex = _make_exchange()
        ids = {rl.limit_id for rl in ex.rate_limits_rules}
        # Inherited REST IDs are present.
        from hummingbot.connector.exchange.binance.binance_constants import REQUEST_WEIGHT, ORDERS
        self.assertIn(REQUEST_WEIGHT, ids)
        self.assertIn(ORDERS, ids)
        # WS-specific ids added.
        self.assertIn("WS_ORDER_PLACE", ids)
        self.assertIn("WS_ORDER_CANCEL", ids)


class CheckNetworkTest(unittest.IsolatedAsyncioTestCase):

    async def test_returns_NOT_CONNECTED_when_router_disconnected(self):
        ex = _make_exchange()
        # Force REST happy path: stub _make_network_check_request to a no-op.
        async def _noop():
            return None
        ex._make_network_check_request = _noop  # type: ignore[assignment]
        # Router is constructed but never started → not connected.
        self.assertFalse(ex._router.connected)
        status = await ex.check_network()
        self.assertEqual(status, NetworkStatus.NOT_CONNECTED)

    async def test_returns_CONNECTED_when_use_ws_trading_false(self):
        ex = _make_exchange(use_ws_trading=False)
        async def _noop():
            return None
        ex._make_network_check_request = _noop  # type: ignore[assignment]
        status = await ex.check_network()
        self.assertEqual(status, NetworkStatus.CONNECTED)


class HealthDictTest(unittest.TestCase):

    def test_health_when_router_present(self):
        ex = _make_exchange()
        h = ex._ws_health_dict()
        self.assertIn("ws_trading_connected", h)
        self.assertIn("ws_state", h)
        self.assertIn("ws_reconnect_count", h)
        self.assertIn("ws_pending_requests", h)

    def test_health_when_router_absent(self):
        ex = _make_exchange(use_ws_trading=False)
        h = ex._ws_health_dict()
        self.assertFalse(h["ws_trading_connected"])
        self.assertEqual(h["ws_state"], "absent")


class DataSourceFactoryIntactTest(unittest.TestCase):
    """The point of this connector is the trading-send override; market
    data must continue using the JSON connector's data source. If a
    future change accidentally substitutes a different data source,
    this test catches it."""

    def test_data_source_factory_is_the_one_from_BinanceExchange(self):
        self.assertIs(
            BinanceWsExchange._create_order_book_data_source,
            BinanceExchange._create_order_book_data_source,
        )


if __name__ == "__main__":
    unittest.main()
