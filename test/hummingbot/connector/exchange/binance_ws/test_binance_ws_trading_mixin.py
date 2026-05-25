"""Tests for ``BinanceWsTradingMixin``: place / cancel / reconciliation.

The mixin is tested in isolation from the full ``BinanceWsExchange``
boot sequence: we build a tiny ``_MixinHarness`` class that satisfies
the mixin's contract (``_router``, ``_use_ws_trading``,
``_time_synchronizer``, plus the parent methods the mixin calls into)
with mocks. This keeps the tests fast and focused on the mixin's
branches rather than re-running ``ExchangePyBase.__init__``.
"""
from __future__ import annotations

import asyncio
import logging
import unittest
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.binance_ws.binance_ws_request_router import (
    BinanceWsDisconnectedError,
    BinanceWsRequestError,
    BinanceWsResponse,
    BinanceWsTimeoutError,
    BinanceWsUnknownExecutionError,
)
from hummingbot.connector.exchange.binance_ws.binance_ws_trading_mixin import (
    BinanceWsTradingMixin,
    BinanceWsUnknownCancelError,
)
from hummingbot.core.data_type.common import OrderType, TradeType


# ---------------------------------------------------------------------------
# Harness — minimal "parent" surface area the mixin calls into
# ---------------------------------------------------------------------------

@dataclass
class _StubTrackedOrder:
    trading_pair: str = "BTC-USDT"
    exchange_order_id: Optional[str] = None


class _MixinHarness(BinanceWsTradingMixin):
    """Test rig: stand-in for ``BinanceWsExchange`` without booting it."""

    def __init__(self):
        self._router = MagicMock()
        self._router.send_signed = AsyncMock()
        self._use_ws_trading = True

        self._time_synchronizer = MagicMock()
        self._time_synchronizer.time = MagicMock(return_value=1_700_000_000.0)

        # Tracking of calls to the inherited overrides we substitute.
        self.super_place_called_with: Optional[Dict[str, Any]] = None
        self.super_cancel_called_with: Optional[Dict[str, Any]] = None
        self._api_get_calls: List[Dict[str, Any]] = []
        self._api_get_responses: List[Any] = []

    # The mixin's MRO calls super()._place_order / _place_cancel for the
    # REST fallback path. We don't have a real parent here, so we
    # provide trivial stubs the mixin will delegate to when fallback
    # is triggered.
    async def _super_place_order(self, **kwargs):
        return "REST-FALLBACK-ID", 1.0

    async def _super_cancel(self, **kwargs):
        return True

    # Patch BinanceWsTradingMixin's super() calls by overriding the
    # ``super()._place_order`` lookup via descriptor: easier to just
    # invoke the harness's own methods explicitly in tests for the
    # fallback case — see FallbackToRestTest below.

    def quantize_order_amount(self, trading_pair: str, amount: Decimal) -> Decimal:
        return amount

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        return price

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        return trading_pair.replace("-", "")

    async def _api_get(self, path_url: str, params: Dict[str, Any], is_auth_required: bool):
        self._api_get_calls.append({"path_url": path_url, "params": params,
                                    "is_auth_required": is_auth_required})
        if not self._api_get_responses:
            raise RuntimeError("test forgot to queue an _api_get response")
        nxt = self._api_get_responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    def logger(self) -> logging.Logger:
        return logging.getLogger("test_mixin")


def _ok(result: Dict[str, Any]) -> BinanceWsResponse:
    return BinanceWsResponse(id="x", status=200, result=result)


# ---------------------------------------------------------------------------
# place_order
# ---------------------------------------------------------------------------

class PlaceOrderTest(unittest.IsolatedAsyncioTestCase):

    async def test_limit_buy_sends_correct_params(self):
        h = _MixinHarness()
        h._router.send_signed.return_value = _ok({"orderId": 12345, "transactTime": 1_700_000_500_000})

        oid, ts = await h._place_order(
            order_id="abc", trading_pair="BTC-USDT",
            amount=Decimal("0.01"), trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT, price=Decimal("20000"),
        )
        self.assertEqual(oid, "12345")
        self.assertAlmostEqual(ts, 1_700_000_500.0)

        method, params = h._router.send_signed.call_args.args
        self.assertEqual(method, "order.place")
        self.assertEqual(params["symbol"], "BTCUSDT")
        self.assertEqual(params["side"], "BUY")
        self.assertEqual(params["type"], "LIMIT")
        self.assertEqual(params["quantity"], "0.01")
        self.assertEqual(params["price"], "20000")
        self.assertEqual(params["timeInForce"], "GTC")
        self.assertEqual(params["newClientOrderId"], "abc")
        self.assertEqual(params["newOrderRespType"], "ACK")

    async def test_limit_maker_does_not_send_timeInForce(self):
        # Binance rejects timeInForce on LIMIT_MAKER. Pinning this
        # protects the XEMM strategy which post-only's exclusively.
        h = _MixinHarness()
        h._router.send_signed.return_value = _ok({"orderId": 1, "transactTime": 1_700_000_500_000})

        await h._place_order(
            order_id="o", trading_pair="BTC-USDT",
            amount=Decimal("0.01"), trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT_MAKER, price=Decimal("21000"),
        )
        _, params = h._router.send_signed.call_args.args
        self.assertEqual(params["type"], "LIMIT_MAKER")
        self.assertEqual(params["price"], "21000")
        self.assertNotIn("timeInForce", params)

    async def test_market_omits_price_and_timeInForce(self):
        h = _MixinHarness()
        h._router.send_signed.return_value = _ok({"orderId": 1, "transactTime": 1_700_000_500_000})

        await h._place_order(
            order_id="o", trading_pair="BTC-USDT",
            amount=Decimal("0.01"), trade_type=TradeType.BUY,
            order_type=OrderType.MARKET, price=Decimal("0"),
        )
        _, params = h._router.send_signed.call_args.args
        self.assertEqual(params["type"], "MARKET")
        self.assertNotIn("price", params)
        self.assertNotIn("timeInForce", params)

    async def test_timeout_returns_UNKNOWN(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsTimeoutError("timed out")
        oid, ts = await h._place_order(
            order_id="o", trading_pair="BTC-USDT",
            amount=Decimal("0.01"), trade_type=TradeType.BUY,
            order_type=OrderType.MARKET, price=Decimal("0"),
        )
        self.assertEqual(oid, "UNKNOWN")
        self.assertEqual(ts, 1_700_000_000.0)

    async def test_unknown_execution_returns_UNKNOWN(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsUnknownExecutionError("503")
        oid, _ = await h._place_order(
            order_id="o", trading_pair="BTC-USDT",
            amount=Decimal("0.01"), trade_type=TradeType.BUY,
            order_type=OrderType.MARKET, price=Decimal("0"),
        )
        self.assertEqual(oid, "UNKNOWN")

    async def test_disconnected_returns_UNKNOWN(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsDisconnectedError("dropped")
        oid, _ = await h._place_order(
            order_id="o", trading_pair="BTC-USDT",
            amount=Decimal("0.01"), trade_type=TradeType.BUY,
            order_type=OrderType.MARKET, price=Decimal("0"),
        )
        self.assertEqual(oid, "UNKNOWN")

    async def test_duplicate_clientOrderId_recovers_via_rest_status(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsRequestError(
            -2010, "Duplicate order sent."
        )
        h._api_get_responses.append({"orderId": 99, "transactTime": 1_700_000_500_000})

        oid, ts = await h._place_order(
            order_id="dup-id", trading_pair="BTC-USDT",
            amount=Decimal("0.01"), trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT, price=Decimal("20000"),
        )
        self.assertEqual(oid, "99")
        self.assertAlmostEqual(ts, 1_700_000_500.0)
        self.assertEqual(h._api_get_calls[0]["path_url"], "/order")
        self.assertEqual(h._api_get_calls[0]["params"]["origClientOrderId"], "dup-id")

    async def test_duplicate_when_rest_lookup_fails_propagates_original_error(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsRequestError(
            -2010, "Duplicate order sent."
        )
        h._api_get_responses.append(RuntimeError("rest down"))

        with self.assertRaises(BinanceWsRequestError):
            await h._place_order(
                order_id="dup-id", trading_pair="BTC-USDT",
                amount=Decimal("0.01"), trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT, price=Decimal("20000"),
            )

    async def test_non_duplicate_request_error_propagates(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsRequestError(
            -2010, "Account has insufficient balance."
        )
        with self.assertRaises(BinanceWsRequestError):
            await h._place_order(
                order_id="o", trading_pair="BTC-USDT",
                amount=Decimal("0.01"), trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT, price=Decimal("20000"),
            )
        # REST status NOT consulted on non-duplicate errors.
        self.assertEqual(len(h._api_get_calls), 0)


# ---------------------------------------------------------------------------
# place_cancel
# ---------------------------------------------------------------------------

class PlaceCancelTest(unittest.IsolatedAsyncioTestCase):

    async def test_fast_path_uses_orderId_when_known(self):
        h = _MixinHarness()
        h._router.send_signed.return_value = _ok({"status": "CANCELED"})

        tracked = _StubTrackedOrder(trading_pair="BTC-USDT", exchange_order_id="555")
        ok = await h._place_cancel(order_id="c-id", tracked_order=tracked)
        self.assertTrue(ok)
        _, params = h._router.send_signed.call_args.args
        self.assertEqual(params["orderId"], 555)
        self.assertNotIn("origClientOrderId", params)

    async def test_fallback_to_origClientOrderId_when_exchange_id_unknown(self):
        h = _MixinHarness()
        h._router.send_signed.return_value = _ok({"status": "CANCELED"})

        tracked = _StubTrackedOrder(trading_pair="BTC-USDT", exchange_order_id="UNKNOWN")
        ok = await h._place_cancel(order_id="c-id", tracked_order=tracked)
        self.assertTrue(ok)
        _, params = h._router.send_signed.call_args.args
        self.assertEqual(params["origClientOrderId"], "c-id")
        self.assertNotIn("orderId", params)

    async def test_fallback_to_origClientOrderId_when_exchange_id_None(self):
        h = _MixinHarness()
        h._router.send_signed.return_value = _ok({"status": "CANCELED"})
        tracked = _StubTrackedOrder(trading_pair="BTC-USDT", exchange_order_id=None)
        await h._place_cancel(order_id="c-id", tracked_order=tracked)
        _, params = h._router.send_signed.call_args.args
        self.assertEqual(params["origClientOrderId"], "c-id")
        self.assertNotIn("orderId", params)

    async def test_never_sends_both_orderId_and_clientOrderId(self):
        h = _MixinHarness()
        h._router.send_signed.return_value = _ok({"status": "CANCELED"})
        tracked = _StubTrackedOrder(trading_pair="BTC-USDT", exchange_order_id="555")
        await h._place_cancel(order_id="c-id", tracked_order=tracked)
        _, params = h._router.send_signed.call_args.args
        self.assertTrue(("orderId" in params) ^ ("origClientOrderId" in params))

    async def test_returns_True_only_on_CANCELED(self):
        h = _MixinHarness()
        h._router.send_signed.return_value = _ok({"status": "CANCELED"})
        ok = await h._place_cancel(order_id="c", tracked_order=_StubTrackedOrder(exchange_order_id="5"))
        self.assertTrue(ok)

    async def test_returns_False_on_NEW_or_PARTIALLY_FILLED(self):
        h = _MixinHarness()
        for status in ("NEW", "PARTIALLY_FILLED"):
            with self.subTest(status=status):
                h._router.send_signed.return_value = _ok({"status": status})
                ok = await h._place_cancel(order_id="c", tracked_order=_StubTrackedOrder(exchange_order_id="5"))
                self.assertFalse(ok)

    async def test_returns_False_on_terminal_states_with_log(self):
        h = _MixinHarness()
        for status in ("FILLED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH"):
            with self.subTest(status=status):
                h._router.send_signed.return_value = _ok({"status": status})
                ok = await h._place_cancel(order_id="c", tracked_order=_StubTrackedOrder(exchange_order_id="5"))
                self.assertFalse(ok)

    async def test_code_neg_2011_returns_False(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsRequestError(-2011, "Unknown order sent.")
        ok = await h._place_cancel(order_id="c", tracked_order=_StubTrackedOrder(exchange_order_id="5"))
        self.assertFalse(ok)

    async def test_unknown_execution_consults_REST_then_decides(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsUnknownExecutionError("disconnect after send")
        h._api_get_responses.append({"status": "CANCELED"})
        ok = await h._place_cancel(order_id="c", tracked_order=_StubTrackedOrder(exchange_order_id="5"))
        self.assertTrue(ok)
        self.assertEqual(len(h._api_get_calls), 1)

    async def test_unknown_execution_rest_says_NEW_returns_False(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsUnknownExecutionError("disconnect")
        h._api_get_responses.append({"status": "NEW"})
        ok = await h._place_cancel(order_id="c", tracked_order=_StubTrackedOrder(exchange_order_id="5"))
        self.assertFalse(ok)

    async def test_unknown_execution_rest_terminal_returns_False(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsUnknownExecutionError("disconnect")
        h._api_get_responses.append({"status": "FILLED"})
        ok = await h._place_cancel(order_id="c", tracked_order=_StubTrackedOrder(exchange_order_id="5"))
        self.assertFalse(ok)

    async def test_unknown_execution_rest_failed_raises_UnknownCancelError(self):
        h = _MixinHarness()
        h._router.send_signed.side_effect = BinanceWsTimeoutError("timeout")
        h._api_get_responses.append(RuntimeError("rest unreachable"))
        with self.assertRaises(BinanceWsUnknownCancelError):
            await h._place_cancel(order_id="c", tracked_order=_StubTrackedOrder(exchange_order_id="5"))


# ---------------------------------------------------------------------------
# Fallback path when use_ws_trading=False
# ---------------------------------------------------------------------------

class FallbackToRestTest(unittest.IsolatedAsyncioTestCase):
    """When the operator turns off WS trading, mixin must delegate to
    super()._place_order / super()._place_cancel — we test by chaining
    a stub parent under the mixin in a fresh MRO."""

    async def test_place_order_falls_back_to_super(self):
        class _Parent:
            async def _place_order(self, **kwargs):
                return "rest-id", 42.0

        class _Combo(BinanceWsTradingMixin, _Parent):
            def __init__(self):
                self._router = None
                self._use_ws_trading = False
                self._time_synchronizer = MagicMock(time=lambda: 0)

            def logger(self):
                return logging.getLogger("combo")

        c = _Combo()
        oid, ts = await c._place_order(
            order_id="x", trading_pair="BTC-USDT",
            amount=Decimal("0.01"), trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT, price=Decimal("20000"),
        )
        self.assertEqual(oid, "rest-id")
        self.assertEqual(ts, 42.0)

    async def test_place_cancel_falls_back_to_super(self):
        class _Parent:
            async def _place_cancel(self, **kwargs):
                return True

        class _Combo(BinanceWsTradingMixin, _Parent):
            def __init__(self):
                self._router = None
                self._use_ws_trading = False
                self._time_synchronizer = MagicMock(time=lambda: 0)

            def logger(self):
                return logging.getLogger("combo")

        c = _Combo()
        ok = await c._place_cancel(order_id="x", tracked_order=_StubTrackedOrder())
        self.assertTrue(ok)


# ---------------------------------------------------------------------------
# Composition smoke for the future BinanceSbeWsExchange
# ---------------------------------------------------------------------------

class MixinComposesWithSbeExchangeTest(unittest.TestCase):
    """Pin the MRO/composability claim from the plan: the mixin must
    compose cleanly with BinanceSbeExchange to produce a (currently
    hypothetical) BinanceSbeWsExchange. We don't instantiate — just
    verify the class definition succeeds and method resolution lands
    on the mixin's overrides."""

    def test_class_definition_succeeds(self):
        from hummingbot.connector.exchange.binance_sbe.binance_sbe_exchange import (
            BinanceSbeExchange,
        )

        class _Hypothetical(BinanceWsTradingMixin, BinanceSbeExchange):
            pass

        # Both _place_order and _place_cancel resolve to mixin versions.
        self.assertIs(
            _Hypothetical._place_order, BinanceWsTradingMixin._place_order
        )
        self.assertIs(
            _Hypothetical._place_cancel, BinanceWsTradingMixin._place_cancel
        )


if __name__ == "__main__":
    unittest.main()
