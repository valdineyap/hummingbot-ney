"""Tests for MARKET BUY volume conversion on BitPreco.

BitPreco's MARKET BUY API interprets `amount` as quote currency (BRL),
not base currency (BTC) like its LIMIT and MARKET SELL counterparts.
Every MARKET BUY attempt in the 2026-05-05 → 2026-05-11 log history
was rejected with ``BELOW_MINIMUM_VOLUME`` + ``requested_volume: 0``
because the connector sent the BTC amount directly. ``_place_order``
now converts ``amount_btc`` → ``amount_brl = amount_btc * best_ask * 1.01``
for this specific path; this test pins the behaviour.
"""

import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange
from hummingbot.core.data_type.common import OrderType, TradeType


def _make_exchange(best_ask: Decimal) -> BitprecoExchange:
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    ex._add_auth_token_to_req_body = lambda body: {**body, "auth_token": "stub"}
    # exchange_symbol_associated_to_pair is async on the real class.
    ex.exchange_symbol_associated_to_pair = AsyncMock(return_value="BTC-BRL")
    # get_price is the synchronous best-bid/ask accessor on the base class.
    ex.get_price = MagicMock(return_value=best_ask)
    return ex


class MarketBuyVolumeConversionTest(unittest.TestCase):

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_market_buy_converts_btc_to_brl_volume(self):
        """For MARKET BUY, the payload's `amount` must be in BRL (volume to
        spend), computed as base_amount * best_ask * 1.01."""
        captured_data = {}

        async def _fake_request(method, path_url, data, **kw):
            captured_data.update(data)
            return {"success": True, "order_id": "O-1"}

        ex = _make_exchange(best_ask=Decimal("400000"))
        ex._api_request = AsyncMock(side_effect=_fake_request)

        self._run(ex._place_order(
            order_id="BBCBL_test_market_buy",
            trading_pair="BTC-BRL",
            amount=Decimal("0.0002"),
            trade_type=TradeType.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("0"),
        ))

        # `amount` field now holds BRL volume, not BTC.
        # 0.0002 * 400000 * 1.01 = 80.80 BRL
        self.assertEqual(captured_data["amount"], "80.80")
        self.assertEqual(captured_data["limited"], False)
        self.assertEqual(captured_data["cmd"], "buy")
        # Price not used by BitPreco for MARKET BUY; we send 0.
        self.assertEqual(captured_data["price"], "0")

    def test_market_sell_keeps_btc_amount(self):
        """MARKET SELL must keep amount in BTC (base) — that path already
        works in production and must NOT be touched."""
        captured_data = {}

        async def _fake_request(method, path_url, data, **kw):
            captured_data.update(data)
            return {"success": True, "order_id": "O-2"}

        ex = _make_exchange(best_ask=Decimal("400000"))
        ex._api_request = AsyncMock(side_effect=_fake_request)

        self._run(ex._place_order(
            order_id="SBCBL_test_market_sell",
            trading_pair="BTC-BRL",
            amount=Decimal("0.0002"),
            trade_type=TradeType.SELL,
            order_type=OrderType.MARKET,
            price=Decimal("0"),
        ))

        # amount stays as BTC (base) for SELL.
        self.assertEqual(captured_data["amount"], "0.0002")
        self.assertEqual(captured_data["cmd"], "sell")
        self.assertEqual(captured_data["limited"], False)

    def test_limit_buy_keeps_btc_amount(self):
        """LIMIT BUY (LIMIT_MAKER) must keep amount in BTC — the conversion
        is MARKET-BUY-only."""
        captured_data = {}

        async def _fake_request(method, path_url, data, **kw):
            captured_data.update(data)
            return {"success": True, "order_id": "O-3"}

        ex = _make_exchange(best_ask=Decimal("400000"))
        ex._api_request = AsyncMock(side_effect=_fake_request)

        self._run(ex._place_order(
            order_id="BBCBL_test_limit_buy",
            trading_pair="BTC-BRL",
            amount=Decimal("0.0002"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT_MAKER,
            price=Decimal("399000"),
        ))

        self.assertEqual(captured_data["amount"], "0.0002")
        self.assertEqual(captured_data["limited"], True)
        # LIMIT BUY price stays in BRL/BTC with integer rounding (floor).
        self.assertEqual(captured_data["price"], "399000")

    def test_market_buy_with_high_price_produces_high_brl_volume(self):
        """Sanity: a high best_ask scales the BRL volume linearly."""
        captured_data = {}

        async def _fake_request(method, path_url, data, **kw):
            captured_data.update(data)
            return {"success": True, "order_id": "O-4"}

        ex = _make_exchange(best_ask=Decimal("500000"))
        ex._api_request = AsyncMock(side_effect=_fake_request)

        self._run(ex._place_order(
            order_id="BBCBL_test_market_buy_high",
            trading_pair="BTC-BRL",
            amount=Decimal("0.001"),
            trade_type=TradeType.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("0"),
        ))

        # 0.001 * 500000 * 1.01 = 505.00 BRL
        self.assertEqual(captured_data["amount"], "505.00")

    def test_market_buy_brl_volume_rounded_up(self):
        """Quote volume must round UP to 0.01 BRL precision so we never
        send slightly LESS than required and trigger BELOW_MINIMUM_VOLUME.
        """
        captured_data = {}

        async def _fake_request(method, path_url, data, **kw):
            captured_data.update(data)
            return {"success": True, "order_id": "O-5"}

        ex = _make_exchange(best_ask=Decimal("396039.9933"))
        ex._api_request = AsyncMock(side_effect=_fake_request)

        self._run(ex._place_order(
            order_id="BBCBL_test_round_up",
            trading_pair="BTC-BRL",
            amount=Decimal("0.0001"),  # tiny amount → tight rounding sensitivity
            trade_type=TradeType.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("0"),
        ))

        # 0.0001 * 396039.9933 * 1.01 = 40.00003932833 → rounds up to 40.01
        # (truncating down to 40.00 would risk BELOW_MINIMUM_VOLUME for
        # marginal sizes — ROUND_UP guarantees we stay above the threshold).
        sent = Decimal(captured_data["amount"])
        self.assertGreaterEqual(sent, Decimal("40.00"))
        self.assertLess(sent, Decimal("40.10"))
        # Two decimal places only.
        self.assertEqual(sent.as_tuple().exponent, -2)


if __name__ == "__main__":
    unittest.main()
