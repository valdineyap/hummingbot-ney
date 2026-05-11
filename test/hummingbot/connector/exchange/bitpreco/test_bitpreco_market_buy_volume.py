"""Tests for MARKET BUY payload on BitPreco.

Per BitPreco API docs (https://apidocs.bitpreco.com/):

  > When limited is set to false, the price and amount are not considered,
  > therefore, ignored. ... Turns volume mandatory.

So MARKET orders need a `volume` (BRL) field in the payload. The connector
keeps the framework's BTC ``amount`` and adds a ``volume`` field for the
BUY case, computed as ``amount(BTC) * best_ask * 1.01`` (1% buffer so the
final BTC fill is ≥ what was requested even with book walk).

MARKET SELL has a long history of successful executions using just
``amount`` (BTC) without an explicit ``volume`` — kept unchanged to avoid
regressing the working path.

History of previous (failed) attempts:
- 2026-05-11 01:10  ``price=synthesized = best_ask * 1.01``, amount=BTC.
                    Wrong hypothesis: ``volume = amount × price`` server-side.
- 2026-05-11        ``amount`` reinterpreted as BRL, ``price=0``.
                    Wrong hypothesis: BitPreco reads ``amount`` as BRL for
                    MARKET BUY. Server response ``requested_volume: 0``
                    confirmed BitPreco never saw a ``volume`` field at all.
- 2026-05-11 18:30  (this commit) Add ``volume`` field. Matches docs.
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
    ex.exchange_symbol_associated_to_pair = AsyncMock(return_value="BTC-BRL")
    ex.get_price = MagicMock(return_value=best_ask)
    return ex


class MarketBuyVolumePayloadTest(unittest.TestCase):

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def _place(self, ex, **kw):
        defaults = dict(
            order_id="BBCBL_test",
            trading_pair="BTC-BRL",
            amount=Decimal("0.0002"),
            trade_type=TradeType.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("0"),
        )
        defaults.update(kw)
        return self._run(ex._place_order(**defaults))

    def _capture_payload(self):
        captured = {}

        async def _fake_request(method, path_url, data, **kw):
            captured.update(data)
            return {"success": True, "order_id": "O-1"}

        return captured, _fake_request

    def test_market_buy_payload_includes_volume_in_brl(self):
        """MARKET BUY: payload MUST contain `volume` (BRL) = amount * best_ask * 1.01."""
        captured, fake = self._capture_payload()
        ex = _make_exchange(best_ask=Decimal("400000"))
        ex._api_request = AsyncMock(side_effect=fake)

        self._place(ex, amount=Decimal("0.0002"))

        # New required field — this is the actual fix.
        self.assertIn("volume", captured)
        # 0.0002 * 400000 * 1.01 = 80.80 BRL
        self.assertEqual(captured["volume"], "80.80")
        self.assertEqual(captured["limited"], False)
        self.assertEqual(captured["cmd"], "buy")
        # amount field stays as BTC (ignored server-side per docs but kept
        # in payload for backward compatibility / clarity).
        self.assertEqual(captured["amount"], "0.0002")

    def test_market_buy_volume_rounds_up_to_two_decimals(self):
        """Volume must round UP to 0.01 BRL so we never under-spend and
        trigger BELOW_MINIMUM_VOLUME at the margin."""
        captured, fake = self._capture_payload()
        ex = _make_exchange(best_ask=Decimal("396039.9933"))
        ex._api_request = AsyncMock(side_effect=fake)

        self._place(ex, amount=Decimal("0.0001"))

        # 0.0001 * 396039.9933 * 1.01 = 40.00003932833 → round up → 40.01
        sent = Decimal(captured["volume"])
        self.assertGreaterEqual(sent, Decimal("40.00"))
        self.assertLess(sent, Decimal("40.10"))
        self.assertEqual(sent.as_tuple().exponent, -2)

    def test_market_buy_volume_includes_1pct_buffer(self):
        """1% buffer = ceiling_price * 1.01 — protects against book walk."""
        captured, fake = self._capture_payload()
        ex = _make_exchange(best_ask=Decimal("500000"))
        ex._api_request = AsyncMock(side_effect=fake)

        self._place(ex, amount=Decimal("0.001"))

        # 0.001 * 500000 * 1.01 = 505.00
        self.assertEqual(captured["volume"], "505.00")

    def test_market_sell_does_not_include_volume(self):
        """MARKET SELL keeps the historical working payload: amount=BTC,
        no volume field. Many successful prod executions on this path."""
        captured, fake = self._capture_payload()
        ex = _make_exchange(best_ask=Decimal("400000"))
        ex._api_request = AsyncMock(side_effect=fake)

        self._place(ex, trade_type=TradeType.SELL)

        self.assertNotIn("volume", captured)
        self.assertEqual(captured["amount"], "0.0002")
        self.assertEqual(captured["cmd"], "sell")
        self.assertEqual(captured["limited"], False)

    def test_limit_buy_does_not_include_volume(self):
        """LIMIT BUY/LIMIT_MAKER: volume field is MARKET-BUY-only."""
        captured, fake = self._capture_payload()
        ex = _make_exchange(best_ask=Decimal("400000"))
        ex._api_request = AsyncMock(side_effect=fake)

        self._place(
            ex,
            order_type=OrderType.LIMIT_MAKER,
            price=Decimal("399000"),
        )

        self.assertNotIn("volume", captured)
        self.assertEqual(captured["amount"], "0.0002")
        self.assertEqual(captured["limited"], True)
        self.assertEqual(captured["price"], "399000")

    def test_market_buy_when_get_price_fails_falls_back_safely(self):
        """If best_ask is unavailable, log error and DO NOT add `volume`
        (BitPreco will reject — operator sees the error rather than us
        sending a silent zero-volume order)."""
        captured, fake = self._capture_payload()
        ex = _make_exchange(best_ask=Decimal("400000"))
        ex.get_price = MagicMock(return_value=None)  # broken price feed
        ex._api_request = AsyncMock(side_effect=fake)

        self._place(ex)

        # Volume should NOT be set when ceiling_price unavailable.
        self.assertNotIn("volume", captured)


if __name__ == "__main__":
    unittest.main()
