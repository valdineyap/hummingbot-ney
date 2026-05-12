"""Tests for ``BitprecoExchange.fetch_fresh_vwap``.

The method bypasses the 500ms REST-poll-based OrderBook cache (see
bitpreco_api_order_book_data_source.py) by issuing one direct snapshot
request and computing VWAP over the returned levels. Used by the
controller's arb spawn gate to reduce staleness-induced slippage.
"""
import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


def _make_exchange(snapshot_response):
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    ex._orderbook_ds = MagicMock()
    ex._orderbook_ds._request_order_book_snapshot = AsyncMock(return_value=snapshot_response)
    return ex


class FetchFreshVwapTest(unittest.TestCase):

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_buy_walks_asks_in_order(self):
        # Asks ascending — VWAP for 0.0003 BTC eats L1 (0.0001 @ 100k)
        # + 0.0002 of L2 (@ 100,100) → (0.0001*100000 + 0.0002*100100) / 0.0003
        snapshot = {
            "asks": [
                {"price": "100000", "amount": "0.0001", "id": "1"},
                {"price": "100100", "amount": "0.0005", "id": "2"},
                {"price": "100500", "amount": "0.001", "id": "3"},
            ],
            "bids": [],
        }
        ex = _make_exchange(snapshot)
        vwap = self._run(ex.fetch_fresh_vwap("BTC-BRL", is_buy=True, amount=Decimal("0.0003")))
        expected = (Decimal("100000") * Decimal("0.0001")
                    + Decimal("100100") * Decimal("0.0002")) / Decimal("0.0003")
        self.assertEqual(vwap, expected)

    def test_sell_walks_bids_in_order(self):
        # Bids descending — VWAP for 0.0002 BTC eats 0.0001 @ 99,900 + 0.0001 @ 99,800
        snapshot = {
            "asks": [],
            "bids": [
                {"price": "99900", "amount": "0.0001", "id": "1"},
                {"price": "99800", "amount": "0.0003", "id": "2"},
            ],
        }
        ex = _make_exchange(snapshot)
        vwap = self._run(ex.fetch_fresh_vwap("BTC-BRL", is_buy=False, amount=Decimal("0.0002")))
        expected = (Decimal("99900") * Decimal("0.0001")
                    + Decimal("99800") * Decimal("0.0001")) / Decimal("0.0002")
        self.assertEqual(vwap, expected)

    def test_returns_none_on_insufficient_depth(self):
        # Only 0.0001 BTC available, but caller wants 0.0005
        snapshot = {"asks": [{"price": "100000", "amount": "0.0001", "id": "1"}],
                    "bids": []}
        ex = _make_exchange(snapshot)
        vwap = self._run(ex.fetch_fresh_vwap("BTC-BRL", is_buy=True, amount=Decimal("0.0005")))
        self.assertIsNone(vwap)

    def test_returns_none_on_empty_side(self):
        snapshot = {"asks": [], "bids": [{"price": "99000", "amount": "0.001", "id": "1"}]}
        ex = _make_exchange(snapshot)
        vwap = self._run(ex.fetch_fresh_vwap("BTC-BRL", is_buy=True, amount=Decimal("0.0001")))
        self.assertIsNone(vwap)

    def test_returns_none_on_rest_exception(self):
        ex = BitprecoExchange.__new__(BitprecoExchange)
        ex.logger = lambda: MagicMock()
        ex._orderbook_ds = MagicMock()
        ex._orderbook_ds._request_order_book_snapshot = AsyncMock(
            side_effect=ConnectionError("network down"))
        vwap = self._run(ex.fetch_fresh_vwap("BTC-BRL", is_buy=True, amount=Decimal("0.0001")))
        self.assertIsNone(vwap)

    def test_exact_amount_at_l1_returns_l1_price(self):
        snapshot = {"asks": [{"price": "100000", "amount": "0.0001", "id": "1"}],
                    "bids": []}
        ex = _make_exchange(snapshot)
        vwap = self._run(ex.fetch_fresh_vwap("BTC-BRL", is_buy=True, amount=Decimal("0.0001")))
        self.assertEqual(vwap, Decimal("100000"))


if __name__ == "__main__":
    unittest.main()
