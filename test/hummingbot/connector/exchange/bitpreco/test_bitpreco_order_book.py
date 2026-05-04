from unittest import TestCase

from hummingbot.connector.exchange.bitpreco.bitpreco_order_book import BitprecoOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class BitprecoOrderBookTests(TestCase):

    def test_snapshot_message_from_exchange_rest(self):
        msg = {
            "bids": [
                {"price": "100000.00", "amount": "0.5", "id": "1"},
                {"price": "99000.00", "amount": "1.0", "id": "2"},
            ],
            "asks": [
                {"price": "101000.00", "amount": "0.3", "id": "3"},
                {"price": "102000.00", "amount": "0.8", "id": "4"},
            ],
        }
        timestamp = 1640000000.0
        metadata = {"trading_pair": "BTC-BRL"}

        snapshot = BitprecoOrderBook.snapshot_message_from_exchange_rest(
            msg=msg,
            timestamp=timestamp,
            metadata=metadata,
        )

        self.assertEqual("BTC-BRL", snapshot.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot.type)
        self.assertEqual(timestamp, snapshot.timestamp)
        self.assertEqual(2, len(snapshot.bids))
        self.assertEqual(2, len(snapshot.asks))
        self.assertEqual(100000.0, snapshot.bids[0].price)
        self.assertEqual(0.5, snapshot.bids[0].amount)
        self.assertEqual(timestamp, snapshot.bids[0].update_id)
        self.assertEqual(99000.0, snapshot.bids[1].price)
        self.assertEqual(101000.0, snapshot.asks[0].price)
        self.assertEqual(0.3, snapshot.asks[0].amount)
        self.assertEqual(timestamp, snapshot.asks[0].update_id)

    def test_snapshot_message_from_exchange_rest_update_id_is_timestamp(self):
        msg = {
            "bids": [{"price": "100000.00", "amount": "0.5", "id": "1"}],
            "asks": [{"price": "101000.00", "amount": "0.3", "id": "2"}],
        }
        timestamp = 1640000001.5
        snapshot = BitprecoOrderBook.snapshot_message_from_exchange_rest(
            msg=msg, timestamp=timestamp, metadata={"trading_pair": "ETH-BRL"}
        )
        self.assertEqual(timestamp, snapshot.update_id)

    def test_snapshot_message_from_exchange_websocket(self):
        msg = {
            "t": 12345,
            "b": [["100000.0", "0.5"], ["99000.0", "1.0"]],
            "a": [["101000.0", "0.3"]],
        }
        snapshot = BitprecoOrderBook.snapshot_message_from_exchange_websocket(
            msg=msg,
            timestamp=1640000000.0,
            metadata={"trading_pair": "BTC-BRL"},
        )
        self.assertEqual("BTC-BRL", snapshot.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot.type)
        self.assertEqual(12345, snapshot.update_id)

    def test_diff_message_from_exchange(self):
        msg = {
            "event": "update",
            "topic": "orderbook:BTC-BRL",
            "payload": {
                "bids": [
                    {"price": "99500.00", "amount": "0.2", "id": "10"},
                ],
                "asks": [
                    {"price": "100500.00", "amount": "0.4", "id": "11"},
                ],
            },
        }
        timestamp = 1640000010.0
        metadata = {"trading_pair": "BTC-BRL"}

        diff = BitprecoOrderBook.diff_message_from_exchange(
            msg=msg,
            timestamp=timestamp,
            metadata=metadata,
        )

        self.assertEqual("BTC-BRL", diff.trading_pair)
        self.assertEqual(OrderBookMessageType.DIFF, diff.type)
        self.assertEqual(timestamp, diff.timestamp)
        self.assertEqual(1, len(diff.bids))
        self.assertEqual(1, len(diff.asks))
        self.assertEqual(99500.0, diff.bids[0].price)
        self.assertEqual(0.2, diff.bids[0].amount)
        self.assertEqual(100500.0, diff.asks[0].price)

    def test_diff_message_with_empty_bids(self):
        msg = {
            "event": "update",
            "payload": {
                "bids": [],
                "asks": [{"price": "100500.00", "amount": "0.4", "id": "11"}],
            },
        }
        diff = BitprecoOrderBook.diff_message_from_exchange(
            msg=msg, timestamp=1640000010.0, metadata={"trading_pair": "BTC-BRL"}
        )
        self.assertEqual(0, len(diff.bids))
        self.assertEqual(1, len(diff.asks))

    def test_diff_message_with_empty_asks(self):
        msg = {
            "event": "update",
            "payload": {
                "bids": [{"price": "99500.00", "amount": "0.2", "id": "10"}],
                "asks": [],
            },
        }
        diff = BitprecoOrderBook.diff_message_from_exchange(
            msg=msg, timestamp=1640000010.0, metadata={"trading_pair": "BTC-BRL"}
        )
        self.assertEqual(1, len(diff.bids))
        self.assertEqual(0, len(diff.asks))

    def test_trade_message_from_exchange(self):
        msg = {
            "t": 1640000000123,
            "m": True,
            "p": "100000.00",
            "q": "0.01",
        }
        trade = BitprecoOrderBook.trade_message_from_exchange(
            msg=msg,
            metadata={"trading_pair": "BTC-BRL"},
        )
        self.assertEqual("BTC-BRL", trade.trading_pair)
        self.assertEqual(OrderBookMessageType.TRADE, trade.type)
        self.assertEqual(1640000000123 * 1e-3, trade.timestamp)
        self.assertEqual(1640000000123, trade.trade_id)
        self.assertEqual("100000.00", trade.content["price"])
        self.assertEqual("0.01", trade.content["amount"])
