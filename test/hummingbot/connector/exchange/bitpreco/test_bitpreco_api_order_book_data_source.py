import asyncio
import json
import re
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses

import hummingbot.connector.exchange.bitpreco.bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_api_order_book_data_source import BitprecoAPIOrderBookDataSource
from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange
import hummingbot.connector.exchange.bitpreco.bitpreco_web_utils as web_utils
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class BitprecoAPIOrderBookDataSourceTests(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.trading_pair = "BTC-BRL"
        cls.api_key = "testApiKey"
        cls.api_secret = "testApiSecret"

    def setUp(self) -> None:
        super().setUp()
        self.loop = asyncio.new_event_loop()
        self.connector = self._create_connector()
        self.data_source = BitprecoAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=web_utils.build_api_factory(),
        )
        from bidict import bidict
        self.connector._set_trading_pair_symbol_map(bidict({"BTC-BRL": "BTC-BRL"}))

    def tearDown(self) -> None:
        self.loop.close()
        super().tearDown()

    def _create_connector(self):
        mock_config = MagicMock()
        connector = BitprecoExchange(
            client_config_map=mock_config,
            bitpreco_api_key=self.api_key,
            bitpreco_api_secret=self.api_secret,
            trading_pairs=[self.trading_pair],
        )
        return connector

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    def _ticker_response(self):
        return {
            "success": True,
            "BTC-BRL": {
                "last": "100000.00",
                "high": "105000.00",
                "low": "98000.00",
                "vol": "12.5",
                "buy": "99900.00",
                "sell": "100100.00",
            },
            "ETH-BRL": {
                "last": "5000.00",
                "high": "5500.00",
                "low": "4800.00",
                "vol": "50.0",
                "buy": "4990.00",
                "sell": "5010.00",
            },
        }

    def _order_book_response(self):
        return {
            "bids": [
                {"price": "99900.00", "amount": "0.5", "id": "1"},
                {"price": "99800.00", "amount": "1.0", "id": "2"},
            ],
            "asks": [
                {"price": "100100.00", "amount": "0.3", "id": "3"},
                {"price": "100200.00", "amount": "0.8", "id": "4"},
            ],
        }

    @aioresponses()
    def test_get_last_traded_prices_returns_all_pairs(self, mock_api):
        mock_api.get(CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL, body=json.dumps(self._ticker_response()))

        prices = self._run(
            self.data_source.get_last_traded_prices(trading_pairs=[self.trading_pair])
        )

        self.assertIn("BTC-BRL", prices)
        self.assertEqual(100000.0, prices["BTC-BRL"])
        self.assertIn("ETH-BRL", prices)
        self.assertEqual(5000.0, prices["ETH-BRL"])

    @aioresponses()
    def test_request_order_book_snapshot(self, mock_api):
        order_book_url = CONSTANTS.ORDER_BOOK_PATH_URL.format("btc-brl")
        mock_api.get(order_book_url, body=json.dumps(self._order_book_response()))

        snapshot = self._run(self.data_source._request_order_book_snapshot(self.trading_pair))

        self.assertIn("bids", snapshot)
        self.assertIn("asks", snapshot)
        self.assertEqual(2, len(snapshot["bids"]))
        self.assertEqual(2, len(snapshot["asks"]))

    @aioresponses()
    def test_process_websocket_messages_enqueues_snapshot(self, mock_api):
        order_book_url = CONSTANTS.ORDER_BOOK_PATH_URL.format("btc-brl")
        mock_api.get(order_book_url, body=json.dumps(self._order_book_response()))

        ws_mock = MagicMock()

        async def run_process():
            await self.data_source._process_websocket_messages(websocket_assistant=ws_mock)

        self._run(run_process())

        queue = self.data_source._message_queue[self.data_source._diff_messages_queue_key]
        self.assertFalse(queue.empty())
        msg = self.loop.run_until_complete(queue.get())
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

    @aioresponses()
    def test_parse_order_book_diff_message(self, mock_api):
        raw_msg = {
            "event": "update",
            "topic": f"orderbook:{self.trading_pair}",
            "payload": {
                "bids": [{"price": "99500.00", "amount": "0.2", "id": "10"}],
                "asks": [{"price": "100500.00", "amount": "0.4", "id": "11"}],
            },
        }
        queue = asyncio.Queue()

        async def run_parse():
            await self.data_source._parse_order_book_diff_message(
                raw_message=raw_msg, message_queue=queue
            )

        self._run(run_parse())

        self.assertFalse(queue.empty())
        msg = self.loop.run_until_complete(queue.get())
        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)
        self.assertEqual(1, len(msg.bids))
        self.assertEqual(1, len(msg.asks))

    @aioresponses()
    def test_listen_for_subscriptions_retries_on_error(self, mock_api):
        order_book_url = CONSTANTS.ORDER_BOOK_PATH_URL.format("btc-brl")
        mock_api.get(order_book_url, exception=ConnectionError("Network error"))
        mock_api.get(order_book_url, body=json.dumps(self._order_book_response()))

        call_count = 0
        original_sleep = self.data_source._sleep

        async def mock_sleep(delay):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        self.data_source._sleep = mock_sleep

        try:
            self._run(self.data_source.listen_for_subscriptions())
        except asyncio.CancelledError:
            pass

        self.data_source._sleep = original_sleep
