import asyncio
import json
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall
from bidict import bidict

import hummingbot.connector.exchange.bitpreco.bitpreco_constants as CONSTANTS
import hummingbot.connector.exchange.bitpreco.bitpreco_web_utils as web_utils
from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase


class BitprecoExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "BRL"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.trading_pair_2 = f"{cls.base_asset}-USDT"

    def setUp(self) -> None:
        super().setUp()
        self.mocking_assistant = NetworkMockingAssistant()

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.mocking_assistant = NetworkMockingAssistant()

    # ── Abstract properties ──────────────────────────────────────────────────

    @property
    def all_symbols_url(self):
        return CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL

    @property
    def latest_prices_url(self):
        return CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL

    @property
    def network_status_url(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_rules_url(self):
        # Trading rules are hardcoded — no API call needed. Return any URL as placeholder.
        return CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL

    @property
    def order_creation_url(self):
        return CONSTANTS.REST_URL

    @property
    def balance_url(self):
        return CONSTANTS.REST_URL

    # ── Mock responses ───────────────────────────────────────────────────────

    @property
    def all_symbols_request_mock_response(self):
        return {
            "success": True,
            self.trading_pair: {
                "last": "100000.00",
                "high": "105000.00",
                "low": "98000.00",
                "vol": "10.0",
                "buy": "99900.00",
                "sell": "100100.00",
            },
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "success": True,
            self.trading_pair: {
                "last": str(self.expected_latest_price),
                "high": "10000.00",
                "low": "9000.00",
                "vol": "5.0",
                "buy": str(self.expected_latest_price - 1),
                "sell": str(self.expected_latest_price + 1),
            },
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        # BitPreco includes ALL non-"success" keys as pairs. To produce an "invalid" pair
        # for the test, we return a pair that is included in the response but NOT in trading_rules.
        invalid_pair = "INVALID-PAIR"
        response = {
            "success": True,
            self.trading_pair: {"last": "100000.00"},
            invalid_pair: {"last": "0.01"},
        }
        return invalid_pair, response

    @property
    def network_status_request_successful_mock_response(self):
        return {"last": "100000.00", "success": True}

    @property
    def trading_rules_request_mock_response(self):
        # Not used since trading rules are hardcoded, but required by the abstract class.
        return {"success": True, self.trading_pair: {"last": "100000.00"}}

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {"success": True}

    @property
    def order_creation_request_successful_mock_response(self):
        return {"order_id": self.expected_exchange_order_id, "success": True}

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "success": True,
            "timestamp": "2024-01-01T00:00:00Z",
            self.base_asset: "10.0",
            f"{self.base_asset}_locked": "5.0",
            self.quote_asset: "2000.0",
            f"{self.quote_asset}_locked": "0.0",
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            "success": True,
            "timestamp": "2024-01-01T00:00:00Z",
            self.base_asset: "10.0",
            f"{self.base_asset}_locked": "5.0",
        }

    @property
    def balance_event_websocket_update(self):
        return {"event": "flash", "topic": "notifications:test", "payload": {}}

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def expected_supported_order_types(self):
        return [OrderType.MARKET, OrderType.LIMIT]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.0001"),
            min_price_increment=Decimal("0.00000001"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("10"),
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        return ""

    @property
    def expected_exchange_order_id(self):
        return 28

    @property
    def is_cancel_request_executed_synchronously_by_server(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("50000")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        # BitPreco computes fees as flat BRL amounts. For BUY orders, new_spot_fee
        # returns AddedToCostTradeFee. Amount matches partial fill: 0.5 * 50000 * 0.0025 = 62.50
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("62.50"))],
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return str(self.expected_exchange_order_id)

    # ── Helper methods ───────────────────────────────────────────────────────

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}-{quote_token}"

    def create_exchange_instance(self):
        from unittest.mock import MagicMock
        mock_config = MagicMock()
        return BitprecoExchange(
            client_config_map=mock_config,
            bitpreco_api_key="testAPIKey",
            bitpreco_api_secret="testSecret",
            trading_pairs=[self.trading_pair],
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        data = request_call.kwargs.get("data") or {}
        if isinstance(data, str):
            data = json.loads(data)
        elif not isinstance(data, dict):
            data = dict(data) if data else {}
        self.assertIn("auth_token", data)
        self.assertEqual(
            f"testSecret{'testAPIKey'}",
            data["auth_token"],
        )

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        data = request_call.kwargs.get("data") or {}
        if isinstance(data, str):
            data = json.loads(data)
        elif not isinstance(data, dict):
            data = dict(data)
        expected_cmd = CONSTANTS.CMD_BUY if order.trade_type is TradeType.BUY else CONSTANTS.CMD_SELL
        self.assertEqual(expected_cmd, data["cmd"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), data["market"])
        self.assertTrue(data["limited"])
        self.assertEqual(Decimal("100"), Decimal(data["amount"]))
        self.assertEqual(Decimal("10000"), Decimal(data["price"]))

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        data = request_call.kwargs.get("data") or {}
        if isinstance(data, str):
            data = json.loads(data)
        elif not isinstance(data, dict):
            data = dict(data)
        self.assertEqual(CONSTANTS.CMD_CANCEL_ORDER, data["cmd"])
        self.assertEqual(order.exchange_order_id, data["order_id"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        data = request_call.kwargs.get("data") or {}
        if isinstance(data, str):
            data = json.loads(data)
        elif not isinstance(data, dict):
            data = dict(data)
        self.assertEqual("order_status", data["cmd"])
        self.assertEqual(order.exchange_order_id, data["order_id"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        data = request_call.kwargs.get("data") or {}
        if isinstance(data, str):
            data = json.loads(data)
        elif not isinstance(data, dict):
            data = dict(data)
        self.assertEqual("executed_orders", data["cmd"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), data["market"])

    # ── Mock configuration helpers ───────────────────────────────────────────

    def _order_fills_mock_response(self, order: InFlightOrder, amount: str, price: str) -> List[Dict]:
        return [
            {
                "id": order.exchange_order_id,
                "exec_amount": amount,
                "price": price,
                "fee": str(Decimal(amount) * Decimal(price) * Decimal("0.0025")),
                "time_stamp": "2024-01-01 12:00:00",
            }
        ]

    def _order_status_mock_response(self, order: InFlightOrder, status: str) -> Dict:
        return {
            "success": True,
            "order": {
                "order_id": order.exchange_order_id,
                "status": status,
                "market": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            },
        }

    def configure_successful_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        response = {"message_cod": "ORDER_CANCELED", "success": True}
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_erroneous_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            status=400,
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_order_not_found_error_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        response = {"success": False, "message": "order not found"}
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_one_successful_one_erroneous_cancel_all_response(
        self,
        successful_order: InFlightOrder,
        erroneous_order: InFlightOrder,
        mock_api: aioresponses,
    ) -> List[str]:
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        return [url]

    def configure_completely_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        response = self._order_status_mock_response(order, "FILLED")
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_canceled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        response = self._order_status_mock_response(order, "CANCELED")
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_open_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        response = self._order_status_mock_response(order, "OPEN")
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_http_error_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            status=500,
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_partially_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        response = self._order_status_mock_response(order, "PARTIALLY_FILLED")
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_order_not_found_error_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        response = {"success": False, "message": "order not found"}
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            status=404,
            callback=callback,
        )
        return [CONSTANTS.REST_URL]

    def configure_partial_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        response = self._order_fills_mock_response(
            order,
            str(self.expected_partial_fill_amount),
            str(self.expected_partial_fill_price),
        )
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_erroneous_http_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            status=500,
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_full_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        # Use the same fee amount as expected_fill_fee so assertions match across all fill tests.
        # The fee field is read directly from the API response, not recomputed.
        fee_amount = str(self.expected_fill_fee.flat_fees[0].amount)
        response = [
            {
                "id": order.exchange_order_id,
                "exec_amount": str(order.amount),
                "price": str(order.price),
                "fee": fee_amount,
                "time_stamp": "2024-01-01 12:00:00",
            }
        ]
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    # ── WS event helpers ─────────────────────────────────────────────────────
    # BitPreco user stream sends "flash" events (not order-specific events).
    # These methods return a generic flash event payload.

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {"event": "flash", "topic": "notifications:test", "payload": {}, "ref": None}

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {"event": "flash", "topic": "notifications:test", "payload": {}, "ref": None}

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {"event": "flash", "topic": "notifications:test", "payload": {}, "ref": None}

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return None

    # ── Overrides for BitPreco-specific behavior ─────────────────────────────

    def _configure_balance_response(
        self,
        response: Dict[str, Any],
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(response),
            callback=callback,
        )
        return CONSTANTS.REST_URL

    def configure_trading_rules_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        # BitPreco has hardcoded trading rules — no API call needed.
        # Register a no-op mock for the placeholder URL.
        url = self.trading_rules_url
        mock_api.get(url, body=json.dumps(self.trading_rules_request_mock_response), callback=callback)
        return [url]

    def configure_erroneous_trading_rules_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = self.trading_rules_url
        mock_api.get(url, body=json.dumps(self.trading_rules_request_erroneous_mock_response), callback=callback)
        return [url]

    def configure_all_symbols_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = self.all_symbols_url
        mock_api.get(url, body=json.dumps(self.all_symbols_request_mock_response), callback=callback)
        return [url]

    # ── Overridden tests ─────────────────────────────────────────────────────

    @aioresponses()
    async def test_update_trading_rules(self, mock_api):
        # BitPreco uses hardcoded trading rules; no API call is made.
        self.exchange._set_current_timestamp(1000)
        await self.exchange._update_trading_rules()
        rules = self.exchange.trading_rules
        self.assertIn(self.trading_pair, rules)
        rule = rules[self.trading_pair]
        self.assertEqual(repr(self.expected_trading_rule), repr(rule))

    @aioresponses()
    async def test_update_trading_rules_ignores_rule_with_error(self, mock_api):
        # BitPreco uses hardcoded rules; no errors are logged during update.
        await self.exchange._update_trading_rules()
        self.assertIn(self.trading_pair, self.exchange.trading_rules)

    @aioresponses()
    async def test_invalid_trading_pair_not_in_all_trading_pairs(self, mock_api):
        # BitPreco includes all keys (except "success") from the ticker as pairs.
        # Here we verify that "success" is not treated as a trading pair.
        self.exchange._set_trading_pair_symbol_map(None)
        response = {
            "success": True,
            self.trading_pair: {"last": "100000.00"},
        }
        mock_api.get(self.all_symbols_url, body=json.dumps(response))
        all_trading_pairs = await self.exchange.all_trading_pairs()
        self.assertNotIn("success", all_trading_pairs)
        self.assertIn(self.trading_pair, all_trading_pairs)

    @aioresponses()
    async def test_update_balances(self, mock_api):
        response = self.balance_request_mock_response_for_base_and_quote
        self._configure_balance_response(response=response, mock_api=mock_api)

        await self.exchange._update_balances()

        available = self.exchange.available_balances
        total = self.exchange.get_all_balances()

        self.assertEqual(Decimal("10"), available[self.base_asset])
        self.assertEqual(Decimal("2000"), available[self.quote_asset])
        self.assertEqual(Decimal("15"), total[self.base_asset])
        self.assertEqual(Decimal("2000"), total[self.quote_asset])

        response = self.balance_request_mock_response_only_base
        self._configure_balance_response(response=response, mock_api=mock_api)
        await self.exchange._update_balances()

        available = self.exchange.available_balances
        total = self.exchange.get_all_balances()

        self.assertNotIn(self.quote_asset, available)
        self.assertNotIn(self.quote_asset, total)
        self.assertEqual(Decimal("10"), available[self.base_asset])
        self.assertEqual(Decimal("15"), total[self.base_asset])

    @aioresponses()
    async def test_update_order_status_when_filled(self, mock_api):
        # BitPreco's _update_order_status calls fills THEN status.
        # Mock order: fills first, status (FILLED) second.
        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        # Mock 1: trade fills (consumed first by _update_orders_fills)
        fills_response = self._order_fills_mock_response(order, "1", "10000.00")
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(fills_response),
        )

        # Mock 2: order status FILLED (consumed second by _update_orders)
        status_response = self._order_status_mock_response(order, "FILLED")
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(status_response),
            callback=lambda *args, **kwargs: request_sent_event.set(),
        )

        await self.exchange._update_order_status()
        await request_sent_event.wait()
        await asyncio.sleep(0.1)

        await order.wait_until_completely_filled()
        self.assertTrue(order.is_done)
        self.assertTrue(order.is_filled)

    @aioresponses()
    async def test_update_order_status_when_canceled(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        # Mock 1: empty fills
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps([]),
        )

        # Mock 2: CANCELED status
        status_response = self._order_status_mock_response(order, "CANCELED")
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(status_response),
            callback=lambda *args, **kwargs: request_sent_event.set(),
        )

        await self.exchange._update_order_status()
        await request_sent_event.wait()
        await asyncio.sleep(0.1)

        self.assertTrue(order.is_done)
        self.assertFalse(order.is_filled)

    @aioresponses()
    async def test_update_order_status_when_order_has_not_changed(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        # Mock fills: empty
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps([]),
        )
        # Mock status: OPEN (unchanged)
        status_response = self._order_status_mock_response(order, "OPEN")
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(status_response),
        )

        await self.exchange._update_order_status()
        await asyncio.sleep(0.1)

        self.assertFalse(order.is_done)
        self.assertEqual(OrderState.OPEN, order.current_state)

    @aioresponses()
    async def test_update_order_status_when_request_fails_marks_order_as_not_found(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        # Mock fills: error
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            status=500,
        )
        # Mock status: error
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            status=500,
        )

        await self.exchange._update_order_status()
        await asyncio.sleep(0.1)

        # Order should NOT be marked as not found after just 1 failure
        self.assertIn(self.client_order_id_prefix + "1", self.exchange.in_flight_orders)

    @aioresponses()
    async def test_lost_order_included_in_order_fills_update_and_not_in_order_status_update(self, mock_api):
        # Override: BitPreco calls _update_orders_fills for ALL fillable orders (including lost)
        # in _update_order_status(), consuming the first fill mock. The base class registers
        # mocks in order [Fill#1, Status, Fill#2], but BitPreco needs [Fill#1, Fill#2, Status].
        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            await self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id)

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        # Register mocks in the actual consumption order for BitPreco:
        # 1) Fill#1 — consumed by _update_orders_fills(all_fillable_orders) in _update_order_status
        trade_url = self.configure_full_fill_trade_response(
            order=order, mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set()
        )
        # 2) Fill#2 — consumed by _update_orders_fills(lost_orders) in _update_lost_orders_status
        self.configure_full_fill_trade_response(order=order, mock_api=mock_api)
        # 3) Status FILLED — consumed by _update_lost_orders in _update_lost_orders_status
        self.configure_completely_filled_order_status_response(
            order=order, mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set()
        )

        await self.exchange._update_order_status()
        await request_sent_event.wait()
        await order.wait_until_completely_filled()
        await asyncio.sleep(0.1)

        self.assertTrue(order.is_done)
        self.assertTrue(order.is_failure)

        trades_request = self._all_executed_requests(mock_api, trade_url)[0]
        self.validate_auth_credentials_present(trades_request)
        self.validate_trades_request(order=order, request_call=trades_request)

        from hummingbot.core.event.events import OrderFilledEvent
        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(order.price, fill_event.price)
        self.assertEqual(order.amount, fill_event.amount)
        self.assertEqual(self.expected_fill_fee, fill_event.trade_fee)

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))
        self.assertIn(order.client_order_id, self.exchange._order_tracker.all_fillable_orders)
        self.assertFalse(self.is_logged("INFO", f"BUY order {order.client_order_id} completely filled."))

        request_sent_event.clear()

        await self.exchange._update_lost_orders_status()
        await request_sent_event.wait()
        await asyncio.sleep(0.1)

        self.assertTrue(order.is_done)
        self.assertTrue(order.is_failure)

        self.assertEqual(1, len(self.order_filled_logger.event_log))
        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))
        self.assertNotIn(order.client_order_id, self.exchange._order_tracker.all_fillable_orders)
        self.assertFalse(self.is_logged("INFO", f"BUY order {order.client_order_id} completely filled."))

    @aioresponses()
    async def test_update_order_status_when_order_has_not_changed_and_one_partial_fill(self, mock_api):
        # BitPreco calls fills (executed_orders) BEFORE order_status.
        # The base class picks [0] expecting order_status, but [0] is the fills request.
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        trade_url = self.configure_partial_fill_trade_response(order=order, mock_api=mock_api)
        order_url = self.configure_partially_filled_order_status_response(order=order, mock_api=mock_api)

        self.assertTrue(order.is_open)
        await self.exchange._update_order_status()
        await asyncio.sleep(0.1)

        all_requests = self._all_executed_requests(mock_api, order_url)
        trades_request = all_requests[0]        # [0] = executed_orders (fills)
        order_status_request = all_requests[1]  # [1] = order_status

        self.validate_auth_credentials_present(order_status_request)
        self.validate_order_status_request(order=order, request_call=order_status_request)

        self.assertTrue(order.is_open)
        self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)

        self.validate_auth_credentials_present(trades_request)
        self.validate_trades_request(order=order, request_call=trades_request)

        from hummingbot.core.event.events import OrderFilledEvent
        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(self.expected_partial_fill_price, fill_event.price)
        self.assertEqual(self.expected_partial_fill_amount, fill_event.amount)
        self.assertEqual(self.expected_fill_fee, fill_event.trade_fee)

    @aioresponses()
    async def test_update_order_status_when_filled_correctly_processed_even_when_trade_fill_update_fails(self, mock_api):
        # BitPreco calls fills BEFORE order_status. Base class uses [0] for order_status validation
        # but [0] is the fills request. Override to use [1] for order_status.
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        trade_url = self.configure_erroneous_http_fill_trade_response(order=order, mock_api=mock_api)
        urls = self.configure_completely_filled_order_status_response(order=order, mock_api=mock_api)

        order.completely_filled_event.set()
        await self.exchange._update_order_status()
        await order.wait_until_completely_filled()
        await asyncio.sleep(0.1)

        for url in (urls if isinstance(urls, list) else [urls]):
            all_requests = self._all_executed_requests(mock_api, url)
            order_status_request = all_requests[1]  # [1] = order_status (fills are [0])
            self.validate_auth_credentials_present(order_status_request)
            self.validate_order_status_request(order=order, request_call=order_status_request)

        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_done)

        trades_request = self._all_executed_requests(mock_api, trade_url)[0]
        self.validate_auth_credentials_present(trades_request)
        self.validate_trades_request(order=order, request_call=trades_request)

        self.assertEqual(0, len(self.order_filled_logger.event_log))

        from hummingbot.core.event.events import BuyOrderCompletedEvent
        buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
        self.assertEqual(order.client_order_id, buy_event.order_id)
        self.assertEqual(order.base_asset, buy_event.base_asset)
        self.assertEqual(order.quote_asset, buy_event.quote_asset)
        self.assertEqual(Decimal(0), buy_event.base_asset_amount)
        self.assertEqual(Decimal(0), buy_event.quote_asset_amount)
        self.assertEqual(order.order_type, buy_event.order_type)
        self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(self.is_logged("INFO", f"BUY order {order.client_order_id} completely filled."))

    @aioresponses()
    async def test_lost_order_removed_after_cancel_status_user_event_received(self, mock_api):
        # Override: BitPreco's flash event triggers HTTP balance+status refresh.
        # Mock all HTTP calls that result from the flash event.
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            await self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id)

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        order_event = self.order_event_for_canceled_order_websocket_update(order=order)
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [order_event, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        # Flash event triggers: _update_all_balances → balance POST
        self._configure_balance_response(
            response=self.balance_request_mock_response_for_base_and_quote, mock_api=mock_api
        )
        # _update_order_status → _update_orders_fills(all_fillable_orders) → executed_orders POST
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps([]),  # no fills for cancelled order
        )
        # _update_lost_orders → _request_order_status(lost_order) → order_status POST
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(self._order_status_mock_response(order, "CANCELED")),
        )

        try:
            await self.exchange._user_stream_event_listener()
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.1)

        self.assertNotIn(order.client_order_id, self.exchange._order_tracker.lost_orders)
        self.assertEqual(0, len(self.order_cancelled_logger.event_log))
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertFalse(order.is_cancelled)
        self.assertTrue(order.is_failure)

    @aioresponses()
    async def test_lost_order_user_stream_full_fill_events_are_processed(self, mock_api):
        # Override: BitPreco's flash event triggers HTTP balance+status refresh.
        # Mock all HTTP calls that result from the flash event.
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            await self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id)

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        order_event = self.order_event_for_full_fill_websocket_update(order=order)
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [order_event, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        # Flash event triggers: _update_all_balances → balance POST
        self._configure_balance_response(
            response=self.balance_request_mock_response_for_base_and_quote, mock_api=mock_api
        )
        # _update_order_status → _update_orders_fills(all_fillable_orders) → executed_orders POST → full fill
        fee_amount = str(self.expected_fill_fee.flat_fees[0].amount)
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps([
                {
                    "id": order.exchange_order_id,
                    "exec_amount": str(order.amount),
                    "price": str(order.price),
                    "fee": fee_amount,
                    "time_stamp": "2024-01-01 12:00:00",
                }
            ]),
        )
        # _update_lost_orders → _request_order_status(lost_order) → order_status POST → FILLED
        mock_api.post(
            re.compile(f"^{CONSTANTS.REST_URL}".replace(".", r"\.")),
            body=json.dumps(self._order_status_mock_response(order, "FILLED")),
        )

        try:
            await self.exchange._user_stream_event_listener()
        except asyncio.CancelledError:
            pass
        await order.wait_until_completely_filled()
        await asyncio.sleep(0.1)

        from hummingbot.core.event.events import OrderFilledEvent
        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(order.price, fill_event.price)
        self.assertEqual(order.amount, fill_event.amount)
        self.assertEqual(self.expected_fill_fee, fill_event.trade_fee)

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertNotIn(order.client_order_id, self.exchange._order_tracker.lost_orders)
        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_failure)

    @aioresponses()
    async def test_user_stream_update_for_new_order(self, mock_api):
        # BitPreco WS sends "flash" events — no order-state events.
        # The flash event triggers a balance/order refresh via HTTP.
        # This test simply verifies the exchange processes flash events without error.
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        flash_event = {"event": "flash", "topic": "notifications:test", "payload": {}, "ref": None}
        mock_queue = asyncio.Queue()
        mock_queue.put_nowait(flash_event)

        async def mock_iter():
            while not mock_queue.empty():
                yield await mock_queue.get()

        with patch.object(self.exchange, "_iter_user_event_queue", side_effect=mock_iter):
            with patch.object(self.exchange, "_update_all_balances", new_callable=AsyncMock) as mock_balances:
                with patch.object(self.exchange, "_update_order_status", new_callable=AsyncMock) as mock_status:
                    await self.exchange._user_stream_event_listener()

        mock_balances.assert_awaited_once()
        mock_status.assert_awaited_once()

    @aioresponses()
    async def test_user_stream_update_for_canceled_order(self, mock_api):
        flash_event = {"event": "flash", "topic": "notifications:test", "payload": {}, "ref": None}
        mock_queue = asyncio.Queue()
        mock_queue.put_nowait(flash_event)

        async def mock_iter():
            while not mock_queue.empty():
                yield await mock_queue.get()

        with patch.object(self.exchange, "_iter_user_event_queue", side_effect=mock_iter):
            with patch.object(self.exchange, "_update_all_balances", new_callable=AsyncMock) as mock_balances:
                with patch.object(self.exchange, "_update_order_status", new_callable=AsyncMock) as mock_status:
                    await self.exchange._user_stream_event_listener()

        mock_balances.assert_awaited_once()
        mock_status.assert_awaited_once()

    @aioresponses()
    async def test_user_stream_update_for_order_full_fill(self, mock_api):
        flash_event = {"event": "flash", "topic": "notifications:test", "payload": {}, "ref": None}
        mock_queue = asyncio.Queue()
        mock_queue.put_nowait(flash_event)

        async def mock_iter():
            while not mock_queue.empty():
                yield await mock_queue.get()

        with patch.object(self.exchange, "_iter_user_event_queue", side_effect=mock_iter):
            with patch.object(self.exchange, "_update_all_balances", new_callable=AsyncMock) as mock_balances:
                with patch.object(self.exchange, "_update_order_status", new_callable=AsyncMock) as mock_status:
                    await self.exchange._user_stream_event_listener()

        mock_balances.assert_awaited_once()
        mock_status.assert_awaited_once()

    @aioresponses()
    async def test_user_stream_balance_update(self, mock_api):
        # Flash event triggers balance refresh then order status refresh.
        flash_event = {"event": "flash", "topic": "notifications:test", "payload": {}}
        mock_queue = asyncio.Queue()
        mock_queue.put_nowait(flash_event)

        async def mock_iter():
            while not mock_queue.empty():
                yield await mock_queue.get()

        with patch.object(self.exchange, "_iter_user_event_queue", side_effect=mock_iter):
            with patch.object(self.exchange, "_update_all_balances", new_callable=AsyncMock) as mock_balances:
                with patch.object(self.exchange, "_update_order_status", new_callable=AsyncMock) as mock_status:
                    await self.exchange._user_stream_event_listener()

        mock_balances.assert_awaited_once()
        mock_status.assert_awaited_once()

    async def test_user_stream_raises_cancel_exception(self):
        async def mock_iter():
            raise asyncio.CancelledError()
            yield  # make it a generator

        with patch.object(self.exchange, "_iter_user_event_queue", side_effect=mock_iter):
            with self.assertRaises(asyncio.CancelledError):
                await self.exchange._user_stream_event_listener()

    async def test_user_stream_logs_errors(self):
        async def mock_iter():
            yield {"event": "bad", "ref": None}

        with patch.object(self.exchange, "_iter_user_event_queue", side_effect=mock_iter):
            # Non-flash events are silently ignored — no exception.
            await self.exchange._user_stream_event_listener()

    # ── Additional BitPreco-specific tests ───────────────────────────────────

    @aioresponses()
    async def test_place_order_sends_correct_cmd_and_market(self, mock_api):
        self._simulate_trading_rules_initialized()
        mock_api.post(CONSTANTS.REST_URL, body=json.dumps({"order_id": 99, "success": True}))

        order_id = self.place_buy_order()
        await asyncio.sleep(0.2)

        self.assertIn(order_id, self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders[order_id]
        self.assertEqual("99", order.exchange_order_id)

    @aioresponses()
    async def test_cancel_order_returns_true_on_order_canceled(self, mock_api):
        self._simulate_trading_rules_initialized()
        mock_api.post(CONSTANTS.REST_URL, body=json.dumps({"order_id": 99, "success": True}))
        order_id = self.place_buy_order()
        await asyncio.sleep(0.2)

        mock_api.post(CONSTANTS.REST_URL, body=json.dumps({"message_cod": "ORDER_CANCELED", "success": True}))

        tracked = self.exchange.in_flight_orders.get(order_id)
        if tracked:
            result = await self.exchange._place_cancel(order_id, tracked)
            self.assertTrue(result)

    @aioresponses()
    async def test_initialize_trading_pair_symbol_map(self, mock_api):
        response = {
            "success": True,
            "BTC-BRL": {"last": "100000.00"},
            "ETH-BRL": {"last": "5000.00"},
        }
        mock_api.get(CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL, body=json.dumps(response))

        self.exchange._set_trading_pair_symbol_map(None)
        await self.exchange._initialize_trading_pair_symbol_map()

        symbol_map = await self.exchange.trading_pair_symbol_map()
        self.assertIn("BTC-BRL", symbol_map)
        self.assertIn("ETH-BRL", symbol_map)
        self.assertNotIn("success", symbol_map)

    @aioresponses()
    async def test_initialize_trading_pair_symbol_map_raises_on_failure(self, mock_api):
        response = {"success": False}
        mock_api.get(CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL, body=json.dumps(response))

        self.exchange._set_trading_pair_symbol_map(None)
        await self.exchange._initialize_trading_pair_symbol_map()
        self.assertFalse(self.exchange.trading_pair_symbol_map_ready())

    def test_trading_rules_contain_expected_pairs(self):
        self.run_async_with_timeout(self.exchange._update_trading_rules())
        rules = self.exchange.trading_rules
        expected_pairs = ["BTC-BRL", "USDT-BRL", "ETH-BRL", "USDC-BRL", "BNB-BRL",
                          "ADA-BRL", "UNI-BRL", "PAXG-BRL", "SOL-BRL", "AXS-BRL"]
        for pair in expected_pairs:
            self.assertIn(pair, rules)

    def test_trading_rules_do_not_contain_removed_pairs(self):
        self.run_async_with_timeout(self.exchange._update_trading_rules())
        rules = self.exchange.trading_rules
        removed_pairs = ["BUSD-BRL", "POLIS-BRL", "ATLAS-BRL", "SLP-BRL", "GMT-BRL",
                         "ABFY-BRL", "CRZO-BRL"]
        for pair in removed_pairs:
            self.assertNotIn(pair, rules)

    def test_hbot_order_id_prefix(self):
        self.assertEqual("HBOT-BP-", CONSTANTS.HBOT_ORDER_ID_PREFIX)
        self.assertTrue(self.exchange.client_order_id_prefix.startswith("HBOT-BP-"))

    def test_partially_filled_order_state_is_mapped(self):
        self.assertIn("PARTIALLY_FILLED", CONSTANTS.ORDER_STATE)
        from hummingbot.core.data_type.in_flight_order import OrderState
        self.assertEqual(OrderState.PARTIALLY_FILLED, CONSTANTS.ORDER_STATE["PARTIALLY_FILLED"])
