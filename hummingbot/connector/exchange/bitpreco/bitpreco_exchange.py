import asyncio
import copy
import datetime
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS, bitpreco_web_utils as web_utils
from hummingbot.connector.exchange.bitpreco.bitpreco_api_order_book_data_source import BitprecoAPIOrderBookDataSource
from hummingbot.connector.exchange.bitpreco.bitpreco_api_user_stream_data_source import BitprecoAPIUserStreamDataSource
from hummingbot.connector.exchange.bitpreco.bitpreco_auth import BitprecoAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("nan")


class BitprecoExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 bitpreco_api_key: str,
                 bitpreco_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN
                 ):
        self.api_key = bitpreco_api_key
        self.secret_key = bitpreco_api_secret
        self._domain = domain
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        super().__init__(client_config_map)
        self._throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self._api_factory = web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self.authenticator)
        self._rest_assistant = None
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._trading_rules = {}
        self._trade_fees = {}
        self._status_polling_task = None
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._last_poll_timestamp = 0
        self._order_tracker: ClientOrderTracker = ClientOrderTracker(connector=self)

    @property
    def name(self) -> str:
        return "bitpreco"

    @property
    def authenticator(self):
        return BitprecoAuth(
            api_key=self.api_key,
            secret_key=self.secret_key)

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.REST_URL

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.REST_URL

    def supported_order_types(self):
        return [OrderType.MARKET, OrderType.LIMIT]

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        amount_str = f"{amount:f}"
        price_str = f"{price:f}"
        data = {
            "cmd": CONSTANTS.CMD_BUY if trade_type is TradeType.BUY else CONSTANTS.CMD_SELL,
            "market": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limited": True if order_type is OrderType.LIMIT else False,
            "amount": amount_str,
            "price": price_str
        }

        transact_time = time.time()
        response = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=data,
            is_auth_required=True,
        )

        o_id = str(response["order_id"])
        return (o_id, transact_time)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        data = {
            "cmd": CONSTANTS.CMD_CANCEL_ORDER,
            "order_id": tracked_order.exchange_order_id
        }

        response = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=data,
            is_auth_required=True,
        )
        if response.get("message_cod") == "ORDER_CANCELED":
            return True
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:

        rules = [
            {
                "symbol": "BTC-BRL",
                "min_order_size": 0.0001,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "USDT-BRL",
                "min_order_size": 1,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "ETH-BRL",
                "min_order_size": 0.1,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "USDC-BRL",
                "min_order_size": 1,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "BNB-BRL",
                "min_order_size": 0.1,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "ADA-BRL",
                "min_order_size": 0.1,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "UNI-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "PAXG-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "SOL-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "AXS-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
        ]

        retval = []
        for rule in rules:
            trading_pair = rule.get("symbol")
            min_order_size = rule.get("min_order_size")
            min_price_increment = rule.get("min_price_increment")
            min_base_amount_increment = rule.get("min_base_amount_increment")
            min_notional_size = rule.get("min_notional_size")
            retval.append(
                TradingRule(trading_pair,
                            min_order_size=min_order_size,
                            min_price_increment=Decimal(min_price_increment),
                            min_base_amount_increment=Decimal(min_base_amount_increment),
                            min_notional_size=Decimal(min_notional_size)))

        return retval

    async def _update_trading_rules(self):
        trading_rules_list = await self._format_trading_rules({})
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculates the estimated fee an order would pay based on the connector configuration
        :param base_currency: the order base currency
        :param quote_currency: the order quote currency
        :param order_type: the type of order (MARKET, LIMIT, LIMIT_MAKER)
        :param order_side: if the order is for buying or selling
        :param amount: the order amount
        :param price: the order price
        :return: the estimated fee for the order
        """

        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        trade_base_fee = build_trade_fee(
            exchange=self.name,
            is_maker=is_maker,
            order_side=order_side,
            order_type=order_type,
            amount=amount,
            price=price,
            base_currency=base_currency,
            quote_currency=quote_currency
        )
        return trade_base_fee

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            market = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            data = {
                "market": market,
                "cmd": "executed_orders"
            }
            response = await self._api_request(
                method=RESTMethod.POST,
                path_url=CONSTANTS.REST_URL,
                data=data,
                is_auth_required=True,
                limit_id=CONSTANTS.REST_URL)

            for executed_order in list(response):
                if (order.exchange_order_id == executed_order.get("id")):
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token="BRL",
                        flat_fees=[TokenAmount(amount=Decimal(executed_order["fee"]), token="BRL")]
                    )
                    fill_quote_amount = Decimal(executed_order.get("exec_amount")) * Decimal(executed_order.get("price"))
                    timestamp = datetime.datetime.strptime(executed_order.get("time_stamp"),
                                                           "%Y-%m-%d %H:%M:%S").timestamp()
                    fill_timestamp = timestamp
                    trade_update = TradeUpdate(
                        trade_id=str(executed_order.get("id")),
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=market,
                        fee=fee,
                        fill_base_amount=Decimal(executed_order.get("exec_amount")),
                        fill_quote_amount=Decimal(fill_quote_amount),
                        fill_price=Decimal(executed_order.get("price")),
                        fill_timestamp=fill_timestamp,
                    )
                    trade_updates.append(trade_update)
        return trade_updates

    def _create_order_book_data_source(self) -> BitprecoAPIOrderBookDataSource:
        return BitprecoAPIOrderBookDataSource(trading_pairs=self._trading_pairs, connector=self)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        return DeductedFromReturnsTradeFee(percent=Decimal(0.0))

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: list):
        mapping = bidict()

        for pair in exchange_info:
            mapping[pair] = pair

        self._set_trading_pair_symbol_map(mapping)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.exchange_order_id:
            data = {
                "cmd": "order_status",
                "order_id": tracked_order.exchange_order_id
            }

            response = await self._api_request(
                method=RESTMethod.POST,
                path_url=CONSTANTS.REST_URL,
                data=data,
                is_auth_required=True,
            )

            order = response.get("order")

            order_status = order["status"]
            update_timestamp = time.time()

            new_state = CONSTANTS.ORDER_STATE[order_status] if order_status in CONSTANTS.ORDER_STATE else \
                CONSTANTS.ORDER_STATE["OPEN"]

            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=update_timestamp,
                new_state=new_state,
            )
            return order_update

    async def _update_balances(self):

        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        data = {"cmd": "balance"}
        balances = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=data,
            is_auth_required=True,
        )
        for item in balances:
            if item != "success" and item != "timestamp" and "locked" not in item:
                asset_name = item
                free_balance = Decimal(balances[item])
                total_balance = Decimal(balances[item]) + Decimal(balances[f'{item}_locked'])
                self._account_available_balances[asset_name] = free_balance
                self._account_balances[asset_name] = total_balance
                remote_asset_names.add(asset_name)
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _update_all_balances(self):

        await self._update_balances()
        # if not self.real_time_balance_update:
        # This is only required for exchanges that do not provide balance update notifications through websocket
        self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self.in_flight_orders.items()}
        self._in_flight_orders_snapshot_timestamp = self.current_timestamp

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            if event_message.get("event") == "flash":
                await self._update_all_balances()
                await self._update_order_status()

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BitprecoAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    async def _initialize_trading_pair_symbol_map(self):

        response = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL,
        )

        if not response["success"]:
            raise IOError(f"BitPreco returned failure on trading pair initialization: {response}")

        bitpreco_pairs = list(filter(lambda pair: pair != "success", response.keys()))

        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=bitpreco_pairs)

