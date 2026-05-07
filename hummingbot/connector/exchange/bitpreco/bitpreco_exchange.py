import asyncio
import copy
import datetime
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

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
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("nan")


class BitprecoExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 bitpreco_api_key: str,
                 bitpreco_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = bitpreco_api_key
        self.secret_key = bitpreco_api_secret
        self._domain = domain
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        super().__init__(balance_asset_limit, rate_limits_share_pct)
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
        # NOTE: BitPreco's REST API has no native post-only flag, so LIMIT_MAKER
        # is mapped internally to a regular LIMIT order (see _place_order). We
        # advertise LIMIT_MAKER support for compatibility with strategies that
        # require it (e.g., XEMMLeadLagExecutor); callers must accept that an
        # order priced at/across the book may fill as a taker. This is a safe
        # trade-off given BitPreco currently charges zero fees on both sides.
        return [OrderType.MARKET, OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _try_recover_placement(
        self,
        market: str,
        trade_type: TradeType,
        amount_str: str,
        price_str: str,
        since_ts: float,
    ) -> Optional[str]:
        """
        Defensive recovery for ambiguous placement failures.

        When `_place_order` is about to raise (network error, malformed
        response, missing order_id), the order MAY still have been accepted
        and queued by BitPreco — leaving an unhedged order on the book is the
        worst outcome (no taker leg, guaranteed slippage at unwind). Wait
        briefly for the matching engine to settle, then query `open_orders`
        and look for an order matching (market, side, price, amount, recent
        timestamp). If found, return its exchange_order_id so the framework
        tracks it instead of treating it as failed.

        Returns None if no matching order is on the exchange — in that case
        the caller propagates the original failure as-is.
        """
        # Give BitPreco's matching engine a moment to internalize the placement.
        await asyncio.sleep(0.7)

        expected_side = "buy" if trade_type is TradeType.BUY else "sell"
        body = {"cmd": "open_orders", "market": market}
        try:
            response = await self._api_request(
                method=RESTMethod.POST,
                path_url=CONSTANTS.REST_URL,
                data=self._add_auth_token_to_req_body(body),
            )
        except Exception as e:
            self.logger().warning(
                f"BitPreco placement recovery: open_orders fetch failed: {e}"
            )
            return None

        # BitPreco returns either a list, {"orders": [...]}, or a failure dict.
        if isinstance(response, list):
            orders = response
        elif isinstance(response, dict):
            inner = response.get("orders")
            if isinstance(inner, list):
                orders = inner
            else:
                self.logger().warning(
                    f"BitPreco placement recovery: unexpected response shape: {response}"
                )
                return None
        else:
            return None

        try:
            target_price = Decimal(price_str)
            target_amount = Decimal(amount_str)
        except Exception:
            return None

        # Match: same side, price within 0.5 BRL (tick is 1 BRL for BTC-BRL),
        # amount within 1e-7 BTC, time_stamp not older than `since_ts - 5s`.
        candidates: List[Tuple[str, float]] = []
        for o in orders:
            if not isinstance(o, dict):
                continue
            oid = o.get("id")
            if oid is None:
                continue
            if str(o.get("side", "")).lower() != expected_side:
                continue
            try:
                o_price = Decimal(str(o.get("price", "0")))
                o_amount = Decimal(str(o.get("amount", "0")))
            except Exception:
                continue
            if abs(o_price - target_price) > Decimal("0.5"):
                continue
            if abs(o_amount - target_amount) > Decimal("0.0000001"):
                continue
            ts_str = o.get("time_stamp")
            try:
                o_ts = datetime.datetime.strptime(
                    str(ts_str), "%Y-%m-%d %H:%M:%S"
                ).timestamp()
            except (TypeError, ValueError):
                o_ts = 0.0
            if o_ts and o_ts < since_ts - 5:
                continue
            candidates.append((str(oid), o_ts))

        if not candidates:
            return None
        # Most recent matching candidate.
        candidates.sort(key=lambda x: x[1], reverse=True)
        recovered_id = candidates[0][0]
        self.logger().warning(
            f"BitPreco placement RECOVERED orphan in-flight: "
            f"exchange_order_id={recovered_id} side={expected_side} "
            f"price={price_str} amount={amount_str} — adopting as tracked order "
            f"instead of leaving it orphan on the book."
        )
        return recovered_id

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        amount_str = f"{amount:f}"
        # BitPreco rejects fractional prices for BTC-BRL. We must send integer
        # BRL prices, with SIDE-AWARE rounding to keep LIMIT_MAKER intent:
        #   BUY  → floor (stays below the ask, won't cross)
        #   SELL → ceil  (stays above the bid, won't cross)
        # Without this, BitPreco would silently round (typically up), which
        # could flip a LIMIT_MAKER BUY into a taker fill and drain inventory.
        # Only applied to BTC-BRL where the integer-price rule is confirmed;
        # other pairs keep the price as-is for now.
        market = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        if market.upper() in ("BTC-BRL", "BTCBRL"):
            if trade_type is TradeType.BUY:
                price_to_send = price.quantize(Decimal("1"), rounding="ROUND_DOWN")
            else:
                price_to_send = price.quantize(Decimal("1"), rounding="ROUND_UP")
        else:
            price_to_send = price
        price_str = f"{price_to_send:f}"
        # BitPreco has no native LIMIT_MAKER (post-only) flag; treat LIMIT_MAKER
        # as a regular LIMIT order. See supported_order_types() for the rationale.
        is_limited = order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER)
        data = {
            "cmd": CONSTANTS.CMD_BUY if trade_type is TradeType.BUY else CONSTANTS.CMD_SELL,
            "market": market,
            "limited": is_limited,
            "amount": amount_str,
            "price": price_str
        }

        transact_time = time.time()

        # Wrap the request itself: on network/parse exceptions the order may
        # have been accepted by BitPreco anyway. Try to recover before
        # propagating, otherwise we leave an unhedged orphan on the book.
        try:
            response = await self._api_request(
                method=RESTMethod.POST,
                path_url=CONSTANTS.REST_URL,
                data=self._add_auth_token_to_req_body(data),
            )
        except Exception as net_err:
            self.logger().warning(
                f"BitPreco place_order request raised "
                f"({type(net_err).__name__}: {net_err}) — attempting recovery via open_orders."
            )
            recovered = await self._try_recover_placement(
                market, trade_type, amount_str, price_str, transact_time)
            if recovered is not None:
                return (recovered, transact_time)
            raise

        # Defensive parsing — BitPreco returns different shapes for success/failure
        # and we were silently raising KeyError, leaving orphan orders on the
        # exchange whenever the response was unexpected.
        if not isinstance(response, dict):
            self.logger().error(
                f"BitPreco place_order returned non-dict response (orphan risk): "
                f"type={type(response).__name__} value={response!r}"
            )
            recovered = await self._try_recover_placement(
                market, trade_type, amount_str, price_str, transact_time)
            if recovered is not None:
                return (recovered, transact_time)
            raise IOError(f"BitPreco place_order: non-dict response: {response!r}")

        if response.get("success") is False:
            # Explicit rejection — order was NOT placed. No recovery needed:
            # BitPreco's `success: false` is a guaranteed-not-on-book signal
            # (rate limit, balance, market closed, etc.).
            msg = response.get("message") or response.get("message_cod") or "<no message>"
            self.logger().warning(
                f"BitPreco rejected {data['cmd']} order (amount={amount_str} price={price_str}): "
                f"{msg} | response={response}"
            )
            raise IOError(f"BitPreco rejected order: {msg}")

        # On success the canonical key is 'order_id' but be tolerant of 'id'.
        # If neither is present we have no way to track the order — but the
        # order may have been placed anyway, so try to recover before raising.
        oid_value = response.get("order_id") or response.get("id")
        if oid_value is None:
            self.logger().error(
                f"BitPreco place_order: response missing order_id/id "
                f"(attempting recovery) — full response: {response}"
            )
            recovered = await self._try_recover_placement(
                market, trade_type, amount_str, price_str, transact_time)
            if recovered is not None:
                return (recovered, transact_time)
            raise IOError(f"BitPreco place_order: no order id in response: {response}")

        o_id = str(oid_value)
        return (o_id, transact_time)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        # Robust cancel — confirm the cancel actually happened, retry on
        # transient failures, and log every response so the cause is visible
        # when something goes wrong. Returning False here causes the framework
        # to retry; we only return True when the cancel is confirmed (or the
        # order is gone for any reason — already cancelled, already filled).
        exchange_order_id = tracked_order.exchange_order_id

        # Guard: if the order is still in PENDING_CREATE, exchange_order_id is
        # None. Sending None to BitPreco returns INVALID_ORDER_ID (not a real
        # error — the cancel just arrived before the creation response). Return
        # False so the framework retries on the next cycle; the executor's
        # _cancel_sent_with_id flag ensures a retry is issued once the real
        # exchange_order_id is available.
        if not exchange_order_id:
            self.logger().warning(
                f"_place_cancel: order_id={order_id} has no exchange_order_id yet "
                f"(still PENDING_CREATE) — skipping API call, returning False for retry."
            )
            return False

        data = {
            "cmd": CONSTANTS.CMD_CANCEL_ORDER,
            "order_id": exchange_order_id,
        }

        # Responses BitPreco may return for cancels:
        #   {success: true,  message_cod: "ORDER_CANCELED"}      → confirmed cancelled
        #   {success: false, message_cod: "ORDER_NOT_FOUND"}     → already gone (treat as success)
        #   {success: false, message_cod: "RATE_LIMIT_EXCEEDED"} → retry
        #   {success: false, message_cod: "INVALID_TOKEN"/...}   → don't retry, log error
        # Anything else with success=true that doesn't say ORDER_CANCELED is
        # ambiguous — log it loudly and treat as not-cancelled (framework will
        # retry on its next cancel cycle).
        # INVALID_ORDER_ID is included here only because the early-exit guard
        # above ensures we never call the API with exchange_order_id=None.
        # So the only remaining cause for INVALID_ORDER_ID is: the order was
        # already cancelled/filled by another code path (orphan_check, manual
        # cancel from the UI, or a parallel cancel that won the race). Treat
        # as gone — the framework just needs to know the order is no longer
        # active. Returning True here also avoids the noisy ERROR log spam.
        GONE_CODES = {"ORDER_CANCELED", "ORDER_NOT_FOUND", "ORDER_ALREADY_CANCELED",
                      "ORDER_FILLED", "ORDER_ALREADY_FILLED", "INVALID_ORDER_ID"}
        TRANSIENT_CODES = {"RATE_LIMIT_EXCEEDED"}
        max_attempts = 3
        backoff_sec = 0.5
        last_response = None

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self._api_request(
                    method=RESTMethod.POST,
                    path_url=CONSTANTS.REST_URL,
                    data=self._add_auth_token_to_req_body(data),
                )
            except Exception as e:
                self.logger().warning(
                    f"BitPreco cancel attempt {attempt}/{max_attempts} for "
                    f"exchange_order_id={exchange_order_id} raised: {type(e).__name__}: {e}"
                )
                if attempt < max_attempts:
                    await asyncio.sleep(backoff_sec * attempt)
                    continue
                return False

            last_response = response
            code = response.get("message_cod") if isinstance(response, dict) else None

            if code in GONE_CODES:
                self.logger().info(
                    f"BitPreco cancel confirmed for exchange_order_id={exchange_order_id} "
                    f"(code={code}, attempt={attempt}): {response}"
                )
                return True

            if code in TRANSIENT_CODES and attempt < max_attempts:
                self.logger().warning(
                    f"BitPreco cancel transient failure for exchange_order_id={exchange_order_id} "
                    f"(code={code}, attempt={attempt}/{max_attempts}) — retrying after "
                    f"{backoff_sec * attempt}s: {response}"
                )
                await asyncio.sleep(backoff_sec * attempt)
                continue

            # Unrecognised response shape OR non-transient failure: don't keep
            # retrying inside this call — let the framework decide.
            self.logger().error(
                f"BitPreco cancel UNCONFIRMED for exchange_order_id={exchange_order_id} "
                f"(code={code}, attempt={attempt}/{max_attempts}) — returning False so the "
                f"framework retries. Full response: {response}"
            )
            return False

        # Exhausted retries without confirmation
        self.logger().error(
            f"BitPreco cancel FAILED for exchange_order_id={exchange_order_id} after "
            f"{max_attempts} attempts. Last response: {last_response}"
        )
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:

        rules = [
            {
                "symbol": "BTC-BRL",
                "min_order_size": 0.0001,
                # BitPreco rejects fractional prices for BTC-BRL ("Fractional
                # prices are not allowed by tradding API"). The framework's
                # default quantize is floor — fine for BUY but unsafe for SELL
                # (would cross the bid). _place_order applies SIDE-AWARE
                # integer rounding (floor for BUY, ceil for SELL); we keep the
                # framework quantum at 0.01 so we don't lose sub-integer
                # precision before that side-aware rounding step.
                "min_price_increment": Decimal("0.01"),
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
                "symbol": "BUSD-BRL",
                "min_order_size": 1,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
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
                "symbol": "ABFY-BRL",
                "min_order_size": 10,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
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
                "symbol": "GMT-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "POLIS-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "ATLAS-BRL",
                "min_order_size": 10,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "AXS-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "SLP-BRL",
                "min_order_size": 10,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "CRZO-BRL",
                "min_order_size": 0.5,
                "min_price_increment": 0.5,
                "min_base_amount_increment": 0.1,
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
                data=self._add_auth_token_to_req_body(data),
                limit_id=CONSTANTS.REST_URL)

            for executed_order in list(response):
                # BitPreco may return non-dict items (e.g. strings) when there
                # are no fills or the response shape is unexpected — skip them.
                if not isinstance(executed_order, dict):
                    continue
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
                    fill_timestamp = timestamp * 1e-3
                    trade_update = TradeUpdate(
                        trade_id=order.client_order_id,
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

    async def _make_network_check_request(self):
        """
        Override base: BitPreco's PING_PATH_URL is a PUBLIC ticker endpoint
        that responds 200 regardless of credentials. The base implementation
        (`_api_get(check_network_request_path)`) would therefore accept any
        api_key/secret during the network check, leading to the user only
        discovering bad credentials when the bot tries to trade.

        Use `cmd: "balance"` instead — an authenticated endpoint that, after
        the BitPreco-side fix (May 2026), correctly returns
        `{"success": false, "message_cod": "INVALID_TOKEN"}` for invalid
        credentials.
        """
        response = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=self._add_auth_token_to_req_body({"cmd": "balance"}),
            limit_id=CONSTANTS.REST_URL,
        )
        if isinstance(response, dict) and response.get("success") is not True:
            raise IOError(f"BitPreco auth/network check failed: {response}")

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # BitPreco does not document a stable "order not found" error code/message.
        # Returning False causes the framework to treat any status-update error as
        # a real failure (with retries), which is the safe conservative behaviour
        # — at the cost of slightly noisier logs when an order has just been
        # filled or cancelled. Tighten this if/when error semantics are confirmed.
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # Same rationale as above. _place_cancel already returns False on a
        # non-success response, so the framework rarely needs to introspect the
        # exception in practice.
        return False

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        # If the order doesn't yet have an exchange_order_id (still in
        # PENDING_CREATE — placement ack hasn't returned), there's nothing
        # to poll on the exchange. Return a no-op OrderUpdate that preserves
        # the current state so the framework's tracker doesn't crash.
        # Previously this branch fell through and returned None implicitly,
        # surfacing as `'NoneType' object has no attribute 'client_order_id'`
        # in client_order_tracker._process_order_update.
        if not tracked_order.exchange_order_id:
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=None,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=tracked_order.current_state,
            )

        data = {
            "cmd": "order_status",
            "order_id": tracked_order.exchange_order_id
        }

        response = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=self._add_auth_token_to_req_body(data)
        )

        order = response.get("order") if isinstance(response, dict) else None

        # If the exchange has no record of the order (e.g. it was manually
        # cancelled and is no longer in the book), treat it as CANCELLED so
        # the connector stops retrying and the executor can react accordingly.
        if order is None:
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=OrderState.CANCELED,
            )

        order_status = order.get("status") if isinstance(order, dict) else None
        update_timestamp = time.time()

        if order_status and order_status in CONSTANTS.ORDER_STATE:
            new_state = CONSTANTS.ORDER_STATE[order_status]
        else:
            # Unknown / missing status — keep current tracker state. Don't
            # default to OPEN, because that could resurrect an order the
            # tracker had already moved out of OPEN (e.g. CANCELED).
            self.logger().warning(
                f"BitPreco order_status response missing/unknown status for "
                f"exchange_order_id={tracked_order.exchange_order_id}: "
                f"order={order} — preserving tracker state {tracked_order.current_state.name}"
            )
            new_state = tracked_order.current_state

        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=new_state,
        )

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        data = {"cmd": "balance"}
        balances = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=self._add_auth_token_to_req_body(data),
        )
        self.logger().debug(
            f"bitpreco _update_balances raw response: type={type(balances).__name__} "
            f"keys={list(balances.keys()) if isinstance(balances, dict) else 'n/a'}"
        )
        # Validate auth/shape. After the BitPreco-side fix (May 2026), an
        # invalid auth_token returns {"success": false, "message_cod":
        # "INVALID_TOKEN"} rather than the previous silent-zero response.
        if not isinstance(balances, dict) or balances.get("success") is not True:
            raise IOError(
                f"BitPreco balance fetch failed (likely invalid credentials): {balances}"
            )
        # The response is a flat dict mixing metadata (`success`, `timestamp`,
        # possibly `message_cod`, etc.) with balance entries (`BTC`, `BTC_locked`,
        # `BRL`, `BRL_locked`, ...). The original loop tried to Decimal()-cast
        # every key whose name didn't match the explicit skip-list, which
        # surfaced as `decimal.ConversionSyntax` when BitPreco added new
        # non-numeric metadata fields. Coerce defensively and log the raw
        # response on failure so the cause is visible.
        # We also only consider keys whose `<asset>_locked` counterpart exists
        # — that pairing is what marks the entry as a balance row.
        for item in balances:
            if item == "success" or item == "timestamp" or "_locked" in item:
                continue
            locked_key = f"{item}_locked"
            if locked_key not in balances:
                # Not a balance entry (e.g., 'message', 'message_cod', 'data').
                continue
            try:
                free_amount = Decimal(str(balances[item]))
                locked_amount = Decimal(str(balances[locked_key]))
            except Exception as e:
                self.logger().warning(
                    f"bitpreco _update_balances: skipping non-numeric entry "
                    f"{item!r}={balances[item]!r} (err={e}); raw response keys="
                    f"{list(balances.keys())}"
                )
                continue
            asset_name = item
            self._account_available_balances[asset_name] = free_amount
            self._account_balances[asset_name] = free_amount + locked_amount
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
                await self._sleep(5.0)
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
            raise

        bitpreco_pairs = list(filter(lambda pair: pair != "success", response.keys()))

        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=bitpreco_pairs)

    def _add_auth_token_to_req_body(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data["auth_token"] = f'{self.secret_key}{self.api_key}'
        return data
