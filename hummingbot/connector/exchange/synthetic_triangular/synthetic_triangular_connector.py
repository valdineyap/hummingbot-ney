from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_query_result import ClientOrderBookQueryResult
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee

s_decimal_NaN = Decimal("NaN")
s_decimal_0 = Decimal("0")


class SyntheticTriangularConnector(ExchangeBase):
    """
    Virtual connector that composes two real pairs on the same exchange into a synthetic pair.

    Example: BTC-USDT × USDT-BRL → synthetic BTC-BRL on Binance.

    Designed to be injected into the strategy's connectors dict so that SimpleXEMM (and any
    StrategyV2Base script) can treat a triangulated maker as if it were a direct-pair connector.

    Order type dispatch:
        LIMIT / LIMIT_MAKER → _place_maker_limit()
            Places leg1 (BTC-USDT) as a real LIMIT order and returns the real order ID.
            leg2 (USDT-BRL) fires as MARKET only after leg1 fills, via complete_maker_fill().

        MARKET → _place_taker_market()
            Fires both legs as MARKET immediately (simple, fire-and-forget).
    """

    def __init__(self, real_connector, leg1_pair: str, leg2_pair: str) -> None:
        """
        :param real_connector: Live ConnectorBase for the exchange (e.g. Binance).
        :param leg1_pair:      Base-to-intermediate pair (e.g. "BTC-USDT").
        :param leg2_pair:      Intermediate-to-quote pair (e.g. "USDT-BRL").
        """
        super().__init__()
        self._conn = real_connector
        self._leg1 = leg1_pair
        self._leg2 = leg2_pair

    # ------------------------------------------------------------------
    # Order placement — Python-level, called by ExchangeBase.c_buy / c_sell
    # ------------------------------------------------------------------

    def buy(self,
            trading_pair: str,
            amount: Decimal,
            order_type: OrderType = OrderType.MARKET,
            price: Decimal = s_decimal_NaN,
            **kwargs) -> str:
        if order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER):
            return self._place_maker_limit(is_buy=True, amount=Decimal(str(amount)),
                                           synthetic_price=Decimal(str(price)))
        return self._place_taker_market(is_buy=True, amount=Decimal(str(amount)))

    def sell(self,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType = OrderType.MARKET,
             price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        if order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER):
            return self._place_maker_limit(is_buy=False, amount=Decimal(str(amount)),
                                           synthetic_price=Decimal(str(price)))
        return self._place_taker_market(is_buy=False, amount=Decimal(str(amount)))

    def _place_maker_limit(self, is_buy: bool, amount: Decimal, synthetic_price: Decimal) -> str:
        """
        Place leg1 (BTC-USDT) as a real LIMIT order.

        Translates the synthetic BTC-BRL price to a real BTC-USDT price:
            leg1_price = synthetic_brl_price / usdt_brl_mid  (BRL/BTC ÷ BRL/USDT = USDT/BTC)

        Returns the REAL order ID so the strategy tracker can match fill events correctly.
        leg2 (USDT-BRL) fires later via complete_maker_fill() when leg1 actually fills.
        """
        usdt_brl_mid = Decimal(str(self._conn.get_mid_price(self._leg2)))
        leg1_price = synthetic_price / usdt_brl_mid
        leg1_price = self._conn.quantize_order_price(self._leg1, leg1_price)
        amount = self._conn.quantize_order_amount(self._leg1, amount)
        if is_buy:
            return self._conn.buy(self._leg1, amount, OrderType.LIMIT, leg1_price)
        return self._conn.sell(self._leg1, amount, OrderType.LIMIT, leg1_price)

    def _place_taker_market(self, is_buy: bool, amount: Decimal) -> str:
        """
        Fire both legs as MARKET orders immediately (taker / hedge path).

        buy:  leg2 (USDT-BRL) first → spend BRL, receive USDT
              leg1 (BTC-USDT) second → spend USDT, receive BTC
        sell: leg1 (BTC-USDT) first → receive USDT
              leg2 (USDT-BRL) second → convert USDT to BRL
        """
        if is_buy:
            leg1_result = self._conn.get_price_for_volume(self._leg1, True, float(amount))
            p1 = Decimal(str(leg1_result.result_price))
            usdt_needed = amount * p1
            leg2_result = self._conn.get_price_for_volume(self._leg2, True, float(usdt_needed))
            p2 = Decimal(str(leg2_result.result_price))
            self._conn.buy(self._leg2, usdt_needed, OrderType.MARKET, p2)
            leg1_id = self._conn.buy(self._leg1, amount, OrderType.MARKET, p1)
        else:
            leg1_result = self._conn.get_price_for_volume(self._leg1, False, float(amount))
            p1 = Decimal(str(leg1_result.result_price))
            usdt_received = amount * p1
            leg2_result = self._conn.get_price_for_volume(self._leg2, False, float(usdt_received))
            p2 = Decimal(str(leg2_result.result_price))
            leg1_id = self._conn.sell(self._leg1, amount, OrderType.MARKET, p1)
            self._conn.sell(self._leg2, usdt_received, OrderType.MARKET, p2)
        return f"SYN_{leg1_id[:8] if len(leg1_id) >= 8 else leg1_id}"

    def complete_maker_fill(self, is_buy: bool, btc_amount: Decimal, btc_usdt_fill_price: Decimal):
        """
        Called by the script's did_fill_order when a LIMIT leg1 order fills.

        Fires leg2 (USDT-BRL) as MARKET to neutralize the USDT position:
          buy fill:  BTC-USDT LIMIT filled → spent USDT → BUY USDT-BRL to replenish
          sell fill: BTC-USDT LIMIT filled → received USDT → SELL USDT-BRL to convert
        """
        usdt_amount = Decimal(str(btc_amount)) * Decimal(str(btc_usdt_fill_price))
        mid = Decimal(str(self._conn.get_mid_price(self._leg2)))
        if is_buy:
            return self._conn.buy(self._leg2, usdt_amount, OrderType.MARKET, mid)
        return self._conn.sell(self._leg2, usdt_amount, OrderType.MARKET, mid)

    def cancel(self, trading_pair: str, client_order_id: str):
        if isinstance(client_order_id, str) and client_order_id.startswith("SYN_"):
            return  # MARKET bundle — already filled, nothing to cancel
        self._conn.cancel(self._leg1, client_order_id)  # real LIMIT order

    # ------------------------------------------------------------------
    # Price queries — used by SimpleXEMM for spread calculation
    # ------------------------------------------------------------------

    def get_price_for_volume(self, trading_pair: str, is_buy: bool, volume) -> ClientOrderBookQueryResult:
        """Synthetic price = P(BTC-USDT) × P(USDT-BRL), orderbook-depth aware."""
        volume = Decimal(str(volume))
        leg1 = self._conn.get_price_for_volume(self._leg1, is_buy, float(volume))
        p1 = Decimal(str(leg1.result_price))
        usdt_notional = volume * p1
        leg2 = self._conn.get_price_for_volume(self._leg2, is_buy, float(usdt_notional))
        p2 = Decimal(str(leg2.result_price))
        synthetic_price = p1 * p2
        return ClientOrderBookQueryResult(s_decimal_NaN, volume, synthetic_price, volume)

    def get_mid_price(self, trading_pair: str) -> Decimal:
        mid1 = Decimal(str(self._conn.get_mid_price(self._leg1)))
        mid2 = Decimal(str(self._conn.get_mid_price(self._leg2)))
        return mid1 * mid2

    # ------------------------------------------------------------------
    # Balance and fee — used by SimpleXEMM budget checks
    # ------------------------------------------------------------------

    def get_available_balance(self, currency: str) -> Decimal:
        return Decimal(str(self._conn.get_available_balance(currency)))

    def get_balance(self, currency: str) -> Decimal:
        return Decimal(str(self._conn.get_balance(currency)))

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        return self._conn.get_fee(base_currency, quote_currency, order_type, order_side,
                                  amount, price, is_maker)

    # ------------------------------------------------------------------
    # Connectivity — delegates to real connector
    # ------------------------------------------------------------------

    @property
    def ready(self) -> bool:
        return self._conn.ready

    @property
    def status_dict(self) -> Dict[str, bool]:
        return self._conn.status_dict

    @property
    def name(self) -> str:
        return f"synthetic({self._conn.name}:{self._leg1}×{self._leg2})"

    # ------------------------------------------------------------------
    # Required abstract stubs (not called during XEMM operation)
    # ------------------------------------------------------------------

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return {}

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return []

    async def cancel_all(self, timeout_seconds: float):
        return []
