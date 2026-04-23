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

    Example: BTC-USDT × USDT-BRL → synthetic BTC-BRL on Bybit.

    Designed to be injected into the strategy's connectors dict so that SimpleXEMM (and any
    StrategyV2Base script) can treat a triangulated taker as if it were a direct-pair connector.

    Dispatch chain for order placement:
        strategy.buy("bybit_synthetic", "BTC-BRL", amount, ...)
          → c_buy_with_specific_market                 (strategy_base.pyx)
            → market.c_buy(...)                        (Cython vtable dispatch)
              → ExchangeBase.c_buy → self.buy(...)     (Python MRO override below)
                → _conn.buy(leg2, MARKET)              (buy USDT first)
                → _conn.buy(leg1, MARKET)              (buy BTC with USDT)

    USDT neutrality:
        - buy:  leg2 (USDT-BRL) fires before leg1 (BTC-USDT) so USDT is available.
        - sell: leg1 (BTC-USDT) fires first to receive USDT, then leg2 (USDT-BRL) sells it.
        - Residual drift is corrected by the rebalancer in xemm_triangular.py.
    """

    def __init__(self, real_connector, leg1_pair: str, leg2_pair: str) -> None:
        """
        :param real_connector: Live ConnectorBase for the taker exchange (e.g. Bybit).
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
        """
        BUY synthetic (want BTC, pay BRL):
          leg2: buy USDT-BRL first  → spend BRL, receive USDT
          leg1: buy BTC-USDT second → spend USDT, receive BTC
        """
        amount = Decimal(str(amount))
        leg1_result = self._conn.get_price_for_volume(self._leg1, True, float(amount))
        p1 = Decimal(str(leg1_result.result_price))
        usdt_needed = amount * p1

        leg2_result = self._conn.get_price_for_volume(self._leg2, True, float(usdt_needed))
        p2 = Decimal(str(leg2_result.result_price))

        leg2_id = self._conn.buy(self._leg2, usdt_needed, OrderType.MARKET, p2)
        leg1_id = self._conn.buy(self._leg1, amount, OrderType.MARKET, p1)
        return f"SYN_{leg1_id[:8] if len(leg1_id) >= 8 else leg1_id}"

    def sell(self,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType = OrderType.MARKET,
             price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        """
        SELL synthetic (have BTC, receive BRL):
          leg1: sell BTC-USDT first → receive USDT
          leg2: sell USDT-BRL second → receive BRL
        """
        amount = Decimal(str(amount))
        leg1_result = self._conn.get_price_for_volume(self._leg1, False, float(amount))
        p1 = Decimal(str(leg1_result.result_price))
        usdt_received = amount * p1

        leg2_result = self._conn.get_price_for_volume(self._leg2, False, float(usdt_received))
        p2 = Decimal(str(leg2_result.result_price))

        leg1_id = self._conn.sell(self._leg1, amount, OrderType.MARKET, p1)
        self._conn.sell(self._leg2, usdt_received, OrderType.MARKET, p2)
        return f"SYN_{leg1_id[:8] if len(leg1_id) >= 8 else leg1_id}"

    def cancel(self, trading_pair: str, client_order_id: str):
        pass  # MARKET orders do not need cancellation

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
