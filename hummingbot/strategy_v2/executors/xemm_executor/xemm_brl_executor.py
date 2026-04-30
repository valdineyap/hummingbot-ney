"""
XEMMBRLExecutor — XEMMExecutor subclass that uses OrderType.LIMIT_MAKER for
the maker leg.

The base XEMMExecutor uses OrderType.LIMIT, which means an order that crosses
the book at placement time will execute as taker. With ~100-300ms home
latency between maker_target_price calculation and order arrival at the
exchange, that risk is real and would double the round-trip taker fee,
making the strategy uneconomic.

OrderType.LIMIT_MAKER is rejected by the exchange if it would cross the book
at placement time — failing safe. Both Binance and Bybit Spot connectors
support it (verified in their `supported_order_types`).

We override only `create_maker_order` and `validate_sufficient_balance` for
the maker_order_candidate; everything else (taker hedge, lifecycle, cancel,
profitability monitoring) is inherited unchanged.
"""
from decimal import Decimal

from hummingbot.core.data_type.common import OrderType, PriceType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors import TrackedOrder


class XEMMBRLExecutor(XEMMExecutor):
    """
    XEMM executor that uses LIMIT_MAKER for the maker order leg.

    Identical to XEMMExecutor in every other respect (config, lifecycle,
    taker hedge, cancel, profitability monitoring, custom_info, etc).
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            import logging
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    async def validate_sufficient_balance(self):
        """
        Override base: the maker_order_candidate uses LIMIT_MAKER so the fee
        estimation matches the order type that will be placed.
        """
        mid_price = self.get_price(
            self.maker_connector, self.maker_trading_pair,
            price_type=PriceType.MidPrice,
        )
        maker_order_candidate = OrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT_MAKER,
            order_side=self.maker_order_side,
            amount=self.config.order_amount,
            price=mid_price,
        )
        taker_order_candidate = OrderCandidate(
            trading_pair=self.taker_trading_pair,
            is_maker=False,
            order_type=OrderType.MARKET,
            order_side=self.taker_order_side,
            amount=self.config.order_amount,
            price=mid_price,
        )
        maker_adjusted_candidate = self.adjust_order_candidates(
            self.maker_connector, [maker_order_candidate])[0]
        taker_adjusted_candidate = self.adjust_order_candidates(
            self.taker_connector, [taker_order_candidate])[0]
        if maker_adjusted_candidate.amount == Decimal("0") or taker_adjusted_candidate.amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough budget to open position.")
            self.stop()

    async def create_maker_order(self):
        """
        Override base: place the maker order with LIMIT_MAKER instead of
        LIMIT. If the price would cross the book at the exchange, the order
        is rejected (safe) instead of executing as taker (unsafe).
        """
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=self.maker_order_side,
            amount=self.config.order_amount,
            price=self._maker_target_price,
        )
        self.maker_order = TrackedOrder(order_id=order_id)
        self.logger().info(
            f"Created maker order (LIMIT_MAKER) {order_id} at price {self._maker_target_price}."
        )
