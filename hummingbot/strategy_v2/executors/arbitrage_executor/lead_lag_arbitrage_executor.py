"""
LeadLagArbitrageExecutor — ArbitrageExecutor subclass with automatic unwind on
partial-leg failure.

The base ArbitrageExecutor only retries the failed leg up to max_retries times.
If one leg succeeds and the other fails, the position is left open until manual
intervention.

This subclass adds:
1. Detection of partial-leg failure (one side filled, the other failed).
2. Re-fetching live prices on both exchanges.
3. Slippage gate (`arb_max_unwind_slippage_bps`): if the cheapest unwind exceeds
   the gate, behaviour depends on `arb_unwind_strategy`:
     - "abort_and_alert" (default): leave position open, log CRITICAL, stop executor.
     - "force_unwind": execute at the best available price even with high slippage.
4. Unwind execution: MARKET inverse order on the exchange with lower estimated
   slippage. Closes the executor with `CloseType.UNWOUND` (success) or
   `CloseType.UNWIND_ABORTED` (gate triggered).
"""
import logging
from decimal import Decimal
from typing import Optional, Union

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import (
    LeadLagArbitrageExecutorConfig,
)
from hummingbot.strategy_v2.models.executors import CloseType


class LeadLagArbitrageExecutor(ArbitrageExecutor):
    """Arbitrage executor with partial-leg failure unwind."""
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy: StrategyV2Base,
        config: LeadLagArbitrageExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 3,
    ):
        super().__init__(
            strategy=strategy,
            config=config,
            update_interval=update_interval,
            max_retries=max_retries,
        )
        self.config: LeadLagArbitrageExecutorConfig = config
        self._unwind_attempted: bool = False

    # ------------------------------------------------------------------ #
    # Failure handling                                                     #
    # ------------------------------------------------------------------ #
    def process_order_failed_event(
        self, _, market, event: MarketOrderFailureEvent,
    ):
        """
        Override base: instead of blindly retrying the failed leg, check whether
        the OTHER leg already filled — if so, we have a partial-leg failure and
        must unwind (not retry, which would create more exposure).
        """
        is_buy_failure = self.buy_order.order_id == event.order_id
        is_sell_failure = self.sell_order.order_id == event.order_id

        if not (is_buy_failure or is_sell_failure):
            return  # not our event

        # Did the OTHER leg fill?
        buy_filled_amount = (
            self.buy_order.executed_amount_base
            if self.buy_order.order is not None else Decimal("0")
        )
        sell_filled_amount = (
            self.sell_order.executed_amount_base
            if self.sell_order.order is not None else Decimal("0")
        )

        partial_failure = (
            (is_buy_failure and sell_filled_amount > Decimal("0"))
            or (is_sell_failure and buy_filled_amount > Decimal("0"))
        )

        if partial_failure and not self._unwind_attempted:
            self._unwind_attempted = True
            self.logger().warning(
                f"Partial-leg failure detected. "
                f"buy_filled={buy_filled_amount} sell_filled={sell_filled_amount}. "
                f"Triggering unwind."
            )
            # Schedule unwind asynchronously (we're in a sync event handler)
            safe_ensure_future(self._unwind_position(
                executed_side=TradeType.SELL if sell_filled_amount > 0 else TradeType.BUY,
                executed_amount=sell_filled_amount if sell_filled_amount > 0 else buy_filled_amount,
            ))
            return

        # No partial failure → fall back to base retry logic
        super().process_order_failed_event(_, market, event)

    # ------------------------------------------------------------------ #
    # Unwind                                                              #
    # ------------------------------------------------------------------ #
    async def _unwind_position(
        self,
        executed_side: TradeType,
        executed_amount: Decimal,
    ):
        """
        Execute MARKET inverse on the exchange with lower slippage to flatten
        the residual exposure. Honours `arb_max_unwind_slippage_bps` gate.
        """
        # Inverse side to flatten exposure
        inverse_side = (
            TradeType.SELL if executed_side == TradeType.BUY else TradeType.BUY
        )

        # Fetch live prices on both connectors for the inverse trade
        try:
            slip_buy_market = await self._estimate_unwind_slippage(
                self.buying_market, inverse_side, executed_amount,
            )
            slip_sell_market = await self._estimate_unwind_slippage(
                self.selling_market, inverse_side, executed_amount,
            )
        except Exception as e:
            self.logger().critical(
                f"UNWIND FAILED to fetch slippage estimates: {e}. "
                f"Position {executed_side.name} {executed_amount} OPEN."
            )
            self.close_type = CloseType.UNWIND_ABORTED
            self.stop()
            return

        best_market, best_slip = (
            (self.buying_market, slip_buy_market)
            if slip_buy_market <= slip_sell_market
            else (self.selling_market, slip_sell_market)
        )

        gate = self.config.arb_max_unwind_slippage_bps

        if best_slip > gate:
            if self.config.arb_unwind_strategy == "abort_and_alert":
                self.logger().critical(
                    f"UNWIND ABORTED: best slippage {best_slip:.2f} bps "
                    f"> max allowed {gate} bps. "
                    f"Position {executed_side.name} {executed_amount} on the executed leg "
                    f"is OPEN. MANUAL INTERVENTION REQUIRED."
                )
                self.close_type = CloseType.UNWIND_ABORTED
                self.stop()
                return
            else:  # force_unwind
                self.logger().warning(
                    f"UNWIND FORCED with high slippage {best_slip:.2f} bps "
                    f"(gate {gate}). Strategy=force_unwind."
                )

        # Execute MARKET inverse on the chosen connector
        try:
            order_id = self.place_order(
                connector_name=best_market.connector_name,
                trading_pair=best_market.trading_pair,
                order_type=OrderType.MARKET,
                side=inverse_side,
                amount=executed_amount,
                price=Decimal("1"),  # MARKET ignores
            )
            self.logger().info(
                f"Unwind MARKET {inverse_side.name} {executed_amount} on "
                f"{best_market.connector_name} (estimated slip {best_slip:.2f} bps), "
                f"order_id={order_id}"
            )
            self.close_type = CloseType.UNWOUND
            self.stop()
        except Exception as e:
            self.logger().critical(
                f"UNWIND ORDER PLACEMENT FAILED on {best_market.connector_name}: {e}. "
                f"Position {executed_side.name} {executed_amount} OPEN."
            )
            self.close_type = CloseType.UNWIND_ABORTED
            self.stop()

    async def _estimate_unwind_slippage(
        self,
        market,
        side: TradeType,
        amount: Decimal,
    ) -> Decimal:
        """
        Estimate slippage in bps for a MARKET order of `amount` on the given side.
        Uses VWAP from current order book vs current best bid/ask.
        """
        connector = self.connectors[market.connector_name]
        is_buy = (side == TradeType.BUY)
        # VWAP-based execution price
        vwap_price = await self.get_resulting_price_for_amount(
            connector=market.connector_name,
            trading_pair=market.trading_pair,
            is_buy=is_buy,
            order_amount=amount,
        )
        # Reference price (best bid/ask on the side we'd take)
        ob = connector.get_order_book(market.trading_pair)
        if is_buy:
            reference = Decimal(str(ob.get_price(True)))   # best ask
        else:
            reference = Decimal(str(ob.get_price(False)))  # best bid
        if reference is None or reference <= 0 or reference.is_nan():
            raise ValueError(f"invalid reference price for {market.trading_pair}")

        # Slippage = how much worse VWAP is than the touch price
        if is_buy:
            # buying: VWAP higher = worse
            slip_bps = (vwap_price - reference) / reference * Decimal("10000")
        else:
            # selling: VWAP lower = worse
            slip_bps = (reference - vwap_price) / reference * Decimal("10000")
        return max(slip_bps, Decimal("0"))
