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
import asyncio
import logging
from decimal import Decimal
from typing import Optional, Union

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
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
        # Tracks which legs (if any) were placed as aggressive LIMITs so the
        # watchdog only fires on those — MARKET legs don't need timeout
        # supervision (they're synchronously matched server-side).
        self._aggressive_limit_buy_active: bool = False
        self._aggressive_limit_sell_active: bool = False

    # ------------------------------------------------------------------ #
    # Leg ordering (parallel / maker_first / taker_first)                  #
    # ------------------------------------------------------------------ #
    async def execute_arbitrage(self):
        """Override base to support configurable leg ordering.

        Modes (role-based, exchange-agnostic):
          - ``parallel`` (default): place both legs back-to-back without
            awaiting between them. The REST round-trips overlap on the
            event loop — minimal exposure window between spawn and both
            legs landing on their respective exchanges.
          - ``maker_first``: dispatch the leg on the maker connector
            first, wait for its REST to ACK (exchange_order_id assigned),
            then dispatch the taker leg.
          - ``taker_first``: symmetric — taker leg first.

        Maker/taker identification comes from ``config.maker_connector_name``
        (passed by the controller). If absent and a non-parallel mode is
        requested, the executor falls back to parallel with a warning.

        Unknown modes also fall back to parallel with a warning.
        """
        from hummingbot.strategy_v2.runnable_base import RunnableStatus
        self._status = RunnableStatus.SHUTTING_DOWN
        mode = getattr(self.config, "arb_leg_execution_order", "parallel")

        if mode == "parallel":
            self.place_buy_arbitrage_order()
            self.place_sell_arbitrage_order()
            return

        maker_name = getattr(self.config, "maker_connector_name", None)
        if maker_name is None:
            self.logger().warning(
                f"[leg_ordering] mode={mode!r} but maker_connector_name "
                f"not set on config — falling back to parallel"
            )
            self.place_buy_arbitrage_order()
            self.place_sell_arbitrage_order()
            return

        # Sequential modes — determine which leg is "first" based on
        # whether the buying or selling leg is on the maker connector.
        buying_is_maker = self.buying_market.connector_name == maker_name
        if mode == "maker_first":
            buy_is_first = buying_is_maker
        elif mode == "taker_first":
            buy_is_first = not buying_is_maker
        else:
            self.logger().warning(
                f"[leg_ordering] unknown mode {mode!r} — falling back to "
                f"parallel"
            )
            self.place_buy_arbitrage_order()
            self.place_sell_arbitrage_order()
            return

        if buy_is_first:
            self.place_buy_arbitrage_order()
            await self._wait_for_order_ack(self.buy_order)
            self.place_sell_arbitrage_order()
        else:
            self.place_sell_arbitrage_order()
            await self._wait_for_order_ack(self.sell_order)
            self.place_buy_arbitrage_order()

    async def _wait_for_order_ack(
        self,
        tracked_order_wrapper,
        max_wait_sec: float = 5.0,
        poll_interval_sec: float = 0.05,
    ) -> bool:
        """Wait until the order has been acknowledged by the exchange
        (i.e., ``exchange_order_id`` is assigned on the tracker). This is
        the moment ``_place_order`` REST returned successfully. Does NOT
        wait for fill — that would take arbitrarily long for non-crossing
        LIMITs and conflict with the AGGRESSIVE_LIMIT timeout watchdog.

        On timeout (max_wait_sec elapsed), logs a warning and returns
        False — the caller proceeds to the next leg anyway, treating
        the timeout as a placement failure that the partial-leg
        ``UNWIND`` path will handle if the order eventually lands.
        """
        max_iterations = int(max_wait_sec / poll_interval_sec)
        for _ in range(max_iterations):
            order = tracked_order_wrapper.order
            if order is not None and order.exchange_order_id is not None:
                return True
            await asyncio.sleep(poll_interval_sec)
        self.logger().warning(
            f"[leg_ordering] timeout {max_wait_sec}s waiting for ACK on "
            f"order_id={tracked_order_wrapper.order_id} — placing next leg "
            f"anyway (partial-leg failure path will handle inconsistency)"
        )
        return False

    # ------------------------------------------------------------------ #
    # Order placement — aggressive LIMIT for BitPreco                     #
    # ------------------------------------------------------------------ #
    def _should_use_aggressive_limit(self, market) -> bool:
        """The aggressive-LIMIT path triggers only for the MAKER leg when
        explicitly enabled in config. The taker leg (typically deep and
        fast on its own exchange) keeps the classical MARKET path.

        Maker identity comes from ``config.maker_connector_name``. If not
        set, the feature is silently disabled (treat as MARKET).
        """
        if getattr(self.config, "arb_maker_leg_type", "MARKET") != "AGGRESSIVE_LIMIT":
            return False
        maker_name = getattr(self.config, "maker_connector_name", None)
        if maker_name is None:
            return False
        return market.connector_name == maker_name

    def _aggressive_limit_price(self, market, side: TradeType) -> Decimal:
        """Compute a marketable LIMIT price.

          BUY  → best_ask × (1 + margin_pct)   (crosses upward into asks)
          SELL → best_bid × (1 − margin_pct)   (crosses downward into bids)

        The margin (default 0.5%) is well beyond typical book movement in
        the timeout window, so under normal conditions the LIMIT fills
        fully on placement (like a MARKET) but with bounded slippage:
        BitPreco will not match worse than this limit price.
        """
        connector = self.connectors[market.connector_name]
        margin = self.config.arb_aggressive_limit_margin_pct
        if side == TradeType.BUY:
            best_ask = connector.get_price_by_type(
                market.trading_pair, PriceType.BestAsk
            )
            return best_ask * (Decimal("1") + margin)
        else:
            best_bid = connector.get_price_by_type(
                market.trading_pair, PriceType.BestBid
            )
            return best_bid * (Decimal("1") - margin)

    def place_buy_arbitrage_order(self):
        if not self._should_use_aggressive_limit(self.buying_market):
            super().place_buy_arbitrage_order()
            return
        price = self._aggressive_limit_price(self.buying_market, TradeType.BUY)
        self.buy_order.order_id = self.place_order(
            connector_name=self.buying_market.connector_name,
            trading_pair=self.buying_market.trading_pair,
            order_type=OrderType.LIMIT,
            side=TradeType.BUY,
            amount=self.order_amount,
            price=price,
        )
        self._aggressive_limit_buy_active = True
        self.logger().info(
            f"[aggressive_limit] placed BUY LIMIT on "
            f"{self.buying_market.connector_name} amount={self.order_amount} "
            f"price={price} (margin={self.config.arb_aggressive_limit_margin_pct})"
        )
        safe_ensure_future(self._aggressive_limit_watchdog(is_buy_leg=True))

    def place_sell_arbitrage_order(self):
        if not self._should_use_aggressive_limit(self.selling_market):
            super().place_sell_arbitrage_order()
            return
        price = self._aggressive_limit_price(self.selling_market, TradeType.SELL)
        self.sell_order.order_id = self.place_order(
            connector_name=self.selling_market.connector_name,
            trading_pair=self.selling_market.trading_pair,
            order_type=OrderType.LIMIT,
            side=TradeType.SELL,
            amount=self.order_amount,
            price=price,
        )
        self._aggressive_limit_sell_active = True
        self.logger().info(
            f"[aggressive_limit] placed SELL LIMIT on "
            f"{self.selling_market.connector_name} amount={self.order_amount} "
            f"price={price} (margin={self.config.arb_aggressive_limit_margin_pct})"
        )
        safe_ensure_future(self._aggressive_limit_watchdog(is_buy_leg=False))

    async def _aggressive_limit_watchdog(self, is_buy_leg: bool):
        """Force-cancel an aggressive LIMIT if it doesn't fully fill within
        ``arb_aggressive_limit_timeout_sec``. After cancel, the cancel-side
        late_fill_recovery in the BitPreco connector emits any partial fill,
        and this method computes the resulting imbalance and triggers the
        existing ``_unwind_position`` path on the residual.

        Scenarios after timeout:
          - Fully filled in window  → noop (graceful exit)
          - Partially filled        → cancel + unwind the unhedged portion
          - Not filled at all       → cancel + unwind the entire opposite leg
        """
        timeout = self.config.arb_aggressive_limit_timeout_sec
        await asyncio.sleep(timeout)

        leg_order = self.buy_order if is_buy_leg else self.sell_order
        market = self.buying_market if is_buy_leg else self.selling_market

        # Fully filled before timeout → nothing to do
        if leg_order.order is not None and leg_order.order.is_filled:
            return
        # Order object not even registered yet (rare race) → bail
        if leg_order.order_id is None:
            return

        self.logger().warning(
            f"[aggressive_limit_timeout] {market.connector_name} leg "
            f"{leg_order.order_id} not fully filled in {timeout}s — "
            f"forcing cancel + unwind residue"
        )
        try:
            self._strategy.cancel(
                connector_name=market.connector_name,
                trading_pair=market.trading_pair,
                order_id=leg_order.order_id,
            )
        except Exception as e:
            self.logger().error(
                f"[aggressive_limit_timeout] cancel raised: "
                f"{type(e).__name__}: {e} — proceeding to unwind check anyway"
            )

        # Give the cancel + late_fill_recovery path a moment to update the
        # tracker's executed_amount before we compute the imbalance.
        await asyncio.sleep(0.5)

        buy_filled = (
            self.buy_order.executed_amount_base
            if self.buy_order.order is not None else Decimal("0")
        )
        sell_filled = (
            self.sell_order.executed_amount_base
            if self.sell_order.order is not None else Decimal("0")
        )

        # Imbalance: positive → excess long (BUY > SELL filled).
        imbalance = buy_filled - sell_filled
        if abs(imbalance) <= Decimal("0"):
            # Both legs exactly aligned (rare but possible) — nothing to unwind
            return

        if self._unwind_attempted:
            # Handler already on the way from process_order_failed_event
            return
        self._unwind_attempted = True

        executed_side = TradeType.BUY if imbalance > Decimal("0") else TradeType.SELL
        residue = abs(imbalance)
        self.logger().warning(
            f"[aggressive_limit_timeout] partial fill on aggressive LIMIT. "
            f"buy_filled={buy_filled} sell_filled={sell_filled} "
            f"residue={residue} side_to_unwind={executed_side.name} — "
            f"calling _unwind_position"
        )
        await self._unwind_position(
            executed_side=executed_side,
            executed_amount=residue,
        )

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
        # VWAP-based execution price.
        # Note: base ``ArbitrageExecutor.get_resulting_price_for_amount`` takes
        # ``exchange``, not ``connector`` — using the wrong kwarg here was the
        # bug that made every unwind path raise TypeError, leaving partial-leg
        # failures with an open position (observed prod 2026-05-11 18:06:53).
        vwap_price = await self.get_resulting_price_for_amount(
            exchange=market.connector_name,
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
