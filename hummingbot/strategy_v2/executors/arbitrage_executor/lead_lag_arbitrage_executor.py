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
from typing import Optional

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.event.events import (
    MarketOrderFailureEvent,
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
        # Set True when execute_arbitrage runs in a fill-based leg-ordering
        # mode ("maker_first" / "taker_first"). The aggressive-limit watchdog
        # uses this to skip the unwind path — in fill-based modes the second
        # leg is sized to the realized executed_amount, so by construction
        # there is no taker exposure to unwind.
        self._fill_based_leg_ordering_active: bool = False

    # ------------------------------------------------------------------ #
    # Leg ordering (parallel / maker_first / taker_first)                  #
    # ------------------------------------------------------------------ #
    async def execute_arbitrage(self):
        """Override base to support configurable leg ordering.

        Modes (role-based, exchange-agnostic):

          ``parallel`` (default)
            Place both legs back-to-back without awaiting between them.
            REST round-trips overlap on the event loop. Lowest latency,
            highest exposure if one leg fails to fill (failed-leg unwind
            path then fires).

          ``maker_first``
            Place the maker leg first at full ``order_amount``. Poll
            until ``executed_amount_base > 0`` OR the maker order
            terminates with zero fills.
              - Any fill (full or partial) → cancel any remaining open
                portion, then place the taker leg sized to the realized
                maker ``executed_amount_base``. The 2nd leg matches what
                actually crossed, never more.
              - Zero fills at terminal state (typically the aggressive-
                limit watchdog's cancel at timeout) → close cleanly with
                no taker placement. By construction there is no exposure
                to unwind.

          ``taker_first``
            Symmetric to ``maker_first`` — taker leg placed first,
            second leg (maker) sized to the realized taker fill.

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
            # Suppress controller-level orphan_hedge for whichever leg
            # lands on the maker connector. Without this the controller's
            # _maybe_dispatch_orphan_hedge fires in parallel with the arb
            # executor processing its own fill — observed 2026-05-18 19:01Z
            # double-hedge incident on a maker_first arb. The taker leg's
            # id is also registered (defensive no-op: orphan_hedge only
            # dispatches on maker-side fills).
            self._notify_controller_self_dispatched_market(self.buy_order.order_id)
            self._notify_controller_self_dispatched_market(self.sell_order.order_id)
            return

        maker_name = getattr(self.config, "maker_connector_name", None)
        if maker_name is None:
            self.logger().warning(
                f"[leg_ordering] mode={mode!r} but maker_connector_name "
                f"not set on config — falling back to parallel"
            )
            self.place_buy_arbitrage_order()
            self.place_sell_arbitrage_order()
            self._notify_controller_self_dispatched_market(self.buy_order.order_id)
            self._notify_controller_self_dispatched_market(self.sell_order.order_id)
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
            self._notify_controller_self_dispatched_market(self.buy_order.order_id)
            self._notify_controller_self_dispatched_market(self.sell_order.order_id)
            return

        # Fill-based execution: place 1st leg, wait for any fill, then
        # place 2nd leg sized to the realized executed_amount.
        self._fill_based_leg_ordering_active = True
        if buy_is_first:
            self.place_buy_arbitrage_order()
            first_wrapper = self.buy_order
            first_market = self.buying_market
        else:
            self.place_sell_arbitrage_order()
            first_wrapper = self.sell_order
            first_market = self.selling_market

        # Suppress controller-level orphan_hedge for the first leg. The
        # arb executor owns this order and processes the fill via
        # _wait_for_first_fill below; without this, the controller's
        # _maybe_dispatch_orphan_hedge fires in parallel and the resulting
        # MARKET hedge runs alongside the executor's own leg-2 dispatch
        # → double-hedge → short position → MINUTE_BURN kill (observed
        # 2026-05-18 19:01Z, ~R$75 loss). The controller's ownership
        # lookup (_find_live_executor_owning_maker) can't see arb owners
        # because ArbitrageExecutor.get_custom_info doesn't expose
        # maker_order_id; this explicit registration bridges that gap.
        self._notify_controller_self_dispatched_market(first_wrapper.order_id)

        # Wait until first leg has any executed amount > 0 OR reaches a
        # terminal state with zero fills. Time budget = aggressive-limit
        # timeout + 1.5s grace (so we observe the watchdog's cancel and
        # any late_fill_recovery emission before bailing out).
        timeout_sec = (
            self.config.arb_aggressive_limit_timeout_sec + 1.5
        )
        filled_amount = await self._wait_for_first_fill(
            first_wrapper,
            timeout_sec=timeout_sec,
        )

        if filled_amount is None or filled_amount <= Decimal("0"):
            self.logger().info(
                f"[leg_ordering] {mode}: first leg "
                f"{first_wrapper.order_id} terminated with zero fills — "
                f"closing executor with no taker placement (no exposure)"
            )
            self.close_type = CloseType.EXPIRED
            self.stop()
            return

        # Any fill (full or partial) — cancel any remaining open portion
        # of the first leg and capture late fills that land during the
        # cancel round-trip.
        full_amount = self.order_amount
        first_order_id = first_wrapper.order_id
        if filled_amount < full_amount and first_order_id is not None:
            try:
                self._strategy.cancel(
                    connector_name=first_market.connector_name,
                    trading_pair=first_market.trading_pair,
                    order_id=first_order_id,
                )
            except Exception as e:
                self.logger().error(
                    f"[leg_ordering] {mode}: cancel of partial first leg "
                    f"{first_order_id} raised "
                    f"{type(e).__name__}: {e} — proceeding to hedge "
                    f"the {filled_amount} already filled"
                )
            # Give the cancel REST + late_fill_recovery a moment to
            # reconcile the tracker. Re-read executed_amount in case more
            # filled during the cancel.
            await asyncio.sleep(0.5)
            if first_wrapper.order is not None:
                latest_filled = first_wrapper.order.executed_amount_base
                if latest_filled > filled_amount:
                    self.logger().info(
                        f"[leg_ordering] {mode}: late fill during cancel "
                        f"raised executed from {filled_amount} to "
                        f"{latest_filled} — hedging the larger amount"
                    )
                    filled_amount = latest_filled
                    # If the late fill brought us all the way to the
                    # original size, no shrink is needed below — but
                    # cap defensively anyway.
                    if filled_amount > full_amount:
                        filled_amount = full_amount

        # Resize order_amount so the 2nd leg matches the realized fill of
        # the 1st leg. The 1st leg already placed at full size (above) so
        # this mutation only affects the 2nd-leg dispatch path.
        self.order_amount = filled_amount
        self.logger().info(
            f"[leg_ordering] {mode}: first leg filled {filled_amount} of "
            f"{full_amount} (ratio={filled_amount / full_amount:.4f}) — "
            f"placing second leg sized to the realized fill"
        )
        if buy_is_first:
            self.place_sell_arbitrage_order()
        else:
            self.place_buy_arbitrage_order()

    async def _wait_for_first_fill(
        self,
        tracked_order_wrapper,
        timeout_sec: float,
        poll_interval_sec: float = 0.05,
    ) -> Optional[Decimal]:
        """Poll the order until ``executed_amount_base`` is positive OR
        the order reaches a terminal state (cancelled / failed / done).

        Returns:
            Decimal(executed_amount_base) — when any fill is detected.
            None — when the order terminated with zero fills (or the
                   timeout elapsed without any fill).

        This is the gating primitive for the fill-based leg-ordering
        modes (``maker_first`` and ``taker_first``). The caller cancels
        any remaining open portion when the returned amount is < the
        full ``order_amount``.
        """
        max_iterations = int(timeout_sec / poll_interval_sec)
        for _ in range(max_iterations):
            order = tracked_order_wrapper.order
            if order is not None:
                executed = order.executed_amount_base
                if executed > Decimal("0"):
                    return executed
                if (
                    order.is_cancelled
                    or order.is_failure
                    or order.is_done
                ):
                    # Terminal with zero fills
                    return None
            await asyncio.sleep(poll_interval_sec)
        self.logger().warning(
            f"[leg_ordering] _wait_for_first_fill: timeout {timeout_sec}s "
            f"waiting on {tracked_order_wrapper.order_id} — treating as "
            f"zero-fill termination"
        )
        return None

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

        # Fill-based leg ordering (``maker_first`` / ``taker_first``) sizes
        # the 2nd leg to the realized executed_amount of the 1st leg, so by
        # construction there is no taker exposure to unwind. Suppress the
        # unwind path to avoid racing the 2nd-leg placement.
        if self._fill_based_leg_ordering_active:
            self.logger().info(
                "[aggressive_limit_timeout] fill-based leg ordering active "
                "— skipping unwind path (2nd leg sized to realized fill)"
            )
            return

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
        Execute MARKET inverse on the exchange with the cheapest absolute
        expected price to flatten the residual exposure. Honours
        `arb_max_unwind_slippage_bps` gate.

        Routing (fix 2026-05-16): pick by **expected absolute exec price**
        (touch × slip), not by per-venue slip alone. The previous
        ``slip_buy <= slip_sell`` tie-break was blind to cross-venue
        spread — observed 2026-05-16 11:36:01Z, both venues estimated
        slip=0 (small qty, deep books) and the tie-break routed to
        ``buying_market`` (BitPreco) at 396712 while Binance was at
        396570 = 36 bps cheaper on the same side.
        """
        # Inverse side to flatten exposure
        inverse_side = (
            TradeType.SELL if executed_side == TradeType.BUY else TradeType.BUY
        )

        # Fetch slip + touch on both connectors for the inverse trade
        try:
            slip_buy_market = await self._estimate_unwind_slippage(
                self.buying_market, inverse_side, executed_amount,
            )
            slip_sell_market = await self._estimate_unwind_slippage(
                self.selling_market, inverse_side, executed_amount,
            )
            touch_buy_market = self._get_touch_price(self.buying_market, inverse_side)
            touch_sell_market = self._get_touch_price(self.selling_market, inverse_side)
        except Exception as e:
            self.logger().critical(
                f"UNWIND FAILED to fetch slippage estimates: {e}. "
                f"Position {executed_side.name} {executed_amount} OPEN."
            )
            self.close_type = CloseType.UNWIND_ABORTED
            self.stop()
            return

        # Expected absolute exec price = touch × (1 + slip_bps/10000) for BUY
        # (paying up the ask), touch × (1 − slip_bps/10000) for SELL (selling
        # down the bid). Lower is better for BUY (less paid); higher is
        # better for SELL (more received).
        sign = Decimal("1") if inverse_side == TradeType.BUY else Decimal("-1")
        bps = Decimal("10000")
        exec_buy_market = touch_buy_market * (Decimal("1") + sign * slip_buy_market / bps)
        exec_sell_market = touch_sell_market * (Decimal("1") + sign * slip_sell_market / bps)

        if inverse_side == TradeType.BUY:
            # Pay less: pick lower expected exec price
            if exec_buy_market <= exec_sell_market:
                best_market, best_slip, best_exec = (
                    self.buying_market, slip_buy_market, exec_buy_market,
                )
                alt_market, alt_exec = self.selling_market, exec_sell_market
            else:
                best_market, best_slip, best_exec = (
                    self.selling_market, slip_sell_market, exec_sell_market,
                )
                alt_market, alt_exec = self.buying_market, exec_buy_market
        else:
            # Receive more: pick higher expected exec price
            if exec_buy_market >= exec_sell_market:
                best_market, best_slip, best_exec = (
                    self.buying_market, slip_buy_market, exec_buy_market,
                )
                alt_market, alt_exec = self.selling_market, exec_sell_market
            else:
                best_market, best_slip, best_exec = (
                    self.selling_market, slip_sell_market, exec_sell_market,
                )
                alt_market, alt_exec = self.buying_market, exec_buy_market

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
                # price=0 triggers exchange_py_base to look up current mid for
                # the min-notional check (otherwise a placeholder like 1 makes
                # notional=1*amount and trips min_notional_size on small qtys).
                price=Decimal("0"),
            )
            self.logger().info(
                f"Unwind MARKET {inverse_side.name} {executed_amount} on "
                f"{best_market.connector_name} "
                f"(slip {best_slip:.2f} bps, exec~{best_exec:.2f}; "
                f"alt {alt_market.connector_name} exec~{alt_exec:.2f}), "
                f"order_id={order_id}"
            )
            # Suppress controller-level orphan_hedge dispatch on the
            # resulting fill: the unwind is a one-sided correction by
            # design (it flattens the leg-1 fill we already have on the
            # other venue); hedging it again on the taker would re-open
            # the exposure and create a drift→rebalance loop. The
            # controller-side counterpart of this fix lives in
            # ``XEMMLeadLag.register_self_dispatched_market_id``.
            self._notify_controller_self_dispatched_market(order_id)
            self.close_type = CloseType.UNWOUND
            self.stop()
        except Exception as e:
            self.logger().critical(
                f"UNWIND ORDER PLACEMENT FAILED on {best_market.connector_name}: {e}. "
                f"Position {executed_side.name} {executed_amount} OPEN."
            )
            self.close_type = CloseType.UNWIND_ABORTED
            self.stop()

    def _notify_controller_self_dispatched_market(self, order_id: Optional[str]) -> None:
        """Tell controllers to skip orphan_hedge for ``order_id``.

        Used in two paths:

          1. **Unwind** (`_unwind_partial`) — a self-dispatched MARKET
             that closes a one-sided exposure. Firing controller's
             orphan_hedge on it re-opens the exposure (drift→rebalance
             loop seen 2026-05-16).

          2. **Arb leg placement** (`execute_arbitrage`) — every leg
             placed by the arb executor is owned by it: the executor
             processes its own fill via the normal listener path or via
             `_wait_for_first_fill`. The controller's orphan_hedge would
             fire in parallel and create a double-hedge (observed
             2026-05-18 19:01Z, ~R$75 loss). The controller's owner
             lookup (`_find_live_executor_owning_maker`) doesn't catch
             arb owners because `ArbitrageExecutor.get_custom_info`
             doesn't expose `maker_order_id`; this explicit registration
             bridges that gap.

        Best-effort: discovers controllers via ``self._strategy.controllers``;
        silently no-ops if absent (e.g. MagicMock strategies in tests).
        Failure here only re-enables the prior orphan_hedge behaviour
        on that order's fill — it never breaks the arb itself.
        """
        if not order_id:
            return
        try:
            controllers = getattr(self._strategy, "controllers", None) or {}
            for ctrl in controllers.values():
                register = getattr(ctrl, "register_self_dispatched_market_id", None)
                if callable(register):
                    register(order_id)
        except Exception as e:
            self.logger().debug(
                f"[unwind] controller notify failed (non-fatal): "
                f"{type(e).__name__}: {e}"
            )

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

    def _get_touch_price(self, market, side: TradeType) -> Decimal:
        """Best bid/ask on ``market`` for ``side``. Used by the unwind
        router to compute expected absolute exec price across venues
        (touch × slip). Raises if the order book is missing or returns
        a non-positive price."""
        connector = self.connectors[market.connector_name]
        ob = connector.get_order_book(market.trading_pair)
        if side == TradeType.BUY:
            reference = Decimal(str(ob.get_price(True)))   # best ask
        else:
            reference = Decimal(str(ob.get_price(False)))  # best bid
        if reference is None or reference <= 0 or reference.is_nan():
            raise ValueError(f"invalid touch price for {market.trading_pair}")
        return reference
