"""
XEMMLeadLagExecutor — XEMMExecutor subclass that uses OrderType.LIMIT_MAKER for
the maker leg.

The base XEMMExecutor uses OrderType.LIMIT, which means an order that crosses
the book at placement time will execute as taker. With ~100-300ms home
latency between maker_target_price calculation and order arrival at the
exchange, that risk is real and would double the round-trip taker fee,
making the strategy uneconomic.

OrderType.LIMIT_MAKER is rejected by the exchange if it would cross the book
at placement time — failing safe. Both Binance and Bybit Spot connectors
support it (verified in their `supported_order_types`).

We override `create_maker_order`, `validate_sufficient_balance`, and
`control_shutdown_process`. Everything else (taker hedge, lifecycle, cancel,
profitability monitoring) is inherited unchanged.
"""
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors import TrackedOrder


class XEMMLeadLagExecutor(XEMMExecutor):
    """
    XEMM executor that uses LIMIT_MAKER for the maker order leg.

    Identical to XEMMExecutor in every other respect (config, lifecycle,
    taker hedge, cancel, profitability monitoring, custom_info, etc).
    """
    _logger = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Ghost fill guard: maker order IDs that were 'cancelled' with
        # executed_amount_base == 0.  BitPreco's CANT_CANCEL_FILLED_ORDER
        # response causes the connector to fire OrderCancelledEvent BEFORE
        # the WS fill event arrives (up to 20 s later).  We register the
        # order_id here so process_order_completed_event can detect and hedge
        # the fill when the event finally lands.
        self._ghost_maker_order_ids: set = set()

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

    async def control_shutdown_process(self):
        """
        Override base: guard against maker_order/taker_order being None.

        The base class calls self.maker_order.is_done unconditionally (line 226),
        but both fields can be None — maker_order after a profitability cancel,
        taker_order before any fill. Treat None as "nothing to wait for" (done).
        """
        maker_done = self.maker_order is None or self.maker_order.is_done
        taker_done = self.taker_order is None or self.taker_order.is_done
        if maker_done and taker_done:
            self.logger().info("Both orders are done, executor terminated.")
            self.stop()

    async def control_maker_order(self):
        """Override: place taker hedge IMMEDIATELY when maker is detected
        done with executed_amount > 0, BEFORE the base class clears
        ``self.maker_order``.

        Why this matters: fills can be discovered via REST reconcile (the
        controller's orphan_check / connector status poll) when the
        WebSocket account-stream is lagging or dropped. In that flow the
        connector updates ``maker_order.order.is_done = True`` first, then
        emits ``OrderCompletedEvent`` asynchronously on a later tick.

        Without this override, the base class's ``control_maker_order``
        clears ``self.maker_order`` on the very next tick (its
        ``elif self.maker_order.is_done`` branch), and a new maker is
        created before the event arrives. By the time
        ``process_order_completed_event`` fires, ``self.maker_order.order_id``
        no longer matches ``event.order_id`` and the hedge is never placed
        — leaving inventory drift that only the slower auto_rebalance path
        recovers, often with worse PnL (it can rebalance on the SAME
        exchange where the maker filled, missing the cross-exchange edge).

        Observed in prod 2026-05-11 12:07:18: maker BUY filled via REST
        reconcile, no hedge ever placed, audit detected drift 20s later and
        rebalanced MARKET SELL on BitPreco (the maker side) losing ~4 BRL
        vs. the proper cross-exchange hedge that would have made ~+0.025 BRL.

        Anti-double-hedge: the order_id is registered in
        ``_ghost_maker_order_ids`` here, so when the delayed
        OrderCompletedEvent arrives later, the ghost path in
        ``process_order_completed_event`` sees ``taker_order is not None``
        and skips with a warning instead of placing a second hedge.
        """
        if (self.maker_order is not None
                and self.maker_order.order is not None
                and self.maker_order.order.is_done
                and self.taker_order is None):
            executed = self.maker_order.order.executed_amount_base or Decimal("0")
            if executed > 0:
                order_id = self.maker_order.order_id
                self.logger().warning(
                    f"[reconcile_hedge] Maker order {order_id} done with "
                    f"executed={executed} (likely REST-reconciled fill or "
                    f"event arriving on same tick); placing taker hedge NOW "
                    f"before base clears maker_order."
                )
                # Anti-double-hedge: register so the delayed event no-ops.
                self._ghost_maker_order_ids.add(order_id)
                # Activate audit's 10s inflight window — avoids double action
                # from inventory_audit while the taker MARKET is settling.
                self._touch_controller_last_fill_time()
                try:
                    self.place_taker_order(amount=executed)
                    # Match base's flow after place_taker_order: transition
                    # so the next tick goes through control_shutdown_process.
                    self._status = RunnableStatus.SHUTTING_DOWN
                except Exception as e:
                    self.logger().error(
                        f"[reconcile_hedge] place_taker_order failed for "
                        f"{order_id}: {type(e).__name__}: {e}. "
                        f"inventory_audit will reconcile (worse PnL)."
                    )
        # Delegate to base for the normal state machine (cancel/refresh/etc).
        await super().control_maker_order()

    # ------------------------------------------------------------------
    # Ghost fill guard — BitPreco CANT_CANCEL_FILLED_ORDER race fix
    # ------------------------------------------------------------------

    def process_order_canceled_event(self, event_tag: int, market, event):
        """Override: register maker orders that may have filled while in-flight.

        BitPreco's cancel API returns ``CANT_CANCEL_FILLED_ORDER`` when the
        order matched right before the cancel arrived.  The connector maps
        this to ``return True`` (order is gone), which fires
        ``OrderCancelledEvent`` before the WS fill event arrives (typically
        5–20 s later).  The base class then reads ``executed_amount_base``
        which is still 0 at that moment and concludes "no fill, no hedge".

        We register the order_id here so that when the delayed
        ``BuyOrderCompletedEvent`` / ``SellOrderCompletedEvent`` eventually
        arrives, ``process_order_completed_event`` below can detect and hedge
        the ghost fill.
        """
        # Unconditional trace so we can confirm in prod whether the handler
        # is wired to the BitPreco cancel path. Audit 2026-05-11 showed 2x
        # CANT_CANCEL_FILLED_ORDER events with 0 `[ghost_guard]` log lines,
        # suggesting the handler may never have been invoked. Keep this at
        # debug level to avoid log spam from normal cancels.
        self.logger().debug(
            f"[ghost_guard] process_order_canceled_event called: "
            f"order_id={getattr(event, 'order_id', None)} "
            f"maker_order_id={self.maker_order.order_id if self.maker_order else None} "
            f"taker_order={'set' if self.taker_order else 'None'}"
        )
        if (self.maker_order
                and event.order_id == self.maker_order.order_id
                and self.taker_order is None):
            executed = Decimal("0")
            if self.maker_order.order is not None:
                executed = self.maker_order.order.executed_amount_base or Decimal("0")
            if executed <= 0:
                self._ghost_maker_order_ids.add(event.order_id)
                self.logger().info(
                    f"[ghost_guard] Maker order {event.order_id} registered as "
                    f"potential ghost fill (cancelled with executed=0)."
                )
        super().process_order_canceled_event(event_tag, market, event)

    def process_order_completed_event(self, event_tag: int, market, event):
        """Override: also handle ghost fills for the CANT_CANCEL_FILLED_ORDER race.

        When a ``BuyOrderCompletedEvent`` / ``SellOrderCompletedEvent`` arrives
        for an order already in ``_ghost_maker_order_ids``, the executor has
        moved on to a new maker order so the normal check
        ``self.maker_order.order_id == event.order_id`` fails.  We detect that
        here and place the taker hedge directly — bypassing
        ``place_taker_order()`` to avoid overwriting ``self.taker_order`` /
        ``_status`` for the running cycle.

        We also touch the controller's ``_last_fill_time`` which activates the
        10-second inflight window in ``_has_inflight_activity()``, preventing
        the inventory audit from double-hedging before our MARKET taker fills.
        """
        # Unconditional trace — same rationale as process_order_canceled_event.
        # Helps verify in prod that completed events reach this handler when
        # the order_id is in `_ghost_maker_order_ids` (i.e. arrived after the
        # CANT_CANCEL_FILLED_ORDER race window).
        self.logger().debug(
            f"[ghost_guard] process_order_completed_event called: "
            f"order_id={getattr(event, 'order_id', None)} "
            f"in_ghost_set={getattr(event, 'order_id', None) in self._ghost_maker_order_ids} "
            f"maker_order_id={self.maker_order.order_id if self.maker_order else None}"
        )
        # Normal path: event is for the current active maker order.
        if self.maker_order and event.order_id == self.maker_order.order_id:
            super().process_order_completed_event(event_tag, market, event)
            return

        # Ghost fill path: delayed complete for a previously 'cancelled' order.
        if event.order_id not in self._ghost_maker_order_ids:
            return

        self._ghost_maker_order_ids.discard(event.order_id)

        if self.taker_order is not None:
            # Already hedged via the normal completed-event path or a prior
            # ghost event — skip to avoid double-hedging.
            self.logger().warning(
                f"[ghost_fill] {event.order_id}: taker already set — "
                f"skipping duplicate hedge."
            )
            return

        amount = getattr(event, "base_asset_amount", Decimal("0")) or Decimal("0")
        if amount <= 0:
            self.logger().warning(
                f"[ghost_fill] {event.order_id}: zero base_asset_amount — "
                f"skipping (inventory_audit will reconcile if needed)."
            )
            return

        self.logger().warning(
            f"[ghost_fill] Late complete for maker order {event.order_id} "
            f"({amount} base): placing taker hedge on {self.taker_connector} "
            f"and suppressing inventory_audit for 10 s."
        )

        # Suppress the inventory_audit's auto_rebalance for 10 s so it does
        # not double-hedge before our MARKET taker fills (< 1 s on Binance).
        self._touch_controller_last_fill_time()

        # Place the MARKET taker hedge directly — do NOT call
        # place_taker_order() as that would set self.taker_order / SHUTTING_DOWN
        # and terminate the current active maker cycle.
        try:
            order_id = self.place_order(
                connector_name=self.taker_connector,
                trading_pair=self.taker_trading_pair,
                order_type=OrderType.MARKET,
                side=self.taker_order_side,
                amount=amount,
            )
            self.logger().warning(
                f"[ghost_fill] Taker hedge placed: {order_id} "
                f"MARKET {self.taker_order_side.name} {amount} {self.taker_trading_pair} "
                f"on {self.taker_connector}."
            )
        except Exception as e:
            self.logger().error(
                f"[ghost_fill] Failed to place taker hedge for {event.order_id}: "
                f"{type(e).__name__}: {e}. inventory_audit will reconcile."
            )

    def _touch_controller_last_fill_time(self) -> None:
        """Update the owning controller's ``_last_fill_time`` to now.

        This activates the 10-second inflight window in
        ``_has_inflight_activity()`` and prevents the inventory audit from
        queuing an auto_rebalance while our ghost-fill taker hedge is settling.
        Uses the same ``self.strategy.controllers`` accessor as
        ``_get_live_lead_bps()``.
        """
        try:
            cid = getattr(self.config, "controller_id", None)
            if cid:
                ctrl = getattr(self._strategy, "controllers", {}).get(cid)
                if ctrl is not None:
                    ctrl._last_fill_time = time.time()
                    self.logger().info(
                        f"[ghost_fill] controller._last_fill_time updated "
                        f"(10 s audit-suppression window started)."
                    )
        except Exception:
            pass  # best-effort; inventory_audit is the fallback

    def _get_live_lead_bps(self) -> Decimal:
        """Return the freshest lead-signal value available.

        The executor config is immutable (set at creation time), so
        ``config.lead_signal_bps`` goes stale when the executor lives
        across many order cycles without being recreated.  We prefer
        reading from the controller's ``processed_data`` (updated every
        tick) when accessible via ``self.strategy.controllers``.

        Falls back to the frozen config value gracefully so tests and
        any environment that doesn't expose ``strategy.controllers``
        continue to work unchanged.
        """
        try:
            cid = getattr(self.config, "controller_id", None)
            if cid:
                ctrl = getattr(self._strategy, "controllers", {}).get(cid)
                if ctrl is not None and ctrl.processed_data:
                    live = ctrl.processed_data.get("best_lead_bps")
                    if live is not None:
                        return live
        except Exception:
            pass
        return getattr(self.config, "lead_signal_bps", Decimal("0"))

    def _log_throttled_skip(self, msg: str, interval_sec: float = 60.0) -> None:
        """Log placement-skip messages at most once per ``interval_sec``.

        Called every executor tick (~500ms) when the maker book is outside the
        cancel band — without throttling this floods the log with thousands of
        identical lines. Stores per-instance state on the executor itself.
        """
        now = time.time()
        last = getattr(self, "_last_skip_log_ts", 0.0)
        if now - last >= interval_sec:
            self._last_skip_log_ts = now
            self.logger().info(msg)

    async def create_maker_order(self):
        """
        Override base: LIMIT_MAKER with book-aware pricing.

        Attempts to place 1 tick ahead of the best bid (BUY) or best ask (SELL)
        to maximise queue priority. Falls back to the profitability-floor price
        when the book is unavailable or when 1-tick improvement would violate
        min_profitability. The placement floor uses (min_profitability +
        placement_profitability_buffer) to create hysteresis and avoid
        immediately re-triggering the cancel floor on the next tick.
        """
        bound_price = None
        used_fallback = False
        book_mode = "fallback"
        lead_mode = "neutral"
        lead_bps = self._get_live_lead_bps()   # initialised here so fallback path can log it

        try:
            tick = self.get_trading_rules(
                self.maker_connector, self.maker_trading_pair
            ).min_price_increment

            best_bid = self.get_price(
                self.maker_connector, self.maker_trading_pair, PriceType.BestBid
            )
            best_ask = self.get_price(
                self.maker_connector, self.maker_trading_pair, PriceType.BestAsk
            )

            if (best_bid is None or best_ask is None
                    or best_bid.is_nan() or best_ask.is_nan()
                    or best_bid <= 0 or best_ask <= 0
                    or best_bid >= best_ask):
                raise ValueError(f"invalid maker book: bid={best_bid} ask={best_ask}")

            effective_min = (
                self.config.min_profitability
                + self.config.placement_profitability_buffer
            )

            # Lead-aware placement (Priority 4): tighten when lead favours the
            # side, widen when it opposes. In dead zone (|lead| < threshold)
            # no adjustment is applied. Configured at the controller level.
            lead_delta_bps = getattr(
                self.config, "placement_lead_aware_delta_bps", Decimal("0")
            )
            lead_bps = self._get_live_lead_bps()   # refresh inside try (may differ from pre-try read)
            lead_thr_bps = getattr(
                self.config, "placement_lead_signal_threshold_bps", Decimal("3")
            )
            lead_mode = "neutral"
            if lead_delta_bps > 0 and abs(lead_bps) > lead_thr_bps:
                # lead_bps > 0 means fair has risen relative to local → expect local up
                #   → favours BUY (tighter), opposes SELL (wider)
                # lead_bps < 0 means fair has fallen → favours SELL (tighter), opposes BUY
                favours = (
                    (self.maker_order_side == TradeType.BUY and lead_bps > 0)
                    or (self.maker_order_side == TradeType.SELL and lead_bps < 0)
                )
                delta = lead_delta_bps / Decimal("10000")
                if favours:
                    effective_min = max(
                        self.config.min_profitability,
                        effective_min - delta,
                    )
                    lead_mode = "favours"
                else:
                    effective_min = effective_min + delta
                    lead_mode = "opposes"

            # Cancel band: an order whose implied profitability falls outside
            # [min_profitability, max_profitability] is refreshed by the base
            # XEMMExecutor's profitability-monitoring loop. If we place outside
            # this band, the order is cancelled within a single tick — wasted
            # OTR. Compute both edges and clip placement into the band.
            #   BUY:  [min_buy_price (max profit cap),  max_buy_price (effective_min floor)]
            #   SELL: [min_sell_price (effective_min floor), max_sell_price (max profit cap)]
            if self.maker_order_side == TradeType.BUY:
                # Improve queue priority if spread allows; otherwise join best bid.
                if best_bid + tick < best_ask:
                    competitive_price = best_bid + tick
                    book_mode = "improve"
                else:
                    competitive_price = best_bid
                    book_mode = "join"

                # Pay at most max_buy_price (slipping above costs us profit floor).
                max_buy_price = self._taker_result_price / (
                    Decimal("1") + effective_min + self._tx_cost_pct
                )
                # Pay at least min_buy_price (paying less = profitability >
                # max_profitability → instant refresh). Cap is required to keep
                # the order alive past the next tick of the monitoring loop.
                min_buy_price = self._taker_result_price / (
                    Decimal("1") + self.config.max_profitability + self._tx_cost_pct
                )
                # Local book is too high relative to taker — any maker BUY at
                # or below best_ask would price below min_buy_price (instantly
                # refreshable). Skip placement until the book moves.
                if min_buy_price >= best_ask:
                    self._log_throttled_skip(
                        f"Skipping BUY placement: maker book too rich "
                        f"(best_ask={best_ask} <= min_buy_price={min_buy_price:.2f})."
                    )
                    return
                price = min(competitive_price, max_buy_price)
                if price < min_buy_price:
                    price = min_buy_price
                    book_mode = "clip_max"
                bound_price = max_buy_price

                # Round DOWN — never overpay into min_profitability territory.
                price = (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick
                # clip_max guard: ROUND_DOWN may push price below min_buy_price,
                # making profitability > max_profitability → instant cancel.
                # Nudge up by one tick to stay inside the cancel band.
                if price < min_buy_price:
                    price += tick

                # Post-rounding safety: ensure we still don't cross the ask.
                if price >= best_ask:
                    price -= tick

            else:  # SELL
                # Improve queue priority if spread allows; otherwise join best ask.
                if best_ask - tick > best_bid:
                    competitive_price = best_ask - tick
                    book_mode = "improve"
                else:
                    competitive_price = best_ask
                    book_mode = "join"

                # Receive at least min_sell_price (selling below = under floor).
                min_sell_price = self._taker_result_price / (
                    Decimal("1") - effective_min - self._tx_cost_pct
                )
                # Receive at most max_sell_price (above = profitability >
                # max_profitability → instant refresh). Cap is required to keep
                # the order alive past the next tick of the monitoring loop.
                max_sell_price = self._taker_result_price / (
                    Decimal("1") - self.config.max_profitability - self._tx_cost_pct
                )
                # Local book is too low relative to taker — any maker SELL at
                # or above best_bid would price above max_sell_price (instantly
                # refreshable). Skip placement until the book moves.
                if max_sell_price <= best_bid:
                    self._log_throttled_skip(
                        f"Skipping SELL placement: maker book too cheap "
                        f"(best_bid={best_bid} >= max_sell_price={max_sell_price:.2f})."
                    )
                    return
                price = max(competitive_price, min_sell_price)
                if price > max_sell_price:
                    price = max_sell_price
                    book_mode = "clip_max"
                bound_price = min_sell_price

                # Round UP — never undersell into min_profitability territory.
                price = (price / tick).to_integral_value(rounding=ROUND_UP) * tick
                # clip_max guard: ROUND_UP may push price above max_sell_price,
                # making profitability > max_profitability → instant cancel.
                # Nudge down by one tick to stay inside the cancel band.
                if price > max_sell_price:
                    price -= tick

                # Post-rounding safety: ensure we still don't cross the bid.
                if price <= best_bid:
                    price += tick

        except Exception as e:
            price = self._maker_target_price
            used_fallback = True
            book_mode = "fallback"
            self.logger().warning(
                f"Book-aware pricing failed ({e}), falling back to target price."
            )

        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=self.maker_order_side,
            amount=self.config.order_amount,
            price=price,
        )
        self.maker_order = TrackedOrder(order_id=order_id)
        self.logger().info(
            f"Created maker order (LIMIT_MAKER) {order_id} "
            f"side={self.maker_order_side.name} price={price} mode={book_mode} "
            f"lead={lead_mode} lead_bps={lead_bps} "
            f"target={self._maker_target_price} bound={bound_price} fallback={used_fallback}"
        )
