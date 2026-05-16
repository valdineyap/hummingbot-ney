import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, Optional

from hummingbot.connector.connector_base import ConnectorBase, Union
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class XEMMExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @staticmethod
    def _are_tokens_interchangeable(first_token: str, second_token: str):
        interchangeable_tokens = [
            {"WETH", "ETH"},
            {"WBTC", "BTC"},
            {"WBNB", "BNB"},
            {"WPOL", "POL"},
            {"WAVAX", "AVAX"},
            {"WONE", "ONE"},
            {"USDC", "USDC.E"},
            {"WBTC", "BTC"},
            {"USOL", "SOL"},
            {"UETH", "ETH"},
            {"UBTC", "BTC"}
        ]
        same_token_condition = first_token == second_token
        tokens_interchangeable_condition = any(({first_token, second_token} <= interchangeable_pair
                                                for interchangeable_pair
                                                in interchangeable_tokens))
        # for now, we will consider all the stablecoins interchangeable
        stable_coins_condition = "USD" in first_token and "USD" in second_token
        return same_token_condition or tokens_interchangeable_condition or stable_coins_condition

    def is_arbitrage_valid(self, pair1, pair2):
        base_asset1, _ = split_hb_trading_pair(pair1)
        base_asset2, _ = split_hb_trading_pair(pair2)
        return self._are_tokens_interchangeable(base_asset1, base_asset2)

    def __init__(self, strategy: StrategyV2Base, config: XEMMExecutorConfig, update_interval: float = 1.0,
                 max_retries: int = 10):
        if not self.is_arbitrage_valid(pair1=config.buying_market.trading_pair,
                                       pair2=config.selling_market.trading_pair):
            raise Exception("XEMM is not valid since the trading pairs are not interchangeable.")
        self.config = config
        self.rate_oracle = RateOracle.get_instance()
        if config.maker_side == TradeType.BUY:
            self.maker_connector = config.buying_market.connector_name
            self.maker_trading_pair = config.buying_market.trading_pair
            self.maker_order_side = TradeType.BUY
            self.taker_connector = config.selling_market.connector_name
            self.taker_trading_pair = config.selling_market.trading_pair
            self.taker_order_side = TradeType.SELL
        else:
            self.maker_connector = config.selling_market.connector_name
            self.maker_trading_pair = config.selling_market.trading_pair
            self.maker_order_side = TradeType.SELL
            self.taker_connector = config.buying_market.connector_name
            self.taker_trading_pair = config.buying_market.trading_pair
            self.taker_order_side = TradeType.BUY

        # Set up quote conversion pair
        _, maker_quote = split_hb_trading_pair(self.maker_trading_pair)
        _, taker_quote = split_hb_trading_pair(self.taker_trading_pair)
        self.quote_conversion_pair = f"{taker_quote}-{maker_quote}"

        taker_connector = strategy.connectors[self.taker_connector]
        if not self.is_amm_connector(exchange=self.taker_connector):
            if OrderType.MARKET not in taker_connector.supported_order_types():
                raise ValueError(f"{self.taker_connector} does not support market orders.")
        self._taker_result_price = Decimal("1")
        self._maker_target_price = Decimal("1")
        self._tx_cost = Decimal("1")
        self._tx_cost_pct = Decimal("1")
        self._current_trade_profitability = Decimal("0")
        self.maker_order = None
        self.taker_order = None
        self.failed_orders = []
        # Tracks whether a cancel has already been issued for the current
        # maker_order. While True, control_update_maker_order skips further
        # profitability checks (no double-cancel) and control_maker_order
        # waits for is_done before clearing maker_order and creating a new one.
        # This prevents the race where the executor used to cancel + null-out
        # the maker_order in the same tick, then immediately place a new one
        # before the cancel was confirmed — leaving multiple orders open
        # simultaneously on the exchange.
        self._cancel_requested = False
        # Timestamp (time.time()) when ``_cancel_requested`` flipped to True.
        # Used by ``control_maker_order`` to detect a stale cancel — when the
        # connector + framework retries have collectively been unable to
        # confirm cancellation for too long, we surface a CRITICAL log so
        # monitoring can flag the situation. We deliberately do NOT
        # auto-clear ``maker_order`` here: that would risk placing a second
        # same-side order while the original is still alive on exchange. The
        # orphan_check (controller-side) is the safety net for that case.
        self._cancel_requested_ts: float = 0.0
        # If a cancel stays unconfirmed for longer than this, take action.
        # 5s sits just above the connector's internal retry budget
        # (≤ 1.5s PENDING_CREATE wait + 3 × 0.5s backoff ≈ 3s), so by the
        # time we reach 5s the connector has already exhausted its retries
        # and the only remaining cause for the cancel still being pending
        # is either (a) the framework dropped the cancel intent, or (b) the
        # underlying ``_strategy.cancel`` is sitting in a dedup bucket
        # waiting for a state transition that never came. Either way the
        # right move is to re-issue ``_strategy.cancel`` to force a fresh
        # ``_place_cancel`` round-trip.
        self._cancel_stale_warn_after_sec: float = 5.0
        # Last time we re-issued the cancel — used to throttle so we don't
        # hammer the connector with the same cancel every tick. 5s window
        # mirrors the connector's full retry budget so each re-issue gets
        # a full set of attempts before we try again.
        self._last_stale_cancel_log_ts: float = 0.0
        # ----- Fill-to-hedge latency (Task 3.4) -----
        # When the maker-side order (partial or full) emits its first fill,
        # we stamp ``_first_fill_ts``. When ``place_taker_order`` runs, we
        # compute and log the latency. Surfaced as ``fill_to_hedge_latency_ms``
        # in custom_info so the monitor digest can chart it. Stamped once per
        # executor lifecycle — additional fills on the same order don't reset
        # it (the first fill is what actually exposed us).
        self._first_fill_ts: float = 0.0
        self._hedge_placed_ts: float = 0.0
        self._fill_to_hedge_latency_ms: Optional[int] = None
        # Task 4.3: snapshot the taker_result_price at the moment we placed
        # the hedge — the price the executor *expected* the taker MARKET to
        # fill at. After settlement we compare against the actual taker
        # fill price to compute slippage_bps for the trade ledger.
        self._taker_expected_price: Optional[Decimal] = None
        super().__init__(strategy=strategy,
                         connectors=[config.buying_market.connector_name, config.selling_market.connector_name],
                         config=config, update_interval=update_interval, max_retries=max_retries)

    async def validate_sufficient_balance(self):
        mid_price = self.get_price(self.maker_connector, self.maker_trading_pair,
                                   price_type=PriceType.MidPrice)
        maker_order_candidate = OrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=self.maker_order_side,
            amount=self.config.order_amount,
            price=mid_price,)
        taker_order_candidate = OrderCandidate(
            trading_pair=self.taker_trading_pair,
            is_maker=False,
            order_type=OrderType.MARKET,
            order_side=self.taker_order_side,
            amount=self.config.order_amount,
            price=mid_price,)
        maker_adjusted_candidate = self.adjust_order_candidates(self.maker_connector, [maker_order_candidate])[0]
        taker_adjusted_candidate = self.adjust_order_candidates(self.taker_connector, [taker_order_candidate])[0]
        if maker_adjusted_candidate.amount == Decimal("0") or taker_adjusted_candidate.amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough budget to open position.")
            self.stop()

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            await self.update_prices_and_tx_costs()
            await self.control_maker_order()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()

    async def control_maker_order(self):
        # State machine for the single maker order:
        #   None              → place a new one
        #   tracked & is_done → previous cancel/fill confirmed; clear and
        #                       let next tick place a new one
        #   tracked & live    → run profitability checks (may issue cancel)
        #   tracked & cancel-already-requested but not yet done → wait
        #
        # The wait branch is critical: previously this method called
        # create_maker_order() the moment maker_order was None, but the
        # cancel path used to set maker_order=None synchronously *before*
        # the cancel had been confirmed by the exchange. That caused the
        # executor to stack multiple live orders on the maker side while the
        # cancel was still in flight. Now we keep the reference until the
        # cancel actually clears (is_done == True).
        if self.maker_order is None:
            self._cancel_requested = False
            self._cancel_requested_ts = 0.0
            self._last_stale_cancel_log_ts = 0.0
            await self.create_maker_order()
        elif self.maker_order.is_done:
            # Cancel or fill confirmed by the exchange. Clear the slot — the
            # next tick will create a new order (cancel case) or the executor
            # will already be in SHUTTING_DOWN status (fill case, handled by
            # control_task before reaching here).
            self.maker_order = None
            self._cancel_requested = False
            self._cancel_requested_ts = 0.0
            self._last_stale_cancel_log_ts = 0.0
        elif self._cancel_requested:
            # Cancel already issued; we're waiting for the exchange to
            # confirm. The connector's ``_place_cancel`` does its own retry
            # with backoff (Task 2.1) — 3 attempts × 0.5s ≈ 3s — but if
            # those exhaust without confirmation we have no recovery: the
            # original ``self._strategy.cancel`` call returned False inside
            # the framework, the order tracker did not transition state,
            # and nothing else re-pokes the cancel. Production logs (Sprint 5
            # analysis) showed cases where an order sat alive for 3min46s
            # after a single failed cancel, eventually filling unhedged.
            #
            # Once age ≥ ``_cancel_stale_warn_after_sec`` (= 5s — connector
            # retries had time to land) we re-issue ``_strategy.cancel``.
            # Re-issuance is throttled to once per stale-window so each
            # attempt gets a full connector retry budget before the next.
            if (self._cancel_requested_ts > 0
                    and self.maker_order is not None
                    and self.maker_order.order is not None):
                age = time.time() - self._cancel_requested_ts
                window = self._cancel_stale_warn_after_sec
                if (age >= window
                        and (time.time() - self._last_stale_cancel_log_ts) >= window):
                    self._last_stale_cancel_log_ts = time.time()
                    eoid = self.maker_order.order.exchange_order_id
                    self.logger().warning(
                        f"[cancel_stale] Cancel request for maker_order "
                        f"{self.maker_order.order_id} "
                        f"(exchange_order_id={eoid}) has been pending for "
                        f"{age:.1f}s — re-issuing _strategy.cancel."
                    )
                    # Re-issue. The framework's tracker dedup keys on state
                    # transitions, not on call count: if the prior cancel
                    # already returned False, the order is back in OPEN (not
                    # PENDING_CANCEL), so this call lands as a fresh cancel.
                    try:
                        self._strategy.cancel(
                            self.maker_connector,
                            self.maker_trading_pair,
                            self.maker_order.order_id,
                        )
                    except Exception as e:
                        self.logger().error(
                            f"[cancel_stale] re-issue raised "
                            f"{type(e).__name__}: {e} — leaving for orphan_check"
                        )
            return
        else:
            await self.control_update_maker_order()

    async def update_prices_and_tx_costs(self):
        self._taker_result_price = await self.get_resulting_price_for_amount(
            connector=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            is_buy=self.taker_order_side == TradeType.BUY,
            order_amount=self.config.order_amount)
        await self.update_tx_costs()
        if self.taker_order_side == TradeType.BUY:
            # Maker is SELL: profitability = (maker_price - taker_price) / maker_price
            # To achieve target: maker_price = taker_price / (1 - target_profitability - tx_cost_pct)
            self._maker_target_price = self._taker_result_price / (Decimal("1") - self.config.target_profitability - self._tx_cost_pct)
        else:
            # Maker is BUY: profitability = (taker_price - maker_price) / maker_price
            # To achieve target: maker_price = taker_price / (1 + target_profitability + tx_cost_pct)
            self._maker_target_price = self._taker_result_price / (Decimal("1") + self.config.target_profitability + self._tx_cost_pct)

    async def update_tx_costs(self):
        base, quote = split_hb_trading_pair(trading_pair=self.config.buying_market.trading_pair)
        base_without_wrapped = base[1:] if base.startswith("W") else base
        taker_fee_task = asyncio.create_task(self.get_tx_cost_in_asset(
            exchange=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=OrderType.MARKET,
            is_buy=self.taker_order_side == TradeType.BUY,
            order_amount=self.config.order_amount,
            asset=base_without_wrapped
        ))
        maker_fee_task = asyncio.create_task(self.get_tx_cost_in_asset(
            exchange=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT,
            is_buy=self.maker_order_side == TradeType.BUY,
            order_amount=self.config.order_amount,
            asset=base_without_wrapped
        ))

        taker_fee, maker_fee = await asyncio.gather(taker_fee_task, maker_fee_task)
        self._tx_cost = taker_fee + maker_fee
        self._tx_cost_pct = self._tx_cost / self.config.order_amount

    async def get_tx_cost_in_asset(self, exchange: str, trading_pair: str, is_buy: bool, order_amount: Decimal,
                                   asset: str, order_type: OrderType = OrderType.MARKET):
        connector = self.connectors[exchange]
        if self.is_amm_connector(exchange=exchange):
            gas_cost = connector.network_transaction_fee
            conversion_price = RateOracle.get_instance().get_pair_rate(f"{asset}-{gas_cost.token}")
            if conversion_price is None:
                self.logger().warning(f"Could not get conversion rate for {asset}-{gas_cost.token}")
                return Decimal("0")
            return gas_cost.amount / conversion_price
        else:
            fee = connector.get_fee(
                base_currency=asset,
                quote_currency=trading_pair.split("-")[1],
                order_type=order_type,
                order_side=TradeType.BUY if is_buy else TradeType.SELL,
                amount=order_amount,
                price=self._taker_result_price,
                is_maker=order_type.is_limit_type(),
            )
            return fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=self._taker_result_price,
                order_amount=order_amount,
                token=asset,
            )

    async def get_resulting_price_for_amount(self, connector: str, trading_pair: str, is_buy: bool,
                                             order_amount: Decimal):
        return await self.connectors[connector].get_quote_price(trading_pair, is_buy, order_amount)

    async def create_maker_order(self):
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT,
            side=self.maker_order_side,
            amount=self.config.order_amount,
            price=self._maker_target_price)
        self.maker_order = TrackedOrder(order_id=order_id)
        self.logger().info(f"Created maker order {order_id} at price {self._maker_target_price}.")

    async def control_shutdown_process(self):
        if self.maker_order.is_done and self.taker_order.is_done:
            self.logger().info("Both orders are done, executor terminated.")
            self.stop()

    async def control_update_maker_order(self):
        await self.update_current_trade_profitability()
        net_profitability = self._current_trade_profitability - self._tx_cost_pct
        if net_profitability < self.config.min_profitability:
            self.logger().info(
                f"Order {self.maker_order.order_id} profitability "
                f"{net_profitability * Decimal('10000'):.2f} bps "
                f"< min {self.config.min_profitability * Decimal('10000'):.2f} bps. "
                f"Cancelling order."
            )
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, self.maker_order.order_id)
            # Mark cancel-in-flight; do NOT clear maker_order. control_maker_order
            # will hold off on creating a new order until is_done flips True.
            self._cancel_requested = True
            self._cancel_requested_ts = time.time()
        elif net_profitability > self.config.max_profitability:
            self.logger().info(
                f"Order {self.maker_order.order_id} profitability "
                f"{net_profitability * Decimal('10000'):.2f} bps "
                f"> max {self.config.max_profitability * Decimal('10000'):.2f} bps. "
                f"Cancelling order."
            )
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, self.maker_order.order_id)
            self._cancel_requested = True
            self._cancel_requested_ts = time.time()

    async def update_current_trade_profitability(self):
        trade_profitability = Decimal("0")
        if self.maker_order and self.maker_order.order and self.maker_order.order.is_open:
            maker_price = self.maker_order.order.price
            # Get the conversion rate to normalize prices to the same quote asset
            try:
                conversion_rate = await self.get_quote_asset_conversion_rate()
                if self.maker_order_side == TradeType.BUY:
                    # If maker is buying, normalize taker (sell) price to maker quote asset
                    normalized_taker_price = self._taker_result_price * conversion_rate
                    trade_profitability = (normalized_taker_price - maker_price) / maker_price
                else:
                    # If maker is selling, normalize taker (buy) price to maker quote asset
                    normalized_taker_price = self._taker_result_price * conversion_rate
                    trade_profitability = (maker_price - normalized_taker_price) / maker_price
            except Exception as e:
                self.logger().error(f"Error calculating trade profitability: {e}")
                return Decimal("0")
        self._current_trade_profitability = trade_profitability
        return trade_profitability

    def process_order_created_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order {event.order_id} created.")
            self.maker_order.order = self.get_in_flight_order(self.maker_connector, event.order_id)
        elif self.taker_order and event.order_id == self.taker_order.order_id:
            self.logger().info(f"Taker order {event.order_id} created.")
            self.taker_order.order = self.get_in_flight_order(self.taker_connector, event.order_id)

    def process_order_filled_event(self,
                                   event_tag: int,
                                   market: ConnectorBase,
                                   event):
        """Stamp the first fill timestamp on the maker side (Task 3.4).

        ``fill_to_hedge_latency_ms`` is the wall-clock time between this
        event and the moment ``place_taker_order`` actually issues the
        taker hedge. The metric drives the digest's ``fill_to_hedge_p99``
        which is how we'll spot regressions in Task 2.2 / 2.3 / 3.3.
        """
        if self.maker_order and event.order_id == self.maker_order.order_id:
            if self._first_fill_ts == 0.0:
                self._first_fill_ts = time.time()

    def process_order_completed_event(self,
                                      event_tag: int,
                                      market: ConnectorBase,
                                      event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order {event.order_id} completed. Executing taker order.")
            self.place_taker_order()
            self._status = RunnableStatus.SHUTTING_DOWN

    def process_order_canceled_event(self,
                                     event_tag: int,
                                     market: ConnectorBase,
                                     event):
        """Handle the cancel-with-partial-fill case (Task 3.3).

        When BitPreco partially fills a maker order and then we cancel it
        (because profitability moved out of band, or the executor decided
        to refresh), the framework fires:

          1. ``OrderFilledEvent`` with the partial executed amount
          2. ``OrderCancelledEvent`` for the remainder

        Without this override, neither event triggers a taker hedge:
        ``process_order_completed_event`` only fires for *full* fills, so
        the partial inventory stays on BitPreco and the slow inventory_audit
        path eventually corrects it via a MARKET dump (with slippage).

        Here we detect cancel-with-partial-fill and immediately place a
        taker MARKET for the executed amount. We guard against:

          * double-hedging (if a completed event already fired for this
            order, ``self.taker_order`` is set — abort);
          * tiny residuals (some venues reject orders below a min size — we
            log and let inventory_audit pick those up);
          * ordering: the cancelled event can race the filled event. If
            ``executed_amount_base == 0`` at cancel time we just clean up.
        """
        if not (self.maker_order and event.order_id == self.maker_order.order_id):
            return  # not our maker order — nothing to do

        if self.taker_order is not None:
            # Already hedged via the completed-event path or a prior cancel
            # event (defensive — the framework can fire cancel after fill).
            return

        # ``executed_amount_base`` is updated by the connector's
        # client_order_tracker as TradeUpdates flow in. In the partial-fill-
        # then-cancel race, the OrderFilledEvent that produces the
        # exec_amount usually lands a few ms before OrderCancelledEvent.
        executed = Decimal("0")
        if self.maker_order.order is not None:
            executed = self.maker_order.order.executed_amount_base or Decimal("0")

        if executed <= 0:
            self.logger().info(
                f"Maker order {event.order_id} cancelled with no fills — "
                f"no hedge needed."
            )
            return

        self.logger().info(
            f"Maker order {event.order_id} cancelled after partial fill of "
            f"{executed} base. Placing taker hedge for the executed amount."
        )
        try:
            self.place_taker_order(amount=executed)
        except Exception as e:
            # Don't let a hedge placement failure crash the executor —
            # inventory_audit is the safety net. Log loudly so we can see
            # this in the digest's recent ERRORs.
            self.logger().error(
                f"[partial_hedge] Failed to place taker hedge for "
                f"{executed} base on order {event.order_id}: "
                f"{type(e).__name__}: {e}. inventory_audit will reconcile."
            )
            return
        self._status = RunnableStatus.SHUTTING_DOWN

    def place_taker_order(self, amount: Optional[Decimal] = None):
        """Place the taker MARKET hedge.

        :param amount: The base-asset amount to hedge. Defaults to the
            executor's configured ``order_amount`` (full XEMM cycle); pass
            an explicit value when hedging a partial fill so we don't
            over-hedge.
        """
        order_amount = amount if amount is not None else self.config.order_amount
        taker_order_id = self.place_order(
            connector_name=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=OrderType.MARKET,
            side=self.taker_order_side,
            amount=order_amount)
        self.taker_order = TrackedOrder(order_id=taker_order_id)
        # Task 3.4: capture fill-to-hedge latency (only stamp once).
        if self._hedge_placed_ts == 0.0:
            self._hedge_placed_ts = time.time()
            if self._first_fill_ts > 0.0:
                self._fill_to_hedge_latency_ms = int(
                    (self._hedge_placed_ts - self._first_fill_ts) * 1000
                )
                self.logger().info(
                    f"[fill_to_hedge] maker_order={self.maker_order.order_id if self.maker_order else 'n/a'} "
                    f"first_fill_ts={self._first_fill_ts:.3f} hedge_ts={self._hedge_placed_ts:.3f} "
                    f"latency_ms={self._fill_to_hedge_latency_ms}"
                )
        # Task 4.3: snapshot the expected taker price for later slippage calc.
        if self._taker_expected_price is None and self._taker_result_price > 0:
            self._taker_expected_price = self._taker_result_price

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if self.maker_order and self.maker_order.order_id == event.order_id:
            self.failed_orders.append(self.maker_order)
            self.maker_order = None
            self._current_retries += 1
        elif self.taker_order and self.taker_order.order_id == event.order_id:
            self.failed_orders.append(self.taker_order)
            self._current_retries += 1
            self.place_taker_order()

    def get_custom_info(self) -> Dict:
        # Since we can't make this method async, we'll skip the profitability calculation
        # The profitability will still be shown in the status message which is async
        return {
            "side": self.config.maker_side,
            "maker_connector": self.maker_connector,
            "maker_trading_pair": self.maker_trading_pair,
            "taker_connector": self.taker_connector,
            "taker_trading_pair": self.taker_trading_pair,
            "min_profitability": self.config.min_profitability,
            "target_profitability_pct": self.config.target_profitability,
            "max_profitability": self.config.max_profitability,
            "trade_profitability": self._current_trade_profitability,
            "tx_cost": self._tx_cost,
            "tx_cost_pct": self._tx_cost_pct,
            "taker_price": self._taker_result_price,
            "maker_target_price": self._maker_target_price,
            "net_profitability": self._current_trade_profitability - self._tx_cost_pct,
            "order_amount": self.config.order_amount,
            # Task 3.4: surface fill→hedge latency so the trade ledger can
            # persist it (and the digest can compute p99 across sessions).
            "fill_to_hedge_latency_ms": self._fill_to_hedge_latency_ms,
            # Task 4.3: expected taker price at hedge placement — used by
            # the trade ledger to compute slippage_bps post-settlement.
            "taker_expected_price": self._taker_expected_price,
        }

    def early_stop(self, keep_position: bool = False):
        # Cancel maker order whenever we have an order_id, regardless of
        # whether the BuyOrderCreated/SellOrderCreated event has been
        # observed yet. The previous gate (``self.maker_order.order`` set
        # AND ``is_open``) missed the race window between ``place_order``
        # returning the client_order_id synchronously and the Created event
        # firing after the REST POST returns (~50–100 ms on BitPreco). A
        # cancel-gate (LEAD_SIGNAL_STRONG / EVENT_LOOP_LAG / barrier) that
        # fired in this window would skip the cancel and leave the maker
        # alive on the exchange book — see 2026-05-16 07:13:21Z incident:
        # gate fired 58 ms after place, order remained on book and filled
        # 30 s later as an orphan, rebalance panic-unwound via MARKET on
        # the same exchange at adverse price (loss = R$0.089 per orphan,
        # 5 occurrences across 2 sessions).
        #
        # ``ExchangePyBase._execute_cancel`` awaits ``get_exchange_order_id()``
        # before issuing the cancel REST, so calling cancel before the
        # place REST has returned is safe — the cancel queues until the
        # exchange_order_id is known. This mirrors the unconditional cancel
        # pattern already used in ``control_update_maker_order``.
        if self.maker_order and self.maker_order.order_id:
            already_done = (
                self.maker_order.order is not None
                and self.maker_order.order.is_done
            )
            if not already_done:
                state = "pending_create" if self.maker_order.order is None else "tracked"
                self.logger().info(
                    f"Cancelling maker order {self.maker_order.order_id} (state={state})."
                )
                self._strategy.cancel(
                    self.maker_connector, self.maker_trading_pair, self.maker_order.order_id
                )
        self.close_type = CloseType.POSITION_HOLD if keep_position else CloseType.EARLY_STOP
        self.stop()

    def get_cum_fees_quote(self) -> Decimal:
        if self.is_closed and self.maker_order and self.taker_order:
            return self.maker_order.cum_fees_quote + self.taker_order.cum_fees_quote
        else:
            return Decimal("0")

    def get_net_pnl_quote(self) -> Decimal:
        if self.is_closed and self.maker_order and self.taker_order and self.maker_order.is_done and self.taker_order.is_done:
            maker_pnl = self.maker_order.executed_amount_base * self.maker_order.average_executed_price
            taker_pnl = self.taker_order.executed_amount_base * self.taker_order.average_executed_price
            return taker_pnl - maker_pnl - self.get_cum_fees_quote()
        else:
            return Decimal("0")

    def get_net_pnl_pct(self) -> Decimal:
        pnl_quote = self.get_net_pnl_quote()
        return pnl_quote / self.config.order_amount

    async def get_quote_asset_conversion_rate(self) -> Decimal:
        """
        Fetch the conversion rate between the quote assets of the buying and selling markets.
        Example: For M3M3/USDT and M3M3/USDC, fetch the USDC/USDT rate.
        """
        try:
            conversion_rate = self.rate_oracle.get_pair_rate(self.quote_conversion_pair)
            if conversion_rate is None:
                self.logger().error(f"Could not fetch conversion rate for {self.quote_conversion_pair}")
                raise ValueError(f"Could not fetch conversion rate for {self.quote_conversion_pair}")
            return conversion_rate
        except Exception as e:
            self.logger().error(f"Error fetching conversion rate for {self.quote_conversion_pair}: {e}")
            raise

    def to_format_status(self):
        return f"""
Maker Side: {self.maker_order_side}
-----------------------------------------------------------------------------------------------------------------------
    - Maker: {self.maker_connector} {self.maker_trading_pair} | Taker: {self.taker_connector} {self.taker_trading_pair}
    - Min profitability: {self.config.min_profitability * 100:.2f}% | Target profitability: {self.config.target_profitability * 100:.2f}% | Max profitability: {self.config.max_profitability * 100:.2f}% | Current profitability: {(self._current_trade_profitability - self._tx_cost_pct) * 100:.2f}%
    - Trade profitability: {self._current_trade_profitability * 100:.2f}% | Tx cost: {self._tx_cost_pct * 100:.2f}%
    - Taker result price: {self._taker_result_price:.3f} | Tx cost: {self._tx_cost:.3f} {self.maker_trading_pair.split('-')[-1]} | Order amount (Base): {self.config.order_amount:.2f}
-----------------------------------------------------------------------------------------------------------------------
"""
