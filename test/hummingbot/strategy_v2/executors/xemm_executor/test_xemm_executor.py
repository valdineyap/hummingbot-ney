from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import MagicMock, Mock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent, BuyOrderCreatedEvent, MarketOrderFailureEvent, OrderCancelledEvent,
)
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class TestXEMMExecutor(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self):
        super().setUp()
        self.strategy = self.create_mock_strategy()
        self.xemm_base_config = self.base_config_long
        self.update_interval = 0.5
        self.executor = XEMMExecutor(self.strategy, self.xemm_base_config, self.update_interval)
        self.set_loggers(loggers=[self.executor.logger()])

    @property
    def base_config_long(self) -> XEMMExecutorConfig:
        return XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name='binance', trading_pair='ETH-USDT'),
            selling_market=ConnectorPair(connector_name='kucoin', trading_pair='ETH-USDT'),
            maker_side=TradeType.BUY,
            order_amount=Decimal('100'),
            min_profitability=Decimal('0.01'),
            target_profitability=Decimal('0.015'),
            max_profitability=Decimal('0.02'),
        )

    @property
    def base_config_short(self) -> XEMMExecutorConfig:
        return XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name='binance', trading_pair='ETH-USDT'),
            selling_market=ConnectorPair(connector_name='kucoin', trading_pair='ETH-USDT'),
            maker_side=TradeType.SELL,
            order_amount=Decimal('100'),
            min_profitability=Decimal('0.01'),
            target_profitability=Decimal('0.015'),
            max_profitability=Decimal('0.02'),
        )

    @staticmethod
    def create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="ETH-USDT")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        binance_connector = MagicMock(spec=ExchangePyBase)
        binance_connector.supported_order_types = MagicMock(return_value=[OrderType.LIMIT, OrderType.MARKET])
        kucoin_connector = MagicMock(spec=ExchangePyBase)
        kucoin_connector.supported_order_types = MagicMock(return_value=[OrderType.LIMIT, OrderType.MARKET])
        strategy.connectors = {
            "binance": binance_connector,
            "kucoin": kucoin_connector,
        }
        return strategy

    def test_is_arbitrage_valid(self):
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'ETH-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-BUSD', 'ETH-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'WETH-USDT'))
        self.assertFalse(self.executor.is_arbitrage_valid('ETH-USDT', 'BTC-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'ETH-BTC'))

    def test_net_pnl_long(self):
        self.executor._status = RunnableStatus.TERMINATED
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.taker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.executed_amount_base = Decimal('1')
        self.executor.taker_order.executed_amount_base = Decimal('1')
        self.executor.maker_order.average_executed_price = Decimal('100')
        self.executor.taker_order.average_executed_price = Decimal('200')
        self.executor.maker_order.cum_fees_quote = Decimal('1')
        self.executor.taker_order.cum_fees_quote = Decimal('1')
        self.assertEqual(self.executor.net_pnl_quote, Decimal('98'))
        self.assertEqual(self.executor.net_pnl_pct, Decimal('0.98'))

    def test_net_pnl_short(self):
        executor = XEMMExecutor(self.strategy, self.base_config_short, self.update_interval)
        executor._status = RunnableStatus.TERMINATED
        executor.maker_order = Mock(spec=TrackedOrder)
        executor.taker_order = Mock(spec=TrackedOrder)
        executor.maker_order.executed_amount_base = Decimal('1')
        executor.taker_order.executed_amount_base = Decimal('1')
        executor.maker_order.average_executed_price = Decimal('100')
        executor.taker_order.average_executed_price = Decimal('200')
        executor.maker_order.cum_fees_quote = Decimal('1')
        executor.taker_order.cum_fees_quote = Decimal('1')
        self.assertEqual(executor.net_pnl_quote, Decimal('98'))
        self.assertEqual(executor.net_pnl_pct, Decimal('0.98'))

    @patch.object(XEMMExecutor, 'get_trading_rules')
    @patch.object(XEMMExecutor, 'adjust_order_candidates')
    async def test_validate_sufficient_balance(self, mock_adjust_order_candidates, mock_get_trading_rules):
        # Mock trading rules
        trading_rules = TradingRule(trading_pair="ETH-USDT", min_order_size=Decimal("0.1"),
                                    min_price_increment=Decimal("0.1"), min_base_amount_increment=Decimal("0.1"))
        mock_get_trading_rules.return_value = trading_rules
        order_candidate = OrderCandidate(
            trading_pair="ETH-USDT",
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100")
        )
        # Test for sufficient balance
        mock_adjust_order_candidates.return_value = [order_candidate]
        await self.executor.validate_sufficient_balance()
        self.assertNotEqual(self.executor.close_type, CloseType.INSUFFICIENT_BALANCE)

        # Test for insufficient balance
        order_candidate.amount = Decimal("0")
        mock_adjust_order_candidates.return_value = [order_candidate]
        await self.executor.validate_sufficient_balance()
        self.assertEqual(self.executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        self.assertEqual(self.executor.status, RunnableStatus.TERMINATED)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_running_order_not_placed(self, tx_cost_mock, resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("100")
        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        # Calculate expected maker target price using the new formula:
        # maker_price = taker_price / (1 + target_profitability + tx_cost_pct)
        # tx_cost_pct = (0.01 + 0.01) / 100 = 0.0002
        # maker_price = 100 / (1 + 0.015 + 0.0002) = 100 / 1.0152
        expected_price = Decimal("100") / (Decimal("1") + Decimal("0.015") + Decimal("0.02") / Decimal("100"))
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)
        self.assertEqual(self.executor.maker_order.order_id, "OID-BUY-1")
        self.assertEqual(self.executor._maker_target_price, expected_price)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_running_order_not_placed_sell_side(self, tx_cost_mock, resulting_price_mock):
        # Test maker SELL side (taker BUY) to cover line 155
        executor = XEMMExecutor(self.strategy, self.base_config_short, self.update_interval)
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("100")
        executor._status = RunnableStatus.RUNNING
        await executor.control_task()
        # Calculate expected maker target price using the new formula for SELL side:
        # maker_price = taker_price / (1 - target_profitability - tx_cost_pct)
        # tx_cost_pct = (0.01 + 0.01) / 100 = 0.0002
        # maker_price = 100 / (1 - 0.015 - 0.0002) = 100 / 0.9848
        expected_price = Decimal("100") / (Decimal("1") - Decimal("0.015") - Decimal("0.02") / Decimal("100"))
        self.assertEqual(executor._status, RunnableStatus.RUNNING)
        self.assertEqual(executor.maker_order.order_id, "OID-SELL-1")
        self.assertEqual(executor._maker_target_price, expected_price)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_running_order_placed_refresh_condition_min_profitability(self, tx_cost_mock,
                                                                                         resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("100")
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.order_id = "OID-BUY-1"
        self.executor.maker_order.order = InFlightOrder(
            creation_timestamp=1234,
            trading_pair="ETH-USDT",
            client_order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("100"),
            price=Decimal("99.5"),
            initial_state=OrderState.OPEN,
        )
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)
        self.assertEqual(self.executor.maker_order, None)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_running_order_placed_refresh_condition_max_profitability(self, tx_cost_mock,
                                                                                         resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("103")
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.order_id = "OID-BUY-1"
        self.executor.maker_order.order = InFlightOrder(
            creation_timestamp=1234,
            trading_pair="ETH-USDT",
            client_order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("100"),
            price=Decimal("99.5"),
            initial_state=OrderState.OPEN,
        )
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)
        self.assertEqual(self.executor.maker_order, None)

    async def test_control_task_shut_down_process(self):
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.is_done = True
        self.executor.taker_order = Mock(spec=TrackedOrder)
        self.executor.taker_order.is_done = True
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    async def test_cancel_stale_reissues_after_window(self):
        """When a cancel sits unconfirmed past
        ``_cancel_stale_warn_after_sec`` (5s), the executor re-issues
        ``_strategy.cancel`` (Sprint 5 / Bug D). Without this, production
        logs showed orders sitting alive 3min46s after a single failed
        cancel — eventually filling unhedged."""
        import time as _t
        # Set up: a maker order is in flight, cancel was requested 6s ago.
        self.executor._status = RunnableStatus.RUNNING
        in_flight = MagicMock()
        in_flight.is_done = False
        in_flight.exchange_order_id = "EX-12345"
        order = TrackedOrder(order_id="OID-BUY-1")
        order.order = in_flight
        self.executor.maker_order = order
        self.executor._cancel_requested = True
        self.executor._cancel_requested_ts = _t.time() - 6.0  # 6s ago
        self.executor._last_stale_cancel_log_ts = 0.0
        self.strategy.cancel.reset_mock()

        await self.executor.control_maker_order()

        # Re-issue happened.
        self.strategy.cancel.assert_called_once()
        args = self.strategy.cancel.call_args.args
        self.assertEqual(args[2], "OID-BUY-1")  # order_id

    async def test_cancel_stale_does_not_reissue_within_window(self):
        """If the cancel was requested only 2s ago (< 5s window), no
        re-issue — the connector's internal retry is still running."""
        import time as _t
        self.executor._status = RunnableStatus.RUNNING
        in_flight = MagicMock()
        in_flight.is_done = False
        in_flight.exchange_order_id = "EX-12345"
        order = TrackedOrder(order_id="OID-BUY-1")
        order.order = in_flight
        self.executor.maker_order = order
        self.executor._cancel_requested = True
        self.executor._cancel_requested_ts = _t.time() - 2.0  # 2s ago
        self.strategy.cancel.reset_mock()

        await self.executor.control_maker_order()

        # No re-issue — connector's retry budget hasn't been exhausted yet.
        self.strategy.cancel.assert_not_called()

    async def test_cancel_stale_throttles_repeated_reissues(self):
        """Re-issue is throttled to once per stale window (5s) so we don't
        spam the connector with same cancel every tick."""
        import time as _t
        self.executor._status = RunnableStatus.RUNNING
        in_flight = MagicMock()
        in_flight.is_done = False
        in_flight.exchange_order_id = "EX-12345"
        order = TrackedOrder(order_id="OID-BUY-1")
        order.order = in_flight
        self.executor.maker_order = order
        self.executor._cancel_requested = True
        self.executor._cancel_requested_ts = _t.time() - 6.0
        # Already re-issued once moments ago.
        self.executor._last_stale_cancel_log_ts = _t.time() - 1.0
        self.strategy.cancel.reset_mock()

        await self.executor.control_maker_order()

        # Throttled — only 1s passed since last re-issue, window is 5s.
        self.strategy.cancel.assert_not_called()

    @patch.object(XEMMExecutor, "get_in_flight_order")
    def test_process_order_created_event(self, in_flight_order_mock):
        self.executor._status = RunnableStatus.RUNNING
        in_flight_order_mock.side_effect = [
            InFlightOrder(
                client_order_id="OID-BUY-1",
                creation_timestamp=1234,
                trading_pair="ETH-USDT",
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY,
                amount=Decimal("100"),
                price=Decimal("100"),
            ),
            InFlightOrder(
                client_order_id="OID-SELL-1",
                creation_timestamp=1234,
                trading_pair="ETH-USDT",
                order_type=OrderType.MARKET,
                trade_type=TradeType.SELL,
                amount=Decimal("100"),
                price=Decimal("100"),
            )
        ]

        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        self.executor.taker_order = TrackedOrder(order_id="OID-SELL-1")
        buy_order_created_event = BuyOrderCreatedEvent(
            timestamp=1234,
            type=OrderType.LIMIT,
            creation_timestamp=1233,
            order_id="OID-BUY-1",
            trading_pair="ETH-USDT",
            amount=Decimal("100"),
            price=Decimal("100"),
        )
        sell_order_created_event = BuyOrderCreatedEvent(
            timestamp=1234,
            type=OrderType.MARKET,
            creation_timestamp=1233,
            order_id="OID-SELL-1",
            trading_pair="ETH-USDT",
            amount=Decimal("100"),
            price=Decimal("100"),
        )
        self.assertEqual(self.executor.maker_order.order, None)
        self.assertEqual(self.executor.taker_order.order, None)
        self.executor.process_order_created_event(1, MagicMock(), buy_order_created_event)
        self.assertEqual(self.executor.maker_order.order.client_order_id, "OID-BUY-1")
        self.executor.process_order_created_event(1, MagicMock(), sell_order_created_event)
        self.assertEqual(self.executor.taker_order.order.client_order_id, "OID-SELL-1")

    def test_process_order_completed_event(self):
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        self.assertEqual(self.executor.taker_order, None)
        buy_order_created_event = BuyOrderCompletedEvent(
            base_asset="ETH",
            quote_asset="USDT",
            base_asset_amount=Decimal("100"),
            quote_asset_amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            timestamp=1234,
            order_id="OID-BUY-1",
        )
        self.executor.process_order_completed_event(1, MagicMock(), buy_order_created_event)
        self.assertEqual(self.executor.status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(self.executor.taker_order.order_id, "OID-SELL-1")

    def test_process_order_canceled_with_partial_fill_places_taker(self):
        """Cancel-with-partial-fill must trigger a taker hedge for the
        executed_amount_base (Task 3.3). Without this, partial fills sit
        unhedged until inventory_audit dumps them via MARKET (slippage)."""
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        # Stub the maker_order's underlying InFlightOrder with a partial
        # executed_amount_base — this is what the connector's tracker has
        # propagated by the time the cancel event lands.
        in_flight = MagicMock()
        in_flight.executed_amount_base = Decimal("37.5")  # partial fill
        self.executor.maker_order.order = in_flight

        cancel_event = OrderCancelledEvent(timestamp=1234, order_id="OID-BUY-1",
                                           exchange_order_id="ex-OID-BUY-1")
        self.executor.process_order_canceled_event(1, MagicMock(), cancel_event)

        # Hedge must have been placed for the partial amount, not the
        # configured order_amount (100). place_order calls
        # strategy.sell(connector, pair, amount, order_type, price, pos),
        # so args[2] is the amount.
        self.assertIsNotNone(self.executor.taker_order)
        self.strategy.sell.assert_called_once()
        self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("37.5"))
        # Status moves to SHUTTING_DOWN so control_task switches to the
        # shutdown path (waiting for taker to settle).
        self.assertEqual(self.executor.status, RunnableStatus.SHUTTING_DOWN)

    def test_process_order_canceled_skips_hedge_for_sub_threshold_partial(self):
        """When ``executed × mid < min_hedge_value_quote``, the executor
        must NOT place a taker hedge — let inventory_audit absorb the
        residual as drift. Prevents the 2026-05-18 11:32 over-hedge bug
        chain: MIN_NOTIONAL reject → buggy retry → 2000x full-lot hedge.
        """
        self.executor._status = RunnableStatus.RUNNING
        # Inject min_hedge_value_quote on the config (default 60 BRL on
        # XEMMLeadLagExecutorConfig; base XEMMExecutorConfig is patched
        # via setattr for this test).
        self.executor.config.min_hedge_value_quote = Decimal("60")
        # Tiny partial fill: 0.001 base × mid 1.0 = 0.001 quote ≪ 60.
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        in_flight = MagicMock()
        in_flight.executed_amount_base = Decimal("0.001")
        self.executor.maker_order.order = in_flight

        with patch.object(self.executor, "get_price", return_value=Decimal("1")):
            cancel_event = OrderCancelledEvent(
                timestamp=1234, order_id="OID-BUY-1",
                exchange_order_id="ex-OID-BUY-1",
            )
            self.executor.process_order_canceled_event(
                1, MagicMock(), cancel_event,
            )

        # No hedge placed (the fix).
        self.assertIsNone(self.executor.taker_order)
        self.strategy.sell.assert_not_called()
        # Executor shuts down — controller spawns fresh on next tick.
        self.assertEqual(self.executor.status, RunnableStatus.SHUTTING_DOWN)

    def test_process_order_canceled_hedges_above_threshold_partial(self):
        """Above-threshold partial fills still get hedged — the skip is
        only for residuals too small to economically hedge."""
        self.executor._status = RunnableStatus.RUNNING
        self.executor.config.min_hedge_value_quote = Decimal("60")
        # 50 base × mid 2.0 = 100 quote > 60 → hedge normally.
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        in_flight = MagicMock()
        in_flight.executed_amount_base = Decimal("50")
        self.executor.maker_order.order = in_flight

        with patch.object(self.executor, "get_price", return_value=Decimal("2")):
            cancel_event = OrderCancelledEvent(
                timestamp=1234, order_id="OID-BUY-1",
                exchange_order_id="ex-OID-BUY-1",
            )
            self.executor.process_order_canceled_event(
                1, MagicMock(), cancel_event,
            )

        self.assertIsNotNone(self.executor.taker_order)
        self.strategy.sell.assert_called_once()
        # Hedged for the executed amount, not the full config order_amount.
        self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("50"))

    def test_process_order_canceled_no_fill_no_hedge(self):
        """Cancel with zero executed amount must not place a hedge."""
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        in_flight = MagicMock()
        in_flight.executed_amount_base = Decimal("0")
        self.executor.maker_order.order = in_flight

        cancel_event = OrderCancelledEvent(timestamp=1234, order_id="OID-BUY-1",
                                           exchange_order_id="ex-OID-BUY-1")
        self.executor.process_order_canceled_event(1, MagicMock(), cancel_event)

        self.assertIsNone(self.executor.taker_order)
        self.strategy.sell.assert_not_called()
        # Status stays RUNNING — control_maker_order will create a fresh
        # maker order on the next tick.
        self.assertEqual(self.executor.status, RunnableStatus.RUNNING)

    def test_process_order_canceled_does_not_double_hedge(self):
        """If the taker_order already exists (e.g. completed_event fired
        first), cancel must not place a second hedge."""
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        in_flight = MagicMock()
        in_flight.executed_amount_base = Decimal("100")
        self.executor.maker_order.order = in_flight
        # Pre-existing taker — the completed_event path placed it.
        self.executor.taker_order = TrackedOrder(order_id="OID-SELL-pre")

        cancel_event = OrderCancelledEvent(timestamp=1234, order_id="OID-BUY-1",
                                           exchange_order_id="ex-OID-BUY-1")
        self.executor.process_order_canceled_event(1, MagicMock(), cancel_event)

        # No new sell — the pre-existing taker_order stays.
        self.assertEqual(self.executor.taker_order.order_id, "OID-SELL-pre")
        self.strategy.sell.assert_not_called()

    def test_process_order_canceled_unknown_order_ignored(self):
        """Cancel for an order_id that's not our maker_order is a no-op."""
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        in_flight = MagicMock()
        in_flight.executed_amount_base = Decimal("50")
        self.executor.maker_order.order = in_flight

        cancel_event = OrderCancelledEvent(timestamp=1234, order_id="STRANGER",
                                           exchange_order_id="ex-STRANGER")
        self.executor.process_order_canceled_event(1, MagicMock(), cancel_event)

        self.assertIsNone(self.executor.taker_order)
        self.strategy.sell.assert_not_called()

    def test_fill_to_hedge_latency_captured(self):
        """``process_order_filled_event`` stamps first-fill ts; the next
        ``place_taker_order`` computes the latency in ms and exposes it
        in custom_info (Task 3.4)."""
        import time as _time
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        # Simulate the framework firing OrderFilledEvent for a partial fill.
        # We don't need a real OrderFilledEvent — only event.order_id is read.
        fill_event = MagicMock()
        fill_event.order_id = "OID-BUY-1"
        before = _time.time()
        self.executor.process_order_filled_event(1, MagicMock(), fill_event)
        self.assertGreaterEqual(self.executor._first_fill_ts, before)

        # Place the taker hedge — latency must be a non-negative int ms.
        self.executor.place_taker_order(amount=Decimal("0.5"))
        self.assertIsNotNone(self.executor._fill_to_hedge_latency_ms)
        self.assertIsInstance(self.executor._fill_to_hedge_latency_ms, int)
        self.assertGreaterEqual(self.executor._fill_to_hedge_latency_ms, 0)
        # Surfaces in custom_info for the ledger / digest.
        self.assertEqual(
            self.executor.get_custom_info()["fill_to_hedge_latency_ms"],
            self.executor._fill_to_hedge_latency_ms,
        )

    def test_fill_to_hedge_latency_not_set_without_fill(self):
        """If the maker order never reported a fill (e.g. the executor was
        early-stopped), latency stays None — never a negative or zero value
        that could be misread."""
        self.executor.place_taker_order(amount=Decimal("0.5"))
        self.assertIsNone(self.executor._fill_to_hedge_latency_ms)
        self.assertIsNone(
            self.executor.get_custom_info()["fill_to_hedge_latency_ms"]
        )

    def test_fill_to_hedge_latency_only_first_fill_counts(self):
        """Subsequent fills on the same maker order don't reset the timer —
        the first fill is what created the unhedged exposure."""
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        ev = MagicMock(); ev.order_id = "OID-BUY-1"
        self.executor.process_order_filled_event(1, MagicMock(), ev)
        first_ts = self.executor._first_fill_ts
        # Time passes, a second partial fill comes through.
        import time as _t
        _t.sleep(0.01)
        self.executor.process_order_filled_event(1, MagicMock(), ev)
        self.assertEqual(self.executor._first_fill_ts, first_ts)

    def test_place_taker_order_with_explicit_amount(self):
        """The new ``amount`` parameter overrides the config order_amount."""
        self.executor.place_taker_order(amount=Decimal("12.34"))
        self.strategy.sell.assert_called_once()
        # place_order → strategy.sell(connector, pair, amount, type, price, pos)
        self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("12.34"))

    def test_place_taker_order_default_uses_config_amount(self):
        """Backward compat: no amount → config.order_amount (100)."""
        self.executor.place_taker_order()
        self.strategy.sell.assert_called_once()
        self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("100"))

    def test_process_order_failed_event_maker_clears_state(self):
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        maker_failure_event = MarketOrderFailureEvent(
            timestamp=1234,
            order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
        )
        self.executor.process_order_failed_event(1, MagicMock(), maker_failure_event)
        self.assertEqual(self.executor.maker_order, None)

    def test_process_order_failed_event_taker_does_not_retry(self):
        """When the taker hedge fails (e.g. Binance MIN_NOTIONAL reject on a
        tiny partial-fill hedge), the executor MUST NOT silently retry with
        the default ``self.config.order_amount`` (full lot) — that bug
        produced a 2000x over-hedge in prod 2026-05-18 11:32. The new
        behaviour: log + transition to SHUTTING_DOWN; let inventory_audit
        reconcile via auto_rebalance.
        """
        self.executor._status = RunnableStatus.RUNNING
        self.executor.taker_order = TrackedOrder(order_id="OID-SELL-0")
        # Side-effect tracker — we must NOT call strategy.sell again after
        # the original "OID-SELL-0" placement.
        self.strategy.sell.reset_mock()

        taker_failure_event = MarketOrderFailureEvent(
            timestamp=1234,
            order_id="OID-SELL-0",
            order_type=OrderType.MARKET,
        )
        self.executor.process_order_failed_event(1, MagicMock(), taker_failure_event)

        # No new hedge placed (the fix).
        self.strategy.sell.assert_not_called()
        # Executor shuts down so the controller can spawn fresh on next tick;
        # audit will catch any residual drift.
        self.assertEqual(self.executor.status, RunnableStatus.SHUTTING_DOWN)
        # Original failed order recorded for diagnostics.
        self.assertIn(self.executor.taker_order, self.executor.failed_orders)

    def test_get_custom_info(self):
        self.assertEqual(self.executor.get_custom_info(), {'maker_connector': 'binance',
                                                           'maker_target_price': Decimal('1'),
                                                           'maker_trading_pair': 'ETH-USDT',
                                                           'max_profitability': Decimal('0.02'),
                                                           'min_profitability': Decimal('0.01'),
                                                           'net_profitability': Decimal('-1'),
                                                           'order_amount': Decimal('100'),
                                                           'side': TradeType.BUY,
                                                           'taker_connector': 'kucoin',
                                                           'taker_price': Decimal('1'),
                                                           'taker_trading_pair': 'ETH-USDT',
                                                           'target_profitability_pct': Decimal('0.015'),
                                                           'trade_profitability': Decimal('0'),
                                                           'tx_cost': Decimal('1'),
                                                           'tx_cost_pct': Decimal('1'),
                                                           'fill_to_hedge_latency_ms': None,
                                                           'taker_expected_price': None})

    def test_to_format_status(self):
        self.assertIn("Maker Side: TradeType.BUY", self.executor.to_format_status())

    def test_early_stop(self):
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.is_open = True
        self.executor.early_stop()
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    def test_early_stop_cancels_pending_create_order(self):
        """Regression: cancel-gate firing in the race window between
        ``place_order`` return and ``BuyOrderCreated`` event must still
        cancel the order.

        Before 2026-05-16, ``early_stop`` only cancelled when
        ``maker_order.order`` was set AND ``is_open``. During the 50–100 ms
        race window after ``place_order`` returns the client_order_id but
        before the connector emits the Created event, ``maker_order.order``
        is None — the cancel was silently skipped and the order stayed on
        the exchange book. Live incident at 07:13:21Z saw a gate fire 58 ms
        after place, order filled 30 s later as an orphan, rebalance
        panic-unwound at adverse price (loss R$0.089 per occurrence)."""
        self.executor._status = RunnableStatus.RUNNING
        # Simulate the race: order_id known, but underlying InFlightOrder
        # not yet assigned (BuyOrderCreatedEvent hasn't fired).
        tracked = TrackedOrder(order_id="OID-PENDING-1")
        # Explicitly leave tracked._order = None
        self.executor.maker_order = tracked
        self.executor.early_stop()
        # Cancel MUST have been dispatched despite order=None.
        self.executor._strategy.cancel.assert_called_once_with(
            self.executor.maker_connector,
            self.executor.maker_trading_pair,
            "OID-PENDING-1",
        )
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)
        self.assertEqual(self.executor.close_type, CloseType.EARLY_STOP)

    def test_early_stop_skips_cancel_for_done_order(self):
        """Already-done orders (FILLED / CANCELED / FAILED) should not
        trigger a redundant cancel REST."""
        self.executor._status = RunnableStatus.RUNNING
        tracked = TrackedOrder(order_id="OID-DONE-1")
        done_order = MagicMock(spec=InFlightOrder)
        done_order.is_done = True
        done_order.is_open = False
        tracked.order = done_order
        self.executor.maker_order = tracked
        self.executor.early_stop()
        self.executor._strategy.cancel.assert_not_called()
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    def test_early_stop_no_maker_order_is_noop_cancel(self):
        """No maker_order at all — no cancel attempt, just terminate."""
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = None
        self.executor.early_stop()
        self.executor._strategy.cancel.assert_not_called()
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    def test_early_stop_keep_position_sets_position_hold(self):
        """``keep_position=True`` → CloseType.POSITION_HOLD."""
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = None
        self.executor.early_stop(keep_position=True)
        self.assertEqual(self.executor.close_type, CloseType.POSITION_HOLD)

    def test_get_cum_fees_quote_not_executed(self):
        self.assertEqual(self.executor.get_cum_fees_quote(), Decimal('0'))

    @patch.object(XEMMExecutor, 'rate_oracle', create=True)
    async def test_get_quote_asset_conversion_rate_none(self, mock_rate_oracle):
        mock_rate_oracle.get_pair_rate.return_value = None
        self.executor.quote_conversion_pair = "USDC-USDT"
        with self.assertRaises(ValueError):
            await self.executor.get_quote_asset_conversion_rate()

    @patch.object(XEMMExecutor, 'rate_oracle', create=True)
    async def test_get_quote_asset_conversion_rate_exception(self, mock_rate_oracle):
        mock_rate_oracle.get_pair_rate.side_effect = Exception("Test exception")
        self.executor.quote_conversion_pair = "USDC-USDT"
        with self.assertRaises(Exception):
            await self.executor.get_quote_asset_conversion_rate()
