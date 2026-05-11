"""Unit tests for LeadLagArbitrageExecutor — partial-leg failure unwind."""
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import (
    LeadLagArbitrageExecutorConfig,
)
from hummingbot.strategy_v2.executors.arbitrage_executor.lead_lag_arbitrage_executor import (
    LeadLagArbitrageExecutor,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.executors import CloseType


class TestLeadLagArbitrageExecutor(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self):
        super().setUp()
        self.strategy = self._create_mock_strategy()
        self.config = LeadLagArbitrageExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name="binance", trading_pair="BTC-USDT"),
            selling_market=ConnectorPair(connector_name="bybit", trading_pair="BTC-USDT"),
            order_amount=Decimal("0.001"),
            min_profitability=Decimal("0.0015"),
            arb_max_unwind_slippage_bps=Decimal("50"),
            arb_unwind_strategy="abort_and_alert",
        )
        self.executor = LeadLagArbitrageExecutor(self.strategy, self.config, update_interval=0.5)
        self.set_loggers(loggers=[self.executor.logger()])

    @staticmethod
    def _create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market
        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="BTC-USDT")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2"]
        strategy.cancel.return_value = None
        binance = MagicMock(spec=ConnectorBase)
        bybit = MagicMock(spec=ConnectorBase)
        strategy.connectors = {"binance": binance, "bybit": bybit}
        return strategy

    def test_inherits_arbitrage_executor(self):
        self.assertIsInstance(self.executor, ArbitrageExecutor)

    def test_uses_lead_lag_arbitrage_config_type(self):
        self.assertEqual(self.config.type, "lead_lag_arbitrage_executor")

    def test_default_unwind_strategy_is_abort_and_alert(self):
        cfg = LeadLagArbitrageExecutorConfig(
            timestamp=1,
            buying_market=ConnectorPair(connector_name="binance", trading_pair="BTC-USDT"),
            selling_market=ConnectorPair(connector_name="bybit", trading_pair="BTC-USDT"),
            order_amount=Decimal("0.001"),
            min_profitability=Decimal("0.001"),
        )
        self.assertEqual(cfg.arb_unwind_strategy, "abort_and_alert")
        self.assertEqual(cfg.arb_max_unwind_slippage_bps, Decimal("50"))

    async def test_unwind_aborts_when_slippage_exceeds_max(self):
        """Slippage > gate + abort_and_alert → does NOT place order, sets UNWIND_ABORTED."""
        # Mock high slippage on both legs
        with patch.object(self.executor, "_estimate_unwind_slippage",
                          new=AsyncMock(return_value=Decimal("100"))):  # 100bps > 50bps gate
            with patch.object(self.executor, "place_order") as mock_place:
                with patch.object(self.executor, "stop") as mock_stop:
                    await self.executor._unwind_position(
                        executed_side=TradeType.BUY,
                        executed_amount=Decimal("0.001"),
                    )
        mock_place.assert_not_called()
        self.assertEqual(self.executor.close_type, CloseType.UNWIND_ABORTED)
        mock_stop.assert_called_once()

    async def test_unwind_force_executes_even_with_high_slip(self):
        """Slippage > gate + force_unwind → does place order anyway."""
        self.executor.config.arb_unwind_strategy = "force_unwind"
        with patch.object(self.executor, "_estimate_unwind_slippage",
                          new=AsyncMock(return_value=Decimal("100"))):
            with patch.object(self.executor, "place_order", return_value="OID-FORCED") as mock_place:
                with patch.object(self.executor, "stop"):
                    await self.executor._unwind_position(
                        executed_side=TradeType.BUY,
                        executed_amount=Decimal("0.001"),
                    )
        mock_place.assert_called_once()
        self.assertEqual(self.executor.close_type, CloseType.UNWOUND)

    async def test_unwind_picks_exchange_with_lower_slippage(self):
        """Of the two markets, the one with lower slippage gets the unwind order."""
        async def slip_side_effect(market, side, amount):
            # buying_market=binance has higher slip than selling_market=bybit
            if market.connector_name == "binance":
                return Decimal("30")
            return Decimal("10")

        with patch.object(self.executor, "_estimate_unwind_slippage",
                          new=AsyncMock(side_effect=slip_side_effect)):
            with patch.object(self.executor, "place_order", return_value="OID-LOWER") as mock_place:
                with patch.object(self.executor, "stop"):
                    await self.executor._unwind_position(
                        executed_side=TradeType.BUY,
                        executed_amount=Decimal("0.001"),
                    )
        # Should pick bybit (lower slip)
        self.assertEqual(mock_place.call_args.kwargs["connector_name"], "bybit")
        self.assertEqual(self.executor.close_type, CloseType.UNWOUND)

    async def test_unwind_inverse_side_for_buy_executed(self):
        """If BUY was the executed side, unwind is SELL."""
        with patch.object(self.executor, "_estimate_unwind_slippage",
                          new=AsyncMock(return_value=Decimal("10"))):
            with patch.object(self.executor, "place_order", return_value="OID-INV") as mock_place:
                with patch.object(self.executor, "stop"):
                    await self.executor._unwind_position(
                        executed_side=TradeType.BUY,
                        executed_amount=Decimal("0.001"),
                    )
        self.assertEqual(mock_place.call_args.kwargs["side"], TradeType.SELL)
        self.assertEqual(mock_place.call_args.kwargs["order_type"], OrderType.MARKET)

    async def test_unwind_inverse_side_for_sell_executed(self):
        """If SELL was the executed side, unwind is BUY."""
        with patch.object(self.executor, "_estimate_unwind_slippage",
                          new=AsyncMock(return_value=Decimal("10"))):
            with patch.object(self.executor, "place_order", return_value="OID-INV") as mock_place:
                with patch.object(self.executor, "stop"):
                    await self.executor._unwind_position(
                        executed_side=TradeType.SELL,
                        executed_amount=Decimal("0.001"),
                    )
        self.assertEqual(mock_place.call_args.kwargs["side"], TradeType.BUY)

    async def test_unwind_aborts_if_slippage_estimation_fails(self):
        """If we can't estimate slippage on either leg, abort safely."""
        with patch.object(self.executor, "_estimate_unwind_slippage",
                          new=AsyncMock(side_effect=ValueError("book empty"))):
            with patch.object(self.executor, "place_order") as mock_place:
                with patch.object(self.executor, "stop") as mock_stop:
                    await self.executor._unwind_position(
                        executed_side=TradeType.BUY,
                        executed_amount=Decimal("0.001"),
                    )
        mock_place.assert_not_called()
        self.assertEqual(self.executor.close_type, CloseType.UNWIND_ABORTED)
        mock_stop.assert_called_once()

    def test_partial_failure_detection_buy_filled_sell_failed(self):
        """When buy already filled and sell fails → unwind_attempted=True (partial failure path)."""
        # Set buy_order to have executed amount
        self.executor._buy_order.order = MagicMock()
        self.executor._buy_order.order.executed_amount_base = Decimal("0.001")
        self.executor._buy_order.order_id = "OID-BUY-1"
        # sell failed
        self.executor._sell_order.order = None
        self.executor._sell_order.order_id = "OID-SELL-1"
        # before: not attempted
        self.assertFalse(self.executor._unwind_attempted)

        # Patch safe_ensure_future to avoid scheduling the coroutine
        with patch(
            "hummingbot.strategy_v2.executors.arbitrage_executor."
            "lead_lag_arbitrage_executor.safe_ensure_future"
        ) as mock_sef:
            with patch.object(self.executor, "_unwind_position",
                              new=MagicMock(return_value=AsyncMock()())):
                event = MagicMock(spec=MarketOrderFailureEvent)
                event.order_id = "OID-SELL-1"
                self.executor.process_order_failed_event(None, None, event)

        # After: attempted flag set
        self.assertTrue(self.executor._unwind_attempted)
        mock_sef.assert_called_once()

    def test_no_partial_failure_falls_back_to_base_retry(self):
        """When neither side is filled, treat as plain retry (base behaviour)."""
        # Both orders failed without execution
        self.executor._buy_order.order = None
        self.executor._buy_order.order_id = "OID-BUY-1"
        self.executor._sell_order.order = None
        self.executor._sell_order.order_id = "OID-SELL-1"

        with patch.object(ArbitrageExecutor, "process_order_failed_event") as mock_base:
            event = MagicMock(spec=MarketOrderFailureEvent)
            event.order_id = "OID-BUY-1"
            self.executor.process_order_failed_event(None, None, event)

        # Base class retry was invoked
        mock_base.assert_called_once()
        # Unwind not attempted
        self.assertFalse(self.executor._unwind_attempted)
