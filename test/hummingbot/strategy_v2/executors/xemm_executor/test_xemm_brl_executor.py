"""Unit tests for XEMMBRLExecutor — verifies LIMIT_MAKER usage in place of LIMIT."""
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import MagicMock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.executors.xemm_executor.xemm_brl_executor import XEMMBRLExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


class TestXEMMBRLExecutor(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self):
        super().setUp()
        self.strategy = self._create_mock_strategy()
        self.config = XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name="bybit", trading_pair="BTC-BRL"),
            selling_market=ConnectorPair(connector_name="binance", trading_pair="BTC-BRL"),
            maker_side=TradeType.BUY,
            order_amount=Decimal("0.001"),
            min_profitability=Decimal("0.0007"),
            target_profitability=Decimal("0.002"),
            max_profitability=Decimal("0.008"),
        )
        self.executor = XEMMBRLExecutor(self.strategy, self.config, update_interval=0.5)
        self.set_loggers(loggers=[self.executor.logger()])

    @staticmethod
    def _create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market
        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="BTC-BRL")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2"]
        strategy.cancel.return_value = None
        bybit_connector = MagicMock(spec=ExchangePyBase)
        bybit_connector.supported_order_types = MagicMock(
            return_value=[OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET])
        binance_connector = MagicMock(spec=ExchangePyBase)
        binance_connector.supported_order_types = MagicMock(
            return_value=[OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET])
        strategy.connectors = {"bybit": bybit_connector, "binance": binance_connector}
        return strategy

    async def test_create_maker_order_uses_limit_maker_buy(self):
        """The new maker order must be placed as LIMIT_MAKER, never LIMIT."""
        self.executor._maker_target_price = Decimal("300000")
        with patch.object(self.executor, "place_order", return_value="OID-1") as mock_place:
            await self.executor.create_maker_order()
        mock_place.assert_called_once()
        call_kwargs = mock_place.call_args.kwargs
        self.assertEqual(call_kwargs["order_type"], OrderType.LIMIT_MAKER)
        self.assertEqual(call_kwargs["connector_name"], "bybit")
        self.assertEqual(call_kwargs["trading_pair"], "BTC-BRL")
        self.assertEqual(call_kwargs["side"], TradeType.BUY)
        self.assertEqual(call_kwargs["amount"], Decimal("0.001"))
        self.assertEqual(call_kwargs["price"], Decimal("300000"))
        self.assertIsNotNone(self.executor.maker_order)
        self.assertEqual(self.executor.maker_order.order_id, "OID-1")

    async def test_create_maker_order_uses_limit_maker_sell(self):
        """Same check for SELL maker side."""
        sell_config = XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name="binance", trading_pair="BTC-BRL"),
            selling_market=ConnectorPair(connector_name="bybit", trading_pair="BTC-BRL"),
            maker_side=TradeType.SELL,
            order_amount=Decimal("0.001"),
            min_profitability=Decimal("0.0007"),
            target_profitability=Decimal("0.002"),
            max_profitability=Decimal("0.008"),
        )
        executor = XEMMBRLExecutor(self.strategy, sell_config, update_interval=0.5)
        executor._maker_target_price = Decimal("300100")
        with patch.object(executor, "place_order", return_value="OID-S-1") as mock_place:
            await executor.create_maker_order()
        call_kwargs = mock_place.call_args.kwargs
        self.assertEqual(call_kwargs["order_type"], OrderType.LIMIT_MAKER)
        self.assertEqual(call_kwargs["side"], TradeType.SELL)
        self.assertEqual(call_kwargs["connector_name"], "bybit")  # maker side = bybit

    @patch.object(XEMMBRLExecutor, "get_trading_rules")
    @patch.object(XEMMBRLExecutor, "adjust_order_candidates")
    async def test_validate_sufficient_balance_uses_limit_maker_for_maker_candidate(
        self, mock_adjust, mock_get_rules,
    ):
        """The maker_order_candidate inside validate_sufficient_balance must be LIMIT_MAKER."""
        rules = TradingRule(
            trading_pair="BTC-BRL",
            min_order_size=Decimal("0.0001"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.0001"),
        )
        mock_get_rules.return_value = rules

        # Capture the OrderCandidate passed to adjust_order_candidates.
        captured = []

        def capture_side_effect(connector_name, candidates):
            captured.append((connector_name, candidates))
            # Return candidates with non-zero amount → balance is sufficient.
            return candidates

        mock_adjust.side_effect = capture_side_effect

        await self.executor.validate_sufficient_balance()

        # adjust_order_candidates is called twice: once for maker, once for taker.
        self.assertEqual(len(captured), 2)
        maker_call = captured[0]
        self.assertEqual(maker_call[0], "bybit")  # maker connector
        maker_candidate: OrderCandidate = maker_call[1][0]
        self.assertEqual(maker_candidate.order_type, OrderType.LIMIT_MAKER)
        self.assertTrue(maker_candidate.is_maker)
        # Taker candidate should still be MARKET.
        taker_call = captured[1]
        self.assertEqual(taker_call[0], "binance")
        taker_candidate: OrderCandidate = taker_call[1][0]
        self.assertEqual(taker_candidate.order_type, OrderType.MARKET)
        self.assertFalse(taker_candidate.is_maker)

    @patch.object(XEMMBRLExecutor, "get_trading_rules")
    @patch.object(XEMMBRLExecutor, "adjust_order_candidates")
    async def test_validate_sufficient_balance_marks_insufficient(
        self, mock_adjust, mock_get_rules,
    ):
        rules = TradingRule(
            trading_pair="BTC-BRL",
            min_order_size=Decimal("0.0001"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.0001"),
        )
        mock_get_rules.return_value = rules

        # Return a candidate with amount=0 to simulate insufficient balance.
        zero_candidate = OrderCandidate(
            trading_pair="BTC-BRL",
            is_maker=True,
            order_type=OrderType.LIMIT_MAKER,
            order_side=TradeType.BUY,
            amount=Decimal("0"),
            price=Decimal("300000"),
        )
        mock_adjust.return_value = [zero_candidate]

        await self.executor.validate_sufficient_balance()

        self.assertEqual(self.executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        self.assertEqual(self.executor.status, RunnableStatus.TERMINATED)

    def test_inherits_xemm_lifecycle_attributes(self):
        """XEMMBRLExecutor is a true XEMMExecutor (instance & method inheritance)."""
        from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
        self.assertIsInstance(self.executor, XEMMExecutor)
        # Methods we did not override must come from the base class.
        self.assertEqual(self.executor.maker_connector, "bybit")
        self.assertEqual(self.executor.taker_connector, "binance")
        self.assertEqual(self.executor.maker_order_side, TradeType.BUY)
        self.assertEqual(self.executor.taker_order_side, TradeType.SELL)
