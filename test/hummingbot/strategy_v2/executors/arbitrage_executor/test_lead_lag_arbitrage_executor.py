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

    async def test_estimate_unwind_slippage_uses_exchange_kwarg(self):
        """Regression pin (2026-05-11 18:06 incident): the base method
        ``ArbitrageExecutor.get_resulting_price_for_amount`` takes
        ``exchange`` as the connector-name kwarg, not ``connector``.
        Calling with the wrong name raised TypeError, which the unwind
        path's outer try caught and turned into UNWIND_ABORTED — leaving
        partial-leg failures with an open position.
        """
        market = MagicMock()
        market.connector_name = "binance"
        market.trading_pair = "BTC-USDT"
        # Override the spec'd connector with a free-form mock so we can
        # add the get_order_book attribute the call path needs after
        # the kwarg-typing check passes. ExecutorBase copies the
        # strategy.connectors dict into self.connectors at __init__
        # time (see executor_base.py:53), so we patch the executor's
        # own dict directly.
        free_conn = MagicMock()
        ob_mock = MagicMock()
        ob_mock.get_price.return_value = 50_000.0
        free_conn.get_order_book.return_value = ob_mock
        self.executor.connectors["binance"] = free_conn

        called_with = {}

        async def _fake_get_price(**kw):
            called_with.update(kw)
            return Decimal("50050")  # 10 bps over best ask

        with patch.object(self.executor, "get_resulting_price_for_amount",
                          new=AsyncMock(side_effect=_fake_get_price)):
            slip = await self.executor._estimate_unwind_slippage(
                market=market,
                side=TradeType.BUY,
                amount=Decimal("0.001"),
            )

        # The kwarg name passed by the call site MUST match the base signature.
        # Asserting on the captured kwargs is the actual regression guard:
        # if someone re-introduces ``connector=...`` the base would reject
        # it as TypeError and we'd never reach this assert.
        self.assertIn("exchange", called_with)
        self.assertEqual(called_with["exchange"], "binance")
        self.assertNotIn("connector", called_with)
        # And slippage is computed correctly: 10 bps.
        self.assertAlmostEqual(float(slip), 10.0, places=1)

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


class TestAggressiveLimitPlacement(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """When ``arb_maker_leg_type=AGGRESSIVE_LIMIT``, the BitPreco leg is
    placed as a marketable LIMIT (best ± margin) instead of MARKET. The
    other leg (binance) stays MARKET regardless of config.
    """

    def setUp(self):
        super().setUp()
        self.strategy = self._make_strategy()
        self.config = LeadLagArbitrageExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name="binance", trading_pair="BTC-BRL"),
            selling_market=ConnectorPair(connector_name="bitpreco", trading_pair="BTC-BRL"),
            order_amount=Decimal("0.0002"),
            min_profitability=Decimal("0.0004"),
            arb_max_unwind_slippage_bps=Decimal("50"),
            arb_unwind_strategy="abort_and_alert",
            arb_maker_leg_type="AGGRESSIVE_LIMIT",
            arb_aggressive_limit_margin_pct=Decimal("0.005"),
            arb_aggressive_limit_timeout_sec=3.0,
            maker_connector_name="bitpreco",
        )
        self.executor = LeadLagArbitrageExecutor(self.strategy, self.config, update_interval=0.5)
        self.set_loggers(loggers=[self.executor.logger()])

    def _make_strategy(self):
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market
        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="BTC-BRL")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2"]
        strategy.cancel.return_value = None

        # binance connector — for the BUY leg (MARKET, no aggressive LIMIT)
        binance = MagicMock(spec=ConnectorBase)
        # bitpreco connector — for the SELL leg (AGGRESSIVE_LIMIT)
        bitpreco = MagicMock(spec=ConnectorBase)
        # bitpreco best_bid = 400,000 → SELL LIMIT @ 400,000 × (1 - 0.005) = 398,000
        bitpreco.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        strategy.connectors = {"binance": binance, "bitpreco": bitpreco}
        return strategy

    def test_short_arb_sell_uses_aggressive_limit_on_bitpreco(self):
        """For SHORT arb (selling=bitpreco), the SELL is LIMIT priced at
        best_bid × (1 - margin)."""
        with patch.object(self.executor, "place_order",
                          wraps=self.executor.place_order) as mock_place:
            self.executor.place_sell_arbitrage_order()
        mock_place.assert_called_once()
        kwargs = mock_place.call_args.kwargs
        self.assertEqual(kwargs["connector_name"], "bitpreco")
        self.assertEqual(kwargs["order_type"], OrderType.LIMIT)
        self.assertEqual(kwargs["side"], TradeType.SELL)
        # 400000 × (1 - 0.005) = 398000
        self.assertEqual(kwargs["price"], Decimal("398000.000"))
        self.assertTrue(self.executor._aggressive_limit_sell_active)

    def test_long_arb_buy_uses_aggressive_limit_on_bitpreco(self):
        """For LONG arb (buying=bitpreco), the BUY is LIMIT priced at
        best_ask × (1 + margin)."""
        # Flip the markets so bitpreco is the BUY side
        self.config.buying_market = ConnectorPair(connector_name="bitpreco", trading_pair="BTC-BRL")
        self.config.selling_market = ConnectorPair(connector_name="binance", trading_pair="BTC-BRL")
        self.executor.buying_market = self.config.buying_market
        self.executor.selling_market = self.config.selling_market

        with patch.object(self.executor, "place_order",
                          wraps=self.executor.place_order) as mock_place:
            self.executor.place_buy_arbitrage_order()
        kwargs = mock_place.call_args.kwargs
        self.assertEqual(kwargs["connector_name"], "bitpreco")
        self.assertEqual(kwargs["order_type"], OrderType.LIMIT)
        self.assertEqual(kwargs["side"], TradeType.BUY)
        # 400000 × (1 + 0.005) = 402000
        self.assertEqual(kwargs["price"], Decimal("402000.000"))
        self.assertTrue(self.executor._aggressive_limit_buy_active)

    def test_binance_leg_stays_market(self):
        """The other leg (binance side) stays MARKET regardless of config."""
        with patch.object(self.executor, "place_order",
                          wraps=self.executor.place_order) as mock_place:
            self.executor.place_buy_arbitrage_order()
        kwargs = mock_place.call_args.kwargs
        self.assertEqual(kwargs["connector_name"], "binance")
        self.assertEqual(kwargs["order_type"], OrderType.MARKET)
        # No watchdog state set for binance
        self.assertFalse(self.executor._aggressive_limit_buy_active)

    def test_market_mode_disables_aggressive_limit_entirely(self):
        """``arb_maker_leg_type=MARKET`` reverts to classical behaviour."""
        self.config.arb_maker_leg_type = "MARKET"
        with patch.object(self.executor, "place_order",
                          wraps=self.executor.place_order) as mock_place:
            self.executor.place_sell_arbitrage_order()
        kwargs = mock_place.call_args.kwargs
        self.assertEqual(kwargs["order_type"], OrderType.MARKET)
        self.assertFalse(self.executor._aggressive_limit_sell_active)


class TestAggressiveLimitWatchdog(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """Watchdog cancels the aggressive LIMIT after timeout and triggers
    unwind on the imbalance.
    """

    def setUp(self):
        super().setUp()
        self.strategy = MagicMock(spec=StrategyV2Base)
        type(self.strategy).market_info = PropertyMock(return_value=MagicMock())
        self.strategy.connectors = {
            "binance": MagicMock(spec=ConnectorBase),
            "bitpreco": MagicMock(spec=ConnectorBase),
        }
        self.config = LeadLagArbitrageExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name="binance", trading_pair="BTC-BRL"),
            selling_market=ConnectorPair(connector_name="bitpreco", trading_pair="BTC-BRL"),
            order_amount=Decimal("0.0002"),
            min_profitability=Decimal("0.0004"),
            arb_maker_leg_type="AGGRESSIVE_LIMIT",
            arb_aggressive_limit_margin_pct=Decimal("0.005"),
            arb_aggressive_limit_timeout_sec=0.05,  # tiny for tests
            maker_connector_name="bitpreco",
        )
        self.executor = LeadLagArbitrageExecutor(self.strategy, self.config, update_interval=0.5)
        self.set_loggers(loggers=[self.executor.logger()])

    async def test_fully_filled_in_window_skips_unwind(self):
        """Order is_filled before timeout → watchdog exits without cancel/unwind."""
        self.executor._sell_order.order = MagicMock()
        self.executor._sell_order.order.is_filled = True
        self.executor._sell_order.order_id = "SELL-1"

        with patch.object(self.executor, "_unwind_position",
                          new=AsyncMock()) as mock_unwind:
            await self.executor._aggressive_limit_watchdog(is_buy_leg=False)
        mock_unwind.assert_not_awaited()
        self.strategy.cancel.assert_not_called()

    async def test_partial_fill_after_timeout_triggers_unwind_of_residue(self):
        """SELL leg partial (0.00012 of 0.0002), BUY leg full (0.0002) → unwind 0.00008 BUY."""
        # BUY leg fully filled (binance MARKET)
        self.executor._buy_order.order = MagicMock()
        self.executor._buy_order.order.executed_amount_base = Decimal("0.0002")
        self.executor._buy_order.order.is_filled = True
        self.executor._buy_order.order_id = "BUY-1"
        # SELL leg partially filled
        self.executor._sell_order.order = MagicMock()
        self.executor._sell_order.order.executed_amount_base = Decimal("0.00012")
        self.executor._sell_order.order.is_filled = False
        self.executor._sell_order.order_id = "SELL-1"

        with patch.object(self.executor, "_unwind_position",
                          new=AsyncMock()) as mock_unwind:
            await self.executor._aggressive_limit_watchdog(is_buy_leg=False)

        # cancel was issued on the bitpreco SELL leg
        self.strategy.cancel.assert_called_once()
        cancel_kwargs = self.strategy.cancel.call_args.kwargs
        self.assertEqual(cancel_kwargs["connector_name"], "bitpreco")
        self.assertEqual(cancel_kwargs["order_id"], "SELL-1")

        # unwind called with residue
        mock_unwind.assert_awaited_once()
        unwind_kwargs = mock_unwind.call_args.kwargs
        self.assertEqual(unwind_kwargs["executed_side"], TradeType.BUY)
        self.assertEqual(unwind_kwargs["executed_amount"], Decimal("0.00008"))
        self.assertTrue(self.executor._unwind_attempted)

    async def test_zero_fill_after_timeout_unwinds_full_opposite_leg(self):
        """SELL didn't fill at all → unwind full BUY leg (0.0002)."""
        # BUY filled
        self.executor._buy_order.order = MagicMock()
        self.executor._buy_order.order.executed_amount_base = Decimal("0.0002")
        self.executor._buy_order.order.is_filled = True
        self.executor._buy_order.order_id = "BUY-1"
        # SELL not filled
        self.executor._sell_order.order = MagicMock()
        self.executor._sell_order.order.executed_amount_base = Decimal("0")
        self.executor._sell_order.order.is_filled = False
        self.executor._sell_order.order_id = "SELL-1"

        with patch.object(self.executor, "_unwind_position",
                          new=AsyncMock()) as mock_unwind:
            await self.executor._aggressive_limit_watchdog(is_buy_leg=False)

        mock_unwind.assert_awaited_once()
        unwind_kwargs = mock_unwind.call_args.kwargs
        self.assertEqual(unwind_kwargs["executed_side"], TradeType.BUY)
        self.assertEqual(unwind_kwargs["executed_amount"], Decimal("0.0002"))

    async def test_unwind_attempted_flag_prevents_double_unwind(self):
        """If process_order_failed_event already set the flag, watchdog skips."""
        self.executor._unwind_attempted = True
        # SELL leg partial — would normally trigger
        self.executor._buy_order.order = MagicMock()
        self.executor._buy_order.order.executed_amount_base = Decimal("0.0002")
        self.executor._buy_order.order.is_filled = True
        self.executor._buy_order.order_id = "BUY-1"
        self.executor._sell_order.order = MagicMock()
        self.executor._sell_order.order.executed_amount_base = Decimal("0.00012")
        self.executor._sell_order.order.is_filled = False
        self.executor._sell_order.order_id = "SELL-1"

        with patch.object(self.executor, "_unwind_position",
                          new=AsyncMock()) as mock_unwind:
            await self.executor._aggressive_limit_watchdog(is_buy_leg=False)

        # cancel still issued (cleanup), but unwind NOT (already attempted)
        mock_unwind.assert_not_awaited()


class TestLegExecutionOrder(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """Pin the leg-ordering behaviour of ``execute_arbitrage``:
    parallel, maker_first, taker_first.

    Role-based (maker/taker) instead of exchange-specific so the
    configuration stays valid when swapping connectors.
    """

    def _make_executor(self, leg_order: str, buying: str, selling: str,
                       maker_connector_name: str = "bitpreco"):
        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=MagicMock())
        strategy.connectors = {
            buying: MagicMock(spec=ConnectorBase),
            selling: MagicMock(spec=ConnectorBase),
        }
        cfg = LeadLagArbitrageExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name=buying, trading_pair="BTC-BRL"),
            selling_market=ConnectorPair(connector_name=selling, trading_pair="BTC-BRL"),
            order_amount=Decimal("0.0002"),
            min_profitability=Decimal("0.0004"),
            arb_leg_execution_order=leg_order,
            arb_maker_leg_type="MARKET",  # don't entangle with aggressive-LIMIT in this test
            maker_connector_name=maker_connector_name,
        )
        return LeadLagArbitrageExecutor(strategy, cfg, update_interval=0.5)

    async def test_parallel_places_both_without_awaiting_between(self):
        """parallel mode: both placements happen back-to-back, no ack wait."""
        ex = self._make_executor("parallel", "binance", "bitpreco")
        call_order = []
        with patch.object(ex, "place_buy_arbitrage_order",
                          side_effect=lambda: call_order.append("buy")):
            with patch.object(ex, "place_sell_arbitrage_order",
                              side_effect=lambda: call_order.append("sell")):
                with patch.object(ex, "_wait_for_order_ack",
                                  new=AsyncMock()) as mock_wait:
                    await ex.execute_arbitrage()
        # Both called, in original order (buy first per base class semantics)
        self.assertEqual(call_order, ["buy", "sell"])
        # No ack wait in parallel mode
        mock_wait.assert_not_called()

    async def test_maker_first_long_arb_places_sell_first(self):
        """LONG arb: buying=taker (binance), selling=maker (bitpreco).
        ``maker_first`` → SELL (maker) first, await, then BUY (taker)."""
        ex = self._make_executor("maker_first", "binance", "bitpreco",
                                 maker_connector_name="bitpreco")
        call_order = []
        with patch.object(ex, "place_buy_arbitrage_order",
                          side_effect=lambda: call_order.append("buy")):
            with patch.object(ex, "place_sell_arbitrage_order",
                              side_effect=lambda: call_order.append("sell")):
                with patch.object(ex, "_wait_for_order_ack",
                                  new=AsyncMock(return_value=True)) as mock_wait:
                    await ex.execute_arbitrage()
        # selling is on the maker (bitpreco) → sell first
        self.assertEqual(call_order, ["sell", "buy"])
        mock_wait.assert_awaited_once()
        self.assertIs(mock_wait.await_args.args[0], ex.sell_order)

    async def test_maker_first_short_arb_places_buy_first(self):
        """SHORT arb: buying=maker (bitpreco), selling=taker (binance).
        ``maker_first`` → BUY (maker) first, await, then SELL (taker)."""
        ex = self._make_executor("maker_first", "bitpreco", "binance",
                                 maker_connector_name="bitpreco")
        call_order = []
        with patch.object(ex, "place_buy_arbitrage_order",
                          side_effect=lambda: call_order.append("buy")):
            with patch.object(ex, "place_sell_arbitrage_order",
                              side_effect=lambda: call_order.append("sell")):
                with patch.object(ex, "_wait_for_order_ack",
                                  new=AsyncMock(return_value=True)) as mock_wait:
                    await ex.execute_arbitrage()
        self.assertEqual(call_order, ["buy", "sell"])
        self.assertIs(mock_wait.await_args.args[0], ex.buy_order)

    async def test_taker_first_long_arb_places_buy_first(self):
        """LONG arb: buying=taker. ``taker_first`` → BUY first."""
        ex = self._make_executor("taker_first", "binance", "bitpreco",
                                 maker_connector_name="bitpreco")
        call_order = []
        with patch.object(ex, "place_buy_arbitrage_order",
                          side_effect=lambda: call_order.append("buy")):
            with patch.object(ex, "place_sell_arbitrage_order",
                              side_effect=lambda: call_order.append("sell")):
                with patch.object(ex, "_wait_for_order_ack",
                                  new=AsyncMock(return_value=True)) as mock_wait:
                    await ex.execute_arbitrage()
        self.assertEqual(call_order, ["buy", "sell"])
        self.assertIs(mock_wait.await_args.args[0], ex.buy_order)

    async def test_works_with_arbitrary_exchange_names(self):
        """Role-based: maker/taker abstraction holds for any pair.
        Swap to kraken (maker) + bybit (taker) — same semantics."""
        ex = self._make_executor("maker_first", "bybit", "kraken",
                                 maker_connector_name="kraken")
        call_order = []
        with patch.object(ex, "place_buy_arbitrage_order",
                          side_effect=lambda: call_order.append("buy")):
            with patch.object(ex, "place_sell_arbitrage_order",
                              side_effect=lambda: call_order.append("sell")):
                with patch.object(ex, "_wait_for_order_ack",
                                  new=AsyncMock(return_value=True)):
                    await ex.execute_arbitrage()
        # selling_market is on kraken (the maker) → sell first
        self.assertEqual(call_order, ["sell", "buy"])

    async def test_unknown_mode_falls_back_to_parallel(self):
        """Unknown ordering mode → warn + parallel placement. Pydantic
        normally rejects invalid Literal values at construction time, but
        defensive runtime fallback covers attribute-set bypass."""
        ex = self._make_executor("parallel", "binance", "bitpreco")
        # Bypass Pydantic to inject an invalid value, simulating runtime mutation
        object.__setattr__(ex.config, "arb_leg_execution_order", "nonsense_mode")
        call_order = []
        with patch.object(ex, "place_buy_arbitrage_order",
                          side_effect=lambda: call_order.append("buy")):
            with patch.object(ex, "place_sell_arbitrage_order",
                              side_effect=lambda: call_order.append("sell")):
                with patch.object(ex, "_wait_for_order_ack",
                                  new=AsyncMock()) as mock_wait:
                    await ex.execute_arbitrage()
        # Falls back to parallel — both placed, no await
        self.assertEqual(call_order, ["buy", "sell"])
        mock_wait.assert_not_called()

    async def test_missing_maker_connector_name_falls_back_to_parallel(self):
        """If non-parallel mode is requested but maker_connector_name is
        absent, fall back to parallel + log warning."""
        ex = self._make_executor("maker_first", "binance", "bitpreco",
                                 maker_connector_name="bitpreco")
        # Wipe the maker_connector_name to simulate misconfigured controller
        object.__setattr__(ex.config, "maker_connector_name", None)
        call_order = []
        with patch.object(ex, "place_buy_arbitrage_order",
                          side_effect=lambda: call_order.append("buy")):
            with patch.object(ex, "place_sell_arbitrage_order",
                              side_effect=lambda: call_order.append("sell")):
                with patch.object(ex, "_wait_for_order_ack",
                                  new=AsyncMock()) as mock_wait:
                    await ex.execute_arbitrage()
        self.assertEqual(call_order, ["buy", "sell"])
        mock_wait.assert_not_called()

    async def test_wait_for_order_ack_returns_true_when_exchange_id_assigned(self):
        ex = self._make_executor("parallel", "binance", "bitpreco")
        # Simulate the tracker registering with exchange_order_id immediately
        ex.buy_order.order = MagicMock()
        ex.buy_order.order.exchange_order_id = "EX-1234"
        result = await ex._wait_for_order_ack(ex.buy_order, max_wait_sec=0.1)
        self.assertTrue(result)

    async def test_wait_for_order_ack_times_out_when_no_id_assigned(self):
        ex = self._make_executor("parallel", "binance", "bitpreco")
        ex.buy_order.order = None  # never registered
        result = await ex._wait_for_order_ack(
            ex.buy_order, max_wait_sec=0.05, poll_interval_sec=0.01
        )
        self.assertFalse(result)
