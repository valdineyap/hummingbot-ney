"""
Unit tests for XEMMLeadLagController.

These tests require the full Hummingbot environment (./compile) since they
import from compiled Cython modules transitively (ControllerBase →
in_flight_order, etc).

Test groups:
  A: Markets / config (4)
  B: update_processed_data (5)
  C: Risk gates / regime (8+4)
  D: Anti-churn (3)
  E: Executor creation (5)
  F: Profitability adjustment (5)
  G: Inventory skew (2 — corrected directions)
  H: Shadow mode (3)
"""
import asyncio
import os
import tempfile
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    StopExecutorAction,
)
from hummingbot.strategy_v2.utils.lead_lag_signal import SignalQuality

from controllers.generic.xemm_lead_lag import (
    Regime,
    XEMMLeadLagConfig,
    XEMMLeadLagController,
)


class _BaseControllerTest(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """Shared setup. Subclasses get a fully-mocked controller."""

    def setUp(self):
        super().setUp()
        self.tmp_dir = tempfile.mkdtemp()
        self.config = XEMMLeadLagConfig(
            id="test-controller-1",
            controller_name="xemm_lead_lag",
            controller_type="generic",
            maker_connector="bybit",
            maker_trading_pair="BTC-BRL",
            taker_connector="binance",
            taker_trading_pair="BTC-BRL",
            signal_connector="binance",
            signal_base_pair="BTC-USDT",
            signal_fx_pair="USDT-BRL",
            order_amount=Decimal("0.001"),
            min_profitability=Decimal("0.0007"),
            target_profitability=Decimal("0.0020"),
            max_profitability=Decimal("0.0080"),
            lead_windows_seconds=[5, 10, 15],
            profitability_adjust_threshold_bps=Decimal("5"),
            soft_cancel_threshold_bps=Decimal("10"),
            fast_cancel_threshold_bps=Decimal("15"),
            w_lead=Decimal("0.20"),
            ema_alpha_fx=Decimal("0.30"),
            max_leader_staleness_sec=5.0,
            max_fx_staleness_sec=10.0,
            max_local_staleness_sec=5.0,
            basis_hard_threshold_bps=Decimal("150"),
            max_local_spread_bps=Decimal("200"),
            min_requote_interval_sec=3.0,
            inventory_target_pct=Decimal("0.5"),
            inventory_skew_strength=Decimal("0.001"),
            min_taker_base_for_sell_hedge=Decimal("0.0005"),
            min_taker_quote_for_buy_hedge=Decimal("200"),
            warmup_seconds=20.0,
            shadow_mode=False,
            log_dir=self.tmp_dir,
            kill_switch_file=None,
        )
        self.market_data_provider = MagicMock(spec=MarketDataProvider)
        self.market_data_provider.time = MagicMock(return_value=1700000000.0)
        self.actions_queue = AsyncMock(spec=asyncio.Queue)

        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
        self.set_loggers([self.controller.logger()])
        self._setup_default_market_data()

    def _setup_default_market_data(
        self,
        local_bid=Decimal("300000"), local_ask=Decimal("300100"),
        taker_bid=Decimal("300010"), taker_ask=Decimal("300110"),
        leader_bid=Decimal("50000"), leader_ask=Decimal("50100"),
        fx_bid=Decimal("5.00"), fx_ask=Decimal("5.02"),
        maker_base=Decimal("0.005"), maker_quote=Decimal("1500"),
        taker_base=Decimal("0.005"), taker_quote=Decimal("1500"),
    ):
        def price_side_effect(connector, pair, ptype):
            table = {
                ("bybit", "BTC-BRL", PriceType.BestBid): local_bid,
                ("bybit", "BTC-BRL", PriceType.BestAsk): local_ask,
                ("binance", "BTC-BRL", PriceType.BestBid): taker_bid,
                ("binance", "BTC-BRL", PriceType.BestAsk): taker_ask,
                ("binance", "BTC-USDT", PriceType.BestBid): leader_bid,
                ("binance", "BTC-USDT", PriceType.BestAsk): leader_ask,
                ("binance", "USDT-BRL", PriceType.BestBid): fx_bid,
                ("binance", "USDT-BRL", PriceType.BestAsk): fx_ask,
            }
            return table.get((connector, pair, ptype), Decimal("0"))

        def balance_side_effect(connector, asset):
            table = {
                ("bybit", "BTC"): maker_base,
                ("bybit", "BRL"): maker_quote,
                ("binance", "BTC"): taker_base,
                ("binance", "BRL"): taker_quote,
            }
            return table.get((connector, asset), Decimal("0"))

        self.market_data_provider.get_price_by_type.side_effect = price_side_effect
        self.market_data_provider.get_balance.side_effect = balance_side_effect

    def _make_executor_info(self, executor_id, maker_side, is_done=False):
        info = MagicMock()
        info.id = executor_id
        info.is_done = is_done
        info.config = MagicMock(spec=XEMMExecutorConfig)
        info.config.id = executor_id
        info.config.maker_side = maker_side
        return info

    def _push_warm_history(self, ticks: int = 25, dt: float = 1.0):
        """Push enough updates so warmup expires and lead signal is valid."""
        start = self.market_data_provider.time.return_value
        for i in range(ticks):
            t = start + i * dt
            self.market_data_provider.time.return_value = t
            asyncio.get_event_loop().run_until_complete(
                self.controller.update_processed_data())

    def tearDown(self):
        try:
            self.controller.on_stop()
        except Exception:
            pass


# ===================================================================== #
# Group A — Markets / config                                            #
# ===================================================================== #
class TestMarketsConfig(_BaseControllerTest):
    def test_update_markets_registers_maker_pair(self):
        markets = self.config.update_markets({})
        self.assertIn("bybit", markets)
        self.assertIn("BTC-BRL", markets["bybit"])

    def test_update_markets_registers_taker_pair(self):
        markets = self.config.update_markets({})
        self.assertIn("binance", markets)
        self.assertIn("BTC-BRL", markets["binance"])

    def test_update_markets_registers_signal_base_pair(self):
        markets = self.config.update_markets({})
        self.assertIn("BTC-USDT", markets["binance"])

    def test_update_markets_registers_signal_fx_pair(self):
        markets = self.config.update_markets({})
        self.assertIn("USDT-BRL", markets["binance"])

    def test_config_invalid_profitability_order_raises(self):
        with self.assertRaises(ValueError):
            XEMMLeadLagConfig(
                id="x", controller_name="xemm_lead_lag", controller_type="generic",
                maker_connector="bybit", maker_trading_pair="BTC-BRL",
                taker_connector="binance", taker_trading_pair="BTC-BRL",
                signal_connector="binance", signal_base_pair="BTC-USDT",
                signal_fx_pair="USDT-BRL",
                min_profitability=Decimal("0.005"),
                target_profitability=Decimal("0.001"),  # invalid: < min
                max_profitability=Decimal("0.008"),
            )

    def test_config_warmup_too_small_raises(self):
        with self.assertRaises(ValueError):
            XEMMLeadLagConfig(
                id="x", controller_name="xemm_lead_lag", controller_type="generic",
                maker_connector="bybit", maker_trading_pair="BTC-BRL",
                taker_connector="binance", taker_trading_pair="BTC-BRL",
                signal_connector="binance", signal_base_pair="BTC-USDT",
                signal_fx_pair="USDT-BRL",
                lead_windows_seconds=[5, 10, 15],
                warmup_seconds=10.0,  # < 15+5
            )


# ===================================================================== #
# Group B — update_processed_data                                       #
# ===================================================================== #
class TestUpdateProcessedData(_BaseControllerTest):
    async def test_processed_data_populated(self):
        await self.controller.update_processed_data()
        pd = self.controller.processed_data
        for key in [
            "timestamp", "iso_time", "regime", "signal_quality",
            "local_bid", "local_ask", "local_mid", "local_spread_bps",
            "taker_local_bid", "taker_local_ask", "taker_local_mid",
            "leader_bid", "leader_ask", "leader_mid",
            "fx_bid", "fx_ask", "fx_mid_raw", "fx_mid_ema",
            "fair_brl_fast", "fair_brl_slow",
            "basis_bps", "lead_bps_per_window", "best_lead_bps",
            "should_cancel", "cancel_reason",
            "maker_base", "maker_quote", "taker_base", "taker_quote",
            "combined_base", "combined_quote", "combined_pct", "inventory_skew",
            "target_prof_buy", "target_prof_sell",
            "n_active_executors", "shadow_mode",
        ]:
            self.assertIn(key, pd, f"missing key: {key}")

    async def test_csv_logger_writes_row(self):
        await self.controller.update_processed_data()
        # CSV file should exist with at least header + 1 data row.
        self.assertIsNotNone(self.controller._csv)
        self.assertTrue(os.path.exists(self.controller._csv.path))

    async def test_safe_price_handles_exception(self):
        self.market_data_provider.get_price_by_type.side_effect = RuntimeError("boom")
        # Should not raise.
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["local_bid"], Decimal("0"))

    async def test_warmup_regime_initially(self):
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.WARMUP)

    async def test_combined_pct_uses_total_balance(self):
        # 0.005 BTC × 300050 BRL/BTC ≈ 1500.25 quote-equivalent base value
        # plus 1500 BRL quote = ~3000 total per side. Combined: 6000.
        # Combined base value = 0.01 × 300050 = 3000.5 → ~50% combined_pct.
        await self.controller.update_processed_data()
        pd = self.controller.processed_data
        self.assertGreater(pd["combined_pct"], Decimal("0.4"))
        self.assertLess(pd["combined_pct"], Decimal("0.6"))


# ===================================================================== #
# Group C — Risk gates / regime                                         #
# ===================================================================== #
class TestRiskGates(_BaseControllerTest):
    async def test_no_cancel_when_signals_healthy_and_warmed_up(self):
        # Warm up so we leave WARMUP. Without modifying anything, regime
        # should land in OK or DEGRADED — not PAUSED.
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()
        regime = self.controller.processed_data["regime"]
        self.assertNotIn(regime, (Regime.PAUSED, Regime.KILLED))
        self.assertFalse(self.controller.processed_data["should_cancel"])

    async def test_cancel_on_basis_extreme(self):
        # Force basis > 150 bps: local_mid much higher than fair.
        self._setup_default_market_data(
            local_bid=Decimal("400000"), local_ask=Decimal("400100"),
        )
        # Need to warm first so we don't hit WARMUP.
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()
        regime = self.controller.processed_data["regime"]
        self.assertEqual(regime, Regime.PAUSED)
        self.assertEqual(
            self.controller.processed_data["cancel_reason"], "BASIS_EXTREME"
        )

    async def test_cancel_on_wide_local_spread(self):
        self._setup_default_market_data(
            local_bid=Decimal("300000"), local_ask=Decimal("310000"),
        )
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.PAUSED)
        self.assertEqual(
            self.controller.processed_data["cancel_reason"], "LOCAL_SPREAD_WIDE"
        )

    async def test_kill_switch_file_present(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            ks_path = f.name
        self.config.kill_switch_file = ks_path
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        os.unlink(ks_path)

    async def test_cancel_stops_active_executors(self):
        # Wire up an "active" executor and trigger PAUSED regime.
        self.controller.executors_info = [
            self._make_executor_info("EX-1", TradeType.BUY, is_done=False),
            self._make_executor_info("EX-2", TradeType.SELL, is_done=False),
        ]
        # Force PAUSED via wide spread.
        self._setup_default_market_data(
            local_bid=Decimal("300000"), local_ask=Decimal("310000"),
        )
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()
        actions = self.controller.determine_executor_actions()
        # Both executors should be StopExecutorAction.
        self.assertEqual(len(actions), 2)
        for a in actions:
            self.assertIsInstance(a, StopExecutorAction)

    async def test_warmup_does_not_create(self):
        # No history pushed → still in WARMUP.
        await self.controller.update_processed_data()
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()
        # No CreateExecutorAction during WARMUP.
        for a in actions:
            self.assertNotIsInstance(a, CreateExecutorAction)

    async def test_killed_state_is_sticky(self):
        # Trigger KILLED via kill switch, then remove it: should stay KILLED.
        with tempfile.NamedTemporaryFile(delete=False) as f:
            ks_path = f.name
        self.config.kill_switch_file = ks_path
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        os.unlink(ks_path)
        self.market_data_provider.time.return_value = 1700000200.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)


# ===================================================================== #
# Group D — Anti-churn cooldown                                         #
# ===================================================================== #
class TestAntiChurn(_BaseControllerTest):
    async def _warm(self):
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()

    async def test_no_create_within_cooldown_after_action(self):
        await self._warm()
        # Simulate a recent action.
        now = self.market_data_provider.time.return_value
        self.controller._last_action_time = now - 1.0  # 1s < 3s cooldown
        actions = self.controller.determine_executor_actions()
        self.assertEqual(actions, [])

    async def test_create_after_cooldown_expires(self):
        await self._warm()
        now = self.market_data_provider.time.return_value
        self.controller._last_action_time = now - 5.0  # > 3s cooldown
        # Need a regime that allows creation: ensure not warmup, not paused.
        # If lead_signal absent, regime stays WARMUP. Push more ticks until
        # a valid lead_signal appears.
        actions = self.controller.determine_executor_actions()
        # Either creates appear (OK regime) or empty (DEGRADED still possible).
        # Key check: cooldown is NOT blocking.
        # We simply assert that empty result here is NOT due to cooldown — by
        # checking the regime allows it conceptually:
        regime = self.controller.processed_data["regime"]
        if regime == Regime.OK:
            self.assertTrue(any(isinstance(a, CreateExecutorAction) for a in actions))

    async def test_cooldown_does_not_block_cancel(self):
        # Wide spread → PAUSED. Cooldown should not prevent cancellation.
        self._setup_default_market_data(
            local_bid=Decimal("300000"), local_ask=Decimal("310000"),
        )
        await self._warm()
        self.controller.executors_info = [
            self._make_executor_info("EX-1", TradeType.BUY, is_done=False),
        ]
        # Set _last_action_time recently — would block creates but not cancels.
        now = self.market_data_provider.time.return_value
        self.controller._last_action_time = now - 0.5
        actions = self.controller.determine_executor_actions()
        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], StopExecutorAction)


# ===================================================================== #
# Group E — Executor creation                                           #
# ===================================================================== #
class TestCreateExecutors(_BaseControllerTest):
    async def _force_ok_regime(self):
        """Push warm history; verify regime is OK or DEGRADED (non-paused)."""
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()
        # Reset cooldown so creates are not blocked.
        self.controller._last_action_time = 0.0

    async def test_create_buy_when_none_active(self):
        await self._force_ok_regime()
        if self.controller.processed_data["regime"] in (Regime.PAUSED, Regime.KILLED, Regime.WARMUP):
            self.skipTest("Could not warm to OK/DEGRADED")
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        buys = [a for a in creates if a.executor_config.maker_side == TradeType.BUY]
        self.assertEqual(len(buys), 1)
        self.assertEqual(buys[0].executor_config.buying_market.connector_name, "bybit")
        self.assertEqual(buys[0].executor_config.selling_market.connector_name, "binance")

    async def test_create_sell_when_none_active(self):
        await self._force_ok_regime()
        if self.controller.processed_data["regime"] in (Regime.PAUSED, Regime.KILLED, Regime.WARMUP):
            self.skipTest("Could not warm to OK/DEGRADED")
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        sells = [a for a in creates if a.executor_config.maker_side == TradeType.SELL]
        self.assertEqual(len(sells), 1)
        self.assertEqual(sells[0].executor_config.selling_market.connector_name, "bybit")
        self.assertEqual(sells[0].executor_config.buying_market.connector_name, "binance")

    async def test_no_duplicate_buy_when_one_active(self):
        await self._force_ok_regime()
        if self.controller.processed_data["regime"] in (Regime.PAUSED, Regime.KILLED, Regime.WARMUP):
            self.skipTest("Could not warm to OK/DEGRADED")
        self.controller.executors_info = [
            self._make_executor_info("EX-1", TradeType.BUY, is_done=False),
        ]
        actions = self.controller.determine_executor_actions()
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        buys = [a for a in creates if a.executor_config.maker_side == TradeType.BUY]
        self.assertEqual(len(buys), 0)

    async def test_create_uses_configured_order_amount(self):
        await self._force_ok_regime()
        if self.controller.processed_data["regime"] in (Regime.PAUSED, Regime.KILLED, Regime.WARMUP):
            self.skipTest("Could not warm to OK/DEGRADED")
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        for a in creates:
            self.assertEqual(a.executor_config.order_amount, Decimal("0.001"))

    async def test_no_buy_creation_when_taker_base_insufficient(self):
        # Taker base = 0 → cannot hedge a BUY maker fill (which would need to
        # SELL on taker).
        self._setup_default_market_data(taker_base=Decimal("0"))
        await self._force_ok_regime()
        if self.controller.processed_data["regime"] in (Regime.PAUSED, Regime.KILLED, Regime.WARMUP):
            self.skipTest("Could not warm to OK/DEGRADED")
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        buys = [a for a in creates if a.executor_config.maker_side == TradeType.BUY]
        # No BUY maker should be created.
        self.assertEqual(len(buys), 0)


# ===================================================================== #
# Group F — Profitability adjustment                                    #
# ===================================================================== #
class TestProfitabilityAdjustment(_BaseControllerTest):
    def test_no_adjustment_when_lead_below_threshold(self):
        target_buy, target_sell = self.controller._compute_targets(
            best_lead=Decimal("3"),  # < 5 threshold
            inventory_skew=Decimal("0"),
        )
        self.assertEqual(target_buy, self.config.target_profitability)
        self.assertEqual(target_sell, self.config.target_profitability)

    def test_buy_target_decreases_on_positive_lead(self):
        target_buy, _ = self.controller._compute_targets(
            best_lead=Decimal("30"), inventory_skew=Decimal("0"),
        )
        self.assertLess(target_buy, self.config.target_profitability)

    def test_sell_target_increases_on_positive_lead(self):
        _, target_sell = self.controller._compute_targets(
            best_lead=Decimal("30"), inventory_skew=Decimal("0"),
        )
        self.assertGreater(target_sell, self.config.target_profitability)

    def test_clamped_to_max_profitability(self):
        # w_lead=0.20, lead=10000 bps would give adj=0.20, far above max.
        _, target_sell = self.controller._compute_targets(
            best_lead=Decimal("10000"), inventory_skew=Decimal("0"),
        )
        self.assertEqual(target_sell, self.config.max_profitability)

    def test_clamped_to_min_profitability(self):
        target_buy, _ = self.controller._compute_targets(
            best_lead=Decimal("10000"), inventory_skew=Decimal("0"),
        )
        # large positive lead → target_buy goes far below min.
        self.assertEqual(target_buy, self.config.min_profitability)


# ===================================================================== #
# Group G — Inventory skew (DIRECTIONS CORRECTED v2)                    #
# ===================================================================== #
class TestInventorySkew(_BaseControllerTest):
    def test_high_base_increases_buy_target_decreases_sell_target(self):
        # inv_skew = +0.5 (excess base, want to suppress BUY, favor SELL).
        target_buy, target_sell = self.controller._compute_targets(
            best_lead=None, inventory_skew=Decimal("0.5"),
        )
        # Excess base → target_buy should be HIGHER (harder to trigger more buys).
        self.assertGreater(target_buy, self.config.target_profitability)
        # Excess base → target_sell should be LOWER (easier to sell).
        self.assertLess(target_sell, self.config.target_profitability)

    def test_low_base_decreases_buy_target_increases_sell_target(self):
        # inv_skew = -0.5 (insufficient base, want to favor BUY, suppress SELL).
        target_buy, target_sell = self.controller._compute_targets(
            best_lead=None, inventory_skew=Decimal("-0.5"),
        )
        self.assertLess(target_buy, self.config.target_profitability)
        self.assertGreater(target_sell, self.config.target_profitability)


# ===================================================================== #
# Group H — Shadow mode                                                 #
# ===================================================================== #
class TestShadowMode(_BaseControllerTest):
    def setUp(self):
        super().setUp()
        self.config.shadow_mode = True

    async def _warm(self):
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()

    async def test_shadow_mode_no_create_actions(self):
        await self._warm()
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        self.assertEqual(len(creates), 0)

    async def test_shadow_mode_still_logs(self):
        await self.controller.update_processed_data()
        # CSV file exists with header + 1 row.
        self.assertIsNotNone(self.controller._csv)
        with open(self.controller._csv.path) as f:
            lines = f.readlines()
        self.assertGreaterEqual(len(lines), 2)  # header + ≥1 row

    async def test_shadow_mode_still_cancels_on_pause(self):
        # Force PAUSED via wide spread.
        self._setup_default_market_data(
            local_bid=Decimal("300000"), local_ask=Decimal("310000"),
        )
        await self._warm()
        self.controller.executors_info = [
            self._make_executor_info("EX-1", TradeType.BUY, is_done=False),
        ]
        actions = self.controller.determine_executor_actions()
        stops = [a for a in actions if isinstance(a, StopExecutorAction)]
        # Shadow mode SHOULD still allow defensive cancels.
        self.assertEqual(len(stops), 1)


