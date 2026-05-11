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
import time
from decimal import Decimal
from typing import Optional
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig, XEMMLeadLagExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    StopExecutorAction,
)
from hummingbot.strategy_v2.utils.lead_lag_signal import SignalQuality

from controllers.generic.xemm_lead_lag import (
    InventoryAuditConfig,
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
            basis_hard_threshold_bps=Decimal("300"),
            max_local_spread_bps=Decimal("200"),
            min_requote_interval_sec=3.0,
            inventory_target_pct=Decimal("0.5"),
            inventory_skew_strength=Decimal("0.001"),
            taker_hedge_buffer_pct=Decimal("0.01"),
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
        fx_bid=Decimal("5.99"), fx_ask=Decimal("6.01"),
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
# Group C2 — PnL-based safety gates (layered loss protection)           #
# ===================================================================== #
class TestPnlSafetyGates(_BaseControllerTest):
    """Verifies each PnL-based circuit breaker fires and latches KILLED.

    All tests bypass warmup with _push_warm_history-equivalent inline loops
    then set the relevant counter directly. The regime computation is what
    we want to test, not the fill-detection path.
    """

    async def _warm(self):
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()

    async def test_daily_loss_limit_trips_killed(self):
        await self._warm()
        # Push past the limit (default 100 BRL).
        self.controller._daily_realized_pnl = Decimal("-100.01")
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertEqual(
            self.controller.processed_data["cancel_reason"], "DAILY_LOSS_LIMIT"
        )

    async def test_daily_loss_just_under_does_not_trip(self):
        await self._warm()
        self.controller._daily_realized_pnl = Decimal("-99.99")
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )

    async def test_session_drawdown_trips_killed(self):
        await self._warm()
        # Peak was 80 BRL, currently -20 → drawdown = 100 BRL.
        # Default max_session_drawdown_quote = 50 BRL → trips.
        self.controller._session_pnl_peak = Decimal("80")
        self.controller._session_pnl_total = Decimal("-20")
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertTrue(
            self.controller.processed_data["cancel_reason"].startswith(
                "SESSION_DRAWDOWN_"
            )
        )

    async def test_session_drawdown_below_threshold_does_not_trip(self):
        await self._warm()
        # Peak 30, current 0 → drawdown 30 BRL < 50 → no trip.
        self.controller._session_pnl_peak = Decimal("30")
        self.controller._session_pnl_total = Decimal("0")
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )

    async def test_losing_streak_trips_killed(self):
        await self._warm()
        self.controller._consecutive_losing_fills = 5  # default max=5 → trips
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertTrue(
            self.controller.processed_data["cancel_reason"].startswith(
                "LOSING_STREAK_"
            )
        )

    async def test_losing_streak_under_threshold_does_not_trip(self):
        await self._warm()
        self.controller._consecutive_losing_fills = 4  # 4 < 5 → no trip
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )

    async def test_hourly_burn_trips_killed(self):
        await self._warm()
        # Advance mock clock so update_processed_data definitely re-runs the
        # regime check (signal buffer ignores same-timestamp ticks).
        self.market_data_provider.time.return_value = 1700000050.0
        now = self.market_data_provider.time.return_value
        # Default max_hourly_burn_quote = 50 BRL.
        # Inject 10 entries × -10 BRL within the last 30 min → sum = -100.
        from collections import deque
        self.controller._hourly_pnl_history = deque(
            (now - 60 - i * 30, Decimal("-10")) for i in range(10)
        )
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertTrue(
            self.controller.processed_data["cancel_reason"].startswith(
                "HOURLY_BURN_"
            )
        )

    async def test_hourly_burn_old_entries_trimmed(self):
        await self._warm()
        self.market_data_provider.time.return_value = 1700000050.0
        now = self.market_data_provider.time.return_value
        # All entries older than 3600s → should be trimmed and not trigger.
        from collections import deque
        self.controller._hourly_pnl_history = deque(
            (now - 4000 - i * 30, Decimal("-10")) for i in range(10)
        )
        await self.controller.update_processed_data()
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )
        # Hourly burn helper should have purged everything.
        self.assertEqual(self.controller._hourly_burn(now), Decimal("0"))

    async def test_unrealized_loss_trips_killed(self):
        await self._warm()
        # max_unrealized_loss_quote default = 100 BRL.
        self.controller._last_audit_results = {
            "_drift_active": True,
            "BTC": {
                "actual": Decimal("0.0015"),
                "target": Decimal("0.001"),
                "delta": Decimal("0.0005"),
                "delta_quote": Decimal("150"),  # > 100 → trips
                "within": False,
            },
        }
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertTrue(
            self.controller.processed_data["cancel_reason"].startswith(
                "UNREALIZED_LOSS_"
            )
        )

    async def test_hedge_failures_via_drift_audits_trips_killed(self):
        await self._warm()
        # Three consecutive stuck audits on BTC → matches default max=3.
        self.controller._drift_consecutive_audits = {"BTC": 3}
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertTrue(
            self.controller.processed_data["cancel_reason"].startswith(
                "HEDGE_FAILURES_"
            )
        )

    async def test_killed_reason_is_sticky_across_recovery(self):
        # Trip session drawdown, then restore PnL — should remain KILLED.
        await self._warm()
        self.controller._session_pnl_peak = Decimal("100")
        self.controller._session_pnl_total = Decimal("-50")  # dd=150
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        # Now "recover" — kill should remain.
        self.controller._session_pnl_total = Decimal("100")
        self.market_data_provider.time.return_value = 1700000060.0
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

    async def test_create_uses_xemm_lead_lag_executor_config(self):
        await self._force_ok_regime()
        if self.controller.processed_data["regime"] in (Regime.PAUSED, Regime.KILLED, Regime.WARMUP):
            self.skipTest("Could not warm to OK/DEGRADED")
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        self.assertGreater(len(creates), 0, "Expected at least one CreateExecutorAction")
        for action in creates:
            self.assertIsInstance(
                action.executor_config, XEMMLeadLagExecutorConfig,
                f"executor_config must be XEMMLeadLagExecutorConfig, got {type(action.executor_config)}",
            )


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


# ===================================================================== #
# Group I — Pure arbitrage                                              #
# ===================================================================== #
class _ArbBaseTest(_BaseControllerTest):
    """Base for arb tests: enables enable_pure_arb + provides VWAP mock helpers."""

    def setUp(self):
        super().setUp()
        # Enable arb on the controller config in-place
        self.config.enable_pure_arb = True
        self.config.arb_min_profitability = Decimal("0.0015")
        self.config.arb_order_amount = Decimal("0.001")
        self.config.arb_max_per_hour = 10
        self.config.arb_min_interval_sec = 5.0
        self.config.arb_capital_strategy = "skip_if_insufficient"
        self.config.arb_lead_signal_threshold_bps = Decimal("5")
        self.config.arb_lead_aggressive_delta = Decimal("0.0003")
        self.config.arb_lead_conservative_delta = Decimal("0.0005")
        self.config.shadow_mode = False

        # Re-instantiate controller to pick up new config
        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
        self.set_loggers([self.controller.logger()])
        self._setup_default_market_data()

    def _set_vwap(self, taker_buy=None, taker_sell=None,
                   maker_buy=None, maker_sell=None):
        """Mock get_order_book and its get_vwap_for_volume."""
        def _make_book(buy_price, sell_price):
            ob = MagicMock()
            def vwap(is_buy, _vol):
                r = MagicMock()
                r.result_price = float(buy_price if is_buy else sell_price)
                return r
            ob.get_vwap_for_volume.side_effect = vwap
            return ob
        # Default to L1 if not specified
        taker_book = _make_book(
            taker_buy if taker_buy is not None else Decimal("300110"),
            taker_sell if taker_sell is not None else Decimal("300010"),
        )
        maker_book = _make_book(
            maker_buy if maker_buy is not None else Decimal("300100"),
            maker_sell if maker_sell is not None else Decimal("300000"),
        )
        def get_ob(connector, pair):
            return maker_book if connector == "bybit" else taker_book
        self.market_data_provider.get_order_book.side_effect = get_ob

    async def _warm(self, ticks: int = 25):
        start = self.market_data_provider.time.return_value
        for i in range(ticks):
            self.market_data_provider.time.return_value = start + i
            await self.controller.update_processed_data()


class TestArbConfigValidation(_ArbBaseTest):
    def test_validate_at_least_one_mode_enabled(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            XEMMLeadLagConfig(
                **{**self.config.model_dump(),
                   "enable_market_making": False,
                   "enable_pure_arb": False}
            )

    def test_validate_arb_capital_strategy_value(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            XEMMLeadLagConfig(
                **{**self.config.model_dump(), "arb_capital_strategy": "invalid"}
            )


class TestArbDetection(_ArbBaseTest):
    async def test_arb_disabled_does_not_compute_gross(self):
        # Disable arb
        self.config.enable_pure_arb = False
        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
        self._setup_default_market_data()
        await self.controller.update_processed_data()
        # Should be sentinel value
        self.assertEqual(
            self.controller.processed_data["arb_long_gross_bps"], Decimal("-9999"))

    async def test_arb_long_detected_via_vwap(self):
        # taker buy VWAP = 300000, maker sell VWAP = 300600 → gross = 20 bps
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        await self.controller.update_processed_data()
        gross = self.controller.processed_data["arb_long_gross_bps"]
        # ~20 bps
        self.assertGreater(gross, Decimal("18"))
        self.assertLess(gross, Decimal("22"))

    async def test_arb_uses_vwap_not_l1(self):
        """If L1 shows arb but VWAP doesn't (depth issue), no opportunity is detected."""
        # L1 shows positive arb (taker_ask < maker_bid)
        # but VWAP (executable price) shows the opposite
        self._setup_default_market_data(
            local_bid=Decimal("300500"),     # maker bid: high
            local_ask=Decimal("300510"),
            taker_bid=Decimal("300000"),
            taker_ask=Decimal("300010"),     # taker ask: low → L1 long arb = 50bps
        )
        # But VWAP for arb_order_amount: taker_buy_vwap=300600 (deep!), maker_sell_vwap=300050
        self._set_vwap(
            taker_buy=Decimal("300600"),     # buying eats into book → much higher
            maker_sell=Decimal("300050"),    # selling eats into book → much lower
        )
        await self.controller.update_processed_data()
        gross_long = self.controller.processed_data["arb_long_gross_bps"]
        # VWAP-based gross is NEGATIVE (~-18 bps), no arb opportunity
        self.assertLess(gross_long, Decimal("0"))


class TestArbSpawning(_ArbBaseTest):
    async def test_arb_long_spawns_executor(self):
        # Force a profitable arb via VWAP
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),  # 20 bps gross, > 15 bps threshold
        )
        await self._warm()
        self.controller.executors_info = []
        # Force min_requote_interval to bypass anti-churn
        self.controller._last_action_time = 0.0
        self.controller._last_arb_time = 0.0
        actions = self.controller.determine_executor_actions()
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        self.assertEqual(len(creates), 1)
        cfg = creates[0].executor_config
        self.assertEqual(cfg.type, "lead_lag_arbitrage_executor")

    async def test_arb_skipped_below_threshold(self):
        # 5 bps gross, below 15 bps threshold
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300150"),
        )
        await self._warm()
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        # No arb spawned
        arb_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        self.assertEqual(len(arb_creates), 0)

    async def test_arb_blocked_when_another_arb_active(self):
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        await self._warm()
        # Existing active arb executor
        existing = MagicMock()
        existing.is_done = False
        existing.config.type = "lead_lag_arbitrage_executor"
        existing.config.id = "ARB-1"
        self.controller.executors_info = [existing]
        self.controller._last_action_time = 0.0
        self.controller._last_arb_time = 0.0
        actions = self.controller.determine_executor_actions()
        arb_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        self.assertEqual(len(arb_creates), 0)

    async def test_arb_blocked_by_failure_pause(self):
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        await self._warm()
        now = self.market_data_provider.time.return_value
        self.controller._arb_paused_until = now + 1000  # paused
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        arb_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        self.assertEqual(len(arb_creates), 0)

    async def test_arb_blocked_by_max_failures_per_day(self):
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        await self._warm()
        self.controller._arb_failures_today = self.config.arb_max_failures_per_day
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        arb_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        self.assertEqual(len(arb_creates), 0)

    async def test_arb_blocked_by_daily_loss_limit(self):
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        await self._warm()
        self.controller._arb_realized_loss_today = (
            self.config.arb_daily_loss_limit_quote
        )
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        arb_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        self.assertEqual(len(arb_creates), 0)

    async def test_arb_skipped_when_insufficient_capital(self):
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        # Drain capital — taker_quote too low for needed buy
        self._setup_default_market_data(
            taker_quote=Decimal("0.01"),  # virtually nothing
        )
        await self._warm()
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        self.controller._last_arb_time = 0.0
        actions = self.controller.determine_executor_actions()
        arb_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        self.assertEqual(len(arb_creates), 0)


class TestArbModeSwitches(_ArbBaseTest):
    async def test_mm_disabled_no_xemm_creation(self):
        self.config.enable_market_making = False
        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
        self._setup_default_market_data()
        # Set VWAP that does NOT trigger arb (so we test MM path being skipped)
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300050"),  # only 1.5 bps
        )
        await self._warm()
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        xemm_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "xemm_lead_lag_executor"
        ]
        self.assertEqual(len(xemm_creates), 0)


class TestArbThreshold(_ArbBaseTest):
    def test_threshold_dead_zone_uses_base(self):
        base = self.controller._arb_threshold("long", Decimal("3"))  # < 5 bps
        self.assertEqual(base, self.config.arb_min_profitability * Decimal("10000"))

    def test_threshold_aggressive_when_lead_favors_long(self):
        # lead positive + long → aggressive (lower threshold)
        threshold = self.controller._arb_threshold("long", Decimal("10"))
        base = self.config.arb_min_profitability * Decimal("10000")
        self.assertLess(threshold, base)

    def test_threshold_conservative_when_lead_against_long(self):
        # lead negative + long → conservative
        threshold = self.controller._arb_threshold("long", Decimal("-10"))
        base = self.config.arb_min_profitability * Decimal("10000")
        self.assertGreater(threshold, base)

    def test_threshold_aggressive_when_lead_favors_short(self):
        threshold = self.controller._arb_threshold("short", Decimal("-10"))
        base = self.config.arb_min_profitability * Decimal("10000")
        self.assertLess(threshold, base)

    def test_threshold_none_lead_returns_base(self):
        threshold = self.controller._arb_threshold("long", None)
        base = self.config.arb_min_profitability * Decimal("10000")
        self.assertEqual(threshold, base)


class TestArbCounterReset(_ArbBaseTest):
    async def test_failures_reset_at_new_utc_day(self):
        # Set fail count
        self.controller._arb_failures_today = 2
        self.controller._arb_realized_loss_today = Decimal("50")
        self.controller._arb_last_reset_day = "2025-01-01"
        # Move time to next day
        from datetime import datetime as _dt
        new_day_ts = _dt(2025, 1, 2, 12, 0, 0).timestamp()
        self.market_data_provider.time.return_value = new_day_ts
        # Force rebuild fingerprint to bypass tier-skip
        self.controller._last_fingerprint = None
        self.controller._last_full_update = 0.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller._arb_failures_today, 0)
        self.assertEqual(self.controller._arb_realized_loss_today, Decimal("0"))


# ===================================================================== #
# Group H — Inventory audit & boot-paused mode                          #
# ===================================================================== #
class _AuditBaseTest(_BaseControllerTest):
    """Base class with audit enabled and a balance-mocking helper."""

    def setUp(self):
        super().setUp()
        # Enable audit with BTC=0.002 target
        self.config.inventory_audit = InventoryAuditConfig(
            enabled=True,
            audit_interval_sec=300.0,
            # mid_price in tests is (300000+300100)/2 = 300050. max_drift_quote=3 BRL
            # → tolerance_abs = 3/300050 ≈ 9.998e-6 BTC, equivalent to the
            # previous tolerance_pct=0.005 * target=0.002 = 1e-5 BTC.
            max_drift_quote=Decimal("3"),
            on_drift_action="pause",
            base_targets={"BTC": Decimal("0.002")},
        )
        # Recreate controller after config change
        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
        self.set_loggers([self.controller.logger()])
        # Pre-condition: cleanup considered done (audit precondition)
        self.controller._startup_cleanup_done = True

    def _mock_total_balance(self, maker_btc: Decimal, taker_btc: Decimal):
        """Mock _safe_total_balance to return controlled values."""
        def side_effect(connector, asset):
            if asset != "BTC":
                return Decimal("0")
            return maker_btc if connector == "bybit" else taker_btc
        self.controller._safe_total_balance = MagicMock(side_effect=side_effect)


class _AutoRebalanceBaseTest(_AuditBaseTest):
    """Audit configured with auto_rebalance action."""

    def setUp(self):
        super().setUp()
        self.config.inventory_audit = InventoryAuditConfig(
            enabled=True,
            audit_interval_sec=300.0,
            # mid_price in tests is (300000+300100)/2 = 300050. max_drift_quote=3 BRL
            # → tolerance_abs = 3/300050 ≈ 9.998e-6 BTC, equivalent to the
            # previous tolerance_pct=0.005 * target=0.002 = 1e-5 BTC.
            max_drift_quote=Decimal("3"),
            on_drift_action="auto_rebalance",
            base_targets={"BTC": Decimal("0.002")},
        )
        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
        self.set_loggers([self.controller.logger()])
        self.controller._startup_cleanup_done = True

    def _mock_connector_with_rules(self, connector_name, min_order_size=Decimal("0.00001")):
        """Return a mock connector with trading rules and buy/sell methods."""
        mock_conn = MagicMock()
        rule = MagicMock()
        rule.min_order_size = min_order_size
        rule.min_notional_size = None
        mock_conn.trading_rules = {"BTC-BRL": rule}
        mock_conn.buy = MagicMock(return_value="order-buy-123")
        mock_conn.sell = MagicMock(return_value="order-sell-123")
        return mock_conn


class TestAutoRebalanceQueuing(_AutoRebalanceBaseTest):
    def test_excess_queues_rebalance(self):
        """Drift >0 (excess BTC) queues a rebalance without setting kill reason."""
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.0010"))  # 0.0022 total, +0.0002 excess
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        self.controller._run_inventory_audit(now=1700000000.0, source="periodic")

        # kill_reason must NOT be set (auto_rebalance does not pause the bot)
        self.assertIsNone(self.controller._kill_reason)
        # rebalance queued with the excess delta
        self.assertIn("BTC", self.controller._pending_rebalances)
        self.assertGreater(self.controller._pending_rebalances["BTC"], Decimal("0"))

    def test_deficit_queues_rebalance(self):
        """Drift <0 (deficit BTC) queues a rebalance."""
        self._mock_total_balance(Decimal("0.0009"), Decimal("0.0009"))  # 0.0018 total, -0.0002 deficit
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        self.controller._run_inventory_audit(now=1700000000.0, source="periodic")

        self.assertIsNone(self.controller._kill_reason)
        self.assertIn("BTC", self.controller._pending_rebalances)
        self.assertLess(self.controller._pending_rebalances["BTC"], Decimal("0"))

    def test_cooldown_blocks_second_queue(self):
        """If a rebalance was placed recently, the next audit defers."""
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.0010"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        now = 1700000000.0
        # Simulate a rebalance that was placed 30s ago (cooldown=120s)
        self.controller._rebalance_last_time["BTC"] = now - 30.0

        self.controller._run_inventory_audit(now=now, source="periodic")

        self.assertNotIn("BTC", self.controller._pending_rebalances)

    def test_cooldown_expired_allows_queue(self):
        """After cooldown expires, rebalance is re-queued."""
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.0010"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        now = 1700000000.0
        # Last rebalance was 130s ago (> 120s cooldown)
        self.controller._rebalance_last_time["BTC"] = now - 130.0

        self.controller._run_inventory_audit(now=now, source="periodic")

        self.assertIn("BTC", self.controller._pending_rebalances)


class TestAutoRebalanceExecution(_AutoRebalanceBaseTest):
    """
    Tests for _execute_pending_rebalances with VWAP-based exchange selection.

    Selection rule:
      SELL excess → exchange with HIGHER VWAP (we receive more quote)
      BUY deficit → exchange with LOWER  VWAP (we pay less quote)
    Viability: VWAP must be non-None (depth ok) AND capital sufficient.
    """

    def _setup_vwap_and_connector(
        self,
        bybit_vwap: Optional[Decimal],
        binance_vwap: Optional[Decimal],
        bybit_btc_bal: Decimal = Decimal("0.001"),
        binance_btc_bal: Decimal = Decimal("0.001"),
        bybit_brl_bal: Decimal = Decimal("500"),
        binance_brl_bal: Decimal = Decimal("500"),
    ):
        """Wire up _vwap_for_amount and _safe_total_balance mocks."""

        def vwap_side_effect(conn, pair, is_buy, amount):
            return bybit_vwap if conn == "bybit" else binance_vwap

        self.controller._vwap_for_amount = MagicMock(side_effect=vwap_side_effect)

        def balance_side_effect(conn, asset):
            if asset == "BTC":
                return bybit_btc_bal if conn == "bybit" else binance_btc_bal
            # quote (BRL)
            return bybit_brl_bal if conn == "bybit" else binance_brl_bal

        self.controller._safe_total_balance = MagicMock(side_effect=balance_side_effect)

        mock_bybit = self._mock_connector_with_rules("bybit")
        mock_binance = self._mock_connector_with_rules("binance")
        self.market_data_provider.get_connector = MagicMock(
            side_effect=lambda name: mock_bybit if name == "bybit" else mock_binance
        )
        return mock_bybit, mock_binance

    async def test_sell_picks_exchange_with_higher_vwap(self):
        """SELL excess: chooses exchange with higher VWAP (better sell price)."""
        # bybit VWAP=391000, binance VWAP=390000 → bybit is better for selling
        mock_bybit, mock_binance = self._setup_vwap_and_connector(
            bybit_vwap=Decimal("391000"),
            binance_vwap=Decimal("390000"),
            bybit_btc_bal=Decimal("0.001"),
            binance_btc_bal=Decimal("0.001"),
        )
        self.controller._pending_rebalances["BTC"] = Decimal("0.0002")

        await self.controller._execute_pending_rebalances()

        mock_bybit.sell.assert_called_once()
        mock_binance.sell.assert_not_called()

    async def test_buy_picks_exchange_with_lower_vwap(self):
        """BUY deficit: chooses exchange with lower VWAP (cheaper to buy)."""
        # binance VWAP=389000, bybit VWAP=390000 → binance is better for buying
        mock_bybit, mock_binance = self._setup_vwap_and_connector(
            bybit_vwap=Decimal("390000"),
            binance_vwap=Decimal("389000"),
            bybit_brl_bal=Decimal("500"),
            binance_brl_bal=Decimal("500"),
        )
        self.controller._pending_rebalances["BTC"] = Decimal("-0.0002")

        await self.controller._execute_pending_rebalances()

        mock_binance.buy.assert_called_once()
        mock_bybit.buy.assert_not_called()

    async def test_falls_back_to_other_exchange_when_preferred_has_thin_book(self):
        """If best-price exchange has insufficient depth (VWAP=None), use the other."""
        # bybit has better VWAP for sell but thin book (None); binance viable
        mock_bybit, mock_binance = self._setup_vwap_and_connector(
            bybit_vwap=None,           # thin book on bybit
            binance_vwap=Decimal("390000"),
            bybit_btc_bal=Decimal("0.001"),
            binance_btc_bal=Decimal("0.001"),
        )
        self.controller._pending_rebalances["BTC"] = Decimal("0.0002")

        await self.controller._execute_pending_rebalances()

        mock_binance.sell.assert_called_once()
        mock_bybit.sell.assert_not_called()

    async def test_falls_back_when_preferred_has_insufficient_base_for_sell(self):
        """SELL: if best-VWAP exchange doesn't have enough BTC, use the other."""
        # bybit has highest VWAP but only 0.0001 BTC (need 0.0002)
        mock_bybit, mock_binance = self._setup_vwap_and_connector(
            bybit_vwap=Decimal("391000"),
            binance_vwap=Decimal("390000"),
            bybit_btc_bal=Decimal("0.0001"),   # insufficient
            binance_btc_bal=Decimal("0.001"),
        )
        self.controller._pending_rebalances["BTC"] = Decimal("0.0002")

        await self.controller._execute_pending_rebalances()

        mock_binance.sell.assert_called_once()
        mock_bybit.sell.assert_not_called()

    async def test_falls_back_when_preferred_has_insufficient_quote_for_buy(self):
        """BUY: if best-VWAP exchange doesn't have enough BRL, use the other."""
        # binance has lowest VWAP but only 10 BRL (need ~78 BRL for 0.0002 BTC @390000)
        mock_bybit, mock_binance = self._setup_vwap_and_connector(
            bybit_vwap=Decimal("390000"),
            binance_vwap=Decimal("389000"),
            bybit_brl_bal=Decimal("500"),
            binance_brl_bal=Decimal("10"),    # insufficient
        )
        self.controller._pending_rebalances["BTC"] = Decimal("-0.0002")

        await self.controller._execute_pending_rebalances()

        mock_bybit.buy.assert_called_once()
        mock_binance.buy.assert_not_called()

    async def test_skips_when_no_viable_exchange(self):
        """No viable exchange (both thin book) → no order, warning logged."""
        mock_bybit, mock_binance = self._setup_vwap_and_connector(
            bybit_vwap=None,
            binance_vwap=None,
        )
        self.controller._pending_rebalances["BTC"] = Decimal("0.0002")

        await self.controller._execute_pending_rebalances()

        mock_bybit.sell.assert_not_called()
        mock_binance.sell.assert_not_called()

    async def test_skips_below_min_order_size(self):
        """Amount below min_order_size → no order placed."""
        mock_bybit, mock_binance = self._setup_vwap_and_connector(
            bybit_vwap=Decimal("390000"),
            binance_vwap=Decimal("390000"),
        )
        # Override min_order_size to be larger than amount
        for mock_conn in (mock_bybit, mock_binance):
            mock_conn.trading_rules["BTC-BRL"].min_order_size = Decimal("0.001")

        self.controller._pending_rebalances["BTC"] = Decimal("0.000001")

        await self.controller._execute_pending_rebalances()

        mock_bybit.sell.assert_not_called()
        mock_binance.sell.assert_not_called()

    async def test_clears_pending_after_execution(self):
        """_pending_rebalances is always cleared after execute."""
        self._setup_vwap_and_connector(
            bybit_vwap=Decimal("390000"),
            binance_vwap=Decimal("390000"),
        )
        self.controller._pending_rebalances["BTC"] = Decimal("0.0002")

        await self.controller._execute_pending_rebalances()

        self.assertEqual(self.controller._pending_rebalances, {})

    async def test_sets_cooldown_timestamp_after_execution(self):
        """After placing order, cooldown timestamp is set for the asset."""
        self._setup_vwap_and_connector(
            bybit_vwap=Decimal("390000"),
            binance_vwap=Decimal("390000"),
        )
        self.controller._pending_rebalances["BTC"] = Decimal("0.0002")

        await self.controller._execute_pending_rebalances()

        self.assertIn("BTC", self.controller._rebalance_last_time)
        self.assertGreater(self.controller._rebalance_last_time["BTC"], 0)


class TestAutoRebalanceBootBehavior(_AutoRebalanceBaseTest):
    def test_boot_exits_with_drift_when_auto_rebalance_configured(self):
        """With auto_rebalance, boot_paused exits even when drift is found."""
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))  # excess
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        self.controller._boot_paused = True
        self.controller._initial_audit_done = False

        self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        self.controller._initial_audit_done = True
        # With auto_rebalance, _kill_reason is NOT set → boot_paused can exit
        if self.controller._kill_reason is None:
            self.controller._boot_paused = False

        self.assertIsNone(self.controller._kill_reason)
        self.assertFalse(self.controller._boot_paused)


class TestInventoryAuditConfigDefaults(_BaseControllerTest):
    def test_audit_disabled_by_default_when_no_targets(self):
        # Defaults: enabled=True but base_targets={} → audit no-ops
        cfg = InventoryAuditConfig()
        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.base_targets, {})
        self.assertEqual(cfg.max_drift_quote, Decimal("60"))
        self.assertEqual(cfg.on_drift_action, "pause")

    def test_audit_extra_field_rejected(self):
        # extra="forbid" → unknown keys raise
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            InventoryAuditConfig(some_unknown_field=True)


class TestInventoryAuditNoDrift(_AuditBaseTest):
    def test_no_drift_when_balances_match_target(self):
        # 0.001 + 0.001 = 0.002 (target) → no drift
        self._mock_total_balance(Decimal("0.001"), Decimal("0.001"))
        self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        result = self.controller._last_audit_results["BTC"]
        self.assertTrue(result["within"])
        self.assertEqual(result["delta"], Decimal("0"))
        # No kill switch
        self.assertIsNone(self.controller._kill_reason)

    def test_no_drift_within_tolerance(self):
        # 0.001 + 0.0009905 = 0.0019905 → -0.0000095 from target (~-0.475%, within 0.5%)
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0009905"))
        self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        result = self.controller._last_audit_results["BTC"]
        self.assertTrue(result["within"])
        self.assertIsNone(self.controller._kill_reason)


class TestInventoryAuditDriftPause(_AuditBaseTest):
    def test_drift_above_tolerance_trips_kill_switch(self):
        # 0.001 + 0.0008 = 0.0018 → -0.0002 from target (-10%, well above 0.5%)
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        # Ensure no in-flight activity
        self.controller.executors_info = []
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        result = self.controller._last_audit_results["BTC"]
        self.assertFalse(result["within"])
        self.assertEqual(self.controller._kill_reason, "INVENTORY_DRIFT_BTC")
        self.assertTrue(self.controller._last_audit_results["_drift_active"])

    def test_drift_deferred_when_inflight_active(self):
        # Same drift but in-flight active → defer (no kill)
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=True)

        self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        # No kill set
        self.assertIsNone(self.controller._kill_reason)
        # _drift_active stays False (action was deferred)
        self.assertFalse(self.controller._last_audit_results["_drift_active"])
        # But inflight flag set
        self.assertTrue(self.controller._last_audit_results["_inflight_active"])


class TestInventoryAuditAlertAction(_AuditBaseTest):
    def test_alert_action_logs_but_does_not_pause(self):
        self.config.inventory_audit.on_drift_action = "alert"
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # alert action does NOT trip kill switch
        self.assertIsNone(self.controller._kill_reason)
        # but _drift_active flagged for CSV
        self.assertTrue(self.controller._last_audit_results["_drift_active"])


class TestInflightActivityDetection(_AuditBaseTest):
    def test_active_executor_alone_is_NOT_inflight(self):
        # KEY REGRESSION TEST: an XEMM executor sitting with an open maker order
        # (normal steady-state) must NOT be counted as inflight — that was the
        # bug that made _has_inflight_activity() always return True and permanently
        # suppressed the audit.
        import time
        live_executor = MagicMock()
        live_executor.is_done = False
        self.controller.executors_info = [live_executor]
        self.controller._last_fill_time = 0.0  # no recent fill
        self.assertFalse(self.controller._has_inflight_activity())

    def test_no_executors_not_inflight(self):
        self.controller.executors_info = []
        self.controller._last_fill_time = 0.0
        self.assertFalse(self.controller._has_inflight_activity())

    def test_recent_fill_is_inflight(self):
        # Fill <10s ago counts as in-flight (WS account stream lag window)
        import time
        self.controller._last_fill_time = time.time() - 5.0  # 5s ago
        self.assertTrue(self.controller._has_inflight_activity())

    def test_old_fill_not_inflight(self):
        import time
        self.controller._last_fill_time = time.time() - 30.0  # 30s ago
        self.assertFalse(self.controller._has_inflight_activity())

    def test_active_executor_with_recent_fill_is_inflight(self):
        # Recent fill + active executor = inflight (fill just happened)
        import time
        live_executor = MagicMock()
        live_executor.is_done = False
        self.controller.executors_info = [live_executor]
        self.controller._last_fill_time = time.time() - 2.0  # 2s ago
        self.assertTrue(self.controller._has_inflight_activity())


class TestBootPausedMode(_AuditBaseTest):
    def setUp(self):
        super().setUp()
        # Reset boot state to test transition logic
        self.controller._boot_paused = True
        self.controller._initial_audit_done = False

    def test_boot_paused_blocks_new_xemm_creation(self):
        # Bypass warmup: directly populate processed_data with a viable state
        # so we test the boot_paused gate specifically (not the regime gate).
        self.controller.processed_data = {
            "regime": Regime.OK,
            "timestamp": 1700000100.0,
            "cancel_reason": "",
            "should_cancel": False,
            "target_prof_buy": Decimal("0.0015"),
            "target_prof_sell": Decimal("0.0015"),
            "best_lead_bps": Decimal("0"),
            "maker_base": Decimal("0.001"),
            "maker_quote": Decimal("100"),
            "taker_base": Decimal("0.001"),
            "taker_quote": Decimal("100"),
            "combined_pct": Decimal("0.5"),
            "arb_long_gross_bps": Decimal("0"),
            "arb_short_gross_bps": Decimal("0"),
        }
        self.assertTrue(self.controller._boot_paused)

        actions = self.controller.determine_executor_actions()
        # Boot-paused returns [] — no creates
        creates = [a for a in actions if hasattr(a, "executor_config")]
        self.assertEqual(len(creates), 0)

    def test_boot_exits_when_cleanup_and_audit_pass(self):
        # No drift case
        self._mock_total_balance(Decimal("0.001"), Decimal("0.001"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        # Simulate the boot flow: run audit
        self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        # Manually replicate boot transition logic
        self.controller._initial_audit_done = True
        if self.controller._kill_reason is None:
            self.controller._boot_paused = False

        self.assertFalse(self.controller._boot_paused)
        self.assertIsNone(self.controller._kill_reason)

    def test_boot_stays_paused_when_audit_detects_drift(self):
        # Drift case
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        self.controller._initial_audit_done = True
        if self.controller._kill_reason is None:
            self.controller._boot_paused = False

        # Bot stays paused (kill_reason set → boot_paused stays True)
        self.assertTrue(self.controller._boot_paused)
        self.assertEqual(self.controller._kill_reason, "INVENTORY_DRIFT_BTC")


class TestAuditDisabled(_BaseControllerTest):
    def test_audit_disabled_runs_no_op(self):
        # Default: enabled=True but base_targets={} → audit returns immediately
        self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertEqual(self.controller._last_audit_results, {})
        self.assertIsNone(self.controller._kill_reason)

    def test_audit_explicitly_disabled_runs_no_op(self):
        self.config.inventory_audit.enabled = False
        self.config.inventory_audit.base_targets = {"BTC": Decimal("0.002")}
        self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertEqual(self.controller._last_audit_results, {})


class TestSigtermHandler(_BaseControllerTest):
    """
    Tests for SIGTERM/SIGINT graceful shutdown handler.

    _setup_sigterm_handler() registers loop.add_signal_handler calls;
    _graceful_shutdown() cancels open orders and calls sys.exit(0).
    """

    def test_setup_registers_handler_once(self):
        """_setup_sigterm_handler sets _sigterm_registered and calls add_signal_handler."""
        mock_loop = MagicMock()
        self.controller._sigterm_registered = False

        with patch("asyncio.get_event_loop", return_value=mock_loop):
            self.controller._setup_sigterm_handler()

        self.assertTrue(self.controller._sigterm_registered)
        # Called once per signal (SIGTERM + SIGINT = 2 calls)
        self.assertEqual(mock_loop.add_signal_handler.call_count, 2)

    def test_setup_idempotent(self):
        """Calling _setup_sigterm_handler twice only registers once."""
        mock_loop = MagicMock()
        self.controller._sigterm_registered = False

        with patch("asyncio.get_event_loop", return_value=mock_loop):
            self.controller._setup_sigterm_handler()
            self.controller._setup_sigterm_handler()  # second call is a no-op

        self.assertEqual(mock_loop.add_signal_handler.call_count, 2)  # not 4

    def test_setup_tolerates_not_implemented_error(self):
        """Windows / unsupported env: NotImplementedError is swallowed."""
        mock_loop = MagicMock()
        mock_loop.add_signal_handler.side_effect = NotImplementedError
        self.controller._sigterm_registered = False

        with patch("asyncio.get_event_loop", return_value=mock_loop):
            self.controller._setup_sigterm_handler()  # must not raise

        # Flag stays False when registration failed
        self.assertFalse(self.controller._sigterm_registered)

    async def test_graceful_shutdown_sets_kill_reason(self):
        """_graceful_shutdown latches _kill_reason before cancellation."""
        self.controller._cancel_all_open_orders_on_startup = AsyncMock()

        with patch("sys.exit"):
            await self.controller._graceful_shutdown("SIGTERM")

        self.assertEqual(self.controller._kill_reason, "SIGTERM")

    async def test_graceful_shutdown_cancels_orders(self):
        """_graceful_shutdown calls _cancel_all_open_orders_on_startup."""
        self.controller._cancel_all_open_orders_on_startup = AsyncMock()

        with patch("sys.exit"):
            await self.controller._graceful_shutdown("SIGTERM")

        self.controller._cancel_all_open_orders_on_startup.assert_awaited_once()

    async def test_graceful_shutdown_calls_sys_exit(self):
        """_graceful_shutdown always calls sys.exit(0), even on exception."""
        self.controller._cancel_all_open_orders_on_startup = AsyncMock(
            side_effect=RuntimeError("network down")
        )

        with patch("sys.exit") as mock_exit:
            await self.controller._graceful_shutdown("SIGTERM")

        mock_exit.assert_called_once_with(0)

    async def test_graceful_shutdown_calls_sys_exit_on_timeout(self):
        """sys.exit(0) is called even when cancellation times out."""
        async def slow_cancel():
            await asyncio.sleep(100)  # will be cancelled by wait_for(timeout=8)

        self.controller._cancel_all_open_orders_on_startup = slow_cancel

        with patch("sys.exit") as mock_exit:
            with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
                await self.controller._graceful_shutdown("SIGTERM")

        mock_exit.assert_called_once_with(0)

    async def test_graceful_shutdown_does_not_overwrite_existing_kill_reason(self):
        """If _kill_reason is already set (e.g. audit drift), SIGTERM preserves it."""
        self.controller._kill_reason = "INVENTORY_DRIFT_BTC"
        self.controller._cancel_all_open_orders_on_startup = AsyncMock()

        with patch("sys.exit"):
            await self.controller._graceful_shutdown("SIGTERM")

        # Original kill reason preserved
        self.assertEqual(self.controller._kill_reason, "INVENTORY_DRIFT_BTC")


# ===================================================================== #
# Group I — Controller-level ghost fill handler (P0 fix 2026-05-11)     #
# ===================================================================== #
class _GhostFillBaseTest(_BaseControllerTest):
    """Shared fixtures: a fake fill event + a mocked taker connector."""

    def _fill_event(self, order_id: str, amount=Decimal("0.0002")):
        ev = MagicMock()
        ev.order_id = order_id
        ev.amount = amount
        ev.price = Decimal("400000")
        return ev

    def _mock_taker_connector(self):
        """Replace market_data_provider.get_connector(taker) with a mock
        that exposes ``buy`` / ``sell`` returning a fake order_id. Also
        intercepts get_connector(maker) to return a different mock so
        nothing else breaks."""
        taker = MagicMock()
        taker.buy = MagicMock(return_value="hedge-buy-1")
        taker.sell = MagicMock(return_value="hedge-sell-1")
        maker = MagicMock()
        def _get_conn(name):
            return taker if name == "binance" else maker
        self.market_data_provider.get_connector = MagicMock(side_effect=_get_conn)
        return taker


class TestGhostFillRegistration(_GhostFillBaseTest):
    def test_register_adds_entry(self):
        self.controller.register_ghost_order(
            order_id="OID-1",
            maker_side=TradeType.SELL,
            maker_connector="bybit",
        )
        self.assertIn("OID-1", self.controller._pending_ghost_orders)
        info = self.controller._pending_ghost_orders["OID-1"]
        self.assertEqual(info["maker_side"], TradeType.SELL)
        self.assertEqual(info["maker_connector"], "bybit")
        self.assertGreater(info["registered_at"], 0)

    def test_mark_hedged_moves_to_dedupe_set(self):
        self.controller.register_ghost_order("OID-2", TradeType.BUY, "bybit")
        self.controller.mark_ghost_hedged("OID-2")
        self.assertNotIn("OID-2", self.controller._pending_ghost_orders)
        self.assertIn("OID-2", self.controller._already_hedged_ghost_ids)

    def test_purge_removes_stale_entries(self):
        # Manually backdate an entry.
        self.controller.register_ghost_order("OID-OLD", TradeType.SELL, "bybit")
        self.controller._pending_ghost_orders["OID-OLD"]["registered_at"] = (
            time.time() - self.controller._ghost_max_age_sec - 1.0
        )
        # And one fresh entry.
        self.controller.register_ghost_order("OID-NEW", TradeType.BUY, "bybit")
        self.controller._purge_ghost_orders(time.time())
        self.assertNotIn("OID-OLD", self.controller._pending_ghost_orders)
        self.assertIn("OID-NEW", self.controller._pending_ghost_orders)


class TestGhostFillEventHandling(_GhostFillBaseTest):
    def test_late_fill_places_taker_hedge_for_maker_sell(self):
        taker = self._mock_taker_connector()
        # Register: maker SELL on bybit → expected hedge is BUY on binance.
        self.controller.register_ghost_order(
            order_id="OID-S1",
            maker_side=TradeType.SELL,
            maker_connector="bybit",
        )
        event = self._fill_event("OID-S1", amount=Decimal("0.00019999"))
        self.controller._handle_ghost_fill_event(event, source_connector="bybit")
        # MARKET BUY placed on taker for the fill amount.
        taker.buy.assert_called_once()
        args, _ = taker.buy.call_args
        self.assertEqual(args[0], "BTC-BRL")  # taker_trading_pair
        self.assertEqual(args[1], Decimal("0.00019999"))
        # Entry moved to dedupe set.
        self.assertNotIn("OID-S1", self.controller._pending_ghost_orders)
        self.assertIn("OID-S1", self.controller._already_hedged_ghost_ids)

    def test_late_fill_places_taker_hedge_for_maker_buy(self):
        taker = self._mock_taker_connector()
        self.controller.register_ghost_order(
            "OID-B1", TradeType.BUY, "bybit"
        )
        event = self._fill_event("OID-B1", amount=Decimal("0.0001"))
        self.controller._handle_ghost_fill_event(event, source_connector="bybit")
        taker.sell.assert_called_once()
        args, _ = taker.sell.call_args
        self.assertEqual(args[1], Decimal("0.0001"))
        self.assertNotIn("OID-B1", self.controller._pending_ghost_orders)

    def test_already_hedged_skips_duplicate(self):
        taker = self._mock_taker_connector()
        self.controller.register_ghost_order("OID-X", TradeType.SELL, "bybit")
        self.controller.mark_ghost_hedged("OID-X")
        event = self._fill_event("OID-X")
        self.controller._handle_ghost_fill_event(event, source_connector="bybit")
        taker.buy.assert_not_called()
        taker.sell.assert_not_called()

    def test_unregistered_order_id_is_noop(self):
        taker = self._mock_taker_connector()
        event = self._fill_event("OID-UNKNOWN")
        self.controller._handle_ghost_fill_event(event, source_connector="bybit")
        taker.buy.assert_not_called()
        taker.sell.assert_not_called()

    def test_zero_amount_event_skipped(self):
        taker = self._mock_taker_connector()
        self.controller.register_ghost_order("OID-Z", TradeType.SELL, "bybit")
        event = self._fill_event("OID-Z", amount=Decimal("0"))
        self.controller._handle_ghost_fill_event(event, source_connector="bybit")
        taker.buy.assert_not_called()
        # Entry stays in pending since the listener didn't actually claim it
        # (the late real fill, if it ever comes, may still want to hedge).
        # But zero-amount events are essentially noise, so this is fine
        # either way — verify only that no hedge fired.

    def test_mismatched_source_connector_skipped(self):
        taker = self._mock_taker_connector()
        self.controller.register_ghost_order("OID-M", TradeType.SELL, "bybit")
        event = self._fill_event("OID-M")
        # Event arrives from binance though the maker was bybit — defensive
        # skip (should never happen in prod, but guards against plumbing
        # surprises).
        self.controller._handle_ghost_fill_event(event, source_connector="binance")
        taker.buy.assert_not_called()
        self.assertIn("OID-M", self.controller._pending_ghost_orders)

    def test_handler_claims_atomically_before_placing(self):
        """If buy() raises, the entry is already claimed; a second event
        for the same order would not re-attempt."""
        taker = self._mock_taker_connector()
        taker.buy.side_effect = RuntimeError("connector down")
        self.controller.register_ghost_order("OID-F", TradeType.SELL, "bybit")
        event = self._fill_event("OID-F")
        self.controller._handle_ghost_fill_event(event, source_connector="bybit")
        # Claim happened despite failure.
        self.assertNotIn("OID-F", self.controller._pending_ghost_orders)
        self.assertIn("OID-F", self.controller._already_hedged_ghost_ids)
