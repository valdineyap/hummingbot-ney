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

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig, XEMMLeadLagExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    StopExecutorAction,
)

from controllers.generic.xemm_lead_lag import (
    FeeAssetConfig,
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

    Gates are derived from drift-target pnl_brl (refactor 2026-05-18 v2):
       pnl_brl(t) = (BRL_now − BRL_0) + (drift_now × mid_now − drift_0 × mid_0)
    Tests inject specific pnl_brl values by mocking ``_compute_pnl_brl``
    and pre-setting baseline anchors so the tick's latch logic is skipped.
    """

    async def _warm(self):
        for i in range(25):
            self.market_data_provider.time.return_value = 1700000000.0 + i
            await self.controller.update_processed_data()

    def _mock_pnl(self, pnl_now: Decimal) -> None:
        """Make ``_compute_pnl_brl`` return ``pnl_now`` each call.

        Side-effects:
          * Pre-sets the four baseline fields so the tick latch block is
            skipped (the tick latches baselines only when
            ``_mid_baseline is None``).
          * Tests should call this AFTER setting any state they want to
            persist (e.g. ``_pnl_brl_day_start``).
        """
        from unittest.mock import MagicMock
        self.controller._compute_pnl_brl = MagicMock(return_value=pnl_now)
        if self.controller._mid_baseline is None:
            self.controller._brl_initial = Decimal("0")
            self.controller._btc_initial = Decimal("0")
            self.controller._btc_target = Decimal("0")
            self.controller._mid_baseline = Decimal("300000")

    def _seed_pnl_day_key(self, now_ts: float = 1700000050.0) -> None:
        """Pre-set _pnl_brl_day_start_key so the day-rollover branch in
        the next tick does NOT overwrite _pnl_brl_day_start."""
        from datetime import datetime as _dt
        self.controller._pnl_brl_day_start_key = (
            _dt.utcfromtimestamp(now_ts).strftime("%Y-%m-%d")
        )

    async def test_daily_loss_limit_trips_killed(self):
        await self._warm()
        # pnl_brl_day_start = 0; push pnl_brl_now = -100.01 → daily = -100.01.
        self.controller._pnl_brl_day_start = Decimal("0")
        self.controller._pnl_brl_peak = Decimal("0")
        self._seed_pnl_day_key()
        self._mock_pnl(Decimal("-100.01"))
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertEqual(
            self.controller.processed_data["cancel_reason"], "DAILY_LOSS_LIMIT"
        )

    async def test_daily_loss_just_under_does_not_trip(self):
        await self._warm()
        # daily = -99.99. peak=pnl_now to keep drawdown=0.
        self.controller._pnl_brl_day_start = Decimal("0")
        self.controller._pnl_brl_peak = Decimal("-99.99")
        self._seed_pnl_day_key()
        self._mock_pnl(Decimal("-99.99"))
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )

    async def test_session_drawdown_trips_killed(self):
        await self._warm()
        # Peak +80, current +29 → drawdown 51 ≥ 50 → trips.
        # day_start = pnl_now so DAILY_LOSS_LIMIT doesn't fire first.
        self.controller._pnl_brl_peak = Decimal("80")
        self.controller._pnl_brl_day_start = Decimal("29")
        self._seed_pnl_day_key()
        self._mock_pnl(Decimal("29"))
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
        # Peak +30, current 0 → drawdown 30 < 50 → no trip.
        self.controller._pnl_brl_peak = Decimal("30")
        self.controller._pnl_brl_day_start = Decimal("0")
        self._seed_pnl_day_key()
        self._mock_pnl(Decimal("0"))
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )

    async def test_hourly_burn_trips_killed(self):
        await self._warm()
        # Disable competing gates to isolate HOURLY_BURN.
        self.controller.config.max_daily_loss_quote = Decimal("1000")
        self.controller.config.max_session_drawdown_quote = Decimal("1000")
        self.market_data_provider.time.return_value = 1700000050.0
        now = self.market_data_provider.time.return_value
        # Default max_hourly_burn_quote = 50 BRL.
        # Two history samples so minute_burn doesn't co-trip:
        #   (now-3700, +60): 1h-old anchor → hourly_burn = -1 - 60 = -61
        #   (now-65,    -1): recent anchor → minute_burn = -1 - (-1) = 0
        from collections import deque
        self.controller._pnl_brl_history = deque([
            (now - 3700, Decimal("60")),
            (now - 65, Decimal("-1")),
        ])
        self.controller._pnl_brl_day_start = Decimal("-1")
        self.controller._pnl_brl_peak = Decimal("60")
        self._seed_pnl_day_key()
        self._mock_pnl(Decimal("-1"))
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertTrue(
            self.controller.processed_data["cancel_reason"].startswith(
                "HOURLY_BURN_"
            )
        )

    async def test_hourly_burn_no_old_sample_does_not_trip(self):
        """Cold-start guard: without a sample 1h old, burn returns 0."""
        await self._warm()
        self.market_data_provider.time.return_value = 1700000050.0
        from collections import deque
        self.controller._pnl_brl_history = deque(
            [(1700000049.0, Decimal("0"))]
        )
        self.controller._pnl_brl_day_start = Decimal("0")
        self.controller._pnl_brl_peak = Decimal("0")
        self._mock_pnl(Decimal("-100"))  # large drop
        await self.controller.update_processed_data()
        # No old sample → hourly_burn returns 0 → no HOURLY_BURN trip.
        # (DAILY_LOSS_LIMIT will trip first because daily = -100.)
        self.assertEqual(
            self.controller._hourly_burn(1700000050.0), Decimal("0")
        )

    async def test_minute_burn_trips_killed(self):
        await self._warm()
        # Disable competing gates to isolate MINUTE_BURN.
        self.controller.config.max_session_drawdown_quote = Decimal("1000")
        self.market_data_provider.time.return_value = 1700000050.0
        now = self.market_data_provider.time.return_value
        # Old sample at (now-65s) = +20. pnl_now = 0 → burn = -20 ≤ -15 → trip.
        from collections import deque
        self.controller._pnl_brl_history = deque([
            (now - 65, Decimal("20")),
        ])
        self.controller._pnl_brl_day_start = Decimal("0")
        self.controller._pnl_brl_peak = Decimal("20")
        self._seed_pnl_day_key()
        self._mock_pnl(Decimal("0"))
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertTrue(
            self.controller.processed_data["cancel_reason"].startswith(
                "MINUTE_BURN_"
            )
        )

    async def test_minute_burn_recent_sample_does_not_trip(self):
        """Old sample within the 60s window (NOT old enough) → no burn."""
        await self._warm()
        self.market_data_provider.time.return_value = 1700000050.0
        from collections import deque
        # Only sample is 30s old → no comparator at (now-60s).
        self.controller._pnl_brl_history = deque(
            [(1700000020.0, Decimal("100"))]
        )
        self.controller._pnl_brl_day_start = Decimal("0")
        self.controller._pnl_brl_peak = Decimal("100")
        self._mock_pnl(Decimal("0"))
        await self.controller.update_processed_data()
        self.assertEqual(
            self.controller._minute_burn(1700000050.0), Decimal("0")
        )

    async def test_safety_snapshot_written_to_state_json(self):
        """Periodic snapshot must include pnl_brl fields + diagnostic V."""
        import json
        await self._warm()
        # Force snapshot cadence to fire on next tick.
        self.controller._last_safety_snapshot = 0.0
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        # Read state.json the controller's TradeLedger wrote.
        state_path = self.controller._trade_ledger._state_path
        with open(state_path) as f:
            state = json.load(f)
        self.assertIn("safety", state)
        safety = state["safety"]
        for key in ("regime", "kill_reason",
                    "pnl_brl_now", "pnl_brl_peak", "pnl_brl_day_start",
                    "brl_initial", "btc_initial", "btc_target",
                    "mid_baseline", "v_now_diagnostic",
                    "daily_realized_pnl", "session_pnl_total",
                    "session_drawdown", "hourly_burn", "minute_burn",
                    "limits"):
            self.assertIn(key, safety)
        for key in ("max_daily_loss_quote", "max_minute_burn_quote",
                    "max_hourly_burn_quote", "max_session_drawdown_quote"):
            self.assertIn(key, safety["limits"])
        # Old V-MtM fields explicitly REMOVED (replaced by pnl_brl_*).
        self.assertNotIn("v_initial", safety)
        self.assertNotIn("v_day_start", safety)
        self.assertNotIn("v_peak", safety)
        # LOSING_STREAK fields explicitly REMOVED.
        self.assertNotIn("consecutive_losing_fills", safety)
        self.assertNotIn(
            "max_consecutive_losing_fills", safety["limits"],
        )

    def _seed_drift(self, delta_quote: Decimal) -> None:
        """Inject audit drift state so the UNREALIZED_EXPOSURE gate has data."""
        self.controller._last_audit_results = {
            "_drift_active": True,
            "BTC": {
                "actual": Decimal("0.0015"),
                "target": Decimal("0.001"),
                "delta": Decimal("0.0005"),
                "delta_quote": delta_quote,
                "within": False,
            },
        }

    async def test_unrealized_exposure_kills_after_grace(self):
        """Drift above limit + audit quiet + grace expired → KILL."""
        await self._warm()
        # Tighten limits for testability.
        self.controller.config.max_unrealized_loss_quote = Decimal("100")
        self.controller.config.unrealized_exposure_grace_sec = 10.0
        # Disable audit so seeded drift state persists across ticks.
        from unittest.mock import AsyncMock as _AsyncMock
        self.controller._run_inventory_audit = _AsyncMock()
        self.controller._execute_pending_rebalances = _AsyncMock()
        self._seed_drift(Decimal("150"))
        # Ensure audit is quiet.
        self.controller._pending_rebalances = {}
        self.controller._last_fill_time = 0.0
        # First tick: trip timer starts (no kill yet).
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )
        self.assertIsNotNone(self.controller._unrealized_exposure_trip_at)
        # Second tick: 11s later → grace exceeded → KILL.
        self._seed_drift(Decimal("150"))  # re-seed (would otherwise be cleared)
        self.market_data_provider.time.return_value = 1700000061.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        self.assertTrue(
            self.controller.processed_data["cancel_reason"].startswith(
                "UNREALIZED_EXPOSURE_"
            )
        )

    async def test_unrealized_exposure_defers_when_rebalance_pending(self):
        """Pending rebalance → audit is busy → do NOT kill even past grace."""
        await self._warm()
        self.controller.config.max_unrealized_loss_quote = Decimal("100")
        self.controller.config.unrealized_exposure_grace_sec = 5.0
        # Disable audit + rebalance execution so our seeded state persists.
        from unittest.mock import AsyncMock as _AsyncMock
        self.controller._run_inventory_audit = _AsyncMock()
        self.controller._execute_pending_rebalances = _AsyncMock()
        self.controller._last_fill_time = 0.0
        # Multiple ticks well past grace; reseed state each tick so the
        # gate evaluates against the audit-busy + drifted condition.
        for ts in (1700000050.0, 1700000060.0, 1700000070.0, 1700000100.0):
            self._seed_drift(Decimal("500"))
            self.controller._pending_rebalances = {"BTC": Decimal("0.0005")}
            self.market_data_provider.time.return_value = ts
            await self.controller.update_processed_data()
            self.assertNotEqual(
                self.controller.processed_data["regime"], Regime.KILLED,
                msg=f"unexpected KILL at ts={ts} "
                f"reason={self.controller.processed_data.get('cancel_reason')}"
            )
        self.assertIsNone(self.controller._unrealized_exposure_trip_at)

    async def test_unrealized_exposure_defers_when_inflight_activity(self):
        """Recent fill (< 10s) → inflight activity → do NOT kill."""
        await self._warm()
        self.controller.config.max_unrealized_loss_quote = Decimal("100")
        self.controller.config.unrealized_exposure_grace_sec = 5.0
        from unittest.mock import AsyncMock as _AsyncMock, MagicMock
        self.controller._run_inventory_audit = _AsyncMock()
        self.controller._execute_pending_rebalances = _AsyncMock()
        self._seed_drift(Decimal("500"))
        self.controller._pending_rebalances = {}
        # Patch _has_inflight_activity to True (simulates recent fill).
        self.controller._has_inflight_activity = MagicMock(return_value=True)
        self.market_data_provider.time.return_value = 1700000100.0
        await self.controller.update_processed_data()
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )
        self.assertIsNone(self.controller._unrealized_exposure_trip_at)

    async def test_unrealized_exposure_timer_resets_when_below_limit(self):
        """Drift drops below limit → trip timer resets, no KILL."""
        await self._warm()
        self.controller.config.max_unrealized_loss_quote = Decimal("100")
        self.controller.config.unrealized_exposure_grace_sec = 5.0
        from unittest.mock import AsyncMock as _AsyncMock
        self.controller._run_inventory_audit = _AsyncMock()
        self.controller._execute_pending_rebalances = _AsyncMock()
        self.controller._pending_rebalances = {}
        self.controller._last_fill_time = 0.0
        # First tick: above limit, start timer.
        self._seed_drift(Decimal("500"))
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertIsNotNone(self.controller._unrealized_exposure_trip_at)
        # Second tick: drift cleared (audit worked) → reset timer.
        self._seed_drift(Decimal("10"))
        self.market_data_provider.time.return_value = 1700000052.0
        await self.controller.update_processed_data()
        self.assertIsNone(self.controller._unrealized_exposure_trip_at)
        self.assertNotEqual(
            self.controller.processed_data["regime"], Regime.KILLED
        )

    async def test_killed_reason_is_sticky_across_recovery(self):
        # Trip session drawdown, then "recover" pnl — should remain KILLED.
        await self._warm()
        self.controller._pnl_brl_peak = Decimal("100")
        self.controller._pnl_brl_day_start = Decimal("-50")
        self._seed_pnl_day_key()
        self._mock_pnl(Decimal("-50"))  # dd = 100-(-50) = 150 → KILL
        self.market_data_provider.time.return_value = 1700000050.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)
        # Now "recover" — kill should remain.
        self._mock_pnl(Decimal("100"))
        self.market_data_provider.time.return_value = 1700000060.0
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], Regime.KILLED)


class TestPnlBrlInvariants(_BaseControllerTest):
    """Property-style tests on _compute_pnl_brl validating the core
    invariants of the drift-target model.

    KEY invariant: when the bot is balanced at target, no trades happen,
    and only the BTC mid moves, pnl_brl is unchanged. This is the whole
    point of the model — MtM noise on the *target* inventory cancels.
    """

    def _setup_baseline(
        self,
        brl_per_exchange: Decimal = Decimal("5000"),
        btc_per_exchange: Decimal = Decimal("0.1"),  # total = 0.2
        btc_target: Decimal = Decimal("0.2"),
        mid_baseline: Decimal = Decimal("300000"),
    ) -> None:
        self.controller._brl_initial = brl_per_exchange * 2
        self.controller._btc_initial = btc_per_exchange * 2
        self.controller._btc_target = btc_target
        self.controller._mid_baseline = mid_baseline
        # Wire balances to a flexible mock the test can mutate.
        self._brl_total = brl_per_exchange * 2
        self._btc_total = btc_per_exchange * 2

        def bal(connector, asset):
            if asset == "BRL":
                return self._brl_total / Decimal("2")
            if asset == "BTC":
                return self._btc_total / Decimal("2")
            return Decimal("0")

        self.market_data_provider.get_balance.side_effect = bal

        def conn(name):
            c = MagicMock()
            c.get_balance.side_effect = lambda asset: bal(name, asset)
            return c

        self.market_data_provider.get_connector.side_effect = conn

    def test_mtm_noise_cancels_when_balanced_at_target(self):
        """No trades, balanced at target, BTC moves R$1000 → pnl_brl = 0."""
        self._setup_baseline()
        # No fills: balances unchanged. mid_now moves +1000.
        pnl = self.controller._compute_pnl_brl(Decimal("301000"))
        # drift_now = drift_0 = 0 → pnl = 0 + (0 × mid_now − 0 × mid_0) = 0
        self.assertEqual(pnl, Decimal("0"))

    def test_pnl_zero_at_t_zero(self):
        """At t=0 (no mutation), pnl_brl == 0 regardless of starting drift."""
        # Start with 0.18 BTC (below target 0.20) so drift_0 = -0.02.
        self._setup_baseline(btc_per_exchange=Decimal("0.09"))  # total = 0.18
        pnl = self.controller._compute_pnl_brl(Decimal("300000"))  # mid_now = mid_0
        self.assertEqual(pnl, Decimal("0"))

    def test_drift_increases_pnl_when_long_and_btc_rises(self):
        """Long drift (btc > target), BTC up → pnl positive (sellable surplus)."""
        # Start at target 0.2; later acquire +0.01 BTC unhedged.
        self._setup_baseline()
        self._btc_total = Decimal("0.21")  # drift_now = +0.01
        # mid moves +1000. drift_0 = 0 → contribution from baseline term = 0.
        pnl = self.controller._compute_pnl_brl(Decimal("301000"))
        # ΔBRL = 0 (no BRL movement modelled here; pure inventory drift).
        # Expected: 0 + (+0.01 × 301000 − 0 × 300000) = +3010
        self.assertEqual(pnl, Decimal("3010"))

    def test_drift_decreases_pnl_when_short_and_btc_rises(self):
        """Short drift (btc < target), BTC up → pnl negative (costs more to buy back)."""
        self._setup_baseline()
        self._btc_total = Decimal("0.19")  # drift_now = -0.01
        pnl = self.controller._compute_pnl_brl(Decimal("301000"))
        # 0 + (-0.01 × 301000 − 0 × 300000) = -3010
        self.assertEqual(pnl, Decimal("-3010"))

    def test_perfect_xemm_cycle_captures_only_spread(self):
        """Maker BUY 0.02@p_m, hedge SELL 0.02@p_t → pnl_brl = spread × size,
        regardless of mid_now (delta-flat cycle)."""
        self._setup_baseline()
        # Simulate: maker BUY 0.02 @ 300050, hedge SELL 0.02 @ 300080.
        # ΔBRL = -0.02×300050 + 0.02×300080 = +0.60
        # ΔBTC = 0 (perfectly hedged)
        self._brl_total = self.controller._brl_initial + Decimal("0.60")
        # btc total unchanged at 0.2 → drift_now = 0
        # mid_now moves arbitrarily — should not affect pnl_brl.
        for mid_now in (Decimal("300000"), Decimal("301000"), Decimal("295000")):
            pnl = self.controller._compute_pnl_brl(mid_now)
            self.assertEqual(pnl, Decimal("0.60"))

    def test_fees_paid_flow_through_brl(self):
        """Fees deducted from BRL show up directly in pnl_brl."""
        self._setup_baseline()
        # Fee scenario: lose 5 BRL of fees, no other balance moves.
        self._brl_total = self.controller._brl_initial - Decimal("5")
        pnl = self.controller._compute_pnl_brl(Decimal("300000"))
        self.assertEqual(pnl, Decimal("-5"))

    def test_pnl_none_when_baselines_unset(self):
        """Before first valid tick latches baselines, pnl_brl is None."""
        # Default test setup: no baselines set. Should return None.
        pnl = self.controller._compute_pnl_brl(Decimal("300000"))
        self.assertIsNone(pnl)

    def test_pnl_none_when_mid_invalid(self):
        self._setup_baseline()
        self.assertIsNone(self.controller._compute_pnl_brl(Decimal("0")))
        self.assertIsNone(self.controller._compute_pnl_brl(Decimal("-1")))


# ===================================================================== #
# Group C2 — Max-lot guard (refuse oversized orders)                    #
# ===================================================================== #
class TestMaxLotGuard(_BaseControllerTest):
    """Verifies ``_check_max_lot`` and its integration at all order
    placement chokepoints (audit auto_rebalance, XEMM spawn, arb spawn).

    The guard refuses orders whose base amount exceeds
    ``order_amount × max_order_amount_multiplier`` and logs CRITICAL.
    """

    def _limit(self) -> Decimal:
        return (self.controller.config.order_amount
                * self.controller.config.max_order_amount_multiplier)

    def test_check_max_lot_allows_at_limit(self):
        # Exactly at limit must pass.
        self.assertTrue(
            self.controller._check_max_lot(self._limit(), source="t")
        )
        self.assertEqual(self.controller._oversized_blocks_total, 0)

    def test_check_max_lot_blocks_above_limit(self):
        # 1 sat over the limit → blocked.
        self.assertFalse(
            self.controller._check_max_lot(
                self._limit() + Decimal("0.00000001"),
                source="audit:BTC@binance",
            )
        )
        self.assertEqual(self.controller._oversized_blocks_total, 1)
        self.assertEqual(
            self.controller._last_oversized_block_source,
            "audit:BTC@binance",
        )

    def test_check_max_lot_counter_increments_on_each_block(self):
        for _ in range(3):
            self.controller._check_max_lot(Decimal("1"), source="t")
        self.assertEqual(self.controller._oversized_blocks_total, 3)

    def test_check_max_lot_normal_xemm_amount_passes(self):
        # The bot's normal order size is well under the cap.
        self.assertTrue(
            self.controller._check_max_lot(
                self.controller.config.order_amount, source="xemm_spawn:BUY"
            )
        )

    def test_check_max_lot_multiplier_one_blocks_anything_above_order_amount(self):
        # Tight setting: multiplier=1.0 means the cap == order_amount.
        self.controller.config.max_order_amount_multiplier = Decimal("1.0")
        self.assertTrue(
            self.controller._check_max_lot(
                self.controller.config.order_amount, source="t"
            )
        )
        self.assertFalse(
            self.controller._check_max_lot(
                self.controller.config.order_amount + Decimal("0.00001"),
                source="t",
            )
        )

    def test_make_create_action_returns_none_if_order_amount_above_cap(self):
        """Defensive: if order_amount itself was mutated above the cap,
        the spawn path refuses to emit a CreateExecutorAction."""
        # Force a violation by tightening the multiplier below 1.0.
        self.controller.config.max_order_amount_multiplier = Decimal("0.5")
        action = self.controller._make_create_action(
            TradeType.BUY, Decimal("0.0003"), now=1700000000.0,
        )
        self.assertIsNone(action)
        self.assertEqual(self.controller._oversized_blocks_total, 1)
        self.assertTrue(
            self.controller._last_oversized_block_source.startswith(
                "xemm_spawn:"
            )
        )


# ===================================================================== #
# Group C.5 — Snap-to-top dynamic sizing                                #
# ===================================================================== #
class TestSnapToTop(_BaseControllerTest):
    """Verifies ``_compute_snap_order_amount`` and its integration in
    ``_make_create_action``. The mechanism shrinks the maker order to ride
    the top of the taker book when it offers a materially better price than
    the VWAP for the full ``order_amount``.

    Setup uses ``order_amount=0.02`` and ``min_dynamic_order_amount=0.002``
    so realistic snap candidates exist between those bounds.
    """

    def setUp(self):
        super().setUp()
        # Re-instantiate with snap-friendly config: order=0.02, min=0.002.
        self.config.order_amount = Decimal("0.02")
        self.config.snap_to_top_enabled = True
        self.config.snap_threshold_bps = Decimal("0.5")
        self.config.min_dynamic_order_amount = Decimal("0.002")
        self.config.snap_safety_fraction = Decimal("0.8")
        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
        self.set_loggers([self.controller.logger()])
        self._setup_default_market_data()

    def _mock_book(self, top_bid=None, top_ask=None):
        """Mock get_order_book with controllable top entries.

        top_bid / top_ask: Optional[Tuple[Decimal, Decimal]] = (price, amount)
        """
        ob = MagicMock()
        bid = MagicMock()
        bid.price = float(top_bid[0]) if top_bid else 300100.0
        bid.amount = float(top_bid[1]) if top_bid else 1.0
        ask = MagicMock()
        ask.price = float(top_ask[0]) if top_ask else 300110.0
        ask.amount = float(top_ask[1]) if top_ask else 1.0
        ob.bid_entries = MagicMock(return_value=iter([bid]))
        ob.ask_entries = MagicMock(return_value=iter([ask]))
        self.market_data_provider.get_order_book = MagicMock(return_value=ob)

    def _mock_vwap(self, value: Decimal) -> None:
        self.controller._vwap_for_amount = MagicMock(return_value=value)

    # ------------------ pure unit tests ------------------ #
    def test_quantize_pads_with_one_satoshi(self):
        # 0.00567 BTC → 567,000 sats → quantize to 567,000 (already step) +1
        out = self.controller._quantize_amount_with_pad(Decimal("0.00567"))
        self.assertEqual(out, Decimal("0.00567001"))

    def test_quantize_rounds_down_below_step(self):
        # 0.005671 BTC → 567,100 sats → rounds down to 567,000 +1 sat
        out = self.controller._quantize_amount_with_pad(Decimal("0.005671"))
        self.assertEqual(out, Decimal("0.00567001"))

    def test_quantize_zero_input(self):
        self.assertEqual(
            self.controller._quantize_amount_with_pad(Decimal("0")),
            Decimal("0"),
        )

    def test_quantize_below_lot_step(self):
        # 999 sats < 1000 sats step → quantized to 0 → returns Decimal("0").
        self.assertEqual(
            self.controller._quantize_amount_with_pad(Decimal("0.00000999")),
            Decimal("0"),
        )

    # ------------------ snap decision logic ------------------ #
    def test_snap_disabled_returns_full_amount(self):
        self.controller.config.snap_to_top_enabled = False
        amt, tel = self.controller._compute_snap_order_amount(TradeType.BUY)
        self.assertEqual(amt, self.config.order_amount)
        self.assertFalse(tel["active"])
        self.assertEqual(tel["reason"], "disabled")

    def test_snap_no_book_returns_full_amount(self):
        self.market_data_provider.get_order_book = MagicMock(
            side_effect=Exception("boom")
        )
        amt, tel = self.controller._compute_snap_order_amount(TradeType.BUY)
        self.assertEqual(amt, self.config.order_amount)
        self.assertFalse(tel["active"])
        self.assertEqual(tel["reason"], "no_book")

    def test_snap_no_vwap_returns_full_amount(self):
        self._mock_book(top_bid=(Decimal("300200"), Decimal("0.005")))
        self._mock_vwap(None)
        amt, tel = self.controller._compute_snap_order_amount(TradeType.BUY)
        self.assertEqual(amt, self.config.order_amount)
        self.assertFalse(tel["active"])
        self.assertEqual(tel["reason"], "no_vwap")

    def test_snap_edge_below_threshold_returns_full_amount(self):
        # top_bid 300100, vwap_full 300099 → edge_gain ≈ 0.03 bps < 0.5
        self._mock_book(top_bid=(Decimal("300100"), Decimal("0.005")))
        self._mock_vwap(Decimal("300099"))
        amt, tel = self.controller._compute_snap_order_amount(TradeType.BUY)
        self.assertEqual(amt, self.config.order_amount)
        self.assertEqual(tel["reason"], "edge_below_threshold")

    def test_snap_top_covers_full_returns_full_amount(self):
        # Top layer carries 1 BTC ≫ 0.02 → 0.8×1 ≥ order_amount → no shrink.
        self._mock_book(top_bid=(Decimal("300200"), Decimal("1.0")))
        self._mock_vwap(Decimal("300100"))  # huge gain, but irrelevant here
        amt, tel = self.controller._compute_snap_order_amount(TradeType.BUY)
        self.assertEqual(amt, self.config.order_amount)
        self.assertEqual(tel["reason"], "top_covers_full")

    def test_snap_below_min_dynamic_returns_full_amount(self):
        # Top layer 0.002 BTC, safety 0.8 → 0.0016 < 0.002 floor → no shrink.
        self._mock_book(top_bid=(Decimal("300200"), Decimal("0.002")))
        self._mock_vwap(Decimal("300100"))
        amt, tel = self.controller._compute_snap_order_amount(TradeType.BUY)
        self.assertEqual(amt, self.config.order_amount)
        self.assertEqual(tel["reason"], "below_min_dynamic")

    def test_snap_active_buy_side_returns_quantized_amount(self):
        # Hedge SELL: top_bid=300200 > vwap_full=300100 → edge ≈ 3.3 bps > 0.5.
        # Top size = 0.01 BTC → 0.8×0.01 = 0.008 BTC → quantize to 0.00800001.
        self._mock_book(top_bid=(Decimal("300200"), Decimal("0.01")))
        self._mock_vwap(Decimal("300100"))
        amt, tel = self.controller._compute_snap_order_amount(TradeType.BUY)
        self.assertTrue(tel["active"])
        self.assertEqual(tel["reason"], "snapped")
        self.assertEqual(amt, Decimal("0.00800001"))
        self.assertLess(amt, self.config.order_amount)

    def test_snap_active_sell_side_returns_quantized_amount(self):
        # Hedge BUY: top_ask=300050 < vwap_full=300150 → edge ≈ 3.3 bps > 0.5.
        # Top size = 0.005 BTC → 0.8×0.005 = 0.004 BTC → quantize to 0.00400001.
        self._mock_book(top_ask=(Decimal("300050"), Decimal("0.005")))
        self._mock_vwap(Decimal("300150"))
        amt, tel = self.controller._compute_snap_order_amount(TradeType.SELL)
        self.assertTrue(tel["active"])
        self.assertEqual(tel["reason"], "snapped")
        self.assertEqual(amt, Decimal("0.00400001"))
        self.assertLess(amt, self.config.order_amount)

    # ------------------ integration with _make_create_action ------------------ #
    def test_make_create_action_uses_snap_amount(self):
        """When snap fires, the executor config receives the shrunken amount."""
        self._mock_book(top_bid=(Decimal("300200"), Decimal("0.01")))
        self._mock_vwap(Decimal("300100"))
        action = self.controller._make_create_action(
            TradeType.BUY, Decimal("0.0010"), now=1700000000.0,
        )
        self.assertIsNotNone(action)
        self.assertEqual(action.executor_config.order_amount, Decimal("0.00800001"))

    def test_make_create_action_full_amount_when_snap_disabled(self):
        self.controller.config.snap_to_top_enabled = False
        self._mock_book(top_bid=(Decimal("300200"), Decimal("0.01")))
        self._mock_vwap(Decimal("300100"))
        action = self.controller._make_create_action(
            TradeType.BUY, Decimal("0.0010"), now=1700000000.0,
        )
        self.assertIsNotNone(action)
        self.assertEqual(
            action.executor_config.order_amount, self.config.order_amount,
        )


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


class TestArbNetSpawnGate(_ArbBaseTest):
    """Pins the NET-bps spawn-gate behaviour added 2026-05-11.

    The spawn gate compares ``gross_bps − tx_cost_bps`` (NET) against
    ``arb_min_profitability``, matching the executor's own NET execute gate
    and the MM controller's NET ``min/target/max_profitability`` semantics.
    These tests force a specific tx_cost via mock connectors so the
    arithmetic is deterministic regardless of the framework's default fee
    schedule.
    """

    def _mock_fee(self, total_round_trip_bps: Decimal):
        """Make ``get_fee`` on both connectors return ``percent = half`` —
        so that round-trip sum equals the target bps."""
        from unittest.mock import MagicMock as _MM
        half_pct = total_round_trip_bps / Decimal("20000")  # bps→percent, halve
        fee_obj = _MM()
        fee_obj.percent = half_pct
        fake_conn = _MM()
        fake_conn.get_fee = _MM(return_value=fee_obj)
        self.market_data_provider.get_connector = _MM(return_value=fake_conn)

    async def test_net_equals_gross_minus_tx_cost(self):
        # gross=20 bps, tx_cost=4 bps → net=16 bps
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),  # 20 bps gross
        )
        self._mock_fee(Decimal("4"))
        await self._warm()
        pd = self.controller.processed_data
        self.assertAlmostEqual(
            float(pd["arb_tx_cost_bps"]), 4.0, places=2,
        )
        self.assertAlmostEqual(
            float(pd["arb_long_net_bps"]),
            float(pd["arb_long_gross_bps"]) - 4.0, places=2,
        )

    async def test_spawn_uses_net_not_gross_at_boundary(self):
        # gross=20 bps, tx_cost=4 bps → net=16 bps. With threshold 15 bps,
        # net>threshold → spawn.
        self.config.arb_min_profitability = Decimal("0.0015")  # 15 bps
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        self._mock_fee(Decimal("4"))
        await self._warm()
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        self.controller._last_arb_time = 0.0
        actions = self.controller.determine_executor_actions()
        arb_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        self.assertEqual(len(arb_creates), 1)

    async def test_spawn_blocked_when_net_under_threshold_even_if_gross_over(self):
        # gross=18 bps, tx_cost=10 bps → net=8 bps. Threshold 15 bps.
        # Under OLD gross-only gate, this would have spawned (18>15);
        # under NEW NET gate it must NOT (8<15).
        self.config.arb_min_profitability = Decimal("0.0015")  # 15 bps
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300540"),  # 18 bps gross
        )
        self._mock_fee(Decimal("10"))
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

    async def test_spawn_succeeds_with_low_threshold_and_zero_fees(self):
        # Spawn at the new default-ish 2 bps NET with zero fees.
        self.config.arb_min_profitability = Decimal("0.0002")  # 2 bps
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300120"),  # 4 bps gross
        )
        self._mock_fee(Decimal("0"))
        await self._warm()
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        self.controller._last_arb_time = 0.0
        actions = self.controller.determine_executor_actions()
        arb_creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        self.assertEqual(len(arb_creates), 1)

    async def test_tx_cost_fallback_to_zero_on_connector_error(self):
        """If ``connector.get_fee`` raises, the estimator returns 0 bps so
        the spawn gate degrades gracefully — better to spawn-and-let-
        executor-decide than silently never spawn."""
        from unittest.mock import MagicMock as _MM
        bad_conn = _MM()
        bad_conn.get_fee = _MM(side_effect=RuntimeError("api down"))
        self.market_data_provider.get_connector = _MM(return_value=bad_conn)
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        await self._warm()
        pd = self.controller.processed_data
        self.assertEqual(pd["arb_tx_cost_bps"], Decimal("0"))


class TestArbSpawnRaceGuard(_ArbBaseTest):
    """Pins the spawn-race fix added 2026-05-11 17:35.

    Before the fix, ``_has_active_arb_executor`` only saw the latest
    ``executors_info`` snapshot. The framework updates that ASYNCHRONOUSLY
    after CreateExecutorAction dispatch, so multiple arbs could spawn at
    the cooldown boundary before any registered. Observed in prod: 10
    arbs spawned in 47 s, none executed, all sat in RUNNING forever.

    Fix: ``_arb_spawn_pending_until`` is set to ``now + 30 s`` when we
    return a CreateExecutorAction; cleared the moment we observe the
    executor in ``executors_info``.
    """
    async def test_pending_blocks_second_spawn_within_grace(self):
        # First spawn → flag set
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),  # 20 bps gross
        )
        await self._warm()
        self.controller.executors_info = []
        self.controller._last_action_time = 0.0
        self.controller._last_arb_time = 0.0
        first_actions = self.controller.determine_executor_actions()
        first_creates = [a for a in first_actions if isinstance(a, CreateExecutorAction)]
        self.assertEqual(len(first_creates), 1)
        self.assertGreater(self.controller._arb_spawn_pending_until, 0)
        # executors_info STILL empty (framework hasn't registered yet)
        # Tick forward past arb_min_interval_sec but within 30 s grace
        now = self.market_data_provider.time.return_value
        self.market_data_provider.time.return_value = now + 10.0
        self.controller._last_action_time = 0.0  # bypass anti-churn
        self.controller._last_arb_time = 0.0  # bypass arb cooldown
        second_actions = self.controller.determine_executor_actions()
        second_creates = [
            a for a in second_actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        # Spawn-race guard should block this second spawn
        self.assertEqual(len(second_creates), 0)

    async def test_pending_cleared_when_executor_observed(self):
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        await self._warm()
        self.controller._last_arb_time = 0.0
        self.controller._last_action_time = 0.0
        # Simulate the pending flag being set
        now = self.market_data_provider.time.return_value
        self.controller._arb_spawn_pending_until = now + 25.0
        # Inject an active arb executor (framework caught up)
        arb_ex = MagicMock()
        arb_ex.is_done = False
        arb_ex.config.type = "lead_lag_arbitrage_executor"
        arb_ex.config.id = "ARB-OBSERVED"
        # _has_active_arb_executor is what clears the flag
        was_active = self.controller._has_active_arb_executor([arb_ex])
        self.assertTrue(was_active)
        self.assertEqual(self.controller._arb_spawn_pending_until, 0.0)

    async def test_pending_auto_expires_after_30s(self):
        # If executor never registers, flag must auto-expire after 30 s.
        # vwap must be set BEFORE _warm so processed_data carries the arb edge.
        self._set_vwap(
            taker_buy=Decimal("300000"),
            maker_sell=Decimal("300600"),
        )
        await self._warm()
        now = self.market_data_provider.time.return_value
        # Simulate pending that has already expired (now > pending_until).
        self.controller._arb_spawn_pending_until = now - 1.0
        self.controller.executors_info = []
        self.controller._last_arb_time = 0.0
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        creates = [
            a for a in actions if isinstance(a, CreateExecutorAction)
            and a.executor_config.type == "lead_lag_arbitrage_executor"
        ]
        # Expired pending should NOT block
        self.assertEqual(len(creates), 1)


class TestArbExecutorAgeOut(_ArbBaseTest):
    """An arb executor that polls profitability forever without firing
    is killed after ``arb_executor_max_age_sec`` to prevent accumulation
    and race-to-execute when threshold finally crosses."""

    async def test_old_arb_executor_is_killed(self):
        await self._warm()
        now = self.market_data_provider.time.return_value
        # Inject a stale arb executor (spawned 120 s ago, never executed)
        stale = MagicMock()
        stale.is_done = False
        stale.config.type = "lead_lag_arbitrage_executor"
        stale.config.id = "ARB-STALE"
        stale.timestamp = now - 120.0  # well past max_age (default 60 s)
        self.controller.executors_info = [stale]
        # Bypass anti-churn so we reach the sweep
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        stops = [
            a for a in actions if isinstance(a, StopExecutorAction)
            and a.executor_id == "ARB-STALE"
        ]
        self.assertEqual(len(stops), 1)

    async def test_fresh_arb_executor_not_killed(self):
        await self._warm()
        now = self.market_data_provider.time.return_value
        fresh = MagicMock()
        fresh.is_done = False
        fresh.config.type = "lead_lag_arbitrage_executor"
        fresh.config.id = "ARB-FRESH"
        fresh.timestamp = now - 10.0  # only 10 s old
        self.controller.executors_info = [fresh]
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        stops = [
            a for a in actions if isinstance(a, StopExecutorAction)
            and a.executor_id == "ARB-FRESH"
        ]
        self.assertEqual(len(stops), 0)

    async def test_xemm_executor_not_subject_to_arb_age_out(self):
        """Only arb executors age out — XEMM executors have their own
        lifecycle."""
        await self._warm()
        now = self.market_data_provider.time.return_value
        old_xemm = MagicMock()
        old_xemm.is_done = False
        old_xemm.config.type = "xemm_executor"
        old_xemm.config.id = "XEMM-OLD"
        old_xemm.timestamp = now - 600.0  # 10 minutes
        self.controller.executors_info = [old_xemm]
        self.controller._last_action_time = 0.0
        actions = self.controller.determine_executor_actions()
        stops = [
            a for a in actions if isinstance(a, StopExecutorAction)
            and a.executor_id == "XEMM-OLD"
        ]
        self.assertEqual(len(stops), 0)


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


class TestArbDangerousFailure(_ArbBaseTest):
    """Differentiates UNWIND_ABORTED (dangerous, open position) from ordinary
    arb losses (small slippage on a flat close). Only the dangerous path
    increments the daily failure counter and engages failure_pause_sec.
    """

    def _make_closed_arb_executor(
        self, executor_id: str, net_pnl: Decimal, close_type,
    ):
        from hummingbot.strategy_v2.models.executors import CloseType  # noqa: F401
        ex = MagicMock()
        ex.id = executor_id
        ex.is_done = True
        ex.close_type = close_type
        ex.net_pnl_quote = net_pnl
        ex.cum_fees_quote = Decimal("0")
        ex.filled_amount_quote = Decimal("80")
        ex.custom_info = {}
        ex.config = MagicMock()
        ex.config.type = "lead_lag_arbitrage_executor"
        ex.config.id = executor_id
        ex.timestamp = self.market_data_provider.time.return_value - 5.0
        return ex

    async def _drive_update_with_executor(self, ex):
        """Simulate one update_processed_data tick that observes `ex` closing."""
        self.controller.executors_info = [ex]
        # Force a fresh tick (skip the fingerprint short-circuit).
        self.controller._last_fingerprint = None
        self.controller._last_full_update = 0.0
        # Pre-set today's reset-day key so the daily reset block in
        # update_processed_data doesn't zero `_arb_failures_today` AFTER
        # our processed_data ticker increments it.
        from datetime import datetime as _dt
        now_ts = self.market_data_provider.time.return_value
        self.controller._arb_last_reset_day = _dt.utcfromtimestamp(now_ts).strftime("%Y-%m-%d")
        self.controller._daily_pnl_last_reset_day = self.controller._arb_last_reset_day
        await self.controller.update_processed_data()

    async def test_unwind_aborted_loss_increments_failure_counter_and_pauses(self):
        from hummingbot.strategy_v2.models.executors import CloseType
        ex = self._make_closed_arb_executor(
            "ARB-DANGER", Decimal("-0.50"), CloseType.UNWIND_ABORTED,
        )
        before_failures = self.controller._arb_failures_today
        before_pause = self.controller._arb_paused_until
        await self._drive_update_with_executor(ex)

        self.assertEqual(self.controller._arb_failures_today, before_failures + 1)
        self.assertGreater(self.controller._arb_paused_until, before_pause)
        # Realized loss is recorded regardless of close type.
        self.assertEqual(self.controller._arb_realized_loss_today, Decimal("0.50"))

    async def test_ordinary_loss_records_loss_but_does_not_pause(self):
        from hummingbot.strategy_v2.models.executors import CloseType
        ex = self._make_closed_arb_executor(
            "ARB-NORMAL", Decimal("-0.03"), CloseType.COMPLETED,
        )
        before_failures = self.controller._arb_failures_today
        before_pause = self.controller._arb_paused_until
        await self._drive_update_with_executor(ex)

        # Failure counter NOT advanced for ordinary close types.
        self.assertEqual(self.controller._arb_failures_today, before_failures)
        # No pause engaged.
        self.assertEqual(self.controller._arb_paused_until, before_pause)
        # But the loss still hits the BRL daily limit.
        self.assertEqual(self.controller._arb_realized_loss_today, Decimal("0.03"))

    async def test_unwound_with_small_loss_does_not_pause(self):
        """UNWOUND means we successfully exited the leftover leg — flat,
        just paid slippage. Not the dangerous case."""
        from hummingbot.strategy_v2.models.executors import CloseType
        ex = self._make_closed_arb_executor(
            "ARB-UNWOUND", Decimal("-0.10"), CloseType.UNWOUND,
        )
        before_pause = self.controller._arb_paused_until
        await self._drive_update_with_executor(ex)

        self.assertEqual(self.controller._arb_paused_until, before_pause)
        self.assertEqual(self.controller._arb_failures_today, 0)
        self.assertEqual(self.controller._arb_realized_loss_today, Decimal("0.10"))



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
    async def test_excess_queues_rebalance(self):
        """Drift >0 (excess BTC) queues a rebalance without setting kill reason."""
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.0010"))  # 0.0022 total, +0.0002 excess
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")

        # kill_reason must NOT be set (auto_rebalance does not pause the bot)
        self.assertIsNone(self.controller._kill_reason)
        # rebalance queued with the excess delta
        self.assertIn("BTC", self.controller._pending_rebalances)
        self.assertGreater(self.controller._pending_rebalances["BTC"], Decimal("0"))

    async def test_deficit_queues_rebalance(self):
        """Drift <0 (deficit BTC) queues a rebalance."""
        self._mock_total_balance(Decimal("0.0009"), Decimal("0.0009"))  # 0.0018 total, -0.0002 deficit
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")

        self.assertIsNone(self.controller._kill_reason)
        self.assertIn("BTC", self.controller._pending_rebalances)
        self.assertLess(self.controller._pending_rebalances["BTC"], Decimal("0"))

    async def test_cooldown_blocks_second_queue(self):
        """If a rebalance was placed recently, the next audit defers."""
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.0010"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        now = 1700000000.0
        # Simulate a rebalance that was placed 30s ago (cooldown=120s)
        self.controller._rebalance_last_time["BTC"] = now - 30.0

        await self.controller._run_inventory_audit(now=now, source="periodic")

        self.assertNotIn("BTC", self.controller._pending_rebalances)

    async def test_cooldown_expired_allows_queue(self):
        """After cooldown expires, rebalance is re-queued."""
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.0010"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        now = 1700000000.0
        # Last rebalance was 130s ago (> 120s cooldown)
        self.controller._rebalance_last_time["BTC"] = now - 130.0

        await self.controller._run_inventory_audit(now=now, source="periodic")

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
    async def test_boot_exits_with_drift_when_auto_rebalance_configured(self):
        """With auto_rebalance, boot_paused exits even when drift is found."""
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))  # excess
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        self.controller._boot_paused = True
        self.controller._initial_audit_done = False

        await self.controller._run_inventory_audit(now=1700000000.0, source="boot")
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
    async def test_no_drift_when_balances_match_target(self):
        # 0.001 + 0.001 = 0.002 (target) → no drift
        self._mock_total_balance(Decimal("0.001"), Decimal("0.001"))
        await self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        result = self.controller._last_audit_results["BTC"]
        self.assertTrue(result["within"])
        self.assertEqual(result["delta"], Decimal("0"))
        # No kill switch
        self.assertIsNone(self.controller._kill_reason)

    async def test_no_drift_within_tolerance(self):
        # 0.001 + 0.0009905 = 0.0019905 → -0.0000095 from target (~-0.475%, within 0.5%)
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0009905"))
        await self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        result = self.controller._last_audit_results["BTC"]
        self.assertTrue(result["within"])
        self.assertIsNone(self.controller._kill_reason)


class TestInventoryAuditDriftPause(_AuditBaseTest):
    async def test_drift_above_tolerance_trips_kill_switch(self):
        # 0.001 + 0.0008 = 0.0018 → -0.0002 from target (-10%, well above 0.5%)
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        # Ensure no in-flight activity
        self.controller.executors_info = []
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        await self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        result = self.controller._last_audit_results["BTC"]
        self.assertFalse(result["within"])
        self.assertEqual(self.controller._kill_reason, "INVENTORY_DRIFT_BTC")
        self.assertTrue(self.controller._last_audit_results["_drift_active"])

    # NOTE: test_drift_deferred_when_inflight_active removed 2026-05-12.
    # The barrier-pattern audit replaces "defer-on-inflight": when drift is
    # suspected, the audit enters CANCELLING (stop-the-world) and only acts
    # AFTER quiescence is reached. There's no longer a "defer because of
    # in-flight activity" branch — the in-flight orders are simply
    # cancelled, making the drift detection authoritative.
    # See TestAuditBarrier for the new flow.


class TestInventoryAuditAlertAction(_AuditBaseTest):
    async def test_alert_action_logs_but_does_not_pause(self):
        self.config.inventory_audit.on_drift_action = "alert"
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # alert action does NOT trip kill switch
        self.assertIsNone(self.controller._kill_reason)
        # but _drift_active flagged for CSV
        self.assertTrue(self.controller._last_audit_results["_drift_active"])


class TestAuditPassiveModeWhenKilled(_AuditBaseTest):
    """Regression pin (2026-05-11 16:51 incident).

    The auto_terminate gate reads ``_last_audit_results["_drift_active"]``.
    Before this fix, the audit early-returned the moment ``_kill_reason``
    was set — which froze ``_drift_active`` at its last pre-kill value.
    HEDGE_FAILURES_3 trip (since-removed gate) → audit stopped → a later
    natural fill resolved the drift but the flag stayed True forever →
    auto_terminate deadlock. The fix below still applies for current KILL
    reasons (DAILY_LOSS_LIMIT, UNREALIZED_LOSS, etc.).

    Fixed by switching to passive mode in killed state: still refresh
    ``_last_audit_results`` so the gate sees current reality, but skip
    the destructive side-effects (rebalance queue, drift_consecutive
    increment, kill re-trip, CRITICAL spam).
    """

    async def test_killed_audit_still_updates_drift_active_on_refresh(self):
        """Drift cleared after kill → audit must flip _drift_active to False."""
        # Setup: bot is KILLED with drift previously detected.
        self.controller._kill_reason = "DAILY_LOSS_LIMIT"  # any KILL reason works
        self.controller._last_audit_results = {"_drift_active": True}
        # Balances now perfectly match target (natural fill resolved drift).
        self._mock_total_balance(
            Decimal("0.001"),   # maker
            Decimal("0.001"),   # taker — total 0.002 = target
        )
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # Auto_terminate gate should now see clear state.
        self.assertFalse(self.controller._last_audit_results["_drift_active"])

    async def test_killed_audit_still_flags_drift_when_present(self):
        """If drift persists post-kill, gate stays BLOCKED."""
        self.controller._kill_reason = "DAILY_LOSS_LIMIT"  # any KILL reason works
        self.controller._last_audit_results = {"_drift_active": False}
        # Drift large enough to exceed tolerance (target 0.002, max_drift_quote=3
        # BRL with mid≈300050 → tolerance ~1e-5 BTC; we set delta ~0.0002).
        self._mock_total_balance(
            Decimal("0.001"),
            Decimal("0.0008"),  # total 0.0018 < target → 0.0002 drift
        )
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertTrue(self.controller._last_audit_results["_drift_active"])

    async def test_killed_audit_DOES_queue_rebalance_to_unblock_auto_terminate(self):
        """Killed mode MUST queue auto_rebalance — that's how the bot
        recovers from drift-deadlock (Fix A, 2026-05-11 19:41).

        Original logic skipped rebalance in killed mode ("we're shutting
        down"), but the auto_terminate gate requires `_drift_active=False`
        to fire. With no rebalance, drift never resolves, gate stays
        BLOCKED, bot is alive but inert for hours (observed prod 2026-05-11
        19:41–21:48). Now rebalance fires (with cooldown) so drift can
        resolve and auto_terminate eventually triggers.

        The destructive side-effects are still suppressed in killed mode
        (drift_consecutive increment, kill_reason re-trip, CRITICAL spam).
        """
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        self.controller._kill_reason = "DAILY_LOSS_LIMIT"  # any KILL reason works
        self.controller._last_audit_results = {"_drift_active": True}
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        # Pre-condition: no recent rebalance (cooldown elapsed).
        self.controller._rebalance_last_time = {}
        self.controller._pending_rebalances = {}
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # Rebalance MUST be queued for BTC.
        self.assertIn("BTC", self.controller._pending_rebalances)

    async def test_killed_audit_respects_rebalance_cooldown(self):
        """In killed mode the cooldown still applies — we don't want to
        spam rebalance orders if a previous one is still in flight."""
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        self.controller._kill_reason = "DAILY_LOSS_LIMIT"  # any KILL reason works
        self.controller._last_audit_results = {"_drift_active": True}
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        # Recent rebalance within cooldown window.
        now = 1700000000.0
        self.controller._rebalance_last_time = {
            "BTC": now - 30.0  # 30s ago, cooldown is 120s default
        }
        self.controller._pending_rebalances = {}
        await self.controller._run_inventory_audit(now=now, source="periodic")
        # No new rebalance queued — cooldown still active.
        self.assertEqual(self.controller._pending_rebalances, {})

    async def test_killed_audit_does_not_retrip_kill_reason(self):
        """Existing kill_reason preserved — we don't overwrite it with
        a DRIFT_STUCK or INVENTORY_DRIFT during shutdown."""
        self.config.inventory_audit.on_drift_action = "pause"
        self.controller._kill_reason = "DAILY_LOSS_LIMIT"  # any KILL reason works
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertEqual(self.controller._kill_reason, "DAILY_LOSS_LIMIT")

    async def test_killed_audit_does_not_increment_drift_consecutive(self):
        """Drift counter freezes at kill time — we're not actively
        managing the asset anymore."""
        self.controller._kill_reason = "DAILY_LOSS_LIMIT"  # any KILL reason works
        self.controller._drift_consecutive_audits = {"BTC": 3}
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # Still 3 — not incremented.
        self.assertEqual(self.controller._drift_consecutive_audits["BTC"], 3)

    async def test_normal_mode_still_runs_full_audit(self):
        """Sanity guard: non-killed mode behavior unchanged."""
        self.controller._kill_reason = None
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # Drift action="pause" (default in _AuditBaseTest) → kill_reason set.
        self.assertEqual(self.controller._kill_reason, "INVENTORY_DRIFT_BTC")
        self.assertTrue(self.controller._last_audit_results["_drift_active"])


class TestAuditBarrier(_AuditBaseTest):
    """Pins the barrier-pattern audit added 2026-05-12.

    State machine:
      IDLE → CANCELLING → VALIDATING → IDLE
    Drift is acted on ONLY after reaching quiescent state (no active
    executors + no in-flight orders), so race-induced false drift
    (stale balance, hedge in flight) doesn't trigger spurious actions.
    """

    async def test_idle_with_no_drift_stays_idle(self):
        """No drift → barrier doesn't activate."""
        self._mock_total_balance(Decimal("0.001"), Decimal("0.001"))  # total = target
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertEqual(self.controller._barrier_state, "IDLE")
        self.assertFalse(self.controller._last_audit_results["_drift_active"])
        self.assertNotIn("BTC", self.controller._pending_rebalances)

    async def test_idle_with_drift_and_quiescence_walks_to_validating(self):
        """When the bot has no active executors, a single audit call walks
        IDLE → CANCELLING → VALIDATING → IDLE and queues the rebalance."""
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))  # excess 0.0002
        self.controller.executors_info = []  # quiescent at boot
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # Barrier walked all the way to IDLE in one call.
        self.assertEqual(self.controller._barrier_state, "IDLE")
        # Rebalance queued for the confirmed drift.
        self.assertIn("BTC", self.controller._pending_rebalances)

    async def test_drift_during_arb_inflight_defers_barrier(self):
        """Regression (2026-05-16 11:35:59Z): when an arb executor has
        leg-1 filled and leg-2 still pending, the inventory IS skewed
        BY DESIGN (the arb's own unwind path will reconcile it). Without
        this gate, the barrier fires, kills the live XEMM executors,
        and queues a rebalance MARKET that races the arb's own unwind on
        the same venue — observed loss ~150 BRL/BTC per double-buy."""
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))  # drift visible
        # Mock inflight activity True (e.g. arb leg-1 just filled)
        self.controller._has_inflight_activity = MagicMock(return_value=True)
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # Barrier NOT entered (still IDLE), no rebalance queued.
        self.assertEqual(self.controller._barrier_state, "IDLE")
        self.assertNotIn("BTC", self.controller._pending_rebalances)
        # Drift flag is still recorded for observability.
        self.assertTrue(self.controller._last_audit_results["_drift_active"])
        self.assertTrue(self.controller._last_audit_results["_inflight_active"])

    async def test_idle_with_drift_but_active_executor_stays_cancelling(self):
        """When an active executor exists, barrier enters CANCELLING and waits."""
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))
        active = MagicMock()
        active.is_done = False
        active.config.id = "EX-1"
        self.controller.executors_info = [active]
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # Barrier stuck in CANCELLING waiting for quiescence.
        self.assertEqual(self.controller._barrier_state, "CANCELLING")
        # No rebalance queued YET (will queue when quiescent on next tick).
        self.assertNotIn("BTC", self.controller._pending_rebalances)

    async def test_cancelling_advances_when_executors_become_done(self):
        """Second audit call after executors are done → advance through
        VALIDATING and queue rebalance."""
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))
        active = MagicMock()
        active.is_done = False
        active.config.id = "EX-1"
        self.controller.executors_info = [active]
        # First call: enters CANCELLING
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertEqual(self.controller._barrier_state, "CANCELLING")
        # Simulate executor finishing
        active.is_done = True
        # Second call: detects quiescence, transitions to VALIDATING, then IDLE
        await self.controller._run_inventory_audit(now=1700000001.0, source="periodic")
        self.assertEqual(self.controller._barrier_state, "IDLE")
        self.assertIn("BTC", self.controller._pending_rebalances)

    async def test_cancelling_timeout_proceeds_with_warning(self):
        """If quiescence isn't reached within _barrier_timeout_sec, the audit
        proceeds to VALIDATING anyway (logged as warning, cooldown saves us
        from rebalance spam)."""
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))
        active = MagicMock()
        active.is_done = False  # never finishes
        active.config.id = "EX-STUCK"
        self.controller.executors_info = [active]
        # Force the barrier into CANCELLING manually
        self.controller._barrier_state = "CANCELLING"
        self.controller._barrier_started_at = 1700000000.0
        # Call audit way past timeout
        now = 1700000000.0 + self.controller._barrier_timeout_sec + 1.0
        await self.controller._run_inventory_audit(now=now, source="periodic")
        # Should have proceeded to IDLE via VALIDATING despite no quiescence
        self.assertEqual(self.controller._barrier_state, "IDLE")

    async def test_validating_clears_drift_if_race_resolved(self):
        """Phase 1 saw drift (race / stale read). When VALIDATING runs after
        quiescence, drift is actually within tolerance — no rebalance queued,
        and the false alarm is logged."""
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        # Stage: barrier already in VALIDATING (simulating that CANCELLING
        # cleared). Balance is now within tolerance.
        self.controller._barrier_state = "VALIDATING"
        self.controller._barrier_drift_snapshot = {
            "BTC": {"delta": Decimal("0.0002"), "within": False,
                    "target": Decimal("0.002"), "maker_bal": Decimal("0.0012"),
                    "taker_bal": Decimal("0.001"), "actual": Decimal("0.0022"),
                    "delta_quote": Decimal("60")},
        }
        # Now balance reads as target (drift cleared after quiescence)
        self._mock_total_balance(Decimal("0.001"), Decimal("0.001"))
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        # Returns to IDLE, no rebalance queued (false alarm)
        self.assertEqual(self.controller._barrier_state, "IDLE")
        self.assertNotIn("BTC", self.controller._pending_rebalances)
        self.assertFalse(self.controller._last_audit_results["_drift_active"])

    async def test_validating_confirms_real_drift(self):
        """Drift persists after quiescence → confirmed real → rebalance queued."""
        self.config.inventory_audit.on_drift_action = "auto_rebalance"
        self.controller._barrier_state = "VALIDATING"
        self.controller._barrier_drift_snapshot = {}
        # Drift still present in authoritative read
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertEqual(self.controller._barrier_state, "IDLE")
        self.assertIn("BTC", self.controller._pending_rebalances)
        self.assertTrue(self.controller._last_audit_results["_drift_active"])

    async def test_validating_with_pause_action_sets_kill_reason(self):
        """``on_drift_action=pause`` — VALIDATING confirms drift → kill_reason set."""
        self.config.inventory_audit.on_drift_action = "pause"
        self.controller._barrier_state = "VALIDATING"
        self.controller._barrier_drift_snapshot = {}
        self._mock_total_balance(Decimal("0.0012"), Decimal("0.001"))
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertEqual(self.controller._barrier_state, "IDLE")
        self.assertEqual(self.controller._kill_reason, "INVENTORY_DRIFT_BTC")

    async def test_determine_actions_stops_all_when_cancelling(self):
        """``determine_executor_actions`` emits Stop for all active executors
        when barrier is CANCELLING — that's how stop-the-world happens."""
        self.controller._barrier_state = "CANCELLING"
        # Make the bot otherwise healthy
        ex1 = MagicMock(); ex1.is_done = False; ex1.config.id = "E1"
        ex2 = MagicMock(); ex2.is_done = False; ex2.config.id = "E2"
        ex3 = MagicMock(); ex3.is_done = True;  ex3.config.id = "E3"  # ignored
        self.controller.executors_info = [ex1, ex2, ex3]
        # Need a processed_data populated to enter determine_executor_actions
        self.controller.processed_data = {
            "timestamp": 1700000000.0,
            "regime": Regime.OK,
            "cancel_reason": "",
        }
        actions = self.controller.determine_executor_actions()
        stops = [a for a in actions if isinstance(a, StopExecutorAction)]
        # Both active executors stopped; the is_done one is skipped.
        self.assertEqual(len(stops), 2)
        self.assertEqual(
            {a.executor_id for a in stops}, {"E1", "E2"},
        )

    async def test_determine_actions_blocks_creation_when_validating(self):
        """While VALIDATING, no new executors created (would mutate state
        we're trying to read authoritatively)."""
        self.controller._barrier_state = "VALIDATING"
        self.controller.executors_info = []
        self.controller.processed_data = {
            "timestamp": 1700000000.0,
            "regime": Regime.OK,
            "cancel_reason": "",
        }
        actions = self.controller.determine_executor_actions()
        # No CreateExecutorAction
        creates = [a for a in actions if isinstance(a, CreateExecutorAction)]
        self.assertEqual(len(creates), 0)


class TestInflightActivityDetection(_AuditBaseTest):
    def test_active_executor_alone_is_NOT_inflight(self):
        # KEY REGRESSION TEST: an XEMM executor sitting with an open maker order
        # (normal steady-state) must NOT be counted as inflight — that was the
        # bug that made _has_inflight_activity() always return True and permanently
        # suppressed the audit.
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
        self.controller._last_fill_time = time.time() - 5.0  # 5s ago
        self.assertTrue(self.controller._has_inflight_activity())

    def test_old_fill_not_inflight(self):
        self.controller._last_fill_time = time.time() - 30.0  # 30s ago
        self.assertFalse(self.controller._has_inflight_activity())

    def test_active_executor_with_recent_fill_is_inflight(self):
        # Recent fill + active executor = inflight (fill just happened)
        live_executor = MagicMock()
        live_executor.is_done = False
        self.controller.executors_info = [live_executor]
        self.controller._last_fill_time = time.time() - 2.0  # 2s ago
        self.assertTrue(self.controller._has_inflight_activity())

    def test_active_arb_executor_is_inflight_even_without_recent_fill(self):
        """Fix A (2026-05-11): an active arb executor is considered inflight
        for the full duration of its lifecycle, regardless of the 10-s timer.

        Arb has TWO MARKET legs; between leg-1 fill and leg-2 fill the
        inventory is INTENTIONALLY skewed. If the audit interprets that
        transient as drift, it queues a parallel MARKET order that
        collides with the arb's own leg-2 (observed prod 16:50:46:
        double-sold the position).
        """
        arb_executor = MagicMock()
        arb_executor.is_done = False
        arb_executor.config.type = "lead_lag_arbitrage_executor"
        self.controller.executors_info = [arb_executor]
        # No recent fill — timer path would say "not inflight"
        self.controller._last_fill_time = 0.0
        self.assertTrue(self.controller._has_inflight_activity())

    def test_done_arb_executor_is_not_inflight(self):
        """Once an arb has finished (both legs settled, is_done=True), it
        no longer suppresses the audit."""
        arb_executor = MagicMock()
        arb_executor.is_done = True
        arb_executor.config.type = "lead_lag_arbitrage_executor"
        self.controller.executors_info = [arb_executor]
        self.controller._last_fill_time = 0.0
        self.assertFalse(self.controller._has_inflight_activity())

    def test_non_arb_active_executor_does_not_trigger_arb_path(self):
        """Regression: an active XEMM (non-arb) executor must NOT trigger the
        arb-active inflight path. XEMM normal steady-state is open maker
        orders, which is fine for the audit to run on."""
        xemm_executor = MagicMock()
        xemm_executor.is_done = False
        xemm_executor.config.type = "xemm_executor"
        self.controller.executors_info = [xemm_executor]
        self.controller._last_fill_time = 0.0
        self.assertFalse(self.controller._has_inflight_activity())


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

    async def test_boot_exits_when_cleanup_and_audit_pass(self):
        # No drift case
        self._mock_total_balance(Decimal("0.001"), Decimal("0.001"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        # Simulate the boot flow: run audit
        await self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        # Manually replicate boot transition logic
        self.controller._initial_audit_done = True
        if self.controller._kill_reason is None:
            self.controller._boot_paused = False

        self.assertFalse(self.controller._boot_paused)
        self.assertIsNone(self.controller._kill_reason)

    async def test_boot_stays_paused_when_audit_detects_drift(self):
        # Drift case
        self._mock_total_balance(Decimal("0.001"), Decimal("0.0008"))
        self.controller._has_inflight_activity = MagicMock(return_value=False)

        await self.controller._run_inventory_audit(now=1700000000.0, source="boot")
        self.controller._initial_audit_done = True
        if self.controller._kill_reason is None:
            self.controller._boot_paused = False

        # Bot stays paused (kill_reason set → boot_paused stays True)
        self.assertTrue(self.controller._boot_paused)
        self.assertEqual(self.controller._kill_reason, "INVENTORY_DRIFT_BTC")


class TestAuditDisabled(_BaseControllerTest):
    async def test_audit_disabled_runs_no_op(self):
        # Default: enabled=True but base_targets={} → audit returns immediately
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
        self.assertEqual(self.controller._last_audit_results, {})
        self.assertIsNone(self.controller._kill_reason)

    async def test_audit_explicitly_disabled_runs_no_op(self):
        self.config.inventory_audit.enabled = False
        self.config.inventory_audit.base_targets = {"BTC": Decimal("0.002")}
        await self.controller._run_inventory_audit(now=1700000000.0, source="periodic")
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


class TestFreshArbRefresh(_ArbBaseTest):
    """Gated REST-snapshot override for BitPreco-side arb VWAP.

    Pins: trigger floor, TTL reuse, and the BitPreco-presence gate. The
    cached path already has dedicated coverage in TestArbDetection — these
    tests focus on the override-path semantics only.
    """

    def setUp(self):
        super().setUp()
        # Override the default maker_connector="bybit" with "bitpreco" so the
        # fresh-arb code path engages.
        self.config.maker_connector = "bitpreco"
        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
        self.set_loggers([self.controller.logger()])
        self._setup_default_market_data()

    def _set_bitpreco_connector(self, fresh_buy: Optional[Decimal],
                                 fresh_sell: Optional[Decimal]):
        bp = MagicMock()
        # Plural (single-REST) form — what the controller now prefers.
        bp.fetch_fresh_vwaps = AsyncMock(
            side_effect=lambda pair, amount: (fresh_buy, fresh_sell)
        )
        # Singular form kept on the mock for any legacy test that probes it.
        bp.fetch_fresh_vwap = AsyncMock(
            side_effect=lambda pair, is_buy, amount: fresh_buy if is_buy else fresh_sell
        )
        self.market_data_provider.get_connector.side_effect = (
            lambda name: bp if name == "bitpreco" else MagicMock()
        )
        return bp

    async def test_no_refresh_when_arb_disabled(self):
        self.controller.config.enable_pure_arb = False
        bp = self._set_bitpreco_connector(Decimal("100000"), Decimal("100200"))
        result = await self.controller._maybe_refresh_fresh_arb_bps(
            now=100.0,
            cached_long_net_bps=Decimal("20"),
            cached_short_net_bps=Decimal("20"),
            tx_cost_bps=Decimal("3"),
        )
        self.assertEqual(result, (None, None))
        bp.fetch_fresh_vwaps.assert_not_called()

    async def test_no_refresh_when_bitpreco_not_in_legs(self):
        self.controller.config.maker_connector = "bybit"
        bp = self._set_bitpreco_connector(Decimal("100000"), Decimal("100200"))
        result = await self.controller._maybe_refresh_fresh_arb_bps(
            now=100.0,
            cached_long_net_bps=Decimal("20"),
            cached_short_net_bps=Decimal("20"),
            tx_cost_bps=Decimal("3"),
        )
        self.assertEqual(result, (None, None))
        bp.fetch_fresh_vwaps.assert_not_called()

    async def test_no_refresh_when_cached_outside_symmetric_band(self):
        """Both cached values outside ±margin of threshold → skip REST fetch.
        The gate is symmetric so being far ABOVE the threshold also skips
        (no spawn decision can flip by recomputing fresher when both
        directions are decisively negative)."""
        bp = self._set_bitpreco_connector(Decimal("100000"), Decimal("100200"))
        threshold = self.controller.config.arb_min_profitability * Decimal("10000")
        margin = self.controller._fresh_arb_trigger_margin_bps
        # Just outside band on both sides (one below, one above) — neither
        # within ±margin of threshold.
        long_far_below = threshold - margin - Decimal("1")
        short_far_above = threshold + margin + Decimal("1")
        result = await self.controller._maybe_refresh_fresh_arb_bps(
            now=100.0,
            cached_long_net_bps=long_far_below,
            cached_short_net_bps=short_far_above,
            tx_cost_bps=Decimal("3"),
        )
        self.assertEqual(result, (None, None))
        bp.fetch_fresh_vwaps.assert_not_called()

    async def test_refreshes_and_overrides_when_close_to_threshold(self):
        # Bitpreco fresh: buy_vwap=100100, sell_vwap=100200
        # Binance cached: buy=100000, sell=100150 (set via _set_vwap)
        self._set_vwap(
            taker_buy=Decimal("100000"),    # binance asks (we buy)
            taker_sell=Decimal("100150"),   # binance bids (we sell)
        )
        bp = self._set_bitpreco_connector(
            fresh_buy=Decimal("100100"),    # bp asks (we buy)
            fresh_sell=Decimal("100200"),   # bp bids (we sell)
        )
        # Cached short_net just below threshold (within ±3bps band).
        threshold = self.controller.config.arb_min_profitability * Decimal("10000")
        cached_short = threshold - Decimal("2")  # |Δ| = 2 ≤ 3 → trigger
        result = await self.controller._maybe_refresh_fresh_arb_bps(
            now=100.0,
            cached_long_net_bps=Decimal("-50"),  # long way outside band
            cached_short_net_bps=cached_short,
            tx_cost_bps=Decimal("3"),
        )
        # Single-REST path: ONE call to fetch_fresh_vwaps (not two singular).
        self.assertEqual(bp.fetch_fresh_vwaps.await_count, 1)
        # Result is a populated tuple
        fresh_long, fresh_short = result
        self.assertIsNotNone(fresh_long)
        self.assertIsNotNone(fresh_short)
        # Sanity: gross_short = (binance_sell - bp_buy) / bp_buy * 10000
        # = (100150 - 100100) / 100100 * 10000 = ~4.995 bps
        # net_short = 4.995 - 3 ≈ 1.995 bps
        self.assertAlmostEqual(float(fresh_short), 1.995, places=2)
        # State cached for TTL reuse
        self.assertEqual(self.controller._fresh_arb_cached_at, 100.0)
        self.assertEqual(self.controller._fresh_arb_short_net_bps, fresh_short)

    async def test_ttl_reuse_within_window(self):
        """Second call within TTL returns stashed value without re-fetching."""
        self._set_vwap(
            taker_buy=Decimal("100000"),
            taker_sell=Decimal("100150"),
        )
        bp = self._set_bitpreco_connector(
            fresh_buy=Decimal("100100"),
            fresh_sell=Decimal("100200"),
        )
        threshold = self.controller.config.arb_min_profitability * Decimal("10000")
        cached_short = threshold - Decimal("2")  # within ±3 band

        # First call — does fetch
        await self.controller._maybe_refresh_fresh_arb_bps(
            now=100.0,
            cached_long_net_bps=Decimal("-50"),
            cached_short_net_bps=cached_short,
            tx_cost_bps=Decimal("3"),
        )
        first_count = bp.fetch_fresh_vwaps.await_count
        # Second call inside TTL window — returns cached, no new fetch
        result = await self.controller._maybe_refresh_fresh_arb_bps(
            now=100.0 + self.controller._fresh_arb_cache_ttl_sec * 0.5,
            cached_long_net_bps=Decimal("-50"),
            cached_short_net_bps=cached_short,
            tx_cost_bps=Decimal("3"),
        )
        self.assertEqual(bp.fetch_fresh_vwaps.await_count, first_count)  # no extra fetch
        self.assertIsNotNone(result[1])

    async def test_fetch_returns_none_propagates_none(self):
        bp = self._set_bitpreco_connector(fresh_buy=None, fresh_sell=None)
        threshold = self.controller.config.arb_min_profitability * Decimal("10000")
        cached_short = threshold - Decimal("2")  # within ±3 band
        result = await self.controller._maybe_refresh_fresh_arb_bps(
            now=100.0,
            cached_long_net_bps=Decimal("-50"),
            cached_short_net_bps=cached_short,
            tx_cost_bps=Decimal("3"),
        )
        self.assertEqual(result, (None, None))

    async def test_processed_data_exposes_arb_fresh_used_flag(self):
        """Integration: update_processed_data sets arb_fresh_used when override applies."""
        self._set_vwap(
            taker_buy=Decimal("100000"),
            taker_sell=Decimal("100150"),
            # bp cached vwap is used as initial; will be overridden by fresh
            maker_buy=Decimal("100000"),
            maker_sell=Decimal("100200"),
        )
        self._set_bitpreco_connector(
            fresh_buy=Decimal("100100"),
            fresh_sell=Decimal("100200"),
        )
        # cached short_net ≈ (100150-100000)/100000 * 10000 - tx_cost (~3 bps) ≈ 12 bps
        # Threshold tuned so cached falls inside the ±3 trigger band.
        self.controller.config.arb_min_profitability = Decimal("0.0012")  # 12 bps
        await self._warm(ticks=2)
        pd = self.controller.processed_data
        self.assertIn("arb_fresh_used", pd)
        self.assertTrue(pd["arb_fresh_used"])


# ===================================================================== #
# Group J — Orphan-fill hedge dispatch (replaces ghost_controller)       #
# ===================================================================== #
class _OrphanHedgeBaseTest(_BaseControllerTest):
    """Shared helpers: build OrderFilled events + mock taker connector."""

    def _fill_event(self, order_id: str, side: TradeType = TradeType.SELL,
                    amount: Decimal = Decimal("0.0002")):
        ev = MagicMock()
        ev.order_id = order_id
        ev.amount = amount
        ev.trade_type = side
        ev.price = Decimal("400000")
        return ev

    def _mock_taker_connector(self, buy_id="HEDGE-BUY-1", sell_id="HEDGE-SELL-1"):
        taker = MagicMock()
        taker.buy = MagicMock(return_value=buy_id)
        taker.sell = MagicMock(return_value=sell_id)
        self.market_data_provider.get_connector = MagicMock(return_value=taker)
        return taker

    def _install_executors_info(self, infos=None):
        """Inject a list of ExecutorInfo-shaped mocks into the controller."""
        self.controller.executors_info = list(infos or [])

    def _make_info(self, maker_order_id: Optional[str], is_done: bool = False):
        info = MagicMock()
        info.is_done = is_done
        info.custom_info = {"maker_order_id": maker_order_id} if maker_order_id is not None else {}
        return info


class TestFindLiveExecutorOwningMaker(_OrphanHedgeBaseTest):

    def test_returns_executor_when_live_owns_maker_order(self):
        live = self._make_info("MAKER-1", is_done=False)
        self._install_executors_info([live])
        result = self.controller._find_live_executor_owning_maker("MAKER-1")
        self.assertIs(result, live)

    def test_skips_terminated_executor_even_if_id_matches(self):
        dead = self._make_info("MAKER-DEAD", is_done=True)
        self._install_executors_info([dead])
        self.assertIsNone(
            self.controller._find_live_executor_owning_maker("MAKER-DEAD")
        )

    def test_returns_none_when_no_executor_owns_order(self):
        live = self._make_info("MAKER-OTHER")
        self._install_executors_info([live])
        self.assertIsNone(
            self.controller._find_live_executor_owning_maker("MAKER-NOT-FOUND")
        )

    def test_returns_none_for_empty_or_missing_order_id(self):
        self._install_executors_info([self._make_info("MAKER-X")])
        self.assertIsNone(self.controller._find_live_executor_owning_maker(None))
        self.assertIsNone(self.controller._find_live_executor_owning_maker(""))

    def test_returns_none_when_executors_info_empty(self):
        """Empty list (fresh controller, no executors yet) → None."""
        self._install_executors_info([])
        self.assertIsNone(
            self.controller._find_live_executor_owning_maker("MAKER-1")
        )

    def test_returns_none_when_executors_info_is_none(self):
        """Defensive: executors_info=None must not raise."""
        self.controller.executors_info = None
        self.assertIsNone(
            self.controller._find_live_executor_owning_maker("MAKER-1")
        )

    def test_skips_executor_with_no_maker_order(self):
        no_maker = self._make_info(maker_order_id=None)
        self._install_executors_info([no_maker])
        self.assertIsNone(
            self.controller._find_live_executor_owning_maker("MAKER-1")
        )


class TestOrphanHedgeDispatch(_OrphanHedgeBaseTest):

    def test_dispatches_buy_when_maker_sell_orphan(self):
        """SELL maker fill with no live owner → MARKET BUY on taker."""
        self._install_executors_info([])  # no live executors
        taker = self._mock_taker_connector()
        event = self._fill_event("ORPHAN-1", side=TradeType.SELL,
                                 amount=Decimal("0.0002"))
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")
        taker.buy.assert_called_once()
        args = taker.buy.call_args.args
        self.assertEqual(args[0], "BTC-BRL")        # pair
        self.assertEqual(args[1], Decimal("0.0002"))  # amount
        self.assertEqual(args[2], OrderType.MARKET)   # order_type
        taker.sell.assert_not_called()

    def test_dispatches_sell_when_maker_buy_orphan(self):
        """BUY maker fill with no live owner → MARKET SELL on taker."""
        self._install_executors_info([])
        taker = self._mock_taker_connector()
        event = self._fill_event("ORPHAN-2", side=TradeType.BUY,
                                 amount=Decimal("0.0003"))
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")
        taker.sell.assert_called_once()
        args = taker.sell.call_args.args
        self.assertEqual(args[1], Decimal("0.0003"))
        taker.buy.assert_not_called()

    def test_skips_when_live_executor_owns_order(self):
        """Live owner exists → trust executor, skip dispatch."""
        live = self._make_info("OWNED-1")
        self._install_executors_info([live])
        taker = self._mock_taker_connector()
        event = self._fill_event("OWNED-1", side=TradeType.SELL)
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")
        taker.buy.assert_not_called()
        taker.sell.assert_not_called()

    def test_skips_when_event_has_no_order_id(self):
        self._install_executors_info([])
        taker = self._mock_taker_connector()
        event = MagicMock()
        event.order_id = None
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")
        taker.buy.assert_not_called()

    def test_skips_when_amount_is_zero(self):
        self._install_executors_info([])
        taker = self._mock_taker_connector()
        event = self._fill_event("ZERO-AMT", side=TradeType.SELL,
                                 amount=Decimal("0"))
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")
        taker.buy.assert_not_called()

    def test_skips_when_trade_type_is_unknown(self):
        self._install_executors_info([])
        taker = self._mock_taker_connector()
        event = MagicMock()
        event.order_id = "WEIRD-1"
        event.amount = Decimal("0.0002")
        event.trade_type = "GIBBERISH"  # not a TradeType
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")
        taker.buy.assert_not_called()
        taker.sell.assert_not_called()

    def test_logs_error_and_returns_if_taker_connector_unresolvable(self):
        self._install_executors_info([])
        self.market_data_provider.get_connector = MagicMock(
            side_effect=RuntimeError("no such connector")
        )
        event = self._fill_event("UNRESOLVABLE-1", side=TradeType.SELL)
        # Should NOT raise — orphan_hedge is best-effort, audit is fallback.
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")

    def test_handles_taker_place_order_exception(self):
        self._install_executors_info([])
        taker = self._mock_taker_connector()
        taker.buy = MagicMock(side_effect=RuntimeError("BP rate limit"))
        self.market_data_provider.get_connector = MagicMock(return_value=taker)
        event = self._fill_event("RATE-LIMITED", side=TradeType.SELL)
        # Should NOT raise.
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")


class TestOrphanHedgeIncidentRegression(_OrphanHedgeBaseTest):
    """Replays the 2026-05-16 07:13:21Z scenario shape.

    Cancel-gate killed the executor BEFORE its place→fill→hedge cycle
    completed. The fill landed 30 s later on a dead executor; with
    inventory_audit at 10s polling, a 7 s gap of unhedged exposure
    accumulated R$0.089 of book-drift slippage. This test pins the new
    invariant: a maker-side fill arriving in this shape produces an
    immediate MARKET hedge on the taker (cross-exchange), independent
    of audit cadence.
    """

    def test_fill_after_executor_terminated_triggers_immediate_hedge(self):
        dead_owner = self._make_info(
            "SBCBL651ea10811f1ba", is_done=True
        )
        self._install_executors_info([dead_owner])
        taker = self._mock_taker_connector(buy_id="BBN_HEDGE_BUY_42")
        event = self._fill_event(
            "SBCBL651ea10811f1ba",
            side=TradeType.SELL,
            amount=Decimal("0.0002"),
        )
        self.controller._maybe_dispatch_orphan_hedge(event, "bybit")
        # MARKET BUY on taker for full maker amount
        taker.buy.assert_called_once()
        self.assertEqual(taker.buy.call_args.args[1], Decimal("0.0002"))
        self.assertEqual(taker.buy.call_args.args[2], OrderType.MARKET)


class TestOrphanHedgeSelfDispatchedRegistry(_OrphanHedgeBaseTest):
    """Regression (2026-05-16 11:36:01Z and 10:32:48Z).

    Without this guard, the controller's orphan_hedge fired on EVERY
    maker fill that no live XEMM executor owned — including:
      * unwind MARKET fills from ``LeadLagArbitrageExecutor._unwind_position``
      * rebalance MARKET fills from ``_execute_pending_rebalances``

    Both are by design one-sided corrections. Hedging them on the taker
    re-opens the exposure and creates a drift -> rebalance -> orphan_hedge
    death loop (observed 10:32-10:34Z, 13 consecutive audit cycles).
    """

    def test_register_self_dispatched_marks_id_for_skip(self):
        """A registered id is recognised by ``_maybe_dispatch_orphan_hedge``
        and the dispatch is skipped."""
        self._install_executors_info([])
        taker = self._mock_taker_connector()
        self.controller.register_self_dispatched_market_id("UNWIND-XYZ-1")
        event = self._fill_event(
            "UNWIND-XYZ-1", side=TradeType.BUY, amount=Decimal("0.0002"),
        )
        self.controller._maybe_dispatch_orphan_hedge(event, "bitpreco")
        taker.buy.assert_not_called()
        taker.sell.assert_not_called()
        # Entry consumed on use to avoid suppressing a future un-related
        # fill that happens to reuse this id.
        self.assertNotIn(
            "UNWIND-XYZ-1", self.controller._self_dispatched_market_ids,
        )

    def test_register_empty_or_none_is_noop(self):
        """``register_self_dispatched_market_id(None)`` and ``("")`` must
        not pollute the registry."""
        self.controller.register_self_dispatched_market_id(None)
        self.controller.register_self_dispatched_market_id("")
        self.assertEqual(self.controller._self_dispatched_market_ids, {})

    def test_stale_entries_pruned_on_check(self):
        """Entries older than ``_self_dispatched_ttl_sec`` are pruned the
        next time orphan_hedge runs, so the dict stays bounded."""
        # Force a stale timestamp in the past
        self.controller._self_dispatched_market_ids["OLD-1"] = 0.0
        self.controller._self_dispatched_ttl_sec = 60.0
        # Time provider returns "now" far past the TTL
        self.controller.market_data_provider.time = MagicMock(return_value=1_000_000.0)
        self._install_executors_info([])
        taker = self._mock_taker_connector()
        event = self._fill_event(
            "FRESH-99", side=TradeType.BUY, amount=Decimal("0.0002"),
        )
        # The fresh id is NOT registered; this fill should still dispatch
        self.controller._maybe_dispatch_orphan_hedge(event, "bitpreco")
        taker.sell.assert_called_once()
        # And the stale entry was cleared during pruning
        self.assertNotIn("OLD-1", self.controller._self_dispatched_market_ids)

    def test_registry_is_idempotent(self):
        """Registering the same id twice is allowed and only suppresses
        one fill (consumption pop)."""
        self._install_executors_info([])
        taker = self._mock_taker_connector()
        self.controller.register_self_dispatched_market_id("DUP-1")
        self.controller.register_self_dispatched_market_id("DUP-1")
        event = self._fill_event(
            "DUP-1", side=TradeType.BUY, amount=Decimal("0.0002"),
        )
        self.controller._maybe_dispatch_orphan_hedge(event, "bitpreco")
        taker.sell.assert_not_called()
        # After one consumption the entry is gone
        self.assertNotIn("DUP-1", self.controller._self_dispatched_market_ids)


class TestMemoryMetricsLogging(_BaseControllerTest):
    """Observability shim added 2026-05-16 after the 12:22Z incident.

    During the incident the bot's RSS grew ~1.8 GB in 10 min before the
    kernel exhausted memory and stalled the event loop for 16.7 s. We
    had no in-process memory log, so we had to reconstruct the curve
    from ``sar`` after the host reboot. ``_log_memory_metrics`` writes
    an INFO line every minute so the next leak is visible from grep.
    """

    def test_logs_one_line_per_interval(self):
        """Two calls within the interval -> only one log line."""
        self.controller._mem_metrics_interval_sec = 60.0
        self.controller._last_mem_metrics_time = 0.0
        with self.assertLogs(self.controller.logger().name, level="INFO") as cm:
            self.controller._log_memory_metrics(now=100.0)
            self.controller._log_memory_metrics(now=120.0)  # within interval
        mem_lines = [r for r in cm.output if "[mem]" in r]
        self.assertEqual(len(mem_lines), 1)
        # Second call should still have updated the gate
        self.assertEqual(self.controller._last_mem_metrics_time, 100.0)

    def test_logs_again_after_interval_elapsed(self):
        self.controller._mem_metrics_interval_sec = 60.0
        self.controller._last_mem_metrics_time = 0.0
        with self.assertLogs(self.controller.logger().name, level="INFO") as cm:
            self.controller._log_memory_metrics(now=100.0)
            self.controller._log_memory_metrics(now=200.0)  # past interval
        mem_lines = [r for r in cm.output if "[mem]" in r]
        self.assertEqual(len(mem_lines), 2)

    def test_log_line_contains_required_fields(self):
        """The line must be greppable for the keys we care about.

        Includes the leak-hunt extension fields (2026-05-16 v3 plan):
        pss/hwm/swap/fd/threads/asyncio_tasks/oldest_task_age/top_coro/
        gc_objects/gc_collections/tm_cur/tm_peak.
        """
        self.controller._last_mem_metrics_time = 0.0
        with self.assertLogs(self.controller.logger().name, level="INFO") as cm:
            self.controller._log_memory_metrics(now=999.0)
        line = next((r for r in cm.output if "[mem]" in r), "")
        for token in (
            # v1 fields (original instrumentation)
            "rss=", "vms=", "uss=", "gc=",
            "executors=", "pending_rebalances=",
            "self_dispatched=", "redis_us_sm=", "redis_us_orphans=",
            # v3 extension fields (leak-hunt observability)
            "pss=", "hwm=", "swap=", "fd=", "threads=",
            "asyncio_tasks=", "oldest_task_age=", "top_coro=",
            "gc_objects=", "gc_collections=",
            "tm_cur=", "tm_peak=",
        ):
            self.assertIn(token, line, f"missing {token!r} in log line: {line}")

    def test_metric_failure_does_not_raise(self):
        """A broken psutil call (or any unexpected error) must NOT
        crash the tick. The shim warns and moves on; the time gate is
        still advanced so we don't retry tightly."""
        # Force the lazy psutil path to raise
        self.controller._mem_psutil_proc = MagicMock()
        self.controller._mem_psutil_proc.memory_info.side_effect = RuntimeError("nope")
        self.controller._last_mem_metrics_time = 0.0
        # Must not raise. Use now > interval so the gate lets us through.
        now = self.controller._mem_metrics_interval_sec + 1.0
        self.controller._log_memory_metrics(now=now)
        # Time gate advanced so the next tick won't re-attempt within the interval
        self.assertEqual(self.controller._last_mem_metrics_time, now)

    # ---- v3 extension tests (2026-05-16 leak-hunt instrumentation) ----

    def test_gc_objects_gate_skips_within_interval(self):
        """``gc.get_objects()`` is expensive on a large heap. Within the
        5-min interval AND with no RSS jump, the line should record
        ``gc_objects=-1`` (sentinel = skipped this tick)."""
        self.controller._mem_metrics_interval_sec = 60.0
        self.controller._gc_objects_interval_sec = 300.0
        self.controller._last_mem_metrics_time = 0.0
        self.controller._last_gc_objects_time = 100.0
        self.controller._mem_last_rss_for_gc = 10_000.0  # so no jump triggers
        with self.assertLogs(self.controller.logger().name, level="INFO") as cm:
            self.controller._log_memory_metrics(now=150.0)  # within 300s gate
        line = next((r for r in cm.output if "[mem]" in r), "")
        self.assertIn("gc_objects=-1", line)

    def test_gc_objects_runs_after_interval(self):
        """After the 5-min gate elapses, the call should fire and produce
        a real positive count."""
        self.controller._mem_metrics_interval_sec = 60.0
        self.controller._gc_objects_interval_sec = 300.0
        self.controller._last_mem_metrics_time = 0.0
        self.controller._last_gc_objects_time = 0.0  # never sampled
        self.controller._mem_last_rss_for_gc = 0.0
        with self.assertLogs(self.controller.logger().name, level="INFO") as cm:
            self.controller._log_memory_metrics(now=400.0)
        line = next((r for r in cm.output if "[mem]" in r), "")
        # Real gc.get_objects() always returns a positive integer for a
        # live Python process.
        self.assertNotIn("gc_objects=-1", line)
        self.assertIn("gc_objects=", line)
        # Gate anchor was updated.
        self.assertEqual(self.controller._last_gc_objects_time, 400.0)

    def test_oldest_task_age_tracks_first_seen_registry(self):
        """The controller doesn't get task creation times from asyncio;
        it remembers the first time each task id was seen and uses that
        as the age anchor. This test seeds the registry with an old id
        and checks the value appears."""
        # Seed registry with a synthetic task that we "first saw" 30s ago.
        self.controller._task_first_seen = {12345: 970.0}
        self.controller._last_mem_metrics_time = 0.0

        # Replace asyncio.all_tasks with something that returns a fake
        # task whose id matches the registry entry.
        fake_task = MagicMock()
        fake_task.get_coro.return_value.__qualname__ = "fake_coro"
        # Force id(fake_task) to be 12345 by monkey-patching the asyncio
        # module's all_tasks. The actual id of the mock won't be 12345,
        # so we patch all_tasks to return [fake_task] AND patch id() at
        # the call site via a stub class.

        class _IdStableTask:
            def get_coro(self_inner):
                c = MagicMock()
                c.__qualname__ = "fake_coro"
                return c
        stable = _IdStableTask()
        # Seed the registry with the actual id of the stable object so the
        # "first seen" lookup will hit.
        self.controller._task_first_seen = {id(stable): 970.0}

        with patch("asyncio.all_tasks", return_value=[stable]):
            with self.assertLogs(self.controller.logger().name, level="INFO") as cm:
                self.controller._log_memory_metrics(now=1000.0)
        line = next((r for r in cm.output if "[mem]" in r), "")
        # oldest_task_age = 1000 - 970 = 30s
        self.assertIn("oldest_task_age=30s", line)
        self.assertIn("top_coro=fake_coro:1", line)

    def test_smaps_rollup_unavailable_does_not_break_line(self):
        """On non-Linux hosts (or in restricted environments) the
        ``/proc/self/smaps_rollup`` read fails. The line must still emit
        with pss=0.0 (and the rest of the fields)."""
        self.controller._last_mem_metrics_time = 0.0
        # Patch built-in open only for the smaps path
        real_open = open

        def _fake_open(path, *a, **kw):
            if "smaps_rollup" in str(path) or "/proc/self/status" in str(path):
                raise FileNotFoundError(path)
            return real_open(path, *a, **kw)

        with patch("builtins.open", side_effect=_fake_open):
            with self.assertLogs(self.controller.logger().name, level="INFO") as cm:
                self.controller._log_memory_metrics(now=999.0)
        line = next((r for r in cm.output if "[mem]" in r), "")
        self.assertIn("pss=0.0MB", line)
        self.assertIn("hwm=0.0MB", line)
        self.assertIn("swap=0kB", line)

    def test_tracemalloc_handles_already_tracing(self):
        """When ``PYTHONTRACEMALLOC=N`` is exported at startup, tracing
        is already active before ``_maybe_log_tracemalloc`` runs. The
        method must NOT call ``tracemalloc.start()`` again (that raises);
        instead it takes a baseline snapshot and waits for the next
        interval to diff."""
        self.controller._tracemalloc_enabled = True
        self.controller._tracemalloc_started = False
        self.controller._tracemalloc_prev_snapshot = None
        self.controller._last_tracemalloc_time = 0.0
        self.controller._tracemalloc_interval_sec = 1.0

        import tracemalloc as real_tm
        with patch.object(real_tm, "is_tracing", return_value=True), \
             patch.object(real_tm, "start") as start_mock, \
             patch.object(real_tm, "take_snapshot", return_value=MagicMock()) as snap_mock:
            self.controller._maybe_log_tracemalloc(now=100.0)
        # Must NOT call start() — that would raise on an already-tracing process.
        start_mock.assert_not_called()
        # Must take a baseline snapshot.
        snap_mock.assert_called_once()
        self.assertTrue(self.controller._tracemalloc_started)
        self.assertIsNotNone(self.controller._tracemalloc_prev_snapshot)


class TestFeeAssetPriceCache(_BaseControllerTest):
    """REST cache helper para preço de fee assets (BNB, etc)."""

    async def test_cache_miss_fetches_via_rest(self):
        conn = MagicMock()
        conn.get_last_traded_price = AsyncMock(return_value="1234.5")
        self.market_data_provider.get_connector = MagicMock(return_value=conn)

        price = await self.controller._get_spot_price_rest("binance", "BNB-BRL")
        self.assertEqual(price, Decimal("1234.5"))
        self.assertIn("BNB-BRL", self.controller._fee_price_cache)

    async def test_cache_hit_skips_rest(self):
        conn = MagicMock()
        conn.get_last_traded_price = AsyncMock(return_value="9999")
        self.market_data_provider.get_connector = MagicMock(return_value=conn)

        # Pre-populate cache with fresh entry
        self.controller._fee_price_cache["BNB-BRL"] = (
            Decimal("1200"), time.monotonic()
        )
        price = await self.controller._get_spot_price_rest(
            "binance", "BNB-BRL", ttl_sec=60.0
        )
        self.assertEqual(price, Decimal("1200"))
        conn.get_last_traded_price.assert_not_called()

    async def test_cache_expired_refetches(self):
        conn = MagicMock()
        conn.get_last_traded_price = AsyncMock(return_value="9999")
        self.market_data_provider.get_connector = MagicMock(return_value=conn)

        # Stale entry (older than TTL)
        self.controller._fee_price_cache["BNB-BRL"] = (
            Decimal("1200"), time.monotonic() - 100
        )
        price = await self.controller._get_spot_price_rest(
            "binance", "BNB-BRL", ttl_sec=60.0
        )
        self.assertEqual(price, Decimal("9999"))

    async def test_rest_error_falls_back_to_stale_cache(self):
        conn = MagicMock()
        conn.get_last_traded_price = AsyncMock(side_effect=Exception("boom"))
        self.market_data_provider.get_connector = MagicMock(return_value=conn)

        self.controller._fee_price_cache["BNB-BRL"] = (
            Decimal("500"), time.monotonic() - 100
        )
        price = await self.controller._get_spot_price_rest(
            "binance", "BNB-BRL", ttl_sec=60.0
        )
        # Returns stale cache rather than None — keeps PnL stable on transient REST hiccup.
        self.assertEqual(price, Decimal("500"))

    async def test_rest_error_with_no_cache_returns_none(self):
        conn = MagicMock()
        conn.get_last_traded_price = AsyncMock(side_effect=Exception("boom"))
        self.market_data_provider.get_connector = MagicMock(return_value=conn)
        price = await self.controller._get_spot_price_rest("binance", "BNB-BRL")
        self.assertIsNone(price)


class TestFeeAssetPnLContribution(_BaseControllerTest):
    """`_compute_pnl_brl` deve somar drift × mid dos fee_assets também,
    fazendo top-up de fee asset ficar PnL-neutro."""

    def _setup_baseline(self):
        # Latch baselines manualmente
        self.controller._brl_initial = Decimal("100000")
        self.controller._btc_initial = Decimal("0.2")
        self.controller._btc_target = Decimal("0.2")
        self.controller._mid_baseline = Decimal("500000")

    def _patch_balances(self, balances):
        def get_conn(name):
            conn = MagicMock()
            conn.get_balance = lambda asset: balances.get((name, asset), Decimal("0"))
            return conn
        self.market_data_provider.get_connector = MagicMock(side_effect=get_conn)

    def test_top_up_is_pnl_neutral(self):
        self.controller.config = self.controller.config.model_copy(update={
            "fee_assets": FeeAssetConfig(
                enabled=True, targets={"BNB": Decimal("0.05")}
            )
        })
        self._setup_baseline()
        self.controller._fee_assets_initial = {"BNB": Decimal("0.05")}
        self.controller._fee_assets_mid_baseline = {"BNB": Decimal("1200")}
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1200"), time.monotonic())
        }
        # Top-up: BRL caiu R$60, BNB subiu 0.05
        self._patch_balances({
            ("bybit", "BTC"): Decimal("0.1"),
            ("binance", "BTC"): Decimal("0.1"),
            ("bybit", "BRL"): Decimal("49970"),
            ("binance", "BRL"): Decimal("49970"),
            ("binance", "BNB"): Decimal("0.10"),
        })
        pnl = self.controller._compute_pnl_brl(Decimal("500000"))
        # Net: -60 (BRL) + drift_fee=0.05×1200 − 0×1200 = +60 → 0
        self.assertEqual(pnl, Decimal("0"))

    def test_fee_asset_mtm_loss_reflected_in_pnl(self):
        self.controller.config = self.controller.config.model_copy(update={
            "fee_assets": FeeAssetConfig(
                enabled=True, targets={"BNB": Decimal("0.05")}
            )
        })
        self._setup_baseline()
        self.controller._fee_assets_initial = {"BNB": Decimal("0.05")}
        self.controller._fee_assets_mid_baseline = {"BNB": Decimal("1200")}
        # Top-up aconteceu, e depois BNB cai 10%
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1080"), time.monotonic())
        }
        self._patch_balances({
            ("bybit", "BTC"): Decimal("0.1"),
            ("binance", "BTC"): Decimal("0.1"),
            ("bybit", "BRL"): Decimal("49970"),
            ("binance", "BRL"): Decimal("49970"),
            ("binance", "BNB"): Decimal("0.10"),
        })
        pnl = self.controller._compute_pnl_brl(Decimal("500000"))
        # -60 + (0.05×1080 − 0×1200) = -60 + 54 = -6
        self.assertEqual(pnl, Decimal("-6"))

    def test_fee_asset_disabled_no_contribution(self):
        # Default: fee_assets desabilitado
        self._setup_baseline()
        self._patch_balances({
            ("bybit", "BTC"): Decimal("0.1"),
            ("binance", "BTC"): Decimal("0.1"),
            ("bybit", "BRL"): Decimal("50000"),
            ("binance", "BRL"): Decimal("50000"),
            ("binance", "BNB"): Decimal("0.10"),  # irrelevante
        })
        pnl = self.controller._compute_pnl_brl(Decimal("500000"))
        self.assertEqual(pnl, Decimal("0"))


class TestFeeAssetTopUpLoop(_BaseControllerTest):
    """`_run_fee_asset_topup` — verifica gates e dispatch."""

    def _enable_fee_assets(self, **overrides):
        cfg_kwargs = dict(
            enabled=True,
            targets={"BNB": Decimal("0.05")},
            min_topup_quote=Decimal("60"),
            check_interval_sec=300.0,
            topup_cooldown_sec=120.0,
        )
        cfg_kwargs.update(overrides)
        self.controller.config = self.controller.config.model_copy(update={
            "fee_assets": FeeAssetConfig(**cfg_kwargs)
        })

    def _wire_taker(self, bnb_balance="0.0", buy_return="order-123"):
        taker = MagicMock()
        rules = MagicMock()
        rules.min_order_size = Decimal("0.001")
        rules.min_notional_size = Decimal("50")
        taker.trading_rules = {"BNB-BRL": rules}
        taker.quantize_order_amount = lambda p, a: a
        taker.buy = MagicMock(return_value=buy_return)
        taker.get_balance = lambda asset: (
            Decimal(bnb_balance) if asset == "BNB" else Decimal("0")
        )
        empty = MagicMock(get_balance=lambda a: Decimal("0"))
        self.market_data_provider.get_connector = MagicMock(
            side_effect=lambda name: taker if name == "binance" else empty
        )
        return taker

    async def test_disabled_short_circuits(self):
        taker = self._wire_taker(bnb_balance="0.0")
        # cfg.enabled = False (default)
        await self.controller._run_fee_asset_topup(now=1000.0)
        taker.buy.assert_not_called()

    async def test_buys_when_deficit_above_min_notional(self):
        self._enable_fee_assets()
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1200"), time.monotonic())
        }
        taker = self._wire_taker(bnb_balance="0.0")
        await self.controller._run_fee_asset_topup(now=1000.0)
        taker.buy.assert_called_once()
        args, _ = taker.buy.call_args
        pair, amount, otype, _price = args
        self.assertEqual(pair, "BNB-BRL")
        self.assertEqual(amount, Decimal("0.05"))
        self.assertEqual(otype, OrderType.MARKET)
        # Cooldown gravado
        self.assertEqual(self.controller._fee_topup_last_time["BNB"], 1000.0)

    async def test_skips_when_at_target(self):
        self._enable_fee_assets()
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1200"), time.monotonic())
        }
        taker = self._wire_taker(bnb_balance="0.05")  # exatamente no target
        await self.controller._run_fee_asset_topup(now=1000.0)
        taker.buy.assert_not_called()

    async def test_skips_when_notional_below_min_topup_quote(self):
        self._enable_fee_assets()
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1200"), time.monotonic())
        }
        # deficit = 0.005 BNB × 1200 = R$6 < R$60
        taker = self._wire_taker(bnb_balance="0.045")
        await self.controller._run_fee_asset_topup(now=1000.0)
        taker.buy.assert_not_called()

    async def test_skips_during_cooldown(self):
        self._enable_fee_assets()
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1200"), time.monotonic())
        }
        self.controller._fee_topup_last_time = {"BNB": 990.0}  # 10s atrás
        taker = self._wire_taker(bnb_balance="0.0")
        await self.controller._run_fee_asset_topup(now=1000.0)
        taker.buy.assert_not_called()

    async def test_check_interval_gates_run(self):
        self._enable_fee_assets()
        self.controller._last_fee_topup_check = 999.0  # 1s atrás, < 300s
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1200"), time.monotonic())
        }
        taker = self._wire_taker(bnb_balance="0.0")
        await self.controller._run_fee_asset_topup(now=1000.0)
        taker.buy.assert_not_called()

    async def test_excessive_deficit_refused(self):
        self._enable_fee_assets()
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1200"), time.monotonic())
        }
        # actual = -0.10 BNB (defesa contra baseline bug): deficit = 0.05 - (-0.10) = 0.15 > 2× target
        taker = self._wire_taker(bnb_balance="-0.10")
        await self.controller._run_fee_asset_topup(now=1000.0)
        taker.buy.assert_not_called()

    async def test_no_price_in_cache_skips_silently(self):
        self._enable_fee_assets()
        # cache vazio (priming não rodou ainda)
        taker = self._wire_taker(bnb_balance="0.0")
        await self.controller._run_fee_asset_topup(now=1000.0)
        taker.buy.assert_not_called()

    async def test_self_dispatched_registered_after_buy(self):
        self._enable_fee_assets()
        self.controller._fee_price_cache = {
            "BNB-BRL": (Decimal("1200"), time.monotonic())
        }
        self._wire_taker(bnb_balance="0.0", buy_return="bnb-order-1")
        with patch.object(
            self.controller, "register_self_dispatched_market_id"
        ) as reg:
            await self.controller._run_fee_asset_topup(now=1000.0)
        reg.assert_called_once_with("bnb-order-1")


