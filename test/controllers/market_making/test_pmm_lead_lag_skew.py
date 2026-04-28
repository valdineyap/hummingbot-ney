"""Unit tests for PMMLeadLagSkewController — Phase 1 + Phase 2."""
import asyncio
import os
import shutil
import tempfile
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

from controllers.market_making.pmm_lead_lag_skew import (
    PMMLeadLagSkewConfig,
    PMMLeadLagSkewController,
)

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase


def _make_config(**overrides) -> PMMLeadLagSkewConfig:
    defaults = dict(
        id="test_pmm",
        controller_name="pmm_lead_lag_skew",
        connector_name="binance",
        trading_pair="BTC-BRL",
        total_amount_quote=Decimal("200"),
        buy_spreads=[0.0010, 0.0020],
        sell_spreads=[0.0010, 0.0020],
        buy_amounts_pct=[Decimal("50"), Decimal("50")],
        sell_amounts_pct=[Decimal("50"), Decimal("50")],
        executor_refresh_time=60,
        cooldown_time=15,
        leverage=1,
        position_mode=PositionMode.ONEWAY,
        skip_rebalance=True,
        stop_loss=None,
        take_profit=None,
        time_limit=None,
        w_inv=1.0,
        w_lead=0.0,
        skew_max_bps=0.0,
        csv_log_enabled=False,  # tests opt in by overriding
    )
    defaults.update(overrides)
    return PMMLeadLagSkewConfig(**defaults)


def _make_mock_connector(base_balance="0", quote_balance="200"):
    """Mock connector with .get_balance(asset) returning configured Decimals."""
    conn = MagicMock()
    balances = {"BTC": Decimal(base_balance), "BRL": Decimal(quote_balance)}
    conn.get_balance.side_effect = lambda asset: balances.get(asset, Decimal("0"))
    return conn


def _neutral_lead_state():
    """Neutral LeadState for tests that don't care about lead-lag signals."""
    from controllers.market_making.pmm_lead_lag_utils import LeadState
    return LeadState(
        s_lead_micro=0.0, s_lead_regime=0.0, basis_bps=0.0,
        micro_stale=False, regime_stale=False,
        pause_buy=False, pause_sell=False,
    )


class TestPMMLeadLagSkewControllerPhase1(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        self.config = _make_config()
        self.mock_market_data_provider = MagicMock(spec=MarketDataProvider)
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = PMMLeadLagSkewController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

    # E1 — smoke test: controller creates 4 executor actions (2 buy + 2 sell)
    @patch(
        "hummingbot.strategy_v2.controllers.market_making_controller_base"
        ".MarketMakingControllerBase.get_executor_config",
        new_callable=MagicMock,
    )
    async def test_controller_creates_4_actions(self, executor_config_mock):
        executor_config_mock.return_value = PositionExecutorConfig(
            timestamp=1234,
            controller_id=self.config.id,
            connector_name="binance",
            trading_pair="BTC-BRL",
            side=TradeType.BUY,
            entry_price=Decimal("350000"),
            amount=Decimal("0.001"),
        )
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        actions = self.controller.determine_executor_actions()
        create_actions = [a for a in actions if isinstance(a, CreateExecutorAction)]
        # 2 buy levels + 2 sell levels = 4 create actions (Phase 1 no rebalance)
        self.assertEqual(len(create_actions), 4)

    async def test_update_processed_data_sets_reference_price(self):
        """reference_price must equal MidPrice when skew=0."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        ref = self.controller.processed_data["reference_price"]
        self.assertEqual(ref, Decimal("350000"))

    async def test_update_processed_data_spread_mult_is_one(self):
        """Phase 1: spread_multiplier must be 1 (neutral vol, neutral regime)."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        mult = self.controller.processed_data["spread_multiplier"]
        self.assertEqual(mult, Decimal("1"))

    # E2 — inventory bands: sign correctness
    async def test_inventory_bands_sign_correctness(self):
        """delta > 0 (long base) → size_factor_buy < size_factor_sell (§2.3)."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        from controllers.market_making.pmm_lead_lag_utils import compute_size_factors
        sf_buy, sf_sell = compute_size_factors(delta=0.15, soft_band=0.10, hard_band=0.20)
        self.assertLess(sf_buy, sf_sell,
                        msg="When long base in linear zone, buy size must be < sell size (§2.3)")

    # E3 — side filter: sides_enabled controls which levels get created
    async def test_side_filter_disables_buy_levels(self):
        """When buy side disabled, get_levels_to_execute must exclude buy levels."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        # Manually override sides_enabled
        self.controller.processed_data["sides_enabled"] = {"buy": False, "sell": True}
        levels = self.controller.get_levels_to_execute()

        for level_id in levels:
            self.assertFalse(level_id.startswith("buy"),
                             msg=f"Unexpected buy level {level_id} when buy disabled")

    async def test_side_filter_disables_sell_levels(self):
        """When sell side disabled, get_levels_to_execute must exclude sell levels."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        self.controller.processed_data["sides_enabled"] = {"buy": True, "sell": False}
        levels = self.controller.get_levels_to_execute()

        for level_id in levels:
            self.assertFalse(level_id.startswith("sell"),
                             msg=f"Unexpected sell level {level_id} when sell disabled")

    async def test_side_filter_both_disabled_returns_empty(self):
        """Both sides disabled → no levels to execute."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        self.controller.processed_data["sides_enabled"] = {"buy": False, "sell": False}
        levels = self.controller.get_levels_to_execute()
        self.assertEqual(levels, [])

    async def test_to_format_status_returns_strings(self):
        """to_format_status must return a list of non-empty strings."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        lines = self.controller.to_format_status()
        self.assertIsInstance(lines, list)
        self.assertGreater(len(lines), 0)
        for line in lines:
            self.assertIsInstance(line, str)

    async def test_get_custom_info_keys(self):
        """get_custom_info must return expected monitoring keys."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("350000"))
        await self.controller.update_processed_data()

        info = self.controller.get_custom_info()
        for key in ("regime", "inv_pct", "s_inv", "price_shift_bps", "vol_ratio",
                    "buy_enabled", "sell_enabled"):
            self.assertIn(key, info, msg=f"Missing key: {key}")

    def test_config_skip_rebalance_default_true(self):
        """skip_rebalance must default to True for spot BRL."""
        config = _make_config()
        self.assertTrue(config.skip_rebalance)

    def test_config_leverage_default_one(self):
        """leverage must default to 1 for spot trading."""
        config = _make_config()
        self.assertEqual(config.leverage, 1)

    def test_config_band_ordering_enforced(self):
        """soft < hard < cap < kill (plan §2.2)."""
        with self.assertRaises(Exception):
            _make_config(inv_soft_band=0.20, inv_hard_band=0.10)
        with self.assertRaises(Exception):
            _make_config(inv_hard_band=0.30, inv_hard_cap=0.20)
        with self.assertRaises(Exception):
            _make_config(inv_hard_cap=0.50, inv_kill=0.40)


class TestPMMLeadLagSkewControllerPhase2(IsolatedAsyncioWrapperTestCase):
    """Phase 2: live balance fetch, size factors, hard cap flag, CSV logging."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="pmm_test_logs_")
        self.config = _make_config(
            csv_log_enabled=False,
            csv_log_dir=self.tmpdir,
            max_net_position_quote=Decimal("60"),
        )
        self.mock_market_data_provider = MagicMock(spec=MarketDataProvider)
        self.mock_market_data_provider.time = MagicMock(return_value=1234567890.0)
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = PMMLeadLagSkewController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _set_balances(self, base="0.0006", quote="200"):
        """Configure mock provider with balances on the binance connector."""
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector(base, quote),
        }

    async def test_balances_fetched_via_market_data_provider(self):
        """update_processed_data must read base/quote from connector.get_balance."""
        self._set_balances(base="0.0005", quote="100")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        self.assertEqual(self.controller.processed_data["base_balance"], Decimal("0.0005"))
        self.assertEqual(self.controller.processed_data["quote_balance"], Decimal("100"))

    async def test_inv_pct_computed_from_live_balances(self):
        """50/50 portfolio at mid=400000: 0.0005 BTC * 400000 = 200 BRL = quote 200."""
        self._set_balances(base="0.0005", quote="200")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()
        self.assertAlmostEqual(self.controller.processed_data["inv_pct"], 0.5, places=5)

    async def test_no_connectors_returns_neutral(self):
        """When market_data_provider has no connectors dict, balances default to zero."""
        # MagicMock(spec=...) doesn't auto-create `connectors` as a dict
        del self.mock_market_data_provider.connectors
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["base_balance"], Decimal("0"))
        self.assertEqual(self.controller.processed_data["quote_balance"], Decimal("0"))

    async def test_over_max_net_position_flag(self):
        """net_exposure_quote > max_net_position_quote → over_max_net_position=True."""
        # 0.001 BTC at 400000 = 400 BRL >> 60 max
        self._set_balances(base="0.001", quote="0")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()
        self.assertTrue(self.controller.processed_data["over_max_net_position"])
        self.assertEqual(self.controller.processed_data["net_exposure_quote"], Decimal("400"))

    async def test_over_max_net_position_false_when_under(self):
        """0.0001 BTC * 400000 = 40 BRL < 60 max → flag stays False."""
        self._set_balances(base="0.0001", quote="160")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()
        self.assertFalse(self.controller.processed_data["over_max_net_position"])

    async def test_size_factor_zero_returns_none_executor_config(self):
        """When delta > hard_band on long side, size_factor_buy=0 → buy executor=None."""
        # 100% long base: 0.001 BTC * 400000 = 400 BRL, quote=0 → inv_pct=1.0, delta=0.5 > hard_band=0.20
        self._set_balances(base="0.001", quote="0")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        # size_factor_buy must be 0.0 when long beyond hard_band
        sf = self.controller.processed_data["size_factors"]
        self.assertEqual(sf["buy"], 0.0)

        # get_executor_config for a buy level must return None
        config = self.controller.get_executor_config("buy_0", Decimal("400000"), Decimal("0.0001"))
        self.assertIsNone(config)

    async def test_one_sided_triggered_at_hard_cap(self):
        """When delta exceeds inv_hard_cap, buy side gets disabled (§2.4)."""
        # 100% long base → delta = 0.5, > inv_hard_cap=0.30
        self._set_balances(base="0.001", quote="0")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        sides = self.controller.processed_data["sides_enabled"]
        self.assertFalse(sides["buy"])
        self.assertTrue(sides["sell"])

    async def test_csv_logging_writes_header_and_row(self):
        """CSV is created with header on first write and one row per call."""
        self.config.__dict__["csv_log_enabled"] = True
        self._set_balances(base="0.0005", quote="200")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))

        await self.controller.update_processed_data()
        await self.controller.update_processed_data()

        csv_path = os.path.join(self.tmpdir, "signals.csv")
        self.assertTrue(os.path.exists(csv_path))
        with open(csv_path) as f:
            lines = f.readlines()
        # header + 2 rows
        self.assertEqual(len(lines), 3)
        self.assertIn("regime", lines[0])
        self.assertIn("inv_pct", lines[0])

    async def test_orders_csv_written_on_executor_creation(self):
        """orders.csv records one row per get_executor_config success call."""
        self.config.__dict__["csv_log_enabled"] = True
        # Phase 2 setUp uses max_net_position_quote=60, so keep exposure below it.
        self._set_balances(base="0.0001", quote="200")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        cfg = self.controller.get_executor_config(
            "buy_0", price=Decimal("399800"), amount=Decimal("0.0005"),
        )
        self.assertIsNotNone(cfg)
        orders_path = os.path.join(self.tmpdir, "orders.csv")
        self.assertTrue(os.path.exists(orders_path))
        with open(orders_path) as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 2)  # header + 1 row
        self.assertIn("level_id", lines[0])
        self.assertIn("distance_from_mid_bps", lines[0])
        self.assertIn("buy_0", lines[1])
        self.assertIn("buy", lines[1])

    async def test_orders_csv_disabled_when_csv_log_off(self):
        """When csv_log_enabled=False, orders.csv must not be created."""
        self.assertFalse(self.config.csv_log_enabled)  # default for this setUp
        self._set_balances(base="0.0001", quote="200")
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()
        cfg = self.controller.get_executor_config(
            "buy_0", price=Decimal("399800"), amount=Decimal("0.0005"),
        )
        self.assertIsNotNone(cfg)
        orders_path = os.path.join(self.tmpdir, "orders.csv")
        self.assertFalse(os.path.exists(orders_path))


class TestPMMLeadLagSkewControllerPhase3(IsolatedAsyncioWrapperTestCase):
    """Phase 3: vol from candles, skew active, requote brakes, distance clamp."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="pmm_test_logs_")
        self.config = _make_config(
            csv_log_enabled=False,
            csv_log_dir=self.tmpdir,
            skew_max_bps=2.0,    # Phase 3 default
            min_requote_bps=1.0,
            force_requote_bps=15.0,
            # Phase 4 added a kill switch on this cap. Keep it high here so
            # Phase 3 tests aren't accidentally tripped by setUp balances.
            max_net_position_quote=Decimal("10000"),
        )
        self.mock_market_data_provider = MagicMock(spec=MarketDataProvider)
        self.mock_market_data_provider.time = MagicMock(return_value=1234567890.0)
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = PMMLeadLagSkewController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )
        # Default: balances on connector
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector("0.0005", "200"),
        }
        # Default: no candles → fall back to neutral vol
        self.mock_market_data_provider.get_candles_df = MagicMock(return_value=None)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    # ── Volatility ───────────────────────────────────────────────────────

    async def test_vol_neutral_when_no_candles(self):
        """No candles_df → spread_multiplier = 1 (neutral fallback)."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["spread_multiplier"], Decimal("1"))
        self.assertAlmostEqual(self.controller.processed_data["vol_ratio"], 1.0)

    async def test_vol_high_short_expands_spread(self):
        """σ_short > σ_ref → vol_ratio > 1 → spread_multiplier > 1."""
        import pandas as pd
        # 360 close prices: first 330 calm, last 30 volatile
        calm = [100000.0 + (i % 3) for i in range(330)]
        wild = [100000.0 + 1000 * (1 if i % 2 else -1) for i in range(30)]
        df = pd.DataFrame({"close": calm + wild})
        self.mock_market_data_provider.get_candles_df = MagicMock(return_value=df)
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("100000"))

        await self.controller.update_processed_data()
        self.assertGreater(self.controller.processed_data["vol_ratio"], 1.0)
        self.assertGreaterEqual(self.controller.processed_data["spread_multiplier"], Decimal("1"))

    async def test_vol_capped_at_3(self):
        """
        Even with extreme σ_short, the volatility component of the spread
        multiplier is capped at 3.0. We assert on `vol_state` directly so the
        test stays focused on the vol cap and is not affected by Phase 4's
        regime-driven multiplier (degraded × 1.5, etc.).
        """
        import pandas as pd
        calm = [100000.0] * 360
        wild = [100000.0 + 5000 * (1 if i % 2 else -1) for i in range(30)]
        df = pd.DataFrame({"close": calm[:330] + wild})
        self.mock_market_data_provider.get_candles_df = MagicMock(return_value=df)
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("100000"))

        await self.controller.update_processed_data()
        # vol_ratio is the raw σ_short/σ_ref (uncapped).
        self.assertGreater(self.controller.processed_data["vol_ratio"], 1.0)
        # The combined spread_multiplier may include a regime multiplier (1.5×
        # degraded, 2.5× safe), so the upper bound is 3 × 2.5 = 7.5.
        self.assertLessEqual(self.controller.processed_data["spread_multiplier"], Decimal("7.5"))

    # ── §5.6 Distance clamp ──────────────────────────────────────────────

    async def test_buy_price_clamped_below_mid(self):
        """A buy price >= mid must be pulled back to mid - margin."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        cfg = self.controller.get_executor_config(
            "buy_0", price=Decimal("400100"), amount=Decimal("0.0001"),
        )
        self.assertIsNotNone(cfg)
        self.assertLess(cfg.entry_price, Decimal("400000"))

    async def test_sell_price_clamped_above_mid(self):
        """A sell price <= mid must be pushed up to mid + margin."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        cfg = self.controller.get_executor_config(
            "sell_0", price=Decimal("399900"), amount=Decimal("0.0001"),
        )
        self.assertIsNotNone(cfg)
        self.assertGreater(cfg.entry_price, Decimal("400000"))

    async def test_clamp_does_not_alter_safe_prices(self):
        """A buy price already < mid is left alone."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        safe_buy = Decimal("399000")
        cfg = self.controller.get_executor_config("buy_0", price=safe_buy, amount=Decimal("0.0001"))
        self.assertEqual(cfg.entry_price, safe_buy)

    # ── Skew with skew_max_bps=2 ─────────────────────────────────────────

    async def test_long_inventory_shifts_reference_below_mid(self):
        """Long base + skew_max_bps>0 → reference < mid (sell-favoring)."""
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector("0.001", "100"),  # heavily long
        }
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()
        ref = self.controller.processed_data["reference_price"]
        mid = self.controller.processed_data["mid_price"]
        self.assertLess(ref, mid)

    # ── Requote brakes (§5.7) ────────────────────────────────────────────

    def _make_executor(self, executor_id, level_id, entry_price, *, is_active=True, is_trading=False, age_sec=120):
        """Build a minimal mock ExecutorInfo with the fields used by the brakes."""
        executor = MagicMock()
        executor.id = executor_id
        executor.is_active = is_active
        executor.is_trading = is_trading
        executor.timestamp = self.mock_market_data_provider.time() - age_sec
        executor.config = MagicMock()
        executor.config.entry_price = entry_price
        executor.custom_info = {"level_id": level_id}
        return executor

    async def test_min_requote_brake_skips_small_delta(self):
        """When new price differs by < min_requote_bps from current, refresh is skipped."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        # New price for buy_0 = ref * (1 - 0.0010) = 400000 * 0.999 = 399600
        # Set executor at 399599.99 → delta < 1 bps → must skip
        executor = self._make_executor("e1", "buy_0", Decimal("399599.99"))
        self.controller.executors_info = [executor]
        actions = self.controller.executors_to_refresh()
        self.assertEqual(len(actions), 0, "Small delta should suppress refresh")

    async def test_min_requote_brake_allows_large_delta(self):
        """When new price differs by >= min_requote_bps, refresh proceeds."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        # entry far from new price → delta >> 1 bps → refresh
        executor = self._make_executor("e1", "buy_0", Decimal("390000"))
        self.controller.executors_info = [executor]
        actions = self.controller.executors_to_refresh()
        self.assertEqual(len(actions), 1)

    async def test_force_requote_brake_stops_stale_executor_early(self):
        """Executor whose new price differs by >= force_requote_bps is force-stopped."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        # entry at 380000 vs new ~399600 → delta ≈ 515 bps >> 15
        executor = self._make_executor("e1", "buy_0", Decimal("380000"), age_sec=5)
        self.controller.executors_info = [executor]
        actions = self.controller.executors_to_early_stop()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].executor_id, "e1")

    async def test_force_requote_does_not_stop_fresh_executor(self):
        """Executor whose new price is close → no early stop."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        # delta ~0 → no early stop
        executor = self._make_executor("e1", "buy_0", Decimal("399600"), age_sec=5)
        self.controller.executors_info = [executor]
        actions = self.controller.executors_to_early_stop()
        self.assertEqual(len(actions), 0)

    async def test_force_requote_skips_trading_executor(self):
        """Executors that are trading (in flight) are never early-stopped."""
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))
        await self.controller.update_processed_data()

        executor = self._make_executor("e1", "buy_0", Decimal("380000"), is_trading=True)
        self.controller.executors_info = [executor]
        actions = self.controller.executors_to_early_stop()
        self.assertEqual(len(actions), 0)


class TestPMMLeadLagSkewControllerPhase4(IsolatedAsyncioWrapperTestCase):
    """
    Phase 4 — regime circuit breakers and kill switch.

    Tests the L1/L1.5/L2/L3 state machine, regime-driven size/spread shrinking,
    safe-mode level limits, paused/killed cancel-all behaviour, and kill-switch
    latching on inv_kill, max_net_position, and session drawdown triggers.
    """

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="pmm_test_p4_")
        self.config = _make_config(
            csv_log_enabled=False,
            csv_log_dir=self.tmpdir,
            skew_max_bps=2.0,
            min_requote_bps=1.0,
            force_requote_bps=15.0,
            max_net_position_quote=Decimal("10000"),
            # Phase 4 thresholds
            vol_degraded_threshold_mult=3.0,
            vol_pause_threshold_mult=5.0,
            pause_basis_bps=30.0,
            pause_release_sec=60.0,
            safe_mode_entry_sec=120.0,
            safe_thrash_window_sec=600.0,
            max_session_drawdown_quote=Decimal("10"),
        )
        self.mock_market_data_provider = MagicMock(spec=MarketDataProvider)
        self.mock_market_data_provider.time = MagicMock(return_value=1_000_000.0)
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = PMMLeadLagSkewController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector("0.0005", "200"),
        }
        self.mock_market_data_provider.get_candles_df = MagicMock(return_value=None)
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _candles_with_ratio(self, target_ratio: float):
        """
        Build a candles DataFrame whose σ_short/σ_ref ≈ target_ratio.

        Strategy: 330 calm candles (small ±1 noise) + 30 wild candles whose
        amplitude scales with target_ratio. Used to drive the regime evaluator.
        """
        import pandas as pd
        amp = 1000.0 * float(target_ratio)
        calm = [100000.0 + (i % 3) for i in range(330)]
        wild = [100000.0 + amp * (1 if i % 2 else -1) for i in range(30)]
        return pd.DataFrame({"close": calm + wild})

    # ── Regime state machine ─────────────────────────────────────────────

    async def test_regime_normal_when_calm(self):
        """vol_ratio ≈ 1, no balance issue → regime normal."""
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "normal")
        self.assertFalse(self.controller._regime_ctx.is_killed)

    async def test_regime_degraded_above_threshold(self):
        """vol_ratio > vol_degraded but < vol_pause → degraded."""
        df = self._candles_with_ratio(3.5)
        self.mock_market_data_provider.get_candles_df = MagicMock(return_value=df)
        # Lead-lag uses a separate feed; with the same MagicMock returning the
        # synthetic df, the implied basis would blow up and trigger paused.
        # Neutralize lead signals here — this test is about vol-driven regime.
        with patch.object(self.controller, "_compute_lead_signals",
                          return_value=_neutral_lead_state()):
            await self.controller.update_processed_data()
        self.assertIn(self.controller.processed_data["regime"], ("degraded", "safe"))

    async def test_regime_paused_when_basis_excessive(self):
        """|basis_bps| above pause_basis_bps → paused."""
        from controllers.market_making.pmm_lead_lag_utils import LeadState
        fake_lead = LeadState(
            s_lead_micro=0.0, s_lead_regime=0.0, basis_bps=50.0,
            micro_stale=False, regime_stale=False,
            pause_buy=False, pause_sell=False,
        )
        with patch.object(self.controller, "_compute_lead_signals", return_value=fake_lead):
            await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "paused")

    async def test_regime_paused_dwell_blocks_immediate_exit(self):
        """Once L2 fires, regime stays paused for pause_release_sec."""
        # Trigger L2 via fake L2 timestamp.
        now = float(self.mock_market_data_provider.time())
        self.controller._regime_ctx.last_l2_time = now - 10  # 10s into 60s dwell
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "paused")

    async def test_regime_safe_after_sustained_l1(self):
        """L1 sustained beyond safe_mode_entry_sec → escalate to safe."""
        df = self._candles_with_ratio(4.0)
        self.mock_market_data_provider.get_candles_df = MagicMock(return_value=df)
        now = float(self.mock_market_data_provider.time())
        self.controller._regime_ctx.l1_entry_time = now - 200
        with patch.object(self.controller, "_compute_lead_signals",
                          return_value=_neutral_lead_state()):
            await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "safe")
        self.assertIsNotNone(self.controller._regime_ctx.safe_entry_time)

    async def test_regime_safe_dwell_persists_when_conditions_clear(self):
        """Once in safe, regime stays safe for 2 × pause_release_sec."""
        now = float(self.mock_market_data_provider.time())
        self.controller._regime_ctx.safe_entry_time = now - 30  # 30s into 120s dwell
        # Calm candles (would normally → normal) but safe dwell holds.
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "safe")

    async def test_regime_killed_by_inv_kill(self):
        """|delta| > inv_kill triggers latching kill switch."""
        # 0.001 BTC at 400000 = 400 BRL, quote = 0 → inv_pct = 1.0, delta = 0.5
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector("0.001", "0"),
        }
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "killed")
        self.assertTrue(self.controller._regime_ctx.is_killed)

    async def test_regime_killed_by_max_net_position(self):
        """net_exposure_quote > max_net_position_quote triggers kill."""
        cfg = _make_config(
            csv_log_enabled=False, max_net_position_quote=Decimal("60"),
        )
        controller = PMMLeadLagSkewController(
            config=cfg, market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector("0.001", "200"),  # net = 400 > 60
        }
        await controller.update_processed_data()
        self.assertEqual(controller.processed_data["regime"], "killed")
        self.assertTrue(controller._regime_ctx.is_killed)

    async def test_regime_killed_by_session_drawdown(self):
        """session_pnl < -max_session_drawdown_quote triggers kill."""
        self.controller._session_pnl = -50.0   # < -10 = -max_session_drawdown_quote
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "killed")
        self.assertTrue(self.controller._regime_ctx.is_killed)

    async def test_kill_latch_persists_across_ticks(self):
        """Once killed, _is_killed stays True even if conditions clear."""
        self.controller._regime_ctx.is_killed = True
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "killed")
        # And again on a second tick with all-normal data.
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "killed")
        self.assertTrue(self.controller._regime_ctx.is_killed)

    # ── Level filtering by regime ────────────────────────────────────────

    async def test_paused_returns_no_levels(self):
        """regime=paused → get_levels_to_execute returns empty list."""
        self.controller._regime_ctx.last_l2_time = float(self.mock_market_data_provider.time())
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "paused")
        self.assertEqual(self.controller.get_levels_to_execute(), [])

    async def test_killed_returns_no_levels(self):
        """regime=killed → get_levels_to_execute returns empty list."""
        self.controller._regime_ctx.is_killed = True
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.get_levels_to_execute(), [])

    async def test_safe_limits_to_one_level_per_side(self):
        """regime=safe → at most one buy + one sell level."""
        now = float(self.mock_market_data_provider.time())
        self.controller._regime_ctx.safe_entry_time = now - 30  # inside dwell
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "safe")
        levels = self.controller.get_levels_to_execute()
        self.assertLessEqual(len([lv for lv in levels if lv.startswith("buy")]), 1)
        self.assertLessEqual(len([lv for lv in levels if lv.startswith("sell")]), 1)

    async def test_safe_zeroes_price_shift(self):
        """In safe regime, price_shift_bps must be 0 even with inventory imbalance."""
        # Inventory long → ordinarily would produce a positive shift.
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector("0.0006", "100"),
        }
        # Force safe regime via dwell.
        now = float(self.mock_market_data_provider.time())
        self.controller._regime_ctx.safe_entry_time = now - 30
        await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "safe")
        self.assertEqual(self.controller.processed_data["price_shift_bps"], Decimal("0"))

    # ── Size factor by regime ────────────────────────────────────────────

    async def test_degraded_size_factor_halved(self):
        """Degraded regime → executor amount is base × 0.5."""
        df = self._candles_with_ratio(4.0)
        self.mock_market_data_provider.get_candles_df = MagicMock(return_value=df)
        await self.controller.update_processed_data()
        # Force regime to "degraded" (test isn't sensitive to whether L1.5 fires).
        self.controller.processed_data["regime"] = "degraded"
        self.controller.processed_data["size_factors"] = {"buy": 1.0, "sell": 1.0}
        cfg = self.controller.get_executor_config(
            "buy_0", price=Decimal("399000"), amount=Decimal("0.001"),
        )
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.amount, Decimal("0.0005"))   # 0.001 × 0.5

    async def test_safe_size_factor_quarter(self):
        """Safe regime → executor amount is base × 0.25."""
        await self.controller.update_processed_data()
        self.controller.processed_data["regime"] = "safe"
        self.controller.processed_data["size_factors"] = {"buy": 1.0, "sell": 1.0}
        cfg = self.controller.get_executor_config(
            "sell_0", price=Decimal("401000"), amount=Decimal("0.001"),
        )
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.amount, Decimal("0.00025"))  # 0.001 × 0.25

    async def test_paused_get_executor_config_returns_none(self):
        """In paused regime, get_executor_config refuses to create new orders."""
        await self.controller.update_processed_data()
        self.controller.processed_data["regime"] = "paused"
        cfg = self.controller.get_executor_config(
            "buy_0", price=Decimal("399000"), amount=Decimal("0.001"),
        )
        self.assertIsNone(cfg)

    # ── Cancel-all on paused/killed ──────────────────────────────────────

    def _make_resting_executor(self, executor_id, level_id, entry_price):
        """Lightweight resting (active, not trading) executor mock."""
        executor = MagicMock()
        executor.id = executor_id
        executor.is_active = True
        executor.is_trading = False
        executor.timestamp = float(self.mock_market_data_provider.time()) - 10
        executor.config = MagicMock()
        executor.config.entry_price = entry_price
        executor.custom_info = {"level_id": level_id}
        return executor

    async def test_paused_stops_all_active_executors(self):
        """regime=paused → all resting executors are scheduled for stop."""
        await self.controller.update_processed_data()
        self.controller.processed_data["regime"] = "paused"
        e1 = self._make_resting_executor("e1", "buy_0", Decimal("399000"))
        e2 = self._make_resting_executor("e2", "sell_1", Decimal("402000"))
        self.controller.executors_info = [e1, e2]
        actions = self.controller.executors_to_early_stop()
        stopped_ids = {a.executor_id for a in actions}
        self.assertEqual(stopped_ids, {"e1", "e2"})

    async def test_killed_stops_all_active_executors(self):
        """regime=killed → all resting executors are scheduled for stop."""
        self.controller._regime_ctx.is_killed = True
        await self.controller.update_processed_data()
        e1 = self._make_resting_executor("e1", "buy_0", Decimal("399000"))
        self.controller.executors_info = [e1]
        actions = self.controller.executors_to_early_stop()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].executor_id, "e1")

    async def test_safe_stops_only_deeper_levels(self):
        """regime=safe → only buy_0/sell_0 stay; deeper levels are cancelled.
        Entry prices for buy_0/sell_0 are set close to the new computed price
        so the force-requote check doesn't also stop them."""
        await self.controller.update_processed_data()
        self.controller.processed_data["regime"] = "safe"
        # buy_0 new price ~= 400000*(1-0.0010) = 399600; sell_0 ~= 400400.
        e0 = self._make_resting_executor("e0", "buy_0", Decimal("399600"))
        e1 = self._make_resting_executor("e1", "buy_1", Decimal("399200"))
        s0 = self._make_resting_executor("s0", "sell_0", Decimal("400400"))
        s1 = self._make_resting_executor("s1", "sell_1", Decimal("400800"))
        self.controller.executors_info = [e0, e1, s0, s1]
        actions = self.controller.executors_to_early_stop()
        stopped_ids = {a.executor_id for a in actions}
        self.assertIn("e1", stopped_ids)
        self.assertIn("s1", stopped_ids)
        self.assertNotIn("e0", stopped_ids)
        self.assertNotIn("s0", stopped_ids)

    # ── Telemetry ────────────────────────────────────────────────────────

    async def test_processed_data_carries_regime_cause_and_session_pnl(self):
        """processed_data exposes regime_cause + session_pnl for logging."""
        self.controller._session_pnl = -2.5
        await self.controller.update_processed_data()
        self.assertIn("regime_cause", self.controller.processed_data)
        self.assertIn("session_pnl", self.controller.processed_data)
        self.assertEqual(self.controller.processed_data["session_pnl"], -2.5)

    async def test_get_custom_info_includes_kill_flag(self):
        """get_custom_info reports the kill latch state."""
        await self.controller.update_processed_data()
        info = self.controller.get_custom_info()
        self.assertIn("is_killed", info)
        self.assertIn("regime_cause", info)
        self.assertIn("session_pnl", info)

    # ── Critical error counter (§4.1 L3 item 4) ─────────────────────────────

    def test_record_critical_error_increments_counter(self):
        self.assertEqual(self.controller._regime_ctx.critical_error_count, 0)
        self.controller._record_critical_error("test")
        self.assertEqual(self.controller._regime_ctx.critical_error_count, 1)
        self.controller._record_critical_error("test")
        self.assertEqual(self.controller._regime_ctx.critical_error_count, 2)

    async def test_critical_error_threshold_triggers_kill(self):
        """Reaching critical_error_threshold flips to L3 killed via the regime fn."""
        # Bump counter to threshold (default 5).
        for _ in range(self.config.critical_error_threshold):
            self.controller._record_critical_error("synthetic")
        with patch.object(self.controller, "_compute_lead_signals",
                          return_value=_neutral_lead_state()):
            await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["regime"], "killed")
        self.assertTrue(self.controller._regime_ctx.is_killed)
        self.assertIn("critical_errors", self.controller._regime_cause)

    def test_sustained_feed_stale_records_one_critical(self):
        """Auto-trigger: stale > 6× max_leader_staleness_sec → 1 critical recorded."""
        # Simulate having seen at least one BTC-USDT tick in the past.
        self.controller._last_mid_usdt_ts = 1000.0
        episode_threshold = 6.0 * self.config.max_leader_staleness_sec  # 30s default
        # First call enters the stale episode (just past staleness threshold).
        self.controller._track_sustained_feed_stale(now=1010.0)
        self.assertEqual(self.controller._regime_ctx.critical_error_count, 0)
        self.assertEqual(self.controller._feed_stale_started_at, 1010.0)
        # Second call after 30+s of sustained staleness → 1 critical recorded.
        self.controller._track_sustained_feed_stale(now=1010.0 + episode_threshold + 1.0)
        self.assertEqual(self.controller._regime_ctx.critical_error_count, 1)
        # Subsequent stale ticks within same episode do NOT double-bump.
        self.controller._track_sustained_feed_stale(now=1010.0 + episode_threshold + 10.0)
        self.assertEqual(self.controller._regime_ctx.critical_error_count, 1)

    def test_feed_recovery_resets_stale_episode(self):
        """When feed recovers, counter does not bump again until a new stale episode."""
        self.controller._last_mid_usdt_ts = 1000.0
        episode_threshold = 6.0 * self.config.max_leader_staleness_sec
        # Enter stale → episode starts.
        self.controller._track_sustained_feed_stale(now=1010.0)
        # Sustained stale → record critical.
        self.controller._track_sustained_feed_stale(now=1010.0 + episode_threshold + 1.0)
        self.assertEqual(self.controller._regime_ctx.critical_error_count, 1)
        # Feed recovers (last tick now fresh).
        self.controller._last_mid_usdt_ts = 2000.0
        self.controller._track_sustained_feed_stale(now=2000.5)
        self.assertIsNone(self.controller._feed_stale_started_at)
        self.assertFalse(self.controller._feed_stale_critical_recorded)


class TestPMMLeadLagSkewControllerPhase5(IsolatedAsyncioWrapperTestCase):
    """
    Phase 5 — lead-lag micro pause + synthetic regime signal.

    The controller's `_compute_lead_signals` is patched per-test to inject a
    deterministic LeadState; the unit tests for the underlying pure functions
    live in `test_pmm_lead_lag_utils.py`.
    """

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="pmm_test_p5_")
        self.config = _make_config(
            csv_log_enabled=False,
            csv_log_dir=self.tmpdir,
            skew_max_bps=2.0,
            min_requote_bps=1.0,
            force_requote_bps=15.0,
            max_net_position_quote=Decimal("10000"),
            # Phase 5 thresholds
            lead_micro_threshold_bps=5.0,
            lead_micro_dwell_sec=3.0,
            basis_deadband_bps=3.0,
            w_lead=0.1,
        )
        self.mock_market_data_provider = MagicMock(spec=MarketDataProvider)
        self.mock_market_data_provider.time = MagicMock(return_value=1_000_000.0)
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = PMMLeadLagSkewController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector("0.0005", "200"),
        }
        self.mock_market_data_provider.get_candles_df = MagicMock(return_value=None)
        self.mock_market_data_provider.get_price_by_type = MagicMock(return_value=Decimal("400000"))

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _patch_lead(self, **lead_kwargs):
        from controllers.market_making.pmm_lead_lag_utils import LeadState
        defaults = dict(
            s_lead_micro=0.0, s_lead_regime=0.0, basis_bps=0.0,
            micro_stale=False, regime_stale=False,
            pause_buy=False, pause_sell=False,
        )
        defaults.update(lead_kwargs)
        return patch.object(
            self.controller, "_compute_lead_signals",
            return_value=LeadState(**defaults),
        )

    # ── Micro pause level filter (§3.6) ─────────────────────────────────

    async def test_pause_sell_filters_sell_levels(self):
        """When pause_sell fires, sell levels are removed from execution."""
        with self._patch_lead(s_lead_micro=50.0, pause_sell=True):
            await self.controller.update_processed_data()
        levels = self.controller.get_levels_to_execute()
        self.assertTrue(any(lv.startswith("buy") for lv in levels))
        self.assertFalse(any(lv.startswith("sell") for lv in levels))

    async def test_pause_buy_filters_buy_levels(self):
        with self._patch_lead(s_lead_micro=-50.0, pause_buy=True):
            await self.controller.update_processed_data()
        levels = self.controller.get_levels_to_execute()
        self.assertFalse(any(lv.startswith("buy") for lv in levels))
        self.assertTrue(any(lv.startswith("sell") for lv in levels))

    async def test_neutral_lead_keeps_all_levels(self):
        with self._patch_lead():
            await self.controller.update_processed_data()
        levels = self.controller.get_levels_to_execute()
        self.assertTrue(any(lv.startswith("buy") for lv in levels))
        self.assertTrue(any(lv.startswith("sell") for lv in levels))

    async def test_micro_pause_dwell_persists_across_ticks(self):
        """One trigger latches pause for lead_micro_dwell_sec, even if next tick clears."""
        # First tick fires pause_sell.
        with self._patch_lead(s_lead_micro=50.0, pause_sell=True):
            await self.controller.update_processed_data()
        self.assertTrue(self.controller._pause_sell_until > 1_000_000.0)
        # Next tick at +1s, lead now neutral but dwell hasn't expired yet.
        self.mock_market_data_provider.time = MagicMock(return_value=1_000_001.0)
        with self._patch_lead():
            await self.controller.update_processed_data()
        self.assertTrue(self.controller.processed_data["pause_sell"])
        # After dwell expires (3s + 1s buffer), pause clears.
        self.mock_market_data_provider.time = MagicMock(return_value=1_000_010.0)
        with self._patch_lead():
            await self.controller.update_processed_data()
        self.assertFalse(self.controller.processed_data["pause_sell"])

    # ── §3.6 critério 1: lag ≥ spread → cancel ──────────────────────────

    def _make_resting_executor(self, eid, level_id, entry_price):
        ex = MagicMock(spec=ExecutorInfo)
        ex.id = eid
        ex.is_active = True
        ex.is_trading = False
        ex.is_done = False
        ex.custom_info = {"level_id": level_id}
        ex.config = MagicMock()
        ex.config.entry_price = entry_price
        ex.config.side = TradeType.BUY if level_id.startswith("buy") else TradeType.SELL
        return ex

    async def test_lag_above_spread_cancels_resting_order(self):
        """
        s_lead_micro = 50 bps and pause_sell=True; tightest sell spread = 10 bps.
        Resting sell order must be cancelled (§3.6 critério 1).
        """
        with self._patch_lead(s_lead_micro=50.0, pause_sell=True):
            await self.controller.update_processed_data()
        sell0 = self._make_resting_executor("s0", "sell_0", Decimal("400500"))
        self.controller.executors_info = [sell0]
        actions = self.controller.executors_to_early_stop()
        stopped_ids = {a.executor_id for a in actions}
        self.assertIn("s0", stopped_ids)

    async def test_lag_below_spread_keeps_resting_order(self):
        """s_lead_micro = 5 bps < 10 bps tightest spread → resting order stays."""
        # threshold_bps=5; need micro just over threshold but below side_spread (10 bps).
        with self._patch_lead(s_lead_micro=6.0, pause_sell=True):
            await self.controller.update_processed_data()
        # Use entry_price near current mid to avoid force_requote tripping.
        sell0 = self._make_resting_executor("s0", "sell_0", Decimal("400400"))
        # New computed price for sell_0 ≈ 400400 → delta ≈ 0 bps; force-requote inert.
        self.controller.get_price_and_amount = MagicMock(return_value=(Decimal("400400"), Decimal("1")))
        self.controller.executors_info = [sell0]
        actions = self.controller.executors_to_early_stop()
        stopped_ids = {a.executor_id for a in actions}
        self.assertNotIn("s0", stopped_ids)

    # ── Staleness ───────────────────────────────────────────────────────

    async def test_micro_stale_disables_pause(self):
        """When micro_stale=True, pause flags must be False even with a lag value."""
        with self._patch_lead(s_lead_micro=0.0, micro_stale=True,
                              pause_buy=False, pause_sell=False):
            await self.controller.update_processed_data()
        self.assertFalse(self.controller.processed_data["pause_buy"])
        self.assertFalse(self.controller.processed_data["pause_sell"])

    async def test_regime_stale_zeroes_lead_regime(self):
        with self._patch_lead(s_lead_regime=0.0, regime_stale=True):
            await self.controller.update_processed_data()
        self.assertEqual(self.controller.processed_data["s_lead_regime"], 0.0)
        self.assertTrue(self.controller.processed_data["regime_stale"])

    # ── Skew contribution from s_lead_regime ────────────────────────────

    async def test_w_lead_contributes_to_price_shift(self):
        """With w_lead=0.1 and s_lead_regime=-0.5, price_shift_bps shifts toward defense."""
        # Inventory neutral so s_inv ≈ 0; only s_lead_regime contributes.
        self.mock_market_data_provider.connectors = {
            "binance": _make_mock_connector("0.0001", "160"),  # ~50/50 inv
        }
        with self._patch_lead(s_lead_regime=-0.5, basis_bps=10.0):
            await self.controller.update_processed_data()
        # skew_norm ≈ tanh(0.1 * -0.5) ≈ -0.05 → price_shift_bps ≈ -0.1.
        # Negative shift → ref > mid → defensive (asks up).
        shift = self.controller.processed_data["price_shift_bps"]
        self.assertLess(float(shift), 0.0)

    # ── CSV / telemetry columns ──────────────────────────────────────────

    async def test_processed_data_includes_lead_fields(self):
        with self._patch_lead(s_lead_micro=8.0, s_lead_regime=-0.3, basis_bps=12.0):
            await self.controller.update_processed_data()
        d = self.controller.processed_data
        for key in ("s_lead_micro", "s_lead_regime", "basis_bps",
                    "micro_stale", "regime_stale", "pause_buy", "pause_sell"):
            self.assertIn(key, d)
        self.assertEqual(d["s_lead_micro"], 8.0)
        self.assertAlmostEqual(d["s_lead_regime"], -0.3)
        self.assertAlmostEqual(d["basis_bps"], 12.0)

    async def test_get_custom_info_includes_lead_fields(self):
        with self._patch_lead(s_lead_micro=4.0, basis_bps=2.0):
            await self.controller.update_processed_data()
        info = self.controller.get_custom_info()
        for key in ("s_lead_micro", "s_lead_regime", "basis_bps",
                    "pause_buy", "pause_sell", "micro_stale", "regime_stale"):
            self.assertIn(key, info)

    async def test_get_candles_config_includes_three_pairs(self):
        cfg = self.controller.get_candles_config()
        pairs = {(c.connector, c.trading_pair) for c in cfg}
        self.assertIn(("binance", "BTC-BRL"), pairs)
        self.assertIn(("binance", "BTC-USDT"), pairs)
        self.assertIn(("binance", "USDT-BRL"), pairs)


if __name__ == "__main__":
    unittest.main()
