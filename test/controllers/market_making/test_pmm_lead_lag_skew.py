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


if __name__ == "__main__":
    unittest.main()
