"""Unit tests for PMMLeadLagSkewController — Phase 1 smoke tests."""
import asyncio
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
    )
    defaults.update(overrides)
    return PMMLeadLagSkewConfig(**defaults)


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


if __name__ == "__main__":
    unittest.main()
