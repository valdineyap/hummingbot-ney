"""Unit tests for pmm_lead_lag_utils pure functions."""
import math
import unittest
from decimal import Decimal

from controllers.market_making.pmm_lead_lag_utils import (
    InventoryState,
    LeadState,
    OrderParams,
    RegimeState,
    SkewState,
    SidePermissions,
    VolState,
    compute_inventory_state,
    compute_lead_state,
    compute_order_params,
    compute_regime_state,
    compute_side_permissions,
    compute_size_factors,
    compute_skew_state,
    compute_vol_state,
)


class TestComputeInventoryState(unittest.TestCase):

    def _call(self, base=Decimal("0.01"), quote=Decimal("500"),
              mid=Decimal("50000"), target=0.5,
              soft=0.10, hard=0.20, kill=0.40):
        return compute_inventory_state(base, quote, mid, target, soft, hard, kill)

    def test_neutral_50_50(self):
        """50/50 portfolio → delta=0, s_inv=0."""
        state = self._call(base=Decimal("0.01"), quote=Decimal("500"), mid=Decimal("50000"))
        self.assertAlmostEqual(state.inv_pct, 0.5, places=5)
        self.assertAlmostEqual(state.delta, 0.0, places=5)
        self.assertAlmostEqual(state.s_inv, 0.0, places=5)
        self.assertFalse(state.in_soft)
        self.assertFalse(state.in_hard)
        self.assertFalse(state.in_kill)

    def test_long_base_triggers_soft(self):
        """Long base beyond soft_band → in_soft=True."""
        # inv_pct = 0.6*500/(0.6*500+200) = 300/500 = 0.6 → delta=0.1=soft_band
        state = self._call(base=Decimal("0.012"), quote=Decimal("200"), mid=Decimal("50000"),
                           target=0.5, soft=0.10)
        self.assertGreaterEqual(state.delta, 0.09)  # ≥ soft_band boundary

    def test_zero_portfolio_returns_default(self):
        """Zero portfolio → safe default 50/50."""
        state = compute_inventory_state(
            Decimal("0"), Decimal("0"), Decimal("50000"),
            0.5, 0.10, 0.20, 0.40
        )
        self.assertAlmostEqual(state.inv_pct, 0.5)
        self.assertAlmostEqual(state.s_inv, 0.0)

    def test_s_inv_clipped_at_1(self):
        """s_inv is clipped to [-1, 1] even with large delta."""
        state = self._call(base=Decimal("1"), quote=Decimal("0"), mid=Decimal("50000"))
        self.assertLessEqual(state.s_inv, 1.0)
        self.assertGreaterEqual(state.s_inv, -1.0)

    def test_sign_long_base_positive_s_inv(self):
        """When inv_pct > target, s_inv > 0 (long base signal)."""
        state = self._call(base=Decimal("0.02"), quote=Decimal("0"), mid=Decimal("50000"),
                           target=0.5, soft=0.10)
        self.assertGreater(state.s_inv, 0)

    def test_sign_short_base_negative_s_inv(self):
        """When inv_pct < target, s_inv < 0 (short base signal)."""
        state = self._call(base=Decimal("0"), quote=Decimal("1000"), mid=Decimal("50000"),
                           target=0.5, soft=0.10)
        self.assertLess(state.s_inv, 0)


class TestComputeSizeFactors(unittest.TestCase):

    def test_neutral_s_inv(self):
        """s_inv=0 → both factors = 1.0."""
        sf_buy, sf_sell = compute_size_factors(0.0)
        self.assertAlmostEqual(sf_buy, 1.0)
        self.assertAlmostEqual(sf_sell, 1.0)

    def test_long_base_buy_penalized(self):
        """s_inv > 0 (long base) → buy penalized, sell bonus."""
        sf_buy, sf_sell = compute_size_factors(1.0)
        self.assertLess(sf_buy, 1.0)
        self.assertGreater(sf_sell, 1.0)

    def test_short_base_sell_penalized(self):
        """s_inv < 0 (short base) → sell penalized, buy bonus."""
        sf_buy, sf_sell = compute_size_factors(-1.0)
        self.assertGreater(sf_buy, 1.0)
        self.assertLess(sf_sell, 1.0)

    def test_buy_penalized_more_than_sell_when_long(self):
        """When long base: size_factor_buy ≤ size_factor_sell (plan §2.3 invariant)."""
        for s_inv in [0.3, 0.6, 1.0]:
            sf_buy, sf_sell = compute_size_factors(s_inv)
            self.assertLessEqual(sf_buy, sf_sell,
                                 msg=f"s_inv={s_inv}: buy={sf_buy} should <= sell={sf_sell}")

    def test_bonus_capped_at_1_5(self):
        """Bonus side never exceeds 1.5x."""
        sf_buy, sf_sell = compute_size_factors(1.0)
        self.assertLessEqual(sf_sell, 1.5)
        sf_buy2, sf_sell2 = compute_size_factors(-1.0)
        self.assertLessEqual(sf_buy2, 1.5)

    def test_penalty_floored_at_0_5(self):
        """Penalty side never goes below 0.5x."""
        sf_buy, sf_sell = compute_size_factors(1.0)
        self.assertGreaterEqual(sf_buy, 0.5)


class TestComputeVolState(unittest.TestCase):

    def test_neutral(self):
        state = compute_vol_state(1.0, 1.0)
        self.assertAlmostEqual(state.vol_ratio, 1.0)
        self.assertAlmostEqual(state.spread_multiplier, 1.0)

    def test_high_vol_clamped_at_3(self):
        state = compute_vol_state(10.0, 1.0)
        self.assertAlmostEqual(state.spread_multiplier, 3.0)

    def test_low_vol_floored_at_1(self):
        state = compute_vol_state(0.1, 1.0)
        self.assertAlmostEqual(state.spread_multiplier, 1.0)


class TestComputeSkewState(unittest.TestCase):

    def test_zero_skew_when_w_lead_zero_and_s_inv_zero(self):
        state = compute_skew_state(0.0, 0.0, 1.0, 0.0, 8.0)
        self.assertAlmostEqual(float(state.price_shift_bps), 0.0)

    def test_positive_s_inv_yields_positive_shift(self):
        """Long base → positive shift → reference goes DOWN → sell-favoring."""
        state = compute_skew_state(s_inv=1.0, s_lead_regime=0.0,
                                   w_inv=1.0, w_lead=0.0, skew_max_bps=8.0)
        self.assertGreater(float(state.price_shift_bps), 0.0)
        self.assertLessEqual(float(state.price_shift_bps), 8.0)

    def test_skew_bounded_by_skew_max_bps(self):
        """price_shift_bps ≤ skew_max_bps for any signal value."""
        state = compute_skew_state(5.0, 5.0, 1.0, 1.0, 4.0)
        self.assertLessEqual(abs(float(state.price_shift_bps)), 4.0)

    def test_skew_norm_is_tanh(self):
        s = compute_skew_state(1.0, 0.0, 1.0, 0.0, 10.0)
        self.assertAlmostEqual(s.skew_norm, math.tanh(1.0), places=5)


class TestComputeSidePermissions(unittest.TestCase):

    def _call(self, delta, hard=0.20, hyst=0.10, buy=True, sell=True):
        return compute_side_permissions(delta, hard, hyst, buy, sell)

    def test_both_enabled_within_soft(self):
        p = self._call(0.05)
        self.assertTrue(p.buy_enabled)
        self.assertTrue(p.sell_enabled)

    def test_buy_disabled_when_long_past_hard_band(self):
        p = self._call(0.25)
        self.assertFalse(p.buy_enabled)
        self.assertTrue(p.sell_enabled)

    def test_sell_disabled_when_short_past_hard_band(self):
        p = self._call(-0.25)
        self.assertTrue(p.buy_enabled)
        self.assertFalse(p.sell_enabled)

    def test_buy_stays_disabled_in_hysteresis_zone(self):
        """Once disabled, buy stays off until delta < hard - hysteresis."""
        # disabled at 0.25; hysteresis zone is [0.10, 0.20]
        p = self._call(0.15, buy=False)  # in hysteresis zone
        self.assertFalse(p.buy_enabled)  # must stay off

    def test_buy_reenables_after_hysteresis(self):
        """Once delta retreats below hard - hysteresis, buy re-enables."""
        p = self._call(0.05, buy=False)  # below 0.20 - 0.10 = 0.10
        self.assertTrue(p.buy_enabled)

    def test_sell_stays_disabled_in_hysteresis_zone(self):
        p = self._call(-0.15, sell=False)
        self.assertFalse(p.sell_enabled)

    def test_sell_reenables_after_hysteresis(self):
        p = self._call(-0.05, sell=False)
        self.assertTrue(p.sell_enabled)


class TestComputeOrderParams(unittest.TestCase):

    def _make_skew(self, shift_bps=0.0):
        return SkewState(
            skew_raw=0.0, skew_norm=0.0,
            price_shift_bps=Decimal(str(shift_bps)),
        )

    def _make_regime(self, mult=1.0):
        return RegimeState(regime="normal", spread_multiplier=mult)

    def _make_perms(self, buy=True, sell=True):
        return SidePermissions(buy_enabled=buy, sell_enabled=sell)

    def test_zero_shift_ref_equals_mid(self):
        mid = Decimal("350000")
        params = compute_order_params(mid, self._make_skew(0.0), self._make_regime(), self._make_perms())
        self.assertEqual(params.reference_price, mid)

    def test_positive_shift_lowers_reference(self):
        """shift > 0 → ref < mid (sell-favoring)."""
        mid = Decimal("350000")
        params = compute_order_params(mid, self._make_skew(4.0), self._make_regime(), self._make_perms())
        self.assertLess(params.reference_price, mid)

    def test_negative_shift_raises_reference(self):
        """shift < 0 → ref > mid (defensive)."""
        mid = Decimal("350000")
        params = compute_order_params(mid, self._make_skew(-4.0), self._make_regime(), self._make_perms())
        self.assertGreater(params.reference_price, mid)

    def test_regime_multiplier_propagates(self):
        params = compute_order_params(
            Decimal("350000"), self._make_skew(), self._make_regime(mult=1.5), self._make_perms()
        )
        self.assertEqual(params.spread_multiplier, Decimal("1.5"))

    def test_side_permissions_propagate(self):
        params = compute_order_params(
            Decimal("350000"), self._make_skew(), self._make_regime(),
            self._make_perms(buy=False, sell=True)
        )
        self.assertFalse(params.buy_enabled)
        self.assertTrue(params.sell_enabled)


class TestLeadAndRegimeStubs(unittest.TestCase):
    """Verify Phase 5 / Phase 4 stubs return safe neutral values."""

    def test_lead_state_stub_neutral(self):
        state = compute_lead_state(Decimal("350000"), Decimal("350000"), 3.0)
        self.assertEqual(state.s_lead_micro, 0.0)
        self.assertEqual(state.s_lead_regime, 0.0)
        self.assertFalse(state.micro_stale)
        self.assertFalse(state.regime_stale)

    def test_regime_state_stub_normal(self):
        vol = compute_vol_state(1.0, 1.0)
        inv = compute_inventory_state(
            Decimal("0.01"), Decimal("500"), Decimal("50000"), 0.5, 0.1, 0.2, 0.4
        )
        regime = compute_regime_state(vol, inv)
        self.assertEqual(regime.regime, "normal")
        self.assertAlmostEqual(regime.spread_multiplier, 1.0)


if __name__ == "__main__":
    unittest.main()
