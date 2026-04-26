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
    compute_volatility_from_prices,
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
    """Plan §2.3: zone-based size factors with explicit unit-test invariant."""

    SOFT = 0.10
    HARD = 0.20

    def _f(self, delta):
        return compute_size_factors(delta, self.SOFT, self.HARD)

    def test_neutral_zone_within_soft(self):
        """|Δ| ≤ soft → both factors = 1.0."""
        for delta in [0.0, 0.05, -0.05, 0.10, -0.10]:
            sf_buy, sf_sell = self._f(delta)
            self.assertAlmostEqual(sf_buy, 1.0, msg=f"delta={delta}")
            self.assertAlmostEqual(sf_sell, 1.0, msg=f"delta={delta}")

    def test_linear_zone_long_base(self):
        """soft < Δ ≤ hard, long base → buy decays, sell grows."""
        sf_buy, sf_sell = self._f(0.15)  # midway: t = 0.5
        self.assertAlmostEqual(sf_buy, 1.0 - 0.8 * 0.5, places=5)   # 0.6
        self.assertAlmostEqual(sf_sell, 1.0 + 0.5 * 0.5, places=5)  # 1.25

    def test_beyond_hard_band_long_base_buy_off(self):
        """|Δ| > hard, long base → buy = 0.0 (off), sell = 1.5."""
        sf_buy, sf_sell = self._f(0.30)
        self.assertEqual(sf_buy, 0.0)
        self.assertEqual(sf_sell, 1.5)

    def test_beyond_hard_band_short_base_sell_off(self):
        """|Δ| > hard, short base → sell = 0.0, buy = 1.5."""
        sf_buy, sf_sell = self._f(-0.30)
        self.assertEqual(sf_buy, 1.5)
        self.assertEqual(sf_sell, 0.0)

    def test_invariant_buy_le_sell_when_long(self):
        """§2.3 unit-test invariant: Δ > 0 → size_factor_buy ≤ size_factor_sell."""
        for delta in [-0.05, 0.0, 0.05, 0.15, 0.25, 0.35]:
            sf_buy, sf_sell = self._f(delta)
            if delta > 0:
                self.assertLessEqual(sf_buy, sf_sell,
                                     msg=f"delta={delta}: buy={sf_buy} should <= sell={sf_sell}")

    def test_branch_safety_no_t_undefined(self):
        """Beyond hard, branch must not reference `t` from previous scope."""
        # If the implementation incorrectly used `t` outside the linear zone, this would crash.
        sf_buy, sf_sell = self._f(0.50)
        self.assertEqual((sf_buy, sf_sell), (0.0, 1.5))

    def test_bonus_capped_at_1_5(self):
        sf_buy, sf_sell = self._f(0.50)
        self.assertLessEqual(sf_sell, 1.5)
        sf_buy2, sf_sell2 = self._f(-0.50)
        self.assertLessEqual(sf_buy2, 1.5)


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
    """Plan §2.4: trigger at hard_cap=0.30, release at hard_band=0.20."""

    HARD_CAP = 0.30
    HARD_BAND = 0.20

    def _call(self, delta, buy=True, sell=True):
        return compute_side_permissions(
            delta, self.HARD_CAP, self.HARD_BAND, buy, sell,
        )

    def test_both_enabled_within_band(self):
        p = self._call(0.05)
        self.assertTrue(p.buy_enabled)
        self.assertTrue(p.sell_enabled)

    def test_buy_disabled_when_long_past_hard_cap(self):
        """delta > hard_cap (0.30) → buy disabled."""
        p = self._call(0.35)
        self.assertFalse(p.buy_enabled)
        self.assertTrue(p.sell_enabled)

    def test_buy_NOT_disabled_at_hard_band(self):
        """delta = hard_band (0.20) is NOT enough to disable — only hard_cap."""
        p = self._call(0.20)
        self.assertTrue(p.buy_enabled)

    def test_sell_disabled_when_short_past_hard_cap(self):
        p = self._call(-0.35)
        self.assertTrue(p.buy_enabled)
        self.assertFalse(p.sell_enabled)

    def test_buy_stays_disabled_in_hysteresis_zone(self):
        """Once disabled at hard_cap, buy stays off in [hard_band, hard_cap]."""
        p = self._call(0.25, buy=False)  # inside hysteresis zone [0.20, 0.30]
        self.assertFalse(p.buy_enabled)

    def test_buy_reenables_below_hard_band(self):
        """Once delta retreats below hard_band, buy re-enables."""
        p = self._call(0.15, buy=False)  # below 0.20
        self.assertTrue(p.buy_enabled)

    def test_sell_stays_disabled_in_hysteresis_zone(self):
        p = self._call(-0.25, sell=False)
        self.assertFalse(p.sell_enabled)

    def test_sell_reenables_above_negative_hard_band(self):
        p = self._call(-0.15, sell=False)
        self.assertTrue(p.sell_enabled)

    def test_no_flipflop_in_hysteresis_zone(self):
        """In [hard_band, hard_cap], current state is preserved (no oscillation)."""
        # currently enabled, delta crosses hard_band but not hard_cap
        p = self._call(0.25, buy=True)
        self.assertTrue(p.buy_enabled)  # stays on
        # currently disabled, same delta
        p = self._call(0.25, buy=False)
        self.assertFalse(p.buy_enabled)  # stays off


class TestComputeOrderParams(unittest.TestCase):

    def _make_skew(self, shift_bps=0.0):
        return SkewState(
            skew_raw=0.0, skew_norm=0.0,
            price_shift_bps=Decimal(str(shift_bps)),
        )

    def _make_regime(self, mult=1.0):
        return RegimeState(regime="normal", spread_multiplier=mult)

    def _make_vol(self, mult=1.0):
        return VolState(vol_ratio=mult, spread_multiplier=mult)

    def _make_perms(self, buy=True, sell=True):
        return SidePermissions(buy_enabled=buy, sell_enabled=sell)

    def _call(self, mid, shift=0.0, regime_mult=1.0, vol_mult=1.0, buy=True, sell=True):
        return compute_order_params(
            mid_price=mid,
            skew_state=self._make_skew(shift),
            regime_state=self._make_regime(regime_mult),
            vol_state=self._make_vol(vol_mult),
            side_perms=self._make_perms(buy, sell),
        )

    def test_zero_shift_ref_equals_mid(self):
        mid = Decimal("350000")
        params = self._call(mid, shift=0.0)
        self.assertEqual(params.reference_price, mid)

    def test_positive_shift_lowers_reference(self):
        """shift > 0 → ref < mid (sell-favoring)."""
        mid = Decimal("350000")
        params = self._call(mid, shift=4.0)
        self.assertLess(params.reference_price, mid)

    def test_negative_shift_raises_reference(self):
        """shift < 0 → ref > mid (defensive)."""
        mid = Decimal("350000")
        params = self._call(mid, shift=-4.0)
        self.assertGreater(params.reference_price, mid)

    def test_combined_spread_multiplier_is_vol_times_regime(self):
        """§5.3 — final spread_multiplier = vol_mult * regime_mult."""
        params = self._call(Decimal("350000"), regime_mult=1.5, vol_mult=2.0)
        self.assertEqual(params.spread_multiplier, Decimal("2.0") * Decimal("1.5"))

    def test_side_permissions_propagate(self):
        params = self._call(Decimal("350000"), buy=False, sell=True)
        self.assertFalse(params.buy_enabled)
        self.assertTrue(params.sell_enabled)


class TestComputeVolatilityFromPrices(unittest.TestCase):
    """Phase 3 — σ of log returns from price arrays."""

    def test_constant_prices_zero_volatility(self):
        self.assertEqual(compute_volatility_from_prices([100.0] * 30), 0.0)

    def test_empty_returns_zero(self):
        self.assertEqual(compute_volatility_from_prices([]), 0.0)
        self.assertEqual(compute_volatility_from_prices(None), 0.0)
        self.assertEqual(compute_volatility_from_prices([100.0]), 0.0)

    def test_non_positive_prices_returns_zero(self):
        self.assertEqual(compute_volatility_from_prices([100, 0, 100]), 0.0)
        self.assertEqual(compute_volatility_from_prices([100, -1, 100]), 0.0)

    def test_increasing_volatility(self):
        """Larger swings → larger σ."""
        small = [100.0 + i * 0.01 for i in range(30)]
        large = [100.0 + i * 1.0 for i in range(30)]
        sigma_small = compute_volatility_from_prices(small)
        sigma_large = compute_volatility_from_prices(large)
        self.assertLess(sigma_small, sigma_large)

    def test_handles_non_numeric_gracefully(self):
        self.assertEqual(compute_volatility_from_prices(["foo", "bar"]), 0.0)


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
