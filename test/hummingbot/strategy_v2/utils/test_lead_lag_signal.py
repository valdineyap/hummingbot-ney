"""Unit tests for the lead-lag signal provider module."""
import unittest
from decimal import Decimal

from hummingbot.strategy_v2.utils.lead_lag_signal import (
    CircularPriceBuffer,
    EMAFilter,
    FeedHealth,
    LeadLagSignalProvider,
    SignalQuality,
)


class TestCircularPriceBuffer(unittest.TestCase):
    def test_eviction_removes_old_entries(self):
        buf = CircularPriceBuffer(max_duration_sec=10.0)
        buf.push(0.0, Decimal("100"))
        buf.push(5.0, Decimal("105"))
        buf.push(20.0, Decimal("120"))
        # After pushing t=20, anything older than t=10 (strict <) is evicted.
        # t=0 and t=5 are both < 10 → evicted. Only t=20 remains.
        self.assertEqual(len(buf), 1)
        self.assertEqual(buf.latest_price(), Decimal("120"))
        self.assertEqual(buf.latest_timestamp(), 20.0)

    def test_get_price_at_or_before_exact_timestamp(self):
        buf = CircularPriceBuffer(max_duration_sec=60.0)
        buf.push(10.0, Decimal("100"))
        buf.push(20.0, Decimal("200"))
        self.assertEqual(buf.get_price_at_or_before(10.0), Decimal("100"))
        self.assertEqual(buf.get_price_at_or_before(20.0), Decimal("200"))

    def test_get_price_at_or_before_between_timestamps(self):
        buf = CircularPriceBuffer(max_duration_sec=60.0)
        buf.push(10.0, Decimal("100"))
        buf.push(20.0, Decimal("200"))
        # Most recent entry at or before t=15 is t=10.
        self.assertEqual(buf.get_price_at_or_before(15.0), Decimal("100"))

    def test_get_price_at_or_before_empty_buffer(self):
        buf = CircularPriceBuffer(max_duration_sec=60.0)
        self.assertIsNone(buf.get_price_at_or_before(100.0))

    def test_get_price_at_or_before_all_entries_after_t(self):
        buf = CircularPriceBuffer(max_duration_sec=60.0)
        buf.push(20.0, Decimal("200"))
        # No entry at or before t=10.
        self.assertIsNone(buf.get_price_at_or_before(10.0))

    def test_latest_price_empty_buffer(self):
        buf = CircularPriceBuffer(max_duration_sec=60.0)
        self.assertIsNone(buf.latest_price())
        self.assertIsNone(buf.latest_timestamp())

    def test_latest_price_returns_most_recent(self):
        buf = CircularPriceBuffer(max_duration_sec=60.0)
        buf.push(5.0, Decimal("100"))
        buf.push(10.0, Decimal("200"))
        self.assertEqual(buf.latest_price(), Decimal("200"))
        self.assertEqual(buf.latest_timestamp(), 10.0)

    def test_eviction_exactly_at_boundary(self):
        # max_duration=10, push at t=0 and t=10. Item at t=0 has age exactly
        # 10s. Eviction predicate is strict `<`, so age == 10 is NOT evicted.
        buf = CircularPriceBuffer(max_duration_sec=10.0)
        buf.push(0.0, Decimal("100"))
        buf.push(10.0, Decimal("110"))
        self.assertEqual(len(buf), 2)

    def test_buffer_pushes_decimal_not_float(self):
        buf = CircularPriceBuffer(max_duration_sec=60.0)
        buf.push(1.0, Decimal("100.50"))
        self.assertEqual(buf.latest_price(), Decimal("100.50"))
        self.assertIsInstance(buf.latest_price(), Decimal)


class TestEMAFilter(unittest.TestCase):
    def test_first_value_equals_input(self):
        ema = EMAFilter(Decimal("0.5"))
        result = ema.update(Decimal("100"))
        self.assertEqual(result, Decimal("100"))
        self.assertEqual(ema.value, Decimal("100"))

    def test_value_none_before_first_update(self):
        ema = EMAFilter(Decimal("0.3"))
        self.assertIsNone(ema.value)

    def test_second_value_blends(self):
        ema = EMAFilter(Decimal("0.5"))
        ema.update(Decimal("100"))
        result = ema.update(Decimal("200"))
        # 0.5 * 200 + 0.5 * 100 = 150
        self.assertEqual(result, Decimal("150"))
        self.assertEqual(ema.value, Decimal("150"))

    def test_alpha_one_equals_last_value(self):
        ema = EMAFilter(Decimal("1"))
        ema.update(Decimal("100"))
        ema.update(Decimal("200"))
        ema.update(Decimal("50"))
        self.assertEqual(ema.value, Decimal("50"))

    def test_alpha_zero_never_changes(self):
        ema = EMAFilter(Decimal("0"))
        ema.update(Decimal("100"))
        ema.update(Decimal("200"))
        ema.update(Decimal("50"))
        self.assertEqual(ema.value, Decimal("100"))

    def test_convergence_to_constant(self):
        ema = EMAFilter(Decimal("0.5"))
        for _ in range(50):
            ema.update(Decimal("100"))
        self.assertLess(abs(ema.value - Decimal("100")), Decimal("1E-10"))

    def test_reset_clears_value(self):
        ema = EMAFilter(Decimal("0.5"))
        ema.update(Decimal("100"))
        ema.update(Decimal("200"))
        ema.reset()
        self.assertIsNone(ema.value)
        # Next update reinitializes.
        result = ema.update(Decimal("50"))
        self.assertEqual(result, Decimal("50"))

    def test_invalid_alpha_raises(self):
        with self.assertRaises(ValueError):
            EMAFilter(Decimal("1.5"))
        with self.assertRaises(ValueError):
            EMAFilter(Decimal("-0.1"))


class TestFeedHealth(unittest.TestCase):
    def test_initially_stale(self):
        h = FeedHealth("test", max_staleness_sec=5.0)
        self.assertTrue(h.is_stale(now=1000.0))

    def test_not_stale_after_update(self):
        h = FeedHealth("test", max_staleness_sec=5.0)
        h.mark_update(1000.0)
        self.assertFalse(h.is_stale(now=1000.0))
        self.assertFalse(h.is_stale(now=1004.9))

    def test_becomes_stale_after_max_staleness(self):
        h = FeedHealth("test", max_staleness_sec=5.0)
        h.mark_update(1000.0)
        self.assertTrue(h.is_stale(now=1005.1))

    def test_exactly_at_boundary(self):
        # Strict `>` means at exactly max_staleness, the feed is still fresh.
        h = FeedHealth("test", max_staleness_sec=5.0)
        h.mark_update(1000.0)
        self.assertFalse(h.is_stale(now=1005.0))

    def test_seconds_since_update_none_before_update(self):
        h = FeedHealth("test", max_staleness_sec=5.0)
        self.assertIsNone(h.seconds_since_update(now=1000.0))

    def test_seconds_since_update_after_update(self):
        h = FeedHealth("test", max_staleness_sec=5.0)
        h.mark_update(1000.0)
        self.assertEqual(h.seconds_since_update(now=1003.0), 3.0)

    def test_uid_unchanged_does_not_advance_timestamp(self):
        h = FeedHealth("test", max_staleness_sec=5.0)
        accepted = h.mark_update(1000.0, uid=42)
        self.assertTrue(accepted)
        # Same uid → rejected, timestamp not advanced.
        accepted2 = h.mark_update(1010.0, uid=42)
        self.assertFalse(accepted2)
        # Feed should now be stale based on the original 1000.0 timestamp.
        self.assertTrue(h.is_stale(now=1006.0))

    def test_uid_changed_advances_timestamp(self):
        h = FeedHealth("test", max_staleness_sec=5.0)
        h.mark_update(1000.0, uid=42)
        accepted = h.mark_update(1003.0, uid=43)
        self.assertTrue(accepted)
        self.assertFalse(h.is_stale(now=1004.0))


class TestLeadLagSignalProvider(unittest.TestCase):
    def _make(self, **overrides) -> LeadLagSignalProvider:
        defaults = dict(
            lead_windows_sec=[5, 10],
            buffer_duration_sec=60.0,
            ema_alpha_fx=Decimal("0.5"),
            max_leader_staleness_sec=5.0,
            max_fx_staleness_sec=5.0,
            max_local_staleness_sec=5.0,
        )
        defaults.update(overrides)
        return LeadLagSignalProvider(**defaults)

    def _push(
        self,
        p: LeadLagSignalProvider,
        t: float,
        local: tuple = (300_000, 300_100),
        leader: tuple = (50_000, 50_100),
        fx: tuple = (5.00, 5.02),
    ):
        p.update(
            t,
            Decimal(str(local[0])), Decimal(str(local[1])),
            Decimal(str(leader[0])), Decimal(str(leader[1])),
            Decimal(str(fx[0])), Decimal(str(fx[1])),
        )

    # ---- Basic price computation ---------------------------------------- #
    def test_local_mid_calculation(self):
        p = self._make()
        p.update(
            1000.0,
            Decimal("99.5"), Decimal("100.5"),
            Decimal("50000"), Decimal("50100"),
            Decimal("5.0"), Decimal("5.02"),
        )
        self.assertEqual(p.local_mid, Decimal("100.0"))

    def test_fair_brl_fast_basic_calculation(self):
        p = self._make()
        # fx=(5.00, 5.02) → fx_mid_raw=5.01; leader_mid=50050
        # fair_brl_fast = 50050 * 5.01 = 250750.5
        p.update(
            1000.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("50000"), Decimal("50100"),
            Decimal("5.00"), Decimal("5.02"),
        )
        self.assertEqual(p.fair_brl_fast, Decimal("250750.5"))

    def test_fair_brl_slow_uses_ema_for_fx_not_raw(self):
        p = self._make(ema_alpha_fx=Decimal("0.5"))
        # First update: fx=(5,5) → fx_mid_raw=5, fx_mid_ema=5
        p.update(
            1000.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("50000"), Decimal("50100"),
            Decimal("5"), Decimal("5"),
        )
        # Second update: fx=(7,7) → fx_mid_raw=7, fx_mid_ema=0.5*7+0.5*5=6
        p.update(
            1001.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("50000"), Decimal("50100"),
            Decimal("7"), Decimal("7"),
        )
        self.assertEqual(p.fx_mid_ema, Decimal("6"))
        # fair_brl_slow uses 6 (EMA), not 7 (raw). fast uses 7.
        self.assertEqual(p.fair_brl_slow, Decimal("50050") * Decimal("6"))
        self.assertEqual(p.fair_brl_fast, Decimal("50050") * Decimal("7"))

    # ---- Basis ---------------------------------------------------------- #
    def test_basis_bps_positive_when_local_above_fair(self):
        p = self._make()
        # Set up so that local_mid >> fair_brl_slow.
        # leader=50000, fx=4 → fair_slow ≈ 200_000
        # local_mid = 300_000 → basis = 10000 * (300000/200000 - 1) = 5000
        p.update(
            1000.0,
            Decimal("300000"), Decimal("300000"),
            Decimal("50000"), Decimal("50000"),
            Decimal("4"), Decimal("4"),
        )
        self.assertEqual(p.basis_bps, Decimal("5000"))

    def test_basis_bps_negative_when_local_below_fair(self):
        p = self._make()
        p.update(
            1000.0,
            Decimal("100000"), Decimal("100000"),
            Decimal("50000"), Decimal("50000"),
            Decimal("4"), Decimal("4"),
        )
        # fair = 200000, local = 100000 → basis = 10000 * (0.5 - 1) = -5000
        self.assertEqual(p.basis_bps, Decimal("-5000"))

    def test_basis_bps_zero_when_equal(self):
        p = self._make()
        # Set up so that local_mid == fair_slow exactly: leader=50000, fx=6
        # → fair_slow = 300000 = local_mid
        p.update(
            1000.0,
            Decimal("300000"), Decimal("300000"),
            Decimal("50000"), Decimal("50000"),
            Decimal("6"), Decimal("6"),
        )
        self.assertEqual(p.basis_bps, Decimal("0"))

    def test_basis_bps_zero_when_fair_zero(self):
        # No leader/fx data → fair is 0 → basis returns 0 (no ZeroDivisionError).
        p = self._make()
        p.update(
            1000.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("0"), Decimal("0"),
            Decimal("0"), Decimal("0"),
        )
        self.assertEqual(p.basis_bps, Decimal("0"))

    # ---- Lead signals --------------------------------------------------- #
    def test_lead_signal_leader_moves_first(self):
        # Time series:
        #   t=0..5: fair=100k, local=100k (baseline)
        #   t=1..5: fair jumps to 101k while local stays at 100k
        # At t=5 with window=5: fair_now=101k, fair_past(t=0)=100k
        #                       local_now=100k, local_past(t=0)=100k
        # leader_ret = log(1.01) ≈ +0.00995
        # local_ret  = 0
        # lead_signal_bps ≈ +99.5 → expect > 50.
        p = self._make(lead_windows_sec=[5])
        # t=0: baseline
        self._push(p, 0.0,
                   local=(100_000, 100_000), leader=(50_000, 50_000), fx=(2.0, 2.0))
        # fair_brl_fast = 50000 * 2 = 100_000 ✓
        # t=1..5: fair jumps to 101k, local stays
        # New fair = 101k → leader * fx must equal 101k. With fx=2, leader=50500.
        for t in [1.0, 2.0, 3.0, 4.0, 5.0]:
            self._push(p, t,
                       local=(100_000, 100_000),
                       leader=(50_500, 50_500),
                       fx=(2.0, 2.0))
        signal = p.lead_signal_bps(5)
        self.assertIsNotNone(signal)
        self.assertGreater(signal, Decimal("50"))

    def test_lead_signal_local_moves_first(self):
        # Inverse: local moves up 1%, fair stays.
        p = self._make(lead_windows_sec=[5])
        self._push(p, 0.0,
                   local=(100_000, 100_000), leader=(50_000, 50_000), fx=(2.0, 2.0))
        for t in [1.0, 2.0, 3.0, 4.0, 5.0]:
            self._push(p, t,
                       local=(101_000, 101_000),
                       leader=(50_000, 50_000),
                       fx=(2.0, 2.0))
        signal = p.lead_signal_bps(5)
        self.assertIsNotNone(signal)
        self.assertLess(signal, Decimal("-50"))

    def test_lead_signal_no_movement_both_flat(self):
        p = self._make(lead_windows_sec=[5])
        for t in [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]:
            self._push(p, t,
                       local=(100_000, 100_000),
                       leader=(50_000, 50_000),
                       fx=(2.0, 2.0))
        signal = p.lead_signal_bps(5)
        self.assertIsNotNone(signal)
        self.assertLess(abs(signal), Decimal("0.1"))

    def test_lead_signal_insufficient_history_returns_none(self):
        # Only 2s of buffer, request window=10.
        p = self._make(lead_windows_sec=[10])
        self._push(p, 0.0)
        self._push(p, 1.0)
        # window=10 → past=t=-9 → buffer doesn't have it → None
        self.assertIsNone(p.lead_signal_bps(10))

    def test_lead_signal_stale_leader_returns_none(self):
        # First update healthy. Next updates pass leader=(0,0), so leader feed
        # ages without being marked. After threshold, signal is None.
        p = self._make(lead_windows_sec=[5], max_leader_staleness_sec=5.0)
        # Healthy starting state with a few seconds of history
        for t in [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]:
            self._push(p, t)
        # At t=5 should have valid signal
        self.assertIsNotNone(p.lead_signal_bps(5))
        # Now skip forward without leader updates (leader=0,0)
        p.update(
            12.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("0"), Decimal("0"),
            Decimal("5.0"), Decimal("5.02"),
        )
        # last leader update was t=5; at t=12, age=7 > 5 → stale
        self.assertTrue(p.is_leader_stale)
        self.assertIsNone(p.lead_signal_bps(5))

    def test_lead_signal_stale_fx_returns_none(self):
        p = self._make(lead_windows_sec=[5], max_fx_staleness_sec=5.0)
        for t in [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]:
            self._push(p, t)
        self.assertIsNotNone(p.lead_signal_bps(5))
        p.update(
            12.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("50000"), Decimal("50100"),
            Decimal("0"), Decimal("0"),
        )
        self.assertTrue(p.is_fx_stale)
        self.assertIsNone(p.lead_signal_bps(5))

    def test_best_lead_signal_returns_max_abs_value(self):
        # We want signal_5s ≈ +30, signal_10s ≈ -50 → best returns -50.
        # Construct: at t=10, fair grew between t=0 and t=5, then dropped.
        # local stayed flat the whole time.
        p = self._make(lead_windows_sec=[5, 10])
        # t=0: baseline; fair_fast = 50000*2 = 100000
        self._push(p, 0.0,
                   local=(100_000, 100_000), leader=(50_000, 50_000), fx=(2.0, 2.0))
        # t=5: fair up moderately (~+30 bps cumulative on fair, local flat)
        # leader=50150 → fair_fast = 100300, log(100300/100000) ≈ 0.002995 → +30 bps
        for t in [1.0, 2.0, 3.0, 4.0, 5.0]:
            self._push(p, t,
                       local=(100_000, 100_000),
                       leader=(50_150, 50_150),
                       fx=(2.0, 2.0))
        # t=6..10: fair drops below baseline (cumulative -50 bps from t=0)
        # fair_fast at t=10 = 100000*exp(-0.005) ≈ 99501
        # Need leader=49750.5 with fx=2 → fair_fast=99501
        for t in [6.0, 7.0, 8.0, 9.0, 10.0]:
            self._push(p, t,
                       local=(100_000, 100_000),
                       leader=(49_750, 49_750),
                       fx=(2.0, 2.0))
        # window=5 measures lead from t=5 to t=10 → fair went 100300→99500 (~-80 bps)
        # window=10 measures from t=0 to t=10 → fair went 100000→99500 (~-50 bps)
        sig5 = p.lead_signal_bps(5)
        sig10 = p.lead_signal_bps(10)
        best = p.best_lead_signal_bps()
        self.assertIsNotNone(sig5)
        self.assertIsNotNone(sig10)
        self.assertIsNotNone(best)
        # best is max |.|, both are negative; sig5 has greater magnitude
        self.assertEqual(best, sig5)

    def test_best_lead_signal_returns_none_when_all_none(self):
        # Insufficient history.
        p = self._make(lead_windows_sec=[5, 10])
        self._push(p, 0.0)
        self._push(p, 1.0)
        self.assertIsNone(p.best_lead_signal_bps())

    # ---- Signal quality ------------------------------------------------- #
    def test_signal_quality_ok_with_healthy_feeds(self):
        p = self._make()
        self._push(p, 1000.0)
        self.assertEqual(p.signal_quality, SignalQuality.OK)
        self.assertFalse(p.is_any_stale)

    def test_signal_quality_degraded_fx_when_only_fx_stale(self):
        p = self._make()
        # Healthy first, then advance time without FX updates
        self._push(p, 1000.0)
        # Advance 7 sec with leader+local but no FX
        p.update(
            1007.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("50000"), Decimal("50100"),
            Decimal("0"), Decimal("0"),
        )
        self.assertEqual(p.signal_quality, SignalQuality.DEGRADED_FX)

    def test_signal_quality_degraded_leader_when_only_leader_stale(self):
        p = self._make()
        self._push(p, 1000.0)
        p.update(
            1007.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("0"), Decimal("0"),
            Decimal("5.0"), Decimal("5.02"),
        )
        self.assertEqual(p.signal_quality, SignalQuality.DEGRADED_LEADER)

    def test_signal_quality_degraded_local_when_only_local_stale(self):
        p = self._make()
        self._push(p, 1000.0)
        p.update(
            1007.0,
            Decimal("0"), Decimal("0"),
            Decimal("50000"), Decimal("50100"),
            Decimal("5.0"), Decimal("5.02"),
        )
        self.assertEqual(p.signal_quality, SignalQuality.DEGRADED_LOCAL)

    def test_signal_quality_bad_when_two_stale(self):
        p = self._make()
        self._push(p, 1000.0)
        # Both leader and fx stale (only local updates)
        p.update(
            1007.0,
            Decimal("300000"), Decimal("300100"),
            Decimal("0"), Decimal("0"),
            Decimal("0"), Decimal("0"),
        )
        self.assertEqual(p.signal_quality, SignalQuality.BAD)

    # ---- Spread --------------------------------------------------------- #
    def test_local_spread_bps_basic(self):
        p = self._make()
        p.update(
            1000.0,
            Decimal("99.5"), Decimal("100.5"),
            Decimal("50000"), Decimal("50100"),
            Decimal("5.0"), Decimal("5.02"),
        )
        # spread = 1.0, mid = 100.0, bps = 100
        self.assertEqual(p.local_spread_bps, Decimal("100"))

    def test_local_spread_bps_zero_when_no_data(self):
        p = self._make()
        p.update(
            1000.0,
            Decimal("0"), Decimal("0"),
            Decimal("50000"), Decimal("50100"),
            Decimal("5.0"), Decimal("5.02"),
        )
        self.assertEqual(p.local_spread_bps, Decimal("0"))

    # ---- Validation ----------------------------------------------------- #
    def test_invalid_lead_windows_raises(self):
        with self.assertRaises(ValueError):
            LeadLagSignalProvider(lead_windows_sec=[])
        with self.assertRaises(ValueError):
            LeadLagSignalProvider(lead_windows_sec=[5, -1])


if __name__ == "__main__":
    unittest.main()
