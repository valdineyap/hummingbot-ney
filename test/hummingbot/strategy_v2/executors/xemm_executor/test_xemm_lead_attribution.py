"""Tests for lead-lag P&L attribution instrumentation.

The lead-lag signal influences the maker placement via ``lead_mode``
(favours/neutral/opposes) and ``lead_bps``. To measure whether the
signal adds edge over a fixed-threshold baseline, every executor close
record must carry the signal state from the placement that produced
the fill. These tests verify the carry-through:

  1. ``__init__`` initialises the snapshot fields to None.
  2. ``get_custom_info`` exposes both fields alongside the base fields.
  3. The fields are persisted onto the trade record via
     ``TradeLedger.record_fill`` reading from ``custom_info``.

The actual write-at-placement (in ``create_maker_order``) is exercised
indirectly here by setting the instance attrs and asserting they
propagate. ``create_maker_order``'s price-calculation path has its own
test file; we don't re-test it.
"""

import asyncio
import json
import os
import tempfile
import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from hummingbot.strategy_v2.executors.xemm_executor.xemm_lead_lag_executor import (
    XEMMLeadLagExecutor,
)


def _make_executor() -> XEMMLeadLagExecutor:
    ex = XEMMLeadLagExecutor.__new__(XEMMLeadLagExecutor)
    # Match the __init__ effects we care about.
    ex._hedged_maker_order_ids = set()
    ex._lead_mode_at_placement = None
    ex._lead_bps_at_placement = None
    return ex


class LeadSnapshotInitTest(unittest.TestCase):
    """Phase 1: snapshot fields start unset."""

    def test_init_attrs_default_to_none(self):
        ex = _make_executor()
        self.assertIsNone(ex._lead_mode_at_placement)
        self.assertIsNone(ex._lead_bps_at_placement)


class GetCustomInfoSurfacesLeadTest(unittest.TestCase):
    """``get_custom_info`` exposes the snapshot via super().get_custom_info()
    + the two new keys."""

    def _patch_super_custom_info(self, ex, base_info):
        """Replace the base class's get_custom_info via a parent class proxy."""
        from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import (
            XEMMExecutor,
        )
        # Patch at the class level for this test only (restored by tearDown).
        self._orig = XEMMExecutor.get_custom_info
        XEMMExecutor.get_custom_info = lambda self_: dict(base_info)

    def tearDown(self):
        if hasattr(self, "_orig"):
            from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import (
                XEMMExecutor,
            )
            XEMMExecutor.get_custom_info = self._orig

    def test_lead_fields_appear_in_custom_info_when_unset(self):
        ex = _make_executor()
        self._patch_super_custom_info(ex, {"order_amount": Decimal("0.0002")})
        info = ex.get_custom_info()
        self.assertIn("lead_mode_at_placement", info)
        self.assertIn("lead_bps_at_placement", info)
        self.assertIsNone(info["lead_mode_at_placement"])
        self.assertIsNone(info["lead_bps_at_placement"])
        # Base fields still present.
        self.assertEqual(info["order_amount"], Decimal("0.0002"))

    def test_lead_fields_reflect_snapshot_when_set(self):
        ex = _make_executor()
        ex._lead_mode_at_placement = "favours"
        ex._lead_bps_at_placement = Decimal("5.43")
        self._patch_super_custom_info(ex, {"order_amount": Decimal("0.0002")})
        info = ex.get_custom_info()
        self.assertEqual(info["lead_mode_at_placement"], "favours")
        self.assertEqual(info["lead_bps_at_placement"], Decimal("5.43"))

    def test_lead_fields_are_last_write_wins(self):
        """If a single executor places twice (cancel + replace), the snapshot
        reflects the LAST placement — the one alive at fill time."""
        ex = _make_executor()
        ex._lead_mode_at_placement = "neutral"
        ex._lead_bps_at_placement = Decimal("1.20")
        # Simulate a second placement overwriting.
        ex._lead_mode_at_placement = "opposes"
        ex._lead_bps_at_placement = Decimal("-7.50")
        self._patch_super_custom_info(ex, {})
        info = ex.get_custom_info()
        self.assertEqual(info["lead_mode_at_placement"], "opposes")
        self.assertEqual(info["lead_bps_at_placement"], Decimal("-7.50"))


class TradeLedgerRecordsLeadFieldsTest(unittest.TestCase):
    """``TradeLedger.record_fill`` reads from ``custom_info`` and writes the
    two new columns to trades.jsonl."""

    def setUp(self):
        # Import lazily — avoids loading the controller's Pydantic stack at
        # module import time (which pulls Decimal config defaults).
        from controllers.generic.xemm_lead_lag import TradeLedger
        self._TradeLedger = TradeLedger
        self._tmpdir = tempfile.mkdtemp(prefix="ledger_test_")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _make_ledger(self):
        return self._TradeLedger(log_dir=self._tmpdir, quote_asset="BRL")

    def _make_executor_info(self, ci: dict, net_pnl: str = "-0.05"):
        # NOT a MagicMock — record_fill walks several attrs via getattr with
        # defaults, and a bare MagicMock returns nested MagicMocks for any
        # un-set attr which then fails JSON serialisation. Use a plain
        # object with only the attrs record_fill actually reads.
        ex = type("ExecInfo", (), {
            "id": "test_exec_id",
            "filled_amount_quote": Decimal("80"),
            "net_pnl_quote": Decimal(net_pnl),
            "cum_fees_quote": Decimal("0.05"),
            "custom_info": ci,
            "close_type": None,
            "close_timestamp": 1747300000,
            "trading_pair": None,
        })()
        return ex

    def _read_last_record(self) -> dict:
        path = os.path.join(self._tmpdir, "trades.jsonl")
        with open(path) as f:
            lines = [ln for ln in f if ln.strip()]
        return json.loads(lines[-1])

    def test_records_lead_mode_favours(self):
        ledger = self._make_ledger()
        ex = self._make_executor_info({
            "side": "BUY",
            "maker_connector": "bitpreco",
            "taker_connector": "binance_sbe",
            "order_amount": Decimal("0.0002"),
            "lead_mode_at_placement": "favours",
            "lead_bps_at_placement": Decimal("5.43"),
        })
        ledger.record_fill(ex)
        rec = self._read_last_record()
        self.assertEqual(rec["lead_mode_at_placement"], "favours")
        self.assertEqual(rec["lead_bps_at_placement"], "5.43")
        self.assertEqual(rec["kind"], "trade")

    def test_records_lead_mode_opposes_with_negative_bps(self):
        ledger = self._make_ledger()
        ex = self._make_executor_info({
            "side": "SELL",
            "lead_mode_at_placement": "opposes",
            "lead_bps_at_placement": Decimal("-9.18"),
        })
        ledger.record_fill(ex)
        rec = self._read_last_record()
        self.assertEqual(rec["lead_mode_at_placement"], "opposes")
        self.assertEqual(rec["lead_bps_at_placement"], "-9.18")

    def test_missing_lead_fields_become_null_in_record(self):
        """Executors that predate the instrumentation (no lead fields in
        custom_info) still record cleanly with null lead fields."""
        ledger = self._make_ledger()
        ex = self._make_executor_info({
            "side": "BUY",
            # No lead_mode_at_placement / lead_bps_at_placement
        })
        ledger.record_fill(ex)
        rec = self._read_last_record()
        self.assertIsNone(rec["lead_mode_at_placement"])
        self.assertIsNone(rec["lead_bps_at_placement"])

    def test_lead_neutral_zero_bps(self):
        ledger = self._make_ledger()
        ex = self._make_executor_info({
            "side": "BUY",
            "lead_mode_at_placement": "neutral",
            "lead_bps_at_placement": Decimal("0"),
        })
        ledger.record_fill(ex)
        rec = self._read_last_record()
        self.assertEqual(rec["lead_mode_at_placement"], "neutral")
        # Decimal('0') → "0" (jsonable Decimal stringification)
        self.assertEqual(rec["lead_bps_at_placement"], "0")


if __name__ == "__main__":
    unittest.main()
