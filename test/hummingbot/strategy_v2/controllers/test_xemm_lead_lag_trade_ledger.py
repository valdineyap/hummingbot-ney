"""Tests for ``TradeLedger.record_raw_fill`` and friends.

Why a focused suite: production logs showed real maker fills happening
without ``state.json``/``trades.jsonl`` ever being created — the prior
``record_fill(ex)`` path missed partial-fill-then-cancel and rebalance
``MARKET`` flows. ``record_raw_fill`` is the new, event-driven capture
path. These tests pin its behaviour down so the regression can't sneak
back.
"""

import json
import os
import tempfile
import unittest
from collections import namedtuple
from decimal import Decimal

from controllers.generic.xemm_lead_lag import TradeLedger


# Minimal stand-in for OrderFilledEvent — it is a NamedTuple in production
# so a regular namedtuple with the same field names is interchangeable for
# the ledger's purposes.
FakeFillEvent = namedtuple(
    "FakeFillEvent",
    [
        "timestamp", "order_id", "trading_pair", "trade_type", "order_type",
        "price", "amount", "trade_fee", "exchange_trade_id", "exchange_order_id",
    ],
)


class _StrEnum(str):
    """Mimics enum members with a ``.name`` attribute used by record_raw_fill."""

    def __new__(cls, name):
        obj = super().__new__(cls, name)
        obj.name = name
        return obj


SELL = _StrEnum("SELL")
BUY = _StrEnum("BUY")
LIMIT_MAKER = _StrEnum("LIMIT_MAKER")
MARKET = _StrEnum("MARKET")


class TradeLedgerRawFillTest(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ledger_test_")
        self.ledger = TradeLedger(self.tmpdir, quote_asset="BRL")
        self.jsonl_path = os.path.join(self.tmpdir, "trades.jsonl")
        self.state_path = os.path.join(self.tmpdir, "state.json")
        self.touch_path = os.path.join(self.tmpdir, "last_fill.touch")

    def _make_event(self, **overrides):
        defaults = dict(
            timestamp=1778173123.0,
            order_id="SBCBL_test",
            trading_pair="BTC-BRL",
            trade_type=SELL,
            order_type=LIMIT_MAKER,
            price=Decimal("393401.99"),
            amount=Decimal("0.00003812"),
            trade_fee=None,  # unused
            exchange_trade_id="trade_xyz",
            exchange_order_id="2055034667",
        )
        defaults.update(overrides)
        return FakeFillEvent(**defaults)

    def test_partial_maker_fill_creates_all_three_artefacts(self):
        """The 16:58:43 production scenario — small partial sell fill.
        Before the fix this never reached the ledger because the executor
        never closed with ``filled_amount_quote > 0``."""
        event = self._make_event()
        self.ledger.record_raw_fill(event, source_connector="bitpreco")

        # All three artefacts must exist after the first fill.
        self.assertTrue(os.path.isfile(self.jsonl_path), "trades.jsonl missing")
        self.assertTrue(os.path.isfile(self.state_path), "state.json missing")
        self.assertTrue(os.path.isfile(self.touch_path), "last_fill.touch missing")

        # trades.jsonl line must round-trip and carry the right fields.
        with open(self.jsonl_path) as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 1)
        rec = json.loads(lines[0])
        self.assertEqual(rec["kind"], "fill")
        self.assertEqual(rec["source_connector"], "bitpreco")
        self.assertEqual(rec["side"], "SELL")
        self.assertEqual(rec["order_type"], "LIMIT_MAKER")
        self.assertEqual(rec["client_order_id"], "SBCBL_test")
        self.assertEqual(rec["exchange_order_id"], "2055034667")
        self.assertEqual(rec["fill_amount_base"], "0.00003812")
        self.assertEqual(rec["fill_price"], "393401.99")
        # Decimal precision preserved end-to-end; verify by recomputing.
        self.assertEqual(
            Decimal(rec["fill_amount_quote"]),
            Decimal("0.00003812") * Decimal("393401.99"),
        )

    def test_state_json_reflects_latest_fill(self):
        """``last_trade`` in ``state.json`` is overwritten on every fill."""
        first = self._make_event(amount=Decimal("0.0001"), exchange_order_id="A")
        second = self._make_event(amount=Decimal("0.0002"), exchange_order_id="B")
        self.ledger.record_raw_fill(first, source_connector="bitpreco")
        self.ledger.record_raw_fill(second, source_connector="bitpreco")

        with open(self.state_path) as f:
            state = json.load(f)
        self.assertEqual(state["trades_total_session"], 2)
        self.assertEqual(state["last_trade"]["exchange_order_id"], "B")

    def test_zero_amount_fill_is_skipped(self):
        """Defensive: amount=0 should never produce a record (would break
        downstream consumers expecting amount > 0)."""
        event = self._make_event(amount=Decimal("0"))
        self.ledger.record_raw_fill(event, source_connector="bitpreco")
        self.assertFalse(os.path.isfile(self.jsonl_path))

    def test_market_rebalance_fill_recorded_with_correct_label(self):
        """Inventory_audit's MARKET rebalance also flows through
        ``record_raw_fill`` and must be distinguishable in the ledger."""
        event = self._make_event(
            order_type=MARKET,
            trade_type=BUY,
            order_id="x-MG43PCSNBBCBL_rebalance",
        )
        self.ledger.record_raw_fill(event, source_connector="binance")

        with open(self.jsonl_path) as f:
            rec = json.loads(f.readline())
        self.assertEqual(rec["order_type"], "MARKET")
        self.assertEqual(rec["source_connector"], "binance")
        self.assertEqual(rec["side"], "BUY")
        # ``kind: "fill"`` keeps it distinct from the executor-completion
        # records (``kind: "trade"``).
        self.assertEqual(rec["kind"], "fill")

    def test_exception_in_recording_does_not_propagate(self):
        """A disk failure must never crash the strategy. We force this by
        making jsonl_path a directory so the open() fails."""
        # Wreck the path on purpose.
        os.remove(self.jsonl_path) if os.path.exists(self.jsonl_path) else None
        os.mkdir(self.jsonl_path)  # now opening for append will fail
        event = self._make_event()
        try:
            self.ledger.record_raw_fill(event, source_connector="bitpreco")
        except Exception as e:
            self.fail(f"record_raw_fill must swallow exceptions, got {e}")


class TradeLedgerRecordFillSlippageTest(unittest.TestCase):
    """Task 4.3 — slippage_bps and fill_to_hedge_latency_ms in trade records."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ledger_slip_test_")
        self.ledger = TradeLedger(self.tmpdir, quote_asset="BRL")
        self.jsonl_path = os.path.join(self.tmpdir, "trades.jsonl")

    def _make_executor_info(self, *, side, filled_quote, expected_price,
                            order_amount, latency_ms=42):
        """Stand-in for ExecutorInfo. Only the attributes ``record_fill``
        actually reads need to be present."""
        ex = type("ExStub", (), {})()
        ex.id = "test-exec"
        ex.filled_amount_quote = Decimal(filled_quote)
        ex.net_pnl_quote = Decimal("0.05")
        ex.cum_fees_quote = Decimal("0.02")
        ex.close_type = "COMPLETED"
        ex.close_timestamp = 1234.0
        ex.custom_info = {
            "side": _StrEnum(side),
            "maker_connector": "bitpreco",
            "taker_connector": "binance",
            "maker_trading_pair": "BTC-BRL",
            "taker_expected_price": Decimal(expected_price),
            "order_amount": Decimal(order_amount),
            "fill_to_hedge_latency_ms": latency_ms,
        }
        return ex

    def _read_record(self):
        with open(self.jsonl_path) as f:
            return json.loads(f.readline())

    def test_slippage_for_maker_sell_when_taker_buys_higher_is_negative(self):
        """Maker SELL → taker BUY. If taker filled at a HIGHER price than
        expected, we paid more for the hedge → negative slippage_bps."""
        # Maker sold 0.0002 @ ~395000; expected taker BUY @ 394700.
        # Actual taker_avg = filled_quote / order_amount
        #                  = 78.95 / 0.0002 = 394750
        # So actual (394750) > expected (394700) → bad for maker SELL.
        ex = self._make_executor_info(
            side="SELL", filled_quote="78.95", expected_price="394700",
            order_amount="0.0002",
        )
        self.ledger.record_fill(ex)
        rec = self._read_record()
        self.assertEqual(rec["kind"], "trade")
        self.assertIsNotNone(rec["slippage_bps"])
        # (394700 - 394750) / 394700 * 10000 = -1.27 bps
        self.assertAlmostEqual(rec["slippage_bps"], -1.27, places=1)
        self.assertEqual(rec["fill_to_hedge_latency_ms"], 42)

    def test_slippage_for_maker_buy_when_taker_sells_lower_is_negative(self):
        """Maker BUY → taker SELL. If taker filled LOWER than expected, we
        received less for the hedge → negative slippage_bps."""
        # Expected taker SELL @ 395000, actual = 78.94 / 0.0002 = 394700
        ex = self._make_executor_info(
            side="BUY", filled_quote="78.94", expected_price="395000",
            order_amount="0.0002",
        )
        self.ledger.record_fill(ex)
        rec = self._read_record()
        # (394700 - 395000) / 395000 * 10000 = -7.59 bps
        self.assertAlmostEqual(rec["slippage_bps"], -7.59, places=1)

    def test_no_slippage_when_expected_price_missing(self):
        """If the executor never set taker_expected_price (e.g. cold path),
        slippage_bps must be None — not a junk value or exception."""
        ex = self._make_executor_info(
            side="SELL", filled_quote="78.95", expected_price="0",
            order_amount="0.0002",
        )
        ex.custom_info["taker_expected_price"] = None
        self.ledger.record_fill(ex)
        rec = self._read_record()
        self.assertIsNone(rec["slippage_bps"])

    def test_zero_filled_executor_skipped(self):
        """No fill → no record (existing record_fill guard)."""
        ex = self._make_executor_info(
            side="SELL", filled_quote="0", expected_price="394700",
            order_amount="0.0002",
        )
        self.ledger.record_fill(ex)
        self.assertFalse(os.path.isfile(self.jsonl_path))


class BitprecoTimestampParseTest(unittest.TestCase):
    """Pin down ``_parse_bitpreco_timestamp`` so the TZ-naive bug stays
    fixed. Production cost of regression: legitimate orders cancelled
    within ~60ms of life by orphan_check (false positive)."""

    def test_returns_none_for_empty(self):
        from controllers.generic.xemm_lead_lag import XEMMLeadLagController
        self.assertIsNone(XEMMLeadLagController._parse_bitpreco_timestamp(None))
        self.assertIsNone(XEMMLeadLagController._parse_bitpreco_timestamp(""))

    def test_returns_none_for_garbage(self):
        from controllers.generic.xemm_lead_lag import XEMMLeadLagController
        self.assertIsNone(XEMMLeadLagController._parse_bitpreco_timestamp("not-a-time"))

    def test_known_bitpreco_string_resolves_to_correct_epoch(self):
        """Production sample: BitPreco returned ``time_stamp='2026-05-07 13:58:37'``
        for an event whose UTC log timestamp was ``16:58:37``. The parser must
        produce the epoch corresponding to ``16:58:37 UTC``, not ``13:58:37 UTC``
        (which would be 10800 s in the past — the bug)."""
        from controllers.generic.xemm_lead_lag import XEMMLeadLagController
        from datetime import datetime, timezone
        epoch = XEMMLeadLagController._parse_bitpreco_timestamp("2026-05-07 13:58:37")
        # Compute the expected: 16:58:37 UTC on the same day, as epoch.
        expected = datetime(2026, 5, 7, 16, 58, 37, tzinfo=timezone.utc).timestamp()
        self.assertEqual(epoch, expected)

    def test_age_calculation_is_independent_of_host_tz(self):
        """Smoke test: a freshly placed order (BitPreco time_stamp matches
        ``now`` in São Paulo time) must yield an age of ~0 seconds, regardless
        of the host's TZ. Pre-fix the test would have shown ~10800 s on a UTC
        host."""
        from controllers.generic.xemm_lead_lag import XEMMLeadLagController
        from datetime import datetime, timezone, timedelta
        # Simulate a "just now" BitPreco time_stamp (UTC-3) corresponding to
        # the current real moment.
        now_utc = datetime.now(timezone.utc)
        bp_time = (now_utc.astimezone(timezone(timedelta(hours=-3)))
                   .strftime("%Y-%m-%d %H:%M:%S"))
        parsed = XEMMLeadLagController._parse_bitpreco_timestamp(bp_time)
        self.assertIsNotNone(parsed)
        # Age should be at most a few seconds (string truncation discards
        # subsecond), never on the order of hours.
        age = now_utc.timestamp() - parsed
        self.assertLess(age, 5.0,
                        f"age={age:.2f}s — TZ regression: "
                        "orphan_check would cancel fresh orders.")


if __name__ == "__main__":
    unittest.main()
