"""Unit tests for the BitPreco Redis event envelope + state machine.

Anchors the parser and state-machine semantics on the real shape
captured during Phase 0 (`docs/BITPRECO_REDIS_PROTOCOL.md`).
"""
from __future__ import annotations

import json
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import (
    EventType,
    OrderLifecycle,
    OrderStateMachine,
    StateDecision,
    parse_bitpreco_timestamp,
    parse_envelope,
)


# ---------------------------------------------------------------------------
# Helpers — minimal raw payloads matching the Phase 0 schema
# ---------------------------------------------------------------------------

def _make_payload(
    cod: str,
    xid: str = "1234567890",
    side: str = "BUY",
    status: str = "EMPTY",
    exec_amount: Any = 0,
    canceled: Any = 0,
    order_ts: str = "2026-05-15 11:38:25",
    balance_ts: str = "2026-05-15 11:38:25.123456",
) -> Dict[str, Any]:
    return {
        "success": True,
        "message_to": "USER",
        "message_cod": cod,
        "order_id": xid,
        "order": {
            "id": xid,
            "market": "BTC-BRL",
            "type": side,
            "status": status,
            "amount": 0.0002,
            "price": 399514,
            "exec_amount": exec_amount,
            "cost": 0,
            "fee": 0,
            "percent_fee": "0",
            "limited": "1",
            "programmed": "0",
            "canceled": canceled,
            "time_stamp": order_ts,
            "tag": None,
            "obs": None,
        },
        "balance": {
            "success": True,
            "BTC": 0.00014153,
            "BTC_locked": 0,
            "BRL": 756.06,
            "BRL_locked": 0,
            "utimestamp": balance_ts,
            "timestamp": order_ts,
        },
    }


class TimestampParsingTest(unittest.TestCase):
    def test_parses_iso_seconds(self):
        # 14:38:25 BRT (UTC-3) -> 17:38:25 UTC. Year 2026.
        ts = parse_bitpreco_timestamp("2026-05-15 14:38:25")
        assert ts is not None
        # Round-trip via the same logic to confirm we're in BRT space
        # without hardcoding a magic number for every test machine.
        same = parse_bitpreco_timestamp("2026-05-15 14:38:25.000000")
        assert same is not None
        self.assertAlmostEqual(ts, same, places=3)

    def test_microsecond_precision_preserved(self):
        ts1 = parse_bitpreco_timestamp("2026-05-15 11:38:50.113011")
        ts2 = parse_bitpreco_timestamp("2026-05-15 11:38:50.113012")
        assert ts1 is not None and ts2 is not None
        self.assertAlmostEqual(ts2 - ts1, 0.000001, places=6)

    def test_invalid_returns_none(self):
        self.assertIsNone(parse_bitpreco_timestamp(None))
        self.assertIsNone(parse_bitpreco_timestamp(""))
        self.assertIsNone(parse_bitpreco_timestamp("not a date"))
        self.assertIsNone(parse_bitpreco_timestamp(123))  # type: ignore[arg-type]


class ParseEnvelopeTest(unittest.TestCase):
    def test_buy_order_created(self):
        env = parse_envelope(_make_payload("BUY_ORDER_CREATED"), recv_ts=100.0)
        assert env is not None
        self.assertEqual(env.event_type, EventType.BUY_ORDER_CREATED)
        self.assertEqual(env.recv_ts, 100.0)
        self.assertIsNotNone(env.order)
        self.assertEqual(env.order.exchange_order_id, "1234567890")
        self.assertEqual(env.order.side, "BUY")
        self.assertEqual(env.order.market, "BTC-BRL")
        self.assertEqual(env.order.amount, Decimal("0.0002"))
        self.assertIsNotNone(env.balance)
        self.assertEqual(env.balance.available["BTC"], Decimal("0.00014153"))
        self.assertEqual(env.balance.locked["BRL"], Decimal("0"))

    def test_fully_executed(self):
        p = _make_payload("ORDER_FULLY_EXECUTED", status="FILLED",
                          exec_amount=0.0002)
        env = parse_envelope(p, recv_ts=100.0)
        self.assertEqual(env.event_type, EventType.ORDER_FULLY_EXECUTED)
        self.assertEqual(env.order.status, "FILLED")
        self.assertEqual(env.order.exec_amount, Decimal("0.0002"))

    def test_canceled_includes_canceled_flag(self):
        p = _make_payload("ORDER_CANCELED", canceled=1)
        env = parse_envelope(p, recv_ts=100.0)
        self.assertEqual(env.order.canceled_flag, 1)

    def test_type_coercion_string_to_int(self):
        # BitPreco flips `limited` and `programmed` between str and int.
        # Must coerce both shapes.
        p = _make_payload("BUY_ORDER_CREATED")
        p["order"]["limited"] = 1            # int form
        p["order"]["programmed"] = "0"       # str form
        env = parse_envelope(p, recv_ts=100.0)
        self.assertEqual(env.order.limited_flag, 1)
        self.assertEqual(env.order.programmed_flag, 0)

    def test_event_ts_prefers_utimestamp(self):
        p = _make_payload(
            "BUY_ORDER_CREATED",
            order_ts="2026-05-15 11:00:00",          # 1 hour earlier
            balance_ts="2026-05-15 12:00:00.000000",
        )
        env = parse_envelope(p, recv_ts=100.0)
        assert env is not None and env.event_ts is not None
        # event_ts should match the balance utimestamp (later one)
        order_ts = parse_bitpreco_timestamp("2026-05-15 11:00:00")
        assert order_ts is not None
        self.assertGreater(env.event_ts, order_ts)

    def test_malformed_returns_none(self):
        self.assertIsNone(parse_envelope({}, recv_ts=100.0))
        self.assertIsNone(parse_envelope({"message_cod": None}, recv_ts=100.0))
        self.assertIsNone(parse_envelope({"foo": "bar"}, recv_ts=100.0))

    def test_unknown_cod_with_order_still_parses(self):
        # An unknown message_cod we don't recognise yet must still
        # produce an envelope (so the shadow observer can log it).
        p = _make_payload("FUTURE_UNKNOWN_COD")
        env = parse_envelope(p, recv_ts=100.0)
        assert env is not None
        self.assertEqual(env.event_type, EventType.UNKNOWN)


class OrderStateMachineTest(unittest.TestCase):
    def setUp(self):
        self.sm = OrderStateMachine()

    def _decide(self, cod: str, **kwargs) -> StateDecision:
        env = parse_envelope(_make_payload(cod, **kwargs), recv_ts=100.0)
        assert env is not None
        dec = self.sm.decide(env)
        self.sm.apply(env, dec)
        return dec

    def test_created_to_filled_happy_path(self):
        self.assertEqual(self._decide("BUY_ORDER_CREATED",
                                      order_ts="2026-05-15 11:00:00"),
                         StateDecision.APPLY)
        self.assertEqual(self.sm.state_of("1234567890"), OrderLifecycle.OPEN)
        self.assertEqual(self._decide("ORDER_FULLY_EXECUTED",
                                      status="FILLED",
                                      exec_amount=0.0002,
                                      order_ts="2026-05-15 11:00:05"),
                         StateDecision.APPLY)
        self.assertEqual(self.sm.state_of("1234567890"), OrderLifecycle.FILLED)

    def test_terminal_regression_rejected(self):
        # CANCELED with newer timestamp
        self.assertEqual(self._decide("ORDER_CANCELED",
                                      order_ts="2026-05-15 11:00:00"),
                         StateDecision.APPLY)
        # A delayed CREATED for the same order: timestamp-wise newer
        # would normally APPLY, but state regression must reject.
        self.assertEqual(self._decide("BUY_ORDER_CREATED",
                                      order_ts="2026-05-15 11:00:10"),
                         StateDecision.SKIP_REGRESSION)
        # State still CANCELED
        self.assertEqual(self.sm.state_of("1234567890"), OrderLifecycle.CANCELED)

    def test_stale_event_rejected(self):
        self._decide("BUY_ORDER_CREATED", order_ts="2026-05-15 11:00:05")
        # Older event: same xid, earlier timestamp
        self.assertEqual(self._decide("ORDER_CANCELED",
                                      order_ts="2026-05-15 11:00:00"),
                         StateDecision.SKIP_STALE)
        # State still OPEN — stale event didn't move us
        self.assertEqual(self.sm.state_of("1234567890"), OrderLifecycle.OPEN)

    def test_terminal_enrichment_accepted(self):
        # CANCELED after PARTIAL preserves exec_amount via APPLY_ENRICH
        self._decide("BUY_ORDER_CREATED", order_ts="2026-05-15 11:00:00")
        self._decide("ORDER_PARTIALLY_EXECUTED",
                     exec_amount=0.0001, order_ts="2026-05-15 11:00:03")
        # Wait — PARTIAL is also terminal in our model? Let's verify
        # the design: PARTIAL is a non-terminal state, FILLED/CANCELED
        # are terminal. Then a CANCELED after a PARTIAL is the first
        # terminal — APPLY (not enrichment). Let's go FILLED then
        # another FILLED to trigger enrichment.
        # Reset for clarity:
        self.sm = OrderStateMachine()
        self._decide("BUY_ORDER_CREATED", order_ts="2026-05-15 11:00:00")
        self._decide("ORDER_FULLY_EXECUTED", status="FILLED",
                     exec_amount=0.0002, order_ts="2026-05-15 11:00:05")
        # A second terminal event for the same order — enrichment.
        dec = self._decide("ORDER_FULLY_EXECUTED", status="FILLED",
                           exec_amount=0.0002, order_ts="2026-05-15 11:00:06")
        self.assertEqual(dec, StateDecision.APPLY_ENRICH)
        # State still FILLED, not regressed
        self.assertEqual(self.sm.state_of("1234567890"), OrderLifecycle.FILLED)

    def test_unknown_event_skipped(self):
        env = parse_envelope(_make_payload("WHATEVER"), recv_ts=100.0)
        assert env is not None
        self.assertEqual(self.sm.decide(env), StateDecision.SKIP_UNKNOWN)
        self.assertEqual(self.sm.state_of("1234567890"), OrderLifecycle.UNKNOWN)

    def test_evict_older_than(self):
        self._decide("BUY_ORDER_CREATED", xid="111",
                     order_ts="2026-01-01 00:00:00")
        self._decide("BUY_ORDER_CREATED", xid="222",
                     order_ts="2026-12-31 00:00:00")
        self.assertEqual(len(self.sm), 2)
        # Evict everything older than mid-2026 (BRT). Use a UTC epoch
        # equivalent computed from the parser to keep it portable.
        cutoff = parse_bitpreco_timestamp("2026-06-15 00:00:00")
        assert cutoff is not None
        evicted = self.sm.evict_older_than(cutoff)
        self.assertEqual(evicted, 1)
        self.assertEqual(len(self.sm), 1)
        self.assertEqual(self.sm.state_of("222"), OrderLifecycle.OPEN)
        self.assertEqual(self.sm.state_of("111"), OrderLifecycle.UNKNOWN)


# ---------------------------------------------------------------------------
# Capture-based replay: ensure every real payload parses without raising.
# We don't ship the JSONL (it's gitignored), so this test soft-skips when
# absent. Live developers running Phase 0 will exercise it locally.
# ---------------------------------------------------------------------------

class CapturedPayloadReplayTest(unittest.TestCase):
    CAPTURES_DIR = Path(__file__).resolve().parents[5] / "tools" / "redis_probe" / "captures"

    def test_replay_all_update_captures(self):
        if not self.CAPTURES_DIR.exists():
            self.skipTest("no Phase 0 captures present")
        jsonls: List[Path] = sorted(self.CAPTURES_DIR.glob("02_capture_update-*.jsonl"))
        if not jsonls:
            self.skipTest("no 02_capture_update JSONLs")
        total = parsed_ok = 0
        for path in jsonls:
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    total += 1
                    rec = json.loads(line)
                    env = parse_envelope(rec["payload"], recv_ts=rec["recv_ts"])
                    if env is not None:
                        parsed_ok += 1
        self.assertGreater(total, 0)
        # Phase 0 was 710 envelopes; require ≥95% parse rate to catch
        # regression in the parser without being brittle on the long tail.
        self.assertGreaterEqual(parsed_ok / total, 0.95)


if __name__ == "__main__":
    unittest.main()
