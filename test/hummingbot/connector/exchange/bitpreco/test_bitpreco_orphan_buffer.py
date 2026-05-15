"""Unit tests for OrphanEventBuffer.

The buffer holds Redis events for an exchange_order_id the connector
hasn't mapped yet. Critical invariants:

- ``observe`` stores in arrival order
- ``replay`` drains AND removes
- ``sweep`` evicts past TTL
- Max-entry cap drops the oldest bucket, not random ones
"""
from __future__ import annotations

import unittest
from itertools import count

from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import (
    EventEnvelope,
    EventType,
    OrderInfo,
    parse_envelope,
)
from hummingbot.connector.exchange.bitpreco.bitpreco_orphan_buffer import OrphanEventBuffer


def _envelope_for(xid: str, recv_ts: float = 0.0,
                  cod: str = "BUY_ORDER_CREATED") -> EventEnvelope:
    """Build a minimal envelope without going through parse_envelope —
    fewer fields to fuss with in these tests."""
    return EventEnvelope(
        event_type=EventType(cod),
        order=OrderInfo(
            exchange_order_id=xid,
            market="BTC-BRL",
            side="BUY",
            status="EMPTY",
            amount=None,
            price=None,
        ),
        balance=None,
        recv_ts=recv_ts,
        event_ts=recv_ts,
    )


class OrphanBufferTest(unittest.TestCase):
    def test_observe_and_replay_preserves_order(self):
        clock = count(start=100.0, step=0.5)
        buf = OrphanEventBuffer(ttl_seconds=60, now_fn=lambda: next(clock))
        buf.observe(_envelope_for("X1", recv_ts=100.0, cod="BUY_ORDER_CREATED"))
        buf.observe(_envelope_for("X1", recv_ts=100.5, cod="ORDER_FULLY_EXECUTED"))
        buf.observe(_envelope_for("X2", recv_ts=101.0, cod="SELL_ORDER_CREATED"))

        self.assertEqual(len(buf), 2)
        self.assertEqual(buf.total_events(), 3)
        self.assertTrue(buf.contains("X1"))
        self.assertTrue(buf.contains("X2"))

        x1_events = buf.replay("X1")
        self.assertEqual(len(x1_events), 2)
        self.assertEqual(x1_events[0].event_type, EventType.BUY_ORDER_CREATED)
        self.assertEqual(x1_events[1].event_type, EventType.ORDER_FULLY_EXECUTED)
        # Replayed → no longer in buffer
        self.assertFalse(buf.contains("X1"))
        self.assertEqual(len(buf), 1)
        self.assertEqual(buf.total_events(), 1)

    def test_replay_unknown_returns_empty(self):
        buf = OrphanEventBuffer()
        self.assertEqual(buf.replay("nonexistent"), [])

    def test_sweep_evicts_only_expired(self):
        clock = [100.0]
        buf = OrphanEventBuffer(ttl_seconds=30, now_fn=lambda: clock[0])
        clock[0] = 100.0
        buf.observe(_envelope_for("OLD"))
        clock[0] = 150.0
        buf.observe(_envelope_for("NEW"))
        # At now=160, OLD (first_seen=100) is > TTL=30 old; NEW (=150) isn't.
        clock[0] = 160.0
        evicted = buf.sweep()
        self.assertEqual(evicted, 1)
        self.assertFalse(buf.contains("OLD"))
        self.assertTrue(buf.contains("NEW"))

    def test_sweep_uses_explicit_now(self):
        clock = [100.0]
        buf = OrphanEventBuffer(ttl_seconds=30, now_fn=lambda: clock[0])
        buf.observe(_envelope_for("X"))
        # Far-future explicit now expires everything
        self.assertEqual(buf.sweep(now_ts=10_000), 1)

    def test_max_entries_drops_oldest(self):
        clock = [0.0]
        buf = OrphanEventBuffer(ttl_seconds=600, max_entries=3,
                                now_fn=lambda: clock[0])
        for i, xid in enumerate(("A", "B", "C", "D")):
            clock[0] = i  # ascending first_seen
            buf.observe(_envelope_for(xid))
        # Buffer was at cap (3) when D arrived → A (oldest) dropped.
        self.assertEqual(len(buf), 3)
        self.assertFalse(buf.contains("A"))
        for xid in ("B", "C", "D"):
            self.assertTrue(buf.contains(xid))

    def test_observe_without_order_is_noop(self):
        buf = OrphanEventBuffer()
        env = EventEnvelope(
            event_type=EventType.UNKNOWN,
            order=None,
            balance=None,
            recv_ts=0.0,
            event_ts=None,
        )
        buf.observe(env)
        self.assertEqual(len(buf), 0)

    def test_real_envelope_round_trip(self):
        """Make sure the buffer works with envelopes from the real parser
        (not just the hand-built helper)."""
        payload = {
            "message_cod": "BUY_ORDER_CREATED",
            "order_id": "999",
            "order": {
                "id": "999",
                "market": "BTC-BRL",
                "type": "BUY",
                "status": "EMPTY",
                "amount": 0.0002,
                "price": 100000,
                "time_stamp": "2026-05-15 11:00:00",
            },
            "balance": {
                "BTC": 0.001,
                "BRL": 500,
                "utimestamp": "2026-05-15 11:00:00.000001",
            },
        }
        env = parse_envelope(payload, recv_ts=100.0)
        self.assertIsNotNone(env)
        buf = OrphanEventBuffer()
        buf.observe(env)
        out = buf.replay("999")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].order.exchange_order_id, "999")


if __name__ == "__main__":
    unittest.main()
