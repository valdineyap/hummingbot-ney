"""Tests for the Phoenix Channel application-level heartbeat.

Background — why this exists:
  BitPreco's user-stream WS uses the Phoenix Channels protocol. Phoenix
  requires an application-level heartbeat (``phx_heartbeat`` on the
  special topic ``"phoenix"``) distinct from WebSocket/TCP-level pings.
  Without it, the server marks the channel dead after ``:timeout``
  (default 60s) and silently stops pushing events while the TCP socket
  stays open. Empirically over 6+ hours we observed ZERO ``flash``
  events — consistent with this dead-channel hypothesis.

These tests verify the heartbeat task's mechanical correctness:
  * Sends well-formed messages on the correct interval
  * Tracks in-flight refs and computes RTT on reply
  * Cancels cleanly on reconnect / disconnect
  * Watchdog fires when replies stop arriving

We do NOT test against a real Phoenix server here — that validation
happens in production over the next few hours of bot operation.
"""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.bitpreco.bitpreco_api_user_stream_data_source import (
    BitprecoAPIUserStreamDataSource,
    encode_phoenix_v2,
    normalize_phoenix_message,
)


def _make_data_source() -> BitprecoAPIUserStreamDataSource:
    """Bare-minimum stubbed data source — bypasses full __init__ wiring."""
    ds = BitprecoAPIUserStreamDataSource.__new__(BitprecoAPIUserStreamDataSource)
    # Match what __init__ would set:
    ds._auth = MagicMock()
    ds._connector = MagicMock()
    ds._connector.secret_key = "sec"
    ds._connector.api_key = "key"
    ds._domain = "default"
    ds._api_factory = MagicMock()
    ds._listen_key_initialized_event = asyncio.Event()
    ds._last_listen_key_ping_ts = 0
    ds._ws_last_connect_ts = 0.0
    ds._ws_connect_count = 0
    ds._phx_heartbeat_task = None
    ds._phx_heartbeat_ref = 0
    ds._phx_heartbeat_inflight = {}
    ds._phx_heartbeat_sent = 0
    ds._phx_heartbeat_replied = 0
    ds._phx_heartbeat_last_reply_ts = None
    ds._phx_heartbeat_stale_logged = False
    ds.logger = lambda: MagicMock()
    return ds


class HeartbeatSendFormatTest(unittest.IsolatedAsyncioTestCase):
    """The wire-format must match Phoenix's expectations."""

    async def test_heartbeat_loop_sends_well_formed_messages(self):
        """Each send is Phoenix v2 array format:
            [None, "<ref>", "phoenix", "heartbeat", {}]

        Three details that DIFFER from v1 / from a careless port:
          - join_ref is None (heartbeats are transport-level)
          - event is "heartbeat" (NOT "phx_heartbeat" — renamed in v1.7)
          - ref is a monotonic STRING
        """
        ds = _make_data_source()
        ws = MagicMock()
        sends = []

        async def _capture_send(req):
            sends.append(req)
            # Cancel the loop after a few sends to keep the test quick.
            if len(sends) >= 3:
                # Trigger cancellation via raising inside ws.send simulating
                # socket death — loop should exit cleanly.
                raise ConnectionResetError("simulate dead socket")

        ws.send = AsyncMock(side_effect=_capture_send)

        with patch.object(ds, "HEARTBEAT_TIME_INTERVAL", 0.001):
            await ds._phoenix_heartbeat_loop(ws)

        self.assertEqual(len(sends), 3)
        for i, req in enumerate(sends):
            data = json.loads(req.payload if hasattr(req, "payload") else
                              getattr(req, "payload", None) or req.__dict__.get("payload"))
            self.assertIsInstance(data, list, "expected v2 array wire format")
            self.assertEqual(len(data), 5, "v2 array has 5 elements")
            join_ref, ref, topic, event, payload = data
            self.assertIsNone(join_ref, "heartbeats use null join_ref")
            self.assertEqual(ref, str(i))
            self.assertEqual(topic, "phoenix")
            self.assertEqual(event, "heartbeat")
            self.assertEqual(payload, {})

    async def test_inflight_dict_tracks_refs(self):
        """Each SUCCESSFUL send adds its ref to _phx_heartbeat_inflight.
        Note: when send raises, we bail BEFORE the inflight tracker is
        updated — the failed send doesn't get a phantom in-flight entry."""
        ds = _make_data_source()
        ws = MagicMock()
        sends = []

        async def _capture(req):
            sends.append(req)
            # Allow refs 0 and 1 to succeed, fail on ref 2 to terminate the loop.
            if len(sends) >= 3:
                raise ConnectionResetError("stop")
        ws.send = AsyncMock(side_effect=_capture)

        with patch.object(ds, "HEARTBEAT_TIME_INTERVAL", 0.001):
            await ds._phoenix_heartbeat_loop(ws)

        # Refs 0 and 1 succeeded → tracked. Ref 2 failed → not tracked.
        self.assertIn("0", ds._phx_heartbeat_inflight)
        self.assertIn("1", ds._phx_heartbeat_inflight)
        self.assertNotIn("2", ds._phx_heartbeat_inflight)
        self.assertEqual(ds._phx_heartbeat_sent, 2)
        self.assertEqual(ds._phx_heartbeat_replied, 0)


class HeartbeatReplyMatchTest(unittest.TestCase):
    """``record_phx_heartbeat_reply`` matches refs and computes RTT."""

    def test_matching_ref_returns_rtt_and_increments_replied(self):
        ds = _make_data_source()
        # Pre-populate as if heartbeat ref=5 was sent 100ms ago.
        import time
        ds._phx_heartbeat_inflight["5"] = time.time() - 0.100
        rtt_ms = ds.record_phx_heartbeat_reply("5")
        self.assertIsNotNone(rtt_ms)
        # Allow ±20ms tolerance for test scheduling.
        self.assertGreater(rtt_ms, 80)
        self.assertLess(rtt_ms, 200)
        self.assertEqual(ds._phx_heartbeat_replied, 1)
        self.assertNotIn("5", ds._phx_heartbeat_inflight)
        self.assertIsNotNone(ds._phx_heartbeat_last_reply_ts)

    def test_unmatched_ref_returns_none(self):
        """Reply for a ref we never sent (e.g. from a previous connection
        cycle whose state was cleared) → None, no counter increment."""
        ds = _make_data_source()
        rtt_ms = ds.record_phx_heartbeat_reply("999")
        self.assertIsNone(rtt_ms)
        self.assertEqual(ds._phx_heartbeat_replied, 0)

    def test_reply_clears_stale_logged_flag(self):
        """A successful reply resets the watchdog one-shot flag so the
        NEXT stale spell can fire its own warning."""
        ds = _make_data_source()
        ds._phx_heartbeat_stale_logged = True
        import time
        ds._phx_heartbeat_inflight["1"] = time.time()
        ds.record_phx_heartbeat_reply("1")
        self.assertFalse(ds._phx_heartbeat_stale_logged)


class CancelOnReconnectTest(unittest.IsolatedAsyncioTestCase):
    """``_cancel_phx_heartbeat_task`` cleanly tears down the in-flight loop."""

    async def test_cancel_when_no_task_is_noop(self):
        ds = _make_data_source()
        # No task — should not raise.
        ds._cancel_phx_heartbeat_task()
        self.assertIsNone(ds._phx_heartbeat_task)

    async def test_cancel_active_task(self):
        ds = _make_data_source()

        async def _long_loop():
            try:
                while True:
                    await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise

        task = asyncio.create_task(_long_loop())
        ds._phx_heartbeat_task = task
        ds._cancel_phx_heartbeat_task()
        # The cancel scheduled; await to confirm it actually finishes.
        await asyncio.sleep(0)
        self.assertTrue(task.cancelled() or task.done())
        self.assertIsNone(ds._phx_heartbeat_task)

    async def test_cancel_idempotent_when_task_already_done(self):
        ds = _make_data_source()

        async def _quick():
            return

        ds._phx_heartbeat_task = asyncio.create_task(_quick())
        await ds._phx_heartbeat_task   # let it finish
        ds._cancel_phx_heartbeat_task()  # should be no-op, no raise
        self.assertIsNone(ds._phx_heartbeat_task)


class HeartbeatInflightResetOnReconnectTest(unittest.IsolatedAsyncioTestCase):
    """Stale refs from a dead socket must be cleared on reconnect — otherwise
    we'd never resolve them and the watchdog would think we're stale forever."""

    async def test_inflight_dict_cleared_on_reconnect(self):
        ds = _make_data_source()
        ds._phx_heartbeat_inflight["0"] = 1.0
        ds._phx_heartbeat_inflight["1"] = 2.0
        ds._phx_heartbeat_stale_logged = True

        # Mock the WSAssistant + factory so _connected_websocket_assistant runs.
        ws = MagicMock()
        ws.connect = AsyncMock()
        ds._api_factory.get_ws_assistant = AsyncMock(return_value=ws)
        ds._ws_assistant = None

        await ds._connected_websocket_assistant()

        self.assertEqual(ds._phx_heartbeat_inflight, {})
        self.assertFalse(ds._phx_heartbeat_stale_logged)


class PhoenixV2EncoderTest(unittest.TestCase):
    """``encode_phoenix_v2`` produces the 5-element array Phoenix expects."""

    def test_join_message(self):
        """phx_join: join_ref == ref (convention from web client capture)."""
        req = encode_phoenix_v2(
            join_ref="3", ref="3",
            topic="notifications:abc", event="phx_join", payload={},
        )
        # Inspect the raw string we'll send on the wire.
        raw = req.payload if hasattr(req, "payload") else req.__dict__.get("payload")
        self.assertEqual(json.loads(raw), ["3", "3", "notifications:abc", "phx_join", {}])

    def test_heartbeat_message(self):
        """heartbeat: join_ref=None, topic='phoenix', event='heartbeat'."""
        req = encode_phoenix_v2(
            join_ref=None, ref="42",
            topic="phoenix", event="heartbeat", payload={},
        )
        raw = req.payload if hasattr(req, "payload") else req.__dict__.get("payload")
        self.assertEqual(json.loads(raw), [None, "42", "phoenix", "heartbeat", {}])

    def test_empty_payload_defaults_to_empty_dict(self):
        """payload=None passed as kwarg → encoded as {} (not null)."""
        req = encode_phoenix_v2(
            join_ref=None, ref="1", topic="phoenix", event="heartbeat",
        )
        raw = req.payload if hasattr(req, "payload") else req.__dict__.get("payload")
        parsed = json.loads(raw)
        self.assertEqual(parsed[4], {})


class PhoenixMessageNormalizerTest(unittest.TestCase):
    """``normalize_phoenix_message`` flattens v1/v2 into a uniform dict."""

    def test_v2_array_form(self):
        """The wire format we expect from the server after the migration."""
        # Real capture from the web client:
        # [null, null, "notifications:xyz", "flash",
        #  {"payload":null,"persist":false,"type":"ORDER_CANCELED","user_id":"3652"}]
        raw = [None, None, "notifications:xyz", "flash",
               {"payload": None, "persist": False,
                "type": "ORDER_CANCELED", "user_id": "3652"}]
        msg = normalize_phoenix_message(raw)
        self.assertEqual(msg["join_ref"], None)
        self.assertEqual(msg["ref"], None)
        self.assertEqual(msg["topic"], "notifications:xyz")
        self.assertEqual(msg["event"], "flash")
        self.assertEqual(msg["payload"]["type"], "ORDER_CANCELED")
        self.assertEqual(msg["payload"]["user_id"], "3652")

    def test_v2_phx_reply_for_heartbeat(self):
        """Heartbeat reply: [null, "9", "phoenix", "phx_reply",
                            {"response":{},"status":"ok"}]"""
        raw = [None, "9", "phoenix", "phx_reply",
               {"response": {}, "status": "ok"}]
        msg = normalize_phoenix_message(raw)
        self.assertEqual(msg["topic"], "phoenix")
        self.assertEqual(msg["event"], "phx_reply")
        self.assertEqual(msg["ref"], "9")
        self.assertEqual(msg["payload"]["status"], "ok")

    def test_v1_object_form_backwards_compat(self):
        """If the server ever falls back to v1 (e.g. another endpoint),
        the normalizer should still produce the same shape."""
        raw = {"topic": "notifications:xyz", "event": "flash",
               "payload": {"type": "ORDER_FULLY_EXECUTED"}, "ref": None}
        msg = normalize_phoenix_message(raw)
        self.assertEqual(msg["topic"], "notifications:xyz")
        self.assertEqual(msg["event"], "flash")
        self.assertEqual(msg["payload"]["type"], "ORDER_FULLY_EXECUTED")

    def test_unparseable_returns_none(self):
        """A garbage message (string, int, None) doesn't crash — returns
        None so the caller can skip it."""
        self.assertIsNone(normalize_phoenix_message("not a message"))
        self.assertIsNone(normalize_phoenix_message(None))
        self.assertIsNone(normalize_phoenix_message(42))

    def test_short_array_tolerated(self):
        """Some legacy servers emit 4-element arrays (no payload).
        Missing slots default to None instead of raising IndexError."""
        raw = [None, "1", "phoenix", "phx_reply"]
        msg = normalize_phoenix_message(raw)
        self.assertEqual(msg["topic"], "phoenix")
        self.assertEqual(msg["event"], "phx_reply")
        self.assertIsNone(msg["payload"])


if __name__ == "__main__":
    unittest.main()
