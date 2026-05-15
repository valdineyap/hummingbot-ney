"""Tests for the ``_get_poll_interval`` override.

BitPreco's Phoenix WS doesn't push trade events to API users (confirmed
empirically across multiple sessions: 6h51m → 2 flashes vs 500+ order
events). The parent class would use WS message activity to back off REST
polling (``last_user_stream_message_time`` → LONG_POLL_INTERVAL). Our
override decouples poll cadence from WS state — REST poll is the sole
source of truth.

These tests pin down that the connector ALWAYS returns
``SHORT_POLL_INTERVAL``, regardless of how recently the WS received a
message (heartbeat replies, the rare flash, etc).
"""

import time
import unittest
from unittest.mock import MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


def _make_exchange() -> BitprecoExchange:
    """Bare-minimum stubbed connector — bypasses full __init__."""
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    return ex


class PollIntervalIsConstantTest(unittest.TestCase):
    """Poll interval must be ``SHORT_POLL_INTERVAL`` independent of WS."""

    def test_with_no_user_stream_tracker(self):
        """Parent class branches on ``self._user_stream_tracker`` being
        present. Our override must work even when it's None."""
        ex = _make_exchange()
        ex._user_stream_tracker = None
        self.assertEqual(ex._get_poll_interval(time.time()), BitprecoExchange.SHORT_POLL_INTERVAL)

    def test_with_recent_ws_message(self):
        """Parent class would return LONG_POLL when a WS message arrived
        recently (within TICK_INTERVAL_LIMIT seconds). Our override must
        ignore that and stay on SHORT_POLL."""
        ex = _make_exchange()
        tracker = MagicMock()
        tracker.last_recv_time = time.time()   # "just received a message"
        ex._user_stream_tracker = tracker
        self.assertEqual(ex._get_poll_interval(time.time()), BitprecoExchange.SHORT_POLL_INTERVAL)

    def test_with_stale_ws_message(self):
        """When WS is stale, parent would use SHORT_POLL anyway. Sanity:
        our override gives the same result, so behaviour doesn't flip."""
        ex = _make_exchange()
        tracker = MagicMock()
        tracker.last_recv_time = 0   # ancient
        ex._user_stream_tracker = tracker
        self.assertEqual(ex._get_poll_interval(time.time()), BitprecoExchange.SHORT_POLL_INTERVAL)

    def test_long_poll_value_never_returned(self):
        """Defensive: regardless of how the test exercises the function,
        we never want LONG_POLL_INTERVAL coming out. If someone later
        deletes the override or breaks it, this test fails loud."""
        ex = _make_exchange()
        ex._user_stream_tracker = None
        for ts in [0, time.time(), time.time() + 100, 1e10]:
            result = ex._get_poll_interval(ts)
            self.assertNotEqual(
                result, BitprecoExchange.LONG_POLL_INTERVAL,
                f"poll interval at ts={ts} unexpectedly returned LONG_POLL",
            )
            self.assertEqual(result, BitprecoExchange.SHORT_POLL_INTERVAL)


if __name__ == "__main__":
    unittest.main()
