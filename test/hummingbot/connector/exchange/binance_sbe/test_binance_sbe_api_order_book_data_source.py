"""Unit tests for ``BinanceSbeAPIOrderBookDataSource``.

These focus on the SBE-specific overrides: binary-frame dispatch, JSON
ack handling, stream-name overrides (``@depth`` not ``@depth@100ms``),
the ``X-MBX-APIKEY`` header, and the proactive-reconnect TTL guard.
The base ``BinanceAPIOrderBookDataSource`` already has its own coverage
upstream — we don't re-test inherited behaviour.
"""
from __future__ import annotations

import asyncio
import struct
import time
import unittest
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.binance_sbe import binance_sbe_constants as CONSTANTS
from hummingbot.connector.exchange.binance_sbe.binance_sbe_api_order_book_data_source import (
    BinanceSbeAPIOrderBookDataSource,
)
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest


# ---------------------------------------------------------------------------
# Builders — minimal SBE frames sufficient to exercise the dispatch path.
# Reuse the same struct layouts as test_sbe_decoder; kept inline so this
# file is self-contained.
# ---------------------------------------------------------------------------


def _header(template_id: int, block_length: int) -> bytes:
    return struct.pack("<HHHH", block_length, template_id,
                       CONSTANTS.SBE_SCHEMA_ID, CONSTANTS.SBE_SCHEMA_VERSION)


def _build_trade_frame(trade_id: int = 1, symbol: str = "BTCUSDT") -> bytes:
    """Build a minimal trade frame (1 trade)."""
    header = _header(CONSTANTS.SBE_TEMPLATE_ID_TRADES, block_length=18)
    root = struct.pack("<qqbb", 1_700_000_000_000_000, 1_700_000_000_000_000, -8, -8)
    group_hdr = struct.pack("<HI", 25, 1)  # blockLength=25, numInGroup=1
    trade = struct.pack("<qqqB", trade_id, 39550000000, 100000, 0)
    sym = bytes([len(symbol)]) + symbol.encode("utf-8")
    return header + root + group_hdr + trade + sym


def _build_multi_trade_frame(n: int, symbol: str = "BTCUSDT") -> bytes:
    header = _header(CONSTANTS.SBE_TEMPLATE_ID_TRADES, block_length=18)
    root = struct.pack("<qqbb", 1_700_000_000_000_000, 1_700_000_000_000_000, -8, -8)
    group_hdr = struct.pack("<HI", 25, n)
    trades_bytes = b"".join(
        struct.pack("<qqqB", 1000 + i, 39550000000, 100000, 0) for i in range(n)
    )
    sym = bytes([len(symbol)]) + symbol.encode("utf-8")
    return header + root + group_hdr + trades_bytes + sym


def _build_depth_frame(symbol: str = "BTCUSDT") -> bytes:
    """Minimal depth diff with 1 bid + 1 ask."""
    header = _header(CONSTANTS.SBE_TEMPLATE_ID_DEPTH_DIFF, block_length=26)
    root = struct.pack("<qqqbb", 1_700_000_000_000_000, 100, 110, -8, -8)
    bids_hdr = struct.pack("<HH", 16, 1)
    bid = struct.pack("<qq", 39550000000, 100000)
    asks_hdr = struct.pack("<HH", 16, 1)
    ask = struct.pack("<qq", 39560000000, 200000)
    sym = bytes([len(symbol)]) + symbol.encode("utf-8")
    return header + root + bids_hdr + bid + asks_hdr + ask + sym


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ws_response(data) -> MagicMock:
    """Mock a ``WSResponse`` carrying ``data`` (bytes or dict)."""
    resp = MagicMock()
    resp.data = data
    return resp


def _make_ws_assistant_with_messages(messages) -> MagicMock:
    """Return a MagicMock WSAssistant whose ``iter_messages`` yields the
    provided messages and then stops."""
    ws = MagicMock()

    async def _iter():
        for m in messages:
            yield m

    ws.iter_messages = _iter
    ws.send = AsyncMock()
    ws.connect = AsyncMock()
    ws.disconnect = AsyncMock()
    return ws


def _make_data_source(trading_pairs=None, sbe_api_key="dummy-key") -> BinanceSbeAPIOrderBookDataSource:
    """Construct a data source with mock connector + factory."""
    trading_pairs = trading_pairs or ["BTC-USDT"]
    connector = MagicMock()

    async def _resolve(trading_pair):
        # "BTC-USDT" -> "BTCUSDT" for our tests
        return trading_pair.replace("-", "")
    connector.exchange_symbol_associated_to_pair = _resolve

    async def _resolve_pair(symbol):
        # "BTCUSDT" -> "BTC-USDT"
        return f"{symbol[:3]}-{symbol[3:]}"
    connector.trading_pair_associated_to_exchange_symbol = _resolve_pair

    api_factory = MagicMock()
    return BinanceSbeAPIOrderBookDataSource(
        trading_pairs=trading_pairs,
        connector=connector,
        api_factory=api_factory,
        sbe_api_key=sbe_api_key,
    )


# ---------------------------------------------------------------------------
# Stream-name overrides
# ---------------------------------------------------------------------------


class StreamNameOverridesTest(unittest.TestCase):

    def test_depth_stream_omits_100ms_suffix(self):
        # The SBE depth stream is 25ms by design; the JSON connector's
        # @depth@100ms would be silently wrong on the SBE host.
        self.assertEqual(BinanceSbeAPIOrderBookDataSource._depth_stream("BTCUSDT"),
                         "btcusdt@depth")
        self.assertNotIn("@100ms",
                         BinanceSbeAPIOrderBookDataSource._depth_stream("BTCUSDT"))

    def test_trade_stream_unchanged_from_json(self):
        self.assertEqual(BinanceSbeAPIOrderBookDataSource._trade_stream("BTCUSDT"),
                         "btcusdt@trade")


# ---------------------------------------------------------------------------
# Subscribe / unsubscribe payloads
# ---------------------------------------------------------------------------


class DynamicSubscribeUnsubscribeTest(IsolatedAsyncioWrapperTestCase):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.ds = _make_data_source()
        # Pretend a WS is connected so subscribe_to_trading_pair proceeds.
        self.ds._ws_assistant = MagicMock()
        self.ds._ws_assistant.send = AsyncMock()

    def _sent_payloads(self):
        return [call.args[0].payload for call in self.ds._ws_assistant.send.call_args_list]

    async def test_subscribe_uses_at_depth_not_at_depth_100ms(self):
        ok = await self.ds.subscribe_to_trading_pair("ETH-USDT")
        self.assertTrue(ok)
        params_seen = [p for payload in self._sent_payloads() for p in payload["params"]]
        self.assertIn("ethusdt@trade", params_seen)
        self.assertIn("ethusdt@depth", params_seen)
        for p in params_seen:
            self.assertNotIn("@100ms", p,
                             f"unexpected @100ms suffix on stream {p!r}")

    async def test_unsubscribe_uses_at_depth_not_at_depth_100ms(self):
        ok = await self.ds.unsubscribe_from_trading_pair("ETH-USDT")
        self.assertTrue(ok)
        params_seen = [p for payload in self._sent_payloads() for p in payload["params"]]
        self.assertIn("ethusdt@trade", params_seen)
        self.assertIn("ethusdt@depth", params_seen)
        for p in params_seen:
            self.assertNotIn("@100ms", p)

    async def test_subscribe_channels_initial_uses_at_depth(self):
        # The initial bulk subscribe path (called once at session start).
        ws_mock = MagicMock()
        ws_mock.send = AsyncMock()
        await self.ds._subscribe_channels(ws_mock)
        payloads = [call.args[0].payload for call in ws_mock.send.call_args_list]
        all_params = [p for pl in payloads for p in pl["params"]]
        self.assertIn("btcusdt@depth", all_params)
        self.assertIn("btcusdt@trade", all_params)
        for p in all_params:
            self.assertNotIn("@100ms", p)


# ---------------------------------------------------------------------------
# Connection + auth header
# ---------------------------------------------------------------------------


class ConnectAndAuthTest(IsolatedAsyncioWrapperTestCase):

    async def test_connect_uses_sbe_url_and_apikey_header(self):
        ds = _make_data_source(sbe_api_key="MY-ED25519-API-KEY-STRING")
        ws_mock = MagicMock()
        ws_mock.connect = AsyncMock()
        ds._api_factory.get_ws_assistant = AsyncMock(return_value=ws_mock)

        await ds._connected_websocket_assistant()

        ws_mock.connect.assert_awaited_once()
        kwargs = ws_mock.connect.await_args.kwargs
        self.assertEqual(kwargs["ws_url"], CONSTANTS.WSS_SBE_URL)
        self.assertEqual(kwargs["ws_headers"]["X-MBX-APIKEY"], "MY-ED25519-API-KEY-STRING")
        # Connect monotonic timestamp must be set so reconnect TTL works.
        self.assertIsNotNone(ds._connect_monotonic)

    async def test_connect_with_empty_key_raises_value_error(self):
        ds = _make_data_source(sbe_api_key="")
        ds._api_factory.get_ws_assistant = AsyncMock(return_value=MagicMock())
        with self.assertRaises(ValueError):
            await ds._connected_websocket_assistant()


# ---------------------------------------------------------------------------
# Message processing — binary frames dispatched, JSON acks logged
# ---------------------------------------------------------------------------


class WebsocketMessagesTest(IsolatedAsyncioWrapperTestCase):

    async def test_binary_trade_frame_enqueued_to_trade_queue(self):
        ds = _make_data_source()
        ws = _make_ws_assistant_with_messages([_make_ws_response(_build_trade_frame(trade_id=42))])
        await ds._process_websocket_messages(ws)
        trade_q = ds._message_queue[ds._trade_messages_queue_key]
        self.assertEqual(trade_q.qsize(), 1)
        msg = trade_q.get_nowait()
        self.assertEqual(msg["e"], "trade")
        self.assertEqual(msg["t"], 42)

    async def test_multi_trade_frame_enqueues_all_trades(self):
        """The classic decoder/dispatch bug: only the first trade in a
        multi-trade frame reaches the queue."""
        ds = _make_data_source()
        ws = _make_ws_assistant_with_messages([_make_ws_response(_build_multi_trade_frame(5))])
        await ds._process_websocket_messages(ws)
        trade_q = ds._message_queue[ds._trade_messages_queue_key]
        self.assertEqual(trade_q.qsize(), 5)
        ids = [trade_q.get_nowait()["t"] for _ in range(5)]
        self.assertEqual(ids, [1000, 1001, 1002, 1003, 1004])

    async def test_binary_depth_frame_enqueued_to_diff_queue(self):
        ds = _make_data_source()
        ws = _make_ws_assistant_with_messages([_make_ws_response(_build_depth_frame())])
        await ds._process_websocket_messages(ws)
        diff_q = ds._message_queue[ds._diff_messages_queue_key]
        self.assertEqual(diff_q.qsize(), 1)
        msg = diff_q.get_nowait()
        self.assertEqual(msg["e"], "depthUpdate")
        self.assertEqual(msg["U"], 100)
        self.assertEqual(msg["u"], 110)

    async def test_json_subscription_ack_does_not_enqueue(self):
        ds = _make_data_source()
        ws = _make_ws_assistant_with_messages([
            _make_ws_response({"id": 1, "result": None}),
        ])
        await ds._process_websocket_messages(ws)
        # All queues should be empty — the ack is logged and ignored.
        for k in ds._get_messages_queue_keys():
            self.assertEqual(ds._message_queue[k].qsize(), 0)

    async def test_mixed_frames_in_one_session(self):
        ds = _make_data_source()
        ws = _make_ws_assistant_with_messages([
            _make_ws_response({"id": 1, "result": None}),
            _make_ws_response(_build_trade_frame(trade_id=7)),
            _make_ws_response(_build_depth_frame()),
            _make_ws_response({"id": 2, "result": None}),
        ])
        await ds._process_websocket_messages(ws)
        self.assertEqual(ds._message_queue[ds._trade_messages_queue_key].qsize(), 1)
        self.assertEqual(ds._message_queue[ds._diff_messages_queue_key].qsize(), 1)

    async def test_malformed_frame_does_not_kill_stream(self):
        """A single broken frame should be logged and skipped — the next
        frame must still be processed."""
        ds = _make_data_source()
        ws = _make_ws_assistant_with_messages([
            _make_ws_response(b"\x00\x00"),  # too short for header
            _make_ws_response(_build_trade_frame(trade_id=99)),
        ])
        await ds._process_websocket_messages(ws)
        trade_q = ds._message_queue[ds._trade_messages_queue_key]
        self.assertEqual(trade_q.qsize(), 1)
        self.assertEqual(trade_q.get_nowait()["t"], 99)

    async def test_unexpected_payload_type_does_not_crash(self):
        ds = _make_data_source()
        ws = _make_ws_assistant_with_messages([_make_ws_response(12345)])  # neither bytes nor dict
        await ds._process_websocket_messages(ws)
        # No exception; nothing queued.
        for k in ds._get_messages_queue_keys():
            self.assertEqual(ds._message_queue[k].qsize(), 0)


# ---------------------------------------------------------------------------
# Proactive reconnect — must trigger before Binance's 24h hard cap.
# ---------------------------------------------------------------------------


class ProactiveReconnectTest(IsolatedAsyncioWrapperTestCase):

    async def test_reconnect_raises_after_ttl_elapsed(self):
        """Set TTL to ~0s and verify the first frame triggers a
        ConnectionError so the base loop reconnects."""
        ds = _make_data_source()
        # Pretend we connected far in the past.
        ds._connect_monotonic = time.monotonic() - (CONSTANTS.WS_RECONNECT_INTERVAL_SEC + 10)
        ws = _make_ws_assistant_with_messages([_make_ws_response(_build_trade_frame())])
        with self.assertRaises(ConnectionError):
            await ds._process_websocket_messages(ws)

    async def test_no_reconnect_well_within_ttl(self):
        """A freshly connected session should not raise."""
        ds = _make_data_source()
        ds._connect_monotonic = time.monotonic()  # just connected
        ws = _make_ws_assistant_with_messages([_make_ws_response(_build_trade_frame())])
        # Should NOT raise — well within TTL window.
        await ds._process_websocket_messages(ws)
        self.assertEqual(ds._message_queue[ds._trade_messages_queue_key].qsize(), 1)

    async def test_reconnect_clears_connect_monotonic_to_avoid_repeat_raise(self):
        """If the base loop's reconnect cycle re-enters before we set
        _connect_monotonic on the new WS, we shouldn't raise immediately
        again — which would loop. The flag is cleared on raise."""
        ds = _make_data_source()
        ds._connect_monotonic = time.monotonic() - (CONSTANTS.WS_RECONNECT_INTERVAL_SEC + 10)
        ws = _make_ws_assistant_with_messages([_make_ws_response(_build_trade_frame())])
        with self.assertRaises(ConnectionError):
            await ds._process_websocket_messages(ws)
        self.assertIsNone(ds._connect_monotonic)


# ---------------------------------------------------------------------------
# Inheritance smoke check — REST snapshot path is reused unchanged.
# ---------------------------------------------------------------------------


class InheritanceSmokeTest(unittest.TestCase):

    def test_inherits_request_order_book_snapshot(self):
        # Sanity: REST snapshot method is inherited from the JSON
        # connector. SBE doesn't provide a 1000-level snapshot, so this
        # MUST stay JSON-REST-based.
        from hummingbot.connector.exchange.binance.binance_api_order_book_data_source import (
            BinanceAPIOrderBookDataSource,
        )
        self.assertIs(
            BinanceSbeAPIOrderBookDataSource._request_order_book_snapshot,
            BinanceAPIOrderBookDataSource._request_order_book_snapshot,
        )

    def test_channel_originating_message_inherited(self):
        # The decoder produces {"e": "trade"} and {"e": "depthUpdate"},
        # which match exactly what the JSON-side _channel_originating_message
        # expects. So we deliberately inherit that method unchanged.
        from hummingbot.connector.exchange.binance.binance_api_order_book_data_source import (
            BinanceAPIOrderBookDataSource,
        )
        self.assertIs(
            BinanceSbeAPIOrderBookDataSource._channel_originating_message,
            BinanceAPIOrderBookDataSource._channel_originating_message,
        )


if __name__ == "__main__":
    unittest.main()
