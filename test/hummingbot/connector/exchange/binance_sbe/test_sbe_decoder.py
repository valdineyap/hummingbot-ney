"""Unit tests for ``sbe_decoder``.

These tests build SBE frames byte-by-byte using ``struct.pack`` so each
golden fixture is fully reproducible from the test source — no captured
binary blobs to bit-rot. The frame layouts mirror ``stream_1_0.xml``
exactly; if the schema bumps and breaks this assumption, the live
integration test ``test_live_schema_version_matches`` catches it.
"""
from __future__ import annotations

import struct
import unittest

from hummingbot.connector.exchange.binance_sbe import (
    binance_sbe_constants as CONSTANTS,
    sbe_decoder,
)
from hummingbot.connector.exchange.binance_sbe.sbe_decoder import (
    SbeSchemaMismatchError,
    SbeTruncatedFrameError,
    _mantissa_to_decimal_str,
    decode_frame,
)


# ---------------------------------------------------------------------------
# Frame builders — keep these aligned with stream_1_0.xml.
# ---------------------------------------------------------------------------


def _header(template_id: int, block_length: int,
            schema_id: int = CONSTANTS.SBE_SCHEMA_ID,
            version: int = CONSTANTS.SBE_SCHEMA_VERSION) -> bytes:
    return struct.pack("<HHHH", block_length, template_id, schema_id, version)


def _var_string8(s: str) -> bytes:
    encoded = s.encode("utf-8")
    return struct.pack("<B", len(encoded)) + encoded


def _build_trades_frame(*,
                        trades,                          # list[dict] with id/price_mantissa/qty_mantissa/is_buyer_maker
                        event_time_us: int = 1_700_000_000_000_000,
                        transact_time_us: int = 1_700_000_000_000_500,
                        price_exponent: int = -8,
                        qty_exponent: int = -8,
                        symbol: str = "BTCUSDT",
                        # Versioning knobs for compat tests:
                        wire_block_length: int = 18,
                        wire_trade_block_length: int = 25,
                        schema_version: int = CONSTANTS.SBE_SCHEMA_VERSION,
                        schema_id: int = CONSTANTS.SBE_SCHEMA_ID,
                        extra_root_bytes: bytes = b"",
                        extra_trade_bytes: bytes = b"") -> bytes:
    """Construct a TradesStreamEvent SBE frame.

    ``wire_block_length`` and ``wire_trade_block_length`` let tests
    exercise version compatibility paths (additive growth vs truncation).
    """
    header = _header(CONSTANTS.SBE_TEMPLATE_ID_TRADES,
                     block_length=wire_block_length,
                     schema_id=schema_id, version=schema_version)
    root = struct.pack("<qqbb",
                       event_time_us, transact_time_us,
                       price_exponent, qty_exponent) + extra_root_bytes
    # Group header for `trades`: default groupSizeEncoding (uint16 blockLength + uint32 numInGroup)
    group_hdr = struct.pack("<HI", wire_trade_block_length, len(trades))

    trade_bytes = b""
    for t in trades:
        trade_bytes += struct.pack("<qqqB",
                                   t["id"],
                                   t["price_mantissa"],
                                   t["qty_mantissa"],
                                   1 if t["is_buyer_maker"] else 0) + extra_trade_bytes

    return header + root + group_hdr + trade_bytes + _var_string8(symbol)


def _build_depth_diff_frame(*,
                            bids,                         # list[(price_mantissa, qty_mantissa)]
                            asks,                         # idem
                            event_time_us: int = 1_700_000_000_000_000,
                            first_update_id: int = 100,
                            last_update_id: int = 110,
                            price_exponent: int = -8,
                            qty_exponent: int = -8,
                            symbol: str = "BTCUSDT",
                            wire_block_length: int = 26,
                            wire_level_block_length: int = 16,
                            schema_version: int = CONSTANTS.SBE_SCHEMA_VERSION,
                            schema_id: int = CONSTANTS.SBE_SCHEMA_ID,
                            extra_root_bytes: bytes = b"") -> bytes:
    header = _header(CONSTANTS.SBE_TEMPLATE_ID_DEPTH_DIFF,
                     block_length=wire_block_length,
                     schema_id=schema_id, version=schema_version)
    root = struct.pack("<qqqbb",
                       event_time_us, first_update_id, last_update_id,
                       price_exponent, qty_exponent) + extra_root_bytes

    def _level_group(levels):
        # groupSize16Encoding: uint16 blockLength + uint16 numInGroup
        hdr = struct.pack("<HH", wire_level_block_length, len(levels))
        body = b"".join(struct.pack("<qq", p, q) for p, q in levels)
        return hdr + body

    return header + root + _level_group(bids) + _level_group(asks) + _var_string8(symbol)


# ---------------------------------------------------------------------------
# Decimal helper
# ---------------------------------------------------------------------------


class MantissaDecimalTest(unittest.TestCase):
    """The helper that converts (mantissa, exponent) to an exact string is
    in the hot path — pin every interesting case here."""

    def test_basic_negative_exponent(self):
        self.assertEqual(_mantissa_to_decimal_str(39550000000, -8), "395.50000000")

    def test_value_below_one(self):
        self.assertEqual(_mantissa_to_decimal_str(1, -8), "0.00000001")

    def test_zero(self):
        # 0.00000000 — preserve the same 8-decimal width as the JSON stream.
        self.assertEqual(_mantissa_to_decimal_str(0, -8), "0.00000000")

    def test_negative_mantissa(self):
        self.assertEqual(_mantissa_to_decimal_str(-5, -2), "-0.05")

    def test_exponent_zero(self):
        self.assertEqual(_mantissa_to_decimal_str(100, 0), "100")

    def test_positive_exponent(self):
        # mantissa=5, exponent=2 → 500
        self.assertEqual(_mantissa_to_decimal_str(5, 2), "500")

    def test_dynamic_exponent_used_not_hardcoded(self):
        # If the schema reports priceExponent=-2 we must not assume -8.
        self.assertEqual(_mantissa_to_decimal_str(12345, -2), "123.45")
        self.assertEqual(_mantissa_to_decimal_str(12345, -4), "1.2345")
        self.assertEqual(_mantissa_to_decimal_str(12345, -6), "0.012345")


# ---------------------------------------------------------------------------
# Trade decode tests
# ---------------------------------------------------------------------------


class TradesDecodeTest(unittest.TestCase):

    def setUp(self):
        # Avoid leaking warned-template state from earlier tests so the
        # "unknown template warns once" test has a clean baseline.
        sbe_decoder._warned_unknown_templates.clear()

    def test_decode_single_trade_golden(self):
        frame = _build_trades_frame(trades=[
            {"id": 555, "price_mantissa": 39550000000, "qty_mantissa": 12345000,
             "is_buyer_maker": False},
        ])
        events = decode_frame(frame)
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev["e"], "trade")
        self.assertEqual(ev["s"], "BTCUSDT")
        self.assertEqual(ev["t"], 555)
        self.assertEqual(ev["p"], "395.50000000")
        self.assertEqual(ev["q"], "0.12345000")
        self.assertFalse(ev["m"])

    def test_decode_multi_trade_critical(self):
        """Frames with N>1 trades must surface ALL trades — historically a
        common decoder bug is keeping only the first."""
        trades = [
            {"id": 1001, "price_mantissa": 39500000000, "qty_mantissa": 100000,
             "is_buyer_maker": True},
            {"id": 1002, "price_mantissa": 39510000000, "qty_mantissa": 200000,
             "is_buyer_maker": False},
            {"id": 1003, "price_mantissa": 39520000000, "qty_mantissa": 300000,
             "is_buyer_maker": True},
        ]
        events = decode_frame(_build_trades_frame(trades=trades))
        self.assertEqual(len(events), 3)
        self.assertEqual([e["t"] for e in events], [1001, 1002, 1003])
        # Each must carry the same symbol (set from the post-group varString).
        for ev in events:
            self.assertEqual(ev["s"], "BTCUSDT")
        # is_buyer_maker preserved per element (not reused from the last one).
        self.assertEqual([e["m"] for e in events], [True, False, True])

    def test_decode_trade_timestamp_us_to_ms(self):
        """SBE delivers eventTime in microseconds; BinanceOrderBook does
        ``ts * 1e-3`` assuming millis — decoder MUST divide."""
        event_us = 1_700_000_000_123_456  # arbitrary microsecond timestamp
        frame = _build_trades_frame(
            event_time_us=event_us,
            trades=[{"id": 1, "price_mantissa": 1, "qty_mantissa": 1,
                     "is_buyer_maker": False}],
        )
        ev = decode_frame(frame)[0]
        self.assertEqual(ev["E"], event_us // 1000)         # 1_700_000_000_123
        self.assertEqual(ev["E_us"], event_us)              # micros preserved

    def test_decode_dynamic_exponent_per_frame(self):
        """priceExponent/qtyExponent come from the frame — must not be
        hardcoded to -8."""
        frame = _build_trades_frame(
            price_exponent=-2, qty_exponent=-4,
            trades=[{"id": 1, "price_mantissa": 12345, "qty_mantissa": 67890,
                     "is_buyer_maker": False}],
        )
        ev = decode_frame(frame)[0]
        self.assertEqual(ev["p"], "123.45")     # 12345 * 10^-2
        self.assertEqual(ev["q"], "6.7890")     # 67890 * 10^-4

    def test_trades_with_zero_trades(self):
        """Empty repeating group is a valid edge case (heartbeat-like)."""
        events = decode_frame(_build_trades_frame(trades=[]))
        self.assertEqual(events, [])


# ---------------------------------------------------------------------------
# Depth diff decode tests
# ---------------------------------------------------------------------------


class DepthDiffDecodeTest(unittest.TestCase):

    def test_decode_zero_levels(self):
        frame = _build_depth_diff_frame(bids=[], asks=[])
        events = decode_frame(frame)
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev["e"], "depthUpdate")
        self.assertEqual(ev["U"], 100)
        self.assertEqual(ev["u"], 110)
        self.assertEqual(ev["b"], [])
        self.assertEqual(ev["a"], [])
        self.assertEqual(ev["s"], "BTCUSDT")

    def test_decode_one_level_each_side(self):
        frame = _build_depth_diff_frame(
            bids=[(39550000000, 12300000)],
            asks=[(39560000000, 45600000)],
        )
        ev = decode_frame(frame)[0]
        self.assertEqual(ev["b"], [["395.50000000", "0.12300000"]])
        self.assertEqual(ev["a"], [["395.60000000", "0.45600000"]])

    def test_decode_many_levels_offset_alignment(self):
        """The bug that ``groupSize16Encoding`` vs ``groupSizeEncoding`` is
        meant to prevent: misaligned offsets after the first group corrupt
        every subsequent read. Many levels makes any drift visible."""
        bids = [(39500000000 - i * 100000, 100000 + i * 1000) for i in range(12)]
        asks = [(39600000000 + i * 100000, 200000 + i * 1000) for i in range(15)]
        ev = decode_frame(_build_depth_diff_frame(bids=bids, asks=asks))[0]

        self.assertEqual(len(ev["b"]), 12)
        self.assertEqual(len(ev["a"]), 15)
        # First and last entries must be exactly what we encoded.
        # bid[i]: price=39500000000-i*100000, qty=100000+i*1000
        # bid[11]: price=39498900000 → "394.98900000"; qty=111000 → "0.00111000"
        self.assertEqual(ev["b"][0], ["395.00000000", "0.00100000"])
        self.assertEqual(ev["b"][-1], ["394.98900000", "0.00111000"])
        # ask[i]: price=39600000000+i*100000, qty=200000+i*1000
        # ask[14]: price=39601400000 → "396.01400000"; qty=214000 → "0.00214000"
        self.assertEqual(ev["a"][0], ["396.00000000", "0.00200000"])
        self.assertEqual(ev["a"][-1], ["396.01400000", "0.00214000"])

    def test_decode_depth_timestamp_us_to_ms(self):
        event_us = 1_700_000_000_987_654
        frame = _build_depth_diff_frame(event_time_us=event_us, bids=[], asks=[])
        ev = decode_frame(frame)[0]
        self.assertEqual(ev["E"], event_us // 1000)
        self.assertEqual(ev["E_us"], event_us)


# ---------------------------------------------------------------------------
# Schema-version compatibility
# ---------------------------------------------------------------------------


class SchemaCompatibilityTest(unittest.TestCase):

    def test_schema_id_mismatch_raises(self):
        frame = _build_trades_frame(
            trades=[{"id": 1, "price_mantissa": 1, "qty_mantissa": 1,
                     "is_buyer_maker": False}],
            schema_id=CONSTANTS.SBE_SCHEMA_ID + 7,
        )
        with self.assertRaises(SbeSchemaMismatchError):
            decode_frame(frame)

    def test_additive_version_bump_accepted(self):
        """Sender bumped version + added 4 trailing bytes to root and 2 to
        each trade. blockLength reflects the new sizes. Decoder must still
        produce the known fields correctly."""
        frame = _build_trades_frame(
            trades=[{"id": 42, "price_mantissa": 39550000000,
                     "qty_mantissa": 100000, "is_buyer_maker": True}],
            schema_version=CONSTANTS.SBE_SCHEMA_VERSION + 1,
            wire_block_length=18 + 4,
            extra_root_bytes=b"\x00\x00\x00\x00",
            wire_trade_block_length=25 + 2,
            extra_trade_bytes=b"\x00\x00",
        )
        events = decode_frame(frame)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["t"], 42)
        self.assertEqual(events[0]["p"], "395.50000000")

    def test_non_additive_version_bump_fail_stops(self):
        """version higher AND wire blockLength SMALLER than our minimum
        means fields we depend on were removed/renamed — fail-stop."""
        # We have to hand-build a too-short frame; the helper assumes
        # blockLength matches. Just construct manually.
        header = _header(CONSTANTS.SBE_TEMPLATE_ID_TRADES,
                         block_length=10,  # < required 18
                         version=CONSTANTS.SBE_SCHEMA_VERSION + 1)
        body = b"\x00" * 10 + struct.pack("<HI", 25, 0) + _var_string8("X")
        with self.assertRaises(SbeSchemaMismatchError):
            decode_frame(header + body)


# ---------------------------------------------------------------------------
# Unknown templates + malformed buffers
# ---------------------------------------------------------------------------


class UnknownAndMalformedTest(unittest.TestCase):

    def setUp(self):
        sbe_decoder._warned_unknown_templates.clear()

    def test_unknown_template_returns_empty_list_and_warns_once(self):
        # 10001 (BestBidAsk) and 10002 (DepthSnapshot) are not handled in
        # Phase 1 — they must be ignored, not raise.
        header_1 = _header(CONSTANTS.SBE_TEMPLATE_ID_BEST_BID_ASK, block_length=42)
        header_2 = _header(CONSTANTS.SBE_TEMPLATE_ID_DEPTH_SNAPSHOT, block_length=42)
        # Decoder doesn't try to parse unknown body, so trailing bytes are fine.
        body = b"\x00" * 60

        self.assertEqual(decode_frame(header_1 + body), [])
        self.assertEqual(decode_frame(header_2 + body), [])
        # Repeated call shouldn't add another entry to the warn-once set,
        # but it remains in the set.
        self.assertEqual(decode_frame(header_1 + body), [])
        self.assertIn(CONSTANTS.SBE_TEMPLATE_ID_BEST_BID_ASK,
                      sbe_decoder._warned_unknown_templates)
        self.assertIn(CONSTANTS.SBE_TEMPLATE_ID_DEPTH_SNAPSHOT,
                      sbe_decoder._warned_unknown_templates)

    def test_unknown_template_with_bad_schema_id_still_raises(self):
        """Even for templates we don't handle, a schemaId mismatch must
        fail loudly — silently dropping frames after a schema replacement
        would be invisible data loss."""
        # Pick an unknown template id within uint16 range (max 65535).
        unknown_template_id = 60000
        header = _header(unknown_template_id, block_length=42,
                         schema_id=CONSTANTS.SBE_SCHEMA_ID + 1)
        with self.assertRaises(SbeSchemaMismatchError):
            decode_frame(header + b"\x00" * 60)

    def test_truncated_header_raises(self):
        with self.assertRaises(SbeTruncatedFrameError):
            decode_frame(b"\x00\x00\x00")  # 3 bytes — header is 8

    def test_truncated_trade_body_raises(self):
        # Build a valid-looking frame but lop off the symbol's bytes.
        frame = _build_trades_frame(
            trades=[{"id": 1, "price_mantissa": 1, "qty_mantissa": 1,
                     "is_buyer_maker": False}],
            symbol="ABCDEFG",
        )
        with self.assertRaises(SbeTruncatedFrameError):
            decode_frame(frame[:-3])  # last 3 bytes of symbol missing


if __name__ == "__main__":
    unittest.main()
