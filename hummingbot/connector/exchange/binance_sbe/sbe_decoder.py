"""Pure decoder for Binance Spot SBE Market Data frames.

Decodes binary frames received on the SBE WebSocket stream into plain Python
dicts shaped identically to the JSON event payloads consumed by
``hummingbot.connector.exchange.binance.binance_order_book``. This lets the
existing ``BinanceOrderBook`` translation layer be reused verbatim.

Wire format reference: ``stream_1_0.xml`` (``schemaId=1, version=0``) at
https://github.com/binance/binance-spot-api-docs/tree/master/sbe/schemas

Implementation notes
====================

The decoder uses fixed-offset reads (``struct.unpack_from``) for the root
block and explicit walking over repeating groups. Two SBE group-header
encodings appear in this schema:

* ``groupSizeEncoding``    — uint16 ``blockLength`` + **uint32** ``numInGroup``
  used by: ``TradesStreamEvent.trades`` (default ``dimensionType``)
* ``groupSize16Encoding``  — uint16 ``blockLength`` + **uint16** ``numInGroup``
  used by: ``DepthDiffStreamEvent.bids/asks``, ``DepthSnapshotStreamEvent.bids/asks``

Confusing these two desynchronises *every* frame after the first group —
the decoder has separate helpers and the unit tests pin each path.

Schema-version compatibility
============================

The ``MessageHeader`` carries the sender's ``blockLength`` for the root block
and each repeating group carries its own ``blockLength``. We always advance
the cursor by the wire-reported block length, ignoring any trailing fields
the sender added in a newer schema version (additive change). This allows
Binance to extend the wire format without breaking our decoder, as long as
``schemaId`` does not change.

* ``schemaId`` mismatch          → ``SbeSchemaMismatchError`` (fail-stop).
* ``version > expected`` with    → log warning + parse normally (additive).
  wire blockLength >= our minimum
* ``version > expected`` with    → ``SbeSchemaMismatchError`` (non-additive).
  wire blockLength < our minimum

For unknown ``templateId`` we return ``[]`` with a one-shot warning per
template (Binance may add new message types — additive on the schema).

Output shape (matches what ``BinanceOrderBook`` expects)
========================================================

Trade::

    {"e": "trade",
     "E": event_time_ms,      # milliseconds — BinanceOrderBook
                              # trade_message_from_exchange does ts * 1e-3
     "E_us": event_time_us,   # full microsecond precision preserved
     "s": symbol,             # e.g. "BTCUSDT"
     "t": trade_id,           # int64
     "p": "395.50000000",     # exact-decimal string (NOT float)
     "q": "0.00012345",       # exact-decimal string
     "m": is_buyer_maker}     # bool

Depth diff::

    {"e": "depthUpdate",
     "E": event_time_ms,
     "E_us": event_time_us,
     "s": symbol,
     "U": first_book_update_id,
     "u": last_book_update_id,
     "b": [["price_str", "qty_str"], ...],
     "a": [["price_str", "qty_str"], ...]}
"""
from __future__ import annotations

import logging
import struct
from typing import List, Dict, Any, Tuple

from hummingbot.connector.exchange.binance_sbe import binance_sbe_constants as CONSTANTS

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class SbeDecodeError(Exception):
    """Base class for all decoder failures."""


class SbeSchemaMismatchError(SbeDecodeError):
    """Raised when the wire schema cannot be parsed by this decoder.

    Distinct from generic decode errors so the data-source layer can treat
    this as a fatal "stop and surface" signal (vs. a recoverable malformed
    frame).
    """


class SbeTruncatedFrameError(SbeDecodeError):
    """Raised when the buffer ends before we've read all declared fields."""


# ---------------------------------------------------------------------------
# Layout constants (offsets/sizes pinned to stream_1_0.xml)
# ---------------------------------------------------------------------------

# MessageHeader = blockLength u16 + templateId u16 + schemaId u16 + version u16
_HEADER_LAYOUT = "<HHHH"
_HEADER_SIZE = struct.calcsize(_HEADER_LAYOUT)  # 8

# groupSizeEncoding (default) = blockLength u16 + numInGroup u32
_GROUP_HEADER_32_LAYOUT = "<HI"
_GROUP_HEADER_32_SIZE = struct.calcsize(_GROUP_HEADER_32_LAYOUT)  # 6

# groupSize16Encoding = blockLength u16 + numInGroup u16
_GROUP_HEADER_16_LAYOUT = "<HH"
_GROUP_HEADER_16_SIZE = struct.calcsize(_GROUP_HEADER_16_LAYOUT)  # 4

# TradesStreamEvent root block (id=10000):
#   eventTime    int64 @ 0
#   transactTime int64 @ 8
#   priceExponent int8 @ 16
#   qtyExponent   int8 @ 17
# Total = 18 bytes (this decoder requires at least these fields).
_TRADES_ROOT_MIN_SIZE = 18
# Per-trade element fields (after trades-group header):
#   id           int64 @ 0
#   price        int64 @ 8
#   qty          int64 @ 16
#   isBuyerMaker uint8 @ 24
# isBestMatch is presence="constant" — NOT serialised on the wire.
_TRADE_ELEM_MIN_SIZE = 25

# DepthDiffStreamEvent root block (id=10003):
#   eventTime         int64 @ 0
#   firstBookUpdateId int64 @ 8
#   lastBookUpdateId  int64 @ 16
#   priceExponent     int8  @ 24
#   qtyExponent       int8  @ 25
# Total = 26 bytes.
_DEPTH_ROOT_MIN_SIZE = 26
# Per-level element fields:
#   price int64 @ 0
#   qty   int64 @ 8
_DEPTH_LEVEL_MIN_SIZE = 16

# Tracks templateIds we've already warned about so logs don't explode.
_warned_unknown_templates: set = set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_bytes_available(buf: bytes, offset: int, need: int) -> None:
    if offset + need > len(buf):
        raise SbeTruncatedFrameError(
            f"buffer too short: need {need} bytes at offset {offset}, have {len(buf) - offset}"
        )


def _read_header(buf: bytes) -> Tuple[int, int, int, int]:
    """Return ``(blockLength, templateId, schemaId, version)``."""
    _ensure_bytes_available(buf, 0, _HEADER_SIZE)
    return struct.unpack_from(_HEADER_LAYOUT, buf, 0)


def _validate_header(template_id: int, schema_id: int, version: int,
                     block_length: int, min_block_length: int) -> None:
    """Apply the schema-compatibility policy described in the module docstring."""
    if schema_id != CONSTANTS.SBE_SCHEMA_ID:
        raise SbeSchemaMismatchError(
            f"unexpected schemaId={schema_id} (expected {CONSTANTS.SBE_SCHEMA_ID}); "
            f"templateId={template_id}, version={version}"
        )
    if version > CONSTANTS.SBE_SCHEMA_VERSION:
        if block_length < min_block_length:
            raise SbeSchemaMismatchError(
                f"non-additive schema bump for templateId={template_id}: "
                f"wire blockLength={block_length} < required {min_block_length} "
                f"(schemaId={schema_id}, version={version} > expected {CONSTANTS.SBE_SCHEMA_VERSION})"
            )
        _logger.warning(
            "[binance_sbe] schema version=%d > expected %d for templateId=%d "
            "(additive change, parsing compatible fields only)",
            version, CONSTANTS.SBE_SCHEMA_VERSION, template_id,
        )


def _read_group_header_size32(buf: bytes, offset: int) -> Tuple[int, int, int]:
    """Read a ``groupSizeEncoding`` (uint16+uint32) group header.

    Returns ``(block_length, num_in_group, next_offset)``.
    """
    _ensure_bytes_available(buf, offset, _GROUP_HEADER_32_SIZE)
    block_length, num_in_group = struct.unpack_from(_GROUP_HEADER_32_LAYOUT, buf, offset)
    return block_length, num_in_group, offset + _GROUP_HEADER_32_SIZE


def _read_group_header_size16(buf: bytes, offset: int) -> Tuple[int, int, int]:
    """Read a ``groupSize16Encoding`` (uint16+uint16) group header.

    Returns ``(block_length, num_in_group, next_offset)``.
    """
    _ensure_bytes_available(buf, offset, _GROUP_HEADER_16_SIZE)
    block_length, num_in_group = struct.unpack_from(_GROUP_HEADER_16_LAYOUT, buf, offset)
    return block_length, num_in_group, offset + _GROUP_HEADER_16_SIZE


def _read_var_string8(buf: bytes, offset: int) -> Tuple[str, int]:
    """Read a ``varString8`` (uint8 length + UTF-8 bytes). Returns
    ``(value, next_offset)``."""
    _ensure_bytes_available(buf, offset, 1)
    length = buf[offset]
    next_off = offset + 1 + length
    _ensure_bytes_available(buf, offset + 1, length)
    return buf[offset + 1:next_off].decode("utf-8"), next_off


def _mantissa_to_decimal_str(mantissa: int, exponent: int) -> str:
    """Format a SBE decimal (``mantissa`` × 10^``exponent``) as an exact
    decimal string, mimicking the format Binance JSON streams emit.

    Pure string manipulation — no ``Decimal``, no ``float``. This keeps the
    output suitable for ``OrderBookMessage`` (which carries strings, not
    numeric types) and avoids both float rounding and the cost of building
    ``Decimal`` objects per level in the hot path.
    """
    if exponent == 0:
        return str(mantissa)
    if exponent > 0:
        # Positive exponent appends zeros to the integer representation.
        return str(mantissa) + "0" * exponent
    # exponent < 0: shift the decimal point left by abs(exponent).
    width = -exponent
    sign = "-" if mantissa < 0 else ""
    digits = str(abs(mantissa))
    if len(digits) <= width:
        # Result is <1: pad with leading zeros so we always emit one
        # integer digit ("0.xxxxxxxx" rather than ".xxxxxxxx").
        digits = "0" * (width - len(digits) + 1) + digits
    integer_part = digits[:-width]
    fractional_part = digits[-width:]
    return f"{sign}{integer_part}.{fractional_part}"


# ---------------------------------------------------------------------------
# Per-template parsers
# ---------------------------------------------------------------------------


def _parse_trades(buf: bytes, root_block_length: int) -> List[Dict[str, Any]]:
    """Parse a ``TradesStreamEvent`` frame (templateId=10000).

    Returns a list of trade dicts — one per element in the ``trades`` group.
    """
    offset = _HEADER_SIZE
    _ensure_bytes_available(buf, offset, _TRADES_ROOT_MIN_SIZE)

    # Root block fixed-position reads.
    event_time_us, transact_time_us = struct.unpack_from("<qq", buf, offset)
    price_exponent = struct.unpack_from("<b", buf, offset + 16)[0]
    qty_exponent = struct.unpack_from("<b", buf, offset + 17)[0]

    # Skip past the entire wire root block, including any newer-version fields.
    offset += root_block_length

    # Group header: TradesStreamEvent.trades uses the *default*
    # groupSizeEncoding (uint16+uint32).
    elem_block_length, num_trades, offset = _read_group_header_size32(buf, offset)

    event_time_ms = event_time_us // 1000

    out: List[Dict[str, Any]] = []
    for _ in range(num_trades):
        _ensure_bytes_available(buf, offset, _TRADE_ELEM_MIN_SIZE)
        trade_id, price_mantissa, qty_mantissa = struct.unpack_from("<qqq", buf, offset)
        is_buyer_maker = buf[offset + 24] != 0
        # isBestMatch is presence="constant" — not on wire, always True.

        out.append({
            "e": "trade",
            "E": event_time_ms,
            "E_us": event_time_us,
            "transactTime_us": transact_time_us,
            # symbol filled after the loop (varString8 follows the group)
            "t": trade_id,
            "p": _mantissa_to_decimal_str(price_mantissa, price_exponent),
            "q": _mantissa_to_decimal_str(qty_mantissa, qty_exponent),
            "m": is_buyer_maker,
        })
        offset += elem_block_length

    symbol, _ = _read_var_string8(buf, offset)
    for ev in out:
        ev["s"] = symbol
    return out


def _parse_depth_diff(buf: bytes, root_block_length: int) -> List[Dict[str, Any]]:
    """Parse a ``DepthDiffStreamEvent`` frame (templateId=10003).

    Always returns a list of exactly one ``depthUpdate`` dict (the API has
    only one depth diff per frame). Wrapping in a list keeps the public
    ``decode_frame`` signature uniform across template types.
    """
    offset = _HEADER_SIZE
    _ensure_bytes_available(buf, offset, _DEPTH_ROOT_MIN_SIZE)

    event_time_us, first_update_id, last_update_id = struct.unpack_from("<qqq", buf, offset)
    price_exponent = struct.unpack_from("<b", buf, offset + 24)[0]
    qty_exponent = struct.unpack_from("<b", buf, offset + 25)[0]

    offset += root_block_length

    # Both depth groups use groupSize16Encoding (uint16+uint16).
    bids, offset = _read_levels(buf, offset, price_exponent, qty_exponent)
    asks, offset = _read_levels(buf, offset, price_exponent, qty_exponent)

    symbol, _ = _read_var_string8(buf, offset)

    return [{
        "e": "depthUpdate",
        "E": event_time_us // 1000,
        "E_us": event_time_us,
        "s": symbol,
        "U": first_update_id,
        "u": last_update_id,
        "b": bids,
        "a": asks,
    }]


def _read_levels(buf: bytes, offset: int,
                 price_exponent: int, qty_exponent: int
                 ) -> Tuple[List[List[str]], int]:
    """Read a price-level repeating group encoded with ``groupSize16Encoding``.

    Returns ``(levels, next_offset)`` where ``levels`` is a list of
    ``[price_str, qty_str]`` pairs.
    """
    elem_block_length, num_levels, offset = _read_group_header_size16(buf, offset)
    out: List[List[str]] = []
    for _ in range(num_levels):
        _ensure_bytes_available(buf, offset, _DEPTH_LEVEL_MIN_SIZE)
        price_mantissa, qty_mantissa = struct.unpack_from("<qq", buf, offset)
        out.append([
            _mantissa_to_decimal_str(price_mantissa, price_exponent),
            _mantissa_to_decimal_str(qty_mantissa, qty_exponent),
        ])
        offset += elem_block_length
    return out, offset


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def decode_frame(buf: bytes) -> List[Dict[str, Any]]:
    """Decode a single SBE frame into a list of event dicts.

    Returns a list because ``TradesStreamEvent`` can carry multiple trades
    in one frame (the ``trades`` repeating group). Other templates return
    a single-element list for shape uniformity.

    Behaviour:

    * Unknown template id           → empty list + one-shot WARNING log.
    * ``schemaId`` mismatch          → ``SbeSchemaMismatchError`` (fail-stop).
    * Non-additive schema bump      → ``SbeSchemaMismatchError`` (fail-stop).
    * Additive schema bump          → parse compatible fields + WARNING log.
    * Truncated buffer              → ``SbeTruncatedFrameError``.
    """
    block_length, template_id, schema_id, version = _read_header(buf)

    if template_id == CONSTANTS.SBE_TEMPLATE_ID_TRADES:
        _validate_header(template_id, schema_id, version, block_length, _TRADES_ROOT_MIN_SIZE)
        return _parse_trades(buf, block_length)

    if template_id == CONSTANTS.SBE_TEMPLATE_ID_DEPTH_DIFF:
        _validate_header(template_id, schema_id, version, block_length, _DEPTH_ROOT_MIN_SIZE)
        return _parse_depth_diff(buf, block_length)

    # Validate schemaId even for unknown templates — if the schema itself
    # has been replaced, we want to fail loudly rather than silently
    # dropping everything as "unknown".
    if schema_id != CONSTANTS.SBE_SCHEMA_ID:
        raise SbeSchemaMismatchError(
            f"unexpected schemaId={schema_id} (expected {CONSTANTS.SBE_SCHEMA_ID}); "
            f"templateId={template_id}, version={version}"
        )

    if template_id not in _warned_unknown_templates:
        _warned_unknown_templates.add(template_id)
        _logger.warning(
            "[binance_sbe] ignoring unknown SBE templateId=%d "
            "(schemaId=%d, version=%d) — not handled in Phase 1",
            template_id, schema_id, version,
        )
    return []
