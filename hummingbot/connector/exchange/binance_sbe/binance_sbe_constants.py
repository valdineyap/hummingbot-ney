"""Constants for the Binance Spot SBE Market Data connector.

This connector consumes the binary SBE stream at ``stream-sbe.binance.com``
instead of the JSON stream at ``stream.binance.com``. All REST/trading paths
are inherited from the public-JSON ``binance`` connector via re-export.

Schema values below are pinned to ``stream_1_0.xml`` (schemaId=1, version=0).
If Binance bumps the schema, update these constants and run the integration
test ``test_live_schema_version_matches`` to validate.

See: https://developers.binance.com/docs/binance-spot-api-docs/sbe-market-data-streams
"""
# Re-export everything from the JSON connector's constants so we inherit
# REST URLs, paths, rate limits, error codes, etc. The only thing SBE
# differs in is the WebSocket public-stream URL and the binary message
# format on that stream.
from hummingbot.connector.exchange.binance.binance_constants import *  # noqa: F401, F403

# ----------------------------------------------------------------------------
# SBE Market Data Stream — WebSocket
# ----------------------------------------------------------------------------

# Domain key used by Hummingbot when this connector is selected by name.
# Distinct from the JSON connector's domains ("com", "us") because the SBE
# host is a separate endpoint, not a regional variant.
WSS_SBE_URL = "wss://stream-sbe.binance.com:9443/ws"

# ----------------------------------------------------------------------------
# Schema identity (validated in every frame's MessageHeader)
# ----------------------------------------------------------------------------
# stream_1_0.xml declares:
#     <sbe:messageSchema ... id="1" version="0" ...>
# Decoder fails fast on schemaId mismatch. Version increments are tolerated
# when additive (blockLength grows); see sbe_decoder._validate_header.
SBE_SCHEMA_ID = 1
SBE_SCHEMA_VERSION = 0

# ----------------------------------------------------------------------------
# Template IDs for the messages we consume in Phase 1
# ----------------------------------------------------------------------------
# Confirmed in stream_1_0.xml:
#   <sbe:message name="TradesStreamEvent"     id="10000">
#   <sbe:message name="BestBidAskStreamEvent" id="10001">
#   <sbe:message name="DepthSnapshotStreamEvent" id="10002">
#   <sbe:message name="DepthDiffStreamEvent"  id="10003">
SBE_TEMPLATE_ID_TRADES = 10000
SBE_TEMPLATE_ID_BEST_BID_ASK = 10001  # decoded as unknown in Phase 1
SBE_TEMPLATE_ID_DEPTH_SNAPSHOT = 10002  # decoded as unknown in Phase 1
SBE_TEMPLATE_ID_DEPTH_DIFF = 10003

# Templates handled by the Phase-1 decoder. Anything outside this set is
# logged as "unknown templateId" (warning, not fail-stop) so Binance can
# add new message types without breaking us.
SBE_TEMPLATES_HANDLED = (
    SBE_TEMPLATE_ID_TRADES,
    SBE_TEMPLATE_ID_DEPTH_DIFF,
)

# ----------------------------------------------------------------------------
# Reconnect — Binance enforces a 24h max per WS connection
# ----------------------------------------------------------------------------
# We proactively recycle the connection well before the hard cap so a
# reconnect never coincides with peak traffic. Tunable via env var for
# tests (see test_reconnect_on_artificial_ttl_short_window).
import os as _os  # noqa: E402

WS_RECONNECT_INTERVAL_SEC = int(_os.environ.get("BINANCE_SBE_WS_RECONNECT_SEC", str(23 * 3600)))
