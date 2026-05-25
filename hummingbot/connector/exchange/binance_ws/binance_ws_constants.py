"""Constants for the Binance Spot WebSocket API Trading connector.

This connector substitutes REST ``POST /api/v3/order`` and
``DELETE /api/v3/order`` with their WebSocket-API equivalents
(``order.place`` and ``order.cancel``) on
``wss://ws-api.binance.com:443/ws-api/v3``. Everything else (market data,
user stream, auth, fees, rate limits) is inherited from the JSON
``binance`` connector via re-export — only the trading-send path
diverges.

If Binance changes endpoint or rate-limit semantics, update these
constants and run the integration smoke ``test_live_order_test_no_submit``
to validate.

See: https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/general-api-information
"""
# Re-export REST URLs, rate-limit pools, order-state map, etc. from the
# JSON connector. Trading-WS adds *new* limit ids on top, linked to the
# same shared buckets (REQUEST_WEIGHT, ORDERS) since Binance counts
# per-IP, not per-channel.
from hummingbot.connector.exchange.binance.binance_constants import *  # noqa: F401, F403

import os as _os

# ----------------------------------------------------------------------------
# WebSocket Trading API — endpoints
# ----------------------------------------------------------------------------
# Format with .format(domain) — mirrors binance_constants.REST_URL pattern.
WSS_API_TRADING_URL = "wss://ws-api.binance.{}:443/ws-api/v3"
WSS_API_TRADING_URL_TESTNET = "wss://ws-api.testnet.binance.vision/ws-api/v3"

# ----------------------------------------------------------------------------
# Lifecycle — Binance enforces TTL 24h per WS connection
# ----------------------------------------------------------------------------
# Reconnect proactively well before the hard cap so a forced reconnect
# never coincides with peak traffic. Tunable via env var for tests.
WS_RECONNECT_INTERVAL_SEC = int(
    _os.environ.get("BINANCE_WS_RECONNECT_SEC", str(23 * 3600))
)

# Binance documents a server-side max of ~10s before declaring -1007 Timeout.
# Local timeout must allow margin above that — 5s would mark UNKNOWN on
# requests that the matching engine would still answer in 6-8s.
WS_REQUEST_TIMEOUT_SEC = float(_os.environ.get("BINANCE_WS_REQUEST_TIMEOUT_SEC", "11.0"))

# Late-response capture window: pending entry is kept this long after the
# local timeout so a delayed reply gets logged (for manual reconciliation
# of UNKNOWN orders) instead of being silently dropped.
WS_LATE_RESPONSE_WINDOW_SEC = float(
    _os.environ.get("BINANCE_WS_LATE_RESPONSE_WINDOW_SEC", "15.0")
)

# Warn (but do not error) when a single request takes longer than this.
WS_SLOW_REQUEST_WARN_MS = 1000.0

# Server-side ping cadence is ~20s; aiohttp's autoping=True handles
# responding without an application-level pong handler.
WS_PING_INTERVAL_SEC = 20

# serverShutdown event arrives ~10min before the connection cap. Grace
# window allows the new connection to absorb new traffic while the
# old socket drains pending requests before hard-closing.
WS_SERVER_SHUTDOWN_DRAIN_SEC = 300

# Reconnect backoff: exponential with jitter. Resets after a connection
# stays stable >60s. If >MAX_RECONNECTS_IN_WINDOW reconnects fire in
# RECONNECT_WINDOW_SEC, router enters FAILED state.
WS_RECONNECT_BACKOFF_MIN_SEC = 1.0
WS_RECONNECT_BACKOFF_MAX_SEC = 30.0
WS_RECONNECT_STABLE_RESET_SEC = 60.0
WS_RECONNECT_WINDOW_SEC = 300.0
WS_MAX_RECONNECTS_IN_WINDOW = 10
WS_FAILED_RETRY_INTERVAL_SEC = 60.0

# ----------------------------------------------------------------------------
# Rate-limit sampling
# ----------------------------------------------------------------------------
# Default request URL omits returnRateLimits=true to save bytes. Every N
# requests OR N seconds we sample one with the flag flipped on so we
# track usage versus the IP cap.
WS_RATE_LIMIT_SAMPLE_EVERY_N = 100
WS_RATE_LIMIT_SAMPLE_INTERVAL_SEC = 30.0
WS_RATE_LIMIT_ALERT_THRESHOLD_PCT = 80.0

# ----------------------------------------------------------------------------
# Auth / signing
# ----------------------------------------------------------------------------
# WS API SIGNED methods accept recvWindow up to 60000ms; default 5000ms
# matches the REST connector behaviour.
RECV_WINDOW_MS = 5000

# ----------------------------------------------------------------------------
# Method names (Binance WS API verbs)
# ----------------------------------------------------------------------------
WS_METHOD_PING = "ping"
WS_METHOD_TIME = "time"
WS_METHOD_ORDER_PLACE = "order.place"
WS_METHOD_ORDER_CANCEL = "order.cancel"
WS_METHOD_ORDER_TEST = "order.test"
WS_METHOD_ORDER_STATUS = "order.status"
WS_METHOD_ACCOUNT_RATE_LIMITS = "account.rateLimits.orders"

# ----------------------------------------------------------------------------
# Throttler limit ids
# ----------------------------------------------------------------------------
# Per-method ids so the throttler can apply distinct weights. All are
# linked (in binance_ws_utils.build_rate_limits) to the same shared
# REQUEST_WEIGHT and ORDERS pools the REST connector uses, since Binance
# counts both channels against the same per-IP cap.
WS_LIMIT_CONNECT = "WS_CONNECT"
WS_LIMIT_PING = "WS_PING"
WS_LIMIT_ORDER_TEST = "WS_ORDER_TEST"
WS_LIMIT_ORDER_TEST_COMMISSION = "WS_ORDER_TEST_COMMISSION"
WS_LIMIT_ORDER_PLACE = "WS_ORDER_PLACE"
WS_LIMIT_ORDER_CANCEL = "WS_ORDER_CANCEL"
WS_LIMIT_ORDER_STATUS = "WS_ORDER_STATUS"
WS_LIMIT_ACCOUNT_RATE_LIMITS = "WS_ACCOUNT_RATE_LIMITS"

# ----------------------------------------------------------------------------
# Logging tags (grep-friendly, mirror BitPreco's [bp_timing] convention)
# ----------------------------------------------------------------------------
WS_LOG_TIMING = "[bws_timing]"
WS_LOG_LATE_RESPONSE = "[bws_late_response]"
WS_LOG_RATE_LIMIT = "[bws_rate_limit]"
WS_LOG_CANCEL_TERMINAL = "[bws_cancel_terminal_state]"
WS_LOG_UNHANDLED_EVENT = "[bws_unhandled_event]"
WS_LOG_RECONNECT = "[bws_reconnect]"
WS_LOG_SERVER_SHUTDOWN = "[bws_server_shutdown]"
