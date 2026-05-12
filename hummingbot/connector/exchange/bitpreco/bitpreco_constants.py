import os

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

# ----------------------------------------------------------------------------
# Fast-path internal routing (BitPreco-owned operators only)
# ----------------------------------------------------------------------------
# Two independent overrides, with DIFFERENT semantics:
#
#   BITPRECO_INTERNAL_BOOKS  → HOST for public market data. Paths
#                              ({pair}/orderbook, btc-brl/ticker, ...)
#                              are appended by the connector.
#                              Example: http://54.232.138.12
#
#   BITPRECO_INTERNAL_API    → FULL URL for private REST trading. Every
#                              cmd (buy, sell, order_cancel, balance,
#                              all_orders_cancel, ...) is POSTed to this
#                              single endpoint with the cmd in the JSON
#                              body — same as the public /trading.
#                              Example:
#                              https://backend.bitpreco.com/exchange/exch_api.php
#
# The asymmetry exists because the private side is a single endpoint
# discriminated by `cmd` in the body, while the public side is REST with
# multiple paths.
#
# Both default to the public host. If set, there is NO fallback on
# failure — the internal route MUST be reachable. A network error
# propagates as a normal IOError, intentionally (silent fallback would
# mask a real outage).
#
# Legacy env vars `BITPRECO_ORDER_BOOK_URL` and `BITPRECO_TRADING_URL`
# are kept as low-level escape hatches: if set, they win over the
# `*_INTERNAL_*` variables for their specific endpoints.
# ----------------------------------------------------------------------------

_DEFAULT_HOST = "https://api.bitpreco.com"

_PUBLIC_HOST = os.environ.get("BITPRECO_INTERNAL_BOOKS", _DEFAULT_HOST).rstrip("/")

# Public market data endpoints (host + path)
ORDER_BOOK_PATH_URL = f"{_PUBLIC_HOST}/{{}}/orderbook"


# REST API ENDPOINTS
TRADING_PATH_URL = "trading"
DEFAULT_DOMAIN = "bitpreco_trading"

# Private REST trading endpoint: BITPRECO_INTERNAL_API, if set, is the
# FULL URL — used verbatim. Otherwise default to the public host + /trading.
_internal_api = os.environ.get("BITPRECO_INTERNAL_API")
if _internal_api:
    REST_URL = _internal_api.rstrip("/")
else:
    REST_URL = f"{_DEFAULT_HOST}/{TRADING_PATH_URL}"

# Legacy escape hatches — take precedence over *_INTERNAL_* for their endpoint.
if os.environ.get('BITPRECO_ORDER_BOOK_URL') is not None:
    url = os.environ.get("BITPRECO_ORDER_BOOK_URL", "")
    slash = "" if url.endswith("/") else "/"
    ORDER_BOOK_PATH_URL = url + slash + "{}/orderbook"

if os.environ.get('BITPRECO_TRADING_URL') is not None:
    REST_URL = os.environ.get('BITPRECO_TRADING_URL')

MAX_ORDER_ID_LEN = 100

HBOT_ORDER_ID_PREFIX = ""

# Websocket event types TODO
DIFF_EVENT_TYPE = "diffDepth"
TRADE_EVENT_TYPE = "trade"
SNAPSHOT_EVENT_TYPE = "depth"

# Order States

# BitPreco status values observed in production:
#   OPEN     — resting on the book (no fills yet)
#   FILLED   — fully executed
#   CANCELED — fully canceled (no fills) OR canceled after partial fill
#   EMPTY    — book entry not yet visible to status endpoint (treated as OPEN)
#   PARTIAL  — partially executed AND still resting on the book
#
# When BitPreco returns ``status: "PARTIAL"`` together with ``canceled: "1"``,
# the order is no longer on the book — the partial fill is what we got and the
# remainder was cancelled. ``_request_order_status`` post-processes that case
# and overrides the state to CANCELED so the tracker reaches a terminal state
# and the executor can proceed with hedging the partial executed amount.
ORDER_STATE = {
    "OPEN": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "CANCELED": OrderState.CANCELED,
    "EMPTY": OrderState.OPEN,
    "PARTIAL": OrderState.PARTIALLY_FILLED,
}

REQUEST_WEIGHT = "REQUEST_WEIGHT"
RAW_REQUESTS = "RAW_REQUESTS"

# Base URL
PING_PATH_URL = f"{_PUBLIC_HOST}/btc-brl/ticker"
ALL_CURRENCY_TICKER_PATH_URL = f"{_PUBLIC_HOST}/all-brl/ticker"

WSS_ORDERBOOK_URL = "wss://bp-channels.gigalixirapp.com/orderbook/socket/websocket"
WSS_NOTIFICATIONS_URL = "wss://bp-channels.gigalixirapp.com/notifications/socket/websocket"
WS_ORDERBOOK_TOPIC = "orderbook"
WS_NOTIFICATIONS_TOPIC = "notifications"

WS_HEARTBEAT_TIME_INTERVAL = 30

ONE_MINUTE = 60

CMD_BUY = "buy"
CMD_SELL = "sell"
CMD_CANCEL_ORDER = "order_cancel"


RATE_LIMITS = [
    RateLimit(limit_id=REST_URL, limit=100, time_interval=1,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 40),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ALL_CURRENCY_TICKER_PATH_URL, limit=100, time_interval=1,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 40),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=PING_PATH_URL, limit=100, time_interval=1,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 40),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
]
