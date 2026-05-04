import os

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

ORDER_BOOK_PATH_URL = "https://api.bitpreco.com/{}/orderbook"

if os.environ.get('BITPRECO_ORDER_BOOK_URL') is not None:
    url = os.environ.get("BITPRECO_ORDER_BOOK_URL", "")
    slash = "" if url.endswith("/") else "/"
    ORDER_BOOK_PATH_URL = url + slash + "{}/orderbook"


# REST API ENDPOINTS
TRADING_PATH_URL = "trading"
DEFAULT_DOMAIN = "bitpreco_trading"
REST_URL = f'https://api.bitpreco.com/{TRADING_PATH_URL}'
if os.environ.get('BITPRECO_TRADING_URL') is not None:
    REST_URL = os.environ.get('BITPRECO_TRADING_URL')

MAX_ORDER_ID_LEN = 100

HBOT_ORDER_ID_PREFIX = ""

# Websocket event types TODO
DIFF_EVENT_TYPE = "diffDepth"
TRADE_EVENT_TYPE = "trade"
SNAPSHOT_EVENT_TYPE = "depth"

# Order States

ORDER_STATE = {
    "OPEN": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "CANCELED": OrderState.CANCELED,
    "EMPTY": OrderState.OPEN,
}

REQUEST_WEIGHT = "REQUEST_WEIGHT"
RAW_REQUESTS = "RAW_REQUESTS"

# Base URL
PING_PATH_URL = "https://api.bitpreco.com/btc-brl/ticker"
ALL_CURRENCY_TICKER_PATH_URL = "https://api.bitpreco.com/all-brl/ticker"

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
