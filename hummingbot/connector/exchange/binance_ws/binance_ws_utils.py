"""Connector registration + config schema + rate-limit table for ``binance_ws``.

Hummingbot auto-discovers connectors by scanning
``hummingbot/connector/exchange/*/`` for ``<name>_utils.py`` modules
exporting ``KEYS``, ``EXAMPLE_PAIR``, and ``DEFAULT_FEES``. Reusing the
JSON connector's fee schema and exchange-info validator keeps this
module thin — only the secrets schema and the WS-specific rate-limit
entries are new.

Credentials model:
  - ``binance_ws_api_key`` / ``binance_ws_api_secret`` (REQUIRED, secure):
    HMAC credentials used by ``BinanceAuth.generate_ws_signature`` to
    sign each ``order.place`` / ``order.cancel`` payload. Both fields
    are REQUIRED (no ``Optional[SecretStr] = None``) for the same
    reason as ``binance_sbe``: optional fields serialise as ``null``
    and ``Security.decrypt_all`` raises TypeError on the ``None``,
    poisoning decryption for unrelated connectors.

The HMAC is the SAME secret the operator already uses for the ``binance``
connector (Binance issues one HMAC per API key — REST and WS-API share
it). ``binance_ws_register.py`` copies the credentials automatically
from ``conf/connectors/binance.yml`` so the operator never types them
twice.

Operator-visible runtime flag:
  - ``use_ws_trading`` (default True): revert switch. Setting to False
    + restart makes the connector fall back to the inherited REST path
    in ``_place_order`` / ``_place_cancel``. No need to uninstall.
"""
from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
# Reuse fees + exchange-info validator from the JSON connector — Binance
# Spot has one fee schedule and one /exchangeInfo response shape
# regardless of which channel places the order.
from hummingbot.connector.exchange.binance.binance_utils import (  # noqa: F401
    CENTRALIZED,
    DEFAULT_FEES,
    is_exchange_information_valid,
)
from hummingbot.connector.exchange.binance_ws import binance_ws_constants as CONSTANTS
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

EXAMPLE_PAIR = "BTC-USDT"


class BinanceWsConfigMap(BaseConnectorConfigMap):
    connector: str = "binance_ws"

    # Two SecretStr fields (not three): the WS-API connector does not
    # need a separate "identifier" key — its HMAC is sufficient to both
    # authenticate signed REST polls (inherited from BinanceExchange)
    # AND sign WS-API trading methods.
    binance_ws_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Binance HMAC API key (used for both REST polls and WS-API trading)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    binance_ws_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Binance HMAC API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )

    # Revert switch. False + restart → fall back to REST trading path.
    # Lives on the ConfigMap (not a controller flag) so it survives
    # across restarts without operator editing the controller YAML.
    use_ws_trading: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda cm: "Use WebSocket-API for trading (False = REST fallback)? [Yes/No]",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )

    model_config = ConfigDict(title="binance_ws")


KEYS = BinanceWsConfigMap.model_construct()


# ----------------------------------------------------------------------------
# WS-API rate limits
# ----------------------------------------------------------------------------
# Per-method weights derived from
# https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/trading-requests
# (and -api-docs/general-api-information for ping/time/account).
#
# Each limit_id has ``linked_limits`` pointing to the shared REQUEST_WEIGHT
# and (where applicable) ORDERS pools that the REST connector uses. Binance
# counts both channels against the same per-IP cap, so bypassing this
# wiring would cause silent 429s under load.
#
# Outer ``limit`` value is set high (MAX_REQUEST) because the *effective*
# cap comes from the linked shared pools; the throttler walks
# ``linked_limits`` on every ``execute_task`` call.
def build_rate_limits():
    """Return the additive list of RateLimit entries for binance_ws.

    Caller (BinanceWsExchange.__init__) appends this to
    ``binance_constants.RATE_LIMITS`` so the AsyncThrottler sees both
    REST and WS-API entries linked to the same shared pools.
    """
    from hummingbot.connector.exchange.binance.binance_constants import (
        ORDERS,
        REQUEST_WEIGHT,
        RAW_REQUESTS,
        MAX_REQUEST,
        ONE_MINUTE,
    )

    def _linked(weight: int, orders: int = 0):
        pairs = [
            LinkedLimitWeightPair(REQUEST_WEIGHT, weight),
            LinkedLimitWeightPair(RAW_REQUESTS, 1),
        ]
        if orders > 0:
            pairs.append(LinkedLimitWeightPair(ORDERS, orders))
        return pairs

    return [
        RateLimit(limit_id=CONSTANTS.WS_LIMIT_CONNECT, limit=MAX_REQUEST,
                  time_interval=ONE_MINUTE, linked_limits=_linked(weight=2)),
        RateLimit(limit_id=CONSTANTS.WS_LIMIT_PING, limit=MAX_REQUEST,
                  time_interval=ONE_MINUTE, linked_limits=_linked(weight=1)),
        RateLimit(limit_id=CONSTANTS.WS_LIMIT_ORDER_TEST, limit=MAX_REQUEST,
                  time_interval=ONE_MINUTE, linked_limits=_linked(weight=1)),
        RateLimit(limit_id=CONSTANTS.WS_LIMIT_ORDER_TEST_COMMISSION, limit=MAX_REQUEST,
                  time_interval=ONE_MINUTE, linked_limits=_linked(weight=20)),
        # order.place: REQUEST_WEIGHT 1, ORDERS 1.
        RateLimit(limit_id=CONSTANTS.WS_LIMIT_ORDER_PLACE, limit=MAX_REQUEST,
                  time_interval=ONE_MINUTE, linked_limits=_linked(weight=1, orders=1)),
        # order.cancel: REQUEST_WEIGHT 1, no ORDERS slot consumed.
        RateLimit(limit_id=CONSTANTS.WS_LIMIT_ORDER_CANCEL, limit=MAX_REQUEST,
                  time_interval=ONE_MINUTE, linked_limits=_linked(weight=1)),
        RateLimit(limit_id=CONSTANTS.WS_LIMIT_ORDER_STATUS, limit=MAX_REQUEST,
                  time_interval=ONE_MINUTE, linked_limits=_linked(weight=4)),
        RateLimit(limit_id=CONSTANTS.WS_LIMIT_ACCOUNT_RATE_LIMITS, limit=MAX_REQUEST,
                  time_interval=ONE_MINUTE, linked_limits=_linked(weight=40)),
    ]
