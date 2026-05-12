"""Connector registration + config schema for ``binance_sbe``.

The Hummingbot framework auto-discovers connectors by scanning
``hummingbot/connector/exchange/*/`` for a ``<name>_utils.py`` module that
exports ``KEYS``, ``EXAMPLE_PAIR``, and ``DEFAULT_FEES``. By keeping this
module thin and reusing the JSON connector's fee schema and helpers, we
avoid duplicating any business logic.

Credentials model:
  - ``binance_sbe_api_key`` (REQUIRED, secure): the Ed25519 API key STRING
    registered in the Binance portal. Goes verbatim into the
    ``X-MBX-APIKEY`` header on the SBE WebSocket. No PEM, no signing —
    public market data only.
  - ``binance_api_key`` / ``binance_api_secret`` (optional, HMAC): only
    needed when this connector is used as a *trading* connector (taker
    role). For signal-only use (the recommended Phase-1 rollout) these
    can be omitted. BinanceSbeExchange validates their presence when
    ``trading_required=True``.
"""
from typing import Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
# Reuse fee schema and exchange-info validator from the JSON connector.
# These are not SBE-specific — fees apply to trading (which still goes
# through REST) and the exchange-info filter is identical for both
# connectors since they hit the same /exchangeInfo endpoint.
from hummingbot.connector.exchange.binance.binance_utils import (  # noqa: F401
    CENTRALIZED,
    DEFAULT_FEES,
    is_exchange_information_valid,
)

EXAMPLE_PAIR = "BTC-USDT"


class BinanceSbeConfigMap(BaseConnectorConfigMap):
    connector: str = "binance_sbe"

    # The Ed25519 API key STRING (not the PEM private key). Goes into
    # X-MBX-APIKEY header on the SBE WebSocket. Sufficient on its own for
    # public market data — Binance SBE does not require signed handshakes.
    binance_sbe_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Binance Ed25519 API key string (SBE market data)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )

    # HMAC keys for trading. Optional at the schema level so the connector
    # can be used as signal_connector without trading credentials. The
    # exchange class raises ValueError if trading_required=True and these
    # are missing — fail-loud at boot instead of silently at first order.
    binance_api_key: Optional[SecretStr] = Field(
        default=None,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Binance HMAC API key (only if using binance_sbe as taker)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": False,
        }
    )
    binance_api_secret: Optional[SecretStr] = Field(
        default=None,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Binance HMAC API secret (only if using binance_sbe as taker)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": False,
        }
    )

    model_config = ConfigDict(title="binance_sbe")


KEYS = BinanceSbeConfigMap.model_construct()
