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
  - ``binance_api_key`` / ``binance_api_secret`` (REQUIRED, secure):
    HMAC credentials used by the inherited ``BinanceExchange`` for the
    REST endpoints the framework polls on every connector regardless of
    role (``/api/v3/exchangeInfo``, ``/api/v3/account``, listen-key for
    user data stream). Even when this connector is configured as
    ``signal_connector`` (Phase 1, market-data-only role), Hummingbot's
    framework passes ``trading_required=True`` uniformly and the parent
    class still issues those signed REST calls during startup; without
    valid HMAC they fail with HTTP 401 ``API-key format invalid`` and
    the connector never reaches ``ready=True``. The simplest path is
    to reuse the same HMAC credentials as the operator's existing
    ``binance`` connector — ``binance_sbe_register.py`` copies them
    automatically from ``conf/connectors/binance.yml``.

  All three fields are REQUIRED (no ``Optional[SecretStr] = None``)
  because an earlier iteration had them optional and the yaml was
  serialised with ``null`` for missing fields; ``Security.decrypt_all()``
  then called ``decrypt_secret_value(attr, None)`` and raised TypeError,
  poisoning decryption for unrelated connectors (binance, bitpreco).
  Requiring them avoids that whole class of bug.
"""
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

    # HMAC credentials inherited from BinanceExchange. Required even for
    # signal-only Phase 1 — see the module docstring for why.
    binance_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Binance HMAC API key (same as the binance connector uses)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    binance_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Binance HMAC API secret (same as the binance connector uses)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )

    model_config = ConfigDict(title="binance_sbe")


KEYS = BinanceSbeConfigMap.model_construct()
