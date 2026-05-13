"""Connector registration + config schema for ``binance_sbe``.

The Hummingbot framework auto-discovers connectors by scanning
``hummingbot/connector/exchange/*/`` for a ``<name>_utils.py`` module that
exports ``KEYS``, ``EXAMPLE_PAIR``, and ``DEFAULT_FEES``. By keeping this
module thin and reusing the JSON connector's fee schema and helpers, we
avoid duplicating any business logic.

Credentials model (Phase-1 / signal-only):
  - ``binance_sbe_api_key`` (REQUIRED, secure): the Ed25519 API key STRING
    registered in the Binance portal. Goes verbatim into the
    ``X-MBX-APIKEY`` header on the SBE WebSocket. No PEM, no signing —
    public market data only.

  - HMAC fields (``binance_api_key`` / ``binance_api_secret``) are
    INTENTIONALLY OMITTED from this ConfigMap. They were here in an
    earlier iteration as ``Optional[SecretStr] = None`` but that broke
    ``Security.decrypt_all()`` across ALL connector configs: when the
    yaml was written with ``null`` for those fields, the traversal in
    ``_decrypt_all_internal_secrets`` called
    ``decrypt_secret_value(attr, None)`` and raised TypeError, poisoning
    decryption for unrelated connectors (binance, bitpreco). For Phase 2
    (this connector as ``taker_connector``, no latency gain — see plan)
    re-add the fields and ensure the yaml never serialises ``null`` for
    SecretStr — for instance by writing two separate ConfigMap variants
    (signal-only vs trading) or by using a custom serialiser that drops
    None fields.
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

    model_config = ConfigDict(title="binance_sbe")


KEYS = BinanceSbeConfigMap.model_construct()
