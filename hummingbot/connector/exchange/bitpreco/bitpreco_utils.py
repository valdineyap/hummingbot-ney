from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-BRL"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0"),
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return True


class BitprecoConfigMap(BaseConnectorConfigMap):
    connector: str = "bitpreco"
    bitpreco_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your BitPreco API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bitpreco_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your BitPreco API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    # Phase 1A toggle. When True, the connector spawns the
    # BitprecoRedisUserStreamShadow + BitprecoRedisOrderBookShadow
    # tasks alongside the legacy Phoenix/REST path. The shadow
    # observers consume Redis pub/sub, exercise the production
    # parser/state-machine pipeline and emit [redis_shadow*]
    # metrics, but do NOT mutate bot state. Default off so existing
    # deployments are unchanged. Requires BITPRECO_REDIS_* + BITPRECO_USER_ID
    # env vars (see .env.example); missing env vars → shadow mode
    # disables itself with a single WARN at startup.
    bitpreco_redis_shadow_mode: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda cm: (
                "Run the BitPreco Redis pub/sub observer in shadow mode "
                "alongside the legacy path? (no impact on trading) (yes/no)"
            ),
            "prompt_on_new": False,
        }
    )
    model_config = ConfigDict(title="bitpreco")


KEYS = BitprecoConfigMap.model_construct()
