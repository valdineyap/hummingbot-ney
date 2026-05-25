"""Exchange class for the ``binance_ws`` connector.

Inherits ``BinanceExchange`` and composes ``BinanceWsTradingMixin`` to
swap the trading-send path (REST → WS-API) without touching market
data, user stream, auth, fees, exchange-info, or rate-limit polling.

Credentials model:
  - ``binance_ws_api_key`` / ``binance_ws_api_secret`` (REQUIRED): the
    HMAC pair used by both inherited REST polls AND the WS-API
    signature. Binance issues one HMAC per API key — REST and WS-API
    share it. The ConfigMap exposes namespaced fields so operators can
    keep ``binance.yml`` and ``binance_ws.yml`` distinct (different
    rate-limit budgets, different revert flags) while
    ``binance_ws_register.py`` copies the secret from the existing
    ``binance.yml`` for them.

Revert: ``use_ws_trading=False`` on the ConfigMap (or
``BINANCE_WS_USE_WS_TRADING=false`` env var) makes the connector fall
back to the inherited REST trading path while keeping every other
behaviour identical.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.connector.exchange.binance import binance_constants as BINANCE_CONSTANTS
from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange
from hummingbot.connector.exchange.binance_ws.binance_ws_request_router import (
    BinanceWsRequestRouter,
)
from hummingbot.connector.exchange.binance_ws.binance_ws_trading_mixin import (
    BinanceWsTradingMixin,
)
from hummingbot.connector.exchange.binance_ws.binance_ws_utils import build_rate_limits
from hummingbot.core.network_iterator import NetworkStatus


class BinanceWsExchange(BinanceWsTradingMixin, BinanceExchange):

    def __init__(self,
                 binance_ws_api_key: str = "",
                 binance_ws_api_secret: str = "",
                 use_ws_trading: bool = True,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = BINANCE_CONSTANTS.DEFAULT_DOMAIN,
                 ):
        # Fail-fast on missing HMAC when trading is required. The
        # parent BinanceExchange issues signed REST calls during boot
        # (/api/v3/account, listen-key) regardless of role; without
        # HMAC those calls return opaque HTTP 401s minutes later.
        if trading_required and (not binance_ws_api_key or not binance_ws_api_secret):
            raise ValueError(
                "binance_ws: HMAC credentials are required when "
                "trading_required=True. Provide them via "
                "`connect binance_ws` on the Hummingbot CLI or copy "
                "from the existing binance.yml via "
                "tools/binance_ws_register.py."
            )

        # Allow env-var override of the revert flag. Useful for
        # quickly toggling REST fallback without re-running the
        # encrypted-config flow.
        env_override = os.environ.get("BINANCE_WS_USE_WS_TRADING")
        if env_override is not None:
            use_ws_trading = env_override.strip().lower() not in ("0", "false", "no", "off", "")

        self._use_ws_trading = use_ws_trading
        self._router: Optional[BinanceWsRequestRouter] = None

        super().__init__(
            binance_api_key=binance_ws_api_key,
            binance_api_secret=binance_ws_api_secret,
            balance_asset_limit=balance_asset_limit,
            rate_limits_share_pct=rate_limits_share_pct,
            trading_pairs=trading_pairs,
            trading_required=trading_required,
            domain=domain,
        )

        # Construct the router *after* super().__init__ so
        # self._throttler and self._time_synchronizer exist.
        if self._use_ws_trading:
            self._router = BinanceWsRequestRouter(
                api_key=binance_ws_api_key,
                api_secret=binance_ws_api_secret,
                throttler=self._throttler,
                time_provider=self._time_synchronizer.time,
                domain=domain,
            )

        self._log_ws_boot_state()

    # ------------------------------------------------------------------
    # Identity / config
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        if self._domain == "com":
            return "binance_ws"
        return f"binance_ws_{self._domain}"

    @property
    def rate_limits_rules(self):
        # Extend (not replace) the inherited REST table so all WS-method
        # limit ids exist in the same throttler instance the parent uses.
        return list(BINANCE_CONSTANTS.RATE_LIMITS) + build_rate_limits()

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    async def start_network(self):
        await super().start_network()
        await self._start_ws_trading()

    async def stop_network(self):
        await self._stop_ws_trading()
        await super().stop_network()

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def check_network(self) -> NetworkStatus:
        rest_status = await super().check_network()
        if rest_status != NetworkStatus.CONNECTED:
            return rest_status
        # When WS trading is active, healthy means REST + router both up.
        if self._use_ws_trading and self._router is not None and not self._router.connected:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED
