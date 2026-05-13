"""Exchange class for the ``binance_sbe`` connector.

Inherits the entire :class:`BinanceExchange` — REST trading, user stream,
auth, rate limits, fees, exchange-info, etc. The only override is the
order-book data source factory: we substitute the SBE binary variant.

Optional credentials model:
  - ``binance_sbe_api_key`` is REQUIRED — the Ed25519 API key string used
    for the public SBE market data WebSocket.
  - ``binance_api_key`` / ``binance_api_secret`` are OPTIONAL — only
    needed when this connector is used in a trading role. We validate
    their presence early (fail-loud at boot) when ``trading_required``
    is set, so the operator gets an actionable error rather than a
    silent auth failure at first order submission.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.connector.exchange.binance import binance_constants as CONSTANTS
from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange
from hummingbot.connector.exchange.binance_sbe.binance_sbe_api_order_book_data_source import (
    BinanceSbeAPIOrderBookDataSource,
)
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource


# Env var consulted as a fallback when the constructor's ``binance_sbe_api_key``
# is empty. The shadow tools and the start script already source ``.env``
# (see :mod:`tools.binance_sbe_shadow` and ``start_xemm_lead_lag.sh``), so by
# the time the Hummingbot framework instantiates this connector this variable
# is already in ``os.environ``. Falling back to it here means a deployment
# can choose between two credential paths:
#
#   1. Hummingbot's encrypted ``conf/connectors/binance_sbe.yml`` (via
#      ``connect binance_sbe`` on the CLI). The framework reads it and
#      passes ``binance_sbe_api_key`` as a constructor kwarg — same path
#      used by every other connector.
#
#   2. A plaintext ``.env`` file at the repo root (gitignored). Simpler
#      for headless deployments where running the interactive ``connect``
#      flow is awkward. Lower security guarantees than the encrypted path
#      because the file isn't password-protected — operator's choice.
_SBE_API_KEY_ENV_VAR = "BINANCE_SBE_API_KEY"


class BinanceSbeExchange(BinanceExchange):
    """Binance Spot connector that consumes the SBE market data stream.

    Trading paths remain on the public REST/JSON connector — SBE on the
    Binance side covers only public market data (trade/depth/bestBidAsk),
    not order placement.
    """

    def __init__(self,
                 binance_sbe_api_key: str = "",
                 binance_api_key: Optional[str] = None,
                 binance_api_secret: Optional[str] = None,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        if trading_required and (not binance_api_key or not binance_api_secret):
            # Fail-loud at boot. The alternative — letting the inherited
            # BinanceExchange.__init__ proceed with empty strings — would
            # produce an opaque HTTP 401 on the first order placement,
            # which is much harder to diagnose in live operations.
            raise ValueError(
                "binance_sbe requires HMAC binance_api_key + binance_api_secret "
                "when trading_required=True. For signal-only roles "
                "(signal_connector) configure with trading_required=False "
                "and omit the HMAC credentials."
            )

        # Resolve the SBE API key. The constructor arg wins (Hummingbot's
        # standard encrypted-config path); fall back to the env var if
        # the kwarg is empty. Either route must yield a non-empty string
        # — fail loud here rather than hitting an opaque WS 401 later.
        if not binance_sbe_api_key:
            binance_sbe_api_key = os.environ.get(_SBE_API_KEY_ENV_VAR, "")
        if not binance_sbe_api_key:
            raise ValueError(
                "binance_sbe requires an Ed25519 API key string. "
                "Provide it either via Hummingbot's encrypted config "
                "(run `connect binance_sbe` on the CLI) OR by exporting "
                f"the {_SBE_API_KEY_ENV_VAR} environment variable "
                "(e.g. via the repo's .env file)."
            )
        self._sbe_api_key = binance_sbe_api_key

        # Pass through the HMAC creds (possibly empty strings, mirroring
        # how BinanceExchange handles read-only mode). The framework's
        # `trading_required` flag governs whether those strings are
        # actually used downstream.
        super().__init__(
            binance_api_key=binance_api_key or "",
            binance_api_secret=binance_api_secret or "",
            balance_asset_limit=balance_asset_limit,
            rate_limits_share_pct=rate_limits_share_pct,
            trading_pairs=trading_pairs,
            trading_required=trading_required,
            domain=domain,
        )

    @property
    def name(self) -> str:
        # Distinct from the JSON connector's "binance" so the framework
        # resolves them as separate instances. The trailing _<domain>
        # suffix (e.g. binance_sbe_us) keeps non-com regions addressable
        # if Binance ever expands SBE beyond .com.
        if self._domain == "com":
            return "binance_sbe"
        return f"binance_sbe_{self._domain}"

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        # The single line that makes this a different connector.
        return BinanceSbeAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            sbe_api_key=self._sbe_api_key,
            domain=self.domain,
        )
