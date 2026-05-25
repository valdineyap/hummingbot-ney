"""Protocol that any controller-specific adapter must satisfy.

The HTTP server (`server.py`) talks to adapters through this Protocol only —
it has no knowledge of any specific strategy. A controller plugs in by
implementing this Protocol (usually by subclassing `BaseDashboardAdapter`,
which provides sensible defaults for every method).

Methods marked OPTIONAL may raise NotImplementedError; the server translates
that to HTTP 501. Methods marked REQUIRED must always succeed (they may
return None / empty containers, but must not raise).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class DashboardBotAdapter(Protocol):
    """Bitbots-v1-compat adapter contract.

    A controller exposes itself to the Dashboard API by providing an instance
    of this Protocol (typically via `BaseDashboardAdapter` or a subclass).
    """

    # ------------------------------------------------------------------ #
    # Identity                                                           #
    # ------------------------------------------------------------------ #
    bot_name: str
    """The identifier the dashboard uses to address this bot. Must match
    the `botName` field in the dashboard's `config/prod.yml` entry."""

    # ------------------------------------------------------------------ #
    # Read-only getters (sync — called from event loop, must be cheap)   #
    # ------------------------------------------------------------------ #
    def get_status(self) -> str:
        """REQUIRED. Returns one of: ``running`` | ``paused`` | ``errored``."""
        ...

    def get_status_metadata(self) -> Dict[str, Any]:
        """REQUIRED. Returns a dict with keys ``reason``, ``requester``,
        ``lastUpdated``, ``stopped`` (last command + derived health info)."""
        ...

    def get_info(self) -> Dict[str, Any]:
        """REQUIRED. Returns the ``info`` block of the bitbots v1 ``getdata``
        payload (quote, bases, spreads, targets, exchanges, wallets, config,
        prices). Strategy-specific vocabulary; subclass overrides this."""
        ...

    def get_balances(self) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Optional[datetime]]:
        """REQUIRED. Returns ``(exchange1_balance, exchange2_balance,
        total_balance, last_updated)``. Errors per-exchange must yield
        empty dict, not raise."""
        ...

    def get_prices(self) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
        """REQUIRED. Returns ``{exchange_name: {base: {bid, ask}}}``.
        Missing/empty books must yield ``{bid: None, ask: None}``."""
        ...

    def get_bot_version(self) -> str:
        """REQUIRED. String identifying the bot binary/build."""
        ...

    def get_uptime_sec(self) -> float:
        """REQUIRED. Seconds since the adapter was instantiated."""
        ...

    def get_avg_cycle_duration_ms(self) -> int:
        """REQUIRED. Bitbots schema field; defaults to 1000 in BaseAdapter."""
        ...

    # ------------------------------------------------------------------ #
    # Commands (async — may persist state, may schedule SIGTERM)         #
    # ------------------------------------------------------------------ #
    async def pause(self, reason: str, requester: Optional[str], force: bool) -> Dict[str, Any]:
        """REQUIRED. Pause trading (sticky)."""
        ...

    async def resume(self, requester: Optional[str]) -> Dict[str, Any]:
        """REQUIRED. Resume trading."""
        ...

    async def kill(self, reason: str, requester: Optional[str]) -> Dict[str, Any]:
        """REQUIRED. Schedule SIGTERM (idempotent). Must return BEFORE the
        signal fires so the HTTP response reaches the dashboard. Subsequent
        calls while a kill is scheduled return ``{status: "already_scheduled"}``."""
        ...

    async def reset(self, requester: Optional[str]) -> Dict[str, Any]:
        """REQUIRED. Same semantics as ``kill`` by default."""
        ...

    async def settle(self, params: Dict[str, Any], requester: Optional[str]) -> Dict[str, Any]:
        """OPTIONAL. Raise NotImplementedError → HTTP 501."""
        ...

    # ------------------------------------------------------------------ #
    # Auxiliary endpoints                                                #
    # ------------------------------------------------------------------ #
    async def get_books(
        self, pair: str, exchanges: Optional[List[int]], depth: int, apply_spread: bool
    ) -> Dict[str, Any]:
        """REQUIRED. Returns the ``getBooks`` payload (bitbots schema).
        Unavailable books yield exchange entries with empty ``bids``/``asks``,
        not omission."""
        ...

    async def get_spec_orders(self) -> Dict[str, Any]:
        """REQUIRED. Returns active executors / open orders partitioned by
        exchange (bitbots ``specOrders`` schema)."""
        ...
