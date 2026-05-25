"""Shared fixtures for dashboard_api tests.

Defines a minimal in-memory ``FakeAdapter`` that satisfies
``DashboardBotAdapter`` without touching real Hummingbot controllers.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytest


class FakeAdapter:
    """Programmable adapter for server tests.

    Records all command invocations in ``self.calls`` so tests can assert
    on side effects. Fields can be overridden per-test by mutating the
    public attributes (``_status``, ``_balances``, etc.).
    """
    def __init__(self, bot_name: str = "test_bot"):
        self.bot_name = bot_name
        self._status = "running"
        self._status_meta: Dict[str, Any] = {
            "reason": None, "requester": None,
            "lastUpdated": None, "stopped": False,
        }
        self._info: Dict[str, Any] = {"quote": "USD", "bases": ["BTC"]}
        self._balances: Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Optional[datetime]] = (
            {"BTC": 1.0, "USD": 1000.0},
            {"BTC": 0.5, "USD": 500.0},
            {"BTC": 1.5, "USD": 1500.0},
            datetime(2026, 5, 18, 15, 0, 0, tzinfo=timezone.utc),
        )
        self._prices: Dict[str, Any] = {
            "venueA": {"BTC": {"bid": 50000.0, "ask": 50100.0}},
            "venueB": {"BTC": {"bid": 49900.0, "ask": 50050.0}},
        }
        self.calls: List[Dict[str, Any]] = []
        self._raise_on: Optional[str] = None  # method name to raise from
        self._books_payload: Dict[str, Any] = {"books": []}
        self._spec_orders_payload: Dict[str, Any] = {"total": 0}

    # ---- Read getters --------------------------------------------------
    def _maybe_raise(self, name: str) -> None:
        if self._raise_on == name:
            raise RuntimeError(f"FakeAdapter forced failure in {name}")

    def get_status(self) -> str:
        self._maybe_raise("get_status")
        return self._status

    def get_status_metadata(self) -> Dict[str, Any]:
        self._maybe_raise("get_status_metadata")
        return dict(self._status_meta)

    def get_info(self) -> Dict[str, Any]:
        self._maybe_raise("get_info")
        return dict(self._info)

    def get_balances(self):
        self._maybe_raise("get_balances")
        return self._balances

    def get_prices(self) -> Dict[str, Any]:
        self._maybe_raise("get_prices")
        return self._prices

    def get_bot_version(self) -> str:
        return "fake-1.0"

    def get_uptime_sec(self) -> float:
        return 42.0

    def get_avg_cycle_duration_ms(self) -> int:
        return 1000

    # ---- Commands ------------------------------------------------------
    async def pause(self, reason, requester, force):
        self.calls.append({"op": "pause", "reason": reason, "requester": requester, "force": force})
        return {"status": "ok"}

    async def resume(self, requester):
        self.calls.append({"op": "resume", "requester": requester})
        return {"status": "ok"}

    async def kill(self, reason, requester):
        self.calls.append({"op": "kill", "reason": reason, "requester": requester})
        return {"status": "scheduled"}

    async def reset(self, requester):
        self.calls.append({"op": "reset", "requester": requester})
        return {"status": "scheduled"}

    async def settle(self, params, requester):
        raise NotImplementedError("settle disabled in FakeAdapter")

    # ---- Aux endpoints --------------------------------------------------
    async def get_books(self, pair, exchanges, depth, apply_spread):
        self.calls.append({"op": "get_books", "pair": pair, "exchanges": exchanges,
                           "depth": depth, "apply_spread": apply_spread})
        return self._books_payload

    async def get_spec_orders(self):
        self.calls.append({"op": "get_spec_orders"})
        return self._spec_orders_payload


@pytest.fixture
def fake_adapter():
    return FakeAdapter()


@pytest.fixture
def registry(fake_adapter):
    return {fake_adapter.bot_name: fake_adapter}
