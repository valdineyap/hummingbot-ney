"""HTTP integration tests for hummingbot.dashboard_api.server.

Uses aiohttp's TestClient to drive the routes end-to-end against a
FakeAdapter (see conftest.py).
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from hummingbot.dashboard_api import server


# ---------------------------------------------------------------------------
# Test client fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client(registry: Dict[str, Any]):
    app = server.build_app(registry=registry, auth_token=None)
    async with TestClient(TestServer(app)) as c:
        yield c


@pytest_asyncio.fixture
async def client_with_auth(registry: Dict[str, Any]):
    app = server.build_app(registry=registry, auth_token="secret-token")
    async with TestClient(TestServer(app)) as c:
        yield c


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health_returns_basic_payload(client):
    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["ok"] is True
    assert data["botName"] == "test_bot"
    assert data["status"] == "running"
    assert data["apiVersion"] == "bitbots-v1-compat"
    assert "dashboardApiVersion" in data
    assert "botVersion" in data
    assert "uptimeSec" in data


@pytest.mark.asyncio
async def test_health_works_without_auth_even_when_token_set(client_with_auth):
    """/health must always be reachable for systemd/monitor probes."""
    resp = await client_with_auth.get("/health")
    assert resp.status == 200


# ---------------------------------------------------------------------------
# /api/v1/getdata
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_getdata_404_for_unknown_bot(client):
    resp = await client.get("/api/v1/getdata/unknown_bot")
    assert resp.status == 404


@pytest.mark.asyncio
async def test_getdata_returns_full_payload(client):
    resp = await client.get("/api/v1/getdata/test_bot")
    assert resp.status == 200
    data = await resp.json()
    # Spot-check the bitbots v1 schema is present.
    for key in ("exchName", "status", "info", "exchangeBalance",
                "bitprecoBalance", "totalBalance", "lastOnline",
                "statusMetadata", "botVersion", "avgCycleDuration"):
        assert key in data, f"missing field {key}"
    assert data["exchName"] == "test_bot"
    assert data["status"] == "running"
    assert data["lastOnline"].endswith("Z")  # bitbots format
    assert data["exchangeBalance"] == {"BTC": 1.0, "USD": 1000.0}


@pytest.mark.asyncio
async def test_getdata_best_effort_partial_failure(client, fake_adapter):
    """When one field's getter raises, the endpoint still returns 200 and
    injects the failure into statusMetadata.reason."""
    fake_adapter._raise_on = "get_balances"
    resp = await client.get("/api/v1/getdata/test_bot")
    assert resp.status == 200
    data = await resp.json()
    # Balances default to empty dict, but other fields still populated.
    assert data["exchangeBalance"] == {}
    assert data["status"] == "running"
    # Reason mentions the failed field.
    reason = (data.get("statusMetadata") or {}).get("reason", "") or ""
    assert "balances" in reason


@pytest.mark.asyncio
async def test_getdata_never_500s_on_adapter_explosion(client, fake_adapter):
    """Even if multiple fields fail, the response stays 200."""
    # Make every read getter raise.
    original_status = fake_adapter.get_status
    original_meta = fake_adapter.get_status_metadata
    original_info = fake_adapter.get_info

    def boom_status():
        raise RuntimeError("status fail")
    def boom_meta():
        raise RuntimeError("meta fail")
    def boom_info():
        raise RuntimeError("info fail")

    fake_adapter.get_status = boom_status
    fake_adapter.get_status_metadata = boom_meta
    fake_adapter.get_info = boom_info
    fake_adapter._raise_on = "get_balances"

    try:
        resp = await client.get("/api/v1/getdata/test_bot")
        assert resp.status == 200
        data = await resp.json()
        # All defaulted but the request never escalated.
        assert data["status"] == "errored"  # default
        assert data["info"] == {}
        assert data["exchangeBalance"] == {}
    finally:
        fake_adapter.get_status = original_status
        fake_adapter.get_status_metadata = original_meta
        fake_adapter.get_info = original_info


# ---------------------------------------------------------------------------
# /api/v1/bot-command
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bot_command_pause(client, fake_adapter):
    resp = await client.post(
        "/api/v1/bot-command/test_bot",
        json={"botCommand": "pause", "user": "valdiney"},
    )
    assert resp.status == 201
    assert any(c["op"] == "pause" for c in fake_adapter.calls)


@pytest.mark.asyncio
async def test_bot_command_resume(client, fake_adapter):
    resp = await client.post(
        "/api/v1/bot-command/test_bot",
        json={"botCommand": "resume", "user": "alice"},
    )
    assert resp.status == 201
    assert any(c["op"] == "resume" and c["requester"] == "alice"
               for c in fake_adapter.calls)


@pytest.mark.asyncio
async def test_bot_command_kill_returns_202(client, fake_adapter):
    resp = await client.post(
        "/api/v1/bot-command/test_bot",
        json={"botCommand": "kill", "user": "v"},
    )
    assert resp.status == 202
    data = await resp.json()
    assert data["status"] == "scheduled" or data["status"] == 202
    assert any(c["op"] == "kill" for c in fake_adapter.calls)


@pytest.mark.asyncio
async def test_bot_command_reset_returns_202(client, fake_adapter):
    resp = await client.post(
        "/api/v1/bot-command/test_bot",
        json={"botCommand": "reset", "user": "v"},
    )
    assert resp.status == 202
    assert any(c["op"] == "reset" for c in fake_adapter.calls)


@pytest.mark.asyncio
async def test_bot_command_settle_returns_501(client):
    resp = await client.post(
        "/api/v1/bot-command/test_bot",
        json={"botCommand": "settle", "user": "v", "params": {"exchangeName": "x"}},
    )
    assert resp.status == 501


@pytest.mark.asyncio
async def test_bot_command_400_on_unknown(client):
    resp = await client.post(
        "/api/v1/bot-command/test_bot",
        json={"botCommand": "foobar"},
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_bot_command_400_on_missing_body(client):
    resp = await client.post("/api/v1/bot-command/test_bot", json={})
    assert resp.status == 400


@pytest.mark.asyncio
async def test_bot_command_404_unknown_bot(client):
    resp = await client.post(
        "/api/v1/bot-command/missing",
        json={"botCommand": "pause"},
    )
    assert resp.status == 404


# ---------------------------------------------------------------------------
# /api/v1/getBooks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_books_calls_adapter_with_clamped_depth(client, fake_adapter):
    fake_adapter._books_payload = {"books": [{"exchangeNumber": 1, "book": {"bids": [], "asks": []}}]}
    resp = await client.get("/api/v1/getBooks/test_bot/BTC-USD?depth=9999")
    assert resp.status == 200
    call = next(c for c in fake_adapter.calls if c["op"] == "get_books")
    assert call["pair"] == "BTC-USD"
    assert call["depth"] == 100  # clamped


@pytest.mark.asyncio
async def test_get_books_normalizes_pair_to_uppercase(client, fake_adapter):
    fake_adapter._books_payload = {"books": []}
    resp = await client.get("/api/v1/getBooks/test_bot/btc-usd")
    assert resp.status == 200
    call = next(c for c in fake_adapter.calls if c["op"] == "get_books")
    assert call["pair"] == "BTC-USD"


@pytest.mark.asyncio
async def test_get_books_400_on_malformed_pair(client):
    resp = await client.get("/api/v1/getBooks/test_bot/INVALID")
    assert resp.status == 400


@pytest.mark.asyncio
async def test_get_books_parses_exchanges_csv(client, fake_adapter):
    fake_adapter._books_payload = {"books": []}
    resp = await client.get("/api/v1/getBooks/test_bot/BTC-USD?exchanges=1,2")
    assert resp.status == 200
    call = next(c for c in fake_adapter.calls if c["op"] == "get_books")
    assert call["exchanges"] == [1, 2]


# ---------------------------------------------------------------------------
# /api/v1/specOrders
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_spec_orders_returns_adapter_payload(client, fake_adapter):
    fake_adapter._spec_orders_payload = {"total": 3, "exchange1": {"orders": []}}
    resp = await client.get("/api/v1/specOrders/test_bot")
    assert resp.status == 200
    data = await resp.json()
    assert data["total"] == 3


# ---------------------------------------------------------------------------
# 501 stubs for unimplemented endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_place_order_returns_501(client):
    resp = await client.post(
        "/api/v1/placeOrder/test_bot",
        json={"side": "BUY", "base": "BTC", "amount": "0.1"},
    )
    assert resp.status == 501


@pytest.mark.asyncio
async def test_transfers_returns_501(client):
    resp = await client.post(
        "/api/v1/bots/test_bot/transfers",
        json={"from": "a", "to": "b", "amount": 1},
    )
    assert resp.status == 501


@pytest.mark.asyncio
async def test_get_op_price_returns_501(client):
    resp = await client.get("/api/v1/getOpPrice/test_bot/BTC-USD/buy/0.1")
    assert resp.status == 501


# ---------------------------------------------------------------------------
# Auth middleware
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auth_401_when_missing_header(client_with_auth):
    resp = await client_with_auth.get("/api/v1/getdata/test_bot")
    assert resp.status == 401


@pytest.mark.asyncio
async def test_auth_401_when_wrong_token(client_with_auth):
    resp = await client_with_auth.get(
        "/api/v1/getdata/test_bot",
        headers={"Authorization": "Bearer wrong"},
    )
    assert resp.status == 401


@pytest.mark.asyncio
async def test_auth_200_with_correct_token(client_with_auth):
    resp = await client_with_auth.get(
        "/api/v1/getdata/test_bot",
        headers={"Authorization": "Bearer secret-token"},
    )
    assert resp.status == 200


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_50_concurrent_getdata_requests(client):
    """Server stays correct under modest concurrency."""
    coros = [client.get("/api/v1/getdata/test_bot") for _ in range(50)]
    results = await asyncio.gather(*coros)
    statuses = [r.status for r in results]
    for r in results:
        await r.release()
    assert all(s == 200 for s in statuses), statuses
