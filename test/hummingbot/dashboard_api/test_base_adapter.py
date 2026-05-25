"""Tests for hummingbot.dashboard_api.base_adapter.

These tests exercise BaseDashboardAdapter against a minimal "stub" object
that mimics the public surface of any Hummingbot ControllerBase
(``config``, ``market_data_provider.connectors``, ``get_active_executors``).

This validates the plugin invariant: **a strategy that does NOT subclass
BaseDashboardAdapter must still get a working dashboard out of the box.**
"""
from __future__ import annotations

import asyncio
import os
import signal
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Dict
from unittest.mock import patch

import pytest

from hummingbot.dashboard_api.base_adapter import BaseDashboardAdapter


# ---------------------------------------------------------------------------
# Fake connector / order book primitives
# ---------------------------------------------------------------------------

class _Row:
    def __init__(self, price: float, amount: float):
        self.price = price
        self.amount = amount


class _FakeOrderBook:
    def __init__(self, bids, asks):
        self._bids = [_Row(p, a) for p, a in bids]
        self._asks = [_Row(p, a) for p, a in asks]

    def bid_entries(self):
        return iter(self._bids)

    def ask_entries(self):
        return iter(self._asks)


class _FakeConnector:
    def __init__(self, name: str, balances: Dict[str, float], books: Dict[str, _FakeOrderBook]):
        self.name = name
        self._balances = balances
        self._books = books
        self.trading_pairs = list(books.keys())

    def get_all_balances(self) -> Dict[str, float]:
        return dict(self._balances)

    def get_order_book(self, pair: str):
        return self._books.get(pair)


def _make_stub_controller(tmp_path: Path, pause_path: str = None):
    """Bare ControllerBase-like stub. No real Hummingbot setup needed."""
    binance = _FakeConnector(
        "binance",
        balances={"BTC": 1.0, "USD": 1000.0},
        books={"BTC-USD": _FakeOrderBook([(50000, 1), (49990, 2)], [(50100, 1), (50110, 2)])},
    )
    bitpreco = _FakeConnector(
        "bitpreco",
        balances={"BTC": 0.5, "BRL": 5000.0},
        books={"BTC-USD": _FakeOrderBook([(49900, 1)], [(50050, 1)])},
    )
    market_data_provider = SimpleNamespace(connectors={"binance": binance, "bitpreco": bitpreco})
    config = SimpleNamespace(id="stub_bot")
    return SimpleNamespace(
        config=config,
        market_data_provider=market_data_provider,
        get_active_executors=lambda: [],
        _kill_reason=None,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_base_adapter_works_against_minimal_stub(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    assert adapter.bot_name == "stub_bot"
    assert adapter.get_uptime_sec() >= 0
    assert adapter.get_bot_version() in ("hummingbot-ney", os.environ.get("BOT_VERSION", "hummingbot-ney"))


# ---------------------------------------------------------------------------
# get_status (default mapping)
# ---------------------------------------------------------------------------

def test_get_status_running_by_default(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    assert adapter.get_status() == "running"


def test_get_status_paused_when_pause_file_exists(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    Path(adapter._pause_file_path()).touch()
    try:
        assert adapter.get_status() == "paused"
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)


def test_get_status_errored_when_kill_reason(tmp_path):
    controller = _make_stub_controller(tmp_path)
    controller._kill_reason = "DAILY_LOSS"
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    assert adapter.get_status() == "errored"


# ---------------------------------------------------------------------------
# get_balances
# ---------------------------------------------------------------------------

def test_get_balances_partitions_by_role_and_sums_total(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    exch1, exch2, total, last_updated = adapter.get_balances()
    assert exch1 == {"BTC": 1.0, "USD": 1000.0}
    assert exch2 == {"BTC": 0.5, "BRL": 5000.0}
    # Total is element-wise UNION (BRL exists only in exch2).
    assert total == {"BTC": 1.5, "USD": 1000.0, "BRL": 5000.0}


def test_get_balances_swallows_per_connector_error(tmp_path):
    controller = _make_stub_controller(tmp_path)
    # Make first connector explode.
    controller.market_data_provider.connectors["binance"].get_all_balances = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    exch1, exch2, total, _ = adapter.get_balances()
    assert exch1 == {}  # error swallowed
    assert exch2 == {"BTC": 0.5, "BRL": 5000.0}
    assert total == {"BTC": 0.5, "BRL": 5000.0}


def test_get_balances_stamps_last_updated_when_data_present(tmp_path):
    """`balanceLastUpdated` must reflect the time we last observed any
    balance — used by the dashboard to show freshness."""
    from datetime import datetime, timezone
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    before = datetime.now(timezone.utc)
    _, _, _, ts = adapter.get_balances()
    after = datetime.now(timezone.utc)
    assert ts is not None
    assert ts.tzinfo is not None  # tz-aware
    assert before <= ts <= after


def test_get_balances_keeps_prior_timestamp_when_all_connectors_fail(tmp_path):
    """If a transient outage makes both connectors return empty/error, we
    must NOT pretend a fresh read happened — preserve the previous stamp
    so the dashboard can see staleness."""
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    # First call: succeeds, sets timestamp.
    _, _, _, first_ts = adapter.get_balances()
    assert first_ts is not None
    # Now break both connectors.
    for name in ("binance", "bitpreco"):
        controller.market_data_provider.connectors[name].get_all_balances = (
            lambda: (_ for _ in ()).throw(RuntimeError("outage"))
        )
    _, _, _, second_ts = adapter.get_balances()
    # Same stamp preserved (didn't refresh to now()).
    assert second_ts == first_ts


def test_get_balances_returns_none_timestamp_before_first_read(tmp_path):
    """No reads yet → None (matches bitbots semantics: no value at boot)."""
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    assert adapter._last_balances_at is None


# ---------------------------------------------------------------------------
# get_prices
# ---------------------------------------------------------------------------

def test_get_prices_top_of_book(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    prices = adapter.get_prices()
    assert prices["binance"]["BTC"] == {"bid": 50000.0, "ask": 50100.0}
    assert prices["bitpreco"]["BTC"] == {"bid": 49900.0, "ask": 50050.0}


def test_get_prices_handles_missing_book(tmp_path):
    controller = _make_stub_controller(tmp_path)
    controller.market_data_provider.connectors["binance"]._books = {}
    controller.market_data_provider.connectors["binance"].trading_pairs = ["BTC-USD"]
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    prices = adapter.get_prices()
    assert prices["binance"]["BTC"] == {"bid": None, "ask": None}


# ---------------------------------------------------------------------------
# get_books (the auxiliary endpoint)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_books_returns_both_exchanges_even_if_one_is_empty(tmp_path):
    controller = _make_stub_controller(tmp_path)
    # Make second exchange book unavailable.
    controller.market_data_provider.connectors["bitpreco"]._books = {}
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    result = await adapter.get_books("BTC-USD", exchanges=None, depth=5, apply_spread=False)
    books = result["books"]
    assert len(books) == 2
    # exchange 1 (binance) populated
    e1 = next(b for b in books if b["exchangeNumber"] == 1)
    assert len(e1["book"]["bids"]) > 0
    # exchange 2 (bitpreco) empty book — but ENTRY PRESENT (compat invariant)
    e2 = next(b for b in books if b["exchangeNumber"] == 2)
    assert e2["book"] == {"bids": [], "asks": []}


@pytest.mark.asyncio
async def test_get_books_respects_depth(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    result = await adapter.get_books("BTC-USD", exchanges=[1], depth=1, apply_spread=False)
    e1 = next(b for b in result["books"] if b["exchangeNumber"] == 1)
    assert len(e1["book"]["bids"]) == 1
    assert len(e1["book"]["asks"]) == 1


# ---------------------------------------------------------------------------
# get_spec_orders
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_spec_orders_partitions_by_connector(tmp_path):
    controller = _make_stub_controller(tmp_path)
    ex1 = SimpleNamespace(
        id="e-1", connector_name="binance", trading_pair="BTC-USD",
        side="BUY", status="RUNNING", type="PositionExecutor",
        config=SimpleNamespace(amount=0.001, entry_price=50000),
    )
    ex2 = SimpleNamespace(
        id="e-2", connector_name="bitpreco", trading_pair="BTC-BRL",
        side="SELL", status="RUNNING", type="PositionExecutor",
        config=SimpleNamespace(amount=0.002, entry_price=300000),
    )
    controller.get_active_executors = lambda: [ex1, ex2]
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    result = await adapter.get_spec_orders()
    assert result["total"] == 2
    assert result["exchange1"]["count"] == 1
    assert result["exchange1"]["orders"][0]["executorId"] == "e-1"
    assert result["exchange2"]["count"] == 1


# ---------------------------------------------------------------------------
# Commands: pause / resume
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pause_creates_file_and_persists_meta(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    pause_path = Path(adapter._pause_file_path())
    pause_path.unlink(missing_ok=True)
    try:
        await adapter.pause("manual", "valdiney", force=False)
        assert pause_path.exists()
        # Persisted file exists in tmp_path:
        status_file = tmp_path / "dashboard_status_stub_bot.json"
        assert status_file.exists()
    finally:
        pause_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_resume_removes_file_idempotent(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    pause_path = Path(adapter._pause_file_path())
    pause_path.unlink(missing_ok=True)
    # No file → resume must still succeed.
    res = await adapter.resume("alice")
    assert res["status"] == "ok"


@pytest.mark.asyncio
async def test_resume_clears_reason(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    await adapter.pause("manual", "valdiney", force=False)
    assert adapter._status_meta["reason"] is not None
    await adapter.resume("valdiney")
    assert adapter._status_meta["reason"] is None
    assert adapter._status_meta["stopped"] is False


# ---------------------------------------------------------------------------
# Commands: kill (idempotent, deferred SIGTERM)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_kill_returns_scheduled_before_sigterm(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    with patch("hummingbot.dashboard_api.base_adapter.os.kill") as mock_kill:
        result = await adapter.kill("test", "valdiney")
        assert result["status"] == "scheduled"
        mock_kill.assert_not_called()  # still scheduled, not yet fired
        # Advance time so call_later fires.
        await asyncio.sleep(0.30)
        mock_kill.assert_called_once_with(os.getpid(), signal.SIGTERM)


@pytest.mark.asyncio
async def test_kill_is_idempotent(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    with patch("hummingbot.dashboard_api.base_adapter.os.kill") as mock_kill:
        first = await adapter.kill("test", "v")
        second = await adapter.kill("test", "v")
        third = await adapter.reset("v")
        assert first["status"] == "scheduled"
        assert second["status"] == "already_scheduled"
        assert third["status"] == "already_scheduled"
        await asyncio.sleep(0.30)
        # Only the first SIGTERM scheduled.
        assert mock_kill.call_count == 1


@pytest.mark.asyncio
async def test_kill_persists_meta_before_signal(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    with patch("hummingbot.dashboard_api.base_adapter.os.kill"):
        await adapter.kill("manual reset", "v")
        # Cancel the scheduled call so it doesn't fire during teardown.
        if adapter._kill_handle:
            adapter._kill_handle.cancel()
    status_file = tmp_path / "dashboard_status_stub_bot.json"
    assert status_file.exists()
    contents = status_file.read_text()
    assert "killed via API" in contents


# ---------------------------------------------------------------------------
# settle → NotImplementedError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_settle_raises_not_implemented(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)
    with pytest.raises(NotImplementedError):
        await adapter.settle({}, "v")


# ---------------------------------------------------------------------------
# Lifecycle hooks (_before_pause, _after_resume, _before_kill)
# ---------------------------------------------------------------------------
#
# These tests target the wrapper logic in BaseDashboardAdapter._run_hook —
# subclasses' actual cancel/audit logic is tested in
# test_xemm_lead_lag_dashboard.py. Here we verify:
#   * Hooks are called in the right order relative to file ops / SIGTERM.
#   * Hook errors / timeouts are caught and annotated in statusMetadata.
#   * Phase timings flow into the response timings dict.
#   * Touch / unlink / SIGTERM happen ALWAYS, even if the hook fails.

import asyncio as _asyncio  # noqa: E402
import json as _json  # noqa: E402


class _AdapterWithCallableHooks(BaseDashboardAdapter):
    """BaseDashboardAdapter subclass with hooks driven by callable factories
    set per test. Lets us inject custom behavior (record calls, raise, sleep)
    without writing a new subclass per scenario."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.before_pause = lambda: None
        self.after_resume = lambda: None
        self.before_kill = lambda: None

    async def _before_pause(self):
        result = self.before_pause()
        if _asyncio.iscoroutine(result):
            await result

    async def _after_resume(self):
        result = self.after_resume()
        if _asyncio.iscoroutine(result):
            await result

    async def _before_kill(self):
        result = self.before_kill()
        if _asyncio.iscoroutine(result):
            await result


@pytest.mark.asyncio
async def test_pause_calls_before_pause_hook_then_touches_file(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = _AdapterWithCallableHooks(controller, data_dir=tmp_path)
    order = []

    async def hook():
        order.append("before_pause")
        # Hook must run BEFORE the pause file is touched.
        assert not Path(adapter._pause_file_path()).exists()

    adapter.before_pause = hook
    try:
        await adapter.pause("manual", "v", force=False)
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)
    assert order == ["before_pause"]


@pytest.mark.asyncio
async def test_pause_hook_failure_still_touches_pause_file(tmp_path):
    """If _before_pause raises, the touch must still happen and HTTP returns ok
    — we honor the user's intent to pause even when cancel/audit fail."""
    controller = _make_stub_controller(tmp_path)
    adapter = _AdapterWithCallableHooks(controller, data_dir=tmp_path)

    def boom():
        raise RuntimeError("cancel failed")

    adapter.before_pause = boom
    try:
        result = await adapter.pause("manual", "v", force=False)
        assert result["status"] == "ok"
        assert Path(adapter._pause_file_path()).exists()
        # statusMetadata gets the failure suffix.
        meta = adapter._status_meta
        assert "hook incomplete" in (meta.get("reason") or "")
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_pause_hook_timeout_is_capped(tmp_path):
    """Slow hooks are capped by HOOK_TIMEOUT_SEC."""
    controller = _make_stub_controller(tmp_path)
    adapter = _AdapterWithCallableHooks(controller, data_dir=tmp_path)
    adapter.HOOK_TIMEOUT_SEC = 0.05  # 50ms cap for this test

    async def slow():
        await _asyncio.sleep(0.5)

    adapter.before_pause = slow
    started = time.monotonic()
    try:
        result = await adapter.pause("manual", "v", force=False)
        elapsed = time.monotonic() - started
        assert elapsed < 0.3, f"timeout not enforced ({elapsed}s)"
        assert result["status"] == "ok"
        # File touched despite timeout.
        assert Path(adapter._pause_file_path()).exists()
        # Reason mentions timeout.
        assert "timeout" in (adapter._status_meta.get("reason") or "")
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_resume_calls_after_resume_hook_then_clears_reason(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = _AdapterWithCallableHooks(controller, data_dir=tmp_path)
    Path(adapter._pause_file_path()).touch()
    order = []

    async def hook():
        order.append("after_resume")
        # By the time after_resume runs, the file should already be gone.
        assert not Path(adapter._pause_file_path()).exists()

    adapter.after_resume = hook
    # Set a stale reason first so we can verify it's cleared.
    adapter._status_meta["reason"] = "paused via API by X"
    await adapter.resume("v")
    assert order == ["after_resume"]
    assert adapter._status_meta["reason"] is None


@pytest.mark.asyncio
async def test_resume_hook_error_keeps_reason_annotated(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = _AdapterWithCallableHooks(controller, data_dir=tmp_path)

    def boom():
        raise RuntimeError("audit failed")

    adapter.after_resume = boom
    result = await adapter.resume("v")
    assert result["status"] == "ok"
    assert "hook incomplete" in (adapter._status_meta.get("reason") or "")
    # Bot is no longer marked stopped.
    assert adapter._status_meta["stopped"] is False


@pytest.mark.asyncio
async def test_kill_runs_before_kill_hook_before_scheduling_sigterm(tmp_path):
    controller = _make_stub_controller(tmp_path)
    adapter = _AdapterWithCallableHooks(controller, data_dir=tmp_path)
    order = []

    async def hook():
        order.append("before_kill")

    adapter.before_kill = hook
    with patch("hummingbot.dashboard_api.base_adapter.os.kill") as mock_kill:
        result = await adapter.kill("test", "v")
        if adapter._kill_handle:
            adapter._kill_handle.cancel()
        # Hook ran; kill is scheduled but SIGTERM not yet fired.
        assert order == ["before_kill"]
        assert result["status"] == "scheduled"
        mock_kill.assert_not_called()


@pytest.mark.asyncio
async def test_kill_hook_failure_still_schedules_sigterm(tmp_path):
    """Hook failure must NOT prevent the kill — bot must die regardless."""
    controller = _make_stub_controller(tmp_path)
    adapter = _AdapterWithCallableHooks(controller, data_dir=tmp_path)

    def boom():
        raise RuntimeError("cancel failed")

    adapter.before_kill = boom
    with patch("hummingbot.dashboard_api.base_adapter.os.kill"):
        result = await adapter.kill("test", "v")
        if adapter._kill_handle:
            adapter._kill_handle.cancel()
    assert result["status"] == "scheduled"
    # Persisted reason mentions the hook failure.
    status_file = tmp_path / "dashboard_status_stub_bot.json"
    contents = _json.loads(status_file.read_text())
    assert "hook incomplete" in (contents.get("reason") or "")


@pytest.mark.asyncio
async def test_hook_phase_timings_flow_into_response(tmp_path):
    """Subclasses can record per-phase latencies via _record_phase, and
    those flow into the HTTP response."""
    controller = _make_stub_controller(tmp_path)
    adapter = _AdapterWithCallableHooks(controller, data_dir=tmp_path)

    async def hook():
        t0 = time.monotonic()
        await _asyncio.sleep(0.02)
        adapter._record_phase("cancel", t0)
        t1 = time.monotonic()
        await _asyncio.sleep(0.02)
        adapter._record_phase("audit", t1)

    adapter.before_pause = hook
    try:
        result = await adapter.pause("manual", "v", force=False)
        assert "cancel_ms" in result
        assert "audit_ms" in result
        assert "total_ms" in result
        assert result["cancel_ms"] >= 15
        assert result["audit_ms"] >= 15
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_default_hooks_are_noop(tmp_path):
    """Subclass that doesn't override hooks still works (backward compat)."""
    controller = _make_stub_controller(tmp_path)
    adapter = BaseDashboardAdapter(controller, data_dir=tmp_path)  # raw base
    try:
        result_pause = await adapter.pause("manual", "v", force=False)
        result_resume = await adapter.resume("v")
        assert result_pause["status"] == "ok"
        assert result_resume["status"] == "ok"
        # No errors, no annotations.
        assert "hook incomplete" not in (adapter._status_meta.get("reason") or "")
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)
