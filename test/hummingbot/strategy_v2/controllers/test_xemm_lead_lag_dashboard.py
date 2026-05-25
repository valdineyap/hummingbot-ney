"""Tests for controllers.generic.xemm_lead_lag_dashboard.

Focus: XEMM-specific overrides on top of BaseDashboardAdapter. The base
class is already exhaustively tested in test/hummingbot/dashboard_api/
test_base_adapter.py — here we only verify the deltas.
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Dict
from unittest.mock import patch

import pytest

from controllers.generic.xemm_lead_lag_dashboard import (
    XEMMLeadLagDashboardAdapter,
    _to_percent_fraction,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

class _Row:
    def __init__(self, price, amount):
        self.price = price
        self.amount = amount


class _Book:
    def __init__(self, bids, asks):
        self._b = [_Row(p, a) for p, a in bids]
        self._a = [_Row(p, a) for p, a in asks]

    def bid_entries(self):
        return iter(self._b)

    def ask_entries(self):
        return iter(self._a)


class _Connector:
    def __init__(self, balances: Dict[str, float], books: Dict[str, _Book]):
        self._balances = balances
        self._books = books
        self.trading_pairs = list(books.keys())

    def get_all_balances(self):
        return dict(self._balances)

    def get_order_book(self, pair):
        return self._books.get(pair)


def _make_config(**overrides):
    base = dict(
        id="xemm_lead_lag_btc_brl_sbe",
        controller_name="xemm_lead_lag",
        controller_type="generic",
        maker_connector="bitpreco",
        maker_trading_pair="BTC-BRL",
        taker_connector="binance",
        taker_trading_pair="BTC-USDT",
        order_amount=Decimal("0.001"),
        max_order_amount_multiplier=Decimal("2.0"),
        target_profitability=Decimal("0.0020"),
        min_profitability=Decimal("0.0007"),
        max_profitability=Decimal("0.0080"),
        inventory_target_pct=Decimal("0.5"),
        max_daily_loss_quote=Decimal("100"),
        kill_switch_file="/tmp/xemm_lead_lag_pause",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _make_controller(tmp_path, *, kill_reason=None, regime=None, snapshot=None):
    binance = _Connector(
        balances={"BTC": 0.0, "USDT": 500.0},
        books={"BTC-USDT": _Book([(50000, 1)], [(50100, 1)])},
    )
    bitpreco = _Connector(
        balances={"BTC": 0.005, "BRL": 1500.0},
        books={"BTC-BRL": _Book([(300000, 1)], [(301000, 1)])},
    )
    mdp = SimpleNamespace(connectors={"binance": binance, "bitpreco": bitpreco})
    return SimpleNamespace(
        config=_make_config(),
        market_data_provider=mdp,
        get_active_executors=lambda: [],
        _kill_reason=kill_reason,
        _regime=regime,
        _safety_snapshot=snapshot,
        _trade_ledger=None,
    )


# ---------------------------------------------------------------------------
# _to_percent_fraction
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("frac,expected", [
    (Decimal("0.0020"), "0.2%"),
    (Decimal("0.005"), "0.5%"),
    (Decimal("0.01"), "1%"),
    (Decimal("0"), "0%"),
    (None, ""),
])
def test_to_percent_fraction(frac, expected):
    assert _to_percent_fraction(frac) == expected


# ---------------------------------------------------------------------------
# Role mapping
# ---------------------------------------------------------------------------

def test_connector_role_mapping_taker_first(tmp_path):
    controller = _make_controller(tmp_path)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    mapping = adapter._connector_role_mapping()
    assert mapping[0] == (1, "binance")   # taker = exchange 1
    assert mapping[1] == (2, "bitpreco")  # maker = exchange 2


def test_pause_file_uses_legacy_path(tmp_path):
    controller = _make_controller(tmp_path)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    assert adapter._pause_file_path() == "/tmp/xemm_lead_lag_pause"


def test_pause_file_falls_back_when_unset(tmp_path):
    controller = _make_controller(tmp_path)
    controller.config.kill_switch_file = None
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    # Falls back to BaseDashboardAdapter default: /tmp/<safe_name>.pause
    assert adapter._pause_file_path().startswith("/tmp/")
    assert "xemm_lead_lag_btc_brl_sbe" in adapter._pause_file_path()


# ---------------------------------------------------------------------------
# get_status — initializing gate
# ---------------------------------------------------------------------------

def test_status_initializing_under_grace_returns_running(tmp_path):
    controller = _make_controller(tmp_path, snapshot=None)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    # Fresh boot → uptime ~0s, no snapshot → still running.
    assert adapter.get_status() == "running"


def test_status_initializing_past_grace_returns_errored(tmp_path):
    controller = _make_controller(tmp_path, snapshot=None)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    # Force boot time into the past.
    adapter._boot_at -= 150
    assert adapter.get_status() == "errored"


def test_status_with_snapshot_running(tmp_path):
    controller = _make_controller(tmp_path, snapshot={"ts": __import__("time").time()})
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    assert adapter.get_status() == "running"


def test_status_with_kill_reason_errored(tmp_path):
    controller = _make_controller(tmp_path,
                                  snapshot={"ts": __import__("time").time()},
                                  kill_reason="DAILY_LOSS")
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    assert adapter.get_status() == "errored"


def test_status_with_pause_file(tmp_path):
    controller = _make_controller(tmp_path, snapshot={"ts": __import__("time").time()})
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    pause = Path(adapter._pause_file_path())
    pause.unlink(missing_ok=True)
    pause.touch()
    try:
        assert adapter.get_status() == "paused"
    finally:
        pause.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# get_status_metadata — WARN_DRIFT + initializing
# ---------------------------------------------------------------------------

def test_meta_initializing_message(tmp_path):
    controller = _make_controller(tmp_path, snapshot=None)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    meta = adapter.get_status_metadata()
    assert meta["reason"] == "initializing"


def test_meta_initializing_timeout_message(tmp_path):
    controller = _make_controller(tmp_path, snapshot=None)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    adapter._boot_at -= 200
    meta = adapter.get_status_metadata()
    assert meta["reason"] == "initializing timeout"


def test_meta_warn_drift(tmp_path):
    controller = _make_controller(tmp_path,
                                  snapshot={"ts": __import__("time").time()},
                                  regime="WARN_DRIFT")
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    meta = adapter.get_status_metadata()
    assert meta["reason"] is not None
    assert "WARN_DRIFT" in meta["reason"]


def test_meta_kill_reason_overrides_warn(tmp_path):
    """kill_reason has priority over WARN_DRIFT."""
    controller = _make_controller(tmp_path,
                                  snapshot={"ts": __import__("time").time()},
                                  regime="WARN_DRIFT",
                                  kill_reason="DAILY_LOSS")
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    meta = adapter.get_status_metadata()
    assert "DAILY_LOSS" in (meta["reason"] or "")


# ---------------------------------------------------------------------------
# get_info — XEMM vocabulary
# ---------------------------------------------------------------------------

def test_info_returns_xemm_vocabulary(tmp_path):
    controller = _make_controller(tmp_path)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    info = adapter.get_info()
    assert info["quote"] == "BRL"
    assert info["bases"] == ["BTC"]
    assert info["maxLoss"] == 100.0
    assert info["arbitrageSpreads"]["BTC"]["buy"] == "0.2%"  # target_profitability 0.002 → 0.2%
    assert info["arbitrageSpreads"]["BTC"]["sell"] == "0.2%"
    assert info["targets"]["BTC"] == "0.5"  # inventory_target_pct
    # order_amount (0.001) * max_order_amount_multiplier (2.0) — Decimal arithmetic
    # preserves scale, so the string is "0.0020".
    assert info["locks"]["BTC"] == "0.0020"
    assert info["exchanges"] == ["binance", "bitpreco"]
    # config dict has the salient fields
    cfg = info["config"]
    assert cfg["maker_connector"] == "bitpreco"
    assert cfg["taker_connector"] == "binance"


# ---------------------------------------------------------------------------
# get_balances — bitbots role layout
# ---------------------------------------------------------------------------

def test_balances_taker_is_exchange1(tmp_path):
    controller = _make_controller(tmp_path)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    exch1, exch2, total, _ = adapter.get_balances()
    # exchange1 = taker (binance), exchange2 = maker (bitpreco/BRL).
    assert "USDT" in exch1
    assert "BRL" in exch2


# ---------------------------------------------------------------------------
# kill flush
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_before_kill_flushes_ledger_state(tmp_path):
    controller = _make_controller(tmp_path)
    written = {}
    ledger = SimpleNamespace(
        _last_trade_record={"hello": "world"},
        _write_state=lambda r: written.setdefault("called_with", r),
    )
    controller._trade_ledger = ledger
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    with patch("hummingbot.dashboard_api.base_adapter.os.kill"):
        await adapter.kill("test", "user")
        if adapter._kill_handle:
            adapter._kill_handle.cancel()
    assert written.get("called_with") == {"hello": "world"}


@pytest.mark.asyncio
async def test_before_kill_swallows_ledger_failure(tmp_path):
    controller = _make_controller(tmp_path)
    def boom(_):
        raise RuntimeError("ledger broken")
    controller._trade_ledger = SimpleNamespace(
        _last_trade_record={"x": 1}, _write_state=boom,
    )
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    with patch("hummingbot.dashboard_api.base_adapter.os.kill"):
        result = await adapter.kill("test", "u")
        if adapter._kill_handle:
            adapter._kill_handle.cancel()
    assert result["status"] == "scheduled"  # kill still proceeded


# ---------------------------------------------------------------------------
# Golden test: payload matches bitbots-v1 schema fields/types
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_golden_payload_matches_bitbots_schema(tmp_path):
    """The full payload produced by the adapter through the HTTP layer
    must contain every top-level field that the bitbots dashboard expects,
    with the right type. We compare names+types against a fixture rather
    than values — values are runtime."""
    from aiohttp.test_utils import TestClient, TestServer

    from hummingbot.dashboard_api import server

    controller = _make_controller(tmp_path,
                                  snapshot={"ts": __import__("time").time()})
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    app = server.build_app(registry={adapter.bot_name: adapter}, auth_token=None)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get(f"/api/v1/getdata/{adapter.bot_name}")
        assert resp.status == 200
        data = await resp.json()

    # Top-level fields required by Endpoint.Exchange in bitbots-js
    required_top = {
        "exchName": str,
        "hidden": bool,
        "status": str,
        "lastOnline": str,
        "info": dict,
        "exchangeBalance": dict,
        "bitprecoBalance": dict,
        "totalBalance": dict,
        "botVersion": str,
        "statusMetadata": dict,
        "avgCycleDuration": int,
    }
    for key, ty in required_top.items():
        assert key in data, f"missing top-level field: {key}"
        if data[key] is not None:
            assert isinstance(data[key], ty), (
                f"field {key} expected {ty.__name__}, got {type(data[key]).__name__}"
            )

    info_required = {
        "quote": str, "bases": list, "exchanges": list,
        "arbitrageSpreads": dict, "targets": dict, "locks": dict,
    }
    for key, ty in info_required.items():
        assert key in data["info"], f"missing info field: {key}"
        if data["info"][key] is not None:
            assert isinstance(data["info"][key], ty)

    # lastOnline must be ISO ...Z
    assert data["lastOnline"].endswith("Z")


# ---------------------------------------------------------------------------
# Cancel + audit hooks (XEMM-specific implementations)
# ---------------------------------------------------------------------------
#
# The base adapter wraps these in asyncio.wait_for + try/except. Here we
# verify that the XEMM subclass:
#   * Wires _before_pause / _after_resume / _before_kill to the right
#     controller methods (cancel + audit + flush).
#   * Records per-phase latencies (cancel_ms / audit_ms / flush_ms).
#   * Continues running subsequent phases if one fails (e.g. cancel error
#     does NOT skip audit).
#   * Uses market_data_provider.time() as the audit clock when available.


def _make_controller_with_cancel_audit(tmp_path, *, cancel_raises=None,
                                       audit_raises=None,
                                       cancel_sleep=0.0,
                                       audit_sleep=0.0,
                                       has_audit=True,
                                       has_cancel=True,
                                       mdp_time=1700000000.0):
    """Like _make_controller but with controllable cancel/audit fakes."""
    base = _make_controller(tmp_path)

    cancel_calls = []
    audit_calls = []

    async def fake_cancel():
        cancel_calls.append("called")
        if cancel_sleep:
            await asyncio.sleep(cancel_sleep)
        if cancel_raises:
            raise cancel_raises

    async def fake_audit(now, *, source):
        audit_calls.append({"now": now, "source": source})
        if audit_sleep:
            await asyncio.sleep(audit_sleep)
        if audit_raises:
            raise audit_raises

    if has_cancel:
        base._cancel_all_open_orders_on_startup = fake_cancel
    if has_audit:
        base._run_inventory_audit = fake_audit

    # market_data_provider.time() stub
    base.market_data_provider = SimpleNamespace(
        connectors=base.market_data_provider.connectors,
        time=lambda: mdp_time,
    )

    base._test_cancel_calls = cancel_calls
    base._test_audit_calls = audit_calls
    return base


@pytest.mark.asyncio
async def test_before_pause_runs_cancel_then_audit(tmp_path):
    controller = _make_controller_with_cancel_audit(tmp_path)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    pause_path = Path(adapter._pause_file_path())
    pause_path.unlink(missing_ok=True)
    try:
        result = await adapter.pause("manual", "valdiney", force=False)
        assert result["status"] == "ok"
        assert pause_path.exists()
        # Both phases ran exactly once.
        assert controller._test_cancel_calls == ["called"]
        assert len(controller._test_audit_calls) == 1
        # Audit got the controller clock + source label.
        ac = controller._test_audit_calls[0]
        assert ac["source"] == "dashboard_pause"
        assert ac["now"] == 1700000000.0
        # Per-phase timings flow into response.
        assert "cancel_ms" in result
        assert "audit_ms" in result
    finally:
        pause_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_after_resume_runs_cancel_then_audit(tmp_path):
    controller = _make_controller_with_cancel_audit(tmp_path)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    Path(adapter._pause_file_path()).touch()
    try:
        result = await adapter.resume("valdiney")
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)
    assert result["status"] == "ok"
    assert controller._test_cancel_calls == ["called"]
    assert controller._test_audit_calls[0]["source"] == "dashboard_resume"


@pytest.mark.asyncio
async def test_before_kill_runs_cancel_audit_then_flush(tmp_path):
    controller = _make_controller_with_cancel_audit(tmp_path)
    flushed = []
    controller._trade_ledger = SimpleNamespace(
        _last_trade_record={"x": 1},
        _write_state=lambda r: flushed.append(r),
    )
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    with patch("hummingbot.dashboard_api.base_adapter.os.kill"):
        result = await adapter.kill("manual", "valdiney")
        if adapter._kill_handle:
            adapter._kill_handle.cancel()
    assert result["status"] == "scheduled"
    assert controller._test_cancel_calls == ["called"]
    assert controller._test_audit_calls[0]["source"] == "dashboard_kill"
    assert flushed == [{"x": 1}]
    assert "cancel_ms" in result
    assert "audit_ms" in result
    assert "flush_ms" in result


@pytest.mark.asyncio
async def test_cancel_failure_does_not_skip_audit(tmp_path):
    """If cancel raises, audit should still run."""
    controller = _make_controller_with_cancel_audit(
        tmp_path, cancel_raises=RuntimeError("boom"),
    )
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    try:
        result = await adapter.pause("manual", "v", force=False)
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)
    # Audit ran despite cancel failure.
    assert len(controller._test_audit_calls) == 1
    # Per-step error is swallowed inside the hook, so no top-level
    # "hook incomplete" annotation expected.
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_audit_failure_does_not_skip_flush_in_kill(tmp_path):
    controller = _make_controller_with_cancel_audit(
        tmp_path, audit_raises=RuntimeError("audit bug"),
    )
    flushed = []
    controller._trade_ledger = SimpleNamespace(
        _last_trade_record={"x": 1},
        _write_state=lambda r: flushed.append(r),
    )
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    with patch("hummingbot.dashboard_api.base_adapter.os.kill"):
        result = await adapter.kill("manual", "v")
        if adapter._kill_handle:
            adapter._kill_handle.cancel()
    assert result["status"] == "scheduled"
    assert flushed == [{"x": 1}]  # flush ran despite audit failure


@pytest.mark.asyncio
async def test_pause_with_missing_controller_methods_does_not_explode(tmp_path):
    """Strategy-side methods may genuinely not exist (e.g. simpler controller).
    Hook must log a warning but still let pause file be touched."""
    # When has_cancel=False and has_audit=False, the factory simply skips
    # attaching those methods to the SimpleNamespace, so getattr(...)
    # returns the default fallback. That's exactly the "missing methods"
    # condition the adapter must handle.
    controller = _make_controller_with_cancel_audit(
        tmp_path, has_cancel=False, has_audit=False,
    )
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    try:
        result = await adapter.pause("manual", "v", force=False)
        assert result["status"] == "ok"
        assert Path(adapter._pause_file_path()).exists()
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_idempotent_kill_does_not_re_run_hook(tmp_path):
    """Second kill while one is scheduled must NOT trigger another hook."""
    controller = _make_controller_with_cancel_audit(tmp_path)
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    with patch("hummingbot.dashboard_api.base_adapter.os.kill"):
        first = await adapter.kill("test", "v")
        second = await adapter.kill("test", "v")
        if adapter._kill_handle:
            adapter._kill_handle.cancel()
    assert first["status"] == "scheduled"
    assert second["status"] == "already_scheduled"
    # cancel/audit each ran exactly once (from the first kill only).
    assert controller._test_cancel_calls == ["called"]
    assert len(controller._test_audit_calls) == 1


@pytest.mark.asyncio
async def test_hook_timeout_caps_pause_latency(tmp_path):
    """Slow cancel+audit hits the wait_for cap; pause file still touched."""
    controller = _make_controller_with_cancel_audit(
        tmp_path, cancel_sleep=2.0, audit_sleep=2.0,
    )
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    adapter.HOOK_TIMEOUT_SEC = 0.1
    try:
        started = time.monotonic()
        result = await adapter.pause("manual", "v", force=False)
        elapsed = time.monotonic() - started
        assert elapsed < 0.5, f"timeout not enforced: {elapsed}s"
        assert result["status"] == "ok"
        assert Path(adapter._pause_file_path()).exists()
        # Reason annotated with timeout.
        assert "timeout" in (adapter._status_meta.get("reason") or "")
    finally:
        Path(adapter._pause_file_path()).unlink(missing_ok=True)


def test_now_falls_back_to_wall_clock_when_mdp_missing(tmp_path):
    controller = _make_controller_with_cancel_audit(tmp_path)
    delattr(controller, "market_data_provider")
    adapter = XEMMLeadLagDashboardAdapter(controller, data_dir=tmp_path)
    now = adapter._now()
    # Should be a Unix timestamp roughly = wall clock; just check it's positive
    # and a recent value (after 2020).
    assert now > 1577836800  # 2020-01-01


