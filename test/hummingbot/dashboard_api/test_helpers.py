"""Tests for hummingbot.dashboard_api.helpers.maybe_start_dashboard.

Covers the plugin contract: any controller with the dashboard_api config
field can call maybe_start_dashboard(self) with no extra setup.
"""
from __future__ import annotations

import socket
from types import SimpleNamespace

import pytest

from hummingbot.dashboard_api import lifecycle, maybe_start_dashboard


def _make_cfg(enabled=True, host="127.0.0.1", port=0, required=False, auth_token=None):
    return SimpleNamespace(
        enabled=enabled, host=host, port=port,
        required=required, auth_token=auth_token,
    )


def _make_controller(dashboard_cfg, tmp_path):
    from test.hummingbot.dashboard_api.test_base_adapter import _make_stub_controller
    controller = _make_stub_controller(tmp_path)
    controller.config.dashboard_api = dashboard_cfg
    return controller


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# enabled / disabled
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_none_when_disabled(tmp_path):
    controller = _make_controller(_make_cfg(enabled=False), tmp_path)
    runner = await maybe_start_dashboard(controller)
    assert runner is None


@pytest.mark.asyncio
async def test_returns_none_when_config_missing(tmp_path):
    from test.hummingbot.dashboard_api.test_base_adapter import _make_stub_controller
    controller = _make_stub_controller(tmp_path)
    # No dashboard_api attribute.
    runner = await maybe_start_dashboard(controller)
    assert runner is None


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_starts_server_with_default_adapter(tmp_path):
    port = _free_port()
    controller = _make_controller(_make_cfg(port=port), tmp_path)
    runner = await maybe_start_dashboard(controller)
    try:
        assert runner is not None
    finally:
        await lifecycle.stop_server(runner)


# ---------------------------------------------------------------------------
# Required flag — fail fast vs graceful degrade
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_required_false_returns_none_on_port_busy(tmp_path):
    # Occupy a port, then ask the helper to bind to the same one with required=False.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.listen()
    try:
        controller = _make_controller(_make_cfg(port=port, required=False), tmp_path)
        runner = await maybe_start_dashboard(controller)
        assert runner is None
    finally:
        s.close()


@pytest.mark.asyncio
async def test_required_true_raises_on_port_busy(tmp_path):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.listen()
    try:
        controller = _make_controller(_make_cfg(port=port, required=True), tmp_path)
        with pytest.raises(OSError):
            await maybe_start_dashboard(controller)
    finally:
        s.close()


# ---------------------------------------------------------------------------
# 0.0.0.0 without auth — security gate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_zero_zero_zero_zero_without_auth_raises(tmp_path):
    controller = _make_controller(_make_cfg(host="0.0.0.0", auth_token=None), tmp_path)
    with pytest.raises(ValueError):
        await maybe_start_dashboard(controller)


# ---------------------------------------------------------------------------
# Plugin invariant: works for any controller without subclassing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_plugin_invariant_works_without_subclass(tmp_path):
    """A controller stub that has NOT subclassed BaseDashboardAdapter must
    still get a working dashboard. This is the cornerstone test for the
    plugin contract."""
    port = _free_port()
    controller = _make_controller(_make_cfg(port=port), tmp_path)
    runner = await maybe_start_dashboard(controller)  # no adapter_cls kwarg
    try:
        assert runner is not None
        # Hit /health to confirm server is reachable.
        from aiohttp import ClientSession
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/health") as r:
                assert r.status == 200
                data = await r.json()
                assert data["botName"] == "stub_bot"
                assert data["status"] == "running"
            async with session.get(f"http://127.0.0.1:{port}/api/v1/getdata/stub_bot") as r:
                assert r.status == 200
                data = await r.json()
                assert data["exchName"] == "stub_bot"
                assert "exchangeBalance" in data
                assert "info" in data
    finally:
        await lifecycle.stop_server(runner)
