"""aiohttp app + handlers exposing the bitbots-v1 endpoints.

Routes implemented in phase 1:

* ``GET  /api/v1/getdata/{bot_name}`` — polling endpoint (best-effort)
* ``POST /api/v1/bot-command/{bot_name}`` — pause/resume/kill/reset/stop/settle
* ``GET  /api/v1/getBooks/{bot_name}/{pair}`` — order books
* ``GET  /api/v1/specOrders/{bot_name}`` — active executors/orders
* ``GET  /health`` — liveness + version (no auth, ever)
* All other bitbots paths → 501 Not Implemented

The handlers talk to a ``registry: Dict[str, DashboardBotAdapter]`` keyed
by ``bot_name``. The server has zero knowledge of any specific strategy.
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from aiohttp import web

from .payload import (
    API_VERSION,
    DASHBOARD_API_VERSION,
    format_utc_z,
    to_jsonable,
    to_percent_bps,
)

_logger = logging.getLogger(__name__)

# Typed key for the adapter registry, avoids aiohttp's NotAppKeyWarning.
REGISTRY_KEY: "web.AppKey[Dict[str, Any]]" = web.AppKey("registry", dict)

_MAX_BOOK_DEPTH = 100
_DEFAULT_BOOK_DEPTH = 10

# Endpoints we explicitly advertise as not implemented (bitbots compat).
_NOT_IMPLEMENTED_PATHS = (
    "placeOrder",
    "bots/{bot_name}/transfers",
    "bots/{bot_name}/transfers/subaccounts",
    "bots/{bot_name}/transfers/sync",
    "bots/{bot_name}/request",
    "getOpPrice",
    "delist-schedule",
    "memory/snapshot",
)


# ---------------------------------------------------------------------------
# App builder
# ---------------------------------------------------------------------------

def build_app(
    registry: Dict[str, Any],
    auth_token: Optional[str] = None,
) -> web.Application:
    """Build the aiohttp ``Application`` wired to the given adapter registry.

    Exposed as a function (not class) to keep the module trivially mockable
    in tests.
    """
    app = web.Application(middlewares=[
        _make_auth_middleware(auth_token),
        _make_access_log_middleware(),
    ])
    app[REGISTRY_KEY] = registry

    # /health — always available, never auth-gated (handled in middleware).
    app.router.add_get("/health", _handle_health)

    # Read endpoints
    app.router.add_get("/api/v1/getdata/{bot_name}", _handle_getdata)
    app.router.add_get("/api/v1/getBooks/{bot_name}/{pair}", _handle_get_books)
    app.router.add_get("/api/v1/specOrders/{bot_name}", _handle_spec_orders)

    # Write endpoints
    app.router.add_post("/api/v1/bot-command/{bot_name}", _handle_bot_command)

    # 501 stubs for everything else bitbots clients might call.
    app.router.add_post("/api/v1/placeOrder/{bot_name}", _handle_not_implemented)
    app.router.add_post("/api/v1/bots/{bot_name}/transfers", _handle_not_implemented)
    app.router.add_post("/api/v1/bots/{bot_name}/transfers/subaccounts", _handle_not_implemented)
    app.router.add_post("/api/v1/bots/{bot_name}/transfers/sync", _handle_not_implemented)
    app.router.add_post("/api/v1/bots/{bot_name}/request", _handle_not_implemented)
    app.router.add_get("/api/v1/getOpPrice/{bot_name}/{pair}/{op}/{amount}", _handle_not_implemented)
    app.router.add_get("/api/v1/delist-schedule/{bot_name}/{exchange_name}", _handle_not_implemented)
    app.router.add_get("/api/v1/memory/snapshot", _handle_not_implemented)
    app.router.add_get("/api/v1/prometheus/metrics", _handle_not_implemented)

    return app


# ---------------------------------------------------------------------------
# Middlewares
# ---------------------------------------------------------------------------

def _make_auth_middleware(auth_token: Optional[str]) -> Callable:
    """Bearer-token middleware. Disabled (passthrough) if ``auth_token`` is None.

    /health is always exempt — useful for systemd / monitor probes even when
    the rest of the API requires auth.
    """
    @web.middleware
    async def middleware(request: web.Request, handler: Callable[..., Awaitable[web.StreamResponse]]) -> web.StreamResponse:
        if auth_token is None or request.path == "/health":
            return await handler(request)
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return web.json_response(
                {"status": 401, "message": "missing Authorization: Bearer header"},
                status=401,
            )
        token = header[len("Bearer "):].strip()
        if token != auth_token:
            return web.json_response(
                {"status": 401, "message": "invalid bearer token"},
                status=401,
            )
        return await handler(request)

    return middleware


def _make_access_log_middleware() -> Callable:
    """Log writes (pause/kill/resume/reset/settle) at INFO with user/remote.

    Reads are logged at DEBUG to avoid spamming on the 15-second polling.
    """
    @web.middleware
    async def middleware(request: web.Request, handler: Callable[..., Awaitable[web.StreamResponse]]) -> web.StreamResponse:
        is_write = request.method == "POST" and "bot-command" in request.path
        if is_write:
            try:
                body = await request.json()
            except Exception:
                body = {}
            request["_logged_body"] = body  # stash for handler
            cmd = body.get("botCommand") if isinstance(body, dict) else None
            user = body.get("user") if isinstance(body, dict) else None
            _logger.info(
                "dashboard_api WRITE %s cmd=%s user=%s remote=%s",
                request.path, cmd, user, request.remote,
            )
        else:
            _logger.debug("dashboard_api %s %s remote=%s",
                          request.method, request.path, request.remote)
        return await handler(request)

    return middleware


# ---------------------------------------------------------------------------
# Handler helpers
# ---------------------------------------------------------------------------

def _lookup_adapter(request: web.Request) -> Optional[Any]:
    registry = request.app[REGISTRY_KEY]
    bot_name = request.match_info.get("bot_name", "")
    return registry.get(bot_name)


def _not_found(bot_name: str) -> web.Response:
    return web.json_response(
        {"status": 404, "message": f"bot not found: {bot_name}"},
        status=404,
    )


def _error_response(e: Exception) -> web.Response:
    return web.json_response(
        {"status": 500, "message": "internal server error",
         "details": f"{type(e).__name__}: {e}"},
        status=500,
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def _handle_health(request: web.Request) -> web.Response:
    """Minimal liveness + version. No auth. Suitable for systemd probes."""
    registry = request.app[REGISTRY_KEY]
    # If multiple bots are registered, return the first; in practice we have 1.
    adapter = next(iter(registry.values()), None)
    payload: Dict[str, Any] = {
        "ok": True,
        "apiVersion": API_VERSION,
        "dashboardApiVersion": DASHBOARD_API_VERSION,
    }
    if adapter is not None:
        try:
            payload["botName"] = adapter.bot_name
            payload["status"] = adapter.get_status()
            payload["uptimeSec"] = int(adapter.get_uptime_sec())
            payload["botVersion"] = adapter.get_bot_version()
        except Exception as e:
            _logger.exception("health: adapter raised %s", e)
            payload["ok"] = False
            payload["error"] = str(e)
    return web.json_response(payload)


async def _handle_not_implemented(request: web.Request) -> web.Response:
    return web.json_response(
        {"status": 501, "message": "not implemented",
         "endpoint": request.path, "method": request.method},
        status=501,
    )


async def _handle_getdata(request: web.Request) -> web.Response:
    """Bitbots-v1 ``getdata`` payload. Best-effort: each field is computed in
    its own try/except so partial failures never escalate to HTTP 500."""
    adapter = _lookup_adapter(request)
    if adapter is None:
        return _not_found(request.match_info.get("bot_name", ""))

    partial_errors: list = []  # collected to inject into statusMetadata.reason

    def _field(name: str, getter: Callable[[], Any], default: Any) -> Any:
        try:
            return getter()
        except NotImplementedError:
            return default
        except Exception as e:
            _logger.exception("getdata field %s failed: %s", name, e)
            partial_errors.append(name)
            return default

    status = _field("status", adapter.get_status, "errored")
    status_meta = _field("statusMetadata", adapter.get_status_metadata, {})
    info = _field("info", adapter.get_info, {})

    balances = _field("balances", adapter.get_balances, ({}, {}, {}, None))
    exch1_balance, exch2_balance, total_balance, balance_last_updated = balances

    # Inject partial error info into statusMetadata.reason (priority 1).
    if partial_errors:
        existing = status_meta.get("reason") if isinstance(status_meta, dict) else None
        msg = f"partial getdata error: {', '.join(partial_errors)}"
        if existing:
            msg = f"{msg}; {existing}"
        if not isinstance(status_meta, dict):
            status_meta = {}
        status_meta["reason"] = msg

    payload = {
        "exchName": adapter.bot_name,
        "hidden": False,
        "status": status,
        "lastOnline": format_utc_z(),
        "info": info,
        "exchangeBalance": exch1_balance,
        "bitprecoBalance": exch2_balance,
        "totalBalance": total_balance,
        "balanceLastUpdated": format_utc_z(balance_last_updated) if balance_last_updated else None,
        "balancesIncInTraffic": total_balance,
        "botVersion": _field("botVersion", adapter.get_bot_version, "unknown"),
        "statusMetadata": status_meta,
        "marketSpread": None,
        "avgCycleDuration": _field("avgCycleDuration", adapter.get_avg_cycle_duration_ms, 1000),
    }
    return web.json_response(to_jsonable(payload))


async def _handle_bot_command(request: web.Request) -> web.Response:
    """Dispatch pause/resume/kill/reset/stop/settle."""
    adapter = _lookup_adapter(request)
    if adapter is None:
        return _not_found(request.match_info.get("bot_name", ""))

    body = request.get("_logged_body")
    if body is None:
        try:
            body = await request.json()
        except Exception:
            return web.json_response(
                {"status": 400, "message": "invalid JSON body"}, status=400,
            )
    if not isinstance(body, dict):
        return web.json_response(
            {"status": 400, "message": "body must be a JSON object"}, status=400,
        )
    command = body.get("botCommand")
    if not command:
        return web.json_response(
            {"status": 400, "message": "botCommand body property is required"},
            status=400,
        )

    requester = body.get("user")
    params = body.get("params") or {}

    try:
        if command == "pause":
            result = await adapter.pause(
                reason=f"Bot paused via API by {requester or 'anonymous'}",
                requester=requester, force=False,
            )
            return web.json_response({"status": 201, "message": "ok", **result}, status=201)
        elif command == "stop":
            result = await adapter.pause(
                reason=f"Bot stopped via API by {requester or 'anonymous'}",
                requester=requester, force=True,
            )
            return web.json_response({"status": 201, "message": "ok", **result}, status=201)
        elif command == "resume":
            result = await adapter.resume(requester=requester)
            return web.json_response({"status": 201, "message": "ok", **result}, status=201)
        elif command == "kill":
            result = await adapter.kill(reason="kill via API", requester=requester)
            msg = "already scheduled" if result.get("status") == "already_scheduled" else "scheduled"
            return web.json_response({"status": 202, "message": msg, **result}, status=202)
        elif command == "reset":
            result = await adapter.reset(requester=requester)
            msg = "already scheduled" if result.get("status") == "already_scheduled" else "scheduled"
            return web.json_response({"status": 202, "message": msg, **result}, status=202)
        elif command == "settle":
            try:
                result = await adapter.settle(params, requester)
                return web.json_response({"status": 201, "message": "ok", **result}, status=201)
            except NotImplementedError:
                return web.json_response(
                    {"status": 501, "message": "settle not implemented"}, status=501,
                )
        else:
            return web.json_response(
                {"status": 400, "message": f"unknown command: {command}"}, status=400,
            )
    except Exception as e:
        _logger.exception("bot-command %s failed: %s", command, e)
        return _error_response(e)


async def _handle_get_books(request: web.Request) -> web.Response:
    adapter = _lookup_adapter(request)
    if adapter is None:
        return _not_found(request.match_info.get("bot_name", ""))

    pair_raw = request.match_info.get("pair", "")
    pair = pair_raw.upper()
    if not pair or "-" not in pair:
        return web.json_response(
            {"status": 400,
             "message": f"invalid pair: {pair_raw!r} (expected BASE-QUOTE)"},
            status=400,
        )

    # depth: clamp to [1, 100]
    try:
        depth = int(request.query.get("depth", _DEFAULT_BOOK_DEPTH))
    except (TypeError, ValueError):
        return web.json_response(
            {"status": 400, "message": "depth must be an integer"}, status=400,
        )
    depth = max(1, min(depth, _MAX_BOOK_DEPTH))

    # exchanges: comma-separated ints
    exchanges_param = request.query.get("exchanges")
    exchanges: Optional[list] = None
    if exchanges_param:
        try:
            exchanges = [int(x) for x in exchanges_param.split(",") if x]
        except ValueError:
            return web.json_response(
                {"status": 400,
                 "message": f"invalid exchanges: {exchanges_param!r}"},
                status=400,
            )

    apply_spread_param = request.query.get("applySpread", "false").lower()
    apply_spread = apply_spread_param in ("true", "1", "yes")

    try:
        result = await adapter.get_books(pair, exchanges, depth, apply_spread)
    except Exception as e:
        _logger.exception("getBooks failed: %s", e)
        return _error_response(e)
    return web.json_response(to_jsonable(result))


async def _handle_spec_orders(request: web.Request) -> web.Response:
    adapter = _lookup_adapter(request)
    if adapter is None:
        return _not_found(request.match_info.get("bot_name", ""))
    try:
        result = await adapter.get_spec_orders()
    except Exception as e:
        _logger.exception("specOrders failed: %s", e)
        return _error_response(e)
    return web.json_response(to_jsonable(result))


# Re-export the helper that the XEMM adapter uses.
__all__ = ["build_app", "to_percent_bps"]
