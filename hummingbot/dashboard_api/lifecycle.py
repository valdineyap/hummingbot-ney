"""Start/stop helpers for the aiohttp dashboard server.

Wraps the boilerplate around ``aiohttp.web.AppRunner`` so callers don't
have to deal with ``TCPSite`` setup, ``OSError`` for port-in-use, or
graceful cleanup timeouts. Used by :func:`maybe_start_dashboard` in
``helpers.py``; can also be used directly in tests.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from aiohttp import web

from .server import build_app

_logger = logging.getLogger(__name__)


async def start_server(
    host: str,
    port: int,
    registry: Dict[str, Any],
    auth_token: Optional[str] = None,
) -> web.AppRunner:
    """Bind an aiohttp ``AppRunner`` on ``host:port`` serving ``registry``.

    Raises ``OSError`` if the port is in use; raises ``ValueError`` if
    ``host == "0.0.0.0"`` and ``auth_token`` is None (security gate).
    """
    if host == "0.0.0.0" and not auth_token:
        raise ValueError(
            "Dashboard API on 0.0.0.0 requires auth_token to be set "
            "(refusing to expose pause/kill endpoints unauthenticated)."
        )

    app = build_app(registry=registry, auth_token=auth_token)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    _logger.info("Dashboard API listening on http://%s:%d", host, port)
    return runner


async def stop_server(runner: Optional[web.AppRunner], grace_sec: float = 5.0) -> None:
    """Cleanup the runner with a bounded grace period.

    Never raises. Used in lifecycle ``on_stop`` where the event loop may
    be tearing down — failures here are best-effort.
    """
    if runner is None:
        return
    try:
        await asyncio.wait_for(runner.cleanup(), timeout=grace_sec)
    except asyncio.TimeoutError:
        _logger.warning("Dashboard API cleanup exceeded %.1fs; forcing exit", grace_sec)
    except Exception as e:
        _logger.warning("Dashboard API cleanup failed: %s", e)
