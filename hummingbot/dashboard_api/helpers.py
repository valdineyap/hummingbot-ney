"""Convenience wrapper to spin up the dashboard API from any controller.

A controller's ``on_start`` reduces to::

    self._dashboard_runner = await maybe_start_dashboard(self)

(plus, if needed, an ``adapter_cls=...`` kwarg). All boilerplate around
config reading, port-in-use handling, and the ``required`` flag lives
here.
"""
from __future__ import annotations

import logging
from typing import Any, Optional, Type

from aiohttp import web

from .base_adapter import BaseDashboardAdapter
from .lifecycle import start_server

_logger = logging.getLogger(__name__)


async def maybe_start_dashboard(
    controller: Any,
    adapter_cls: Optional[Type[BaseDashboardAdapter]] = None,
) -> Optional[web.AppRunner]:
    """Start the dashboard API for ``controller`` if its config opts in.

    :param controller: A controller with a ``dashboard_api`` config block
        exposing ``enabled``, ``host``, ``port``, ``required``,
        ``auth_token``.
    :param adapter_cls: Subclass of :class:`BaseDashboardAdapter` to use.
        If ``None``, the base class is used — works for any controller
        that exposes ``market_data_provider`` and ``get_active_executors``.

    :returns: The ``AppRunner`` if the server bound successfully, or
        ``None`` if ``enabled=False`` or the port was busy with
        ``required=False``.

    :raises OSError: if ``required=True`` and the port is busy.
    :raises ValueError: if ``host=0.0.0.0`` without ``auth_token`` (security gate).
    """
    cfg = getattr(controller.config, "dashboard_api", None)
    if cfg is None or not getattr(cfg, "enabled", False):
        _logger.debug("dashboard_api disabled or absent in config; skipping startup")
        return None

    AdapterCls = adapter_cls or BaseDashboardAdapter
    adapter = AdapterCls(controller)

    try:
        return await start_server(
            host=cfg.host,
            port=cfg.port,
            registry={adapter.bot_name: adapter},
            auth_token=getattr(cfg, "auth_token", None),
        )
    except OSError as e:
        if getattr(cfg, "required", False):
            raise
        _logger.warning(
            "Dashboard API failed to start (%s); continuing without it. "
            "Set dashboard_api.required=true if this should be fatal.",
            e,
        )
        return None
    except ValueError:
        # 0.0.0.0 without auth — always fatal, regardless of `required`.
        raise
