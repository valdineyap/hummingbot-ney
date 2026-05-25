"""Bitbots-v1-compatible HTTP API exposed by Hummingbot controllers.

The module is a self-contained plugin: any controller can opt in by
adding a ``dashboard_api`` config block and the two lifecycle hooks
documented in ``helpers.maybe_start_dashboard``.

The server itself has zero knowledge of any strategy. Per-strategy
customization (status mapping, info vocabulary, custom pause mechanism)
happens via subclasses of :class:`BaseDashboardAdapter`.

Reference protocol: ``bp-research/bitbots-js/src/api/self/robotEndpoint.ts``.
"""
from .adapter_protocol import DashboardBotAdapter
from .base_adapter import BaseDashboardAdapter
from .helpers import maybe_start_dashboard
from .lifecycle import start_server, stop_server
from .payload import (
    API_VERSION,
    DASHBOARD_API_VERSION,
    format_utc_z,
    safe_bot_name,
    to_jsonable,
    to_percent_bps,
)
from .server import build_app
from .status_store import read_status, status_file_path, write_status

__all__ = [
    "DashboardBotAdapter",
    "BaseDashboardAdapter",
    "maybe_start_dashboard",
    "start_server",
    "stop_server",
    "build_app",
    "read_status",
    "write_status",
    "status_file_path",
    "safe_bot_name",
    "to_percent_bps",
    "to_jsonable",
    "format_utc_z",
    "API_VERSION",
    "DASHBOARD_API_VERSION",
]
