"""Bitbots-v1 payload helpers and the API/dashboard version constants.

The dataclasses here mirror the TypeScript interfaces in
``bp-research/bitbots-js/src/api/self/robotEndpoint.ts`` (namespace
``Endpoint``). They are NOT used for input validation — we accept whatever
the adapter returns. Their main job is documentation + the
``to_jsonable`` helpers used when serializing.

Helpers:

* :func:`to_percent_bps` — convert basis points to bitbots-style
  ``"X.Y%"`` strings.
* :func:`format_utc_z` — ISO8601 UTC with ``Z`` suffix (bitbots convention).
* :func:`safe_bot_name` — sanitize a bot_name for use in filesystem paths.
* :func:`to_jsonable` — best-effort JSON serialization for Decimals etc.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Version constants surfaced through /health
# ---------------------------------------------------------------------------

API_VERSION = "bitbots-v1-compat"
DASHBOARD_API_VERSION = "0.1.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_.-]")


def safe_bot_name(bot_name: str) -> str:
    """Sanitize ``bot_name`` for use as a filename component.

    Replaces any character outside ``[A-Za-z0-9_.-]`` with ``_``.
    Defensive against malformed ``config.id`` values.
    """
    return _SAFE_NAME_RE.sub("_", bot_name)


def to_percent_bps(bps: Optional[float], precision: int = 5) -> str:
    """Convert basis points to a bitbots-style ``"X.Y%"`` string.

    The bitbots dashboard expects the ``arbitrageSpreads`` field as
    formatted percent strings (see ``robotEndpoint.ts:97-99``). Examples:

        >>> to_percent_bps(30)
        '0.3%'
        >>> to_percent_bps(5)
        '0.05%'
        >>> to_percent_bps(100)
        '1%'
        >>> to_percent_bps(None)
        ''
        >>> to_percent_bps(0)
        '0%'

    :param bps: Value in basis points (1 bp = 0.01%). ``None`` → empty string.
    :param precision: Decimal places to round to, mirroring bitbots default.
    """
    if bps is None:
        return ""
    pct = round(float(bps) / 100.0, precision)
    # Avoid trailing ``.0`` for integer percentages (matches bitbots output).
    if pct == int(pct):
        return f"{int(pct)}%"
    # Strip trailing zeros from the fractional part.
    formatted = f"{pct:.{precision}f}".rstrip("0").rstrip(".")
    return f"{formatted}%"


def format_utc_z(dt: Optional[datetime] = None) -> str:
    """Return an ISO 8601 UTC timestamp with the trailing ``Z`` suffix.

    Bitbots uses ``new Date().toISOString()`` which produces ``...Z``
    rather than ``+00:00``. Python's ``isoformat()`` produces the latter,
    so we substitute.
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def to_jsonable(value: Any) -> Any:
    """Best-effort conversion of common Python types to JSON-serializable.

    Decimal → float, datetime → ISO ``Z`` string, set/tuple → list. Nested
    dicts and lists are walked recursively. Unknown types fall through to
    ``str(value)`` as a last resort.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        try:
            return float(value)
        except Exception:
            return str(value)
    if isinstance(value, datetime):
        return format_utc_z(value)
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(v) for v in value]
    try:
        return str(value)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Default status_metadata shape
# ---------------------------------------------------------------------------

def default_status_metadata() -> dict:
    """Empty status_metadata block, used on first boot or after corruption."""
    return {
        "reason": None,
        "requester": None,
        "lastUpdated": None,
        "stopped": False,
    }
