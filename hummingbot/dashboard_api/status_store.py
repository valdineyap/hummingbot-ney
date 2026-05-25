"""Atomic read/write for the persisted ``statusMetadata`` block.

The dashboard's ``statusMetadata`` field must survive bot restart so the
operator can see *why* the bot was last killed/paused. We persist it in
``data/dashboard_status_<safe_bot_name>.json`` (next to ``state.json``)
using the same atomic ``tmp + os.replace`` pattern as ``TradeLedger``.

This module is intentionally tiny and dependency-free so it can be
imported by any adapter.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict

from .payload import default_status_metadata, safe_bot_name, to_jsonable

_logger = logging.getLogger(__name__)


def status_file_path(data_dir: Path, bot_name: str) -> Path:
    """Return the canonical persistence path for ``bot_name`` under ``data_dir``.

    Sanitizes ``bot_name`` against path injection (replaces anything outside
    ``[A-Za-z0-9_.-]`` with ``_``).
    """
    return Path(data_dir) / f"dashboard_status_{safe_bot_name(bot_name)}.json"


def read_status(path: Path) -> Dict[str, Any]:
    """Read the persisted status metadata, or return defaults.

    Any I/O error or JSON corruption yields the default block; we never
    block the bot on a bad status file.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            _logger.warning("status_store: %s did not contain a dict; using defaults", path)
            return default_status_metadata()
        # Merge so missing keys get defaults.
        merged = default_status_metadata()
        merged.update({k: data.get(k, merged[k]) for k in merged})
        return merged
    except FileNotFoundError:
        return default_status_metadata()
    except Exception as e:
        _logger.warning("status_store: failed to read %s (%s); using defaults", path, e)
        return default_status_metadata()


def write_status(path: Path, data: Dict[str, Any]) -> None:
    """Atomically persist ``data`` to ``path``.

    Creates the parent directory if missing. Writes to ``<path>.tmp`` then
    ``os.replace`` for atomic swap. Failures are logged but never raised —
    the dashboard API must not block the controller.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(to_jsonable(data), f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass  # fsync not supported (e.g., tmpfs); best-effort.
        os.replace(tmp, path)
    except Exception as e:
        _logger.warning("status_store: failed to write %s (%s); continuing", path, e)
