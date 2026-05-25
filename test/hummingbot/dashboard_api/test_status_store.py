"""Unit tests for hummingbot.dashboard_api.status_store."""
from __future__ import annotations

import json
from pathlib import Path

from hummingbot.dashboard_api import status_store as s
from hummingbot.dashboard_api.payload import default_status_metadata


def test_status_file_path_sanitizes(tmp_path: Path):
    p = s.status_file_path(tmp_path, "../weird/name")
    assert p.name == "dashboard_status_.._weird_name.json"
    assert p.parent == tmp_path


def test_read_status_missing_returns_default(tmp_path: Path):
    p = tmp_path / "dashboard_status_missing.json"
    assert s.read_status(p) == default_status_metadata()


def test_write_then_read_roundtrip(tmp_path: Path):
    p = tmp_path / "dashboard_status_x.json"
    data = {
        "reason": "killed via API",
        "requester": "valdiney",
        "lastUpdated": "2026-05-18T15:32:01Z",
        "stopped": False,
    }
    s.write_status(p, data)
    assert p.exists()
    assert s.read_status(p) == data


def test_write_creates_parent_dir(tmp_path: Path):
    # Nested directory that doesn't exist yet.
    deep = tmp_path / "data" / "bots" / "abc"
    p = deep / "dashboard_status_x.json"
    assert not deep.exists()
    s.write_status(p, {"reason": "hi"})
    assert p.exists()


def test_write_is_atomic_via_tmp_swap(tmp_path: Path, monkeypatch):
    """The tmp file must not linger on disk after a successful write."""
    p = tmp_path / "dashboard_status_x.json"
    s.write_status(p, {"reason": "test"})
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == []


def test_read_corrupted_json_returns_default(tmp_path: Path):
    p = tmp_path / "dashboard_status_x.json"
    p.write_text("{not valid json")
    assert s.read_status(p) == default_status_metadata()


def test_read_non_dict_payload_returns_default(tmp_path: Path):
    p = tmp_path / "dashboard_status_x.json"
    p.write_text(json.dumps([1, 2, 3]))
    assert s.read_status(p) == default_status_metadata()


def test_read_partial_payload_merges_with_defaults(tmp_path: Path):
    p = tmp_path / "dashboard_status_x.json"
    p.write_text(json.dumps({"reason": "only this"}))
    out = s.read_status(p)
    assert out["reason"] == "only this"
    assert out["stopped"] is False
    assert out["requester"] is None
    assert out["lastUpdated"] is None


def test_write_never_raises_on_io_error(tmp_path: Path, monkeypatch):
    """status_store.write_status logs but doesn't propagate errors."""
    # Force os.replace to fail.
    def boom(*a, **k):
        raise OSError("boom")
    monkeypatch.setattr("hummingbot.dashboard_api.status_store.os.replace", boom)
    s.write_status(tmp_path / "x.json", {"reason": "x"})  # must not raise
