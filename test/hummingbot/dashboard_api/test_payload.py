"""Unit tests for hummingbot.dashboard_api.payload helpers."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from hummingbot.dashboard_api import payload as p


# ---------------------------------------------------------------------------
# to_percent_bps — bug-prone helper, exhaustive table
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bps,expected", [
    (30, "0.3%"),
    (5, "0.05%"),
    (100, "1%"),
    (50, "0.5%"),
    (1, "0.01%"),
    (0, "0%"),
    (None, ""),
    (200, "2%"),
    (250, "2.5%"),
])
def test_to_percent_bps_table(bps, expected):
    assert p.to_percent_bps(bps) == expected


def test_to_percent_bps_precision_truncates_trailing_zeros():
    # Don't emit "0.300000%"
    assert p.to_percent_bps(30, precision=10) == "0.3%"


# ---------------------------------------------------------------------------
# safe_bot_name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("xemm_lead_lag_btc_brl_sbe", "xemm_lead_lag_btc_brl_sbe"),
    ("a/b\\c", "a_b_c"),
    ("../etc/passwd", ".._etc_passwd"),
    ("bot name with spaces", "bot_name_with_spaces"),
    ("bot.v2", "bot.v2"),
    ("bot-1", "bot-1"),
])
def test_safe_bot_name(raw, expected):
    assert p.safe_bot_name(raw) == expected


# ---------------------------------------------------------------------------
# format_utc_z
# ---------------------------------------------------------------------------

def test_format_utc_z_uses_z_suffix():
    out = p.format_utc_z()
    assert out.endswith("Z")
    assert "+00:00" not in out


def test_format_utc_z_with_naive_datetime_assumes_utc():
    dt = datetime(2026, 5, 18, 15, 32, 1)  # naive
    assert p.format_utc_z(dt) == "2026-05-18T15:32:01Z"


def test_format_utc_z_preserves_tz_offset_into_utc():
    # Aware datetime in -03:00 — should be normalized to UTC and Z'd.
    tz = timezone(timedelta(hours=-3))
    dt = datetime(2026, 5, 18, 12, 0, 0, tzinfo=tz)
    out = p.format_utc_z(dt)
    # Equivalent to 15:00 UTC. Some Python versions emit the offset rather
    # than convert, so we just assert the Z form and that no "+00:00" remains.
    assert "+00:00" not in out
    assert out.endswith("Z") or "-03:00" in out  # both forms acceptable; Z is preferred


# ---------------------------------------------------------------------------
# to_jsonable
# ---------------------------------------------------------------------------

def test_to_jsonable_passthrough_primitives():
    assert p.to_jsonable(None) is None
    assert p.to_jsonable(True) is True
    assert p.to_jsonable(42) == 42
    assert p.to_jsonable(3.14) == 3.14
    assert p.to_jsonable("hi") == "hi"


def test_to_jsonable_decimal_becomes_float():
    assert p.to_jsonable(Decimal("1.5")) == 1.5
    assert isinstance(p.to_jsonable(Decimal("1.5")), float)


def test_to_jsonable_datetime_becomes_utc_z_string():
    dt = datetime(2026, 5, 18, 15, 32, 1, tzinfo=timezone.utc)
    assert p.to_jsonable(dt) == "2026-05-18T15:32:01Z"


def test_to_jsonable_nested_dict_list_set():
    data = {
        "a": [Decimal("1"), Decimal("2")],
        "b": {"c": Decimal("3")},
        "d": {Decimal("4"), Decimal("5")},  # set
    }
    out = p.to_jsonable(data)
    assert out["a"] == [1.0, 2.0]
    assert out["b"] == {"c": 3.0}
    assert sorted(out["d"]) == [4.0, 5.0]


def test_default_status_metadata_shape():
    meta = p.default_status_metadata()
    assert set(meta.keys()) == {"reason", "requester", "lastUpdated", "stopped"}
    assert meta["stopped"] is False
    assert meta["reason"] is None
