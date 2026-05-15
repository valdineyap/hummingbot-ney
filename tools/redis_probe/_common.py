"""Shared helpers for Phase 0 BitPreco Redis probe scripts.

All scripts under ``tools/redis_probe/`` import from here. Keeping
everything in one place avoids drift in connection setup, redaction
rules, and schema-discovery logic.

This file is **not** production code — it ships with the exploration
scripts and is excluded from the production data sources we'll build
later.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import ssl
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple

# We import redis.asyncio lazily inside `connect_pubsub` so that the
# scripts can fail with a clean message if the dep is missing.

CAPTURES_DIR = Path(__file__).resolve().parent / "captures"
LOG_FMT = "%(asctime)s %(levelname)s %(name)s :: %(message)s"


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure root logging once + return a named logger."""
    logging.basicConfig(level=level, format=LOG_FMT, stream=sys.stderr)
    return logging.getLogger(name)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

@dataclass
class RedisConfig:
    """Loaded from `BITPRECO_REDIS_*` env vars. See `.env.example`."""
    host: str
    port: int
    password: str
    tls: bool
    tls_verify: bool
    tls_ca_path: Optional[str]
    db: int
    user_id: Optional[str]

    @classmethod
    def from_env(cls) -> "RedisConfig":
        missing: List[str] = []

        def _get(key: str, required: bool = True, default: Optional[str] = None) -> str:
            v = os.environ.get(key, default)
            if required and (v is None or v == ""):
                missing.append(key)
                return ""
            return v or ""

        host = _get("BITPRECO_REDIS_HOST")
        port_s = _get("BITPRECO_REDIS_PORT", required=False, default="56379")
        password = _get("BITPRECO_REDIS_PASSWORD")
        tls_s = _get("BITPRECO_REDIS_TLS", required=False, default="true")
        tls_verify_s = _get("BITPRECO_REDIS_TLS_VERIFY", required=False, default="true")
        tls_ca_path = _get("BITPRECO_REDIS_TLS_CA_PATH", required=False, default="") or None
        db_s = _get("BITPRECO_REDIS_DB", required=False, default="0")
        user_id = _get("BITPRECO_USER_ID", required=False, default="") or None

        if missing:
            raise RuntimeError(
                "Missing required env vars (see .env.example): "
                + ", ".join(missing)
            )
        return cls(
            host=host,
            port=int(port_s),
            password=password,
            tls=tls_s.lower() in {"1", "true", "yes"},
            tls_verify=tls_verify_s.lower() in {"1", "true", "yes"},
            tls_ca_path=tls_ca_path,
            db=int(db_s),
            user_id=user_id,
        )


def _build_ssl_context(cfg: RedisConfig) -> Optional[ssl.SSLContext]:
    if not cfg.tls:
        return None
    ctx = ssl.create_default_context(cafile=cfg.tls_ca_path) if cfg.tls_ca_path \
        else ssl.create_default_context()
    if not cfg.tls_verify:
        # Explicitly disable. Only ever for local debugging — never in
        # production. Logged at WARN so it's visible.
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


@asynccontextmanager
async def connect_pubsub(cfg: RedisConfig) -> AsyncIterator[Any]:
    """Open a dedicated pubsub connection. Caller subscribes inside.

    Context manager: closes the connection on exit even on exception.
    """
    try:
        import redis.asyncio as redis_async
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "redis-py not available. `conda activate hummingbot` first."
        ) from e

    ssl_kwargs: Dict[str, Any] = {}
    if cfg.tls:
        ssl_kwargs["ssl"] = True
        ssl_kwargs["ssl_cert_reqs"] = "required" if cfg.tls_verify else "none"
        if cfg.tls_ca_path:
            ssl_kwargs["ssl_ca_certs"] = cfg.tls_ca_path

    client = redis_async.Redis(
        host=cfg.host,
        port=cfg.port,
        password=cfg.password,
        db=cfg.db,
        socket_keepalive=True,
        socket_connect_timeout=10,   # don't hang forever on bad host/TLS
        socket_timeout=10,           # ditto for any blocking op
        health_check_interval=30,
        decode_responses=True,
        **ssl_kwargs,
    )
    try:
        await asyncio.wait_for(client.ping(), timeout=10.0)
    except Exception:
        await client.aclose()
        raise

    pubsub = client.pubsub()
    try:
        yield client, pubsub
    finally:
        try:
            await pubsub.aclose()
        finally:
            await client.aclose()


# ---------------------------------------------------------------------------
# Capture file management — 0600 perms, JSONL append
# ---------------------------------------------------------------------------

def open_capture(script_name: str) -> Tuple[Path, "JsonlWriter"]:
    """Open a capture file, returning (path, writer)."""
    CAPTURES_DIR.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M")
    path = CAPTURES_DIR / f"{script_name}-{ts}.jsonl"
    # Best-effort umask for new file. tighten existing too.
    old_umask = os.umask(0o077)
    try:
        fh = path.open("a", encoding="utf-8")
    finally:
        os.umask(old_umask)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass  # FS may not support
    return path, JsonlWriter(fh)


class JsonlWriter:
    def __init__(self, fh):
        self._fh = fh

    def write(self, obj: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(obj, default=str))
        self._fh.write("\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


# ---------------------------------------------------------------------------
# Schema discovery — finds every (field, type) pair across N payloads
# ---------------------------------------------------------------------------

@dataclass
class SchemaDiscovery:
    """Accumulates field paths + observed types across many payloads.

    Walks JSON objects recursively. For lists, samples the type of the
    first element only (sufficient for orderbook bid/ask rows).
    """
    fields: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    payload_count: int = 0

    def observe(self, payload: Any, path: str = "$") -> None:
        self.payload_count += 1 if path == "$" else 0
        if isinstance(payload, dict):
            for k, v in payload.items():
                self.observe(v, f"{path}.{k}")
        elif isinstance(payload, list):
            entry = self.fields.setdefault(
                f"{path}[]", {"types": set(), "samples": []}
            )
            entry["types"].add("array")
            if payload:
                self.observe(payload[0], f"{path}[0]")
        else:
            entry = self.fields.setdefault(
                path, {"types": set(), "samples": []}
            )
            entry["types"].add(type(payload).__name__)
            if len(entry["samples"]) < 3 and payload is not None:
                entry["samples"].append(payload)

    def report(self) -> Dict[str, Any]:
        return {
            "payload_count": self.payload_count,
            "fields": {
                k: {
                    "types": sorted(v["types"]),
                    "samples": v["samples"],
                }
                for k, v in sorted(self.fields.items())
            },
        }


# ---------------------------------------------------------------------------
# Timestamp candidate detection
# ---------------------------------------------------------------------------

_TS_NAME_RE = re.compile(r"(time|stamp|ts|date|epoch)", re.IGNORECASE)


def looks_like_timestamp(name: str, value: Any) -> Optional[str]:
    """Heuristic: is this field a plausible timestamp?

    Returns the detected unit ('unix_s', 'unix_ms', 'unix_us', 'iso',
    or None).
    """
    name_match = bool(_TS_NAME_RE.search(name))
    if isinstance(value, (int, float)) and value > 0:
        # 10^9 ≈ 2001, 10^12 ≈ 2001 in ms, 10^15 in us. Use magnitudes.
        if 1_000_000_000 <= value < 10_000_000_000:
            return "unix_s" if name_match or True else None
        if 1_000_000_000_000 <= value < 10_000_000_000_000:
            return "unix_ms"
        if 1_000_000_000_000_000 <= value < 10_000_000_000_000_000:
            return "unix_us"
    if isinstance(value, str) and name_match:
        # crude ISO check
        if re.match(r"^\d{4}-\d{2}-\d{2}", value):
            return "iso"
    return None


@dataclass
class TimestampCandidate:
    field_path: str
    unit: str
    monotonic_per_key: Optional[bool] = None  # filled after analysis
    samples: List[Any] = field(default_factory=list)


def scan_timestamps(payload: Any, path: str = "$") -> List[Tuple[str, str, Any]]:
    """Return list of (field_path, detected_unit, sample_value) for
    every timestamp-looking field in this payload."""
    found: List[Tuple[str, str, Any]] = []
    if isinstance(payload, dict):
        for k, v in payload.items():
            sub_path = f"{path}.{k}"
            unit = looks_like_timestamp(k, v)
            if unit:
                found.append((sub_path, unit, v))
            found.extend(scan_timestamps(v, sub_path))
    elif isinstance(payload, list) and payload:
        found.extend(scan_timestamps(payload[0], f"{path}[0]"))
    return found


# ---------------------------------------------------------------------------
# Redaction — for safe quotation in published docs
# ---------------------------------------------------------------------------

_REDACT_KEYS = {
    "user_id", "userid", "id_bpbot", "idBPBot",
    "order_id", "orderid", "id",
    "api_key", "apikey", "secret", "token", "auth",
}

_REDACT_BALANCE_KEYS_RE = re.compile(
    r"^(?:[A-Z]{2,8}|balance|available|locked|total|amount|volume|cost|fee)$",
    re.IGNORECASE,
)


def redact(payload: Any) -> Any:
    """Return a deep copy of `payload` with sensitive fields masked.

    - Known id fields become placeholders like ``<USER_ID>``.
    - Balance / amount / price fields become a magnitude marker
      (eg. ``"~1e3"``) so structure is preserved but exact values aren't.
    - Strings that look like API tokens are masked.
    """
    if isinstance(payload, dict):
        out = {}
        keyset = {x.lower() for x in _REDACT_KEYS}
        for k, v in payload.items():
            kl = k.lower()
            if kl in keyset:
                out[k] = f"<{kl.upper()}>"
                continue
            # Recurse into nested structures BEFORE applying the
            # balance-key regex, otherwise short keys like "order"
            # match `[A-Z]{2,8}` and the dict gets ignored.
            if isinstance(v, (dict, list)):
                out[k] = redact(v)
                continue
            if _REDACT_BALANCE_KEYS_RE.match(k):
                out[k] = _mask_numeric(v)
            else:
                out[k] = redact(v)
        return out
    if isinstance(payload, list):
        return [redact(x) for x in payload]
    return payload


def _mask_numeric(value: Any) -> Any:
    """Replace a number-or-numeric-string with its magnitude marker."""
    if isinstance(value, bool):
        return value
    try:
        f = float(value)
    except (TypeError, ValueError):
        return value
    if f == 0:
        return "~0"
    sign = "-" if f < 0 else ""
    exp = int(abs(f).__format__(".0e").split("e")[1])
    return f"{sign}~1e{exp}"


# ---------------------------------------------------------------------------
# Tiny utility: estimate end-time from --duration / --until args
# ---------------------------------------------------------------------------

def deadline(duration_s: float) -> float:
    return time.monotonic() + duration_s


def time_left(deadline_mono: float) -> float:
    return max(0.0, deadline_mono - time.monotonic())


# ---------------------------------------------------------------------------
# REST auth helper — mirrors hummingbot/.../bitpreco_auth.py
# ---------------------------------------------------------------------------

@dataclass
class RestCreds:
    """REST auth for BitPreco.

    Holds the pre-computed ``auth_token`` (concatenation of
    ``secret_key + api_key``, same as
    ``BitprecoAuth._add_auth_token_to_data`` builds at runtime). The
    probe scripts don't need the individual pieces — they only POST
    the token in the request body.
    """
    auth_token: str

    @classmethod
    def from_env(cls) -> "RestCreds":
        token = os.environ.get("BITPRECO_API_AUTHTOKEN", "")
        if not token:
            raise RuntimeError(
                "Missing required env var BITPRECO_API_AUTHTOKEN "
                "(see .env.example). Set it to the concatenation "
                "secret_key + api_key."
            )
        return cls(auth_token=token)


def rest_url() -> str:
    """Return the REST trading URL. Honours env overrides like the connector."""
    internal = os.environ.get("BITPRECO_INTERNAL_API")
    if internal:
        return internal.rstrip("/")
    legacy = os.environ.get("BITPRECO_TRADING_URL")
    if legacy:
        return legacy.rstrip("/")
    return "https://api.bitpreco.com/trading"


async def rest_post(
    cmd: str,
    creds: RestCreds,
    extra: Optional[Dict[str, Any]] = None,
    timeout_s: float = 10.0,
) -> Dict[str, Any]:
    """POST a single cmd to the BitPreco REST trading endpoint.

    Uses aiohttp if available, else urllib in a thread. Returns the
    decoded JSON. Raises on transport error or non-JSON response.
    """
    body: Dict[str, Any] = {"cmd": cmd}
    if extra:
        body.update(extra)
    body["auth_token"] = creds.auth_token
    url = rest_url()

    try:
        import aiohttp
    except ImportError:
        aiohttp = None

    if aiohttp is not None:
        timeout = aiohttp.ClientTimeout(total=timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=body) as resp:
                text = await resp.text()
    else:  # pragma: no cover
        import urllib.request
        req = urllib.request.Request(
            url, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        loop = asyncio.get_event_loop()
        text = await loop.run_in_executor(
            None,
            lambda: urllib.request.urlopen(req, timeout=timeout_s).read().decode())

    try:
        return json.loads(text)
    except Exception as e:
        raise RuntimeError(f"non-JSON REST response: {text[:200]}") from e


# ---------------------------------------------------------------------------
# Iterable helpers
# ---------------------------------------------------------------------------

def percentile(values: Iterable[float], p: float) -> Optional[float]:
    """Simple p50/p99 — no numpy. Returns None on empty."""
    vs = sorted(values)
    if not vs:
        return None
    if p <= 0:
        return vs[0]
    if p >= 100:
        return vs[-1]
    k = (len(vs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(vs) - 1)
    return vs[f] + (vs[c] - vs[f]) * (k - f)
