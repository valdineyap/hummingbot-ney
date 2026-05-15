"""Redis pub/sub connection factory for the BitPreco backend.

Two design choices documented in the plan
(`.claude/plans/golden-crafting-candle.md`):

1. **Config singleton, dedicated connections.** We hold one
   ``RedisBackendConfig`` instance per process (cheap, immutable
   after construction) and hand out new pub/sub connections on
   demand. The user-stream subscriber and the orderbook subscriber
   get separate connections so that orderbook backpressure (high
   frequency, large snapshots) can't delay fill events on the
   private channel.

2. **Lazy connection.** The connection isn't opened at config-load
   time. ``create_pubsub_connection()`` (async) opens it and pings
   before returning. This means the connector can boot in legacy
   mode with Redis env vars missing; only flipping shadow mode on
   will exercise the connection.

Credentials come from environment variables (loaded from `.env`):

  BITPRECO_REDIS_HOST                 required
  BITPRECO_REDIS_PORT                 default 56379
  BITPRECO_REDIS_PASSWORD             required
  BITPRECO_REDIS_TLS                  true/false (default false, see
                                      Phase 0 findings: BitPreco's
                                      Redis lives on a private VPC)
  BITPRECO_REDIS_TLS_VERIFY           true/false (default true; only
                                      meaningful when TLS=true)
  BITPRECO_REDIS_TLS_CA_PATH          optional CA bundle path
  BITPRECO_REDIS_DB                   default 0
  BITPRECO_USER_ID                    required (the numeric idBPBot
                                      used to build update:<id>)

Why env vars instead of the connector YAML
==========================================

The connector config (`conf/connectors/bitpreco.yml`) is encrypted
by Hummingbot and intended for credentials the strategy itself
uses (api_key, api_secret). The Redis credentials are a separate
infrastructure detail — they live alongside operational env vars
(`BITPRECO_INTERNAL_*`, `BINANCE_SBE_API_KEY`) in `.env`.
"""
from __future__ import annotations

import asyncio
import logging
import os
import ssl
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, cast

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class RedisBackendConfig:
    """Immutable snapshot of the Redis env vars at load time.

    Construct via :meth:`from_env`. Raises ``RedisConfigError`` if a
    required field is missing — callers can catch and fall back to
    legacy mode.
    """
    host: str
    port: int
    password: str
    db: int
    user_id: str
    tls: bool
    tls_verify: bool
    tls_ca_path: Optional[str]

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "RedisBackendConfig":
        e: Mapping[str, str] = env if env is not None else cast(Mapping[str, str], os.environ)
        required = ("BITPRECO_REDIS_HOST", "BITPRECO_REDIS_PASSWORD",
                    "BITPRECO_USER_ID")
        missing = [k for k in required if not e.get(k)]
        if missing:
            raise RedisConfigError(
                "Missing required env vars: " + ", ".join(missing)
                + ". See .env.example for the full template.")

        def _bool(key: str, default: str) -> bool:
            return str(e.get(key, default)).strip().lower() in {"1", "true", "yes"}

        try:
            port = int(e.get("BITPRECO_REDIS_PORT", "56379"))
            db = int(e.get("BITPRECO_REDIS_DB", "0"))
        except ValueError as exc:
            raise RedisConfigError(f"Bad numeric Redis env var: {exc}") from exc

        return cls(
            host=e["BITPRECO_REDIS_HOST"],
            port=port,
            password=e["BITPRECO_REDIS_PASSWORD"],
            db=db,
            user_id=e["BITPRECO_USER_ID"],
            tls=_bool("BITPRECO_REDIS_TLS", "false"),
            tls_verify=_bool("BITPRECO_REDIS_TLS_VERIFY", "true"),
            tls_ca_path=e.get("BITPRECO_REDIS_TLS_CA_PATH") or None,
        )

    # ------------------------------------------------------------------
    # Derived channel names
    # ------------------------------------------------------------------

    @property
    def update_channel(self) -> str:
        return f"{CONSTANTS.REDIS_UPDATE_CHANNEL_PREFIX}:{self.user_id}"

    def orderbook_channel(self, trading_pair: str) -> str:
        return f"{CONSTANTS.REDIS_ORDERBOOK_CHANNEL_PREFIX}:{trading_pair}"


class RedisConfigError(RuntimeError):
    """Raised when env vars are missing or malformed."""


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

class RedisConnectionFactory:
    """Builds a fresh ``redis.asyncio.Redis`` client per call.

    No connection pool sharing — pub/sub connections aren't reusable
    for other commands so a single per-subscriber client is the
    correct pattern. This keeps backpressure isolated between the
    user-stream and orderbook subscribers (see plan §"Cliente Redis
    — config-singleton, conexões dedicadas").
    """

    def __init__(self, config: RedisBackendConfig) -> None:
        self._config = config

    @property
    def config(self) -> RedisBackendConfig:
        return self._config

    def _ssl_kwargs(self) -> Dict[str, Any]:
        if not self._config.tls:
            return {}
        kwargs: Dict[str, Any] = {
            "ssl": True,
            "ssl_cert_reqs": ssl.CERT_REQUIRED if self._config.tls_verify
            else ssl.CERT_NONE,
        }
        if self._config.tls_ca_path:
            kwargs["ssl_ca_certs"] = self._config.tls_ca_path
        return kwargs

    async def create_pubsub_connection(self) -> Any:
        """Open a new Redis client + PING it. Returns the Redis
        client object (caller calls ``.pubsub()`` to get the pubsub
        handle and is responsible for closing). Raises on PING
        failure within ``REDIS_CONNECT_TIMEOUT_SEC``."""
        # Import lazily so the connector can boot in legacy mode
        # without redis-py installed.
        import redis.asyncio as redis_async   # type: ignore[import-not-found]

        client = redis_async.Redis(
            host=self._config.host,
            port=self._config.port,
            password=self._config.password,
            db=self._config.db,
            socket_connect_timeout=CONSTANTS.REDIS_CONNECT_TIMEOUT_SEC,
            socket_timeout=CONSTANTS.REDIS_SOCKET_TIMEOUT_SEC,
            socket_keepalive=True,
            health_check_interval=CONSTANTS.REDIS_HEALTH_CHECK_INTERVAL_SEC,
            decode_responses=True,
            **self._ssl_kwargs(),
        )
        try:
            # client.ping() is an awaitable in redis.asyncio even though
            # the sync redis.Redis declares it as `bool` — wrap to make
            # both pyright and runtime happy.
            await asyncio.wait_for(
                cast(Any, client.ping()),
                timeout=CONSTANTS.REDIS_CONNECT_TIMEOUT_SEC,
            )
        except Exception:
            try:
                await client.aclose()
            except Exception:
                pass
            raise
        return client
