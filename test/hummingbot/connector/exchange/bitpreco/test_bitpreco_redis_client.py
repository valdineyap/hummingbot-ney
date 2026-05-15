"""Unit tests for RedisBackendConfig and channel-name derivation.

We don't exercise actual network connections here — those are
covered by Phase 0 probes (`tools/redis_probe/01_connect.py`) and
the upcoming Phase 1A live shadow run. This file pins down the
env-var parsing + URL/channel building so a bad config never
makes it to runtime.
"""
from __future__ import annotations

import unittest

from hummingbot.connector.exchange.bitpreco.bitpreco_redis_client import (
    RedisBackendConfig,
    RedisConfigError,
    RedisConnectionFactory,
)


def _minimal_env(**overrides) -> dict:
    base = {
        "BITPRECO_REDIS_HOST": "10.0.0.1",
        "BITPRECO_REDIS_PASSWORD": "s3cret",
        "BITPRECO_USER_ID": "3652",
    }
    base.update(overrides)
    return base


class RedisBackendConfigTest(unittest.TestCase):
    def test_minimal_env_loads(self):
        cfg = RedisBackendConfig.from_env(_minimal_env())
        self.assertEqual(cfg.host, "10.0.0.1")
        self.assertEqual(cfg.password, "s3cret")
        self.assertEqual(cfg.user_id, "3652")
        # Defaults
        self.assertEqual(cfg.port, 56379)
        self.assertEqual(cfg.db, 0)
        self.assertFalse(cfg.tls)
        self.assertTrue(cfg.tls_verify)
        self.assertIsNone(cfg.tls_ca_path)

    def test_missing_host_raises(self):
        env = _minimal_env()
        env.pop("BITPRECO_REDIS_HOST")
        with self.assertRaises(RedisConfigError) as ctx:
            RedisBackendConfig.from_env(env)
        self.assertIn("BITPRECO_REDIS_HOST", str(ctx.exception))

    def test_missing_password_raises(self):
        env = _minimal_env()
        env.pop("BITPRECO_REDIS_PASSWORD")
        with self.assertRaises(RedisConfigError):
            RedisBackendConfig.from_env(env)

    def test_missing_user_id_raises(self):
        env = _minimal_env()
        env.pop("BITPRECO_USER_ID")
        with self.assertRaises(RedisConfigError):
            RedisBackendConfig.from_env(env)

    def test_empty_string_counts_as_missing(self):
        env = _minimal_env(BITPRECO_REDIS_HOST="")
        with self.assertRaises(RedisConfigError):
            RedisBackendConfig.from_env(env)

    def test_bad_port_raises(self):
        env = _minimal_env(BITPRECO_REDIS_PORT="not-a-number")
        with self.assertRaises(RedisConfigError) as ctx:
            RedisBackendConfig.from_env(env)
        self.assertIn("Bad numeric", str(ctx.exception))

    def test_bool_parsing(self):
        for truthy in ("true", "TRUE", "1", "yes", "YES"):
            cfg = RedisBackendConfig.from_env(_minimal_env(BITPRECO_REDIS_TLS=truthy))
            self.assertTrue(cfg.tls, f"failed for {truthy!r}")
        for falsy in ("false", "0", "no", "", "anything-else"):
            cfg = RedisBackendConfig.from_env(_minimal_env(BITPRECO_REDIS_TLS=falsy))
            self.assertFalse(cfg.tls, f"failed for {falsy!r}")

    def test_tls_ca_path_blank_becomes_none(self):
        cfg = RedisBackendConfig.from_env(_minimal_env(BITPRECO_REDIS_TLS_CA_PATH=""))
        self.assertIsNone(cfg.tls_ca_path)
        cfg = RedisBackendConfig.from_env(
            _minimal_env(BITPRECO_REDIS_TLS_CA_PATH="/etc/ssl/ca.pem"))
        self.assertEqual(cfg.tls_ca_path, "/etc/ssl/ca.pem")

    def test_from_env_defaults_to_os_environ(self):
        # Just verify the call signature works without args.
        # We can't actually load real env safely, but if the required
        # vars happen to be set the call succeeds; if not, it raises
        # RedisConfigError. Either is fine — we just want no TypeError.
        try:
            RedisBackendConfig.from_env()
        except RedisConfigError:
            pass


class ChannelNameTest(unittest.TestCase):
    def test_update_channel(self):
        cfg = RedisBackendConfig.from_env(_minimal_env(BITPRECO_USER_ID="42"))
        self.assertEqual(cfg.update_channel, "update:42")

    def test_orderbook_channel(self):
        cfg = RedisBackendConfig.from_env(_minimal_env())
        self.assertEqual(cfg.orderbook_channel("BTC-BRL"), "orderbook:BTC-BRL")
        self.assertEqual(cfg.orderbook_channel("ETH-BRL"), "orderbook:ETH-BRL")


class RedisConnectionFactoryTest(unittest.TestCase):
    """Smoke tests — verify the factory holds onto the config and
    constructs without raising. Actual `create_pubsub_connection`
    needs a live Redis (out of scope here)."""

    def test_factory_exposes_config(self):
        cfg = RedisBackendConfig.from_env(_minimal_env())
        factory = RedisConnectionFactory(cfg)
        self.assertIs(factory.config, cfg)

    def test_ssl_kwargs_off_by_default(self):
        cfg = RedisBackendConfig.from_env(_minimal_env())
        factory = RedisConnectionFactory(cfg)
        self.assertEqual(factory._ssl_kwargs(), {})  # type: ignore[attr-defined]

    def test_ssl_kwargs_on_when_tls_true(self):
        cfg = RedisBackendConfig.from_env(_minimal_env(BITPRECO_REDIS_TLS="true"))
        factory = RedisConnectionFactory(cfg)
        kw = factory._ssl_kwargs()  # type: ignore[attr-defined]
        self.assertTrue(kw["ssl"])
        # When verify=true (default), cert_reqs should require
        self.assertIn("ssl_cert_reqs", kw)

    def test_ssl_kwargs_ca_path_propagated(self):
        env = _minimal_env(BITPRECO_REDIS_TLS="true",
                           BITPRECO_REDIS_TLS_CA_PATH="/etc/ssl/ca.pem")
        cfg = RedisBackendConfig.from_env(env)
        factory = RedisConnectionFactory(cfg)
        kw = factory._ssl_kwargs()  # type: ignore[attr-defined]
        self.assertEqual(kw["ssl_ca_certs"], "/etc/ssl/ca.pem")


if __name__ == "__main__":
    unittest.main()
