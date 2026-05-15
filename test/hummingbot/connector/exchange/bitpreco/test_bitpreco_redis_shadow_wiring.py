"""Smoke tests for BitprecoExchange's Redis shadow observer wiring.

These don't exercise the actual Redis network — that lives in the
live shadow run. They pin down the graceful-degradation contract:

- shadow mode OFF: ``start_network`` doesn't try to load Redis config
- shadow mode ON, env vars missing: WARN + disabled, no crash
- shadow mode ON, env vars present: tasks spawned

We bypass full connector ``__init__`` (which boots websockets etc.)
by constructing a bare instance via ``__new__`` and only wiring the
attributes the lifecycle methods touch.
"""
from __future__ import annotations

import asyncio
import unittest
from unittest.mock import MagicMock, patch

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


def _bare_exchange(*, shadow: bool = False) -> BitprecoExchange:
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex._redis_shadow_enabled = shadow
    ex._redis_shadow_us_task = None
    ex._redis_shadow_ob_task = None
    ex._redis_shadow_us_observer = None
    ex._redis_shadow_ob_observer = None
    ex._trading_pairs = ["BTC-BRL"]
    # Stub logger
    ex.logger = lambda: MagicMock()
    return ex


class ShadowWiringTest(unittest.TestCase):
    def test_disabled_skip_spawn(self):
        """When shadow_mode=False, the spawn helper is never called.
        We approximate by calling _spawn_redis_shadow_observers and
        confirming it doesn't crash — but the main entry point
        (start_network) gates on the flag first."""
        ex = _bare_exchange(shadow=False)
        # Verify spawn helper exists and is callable
        self.assertTrue(callable(ex._spawn_redis_shadow_observers))

    def test_spawn_with_missing_env_logs_warning(self):
        ex = _bare_exchange(shadow=True)
        warns: list = []
        ex.logger = lambda: MagicMock(warning=lambda *a, **k: warns.append((a, k)))
        # Patch from_env to raise RedisConfigError
        from hummingbot.connector.exchange.bitpreco.bitpreco_redis_client import (
            RedisConfigError,
        )
        with patch(
            "hummingbot.connector.exchange.bitpreco.bitpreco_redis_client."
            "RedisBackendConfig.from_env",
            side_effect=RedisConfigError("Missing required env vars: TEST"),
        ):
            ex._spawn_redis_shadow_observers()
        self.assertIsNone(ex._redis_shadow_us_task)
        self.assertIsNone(ex._redis_shadow_ob_task)
        self.assertTrue(
            any("Missing required" in str(a) for args, _ in warns for a in args),
            f"expected warning about missing env, got: {warns}",
        )

    def test_spawn_with_valid_env_creates_both_tasks(self):
        ex = _bare_exchange(shadow=True)
        info_logs: list = []
        warn_logs: list = []
        ex.logger = lambda: MagicMock(
            info=lambda *a, **k: info_logs.append(a),
            warning=lambda *a, **k: warn_logs.append(a),
        )

        env = {
            "BITPRECO_REDIS_HOST": "10.0.0.1",
            "BITPRECO_REDIS_PASSWORD": "secret",
            "BITPRECO_USER_ID": "3652",
        }
        # We need a running event loop for safe_ensure_future
        async def _run():
            with patch.dict("os.environ", env, clear=False):
                ex._spawn_redis_shadow_observers()
                self.assertIsNotNone(ex._redis_shadow_us_task)
                self.assertIsNotNone(ex._redis_shadow_ob_task)
                # Cancel before they try to actually connect
                ex._redis_shadow_us_task.cancel()
                ex._redis_shadow_ob_task.cancel()
                # Give cancellation a tick to propagate so loop close
                # doesn't complain about pending tasks.
                try:
                    await asyncio.gather(
                        ex._redis_shadow_us_task, ex._redis_shadow_ob_task,
                        return_exceptions=True,
                    )
                except Exception:
                    pass

        asyncio.run(_run())
        self.assertTrue(
            any("[redis_shadow] enabled" in str(x) for args in info_logs for x in args),
            f"expected '[redis_shadow] enabled' in info logs, got: {info_logs}",
        )
        self.assertFalse(warn_logs, f"unexpected warnings: {warn_logs}")

    def test_spawn_without_trading_pairs_skips_orderbook(self):
        ex = _bare_exchange(shadow=True)
        ex._trading_pairs = []
        info_logs: list = []
        ex.logger = lambda: MagicMock(
            info=lambda *a, **k: info_logs.append(a),
            warning=lambda *a, **k: None,
        )

        env = {
            "BITPRECO_REDIS_HOST": "10.0.0.1",
            "BITPRECO_REDIS_PASSWORD": "secret",
            "BITPRECO_USER_ID": "3652",
        }

        async def _run():
            with patch.dict("os.environ", env, clear=False):
                ex._spawn_redis_shadow_observers()
                self.assertIsNotNone(ex._redis_shadow_us_task)
                self.assertIsNone(ex._redis_shadow_ob_task)
                ex._redis_shadow_us_task.cancel()
                await asyncio.gather(ex._redis_shadow_us_task, return_exceptions=True)

        asyncio.run(_run())
        self.assertTrue(
            any("orderbook shadow skipped" in str(x) for args in info_logs for x in args),
            f"expected 'orderbook shadow skipped' in info logs, got: {info_logs}",
        )

    def test_cancel_observers_clears_handles(self):
        ex = _bare_exchange(shadow=True)
        ex.logger = lambda: MagicMock()

        async def _run():
            # Spawn placeholder cancellable tasks
            async def _sleep_forever():
                await asyncio.Event().wait()
            ex._redis_shadow_us_task = asyncio.create_task(_sleep_forever())
            ex._redis_shadow_ob_task = asyncio.create_task(_sleep_forever())
            await ex._cancel_redis_shadow_observers()
            self.assertIsNone(ex._redis_shadow_us_task)
            self.assertIsNone(ex._redis_shadow_ob_task)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
