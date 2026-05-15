"""Unit tests for the Phase 2 backend factory.

Pins down ``_create_user_stream_data_source`` and
``_create_order_book_data_source`` selecting the right class
based on ``bitpreco_data_backend``. Bare-instance pattern avoids
booting the full connector.
"""
from __future__ import annotations

import asyncio
import unittest
from unittest.mock import MagicMock, patch

from hummingbot.connector.exchange.bitpreco.bitpreco_api_order_book_data_source import (
    BitprecoAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.bitpreco.bitpreco_api_user_stream_data_source import (
    BitprecoAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


def _bare(*, backend: str = "legacy") -> BitprecoExchange:
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex._data_backend = backend
    ex._trading_pairs = ["BTC-BRL"]
    ex._redis_factory = None
    ex._balance_refresh_in_flight = None
    ex._redis_shadow_enabled = False
    ex._redis_shadow_us_task = None
    ex._redis_shadow_ob_task = None
    ex._redis_shadow_us_observer = None
    ex._redis_shadow_ob_observer = None
    # _auth / _web_assistants_factory: set by ExchangePyBase.__init__
    # which we're skipping. Stub minimal mocks.
    ex._auth = MagicMock()
    ex._web_assistants_factory = MagicMock()
    ex._domain = "bitpreco_trading"
    ex.logger = lambda: MagicMock()
    return ex


_FAKE_ENV = {
    "BITPRECO_REDIS_HOST": "10.0.0.1",
    "BITPRECO_REDIS_PASSWORD": "secret",
    "BITPRECO_USER_ID": "3652",
}


class DataBackendFactoryTest(unittest.TestCase):

    def test_legacy_user_stream(self):
        ex = _bare(backend="legacy")
        ds = ex._create_user_stream_data_source()
        self.assertIsInstance(ds, BitprecoAPIUserStreamDataSource)

    def test_legacy_order_book(self):
        ex = _bare(backend="legacy")
        ds = ex._create_order_book_data_source()
        self.assertIsInstance(ds, BitprecoAPIOrderBookDataSource)

    def test_redis_user_stream(self):
        ex = _bare(backend="redis")
        with patch.dict("os.environ", _FAKE_ENV, clear=False):
            ds = ex._create_user_stream_data_source()
        # Don't import the class at top-level (lazy load on demand)
        from hummingbot.connector.exchange.bitpreco.bitpreco_redis_user_stream import (
            BitprecoRedisUserStream,
        )
        self.assertIsInstance(ds, BitprecoRedisUserStream)

    def test_redis_order_book(self):
        ex = _bare(backend="redis")
        with patch.dict("os.environ", _FAKE_ENV, clear=False):
            ds = ex._create_order_book_data_source()
        from hummingbot.connector.exchange.bitpreco.bitpreco_redis_order_book_data_source import (
            BitprecoRedisOrderBookDataSource,
        )
        self.assertIsInstance(ds, BitprecoRedisOrderBookDataSource)
        # AND it's still a subclass of the legacy class — parser
        # inheritance is part of the contract.
        self.assertIsInstance(ds, BitprecoAPIOrderBookDataSource)

    def test_redis_factory_is_lazy_and_cached(self):
        ex = _bare(backend="redis")
        with patch.dict("os.environ", _FAKE_ENV, clear=False):
            f1 = ex._get_redis_factory()
            f2 = ex._get_redis_factory()
        self.assertIs(f1, f2, "factory must be cached")

    def test_redis_missing_env_raises(self):
        ex = _bare(backend="redis")
        # No relevant env vars — RedisConfigError should bubble up
        # (lazy + fail loud, not silent legacy fallback).
        empty = {k: "" for k in _FAKE_ENV}
        with patch.dict("os.environ", empty, clear=False):
            with self.assertRaises(Exception):
                ex._create_user_stream_data_source()

    def test_data_backend_property(self):
        ex = _bare(backend="redis")
        self.assertEqual(ex.data_backend, "redis")
        ex2 = _bare(backend="legacy")
        self.assertEqual(ex2.data_backend, "legacy")


class ForceBalanceRefreshTest(unittest.TestCase):
    """Phase 2 API: REST balance fetch with Future coalescing.

    Uses manual loop management (new_event_loop + set_event_loop) so
    other bitpreco tests that rely on get_event_loop() still have a
    valid current loop after we run.
    """

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())

    def _await(self, coro):
        return self.loop.run_until_complete(coro)

    def test_single_caller(self):
        ex = _bare()
        called = 0

        async def _fake_update(_trigger="?"):
            nonlocal called
            called += 1
            await asyncio.sleep(0.01)

        ex._update_balances = _fake_update
        self._await(ex.force_balance_refresh())
        self.assertEqual(called, 1)

    def test_concurrent_callers_coalesce(self):
        ex = _bare()
        called = 0

        async def _fake_update(_trigger="?"):
            nonlocal called
            called += 1
            await asyncio.sleep(0.05)

        ex._update_balances = _fake_update

        async def _run():
            await asyncio.gather(
                ex.force_balance_refresh(),
                ex.force_balance_refresh(),
                ex.force_balance_refresh(),
            )

        self._await(_run())
        self.assertEqual(called, 1, "concurrent callers must coalesce into 1 REST call")

    def test_sequential_callers_each_refresh(self):
        ex = _bare()
        called = 0

        async def _fake_update(_trigger="?"):
            nonlocal called
            called += 1

        ex._update_balances = _fake_update

        async def _run():
            await ex.force_balance_refresh()
            await ex.force_balance_refresh()

        self._await(_run())
        self.assertEqual(called, 2)


class HandleRedisEnvelopeTest(unittest.TestCase):
    """The Redis listener dispatches to _handle_redis_envelope. The
    handler must:
      - schedule _update_order_status on every terminal event
      - schedule force_balance_refresh only when balance actually moved
      - skip CREATED events (no-op)
    """

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())

    def _make_ex(self):
        ex = _bare(backend="redis")
        ex._update_order_status_calls = 0
        ex._force_balance_refresh_calls = 0

        async def _us():
            ex._update_order_status_calls += 1

        async def _fbr():
            ex._force_balance_refresh_calls += 1

        ex._update_order_status = _us
        ex.force_balance_refresh = _fbr
        return ex

    def _run(self, ex, envelope):
        async def _wrap():
            await ex._handle_redis_envelope(envelope)
            # Let any scheduled tasks finish
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(_wrap())

    def _envelope(self, cod, exec_amount=None):
        from decimal import Decimal
        from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import (
            EventEnvelope, EventType, OrderInfo,
        )
        return EventEnvelope(
            event_type=EventType(cod),
            order=OrderInfo(
                exchange_order_id="2063812439",
                market="BTC-BRL",
                side="BUY",
                status="FILLED" if cod == "ORDER_FULLY_EXECUTED" else "EMPTY",
                amount=Decimal("0.0002"),
                price=Decimal("399514"),
                exec_amount=Decimal(exec_amount) if exec_amount is not None else None,
            ),
            balance=None,
            recv_ts=100.0,
            event_ts=99.997,
        )

    def test_created_is_noop(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("BUY_ORDER_CREATED"))
        self.assertEqual(ex._update_order_status_calls, 0)
        self.assertEqual(ex._force_balance_refresh_calls, 0)

    def test_fully_executed_triggers_both(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_FULLY_EXECUTED", exec_amount="0.0002"))
        self.assertEqual(ex._update_order_status_calls, 1)
        self.assertEqual(ex._force_balance_refresh_calls, 1)

    def test_cancel_without_exec_skips_balance_refresh(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_CANCELED", exec_amount="0"))
        # Order status still refreshed — terminal event matters
        self.assertEqual(ex._update_order_status_calls, 1)
        # But balance untouched: a never-filled cancel doesn't move it
        self.assertEqual(ex._force_balance_refresh_calls, 0)

    def test_cancel_with_partial_fill_triggers_balance(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_CANCELED", exec_amount="0.0001"))
        self.assertEqual(ex._update_order_status_calls, 1)
        self.assertEqual(ex._force_balance_refresh_calls, 1)

    def test_partially_executed_triggers_both(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_PARTIALLY_EXECUTED", exec_amount="0.0001"))
        self.assertEqual(ex._update_order_status_calls, 1)
        self.assertEqual(ex._force_balance_refresh_calls, 1)


if __name__ == "__main__":
    unittest.main()
