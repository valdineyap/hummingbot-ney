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
    """PR 3b: ``_handle_redis_envelope`` is now optimistic.

    Routing by ``message_cod``:
      - CREATED (BUY/SELL): no-op
      - ORDER_CANCELED with exec=0: emit OrderUpdate(CANCELED) direct
        to ``_order_tracker.process_order_update`` — **zero REST**
      - ORDER_FULLY_EXECUTED: schedule ``_emit_synchronous_fill`` +
        ``force_balance_refresh``
      - ORDER_PARTIALLY_EXECUTED / CANCEL with exec>0: schedule
        ``_cancel_partial_and_emit_final`` + ``force_balance_refresh``
      - Unknown xid (not in tracker): fall back to ``_update_order_status``
    """

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())

    def _make_ex(self, *, tracked_xid: str = "2063812439"):
        """Build a bare exchange instance and wire a fake order_tracker.

        ``tracked_xid`` controls which exchange_order_id the fake
        tracker pretends to know. Pass None to test the unknown-xid
        fallback.
        """
        ex = _bare(backend="redis")
        ex._update_order_status_calls = 0
        ex._force_balance_refresh_calls = 0
        ex._emit_synchronous_fill_calls = []
        ex._cancel_partial_and_emit_final_calls = []
        ex._process_order_update_calls = []

        async def _us():
            ex._update_order_status_calls += 1

        async def _fbr():
            ex._force_balance_refresh_calls += 1

        async def _esf(xid, cid, msg_cod):
            ex._emit_synchronous_fill_calls.append((xid, cid, msg_cod))

        async def _cpef(xid, cid):
            ex._cancel_partial_and_emit_final_calls.append((xid, cid))

        ex._update_order_status = _us
        ex.force_balance_refresh = _fbr
        ex._emit_synchronous_fill = _esf
        ex._cancel_partial_and_emit_final = _cpef

        # Fake tracker with one in-flight order for the given xid.
        tracker = MagicMock()
        if tracked_xid is not None:
            from unittest.mock import MagicMock as _MM
            in_flight = _MM()
            in_flight.client_order_id = "BBCBL_test_cid_42"
            in_flight.exchange_order_id = tracked_xid
            in_flight.trading_pair = "BTC-BRL"
            tracker.active_orders = {"BBCBL_test_cid_42": in_flight}
        else:
            tracker.active_orders = {}
        tracker.process_order_update = lambda upd: ex._process_order_update_calls.append(upd)
        ex._order_tracker = tracker
        return ex

    def _run(self, ex, envelope):
        async def _wrap():
            await ex._handle_redis_envelope(envelope)
            # Let any scheduled tasks finish
            await asyncio.sleep(0.01)
        self.loop.run_until_complete(_wrap())

    def _envelope(self, cod, exec_amount=None, xid="2063812439"):
        from decimal import Decimal
        from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import (
            EventEnvelope, EventType, OrderInfo,
        )
        return EventEnvelope(
            event_type=EventType(cod),
            order=OrderInfo(
                exchange_order_id=xid,
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

    # ---- CREATED is no-op ----

    def test_buy_created_is_noop(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("BUY_ORDER_CREATED"))
        self.assertEqual(ex._update_order_status_calls, 0)
        self.assertEqual(ex._force_balance_refresh_calls, 0)
        self.assertEqual(ex._emit_synchronous_fill_calls, [])
        self.assertEqual(ex._cancel_partial_and_emit_final_calls, [])
        self.assertEqual(ex._process_order_update_calls, [])

    def test_sell_created_is_noop(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("SELL_ORDER_CREATED"))
        self.assertEqual(ex._update_order_status_calls, 0)

    # ---- CANCEL exec=0: optimistic path, zero REST ----

    def test_cancel_without_exec_is_optimistic_no_rest(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_CANCELED", exec_amount="0"))
        # NO REST sweep, NO sync_fill, NO balance refresh
        self.assertEqual(ex._update_order_status_calls, 0)
        self.assertEqual(ex._force_balance_refresh_calls, 0)
        self.assertEqual(ex._emit_synchronous_fill_calls, [])
        self.assertEqual(ex._cancel_partial_and_emit_final_calls, [])
        # Single OrderUpdate(CANCELED) pushed to tracker
        self.assertEqual(len(ex._process_order_update_calls), 1)
        upd = ex._process_order_update_calls[0]
        from hummingbot.core.data_type.in_flight_order import OrderState
        self.assertEqual(upd.new_state, OrderState.CANCELED)
        self.assertEqual(upd.exchange_order_id, "2063812439")
        self.assertEqual(upd.client_order_id, "BBCBL_test_cid_42")

    def test_cancel_with_none_exec_treated_as_zero(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_CANCELED", exec_amount=None))
        # exec_amount=None → still optimistic cancel path
        self.assertEqual(len(ex._process_order_update_calls), 1)
        self.assertEqual(ex._update_order_status_calls, 0)

    # ---- FULLY_EXECUTED: sync_fill + balance refresh, no generic sweep ----

    def test_fully_executed_goes_through_sync_fill(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_FULLY_EXECUTED", exec_amount="0.0002"))
        # No generic _update_order_status, no direct OrderUpdate emission
        self.assertEqual(ex._update_order_status_calls, 0)
        self.assertEqual(ex._process_order_update_calls, [])
        # Single sync_fill scheduled with the right xid/cid
        self.assertEqual(len(ex._emit_synchronous_fill_calls), 1)
        xid, cid, cod = ex._emit_synchronous_fill_calls[0]
        self.assertEqual(xid, "2063812439")
        self.assertEqual(cid, "BBCBL_test_cid_42")
        self.assertEqual(cod, "ORDER_FULLY_EXECUTED")
        # Balance refresh fired
        self.assertEqual(ex._force_balance_refresh_calls, 1)

    # ---- PARTIAL / CANCEL with exec>0: cancel-and-emit-final ----

    def test_partial_goes_through_cancel_and_emit_final(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_PARTIALLY_EXECUTED",
                                     exec_amount="0.0001"))
        self.assertEqual(len(ex._cancel_partial_and_emit_final_calls), 1)
        xid, cid = ex._cancel_partial_and_emit_final_calls[0]
        self.assertEqual(xid, "2063812439")
        self.assertEqual(cid, "BBCBL_test_cid_42")
        self.assertEqual(ex._force_balance_refresh_calls, 1)
        # NOT direct OrderUpdate, NOT sync_fill
        self.assertEqual(ex._process_order_update_calls, [])
        self.assertEqual(ex._emit_synchronous_fill_calls, [])

    def test_cancel_with_partial_fill_goes_through_cancel_and_emit_final(self):
        ex = self._make_ex()
        self._run(ex, self._envelope("ORDER_CANCELED", exec_amount="0.0001"))
        # Partial-then-cancel route
        self.assertEqual(len(ex._cancel_partial_and_emit_final_calls), 1)
        self.assertEqual(ex._force_balance_refresh_calls, 1)
        # NOT the optimistic direct-cancel route
        self.assertEqual(ex._process_order_update_calls, [])

    # ---- Unknown xid: fall back to REST sweep ----

    def test_unknown_xid_falls_back_to_rest_sweep(self):
        ex = self._make_ex(tracked_xid=None)
        self._run(ex, self._envelope("ORDER_CANCELED", exec_amount="0"))
        # Without a tracked order we don't have a cid → can't build
        # an OrderUpdate. Safety fallback: schedule a REST sweep.
        self.assertEqual(ex._update_order_status_calls, 1)
        self.assertEqual(ex._process_order_update_calls, [])
        self.assertEqual(ex._emit_synchronous_fill_calls, [])

    def test_unknown_xid_created_still_noop(self):
        ex = self._make_ex(tracked_xid=None)
        self._run(ex, self._envelope("BUY_ORDER_CREATED"))
        # CREATED is no-op regardless of tracker state
        self.assertEqual(ex._update_order_status_calls, 0)
        self.assertEqual(ex._process_order_update_calls, [])


if __name__ == "__main__":
    unittest.main()
