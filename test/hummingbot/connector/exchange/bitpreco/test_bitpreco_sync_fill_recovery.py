"""Tests for synchronous fill recovery on ``_place_order`` response.

When BitPreco's place_order REST returns ``ORDER_FULLY_EXECUTED`` or
``ORDER_PARTIALLY_EXECUTED``, the order has matched immediately on the
exchange side. The framework's normal path waits for the next periodic
status poll (~5s) to detect the fill — wasted latency. This module pins
the connector behaviour that emits the fill immediately.

Mirrors the cancel-side ``late_fill_recovery`` tests.
"""
import asyncio
import unittest
import unittest.mock
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange
from hummingbot.core.data_type.common import OrderType, TradeType


def _make_exchange(best_ask: Decimal = Decimal("400000")) -> BitprecoExchange:
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    ex._add_auth_token_to_req_body = lambda body: {**body, "auth_token": "stub"}
    ex.exchange_symbol_associated_to_pair = AsyncMock(return_value="BTC-BRL")
    ex.get_price = MagicMock(return_value=best_ask)
    return ex


class SyncFillEmissionOnPlaceOrderTest(unittest.IsolatedAsyncioTestCase):
    """When place_order REST returns FULLY_EXECUTED or PARTIALLY_EXECUTED,
    the connector should schedule an immediate trade-update emission via
    `_emit_synchronous_fill` rather than waiting for the periodic poll.
    """

    def _place(self, ex, **kw):
        defaults = dict(
            order_id="SBCBL_test_sync",
            trading_pair="BTC-BRL",
            amount=Decimal("0.0002"),
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("390000"),
        )
        defaults.update(kw)
        return ex._place_order(**defaults)

    def _setup_exchange(self, response, trade_updates):
        ex = _make_exchange()
        ex._api_request = AsyncMock(return_value=response)
        ex._order_tracker = MagicMock()
        ex._order_tracker.fetch_order = MagicMock(return_value=MagicMock(name="tracked_order"))
        ex._all_trade_updates_for_order = AsyncMock(return_value=trade_updates)
        return ex

    async def _await_pending_tasks(self):
        """Let ``asyncio.create_task`` work run on the event loop."""
        # Yield enough times for the scheduled task to start, await its
        # internal awaits (mocks resolve immediately), and finish.
        for _ in range(5):
            await asyncio.sleep(0)

    async def test_fully_executed_emits_trade_update(self):
        fake_trade = MagicMock(name="TradeUpdate")
        ex = self._setup_exchange(
            response={"success": True, "message_cod": "ORDER_FULLY_EXECUTED",
                      "order_id": "2055000301"},
            trade_updates=[fake_trade],
        )
        await self._place(ex)
        await self._await_pending_tasks()
        ex._order_tracker.process_trade_update.assert_called_once_with(fake_trade)

    async def test_partially_executed_triggers_cancel_not_direct_emit(self):
        """ORDER_PARTIALLY_EXECUTED → cancel immediately (don't emit yet).
        The cancel-side late_fill_recovery emits ONE TradeUpdate with the
        FINAL exec_amount (which may include fills that landed between the
        placement response and the cancel arrival)."""
        ex = self._setup_exchange(
            response={"success": True, "message_cod": "ORDER_PARTIALLY_EXECUTED",
                      "order_id": "2055000302"},
            trade_updates=[],
        )
        # Patch _cancel_partial_and_emit_final to verify it's invoked
        with unittest.mock.patch.object(
            ex, "_cancel_partial_and_emit_final", new=AsyncMock()
        ) as mock_cancel:
            await self._place(ex)
            await self._await_pending_tasks()
        mock_cancel.assert_awaited_once_with("2055000302", "SBCBL_test_sync")
        # No direct trade emission on the place_order path itself
        ex._order_tracker.process_trade_update.assert_not_called()

    async def test_order_created_does_not_trigger_fill_emission(self):
        """ORDER_CREATED = LIMIT just placed, no synchronous fill → no extra REST."""
        ex = self._setup_exchange(
            response={"success": True, "message_cod": "ORDER_CREATED",
                      "order_id": "2055000303"},
            trade_updates=[MagicMock()],
        )
        await self._place(ex)
        await self._await_pending_tasks()
        ex._all_trade_updates_for_order.assert_not_called()
        ex._order_tracker.process_trade_update.assert_not_called()

    async def test_empty_trade_updates_logs_warning_but_does_not_crash(self):
        """If executed_orders REST returns no matching trade (timing race),
        we log and move on — periodic poll will pick it up later."""
        ex = self._setup_exchange(
            response={"success": True, "message_cod": "ORDER_FULLY_EXECUTED",
                      "order_id": "2055000304"},
            trade_updates=[],
        )
        await self._place(ex)
        await self._await_pending_tasks()
        # Fetched once, but emitted nothing
        ex._all_trade_updates_for_order.assert_awaited_once()
        ex._order_tracker.process_trade_update.assert_not_called()

    async def test_fetch_failure_does_not_crash_caller(self):
        """If the executed_orders fetch raises, the place_order return value
        is still valid — periodic poll will eventually catch up."""
        ex = self._setup_exchange(
            response={"success": True, "message_cod": "ORDER_FULLY_EXECUTED",
                      "order_id": "2055000305"},
            trade_updates=[],
        )
        ex._all_trade_updates_for_order = AsyncMock(
            side_effect=ConnectionError("REST down")
        )
        oid, ts = await self._place(ex)
        await self._await_pending_tasks()
        self.assertEqual(oid, "2055000305")
        # Cleanly suppressed — no exception propagated to caller


class EmitFillsWithRetryTest(unittest.IsolatedAsyncioTestCase):
    """``_emit_fills_with_retry`` is the shared helper that both the
    cancel-side late_fill_recovery and the placement-side sync paths use.
    It retries the ``_all_trade_updates_for_order`` fetch on empty
    response to absorb BitPreco's executed_orders indexing race.
    """

    def _make(self, fetch_responses, tracker=None):
        """fetch_responses: list of lists — one per expected attempt.
        Each inner list is the return value of _all_trade_updates_for_order.
        """
        ex = _make_exchange()
        ex._order_tracker = tracker if tracker is not None else MagicMock()
        ex._all_trade_updates_for_order = AsyncMock(side_effect=fetch_responses)
        return ex

    def _tracked(self):
        return MagicMock(exchange_order_id="2055000400")

    async def test_emits_on_first_success(self):
        fake = MagicMock(name="TradeUpdate")
        ex = self._make([[fake]])
        count = await ex._emit_fills_with_retry(
            self._tracked(), context="test", max_attempts=3, backoff_sec=0.001,
        )
        self.assertEqual(count, 1)
        self.assertEqual(ex._all_trade_updates_for_order.await_count, 1)
        ex._order_tracker.process_trade_update.assert_called_once_with(fake)

    async def test_retries_on_empty_then_succeeds(self):
        """Empty first → retry → second attempt finds trade → emit."""
        fake = MagicMock(name="TradeUpdate")
        ex = self._make([[], [fake]])
        count = await ex._emit_fills_with_retry(
            self._tracked(), context="test", max_attempts=3, backoff_sec=0.001,
        )
        self.assertEqual(count, 1)
        self.assertEqual(ex._all_trade_updates_for_order.await_count, 2)
        ex._order_tracker.process_trade_update.assert_called_once_with(fake)

    async def test_returns_zero_when_all_attempts_empty(self):
        ex = self._make([[], [], []])
        count = await ex._emit_fills_with_retry(
            self._tracked(), context="test", max_attempts=3, backoff_sec=0.001,
        )
        self.assertEqual(count, 0)
        self.assertEqual(ex._all_trade_updates_for_order.await_count, 3)
        ex._order_tracker.process_trade_update.assert_not_called()

    async def test_exception_aborts_retries(self):
        """If fetch raises, we don't retry — better to defer to periodic poll."""
        ex = _make_exchange()
        ex._order_tracker = MagicMock()
        ex._all_trade_updates_for_order = AsyncMock(
            side_effect=ConnectionError("REST down")
        )
        count = await ex._emit_fills_with_retry(
            self._tracked(), context="test", max_attempts=3, backoff_sec=0.001,
        )
        self.assertEqual(count, 0)
        self.assertEqual(ex._all_trade_updates_for_order.await_count, 1)

    async def test_no_tracker_returns_zero_gracefully(self):
        """Test fixture path (no _order_tracker) — skip emission."""
        ex = _make_exchange()
        # No _order_tracker attribute
        ex._all_trade_updates_for_order = AsyncMock(return_value=[MagicMock()])
        count = await ex._emit_fills_with_retry(
            self._tracked(), context="test", max_attempts=3, backoff_sec=0.001,
        )
        self.assertEqual(count, 0)
        ex._all_trade_updates_for_order.assert_not_called()


class CancelPartialAndEmitFinalTest(unittest.IsolatedAsyncioTestCase):
    """``_cancel_partial_and_emit_final`` handles ORDER_PARTIALLY_EXECUTED
    via cancel-first-then-emit-final. Robust against tracker registration
    lag (race a) — retries the lookup with backoff before giving up.
    """

    def _make(self):
        ex = _make_exchange()
        ex._order_tracker = MagicMock()
        ex._place_cancel = AsyncMock(return_value=True)
        return ex

    async def test_calls_place_cancel_when_tracker_has_order(self):
        ex = self._make()
        fake_order = MagicMock(exchange_order_id=None)
        ex._order_tracker.fetch_order.return_value = fake_order

        await ex._cancel_partial_and_emit_final("2055000500", "SBCBL_test")

        # Sets exchange_order_id if missing
        fake_order.update_exchange_order_id.assert_called_once_with("2055000500")
        # Delegates to _place_cancel (which has late_fill_recovery)
        ex._place_cancel.assert_awaited_once_with("SBCBL_test", fake_order)

    async def test_retries_tracker_lookup_on_initial_miss(self):
        """If tracker hasn't registered the order yet, retry briefly."""
        ex = self._make()
        fake_order = MagicMock(exchange_order_id="2055000501")
        # First 2 lookups return None, 3rd succeeds
        ex._order_tracker.fetch_order.side_effect = [None, None, fake_order]

        await ex._cancel_partial_and_emit_final("2055000501", "SBCBL_test")

        self.assertEqual(ex._order_tracker.fetch_order.call_count, 3)
        ex._place_cancel.assert_awaited_once()

    async def test_gives_up_after_all_attempts_fail(self):
        ex = self._make()
        ex._order_tracker.fetch_order.return_value = None  # never registers

        await ex._cancel_partial_and_emit_final("2055000502", "SBCBL_test")

        self.assertEqual(ex._order_tracker.fetch_order.call_count, 6)
        ex._place_cancel.assert_not_called()

    async def test_cancel_failure_is_logged_not_raised(self):
        """If _place_cancel raises, swallow it — watchdog will retry."""
        ex = self._make()
        fake_order = MagicMock(exchange_order_id="2055000503")
        ex._order_tracker.fetch_order.return_value = fake_order
        ex._place_cancel = AsyncMock(side_effect=RuntimeError("network down"))

        # Should NOT raise
        await ex._cancel_partial_and_emit_final("2055000503", "SBCBL_test")
        ex._place_cancel.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
