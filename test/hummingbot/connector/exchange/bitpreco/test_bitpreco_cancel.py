"""Tests for ``_place_cancel`` resilience improvements (Task 2.1).

The two new behaviours pinned down here:

  * If the order is still in PENDING_CREATE when cancel is requested, the
    connector now polls briefly (up to 1.5s) for the exchange_order_id
    instead of immediately returning False. This closes the race that
    produces orphans when create REST latency exceeds the framework's
    cancel cadence.

  * On timeout (id never arrives), the call still returns False so the
    framework can retry without the connector blocking forever.
"""

import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


def _make_tracked_order(state: OrderState = OrderState.PENDING_CREATE,
                        exchange_order_id=None) -> InFlightOrder:
    return InFlightOrder(
        client_order_id="SBCBL_test_cancel",
        trading_pair="BTC-BRL",
        order_type=OrderType.LIMIT_MAKER,
        trade_type=TradeType.SELL,
        amount=Decimal("0.0002"),
        price=Decimal("400000"),
        creation_timestamp=1.0,
        exchange_order_id=exchange_order_id,
        initial_state=state,
    )


def _make_exchange() -> BitprecoExchange:
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    ex._add_auth_token_to_req_body = lambda body: {**body, "auth_token": "stub"}
    return ex


class PlaceCancelResilienceTest(unittest.TestCase):

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_cancel_succeeds_after_pending_create_resolves(self):
        """exchange_order_id arrives mid-poll; cancel proceeds normally."""
        ex = _make_exchange()
        ex._api_request = AsyncMock(return_value={
            "success": True, "message_cod": "ORDER_CANCELED"})
        order = _make_tracked_order(exchange_order_id=None)

        # Schedule the id to land 300ms in (well under the 1.5s budget).
        async def _flip_id():
            await asyncio.sleep(0.3)
            order.update_exchange_order_id("2055000001")

        async def _drive():
            flip = asyncio.create_task(_flip_id())
            result = await ex._place_cancel("SBCBL_test_cancel", order)
            await flip
            return result

        result = self._run(_drive())
        self.assertTrue(result)
        # The API call must have been made *after* the id resolved — the
        # order id in the request body is what BitPreco needs.
        ex._api_request.assert_called_once()
        sent_body = ex._api_request.call_args.kwargs.get("data") \
            or ex._api_request.call_args[1]["data"]
        self.assertEqual(sent_body["order_id"], "2055000001")

    def test_cancel_returns_false_after_pending_create_timeout(self):
        """If the id never arrives, return False (framework retries)."""
        ex = _make_exchange()
        ex._api_request = AsyncMock(return_value={
            "success": True, "message_cod": "ORDER_CANCELED"})
        order = _make_tracked_order(exchange_order_id=None)

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertFalse(result)
        # API must not have been called — we never had a usable id.
        ex._api_request.assert_not_called()

    def test_cancel_with_existing_id_skips_polling(self):
        """If exchange_order_id is already set, no polling delay."""
        ex = _make_exchange()
        ex._api_request = AsyncMock(return_value={
            "success": True, "message_cod": "ORDER_CANCELED"})
        order = _make_tracked_order(exchange_order_id="2055000002")

        async def _drive():
            return await ex._place_cancel("SBCBL_test_cancel", order)

        result = self._run(_drive())
        self.assertTrue(result)
        ex._api_request.assert_called_once()

    def test_cancel_with_already_canceled_treated_as_success(self):
        """GONE_CODES path: already-cancelled returns True so the framework
        stops retrying."""
        ex = _make_exchange()
        ex._api_request = AsyncMock(return_value={
            "success": False, "message_cod": "ORDER_ALREADY_CANCELED"})
        order = _make_tracked_order(exchange_order_id="2055000003")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)

    def test_cancel_invalid_order_id_retried_then_succeeds(self):
        """INVALID_ORDER_ID is now TRANSIENT, not GONE.

        Production scenario (May 2026): the executor cancels ~400ms after
        order create. BitPreco's lookup-by-id endpoint hasn't yet indexed
        the new order, so it returns INVALID_ORDER_ID even though the order
        is alive on the book. We must retry — BitPreco's index catches up
        within ~1-2s. After that, the second attempt confirms the cancel.

        Regression cost (the bug we're guarding against): treating
        INVALID_ORDER_ID as gone made the local tracker mark the order
        CANCELED while the exchange still had it open. orphan_check then
        had to clean up the leftover, with a 500ms-3s window where the
        order could have filled without local hedge."""
        ex = _make_exchange()
        responses = [
            {"success": False, "message_cod": "INVALID_ORDER_ID"},
            {"success": True, "message_cod": "ORDER_CANCELED"},
        ]
        ex._api_request = AsyncMock(side_effect=responses)
        order = _make_tracked_order(exchange_order_id="2055000004")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)
        # Two API calls: first INVALID_ORDER_ID (transient retry), then
        # ORDER_CANCELED on the second attempt.
        self.assertEqual(ex._api_request.await_count, 2)

    def test_cancel_cant_cancel_filled_order_treated_as_gone(self):
        """``CANT_CANCEL_FILLED_ORDER`` means the order matched right before
        the cancel arrived (Sprint 5 / Bug A). It's semantically identical
        to GONE — return True so the framework doesn't retry, and avoid
        noisy ERROR logs. The fill propagates separately via
        OrderFilledEvent."""
        ex = _make_exchange()
        ex._api_request = AsyncMock(return_value={
            "success": False, "message_cod": "CANT_CANCEL_FILLED_ORDER"})
        order = _make_tracked_order(exchange_order_id="2055000099")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)
        # Single API call — no retry budget burned.
        self.assertEqual(ex._api_request.await_count, 1)

    def test_cancel_invalid_order_id_persistent_returns_false(self):
        """If INVALID_ORDER_ID persists across all 3 attempts, give up and
        return False — the framework will retry later. We must NOT treat
        sustained INVALID_ORDER_ID as success: that's the original orphan
        bug returning. orphan_check is the safety net for the truly-stale
        case (where the order_id never materialises)."""
        ex = _make_exchange()
        ex._api_request = AsyncMock(return_value={
            "success": False, "message_cod": "INVALID_ORDER_ID"})
        order = _make_tracked_order(exchange_order_id="2055000005")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertFalse(result)
        # All three attempts fired — internal retry loop honoured.
        self.assertEqual(ex._api_request.await_count, 3)


class LateFillRecoveryTest(unittest.TestCase):
    """When the cancel response signals the order is gone *because it
    filled* (CANT_CANCEL_FILLED_ORDER, ORDER_FILLED, or ORDER_CANCELED
    with non-zero partial fill field), the connector must fetch and
    emit the trade(s) immediately — otherwise the framework removes
    the order from in_flight_orders and the periodic status poll never
    sees the fill.
    """

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def _make_exchange_with_tracker(self, trade_updates):
        """Exchange with a mocked _order_tracker and a stubbed
        _all_trade_updates_for_order returning the given updates."""
        ex = _make_exchange()
        ex._order_tracker = MagicMock()
        ex._all_trade_updates_for_order = AsyncMock(return_value=trade_updates)
        return ex

    def test_cant_cancel_filled_emits_fill_via_tracker(self):
        """CANT_CANCEL_FILLED_ORDER → fetch trade updates → process via tracker."""
        fake_trade = MagicMock(name="TradeUpdate")
        ex = self._make_exchange_with_tracker([fake_trade])
        ex._api_request = AsyncMock(return_value={
            "success": False, "message_cod": "CANT_CANCEL_FILLED_ORDER"})
        order = _make_tracked_order(exchange_order_id="2055000200")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)
        # Trade update fetched and emitted exactly once
        ex._all_trade_updates_for_order.assert_awaited_once_with(order=order)
        ex._order_tracker.process_trade_update.assert_called_once_with(fake_trade)

    def test_order_filled_emits_fill_via_tracker(self):
        """ORDER_FILLED is treated like CANT_CANCEL_FILLED_ORDER."""
        fake_trade = MagicMock(name="TradeUpdate")
        ex = self._make_exchange_with_tracker([fake_trade])
        ex._api_request = AsyncMock(return_value={
            "success": False, "message_cod": "ORDER_FILLED"})
        order = _make_tracked_order(exchange_order_id="2055000201")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)
        ex._order_tracker.process_trade_update.assert_called_once_with(fake_trade)

    def test_order_canceled_with_partial_fill_emits_trade(self):
        """ORDER_CANCELED + exec_amount > 0 → emit the partial fill."""
        fake_trade = MagicMock(name="TradeUpdate")
        ex = self._make_exchange_with_tracker([fake_trade])
        ex._api_request = AsyncMock(return_value={
            "success": True,
            "message_cod": "ORDER_CANCELED",
            "exec_amount": "0.0001",
        })
        order = _make_tracked_order(exchange_order_id="2055000202")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)
        ex._order_tracker.process_trade_update.assert_called_once_with(fake_trade)

    def test_clean_order_canceled_does_not_emit_trade(self):
        """ORDER_CANCELED with NO partial fill field → no trade fetch."""
        ex = self._make_exchange_with_tracker([])
        ex._api_request = AsyncMock(return_value={
            "success": True, "message_cod": "ORDER_CANCELED"})
        order = _make_tracked_order(exchange_order_id="2055000203")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)
        # Crucially: NO extra REST call on the happy path
        ex._all_trade_updates_for_order.assert_not_called()
        ex._order_tracker.process_trade_update.assert_not_called()

    def test_order_canceled_with_zero_partial_does_not_emit(self):
        """ORDER_CANCELED + exec_amount=0 is treated as clean cancel."""
        ex = self._make_exchange_with_tracker([])
        ex._api_request = AsyncMock(return_value={
            "success": True,
            "message_cod": "ORDER_CANCELED",
            "exec_amount": "0",
        })
        order = _make_tracked_order(exchange_order_id="2055000204")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)
        ex._all_trade_updates_for_order.assert_not_called()

    def test_late_fill_fetch_failure_does_not_break_cancel(self):
        """If _all_trade_updates_for_order raises, log warning and still
        return True. The fill loss will still be caught by orphan_check
        later — better partial recovery than blocking the cancel return."""
        ex = self._make_exchange_with_tracker([])
        ex._api_request = AsyncMock(return_value={
            "success": False, "message_cod": "CANT_CANCEL_FILLED_ORDER"})
        ex._all_trade_updates_for_order = AsyncMock(
            side_effect=ConnectionError("REST down")
        )
        order = _make_tracked_order(exchange_order_id="2055000205")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        # Cancel itself still confirms — framework moves on
        self.assertTrue(result)
        # No trade emitted (the fetch failed)
        ex._order_tracker.process_trade_update.assert_not_called()

    def test_no_tracker_skips_emission_gracefully(self):
        """Test fixture path without _order_tracker — code logs and moves on."""
        ex = _make_exchange()  # no _order_tracker set
        ex._api_request = AsyncMock(return_value={
            "success": False, "message_cod": "CANT_CANCEL_FILLED_ORDER"})
        order = _make_tracked_order(exchange_order_id="2055000206")

        result = self._run(ex._place_cancel("SBCBL_test_cancel", order))
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
