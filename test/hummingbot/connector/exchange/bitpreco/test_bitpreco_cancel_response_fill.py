"""Tests for the synchronous-fill path in ``_place_cancel``.

Since 2026-05-14 BitPreco returns the full order block flat in the
cancel_order response (``exec_amount``, ``cost``, ``fee``, ``status``,
``time_stamp``, ...). When ``exec_amount > 0`` we emit a single
``TradeUpdate`` directly from this payload — no REST round-trip to
``executed_orders`` needed.

These tests exercise ``_try_emit_fill_from_cancel_response`` in
isolation (the helper called from ``_place_cancel``).
"""

import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


def _make_tracked_order(
    side: TradeType = TradeType.SELL,
    amount: str = "0.0002",
    price: str = "400000",
    cid: str = "SBCBL_test_cancel_sync",
) -> InFlightOrder:
    return InFlightOrder(
        client_order_id=cid,
        trading_pair="BTC-BRL",
        order_type=OrderType.LIMIT_MAKER,
        trade_type=side,
        amount=Decimal(amount),
        price=Decimal(price),
        creation_timestamp=1.0,
        exchange_order_id="2062452569",
        initial_state=OrderState.OPEN,
    )


def _make_exchange() -> BitprecoExchange:
    """Bare-minimum stubbed connector — bypasses full __init__ since
    ``_try_emit_fill_from_cancel_response`` only touches:
      - self._order_tracker.process_trade_update
      - self.trade_fee_schema()
      - self.current_timestamp
      - self.logger()
      - self._post_fill_balance_refresh (which spawns a task; mocked)
    """
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    ex._order_tracker = MagicMock()
    ex.trade_fee_schema = MagicMock(return_value=MagicMock(
        maker_percent_fee_decimal=Decimal("0"),
        taker_percent_fee_decimal=Decimal("0"),
        buy_percent_fee_deducted_from_returns=False,
    ))
    # current_timestamp is a property on ExchangePyBase; stub via attribute
    type(ex).current_timestamp = property(lambda self: 1747300000.0)
    # Stub out the post-fill refresh — it spawns asyncio.create_task which
    # would need a running loop. Behaviour of the refresh itself is covered
    # by ``test_bitpreco_post_fill_refresh.py``.
    ex._post_fill_balance_refresh = MagicMock()
    return ex


class TryEmitFillFromCancelResponseTest(unittest.TestCase):

    # ------------------------------------------------------------------
    # Clean cancel (exec_amount=0): authoritative, no emission
    # ------------------------------------------------------------------

    def test_empty_status_zero_exec_returns_true_no_emit(self):
        """Clean cancel: ``status=EMPTY``, ``exec_amount=0`` ⇒ return True,
        no TradeUpdate. Caller skips fallback REST."""
        ex = _make_exchange()
        response = {
            "id": "2062452569", "market": "BTC-BRL", "type": "SELL",
            "status": "EMPTY", "amount": 0.0002, "price": 399105,
            "exec_amount": 0, "cost": 0, "fee": 0, "percent_fee": "0",
            "limited": "1", "programmed": "0", "canceled": 1,
            "time_stamp": "2026-05-14 08:48:43", "tag": None, "obs": None,
            "success": True, "message_cod": "ORDER_CANCELED",
        }
        emitted = ex._try_emit_fill_from_cancel_response(
            tracked_order=_make_tracked_order(),
            response=response,
            exchange_order_id="2062452569",
            code="ORDER_CANCELED",
        )
        self.assertTrue(emitted, "clean cancel should be authoritative (no fallback)")
        ex._order_tracker.process_trade_update.assert_not_called()

    # ------------------------------------------------------------------
    # Partial fill via cancel response: emit TradeUpdate directly
    # ------------------------------------------------------------------

    def test_partial_emits_single_trade_update_with_avg_price(self):
        """exec_amount=0.00005, cost=20.0075 ⇒ avg_price = cost/exec_amount =
        400150.0. One TradeUpdate emitted, no REST call."""
        ex = _make_exchange()
        response = {
            "id": "2062420687", "market": "BTC-BRL", "type": "SELL",
            "status": "PARTIAL", "amount": 0.0002, "price": 400148.99,
            "exec_amount": 0.00005, "cost": 20.0075, "fee": 0.04,
            "percent_fee": "0.2", "limited": "1", "programmed": "0",
            "canceled": 1, "time_stamp": "2026-05-14 11:07:15",
            "tag": None, "obs": None,
            "success": True, "message_cod": "ORDER_CANCELED",
        }
        tracked = _make_tracked_order()
        emitted = ex._try_emit_fill_from_cancel_response(
            tracked_order=tracked,
            response=response,
            exchange_order_id="2062420687",
            code="ORDER_CANCELED",
        )
        self.assertTrue(emitted)
        ex._order_tracker.process_trade_update.assert_called_once()
        trade_update = ex._order_tracker.process_trade_update.call_args[0][0]
        self.assertEqual(trade_update.client_order_id, tracked.client_order_id)
        self.assertEqual(trade_update.trade_id, tracked.client_order_id)
        self.assertEqual(trade_update.exchange_order_id, "2062420687")
        self.assertEqual(trade_update.fill_base_amount, Decimal("0.00005"))
        self.assertEqual(trade_update.fill_quote_amount, Decimal("20.0075"))
        self.assertEqual(trade_update.fill_price, Decimal("400150"))

    def test_partial_with_filled_status_emits_too(self):
        """``status=FILLED`` (full match before cancel) should also emit
        — same code path, just larger exec_amount."""
        ex = _make_exchange()
        response = {
            "id": "2062420687", "market": "BTC-BRL", "type": "BUY",
            "status": "FILLED", "amount": 0.0002, "price": 398348,
            "exec_amount": 0.0002, "cost": 79.6696, "fee": 0.16,
            "percent_fee": "0.2", "limited": "1", "programmed": "0",
            "canceled": 1, "time_stamp": "2026-05-14 08:35:15",
            "success": True, "message_cod": "ORDER_CANCELED",
        }
        emitted = ex._try_emit_fill_from_cancel_response(
            tracked_order=_make_tracked_order(side=TradeType.BUY),
            response=response,
            exchange_order_id="2062420687",
            code="ORDER_CANCELED",
        )
        self.assertTrue(emitted)
        ex._order_tracker.process_trade_update.assert_called_once()
        trade_update = ex._order_tracker.process_trade_update.call_args[0][0]
        self.assertEqual(trade_update.fill_base_amount, Decimal("0.0002"))
        self.assertEqual(trade_update.fill_price, Decimal("398348"))

    # ------------------------------------------------------------------
    # Defensive fallback paths: return False so caller hits REST
    # ------------------------------------------------------------------

    def test_non_dict_response_returns_false(self):
        """Connector got a list/string back (e.g. error envelope variant) —
        return False so caller decides whether to fall back."""
        ex = _make_exchange()
        emitted = ex._try_emit_fill_from_cancel_response(
            tracked_order=_make_tracked_order(),
            response=["unexpected"],
            exchange_order_id="2062452569",
            code="ORDER_CANCELED",
        )
        self.assertFalse(emitted)
        ex._order_tracker.process_trade_update.assert_not_called()

    def test_missing_exec_amount_returns_false(self):
        """Old BitPreco payload shape (no order fields flat) — return False
        so caller can fall back to ``_emit_fills_with_retry`` for codes in
        ``GONE_BY_FILL_CODES``."""
        ex = _make_exchange()
        response = {"success": True, "message_cod": "ORDER_CANCELED"}
        emitted = ex._try_emit_fill_from_cancel_response(
            tracked_order=_make_tracked_order(),
            response=response,
            exchange_order_id="2062452569",
            code="ORDER_CANCELED",
        )
        self.assertFalse(emitted)
        ex._order_tracker.process_trade_update.assert_not_called()

    def test_unparseable_exec_amount_returns_false(self):
        ex = _make_exchange()
        response = {
            "exec_amount": "not_a_number",
            "success": True, "message_cod": "ORDER_CANCELED",
        }
        emitted = ex._try_emit_fill_from_cancel_response(
            tracked_order=_make_tracked_order(),
            response=response,
            exchange_order_id="2062452569",
            code="ORDER_CANCELED",
        )
        self.assertFalse(emitted)
        ex._order_tracker.process_trade_update.assert_not_called()

    def test_partial_without_cost_returns_false_for_fallback(self):
        """``exec_amount > 0`` but ``cost`` missing/zero ⇒ cannot compute
        avg_price. Return False so caller falls back to REST."""
        ex = _make_exchange()
        response = {
            "exec_amount": 0.00005, "cost": 0, "fee": 0,
            "status": "PARTIAL",
            "success": True, "message_cod": "ORDER_CANCELED",
        }
        emitted = ex._try_emit_fill_from_cancel_response(
            tracked_order=_make_tracked_order(),
            response=response,
            exchange_order_id="2062452569",
            code="ORDER_CANCELED",
        )
        self.assertFalse(emitted)
        ex._order_tracker.process_trade_update.assert_not_called()

    # ------------------------------------------------------------------
    # CANT_CANCEL_FILLED_ORDER path: emit directly if payload carries data
    # ------------------------------------------------------------------

    def test_cant_cancel_filled_with_payload_emits(self):
        """Order fully matched before cancel landed; BitPreco includes the
        order block in the response → emit directly (no fallback REST)."""
        ex = _make_exchange()
        response = {
            "id": "2062420687", "status": "FILLED",
            "amount": 0.0002, "exec_amount": 0.0002, "cost": 79.66,
            "fee": 0.16, "time_stamp": "2026-05-14 11:07:15",
            "success": False, "message_cod": "CANT_CANCEL_FILLED_ORDER",
        }
        emitted = ex._try_emit_fill_from_cancel_response(
            tracked_order=_make_tracked_order(),
            response=response,
            exchange_order_id="2062420687",
            code="CANT_CANCEL_FILLED_ORDER",
        )
        self.assertTrue(emitted)
        ex._order_tracker.process_trade_update.assert_called_once()


if __name__ == "__main__":
    unittest.main()
