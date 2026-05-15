"""Focused unit test for the PARTIAL+canceled state-coercion fix.

Reproduces the production scenario where BitPreco returns
``status: "PARTIAL"`` together with ``canceled: "1"`` after a partial fill,
which previously left the tracker stuck in OPEN (preserving the unknown
state) instead of progressing to a terminal CANCELED state.

This test stubs the network call and exercises only the state-mapping
logic so it doesn't need a running BitPreco endpoint.
"""

import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.common import OrderType, TradeType


def _make_tracked_order(state: OrderState = OrderState.OPEN) -> InFlightOrder:
    return InFlightOrder(
        client_order_id="SBCBL_test_partial_cancel",
        trading_pair="BTC-BRL",
        order_type=OrderType.LIMIT_MAKER,
        trade_type=TradeType.SELL,
        amount=Decimal("0.00019999"),
        price=Decimal("393401.99"),
        creation_timestamp=1.0,
        exchange_order_id="2055034667",
        initial_state=state,
    )


def _make_exchange() -> BitprecoExchange:
    """Construct a connector with the bare minimum stubbed dependencies.

    We bypass the full ``__init__`` because we only exercise
    ``_request_order_status``; touching real config / event loops would
    require integration setup we don't need here.
    """
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    ex._add_auth_token_to_req_body = lambda body: {**body, "auth_token": "stub"}
    return ex


class PartialCancelStateCoercionTest(unittest.TestCase):

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def _stub_response(self, exchange: BitprecoExchange, body: dict) -> None:
        exchange._api_request = AsyncMock(return_value=body)

    def test_partial_status_with_canceled_flag_becomes_terminal_canceled(self):
        """The exact production payload from logs at 16:58:38 — order
        partially filled (3.812e-05 BTC) and then cancelled. Before the fix
        this returned ``preserve OPEN`` and the order was never reconciled
        to a terminal state until inventory_audit fired minutes later."""
        ex = _make_exchange()
        self._stub_response(ex, {
            "order": {
                "id": "2055034667",
                "market": "BTC-BRL",
                "type": "SELL",
                "status": "PARTIAL",
                "amount": 0.00019999,
                "price": 393401.993704,
                "exec_amount": 3.812e-05,
                "cost": 14.996484,
                "fee": 0,
                "limited": "1",
                "canceled": "1",
                "tag": None,
            }
        })
        update = self._run(ex._request_order_status(_make_tracked_order()))
        self.assertEqual(update.new_state, OrderState.CANCELED)

    def test_partial_status_without_canceled_flag_stays_partially_filled(self):
        """Same partial fill, but the order is still resting on the book.
        We want PARTIALLY_FILLED so the tracker can keep matching subsequent
        TradeUpdate events to the same in-flight order."""
        ex = _make_exchange()
        self._stub_response(ex, {
            "order": {
                "id": "2055034667", "market": "BTC-BRL", "type": "SELL",
                "status": "PARTIAL", "amount": 0.00019999, "price": 393401.99,
                "exec_amount": 3.812e-05, "cost": 14.99, "fee": 0,
                "limited": "1", "canceled": "0",
            }
        })
        update = self._run(ex._request_order_status(_make_tracked_order()))
        self.assertEqual(update.new_state, OrderState.PARTIALLY_FILLED)

    def test_open_with_canceled_flag_also_coerced(self):
        """Defensive: if the exchange ever returns OPEN+canceled=1, treat as
        CANCELED (terminal) rather than leaving it on the book."""
        ex = _make_exchange()
        self._stub_response(ex, {
            "order": {
                "id": "2055034667", "market": "BTC-BRL", "type": "SELL",
                "status": "OPEN", "amount": 0.00019999, "price": 393401.99,
                "exec_amount": 0, "cost": 0, "fee": 0,
                "limited": "1", "canceled": "1",
            }
        })
        update = self._run(ex._request_order_status(_make_tracked_order()))
        self.assertEqual(update.new_state, OrderState.CANCELED)

    def test_filled_status_not_overridden_even_with_canceled_flag(self):
        """A fully filled order should reach FILLED regardless of stale
        ``canceled`` metadata. The override only activates for non-terminal
        states (OPEN/PARTIAL/PENDING_CANCEL)."""
        ex = _make_exchange()
        self._stub_response(ex, {
            "order": {
                "id": "2055034667", "market": "BTC-BRL", "type": "SELL",
                "status": "FILLED", "amount": 0.00019999, "price": 393401.99,
                "exec_amount": 0.00019999, "cost": 78.66, "fee": 0,
                "limited": "1", "canceled": "1",
            }
        })
        update = self._run(ex._request_order_status(_make_tracked_order()))
        self.assertEqual(update.new_state, OrderState.FILLED)

    def test_unknown_status_still_preserves_tracker_state(self):
        """Existing fallback behaviour must remain — unknown status without
        canceled flag preserves the prior tracker state and warns."""
        ex = _make_exchange()
        self._stub_response(ex, {
            "order": {"id": "2055034667", "status": "MYSTERY_STATE",
                      "canceled": "0"}
        })
        tracked = _make_tracked_order(state=OrderState.OPEN)
        update = self._run(ex._request_order_status(tracked))
        self.assertEqual(update.new_state, OrderState.OPEN)


if __name__ == "__main__":
    unittest.main()
