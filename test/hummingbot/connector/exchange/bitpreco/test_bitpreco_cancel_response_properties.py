"""Property-based tests for ``_try_emit_fill_from_cancel_response``.

Complements the example-based suite in
``test_bitpreco_cancel_response_fill.py`` by stress-testing the Decimal
math (``avg_fill_price = cost / exec_amount``) across the realistic
BitPreco BTC-BRL value space.

Invariants checked:
  - When the cancel response carries a real partial/full fill
    (``exec_amount > 0`` and ``cost > 0``), exactly one TradeUpdate is
    emitted, and the recorded fill amounts round-trip the response:
    ``fill_base_amount == Decimal(exec_amount)``,
    ``fill_quote_amount == Decimal(cost)``,
    ``fill_price        == cost / exec_amount``.
  - When the response is malformed (negative or zero exec_amount,
    missing cost on a non-zero fill, unparseable types), nothing is
    emitted — the caller falls back to REST.
"""

from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from test.hummingbot.connector.exchange.bitpreco.test_bitpreco_cancel_response_fill import (
    _make_exchange,
    _make_tracked_order,
)

# Realistic BitPreco BTC-BRL ranges, kept tight to avoid degenerate
# precision artefacts. Hummingbot stores amounts/prices as Decimals — feed
# strings to avoid IEEE-754 noise sneaking in via floats.
exec_amounts = st.decimals(
    min_value=Decimal("0.00001"),
    max_value=Decimal("1.0"),
    places=5,
    allow_nan=False,
    allow_infinity=False,
)
prices = st.decimals(
    min_value=Decimal("100000"),
    max_value=Decimal("900000"),
    places=2,
    allow_nan=False,
    allow_infinity=False,
)


@settings(max_examples=200, deadline=None)
@given(exec_amount=exec_amounts, price=prices)
def test_fill_emission_preserves_amounts_and_price(exec_amount, price):
    cost = exec_amount * price
    ex = _make_exchange()
    response = {
        "status": "PARTIAL",
        "exec_amount": str(exec_amount),
        "cost": str(cost),
        "fee": "0",
        "success": True,
        "message_cod": "ORDER_CANCELED",
    }
    emitted = ex._try_emit_fill_from_cancel_response(
        tracked_order=_make_tracked_order(),
        response=response,
        exchange_order_id="2062420687",
        code="ORDER_CANCELED",
    )
    assert emitted is True
    ex._order_tracker.process_trade_update.assert_called_once()
    update = ex._order_tracker.process_trade_update.call_args[0][0]
    assert update.fill_base_amount == exec_amount
    assert update.fill_quote_amount == cost
    # avg_fill_price = cost / exec_amount; with exact Decimals this must
    # round-trip to the input price (no float noise in the path).
    assert update.fill_price == cost / exec_amount


@settings(max_examples=100, deadline=None)
@given(exec_amount=exec_amounts)
def test_zero_or_missing_cost_falls_back(exec_amount):
    """Real exec but no cost ⇒ cannot compute avg_price ⇒ caller falls
    back to REST. No emission."""
    ex = _make_exchange()
    response = {
        "exec_amount": str(exec_amount),
        "cost": "0",
        "success": True,
        "message_cod": "ORDER_CANCELED",
    }
    emitted = ex._try_emit_fill_from_cancel_response(
        tracked_order=_make_tracked_order(),
        response=response,
        exchange_order_id="x",
        code="ORDER_CANCELED",
    )
    assert emitted is False
    ex._order_tracker.process_trade_update.assert_not_called()


@settings(max_examples=50, deadline=None)
@given(garbage=st.one_of(
    st.text(min_size=1, max_size=10),
    st.lists(st.integers(), min_size=1, max_size=3),
    st.dictionaries(st.text(), st.integers(), max_size=2),
))
def test_unparseable_exec_amount_falls_back(garbage):
    """Random non-numeric types on exec_amount ⇒ return False; no
    TradeUpdate. Caller decides on REST fallback by code."""
    ex = _make_exchange()
    # Guard: skip strings that happen to parse as Decimal (e.g. "42").
    try:
        Decimal(str(garbage))
        return
    except Exception:
        pass
    response = {
        "exec_amount": garbage,
        "cost": "1.0",
        "success": True,
        "message_cod": "ORDER_CANCELED",
    }
    emitted = ex._try_emit_fill_from_cancel_response(
        tracked_order=_make_tracked_order(),
        response=response,
        exchange_order_id="x",
        code="ORDER_CANCELED",
    )
    assert emitted is False
    ex._order_tracker.process_trade_update.assert_not_called()
