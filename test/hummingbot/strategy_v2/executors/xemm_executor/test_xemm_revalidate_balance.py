"""Tests for per-placement balance revalidation in
``XEMMLeadLagExecutor.create_maker_order``.

The framework's ``validate_sufficient_balance`` only fires on
``on_start``. Once the executor is running, balance drops (e.g. after
a fill detected with delay) are invisible until placement is rejected
by the exchange. That produced the 8-rejection retry loop seen at
2026-05-14 12:03Z.

These tests verify that ``create_maker_order`` revalidates BEFORE
placing, and short-circuits cleanly when ``validate_sufficient_balance``
flips the executor out of RUNNING state.
"""

import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.strategy_v2.executors.xemm_executor.xemm_lead_lag_executor import (
    XEMMLeadLagExecutor,
)
from hummingbot.strategy_v2.models.base import RunnableStatus


def _make_executor() -> XEMMLeadLagExecutor:
    ex = XEMMLeadLagExecutor.__new__(XEMMLeadLagExecutor)
    ex._status = RunnableStatus.RUNNING
    ex.logger = lambda: MagicMock()
    # Stub the parent control flow — _create_maker_order_inner is whatever
    # comes after our revalidation gate. We just want to verify it's
    # called when validation passes and SKIPPED when it doesn't.
    return ex


class CreateMakerOrderRevalidationTest(unittest.IsolatedAsyncioTestCase):

    async def test_validates_before_placing(self):
        """validate_sufficient_balance() runs at the top of every
        create_maker_order invocation."""
        ex = _make_executor()
        ex.validate_sufficient_balance = AsyncMock()

        # Patch out everything below the gate so the test focuses on it.
        # We stop at the first thing the gate doesn't reach.
        with patch.object(ex, "_get_live_lead_bps", return_value=Decimal("0")):
            # The function will raise once it hits the real placement logic,
            # because we haven't stubbed get_trading_rules. That's fine —
            # we only care that validate ran first.
            try:
                await ex.create_maker_order()
            except (AttributeError, TypeError):
                pass
        ex.validate_sufficient_balance.assert_awaited_once()

    async def test_skips_placement_when_status_shutting_down(self):
        """If validate_sufficient_balance flips status to SHUTTING_DOWN
        (e.g. via stop() on INSUFFICIENT_BALANCE), create_maker_order
        exits cleanly WITHOUT touching the placement logic."""
        ex = _make_executor()

        async def _validate_and_stop():
            # Simulate the base behaviour: stop() transitions status.
            ex._status = RunnableStatus.SHUTTING_DOWN

        ex.validate_sufficient_balance = AsyncMock(side_effect=_validate_and_stop)
        ex._get_live_lead_bps = MagicMock(return_value=Decimal("0"))

        # If the gate works, create_maker_order returns before touching
        # any of these — they would have raised if called.
        ex._get_live_lead_bps = MagicMock(side_effect=AssertionError(
            "placement logic should not run when status is SHUTTING_DOWN"
        ))

        await ex.create_maker_order()   # should NOT raise
        ex.validate_sufficient_balance.assert_awaited_once()

    async def test_proceeds_past_gate_when_validate_passes(self):
        """When validate keeps status=RUNNING, create_maker_order proceeds
        past the gate. We don't try to reach all the way to placement
        (too many dependencies); we just verify the gate doesn't
        short-circuit by checking that ``_get_live_lead_bps`` runs
        (the first line AFTER the gate)."""
        ex = _make_executor()
        ex.validate_sufficient_balance = AsyncMock()   # no-op, keeps RUNNING

        post_gate_called = {"hit": False}

        def _post_gate_marker():
            post_gate_called["hit"] = True
            # Raise to short-circuit downstream attribute access we
            # haven't stubbed.
            raise RuntimeError("intentional stop — gate passed")

        ex._get_live_lead_bps = MagicMock(side_effect=_post_gate_marker)

        try:
            await ex.create_maker_order()
        except RuntimeError:
            pass
        self.assertTrue(
            post_gate_called["hit"],
            "create_maker_order should call _get_live_lead_bps (first line "
            "after the revalidation gate) when status is RUNNING",
        )


if __name__ == "__main__":
    unittest.main()
