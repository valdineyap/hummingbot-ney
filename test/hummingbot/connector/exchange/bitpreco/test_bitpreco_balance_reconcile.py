"""Tests for balance-cache reconciliation on NOT_ENOUGH_USER_BALANCE.

When BitPreco rejects an order for insufficient balance, the response
carries authoritative balance data: ``{currency: "BTC", max: 6.056e-05}``.
We use this to:

  (a) Update the local available-balance cache IMMEDIATELY (sync, zero
      round-trip), so subsequent ``validate_sufficient_balance`` checks
      see real values without waiting for the 5s periodic poll.
  (b) Schedule a full ``_update_all_balances`` REST refresh in background
      to cover any other stale assets the ``max`` field doesn't reach.

Without (a), we observed 8 NOT_ENOUGH_USER_BALANCE rejections in 8 seconds
on 2026-05-14 12:03Z — the executor kept retrying because cache was stale
for 11.4s after a fill that the periodic poll hadn't yet picked up.

These tests exercise ``_reconcile_balance_on_rejection`` in isolation
(the helper called from ``_place_order``'s rejection branch).
"""

import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


def _make_exchange() -> BitprecoExchange:
    """Bare-minimum stubbed connector — bypasses full __init__ since
    ``_reconcile_balance_on_rejection`` only touches:
      - self._account_available_balances (dict)
      - self._account_balances (dict)
      - self._update_all_balances (mocked)
      - self.logger()
    """
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    ex._account_available_balances = {"BTC": Decimal("0.0002"), "BRL": Decimal("100")}
    ex._account_balances = {"BTC": Decimal("0.0002"), "BRL": Decimal("100")}
    ex._update_all_balances = AsyncMock()
    return ex


class BalanceReconcileOnRejectionTest(unittest.TestCase):

    def _wait_pending_tasks(self):
        """Drain pending tasks (the background _update_all_balances)."""
        loop = asyncio.get_event_loop()
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

    def _run_reconcile(self, ex: BitprecoExchange, response: dict):
        """Helper needs to be invoked inside an event loop (it calls
        asyncio.create_task). Wrap in a tiny coroutine."""
        msg = response.get("message_cod")

        async def _drive():
            ex._reconcile_balance_on_rejection(msg, response)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(_drive())

    # ------------------------------------------------------------------
    # Cache write from rejection payload — happy path
    # ------------------------------------------------------------------

    def test_not_enough_balance_updates_available_cache_immediately(self):
        """The ``max`` value from the rejection becomes the new available
        balance for the rejected currency — no REST needed."""
        ex = _make_exchange()
        self._run_reconcile(ex, {
            "success": False,
            "message_cod": "NOT_ENOUGH_USER_BALANCE",
            "currency": "BTC",
            "requested": 0.0002,
            "max": 6.056e-05,
        })
        self.assertEqual(
            ex._account_available_balances["BTC"],
            Decimal(str(6.056e-05)),
        )
        # Other assets untouched.
        self.assertEqual(ex._account_available_balances["BRL"], Decimal("100"))
        self._wait_pending_tasks()

    def test_total_balance_clamped_up_if_below_max(self):
        """Total >= available. If cached total claimed less than the
        exchange-reported max, bump total up to match. Otherwise leave
        total alone (it may legitimately exceed available due to locked
        amounts in other orders)."""
        ex = _make_exchange()
        ex._account_balances["BTC"] = Decimal("0.00001")  # claims less than max
        self._run_reconcile(ex, {
            "success": False, "message_cod": "NOT_ENOUGH_USER_BALANCE",
            "currency": "BTC", "max": 6.056e-05,
        })
        # Total bumped up to match max (consistency).
        self.assertEqual(
            ex._account_balances["BTC"],
            Decimal(str(6.056e-05)),
        )
        self._wait_pending_tasks()

    def test_total_balance_not_clobbered_when_already_higher(self):
        """If our cached total is HIGHER than the reported max (locked
        funds in other orders), don't clobber it."""
        ex = _make_exchange()
        ex._account_balances["BTC"] = Decimal("0.001")  # total includes locked
        self._run_reconcile(ex, {
            "success": False, "message_cod": "NOT_ENOUGH_USER_BALANCE",
            "currency": "BTC", "max": 6.056e-05,
        })
        # Total preserved — locked funds may justify the gap.
        self.assertEqual(ex._account_balances["BTC"], Decimal("0.001"))
        # But available reflects what's spendable now.
        self.assertEqual(
            ex._account_available_balances["BTC"],
            Decimal(str(6.056e-05)),
        )
        self._wait_pending_tasks()

    # ------------------------------------------------------------------
    # Background REST refresh scheduled
    # ------------------------------------------------------------------

    def test_background_full_refresh_scheduled(self):
        """A NOT_ENOUGH_USER_BALANCE rejection schedules a full REST
        refresh in background (best-effort, non-blocking)."""
        ex = _make_exchange()
        self._run_reconcile(ex, {
            "success": False, "message_cod": "NOT_ENOUGH_USER_BALANCE",
            "currency": "BTC", "max": 6.056e-05,
        })
        self._wait_pending_tasks()
        ex._update_all_balances.assert_awaited_once()
        kwargs = ex._update_all_balances.call_args.kwargs
        self.assertEqual(kwargs.get("_trigger"), "balance_rejection")

    # ------------------------------------------------------------------
    # Robustness: unparseable / missing fields
    # ------------------------------------------------------------------

    def test_missing_max_field_falls_back_to_refresh_only(self):
        """If the rejection lacks the ``max`` field (defensive), cache
        write is skipped but background REST refresh still scheduled."""
        ex = _make_exchange()
        original_btc = ex._account_available_balances["BTC"]
        self._run_reconcile(ex, {
            "success": False, "message_cod": "NOT_ENOUGH_USER_BALANCE",
            "currency": "BTC",
            # no `max`
        })
        # No sync update happened.
        self.assertEqual(ex._account_available_balances["BTC"], original_btc)
        self._wait_pending_tasks()
        # But background refresh still scheduled.
        ex._update_all_balances.assert_awaited_once()

    def test_unparseable_max_falls_back_to_refresh_only(self):
        ex = _make_exchange()
        original_btc = ex._account_available_balances["BTC"]
        self._run_reconcile(ex, {
            "success": False, "message_cod": "NOT_ENOUGH_USER_BALANCE",
            "currency": "BTC", "max": "garbage_value",
        })
        self.assertEqual(ex._account_available_balances["BTC"], original_btc)
        self._wait_pending_tasks()
        ex._update_all_balances.assert_awaited_once()

    # ------------------------------------------------------------------
    # Other rejection codes don't trigger this path
    # ------------------------------------------------------------------

    def test_other_rejection_codes_do_not_touch_balance(self):
        """Only NOT_ENOUGH_USER_BALANCE triggers the reconcile path.
        Other rejections (RATE_LIMIT_EXCEEDED, MARKET_CLOSED, etc.)
        should leave the cache alone and NOT spawn a refresh."""
        ex = _make_exchange()
        original = dict(ex._account_available_balances)
        self._run_reconcile(ex, {
            "success": False, "message_cod": "RATE_LIMIT_EXCEEDED",
        })
        self.assertEqual(dict(ex._account_available_balances), original)
        self._wait_pending_tasks()
        ex._update_all_balances.assert_not_called()

    def test_currency_not_yet_in_cache_creates_entry(self):
        """If the rejected currency hasn't been seen yet (e.g. an asset
        the connector hasn't polled balance for), the helper should
        create the entry rather than silently skip."""
        ex = _make_exchange()
        del ex._account_available_balances["BTC"]  # not in cache
        del ex._account_balances["BTC"]
        self._run_reconcile(ex, {
            "success": False, "message_cod": "NOT_ENOUGH_USER_BALANCE",
            "currency": "BTC", "max": 0.0001,
        })
        self.assertEqual(ex._account_available_balances["BTC"], Decimal("0.0001"))
        self.assertEqual(ex._account_balances["BTC"], Decimal("0.0001"))
        self._wait_pending_tasks()


if __name__ == "__main__":
    unittest.main()
