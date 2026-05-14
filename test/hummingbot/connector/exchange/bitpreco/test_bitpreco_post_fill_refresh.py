"""Tests for ``_post_fill_balance_refresh`` debouncing semantics.

After a fill is emitted, the local available-balance cache becomes
stale (the fill consumed some asset that the cache still credits).
The framework's periodic poll fixes this within 5s, but during that
window any new placement attempt sees an optimistic stale balance —
the 2026-05-14 12:03Z scenario where 8 NOT_ENOUGH_USER_BALANCE
rejections fired in 8s.

To close that window the connector now triggers an async REST refresh
on every fill emission, with two-flag debouncing:
  - At most ONE refresh in flight at a time.
  - At most ONE follow-up refresh queued (covers fills arriving during
    the in-flight window that the server-side read may have missed).
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


def _make_exchange() -> BitprecoExchange:
    ex = BitprecoExchange.__new__(BitprecoExchange)
    ex.logger = lambda: MagicMock()
    ex._update_all_balances = AsyncMock()
    return ex


class PostFillBalanceRefreshTest(unittest.TestCase):

    def _drain(self):
        """Drain all pending tasks on the running loop."""
        loop = asyncio.get_event_loop()
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    # ------------------------------------------------------------------
    # Single fill → exactly one REST refresh
    # ------------------------------------------------------------------

    def test_single_fill_triggers_one_refresh(self):
        ex = _make_exchange()

        async def _drive():
            ex._post_fill_balance_refresh()
        self._run(_drive())
        self._drain()
        ex._update_all_balances.assert_awaited_once()
        kwargs = ex._update_all_balances.call_args.kwargs
        self.assertEqual(kwargs.get("_trigger"), "post_fill")

    # ------------------------------------------------------------------
    # Burst of fills during in-flight → trailing refresh ONCE
    # ------------------------------------------------------------------

    def test_burst_of_fills_coalesces_into_two_refreshes(self):
        """Fills arriving while a refresh is in flight queue exactly ONE
        trailing refresh (regardless of how many fills landed). This
        guarantees the post-burst cache is current without thrashing
        REST with N concurrent calls."""
        ex = _make_exchange()

        # Make _update_all_balances slow so subsequent fills land during
        # the in-flight window.
        refresh_started = asyncio.Event()
        refresh_can_finish = asyncio.Event()

        async def _slow_refresh(_trigger=None):
            refresh_started.set()
            await refresh_can_finish.wait()

        ex._update_all_balances = AsyncMock(side_effect=_slow_refresh)

        async def _drive():
            ex._post_fill_balance_refresh()    # fill 1 → starts refresh
            await refresh_started.wait()
            # Fills 2..5 arrive during the in-flight window
            ex._post_fill_balance_refresh()
            ex._post_fill_balance_refresh()
            ex._post_fill_balance_refresh()
            ex._post_fill_balance_refresh()
            # Release the first refresh; it should detect the queued
            # flag and schedule exactly ONE trailing refresh.
            refresh_can_finish.set()

        self._run(_drive())
        self._drain()
        # 1 initial + 1 trailing = 2. NOT 5 (one per fill).
        self.assertEqual(ex._update_all_balances.await_count, 2)

    # ------------------------------------------------------------------
    # Fill during in-flight, then no more → trailing refresh fires once
    # ------------------------------------------------------------------

    def test_fill_during_inflight_triggers_one_trailing_refresh(self):
        ex = _make_exchange()
        refresh_started = asyncio.Event()
        refresh_can_finish = asyncio.Event()

        async def _slow_refresh(_trigger=None):
            refresh_started.set()
            await refresh_can_finish.wait()

        ex._update_all_balances = AsyncMock(side_effect=_slow_refresh)

        async def _drive():
            ex._post_fill_balance_refresh()    # fill 1 starts refresh
            await refresh_started.wait()
            ex._post_fill_balance_refresh()    # fill 2 during in-flight
            refresh_can_finish.set()

        self._run(_drive())
        self._drain()
        self.assertEqual(ex._update_all_balances.await_count, 2)

    # ------------------------------------------------------------------
    # Sequential fills (no overlap) → one refresh each
    # ------------------------------------------------------------------

    def test_sequential_fills_each_trigger_refresh(self):
        """If fills are spaced out (refresh completes before next fill),
        each fill triggers its own refresh — no skipping."""
        ex = _make_exchange()

        async def _drive():
            ex._post_fill_balance_refresh()
            # Drain in the middle: simulate refresh completing fully.
            await asyncio.sleep(0)
            pending = [t for t in asyncio.all_tasks() if not t.done() and t is not asyncio.current_task()]
            if pending:
                await asyncio.gather(*pending)

            ex._post_fill_balance_refresh()
            await asyncio.sleep(0)
            pending = [t for t in asyncio.all_tasks() if not t.done() and t is not asyncio.current_task()]
            if pending:
                await asyncio.gather(*pending)

        self._run(_drive())
        self._drain()
        self.assertEqual(ex._update_all_balances.await_count, 2)

    # ------------------------------------------------------------------
    # Refresh failures don't break the system
    # ------------------------------------------------------------------

    def test_refresh_failure_clears_inflight_flag(self):
        """If the REST refresh raises, the in-flight flag must be cleared
        so the NEXT fill can trigger a fresh refresh (otherwise we'd
        deadlock the refresh path)."""
        ex = _make_exchange()
        ex._update_all_balances = AsyncMock(side_effect=ConnectionError("network down"))

        async def _drive():
            ex._post_fill_balance_refresh()
        self._run(_drive())
        self._drain()
        # First refresh raised, but inflight flag should be cleared.
        self.assertFalse(getattr(ex, "_balance_refresh_inflight", False))

        # Next fill should re-trigger.
        async def _drive2():
            ex._post_fill_balance_refresh()
        self._run(_drive2())
        self._drain()
        self.assertEqual(ex._update_all_balances.await_count, 2)


if __name__ == "__main__":
    unittest.main()
