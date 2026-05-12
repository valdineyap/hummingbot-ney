"""Pins the fast-path host resolution in ``bitpreco_constants``.

Two independent env vars override the BitPreco host:

  * ``BITPRECO_INTERNAL_BOOKS`` — public endpoints (orderbook, tickers).
  * ``BITPRECO_INTERNAL_API``   — private REST trading.

Legacy escape hatches ``BITPRECO_ORDER_BOOK_URL`` and ``BITPRECO_TRADING_URL``
take precedence over their respective higher-level overrides — kept for ops
debugging and to preserve the prior contract.

The constants are computed at module import time, so each test reloads the
module under a controlled ``os.environ`` snapshot.
"""
import importlib
import os
import unittest
from unittest.mock import patch


_OVERRIDE_VARS = (
    "BITPRECO_INTERNAL_BOOKS",
    "BITPRECO_INTERNAL_API",
    "BITPRECO_ORDER_BOOK_URL",
    "BITPRECO_TRADING_URL",
)

_PUBLIC = "https://api.bitpreco.com"


class TestBitprecoInternalHosts(unittest.TestCase):

    def _reload_with_env(self, env: dict):
        """Reload bitpreco_constants under an isolated env, wiping all
        relevant overrides and re-applying only what the test passed."""
        clean_env = {k: v for k, v in os.environ.items() if k not in _OVERRIDE_VARS}
        clean_env.update(env)
        with patch.dict(os.environ, clean_env, clear=True):
            from hummingbot.connector.exchange.bitpreco import bitpreco_constants
            return importlib.reload(bitpreco_constants)

    def tearDown(self):
        # Restore module to default env so other test files don't observe
        # a stale override left in place by a previous test.
        self._reload_with_env({})

    # ------------------------------------------------------------------
    # Default
    # ------------------------------------------------------------------

    def test_defaults_use_public_host(self):
        c = self._reload_with_env({})
        self.assertEqual(c.ORDER_BOOK_PATH_URL, f"{_PUBLIC}/{{}}/orderbook")
        self.assertEqual(c.PING_PATH_URL, f"{_PUBLIC}/btc-brl/ticker")
        self.assertEqual(c.ALL_CURRENCY_TICKER_PATH_URL, f"{_PUBLIC}/all-brl/ticker")
        self.assertEqual(c.REST_URL, f"{_PUBLIC}/trading")

    # ------------------------------------------------------------------
    # Phase 1: BITPRECO_INTERNAL_BOOKS
    # ------------------------------------------------------------------

    def test_internal_books_overrides_all_public_endpoints(self):
        c = self._reload_with_env({"BITPRECO_INTERNAL_BOOKS": "http://54.232.138.12"})
        self.assertEqual(c.ORDER_BOOK_PATH_URL, "http://54.232.138.12/{}/orderbook")
        self.assertEqual(c.PING_PATH_URL, "http://54.232.138.12/btc-brl/ticker")
        self.assertEqual(c.ALL_CURRENCY_TICKER_PATH_URL,
                         "http://54.232.138.12/all-brl/ticker")

    def test_internal_books_does_not_affect_trading(self):
        c = self._reload_with_env({"BITPRECO_INTERNAL_BOOKS": "http://54.232.138.12"})
        self.assertEqual(c.REST_URL, f"{_PUBLIC}/trading")

    def test_internal_books_strips_trailing_slash(self):
        c = self._reload_with_env({"BITPRECO_INTERNAL_BOOKS": "http://54.232.138.12/"})
        self.assertEqual(c.ORDER_BOOK_PATH_URL, "http://54.232.138.12/{}/orderbook")
        self.assertEqual(c.PING_PATH_URL, "http://54.232.138.12/btc-brl/ticker")

    # ------------------------------------------------------------------
    # Phase 2: BITPRECO_INTERNAL_API
    # ------------------------------------------------------------------

    def test_internal_api_overrides_trading(self):
        c = self._reload_with_env({"BITPRECO_INTERNAL_API": "http://10.0.0.5"})
        self.assertEqual(c.REST_URL, "http://10.0.0.5/trading")

    def test_internal_api_does_not_affect_public(self):
        c = self._reload_with_env({"BITPRECO_INTERNAL_API": "http://10.0.0.5"})
        self.assertEqual(c.ORDER_BOOK_PATH_URL, f"{_PUBLIC}/{{}}/orderbook")
        self.assertEqual(c.PING_PATH_URL, f"{_PUBLIC}/btc-brl/ticker")

    def test_both_phases_independently(self):
        c = self._reload_with_env({
            "BITPRECO_INTERNAL_BOOKS": "http://54.232.138.12",
            "BITPRECO_INTERNAL_API": "http://10.0.0.5",
        })
        self.assertEqual(c.ORDER_BOOK_PATH_URL, "http://54.232.138.12/{}/orderbook")
        self.assertEqual(c.PING_PATH_URL, "http://54.232.138.12/btc-brl/ticker")
        self.assertEqual(c.ALL_CURRENCY_TICKER_PATH_URL,
                         "http://54.232.138.12/all-brl/ticker")
        self.assertEqual(c.REST_URL, "http://10.0.0.5/trading")

    # ------------------------------------------------------------------
    # Legacy escape hatches keep precedence (ops-level override)
    # ------------------------------------------------------------------

    def test_legacy_order_book_url_wins_over_internal_books(self):
        c = self._reload_with_env({
            "BITPRECO_INTERNAL_BOOKS": "http://54.232.138.12",
            "BITPRECO_ORDER_BOOK_URL": "http://debug.local",
        })
        # legacy wins for /orderbook
        self.assertEqual(c.ORDER_BOOK_PATH_URL, "http://debug.local/{}/orderbook")
        # but tickers still follow INTERNAL_BOOKS (legacy never covered them)
        self.assertEqual(c.PING_PATH_URL, "http://54.232.138.12/btc-brl/ticker")
        self.assertEqual(c.ALL_CURRENCY_TICKER_PATH_URL,
                         "http://54.232.138.12/all-brl/ticker")

    def test_legacy_trading_url_wins_over_internal_api(self):
        c = self._reload_with_env({
            "BITPRECO_INTERNAL_API": "http://10.0.0.5",
            "BITPRECO_TRADING_URL": "http://debug.local/trading-v2",
        })
        self.assertEqual(c.REST_URL, "http://debug.local/trading-v2")


if __name__ == "__main__":
    unittest.main()
