"""Tests for the ``BinanceSbeExchange`` class.

We pin only the SBE-specific contract — name, the override of the order
book data source factory, the trading-required validation, and a smoke
check that inherited methods still resolve to the base class. Full
trading-path coverage is the JSON connector's responsibility and not
re-tested here (those tests would just exercise the parent class).
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from hummingbot.connector.exchange.binance.binance_api_order_book_data_source import (
    BinanceAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange
from hummingbot.connector.exchange.binance_sbe.binance_sbe_api_order_book_data_source import (
    BinanceSbeAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.binance_sbe.binance_sbe_exchange import (
    BinanceSbeExchange,
)


class BinanceSbeExchangeNameTest(unittest.TestCase):

    def test_name_property_returns_binance_sbe_for_com(self):
        ex = BinanceSbeExchange(
            binance_sbe_api_key="K",
            binance_api_key="",
            binance_api_secret="",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
            domain="com",
        )
        self.assertEqual(ex.name, "binance_sbe")

    def test_name_property_includes_domain_for_non_com(self):
        ex = BinanceSbeExchange(
            binance_sbe_api_key="K",
            binance_api_key="",
            binance_api_secret="",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
            domain="us",
        )
        self.assertEqual(ex.name, "binance_sbe_us")


class CreateOrderBookDataSourceTest(unittest.TestCase):

    def test_create_data_source_returns_sbe_variant(self):
        ex = BinanceSbeExchange(
            binance_sbe_api_key="K",
            binance_api_key="",
            binance_api_secret="",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
        )
        ds = ex._create_order_book_data_source()
        # SBE-specific class, NOT the JSON-side base.
        self.assertIsInstance(ds, BinanceSbeAPIOrderBookDataSource)
        # IsInstance check inherits, so also pin the exact type.
        self.assertIs(type(ds), BinanceSbeAPIOrderBookDataSource)

    def test_create_data_source_forwards_sbe_api_key(self):
        ex = BinanceSbeExchange(
            binance_sbe_api_key="my-ed25519-key",
            binance_api_key="",
            binance_api_secret="",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
        )
        ds = ex._create_order_book_data_source()
        self.assertEqual(ds._sbe_api_key, "my-ed25519-key")


class TradingRequiredCompatibilityTest(unittest.TestCase):
    """HMAC creds are required by the framework even for signal-only
    roles (the parent BinanceExchange issues signed REST calls during
    startup regardless of role). These tests pin that contract: full
    construction with HMAC works, partial construction is documented
    as the unsupported path."""

    def test_full_construction_with_hmac(self):
        ex = BinanceSbeExchange(
            binance_sbe_api_key="K",
            binance_api_key="hmac-key",
            binance_api_secret="hmac-secret",
            trading_pairs=["BTC-USDT"],
            trading_required=True,
        )
        self.assertEqual(ex.name, "binance_sbe")
        # trading_required NOT downgraded — framework's value preserved.
        self.assertTrue(ex._trading_required)

    def test_trading_required_false_also_constructs(self):
        # If the framework explicitly says trading_required=False (rare
        # — e.g. when running tools manually), we still accept it.
        ex = BinanceSbeExchange(
            binance_sbe_api_key="K",
            binance_api_key="hmac-key",
            binance_api_secret="hmac-secret",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
        )
        self.assertFalse(ex._trading_required)


class SbeApiKeyResolutionTest(unittest.TestCase):
    """The SBE API key may come from either Hummingbot's encrypted config
    (constructor kwarg) OR a plaintext .env (env var fallback). Test both
    and the failure mode when neither is set."""

    def setUp(self):
        # Snapshot env and strip the fallback so tests don't see whatever
        # the dev's shell has exported.
        self._saved_env = os.environ.get("BINANCE_SBE_API_KEY")
        os.environ.pop("BINANCE_SBE_API_KEY", None)

    def tearDown(self):
        if self._saved_env is None:
            os.environ.pop("BINANCE_SBE_API_KEY", None)
        else:
            os.environ["BINANCE_SBE_API_KEY"] = self._saved_env

    def test_constructor_arg_wins_over_env(self):
        # Hummingbot's encrypted-config path supplies the kwarg; that
        # MUST take precedence over any env var so operators can
        # override per-instance without exporting different env vars.
        with patch.dict(os.environ, {"BINANCE_SBE_API_KEY": "from-env"}):
            ex = BinanceSbeExchange(
                binance_sbe_api_key="from-kwarg",
                trading_pairs=["BTC-USDT"],
                trading_required=False,
            )
        self.assertEqual(ex._sbe_api_key, "from-kwarg")

    def test_env_var_used_when_kwarg_empty(self):
        # Headless deployments without `connect binance_sbe`: the
        # framework calls the constructor with empty string, env var
        # picks up the slack.
        with patch.dict(os.environ, {"BINANCE_SBE_API_KEY": "from-env-only"}):
            ex = BinanceSbeExchange(
                binance_sbe_api_key="",
                trading_pairs=["BTC-USDT"],
                trading_required=False,
            )
        self.assertEqual(ex._sbe_api_key, "from-env-only")

    def test_no_key_anywhere_raises(self):
        # Both paths empty → fail loud at boot, not later at WS 401.
        with self.assertRaises(ValueError) as cm:
            BinanceSbeExchange(
                binance_sbe_api_key="",
                trading_pairs=["BTC-USDT"],
                trading_required=False,
            )
        # Error message must point to BOTH credential paths so the
        # operator knows their options.
        msg = str(cm.exception)
        self.assertIn("connect binance_sbe", msg)
        self.assertIn("BINANCE_SBE_API_KEY", msg)


class InheritanceSmokeTest(unittest.TestCase):

    def test_inherits_binance_exchange(self):
        ex = BinanceSbeExchange(
            binance_sbe_api_key="K",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
        )
        self.assertIsInstance(ex, BinanceExchange)

    def test_authenticator_inherited_from_binance(self):
        # We rely on BinanceAuth (HMAC) for trading paths. Asserting that
        # the @property is the same callable that the JSON connector
        # exposes pins the inheritance contract.
        ex = BinanceSbeExchange(
            binance_sbe_api_key="K",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
        )
        # The property must come from BinanceExchange (not overridden in
        # the SBE subclass). Comparing descriptors directly checks this.
        self.assertIs(type(ex).authenticator, BinanceExchange.authenticator)

    def test_data_source_factory_overridden_not_inherited(self):
        # The override is the whole point of the subclass — assert that
        # we are NOT using the JSON connector's data source factory.
        self.assertIsNot(
            BinanceSbeExchange._create_order_book_data_source,
            BinanceExchange._create_order_book_data_source,
        )


if __name__ == "__main__":
    unittest.main()
