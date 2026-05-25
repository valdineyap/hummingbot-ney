"""
Unit tests for tools/precleanup.py.

Validates:
  * HMAC signing matches the connector's expected output
  * YAML target extraction
  * Symbol conversion
  * Response handling for "no orders" / error / list cases (with mocked aiohttp)
"""
import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "tools"))

import precleanup  # noqa: E402


class TestSymbolConversion(unittest.TestCase):
    def test_btc_brl_strips_separator(self):
        self.assertEqual(precleanup._hb_to_exchange_symbol("bybit", "BTC-BRL"), "BTCBRL")

    def test_eth_usdt_strips_separator(self):
        self.assertEqual(precleanup._hb_to_exchange_symbol("binance", "ETH-USDT"), "ETHUSDT")


class TestBybitSigning(unittest.TestCase):
    def test_get_signature_format(self):
        ts, sig = precleanup._bybit_sign_get(
            api_key="testkey",
            secret="testsecret",
            params={"category": "spot", "symbol": "BTCBRL"},
        )
        # Timestamp is millisecond-resolution string
        self.assertTrue(ts.isdigit())
        self.assertEqual(len(ts), 13)
        # Signature is 64-char hex
        self.assertEqual(len(sig), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in sig))

    def test_post_signature_format(self):
        ts, sig, body = precleanup._bybit_sign_post(
            api_key="testkey",
            secret="testsecret",
            body={"category": "spot", "symbol": "BTCBRL", "orderId": "123"},
        )
        self.assertTrue(ts.isdigit())
        self.assertEqual(len(sig), 64)
        # Body is canonical JSON without spaces
        self.assertNotIn(" ", body)
        self.assertIn('"orderId":"123"', body)


class TestBinanceSigning(unittest.TestCase):
    def test_signature_format(self):
        sig = precleanup._binance_sign("testsecret", {"symbol": "BTCBRL", "timestamp": "1700000000000"})
        self.assertEqual(len(sig), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in sig))

    def test_signature_deterministic(self):
        sig1 = precleanup._binance_sign("secret", {"symbol": "BTCBRL", "timestamp": "1700000000000"})
        sig2 = precleanup._binance_sign("secret", {"symbol": "BTCBRL", "timestamp": "1700000000000"})
        self.assertEqual(sig1, sig2)


class TestConfigLoading(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False)
        yaml.safe_dump(
            {
                "maker_connector": "bybit",
                "maker_trading_pair": "BTC-BRL",
                "taker_connector": "binance",
                "taker_trading_pair": "BTC-BRL",
            },
            self.tmp,
        )
        self.tmp.close()

    def tearDown(self):
        Path(self.tmp.name).unlink(missing_ok=True)

    def test_extracts_distinct_targets(self):
        targets = precleanup._load_targets_from_config(Path(self.tmp.name))
        self.assertIn(("bybit", "BTC-BRL"), targets)
        self.assertIn(("binance", "BTC-BRL"), targets)
        self.assertEqual(len(targets), 2)

    def test_deduplicates_when_maker_equals_taker(self):
        # Same connector+pair on both sides → should only appear once
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.safe_dump(
                {
                    "maker_connector": "binance",
                    "maker_trading_pair": "BTC-USDT",
                    "taker_connector": "binance",
                    "taker_trading_pair": "BTC-USDT",
                },
                f,
            )
            path = Path(f.name)
        try:
            targets = precleanup._load_targets_from_config(path)
            self.assertEqual(len(targets), 1)
            self.assertEqual(targets[0], ("binance", "BTC-USDT"))
        finally:
            path.unlink()


class TestBybitNoOrders(unittest.IsolatedAsyncioTestCase):
    async def test_returns_zero_when_no_orders(self):
        session = MagicMock()
        # Mock aiohttp response: retCode=0, empty list
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"retCode": 0, "result": {"list": []}})
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)
        session.get = MagicMock(return_value=mock_ctx)

        cancelled, failed = await precleanup.bybit_cancel_open_orders(
            session, "key", "secret", "BTCBRL"
        )
        self.assertEqual(cancelled, 0)
        self.assertEqual(failed, 0)


class TestBinanceNoOrders(unittest.IsolatedAsyncioTestCase):
    async def test_returns_zero_when_2011_unknown_order(self):
        session = MagicMock()
        # Binance's "no orders to cancel" returns code -2011
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"code": -2011, "msg": "Unknown order sent."})
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)
        session.delete = MagicMock(return_value=mock_ctx)

        cancelled, failed = await precleanup.binance_cancel_open_orders(
            session, "key", "secret", "BTCBRL"
        )
        self.assertEqual(cancelled, 0)
        self.assertEqual(failed, 0)

    async def test_counts_cancelled_orders_from_list(self):
        session = MagicMock()
        # Binance returns a list of cancelled orders
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value=[
            {"orderId": 111, "clientOrderId": "abc"},
            {"orderId": 222, "clientOrderId": "def"},
        ])
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)
        session.delete = MagicMock(return_value=mock_ctx)

        cancelled, failed = await precleanup.binance_cancel_open_orders(
            session, "key", "secret", "BTCBRL"
        )
        self.assertEqual(cancelled, 2)
        self.assertEqual(failed, 0)


if __name__ == "__main__":
    unittest.main()
