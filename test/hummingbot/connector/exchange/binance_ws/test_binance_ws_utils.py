"""Tests for ``binance_ws_utils``: ConfigMap schema + rate-limit table."""
from __future__ import annotations

import unittest

from hummingbot.connector.exchange.binance.binance_constants import (
    ORDERS,
    RAW_REQUESTS,
    REQUEST_WEIGHT,
)
from hummingbot.connector.exchange.binance_ws import binance_ws_constants as CONSTANTS
from hummingbot.connector.exchange.binance_ws.binance_ws_utils import (
    EXAMPLE_PAIR,
    KEYS,
    BinanceWsConfigMap,
    build_rate_limits,
)


class ConfigMapSchemaTest(unittest.TestCase):

    def test_two_secret_str_fields_required(self):
        # Pin the field count + the names. If a future refactor adds a
        # third secret field, this test forces the author to revisit
        # binance_ws_register.py at the same time.
        fields = BinanceWsConfigMap.model_fields
        # Exactly the two declared SecretStr fields + 'connector' literal
        # + 'use_ws_trading' bool flag.
        self.assertIn("binance_ws_api_key", fields)
        self.assertIn("binance_ws_api_secret", fields)
        self.assertIn("use_ws_trading", fields)
        # No `binance_api_key`/`binance_api_secret` namespaced bleed-through.
        self.assertNotIn("binance_api_key", fields)
        self.assertNotIn("binance_api_secret", fields)

    def test_use_ws_trading_defaults_true(self):
        cm = BinanceWsConfigMap.model_construct()
        self.assertTrue(cm.use_ws_trading)

    def test_keys_is_a_constructed_instance(self):
        # Sentinel for the framework's auto-discovery — must be an
        # instance, not the class itself.
        self.assertIsInstance(KEYS, BinanceWsConfigMap)

    def test_example_pair_is_btc_usdt(self):
        self.assertEqual(EXAMPLE_PAIR, "BTC-USDT")


class RateLimitTableTest(unittest.TestCase):

    def setUp(self):
        self._limits = build_rate_limits()
        self._by_id = {rl.limit_id: rl for rl in self._limits}

    def _weight_for(self, limit_id: str, pool: str):
        rl = self._by_id[limit_id]
        for pair in rl.linked_limits:
            if pair.limit_id == pool:
                return pair.weight
        return None

    def test_all_expected_limit_ids_present(self):
        expected = {
            CONSTANTS.WS_LIMIT_CONNECT,
            CONSTANTS.WS_LIMIT_PING,
            CONSTANTS.WS_LIMIT_ORDER_TEST,
            CONSTANTS.WS_LIMIT_ORDER_TEST_COMMISSION,
            CONSTANTS.WS_LIMIT_ORDER_PLACE,
            CONSTANTS.WS_LIMIT_ORDER_CANCEL,
            CONSTANTS.WS_LIMIT_ORDER_STATUS,
            CONSTANTS.WS_LIMIT_ACCOUNT_RATE_LIMITS,
        }
        self.assertEqual(set(self._by_id.keys()), expected)

    def test_weights_per_method(self):
        cases = [
            (CONSTANTS.WS_LIMIT_CONNECT, 2, 0),
            (CONSTANTS.WS_LIMIT_PING, 1, 0),
            (CONSTANTS.WS_LIMIT_ORDER_TEST, 1, 0),
            (CONSTANTS.WS_LIMIT_ORDER_TEST_COMMISSION, 20, 0),
            (CONSTANTS.WS_LIMIT_ORDER_PLACE, 1, 1),
            (CONSTANTS.WS_LIMIT_ORDER_CANCEL, 1, 0),
            (CONSTANTS.WS_LIMIT_ORDER_STATUS, 4, 0),
            (CONSTANTS.WS_LIMIT_ACCOUNT_RATE_LIMITS, 40, 0),
        ]
        for limit_id, weight_req, weight_orders in cases:
            with self.subTest(limit_id=limit_id):
                self.assertEqual(self._weight_for(limit_id, REQUEST_WEIGHT), weight_req)
                if weight_orders == 0:
                    # No ORDERS slot — must be absent, not zero-weighted.
                    self.assertIsNone(self._weight_for(limit_id, ORDERS))
                else:
                    self.assertEqual(self._weight_for(limit_id, ORDERS), weight_orders)

    def test_every_limit_links_raw_requests(self):
        # RAW_REQUESTS is per-IP across REST + WS. Each method consumes
        # one raw-request slot regardless of weight.
        for rl in self._limits:
            with self.subTest(limit_id=rl.limit_id):
                self.assertEqual(self._weight_for(rl.limit_id, RAW_REQUESTS), 1)


if __name__ == "__main__":
    unittest.main()
