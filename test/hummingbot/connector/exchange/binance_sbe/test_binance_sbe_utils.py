"""Auto-discovery sanity and config-map shape tests for binance_sbe.

The Hummingbot framework scans ``hummingbot/connector/exchange/*/`` and
expects ``<name>_utils.py`` to expose ``KEYS``, ``EXAMPLE_PAIR``, and
``DEFAULT_FEES``. If any of those break we want a unit test failure
rather than an opaque "connector not found" runtime error.
"""
from __future__ import annotations

import unittest

from pydantic import SecretStr

from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.exchange.binance_sbe import binance_sbe_utils


class AutoDiscoveryTest(unittest.TestCase):

    def test_binance_sbe_discovered_alongside_binance(self):
        # Recreate the registry — the test must work even when run in
        # isolation, before any other test has triggered creation.
        AllConnectorSettings.create_connector_settings()
        settings = AllConnectorSettings.get_connector_settings()
        self.assertIn("binance_sbe", settings,
                      "auto-discovery did not pick up the binance_sbe connector")
        self.assertIn("binance", settings,
                      "binance original must remain registered alongside binance_sbe")

    def test_binance_sbe_resolves_to_correct_module_and_class(self):
        AllConnectorSettings.create_connector_settings()
        s = AllConnectorSettings.get_connector_settings()["binance_sbe"]
        self.assertEqual(s.class_name(), "BinanceSbeExchange")
        self.assertEqual(
            s.module_path(),
            "hummingbot.connector.exchange.binance_sbe.binance_sbe_exchange",
        )


class ConfigMapShapeTest(unittest.TestCase):

    def test_keys_constant_present(self):
        self.assertTrue(hasattr(binance_sbe_utils, "KEYS"),
                        "binance_sbe_utils.KEYS missing — auto-discovery will skip")

    def test_connector_name_is_binance_sbe(self):
        # The ConfigMap.connector field is the canonical name the
        # framework uses to wire YAML referenced connectors.
        self.assertEqual(binance_sbe_utils.KEYS.connector, "binance_sbe")

    def test_required_sbe_api_key_field(self):
        cm = binance_sbe_utils.BinanceSbeConfigMap.model_construct(
            binance_sbe_api_key=SecretStr("my-key"),
        )
        self.assertEqual(cm.binance_sbe_api_key.get_secret_value(), "my-key")

    def test_configmap_has_only_sbe_key_no_hmac_fields(self):
        # The ConfigMap intentionally omits HMAC fields. Earlier
        # iterations had them as ``Optional[SecretStr] = None`` but
        # that broke Security.decrypt_all() across all connectors when
        # the resulting yaml serialised them as ``null``. See the
        # docstring of binance_sbe_utils.BinanceSbeConfigMap for the
        # full history; Phase 2 (taker role) will need a different
        # mechanism if/when it lands.
        fields = binance_sbe_utils.BinanceSbeConfigMap.model_fields
        self.assertIn("binance_sbe_api_key", fields)
        self.assertNotIn("binance_api_key", fields,
                         "binance_api_key must NOT be on the ConfigMap — null serialisation breaks decrypt_all")
        self.assertNotIn("binance_api_secret", fields,
                         "binance_api_secret must NOT be on the ConfigMap — null serialisation breaks decrypt_all")

    def test_example_pair_present(self):
        self.assertTrue(binance_sbe_utils.EXAMPLE_PAIR)

    def test_default_fees_re_exported(self):
        # We reuse the JSON connector's fee schema because fees apply to
        # trading (which still goes REST), not market data.
        from hummingbot.connector.exchange.binance.binance_utils import DEFAULT_FEES as JSON_FEES
        self.assertIs(binance_sbe_utils.DEFAULT_FEES, JSON_FEES)


if __name__ == "__main__":
    unittest.main()
