"""Auto-discovery sentinel.

Hummingbot's framework iterates ``hummingbot/connector/exchange/*/``
looking for ``<name>_utils.py`` files exporting ``KEYS``, ``EXAMPLE_PAIR``
and ``DEFAULT_FEES``. Anything missed there is invisible to the CLI
(``connect <name>`` would error) AND to the controller-loading path,
which is the same defect that would silently break the bot at boot.

This test pins that ``binance_ws`` appears alongside ``binance`` and
``binance_sbe`` after a fresh registration cycle. A regression here is
either:
  - a missing/renamed file in the connector dir, or
  - a malformed ``KEYS`` (e.g. ``Optional[SecretStr] = None`` reintroduced
    after the encrypted-config bug we already hit on SBE).
"""
from __future__ import annotations

import unittest


class AutoDiscoveryTest(unittest.TestCase):

    def test_all_three_binance_variants_registered(self):
        from hummingbot.client.settings import AllConnectorSettings
        AllConnectorSettings.create_connector_settings()
        settings = AllConnectorSettings.get_connector_settings()
        for name in ("binance", "binance_sbe", "binance_ws"):
            with self.subTest(name=name):
                self.assertIn(name, settings)


if __name__ == "__main__":
    unittest.main()
