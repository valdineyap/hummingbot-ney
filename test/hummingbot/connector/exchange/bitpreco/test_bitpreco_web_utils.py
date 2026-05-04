import unittest

import hummingbot.connector.exchange.bitpreco.bitpreco_constants as CONSTANTS
import hummingbot.connector.exchange.bitpreco.bitpreco_web_utils as web_utils


class BitprecoWebUtilsTests(unittest.TestCase):

    def test_public_rest_url_with_absolute_url_returns_unchanged(self):
        absolute_url = CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL
        result = web_utils.public_rest_url(absolute_url)
        self.assertEqual(absolute_url, result)

    def test_public_rest_url_with_orderbook_url_returns_unchanged(self):
        order_book_url = CONSTANTS.ORDER_BOOK_PATH_URL.format("btc-brl")
        result = web_utils.public_rest_url(order_book_url)
        self.assertEqual(order_book_url, result)

    def test_public_rest_url_with_relative_path_returns_rest_url(self):
        result = web_utils.public_rest_url("some/relative/path")
        self.assertEqual(CONSTANTS.REST_URL, result)

    def test_private_rest_url_with_absolute_url_returns_unchanged(self):
        absolute_url = CONSTANTS.REST_URL
        result = web_utils.private_rest_url(absolute_url)
        self.assertEqual(absolute_url, result)

    def test_private_rest_url_with_relative_path_returns_rest_url(self):
        result = web_utils.private_rest_url("trading")
        self.assertEqual(CONSTANTS.REST_URL, result)

    def test_public_and_private_rest_url_with_same_absolute_input(self):
        url = CONSTANTS.PING_PATH_URL
        self.assertEqual(web_utils.public_rest_url(url), web_utils.private_rest_url(url))

    def test_create_throttler_returns_non_none(self):
        throttler = web_utils.create_throttler()
        self.assertIsNotNone(throttler)

    def test_build_api_factory_returns_non_none(self):
        factory = web_utils.build_api_factory()
        self.assertIsNotNone(factory)

    def test_build_api_factory_without_time_synchronizer(self):
        throttler = web_utils.create_throttler()
        factory = web_utils.build_api_factory_without_time_synchronizer_pre_processor(throttler)
        self.assertIsNotNone(factory)

    def test_rest_url_constants_are_absolute(self):
        self.assertTrue(CONSTANTS.REST_URL.startswith("https://"))
        self.assertTrue(CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL.startswith("https://"))
        self.assertTrue(CONSTANTS.PING_PATH_URL.startswith("https://"))

    def test_domain_parameter_is_optional(self):
        url = CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL
        result_no_domain = web_utils.public_rest_url(url)
        result_with_domain = web_utils.public_rest_url(url, domain=CONSTANTS.DEFAULT_DOMAIN)
        self.assertEqual(result_no_domain, result_with_domain)
