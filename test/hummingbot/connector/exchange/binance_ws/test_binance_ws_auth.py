"""Tests for ``binance_ws_auth.sign_request_params``.

Pin the signature output against a golden HMAC vector computed by hand
from the documented spec. Any drift here means we'd silently fail every
signed call at runtime — first request would return -1022 "Signature
for this request is not valid".
"""
from __future__ import annotations

import hashlib
import hmac
import unittest

from hummingbot.connector.exchange.binance_ws.binance_ws_auth import sign_request_params


def _golden_signature(payload: str, secret: str) -> str:
    return hmac.new(secret.encode("utf8"), payload.encode("utf8"), hashlib.sha256).hexdigest()


class SignRequestParamsTest(unittest.TestCase):

    API_KEY = "test-api-key"
    SECRET = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
    FIXED_TS_SECONDS = 1_700_000_000.0
    FIXED_TS_MS = 1_700_000_000_000

    def _time_provider(self) -> float:
        return self.FIXED_TS_SECONDS

    def test_returns_new_dict_does_not_mutate_input(self):
        params = {"symbol": "BTCUSDT", "side": "BUY"}
        snapshot = dict(params)
        out = sign_request_params(params, self.API_KEY, self.SECRET, self._time_provider)
        self.assertEqual(params, snapshot)
        self.assertIsNot(out, params)

    def test_injects_apiKey_timestamp_signature(self):
        out = sign_request_params({"symbol": "BTCUSDT"}, self.API_KEY, self.SECRET, self._time_provider)
        self.assertEqual(out["apiKey"], self.API_KEY)
        self.assertEqual(out["timestamp"], self.FIXED_TS_MS)
        self.assertIn("signature", out)
        # 64 hex chars = 32 bytes = SHA256.
        self.assertEqual(len(out["signature"]), 64)

    def test_signature_payload_is_alphabetical(self):
        # Build the same params in two different insertion orders and
        # verify both produce the identical signature — the signing
        # algorithm must be order-insensitive.
        a = sign_request_params(
            {"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT"},
            self.API_KEY, self.SECRET, self._time_provider,
        )
        b = sign_request_params(
            {"type": "LIMIT", "side": "BUY", "symbol": "BTCUSDT"},
            self.API_KEY, self.SECRET, self._time_provider,
        )
        self.assertEqual(a["signature"], b["signature"])

    def test_signature_matches_documented_golden_vector(self):
        # Golden vector: payload must be alphabetical, no URL-encoding
        # of values. Build the expected payload by hand and HMAC it.
        params = {"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
                  "quantity": "0.001", "price": "20000", "timeInForce": "GTC"}
        out = sign_request_params(params, self.API_KEY, self.SECRET, self._time_provider)

        expected_payload_items = sorted({
            **params,
            "apiKey": self.API_KEY,
            "timestamp": self.FIXED_TS_MS,
        }.items())
        expected_payload = "&".join(f"{k}={v}" for k, v in expected_payload_items)
        expected = _golden_signature(expected_payload, self.SECRET)
        self.assertEqual(out["signature"], expected)

    def test_no_url_encoding_of_values(self):
        # Confirm raw concatenation: a value with characters that would
        # be percent-encoded by urlencode must not be re-encoded here.
        params = {"newClientOrderId": "x-MG43PCSN_abc=123"}
        out = sign_request_params(params, self.API_KEY, self.SECRET, self._time_provider)

        # Build expected payload alphabetically (apiKey < newClientOrderId < timestamp).
        items = sorted({
            "apiKey": self.API_KEY,
            "newClientOrderId": "x-MG43PCSN_abc=123",
            "timestamp": self.FIXED_TS_MS,
        }.items())
        expected_payload = "&".join(f"{k}={v}" for k, v in items)
        expected = _golden_signature(expected_payload, self.SECRET)
        self.assertEqual(out["signature"], expected)

    def test_timestamp_comes_from_time_provider(self):
        # Two distinct providers → two distinct signatures.
        out_a = sign_request_params({"symbol": "BTCUSDT"}, self.API_KEY, self.SECRET,
                                    lambda: 1_700_000_000.0)
        out_b = sign_request_params({"symbol": "BTCUSDT"}, self.API_KEY, self.SECRET,
                                    lambda: 1_700_000_001.0)
        self.assertNotEqual(out_a["signature"], out_b["signature"])
        self.assertNotEqual(out_a["timestamp"], out_b["timestamp"])

    def test_idempotent_same_inputs_same_signature(self):
        # Calling twice with identical inputs (incl. frozen clock) must
        # produce the identical dict. Guards against accidental random
        # salts being introduced later.
        out_a = sign_request_params({"symbol": "BTCUSDT"}, self.API_KEY, self.SECRET, self._time_provider)
        out_b = sign_request_params({"symbol": "BTCUSDT"}, self.API_KEY, self.SECRET, self._time_provider)
        self.assertEqual(out_a, out_b)


if __name__ == "__main__":
    unittest.main()
