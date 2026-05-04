import asyncio
import json
from unittest import TestCase

from hummingbot.connector.exchange.bitpreco.bitpreco_auth import BitprecoAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest


class BitprecoAuthTests(TestCase):

    def setUp(self) -> None:
        self._api_key = "testApiKey"
        self._secret = "testSecret"
        self._auth = BitprecoAuth(api_key=self._api_key, secret_key=self._secret)

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_rest_authenticate_post_adds_auth_token(self):
        body = {"cmd": "balance", "market": "btc-brl"}
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.bitpreco.com/trading",
            data=json.dumps(body),
            is_auth_required=True,
        )
        authenticated = self._run(self._auth.rest_authenticate(request))

        data = dict(authenticated.data)
        self.assertIn("auth_token", data)
        self.assertEqual(f"{self._secret}{self._api_key}", data["auth_token"])
        self.assertEqual(body["cmd"], data["cmd"])

    def test_rest_authenticate_get_does_not_add_auth_token(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.bitpreco.com/btc-brl/ticker",
            is_auth_required=False,
        )
        authenticated = self._run(self._auth.rest_authenticate(request))
        self.assertIsNone(authenticated.data)

    def test_rest_authenticate_preserves_existing_fields(self):
        body = {"cmd": "buy", "market": "btc-brl", "amount": "0.01", "price": "100000"}
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.bitpreco.com/trading",
            data=json.dumps(body),
            is_auth_required=True,
        )
        authenticated = self._run(self._auth.rest_authenticate(request))

        data = dict(authenticated.data)
        self.assertEqual("buy", data["cmd"])
        self.assertEqual("btc-brl", data["market"])
        self.assertEqual("0.01", data["amount"])
        self.assertEqual("100000", data["price"])
        self.assertIn("auth_token", data)

    def test_ws_authenticate_returns_request_unchanged(self):
        request = WSJSONRequest(payload={"event": "phx_join", "topic": "notifications:test"})
        result = self._run(self._auth.ws_authenticate(request))
        self.assertIs(request, result)

    def test_auth_token_format(self):
        expected_token = f"{self._secret}{self._api_key}"
        self.assertEqual("testSecrettestApiKey", expected_token)

        body = {"cmd": "balance"}
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.bitpreco.com/trading",
            data=json.dumps(body),
            is_auth_required=True,
        )
        authenticated = self._run(self._auth.rest_authenticate(request))
        self.assertEqual(expected_token, dict(authenticated.data)["auth_token"])
