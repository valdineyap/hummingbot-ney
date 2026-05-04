import asyncio
import json
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import hummingbot.connector.exchange.bitpreco.bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_api_user_stream_data_source import BitprecoAPIUserStreamDataSource
from hummingbot.connector.exchange.bitpreco.bitpreco_auth import BitprecoAuth
from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange
import hummingbot.connector.exchange.bitpreco.bitpreco_web_utils as web_utils


class BitprecoAPIUserStreamDataSourceTests(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.api_key = "testApiKey"
        cls.api_secret = "testApiSecret"
        cls.trading_pair = "BTC-BRL"

    def setUp(self) -> None:
        super().setUp()
        self.loop = asyncio.new_event_loop()
        self.mock_config = MagicMock()
        self.connector = BitprecoExchange(
            client_config_map=self.mock_config,
            bitpreco_api_key=self.api_key,
            bitpreco_api_secret=self.api_secret,
            trading_pairs=[self.trading_pair],
        )
        self.auth = BitprecoAuth(api_key=self.api_key, secret_key=self.api_secret)
        self.api_factory = web_utils.build_api_factory(auth=self.auth)
        self.data_source = BitprecoAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )

    def tearDown(self) -> None:
        self.loop.close()
        super().tearDown()

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    def test_subscribe_channels_sends_phx_join_with_correct_topic(self):
        sent_messages = []

        async def mock_send(request):
            import json
            sent_messages.append(json.loads(request.payload))

        ws_mock = AsyncMock()
        ws_mock.send = mock_send

        self._run(self.data_source._subscribe_channels(ws_mock))

        self.assertEqual(1, len(sent_messages))
        msg = sent_messages[0]
        self.assertEqual("phx_join", msg["event"])
        expected_token = f"{self.api_secret}{self.api_key}"
        expected_topic = f"{CONSTANTS.WS_NOTIFICATIONS_TOPIC}:{expected_token}"
        self.assertEqual(expected_topic, msg["topic"])

    def test_subscribe_channels_sends_empty_payload(self):
        sent_messages = []

        async def mock_send(request):
            import json
            sent_messages.append(json.loads(request.payload))

        ws_mock = AsyncMock()
        ws_mock.send = mock_send

        self._run(self.data_source._subscribe_channels(ws_mock))

        msg = sent_messages[0]
        self.assertEqual({}, msg["payload"])
        self.assertIsNone(msg["ref"])

    def test_connected_websocket_assistant_connects_to_notifications_url(self):
        connected_urls = []

        async def mock_connect(ws_url):
            connected_urls.append(ws_url)

        ws_mock = AsyncMock()
        ws_mock.connect = mock_connect

        async def mock_get_ws():
            return ws_mock

        self.data_source._get_ws_assistant = mock_get_ws

        self._run(self.data_source._connected_websocket_assistant())

        self.assertEqual(1, len(connected_urls))
        self.assertEqual(CONSTANTS.WSS_NOTIFICATIONS_URL, connected_urls[0])

    def test_auth_token_is_correct_psk(self):
        expected_token = f"{self.api_secret}{self.api_key}"
        actual_token = f"{self.connector.secret_key}{self.connector.api_key}"
        self.assertEqual(expected_token, actual_token)
