import asyncio
import json
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_auth import BitprecoAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


class BitprecoAPIUserStreamDataSource(UserStreamTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: BitprecoAuth,
                 trading_pairs: List[str],
                 connector: 'BitprecoExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: BitprecoAuth = auth
        self._current_listen_key = None
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector

        self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        self._last_listen_key_ping_ts = 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._get_ws_assistant()

        await ws.connect(ws_url=CONSTANTS.WSS_NOTIFICATIONS_URL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            auth_token = f'{self._connector.secret_key}{self._connector.api_key}'

            notifications_topic = f'{CONSTANTS.WS_NOTIFICATIONS_TOPIC}:{auth_token}'

            payload = dict(topic=notifications_topic, event="phx_join", payload={}, ref=None)

            subscribe_notifications_request: WSPlainTextRequest = WSPlainTextRequest(json.dumps(payload))

            await websocket_assistant.send(subscribe_notifications_request)

            self.logger().info("Subscribed to private notification channel of bitpreco...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to private notification channel of BitPreco......",
                exc_info=True
            )
            raise

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
