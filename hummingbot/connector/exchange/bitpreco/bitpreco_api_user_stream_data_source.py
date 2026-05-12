import asyncio
import json
import time
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
        # Q3 instrumentation: track WS lifecycle so dropouts are visible. Each
        # _connected_websocket_assistant() call = one connect (a reconnect if
        # we had a prior one). Cycle duration = uptime since previous connect.
        self._ws_last_connect_ts: float = 0.0
        self._ws_connect_count: int = 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._get_ws_assistant()

        await ws.connect(ws_url=CONSTANTS.WSS_NOTIFICATIONS_URL)
        now_t = time.time()
        self._ws_connect_count += 1
        if self._ws_last_connect_ts > 0:
            uptime_s = now_t - self._ws_last_connect_ts
            self.logger().info(
                f"[ws_lifecycle] connected #{self._ws_connect_count} "
                f"(prior connection cycle: {uptime_s:.1f}s)"
            )
        else:
            self.logger().info(
                f"[ws_lifecycle] connected #{self._ws_connect_count} (initial)"
            )
        self._ws_last_connect_ts = now_t
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

            # ----- Post-reconnect catch-up (Task 2.2) -----
            # The WS user-stream drops every ~70-90s in production. Each
            # reconnect creates a window where order state changes (fills,
            # cancels) emitted by the exchange are not delivered. After
            # subscribe success, fire a one-shot REST status poll to
            # reconcile any in-flight orders. The connector's
            # ``_update_order_status`` walks ``in_flight_orders`` and pulls
            # current status from REST — anything that changed during the
            # gap surfaces here. We schedule it as a fire-and-forget task
            # so it doesn't block the WS handshake.
            try:
                if self._connector is not None and getattr(self._connector, "in_flight_orders", None):
                    asyncio.create_task(self._post_reconnect_catch_up())
            except Exception as e:
                self.logger().warning(
                    f"[ws_catchup] failed to schedule post-reconnect catch-up: {e}"
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to private notification channel of BitPreco......",
                exc_info=True
            )
            raise

    async def _post_reconnect_catch_up(self) -> None:
        """One-shot REST status poll triggered after every WS reconnect.

        Hummingbot's tracker reconciles the result against in-flight orders
        and emits ``OrderFilledEvent`` / ``OrderCancelledEvent`` for any
        state change discovered. Idempotent: re-poll of an unchanged order
        is a no-op for downstream consumers.
        """
        try:
            n = len(self._connector.in_flight_orders) if hasattr(self._connector, "in_flight_orders") else 0
            if n == 0:
                return  # nothing to reconcile (cold connect)
            self.logger().info(
                f"[ws_catchup] post-reconnect REST status poll for "
                f"{n} in-flight orders"
            )
            await self._connector._update_order_status()
        except Exception as e:
            # Never let the catch-up crash the listener loop.
            self.logger().warning(
                f"[ws_catchup] REST status poll failed: {type(e).__name__}: {e}"
            )

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
