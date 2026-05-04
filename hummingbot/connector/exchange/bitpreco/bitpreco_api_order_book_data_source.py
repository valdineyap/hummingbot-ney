import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import hummingbot.connector.exchange.bitpreco.bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco import bitpreco_web_utils as web_utils
from hummingbot.connector.exchange.bitpreco.bitpreco_order_book import BitprecoOrderBook
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


class BitprecoAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BitprecoExchange',
                 api_factory: Optional[WebAssistantsFactory] = None,
                 throttler: Optional[AsyncThrottler] = None):
        super().__init__(trading_pairs)
        self._trade_messages_queue_key = "trade"
        self._diff_messages_queue_key = "orderbook"
        self._snapshot_messages_queue_key = "snapshot"
        self._connector = connector
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
        )
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._last_ws_message_sent_timestamp = 0

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: str = CONSTANTS.DEFAULT_DOMAIN,
                                     api_factory: Optional[WebAssistantsFactory] = None,
                                     throttler: Optional[AsyncThrottler] = None,
                                     time_synchronizer: Optional[TimeSynchronizer] = None) -> Dict[str, float]:
        """
        Return a dictionary the trading_pair as key and the current price as value for each trading pair passed as
        parameter
        :param trading_pairs: list of trading pairs to get the prices for
        :param domain: which BitPreco domain we are connecting to (the default value is 'bitpreco_trading')
        :param api_factory: the instance of the web assistant factory to be used when doing requests to the server.
            If no instance is provided then a new one will be created.
        :param throttler: the instance of the throttler to use to limit request to the server. If it is not specified
        the function will create a new one.
        :param time_synchronizer: the synchronizer instance being used to keep track of the time difference with the
            exchange
        :return: Dictionary of associations between token pair and its latest price
        """
        mapping = {}

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL, domain=""),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL
        )

        for pair in data:
            if pair != "success":
                mapping[pair] = float(data[pair]["last"])

        return mapping

    def _format_orderbook_url(self, trading_pair: str):
        lower_trading_pair = trading_pair.lower()
        return CONSTANTS.ORDER_BOOK_PATH_URL.format(lower_trading_pair)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        market = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        order_book_url = self._format_orderbook_url(market)

        rest_assistant = await self._api_factory.get_rest_assistant()

        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=order_book_url, domain=""),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.REST_URL
        )
        return data

    async def listen_for_subscriptions(self):
        ws: Optional[WSAssistant] = None
        while True:
            try:
                await self._process_websocket_messages(websocket_assistant=ws)
                await self._sleep(0.5)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                )
                await self._sleep(5.0)
            finally:
                ws and await ws.disconnect()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        for trading_pair in self._trading_pairs:
            order_book = await self._request_order_book_snapshot(trading_pair=trading_pair)
            snapshot_msg: OrderBookMessage = BitprecoOrderBook.snapshot_message_from_exchange_rest(
                msg=order_book,
                timestamp=self._time(),
                metadata={"trading_pair": trading_pair},
            )
            self._message_queue[self._diff_messages_queue_key].put_nowait(snapshot_msg)

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        pass

    async def _connected_websocket_assistant(self) -> WSAssistant:
        pass

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = self._time()
        snapshot_msg: OrderBookMessage = BitprecoOrderBook.snapshot_message_from_exchange_rest(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if "event" in raw_message:
            if raw_message.get("event") != "phx_reply":
                topic = raw_message.get("topic")
                symbol = topic.split(":", 1)[-1]
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                order_book_message: OrderBookMessage = BitprecoOrderBook.diff_message_from_exchange(
                    raw_message, self._time(), {"trading_pair": trading_pair})
                message_queue.put_nowait(order_book_message)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        # TODO understand what the trades channel must bring
        pass

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "topic" in event_message:
            topic = event_message.get("topic")
            if ":" in topic:
                channel = topic.split(":", 1)[0]

        return channel

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True

    def _time(self):
        return time.time()
