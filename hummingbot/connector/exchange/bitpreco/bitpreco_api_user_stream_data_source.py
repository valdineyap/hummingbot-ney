import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS
from hummingbot.connector.exchange.bitpreco.bitpreco_auth import BitprecoAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitpreco.bitpreco_exchange import BitprecoExchange


# ----------------------------------------------------------------------
# Phoenix v2 wire format helpers
# ----------------------------------------------------------------------
# Phoenix v2 sends each message as a 5-element array:
#   [join_ref, ref, topic, event, payload]
#
#   join_ref  — set to the ``ref`` of the original ``phx_join`` for
#               channel messages; ``None`` for transport messages
#               (heartbeats on the special "phoenix" topic). Server
#               broadcasts use ``None`` too.
#   ref       — request id (string). The reply ``phx_reply`` echoes
#               this so clients can correlate.
#   topic     — channel topic (e.g. "notifications:<auth_token>" or
#               the special "phoenix" topic).
#   event     — event name (e.g. "phx_join", "heartbeat", "flash",
#               "phx_reply").
#   payload   — event-specific JSON object.
#
# Confirmed against a live network capture from the BitPreco web client
# (https://market.bitypreco.com). The web client uses phoenix-js 1.7.10
# which defaults to ``?vsn=2.0.0`` on the connect URL and serializes in
# this array format. With the legacy v1 (object) format the broadcaster
# pipeline does NOT push events to the client even though the channel
# join succeeds — exactly the "joined but silent" symptom we observed
# before this migration.

def encode_phoenix_v2(
    join_ref: Optional[str],
    ref: Optional[str],
    topic: str,
    event: str,
    payload: Optional[dict] = None,
) -> WSPlainTextRequest:
    """Encode an outgoing Phoenix v2 message into a ``WSPlainTextRequest``."""
    return WSPlainTextRequest(
        json.dumps([join_ref, ref, topic, event, payload or {}])
    )


def normalize_phoenix_message(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize an incoming Phoenix message into a uniform dict.

    Accepts either:
      * v2 array ``[join_ref, ref, topic, event, payload]``
      * v1 object ``{"topic": ..., "event": ..., "payload": ..., "ref": ...}``

    Returns a dict with keys ``join_ref``, ``ref``, ``topic``, ``event``,
    ``payload`` for unified downstream handling. Returns None on
    unparseable input — caller must skip the message.

    Defensive parser: tolerates short/malformed arrays so we don't crash
    on edge cases (e.g. some servers emit 4-element arrays for legacy
    broadcasts). Missing fields become None.
    """
    if isinstance(raw, list):
        # v2 protocol — array form
        return {
            "join_ref": raw[0] if len(raw) > 0 else None,
            "ref":      raw[1] if len(raw) > 1 else None,
            "topic":    raw[2] if len(raw) > 2 else None,
            "event":    raw[3] if len(raw) > 3 else None,
            "payload":  raw[4] if len(raw) > 4 else None,
        }
    if isinstance(raw, dict):
        # v1 protocol — object form. Map to same dict shape for the
        # listener so it doesn't need to branch.
        return {
            "join_ref": raw.get("join_ref"),
            "ref":      raw.get("ref"),
            "topic":    raw.get("topic"),
            "event":    raw.get("event"),
            "payload":  raw.get("payload"),
        }
    return None


class BitprecoAPIUserStreamDataSource(UserStreamTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    # If the server doesn't reply within this many seconds, assume the
    # Phoenix channel went stale and warn. Phoenix default channel
    # ``:timeout`` is 60s, so we set the watchdog at 2× the heartbeat
    # interval (60s) to catch the transition cleanly.
    PHX_HEARTBEAT_STALE_AFTER_SEC = 60.0

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

        # ----- Phoenix application-level heartbeat (2026-05-14) -----
        # Phoenix Channels require an application-level heartbeat on the
        # special topic "phoenix" — distinct from WebSocket/TCP-level pings
        # already handled by aiohttp (heartbeat=10s). Without ``phx_heartbeat``
        # the server marks the channel as dead after ``:timeout`` (default
        # 60s) and stops pushing events, while the TCP socket stays open.
        # That's the hypothesis explaining why ``flash`` events have NEVER
        # arrived in 6+ hours of observation: server-side channel is dead
        # within 60s of join, but we see the connection as healthy.
        # See bitpreco_api_user_stream_data_source.py comment block on
        # _phoenix_heartbeat_loop for protocol details.
        self._phx_heartbeat_task: Optional[asyncio.Task] = None
        # Monotonic counter for ``ref`` field. Phoenix uses ``ref`` to match
        # requests with replies (we'll use it to compute RTT).
        self._phx_heartbeat_ref: int = 0
        # In-flight heartbeats: ref (as string, since the wire format is
        # JSON) → send timestamp. The connector's ``_user_stream_event_listener``
        # pops entries here on matching ``phx_reply`` to compute RTT.
        self._phx_heartbeat_inflight: Dict[str, float] = {}
        # Totals for periodic summary logging:
        self._phx_heartbeat_sent: int = 0
        self._phx_heartbeat_replied: int = 0
        # Last successful reply timestamp — None until first reply lands.
        self._phx_heartbeat_last_reply_ts: Optional[float] = None
        # One-shot flag: has the stale-channel warning already fired since
        # the last successful reply? Prevents log spam if the server stays
        # silent for hours.
        self._phx_heartbeat_stale_logged: bool = False
        # ``join_ref`` for the notifications channel. Set when ``phx_join``
        # is sent in ``_subscribe_channels``; used by channel-targeted
        # messages (none today, but the field is here for future channel
        # push-from-client semantics like ``setNotificationsRead``).
        # Heartbeats use ``None`` (transport-level, not channel-level).
        self._notifications_join_ref: Optional[str] = None
        # Monotonic counter for non-heartbeat refs (join, channel pushes).
        # Heartbeats have their own counter (``_phx_heartbeat_ref``) to
        # keep the namespaces independent — easier to debug from logs.
        self._phx_send_ref: int = 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        # Cancel any previous heartbeat task before reconnecting — otherwise
        # we'd accumulate one task per reconnect, each tied to a dead socket.
        self._cancel_phx_heartbeat_task()

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
        # Reset heartbeat state for the new connection (refs from a dead
        # socket are meaningless; in-flight tracker would never resolve).
        self._phx_heartbeat_inflight.clear()
        self._phx_heartbeat_stale_logged = False
        return ws

    def _cancel_phx_heartbeat_task(self) -> None:
        """Cancel the in-flight heartbeat task, if any. Safe to call when
        no task exists."""
        if self._phx_heartbeat_task is not None and not self._phx_heartbeat_task.done():
            self._phx_heartbeat_task.cancel()
        self._phx_heartbeat_task = None

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            auth_token = f'{self._connector.secret_key}{self._connector.api_key}'

            notifications_topic = f'{CONSTANTS.WS_NOTIFICATIONS_TOPIC}:{auth_token}'

            # Phoenix v2 join. The ``join_ref`` MUST equal the ``ref`` of
            # this very message — that's how the server identifies the
            # channel for future routed pushes. The web client confirmed
            # this convention via live capture: ``["8","8","notifications:...",
            # "phx_join",{}]`` — both array slots are "8".
            self._phx_send_ref += 1
            join_ref = str(self._phx_send_ref)
            self._notifications_join_ref = join_ref

            subscribe_notifications_request = encode_phoenix_v2(
                join_ref=join_ref,
                ref=join_ref,
                topic=notifications_topic,
                event="phx_join",
                payload={},
            )

            await websocket_assistant.send(subscribe_notifications_request)

            self.logger().info(
                f"Subscribed to private notification channel of bitpreco... "
                f"(v2 protocol, join_ref={join_ref})"
            )

            # ----- Phoenix application-level heartbeat (2026-05-14) -----
            # Start the heartbeat AFTER subscribe so the channel is joined
            # before we try to keep it alive. Spawn on the running loop so
            # it survives until either explicit cancel or task exception.
            self._cancel_phx_heartbeat_task()  # defensive: ensure no stale task
            self._phx_heartbeat_task = asyncio.create_task(
                self._phoenix_heartbeat_loop(websocket_assistant)
            )
            self.logger().info(
                f"[phx_hb] starting Phoenix heartbeat loop "
                f"(interval={self.HEARTBEAT_TIME_INTERVAL}s, "
                f"stale_after={self.PHX_HEARTBEAT_STALE_AFTER_SEC}s)"
            )

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

    async def _phoenix_heartbeat_loop(self, ws: WSAssistant) -> None:
        """Application-level keep-alive for the Phoenix Channel (protocol v2).

        Phoenix Channels protocol (https://hexdocs.pm/phoenix/channels.html)
        requires a periodic heartbeat distinct from WebSocket/TCP ping.
        Phoenix v2 array wire format::

            [null, "<ref>", "phoenix", "heartbeat", {}]

        Note three details (all confirmed via live capture from the
        web client at https://market.bitypreco.com 2026-05-14):

          1. ``join_ref`` is ``None`` (heartbeats are transport-level,
             not bound to any joined channel).
          2. ``topic`` is the literal string ``"phoenix"`` (NOT the
             channel topic).
          3. The event name is ``"heartbeat"``, NOT ``"phx_heartbeat"``
             — Phoenix renamed it in v1.7+. Sending ``phx_heartbeat``
             returns ``status: error, reason: unmatched topic`` (the
             server doesn't have a handler for that event name).

        Server reply (also v2 array)::

            [null, "<same-ref>", "phoenix", "phx_reply",
             {"response": {}, "status": "ok"}]

        ``ref`` lets us match request→reply and compute RTT.

        Loop guarantees:
          * Sends one heartbeat every ``HEARTBEAT_TIME_INTERVAL`` seconds.
          * Each send is logged with its ``ref`` for matching against
            replies in ``BitprecoExchange._user_stream_event_listener``.
          * Stale watchdog: if no reply landed in
            ``PHX_HEARTBEAT_STALE_AFTER_SEC`` seconds, log a WARNING
            (one-shot per stale spell) so we know if the heartbeat fix
            isn't working.
          * Periodic summary every 5 minutes: sent / replied / RTT,
            so we can compare to ``flash`` arrival rate.
          * On any exception, log and exit cleanly — the next reconnect
            cycle will spawn a fresh task.
        """
        try:
            summary_interval = 300.0  # 5 min
            last_summary = time.time()
            last_summary_sent = self._phx_heartbeat_sent
            last_summary_replied = self._phx_heartbeat_replied

            while True:
                ref = str(self._phx_heartbeat_ref)
                self._phx_heartbeat_ref += 1
                send_ts = time.time()

                # Phoenix v2 array form. join_ref=None for transport-level
                # heartbeats. Event name is "heartbeat" (NOT "phx_heartbeat"
                # — Phoenix renamed in v1.7+).
                hb_request = encode_phoenix_v2(
                    join_ref=None,
                    ref=ref,
                    topic="phoenix",
                    event="heartbeat",
                    payload={},
                )
                try:
                    await ws.send(hb_request)
                except Exception as send_err:
                    # Socket likely dead — abort loop; reconnect cycle will
                    # spawn a fresh task.
                    self.logger().warning(
                        f"[phx_hb] send failed (ref={ref}, will exit "
                        f"loop and rely on reconnect): "
                        f"{type(send_err).__name__}: {send_err}"
                    )
                    return

                self._phx_heartbeat_inflight[ref] = send_ts
                self._phx_heartbeat_sent += 1
                self.logger().info(
                    f"[phx_hb_send] ref={ref} "
                    f"inflight={len(self._phx_heartbeat_inflight)} "
                    f"total_sent={self._phx_heartbeat_sent}"
                )

                # Sleep, then check staleness and possibly log summary.
                await asyncio.sleep(self.HEARTBEAT_TIME_INTERVAL)

                # Stale watchdog: if the last reply (if any) is older than
                # PHX_HEARTBEAT_STALE_AFTER_SEC, the server is mute. Fire
                # a one-shot WARN. Reset the flag when a reply lands.
                now_t = time.time()
                last_reply = self._phx_heartbeat_last_reply_ts
                stale = (
                    last_reply is not None
                    and (now_t - last_reply) > self.PHX_HEARTBEAT_STALE_AFTER_SEC
                ) or (
                    last_reply is None
                    and self._phx_heartbeat_sent >= 3
                    # Three full heartbeats sent with zero replies → suspicious
                )
                if stale and not self._phx_heartbeat_stale_logged:
                    self._phx_heartbeat_stale_logged = True
                    self.logger().warning(
                        f"[phx_hb_stale] no reply for "
                        f"{(now_t - (last_reply or send_ts)):.1f}s; "
                        f"Phoenix channel likely dropped server-side. "
                        f"sent={self._phx_heartbeat_sent} "
                        f"replied={self._phx_heartbeat_replied} "
                        f"inflight={len(self._phx_heartbeat_inflight)}"
                    )

                # Periodic summary (so we can see the heartbeat / flash
                # balance over time without grepping every send).
                if (now_t - last_summary) >= summary_interval:
                    new_sent = self._phx_heartbeat_sent - last_summary_sent
                    new_replied = self._phx_heartbeat_replied - last_summary_replied
                    self.logger().info(
                        f"[phx_hb_summary] window={summary_interval:.0f}s "
                        f"sent={new_sent} replied={new_replied} "
                        f"(totals: sent={self._phx_heartbeat_sent} "
                        f"replied={self._phx_heartbeat_replied})"
                    )
                    last_summary = now_t
                    last_summary_sent = self._phx_heartbeat_sent
                    last_summary_replied = self._phx_heartbeat_replied
        except asyncio.CancelledError:
            self.logger().info(
                f"[phx_hb] heartbeat loop cancelled "
                f"(sent={self._phx_heartbeat_sent} "
                f"replied={self._phx_heartbeat_replied})"
            )
            raise
        except Exception as e:
            self.logger().error(
                f"[phx_hb] heartbeat loop crashed: "
                f"{type(e).__name__}: {e}",
                exc_info=True,
            )
            # Don't re-raise — let next reconnect spawn fresh task.

    def record_phx_heartbeat_reply(self, ref: str) -> Optional[float]:
        """Called by ``BitprecoExchange._user_stream_event_listener`` when a
        ``phx_reply`` on topic ``"phoenix"`` arrives. Pops the matching
        in-flight entry, returns the RTT in milliseconds (or None if the
        ref isn't ours — e.g. a reply for a request from a previous
        connection cycle that arrived after reset).
        """
        send_ts = self._phx_heartbeat_inflight.pop(ref, None)
        if send_ts is None:
            return None
        rtt_ms = (time.time() - send_ts) * 1000
        self._phx_heartbeat_replied += 1
        self._phx_heartbeat_last_reply_ts = time.time()
        # Receiving a reply clears the stale flag — next stale spell can
        # fire its own one-shot warning.
        self._phx_heartbeat_stale_logged = False
        return rtt_ms

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
