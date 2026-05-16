import asyncio
import copy
import datetime
import time
import zoneinfo
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS, bitpreco_web_utils as web_utils
from hummingbot.connector.exchange.bitpreco.bitpreco_api_order_book_data_source import BitprecoAPIOrderBookDataSource
from hummingbot.connector.exchange.bitpreco.bitpreco_api_user_stream_data_source import (
    BitprecoAPIUserStreamDataSource,
    normalize_phoenix_message,
)
from hummingbot.connector.exchange.bitpreco.bitpreco_auth import BitprecoAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("nan")


class BitprecoExchange(ExchangePyBase):
    # ----- Polling cadence -----
    # BitPreco's WS user-stream has been observed to be EFFECTIVELY DEAD
    # for fill detection: across 6h51m of distributed sessions (May 14-15
    # 2026) we received 2 ``flash`` events versus ~500 expected order
    # events (creates + cancels + fills). The Phoenix v2 protocol works
    # technically — heartbeats are acked with status:ok — but the
    # server-side broadcaster doesn't push trade-event notifications for
    # API users. Behaviour appears to be by design (push pipeline targets
    # browser sessions on market.bitypreco.com, not API key holders).
    #
    # Consequence: WS cannot be trusted as a "channel is healthy" hint
    # to relax polling cadence. We override ``_get_poll_interval`` to
    # ALWAYS use ``SHORT_POLL_INTERVAL`` regardless of WS state — the
    # framework's default logic backs off to LONG_POLL when WS messages
    # arrive recently, but for us "recent WS message" only means
    # heartbeat replies which carry zero useful information about fills.
    #
    # The WS connection itself is kept alive for:
    #   1. The reconnect catch-up REST poll (recovers any state changes
    #      missed during disconnect windows).
    #   2. The ~0.5% of flash events that do leak through.
    #   3. Future-proofing in case BitPreco enables push for API users.
    # But behaviourally the bot is REST-poll-driven, full stop.
    SHORT_POLL_INTERVAL = 3.0
    # LONG_POLL_INTERVAL is kept for compatibility with the parent class
    # but is effectively unused — see _get_poll_interval override below.
    LONG_POLL_INTERVAL = 5.0
    # Minimum gap between two ``_update_order_status`` calls. Below this the
    # connector skips the call and waits — a guard against frantic polling
    # when both the user-stream listener and the status loop fire together.
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 5.0

    web_utils = web_utils

    def __init__(self,
                 bitpreco_api_key: str,
                 bitpreco_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 bitpreco_redis_shadow_mode: bool = False,
                 bitpreco_data_backend: str = "legacy",
                 ):
        self.api_key = bitpreco_api_key
        self.secret_key = bitpreco_api_secret
        self._domain = domain
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        # Phase 2: which backend drives user-stream + orderbook.
        backend = (bitpreco_data_backend or "legacy").lower().strip()
        if backend not in {"legacy", "redis"}:
            raise ValueError(
                f"bitpreco_data_backend must be 'legacy' or 'redis', "
                f"got {bitpreco_data_backend!r}"
            )
        self._data_backend: str = backend
        # Lazy: built on first call in _create_*_data_source. Reused
        # by both user-stream and orderbook factories so they share
        # the same config snapshot.
        self._redis_factory = None  # type: ignore[var-annotated]
        # force_balance_refresh deduplication: a single in-flight task
        # serves N concurrent callers.
        self._balance_refresh_in_flight: Optional[asyncio.Task] = None
        # Phase 1A: Redis shadow observers run in parallel with the
        # legacy Phoenix/REST path, comparing what each backend sees
        # and emitting metrics. Disabled by default; only flipping the
        # connector config field on AND having BITPRECO_REDIS_* env
        # vars set will spawn them. See
        # docs/BITPRECO_REDIS_GAINS.md for the motivation.
        self._redis_shadow_enabled: bool = bool(bitpreco_redis_shadow_mode)
        self._redis_shadow_us_task: Optional[asyncio.Task] = None
        self._redis_shadow_ob_task: Optional[asyncio.Task] = None
        # Held on the instance so tests can inspect counters/state.
        self._redis_shadow_us_observer = None  # type: ignore[var-annotated]
        self._redis_shadow_ob_observer = None  # type: ignore[var-annotated]
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self._throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self._api_factory = web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self.authenticator)
        self._rest_assistant = None
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._trading_rules = {}
        self._trade_fees = {}
        self._status_polling_task = None
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._last_poll_timestamp = 0
        self._order_tracker: ClientOrderTracker = ClientOrderTracker(connector=self)
        # === Latency instrumentation (2026-05-11) ===
        # Maps client_order_id → time.time() at REST `place_order` submission,
        # used to compute submit-to-fill latency when a TradeUpdate arrives in
        # ``_all_trade_updates_for_order``. Cleared per-order on first fill.
        # Self-pruning: capped at 200 entries (FIFO drop) to keep memory bounded.
        self._place_order_submit_times: Dict[str, float] = {}
        # Last time `_update_balances` SUCCESSFULLY refreshed `_account_balances`.
        # Read by callers (e.g. controller's inventory audit) to gauge staleness
        # before trusting the cached balance for drift detection.
        # Initial 0.0 means "never refreshed yet"; treat as max-stale.
        self._last_balance_update_ts: float = 0.0

    # ------------------------------------------------------------------
    # Phase 1A: Redis shadow observer lifecycle
    # ------------------------------------------------------------------
    async def start_network(self):
        """Bring up the connector AND, if enabled, the Redis shadow
        observers in parallel.

        Shadow observers never push to the order tracker / message
        queues. They run in their own asyncio tasks and emit
        ``[redis_shadow]`` / ``[redis_shadow_ob]`` log lines.
        """
        await super().start_network()
        if not self._redis_shadow_enabled:
            return
        self._spawn_redis_shadow_observers()

    async def stop_network(self):
        # Tear down our shadow tasks BEFORE parent's stop_network so
        # that any in-flight Redis socket can drain cleanly without
        # the framework also racing to cancel them.
        await self._cancel_redis_shadow_observers()
        await super().stop_network()

    def _spawn_redis_shadow_observers(self) -> None:
        """Build the Redis connection factory and spawn both shadow
        observers. Any setup error (missing env vars, redis-py not
        installed) logs WARN and silently disables — never raises.

        Imports are local so a connector running in legacy mode
        doesn't pay the import cost or require redis-py at all.
        """
        try:
            from hummingbot.connector.exchange.bitpreco.bitpreco_redis_client import (
                RedisBackendConfig,
                RedisConfigError,
                RedisConnectionFactory,
            )
            from hummingbot.connector.exchange.bitpreco.bitpreco_redis_order_book_shadow import (
                BitprecoRedisOrderBookShadow,
            )
            from hummingbot.connector.exchange.bitpreco.bitpreco_redis_user_stream_shadow import (
                BitprecoRedisUserStreamShadow,
            )
        except Exception as exc:
            self.logger().warning(
                "[redis_shadow] could not import Redis backend modules "
                "(redis-py missing?): %s — shadow mode disabled", exc)
            return

        try:
            redis_cfg = RedisBackendConfig.from_env()
        except RedisConfigError as exc:
            self.logger().warning(
                "[redis_shadow] %s — shadow mode disabled", exc)
            return
        except Exception as exc:
            self.logger().warning(
                "[redis_shadow] unexpected error loading Redis config: %s "
                "— shadow mode disabled", exc)
            return

        factory = RedisConnectionFactory(redis_cfg)

        us_observer = BitprecoRedisUserStreamShadow(connector=self, factory=factory)
        self._redis_shadow_us_observer = us_observer
        self._redis_shadow_us_task = safe_ensure_future(us_observer.run())

        if self._trading_pairs:
            ob_observer = BitprecoRedisOrderBookShadow(
                connector=self,
                factory=factory,
                trading_pairs=self._trading_pairs,
            )
            self._redis_shadow_ob_observer = ob_observer
            self._redis_shadow_ob_task = safe_ensure_future(ob_observer.run())
            ob_pairs_msg = f"orderbook channels: {self._trading_pairs}"
        else:
            ob_pairs_msg = "orderbook shadow skipped (no trading_pairs)"

        self.logger().info(
            "[redis_shadow] enabled — host=%s:%s user_id=%s tls=%s "
            "update_channel=%s | %s",
            redis_cfg.host, redis_cfg.port, redis_cfg.user_id, redis_cfg.tls,
            redis_cfg.update_channel, ob_pairs_msg,
        )

    async def _cancel_redis_shadow_observers(self) -> None:
        for attr in ("_redis_shadow_us_task", "_redis_shadow_ob_task"):
            task = getattr(self, attr, None)
            if task is None or task.done():
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            setattr(self, attr, None)
        self._redis_shadow_us_observer = None
        self._redis_shadow_ob_observer = None

    # ------------------------------------------------------------------
    # Phase 2: Redis backend factory + REST fallback API
    # ------------------------------------------------------------------

    @property
    def data_backend(self) -> str:
        """``"legacy"`` (Phoenix+REST) or ``"redis"`` (pub/sub)."""
        return self._data_backend

    def _get_redis_factory(self):
        """Lazy build of the Redis connection factory. Cached so the
        user-stream and orderbook data sources share one config
        snapshot."""
        if self._redis_factory is not None:
            return self._redis_factory
        from hummingbot.connector.exchange.bitpreco.bitpreco_redis_client import (
            RedisBackendConfig,
            RedisConnectionFactory,
        )
        cfg = RedisBackendConfig.from_env()
        self._redis_factory = RedisConnectionFactory(cfg)
        return self._redis_factory

    async def force_balance_refresh(self) -> None:
        """Bypass any cache layer and fetch balance from REST now.

        Coalesced via a single in-flight task: if a refresh is
        already running, concurrent callers await the same task
        rather than spawn N redundant REST calls. Phase 2 design
        decision (see plan): order events Redis trigger this; balance
        snapshots in the Redis payload do NOT mutate authoritative
        balance — REST is authoritative.
        """
        existing = self._balance_refresh_in_flight
        if existing is not None and not existing.done():
            await existing
            return
        task = asyncio.create_task(
            self._update_balances(_trigger="force_balance_refresh"))
        self._balance_refresh_in_flight = task
        try:
            await task
        finally:
            self._balance_refresh_in_flight = None

    def _on_redis_reconnect_catchup(self):
        """Callback the Redis user-stream invokes after (re)connecting.

        Pub/sub has no replay — events published while we were
        disconnected are lost. We respond with two REST sweeps:

        1. ``_update_order_status()`` to re-check every in-flight order
        2. ``force_balance_refresh()`` for accurate balance

        Both are scheduled rather than awaited so the data source's
        consume loop isn't blocked.
        """
        loop = asyncio.get_event_loop()
        try:
            loop.create_task(self._update_order_status())
        except Exception:
            self.logger().exception("[redis_us] catch-up: _update_order_status failed to schedule")
        try:
            loop.create_task(self.force_balance_refresh())
        except Exception:
            self.logger().exception("[redis_us] catch-up: force_balance_refresh failed to schedule")

    def _replay_redis_orphans_if_any(self, exchange_order_id: str) -> None:
        """Best-effort orphan replay for a newly-mapped exchange_order_id.

        Only meaningful when ``data_backend=redis``; on legacy it's a
        no-op. Never raises — orphan-replay failure must not
        interfere with place_order. Uses ``getattr`` so test-style
        bare-instance constructions (``__new__`` without ``__init__``)
        keep working.
        """
        if getattr(self, "_data_backend", "legacy") != "redis":
            return
        tracker = getattr(self, "_user_stream_tracker", None)
        if tracker is None:
            return
        ds = getattr(tracker, "data_source", None)
        if ds is None or not hasattr(ds, "replay_orphans"):
            return
        try:
            drained = ds.replay_orphans(str(exchange_order_id))
        except Exception:
            self.logger().exception(
                "[redis_us] replay_orphans raised for xid=%s", exchange_order_id)
            return
        if drained:
            self.logger().info(
                "[redis_us] replayed %d orphan event(s) for xid=%s",
                len(drained), exchange_order_id,
            )

    def _lookup_tracked_by_xid(self, exchange_order_id: str):
        """Find an in-flight order by its exchange (BitPreco numeric)
        id. Returns the :class:`InFlightOrder` or ``None``.

        Used by the optimistic Redis path — the envelope carries the
        xid, but the order tracker is keyed by ``client_order_id``.
        """
        tracker = getattr(self, "_order_tracker", None)
        if tracker is None:
            return None
        try:
            for in_flight in tracker.active_orders.values():
                if in_flight.exchange_order_id == str(exchange_order_id):
                    return in_flight
        except Exception:
            return None
        return None

    async def _handle_redis_envelope(self, envelope) -> None:
        """Optimistic dispatch for Redis user-stream envelopes.

        PR 3b — the bot mutates order state directly from the Redis
        payload rather than triggering a full REST sweep on every
        event. Mapping per ``message_cod``:

        - ``BUY_ORDER_CREATED`` / ``SELL_ORDER_CREATED``: no-op.
          ``place_order``'s REST response already registered the
          order in the tracker (and our ``_replay_redis_orphans_if_any``
          drained any envelopes that raced ahead of it).

        - ``ORDER_CANCELED`` with ``exec_amount == 0``: emit
          ``OrderUpdate(new_state=CANCELED)`` directly into the
          order tracker. **No REST call.** This is the bulk of the
          win — overnight 841 of 845 terminals were pure cancels
          (Phase 1A measurement) — and the cancel-side latency drops
          from ~300 ms (REST round-trip) to <1 ms.

        - ``ORDER_FULLY_EXECUTED``: schedule ``_emit_synchronous_fill``,
          which fetches the trade list for **this single xid** (real
          ``trade_id`` + ``fee`` required by the framework) and
          emits ``OrderFilledEvent`` via the order tracker. Cheaper
          than the legacy ``_update_order_status`` sweep (one order
          instead of every in-flight). Also fires
          ``force_balance_refresh``.

        - ``ORDER_PARTIALLY_EXECUTED`` (rare with our 0.0002 size):
          reuse ``_cancel_partial_and_emit_final``, which cancels
          the remainder and emits one final ``TradeUpdate`` with
          the actual ``exec_amount``. Also fires
          ``force_balance_refresh``.

        - ``ORDER_CANCELED`` with ``exec_amount > 0`` (partial fill
          then cancel): same as PARTIAL — go through
          ``_cancel_partial_and_emit_final`` so we capture the
          partial fill before transitioning to CANCELED.

        Fallback paths
        --------------
        If the connector isn't tracking the xid (rare race: orphan
        replay didn't fire, or the order belongs to another session),
        we still schedule a one-shot REST poll so we don't lose the
        event silently. Idempotency is provided by the order tracker
        — ``process_order_update`` is a no-op when the incoming state
        matches the current state.

        Safety
        ------
        REST poll continues on its own cadence (``SHORT_POLL_INTERVAL
        = 3 s``) as defense-in-depth. If anything in this optimistic
        path goes wrong, REST converges within 3 s anyway.
        """
        from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import (
            EventType,
        )
        et = envelope.event_type
        order = envelope.order
        if order is None:
            return
        xid = order.exchange_order_id

        # CREATED is a pure no-op — the place_order REST response
        # already registered the order in the tracker.
        if et in (EventType.BUY_ORDER_CREATED, EventType.SELL_ORDER_CREATED):
            return

        tracked = self._lookup_tracked_by_xid(xid)
        if tracked is None:
            # No in-flight order matches. Could be an old order from
            # a previous bot session, or an orphan that wasn't
            # replayed yet (very unlikely given the
            # _replay_redis_orphans_if_any hook). Fall back to a
            # REST sweep so we don't silently lose state.
            self.logger().info(
                "[redis_us] handle xid=%s type=%s tracked=None — "
                "scheduling REST sweep", xid, et.value,
            )
            try:
                asyncio.create_task(self._update_order_status())
            except Exception:
                self.logger().exception(
                    "[redis_us] xid=%s: _update_order_status failed to schedule",
                    xid)
            return

        cid = tracked.client_order_id
        exec_amount = order.exec_amount

        # ORDER_CANCELED with no fills — the optimisation. Build the
        # OrderUpdate locally and feed the tracker. Zero REST calls.
        if (et == EventType.ORDER_CANCELED
                and (exec_amount is None or exec_amount == 0)):
            update = OrderUpdate(
                client_order_id=cid,
                exchange_order_id=str(xid),
                trading_pair=tracked.trading_pair,
                update_timestamp=time.time(),
                new_state=OrderState.CANCELED,
            )
            try:
                self._order_tracker.process_order_update(update)
            except Exception:
                self.logger().exception(
                    "[redis_us] xid=%s: process_order_update raised", xid)
            self.logger().info(
                "[redis_us] handle xid=%s type=%s path=optimistic_cancel",
                xid, et.value,
            )
            return

        # Fill paths — schedule the existing emission helpers. These
        # do a single-order REST trade-fetch (needed for real
        # trade_id and fee) plus emit the OrderFilledEvent.
        if et == EventType.ORDER_FULLY_EXECUTED:
            try:
                asyncio.create_task(
                    self._emit_synchronous_fill(
                        str(xid), cid, "ORDER_FULLY_EXECUTED"))
            except Exception:
                self.logger().exception(
                    "[redis_us] xid=%s: _emit_synchronous_fill failed to schedule",
                    xid)
            try:
                asyncio.create_task(self.force_balance_refresh())
            except Exception:
                self.logger().exception(
                    "[redis_us] xid=%s: force_balance_refresh failed to schedule",
                    xid)
            self.logger().info(
                "[redis_us] handle xid=%s type=%s path=sync_fill exec=%s",
                xid, et.value, exec_amount,
            )
            return

        # PARTIAL (rare) or CANCELED-with-fills: cancel-and-emit-final
        # captures the partial exec_amount before terminal CANCELED.
        try:
            asyncio.create_task(
                self._cancel_partial_and_emit_final(str(xid), cid))
        except Exception:
            self.logger().exception(
                "[redis_us] xid=%s: _cancel_partial_and_emit_final failed",
                xid)
        try:
            asyncio.create_task(self.force_balance_refresh())
        except Exception:
            self.logger().exception(
                "[redis_us] xid=%s: force_balance_refresh failed to schedule", xid)
        self.logger().info(
            "[redis_us] handle xid=%s type=%s path=partial_and_emit exec=%s",
            xid, et.value, exec_amount,
        )

    @property
    def name(self) -> str:
        return "bitpreco"

    @property
    def authenticator(self):
        return BitprecoAuth(
            api_key=self.api_key,
            secret_key=self.secret_key)

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.REST_URL

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.REST_URL

    def supported_order_types(self):
        # NOTE: BitPreco's REST API has no native post-only flag, so LIMIT_MAKER
        # is mapped internally to a regular LIMIT order (see _place_order). We
        # advertise LIMIT_MAKER support for compatibility with strategies that
        # require it (e.g., XEMMLeadLagExecutor); callers must accept that an
        # order priced at/across the book may fill as a taker. This is a safe
        # trade-off given BitPreco currently charges zero fees on both sides.
        return [OrderType.MARKET, OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _try_recover_placement(
        self,
        market: str,
        trade_type: TradeType,
        amount_str: str,
        price_str: str,
        since_ts: float,
    ) -> Optional[str]:
        """
        Defensive recovery for ambiguous placement failures.

        When `_place_order` is about to raise (network error, malformed
        response, missing order_id), the order MAY still have been accepted
        and queued by BitPreco — leaving an unhedged order on the book is the
        worst outcome (no taker leg, guaranteed slippage at unwind). Wait
        briefly for the matching engine to settle, then query `open_orders`
        and look for an order matching (market, side, price, amount, recent
        timestamp). If found, return its exchange_order_id so the framework
        tracks it instead of treating it as failed.

        Returns None if no matching order is on the exchange — in that case
        the caller propagates the original failure as-is.
        """
        # Give BitPreco's matching engine a moment to internalize the placement.
        await asyncio.sleep(0.7)

        expected_side = "buy" if trade_type is TradeType.BUY else "sell"
        body = {"cmd": "open_orders", "market": market}
        try:
            response = await self._api_request(
                method=RESTMethod.POST,
                path_url=CONSTANTS.REST_URL,
                data=self._add_auth_token_to_req_body(body),
            )
        except Exception as e:
            self.logger().warning(
                f"BitPreco placement recovery: open_orders fetch failed: {e}"
            )
            return None

        # BitPreco returns either a list, {"orders": [...]}, or a failure dict.
        if isinstance(response, list):
            orders = response
        elif isinstance(response, dict):
            inner = response.get("orders")
            if isinstance(inner, list):
                orders = inner
            else:
                self.logger().warning(
                    f"BitPreco placement recovery: unexpected response shape: {response}"
                )
                return None
        else:
            return None

        try:
            target_price = Decimal(price_str)
            target_amount = Decimal(amount_str)
        except Exception:
            return None

        # Match: same side, price within 0.5 BRL (tick is 1 BRL for BTC-BRL),
        # amount within 1e-7 BTC, time_stamp not older than `since_ts - 5s`.
        candidates: List[Tuple[str, float]] = []
        for o in orders:
            if not isinstance(o, dict):
                continue
            oid = o.get("id")
            if oid is None:
                continue
            if str(o.get("side", "")).lower() != expected_side:
                continue
            try:
                o_price = Decimal(str(o.get("price", "0")))
                o_amount = Decimal(str(o.get("amount", "0")))
            except Exception:
                continue
            if abs(o_price - target_price) > Decimal("0.5"):
                continue
            if abs(o_amount - target_amount) > Decimal("0.0000001"):
                continue
            ts_str = o.get("time_stamp")
            try:
                o_ts = datetime.datetime.strptime(
                    str(ts_str), "%Y-%m-%d %H:%M:%S"
                ).timestamp()
            except (TypeError, ValueError):
                o_ts = 0.0
            if o_ts and o_ts < since_ts - 5:
                continue
            candidates.append((str(oid), o_ts))

        if not candidates:
            return None
        # Most recent matching candidate.
        candidates.sort(key=lambda x: x[1], reverse=True)
        recovered_id = candidates[0][0]
        self.logger().warning(
            f"BitPreco placement RECOVERED orphan in-flight: "
            f"exchange_order_id={recovered_id} side={expected_side} "
            f"price={price_str} amount={amount_str} — adopting as tracked order "
            f"instead of leaving it orphan on the book."
        )
        return recovered_id

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        amount_str = f"{amount:f}"
        market = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        # BitPreco MARKET orders require a `volume` (BRL) field per the API
        # docs (https://apidocs.bitpreco.com/, "limited" section):
        #   > When limited is set to false, the price and amount are not
        #   > considered, therefore, ignored. ... Turns volume mandatory.
        #
        # Prior attempts (all wrong):
        #   - 2026-05-11 01:10: synthesized price = best_ask * 1.01, kept
        #     amount as BTC. Hypothesis: BitPreco computes volume = amount*price.
        #     Hypothesis was wrong; price is ignored per docs.
        #   - 2026-05-11 (commit 29a2a186e): converted `amount` to BRL,
        #     price=0. Hypothesis: BitPreco's `amount` field is BRL for
        #     MARKET BUY. Also wrong; `amount` is ignored, the field name
        #     is `volume`. Server response confirmed: `requested_volume: 0`
        #     because BitPreco never saw a `volume` field at all.
        #
        # Correct (per docs): for MARKET BUY add `volume=<BRL>` to the
        # payload. Convert from the framework's BTC `amount` using
        # best_ask * 1.01 (1% buffer for book walk) so the resulting BTC
        # fill is >= what we wanted. BitPreco refunds unused BRL.
        # For MARKET SELL: history shows `amount` (BTC) without `volume`
        # works in practice (many successful prod fills). Keep that path
        # unchanged to avoid regressing.
        volume_str: Optional[str] = None
        if order_type is OrderType.MARKET and trade_type is TradeType.BUY:
            try:
                ceiling_price = self.get_price(trading_pair, True)  # best_ask
                if ceiling_price is None or ceiling_price <= 0 or ceiling_price.is_nan():
                    raise ValueError(f"get_price returned {ceiling_price!r}")
                ceiling_price = ceiling_price * Decimal("1.01")
                volume_brl = (amount * ceiling_price).quantize(
                    Decimal("0.01"), rounding="ROUND_UP"
                )
                volume_str = f"{volume_brl:f}"
                self.logger().info(
                    f"[market_buy_volume] {amount:f} BTC → "
                    f"volume={volume_str} BRL (ceiling_price={ceiling_price:f}) "
                    f"for MARKET BUY on BitPreco."
                )
            except Exception as e:
                self.logger().error(
                    f"[market_buy_volume] failed to compute BRL volume for "
                    f"MARKET BUY {amount} {market}: {type(e).__name__}: {e}. "
                    f"BitPreco will likely reject as BELOW_MINIMUM_VOLUME."
                )

        # BitPreco rejects fractional prices for BTC-BRL. We must send integer
        # BRL prices, with SIDE-AWARE rounding to keep LIMIT_MAKER intent:
        #   LIMIT BUY  → floor (stays below the ask, won't cross)
        #   LIMIT SELL → ceil  (stays above the bid, won't cross)
        # Without this, BitPreco would silently round (typically up), which
        # could flip a LIMIT_MAKER BUY into a taker fill and drain inventory.
        # MARKET orders: price is ignored per docs but we still send a
        # quantized value to keep the payload well-formed.
        if market.upper() in ("BTC-BRL", "BTCBRL"):
            if trade_type is TradeType.BUY:
                price_to_send = price.quantize(Decimal("1"), rounding="ROUND_DOWN")
            else:
                price_to_send = price.quantize(Decimal("1"), rounding="ROUND_UP")
        else:
            price_to_send = price
        price_str = f"{price_to_send:f}"
        # BitPreco has no native LIMIT_MAKER (post-only) flag; treat LIMIT_MAKER
        # as a regular LIMIT order. See supported_order_types() for the rationale.
        is_limited = order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER)
        transact_time = time.time()
        data = {
            "cmd": CONSTANTS.CMD_BUY if trade_type is TradeType.BUY else CONSTANTS.CMD_SELL,
            "market": market,
            "limited": is_limited,
            "amount": amount_str,
            "price": price_str,
            # Nanosecond Unix timestamp — defeats BitPreco's server-side
            # request deduplication. Observed in prod 2026-05-12 (4 incidents):
            # placing the same (side, price, amount) within ~3-5s of a cancel
            # made BitPreco return the *previous* exchange_order_id without
            # creating a real new order — the subsequent cancel of that
            # phantom returned INVALID_ORDER_ID. With a unique timestamp per
            # request the API sees each call as distinct. ``time_ns`` is used
            # (not ms) so two back-to-back placements in the same wall-clock
            # millisecond still get distinct values. The field is ignored by
            # BitPreco's matching logic (unknown-field tolerance).
            "timestamp": time.time_ns(),
        }
        # MARKET BUY: add the mandatory `volume` field per BitPreco docs.
        # `amount` and `price` above are ignored server-side for limited=False.
        if volume_str is not None:
            data["volume"] = volume_str

        # Wrap the request itself: on network/parse exceptions the order may
        # have been accepted by BitPreco anyway. Try to recover before
        # propagating, otherwise we leave an unhedged orphan on the book.
        # === Latency instrumentation ===
        # Time just the REST round-trip; combined with the fill-side log in
        # _all_trade_updates_for_order, this separates BitPreco server-side
        # latency from our own fill-detection latency.
        api_start = time.time()
        try:
            response = await self._api_request(
                method=RESTMethod.POST,
                path_url=CONSTANTS.REST_URL,
                data=self._add_auth_token_to_req_body(data),
            )
        except Exception as net_err:
            api_elapsed_ms = (time.time() - api_start) * 1000
            self.logger().warning(
                f"[bp_timing] place_order REST raised after {api_elapsed_ms:.0f}ms "
                f"cmd={data['cmd']} limited={is_limited} "
                f"({type(net_err).__name__}: {net_err}) — attempting recovery."
            )
            recovered = await self._try_recover_placement(
                market, trade_type, amount_str, price_str, transact_time)
            if recovered is not None:
                return (recovered, transact_time)
            raise
        api_elapsed_ms = (time.time() - api_start) * 1000
        self.logger().info(
            f"[bp_timing] place_order REST cmd={data['cmd']} "
            f"limited={is_limited} amount={amount_str} → "
            f"took {api_elapsed_ms:.0f}ms "
            f"response_cod={response.get('message_cod') if isinstance(response, dict) else 'non-dict'}"
        )

        # Defensive parsing — BitPreco returns different shapes for success/failure
        # and we were silently raising KeyError, leaving orphan orders on the
        # exchange whenever the response was unexpected.
        if not isinstance(response, dict):
            self.logger().error(
                f"BitPreco place_order returned non-dict response (orphan risk): "
                f"type={type(response).__name__} value={response!r}"
            )
            recovered = await self._try_recover_placement(
                market, trade_type, amount_str, price_str, transact_time)
            if recovered is not None:
                return (recovered, transact_time)
            raise IOError(f"BitPreco place_order: non-dict response: {response!r}")

        if response.get("success") is False:
            # Explicit rejection — order was NOT placed. No recovery needed:
            # BitPreco's `success: false` is a guaranteed-not-on-book signal
            # (rate limit, balance, market closed, etc.).
            msg = response.get("message") or response.get("message_cod") or "<no message>"
            self.logger().warning(
                f"BitPreco rejected {data['cmd']} order (amount={amount_str} price={price_str}): "
                f"{msg} | response={response}"
            )

            self._reconcile_balance_on_rejection(msg, response)
            raise IOError(f"BitPreco rejected order: {msg}")

        # On success the canonical key is 'order_id' but be tolerant of 'id'.
        # If neither is present we have no way to track the order — but the
        # order may have been placed anyway, so try to recover before raising.
        oid_value = response.get("order_id") or response.get("id")
        if oid_value is None:
            self.logger().error(
                f"BitPreco place_order: response missing order_id/id "
                f"(attempting recovery) — full response: {response}"
            )
            recovered = await self._try_recover_placement(
                market, trade_type, amount_str, price_str, transact_time)
            if recovered is not None:
                return (recovered, transact_time)
            raise IOError(f"BitPreco place_order: no order id in response: {response}")

        o_id = str(oid_value)
        # Record submit time for fill-latency instrumentation. Read by
        # _all_trade_updates_for_order when this order's fill is detected.
        # Keep the dict bounded — drop the oldest entry when it grows past
        # 200 (FIFO via insertion order on Python 3.7+ dicts).
        # Defensive lazy-init: some unit tests bypass __init__ via
        # ``BitprecoExchange.__new__``, so the attribute may not exist yet.
        if not hasattr(self, "_place_order_submit_times"):
            self._place_order_submit_times = {}
        self._place_order_submit_times[order_id] = transact_time
        if len(self._place_order_submit_times) > 200:
            oldest_key = next(iter(self._place_order_submit_times))
            self._place_order_submit_times.pop(oldest_key, None)

        # Two synchronous-fill paths after place_order REST response:
        #
        #   ORDER_FULLY_EXECUTED  → emit fill immediately. Terminal state
        #                           on BitPreco's side; no more fills will
        #                           land. Single TradeUpdate suffices.
        #
        #   ORDER_PARTIALLY_EXECUTED → DO NOT emit the partial yet. Trigger
        #                              an immediate cancel so further fills
        #                              cannot land at our aggressive limit,
        #                              then let the cancel-side
        #                              late_fill_recovery emit ONE
        #                              TradeUpdate with the FINAL
        #                              exec_amount (which may be larger
        #                              than what the placement response
        #                              showed — BitPreco can fill more
        #                              between placement and cancel
        #                              arrival). Single emission avoids
        #                              the framework's dedup-by-trade_id
        #                              dropping later fills.
        #
        # Both paths are scheduled as tasks so place_order returns
        # immediately — the framework needs to register the order in
        # in_flight_orders before the recovery task runs.
        msg_cod = response.get("message_cod") if isinstance(response, dict) else None
        if msg_cod == "ORDER_FULLY_EXECUTED":
            asyncio.create_task(
                self._emit_synchronous_fill(o_id, order_id, msg_cod)
            )
        elif msg_cod == "ORDER_PARTIALLY_EXECUTED":
            asyncio.create_task(
                self._cancel_partial_and_emit_final(o_id, order_id)
            )
        # Phase 2: drain any Redis events that arrived for this xid
        # while place_order was still in flight (race between the
        # REST response and the Redis publish). No-op on legacy.
        self._replay_redis_orphans_if_any(o_id)
        return (o_id, transact_time)

    async def _emit_synchronous_fill(
        self, exchange_order_id: str, client_order_id: str, msg_cod: str
    ) -> None:
        """Fetch trade(s) for an order whose placement response signalled
        a FULL synchronous fill (``ORDER_FULLY_EXECUTED``), and feed them
        through the order tracker so the framework emits
        ``OrderFilledEvent`` without waiting for the next status poll.

        Only called for the terminal-state case. ``ORDER_PARTIALLY_EXECUTED``
        takes the cancel-first path (``_cancel_partial_and_emit_final``)
        to avoid the dedup-by-trade_id problem when later fills land.

        Robust against the tracker-registration race: ``_place_order``
        returns the oid, and the framework needs a moment to register it
        in ``in_flight_orders``. Retry briefly before giving up.
        """
        tracked_order = None
        for _ in range(6):  # up to ~600ms total
            tracker = getattr(self, "_order_tracker", None)
            if tracker is not None:
                tracked_order = tracker.fetch_order(
                    client_order_id=client_order_id
                )
                if tracked_order is not None:
                    break
            await asyncio.sleep(0.1)

        if tracked_order is None:
            self.logger().warning(
                f"[sync_fill_recovery] tracker never registered "
                f"client_order_id={client_order_id} (exchange_order_id="
                f"{exchange_order_id}) within 600ms — periodic poll is "
                f"the last resort for the fill emission"
            )
            return
        if tracked_order.exchange_order_id is None:
            tracked_order.update_exchange_order_id(exchange_order_id)

        await self._emit_fills_with_retry(
            tracked_order=tracked_order,
            context=f"sync_fill_recovery msg_cod={msg_cod}",
        )

    async def _emit_fills_with_retry(
        self,
        tracked_order: InFlightOrder,
        context: str,
        max_attempts: int = 3,
        backoff_sec: float = 0.2,
    ) -> int:
        """Fetch trades via ``_all_trade_updates_for_order`` and push them
        through the order tracker, retrying on empty response.

        Used both by:
          - The cancel-side ``late_fill_recovery`` (when cancel response
            indicates a fill happened that the tracker doesn't know yet).
          - The placement-side ``sync_partial_cancel`` (when place_order
            returned ``ORDER_PARTIALLY_EXECUTED`` and we cancelled to
            settle the final state).

        The retry handles BitPreco's executed_orders indexing race: the
        cancel/placement response may confirm a fill before the
        ``executed_orders`` endpoint has indexed it. First fetch returns
        empty → backoff → retry. Typical resolution within 1-2 retries.

        Returns total TradeUpdate count emitted (0 means nothing was
        found across all attempts — periodic status poll is last resort).
        """
        tracker = getattr(self, "_order_tracker", None)
        if tracker is None:
            # Test fixture path (BitprecoExchange.__new__ bypass).
            self.logger().debug(
                f"[{context}] no _order_tracker available for "
                f"exchange_order_id={tracked_order.exchange_order_id}; "
                f"skipping emission"
            )
            return 0

        total = 0
        for attempt in range(1, max_attempts + 1):
            try:
                trade_updates = await self._all_trade_updates_for_order(
                    order=tracked_order
                )
            except Exception as e:
                self.logger().warning(
                    f"[{context}] _all_trade_updates_for_order raised on "
                    f"attempt {attempt}/{max_attempts}: "
                    f"{type(e).__name__}: {e}"
                )
                return total

            if trade_updates:
                for tu in trade_updates:
                    tracker.process_trade_update(tu)
                total += len(trade_updates)
                self.logger().info(
                    f"[{context}] emitted {len(trade_updates)} TradeUpdate(s) "
                    f"for exchange_order_id={tracked_order.exchange_order_id} "
                    f"(attempt {attempt}/{max_attempts})"
                )
                # Force fresh balance read — see _post_fill_balance_refresh
                # docstring for rationale (closes the 5s stale-cache window).
                self._post_fill_balance_refresh()
                return total

            # Empty response — BitPreco may not have indexed yet. Backoff.
            if attempt < max_attempts:
                await asyncio.sleep(backoff_sec)

        self.logger().warning(
            f"[{context}] no TradeUpdate emitted for "
            f"exchange_order_id={tracked_order.exchange_order_id} after "
            f"{max_attempts} attempts — periodic poll / orphan_check is "
            f"the last resort"
        )
        return total

    def _try_emit_fill_from_cancel_response(
        self,
        tracked_order: InFlightOrder,
        response: Any,
        exchange_order_id: str,
        code: Optional[str],
    ) -> bool:
        """Emit a single ``TradeUpdate`` synchronously from a cancel response.

        Since 2026-05-14 BitPreco includes the order block fields flat in
        the cancel response::

            {
              "id": "2062452569", "market": "BTC-BRL", "type": "SELL",
              "status": "EMPTY" | "PARTIAL" | "FILLED",
              "amount": <requested>, "price": <limit price>,
              "exec_amount": <aggregate base filled>,
              "cost": <aggregate quote spent/received>,
              "fee": <aggregate BRL fee>, "percent_fee": "<%>",
              "time_stamp": "YYYY-MM-DD HH:MM:SS",
              ..., "success": true, "message_cod": "ORDER_CANCELED"
            }

        ``exec_amount > 0`` means the order matched (partially or fully)
        before the cancel landed. BitPreco aggregates multi-fill partials
        server-side, so ``cost / exec_amount`` is the volume-weighted
        average fill price — sufficient for downstream hedge sizing and
        PnL accounting.

        The synthetic trade_id reuses ``client_order_id`` (matching the
        existing pattern in ``_all_trade_updates_for_order``). The framework's
        ``ClientOrderTracker`` dedups by ``(order, trade_id)``, so if a
        periodic poll later re-emits the same fill via ``executed_orders``
        it is silently skipped — no double-counting.

        Returns:
            True if the response was authoritative — either a fill was
            emitted (``exec_amount > 0``), or a clean cancel was confirmed
            (``exec_amount == 0``). In both cases the caller should skip
            the ``executed_orders`` REST fallback.
            False if the response lacked the expected fields (old BitPreco
            payload shape, or unparseable ``exec_amount``, or ``cost``
            missing on a partial). Caller may fall back to
            ``_emit_fills_with_retry`` for codes in ``GONE_BY_FILL_CODES``.
        """
        if not isinstance(response, dict):
            return False

        exec_amount_raw = response.get("exec_amount")
        if exec_amount_raw is None:
            return False

        try:
            exec_amount = Decimal(str(exec_amount_raw))
        except (InvalidOperation, ValueError, TypeError):
            self.logger().warning(
                f"[cancel_fill_sync] exchange_order_id={exchange_order_id} "
                f"unparseable exec_amount={exec_amount_raw!r} — falling back"
            )
            return False

        if exec_amount <= 0:
            # Clean cancel confirmed by response (status=EMPTY typical).
            # No fill to emit; caller proceeds to log + return True.
            return True

        cost_raw = response.get("cost", 0)
        fee_raw = response.get("fee", 0)
        status = response.get("status")

        try:
            cost = Decimal(str(cost_raw))
        except (InvalidOperation, ValueError, TypeError):
            cost = Decimal(0)
        try:
            fee_amount = Decimal(str(fee_raw))
        except (InvalidOperation, ValueError, TypeError):
            fee_amount = Decimal(0)

        if cost <= 0:
            # exec_amount > 0 but cost missing: cannot compute avg price.
            # Fall back to executed_orders path so caller can hit REST.
            self.logger().warning(
                f"[cancel_fill_sync] exchange_order_id={exchange_order_id} "
                f"exec_amount={exec_amount} but cost={cost_raw!r} — falling "
                f"back to executed_orders REST"
            )
            return False

        avg_fill_price = cost / exec_amount

        # Use connector clock for fill_timestamp. BitPreco's ``time_stamp``
        # is in BRT (America/Sao_Paulo) and the existing executed_orders
        # parser doesn't handle the offset cleanly. Since the cancel ack
        # we just received reflects a fill from seconds ago, ``current_timestamp``
        # is accurate within ~1s and avoids timezone parsing pitfalls.
        fill_ts = self.current_timestamp

        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=tracked_order.trade_type,
            percent_token="BRL",
            flat_fees=[TokenAmount(amount=fee_amount, token="BRL")],
        )

        trade_update = TradeUpdate(
            trade_id=tracked_order.client_order_id,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fee=fee,
            fill_base_amount=exec_amount,
            fill_quote_amount=cost,
            fill_price=avg_fill_price,
            fill_timestamp=fill_ts,
        )
        self._order_tracker.process_trade_update(trade_update)
        # Force fresh balance read — see _post_fill_balance_refresh
        # docstring for rationale (closes the 5s stale-cache window).
        self._post_fill_balance_refresh()

        self.logger().warning(
            f"[cancel_fill_sync] {tracked_order.client_order_id} emitted "
            f"directly from cancel response (code={code}): "
            f"exec_amount={exec_amount} cost={cost} avg_price={avg_fill_price} "
            f"fee={fee_amount} status={status}"
        )
        return True

    async def _cancel_partial_and_emit_final(
        self,
        exchange_order_id: str,
        client_order_id: str,
    ) -> None:
        """Triggered when ``_place_order`` returns ``ORDER_PARTIALLY_EXECUTED``.

        Cancel the order immediately so further fills cannot land, then let
        the cancel-side ``late_fill_recovery`` emit ONE TradeUpdate with
        the final ``exec_amount`` (which may be larger than the partial in
        the placement response — BitPreco can fill more between our
        placement response and our cancel arrival).

        Robust against two races:
          (a) Tracker registration lag — the framework registers the order
              in ``in_flight_orders`` AFTER ``_place_order`` returns, but
              before our scheduled task runs. Retry the lookup briefly.
          (b) BitPreco indexing lag — handled inside ``_place_cancel`` via
              ``_emit_fills_with_retry``.
        """
        # (a) Wait for the framework to register the order
        tracked_order = None
        for _ in range(6):  # up to ~600ms total
            tracker = getattr(self, "_order_tracker", None)
            if tracker is not None:
                tracked_order = tracker.fetch_order(client_order_id=client_order_id)
                if tracked_order is not None:
                    break
            await asyncio.sleep(0.1)

        if tracked_order is None:
            self.logger().warning(
                f"[sync_partial_cancel] tracker never registered "
                f"client_order_id={client_order_id} (exchange_order_id="
                f"{exchange_order_id}) within 600ms — periodic poll / "
                f"orphan_check is the last resort for any fill"
            )
            return

        if tracked_order.exchange_order_id is None:
            tracked_order.update_exchange_order_id(exchange_order_id)

        self.logger().info(
            f"[sync_partial_cancel] place_order returned "
            f"ORDER_PARTIALLY_EXECUTED for exchange_order_id="
            f"{exchange_order_id} — issuing immediate cancel to settle "
            f"final state (no more fills possible after cancel)"
        )
        try:
            await self._place_cancel(client_order_id, tracked_order)
            # ``_place_cancel`` internally calls ``_emit_fills_with_retry`` via
            # the GONE_CODES path — final exec_amount is now in the tracker.
        except Exception as e:
            self.logger().warning(
                f"[sync_partial_cancel] cancel raised for "
                f"{exchange_order_id}: {type(e).__name__}: {e} — "
                f"watchdog / periodic poll will recover"
            )

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        # Robust cancel — confirm the cancel actually happened, retry on
        # transient failures, and log every response so the cause is visible
        # when something goes wrong. Returning False here causes the framework
        # to retry; we only return True when the cancel is confirmed (or the
        # order is gone for any reason — already cancelled, already filled).
        exchange_order_id = tracked_order.exchange_order_id

        # Guard: if the order is still in PENDING_CREATE, exchange_order_id is
        # None. Sending None to BitPreco returns INVALID_ORDER_ID. Production
        # logs (May 2026) show the create REST response typically lands in
        # 500–700 ms after the cancel intent, so we briefly poll the tracked
        # order for the id rather than bouncing the cancel back to the
        # framework. This collapses the cancel-after-create race into a
        # single self-resolving call and avoids orphan windows where the
        # framework's outer retry cadence is slower than the create latency.
        if not exchange_order_id:
            # Up to ~1.5s of polling at 100ms — covers the BitPreco p99 create
            # latency we've measured. We deliberately do not block forever: if
            # the create truly failed, returning False keeps the strategy in a
            # known state.
            for _attempt in range(15):
                await asyncio.sleep(0.1)
                exchange_order_id = tracked_order.exchange_order_id
                if exchange_order_id:
                    break
            if not exchange_order_id:
                self.logger().warning(
                    f"_place_cancel: order_id={order_id} still has no exchange_order_id "
                    f"after 1.5s wait (PENDING_CREATE timed out) — returning False for "
                    f"framework retry."
                )
                return False
            self.logger().info(
                f"_place_cancel: order_id={order_id} resolved exchange_order_id="
                f"{exchange_order_id} after PENDING_CREATE wait — proceeding with cancel."
            )

        data = {
            "cmd": CONSTANTS.CMD_CANCEL_ORDER,
            "order_id": exchange_order_id,
        }

        # Responses BitPreco may return for cancels:
        #   {success: true,  message_cod: "ORDER_CANCELED"}      → confirmed cancelled
        #   {success: false, message_cod: "ORDER_NOT_FOUND"}     → already gone (treat as success)
        #   {success: false, message_cod: "RATE_LIMIT_EXCEEDED"} → retry
        #   {success: false, message_cod: "INVALID_ORDER_ID"}    → race: order just created,
        #                                                          BitPreco DB still indexing
        #   {success: false, message_cod: "INVALID_TOKEN"/...}   → don't retry, log error
        # Anything else with success=true that doesn't say ORDER_CANCELED is
        # ambiguous — log it loudly and treat as not-cancelled (framework will
        # retry on its next cancel cycle).
        #
        # INVALID_ORDER_ID handling — important subtlety:
        # We previously treated INVALID_ORDER_ID as "gone" (a cousin of
        # ORDER_NOT_FOUND). That assumption was wrong: production logs show
        # BitPreco can also return INVALID_ORDER_ID for a freshly-created
        # order whose ``exchange_order_id`` is known to us (we got it from
        # the create response) but whose lookup-by-id record is not yet
        # visible to the cancel endpoint. Treating that race as "gone" caused
        # the local tracker to mark the order CANCELED while it was still
        # alive on the exchange — exactly the orphan situation orphan_check
        # then had to clean up (~500ms-3s window of mismatch). Move it to
        # TRANSIENT so the connector's internal retry (3 × 0.5s backoff ≈ 3s)
        # gives BitPreco time to index the order before we declare it gone.
        # ``CANT_CANCEL_FILLED_ORDER`` is the exchange's explicit code for
        # "the order matched right before your cancel arrived". Semantically
        # identical to ORDER_FILLED for cancel-confirmation purposes — the
        # order is no longer cancelable because it's gone (filled). Treating
        # it as GONE avoids noisy ERROR logs and saves the framework a retry
        # cycle. The fill propagates separately through OrderFilledEvent.
        # BitPreco's actual response codes (verified against apidocs.bitpreco.com):
        # ``ORDER_ALREADY_FILLED`` does NOT exist in the API; removed 2026-05-13.
        # ``ORDER_FILLED`` kept as a defensive entry in case a future endpoint
        # variant returns it instead of ``CANT_CANCEL_FILLED_ORDER``.
        GONE_CODES = {"ORDER_CANCELED", "ORDER_NOT_FOUND", "ORDER_ALREADY_CANCELED",
                      "ORDER_FILLED",
                      "CANT_CANCEL_FILLED_ORDER"}
        # Subset of GONE_CODES that signal "the order is gone because it
        # FILLED" (not because it was actually cancelled). For these we need
        # to recover the fill data — otherwise the framework removes the
        # order from in_flight_orders on the next tick and the periodic
        # status poll never sees it.
        GONE_BY_FILL_CODES = {"CANT_CANCEL_FILLED_ORDER", "ORDER_FILLED"}
        TRANSIENT_CODES = {"RATE_LIMIT_EXCEEDED", "INVALID_ORDER_ID"}
        max_attempts = 3
        backoff_sec = 0.5
        last_response = None

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self._api_request(
                    method=RESTMethod.POST,
                    path_url=CONSTANTS.REST_URL,
                    data=self._add_auth_token_to_req_body(data),
                )
            except Exception as e:
                self.logger().warning(
                    f"BitPreco cancel attempt {attempt}/{max_attempts} for "
                    f"exchange_order_id={exchange_order_id} raised: {type(e).__name__}: {e}"
                )
                if attempt < max_attempts:
                    await asyncio.sleep(backoff_sec * attempt)
                    continue
                return False

            last_response = response
            code = response.get("message_cod") if isinstance(response, dict) else None

            if code in GONE_CODES:
                # Detect two failure modes that LOOK like clean cancel but
                # actually have an un-emitted fill behind them:
                #
                #   (1) ``CANT_CANCEL_FILLED_ORDER`` / ``ORDER_FILLED`` — the
                #       order fully filled before the cancel landed.
                #   (2) ``ORDER_CANCELED`` with ``exec_amount > 0`` in the
                #       response payload — partial fill before cancel landed.
                #
                # In both cases the framework removes the order from
                # ``in_flight_orders`` shortly after we return True, and the
                # periodic ``_all_trade_updates_for_order`` poll will never
                # find it again — the fill is invisible until ``orphan_check``
                # picks up the inventory mismatch (10+s later) and rebalance
                # corrects it via a MARKET on the same exchange (paying the
                # spread instead of cross-exchange hedging on Binance).
                #
                # FAST PATH (since 2026-05-14): BitPreco now includes the
                # order block fields (exec_amount, cost, fee, status,
                # time_stamp, price) flat in the cancel response. When
                # ``exec_amount > 0`` we can emit a single TradeUpdate
                # directly from this payload — BitPreco aggregates multi-
                # fill partials server-side, so ``cost / exec_amount`` is
                # the volume-weighted average fill price (sufficient for
                # hedge sizing and PnL). No round-trip to ``executed_orders``
                # needed, eliminating the ~3s gap previously observed
                # between cancel ack and periodic-poll fill detection
                # (the ``seq=6`` ghost-fill scenario, 2026-05-14 11:07Z).
                #
                # FALLBACK: for ``CANT_CANCEL_FILLED_ORDER`` / ``ORDER_FILLED``
                # where the response payload may not carry fill aggregates
                # (older BitPreco code path), fall back to the original
                # ``_emit_fills_with_retry`` which queries ``executed_orders``
                # with retry-and-backoff.
                emitted_from_response = self._try_emit_fill_from_cancel_response(
                    tracked_order=tracked_order,
                    response=response,
                    exchange_order_id=exchange_order_id,
                    code=code,
                )

                # Fall back to executed_orders REST when the synchronous emit
                # path couldn't act authoritatively AND we have reason to
                # suspect a fill exists:
                #   - code in GONE_BY_FILL_CODES (always implies a fill);
                #   - or ORDER_CANCELED with a non-zero ``exec_amount`` in the
                #     response that the helper couldn't process (e.g. partial
                #     reported without ``cost``, or old payload variants with
                #     legacy field names like ``filled`` / ``executed``).
                if not emitted_from_response:
                    has_fill_signal = code in GONE_BY_FILL_CODES
                    if not has_fill_signal and isinstance(response, dict):
                        for legacy_field in ("exec_amount", "executed_amount",
                                             "executed", "filled",
                                             "filled_amount", "matched_amount"):
                            raw = response.get(legacy_field)
                            if raw is None:
                                continue
                            try:
                                if Decimal(str(raw)) > 0:
                                    has_fill_signal = True
                                    break
                            except (InvalidOperation, ValueError, TypeError):
                                continue
                    if has_fill_signal:
                        await self._emit_fills_with_retry(
                            tracked_order=tracked_order,
                            context=f"late_fill_recovery_fallback code={code}",
                        )

                self.logger().info(
                    f"BitPreco cancel confirmed for exchange_order_id={exchange_order_id} "
                    f"(code={code}, attempt={attempt}): {response}"
                )
                return True

            if code in TRANSIENT_CODES and attempt < max_attempts:
                self.logger().warning(
                    f"BitPreco cancel transient failure for exchange_order_id={exchange_order_id} "
                    f"(code={code}, attempt={attempt}/{max_attempts}) — retrying after "
                    f"{backoff_sec * attempt}s: {response}"
                )
                await asyncio.sleep(backoff_sec * attempt)
                continue

            # Unrecognised response shape OR non-transient failure: don't keep
            # retrying inside this call — let the framework decide.
            self.logger().error(
                f"BitPreco cancel UNCONFIRMED for exchange_order_id={exchange_order_id} "
                f"(code={code}, attempt={attempt}/{max_attempts}) — returning False so the "
                f"framework retries. Full response: {response}"
            )
            return False

        # Exhausted retries without confirmation
        self.logger().error(
            f"BitPreco cancel FAILED for exchange_order_id={exchange_order_id} after "
            f"{max_attempts} attempts. Last response: {last_response}"
        )
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:

        rules = [
            {
                "symbol": "BTC-BRL",
                "min_order_size": 0.0001,
                # BitPreco rejects fractional prices for BTC-BRL ("Fractional
                # prices are not allowed by tradding API"). The framework's
                # default quantize is floor — fine for BUY but unsafe for SELL
                # (would cross the bid). _place_order applies SIDE-AWARE
                # integer rounding (floor for BUY, ceil for SELL); we keep the
                # framework quantum at 0.01 so we don't lose sub-integer
                # precision before that side-aware rounding step.
                "min_price_increment": Decimal("0.01"),
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "USDT-BRL",
                "min_order_size": 1,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "ETH-BRL",
                "min_order_size": 0.1,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "BUSD-BRL",
                "min_order_size": 1,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "USDC-BRL",
                "min_order_size": 1,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "BNB-BRL",
                "min_order_size": 0.1,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "ADA-BRL",
                "min_order_size": 0.1,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "UNI-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "PAXG-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "ABFY-BRL",
                "min_order_size": 10,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "SOL-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "GMT-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "POLIS-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "ATLAS-BRL",
                "min_order_size": 10,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "AXS-BRL",
                "min_order_size": 0.01,
                "min_price_increment": 0.00000001,
                "min_base_amount_increment": 0.00000001,
                "min_notional_size": 10
            },
            {
                "symbol": "SLP-BRL",
                "min_order_size": 10,
                "min_price_increment": 0.0001,
                "min_base_amount_increment": 0.0001,
                "min_notional_size": 10
            },
            {
                "symbol": "CRZO-BRL",
                "min_order_size": 0.5,
                "min_price_increment": 0.5,
                "min_base_amount_increment": 0.1,
                "min_notional_size": 10
            },
        ]

        retval = []
        for rule in rules:
            trading_pair = rule.get("symbol")
            min_order_size = rule.get("min_order_size")
            min_price_increment = rule.get("min_price_increment")
            min_base_amount_increment = rule.get("min_base_amount_increment")
            min_notional_size = rule.get("min_notional_size")
            retval.append(
                TradingRule(trading_pair,
                            min_order_size=min_order_size,
                            min_price_increment=Decimal(min_price_increment),
                            min_base_amount_increment=Decimal(min_base_amount_increment),
                            min_notional_size=Decimal(min_notional_size)))

        return retval

    async def _update_trading_rules(self):
        trading_rules_list = await self._format_trading_rules({})
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculates the estimated fee an order would pay based on the connector configuration
        :param base_currency: the order base currency
        :param quote_currency: the order quote currency
        :param order_type: the type of order (MARKET, LIMIT, LIMIT_MAKER)
        :param order_side: if the order is for buying or selling
        :param amount: the order amount
        :param price: the order price
        :return: the estimated fee for the order
        """

        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        trade_base_fee = build_trade_fee(
            exchange=self.name,
            is_maker=is_maker,
            order_side=order_side,
            order_type=order_type,
            amount=amount,
            price=price,
            base_currency=base_currency,
            quote_currency=quote_currency
        )
        return trade_base_fee

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            market = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            data = {
                "market": market,
                "cmd": "executed_orders"
            }
            response = await self._api_request(
                method=RESTMethod.POST,
                path_url=CONSTANTS.REST_URL,
                data=self._add_auth_token_to_req_body(data),
                limit_id=CONSTANTS.REST_URL)

            for executed_order in list(response):
                # BitPreco may return non-dict items (e.g. strings) when there
                # are no fills or the response shape is unexpected — skip them.
                if not isinstance(executed_order, dict):
                    continue
                if (order.exchange_order_id == executed_order.get("id")):
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token="BRL",
                        flat_fees=[TokenAmount(amount=Decimal(executed_order["fee"]), token="BRL")]
                    )
                    fill_quote_amount = Decimal(executed_order.get("exec_amount")) * Decimal(executed_order.get("price"))
                    timestamp = datetime.datetime.strptime(executed_order.get("time_stamp"),
                                                           "%Y-%m-%d %H:%M:%S").timestamp()
                    fill_timestamp = timestamp * 1e-3
                    trade_update = TradeUpdate(
                        trade_id=order.client_order_id,
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=market,
                        fee=fee,
                        fill_base_amount=Decimal(executed_order.get("exec_amount")),
                        fill_quote_amount=Decimal(fill_quote_amount),
                        fill_price=Decimal(executed_order.get("price")),
                        fill_timestamp=fill_timestamp,
                    )
                    trade_updates.append(trade_update)
                    # Submit-to-fill latency log. Distinguishes our fill-
                    # detection lag from BitPreco's server-side matching
                    # latency (the place_order REST log measures the latter).
                    submit_ts = self._place_order_submit_times.pop(
                        order.client_order_id, None
                    )
                    # Q2 instrumentation: separate "BitPreco-side fill time" from
                    # "our detection time".
                    #
                    # BitPreco returns ``time_stamp`` as a BRT (UTC-3) string
                    # — empirically verified 2026-05-13 when an earlier "fix"
                    # that assumed UTC still produced ~10.8M ms offsets
                    # (= 3h), proving BitPreco's clock-of-record is São Paulo
                    # local time, not UTC. Tag with the correct zone before
                    # converting to epoch so the diff to ``time.time()``
                    # (always UTC) is the real detection lag.
                    bp_fill_epoch = datetime.datetime.strptime(
                        executed_order.get("time_stamp"), "%Y-%m-%d %H:%M:%S"
                    ).replace(tzinfo=zoneinfo.ZoneInfo("America/Sao_Paulo")).timestamp()
                    bp_to_detection_ms = (time.time() - bp_fill_epoch) * 1000
                    if submit_ts is not None:
                        e2e_ms = (time.time() - submit_ts) * 1000
                        self.logger().info(
                            f"[bp_timing] fill_detected order={order.client_order_id} "
                            f"side={order.trade_type.name} amount={trade_update.fill_base_amount} "
                            f"price={trade_update.fill_price} → "
                            f"submit_to_fill={e2e_ms:.0f}ms "
                            f"bp_fill_to_detection={bp_to_detection_ms:.0f}ms "
                            f"(REST poll path)"
                        )
                    else:
                        # Submit time was already popped (e.g., late detection
                        # of a ghost cancel) — still log the BitPreco-side gap.
                        self.logger().info(
                            f"[bp_timing] fill_detected order={order.client_order_id} "
                            f"side={order.trade_type.name} amount={trade_update.fill_base_amount} "
                            f"price={trade_update.fill_price} → "
                            f"submit_to_fill=unknown "
                            f"bp_fill_to_detection={bp_to_detection_ms:.0f}ms "
                            f"(REST poll path, late)"
                        )
        return trade_updates

    def _create_order_book_data_source(self) -> BitprecoAPIOrderBookDataSource:
        # Phase 2 factory: Redis backend swaps the data source. Both
        # variants put snapshots on the same _message_queue slot, so
        # downstream code is identical.
        if self._data_backend == "redis":
            from hummingbot.connector.exchange.bitpreco.bitpreco_redis_order_book_data_source import (
                BitprecoRedisOrderBookDataSource,
            )
            return BitprecoRedisOrderBookDataSource(
                trading_pairs=self._trading_pairs,
                connector=self,
                factory=self._get_redis_factory(),
                api_factory=self._web_assistants_factory,
            )
        return BitprecoAPIOrderBookDataSource(trading_pairs=self._trading_pairs, connector=self)

    @staticmethod
    def _walk_levels_for_vwap(
        levels: List[Dict[str, Any]],
        amount: Decimal,
    ) -> Optional[Decimal]:
        """Walk one side of an order book snapshot and compute VWAP for
        ``amount`` of base asset. Returns ``None`` if the side is empty or
        depth is insufficient.

        Shared helper between ``fetch_fresh_vwap`` (one-sided) and
        ``fetch_fresh_vwaps`` (both sides from a single REST snapshot).
        """
        if not levels:
            return None
        remaining = amount
        quote_total = Decimal("0")
        for level in levels:
            try:
                price = Decimal(str(level["price"]))
                size = Decimal(str(level["amount"]))
            except (KeyError, TypeError, ValueError, InvalidOperation):
                continue
            take = min(size, remaining)
            quote_total += take * price
            remaining -= take
            if remaining <= 0:
                return quote_total / amount
        return None

    async def fetch_fresh_vwap(
        self,
        trading_pair: str,
        is_buy: bool,
        amount: Decimal,
    ) -> Optional[Decimal]:
        """Bypass the 500ms-polled OrderBook cache and compute VWAP directly
        from a fresh REST snapshot of /orderbook/<pair>.

        BitPreco's order-book "WS" is a REST poll loop with a 500ms sleep
        (see bitpreco_api_order_book_data_source.py: listen_for_subscriptions),
        so the cached OrderBook can be up to ~500ms stale plus the REST
        latency itself. For latency-sensitive checks (arbitrage spawn gate)
        this method forces a fresh snapshot at the cost of one extra REST
        call (~50-100ms).

        Returns ``None`` on REST failure, empty side, or insufficient depth.

        For computing BOTH buy and sell VWAPs, prefer ``fetch_fresh_vwaps``
        which uses a single REST snapshot (one /orderbook response carries
        both asks and bids).
        """
        try:
            data = await self._orderbook_ds._request_order_book_snapshot(trading_pair)
        except Exception as e:
            self.logger().warning(
                f"[fresh_vwap] REST snapshot failed for {trading_pair} "
                f"is_buy={is_buy}: {type(e).__name__}: {e}"
            )
            return None

        levels = data.get("asks" if is_buy else "bids") or []
        return self._walk_levels_for_vwap(levels, amount)

    async def fetch_fresh_vwaps(
        self,
        trading_pair: str,
        amount: Decimal,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Single-snapshot variant of ``fetch_fresh_vwap``: one REST call
        returns both buy and sell VWAPs.

        Same staleness guarantees as ``fetch_fresh_vwap`` but halves the REST
        traffic when the caller needs both sides (the typical arb-gate case).
        Returns ``(buy_vwap, sell_vwap)``; either entry is ``None`` on its
        side having insufficient depth.
        """
        try:
            data = await self._orderbook_ds._request_order_book_snapshot(trading_pair)
        except Exception as e:
            self.logger().warning(
                f"[fresh_vwap] REST snapshot failed for {trading_pair} "
                f"(both sides): {type(e).__name__}: {e}"
            )
            return (None, None)

        buy_vwap = self._walk_levels_for_vwap(data.get("asks") or [], amount)
        sell_vwap = self._walk_levels_for_vwap(data.get("bids") or [], amount)
        return (buy_vwap, sell_vwap)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        return DeductedFromReturnsTradeFee(percent=Decimal(0.0))

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: list):
        mapping = bidict()

        for pair in exchange_info:
            mapping[pair] = pair

        self._set_trading_pair_symbol_map(mapping)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    async def _make_network_check_request(self):
        """
        Override base: BitPreco's PING_PATH_URL is a PUBLIC ticker endpoint
        that responds 200 regardless of credentials. The base implementation
        (`_api_get(check_network_request_path)`) would therefore accept any
        api_key/secret during the network check, leading to the user only
        discovering bad credentials when the bot tries to trade.

        Use `cmd: "balance"` instead — an authenticated endpoint that, after
        the BitPreco-side fix (May 2026), correctly returns
        `{"success": false, "message_cod": "INVALID_TOKEN"}` for invalid
        credentials.
        """
        response = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=self._add_auth_token_to_req_body({"cmd": "balance"}),
            limit_id=CONSTANTS.REST_URL,
        )
        if isinstance(response, dict) and response.get("success") is not True:
            raise IOError(f"BitPreco auth/network check failed: {response}")

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # BitPreco does not document a stable "order not found" error code/message.
        # Returning False causes the framework to treat any status-update error as
        # a real failure (with retries), which is the safe conservative behaviour
        # — at the cost of slightly noisier logs when an order has just been
        # filled or cancelled. Tighten this if/when error semantics are confirmed.
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # Same rationale as above. _place_cancel already returns False on a
        # non-success response, so the framework rarely needs to introspect the
        # exception in practice.
        return False

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        # If the order doesn't yet have an exchange_order_id (still in
        # PENDING_CREATE — placement ack hasn't returned), there's nothing
        # to poll on the exchange. Return a no-op OrderUpdate that preserves
        # the current state so the framework's tracker doesn't crash.
        # Previously this branch fell through and returned None implicitly,
        # surfacing as `'NoneType' object has no attribute 'client_order_id'`
        # in client_order_tracker._process_order_update.
        if not tracked_order.exchange_order_id:
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=None,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=tracked_order.current_state,
            )

        data = {
            "cmd": "order_status",
            "order_id": tracked_order.exchange_order_id
        }

        response = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=self._add_auth_token_to_req_body(data)
        )

        order = response.get("order") if isinstance(response, dict) else None

        # If the exchange has no record of the order (e.g. it was manually
        # cancelled and is no longer in the book), treat it as CANCELLED so
        # the connector stops retrying and the executor can react accordingly.
        if order is None:
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=OrderState.CANCELED,
            )

        order_status = order.get("status") if isinstance(order, dict) else None
        update_timestamp = time.time()

        if order_status and order_status in CONSTANTS.ORDER_STATE:
            new_state = CONSTANTS.ORDER_STATE[order_status]
        else:
            # Unknown / missing status — keep current tracker state. Don't
            # default to OPEN, because that could resurrect an order the
            # tracker had already moved out of OPEN (e.g. CANCELED).
            self.logger().warning(
                f"BitPreco order_status response missing/unknown status for "
                f"exchange_order_id={tracked_order.exchange_order_id}: "
                f"order={order} — preserving tracker state {tracked_order.current_state.name}"
            )
            new_state = tracked_order.current_state

        # BitPreco signals a partial-fill-then-cancel as
        # ``status: "PARTIAL"`` + ``canceled: "1"``. Without this override the
        # order would remain in PARTIALLY_FILLED forever (non-terminal),
        # blocking the executor from completing its lifecycle. The partial
        # ``exec_amount`` is independently captured by
        # ``_all_trade_updates_for_order`` so this state change does not lose
        # fill information — it only ensures the tracker reaches CANCELED.
        canceled_flag = order.get("canceled") if isinstance(order, dict) else None
        if str(canceled_flag) == "1" and new_state in (
                OrderState.PARTIALLY_FILLED, OrderState.OPEN, OrderState.PENDING_CANCEL):
            self.logger().info(
                f"BitPreco order {tracked_order.exchange_order_id} reported "
                f"status={order_status} canceled=1; coercing tracker state to CANCELED."
            )
            new_state = OrderState.CANCELED

        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=new_state,
        )

    def _post_fill_balance_refresh(self) -> None:
        """Schedule a background balance refresh after a fill is emitted.

        Why: BitPreco's ``_account_available_balances`` cache is normally
        refreshed by a 5s periodic poll. A fill we just emitted reduces
        the underlying available balance (e.g. SELL fill reduces BTC
        available), but the cache won't reflect that for up to 5s. During
        that window any new placement attempt sees a stale-optimistic
        balance — exactly the 2026-05-14 12:03Z scenario that produced
        8 ``NOT_ENOUGH_USER_BALANCE`` rejections in 8 seconds.

        Debouncing strategy (two-flag): at most ONE refresh is in flight
        at any moment; at most ONE follow-up refresh is queued. Fills
        arriving DURING an in-flight REST may not be visible to the
        server-side balance read (depends on commit ordering); the queued
        trailing refresh guarantees we eventually pick them up.

        This is best-effort and non-blocking. The fill emission itself
        is unaffected by failures here.
        """
        if getattr(self, "_balance_refresh_inflight", False):
            # A refresh is already running. Queue at most one trailing
            # refresh so fills arriving during this window are picked up
            # on the next pass.
            self._balance_refresh_queued = True
            return
        self._balance_refresh_inflight = True
        self._balance_refresh_queued = False
        asyncio.create_task(self._do_post_fill_balance_refresh())

    async def _do_post_fill_balance_refresh(self) -> None:
        """Internal: runs the refresh REST and handles trailing-edge
        re-trigger when fills arrived during the in-flight window."""
        try:
            await self._update_all_balances(_trigger="post_fill")
        except Exception as e:
            self.logger().warning(
                f"[balance_refresh_post_fill] update failed (non-fatal, "
                f"periodic poll will catch up in ≤5s): "
                f"{type(e).__name__}: {e}"
            )
        finally:
            self._balance_refresh_inflight = False
            if getattr(self, "_balance_refresh_queued", False):
                self._balance_refresh_queued = False
                # Trailing refresh: a fill landed during the previous
                # refresh window and may not have been visible. Issue
                # another pass. Re-entry through the same helper keeps
                # the debounce state coherent.
                self._post_fill_balance_refresh()

    def _reconcile_balance_on_rejection(self, msg: Optional[str], response: dict) -> None:
        """Reconcile the local balance cache when an order is rejected.

        Only ``NOT_ENOUGH_USER_BALANCE`` triggers reconciliation — that's
        the rejection code that carries authoritative balance data from
        the exchange itself, e.g.::

            {"success": false, "message_cod": "NOT_ENOUGH_USER_BALANCE",
             "currency": "BTC", "requested": 0.0002, "max": 6.056e-05}

        Two-pronged update to break the retry-loop pattern observed at
        2026-05-14 12:03Z (8 rejections in 8 seconds because
        ``validate_sufficient_balance`` only runs ``on_start`` and the
        periodic 5s balance poll had an 11.4s stale window after a fill):

          (a) Update the local cache IMMEDIATELY with ``max`` (sync, zero
              round-trip) so the NEXT placement / revalidation sees the
              real value.
          (b) Schedule a full ``_update_all_balances`` REST refresh in
              background so any *other* stale assets (e.g. locked by
              other in-flight orders) are also brought current — the
              ``max`` field alone covers only the rejected currency.

        ``_account_balances`` (total) vs ``_account_available_balances``
        (free) — total can legitimately exceed available when funds are
        locked in other orders, so we only bump total UP to ``max`` when
        the cached total is *below* ``max`` (inconsistency repair); we
        never inflate total downward to ``max``.
        """
        if msg != "NOT_ENOUGH_USER_BALANCE":
            return

        currency = response.get("currency")
        max_raw = response.get("max")
        if currency and max_raw is not None:
            try:
                max_val = Decimal(str(max_raw))
                self._account_available_balances[currency] = max_val
                # Total must be >= available (locked funds may push it up).
                # If currency is unseen, bootstrap total = max_val.
                # If cached total is below max_val, repair the inconsistency.
                # Never inflate total downward — locked-elsewhere amounts
                # may legitimately justify a higher total than ``max``.
                if currency not in self._account_balances or \
                        self._account_balances[currency] < max_val:
                    self._account_balances[currency] = max_val
                self.logger().warning(
                    f"[balance_reconcile_sync] {currency} available cache "
                    f"updated from rejection: max={max_val} "
                    f"(was likely stale post-fill)"
                )
            except (InvalidOperation, ValueError, TypeError):
                self.logger().warning(
                    f"[balance_reconcile_sync] unparseable max={max_raw!r} "
                    f"for currency={currency} — full refresh only"
                )

        # Background full refresh — non-blocking. Best-effort: if it fails,
        # the local update above already broke the immediate retry loop.
        asyncio.create_task(
            self._update_all_balances(_trigger="balance_rejection")
        )

    async def _update_balances(self, _trigger: str = "unknown"):
        """Refresh account balances from BitPreco REST.

        ``_trigger`` is a free-form string identifying the caller; used in
        ``[bp_balance]`` log so we can see WHICH entry point is keeping the
        cache fresh (or not). Common triggers:
          - ``ws_flash``     : WS user-stream "flash" event listener
          - ``periodic``     : framework's ``_status_polling_loop``
          - ``post_place``   : forced after place_order (added 2026-05-12)
          - ``audit_force``  : forced by controller audit before reading
          - ``unknown``      : any other caller (default; investigation aid)
        """
        if not hasattr(self, "_last_balance_update_ts"):
            self._last_balance_update_ts = 0.0
        api_start = time.time()
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        data = {"cmd": "balance"}
        balances = await self._api_request(
            method=RESTMethod.POST,
            path_url=CONSTANTS.REST_URL,
            data=self._add_auth_token_to_req_body(data),
        )
        api_elapsed_ms = (time.time() - api_start) * 1000
        self.logger().debug(
            f"bitpreco _update_balances raw response: type={type(balances).__name__} "
            f"keys={list(balances.keys()) if isinstance(balances, dict) else 'n/a'}"
        )
        # Validate auth/shape. After the BitPreco-side fix (May 2026), an
        # invalid auth_token returns {"success": false, "message_cod":
        # "INVALID_TOKEN"} rather than the previous silent-zero response.
        if not isinstance(balances, dict) or balances.get("success") is not True:
            raise IOError(
                f"BitPreco balance fetch failed (likely invalid credentials): {balances}"
            )
        # The response is a flat dict mixing metadata (`success`, `timestamp`,
        # possibly `message_cod`, etc.) with balance entries (`BTC`, `BTC_locked`,
        # `BRL`, `BRL_locked`, ...). The original loop tried to Decimal()-cast
        # every key whose name didn't match the explicit skip-list, which
        # surfaced as `decimal.ConversionSyntax` when BitPreco added new
        # non-numeric metadata fields. Coerce defensively and log the raw
        # response on failure so the cause is visible.
        # We also only consider keys whose `<asset>_locked` counterpart exists
        # — that pairing is what marks the entry as a balance row.
        for item in balances:
            if item == "success" or item == "timestamp" or "_locked" in item:
                continue
            locked_key = f"{item}_locked"
            if locked_key not in balances:
                # Not a balance entry (e.g., 'message', 'message_cod', 'data').
                continue
            try:
                free_amount = Decimal(str(balances[item]))
                locked_amount = Decimal(str(balances[locked_key]))
            except Exception as e:
                self.logger().warning(
                    f"bitpreco _update_balances: skipping non-numeric entry "
                    f"{item!r}={balances[item]!r} (err={e}); raw response keys="
                    f"{list(balances.keys())}"
                )
                continue
            asset_name = item
            self._account_available_balances[asset_name] = free_amount
            self._account_balances[asset_name] = free_amount + locked_amount
            remote_asset_names.add(asset_name)
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

        # Stamp success and log so we can see the staleness from the audit side.
        prev_ts = self._last_balance_update_ts
        self._last_balance_update_ts = time.time()
        gap_s = (self._last_balance_update_ts - prev_ts) if prev_ts > 0 else None
        gap_str = f"gap={gap_s:.1f}s" if gap_s is not None else "first_refresh"
        btc = self._account_balances.get("BTC", "n/a")
        brl = self._account_balances.get("BRL", "n/a")
        self.logger().info(
            f"[bp_balance] trigger={_trigger} REST took {api_elapsed_ms:.0f}ms "
            f"BTC={btc} BRL={brl} {gap_str}"
        )

    async def _update_all_balances(self, _trigger: str = "periodic"):
        await self._update_balances(_trigger=_trigger)
        # if not self.real_time_balance_update:
        # This is only required for exchanges that do not provide balance update notifications through websocket
        self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self.in_flight_orders.items()}
        self._in_flight_orders_snapshot_timestamp = self.current_timestamp

    async def _user_stream_event_listener(self):
        async for raw_message in self._iter_user_event_queue():
            # ---- Phase 2: Redis path produces EventEnvelope ----
            # When data_backend=redis, the user-stream data source
            # pushes typed EventEnvelope objects to the queue. Route
            # them to the Redis-specific handler and skip the rest of
            # the Phoenix flow. Legacy code below stays exactly as it
            # was so flipping the flag is the ONLY thing that changes.
            from hummingbot.connector.exchange.bitpreco.bitpreco_event_envelope import (
                EventEnvelope,
            )
            if isinstance(raw_message, EventEnvelope):
                try:
                    await self._handle_redis_envelope(raw_message)
                except Exception:
                    self.logger().exception(
                        "[redis_us] envelope handler raised — continuing")
                continue
            # Normalize Phoenix v1 (object) ↔ v2 (array) wire formats into
            # a uniform dict. After the 2026-05-14 migration to ``?vsn=2.0.0``
            # the server sends arrays ``[join_ref, ref, topic, event, payload]``;
            # before that it sent objects. Helper tolerates both so a server-
            # side toggle wouldn't crash us.
            event_message = normalize_phoenix_message(raw_message)
            if event_message is None:
                self.logger().warning(
                    f"[ws_event_seen] dropping unparseable WS message: "
                    f"{str(raw_message)[:200]}"
                )
                continue
            # Q1 instrumentation: log every event TYPE received from WS
            # (not the payload — would flood). Currently only `flash` is acted
            # on; if BitPreco's WS emits fill-specific events we'd be missing
            # them silently. Throttled to 1 log per type per 60s, plus a
            # one-shot "first seen" log per unknown type.
            evt_type = event_message.get("event", "<no-event>")
            # ---- Phoenix heartbeat reply matcher (2026-05-14) ----
            # A phx_reply on topic="phoenix" is an ACK for one of our
            # phx_heartbeat sends. Match by ``ref`` to compute RTT and
            # detect when the heartbeat fix is actually working
            # (= server is still alive and pushing). Done BEFORE the
            # generic ws_event_seen log so the heartbeat noise stays
            # in its own [phx_hb_reply] log line.
            if (evt_type == "phx_reply"
                    and event_message.get("topic") == "phoenix"):
                ref = event_message.get("ref")
                ds = self._user_stream_tracker.data_source if hasattr(
                    self, "_user_stream_tracker"
                ) and self._user_stream_tracker is not None else None
                rtt_ms = None
                if ds is not None and hasattr(ds, "record_phx_heartbeat_reply"):
                    try:
                        rtt_ms = ds.record_phx_heartbeat_reply(ref)
                    except Exception as e:
                        self.logger().warning(
                            f"[phx_hb_reply] record_phx_heartbeat_reply "
                            f"raised: {type(e).__name__}: {e}"
                        )
                self.logger().info(
                    f"[phx_hb_reply] ref={ref} "
                    f"rtt_ms={f'{rtt_ms:.0f}' if rtt_ms is not None else 'unmatched'} "
                    f"payload={event_message.get('payload')}"
                )
                # Don't run the generic event-seen path for heartbeat
                # replies — they'd dominate the counts.
                continue

            if not hasattr(self, "_ws_event_seen_counts"):
                self._ws_event_seen_counts: Dict[str, int] = {}
                self._ws_event_last_log_ts: Dict[str, float] = {}
                self._ws_event_first_seen_logged: set = set()
            self._ws_event_seen_counts[evt_type] = (
                self._ws_event_seen_counts.get(evt_type, 0) + 1
            )
            now_t = time.time()
            # First-seen: log payload sample for unknown types (helps discover
            # new event shapes BitPreco might send).
            if evt_type not in self._ws_event_first_seen_logged and evt_type != "flash":
                self._ws_event_first_seen_logged.add(evt_type)
                self.logger().info(
                    f"[ws_event_seen] FIRST event={evt_type!r} "
                    f"sample={str(event_message)[:300]}"
                )
            # Periodic count log per type (60s window).
            last_log = self._ws_event_last_log_ts.get(evt_type, 0.0)
            if now_t - last_log >= 60.0:
                self._ws_event_last_log_ts[evt_type] = now_t
                count = self._ws_event_seen_counts[evt_type]
                self.logger().info(
                    f"[ws_event_seen] event={evt_type} count_in_window={count}"
                )
                self._ws_event_seen_counts[evt_type] = 0

            if event_message.get("event") == "flash":
                # Instrumentation: how long after our most recent place_order
                # did this WS flash arrive? Identifies whether the lag is on
                # BitPreco's WS push or on our own poll path below.
                flash_t = time.time()
                # Phoenix v2 flash payload (confirmed via web client capture):
                #   {"payload": <obj|null>, "persist": <bool>,
                #    "type": "<ORDER_FULLY_EXECUTED|ORDER_PARTIALLY_EXECUTED|"
                #            "ORDER_CANCELED|BUY_ORDER_CREATED|"
                #            "SELL_ORDER_CREATED|...>",
                #    "user_id": "<id>"}
                flash_payload = event_message.get("payload") or {}
                flash_type = (
                    flash_payload.get("type") if isinstance(flash_payload, dict)
                    else None
                )
                flash_user = (
                    flash_payload.get("user_id") if isinstance(flash_payload, dict)
                    else None
                )
                if self._place_order_submit_times:
                    most_recent = max(self._place_order_submit_times.values())
                    ws_lag_ms = (flash_t - most_recent) * 1000
                    self.logger().info(
                        f"[bp_timing] WS flash arrived {ws_lag_ms:.0f}ms after "
                        f"most recent place_order type={flash_type!r} "
                        f"user_id={flash_user!r} "
                        f"(pending_orders={len(self._place_order_submit_times)})"
                    )
                else:
                    self.logger().info(
                        f"[bp_timing] WS flash arrived (no recent order to "
                        f"correlate) type={flash_type!r} user_id={flash_user!r}"
                    )
                await self._update_all_balances(_trigger="ws_flash")
                # === FOLLOW-UP REQUIRED — see DEVELOPMENT_STATUS.md ===
                # Reduced from 5.0s → 0.5s on 2026-05-11. Original 5s was
                # undocumented (present since the connector's first commit
                # 461dc29e6). Hypothesized purposes (without ground truth):
                #   H1. REST eventual-consistency window after WS notify
                #   H2. Debouncing burst flashes to avoid REST rate-limit
                #   H3. Avoid races with periodic `_status_polling_task`
                #   H4. Empirical workaround the original author didn't note
                # Validation criteria to reduce FURTHER (100ms or remove):
                #   (a) ≥ 24h of [bp_timing] logs showing fill_detected
                #       arriving cleanly after the 500ms wait (no stale
                #       "still OPEN" reads, no missed fills picked up by
                #       inventory_audit instead).
                #   (b) No new 429 / rate-limit responses in REST logs.
                #   (c) Especially watch arb MARKET fills — those are the
                #       only ones where the wait actually hurts (LIMIT
                #       cycles tolerate 500ms fine).
                # When reducing further, halve in steps (500→250→100) and
                # re-validate (a)/(b)/(c) for ≥ 12h between steps.
                # Memory pointer: ~/.claude/projects/.../memory/project_bitpreco_flash_sleep.md
                await self._sleep(0.5)
                await self._update_order_status()

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        # Phase 2 factory. Redis variant pushes EventEnvelope to the
        # user-stream queue; the listener dispatches on
        # isinstance(msg, EventEnvelope) for the Redis path.
        if self._data_backend == "redis":
            from hummingbot.connector.exchange.bitpreco.bitpreco_redis_user_stream import (
                BitprecoRedisUserStream,
            )
            return BitprecoRedisUserStream(
                connector=self,
                factory=self._get_redis_factory(),
            )
        return BitprecoAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_poll_interval(self, timestamp: float) -> float:
        """Override base: ALWAYS use ``SHORT_POLL_INTERVAL``.

        The parent class' logic backs off from polling when WS messages
        arrived recently (``last_user_stream_message_time``), on the
        assumption that an active WS reduces the need for REST polls.
        That assumption is false for BitPreco — see the class-level
        polling-cadence comment block. The WS pushes heartbeat replies
        every 30s (which DO refresh ``last_recv_time``) but no actual
        trade events, so the framework would otherwise wrongly think
        "WS is healthy → relax polling".

        By always returning ``SHORT_POLL_INTERVAL`` we make the polling
        cadence depend ONLY on our configured value, not on WS state.
        REST poll becomes the deterministic, sole source of truth for
        fill / order-status detection.
        """
        return self.SHORT_POLL_INTERVAL

    async def _initialize_trading_pair_symbol_map(self):

        response = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.ALL_CURRENCY_TICKER_PATH_URL,
        )

        if not response["success"]:
            raise

        bitpreco_pairs = list(filter(lambda pair: pair != "success", response.keys()))

        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=bitpreco_pairs)

    def _add_auth_token_to_req_body(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data["auth_token"] = f'{self.secret_key}{self.api_key}'
        return data
