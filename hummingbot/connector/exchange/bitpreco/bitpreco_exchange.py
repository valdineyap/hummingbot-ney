import asyncio
import copy
import datetime
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.exchange.bitpreco import bitpreco_constants as CONSTANTS, bitpreco_web_utils as web_utils
from hummingbot.connector.exchange.bitpreco.bitpreco_api_order_book_data_source import BitprecoAPIOrderBookDataSource
from hummingbot.connector.exchange.bitpreco.bitpreco_api_user_stream_data_source import BitprecoAPIUserStreamDataSource
from hummingbot.connector.exchange.bitpreco.bitpreco_auth import BitprecoAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("nan")


class BitprecoExchange(ExchangePyBase):
    # ----- Polling cadence (Task 2.3) -----
    # BitPreco's WS user-stream drops frequently (we measured ~46 disconnects
    # in 80min on a single session). When the framework decides WS is healthy
    # it polls every ``LONG_POLL_INTERVAL``; with the 120s default a fill that
    # the WS misses can sit unhedged for two minutes before the connector
    # notices. We tighten both intervals — the 100 req/s rate limit makes
    # this trivially affordable at our order volume.
    SHORT_POLL_INTERVAL = 3.0
    LONG_POLL_INTERVAL = 30.0
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
                 ):
        self.api_key = bitpreco_api_key
        self.secret_key = bitpreco_api_secret
        self._domain = domain
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
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
        data = {
            "cmd": CONSTANTS.CMD_BUY if trade_type is TradeType.BUY else CONSTANTS.CMD_SELL,
            "market": market,
            "limited": is_limited,
            "amount": amount_str,
            "price": price_str
        }
        # MARKET BUY: add the mandatory `volume` field per BitPreco docs.
        # `amount` and `price` above are ignored server-side for limited=False.
        if volume_str is not None:
            data["volume"] = volume_str

        transact_time = time.time()

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
        return (o_id, transact_time)

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
        GONE_CODES = {"ORDER_CANCELED", "ORDER_NOT_FOUND", "ORDER_ALREADY_CANCELED",
                      "ORDER_FILLED", "ORDER_ALREADY_FILLED",
                      "CANT_CANCEL_FILLED_ORDER"}
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
                    if submit_ts is not None:
                        e2e_ms = (time.time() - submit_ts) * 1000
                        self.logger().info(
                            f"[bp_timing] fill_detected order={order.client_order_id} "
                            f"side={order.trade_type.name} amount={trade_update.fill_base_amount} "
                            f"price={trade_update.fill_price} → "
                            f"submit_to_fill={e2e_ms:.0f}ms (REST poll path)"
                        )
        return trade_updates

    def _create_order_book_data_source(self) -> BitprecoAPIOrderBookDataSource:
        return BitprecoAPIOrderBookDataSource(trading_pairs=self._trading_pairs, connector=self)

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
        async for event_message in self._iter_user_event_queue():
            if event_message.get("event") == "flash":
                # Instrumentation: how long after our most recent place_order
                # did this WS flash arrive? Identifies whether the lag is on
                # BitPreco's WS push or on our own poll path below.
                flash_t = time.time()
                if self._place_order_submit_times:
                    most_recent = max(self._place_order_submit_times.values())
                    ws_lag_ms = (flash_t - most_recent) * 1000
                    self.logger().info(
                        f"[bp_timing] WS flash arrived {ws_lag_ms:.0f}ms after "
                        f"most recent place_order "
                        f"(pending_orders={len(self._place_order_submit_times)})"
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
        return BitprecoAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

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
