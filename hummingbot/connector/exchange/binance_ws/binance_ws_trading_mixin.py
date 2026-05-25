"""Reusable WS-trading overrides for the Binance Spot connector family.

Extracted into a mixin so a future ``BinanceSbeWsExchange`` (composing
SBE market data + WS trading) can be a one-liner:

    class BinanceSbeWsExchange(BinanceWsTradingMixin, BinanceSbeExchange):
        ...

Trading-path responsibilities consolidated here:

  - ``_place_order``: substitute REST ``POST /api/v3/order`` with WS
    ``order.place``. Handles UNKNOWN executions (timeout, 5xx, -1007,
    disconnect-while-sent) by returning ``("UNKNOWN", now)`` so the
    framework's REST polling reconciles state. Detects duplicate-
    clientOrderId responses (codes -1010 / -2010 / -2011 with message
    ``"Duplicate order sent"``) and reconciles via ``order.status``
    instead of re-submitting blindly.

  - ``_place_cancel``: substitute REST ``DELETE /api/v3/order`` with
    WS ``order.cancel``. Fast path by numeric ``orderId`` when
    ``tracked_order.exchange_order_id`` is known; ``origClientOrderId``
    only as fallback. NEVER both (Binance documents that as a slower
    path). On UNKNOWN execution, consults REST ``GET /api/v3/order``
    before deciding True/False — never returns False blindly.

  - ``_query_order_status_rest``: small helper using the inherited
    ``_api_get`` to fetch the canonical state of an order. Used to
    reconcile both UNKNOWN cancels and duplicate-clientOrderId places.

  - ``_ws_health_dict``: snapshot of router health for ``check_network``
    override and operator dashboards.

The mixin assumes the consumer class:

  - inherits ``BinanceExchange`` (or subclass) so ``_api_get``,
    ``exchange_symbol_associated_to_pair``, ``quantize_order_amount``,
    ``quantize_order_price``, ``_time_synchronizer`` are all available;
  - sets ``self._router: Optional[BinanceWsRequestRouter]`` and
    ``self._use_ws_trading: bool`` in ``__init__``.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from hummingbot.connector.exchange.binance import binance_constants as BINANCE_CONSTANTS
from hummingbot.connector.exchange.binance_ws import binance_ws_constants as CONSTANTS
from hummingbot.connector.exchange.binance_ws.binance_ws_request_router import (
    BinanceWsDisconnectedError,
    BinanceWsRequestError,
    BinanceWsRequestRouter,
    BinanceWsTimeoutError,
    BinanceWsUnknownExecutionError,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder


# Sentinel codes whose msg can carry "Duplicate order sent.":
# -1010 ERROR_MSG_RECEIVED, -2010 NEW_ORDER_REJECTED, -2011 CANCEL_REJECTED.
# Note: -2026 is ORDER_ARCHIVED, NOT duplicate (older docs/articles
# conflate the two).
_DUPLICATE_ORDER_CODES = (-1010, -2010, -2011)
_DUPLICATE_ORDER_PHRASE = "Duplicate order sent"

# Terminal order states reported by Binance — for cancel reconciliation.
_TERMINAL_STATUSES = {"FILLED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH"}


class BinanceWsUnknownCancelError(Exception):
    """Cancel could not be reconciled — neither WS nor REST gave a
    definitive answer. Caller (tracker/strategy) decides how to recover.
    """


class BinanceWsTradingMixin:
    # Type hints for the IDE; real values come from the concrete subclass.
    _router: Optional[BinanceWsRequestRouter]
    _use_ws_trading: bool

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _start_ws_trading(self) -> None:
        if self._router is not None and self._use_ws_trading:
            await self._router.start()

    async def _stop_ws_trading(self) -> None:
        if self._router is not None:
            await self._router.stop()

    # ------------------------------------------------------------------
    # Place / cancel overrides
    # ------------------------------------------------------------------

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        if not self._use_ws_trading or self._router is None:
            return await super()._place_order(
                order_id=order_id, trading_pair=trading_pair, amount=amount,
                trade_type=trade_type, order_type=order_type, price=price,
                **kwargs,
            )

        amount = self.quantize_order_amount(trading_pair, amount)
        price_q = self.quantize_order_price(trading_pair, price) if price is not None else None
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": BINANCE_CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else BINANCE_CONSTANTS.SIDE_SELL,
            "type": order_type.name.upper(),
            # Decimal must be a string — JSON can't serialise Decimal AND
            # the WS-API signature is computed over the raw string, so
            # any reformatting downstream would break the signature.
            "quantity": f"{amount:f}",
            "newClientOrderId": order_id,
            # ACK keeps the response slim. Without this Binance defaults
            # to FULL for LIMIT, which carries fill arrays we don't need
            # (the user stream delivers fills anyway).
            "newOrderRespType": "ACK",
        }
        if order_type is OrderType.LIMIT:
            params["timeInForce"] = BINANCE_CONSTANTS.TIME_IN_FORCE_GTC
            params["price"] = f"{price_q:f}"
        elif order_type is OrderType.LIMIT_MAKER:
            # LIMIT_MAKER is post-only; Binance rejects timeInForce on
            # this type.
            params["price"] = f"{price_q:f}"
        # MARKET: only quantity is needed.

        try:
            response = await self._router.send_signed(
                CONSTANTS.WS_METHOD_ORDER_PLACE, params,
            )
        except (BinanceWsTimeoutError, BinanceWsUnknownExecutionError,
                BinanceWsDisconnectedError):
            return "UNKNOWN", self._time_synchronizer.time()
        except BinanceWsRequestError as err:
            if self._is_duplicate_error(err):
                reconciled = await self._reconcile_duplicate_order(symbol, order_id)
                if reconciled is not None:
                    return reconciled
            raise

        return str(response.result["orderId"]), float(response.result["transactTime"]) * 1e-3

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        if not self._use_ws_trading or self._router is None:
            return await super()._place_cancel(order_id=order_id, tracked_order=tracked_order)

        symbol = await self.exchange_symbol_associated_to_pair(
            trading_pair=tracked_order.trading_pair,
        )

        # Fast path: numeric orderId is marginally faster server-side
        # than clientOrderId. Use only when known and not UNKNOWN.
        params: Dict[str, Any] = {"symbol": symbol}
        exchange_oid = getattr(tracked_order, "exchange_order_id", None)
        if exchange_oid and exchange_oid != "UNKNOWN":
            try:
                params["orderId"] = int(exchange_oid)
            except (TypeError, ValueError):
                params["origClientOrderId"] = order_id
        else:
            params["origClientOrderId"] = order_id

        try:
            response = await self._router.send_signed(
                CONSTANTS.WS_METHOD_ORDER_CANCEL, params,
            )
        except BinanceWsRequestError as err:
            if err.code == -2011:
                # "Unknown order sent." — already closed / not on book.
                return False
            raise
        except (BinanceWsTimeoutError, BinanceWsUnknownExecutionError,
                BinanceWsDisconnectedError):
            # Don't return False blind — consult REST. The send might
            # have reached the matching engine.
            return await self._reconcile_cancel_via_rest(symbol, order_id, tracked_order)

        status = (response.result or {}).get("status")
        if status == "CANCELED":
            return True
        if status in _TERMINAL_STATUSES:
            self.logger().info(
                "%s symbol=%s order_id=%s status=%s",
                CONSTANTS.WS_LOG_CANCEL_TERMINAL, symbol, order_id, status,
            )
            return False
        # NEW or PARTIALLY_FILLED → not cancelled.
        return False

    # ------------------------------------------------------------------
    # Reconciliation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_duplicate_error(err: BinanceWsRequestError) -> bool:
        return err.code in _DUPLICATE_ORDER_CODES and _DUPLICATE_ORDER_PHRASE in err.msg

    async def _reconcile_duplicate_order(
        self, symbol: str, client_order_id: str,
    ) -> Optional[Tuple[str, float]]:
        """Look up an order Binance refused as duplicate.

        Returns ``(orderId, transactTime)`` if the existing order is
        recoverable (idempotent success); ``None`` if reconciliation
        failed (caller should propagate the original error).
        """
        info = await self._query_order_status_rest(symbol, client_order_id)
        if info is None:
            return None
        return str(info["orderId"]), float(info.get("transactTime", info.get("time", 0))) * 1e-3

    async def _reconcile_cancel_via_rest(
        self, symbol: str, client_order_id: str, tracked_order: InFlightOrder,
    ) -> bool:
        info = await self._query_order_status_rest(symbol, client_order_id)
        if info is None:
            raise BinanceWsUnknownCancelError(
                f"cancel UNKNOWN and REST status lookup failed (symbol={symbol} order_id={client_order_id})"
            )
        status = info.get("status")
        if status == "CANCELED":
            return True
        if status in _TERMINAL_STATUSES:
            self.logger().info(
                "%s reconciled_via=rest symbol=%s order_id=%s status=%s",
                CONSTANTS.WS_LOG_CANCEL_TERMINAL, symbol, client_order_id, status,
            )
            return False
        return False  # NEW / PARTIALLY_FILLED — cancel didn't land.

    async def _query_order_status_rest(
        self, symbol: str, client_order_id: str,
    ) -> Optional[Dict[str, Any]]:
        """REST ``GET /api/v3/order`` keyed by ``origClientOrderId``.

        Returns the parsed JSON dict on success, ``None`` if the call
        fails for any reason — callers translate ``None`` into their
        own escalation path.
        """
        try:
            return await self._api_get(
                path_url=BINANCE_CONSTANTS.ORDER_PATH_URL,
                params={"symbol": symbol, "origClientOrderId": client_order_id},
                is_auth_required=True,
            )
        except Exception as exc:  # noqa: BLE001 — broad on purpose; any failure → None.
            self.logger().warning(
                "REST order.status fallback failed for symbol=%s order_id=%s: %r",
                symbol, client_order_id, exc,
            )
            return None

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def _ws_health_dict(self) -> Dict[str, Any]:
        if self._router is None:
            return {
                "ws_trading_connected": False,
                "ws_state": "absent",
                "ws_reconnect_count": 0,
                "ws_pending_requests": 0,
                "ws_last_message_ts": 0.0,
                "ws_consecutive_timeouts": 0,
            }
        return self._router.health()

    # ------------------------------------------------------------------
    # Boot-time defensive logging — concrete class should call this.
    # ------------------------------------------------------------------

    def _log_ws_boot_state(self) -> None:
        self.logger().info(
            "[binance_ws] boot: use_ws_trading=%s router=%s",
            self._use_ws_trading,
            "ready" if self._router is not None else "absent",
        )
