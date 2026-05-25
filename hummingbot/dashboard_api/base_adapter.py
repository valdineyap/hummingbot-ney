"""Default ``DashboardBotAdapter`` implementation reusable by any controller.

``BaseDashboardAdapter`` reads only public attributes of a Hummingbot
``ControllerBase`` subclass: ``config``, ``market_data_provider``, and
``get_active_executors()``. It works out-of-the-box for any controller
that uses these — no per-strategy code required.

Strategies with custom regimes, vocabularies, or pause mechanisms subclass
and override individual hooks (``get_status``, ``get_info``,
``_pause_file_path``, etc.). All other methods are inherited.

**Invariant**: this adapter never raises into the HTTP handler. Errors
from the controller side become empty/None fields. The HTTP server is
also defensive, but adapters should not depend on that.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .payload import (
    format_utc_z,
    safe_bot_name,
    to_jsonable,
)
from .status_store import read_status, status_file_path, write_status

_logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path("data")


@dataclass
class _HookResult:
    """Result of running a lifecycle hook (``_before_pause`` / etc.).

    Carried back to the command handler so failure information can be
    surfaced in HTTP responses and persisted in ``statusMetadata``.
    """
    error: Optional[str] = None
    timed_out: bool = False
    timings: Dict[str, int] = field(default_factory=dict)


class BaseDashboardAdapter:
    """Default adapter that satisfies :class:`DashboardBotAdapter`.

    :param controller: Any object with ``config``, ``market_data_provider``,
        and ``get_active_executors()``. Typically a ``ControllerBase``
        subclass, but a fake/stub also works (useful in tests).
    :param data_dir: Where ``dashboard_status_<bot>.json`` is persisted.
        Defaults to ``./data``.
    :param bot_version: Free-form version string surfaced in
        ``getdata.botVersion`` and ``/health``. Falls back to env
        ``BOT_VERSION`` and finally ``"hummingbot-ney"``.
    """

    # ------------------------------------------------------------------ #
    # Construction                                                       #
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        controller: Any,
        data_dir: Optional[Path] = None,
        bot_version: Optional[str] = None,
    ) -> None:
        self._c = controller
        self.bot_name: str = getattr(controller.config, "id", "unknown_bot")
        self._data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        self._status_path = status_file_path(self._data_dir, self.bot_name)
        self._boot_at = time.monotonic()
        self._bot_version = bot_version or os.environ.get("BOT_VERSION") or "hummingbot-ney"

        # Persisted status metadata (last command's reason/requester/etc).
        self._status_meta: Dict[str, Any] = read_status(self._status_path)

        # Idempotency state for kill/reset.
        self._kill_scheduled: bool = False
        self._kill_handle: Optional[asyncio.TimerHandle] = None

        # Timestamp of the last successful balance read (any exchange with
        # at least one currency). Surfaces in getdata.balanceLastUpdated so
        # the dashboard can show "X seconds ago" — useful operationally
        # when WS user-stream is down and we're falling back to REST poll.
        self._last_balances_at: Optional[datetime] = None

    # ------------------------------------------------------------------ #
    # Hooks subclasses override                                          #
    # ------------------------------------------------------------------ #
    def _pause_file_path(self) -> str:
        """Default kill-switch file path. Override if your strategy uses a
        different convention (XEMM lead-lag uses ``/tmp/xemm_lead_lag_pause``).
        """
        return f"/tmp/{safe_bot_name(self.bot_name)}.pause"

    def _connector_role_mapping(self) -> List[Tuple[int, str]]:
        """Map bitbots ``exchangeNumber`` (1, 2) → connector name.

        Default: takes the first 2 connectors from
        ``controller.market_data_provider.connectors`` in iteration order
        (Python ≥3.7 dicts preserve insertion order). Override per
        strategy when the role (maker/taker) is meaningful.
        """
        connectors = self._connectors()
        names = list(connectors.keys())[:2]
        return [(i + 1, name) for i, name in enumerate(names)]

    # ------------------------------------------------------------------ #
    # Internal helpers (defensive reads of the controller)               #
    # ------------------------------------------------------------------ #
    def _connectors(self) -> Dict[str, Any]:
        mdp = getattr(self._c, "market_data_provider", None)
        if mdp is None:
            return {}
        return getattr(mdp, "connectors", {}) or {}

    def _trading_pairs_for(self, connector: Any) -> List[str]:
        """Best-effort list of trading pairs this connector tracks."""
        pairs = getattr(connector, "trading_pairs", None)
        if pairs:
            return list(pairs)
        # Fallback: pull from order books dict if exposed.
        obs = getattr(connector, "order_books", None) or {}
        return list(obs.keys())

    def _safety_snapshot(self) -> Optional[dict]:
        return getattr(self._c, "_safety_snapshot", None)

    def _snapshot_age_sec(self) -> Optional[float]:
        snap = self._safety_snapshot()
        if not isinstance(snap, dict):
            return None
        ts = snap.get("ts") or snap.get("timestamp")
        if ts is None:
            return None
        try:
            return max(0.0, time.time() - float(ts))
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------ #
    # Status                                                             #
    # ------------------------------------------------------------------ #
    def get_status(self) -> str:
        """Default mapping: pause file → paused, _kill_reason → errored, else running."""
        try:
            if Path(self._pause_file_path()).exists():
                return "paused"
        except OSError:
            pass
        if getattr(self._c, "_kill_reason", None):
            return "errored"
        return "running"

    def get_status_metadata(self) -> Dict[str, Any]:
        """Return persisted metadata, annotated with derived health info.

        Subclasses override to inject regime-specific reasons (e.g.
        ``WARN_DRIFT``). The base implementation handles:
        * staleness annotation if ``_safety_snapshot`` is older than 180s
        * surface of ``_kill_reason`` if present
        * clearing the persisted reason once status returns to running normally
        """
        meta = dict(self._status_meta)

        kill_reason = getattr(self._c, "_kill_reason", None)
        if kill_reason:
            # Priority 2 in the spec: surface the actual kill reason.
            meta["reason"] = str(kill_reason)

        # Priority 5: annotate staleness.
        age = self._snapshot_age_sec()
        if age is not None and age > 180:
            base = meta.get("reason") or ""
            sep = " " if base else ""
            meta["reason"] = f"{base}{sep}(snapshot stale: {int(age)}s)".strip()

        return meta

    # ------------------------------------------------------------------ #
    # Info / balances / prices (best-effort, default impls)              #
    # ------------------------------------------------------------------ #
    def get_info(self) -> Dict[str, Any]:
        """Minimal default ``info`` block. Subclasses extend with
        strategy-specific keys (spreads, targets, locks, etc.)."""
        cfg = self._c.config
        # Try to infer quote/base from common config fields.
        quote = getattr(cfg, "quote", None)
        bases = getattr(cfg, "bases", None)
        if not quote or not bases:
            pair = getattr(cfg, "maker_trading_pair", None) or getattr(cfg, "trading_pair", None)
            if pair and "-" in pair:
                base, q = pair.split("-", 1)
                bases = bases or [base]
                quote = quote or q
        exchanges = [name for _, name in self._connector_role_mapping()]
        return {
            "quote": quote,
            "bases": list(bases) if bases else [],
            "exchanges": exchanges,
            "wallets": {name: [] for name in exchanges},
            "config": {},
            "prices": self.get_prices(),
            "sanfona": "0",
        }

    def get_balances(self) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Optional[datetime]]:
        """Read balances from each connector independently. Errors per
        connector yield empty dicts but never raise."""
        mapping = self._connector_role_mapping()
        connectors = self._connectors()
        balances_by_role: Dict[int, Dict[str, float]] = {1: {}, 2: {}}
        for role, name in mapping:
            try:
                raw = connectors[name].get_all_balances() or {}
                balances_by_role[role] = {k: float(v) for k, v in raw.items()}
            except Exception as e:
                _logger.debug("get_balances: %s failed (%s); empty", name, e)
                balances_by_role[role] = {}

        exch1 = balances_by_role.get(1, {})
        exch2 = balances_by_role.get(2, {})
        total: Dict[str, float] = {}
        for k, v in exch1.items():
            total[k] = total.get(k, 0.0) + v
        for k, v in exch2.items():
            total[k] = total.get(k, 0.0) + v
        # Stamp balanceLastUpdated when at least one exchange returned data.
        # The connector cache is continuously fed by WS user-stream + REST
        # poll fallback, so "now" reflects the freshest cached value.
        if exch1 or exch2:
            self._last_balances_at = datetime.now(timezone.utc)
        return exch1, exch2, total, self._last_balances_at

    def get_prices(self) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
        """Top-of-book bid/ask per connector per pair. Missing books yield
        ``{bid: None, ask: None}``."""
        result: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
        for _, name in self._connector_role_mapping():
            connector = self._connectors().get(name)
            if connector is None:
                continue
            entry: Dict[str, Dict[str, Optional[float]]] = {}
            for pair in self._trading_pairs_for(connector):
                base = pair.split("-")[0] if "-" in pair else pair
                bid, ask = self._best_bid_ask(connector, pair)
                entry[base] = {"bid": bid, "ask": ask}
            result[name] = entry
        return result

    @staticmethod
    def _best_bid_ask(connector: Any, pair: str) -> Tuple[Optional[float], Optional[float]]:
        try:
            ob = connector.get_order_book(pair)
        except Exception:
            return None, None
        if ob is None:
            return None, None
        try:
            best_bid_row = next(iter(ob.bid_entries()), None)
            best_ask_row = next(iter(ob.ask_entries()), None)
        except Exception:
            return None, None
        bid = float(best_bid_row.price) if best_bid_row is not None else None
        ask = float(best_ask_row.price) if best_ask_row is not None else None
        return bid, ask

    # ------------------------------------------------------------------ #
    # Misc getters                                                       #
    # ------------------------------------------------------------------ #
    def get_bot_version(self) -> str:
        return self._bot_version

    def get_uptime_sec(self) -> float:
        return time.monotonic() - self._boot_at

    def get_avg_cycle_duration_ms(self) -> int:
        # Bitbots uses milliseconds. Hummingbot tick interval is 1s by default;
        # fall back to that.
        return 1000

    # ------------------------------------------------------------------ #
    # Hook timeout (per-command, applied via asyncio.wait_for)            #
    # ------------------------------------------------------------------ #
    HOOK_TIMEOUT_SEC: float = 45.0

    # ------------------------------------------------------------------ #
    # Commands                                                           #
    # ------------------------------------------------------------------ #
    async def pause(self, reason: str, requester: Optional[str], force: bool) -> Dict[str, Any]:
        verb = "stopped" if force else "paused"
        # 1. Before-pause hook — strategy may cancel orders, run audit, etc.
        hook_result = await self._run_hook("pause", self._before_pause)
        # 2. Touch pause file (ALWAYS — even if hook failed/timed-out, we
        # honor the user's intent to pause).
        try:
            Path(self._pause_file_path()).touch()
        except OSError as e:
            _logger.warning("pause: failed to touch %s (%s)", self._pause_file_path(), e)
        # 3. Persist metadata (annotate hook failures in `reason` so the
        # dashboard surfaces them).
        annotated_reason = self._annotate_reason(
            reason or f"Bot {verb} via API by {requester or 'anonymous'}",
            hook_result,
        )
        self._update_meta(
            reason=annotated_reason,
            requester=requester,
            stopped=bool(force),
        )
        return {"status": "ok", **hook_result.timings}

    async def resume(self, requester: Optional[str]) -> Dict[str, Any]:
        # 1. Remove pause file first — semantically "unblock new orders".
        try:
            Path(self._pause_file_path()).unlink()
        except FileNotFoundError:
            pass  # idempotent
        except OSError as e:
            _logger.warning("resume: failed to unlink %s (%s)", self._pause_file_path(), e)
        # 2. After-resume hook (cancel + audit to restore target).
        hook_result = await self._run_hook("resume", self._after_resume)
        # 3. Clear reason — a healthy running bot shouldn't display
        # "paused by X" forever. If the hook failed, annotate but still
        # mark not-paused.
        reason = None
        if hook_result.error:
            reason = self._annotate_reason(None, hook_result)
        self._update_meta(reason=reason, requester=requester, stopped=False)
        return {"status": "ok", **hook_result.timings}

    async def kill(self, reason: str, requester: Optional[str]) -> Dict[str, Any]:
        if self._kill_scheduled:
            return {"status": "already_scheduled"}
        # 1. Run before-kill hook (cancel + audit + flush). This is the
        # only chance to do these things before SIGTERM fires.
        hook_result = await self._run_hook("kill", self._before_kill)
        # 2. Persist metadata (BEFORE scheduling the kill, so the file is
        # already on disk when the next process boots).
        annotated_reason = self._annotate_reason(
            f"killed via API: {reason}" if reason else "killed via API",
            hook_result,
        )
        self._update_meta(reason=annotated_reason, requester=requester)
        # 3. Schedule SIGTERM. systemd will relaunch.
        try:
            loop = asyncio.get_running_loop()
            self._kill_handle = loop.call_later(
                0.2, os.kill, os.getpid(), signal.SIGTERM
            )
        except RuntimeError:
            _logger.warning("kill: no running loop; falling back to immediate SIGTERM")
            os.kill(os.getpid(), signal.SIGTERM)
        self._kill_scheduled = True
        return {"status": "scheduled", **hook_result.timings}

    async def reset(self, requester: Optional[str]) -> Dict[str, Any]:
        return await self.kill(reason="reset via API", requester=requester)

    async def settle(self, params: Dict[str, Any], requester: Optional[str]) -> Dict[str, Any]:
        raise NotImplementedError("settle is not implemented for BaseDashboardAdapter")

    # ------------------------------------------------------------------ #
    # Hooks — subclasses override to inject cancel/audit/etc.            #
    # All hooks are best-effort: their success/failure NEVER blocks the  #
    # touch/unlink/SIGTERM path; failures are annotated in statusMetadata.
    # ------------------------------------------------------------------ #
    async def _before_pause(self) -> None:
        """Called inside ``pause()`` before the pause file is touched.

        Default: no-op. Subclasses override to cancel open orders, run
        inventory audit, etc. Must not raise — exceptions are caught by
        the wrapper, logged, and surfaced in ``statusMetadata.reason``.
        """
        return None

    async def _after_resume(self) -> None:
        """Called inside ``resume()`` after the pause file is removed.

        Default: no-op. Subclasses override (typically symmetric to
        ``_before_pause``).
        """
        return None

    async def _before_kill(self) -> None:
        """Called inside ``kill()`` before SIGTERM is scheduled.

        Default: no-op. Subclasses override to cancel orders, run audit,
        and/or flush persistent state (e.g., TradeLedger).
        """
        return None

    # ------------------------------------------------------------------ #
    # Hook execution wrapper                                             #
    # ------------------------------------------------------------------ #
    async def _run_hook(self, label: str, coro_fn) -> "_HookResult":
        """Invoke ``coro_fn`` with timeout/exception trapping.

        Returns a :class:`_HookResult` carrying timings, error string,
        and a ``timings`` dict suitable to merge into the HTTP response.
        Logs an INFO line ``[dashboard_cmd] <label> total_ms=...`` with
        component-level latencies populated by the hook itself via
        :meth:`_record_phase`.
        """
        self._hook_phases = {}  # cleared each invocation
        start = time.monotonic()
        error: Optional[str] = None
        timed_out = False
        try:
            await asyncio.wait_for(coro_fn(), timeout=self.HOOK_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            timed_out = True
            error = f"timeout after {self.HOOK_TIMEOUT_SEC}s"
            _logger.warning("[dashboard_cmd] %s hook %s", label, error)
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            _logger.exception("[dashboard_cmd] %s hook failed", label)
        total_ms = int((time.monotonic() - start) * 1000)
        # Log a single INFO line — phase timings included if recorded.
        phase_str = " ".join(f"{k}_ms={v}" for k, v in self._hook_phases.items())
        _logger.info(
            "[dashboard_cmd] %s %s total_ms=%d%s",
            label,
            phase_str,
            total_ms,
            f" error={error}" if error else "",
        )
        timings = {**self._hook_phases, "total_ms": total_ms}
        return _HookResult(error=error, timed_out=timed_out, timings=timings)

    def _record_phase(self, name: str, started_at: float) -> None:
        """Record a sub-phase latency (ms) for the current hook.

        Subclasses call this from inside ``_before_pause`` / etc::

            t0 = time.monotonic()
            await self._c._cancel_all_open_orders_on_startup()
            self._record_phase("cancel", t0)
        """
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
        if not hasattr(self, "_hook_phases") or self._hook_phases is None:
            self._hook_phases = {}
        self._hook_phases[f"{name}_ms"] = elapsed_ms

    def _annotate_reason(self, base_reason: Optional[str], hook: "_HookResult") -> Optional[str]:
        """Append a hook-failure suffix to ``base_reason`` if needed."""
        if not hook.error:
            return base_reason
        suffix = f"hook incomplete ({hook.error})"
        if base_reason:
            return f"{base_reason}; {suffix}"
        return suffix

    # ------------------------------------------------------------------ #
    # Auxiliary endpoints                                                #
    # ------------------------------------------------------------------ #
    async def get_books(
        self,
        pair: str,
        exchanges: Optional[List[int]],
        depth: int,
        apply_spread: bool,
    ) -> Dict[str, Any]:
        """Return a bitbots-shape ``getBooks`` payload."""
        mapping = dict(self._connector_role_mapping())
        wanted = exchanges or list(mapping.keys())
        base, quote = (pair.split("-", 1) + [None])[:2] if "-" in pair else (pair, None)

        books: List[Dict[str, Any]] = []
        for ex_num in wanted:
            connector_name = mapping.get(ex_num)
            if connector_name is None:
                # Preserve compat: still include the entry with empty book.
                books.append({
                    "exchangeNumber": ex_num,
                    "book": {"bids": [], "asks": []},
                    "spreads": None,
                })
                continue
            connector = self._connectors().get(connector_name)
            bids: List[Dict[str, float]] = []
            asks: List[Dict[str, float]] = []
            try:
                ob = connector.get_order_book(pair) if connector is not None else None
                if ob is not None:
                    bids = self._book_side(ob.bid_entries(), depth)
                    asks = self._book_side(ob.ask_entries(), depth)
            except Exception as e:
                _logger.debug("get_books: %s/%s failed (%s); empty book", connector_name, pair, e)
            books.append({
                "exchangeNumber": ex_num,
                "exchangeName": connector_name,
                "book": {"bids": bids, "asks": asks},
                "spreads": None,
            })
        return {
            "base": base,
            "quote": quote,
            "botName": self.bot_name,
            "botStatus": self.get_status(),
            "books": books,
        }

    @staticmethod
    def _book_side(entries_iter, depth: int) -> List[Dict[str, float]]:
        out: List[Dict[str, float]] = []
        for i, row in enumerate(entries_iter):
            if i >= depth:
                break
            try:
                out.append({"price": float(row.price), "amount": float(row.amount)})
            except Exception:
                continue
        return out

    async def get_spec_orders(self) -> Dict[str, Any]:
        """Return active executors partitioned by exchange (bitbots ``specOrders``)."""
        mapping = self._connector_role_mapping()
        # Default: empty buckets for each known exchange.
        buckets: Dict[int, List[Dict[str, Any]]] = {num: [] for num, _ in mapping}
        name_to_num = {name: num for num, name in mapping}

        try:
            getter = getattr(self._c, "get_active_executors", None)
            executors = getter() if callable(getter) else []
        except Exception as e:
            _logger.debug("get_spec_orders: get_active_executors failed (%s)", e)
            executors = []

        for ex in executors:
            connector_name = getattr(ex, "connector_name", None)
            num = name_to_num.get(connector_name)
            if num is None:
                continue
            try:
                amount = float(getattr(ex.config, "amount", 0)) if hasattr(ex, "config") else 0.0
            except Exception:
                amount = 0.0
            try:
                price = getattr(ex.config, "entry_price", None) if hasattr(ex, "config") else None
                price = float(price) if price is not None else None
            except Exception:
                price = None
            pair = getattr(ex, "trading_pair", "")
            base, quote = (pair.split("-", 1) + [None])[:2] if "-" in pair else (pair, None)
            buckets[num].append({
                "executorId": getattr(ex, "id", None),
                "side": str(getattr(ex, "side", "")),
                "amount": amount,
                "price": price,
                "exchangeName": connector_name,
                "base": base,
                "quote": quote,
                "status": str(getattr(ex, "status", "")),
                "type": getattr(ex, "type", None),
            })

        result = {"botName": self.bot_name, "timestamp": format_utc_z()}
        for num, name in mapping:
            key = f"exchange{num}"
            result[key] = {
                "name": name,
                "orders": buckets[num],
                "count": len(buckets[num]),
            }
        result["total"] = sum(len(b) for b in buckets.values())
        return result

    # ------------------------------------------------------------------ #
    # Internal: persist status metadata                                  #
    # ------------------------------------------------------------------ #
    def _update_meta(self, **fields: Any) -> None:
        sanitized = {k: to_jsonable(v) for k, v in fields.items()}
        self._status_meta.update(sanitized)
        self._status_meta["lastUpdated"] = format_utc_z()
        write_status(self._status_path, self._status_meta)
