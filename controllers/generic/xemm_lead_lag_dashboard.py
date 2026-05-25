"""XEMM lead-lag-specific Dashboard API adapter.

Subclasses :class:`BaseDashboardAdapter` to add strategy-specific bits:

* ``WARN_DRIFT`` regime visibility
* Uptime gate for the ``initializing`` status (avoids false ``errored``
  alarms during the first 2 minutes after boot)
* Mapping ``exchangeNumber=1`` → taker, ``=2`` → maker (bitbots convention
  where ``bitprecoBalance`` is the maker side)
* Legacy ``kill_switch_file`` path (``/tmp/xemm_lead_lag_pause``) used by
  ``monitor_heartbeat.sh`` and existing documentation
* XEMM-specific ``info`` block (``arbitrageSpreads``, ``targets``,
  ``locks``) using the controller's ``target_profitability`` and
  ``order_amount`` fields
* ``_before_kill_flush`` flushes the ``TradeLedger`` state.json so a kill
  via API doesn't drop unwritten fills
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Tuple

from hummingbot.dashboard_api.base_adapter import BaseDashboardAdapter

_logger = logging.getLogger(__name__)

# How long after boot is it OK to lack a _safety_snapshot before reporting
# the bot as errored. The snapshot is written once per 60 s by the
# controller (see XEMMLeadLagController._safety_snapshot_interval), so
# 120 s gives one missed tick of slack.
_INITIALIZING_GRACE_SEC = 120.0


class XEMMLeadLagDashboardAdapter(BaseDashboardAdapter):
    """Bitbots-v1-compatible adapter for the XEMM lead-lag controller."""

    # ------------------------------------------------------------------ #
    # Identity / role mapping                                            #
    # ------------------------------------------------------------------ #
    def _connector_role_mapping(self) -> List[Tuple[int, str]]:
        """Bitbots convention: ``exchange1`` = taker (e.g. Binance),
        ``exchange2`` = maker (BitPreco, surfaced as ``bitprecoBalance``).
        """
        cfg = self._c.config
        taker = getattr(cfg, "taker_connector", None)
        maker = getattr(cfg, "maker_connector", None)
        mapping: List[Tuple[int, str]] = []
        if taker:
            mapping.append((1, taker))
        if maker:
            mapping.append((2, maker))
        return mapping or super()._connector_role_mapping()

    def _pause_file_path(self) -> str:
        """Use the legacy kill switch path (``/tmp/xemm_lead_lag_pause``)
        that ``monitor_heartbeat.sh`` and ``OPERATIONS.md`` already
        document. Falls back to the base adapter's default if the config
        doesn't set one.
        """
        ks = getattr(self._c.config, "kill_switch_file", None)
        return ks or super()._pause_file_path()

    # ------------------------------------------------------------------ #
    # Status                                                             #
    # ------------------------------------------------------------------ #
    def get_status(self) -> str:
        snapshot = self._safety_snapshot()
        if not snapshot:
            uptime = self.get_uptime_sec()
            return "running" if uptime < _INITIALIZING_GRACE_SEC else "errored"
        # Snapshot present → delegate to base mapping
        # (pause file → paused, _kill_reason → errored, else running).
        return super().get_status()

    def get_status_metadata(self) -> Dict[str, Any]:
        meta = super().get_status_metadata()

        # If still initializing (no snapshot yet), annotate the reason.
        snapshot = self._safety_snapshot()
        if not snapshot:
            uptime = self.get_uptime_sec()
            if uptime < _INITIALIZING_GRACE_SEC and not meta.get("reason"):
                meta["reason"] = "initializing"
            elif uptime >= _INITIALIZING_GRACE_SEC and not meta.get("reason"):
                meta["reason"] = "initializing timeout"
            return meta

        # WARN_DRIFT regime visibility — only set reason if nothing more
        # specific is already there (kill_reason / staleness / etc.).
        try:
            regime = getattr(self._c, "_regime", None)
            regime_str = str(regime) if regime is not None else None
        except Exception:
            regime_str = None

        if regime_str and "WARN_DRIFT" in regime_str and not meta.get("reason"):
            meta["reason"] = f"WARN_DRIFT: regime={regime_str}"
        return meta

    # ------------------------------------------------------------------ #
    # Info — XEMM vocabulary                                             #
    # ------------------------------------------------------------------ #
    def get_info(self) -> Dict[str, Any]:
        cfg = self._c.config
        # Quote/base from the maker trading pair (canonical for XEMM).
        pair = getattr(cfg, "maker_trading_pair", None) or ""
        if "-" in pair:
            base, quote = pair.split("-", 1)
        else:
            base, quote = "BTC", "BRL"

        target_pct = _decimal_or_zero(getattr(cfg, "target_profitability", 0))
        min_pct = _decimal_or_zero(getattr(cfg, "min_profitability", 0))
        max_pct = _decimal_or_zero(getattr(cfg, "max_profitability", 0))

        order_amount = _decimal_or_zero(getattr(cfg, "order_amount", 0))
        max_mul = _decimal_or_zero(getattr(cfg, "max_order_amount_multiplier", 1))
        lock = order_amount * max_mul

        # `inventory_target_pct` is a fraction of portfolio in base; we
        # expose it as the "target" string for parity with bitbots.
        inv_target_pct = _decimal_or_zero(getattr(cfg, "inventory_target_pct", 0))

        spreads = {base: {"buy": _to_percent_fraction(target_pct),
                          "sell": _to_percent_fraction(target_pct)}}
        spreads_min = {base: {"buy": _to_percent_fraction(min_pct),
                              "sell": _to_percent_fraction(min_pct)}}
        spreads_max = {base: {"buy": _to_percent_fraction(max_pct),
                              "sell": _to_percent_fraction(max_pct)}}

        exchanges = [name for _, name in self._connector_role_mapping()]
        return {
            "quote": quote,
            "bases": [base],
            "maxLoss": float(_decimal_or_zero(getattr(cfg, "max_daily_loss_quote", 0))),
            "arbitrageSpreads": spreads,
            "speculativeSpreads": {
                "exchange": None,
                "bitpreco": spreads,
            },
            "spreadsMin": spreads_min,
            "spreadsMax": spreads_max,
            "buy_spread": _to_percent_fraction(target_pct),
            "sell_spread": _to_percent_fraction(target_pct),
            "sanfona": "0",
            "targets": {base: str(inv_target_pct)},
            "locks": {base: str(lock)},
            "exchanges": exchanges,
            "wallets": {name: [] for name in exchanges},
            "prices": self.get_prices(),
            "config": _config_summary(cfg),
        }

    # ------------------------------------------------------------------ #
    # Lifecycle hooks — cancel orders + run audit on pause/resume/kill   #
    #                                                                    #
    # All hooks are wrapped by BaseDashboardAdapter._run_hook with a     #
    # global asyncio.wait_for(timeout=45). Exceptions are caught there  #
    # and surfaced in statusMetadata.reason. We additionally swallow    #
    # per-step errors so a failed cancel doesn't abort the subsequent   #
    # audit (still useful) and vice-versa.                              #
    # ------------------------------------------------------------------ #
    async def _before_pause(self) -> None:
        """On pause/stop: cancel all open orders, then run inventory audit."""
        await self._cancel_then_audit(source="dashboard_pause")

    async def _after_resume(self) -> None:
        """On resume: symmetric with pause — cancel (no-op typically)
        then audit to ensure the bot is at target before resuming trading.
        """
        await self._cancel_then_audit(source="dashboard_resume")

    async def _before_kill(self) -> None:
        """On kill/reset: cancel orders, run audit, then flush TradeLedger
        state (best-effort) before SIGTERM is scheduled by the base class.
        """
        await self._cancel_then_audit(source="dashboard_kill")
        self._flush_ledger()

    # ------------------------------------------------------------------ #
    # Shared steps                                                       #
    # ------------------------------------------------------------------ #
    async def _cancel_then_audit(self, *, source: str) -> None:
        """Run cancel-all + inventory audit, recording per-phase latencies.

        Each phase is wrapped in its own try/except so partial failures
        don't abort the rest. The base ``_run_hook`` wrapper will catch
        any unexpected exception and surface it in ``statusMetadata``.
        """
        # Phase 1: cancel all open orders via REST on both exchanges.
        t_cancel = time.monotonic()
        try:
            cancel_fn = getattr(self._c, "_cancel_all_open_orders_on_startup", None)
            if callable(cancel_fn):
                await cancel_fn()
            else:
                _logger.warning(
                    "[xemm_dashboard] cancel skipped: controller has no "
                    "_cancel_all_open_orders_on_startup method"
                )
        except Exception as e:
            _logger.exception("[xemm_dashboard] %s cancel failed: %s", source, e)
        finally:
            self._record_phase("cancel", t_cancel)

        # Phase 2: inventory audit (state machine, up to 3 transitions).
        t_audit = time.monotonic()
        try:
            audit_fn = getattr(self._c, "_run_inventory_audit", None)
            if callable(audit_fn):
                now = self._now()
                await audit_fn(now, source=source)
            else:
                _logger.warning(
                    "[xemm_dashboard] audit skipped: controller has no "
                    "_run_inventory_audit method"
                )
        except Exception as e:
            _logger.exception("[xemm_dashboard] %s audit failed: %s", source, e)
        finally:
            self._record_phase("audit", t_audit)

    def _flush_ledger(self) -> None:
        """Best-effort flush of the TradeLedger state.json before SIGTERM."""
        t_flush = time.monotonic()
        try:
            ledger = getattr(self._c, "_trade_ledger", None)
            if ledger is None:
                return
            last_record = getattr(ledger, "_last_trade_record", None) or {}
            ledger._write_state(last_record)
        except Exception as e:
            _logger.debug("[xemm_dashboard] ledger flush failed: %s", e)
        finally:
            self._record_phase("flush", t_flush)

    def _now(self) -> float:
        """Controller-aware clock: prefer market_data_provider.time() so
        we share the same clock the audit uses internally; fall back to
        wall clock if the provider is unavailable.
        """
        try:
            mdp = getattr(self._c, "market_data_provider", None)
            if mdp is not None and hasattr(mdp, "time"):
                return float(mdp.time())
        except Exception:
            pass
        return time.time()


# ---------------------------------------------------------------------------
# Helpers (file-local)
# ---------------------------------------------------------------------------

def _decimal_or_zero(value):
    """Coerce to Decimal-like, returning Decimal(0) on failure."""
    from decimal import Decimal
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _to_percent_fraction(frac, precision: int = 5) -> str:
    """Convert a fraction (e.g. 0.0020 → "0.2%") to a bitbots percent string."""
    if frac is None:
        return ""
    pct = round(float(frac) * 100.0, precision)
    if pct == int(pct):
        return f"{int(pct)}%"
    formatted = f"{pct:.{precision}f}".rstrip("0").rstrip(".")
    return f"{formatted}%"


def _config_summary(cfg: Any) -> Dict[str, Any]:
    """Subset of controller config surfaced in ``info.config`` (bitbots compat).

    Picks only fields that are safe to expose (no secrets, no internal
    runtime state). Missing fields are silently omitted.
    """
    keys = (
        "id", "controller_name", "controller_type",
        "maker_connector", "maker_trading_pair",
        "taker_connector", "taker_trading_pair",
        "order_amount", "min_dynamic_order_amount",
        "max_order_amount_multiplier",
        "min_profitability", "target_profitability", "max_profitability",
        "inventory_target_pct",
        "max_daily_loss_quote", "max_session_drawdown_quote",
        "max_hourly_burn_quote", "max_minute_burn_quote",
        "max_consecutive_losing_fills",
        "kill_switch_file",
    )
    out: Dict[str, Any] = {}
    for k in keys:
        if hasattr(cfg, k):
            try:
                v = getattr(cfg, k)
                # Decimal/Path → str for JSON.
                from decimal import Decimal
                if isinstance(v, Decimal):
                    v = str(v)
                out[k] = v
            except Exception:
                continue
    return out


__all__ = ["XEMMLeadLagDashboardAdapter"]
