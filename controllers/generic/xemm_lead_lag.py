"""
XEMMLeadLag controller — XEMM with synthetic lead-lag signal.

Places passive maker orders on Exchange A (e.g. Bybit BTC-BRL), hedges fills
via taker on Exchange B (e.g. Binance BTC-BRL), and uses a synthetic fair
price (BTC-USDT × USDT-BRL on a third "signal" connector) only as a
lead-lag signal — never as a hedge leg.

The signal is used to:
  - Cancel maker orders before adverse fills (when fair moves > local)
  - Adjust target_profitability per side dynamically
  - Detect regime changes (basis extreme, feed staleness)

Uses XEMMLeadLagExecutor (LIMIT_MAKER) instead of XEMMExecutor (LIMIT) to avoid
maker orders being filled as taker due to home-latency price drift.
"""
import asyncio
import csv
import json
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Deque, Dict, List, Literal, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import MarketDict, OrderType, PriceType, TradeType
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import MarketEvent
from hummingbot.strategy_v2.controllers.controller_base import (
    ControllerBase,
    ControllerConfigBase,
)
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import (
    LeadLagArbitrageExecutorConfig,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import (
    XEMMLeadLagExecutorConfig,
)
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    ExecutorAction,
    StopExecutorAction,
)
from hummingbot.strategy_v2.utils.lead_lag_signal import (
    LeadLagSignalProvider,
    SignalQuality,
)


class InventoryAuditConfig(BaseModel):
    """
    State-based inventory reconciliation: for each base asset, the bot's total
    holdings (maker_balance + taker_balance) are compared against a fixed target
    every `audit_interval_sec`. If the divergence (converted to quote currency
    via the maker's current mid_price) exceeds `max_drift_quote` AND there is no
    in-flight trade activity that could explain it, `on_drift_action` fires.

    Why an absolute quote threshold (not percentage):
      * Percentage tolerance scales with `target`, leading to absurd absolute
        values for large books (0.5% × 1 BTC = ~2000 BRL of "noise" — way past
        any rebalance fee). An absolute BRL threshold reflects the real economics:
        below `max_drift_quote` it is not worth rebalancing (fees + slippage
        dominate), regardless of book size.
      * Below this threshold the audit is SILENT — no CRITICAL, no anomaly
        counter increment.

    Notes:
      * Only base assets (BTC, ETH...) get a target — quote (BRL) grows with PnL.
      * `max_drift_quote` is global (applies to every asset, expressed in the
        maker's quote currency — BRL for BTC-BRL).
      * Hedge in-flight detection (executors active, in_flight_orders, recent fill)
        is preferred over a fixed grace period — defers drift action only when
        a legitimate trade can explain the imbalance.
    """
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    audit_interval_sec: float = 300.0
    # Drift máximo aceitável em quote currency (e.g. BRL para BTC-BRL).
    # Default: 60 BRL (~$12 USD) — acima do min_notional típico das exchanges
    # e do break-even de fees+slippage do MARKET rebalance. Convertido para BTC
    # em runtime usando o mid_price corrente do maker.
    max_drift_quote: Decimal = Decimal("60")
    on_drift_action: Literal["alert", "pause", "auto_rebalance"] = "pause"
    base_targets: Dict[str, Decimal] = Field(default_factory=dict)


class Regime(str, Enum):
    """Operational state of the controller. Drives action gating."""
    WARMUP = "WARMUP"           # No history yet → don't trade
    OK = "OK"                   # Healthy → create + adjust by lead
    DEGRADED = "DEGRADED"       # One feed degraded → trade w/o lead adjustment
    PAUSED = "PAUSED"           # Strong gate → cancel all + don't recreate
    KILLED = "KILLED"           # Permanent stop (kill switch / circuit breaker)


class XEMMLeadLagConfig(ControllerConfigBase):
    """Configuration for the XEMM Lead-Lag controller."""
    model_config = ConfigDict(extra="forbid")

    controller_name: str = "xemm_lead_lag"
    controller_type: str = "generic"

    # === Markets ===
    maker_connector: str = Field(
        json_schema_extra={"prompt": "Maker connector (e.g. bybit): ", "prompt_on_new": True})
    maker_trading_pair: str = Field(
        json_schema_extra={"prompt": "Maker trading pair (e.g. BTC-BRL): ", "prompt_on_new": True})
    taker_connector: str = Field(
        json_schema_extra={"prompt": "Taker connector (e.g. binance): ", "prompt_on_new": True})
    taker_trading_pair: str = Field(
        json_schema_extra={"prompt": "Taker trading pair (e.g. BTC-BRL): ", "prompt_on_new": True})
    signal_connector: str = Field(
        json_schema_extra={"prompt": "Signal connector (e.g. binance): ", "prompt_on_new": True})
    signal_base_pair: str = Field(
        json_schema_extra={"prompt": "Signal base pair (e.g. BTC-USDT): ", "prompt_on_new": True})
    signal_fx_pair: str = Field(
        json_schema_extra={"prompt": "Signal FX pair (e.g. USDT-BRL): ", "prompt_on_new": True})

    # === Sizing ===
    order_amount: Decimal = Field(
        default=Decimal("0.0002"),
        json_schema_extra={"prompt": "Order amount in base asset: ", "prompt_on_new": True})

    # === Profitability (NET of fees — XEMMExecutor adds tx_cost_pct internally) ===
    min_profitability: Decimal = Field(default=Decimal("0.0007"))
    target_profitability: Decimal = Field(default=Decimal("0.0020"))
    max_profitability: Decimal = Field(default=Decimal("0.0080"))

    # === Lead-lag signal ===
    lead_windows_seconds: List[int] = Field(default=[5, 10, 15])
    profitability_adjust_threshold_bps: Decimal = Field(default=Decimal("5"))
    soft_cancel_threshold_bps: Decimal = Field(default=Decimal("10"))
    fast_cancel_threshold_bps: Decimal = Field(default=Decimal("15"))
    w_lead: Decimal = Field(default=Decimal("0.20"))
    ema_alpha_fx: Decimal = Field(default=Decimal("0.30"))

    # === Feed staleness (seconds) ===
    max_leader_staleness_sec: float = Field(default=5.0)
    max_fx_staleness_sec: float = Field(default=10.0)
    max_local_staleness_sec: float = Field(default=5.0)

    # === Risk gates ===
    basis_hard_threshold_bps: Decimal = Field(default=Decimal("150"))
    max_local_spread_bps: Decimal = Field(default=Decimal("200"))

    # === Anti-churn ===
    min_requote_interval_sec: float = Field(default=3.0)

    # === Book-aware placement ===
    # Extra margin above min_profitability used only at order placement.
    # Creates hysteresis so placed orders don't immediately re-trigger the cancel floor.
    placement_profitability_buffer: Decimal = Field(default=Decimal("0.0002"))

    # === Lead-aware placement (Priority 4) ===
    # When |best_lead_bps| > placement_lead_signal_threshold_bps, the executor
    # adjusts effective_min in create_maker_order:
    #   favours side: -placement_lead_aware_delta_bps (tighter, more aggressive)
    #   opposes side: +placement_lead_aware_delta_bps (wider, more conservative)
    # Set placement_lead_aware_delta_bps=0 to disable.
    placement_lead_aware_delta_bps: Decimal = Field(default=Decimal("1.5"))
    placement_lead_signal_threshold_bps: Decimal = Field(default=Decimal("3"))

    # === Operational mode switches (independent) ===
    enable_market_making: bool = Field(default=True)
    enable_pure_arb: bool = Field(default=False)

    # === Pure arbitrage (taker:taker) ===
    # NET threshold — controller spawn gate compares ``gross_bps − tx_cost_bps``
    # against this value, matching the executor's own NET execute gate and the
    # MM controller's NET min/target/max_profitability semantics.
    arb_order_amount: Decimal = Field(default=Decimal("0.0002"))
    arb_min_profitability: Decimal = Field(default=Decimal("0.0002"))   # 2 bps net
    arb_lead_aggressive_delta: Decimal = Field(default=Decimal("0.0003"))   # -3 bps when lead favors
    arb_lead_conservative_delta: Decimal = Field(default=Decimal("0.0005")) # +5 bps when lead contradicts
    arb_lead_signal_threshold_bps: Decimal = Field(default=Decimal("5"))    # ±5 bps dead zone

    # Rate limits
    arb_max_per_hour: int = Field(default=10)
    arb_min_interval_sec: float = Field(default=5.0)
    # Max time an arb executor may stay in RUNNING without firing
    # execute_arbitrage. The executor polls profitability each tick; if a
    # spawn condition disappears immediately after spawn, the executor
    # would otherwise sit forever waiting for an opportunity. Observed in
    # prod 2026-05-11 17:35: 10 arbs spawned, none executed, all stuck.
    # 60s is long enough that a real fast-moving opportunity has time to
    # cross threshold + execute, but short enough that stuck executors
    # don't accumulate.
    arb_executor_max_age_sec: float = Field(default=60.0)

    # Capital strategy (dynamic — checks free balance at spawn time)
    arb_capital_strategy: str = Field(default="skip_if_insufficient")  # or "cancel_xemm_to_free"

    # Unwind on partial failure
    arb_max_unwind_slippage_bps: Decimal = Field(default=Decimal("50"))
    arb_unwind_strategy: str = Field(default="abort_and_alert")  # or "force_unwind"

    # Circuit breakers
    arb_failure_pause_sec: float = Field(default=1800.0)            # 30 min after failure
    arb_max_failures_per_day: int = Field(default=3)
    arb_daily_loss_limit_quote: Decimal = Field(default=Decimal("100"))

    # Alert threshold: log INFO + flag CSV column when spread crosses this (even with arb disabled).
    # Set below fees (~11bps) to catch near-misses for calibration. 0 = disabled.
    arb_alert_threshold_bps: float = Field(default=8.0)

    # === Inventory (per-exchange) ===
    inventory_target_pct: Decimal = Field(default=Decimal("0.5"))
    inventory_skew_strength: Decimal = Field(default=Decimal("0.001"))

    # === Basis-aware directional skew (Priority 5) ===
    # Biases target_profitability per side based on persistent basis between
    # maker and taker exchanges. basis_bps > 0 (maker above taker) favours SELL.
    # Strength is in absolute Decimal applied per bp of basis. Set 0 to disable.
    basis_skew_strength: Decimal = Field(default=Decimal("0.00002"))
    # Taker-side balance gate as a fraction of the maker order's hedge cost.
    #   Required taker balance = order_amount * taker_mid * (1 + taker_hedge_buffer_pct)
    # Examples (order ≈ 78 BRL):
    #   0.01  → 1% buffer (covers fee + slippage on a single hedge)  ≈ 78.78 BRL needed
    #   1.01  → buffer of one extra order + 1% (covers two consecutive fills before
    #           any opposite-side BUY hedge replenishes the balance)
    #   2.01  → buffer of two extra orders + 1% (three consecutive fills)
    # This scales automatically with order_amount and price; replaces the previous
    # absolute thresholds (min_taker_base_for_sell_hedge / min_taker_quote_for_buy_hedge)
    # which had to be retuned every time order_amount or price moved.
    taker_hedge_buffer_pct: Decimal = Field(default=Decimal("0.01"))

    # === Warmup ===
    warmup_seconds: float = Field(default=20.0)

    # === Circuit breakers (PnL-based) ===
    # All limits below are expressed in QUOTE currency (BRL for BTC-BRL pair).
    # They are positive numbers; the gate compares against -limit for losses.
    max_consecutive_hedge_failures: int = Field(default=3)
    max_daily_loss_quote: Decimal = Field(default=Decimal("100"))
    # Session drawdown: kill when (session_peak - current_session_pnl) >= this.
    # Captures "won early, gave it back" patterns that net daily PnL hides.
    # Resets only on process restart — peak is per-process, not per-day.
    max_session_drawdown_quote: Decimal = Field(default=Decimal("50"))
    # Consecutive closed executors with net_pnl_quote < 0. Catches adverse-selection
    # clusters or signal inversion before total loss reaches daily limit.
    max_consecutive_losing_fills: int = Field(default=5)
    # Rolling 1h burn rate: kill when sum(net_pnl) over last 3600s <= -limit.
    # Detects slow bleeds that would not trip daily_loss until hours later.
    max_hourly_burn_quote: Decimal = Field(default=Decimal("50"))
    # Unrealized-PnL proxy via inventory drift in quote terms (sum across base
    # assets of |actual-target| * mid_price). Catches large open exposure even
    # when realized PnL still looks fine. Should be > inventory_audit.max_drift_quote.
    max_unrealized_loss_quote: Decimal = Field(default=Decimal("100"))
    # When KILLED and there are no active executors and inventory is in tolerance,
    # the controller sends SIGTERM to its own process so the supervisor (or operator)
    # can restart cleanly. Opt-in to avoid surprising shutdowns in shadow_mode.
    auto_terminate_on_kill: bool = Field(default=False)
    # Seconds to wait after KILL latches before auto_terminate fires (gives
    # graceful cancel + audit one cycle each).
    auto_terminate_grace_sec: float = Field(default=30.0)

    # === Inventory audit (state-based reconciliation) ===
    inventory_audit: InventoryAuditConfig = Field(default_factory=InventoryAuditConfig)

    # === Operational ===
    shadow_mode: bool = Field(default=True)
    log_dir: str = Field(default="logs/xemm_lead_lag")
    kill_switch_file: Optional[str] = Field(default=None)

    @field_validator("lead_windows_seconds", mode="before")
    @classmethod
    def _parse_lead_windows(cls, v):
        # Accept "5,10,15" or [5, 10, 15]
        if isinstance(v, str):
            return [int(x.strip()) for x in v.split(",") if x.strip()]
        return v

    @model_validator(mode="after")
    def _validate_profitability_order(self):
        if not (self.min_profitability <= self.target_profitability <= self.max_profitability):
            raise ValueError(
                f"Require min_profitability ({self.min_profitability}) "
                f"<= target_profitability ({self.target_profitability}) "
                f"<= max_profitability ({self.max_profitability})"
            )
        if self.warmup_seconds < max(self.lead_windows_seconds) + 5:
            raise ValueError(
                f"warmup_seconds ({self.warmup_seconds}) must be at least "
                f"max(lead_windows_seconds)+5 = {max(self.lead_windows_seconds) + 5}"
            )
        return self

    @model_validator(mode="after")
    def _validate_at_least_one_mode(self):
        if not self.enable_market_making and not self.enable_pure_arb:
            raise ValueError(
                "At least one of enable_market_making or enable_pure_arb must be True"
            )
        if self.arb_capital_strategy not in ("skip_if_insufficient", "cancel_xemm_to_free"):
            raise ValueError(
                f"arb_capital_strategy must be 'skip_if_insufficient' or 'cancel_xemm_to_free', "
                f"got '{self.arb_capital_strategy}'"
            )
        if self.arb_unwind_strategy not in ("abort_and_alert", "force_unwind"):
            raise ValueError(
                f"arb_unwind_strategy must be 'abort_and_alert' or 'force_unwind', "
                f"got '{self.arb_unwind_strategy}'"
            )
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for connector, pair in [
            (self.maker_connector, self.maker_trading_pair),
            (self.taker_connector, self.taker_trading_pair),
            (self.signal_connector, self.signal_base_pair),
            (self.signal_connector, self.signal_fx_pair),
        ]:
            if connector not in markets:
                markets[connector] = set()
            markets[connector].add(pair)
        return markets


class XEMMLeadLagCSVLogger:
    """Line-buffered CSV writer for one row per controller tick."""
    COLUMNS = [
        "timestamp", "iso_time", "regime", "signal_quality",
        # Local L1 (maker exchange)
        "local_bid", "local_ask", "local_mid", "local_spread_bps",
        # Taker L1 (Binance BTC-BRL)
        "taker_local_bid", "taker_local_ask", "taker_local_mid",
        "taker_local_spread_bps",
        # Leader L1 (BTC-USDT)
        "leader_bid", "leader_ask", "leader_mid",
        # FX L1 (USDT-BRL)
        "fx_bid", "fx_ask", "fx_mid_raw", "fx_mid_ema",
        # Synthetic fairs
        "fair_brl_fast", "fair_brl_slow",
        # Bps relations
        "basis_bps", "maker_vs_fair_bps", "taker_vs_fair_bps", "maker_vs_taker_bps",
        # Lead signals
        "lead_5s", "lead_10s", "lead_15s", "best_lead_bps",
        # Decision
        "should_cancel", "cancel_reason",
        # Inventory per exchange
        "maker_base", "maker_quote", "taker_base", "taker_quote",
        "combined_base", "combined_quote", "combined_pct", "inventory_skew",
        # Adjusted targets
        "target_prof_buy", "target_prof_sell",
        # Active state
        "n_active_executors", "shadow_mode",
        # Pure-arb telemetry (VWAP detector + circuit breakers)
        "arb_long_gross_bps", "arb_short_gross_bps",
        "arb_long_net_bps", "arb_short_net_bps", "arb_tx_cost_bps",
        "arb_near_threshold",
        "arb_failures_today", "arb_realized_loss_today",
        # Inventory audit (state-based reconciliation)
        "audit_btc_actual", "audit_btc_target", "audit_btc_delta",
        "audit_drift_active", "audit_inflight_active",
        # Boot state
        "boot_paused",
        # PnL-safety telemetry (drives circuit breakers and external watchdog)
        "daily_realized_pnl", "session_pnl_total", "session_pnl_peak",
        "session_drawdown", "consecutive_losing_fills", "hourly_burn",
    ]

    def __init__(self, log_dir: str, controller_id: str):
        os.makedirs(log_dir, exist_ok=True)
        ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_id = controller_id.replace("/", "_").replace(" ", "_")
        self.path = os.path.join(log_dir, f"xemm_lead_lag_{safe_id}_{ts_str}.csv")
        self._file = open(self.path, "w", newline="", buffering=1)
        self._writer = csv.DictWriter(
            self._file, fieldnames=self.COLUMNS, extrasaction="ignore")
        self._writer.writeheader()

    def log(self, processed_data: dict) -> None:
        row = {col: self._format(processed_data.get(col)) for col in self.COLUMNS}
        # Flatten lead_bps_per_window dict.
        lead_dict = processed_data.get("lead_bps_per_window") or {}
        for w in (5, 10, 15):
            row[f"lead_{w}s"] = self._format(lead_dict.get(w))
        try:
            self._writer.writerow(row)
        except Exception:
            # Never let CSV failure crash the strategy.
            pass

    @staticmethod
    def _format(v):
        if v is None:
            return ""
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, Enum):
            return v.value if isinstance(v.value, str) else str(v.value)
        return v

    def close(self):
        try:
            self._file.flush()
            self._file.close()
        except Exception:
            pass


class TradeLedger:
    """Minimalist trade ledger for fast external monitoring.

    Writes three artefacts to ``log_dir`` on every completed executor that
    actually traded (filled_amount_quote != 0):

      * ``trades.jsonl``    — append-only audit trail, one JSON line per trade.
                              Each line is self-contained and includes a
                              monotonic ``seq`` so external tooling can
                              detect new entries by counting lines.
      * ``state.json``      — atomic snapshot dashboard (write-to-tmp + rename)
                              with cumulative counters and a copy of the most
                              recent trade. Cheapest possible "what's the
                              current state of trading?" check.
      * ``last_fill.touch`` — empty file whose mtime is updated on each fill.
                              Filesystem-native heartbeat — ``stat -c %Y`` is
                              the fastest possible "did anything happen?"
                              probe.

    Design constraints (deliberate, to keep the surface small):
      * No anomaly classification, no rotation, no PnL recomputation. The
        controller already knows ``net_pnl_quote`` from ExecutorInfo; we just
        persist what we already have.
      * No daily-bucket persistence. ``trades_today`` and ``pnl_today_brl``
        are computed in-memory from the day-of-process-start; on restart they
        reset. trades.jsonl is the source of truth for true historical sums.
      * All file I/O is wrapped in try/except — a disk error must never
        crash the strategy.
    """

    def __init__(self, log_dir: str, quote_asset: str = "BRL"):
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._quote = quote_asset
        self._jsonl_path = os.path.join(log_dir, "trades.jsonl")
        self._state_path = os.path.join(log_dir, "state.json")
        self._touch_path = os.path.join(log_dir, "last_fill.touch")
        # Counters survive only for the current process — restart resets them.
        # trades.jsonl is the durable record; state.json is a session snapshot.
        self._seq = 0
        self._pnl_session = Decimal("0")
        self._pnl_today = Decimal("0")
        self._trades_today = 0
        self._today = self._utc_today()

    @staticmethod
    def _utc_today() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    @staticmethod
    def _iso_now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _to_jsonable(v):
        # Decimals → str (preserves precision), Enums → .value, leave the rest.
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, Enum):
            return v.value if isinstance(v.value, (str, int, float)) else str(v.value)
        return v

    def record_raw_fill(self, event, source_connector: str) -> None:
        """Persist an individual ``OrderFilledEvent`` as a fine-grained record.

        This complements ``record_fill`` (which aggregates at executor close).
        Raw fills are written immediately as the connector emits them, so the
        ledger captures fills even when:

          * the order is partial-filled then cancelled (executor never closes
            with ``filled_amount_quote > 0`` because the partial flowed
            through ``_all_trade_updates_for_order`` outside the executor's
            sync path);
          * the fill arrives via a ``MARKET`` rebalance order placed by
            ``inventory_audit`` (no executor at all);
          * the executor's poll-based detection misses the ``is_done``
            transition due to a race with the controller tick.

        Records are tagged with ``kind: "fill"`` so they're easy to
        distinguish from ``kind: "trade"`` (the executor-level summary).
        ``state.json`` is rewritten on every raw fill — the snapshot's
        ``last_trade`` always reflects the most recent activity.
        """
        try:
            amount = Decimal(str(getattr(event, "amount", 0)))
            if amount == 0:
                return  # nothing to record

            price = Decimal(str(getattr(event, "price", 0)))
            quote_amount = amount * price

            today = self._utc_today()
            if today != self._today:
                self._today = today
                self._pnl_today = Decimal("0")
                self._trades_today = 0

            self._seq += 1
            # We do not have realised PnL on a per-fill basis (that is only
            # known once the maker+taker pair completes). Counters reflect
            # fill volume, not net PnL — see ``record_fill`` for PnL totals.

            trade_type = getattr(event, "trade_type", None)
            if hasattr(trade_type, "name"):
                trade_type = trade_type.name
            order_type = getattr(event, "order_type", None)
            if hasattr(order_type, "name"):
                order_type = order_type.name

            record = {
                "ts": self._iso_now(),
                "seq": self._seq,
                "kind": "fill",
                "source_connector": source_connector,
                "trading_pair": getattr(event, "trading_pair", None),
                "side": trade_type,
                "order_type": order_type,
                "client_order_id": getattr(event, "order_id", None),
                "exchange_order_id": getattr(event, "exchange_order_id", None),
                "exchange_trade_id": getattr(event, "exchange_trade_id", None),
                "fill_price": self._to_jsonable(price),
                "fill_amount_base": self._to_jsonable(amount),
                "fill_amount_quote": self._to_jsonable(quote_amount),
                "quote_asset": self._quote,
            }
            self._append_jsonl(record)
            self._write_state(record)
            self._touch()
        except Exception as e:
            try:
                import logging
                logging.getLogger(__name__).error(
                    f"[trade_ledger] failed to record raw fill: {type(e).__name__}: {e}"
                )
            except Exception:
                pass

    def record_fill(self, ex) -> None:
        """Persist a completed executor as a trade record.

        ``ex`` is an ExecutorInfo (Pydantic model). Called from the
        controller's existing fill-detection loop; we do nothing if the
        executor produced no fills.
        """
        try:
            # Guard: only record executors that actually traded.
            filled = getattr(ex, "filled_amount_quote", Decimal("0")) or Decimal("0")
            if not filled:
                return

            # Roll daily bucket if the UTC date crossed.
            today = self._utc_today()
            if today != self._today:
                self._today = today
                self._pnl_today = Decimal("0")
                self._trades_today = 0

            self._seq += 1
            net_pnl = getattr(ex, "net_pnl_quote", Decimal("0")) or Decimal("0")
            cum_fees = getattr(ex, "cum_fees_quote", Decimal("0")) or Decimal("0")
            self._pnl_session += net_pnl
            self._pnl_today += net_pnl
            self._trades_today += 1

            ci = getattr(ex, "custom_info", {}) or {}
            side = ci.get("side")
            if hasattr(side, "name"):  # TradeType enum
                side = side.name

            # Task 4.3: compute slippage_bps from the expected vs realised
            # taker price. ``taker_expected_price`` is the ``_taker_result_price``
            # at hedge placement; ``filled_amount_quote / order_amount`` ≈ avg
            # taker fill price for a MARKET order. We treat slippage as
            # negative when the taker filled WORSE than expected (paying more
            # to BUY or receiving less for SELL).
            slippage_bps = None
            taker_expected = ci.get("taker_expected_price")
            order_amount = ci.get("order_amount")
            try:
                if (taker_expected is not None and order_amount
                        and Decimal(str(taker_expected)) > 0
                        and Decimal(str(order_amount)) > 0):
                    expected = Decimal(str(taker_expected))
                    # Best-effort actual: filled_amount_quote / order_amount
                    # gives the average taker fill price when the executor
                    # fully hedged. For partial fills this is still a good
                    # proxy because the taker MARKET runs against the same
                    # book level for similar sizes.
                    actual = Decimal(str(filled)) / Decimal(str(order_amount))
                    # ``side`` here is the maker side. Taker side is the
                    # opposite — if maker is BUY we expected a taker SELL,
                    # so worse-than-expected means actual < expected.
                    if side == "BUY":
                        # Maker BUY → taker SELL: lower actual price = worse
                        slippage_bps = float(
                            (actual - expected) / expected * Decimal("10000")
                        )
                    else:
                        # Maker SELL → taker BUY: higher actual price = worse
                        slippage_bps = float(
                            (expected - actual) / expected * Decimal("10000")
                        )
                    # Round to 2 decimals — bps with sub-bps precision is noise.
                    slippage_bps = round(slippage_bps, 2)
            except Exception:
                slippage_bps = None  # don't let a math edge-case fail the record

            record = {
                "ts": self._iso_now(),
                "seq": self._seq,
                "kind": "trade",
                "executor_id": getattr(ex, "id", None),
                "side": side,
                "trading_pair": ci.get("maker_trading_pair") or getattr(ex, "trading_pair", None),
                "maker_connector": ci.get("maker_connector"),
                "taker_connector": ci.get("taker_connector"),
                "filled_amount_quote": self._to_jsonable(filled),
                "net_pnl_quote": self._to_jsonable(net_pnl),
                "cum_fees_quote": self._to_jsonable(cum_fees),
                "close_type": self._to_jsonable(getattr(ex, "close_type", None)),
                "close_timestamp": getattr(ex, "close_timestamp", None),
                "quote_asset": self._quote,
                # Task 3.4 + 4.3: latency and slippage telemetry
                "fill_to_hedge_latency_ms": ci.get("fill_to_hedge_latency_ms"),
                "taker_expected_price": self._to_jsonable(taker_expected),
                "slippage_bps": slippage_bps,
            }
            self._append_jsonl(record)
            self._write_state(record)
            self._touch()
        except Exception as e:
            # Never let ledger I/O crash the strategy.
            try:
                from hummingbot.logger import HummingbotLogger  # noqa: F401
                import logging
                logging.getLogger(__name__).error(
                    f"[trade_ledger] failed to record fill: {type(e).__name__}: {e}"
                )
            except Exception:
                pass

    def _append_jsonl(self, record: dict) -> None:
        # Append + fsync so a kill -9 doesn't lose the line.
        line = json.dumps(record, separators=(",", ":")) + "\n"
        with open(self._jsonl_path, "a", buffering=1) as f:
            f.write(line)
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass

    def _write_state(self, last_record: dict) -> None:
        # Atomic overwrite: write to .tmp then rename. Readers always see
        # either the previous full file or the new full file — never half.
        state = {
            "updated_at": self._iso_now(),
            "trades_total_session": self._seq,
            "trades_today": self._trades_today,
            "pnl_today": self._to_jsonable(self._pnl_today),
            "pnl_session": self._to_jsonable(self._pnl_session),
            "quote_asset": self._quote,
            "last_trade": last_record,
        }
        tmp = self._state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, self._state_path)

    def _touch(self) -> None:
        # Empty file; only mtime matters. Idempotent.
        try:
            with open(self._touch_path, "a"):
                os.utime(self._touch_path, None)
        except Exception:
            pass


class XEMMLeadLagController(ControllerBase):
    """XEMM controller with synthetic lead-lag signal."""

    def __init__(self, config: XEMMLeadLagConfig, *args, **kwargs):
        self.config = config
        # Priority 3: 200ms tier-1 responsiveness for cancel/regime/arb decisions.
        # The fingerprint check in update_processed_data prevents redundant work
        # when the book hasn't changed; CSV writes are throttled to 1s separately.
        if "update_interval" not in kwargs:
            kwargs["update_interval"] = 0.2
        super().__init__(config, *args, **kwargs)
        self._signal = LeadLagSignalProvider(
            lead_windows_sec=config.lead_windows_seconds,
            ema_alpha_fx=config.ema_alpha_fx,
            max_leader_staleness_sec=config.max_leader_staleness_sec,
            max_fx_staleness_sec=config.max_fx_staleness_sec,
            max_local_staleness_sec=config.max_local_staleness_sec,
        )
        # Logger; tolerant to dir creation failures.
        try:
            self._csv: Optional[XEMMLeadLagCSVLogger] = XEMMLeadLagCSVLogger(
                config.log_dir, config.id
            )
        except Exception as e:
            self.logger().error(f"Failed to init CSV logger: {e}")
            self._csv = None

        # Trade ledger — tiny, fast-readable record of completed trades.
        # See the TradeLedger class docstring for the file layout.
        try:
            quote = config.maker_trading_pair.split("-")[-1] if config.maker_trading_pair else "BRL"
            self._trade_ledger: Optional[TradeLedger] = TradeLedger(
                config.log_dir, quote_asset=quote
            )
        except Exception as e:
            self.logger().error(f"Failed to init trade ledger: {e}")
            self._trade_ledger = None

        self._started_at: Optional[float] = None
        self._last_action_time: float = 0.0
        self._last_balance_warn_ts: float = 0.0
        self._last_cancel_reason: Optional[str] = None
        self._consecutive_hedge_failures: int = 0
        self._daily_realized_pnl: Decimal = Decimal("0")
        self._kill_reason: Optional[str] = None

        # === Ghost-fill controller-level handling ===
        # The XEMMLeadLagExecutor used to track potentially-filled cancelled
        # maker orders in a local set. Problem: when BitPreco's CANT_CANCEL
        # _FILLED_ORDER race leaves a delayed OrderFilledEvent (5–30 s after
        # cancel), the original executor has usually terminated already, so
        # its handler never fires and the maker fill ends up un-hedged —
        # `inventory_audit` only catches it minutes later via slower MARKET
        # rebalance on the WRONG side (no cross-exchange edge).
        # Observed 2026-05-11 14:11:31: 3 maker fills in 25 min, 0 taker
        # hedges placed via the XEMM cycle.
        # Fix: register pending ghosts here (controller lives across executor
        # cycles); the controller's fill-event listener hedges late fills
        # directly via the taker connector.
        self._pending_ghost_orders: Dict[str, Dict[str, Any]] = {}
        # Order IDs that an executor already hedged (defensive deduplication
        # — controller's listener checks this before firing its own hedge).
        self._already_hedged_ghost_ids: Set[str] = set()
        # BitPreco confirms cancel within ~30 s; 60 s buffer avoids
        # purging entries while a real late-fill event is still in flight.
        self._ghost_max_age_sec: float = 60.0

        # === Layered PnL safety state (see _compute_regime gates) ===
        # Session-level: persists for the process lifetime.
        self._session_pnl_total: Decimal = Decimal("0")
        self._session_pnl_peak: Decimal = Decimal("0")
        # Counts how many closed executors in a row had net_pnl_quote < 0.
        # Resets on the first non-loss fill.
        self._consecutive_losing_fills: int = 0
        # Sliding 1h history of (timestamp, net_pnl_quote) tuples used by the
        # hourly burn-rate gate. Trimmed lazily on each access in _hourly_burn.
        self._hourly_pnl_history: Deque[Tuple[float, Decimal]] = deque()
        # Day key (UTC) for the daily_realized_pnl reset — separate from
        # _arb_last_reset_day so the two reset paths are independent.
        self._daily_pnl_last_reset_day: str = ""
        # Set when KILLED has latched; the auto-terminate path uses this to
        # know when the grace period started.
        self._kill_latched_at: Optional[float] = None
        # Latched when auto-terminate has fired (single-shot guard).
        self._auto_terminated: bool = False

        # === Tiered polling state ===
        self._last_fingerprint: Optional[tuple] = None
        self._last_signal_update: float = 0.0
        self._last_full_update: float = 0.0
        self._balance_version: int = 0  # incremented on fill events

        # === Watchdog: detect event loop lag (e.g. MQTT-style asyncio blocking) ===
        # Tracks wall-clock time of last successful tick. If a tick observes a gap
        # > _watchdog_threshold_sec since the previous one, the event loop was
        # blocked for that long — we log CRITICAL and trigger the kill switch
        # gracefully rather than letting the backlog grow unbounded.
        self._last_tick_wall_time: float = 0.0
        self._watchdog_threshold_sec: float = 10.0
        self._watchdog_tripped: bool = False

        # === Startup cleanup state ===
        # True once we have attempted to cancel orphaned exchange orders.
        # Guards against pkill-induced orphaned orders consuming available_balance
        # and causing 100% "Not enough budget" failures on bot restart.
        self._startup_cleanup_done: bool = False

        # === Arb circuit breaker state ===
        self._last_arb_time: float = 0.0
        self._arb_history: List[float] = []   # timestamps within sliding 1h window
        # Spawn-race fix (2026-05-11): _has_active_arb_executor checks
        # self.executors_info which is updated ASYNCHRONOUSLY by the
        # framework after CreateExecutorAction is dispatched. Between
        # dispatching the action and the executor appearing in
        # executors_info, multiple ticks can pass — 10 arbs spawned in 47 s
        # at the 5-s cooldown boundary because none had registered yet.
        # ``_arb_spawn_pending_until`` is set to ``now + 30 s`` when we
        # return a CreateExecutorAction, and cleared the moment we observe
        # the resulting executor in ``executors_info`` (i.e. inside
        # ``_has_active_arb_executor`` returning True). The 30-s ceiling
        # guards against the rare case where an executor fails to register
        # at all — we'd lose at most one spawn window, not block forever.
        self._arb_spawn_pending_until: float = 0.0
        self._arb_paused_until: float = 0.0
        self._arb_failures_today: int = 0
        self._arb_realized_loss_today: Decimal = Decimal("0")
        self._arb_last_reset_day: str = ""
        self._arb_last_alert_time: float = 0.0  # throttle: max 1 log line per 30s
        # Throttle the per-tick "why didn't an arb spawn?" debug logs
        # (every gate path in _maybe_create_arb_action) — 5s window keeps
        # them visible without flooding.
        self._arb_last_gate_log_ts: float = 0.0

        # === Fresh-REST refresh for BitPreco-side arb VWAP ===
        # BitPreco's order book "WS" is REST-polled (~500ms cadence, see
        # bitpreco_api_order_book_data_source.py: listen_for_subscriptions),
        # so the cached OrderBook used by `_compute_arb_gross_bps` can be
        # 500-1000ms stale. When the cached net_bps is close to threshold
        # (within `_fresh_arb_trigger_margin_bps`), we override with a fresh
        # REST snapshot. TTL prevents duplicate fetches within one tick.
        self._fresh_arb_cached_at: float = 0.0
        self._fresh_arb_long_net_bps: Optional[Decimal] = None
        self._fresh_arb_short_net_bps: Optional[Decimal] = None
        self._fresh_arb_cache_ttl_sec: float = 0.2
        # Margin (bps) below threshold to trigger fresh fetch. 10 bps covers
        # the typical staleness slippage we observed (~11 bps on 0.0002 BTC).
        self._fresh_arb_trigger_margin_bps: Decimal = Decimal("10")

        # === Inventory audit state (state-based reconciliation) ===
        # Audit compares (maker_balance + taker_balance) against config target
        # for each base asset every audit_interval_sec. Drift action only fires
        # when no in-flight trade activity could explain the imbalance.
        self._last_audit_time: float = 0.0
        self._last_audit_results: Dict[str, Dict[str, Decimal]] = {}
        # Set when an executor transitions to is_done with non-zero fill —
        # gives the audit a 10s grace window for WS account-stream latency.
        self._last_fill_time: float = 0.0
        # Tracks which executor IDs have already been seen as "done" so we
        # detect transitions exactly once (also bumps _balance_version for
        # the fingerprint).
        self._known_done_executor_ids: Set[str] = set()
        # auto_rebalance: assets queued for corrective MARKET order (asset → delta).
        # Populated by _run_inventory_audit, consumed by _execute_pending_rebalances.
        self._pending_rebalances: Dict[str, Decimal] = {}
        # Cooldown per asset: timestamp of last rebalance order placed.
        self._rebalance_last_time: Dict[str, float] = {}
        # Minimum seconds between rebalance orders for the same asset.
        self._rebalance_cooldown_sec: float = 120.0
        # === Barrier-pattern audit state (2026-05-12 simplification) ===
        # Replaces _drift_consecutive_audits/HEDGE_FAILURES_N strike-rule and
        # the audit-in-killed-mode complexity. State machine:
        #   IDLE         — normal: each tick, audit refreshes balance, checks
        #                  drift; if drift suspected, transitions to CANCELLING.
        #   CANCELLING   — audit requested stop-the-world; determine_executor_
        #                  actions emits StopExecutorAction for ALL active
        #                  executors. Next tick, audit checks quiescence
        #                  (no active executors + no in-flight orders on
        #                  either connector).
        #   VALIDATING   — quiescent reached; audit re-reads balance
        #                  authoritatively. Drift confirmed → queue rebalance.
        #                  Drift cleared (false alarm / race) → return to IDLE.
        # Eliminates the entire class of false-positive drifts caused by
        # stale balance / hedge-in-flight / arb-leg-in-flight races.
        self._barrier_state: str = "IDLE"
        self._barrier_started_at: float = 0.0
        self._barrier_timeout_sec: float = 5.0
        # Snapshot from the phase-1 audit that triggered the barrier. Kept
        # for log/forensic correlation with the post-barrier outcome.
        self._barrier_drift_snapshot: Dict[str, Dict[str, Decimal]] = {}

        # === Orphan order reconciliation ===
        # Periodic check (every _orphan_check_interval_sec) that compares the
        # exchange's open-orders list against the connector's in_flight_orders
        # tracker. Anything on the exchange that the bot doesn't know about is
        # an orphan and gets cancelled immediately. Acts as a safety net for
        # races between cancel/place cycles or unconfirmed cancels.
        self._last_orphan_check_time: float = 0.0
        # Throttle: log orphan_check only when state changes or every 60s for
        # liveness. Saves ~50 lines/min when the system is idle.
        self._last_orphan_log_signature: Optional[tuple] = None
        self._last_orphan_log_ts: float = 0.0
        # Tightened from 30s → 10s to shrink the worst-case exposure window
        # when the executor's throttled cancel retries fail to clear an order.
        # Combined with the executor retry every 3s, this gives 3 retry
        # attempts (t≈1,4,7s) before orphan_check kicks in at t≈10s.
        self._orphan_check_interval_sec: float = 10.0
        # Minimum age (seconds) before a missing-from-tracker order is
        # considered orphan. Protects against the race where a freshly placed
        # order shows up on the exchange (open_orders) before the connector's
        # ack updates its in_flight_orders tracker. Combined with the
        # double-snapshot guard in _run_orphan_check, this eliminates the
        # false-positive that was killing legitimate orders within ~500ms of
        # placement. 3s is far longer than any observed placement→tracker lag
        # (typically <500ms) but short enough to react quickly to true orphans.
        self._orphan_min_age_sec: float = 3.0
        # Total orphans cancelled (cumulative, for telemetry).
        self._orphans_cancelled_total: int = 0

        # === Boot-paused mode (Solution C) ===
        # Bot starts in PAUSED state — only transitions to OK after:
        #   1. startup_cleanup completes (all orphan orders cancelled)
        #   2. initial inventory audit shows zero drift
        # If audit fails on boot, regime stays at KILLED (manual intervention).
        self._boot_paused: bool = True
        self._initial_audit_done: bool = False

        # === SIGTERM / SIGINT graceful shutdown ===
        # Registered on the first tick of update_processed_data() (not in __init__)
        # so the asyncio event loop is guaranteed to be running.
        self._sigterm_registered: bool = False

    # ------------------------------------------------------------------ #
    # Startup / shutdown helpers                                        #
    # ------------------------------------------------------------------ #
    def _attach_ledger_fill_listeners(self) -> None:
        """Subscribe ``TradeLedger.record_raw_fill`` to OrderFilled events
        on both maker and taker connectors.

        We hold a reference to the forwarders on ``self`` so they survive the
        method scope (Hummingbot listeners are weak-referenced via the
        connector's internal event-emitter book-keeping). One forwarder per
        connector keeps the source labelling clean.
        """
        self._ledger_fill_forwarders = {}
        for conn_name in (self.config.maker_connector, self.config.taker_connector):
            if not conn_name:
                continue
            try:
                conn = self.market_data_provider.get_connector(conn_name)
            except Exception:
                continue
            # Bind the connector name into the forwarded callback so the
            # ledger record knows which exchange the fill came from.
            # SourceInfoEventForwarder calls back with
            # (event_tag, event_caller, event); we only need ``event``.
            # Two responsibilities on every fill:
            #   1. Persist to the trade ledger.
            #   2. Hedge the order if it was registered as a ghost
            #      (BitPreco delayed-fill race) — the original executor
            #      may already be dead, so the controller must own this.
            def _on_fill(_tag, _caller, event, _src=conn_name):
                # Touch _last_fill_time FIRST. The 10-s inflight window now
                # covers any fill — XEMM closes, ghost late fills, arb legs
                # mid-execution, even auto_rebalance MARKET fills — not just
                # executor-close transitions. Prevents the race observed in
                # 2026-05-11 16:50:46 where the inventory_audit ran 1 s after
                # an arb's leg-1 fill, saw the transient drift, and queued a
                # MARKET SELL that collided with the arb's own leg-2 SELL.
                self._last_fill_time = time.time()
                if self._trade_ledger is not None:
                    try:
                        self._trade_ledger.record_raw_fill(event, source_connector=_src)
                    except Exception as e:
                        self.logger().warning(
                            f"[trade_ledger] record_raw_fill failed: {type(e).__name__}: {e}"
                        )
                try:
                    self._handle_ghost_fill_event(event, _src)
                except Exception as e:
                    self.logger().warning(
                        f"[ghost_controller] handler raised: {type(e).__name__}: {e}"
                    )
            forwarder = SourceInfoEventForwarder(_on_fill)
            conn.add_listener(MarketEvent.OrderFilled, forwarder)
            self._ledger_fill_forwarders[conn_name] = forwarder
        self.logger().info(
            f"[trade_ledger] attached OrderFilled listeners on "
            f"{list(self._ledger_fill_forwarders.keys())}"
        )

    async def _cancel_all_open_orders_on_startup(self) -> None:
        """
        Cancel ALL open orders on maker and taker exchanges via direct REST API.

        Querying the exchange directly — not Hummingbot's SQLite-persisted
        in_flight_orders — guarantees we see every orphaned order regardless of
        whether a previous session crashed before recording it to the database.

        Runs both exchanges in parallel; errors on one do not abort the other.
        """
        tasks = []
        seen: set = set()
        for connector_name, trading_pair in [
            (self.config.maker_connector, self.config.maker_trading_pair),
            (self.config.taker_connector, self.config.taker_trading_pair),
        ]:
            key = (connector_name, trading_pair)
            if key in seen:
                continue
            seen.add(key)
            tasks.append(self._cancel_exchange_open_orders(connector_name, trading_pair))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (connector_name, trading_pair), result in zip(seen, results):
            if isinstance(result, Exception):
                self.logger().error(
                    f"[startup_cleanup] {connector_name}/{trading_pair} raised: {result}"
                )

    async def _cancel_exchange_open_orders(self, connector_name: str, trading_pair: str) -> None:
        """
        Query the exchange REST API for open orders on `trading_pair` and cancel each.
        Dispatches to exchange-specific helpers; falls back to in_flight_orders for
        unknown connectors.
        """
        try:
            connector = self.market_data_provider.get_connector(connector_name)
            exchange_symbol = await connector.exchange_symbol_associated_to_pair(
                trading_pair=trading_pair
            )
        except Exception as e:
            self.logger().warning(
                f"[startup_cleanup] Cannot resolve connector/symbol for "
                f"{connector_name}/{trading_pair}: {e}"
            )
            return

        if connector_name == "bybit":
            await self._bybit_cancel_open_orders(connector, exchange_symbol, trading_pair)
        elif connector_name == "binance":
            await self._binance_cancel_open_orders(connector, exchange_symbol, trading_pair)
        elif connector_name == "bitpreco":
            await self._bitpreco_cancel_open_orders(connector, exchange_symbol, trading_pair)
        else:
            # Fallback: cancel orders Hummingbot knows about (less reliable but universal)
            self.logger().warning(
                f"[startup_cleanup] No REST cancel implementation for {connector_name}. "
                f"Using in_flight_orders fallback."
            )
            orders = [
                o for o in connector.in_flight_orders.values()
                if o.trading_pair == trading_pair and not o.is_done
            ]
            if orders:
                for o in orders:
                    connector.cancel(o.trading_pair, o.client_order_id)
                self.logger().warning(
                    f"[startup_cleanup] {connector_name}/{trading_pair}: "
                    f"queued cancel for {len(orders)} tracked order(s)."
                )
            else:
                self.logger().info(
                    f"[startup_cleanup] {connector_name}/{trading_pair}: "
                    f"no tracked orders — clean start."
                )

    async def _bybit_cancel_open_orders(
        self, connector, exchange_symbol: str, trading_pair: str
    ) -> None:
        """
        GET /v5/order/realtime → cancel each open order on Bybit via REST.
        Uses the connector's own authenticated _api_get/_api_post to stay within
        the existing rate-limiter and auth machinery.
        """
        try:
            result = await connector._api_get(
                path_url="/v5/order/realtime",
                params={
                    "category": connector._category,
                    "symbol": exchange_symbol,
                    "orderStatus": "New",
                    "limit": 50,
                },
                is_auth_required=True,
                limit_id="/v5/order/realtime",
            )
            orders = result.get("result", {}).get("list", [])
        except Exception as e:
            self.logger().error(
                f"[startup_cleanup] bybit/{trading_pair}: GET open orders failed: {e}"
            )
            return

        if not orders:
            self.logger().info(
                f"[startup_cleanup] bybit/{trading_pair}: no open orders — clean start."
            )
            return

        self.logger().warning(
            f"[startup_cleanup] bybit/{trading_pair}: "
            f"found {len(orders)} open order(s) — cancelling..."
        )
        for order in orders:
            try:
                await connector._api_post(
                    path_url="/v5/order/cancel",
                    data={
                        "category": connector._category,
                        "symbol": exchange_symbol,
                        "orderId": order["orderId"],
                    },
                    is_auth_required=True,
                    headers={"referer": "Hummingbot"},
                )
                self.logger().warning(
                    f"[startup_cleanup] bybit/{trading_pair}: cancelled "
                    f"orderId={order['orderId']} link={order.get('orderLinkId', '?')}"
                )
            except Exception as e:
                self.logger().error(
                    f"[startup_cleanup] bybit/{trading_pair}: failed to cancel "
                    f"orderId={order['orderId']}: {e}"
                )

    async def _binance_cancel_open_orders(
        self, connector, exchange_symbol: str, trading_pair: str
    ) -> None:
        """
        DELETE /openOrders — Binance batch cancel-all for the symbol.

        Uses a single REST call instead of GET + N deletes.
        Returns the list of cancelled orders (empty list = nothing to cancel).

        IMPORTANT: limit_id must be "/order" (ORDER_PATH_URL in binance_constants).
        Binance's AsyncThrottler crashes with 'NoneType has no attribute weight'
        when the limit_id is not in the rate-limit registry. "/openOrders" is not
        registered; "/order" is, and carries the same order-weight semantics.
        """
        try:
            result = await connector._api_delete(
                path_url="/openOrders",
                params={"symbol": exchange_symbol},
                is_auth_required=True,
                limit_id="/order",   # registered rate-limit key for order operations
            )
            orders = result if isinstance(result, list) else []
        except Exception as e:
            # Binance returns HTTP 400 code -2011 ("Unknown order sent") when
            # DELETE /openOrders finds nothing to cancel — treat as clean start.
            err = str(e)
            if "-2011" in err or "Unknown order" in err:
                self.logger().info(
                    f"[startup_cleanup] binance/{trading_pair}: no open orders — clean start."
                )
            else:
                self.logger().error(
                    f"[startup_cleanup] binance/{trading_pair}: DELETE /openOrders failed: {e}"
                )
            return

        if not orders:
            self.logger().info(
                f"[startup_cleanup] binance/{trading_pair}: no open orders — clean start."
            )
        else:
            for order in orders:
                self.logger().warning(
                    f"[startup_cleanup] binance/{trading_pair}: cancelled "
                    f"orderId={order.get('orderId')} client={order.get('clientOrderId', '?')}"
                )

    async def _bitpreco_cancel_open_orders(
        self, connector, exchange_symbol: str, trading_pair: str
    ) -> None:
        """
        POST /v1/trading/all_orders_cancel — BitPreco's batch cancel-all endpoint.

        IMPORTANT: this endpoint cancels ALL open orders on the BitPreco account,
        not just for `trading_pair`. The `exchange_symbol` argument is unused but
        kept in the signature for dispatch uniformity. Acceptable here because the
        BitPreco account is dedicated to this bot — if that ever changes, swap to
        a list-then-cancel-per-symbol pattern (see Bybit/Binance helpers above).

        Auth is a simple concatenation `secret + api_key` posted in the JSON body
        — no HMAC, no timestamp. Matches BitprecoAuth.rest_authenticate().
        """
        import aiohttp

        api_key = getattr(connector, "api_key", None)
        secret = getattr(connector, "secret_key", None)
        if not api_key or not secret:
            self.logger().error(
                f"[startup_cleanup] bitpreco/{trading_pair}: missing api_key/secret on connector"
            )
            return

        url = "https://api.bitpreco.com/v1/trading/all_orders_cancel"
        body = {"auth_token": f"{secret}{api_key}"}
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=body) as resp:
                    data = await resp.json()
        except Exception as e:
            self.logger().error(
                f"[startup_cleanup] bitpreco/{trading_pair}: all_orders_cancel failed: {e}"
            )
            return

        if isinstance(data, dict) and data.get("success"):
            n = data.get("orders_canceled", 0)
            if n:
                self.logger().warning(
                    f"[startup_cleanup] bitpreco/{trading_pair}: cancelled {n} order(s) (account-wide)."
                )
            else:
                self.logger().info(
                    f"[startup_cleanup] bitpreco/{trading_pair}: no open orders — clean start."
                )
        else:
            self.logger().error(
                f"[startup_cleanup] bitpreco/{trading_pair}: unexpected response: {data}"
            )

    # ------------------------------------------------------------------ #
    # Orphan order reconciliation (periodic safety net)                 #
    # ------------------------------------------------------------------ #
    async def _bitpreco_list_open_orders(self, connector, market: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch the live open-orders list from BitPreco for a given market.
        Returns a list of order dicts on success, or None on any failure
        (the caller treats None as "skip this cycle, try again next time").

        Uses raw aiohttp + simple `secret + api_key` auth like the rest of the
        BitPreco helpers in this file (no HMAC).
        """
        import aiohttp

        api_key = getattr(connector, "api_key", None)
        secret = getattr(connector, "secret_key", None)
        if not api_key or not secret:
            self.logger().error("[orphan_check] bitpreco: missing api_key/secret on connector")
            return None

        url = "https://api.bitpreco.com/trading"
        body = {
            "cmd": "open_orders",
            "market": market,
            "auth_token": f"{secret}{api_key}",
        }
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=body) as resp:
                    data = await resp.json()
        except Exception as e:
            self.logger().warning(f"[orphan_check] bitpreco/open_orders request failed: {e}")
            return None

        # BitPreco returns either a list of order dicts (success) or a dict
        # with success=false on auth/rate-limit failure. Be tolerant.
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if data.get("success") is False:
                self.logger().warning(
                    f"[orphan_check] bitpreco/open_orders rejected: {data}"
                )
                return None
            # Sometimes wrapped: {"orders": [...]} — handle both shapes.
            inner = data.get("orders")
            if isinstance(inner, list):
                return inner
            self.logger().warning(
                f"[orphan_check] bitpreco/open_orders unexpected dict shape: keys={list(data.keys())}"
            )
            return None
        self.logger().warning(
            f"[orphan_check] bitpreco/open_orders non-list/dict response: {type(data).__name__}"
        )
        return None

    async def _bitpreco_cancel_one_orphan(self, connector, exchange_order_id: str) -> bool:
        """Cancel a single order by exchange_order_id via BitPreco's order_cancel endpoint."""
        import aiohttp
        api_key = getattr(connector, "api_key", None)
        secret = getattr(connector, "secret_key", None)
        if not api_key or not secret:
            return False
        url = "https://api.bitpreco.com/trading"
        body = {
            "cmd": "order_cancel",
            "order_id": exchange_order_id,
            "auth_token": f"{secret}{api_key}",
        }
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=body) as resp:
                    data = await resp.json()
        except Exception as e:
            self.logger().error(
                f"[orphan_check] cancel of orphan {exchange_order_id} raised: {e}"
            )
            return False
        ok_codes = {"ORDER_CANCELED", "ORDER_NOT_FOUND", "ORDER_ALREADY_CANCELED"}
        code = data.get("message_cod") if isinstance(data, dict) else None
        if code in ok_codes:
            self.logger().warning(
                f"[orphan_check] cancelled orphan exchange_order_id={exchange_order_id} (code={code})"
            )
            return True
        self.logger().error(
            f"[orphan_check] failed to cancel orphan {exchange_order_id}: {data}"
        )
        return False

    @staticmethod
    def _snapshot_tracker_ids(connector) -> Set[str]:
        """Snapshot the set of exchange_order_ids currently in the connector's tracker."""
        ids: Set[str] = set()
        in_flight = getattr(connector, "in_flight_orders", {}) or {}
        for o in in_flight.values():
            xid = getattr(o, "exchange_order_id", None)
            if xid:
                ids.add(str(xid))
        return ids

    # BitPreco's REST API returns ``time_stamp`` strings in America/Sao_Paulo
    # (UTC-3) without an explicit offset — confirmed empirically against
    # OrderCreatedEvent log timestamps. ``datetime.strptime`` produces a naive
    # datetime; ``naive.timestamp()`` then interprets it as the host's *local*
    # time. On a UTC host (typical Linux server) that meant we treated a fresh
    # order as having been placed 3 hours ago, breezing past the 3s ``min_age``
    # filter and cancelling it as a phantom orphan within the first ~60ms of
    # life. Production logs show this hit ~3 orders/h. We fix by attaching the
    # explicit BitPreco offset before computing the epoch.
    BITPRECO_TZ = timezone(timedelta(hours=-3))

    @classmethod
    def _parse_bitpreco_timestamp(cls, value: Any) -> Optional[float]:
        """
        BitPreco's open_orders entries carry a ``time_stamp`` field formatted
        as ``"YYYY-MM-DD HH:MM:SS"`` in **America/Sao_Paulo (UTC-3)**, with no
        explicit offset. We attach the offset before calling ``.timestamp()``
        so the resulting epoch is correct regardless of the host timezone.

        Returns the epoch seconds, or None if the field is missing /
        unparseable. Used by the orphan-check min-age filter so we don't race
        with in-flight placements.
        """
        if not value:
            return None
        try:
            naive = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
            aware = naive.replace(tzinfo=cls.BITPRECO_TZ)
            return aware.timestamp()
        except (TypeError, ValueError):
            return None

    async def _run_orphan_check(self, now: float) -> None:
        """
        Periodic reconciliation with two race-resistance guards:

        1. **Double-snapshot tracker reads.** The connector's in_flight_orders
           tracker is read both BEFORE and AFTER the open_orders REST call.
           A candidate is only considered an orphan if it's missing from BOTH
           snapshots — anything that appeared in the tracker during the API
           round-trip is treated as a legitimately-tracked order whose ack
           was racing with our snapshot.

        2. **Minimum-age filter.** Orders younger than `_orphan_min_age_sec`
           are skipped entirely. Placement → tracker-update → next-tick is a
           sub-second flow; a 5s floor is more than enough to absorb any race
           window without leaving real orphans alive too long (the periodic
           cycle still runs every `_orphan_check_interval_sec`).

        Without these guards, the orphan_check itself was killing legitimate
        orders during their first ~500ms of life (false-positive orphan).

        Only runs for the maker connector when it's BitPreco — Bybit/Binance
        use different (and well-tested) cancel flows.
        """
        if self.config.maker_connector != "bitpreco":
            return
        if (now - self._last_orphan_check_time) < self._orphan_check_interval_sec:
            return
        self._last_orphan_check_time = now

        connector = self.market_data_provider.get_connector(self.config.maker_connector)
        if connector is None:
            return

        # exchange_symbol here is "BTC-BRL" — BitPreco uses dashed form.
        try:
            market = await connector.exchange_symbol_associated_to_pair(
                trading_pair=self.config.maker_trading_pair
            )
        except Exception:
            market = self.config.maker_trading_pair

        # Race-resistance guard #1: snapshot tracker BEFORE the API call.
        tracked_before = self._snapshot_tracker_ids(connector)
        # API call to BitPreco — typically 100-300ms; any placement happening
        # concurrently can land in the tracker after this returns.
        exchange_orders = await self._bitpreco_list_open_orders(connector, market)
        # Race-resistance guard #1: snapshot tracker AFTER the API call.
        tracked_after = self._snapshot_tracker_ids(connector)

        if exchange_orders is None:
            return  # transient error, try again next cycle

        # Each BitPreco entry has an "id" field for the exchange order id and
        # a "time_stamp" field for placement time.
        exchange_entries: List[Tuple[str, Optional[float]]] = []
        for entry in exchange_orders:
            if isinstance(entry, dict) and entry.get("id") is not None:
                ts = self._parse_bitpreco_timestamp(entry.get("time_stamp"))
                exchange_entries.append((str(entry["id"]), ts))

        # Combined tracked set. Anything in EITHER snapshot is "known" to the bot.
        tracked_combined = tracked_before | tracked_after

        # Race-resistance guard #2: only flag as orphan if older than min_age.
        # If we can't parse the timestamp, default to "young" (skip this cycle).
        orphans: List[str] = []
        skipped_too_young = 0
        for eid, ts in exchange_entries:
            if eid in tracked_combined:
                continue
            age = (now - ts) if ts is not None else 0.0
            if age < self._orphan_min_age_sec:
                skipped_too_young += 1
                continue
            orphans.append(eid)

        # Task 4.2: detect the *inverse* direction — orders the tracker still
        # holds but the exchange no longer has open. This means the order was
        # filled or cancelled at the exchange but the WS update never made it
        # to the tracker (the reason fills/cancels arrive "did not arrive on
        # time"). Trigger a one-shot REST status poll to reconcile so the
        # executor's lifecycle isn't stuck waiting for an event that never
        # comes. We compute this on ``tracked_after ∩ tracked_before`` to
        # avoid flapping during the API round-trip.
        exchange_id_set = {eid for eid, _ts in exchange_entries}
        tracker_only = (tracked_after & tracked_before) - exchange_id_set

        # Throttled heartbeat: only log when state changes vs last log, or
        # every 60s so the line still appears in long idle stretches.
        sig = (
            len(tracked_before), len(tracked_after), len(exchange_entries),
            len(orphans), skipped_too_young, len(tracker_only),
        )
        if sig != self._last_orphan_log_signature or (now - self._last_orphan_log_ts) >= 60.0:
            self._last_orphan_log_signature = sig
            self._last_orphan_log_ts = now
            self.logger().info(
                f"[orphan_check] tracker_before={len(tracked_before)} "
                f"tracker_after={len(tracked_after)} "
                f"exchange_open={len(exchange_entries)} "
                f"orphans={len(orphans)} skipped_young={skipped_too_young} "
                f"tracker_only={len(tracker_only)} "
                f"(interval={self._orphan_check_interval_sec}s, min_age={self._orphan_min_age_sec}s)"
            )

        for orphan_id in orphans:
            ok = await self._bitpreco_cancel_one_orphan(connector, orphan_id)
            if ok:
                self._orphans_cancelled_total += 1

        # If we found tracker-only entries, trigger a single REST status poll.
        # The connector reconciles each in-flight order against REST and
        # emits OrderFilledEvent / OrderCancelledEvent for any state change
        # discovered. We don't loop per-id — one ``_update_order_status``
        # call covers all in-flight orders and is rate-limit-cheap.
        if tracker_only:
            try:
                self.logger().info(
                    f"[reconcile] {len(tracker_only)} tracker-only orders "
                    f"({sorted(tracker_only)[:3]}{'...' if len(tracker_only) > 3 else ''}) — "
                    f"triggering connector REST status poll to reconcile."
                )
                await connector._update_order_status()
            except Exception as e:
                self.logger().warning(
                    f"[reconcile] REST status poll failed: {type(e).__name__}: {e}"
                )

    # ------------------------------------------------------------------ #
    # Graceful shutdown (SIGTERM / SIGINT)                              #
    # ------------------------------------------------------------------ #
    def _setup_sigterm_handler(self) -> None:
        """
        Register SIGTERM and SIGINT handlers on the running asyncio event loop.

        Must be called from within the event loop (i.e., from update_processed_data
        on the first tick) — loop.add_signal_handler() requires the loop to be
        running and is not safe to call from __init__ (before the loop starts).

        On SIGTERM/SIGINT: immediately sets regime to KILLED (no new orders),
        then cancels all open maker/taker orders via direct REST before exiting.
        Gives up to 8 seconds for REST round-trips; if the network is already
        going down the timeout fires and we exit with whatever was cancelled.
        """
        if self._sigterm_registered:
            return
        try:
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.ensure_future(
                        self._graceful_shutdown(s.name)
                    ),
                )
            self._sigterm_registered = True
            self.logger().info(
                "[signal] SIGTERM/SIGINT handler registered — "
                "open orders will be cancelled on shutdown."
            )
        except (NotImplementedError, RuntimeError) as e:
            # NotImplementedError on Windows; RuntimeError if loop not running yet
            self.logger().warning(f"[signal] Could not register signal handler: {e}")

    async def _graceful_shutdown(self, sig_name: str) -> None:
        """
        Cancel all open orders on both exchanges before exiting.

        Designed to handle the case where the OS sends SIGTERM during a server
        reboot or service stop — giving the bot a chance to cancel open maker
        orders (which would otherwise become orphans until the next pre-cleanup).

        Timeout: 8 seconds. If the network is going down the REST calls will
        fail or time out; we log and exit anyway to avoid hanging indefinitely
        (systemd will send SIGKILL after TimeoutStopSec regardless).
        """
        self.logger().warning(
            f"[signal] {sig_name} received — cancelling open orders before exit..."
        )
        # Immediately latch kill reason so _compute_regime() returns KILLED on
        # the next tick, preventing any new executor from being created while
        # we await the REST cancellation calls below.
        if self._kill_reason is None:
            self._kill_reason = sig_name

        try:
            await asyncio.wait_for(
                self._cancel_all_open_orders_on_startup(),
                timeout=8.0,
            )
            self.logger().warning("[signal] Graceful shutdown complete — all orders cancelled.")
        except asyncio.TimeoutError:
            self.logger().error(
                "[signal] Graceful shutdown timed out after 8s — "
                "some orders may remain open. Pre-cleanup will handle them on next boot."
            )
        except Exception as e:
            self.logger().error(f"[signal] Graceful shutdown error: {e}")
        finally:
            sys.exit(0)

    # ------------------------------------------------------------------ #
    # Update                                                             #
    # ------------------------------------------------------------------ #
    def _safe_price(self, connector: str, pair: str, ptype: PriceType) -> Decimal:
        """get_price_by_type with graceful fallback to Decimal('0')."""
        try:
            value = self.market_data_provider.get_price_by_type(connector, pair, ptype)
            return value if value is not None else Decimal("0")
        except Exception as e:
            self.logger().warning(
                f"Price fetch failed for {connector}/{pair}/{ptype}: {e}"
            )
            return Decimal("0")

    def _safe_balance(self, connector: str, asset: str) -> Decimal:
        try:
            value = self.market_data_provider.get_balance(connector, asset)
            return value if value is not None else Decimal("0")
        except Exception:
            return Decimal("0")

    def _safe_total_balance(self, connector: str, asset: str) -> Decimal:
        """
        Total balance = available + locked-in-orders.

        Used by inventory audit: when an order is placed, the exchange locks
        part of the balance — `get_balance(asset)` (available) drops, but the
        TOTAL holdings on the exchange remain the same. The connector's own
        `get_balance` API returns the total (not available); we go through it
        directly here, bypassing market_data_provider.get_balance which only
        returns available.
        """
        try:
            conn = self.market_data_provider.get_connector(connector)
            value = conn.get_balance(asset)
            return Decimal(str(value)) if value is not None else Decimal("0")
        except Exception:
            return Decimal("0")

    # ------------------------------------------------------------------ #
    # Inventory audit (state-based reconciliation)                       #
    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
    # Ghost-fill handling (controller-level)                             #
    # ------------------------------------------------------------------ #
    def register_ghost_order(
        self,
        order_id: str,
        maker_side: TradeType,
        maker_connector: str,
    ) -> None:
        """Called by XEMMLeadLagExecutor when a maker order is cancelled
        with ``executed_amount_base == 0``.  The BitPreco CANT_CANCEL_FILLED
        _ORDER race can leave a delayed fill event that arrives long after
        the executor has terminated; this dict lets the controller's own
        fill listener place the taker hedge instead."""
        self._pending_ghost_orders[order_id] = {
            "maker_side": maker_side,
            "maker_connector": maker_connector,
            "registered_at": time.time(),
        }
        self.logger().info(
            f"[ghost_controller] registered {order_id} "
            f"side={maker_side.name if hasattr(maker_side, 'name') else maker_side} "
            f"conn={maker_connector} | pending={len(self._pending_ghost_orders)}"
        )

    def mark_ghost_hedged(self, order_id: str) -> None:
        """Called by the executor when it placed the taker hedge for an
        order also registered as a ghost.  Prevents the controller's
        listener from firing a duplicate hedge on the same event."""
        self._pending_ghost_orders.pop(order_id, None)
        self._already_hedged_ghost_ids.add(order_id)

    def _purge_ghost_orders(self, now: float) -> None:
        """Drop ghost entries older than ``_ghost_max_age_sec``.  Cap the
        hedged-id set size so it cannot grow unboundedly across a long
        session (we trade ~hundreds of orders/hour; trim conservatively)."""
        expired = [
            oid for oid, info in self._pending_ghost_orders.items()
            if (now - info["registered_at"]) > self._ghost_max_age_sec
        ]
        for oid in expired:
            del self._pending_ghost_orders[oid]
            self.logger().info(
                f"[ghost_controller] purged stale entry {oid} "
                f"(no fill event arrived within {self._ghost_max_age_sec}s)"
            )
        if len(self._already_hedged_ghost_ids) > 200:
            self._already_hedged_ghost_ids.clear()

    def _handle_ghost_fill_event(self, event, source_connector: str) -> None:
        """Listener wired in ``_attach_ledger_fill_listeners``.  For every
        OrderFilled event on the maker connector, see if the order_id was
        registered as a ghost — if so, place a MARKET hedge on the taker
        for ``event.amount`` BTC.

        The executor's own ``process_order_completed_event`` may also fire
        on the same underlying fill (different event type) if the executor
        is still alive; the de-dup set ``_already_hedged_ghost_ids``
        prevents double-hedging.  Whichever path fires first wins."""
        order_id = getattr(event, "order_id", None)
        if not order_id:
            return
        if order_id in self._already_hedged_ghost_ids:
            return
        info = self._pending_ghost_orders.get(order_id)
        if info is None:
            return
        if source_connector != info["maker_connector"]:
            self.logger().warning(
                f"[ghost_controller] {order_id}: fill from {source_connector} "
                f"but registered for {info['maker_connector']} — skipping."
            )
            return
        try:
            amount = Decimal(str(getattr(event, "amount", 0)))
        except Exception:
            amount = Decimal("0")
        if amount <= 0:
            return
        # Atomically claim ownership BEFORE side effects: this guarantees
        # that if two handlers somehow process the same event back-to-back,
        # only one places the hedge.
        self._pending_ghost_orders.pop(order_id, None)
        self._already_hedged_ghost_ids.add(order_id)

        maker_side = info["maker_side"]
        hedge_side = TradeType.BUY if maker_side == TradeType.SELL else TradeType.SELL
        try:
            taker_conn = self.market_data_provider.get_connector(
                self.config.taker_connector
            )
        except Exception as e:
            self.logger().error(
                f"[ghost_controller] {order_id}: cannot resolve taker connector "
                f"{self.config.taker_connector}: {type(e).__name__}: {e}. "
                f"inventory_audit will reconcile (worse PnL)."
            )
            return

        pair = self.config.taker_trading_pair
        try:
            if hedge_side == TradeType.BUY:
                hedge_id = taker_conn.buy(pair, amount, OrderType.MARKET, Decimal("0"))
            else:
                hedge_id = taker_conn.sell(pair, amount, OrderType.MARKET, Decimal("0"))
            self.logger().warning(
                f"[ghost_controller] {order_id}: late fill on {source_connector} "
                f"({maker_side.name} {amount}) → MARKET {hedge_side.name} {hedge_id} "
                f"placed on {self.config.taker_connector}/{pair}"
            )
            # Suppress inventory_audit's auto_rebalance for 10 s (same window
            # used by the within-cycle ghost path in the executor).
            self._last_fill_time = time.time()
        except Exception as e:
            self.logger().error(
                f"[ghost_controller] {order_id}: failed to place taker hedge "
                f"({hedge_side.name} {amount} on {self.config.taker_connector}): "
                f"{type(e).__name__}: {e}. inventory_audit will reconcile."
            )

    def _has_inflight_activity(self) -> bool:
        """
        True if a trade is in a transient state that could legitimately explain
        a temporary inventory imbalance — e.g. maker just filled, hedge order
        not yet placed/confirmed, or arb leg A done and leg B still pending.

        IMPORTANT — what does NOT count as "inflight":
          * An XEMM executor that is alive and simply waiting for its maker
            order to fill (normal steady-state operation). The bot always has
            1-2 such executors running; treating them as inflight would make
            this function return True 100% of the time and permanently suppress
            the audit — exactly the bug we are fixing.
          * Open maker/taker orders sitting on the book (stable state; their
            locked balance is already included in get_balance() totals).

        What DOES count as inflight:
          * A fill event occurred within the last 10 seconds: the WS account
            stream may not have propagated the balance update yet, and the
            hedge order may still be in flight.
          * An arbitrage executor is currently running (Fix A 2026-05-11):
            arb has TWO MARKET legs; between leg-1 fill and leg-2 fill the
            inventory is INTENTIONALLY skewed. Without this check the audit
            mistook the transient as drift and queued a third MARKET order
            on the same exchange leg-2 was about to use — double-sold the
            position (observed 16:50:46). Active arb is a stronger signal
            than the 10s timer because BitPreco's REST poll path for fill
            detection can take longer than 10s.

        Why 10 seconds is sufficient for the timer path:
          * XEMM fill → hedge MARKET order typically completes in < 2s.
          * Audit interval is 5 minutes — any drift that persists for 5 minutes
            is real, not transient. The 10s grace window is more than enough.
        """
        # Recent fill: WS account-stream balance update may still be in flight.
        if self._last_fill_time > 0 and (time.time() - self._last_fill_time) < 10.0:
            return True

        # Active arb executor: leg-1 already filled, leg-2 pending. Drift is
        # by design here — do not interfere.
        for ex in (self.executors_info or []):
            if not getattr(ex, "is_done", True):
                ex_type = getattr(getattr(ex, "config", None), "type", "")
                if ex_type == "lead_lag_arbitrage_executor":
                    return True

        return False

    def _is_quiescent(self) -> bool:
        """True when the bot is in a stop-the-world state suitable for an
        authoritative balance read:
          - no active executors (executors_info has no `not is_done`)
          - no in-flight orders on either connector

        Used by the barrier-pattern audit (CANCELLING → VALIDATING transition).
        Defensive against MagicMock'ed connectors in tests: ``in_flight_orders``
        is only inspected when it is an actual dict; anything else is treated
        as "zero" (the test would have explicitly mocked it otherwise).
        """
        for ex in (self.executors_info or []):
            if not getattr(ex, "is_done", True):
                return False
        for conn_name in (self.config.maker_connector, self.config.taker_connector):
            try:
                conn = self.market_data_provider.get_connector(conn_name)
                in_flight = getattr(conn, "in_flight_orders", None)
                if isinstance(in_flight, dict) and len(in_flight) > 0:
                    return False
            except Exception:
                continue
        return True

    def _count_inflight_orders(self) -> int:
        """Diagnostic helper: total in-flight orders across both connectors.
        Defensive: only counts when ``in_flight_orders`` is a real dict."""
        total = 0
        for conn_name in (self.config.maker_connector, self.config.taker_connector):
            try:
                conn = self.market_data_provider.get_connector(conn_name)
                in_flight = getattr(conn, "in_flight_orders", None)
                if isinstance(in_flight, dict):
                    total += len(in_flight)
            except Exception:
                continue
        return total

    async def _run_inventory_audit(self, now: float, source: str = "periodic") -> None:
        """Barrier-pattern inventory audit (2026-05-12 simplification).

        State machine:
          IDLE       → suspect drift → CANCELLING
          CANCELLING → wait for quiescence → VALIDATING (or timeout)
          VALIDATING → authoritative re-read → confirmed: queue rebalance;
                                              cleared: return to IDLE
                       (always returns to IDLE after this phase)

        Walks as many transitions as possible in a single call. When the
        bot has no active executors (boot, tests, quiet markets), the full
        sequence IDLE → CANCELLING → VALIDATING → IDLE runs in one call.
        When there are active executors that need cancelling, the call
        ends in CANCELLING and resumes next tick.

        Replaces the previous multi-defense complexity:
        - ``_has_inflight_activity`` 10s-timer defer-on-recent-fill
        - ``_drift_consecutive_audits`` 3-strike kill rule
        - "Audit in killed mode" passive observation
        All of those were paper-cuts compensating for false-positive drift
        from stale balance / hedge-in-flight / arb-leg-in-flight races.
        The barrier eliminates the race window itself.
        """
        cfg = self.config.inventory_audit
        if not cfg.enabled or not cfg.base_targets:
            return

        self._last_audit_time = now

        # State-machine walker. Max 3 transitions in a single call:
        #   IDLE → CANCELLING → VALIDATING → IDLE.
        # Any iteration that returns (instead of transitioning) ends the call.
        for _ in range(3):
            if self._barrier_state == "IDLE":
                advance = await self._audit_step_idle(now, source, cfg)
                if not advance:
                    return  # no drift detected; stay IDLE
                # else transitioned to CANCELLING; loop continues
                continue

            if self._barrier_state == "CANCELLING":
                advance = self._audit_step_cancelling(now)
                if not advance:
                    return  # not quiescent yet; wait next tick
                # else transitioned to VALIDATING; loop continues
                continue

            if self._barrier_state == "VALIDATING":
                await self._audit_step_validating(now, source, cfg)
                return  # always returns to IDLE; done for this call

            # Unknown state (defensive)
            self.logger().error(
                f"[audit/{source}] unknown _barrier_state={self._barrier_state}; resetting to IDLE"
            )
            self._barrier_state = "IDLE"
            return

    async def _audit_step_idle(
        self, now: float, source: str, cfg: "InventoryAuditConfig",
    ) -> bool:
        """IDLE → CANCELLING transition. Returns True if barrier was entered
        (loop continues), False if no drift was found (stay IDLE)."""
        await self._force_refresh_balances(source)
        mid_price = self._read_audit_mid_price(source)
        if mid_price is None:
            return False

        results, any_drift = self._compute_drift_per_asset(cfg, mid_price)
        results["_drift_active"] = any_drift
        results["_inflight_active"] = self._has_inflight_activity()
        self._last_audit_results = results

        if source == "boot":
            self._log_side_imbalance_if_any(results, source)

        if not any_drift:
            return False

        # Enter barrier
        self._barrier_state = "CANCELLING"
        self._barrier_started_at = now
        self._barrier_drift_snapshot = results
        self.logger().info(
            f"[audit/{source}] drift suspected — entering barrier. "
            f"{self._format_drift_summary(results)}; "
            f"validation timeout {self._barrier_timeout_sec:.0f}s"
        )
        return True

    def _audit_step_cancelling(self, now: float) -> bool:
        """CANCELLING → VALIDATING transition. Returns True if advanced
        (quiescent reached OR timeout), False if still waiting."""
        if self._is_quiescent():
            elapsed = now - self._barrier_started_at
            self.logger().info(
                f"[barrier] quiescent after {elapsed:.1f}s — VALIDATING"
            )
            self._barrier_state = "VALIDATING"
            return True
        if (now - self._barrier_started_at) > self._barrier_timeout_sec:
            self.logger().warning(
                f"[barrier] timeout {self._barrier_timeout_sec:.0f}s reached "
                f"with {self._count_inflight_orders()} order(s) still in flight "
                f"— proceeding to VALIDATING anyway"
            )
            self._barrier_state = "VALIDATING"
            return True
        return False

    async def _audit_step_validating(
        self, now: float, source: str, cfg: "InventoryAuditConfig",
    ) -> None:
        """VALIDATING → IDLE transition (always)."""
        await self._force_refresh_balances(source)
        mid_price = self._read_audit_mid_price(source)
        if mid_price is None:
            # Can't validate without a price — return to IDLE without
            # queuing rebalance. Next audit cycle will retry from IDLE.
            self._barrier_state = "IDLE"
            self._barrier_drift_snapshot = {}
            return

        results, any_drift = self._compute_drift_per_asset(cfg, mid_price)

        if any_drift:
            # Confirmed drift is the normal trigger for the auto-rebalance
            # MARKET — not an emergency. Pause action escalates separately
            # below. WARNING level keeps it visible without dominating the
            # log feed.
            self.logger().warning(
                f"[barrier] drift CONFIRMED after quiescence — "
                f"{self._format_drift_summary(results)}"
            )
            self._queue_rebalance_for_confirmed(now, source, cfg, results)
            if cfg.on_drift_action == "pause":
                self._maybe_set_kill_reason_from_drift(results)
        else:
            self.logger().info(
                f"[barrier] drift CLEARED after quiescence (was a race / "
                f"stale read). suspicion snapshot: "
                f"{self._format_drift_summary(self._barrier_drift_snapshot)}"
            )

        self._barrier_state = "IDLE"
        self._barrier_drift_snapshot = {}
        results["_drift_active"] = any_drift
        results["_inflight_active"] = False  # we just reached quiescence
        self._last_audit_results = results

    # ----- audit helpers -----

    async def _force_refresh_balances(self, source: str) -> None:
        """Ask both connectors to refresh their account_balances cache via
        REST. BitPreco's override accepts a ``_trigger`` kwarg for log
        tagging; framework default doesn't — try named, fall back graceful.
        """
        for conn_name in (self.config.maker_connector, self.config.taker_connector):
            try:
                conn = self.market_data_provider.get_connector(conn_name)
            except Exception:
                continue
            update_fn = getattr(conn, "_update_balances", None)
            if update_fn is None:
                continue
            try:
                await update_fn(_trigger=f"audit_force_{source}")
            except TypeError:
                try:
                    await update_fn()
                except Exception as e:
                    self.logger().warning(
                        f"[audit/{source}] force-refresh balance failed "
                        f"for {conn_name}: {type(e).__name__}: {e}"
                    )
            except Exception as e:
                self.logger().warning(
                    f"[audit/{source}] force-refresh balance failed "
                    f"for {conn_name}: {type(e).__name__}: {e}"
                )

    def _read_audit_mid_price(self, source: str) -> Optional[Decimal]:
        """Maker mid_price used to convert ``max_drift_quote`` (BRL) into a
        base-asset tolerance. Returns None on missing data (audit skips)."""
        try:
            best_bid = self._safe_price(
                self.config.maker_connector, self.config.maker_trading_pair, PriceType.BestBid
            )
            best_ask = self._safe_price(
                self.config.maker_connector, self.config.maker_trading_pair, PriceType.BestAsk
            )
            if best_bid is None or best_ask is None or best_bid <= 0 or best_ask <= 0:
                raise ValueError(f"invalid prices bid={best_bid} ask={best_ask}")
            return (best_bid + best_ask) / Decimal("2")
        except Exception as e:
            self.logger().warning(
                f"[audit/{source}] cannot compute mid_price for "
                f"{self.config.maker_trading_pair} ({type(e).__name__}: {e}) — "
                f"skipping audit cycle."
            )
            return None

    def _compute_drift_per_asset(
        self, cfg: "InventoryAuditConfig", mid_price: Decimal
    ) -> Tuple[Dict[str, Dict[str, Decimal]], bool]:
        """Returns (results, any_drift). results[asset] contains delta /
        maker_bal / taker_bal / within / delta_quote for each tracked asset.
        Special keys prefixed with `_` (e.g. `_drift_active`) are added
        by the caller."""
        results: Dict[str, Dict[str, Decimal]] = {}
        any_drift = False
        tolerance_abs = cfg.max_drift_quote / mid_price
        for asset, target in cfg.base_targets.items():
            target_dec = Decimal(str(target))
            maker_bal = self._safe_total_balance(self.config.maker_connector, asset)
            taker_bal = self._safe_total_balance(self.config.taker_connector, asset)
            actual = maker_bal + taker_bal
            delta = actual - target_dec
            within = abs(delta) <= tolerance_abs
            results[asset] = {
                "actual": actual,
                "target": target_dec,
                "delta": delta,
                "delta_quote": abs(delta) * mid_price,
                "maker_bal": maker_bal,
                "taker_bal": taker_bal,
                "within": within,
            }
            if not within:
                any_drift = True
        return results, any_drift

    def _format_drift_summary(self, results: Dict[str, Dict[str, Decimal]]) -> str:
        parts = []
        for asset, r in results.items():
            if asset.startswith("_") or not isinstance(r, dict):
                continue
            d = r.get("delta", 0)
            t = r.get("target", 0)
            try:
                pct = (d / t * 100) if t else Decimal("0")
            except Exception:
                pct = Decimal("0")
            parts.append(
                f"{asset} delta={d:+f} ({pct:+.2f}%) "
                f"maker={r.get('maker_bal', 0)} taker={r.get('taker_bal', 0)}"
            )
        return " | ".join(parts) or "(empty)"

    def _log_side_imbalance_if_any(
        self,
        results: Dict[str, Dict[str, Decimal]],
        source: str,
    ) -> None:
        """Side-imbalance observability (kept from earlier P1 work). Total
        may be within tolerance but one exchange depleted — flag it so
        operator knows balance_gate pauses are expected until natural fills
        restore parity."""
        for asset, r in results.items():
            if asset.startswith("_") or not isinstance(r, dict):
                continue
            if not r.get("within"):
                continue
            target = r["target"]
            if target <= 0:
                continue
            maker_bal = r["maker_bal"]
            taker_bal = r["taker_bal"]
            low_side_ratio = min(maker_bal, taker_bal) / target
            if low_side_ratio < Decimal("0.25"):
                low_side = (
                    self.config.maker_connector if maker_bal <= taker_bal
                    else self.config.taker_connector
                )
                self.logger().warning(
                    f"[audit/{source}] {asset} TOTAL OK but side-imbalance: "
                    f"maker={maker_bal:.8f} taker={taker_bal:.8f} target={target} "
                    f"({low_side} has <25% of target). Bot may pause one side "
                    f"of hedging until a natural fill restores parity. "
                    f"Consider depositing on {low_side} if persistent."
                )

    def _queue_rebalance_for_confirmed(
        self,
        now: float,
        source: str,
        cfg: "InventoryAuditConfig",
        results: Dict[str, Dict[str, Decimal]],
    ) -> None:
        """Queue auto_rebalance MARKET orders for assets confirmed in drift
        AFTER the barrier validated them. Respects cooldown to prevent spam.
        """
        if cfg.on_drift_action != "auto_rebalance":
            return  # "pause" handled via _maybe_set_kill_reason_from_drift
        for asset, r in results.items():
            if asset.startswith("_") or not isinstance(r, dict):
                continue
            if r.get("within"):
                continue
            last = self._rebalance_last_time.get(asset, 0.0)
            if (now - last) < self._rebalance_cooldown_sec:
                self.logger().info(
                    f"[audit/{source}] {asset} confirmed-drift rebalance "
                    f"deferred — cooldown active "
                    f"({self._rebalance_cooldown_sec - (now - last):.0f}s remaining)"
                )
                continue
            self._pending_rebalances[asset] = r["delta"]

    def _maybe_set_kill_reason_from_drift(
        self, results: Dict[str, Dict[str, Decimal]]
    ) -> None:
        """For ``on_drift_action="pause"``: trip kill switch when drift is
        confirmed post-barrier. Same path as watchdog/circuit-breakers."""
        for asset, r in results.items():
            if asset.startswith("_") or not isinstance(r, dict):
                continue
            if r.get("within"):
                continue
            if self._kill_reason is None:
                self._kill_reason = f"INVENTORY_DRIFT_{asset}"
            return  # one is enough

    def _rebalance_evaluate_exchange(
        self,
        conn_name: str,
        pair: str,
        asset: str,
        amount: Decimal,
        is_sell: bool,
    ) -> Optional[Decimal]:
        """
        Evaluate whether `conn_name` is viable for a rebalance order of `amount`
        base asset on the given side.

        Returns the VWAP price if viable (book has sufficient depth AND the
        exchange has the required capital), or None if not viable.

        Depth check (via VWAP):
          `_vwap_for_amount` walks the real order book for the full `amount`.
          If the book cannot fill `amount` it returns None — that alone filters
          out exchanges with thin books. The VWAP also directly encodes the
          expected average execution price, so we can compare exchanges on
          a like-for-like basis (not just L1 best bid/ask which ignores depth).

        Capital check:
          SELL: exchange must hold at least `amount` of the base asset (BTC).
          BUY:  exchange must hold at least `amount * vwap` of the quote (BRL),
                using the VWAP itself (already depth-adjusted) for the notional.
        """
        vwap = self._vwap_for_amount(conn_name, pair, is_buy=not is_sell, amount=amount)
        if vwap is None:
            return None  # book too thin or unavailable

        if is_sell:
            # Need enough base to sell
            base_bal = self._safe_total_balance(conn_name, asset)
            if base_bal < amount:
                self.logger().debug(
                    f"[rebalance] {conn_name} SELL: base_bal={base_bal:.8f} < amount={amount:.8f} — not viable"
                )
                return None
        else:
            # Need enough quote to buy (notional = amount * vwap, +1% buffer)
            _, quote_asset = split_hb_trading_pair(pair)
            quote_bal = self._safe_total_balance(conn_name, quote_asset)
            needed = amount * vwap * Decimal("1.01")
            if quote_bal < needed:
                self.logger().debug(
                    f"[rebalance] {conn_name} BUY: quote_bal={quote_bal:.2f} < needed={needed:.2f} — not viable"
                )
                return None

        return vwap

    async def _execute_pending_rebalances(self) -> None:
        """
        For each asset queued in _pending_rebalances, place a MARKET order on
        the best available exchange to restore the configured target.

        Exchange selection (per side):
          SELL (excess):  prefer exchange with HIGHER VWAP → we receive more quote
          BUY  (deficit): prefer exchange with LOWER  VWAP → we pay less quote

        Viability gate (both must pass to be a candidate):
          1. Book depth: VWAP is non-None for the full rebalance amount
             (`_vwap_for_amount` walks the real order book — thin books return None)
          2. Capital:    sufficient base (SELL) or quote (BUY) balance

        Fallback chain:
          best candidate → other exchange → skip with WARNING
        """
        if not self._pending_rebalances:
            return

        now = self.market_data_provider.time()

        for asset, delta in list(self._pending_rebalances.items()):
            maker_base, _ = split_hb_trading_pair(self.config.maker_trading_pair)
            taker_base, _ = split_hb_trading_pair(self.config.taker_trading_pair)

            if asset == maker_base:
                exchanges = [
                    (self.config.maker_connector, self.config.maker_trading_pair),
                    (self.config.taker_connector, self.config.taker_trading_pair),
                ]
            elif asset == taker_base:
                exchanges = [
                    (self.config.taker_connector, self.config.taker_trading_pair),
                    (self.config.maker_connector, self.config.maker_trading_pair),
                ]
            else:
                self.logger().error(
                    f"[rebalance] Asset {asset} not found as base in any trading pair — skipping."
                )
                continue

            amount = abs(delta)
            is_sell = delta > 0  # excess → sell; deficit → buy

            # Evaluate both exchanges: collect (conn_name, pair, vwap) for viable ones
            candidates = []
            for conn_name, pair in exchanges:
                vwap = self._rebalance_evaluate_exchange(conn_name, pair, asset, amount, is_sell)
                if vwap is not None:
                    candidates.append((conn_name, pair, vwap))
                    self.logger().debug(
                        f"[rebalance] {conn_name} viable: vwap={vwap:.2f} "
                        f"({'SELL' if is_sell else 'BUY'} {amount:.8f} {asset})"
                    )

            if not candidates:
                self.logger().warning(
                    f"[rebalance] {asset} {'SELL' if is_sell else 'BUY'} {amount:.8f}: "
                    f"no viable exchange (thin book or insufficient capital on both). Skipping."
                )
                continue

            # Pick best price: SELL → highest VWAP; BUY → lowest VWAP
            if is_sell:
                conn_name, pair, vwap = max(candidates, key=lambda c: c[2])
            else:
                conn_name, pair, vwap = min(candidates, key=lambda c: c[2])

            # Trading rules check (min_order_size, min_notional)
            try:
                connector = self.market_data_provider.get_connector(conn_name)
                rules = connector.trading_rules.get(pair)
                if rules:
                    if amount < rules.min_order_size:
                        self.logger().warning(
                            f"[rebalance] {asset} amount {amount} < min_order_size "
                            f"{rules.min_order_size} on {conn_name} — skipping."
                        )
                        continue
                    if rules.min_notional_size and amount * vwap < rules.min_notional_size:
                        self.logger().warning(
                            f"[rebalance] {asset} notional {amount * vwap:.2f} < "
                            f"min_notional {rules.min_notional_size} on {conn_name} — skipping."
                        )
                        continue
            except Exception as e:
                self.logger().warning(
                    f"[rebalance] Could not validate trading rules for {conn_name}/{pair}: {e}"
                )

            # Quantize amount to the exchange's LOT_SIZE BEFORE placement.
            # Without this the connector silently rounds down (e.g. Binance
            # BTC-BRL has min_base_amount_increment=0.00001 BTC, so a queued
            # rebalance of 0.00018995 BTC gets executed as 0.00018000 — losing
            # 0.00000995 BTC which then re-triggers the audit next cycle and
            # creates a drift-rebalance loop. Observed in prod 2026-05-10 23:14
            # log line 12690-12691. We quantize here so the loss is visible
            # and intentional; if the truncation drops below min_order_size,
            # we skip rebalance and accept the residual drift (the next
            # natural fill will absorb it).
            try:
                amount_q = connector.quantize_order_amount(pair, amount)
                if amount_q != amount:
                    self.logger().info(
                        f"[rebalance] {asset} amount quantized: "
                        f"{amount:.8f} → {amount_q:.8f} on {conn_name} "
                        f"(LOT_SIZE truncation; residual "
                        f"{amount - amount_q:+.8f} stays as drift)"
                    )
                if amount_q <= 0:
                    self.logger().warning(
                        f"[rebalance] {asset} quantized amount is 0 — below "
                        f"LOT_SIZE for {conn_name}. Skipping rebalance; "
                        f"residual drift {amount:.8f} accepted (will absorb "
                        f"into next natural fill)."
                    )
                    continue
                amount = amount_q
            except Exception as e:
                self.logger().warning(
                    f"[rebalance] quantize_order_amount failed for "
                    f"{conn_name}/{pair}: {type(e).__name__}: {e} — "
                    f"proceeding with unquantized {amount:.8f}."
                )

            # Place MARKET order
            try:
                if is_sell:
                    order_id = connector.sell(pair, amount, OrderType.MARKET, Decimal("0"))
                else:
                    order_id = connector.buy(pair, amount, OrderType.MARKET, Decimal("0"))

                direction = "SELL" if is_sell else "BUY"
                other_candidates = [c for c in candidates if c[0] != conn_name]
                alt_str = (
                    f" (alt: {other_candidates[0][0]} vwap={other_candidates[0][2]:.2f})"
                    if other_candidates else ""
                )
                self.logger().warning(
                    f"[rebalance] Placed MARKET {direction} {amount:.8f} {asset} "
                    f"on {conn_name}/{pair} vwap={vwap:.2f}{alt_str} "
                    f"delta={delta:+.8f} order_id={order_id}"
                )
                self._rebalance_last_time[asset] = now
            except Exception as e:
                self.logger().error(
                    f"[rebalance] Failed to place MARKET order for {asset} on {conn_name}: {e}"
                )

        self._pending_rebalances.clear()

    def _vwap_for_amount(
        self, connector: str, pair: str, is_buy: bool, amount: Decimal,
    ) -> Optional[Decimal]:
        """
        VWAP for executing `amount` (base) on the given side.
        Returns None if order book is unavailable or has insufficient depth.
        """
        try:
            ob = self.market_data_provider.get_order_book(connector, pair)
            result = ob.get_vwap_for_volume(is_buy, float(amount))
            price = Decimal(str(result.result_price))
            if price <= 0 or price.is_nan():
                return None
            return price
        except Exception as e:
            self.logger().warning(
                f"VWAP fetch failed for {connector}/{pair} is_buy={is_buy}: {e}"
            )
            return None

    def _compute_arb_gross_bps(self, side: str) -> Decimal:
        """
        Compute GROSS spread in bps for the given arb side, using VWAP for
        `arb_order_amount` (not L1 best bid/ask).

        Returns Decimal("-9999") if any leg has insufficient depth.
        GROSS only — used for telemetry / arb_alert_threshold near-miss
        detection. The spawn gate compares NET (via _compute_arb_net_bps).
        """
        amount = self.config.arb_order_amount
        if side == "long":
            # buy taker (Binance), sell maker (Bybit)
            buy_vwap = self._vwap_for_amount(
                self.config.taker_connector, self.config.taker_trading_pair,
                is_buy=True, amount=amount,
            )
            sell_vwap = self._vwap_for_amount(
                self.config.maker_connector, self.config.maker_trading_pair,
                is_buy=False, amount=amount,
            )
        else:  # short: buy maker (Bybit), sell taker (Binance)
            buy_vwap = self._vwap_for_amount(
                self.config.maker_connector, self.config.maker_trading_pair,
                is_buy=True, amount=amount,
            )
            sell_vwap = self._vwap_for_amount(
                self.config.taker_connector, self.config.taker_trading_pair,
                is_buy=False, amount=amount,
            )
        if buy_vwap is None or sell_vwap is None or buy_vwap <= 0:
            return Decimal("-9999")
        return (sell_vwap - buy_vwap) / buy_vwap * Decimal("10000")

    def _estimate_arb_tx_cost_bps(self) -> Decimal:
        """Round-trip taker fee in bps for an arbitrage (both legs MARKET).

        Reads ``connector.get_fee(...).percent`` from both maker and taker
        connectors. Both legs are taker MARKET orders, so we sum the taker
        fees. Symmetric for "long" and "short" directions (the side just
        flips which connector is buyer vs seller — same fee schedule).

        Returns 0 bps on any failure (defensive — better to spawn-and-let-
        executor-decide than to silently never spawn).
        """
        amount = self.config.arb_order_amount
        # Mid price guesstimate; fee.percent doesn't usually depend on price
        # for these connectors but the API requires it.
        try:
            best_bid = self._safe_price(
                self.config.maker_connector, self.config.maker_trading_pair,
                PriceType.BestBid,
            )
            best_ask = self._safe_price(
                self.config.maker_connector, self.config.maker_trading_pair,
                PriceType.BestAsk,
            )
            mid_price = (best_bid + best_ask) / Decimal("2") if (best_bid and best_ask) else Decimal("1")
        except Exception:
            mid_price = Decimal("1")

        total_pct = Decimal("0")
        for conn_name in (self.config.maker_connector, self.config.taker_connector):
            try:
                conn = self.market_data_provider.get_connector(conn_name)
                fee = conn.get_fee(
                    base_currency="BTC",
                    quote_currency="BRL",
                    order_type=OrderType.MARKET,
                    order_side=TradeType.BUY,
                    amount=amount,
                    price=mid_price,
                    is_maker=False,
                )
                total_pct += Decimal(str(fee.percent))
            except Exception as e:
                self.logger().warning(
                    f"[arb_fee] cannot read fee from {conn_name}: "
                    f"{type(e).__name__}: {e}. Treating as 0 bps."
                )
        return total_pct * Decimal("10000")

    def _compute_arb_net_bps(self, side: str) -> Decimal:
        """NET spread in bps after deducting round-trip taker fees.

        This is the value the spawn gate compares against ``arb_min_profitability``
        — keeps semantics consistent with the executor's own NET gate (it
        also subtracts fees before comparing to the same threshold) and
        with the MM controller which expresses profitability NET of fees.
        """
        gross = self._compute_arb_gross_bps(side)
        if gross == Decimal("-9999"):
            return gross  # propagate sentinel
        return gross - self._estimate_arb_tx_cost_bps()

    async def _maybe_refresh_fresh_arb_bps(
        self,
        now: float,
        cached_long_net_bps: Decimal,
        cached_short_net_bps: Decimal,
        tx_cost_bps: Decimal,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Conditional fresh-REST refresh for BitPreco-side arb VWAP.

        Triggered only when:
          - ``enable_pure_arb`` is on AND
          - bitpreco is one of the legs AND
          - cached net_bps on either side is within
            ``_fresh_arb_trigger_margin_bps`` of ``arb_min_profitability``
            (so a spawn could happen this tick or the next)

        Reuses a stashed value within ``_fresh_arb_cache_ttl_sec`` to avoid
        duplicate REST calls inside one controller tick.

        Returns ``(fresh_long_net, fresh_short_net)`` or ``(None, None)``
        when refresh is not warranted or failed. Caller treats ``None`` as
        "keep the cached value".
        """
        if not self.config.enable_pure_arb:
            return (None, None)
        if "bitpreco" not in (self.config.maker_connector, self.config.taker_connector):
            return (None, None)

        threshold_bps = self.config.arb_min_profitability * Decimal("10000")
        trigger_floor = threshold_bps - self._fresh_arb_trigger_margin_bps
        if (cached_long_net_bps < trigger_floor) and (cached_short_net_bps < trigger_floor):
            return (None, None)

        if (now - self._fresh_arb_cached_at) < self._fresh_arb_cache_ttl_sec:
            return (self._fresh_arb_long_net_bps, self._fresh_arb_short_net_bps)

        bp_conn = self.market_data_provider.get_connector("bitpreco")
        fetch_fn = getattr(bp_conn, "fetch_fresh_vwap", None)
        if fetch_fn is None:
            return (None, None)

        amount = self.config.arb_order_amount
        try:
            bp_buy_vwap = await fetch_fn(self.config.maker_trading_pair, True, amount)
            bp_sell_vwap = await fetch_fn(self.config.maker_trading_pair, False, amount)
        except Exception as e:
            self.logger().warning(
                f"[fresh_arb] fetch failed: {type(e).__name__}: {e}"
            )
            return (None, None)

        if bp_buy_vwap is None or bp_sell_vwap is None:
            return (None, None)

        binance_buy_vwap = self._vwap_for_amount(
            self.config.taker_connector, self.config.taker_trading_pair,
            is_buy=True, amount=amount,
        )
        binance_sell_vwap = self._vwap_for_amount(
            self.config.taker_connector, self.config.taker_trading_pair,
            is_buy=False, amount=amount,
        )
        if binance_buy_vwap is None or binance_sell_vwap is None:
            return (None, None)

        long_gross = (bp_sell_vwap - binance_buy_vwap) / binance_buy_vwap * Decimal("10000")
        short_gross = (binance_sell_vwap - bp_buy_vwap) / bp_buy_vwap * Decimal("10000")
        fresh_long_net = long_gross - tx_cost_bps
        fresh_short_net = short_gross - tx_cost_bps

        self._fresh_arb_cached_at = now
        self._fresh_arb_long_net_bps = fresh_long_net
        self._fresh_arb_short_net_bps = fresh_short_net

        long_delta = fresh_long_net - cached_long_net_bps
        short_delta = fresh_short_net - cached_short_net_bps
        if abs(long_delta) > Decimal("2") or abs(short_delta) > Decimal("2"):
            self.logger().info(
                f"[fresh_arb] staleness detected: cached(long={cached_long_net_bps:.2f} "
                f"short={cached_short_net_bps:.2f}) fresh(long={fresh_long_net:.2f} "
                f"short={fresh_short_net:.2f}) | tx_cost={tx_cost_bps:.2f}bps"
            )

        return (fresh_long_net, fresh_short_net)

    def _market_fingerprint(self) -> tuple:
        """
        Tuple of all market state that, if unchanged, means nothing actionable
        happened since last tick (skip processing).
        Includes balance_version so fill events force a refresh.
        """
        return (
            self._safe_price(self.config.maker_connector, self.config.maker_trading_pair, PriceType.BestBid),
            self._safe_price(self.config.maker_connector, self.config.maker_trading_pair, PriceType.BestAsk),
            self._safe_price(self.config.taker_connector, self.config.taker_trading_pair, PriceType.BestBid),
            self._safe_price(self.config.taker_connector, self.config.taker_trading_pair, PriceType.BestAsk),
            self._safe_price(self.config.signal_connector, self.config.signal_base_pair, PriceType.BestBid),
            self._safe_price(self.config.signal_connector, self.config.signal_base_pair, PriceType.BestAsk),
            self._safe_price(self.config.signal_connector, self.config.signal_fx_pair, PriceType.BestBid),
            self._safe_price(self.config.signal_connector, self.config.signal_fx_pair, PriceType.BestAsk),
            self._balance_version,
        )

    async def update_processed_data(self):
        now = self.market_data_provider.time()
        if self._started_at is None:
            self._started_at = now

        # === SIGTERM/SIGINT handler — registered once on first tick ===
        # Deferred from __init__ because loop.add_signal_handler() requires the
        # event loop to be running; first tick guarantees that.
        if not self._sigterm_registered:
            self._setup_sigterm_handler()

        # === Watchdog: detect event loop lag ===
        # If the gap since the previous tick is > threshold, the event loop was
        # blocked (asyncio coroutine blocking, GC pause, etc). Trip kill switch
        # ONCE — the regime gate then transitions to KILLED and cancels orders
        # gracefully on subsequent ticks instead of accumulating backlog.
        wall = time.time()
        if self._last_tick_wall_time > 0:
            tick_gap = wall - self._last_tick_wall_time
            if tick_gap > self._watchdog_threshold_sec and not self._watchdog_tripped:
                self.logger().critical(
                    f"[watchdog] event loop lag {tick_gap:.1f}s detected "
                    f"(threshold {self._watchdog_threshold_sec}s) — tripping kill switch"
                )
                self._watchdog_tripped = True
                self._kill_reason = f"EVENT_LOOP_LAG_{tick_gap:.0f}s"
        self._last_tick_wall_time = wall

        # === Startup cleanup (run once, when ALL connectors are ready) ===
        # Cancels orphaned orders from prior process kills so available_balance
        # is not locked. We gate on connector.ready (not just mid > 0) because
        # the REST rate-limiter throttler is not initialized until .ready is True
        # — calling _api_get before that raises "NoneType has no attribute weight".
        if not self._startup_cleanup_done:
            try:
                maker_conn = self.market_data_provider.get_connector(self.config.maker_connector)
                taker_conn = self.market_data_provider.get_connector(self.config.taker_connector)
                if maker_conn.ready and taker_conn.ready:
                    await self._cancel_all_open_orders_on_startup()
                    self._startup_cleanup_done = True
            except Exception:
                pass  # connectors not available yet — retry next tick
            # Don't skip the rest of the tick — allow processed_data to be built
            # so the strategy doesn't appear stalled during the brief startup window.

        # === Attach OrderFilledEvent listeners (once connectors ready) ===
        # See ``TradeLedger.record_raw_fill`` for the rationale: executor-level
        # fill detection misses partial-fill-then-cancel and rebalance MARKETs.
        # We subscribe directly to ``MarketEvent.OrderFilled`` on each connector
        # so every fill (regardless of the order's origin) lands in the ledger
        # in real time. Idempotent — guarded by ``_ledger_listeners_attached``.
        if not getattr(self, "_ledger_listeners_attached", False):
            try:
                if self._trade_ledger is not None:
                    self._attach_ledger_fill_listeners()
                    self._ledger_listeners_attached = True
            except Exception as e:
                self.logger().warning(
                    f"[trade_ledger] failed to attach fill listeners: {e}"
                )

        # === Detect newly-completed executors (fills) ===
        # Tracks executor `is_done` transitions to:
        #   1. Bump _balance_version (so fingerprint correctly invalidates)
        #   2. Set _last_fill_time (10s grace window for WS account-stream lag
        #      in _has_inflight_activity)
        for ex in self.executors_info:
            if ex.is_done and ex.id not in self._known_done_executor_ids:
                self._known_done_executor_ids.add(ex.id)
                # Detect "did this executor actually trade?" using multiple
                # signals because `filled_amount_quote` is hardcoded to 0 on
                # the ExecutorBase (and XEMMExecutor doesn't override it).
                # Observed in prod 2026-05-11 09:34:48 — a clean XEMM
                # round-trip (BitPreco maker BUY 0.0001999 + Binance taker
                # SELL 0.0002) reported filled_amount_quote=0, net_pnl_quote=0
                # (price diff was tiny + zero BitPreco fees), so the old
                # check skipped setting `_last_fill_time` — and the audit
                # fired auto_rebalance 9s later thinking the bot was idle.
                # Robust fallback: ALSO trust `cum_fees_quote != 0` (Binance
                # taker always pays a fee) and `is_trading at any prior
                # point` via the executor's filled flags.
                filled_quote = getattr(ex, "filled_amount_quote", None) or Decimal("0")
                net_pnl = getattr(ex, "net_pnl_quote", None) or Decimal("0")
                cum_fees = getattr(ex, "cum_fees_quote", None) or Decimal("0")
                # Trade-fingerprint: any of these being non-zero proves a
                # real fill happened. XEMM with taker hedge ALWAYS pays
                # cum_fees on the taker leg, so this is the most reliable
                # signal for our setup.
                did_trade = bool(filled_quote) or bool(net_pnl) or bool(cum_fees)
                if did_trade:
                    self._last_fill_time = time.time()
                    self._balance_version += 1
                    # Persist to the trade ledger (trades.jsonl + state.json +
                    # last_fill.touch). Never let ledger I/O block the loop.
                    if self._trade_ledger is not None:
                        self._trade_ledger.record_fill(ex)
                    # === Safety counters (drive PnL-based circuit breakers) ===
                    # Increment realized PnL counters AFTER trade-ledger I/O so
                    # ledger failures cannot mask a loss from the gates.
                    self._daily_realized_pnl += net_pnl
                    self._session_pnl_total += net_pnl
                    if self._session_pnl_total > self._session_pnl_peak:
                        self._session_pnl_peak = self._session_pnl_total
                    if net_pnl < Decimal("0"):
                        self._consecutive_losing_fills += 1
                    else:
                        self._consecutive_losing_fills = 0
                    self._hourly_pnl_history.append((time.time(), net_pnl))
                    # Per-executor-type accounting for arb circuit breakers.
                    # Arb executors close once; XEMM executors also close once.
                    # We only count arb losses/failures here; XEMM losses flow
                    # through daily_realized_pnl / session_pnl_total above.
                    ex_type = getattr(ex.config, "type", "")
                    if ex_type == "lead_lag_arbitrage_executor":
                        if net_pnl < Decimal("0"):
                            # Store loss as a POSITIVE magnitude (matches the
                            # gate at line ~2561: `>= arb_daily_loss_limit_quote`).
                            self._arb_realized_loss_today += abs(net_pnl)
                            self._arb_failures_today += 1
                            # Per-failure cooldown: pause arb spawning for
                            # arb_failure_pause_sec. Read at _maybe_create_arb_action.
                            self._arb_paused_until = max(
                                self._arb_paused_until,
                                time.time() + self.config.arb_failure_pause_sec,
                            )
                            self.logger().warning(
                                f"[arb_failure] executor {ex.id} closed with "
                                f"net_pnl={net_pnl} → failures_today="
                                f"{self._arb_failures_today}, "
                                f"loss_today={self._arb_realized_loss_today}, "
                                f"paused for {self.config.arb_failure_pause_sec:.0f}s"
                            )
                    self.logger().info(
                        f"[fill_detected] executor {ex.id} done "
                        f"filled_quote={filled_quote} net_pnl={net_pnl} "
                        f"cum_fees={cum_fees} → _last_fill_time updated "
                        f"(10s audit-suppression window started); "
                        f"safety: daily_pnl={self._daily_realized_pnl} "
                        f"session_pnl={self._session_pnl_total} "
                        f"losing_streak={self._consecutive_losing_fills}"
                    )

        # === Initial inventory audit (Solution C: boot-paused mode) ===
        # Once startup cleanup has confirmed 0 orphan orders, run the audit
        # immediately — if balances match targets, transition out of boot_paused.
        # Behaviour depends on on_drift_action:
        #   "pause"          → KILLED, stays paused (manual intervention needed)
        #   "auto_rebalance" → queues MARKET correction, exits boot_paused optimistically
        #   "alert"          → logs CRITICAL, exits boot_paused (operator monitors)
        if (self._startup_cleanup_done
                and not self._initial_audit_done
                and self.config.inventory_audit.enabled):
            await self._run_inventory_audit(now, source="boot")
            # With the barrier pattern the boot audit may take multiple ticks
            # (IDLE → CANCELLING → VALIDATING → IDLE). Only mark boot audit
            # done when barrier returns to IDLE. At startup the bot has no
            # active executors, so quiescence is instant and the barrier
            # completes in a single call — this guard mostly matters if a
            # boot-time orphan order delays quiescence.
            if self._barrier_state == "IDLE":
                self._initial_audit_done = True
                if self._kill_reason is None:
                    self._boot_paused = False
                    self.logger().info(
                        "[boot] startup_cleanup OK + audit OK — leaving boot_paused mode"
                    )
                else:
                    self.logger().critical(
                        f"[boot] audit detected drift on boot: {self._kill_reason} — "
                        f"bot stays paused, manual intervention required"
                    )
        elif (self._startup_cleanup_done
                and not self._initial_audit_done
                and not self.config.inventory_audit.enabled):
            # Audit disabled — just exit boot_paused after cleanup
            self._initial_audit_done = True
            self._boot_paused = False
            self.logger().info(
                "[boot] startup_cleanup OK + audit disabled — leaving boot_paused mode"
            )

        # === Periodic inventory audit (Tier 4: every audit_interval_sec) ===
        # While a barrier is in flight we poll every tick (200 ms) instead of
        # waiting the full audit_interval_sec — CANCELLING needs prompt
        # quiescence detection, VALIDATING is the post-barrier authoritative
        # read and should complete asap.
        if (self._initial_audit_done
                and self.config.inventory_audit.enabled
                and (self._barrier_state != "IDLE"
                     or (now - self._last_audit_time) >= self.config.inventory_audit.audit_interval_sec)):
            await self._run_inventory_audit(now, source="periodic")

        # === Purge stale ghost-order entries (cheap dict trim) ===
        # Runs every tick; cost is O(N) where N is small (only orders cancelled
        # with executed=0 in the last 60s, typically <20 at peak).
        self._purge_ghost_orders(now)

        # === Periodic orphan-order reconciliation (every 30s) ===
        # Safety net for any race between cancel/place cycles or unconfirmed
        # cancels: queries the exchange's open-orders list, compares with the
        # tracker, and cancels anything that's on the book but not tracked.
        if self._initial_audit_done and not self._boot_paused:
            try:
                await self._run_orphan_check(now)
            except Exception as e:
                self.logger().error(f"[orphan_check] unexpected failure: {e}", exc_info=True)

        # === Execute pending auto-rebalance orders ===
        if self._pending_rebalances:
            await self._execute_pending_rebalances()

        # === Tiered polling (Priority 3) ===
        # Tier 1 (200ms — every tick): Re-evaluate state when fingerprint changes.
        #   Prices, regime, lead, balances, arb VWAPs — used by determine_executor_actions.
        # Tier 3 (1s): CSV write — kept at 1s to avoid 5x I/O.
        # Skip everything if nothing changed AND a Tier 3 row was logged recently.
        fp = self._market_fingerprint()
        time_since_full = now - self._last_full_update
        if (fp == self._last_fingerprint
                and time_since_full < 1.0
                and self.processed_data is not None and len(self.processed_data) > 0):
            return  # nothing changed, skip
        self._last_fingerprint = fp

        # 1. Read L1 from all four feeds (local maker + taker + leader + fx)
        local_bid = self._safe_price(
            self.config.maker_connector, self.config.maker_trading_pair, PriceType.BestBid)
        local_ask = self._safe_price(
            self.config.maker_connector, self.config.maker_trading_pair, PriceType.BestAsk)
        taker_local_bid = self._safe_price(
            self.config.taker_connector, self.config.taker_trading_pair, PriceType.BestBid)
        taker_local_ask = self._safe_price(
            self.config.taker_connector, self.config.taker_trading_pair, PriceType.BestAsk)
        leader_bid = self._safe_price(
            self.config.signal_connector, self.config.signal_base_pair, PriceType.BestBid)
        leader_ask = self._safe_price(
            self.config.signal_connector, self.config.signal_base_pair, PriceType.BestAsk)
        fx_bid = self._safe_price(
            self.config.signal_connector, self.config.signal_fx_pair, PriceType.BestBid)
        fx_ask = self._safe_price(
            self.config.signal_connector, self.config.signal_fx_pair, PriceType.BestAsk)

        # 2. Update signal provider
        self._signal.update(
            now,
            local_bid, local_ask,
            leader_bid, leader_ask,
            fx_bid, fx_ask,
        )

        # 3. Per-exchange inventory
        base_asset, quote_asset = split_hb_trading_pair(self.config.maker_trading_pair)
        maker_base = self._safe_balance(self.config.maker_connector, base_asset)
        maker_quote = self._safe_balance(self.config.maker_connector, quote_asset)
        # Taker pair may share the same base/quote (BTC-BRL on both sides),
        # so we use the same asset symbols.
        taker_base = self._safe_balance(self.config.taker_connector, base_asset)
        taker_quote = self._safe_balance(self.config.taker_connector, quote_asset)
        combined_base = maker_base + taker_base
        combined_quote = maker_quote + taker_quote

        local_mid = self._signal.local_mid
        taker_local_mid = self._mid(taker_local_bid, taker_local_ask)
        valuation_mid = local_mid if local_mid > 0 else taker_local_mid

        if valuation_mid > 0:
            total_in_quote = combined_base * valuation_mid + combined_quote
            combined_pct = (
                (combined_base * valuation_mid) / total_in_quote
                if total_in_quote > 0 else Decimal("0.5")
            )
        else:
            combined_pct = Decimal("0.5")
        inventory_skew = combined_pct - self.config.inventory_target_pct

        # 4. Compute lead signal + adjusted targets (Priority 5: basis-aware skew included)
        best_lead = self._signal.best_lead_signal_bps()
        target_buy, target_sell = self._compute_targets(
            best_lead, inventory_skew, self._signal.basis_bps
        )

        # 5. Compute regime + cancel decision
        regime, cancel_reason = self._compute_regime(
            now=now,
            best_lead=best_lead,
            taker_local_bid=taker_local_bid,
            taker_local_ask=taker_local_ask,
        )
        should_cancel = regime in (Regime.PAUSED, Regime.KILLED)

        # 5b. Auto-terminate (opt-in). When KILLED has latched, no executors
        # are active, and inventory is in tolerance (or audit disabled), send
        # SIGTERM to ourselves so the supervisor restarts cleanly. The grace
        # window gives Phase-1 cancel logic time to actually fly out cancels
        # AND lets the next audit cycle confirm zero drift.
        if regime == Regime.KILLED and self._kill_latched_at is None:
            self._kill_latched_at = now
        if (regime == Regime.KILLED
                and self.config.auto_terminate_on_kill
                and not self._auto_terminated
                and self._kill_latched_at is not None
                and (now - self._kill_latched_at) >= self.config.auto_terminate_grace_sec):
            active_count = sum(1 for e in self.executors_info if not e.is_done)
            drift_active = bool(self._last_audit_results.get("_drift_active"))
            if active_count == 0 and not drift_active:
                self._auto_terminated = True
                self.logger().critical(
                    f"[auto_terminate] KILLED for "
                    f"{now - self._kill_latched_at:.0f}s with no active executors "
                    f"and drift={drift_active}; sending SIGTERM to pid={os.getpid()} "
                    f"(reason={self._kill_reason})"
                )
                try:
                    os.kill(os.getpid(), signal.SIGTERM)
                except Exception as e:
                    self.logger().error(
                        f"[auto_terminate] os.kill failed: {type(e).__name__}: {e}"
                    )

        # 6. Bps relations (for diagnostic CSV columns)
        fair_slow = self._signal.fair_brl_slow
        basis_bps = self._signal.basis_bps
        maker_vs_fair_bps = self._bps_ratio(local_mid, fair_slow)
        taker_vs_fair_bps = self._bps_ratio(taker_local_mid, fair_slow)
        maker_vs_taker_bps = self._bps_ratio(local_mid, taker_local_mid)
        taker_local_spread_bps = self._spread_bps(taker_local_bid, taker_local_ask)

        # 6.5. Pure arb detection (VWAP-based).
        # GROSS values stay for telemetry / arb_alert_threshold near-miss
        # detection. NET values (gross − round-trip taker fees) are what the
        # spawn gate in _maybe_create_arb_action compares against
        # arb_min_profitability — consistent with the executor's own NET
        # execute gate and with the MM controller's NET min/target/max
        # profitability semantics.
        arb_long_gross_bps = self._compute_arb_gross_bps("long")
        arb_short_gross_bps = self._compute_arb_gross_bps("short")
        arb_tx_cost_bps = self._estimate_arb_tx_cost_bps()
        # Preserve the -9999 sentinel when one leg lacks depth.
        arb_long_net_bps = (
            arb_long_gross_bps - arb_tx_cost_bps
            if arb_long_gross_bps > Decimal("-1000")
            else arb_long_gross_bps
        )
        arb_short_net_bps = (
            arb_short_gross_bps - arb_tx_cost_bps
            if arb_short_gross_bps > Decimal("-1000")
            else arb_short_gross_bps
        )

        # 6.5b. Fresh-REST override for BitPreco-side staleness. Only fires
        # when cached net_bps is close to threshold (gate inside helper).
        arb_fresh_used = False
        fresh_long, fresh_short = await self._maybe_refresh_fresh_arb_bps(
            now, arb_long_net_bps, arb_short_net_bps, arb_tx_cost_bps,
        )
        if fresh_long is not None:
            arb_long_net_bps = fresh_long
            arb_fresh_used = True
        if fresh_short is not None:
            arb_short_net_bps = fresh_short
            arb_fresh_used = True

        # 6.6. Arb near-threshold alert (fires even with enable_pure_arb=False)
        thr = Decimal(str(self.config.arb_alert_threshold_bps))
        arb_near_threshold = thr > 0 and (
            arb_long_gross_bps > thr or arb_short_gross_bps > thr
        )
        if arb_near_threshold and (now - self._arb_last_alert_time) >= 30.0:
            side = "long" if arb_long_gross_bps >= arb_short_gross_bps else "short"
            best = arb_long_gross_bps if side == "long" else arb_short_gross_bps
            self.logger().info(
                f"[arb_alert] spread {best:.2f}bps ({side}) — near threshold "
                f"{self.config.arb_alert_threshold_bps}bps | "
                f"long={arb_long_gross_bps:.2f} short={arb_short_gross_bps:.2f}"
            )
            self._arb_last_alert_time = now

        # 6.7. Reset daily arb counters at UTC date rollover
        today = datetime.utcfromtimestamp(now).strftime("%Y-%m-%d")
        if today != self._arb_last_reset_day:
            if self._arb_last_reset_day:  # don't log on first init
                self.logger().info(
                    f"Arb daily counters reset (was failures={self._arb_failures_today}, "
                    f"loss={self._arb_realized_loss_today})"
                )
            self._arb_failures_today = 0
            self._arb_realized_loss_today = Decimal("0")
            self._arb_last_reset_day = today

        # 6.7b. Reset daily realized-PnL counter at UTC date rollover (independent
        # day key from arb so the two paths stay decoupled — if one ever moves
        # to a different rollover schedule the other is unaffected).
        if today != self._daily_pnl_last_reset_day:
            if self._daily_pnl_last_reset_day:
                self.logger().info(
                    f"Daily realized PnL reset (was {self._daily_realized_pnl})"
                )
            self._daily_realized_pnl = Decimal("0")
            self._daily_pnl_last_reset_day = today

        # 7. Active executor count
        active = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: not e.is_done,
        )

        # 8. Populate processed_data + log
        self.processed_data = {
            "timestamp": now,
            "iso_time": datetime.utcfromtimestamp(now).isoformat() + "Z",
            "regime": regime,
            "signal_quality": self._signal.signal_quality,
            "local_bid": local_bid, "local_ask": local_ask, "local_mid": local_mid,
            "local_spread_bps": self._signal.local_spread_bps,
            "taker_local_bid": taker_local_bid, "taker_local_ask": taker_local_ask,
            "taker_local_mid": taker_local_mid,
            "taker_local_spread_bps": taker_local_spread_bps,
            "leader_bid": leader_bid, "leader_ask": leader_ask,
            "leader_mid": self._signal.leader_mid,
            "fx_bid": fx_bid, "fx_ask": fx_ask,
            "fx_mid_raw": self._signal.fx_mid_raw,
            "fx_mid_ema": self._signal.fx_mid_ema or Decimal("0"),
            "fair_brl_fast": self._signal.fair_brl_fast,
            "fair_brl_slow": fair_slow,
            "basis_bps": basis_bps,
            "maker_vs_fair_bps": maker_vs_fair_bps,
            "taker_vs_fair_bps": taker_vs_fair_bps,
            "maker_vs_taker_bps": maker_vs_taker_bps,
            "lead_bps_per_window": {
                w: self._signal.lead_signal_bps(w)
                for w in self.config.lead_windows_seconds
            },
            "best_lead_bps": best_lead,
            "should_cancel": should_cancel,
            "cancel_reason": cancel_reason or "",
            "maker_base": maker_base, "maker_quote": maker_quote,
            "taker_base": taker_base, "taker_quote": taker_quote,
            "combined_base": combined_base, "combined_quote": combined_quote,
            "combined_pct": combined_pct, "inventory_skew": inventory_skew,
            "target_prof_buy": target_buy, "target_prof_sell": target_sell,
            "n_active_executors": len(active),
            "shadow_mode": self.config.shadow_mode,
            "arb_long_gross_bps": arb_long_gross_bps,
            "arb_short_gross_bps": arb_short_gross_bps,
            "arb_long_net_bps": arb_long_net_bps,
            "arb_short_net_bps": arb_short_net_bps,
            "arb_tx_cost_bps": arb_tx_cost_bps,
            "arb_fresh_used": arb_fresh_used,
            "arb_near_threshold": arb_near_threshold,
            "arb_failures_today": self._arb_failures_today,
            "arb_realized_loss_today": self._arb_realized_loss_today,
            # Inventory audit (populated by _run_inventory_audit; default 0)
            "audit_btc_actual": self._last_audit_results.get("BTC", {}).get("actual", Decimal("0")),
            "audit_btc_target": self._last_audit_results.get("BTC", {}).get("target", Decimal("0")),
            "audit_btc_delta": self._last_audit_results.get("BTC", {}).get("delta", Decimal("0")),
            "audit_drift_active": 1 if self._last_audit_results.get("_drift_active") else 0,
            "audit_inflight_active": 1 if self._last_audit_results.get("_inflight_active") else 0,
            "boot_paused": 1 if self._boot_paused else 0,
            # PnL-safety telemetry (drives CSV columns and external watchdog).
            "daily_realized_pnl": self._daily_realized_pnl,
            "session_pnl_total": self._session_pnl_total,
            "session_pnl_peak": self._session_pnl_peak,
            "session_drawdown": self._session_pnl_peak - self._session_pnl_total,
            "consecutive_losing_fills": self._consecutive_losing_fills,
            "hourly_burn": self._hourly_burn(now),
        }

        # Tier 3 throttling: CSV write at most once per second, even when running
        # at 200ms tier-1 cadence. State (self.processed_data) is updated every
        # tier-1 tick; only the CSV row is throttled to keep I/O sane.
        if (now - self._last_full_update) >= 1.0:
            if self._csv is not None:
                self._csv.log(self.processed_data)
            self._last_full_update = now

    # ------------------------------------------------------------------ #
    # Regime / risk gates                                                #
    # ------------------------------------------------------------------ #
    def _is_kill_switch_active(self) -> bool:
        ks = self.config.kill_switch_file
        if not ks:
            return False
        try:
            return os.path.exists(ks)
        except Exception:
            return False

    def _hourly_burn(self, now: float) -> Decimal:
        """Return the net PnL sum across the last 3600 seconds.

        Trims expired entries in-place (deque popleft from the front). Cheap
        in the steady state because closed executors are rare (~tens per hour
        at worst), so the deque stays small.
        """
        cutoff = now - 3600.0
        while self._hourly_pnl_history and self._hourly_pnl_history[0][0] < cutoff:
            self._hourly_pnl_history.popleft()
        if not self._hourly_pnl_history:
            return Decimal("0")
        total = Decimal("0")
        for _, pnl in self._hourly_pnl_history:
            total += pnl
        return total

    def _compute_regime(
        self,
        now: float,
        best_lead: Optional[Decimal],
        taker_local_bid: Decimal,
        taker_local_ask: Decimal,
    ) -> tuple[Regime, Optional[str]]:
        """Returns (regime, cancel_reason). cancel_reason populated only when
        regime is PAUSED or KILLED."""
        # 1. KILLED is sticky once set (only reload clears it).
        if self._kill_reason:
            return Regime.KILLED, self._kill_reason

        # 2. Kill switch file
        if self._is_kill_switch_active():
            self._kill_reason = "KILL_SWITCH"
            return Regime.KILLED, "KILL_SWITCH"

        # 3. Circuit breakers (PnL-based — each measures a different failure mode)

        # NOTE (2026-05-12): HEDGE_FAILURES_N gate removed. It was based on
        # the ``_drift_consecutive_audits`` strike rule which fired on false
        # positives from stale balance / hedge-in-flight races. The barrier-
        # pattern audit (see _run_inventory_audit) eliminates those races at
        # the source, so a separate "N strikes" rule serves no purpose.
        # Real hedge failures still surface via UNREALIZED_LOSS (drift × mid
        # over the absolute threshold) and DAILY_LOSS_LIMIT (cumulative PnL).

        # 3a. DAILY_LOSS_LIMIT — realized PnL since UTC midnight.
        if self._daily_realized_pnl <= -self.config.max_daily_loss_quote:
            self._kill_reason = "DAILY_LOSS_LIMIT"
            return Regime.KILLED, "DAILY_LOSS_LIMIT"

        # 3c. SESSION_DRAWDOWN — peak-to-trough since process start.
        # Catches "made 80 BRL by lunch, gave back 60 by dinner" — net daily
        # is +20 (under DAILY_LOSS_LIMIT) but drawdown is 60 BRL.
        drawdown = self._session_pnl_peak - self._session_pnl_total
        if drawdown >= self.config.max_session_drawdown_quote:
            self._kill_reason = f"SESSION_DRAWDOWN_{drawdown:.2f}"
            return Regime.KILLED, self._kill_reason

        # 3d. LOSING_STREAK — N consecutive closed executors with net_pnl < 0.
        # Fast-trip for adverse-selection clusters before daily_loss accumulates.
        if self._consecutive_losing_fills >= self.config.max_consecutive_losing_fills:
            self._kill_reason = f"LOSING_STREAK_{self._consecutive_losing_fills}"
            return Regime.KILLED, self._kill_reason

        # 3e. HOURLY_BURN — sum of net_pnl over the last 3600s.
        # Detects slow bleeds before daily_loss bites (e.g. consistent ~5 BRL
        # losses across an hour adds to 50 BRL — half the daily limit, but a
        # clear signal something is broken).
        burn = self._hourly_burn(now)
        if burn <= -self.config.max_hourly_burn_quote:
            self._kill_reason = f"HOURLY_BURN_{burn:.2f}"
            return Regime.KILLED, self._kill_reason

        # 3f. UNREALIZED_LOSS — sum of |drift| * mid_price across base assets.
        # Defense-in-depth against the case where inventory_audit's auto_rebalance
        # cannot keep up with growing drift (e.g. taker side keeps rejecting
        # MARKET orders). max_drift_quote triggers rebalance at e.g. 60 BRL;
        # this gate trips KILL once the cumulative exposure crosses the higher
        # threshold (default 100 BRL) without waiting for the 30-audit DRIFT_STUCK.
        total_drift_quote = Decimal("0")
        for asset, info in self._last_audit_results.items():
            if asset.startswith("_") or not isinstance(info, dict):
                continue
            total_drift_quote += info.get("delta_quote", Decimal("0"))
        if total_drift_quote >= self.config.max_unrealized_loss_quote:
            self._kill_reason = f"UNREALIZED_LOSS_{total_drift_quote:.2f}"
            return Regime.KILLED, self._kill_reason

        # 4. Hard PAUSE gates
        if self._signal.is_any_stale:
            quality = self._signal.signal_quality
            return Regime.PAUSED, f"FEED_STALE_{quality.value}"

        if abs(self._signal.basis_bps) > self.config.basis_hard_threshold_bps:
            return Regime.PAUSED, "BASIS_EXTREME"

        if self._signal.local_spread_bps > self.config.max_local_spread_bps:
            return Regime.PAUSED, "LOCAL_SPREAD_WIDE"

        # Also check taker spread (we hedge there, so wide taker = bad slippage).
        taker_spread_bps = self._spread_bps(taker_local_bid, taker_local_ask)
        if taker_spread_bps > self.config.max_local_spread_bps:
            return Regime.PAUSED, "TAKER_SPREAD_WIDE"

        # 5. Strong lead signal → cancel preemptively
        if best_lead is not None and abs(best_lead) > self.config.fast_cancel_threshold_bps:
            return Regime.PAUSED, "LEAD_SIGNAL_STRONG"

        # 6. Warmup
        if (now - self._started_at) < self.config.warmup_seconds:
            return Regime.WARMUP, None

        # 7. Need a valid lead signal to trade in OK mode
        if best_lead is None:
            # Buffer not yet covering the largest window: stay in warmup.
            return Regime.WARMUP, None

        # 8. Soft degradation (one feed slow but operable, no lead adj)
        quality = self._signal.signal_quality
        if quality != SignalQuality.OK:
            return Regime.DEGRADED, None

        # 9. Soft cancel threshold → DEGRADED (don't cancel, but don't be aggressive)
        if abs(best_lead) > self.config.soft_cancel_threshold_bps:
            return Regime.DEGRADED, None

        return Regime.OK, None

    # ------------------------------------------------------------------ #
    # Action emission                                                    #
    # ------------------------------------------------------------------ #
    def determine_executor_actions(self) -> List[ExecutorAction]:
        """Three-phase decision: cancel-gate → anti-churn → create."""
        if not self.processed_data:
            return []

        actions: List[ExecutorAction] = []
        pd = self.processed_data
        regime: Regime = pd["regime"]
        now: float = pd["timestamp"]

        active_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: not e.is_done,
        )

        # === Phase 1: defensive cancellation ===
        if regime in (Regime.PAUSED, Regime.KILLED):
            for executor in active_executors:
                actions.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=executor.config.id,
                    keep_position=False,
                ))
            if actions:
                self._last_action_time = now
                self._last_cancel_reason = pd["cancel_reason"]
                self.logger().info(
                    f"Cancel gate: {pd['cancel_reason']}. "
                    f"Stopped {len(actions)} active executor(s)."
                )
            return actions

        # === Audit barrier: stop-the-world while audit revalidates drift ===
        # Triggered by _run_inventory_audit when it suspects drift and wants
        # an authoritative balance read. We stop ALL active executors and
        # refuse to create new ones until the barrier resolves (CANCELLING →
        # VALIDATING → IDLE). Typical duration: 1-3s.
        if self._barrier_state == "CANCELLING":
            for executor in active_executors:
                actions.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=executor.config.id,
                    keep_position=False,
                ))
            if actions:
                self._last_action_time = now
                self.logger().info(
                    f"[barrier] stopping {len(actions)} active executor(s) "
                    f"to reach quiescence before drift validation"
                )
            return actions
        # While VALIDATING, do not create new executors either — the audit
        # is computing the authoritative drift; new orders would mutate state.
        if self._barrier_state == "VALIDATING":
            return []

        # === No new orders in WARMUP or KILLED ===
        if regime == Regime.WARMUP or regime == Regime.KILLED:
            return []

        # === Boot-paused (Solution C): refuse new orders until cleanup + audit pass ===
        # Phase 1 cancel logic above still runs (so we cancel anything we observe);
        # this only blocks NEW order creation. Once startup_cleanup confirms 0 orphan
        # orders AND initial audit shows 0 drift, _boot_paused becomes False.
        if self._boot_paused:
            return []

        # === Shadow mode: log only, never create ===
        if self.config.shadow_mode:
            return []

        # === Phase 2: anti-churn cooldown ===
        if (now - self._last_action_time) < self.config.min_requote_interval_sec:
            return []

        # === Arb executor age-out ===
        # An arb executor that polls profitability forever without firing
        # blocks all subsequent arbs (one-at-a-time invariant) AND can race
        # to execute concurrently with newly-spawned arbs if the spread
        # eventually crosses the threshold. Hard-kill any arb that has
        # been RUNNING longer than arb_executor_max_age_sec.
        for executor in active_executors:
            ex_type = getattr(getattr(executor, "config", None), "type", "")
            if ex_type != "lead_lag_arbitrage_executor":
                continue
            spawn_ts = getattr(executor, "timestamp", None) or getattr(
                getattr(executor, "config", None), "timestamp", None
            )
            if spawn_ts is None:
                continue
            age = now - float(spawn_ts)
            if age > self.config.arb_executor_max_age_sec:
                self.logger().warning(
                    f"[arb_age_out] killing arb executor "
                    f"{getattr(executor.config, 'id', '?')} age={age:.0f}s "
                    f"(> {self.config.arb_executor_max_age_sec:.0f}s) — "
                    f"never executed."
                )
                actions.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=executor.config.id,
                    keep_position=False,
                ))
        if actions:
            # Don't try to spawn new ones in the same tick where we're killing
            # stale ones; let the cancellations settle first.
            self._last_action_time = now
            return actions

        # === Phase 3a: pure arb gate (capital-aware, has its own cooldown) ===
        if self.config.enable_pure_arb:
            arb_action = self._maybe_create_arb_action(now, active_executors)
            if arb_action is not None:
                self._last_action_time = now
                return [arb_action]

        # === Phase 3b: XEMM creation (gated by enable_market_making) ===
        if not self.config.enable_market_making:
            return []   # arb-only mode

        active_buys = [
            e for e in active_executors
            if hasattr(e.config, "maker_side") and e.config.maker_side == TradeType.BUY
        ]
        active_sells = [
            e for e in active_executors
            if hasattr(e.config, "maker_side") and e.config.maker_side == TradeType.SELL
        ]

        target_buy: Decimal = pd["target_prof_buy"]
        target_sell: Decimal = pd["target_prof_sell"]

        # Per-exchange balance gates (percent-buffer model — see config doc).
        # BUY maker → SELL taker hedge → needs taker BASE >= order_amount * (1 + buf)
        # SELL maker → BUY  taker hedge → needs taker QUOTE >= order_amount * mid * (1 + buf)
        taker_base = pd["taker_base"]
        taker_quote = pd["taker_quote"]
        taker_mid = pd["taker_local_mid"]
        order_amt = self.config.order_amount
        buf_mul = Decimal("1") + self.config.taker_hedge_buffer_pct
        min_base_required = order_amt * buf_mul
        min_quote_required = order_amt * taker_mid * buf_mul if taker_mid > 0 else Decimal("0")

        _warn_balance = False
        if len(active_buys) == 0:
            if taker_base >= min_base_required:
                actions.append(self._make_create_action(TradeType.BUY, target_buy, now))
            else:
                _warn_balance = True

        if len(active_sells) == 0:
            if min_quote_required > 0 and taker_quote >= min_quote_required:
                actions.append(self._make_create_action(TradeType.SELL, target_sell, now))
            else:
                _warn_balance = True

        if _warn_balance and (now - self._last_balance_warn_ts) >= 60.0:
            self._last_balance_warn_ts = now
            if taker_base < min_base_required:
                self.logger().warning(
                    f"[balance_gate] BUY side paused: taker_base ({taker_base}) "
                    f"< required ({min_base_required:.8f}) "
                    f"= order_amount × (1 + {self.config.taker_hedge_buffer_pct})"
                )
            if min_quote_required > 0 and taker_quote < min_quote_required:
                self.logger().warning(
                    f"[balance_gate] SELL side paused: taker_quote ({taker_quote}) "
                    f"< required ({min_quote_required:.4f}) "
                    f"= order_amount × taker_mid × (1 + {self.config.taker_hedge_buffer_pct})"
                )

        if actions:
            self._last_action_time = now

        return actions

    def _make_create_action(
        self, maker_side: TradeType, target_profitability: Decimal, now: float,
    ) -> CreateExecutorAction:
        if maker_side == TradeType.BUY:
            buying = ConnectorPair(
                connector_name=self.config.maker_connector,
                trading_pair=self.config.maker_trading_pair)
            selling = ConnectorPair(
                connector_name=self.config.taker_connector,
                trading_pair=self.config.taker_trading_pair)
        else:
            buying = ConnectorPair(
                connector_name=self.config.taker_connector,
                trading_pair=self.config.taker_trading_pair)
            selling = ConnectorPair(
                connector_name=self.config.maker_connector,
                trading_pair=self.config.maker_trading_pair)

        # Priority 4: pass current lead signal + adjustment params to executor.
        pd = self.processed_data or {}
        best_lead = pd.get("best_lead_bps") or Decimal("0")
        cfg = XEMMLeadLagExecutorConfig(
            controller_id=self.config.id,
            timestamp=now,
            buying_market=buying,
            selling_market=selling,
            maker_side=maker_side,
            order_amount=self.config.order_amount,
            min_profitability=self.config.min_profitability,
            target_profitability=target_profitability,
            max_profitability=self.config.max_profitability,
            placement_profitability_buffer=self.config.placement_profitability_buffer,
            lead_signal_bps=best_lead,
            placement_lead_aware_delta_bps=self.config.placement_lead_aware_delta_bps,
            placement_lead_signal_threshold_bps=self.config.placement_lead_signal_threshold_bps,
        )
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=cfg,
        )

    # ------------------------------------------------------------------ #
    # Pure arbitrage helpers                                             #
    # ------------------------------------------------------------------ #
    def _has_active_arb_executor(self, active_executors) -> bool:
        """Atomicity: only one arb executor at a time.

        Side-effect: when an arb is observed in ``active_executors``, the
        spawn-race pending flag is cleared (the framework has caught up and
        registered the executor we recently dispatched).
        """
        active = any(
            getattr(e.config, "type", "") == "lead_lag_arbitrage_executor"
            and not e.is_done
            for e in active_executors
        )
        if active:
            self._arb_spawn_pending_until = 0.0
        return active

    def _purge_old_arb_history(self, now: float) -> None:
        """Drop history older than 1h (sliding window for arb_max_per_hour)."""
        cutoff = now - 3600.0
        self._arb_history = [t for t in self._arb_history if t >= cutoff]

    def _arb_threshold(self, side: str, best_lead_bps: Optional[Decimal]) -> Decimal:
        """
        Lead-aware NET-bps threshold for the given side. Returned value is
        compared against ``_compute_arb_net_bps(side)`` in the spawn gate.
        - Lead in dead zone: base threshold (= arb_min_profitability bps)
        - Lead favours arb: aggressive (lower threshold)
        - Lead opposes arb: conservative (higher threshold)
        """
        base_bps = self.config.arb_min_profitability * Decimal("10000")
        if best_lead_bps is None:
            return base_bps
        if abs(best_lead_bps) < self.config.arb_lead_signal_threshold_bps:
            return base_bps
        favours = (
            (side == "long" and best_lead_bps > 0)
            or (side == "short" and best_lead_bps < 0)
        )
        if favours:
            return base_bps - self.config.arb_lead_aggressive_delta * Decimal("10000")
        return base_bps + self.config.arb_lead_conservative_delta * Decimal("10000")

    def _has_capital_for_arb(self, side: str) -> bool:
        """
        Dynamic capital check: verify free balance on each leg.
        - long: BUY taker (needs taker_quote), SELL maker (needs maker_base)
        - short: BUY maker (needs maker_quote), SELL taker (needs taker_base)
        Uses 1% buffer to account for fees + small price moves.
        """
        pd = self.processed_data
        amount = self.config.arb_order_amount
        # Reference price for quote calculation: VWAP of the buy leg
        if side == "long":
            buy_vwap_ref = self._vwap_for_amount(
                self.config.taker_connector, self.config.taker_trading_pair,
                is_buy=True, amount=amount)
            if buy_vwap_ref is None:
                return False
            needed_quote_buy_side = amount * buy_vwap_ref * Decimal("1.01")
            needed_base_sell_side = amount * Decimal("1.01")
            return (pd["taker_quote"] >= needed_quote_buy_side
                    and pd["maker_base"] >= needed_base_sell_side)
        else:  # short
            buy_vwap_ref = self._vwap_for_amount(
                self.config.maker_connector, self.config.maker_trading_pair,
                is_buy=True, amount=amount)
            if buy_vwap_ref is None:
                return False
            needed_quote_buy_side = amount * buy_vwap_ref * Decimal("1.01")
            needed_base_sell_side = amount * Decimal("1.01")
            return (pd["maker_quote"] >= needed_quote_buy_side
                    and pd["taker_base"] >= needed_base_sell_side)

    def _maybe_create_arb_action(
        self, now: float, active_executors,
    ) -> Optional[CreateExecutorAction]:
        """
        Returns a CreateExecutorAction for a LeadLagArbitrageExecutor if all
        gates pass; else None.

        Spawn gate uses NET bps (gross spread minus round-trip taker fees),
        consistent with the executor's own NET execution gate and with the
        MM controller's `min/target/max_profitability` (also NET). Every
        rejection emits a throttled debug-level log so the operator can
        see why an arb didn't spawn in any given window.
        """
        # Throttle to avoid log spam — these gates fire every tick.
        def _gate_log(reason: str) -> None:
            if (now - self._arb_last_gate_log_ts) >= 5.0:
                self.logger().info(f"[arb_gate_skip] {reason}")
                self._arb_last_gate_log_ts = now

        # Spawn-race guard: if we recently returned a CreateExecutorAction
        # for an arb but the framework hasn't yet registered it in
        # executors_info, the next call to _has_active_arb_executor would
        # return False (stale snapshot) and we'd spawn duplicates. The flag
        # is cleared as soon as _has_active_arb_executor observes the
        # registered executor; otherwise it auto-clears after 30 s so a
        # never-registered spawn doesn't block forever.
        if now < self._arb_spawn_pending_until:
            _gate_log(
                f"pending spawn registration "
                f"({self._arb_spawn_pending_until - now:.1f}s grace remaining)"
            )
            return None

        # Circuit breakers
        if now < self._arb_paused_until:
            _gate_log(
                f"failure_pause active ({self._arb_paused_until - now:.0f}s "
                f"remaining)"
            )
            return None
        if self._arb_failures_today >= self.config.arb_max_failures_per_day:
            _gate_log(
                f"max_failures_per_day reached "
                f"({self._arb_failures_today}/{self.config.arb_max_failures_per_day})"
            )
            return None
        if self._arb_realized_loss_today >= self.config.arb_daily_loss_limit_quote:
            _gate_log(
                f"daily_loss_limit reached "
                f"({self._arb_realized_loss_today}/{self.config.arb_daily_loss_limit_quote})"
            )
            return None

        # Atomicity: only one arb at a time
        if self._has_active_arb_executor(active_executors):
            _gate_log("active arb executor present (only 1 at a time)")
            return None

        # Cooldown
        if (now - self._last_arb_time) < self.config.arb_min_interval_sec:
            _gate_log(
                f"cooldown active "
                f"({self.config.arb_min_interval_sec - (now - self._last_arb_time):.1f}s "
                f"remaining)"
            )
            return None

        # Sliding window rate limit
        self._purge_old_arb_history(now)
        if len(self._arb_history) >= self.config.arb_max_per_hour:
            _gate_log(
                f"hourly rate limit "
                f"({len(self._arb_history)}/{self.config.arb_max_per_hour})"
            )
            return None

        pd = self.processed_data
        # NET = gross - tx_cost. Computed fresh each tick because tx_cost
        # depends on connector fees, which can change (VIP tier, promotion).
        long_net = pd.get("arb_long_net_bps", Decimal("-9999"))
        short_net = pd.get("arb_short_net_bps", Decimal("-9999"))
        long_gross = pd.get("arb_long_gross_bps", Decimal("-9999"))
        short_gross = pd.get("arb_short_gross_bps", Decimal("-9999"))
        best_lead = pd.get("best_lead_bps")

        threshold_long = self._arb_threshold("long", best_lead)
        threshold_short = self._arb_threshold("short", best_lead)

        candidates = []
        if long_net > threshold_long:
            candidates.append(("long", long_net, long_gross))
        if short_net > threshold_short:
            candidates.append(("short", short_net, short_gross))

        if not candidates:
            # Only log when we have at least usable spread data — silent
            # if VWAP probes returned -9999 (no depth) since that's
            # market state, not a tunable gate.
            if long_net > Decimal("-1000") or short_net > Decimal("-1000"):
                _gate_log(
                    f"edge below threshold | "
                    f"long net={long_net:.2f}bps thr={threshold_long:.2f}bps "
                    f"(gross={long_gross:.2f}) | "
                    f"short net={short_net:.2f}bps thr={threshold_short:.2f}bps "
                    f"(gross={short_gross:.2f})"
                )
            return None

        # Pick the side with the higher NET edge.
        side, edge_net_bps, edge_gross_bps = max(candidates, key=lambda c: c[1])

        # Dynamic capital check
        if not self._has_capital_for_arb(side):
            if self.config.arb_capital_strategy == "skip_if_insufficient":
                self.logger().info(
                    f"[arb_gate_skip] Arb {side} net={edge_net_bps:.2f}bps "
                    f"(gross={edge_gross_bps:.2f}) SKIPPED: "
                    f"insufficient free capital."
                )
                return None
            # cancel_xemm_to_free: not implemented in this iteration — log and skip
            self.logger().warning(
                f"[arb_gate_skip] Arb {side} net={edge_net_bps:.2f}bps "
                f"(gross={edge_gross_bps:.2f}): "
                f"cancel_xemm_to_free strategy not yet implemented; skipping."
            )
            return None

        # All gates passed — spawn.
        self._last_arb_time = now
        self._arb_history.append(now)
        # Spawn-race guard: block further spawns for up to 30 s, OR until
        # _has_active_arb_executor confirms the executor registered.
        self._arb_spawn_pending_until = now + 30.0
        self.logger().warning(
            f"[arb_spawn] SPAWNING {side} arb: "
            f"net={edge_net_bps:.2f}bps gross={edge_gross_bps:.2f}bps "
            f"threshold={threshold_long if side == 'long' else threshold_short:.2f}bps "
            f"(lead={best_lead}) | history_1h={len(self._arb_history)} "
            f"order_amount={self.config.arb_order_amount}"
        )
        return self._make_arb_action(side, edge_net_bps, now)

    def _make_arb_action(
        self, side: str, edge_bps: Decimal, now: float,
    ) -> CreateExecutorAction:
        if side == "long":
            buying = ConnectorPair(
                connector_name=self.config.taker_connector,
                trading_pair=self.config.taker_trading_pair)
            selling = ConnectorPair(
                connector_name=self.config.maker_connector,
                trading_pair=self.config.maker_trading_pair)
        else:  # short
            buying = ConnectorPair(
                connector_name=self.config.maker_connector,
                trading_pair=self.config.maker_trading_pair)
            selling = ConnectorPair(
                connector_name=self.config.taker_connector,
                trading_pair=self.config.taker_trading_pair)

        cfg = LeadLagArbitrageExecutorConfig(
            controller_id=self.config.id,
            timestamp=now,
            buying_market=buying,
            selling_market=selling,
            order_amount=self.config.arb_order_amount,
            min_profitability=self.config.arb_min_profitability,
            arb_max_unwind_slippage_bps=self.config.arb_max_unwind_slippage_bps,
            arb_unwind_strategy=self.config.arb_unwind_strategy,
        )
        self.logger().info(
            f"Spawning arb executor side={side} edge={edge_bps:.2f}bps "
            f"buying={buying.connector_name} selling={selling.connector_name}"
        )
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=cfg,
        )

    # ------------------------------------------------------------------ #
    # Profitability adjustment                                           #
    # ------------------------------------------------------------------ #
    def _compute_targets(
        self,
        best_lead: Optional[Decimal],
        inventory_skew: Decimal,
        basis_bps: Optional[Decimal] = None,
    ) -> tuple[Decimal, Decimal]:
        """
        Adjusts target_profitability per side:

        - Lead signal:
            best_lead > 0  →  fair has risen relative to local
                              → expect local to follow up
                              → favor BUY (target_buy down = more aggressive bid)
                              → protect SELL (target_sell up = wider ask)
        - Inventory skew:
            inv_skew > 0   →  too much base
                              → suppress BUY (target_buy up)
                              → favor SELL (target_sell down)
        - Basis (Priority 5):
            basis_bps > 0  →  maker (e.g. Bybit) above taker (Binance)
                              → favor SELL on maker (target_sell down = more aggressive)
                              → suppress BUY on maker (target_buy up = wider)
        """
        base = self.config.target_profitability

        # Lead-based adjustment (only above the threshold)
        if (best_lead is not None
                and abs(best_lead) > self.config.profitability_adjust_threshold_bps):
            lead_adj = self.config.w_lead * best_lead / Decimal("10000")
            target_buy = base - lead_adj
            target_sell = base + lead_adj
        else:
            target_buy = base
            target_sell = base

        # Inventory-based adjustment
        inv_adj = inventory_skew * self.config.inventory_skew_strength
        target_buy = target_buy + inv_adj   # excess base → harder to buy
        target_sell = target_sell - inv_adj  # excess base → easier to sell

        # Basis-based directional adjustment (Priority 5)
        # basis_bps > 0 means maker is consistently above taker → SELL on maker
        # is statistically more profitable; bias targets accordingly.
        basis_strength = getattr(self.config, "basis_skew_strength", Decimal("0"))
        if basis_bps is not None and basis_strength > 0:
            basis_adj = Decimal(str(basis_bps)) * basis_strength
            target_buy = target_buy + basis_adj    # basis>0 → buy harder
            target_sell = target_sell - basis_adj  # basis>0 → sell easier

        # Clamp to [min, max]
        target_buy = max(
            self.config.min_profitability,
            min(self.config.max_profitability, target_buy),
        )
        target_sell = max(
            self.config.min_profitability,
            min(self.config.max_profitability, target_sell),
        )
        return target_buy, target_sell

    # ------------------------------------------------------------------ #
    # Static helpers                                                     #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _mid(bid: Decimal, ask: Decimal) -> Decimal:
        if bid <= 0 or ask <= 0:
            return Decimal("0")
        return (bid + ask) / Decimal("2")

    @staticmethod
    def _spread_bps(bid: Decimal, ask: Decimal) -> Decimal:
        if bid <= 0 or ask <= 0:
            return Decimal("0")
        mid = (bid + ask) / Decimal("2")
        if mid <= 0:
            return Decimal("0")
        return Decimal("10000") * (ask - bid) / mid

    @staticmethod
    def _bps_ratio(num: Decimal, den: Decimal) -> Decimal:
        if num <= 0 or den <= 0:
            return Decimal("0")
        return Decimal("10000") * (num / den - Decimal("1"))

    # ------------------------------------------------------------------ #
    # Status                                                             #
    # ------------------------------------------------------------------ #
    def to_format_status(self) -> List[str]:
        if not self.processed_data:
            return ["XEMMLeadLag: warming up (no data yet)"]

        pd = self.processed_data
        lines: List[str] = []
        lines.append(
            f"Mode: {'SHADOW' if self.config.shadow_mode else 'LIVE'} "
            f"| Regime: {pd['regime'].value if isinstance(pd['regime'], Regime) else pd['regime']} "
            f"| Signal: {pd['signal_quality'].value if isinstance(pd['signal_quality'], SignalQuality) else pd['signal_quality']}"
        )
        lines.append(
            f"basis: {self._fmt(pd['basis_bps'])} bps "
            f"| local_spread: {self._fmt(pd['local_spread_bps'])} bps "
            f"| taker_spread: {self._fmt(pd['taker_local_spread_bps'])} bps"
        )
        lead_per = pd.get("lead_bps_per_window") or {}
        lead_str = " ".join(
            f"lead_{w}s: {self._fmt(lead_per.get(w))}"
            for w in self.config.lead_windows_seconds
        )
        lines.append(f"{lead_str} | best: {self._fmt(pd['best_lead_bps'])} bps")
        lines.append(
            f"Maker {self.config.maker_connector}: "
            f"{self._fmt(pd['maker_base'])} base / {self._fmt(pd['maker_quote'])} quote "
            f"| Taker {self.config.taker_connector}: "
            f"{self._fmt(pd['taker_base'])} base / {self._fmt(pd['taker_quote'])} quote"
        )
        lines.append(
            f"Combined pct: {self._fmt(pd['combined_pct'])} "
            f"(target {self.config.inventory_target_pct}) "
            f"| skew: {self._fmt(pd['inventory_skew'])}"
        )
        lines.append(
            f"Targets — BUY: {self._fmt(pd['target_prof_buy'])} "
            f"SELL: {self._fmt(pd['target_prof_sell'])} "
            f"| active executors: {pd['n_active_executors']}"
        )
        if pd.get("cancel_reason"):
            lines.append(f"Last cancel: {pd['cancel_reason']}")
        return lines

    @staticmethod
    def _fmt(v) -> str:
        if v is None or v == "":
            return "-"
        if isinstance(v, Decimal):
            try:
                return f"{v:.4f}"
            except Exception:
                return str(v)
        return str(v)

    def on_stop(self):
        if self._csv is not None:
            self._csv.close()


