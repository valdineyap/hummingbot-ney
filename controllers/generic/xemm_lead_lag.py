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

Uses XEMMBRLExecutor (LIMIT_MAKER) instead of XEMMExecutor (LIMIT) to avoid
maker orders being filled as taker due to home-latency price drift.
"""
import csv
import os
import time
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Set

from pydantic import ConfigDict, Field, field_validator, model_validator

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import MarketDict, PriceType, TradeType
from hummingbot.strategy_v2.controllers.controller_base import (
    ControllerBase,
    ControllerConfigBase,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import (
    XEMMExecutorConfig,
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

    # === Inventory (per-exchange) ===
    inventory_target_pct: Decimal = Field(default=Decimal("0.5"))
    inventory_skew_strength: Decimal = Field(default=Decimal("0.001"))
    min_taker_base_for_sell_hedge: Decimal = Field(default=Decimal("0.0005"))
    min_taker_quote_for_buy_hedge: Decimal = Field(default=Decimal("200"))

    # === Warmup ===
    warmup_seconds: float = Field(default=20.0)

    # === Circuit breakers ===
    max_consecutive_hedge_failures: int = Field(default=1)
    max_daily_loss_quote: Decimal = Field(default=Decimal("100"))

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


class XEMMLeadLagController(ControllerBase):
    """XEMM controller with synthetic lead-lag signal."""

    def __init__(self, config: XEMMLeadLagConfig, *args, **kwargs):
        self.config = config
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

        self._started_at: float = time.time()
        self._last_action_time: float = 0.0
        self._last_cancel_reason: Optional[str] = None
        self._consecutive_hedge_failures: int = 0
        self._daily_realized_pnl: Decimal = Decimal("0")
        self._kill_reason: Optional[str] = None

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

    async def update_processed_data(self):
        now = self.market_data_provider.time()

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

        # 4. Compute lead signal + adjusted targets
        best_lead = self._signal.best_lead_signal_bps()
        target_buy, target_sell = self._compute_targets(best_lead, inventory_skew)

        # 5. Compute regime + cancel decision
        regime, cancel_reason = self._compute_regime(
            now=now,
            best_lead=best_lead,
            taker_local_bid=taker_local_bid,
            taker_local_ask=taker_local_ask,
        )
        should_cancel = regime in (Regime.PAUSED, Regime.KILLED)

        # 6. Bps relations (for diagnostic CSV columns)
        fair_slow = self._signal.fair_brl_slow
        basis_bps = self._signal.basis_bps
        maker_vs_fair_bps = self._bps_ratio(local_mid, fair_slow)
        taker_vs_fair_bps = self._bps_ratio(taker_local_mid, fair_slow)
        maker_vs_taker_bps = self._bps_ratio(local_mid, taker_local_mid)
        taker_local_spread_bps = self._spread_bps(taker_local_bid, taker_local_ask)

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
        }

        if self._csv is not None:
            self._csv.log(self.processed_data)

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

        # 3. Circuit breakers
        if self._consecutive_hedge_failures >= self.config.max_consecutive_hedge_failures:
            self._kill_reason = "HEDGE_FAILURES"
            return Regime.KILLED, "HEDGE_FAILURES"
        if self._daily_realized_pnl <= -self.config.max_daily_loss_quote:
            self._kill_reason = "DAILY_LOSS_LIMIT"
            return Regime.KILLED, "DAILY_LOSS_LIMIT"

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

        # === No new orders in WARMUP or KILLED ===
        if regime == Regime.WARMUP or regime == Regime.KILLED:
            return []

        # === Shadow mode: log only, never create ===
        if self.config.shadow_mode:
            return []

        # === Phase 2: anti-churn cooldown ===
        if (now - self._last_action_time) < self.config.min_requote_interval_sec:
            return []

        # === Phase 3: creation ===
        active_buys = [
            e for e in active_executors if e.config.maker_side == TradeType.BUY
        ]
        active_sells = [
            e for e in active_executors if e.config.maker_side == TradeType.SELL
        ]

        target_buy: Decimal = pd["target_prof_buy"]
        target_sell: Decimal = pd["target_prof_sell"]

        # Per-exchange balance gates
        # BUY maker on Bybit → SELL taker hedge on Binance → needs taker BASE
        # SELL maker on Bybit → BUY taker hedge on Binance → needs taker QUOTE
        taker_base = pd["taker_base"]
        taker_quote = pd["taker_quote"]

        if len(active_buys) == 0:
            if taker_base >= self.config.min_taker_base_for_sell_hedge:
                actions.append(self._make_create_action(TradeType.BUY, target_buy, now))
            else:
                self.logger().info(
                    f"Skipping BUY maker creation: taker_base ({taker_base}) "
                    f"< min ({self.config.min_taker_base_for_sell_hedge})."
                )

        if len(active_sells) == 0:
            if taker_quote >= self.config.min_taker_quote_for_buy_hedge:
                actions.append(self._make_create_action(TradeType.SELL, target_sell, now))
            else:
                self.logger().info(
                    f"Skipping SELL maker creation: taker_quote ({taker_quote}) "
                    f"< min ({self.config.min_taker_quote_for_buy_hedge})."
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

        cfg = XEMMExecutorConfig(
            controller_id=self.config.id,
            timestamp=now,
            buying_market=buying,
            selling_market=selling,
            maker_side=maker_side,
            order_amount=self.config.order_amount,
            min_profitability=self.config.min_profitability,
            target_profitability=target_profitability,
            max_profitability=self.config.max_profitability,
        )
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=cfg,
        )

    # ------------------------------------------------------------------ #
    # Profitability adjustment                                           #
    # ------------------------------------------------------------------ #
    def _compute_targets(
        self, best_lead: Optional[Decimal], inventory_skew: Decimal,
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


