"""
PMM Lead-Lag Skew controller for BTC-BRL on Binance Spot (Hummingbot V2).

Phase 1: bilateral PMM puro — no skew, no regime filter, no lead-lag.
Phase 2: live inventory tracking + size factors + one-sided mode + CSV signals.
Phase 3: inventory skew + volatility spread multiplier.
Phase 4: regime circuit breakers + max_net_position_quote kill.
Phase 5: lead-lag micro pause + synthetic fair_brl.
"""
import csv
import os
from decimal import Decimal
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.market_making_controller_base import (
    MarketMakingControllerBase,
    MarketMakingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction, StopExecutorAction

from controllers.market_making.pmm_lead_lag_utils import (
    RegimeContext,
    RegimeState,
    RegimeThresholds,
    SkewState,
    compute_inventory_state,
    compute_lead_state,
    compute_order_params,
    compute_regime_state,
    compute_side_permissions,
    compute_size_factors,
    compute_skew_state,
    compute_vol_state,
    compute_volatility_from_prices,
)


class PMMLeadLagSkewConfig(MarketMakingControllerConfigBase):
    """Configuration for PMM Lead-Lag Skew controller."""

    controller_name: str = "pmm_lead_lag_skew"

    # Spot BRL defaults — override base perpetual defaults
    connector_name: str = Field(
        default="binance",
        json_schema_extra={"prompt": "Enter connector name (e.g., binance): ", "prompt_on_new": True},
    )
    trading_pair: str = Field(
        default="BTC-BRL",
        json_schema_extra={"prompt": "Enter trading pair (e.g., BTC-BRL): ", "prompt_on_new": True},
    )
    leverage: int = Field(
        default=1,
        json_schema_extra={"prompt": "Leverage (1 for spot): "},
    )
    # MARKET rebalance fura o breakeven em spot BRL — desabilitado por padrão
    skip_rebalance: bool = Field(default=True)

    # Phase 1 spreads (bps expressed as fractions: 0.0010 = 10 bps)
    buy_spreads: List[float] = Field(
        default="0.0010,0.0020",
        json_schema_extra={"prompt": "Buy spreads (comma-separated fractions, e.g. 0.0010,0.0020): ", "prompt_on_new": True, "is_updatable": True},
    )
    sell_spreads: List[float] = Field(
        default="0.0010,0.0020",
        json_schema_extra={"prompt": "Sell spreads (comma-separated fractions, e.g. 0.0010,0.0020): ", "prompt_on_new": True, "is_updatable": True},
    )

    # ── Inventory (Phase 2) ────────────────────────────────────────────────
    target_inventory_base_pct: float = Field(
        default=0.5, ge=0.0, le=1.0,
        json_schema_extra={"prompt": "Target base inventory fraction [0-1]: ", "is_updatable": True},
    )
    inv_soft_band: float = Field(
        default=0.10, gt=0.0,
        json_schema_extra={"prompt": "Soft band for inventory (e.g. 0.10): ", "is_updatable": True},
    )
    inv_hard_band: float = Field(
        default=0.20, gt=0.0,
        json_schema_extra={"prompt": "Hard band for size shutdown (e.g. 0.20): ", "is_updatable": True},
    )
    inv_hard_cap: float = Field(
        default=0.30, gt=0.0,
        json_schema_extra={"prompt": "Hard cap triggering one-sided mode (e.g. 0.30): ", "is_updatable": True},
    )
    inv_kill: float = Field(
        default=0.40, gt=0.0,
        json_schema_extra={"prompt": "Kill threshold for inventory (e.g. 0.40): ", "is_updatable": True},
    )

    @field_validator("inv_hard_band")
    @classmethod
    def _hard_band_gt_soft(cls, v: float, info: ValidationInfo) -> float:
        soft = info.data.get("inv_soft_band")
        if soft is not None and v <= soft:
            raise ValueError(f"inv_hard_band ({v}) must be > inv_soft_band ({soft})")
        return v

    @field_validator("inv_hard_cap")
    @classmethod
    def _hard_cap_gt_hard_band(cls, v: float, info: ValidationInfo) -> float:
        hard = info.data.get("inv_hard_band")
        if hard is not None and v <= hard:
            raise ValueError(f"inv_hard_cap ({v}) must be > inv_hard_band ({hard})")
        return v

    @field_validator("inv_kill")
    @classmethod
    def _kill_gt_hard_cap(cls, v: float, info: ValidationInfo) -> float:
        cap = info.data.get("inv_hard_cap")
        if cap is not None and v <= cap:
            raise ValueError(f"inv_kill ({v}) must be > inv_hard_cap ({cap})")
        return v
    max_net_position_quote: Decimal = Field(
        default=Decimal("60"),
        json_schema_extra={"prompt": "Max net position in quote (e.g. 60 BRL): ", "is_updatable": True},
    )

    # ── Skew (Phase 3) ────────────────────────────────────────────────────
    w_inv: float = Field(
        default=1.0, ge=0.0,
        json_schema_extra={"prompt": "Inventory signal weight (Phase 3+): ", "is_updatable": True},
    )
    w_lead: float = Field(
        default=0.0, ge=0.0,
        json_schema_extra={"prompt": "Lead-lag signal weight (0 until Phase 5): ", "is_updatable": True},
    )
    skew_max_bps: float = Field(
        default=2.0, ge=0.0,
        json_schema_extra={"prompt": "Max skew in bps (Phase3=2, teto=8): ", "is_updatable": True},
    )
    min_maker_distance_bps: float = Field(default=1.0, gt=0.0)
    min_requote_bps: float = Field(
        default=1.0, ge=0.0,
        json_schema_extra={"prompt": "Min bps delta to refresh order (churn brake): ", "is_updatable": True},
    )
    force_requote_bps: float = Field(
        default=15.0, gt=0.0,
        json_schema_extra={"prompt": "Force-requote when delta >= bps (escape stale orders): ", "is_updatable": True},
    )

    # ── Volatility (Phase 3) ──────────────────────────────────────────────
    candles_connector: Optional[str] = Field(
        default=None,
        validate_default=True,
        json_schema_extra={"prompt": "Candles connector (blank = same as connector_name): "},
    )
    candles_trading_pair: Optional[str] = Field(
        default=None,
        validate_default=True,
        json_schema_extra={"prompt": "Candles pair (blank = same as trading_pair): "},
    )
    candles_interval: str = Field(default="1m")
    vol_short_length: int = Field(default=30, gt=1)   # ~30 minutes at 1m
    vol_ref_length: int = Field(default=360, gt=30)   # ~6 hours at 1m

    @field_validator("candles_connector", mode="before")
    @classmethod
    def _candles_connector_default(cls, v, info: ValidationInfo):
        if v is None or v == "":
            return info.data.get("connector_name")
        return v

    @field_validator("candles_trading_pair", mode="before")
    @classmethod
    def _candles_trading_pair_default(cls, v, info: ValidationInfo):
        if v is None or v == "":
            return info.data.get("trading_pair")
        return v

    # ── Lead-lag (Phase 5) ───────────────────────────────────────────────
    basis_deadband_bps: float = Field(default=3.0, ge=0.0)
    max_leader_staleness_sec: float = Field(default=5.0, gt=0.0)
    max_usdt_brl_staleness_sec: float = Field(default=15.0, gt=0.0)

    # Leader & FX feeds — both 1m candles by default. The micro signal uses
    # only mid_usdt + mid_brl (no FX delta); usdt_brl_ref is just a scale factor.
    leader_connector: Optional[str] = Field(
        default=None,
        validate_default=True,
        json_schema_extra={"prompt": "Leader connector (blank = same as connector_name): "},
    )
    leader_trading_pair: str = Field(
        default="BTC-USDT",
        json_schema_extra={"prompt": "Leader trading pair (e.g. BTC-USDT): "},
    )
    quote_rate_connector: Optional[str] = Field(
        default=None,
        validate_default=True,
        json_schema_extra={"prompt": "Quote-rate connector (blank = same as connector_name): "},
    )
    quote_rate_trading_pair: str = Field(
        default="USDT-BRL",
        json_schema_extra={"prompt": "Quote-rate pair (e.g. USDT-BRL): "},
    )

    # Micro pause (1–5s, BTC-USDT pure)
    lead_micro_window_sec: float = Field(default=5.0, gt=0.0)
    lead_micro_threshold_bps: float = Field(default=5.0, gt=0.0)
    lead_micro_dwell_sec: float = Field(default=3.0, gt=0.0)

    # Regime synthetic (30s–5min, fair_brl smoothed by EWM)
    lead_lag_short_window_sec: float = Field(default=30.0, gt=0.0)
    lead_lag_ewm_halflife_sec: float = Field(default=10.0, gt=0.0)
    lead_lag_z_window_sec: float = Field(default=600.0, gt=0.0)

    @field_validator("leader_connector", mode="before")
    @classmethod
    def _leader_connector_default(cls, v, info: ValidationInfo):
        if v is None or v == "":
            return info.data.get("connector_name")
        return v

    @field_validator("quote_rate_connector", mode="before")
    @classmethod
    def _quote_rate_connector_default(cls, v, info: ValidationInfo):
        if v is None or v == "":
            return info.data.get("connector_name")
        return v

    # ── Regime + kill switch (Phase 4) ───────────────────────────────────
    vol_degraded_threshold_mult: float = Field(
        default=3.0, gt=0.0,
        json_schema_extra={"prompt": "Vol ratio threshold to enter degraded (e.g. 3.0): ", "is_updatable": True},
    )
    vol_pause_threshold_mult: float = Field(
        default=5.0, gt=0.0,
        json_schema_extra={"prompt": "Vol ratio threshold to enter paused (e.g. 5.0): ", "is_updatable": True},
    )
    pause_basis_bps: float = Field(
        default=30.0, gt=0.0,
        json_schema_extra={"prompt": "|basis_bps| above this triggers paused (e.g. 30): ", "is_updatable": True},
    )
    pause_release_sec: float = Field(
        default=60.0, gt=0.0,
        json_schema_extra={"prompt": "Dwell seconds before exiting paused (e.g. 60): ", "is_updatable": True},
    )
    safe_mode_entry_sec: float = Field(
        default=120.0, gt=0.0,
        json_schema_extra={"prompt": "Sustained L1 seconds to enter safe mode (e.g. 120): ", "is_updatable": True},
    )
    safe_thrash_window_sec: float = Field(
        default=600.0, gt=0.0,
        json_schema_extra={"prompt": "Window for thrash detection in seconds (e.g. 600): ", "is_updatable": True},
    )
    max_session_drawdown_quote: Decimal = Field(
        default=Decimal("10"),
        json_schema_extra={"prompt": "Max session drawdown in quote (BRL) before kill: ", "is_updatable": True},
    )
    critical_error_threshold: int = Field(default=5, gt=0)

    @field_validator("vol_pause_threshold_mult")
    @classmethod
    def _pause_gt_degraded(cls, v: float, info: ValidationInfo) -> float:
        deg = info.data.get("vol_degraded_threshold_mult")
        if deg is not None and v <= deg:
            raise ValueError(f"vol_pause_threshold_mult ({v}) must be > vol_degraded_threshold_mult ({deg})")
        return v

    # ── Logging (Phase 2) ─────────────────────────────────────────────────
    csv_log_dir: str = Field(
        default="logs/pmm_lead_lag",
        json_schema_extra={"prompt": "Directory for signals.csv (relative to bot root): "},
    )
    csv_log_enabled: bool = Field(default=True)


class PMMLeadLagSkewController(MarketMakingControllerBase):
    """
    Pure Market Making controller with inventory skew and lead-lag defense.

    Phase 1: bilateral PMM at fixed spreads. No skew, no regime, no lead-lag.
    All signals computed and stored in processed_data for future phases.
    """

    def __init__(self, config: PMMLeadLagSkewConfig, *args, **kwargs):
        # max_records must be set before super().__init__ if used in get_candles_config
        self.max_records = max(config.vol_ref_length, config.vol_short_length) + 50
        super().__init__(config, *args, **kwargs)
        self.config = config
        # Hysteresis state for one-sided mode (§2.4)
        self._buy_enabled: bool = True
        self._sell_enabled: bool = True
        # CSV loggers (lazy init: opened on first write)
        self._csv_path: str = os.path.join(self.config.csv_log_dir, "signals.csv")
        self._csv_initialized: bool = False
        self._orders_csv_path: str = os.path.join(self.config.csv_log_dir, "orders.csv")
        self._orders_csv_initialized: bool = False
        # Phase 4 — regime state machine (§4.3). All carry-state lives in one
        # RegimeContext owned by the controller; compute_regime_state mutates it.
        # `last_l2_time = -inf` so the very first tick cannot be inside the dwell
        # window from a non-existent L2 entry.
        self._regime_ctx: RegimeContext = RegimeContext(last_l2_time=float("-inf"))
        self._regime_cause: str = ""
        self._seen_executor_ids: set = set()
        self._session_pnl: float = 0.0
        # Sustained-feed-stale tracking — bumps critical_error_count once per
        # stale episode after stale_episode_critical_sec (plan §4.1 L3 item 4).
        self._feed_stale_started_at: Optional[float] = None
        self._feed_stale_critical_recorded: bool = False
        # Phase 5 — lead-lag price history buffers and EWM state.
        # Each entry is (timestamp_sec, mid_brl, mid_usdt, usdt_brl_rate).
        # Length bounded by max(z_window, micro_window, lead_lag_short_window) + buffer.
        history_window = max(
            float(config.lead_lag_z_window_sec),
            float(config.lead_lag_short_window_sec),
            float(config.lead_micro_window_sec),
        ) + 10.0
        # Assume tick rate ≈ 1 Hz; cap at a few thousand to bound memory.
        self._lead_history_max_seconds: float = history_window
        self._lead_history: Deque[Tuple[float, float, float, float]] = deque(maxlen=4096)
        # EWM state for the synthetic fair_brl.
        self._fair_brl_smooth: float = 0.0
        self._fair_brl_smooth_ts: float = 0.0
        # Recent log-lag samples → adaptive sigma for z_lead.
        # (timestamp, lag_short) tuples within z_window_sec.
        self._lag_samples: Deque[Tuple[float, float]] = deque(maxlen=4096)
        # Last-update timestamps for staleness checks.
        self._last_mid_brl_ts: float = 0.0
        self._last_mid_usdt_ts: float = 0.0
        self._last_usdt_brl_ts: float = 0.0
        # Pause expiry (set in update_processed_data) — used by get_levels_to_execute.
        self._pause_buy_until: float = 0.0
        self._pause_sell_until: float = 0.0

    # ── Candles config (Phase 3) ─────────────────────────────────────────

    def get_candles_config(self) -> List[CandlesConfig]:
        """
        Subscribe to:
          - BTC-BRL 1m candles for volatility computation (Phase 3).
          - Leader (BTC-USDT) 1m candles for s_lead_micro / s_lead_regime (Phase 5).
          - Quote rate (USDT-BRL) 1m candles for the synthetic fair_brl (Phase 5).

        De-duplicate on (connector, trading_pair, interval) so a config that
        re-uses the main pair for candles doesn't subscribe twice.
        """
        seen = set()
        out: List[CandlesConfig] = []

        def _add(connector: Optional[str], pair: Optional[str]):
            if not connector or not pair:
                return
            key = (connector, pair, self.config.candles_interval)
            if key in seen:
                return
            seen.add(key)
            out.append(CandlesConfig(
                connector=connector,
                trading_pair=pair,
                interval=self.config.candles_interval,
                max_records=self.max_records,
            ))

        _add(self.config.candles_connector, self.config.candles_trading_pair)
        _add(self.config.leader_connector, self.config.leader_trading_pair)
        _add(self.config.quote_rate_connector, self.config.quote_rate_trading_pair)
        return out

    def _compute_vol_from_candles(self):
        """
        Read close prices from candles_df and return (sigma_short, sigma_ref).
        Both are 0.0 when not enough history → spread_multiplier defaults to 1.0.
        """
        try:
            df = self.market_data_provider.get_candles_df(
                connector_name=self.config.candles_connector,
                trading_pair=self.config.candles_trading_pair,
                interval=self.config.candles_interval,
                max_records=self.max_records,
            )
        except Exception:
            return 0.0, 0.0
        if df is None or len(df) < self.config.vol_short_length + 1:
            return 0.0, 0.0
        closes = df["close"].astype(float).tolist()
        sigma_short = compute_volatility_from_prices(closes[-self.config.vol_short_length:])
        sigma_ref = compute_volatility_from_prices(closes[-self.config.vol_ref_length:])
        return sigma_short, sigma_ref

    # ── Balance fetching (Phase 2) ───────────────────────────────────────

    def _split_pair(self) -> Tuple[str, str]:
        """Split BTC-BRL → ('BTC', 'BRL'). Falls back to (BASE, QUOTE) on parse error."""
        try:
            base, quote = self.config.trading_pair.split("-")
            return base, quote
        except ValueError:
            return "BASE", "QUOTE"

    def _get_balances(self) -> Tuple[Decimal, Decimal]:
        """
        Fetch (base, quote) total balances from the connector.
        Returns (Decimal('0'), Decimal('0')) when connector isn't ready/available
        (e.g., during warmup or in unit tests with mocked providers).
        """
        connectors = getattr(self.market_data_provider, "connectors", None)
        if not isinstance(connectors, dict):
            return Decimal("0"), Decimal("0")
        connector = connectors.get(self.config.connector_name)
        if connector is None:
            return Decimal("0"), Decimal("0")
        base_asset, quote_asset = self._split_pair()
        try:
            base = Decimal(str(connector.get_balance(base_asset)))
            quote = Decimal(str(connector.get_balance(quote_asset)))
        except Exception:
            return Decimal("0"), Decimal("0")
        return base, quote

    # ── Lead-lag helpers (Phase 5) ───────────────────────────────────────

    def _read_latest_close(self, connector: Optional[str], pair: Optional[str]) -> Optional[float]:
        """
        Return the most recent close price from candles for (connector, pair).
        None if the feed is unavailable or empty.
        """
        if not connector or not pair:
            return None
        try:
            df = self.market_data_provider.get_candles_df(
                connector_name=connector,
                trading_pair=pair,
                interval=self.config.candles_interval,
                max_records=2,
            )
        except Exception:
            return None
        if df is None or len(df) == 0:
            return None
        try:
            return float(df["close"].iloc[-1])
        except (KeyError, IndexError, ValueError, TypeError):
            return None

    def _read_leg_prices(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Return (mid_brl, mid_usdt, usdt_brl) sourced from latest candle close.
        Any leg can be None when its feed is unavailable.
        """
        try:
            mid_brl_raw = self.market_data_provider.get_price_by_type(
                self.config.connector_name,
                self.config.trading_pair,
                PriceType.MidPrice,
            )
            mid_brl = float(mid_brl_raw) if mid_brl_raw is not None else None
        except Exception:
            mid_brl = None
        if mid_brl is not None and mid_brl <= 0:
            mid_brl = None
        mid_usdt = self._read_latest_close(
            self.config.leader_connector, self.config.leader_trading_pair,
        )
        usdt_brl = self._read_latest_close(
            self.config.quote_rate_connector, self.config.quote_rate_trading_pair,
        )
        return mid_brl, mid_usdt, usdt_brl

    def _record_lead_history(self, now: float, mid_brl: Optional[float],
                             mid_usdt: Optional[float], usdt_brl: Optional[float]) -> None:
        """
        Append (timestamp, mid_brl, mid_usdt, usdt_brl) to history and update
        last-seen timestamps. Drops samples with non-positive prices for any leg.
        Trims history to lead_history_max_seconds.
        """
        if mid_brl is None or mid_brl <= 0:
            return
        if mid_usdt is None or mid_usdt <= 0:
            return
        if usdt_brl is None or usdt_brl <= 0:
            return
        self._lead_history.append((now, mid_brl, mid_usdt, usdt_brl))
        self._last_mid_brl_ts = now
        self._last_mid_usdt_ts = now
        self._last_usdt_brl_ts = now
        cutoff = now - self._lead_history_max_seconds
        while self._lead_history and self._lead_history[0][0] < cutoff:
            self._lead_history.popleft()

    def _lookup_past(self, target_ts: float) -> Optional[Tuple[float, float, float, float]]:
        """
        Return the closest history entry whose timestamp is <= target_ts.
        Used as the t-Δs reference for lag computations. None when history
        doesn't span far enough back.
        """
        if not self._lead_history:
            return None
        if self._lead_history[0][0] > target_ts:
            return None
        # Walk from oldest, keep last <= target.
        chosen = self._lead_history[0]
        for entry in self._lead_history:
            if entry[0] > target_ts:
                break
            chosen = entry
        return chosen

    def _compute_lead_signals(self, mid_dec: Decimal):
        """
        Phase 5 — assemble micro + regime lead-lag signals.

        Steps:
          1. Read latest mid_brl / mid_usdt / usdt_brl from feeds.
          2. Append to history; update last-seen ts.
          3. Update fair_brl_smooth via EWM step.
          4. Look up past values at t-micro_window and t-short_window.
          5. Compute staleness flags from last-seen ts.
          6. Update sigma_lag from rolling lag samples.
          7. Call compute_lead_state(...) with the assembled inputs.
        """
        from controllers.market_making.pmm_lead_lag_utils import (
            LeadState,
            compute_lead_state,
            ewm_step,
        )

        try:
            now = float(self.market_data_provider.time())
        except (TypeError, ValueError):
            now = 0.0

        mid_brl, mid_usdt, usdt_brl = self._read_leg_prices()
        # If mid_dec is provided and feed read failed, prefer mid_dec for BRL.
        if mid_brl is None and mid_dec is not None and mid_dec > 0:
            try:
                mid_brl = float(mid_dec)
            except (TypeError, ValueError):
                mid_brl = None

        # Staleness — based on most recent successful read (any leg).
        micro_stale = (
            mid_brl is None or mid_usdt is None
            or (now - max(self._last_mid_brl_ts, 0.0)) > self.config.max_leader_staleness_sec
            or (now - max(self._last_mid_usdt_ts, 0.0)) > self.config.max_leader_staleness_sec
        )
        regime_stale = (
            micro_stale or usdt_brl is None
            or (now - max(self._last_usdt_brl_ts, 0.0)) > self.config.max_usdt_brl_staleness_sec
        )

        self._record_lead_history(now, mid_brl, mid_usdt, usdt_brl)

        # If we have no usable history yet, return neutral state with stale flags.
        if mid_brl is None or mid_usdt is None or usdt_brl is None:
            return LeadState(
                s_lead_micro=0.0, s_lead_regime=0.0, basis_bps=0.0,
                micro_stale=True, regime_stale=True,
                pause_buy=False, pause_sell=False,
            )

        # EWM update for fair_brl_smooth.
        fair_brl_raw = mid_usdt * usdt_brl
        dt = now - self._fair_brl_smooth_ts if self._fair_brl_smooth_ts > 0 else 0.0
        self._fair_brl_smooth = ewm_step(
            prev=self._fair_brl_smooth,
            new_value=fair_brl_raw,
            halflife_sec=self.config.lead_lag_ewm_halflife_sec,
            dt_sec=dt,
        )
        self._fair_brl_smooth_ts = now

        # Past references.
        micro_past = self._lookup_past(now - self.config.lead_micro_window_sec)
        regime_past = self._lookup_past(now - self.config.lead_lag_short_window_sec)

        # Past mid_brl / mid_usdt for micro.
        if micro_past is not None and (now - micro_past[0]) >= self.config.lead_micro_window_sec * 0.5:
            mid_brl_past = micro_past[1]
            mid_usdt_past = micro_past[2]
        else:
            mid_brl_past = mid_brl
            mid_usdt_past = mid_usdt
            micro_stale = True  # not enough history → micro disabled

        # Past fair smooth approximation for regime: use the smoothed value
        # at past tick. Since we don't store the historic smooth, fall back to
        # raw fair (mid_usdt_past × usdt_brl_past) as an unbiased estimator —
        # the regime horizon is long enough that EWM mostly approximates raw.
        if regime_past is not None and (now - regime_past[0]) >= self.config.lead_lag_short_window_sec * 0.5:
            mid_brl_regime_past = regime_past[1]
            fair_brl_smooth_past = regime_past[2] * regime_past[3]
        else:
            mid_brl_regime_past = mid_brl
            fair_brl_smooth_past = self._fair_brl_smooth
            regime_stale = True  # not enough history → regime disabled

        # Adaptive sigma_lag from samples in z_window.
        z_cutoff = now - self.config.lead_lag_z_window_sec
        while self._lag_samples and self._lag_samples[0][0] < z_cutoff:
            self._lag_samples.popleft()
        # Use a small default sigma during warmup so z_lead doesn't blow up.
        if len(self._lag_samples) >= 5:
            mean = sum(s for _, s in self._lag_samples) / len(self._lag_samples)
            var = sum((s - mean) ** 2 for _, s in self._lag_samples) / max(len(self._lag_samples) - 1, 1)
            sigma_lag = max(var ** 0.5, 1e-6)
        else:
            sigma_lag = 1e-3

        # Record this tick's raw lag_short for next sigma update.
        # (Compute it inline, not redundantly via compute_lag_regime.)
        if mid_brl_regime_past > 0 and self._fair_brl_smooth > 0 and fair_brl_smooth_past > 0:
            import math as _m
            try:
                this_lag = _m.log(self._fair_brl_smooth / fair_brl_smooth_past) - _m.log(mid_brl / mid_brl_regime_past)
                self._lag_samples.append((now, this_lag))
            except (ValueError, ZeroDivisionError):
                pass

        return compute_lead_state(
            mid_brl_now=mid_brl,
            mid_brl_past=mid_brl_past,
            mid_usdt_now=mid_usdt,
            mid_usdt_past=mid_usdt_past,
            usdt_brl_ref=usdt_brl,
            fair_brl_smooth_now=self._fair_brl_smooth,
            fair_brl_smooth_past=fair_brl_smooth_past,
            sigma_lag=sigma_lag,
            micro_threshold_bps=self.config.lead_micro_threshold_bps,
            basis_deadband_bps=self.config.basis_deadband_bps,
            micro_stale=micro_stale,
            regime_stale=regime_stale,
        )

    # ── Regime helpers (Phase 4) ─────────────────────────────────────────

    def _update_session_pnl(self) -> None:
        """
        Accumulate net_pnl_quote from completed executors. Tracks executor IDs
        in _seen_executor_ids to avoid double-counting across update_processed_data
        ticks.
        """
        for executor in self.executors_info:
            ex_id = getattr(executor, "id", None)
            if ex_id is None or ex_id in self._seen_executor_ids:
                continue
            if not getattr(executor, "is_done", False):
                continue
            try:
                pnl = float(getattr(executor, "net_pnl_quote", 0.0) or 0.0)
            except (TypeError, ValueError):
                pnl = 0.0
            self._session_pnl += pnl
            self._seen_executor_ids.add(ex_id)

    def _record_critical_error(self, source: str = "") -> None:
        """
        Increment the critical-error counter consumed by L3 in compute_regime_state.
        Plan §4.1 L3 item 4: order rejections, websocket loss, time desync.
        """
        self._regime_ctx.critical_error_count += 1
        try:
            self.logger().warning(
                f"[PMM Lead-Lag] critical error recorded ({source}); "
                f"count={self._regime_ctx.critical_error_count}/"
                f"{self.config.critical_error_threshold}"
            )
        except Exception:
            pass

    def _feed_stale_for_regime_pause(self, now: float) -> bool:
        """
        Plan §4.1 L2 item 4: BTC-USDT leader feed older than max_leader_staleness_sec
        triggers L2 pause. Only fires AFTER warmup (we must have seen at least one
        BTC-USDT tick); a never-populated feed is treated as warmup, not failure.
        """
        last_ts = self._last_mid_usdt_ts
        if last_ts <= 0:
            return False  # warmup — no tick yet, not "stale"
        return (now - last_ts) > self.config.max_leader_staleness_sec

    def _track_sustained_feed_stale(self, now: float) -> None:
        """
        Auto-trigger for the L3 critical-error counter: when the leader feed has
        been stale for > 6× max_leader_staleness_sec continuously, record one
        critical error per episode (plan §4.1 L3 item 4 — "websocket loss > 30s"
        analog). Resets when feed recovers.
        """
        if self._feed_stale_for_regime_pause(now):
            if self._feed_stale_started_at is None:
                self._feed_stale_started_at = now
                self._feed_stale_critical_recorded = False
            elif (not self._feed_stale_critical_recorded and
                  (now - self._feed_stale_started_at)
                  > 6.0 * self.config.max_leader_staleness_sec):
                self._record_critical_error("feed_stale_sustained")
                self._feed_stale_critical_recorded = True
        else:
            self._feed_stale_started_at = None
            self._feed_stale_critical_recorded = False

    def _evaluate_regime(self, vol_state, inv_state, basis_bps: float,
                         regime_stale: bool, now: float) -> str:
        """
        Thin wrapper that delegates to compute_regime_state (plan §1.7 modularization).

        All decision logic lives in the pure function; this wrapper:
          - bundles config thresholds into a RegimeThresholds dataclass
          - feeds the controller's RegimeContext (carry-state mutated in place)
          - logs CRITICAL once when the kill latch fires
          - mirrors regime.cause to self._regime_cause for telemetry consumers
        """
        c = self.config
        th = RegimeThresholds(
            vol_pause_threshold_mult=c.vol_pause_threshold_mult,
            vol_degraded_threshold_mult=c.vol_degraded_threshold_mult,
            pause_basis_bps=c.pause_basis_bps,
            pause_release_sec=c.pause_release_sec,
            safe_mode_entry_sec=c.safe_mode_entry_sec,
            safe_thrash_window_sec=c.safe_thrash_window_sec,
            max_session_drawdown_quote=float(c.max_session_drawdown_quote),
            critical_error_threshold=int(c.critical_error_threshold),
        )
        was_killed = self._regime_ctx.is_killed
        result = compute_regime_state(
            vol_state=vol_state,
            inv_state=inv_state,
            basis_bps=basis_bps,
            regime_stale=regime_stale,
            over_max_net_position=self.processed_data.get("over_max_net_position", False),
            session_pnl=self._session_pnl,
            now=now,
            ctx=self._regime_ctx,
            th=th,
        )
        self._regime_cause = result.cause
        if not was_killed and self._regime_ctx.is_killed:
            try:
                self.logger().critical(
                    f"[PMM Lead-Lag] Kill switch triggered: {result.cause}"
                )
            except Exception:
                pass
        return result.regime

    # ── Data update ───────────────────────────────────────────────────────

    async def update_processed_data(self):
        """
        Compute all signals and update processed_data.

        Phase 2: live balances drive inv_pct, size_factors, sides_enabled.
        Phase 3+: skew_max_bps > 0 will start shifting reference_price.
        Phase 4+: regime_state stub will be replaced by real circuit breakers.
        """
        mid_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice
        )
        mid_dec = Decimal(str(mid_price))

        # Phase 3 — volatility from candles
        sigma_short, sigma_ref = self._compute_vol_from_candles()
        if sigma_short > 0 and sigma_ref > 0:
            vol_state = compute_vol_state(sigma_short, sigma_ref)
        else:
            vol_state = compute_vol_state(1.0, 1.0)  # neutral until enough history

        # Phase 5 lead-lag — real signals from price history + EWM.
        lead_state = self._compute_lead_signals(mid_dec)
        # Micro pause has its own dwell — when fired, latch until dwell expires
        # so a single tick over the threshold buys lead_micro_dwell_sec of pause.
        try:
            now_lead = float(self.market_data_provider.time())
        except (TypeError, ValueError):
            now_lead = 0.0
        if lead_state.pause_buy:
            self._pause_buy_until = max(
                self._pause_buy_until,
                now_lead + self.config.lead_micro_dwell_sec,
            )
        if lead_state.pause_sell:
            self._pause_sell_until = max(
                self._pause_sell_until,
                now_lead + self.config.lead_micro_dwell_sec,
            )

        # Phase 2 — live balances
        base_bal, quote_bal = self._get_balances()
        inv_state = compute_inventory_state(
            base_balance=base_bal,
            quote_balance=quote_bal,
            mid_price=mid_dec,
            target_pct=self.config.target_inventory_base_pct,
            soft_band=self.config.inv_soft_band,
            hard_band=self.config.inv_hard_band,
            inv_kill=self.config.inv_kill,
        )

        # §2.5 — net exposure in quote (base value at mid). Phase 4 uses this
        # flag to trigger the kill switch inside _evaluate_regime.
        net_exposure_quote = base_bal * mid_dec
        over_max_net_position = net_exposure_quote > self.config.max_net_position_quote
        # Expose to _evaluate_regime via processed_data BEFORE we evaluate.
        self.processed_data["over_max_net_position"] = over_max_net_position
        self.processed_data["net_exposure_quote"] = net_exposure_quote

        # Phase 4 — regime state machine (§4.3). Uses inv_state.in_kill,
        # over_max_net_position and session pnl as kill triggers.
        self._update_session_pnl()
        try:
            now = float(self.market_data_provider.time())
        except (TypeError, ValueError):
            now = 0.0
        # Track sustained feed staleness as critical-error proxy (§4.1 L3 item 4).
        self._track_sustained_feed_stale(now)
        regime = self._evaluate_regime(
            vol_state, inv_state, lead_state.basis_bps,
            regime_stale=self._feed_stale_for_regime_pause(now), now=now,
        )
        _regime_mult_map = {
            "normal": 1.0, "degraded": 1.5, "safe": 2.5,
            "paused": 1.0, "killed": 1.0,
        }
        regime_state = RegimeState(
            regime=regime,
            spread_multiplier=_regime_mult_map.get(regime, 1.0),
        )

        # Phase 4 — zero skew in elevated regimes (safe/paused/killed) so the
        # bot doesn't keep biasing prices while protective measures are active.
        if regime in ("safe", "paused", "killed"):
            skew_state = SkewState(
                skew_raw=0.0,
                skew_norm=0.0,
                price_shift_bps=Decimal("0"),
            )
        else:
            skew_state = compute_skew_state(
                s_inv=inv_state.s_inv,
                s_lead_regime=lead_state.s_lead_regime,
                w_inv=self.config.w_inv,
                w_lead=self.config.w_lead,
                skew_max_bps=self.config.skew_max_bps,
            )

        # Side permissions: trigger at inv_hard_cap, release at inv_hard_band (§2.4 hysteresis)
        side_perms = compute_side_permissions(
            delta=inv_state.delta,
            hard_cap=self.config.inv_hard_cap,
            hard_band=self.config.inv_hard_band,
            current_buy_enabled=self._buy_enabled,
            current_sell_enabled=self._sell_enabled,
        )
        self._buy_enabled = side_perms.buy_enabled
        self._sell_enabled = side_perms.sell_enabled

        order_params = compute_order_params(
            mid_price=mid_dec,
            skew_state=skew_state,
            regime_state=regime_state,
            vol_state=vol_state,
            side_perms=side_perms,
        )

        size_factor_buy, size_factor_sell = compute_size_factors(
            delta=inv_state.delta,
            soft_band=self.config.inv_soft_band,
            hard_band=self.config.inv_hard_band,
        )

        self.processed_data = {
            "reference_price": order_params.reference_price,
            "spread_multiplier": order_params.spread_multiplier,
            "price_shift_bps": order_params.price_shift_bps,
            "mid_price": mid_dec,
            "base_balance": base_bal,
            "quote_balance": quote_bal,
            "net_exposure_quote": net_exposure_quote,
            "over_max_net_position": over_max_net_position,
            "inv_pct": inv_state.inv_pct,
            "inv_delta": inv_state.delta,
            "s_inv": inv_state.s_inv,
            "s_lead_regime": lead_state.s_lead_regime,
            "s_lead_micro": lead_state.s_lead_micro,
            "basis_bps": lead_state.basis_bps,
            "micro_stale": lead_state.micro_stale,
            "regime_stale": lead_state.regime_stale,
            "pause_buy": lead_state.pause_buy or now_lead < self._pause_buy_until,
            "pause_sell": lead_state.pause_sell or now_lead < self._pause_sell_until,
            "vol_ratio": vol_state.vol_ratio,
            "regime": regime_state.regime,
            "regime_cause": self._regime_cause,
            "session_pnl": self._session_pnl,
            "sides_enabled": {
                "buy": order_params.buy_enabled,
                "sell": order_params.sell_enabled,
            },
            "size_factors": {
                "buy": size_factor_buy,
                "sell": size_factor_sell,
            },
        }

        if self.config.csv_log_enabled:
            self._csv_log_signals()

    # ── Level selection ───────────────────────────────────────────────────

    def get_levels_to_execute(self) -> List[str]:
        """
        Filter levels by:
          - regime (Phase 4): paused/killed → []; safe → 1 level per side.
          - sides_enabled (Phase 2): inventory-driven side shutdown.
          - lead-lag micro pause (Phase 5, §3.6): suppress side until dwell ends.

        Note: micro pause does NOT cancel active orders here — only suppresses
        creation of new ones. Cancel-on-lag-≥-spread lives in
        executors_to_early_stop (B8).
        """
        regime = self.processed_data.get("regime", "normal")
        if regime in ("paused", "killed"):
            return []
        levels = super().get_levels_to_execute()
        sides = self.processed_data.get("sides_enabled", {"buy": True, "sell": True})
        levels = [
            lv for lv in levels
            if (lv.startswith("buy") and sides.get("buy", True)) or
               (lv.startswith("sell") and sides.get("sell", True))
        ]
        # Micro pause filter — only when dwell hasn't expired.
        try:
            now = float(self.market_data_provider.time())
        except (TypeError, ValueError):
            now = 0.0
        pause_buy = now < self._pause_buy_until
        pause_sell = now < self._pause_sell_until
        if pause_buy or pause_sell:
            levels = [
                lv for lv in levels
                if not (lv.startswith("buy") and pause_buy) and
                   not (lv.startswith("sell") and pause_sell)
            ]
        if regime == "safe":
            buy_levels = [lv for lv in levels if lv.startswith("buy")]
            sell_levels = [lv for lv in levels if lv.startswith("sell")]
            return buy_levels[:1] + sell_levels[:1]
        return levels

    # ── Executor config ───────────────────────────────────────────────────

    def get_executor_config(self, level_id: str, price: Decimal, amount: Decimal):
        """
        Create PositionExecutorConfig for a given level, applying inventory and
        regime size factors plus §5.6 distance clamp (orders must not cross mid).

        Returns None when size_factor=0 (adversa side beyond hard_band) or when
        the regime is paused/killed.
        """
        regime = self.processed_data.get("regime", "normal")
        if regime in ("paused", "killed"):
            return None

        trade_type = self.get_trade_type_from_level_id(level_id)
        size_factors: Dict[str, float] = self.processed_data.get(
            "size_factors", {"buy": 1.0, "sell": 1.0},
        )
        size_factor = Decimal(str(size_factors.get(trade_type.name.lower(), 1.0)))
        # Regime-driven size shrink (§4 table): degraded → 0.5×, safe → 0.25×.
        regime_sf = {
            "degraded": Decimal("0.5"),
            "safe": Decimal("0.25"),
        }.get(regime, Decimal("1"))
        size_factor = size_factor * regime_sf
        if size_factor <= Decimal("0"):
            return None

        clamped_price = self._clamp_to_mid(price, trade_type)
        final_amount = amount * size_factor
        self._csv_log_order(
            level_id=level_id,
            side=trade_type.name.lower(),
            price=clamped_price,
            amount=final_amount,
        )
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            level_id=level_id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_price=clamped_price,
            amount=final_amount,
            triple_barrier_config=self.config.triple_barrier_config,
            leverage=self.config.leverage,
            side=trade_type,
        )

    def _clamp_to_mid(self, order_price: Decimal, trade_type: TradeType) -> Decimal:
        """
        §5.6 — never let bid >= mid or ask <= mid (would auto-cross to taker).
        Pulls back to mid * (1 ± min_maker_distance_bps / 10000).
        """
        mid = self.processed_data.get("mid_price")
        if mid is None or mid <= 0:
            return order_price
        margin = mid * Decimal(str(self.config.min_maker_distance_bps)) / Decimal("10000")
        if trade_type == TradeType.BUY and order_price >= mid:
            return mid - margin
        if trade_type == TradeType.SELL and order_price <= mid:
            return mid + margin
        return order_price

    # ── Quote churn brakes (§5.7, Phase 3) ───────────────────────────────

    def _delta_bps_for_executor(self, executor) -> Optional[Decimal]:
        """Compute |new_price − executor.entry_price| / entry_price in bps."""
        custom_info = getattr(executor, "custom_info", None) or {}
        level_id = custom_info.get("level_id")
        if not level_id or level_id == "position_rebalance":
            return None
        try:
            new_price, _ = self.get_price_and_amount(level_id)
        except Exception:
            return None
        current = getattr(getattr(executor, "config", None), "entry_price", None)
        if current is None or current <= 0:
            return None
        try:
            return abs(Decimal(str(new_price)) - Decimal(str(current))) / Decimal(str(current)) * Decimal("10000")
        except Exception:
            return None

    def executors_to_refresh(self) -> List[ExecutorAction]:
        """
        Add the min_requote_bps brake on top of the base time-based refresh: an
        aged executor whose new price differs by < min_requote_bps stays in place
        (§5.7) — avoids churn when the signal barely moved.
        """
        base_actions = super().executors_to_refresh()
        if self.config.min_requote_bps <= 0:
            return base_actions
        threshold = Decimal(str(self.config.min_requote_bps))
        executor_by_id = {e.id: e for e in self.executors_info}
        keep: List[ExecutorAction] = []
        for action in base_actions:
            executor = executor_by_id.get(getattr(action, "executor_id", None))
            if executor is None:
                keep.append(action)
                continue
            delta_bps = self._delta_bps_for_executor(executor)
            if delta_bps is None or delta_bps >= threshold:
                keep.append(action)
        return keep

    def executors_to_early_stop(self) -> List[ExecutorAction]:
        """
        Combine four early-stop drivers:
          1. force_requote_bps (§5.7) — stale-order protection in fast markets.
          2. paused / killed regime (§4) — cancel all active resting orders.
          3. safe regime (§4) — keep only buy_0 / sell_0 active; cancel deeper.
          4. micro lag ≥ spread (§3.6 critério 1, Phase 5) — when the leader
             moved by more than the resting order's own spread, the order is
             effectively at fair value and will be sniped → cancel it.

        Active trading positions (is_trading=True) are never cancelled here —
        triple-barrier handles them.
        """
        base_actions = list(super().executors_to_early_stop())
        threshold = Decimal(str(self.config.force_requote_bps))
        regime = self.processed_data.get("regime", "normal")
        stopped_ids = {getattr(a, "executor_id", None) for a in base_actions}
        extra: List[ExecutorAction] = []

        # Phase 5 — only the absolute magnitude of the micro lag matters here;
        # direction (pause_buy vs pause_sell) is handled by get_levels_to_execute.
        s_lead_micro_abs = abs(float(self.processed_data.get("s_lead_micro", 0.0) or 0.0))

        for executor in self.executors_info:
            if not executor.is_active or executor.is_trading:
                continue
            if executor.id in stopped_ids:
                continue

            # Paused / killed: cancel any resting order.
            if regime in ("paused", "killed"):
                extra.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=executor.id,
                ))
                stopped_ids.add(executor.id)
                continue

            # Safe mode: keep only buy_0 / sell_0; cancel the rest.
            if regime == "safe":
                custom_info = getattr(executor, "custom_info", None) or {}
                level_id = custom_info.get("level_id", "")
                if level_id and level_id not in ("buy_0", "sell_0", "position_rebalance"):
                    extra.append(StopExecutorAction(
                        controller_id=self.config.id,
                        executor_id=executor.id,
                    ))
                    stopped_ids.add(executor.id)
                    continue

            # §3.6 critério 1 — micro lag ≥ side spread → cancel that side's order.
            # We use the configured per-level spread (closest spread for the side)
            # as the comparison. side spread is a fraction; convert to bps × 1e4.
            if s_lead_micro_abs > 0:
                custom_info = getattr(executor, "custom_info", None) or {}
                level_id = custom_info.get("level_id", "")
                side_spreads: List[float] = (
                    self.config.buy_spreads if level_id.startswith("buy")
                    else (self.config.sell_spreads if level_id.startswith("sell") else [])
                )
                if side_spreads:
                    side_spread_bps = float(min(side_spreads)) * 1e4  # tightest level
                    pause_for_side = (
                        (level_id.startswith("buy") and self.processed_data.get("pause_buy"))
                        or (level_id.startswith("sell") and self.processed_data.get("pause_sell"))
                    )
                    if pause_for_side and s_lead_micro_abs >= side_spread_bps:
                        extra.append(StopExecutorAction(
                            controller_id=self.config.id,
                            executor_id=executor.id,
                        ))
                        stopped_ids.add(executor.id)
                        continue

            # Force-requote when price drifted beyond force_requote_bps.
            delta_bps = self._delta_bps_for_executor(executor)
            if delta_bps is not None and delta_bps >= threshold:
                extra.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=executor.id,
                ))
                stopped_ids.add(executor.id)

        return base_actions + extra

    # ── CSV logging (Phase 2) ─────────────────────────────────────────────

    _CSV_COLUMNS = (
        "ts", "regime", "regime_cause", "mid", "ref", "shift_bps", "spread_mult",
        "base_bal", "quote_bal", "net_exposure_quote", "over_max_net_position",
        "inv_pct", "inv_delta", "s_inv",
        "s_lead_micro", "s_lead_regime", "basis_bps",
        "micro_stale", "regime_stale", "pause_buy", "pause_sell",
        "vol_ratio",
        "buy_enabled", "sell_enabled",
        "sf_buy", "sf_sell",
        "session_pnl",
    )

    _ORDERS_CSV_COLUMNS = (
        "ts", "level_id", "side", "price", "amount",
        "distance_from_mid_bps", "shift_bps", "regime",
    )

    def _csv_log_order(
        self,
        level_id: str,
        side: str,
        price: Decimal,
        amount: Decimal,
    ) -> None:
        """
        Plan §1.4 — append one row per executor created to orders.csv.
        Captures distance from mid, applied skew, and regime for post-session
        analysis of fill quality vs adverse selection.
        Silently no-ops if csv_log is disabled or the directory cannot be created.
        """
        if not self.config.csv_log_enabled:
            return
        try:
            os.makedirs(self.config.csv_log_dir, exist_ok=True)
        except OSError:
            return
        d = self.processed_data
        try:
            mid = d.get("mid_price")
            if mid is not None and mid > 0:
                dist_bps = (price - mid) / mid * Decimal("10000")
            else:
                dist_bps = Decimal("0")
        except (TypeError, ZeroDivisionError):
            dist_bps = Decimal("0")
        row = {
            "ts": self.market_data_provider.time(),
            "level_id": level_id,
            "side": side,
            "price": str(price),
            "amount": str(amount),
            "distance_from_mid_bps": str(dist_bps),
            "shift_bps": str(d.get("price_shift_bps", "")),
            "regime": d.get("regime", "normal"),
        }
        write_header = (not self._orders_csv_initialized
                        and not os.path.exists(self._orders_csv_path))
        try:
            with open(self._orders_csv_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self._ORDERS_CSV_COLUMNS)
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
            self._orders_csv_initialized = True
        except OSError:
            return

    def _csv_log_signals(self) -> None:
        """Append one row per update_processed_data() call to signals.csv."""
        try:
            os.makedirs(self.config.csv_log_dir, exist_ok=True)
        except OSError:
            return
        d = self.processed_data
        row = {
            "ts": self.market_data_provider.time(),
            "regime": d.get("regime", "normal"),
            "regime_cause": d.get("regime_cause", ""),
            "mid": str(d.get("mid_price", "")),
            "ref": str(d.get("reference_price", "")),
            "shift_bps": str(d.get("price_shift_bps", "")),
            "spread_mult": str(d.get("spread_multiplier", "")),
            "base_bal": str(d.get("base_balance", "")),
            "quote_bal": str(d.get("quote_balance", "")),
            "net_exposure_quote": str(d.get("net_exposure_quote", "")),
            "over_max_net_position": d.get("over_max_net_position", False),
            "inv_pct": d.get("inv_pct", ""),
            "inv_delta": d.get("inv_delta", ""),
            "s_inv": d.get("s_inv", ""),
            "s_lead_micro": d.get("s_lead_micro", ""),
            "s_lead_regime": d.get("s_lead_regime", ""),
            "basis_bps": d.get("basis_bps", ""),
            "micro_stale": d.get("micro_stale", False),
            "regime_stale": d.get("regime_stale", False),
            "pause_buy": d.get("pause_buy", False),
            "pause_sell": d.get("pause_sell", False),
            "vol_ratio": d.get("vol_ratio", ""),
            "buy_enabled": d.get("sides_enabled", {}).get("buy", True),
            "sell_enabled": d.get("sides_enabled", {}).get("sell", True),
            "sf_buy": d.get("size_factors", {}).get("buy", ""),
            "sf_sell": d.get("size_factors", {}).get("sell", ""),
            "session_pnl": d.get("session_pnl", 0.0),
        }
        write_header = not self._csv_initialized and not os.path.exists(self._csv_path)
        try:
            with open(self._csv_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self._CSV_COLUMNS)
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
            self._csv_initialized = True
        except OSError:
            return

    # ── Status ────────────────────────────────────────────────────────────

    def to_format_status(self) -> List[str]:
        """Return human-readable status lines for the Hummingbot status command."""
        d = self.processed_data
        regime = d.get("regime", "N/A")
        regime_cause = d.get("regime_cause", "")
        inv_pct = d.get("inv_pct", 0.5)
        sides = d.get("sides_enabled", {"buy": True, "sell": True})
        ref_price = d.get("reference_price", "N/A")
        spread_mult = d.get("spread_multiplier", "N/A")
        shift_bps = d.get("price_shift_bps", Decimal("0"))
        vol_ratio = d.get("vol_ratio", 1.0)
        sf = d.get("size_factors", {"buy": 1.0, "sell": 1.0})
        net_exp = d.get("net_exposure_quote", Decimal("0"))
        over_cap = d.get("over_max_net_position", False)
        cap_flag = " ! OVER-CAP" if over_cap else ""
        session_pnl = d.get("session_pnl", 0.0)
        cause_str = f" ({regime_cause})" if regime_cause else ""
        # Phase 5 lead-lag telemetry
        s_lead_micro = float(d.get("s_lead_micro", 0.0) or 0.0)
        s_lead_regime = float(d.get("s_lead_regime", 0.0) or 0.0)
        basis_bps = float(d.get("basis_bps", 0.0) or 0.0)
        pause_buy = d.get("pause_buy", False)
        pause_sell = d.get("pause_sell", False)
        micro_stale = d.get("micro_stale", False)
        regime_stale = d.get("regime_stale", False)
        stale_flags = []
        if micro_stale:
            stale_flags.append("micro")
        if regime_stale:
            stale_flags.append("regime")
        stale_str = f"  Stale: {','.join(stale_flags)}" if stale_flags else ""
        pause_flags = []
        if pause_buy:
            pause_flags.append("BUY")
        if pause_sell:
            pause_flags.append("SELL")
        pause_str = f"  Pause: {'+'.join(pause_flags)}" if pause_flags else ""
        return [
            f"── PMM Lead-Lag Skew ({self.config.trading_pair}) ──────────────────",
            f"  Regime: {regime:<10}{cause_str}  Vol ratio: {vol_ratio:.2f}x  Spread mult: {spread_mult}",
            f"  Ref price: {ref_price}  Shift: {shift_bps} bps",
            f"  Inventory: {inv_pct:.1%}  Buy: {'ON ' if sides.get('buy') else 'OFF'}  Sell: {'ON' if sides.get('sell') else 'OFF'}",
            f"  Size factors  buy: {sf.get('buy'):.2f}  sell: {sf.get('sell'):.2f}",
            f"  Net exposure (quote): {net_exp} / {self.config.max_net_position_quote}{cap_flag}",
            f"  Lead-lag  micro: {s_lead_micro:+.2f} bps  regime: {s_lead_regime:+.3f}  basis: {basis_bps:+.2f} bps{stale_str}{pause_str}",
            f"  Session PnL (quote): {session_pnl:.4f}  Killed: {self._regime_ctx.is_killed}",
        ]

    def get_custom_info(self) -> dict:
        """Publish key signals via MQTT for monitoring."""
        d = self.processed_data
        sf = d.get("size_factors", {"buy": 1.0, "sell": 1.0})
        return {
            "regime": d.get("regime", "normal"),
            "regime_cause": d.get("regime_cause", ""),
            "is_killed": self._regime_ctx.is_killed,
            "session_pnl": float(d.get("session_pnl", 0.0)),
            "inv_pct": round(d.get("inv_pct", 0.5), 4),
            "inv_delta": round(d.get("inv_delta", 0.0), 4),
            "s_inv": round(d.get("s_inv", 0.0), 4),
            "price_shift_bps": float(d.get("price_shift_bps", 0)),
            "vol_ratio": round(d.get("vol_ratio", 1.0), 3),
            "net_exposure_quote": float(d.get("net_exposure_quote", 0)),
            "over_max_net_position": bool(d.get("over_max_net_position", False)),
            "buy_enabled": d.get("sides_enabled", {}).get("buy", True),
            "sell_enabled": d.get("sides_enabled", {}).get("sell", True),
            "sf_buy": round(sf.get("buy", 1.0), 3),
            "sf_sell": round(sf.get("sell", 1.0), 3),
            # Phase 5 lead-lag fields
            "s_lead_micro": round(float(d.get("s_lead_micro", 0.0) or 0.0), 3),
            "s_lead_regime": round(float(d.get("s_lead_regime", 0.0) or 0.0), 3),
            "basis_bps": round(float(d.get("basis_bps", 0.0) or 0.0), 3),
            "pause_buy": bool(d.get("pause_buy", False)),
            "pause_sell": bool(d.get("pause_sell", False)),
            "micro_stale": bool(d.get("micro_stale", False)),
            "regime_stale": bool(d.get("regime_stale", False)),
        }
