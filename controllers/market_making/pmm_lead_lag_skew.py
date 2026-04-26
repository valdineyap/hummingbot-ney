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
from typing import Dict, List, Optional, Tuple

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
        json_schema_extra={"prompt": "Candles connector (blank = same as connector_name): "},
    )
    candles_trading_pair: Optional[str] = Field(
        default=None,
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
        # CSV signals logger (lazy init: opened on first write)
        self._csv_path: str = os.path.join(self.config.csv_log_dir, "signals.csv")
        self._csv_initialized: bool = False

    # ── Candles config (Phase 3) ─────────────────────────────────────────

    def get_candles_config(self) -> List[CandlesConfig]:
        """Subscribe to BTC-BRL 1m candles for volatility computation."""
        return [CandlesConfig(
            connector=self.config.candles_connector,
            trading_pair=self.config.candles_trading_pair,
            interval=self.config.candles_interval,
            max_records=self.max_records,
        )]

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

        # Phase 5 stub — neutral
        lead_state = compute_lead_state(mid_dec, mid_dec, self.config.basis_deadband_bps)

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

        regime_state = compute_regime_state(vol_state, inv_state)

        # Phase 1: skew = 0 (w_lead=0, skew_max_bps=0 by default)
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

        # §2.5 — net exposure in quote (base value at mid). Phase 2 only flags it;
        # Phase 4 will trigger kill switch when it exceeds max_net_position_quote.
        net_exposure_quote = base_bal * mid_dec
        over_max_net_position = net_exposure_quote > self.config.max_net_position_quote

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
            "vol_ratio": vol_state.vol_ratio,
            "regime": regime_state.regime,
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
        """Filter levels by sides_enabled (Phase 2: driven by inventory bands)."""
        levels = super().get_levels_to_execute()
        sides = self.processed_data.get("sides_enabled", {"buy": True, "sell": True})
        return [
            lv for lv in levels
            if (lv.startswith("buy") and sides.get("buy", True)) or
               (lv.startswith("sell") and sides.get("sell", True))
        ]

    # ── Executor config ───────────────────────────────────────────────────

    def get_executor_config(self, level_id: str, price: Decimal, amount: Decimal):
        """
        Create PositionExecutorConfig for a given level, applying inventory size_factor
        and §5.6 distance clamp (orders must not cross mid).

        Returns None when size_factor=0 (adversa side beyond hard_band).
        """
        trade_type = self.get_trade_type_from_level_id(level_id)
        size_factors: Dict[str, float] = self.processed_data.get(
            "size_factors", {"buy": 1.0, "sell": 1.0},
        )
        size_factor = Decimal(str(size_factors.get(trade_type.name.lower(), 1.0)))
        if size_factor == Decimal("0"):
            return None

        clamped_price = self._clamp_to_mid(price, trade_type)
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            level_id=level_id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_price=clamped_price,
            amount=amount * size_factor,
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
        Force-stop executors whose new price differs by ≥ force_requote_bps,
        even before their refresh time — protects against stale orders being
        sniped in fast markets (§5.7).
        """
        actions = list(super().executors_to_early_stop())
        threshold = Decimal(str(self.config.force_requote_bps))
        for executor in self.executors_info:
            if not executor.is_active or executor.is_trading:
                continue
            delta_bps = self._delta_bps_for_executor(executor)
            if delta_bps is not None and delta_bps >= threshold:
                actions.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=executor.id,
                ))
        return actions

    # ── CSV logging (Phase 2) ─────────────────────────────────────────────

    _CSV_COLUMNS = (
        "ts", "regime", "mid", "ref", "shift_bps", "spread_mult",
        "base_bal", "quote_bal", "net_exposure_quote", "over_max_net_position",
        "inv_pct", "inv_delta", "s_inv",
        "s_lead_micro", "s_lead_regime", "basis_bps", "vol_ratio",
        "buy_enabled", "sell_enabled",
        "sf_buy", "sf_sell",
    )

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
            "vol_ratio": d.get("vol_ratio", ""),
            "buy_enabled": d.get("sides_enabled", {}).get("buy", True),
            "sell_enabled": d.get("sides_enabled", {}).get("sell", True),
            "sf_buy": d.get("size_factors", {}).get("buy", ""),
            "sf_sell": d.get("size_factors", {}).get("sell", ""),
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
        inv_pct = d.get("inv_pct", 0.5)
        sides = d.get("sides_enabled", {"buy": True, "sell": True})
        ref_price = d.get("reference_price", "N/A")
        spread_mult = d.get("spread_multiplier", "N/A")
        shift_bps = d.get("price_shift_bps", Decimal("0"))
        vol_ratio = d.get("vol_ratio", 1.0)
        sf = d.get("size_factors", {"buy": 1.0, "sell": 1.0})
        net_exp = d.get("net_exposure_quote", Decimal("0"))
        over_cap = d.get("over_max_net_position", False)
        cap_flag = " ⚠ OVER-CAP" if over_cap else ""
        return [
            f"── PMM Lead-Lag Skew ({self.config.trading_pair}) ──────────────────",
            f"  Regime: {regime:<10}  Vol ratio: {vol_ratio:.2f}x  Spread mult: {spread_mult}",
            f"  Ref price: {ref_price}  Shift: {shift_bps} bps",
            f"  Inventory: {inv_pct:.1%}  Buy: {'ON ' if sides.get('buy') else 'OFF'}  Sell: {'ON' if sides.get('sell') else 'OFF'}",
            f"  Size factors  buy: {sf.get('buy'):.2f}  sell: {sf.get('sell'):.2f}",
            f"  Net exposure (quote): {net_exp} / {self.config.max_net_position_quote}{cap_flag}",
        ]

    def get_custom_info(self) -> dict:
        """Publish key signals via MQTT for monitoring."""
        d = self.processed_data
        sf = d.get("size_factors", {"buy": 1.0, "sell": 1.0})
        return {
            "regime": d.get("regime", "normal"),
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
        }
