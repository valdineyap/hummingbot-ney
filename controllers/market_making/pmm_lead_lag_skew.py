"""
PMM Lead-Lag Skew controller for BTC-BRL on Binance Spot (Hummingbot V2).

Phase 1: bilateral PMM puro — no skew, no regime filter, no lead-lag.
Phase 2: inventory tracking + one-sided mode.
Phase 3: inventory skew + volatility spread multiplier.
Phase 4: regime circuit breakers.
Phase 5: lead-lag micro pause + synthetic fair_brl.
"""
from decimal import Decimal
from typing import Dict, List

from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from hummingbot.core.data_type.common import PriceType
from hummingbot.strategy_v2.controllers.market_making_controller_base import (
    MarketMakingControllerBase,
    MarketMakingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig

from controllers.market_making.pmm_lead_lag_utils import (
    compute_inventory_state,
    compute_lead_state,
    compute_order_params,
    compute_regime_state,
    compute_side_permissions,
    compute_size_factors,
    compute_skew_state,
    compute_vol_state,
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
        default=0.0, ge=0.0,
        json_schema_extra={"prompt": "Max skew in bps (0=disabled, Phase3=2, teto=8): ", "is_updatable": True},
    )
    min_maker_distance_bps: float = Field(default=1.0, gt=0.0)
    min_requote_bps: float = Field(default=1.0, ge=0.0)

    # ── Lead-lag (Phase 5) ───────────────────────────────────────────────
    basis_deadband_bps: float = Field(default=3.0, ge=0.0)
    max_leader_staleness_sec: float = Field(default=5.0, gt=0.0)
    max_usdt_brl_staleness_sec: float = Field(default=15.0, gt=0.0)


class PMMLeadLagSkewController(MarketMakingControllerBase):
    """
    Pure Market Making controller with inventory skew and lead-lag defense.

    Phase 1: bilateral PMM at fixed spreads. No skew, no regime, no lead-lag.
    All signals computed and stored in processed_data for future phases.
    """

    def __init__(self, config: PMMLeadLagSkewConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        # Hysteresis state for one-sided mode (Phase 2+)
        self._buy_enabled: bool = True
        self._sell_enabled: bool = True

    # ── Data update ───────────────────────────────────────────────────────

    async def update_processed_data(self):
        """
        Phase 1: reference_price = MidPrice, spread_multiplier = 1, no skew.
        All signals are neutral stubs — replaced incrementally in Phases 2–5.
        """
        mid_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice
        )
        mid_dec = Decimal(str(mid_price))

        # Phase 1: vol neutral, lead neutral, regime normal
        vol_state = compute_vol_state(1.0, 1.0)
        lead_state = compute_lead_state(mid_dec, mid_dec, self.config.basis_deadband_bps)

        # Phase 1: inventory stub — neutral 50/50 (Phase 2 replaces with live balance fetch)
        inv_state = compute_inventory_state(
            base_balance=Decimal("0"),
            quote_balance=Decimal("1"),
            mid_price=Decimal("1"),
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
        Create PositionExecutorConfig for a given level.
        Phase 1: fixed size.
        Phase 2+: apply size_factor from inventory signal.
        """
        trade_type = self.get_trade_type_from_level_id(level_id)
        size_factors: Dict[str, float] = self.processed_data.get("size_factors", {"buy": 1.0, "sell": 1.0})
        size_factor = Decimal(str(size_factors.get(trade_type.name.lower(), 1.0)))
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            level_id=level_id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_price=price,
            amount=amount * size_factor,
            triple_barrier_config=self.config.triple_barrier_config,
            leverage=self.config.leverage,
            side=trade_type,
        )

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
        return [
            f"── PMM Lead-Lag Skew ({self.config.trading_pair}) ──────────────────",
            f"  Regime: {regime:<10}  Vol ratio: {vol_ratio:.2f}x  Spread mult: {spread_mult}",
            f"  Ref price: {ref_price}  Shift: {shift_bps} bps",
            f"  Inventory: {inv_pct:.1%}  Buy: {'ON ' if sides.get('buy') else 'OFF'}  Sell: {'ON' if sides.get('sell') else 'OFF'}",
        ]

    def get_custom_info(self) -> dict:
        """Publish key signals via MQTT for monitoring."""
        d = self.processed_data
        return {
            "regime": d.get("regime", "normal"),
            "inv_pct": round(d.get("inv_pct", 0.5), 4),
            "s_inv": round(d.get("s_inv", 0.0), 4),
            "price_shift_bps": float(d.get("price_shift_bps", 0)),
            "vol_ratio": round(d.get("vol_ratio", 1.0), 3),
            "buy_enabled": d.get("sides_enabled", {}).get("buy", True),
            "sell_enabled": d.get("sides_enabled", {}).get("sell", True),
        }
