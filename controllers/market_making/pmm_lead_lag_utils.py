"""
Pure functions and dataclasses for PMM Lead-Lag Skew controller.

Phase 1: all functions implemented or stubbed as neutral placeholders.
Phase 2: replace inventory stubs with live balance fetching.
Phase 3: activate skew (w_inv, skew_max_bps).
Phase 4: replace regime stub with real circuit breakers.
Phase 5: replace lead stubs with synthetic fair_brl and micro pause.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Tuple


# ── Dataclasses ──────────────────────────────────────────────────────────────


@dataclass
class InventoryState:
    inv_pct: float    # fraction of portfolio value held in base asset [0, 1]
    delta: float      # inv_pct - target_pct (positive = long base)
    s_inv: float      # normalized inventory signal clipped to [-1, 1]
    in_soft: bool     # |delta| >= soft_band  → size reduction
    in_hard: bool     # |delta| >= hard_band  → one-sided mode
    in_kill: bool     # |delta| >= inv_kill   → kill trigger


@dataclass
class VolState:
    vol_ratio: float          # σ_short / σ_ref (rolling median)
    spread_multiplier: float  # clamped to [1.0, 3.0]


@dataclass
class LeadState:
    """Phase 5 — lead-lag signals. Neutral stub in Phase 1."""
    s_lead_micro: float   # microstructural pause signal [-1, 1]
    s_lead_regime: float  # economic basis signal [-1, 1]
    basis_bps: float      # synthetic (BTCUSDT × USDTBRL) basis vs BTC-BRL mid, in bps
    micro_stale: bool     # True if BTC-USDT bookTicker feed is stale
    regime_stale: bool    # True if any of the 3 synthetic legs is stale


@dataclass
class RegimeState:
    """Phase 4 — regime classification. Always "normal" in Phase 1."""
    regime: str             # "normal" | "degraded" | "safe" | "paused" | "killed"
    spread_multiplier: float  # regime-adjusted multiplier (1x normal, 1.5x degraded)


@dataclass
class SkewState:
    skew_raw: float          # w_inv*s_inv + w_lead*s_lead_regime
    skew_norm: float         # tanh(skew_raw) in (-1, 1)
    price_shift_bps: Decimal # shift applied to reference_price
    # Convention: shift > 0 → ref_adj = mid*(1 - shift/10000) → ref goes DOWN
    #   → lower bids (penalizes buys) + lower asks (rewards sells) → used when LONG base


@dataclass
class SidePermissions:
    buy_enabled: bool
    sell_enabled: bool


@dataclass
class OrderParams:
    reference_price: Decimal   # adjusted reference (mid shifted by skew)
    spread_multiplier: Decimal # combined vol + regime multiplier
    price_shift_bps: Decimal   # informational: how much ref was shifted
    buy_enabled: bool
    sell_enabled: bool


# ── Pure functions ────────────────────────────────────────────────────────────


def compute_inventory_state(
    base_balance: Decimal,
    quote_balance: Decimal,
    mid_price: Decimal,
    target_pct: float,
    soft_band: float,
    hard_band: float,
    inv_kill: float,
) -> InventoryState:
    """
    Compute inventory fraction and normalized signal from raw balances.

    Parameters
    ----------
    base_balance  : base asset held (e.g. BTC)
    quote_balance : quote asset held (e.g. BRL)
    mid_price     : current mid price (quote per base)
    target_pct    : target fraction of portfolio in base [0, 1]
    soft_band     : deviation threshold for size reduction
    hard_band     : deviation threshold for one-sided mode
    inv_kill      : deviation threshold for kill switch
    """
    Q = base_balance * mid_price + quote_balance
    if Q <= Decimal("0"):
        return InventoryState(
            inv_pct=0.5, delta=0.0, s_inv=0.0,
            in_soft=False, in_hard=False, in_kill=False,
        )
    inv_pct = float(base_balance * mid_price / Q)
    delta = inv_pct - target_pct
    s_inv = max(-1.0, min(1.0, delta / soft_band)) if soft_band > 0 else 0.0
    return InventoryState(
        inv_pct=inv_pct,
        delta=delta,
        s_inv=s_inv,
        in_soft=abs(delta) >= soft_band,
        in_hard=abs(delta) >= hard_band,
        in_kill=abs(delta) >= inv_kill,
    )


def compute_size_factors(
    delta: float,
    soft_band: float,
    hard_band: float,
) -> Tuple[float, float]:
    """
    Return (size_factor_buy, size_factor_sell) using plan §2.3 zone-based logic.

    Convention: delta > 0 (long base) → BUY is the "adversa" side (pushes inventory
    further out), SELL is "favoravel" (helps reduce inventory).

    Zones:
      |Δ| ≤ soft   : both factors = 1.0 (neutral zone)
      soft < |Δ| ≤ hard : adversa decays 1.0→0.2 linearly; favoravel grows 1.0→1.5 capped
      |Δ| > hard   : adversa = 0.0 (turn off side); favoravel = 1.5

    See §2.3 unit-test invariant: when Δ > 0, size_factor_buy ≤ size_factor_sell.
    """
    abs_dev = abs(delta)
    if abs_dev <= soft_band:
        return 1.0, 1.0

    if abs_dev <= hard_band:
        t = (abs_dev - soft_band) / (hard_band - soft_band)
        sf_adversa = 1.0 - 0.8 * t
        sf_favoravel = min(1.5, 1.0 + 0.5 * t)
    else:
        sf_adversa = 0.0
        sf_favoravel = 1.5

    if delta > 0:
        # long base → BUY adversa, SELL favoravel
        return sf_adversa, sf_favoravel
    # short base (delta < 0) → SELL adversa, BUY favoravel
    return sf_favoravel, sf_adversa


def compute_vol_state(
    vol_short: float,
    vol_ref: float,
) -> VolState:
    """
    Compute volatility ratio and spread multiplier.
    Phase 1: pass vol_short=1.0, vol_ref=1.0 to get neutral multiplier=1.0.
    """
    ratio = vol_short / max(vol_ref, 1e-9)
    mult = max(1.0, min(3.0, ratio))
    return VolState(vol_ratio=ratio, spread_multiplier=mult)


def compute_volatility_from_prices(prices) -> float:
    """
    Compute σ of log returns from a sequence of prices.
    Returns 0.0 when input is empty, too short, or contains non-positive values.
    Used by the controller to derive σ_short and σ_ref from candles.
    """
    if prices is None:
        return 0.0
    try:
        seq = list(prices)
    except TypeError:
        return 0.0
    if len(seq) < 2:
        return 0.0
    log_returns = []
    prev = None
    for p in seq:
        try:
            x = float(p)
        except (TypeError, ValueError):
            return 0.0
        if prev is not None and prev > 0 and x > 0:
            log_returns.append(math.log(x / prev))
        prev = x
    if len(log_returns) < 2:
        return 0.0
    mean = sum(log_returns) / len(log_returns)
    var = sum((r - mean) ** 2 for r in log_returns) / (len(log_returns) - 1)
    return math.sqrt(var)


def compute_lead_state(
    mid_brl: Decimal,
    fair_brl: Decimal,
    basis_deadband_bps: float,
) -> LeadState:
    """
    Phase 5 stub — returns neutral lead state with zero signals.
    Replace in Phase 5 with real EWM/deadband/staleness logic.
    """
    return LeadState(
        s_lead_micro=0.0,
        s_lead_regime=0.0,
        basis_bps=0.0,
        micro_stale=False,
        regime_stale=False,
    )


def compute_regime_state(
    vol_state: VolState,
    inv_state: InventoryState,
) -> RegimeState:
    """
    Phase 4 stub — always returns normal regime with multiplier=1.0.
    Replace in Phase 4 with real L1/L1.5/L2/L3 circuit breaker logic.
    """
    return RegimeState(regime="normal", spread_multiplier=1.0)


def compute_skew_state(
    s_inv: float,
    s_lead_regime: float,
    w_inv: float,
    w_lead: float,
    skew_max_bps: float,
) -> SkewState:
    """
    Combine inventory and lead-lag signals into a reference price shift.

    sign convention: shift > 0 → reference goes DOWN → sell-favoring skew.
    """
    skew_raw = w_inv * s_inv + w_lead * s_lead_regime
    skew_norm = math.tanh(skew_raw)
    price_shift_bps = Decimal(str(round(skew_norm * skew_max_bps, 6)))
    return SkewState(
        skew_raw=skew_raw,
        skew_norm=skew_norm,
        price_shift_bps=price_shift_bps,
    )


def compute_side_permissions(
    delta: float,
    hard_cap: float,
    hard_band: float,
    current_buy_enabled: bool,
    current_sell_enabled: bool,
) -> SidePermissions:
    """
    One-sided mode with asymmetric hysteresis (plan §2.4).

    Trigger:  |delta| > hard_cap   → disable adverse side
    Release:  |delta| < hard_band  → re-enable both sides
    Inside the [hard_band, hard_cap] zone, current state is preserved (no flip-flop).

    Plan example: hard_band=0.20, hard_cap=0.30 → enter at 0.30, exit at 0.20.
    """
    # Buy side (adversa when long: delta > 0)
    if delta > hard_cap:
        buy_enabled = False
    elif delta < hard_band:
        buy_enabled = True
    else:
        buy_enabled = current_buy_enabled

    # Sell side (adversa when short: delta < 0)
    if delta < -hard_cap:
        sell_enabled = False
    elif delta > -hard_band:
        sell_enabled = True
    else:
        sell_enabled = current_sell_enabled

    return SidePermissions(buy_enabled=buy_enabled, sell_enabled=sell_enabled)


def compute_order_params(
    mid_price: Decimal,
    skew_state: SkewState,
    regime_state: RegimeState,
    vol_state: VolState,
    side_perms: SidePermissions,
) -> OrderParams:
    """
    Combine skew, vol and regime into final order parameters (plan §5.3).

    ref_adj = mid * (1 - price_shift_bps / 10000)
    shift > 0 → ref < mid → lower bids + lower asks → sell-favoring.
    shift < 0 → ref > mid → higher bids + higher asks → defensive.

    spread_multiplier = vol_multiplier * regime_multiplier
      vol_multiplier in [1.0, 3.0]; regime_multiplier = 1.0 (normal) or 1.5 (degraded).
    """
    ref_adj = mid_price * (Decimal("1") - skew_state.price_shift_bps / Decimal("10000"))
    combined_mult = Decimal(str(vol_state.spread_multiplier)) * Decimal(str(regime_state.spread_multiplier))
    return OrderParams(
        reference_price=ref_adj,
        spread_multiplier=combined_mult,
        price_shift_bps=skew_state.price_shift_bps,
        buy_enabled=side_perms.buy_enabled,
        sell_enabled=side_perms.sell_enabled,
    )
