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
    """
    Phase 5 — lead-lag signals.

    Two horizons, separate sources (plan §5.0):
      - micro (1–5s): BTC-USDT pure (no FX); used as defensive pause flag.
      - regime (30s–5min): synthetic fair_brl = mid_usdt × usdt_brl, used as
        a small contribution to skew via w_lead.

    Sign convention for s_lead_regime: positive lag (BRL below fair, USDT moved up)
    must produce a NEGATIVE s_lead_regime so that the skew formula
    (w_inv*s_inv + w_lead*s_lead) lifts ref_adj (asks go higher) — defensive,
    not trend-following. See plan §5.2.
    """
    s_lead_micro: float    # signed lag in bps (micro horizon)
    s_lead_regime: float   # tanh-saturated regime signal in [-1, 1]
    basis_bps: float       # (fair_brl - mid_brl)/mid_brl × 1e4
    micro_stale: bool      # True if BTC-USDT/BTC-BRL feeds are stale (micro disabled)
    regime_stale: bool     # True if any of the 3 synthetic legs is stale (regime=0)
    pause_buy: bool        # micro pause: lag<-threshold (USDT down, BRL lagging) → bids over-priced
    pause_sell: bool       # micro pause: lag>+threshold (USDT up, BRL lagging) → asks under-priced


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


def ewm_step(
    prev: float,
    new_value: float,
    halflife_sec: float,
    dt_sec: float,
) -> float:
    """
    Single-step continuous-time EWM update. Returns prev when prev<=0 (warmup).
    halflife_sec: half-life in seconds; dt_sec: time elapsed since prev.
    α = 1 - 0.5 ** (dt/halflife)  → exponential smoothing with given half-life.
    """
    try:
        x = float(new_value)
    except (TypeError, ValueError):
        return prev
    if x <= 0:
        return prev
    if prev <= 0:
        return x
    if halflife_sec <= 0 or dt_sec <= 0:
        return x
    alpha = 1.0 - 0.5 ** (dt_sec / halflife_sec)
    return prev + alpha * (x - prev)


def compute_lag_regime(
    mid_brl_now: float,
    mid_brl_past: float,
    fair_brl_smooth_now: float,
    fair_brl_smooth_past: float,
    sigma_lag: float,
    basis_bps: float,
    basis_deadband_bps: float,
    regime_stale: bool,
) -> Tuple[float, float]:
    """
    Plan §5.2 — regime lead-lag (30s–5min, synthetic fair_brl).

    Returns
    -------
    (s_lead_regime, z_lead)

    Sign convention (defensive):
      lag_short = ln(fair_now/fair_past) - ln(brl_now/brl_past)
      lag > 0   → fair grew faster than BRL (BRL lagging up) → ASKS under-priced
                  → defense: lift ref → s_lead_regime NEGATIVE (so the skew formula
                  w_inv*s_inv + w_lead*s_lead, when shift = sign-corrected, lifts ref).

    The caller adds an inv-skew sign convention (shift>0 lowers ref → defensive
    means shift<0 → ref up). compute_skew_state combines the two so that a
    negative s_lead_regime contributes to a negative skew_norm and thus a
    negative price_shift_bps → ref_adj > mid → defensive (asks higher).

    Filters:
      - regime_stale → return (0.0, 0.0)
      - |basis_bps| < deadband → return (0.0, z_lead) (suppress contribution
        but keep z for telemetry)
    """
    if regime_stale:
        return 0.0, 0.0
    try:
        mb_now = float(mid_brl_now)
        mb_past = float(mid_brl_past)
        f_now = float(fair_brl_smooth_now)
        f_past = float(fair_brl_smooth_past)
        sig = float(sigma_lag)
    except (TypeError, ValueError):
        return 0.0, 0.0
    if mb_now <= 0 or mb_past <= 0 or f_now <= 0 or f_past <= 0:
        return 0.0, 0.0

    r_fair = math.log(f_now / f_past)
    r_actual = math.log(mb_now / mb_past)
    lag_short = r_fair - r_actual
    z_lead = lag_short / max(sig, 1e-9)

    if abs(basis_bps) < basis_deadband_bps:
        return 0.0, z_lead

    # Defensive sign: lag>0 → s_lead<0 → skew lifts ref → asks higher.
    s_lead = -math.tanh(z_lead / 2.0)
    # Saturate explicitly (tanh already in (-1,1) but clip defensively):
    s_lead = max(-1.0, min(1.0, s_lead))
    return s_lead, z_lead


def compute_lag_micro(
    mid_brl_now: float,
    mid_brl_past: float,
    mid_usdt_now: float,
    mid_usdt_past: float,
    usdt_brl_ref: float,
    threshold_bps: float,
    micro_stale: bool,
) -> Tuple[float, bool, bool]:
    """
    Plan §5.2 — micro lead-lag (1–5s, BTC-USDT pure).

    Computes the implied move of BTC-BRL had it tracked BTC-USDT exactly,
    compared to the actual BTC-BRL move. The FX rate enters only as a static
    scale factor (no FX delta), because variations of usdt_brl in 5s are noise.

    Returns
    -------
    (lag_micro_bps, pause_buy, pause_sell)

    Direction:
      lag > 0  → USDT moved UP, BRL didn't catch up → ASKS under-priced → pause SELL
      lag < 0  → USDT moved DOWN, BRL didn't catch up → BIDS over-priced → pause BUY

    When inputs are stale or invalid, returns (0.0, False, False).
    """
    if micro_stale:
        return 0.0, False, False
    try:
        mb_now = float(mid_brl_now)
        mb_past = float(mid_brl_past)
        mu_now = float(mid_usdt_now)
        mu_past = float(mid_usdt_past)
        rate = float(usdt_brl_ref)
    except (TypeError, ValueError):
        return 0.0, False, False
    if mb_now <= 0 or mb_past <= 0 or mu_now <= 0 or mu_past <= 0 or rate <= 0:
        return 0.0, False, False

    delta_usdt = mu_now - mu_past
    implied_delta_brl = delta_usdt * rate
    actual_delta_brl = mb_now - mb_past
    lag_bps = (implied_delta_brl - actual_delta_brl) / mb_now * 1e4

    pause_sell = lag_bps > threshold_bps
    pause_buy = lag_bps < -threshold_bps
    return lag_bps, pause_buy, pause_sell


def compute_lead_state(
    mid_brl_now: float,
    mid_brl_past: float,
    mid_usdt_now: float,
    mid_usdt_past: float,
    usdt_brl_ref: float,
    fair_brl_smooth_now: float,
    fair_brl_smooth_past: float,
    sigma_lag: float,
    micro_threshold_bps: float,
    basis_deadband_bps: float,
    micro_stale: bool,
    regime_stale: bool,
) -> LeadState:
    """
    Phase 5 — compose micro + regime signals into a single LeadState.

    The synthetic basis_bps is computed from the smoothed fair_brl vs current
    mid_brl. When `regime_stale` is True (any of the 3 legs stale) the regime
    signal is suppressed even before the deadband check (see compute_lag_regime).

    Returns a LeadState with neutral defaults if inputs are unusable.
    """
    # Basis (used by the regime deadband and exposed for telemetry)
    try:
        mb_now = float(mid_brl_now)
        f_smooth = float(fair_brl_smooth_now)
        basis_bps = (f_smooth - mb_now) / mb_now * 1e4 if (mb_now > 0 and f_smooth > 0) else 0.0
    except (TypeError, ValueError):
        basis_bps = 0.0

    s_lead_micro, pause_buy, pause_sell = compute_lag_micro(
        mid_brl_now=mid_brl_now,
        mid_brl_past=mid_brl_past,
        mid_usdt_now=mid_usdt_now,
        mid_usdt_past=mid_usdt_past,
        usdt_brl_ref=usdt_brl_ref,
        threshold_bps=micro_threshold_bps,
        micro_stale=micro_stale,
    )
    s_lead_regime, _z = compute_lag_regime(
        mid_brl_now=mid_brl_now,
        mid_brl_past=mid_brl_past,
        fair_brl_smooth_now=fair_brl_smooth_now,
        fair_brl_smooth_past=fair_brl_smooth_past,
        sigma_lag=sigma_lag,
        basis_bps=basis_bps,
        basis_deadband_bps=basis_deadband_bps,
        regime_stale=regime_stale,
    )
    return LeadState(
        s_lead_micro=s_lead_micro,
        s_lead_regime=s_lead_regime,
        basis_bps=basis_bps,
        micro_stale=micro_stale,
        regime_stale=regime_stale,
        pause_buy=pause_buy,
        pause_sell=pause_sell,
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
