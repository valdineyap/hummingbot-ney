from decimal import Decimal
from typing import Literal, Optional

from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class ArbitrageExecutorConfig(ExecutorConfigBase):
    type: Literal["arbitrage_executor"] = "arbitrage_executor"
    buying_market: ConnectorPair
    selling_market: ConnectorPair
    order_amount: Decimal
    min_profitability: Decimal
    gas_conversion_price: Optional[Decimal] = None


class LeadLagArbitrageExecutorConfig(ArbitrageExecutorConfig):
    """Arbitrage executor with automatic unwind on partial-leg failure."""
    type: Literal["lead_lag_arbitrage_executor"] = "lead_lag_arbitrage_executor"
    # Maximum slippage (bps) we're willing to pay to unwind a stuck position
    arb_max_unwind_slippage_bps: Decimal = Decimal("50")
    # Strategy when slippage exceeds gate: abort (leave position open + alert) or force
    arb_unwind_strategy: Literal["abort_and_alert", "force_unwind"] = "abort_and_alert"

    # Maker-leg execution mode (the slow side in our XEMM topology):
    #   "MARKET"            — classical MARKET order (server-side matching engine
    #                         can add ~600-1000ms vs ~80ms for LIMIT REST round-trip)
    #   "AGGRESSIVE_LIMIT"  — LIMIT priced best ± margin_pct; same matching outcome
    #                         when book is deep enough, but bounded slippage and
    #                         potentially faster REST path
    # Identification of which leg is "maker" comes from ``maker_connector_name``.
    arb_maker_leg_type: Literal["MARKET", "AGGRESSIVE_LIMIT"] = "MARKET"
    # Margin above (BUY) / below (SELL) best price used to make the LIMIT
    # marketable. 0.5% = 50 bps on BTC-BRL ≈ ~R$ 2000 — well beyond typical
    # book movement in <3s. Smaller margins risk no-fill in fast markets.
    arb_aggressive_limit_margin_pct: Decimal = Decimal("0.005")
    # Maximum wait (seconds) for the aggressive LIMIT to fully fill before
    # forcing cancel + handling partial / no fill via the unwind path.
    arb_aggressive_limit_timeout_sec: float = 3.0

    # Leg ordering policy. Controls when each placement REST is dispatched.
    # Role-based (maker/taker) for exchange-agnostic semantics — swapping
    # connectors doesn't invalidate the configuration intent.
    #   "parallel"     — both legs placed concurrently (default). Lowest
    #                    exposure window; REST round-trips overlap on the
    #                    event loop.
    #   "maker_first"  — place the leg on the maker connector first; wait
    #                    for its ACK (exchange_order_id assigned) before
    #                    placing the taker leg. Useful when you want to
    #                    ensure the slow side (typical for maker MARKET)
    #                    accepted before sending the fast taker leg.
    #   "taker_first"  — place the leg on the taker connector first; wait
    #                    for its ACK before placing the maker leg. Useful
    #                    to lock in the deeper-liquidity hedge price first.
    # The maker/taker identification is provided by the controller via
    # ``maker_connector_name``. If absent and a non-parallel mode is
    # requested, the executor falls back to parallel with a warning.
    arb_leg_execution_order: Literal[
        "maker_first", "taker_first", "parallel"
    ] = "parallel"
    # Identifies which leg is the "maker" side for leg-ordering purposes.
    # Required when ``arb_leg_execution_order`` is not "parallel". Set by
    # the controller from its own ``maker_connector`` config.
    maker_connector_name: Optional[str] = None
