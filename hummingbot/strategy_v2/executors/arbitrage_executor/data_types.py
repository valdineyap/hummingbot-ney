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
