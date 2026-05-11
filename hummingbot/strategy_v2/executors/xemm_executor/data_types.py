from decimal import Decimal
from typing import Literal

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class XEMMExecutorConfig(ExecutorConfigBase):
    type: Literal["xemm_executor"] = "xemm_executor"
    buying_market: ConnectorPair
    selling_market: ConnectorPair
    maker_side: TradeType
    order_amount: Decimal
    min_profitability: Decimal
    target_profitability: Decimal
    max_profitability: Decimal


class XEMMLeadLagExecutorConfig(XEMMExecutorConfig):
    type: Literal["xemm_lead_lag_executor"] = "xemm_lead_lag_executor"
    # Extra headroom above min_profitability used only at order placement to create
    # hysteresis and reduce churn near the cancel floor.
    placement_profitability_buffer: Decimal = Decimal("0.0002")
    # Lead-aware placement (Priority 4): controller passes the current best_lead_bps
    # and adjustment params; executor adjusts effective_min in create_maker_order.
    # When the lead signal favours the side, place tighter; when it opposes, place wider.
    # Set placement_lead_aware_delta_bps=0 to disable.
    lead_signal_bps: Decimal = Decimal("0")
    placement_lead_aware_delta_bps: Decimal = Decimal("0")
    placement_lead_signal_threshold_bps: Decimal = Decimal("3")
