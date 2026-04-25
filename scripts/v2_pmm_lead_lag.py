"""
Boot script for PMM Lead-Lag Skew on BTC-BRL (Hummingbot V2).

Derived from scripts/v2_with_controllers.py.
Adds:
  - Wires PMMLeadLagSkewController via YAML config.
  - Session-level drawdown guard (in addition to the controller's own kill switch).

Usage:
  start --script v2_pmm_lead_lag.py --conf conf/scripts/conf_v2_pmm_lead_lag.yml

The script reads the controller config from:
  conf/controllers/conf_pmm_lead_lag_skew.yml
"""
import os
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class V2PMMLeadLagConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    # Global session drawdown limit in quote (e.g. 10 = R$10 = ~5% of R$200 initial capital)
    max_global_drawdown_quote: Optional[float] = 10.0
    # Per-controller drawdown limit (None = rely on controller kill switch)
    max_controller_drawdown_quote: Optional[float] = None


class V2PMMLeadLag(StrategyV2Base):
    """
    Thin wrapper that adds session-level drawdown protection on top of the
    PMMLeadLagSkewController's own regime/kill switch logic.
    """

    performance_report_interval: int = 1

    def __init__(self, connectors: Dict[str, ConnectorBase], config: V2PMMLeadLagConfig):
        super().__init__(connectors, config)
        self.config = config
        self.max_pnl_by_controller: Dict[str, Decimal] = {}
        self.max_global_pnl: Decimal = Decimal("0")
        self.drawdown_exited_controllers: List[str] = []
        self._last_performance_report_timestamp: float = 0

    def on_tick(self):
        super().on_tick()
        if not self._is_stop_triggered:
            self.check_manual_kill_switch()
            self.control_max_drawdown()

    def apply_initial_setting(self):
        for controller_id in self.controllers:
            self.max_pnl_by_controller[controller_id] = Decimal("0")

    def control_max_drawdown(self):
        if self.config.max_controller_drawdown_quote:
            self._check_controller_drawdown()
        if self.config.max_global_drawdown_quote:
            self._check_global_drawdown()

    def _check_controller_drawdown(self):
        for controller_id, controller in self.controllers.items():
            if controller.status != RunnableStatus.RUNNING:
                continue
            pnl = self.get_performance_report(controller_id).global_pnl_quote
            peak = self.max_pnl_by_controller.get(controller_id, Decimal("0"))
            if pnl > peak:
                self.max_pnl_by_controller[controller_id] = pnl
            elif peak - pnl > self.config.max_controller_drawdown_quote:
                self.logger().warning(
                    f"Controller {controller_id} hit max drawdown "
                    f"({float(peak - pnl):.2f} quote). Stopping."
                )
                controller.stop()
                non_trading = self.filter_executors(
                    executors=self.get_executors_by_controller(controller_id),
                    filter_func=lambda x: x.is_active and not x.is_trading,
                )
                self.executor_orchestrator.execute_actions(
                    [StopExecutorAction(controller_id=controller_id, executor_id=e.id)
                     for e in non_trading]
                )
                self.drawdown_exited_controllers.append(controller_id)

    def _check_global_drawdown(self):
        global_pnl = sum(
            self.get_performance_report(cid).global_pnl_quote
            for cid in self.controllers
        )
        if global_pnl > self.max_global_pnl:
            self.max_global_pnl = global_pnl
        elif self.max_global_pnl - global_pnl > self.config.max_global_drawdown_quote:
            self.logger().warning(
                f"Global drawdown exceeded "
                f"({float(self.max_global_pnl - global_pnl):.2f} quote). Stopping strategy."
            )
            self._is_stop_triggered = True
            HummingbotApplication.main_application().stop()

    def check_manual_kill_switch(self):
        for controller_id, controller in self.controllers.items():
            if controller.config.manual_kill_switch and controller.status == RunnableStatus.RUNNING:
                self.logger().info(f"Manual kill switch activated for {controller_id}.")
                controller.stop()
                self.executor_orchestrator.execute_actions(
                    [StopExecutorAction(executor_id=e.id, controller_id=e.controller_id)
                     for e in self.get_executors_by_controller(controller_id)]
                )
            elif not controller.config.manual_kill_switch and controller.status == RunnableStatus.TERMINATED:
                if controller_id not in self.drawdown_exited_controllers:
                    self.logger().info(f"Restarting controller {controller_id}.")
                    controller.start()

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def did_fail_order(self, order_failed_event: MarketOrderFailureEvent):
        self.logger().error(f"Order failed: {order_failed_event}")
