import os
from decimal import Decimal
from typing import Dict

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.synthetic_triangular.synthetic_triangular_connector import (
    SyntheticTriangularConnector,
)
from hummingbot.core.data_type.common import OrderType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from scripts.simple_xemm import SimpleXEMM, SimpleXEMMConfig


class XEMMTriangularConfig(SimpleXEMMConfig):
    """
    Extends SimpleXEMMConfig with fields for the two real taker legs and USDT rebalancer.

    Usage:
      - maker_connector / maker_trading_pair: direct real pair (e.g. Binance BTC-BRL)
      - taker_connector:   logical name for the synthetic connector (e.g. "bybit_synthetic")
      - taker_trading_pair: the virtual pair exposed by the synthetic (e.g. "BTC-BRL")
      - leg1_connector:    exchange that hosts both real legs (e.g. "bybit")
      - leg1_pair:         base-to-intermediate pair on leg1_connector (e.g. "BTC-USDT")
      - leg2_pair:         intermediate-to-quote pair on leg1_connector (e.g. "USDT-BRL")

    The framework creates only maker_connector and leg1_connector as real connectors.
    The taker_connector is the SyntheticTriangularConnector injected in __init__.
    """
    script_file_name: str = os.path.basename(__file__)

    leg1_connector: str = Field("bybit", json_schema_extra={
        "prompt": "Real exchange hosting both taker legs (e.g. bybit)", "prompt_on_new": True})
    leg1_pair: str = Field("BTC-USDT", json_schema_extra={
        "prompt": "Taker leg 1: base-to-intermediate pair (e.g. BTC-USDT)", "prompt_on_new": True})
    leg2_pair: str = Field("USDT-BRL", json_schema_extra={
        "prompt": "Taker leg 2: intermediate-to-quote pair (e.g. USDT-BRL)", "prompt_on_new": True})

    usdt_rebalance_target: Decimal = Field(Decimal("10"), json_schema_extra={
        "prompt": "Target USDT buffer on taker exchange", "prompt_on_new": True})
    usdt_rebalance_threshold: Decimal = Field(Decimal("2"), json_schema_extra={
        "prompt": "USDT drift tolerance before rebalancing", "prompt_on_new": True})
    usdt_rebalance_interval: int = Field(60, json_schema_extra={
        "prompt": "Ticks between USDT balance checks (~60 s at 1 s/tick)", "prompt_on_new": True})

    def update_markets(self, markets):
        markets[self.maker_connector] = markets.get(self.maker_connector, set()) | {self.maker_trading_pair}
        # Register the REAL leg exchange with both underlying pairs (not the synthetic name)
        markets[self.leg1_connector] = markets.get(self.leg1_connector, set()) | {self.leg1_pair, self.leg2_pair}
        # taker_connector ("bybit_synthetic") is NOT registered here — it is injected in __init__
        return markets


class XEMMTriangular(SimpleXEMM):
    """
    Cross-exchange market making with a triangulated taker hedge.

    Maker side → direct real pair (e.g. Binance BTC-BRL, LIMIT orders)
    Taker side → two real MARKET orders via SyntheticTriangularConnector:
        buy  BTC-BRL synthetic: BUY  USDT-BRL then BUY  BTC-USDT
        sell BTC-BRL synthetic: SELL BTC-USDT  then SELL USDT-BRL

    SimpleXEMM (parent) is completely unmodified — it sees only two connectors with
    equal trading pairs and does not know about the triangulation.

    This class only:
      1. Injects SyntheticTriangularConnector as the taker connector before super().__init__
      2. Wraps on_tick() to run the USDT rebalancer after SimpleXEMM's tick
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: XEMMTriangularConfig):
        # Inject the synthetic connector BEFORE super().__init__ so that
        # StrategyV2Base.add_markets() includes it in _sb_markets.
        synthetic = SyntheticTriangularConnector(
            connectors[config.leg1_connector],
            config.leg1_pair,
            config.leg2_pair,
        )
        connectors[config.taker_connector] = synthetic

        super().__init__(connectors, config)

        # Add rate sources for the real underlying pairs
        # (SimpleXEMM already added maker and the synthetic taker pair)
        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(connector_name=config.leg1_connector, trading_pair=config.leg1_pair),
            ConnectorPair(connector_name=config.leg1_connector, trading_pair=config.leg2_pair),
        ])

        self._rebalance_counter = 0

    def on_tick(self):
        super().on_tick()  # full SimpleXEMM logic: prices, maker orders, cancels, hedges

        self._rebalance_counter += 1
        if self._rebalance_counter >= self.config.usdt_rebalance_interval:
            self._rebalance_counter = 0
            self._maybe_rebalance_usdt()

    def _maybe_rebalance_usdt(self):
        """
        Corrects residual USDT drift accumulated from bid-ask slippage across hedge cycles.
        Fires a single MARKET order on the intermediate pair to bring USDT back to target.
        """
        conn = self.connectors[self.config.leg1_connector]
        usdt = Decimal(str(conn.get_available_balance("USDT")))
        drift = usdt - self.config.usdt_rebalance_target

        if abs(drift) <= self.config.usdt_rebalance_threshold:
            return

        mid = Decimal(str(conn.get_mid_price(self.config.leg2_pair)))

        if drift > Decimal("0"):
            # Excess USDT → sell USDT for BRL
            self.sell(self.config.leg1_connector, self.config.leg2_pair,
                      drift, OrderType.MARKET, mid)
            self.logger().info(f"USDT rebalance SELL {drift:.4f} ({self.config.leg2_pair}) — balance={usdt:.4f}")
        else:
            # Deficit USDT → buy USDT with BRL
            self.buy(self.config.leg1_connector, self.config.leg2_pair,
                     abs(drift), OrderType.MARKET, mid)
            self.logger().info(f"USDT rebalance BUY {abs(drift):.4f} ({self.config.leg2_pair}) — balance={usdt:.4f}")
