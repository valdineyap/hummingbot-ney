import os
from decimal import Decimal
from typing import Dict

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.synthetic_triangular.synthetic_triangular_connector import (
    SyntheticTriangularConnector,
)
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from scripts.simple_xemm import SimpleXEMM, SimpleXEMMConfig


class XEMMTriangularConfig(SimpleXEMMConfig):
    """
    Extends SimpleXEMMConfig for triangulated maker on Binance.

    Roles:
      - maker_connector / maker_trading_pair: synthetic BTC-BRL (injected, not registered)
      - taker_connector / taker_trading_pair: real BTC-BRL on Bybit (direct pair)
      - leg_connector:  exchange hosting the real sub-legs (e.g. "binance")
      - leg1_pair:      base-to-intermediate pair (e.g. "BTC-USDT") — placed as LIMIT
      - leg2_pair:      intermediate-to-quote pair (e.g. "USDT-BRL") — placed as MARKET on fill

    The framework creates taker_connector and leg_connector as real connectors.
    The maker_connector (binance_synthetic) is injected in __init__ as a SyntheticTriangularConnector.
    """
    script_file_name: str = os.path.basename(__file__)

    maker_connector: str = Field("binance_synthetic", json_schema_extra={
        "prompt": "Synthetic connector name for the maker side", "prompt_on_new": True})
    maker_trading_pair: str = Field("BTC-BRL", json_schema_extra={
        "prompt": "Synthetic trading pair for the maker side", "prompt_on_new": True})
    taker_connector: str = Field("bybit", json_schema_extra={
        "prompt": "Real exchange for the taker side (direct BTC-BRL pair)", "prompt_on_new": True})
    taker_trading_pair: str = Field("BTC-BRL", json_schema_extra={
        "prompt": "Real trading pair for the taker side", "prompt_on_new": True})

    leg_connector: str = Field("binance", json_schema_extra={
        "prompt": "Real exchange hosting both maker sub-legs (e.g. binance)", "prompt_on_new": True})
    leg1_pair: str = Field("BTC-USDT", json_schema_extra={
        "prompt": "Maker sub-leg 1: base-to-intermediate pair (e.g. BTC-USDT)", "prompt_on_new": True})
    leg2_pair: str = Field("USDT-BRL", json_schema_extra={
        "prompt": "Maker sub-leg 2: intermediate-to-quote pair (e.g. USDT-BRL)", "prompt_on_new": True})

    usdt_rebalance_target: Decimal = Field(Decimal("60"), json_schema_extra={
        "prompt": "Target USDT buffer on maker exchange (raised to cover LIMIT reservation)", "prompt_on_new": True})
    usdt_rebalance_threshold: Decimal = Field(Decimal("10"), json_schema_extra={
        "prompt": "USDT drift tolerance before rebalancing", "prompt_on_new": True})
    usdt_rebalance_interval: int = Field(60, json_schema_extra={
        "prompt": "Ticks between USDT balance checks (~60 s at 1 s/tick)", "prompt_on_new": True})

    def update_markets(self, markets):
        markets[self.taker_connector] = markets.get(self.taker_connector, set()) | {self.taker_trading_pair}
        markets[self.leg_connector] = markets.get(self.leg_connector, set()) | {self.leg1_pair, self.leg2_pair}
        # maker_connector ("binance_synthetic") is NOT registered here — it is injected in __init__
        return markets


class XEMMTriangular(SimpleXEMM):
    """
    Cross-exchange market making with a triangulated maker hedge.

    Maker side → synthetic BTC-BRL on Binance (two real sub-legs):
        BTC-USDT LIMIT order (maker, tracked by strategy)
        USDT-BRL MARKET order (fires in did_fill_order after BTC-USDT fills)

    Taker side → real BTC-BRL MARKET order on Bybit (direct pair, simple hedge)

    SimpleXEMM (parent) is completely unmodified — it sees only two connectors with
    equal trading pairs and does not know about the triangulation.

    This class only:
      1. Injects SyntheticTriangularConnector as the maker connector before super().__init__
      2. Overrides did_fill_order() to complete the synthetic maker fill before taker hedge
      3. Wraps on_tick() to run the USDT rebalancer after SimpleXEMM's tick
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: XEMMTriangularConfig):
        # Inject the synthetic connector BEFORE super().__init__ so that
        # StrategyV2Base.add_markets() includes it in _sb_markets.
        synthetic = SyntheticTriangularConnector(
            connectors[config.leg_connector],
            config.leg1_pair,
            config.leg2_pair,
        )
        connectors[config.maker_connector] = synthetic

        super().__init__(connectors, config)

        # Add rate sources for the real underlying pairs
        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(connector_name=config.leg_connector, trading_pair=config.leg1_pair),
            ConnectorPair(connector_name=config.leg_connector, trading_pair=config.leg2_pair),
        ])

        self._rebalance_counter = 0

    def did_fill_order(self, event: OrderFilledEvent):
        """
        When a maker LIMIT order fills (BTC-USDT on Binance), complete the synthetic:
          - Fire USDT-BRL MARKET to neutralize the intermediate USDT position.
          - Then call super() so SimpleXEMM places the taker hedge on Bybit.
        """
        synthetic = self.connectors[self.config.maker_connector]
        if event.order_id == self.active_buy_order_id:
            synthetic.complete_maker_fill(True, event.amount, event.price)
        elif event.order_id == self.active_sell_order_id:
            synthetic.complete_maker_fill(False, event.amount, event.price)
        super().did_fill_order(event)

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
        conn = self.connectors[self.config.leg_connector]
        usdt = Decimal(str(conn.get_available_balance("USDT")))
        drift = usdt - self.config.usdt_rebalance_target

        if abs(drift) <= self.config.usdt_rebalance_threshold:
            return

        mid = Decimal(str(conn.get_mid_price(self.config.leg2_pair)))

        if drift > Decimal("0"):
            # Excess USDT → sell USDT for BRL
            self.sell(self.config.leg_connector, self.config.leg2_pair,
                      drift, OrderType.MARKET, mid)
            self.logger().info(f"USDT rebalance SELL {drift:.4f} ({self.config.leg2_pair}) — balance={usdt:.4f}")
        else:
            # Deficit USDT → buy USDT with BRL
            self.buy(self.config.leg_connector, self.config.leg2_pair,
                     abs(drift), OrderType.MARKET, mid)
            self.logger().info(f"USDT rebalance BUY {abs(drift):.4f} ({self.config.leg2_pair}) — balance={usdt:.4f}")
