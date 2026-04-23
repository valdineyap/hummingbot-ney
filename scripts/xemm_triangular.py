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
    Extends SimpleXEMMConfig for triangulated XEMM with one synthetic side.

    The synthetic connector is role-agnostic — set it as either maker_connector
    or taker_connector. Defaults below put it as the TAKER (Bybit synthetic),
    with a real direct pair on the maker side (Binance BTC-BRL).

    Roles (defaults):
      - maker_connector / maker_trading_pair: real BTC-BRL on Binance (LIMIT)
      - taker_connector / taker_trading_pair: synthetic BTC-BRL on Bybit (MARKET, fire-and-forget)
      - leg_connector:  exchange hosting the real sub-legs (e.g. "bybit")
      - leg1_pair:      base-to-intermediate pair (e.g. "BTC-USDT")
      - leg2_pair:      intermediate-to-quote pair (e.g. "USDT-BRL")

    The framework creates the real connectors (maker_connector and leg_connector).
    The synthetic side (whichever points to "*_synthetic") is injected in __init__.
    """
    script_file_name: str = os.path.basename(__file__)

    maker_connector: str = Field("binance", json_schema_extra={
        "prompt": "Maker connector (real direct pair, e.g. binance)", "prompt_on_new": True})
    maker_trading_pair: str = Field("BTC-BRL", json_schema_extra={
        "prompt": "Maker trading pair (e.g. BTC-BRL)", "prompt_on_new": True})
    taker_connector: str = Field("bybit_synthetic", json_schema_extra={
        "prompt": "Taker connector (synthetic name, e.g. bybit_synthetic)", "prompt_on_new": True})
    taker_trading_pair: str = Field("BTC-BRL", json_schema_extra={
        "prompt": "Taker trading pair (synthetic, e.g. BTC-BRL)", "prompt_on_new": True})

    leg_connector: str = Field("bybit", json_schema_extra={
        "prompt": "Real exchange hosting both synthetic sub-legs (e.g. bybit)", "prompt_on_new": True})
    leg1_pair: str = Field("BTC-USDT", json_schema_extra={
        "prompt": "Synthetic sub-leg 1: base-to-intermediate pair (e.g. BTC-USDT)", "prompt_on_new": True})
    leg2_pair: str = Field("USDT-BRL", json_schema_extra={
        "prompt": "Synthetic sub-leg 2: intermediate-to-quote pair (e.g. USDT-BRL)", "prompt_on_new": True})

    usdt_rebalance_target: Decimal = Field(Decimal("10"), json_schema_extra={
        "prompt": "Target USDT buffer on the synthetic exchange", "prompt_on_new": True})
    usdt_rebalance_threshold: Decimal = Field(Decimal("2"), json_schema_extra={
        "prompt": "USDT drift tolerance before rebalancing", "prompt_on_new": True})
    usdt_rebalance_interval: int = Field(60, json_schema_extra={
        "prompt": "Ticks between USDT balance checks (~60 s at 1 s/tick)", "prompt_on_new": True})

    def update_markets(self, markets):
        # Register the real direct-pair side (maker by default) and the leg exchange.
        # The synthetic side is injected in __init__ and must NOT be registered here.
        if not self.maker_connector.endswith("_synthetic"):
            markets[self.maker_connector] = markets.get(self.maker_connector, set()) | {self.maker_trading_pair}
        if not self.taker_connector.endswith("_synthetic"):
            markets[self.taker_connector] = markets.get(self.taker_connector, set()) | {self.taker_trading_pair}
        markets[self.leg_connector] = markets.get(self.leg_connector, set()) | {self.leg1_pair, self.leg2_pair}
        return markets


class XEMMTriangular(SimpleXEMM):
    """
    Cross-exchange market making with one triangulated side.

    Default config: synthetic is the TAKER on Bybit (BTC-USDT × USDT-BRL → BTC-BRL),
    fired as MARKET fire-and-forget when the maker fills on Binance.

    Swapping roles is purely a config change — point maker_connector to the
    "*_synthetic" name to make the maker side triangulated instead. The
    did_fill_order override below detects which side is synthetic at runtime.

    SimpleXEMM (parent) is completely unmodified — it sees only two connectors
    with equal trading pairs and does not know about the triangulation.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: XEMMTriangularConfig):
        # Inject the synthetic connector (whichever side is "*_synthetic") BEFORE
        # super().__init__ so that StrategyV2Base.add_markets() includes it in _sb_markets.
        synthetic = SyntheticTriangularConnector(
            connectors[config.leg_connector],
            config.leg1_pair,
            config.leg2_pair,
        )
        if config.maker_connector.endswith("_synthetic"):
            connectors[config.maker_connector] = synthetic
        elif config.taker_connector.endswith("_synthetic"):
            connectors[config.taker_connector] = synthetic
        else:
            raise ValueError(
                "Neither maker_connector nor taker_connector ends with '_synthetic'. "
                "One side must be the synthetic name (e.g. 'bybit_synthetic')."
            )

        super().__init__(connectors, config)

        # Add rate sources for the real underlying pairs
        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(connector_name=config.leg_connector, trading_pair=config.leg1_pair),
            ConnectorPair(connector_name=config.leg_connector, trading_pair=config.leg2_pair),
        ])

        self._rebalance_counter = 0

    def did_fill_order(self, event: OrderFilledEvent):
        """
        If the MAKER side is synthetic, the LIMIT leg1 (BTC-USDT) just filled —
        complete the synthetic by firing leg2 (USDT-BRL) MARKET before super()
        places the taker hedge.

        If the TAKER side is synthetic (default), the maker is a real direct pair
        and SimpleXEMM.did_fill_order will call place_*_order on the synthetic
        taker, which fires both legs atomically via _place_taker_market — no
        completion step needed here.
        """
        maker = self.connectors[self.config.maker_connector]
        if isinstance(maker, SyntheticTriangularConnector):
            if event.order_id == self.active_buy_order_id:
                maker.complete_maker_fill(True, event.amount, event.price)
            elif event.order_id == self.active_sell_order_id:
                maker.complete_maker_fill(False, event.amount, event.price)
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
