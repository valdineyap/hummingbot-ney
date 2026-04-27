import os
from decimal import Decimal
from typing import Dict

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.synthetic_triangular.synthetic_triangular_connector import (
    SyntheticTriangularConnector,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import OrderFilledEvent
import scripts.simple_xemm as _simple_xemm


class XEMMTriangularConfig(_simple_xemm.SimpleXEMMConfig):
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

    triangulation_coin_rebalance_target: Decimal = Field(Decimal("100"), json_schema_extra={
        "prompt": "Target balance for the intermediate coin on the synthetic exchange (e.g. 100 USDT)",
        "prompt_on_new": True})
    triangulation_coin_rebalance_threshold: Decimal = Field(Decimal("10"), json_schema_extra={
        "prompt": "Max drift allowed from target before rebalancing (e.g. 10 USDT)",
        "prompt_on_new": True})
    triangulation_coin_rebalance_interval: int = Field(60, json_schema_extra={
        "prompt": "Ticks between intermediate coin balance checks (~60 s at 1 s/tick)",
        "prompt_on_new": True})

    def update_markets(self, markets):
        # Register the real direct-pair side (maker by default) and the leg exchange.
        # The synthetic side is injected in __init__ and must NOT be registered here.
        if not self.maker_connector.endswith("_synthetic"):
            markets[self.maker_connector] = markets.get(self.maker_connector, set()) | {self.maker_trading_pair}
        if not self.taker_connector.endswith("_synthetic"):
            markets[self.taker_connector] = markets.get(self.taker_connector, set()) | {self.taker_trading_pair}
        markets[self.leg_connector] = markets.get(self.leg_connector, set()) | {self.leg1_pair, self.leg2_pair}
        return markets


class XEMMTriangular(_simple_xemm.SimpleXEMM):
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
        self._rebalance_counter = 0
        self._startup_checked = False
        self._halted = False

    def place_buy_order(self, exchange, trading_pair, amount, order_type=OrderType.LIMIT):
        if exchange.endswith("_synthetic"):
            order_type = OrderType.MARKET
        super().place_buy_order(exchange, trading_pair, amount, order_type)

    def place_sell_order(self, exchange, trading_pair, amount, order_type=OrderType.LIMIT):
        if exchange.endswith("_synthetic"):
            order_type = OrderType.MARKET
        super().place_sell_order(exchange, trading_pair, amount, order_type)

    def did_fill_order(self, event: OrderFilledEvent):
        """
        If the MAKER side is synthetic, the LIMIT leg1 just filled —
        complete the synthetic by firing leg2 MARKET before super() places the taker hedge.

        If the TAKER side is synthetic (default), place_sell/buy_order above forces MARKET,
        so _place_taker_market fires both legs atomically — no completion step needed here.
        """
        maker = self.connectors[self.config.maker_connector]
        if isinstance(maker, SyntheticTriangularConnector):
            if event.order_id == self.active_buy_order_id:
                maker.complete_maker_fill(True, event.amount, event.price)
            elif event.order_id == self.active_sell_order_id:
                maker.complete_maker_fill(False, event.amount, event.price)
        super().did_fill_order(event)

    def _log_triangular_prices(self):
        """Log synthetic price breakdown (leg1 × leg2) and fee split every 60 ticks."""
        try:
            conn = self.connectors[self.config.leg_connector]
            amt = self.config.order_amount

            # Leg prices
            leg1_buy = conn.get_price_for_volume(self.config.leg1_pair, True, float(amt))
            leg1_sell = conn.get_price_for_volume(self.config.leg1_pair, False, float(amt))
            leg1_mid = conn.get_mid_price(self.config.leg1_pair)

            leg1_buy_p = Decimal(str(leg1_buy.result_price))
            leg1_sell_p = Decimal(str(leg1_sell.result_price))
            usdt_buy_notional = amt * leg1_buy_p
            usdt_sell_notional = amt * leg1_sell_p

            leg2_buy = conn.get_price_for_volume(self.config.leg2_pair, True, float(usdt_buy_notional))
            leg2_sell = conn.get_price_for_volume(self.config.leg2_pair, False, float(usdt_sell_notional))
            leg2_mid = conn.get_mid_price(self.config.leg2_pair)

            leg2_buy_p = Decimal(str(leg2_buy.result_price))
            leg2_sell_p = Decimal(str(leg2_sell.result_price))

            syn_buy = leg1_buy_p * leg2_buy_p
            syn_sell = leg1_sell_p * leg2_sell_p
            syn_mid = Decimal(str(leg1_mid)) * Decimal(str(leg2_mid))
            syn_spread_pct = (syn_buy - syn_sell) / syn_mid * Decimal("100") if syn_mid else Decimal("0")

            # Fee breakdown for synthetic taker
            base, quote = self.config.leg1_pair.split("-")
            leg1_fee = conn.get_fee(base, quote, OrderType.MARKET, TradeType.SELL, amt, leg1_sell_p, False)
            int_asset = self.config.leg1_pair.split("-")[1]
            leg2_base, leg2_quote = self.config.leg2_pair.split("-")
            leg2_fee = conn.get_fee(leg2_base, leg2_quote, OrderType.MARKET, TradeType.SELL,
                                    usdt_sell_notional, leg2_sell_p, False)

            # Bid-ask spread of each leg in bps
            leg1_spread_bps = (leg1_buy_p - leg1_sell_p) / Decimal(str(leg1_mid)) * Decimal("10000") if leg1_mid else Decimal("0")
            leg2_spread_bps = (leg2_buy_p - leg2_sell_p) / Decimal(str(leg2_mid)) * Decimal("10000") if leg2_mid else Decimal("0")
            syn_spread_bps = (syn_buy - syn_sell) / syn_mid * Decimal("10000") if syn_mid else Decimal("0")

            # Compare synthetic mid to maker (direct) mid
            maker_ob = self.connectors[self.config.maker_connector].get_order_book(self.config.maker_trading_pair)
            maker_best_bid = Decimal(str(maker_ob.get_price(False)))
            maker_best_ask = Decimal(str(maker_ob.get_price(True)))
            maker_mid = (maker_best_bid + maker_best_ask) / Decimal("2")
            syn_vs_direct_bps = (syn_mid - maker_mid) / maker_mid * Decimal("10000") if maker_mid else Decimal("0")

            self.logger().info(
                f"[TRI PRICES] "
                f"{self.config.leg1_pair}: mid={float(leg1_mid):.4f} bid={float(leg1_sell_p):.4f} ask={float(leg1_buy_p):.4f} spread={float(leg1_spread_bps):.2f}bps | "
                f"{self.config.leg2_pair}: mid={float(leg2_mid):.5f} bid={float(leg2_sell_p):.5f} ask={float(leg2_buy_p):.5f} spread={float(leg2_spread_bps):.2f}bps | "
                f"synthetic: mid={float(syn_mid):.2f} bid={float(syn_sell):.2f} ask={float(syn_buy):.2f} spread={float(syn_spread_bps):.2f}bps | "
                f"direct BTC-BRL: bid={float(maker_best_bid):.2f} ask={float(maker_best_ask):.2f} mid={float(maker_mid):.2f} | "
                f"synthetic_vs_direct={float(syn_vs_direct_bps):.1f}bps"
            )
            self.logger().info(
                f"[TRI FEES] leg1_fee={float(leg1_fee.percent * 100):.4f}% "
                f"leg2_fee={float(leg2_fee.percent * 100):.4f}% "
                f"total_taker_fee={float((leg1_fee.percent + leg2_fee.percent) * 100):.4f}% "
                f"intermediate={int_asset} notional≈{float(usdt_sell_notional):.4f}"
            )
        except Exception as e:
            self.logger().warning(f"[TRI PRICES] Error: {e}")

    def on_tick(self):
        if not self._startup_checked:
            self._startup_checked = True
            self._check_startup()

        if self._halted:
            coin = self.config.leg2_pair.split("-")[0]
            target = self.config.triangulation_coin_rebalance_target
            threshold = self.config.triangulation_coin_rebalance_threshold
            self.logger().warning(
                f"Bot halted: {coin} balance is too far from target={target} "
                f"(threshold={threshold}). Fix triangulation_coin_rebalance_target in config and restart."
            )
            return

        super().on_tick()

        # Log triangular price breakdown every 60 ticks
        if self._tick_counter % 60 == 1:
            self._log_triangular_prices()

        self._rebalance_counter += 1
        if self._rebalance_counter >= self.config.triangulation_coin_rebalance_interval:
            self._rebalance_counter = 0
            self._maybe_rebalance_triangulation_coin()

    def _check_startup(self):
        coin = self.config.leg2_pair.split("-")[0]
        conn = self.connectors[self.config.leg_connector]
        balance = Decimal(str(conn.get_available_balance(coin)))
        target = self.config.triangulation_coin_rebalance_target
        threshold = self.config.triangulation_coin_rebalance_threshold
        drift = abs(balance - target)

        if drift > threshold:
            self._halted = True
            self.logger().error(
                f"STARTUP HALT: {coin} balance={balance:.4f} differs from "
                f"triangulation_coin_rebalance_target={target} by {drift:.4f} "
                f"(max allowed={threshold}). "
                f"Rebalance {coin} manually or adjust the config before restarting."
            )
        else:
            self.logger().info(
                f"Startup check OK: {coin} balance={balance:.4f}, target={target}, drift={drift:.4f}"
            )

    def _maybe_rebalance_triangulation_coin(self):
        """
        Corrects residual drift in the intermediate coin accumulated from bid-ask
        slippage across hedge cycles. Fires a single MARKET order on leg2 to bring
        the balance back to target.
        """
        coin = self.config.leg2_pair.split("-")[0]
        conn = self.connectors[self.config.leg_connector]
        balance = Decimal(str(conn.get_available_balance(coin)))
        drift = balance - self.config.triangulation_coin_rebalance_target

        if abs(drift) <= self.config.triangulation_coin_rebalance_threshold:
            return

        mid = Decimal(str(conn.get_mid_price(self.config.leg2_pair)))

        if drift > Decimal("0"):
            self.sell(self.config.leg_connector, self.config.leg2_pair,
                      drift, OrderType.MARKET, mid)
            self.logger().info(
                f"{coin} rebalance SELL {drift:.4f} ({self.config.leg2_pair}) — balance={balance:.4f}"
            )
        else:
            self.buy(self.config.leg_connector, self.config.leg2_pair,
                     abs(drift), OrderType.MARKET, mid)
            self.logger().info(
                f"{coin} rebalance BUY {abs(drift):.4f} ({self.config.leg2_pair}) — balance={balance:.4f}"
            )
