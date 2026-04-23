import os
from decimal import Decimal
from typing import Dict, List

import pandas as pd
from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.ney_synthetic_triangular.synthetic_triangular_connector import (
    SyntheticTriangularPriceProvider,
)
from hummingbot.core.data_type.common import MarketDict, OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair

s_decimal_nan = Decimal("NaN")


class NeyXEMMTriangularConfig(StrategyV2ConfigBase):
    """
    XEMM with a triangulated taker leg.

    The maker exchange trades a direct pair (e.g. BTC-BRL on Binance).
    The taker exchange hedges via two real pairs that share an intermediate
    currency (e.g. BTC-USDT + USDT-BRL on Bybit), forming a synthetic BTC-BRL.

    USDT neutrality is maintained by:
      1. In-trade sizing: leg2 notional mirrors leg1 USDT amount.
      2. Periodic rebalancer: corrects residual drift every `usdt_rebalance_interval` ticks.
    """
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []

    maker_connector: str = Field("binance", json_schema_extra={
        "prompt": "Maker exchange (direct pair)", "prompt_on_new": True})
    maker_trading_pair: str = Field("BTC-BRL", json_schema_extra={
        "prompt": "Maker trading pair", "prompt_on_new": True})

    taker_connector: str = Field("bybit", json_schema_extra={
        "prompt": "Taker exchange (hosts both triangular legs)", "prompt_on_new": True})
    leg1_pair: str = Field("BTC-USDT", json_schema_extra={
        "prompt": "Taker leg 1: base-to-intermediate pair (e.g. BTC-USDT)", "prompt_on_new": True})
    leg2_pair: str = Field("USDT-BRL", json_schema_extra={
        "prompt": "Taker leg 2: intermediate-to-quote pair (e.g. USDT-BRL)", "prompt_on_new": True})

    order_amount: Decimal = Field(Decimal("0.0005"), json_schema_extra={
        "prompt": "Order amount in base asset (BTC)", "prompt_on_new": True})
    target_profitability: Decimal = Field(Decimal("0.001"), json_schema_extra={
        "prompt": "Target profitability (e.g. 0.001 = 0.1%)", "prompt_on_new": True})
    min_profitability: Decimal = Field(Decimal("0.0005"), json_schema_extra={
        "prompt": "Minimum profitability before cancelling", "prompt_on_new": True})
    max_order_age: int = Field(120, json_schema_extra={
        "prompt": "Max maker order age in seconds before refresh", "prompt_on_new": True})

    usdt_rebalance_target: Decimal = Field(Decimal("10"), json_schema_extra={
        "prompt": "Target USDT buffer on taker exchange (e.g. 10)", "prompt_on_new": True})
    usdt_rebalance_threshold: Decimal = Field(Decimal("2"), json_schema_extra={
        "prompt": "USDT drift tolerance before rebalancing (e.g. 2)", "prompt_on_new": True})
    usdt_rebalance_interval: int = Field(60, json_schema_extra={
        "prompt": "Ticks between USDT balance checks (default 60 = ~60s)", "prompt_on_new": True})

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.maker_connector] = markets.get(self.maker_connector, set()) | {self.maker_trading_pair}
        markets[self.taker_connector] = markets.get(self.taker_connector, set()) | {self.leg1_pair, self.leg2_pair}
        return markets


class NeyXEMMTriangular(StrategyV2Base):
    """
    Cross-exchange market making with a triangulated taker hedge.

    Maker side  → Binance BTC-BRL (real direct pair, LIMIT orders)
    Taker side  → Bybit BTC-USDT + Bybit USDT-BRL (synthetic BTC-BRL, MARKET orders)

    On each maker fill the bot fires TWO market orders on the taker exchange:
      - Maker BUY  filled → sell BTC-USDT + sell USDT-BRL  (net: +USDT-0 +BRL-BTC)
      - Maker SELL filled → buy  USDT-BRL + buy  BTC-USDT  (net: -BRL  +BTC, 0 USDT)

    A periodic rebalancer corrects residual USDT drift from slippage.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: NeyXEMMTriangularConfig):
        super().__init__(connectors, config)
        self.config = config
        self.active_buy_order_id = None
        self.active_sell_order_id = None
        self._rebalance_counter = 0

        self.synthetic = SyntheticTriangularPriceProvider(
            connectors[config.taker_connector],
            config.leg1_pair,
            config.leg2_pair,
        )

        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(connector_name=config.maker_connector, trading_pair=config.maker_trading_pair),
            ConnectorPair(connector_name=config.taker_connector, trading_pair=config.leg1_pair),
            ConnectorPair(connector_name=config.taker_connector, trading_pair=config.leg2_pair),
        ])

    # ------------------------------------------------------------------
    # Core strategy loop
    # ------------------------------------------------------------------

    def on_tick(self):
        taker_buy = self.synthetic.get_price_for_volume(True, self.config.order_amount)
        taker_sell = self.synthetic.get_price_for_volume(False, self.config.order_amount)

        buy_active = self._is_order_active(self.active_buy_order_id)
        sell_active = self._is_order_active(self.active_sell_order_id)

        maker = self.config.maker_connector
        maker_pair = self.config.maker_trading_pair

        # ── Maker BUY order ──────────────────────────────────────────────
        if not buy_active:
            self.active_buy_order_id = None
            maker_buy_price = taker_sell.result_price / (Decimal("1") + self.config.target_profitability)
            buy_amount = min(self.config.order_amount, self._buy_hedging_budget())
            if buy_amount > Decimal("0"):
                candidate = OrderCandidate(
                    trading_pair=maker_pair, is_maker=True,
                    order_type=OrderType.LIMIT, order_side=TradeType.BUY,
                    amount=buy_amount, price=maker_buy_price,
                )
                adjusted = self.connectors[maker].budget_checker.adjust_candidate(candidate, all_or_none=False)
                if adjusted.amount > Decimal("0"):
                    self.active_buy_order_id = self.buy(
                        maker, maker_pair, adjusted.amount, adjusted.order_type, adjusted.price)

        # ── Maker SELL order ─────────────────────────────────────────────
        if not sell_active:
            self.active_sell_order_id = None
            maker_sell_price = taker_buy.result_price / (Decimal("1") - self.config.target_profitability)
            sell_amount = min(self.config.order_amount, self._sell_hedging_budget())
            if sell_amount > Decimal("0"):
                candidate = OrderCandidate(
                    trading_pair=maker_pair, is_maker=True,
                    order_type=OrderType.LIMIT, order_side=TradeType.SELL,
                    amount=sell_amount, price=maker_sell_price,
                )
                adjusted = self.connectors[maker].budget_checker.adjust_candidate(candidate, all_or_none=False)
                if adjusted.amount > Decimal("0"):
                    self.active_sell_order_id = self.sell(
                        maker, maker_pair, adjusted.amount, adjusted.order_type, adjusted.price)

        # ── Cancel stale / unprofitable maker orders ──────────────────────
        for order in self.get_active_orders(connector_name=maker):
            if order.client_order_id not in (self.active_buy_order_id, self.active_sell_order_id):
                continue
            cancel_ts = order.creation_timestamp / 1_000_000 + self.config.max_order_age
            if order.is_buy:
                profit = (taker_sell.result_price - order.price) / order.price
                if profit < self.config.min_profitability or cancel_ts < self.current_timestamp:
                    self.logger().info(f"Cancelling buy {order.client_order_id} profit={profit:.4f}")
                    self.cancel(maker, order.trading_pair, order.client_order_id)
                    self.active_buy_order_id = None
            else:
                profit = (order.price - taker_buy.result_price) / order.price
                if profit < self.config.min_profitability or cancel_ts < self.current_timestamp:
                    self.logger().info(f"Cancelling sell {order.client_order_id} profit={profit:.4f}")
                    self.cancel(maker, order.trading_pair, order.client_order_id)
                    self.active_sell_order_id = None

        # ── USDT rebalancer ───────────────────────────────────────────────
        self._rebalance_counter += 1
        if self._rebalance_counter >= self.config.usdt_rebalance_interval:
            self._rebalance_counter = 0
            self._maybe_rebalance_usdt()

    # ------------------------------------------------------------------
    # Fill handler — fires two-legged hedge
    # ------------------------------------------------------------------

    def did_fill_order(self, event: OrderFilledEvent):
        if event.order_id == self.active_buy_order_id:
            self.logger().info(
                f"Maker BUY filled: {event.amount:.6f} BTC @ {event.price:.2f} BRL — hedging on taker"
            )
            self._place_triangular_sell_hedge(Decimal(str(event.amount)))
            self.cancel(self.config.maker_connector, self.config.maker_trading_pair, event.order_id)
            self.active_buy_order_id = None

        elif event.order_id == self.active_sell_order_id:
            self.logger().info(
                f"Maker SELL filled: {event.amount:.6f} BTC @ {event.price:.2f} BRL — hedging on taker"
            )
            self._place_triangular_buy_hedge(Decimal(str(event.amount)))
            self.cancel(self.config.maker_connector, self.config.maker_trading_pair, event.order_id)
            self.active_sell_order_id = None

    # ------------------------------------------------------------------
    # Two-legged hedge execution
    # ------------------------------------------------------------------

    def _place_triangular_sell_hedge(self, amount: Decimal):
        """
        Maker BUY filled → sell BTC on taker to neutralise inventory.

        leg1: SELL BTC-USDT  → receive USDT
        leg2: SELL USDT-BRL  → receive BRL   (net USDT ≈ 0)
        """
        taker = self.config.taker_connector
        leg1_result = self.connectors[taker].get_price_for_volume(self.config.leg1_pair, False, float(amount))
        p1 = Decimal(str(leg1_result.result_price))
        usdt_notional = amount * p1

        leg2_result = self.connectors[taker].get_price_for_volume(self.config.leg2_pair, False, float(usdt_notional))
        p2 = Decimal(str(leg2_result.result_price))

        self.sell(taker, self.config.leg1_pair, amount, OrderType.MARKET, p1)
        self.sell(taker, self.config.leg2_pair, usdt_notional, OrderType.MARKET, p2)
        self.logger().info(
            f"Triangular SELL hedge: {amount:.6f} BTC-USDT @ {p1:.4f}, "
            f"{usdt_notional:.4f} USDT-BRL @ {p2:.4f}"
        )

    def _place_triangular_buy_hedge(self, amount: Decimal):
        """
        Maker SELL filled → buy BTC on taker to neutralise inventory.

        leg2: BUY USDT-BRL   → spend BRL,  receive USDT
        leg1: BUY BTC-USDT   → spend USDT, receive BTC   (net USDT ≈ 0)

        leg2 fires first so USDT is available before leg1 settles.
        A small pre-funded USDT buffer (usdt_rebalance_target) covers the ms gap.
        """
        taker = self.config.taker_connector
        leg1_result = self.connectors[taker].get_price_for_volume(self.config.leg1_pair, True, float(amount))
        p1 = Decimal(str(leg1_result.result_price))
        usdt_notional = amount * p1

        leg2_result = self.connectors[taker].get_price_for_volume(self.config.leg2_pair, True, float(usdt_notional))
        p2 = Decimal(str(leg2_result.result_price))

        self.buy(taker, self.config.leg2_pair, usdt_notional, OrderType.MARKET, p2)
        self.buy(taker, self.config.leg1_pair, amount, OrderType.MARKET, p1)
        self.logger().info(
            f"Triangular BUY hedge: {usdt_notional:.4f} USDT-BRL @ {p2:.4f}, "
            f"{amount:.6f} BTC-USDT @ {p1:.4f}"
        )

    # ------------------------------------------------------------------
    # USDT rebalancer
    # ------------------------------------------------------------------

    def _maybe_rebalance_usdt(self):
        """
        Corrects residual USDT drift accumulated from slippage across cycles.
        Fires a single MARKET order on USDT-BRL to bring USDT back to target.
        """
        taker = self.config.taker_connector
        usdt_balance = Decimal(str(self.connectors[taker].get_available_balance("USDT")))
        drift = usdt_balance - self.config.usdt_rebalance_target

        if abs(drift) <= self.config.usdt_rebalance_threshold:
            return

        leg2_mid = Decimal(str(self.connectors[taker].get_mid_price(self.config.leg2_pair)))

        if drift > Decimal("0"):
            # Excess USDT → sell USDT for BRL
            self.sell(taker, self.config.leg2_pair, drift, OrderType.MARKET, leg2_mid)
            self.logger().info(f"USDT rebalance SELL {drift:.4f} USDT-BRL (balance={usdt_balance:.4f})")
        else:
            # Deficit USDT → buy USDT with BRL
            self.buy(taker, self.config.leg2_pair, abs(drift), OrderType.MARKET, leg2_mid)
            self.logger().info(f"USDT rebalance BUY {abs(drift):.4f} USDT-BRL (balance={usdt_balance:.4f})")

    # ------------------------------------------------------------------
    # Budget helpers
    # ------------------------------------------------------------------

    def _buy_hedging_budget(self) -> Decimal:
        """Available BTC on taker for sell-hedge (maker BUY fills → sell BTC on taker)."""
        base = self.config.leg1_pair.split("-")[0]
        return Decimal(str(self.connectors[self.config.taker_connector].get_available_balance(base)))

    def _sell_hedging_budget(self) -> Decimal:
        """Available BRL on taker converted to BTC (maker SELL fills → buy BTC on taker)."""
        quote = self.config.leg2_pair.split("-")[1]
        brl = Decimal(str(self.connectors[self.config.taker_connector].get_available_balance(quote)))
        synth = self.synthetic.get_price_for_volume(True, self.config.order_amount)
        if synth.result_price == Decimal("0"):
            return Decimal("0")
        return brl / synth.result_price

    # ------------------------------------------------------------------
    # Order tracking helper
    # ------------------------------------------------------------------

    def _is_order_active(self, order_id: str) -> bool:
        if order_id is None:
            return False
        for order in self.get_active_orders(connector_name=self.config.maker_connector):
            if order.client_order_id == order_id:
                return True
        return False

    # ------------------------------------------------------------------
    # Status display
    # ------------------------------------------------------------------

    def _exchanges_df(self) -> pd.DataFrame:
        maker = self.config.maker_connector
        taker = self.config.taker_connector
        amount = self.config.order_amount

        maker_mid = self.connectors[maker].get_mid_price(self.config.maker_trading_pair)
        maker_buy = self.connectors[maker].get_price_for_volume(self.config.maker_trading_pair, True, float(amount))
        maker_sell = self.connectors[maker].get_price_for_volume(self.config.maker_trading_pair, False, float(amount))

        synth_mid = self.synthetic.get_mid_price()
        synth_buy = self.synthetic.get_price_for_volume(True, amount)
        synth_sell = self.synthetic.get_price_for_volume(False, amount)

        data = [
            [maker, self.config.maker_trading_pair, float(maker_mid),
             float(maker_buy.result_price), float(maker_sell.result_price)],
            [f"{taker} (synthetic)", f"{self.config.leg1_pair}×{self.config.leg2_pair}",
             float(synth_mid), float(synth_buy.result_price), float(synth_sell.result_price)],
        ]
        return pd.DataFrame(data, columns=["Exchange", "Market", "Mid Price", "Buy Price", "Sell Price"])

    def _active_orders_df(self) -> pd.DataFrame:
        maker = self.config.maker_connector
        taker_buy = self.synthetic.get_price_for_volume(True, self.config.order_amount)
        taker_sell = self.synthetic.get_price_for_volume(False, self.config.order_amount)

        data = []
        for order in self.get_active_orders(connector_name=maker):
            age = "n/a" if order.age() <= 0 else pd.Timestamp(order.age(), unit="s").strftime("%H:%M:%S")
            if order.is_buy:
                profit = (taker_sell.result_price - order.price) / order.price * 100
            else:
                profit = (order.price - taker_buy.result_price) / order.price * 100
            data.append([
                maker, order.trading_pair,
                "buy" if order.is_buy else "sell",
                float(order.price), float(order.quantity),
                f"{float(profit):.3f}", f"{float(self.config.min_profitability * 100):.3f}", age,
            ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data, columns=["Exchange", "Market", "Side", "Price", "Amount",
                                         "Current Profit %", "Min Profit %", "Age"])
        df.sort_values(by=["Side"], inplace=True)
        return df

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []
        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + l for l in balance_df.to_string(index=False).split("\n")])

        try:
            ex_df = self._exchanges_df()
            lines.extend(["", "  Markets:"] + ["    " + l for l in ex_df.to_string(index=False).split("\n")])
        except Exception:
            lines.extend(["", "  Markets: (unavailable)"])

        taker = self.config.taker_connector
        usdt_balance = self.connectors[taker].get_available_balance("USDT")
        lines.append(f"\n  USDT buffer on {taker}: {float(usdt_balance):.4f} "
                     f"(target={float(self.config.usdt_rebalance_target):.1f})")

        try:
            orders_df = self._active_orders_df()
            lines.extend(["", "  Active Maker Orders:"] + ["    " + l for l in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)
