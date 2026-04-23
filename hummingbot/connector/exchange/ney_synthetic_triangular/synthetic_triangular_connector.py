from dataclasses import dataclass
from decimal import Decimal


@dataclass
class SyntheticPriceResult:
    """Minimal result object compatible with ConnectorBase.get_price_for_volume return type."""
    result_price: Decimal
    result_volume: Decimal = Decimal("0")


class SyntheticTriangularPriceProvider:
    """
    Computes synthetic prices for a virtual pair derived from two real pairs on the same
    exchange connector via a shared intermediate currency.

    Example: synthetic BTC-BRL from leg1=BTC-USDT and leg2=USDT-BRL on Bybit.

    Price formula:
      - BUY  synthetic (want BTC, pay BRL): P_synth = P(BTC-USDT_ask) × P(USDT-BRL_ask)
      - SELL synthetic (have BTC, get BRL): P_synth = P(BTC-USDT_bid) × P(USDT-BRL_bid)

    USDT exposure:
      Each hedge cycle fires two MARKET orders. The sizing of leg2 mirrors the USDT
      notional of leg1, so the net USDT change per cycle ≈ 0 (residual drift from
      slippage is handled by the periodic rebalancer in the strategy script).
    """

    def __init__(self, connector, leg1_pair: str, leg2_pair: str) -> None:
        """
        :param connector:  Live ConnectorBase for the taker exchange (e.g. Bybit).
        :param leg1_pair:  First leg pair, base → intermediate (e.g. "BTC-USDT").
        :param leg2_pair:  Second leg pair, intermediate → quote (e.g. "USDT-BRL").
        """
        self._connector = connector
        self._leg1 = leg1_pair
        self._leg2 = leg2_pair

    # ------------------------------------------------------------------
    # Public API — mirrors the ConnectorBase methods used by the script
    # ------------------------------------------------------------------

    def get_price_for_volume(self, is_buy: bool, volume: Decimal) -> SyntheticPriceResult:
        """
        Effective synthetic price to trade `volume` base units (BTC).

        For BUY (want BTC with BRL):
          1. Get ask price P1 for BTC-USDT (cost in USDT per BTC).
          2. Compute USDT notional = volume × P1.
          3. Get ask price P2 for USDT-BRL (cost in BRL per USDT).
          4. Synthetic ask = P1 × P2  (BRL per BTC).

        For SELL (have BTC, receive BRL):
          Same logic using bid prices: synthetic bid = P1 × P2.
        """
        leg1 = self._connector.get_price_for_volume(self._leg1, is_buy, float(volume))
        p1 = Decimal(str(leg1.result_price))
        usdt_notional = volume * p1
        leg2 = self._connector.get_price_for_volume(self._leg2, is_buy, float(usdt_notional))
        p2 = Decimal(str(leg2.result_price))
        return SyntheticPriceResult(result_price=p1 * p2, result_volume=volume)

    def get_mid_price(self) -> Decimal:
        """Synthetic mid price: mid(BTC-USDT) × mid(USDT-BRL)."""
        mid1 = self._connector.get_mid_price(self._leg1)
        mid2 = self._connector.get_mid_price(self._leg2)
        return Decimal(str(mid1)) * Decimal(str(mid2))

    @property
    def leg1_pair(self) -> str:
        return self._leg1

    @property
    def leg2_pair(self) -> str:
        return self._leg2
