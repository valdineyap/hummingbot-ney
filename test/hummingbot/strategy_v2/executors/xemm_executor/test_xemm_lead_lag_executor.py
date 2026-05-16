"""Unit tests for XEMMLeadLagExecutor — LIMIT_MAKER usage and book-aware pricing."""
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import (
    XEMMLeadLagExecutorConfig,
)
from hummingbot.strategy_v2.executors.xemm_executor.xemm_lead_lag_executor import XEMMLeadLagExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

TICK = Decimal("1")
MIN_PROF = Decimal("0.0007")
PLACEMENT_BUF = Decimal("0.0002")
TAKER_PRICE = Decimal("385000")
TX_COST = Decimal("0.0009")


def _make_config(
    maker_side: TradeType = TradeType.BUY,
    min_prof: Decimal = MIN_PROF,
    target_prof: Decimal = Decimal("0.0012"),
    max_prof: Decimal = Decimal("0.0012"),
    buf: Decimal = PLACEMENT_BUF,
) -> XEMMLeadLagExecutorConfig:
    if maker_side == TradeType.BUY:
        buying = ConnectorPair(connector_name="bybit", trading_pair="BTC-BRL")
        selling = ConnectorPair(connector_name="binance", trading_pair="BTC-BRL")
    else:
        buying = ConnectorPair(connector_name="binance", trading_pair="BTC-BRL")
        selling = ConnectorPair(connector_name="bybit", trading_pair="BTC-BRL")
    return XEMMLeadLagExecutorConfig(
        timestamp=1234,
        buying_market=buying,
        selling_market=selling,
        maker_side=maker_side,
        order_amount=Decimal("0.001"),
        min_profitability=min_prof,
        target_profitability=target_prof,
        max_profitability=max_prof,
        placement_profitability_buffer=buf,
    )


def _make_rules(tick: Decimal = TICK) -> TradingRule:
    return TradingRule(
        trading_pair="BTC-BRL",
        min_order_size=Decimal("0.0001"),
        min_price_increment=tick,
        min_base_amount_increment=Decimal("0.0001"),
    )


class TestXEMMLeadLagExecutor(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self):
        super().setUp()
        self.strategy = self._create_mock_strategy()
        self.config = _make_config()
        self.executor = XEMMLeadLagExecutor(self.strategy, self.config, update_interval=0.5)
        # Per-placement balance revalidation (Opção 3a, 2026-05-14) added a
        # ``validate_sufficient_balance()`` call at the top of
        # ``create_maker_order``. These tests exercise price-calculation
        # paths and don't care about balance plumbing; mock the gate to a
        # no-op so they don't trip on unstubbed budget machinery. The gate
        # itself is covered by ``test_xemm_revalidate_balance.py``.
        self.executor.validate_sufficient_balance = AsyncMock()
        self.set_loggers(loggers=[self.executor.logger()])

    @staticmethod
    def _create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market
        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="BTC-BRL")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2"]
        strategy.cancel.return_value = None
        bybit_connector = MagicMock(spec=ExchangePyBase)
        bybit_connector.supported_order_types = MagicMock(
            return_value=[OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET])
        binance_connector = MagicMock(spec=ExchangePyBase)
        binance_connector.supported_order_types = MagicMock(
            return_value=[OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET])
        strategy.connectors = {"bybit": bybit_connector, "binance": binance_connector}
        return strategy

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _setup_book(
        self, executor: XEMMLeadLagExecutor,
        best_bid: Decimal, best_ask: Decimal,
        tick: Decimal = TICK,
        taker_price: Decimal = TAKER_PRICE,
        tx_cost: Decimal = TX_COST,
    ):
        """Patch get_trading_rules and get_price on the given executor."""
        rules = _make_rules(tick)
        executor.get_trading_rules = MagicMock(return_value=rules)
        executor._taker_result_price = taker_price
        executor._tx_cost_pct = tx_cost

        def _get_price(connector, pair, price_type=PriceType.MidPrice):
            if price_type == PriceType.BestBid:
                return best_bid
            if price_type == PriceType.BestAsk:
                return best_ask
            return (best_bid + best_ask) / 2

        executor.get_price = MagicMock(side_effect=_get_price)

    # ------------------------------------------------------------------ #
    # Original LIMIT_MAKER tests (updated to use XEMMLeadLagExecutorConfig)
    # ------------------------------------------------------------------ #

    async def test_create_maker_order_uses_limit_maker_order_type(self):
        """place_order must always be called with LIMIT_MAKER."""
        self.executor._maker_target_price = Decimal("300000")
        with patch.object(self.executor, "place_order", return_value="OID-1") as mock_place:
            await self.executor.create_maker_order()
        call_kwargs = mock_place.call_args.kwargs
        self.assertEqual(call_kwargs["order_type"], OrderType.LIMIT_MAKER)
        self.assertIsNotNone(self.executor.maker_order)
        self.assertEqual(self.executor.maker_order.order_id, "OID-1")

    async def test_create_maker_order_buy_connector_and_side(self):
        """BUY maker: connector=bybit, side=BUY."""
        self.executor._maker_target_price = Decimal("300000")
        with patch.object(self.executor, "place_order", return_value="OID-B") as mock_place:
            await self.executor.create_maker_order()
        call_kwargs = mock_place.call_args.kwargs
        self.assertEqual(call_kwargs["connector_name"], "bybit")
        self.assertEqual(call_kwargs["side"], TradeType.BUY)

    async def test_create_maker_order_sell_connector_and_side(self):
        """SELL maker: connector=bybit, side=SELL."""
        sell_config = _make_config(maker_side=TradeType.SELL)
        executor = XEMMLeadLagExecutor(self.strategy, sell_config, update_interval=0.5)
        executor._maker_target_price = Decimal("300100")
        with patch.object(executor, "place_order", return_value="OID-S") as mock_place:
            await executor.create_maker_order()
        call_kwargs = mock_place.call_args.kwargs
        self.assertEqual(call_kwargs["order_type"], OrderType.LIMIT_MAKER)
        self.assertEqual(call_kwargs["side"], TradeType.SELL)
        self.assertEqual(call_kwargs["connector_name"], "bybit")

    # ------------------------------------------------------------------ #
    # Book-aware pricing: BUY side                                         #
    # ------------------------------------------------------------------ #

    async def test_buy_improves_when_spread_wide(self):
        """BUY with spread > 1 tick → competitive_price = best_bid + tick (capped by bound)."""
        # Book inside the cancel band so improve isn't suppressed.
        max_buy_price = TAKER_PRICE / (1 + Decimal("0.0009") + TX_COST)
        best_bid = (max_buy_price - Decimal("20")).to_integral_value()
        best_ask = best_bid + Decimal("10")  # spread = 10 ticks → improve
        self._setup_book(self.executor, best_bid, best_ask, tick=TICK, taker_price=TAKER_PRICE)
        self.executor._maker_target_price = TAKER_PRICE / (1 + Decimal("0.0012") + TX_COST)

        with patch.object(self.executor, "place_order", return_value="OID-IMPROVE") as mock_place:
            await self.executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        # Must be AT or BELOW best_bid + tick (capped by max_buy_price)
        self.assertLessEqual(placed_price, best_bid + TICK)
        # Must be above zero and less than best_ask
        self.assertGreater(placed_price, 0)
        self.assertLess(placed_price, best_ask)

    async def test_buy_joins_when_spread_is_one_tick(self):
        """BUY with spread = 1 tick → join at best_bid (no improve)."""
        # Book inside the cancel band.
        max_buy_price = TAKER_PRICE / (1 + Decimal("0.0009") + TX_COST)
        best_bid = (max_buy_price - Decimal("5")).to_integral_value()
        best_ask = best_bid + TICK  # spread = 1 tick → join
        self._setup_book(self.executor, best_bid, best_ask)
        self.executor._maker_target_price = TAKER_PRICE / (1 + Decimal("0.0012") + TX_COST)

        with patch.object(self.executor, "place_order", return_value="OID-JOIN") as mock_place:
            await self.executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        # Must not exceed best_bid (cannot improve), must be < best_ask
        self.assertLessEqual(placed_price, best_bid)
        self.assertLess(placed_price, best_ask)

    async def test_buy_capped_by_max_buy_price(self):
        """BUY: if competitive_price > max_buy_price, use max_buy_price."""
        # max_buy_price = taker / (1 + effective_min + tx_cost)
        effective_min = MIN_PROF + PLACEMENT_BUF  # 0.0009
        max_buy_price = TAKER_PRICE / (1 + effective_min + TX_COST)
        # Place best_bid well above max_buy_price to force the cap
        best_bid = max_buy_price + Decimal("100")
        best_ask = best_bid + Decimal("100")
        self._setup_book(self.executor, best_bid, best_ask)
        self.executor._maker_target_price = TAKER_PRICE / (1 + Decimal("0.0012") + TX_COST)

        with patch.object(self.executor, "place_order", return_value="OID-CAP") as mock_place:
            await self.executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        self.assertLessEqual(placed_price, max_buy_price)
        self.assertLess(placed_price, best_ask)

    async def test_buy_rounds_down(self):
        """BUY price must be rounded DOWN to the nearest tick."""
        tick = Decimal("1")
        # Book inside the cancel band so the improve placement is exercised.
        max_buy_price = TAKER_PRICE / (1 + Decimal("0.0009") + TX_COST)
        best_bid = (max_buy_price - Decimal("20")) + Decimal("0.7")  # not on a tick boundary
        best_ask = best_bid + Decimal("10")
        self._setup_book(self.executor, best_bid, best_ask, tick=tick)
        self.executor._maker_target_price = TAKER_PRICE / (1 + Decimal("0.0012") + TX_COST)

        with patch.object(self.executor, "place_order", return_value="OID-RDN") as mock_place:
            await self.executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        remainder = placed_price % tick
        self.assertEqual(remainder, Decimal("0"), f"Price {placed_price} not aligned to tick {tick}")

    # ------------------------------------------------------------------ #
    # Book-aware pricing: SELL side                                        #
    # ------------------------------------------------------------------ #

    async def test_sell_improves_when_spread_wide(self):
        """SELL with spread > 1 tick → competitive_price = best_ask - tick (floored by bound)."""
        sell_config = _make_config(maker_side=TradeType.SELL)
        executor = XEMMLeadLagExecutor(self.strategy, sell_config, update_interval=0.5)
        best_bid = Decimal("383000")
        best_ask = Decimal("383010")
        self._setup_book(executor, best_bid, best_ask)
        executor._maker_target_price = TAKER_PRICE / (1 - Decimal("0.0012") - TX_COST)

        with patch.object(executor, "place_order", return_value="OID-SELL-IMP") as mock_place:
            await executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        self.assertGreaterEqual(placed_price, best_ask - TICK)
        self.assertGreater(placed_price, best_bid)

    async def test_sell_joins_when_spread_is_one_tick(self):
        """SELL with spread = 1 tick → join at best_ask (no improve)."""
        sell_config = _make_config(maker_side=TradeType.SELL)
        executor = XEMMLeadLagExecutor(self.strategy, sell_config, update_interval=0.5)
        best_bid = Decimal("383000")
        best_ask = Decimal("383001")
        self._setup_book(executor, best_bid, best_ask)
        executor._maker_target_price = TAKER_PRICE / (1 - Decimal("0.0012") - TX_COST)

        with patch.object(executor, "place_order", return_value="OID-SELL-JOIN") as mock_place:
            await executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        self.assertGreaterEqual(placed_price, best_ask)
        self.assertGreater(placed_price, best_bid)

    async def test_sell_floored_by_min_sell_price(self):
        """SELL: if competitive_price < min_sell_price, use min_sell_price."""
        sell_config = _make_config(maker_side=TradeType.SELL)
        executor = XEMMLeadLagExecutor(self.strategy, sell_config, update_interval=0.5)
        effective_min = MIN_PROF + PLACEMENT_BUF
        min_sell_price = TAKER_PRICE / (1 - effective_min - TX_COST)
        # Place best_ask well below min_sell_price to force the floor
        best_ask = min_sell_price - Decimal("200")
        best_bid = best_ask - Decimal("10")
        self._setup_book(executor, best_bid, best_ask)
        executor._maker_target_price = TAKER_PRICE / (1 - Decimal("0.0012") - TX_COST)

        with patch.object(executor, "place_order", return_value="OID-SELL-FLR") as mock_place:
            await executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        self.assertGreaterEqual(placed_price, min_sell_price)

    # ------------------------------------------------------------------ #
    # Cancel-band clip (avoids 1-tick lifetime due to placement above max
    # profitability — see analysis on 2026-05-08).                        #
    # ------------------------------------------------------------------ #

    async def test_buy_clipped_to_min_buy_price_when_book_too_cheap(self):
        """BUY: if competitive_price < min_buy_price (i.e., placement would
        yield profitability > max_profitability and trigger an instant
        refresh), clip up to min_buy_price.

        After ROUND_DOWN the clip_max guard nudges up by one tick if the
        rounded price fell below min_buy_price, so the final price must
        always be >= min_buy_price (never triggering an instant cancel)."""
        # max_prof distinct from target_prof so the clip band is non-empty.
        cfg = _make_config(
            maker_side=TradeType.BUY,
            min_prof=Decimal("0.0002"),
            target_prof=Decimal("0.0003"),
            max_prof=Decimal("0.0005"),
            buf=Decimal("0.0002"),
        )
        executor = XEMMLeadLagExecutor(self.strategy, cfg, update_interval=0.5)
        # min_buy_price corresponds to max_profitability (cheapest we'd buy).
        min_buy_price = TAKER_PRICE / (1 + Decimal("0.0005") + TX_COST)
        # Place the book around min_buy_price so that improve (best_bid+tick)
        # falls below the cap (triggers clip) and best_ask sits above the cap
        # (so the post-rounding "don't cross book" safety doesn't pull us back).
        best_bid = min_buy_price - Decimal("10")
        best_ask = min_buy_price + Decimal("10")
        self._setup_book(executor, best_bid, best_ask)
        executor._maker_target_price = TAKER_PRICE / (1 + Decimal("0.0003") + TX_COST)

        with patch.object(executor, "place_order", return_value="OID-CLIP-B") as mock_place:
            await executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        # clip_max guard ensures price >= min_buy_price (no instant cancel).
        self.assertGreaterEqual(placed_price, min_buy_price)
        # Must not exceed max_buy_price (the floor side of the band).
        max_buy_price = TAKER_PRICE / (1 + Decimal("0.0004") + TX_COST)
        self.assertLessEqual(placed_price, max_buy_price)

    async def test_buy_skips_when_book_above_band(self):
        """BUY: if the local ask is above min_buy_price (no maker price exists
        that satisfies both 'doesn't cross ask' and 'doesn't trigger
        max_profitability cancel'), placement must be skipped."""
        cfg = _make_config(
            maker_side=TradeType.BUY,
            min_prof=Decimal("0.0002"),
            target_prof=Decimal("0.0003"),
            max_prof=Decimal("0.0005"),
            buf=Decimal("0.0002"),
        )
        executor = XEMMLeadLagExecutor(self.strategy, cfg, update_interval=0.5)
        min_buy_price = TAKER_PRICE / (1 + Decimal("0.0005") + TX_COST)
        # best_ask BELOW min_buy_price → no valid maker BUY (would cross
        # but be too profitable).
        best_ask = min_buy_price - Decimal("10")
        best_bid = best_ask - Decimal("1")
        self._setup_book(executor, best_bid, best_ask)
        executor._maker_target_price = TAKER_PRICE / (1 + Decimal("0.0003") + TX_COST)

        with patch.object(executor, "place_order", return_value="OID-SKIP-B") as mock_place:
            await executor.create_maker_order()

        mock_place.assert_not_called()
        self.assertIsNone(executor.maker_order)

    async def test_sell_clipped_to_max_sell_price_when_book_too_rich(self):
        """SELL: if competitive_price > max_sell_price (i.e., placement would
        yield profitability > max_profitability and trigger an instant
        refresh), clip down to max_sell_price.

        After ROUND_UP the clip_max guard nudges down by one tick if the
        rounded price rose above max_sell_price, so the final price must
        always be <= max_sell_price (never triggering an instant cancel)."""
        cfg = _make_config(
            maker_side=TradeType.SELL,
            min_prof=Decimal("0.0002"),
            target_prof=Decimal("0.0003"),
            max_prof=Decimal("0.0005"),
            buf=Decimal("0.0002"),
        )
        executor = XEMMLeadLagExecutor(self.strategy, cfg, update_interval=0.5)
        # max_sell_price corresponds to max_profitability (richest we'd sell).
        max_sell_price = TAKER_PRICE / (1 - Decimal("0.0005") - TX_COST)
        # Place the book around max_sell_price so that improve (best_ask-tick)
        # falls above the cap (triggers clip) and best_bid sits below the cap
        # (so the post-rounding "don't cross book" safety doesn't push us up).
        best_ask = max_sell_price + Decimal("10")
        best_bid = max_sell_price - Decimal("10")
        self._setup_book(executor, best_bid, best_ask)
        executor._maker_target_price = TAKER_PRICE / (1 - Decimal("0.0003") - TX_COST)

        with patch.object(executor, "place_order", return_value="OID-CLIP-S") as mock_place:
            await executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        # clip_max guard ensures price <= max_sell_price (no instant cancel).
        self.assertLessEqual(placed_price, max_sell_price)
        # Must not fall below min_sell_price (the floor side of the band).
        min_sell_price = TAKER_PRICE / (1 - Decimal("0.0004") - TX_COST)
        self.assertGreaterEqual(placed_price, min_sell_price)

    async def test_sell_skips_when_book_below_band(self):
        """SELL: if the local bid is above max_sell_price (no maker SELL price
        that satisfies both 'doesn't cross bid' and 'doesn't trigger
        max_profitability cancel'), placement must be skipped."""
        cfg = _make_config(
            maker_side=TradeType.SELL,
            min_prof=Decimal("0.0002"),
            target_prof=Decimal("0.0003"),
            max_prof=Decimal("0.0005"),
            buf=Decimal("0.0002"),
        )
        executor = XEMMLeadLagExecutor(self.strategy, cfg, update_interval=0.5)
        max_sell_price = TAKER_PRICE / (1 - Decimal("0.0005") - TX_COST)
        # best_bid ABOVE max_sell_price → no valid maker SELL.
        best_bid = max_sell_price + Decimal("10")
        best_ask = best_bid + Decimal("1")
        self._setup_book(executor, best_bid, best_ask)
        executor._maker_target_price = TAKER_PRICE / (1 - Decimal("0.0003") - TX_COST)

        with patch.object(executor, "place_order", return_value="OID-SKIP-S") as mock_place:
            await executor.create_maker_order()

        mock_place.assert_not_called()
        self.assertIsNone(executor.maker_order)

    async def test_sell_rounds_up(self):
        """SELL price must be rounded UP to the nearest tick."""
        sell_config = _make_config(maker_side=TradeType.SELL)
        executor = XEMMLeadLagExecutor(self.strategy, sell_config, update_interval=0.5)
        tick = Decimal("1")
        best_ask = Decimal("383009.3")  # not on a tick boundary
        best_bid = Decimal("383000")
        self._setup_book(executor, best_bid, best_ask, tick=tick)
        executor._maker_target_price = TAKER_PRICE / (1 - Decimal("0.0012") - TX_COST)

        with patch.object(executor, "place_order", return_value="OID-RUP") as mock_place:
            await executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        remainder = placed_price % tick
        self.assertEqual(remainder, Decimal("0"), f"Price {placed_price} not aligned to tick {tick}")

    # ------------------------------------------------------------------ #
    # Fallback behaviour                                                   #
    # ------------------------------------------------------------------ #

    async def test_fallback_on_invalid_book_bid_gte_ask(self):
        """bid >= ask → fallback to _maker_target_price."""
        target = Decimal("382500")
        self._setup_book(self.executor, best_bid=Decimal("385000"), best_ask=Decimal("383000"))
        self.executor._maker_target_price = target

        with patch.object(self.executor, "place_order", return_value="OID-FB") as mock_place:
            await self.executor.create_maker_order()

        self.assertEqual(mock_place.call_args.kwargs["price"], target)
        self.assertEqual(mock_place.call_args.kwargs["order_type"], OrderType.LIMIT_MAKER)

    async def test_fallback_does_not_raise_in_log(self):
        """When fallback is used, the log line must not raise (bound_price=None is safe)."""
        target = Decimal("382500")
        self.executor._maker_target_price = target
        # NaN bid triggers fallback
        self._setup_book(self.executor, best_bid=Decimal("nan"), best_ask=Decimal("383000"))

        try:
            with patch.object(self.executor, "place_order", return_value="OID-SAFE"):
                await self.executor.create_maker_order()
        except Exception as exc:
            self.fail(f"create_maker_order raised unexpectedly with fallback: {exc}")

    async def test_buy_does_not_cross_ask_after_rounding(self):
        """After rounding, placed BUY price must be strictly < best_ask.

        Book sits inside the cancel band [min_buy_price, max_buy_price] with a
        1-tick spread so the post-rounding safety would matter if it were ever
        about to cross.
        """
        # Default config: effective_min = 0.0009, max_prof = 0.0012.
        max_buy_price = TAKER_PRICE / (1 + Decimal("0.0009") + TX_COST)
        best_bid = (max_buy_price - Decimal("5")).to_integral_value()
        best_ask = best_bid + TICK  # spread = 1 tick → join
        self._setup_book(self.executor, best_bid, best_ask, tick=TICK)
        self.executor._maker_target_price = TAKER_PRICE / (1 + Decimal("0.0012") + TX_COST)

        with patch.object(self.executor, "place_order", return_value="OID-NOCROSS") as mock_place:
            await self.executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        self.assertLess(placed_price, best_ask)

    async def test_sell_does_not_cross_bid_after_rounding(self):
        """After rounding, placed SELL price must be strictly > best_bid.

        Book sits inside the cancel band [min_sell_price, max_sell_price] with
        a 1-tick spread.
        """
        sell_config = _make_config(maker_side=TradeType.SELL)
        executor = XEMMLeadLagExecutor(self.strategy, sell_config, update_interval=0.5)
        # Default config: effective_min = 0.0009, max_prof = 0.0012.
        min_sell_price = TAKER_PRICE / (1 - Decimal("0.0009") - TX_COST)
        best_bid = (min_sell_price + Decimal("5")).to_integral_value()
        best_ask = best_bid + TICK
        self._setup_book(executor, best_bid, best_ask, tick=TICK)
        executor._maker_target_price = TAKER_PRICE / (1 - Decimal("0.0012") - TX_COST)

        with patch.object(executor, "place_order", return_value="OID-NOCROSS-S") as mock_place:
            await executor.create_maker_order()

        placed_price = mock_place.call_args.kwargs["price"]
        self.assertGreater(placed_price, best_bid)

    # ------------------------------------------------------------------ #
    # validate_sufficient_balance                                          #
    # ------------------------------------------------------------------ #

    @patch.object(XEMMLeadLagExecutor, "get_trading_rules")
    @patch.object(XEMMLeadLagExecutor, "adjust_order_candidates")
    async def test_validate_sufficient_balance_uses_limit_maker_for_maker_candidate(
        self, mock_adjust, mock_get_rules,
    ):
        """maker_order_candidate inside validate_sufficient_balance must be LIMIT_MAKER."""
        # Undo setUp's instance-level patch so we exercise the real method.
        del self.executor.validate_sufficient_balance
        mock_get_rules.return_value = _make_rules()
        captured = []

        def capture_side_effect(connector_name, candidates):
            captured.append((connector_name, candidates))
            return candidates

        mock_adjust.side_effect = capture_side_effect
        await self.executor.validate_sufficient_balance()

        self.assertEqual(len(captured), 2)
        maker_call = captured[0]
        self.assertEqual(maker_call[0], "bybit")
        maker_candidate: OrderCandidate = maker_call[1][0]
        self.assertEqual(maker_candidate.order_type, OrderType.LIMIT_MAKER)
        self.assertTrue(maker_candidate.is_maker)
        taker_call = captured[1]
        self.assertEqual(taker_call[0], "binance")
        taker_candidate: OrderCandidate = taker_call[1][0]
        self.assertEqual(taker_candidate.order_type, OrderType.MARKET)
        self.assertFalse(taker_candidate.is_maker)

    @patch.object(XEMMLeadLagExecutor, "get_trading_rules")
    @patch.object(XEMMLeadLagExecutor, "adjust_order_candidates")
    async def test_validate_sufficient_balance_marks_insufficient(
        self, mock_adjust, mock_get_rules,
    ):
        # Undo setUp's instance-level patch so we exercise the real method.
        del self.executor.validate_sufficient_balance
        mock_get_rules.return_value = _make_rules()
        zero_candidate = OrderCandidate(
            trading_pair="BTC-BRL",
            is_maker=True,
            order_type=OrderType.LIMIT_MAKER,
            order_side=TradeType.BUY,
            amount=Decimal("0"),
            price=Decimal("300000"),
        )
        mock_adjust.return_value = [zero_candidate]

        await self.executor.validate_sufficient_balance()

        self.assertEqual(self.executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        self.assertEqual(self.executor.status, RunnableStatus.TERMINATED)

    def test_inherits_xemm_lifecycle_attributes(self):
        """XEMMLeadLagExecutor is a true XEMMExecutor (instance & method inheritance)."""
        from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
        self.assertIsInstance(self.executor, XEMMExecutor)
        self.assertEqual(self.executor.maker_connector, "bybit")
        self.assertEqual(self.executor.taker_connector, "binance")
        self.assertEqual(self.executor.maker_order_side, TradeType.BUY)
        self.assertEqual(self.executor.taker_order_side, TradeType.SELL)

    # ------------------------------------------------------------------ #
    # Reconcile-hedge tests (REST-reconciled fill before event arrives)    #
    # ------------------------------------------------------------------ #

    async def test_reconcile_hedge_fires_on_maker_done_with_executed(self):
        """When control_maker_order runs and finds maker.is_done with
        executed > 0 and taker not yet placed, it must fire the taker hedge
        immediately — before the base class clears self.maker_order."""
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        from hummingbot.strategy_v2.models.base import RunnableStatus
        from unittest.mock import MagicMock, patch

        maker_order_id = "MAKER-RECONCILED-1"
        self.executor.maker_order = TrackedOrder(order_id=maker_order_id)
        # Simulate REST-reconciled fill: order.is_done=True, executed > 0,
        # but no OrderCompletedEvent has fired yet.
        mock_order = MagicMock()
        mock_order.is_done = True
        mock_order.executed_amount_base = Decimal("0.0002")
        self.executor.maker_order.order = mock_order
        self.executor.taker_order = None

        placed = []

        def _place(connector_name, trading_pair, order_type, side, amount, **kw):
            placed.append((connector_name, side.name, amount))
            return f"TAKER-{len(placed)}"

        with patch.object(self.executor, "place_order", side_effect=_place), \
             patch.object(self.executor, "_touch_controller_last_fill_time") as mock_touch, \
             patch.object(self.executor.__class__.__bases__[0], "control_maker_order",
                          new=AsyncMock()):
            await self.executor.control_maker_order()

        # Hedge placed on taker connector with the correct amount
        self.assertEqual(len(placed), 1)
        connector, side_name, amount = placed[0]
        self.assertEqual(connector, "binance")
        self.assertEqual(side_name, "SELL")
        self.assertEqual(amount, Decimal("0.0002"))
        # Order id registered in dedup set so the delayed completed-event no-ops
        self.assertIn(maker_order_id, self.executor._hedged_maker_order_ids)
        # Audit suppression window activated
        mock_touch.assert_called_once()
        # Status transitioned to SHUTTING_DOWN (matches base flow after hedge)
        self.assertEqual(self.executor._status, RunnableStatus.SHUTTING_DOWN)

    async def test_reconcile_hedge_skipped_when_taker_already_placed(self):
        """If taker_order is already set, the reconcile_hedge path must NOT
        place a second hedge (anti-double-hedge guard)."""
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        from unittest.mock import MagicMock, patch

        self.executor.maker_order = TrackedOrder(order_id="MAKER-RECONCILED-2")
        mock_order = MagicMock()
        mock_order.is_done = True
        mock_order.executed_amount_base = Decimal("0.0002")
        self.executor.maker_order.order = mock_order
        self.executor.taker_order = TrackedOrder(order_id="TAKER-ALREADY")

        with patch.object(self.executor, "place_order") as mock_place, \
             patch.object(self.executor.__class__.__bases__[0], "control_maker_order",
                          new=AsyncMock()):
            await self.executor.control_maker_order()

        mock_place.assert_not_called()

    async def test_reconcile_hedge_skipped_when_executed_zero(self):
        """If maker is done but executed == 0 (pure cancel, no fill), the
        reconcile_hedge path must NOT place a hedge."""
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        from unittest.mock import MagicMock, patch

        self.executor.maker_order = TrackedOrder(order_id="MAKER-RECONCILED-3")
        mock_order = MagicMock()
        mock_order.is_done = True
        mock_order.executed_amount_base = Decimal("0")
        self.executor.maker_order.order = mock_order
        self.executor.taker_order = None

        with patch.object(self.executor, "place_order") as mock_place, \
             patch.object(self.executor.__class__.__bases__[0], "control_maker_order",
                          new=AsyncMock()):
            await self.executor.control_maker_order()

        mock_place.assert_not_called()

    async def test_reconcile_hedge_delegates_to_base_when_no_fill(self):
        """control_maker_order must still call super().control_maker_order()
        when there's no reconcile-fill to handle (normal placement path)."""
        from unittest.mock import patch

        self.executor.maker_order = None  # nothing to reconcile
        self.executor.taker_order = None

        with patch.object(self.executor.__class__.__bases__[0], "control_maker_order",
                          new=AsyncMock()) as mock_base:
            await self.executor.control_maker_order()

        mock_base.assert_awaited_once()

    # ------------------------------------------------------------------ #
    # Reconcile-hedge anti-double-hedge on delayed completed event       #
    # ------------------------------------------------------------------ #

    def test_completed_event_dedup_after_reconcile_hedge(self):
        """Regression: when the reconcile-hedge path has already placed the
        taker for a maker order and registered the id in
        ``_hedged_maker_order_ids``, the delayed
        ``BuyOrderCompletedEvent`` / ``SellOrderCompletedEvent`` must NOT
        trigger a duplicate ``place_taker_order()`` via the base handler."""
        from hummingbot.strategy_v2.models.executors import TrackedOrder

        order_id = "MAKER-RECONCILED"
        self.executor._hedged_maker_order_ids.add(order_id)
        # Reconcile path already placed taker_order.
        self.executor.taker_order = TrackedOrder(order_id="TAKER-RECONCILE-HEDGE")

        event = MagicMock()
        event.order_id = order_id
        event.base_asset_amount = Decimal("0.0002")

        # Spy on the base class' method to assert it is NOT called.
        with patch.object(
            self.executor.__class__.__bases__[0],
            "process_order_completed_event",
        ) as mock_base:
            self.executor.process_order_completed_event(0, MagicMock(), event)

        mock_base.assert_not_called()
        # The id is consumed once the dedup hits.
        self.assertNotIn(order_id, self.executor._hedged_maker_order_ids)
        # Executor transitions to SHUTTING_DOWN so the cycle still closes.
        self.assertEqual(self.executor._status, RunnableStatus.SHUTTING_DOWN)

    def test_completed_event_falls_through_to_base_when_not_in_dedup_set(self):
        """For a normal completed event (no reconcile-hedge race), the base
        handler must run as usual."""
        event = MagicMock()
        event.order_id = "MAKER-NORMAL"

        with patch.object(
            self.executor.__class__.__bases__[0],
            "process_order_completed_event",
        ) as mock_base:
            self.executor.process_order_completed_event(0, MagicMock(), event)

        mock_base.assert_called_once()
