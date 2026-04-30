"""
Lead-lag signal provider for cross-market trading strategies.

Pure-Python module with no Hummingbot dependencies. Computes a synthetic fair
price from a leader pair (e.g. BTC-USDT) and an FX leg (e.g. USDT-BRL), and
exposes lead-lag signals comparing returns of the synthetic fair against a
local market (e.g. BTC-BRL on a different exchange).

The synthetic fair is computed two ways:
  - fair_brl_fast = leader_mid * fx_mid_raw  (used for micro lead-lag signals)
  - fair_brl_slow = leader_mid * EMA(fx_mid_raw)  (used for regime/basis)

Separating these avoids the EMA on FX introducing artificial lag in the lead
signal (which would create false micro signals).
"""
import math
from collections import deque
from decimal import Decimal
from enum import Enum
from typing import Deque, List, Optional, Tuple


class SignalQuality(str, Enum):
    OK = "OK"
    DEGRADED_FX = "DEGRADED_FX"
    DEGRADED_LEADER = "DEGRADED_LEADER"
    DEGRADED_LOCAL = "DEGRADED_LOCAL"
    BAD = "BAD"


class CircularPriceBuffer:
    """
    FIFO time-series buffer of (timestamp, price) tuples. Entries strictly
    older than `max_duration_sec` from the latest pushed timestamp are evicted
    on each push. Lookup `get_price_at_or_before` returns the most recent
    price at or before a given timestamp.

    Boundary: an entry exactly at `now - max_duration_sec` is retained (the
    eviction predicate is strict `<`, not `<=`).
    """

    def __init__(self, max_duration_sec: float = 60.0):
        if max_duration_sec <= 0:
            raise ValueError("max_duration_sec must be positive")
        self._max_duration = float(max_duration_sec)
        self._data: Deque[Tuple[float, Decimal]] = deque()

    def push(self, timestamp: float, price: Decimal) -> None:
        self._data.append((float(timestamp), price))
        cutoff = float(timestamp) - self._max_duration
        while self._data and self._data[0][0] < cutoff:
            self._data.popleft()

    def get_price_at_or_before(self, timestamp: float) -> Optional[Decimal]:
        target = float(timestamp)
        result: Optional[Decimal] = None
        for ts, price in self._data:
            if ts <= target:
                result = price
            else:
                break
        return result

    def latest_price(self) -> Optional[Decimal]:
        return self._data[-1][1] if self._data else None

    def latest_timestamp(self) -> Optional[float]:
        return self._data[-1][0] if self._data else None

    def oldest_timestamp(self) -> Optional[float]:
        return self._data[0][0] if self._data else None

    def __len__(self) -> int:
        return len(self._data)


class EMAFilter:
    """
    Exponential Moving Average for Decimal values.
    value = alpha * new + (1 - alpha) * previous_value
    First update initializes value = new (no warm-up smoothing).
    """

    def __init__(self, alpha: Decimal):
        if not (Decimal("0") <= alpha <= Decimal("1")):
            raise ValueError("alpha must be in [0, 1]")
        self._alpha = alpha
        self._value: Optional[Decimal] = None

    def update(self, x: Decimal) -> Decimal:
        if self._value is None:
            self._value = x
        else:
            self._value = self._alpha * x + (Decimal("1") - self._alpha) * self._value
        return self._value

    @property
    def value(self) -> Optional[Decimal]:
        return self._value

    def reset(self) -> None:
        self._value = None


class FeedHealth:
    """
    Tracks staleness of a named feed by timestamp of last update.

    Optional `last_uid` parameter to mark_update: if provided and equal to the
    previously recorded uid, no update is recorded (the feed is considered
    not to have advanced). This allows callers to detect cached/repeated
    snapshots that aren't real new data.

    Boundary: `is_stale` returns True iff `now - last_update > max_staleness`
    (strict `>`). Exactly at the boundary the feed is considered fresh.
    """

    def __init__(self, name: str, max_staleness_sec: float):
        self.name = name
        self._max_staleness = float(max_staleness_sec)
        self._last_update_ts: Optional[float] = None
        self._last_uid: Optional[int] = None

    def mark_update(self, timestamp: float, uid: Optional[int] = None) -> bool:
        """
        Returns True if the update was recorded (new uid or no uid mode),
        False if it was rejected (uid unchanged from previous).
        """
        if uid is not None and self._last_uid is not None and uid == self._last_uid:
            return False
        self._last_update_ts = float(timestamp)
        self._last_uid = uid
        return True

    def is_stale(self, now: float) -> bool:
        if self._last_update_ts is None:
            return True
        return (float(now) - self._last_update_ts) > self._max_staleness

    def seconds_since_update(self, now: float) -> Optional[float]:
        if self._last_update_ts is None:
            return None
        return float(now) - self._last_update_ts


class LeadLagSignalProvider:
    """
    Orchestrates the signal computation pipeline for a XEMM lead-lag
    strategy. Consumed by a controller via `update()` and read via the
    public properties / `lead_signal_bps()` / `best_lead_signal_bps()`.

    All prices are Decimal. All timestamps are Unix seconds (float).
    """

    def __init__(
        self,
        lead_windows_sec: Optional[List[int]] = None,
        buffer_duration_sec: float = 60.0,
        ema_alpha_fx: Decimal = Decimal("0.3"),
        max_leader_staleness_sec: float = 5.0,
        max_fx_staleness_sec: float = 10.0,
        max_local_staleness_sec: float = 5.0,
    ):
        if lead_windows_sec is None:
            lead_windows_sec = [5, 10, 15]
        self._lead_windows: List[int] = list(lead_windows_sec)
        if not self._lead_windows:
            raise ValueError("lead_windows_sec must contain at least one window")
        if min(self._lead_windows) <= 0:
            raise ValueError("all lead_windows_sec must be positive")

        # Buffer must hold history for the largest window plus headroom.
        min_buffer = max(self._lead_windows) + 10
        actual_buffer = max(float(buffer_duration_sec), float(min_buffer))

        self._buffer_local_mid = CircularPriceBuffer(actual_buffer)
        self._buffer_fair_fast = CircularPriceBuffer(actual_buffer)

        self._ema_fx = EMAFilter(ema_alpha_fx)

        self._health_leader = FeedHealth("leader", max_leader_staleness_sec)
        self._health_fx = FeedHealth("fx", max_fx_staleness_sec)
        self._health_local = FeedHealth("local", max_local_staleness_sec)

        # Latest L1 cache (overwritten on each update; zeros when invalid)
        self._local_bid: Decimal = Decimal("0")
        self._local_ask: Decimal = Decimal("0")
        self._leader_bid: Decimal = Decimal("0")
        self._leader_ask: Decimal = Decimal("0")
        self._fx_bid: Decimal = Decimal("0")
        self._fx_ask: Decimal = Decimal("0")

        self._last_update_time: float = 0.0

    # ------------------------------------------------------------------ #
    # Update                                                             #
    # ------------------------------------------------------------------ #
    def update(
        self,
        timestamp: float,
        local_bid: Decimal,
        local_ask: Decimal,
        leader_bid: Decimal,
        leader_ask: Decimal,
        fx_bid: Decimal,
        fx_ask: Decimal,
        local_book_uid: Optional[int] = None,
        leader_book_uid: Optional[int] = None,
        fx_book_uid: Optional[int] = None,
    ) -> None:
        ts = float(timestamp)
        self._last_update_time = ts

        local_valid = self._is_valid_quote(local_bid, local_ask)
        leader_valid = self._is_valid_quote(leader_bid, leader_ask)
        fx_valid = self._is_valid_quote(fx_bid, fx_ask)

        if local_valid:
            self._local_bid = local_bid
            self._local_ask = local_ask
            local_marked = self._health_local.mark_update(ts, local_book_uid)
            if local_marked:
                self._buffer_local_mid.push(ts, self._mid(local_bid, local_ask))

        if leader_valid:
            self._leader_bid = leader_bid
            self._leader_ask = leader_ask
            self._health_leader.mark_update(ts, leader_book_uid)

        if fx_valid:
            self._fx_bid = fx_bid
            self._fx_ask = fx_ask
            fx_marked = self._health_fx.mark_update(ts, fx_book_uid)
            if fx_marked:
                self._ema_fx.update(self._mid(fx_bid, fx_ask))

        # Push fair_fast (no EMA) into its buffer when leader+fx both valid
        if leader_valid and fx_valid:
            fair_fast = self._mid(leader_bid, leader_ask) * self._mid(fx_bid, fx_ask)
            self._buffer_fair_fast.push(ts, fair_fast)

    # ------------------------------------------------------------------ #
    # Latest L1 properties                                               #
    # ------------------------------------------------------------------ #
    @property
    def local_bid(self) -> Decimal:
        return self._local_bid

    @property
    def local_ask(self) -> Decimal:
        return self._local_ask

    @property
    def local_mid(self) -> Decimal:
        if self._local_bid <= 0 or self._local_ask <= 0:
            return Decimal("0")
        return self._mid(self._local_bid, self._local_ask)

    @property
    def local_spread_bps(self) -> Decimal:
        if self._local_bid <= 0 or self._local_ask <= 0:
            return Decimal("0")
        mid = self.local_mid
        if mid <= 0:
            return Decimal("0")
        return Decimal("10000") * (self._local_ask - self._local_bid) / mid

    @property
    def leader_bid(self) -> Decimal:
        return self._leader_bid

    @property
    def leader_ask(self) -> Decimal:
        return self._leader_ask

    @property
    def leader_mid(self) -> Decimal:
        if self._leader_bid <= 0 or self._leader_ask <= 0:
            return Decimal("0")
        return self._mid(self._leader_bid, self._leader_ask)

    @property
    def fx_bid(self) -> Decimal:
        return self._fx_bid

    @property
    def fx_ask(self) -> Decimal:
        return self._fx_ask

    @property
    def fx_mid_raw(self) -> Decimal:
        if self._fx_bid <= 0 or self._fx_ask <= 0:
            return Decimal("0")
        return self._mid(self._fx_bid, self._fx_ask)

    @property
    def fx_mid_ema(self) -> Optional[Decimal]:
        return self._ema_fx.value

    # ------------------------------------------------------------------ #
    # Synthetic fairs                                                    #
    # ------------------------------------------------------------------ #
    @property
    def fair_brl_fast(self) -> Decimal:
        """No EMA on FX. Used for lead-lag micro signals."""
        leader = self.leader_mid
        fx = self.fx_mid_raw
        if leader <= 0 or fx <= 0:
            return Decimal("0")
        return leader * fx

    @property
    def fair_brl_slow(self) -> Decimal:
        """EMA-smoothed FX. Used for regime / basis_bps."""
        leader = self.leader_mid
        fx_ema = self._ema_fx.value
        if leader <= 0 or fx_ema is None or fx_ema <= 0:
            return Decimal("0")
        return leader * fx_ema

    @property
    def basis_bps(self) -> Decimal:
        """basis = 10000 * (local_mid / fair_brl_slow - 1)."""
        local = self.local_mid
        fair = self.fair_brl_slow
        if local <= 0 or fair <= 0:
            return Decimal("0")
        return Decimal("10000") * (local / fair - Decimal("1"))

    # ------------------------------------------------------------------ #
    # Lead signals                                                       #
    # ------------------------------------------------------------------ #
    def lead_signal_bps(self, window_sec: int) -> Optional[Decimal]:
        """
        lead_signal_bps = 10000 * (log(fair_now/fair_past) - log(local_now/local_past))

        Uses fair_brl_fast (raw FX, no EMA) to avoid EMA-induced lag becoming
        a false signal. Returns None if any feed is stale or insufficient
        history.
        """
        if self.is_any_stale:
            return None

        now = self._last_update_time
        past = now - float(window_sec)

        fair_now = self._buffer_fair_fast.latest_price()
        fair_past = self._buffer_fair_fast.get_price_at_or_before(past)
        local_now = self._buffer_local_mid.latest_price()
        local_past = self._buffer_local_mid.get_price_at_or_before(past)

        if fair_now is None or fair_past is None:
            return None
        if local_now is None or local_past is None:
            return None
        if fair_now <= 0 or fair_past <= 0 or local_now <= 0 or local_past <= 0:
            return None

        # Use float math.log; convert back to Decimal. Bps scale tolerates
        # the float roundtrip (errors ~1e-10).
        leader_ret = math.log(float(fair_now) / float(fair_past))
        local_ret = math.log(float(local_now) / float(local_past))
        return Decimal(str(10000.0 * (leader_ret - local_ret)))

    def best_lead_signal_bps(self) -> Optional[Decimal]:
        """Returns the lead signal with maximum |value| across configured windows.
        None if no window has sufficient history."""
        signals = [self.lead_signal_bps(w) for w in self._lead_windows]
        valid = [s for s in signals if s is not None]
        if not valid:
            return None
        return max(valid, key=lambda v: abs(v))

    # ------------------------------------------------------------------ #
    # Health                                                             #
    # ------------------------------------------------------------------ #
    @property
    def is_leader_stale(self) -> bool:
        return self._health_leader.is_stale(self._last_update_time)

    @property
    def is_fx_stale(self) -> bool:
        return self._health_fx.is_stale(self._last_update_time)

    @property
    def is_local_stale(self) -> bool:
        return self._health_local.is_stale(self._last_update_time)

    @property
    def is_any_stale(self) -> bool:
        return self.is_leader_stale or self.is_fx_stale or self.is_local_stale

    @property
    def signal_quality(self) -> SignalQuality:
        leader_stale = self.is_leader_stale
        fx_stale = self.is_fx_stale
        local_stale = self.is_local_stale
        stale_count = int(leader_stale) + int(fx_stale) + int(local_stale)
        if stale_count >= 2:
            return SignalQuality.BAD
        if leader_stale:
            return SignalQuality.DEGRADED_LEADER
        if fx_stale:
            return SignalQuality.DEGRADED_FX
        if local_stale:
            return SignalQuality.DEGRADED_LOCAL
        return SignalQuality.OK

    @property
    def lead_windows_sec(self) -> List[int]:
        return list(self._lead_windows)

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _mid(bid: Decimal, ask: Decimal) -> Decimal:
        return (bid + ask) / Decimal("2")

    @staticmethod
    def _is_valid_quote(bid: Decimal, ask: Decimal) -> bool:
        return bid > Decimal("0") and ask > Decimal("0") and ask >= bid
