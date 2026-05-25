"""
Shadow mode signal test — runs LeadLagSignalProvider with live market data
from public REST APIs (no auth required) for N seconds.

Usage:
    python shadow_mode_test.py [--seconds 120]

Output: one CSV-like line per tick showing signal quality, basis, lead signal.
"""
import argparse
import time
import sys
import os
import urllib.request
import json
from decimal import Decimal

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ".")

from hummingbot.strategy_v2.utils.lead_lag_signal import LeadLagSignalProvider


def fetch_bybit_brl() -> tuple[Decimal, Decimal]:
    """Bybit BTC-BRL best bid/ask via public REST."""
    url = "https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCBRL"
    with urllib.request.urlopen(url, timeout=5) as r:
        data = json.loads(r.read())
    item = data["result"]["list"][0]
    return Decimal(item["bid1Price"]), Decimal(item["ask1Price"])


def fetch_binance(symbol: str) -> tuple[Decimal, Decimal]:
    """Binance best bid/ask via public REST."""
    url = f"https://api.binance.com/api/v3/ticker/bookTicker?symbol={symbol}"
    with urllib.request.urlopen(url, timeout=5) as r:
        data = json.loads(r.read())
    return Decimal(data["bidPrice"]), Decimal(data["askPrice"])


def run(seconds: int):
    signal = LeadLagSignalProvider(
        ema_alpha_fx=Decimal("0.30"),
        max_leader_staleness_sec=10.0,
        max_fx_staleness_sec=15.0,
        max_local_staleness_sec=10.0,
        lead_windows_sec=[5, 10, 15],
    )

    header = (
        f"{'time':>8} {'quality':<20} {'local_mid':>10} {'fair_brl':>10} "
        f"{'basis_bps':>10} {'lead_5s':>8} {'lead_10s':>8} {'lead_15s':>8}"
    )
    print(header)
    print("-" * len(header))

    start = time.time()
    tick = 0

    while time.time() - start < seconds:
        t0 = time.time()
        now = t0

        try:
            local_bid, local_ask = fetch_bybit_brl()
            btcusdt_bid, btcusdt_ask = fetch_binance("BTCUSDT")
            usdtbrl_bid, usdtbrl_ask = fetch_binance("USDTBRL")
        except Exception as e:
            print(f"  [fetch error: {e}]", flush=True)
            time.sleep(1)
            continue

        signal.update(
            timestamp=now,
            local_bid=local_bid,
            local_ask=local_ask,
            leader_bid=btcusdt_bid,
            leader_ask=btcusdt_ask,
            fx_bid=usdtbrl_bid,
            fx_ask=usdtbrl_ask,
        )

        local_mid = (local_bid + local_ask) / 2
        fair = signal.fair_brl_slow
        basis = signal.basis_bps

        leads = {}
        for w in [5, 10, 15]:
            leads[w] = signal.lead_signal_bps(w)

        def fmt(v, decimals=2):
            if v is None:
                return "    None"
            return f"{float(v):+8.{decimals}f}"

        elapsed = int(time.time() - start)
        print(
            f"{elapsed:>7}s "
            f"{signal.signal_quality.value:<20} "
            f"{float(local_mid):>10.0f} "
            f"{float(fair) if fair else 0:>10.0f} "
            f"{fmt(basis, 1):>10} "
            f"{fmt(leads[5]):>8} "
            f"{fmt(leads[10]):>8} "
            f"{fmt(leads[15]):>8}",
            flush=True,
        )

        tick += 1
        elapsed_fetch = time.time() - t0
        sleep = max(0.0, 1.0 - elapsed_fetch)
        time.sleep(sleep)

    print(f"\nDone. {tick} ticks in {seconds}s.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=120)
    args = parser.parse_args()
    run(args.seconds)
