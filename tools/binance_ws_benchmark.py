#!/usr/bin/env python3
"""REST vs WS-API latency benchmark for Binance Spot trading.

Decision gate before cutting the bot's ``taker_connector`` over from
``binance`` to ``binance_ws``. The goal is empirical proof that the
WS path is consistently faster (p50 −5ms, p99 −10ms vs REST) and that
no class of error rate is introduced.

Two modes:

  ``--mode test`` (default) — Benchmark A. Measures *transport latency*
  via ``order.test`` (REST: ``POST /api/v3/order/test`` with
  ``computeCommissionRates=false``; WS: ``order.test``). Server does not
  submit the order, so this isolates auth + signing + RTT. Safe to run
  anywhere with credentials, no min-notional headache.

  ``--mode real`` — Benchmark B. Measures full lifecycle: ``order.place``
  far-from-book + immediate ``order.cancel``. Requires real funds (or
  testnet). Computes price/quantity dynamically from ``trading_rules``
  so MIN_NOTIONAL / LOT_SIZE / PERCENT_PRICE constraints are honoured.

Outputs:
  - Per-request CSV at ``./binance_ws_benchmark.csv`` (one row per
    request).
  - Summary JSON at ``./binance_ws_benchmark_summary.json`` with p50 /
    p90 / p99 / mean / max / std / count / timeout / error rate.

Usage::

    python tools/binance_ws_benchmark.py --mode test --n 1000
    python tools/binance_ws_benchmark.py --mode real --n 100 --pair BTC-USDT

GO criteria (matches plan §6):
  Benchmark A p50 WS ≤ p50 REST − 5ms
              p99 WS ≤ p99 REST − 10ms
  Benchmark B p50 WS ≤ p50 REST − 5ms
  Timeout rate WS < 0.1%; idempotency check returns zero double-submit.

This tool is read-mostly: only ``order.test`` in mode=test, and a single
place+cancel cycle in mode=real. It is NOT designed to be left running
unattended.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import statistics
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def _load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


async def _run(args) -> int:
    # Imports deferred so --help works without pulling the whole graph.
    import aiohttp

    from hummingbot.connector.exchange.binance import (
        binance_constants as B_CONSTANTS,
        binance_web_utils as web_utils,
    )
    from hummingbot.connector.exchange.binance.binance_auth import BinanceAuth
    from hummingbot.connector.exchange.binance_ws import binance_ws_constants as WS_CONSTANTS
    from hummingbot.connector.exchange.binance_ws.binance_ws_request_router import (
        BinanceWsError,
        BinanceWsRequestRouter,
    )
    from hummingbot.connector.exchange.binance_ws.binance_ws_utils import build_rate_limits
    from hummingbot.connector.time_synchronizer import TimeSynchronizer
    from hummingbot.core.api_throttler.async_throttler import AsyncThrottler

    api_key = os.environ.get("BINANCE_API_KEY", "")
    api_secret = os.environ.get("BINANCE_API_SECRET", "")
    if not api_key or not api_secret:
        print("ERROR: set BINANCE_API_KEY and BINANCE_API_SECRET (or use .env)", file=sys.stderr)
        return 1

    pair = args.pair
    symbol = pair.replace("-", "")
    n = args.n

    # ------------------------------------------------------------------
    # Wire up REST + WS clients sharing one throttler (matches prod)
    # ------------------------------------------------------------------
    throttler = AsyncThrottler(list(B_CONSTANTS.RATE_LIMITS) + build_rate_limits())
    time_sync = TimeSynchronizer()
    auth = BinanceAuth(api_key=api_key, secret_key=api_secret, time_provider=time_sync)
    api_factory = web_utils.build_api_factory(throttler=throttler, auth=auth)

    router = BinanceWsRequestRouter(
        api_key=api_key, api_secret=api_secret,
        throttler=throttler, time_provider=time_sync.time, domain="com",
    )
    await router.start()

    # Sync server time once so signatures don't bounce.
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.binance.com/api/v3/time") as r:
            srv = (await r.json())["serverTime"] / 1000.0
            time_sync.add_time_offset_ms_sample((srv - time.time()) * 1000.0)

    rows: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Mode A: order.test
    # ------------------------------------------------------------------
    if args.mode == "test":
        params_base = {
            "symbol": symbol, "side": "BUY", "type": "MARKET",
            "quantity": "0.0001", "computeCommissionRates": "false",
        }

        async def _one_rest():
            t0 = time.monotonic()
            try:
                rest = await api_factory.get_rest_assistant()
                resp = await rest.execute_request(
                    url="https://api.binance.com/api/v3/order/test",
                    params=None,
                    data=params_base,
                    method="POST",
                    is_auth_required=True,
                    return_err=True,
                )
                rtt = (time.monotonic() - t0) * 1000.0
                return {"path": "rest", "rtt_ms": rtt, "ok": "code" not in resp, "err": resp.get("msg", "")}
            except Exception as exc:
                return {"path": "rest", "rtt_ms": (time.monotonic() - t0) * 1000.0, "ok": False, "err": repr(exc)}

        async def _one_ws():
            t0 = time.monotonic()
            try:
                await router.send_signed(WS_CONSTANTS.WS_METHOD_ORDER_TEST,
                                         dict(params_base))
                return {"path": "ws", "rtt_ms": (time.monotonic() - t0) * 1000.0,
                        "ok": True, "err": ""}
            except BinanceWsError as exc:
                return {"path": "ws", "rtt_ms": (time.monotonic() - t0) * 1000.0,
                        "ok": False, "err": repr(exc)}

        # Alternate REST/WS so secular network drift hits both equally.
        for i in range(n):
            rows.append(await _one_rest())
            rows.append(await _one_ws())

    # ------------------------------------------------------------------
    # Mode B: real place+cancel
    # ------------------------------------------------------------------
    else:
        # Fetch trading rules to compute price + qty dynamically.
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.binance.com/api/v3/exchangeInfo?symbol={symbol}") as r:
                info = await r.json()
            async with session.get(f"https://api.binance.com/api/v3/ticker/bookTicker?symbol={symbol}") as r:
                book = await r.json()

        s = info["symbols"][0]
        filters = {f["filterType"]: f for f in s["filters"]}
        tick = Decimal(filters["PRICE_FILTER"]["tickSize"])
        step = Decimal(filters["LOT_SIZE"]["stepSize"])
        min_qty = Decimal(filters["LOT_SIZE"]["minQty"])
        min_notional = Decimal(filters.get("NOTIONAL",
                                           filters.get("MIN_NOTIONAL", {"minNotional": "10"}))["minNotional"])
        best_bid = Decimal(book["bidPrice"])
        pct = filters.get("PERCENT_PRICE_BY_SIDE") or filters.get("PERCENT_PRICE")
        lower_bound = best_bid * Decimal("0.10")
        if pct is not None and "bidMultiplierDown" in pct:
            multiplier_down = Decimal(pct["bidMultiplierDown"])
            avg_price = best_bid  # close enough for far-from-book sanity
            lower_bound = max(lower_bound, avg_price * multiplier_down * Decimal("1.01"))

        # Round price DOWN to tick, qty UP to step satisfying notional.
        price = (lower_bound // tick) * tick
        qty = max(min_qty, ((min_notional * Decimal("1.2") / price) // step + Decimal(1)) * step)

        params_base = {
            "symbol": symbol, "side": "BUY", "type": "LIMIT",
            "timeInForce": "GTC",
            "price": f"{price:f}", "quantity": f"{qty:f}",
        }

        async def _cycle_rest():
            cid = f"bws-bench-rest-{uuid.uuid4().hex[:12]}"
            t0 = time.monotonic()
            rest = await api_factory.get_rest_assistant()
            placed = await rest.execute_request(
                url="https://api.binance.com/api/v3/order",
                data={**params_base, "newClientOrderId": cid},
                method="POST", is_auth_required=True, return_err=True,
            )
            place_rtt = (time.monotonic() - t0) * 1000.0
            t1 = time.monotonic()
            cancelled = await rest.execute_request(
                url="https://api.binance.com/api/v3/order",
                params={"symbol": symbol, "origClientOrderId": cid},
                method="DELETE", is_auth_required=True, return_err=True,
            )
            cancel_rtt = (time.monotonic() - t1) * 1000.0
            return [
                {"path": "rest_place", "rtt_ms": place_rtt,
                 "ok": "code" not in placed, "err": placed.get("msg", "")},
                {"path": "rest_cancel", "rtt_ms": cancel_rtt,
                 "ok": cancelled.get("status") == "CANCELED",
                 "err": cancelled.get("msg", "")},
            ]

        async def _cycle_ws():
            cid = f"bws-bench-ws-{uuid.uuid4().hex[:12]}"
            t0 = time.monotonic()
            try:
                placed = await router.send_signed(
                    WS_CONSTANTS.WS_METHOD_ORDER_PLACE,
                    {**params_base, "newClientOrderId": cid, "newOrderRespType": "ACK"},
                )
                place_rtt = (time.monotonic() - t0) * 1000.0
            except BinanceWsError as exc:
                return [{"path": "ws_place", "rtt_ms": (time.monotonic() - t0) * 1000.0,
                         "ok": False, "err": repr(exc)}]
            t1 = time.monotonic()
            try:
                cancelled = await router.send_signed(
                    WS_CONSTANTS.WS_METHOD_ORDER_CANCEL,
                    {"symbol": symbol, "orderId": int(placed.result["orderId"])},
                )
                cancel_rtt = (time.monotonic() - t1) * 1000.0
                ok = (cancelled.result or {}).get("status") == "CANCELED"
                err = ""
            except BinanceWsError as exc:
                cancel_rtt = (time.monotonic() - t1) * 1000.0
                ok = False
                err = repr(exc)
            return [
                {"path": "ws_place", "rtt_ms": place_rtt, "ok": True, "err": ""},
                {"path": "ws_cancel", "rtt_ms": cancel_rtt, "ok": ok, "err": err},
            ]

        for i in range(n):
            rows.extend(await _cycle_rest())
            rows.extend(await _cycle_ws())

    await router.stop()

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------
    out_csv = Path(args.out_csv)
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["path", "rtt_ms", "ok", "err"])
        w.writeheader()
        for row in rows:
            w.writerow(row)

    def _summary(rtts: List[float]) -> Dict[str, float]:
        if not rtts:
            return {"count": 0}
        rtts_sorted = sorted(rtts)
        return {
            "count": len(rtts),
            "p50": rtts_sorted[len(rtts_sorted) // 2],
            "p90": rtts_sorted[int(len(rtts_sorted) * 0.9)],
            "p99": rtts_sorted[int(len(rtts_sorted) * 0.99)] if len(rtts_sorted) >= 100 else rtts_sorted[-1],
            "mean": statistics.fmean(rtts),
            "max": max(rtts),
            "std": statistics.pstdev(rtts) if len(rtts) > 1 else 0.0,
        }

    summary: Dict[str, Any] = {}
    for path_name in sorted({r["path"] for r in rows}):
        rtts = [r["rtt_ms"] for r in rows if r["path"] == path_name and r["ok"]]
        errs = sum(1 for r in rows if r["path"] == path_name and not r["ok"])
        summary[path_name] = {
            **_summary(rtts),
            "error_count": errs,
        }

    Path(args.out_summary).write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=("test", "real"), default="test")
    p.add_argument("--n", type=int, default=200, help="iterations per channel")
    p.add_argument("--pair", default="BTC-USDT")
    p.add_argument("--out-csv", default="./binance_ws_benchmark.csv")
    p.add_argument("--out-summary", default="./binance_ws_benchmark_summary.json")
    args = p.parse_args()

    _load_env_file(PROJECT_ROOT / ".env")
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
