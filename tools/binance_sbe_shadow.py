#!/usr/bin/env python3
"""Shadow validation tool: run ``binance`` (JSON) and ``binance_sbe`` side
by side and emit a CSV of every order-book event from each stream, plus
a summary. Use this as the GATE-2 validation before flipping the XEMM
controller's ``signal_connector`` to ``binance_sbe`` in production.

Modes
=====

``both`` (default)
  Boots both connectors in the same Python process. Best for latency
  comparison (same wall clock, same network stack). CPU and memory
  metrics are MIXED between the two — use ``json_only`` / ``sbe_only``
  runs separately for absolute CPU comparison.

``json_only`` / ``sbe_only``
  Boots a single connector. Use for clean CPU/memory measurements.

Usage
=====

::

  # Quick smoke (10 min, BTC-USDT)
  python tools/binance_sbe_shadow.py --duration 600 --pairs BTC-USDT

  # Multipair, 30 min
  python tools/binance_sbe_shadow.py --duration 1800 \
      --pairs BTC-USDT USDT-USDC ETH-USDT

  # SBE only, 2h, for clean CPU measurement
  python tools/binance_sbe_shadow.py --mode sbe_only --duration 7200

Credentials
===========

The SBE WS requires an Ed25519 API key STRING in ``X-MBX-APIKEY``. This
tool reads it from ``$BINANCE_SBE_API_KEY`` so credentials never sit on
disk in plain text and so the same script can run in CI smoke tests.

CSV output
==========

One row per event in ``$OUTPUT_DIR/<YYYYMMDDTHHMM>_<mode>.csv``. Columns:

  ts_recv_ns        monotonic_ns when our event handler observed the message
  ts_wall_ns        time_ns at the same moment (wall clock, for cross-host alignment)
  source            "json" or "sbe"
  event_type        "trade" or "diff"
  symbol            exchange symbol (e.g. "BTCUSDT")
  seq_id            trade_id for trades, last update_id (u) for diffs
  best_bid          string decimal, top-of-book bid AFTER this event
  best_ask          string decimal, top-of-book ask AFTER this event
  bid_qty           quantity at best bid
  ask_qty           quantity at best ask
  event_ts_ms       Binance-side event timestamp (ms, for staleness analysis)

A summary JSON is written on shutdown with counts, durations, and CPU samples.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Make hummingbot imports work when running from project root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None  # CPU samples become no-ops if psutil is missing

from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange  # noqa: E402
from hummingbot.connector.exchange.binance_sbe.binance_sbe_exchange import (  # noqa: E402
    BinanceSbeExchange,
)


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

_CSV_HEADER = [
    "ts_recv_ns", "ts_wall_ns", "source", "event_type", "symbol", "seq_id",
    "best_bid", "best_ask", "bid_qty", "ask_qty", "event_ts_ms",
]


def _open_csv(output_dir: Path, mode: str) -> "csv._writer":
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M")
    path = output_dir / f"{stamp}_{mode}.csv"
    fh = open(path, "w", newline="")
    writer = csv.writer(fh)
    writer.writerow(_CSV_HEADER)
    return writer, fh, path


# ---------------------------------------------------------------------------
# Per-source event consumer
# ---------------------------------------------------------------------------


class StreamRecorder:
    """Drain a connector's order-book + trade queues and write rows.

    We intentionally read from the connector's *order book object* (which
    is what downstream strategies see) rather than the raw WS payload, so
    the recorded best_bid/best_ask reflect the same view a real strategy
    would observe.
    """

    def __init__(self,
                 source: str,
                 connector,
                 writer,
                 stats: Dict[str, Any]):
        self._source = source
        self._connector = connector
        self._writer = writer
        self._stats = stats
        self._stop = asyncio.Event()

    async def run(self):
        # Wait for connector readiness — order books need ~5-15s.
        await self._wait_ready(timeout=60.0)

        order_books = self._connector.order_books
        # Use the connector-level event queues that BinanceExchange/SbeExchange expose.
        # Strategies subscribe to events via the OrderBook itself; for shadow
        # measurements we poll the top-of-book at high frequency in a
        # dedicated task per pair instead of trying to plumb in a real
        # MarketEvent listener (which would require a full strategy host).
        tasks = []
        for trading_pair, ob in order_books.items():
            tasks.append(asyncio.create_task(self._poll_top_of_book(trading_pair, ob)))
        await self._stop.wait()
        for t in tasks:
            t.cancel()

    async def _wait_ready(self, timeout: float):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._connector.order_books and all(
                ob.last_trade_price is not None or len(ob.bid_entries()) > 0
                for ob in self._connector.order_books.values()
            ):
                return
            await asyncio.sleep(0.5)
        logging.warning(f"[{self._source}] connector never reached ready state within "
                        f"{timeout}s — recording will proceed with whatever it has")

    async def _poll_top_of_book(self, trading_pair: str, order_book):
        """Sample the connector-visible top of book at high frequency.

        This deliberately doesn't try to count *every* underlying event —
        SBE depth is 25ms vs JSON 100ms, so event-by-event symmetry is
        impossible. What matters for the shadow comparison is **what the
        consumer sees**: the strategy reads top of book, so we record top
        of book at a uniform polling rate.
        """
        last_update_id = -1
        last_trade_price = None
        # Poll faster than the JSON stream cadence (100ms) so we don't
        # miss SBE-only updates that fall within a JSON-stream gap.
        poll_interval = 0.02  # 20ms
        symbol = trading_pair.replace("-", "")
        while not self._stop.is_set():
            try:
                # Cheap snapshot via internal API.
                bids = list(order_book.bid_entries())[:1]
                asks = list(order_book.ask_entries())[:1]
                bid = bids[0] if bids else None
                ask = asks[0] if asks else None
                # Detect a change to avoid spamming the CSV.
                update_id = getattr(order_book, "last_diff_uid", None) or order_book.snapshot_uid
                trade_price = order_book.last_trade_price
                event_changed = (
                    update_id != last_update_id or trade_price != last_trade_price
                )
                if event_changed and bid is not None and ask is not None:
                    self._writer.writerow([
                        time.monotonic_ns(),
                        time.time_ns(),
                        self._source,
                        "diff" if update_id != last_update_id else "trade",
                        symbol,
                        update_id if update_id != last_update_id else 0,
                        str(bid.price),
                        str(ask.price),
                        str(bid.amount),
                        str(ask.amount),
                        "",  # event_ts_ms — not available from order_book object alone
                    ])
                    self._stats[self._source]["events"] += 1
                    last_update_id = update_id
                    last_trade_price = trade_price
                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                return
            except Exception:
                logging.exception(f"[{self._source}/{trading_pair}] poll error")
                await asyncio.sleep(0.5)

    def stop(self):
        self._stop.set()


# ---------------------------------------------------------------------------
# Connector boot helpers
# ---------------------------------------------------------------------------


def _build_json_connector(trading_pairs: List[str]) -> BinanceExchange:
    return BinanceExchange(
        binance_api_key="",
        binance_api_secret="",
        trading_pairs=trading_pairs,
        trading_required=False,
    )


def _build_sbe_connector(trading_pairs: List[str]) -> BinanceSbeExchange:
    sbe_key = os.environ.get("BINANCE_SBE_API_KEY")
    if not sbe_key:
        raise SystemExit(
            "BINANCE_SBE_API_KEY env var is required for SBE mode "
            "(export the Ed25519 API key string from the Binance portal)."
        )
    return BinanceSbeExchange(
        binance_sbe_api_key=sbe_key,
        trading_pairs=trading_pairs,
        trading_required=False,
    )


# ---------------------------------------------------------------------------
# CPU sampler
# ---------------------------------------------------------------------------


async def _sample_cpu(stop_event: asyncio.Event, stats: Dict[str, Any],
                      interval_sec: float = 5.0):
    if psutil is None:
        return
    proc = psutil.Process(os.getpid())
    proc.cpu_percent(interval=None)  # prime
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_sec)
        except asyncio.TimeoutError:
            cpu = proc.cpu_percent(interval=None)
            stats["cpu_samples"].append(cpu)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def _run(args: argparse.Namespace):
    pairs = args.pairs
    stats = {
        "mode": args.mode,
        "pairs": pairs,
        "duration_sec": args.duration,
        "start_iso": datetime.now(timezone.utc).isoformat(),
        "json": {"events": 0},
        "sbe": {"events": 0},
        "cpu_samples": [],
    }
    output_dir = Path(args.output_dir)
    writer, csv_fh, csv_path = _open_csv(output_dir, args.mode)
    logging.info(f"writing CSV to {csv_path}")

    connectors = []
    recorders = []
    if args.mode in ("both", "json_only"):
        json_conn = _build_json_connector(pairs)
        connectors.append(json_conn)
        recorders.append(StreamRecorder("json", json_conn, writer, stats))
    if args.mode in ("both", "sbe_only"):
        sbe_conn = _build_sbe_connector(pairs)
        connectors.append(sbe_conn)
        recorders.append(StreamRecorder("sbe", sbe_conn, writer, stats))

    # Connector networks start when we await start_network() — done
    # implicitly by ExchangePyBase when it's used inside a strategy.
    # Here we drive it manually.
    for conn in connectors:
        await conn.start_network()

    stop_event = asyncio.Event()

    def _handle_sigint(*_):
        logging.info("SIGINT received — stopping")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    tasks = [asyncio.create_task(r.run()) for r in recorders]
    tasks.append(asyncio.create_task(_sample_cpu(stop_event, stats)))

    # Duration timer.
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=args.duration)
    except asyncio.TimeoutError:
        pass

    for r in recorders:
        r.stop()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    for conn in connectors:
        try:
            await conn.stop_network()
        except Exception:
            logging.exception("error stopping connector")

    csv_fh.close()
    stats["end_iso"] = datetime.now(timezone.utc).isoformat()
    summary_path = csv_path.with_suffix(".summary.json")
    with summary_path.open("w") as fh:
        json.dump(stats, fh, indent=2, default=str)
    logging.info(f"wrote summary to {summary_path}")
    logging.info(f"events: json={stats['json']['events']}  sbe={stats['sbe']['events']}")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=("both", "json_only", "sbe_only"), default="both",
                   help="Which connector(s) to boot. Default: both.")
    p.add_argument("--pairs", nargs="+", default=["BTC-USDT"],
                   help="Trading pairs to subscribe to (Hummingbot format).")
    p.add_argument("--duration", type=int, default=600,
                   help="Run duration in seconds. Default: 600 (10 min).")
    p.add_argument("--output-dir", default="var/sbe_shadow",
                   help="Where to write CSV + summary. Default: var/sbe_shadow.")
    p.add_argument("--log-level", default="INFO",
                   choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return p


def main():
    args = _build_parser().parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
