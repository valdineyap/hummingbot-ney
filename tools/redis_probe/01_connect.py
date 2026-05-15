#!/usr/bin/env python3
"""Phase 0 / step 1 — sanity check the BitPreco Redis credentials.

What this does
==============

1. Loads ``BITPRECO_REDIS_*`` from the environment (``.env``).
2. Opens an async Redis connection with the configured TLS settings.
3. Sends ``PING`` and reports round-trip latency.
4. Tries ``PUBSUB CHANNELS update:*`` and ``PUBSUB CHANNELS orderbook:*``
   to enumerate channels visible to this credential. With a properly
   tight ACL these commands may be denied — that's a sign the ACL is
   doing its job, not a failure of this script.
5. Tries ``PUBSUB NUMSUB`` on the channels we'd subscribe to in
   production, to confirm BitPreco's publishers are actually
   broadcasting to them.

Run::

    conda activate hummingbot
    python tools/redis_probe/01_connect.py

Prints results to stdout. Nothing is written to disk.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RedisConfig, connect_pubsub, setup_logging  # noqa: E402


log = setup_logging("redis_probe.01_connect")


async def run(cfg: RedisConfig) -> int:
    print(f"Connecting to {cfg.host}:{cfg.port} "
          f"(TLS={cfg.tls}, verify={cfg.tls_verify}, db={cfg.db})")
    async with connect_pubsub(cfg) as (client, pubsub):  # noqa: F841
        t0 = time.monotonic()
        pong = await client.ping()
        dt_ms = (time.monotonic() - t0) * 1000
        print(f"PING -> {pong!r}  ({dt_ms:.1f} ms RTT)")

        # PUBSUB CHANNELS by pattern. One call per pattern (Redis takes
        # a single pattern per invocation).
        for pattern in ("update:*", "orderbook:*"):
            try:
                channels = await client.pubsub_channels(pattern=pattern)
            except Exception as e:
                print(f"PUBSUB CHANNELS {pattern!r} -> ERROR ({type(e).__name__}: {e})")
                continue
            print(f"PUBSUB CHANNELS {pattern!r} -> {len(channels)} channel(s)")
            for ch in channels[:20]:
                print(f"   {ch}")
            if len(channels) > 20:
                print(f"   ... ({len(channels) - 20} more)")

        # Try the channels we'd actually subscribe to in production.
        probe_channels = []
        if cfg.user_id:
            probe_channels.append(f"update:{cfg.user_id}")
        else:
            print("BITPRECO_USER_ID empty — skipping update:<id> probe. "
                  "Run 04_get_user_id.py first.")
        # Common pairs
        for market in ("BTC-BRL", "ETH-BRL", "USDT-BRL"):
            probe_channels.append(f"orderbook:{market}")

        if probe_channels:
            try:
                numsub = await client.pubsub_numsub(*probe_channels)
            except Exception as e:
                print(f"PUBSUB NUMSUB -> ERROR ({type(e).__name__}: {e})")
            else:
                print("PUBSUB NUMSUB (subscribers per channel):")
                for ch, n in numsub:
                    marker = "" if n > 0 else "  <- nobody subscribed; channel may be inactive"
                    print(f"   {ch}: {n}{marker}")

    print("OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.parse_args()  # no flags yet, but parse anyway for --help

    try:
        cfg = RedisConfig.from_env()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    try:
        return asyncio.run(run(cfg))
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        log.exception("connect failed")
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
