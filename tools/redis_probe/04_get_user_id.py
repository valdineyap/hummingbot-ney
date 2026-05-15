#!/usr/bin/env python3
"""Phase 0 / step 4 — fetch the numeric BitPreco user id.

``bitbots-js`` calls REST ``cmd=get_user_id`` once at startup and uses
the returned ``id`` (the ``idBPBot``) to subscribe to
``update:<idBPBot>``. This script mirrors that call so we can put the
value in ``.env`` (``BITPRECO_USER_ID``) before running the other
capture scripts.

Run::

    conda activate hummingbot
    python tools/redis_probe/04_get_user_id.py

Prints the user id to stdout. If the BitPreco endpoint name differs,
this script also tries a couple of plausible variants and reports
which (if any) returned a usable id.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import RestCreds, rest_post, setup_logging  # noqa: E402


log = setup_logging("redis_probe.04_get_user_id")

# Candidates ordered from most-likely to least. bitbots-js uses the
# first one. The others are fallbacks if the public API named it
# differently.
CMD_CANDIDATES = ("get_user_id", "user_id", "account", "my_user_id")


async def run() -> int:
    try:
        creds = RestCreds.from_env()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    found_id = None
    found_cmd = None
    responses = {}
    for cmd in CMD_CANDIDATES:
        try:
            resp = await rest_post(cmd, creds, timeout_s=10.0)
        except Exception as e:
            log.warning("cmd=%s failed: %s", cmd, e)
            responses[cmd] = {"error": str(e)}
            continue
        responses[cmd] = resp
        if not isinstance(resp, dict):
            continue
        if resp.get("success") is False:
            continue
        # bitbots-js uses resp.id directly. Try common variants.
        candidate = (
            resp.get("id")
            or resp.get("user_id")
            or resp.get("userId")
            or resp.get("idBPBot")
        )
        if candidate:
            found_id = candidate
            found_cmd = cmd
            break

    if found_id is not None:
        print(f"BITPRECO_USER_ID={found_id}")
        print(f"# (returned by cmd={found_cmd!r})", file=sys.stderr)
        print("", file=sys.stderr)
        print("Add this line to .env, then re-source:", file=sys.stderr)
        print("    set -a; source .env; set +a", file=sys.stderr)
        return 0

    print("Could not resolve user id. Responses:", file=sys.stderr)
    print(json.dumps(responses, indent=2, default=str), file=sys.stderr)
    return 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.parse_args()
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
