#!/usr/bin/env python3
"""Register the ``binance_ws`` connector with Hummingbot's encrypted config.

Hummingbot's ``ConnectorManager`` refuses to instantiate a connector
unless :func:`Security.api_keys` finds an entry for it under
``conf/connectors/``. The standard way to populate that entry is the
interactive ``connect <name>`` flow on the CLI, but that requires a
TTY — awkward for headless deployments.

This helper does the same thing non-interactively:

  1. Reads the existing ``binance.yml`` to recover the operator's HMAC
     pair. The WS-API trading connector uses the SAME HMAC the REST
     connector uses (Binance issues one HMAC per API key).
  2. Builds a :class:`BinanceWsConfigMap` populated with that key/secret
     and ``use_ws_trading=True``.
  3. Encrypts and writes ``conf/connectors/binance_ws.yml`` under the
     master password supplied on the command line.

After this runs, ``bash start_xemm_lead_lag.sh <password>`` boots the
bot with the WS-API connector available, identical to having run
``connect binance_ws`` on the CLI.

Idempotent — re-running it overwrites the existing file with whatever
is currently in ``binance.yml`` and (if present) the env-var override
for ``use_ws_trading``. Rotating the HMAC means "rotate binance.yml,
re-run this".

Usage::

    python tools/binance_ws_register.py <master-password>

Exits 0 on success, non-zero on error. Does not echo the HMAC.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def main():
    if len(sys.argv) != 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(2)
    master_password = sys.argv[1]

    # Heavy imports deferred — they pull in the whole Hummingbot graph.
    from pydantic import SecretStr  # noqa: E402

    from hummingbot.client.config.config_crypt import (  # noqa: E402
        ETHKeyFileSecretManger,
    )
    from hummingbot.client.config.config_helpers import ClientConfigAdapter  # noqa: E402
    from hummingbot.client.config.security import Security  # noqa: E402
    from hummingbot.connector.exchange.binance_ws.binance_ws_utils import (  # noqa: E402
        BinanceWsConfigMap,
    )

    secrets_mgr = ETHKeyFileSecretManger(password=master_password)
    Security.login(secrets_mgr)
    if not Security.is_decryption_done():
        Security.decrypt_all()

    binance_keys = Security.api_keys("binance")
    if not binance_keys:
        print(
            "ERROR: conf/connectors/binance.yml not found (or empty). "
            "Run `connect binance` on the Hummingbot CLI first so this "
            "tool can copy the HMAC creds into binance_ws.yml.",
            file=sys.stderr,
        )
        sys.exit(1)
    hmac_key = binance_keys.get("binance_api_key")
    hmac_secret = binance_keys.get("binance_api_secret")
    if not hmac_key or not hmac_secret:
        print(
            "ERROR: binance config is missing binance_api_key or "
            "binance_api_secret. Re-run `connect binance` to fix.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Honour env-var revert flag if explicitly set, so an operator
    # registering after a revert keeps the file consistent with the env.
    env_use = os.environ.get("BINANCE_WS_USE_WS_TRADING", "").strip().lower()
    use_ws_trading = env_use not in ("0", "false", "no", "off")

    cm = BinanceWsConfigMap.model_construct(
        binance_ws_api_key=SecretStr(hmac_key),
        binance_ws_api_secret=SecretStr(hmac_secret),
        use_ws_trading=use_ws_trading,
    )
    adapter = ClientConfigAdapter(cm)
    Security.update_secure_config(adapter)

    print(
        f"OK wrote conf/connectors/binance_ws.yml "
        f"(hmac key length={len(hmac_key)}, hmac secret length={len(hmac_secret)}, "
        f"use_ws_trading={use_ws_trading}). HMAC copied from binance.yml — "
        "WS-API trading connector now shares the same Binance HMAC."
    )


if __name__ == "__main__":
    main()
