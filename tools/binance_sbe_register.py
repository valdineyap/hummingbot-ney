#!/usr/bin/env python3
"""Register the ``binance_sbe`` connector with Hummingbot's encrypted config.

Hummingbot's ``ConnectorManager`` refuses to instantiate a connector unless
:func:`Security.api_keys` finds an entry for it in ``conf/connectors/``.
The standard way to populate that entry is the interactive ``connect <name>``
flow on the CLI, but that requires a TTY — awkward for headless deployments
and for first-time setup over SSH.

This helper does the same thing non-interactively:

  1. Reads ``BINANCE_SBE_API_KEY`` from ``.env`` (or any prior export).
  2. Builds a :class:`BinanceSbeConfigMap` populated with that key.
  3. Encrypts and writes ``conf/connectors/binance_sbe.yml`` using the
     master password supplied on the command line.

After this runs, ``bash start_xemm_lead_lag.sh <password>`` boots the bot
with the SBE connector available, identical to having run
``connect binance_sbe`` on the CLI.

Idempotent — re-running it overwrites the existing file with whatever is
currently in ``.env``, so rotating the API key is just "edit .env, run
this, restart bot".

Usage::

    python tools/binance_sbe_register.py <master-password>

Exits 0 on success, non-zero on error. Does not echo the API key.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def _load_env_file(path: Path) -> None:
    """Minimal KEY=VALUE loader. Mirrors the one in
    ``tools/binance_sbe_shadow.py`` so behaviour is consistent across
    tooling — same precedence rules (env wins over file)."""
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


def main():
    if len(sys.argv) != 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(2)
    master_password = sys.argv[1]

    _load_env_file(PROJECT_ROOT / ".env")
    sbe_key = os.environ.get("BINANCE_SBE_API_KEY", "")
    if not sbe_key:
        print("ERROR: BINANCE_SBE_API_KEY not found in env or .env",
              file=sys.stderr)
        sys.exit(1)

    # Heavy imports deferred — they pull in the whole Hummingbot graph.
    from pydantic import SecretStr  # noqa: E402

    from hummingbot.client.config.config_crypt import (  # noqa: E402
        ETHKeyFileSecretManger,
    )
    from hummingbot.client.config.config_helpers import ClientConfigAdapter  # noqa: E402
    from hummingbot.client.config.security import Security  # noqa: E402
    from hummingbot.connector.exchange.binance_sbe.binance_sbe_utils import (  # noqa: E402
        BinanceSbeConfigMap,
    )

    # Unlock Security with the supplied master password — same flow that
    # the CLI uses on `connect <name>`.
    secrets_mgr = ETHKeyFileSecretManger(password=master_password)
    Security.login(secrets_mgr)
    if not Security.is_decryption_done():
        # decrypt_all returns synchronously after iterating files; wait
        # only as a defensive measure in case of future async refactors.
        Security.decrypt_all()

    # Build the ConfigMap and wrap in the adapter that save_to_yml expects.
    # IMPORTANT: pass ONLY the SBE API key. The ConfigMap intentionally
    # has no other fields (Phase 1 is signal-only). Adding more fields
    # with None values would cause Security.decrypt_all to crash with a
    # TypeError on the null SecretStr — see the docstring of
    # ``binance_sbe_utils.BinanceSbeConfigMap`` for the full history.
    cm = BinanceSbeConfigMap.model_construct(
        binance_sbe_api_key=SecretStr(sbe_key),
    )
    adapter = ClientConfigAdapter(cm)
    Security.update_secure_config(adapter)

    print("OK wrote conf/connectors/binance_sbe.yml "
          f"(key length={len(sbe_key)}, "
          "hmac fields=empty — signal-only role).")


if __name__ == "__main__":
    main()
