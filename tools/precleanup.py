#!/usr/bin/env python
"""
Pre-cleanup: cancel all open orders on the maker and taker exchanges via direct
REST calls, BEFORE starting the main Hummingbot process.

Why this exists
---------------
The in-bot startup_cleanup runs only after the connector reports `ready=True`,
which can take 5-10s after process start. If a previous session crashed without
cancelling its maker order, that orphan order may FILL during this window —
producing a directional position with no hedge.

This script cancels orders BEFORE the new bot is launched, eliminating that
window. It uses the Hummingbot Security module to decrypt API keys (so it
reuses the same credentials as the bot, no duplication) but talks to the
exchange via plain `aiohttp` — no connector lifecycle, no WS sync, no waiting.

Usage
-----
    python tools/precleanup.py \\
        --controller-config conf/controllers/xemm_lead_lag_btc_brl.yml \\
        --password "$PASS"

Exit codes
----------
    0 — success (clean or all orders cancelled)
    1 — auth / config error (cannot proceed; main bot should NOT start)
    2 — partial failure (some cancels failed; main bot will try again at boot)
"""
import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp
import yaml

# Make hummingbot imports work when running from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger  # noqa: E402
from hummingbot.client.config.security import Security  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [precleanup] %(message)s",
)
log = logging.getLogger("precleanup")

# Per-exchange REST endpoints
BYBIT_REST_URL = "https://api.bybit.com"
BINANCE_REST_URL = "https://api.binance.com"
BITPRECO_REST_URL = "https://api.bitpreco.com/v1/trading"

# Bybit constants (must match hummingbot.connector.exchange.bybit.bybit_constants)
BYBIT_RECV_WINDOW = "50000"


def _hb_to_exchange_symbol(connector_name: str, trading_pair: str) -> str:
    """Convert Hummingbot 'BTC-BRL' to exchange-native 'BTCBRL'."""
    return trading_pair.replace("-", "")


# --------------------------------------------------------------------------- #
# Bybit                                                                       #
# --------------------------------------------------------------------------- #
def _bybit_sign_get(api_key: str, secret: str, params: Dict[str, str]) -> Tuple[str, str]:
    """Returns (timestamp, signature). Bybit V5 signing scheme for GET."""
    ts = str(int(time.time() * 1000))
    param_str = ts + api_key + BYBIT_RECV_WINDOW + urlencode(params)
    signature = hmac.new(
        secret.encode("utf-8"),
        param_str.encode("utf-8"),
        digestmod="sha256",
    ).hexdigest()
    return ts, signature


def _bybit_sign_post(api_key: str, secret: str, body: Dict[str, str]) -> Tuple[str, str, str]:
    """Returns (timestamp, signature, body_json). Bybit V5 signing scheme for POST."""
    ts = str(int(time.time() * 1000))
    body_json = json.dumps(body, separators=(",", ":"))
    param_str = ts + api_key + BYBIT_RECV_WINDOW + body_json
    signature = hmac.new(
        secret.encode("utf-8"),
        param_str.encode("utf-8"),
        digestmod="sha256",
    ).hexdigest()
    return ts, signature, body_json


async def bybit_cancel_open_orders(
    session: aiohttp.ClientSession,
    api_key: str,
    secret: str,
    exchange_symbol: str,
) -> Tuple[int, int]:
    """
    Cancel all open spot orders for `exchange_symbol` on Bybit.
    Returns (cancelled_count, failed_count).
    """
    params = {
        "category": "spot",
        "symbol": exchange_symbol,
        "orderStatus": "New",
        "limit": "50",
    }
    ts, sig = _bybit_sign_get(api_key, secret, params)
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-SIGN": sig,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW,
    }
    url = f"{BYBIT_REST_URL}/v5/order/realtime?{urlencode(params)}"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
    except Exception as e:
        log.error(f"bybit/{exchange_symbol}: GET open orders failed: {e}")
        return 0, 1

    if data.get("retCode") != 0:
        log.error(f"bybit/{exchange_symbol}: GET error retCode={data.get('retCode')} msg={data.get('retMsg')}")
        return 0, 1

    orders = data.get("result", {}).get("list", [])
    if not orders:
        log.info(f"bybit/{exchange_symbol}: no open orders — clean.")
        return 0, 0

    log.warning(f"bybit/{exchange_symbol}: found {len(orders)} open order(s) — cancelling...")
    cancelled = 0
    failed = 0
    for order in orders:
        order_id = order["orderId"]
        body = {"category": "spot", "symbol": exchange_symbol, "orderId": order_id}
        ts2, sig2, body_json = _bybit_sign_post(api_key, secret, body)
        cancel_headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-TIMESTAMP": ts2,
            "X-BAPI-SIGN": sig2,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW,
            "Content-Type": "application/json",
            "referer": "Hummingbot",
        }
        cancel_url = f"{BYBIT_REST_URL}/v5/order/cancel"
        try:
            async with session.post(
                cancel_url,
                data=body_json,
                headers=cancel_headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as cresp:
                cdata = await cresp.json()
            if cdata.get("retCode") == 0:
                cancelled += 1
                log.warning(
                    f"bybit/{exchange_symbol}: cancelled orderId={order_id} "
                    f"link={order.get('orderLinkId', '?')}"
                )
            else:
                failed += 1
                log.error(
                    f"bybit/{exchange_symbol}: cancel failed orderId={order_id} "
                    f"retCode={cdata.get('retCode')} msg={cdata.get('retMsg')}"
                )
        except Exception as e:
            failed += 1
            log.error(f"bybit/{exchange_symbol}: cancel exception orderId={order_id}: {e}")
    return cancelled, failed


# --------------------------------------------------------------------------- #
# Binance                                                                     #
# --------------------------------------------------------------------------- #
def _binance_sign(secret: str, params: Dict[str, str]) -> str:
    """Binance HMAC-SHA256 signature over urlencoded params."""
    payload = urlencode(params)
    return hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


async def binance_cancel_open_orders(
    session: aiohttp.ClientSession,
    api_key: str,
    secret: str,
    exchange_symbol: str,
) -> Tuple[int, int]:
    """
    Binance batch-cancel: DELETE /api/v3/openOrders?symbol=...
    Returns (cancelled_count, failed_count).
    """
    params = {
        "symbol": exchange_symbol,
        "timestamp": str(int(time.time() * 1000)),
        "recvWindow": "10000",
    }
    params["signature"] = _binance_sign(secret, params)
    url = f"{BINANCE_REST_URL}/api/v3/openOrders?{urlencode(params)}"
    headers = {"X-MBX-APIKEY": api_key}
    try:
        async with session.delete(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
    except Exception as e:
        log.error(f"binance/{exchange_symbol}: DELETE openOrders failed: {e}")
        return 0, 1

    # Binance returns -2011 ("Unknown order sent") when nothing to cancel
    if isinstance(data, dict) and data.get("code") == -2011:
        log.info(f"binance/{exchange_symbol}: no open orders — clean.")
        return 0, 0

    # Binance also returns -1102/-1021 etc. for parameter/timestamp errors → fail
    if isinstance(data, dict) and "code" in data and data["code"] != 200:
        log.error(f"binance/{exchange_symbol}: error code={data.get('code')} msg={data.get('msg')}")
        return 0, 1

    if isinstance(data, list):
        if not data:
            log.info(f"binance/{exchange_symbol}: no open orders — clean.")
            return 0, 0
        for o in data:
            log.warning(
                f"binance/{exchange_symbol}: cancelled "
                f"orderId={o.get('orderId')} client={o.get('clientOrderId', '?')}"
            )
        return len(data), 0

    log.warning(f"binance/{exchange_symbol}: unexpected response: {data}")
    return 0, 1


# --------------------------------------------------------------------------- #
# BitPreco                                                                    #
# --------------------------------------------------------------------------- #
async def bitpreco_cancel_open_orders(
    session: aiohttp.ClientSession,
    api_key: str,
    secret: str,
    exchange_symbol: str,
) -> Tuple[int, int]:
    """
    BitPreco batch-cancel: POST /v1/trading/all_orders_cancel.

    IMPORTANT: cancels ALL open orders on the BitPreco account, ignoring
    `exchange_symbol` (kept in signature for dispatch uniformity). Safe here
    because the BitPreco account is dedicated to this bot.

    Auth scheme: auth_token = secret + api_key in the JSON body. No HMAC.
    """
    url = f"{BITPRECO_REST_URL}/all_orders_cancel"
    body = {"auth_token": f"{secret}{api_key}"}
    try:
        async with session.post(
            url,
            json=body,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json()
    except Exception as e:
        log.error(f"bitpreco/{exchange_symbol}: all_orders_cancel failed: {e}")
        return 0, 1

    if isinstance(data, dict) and data.get("success"):
        n = int(data.get("orders_canceled", 0))
        if n:
            log.warning(f"bitpreco/{exchange_symbol}: cancelled {n} order(s) (account-wide).")
        else:
            log.info(f"bitpreco/{exchange_symbol}: no open orders — clean.")
        return n, 0

    log.error(f"bitpreco/{exchange_symbol}: unexpected response: {data}")
    return 0, 1


# --------------------------------------------------------------------------- #
# Orchestration                                                               #
# --------------------------------------------------------------------------- #
DISPATCH = {
    "bybit": bybit_cancel_open_orders,
    "binance": binance_cancel_open_orders,
    "bitpreco": bitpreco_cancel_open_orders,
}


def _load_targets_from_config(controller_config_path: Path) -> List[Tuple[str, str]]:
    """
    Read the controller YAML and extract (connector, trading_pair) tuples to
    clean. Deduplicates so each (connector, pair) is processed at most once.
    """
    with open(controller_config_path) as f:
        cfg = yaml.safe_load(f) or {}

    pairs: List[Tuple[str, str]] = []
    seen = set()
    for conn_key, pair_key in [
        ("maker_connector", "maker_trading_pair"),
        ("taker_connector", "taker_trading_pair"),
    ]:
        connector = cfg.get(conn_key)
        pair = cfg.get(pair_key)
        if not connector or not pair:
            continue
        key = (connector, pair)
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)
    return pairs


def _get_keys(connector_name: str) -> Optional[Tuple[str, str]]:
    """Returns (api_key, secret_key) for the given connector, or None on failure."""
    keys = Security.api_keys(connector_name)
    if not keys:
        log.error(f"{connector_name}: no decrypted credentials found")
        return None
    # Hummingbot stores keys as e.g. {"binance_api_key": "...", "binance_api_secret": "..."}
    api_key = None
    secret = None
    for k, v in keys.items():
        kl = k.lower()
        if "api_key" in kl and v:
            api_key = str(v)
        elif "secret" in kl and v:
            secret = str(v)
    if not api_key or not secret:
        log.error(f"{connector_name}: missing api_key or secret in {list(keys.keys())}")
        return None
    return api_key, secret


async def run(controller_config_path: Path, password: str) -> int:
    """Returns the exit code (0/1/2)."""
    # 1. Login + decrypt connector credentials
    secrets_manager = ETHKeyFileSecretManger(password)
    if not Security.login(secrets_manager):
        log.error("Invalid password — cannot decrypt credentials")
        return 1
    await Security.wait_til_decryption_done()

    # 2. Discover targets
    targets = _load_targets_from_config(controller_config_path)
    if not targets:
        log.warning("No (connector, trading_pair) pairs found in config — nothing to clean")
        return 0
    log.info(f"Targets: {targets}")

    # 3. Cancel orders in parallel across exchanges (sequential within an exchange)
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        coros = []
        for connector_name, trading_pair in targets:
            handler = DISPATCH.get(connector_name)
            if handler is None:
                log.warning(f"{connector_name}/{trading_pair}: no precleanup handler — skipping")
                continue
            keys = _get_keys(connector_name)
            if keys is None:
                log.error(f"{connector_name}/{trading_pair}: skipping (no credentials)")
                continue
            api_key, secret = keys
            exchange_symbol = _hb_to_exchange_symbol(connector_name, trading_pair)
            coros.append(handler(session, api_key, secret, exchange_symbol))
        results = await asyncio.gather(*coros, return_exceptions=True)

    # 4. Tally
    total_cancelled = 0
    total_failed = 0
    for r in results:
        if isinstance(r, Exception):
            total_failed += 1
            log.error(f"task raised: {r}")
        else:
            c, f = r
            total_cancelled += c
            total_failed += f

    if total_failed > 0:
        log.warning(
            f"Pre-cleanup completed with {total_cancelled} cancelled, "
            f"{total_failed} failed — bot startup_cleanup will retry at boot"
        )
        return 2
    log.info(f"Pre-cleanup complete: {total_cancelled} cancelled, 0 failed")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Cancel open orders before bot startup")
    parser.add_argument(
        "--controller-config",
        required=True,
        type=Path,
        help="Path to the controller YAML (e.g. conf/controllers/xemm_lead_lag_btc_brl.yml)",
    )
    parser.add_argument("--password", required=True, help="Hummingbot config password")
    args = parser.parse_args()

    if not args.controller_config.exists():
        log.error(f"Controller config not found: {args.controller_config}")
        sys.exit(1)

    code = asyncio.run(run(args.controller_config, args.password))
    sys.exit(code)


if __name__ == "__main__":
    main()
