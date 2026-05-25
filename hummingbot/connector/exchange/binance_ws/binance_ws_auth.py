"""Pure HMAC signing helper for Binance Spot WS-API requests.

WS-API signing differs from REST in three ways:

  1. Params are sorted **alphabetically** before being joined into the
     query string (REST keeps insertion order with ``urlencode``).
  2. Values are joined raw (``k=v``) without URL-encoding (REST percent-
     encodes via ``urllib.parse.urlencode``).
  3. ``apiKey`` is part of the signed payload, not a separate header.

``BinanceAuth.generate_ws_signature`` already implements (1) and (2)
in ``hummingbot/connector/exchange/binance/binance_auth.py:60-72``. This
module wraps it into a pure helper that:

  - Accepts a flat ``params`` dict.
  - Injects ``apiKey``, ``timestamp`` and ``signature`` in canonical
    order (alphabetical), returning a new dict ready for ``ws.send_json``.
  - Takes a callable ``time_provider`` so tests don't need to mock the
    clock.

Kept as a pure function (not a class) so the unit suite can pin its
output against golden HMAC vectors without instantiating
``BinanceAuth`` or any network plumbing.
"""
from __future__ import annotations

import hashlib
import hmac
from typing import Any, Callable, Dict


def sign_request_params(
    params: Dict[str, Any],
    api_key: str,
    secret: str,
    time_provider: Callable[[], float],
) -> Dict[str, Any]:
    """Return a new dict with ``apiKey``, ``timestamp`` and ``signature``
    appended, signed per Binance WS-API rules.

    The signature must be computed over the alphabetically-sorted
    ``apiKey``/``timestamp``/other-params payload BEFORE ``signature``
    itself is added — see the spec at
    https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/general-api-information#signed-request-example-trade

    ``time_provider`` is a no-arg callable returning seconds-since-epoch
    as a float. Production callers pass
    ``TimeSynchronizer.time`` (already maintained by the parent
    ``BinanceExchange``) so the timestamp is server-synced; tests pass
    a stub returning a fixed value for golden-vector comparisons.
    """
    timestamp_ms = int(time_provider() * 1e3)

    signed: Dict[str, Any] = dict(params)
    signed["apiKey"] = api_key
    signed["timestamp"] = timestamp_ms

    sorted_items = sorted(signed.items())
    payload = "&".join(f"{k}={v}" for k, v in sorted_items)
    signature = hmac.new(
        secret.encode("utf8"),
        payload.encode("utf8"),
        hashlib.sha256,
    ).hexdigest()

    signed["signature"] = signature
    return signed
