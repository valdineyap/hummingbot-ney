# BitPreco Redis pub/sub — protocol reference

Phase 0 finding from `tools/redis_probe/`, captured 2026-05-15 in
production (bot running normally, BTC-BRL trading). All identifiers
and absolute values redacted (`<ORDER_ID>`, `~1eN` magnitudes) — raw
captures live under `tools/redis_probe/captures/` (gitignored).

## Connection

| Field | Value |
|---|---|
| Host (internal VPC) | `172.31.x.x` |
| Port | `56379` (non-standard, AWS ElastiCache convention) |
| TLS | **off** — Redis is reachable directly inside BitPreco's VPC |
| Auth | `AUTH <password>` (single password, no ACL username in our cred) |
| DB | `0` |
| Ping RTT | ~1.5 ms |

The connection lives entirely inside BitPreco's private VPC. From any
host with VPC routing in place no TLS is needed and the network round
trip is sub-millisecond. If we ever connect from outside the VPC, TLS
must be turned back on (`BITPRECO_REDIS_TLS=true`).

## Channels visible to our credential

```
PUBSUB CHANNELS update:*     -> 45 channels (one per active user_id)
PUBSUB CHANNELS orderbook:*  -> 151 channels (one per market)
```

Subscribers per probed channel at capture time:

| Channel | Subscribers |
|---|---|
| `update:<our_id>` | 1 (the legacy bot) |
| `orderbook:BTC-BRL` | 23 |
| `orderbook:ETH-BRL` | 14 |
| `orderbook:USDT-BRL` | 23 |

## Channel: `update:<idBPBot>` — private order + balance stream

One message per state change on any order owned by the user. Every
message carries:

- order info (the full order row at the time of the event)
- a balance snapshot for ALL currencies the user holds
- a discrete `message_cod` describing the transition

### `message_cod` enum (observed in 30 min of production trading)

| `message_cod` | Count in 30min | Notes |
|---|---|---|
| `BUY_ORDER_CREATED` | 178 | emitted right after a `cmd=buy` REST call succeeds |
| `SELL_ORDER_CREATED` | 177 | emitted right after a `cmd=sell` REST call succeeds |
| `ORDER_CANCELED` | 351 | emitted on full cancel (incl. partial-fill-then-cancel) |
| `ORDER_FULLY_EXECUTED` | 4 | fill closing the order |
| `ORDER_PARTIALLY_EXECUTED` | **0** | not observed; raw partial fills are rare with our small order size |

`ORDER_PARTIALLY_EXECUTED` is part of the `bitbots-js` `EBitprecoWsPossibleOrderMessageCods` enum so its existence is confirmed. The exact payload shape is not yet captured — see FINDINGS.

### Payload examples (redacted)

### BUY_ORDER_CREATED

```json
{
  "success": true,
  "message_to": "USER",
  "message_cod": "BUY_ORDER_CREATED",
  "order_id": "<ORDER_ID>",
  "order": {
    "market": "BTC-BRL",
    "type": "BUY",
    "amount": "~1e-4",
    "exec_amount": 0,
    "status": "EMPTY",
    "price": "~1e5",
    "cost": "~0",
    "fee": "~0",
    "canceled": "~0",
    "time_stamp": "2026-05-15 11:38:52",
    "limited": "~1e0",
    "programmed": 0,
    "percent_fee": 0,
    "id": "<ID>"
  },
  "balance": {
    "success": true,
    "BTC": "~1e-4",
    "BTC_locked": 0,
    "BRL": "~1e2",
    "BRL_locked": 79.94,
    "utimestamp": "2026-05-15 11:38:52.112213",
    "timestamp": "2026-05-15 11:38:52"
  }
}
```

### ORDER_CANCELED

```json
{
  "success": true,
  "message_to": "USER",
  "message_cod": "ORDER_CANCELED",
  "order_id": "<ORDER_ID>",
  "order": {
    "id": "<ID>",
    "market": "BTC-BRL",
    "type": "BUY",
    "status": "EMPTY",
    "amount": "~1e-4",
    "price": "~1e5",
    "exec_amount": 0,
    "cost": "~0",
    "fee": "~0",
    "percent_fee": "0",
    "limited": "~1e0",
    "programmed": "0",
    "canceled": "~1e0",
    "time_stamp": "2026-05-15 11:38:25",
    "tag": null,
    "obs": null
  },
  "balance": {
    "success": true,
    "BTC": "~1e-4",
    "BTC_locked": 0,
    "BRL": "~1e2",
    "BRL_locked": 0,
    "utimestamp": "2026-05-15 11:38:50.113011",
    "timestamp": "2026-05-15 11:38:50"
  }
}
```

### ORDER_FULLY_EXECUTED

```json
{
  "success": true,
  "message_to": "USER",
  "order_id": "<ORDER_ID>",
  "order": {
    "id": "<ID>",
    "market": "BTC-BRL",
    "type": "BUY",
    "status": "FILLED",
    "amount": "~1e-4",
    "price": "~1e5",
    "exec_amount": 0.0002,
    "cost": "~1e1",
    "fee": "~0",
    "percent_fee": "0",
    "limited": "~1e0",
    "programmed": "0",
    "canceled": "~0",
    "time_stamp": "2026-05-15 11:39:20",
    "tag": null,
    "obs": null
  },
  "message_cod": "ORDER_FULLY_EXECUTED",
  "balance": {
    "success": true,
    "BTC": "~1e-4",
    "BTC_locked": 0,
    "BRL": "~1e2",
    "BRL_locked": 0,
    "utimestamp": "2026-05-15 11:39:30.924187",
    "timestamp": "2026-05-15 11:39:30"
  }
}
```

### Field reference — `order`

| Field | Type | Notes |
|---|---|---|
| `id` | string (numeric) | exchange order id; duplicated at top-level as `order_id` |
| `market` | string | e.g. `"BTC-BRL"` |
| `type` | string | `"BUY"` or `"SELL"` |
| `status` | string | `"EMPTY"` (open / cancelled) or `"FILLED"` |
| `amount` | number | base-asset amount requested |
| `exec_amount` | number | base-asset amount filled so far |
| `price` | number | limit price (0 on market orders, not observed) |
| `cost` | number | quote-asset cost of fills so far (price × exec_amount) |
| `fee` | number | quote-asset fee paid (always 0 in our captures) |
| `percent_fee` | string or number | inconsistent type — sometimes `"0"`, sometimes `0` |
| `limited` | string or number | `"1"` on CANCELED, `1` on CREATED — inconsistent type |
| `programmed` | string or number | always `"0"` / `0` in captures |
| `canceled` | int | `1` after ORDER_CANCELED, `0` otherwise |
| `time_stamp` | ISO string | creation time, `"YYYY-MM-DD HH:MM:SS"` in **BRT (UTC-3)** |
| `tag`, `obs` | nullable string | unused in our captures |

⚠️ Several fields have **inconsistent types between events for the same
order** (`percent_fee`, `limited`, `programmed`, `canceled`). Production
parser must accept either int or string and coerce.

### Field reference — `balance`

Per-currency keys with `<CCY>` and `<CCY>_locked` for every currency
the user holds. In our case: `BTC`, `BTC_locked`, `BRL`, `BRL_locked`.
Locked = sum reserved by open orders.

| Field | Type | Notes |
|---|---|---|
| `<CCY>` | number | available |
| `<CCY>_locked` | number | locked in open orders |
| `success` | bool | always `true` in captures |
| `utimestamp` | ISO microsec string | publish-time, BRT |
| `timestamp` | ISO seconds string | redundant; same instant, less precision |

### Timestamps — monotonicity

| Field path | Count | Regressions per order | Verdict |
|---|---|---|---|
| `order.time_stamp` | 710 | **0** | safe to order on |
| `balance.utimestamp` | 710 | **0** | safe to order on, canonical |
| `balance.timestamp` | 710 | **0** | safe, redundant |

All three fields are strictly monotonic per `order_id` over 30 min of
trading. We can rely on timestamp-based ordering in the production
parser. **State machine guards stay as defense-in-depth, not the
critical path.**

All timestamps are in **BRT (UTC-3)** as ISO strings, not unix epoch.
The production parser must normalise to UTC before comparing with the
local clock.

## Channel: `orderbook:<market>` — public order book stream

Full snapshot (top 100 levels both sides) republished at ~1.4 Hz.
Inter-event p50 628 ms, p99 1.78 s, max 2.24 s.

### Payload example (redacted, BTC-BRL)

```json
{
  "success": true,
  "bids": [
    { "amount": "~1e-2", "price": "~1e5", "id": "<ID>" },
    { "amount": "~1e-4", "price": "~1e5", "id": "<ID>" },
    { "amount": "~1e-2", "price": "~1e5", "id": "<ID>" },
    "... (97 more rows)"
  ],
  "asks": [
    { "amount": "~1e-2", "price": "~1e5", "id": "<ID>" },
    { "amount": "~1e-3", "price": "~1e5", "id": "<ID>" },
    { "amount": "~1e-2", "price": "~1e5", "id": "<ID>" },
    "... (97 more rows)"
  ],
  "utimestamp": "2026-05-15 11:38:45.891789",
  "timestamp": "2026-05-15 11:38:45"
}
```

### Schema vs REST `/{market}/orderbook`

Schema discovery on 1257 Redis snapshots vs one REST snapshot
captured at the same moment: **11/11 shared fields, 0 redis-only,
0 rest-only.** The two payloads are byte-equivalent in structure.

→ The production parser can reuse
`BitprecoOrderBook.snapshot_message_from_exchange_rest` verbatim
for the Redis backend.

### Bid/ask row fields

| Field | Type | Notes |
|---|---|---|
| `price` | number | quote / base |
| `amount` | number | base-asset quantity at this level |
| `id` | string (numeric) | id of the resting order at this level — L3 info BitPreco exposes that we currently aggregate away. Not used by the L2 parser. |

### Volumetrics

| Metric | Value |
|---|---|
| Snapshots per 15 min | 1257 |
| Inter-event latency p50 | 628 ms |
| Inter-event latency p99 | 1.78 s |
| Inter-event latency max | 2.24 s |
| Snapshot byte size p50 | 10.9 KB |
| Snapshot byte size max | 10.97 KB |
| Levels per side | 100 (always) |

## REST endpoints used by the production connector for reconciliation

These are NOT part of the Redis protocol — listed for cross-reference
since the Phase 1A shadow observer and Phase 2 fallback both call them.

### `cmd=open_orders`

POST to `<REST_URL>` with `{"cmd": "open_orders", "auth_token": "<token>"}`.

**Response**: bare top-level JSON array of order dicts. Same fields as
the `order` block in update messages.

⚠️ Not `{"orders": [...]}` — direct array. Parser must handle both
just in case (BitPreco may change this).

### `cmd=executed_orders`

Same call pattern; returns historical filled orders. Each row has a
`concluded` field (ISO string, fill time) in addition to the standard
`order` fields.

### `cmd=get_user_id`

Returns the numeric `idBPBot` we use to build the
`update:<id>` channel name.
