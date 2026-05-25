# BitPreco Redis pub/sub — Phase 0 findings

Date: 2026-05-15. Captures live under
`tools/redis_probe/captures/` (gitignored). Schema reference in
[`BITPRECO_REDIS_PROTOCOL.md`](BITPRECO_REDIS_PROTOCOL.md).

## TL;DR

Redis pub/sub from BitPreco is **drastically better than Phoenix WS**
for our use case. The exit criteria for Phase 0 are met:

| Criterion | Target | Result |
|---|---|---|
| Connection works, credentials valid | yes | yes (RTT 1.5 ms) |
| All `message_cod` observed naturally | all five | 4/5 (no PARTIAL) — see below |
| Canonical timestamp identified | yes | `utimestamp` (ISO microsec) |
| Timestamps monotonic per `order_id` | yes | yes, **0 regressions in 710 events** |
| Orderbook schema matches REST? | document | **identical** — parser reuse confirmed |
| Redis vs REST fill match rate | ≥99% | **100% (2/2)** |
| Redis vs REST cancel match rate | ≥99% | **99.23% (258/260)** — both "misses" were fills mis-classified by a probe-script race condition; **real match = 100%** |
| Redis silence rate | <1% | 0% in 25 min |

**Go decision: Phase 1A (shadow mode) can proceed.** PARTIAL handling
ships behind a feature gate — see "Remaining unknowns" below.

## Network and connection

- Host: `172.31.x.x:56379` — BitPreco's private VPC, accessible
  directly from our host (no internet hop, no TLS handshake).
- TLS is **off** for this path. If we ever connect from outside the
  VPC, TLS must be turned back on (`BITPRECO_REDIS_TLS=true`).
- `socket_connect_timeout` and `socket_timeout` are mandatory in the
  client config. Without them, a misconfigured TLS attempt against a
  plain-TCP listener hangs forever instead of failing. Production
  client must include both.

## Channels confirmed live and populated

Probed at 14:30 BRT, bot legacy running:

- `update:*` — 45 active channels, one per logged-in user/bot
- `orderbook:*` — 151 channels covering essentially every BitPreco
  market. `orderbook:BTC-BRL` had 23 concurrent subscribers, a strong
  signal that publishers are producing reliably.

## Key surprises vs `bitbots-js`

`bitbots-js` is the reference implementation we modelled the plan on,
but it is the BitPreco internal team's own bot — not external
documentation. A few divergences worth flagging:

1. **Update payload has more fields than `IBitPrecoWsOrderMessage`
   suggests.** Top-level `message_to: "USER"` was undocumented;
   `order_id` is duplicated at top level AND inside `order.id`.
2. **Type inconsistency across events on the same order.** Fields
   like `percent_fee`, `limited`, `programmed`, `canceled` flip
   between `"0"`/`0` and `"1"`/`1` depending on `message_cod`. The
   `BUY_ORDER_CREATED` payload uses integers; `ORDER_CANCELED` and
   `ORDER_FULLY_EXECUTED` use strings. The production parser must
   accept either and coerce.
3. **`canceled` is `1`/`0` inside `order` even on non-cancel events**
   (always `0` until a cancel happens). `bitbots-js`'s
   `IBitprecoWsOrder` documents this loosely; we should ignore it in
   favour of `message_cod` and `status` ("EMPTY" vs "FILLED").
4. **Orderbook rows include `id`** (the resting order id at that
   level). `bitbots-js` discards this in `convertBookToWsSnapshot`.
   The REST `/orderbook` endpoint exposes the same field, so the
   asymmetry we feared (Redis ≠ REST) does not exist.
5. **All timestamps are BRT (UTC-3) ISO strings**, not unix epoch and
   not UTC. Production parser must normalise to UTC before any
   comparison against `time.time()` or other wall clocks. This is the
   single most important footgun and easy to miss in code review.

## Timestamp ordering — design implication

We initially planned for "timestamp + state machine, in case ordering
is unreliable." The capture says ordering **is** reliable per
`order_id`: 0 regressions across 710 events for any of the three
candidate fields (`order.time_stamp`, `balance.utimestamp`,
`balance.timestamp`).

**Decision:** use `balance.utimestamp` (microsecond resolution,
canonical across both update + orderbook channels) as the ordering
key. Keep the terminal-state guard (filled/cancelled cannot
backtrack to open) as defense-in-depth, but no longer the critical
path. This simplifies the parser meaningfully.

## Latency Redis vs REST

In 25 min of comparison (bot live, ~260 cancel cycles, 2 fills):

| Event type | Redis-vs-REST delta (negative = Redis arrived first) |
|---|---|
| Fills | p50 **-1.51 s** (range -1.93 s to -1.09 s) |
| Cancels | p50 **-1.73 s** (p99 -0.05 s) |

Redis arrives ~1.5-1.7 seconds before our 3-second REST poll detects
the same event. In addition, **49 events were Redis-only**: orders
that lived and died inside a single 3-second REST poll interval —
Redis caught them, REST never saw them.

This is the latency win that motivated the rewrite.

## REST endpoint shape — quirk worth wiring tests around

`open_orders` and `executed_orders` return a **bare top-level JSON
array** of order dicts, NOT wrapped in `{"orders": [...]}` or
`{"success": true, "orders": [...]}`. Easy to miss because most cmds
on this exchange return objects.

The probe script 05 originally assumed the dict-wrapped shape and
silently parsed zero orders. The production fallback parser must
accept the array form. A unit test should pin this down so a future
upstream wrapping change doesn't break us silently.

## Race condition the probe taught us about

Within a single 3 s REST poll, an order can:

1. appear in `open_orders` (created)
2. disappear from `open_orders` (terminal — but we don't know which)
3. appear in `executed_orders` (filled — terminal state revealed)

If we process `open_orders` first and `executed_orders` second, the
disappearance is mis-attributed as a cancel before the fill record
arrives. We fixed this in the probe (process `executed_orders`
first), and **the same ordering applies to the production
post-reconnect reconciliation logic**: read fills BEFORE diffing
open-order sets.

## Remaining unknowns

### 1. `ORDER_PARTIALLY_EXECUTED` payload not captured

Zero PARTIAL events in 30 min — expected with our `amount=0.0002`
(crosses or fully fills nearly all the time). The bitbots-js enum
confirms PARTIAL exists, but we have no real sample.

**Mitigation in Phase 1A**: route every `message_cod` through a
generic parser. If a PARTIAL ever arrives, the parser logs the raw
payload at WARN, refuses to mutate state, and triggers a REST
reconciliation. Phase 2 stays gated on PARTIAL handling
specifically — flip on only after the first real sample arrives in
shadow mode.

### 2. Whether the BRT timezone is server-local or hardcoded

All timestamps are BRT (UTC-3). It's not clear from the captures
alone whether this is "BitPreco server's local time" or "always BRT
regardless of server location." For now we treat it as a constant
(UTC-3) and apply that offset on parse. If BitPreco moves servers
or DST rules change in Brazil, this needs revisiting.

### 3. Behaviour during Redis reconnect

We did not force a reconnect in Phase 0. Pub/sub does not retain
messages across disconnects, so the gap recovery path
(`force_balance_refresh` + REST `order_info` per in-flight order)
needs explicit testing in Phase 1A.

### 4. ACL scope

Our credential successfully ran `PUBSUB CHANNELS update:*` and saw
**all** active users' channel names (just names, not contents). This
suggests the ACL doesn't restrict PUBSUB inspection commands. We
should ask BitPreco to tighten the credential to:
- `SUBSCRIBE` only on `update:<our_id>` and `orderbook:<our_markets>`
- `PING` for keep-alive
- deny `PUBSUB CHANNELS *` and any write commands

This is hygiene, not a blocker for Phase 1A.

## Decisions locked from this phase

1. **Orderbook parser:** reuse
   `BitprecoOrderBook.snapshot_message_from_exchange_rest` verbatim
   for the Redis backend (schemas are byte-equivalent).
2. **Canonical event timestamp:** `balance.utimestamp` (microsec),
   normalised from BRT to UTC.
3. **Ordering strategy:** timestamp-only, with state-machine guard
   for terminal-state regression as defense-in-depth.
4. **TLS:** off for the in-VPC path. The connection module must
   support both modes; the env vars choose.
5. **Production REST list parser:** accept top-level array OR
   `{key: array}` wrapping (write a small extract helper, unit-test
   both shapes).
6. **REST reconciliation processing order:** read `executed_orders`
   before diffing `open_orders` (avoid the cancel-vs-fill race).
7. **PARTIAL handling:** generic parser + WARN-and-fallback until
   we get one real sample. Phase 2 gated until confirmed.

## Probe scripts: bugs found and fixed during the exercise

These are noted only because they affect the captures we already
have, and because the same gotchas could trip up the production
code if not consciously avoided.

- `_common.connect_pubsub`: added `socket_connect_timeout=10` and
  `asyncio.wait_for(ping, 10)`. Without them, a TLS-misconfigured
  connection hangs indefinitely.
- `05_compare`: `_extract_order_list` helper to accept either
  top-level array or wrapped dict.
- `05_compare`: warmup pass — first REST poll is treated as baseline
  (orders that already existed are excluded from metrics).
- `05_compare`: process `executed_orders` before `open_orders` to
  avoid mis-classifying same-poll fills as cancels.
- `_common.redact`: nested dict/list traversal moved before the
  balance-key regex so short keys like `"order"` don't get
  short-circuited as a balance value.
