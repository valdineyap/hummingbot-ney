# BitPreco Redis pub/sub — Phase 0 exploration

These scripts characterise BitPreco's internal Redis pub/sub feed
before we wire it up as a production backend for the connector. They
run as **passive subscribers in parallel with the legacy bot** —
pub/sub is fan-out, so we observe the exact same events the bot would
see if it were already on Redis, with zero interference.

The plan they implement lives in
`.claude/plans/golden-crafting-candle.md`.

## Why this exists

Phoenix WS to `bp-channels.gigalixirapp.com` is empirically broken for
API users — heartbeats work but `flash` events are essentially never
pushed (2 events in 6h51m of production, vs ~500 expected). The
internal BitPreco bots (`~/bp-research/bitbots-js`) use Redis pub/sub
directly via `ioredis`. We want to do the same, but first we need to
empirically confirm:

- the actual schema of each `message_cod` (`bitbots-js` may be stale)
- whether payloads carry a usable timestamp, and whether it is
  monotonic per `order_id`
- whether the orderbook payload matches the REST `/orderbook` shape
  (decides whether we can reuse the existing snapshot parser)
- the loss rate Redis-vs-REST (must be < 1% to justify switching —
  market making is intolerant of missed fills)
- the latency Redis-vs-REST (the whole point of the switch)

## Prerequisites

1. Credentials in `.env` (template in `.env.example`):
   - `BITPRECO_REDIS_HOST`
   - `BITPRECO_REDIS_PORT` (default 56379)
   - `BITPRECO_REDIS_PASSWORD`
   - `BITPRECO_REDIS_TLS=true`, `BITPRECO_REDIS_TLS_VERIFY=true`
   - `BITPRECO_USER_ID` (filled in by script 04)

2. Python deps already satisfied — `redis>=5.0` is installed in the
   `hummingbot` conda env (verified `redis==7.4.0`).

3. The legacy bot should be running (Phoenix/REST mode) for scripts 02
   / 03 / 05 to see real production traffic.

## Running

```bash
conda activate hummingbot

# 1. Sanity: PING + visible channels
python tools/redis_probe/01_connect.py

# 4. Fetch the numeric user id (idBPBot). Paste into .env.
python tools/redis_probe/04_get_user_id.py

# Re-source .env so BITPRECO_USER_ID is set:
set -a; source .env; set +a

# 2. Capture private order/balance events while bot is live (≥1 session)
python tools/redis_probe/02_capture_update.py --duration 7200

# 3. Capture orderbook snapshots (≥15 min)
python tools/redis_probe/03_capture_orderbook.py --duration 900 \
    --market BTC-BRL

# 5. Compare Redis vs REST live (≥1 hour, must run alongside script 02)
python tools/redis_probe/05_compare_rest_vs_redis.py --duration 3600
```

Each capture script writes to `captures/<script>-YYYYMMDD-HHMM.jsonl`
and a sibling `.report.json` with auto-derived statistics.

## Captures handling — DO NOT COMMIT

`captures/` is in `.gitignore`. Raw payloads contain `order_id`,
`user_id`, exact balances, exact prices. Never commit, never paste
into chat / PRs / commit messages.

Documents published under `docs/` (BITPRECO_REDIS_PROTOCOL.md,
BITPRECO_REDIS_FINDINGS.md) must use **redacted examples**: replace
`order_id`/`user_id` with placeholders (`<ORDER_ID>`, `<USER_ID>`),
mask absolute balance values, scrub anything that ties a specific
account to a specific trade.

The helper `_common.redact(payload)` in `_common.py` does this
mechanically. Apply before copying any example into a doc.

File permissions: scripts write with `umask 0077` so JSONLs land as
`0600`.

## Reading the reports

`<script>.report.json` includes:

- **schema-discovery**: every JSON field seen across all captured
  payloads, with type and a sample value. Surprises vs `bitbots-js`
  show up here.
- **timestamp candidates**: every field that looks like a timestamp
  (name contains time/ts/date OR numeric value parses as epoch). For
  each: detected format (unix s / unix ms / ISO), and whether values
  are monotonic per `order_id`. Pick the canonical field from this
  output.
- **per-message_cod counts** (script 02)
- **inter-event latency p50/p99** (scripts 02, 03)
- **Redis-vs-REST match rate** (script 05)

## Exit criteria for Phase 0

Documented in `.claude/plans/golden-crafting-candle.md` § "Critério
para fechar Fase 0". Briefly:

- ≥1 capture of every observed `message_cod` during ≥1 bot session
- ≥15 min continuous orderbook capture
- canonical timestamp field identified + monotonicity confirmed (or
  fallback to pure state machine documented)
- Redis-vs-REST fill loss < 1%
- orderbook Redis vs REST schema comparison documented
- `docs/BITPRECO_REDIS_PROTOCOL.md` + `BITPRECO_REDIS_FINDINGS.md`
  published with redacted examples
