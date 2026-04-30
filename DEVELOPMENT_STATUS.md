# XEMM BRL Market Making — Development Status

**Branch**: `claude/hummingbot-brl-market-making-iuE1Z`
**Strategy**: XEMM (Cross-Exchange Market Making) for BTC-BRL with synthetic lead-lag signal
**As of**: 2026-04-30

---

## What was implemented

### Chunk 1 — `LeadLagSignalProvider` (DONE ✓)

**File**: `hummingbot/strategy_v2/utils/lead_lag_signal.py`
**Tests**: `test/hummingbot/strategy_v2/utils/test_lead_lag_signal.py` — **48 tests, all passing**
**Commit**: `ef0cd828`

Pure Python module (zero Hummingbot dependencies). Key classes:
- `CircularPriceBuffer` — FIFO price buffer with time-based eviction
- `EMAFilter` — exponential moving average for FX smoothing
- `FeedHealth` — staleness detection with optional `last_diff_uid` for real WS hang detection
- `SignalQuality` enum — OK / DEGRADED_FX / DEGRADED_LEADER / DEGRADED_LOCAL / BAD
- `LeadLagSignalProvider` — orchestrates the above; key distinction:
  - `fair_brl_fast`: uses raw FX mid (no EMA) → used for micro lead-lag windows (5/10/15s)
  - `fair_brl_slow`: uses EMA FX mid → used for basis/regime (stable, not reactive)
  - `lead_signal_bps(window_sec)` uses `fair_brl_fast` to avoid artificial lag

**Run signal tests** (no compile needed):
```bash
cd /home/user/hummingbot-ney
python -m pytest test/hummingbot/strategy_v2/utils/test_lead_lag_signal.py -v
```

---

### Chunk 2 — `XEMMBRLExecutor` (DONE ✓)

**File**: `hummingbot/strategy_v2/executors/xemm_executor/xemm_brl_executor.py`
**Tests**: `test/hummingbot/strategy_v2/executors/xemm_executor/test_xemm_brl_executor.py` — **5 tests** (requires `./compile`)
**Commit**: `a51ba8f2`

Subclass of `XEMMExecutor` that overrides only 2 methods:
- `create_maker_order()` → uses `OrderType.LIMIT_MAKER` instead of `OrderType.LIMIT`
- `validate_sufficient_balance()` → uses `OrderType.LIMIT_MAKER` for the maker candidate

**Why this matters**: With ~100–300ms home latency, a LIMIT order can cross the book at the exchange and execute as taker, doubling the fee. `LIMIT_MAKER` is rejected by the exchange if it would cross (fail-safe). Both Bybit and Binance Spot connectors support `LIMIT_MAKER`.

**Run executor tests** (requires Cython compile first):
```bash
cd /home/user/hummingbot-ney
./compile
python -m pytest test/hummingbot/strategy_v2/executors/xemm_executor/test_xemm_brl_executor.py -v
```

---

### Chunk 3 — `XEMMLeadLagController` (DONE ✓)

**File**: `controllers/generic/xemm_lead_lag.py` (738 lines)
**Tests**: `test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py` (574 lines, requires `./compile`)
**Example config**: `controllers/generic/xemm_lead_lag_example.yml`
**Commit**: `4cf5fc51`

Key components:
- `Regime` enum: WARMUP / OK / DEGRADED / PAUSED / KILLED
- `XEMMLeadLagConfig(ControllerConfigBase)`: Pydantic config with all parameters
- `XEMMLeadLagCSVLogger`: 38-column line-buffered CSV writer
- `XEMMLeadLagController(ControllerBase)`:
  - `update_processed_data()`: reads 8 L1 prices, computes signals, per-exchange inventory, targets, regime
  - `_compute_regime()`: state machine for all risk gates
  - `determine_executor_actions()`: Phase1-cancel / Phase2-cooldown / Phase3-create
  - `_compute_targets()`: adjusts BUY/SELL profitability by lead signal + inventory skew
  - Kill switch file support (`touch /tmp/xemm_lead_lag_pause` to pause)
  - Daily circuit breakers (max loss, max fills, consecutive hedge failures)

**Run controller tests** (requires Cython compile first):
```bash
cd /home/user/hummingbot-ney
./compile
python -m pytest test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py -v
```

---

## CRITICAL PENDING ISSUE — `XEMMBRLExecutor` wiring

**Problem**: The controller's `_make_create_action()` method creates `XEMMExecutorConfig(...)`, which the `ExecutorOrchestrator` maps to `XEMMExecutor` (uses `OrderType.LIMIT`), **not** `XEMMBRLExecutor` (uses `OrderType.LIMIT_MAKER`).

**Location**: `controllers/generic/xemm_lead_lag.py`, method `_make_create_action()` (around line 480–510).

**Must be fixed before live trading.**

### Solution options

**Option A (recommended)**: Create `XEMMBRLExecutorConfig` that inherits `XEMMExecutorConfig` and is registered in the executor registry to map to `XEMMBRLExecutor`.

Steps:
1. In `hummingbot/strategy_v2/executors/xemm_executor/data_types.py`:
   ```python
   class XEMMBRLExecutorConfig(XEMMExecutorConfig):
       type: str = "xemm_brl_executor"  # must match executor's type field
   ```
2. In `hummingbot/strategy_v2/executors/xemm_executor/xemm_brl_executor.py`, set the class attribute:
   ```python
   class XEMMBRLExecutor(XEMMExecutor):
       executor_type: str = "xemm_brl_executor"
   ```
3. Register in `hummingbot/strategy_v2/executor_handler.py` (or wherever executors are registered — check `ExecutorOrchestrator` for the dispatch map).
4. In `xemm_lead_lag.py`, import `XEMMBRLExecutorConfig` and use it in `_make_create_action()`.

**Option B (simpler but hackier)**: Override executor instantiation directly in the controller, bypassing the registry.

### How to find the executor registry

```bash
grep -rn "XEMMExecutor" hummingbot/strategy_v2/executor_handler.py
grep -rn "executor_type\|ExecutorFactory\|create_executor" hummingbot/strategy_v2/ --include="*.py" | head -30
```

---

## How to continue

### Step 1: Fix `XEMMBRLExecutor` wiring (CRITICAL)

Follow Option A above. The test to verify it works:
```bash
./compile
python -m pytest test/hummingbot/strategy_v2/executors/xemm_executor/test_xemm_brl_executor.py -v
python -m pytest test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py -v
```

After fixing, add a test to `test_xemm_lead_lag.py` (group E) that verifies `CreateExecutorAction.executor_config` is an instance of `XEMMBRLExecutorConfig`.

### Step 2: Run all tests

```bash
cd /home/user/hummingbot-ney
./compile
python -m pytest test/hummingbot/strategy_v2/utils/test_lead_lag_signal.py -v
python -m pytest test/hummingbot/strategy_v2/executors/xemm_executor/test_xemm_brl_executor.py -v
python -m pytest test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py -v
```

**Signal tests** (48) should already be green.
**Executor tests** (5) and **controller tests** require `./compile` for Cython extensions.

### Step 3: Validate config loading

Copy the example config:
```bash
cp controllers/generic/xemm_lead_lag_example.yml conf/controllers/xemm_lead_lag_btc_brl.yml
```

Validate Pydantic parsing:
```bash
python3 -c "
import yaml
from controllers.generic.xemm_lead_lag import XEMMLeadLagConfig

with open('conf/controllers/xemm_lead_lag_btc_brl.yml') as f:
    data = yaml.safe_load(f)
config = XEMMLeadLagConfig(**data)
print('id:', config.id)
print('markets:', config.update_markets({}))
print('shadow:', config.shadow_mode)
"
```

Expected output:
```
id: xemm_lead_lag_btcbrl_v1
markets: {'bybit': {'BTC-BRL'}, 'binance': {'BTC-BRL', 'BTC-USDT', 'USDT-BRL'}}
shadow: True
```

### Step 4: Import test inside Hummingbot

```bash
./compile
./start
```

In hummingbot prompt: `start --controller xemm_lead_lag_btc_brl.yml`

If import error occurs, trace it and fix before proceeding.

### Step 5: Shadow mode (48h)

Edit `conf/controllers/xemm_lead_lag_btc_brl.yml` — ensure `shadow_mode: true` (it already is by default).

Configure API keys for Bybit and Binance via the Hummingbot `connect` command.

Start the bot and monitor the CSV log:
```bash
tail -f logs/xemm_lead_lag/xemm_lead_lag_*.csv | column -t -s,
```

**What to look for** (see plan §10.3):
- `signal_quality == OK` in ≥99% of rows
- `basis_bps` typically −20 to +20 bps
- No lines showing order creation (shadow mode)
- CSV file growing ~1 line/second

### Step 6: Live (after 48h shadow analysis)

1. Analyze shadow CSV (see plan §10.4 for statistical analysis notebook)
2. Edit YAML: `shadow_mode: false`, keep `order_amount` at minimum
3. Restart bot
4. Monitor first 2h manually

---

## Architecture summary (for quick orientation)

```
Signal: Binance BTC-USDT × USDT-BRL (synthetic fair_BRL, never traded)
    ↓ lead_signal_bps (5/10/15s windows)
    ↓
XEMMLeadLagController (tick: 1s)
    ├── Regime: WARMUP → OK / DEGRADED / PAUSED / KILLED
    ├── _compute_targets() → target_buy, target_sell (adjusted by lead + inventory)
    └── determine_executor_actions()
            ├── StopExecutorAction (risk gates)
            └── CreateExecutorAction(XEMMBRLExecutorConfig)  ← PENDING FIX
                        ↓
                XEMMBRLExecutor
                    ├── Maker: Bybit BTC-BRL (LIMIT_MAKER)
                    └── On fill → Taker hedge: Binance BTC-BRL (MARKET)
```

**Key invariants**:
- `target_profitability` values are NET of fees (XEMMExecutor adds `_tx_cost_pct` internally)
- `LIMIT_MAKER` → rejected by exchange if would cross book (fail-safe for home latency)
- `find_rate("BRL-BRL")` returns `Decimal("1")` natively (no override needed)
- Shadow mode = full pipeline runs, zero orders placed

---

## Files added/changed in this branch

| File | Status | Description |
|------|--------|-------------|
| `hummingbot/strategy_v2/utils/lead_lag_signal.py` | NEW | Signal module (zero deps) |
| `hummingbot/strategy_v2/utils/__init__.py` | NEW | Package init |
| `test/hummingbot/strategy_v2/utils/__init__.py` | NEW | Test package init |
| `test/hummingbot/strategy_v2/utils/test_lead_lag_signal.py` | NEW | 48 signal tests |
| `hummingbot/strategy_v2/executors/xemm_executor/xemm_brl_executor.py` | NEW | LIMIT_MAKER subclass |
| `test/hummingbot/strategy_v2/executors/xemm_executor/test_xemm_brl_executor.py` | NEW | 5 executor tests |
| `controllers/generic/xemm_lead_lag.py` | NEW | Full controller (738 lines) |
| `controllers/generic/xemm_lead_lag_example.yml` | NEW | Config template |
| `test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py` | NEW | Controller tests |

No core Hummingbot files were modified.

---

## Detailed plan

See `xemm_brl_implementation_plan.md` in the repo root for the full 1361-line design document, including:
- All class/method signatures with pseudocode
- Complete test specifications (48 signal + 29 controller tests)
- Risk analysis (10 risks with mitigations)
- Shadow mode operation procedure
- Statistical analysis notebook for log validation
- Post-shadow criteria for go-live decision
