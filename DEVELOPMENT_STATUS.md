# XEMM Lead-Lag — Development Status

**Branch**: `claude/xemm-leadlag`
**Strategy**: XEMM (Cross-Exchange Market Making) BTC-BRL com sinal lead-lag sintético
**Atualizado em**: 2026-05-04

---

## Estado atual do bot

| Parâmetro | Valor |
|-----------|-------|
| **Rodando** | ✅ Sim — PID 11951 (iniciado 2026-05-04 08:15) |
| **Modo** | Live (`shadow_mode: false`) |
| **Maker** | Bybit BTC-BRL (LIMIT_MAKER) |
| **Taker/hedge** | Binance BTC-BRL (MARKET) |
| **Sinal** | Binance BTC-USDT × USDT-BRL |
| **min_profitability** | 7 bps NET |
| **target/max_profitability** | 15 bps NET |
| **Arbitragem pura** | Desligada (`enable_pure_arb: false`) |
| **Log** | `logs/logs_conf_xemm_lead_lag_shadow.log` |
| **CSV** | `logs/xemm_lead_lag/xemm_lead_lag_btcbrl_v1_*.csv` |

---

## O que está implementado

### 1. LeadLagSignalProvider ✅
**Arquivo**: `hummingbot/strategy_v2/utils/lead_lag_signal.py`

Módulo puro Python (zero dependências Hummingbot):
- `CircularPriceBuffer` — buffer FIFO com eviction por tempo
- `EMAFilter` — média móvel exponencial para suavizar FX
- `FeedHealth` — detecção de staleness com `last_diff_uid`
- `SignalQuality` — OK / DEGRADED_FX / DEGRADED_LEADER / DEGRADED_LOCAL / BAD
- `LeadLagSignalProvider` — orquestra tudo; `fair_brl_fast` (sem EMA, para lead windows) e `fair_brl_slow` (com EMA, para basis/regime)

---

### 2. XEMMLeadLagExecutor ✅
**Arquivo**: `hummingbot/strategy_v2/executors/xemm_executor/xemm_lead_lag_executor.py`

Subclasse de `XEMMExecutor` com 3 overrides:

**`validate_sufficient_balance()`** — usa `LIMIT_MAKER` no candidato maker (fee correta).

**`create_maker_order()`** — book-aware pricing:
- Tenta melhorar fila: `best_bid + tick` (BUY) ou `best_ask - tick` (SELL)
- Se spread = 1 tick: join em vez de improve
- Floor de profitabilidade: `min_profitability + placement_profitability_buffer` (histerese)
- Lead-aware adjustment: ±`placement_lead_aware_delta_bps` quando lead forte fora da dead zone
- Arredondamento direcional: `ROUND_DOWN` (BUY), `ROUND_UP` (SELL)
- Guard pós-arredondamento: nunca cruza o livro
- Fallback para `_maker_target_price` se livro inválido

**`control_shutdown_process()`** — guard contra `maker_order is None` ou `taker_order is None` (bug na base XEMMExecutor linha 226 que causava `'NoneType'.is_done`).

---

### 3. XEMMLeadLagExecutorConfig ✅
**Arquivo**: `hummingbot/strategy_v2/executors/xemm_executor/data_types.py`

Campos extras além do `XEMMExecutorConfig` base:
- `placement_profitability_buffer` — histerese no placement
- `lead_signal_bps` — injetado pelo controller a cada tick
- `placement_lead_aware_delta_bps` — ajuste quando lead é informativo
- `placement_lead_signal_threshold_bps` — dead zone

---

### 4. LeadLagArbitrageExecutor ✅
**Arquivo**: `hummingbot/strategy_v2/executors/arbitrage_executor/lead_lag_arbitrage_executor.py`

Subclasse de `ArbitrageExecutor` para arbitragem pura taker:taker:
- Override `process_order_failed_event()` — detecta falha parcial (1 perna ok, outra falhou)
- `_unwind_position()` — MARKET inverso na exchange com menor slippage
- `_estimate_unwind_slippage()` — calcula slippage em bps via VWAP
- Gate de slippage: `arb_max_unwind_slippage_bps` antes de desunwinding
- Estratégias: `abort_and_alert` (default) ou `force_unwind`
- CloseTypes: `UNWOUND` (sucesso) e `UNWIND_ABORTED` (gate acionado)

---

### 5. LeadLagArbitrageExecutorConfig ✅
**Arquivo**: `hummingbot/strategy_v2/executors/arbitrage_executor/data_types.py`

```python
class LeadLagArbitrageExecutorConfig(ArbitrageExecutorConfig):
    type: Literal["lead_lag_arbitrage_executor"]
    arb_max_unwind_slippage_bps: Decimal = Decimal("50")
    arb_unwind_strategy: Literal["abort_and_alert", "force_unwind"] = "abort_and_alert"
```

---

### 6. Registro no ExecutorOrchestrator ✅
**Arquivo**: `hummingbot/strategy_v2/executors/executor_orchestrator.py`

`"lead_lag_arbitrage_executor": LeadLagArbitrageExecutor` no mapa de dispatch.

---

### 7. CloseType expandido ✅
**Arquivo**: `hummingbot/strategy_v2/models/executors.py`

`UNWOUND = 11` e `UNWIND_ABORTED = 12` adicionados ao enum.

---

### 8. XEMMLeadLagController ✅
**Arquivo**: `controllers/generic/xemm_lead_lag.py`

Features implementadas:

| Feature | Detalhe |
|---------|---------|
| `Regime` enum | WARMUP / OK / DEGRADED / PAUSED / KILLED |
| Tiered polling 200ms | `update_interval=0.2`; fingerprint skip quando livro inalterado |
| Fingerprint | 8 L1 prices + `balance_version` (incrementado em fills) |
| Tier 3 throttle | CSV write máximo 1×/segundo |
| Kill switch | `touch /tmp/xemm_lead_lag_pause` → KILLED + cancel imediato |
| Lead-lag targets | `_compute_targets()` — BUY/SELL ajustados por sinal + inventory skew |
| Basis-aware skew | `basis_skew_strength` — bias direcional por basis persistente |
| Placement buffer | `placement_profitability_buffer` — histerese para evitar requote imediato |
| Lead-aware placement | `lead_signal_bps` injetado no config do executor a cada tick |
| Taker balance gates | `min_taker_base_for_sell_hedge`, `min_taker_quote_for_buy_hedge` |
| Anti-churn | `min_requote_interval_sec` — cooldown entre criações de ordens |
| Circuit breakers MM | `max_daily_loss_quote`, `max_consecutive_hedge_failures` |
| Switches independentes | `enable_market_making` / `enable_pure_arb` |
| Arb detection (VWAP) | `_compute_arb_gross_bps()` — usa VWAP para `arb_order_amount`, não L1 |
| Arb circuit breakers | `arb_failure_pause_sec`, `arb_max_failures_per_day`, `arb_daily_loss_limit_quote` |
| Arb rate limiting | `arb_max_per_hour`, `arb_min_interval_sec` |
| Arb capital check | `_has_capital_for_arb()` — verifica saldo livre na hora do spawn |
| Arb atomicidade | `_has_active_arb_executor()` — nunca 2 arbs simultâneos |
| Arb lead-aware | `_arb_threshold()` — agressivo quando lead favorece, conservador quando contradiz |
| Reset diário | Contadores de arb zerados meia-noite UTC |

---

### 9. Startup/Shutdown ✅
**Arquivo**: `controllers/generic/xemm_lead_lag.py` (seção "Startup / shutdown helpers")

- `_cancel_all_open_orders_on_startup()` — roda 1×, quando ambos connectors `.ready`
- `_bybit_cancel_open_orders()` — `GET /v5/order/realtime` → `POST /v5/order/cancel` por ordem
- `_binance_cancel_open_orders()` — `DELETE /openOrders` (batch); trata HTTP 400 `-2011` como "sem ordens"
- Sem dependência do estado SQLite do Hummingbot — consulta a exchange diretamente

---

### 10. Script de operação ✅
**Arquivo**: `start_xemm_lead_lag.sh`

- Shutdown gracioso: kill switch → wait 12s → SIGTERM → SIGKILL
- Rotação de log: arquivo anterior renomeado com timestamp UTC
- Confirmação automática: aguarda até 60s pela primeira ordem maker, imprime PID e detalhe

---

### 11. SIGTERM/SIGINT handler ✅
**Arquivo**: `controllers/generic/xemm_lead_lag.py`

- `_setup_sigterm_handler()` — registrado no primeiro tick via `loop.add_signal_handler()`; idempotente
- `_graceful_shutdown(sig_name)` — cancela todas as ordens abertas (REST direto) com timeout de 8s, depois `sys.exit(0)`
- Protege contra ordens órfãs quando o SO/usuário derruba o processo (reboot de servidor, CTRL-C, systemd stop)
- Tolera `NotImplementedError` (Windows) sem crashar

---

### 12. Correção `_has_inflight_activity()` ✅
**Arquivo**: `controllers/generic/xemm_lead_lag.py`

Bug anterior: verificava `not ex.is_done` para executores ativos e `not o.is_done` para in_flight_orders — ambos sempre `True` durante operação normal, bloqueando 100% das ações de drift da auditoria.

Correção: a única verificação legítima de "in-flight" é fill recente (`_last_fill_time < 10s`). Executores ativos com ordens maker abertas são esperados e não indicam trade em andamento.

---

### 13. Inventory auto_rebalance com VWAP ✅
**Arquivo**: `controllers/generic/xemm_lead_lag.py`

Quando `on_drift_action: "auto_rebalance"` e drift é detectado:
- `_execute_pending_rebalances()` — avalia ambas exchanges como candidatas
- `_rebalance_evaluate_exchange()` — usa `_vwap_for_amount()` (caminha o livro real via `get_vwap_for_volume`) como proxy de profundidade
  - SELL: escolhe exchange com VWAP **maior** (melhor preço de venda)
  - BUY: escolhe exchange com VWAP **menor** (melhor preço de compra)
  - Capital check: base ≥ amount (SELL) ou quote ≥ amount × vwap × 1.01 (BUY)
  - Book muito raso (VWAP None/0) → descarta candidato automaticamente
- Cooldown de 120s entre rebalances do mesmo asset (evita duplicatas antes do fill propagar)
- No boot com drift + `auto_rebalance`: NÃO seta `_kill_reason`, bot sai de `boot_paused` e começa a operar enquanto rebalance é colocado
- Config: `on_drift_action: "auto_rebalance"` em `xemm_lead_lag_btc_brl.yml`

---

## O que está pendente (rollout)

### Passo seguinte: validar detecção de arb no CSV

O bot **já calcula** `arb_long_gross_bps` e `arb_short_gross_bps` a cada tick e grava no CSV — mesmo com `enable_pure_arb: false`. Basta analisar os dados para validar se as oportunidades detectadas são reais.

```bash
# Ver colunas de arb no CSV atual
head -1 logs/xemm_lead_lag/xemm_lead_lag_btcbrl_v1_*.csv | tr ',' '\n' | grep -n arb

# Ver distribuição de spread detectado (últimas 1000 linhas)
tail -1000 logs/xemm_lead_lag/xemm_lead_lag_btcbrl_v1_*.csv | \
  awk -F, 'NR>1 {print $COL_ARB_LONG}' | sort -n | uniq -c
```

### Rollout faseado restante

| Step | Descrição | Status |
|------|-----------|--------|
| 6 | Analisar CSV arb (24h de dados, `enable_pure_arb=false`) | **próximo** |
| 7 | Live arb com `order_amount` mínimo (`0.00005`, `max/hora=3`) | pendente |
| 8 | Calibrar `arb_max_per_hour` e `arb_order_amount` com base no hit rate | pendente |

---

## Arquitetura resumida

```
Sinal: Binance BTC-USDT × USDT-BRL  →  fair_BRL  →  lead_signal_bps (5/10/15s)
                                                            ↓
                                    XEMMLeadLagController (tick: 200ms)
                                        ├── Regime: WARMUP→OK/DEGRADED/PAUSED/KILLED
                                        ├── _compute_targets() → target_buy, target_sell
                                        ├── MM path → CreateExecutorAction(XEMMLeadLagExecutorConfig)
                                        │               ↓
                                        │       XEMMLeadLagExecutor
                                        │           ├── Maker: Bybit BTC-BRL (LIMIT_MAKER)
                                        │           └── On fill → Hedge: Binance BTC-BRL (MARKET)
                                        │
                                        └── Arb path → CreateExecutorAction(LeadLagArbitrageExecutorConfig)
                                                        ↓ (enable_pure_arb=true)
                                                LeadLagArbitrageExecutor
                                                    ├── Buy: exchange com preço menor (MARKET)
                                                    ├── Sell: exchange com preço maior (MARKET)
                                                    └── On partial fail → _unwind_position()
```

**Invariantes críticos**:
- `target/min/max_profitability` são NET de fees (executor adiciona `_tx_cost_pct` internamente)
- `LIMIT_MAKER` é rejeitado pela exchange se cruzaria o livro (fail-safe para latência doméstica)
- `arb_min_profitability` é **GROSS** — o executor `ArbitrageExecutor` desconta fees; não há dupla dedução
- Startup cleanup consulta a exchange diretamente (não confia no SQLite do Hummingbot)
