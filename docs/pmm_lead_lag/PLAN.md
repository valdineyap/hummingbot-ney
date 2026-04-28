# PMM BTC-BRL com Lead-Lag Skew — Plano de Implementação

> Versão: revisão 6 — 2026-04-25
> Status: Fases 1–5 concluídas. Fase 6 (paper trading + capital real) em andamento.

## 0. Contexto

**Problema:** Operar um bot de Pure Market Making em BTC-BRL (Binance Spot) para maximizar volume de negociação aproveitando maker fee reduzida em VIP4, mantendo operação próxima de breakeven com controle rígido de inventário e risco.

**Premissas operacionais:**
- Capital limitado, sem co-location, um desenvolvedor, Binance VIP4.
- Maker fee ≈ 3 bps spot; taker ≈ 4 bps. Breakeven bilateral (ambas as pontas maker): capturar spread ≥ 6 bps net.
- Simplicidade operacional > otimização marginal de alpha.
- Lead-lag (BTC-USDT leader, BTC-BRL follower) é usado como ajuste **defensivo** de skew, não como estratégia de alpha direcional.
- Arquitetura Hummingbot V2 (`strategy_v2`), controllers + executors.

**Convenção:** Hummingbot V2 = framework fixo. Fase 1..6 = etapas incrementais do roadmap do bot.

---

## 1. Arquitetura

### 1.1 Componentes

| Tipo | Componente | Papel |
|------|-----------|-------|
| Config | `PMMLeadLagSkewConfig` | Todos os parâmetros via Pydantic + YAML |
| Controller | `PMMLeadLagSkewController` | Orquestra; herda `MarketMakingControllerBase` |
| Utils | `pmm_lead_lag_utils.py` | Funções puras testáveis (sem connectors) |
| Script | `v2_pmm_lead_lag.py` | Boot + drawdown global de sessão |
| Executor | `PositionExecutor` (hummingbot core) | Ciclo de vida de cada ordem maker |

### 1.2 Fluxo por tick

```
update_processed_data()
  ├── mid BTC-BRL, balanços base/quote
  ├── compute_inventory_state()  → inv_pct, delta, size_factors
  ├── compute_vol_state()        → sigma_1m, vol_ratio, spread_multiplier
  ├── _compute_lead_signals()    → LeadState (micro + regime)
  ├── _evaluate_regime()         → "normal"|"degraded"|"safe"|"paused"|"killed"
  ├── compute_skew_state()       → price_shift_bps
  └── _csv_log_signals()         → logs/pmm_lead_lag/signals.csv

get_levels_to_execute()
  ├── filtro sides_enabled (inventário/regime)
  └── filtro micro pause (lead-lag)

get_executor_config()
  ├── aplica price_shift e spread_multiplier
  ├── aplica size_factor por lado
  └── _clamp_to_mid (bid nunca ≥ mid, ask nunca ≤ mid)

executors_to_early_stop()
  ├── cancela todos se paused/killed
  ├── cancela deeper se safe
  ├── force_requote_bps: cancela se preço defasou > 15 bps
  └── §3.6: cancela se |lag_micro| ≥ spread_ativo E pause ativo
```

### 1.3 Arquivos do projeto

```
controllers/market_making/
  pmm_lead_lag_skew.py        ← controller + config principal
  pmm_lead_lag_utils.py       ← funções puras (sem connectors)
  IMPL_PROGRESS.md            ← checklist de progresso por fase

scripts/
  v2_pmm_lead_lag.py          ← boot script com drawdown global

conf/controllers/
  conf_pmm_lead_lag_skew.yml  ← YAML de configuração

test/controllers/market_making/
  test_pmm_lead_lag_skew.py   ← 66 testes do controller
  test_pmm_lead_lag_utils.py  ← 64 testes das funções puras

docs/pmm_lead_lag/
  PLAN.md                     ← este arquivo
  HANDOFF.md                  ← prompt de handoff para outra IA
```

---

## 2. Controle de Inventário

### 2.1 Definições

- `P` = mid BTC-BRL
- `Q` = valor total em BRL = `base * P + quote`
- `inv_pct` = `base * P / Q` (fração em BTC)
- `target_pct` = 0.5 (neutro)
- `Δ = inv_pct - target_pct` (positivo = BTC a mais)

### 2.2 Bandas

| Campo | Default | Regra |
|-------|---------|-------|
| `inv_soft_band` | 0.10 | Zona neutra — sem penalidade |
| `inv_hard_band` | 0.20 | Reduzir size adverso + bônus favorável |
| `inv_hard_cap`  | 0.30 | One-sided: desliga o lado adverso |
| `inv_kill`      | 0.40 | Kill switch L3 |

### 2.3 Size factors (3 zonas)

```
abs_dev = |Δ|
zona 1 (abs_dev ≤ soft):
    size_adversa = size_favoravel = 1.0

zona 2 (soft < abs_dev ≤ hard):
    t = (abs_dev - soft) / (hard - soft)
    size_adversa   = 1.0 - 0.8 * t   # linear → 0.2
    size_favoravel = min(1.0 + 0.5 * t, 1.5)

zona 3 (abs_dev > hard):
    size_adversa   = 0.0
    size_favoravel = 1.5
```

Mnemônico: "adversa" = lado que empurra inventário para fora da banda.
- Δ > 0 (BTC a mais) → BUY é adverso, SELL é favorável.
- Δ < 0 (BRL a mais) → SELL é adverso, BUY é favorável.

### 2.4 Histerese one-sided

- Entra em one-sided quando `|Δ| > inv_hard_cap` (0.30).
- Sai de one-sided quando `|Δ| < inv_hard_band` (0.20).
- Evita flip-flop na fronteira.

### 2.5 Hard cap absoluto em BRL

```
net_exposure_quote = base_balance * P
```
Se `net_exposure_quote > max_net_position_quote` → trigger L3 kill.

---

## 3. One-Sided Mode

`sides_enabled ∈ {{"buy","sell"}, {"buy"}, {"sell"}, ∅}`

**Triggers (Fase 2+):**
- Inventário além de `inv_hard_cap` → desliga o lado adverso.

**Retorno:** quando `|Δ| < inv_hard_band` (histerese).

**Lead-lag micro pause (Fase 5+):** NÃO mexe em `sides_enabled` global. Filtra levels individualmente em `get_levels_to_execute()` durante dwell de ~3s. Não cancela executores ativos exceto se `|lag_micro| ≥ spread_ativo` (critério §3.6).

---

## 4. Regime (Circuit Breakers)

| Nível | Estado | Ação | Reversão |
|-------|--------|------|----------|
| L1 | `degraded` | spread ×1.5, size ×0.5 | Automática |
| L1.5 | `safe` | 1 nível/lado, size ×0.25, spread ×2.5, skew=0 | Automática, dwell 2× |
| L2 | `paused` | sides_enabled=∅, cancela tudo | Automática, dwell 60s |
| L3 | `killed` | Para tudo, alerta operador | **Manual** |

### 4.1 Triggers por nível

**L1 Degraded:**
- `vol_ratio > vol_degraded_threshold_mult` (default 3.0×)

**L2 Paused:**
- `vol_ratio > vol_pause_threshold_mult` (default 5.0×)
- `|basis_bps| > pause_basis_bps` (default 30 bps)

**L1.5 Safe:**
- L1 ativo continuamente por > `safe_mode_entry_sec` (default 120s)
- 2 transições degraded↔paused em `safe_thrash_window_sec` (default 600s)

**L3 Kill:**
- `|Δ| > inv_kill` (default 0.40)
- `net_exposure_quote > max_net_position_quote`
- `session_pnl < -max_session_drawdown_quote`
- Erros críticos ≥ `critical_error_threshold`

---

## 5. Sistema de Skew

### 5.0 Dois horizontes de lead-lag

| Sinal | Fonte | Horizonte | Uso |
|-------|-------|-----------|-----|
| `s_lead_micro` | BTC-USDT puro | 1–5s | Pause defensivo por tick |
| `s_lead_regime` | Sintético `fair_brl = BTCUSDT × USDTBRL` | 30s–5min | Skew contínuo via `price_shift_bps` |

### 5.1 Modelo de preço

```
ref_adjusted = reference_price * (1 - price_shift_bps / 10000)
order_price  = ref_adjusted * (1 + side_multiplier * spread * spread_multiplier)
```

Convenção: `price_shift_bps > 0` → ref cai → bids menos atrativos, asks mais atrativos → bot vende mais (reduz BTC em excesso).

### 5.2 Sinais normalizados

```python
# Inventário
s_inv  = clip(Δ / inv_soft_band, -1, 1)

# Lead micro (BTC-USDT puro, sem FX delta)
lag_bps = (delta_usdt × usdt_brl_ref - delta_brl) / mid_brl × 1e4
if lag_bps > threshold: pause SELL por micro_dwell_sec
if lag_bps < -threshold: pause BUY por micro_dwell_sec

# Lead regime (sintético EWM-smoothed)
r_fair   = ln(fair_brl_smooth_t / fair_brl_smooth_{t-30s})
r_actual = ln(mid_brl_t / mid_brl_{t-30s})
lag      = r_fair - r_actual
z_lead   = lag / sigma_lag   # sigma adaptativo
s_lead   = -tanh(z_lead / 2) # negativo: defensivo (eleva ref qdo BRL < fair)

# Volatilidade (não direcional, só multiplica spread)
vol_ratio = sigma_1m / sigma_ref_6h
spread_multiplier = clip(vol_ratio, 1.0, 3.0)
```

### 5.3 Combinação

```
skew_raw        = w_inv * s_inv + w_lead * s_lead
skew_norm       = tanh(skew_raw)
price_shift_bps = skew_norm * skew_max_bps   # |shift| ≤ skew_max_bps
```

### 5.4 Pesos padrão

| Peso | Default | Quando aumentar |
|------|---------|-----------------|
| `w_inv` | 1.0 | Nunca reduzir |
| `w_lead` | 0.0 → 0.1 (Fase 5) | Apenas com evidência A/B de 2 semanas |
| `skew_max_bps` | 2 (Fase 3), teto 8 | Subir para 4 com evidência empírica |

### 5.5 Freios de quote churn

- `min_requote_bps` (default 1): não recota em refresh natural se delta < 1 bps.
- `force_requote_bps` (default 15): cancela e recria imediatamente se delta ≥ 15 bps.

### 5.6 Clamp de distância

Bid nunca ≥ mid. Ask nunca ≤ mid. Garantido em `get_executor_config()`.

---

## 6. Roadmap

### ✅ Fase 1 — PMM puro
Commits: `5d6b1efc`, `1992a90a`
Controller funcional sem skew, sem regime, sem lead-lag. 2 bids + 2 asks, postOnly.

### ✅ Fase 2 — Inventário + size factors + CSV
Commit: `1adf8b08`
Cálculo de `inv_pct` com balanços reais, size factors, one-sided, `signals.csv`.

### ✅ Fase 3 — Skew por inventário + volatilidade
Commit: `d1de1efb`
`price_shift_bps` via `s_inv`, `spread_multiplier` via vol, freios de churn, clamp.

### ✅ Fase 4 — Regime + kill switch
Commit: `5a30246f`
Máquina de estado L1/L1.5/L2/L3, dwell, thrashing detection, session_pnl.

### ✅ Fase 5 — Lead-lag (micro + regime sintético)
Commit: `8c3d4c9e`
`ewm_step()`, `compute_lag_micro()`, `compute_lag_regime()`, micro pause com dwell, §3.6 cancel, 3 pares de candles.

**Limitação conhecida:** `_read_latest_close` usa candle 1m → micro signal tem resolução de ~1m em vez de 1–5s. Para resolução real, precisa de bookTicker subscription dedicada para BTC-USDT (Phase 6 TODO).

### ⏳ Fase 6 — Validação pré-capital

**Sequência obrigatória:**
1. Paper trading Fase 4 baseline (w_lead=0.0), mínimo 1 semana
2. A/B: Fase 4 vs Fase 5 (w_lead=0.1), mínimo 2 semanas
3. Gate Fase 5: `adverse_fill_ratio_10s` cai ≥15%, fills não caem >15%
4. Calibração thresholds com p95 vol observada
5. Dry-run R$200 por 3 dias (confirmar postOnly, fees, precisão decimal)
6. Escala progressiva: 500 → 1k → 3k → alvo

---

## 7. Métricas de Validação

### Prioritárias (Fases 1–3)
1. `turnover` = volume / capital alocado
2. `PnL líquido` = trading_pnl − fees
3. `max_drawdown` da sessão
4. `inventory_drift_stddev` = stddev de `inv_pct` intradiário
5. `adverse_fill_ratio_10s` = fração de fills com mid adverso > 2 bps em 10s

### Adicionais (Fase 4+)
- `cancel_rate`, `force_requote_rate` — proxy de churn
- % do tempo em cada regime
- `edge_per_fill_bps` = (fill_price − mid) × sign(side)

### Critérios A/B
- Fase 2 → 3: `inventory_drift_stddev` cai ≥20% sem perder >10% de volume
- Fase 4 → 5: `adverse_fill_ratio_10s` cai ≥15% sem perder >15% de volume
- Significância: p < 0.05, Cohen's d > 0.3

---

## 8. Riscos e Mitigações

| Risco | Mitigação |
|-------|-----------|
| Overfitting | `w_lead` começa em 0; subir só com A/B; walk-forward validation |
| Latência 80–200ms | Janelas ≥ 30s para regime; micro pause é filtro local, sem API call |
| Feed USDT-BRL ruidoso | Deadband 3 bps + EWM halflife 10s no sintético |
| Flash crash BTC-BRL | Kill switch L3 por `inv_kill` + `max_session_drawdown` |
| VIP tier mudar | Monitorar fees mensalmente; sobrescrever via `client_config_map` |
| `skip_rebalance=true` | Inventário pode ficar enviesado; intervenção manual se necessário |
| Lead-lag piora fills | Toggle `w_lead=0` + monitorar `cancel_rate`; se subir >30% sem alpha → zerar |

---

## 9. Referências de Código

| Componente | Arquivo | Linhas relevantes |
|------------|---------|-------------------|
| MarketMakingControllerBase | `hummingbot/strategy_v2/controllers/market_making_controller_base.py` | 17-210, 223-315 |
| PositionExecutor | `hummingbot/strategy_v2/executors/position_executor/position_executor.py` | 427-750 |
| PMMSimple (molde) | `controllers/market_making/pmm_simple.py` | 1-32 |
| Script base | `scripts/v2_with_controllers.py` | 48-86 |
