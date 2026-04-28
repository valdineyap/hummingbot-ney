# PMM Lead-Lag Skew — Handoff para nova sessão de IA

> Este documento é um prompt de contexto para continuar o desenvolvimento do bot.
> Leia-o inteiro antes de fazer qualquer alteração no código.

---

## 1. O que é este projeto

Bot de Pure Market Making (PMM) para BTC-BRL na Binance Spot, implementado sobre
**Hummingbot V2** (`strategy_v2`, controllers + executors). O objetivo é capturar
spread bilateral com controle rígido de inventário, sem alpha direcional.

**Stack:** Python 3.11, Hummingbot V2, Pydantic v2, pytest.  
**Branch de desenvolvimento:** `claude/pmm-bot-lead-lag-skew-RXuZp`  
**Repositório:** `valdineyap/hummingbot-ney`  
**Working directory:** `/home/user/hummingbot-ney`

---

## 2. Estado atual — o que já foi implementado

**Todas as Fases 1–5 estão concluídas e testadas.** 130 testes passando.

| Fase | Commit | Conteúdo |
|------|--------|----------|
| 1 | `5d6b1efc` | PMM puro, bilateral, postOnly |
| 2 | `1adf8b08` | Inventário real, size factors, one-sided, CSV |
| 3 | `d1de1efb` | Skew por inventário, vol multiplier, freios de churn |
| 4 | `5a30246f` | Regime L1/L1.5/L2/L3, kill switch, session PnL |
| 5 | `8c3d4c9e` | Lead-lag micro pause + sintético fair_brl EWM |

**Fase 6 (paper trading + capital real) ainda não foi iniciada.**

---

## 3. Arquivos principais

```
controllers/market_making/
  pmm_lead_lag_skew.py        ← controller + config (PMMLeadLagSkewController)
  pmm_lead_lag_utils.py       ← funções puras testáveis (sem connectors/feeds)
  IMPL_PROGRESS.md            ← checklist detalhado por fase

scripts/
  v2_pmm_lead_lag.py          ← boot script com drawdown global de sessão

conf/controllers/
  conf_pmm_lead_lag_skew.yml  ← YAML de configuração atual

test/controllers/market_making/
  test_pmm_lead_lag_skew.py   ← 66 testes do controller
  test_pmm_lead_lag_utils.py  ← 64 testes das funções puras

docs/pmm_lead_lag/
  PLAN.md                     ← plano completo de implementação (1468 linhas)
  HANDOFF.md                  ← este arquivo
```

---

## 4. Como rodar os testes

```bash
cd /home/user/hummingbot-ney

# Todos os testes do projeto (130 testes, ~10s)
python -m pytest test/controllers/market_making/test_pmm_lead_lag_skew.py \
                 test/controllers/market_making/test_pmm_lead_lag_utils.py \
                 -v --tb=short

# Só utils (64 testes, funções puras)
python -m pytest test/controllers/market_making/test_pmm_lead_lag_utils.py -v

# Só controller (66 testes)
python -m pytest test/controllers/market_making/test_pmm_lead_lag_skew.py -v

# Um teste específico
python -m pytest test/controllers/market_making/test_pmm_lead_lag_skew.py \
                 -k "test_regime_normal" -v
```

**Os testes devem passar 100% antes de qualquer commit.**

---

## 5. Como rodar o bot em paper trading

### 5.1 Pré-requisitos

O Hummingbot deve estar instalado e configurado. Verificar:

```bash
# No ambiente Hummingbot (conda ou venv):
python -c "from hummingbot.strategy_v2.controllers.market_making_controller_base import MarketMakingControllerBase; print('OK')"
```

### 5.2 Configurar paper connector

No Hummingbot CLI:
```
create --controller-config conf/controllers/conf_pmm_lead_lag_skew.yml
```

Ou manualmente criar connector `paper_trade` com saldo inicial:
- BTC: 0.001 (≈ R$500 ao preço atual)
- BRL: 200

### 5.3 Iniciar o bot

```bash
# No Hummingbot CLI:
start --script v2_pmm_lead_lag.py --conf conf/scripts/conf_v2_pmm_lead_lag.yml
```

Após 2 minutos, verificar com `status`:
- Deve mostrar `Regime: normal`
- `inv_pct` próximo de 0.50
- 2 ordens de compra + 2 ordens de venda ativas

### 5.4 Verificar logs

```bash
# CSV de sinais (atualizado a cada tick ~1s):
tail -f logs/pmm_lead_lag/signals.csv

# Log principal do Hummingbot:
tail -f logs/hummingbot_logs.log | grep -E "(ERROR|WARNING|pmm_lead_lag)"
```

---

## 6. Configuração atual (parâmetros chave)

O arquivo `conf/controllers/conf_pmm_lead_lag_skew.yml` está configurado para
**Fase 4 baseline** (sem lead-lag ativo):

```yaml
w_lead: 0.0          # lead-lag desligado — ativar para 0.1 só após 1 semana de baseline
w_inv: 1.0
skew_max_bps: 2      # teto de 8; não subir sem evidência
total_amount_quote: 200   # R$200 — aumentar progressivamente após validação
executor_refresh_time: 60
```

**Para ativar Phase 5 (A/B test):** criar uma segunda instância com `w_lead: 0.1`
em YAML separado. NUNCA misturar capital entre as duas instâncias.

---

## 7. Próxima tarefa — Fase 6

A próxima tarefa é a **Fase 6: validação pré-capital**. Sequência obrigatória:

### 7.1 Semana 1 — Baseline Phase 4

- Rodar paper trading com `w_lead: 0.0` (config atual)
- Coletar métricas em `logs/pmm_lead_lag/signals.csv`:
  - `turnover` = volume / capital
  - `PnL líquido` = trading_pnl − fees
  - `max_drawdown`
  - `inventory_drift_stddev` = stddev de `inv_pct`
  - `adverse_fill_ratio_10s`

### 7.2 Semanas 2–3 — A/B Phase 4 vs Phase 5

- Segunda instância com `w_lead: 0.1` (Phase 5)
- Gate para aceitar Phase 5:
  - `adverse_fill_ratio_10s` cai ≥ 15%
  - volume de fills não cai > 15%
  - Significância: p < 0.05, Cohen's d > 0.3

### 7.3 Após A/B

1. Calibrar thresholds com p95 de vol observada
2. Dry-run R$200 real por 3 dias
3. Escala progressiva: 500 → 1k → 3k → alvo

### 7.4 Limitação conhecida da Phase 5

O sinal micro (`s_lead_micro`) usa fechamento de candle 1m via `_read_latest_close`,
então tem resolução de ~1min em vez de 1–5s. Para resolução real, seria necessário
uma subscrição bookTicker dedicada para BTC-USDT. Isso é um TODO da Fase 6.
Por ora, o componente mais ativo da Phase 5 é o `s_lead_regime` (sintético 30s–5min).

---

## 8. Arquitetura em resumo (para orientação rápida)

### Fluxo por tick

```
update_processed_data() — chamado ~1s
  1. Fetch mid BTC-BRL + balanços base/quote
  2. compute_inventory_state() → inv_pct, delta, size_factor_buy, size_factor_sell
  3. compute_vol_state()       → sigma_1m, vol_ratio, spread_multiplier
  4. _compute_lead_signals()   → LeadState(s_lead_micro, s_lead_regime, basis_bps, pause_buy, pause_sell)
  5. _evaluate_regime()        → "normal" | "degraded" | "safe" | "paused" | "killed"
  6. compute_skew_state()      → price_shift_bps (via w_inv*s_inv + w_lead*s_lead_regime)
  7. _csv_log_signals()        → logs/pmm_lead_lag/signals.csv

get_levels_to_execute()
  - Filtra sides_enabled (inventário/regime)
  - Filtra micro pause (_pause_buy_until / _pause_sell_until)

get_executor_config(level_id)
  - Aplica price_shift e spread_multiplier
  - Aplica size_factor pelo lado (buy ou sell)
  - _clamp_to_mid: bid nunca ≥ mid, ask nunca ≤ mid

executors_to_early_stop()
  - Paused/killed → cancela tudo
  - Safe → cancela levels além do primeiro por lado
  - force_requote_bps: cancela se preço defasou > 15 bps
  - §3.6: cancela se |lag_micro| ≥ spread_ativo E pause ativo
```

### Regime (máquina de estados)

```
normal → degraded: vol_ratio > 3×
degraded → safe:   L1 por > 120s, ou thrash ≥ 2
degraded → paused: vol_ratio > 5×, ou |basis_bps| > 30
paused → killed:   |inv_delta| > 0.40, ou session_pnl < -R$10, ou net_exposure > max
killed:            MANUAL restart obrigatório
```

### Convenção de sinal do skew

```python
price_shift_bps > 0  → ref cai → bids menos atrativos, asks mais atrativos → bot vende mais
# Quando: Δ > 0 (BTC a mais → quero vender)

price_shift_bps < 0  → ref sobe → bids mais atrativos, asks menos atrativos → bot compra mais
# Quando: Δ < 0 (BRL a mais → quero comprar)
```

---

## 9. Regras de desenvolvimento

### Antes de qualquer alteração

```bash
# 1. Verificar branch correto
git branch  # deve ser claude/pmm-bot-lead-lag-skew-RXuZp

# 2. Rodar testes — todos devem passar
python -m pytest test/controllers/market_making/ -v --tb=short

# 3. Ler o arquivo antes de editar (nunca assumir conteúdo)
# Use Read com offset+limit se o arquivo for grande
```

### Durante o desenvolvimento

- **Um bloco lógico por commit** (máx ~60 linhas por diff)
- **Testes primeiro** — adicionar teste antes de implementar função nova
- **Funções puras em `pmm_lead_lag_utils.py`** — nunca colocar lógica de negócio no controller diretamente
- **`Decimal` para preços/quantidades**, `float` apenas em math (tanh, std, log)
- **Nunca commitar com testes falhando**

### Ao terminar

```bash
# Push obrigatório para o branch designado
git push -u origin claude/pmm-bot-lead-lag-skew-RXuZp

# Atualizar IMPL_PROGRESS.md com o que foi feito
```

---

## 10. Referências rápidas

| O que | Onde |
|-------|------|
| Plano completo (arquitetura, fórmulas, roadmap) | `docs/pmm_lead_lag/PLAN.md` |
| Checklist de progresso por fase | `controllers/market_making/IMPL_PROGRESS.md` |
| Config Pydantic completa | `pmm_lead_lag_skew.py` linha ~42 |
| Funções puras (dataclasses + cálculos) | `pmm_lead_lag_utils.py` |
| YAML de configuração | `conf/controllers/conf_pmm_lead_lag_skew.yml` |
| Testes de utils | `test/controllers/market_making/test_pmm_lead_lag_utils.py` |
| Testes do controller | `test/controllers/market_making/test_pmm_lead_lag_skew.py` |
| Boot script | `scripts/v2_pmm_lead_lag.py` |

---

## 11. Perguntas frequentes

**Por que `w_lead: 0.0` no YAML atual?**  
Lead-lag (Phase 5) só deve ser ativado após 1 semana de baseline Phase 4 e com
evidência empírica de A/B. Ativar antes é sobrecarregar o sistema com sinal não validado.

**Por que `skip_rebalance: true`?**  
Rebalance MARKET em spot BRL usa taker (4 bps) e fura o breakeven bilateral (6 bps net).
Só rebalancear manualmente em emergência.

**O que é `force_requote_bps`?**  
Se o preço calculado para um nível difere > 15 bps do preço atual da ordem ativa,
cancela e recria imediatamente — não espera o `executor_refresh_time`. Protege contra
ordens presas em mercado rápido.

**O que é `min_requote_bps`?**  
No refresh natural (ao expirar `executor_refresh_time`), só recota se a diferença
de preço for ≥ 1 bps. Evita cancel/replace por variação de sinal minúscula.

**Por que o micro pause raramente dispara?**  
`_read_latest_close` usa candle 1m, então a resolução efetiva do sinal micro é ~1min,
não 1–5s. Para resolução real precisaria de bookTicker subscription dedicada para
BTC-USDT (TODO Phase 6).

**Como identificar se o bot está em regime paused/killed?**  
`to_format_status()` mostra `Regime: PAUSED (cause: ...)` no topo.
O CSV `signals.csv` tem colunas `regime` e `regime_cause` em cada linha.
Transições para killed geram log `CRITICAL`.
