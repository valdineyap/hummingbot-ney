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

**Todas as Fases 1–5 estão concluídas e testadas. Fase 6 (paper trade) iniciada.** 150 testes passando.

| Fase | Commit | Conteúdo |
|------|--------|----------|
| 1 | `5d6b1efc` | PMM puro, bilateral, postOnly |
| 2 | `1adf8b08` | Inventário real, size factors, one-sided, CSV |
| 3 | `d1de1efb` | Skew por inventário, vol multiplier, freios de churn |
| 4 | `5a30246f` | Regime L1/L1.5/L2/L3, kill switch, session PnL |
| 5 | `8c3d4c9e` | Lead-lag micro pause + sintético fair_brl EWM |
| Pós-5 refactor | `c432df4e` | Modulariza regime em utils, staleness/critical-error, orders.csv |
| Pós-5 paper boot | `9f4407ea` | Patch V1/V2 compat para paper trade no boot script |
| Pós-5 paper config | `ee7e6980` | Configs paper trade (YAMLs para script + controller) |

**Fase 6 em andamento — paper trade rodando com configs dedicados.**

---

## 3. Arquivos principais

```
controllers/market_making/
  pmm_lead_lag_skew.py              ← controller + config (PMMLeadLagSkewController)
  pmm_lead_lag_utils.py             ← funções puras testáveis (sem connectors/feeds)
  IMPL_PROGRESS.md                  ← checklist detalhado por fase

scripts/
  v2_pmm_lead_lag.py                ← boot script com patch V1/V2 compat + drawdown global

conf/scripts/
  conf_v2_pmm_lead_lag.yml          ← config do boot script (drawdown global, controller list)

conf/controllers/
  conf_pmm_lead_lag_skew.yml        ← YAML live (binance real, max_net_position=60)
  conf_pmm_lead_lag_skew_paper.yml  ← YAML paper trade (binance_paper_trade, sem cap)

test/controllers/market_making/
  test_pmm_lead_lag_skew.py         ← 72 testes do controller
  test_pmm_lead_lag_utils.py        ← 78 testes das funções puras

docs/pmm_lead_lag/
  PLAN.md                           ← plano completo de implementação (1468 linhas)
  HANDOFF.md                        ← este arquivo
```

---

## 4. Como rodar os testes

```bash
cd /home/user/hummingbot-ney

# Todos os testes do projeto (150 testes, ~10s)
python -m pytest test/controllers/market_making/test_pmm_lead_lag_skew.py \
                 test/controllers/market_making/test_pmm_lead_lag_utils.py \
                 -v --tb=short

# Só utils (78 testes, funções puras)
python -m pytest test/controllers/market_making/test_pmm_lead_lag_utils.py -v

# Só controller (72 testes)
python -m pytest test/controllers/market_making/test_pmm_lead_lag_skew.py -v

# Um teste específico
python -m pytest test/controllers/market_making/test_pmm_lead_lag_skew.py \
                 -k "test_regime_normal" -v
```

**Os testes devem passar 100% antes de qualquer commit.**

---

## 5. Como rodar o bot em paper trading

### 5.1 Iniciar (headless)

```bash
conda activate hummingbot
cd ~/hummingbot-ney
conda run -n hummingbot python bin/hummingbot_quickstart.py \
  --v2 conf_v2_pmm_lead_lag.yml -p Senha123 --headless &

# Monitorar sinais em tempo real:
tail -f logs/pmm_lead_lag/signals.csv

# Log geral (filtrar MQTT que é verboso):
tail -f logs/logs_conf_v2_pmm_lead_lag.log | grep -v MQTT
```

O config `conf/scripts/conf_v2_pmm_lead_lag.yml` aponta automaticamente para
`conf/controllers/conf_pmm_lead_lag_skew_paper.yml` (connector `binance_paper_trade`).

### 5.2 Verificar que está operando

Após ~2 minutos nos logs:
- Linha em `signals.csv` com `regime=normal`
- `inv_pct` próximo de 0.50
- Ordens criadas: 2 bids + 2 asks a 10 bps e 20 bps do mid

### 5.3 Limitações do paper trade (importante)

| Funcionalidade | Status |
|----------------|--------|
| Regime (normal/degraded/safe/paused/killed) | ✅ funciona |
| Spread e preço das ordens | ✅ funciona |
| Inventory tracking (`inv_pct`, `s_inv`) | ✅ funciona |
| `signals.csv` e `orders.csv` em tempo real | ✅ funciona |
| Colocação de ordens no paper connector | ✅ funciona |
| Fill tracking / `session_pnl` | ❌ sempre zero |

**Por que fill tracking é zero:** `PaperTradeExchange` é Cython V1 e não completa o
ciclo de vida do executor V2. O patch de compatibilidade (`_patch_executor_base_for_paper_trade`)
resolve o crash de inicialização, mas não emula fills completos.

**Workaround de PnL:** monitorar riqueza total via `signals.csv`:

```python
# Exemplo de cálculo de PnL estimado a partir de signals.csv:
import pandas as pd
df = pd.read_csv("logs/pmm_lead_lag/signals.csv")
df["wealth_brl"] = df["base_balance"].astype(float) * df["mid_brl"].astype(float) \
                 + df["quote_balance"].astype(float)
pnl = df["wealth_brl"].iloc[-1] - df["wealth_brl"].iloc[0]
```

Fill tracking real e `session_pnl` funcionarão corretamente no dry-run com conector
`binance` real (Fase 6 após paper).

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

## 7. Fase 6 em andamento — validação pré-capital

### 7.1 Semana 1 — Baseline Phase 4 (em andamento)

- Paper trade rodando com `w_lead: 0.0` (config `conf_pmm_lead_lag_skew_paper.yml`)
- Métricas a coletar via `logs/pmm_lead_lag/signals.csv`:
  - `inventory_drift_stddev` = stddev de `inv_pct` (coluna da signals)
  - `adverse_fill_ratio_10s` (coluna da signals)
  - PnL estimado: `base_balance * mid_brl + quote_balance` (ver §5.3 workaround)
  - `regime` — % do tempo em cada estado
- **Nota:** `session_pnl` e `fill_rate` são zero no paper trade (limitação V1/V2, ver §5.3)

### 7.2 Semanas 2–3 — A/B Phase 4 vs Phase 5

- Segunda instância com `w_lead: 0.1` (Phase 5) — criar novo YAML copiando o paper com essa mudança
- Gate para aceitar Phase 5:
  - `adverse_fill_ratio_10s` cai ≥ 15%
  - volume de fills não cai > 15%
  - Significância: p < 0.05, Cohen's d > 0.3

### 7.3 Após A/B

1. Calibrar thresholds com p95 de vol observada
2. Dry-run R$200 real por 3 dias (fill tracking funcionará com conector real)
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

## 9. Lições de Configuração — armadilhas conhecidas

> Aprendidas durante a Fase 6 (paper trade + live deploy). Leia antes de criar
> ou alterar qualquer YAML de configuração.

### 9.1 `max_net_position_quote` — semântica crítica

**Cálculo no controller (linha ~806):**
```python
net_exposure_quote = base_bal * mid_dec   # valor TOTAL de BTC em BRL, não desvio
over_max_net_position = net_exposure_quote > config.max_net_position_quote
```

**Erro comum:** definir como `30% do capital total` (ex. R$60 para R$200 de capital).
Com target 50/50, a posição BTC inicial é ~R$100 → dispara imediatamente.

**Regra correta:** o valor deve ser **maior que o valor atual de BTC** e servir como
cap de emergência acima do `inv_kill` (que já protege a 90% do portfolio).

| Capital total | BTC inicial (~50%) | `max_net_position_quote` recomendado |
|---|---|---|
| R$400 | R$200 | **500** (cap em 63% do portfolio) |
| R$800 | R$400 | **650** |
| R$200 | R$100 | **160** |

A proteção primária de inventário é `inv_kill: 0.40` (kill quando inv_pct > 0.90 ou < 0.10).
Use `max_net_position_quote` apenas como guard contra cenários extremos de mercado.

---

### 9.2 `controllers_config` — só o filename, sem path

No YAML do boot script (`conf/scripts/`), o sistema prepende `conf/controllers/`
automaticamente. Passar o path completo causa duplicação:

```yaml
# ERRADO — gera "conf/controllers/conf/controllers/conf_pmm_lead_lag_skew.yml"
controllers_config:
  - conf/controllers/conf_pmm_lead_lag_skew_paper.yml

# CORRETO
controllers_config:
  - conf_pmm_lead_lag_skew_paper.yml
```

---

### 9.3 `controller_type` — obrigatório no YAML

O `trading_core` loader exige explicitamente:
```yaml
controller_type: market_making   # sem isso → "Missing controller_type"
```

---

### 9.4 `candles_connector` — nunca `binance_paper_trade`

A `CandlesFactory` não suporta o conector `binance_paper_trade`. Sempre use o
conector real para dados de mercado, mesmo em paper trade:

```yaml
candles_connector: binance        # CORRETO (mesmo em paper trade)
leader_connector: binance         # idem
quote_rate_connector: binance     # idem
connector_name: binance_paper_trade  # só aqui vai o paper
```

---

### 9.5 PaperTradeExchange (V1) — incompatibilidade com V2 PositionExecutor

`PaperTradeExchange` (Cython V1) não tem `.trading_rules` nem `._order_tracker`.
O `ExecutorBase` (V2) precisa desses atributos. O `scripts/v2_pmm_lead_lag.py`
contém um monkey-patch de compatibilidade que deve ser mantido:

```python
def _patch_executor_base_for_paper_trade():
    # Patcheia get_trading_rules → retorna TradingRule permissivo se não tiver .trading_rules
    # Patcheia get_in_flight_order → retorna None se não tiver ._order_tracker
```

**Não remover** este patch enquanto usar paper trade com V2 controllers.

---

### 9.6 `micro_stale` — causa estrutural (~10% steady-state)

O `micro_stale` é disparado quando `mid_brl is None or mid_usdt is None`, o que
ocorre quando `get_candles_df` retorna DF vazio (~1 tick a cada ~10s, race condition
na atualização do feed). O cache em `_read_latest_close` (adicionado na Fase 6)
resolve o problema de warmup mas não elimina o steady-state.

- **Impacto com w_lead=0.1:** lead signal inativo nos ~10% de ticks stale; aceitável.
- **Diagnóstico:** verificar se os runs são de 1 tick (estrutural) ou longos (rede).
- **Fix definitivo:** implementar subscrição bookTicker dedicada para BTC-USDT.

---

### 9.7 `session_pnl` — zero em paper trade (V1)

O `session_pnl` permanece 0 em paper trade porque o `PaperTradeExchange` V1 não
despacha eventos de fill compatíveis com o V2 executor. Para medir PnL use o proxy:

```python
portfolio_brl = base_bal * mid + quote_bal
```

No **live** (Binance real), `session_pnl` é rastreado corretamente e o kill switch
`max_session_drawdown_quote` funciona de verdade.

---

### 9.8 `--v2` no quickstart — só filename

O flag `--v2` do `bin/hummingbot_quickstart.py` prepende `conf/scripts/` automaticamente:

```bash
# ERRADO — gera "conf/scripts/conf/scripts/conf_v2_pmm_lead_lag.yml"
python bin/hummingbot_quickstart.py --v2 conf/scripts/conf_v2_pmm_lead_lag.yml

# CORRETO
python bin/hummingbot_quickstart.py --v2 conf_v2_pmm_lead_lag.yml
```

---

### 9.9 Taxas Binance VIP 4 — `conf/conf_fee_overrides.yml`

As taxas devem refletir o nível VIP e se BNB é usado para pagar fees:

| Cenário | maker | taker |
|---|---|---|
| VIP 4 **sem BNB** (configurado) | **0.04%** | **0.052%** |
| VIP 4 com BNB (25% off) | 0.030% | 0.039% |
| Padrão retail (default Hummingbot) | 0.100% | 0.100% |

**Impacto no live:** o `session_pnl` usa as taxas reais reportadas pelo fill da Binance
(campo `commission`), não este config. O config afeta principalmente paper trade e
estimativas pré-trade.

**Referência:** `conf/conf_fee_overrides.yml` — requer reinício do bot para entrar em vigor.

---

### 9.11 Estado da Fase 6 (atualizado 2026-04-30)

| Semana | Connector | `w_lead` | Duração | Resultado |
|---|---|---|---|---|
| S1 baseline | binance_paper_trade | 0.0 | 19.5h | normal 100%, stale 10.5%, inv soft 89% |
| S2 A/B | binance_paper_trade | 0.1 | ~38h total | normal 99.9%, lead ativo 82%, inv soft 100% |
| Live deploy | **binance** | 0.1 | em curso | R$407 BRL + 0.001 BTC, 4 ordens ativas |

**Config live ativa:** `conf/controllers/conf_pmm_lead_lag_skew_live.yml`
**Boot script live:** `conf/scripts/conf_v2_pmm_lead_lag_live.yml`

---

## 10. Regras de desenvolvimento

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

## 11. Referências rápidas

| O que | Onde |
|-------|------|
| Plano completo (arquitetura, fórmulas, roadmap) | `docs/pmm_lead_lag/PLAN.md` |
| Checklist de progresso por fase | `controllers/market_making/IMPL_PROGRESS.md` |
| Config Pydantic completa | `pmm_lead_lag_skew.py` linha ~42 |
| Funções puras (dataclasses + cálculos) | `pmm_lead_lag_utils.py` |
| YAML controller — referência (Phase 1) | `conf/controllers/conf_pmm_lead_lag_skew.yml` |
| YAML controller — paper trade (Phase 6) | `conf/controllers/conf_pmm_lead_lag_skew_paper.yml` |
| YAML controller — live Binance (Phase 6) | `conf/controllers/conf_pmm_lead_lag_skew_live.yml` |
| Boot script paper | `conf/scripts/conf_v2_pmm_lead_lag.yml` |
| Boot script live | `conf/scripts/conf_v2_pmm_lead_lag_live.yml` |
| Testes de utils | `test/controllers/market_making/test_pmm_lead_lag_utils.py` |
| Testes do controller | `test/controllers/market_making/test_pmm_lead_lag_skew.py` |
| Boot script Python | `scripts/v2_pmm_lead_lag.py` |
| Armadilhas de configuração | `docs/pmm_lead_lag/HANDOFF.md` §9 |

---

## 12. Perguntas frequentes

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

**Por que `session_pnl` é sempre 0 no paper trade?**  
`PaperTradeExchange` é implementado em Cython V1 e não completa o ciclo de vida do
`PositionExecutor` V2 (o executor nunca recebe evento de fill). O patch de compat
(`_patch_executor_base_for_paper_trade`) resolve o crash de inicialização mas não
emula fills. Workaround: calcular riqueza como `base_balance * mid_brl + quote_balance`
a partir das colunas da `signals.csv`. Fill tracking real funciona com conector `binance`
real (dry-run Fase 6).
