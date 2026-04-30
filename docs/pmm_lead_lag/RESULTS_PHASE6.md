# PMM Lead-Lag Skew — Resultados Fase 6

> Arquivo de memória consolidando todos os testes e operações da Fase 6.
> Gerado em 2026-04-30 ao encerrar o primeiro ciclo de testes live.

---

## 1. Contexto

Fases 1–5 concluídas (150 testes passando). Fase 6 = validação pré-escala:
paper trade → live com capital mínimo → escala progressiva.

**Objetivo da Fase 6:** confirmar estabilidade operacional, medir PnL real com
fills, e comparar baseline (w_lead=0.0) vs lead-lag ativo (w_lead=0.1).

---

## 2. Semana 1 — Paper Trade Baseline (w_lead=0.0)

**Período:** 2026-04-28 16:44 UTC → 2026-04-29 12:13 UTC

| Métrica | Valor |
|---|---|
| Duração | 19.5h |
| Ticks | 19.092 |
| BTC/BRL range | R$378.280 – R$388.301 |
| BTC change | +2.1% |
| Regime normal | 100.0% |
| Vol ratio max | 2.123 (< threshold 3.0) ✅ |
| Inv. média | 0.4164 (target 0.50) |
| Inv. soft band | 89.3% |
| Inv. MAE | 0.0836 |
| shift_bps | -1.357 ± 0.108 |
| spread_mult médio | 1.086 |
| Lead ativo | 77.0% (w_lead=0.0 → calculado mas não aplicado) |
| micro_stale | 10.5% (estrutural) |
| PnL proxy | +0.87% (BTC subiu 2.1%) |
| Max drawdown | 0.28% |
| Ordens | 3.918 (buy 1.970 / sell 1.948) |
| Volume | 0.512 BTC = R$195.800 |
| session_pnl | R$0.00 (V1 paper trade não rastreia fills) |

**Observações:**
- Regime 100% normal — nenhum threshold de vol/basis atingido
- Inventário estruturalmente abaixo de 0.50 (capital inicial desbalanceado: 1 BTC × R$378k = 43% do portfolio paper)
- `session_pnl=0` é limitação conhecida do V1 PaperTradeExchange com V2 executor
- Baseline sólido para comparação A/B

---

## 3. Semana 2 — Paper Trade com Lead-Lag (w_lead=0.1)

**Período:** 2026-04-29 12:18 UTC → 2026-04-30 07:57 UTC

| Métrica | Valor | vs S1 |
|---|---|---|
| Duração | 19.6h | +0.1h |
| Ticks | 18.841 | similar |
| BTC/BRL range | R$375.176 – R$386.010 | |
| BTC change | -1.19% | mercado inverteu |
| Regime normal | 99.9% | 1 evento paused (~18s) |
| Vol ratio max | 2.288 | ligeiramente maior |
| Inv. média | 0.4393 | **+2.3pp ↑** (melhor) |
| Inv. soft band | **100.0%** | **+10.7pp ↑** |
| Inv. MAE | 0.0607 | **-0.023 ↓** (melhor) |
| shift_bps | -1.080 ± 0.082 | std menor (inv menos extremo) |
| spread_mult médio | 1.118 | levemente maior |
| Lead ativo | 81.7% | **+4.7pp ↑** |
| s_lead_micro std | 1.655 | **+0.50 ↑** (mais sinal) |
| micro_stale | 10.7% | similar |
| PnL proxy | -0.52% (BTC caiu 1.19%) | |
| Max drawdown | 1.24% | maior (efeito preço) |
| session_pnl | R$0.00 (limitação paper) | |

**Observações:**
- Inventário melhorou significativamente: S1 iniciou desbalanceado, S2 partiu de posição melhor
- Lead signal mais vivo com w_lead=0.1: basis_bps std 1.35→1.89, s_lead_micro std 1.15→1.65
- `shift_bps` std caiu (0.108→0.082) porque s_inv menos extremo domina o efeito
- 1 evento de `paused` (~18 ticks) — regime respondendo corretamente ao mercado
- Comparação A/B não conclusiva estatisticamente (fills não rastreados no paper)

---

## 4. Live Binance — Primeira Operação Real

**Período:** 2026-04-30 08:02 UTC → 2026-04-30 16:46 UTC

**Capital inicial:** 0.001000 BTC + R$407,97 BRL = **R$788,83 total**

| Métrica | Valor |
|---|---|
| Duração | 8.7h |
| Ticks | 8.969 |
| BTC/BRL range | R$378.934 – R$382.812 |
| BTC change | -0.19% |
| Regime normal | 100.0% ✅ |
| Kill switches | 0 disparados ✅ |
| Vol ratio max | 1.965 (< 3.0) ✅ |
| Inv. média | 0.4135 |
| Inv. soft band | 33.0% |
| Inv. min/max | 0.357 / 0.517 |
| shift_bps | -0.970 ± 0.796 |
| spread_mult médio | 1.178 |
| Lead ativo | 79.5% |
| micro_stale | 10.0% |
| **session_pnl (real)** | **R$ -0,77** ✅ rastreado |
| PnL proxy | -0.18% |
| Max drawdown | 0.363% (R$2,86) |
| Ordens | 1.191 (buy 737 / sell 454) |
| Volume | 0.318 BTC = R$120.812 |

**Saldo final:**
- BTC: 0.001000 → **0.001059** (+0.000059 BTC) — acumulou BTC conforme target
- BRL: R$407,97 → **R$384,80** (-R$23,17) — converteu BRL em BTC
- Net: R$788,83 → **R$787,46** (PnL -R$1,37 = spread cost + BTC caiu 0.19%)

**Observações:**
- **Fills reais confirmados** — executor V2 + Binance live rastreia corretamente
- `session_pnl = -R$0,77` = custo real de spread em 8.7h com capital R$788
- Bot rebalanceou inventory de 48.3% → 51.1% (comprou BTC gradualmente) ✅
- Inventário abaixo do soft band 67% do tempo: bot iniciou desequilibrado e estava
  corrigindo; com mais tempo estabilizaria
- 737 buy vs 454 sell: assimetria esperada (subinventariado → bot compra mais)
- Volume maker R$120k em 8.7h ≈ R$13.800/h — operação dentro do esperado

---

## 5. Consolidado Fase 6

| Sessão | Connector | w_lead | Dur. | Fills | PnL real |
|---|---|---|---|---|---|
| S1 Baseline | paper | 0.0 | 19.5h | ❌ não rastr. | R$0 |
| S2 A/B | paper | 0.1 | 19.6h | ❌ não rastr. | R$0 |
| Live | binance | 0.1 | 8.7h | ✅ rastr. | **-R$0,77** |
| **Total** | | | **47.8h** | | |

**Volume total postado:** 1.028 BTC ≈ R$420.000 notional

---

## 6. Descobertas Técnicas da Fase 6

### 6.1 V1 PaperTradeExchange × V2 PositionExecutor

Incompatibilidade fundamental resolvida via monkey-patch no boot script:
- `PaperTradeExchange` não tem `.trading_rules` nem `._order_tracker`
- Patch em `scripts/v2_pmm_lead_lag.py`: `_patch_executor_base_for_paper_trade()`
- Consequência: `session_pnl=0` no paper (fills não despachados ao executor V2)
- **No live Binance**: funciona corretamente — fills rastreados pelo executor

### 6.2 micro_stale ~10% (estrutural)

- Causa: `get_candles_df` retorna DF vazio ~1 tick a cada 10s (race condition)
- **Não é falha de rede** — runs de 1 tick, uniforme 24h, independe de hora
- Fix parcial: cache em `_read_latest_close` (reduz stale de warmup)
- Fix definitivo pendente: bookTicker WebSocket para BTC-USDT
- **Impacto com w_lead=0.1:** lead inativo 10% dos ticks — aceitável

### 6.3 max_leader_staleness_sec

- Valor correto: **5s** (= `lead_micro_window_sec`)
- Tentativa de mudar para 15s para reduzir stale foi diagnóstico errado
- Revertido: aumentar threshold não reduz stale (causa é `None` reads, não timestamp)
- ⚠️ Nunca aumentar além de `lead_micro_window_sec`

### 6.4 max_net_position_quote

- Semântica: `base_bal × mid` (valor total de BTC em BRL, não desvio)
- Valor `60` para R$200 capital estava errado no Phase 1 config
- Correto para live com ~R$789 portfolio: **500** (cap em ~63% do portfolio)
- Proteção primária é `inv_kill=0.40` (kill quando inv_pct > 0.90)

### 6.5 Taxas Binance VIP 4 (sem BNB)

- Maker: **0.040%** | Taker: **0.052%**
- Configurado em `conf/conf_fee_overrides.yml`
- No live: fees reais vêm do fill response da Binance (config é para estimativas)
- Breakeven round-trip maker/maker: **0.8 bps** (2 × 0.04%)

### 6.6 Resolução real do sinal micro

- `_read_latest_close` usa candle 1m — preço BTC-BRL atualiza a cada ~28s (mediana)
- Sinal "micro" de 5s tem resolução efetiva de 5–30s
- Para resolução real sub-5s: bookTicker WebSocket dedicado (TODO futuro)

---

## 7. Configurações Ativas no Encerramento

| Arquivo | Finalidade |
|---|---|
| `conf/controllers/conf_pmm_lead_lag_skew_live.yml` | Controller live (w_lead=0.1, R$400+0.001 BTC) |
| `conf/scripts/conf_v2_pmm_lead_lag_live.yml` | Boot script live |
| `conf/controllers/conf_pmm_lead_lag_skew_paper.yml` | Controller paper (w_lead=0.1) |
| `conf/scripts/conf_v2_pmm_lead_lag.yml` | Boot script paper |

**Para reiniciar o live:**
```bash
conda activate hummingbot
cd ~/hummingbot-ney
python bin/hummingbot_quickstart.py --headless \
  --v2 conf_v2_pmm_lead_lag_live.yml \
  --config-password Senha123 \
  2>&1 | tee -a logs/logs_conf_v2_pmm_lead_lag_live.log &
```

---

## 8. Próximas Etapas (Fase 6 continuação)

### Prioridade alta
- [ ] **A/B com dados reais**: rodar live 72h+ com w_lead=0.0 vs w_lead=0.1
  - Atualmente só temos paper para comparação (sem fills no paper)
  - Criar config live com w_lead=0.0 para baseline real
- [ ] **bookTicker WebSocket** para BTC-USDT: resolver micro_stale e aumentar resolução do sinal micro de ~30s para ~100ms
- [ ] **Calibrar `max_net_position_quote`**: implementar cálculo dinâmico baseado no portfolio atual em vez de valor fixo

### Prioridade média
- [ ] **`adverse_fill_ratio_10s`**: implementar métrica de fill adverso (HANDOFF §7.1)
- [ ] **Escala progressiva**: R$400 → R$1.000 → R$3.000 após 72h+ de dados live saudáveis
- [ ] **`max_leader_staleness_sec` adaptativo**: investigar se o candle DF pode ser substituído por uma fonte de preço mais granular sem redesign completo

### Prioridade baixa
- [ ] Métricas de `fills.csv` com `adverse_mid_1s` / `adverse_mid_10s`
- [ ] Dashboard de monitoramento (atualmente manual via `tail -f signals.csv`)

---

## 9. Parâmetros Chave para Próxima Instância

```yaml
# Live BTC-BRL com capital ~R$800
connector_name: binance
trading_pair: BTC-BRL
total_amount_quote: 400        # side quote; ordens ~R$100/nível
max_net_position_quote: 500    # cap quando BTC > 63% portfolio
max_session_drawdown_quote: 20 # 5% do capital — kill automático
w_lead: 0.1                    # validado estável no paper S2
skew_max_bps: 2
buy_spreads: [0.0010, 0.0020]
sell_spreads: [0.0010, 0.0020]
max_leader_staleness_sec: 5    # = lead_micro_window_sec (não alterar)
```
