# Plano de Implementação — PMM BTC-BRL com Lead-Lag Skew (Hummingbot V2)

> Status do plano: VERSÃO FINAL pré-implementação — revisão 6 (7 pontos reavaliados: todos já cobertos na rev5 — ver §0.1; §9.6 resiliência a timeout confirmado).
> Última atualização: 2026-04-25

## 0. Contexto

**Problema:** Operar um bot de Pure Market Making em BTC-BRL (Binance Spot) para maximizar volume de negociação (meta: aproveitar maker fee reduzida em VIP4 — não rebate, mas baixa o suficiente para tornar o breakeven viável), mantendo operação próxima de breakeven, com controle rígido de inventário e risco. O par BRL tem liquidez inferior ao BTC-USDT e spreads naturalmente maiores, o que cria oportunidade de capturar spread, mas também expõe o bot a:

- **Adverse selection** quando BTC-USDT (leader global) se move antes do BTC-BRL ajustar.
- **Drift de inventário** em regimes tendenciosos (uma ponta recheia indefinidamente).
- **Regimes tóxicos** (alta vol, gaps) onde spread estático vira sniper food.

**Premissas operacionais (explícitas, conforme usuário):**

- Capital limitado, sem co-location, um desenvolvedor, Binance VIP4. **Hipótese operacional a confirmar no dry-run**: maker fee ≈ 3 bps spot (não há rebate, não é zero); **taker ≈ 4 bps** (confirmado pelo usuário). Breakeven bilateral (ambas as pontas maker): capturar spread ≥ 6 bps net. Se uma ponta vira taker (ex.: rebalance emergencial), custo sobe para 3+4 = 7 bps. `skew_max_bps` não pode consumir esse buffer sem evidência clara de ganho — com taker a 4 bps a penalidade de escorregar para taker é menor, mas continua cara o suficiente para evitar. O tier VIP depende de volume nos últimos 30 dias e pode mudar — monitorar mensalmente.
- Simplicidade operacional > otimização marginal de alpha.
- Lead-lag (BTC-USDT leader, BTC-BRL follower) é usado como ajuste **defensivo** de skew e não como estratégia de alpha direcional.
- Arquitetura Hummingbot V2 (`strategy_v2`), controllers + executors, sem tocar em legacy.

**Resultado esperado:** Controller novo `pmm_lead_lag_skew` em `controllers/market_making/`, derivado de `MarketMakingControllerBase`, com filtros de regime, skew composto e controle de inventário em bandas. Roadmap em fases incrementais (Fase 1 → Fase 6).

**Convenção de nomenclatura (importante, evitar confusão):**
- **Hummingbot V2** = a versão do framework (a stack moderna do Hummingbot, baseada em `strategy_v2`, controllers e executors). É a base tecnológica deste plano e **não muda** ao longo do roadmap.
- **Fase 1, Fase 2, ..., Fase 6** = etapas incrementais do roadmap deste bot. Não têm relação com versões do Hummingbot. A Fase 1 já está sobre Hummingbot V2.

---

### 0.1 Avaliação dos 7 pontos críticos levantados por revisão externa (rev 6)

Uma revisão externa identificou 7 pontos. Todos foram verificados contra o texto atual do plano e encontrados **já cobertos na revisão 5**. Síntese:

| # | Ponto levantado | Status | Onde está coberto |
|---|----------------|--------|-------------------|
| 1 | Bug de sinal em §2.3 (lado BUY/SELL invertido) | ✅ Correto no plano | §2.3 diz "lado BUY quando Δ>0" com mnemônico explícito; teste unitário (c) em §2.3 obriga `size_factor_buy ≤ size_factor_sell` quando Δ>0; branch `else:` sem `t` também corrigido |
| 2 | Inventário ignorando ordens resting | ✅ Deferido conscientemente | §2.1 nota formal: `base_efetivo` = backlog Fase 4+ se drift observado superar esperado; para capital inicial e poucos níveis, diferença é segunda ordem |
| 3 | Micro pause sem cancelar ordens ativas | ✅ Design explícito + override | §3.6: deliberadamente não cancela (churn 2 round-trips > adverse selection em 3s); critério de override: `|lag_micro| ≥ spread_ativo` → cancelamento obrigatório em `stop_actions_proposal()` |
| 4 | Time skew entre feeds no lead-lag micro | ✅ Separado estruturalmente | §5.2: micro usa apenas BTC-USDT puro (2 pernas, sem cambial); regime usa sintético com staleness check das 3 pernas (`return 0.0` se qualquer perna stale) |
| 5 | Hard cap absoluto mal definido | ✅ Fórmula correta + justificada | §2.5: `net_exposure_quote = base_balance * P`; nota "Por que não usar `base_balance*P − base_inicial*P_inicial`" explica exatamente o risco de misturar mark-to-market com posição |
| 6 | L1.5 ausente do pseudocódigo 4.3 | ✅ Já incluído | §4.3: pseudocódigo completo com `_l1_entry_time`, `_safe_entry_time`, critérios de entrada/saída, dwell duplo e `return "safe"` |
| 7 | §8.2 conflita com lead-lag micro | ✅ Já separado em dois blocos | §8.2: parágrafo sobre regime (janelas ≥ 30s) e parágrafo distinto sobre micro ("a janela de 1–5s é intencional — não é skew, é filtro defensivo") |

**Conclusão:** nenhuma alteração de substância necessária. O plano está consistente e implementável.

---

## Índice

1. Arquitetura no Hummingbot (módulos, componentes, decisões estruturais)
2. Controle de inventário e exposição (target, bandas, hard cap, reduções progressivas)
3. One-sided mode (ativação, retorno, interação com skew)
4. Pausa em regime ruim (circuit breakers, kill switch)
5. Sistema inteligente de skew (fórmula, pesos, normalização, prioridade)
6. Roadmap incremental de implementação (Fase 1 → Fase 6)
7. Backtest / replay / validação (dados, A/B, métricas)
8. Riscos e falhas (overfitting, latência, sinais conflitantes)
9. Apêndice: arquivos a criar/modificar, configs iniciais

---

## 1. Arquitetura no Hummingbot

### 1.1 Decisões de alto nível

- **Reutilizar** a stack V2 (`strategy_v2`) e herdar de `MarketMakingControllerBase` em vez de criar controller do zero. Motivo: rebalanceamento de inventário em spot, triple barrier, refresh/cooldown e integração com `ExecutorOrchestrator` já estão resolvidos. Mais código herdado = menos bugs.
- **Usar `PositionExecutor`** (LIMIT entry) para cada nível de ordem, idêntico ao que `pmm_simple`/`pmm_dynamic` fazem. Evita escrever gerenciamento de ciclo de ordem.
- **Usar `MarketDataProvider`** com `CandlesFeed` para BTC-BRL e BTC-USDT (leader) + `get_price_by_type` para mid/best-bid/ask no tick do controller.
- **Separar alpha vs defesa:** o lead-lag **não** entra como sinal direcional puro no `reference_price` (evita virar trend-following). Entra como modulador de skew (shift diferencial de bids e asks) e como trigger de pausa/one-sided.
- **Config → Pydantic** (padrão V2). Tudo que é parametrizável (pesos de sinais, bandas, thresholds) vai em `PMMLeadLagSkewConfig`. Só hardcode em constantes físicas (nome do asset base, mínimo de casas decimais etc.).

### 1.2 Componentes (novos e reutilizados)

| Tipo | Componente | Origem | Papel |
|------|-----------|--------|-------|
| Config | `PMMLeadLagSkewConfig(MarketMakingControllerConfigBase)` | **novo** | Adiciona campos de leader/lag, bandas de inventário, thresholds de regime, pesos de skew |
| Controller | `PMMLeadLagSkewController(MarketMakingControllerBase)` | **novo** | Override de `update_processed_data()`, `get_executor_config()`, `get_levels_to_execute()`, `to_format_status()` |
| Executor | `PositionExecutor` | `hummingbot/strategy_v2/executors/position_executor/position_executor.py` | Ciclo de vida de cada ordem maker |
| Rebalance | `OrderExecutor` (via `check_position_rebalance`) | `MarketMakingControllerBase:339-414` | **Desabilitado por padrão (`skip_rebalance: true`)**. Rebalance MARKET em spot BRL fura o breakeven; só ativar manualmente como exceção/intervenção, não como comportamento automático |
| Sinais | `CandlesFeed` p/ BTC-USDT e BTC-BRL | `data_feed/candles_feed/binance_spot_candles` | Base para volatilidade, tendência, lead-lag |
| Indicadores | `InstantVolatilityIndicator`, `HistoricalVolatilityIndicator` | `hummingbot/strategy/__utils__/trailing_indicators/` | Estimativa rolante de σ |
| Price ref | `market_data_provider.get_price_by_type(..., PriceType.MidPrice)` | — | Mid BTC-BRL |
| Logging | `to_format_status()` do controller + CSV próprio | — | Ver 1.4 |

### 1.3 Responsabilidades do controller (diagrama lógico)

```
on_tick (a cada ~1s):
  1. update_processed_data()
     ├── Atualiza BTC-BRL mid, best_bid, best_ask
     ├── Atualiza BTC-USDT mid (leader) e USDT-BRL (conversão)
     ├── Atualiza σ de curto prazo (1m, N=30)
     ├── Calcula fair_price_brl = mid_usdt * usdt_brl_rate
     ├── Calcula sinais normalizados:
     │     - s_inv  (inventário vs target)
     │     - s_lead (lead-lag defensivo)
     │     - s_vol  (volatilidade)
     ├── Combina em skew_total (ver §5)
     ├── Avalia regime: ok / degraded / paused
     └── Atualiza processed_data:
            reference_price, spread_multiplier,
            price_shift, inventory_pct, regime, sides_enabled

  2. determine_executor_actions()
     ├── create_actions_proposal()
     │    ├── check_position_rebalance()  (só se drift > hard_rebalance_threshold)
     │    └── for level in get_levels_to_execute():   ← override
     │         └── get_executor_config(level, price, amount)  ← override
     │               (aplica shift, reduz size se inv alto, veta se regime paused)
     └── stop_actions_proposal()
          (refresh padrão + early-stop se reference_price mover > threshold)
```

### 1.4 Métricas e logs a persistir

Dois níveis de telemetria:

**A. Já coberto pelo Hummingbot** (não reimplementar):
- Trades/fills em SQLite via `MarketsRecorder` → base para análise posterior.
- `to_format_status()` do `PositionExecutor` → UI.

**B. Novo CSV por bot (persistência explícita)** em `logs/pmm_lead_lag/`:
- `signals.csv`: timestamp, mid_brl, mid_usdt, usdt_brl, fair_brl_raw, fair_brl_smooth, basis_bps, vol_1m, inventory_pct, s_inv, s_lead_micro_buy, s_lead_micro_sell, s_lead_regime, skew_total, regime, sides_enabled. (`fair_brl_smooth` = EWM do sintético; logar ambos para diagnóstico de quanto o EWM amorte picos)
- `orders.csv`: timestamp, level_id, side, price, size, distance_from_mid_bps, skew_bps, regime.
- `fills.csv`: timestamp, side, price, size, slippage_vs_mid, fee_paid, adverse_mid_1s, adverse_mid_10s (preço do mid 1s/10s após o fill — proxy de adverse selection).

Implementação: método helper `_append_csv(path, row)` no controller, chamado em `update_processed_data()` e em handler de fill (usar `did_fill_order` no executor ou ler de `executors_info`). Flush a cada N linhas para não virar I/O blocker.

### 1.5 Configurável vs hardcoded

**Configurável (YAML):**
- `connector_name`, `trading_pair` (BTC-BRL), `leader_connector`, `leader_trading_pair` (BTC-USDT), `quote_rate_pair` (USDT-BRL).
- `target_inventory_base_pct`, `inventory_soft_band_pct`, `inventory_hard_band_pct`, `inventory_hard_cap_pct`.
- `buy_spreads`, `sell_spreads`, `buy_amounts_pct`, `sell_amounts_pct`, `total_amount_quote`.
- Pesos: `w_inv`, `w_lead`, `w_vol`, `skew_max_bps`.
- Regime: `vol_pause_threshold`, `vol_degraded_threshold`, `trend_z_pause`, `drawdown_pause_quote`, `adverse_fill_ratio_pause`.
- Lead-lag: `lead_lag_window_sec`, `lead_lag_ewm_halflife`, `basis_deadband_bps`.
- Executors: `executor_refresh_time`, `cooldown_time`, `time_limit`, `stop_loss`, `take_profit` (pode ser `None` para PMM puro).

**Hardcoded:**
- Schema de fees (lido de `binance_utils.DEFAULT_FEES` e sobrescrito pelo client config para VIP4).
- Formatos de CSV, nomes de colunas, paths (`logs/pmm_lead_lag/`).
- Nome do controller, nomes de level_id (`buy_0…buy_N`, `sell_0…sell_N`).

### 1.6 Arquivos novos e modificados

- **Novo:** `controllers/market_making/pmm_lead_lag_skew.py` — Config + Controller (apenas orquestração; lógica delegada para `_utils`).
- **Novo (obrigatório desde Fase 1):** `controllers/market_making/pmm_lead_lag_utils.py` — funções puras com tipos explícitos (dataclasses) para cada estado computado. Ver §1.7. Razão de ser obrigatório desde a Fase 1: evita o controller virar deus-objeto e mantém todo cálculo testável em isolamento.
- **Novo:** `controllers/market_making/conf/conf_pmm_lead_lag_skew.yml` — YAML de exemplo.
- **Novo:** `scripts/v2_pmm_lead_lag.py` — script que instancia o controller via `StrategyV2Base` (derivar de `scripts/v2_with_controllers.py:1-172`).
- **Novo (test):** `test/controllers/market_making/test_pmm_lead_lag_skew.py` + `test_pmm_lead_lag_utils.py` — baseado em `test/hummingbot/strategy_v2/controllers/test_market_making_controller_base.py`.
- **Sem modificação** no core do Hummingbot (evita conflitos de merge com upstream).

### 1.7 Modularização desde Fase 1 (anti-deus-objeto)

O controller orquestra; toda decisão fica em funções puras em `pmm_lead_lag_utils.py`. Cada função recebe inputs imutáveis e devolve um dataclass tipado. Isso garante que:

- Cada função é testável sem instanciar `StrategyV2Base`/connectors/feeds.
- Adicionar uma camada nova (regime, skew, lead-lag) é alterar uma função, não estender o controller.
- Mudar peso ou threshold é mudar config → input → output, sem efeitos colaterais.

Esqueleto (todas as funções existem desde a Fase 1, mesmo que retornem placeholders neutros nas fases iniciais):

```python
# pmm_lead_lag_utils.py
from dataclasses import dataclass
from decimal import Decimal

@dataclass(frozen=True)
class InventoryState:
    inv_pct: Decimal
    delta: Decimal
    size_factor_buy: Decimal       # multiplicador de amount no lado buy
    size_factor_sell: Decimal      # idem sell

@dataclass(frozen=True)
class VolState:
    sigma_1m: float
    vol_ratio: float               # sigma_1m / sigma_ref_6h
    spread_multiplier: Decimal     # 1.0..3.0

@dataclass(frozen=True)
class LeadState:
    s_lead_regime: float           # [-1,+1]; negativo=BRL abaixo do fair(USDT subiu)→eleva ref
    s_lead_micro_pause_sell: bool  # True quando USDT↑ e BRL atrasa: asks sub-precificadas
    s_lead_micro_pause_buy: bool   # True quando USDT↓ e BRL atrasa: bids sobre-precificadas
    basis_bps: float

@dataclass(frozen=True)
class RegimeState:
    level: str                     # "normal"|"degraded"|"safe"|"paused"|"killed"
    cause: str                     # razão legível em logs

@dataclass(frozen=True)
class SkewState:
    price_shift_bps: Decimal
    skew_raw: float                # debug

@dataclass(frozen=True)
class OrderParams:
    price: Decimal
    amount: Decimal
    side: str

# Funções puras — assinatura por contrato:
def compute_inventory_state(base_bal, quote_bal, mid, config) -> InventoryState: ...
def compute_vol_state(candles_1m, config) -> VolState: ...
def compute_lead_state(mid_brl, mid_usdt, usdt_brl, history, config) -> LeadState: ...
def compute_regime_state(vol, inv, lead, leader_staleness_sec,
                         adverse_state, session_pnl, config) -> RegimeState: ...
def compute_skew_state(inv: InventoryState, lead: LeadState, config) -> SkewState: ...
def compute_side_permissions(inv: InventoryState, regime: RegimeState,
                             lead: LeadState, config) -> frozenset[str]: ...
def compute_order_params(level_id, skew: SkewState, regime: RegimeState,
                         inv: InventoryState, mid, config) -> OrderParams: ...
```

O controller fica enxuto:

```python
async def update_processed_data(self):
    mid_brl, mid_usdt, usdt_brl = self._fetch_prices()
    base, quote                  = self._fetch_balances()

    inv    = compute_inventory_state(base, quote, mid_brl, self.config)
    vol    = compute_vol_state(self._candles_1m(), self.config)
    lead   = compute_lead_state(mid_brl, mid_usdt, usdt_brl, self._lead_hist, self.config)
    regime = compute_regime_state(vol, inv, lead, self._leader_staleness(),
                                  self._adverse_state(), self._session_pnl, self.config)
    skew   = compute_skew_state(inv, lead, self.config)
    sides  = compute_side_permissions(inv, regime, lead, self.config)

    self.processed_data.update({
        "reference_price":   mid_brl * (Decimal("1") - skew.price_shift_bps / Decimal("10000")),
        "spread_multiplier": vol.spread_multiplier * (Decimal("1.5") if regime.level == "degraded" else Decimal("1")),
        "price_shift_bps":   skew.price_shift_bps,
        "inv_pct":           inv.inv_pct,
        "inv_delta":         inv.delta,
        "regime":            regime.level,
        "regime_cause":      regime.cause,
        "sides_enabled":     sides,
        # ... debug fields
    })
    self._csv_log_signals()
```

Nas fases iniciais, `compute_lead_state` retorna `LeadState(0.0, False, False, 0.0)` e `compute_skew_state` ignora `lead`. Cada fase adiciona conteúdo às funções, não muda a forma do controller.

## 2. Controle de Inventário e Exposição

Princípio: **quanto mais dinheiro em risco numa ponta, mais desconfortável o bot tem que ficar para adicionar mais daquele lado.** A resposta é em 3 camadas progressivas, não binária.

### 2.1 Definições

- `P` = mid price BTC-BRL atual.
- `Q` = valor total do portfólio em BRL (= `base_balance * P + quote_balance`).
- `inv_pct` = `base_balance * P / Q` → fração do portfólio em BTC.
- `target_pct` = 0.5 (neutro) como default. Razão: PMM bilateral, expectativa de inventário médio ≈ 50/50.
- `Δ = inv_pct - target_pct` → desvio signado. Positivo = BTC a mais.

Todos os cálculos usam `Q` (não saldo absoluto), porque `P` move e um alvo em quantidade fixa de BTC escorrega ao longo do dia.

**Nota — inventário efetivo (melhoria futura, não obrigatório nas Fases 1–3):** `base_balance` reflete apenas saldo liquidado. O inventário efetivo inclui ordens abertas: `base_efetivo = base_balance + Σ(qty de bids abertas) - Σ(qty de asks abertas)`. Para capital pequeno e poucos níveis, a diferença é pequena e adiciona complexidade de leitura do book. Adotar `base_efetivo` na Fase 4+ se drift de inventário observado superar o esperado — documentar como item de backlog.

### 2.2 Bandas (configuráveis)

| Nome | Valor default | Regra |
|------|---------------|-------|
| `inv_soft_band` | 0.10 (±10 pontos percentuais) | Dentro → operação normal |
| `inv_hard_band` | 0.20 | Entre soft e hard → reduzir size + reforçar skew |
| `inv_hard_cap`  | 0.30 | Além → desligar o lado que empurra inventário para fora |
| `inv_kill`      | 0.40 | Além → **parar tudo**, alertar operador |

`inv_soft_band < inv_hard_band < inv_hard_cap < inv_kill`. Validar via `@field_validator` na config.

### 2.3 Redução progressiva de size

Fator aplicado ao `amount` da ordem do lado "que aumenta inventário" (**lado BUY quando `Δ > 0`** — inventário já pesado em BTC; **lado SELL quando `Δ < 0`** — inventário já pesado em BRL):

> **Mnemonico:** "adversa" = o lado que empurra inventário para fora da banda. Se Δ > 0 (BTC a mais), comprar mais é adverso; vender é favorável. Se Δ < 0 (BRL a mais), vender mais é adverso; comprar é favorável.

```
abs_dev = |Δ|
if abs_dev <= soft:
    size_factor_adversa   = 1.0         # zona neutra, sem penalidade
    size_factor_favoravel = 1.0         # também sem bônus
elif abs_dev <= hard:
    # linear de 1.0 → 0.2 entre soft e hard
    t = (abs_dev - soft) / (hard - soft)
    size_factor_adversa   = 1.0 - 0.8 * t
    size_factor_favoravel = min(1.0 + 0.5 * t, 1.5)   # até +50% de size
else:
    # além do hard_band — `t` não é definido neste branch; usar máximo fixo
    size_factor_adversa   = 0.0         # desliga o lado adverso
    size_factor_favoravel = 1.5         # máximo de bônus (equivalente a t=1)
```

Razão do teto: evitar que o bot ofereça size grande num lado "bom" e seja sniprado quando o mercado vira — a redução de inventário é desejável, mas não vale pagar slippage alta.

**Teste unitário obrigatório (em `test_pmm_lead_lag_utils.py`):** para cada valor de Δ ∈ {-0.05, 0, +0.05, +0.15, +0.25, +0.35} verificar que: (a) `size_factor_adversa` é maior no lado certo; (b) nenhum branch usa `t` fora do scope onde é definido; (c) quando Δ > 0, `size_factor_buy ≤ size_factor_sell` (inventário pesado em BTC → BUY penalizado).

### 2.4 Desligamento de lado (interação com one-sided — ver §3)

- `|Δ| > inv_hard_cap` **e** Δ > 0 → desliga ordens de **compra** (`sides_enabled = {sell}`). Bot só vende para reduzir inventário.
- `|Δ| > inv_hard_cap` **e** Δ < 0 → desliga ordens de **venda** (`sides_enabled = {buy}`).
- Retorno ao bilateral quando `|Δ| < inv_hard_band` (histerese: entra em hard_cap, só sai em hard_band — evita flip-flop).

Implementação: `get_levels_to_execute()` filtra os levels cujo `trade_type` não está em `sides_enabled`. `stop_actions_proposal()` encerra executors ativos do lado desligado (comparando `executor.config.side`).

### 2.5 Hard cap de exposição absoluta

Independente de `%`, existe um **teto em BRL** (`max_net_position_quote`, configurável, ex.: 30% do `total_amount_quote` inicial) sobre o valor **líquido direcional** da posição:

```
net_exposure_quote = base_balance * P   # BTC a mais que zero convertido em BRL
```

Se `net_exposure_quote > max_net_position_quote` → trigger de kill switch (ver §4).

Motivo: `inv_pct` pode ser enganoso quando `Q` flutua muito (e.g., BRL evaporou por drawdown, `%` parece razoável mas exposição absoluta em BTC cresceu). O hard cap em BRL absoluto é o freio final.

**Por que não usar `base_balance * P - base_balance_inicial * P_inicial`:** essa fórmula mistura variação de quantidade com mark-to-market de preço — se BTC subiu 10% sem o bot comprar nada, a fórmula dispara injustificadamente. Usar simplesmente `base_balance * P` (exposição corrente em BRL) é mais limpo e previsível. `target_pct` e as bandas de inventário (`inv_pct`) já cuidam de manter a proporção; o hard cap é apenas o limite absoluto de BRL em risco no lado BTC.

### 2.6 Cálculo e consulta no código

```python
# em update_processed_data()
base_bal  = self.connectors[self.config.connector_name].get_balance("BTC")
quote_bal = self.connectors[self.config.connector_name].get_balance("BRL")
P = Decimal(self.market_data_provider.get_price_by_type(
        self.config.connector_name, self.config.trading_pair, PriceType.MidPrice))
Q = base_bal * P + quote_bal
inv_pct = (base_bal * P) / Q if Q > 0 else Decimal("0.5")
delta   = inv_pct - self.config.target_inventory_base_pct
```

Salvo em `self.processed_data["inv_pct"]`, `self.processed_data["inv_delta"]`. Usado em (a) skew (§5), (b) size factors, (c) one-sided (§3), (d) kill (§4).

### 2.7 Por que não usar o inventory_skew legacy direto

O `inventory_skew_calculator.pyx` (`hummingbot/strategy/pure_market_making/inventory_skew_calculator.pyx:31-57`) faz interpolação linear entre `left_limit/target/right_limit`. É útil, mas:

- Pertence à stack V1 legacy do Hummingbot, não integrado ao `MarketMakingControllerBase` (V2).
- Mistura size e spread skew num único ratio, o que impede separar "não adicione mais" (size) de "peça mais caro" (price skew).
- Não tem conceito de hard cap/kill.

Decisão: **não reutilizar o .pyx**. Implementar a lógica diretamente em Python no controller (fórmulas acima) — é 20 linhas, testável, e separa size de price skew limpa­mente.

## 3. One-Sided Mode

One-sided mode é um estado do controller, não um controller separado. Representado por `processed_data["sides_enabled"] ∈ {{"buy","sell"}, {"buy"}, {"sell"}, ∅}`. ∅ equivale a pausa total (§4).

### 3.1 Triggers de ativação

One-sided pode ser ativado por **qualquer** um dos critérios abaixo (OR lógico). **Atenção à fase em que cada trigger entra**:

1. **Inventário além de hard cap** (§2.4) — **disponível desde a Fase 2**. Trigger mais comum. É sempre "desligar o lado que aumenta a ponta cheia".
2. **Trend filter** (§4) — **disponível desde a Fase 4**. Se z-score da tendência de curto prazo (EMA rápido − EMA lento, normalizado por σ) > `trend_one_sided_z` (default 1.5), desliga o lado adverso.
3. **Lead-lag micro pause** (§5.2 `s_lead_micro`) — **disponível somente a partir da Fase 5**. Quando `s_lead_micro` detecta lag > `lead_micro_threshold_bps` por `lead_micro_dwell_sec`, **pausa local do lado afetado** (não desliga via `sides_enabled` global, ver §3.6). É deliberadamente o trigger mais conservador e específico — evita-se one-sided por skew contínuo de lead-lag para que lead-lag não vire motor direcional sem validação empírica.

**Princípio:** one-sided por inventário e trend são triggers operacionais (proteção contra acúmulo e tendência observável). One-sided por lead-lag é experimental e fica restrito a pause microestrutural com dwell curto até a Fase 5 provar valor.

### 3.2 Prioridade entre triggers

Inventário > tendência > lead-lag. Inventário sempre vence. Lead-lag (micro pause) só atua localmente em níveis individuais (§3.6), não interfere em `sides_enabled` global.

```
sides_to_disable = set()
if inventory_trigger: sides_to_disable.add(side_adversa_inv)   # Fase 2+, sempre vence
elif trend_trigger:   sides_to_disable.add(side_adversa_trend) # Fase 4+
# lead-lag não entra aqui — atua via pause local de nível em get_levels_to_execute (Fase 5+)

sides_enabled = {"buy", "sell"} - sides_to_disable
```

### 3.3 Retorno ao modo normal

**Histerese temporal + condição**:

- Inventário: volta quando `|Δ| < inv_hard_band` (entra em hard_cap=0.30, sai em hard_band=0.20).
- Trend: volta quando `|z_trend| < trend_one_sided_release` (default 0.6) por dwell.
- Lead-lag micro pause (Fase 5+): expira automaticamente após `lead_micro_dwell_sec` (default 3s), sem dwell adicional — pause é por design curto.

**Sem histerese → flip-flop garantido**, gerando ordens canceladas sem fill (custo de rate limit + latência). Isso é particularmente problemático no endpoint `POST /api/v3/order` da Binance (peso 1, mas limite por sub-segundo).

### 3.4 Interação com skew

One-sided **não substitui skew**; é a camada mais agressiva quando skew já não dá conta. Ordem mental:

1. Dentro do soft band, skew ajusta price shift.
2. Entre soft e hard band, size reduz + skew intensifica.
3. Além do hard cap, one-sided desliga o lado adverso completamente. Skew do lado restante continua ativo (para não jogar ordem "gulosa demais" quando mercado vira).

### 3.5 Implementação concreta (one-sided global por inventário/trend)

```python
# em get_levels_to_execute() — override
levels = super().get_levels_to_execute()
sides_enabled = self.processed_data["sides_enabled"]
return [lid for lid in levels
        if self.get_trade_type_from_level_id(lid).name.lower() in
           {s.lower() for s in sides_enabled}]

# em stop_actions_proposal() — estender
stops = super().stop_actions_proposal()
sides_enabled = self.processed_data["sides_enabled"]
for ex in self.executors_info:
    if ex.is_active and ex.config.side.name.lower() not in sides_enabled:
        stops.append(StopExecutorAction(controller_id=self.config.id,
                                        executor_id=ex.id))
return stops
```

Nota: o `stop` é definitivo para aquele executor (cancela ordem aberta). Quando o lado volta a ser permitido, o loop normal recria no próximo tick — é isso que queremos (novo preço e skew).

### 3.6 Pause local por lead-lag micro (Fase 5+)

Lead-lag micro pause **não** mexe em `sides_enabled`. Atua via filtro adicional no `get_levels_to_execute()`:

```python
# pause local por lead-lag micro — só aplica quando Fase 5 está habilitada
lead = self.processed_data.get("lead_state")
filtered = []
for lid in levels_after_sides_filter:
    side = self.get_trade_type_from_level_id(lid).name.lower()
    if side == "buy"  and lead.s_lead_micro_pause_buy:  continue
    if side == "sell" and lead.s_lead_micro_pause_sell: continue
    filtered.append(lid)
return filtered
```

Diferença em relação a `sides_enabled`:
- `sides_enabled` é estado global (inventário/trend), com dwell longo.
- `s_lead_micro_pause_*` é estado por tick, expira em segundos. **Deliberadamente não cancela executores ativos** — apenas evita criar novos enquanto a janela de pause está aberta.

**Razão da escolha de não cancelar:** micro pause dura 3s. Cancelar uma ordem e recriá-la em 3s gera 2 round-trips de API (cancel + create), consome rate limit e cria uma janela sem proteção onde o bot não tem ordem. Para spreads de 10-20 bps, a adverse selection de manter a ordem por 3s extras custa menos que o churn de cancel/recreate.

**Quando reconsiderar (dois critérios independentes):**

1. **Lag ≥ spread ativo**: se `|lag_micro| ≥ sell_spread_bps` (para pause SELL) ou `|lag_micro| ≥ buy_spread_bps` (para pause BUY), a ordem ativa está na faixa de fair value ou abaixo — será imediatamente snipada. Cancelar é obrigatório nesse caso, independente do `dwell_sec`. Implementar em `stop_actions_proposal()` como cancelamento pontual do nível afetado. Isso é o critério de maior impacto e pode ser implementado na Fase 5 desde o início.

2. **Dwell longo ou threshold alto**: se `lead_micro_threshold_bps > 10` bps ou `lead_micro_dwell_sec > 10s`, o custo de manter uma ordem por 10s com adverse selection de 10+ bps supera o custo do churn. Adicionar cancelamento ao `stop_actions_proposal()` similar ao que se faz para `sides_enabled`. Documentar como TODO na Fase 5 após avaliar dados reais.

## 4. Pausa em Regime Ruim (Circuit Breakers e Kill Switch)

Quatro níveis, do mais leve ao mais pesado:

| Nível | Nome | Ação | Reversão |
|-------|------|------|----------|
| L1 | Degraded | `spread_multiplier *= 1.5`, `size_factor_global *= 0.5`, mantém `n_levels` | Automática quando condição sai |
| L1.5 | Safe mode | `n_levels = 1` por lado, `size_factor_global = 0.25`, `spread_multiplier *= 2.5`, `price_shift_bps = 0` (zera skew, inclusive de inventário), lead-lag micro desligado | Automática, dwell duplo (2 × `pause_release_sec`) |
| L2 | Paused | `sides_enabled = ∅`, cancela ordens abertas | Automática, com dwell |
| L3 | Kill | Para controller, alerta, opcionalmente fecha posição direcional com MARKET | **Manual** (operador) |

Safe mode (L1.5) é o nível entre "ainda opera, mas minimal" e "não opera". Reduz exposure sem perder completamente o slot de market making.

**Trigger de entrada em L1.5 (mecânico):** ativa quando ocorre **qualquer** uma das condições enquanto o regime já está em L1 ou seria em L1:

- L1 ativo continuamente por > `safe_mode_entry_sec` (default 120s) — ou seja, condição "degraded" persistente.
- `adverse_fill_ratio_10s` > `safe_adverse_threshold` (default 0.7).
- 2 transições `degraded → paused → degraded` dentro de `safe_thrash_window_sec` (default 600s) — sintoma de thrashing.

**Trigger de saída de L1.5:** as condições de entrada cessam por `2 × pause_release_sec` (default 120s). A duração dobrada vs L2 é deliberada: safe mode é estado conservador; voltar cedo demais leva ao mesmo regime ruim que motivou a entrada.

### 4.1 Critérios objetivos por nível

Todos thresholds são **configuráveis**. Valores iniciais abaixo são chutes calibrados a partir de intuição do par BRL (que tem vol diária típica 2–4%), a calibrar com dados reais na fase de validação (§7).

**Três sinais de adverse fill com janelas e papéis distintos** (importante não confundir):

| Sinal | Janela | Definição | Papel |
|---|---|---|---|
| `adverse_cluster_count` | 1s | Nº de fills consecutivos do mesmo lado com mid move adverso > 5 bps em 1s após o fill | Trigger de L2 (ver L2 abaixo) |
| `adverse_fill_ratio_10s` | 10s, móvel sobre 20 fills | Fração de fills com mid adverso > 2 bps em até 10s | Trigger de L1 + de L1.5 + métrica primária Fases 1–3 |
| `adverse_fill_ratio_60s` | 60s, móvel sobre 20 fills | Idem, janela longa | Métrica de monitoramento, **sem trigger** |

#### L1 Degraded

Ativa se qualquer um:

1. **Volatilidade elevada**: `σ_1m` (desvio-padrão de retornos 1-minuto nos últimos 30 candles) > `vol_degraded_threshold` (default: 3x a mediana das últimas 6h).
2. **Trend moderado**: `|z_trend| ∈ [1.0, 1.5)`, onde `z_trend = (ema_fast - ema_slow) / σ_retornos`. Fast=5m, slow=30m.
3. **`adverse_fill_ratio_10s` > `degraded_adverse_threshold`** (default 0.6) — o mercado está te pegando com frequência em janela curta.

#### L2 Paused

Ativa se:

1. `σ_1m > vol_pause_threshold` (default: 5x a mediana de 6h).
2. `|z_trend| ≥ trend_pause_z` (default 1.5).
3. **Gap check**: `|basis_bps|` = `|(fair_brl - mid_brl) / mid_brl * 1e4|` > `pause_basis_bps` (default 30 bps) — preço BRL descolado da paridade leader*câmbio. Indica dado atrasado ou evento local.
4. **Feed staleness**: último tick BTC-USDT > `max_leader_staleness_sec` (default 5s). Sem leader, skew defensivo perde sentido.
5. **`adverse_cluster_count >= 3`** — 3 fills consecutivos do mesmo lado com adverse mid move > 5 bps em 1s.

Dwell: precisa ficar abaixo do threshold por `pause_release_sec` (default 60s) para voltar.

#### L3 Kill

Ativa se:

1. **Drawdown**: PnL acumulado da sessão (trade_pnl − fees) < `-max_session_drawdown_quote`.
2. **Hard cap de exposição** (§2.5) excedido.
3. **Inventário ultrapassa `inv_kill`** (default 0.40).
4. **Erro crítico**: 5 rejeições de ordem seguidas, perda de websocket por > 30s, timestamp desync (relógio local vs server) > 2s.

Ação: setar `self.config.manual_kill_switch = True` (convenção interna do controller), emitir log level=CRITICAL, opcionalmente executar flattening (MARKET sell/buy do excesso de inventário até target). Reversão somente por restart manual — **nunca automática em L3**.

### 4.2 Reutilização do que já existe

- `scripts/v2_with_controllers.py:48-86` já tem `check_max_global_drawdown()` e `check_max_controller_drawdown()`. Reutilizar no script que instancia o controller, mas **com valores mais conservadores** (o default do exemplo é muito frouxo para um PMM BRL).
- `hummingbot/core/utils/kill_switch.py` (ActiveKillSwitch) opera por profitabilidade global da conta, não por sessão do controller. É **complementar** ao L3 do controller. Manter ativo com threshold ainda mais conservador.

### 4.3 Implementação (esboço)

Método `_evaluate_regime(self) -> str` no controller, chamado no fim de `update_processed_data()`:

```python
def _evaluate_regime(self) -> str:
    pd = self.processed_data
    c = self.config
    now = self._now()

    # Checks L3 (kill)
    if (self._session_pnl < -c.max_session_drawdown_quote or
        abs(pd["inv_delta"]) > c.inv_kill or
        self._critical_error_counter >= c.critical_error_threshold):
        self._trigger_kill("reason")
        return "killed"

    # Checks L2 (pause)
    l2 = (pd["vol_1m"] > c.vol_pause_threshold or
          abs(pd["z_trend"]) >= c.trend_pause_z or
          abs(pd["basis_bps"]) > c.pause_basis_bps or
          pd["leader_staleness_sec"] > c.max_leader_staleness_sec or
          self._adverse_cluster_count >= 3)
    if l2:
        self._last_l2_time = now
        self._record_l2_transition(now)   # para detector de thrashing
        return "paused"
    if now - self._last_l2_time < c.pause_release_sec:
        return "paused"  # dwell

    # L1 degraded
    l1 = (pd["vol_1m"] > c.vol_degraded_threshold or
          1.0 <= abs(pd["z_trend"]) < c.trend_pause_z or
          self._adverse_fill_ratio() > c.degraded_adverse_threshold)
    if not l1:
        self._l1_entry_time = None   # resetar ao sair do L1
        return "normal"

    # Está em L1 — verificar se deve escalar para L1.5 (safe mode)
    if self._l1_entry_time is None:
        self._l1_entry_time = now   # marcar início do L1

    l1_duration = now - self._l1_entry_time
    adverse_ratio = self._adverse_fill_ratio_10s()
    thrash_count  = self._l2_transitions_in_window(c.safe_thrash_window_sec)

    safe = (l1_duration > c.safe_mode_entry_sec or
            adverse_ratio > c.safe_adverse_threshold or
            thrash_count >= 2)

    if safe:
        if self._safe_entry_time is None:
            self._safe_entry_time = now
        return "safe"
    else:
        self._safe_entry_time = None

    return "degraded"
```

**Saída do safe mode:** em `_evaluate_regime`, "safe" só retorna "degraded" (ou abaixo) quando as condições de entrada cessam por `2 × pause_release_sec`. Implementar como dwell adicional no início da avaliação de "safe": se `regime_anterior == "safe"` e `now - _safe_entry_time < 2 * pause_release_sec`, retornar "safe" mesmo que as condições de entrada tenham cessado.

Regime é consumido em:

- `get_levels_to_execute()`: se `paused` ou `killed`, retorna `[]`.
- `get_executor_config()`: se `degraded`, aplica `spread_multiplier ≥ 1.5` e `size_factor_global *= 0.5`.
- `stop_actions_proposal()`: se `paused`/`killed`, cancela todos os executors ativos (`StopExecutorAction` para cada um).

### 4.4 Observabilidade obrigatória

Toda transição de regime (`normal → degraded`, `degraded → paused`, etc.) deve ser:

- Logada em nível `WARNING` (para normal→degraded) ou `CRITICAL` (para → killed).
- Registrada no `signals.csv` com timestamp.
- Visível em `to_format_status()` como header: `Regime: DEGRADED (cause: vol_1m=0.018)`.

Sem isso, você vai perceber que o bot ficou parado só olhando PnL — tarde demais.

### 4.5 Trade-off explícito

O conjunto de thresholds acima **vai custar volume** em dias normais (o bot vai pausar em picos que acabariam dando certo). Isso é intencional: o tamanho esperado da perda num regime ruim (1-2% em minutos) domina o ganho de 5-10 bps extras por fill em dias calmos. Se você decidir priorizar volume puro (rebate VIP4 é tentador), reduza thresholds em 50% mas aceite sessões ocasionais com drawdown grande. Não recomendado até ter dados de pelo menos 2 semanas em paper.

## 5. Sistema Inteligente de Skew

### 5.0 Arquitetura do líder: sintético vs BTC-USDT puro

**Premissa econômica:** BTC-BRL ≈ BTC-USDT × USDT-BRL. O preço justo do BTC em BRL é, em grande medida, o produto do preço global do BTC em USDT com o preço local do dólar digital em BRL. Logo, o "líder econômico" não é apenas BTCUSDT — é o **sintético `fair_brl = mid_usdt × usdt_brl`**.

Porém, o sintético é mais ruidoso que BTCUSDT puro: adiciona a dinâmica própria do USDT-BRL (prêmio/desconto do dólar digital no Brasil), que tem autocorrelação, spreads mais largos e feeds menos líquidos.

**Consequência arquitetural: usar dois signals com fontes distintas:**

| Sinal | Fonte do "líder" | Horizonte | Uso |
|-------|-----------------|-----------|-----|
| `s_lead_micro` (pause defensivo) | `mid_usdt` **puro** (bookTicker BTC-USDT) | 1–5s | Defesa microestrutural — detectar movimento rápido do global antes do BRL ajustar. FX (USDT-BRL) é ignorado nesta janela: em 5s a variação cambial é ruído puro, não informação |
| `s_lead_regime` (skew contínuo) | `fair_brl = mid_usdt × usdt_brl` **sintético** | 30s–5min | Fair value / basis / desalinhamento econômico. Horizonte suficiente para que a componente cambial seja informativa, não ruído |

**Filtros obrigatórios no sintético:**

1. **Deadband**: `|basis_bps| < basis_deadband_bps` (default 3 bps) → `s_lead_regime = 0`. Variações cambiais de baixa magnitude geram basis que oscila por ruído.
2. **Suavização (EWM)**: `fair_brl_smooth = EWM(fair_brl, halflife=lead_lag_ewm_halflife)` antes de calcular `r_fair_short`. Reduz picos de spread no USDT-BRL sem introduzir lag relevante em 30s.
3. **Staleness check das três pernas** (ver §5.2): BTC-BRL, BTC-USDT e USDT-BRL precisam estar frescos. Se qualquer perna estiver stale, `s_lead_regime = 0` (não `s_lead_micro`, que usa apenas BTCUSDT puro).

**O sintético não comanda sozinho:** mesmo com filtros, não permitir que `s_lead_regime` sozinho acione one-sided ou modifique regime sem validação empírica (Fase 5 gate — §6). Nas Fases 1–4, `w_lead = 0`.

**Complexidade de implementação:** o código extra é pequeno — uma multiplicação e uma série de checks de staleness. A dificuldade real está na qualidade e sincronização dos feeds (confirmar disponibilidade de bookTicker USDT-BRL em §9.5 item 3), não na lógica do controller.

### 5.1 Modelo de preço das ordens (base)

No `MarketMakingControllerBase`, preço sai de:

```
order_price = reference_price * (1 + side_multiplier * spread_in_pct * spread_multiplier)
```

onde `side_multiplier = -1` para BUY, `+1` para SELL (ver `market_making_controller_base.py:304-315`). Isso é simétrico: um `spread_multiplier` sozinho abre/fecha os dois lados igualmente, **não** faz skew.

**Para fazer skew real** (assimétrico entre buy e sell), precisamos **deslocar `reference_price`**. Essa é a alavanca principal do skew inteligente.

Vou chamar o shift de `price_shift_bps` (em basis points). Novo modelo:

```
ref_adjusted = reference_price * (1 + price_shift_bps / 1e4)
order_price  = ref_adjusted * (1 + side_multiplier * spread_in_pct * spread_multiplier)
```

Convenção de sinal:
- `price_shift_bps > 0` → empurra ref para cima → bids ficam mais **altos** (mais prováveis de fill) e asks mais **altos** (menos prováveis). Resultado: bot "compra menos agressivo, vende mais agressivo"? **Errado**. Corrigir:
- `price_shift_bps > 0` significa que queremos **vender mais e comprar menos** (quando `Δ > 0` inventário excedente em BTC). Então: bids baixam, asks baixam. Equivalente: `ref_adjusted = ref * (1 - price_shift_bps/1e4)` quando `Δ > 0`. Para não inverter leitor, convenção final:

**Convenção adotada:** `price_shift_bps > 0` → quero reduzir inventário base (vender) → empurra ref **para baixo** (bids menos atrativos, asks mais atrativos). Implementado como:

```
ref_adjusted = reference_price * (1 - price_shift_bps * 1e-4)
```

`price_shift_bps < 0` → quero acumular base → ref para cima → bids sobem (prob fill ↑), asks sobem (prob fill ↓).

### 5.2 Sinais normalizados

Cada sinal é reduzido a um z-like score em ℝ, depois saturado em `[-1, +1]` via `tanh` para acumulação bem-comportada.

#### s_inv — inventário

```
s_inv = clip(delta / inv_soft_band, -1, 1)
      # delta = inv_pct - target_pct (§2.1)
```

Dentro da soft band → mapeia linearmente para `[-1, 1]`. Além disso satura em ±1. Nunca extrapola.

#### s_lead — lead-lag defensivo (Fase 5+, dois horizontes separados)

**Importante:** o `s_lead` descrito aqui só entra na Fase 5. Antes disso, `w_lead=0`.

Lead-lag é separado em **dois horizontes** para não misturar defesa microestrutural com regime:

**s_lead_micro (1–5s) — defesa de quote (usa BTC-USDT puro, não o sintético):**

Quando USDT moveu em < 5s e BTC-BRL não acompanhou, **as asks (quando USDT subiu) ou as bids (quando USDT caiu) podem ser snipradas**. Não usar como `price_shift_bps`; usar como **pause local** do nível afetado por `lead_micro_dwell_sec` (default 3s).

**Fonte: `mid_usdt` puro (bookTicker BTC-USDT), sem USDT-BRL.** Em 5 segundos, a variação cambial (USDT-BRL) é ruído de micoestrutura do câmbio, não informação sobre BTC. Misturar USDT-BRL aqui geraria falsos sinais de lag que são puramente variação do spread do USDT-BRL.

```
delta_usdt_5s  = mid_usdt_t - mid_usdt_{t-5s}     # movimento global em USDT
# Converter para BRL usando a taxa de câmbio atual como fator de escala fixo
# (não calculamos variação do câmbio — apenas convertemos magnitude):
implied_delta_brl = delta_usdt_5s * usdt_brl_ref   # usdt_brl_ref = valor atual, não delta
actual_delta_brl  = mid_brl_t - mid_brl_{t-5s}
lag_micro = (implied_delta_brl - actual_delta_brl) / mid_brl_t * 1e4   # em bps

# Direção defensiva correta:
# lag_micro > 0: USDT subiu, BRL atrasou → ASKS sub-precificadas (arbs compram BTC-BRL)
# lag_micro < 0: USDT caiu, BRL atrasou → BIDS sobre-precificadas (arbs vendem BTC-BRL)
if lag_micro > lead_micro_threshold_bps: pause SELL side por micro_dwell  # protege asks
if lag_micro < -lead_micro_threshold_bps: pause BUY side por micro_dwell  # protege bids
```

`lead_micro_threshold_bps` default: 5 bps (só age em movimentos grandes do leader global).

**Staleness check para micro:** apenas `mid_usdt` e `mid_brl` precisam estar frescos (< `max_leader_staleness_sec`). O `usdt_brl_ref` é usado apenas como fator de escala de conversão — uma atualização de 60s é aceitável aqui porque o que interessa é o movimento relativo de BTC, não a variação do câmbio.

**s_lead_regime (30s–5min) — contribui para skew contínuo (usa sintético fair_brl = BTCUSDT × USDTBRL):**

Aqui o horizonte é longo o suficiente para que a componente cambial (USDT-BRL) seja informativa — o sintético captura desalinhamento econômico real entre BTC-BRL e o seu fair value global.

```
# 1. Staleness check das 3 pernas (se qualquer uma falhar → s_lead_regime = 0)
if (staleness_mid_brl > max_leader_staleness_sec or
    staleness_mid_usdt > max_leader_staleness_sec or
    staleness_usdt_brl > max_usdt_brl_staleness_sec):   # default: 15s para câmbio
    return 0.0

# 2. Sintético com suavização EWM (reduz picos de spread do USDT-BRL)
fair_brl_raw  = mid_usdt_t * usdt_brl_t
fair_brl_t    = ewm_update(fair_brl_raw, halflife=lead_lag_ewm_halflife)  # default 10s EWM

# 3. Retornos comparados
r_fair_short  = ln(fair_brl_t / fair_brl_{t-30s})
r_actual_short= ln(mid_brl_t  / mid_brl_{t-30s})
lag_short     = r_fair_short - r_actual_short

# 4. Normalizar por sigma do próprio lag (z-score adaptativo)
sigma_lag     = std(lag_short em últimos 10 samples, EWM half-life 3min)
z_lead        = lag_short / max(sigma_lag, eps)
# Direção defensiva: lag_short > 0 (BRL abaixo do fair, USDT subiu) → ELEVA ref → asks sobem
# → arbs não compram asks baratas. Portanto s_lead NEGATIVO quando lag > 0.
# (s_lead > 0 significaria baixar ref = vender mais barato = adverse selection)
s_lead        = -tanh(z_lead / 2)
```

**Deadband:** se `|basis_bps| < basis_deadband_bps` (default 3 bps) → `s_lead_regime = 0`. Em regime calmo, o basis do sintético oscila por ruído de spread USDT-BRL, não por informação.

**`reference_price` base permanece `mid BTC-BRL`** — `fair_brl` (sintético) é sinal auxiliar, **não âncora de preço**. Não substituir `reference_price` por `fair_brl`; isso vira semi-arb triangular e ancora o bot num preço teórico que pode estar errado pelo spread do USDT-BRL.

#### s_vol — volatilidade (não direcional)

`s_vol` **não tem sinal** (é sempre `≥ 0`). Entra como multiplicador em `spread_multiplier`, não em `price_shift_bps`:

```
sigma_1m  = std(log_returns_1m, N=30)
sigma_ref = median(sigma_1m últimos 6h)
vol_ratio = sigma_1m / max(sigma_ref, eps)
spread_multiplier = clip(vol_ratio, 1.0, 3.0)
```

Vol baixa não aperta spread abaixo de 1× (você já tem `buy_spreads` mínimos configurados; apertar mais abaixo disso fere breakeven). Vol alta escancara até 3×.

### 5.3 Combinação em skew total

**Preço (direcional):**

```
skew_raw  = w_inv * s_inv + w_lead * s_lead
skew_norm = tanh(skew_raw)          # re-satura após combinar
price_shift_bps = skew_norm * skew_max_bps
```

**Spread (não direcional):**

```
spread_multiplier = spread_multiplier_vol   # já definido em 5.2
if regime == "degraded": spread_multiplier *= 1.5
```

**Size (não direcional, mas lado-a-lado):**

- `size_factor_favoravel` e `size_factor_adversa` vêm de §2.3, aplicados conforme o sinal de `Δ` (inventário), não de `s_lead`. **Explícito:** lead-lag **não** vira size — só vira skew. Evita que um sinal espúrio faça bot jogar 100% do size num lado.

### 5.4 Pesos iniciais

| Peso | Default | Racional |
|------|---------|----------|
| `w_inv`  | 1.0 | Base de referência. Skew de inventário é o único que tem justificativa microestrutural incontestável (reduzir risco posicional). |
| `w_lead` | 0.0 → 0.1 (Fase 5) | Começa desligado. Sobe para 0.1 na Fase 5 apenas com evidência de paper. **Nunca sobe acima de 0.4 sem ablação completa (§7.4).** |
| `w_vol`  | (só multiplicativo, não entra no skew_raw) | — |
| `skew_max_bps` | **2 bps na Fase 3**; subir para 4 depois de validação; 8 é teto absoluto | Com fee maker ~3 bps, deslocar 8 bps já representa > 2× o custo. Começar com 2 bps e medir. |

Thresholds iniciais para lead-lag (Fase 5):
- `basis_deadband_bps = 3`
- `lead_one_sided_threshold = 2.0`
- `lead_one_sided_release = 0.7`
- `lead_one_sided_dwell_sec = 10`

Todos calibráveis via YAML.

### 5.5 Prioridade e conflito

Em caso de conflito (`s_inv` e `s_lead` com sinais opostos): a **soma ponderada** resolve via `w_inv=1.0, w_lead=0.4` → inventário domina em magnitudes iguais.

**Exemplo do conflito com a direção correta:** inventário pesado em BTC (Δ > 0 → `s_inv > 0` → price_shift positivo → ref cai → vende mais) e simultaneamente BRL está abaixo do fair (USDT subiu → `s_lead < 0` → price_shift negativo → ref sobe → protege sells). Os dois sinais se opõem: inventário quer descarregar rápido, lead-lag quer elevar o preço para não vender barato. Com `w_inv > w_lead`, o bot **ainda vende** (inventário domina) mas a um preço melhor do que venderia sem o lead-lag (o sinal defensivo reduz o desconto). Isso é exatamente o comportamento desejado.

Casos especiais:

- `|s_inv| = 1` (inventário saturado): `s_lead` ainda pode modular, mas size factor adverso já é 0 → lead-lag passa a atuar apenas no lado favorável (ajustando quanto desconta a venda).
- `|s_lead| = 1` (lead-lag extremo): verifica §3.1 para one-sided. One-sided vence skew contínuo.

### 5.6 Limites máximos e mínimos

- `|price_shift_bps| ≤ skew_max_bps` (hard clamp pós-tanh).
- `1.0 ≤ spread_multiplier ≤ 3.0`.
- Skew **nunca** pode deslocar o preço além do `reference_price` a ponto de o bid superar o mid ou o ask ficar abaixo do mid. Validar no `get_executor_config()`:

```python
if trade_type == TradeType.BUY and order_price >= mid:
    order_price = mid * (Decimal("1") - min_maker_distance_bps / Decimal("10000"))
if trade_type == TradeType.SELL and order_price <= mid:
    order_price = mid * (Decimal("1") + min_maker_distance_bps / Decimal("10000"))
```

`min_maker_distance_bps` default 1 bps (= postOnly margin). Isso evita que o bot vire taker por acidente, perdendo o rebate.

### 5.7 Evitar virar direcional demais + freio de quote churn

**Quatro freios contra direcionalidade:**

1. **`skew_max_bps` pequeno** (2 bps na Fase 3, máx 8 bps teto). Num dia com 3% de range, mesmo 8 bps é ruído — mas acima disso começa a virar trend-following.
2. **Lead-lag entra apenas quando `|basis|` é informativo** (fora do deadband) e só na Fase 5.
3. **Inventário é a âncora**: `w_inv ≥ w_lead` sempre. Acumulação excessiva de um lado → `s_inv` cresce em oposição.
4. **`reference_price` nunca sai do `mid BTC-BRL`**: `fair_brl` (derivado de USDT-BRL) entra apenas como sinal de `s_lead`. Nunca substituir `reference_price` por `fair_brl` — isso vira semi-arbitragem triangular e ancora o bot num preço teórico que pode estar errado.

**Freio de quote churn — duas direções (`min_requote_bps` e `force_requote_bps`):**

Dois limiares que controlam o cancel/replace de cada nível:

- `min_requote_bps` (default **1 bps**): **piso** abaixo do qual NÃO se recota mesmo em refresh natural. Evita nervosismo em variações de sinal pequenas.
- `force_requote_bps` (default **15 bps**): **teto** acima do qual recota imediatamente, mesmo dentro do refresh window. Evita ordem ficar pendurada com preço defasado em mercado rápido.

Lógica:

```python
current_price = active_executor.config.entry_price
new_price, _  = self.get_price_and_amount(level_id)
delta_bps     = abs(new_price - current_price) / current_price * Decimal("10000")

# 1. Variação enorme → recota imediato (não esperar refresh natural)
if delta_bps >= self.config.force_requote_bps:
    actions.append(StopExecutorAction(...))   # cancela ativo
    actions.append(CreateExecutorAction(...)) # recria com novo preço
    continue

# 2. Variação pequena no refresh natural → mantém ordem antiga (evita churn)
if is_natural_refresh and delta_bps < self.config.min_requote_bps:
    continue   # mantém executor atual

# 3. Caso default → comportamento padrão de refresh
```

Razão dos dois lados: sem `force_requote_bps`, em mercado rápido (vol > 3× normal) a ordem fica até 60s presa em preço defasado e vira sniper food. Sem `min_requote_bps`, refresh natural recota mesmo quando nada relevante mudou.

**Teste de sanidade** (implementar no `test_pmm_lead_lag_skew.py`): simular série com `s_lead = +1` sustentado por 1h. Verificar que `inv_pct` não dispara (inventário deve crescer um pouco e `s_inv` contra-atacar, levando o skew total para ~0).

### 5.8 Espaço futuro para microprice, OFI, triangular

Manter contrato `update_processed_data()` neutro — o resto do controller consome apenas `reference_price`, `spread_multiplier`, `price_shift_bps`, `inv_delta`, `regime`, `sides_enabled`. Adicionar novos sinais significa apenas:

1. Computar `s_micro`, `s_ofi`, `s_tri` e adicionar em `skew_raw`:
   ```
   skew_raw = w_inv*s_inv + w_lead*s_lead + w_micro*s_micro + w_ofi*s_ofi + w_tri*s_tri
   ```
2. Iniciar pesos em 0 e habilitar um por vez via A/B (§7).

Não adicionar pré-emptivamente. Princípio: cada sinal custa manutenção, teste e chance de overfit. Só entra depois de evidência empírica.

### 5.9 Pseudocódigo consolidado de `update_processed_data()`

```python
async def update_processed_data(self):
    # 1) preços
    mid_brl  = self._get_mid(self.config.connector_name, self.config.trading_pair)
    mid_usdt = self._get_mid(self.config.leader_connector, self.config.leader_trading_pair)
    usdt_brl = self._get_quote_conversion()      # candle close de USDT-BRL
    fair_brl = mid_usdt * usdt_brl

    # 2) inventário
    base, quote = self._get_balances()
    Q  = base * mid_brl + quote
    inv_pct = (base * mid_brl) / Q if Q else Decimal("0.5")
    delta   = inv_pct - self.config.target_inventory_base_pct

    # 3) sinais
    s_inv  = clip(delta / self.config.inv_soft_band, -1, 1)
    z_lead, basis_bps = self._compute_lead_lag(mid_brl, fair_brl)
    s_lead = 0.0 if abs(basis_bps) < self.config.basis_deadband_bps \
             else -math.tanh(z_lead / 2)   # negativo = defensivo; eleva ref quando BRL < fair
    sigma_1m, sigma_ref = self._compute_vol()
    vol_ratio = sigma_1m / max(sigma_ref, 1e-9)

    # 4) skew
    skew_raw  = self.config.w_inv * s_inv + self.config.w_lead * s_lead
    skew_norm = math.tanh(skew_raw)
    price_shift_bps = Decimal(skew_norm) * self.config.skew_max_bps
    spread_mult = Decimal(clip(vol_ratio, 1.0, 3.0))

    # 5) reference ajustado
    ref_adj = mid_brl * (Decimal("1") - price_shift_bps / Decimal("10000"))

    # 6) regime e one-sided
    regime        = self._evaluate_regime()        # "normal"|"degraded"|"paused"|"killed"
    sides_enabled = self._evaluate_sides(delta, z_lead, regime)

    if regime == "degraded":
        spread_mult *= Decimal("1.5")

    # 7) publicar
    self.processed_data.update({
        "reference_price": ref_adj,
        "spread_multiplier": spread_mult,
        "price_shift_bps": price_shift_bps,
        "inv_pct": inv_pct,
        "inv_delta": delta,
        "s_inv": s_inv, "s_lead": s_lead, "vol_ratio": vol_ratio,
        "basis_bps": basis_bps, "z_lead": z_lead,
        "regime": regime,
        "sides_enabled": sides_enabled,
        "leader_staleness_sec": self._leader_staleness(),
    })
    self._csv_log_signals()
```

## 6. Roadmap Incremental

Seis fases. **Fase 1..Fase 6 são fases do roadmap deste bot, não versões do Hummingbot** (que está fixo em V2 desde a Fase 1). Cada fase vira um merge request no branch `claude/pmm-bot-lead-lag-skew-RXuZp`. Não avançar para a próxima sem (a) testes unitários verdes, (b) pelo menos 48h em paper trading e (c) métricas-alvo da fase atingidas.

### Fase 1 — PMM puro (2–3 dias de implementação)

Objetivo: tirar do zero um controller funcional, sem skew, sem regime filter, sem rebalance MARKET. Base sólida para iterar.

Entregáveis:
- `controllers/market_making/pmm_lead_lag_skew.py`: config + controller.
- Extends `MarketMakingControllerBase`, sem override de `update_processed_data` (usa default).
- `get_executor_config()` = cópia do `PMMSimpleController`.
- 2 níveis de buy + 2 níveis de sell, `buy_spreads=[0.0010, 0.0020]`, `sell_spreads` idem, `total_amount_quote` pequeno (ex.: 200 BRL).
- **`skip_rebalance: true` por padrão** — rebalance MARKET no spot BRL pode destruir vários dias de edge; entrar manual ou por kill switch antes de aceitar rebalance automático.
- `executor_refresh_time=60`, `cooldown_time=15` (valores iniciais a medir — ver §8.3).
- `stop_loss=None`, `take_profit=None`, `time_limit=None` (PMM puro).
- Integração com `scripts/v2_pmm_lead_lag.py` usando drawdown global de `v2_with_controllers.py`.
- Teste unitário mínimo: controller instancia, `determine_executor_actions()` retorna 4 `CreateExecutorAction`.

Critério de saída:
- Rodar 24h em **paper trading**. Verificar: (a) ordens criadas/canceladas nos refreshes; (b) fills simulados registrados; (c) `to_format_status` mostra posição. Medir `cancel_rate` e `fill_rate` como baseline.

### Fase 2 — Controle de inventário + one-sided (3–4 dias)

Adicionar:
- Cálculo de `inv_pct`, `inv_delta`, bandas (§2.2, §2.3).
- Reduções progressivas de size (`size_factor`) em `get_executor_config()`.
- Hard cap e one-sided **baseado apenas em inventário** (§3.1 item 1, §2.4).
- CSV `signals.csv` e `orders.csv`.
- Métricas prioritárias a validar (só 5 nesta fase): **turnover, PnL líquido, max drawdown, inventory drift, adverse fill ratio**. O restante instrumentar depois.
- Testes unitários para bandas (valores nos limites, histerese).

Ainda sem skew de preço (`reference_price = MidPrice`, `price_shift_bps = 0`).

Critério de saída:
- 48h em paper com trend simulado. Inventário deve ficar dentro do `inv_hard_band` sem intervenção manual.

### Fase 3 — Skew por inventário + volatilidade (3–4 dias)

Adicionar:
- Sinal `s_inv` → `price_shift_bps` (sem lead-lag ainda).
- `spread_multiplier` baseado em vol (§5.2 `s_vol`).
- **Pesos iniciais conservadores: `w_inv=1.0, skew_max_bps=2`** (não 8). Subir para 4 e depois 8 bps somente com evidência empírica.
- **Freio de quote churn**: só recotar se o novo preço quantizado diferir do atual em pelo menos `min_requote_bps` (default 1 bps). Evita nervosismo em variação de sinal pequena.
- Validação de distance (§5.6).
- A/B: rodar Fase 2 e Fase 3 em dois paper accounts (capital separado) → comparar as 5 métricas prioritárias.

Critério de saída:
- Fase 3 deve ter `inventory_drift_stddev` menor que Fase 2 em ≥ 20% **sem** perder mais de 10% de volume.

### Fase 4 — Filtros de regime (3–4 dias)

Adicionar:
- Regime filter completo (L1/L2/L3, §4) — **sem lead-lag ainda**.
- **"Safe mode" (L1.5)**: modo entre Degraded e Paused — 1 nível por lado, size mínimo, spread largo, sem `price_shift_bps` de lead-lag. Útil quando sistema está instável mas não quebrado.
- One-sided com triggers de tendência e vol (§3.1 itens 2 e 3, exceto lead-lag).
- Kill switch L3 com alerta.

Critério de saída:
- 1 semana em paper. Nenhum trigger de kill não-justificado. Regime transitions não thrashing.

### Fase 5 — Lead-lag como ajuste pequeno

Adicionar lead-lag **somente se a Fase 4 já estiver estável em paper**. Tratar como feature experimental que precisa merecer ficar no sistema:

- Candles feeds de BTC-USDT e USDT-BRL.
- **Lead-lag separado em dois horizontes**:
  - `s_lead_micro` (janela 1–5s): defesa de quote microestrutural — **se BTC-USDT moveu em < 5s e BTC-BRL não acompanhou**, pausa aquele lado por `lead_micro_dwell_sec` (default 3s). Não vira `price_shift_bps`, só pause local de nível.
  - `s_lead_regime` (janela 30s–5min): já coberto pelo regime filter de tendência.
- `w_lead = 0.1` como início. `reference_price` base **permanece `mid BTC-BRL`** — `fair_brl` entra só como sinal auxiliar, **não** como âncora de preço. Evita o bot virar semi-arbitragem triangular.
- Ablação agressiva (§7.4): testar Fase 5 com `w_lead=0.1/0.2/0.4`, com one-sided por lead-lag desligado, com pause por lead-lag desligado. O ganho precisa vir do lead-lag em si, não do filtro de regime.

Critério de saída:
- **Lead-lag precisa provar valor em 2 semanas**: `adverse_fill_ratio_10s` cai ≥ 15% vs Fase 4 **sem** reduzir fills em mais de 15%. Se não, `w_lead → 0` e a Fase 5 vira equivalente à Fase 4 permanentemente.

### Fase 6 — Testes pré-capital e refinamento

Antes de colocar capital real:

1. **Backtest por replay** (§7.1) de pelo menos 30 dias em dados históricos reais.
2. **Paper trading extenso** (2 semanas consecutivas, incluindo pelo menos um fim-de-semana de baixa liquidez).
3. **Calibração**: ajustar thresholds de regime com base nos percentis observados (e.g., `vol_pause_threshold` = p95 da vol horária nos 30 dias).
4. **Dry-run com capital nominal pequeno** (ex.: 200 BRL, 3 dias). Confirmar: (a) ordens reais são aceitas pela Binance com postOnly respeitado; (b) fees batem com tabela VIP4; (c) nenhum erro de precisão decimal; (d) drawdown nulo ou < 0.5%.
5. **Aumento progressivo** (por semana): 500 → 1k → 3k → capital alvo. Aumentar apenas se métricas da semana anterior baterem metas.

### Itens fora do escopo deste roadmap

- Multi-pair (rodar várias BTC-XXX simultaneamente). Só depois da Fase 6 estável.
- Microprice / OFI / order flow imbalance. Fase opcional 7 — só entra depois de evidência empírica clara.
- Hedge no perpetual (BTC-USDT-FUT). Abre complexidade enorme (margin, funding, cross-venue). Não incluir sem redesign.

**Princípio geral de avanço de fase:** a complexidade só justifica entrar se a fase anterior está estável há pelo menos 1 semana em paper. Não acelerar fases para ter "mais alpha" — cada camada não-validada é risco extra. Lead-lag chega na Fase 5, não antes.

## 7. Backtest, Replay e Validação

Realismo antes de ambição: backtest de PMM é traiçoeiro porque a presença do bot mudaria o book, e o fill-simulation é frágil. O objetivo aqui é **refutar** hipóteses (o skew **não** está ajudando), não confirmá-las.

### 7.1 Dados a coletar

| Dado | Fonte | Frequência | Uso |
|------|-------|------------|-----|
| Trades tick-by-tick BTC-BRL | Binance WS `btcbrl@trade` | realtime | Simular quando ordem maker seria preenchida |
| Book snapshots L2 BTC-BRL | WS `btcbrl@depth@100ms` | 100 ms | Posição na fila, estimar prob. de fill |
| Mid BTC-USDT | WS `btcusdt@bookTicker` | realtime | Leader price |
| Mid USDT-BRL | WS `usdtbrl@bookTicker` | realtime | Conversão quote |
| Candles 1m dos 3 pares | REST klines | 1 min | Cálculo de σ e baselines |

Armazenar em Parquet (lz4) particionado por dia (`data/btcbrl/2026-04-24.parquet`). Evitar SQLite para esse volume; Parquet + DuckDB ou pandas/pyarrow dá leitura rápida.

**Duração mínima:** 30 dias corridos. Idealmente 90 dias incluindo pelo menos 1 evento macro (FOMC, decisão COPOM) para ver o filtro de regime em ação.

### 7.2 Simulador de fills (simplificado, honesto)

Não reimplementar um matching engine; construir um simulador heurístico e **documentar o viés**:

```
para cada ordem maker colocada em t:
  fila_na_frente = quantidade no mesmo nível de preço no book em t
  cum_volume    = volume cumulativo de trades no lado oposto em [t, t_refresh]
                  que cruzaria o nosso preço
  fill_simulado = cum_volume > fila_na_frente   (regra FIFO simplificada)
  amount_filled = min(nossa_amount, cum_volume - fila_na_frente)
```

Viés conhecido: (a) ignora cancelamentos de outros makers (superestima fill), (b) ignora nossa ordem empurrando preço (superestima fill de novo), (c) ignora ordens hidden. Compensação prática: aplicar `fill_discount` = 0.7 nos backtests (só conta 70% dos fills simulados). Calibrar comparando com paper trading real depois.

### 7.3 Métricas a rastrear

**Métricas prioritárias para Fases 1–3 (foco nessas 5):**

1. **Turnover** = volume / capital alocado.
2. **PnL líquido** (trading_pnl − fees).
3. **Max drawdown** da sessão.
4. **Inventory drift** = stddev de `inv_pct` intradiário.
5. **`adverse_fill_ratio_10s`** (definido em §4.1): fração de fills onde mid moveu contra > 2 bps em 10s.

O resto instrumentar gradualmente (não antes das Fases 4–5 — não virar coleta de métricas).

**Métricas adicionais (Fase 4+):**

**Operacionais:**
- `Fill rate` = fills / ordens colocadas.
- `Cancel rate` = cancelamentos / ordens. (Se subir > 30% vs baseline → quote churn ou lead-lag nervoso).
- `Force requote rate` = nº de requotes por `force_requote_bps` / hora (proxy de mercado rápido).

**Inventário:**
- `inv_pct` médio e stddev (quanto o bot oscila em torno do target).
- % do tempo com `|Δ| > inv_soft_band`, `> inv_hard_band`, `> inv_hard_cap`.
- Max `|Δ|` da sessão.

**Qualidade de fill / alpha:**
- `adverse_fill_ratio_10s` e `adverse_fill_ratio_60s` (§4.1): primário e de monitoramento longo. Trigger de regime apenas no de 10s.
- `adverse_cluster_count` (1s): trigger L2, monitorar pico por dia.
- `Edge por fill (bps)` = (preço do fill − mid no momento do fill) * sign(side).
- `Realized edge (bps)` = edge médio − fee.

**PnL:**
- `PnL bruto` (trading), `Fees pagas`, `PnL líquido`.
- `Drawdown máximo` da sessão.
- `PnL / turnover` (bps).

**Regime:**
- % do tempo em `normal | degraded | paused | killed`.
- Número de transições `degraded → paused`.
- Volume perdido por estar pausado (estimativa: qual seria o fill se estivesse rodando normal — usar o simulador contrafactual).

### 7.4 Experimentos A/B

**Setup:** rodar duas instâncias paralelas do controller, mesmo par, mesma config, **capital separado**, com `variant_id` em logs. Não misturar capital (enviesa por balance compartilhado).

**Experimentos mínimos:**

1. **Fase 2 (PMM + inventário) vs Fase 3 (com skew de inventário/vol)**:
   - Hipótese nula: `inv_drift_stddev` é igual.
   - Métrica primária: `inv_drift_stddev`.
   - Secundárias: volume, `adverse_fill_ratio_10s`.
   - Duração: mínimo 1 semana.
   - Aceitar Fase 3 se `inv_drift_stddev` cai ≥ 20% com volume caindo < 10% e adverse ratio sem piorar.

2. **Fase 4 (com filtros de regime) vs Fase 4 sem regime filter**:
   - Hipótese nula: drawdown max igual.
   - Primária: `max_drawdown / capital`.
   - Secundária: volume semanal.
   - Aceitar filtros se drawdown cai e volume não cai mais que 20%.

3. **Fase 4 vs Fase 5 (com lead-lag)**:
   - Hipótese nula: `adverse_fill_ratio_10s` é igual.
   - Métrica primária: `adverse_fill_ratio_10s`.
   - Secundária: edge líquido por fill.
   - Duração: 2 semanas mínimo (lead-lag tem poucos eventos informativos; precisa de amostra).
   - Aceitar Fase 5 se adverse ratio cai ≥ 15% **sem** reduzir volume de fills em mais de 15%. Se volume cair muito, lead-lag está pausando trades que teriam sido bons.

4. **Fase 5 com `w_lead=0.1` vs `w_lead=0.2` vs `w_lead=0.4`** (ablação dentro da Fase 5):
   - Confirmar que aumentar `w_lead` produz ganho marginal positivo até certo ponto.
   - Se `w_lead=0.1` já capta a maior parte do benefício → manter conservador.

**Significância estatística:** para comparações de médias, teste t pareado por hora-do-dia (pareamento reduz ruído intradiário). Exigir `p < 0.05` e **effect size** mínimo (Cohen's d > 0.3) — p-valor sozinho não convence com N grande.

### 7.5 Validação de que o skew está ajudando (não piorando)

Checklists específicos para não se enganar:

- **Edge por fill do lado favorecido pelo skew é maior que do lado desfavorecido?** Se skew empurra ref pra baixo (vende mais agressivo), asks fecham a um preço melhor que bids abrem? Se **não**, skew está comendo edge em vez de ganhar.
- **Correlação lead-skew vs adverse-next-10s é negativa?** Ou seja: quando o skew é forte (s_lead alto), os fills que acontecem têm adverse menor que quando skew é zero. Se a correlação for zero ou positiva → lead-lag não está agregando.
- **Teste contrafactual**: anotar preço do mid 10s após cada fill. Calcular `hypothetical_edge` sem skew (usando mid puro como ref) e comparar com `actual_edge`. Se `actual_edge - hypothetical_edge` é estatisticamente > 0 → skew funciona.

### 7.6 Ferramental (esqueleto)

Criar notebook `analysis/pmm_analysis.ipynb` que lê `signals.csv`, `orders.csv`, `fills.csv` e produz:

- Séries temporais `inv_pct`, `price_shift_bps`, `regime` alinhadas.
- Heatmap de adverse ratio por regime e por bucket de `|s_lead|`.
- Tabela resumo de métricas por variant_id e por dia.
- Histograma de `edge_bps` por lado e por regime.

Não precisa de web UI: notebook + matplotlib é suficiente para 1 dev.

## 8. Riscos e Falhas

### 8.1 Overfitting

**Sintoma:** parâmetros ajustados em backtest dão PnL ótimo, mas em paper/real decepcionam.

**Mitigações:**
- **Menos parâmetros, valores conservadores.** Começar com `w_lead=0.0` e `skew_max_bps=2` (Fase 3); só subir `w_lead` para 0.1 na Fase 5 com evidência empírica. Resistir à tentação de procurar "o ótimo" em grid search de backtest.
- **Walk-forward validation:** calibrar thresholds em 2/3 do histórico, testar em 1/3 fora da amostra. Se performance no out-of-sample for < 50% do in-sample → overfit.
- **Sanity check simples:** performance com `w_lead=0` (puro inventário, equivale à Fase 4) deve ser **próxima** da Fase 5 com `w_lead=0.1`. Se habilitar lead-lag dobrar o PnL, algo está errado (provavelmente look-ahead bug ou viés de simulador).
- **Testar em outro par** (BTC-USDT Binance) antes de confiar. Se parâmetros não generalizam nem para o próprio leader, overfit.

### 8.2 Latência

**Contexto:** sem co-location, latência casa-exchange típica do Brasil → Binance é 80-200 ms, com jitter. Para market making de latência crítica (competição por fila), isso é desvantagem estrutural.

**Consequências:**
- Skew baseado em lead-lag em janelas < 1s é inútil (o sinal chega tarde). Usar janelas curtas ≥ 30s (§5.2) é defesa contra isso.
- Refresh de ordem (cancel + create) pode atravessar um fill iminente (race condition).

**Mitigações:**
- **Para `s_lead_regime` (skew contínuo):** nunca confiar em janelas curtas. Não descer `lead_lag_short_window_sec` abaixo de 20s. Com latência 80-200 ms, o sinal de 5s chega quando o movimento já aconteceu — ajustar o preço com base nisso é seguidismo, não defesa.
- **Para `s_lead_micro` (pause defensivo):** a janela de 1–5s é intencional e não viola essa regra — não é skew de preço, é apenas "não criar ordem nova neste tick". Nenhum cancel ocorre, nenhuma API call é feita; é um filtro na lista de levels. O custo de falso positivo (não criar uma ordem por 3s) é muito menor que o de adverse selection. A latência não impede esse uso.
- `executor_refresh_time ≥ 30s` na Fase 1 (ordens vivem pelo menos 30s). Reduzir depois com evidência.
- Usar `LIMIT_MAKER` (postOnly) — se a ordem chegaria agressiva por latência, a exchange rejeita. Evita virar taker por acidente.
- `max_leader_staleness_sec = 5` (§4.1 L2) pausa se o feed leader travar.

### 8.3 Sinais conflitantes

**Risco específico:** `s_inv` diz "reduzir BTC" e `s_lead` diz "BTC vai subir, acumular". A fórmula atual (§5.3) resolve pela soma ponderada com `w_inv > w_lead`, mas em casos extremos ambos saturam e o sinal dominante pode não ser o desejado.

**Mitigações:**
- Prioridade hardcoded: inventário **sempre** vence na decisão de one-sided (§3.2). Sinal contínuo pode permitir lead-lag modular, mas escolha binária (ligar/desligar lado) é só do inventário.
- Logar explicitamente em `signals.csv` quando `sign(s_inv) != sign(s_lead)` para auditoria.
- Teste unitário: injetar `s_inv=+1, s_lead=-1` e verificar que `price_shift_bps` ainda tem sinal de `s_inv`.

### 8.4 Lead-lag piora em vez de melhorar

**Risco real, não hipotético.** Lead-lag pode falhar porque:

- **O sintético `fair_brl = BTCUSDT × USDTBRL` é mais ruidoso que BTCUSDT puro.** A componente USDT-BRL tem autocorrelação própria, spreads mais amplos e liquidez menor que BTCUSDT. Em janelas < 30s, o basis oscila predominantemente por ruído cambial — daí o micro pause usar apenas BTCUSDT puro (§5.0, §5.2).
- Market makers do BTC-BRL já incorporam lead-lag — se você está usando um sinal que todos usam, você chega tarde.
- Em regimes de baixa vol, o basis do sintético oscila por ruído de spread USDT-BRL, e o skew gera cancel/replace sem benefício → mais custo operacional.

**Mitigações:**
- Deadband em `basis_bps` + EWM no sintético (§5.0 + §5.2) filtram ruído.
- **Toggle de ablação**: `w_lead` é config — sempre permitir rodar com `w_lead=0` e comparar. Se a Fase 5 não bate a Fase 4 por 2 semanas, lead-lag **não está ajudando** → zerar permanentemente.
- Monitorar `cancel_rate`. Se subir > 30% na Fase 5 vs Fase 4 sem correspondente ganho de edge, lead-lag está fazendo churn sem alpha.
- **Checar correlação temporal BTC-USDT vs USDT-BRL** no histórico: se USDT-BRL e BTCUSDT estiverem correlacionados no horizonte de 30s–5min do regime signal, o sintético pode estar inflando o sinal direcional (ambos sobem juntos em risk-on → sintético move muito, BTC-BRL acompanha, basis fica pequeno). Monitorar `corr(r_usdt_brl, r_btcusdt, 5min)` nos logs.

### 8.5 Precisão decimal / rounding

Hummingbot usa `Decimal` internamente, mas misturar com `float` em cálculo de sinais gera bugs sutis (`TypeError`, arredondamento inesperado).

**Mitigações:**
- `Decimal` para tudo que vira preço ou quantidade de ordem.
- `float` (via `Decimal → float` explicit) apenas em cálculos de `math.tanh`, `std`, etc. Converter de volta para `Decimal` antes de multiplicar por preço.
- `quantize_order_price` / `quantize_order_amount` obrigatórios antes de submeter ordem.

### 8.6 Risco de exchange (fora do código)

- **Flash crash BTC-BRL**: pares com quote local podem ter liquidez evaporando em eventos. `stop_loss` individual por executor é frágil (MARKET em book vazio é prejuízo garantido). Defesa principal: kill switch L3 por `inv_kill` e `max_session_drawdown`.
- **VIP tier rebalance**: fees Binance mudam por volume 30d. Se você cair de VIP4, maker fee pula. Monitorar mensalmente. Hardcode de fee em `binance_utils.py:12-16` não reflete VIP4 — sobrescrever via `client_config_map`.
- **API rate limits**: POST order tem peso 1, mas limite por sub-segundo. Com 4 levels × refresh 60s, estamos longe do limite. Se reduzir refresh para 10s, reavaliar.

### 8.7 Risco operacional (desenvolvedor único)

- **Bot roda, dev dorme**: kill switch L3 **obrigatório** desde a Fase 4. Alerta via webhook (Telegram/Discord) em transição para `killed`. Não confiar em "vou acordar".
- **Sem CI que rode o controller contra fixtures**: graças à modularização da §1.7, os testes das funções puras (`compute_*`) cobrem a maior parte da lógica desde a Fase 1. `test/controllers/market_making/test_pmm_lead_lag_utils.py` roda no CI existente do repo.
- **Deploy sem staging**: sempre rodar 24h em paper após mudança antes de ir para real. Documentar no README do controller.

### 8.8 Trade-offs explícitos (cético)

| Escolha | Vantagem | Custo assumido |
|---------|----------|----------------|
| `skew_max_bps` pequeno (2 na Fase 3, teto 8) | Evita virar direcional; preserva buffer vs maker fee 3 bps (breakeven bilateral = 6 bps) | Lead-lag forte tem efeito limitado; pode não compensar manter o sinal |
| `w_inv >> w_lead` (`w_lead=0` até Fase 5) | Inventário âncora | Se lead-lag tiver de fato alpha em BRL, bot deixa dinheiro na mesa nas fases iniciais |
| Regime filter agressivo | Protege drawdown | Volume cai em dias de alta vol |
| PMM puro (sem take-profit) | Simples, mais fills | Posição acumula até refresh; pior em trend long |
| `executor_refresh_time=60s` | Menos cancel | Posições envelhecem; mitigado por `force_requote_bps` |
| Sem hedge futuro | Sem margin, sem funding | Inventário = exposição direcional 1:1 |
| `skip_rebalance=true` por padrão | Não fura breakeven com MARKET | Inventário pode ficar enviesado por sessões inteiras; precisa intervenção manual em casos extremos |
| Backtest com fill_discount 0.7 | Conservador | Pode subestimar volume real; decisão de descartar variante pode ser prematura |

## 9. Apêndice

### 9.1 Arquivos a criar/modificar

**Criar:**
- `controllers/market_making/pmm_lead_lag_skew.py` — controller + config (principal entregável).
- `controllers/market_making/pmm_lead_lag_utils.py` — funções puras (dataclasses + cálculo de cada estado), obrigatório desde a Fase 1 (ver §1.7).
- `scripts/v2_pmm_lead_lag.py` — script de boot derivado de `scripts/v2_with_controllers.py`, instancia o controller com YAML de config e adiciona drawdown global.
- `conf/controllers/conf_pmm_lead_lag_skew.yml` — config YAML de exemplo para o controller (padrão Hummingbot V2 usa diretório de configs de controller específico; confirmar path durante implementação).
- `test/controllers/market_making/test_pmm_lead_lag_skew.py` — baseado em `test/hummingbot/strategy_v2/controllers/test_market_making_controller_base.py`.
- `analysis/pmm_analysis.ipynb` — notebook de análise de `signals.csv`/`orders.csv`/`fills.csv`.
- `logs/pmm_lead_lag/` — diretório para CSVs (criado em runtime).

**Não modificar** o core do Hummingbot (mantém compatibilidade com upstream). Se for estritamente necessário (e.g., fees VIP4), sobrescrever via `ClientConfigMap` em vez de editar `binance_utils.py`.

### 9.2 Referências-chave de código reutilizado

| Componente | Caminho | Linhas relevantes |
|------------|---------|-------------------|
| MarketMakingControllerBase | `hummingbot/strategy_v2/controllers/market_making_controller_base.py` | 17-210 (config), 223-315 (actions), 339-414 (rebalance spot) |
| PositionExecutor | `hummingbot/strategy_v2/executors/position_executor/position_executor.py` | 427-443 (open), 450-567 (triple barrier), 702-750 (status) |
| TripleBarrierConfig | `hummingbot/strategy_v2/executors/position_executor/data_types.py` | 12-45 |
| PMMSimple (molde) | `controllers/market_making/pmm_simple.py` | 1-32 |
| PMMDynamic (padrão de override) | `controllers/market_making/pmm_dynamic.py` | 73-127 |
| Script com drawdown global | `scripts/v2_with_controllers.py` | 48-86 |
| MarketDataProvider | `hummingbot/data_feed/market_data_provider.py` | 177, 501 (candles), 362 (order book) |
| Volatility indicators | `hummingbot/strategy/__utils__/trailing_indicators/instant_volatility.py` | 1-50 |
| Binance fees | `hummingbot/connector/exchange/binance/binance_utils.py` | 12-16 |
| Kill switch (global, complementar) | `hummingbot/core/utils/kill_switch.py` | 24-77 |

### 9.3 Config YAML inicial (draft)

```yaml
# conf/controllers/conf_pmm_lead_lag_skew.yml
controller_name: pmm_lead_lag_skew
id: pmm_btc_brl
connector_name: binance
trading_pair: BTC-BRL

# Leader e conversão
leader_connector: binance
leader_trading_pair: BTC-USDT
quote_rate_pair: USDT-BRL

# Capital e níveis (Fase 1)
total_amount_quote: 200       # R$ 200 no início
buy_spreads:  [0.0010, 0.0020]  # 10 bps, 20 bps
sell_spreads: [0.0010, 0.0020]
buy_amounts_pct:  [0.5, 0.5]
sell_amounts_pct: [0.5, 0.5]

# Executor
# Testar 15s, 30s, 60s — não assumir 60s como ideal.
# 60s envelhece quote mas reduz cancel rate; 15s atualiza mais mas pode aumentar churn.
executor_refresh_time: 60
cooldown_time: 15
leverage: 1

# Triple barrier — PMM puro (sem SL/TP)
stop_loss: null
take_profit: null
time_limit: null

# Rebalance spot
# skip_rebalance=true por padrão: rebalance MARKET no spot BRL pode destruir dias de edge.
# Rebalancear manualmente ou via kill se necessário.
position_rebalance_threshold_pct: 0.25
skip_rebalance: true

# === Inventário (Fase 2) ===
target_inventory_base_pct: 0.5
inv_soft_band: 0.10
inv_hard_band: 0.20
inv_hard_cap:  0.30
inv_kill:      0.40
max_net_position_quote: 60       # 30% do capital

# === Skew (Fase 3+) ===
w_inv: 1.0
w_lead: 0.0                      # desligado; sobe para 0.1 na Fase 5 somente com evidência
skew_max_bps: 2                  # Fase 3=2, Fase 4=2, Fase 5=2→4 se validado. Teto absoluto: 8
min_maker_distance_bps: 1
min_requote_bps: 1               # não recota se novo preço diferir < 1 bps do atual
force_requote_bps: 15            # recota imediato se delta >= 15 bps, mesmo dentro do refresh

# === Lead-lag micro (Fase 5) ===
lead_micro_window_sec: 5
lead_micro_threshold_bps: 5
lead_micro_dwell_sec: 3

# === Lead-lag regime (Fase 5) — sintético BTCUSDT × USDTBRL ===
lead_lag_short_window_sec: 30
lead_lag_long_window_sec:  300
basis_deadband_bps: 3          # se |basis| < X bps → s_lead_regime = 0 (filtro de ruído)
lead_lag_ewm_halflife: 10      # segundos; suaviza picos de spread do USDT-BRL no sintético
max_leader_staleness_sec: 5    # staleness de BTC-USDT (micro e regime)
max_usdt_brl_staleness_sec: 15 # staleness de USDT-BRL (só regime; micro usa BTCUSDT puro)

# === Vol / regime (Fase 4) ===
vol_degraded_threshold_mult: 3.0  # vs mediana 6h
vol_pause_threshold_mult:    5.0
trend_pause_z: 1.5
trend_one_sided_z: 1.5
trend_one_sided_release: 0.6
pause_basis_bps: 30
pause_release_sec: 60

# === Adverse fill (Fases 2+ métrica, Fase 4 trigger) ===
degraded_adverse_threshold: 0.6   # adverse_fill_ratio_10s > X → L1
safe_adverse_threshold:     0.7   # adverse_fill_ratio_10s > X → L1.5

# === Safe mode (Fase 4) ===
safe_mode_entry_sec: 120          # tempo contínuo em L1 para entrar em L1.5
safe_thrash_window_sec: 600       # janela de detecção de thrashing degraded↔paused

# === Kill switch (Fase 4) ===
max_session_drawdown_quote: 10    # R$10 = 5% do capital inicial
critical_error_threshold: 5
```

### 9.4 Verificação end-to-end

Depois de cada fase:

1. **Unit tests**
   ```bash
   cd /home/user/hummingbot-ney
   python -m pytest test/controllers/market_making/test_pmm_lead_lag_skew.py -v
   ```

2. **Smoke test (paper trading)**
   - Subir `paper_trade` connector: `config create` → escolher `paper_trade` → saldo inicial BTC=0.001, BRL=200.
   - Rodar: `start --script v2_pmm_lead_lag.py`.
   - Verificar em ≤ 2 min: `status` mostra `Regime`, `inv_pct`, 2 bids + 2 asks ativas.
   - Matar + revisar `logs/pmm_lead_lag/signals.csv` — deve ter N linhas crescentes.

3. **Teste de regime manual (Fase 4)**
   - Injetar candle sintético com vol alta via mock ou usar período histórico conhecido (FOMC).
   - Verificar transição para `degraded` e depois `paused`.
   - Verificar dwell de 60s antes de voltar a `normal`.

4. **A/B em paper (Fases 3 e 4)**
   - Rodar duas instâncias do script com configs diferentes.
   - Comparar `analysis/pmm_analysis.ipynb`: `inventory_drift_stddev`, `adverse_fill_ratio_10s`, `edge_per_fill`.

5. **Dry-run com capital real mínimo (Fase 6)**
   - `connector=binance` real, `total_amount_quote=200` BRL.
   - 3 dias de monitoramento. Checar: ordens aceitas com postOnly, fees batem com VIP4, nenhum erro crítico em log.

### 9.5 Questões em aberto (resolver na implementação)

1. **Tarifa VIP4 real:** confirmar via API Binance se maker spot BTC-BRL é rebate ou fee reduzida. Impacta diretamente `skew_max_bps` viável (rebate permite ser mais agressivo).
2. **Latência típica medida:** rodar ping contra `api.binance.com` do ambiente de produção por 1 dia → definir se `executor_refresh_time=60` é adequado ou exagerado.
3. **Feed USDT-BRL (dois usos distintos):**
   - Para `s_lead_regime` (sintético): candle-close 1m é aceitável. Confirmar que `binance_spot_candles` suporta `USDT-BRL` ou `BRL/USDT` invertido — checar `exchange_symbol`. Se não suportar, derivar via `market_data_provider.rate_oracle`.
   - Para `s_lead_micro`: **não é necessário** USDT-BRL em real-time. O micro pause usa BTC-USDT puro (bookTicker) + `usdt_brl_ref` como fator de escala estático (atualização 1m é suficiente — veja §5.2). Isso simplifica a Fase 5: não depende de bookTicker USDT-BRL para o micro, apenas para o regime sintético.
   - Confirmar latência e taxa de atualização do bookTicker USDT-BRL antes de usar como `usdt_brl_ref` no sintético (§5.0).
4. **Path correto do YAML de controller config:** Hummingbot V2 tem convenção própria (`conf/controllers/` ou equivalente). Confirmar ao abrir o repo.
5. **Alerta em kill:** integração com Telegram/Discord webhook — decidir qual na Fase 4.

### 9.6 Estratégia de criação/edição de arquivos resiliente a timeout de API

Contexto: sessões de geração de código com IA podem ser interrompidas (timeout, limite de contexto, falha de rede) antes de um arquivo longo ser concluído. Esta seção define como estruturar o trabalho para que interrupções nunca causem perda ou corrupção de trabalho.

#### Princípios

1. **Preferir `Edit` (append incremental) sobre `Write` (substituição total):** `Write` substitui o arquivo inteiro em um único call — se o call falhar a meio, o arquivo fica vazio ou corrompido. `Edit` opera em diff pequeno; se falhar, o arquivo está no estado anterior (seguro).

2. **Um bloco lógico por commit:** cada função pura, cada classe, cada método não-trivial deve ser commitado separadamente. Nunca enviar 300 linhas em um único commit — se falhar, não sabemos o que está ok.

3. **Arquivo de progresso companion:** criar `controllers/market_making/IMPL_PROGRESS.md` (não versionar no final, pode ser `.gitignore`d depois) como checkpoint. Atualizar a cada função implementada. Exemplo de entrada:

```markdown
# PMM Lead-Lag — Estado da implementação
- [x] PMMLeadLagSkewConfig — Pydantic model completo
- [x] compute_inventory_state() — testada
- [x] compute_vol_state() — testada
- [ ] compute_lead_state() — EM ANDAMENTO
- [ ] compute_regime_state()
- [ ] compute_skew_state()
- [ ] compute_side_permissions()
- [ ] compute_order_params()
- [ ] PMMLeadLagSkewController — esqueleto
- [ ] update_processed_data() override
- [ ] get_levels_to_execute() override
- [ ] get_executor_config() override
- [ ] to_format_status() override
```

#### Ordem de implementação (sequência resistente a interrupção)

Cada item abaixo é uma unidade atômica — pode ser interrompida e retomada:

**Passo A — `pmm_lead_lag_utils.py` (dataclasses + funções puras):**
```
A1. imports + dataclasses (InventoryState, VolState, LeadState, RegimeState, SkewState, OrderParams)
A2. compute_inventory_state() — função + teste unitário
A3. compute_vol_state()       — função + teste unitário
A4. compute_lead_state()      — placeholder neutro (retorna zeros) + teste
A5. compute_regime_state()    — placeholder "normal" + teste
A6. compute_skew_state()      — usa inv, ignora lead por ora + teste
A7. compute_side_permissions() — usa inv_delta + teste
A8. compute_order_params()    — aplica skew + safeguard mid + teste
→ commit: "feat(utils): all pure functions, Fase 1 stubs"
```

**Passo B — `pmm_lead_lag_skew.py` (Config + Controller esqueleto):**
```
B1. PMMLeadLagSkewConfig(MarketMakingControllerConfigBase) — campos Fase 1 + validators
B2. PMMLeadLagSkewController — __init__, _fetch_prices, _fetch_balances
B3. update_processed_data() — Fase 1 (sem skew, sem regime)
B4. get_levels_to_execute() — filtro de sides_enabled
B5. get_executor_config() — copia pmm_simple, aplica size_factor
B6. to_format_status() — header: regime, inv_pct, sides_enabled
→ commit: "feat(controller): Fase 1 skeleton, bilateral PMM, no skew"
```

**Passo C — `scripts/v2_pmm_lead_lag.py`:**
```
C1. derivar de v2_with_controllers.py, instanciar controller
C2. drawdown global check
→ commit: "feat(script): boot script com drawdown global"
```

**Passo D — `conf/controllers/conf_pmm_lead_lag_skew.yml`:**
```
D1. YAML com valores de Fase 1 (§9.3)
→ commit: "conf: YAML inicial Fase 1"
```

**Passo E — Testes mínimos `test_pmm_lead_lag_skew.py`:**
```
E1. test_controller_creates_4_actions() — Fase 1 smoke test
E2. test_inventory_bands() — sign correctness (Δ > 0 → buy penalizado)
E3. test_side_filter() — sides_enabled filtra corretamente
→ commit: "test: Fase 1 unit tests"
```

#### Regras de edição durante sessão de IA

- **Ler o arquivo antes de editar** (`Read` com offset): nunca assumir que o conteúdo está como esperado — um edit anterior pode ter falhado silenciosamente.
- **Strings de replace únicas:** o tool `Edit` falha se `old_string` não for único. Usar contexto suficiente (±3 linhas) para tornar a string inequívoca.
- **Não misturar criação e correção no mesmo call:** se está criando função nova, não corrigir outra no mesmo diff — fica impossível saber qual causou falha se a sessão cair.
- **Comprimento máximo por call:** funções com mais de ~60 linhas devem ser escritas em 2–3 `Edit` calls (esqueleto primeiro, corpo depois).
- **Após cada `Edit` que falhar com "string not found":** ler a seção afetada com `Read` (offset+limit), copiar a string exata e repetir o edit — nunca adivinhar.
- **Commits frequentes no branch `claude/pmm-bot-lead-lag-skew-RXuZp`:** cada passo A1..E3 deve ter seu commit. Git é o checkpoint persistente contra qualquer falha de sessão.

#### Recuperação após interrupção

Se a sessão cair no meio de um passo:
1. Ler `IMPL_PROGRESS.md` para saber o último passo commitado.
2. `git status` para ver o que ficou sem commit.
3. Se arquivo está consistente (compilável/importável), commitar. Se está incompleto, `git checkout -- <arquivo>` para voltar ao estado anterior ao passo.
4. Recomeçar pelo sub-passo não commitado.

Nenhum trabalho é perdido além do sub-passo atual, que é sempre < 60 linhas.
