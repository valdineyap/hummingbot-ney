# Hummingbot NEY — CLAUDE.md

Guia de contexto para sessões de desenvolvimento com Claude Code.

---

## Ambiente

- **Hummingbot**: v2.13.0, instalado via Conda em EC2 Ubuntu (4 GB RAM)
- **Exchange A (maker)**: Binance — par `BTC-BRL`
- **Exchange B (taker)**: Bybit — par `BTC-BRL`
- **Saldo de teste**: ~R$4.000 BRL + 0,01 BTC em cada exchange (~$3.190 USD total)
- **Estratégia ativa**: `scripts/simple_xemm.py` (Script V2)

---

## Objetivo principal

**Maximizar volume de trading**, não PnL. O volume acumulado nas exchanges gera redução de fee tier (ex: Binance VIP 1 → 0,09% maker a partir de $1M/mês). O spread pode ser fino; o que importa é o número de fills por dia.

---

## Arquitetura V2

O Hummingbot V2 organiza a lógica em três camadas:

```
scripts/
  └── simple_xemm.py          ← camada de orquestração (StrategyV2Base)
        │  on_tick() → lê mercado, cria/cancela ordens, chama hedger
        │  did_fill_order() → dispara hedge no taker ao preencher maker
        ▼
controllers/
  └── generic/xemm_multiple_levels.py   ← Fase 2 (controller + multiple levels)
        │  determine_executor_actions() → cria/para XEMMExecutorConfig por nível
        ▼
hummingbot/strategy_v2/executors/
  └── xemm_executor/xemm_executor.py   ← gerencia um único par maker/taker
        │  control_task() → update preços, coloca maker, detecta fill, coloca taker
```

### Scripts (Fase 1 — atual)
- Herdam de `StrategyV2Base`
- `on_tick()` roda a cada tick do clock (padrão 1 s)
- Controlam diretamente as ordens via `self.buy()`, `self.sell()`, `self.cancel()`
- Config via `StrategyV2ConfigBase` (Pydantic), lida automaticamente pelo CLI

### Controllers (Fase 2 — futuro)
- Herdam de `ControllerBase`
- `determine_executor_actions()` retorna lista de `ExecutorAction` (criar/parar executors)
- Usados pelo script genérico `scripts/v2_with_controllers.py`
- `xemm_multiple_levels.py`: mantém N executors por nível de spread, com controle de imbalance entre buys/sells preenchidos

### Executors
- Herdam de `ExecutorBase`
- Cada instância gerencia **um único ciclo** maker → fill → taker
- `XEMMExecutor`: coloca maker limit → ao completar, dispara taker market → finaliza
- Têm `update_interval` próprio (padrão 1 s) e retry automático

---

## Parâmetros atuais do simple_xemm.py

| Parâmetro | Valor padrão | Descrição |
|---|---|---|
| `maker_connector` | `kucoin_paper_trade` | Trocar para `binance` |
| `taker_connector` | `binance_paper_trade` | Trocar para `bybit` |
| `maker_trading_pair` | `ETH-USDT` | Trocar para `BTC-BRL` |
| `taker_trading_pair` | `ETH-USDT` | Trocar para `BTC-BRL` |
| `order_amount` | `0.1` (base) | Ajustar para BTC (ex: `0.0005`) |
| `target_profitability` | `0.001` (0,1%) | Spread alvo acima dos custos |
| `min_profitability` | `0.0005` (0,05%) | Spread mínimo antes de cancelar |
| `max_order_age` | `120 s` | Idade máxima antes de refreshar |

---

## Fluxo de execução (simple_xemm.py)

```
on_tick()
  ├── get_price_for_volume(taker, buy)   → taker_buy_result
  ├── get_price_for_volume(taker, sell)  → taker_sell_result
  │
  ├── se não há buy ativo:
  │     maker_buy_price = taker_sell / (1 + target_profit)
  │     coloca BUY limit no maker
  │
  ├── se não há sell ativo:
  │     maker_sell_price = taker_buy / (1 - target_profit)
  │     coloca SELL limit no maker
  │
  └── para cada ordem ativa:
        se profitability < min_profit OU idade > max_age → cancela

did_fill_order(event)
  ├── fill de buy maker  → place_sell_order(taker)   [hedge]
  └── fill de sell maker → place_buy_order(taker)    [hedge]
```

**Atenção:** o hedge usa `OrderType.LIMIT` com preço do orderbook snapshot. Em mercados rápidos, o limite pode não preencher imediatamente.

---

## Plano de evolução

| Fase | Componente | Status |
|---|---|---|
| 1 | `scripts/simple_xemm.py` otimizado para volume | **ativa** |
| 2 | `controllers/generic/xemm_multiple_levels.py` com múltiplos níveis | futuro |

---

## Convenções de desenvolvimento

1. **Branch de trabalho**: `claude/customize-trading-strategies-furbW`
2. **Não modificar** os executors em `hummingbot/strategy_v2/executors/` — são código upstream
3. **Scripts customizados** ficam em `scripts/` com prefixo `ney_` quando forem versões próprias (ex: `scripts/ney_xemm.py`)
4. **Controllers customizados** ficam em `controllers/generic/` com prefixo `ney_`
5. Configurações de instância ficam em `conf/scripts/` (ignoradas no git se contiverem API keys)
6. **Objetivo de cada mudança**: documentar no commit se aumenta fill rate, reduz latência, ou melhora gestão de inventário
7. Parâmetros de produção para BTC-BRL:
   - `order_amount`: começar com `0.0005` BTC (~R$160 por ordem)
   - Fee base Binance spot: 0,1% maker / 0,1% taker → custo total ~0,2% por ciclo
   - Fee base Bybit spot: 0,1% maker / 0,1% taker → idem
