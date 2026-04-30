# Plano: XEMM Lead-Lag para BTC-BRL com sinal sintético

> **Status do plano**: revisado v2 após crítica externa. Veja seção 15 ("Revisão v2") para o consolidado de mudanças.
>
> **Chunks**:
> - [x] Chunk 1: Contexto + Arquitetura + Estrutura de arquivos
> - [x] Chunk 2: Módulo `lead_lag_signal.py`
> - [x] Chunk 3: Controller `xemm_lead_lag.py`
> - [x] Chunk 4: Testes unitários do signal module
> - [x] Chunk 5: Testes do controller
> - [x] Chunk 6: Config YAML + Shadow mode + Verificação

---

## 1. Contexto

### Problema
Operar market making em BTC-BRL é atrativo (spreads largos, fees VIP4 baixas na Binance ~1.2 bps maker), mas há dois riscos principais:

1. **Adverse selection**: o mercado BRL tem latência maior que mercados USDT — quando BTC sobe globalmente, BTC-BRL local demora alguns segundos a se reprecificar. Quem está com ordem maker (ask) leva fill barato.
2. **Inventário direcional**: PMM puro acumula posição quando o mercado é tendencioso, virando uma aposta direcional disfarçada.

### Solução proposta
Estratégia **XEMM (Cross-Exchange Market Making)** com hedge real entre duas exchanges, ambas operando BTC-BRL:
- **Maker**: Bybit BTC-BRL (ordem passiva — vamos colocar tamanhos pequenos no book)
- **Taker (hedge)**: Binance BTC-BRL (quando o maker preenche, dispara taker imediato pra zerar exposição)

E acima dessa mecânica básica de XEMM, adicionar um **sinal de lead-lag sintético** lido só pra observar:

- `fair_BRL = mid(BTC-USDT na Binance) × EMA(mid(USDT-BRL na Binance))`

O sintético **não é negociado** — ele só serve para:
1. **Cancelar** ordens maker antes de fill tóxico quando detecta que o preço internacional moveu mas o BRL local ainda não acompanhou
2. **Ajustar dinamicamente** o `target_profitability` dos próximos executores criados (mais agressivo num lado, mais conservador no outro)

### Outcome esperado
- Reduzir adverse selection mensurável nos logs (PnL pós-fill em janelas de 5–60s)
- Manter exposição direcional próxima de zero (XEMM hedge garante isso)
- PnL líquido = spread maker × volume − fees taker − slippage − fills tóxicos residuais

### Constraints do desenvolvedor
- Solo dev, prioridade total em código simples e testável
- Sem colocation, latência típica home (~100–300ms até Binance/Bybit)
- VIP4 Binance: maker spot ~0.012% (1.2 bps), taker ~0.024% (2.4 bps). Bybit varia
- Sem paper trading (Hummingbot não tem testnet com BRL)
- Sem backtest agora (custo de implementação alto, valor incerto)
- Logs CSV completos serão o "microscópio" para validar a estratégia em produção com tamanho mínimo

---

## 2. Decisões arquiteturais consolidadas (após exploração do fork)

### O que reusamos (não reescrever)
| Componente | Caminho | Por quê |
|---|---|---|
| `XEMMExecutor` | `hummingbot/strategy_v2/executors/xemm_executor/xemm_executor.py` | Já gerencia maker→fill→taker, profitability monitoring, early_stop. Zero alteração necessária. |
| `XEMMExecutorConfig` | `hummingbot/strategy_v2/executors/xemm_executor/data_types.py` | Já tem `min/target/max_profitability`, `maker_side`, `buying_market`, `selling_market`. Suficiente. |
| `StopExecutorAction` | `hummingbot/strategy_v2/models/executor_actions.py` | Cancela maker imediatamente via `early_stop()`. É o nosso "kill switch" de ordem. |
| `MarketDataProvider.get_price_by_type()` | `hummingbot/data_feed/market_data_provider.py` | Retorna `BestBid`, `BestAsk`, `MidPrice` por connector+pair. Síncrono. Suficiente para sinal em janelas de 3–15s. |
| `RateOracle.find_rate()` | `hummingbot/core/rate_oracle/utils.py:24` | Já trata `base == quote` retornando `Decimal("1")`. **BRL-BRL funciona nativamente.** Não precisamos subclassear o executor. |
| `ControllerBase` + `update_processed_data()` | `hummingbot/strategy_v2/controllers/controller_base.py` | Padrão de tick (1.0s default) onde populamos `self.processed_data`. |
| `IsolatedAsyncioWrapperTestCase` | `test/isolated_asyncio_wrapper_test_case.py` | Base para testes async. |

### O que criamos do zero
1. `LeadLagSignalProvider` — módulo Python puro, **zero dependência de Hummingbot**, isoladamente testável.
2. `XEMMLeadLagController` — herda de `ControllerBase`, compõe o `LeadLagSignalProvider`, emite `CreateExecutorAction(XEMMExecutorConfig(...))` e `StopExecutorAction(...)`.
3. `XEMMLeadLagCSVLogger` — escreve uma linha por tick em CSV line-buffered.
4. Testes unitários (signal module — pura lógica, sem mocks).
5. Testes do controller (com mocks no padrão `test_xemm_executor.py`).

### O que **não** vamos fazer nesta fase
- Subclassar `XEMMExecutor` (não é necessário, BRL-BRL funciona).
- Multi-level (>1 ordem por lado). Fica para fase 3, depois de validar em produção.
- Spread assimétrico complexo. Por enquanto o ajuste é só no `target_profitability` por lado.
- Microprice. Mid suficiente; revisitar se logs mostrarem necessidade.
- Volatility-adaptive spread. Postergar.
- Backtest. Postergar.
- Modificar core do Hummingbot.

---

## 3. Estrutura de arquivos

```
hummingbot-ney/
├── hummingbot/
│   └── strategy_v2/
│       └── utils/
│           └── lead_lag_signal.py          [NOVO] módulo isolado, sem deps Hummingbot
├── controllers/
│   └── generic/
│       └── xemm_lead_lag.py                [NOVO] controller V2
├── conf/
│   └── controllers/
│       └── xemm_lead_lag_btc_brl.yml       [NOVO] exemplo de config
├── test/
│   └── hummingbot/
│       └── strategy_v2/
│           ├── utils/
│           │   ├── __init__.py             [NOVO]
│           │   └── test_lead_lag_signal.py [NOVO]
│           └── controllers/
│               └── test_xemm_lead_lag.py   [NOVO]
└── logs/                                   (já existe — destino dos CSVs)
```

**Observação**: nenhum arquivo do core é alterado. O controller e o módulo de sinal são adições puras. Reversível com `rm`.

---

## 4. Fluxo de dados (alto nível)

```
                   ┌──────────────────────────────────────┐
                   │          MarketDataProvider           │
                   │ (Binance: BTC-USDT, USDT-BRL, BTC-BRL)│
                   │ (Bybit:   BTC-BRL)                    │
                   └──────────────┬───────────────────────┘
                                  │ get_price_by_type()
                                  ▼
                   ┌──────────────────────────────────────┐
                   │  XEMMLeadLagController                │
                   │   update_processed_data() (tick 1.0s) │
                   │    └─► LeadLagSignalProvider.update() │
                   │           - CircularPriceBuffer       │
                   │           - EMAFilter (FX leg)        │
                   │           - FeedHealth                │
                   │    └─► CSVLogger.log(row)             │
                   │                                       │
                   │   determine_executor_actions()        │
                   │    1. Risk gates → StopExecutorAction │
                   │    2. Anti-churn cooldown             │
                   │    3. CreateExecutorAction(XEMM)      │
                   └──────────────┬───────────────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────────────┐
                   │  ExecutorOrchestrator                 │
                   │   └─► XEMMExecutor (maker=Bybit,      │
                   │        taker=Binance)                 │
                   └──────────────────────────────────────┘
```

---

## 5. Módulo `hummingbot/strategy_v2/utils/lead_lag_signal.py`

### 5.1 Objetivo
Módulo Python puro, sem `import hummingbot`. Recebe ticks de preços L1 das três pernas e expõe sinais. Permite test unitário isolado e, no futuro, replay de logs sem precisar instanciar o Hummingbot.

### 5.2 Tipos públicos

#### `CircularPriceBuffer`
Buffer FIFO de tuplas `(timestamp: float, price: Decimal)`, com janela máxima em segundos.

```python
class CircularPriceBuffer:
    def __init__(self, max_duration_sec: float = 60.0): ...
    def push(self, timestamp: float, price: Decimal) -> None: ...
    def get_price_at_or_before(self, timestamp: float) -> Optional[Decimal]: ...
    def latest_price(self) -> Optional[Decimal]: ...
    def latest_timestamp(self) -> Optional[float]: ...
    def __len__(self) -> int: ...
```

**Implementação**: `collections.deque[Tuple[float, Decimal]]`. Em cada `push`, evicta entradas com `timestamp < (now - max_duration_sec)`. `get_price_at_or_before` faz lookup linear (buffer pequeno, 60s × ~1Hz = ~60 entradas, custo desprezível).

#### `EMAFilter`
EMA padrão para suavizar a perna FX (USDT-BRL).

```python
class EMAFilter:
    def __init__(self, alpha: Decimal):
        # alpha entre 0 e 1, típico 0.2-0.5
        ...
    def update(self, new_value: Decimal) -> Decimal: ...
    @property
    def value(self) -> Optional[Decimal]: ...
    def reset(self) -> None: ...
```

**Fórmula**: `value = alpha * new + (1 - alpha) * old`. Primeiro update inicializa `value = new_value`.

#### `FeedHealth`
Detecta staleness de feed por timestamp do último update.

```python
class FeedHealth:
    def __init__(self, max_staleness_sec: float):
        ...
    def mark_update(self, timestamp: float) -> None: ...
    def is_stale(self, now: float) -> bool: ...
    def seconds_since_update(self, now: float) -> Optional[float]: ...
```

#### `SignalQuality` (Enum)
```python
class SignalQuality(str, Enum):
    OK = "OK"
    DEGRADED_FX = "DEGRADED_FX"
    DEGRADED_LEADER = "DEGRADED_LEADER"
    DEGRADED_LOCAL = "DEGRADED_LOCAL"
    BAD = "BAD"  # múltiplas pernas stale
```

#### `LeadLagSignalProvider`
Orquestra os componentes acima.

```python
class LeadLagSignalProvider:
    def __init__(
        self,
        lead_windows_sec: List[int] = [5, 10, 15],
        buffer_duration_sec: float = 60.0,
        ema_alpha_fx: Decimal = Decimal("0.3"),
        max_leader_staleness_sec: float = 5.0,
        max_fx_staleness_sec: float = 10.0,
        max_local_staleness_sec: float = 5.0,
    ): ...

    def update(
        self,
        timestamp: float,
        local_bid: Decimal, local_ask: Decimal,
        leader_bid: Decimal, leader_ask: Decimal,
        fx_bid: Decimal, fx_ask: Decimal,
    ) -> None: ...

    # Propriedades derivadas (lidas pelo controller)
    @property
    def local_mid(self) -> Decimal: ...
    @property
    def leader_mid(self) -> Decimal: ...
    @property
    def fx_mid_raw(self) -> Decimal: ...
    @property
    def fx_mid_ema(self) -> Optional[Decimal]: ...
    @property
    def fair_brl(self) -> Decimal: ...
    @property
    def basis_bps(self) -> Decimal: ...
    @property
    def local_spread_bps(self) -> Decimal: ...

    # Sinais por janela
    def lead_signal_bps(self, window_sec: int) -> Optional[Decimal]: ...
    def best_lead_signal_bps(self) -> Optional[Decimal]:
        """Retorna o sinal de maior |valor| entre as janelas configuradas. None se nenhuma válida."""
        ...

    # Health
    @property
    def is_leader_stale(self) -> bool: ...
    @property
    def is_fx_stale(self) -> bool: ...
    @property
    def is_local_stale(self) -> bool: ...
    @property
    def is_any_stale(self) -> bool: ...
    @property
    def signal_quality(self) -> SignalQuality: ...
```

### 5.3 Lógica crítica (pseudocódigo)

#### `update()`
```
1. Se local_bid > 0 e local_ask > 0:
     marca FeedHealth.local; armazena local_bid/ask; computa local_mid
2. Idem para leader e fx
3. Se fx tem update válido nessa chamada:
     fx_mid_ema = ema_filter.update(fx_mid_raw)
4. Se leader e fx têm update válido:
     fair_brl = leader_mid * fx_mid_ema
5. Se local_mid válido:
     buffer_local.push(timestamp, local_mid)
6. Se fair_brl válido:
     buffer_fair.push(timestamp, fair_brl)
7. self._last_update_time = timestamp  (sempre atualiza, mesmo se nada válido — pra cálculo de staleness)
```

**Decisão importante**: `update()` é chamado a cada tick mesmo se algumas pernas estiverem stale. Cada feed tem seu próprio `FeedHealth` independente; `mark_update` só é chamado pra perna que recebeu valor válido.

#### `basis_bps`
```
if fair_brl <= 0 or local_mid <= 0:
    return Decimal("0")  # graceful degradation; controller usa is_any_stale pra decidir
return Decimal("10000") * (local_mid / fair_brl - Decimal("1"))
```

#### `lead_signal_bps(window_sec)`
```
if is_any_stale:
    return None
now = self._last_update_time
past = now - window_sec
fair_past  = buffer_fair.get_price_at_or_before(past)
local_past = buffer_local.get_price_at_or_before(past)
if any of (fair_past, local_past, fair_brl, local_mid) is None or <= 0:
    return None
leader_ret = ln(fair_brl / fair_past)       # via Decimal(str(math.log(float(...))))
local_ret  = ln(local_mid / local_past)
return Decimal("10000") * (leader_ret - local_ret)
```

**Nota Decimal**: o uso de `math.log(float(...))` introduz pequena imprecisão, mas é aceitável: o sinal é em bps e a margem de threshold é >= 5bps. Alternativa pura-Decimal seria `(a/b - 1)` (aproximação de Taylor para retornos pequenos), aceitável também. **Decisão**: usar `math.log` pra clareza, validar precisão nos testes (sinal em escala bps tolera erro 1e-9).

#### `best_lead_signal_bps()`
```
sigs = [lead_signal_bps(w) for w in self._lead_windows]
valid = [s for s in sigs if s is not None]
if not valid:
    return None
return max(valid, key=abs)
```

#### `signal_quality`
```
leader_stale = is_leader_stale
fx_stale = is_fx_stale
local_stale = is_local_stale
if (leader_stale and fx_stale) or (leader_stale and local_stale) or (fx_stale and local_stale):
    return BAD
if leader_stale: return DEGRADED_LEADER
if fx_stale:     return DEGRADED_FX
if local_stale:  return DEGRADED_LOCAL
return OK
```

### 5.4 Considerações de Decimal e precisão
- Todos os valores externos entram como `Decimal`.
- Multiplicação/divisão de `Decimal` mantém precisão.
- `math.log` exige `float` — fazer conversão `float(decimal_value)` apenas quando estritamente necessário, e o resultado volta para `Decimal` via `Decimal(str(...))`.
- `Decimal("0.3") * Decimal("100.50")` → `Decimal("30.150")`. Sem erros de ponto flutuante.

### 5.5 O que **não** está nesse módulo (e por quê)
- Nenhuma referência a connector, market_data_provider, executor. **Pura aritmética em cima de inputs Decimal/float.**
- Nenhum logging — quem loga é o controller (que sabe o caminho do CSV).
- Nenhum threading/asyncio — totalmente síncrono. Chamado de dentro do `update_processed_data` async, mas a função em si é síncrona.

---

## 6. Controller `controllers/generic/xemm_lead_lag.py`

### 6.1 Config: `XEMMLeadLagConfig`
Herda de `ControllerConfigBase` (Pydantic).

```python
class XEMMLeadLagConfig(ControllerConfigBase):
    controller_name: str = "xemm_lead_lag"

    # === Mercado ===
    maker_connector: str            # ex: "bybit"
    maker_trading_pair: str         # ex: "BTC-BRL"
    taker_connector: str            # ex: "binance"
    taker_trading_pair: str         # ex: "BTC-BRL"
    signal_connector: str           # ex: "binance" (mesma do taker, mas independente)
    signal_base_pair: str           # ex: "BTC-USDT"
    signal_fx_pair: str             # ex: "USDT-BRL"

    # === Sizing ===
    order_amount: Decimal           # em base asset (ex: Decimal("0.0002") BTC)

    # === Profitability (passa direto pro XEMMExecutor) ===
    min_profitability: Decimal      # floor; deve cobrir 2*maker_fee + taker_fee + buffer
    target_profitability: Decimal
    max_profitability: Decimal

    # === Sinal lead-lag ===
    lead_windows_seconds: List[int] = [5, 10, 15]
    fast_cancel_threshold_bps: Decimal = Decimal("20")  # |lead| acima → cancela
    profitability_adjust_threshold_bps: Decimal = Decimal("5")  # |lead| acima → ajusta target
    w_lead: Decimal = Decimal("0.5")
    ema_alpha_fx: Decimal = Decimal("0.3")

    # === Risk gates ===
    max_leader_staleness_sec: float = 5.0
    max_fx_staleness_sec: float = 10.0
    max_local_staleness_sec: float = 5.0
    basis_hard_threshold_bps: Decimal = Decimal("150")
    max_local_spread_bps: Decimal = Decimal("200")

    # === Anti-churn ===
    min_requote_interval_sec: float = 3.0  # cooldown depois de cancel ou fim de executor

    # === Inventário (ainda simplificado) ===
    inventory_target_pct: Decimal = Decimal("0.5")
    inventory_skew_strength: Decimal = Decimal("0.001")  # delta no target_profitability por unidade de skew

    # === Operacional ===
    shadow_mode: bool = False
    log_dir: str = "logs/xemm_lead_lag"
    kill_switch_file: Optional[str] = None  # se existir, pausa criação de ordens

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for conn, pair in [
            (self.maker_connector, self.maker_trading_pair),
            (self.taker_connector, self.taker_trading_pair),
            (self.signal_connector, self.signal_base_pair),
            (self.signal_connector, self.signal_fx_pair),
        ]:
            if conn not in markets:
                markets[conn] = set()
            markets[conn].add(pair)
        return markets
```

**Nota sobre `total_amount_quote`**: a `ControllerConfigBase` já tem esse campo. Para nossa estratégia, `order_amount` (em base) é independente; `total_amount_quote` pode ser usado como bound de risco máximo.

### 6.2 Classe `XEMMLeadLagController`

```python
class XEMMLeadLagController(ControllerBase):
    def __init__(self, config: XEMMLeadLagConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)
        self._signal = LeadLagSignalProvider(
            lead_windows_sec=config.lead_windows_seconds,
            ema_alpha_fx=config.ema_alpha_fx,
            max_leader_staleness_sec=config.max_leader_staleness_sec,
            max_fx_staleness_sec=config.max_fx_staleness_sec,
            max_local_staleness_sec=config.max_local_staleness_sec,
        )
        self._csv = XEMMLeadLagCSVLogger(config.log_dir, controller_id=config.id)
        self._last_action_time: float = 0.0
        self._last_cancel_reason: Optional[str] = None

    async def update_processed_data(self): ...
    def determine_executor_actions(self) -> List[ExecutorAction]: ...
    def to_format_status(self) -> List[str]: ...
```

### 6.3 `update_processed_data()` (pseudocódigo detalhado)

```
now = self.market_data_provider.time()

# 1. Lê L1 das três pernas. Se algum get_price_by_type lançar (raro), captura e marca como 0.
def safe_price(connector, pair, ptype):
    try:
        return self.market_data_provider.get_price_by_type(connector, pair, ptype)
    except Exception as e:
        self.logger().warning(f"price fetch fail {connector}/{pair}/{ptype}: {e}")
        return Decimal("0")

local_bid  = safe_price(maker_connector, maker_trading_pair, BestBid)
local_ask  = safe_price(maker_connector, maker_trading_pair, BestAsk)
leader_bid = safe_price(signal_connector, signal_base_pair, BestBid)
leader_ask = safe_price(signal_connector, signal_base_pair, BestAsk)
fx_bid     = safe_price(signal_connector, signal_fx_pair, BestBid)
fx_ask     = safe_price(signal_connector, signal_fx_pair, BestAsk)

# 2. Atualiza o signal provider
self._signal.update(now, local_bid, local_ask, leader_bid, leader_ask, fx_bid, fx_ask)

# 3. Calcula inventário (usado pra skew)
base, quote = self.config.maker_trading_pair.split("-")
inv_base  = self.market_data_provider.get_balance(self.config.maker_connector, base)
inv_quote = self.market_data_provider.get_balance(self.config.maker_connector, quote)
local_mid = self._signal.local_mid
total_in_quote = inv_base * local_mid + inv_quote
inv_pct = (inv_base * local_mid / total_in_quote) if total_in_quote > 0 else Decimal("0.5")
inv_skew = inv_pct - self.config.inventory_target_pct  # >0 = excesso de base

# 4. Verifica gates de risco
should_cancel, cancel_reason = self._check_risk_gates()

# 5. Calcula target profitability ajustado por lado
lead_bps = self._signal.best_lead_signal_bps()
target_buy, target_sell = self._adjust_profitability(lead_bps, inv_skew)

# 6. Popula processed_data (consumido por determine_executor_actions e logger)
self.processed_data = {
    "timestamp": now,
    "local_bid": local_bid, "local_ask": local_ask, "local_mid": local_mid,
    "leader_bid": leader_bid, "leader_ask": leader_ask,
    "leader_mid": self._signal.leader_mid,
    "fx_bid": fx_bid, "fx_ask": fx_ask,
    "fx_mid_raw": self._signal.fx_mid_raw,
    "fx_mid_ema": self._signal.fx_mid_ema or Decimal("0"),
    "fair_brl": self._signal.fair_brl,
    "basis_bps": self._signal.basis_bps,
    "lead_bps_per_window": {w: self._signal.lead_signal_bps(w) for w in self.config.lead_windows_seconds},
    "best_lead_bps": lead_bps,
    "signal_quality": self._signal.signal_quality.value,
    "should_cancel": should_cancel,
    "cancel_reason": cancel_reason,
    "inventory_base": inv_base,
    "inventory_quote": inv_quote,
    "inventory_pct": inv_pct,
    "inventory_skew": inv_skew,
    "target_prof_buy": target_buy,
    "target_prof_sell": target_sell,
    "kill_switch_active": self._is_kill_switch_active(),
}

# 7. Loga
self._csv.log(self.processed_data)
```

### 6.4 `_check_risk_gates()` (ordem importa — primeiro match vence)

```
1. if kill_switch_active:        return True, "KILL_SWITCH"
2. if signal.is_any_stale:       return True, "FEED_STALE_" + signal.signal_quality
3. if abs(basis_bps) > basis_hard_threshold_bps:
                                 return True, "BASIS_EXTREME"
4. if local_spread_bps > max_local_spread_bps:
                                 return True, "LOCAL_SPREAD_WIDE"
5. lead = best_lead_signal_bps
   if lead is not None and abs(lead) > fast_cancel_threshold_bps:
                                 return True, "LEAD_SIGNAL_STRONG"
6. return False, None
```

**Kill switch**: se `config.kill_switch_file` está setado E o arquivo existe no disco → ativo. Permite o operador parar o bot remotamente sem reiniciar (`touch /tmp/xemm_pause`).

### 6.5 `_adjust_profitability(lead_bps, inv_skew)`

```
target_buy  = config.target_profitability
target_sell = config.target_profitability

# Ajuste por sinal lead-lag
if lead_bps is not None and abs(lead_bps) > config.profitability_adjust_threshold_bps:
    lead_adj = config.w_lead * lead_bps / Decimal("10000")
    # lead_bps > 0: fair_brl subiu mais que local → mercado vai subir →
    #               favorece BUY (target_buy menor = mais agressivo no bid)
    #               protege SELL (target_sell maior = ask mais alto, mais conservador)
    target_buy  -= lead_adj
    target_sell += lead_adj

# Ajuste por inventário
# inv_skew > 0 (excesso de base): protege BUY, agressivo SELL
inv_adj = inv_skew * config.inventory_skew_strength
target_buy  += inv_adj
target_sell -= inv_adj

# Clamp
target_buy  = max(config.min_profitability, min(config.max_profitability, target_buy))
target_sell = max(config.min_profitability, min(config.max_profitability, target_sell))
return target_buy, target_sell
```

### 6.6 `determine_executor_actions()` (3 fases)

```
actions = []
now = self.market_data_provider.time()

# === Fase 1: cancelamento defensivo ===
if processed_data["should_cancel"]:
    for ex in self.executors_info:
        if not ex.is_done:
            actions.append(StopExecutorAction(
                controller_id=self.config.id,
                executor_id=ex.config.id,
                keep_position=False,
            ))
    if actions:
        self._last_action_time = now
        self._last_cancel_reason = processed_data["cancel_reason"]
        self.logger().info(f"Cancelling {len(actions)} executors: {self._last_cancel_reason}")
    return actions

# === Shadow mode: log e sai ===
if self.config.shadow_mode:
    return []

# === Fase 2: anti-churn cooldown ===
if (now - self._last_action_time) < self.config.min_requote_interval_sec:
    return []

# === Fase 3: criação ===
active_buy  = self.filter_executors(self.executors_info,
                lambda e: not e.is_done and e.config.maker_side == TradeType.BUY)
active_sell = self.filter_executors(self.executors_info,
                lambda e: not e.is_done and e.config.maker_side == TradeType.SELL)

target_buy  = processed_data["target_prof_buy"]
target_sell = processed_data["target_prof_sell"]

if len(active_buy) == 0:
    actions.append(CreateExecutorAction(
        controller_id=self.config.id,
        executor_config=XEMMExecutorConfig(
            controller_id=self.config.id,
            timestamp=now,
            buying_market=ConnectorPair(self.config.maker_connector, self.config.maker_trading_pair),
            selling_market=ConnectorPair(self.config.taker_connector, self.config.taker_trading_pair),
            maker_side=TradeType.BUY,
            order_amount=self.config.order_amount,
            min_profitability=self.config.min_profitability,
            target_profitability=target_buy,
            max_profitability=self.config.max_profitability,
        )
    ))

if len(active_sell) == 0:
    actions.append(CreateExecutorAction(
        controller_id=self.config.id,
        executor_config=XEMMExecutorConfig(
            controller_id=self.config.id,
            timestamp=now,
            buying_market=ConnectorPair(self.config.taker_connector, self.config.taker_trading_pair),
            selling_market=ConnectorPair(self.config.maker_connector, self.config.maker_trading_pair),
            maker_side=TradeType.SELL,
            order_amount=self.config.order_amount,
            min_profitability=self.config.min_profitability,
            target_profitability=target_sell,
            max_profitability=self.config.max_profitability,
        )
    ))

if actions:
    self._last_action_time = now

return actions
```

### 6.7 CSV Logger

```python
class XEMMLeadLagCSVLogger:
    COLUMNS = [
        "timestamp",
        "local_bid", "local_ask", "local_mid",
        "leader_bid", "leader_ask", "leader_mid",
        "fx_bid", "fx_ask", "fx_mid_raw", "fx_mid_ema",
        "fair_brl", "basis_bps",
        "lead_5s", "lead_10s", "lead_15s", "best_lead_bps",
        "signal_quality",
        "should_cancel", "cancel_reason",
        "inventory_base", "inventory_quote", "inventory_pct", "inventory_skew",
        "target_prof_buy", "target_prof_sell",
        "kill_switch_active",
    ]

    def __init__(self, log_dir: str, controller_id: str):
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self._path = os.path.join(log_dir, f"xemm_lead_lag_{controller_id}_{ts}.csv")
        self._file = open(self._path, "w", newline="", buffering=1)  # line-buffered
        self._writer = csv.DictWriter(self._file, fieldnames=self.COLUMNS, extrasaction="ignore")
        self._writer.writeheader()

    def log(self, processed_data: dict) -> None:
        row = {col: self._format(processed_data.get(col)) for col in self.COLUMNS}
        # Lead per window (achata o dict)
        lead_dict = processed_data.get("lead_bps_per_window", {})
        for w in (5, 10, 15):
            row[f"lead_{w}s"] = self._format(lead_dict.get(w))
        self._writer.writerow(row)

    @staticmethod
    def _format(v):
        if v is None:
            return ""
        if isinstance(v, Decimal):
            return str(v)
        return v

    def close(self):
        try:
            self._file.flush()
            self._file.close()
        except Exception:
            pass
```

**Buffer de 1 linha (`buffering=1`)**: garante que cada linha vai pro disco sem precisar `flush()` manual a cada chamada — crucial pra não perder dados em crash.

### 6.8 `to_format_status()` (mostrar no `status` do bot)

```
Linhas formatadas:
  - "Signal quality: OK | basis: -3.2bps | lead_5s: 8.1bps lead_10s: 12.4bps lead_15s: 15.2bps"
  - "Inventory: 0.0042 BTC (52% of total) | skew: +0.02"
  - "Target profitability: BUY 0.18% | SELL 0.22%"
  - "Last cancel: LEAD_SIGNAL_STRONG (12s ago)"
  - "Active executors: 2 | shadow_mode: false"
```

### 6.9 Pontos sutis

1. **`update_processed_data` precisa ser robusta a `get_price_by_type` retornando `0` ou lançando** — connector ainda inicializando. Daí o `safe_price` wrapper.
2. **Re-criação de executor não precisa esperar `min_requote_interval_sec` quando o anterior fechou naturalmente** — só esperar no caso de cancelamento defensivo. Mas pra simplificar v1, esperar sempre é aceitável (a cooldown de 3s não é crítica).
3. **`StopExecutorAction.keep_position=False`**: garante que `early_stop` cancela o maker. Como ainda não houve fill (estamos cancelando preventivamente), não há posição aberta no taker — `keep_position` é irrelevante nesse caminho.
4. **Race condition fill-vs-cancel**: se o fill chega entre `should_cancel=True` e o `cancel()` chegar à exchange, o `XEMMExecutor` vai disparar o taker hedge normalmente. Esse é exatamente o comportamento desejado — o XEMMExecutor sempre fecha posição. Nosso lead-lag é apenas redutor de probabilidade desses fills.
5. **Fechamento limpo do CSV**: sobrescrever `on_stop()` (ou método análogo do `ControllerBase`) para fazer `self._csv.close()`. Como já usamos `buffering=1`, perda máxima é a linha em curso.

---

## 7. Testes unitários — `test/hummingbot/strategy_v2/utils/test_lead_lag_signal.py`

### 7.1 Estrutura geral

```python
import math
import unittest
from decimal import Decimal

from hummingbot.strategy_v2.utils.lead_lag_signal import (
    CircularPriceBuffer,
    EMAFilter,
    FeedHealth,
    SignalQuality,
    LeadLagSignalProvider,
)


class TestCircularPriceBuffer(unittest.TestCase): ...
class TestEMAFilter(unittest.TestCase): ...
class TestFeedHealth(unittest.TestCase): ...
class TestLeadLagSignalProvider(unittest.TestCase): ...
```

**Sem `IsolatedAsyncioWrapperTestCase`** — o módulo é 100% síncrono, basta `unittest.TestCase`.

### 7.2 `TestCircularPriceBuffer` — 9 testes

| # | Nome | Cenário e asserção |
|---|---|---|
| 1 | `test_eviction_removes_old_entries` | Buffer com `max_duration=10`. Push em t=0 (price=A), t=5 (B), t=20 (C). Após push t=20, A e B foram evictados (idade 20 e 15 > 10). `len == 1`, único restante = (20, C). |
| 2 | `test_get_price_at_or_before_exact_timestamp` | Push (10, 100), (20, 200). `get_price_at_or_before(10) == 100`. `get_price_at_or_before(20) == 200`. |
| 3 | `test_get_price_at_or_before_between_timestamps` | Push (10, 100), (20, 200). `get_price_at_or_before(15) == 100` (mais recente ≤ 15). |
| 4 | `test_get_price_at_or_before_empty_buffer` | Buffer vazio. `get_price_at_or_before(any) is None`. |
| 5 | `test_get_price_at_or_before_all_entries_after_t` | Push (20, 200). `get_price_at_or_before(10) is None`. |
| 6 | `test_latest_price_empty_buffer` | `latest_price() is None`. |
| 7 | `test_latest_price_returns_most_recent` | Push (5, 100), (10, 200). `latest_price() == 200`. `latest_timestamp() == 10`. |
| 8 | `test_eviction_exactly_at_boundary` | `max_duration=10`. Push (0, A), (10, B). Item em t=0 tem idade exata = 10s. **Decisão de spec**: idade `>` `max_duration` evicta (estrito). Então (0, A) **permanece**. `len == 2`. |
| 9 | `test_buffer_pushes_decimal_not_float` | Push com `Decimal("100.50")`. `latest_price() == Decimal("100.50")`. Não converte a float. |

### 7.3 `TestEMAFilter` — 6 testes

| # | Nome | Cenário e asserção |
|---|---|---|
| 1 | `test_first_value_equals_input` | `EMAFilter(0.5)`. `update(Decimal("100"))` → retorno `Decimal("100")`. `value == Decimal("100")`. |
| 2 | `test_value_none_before_first_update` | Novo filter. `value is None`. |
| 3 | `test_second_value_blends` | `EMAFilter(0.5)`. `update(100)`, `update(200)`. Esperado: `0.5*200 + 0.5*100 = 150`. Asserção exata: `value == Decimal("150")`. |
| 4 | `test_alpha_one_equals_last_value` | `alpha=1`. `update(100)`, `update(200)`, `update(50)`. `value == Decimal("50")`. |
| 5 | `test_alpha_zero_never_changes` | `alpha=0`. `update(100)`, `update(200)`, `update(50)`. `value == Decimal("100")` (nunca muda após primeiro). |
| 6 | `test_convergence_to_constant` | `alpha=0.5`. Loop 50× `update(100)`. `abs(value - 100) < Decimal("1E-10")`. |
| 7 | `test_reset_clears_value` | Após updates, `reset()`. `value is None`. Próximo `update(50)` retorna `50`. |

### 7.4 `TestFeedHealth` — 6 testes

| # | Nome | Cenário e asserção |
|---|---|---|
| 1 | `test_initially_stale` | `FeedHealth(max_staleness_sec=5.0)`. `is_stale(now=1000)` é `True` (nunca atualizou). |
| 2 | `test_not_stale_after_update` | `mark_update(1000)`. `is_stale(1000)` é `False`. `is_stale(1004.9)` é `False`. |
| 3 | `test_becomes_stale_after_max_staleness` | `mark_update(1000)`. `is_stale(1005.1)` é `True`. |
| 4 | `test_exactly_at_boundary` | `mark_update(1000)`. `is_stale(1005.0)` é `False` (estritamente `>` evicta; igual não). Documentar essa convenção. |
| 5 | `test_seconds_since_update_none_before_update` | `seconds_since_update(1000) is None`. |
| 6 | `test_seconds_since_update_after_update` | `mark_update(1000)`. `seconds_since_update(1003) == 3.0`. |

### 7.5 `TestLeadLagSignalProvider` — 18 testes

**Helper de setup** (chamado em cada teste):
```python
def _make_provider(self, **overrides) -> LeadLagSignalProvider:
    defaults = dict(
        lead_windows_sec=[5, 10],
        buffer_duration_sec=60.0,
        ema_alpha_fx=Decimal("0.5"),
        max_leader_staleness_sec=5.0,
        max_fx_staleness_sec=5.0,
        max_local_staleness_sec=5.0,
    )
    defaults.update(overrides)
    return LeadLagSignalProvider(**defaults)

def _push(self, p, t, local=(300_000, 300_100), leader=(50_000, 50_100), fx=(5.00, 5.02)):
    """Helper para chamar update() com defaults razoáveis."""
    p.update(t,
        Decimal(str(local[0])), Decimal(str(local[1])),
        Decimal(str(leader[0])), Decimal(str(leader[1])),
        Decimal(str(fx[0])), Decimal(str(fx[1])))
```

| # | Nome | Cenário |
|---|---|---|
| 1 | `test_local_mid_calculation` | `update(t, local_bid=99.5, local_ask=100.5, ...)`. `local_mid == Decimal("100.0")`. |
| 2 | `test_fair_brl_basic_calculation` | `update(1000, ..., leader_bid=50000, leader_ask=50100, fx_bid=5.00, fx_ask=5.02)`. `leader_mid=50050`, `fx_mid_raw=5.01`, `fx_mid_ema=5.01` (1ª chamada), `fair_brl = 50050 * 5.01 = 250750.5`. |
| 3 | `test_fair_brl_uses_ema_for_fx_not_raw` | Update 1: fx=(5,5). Update 2: fx=(7,7). `alpha=0.5`. fx_ema esperado = 6. fair_brl deve usar 6, não 7. Verifica `provider.fx_mid_ema == Decimal("6")` e `fair_brl == leader_mid * 6`. |
| 4 | `test_basis_bps_positive_when_local_above_fair` | Configura para `local_mid=300000`, `fair_brl=200000`. `basis_bps == 5000`. |
| 5 | `test_basis_bps_negative_when_local_below_fair` | `local_mid=100000`, `fair_brl=200000`. `basis_bps == -5000`. |
| 6 | `test_basis_bps_zero_when_equal` | `local_mid == fair_brl`. `abs(basis_bps) < Decimal("0.001")`. |
| 7 | `test_basis_bps_zero_when_fair_zero` | Sem dados de leader/fx. `fair_brl == 0`. `basis_bps == 0` (graceful degradation, sem ZeroDivisionError). |
| 8 | `test_lead_signal_leader_moves_first` | Sequência: t=0 fair=100k local=100k; t=1..5 fair=101k local=100k. Em t=5, `lead_signal_bps(5)` ≈ +99.5 bps. **Asserção**: `lead_signal_bps(5) > Decimal("50")`. |
| 9 | `test_lead_signal_local_moves_first` | Inverso: local sobe 1%, fair fica. `lead_signal_bps(5) < Decimal("-50")`. |
| 10 | `test_lead_signal_no_movement_both_flat` | Ambos constantes. `abs(lead_signal_bps(5)) < Decimal("0.1")`. |
| 11 | `test_lead_signal_insufficient_history_returns_none` | Apenas 2s de buffer, requisita window=10. `lead_signal_bps(10) is None`. |
| 12 | `test_lead_signal_stale_leader_returns_none` | Update normal em t=1000. Após simular avanço para t=1010 sem update, chama `lead_signal_bps(5)` (com `now=1010` interno). Como `is_leader_stale=True`, retorna `None`. **Implementação do teste**: criar provider, chamar update normal, mas modificar `_last_update_time` ou usar `mark_update` direto pra forçar staleness. Mais limpo: chamar `update()` em t=1010 com `leader_bid=0, leader_ask=0` (não atualiza FeedHealth do leader). |
| 13 | `test_lead_signal_stale_fx_returns_none` | Análogo ao 12, FX stale. |
| 14 | `test_signal_quality_ok` | Todas pernas atualizadas em t=1000. Em t=1003, `signal_quality == OK`. |
| 15 | `test_signal_quality_degraded_fx_when_only_fx_stale` | Update normal mas com `fx_bid=0, fx_ask=0` (não marca FX). Após threshold, `signal_quality == DEGRADED_FX`. |
| 16 | `test_signal_quality_degraded_leader_when_only_leader_stale` | Análogo: leader=0. `signal_quality == DEGRADED_LEADER`. |
| 17 | `test_signal_quality_bad_when_two_pernas_stale` | leader=0 E fx=0. Após threshold, `signal_quality == BAD`. |
| 18 | `test_best_lead_signal_returns_max_abs_value` | Cenário onde `lead_signal_bps(5) = +30`, `lead_signal_bps(10) = -50`. `best_lead_signal_bps() == -50` (maior |valor|). |
| 19 | `test_best_lead_signal_returns_none_when_all_none` | Pouca história em todas as janelas. `best_lead_signal_bps() is None`. |

### 7.6 Notas de implementação dos testes

1. **Sem mocks** — todos os testes operam sobre objetos reais com inputs Decimal. Isso facilita debug e isola bugs aritméticos.
2. **Tolerância de Decimal**: para testes envolvendo `math.log`, usar `assertAlmostEqual` com `places=6` ou comparar `abs(a - b) < Decimal("1E-6")`.
3. **Cobertura mínima esperada**: 95% das linhas em `lead_lag_signal.py`. Se ficar abaixo, há código morto ou ramo não testado.
4. **Tempo de execução**: suite inteira deve rodar em < 1s (puro CPU, sem I/O).

---

## 8. Testes do controller — `test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py`

### 8.1 Estrutura geral

```python
import asyncio
from decimal import Decimal
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    StopExecutorAction,
)
from hummingbot.strategy_v2.utils.lead_lag_signal import SignalQuality

from controllers.generic.xemm_lead_lag import (
    XEMMLeadLagConfig,
    XEMMLeadLagController,
)


class TestXEMMLeadLagController(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self): ...
```

### 8.2 Setup compartilhado

```python
def setUp(self):
    super().setUp()
    self.config = XEMMLeadLagConfig(
        id="test-controller-1",
        controller_name="xemm_lead_lag",
        maker_connector="bybit",
        maker_trading_pair="BTC-BRL",
        taker_connector="binance",
        taker_trading_pair="BTC-BRL",
        signal_connector="binance",
        signal_base_pair="BTC-USDT",
        signal_fx_pair="USDT-BRL",
        order_amount=Decimal("0.001"),
        min_profitability=Decimal("0.0007"),
        target_profitability=Decimal("0.0020"),
        max_profitability=Decimal("0.0080"),
        lead_windows_seconds=[5, 10, 15],
        fast_cancel_threshold_bps=Decimal("20"),
        profitability_adjust_threshold_bps=Decimal("5"),
        w_lead=Decimal("0.5"),
        ema_alpha_fx=Decimal("0.3"),
        max_leader_staleness_sec=5.0,
        max_fx_staleness_sec=5.0,
        max_local_staleness_sec=5.0,
        basis_hard_threshold_bps=Decimal("150"),
        max_local_spread_bps=Decimal("200"),
        min_requote_interval_sec=3.0,
        inventory_target_pct=Decimal("0.5"),
        inventory_skew_strength=Decimal("0.001"),
        shadow_mode=False,
        log_dir="/tmp/test_xemm_lead_lag",
        kill_switch_file=None,
    )
    self.market_data_provider = MagicMock(spec=MarketDataProvider)
    self.market_data_provider.time = MagicMock(return_value=1700000000.0)
    self.actions_queue = AsyncMock(spec=asyncio.Queue)

    # Patcha o CSV para não fazer I/O nos testes
    with patch.object(XEMMLeadLagController, '_initialize_csv_logger'):
        self.controller = XEMMLeadLagController(
            config=self.config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
        )
    self.controller._csv = MagicMock()  # mock do logger
    self.set_loggers([self.controller.logger()])

    self._setup_default_market_data()

def _setup_default_market_data(
    self,
    local_bid=Decimal("300000"), local_ask=Decimal("300100"),
    leader_bid=Decimal("50000"), leader_ask=Decimal("50100"),
    fx_bid=Decimal("5.00"),       fx_ask=Decimal("5.02"),
    inv_base=Decimal("0.01"),     inv_quote=Decimal("3000"),
):
    """Configura get_price_by_type e get_balance com defaults."""
    def price_side_effect(connector, pair, ptype):
        table = {
            ("bybit",   "BTC-BRL",   PriceType.BestBid): local_bid,
            ("bybit",   "BTC-BRL",   PriceType.BestAsk): local_ask,
            ("binance", "BTC-USDT",  PriceType.BestBid): leader_bid,
            ("binance", "BTC-USDT",  PriceType.BestAsk): leader_ask,
            ("binance", "USDT-BRL",  PriceType.BestBid): fx_bid,
            ("binance", "USDT-BRL",  PriceType.BestAsk): fx_ask,
        }
        return table[(connector, pair, ptype)]
    self.market_data_provider.get_price_by_type.side_effect = price_side_effect

    def balance_side_effect(connector, asset):
        return {"BTC": inv_base, "BRL": inv_quote}.get(asset, Decimal("0"))
    self.market_data_provider.get_balance.side_effect = balance_side_effect

def _make_executor_info(self, executor_id, maker_side, is_done=False):
    """Cria mock de ExecutorInfo (item de self.executors_info)."""
    info = MagicMock()
    info.id = executor_id
    info.is_done = is_done
    info.config = MagicMock(spec=XEMMExecutorConfig)
    info.config.id = executor_id
    info.config.maker_side = maker_side
    return info
```

**Notas do setup**:
- `MagicMock(spec=MarketDataProvider)` garante que apenas métodos reais são acessíveis (qualquer typo vira erro).
- `AsyncMock(spec=asyncio.Queue)` para o `actions_queue` (necessário porque `send_actions` faz `await queue.put(...)`).
- `_make_executor_info` espelha o que `self.executors_info` retorna (lista de `ExecutorInfo`-like).

### 8.3 Lista completa de testes (29 testes)

#### Grupo A — Configuração e markets (4 testes)

| # | Nome | Asserção |
|---|---|---|
| A1 | `test_update_markets_registers_maker_pair` | `config.update_markets({})` retorna dict com `"bybit"` → `{"BTC-BRL"}`. |
| A2 | `test_update_markets_registers_taker_pair` | `"binance"` contém `"BTC-BRL"`. |
| A3 | `test_update_markets_registers_signal_base_pair` | `"binance"` contém `"BTC-USDT"`. |
| A4 | `test_update_markets_registers_signal_fx_pair` | `"binance"` contém `"USDT-BRL"`. (Mesma key, conjunto cresce.) |

#### Grupo B — `update_processed_data` (5 testes)

| # | Nome | Asserção |
|---|---|---|
| B1 | `test_processed_data_populated_after_update` | Após `await controller.update_processed_data()`, `processed_data` contém todas as 25 chaves esperadas. |
| B2 | `test_processed_data_signal_quality_ok_with_healthy_feeds` | Defaults saudáveis. `processed_data["signal_quality"] == "OK"`. |
| B3 | `test_csv_logger_called_once_per_update` | `controller._csv.log` chamado 1× após 1 update. |
| B4 | `test_csv_logger_called_three_times_on_three_updates` | 3× update → `log` chamado 3×. |
| B5 | `test_safe_price_handles_exception` | Configura `get_price_by_type` para lançar para uma das pernas. Update não levanta exceção; preço dela vira `Decimal("0")`. |

#### Grupo C — Risk gates / cancelamento (8 testes)

| # | Nome | Asserção |
|---|---|---|
| C1 | `test_no_cancel_when_all_signals_healthy` | Defaults. Sem executores ativos. `determine_executor_actions` retorna **só** `CreateExecutorAction`s, nenhum `StopExecutorAction`. |
| C2 | `test_cancel_on_stale_feed_stops_active_executors` | Configura `_setup_default_market_data` com `leader_bid=Decimal("0"), leader_ask=Decimal("0")` E avança `time` para forçar staleness. Adiciona 2 executores ativos a `executors_info`. Após `update_processed_data` + `determine_executor_actions`, retorna 2 `StopExecutorAction`. |
| C3 | `test_cancel_on_basis_extreme` | Setup com `local_bid=400_000, local_ask=400_100` (muito acima do fair), forçando `basis_bps > 150`. 1 executor ativo → 1 `StopExecutorAction`. |
| C4 | `test_cancel_on_strong_lead_signal` | Patch direto no `controller._signal.best_lead_signal_bps` para retornar `Decimal("30")` (> threshold 20). 1 executor ativo → 1 `StopExecutorAction`. **Alternativa**: simular sequência temporal real injetando 6 updates com fair subindo enquanto local fica. Mais limpo no teste é o patch. |
| C5 | `test_cancel_on_wide_local_spread` | `local_bid=300_000, local_ask=310_000` (spread = ~330 bps > 200). 1 executor ativo → 1 `StopExecutorAction`. |
| C6 | `test_cancel_on_kill_switch_file_present` | `config.kill_switch_file = "/tmp/test_kill_switch"`. `os.path.exists` mock retorna True. 1 executor ativo → 1 `StopExecutorAction` com reason `KILL_SWITCH`. |
| C7 | `test_cancel_does_not_create_new_executors_same_tick` | Risk gate ativo + 1 executor ativo. `determine_executor_actions` retorna **somente** `StopExecutorAction`, nenhum `CreateExecutorAction` na mesma tick. |
| C8 | `test_cancel_reason_logged_in_processed_data` | C2 cenário. `processed_data["cancel_reason"]` contém `"FEED_STALE"` (substring). |

#### Grupo D — Anti-churn (3 testes)

| # | Nome | Asserção |
|---|---|---|
| D1 | `test_no_create_within_cooldown_after_cancel` | Setup: força cancel em t=1000 (`controller._last_action_time=1000`). Em t=1001 com sinais saudáveis, `determine_executor_actions` retorna `[]` (cooldown 3s). |
| D2 | `test_create_after_cooldown_expires` | `_last_action_time=1000`. Em t=1004 (>3s após), retorna `CreateExecutorAction`s. |
| D3 | `test_cooldown_does_not_block_cancel` | `_last_action_time=999.9`. Em t=1000 com gate ativo, ainda emite `StopExecutorAction` (cooldown bloqueia só criação, não cancelamento). |

#### Grupo E — Criação de executores (5 testes)

| # | Nome | Asserção |
|---|---|---|
| E1 | `test_create_buy_when_none_active` | `executors_info=[]`. Sinais saudáveis. Retorna ≥1 `CreateExecutorAction` com `maker_side==BUY`. Verifica `executor_config.buying_market.connector_name == "bybit"` e `selling_market.connector_name == "binance"`. |
| E2 | `test_create_sell_when_none_active` | Análogo: `maker_side==SELL`, `buying_market.connector=="binance"`, `selling_market.connector=="bybit"`. |
| E3 | `test_no_duplicate_buy_when_one_active` | `executors_info=[buy_active]`. Retorna 0 ou 1 `CreateExecutorAction(SELL)` mas nunca outro BUY. |
| E4 | `test_no_duplicate_sell_when_one_active` | Análogo. |
| E5 | `test_create_uses_configured_order_amount` | `CreateExecutorAction.executor_config.order_amount == config.order_amount`. |

#### Grupo F — Profitability dinâmico (5 testes)

| # | Nome | Asserção |
|---|---|---|
| F1 | `test_profitability_unchanged_when_lead_below_threshold` | Patch `best_lead_signal_bps` → `Decimal("3")` (< 5 threshold). Inventário neutro. `target_buy == target_sell == config.target_profitability`. |
| F2 | `test_buy_target_decreases_on_positive_lead` | Patch lead → `Decimal("30")`. Inventário neutro. `target_buy < config.target_profitability`. |
| F3 | `test_sell_target_increases_on_positive_lead` | Mesmo cenário. `target_sell > config.target_profitability`. |
| F4 | `test_targets_clamped_to_max_profitability` | `w_lead=Decimal("100")` (artificialmente grande). Lead grande. `target_sell` calculado excede `max_profitability`. Clamp ativo: `target_sell == config.max_profitability`. |
| F5 | `test_targets_clamped_to_min_profitability` | Mesmo trick com sinal negativo. `target_buy == config.min_profitability`. |

#### Grupo G — Inventory skew (2 testes)

| # | Nome | Asserção |
|---|---|---|
| G1 | `test_inventory_skew_high_base_decreases_buy_target` | `_setup_default_market_data(inv_base=Decimal("0.10"), inv_quote=Decimal("0"))` → 100% base. `inv_skew == 0.5`. Lead = 0. Pelo spec: `target_buy = base + 0 - 0.5 * 0.001 = base - 0.0005`. Asserção: `target_buy < config.target_profitability` (mais agressivo no bid pra... espera — agressivo significa preço mais perto do mid, ou seja, MENOR target_profitability). **Confirmação direcional**: `target_buy < base` quando inventário é todo base. Documentar no docstring do teste que essa é a interpretação canônica do spec. |
| G2 | `test_inventory_skew_low_base_decreases_sell_target` | `inv_base=0`, `inv_quote=alto` → 100% quote. `inv_skew == -0.5`. `target_sell = base + 0 + (-0.5) * 0.001 = base - 0.0005`. Asserção: `target_sell < base` (mais agressivo na venda... mas não temos base pra vender, então é inert. O teste valida o cálculo, não a economia). |

#### Grupo H — Shadow mode (2 testes)

| # | Nome | Asserção |
|---|---|---|
| H1 | `test_shadow_mode_no_create_actions` | `config.shadow_mode=True`. `executors_info=[]`. `determine_executor_actions` retorna `[]` (sem creates). |
| H2 | `test_shadow_mode_still_logs` | Shadow mode. `update_processed_data` ainda chama `controller._csv.log` 1×. |
| H3 | `test_shadow_mode_still_cancels_on_risk_gate` | Shadow mode + risk gate ativo + executor ativo. Ainda emite `StopExecutorAction` (cancelamento sempre tem precedência sobre shadow — embora improvável criar executor em shadow, defensivo). |

### 8.4 Comparação com plano da outra IA — divergências resolvidas

| Tópico | Plano outra IA | Nosso plano | Decisão |
|---|---|---|---|
| Sinal `target = base - lead_adj` direção | Tem ambiguidade reconhecida | Documentamos: `lead_bps > 0 → buy mais agressivo (target_buy menor) E sell mais conservador (target_sell maior)`. Razão: se mercado vai subir, queremos comprar antes; vendas existentes ficam expostas. | Manter como spec; testes validam fórmula, não economia. |
| FeedHealth não detecta WS hang real | Sinalizado | Mitigamos: só chamamos `mark_update` quando `bid > 0 and ask > 0`. Se Hummingbot retornar valores antigos, `bid > 0` ainda é True → não detecta. | **Aceitar limitação na fase 1**. Documentar como Risk #2. Mitigação possível futura: comparar timestamps do `OrderBook.last_diff_uid`. |
| EMA cold start | Sinalizado | Aceitar — durante warm-up basis pode estar ruidoso, mas como `is_any_stale` ainda guarda, não há ordens executadas com EMA selvagem. | Sem mitigação adicional. |
| `test/controllers/generic/__init__.py` | Sinalizado como faltando | Confirmado — incluir criação no plano de implementação. | Adicionar passo "criar `__init__.py`" na implementação. |

### 8.5 Cobertura alvo

- **Linhas**: ≥85% em `xemm_lead_lag.py`. Os 15% não cobertos são razoáveis: branches defensivos (try/except no CSV write, edge cases de connector indisponível).
- **Tempo de execução**: < 5s para a suite inteira (29 testes async com mocks pesados).

> **NOTA v2**: o teste G1 está com descrição direcionalmente errada na v1 deste documento. Veja correção em §15.6.

---

## 9. Config YAML de exemplo

Caminho: `conf/controllers/xemm_lead_lag_btc_brl.yml`

```yaml
# XEMM Lead-Lag — BTC-BRL (Bybit maker / Binance taker)
# Solo dev, VIP4 Binance, sem colocation
# Comece sempre em SHADOW MODE.

id: xemm_lead_lag_btcbrl_v1
controller_name: xemm_lead_lag
controller_type: generic

# === Mercados ===
maker_connector: bybit
maker_trading_pair: BTC-BRL
taker_connector: binance
taker_trading_pair: BTC-BRL
signal_connector: binance
signal_base_pair: BTC-USDT
signal_fx_pair: USDT-BRL

# === Sizing ===
total_amount_quote: "1000"      # bound de risco (BRL); herdado de ControllerConfigBase
order_amount: "0.0002"          # ~60 BRL @ 300k BRL/BTC; ajuste pra min_notional Bybit
order_levels: 1

# === Profitability (decimal: 0.001 = 10 bps) ===
# Round-trip fees estimado:
#  - Bybit maker spot ≈ 1 bps
#  - Binance taker spot VIP4 ≈ 2.4 bps
#  - Total ≈ 3.4 bps de break-even, +1 bps slippage taker = 4.4 bps
# Floor (min_profitability) = 7 bps pra ter margem de segurança.
min_profitability: "0.0007"
target_profitability: "0.0020"  # 20 bps inicial
max_profitability: "0.0080"     # acima disso, refresh

# === Sinal lead-lag ===
lead_windows_seconds: "5,10,15"
profitability_adjust_threshold_bps: "5"
fast_cancel_threshold_bps: "20"
min_requote_interval_sec: 3.0
w_lead: "0.5"
ema_alpha_fx: "0.3"

# === Feed health (segundos) ===
max_leader_staleness_sec: 5.0
max_fx_staleness_sec: 10.0      # USDT-BRL é menos líquido — tolerar mais
max_local_staleness_sec: 5.0

# === Risk gates ===
basis_hard_threshold_bps: "150"
max_local_spread_bps: "200"

# === Inventário ===
inventory_target_pct: "0.5"
inventory_skew_strength: "0.001"
max_inventory_deviation_pct: "0.3"

# === Operacional ===
shadow_mode: true               # **OBRIGATÓRIO INICIAR EM TRUE**
log_dir: "data/xemm_lead_lag"
kill_switch_file: "/tmp/xemm_lead_lag_pause"
```

---

## 10. Procedimento de operação em shadow mode

### 10.1 Setup inicial

1. Configurar API keys de Bybit e Binance no Hummingbot (via `connect`).
2. Verificar saldos:
   - **Binance** (taker): mínimo ~150 BRL + ~0.0005 BTC para suportar takers nos dois lados.
   - **Bybit** (maker): mesmo.
3. Criar diretório `data/xemm_lead_lag` se não existir.
4. Salvar o YAML em `conf/controllers/`.

### 10.2 Execução em shadow

```bash
./bin/hummingbot.py --config-file-name xemm_lead_lag_btc_brl.yml
```

Ou via script V2 que monta o `StrategyV2Base` com a controller.

### 10.3 O que monitorar

**No console:**
- INFO `xemm_lead_lag` aparece a cada 1s (tick).
- WARNING `Cancel gate triggered: FEED_STALE` em momentos de instabilidade — esperado eventualmente.
- **Não deve aparecer**: nenhuma linha `Created maker order`, `Filled order`, etc. (shadow mode = zero ordens).

**No CSV (`data/xemm_lead_lag/xemm_lead_lag_<id>_<ts>.csv`):**
- Uma linha por segundo. Tail em real-time:
  ```bash
  tail -f data/xemm_lead_lag/xemm_lead_lag_*.csv | column -t -s,
  ```
- Coluna `signal_quality` deve ser `OK` em ≥99% das linhas.
- Coluna `basis_bps` típica: −20 a +20 bps. Picos > 150 bps acionam `BASIS_EXTREME`.
- `lead_5s`, `lead_10s`, `lead_15s` em condições normais: ±5 a ±15 bps.

### 10.4 Análise pós-coleta (24–72h)

Notebook Jupyter sugerido (não está no escopo de implementação, mas mencionado pra orientar):

```python
import pandas as pd
df = pd.read_csv("data/xemm_lead_lag/xemm_lead_lag_<id>_<ts>.csv")

# 1. Distribuição de basis (ajustar basis_hard_threshold_bps)
df["basis_bps"].abs().describe(percentiles=[.5, .9, .95, .99, .999])

# 2. Distribuição de lead signal (calibrar fast_cancel_threshold_bps)
df[["lead_5s", "lead_10s", "lead_15s"]].abs().describe(percentiles=[.9, .95, .99])

# 3. Frequência de gates
df["cancel_reason"].value_counts(dropna=False)
# Esperado: "" >> 95%, "LEAD_SIGNAL_STRONG" pequeno, outros raros.

# 4. Predictividade do lead signal (poder preditivo)
# Adverse selection proxy: se eu tivesse um maker bid no local_mid em t,
# qual seria o local_mid em t+5s, t+15s, t+60s?
df["local_mid_+5s"]  = df["local_mid"].shift(-5)
df["local_mid_+15s"] = df["local_mid"].shift(-15)
df["local_mid_+60s"] = df["local_mid"].shift(-60)

# Correlação entre lead_5s atual e retorno futuro do local
df.corr()["lead_5s"][["local_mid_+5s", "local_mid_+15s", "local_mid_+60s"]]
# Se correlação > 0.05 (estatisticamente significativa), sinal vale a pena.
# Se < 0.02, lead-lag é fraco e talvez não compense a complexidade.

# 5. Inventário hipotético se tivesse rodado
# (não testável só com CSV, precisa simular fills — fora do escopo da fase 1)
```

### 10.5 Critério de transição shadow → live

**Mínimo recomendado:**
- ≥48h de dados shadow.
- `signal_quality == OK` em ≥99% das linhas.
- Frequência de cancel `< 30%` das linhas.
- Correlação `lead_5s vs. local_ret_+5s` > 0.03 (positiva e razoável).

**Switch:**
- Editar YAML: `shadow_mode: false`.
- Manter `order_amount` no mínimo absoluto (Bybit min_notional + ε).
- Restart do bot (não há reload em runtime confiável da config).
- Monitorar primeiras 2h manualmente.

### 10.6 Kill switch operacional

Em qualquer momento, criar o arquivo definido em `kill_switch_file` para parar imediatamente:

```bash
touch /tmp/xemm_lead_lag_pause
```

Próxima tick (≤1s) detecta o arquivo, dispara `StopExecutorAction` em todos os executores ativos, e as ordens são canceladas. Para reativar:

```bash
rm /tmp/xemm_lead_lag_pause
```

---

## 11. Verificação end-to-end

### Etapa 1: Testes unitários (ambiente local, sem rede)

```bash
cd /home/user/hummingbot-ney
python -m pytest test/hummingbot/strategy_v2/utils/test_lead_lag_signal.py -v
python -m pytest test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py -v
```

**Aprovação**: 100% verde, ≥85% cobertura combinada (verificar com `pytest --cov`).

### Etapa 2: Carga de config (sintaxe Pydantic)

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

Output esperado: id, dict com `bybit: {BTC-BRL}` e `binance: {BTC-BRL, BTC-USDT, USDT-BRL}`, `shadow: True`.

### Etapa 3: Importação dentro do Hummingbot

```bash
./compile  # se necessário
./start
```

No prompt: `start --controller xemm_lead_lag_btc_brl.yml`. Se houver erro de import, corrigir antes de avançar.

### Etapa 4: Shadow run de 1h em paper trade

- Trocar `bybit` → `binance_paper_trade` e `binance` → `binance_paper_trade` no YAML temporariamente (isso testa o fluxo, mas note que paper trade não tem BTC-BRL — pode usar ETH-USDT temporariamente só pra confirmar que a controller carrega e loga sem erros).
- Verificar:
  - CSV criado e com header correto.
  - Linhas aparecem a cada 1s.
  - `tail` mostra valores realistas.
  - Nenhuma linha de criação de ordem.

### Etapa 5: Shadow run de 48h em produção real

- Restaurar `bybit` e `binance` reais no YAML.
- Manter `shadow_mode: true`.
- Coletar CSV.
- Análise de calibração (seção 10.4).

### Etapa 6: Live mínimo (48h)

- `shadow_mode: false`, `order_amount` mínimo.
- Acompanhar:
  - Existem fills? (esperado: poucos por hora dado ordens pequenas em book pequeno).
  - Taker hedge dispara após cada fill maker?
  - Inventário oscila perto do target?
  - PnL líquido (daily realized) é ≥ 0?

### Etapa 7: Decisão de escalar

Após 48h live:
- Se `n_fills > 10` E `pnl_liq >= 0` E `max(|inventory_skew|) < 0.2`: subir `order_amount` em 2×, repetir.
- Se métricas ruins: voltar para shadow, recalibrar thresholds.

---

## 12. Riscos conhecidos e mitigações

### R1 — Iliquidez do BTC-BRL na Bybit
**Risco**: spreads largos, gaps de book, fills raros. Time to fill alto.
**Mitigação**: `max_local_spread_bps=200` cancela quando book está degradado. CSV registra `local_spread_bps` para análise. Aceito como custo do mercado.

### R2 — Staleness real do WebSocket vs `FeedHealth`
**Risco**: `FeedHealth.mark_update` é chamado quando `update_processed_data` recebe `bid > 0 and ask > 0`. Se Binance/Bybit travarem mas o connector mantiver o último valor em cache, vamos achar que o feed está OK. Não detectamos WS hang real.
**Mitigação fase 1**: aceitar limitação. Documentar.
**Mitigação futura (não nesta fase)**: comparar `OrderBook.last_diff_uid` ou `OrderBook.last_update_id` entre ticks; se não mudou em N ticks, marcar como stale verdadeiramente.

### R3 — `quote_conversion_pair = "BRL-BRL"` no XEMMExecutor
**Verificado**: `find_rate(prices, "BRL-BRL")` retorna `Decimal("1")` na linha 24 de `rate_oracle/utils.py` quando `base == quote`. **Sem código adicional necessário**. Adicionar comentário no controller documentando essa propriedade do core.

### R4 — Latência de 1s no controller loop vs. movimentos rápidos
**Risco**: o `ControllerBase.control_task` roda a cada 1s (default). Movimentos de mercado ≤500ms passam batido entre ticks.
**Mitigação**: as janelas de lead (5/10/15s) são compatíveis com tick de 1s. O `XEMMExecutor` tem seu próprio monitoramento interno de profitability (continua cancelando se taker price desvia muito). Defesa em camadas suficiente para fase 1.

### R5 — Cold start da EMA
**Risco**: primeiro `fx_mid_ema` = primeiro `fx_mid_raw`, valor pode estar fora do realista.
**Mitigação**: durante warm-up, `is_any_stale` ainda guarda contra trades. Após ~10 ticks com `alpha=0.3`, EMA converge. Aceitar.

### R6 — Vazamento de file handle do CSV no shutdown
**Risco**: se o controller é parado abruptamente, último write pode não chegar ao disco.
**Mitigação**: `buffering=1` força line-flush a cada `writerow`. Adicionalmente, override `on_stop()` (ou hook similar do `ControllerBase`) para `flush() + close()`.

### R7 — `executors_info` lag pós-cancel
**Risco**: depois de `StopExecutorAction`, leva 1 tick para o orchestrator atualizar `executors_info`. Próxima tick poderia tentar cancelar de novo.
**Mitigação**: `early_stop()` é idempotente (verifica `is_open` antes de cancelar). E o anti-churn cooldown impede recriação imediata. Sem dano.

### R8 — Diretório `test/hummingbot/strategy_v2/utils/` não existe
**Risco**: `pytest` não acha o pacote.
**Mitigação na implementação**: criar `__init__.py` vazio no momento da implementação:
```
test/hummingbot/strategy_v2/utils/__init__.py
```
(O diretório `test/hummingbot/strategy_v2/controllers/` já existe.)

### R9 — Direção do lead_adj no `target_profitability`
**Risco/Decisão**: a fórmula `target_buy = base - lead_adj` (com `lead_adj > 0` quando fair sobe mais que local) significa: target_buy menor → spread menor no bid → bid mais perto do mid → mais agressivo. **Assumimos que o sinal lead positivo prediz movimento futuro do local pra cima**, então queremos comprar agora antes da subida. Simétrico para sell.
**Mitigação**: documentar essa hipótese no docstring. Validar empiricamente com a análise da seção 10.4 (correlação do `lead_5s` com retorno futuro do local). Se a correlação for **negativa**, inverter o sinal de `lead_adj` na implementação.

### R10 — Falha no carregamento do connector Bybit BTC-BRL
**Risco**: Bybit connector pode ter bug ou rate limit ao subscrever par regional.
**Mitigação**: Etapa 4 da verificação (paper trade) detecta antes da produção. Se falhar, abrir issue no fork.

---

## 13. Critérios de aceitação do plano

Este plano está pronto para implementação se todas as condições abaixo forem verdadeiras:

- [x] Arquitetura define um único caminho de implementação (sem alternativas em aberto).
- [x] Cada arquivo a criar tem seu propósito, classes e métodos com assinaturas explícitas.
- [x] Testes cobrem cenários positivos (caminho feliz), negativos (gates de risco) e edge cases (cold start, staleness, insuficiência de buffer).
- [x] Fluxo de operação shadow → live é claramente faseado.
- [x] Riscos sinalizados têm mitigação ou aceite documentado.
- [x] Zero modificação no core do Hummingbot.

---

## 14. Próximos passos pós-aprovação

Quando este plano for aprovado, a ordem de implementação será:

1. Criar `hummingbot/strategy_v2/utils/lead_lag_signal.py` (módulo puro).
2. Criar `test/hummingbot/strategy_v2/utils/__init__.py` e `test_lead_lag_signal.py`.
3. Rodar testes unitários até verde.
4. Criar `controllers/generic/xemm_lead_lag.py` (controller).
5. Criar `test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py`.
6. Rodar testes do controller até verde.
7. Criar `conf/controllers/xemm_lead_lag_btc_brl.yml`.
8. Etapa 2 da verificação (carga de config).
9. Etapa 3 (importação no Hummingbot).
10. Etapa 4 (paper trade 1h shadow).
11. Etapa 5 (produção real shadow 48h).
12. Análise dos logs (seção 10.4).
13. Decisão go/no-go para live.

Cada item é commit/PR separado para revisão incremental.

---

## 15. Revisão v2 — correções após crítica externa

Após revisão por outra IA, fiz verificações adicionais no código do `XEMMExecutor` (linhas 92–232) e consolido aqui as mudanças. Cada item indica o status: **ACEITO** (incorporar), **PARCIAL** (com ressalva), ou **REJEITADO** (com justificativa).

### 15.1 Modelo de fees — PARCIAL

**Crítica**: VIP4 Binance Spot é 0.040%/0.052%, não 0.012%/0.024% como sugeri.
**Verificação no código**: `XEMMExecutor` calcula `_tx_cost_pct` automaticamente (linhas 159–181) via `get_tx_cost_in_asset` por perna, e usa `_maker_target_price = _taker_result_price / (1 ± (target_profitability + _tx_cost_pct))`. A comparação `min_profitability` em runtime é contra `_current_trade_profitability - _tx_cost_pct` (linha 232). **Logo: `target_profitability`, `min_profitability`, `max_profitability` são valores NET de fees**.
**Conclusão**: a crítica está **parcialmente errada** — as fees não precisam ser somadas no `min_profitability`. Os valores 7/20/80 bps net continuam adequados. Mas:
- O **comentário** "(~1.2 bps)" no YAML é factualmente errado — corrigir para "4 bps maker Binance VIP4 spot, ~5 bps Bybit VIP4 fiat-crypto (confirmar em 'My Fee Rate')".
- **Adicionar verificação operacional**: depois de 1 fill real, conferir no log do executor que `_tx_cost_pct` está coerente com fees reais (1 fill ≈ 9–10 bps round-trip esperado).

### 15.2 LIMIT vs LIMIT_MAKER (post-only) — ACEITO (achado crítico)

**Crítica**: estratégia precisa de post-only para garantir maker fee. Se LIMIT cruzar, vira taker.
**Verificação no código**: `XEMMExecutor` usa `OrderType.LIMIT` (linha 112 no order candidate, linha 218 no `place_order`). **Não usa `LIMIT_MAKER`**.
**Risco real**: latência home (100–300ms) + book volátil → cálculo de `_maker_target_price` baseado em snapshot pode resultar em ordem que cruza o book na chegada à exchange. Resultado: 2× taker fee, ~10 bps a mais por fill — mata a economia.
**Mitigação obrigatória v1** (escolher uma):
1. **Subclasse `XEMMBRLExecutor(XEMMExecutor)`** que sobrescreve `place_maker_order()` para usar `OrderType.LIMIT_MAKER` (Binance) e validar `time_in_force=PostOnly` (Bybit). Custo: ~30 linhas.
2. **Buffer adicional no preço**: na controller, somar margem extra ao `target_profitability` proporcional ao spread local (ex: `effective_target = target + 0.5 * local_spread_bps / 10000`). Reduz risco de cruzamento mas não elimina.
**Decisão**: escolher (1). Adicionar arquivo `hummingbot/strategy_v2/executors/xemm_executor/xemm_brl_executor.py` à estrutura. Atualizar `CreateExecutorAction` no controller para usar essa subclasse.
**Critério de aceite**: log de cada `BuyOrderCreatedEvent`/`SellOrderCreatedEvent` deve mostrar `order_type == LIMIT_MAKER`. Qualquer ordem que executar como taker deve disparar pause.

### 15.3 Inventário por exchange — ACEITO

**Crítica**: o plano só monitora saldo do `maker_connector`. XEMM precisa de saldo nas duas exchanges.
**Mitigação**: trocar §6.3 step 5 por:
```
base, quote = split(maker_trading_pair)
maker_base  = mdp.get_balance(maker_connector, base)
maker_quote = mdp.get_balance(maker_connector, quote)
taker_base  = mdp.get_balance(taker_connector, base)
taker_quote = mdp.get_balance(taker_connector, quote)
combined_base  = maker_base + taker_base
combined_quote = maker_quote + taker_quote
combined_pct = (combined_base * local_mid) / (combined_base * local_mid + combined_quote)
```

**Novos gates** (em `_check_risk_gates`, antes do retorno positivo):
```
6. if active_buy_planned and taker_base < min_taker_base_for_sell_hedge:
       block_buy_creation = True   # buy maker = sell taker hedge precisa de BTC na taker
7. if active_sell_planned and taker_quote < min_taker_quote_for_buy_hedge:
       block_sell_creation = True
```

**Novos campos no config**:
```yaml
min_taker_base_for_sell_hedge: "0.0005"   # BTC; mín. p/ hedgear 1 SELL maker
min_taker_quote_for_buy_hedge: "200"      # BRL; mín. p/ hedgear 1 BUY maker
```

**Novas colunas CSV**: `maker_base`, `maker_quote`, `taker_base`, `taker_quote`, `combined_base`, `combined_quote`, `combined_pct`.

### 15.4 Warmup explícito — ACEITO

**Crítica**: depois do 1º tick, `is_any_stale=False` mas o buffer ainda não tem 5–15s de história — `lead_signal_bps` retorna `None`. Bot pode criar ordens sem sinal válido.

**Mitigação**: adicionar máquina de estados de regime (substitui `should_cancel` boolean):
```python
class Regime(str, Enum):
    WARMUP = "WARMUP"
    OK = "OK"
    DEGRADED = "DEGRADED"   # gate parcial (ex: feed FX stale, ainda tradável com cuidado)
    PAUSED = "PAUSED"       # gate forte (cancela tudo, sem novas ordens)
    KILLED = "KILLED"       # kill switch ou hedge_failure crítico
```

**Lógica**:
```
self._started_at = time()  no __init__

def _compute_regime():
    if kill_switch_file or hedge_failed_count > max_consecutive_hedge_failures:
        return KILLED
    if (now - self._started_at) < config.warmup_seconds:
        return WARMUP
    if signal.is_any_stale or basis_extreme or local_spread_wide:
        return PAUSED
    if best_lead_signal_bps is None:
        return WARMUP  # ainda sem buffer cheio
    if abs(best_lead_signal_bps) > config.fast_cancel_threshold_bps:
        return PAUSED
    if signal.signal_quality in (DEGRADED_FX, DEGRADED_LEADER):
        return DEGRADED  # operável mas sem ajuste por sinal
    return OK
```

**Comportamento por regime**:
| Regime | Cancela ativos | Cria novos | Ajusta target c/ lead |
|---|---|---|---|
| WARMUP | não | **não** | n/a |
| OK | não | sim | sim |
| DEGRADED | não | sim | **não** (usa target base) |
| PAUSED | sim | não | n/a |
| KILLED | sim | não | n/a |

**Novo campo config**: `warmup_seconds: 20` (default; deve ser ≥ `max(lead_windows_seconds) + 5`).

### 15.5 EMA da FX cria sinal artificial — ACEITO

**Crítica**: EMA(USDT-BRL) introduz lag — `fair_BRL` ficaria atrás de movimentos rápidos do FX. Como `lead_signal` mede "fair vs local", lag artificial pode criar sinal espúrio.

**Mitigação**: separar dois fairs no `LeadLagSignalProvider`:
```python
@property
def fair_brl_fast(self) -> Decimal:
    """Sem suavização: usado para lead_signal_bps (micro)"""
    return self.leader_mid * self.fx_mid_raw

@property
def fair_brl_slow(self) -> Decimal:
    """Com EMA da FX: usado para basis_bps (regime)"""
    return self.leader_mid * (self._ema_fx.value or self.fx_mid_raw)
```
- `lead_signal_bps(window)` passa a usar `fair_brl_fast` no buffer.
- `basis_bps` usa `fair_brl_slow` (regime mais estável).
- CSV ganha colunas `fair_brl_fast`, `fair_brl_slow`.

### 15.6 Correção dos testes de inventory skew — ACEITO (bug)

**Bug confirmado**: o pseudocódigo §6.5 diz:
```
target_buy  += inv_adj   # inv_skew > 0 → target_buy MAIOR (bid menos agressivo)
target_sell -= inv_adj   # inv_skew > 0 → target_sell MENOR (ask mais agressivo)
```
Direção correta: excesso de base → segurar BUY (target_buy maior) e empurrar SELL (target_sell menor).

**Erro nos testes G1/G2**:
- G1 v1 dizia "high base **decreases** buy target" — **errado**. Correto: "high base **increases** buy target".
- G2 v1 dizia "low base **decreases** sell target" — **errado**. Correto: "low base **increases** sell target" (segurar venda quando não temos BTC).

**Substituição**:
| # | Nome corrigido | Asserção |
|---|---|---|
| G1 | `test_inventory_skew_high_base_increases_buy_target_and_decreases_sell_target` | inv_pct=1.0, target=0.5 → inv_skew=+0.5. `target_buy > base_target` E `target_sell < base_target`. |
| G2 | `test_inventory_skew_low_base_decreases_buy_target_and_increases_sell_target` | inv_pct=0.0 → inv_skew=-0.5. `target_buy < base_target` E `target_sell > base_target`. |

### 15.7 CSV deve logar Binance BTC-BRL (taker) — ACEITO

**Crítica**: sem o L1 do taker (Binance BTC-BRL), não é possível diagnosticar se o problema foi lead-lag ruim, hedge caro, ou maker fora do fair.

**Mitigação**: adicionar 3º par lido em `update_processed_data`:
```python
taker_local_bid = mdp.get_price_by_type(taker_connector, taker_trading_pair, BestBid)
taker_local_ask = mdp.get_price_by_type(taker_connector, taker_trading_pair, BestAsk)
taker_local_mid = (taker_local_bid + taker_local_ask) / 2
```

**Novas colunas CSV**:
- `taker_local_bid`, `taker_local_ask`, `taker_local_mid`
- `taker_local_spread_bps`
- `maker_vs_taker_bps = 10000 * (maker_mid / taker_mid - 1)`
- `taker_vs_fair_bps = 10000 * (taker_mid / fair_brl_slow - 1)`
- `maker_vs_fair_bps = 10000 * (maker_mid / fair_brl_slow - 1)` (mesmo que `basis_bps`, mas explicitar)

**Nota sobre `update_markets`**: `taker_trading_pair` já está registrado lá. Apenas o controller lê o L1 também (não precisa subscribe extra).

### 15.8 Métricas de hedge — ACEITO

**Crítica**: XEMM tem exposição transitória durante o hedge. Sem métricas, nunca saberemos qual a janela.

**Mitigação**: nova classe `HedgeMonitor` no controller que:
- Em cada `BuyOrderCompletedEvent`/`SellOrderCompletedEvent` no maker: marca `t_fill = now`.
- No `BuyOrderCreatedEvent`/`SellOrderCreatedEvent` no taker: marca `t_taker_sent = now`. `hedge_latency_ms = (t_taker_sent - t_fill) * 1000`.
- No `BuyOrderCompletedEvent`/`SellOrderCompletedEvent` no taker: calcula `hedge_slippage_bps` vs taker_mid esperado no momento do fill maker.
- `MarketOrderFailureEvent` no taker: incrementa `hedge_failed_count`.

**Eventos vão pro `XEMMExecutor` via `process_order_*_event`**, mas o controller também recebe via `executors_info[i].custom_info`. Solução prática v1: ler periodicamente os custom_info dos executores `is_done` e logar em arquivo separado `hedge_metrics.csv`.

**Novos campos config**:
```yaml
max_consecutive_hedge_failures: 1   # 1 falha → KILLED
max_hedge_slippage_bps: "20"        # alerta no log; > 20 bps por 3 fills consecutivos → KILLED
```

### 15.9 Staleness real via `last_diff_uid` — ACEITO (upgrade de prioridade)

**Crítica**: `bid > 0 and ask > 0` não prova feed vivo (cache). Risco real de o WS travar e não detectarmos.

**Mitigação v1** (não mais "futura"):
- Em `LeadLagSignalProvider.update()`, aceitar parâmetros opcionais `local_book_uid`, `leader_book_uid`, `fx_book_uid`.
- Cada `FeedHealth` ganha um `last_uid: Optional[int]` campo. `mark_update(timestamp, uid)` só chama o real `mark_update` se `uid != last_uid`. Senão, considera "no update" e o feed vai envelhecendo.
- No controller, ler via `mdp.get_order_book(connector, pair).last_diff_uid` (atributo do Cython OrderBook).
- Se `last_diff_uid` não estiver disponível em algum connector, fallback para o método antigo (preço positivo) com log WARNING.

**Custo**: ~20 linhas extras. Risco mitigado.

### 15.10 w_lead, fast_cancel_threshold — ACEITO (defaults conservadores)

**Mitigação**: alterar defaults na seção §9 (config YAML):
- `w_lead: "0.5"` → `"0.20"`
- `fast_cancel_threshold_bps: "20"` → `"15"`
- adicionar `soft_cancel_threshold_bps: "10"` (entra em DEGRADED, não cancela ainda)

### 15.11 YAML — listas e campos — ACEITO

**Mitigação**:
- `lead_windows_seconds: "5,10,15"` → bloco YAML real:
  ```yaml
  lead_windows_seconds:
    - 5
    - 10
    - 15
  ```
- Remover do YAML campos não declarados na config (`order_levels`, `max_inventory_deviation_pct`) ou adicionar à `XEMMLeadLagConfig`.
- Adicionar `model_config = ConfigDict(extra="forbid")` no Pydantic config para falhar explicitamente em campo desconhecido.

### 15.12 Análise estatística — ACEITO

**Mitigação**: substituir o snippet em §10.4 por:
```python
import numpy as np

# 1. Compute returns (NOT levels)
df["local_ret_5s_bps"]  = 10000 * np.log(df["local_mid"].shift(-5)  / df["local_mid"])
df["local_ret_15s_bps"] = 10000 * np.log(df["local_mid"].shift(-15) / df["local_mid"])
df["local_ret_60s_bps"] = 10000 * np.log(df["local_mid"].shift(-60) / df["local_mid"])

# 2. Correlation: lead now vs return future
print(df[["lead_5s", "lead_10s", "lead_15s",
          "local_ret_5s_bps", "local_ret_15s_bps", "local_ret_60s_bps"]].corr())

# 3. Conditional expectation (more useful than corr)
threshold = 10  # bps
print("E[ret_5s | lead_5s > +10]:", df.loc[df["lead_5s"] > threshold, "local_ret_5s_bps"].mean())
print("E[ret_5s | lead_5s < -10]:", df.loc[df["lead_5s"] < -threshold, "local_ret_5s_bps"].mean())

# 4. Hit ratio
df["hit_5s"] = np.sign(df["lead_5s"]) == np.sign(df["local_ret_5s_bps"])
print("Hit ratio when |lead_5s|>10:", df.loc[df["lead_5s"].abs() > 10, "hit_5s"].mean())
```

**Nota sobre `shift(-N)`**: assume 1 linha = 1 segundo. Se ticks variarem (raros gaps), usar `pd.merge_asof` por timestamp.

### 15.13 Testes adicionais para `StopExecutorAction` — ACEITO

**Mitigação**: adicionar 4 testes ao grupo C de §8.3:

| # | Nome | Cenário |
|---|---|---|
| C9 | `test_stop_on_open_maker_order_cancels_it` | Executor com maker LIMIT aberta. `early_stop()` chama `strategy.cancel(...)`. |
| C10 | `test_stop_on_partial_fill_does_not_cancel_taker_hedge` | Executor pós-fill maker (status SHUTTING_DOWN), taker em voo. `early_stop()` não interfere no taker. (Verificar comportamento real do XEMMExecutor — se ele NÃO cancelar taker em SHUTTING_DOWN, ok.) |
| C11 | `test_stop_on_done_executor_is_idempotent` | Executor `is_done=True`. `early_stop()` é no-op (não levanta exceção). |
| C12 | `test_stop_does_not_block_pending_taker_hedge` | Estado intermediário: maker filled, taker not yet placed. `early_stop()` deve permitir o taker hedge prosseguir (XEMMExecutor lifecycle). |

**Estes testes provavelmente vão exigir leitura adicional do `XEMMExecutor.early_stop()` para confirmar o comportamento — anotar isso na implementação.**

### 15.14 Shadow mode → micro-live obrigatório — ACEITO

**Mitigação**: dividir Etapa 6 da §11 em duas:
- **Etapa 6a (micro-live, 48h)**: `shadow_mode=false`, `order_amount` mínimo absoluto (1× `min_notional` da Bybit). Foco em: validar fill, hedge latency, slippage, post-only, regime de fees real, comportamento do executor.
- **Etapa 6b (scale-up)**: só após 6a verde por 48h. Subir `order_amount` em 2× e repetir 24h.

### 15.15 Circuit breakers diários — ACEITO

**Novos campos config + lógica no `_compute_regime`**:
```yaml
max_daily_loss_brl: "100"
max_daily_fills: 50           # cap defensivo: limita volume diário
max_consecutive_hedge_failures: 1
max_hedge_slippage_bps: "20"
max_unhedged_time_ms: 3000
```

**Implementação**: o controller mantém estado diário (reset à meia-noite UTC):
- `_daily_pnl_brl`, `_daily_fills`, `_consecutive_hedge_failures`, `_max_hedge_slippage_today`
- Atualiza em cada evento de fill/hedge (lendo `executors_info[i].custom_info`).
- Se qualquer limite for excedido → `Regime.KILLED` permanente até reload.

### 15.16 Resumo das mudanças nos arquivos

| Arquivo | Mudanças |
|---|---|
| `lead_lag_signal.py` | +`fair_brl_fast`/`fair_brl_slow`; FeedHealth aceita `uid` opcional |
| `xemm_brl_executor.py` (NOVO) | Subclasse de `XEMMExecutor` com `OrderType.LIMIT_MAKER` |
| `xemm_lead_lag.py` | +Regime enum; +inventário por exchange; +warmup state; +HedgeMonitor; +circuit breakers; +taker book L1; usa `XEMMBRLExecutor` em vez de `XEMMExecutor` |
| `test_lead_lag_signal.py` | +Testes para `fair_brl_fast/slow`; +Testes para FeedHealth com uid |
| `test_xemm_lead_lag.py` | Correção G1/G2; +C9–C12 (stop edge cases); +Testes para Regime states; +Testes para inventory por exchange gates |
| `xemm_lead_lag_btc_brl.yml` | YAML em listas; novos campos; defaults conservadores; comentários de fees corrigidos |
| `test_xemm_brl_executor.py` (NOVO) | Testes para garantir `OrderType.LIMIT_MAKER` no order candidate |

### 15.17 Crítica REJEITADA / RESSALVADA

**Item 1 (fees)**: parcialmente rejeitada — a soma de fees no `min_profitability` não é necessária porque o XEMMExecutor já trata via `_tx_cost_pct`. Mas o comentário corrigido fica.

**Item 14 (shadow não valida PnL)**: já estava implícito no plano (etapas 6 e 7 separadas), mas tornei explícito em §15.14.

### 15.18 Itens fora do escopo da v1 (registrar para v2 futura)

- Backtest formal com dados históricos (custo > valor agora).
- Microprice como input do quote_center.
- Spread assimétrico além do ajuste por lead.
- Volatility-adaptive spreads.
- Multi-level (>1 ordem por lado).
- Kelly-based sizing.

---

## Apêndice A — Arquivos críticos do core para referência

- `hummingbot/strategy_v2/executors/xemm_executor/xemm_executor.py` — XEMM lifecycle, `early_stop`, `update_current_trade_profitability`.
- `hummingbot/strategy_v2/executors/xemm_executor/data_types.py` — `XEMMExecutorConfig` schema.
- `hummingbot/strategy_v2/controllers/controller_base.py` — `ControllerBase.update_processed_data`, `determine_executor_actions`, `filter_executors`, `executors_info`.
- `hummingbot/strategy_v2/models/executor_actions.py` — `CreateExecutorAction`, `StopExecutorAction`.
- `hummingbot/strategy_v2/executors/executor_orchestrator.py` — dispatch de actions.
- `hummingbot/data_feed/market_data_provider.py` — `get_price_by_type`, `get_balance`, `get_order_book`.
- `hummingbot/core/data_type/common.py` — `PriceType`, `TradeType`.
- `hummingbot/core/rate_oracle/utils.py:24` — `find_rate` retorna `Decimal("1")` para `base == quote` (BRL-BRL).
- `controllers/generic/xemm_multiple_levels.py` — referência de padrão de controller XEMM existente.
- `test/hummingbot/strategy_v2/executors/xemm_executor/test_xemm_executor.py` — referência de padrão de testes com mocks.
