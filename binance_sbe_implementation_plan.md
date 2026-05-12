# Plano: Conector `binance_sbe` — Binance Spot via Simple Binary Encoding

## Context

O bot XEMM Lead-Lag (BTC-BRL, branch `claude/xemm-leadlag`) consome market data da Binance via WebSocket JSON em `wss://stream.binance.com:9443/ws`, usando o conector `binance` para signal (BTC-USDT) e taker (BTC-BRL). A Binance publicou **SBE Market Data Streams** em `wss://stream-sbe.binance.com:9443/ws`, com payload binário menor e parse mais rápido. Trading/user-stream/REST permanecem em JSON+HMAC; SBE é só market data público.

O objetivo é adicionar um conector **irmão** chamado `binance_sbe` que herda 100% do `binance` atual e sobrescreve **apenas** o `OrderBookTrackerDataSource` para consumir o stream SBE. Zero modificações ao conector `binance` existente — o XEMM ativo hoje fica intocado até cutover via 1 linha de YAML. Conector novo entrega potencial de redução de latência e jitter no caminho de sinal/taker, validado em script standalone antes de plugar no bot real.

Decisões registradas:
- **Conector irmão**, não flag — Opção B do framework simples/robusto. Permite rodar `binance` e `binance_sbe` simultâneos para validação AB.
- **Full trading capability mantida via herança** (custo zero de código), MAS rollout live começa por `signal_connector` apenas. Taker fica em `binance` JSON até N dias de estabilidade do signal. Trading via SBE não existe na Binance Spot pública — então usar `binance_sbe` como taker apenas centraliza config, sem ganho de latência (chamadas REST de order ainda vão pra api.binance.com/...).
- **Multipair genérico** — implementar como o conector original (qualquer pair que a Binance Spot suporte em SBE).
- **Ed25519 já provisionada** pelo usuário — config recebe apenas a **API key string** Ed25519 (não PEM). Header `X-MBX-APIKEY` consome a string direto, sem signing (market data público).
- **Decoder primeiro como gate** — `sbe_decoder.py` + golden fixtures de bytes verdes ANTES de qualquer outro arquivo.
- **Validação standalone obrigatória** em `tools/binance_sbe_shadow.py` antes de cutover no bot.
- **Escopo Fase 1 do decoder:** apenas `@trade` (templateId 10000) e `@depth` diff (templateId 10003). `@bestBidAsk` (10001) e `@depth20` (10002) ficam para Fase 2 se aparecer caso de uso — XEMM atual não consome BBO separadamente nem precisa de top-20 snapshot (snapshot REST de 1000 níveis basta).

## Referência SBE (Binance Spot, oficial)

- **Endpoint:** `wss://stream-sbe.binance.com:9443/ws/<streamName>` (single) ou `/stream?streams=<a>/<b>` (multi)
- **Streams:** `<sym>@trade`, `<sym>@bestBidAsk`, `<sym>@depth` (25ms diff), `<sym>@depth20` (50ms top-20 snapshot)
- **Schema XML:** `binance-spot-api-docs/sbe/schemas/stream_1_0.xml` — pelo nome do arquivo `schemaId=1, version=0`. **A primeira tarefa do dev é verificar isso no XML antes de codar e ajustar constantes.**
- **Template IDs esperados** (verificar no XML): trade=10000, bestBidAsk=10001, depth20=10002, depth diff=10003. **Fase 1 do decoder cobre 10000 e 10003 apenas.**
- **Timestamps em microssegundos** no campo `eventTime` de cada frame. **Decoder DEVE converter para milissegundos** ao popular o campo `E` (compatibilidade com `BinanceOrderBook.trade_message_from_exchange:71` que faz `ts * 1e-3` assumindo millis). Preservar `E_us` adicional para métricas.
- **Preços/quantidades** vêm como `mantissa` (int64) + `exponent` lido **do frame** (campos `priceExponent`/`qtyExponent`). NÃO assumir -8 hardcoded. Output do decoder em **string decimal exata** (igual ao JSON), não Decimal — alinhamento com `BinanceOrderBook` que recebe strings.
- **`@trade` frames podem conter MÚLTIPLOS trades** (repeating group `trades` em `TradesStreamEvent`). Decoder retorna `list[dict]`, nunca `dict` único.
- **Auth:** header `X-MBX-APIKEY` recebe a **API key string Ed25519** registrada no portal Binance (NÃO o PEM privado, NÃO public key hex). Sem signing — market data público. Sem timestamp.
- **Reconnect:** conexão tem TTL máximo de 24h. Reconnect proativo antes desse limite deve ser implementado/validado.
- **Doc:** https://developers.binance.com/docs/binance-spot-api-docs/sbe-market-data-streams

## Arquitetura — override mínimo via herança

Override point validado: `BinanceExchange._create_order_book_data_source()` em `hummingbot/connector/exchange/binance/binance_exchange.py:144-149` é a única factory que precisa ser sobrescrita. Auto-discovery do Hummingbot (`hummingbot/client/settings.py:233-308`) detecta o novo conector pelo nome do diretório + presença de `binance_sbe_utils.py` com `KEYS`/`EXAMPLE_PAIR`/`DEFAULT_FEES` — **zero patch em arquivos compartilhados** (`AllConnectorSettings`, controllers, etc.).

Shape exigida pelo decoder (validada em `hummingbot/connector/exchange/binance/binance_order_book.py`):
- Trade: `{"E", "s", "t", "p", "q", "m"}` (timestamp, symbol, trade_id, price, qty, is_buyer_maker)
- Diff: `{"U", "u", "b", "a"}` (first_update_id, last_update_id, bids, asks)
- Snapshot: `{"lastUpdateId", "bids", "asks"}`

Decoder deve produzir dicts com EXATAMENTE essas keys → `binance_order_book.py:32-71` (`diff_message_from_exchange`, `trade_message_from_exchange`, `snapshot_message_from_exchange`) reusa **sem alteração**.

## Arquivos a criar

Tudo em `hummingbot/connector/exchange/binance_sbe/` (novo diretório):

### 1. `__init__.py` (vazio)

### 2. `binance_sbe_constants.py` (~30 linhas)

- `WSS_SBE_URL = "wss://stream-sbe.binance.com:9443/ws"`
- `SBE_SCHEMA_ID = 1`, `SBE_SCHEMA_VERSION = 0` — bate com `stream_1_0.xml` (`id="1" version="0"`). **Re-verificar lendo o XML antes de codar; ajustar se Binance bumpar.**
- Template IDs: `SBE_TRADES_TEMPLATE_ID = 10000`, `SBE_DEPTH_DIFF_TEMPLATE_ID = 10003`. `10001` (bestBidAsk) e `10002` (depth20) ignorados na Fase 1.
- Re-export de tudo de `binance_constants` (REST URLs, rate limits, paths) — `from hummingbot.connector.exchange.binance.binance_constants import *`
- Override só do que muda: WS URL + constantes SBE.

### 3. `binance_sbe_utils.py` (~70 linhas)

```python
class BinanceSbeConfigMap(BaseConnectorConfigMap):
    connector: str = "binance_sbe"
    binance_sbe_api_key: SecretStr = Field(...)          # API key string Ed25519 (header X-MBX-APIKEY)
    # HMAC fields kept optional — only needed if user routes trading through binance_sbe
    # (which gains nothing vs binance, since SBE doesn't cover trading).
    binance_api_key: Optional[SecretStr] = Field(default=None)
    binance_api_secret: Optional[SecretStr] = Field(default=None)
```

- `EXAMPLE_PAIR = "BTC-USDT"`, `DEFAULT_FEES`, `is_exchange_information_valid` re-exportados de `binance_utils`.
- `KEYS = BinanceSbeConfigMap.model_construct()` — gatilho para auto-discovery.
- **NÃO inclui** Ed25519 PEM. Para market data SBE só precisa da string da API key (registrada como tipo Ed25519 no portal Binance) no header. Sem signing.

### 4. `sbe_decoder.py` (~250 linhas) — **GATE DE QUALIDADE #1**

Função pura: `decode_frame(buf: bytes) -> list[dict]` (lista vazia se template ID desconhecido, com warning; nunca `None`).

- Lê `MessageHeader` (8 bytes: `blockLength` u16, `templateId` u16, `schemaId` u16, `version` u16).
- Valida `schemaId == SBE_SCHEMA_ID`; mismatch eleva `SbeSchemaMismatchError` (fail-stop).
- Validação de `version`: se `version > SBE_SCHEMA_VERSION` e `blockLength >= esperado`, aceita parse compatível (mudanças aditivas) **mas loga métrica/alerta**. Se `version > SBE_SCHEMA_VERSION` e `blockLength < esperado`, fail-stop.
- Despacha por `templateId` para 2 parsers dedicados (Fase 1):
  - `_parse_trades(buf, offset, blockLength) -> list[dict]` — TradesStreamEvent contém repeating group `trades`. Output por trade:
    ```python
    {"e": "trade",
     "E": event_time_us // 1000,     # millis, compatível com BinanceOrderBook
     "E_us": event_time_us,           # micros preservados para métricas
     "s": symbol,
     "t": trade_id,
     "p": price_str,                  # string decimal exata, via mantissa+exponent
     "q": qty_str,
     "m": is_buyer_maker}
    ```
  - `_parse_depth_diff(buf, ...) -> list[dict]` (1 dict só, mas retorna lista pra uniformidade). Output:
    ```python
    [{"e": "depthUpdate",
      "E": event_time_us // 1000,
      "E_us": event_time_us,
      "s": symbol,
      "U": first_update_id,
      "u": last_update_id,
      "b": [[px_str, qty_str], ...],
      "a": [[px_str, qty_str], ...]}]
    ```
- `templateId` desconhecido (10001 bestBidAsk ou 10002 depth20, ou novos no futuro): warning + `return []`. Não é fail-stop porque mudanças aditivas no schema são esperadas.
- `struct.unpack_from` em offsets fixos. **CRÍTICO — duas formas de repeating groups no schema:**
  - `groupSizeEncoding`: `blockLength` uint16 + `numInGroup` **uint32** — usado por `TradesStreamEvent` (grupo `trades`)
  - `groupSize16Encoding`: `blockLength` uint16 + `numInGroup` **uint16** — usado por `DepthDiffStreamEvent` (grupos `bids`, `asks`)
  - Decoder DEVE ter dois helpers: `_read_group_header_size32(buf, offset)` e `_read_group_header_size16(buf, offset)`. Usar tamanho errado desalinha offsets e **corrompe o book inteiro**. Confirmar `dimensionType` no XML de cada grupo antes de codar.
- Preços/quantidades: lê `mantissa` (int64) + `exponent` **do próprio frame** (campos `priceExponent`/`qtyExponent`, NÃO hardcoded). Converte para string decimal exata via helper `_mantissa_to_decimal_str(mantissa, exponent) -> str` (sem `Decimal`, sem float — pure string manipulation). Validar em golden test que output bate com string do JSON equivalente.

### 5. `binance_sbe_api_order_book_data_source.py` (~180 linhas)

`class BinanceSbeAPIOrderBookDataSource(BinanceAPIOrderBookDataSource)`:

- Override `_connected_websocket_assistant`: conecta em `CONSTANTS.WSS_SBE_URL`, adiciona header `X-MBX-APIKEY` recebendo **a API key string** direto do config (`binance_sbe_api_key`). Sem PEM, sem signing.
- Override `_subscribe_channels`: payload de subscribe é **JSON** (igual ao conector original) — só a resposta é binária. Reusa `WSJSONRequest`. Streams enviados: `<sym>@trade` e `<sym>@depth` (sem `@100ms` — SBE depth é 25ms).
- Override `subscribe_to_trading_pair` e `unsubscribe_to_trading_pair`: a base usa `@depth@100ms`; para SBE remover o sufixo `@100ms`. Helper privado `_depth_stream(symbol) -> f"{symbol.lower()}@depth"` para evitar drift.
- Override `_process_websocket_messages`: **NÃO depender de `WSResponse.type`** — o `_build_resp()` em `hummingbot/core/web_assistant/connections/ws_connection.py` não preserva o tipo do frame. Usar `isinstance` em `ws_response.data`:
  ```python
  if isinstance(ws_response.data, bytes):
      events = sbe_decoder.decode_frame(ws_response.data)
      for event in events:
          await self._dispatch(event)
  elif isinstance(ws_response.data, dict):
      # JSON response (ex: confirmação de SUBSCRIBE/UNSUBSCRIBE)
      self._handle_subscription_ack(ws_response.data)
  else:
      self.logger().warning(f"unexpected ws payload type: {type(ws_response.data)}")
  ```
  Despachar cada `event` por `event["e"]` (`"trade"` ou `"depthUpdate"`) para o queue correto via `_channel_originating_message`.
- Reusa: `_request_order_book_snapshot` (REST snapshot **continua JSON 1000 níveis** — não usamos `@depth20` SBE para isso), `_order_book_snapshot`, todo boilerplate de `OrderBookTrackerDataSource`.
- **Reconnect proativo:** override `listen_for_subscriptions` para forçar reconexão a cada ~23h (margem de 1h vs TTL de 24h da Binance). Implementação simples: `asyncio.wait_for` com timeout, captura `TimeoutError`, deixa o ciclo de `_connected_websocket_assistant` re-conectar.

### 6. `binance_sbe_exchange.py` (~60 linhas)

`class BinanceSbeExchange(BinanceExchange)`:

- `@property name → "binance_sbe"`
- `__init__`: aceita `binance_sbe_api_key: str` (a API key Ed25519, não PEM), armazena, repassa para o data source na factory. Recebe HMAC (`binance_api_key`/`binance_api_secret`) como `Optional` — só usados se trading via este conector estiver habilitado.
- **Trava explícita de boot:**
  ```python
  if self._trading_required and (not binance_api_key or not binance_api_secret):
      raise ValueError(
          "binance_sbe requires HMAC binance_api_key + binance_api_secret "
          "when trading_required=True. For signal-only use, pass trading_required=False."
      )
  ```
  Em Fase 1 (signal_connector), o framework instancia com `trading_required=False`. Se alguém configurar como taker sem HMAC, falha no boot com erro acionável — não silencioso no primeiro `buy()`.
- `_create_order_book_data_source()`: retorna `BinanceSbeAPIOrderBookDataSource(..., sbe_api_key=self._sbe_api_key)`.
- Todo o resto (auth HMAC, trading, user stream, fees, rate limits, REST) **herdado sem mudança**.

**Total novo:** ~530 linhas (decoder enxugou pra ~250 sem bestBidAsk/depth20). **Reusado por herança:** ~1200 linhas do binance original.

## Plano de testes — 3 camadas

### Camada 1: Unit tests (offline, CI default)

Local: `test/hummingbot/connector/exchange/binance_sbe/`

#### 1a. `test_sbe_decoder.py` (~250 linhas) — **bloqueia tudo se não passar**

- `test_decode_single_trade_golden_fixture` — frame com 1 trade → list[dict] de tamanho 1, shape correta
- `test_decode_multi_trade_golden_fixture` — frame com N trades → list[dict] de tamanho N (caso crítico, fácil de quebrar)
- `test_decode_trade_timestamp_converted_us_to_ms` — `event_time_us=1700000000000000` resulta em `E=1700000000000`, `E_us=1700000000000000`
- `test_decode_depth_diff_golden_fixture_0_levels` (depth update com 0 níveis)
- `test_decode_depth_diff_golden_fixture_1_level` (boundary)
- `test_decode_depth_diff_golden_fixture_n_levels` (10+ níveis, bids+asks)
- `test_decode_price_qty_as_string_via_dynamic_exponent` — mantissa+exponent variável (não hardcoded -8) reconstrói string idêntica ao JSON
- `test_decode_rejects_wrong_schema_id` (SbeSchemaMismatchError)
- `test_decode_version_higher_blocklength_compatible_accepts_with_warning` (mudanças aditivas no schema não quebram)
- `test_decode_version_higher_blocklength_smaller_fail_stops` (não-aditivo)
- `test_decode_handles_truncated_frame` (buffer cortado → exception clara)
- `test_decode_unknown_template_id_returns_empty_list_with_warning` (template 10001/10002 ainda não suportados na Fase 1)

Fixtures de bytes em `test/.../binance_sbe/fixtures/*.bin` (5-10KB total, capturadas uma vez do stream real, commitadas com SHA-256 documentado). Para cada fixture binária, fixture JSON pareada com o mesmo evento para validação cruzada de preço/qty/timestamp.

#### 1b. `test_binance_sbe_api_order_book_data_source.py` (~550 linhas)

Adaptado de `test/hummingbot/connector/exchange/binance/test_binance_api_order_book_data_source.py`:
- Mesmos métodos: `test_listen_for_trades_successful`, `test_listen_for_order_book_diffs_successful`, `test_order_book_snapshot`, `test_subscribe_to_trading_pair`, `test_unsubscribe_from_trading_pair`
- Mock: `NetworkMockingAssistant` enviando `WSResponse(data=<fixture bytes>)`. Validar despacho via `isinstance(data, bytes)` (não via `WSMsgType.BINARY` que `_build_resp` não preserva).
- Asserções: após decode, `OrderBookMessage` produzido tem mesma shape do binance JSON equivalente (mesmo `update_id`, `bids`, `asks`, `trade_id`, `timestamp`).
- **`test_reconnect_on_artificial_ttl_short_window`** (importante): override `RECONNECT_INTERVAL_SEC` para 30s via env var de teste, simula conexão ativa, faz frames fluírem por ~35s, valida ciclo `connect → subscribe → receive → timeout → disconnect-clean → reconnect → resubscribe → receive`. Confirma 0 state leak (queue clean, sem orders fantasma de subscribe duplicado, sem warnings de "channel already subscribed").
- `test_subscribe_uses_at_depth_not_at_depth_100ms` — valida que `subscribe_to_trading_pair` gera URL `@depth` (sem `@100ms`).
- `test_unsubscribe_uses_at_depth_not_at_depth_100ms`.

#### 1c. `test_binance_sbe_exchange.py` (~50 linhas)

- `test_name_property_returns_binance_sbe`
- `test_create_order_book_data_source_returns_sbe_variant`
- `test_inherits_binance_trading_methods` (smoke check de herança)
- NÃO duplica testes de trading/auth — esses ficam no `binance` original.

#### 1d. `test_binance_sbe_utils.py` (~30 linhas)

- Valida `BinanceSbeConfigMap` campos, `KEYS.model_construct`, `EXAMPLE_PAIR`.

### Camada 2: Regression tests (garantir que `binance` original ficou intocado)

- Nenhum arquivo em `hummingbot/connector/exchange/binance/` ou `test/.../binance/` é tocado.
- Comando: `pytest test/hummingbot/connector/exchange/binance/ -v` deve passar 100%.
- Sanity de auto-discovery (adicionar em `test_binance_sbe_utils.py`):
  ```python
  def test_binance_sbe_appears_in_connector_settings():
      from hummingbot.client.settings import AllConnectorSettings
      AllConnectorSettings.create_connector_settings()
      settings = AllConnectorSettings.get_connector_settings()
      assert "binance_sbe" in settings
      assert "binance" in settings  # original ainda lá
  ```

### Camada 3: Integration tests (live, opt-in)

Local: `test/hummingbot/connector/exchange/binance_sbe/integration/test_sbe_live.py`

- Markers `@pytest.mark.live` + `@pytest.mark.binance_sbe`. Registrar em `pyproject.toml` ou `pytest.ini`. CI default: `pytest -m "not live"`.
- Casos:
  - `test_live_connect_and_receive_10_diffs` — conecta, espera 10 frames, valida shape
  - `test_live_subscribe_unsubscribe_cycle` — assina BTC-USDT, desassina, re-assina
  - `test_live_schema_version_matches` — valida `header.version == SBE_SCHEMA_VERSION`. **Canário** se Binance bumpar schema.
- Execução manual:
  ```bash
  export SBE_API_KEY=<api-key-string-ed25519>
  pytest test/hummingbot/connector/exchange/binance_sbe/integration/ \
    -m live --sbe-api-key=$SBE_API_KEY
  ```

## Validação isolada em produção (ANTES de plugar no XEMM)

### `tools/binance_sbe_shadow.py` (~280 linhas, standalone)

- Importa `BinanceExchange` e `BinanceSbeExchange` direto (sem strategy/controller).
- Cria duas instâncias com `trading_required=False`, mesmo set de trading pairs (configurável via CLI, default `["BTC-USDT"]`).
- Inicia ambos os order book trackers em paralelo.
- Loop principal: subscreve a eventos de ambos, registra CSV `var/sbe_shadow/<YYYYMMDD-HHMM>.csv`:
  - `ts_recv_ns` (`time.monotonic_ns()`), `source` (`json|sbe`), `event_type` (`trade|diff`), `seq_id` (`update_id` ou `trade_id`), `best_bid`, `best_ask`, `bid_qty`, `ask_qty`
  - `decode_time_ns` por mensagem (tempo gasto no decode — instrumentado no decoder e no parser JSON do binance). **Isso é o que mede ganho real de parse, mais confiável que CPU agregada do processo (que sofre contaminação JSON+SBE no mesmo event loop).**
  - `queue_delay_ns` opcional (tempo entre frame recebido e dispatch para handler) — detecta backlog.
- Rotação por hora. SIGINT finaliza cleanly e escreve `summary.json`.
- Amostragem CPU via `psutil.Process().cpu_percent()` a cada 5s — **como referência, NÃO como métrica primária de ganho** (mesmo processo mistura cargas dos dois conectores).
- **Modo A/B em processos separados** suportado via flag `--mode=json_only` ou `--mode=sbe_only`: nessa modalidade roda só um conector por vez, runs separados, e CPU agregada vira métrica confiável. Recomendado pra etapa final de validação.

### `tools/binance_sbe_analyze.py` (~150 linhas, pandas)

**IMPORTANTE:** JSON Binance é `@depth@100ms`, SBE é `@depth` 25ms. Comparação simétrica por `update_id` é matematicamente injusta (SBE tem mais eventos por design). Análise correta:

**Para trades** (frequência similar nos dois streams, eventos discretos):
- Pareados por `trade_id` (1:1, deve bater 100%)
- `delta_ms = ts_json_recv - ts_sbe_recv` por trade
- Estatísticas: p50, p90, p95, p99, max, min, média, std-dev

**Para depth** (frequência diferente entre streams):
- **Gaps SBE** detectados por sequência `U/u` (esperado: 0 ou raros após reconnect)
- **Gaps JSON** detectados na mesma janela de tempo (comparativo)
- **Top-of-book reconstruído** a cada 100ms: aplicar todos os diffs SBE/JSON acumulados nessa janela e comparar best bid/ask. Divergência >1 tick = bug.
- **Staleness** por evento: `local_recv_ts - event_ts` (`E_us`). Estatística por stream (p50, p95).
- **Resnapshot count**: contar ocorrências de chamada REST snapshot disparada por gap, comparar SBE vs JSON.

**Sistema (ambos streams):**
- **CPU**: média e p99 ao longo do run, SBE deve estar ≤1.1× JSON
- **Jitter**: std-dev de inter-arrival time entre frames

### Critério GO / NO-GO (revisado)

**Decoder (obrigatório, gate hard):**
- 0 `SbeSchemaMismatchError`
- 0 frames truncados não tratados
- 0 trades perdidos em frames multi-trade (validar contra `trade_id` continuidade vs JSON)
- 0 divergência de price/qty entre SBE decoded e JSON equivalente nas fixtures

**Market data (operacional):**
- Gaps SBE/hora ≤ gaps JSON/hora (ou explicáveis por reconnect)
- Resnapshot count SBE ≤ JSON
- Top-of-book divergente >1 tick em <0.1% do tempo amostrado
- SBE median staleness < JSON median staleness no depth
- SBE p95 staleness ≤ JSON p95 staleness
- CPU total não piora >10% (idealmente melhora)
- Sem backlog crescente nas filas internas

**Trades (real-time nos dois):**
- 100% dos trades pareados por `trade_id` **dentro de janelas sem reconnect**
- ≥99.99% pareados no agregado do run completo, com lacunas explicáveis por reconnect/gap documentado
- `delta_ms` médio favorável ou neutro ao SBE (não exigir mínimos rígidos — ambos são real-time)

**Estratégia (shadow XEMM opcional):**
- Decisões de cancel/replace baseadas em SBE chegam = ou antes do JSON
- Menos quotes baseadas em fair price stale
- Nenhuma ordem real na fase de validação

NO-GO em qualquer falha do bloco "Decoder" ou "Market data" → investigar antes de plugar no controller.

### Duração — staged (5 etapas)

1. **10 min smoke** — BTC-USDT apenas. Confirma handshake, parse, 0 exceptions.
2. **30 min shadow** — BTC-USDT + USDT-BRL + BTC-BRL. Confirma multipair.
3. **2h em mercado normal** — janela calma (madrugada UTC). Estabilidade base.
4. **2h em horário volátil** — abertura de NY (13:30 UTC) ou anúncio macro. Stress.
5. **24h soak** — valida reconnect próximo ao TTL de 24h da Binance. **Crítico** — se reconnect falhar, market data desaparece.

Cada etapa deve passar todos os critérios GO acima antes de avançar pra próxima.

## Rollout no bot real (DEPOIS do GO) — **3 fases conservadoras**

### Fase 1 — `signal_connector` apenas

1. Editar `conf/controllers/xemm_lead_lag_btc_brl.yml`:
   - `signal_connector: binance` → `signal_connector: binance_sbe`
   - **`taker_connector: binance` permanece intocado** — execução continua via JSON.
2. Configurar `binance_sbe.yml` em `conf/connectors/` via `connect binance_sbe` no CLI HB. Insere apenas `binance_sbe_api_key` (string Ed25519).
3. Restart do bot via `bash start_xemm_lead_lag.sh Senha123`.

**Observação primeiros 5 min:**
- `grep -i "binance_sbe\|sbe_decoder\|SBE" logs/logs_conf_xemm_lead_lag_shadow.log` — esperar `Subscribed...` + 0 exceptions
- Quote drift: comparar `best_bid` **reconstruído** pelo `OrderBookTracker` do `binance_sbe` (vem dos diffs aplicados em cima do snapshot REST) vs `wss://stream.binance.com:9443/ws/btcusdt@bookTicker` JSON externo (wscat manual). Idênticos ou SBE adiantado. **Não comparar com stream `@bestBidAsk`** porque Fase 1 do decoder não implementa bestBidAsk.
- Métricas do XEMM: P50 da latência signal→action cai conforme medido no shadow.

**Observação ≥7 dias** antes de considerar Fase 2:
- 0 reconnect spurious
- 0 ghost-fill por timestamp errado
- Gaps de depth não aumentaram vs baseline JSON anterior

### Fase 2 — `taker_connector` (opcional)

Só se houver motivo concreto. SBE não cobre trading (orders continuam REST `api.binance.com/...`). Único valor: centralizar config Binance num único nome ou reduzir DNS-lookups. Provavelmente não vale.

Se decidir avançar:
1. Adicionar HMAC keys (`binance_api_key`, `binance_api_secret`) ao `binance_sbe.yml`.
2. Editar YAML: `taker_connector: binance` → `taker_connector: binance_sbe`.
3. Restart. Observar primeiros 30 min — orders criadas/canceladas com sucesso, REST latency comparável.

### Revert (qualquer fase)

1. Editar YAML de volta (1 linha).
2. Restart. Total: <30s.
3. **Não remover** o conector novo no revert — manter instalado para próxima tentativa após fix.

## Ordem de implementação (caminho crítico)

0. **Pre-flight (não escreve código):** baixar `stream_1_0.xml` do repo binance-spot-api-docs. Verificar e fixar constantes `SBE_SCHEMA_ID`, `SBE_SCHEMA_VERSION`, e templateIds 10000 (trade) / 10003 (depth diff). Confirmar offsets dos campos `eventTime`, `priceMantissa`, `priceExponent`, `qtyMantissa`, `qtyExponent`, `firstUpdateId`, `lastUpdateId`, `numInGroup` em depth e trades.
1. `binance_sbe_constants.py` + `binance_sbe_utils.py` — esqueleto, auto-discovery passa via sentinela import
2. **GATE 1**: `sbe_decoder.py` + `test_sbe_decoder.py` com fixtures golden → tudo verde antes de continuar. Inclui obrigatoriamente o teste de multi-trade frame e o de conversão μs→ms.
3. `binance_sbe_api_order_book_data_source.py` + tests (com override de subscribe/unsubscribe + reconnect proativo)
4. `binance_sbe_exchange.py` + tests
5. Smoke integration test (live, manual) com API key Ed25519
6. `tools/binance_sbe_shadow.py` + `binance_sbe_analyze.py`
7. **GATE 2**: rodar shadow staged (10min/30min/2h calm/2h volatile/24h soak), validar critério GO em cada etapa
8. Cutover Fase 1 (signal_connector apenas) + observação ≥7 dias
9. Cutover Fase 2 (taker_connector, opcional) só se houver motivo concreto

## O que NÃO entra neste plano

- **Sem mudança no conector `binance` original** — zero risco de regressão.
- **Sem tocar XEMM controller** ou executors — cutover é puro YAML.
- **Sem flag global em `bitpreco_constants.py` ou similar** — modelo é conector irmão, não env var.
- **User stream em SBE fora de escopo** — Binance oferece SBE para user data streams (informação corrigida vs versão anterior do plano), mas Fase 1 mantém user stream JSON+HMAC herdado. Migração futura possível mas não necessária pro ganho de latência alvo (signal/depth).
- **`@bestBidAsk` (templateId 10001) e `@depth20` (10002) ficam para Fase 2 do decoder** — XEMM atual não consome BBO separado nem precisa de top-20 snapshot. REST snapshot de 1000 níveis (JSON) continua sendo fonte de snapshot pro `OrderBookTracker`.
- **Sem fallback automático para JSON em runtime** — se SBE quebrar, o XEMM eleva exception e para (mesma política dos outros conectores). Revert é manual via YAML.

## Riscos e mitigações

| # | Risco | Probabilidade | Impacto | Mitigação |
|---|---|---|---|---|
| 1 | Schema SBE muda silenciosamente (Binance bumpa schemaId ou version não-aditiva) | Média | Alto — parse retorna dados inválidos, sinal corrompido | Decoder valida `header.schemaId` (fail-stop em mismatch) e trata `version` aditivamente (warning + parse compatível) vs não-aditivo (fail-stop). Integration test `test_live_schema_version_matches` em cron diário detecta antes de afetar prod. |
| 2 | API key Ed25519 inválida no boot — SBE rejeita handshake (401/403) | Média (1ª vez) | Médio — bot sem market data, no-op | `_connected_websocket_assistant` valida presença antes de conectar e loga erro acionável. Health check: 0 frames em 10s → ERROR + halt. |
| 3 | Decoder bug em depth diff (offsets/blockLength) → bids/asks trocados ou preços errados | Baixa após golden tests | **Crítico** | Golden fixtures cobrindo 0 / 1 / 10+ níveis. Validação cruzada no shadow run compara top-of-book SBE vs JSON em janelas de 100ms — divergência >1 tick = NO-GO. |
| 4 | Frame drop assimétrico (SBE perde mensagens, ex: backpressure) | Baixa | Alto — book diverge real | Métrica gaps por sequência `U/u` no shadow. Em prod, `OrderBookTrackerDataSource` herdado já detecta gap em `update_id` e força resnapshot REST. Contador de gaps logado a cada 1min. |
| 5 | **Timestamp em microssegundos não convertido** — `BinanceOrderBook.trade_message_from_exchange` multiplica por 1e-3 assumindo millis, gerando timestamps 1000× maiores | Alta sem mitigação | **Crítico** | Decoder DEVE dividir `event_time_us // 1000` ao popular `E`. Golden test `test_decode_trade_timestamp_converted_us_to_ms` é o canário. Sem conversão, hedge no XEMM ficaria com timestamp absurdo, possivelmente quebrando lógica de staleness. |
| 6 | **Frame com múltiplos trades silenciosamente perde trades** — se decoder retornar `dict` em vez de `list`, só o primeiro trade do repeating group é processado | Alta sem mitigação | Médio — perda de fills no signal, mas detecção difícil | Decoder retorna `list[dict]`. Golden test `test_decode_multi_trade_golden_fixture` com >2 trades no mesmo frame é gate obrigatório. |
| 7 | Conexão TTL de 24h sem reconnect proativo | Alta após 24h | Alto — market data para | Reconnect proativo a cada ~23h dentro de `listen_for_subscriptions` override. Validado no 24h soak test (etapa 5). |
| 8 | Rate limit do REST snapshot estoura se shadow + bot real correrem no mesmo IP | Baixa | Médio — book sem dados temporariamente | Throttlers Python são independentes por conector, mas IP weight pool é compartilhado. Mitigação no shadow: reduzir pairs para 1-2 OU usar IP distinto via VPN/proxy. |
| 9 | **Confundir `groupSizeEncoding` (uint32) com `groupSize16Encoding` (uint16)** no decoder. Depth usa 16-bit; trade usa 32-bit. Tamanho errado de `numInGroup` desalinha offsets em CADA frame e corrompe book. | Média se não atentado | **Crítico** | Decoder tem 2 helpers explícitos (`_read_group_header_size16`, `_read_group_header_size32`). Schema XML é fonte de verdade — verificar `dimensionType` de cada grupo antes de codar. Golden tests com >5 níveis pegam imediatamente se offsets desalinharem. |
| 10 | Reconnect proativo introduz bug de estado (queue duplicada, subscribe órfão, race no SIGTERM) | Média | Médio — bot perde frames durante reconnect, possível crash | Override de `listen_for_subscriptions` cuidadoso com cleanup; teste `test_reconnect_on_artificial_ttl_short_window` com TTL=30s exercita o ciclo completo. 24h soak captura interação real com TTL Binance. |

## Arquivos críticos lidos/referenciados

- `hummingbot/connector/exchange/binance/binance_exchange.py:144-149` — `_create_order_book_data_source` (override point)
- `hummingbot/connector/exchange/binance/binance_order_book.py:32-71` — shape de eventos exigida (`U`, `u`, `b`, `a`, `E`, `m`, `t`, `p`, `q`)
- `hummingbot/connector/exchange/binance/binance_api_order_book_data_source.py` — base class a herdar
- `hummingbot/connector/exchange/binance/binance_constants.py` — REST URLs, rate limits, paths (re-exportados)
- `hummingbot/connector/exchange/binance/binance_utils.py:44-67` — `BinanceConfigMap` (modelo do nosso ConfigMap)
- `hummingbot/client/settings.py:233-308` — auto-discovery mechanism (zero patch necessário)
- `hummingbot/core/web_assistant/connections/data_types.py:164` — `WSBinaryRequest` (suporte binário já no framework)
- `hummingbot/core/web_assistant/connections/ws_connection.py:141,146` — `_send_binary`, `_build_resp` (trata WSMsgType.BINARY)
- `test/hummingbot/connector/exchange/binance/test_binance_api_order_book_data_source.py` — padrão a adaptar
- Binance docs: https://developers.binance.com/docs/binance-spot-api-docs/sbe-market-data-streams
- Schema: https://github.com/binance/binance-spot-api-docs/blob/master/sbe/schemas/stream_1_0.xml

## Verification end-to-end

Após implementação, validar nesta ordem:

1. **Schema verificado:** `stream_1_0.xml` lido e constantes `SBE_SCHEMA_ID`, `SBE_SCHEMA_VERSION`, templateIds confirmados batendo com o XML oficial.
2. **Unit tests verde:** `conda run -n hummingbot python -m pytest test/hummingbot/connector/exchange/binance_sbe/ -v` — todos passam, decoder + data source + exchange + utils. **Multi-trade test e timestamp μs→ms test são gate obrigatório.**
3. **Regression intacta:** `pytest test/hummingbot/connector/exchange/binance/ -v` — todos passam (zero file touched).
4. **Auto-discovery funciona:** `python -c "from hummingbot.client.settings import AllConnectorSettings; AllConnectorSettings.create_connector_settings(); s = AllConnectorSettings.get_connector_settings(); assert 'binance_sbe' in s and 'binance' in s; print('OK')"`.
5. **Integration smoke (live, manual):** `pytest test/hummingbot/connector/exchange/binance_sbe/integration/ -m live --sbe-api-key=$SBE_API_KEY` — conecta no SBE real, recebe 10 frames sem erro, valida `header.schemaId`/`version` em runtime.
6. **Shadow staged (5 etapas):**
   - 10min smoke: `python tools/binance_sbe_shadow.py --duration 600 --pairs BTC-USDT`
   - 30min multipair: `--duration 1800 --pairs BTC-USDT,USDT-BRL,BTC-BRL`
   - 2h calm UTC: `--duration 7200` (madrugada)
   - 2h volatile: `--duration 7200` (13:30 UTC)
   - 24h soak: `--duration 86400` (validar reconnect TTL)
   
   Após cada etapa: `python tools/binance_sbe_analyze.py var/sbe_shadow/*.csv` — gera summary.json, validar contra critérios revisados (decoder + market data + trades + estratégia).
7. **Cutover live Fase 1 + 5min observação:** flip `signal_connector: binance_sbe`, restart bot, `grep -i sbe logs/logs_conf_xemm_lead_lag_shadow.log` — zero exceptions, frames chegando, quote bate com book externo.
8. **Observação ≥7 dias** antes de considerar Fase 2.
9. **Revert testado em dry-run:** editar YAML de volta, restart, confirmar volta ao `binance` JSON em <30s.
