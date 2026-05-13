# Plano: Conector `binance_sbe` — Binance Spot via Simple Binary Encoding

## Context

O bot XEMM Lead-Lag (BTC-BRL, branch `claude/xemm-leadlag`) consome market data da Binance via WebSocket JSON em `wss://stream.binance.com:9443/ws`, usando o conector `binance` para signal (BTC-USDT) e taker (BTC-BRL). A Binance publicou **SBE Market Data Streams** em `wss://stream-sbe.binance.com:9443/ws`, com payload binário menor e parse mais rápido.

**Escopo desta implementação (Fase 1):** apenas market data público via SBE. Trading continua REST/HTTPS+HMAC herdado do conector `binance` original. **Importante distinção que motivou várias revisões deste plano:** a Binance Spot oferece três caminhos distintos relacionados a SBE/WebSocket que NÃO devem ser confundidos:

1. **SBE Market Data WS** (`wss://stream-sbe.binance.com:9443/ws/...`) — binário, schema `stream_1_0.xml`. **É o que esta Fase 1 implementa.**
2. **WebSocket API Trading** (`wss://ws-api.binance.com:443/ws-api/v3`) — JSON nas requests, com **opcional** `?responseFormat=sbe` para receber respostas em binário (schema `spot_3_0.xml` contém só os response templates). **Não está nesta Fase 1.** Seria um projeto separado, capturado como "Fase futura" no fim deste plano.
3. **REST trading** (`api.binance.com/api/v3/order` etc.) — JSON, HMAC. É o que `binance_sbe` herda inalterado de `BinanceExchange` e continua usando.

Não existe "trading SBE" como protocolo coeso na Binance Spot — SBE é só um encoding opcional de respostas no WS Trading API. Misturar esses conceitos foi uma confusão em iterações anteriores do plano; agora explicitada acima.

O objetivo desta Fase 1 é adicionar um conector **irmão** chamado `binance_sbe` que herda 100% do `binance` atual e sobrescreve **apenas** o `OrderBookTrackerDataSource` para consumir o stream SBE. Zero modificações ao conector `binance` existente — o XEMM ativo hoje fica intocado até cutover via 1 linha de YAML. Conector novo entrega potencial de redução de latência e jitter no caminho de sinal/taker, validado em script standalone antes de plugar no bot real.

Decisões registradas:
- **Conector irmão**, não flag — Opção B do framework simples/robusto. Permite rodar `binance` e `binance_sbe` simultâneos para validação AB.
- **Full trading capability via herança REST/JSON** (custo zero de código): `binance_sbe` herda o trading atual do `BinanceExchange`. Envio/cancelamento de ordens continuam via REST `api.binance.com` com HMAC, exatamente como no conector `binance`. Rollout live começa por `signal_connector` apenas. Taker fica em `binance` JSON até N dias de estabilidade do signal.
- **Trading via WebSocket API existe na Binance Spot, mas é JSON e usa endpoint separado** (`wss://ws-api.binance.com:443/ws-api/v3`) — **não é SBE**. SBE é apenas um formato opcional de resposta (`?responseFormat=sbe`), as requests são JSON. WS trading é um projeto separado (`binance_ws_trading`), fora do escopo desta entrega. Ver seção "Fase futura" no fim deste plano.
- **Usar `binance_sbe` como `taker_connector` nesta fase não reduz latência de execução**, pois ordens continuam REST. Única razão seria centralização operacional de config — provavelmente não vale. Mantém-se `taker_connector: binance` até existir projeto próprio de WS trading.
- **Multipair genérico** — implementar como o conector original (qualquer pair que a Binance Spot suporte em SBE).
- **Ed25519 já provisionada** pelo usuário — config recebe apenas a **API key string** Ed25519 (não PEM). Header `X-MBX-APIKEY` consome a string direto, sem signing (market data público).
- **Decoder primeiro como gate** — `sbe_decoder.py` + golden fixtures de bytes verdes ANTES de qualquer outro arquivo.
- **Validação standalone obrigatória** em `tools/binance_sbe_shadow.py` antes de cutover no bot.
- **Escopo Fase 1 do decoder:** apenas `@trade` (templateId 10000) e `@depth` diff (templateId 10003). `@bestBidAsk` (10001) e `@depth20` (10002) ficam para Fase 2 se aparecer caso de uso — XEMM atual não consome BBO separadamente nem precisa de top-20 snapshot (snapshot REST de 1000 níveis basta).

## Referência SBE (Binance Spot, oficial)

### Schemas SBE publicados pela Binance Spot

A Binance publica vários schemas SBE em `binance-spot-api-docs/sbe/schemas/`, cobrindo superfícies distintas:

| Schema | Onde aparece | O que é | Escopo desta entrega |
|---|---|---|---|
| `stream_1_0.xml` | `wss://stream-sbe.binance.com:9443/ws/...` — frames de notificação push | **Protocolo de market data SBE.** Trade, depth, bestBidAsk, depth20. Binário tanto na request (subscribe é JSON, mas conteúdo é binário) quanto nas respostas. | **Fase 1 ✅** |
| `spot_3_0.xml` | `wss://ws-api.binance.com:443/ws-api/v3?responseFormat=sbe` — respostas opcionais em binário | **Encoding alternativo de respostas do WS Trading API**, que continua sendo JSON nas requests. Contém apenas `*Response` templates (NewOrderAckResponse, CancelOrderResponse, ExchangeInfoResponse, etc.). | Fora desta entrega — projeto futuro de WS trading pode usar |
| `spot-fixsbe-1_*.xml` | FIX gateway | Trading via FIX | Fora de escopo (sem caso de uso aqui) |

**Insight crítico para futuras implementações:** `spot_3_0.xml` **não habilita trading binário** — só economiza bytes nas respostas. Requests continuam JSON. Se um dia migrarmos trading pro WS API, podemos optar por response format SBE pra parsing mais rápido, mas isso é uma decisão de optimização ortogonal ao "usar WS-API trading".

### Detalhes do `stream_1_0.xml` (Fase 1)

- **Endpoint:** `wss://stream-sbe.binance.com:9443/ws/<streamName>` (single) ou `/stream?streams=<a>/<b>` (multi)
- **Streams:** `<sym>@trade`, `<sym>@bestBidAsk`, `<sym>@depth` (25ms diff), `<sym>@depth20` (50ms top-20 snapshot)
- **Schema XML:** `stream_1_0.xml` — `schemaId=1, version=0`. **A primeira tarefa do dev é verificar isso no XML antes de codar e ajustar constantes.**
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
    # HMAC fields kept optional. In Phase 1 trading goes via REST/HTTPS+HMAC
    # inherited from BinanceExchange — using binance_sbe as taker gives the
    # same trading-side latency as binance. (SBE *does* support trading via
    # a different endpoint — see Phase 3 — but that path needs Ed25519
    # signing and is out of scope here.)
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

### 6. `binance_sbe_exchange.py` (~95 linhas, atualizado)

`class BinanceSbeExchange(BinanceExchange)`:

- `@property name → "binance_sbe"`
- `__init__`: aceita `binance_sbe_api_key: str = ""` (default vazio para permitir fallback), armazena, repassa para o data source na factory. Recebe HMAC (`binance_api_key`/`binance_api_secret`) como `Optional` — só usados se trading via este conector estiver habilitado.
- **Trava explícita de boot (HMAC obrigatório quando trading_required):**
  ```python
  if self._trading_required and (not binance_api_key or not binance_api_secret):
      raise ValueError(
          "binance_sbe requires HMAC binance_api_key + binance_api_secret "
          "when trading_required=True. For signal-only use, pass trading_required=False."
      )
  ```
- **Fallback para env var quando o kwarg vier vazio:** se Hummingbot's `Security.api_keys("binance_sbe")` não retornar nada e o framework passar `binance_sbe_api_key=""`, o construtor consulta `os.environ["BINANCE_SBE_API_KEY"]` (carregado do `.env` pelo start script). Permite deployments sem o fluxo interativo `connect binance_sbe`. Falha loud se nenhum dos dois caminhos forneceu key:
  ```python
  if not binance_sbe_api_key:
      binance_sbe_api_key = os.environ.get("BINANCE_SBE_API_KEY", "")
  if not binance_sbe_api_key:
      raise ValueError("provide via `connect binance_sbe` OR via BINANCE_SBE_API_KEY env var")
  ```
- Em Fase 1 (signal_connector), o framework instancia com `trading_required=False`. Se alguém configurar como taker sem HMAC, falha no boot com erro acionável — não silencioso no primeiro `buy()`.
- `_create_order_book_data_source()`: retorna `BinanceSbeAPIOrderBookDataSource(..., sbe_api_key=self._sbe_api_key)`.
- Todo o resto (auth HMAC, trading, user stream, fees, rate limits, REST) **herdado sem mudança**.

### 7. `tools/binance_sbe_register.py` (~110 linhas, helper headless)

Embora o conector tenha fallback env var, o `ConnectorManager` do Hummingbot ainda exige uma entrada em `Security.api_keys("binance_sbe")` para passar o gate de "API keys required for live trading connector" (`hummingbot/core/connector_manager.py:85`). Por isso é necessário ter `conf/connectors/binance_sbe.yml` (encrypted) com a key.

Este helper escreve esse arquivo programaticamente, sem precisar do TTY do CLI interativo `connect binance_sbe`:

```bash
python tools/binance_sbe_register.py <master-password>
```

Lê `BINANCE_SBE_API_KEY` do `.env`, monta um `BinanceSbeConfigMap`, e chama `Security.update_secure_config()`. Idempotente — pode rodar de novo pra rotação de key (edita `.env`, roda o helper, restart do bot).

### 8. `start_xemm_lead_lag_sbe.sh` (launcher isolado do legado)

Clone do `start_xemm_lead_lag.sh` com identificadores trocados para não conflitar com a versão antiga (que outro projeto pode estar usando em paralelo):

| Recurso | Legado | SBE |
|---|---|---|
| Script config | `conf_xemm_lead_lag_shadow.yml` | `conf_xemm_lead_lag_sbe.yml` |
| Controller config | `xemm_lead_lag_btc_brl.yml` | `xemm_lead_lag_btc_brl_sbe.yml` |
| Log file | `logs_conf_xemm_lead_lag_shadow.log` | `logs_conf_xemm_lead_lag_sbe.log` |
| Kill switch | `/tmp/xemm_lead_lag_pause` | `/tmp/xemm_lead_lag_sbe_pause` |
| pgrep pattern | `conf_xemm_lead_lag_shadow` | `conf_xemm_lead_lag_sbe` |

Pgrep patterns são mutuamente exclusivos — nenhum dos scripts mata processos do outro. **Mas os dois bots NÃO podem rodar simultâneos** porque compartilham:

- WS user stream da BitPreco (limite 1 session/conta)
- Ordens maker abertas em `bitpreco BTC-BRL`
- `all_orders_cancel` global na conta BitPreco no precleanup

Operacional: pausar o antigo antes de subir o SBE (`touch /tmp/xemm_lead_lag_pause`; aguardar ~12s; subir SBE).

**Total novo:** ~640 linhas (530 do código + 110 do registro). **Reusado por herança:** ~1200 linhas do binance original.

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

### Fase 2 — `taker_connector` apontando para `binance_sbe` (opcional, sem ganho de latência)

**Importante:** *nesta fase* trading continua via REST `api.binance.com/...` (HMAC herdado de `BinanceExchange`). Não há ganho de latência no caminho de trading. Único valor: centralizar a config Binance sob um único nome (`binance_sbe` faz signal + taker), evitar DNS-lookups extras. Provavelmente não vale o esforço operacional — recomenda-se pular direto pra Fase 3 se quiser ganho real em trading.

Se decidir avançar:
1. Adicionar HMAC keys (`binance_api_key`, `binance_api_secret`) ao `binance_sbe.yml`.
2. Editar YAML: `taker_connector: binance` → `taker_connector: binance_sbe`.
3. Restart. Observar primeiros 30 min — orders criadas/canceladas com sucesso, REST latency comparável.

### Fase futura — Binance Spot WebSocket API trading (projeto separado, NÃO nesta entrega)

**Não confundir com SBE.** O que existe é trading via WebSocket API, JSON nas requests, com SBE opcional só na resposta. Captura abaixo para não esquecermos o conhecimento. Recomendado virar um projeto novo (`binance_ws_trading` ou `binance_ws_api`) depois do `binance_sbe` estar estável ≥7 dias live, NÃO um Phase do PR atual — superfície de risco já está saturada com decoder + order book + reconnect + shadow validation.

**Resumo do que é diferente do que `binance_sbe` herda hoje:**

| | REST trading (herdado em Fase 1) | WS-API trading (projeto futuro) |
|---|---|---|
| Endpoint | `https://api.binance.com/api/v3/order` etc. | `wss://ws-api.binance.com:443/ws-api/v3?returnRateLimits=false` |
| Formato request | JSON sobre HTTPS, params query | JSON envelope `{"id":..., "method":"order.place", "params":{...}}` |
| Formato response | JSON sobre HTTPS | JSON ou (opcional) SBE binário com `responseFormat=sbe` (schema `spot_3_0.xml`) |
| Auth | HMAC: query param `signature=<hmac(secret, payload)>` + `timestamp` | Per-request `signature` + `timestamp` em `params`. Suporta HMAC, RSA OU Ed25519 (à escolha conforme tipo da key). |
| Auth alternativa | n/a | `session.logon` — autentica a sessão WS uma vez; **só funciona com chaves Ed25519** e ainda exige `timestamp` em requests assinados |
| Métodos relevantes | `POST /order`, `DELETE /order`, `POST /order/cancelReplace` | `order.place`, `order.cancel`, `order.cancelReplace`, `order.test`, `account.rateLimits.orders` |
| Rate limits | `ORDERS` + `REQUEST_WEIGHT` (compartilhados com REST) | Mesmos limites do REST — não há "ganho de quota" por usar WS |

**Ganho potencial real:** redução de latência de envio/cancelamento (não precisa estabelecer TCP+TLS por request — sessão WS já está aberta). Cada ms economizado em order placement vira slippage evitada em market making. Magnitude esperada ~5-30ms dependendo do RTT geográfico.

**Riscos críticos que tornam isso um projeto separado, não Phase do PR atual:**

1. **`order.cancelReplace` pode falhar parcialmente** — cancelar OK, recriar fail; ou vice-versa. Estado do bot fica ambíguo entre WS response e user stream update.
2. **Reconciliação de estado** — não dar por finalizado um order baseado SÓ na resposta WS síncrona. Sempre confirmar via user stream / order updates. Risco de double-fill se confiar na resposta e disparar hedge sem confirmação.
3. **Idempotência** — usar `newClientOrderId` consistente para que retries em timeout não criem duas ordens.
4. **Auth chain** — se decidirmos por Ed25519 + `session.logon`, precisamos gerar/proteger PEM privada e implementar signing. Se ficarmos com HMAC per-request, é mais simples mas perde a otimização de session.

**O que precisa para implementar (briefing futuro):**

- Novo módulo `binance_ws_trading_auth.py` com signing (HMAC ou Ed25519 conforme escolha)
- Novo data source para o WS API (gerenciar request/response IDs, timeouts, retry)
- Override de `_place_order` / `_place_cancel` no exchange para rotear pro WS quando habilitado, com fallback REST automático em erro de conexão
- Métodos a habilitar (em ordem): `order.test` (smoke) → `order.cancel` → `order.place` → `order.cancelReplace` (com cuidado adicional)
- Config novo separado:
  ```python
  binance_ws_trading_api_key: Optional[SecretStr]            # API key string
  binance_ws_trading_secret: Optional[SecretStr]             # HMAC secret OU
  binance_ws_trading_ed25519_private_key_path: Optional[str] # Ed25519 PEM path (alternativo)
  ```
- Sanity: manter REST como fallback manual via flag, pra revert sem rebuild
- Considerar `newOrderRespType=ACK` (vs default `RESULT`/`FULL`) para minimizar payload de respostas em ordens normais; usar `FULL` só quando precisar do trade detail síncrono

**Critérios de benchmark (rodar contra REST como baseline antes de virar default):**

- `local_send_ts → Binance transactTime` (medida server-side)
- `local_send_ts → response_recv_ts` (latência RTT completa)
- Cancel latency (`cancel_send → cancel_ack`)
- CancelReplace latency (e taxa de partial-failure)
- Throughput sustentado (orders/segundo) sob rate limits
- Validar idempotência via reenvio de `order.test` com mesmo `newClientOrderId`

**Quando faz sentido perseguir:** somente após (1) `binance_sbe` market data estável ≥7 dias live, (2) medições no shadow XEMM mostrando que **latência de execução** (não de signal) é o gargalo material, (3) volume de orders/min justifica o esforço de testes adicional. Caso contrário, REST+HMAC é mais simples e robusto.

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
9. Cutover Fase 2 (taker_connector, opcional) só se houver motivo concreto — **sem ganho de latência**, só centralização de config.

**Fora desta sequência (projeto separado, briefing capturado em "Fase futura"):**

10. `binance_ws_trading` — WebSocket API trading via JSON requests (com SBE opcional nas responses). Não confundir com SBE market data. Avaliar somente se latência REST de orders for medida como gargalo material no shadow XEMM, e somente após `binance_sbe` estável ≥7 dias live.

## O que NÃO entra neste plano

- **Sem mudança no conector `binance` original** — zero risco de regressão.
- **Sem tocar XEMM controller** ou executors — cutover é puro YAML.
- **Sem flag global em `bitpreco_constants.py` ou similar** — modelo é conector irmão, não env var.
- **User stream em SBE fora de escopo** — quando a session WS-API conecta com `responseFormat=sbe`, eventos de user stream chegam em binary. Migração possível só se/quando o projeto separado de WS-API trading for implementado.
- **WebSocket API trading fora de escopo desta entrega** — existe na Binance Spot mas é JSON (com SBE opcional só nas responses). Endpoint diferente (`wss://ws-api.binance.com:443/ws-api/v3`), auth diferente (signing per-request com HMAC/RSA/Ed25519, ou Ed25519 `session.logon`), critérios de validação diferentes (latência de execução, partial-failure de `order.cancelReplace`, idempotência via `newClientOrderId`). Deve virar projeto separado (`binance_ws_trading`) depois de `binance_sbe` estável. Briefing capturado na seção "Fase futura".
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
