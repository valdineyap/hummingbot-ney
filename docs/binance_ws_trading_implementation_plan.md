# Conector `binance_ws` — Binance Spot Trading via WebSocket API

> Plano de implementação dedicado. Sibling do
> [`binance_sbe_implementation_plan.md`](binance_sbe_implementation_plan.md).
> O bot XEMM Lead-Lag (BTC-BRL, branch `claude/xemm-leadlag`) hoje envia
> e cancela ordens na Binance Spot via REST. Este conector substitui esse
> caminho por WebSocket-API trading, mantendo todo o restante (market
> data, user stream, auth, fees, rate limits) inalterado.

## 1. Contexto

A Binance publica WebSocket-API trading em
`wss://ws-api.binance.com:443/ws-api/v3` com envelope JSON correlacionado
por `id`. Comparado ao REST trading (`POST /api/v3/order`), o ganho é
latência de send/cancel: a sessão TCP+TLS já está aberta, então cada
operação economiza handshake + HTTP request/response parsing. Magnitude
esperada **5-30ms por operação** dependendo de RTT geográfico. Material
em market making sob volatilidade.

**Escopo da v1:** substituir apenas `_place_order` e `_place_cancel` por
WS-API. Tudo o mais — user stream, market data, REST polling de status,
auth HMAC, fees, rate limits, tracker — vem herdado de `BinanceExchange`.

**Distinção ortogonal entre SBE e WS trading:**

| Otimização | Endpoint | Ganho | Conector |
|---|---|---|---|
| Market data binário | `wss://stream-sbe.binance.com:9443/ws` | Parse + bytes menores | `binance_sbe` (já entregue) |
| Trading via WS persistente | `wss://ws-api.binance.com:443/ws-api/v3` | Latência send/cancel | `binance_ws` (este plano) |
| REST trading legado | `https://api.binance.com/api/v3/order` | Baseline | `binance` |

A combinação SBE+WS (`BinanceSbeWsExchange`) fica como projeto futuro de
~30 linhas após v1 estável — viável por composição via mixin.

### Decisões registradas

- **Conector irmão**, não flag global — mesmo padrão do `binance_sbe`.
- **HMAC per-request na v1** — reusa o HMAC que já está em
  `conf/connectors/binance.yml`. `session.logon` Ed25519 é ortogonal,
  adiado para Fase 3 se benchmark medir economia material.
- **Fallback REST manual via flag de config** (`use_ws_trading: bool = True`,
  também sobrescrevível via env var `BINANCE_WS_USE_WS_TRADING`).
  Operador desliga + restart → REST puro. Sem fallback automático em
  runtime.
- **`order.cancelReplace` adiado para Fase 2** desta entrega. v1 cobre
  `order.place`, `order.cancel`, `order.test`.
- **Override via mixin** (`BinanceWsTradingMixin`) ao invés de embutido
  no `BinanceWsExchange`. Permite `BinanceSbeWsExchange` trivial:
  `class BinanceSbeWsExchange(BinanceWsTradingMixin, BinanceSbeExchange)`.
- **`AsyncThrottler` compartilhado obrigatório.** O router NÃO bypassa
  o throttler — todo `send_signed` passa por
  `self._throttler.execute_task(limit_id=...)`. WS e REST contam na
  mesma quota IP da Binance.
- **Override mínimo de superfície:** somente `_place_order`,
  `_place_cancel` e `check_network`. Auto-discovery via diretório +
  `KEYS` no `binance_ws_utils.py`. Zero patch em arquivos
  compartilhados.

## 2. Referência Binance Spot WS-API

### Endpoints

- Prod: `wss://ws-api.binance.com:443/ws-api/v3`
- Testnet: `wss://ws-api.testnet.binance.vision/ws-api/v3`
- Query params: `returnRateLimits=false` (recomendado em prod para
  economizar bytes; sample periódico com `true` para acompanhar uso).

### Lifecycle

- TTL máximo 24h. Server emite evento `serverShutdown` ~10min antes.
- Server-initiated ping a cada 20s. Cliente responde pong em <60s ou é
  derrubado. `aiohttp.ClientSession.ws_connect(autoping=True)` trata
  isso automaticamente.
- Reconnect proativo antes de 24h previne janela sem trading.

### Request envelope

```json
{"id": "<string>", "method": "<string>", "params": {...}}
```

- `id` arbitrário (UUID4). Server ecoa em cada resposta.
- Responses chegam **fora de ordem**. Dispatch por `id`.
- Métodos `SIGNED` exigem em `params`: `apiKey`, `timestamp`,
  `signature`; opcional `recvWindow`.

### Auth HMAC-SHA256 (diferente do REST)

- Params ordenados **alfabeticamente**.
- String a assinar: `k1=v1&k2=v2&...` SEM URL-encoding.
- HMAC-SHA256 com `secret_key`, output em hex lowercase.

`BinanceAuth.generate_ws_signature(params)` já existe em
`hummingbot/connector/exchange/binance/binance_auth.py:60-72`. Reuso
direto.

### Códigos de erro relevantes

- `-1007` Timeout server-side → **execução desconhecida**. Mesmo
  tratamento de timeout local.
- `-1021` timestamp out of recv window → re-sincronizar e retry uma vez.
- `-2010` insufficient balance → propagar.
- `-2011` order does not exist → cancel retorna False, sem exceção.
- **Duplicate clientOrderId** — NÃO é `-2026` (esse é `ORDER_ARCHIVED`).
  Detector correto:
  ```python
  err.code in {-1010, -2010, -2011} and "Duplicate order sent" in err.msg
  ```
- **Status ≥500** → execução desconhecida. Binance pode aceitar a
  ordem mesmo respondendo 5xx.

### Rate limits

Compartilhados com REST (`ORDERS`, `REQUEST_WEIGHT`). Sem ganho de
quota, só de latência. Conexão WS conta peso 2.

## 3. Arquitetura — override mínimo via mixin

Override points validados em `binance_exchange.py`:

- `_place_order` (linhas 171-211) → retorna `Tuple[str, float]`.
- `_place_cancel` (linhas 213-225) → retorna `bool`.
- Caller upstream `ExchangePyBase._place_order_and_process_update`
  (linha 470) processa o retorno via `_order_tracker`. **Não tocar.**
- User stream pós-place vem 100% de `_user_stream_event_listener`
  (linhas 289-353). **Intacto** se mudar só place/cancel.
- Auto-discovery via `hummingbot/client/settings.py:233-308` detecta
  `binance_ws` pelo diretório + `KEYS`.

## 4. Arquivos entregues

Diretório novo: `hummingbot/connector/exchange/binance_ws/`

| Arquivo | Responsabilidade |
|---|---|
| `__init__.py` | marker vazio |
| `binance_ws_constants.py` | URLs, timeouts, intervals, limit-ids |
| `binance_ws_utils.py` | `BinanceWsConfigMap`, `KEYS`, `build_rate_limits()` |
| `binance_ws_auth.py` | função pura `sign_request_params(params, key, secret, time_provider)` |
| `binance_ws_request_router.py` | `BinanceWsRequestRouter` — socket persistente, dispatch por id, reconnect, late-response capture, FAILED state |
| `binance_ws_trading_mixin.py` | `BinanceWsTradingMixin` — override de `_place_order`, `_place_cancel`, reconciliação via REST |
| `binance_ws_exchange.py` | `BinanceWsExchange` — herda `BinanceWsTradingMixin` + `BinanceExchange` |

Ferramentas:

- `tools/binance_ws_register.py` — copia HMAC do `binance.yml` para
  `binance_ws.yml` encriptado (headless equivalent de `connect binance_ws`).
- `tools/binance_ws_benchmark.py` — REST vs WS-API benchmark com 2 modos
  (`--mode test` para latência de transporte via `order.test`,
  `--mode real` para lifecycle completo `place`+`cancel`).

### Detalhes do router (`binance_ws_request_router.py`)

1. **Conexão WS persistente** via
   `aiohttp.ClientSession.ws_connect(URL, autoping=True, heartbeat=None)`.
   Server-initiated ping a cada 20s; aiohttp responde sozinho.
2. **`_pending: Dict[str, _PendingEntry]`** com estados
   `CREATED_NOT_SENT` / `SENT` / `LATE_PENDING`. Estado determina
   exception em disconnect:
   - `CREATED_NOT_SENT` → `BinanceWsDisconnectedError` (seguro retry).
   - `SENT` → `BinanceWsUnknownExecutionError` (request pode ter chegado
     ao matching engine; caller trata como UNKNOWN).
3. **Dispatch task** lê frames, discrimina por shape:
   - frame com `id` → resolve future.
   - frame sem `id` mas com `event.e == "serverShutdown"` → reconnect
     graceful.
   - frame sem `id` mas com `error` top-level → log connection-level
     error.
   - outros → log warning.
4. **`asyncio.shield(future)` dentro de `wait_for`**:
   sem isso, timeout cancela o future e mata o handler de late-response.
   Pending é mantido por janela `WS_LATE_RESPONSE_WINDOW_SEC` após
   timeout para capturar response atrasada via log
   `[bws_late_response]`.
5. **Reconnect** com backoff exponencial + jitter
   (1s±0.2 → 30s±5). Stable >60s reseta contador. Se >10 reconnects em
   5min → estado `FAILED`: `check_network` retorna NOT_CONNECTED,
   router continua tentando 1×/min até voltar.
6. **Throttler integration**: todo `send_signed` passa por
   `async with self._throttler.execute_task(limit_id=...)`. Limit ids
   específicos (`WS_ORDER_PLACE`, etc.) com `linked_limits` apontando
   aos buckets `ORDERS` e `REQUEST_WEIGHT` compartilhados com REST.
7. **`BinanceWsResponse` dataclass** preserva `id`, `status`, `result`,
   `error`, `rate_limits`, `rtt_ms` — `rate_limits` seria perdido se
   retornássemos só `result`.

### Detalhes do mixin (`binance_ws_trading_mixin.py`)

**`_place_order`**:
- Conversão obrigatória `trading_pair → exchange_symbol` via
  `exchange_symbol_associated_to_pair`.
- Quantização (`quantize_order_amount` / `quantize_order_price`) +
  serialização Decimal como string `f"{v:f}"`. Crucial porque a
  assinatura WS é computada sobre a string raw — qualquer reformatação
  downstream quebra a signature.
- `LIMIT` → `timeInForce=GTC` + `price`.
- `LIMIT_MAKER` → `price`, sem `timeInForce` (Binance rejeita).
- `MARKET` → só `quantity`.
- `newOrderRespType="ACK"` para reduzir payload.
- UNKNOWN (timeout, 5xx, -1007, disconnect-while-sent) → retorna
  `("UNKNOWN", now)`. Framework reconcilia via REST polling.
- Duplicate (`-1010/-2010/-2011` com "Duplicate order sent") → consulta
  `order.status` via REST, retorna tuple existente. Idempotência
  preservada.

**`_place_cancel`**:
- Fast path por `orderId` numérico quando
  `tracked_order.exchange_order_id` é conhecido e `!= "UNKNOWN"`.
  `origClientOrderId` só como fallback. **Nunca ambos.**
- Mapeamento de `result.status`:
  - `CANCELED` → True.
  - `NEW` / `PARTIALLY_FILLED` → False.
  - Terminal (`FILLED`/`EXPIRED`/`REJECTED`/`EXPIRED_IN_MATCH`) → False
    + log `[bws_cancel_terminal_state]`.
- `-2011` (no such order) → False, sem exceção.
- UNKNOWN → consulta REST `GET /api/v3/order` antes de decidir; se REST
  também falhar, levanta `BinanceWsUnknownCancelError`. **Nunca retorna
  False cego.**

**`_ws_health_dict()`**: snapshot consumido pelo `check_network`
override e por dashboards externos.

## 5. Cobertura de testes

Diretório: `test/hummingbot/connector/exchange/binance_ws/`

- `test_binance_ws_auth.py` — 7 testes. Golden HMAC vectors, idempotência,
  ordering alfabético, sem URL-encoding.
- `test_binance_ws_utils.py` — 7 testes. Schema do ConfigMap (2
  SecretStr REQUIRED), `use_ws_trading` default True, peso de cada
  limit_id na tabela de rate-limits.
- `test_binance_ws_request_router.py` — 16 testes. Happy path,
  application error, status 5xx → UnknownExecution, code -1007 →
  UnknownExecution, timeout, late-response captura, dispatch shape
  (frames sem id, serverShutdown), SENT vs CREATED_NOT_SENT em
  disconnect, FAILED após reconnect storm, health snapshot.
- `test_binance_ws_trading_mixin.py` — 24 testes. Place params por
  order_type (LIMIT/LIMIT_MAKER/MARKET), UNKNOWN→`("UNKNOWN", now)`,
  duplicate → reconciliação via REST, cancel fast path orderId, cancel
  fallback origClientOrderId, never both, todos os status terminais,
  UNKNOWN → REST, fallback REST quando `use_ws_trading=False`,
  composição com `BinanceSbeExchange` (smoke do MRO para
  `BinanceSbeWsExchange` futuro).
- `test_binance_ws_exchange.py` — 16 testes. Nome `binance_ws` e
  `binance_ws_<domain>`, fail-fast em HMAC vazio quando
  `trading_required=True`, env-var override, rate_limits extends not
  replaces, `check_network` exige router conectado, data source
  inalterado.
- `test_binance_ws_auto_discovery.py` — sentinel: `binance`,
  `binance_sbe` e `binance_ws` aparecem juntos em
  `AllConnectorSettings`.

Total: 71 unit tests, zero regressão em `binance/` e `binance_sbe/`
(236 passes na suite combinada).

Testes de integração live (`--mark live`) ficam para execução manual
com `BINANCE_API_KEY`/`BINANCE_API_SECRET` exportadas; cobrem
ping/pong, `order.test`, place+cancel far-from-book com tamanhos
dinâmicos respeitando MIN_NOTIONAL / LOT_SIZE / PERCENT_PRICE, e
idempotência via `newClientOrderId` reusado.

## 6. Validação antes do cutover

`tools/binance_ws_benchmark.py` mede REST vs WS-API:

- **Modo `test`** — `order.test` (server não submete). Mede transporte +
  signing. Seguro sem custódia.
- **Modo `real`** — `order.place` far-from-book + cancel imediato.
  Calcula preço/qty dinamicamente respeitando filters (incl.
  PERCENT_PRICE).

Saída: CSV por request + summary JSON (p50, p90, p99, mean, max,
error_count).

GO criteria:

| Métrica | Limiar |
|---|---|
| p50 RTT WS (transport) | ≤ p50 REST − 5ms |
| p99 RTT WS (transport) | ≤ p99 REST − 10ms |
| p50 RTT WS (lifecycle) | ≤ p50 REST − 5ms |
| Timeout rate WS em 1h | < 0.1% |
| Idempotência | Zero double-submission |

## 7. Rollout

### Fase 1 — cutover `taker_connector` → `binance_ws`

1. Editar controller YAML (ex.
   `conf/controllers/xemm_lead_lag_btc_brl.yml`):
   `taker_connector: binance` → `taker_connector: binance_ws`.
2. `python tools/binance_ws_register.py <master-password>`.
3. Restart.

Observação primeiros 5min: `grep -i "bws_timing" logs/...log` deve
mostrar `[bws_timing] place rtt_ms=...` em cada ordem. Comparar com
baseline REST anterior.

Observação ≥7 dias antes de Fase 2: zero timeouts persistentes, zero
double-submission, latência confirmada melhor que REST.

### Fase 2 — `order.cancelReplace`

Pré-requisito: mapear se o executor XEMM tem hook estilo
`_place_cancel_and_replace` ou se chama cancel+place separadamente.
Modo `STOP_ON_FAILURE` por default. Reconciliação obrigatória via user
stream antes de declarar sucesso.

### Fase 3 — `session.logon` Ed25519 (opcional)

Apenas se Fase 1 mostrar signing como gargalo material e volume
justificar. Requer chave Ed25519 (HMAC/RSA não funcionam).

### Revert

`BINANCE_WS_USE_WS_TRADING=false` no `.env` + restart → REST puro em
<30s. OU editar controller YAML de volta para `taker_connector: binance`.
Não remover o conector `binance_ws` no revert.

## 8. Riscos materiais — resumo executivo

| # | Risco | Mitigação |
|---|---|---|
| 1 | `order.place` UNKNOWN → reenvio duplica fill | `BinanceWsUnknownExecutionError` distinto. `_place_order` retorna `("UNKNOWN", now)`; framework re-checa via REST. Duplicate detection via msg "Duplicate order sent" em codes `-1010/-2010/-2011`. |
| 2 | `order.cancel` UNKNOWN → estado ambíguo | `_place_cancel` consulta REST `GET /api/v3/order` antes de retornar False. Se REST falhar, levanta `BinanceWsUnknownCancelError`. |
| 3 | Router bypassa throttler → 429 silencioso | `linked_limits` em cada limit-id WS apontando aos buckets `ORDERS`/`REQUEST_WEIGHT` compartilhados. |
| 4 | Reconnect em momento crítico → perda de pending | Drain de pending com exception correta por estado (CREATED_NOT_SENT vs SENT). |
| 5 | `serverShutdown` força UNKNOWN desnecessário | Reconnect graceful: aproveita os 10min de aviso para drenar a conexão antiga e abrir a nova em paralelo. |
| 6 | Ping/pong não respondido → drop em <60s | `aiohttp.ws_connect(autoping=True)`. |
| 7 | Out-of-order responses + frames sem id | Dispatch discrimina por shape: `id` → response, `event.e == "serverShutdown"` → handler, outros → log. |
| 8 | TTL 24h sem reconnect proativo | Reconnect proativo a cada 23h via task em background. |
| 9 | `check_network` reporta CONNECTED com WS morto | Override exige `self._router.connected == True` quando `use_ws_trading`. |
| 10 | Reconnect loop infinito | Backoff exponencial. >10 em 5min → estado `FAILED` + retry a cada 1min até voltar. |
| 11 | Late response após UNKNOWN → bot já marcou ordem | Pending mantido por janela `WS_LATE_RESPONSE_WINDOW_SEC` após timeout; resposta tardia logada para reconciliação manual. |
| 12 | `LIMIT_MAKER` com `timeInForce` é rejeitado | Mixin omite `timeInForce` explicitamente para LIMIT_MAKER. |
| 13 | Decimal não serializa em JSON e quebra signature | `_place_order` quantiza + converte para string `f"{v:f}"` antes de assinar. |

## 9. Arquivos críticos referenciados

- `hummingbot/connector/exchange/binance/binance_exchange.py:171-225` —
  `_place_order` / `_place_cancel` (overridden no mixin).
- `hummingbot/connector/exchange/binance/binance_exchange.py:289-353` —
  user stream listener (intacto).
- `hummingbot/connector/exchange/binance/binance_auth.py:60-72` —
  `generate_ws_signature` (lógica reutilizada em
  `binance_ws_auth.sign_request_params`).
- `hummingbot/connector/exchange/binance/binance_constants.py` —
  re-exports + buckets compartilhados.
- `hummingbot/client/settings.py:233-308` — auto-discovery.
- `hummingbot/connector/exchange/binance_sbe/binance_sbe_exchange.py` —
  padrão de connector irmão.

## 10. Status de entrega

- [x] Pacote `binance_ws/` completo (constants, utils, auth, router,
      mixin, exchange).
- [x] 71 unit tests verde.
- [x] Auto-discovery passa: `binance`, `binance_sbe`, `binance_ws` os
      três presentes.
- [x] Regressão `binance/` + `binance_sbe/` intacta (236 testes verde).
- [x] `tools/binance_ws_register.py` headless.
- [x] `tools/binance_ws_benchmark.py` 2-modos.
- [ ] Smoke live (`order.test`, place+cancel) — requer credenciais
      live, manual.
- [ ] Benchmark live REST vs WS — manual antes de cutover.
- [ ] Cutover Fase 1 — após benchmark GO.
- [ ] Observação ≥7 dias.

Etapas restantes não dependem de mais código; são operacionais.
