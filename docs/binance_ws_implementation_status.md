# Binance WS Trading — Status de Implementação

**Última atualização:** 2026-05-13
**Branch ativa:** `claude/xemm-leadlag`
**Plano canônico (source of truth):**
`/home/ubuntu/.claude/plans/prefiro-que-cancele-todas-partitioned-willow.md` (1232 linhas, refinado após 3 revisões + decisão de credenciais)

> **AVISO:** o arquivo `binance_ws_trading_implementation_plan.md` no repo está **desatualizado** (369 linhas, anterior à 3ª revisão e à decisão Caminho 2a). Use o plano canônico acima até este doc ser sincronizado.

## Decisões já tomadas

| Decisão | Resumo |
|---|---|
| **Tipo de signature (Fases 1-2)** | HMAC, reusa chave do `binance.yml` de produção. Ed25519 só seria necessária para Fase 3 (`session.logon`), fora de escopo. |
| **Caminho de credenciais para benchmark/live tests** | **Caminho 2a** — `.env` plaintext com **as mesmas chaves de produção**, carregado por `python-dotenv` no `conftest.py` do diretório `integration/`. `.env` em `.gitignore`, `chmod 600`, rotação obrigatória pós-cutover. Pré-commit hook bloqueia commit acidental. |
| **Arquitetura** | Override mínimo via herança: `BinanceWsExchange(BinanceExchange)` + `BinanceWsTradingMixin`. Reusa auth, user stream, market data, tracker do conector REST. |
| **Escopo Fase 1** | `order.place` + `order.cancel` via WS. `order.cancelReplace` fica para Fase 2. `session.logon` Ed25519 para Fase 3 (ambas opcionais). |
| **Solução para race UNKNOWN** | `asyncio.shield(wait_for)` + late response handler + estados `CREATED_NOT_SENT` / `SENT` / `LATE_PENDING` para discriminar segurança de retry. |
| **Rate limit strategy** | Throttler com pesos detalhados por método (não agrupado): conexão=2, place=1+ORDERS, status=4, etc. |

## Estado do código

| Componente | Arquivo | Linhas | Status |
|---|---|---|---|
| Init | `hummingbot/connector/exchange/binance_ws/__init__.py` | 0 | ✅ |
| Constants | `binance_ws_constants.py` | 128 | ✅ auditado |
| Utils | `binance_ws_utils.py` | 150 | ⚠️ auditado — falta `account.status`/`account.commission` rate limits |
| Auth | `binance_ws_auth.py` | 67 | ✅ existe + 7 testes passando |
| Request Router | `binance_ws_request_router.py` | 645 | ⚠️ auditado — gap em FAILED-state self-recovery; cancel terminal-state a confirmar |
| Trading Mixin | `binance_ws_trading_mixin.py` | 291 | ✅ auditado — 24 testes passando, todos BLOCKERs J/K/L OK |
| Exchange | `binance_ws_exchange.py` | 138 | ✅ auditado — 16 testes passando |
| Register Tool | `tools/binance_ws_register.py` | 109 | ✅ existe |
| Benchmark Tool | `tools/binance_ws_benchmark.py` | 318 | ⚠️ auditado — gap em PERCENT_PRICE math + `TEST_ORDER_PATH_URL` hardcoded |
| **Total** | | **2.798** | **71/71 unit tests verdes** |

## Critical path (seção 8 do plano)

| # | Step | Status |
|---|---|---|
| 0 | Pre-flight wscat manual (smoke `ping`) | ❌ Não feito |
| 1 | Constants + utils skeleton | ✅ |
| 2 | **GATE 1** — Auth + golden HMAC vectors | ✅ |
| 3 | **GATE 2** — Router + mock WS tests | ✅ |
| 4 | Exchange + smoke compostos | ✅ |
| 5 | Register tool | ✅ |
| 6 | Integration smoke live (`test_live_order_test_no_submit`) | ❌ falta conftest com python-dotenv + `.env` setup |
| 7 | **GATE 3** — Benchmark REST vs WS (janela calma + volátil) | ❌ |
| 8 | Cutover Fase 1 (`taker_connector: binance` → `binance_ws`) | ❌ |
| 9 | Observação ≥7 dias | ❌ |
| 10 | Rotação de chaves de produção (post-go-live checklist) | ❌ |

## BLOCKERs da 3ª revisão — auditoria contra código existente

**Audit completo: 2026-05-13** (3 Agents paralelos: router; mixin/exchange; constants/utils/tools)

| ID | Item | Status | Localização / Gap |
|---|---|---|---|
| F | Resquícios de `-2026` | ✅ | Único `2026` em `mixin.py:62` é comentário esclarecendo que é `ORDER_ARCHIVED` (não duplicate). Detecção real: `_DUPLICATE_ORDER_CODES = (-1010, -2010, -2011)` (L64) + `"Duplicate order sent"` (L65). |
| G | `build_rate_limits()` com pesos por método | ⚠️ **GAP MENOR** | `utils.py:107-150` tem CONNECT=2, PING=1, ORDER_TEST=1, ORDER_TEST_COMMISSION=20, ORDER_PLACE=1+ORDERS, ORDER_CANCEL=1, ORDER_STATUS=4. Mas `ACCOUNT_RATE_LIMITS=40` (L149) é para `account.rateLimits.orders`; faltam `account.status=20` e `account.commission=20` como limit IDs separados. |
| H | `send_public()` separado de `send_signed()` | ✅ | Router L279-287 (`send_public`) e L289-299 (`send_signed`) distintos, pipeline comum em `_send`. |
| I | UNKNOWN states (`CREATED_NOT_SENT`/`SENT`/`LATE_PENDING`) | ✅ | Enum `PendingState` L134-137. Transições: L344 (→SENT após `send_json`), L362-363 (→LATE_PENDING após timeout). `asyncio.shield(future)` L358. Drain L606-622 distingue corretamente em disconnect. |
| J | `newOrderRespType="ACK"` em `_place_order` | ✅ | `mixin.py:129` com comentário L126-128 explicando. |
| K | `exchange_symbol_associated_to_pair()` em overrides | ✅ | `_place_order` L115; `_place_cancel` L160-162. Outros métodos recebem `symbol` já convertido dos callers. |
| L | Quantização + serialização string | ✅ | `quantize_order_amount/price` L113-114; `f"{value:f}"` em L124 (quantity), L133 (LIMIT price), L137 (LIMIT_MAKER price). Comentário L121-123 reconhece o motivo do raw string para signing. |
| M | Benchmark respeita `PERCENT_PRICE` + `MIN_NOTIONAL` + `MIN_PRICE` | ⚠️ **GAP REAL** | `benchmark.py:187-192` lê `PERCENT_PRICE_BY_SIDE`/`PERCENT_PRICE` e aplica `bidMultiplierDown`, mas usa `best_bid` como `avg_price` (L191 "close enough"). Binance calcula contra `/api/v3/avgPrice` (média 5min) — em mercado volátil pode ficar fora dos bounds. `MIN_PRICE` (`PRICE_FILTER.minPrice`) NÃO é checado — só `tickSize`. |
| N | Dispatcher trata `id=None` | ✅ | `_handle_text` L478-496: `if msg_id is None:` antes de qualquer lookup. Discrimina `serverShutdown`, `error` connection-level, fallback log. |
| O | Router FAILED state + slow-retry | ⚠️ **GAP MENOR** | `_reconnect` L547-604: backoff exponencial L579-582, jitter L583, janela `WS_RECONNECT_WINDOW_SEC` L556-559, threshold L561 → `state=FAILED` L568, log `CRITICAL` L562-567. **MAS** L573 sai do loop com um único `wait_for` + `return`; `_proactive_reconnect_loop` L624-645 só agenda se `state==CONNECTED` L636. Em FAILED, **nada relança `_reconnect` periodicamente** — fica preso até `start()` ser chamado externamente. |
| **Cancel UNKNOWN terminal** | Tratar `FILLED`/`EXPIRED`/`REJECTED`/`EXPIRED_IN_MATCH` sem retry | ⚠️ **GAP A CONFIRMAR** | Não está no router (response sai intacta para caller). Possível que esteja no mixin/exchange wrapper, **não auditado**. |
| **`computeCommissionRates=False`** | Benchmark A | ✅ | `benchmark.py:132` `"false"` explícito em `params_base`. |
| **`returnRateLimits` antes da signature** | Router signing | ✅ | `sign_request_params` L296-298 recebe `params` já populado; ordem alfabética depende de `binance_ws_auth` (não auditado mas testes auth verdes). |
| **`BinanceWsResponse` dataclass** | Router | ✅ | L114-127 com `result`, `rate_limits`, `status`, `error`, `rtt_ms`. Preenchido em `_dispatch_response` L513-516. |
| **`autoping=True`, `heartbeat=None`** | Router connect | ✅ | L421-425. |
| **`asyncio.shield` no `wait_for`** | Router | ✅ | L358 + `_expire_late_pending` L395-401 garbage-collect. |
| **`TEST_ORDER_PATH_URL` constante** | Benchmark A | ❌ **GAP** | `benchmark.py:140` hardcoda string `"https://api.binance.com/api/v3/order/test"`. Deveria reusar de `binance_constants.py` REST. |
| **`python-dotenv` no benchmark** | Tool | ⚠️ | Usa `_load_env_file()` artesanal L62-73 chamado em L313, não `python-dotenv` canônico. Funcionalmente OK. Aceitável; docstring não menciona `.env`. |

### Resumo dos gaps reais (a corrigir antes do Step 6)

1. **BLOCKER O — FAILED state sem self-recovery**: router entra em FAILED corretamente, mas nada o tira de lá sem chamada externa a `start()`. Em produção isso vira bot zumbi — exatamente o que BLOCKER O pretendia evitar. **Fix**: adicionar slow-retry loop (~1/min) que tenta `_reconnect` enquanto `state == FAILED`. Se reconnect tiver sucesso, sai do FAILED.

2. **BLOCKER G — Falta `account.status` e `account.commission` rate limits**: throttler subdimensionado se conector chamar esses métodos. Não bloqueia Fase 1 (não usamos esses), mas trivial de adicionar e fica correto para Fase 2/3.

3. **BLOCKER M — Benchmark price math**: usa `best_bid` em vez de `/api/v3/avgPrice`, e não respeita `MIN_PRICE`. Em mercado calmo provavelmente passa; em volátil, ordens são rejeitadas com filter error e benchmark falha confusamente. **Fix**: consultar `avgPrice` endpoint + clamp final em `max(lower_bound, minPrice)`.

4. **Cancel UNKNOWN terminal-state**: a confirmar se está em mixin/exchange ou se realmente está ausente. Se ausente, é gap: cancel de ordem que já foi `FILLED` retorna OK mas o status terminal é informativo, não retry-able.

5. **`TEST_ORDER_PATH_URL` constante**: trivial, extrair de URL hardcoded.

### Gaps fora de escopo (não bloqueantes para Fase 1)

- `python-dotenv` vs `_load_env_file()` artesanal: funcionalmente equivalente. Deixar como está.
- `WSS_API_TRADING_URL_TESTNET` sem `.format(domain)`: inconsistência cosmética; testnet não está em uso.

## Próximos passos (em ordem)

1. **AUDIT BLOCKERs F-O** — 3 Agents paralelos por área (constants/utils/tools; router; mixin/exchange). Resultado: lista de gaps reais.
2. **Aplicar fixes** apenas onde houver gap.
3. **Re-rodar 71 unit tests** + qualquer novo teste para os fixes.
4. **Sincronizar plano** — copiar a versão refinada de `.claude/plans/` para `binance_ws_trading_implementation_plan.md` no repo, ou consolidar e descartar a duplicação.
5. **Setup integration env** — criar `conftest.py` em `test/hummingbot/connector/exchange/binance_ws/integration/`, criar `.env` com chaves de produção (`chmod 600`), validar `.gitignore`.
6. **Step 0** — pre-flight wscat manual.
7. **Step 6** — `pytest -m live test_live_order_test_no_submit`. Depois `test_live_connect_and_ping_pong` (30s). Depois `test_live_order_place_and_cancel_far_from_book_dynamic_size`.
8. **Step 7 — GATE 3** — rodar `binance_ws_benchmark.py` A em janela calma (N=1000), B em janela calma (N≥50). Validar critério GO/NO-GO da seção 6.
9. **Step 8** — cutover Fase 1 do controller. Restart bot. Monitor 5min + 7 dias.
10. **Post-go-live** — checklist de rotação de chaves de produção (§7 do plano).

## Convenções deste doc

- Atualizar este doc **antes** de qualquer mudança não-trivial.
- `🔍 AUDIT` → não verificado. `✅` → verde verificado. `❌` → não feito ainda. `⚠️` → conhecido com gap.
- Quando aplicar um fix de BLOCKER, mudar a linha para `✅ <commit-sha>` com referência ao commit.
- Quando uma decisão for tomada/revogada, anotar em "Decisões já tomadas" com data.
