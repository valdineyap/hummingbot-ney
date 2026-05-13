# BINANCE_SBE — status & retomada

Tracker da implementação do conector `binance_sbe` e da validação Phase 1.
Lido junto com [`binance_sbe_implementation_plan.md`](binance_sbe_implementation_plan.md) (decisões de design).

Última atualização: 2026-05-13.

---

## TL;DR onde estamos

- **Código completo:** decoder + data source + exchange + tools + tests (66/66 passam).
- **Validação shadow:** ✅ 60s smoke, ✅ 10min multipair, ✅ 2h calm — todos verdes
  com SBE p50 ~46ms à frente do JSON, 97-98% das observações.
- **Launcher SBE isolado pronto** (`start_xemm_lead_lag_sbe.sh`) — separado do
  launcher legado pra não comprometer outro projeto que usa o original.
- **Próximo passo concreto:** parar o bot legado e rodar o `start_xemm_lead_lag_sbe.sh`
  pra iniciar a observação de Phase 1.

---

## Validação shadow — resultados consolidados

Comparação JSON vs SBE rodando lado a lado (script `tools/binance_sbe_shadow.py`,
modo `both`). Janelas 100ms, buckets pareados.

| Métrica | 60s (1 pair) | 10min (3 pairs) | 2h (3 pairs) |
|---|---:|---:|---:|
| Paired buckets | 581 | 5,980 | 70,955 |
| JSON events | 671 | 6,432 | 77,358 |
| SBE events | 2,252 | 22,486 | 267,493 |
| SBE/JSON ratio | 3.36× | 3.50× | 3.46× |
| **Top-of-book** divergente (any) | 5 (0.86%) | 21 (0.35%) | 458 (0.65%) |
| Top-of-book divergência p50/p95/p99 | 0.0 / 0.0 / 0.0 | 0.0 / 0.0 / 0.0 | 0.0 / 0.0 / 0.0 |
| **delta_ms p50** (SBE first) | +45.7ms | +44.7ms | +47.6ms |
| **delta_ms p99** | +57.9ms | +66.6ms | +73.5ms |
| **delta_ms mean** | +39.4ms | +40.4ms | +45.7ms |
| **SBE arrived first** | 96.2% | 98.0% | 97.9% |
| CPU steady-state (host total) | ~17% | ~19-21% | ~31% (overnight, multipair) |
| Decoder errors | 0 | 0 | 0 |
| Schema mismatches | 0 | 0 | 0 |
| Exceptions / crashes | 0 | 0 | 0 |

Critérios GO/NO-GO do plano (`binance_sbe_implementation_plan.md`, seção
"Critério GO / NO-GO revisado"): **todos atendidos com folga** em todas as três
janelas. Gains observados (~46ms) excedem os mínimos por ordem de magnitude.

Artefatos: `var/sbe_shadow/*.csv` + `*.summary.json` (não commitados — runtime).

---

## O que está testado em offline (unit/integration)

- `pytest test/hummingbot/connector/exchange/binance_sbe/` — **66/66 passam**:
  - **23** decoder (mantissa→string, multi-trade, μs→ms, dynamic exponent, 0/1/many depth levels, schemaId mismatch fail-stop, additive version bump compat, truncated frame, unknown templateId warns once)
  - **21** order-book data source (`@depth` sem `@100ms`, X-MBX-APIKEY header,
    empty key raises, multi-trade enqueue, malformed frame não mata stream,
    schemaId mismatch propaga vs truncated swallow, reconnect TTL artificial 30s)
  - **14** exchange (name por domain, factory retorna SBE variant,
    trading_required sem HMAC raises, **env var fallback wins from .env, kwarg
    wins over env var, no key anywhere raises**, herança)
  - **8** utils (auto-discovery, ConfigMap shape, KEYS exportado)
- `pytest test/hummingbot/connector/exchange/binance/` — **98/98 passam** (zero
  regressão no conector JSON original).

---

## O que está testado em live (sem trading)

- ✅ Handshake SBE: `X-MBX-APIKEY: <ed25519-api-key-string>` aceito,
  zero 401/403 ao longo de 2h+10min+60s contínuos.
- ✅ Subscribe `<sym>@trade` + `<sym>@depth` (sem `@100ms`) — Binance
  retorna ACK JSON `{"id":1,"result":null}`, frames binários começam a fluir.
- ✅ Decoder em multipair (BTC-USDT, USDT-BRL, BTC-BRL) sem desalinhamento de
  offset.
- ✅ JSON acks (de SUBSCRIBE) co-existem com frames binários no mesmo socket
  e são despachados corretamente (`isinstance(data, bytes)` vs `isinstance(data, dict)`).
- ✅ Top-of-book reconstruído via diffs SBE bate com o que o JSON entrega na
  mesma janela de 100ms (divergência <1% das janelas, max 21.62 — irrelevante
  no contexto de XEMM em BTC ~80k).

---

## O que NÃO está testado ainda

- ⚠️ **Reconnect proativo de ~23h** (`WS_RECONNECT_INTERVAL_SEC = 23*3600`):
  testado com TTL artificial de 30s no unit test, mas a primeira execução
  contra Binance só vai acontecer 23h após a Phase 1 ligar. Risco residual:
  estado interno duplicado em reconnect, queue órfã, ou subscribe redundante.
- ⚠️ **Schema bump da Binance:** se a Binance mudar `schemaId` ou subir
  `version` de forma não-aditiva sem aviso, o decoder fail-stops corretamente
  (validado em test), mas nunca exercitado em prod.
- ⚠️ **Comportamento em janela volátil de mercado** (NY open, anúncios macro):
  shadow rodou em janela calma e overnight. Picos de book churn em volatilidade
  podem expor edge cases que não vimos.
- ⚠️ **Ghost-fill detection com `E_us` truncado:** XEMM usa `E` (ms) — se algum
  código downstream consumir `E_us` (micros) sem awareness, pode quebrar.
  Nenhum consumer atual usa `E_us`, mas é uma pegadinha latente.

A observação de Phase 1 (≥7 dias) cobre todos os 4 itens acima por exposição.

---

## Estado dos arquivos (snapshot)

### Tracked (commitados no repo, branch `claude/xemm-leadlag`)

```
hummingbot/connector/exchange/binance_sbe/
  __init__.py
  binance_sbe_constants.py
  binance_sbe_utils.py
  sbe_decoder.py
  binance_sbe_api_order_book_data_source.py
  binance_sbe_exchange.py                 (modificado: env var fallback)
test/hummingbot/connector/exchange/binance_sbe/
  __init__.py
  test_sbe_decoder.py
  test_binance_sbe_api_order_book_data_source.py
  test_binance_sbe_exchange.py            (modificado: testes do fallback)
  test_binance_sbe_utils.py
tools/
  binance_sbe_shadow.py
  binance_sbe_analyze.py
  binance_sbe_register.py                 (novo)
start_xemm_lead_lag_sbe.sh                (novo, separado do legado)
binance_sbe_implementation_plan.md        (plano vivo, atualizado)
BINANCE_SBE_STATUS.md                     (este arquivo)
.env.example                              (template, sem secrets)
```

### Local-only (gitignored)

```
.env                                      (BINANCE_SBE_API_KEY)
conf/connectors/binance_sbe.yml           (escrito pelo binance_sbe_register.py)
conf/scripts/conf_xemm_lead_lag_sbe.yml   (aponta pro controller SBE)
conf/controllers/xemm_lead_lag_btc_brl_sbe.yml  (cópia do legado com signal_connector: binance_sbe)
var/sbe_shadow/*.csv, *.summary.json      (output das shadow runs)
logs/logs_conf_xemm_lead_lag_sbe*.log     (gerado no cutover live)
```

### Intocados (legado, para outro projeto continuar usando)

```
start_xemm_lead_lag.sh
conf/scripts/conf_xemm_lead_lag_shadow.yml
conf/controllers/xemm_lead_lag_btc_brl.yml   (signal_connector: binance — restaurado)
```

---

## Como retomar (do zero, em outra sessão)

### Pré-requisitos no host
- `.env` com `BINANCE_SBE_API_KEY=<api-key-string>` (já existe, perms 600)
- `conf/connectors/binance_sbe.yml` criado via:
  ```bash
  python tools/binance_sbe_register.py Senha123
  ```
  (idempotente, pode rodar de novo)
- `conf/scripts/conf_xemm_lead_lag_sbe.yml` e `conf/controllers/xemm_lead_lag_btc_brl_sbe.yml`
  já existem locais (gitignored). Se precisar recriar, copiar do legado e
  trocar `signal_connector: binance` → `binance_sbe` no controller.

### Confirmar testes verdes
```bash
conda run -n hummingbot python -m pytest \
  test/hummingbot/connector/exchange/binance_sbe/ \
  test/hummingbot/connector/exchange/binance/
# 196 passed expected
```

### Rodar nova shadow validation (se desejar antes do cutover)
```bash
# Smoke 1min
python tools/binance_sbe_shadow.py --duration 60 --pairs BTC-USDT
python tools/binance_sbe_analyze.py var/sbe_shadow/*.csv

# Multipair 10min
python tools/binance_sbe_shadow.py --duration 600 --pairs BTC-USDT USDT-BRL BTC-BRL

# Long-haul 2h
python tools/binance_sbe_shadow.py --duration 7200 --pairs BTC-USDT USDT-BRL BTC-BRL
```

### Phase 1 cutover (próximo passo agora)

**O cutover requer parar o bot legado primeiro** — os dois compartilham a
session WS user stream da BitPreco e não rodam em paralelo.

```bash
# Parar o legado (se estiver rodando)
touch /tmp/xemm_lead_lag_pause
# Aguardar ~12s pra ele cancelar tudo e sair
pgrep -f conf_xemm_lead_lag_shadow    # repetir até retornar vazio

# Subir SBE
bash start_xemm_lead_lag_sbe.sh Senha123
```

### Voltar pro legado (revert)
```bash
touch /tmp/xemm_lead_lag_sbe_pause
pgrep -f conf_xemm_lead_lag_sbe       # repetir até retornar vazio
bash start_xemm_lead_lag.sh Senha123
```

---

## Observação Phase 1 — duração e critérios

Após cutover, **≥7 dias** de observação contínua antes de declarar Phase 1
estável. Critérios objetivos (verificáveis no log):

### Curto prazo (primeiros 5 min)
- `grep "binance_sbe\|SBE\|sbe_decoder" logs/logs_conf_xemm_lead_lag_sbe.log`
  retorna `Subscribed to SBE public order book and trade channels...`
- `grep -E "ERROR|CRITICAL|SchemaMismatch|Traceback"` retorna vazio
- Primeira ordem maker criada (`Created maker order`)
- `best_bid` reconstruído (via diffs SBE) bate com `wss://stream.binance.com:9443/ws/btcusdt@bookTicker`
  via wscat externo, em janela de 100ms

### Médio prazo (24-30 horas)
- **Reconnect proativo** (a ~23h após o boot) fira limpo: `[binance_sbe] proactive
  reconnect after Xs` no log, seguido de `Subscribed to SBE...` novamente
  em <5s. Zero crash, zero queue órfã.
- Sem aumento mensurável de `[ghost_guard]` ou `[ghost_controller]` events vs
  baseline JSON.

### Longo prazo (7 dias)
- Zero reconnect espúrio (só os ~23h planejados)
- Gaps de depth (por sequência `U/u`) ≤ baseline JSON
- Resnapshot count ≤ baseline JSON
- Latência signal→action (XEMM internal metric) ≤ pré-cutover

---

## Decisões após Phase 1 estável (caminhos)

Quando os 7 dias passarem verde, três caminhos:

### Caminho A — Consolidar (recomendado)
Encerrar o ciclo. `binance_sbe` vira o novo baseline pro signal. Bot legado
fica aposentado. Sem trabalho adicional. Ganho consolidado: +45ms p50 no caminho
de signal.

### Caminho B — Phase 2 (cosmético, não recomendado)
Apontar `taker_connector` também pra `binance_sbe`. Sem ganho de latência
(trading continua REST/HMAC). Só centralização de config. Trabalho:
- Editar `conf/controllers/xemm_lead_lag_btc_brl_sbe.yml`: `taker_connector: binance_sbe`
- Adicionar HMAC keys ao `conf/connectors/binance_sbe.yml` (re-rodar
  `binance_sbe_register.py` com kwargs novos OU rodar `connect binance_sbe`
  interativo)
- Restart

### Caminho C — Phase futura: `binance_ws_trading` (projeto novo)
Endpoint diferente (`wss://ws-api.binance.com:443/ws-api/v3`), schema
diferente (`spot_3_0.xml`), auth diferente (Ed25519 signing). Briefing
completo na seção "Fase futura" do `binance_sbe_implementation_plan.md`.
Vale o esforço só se medições mostrarem que **envio de orders** é o gargalo
material.

---

## Tabela de comandos úteis (cola direta)

| Ação | Comando |
|---|---|
| Verificar bot SBE vivo | `pgrep -fa conf_xemm_lead_lag_sbe \| grep -v grep` |
| Verificar bot legado vivo | `pgrep -fa conf_xemm_lead_lag_shadow \| grep -v grep` |
| Pausar bot SBE | `touch /tmp/xemm_lead_lag_sbe_pause` |
| Pausar bot legado | `touch /tmp/xemm_lead_lag_pause` |
| Tail log SBE filtrado | `tail -F logs/logs_conf_xemm_lead_lag_sbe.log \| grep -E "binance_sbe\|SBE\|Created maker\|ERROR\|CRITICAL"` |
| Rotacionar key Ed25519 | `nano .env  # editar, depois:`<br>`python tools/binance_sbe_register.py Senha123` |
| Rodar shadow 30min | `python tools/binance_sbe_shadow.py --duration 1800 --pairs BTC-USDT USDT-BRL BTC-BRL` |
| Analisar última shadow | `python tools/binance_sbe_analyze.py $(ls -t var/sbe_shadow/*.csv \| head -1)` |
| Verificar key carregada do .env | `conda run -n hummingbot python -c "import os; from pathlib import Path; exec(open('tools/binance_sbe_shadow.py').read().split('try:')[0]); print('len:', len(os.environ.get('BINANCE_SBE_API_KEY','')))"` |
