# Operations & Debug Runbook — XEMM Lead-Lag

Foco: o que um agente (humano ou Claude) precisa para responder rápido a
"o bot está OK?" e investigar quando algo deu errado. Para arquitetura,
ver [`DEVELOPMENT_STATUS.md`](DEVELOPMENT_STATUS.md). Para observabilidade
de memória / hunt de leak / interpretação de `[mem]` e `[sbe_queue]`, ver
[`docs/MEMORY_OBSERVABILITY.md`](docs/MEMORY_OBSERVABILITY.md).

---

## 1. Quick start: "Algo aconteceu desde minha última checagem?"

### Atalho preferido — `monitor_digest.sh`

Um único comando devolve todo o digest processado (ideal para loops de
monitoramento de Claude e shells humanos):

```bash
bash tools/monitor_digest.sh
```

Imprime: status do processo, fills (lê `state.json` se houver),
freshness do master log, ERRORs recentes, anomalias de `cancel_retry`
(>= 3 retries no mesmo order_id), último `[orphan_check]` e últimos 2
eventos do executor.

Exit codes (use em loops):
| código | significado |
|---|---|
| 0 | saudável |
| 1 | fill detectado — analisar |
| 2 | bot caiu |
| 3 | master log travado (>120s sem writes) |

### Comandos manuais

Se precisar de algo específico, três verificações em ordem crescente de detalhe:

```bash
# (a) Beacon de filesystem — apenas mtime. Custo zero.
stat -c '%Y %y' logs/xemm_lead_lag/last_fill.touch 2>/dev/null \
  || echo "nenhum fill ainda nesta sessão"

# (b) Snapshot O(1) — todos os números agregados.
cat logs/xemm_lead_lag/state.json | jq

# (c) Último trade detalhado.
tail -1 logs/xemm_lead_lag/trades.jsonl | jq
```

Se você quer descobrir só os **trades novos** desde a última visita, use o
`seq` como cursor (incrementa monotonamente):

```bash
total=$(wc -l < logs/xemm_lead_lag/trades.jsonl)
seen=$(cat /tmp/.xemm_last_seq 2>/dev/null || echo 0)
tail -n $((total - seen)) logs/xemm_lead_lag/trades.jsonl | jq
echo "$total" > /tmp/.xemm_last_seq
```

---

## 2. Trade ledger — schema e cookbook

### Arquivos em `logs/xemm_lead_lag/`

| Arquivo | Conteúdo | Atualizado quando |
|---|---|---|
| `last_fill.touch` | vazio (mtime importa) | a cada fill |
| `state.json` | snapshot agregado | a cada fill (atomic rename) |
| `trades.jsonl` | append-only audit trail | a cada fill (com fsync) |
| `xemm_lead_lag_*.csv` | tick-by-tick (preços, lead, regime) | ~1 Hz |
| `../logs_conf_xemm_lead_lag_shadow.log` | log textual completo | streaming |

### Schema de uma linha em `trades.jsonl`

```json
{
  "ts": "2026-05-07T15:42:54Z",
  "seq": 1,
  "executor_id": "...",
  "side": "SELL",
  "trading_pair": "BTC-BRL",
  "maker_connector": "bitpreco",
  "taker_connector": "binance",
  "filled_amount_quote": "78.83",
  "net_pnl_quote": "-0.036",
  "cum_fees_quote": "0.039",
  "close_type": "COMPLETED",
  "close_timestamp": 1778168574.034,
  "quote_asset": "BRL"
}
```

Decimais ficam como string para preservar precisão. Use `tonumber` em `jq`.

### jq cookbook

```bash
# PnL total da sessão
jq -s '[.[].net_pnl_quote | tonumber] | add' logs/xemm_lead_lag/trades.jsonl

# Trades com PnL negativo (revisão)
jq 'select(.net_pnl_quote | tonumber < 0)' logs/xemm_lead_lag/trades.jsonl

# Contagem por close_type
jq -s 'group_by(.close_type) | map({type: .[0].close_type, count: length})' \
   logs/xemm_lead_lag/trades.jsonl

# Trades por hora (UTC)
jq -r '.ts[0:13]' logs/xemm_lead_lag/trades.jsonl | sort | uniq -c

# Total de fees pagos
jq -s '[.[].cum_fees_quote | tonumber] | add' logs/xemm_lead_lag/trades.jsonl

# Lado mais ativo (BUY vs SELL)
jq -r .side logs/xemm_lead_lag/trades.jsonl | sort | uniq -c
```

---

## 3. Ciclo de vida do bot

### Iniciar (caminho recomendado)

```bash
cd /home/ubuntu/hummingbot-ney
bash start_xemm_lead_lag.sh Senha123 > /tmp/start.log 2>&1 &
disown
```

`Senha123` é o **master password** do Hummingbot (decifra os conectores em
`conf/connectors/*.yml`). Mude se sua instalação usa outro.

O que o script faz, em ordem:
1. **Detecta instância em curso** (`pgrep -f conf_xemm_lead_lag_shadow`).
   Se existe: cria kill switch → espera até 12s → SIGTERM → SIGKILL se
   ainda vivo.
2. **Remove kill switch** (`rm -f /tmp/xemm_lead_lag_pause`).
3. **Rotaciona o log atual** para `..._<timestamp>.log`.
4. **Roda `tools/precleanup.py`** que cancela ordens órfãs em
   bitpreco+binance via `all_orders_cancel`. Exit codes:
   - `0` → ok, segue
   - `1` → erro de auth/config → ABORTA o boot
   - `2` → falha parcial → continua (startup_cleanup do bot retenta)
5. **Sobe o bot** em `--headless` com `conf_xemm_lead_lag_shadow.yml`.
6. **Aguarda até 60s pela primeira ordem maker** e imprime sucesso/erro.

> ⚠️ **`shadow_mode: false` no YAML = bot LIVE.** O nome do arquivo
> `conf_xemm_lead_lag_shadow.yml` é histórico — a flag real está dentro
> dele. Sempre conferir `grep "^shadow_mode:" conf/conf_xemm_lead_lag_shadow.yml`.

### Parar

**Graceful (preferido):**
```bash
./stop_xemm_bot.sh
# Cria /tmp/xemm_lead_lag_pause, espera até 30s pelo cancel das ordens,
# faz kill -9 se passar do timeout. Sempre remove o pause file no fim.
```

**Imediato (se travado):**
```bash
./stop_xemm_bot.sh --force
# kill -9 direto, sem esperar cancelamento. Pode deixar ordens órfãs —
# a próxima execução do precleanup vai limpar.
```

**Manual (kill switch sem o script):**
```bash
touch /tmp/xemm_lead_lag_pause
# bot detecta em ≤200ms, cancela ordens, sai sozinho em ~12s.
```

### Reiniciar limpo

```bash
./stop_xemm_bot.sh
bash start_xemm_lead_lag.sh Senha123 > /tmp/start.log 2>&1 &
disown
```
O `start_xemm_lead_lag.sh` já chama o stop interno se detectar instância
viva, mas usar o `stop_xemm_bot.sh` separado dá log mais claro do que está
parando.

### Mudar shadow ↔ live (sem hot-reload)

```bash
# 1. Parar
./stop_xemm_bot.sh

# 2. Editar o YAML
sed -i 's/^shadow_mode:.*/shadow_mode: false/' \
    conf/controllers/xemm_lead_lag_btc_brl.yml
# Ou abrir o arquivo e editar manualmente.

# 3. Subir
bash start_xemm_lead_lag.sh Senha123 > /tmp/start.log 2>&1 &
disown
```

Confira a flag aplicada no boot:
```bash
grep "shadow_mode" logs/logs_conf_xemm_lead_lag_shadow.log | head -3
```

### Está vivo?

```bash
pgrep -fa hummingbot_quickstart | grep -v grep
# Saída esperada: 2 PIDs (conda wrapper + python real)
```

Vivo mas sem trades?
```bash
# Quando foi a última vez que algo aconteceu (orphan_check ou ordem)
tail -1 logs/logs_conf_xemm_lead_lag_shadow.log
```

---

## 4. Logs — onde está o quê

### Arquivos em `logs/`

| Arquivo | Quem escreve | Conteúdo |
|---|---|---|
| `logs_conf_xemm_lead_lag_shadow.log` | bot principal | log textual de TUDO (eventos, erros, decisões, transições) — o "log mestre" |
| `logs_conf_xemm_lead_lag_shadow_<UTCts>.log` | start script | rotação do anterior (1 arquivo por restart) |
| `logs_precleanup_<UTCts>.log` | start script | log do `tools/precleanup.py` (1 por boot) |
| `logs_hummingbot.log` | hummingbot core | logs do framework (ínfimo, raramente útil) |

### Arquivos em `logs/xemm_lead_lag/`

| Arquivo | Quem escreve | Conteúdo |
|---|---|---|
| `xemm_lead_lag_<id>_<UTCts>.csv` | controller | tick-by-tick (~1 Hz): preços, lead bps, regime, prof bands, audit, etc. |
| `trades.jsonl` | trade ledger | append-only, 1 linha JSON por trade completo |
| `state.json` | trade ledger | snapshot agregado, atomic overwrite |
| `last_fill.touch` | trade ledger | beacon vazio, mtime = último fill |

### Outros

- `/tmp/start.log` — stdout do `start_xemm_lead_lag.sh` quando rodado em background. Útil para ver se o "Waiting for first order" terminou OK.
- O **terminal interactivo** (sem `--headless`) escreve no mesmo log mestre — é uma alternativa para quando você quer ver o status panel.

### Tail em tempo real

```bash
# Log mestre completo
tail -f logs/logs_conf_xemm_lead_lag_shadow.log

# Filtrado — só o que importa para entender se está saudável
tail -f logs/logs_conf_xemm_lead_lag_shadow.log | \
  grep -E "Created maker|cancel|orphan|FILLED|hedge|ERROR|cancel_retry|regime"

# Só erros e warnings
tail -f logs/logs_conf_xemm_lead_lag_shadow.log | grep -E "ERROR|WARNING|CRITICAL"

# Heartbeat do reconciler (a cada 10s — confirma que o loop está vivo)
tail -f logs/logs_conf_xemm_lead_lag_shadow.log | grep "orphan_check"

# CSV em tempo real (1 linha/seg)
tail -f logs/xemm_lead_lag/$(ls -t logs/xemm_lead_lag/*.csv | head -1)
```

### CSV — colunas mais úteis

O CSV é largo (~50 colunas). Para descobrir o índice de uma:
```bash
head -1 logs/xemm_lead_lag/xemm_lead_lag_*.csv | tr ',' '\n' | grep -n -i "regime\|lead\|prof"
```

Colunas-chave (cite o cabeçalho exato com `head -1`):
- `regime` — OK / DEGRADED_FX / DEGRADED_LEADER / DEGRADED_LOCAL / BAD / KILLED
- `lead_5s`, `lead_10s`, `lead_15s` — sinal lead-lag em bps por janela
- `fair_brl_fast`, `fair_brl_slow` — preço justo BRL (sem/com EMA)
- `local_mid`, `local_bid`, `local_ask` — book do maker (BitPreco)
- `taker_buy_px`, `taker_sell_px` — preço resultante no taker (Binance)
- `prof_buy_bps`, `prof_sell_bps` — profitabilidade NET por lado, em bps
- `audit_btc_actual`, `audit_btc_target`, `audit_btc_delta` — inventory drift
- `boot_paused` — 1 enquanto não passou do startup_cleanup + audit inicial

### Logs antigos / forensics

Para investigar incidente passado (não no log mestre atual):
```bash
ls -lt logs/logs_conf_xemm_lead_lag_shadow_*.log | head -10
# Pega o mais próximo do timestamp do incidente.

# CSVs antigos (stats finos)
ls -lt logs/xemm_lead_lag/*.csv | head -10
```

---

## 5. Troubleshooting comum

### "Bot está rodando mas não cria ordens"

Em ordem de probabilidade:
1. **Kill switch ativo:** `ls /tmp/xemm_lead_lag_pause` — se existe, remova.
2. **Boot-paused mode:** procure no log `[boot] startup_cleanup OK + audit OK
   — leaving boot_paused mode`. Se NÃO apareceu, o audit detectou drift no
   boot — investigar `[audit]` linhas no log.
3. **Regime degradado:** procure `regime=DEGRADED|PAUSED|KILLED` no log ou
   no CSV mais recente.
4. **Profitabilidade fora da banda:** as ordens estão sendo cancelas tão
   rápido que parece que não cria — `grep "profitability.*Cancelling" log`.
5. **Saldo insuficiente:** `grep "INSUFFICIENT_BALANCE\|Not enough budget" log`.

### "Apareceram ordens órfãs na exchange"

O reconciler periódico deve cancelar em ≤13s. Verificar:
```bash
grep "orphan_check" logs/logs_conf_xemm_lead_lag_shadow.log | tail -10
```
Se `orphans=N` com N>0, foi detectado. Se a mesma ordem aparece como órfã
duas rodadas seguidas, o cancel está falhando — investigar a chamada da
API BitPreco.

Manualmente, force a limpeza global:
```bash
conda run -n hummingbot python tools/precleanup.py \
  --controller-config conf/controllers/xemm_lead_lag_btc_brl.yml \
  --password Senha123
```

### "Fill aconteceu mas hedge não fechou"

Crítico — exposição em aberto. Checar:
```bash
# 1. O ledger registrou? Se não, o executor não terminou ainda.
tail -1 logs/xemm_lead_lag/trades.jsonl | jq
# Se o último trade tem close_type=COMPLETED → hedge fechou.
# Se não, ler:

# 2. Status do executor (procurar pela ordem maker preenchida)
grep -E "Maker order .* completed. Executing taker order|Taker order|hedge" \
  logs/logs_conf_xemm_lead_lag_shadow.log | tail -20

# 3. Conexão Binance OK?
grep -E "binance.*ERROR|binance.*disconnect" logs/logs_conf_xemm_lead_lag_shadow.log | tail -10
```

### "PnL negativo no fill — por quê?"

Causas comuns (em ordem decrescente de probabilidade):
1. **Cancel race:** ordem foi preenchida depois que cancel foi emitido.
   `grep "INVALID_ORDER_ID\|cancel_retry" log` — se aparecem juntos perto
   do timestamp do fill, é isso.
2. **Movimento brusco entre placement e fill:** lead-lag detectou tarde.
3. **Slippage no taker:** `taker_avg_px` muito longe do esperado. Verifique
   `cum_fees_quote` no record para isolar fee vs slippage.

### "Bot crashou / saiu sozinho"

```bash
# Última atividade antes do crash
tail -100 logs/logs_conf_xemm_lead_lag_shadow.log

# CSV pode capturar mais que o log textual em fill recente
tail -10 logs/xemm_lead_lag/xemm_lead_lag_*.csv | tail -1

# Verificar se foi morto por OOM
dmesg -T | grep -i "killed process.*hummingbot" | tail -3
```

---

## 6. Safety mechanisms — referência rápida

| Mecanismo | Onde | Trigger | Janela |
|---|---|---|---|
| Kill switch | `/tmp/xemm_lead_lag_pause` | manual | imediato |
| SIGTERM handler | `runnable_base.py` | `pkill` | cancela ordens antes de sair |
| Cancel retry (executor) | `xemm_executor.control_maker_order` | `_cancel_requested=True` | a cada 3s |
| Orphan check | `xemm_lead_lag.update_processed_data` | periódico | 10s interval, 3s min_age |
| Boot-paused | `xemm_lead_lag.__init__` | startup | até audit OK |
| Inventory audit | `xemm_lead_lag._run_inventory_audit` | periódico | 5min |
| Hedge failure breaker | `XEMMExecutor` | N falhas seguidas | 30min pause |
| Daily loss limit | controller | `pnl_today < -limit` | resto do dia |
| BitPreco cancel guard | `bitpreco_exchange._place_cancel` | `exchange_order_id=None` | bypass API call |
| INVALID_ORDER_ID = GONE | `bitpreco_exchange._place_cancel` | resposta INVALID_ORDER_ID | trata como cancelado |

Em sequência, o pior caso de exposição não-hedgeada é ~13 segundos:
- t≈0: cancel emitido (pode falhar se PENDING_CREATE)
- t≈1-3: retry #1 com `exchange_order_id` já conhecido
- t≈4-6: retry #2 se #1 falhou (rede, rate limit)
- t≈7-9: retry #3
- t≈10-13: orphan_check final

---

## 7. Configuração rápida (sem mexer no YAML)

Tudo em `conf/controllers/xemm_lead_lag_btc_brl.yml`. Mudanças **requerem
restart** — não há hot-reload. Knobs mais usados:

| Campo | Significado |
|---|---|
| `shadow_mode` | true = só loga decisões, não envia ordens |
| `min_profitability` / `target` / `max_profitability` | banda de prof. NET (decimal) |
| `order_amount` | tamanho por ordem em base asset |
| `enable_market_making` | liga/desliga XEMM |
| `enable_pure_arb` | liga/desliga arbitragem taker:taker |
| `inventory_audit.on_drift_action` | `pause` \| `auto_rebalance` \| `alert` |
| `max_daily_loss_quote` | circuit breaker diário em quote (BRL) |

Comentários extensos no YAML explicam cada um. Mudou? Reinicie pelo bloco
"Reiniciar limpo" da seção 3.

---

## 8. Monitoramento contínuo durante sessões Claude

> **TL;DR para o agente**: Quando o usuário pedir "ligue o monitoramento" ou
> "monitore a cada N minutos", **não use `CronCreate` do Claude sozinho** —
> ele só dispara em momentos idle e perde ticks em conversas longas. Use
> as **3 camadas** abaixo. Validado em prod 2026-05-11.

### Por que esse protocolo existe

Tentativas anteriores (`ScheduleWakeup` em loop manual, `CronCreate` sozinho)
falharam várias vezes por causa do design "idle-only" do scheduler do harness
Claude. Sintoma típico: usuário vê o cron "ativo" no painel mas mensagens
não aparecem nem o bot é checado. Solução é triangular determinismo (cron OS)
com visibilidade (Monitor + CronCreate paralelo).

### Arquitetura — 3 camadas

| Camada | Mecanismo | Função | Confiabilidade |
|---|---|---|---|
| 1. Cron OS | `crontab -e` | Roda `tools/monitor_heartbeat.sh` a cada 5min, escreve linha em `logs/monitor_heartbeat.log` | **100%** — independe do Claude |
| 2. Monitor persistent | Harness Claude `Monitor` tool | Tail-a o arquivo do (1) → cada linha vira notificação no chat | Alta — só falha se Claude morrer |
| 3. CronCreate Claude | Harness `CronCreate` `*/5 * * * *` | Aparece no painel "Loops ativos" (visual). Quando dispara, lê o tail do arquivo e confirma | Best-effort (idle-only) — só pra UI |

### Como armar (passo a passo)

**Pré-requisito**: o bot está rodando (`pgrep -fa hummingbot_quickstart` retorna pids).

**1. Verificar script de heartbeat existe e funciona:**
```bash
ls -la tools/monitor_heartbeat.sh   # se ausente, criar (ver schema abaixo)
bash tools/monitor_heartbeat.sh
tail -1 logs/monitor_heartbeat.log
```
Saída esperada (1 linha ≤100 chars):
```
2026-05-11T10:45:01Z OK bot_pid=143433 fills=4 anomalies=0 log_lines=2167
```

Status possíveis: `OK` | `FILL_NEW seq=N` | `ANOMALY n=N` | `DOWN`.

**2. Instalar cron OS (idempotente):**
```bash
crontab -l 2>/dev/null > /tmp/cb.before
grep -q monitor_heartbeat /tmp/cb.before || (cat /tmp/cb.before; cat <<EOF
# XEMM Lead-Lag — heartbeat every 5 minutes
*/5 * * * * /home/ubuntu/hummingbot-ney/tools/monitor_heartbeat.sh >> /tmp/cron_heartbeat.err 2>&1
EOF
) | crontab -
crontab -l | grep heartbeat
```

**3. Armar Monitor persistent no Claude:**
```
Monitor tool, persistent=true, timeout_ms=3600000:
  cd /home/ubuntu/hummingbot-ney
  HEARTBEAT=logs/monitor_heartbeat.log
  LOG=logs/logs_conf_xemm_lead_lag_shadow.log
  echo "monitor_armed heartbeat=$HEARTBEAT log=$LOG"
  tail -n 0 -F "$HEARTBEAT" 2>/dev/null &
  tail -n 0 -F "$LOG" 2>/dev/null | grep -E --line-buffered \
    "CRITICAL|ERROR|Traceback|REBALANCE_STUCK|DRIFT_STUCK|EVENT_LOOP_LAG|KILL_SWITCH|orphans=[1-9]|\[ghost_fill\]" &
  wait
```

**4. Armar CronCreate paralelo (só pra UI):**
```
CronCreate, cron="*/5 * * * *", recurring=true, prompt:
  Heartbeat check — leia tail -3 de /home/ubuntu/hummingbot-ney/logs/monitor_heartbeat.log e me mostre.
  Se a linha mais recente for ANOMALY ou DOWN ou FILL_NEW, rode bash tools/monitor_digest.sh e analise.
  Caso contrário, apenas mostre as 3 linhas e confirme "monitoramento OK".
```

### Como desligar

```bash
# 1. Cron OS
crontab -l | grep -v monitor_heartbeat | crontab -

# 2. Monitor Claude — usar TaskStop <task_id> (id retornado quando armado)
# 3. Cron Claude — usar CronList para descobrir id, depois CronDelete <id>
```

### Schema de `tools/monitor_heartbeat.sh`

Faz exatamente uma coisa: escreve 1 linha em `logs/monitor_heartbeat.log` a cada chamada.

Coleta: timestamp UTC, status, `bot_pid`, `fills` (de state.json), `anomalies`
(contagem de CRITICAL/ERROR/orphans=N>0/etc nas últimas 400 linhas do log do
bot), `log_lines` (total). Estado entre execuções: `logs/xemm_lead_lag/.heartbeat_last_fill_seq`
(para detectar `FILL_NEW`).

Se mudar formato de output, manter ≤100 chars/linha (cabe em notification do Monitor).

### Auditoria a qualquer momento

```bash
tail -20 logs/monitor_heartbeat.log    # últimos 20 ticks
grep -v " OK " logs/monitor_heartbeat.log | tail   # só não-OK (fills, anomalias, downs)
```

Esses arquivos sobrevivem mesmo se Claude crashar — o cron OS continua escrevendo.

### Anti-padrões — não usar isoladamente

- ❌ **`ScheduleWakeup` em loop manual** (reagendar a cada turno): esquece, perde ticks
- ❌ **`CronCreate` sozinho**: idle-only, perde ticks em conversa longa
- ❌ **Monitor persistent sem cron OS por trás**: silêncio é ambíguo, usuário perde confiança
- ✅ **As 3 camadas juntas**: cron OS garante determinismo, Monitor garante visibilidade contínua, CronCreate fornece confirmação visual no painel
