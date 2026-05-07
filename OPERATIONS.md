# Operations & Debug Runbook — XEMM Lead-Lag

Foco: o que um agente (humano ou Claude) precisa para responder rápido a
"o bot está OK?" e investigar quando algo deu errado. Para arquitetura,
ver [`DEVELOPMENT_STATUS.md`](DEVELOPMENT_STATUS.md).

---

## 1. Quick start: "Algo aconteceu desde minha última checagem?"

Três verificações em ordem crescente de detalhe:

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

### Iniciar
```bash
cd /home/ubuntu/hummingbot-ney
bash start_xemm_lead_lag.sh Senha123 > /tmp/start.log 2>&1 &
disown
```
O script `start_xemm_lead_lag.sh` roda `tools/precleanup.py` antes (cancela
ordens órfãs no boot) e só então sobe o bot principal.

### Parar (graceful)
```bash
# Opção A: kill switch (pausa, mantém ordens em curso até serem canceladas)
touch /tmp/xemm_lead_lag_pause

# Opção B: SIGTERM direto (cancela ordens via signal handler antes de sair)
pkill -f hummingbot_quickstart
```

### Reiniciar limpo
```bash
pkill -f hummingbot_quickstart
until ! pgrep -f hummingbot_quickstart >/dev/null; do sleep 2; done
rm -f /tmp/xemm_lead_lag_pause   # CRÍTICO — senão o novo bot já sobe pausado
bash start_xemm_lead_lag.sh Senha123 > /tmp/start.log 2>&1 &
disown
```

### Está vivo?
```bash
pgrep -fa hummingbot_quickstart | grep -v grep
```

---

## 4. Tail de logs em tempo real

```bash
# Log textual completo
tail -f logs/logs_conf_xemm_lead_lag_shadow.log

# Filtrado — eventos importantes
tail -f logs/logs_conf_xemm_lead_lag_shadow.log | \
  grep -E "Created maker|cancel|orphan|FILLED|hedge|ERROR|cancel_retry"

# Só erros e warnings
tail -f logs/logs_conf_xemm_lead_lag_shadow.log | grep -E "ERROR|WARNING|CRITICAL"
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
