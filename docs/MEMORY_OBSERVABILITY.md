# Memory observability — reference for the XEMM lead-lag bot

> **Origem**: 2026-05-15 12:22:56Z OOM-driven event-loop lag (16.7s) com OOM-reboot
> do servidor (3.8 GB RAM, swap=0). Sem observabilidade de memória in-process,
> reconstruímos a curva post-mortem via `sar` — não foi suficiente para
> identificar a fonte. Esta instrumentação fecha esse gap.
>
> **Status atual** (2026-05-17): bot rodou 17h+ contínuas após esta
> instrumentação, **RSS estável em ~370 MB**, sem reincidência. O leak do
> 12:22Z não foi reproduzido em steady-state — provavelmente event-triggered.

---

## Onde está instrumentado

| Sinal | Arquivo | Frequência | Como ativar |
|---|---|---|---|
| `[mem]` | `controllers/generic/xemm_lead_lag.py` → `_log_memory_metrics()` | 60 s | sempre on |
| `[sbe_queue]` | `hummingbot/connector/exchange/binance_sbe/binance_sbe_api_order_book_data_source.py` → `_maybe_log_queue_sizes()` | ≤ 60 s (chamada por frame, gated) | sempre on (só emite se há canais) |
| `[mem/tracemalloc]` | `controllers/generic/xemm_lead_lag.py` → `_maybe_log_tracemalloc()` | 300 s | `XEMM_TRACEMALLOC=1` na env do bot |
| Memory guard L1/L2 + dump forense | `tools/monitor_heartbeat.sh` | 5 min (via cron) | cron já configurado (`*/5 * * * *`) |

Todas as linhas vão para `logs/logs_conf_xemm_lead_lag_sbe.log` (mesmo
canal do bot). Memory guard dumps vão para `logs/xemm_lead_lag/memguard_snapshots.log`.

---

## Legenda do `[mem]`

Exemplo:

```
[mem] rss=370.0MB pss=367.2MB vms=1078.5MB uss=367.0MB hwm=456.6MB swap=0kB
      fd=18 threads=4
      asyncio_tasks=47 oldest_task_age=53000s top_coro=safe_wrapper:44,...
      gc=(421, 12, 5) gc_objects=345210 gc_collections=(1234, 95, 8)
      tm_cur=0.0MB tm_peak=0.0MB
      executors=2 pending_rebalances=0 self_dispatched=0
      redis_us_sm=145 redis_us_orphans=143
```

### Campos de memória do processo

| Campo | Fonte | Significado | O que indica anomalia |
|---|---|---|---|
| `rss` | psutil | resident set size — RAM física usada (inclui shared) | crescimento sustentado > 5 MB/h |
| `pss` | `/proc/self/smaps_rollup` | RSS contando shared pages proporcionalmente; **mais honesto** | usar como métrica primária |
| `vms` | psutil | virtual address space (sempre alto, pouco diagnóstico) | mudanças grandes súbitas |
| `uss` | psutil | private memory (free'd se proc morresse) | similar a PSS |
| `hwm` | `/proc/self/status:VmHWM` | RSS peak histórico do processo | se hwm >> rss atual: já houve pico, agora liberou |
| `swap` | `/proc/self/status:VmSwap` | bytes em swap | servidor sem swap = sempre 0; valor > 0 = pressão |

### Campos OS-level

| Campo | Significado | Anomalia |
|---|---|---|
| `fd` | file descriptors abertos (sockets, files, pipes) | crescimento monotônico = leak de socket |
| `threads` | threads do processo | crescimento = thread leak |

### Campos asyncio

| Campo | Significado | Anomalia |
|---|---|---|
| `asyncio_tasks` | total de tasks ativas | crescimento monotônico = task leak (ex: fire-and-forget travada) |
| `oldest_task_age` | idade da task mais antiga, em segundos | aceitável se for task background (listener WS, audit loop); suspeito se cresce sem novas tasks aparecerem |
| `top_coro` | top-3 coroutines por contagem | nome aparecendo crescente = essa coroutine acumula instâncias |

### Campos GC

| Campo | Significado | Anomalia |
|---|---|---|
| `gc=(g0,g1,g2)` | contadores de alocações desde última coleta de cada geração | normal oscilar; valores >>10000 indicam GC com dificuldade |
| `gc_objects` | total de objetos GC-tracked no heap; **chamado a cada 5min ou em RSS jump >100MB** | crescimento sustentado = Python heap leak |
| `gc_collections` | (col_g0, col_g1, col_g2) — número total de coletas executadas | g2 collections frequentes = pressão de heap |

### Campos tracemalloc (só se `XEMM_TRACEMALLOC=1`)

| Campo | Significado |
|---|---|
| `tm_cur` | Python heap atualmente rastreado |
| `tm_peak` | pico desde início do tracing |

### Campos do controller

| Campo | Significado | Anomalia |
|---|---|---|
| `executors` | XEMMLeadLagExecutor + Arb concorrentes | normal: 1-4; >10 sustentado é estranho |
| `pending_rebalances` | rebalances enfileirados aguardando execução | >0 sustentado = stuck |
| `self_dispatched` | MARKET orders self-emitidas (orphan_hedge suppression registry) | TTL 60s, pequeno é normal |
| `redis_us_sm` | tamanho do state machine do BitPreco user-stream | crescimento = leak no SM |
| `redis_us_orphans` | buffer de eventos órfãos do redis_us | crescimento = leak no buffer |

---

## Legenda do `[sbe_queue]`

Exemplo:

```
[sbe_queue] total=1 max=1 n_channels=3 top=depthUpdate:1,trade:0,order_book_snapshot:0
```

| Campo | Significado | Steady-state esperado | Anomalia |
|---|---|---|---|
| `total` | soma de `qsize()` em todas as filas | 1 (em 99% das amostras) | sustained >100 = consumer travado |
| `max` | maior fila individual | 1 | spikes momentâneos (>10) são normais em burst de trades; persistente é leak |
| `n_channels` | número de filas vivas | 3 (trade, depthUpdate, order_book_snapshot) | <3 = falha em subscribe |
| `top` | top-3 canais por tamanho | depthUpdate dominante em mercado movimentado | um canal estagnado >50 = bug no parser daquele canal |

**Background**: o `_message_queue` herdado de `OrderBookTrackerDataSource:26`
é `defaultdict(asyncio.Queue)` **sem maxsize**. Consumer travado faz crescer
indefinidamente. Este log expõe esse risco — historicamente o canal `trade`
viu burst momentâneo de 48 e drenou em <1s.

---

## Memory guard L1/L2

Em `tools/monitor_heartbeat.sh` (rodado pelo cron a cada 5 min):

| Nível | Trigger | Ação | Razão |
|---|---|---|---|
| L1 | `RSS > 1800 MB` **ou** crescimento `> 100 MB/min` **ou** `swap > 500 MB` | `touch /tmp/xemm_lead_lag_pause` + dump forense | reduz risco financeiro (para de criar ordens) |
| L2 | `RSS > 2400 MB` **ou** L1 disparado e RSS continua subindo `> 50 MB/min` | `kill -TERM $BOT_PID` + dump forense | protege servidor (pause não para market data / WS / queue) |

Dump forense (`logs/xemm_lead_lag/memguard_snapshots.log`): inclui
`/proc/$PID/status`, `smaps_rollup`, `fd_count`, últimas 200 linhas do log.

Estado atual de RSS é também escrito a cada tick na linha de heartbeat
`logs/monitor_heartbeat.log` (campos `rss=`, `swap=`, `growth=MB/min`).

---

## Steady state esperado (medido em 17h de runtime)

```
rss                   346 → 380 MB (~2 MB/h, com plateaus)
pss                   ≈ rss (bot praticamente não compartilha)
hwm                   ≈ 460 MB (picos durante boot/atividade intensa)
swap                  0 kB sempre
fd                    18 ± 2
threads               4 estável
asyncio_tasks         44–48 (45 mediana)
oldest_task_age       cresce monotonic (background listeners desde boot)
gc_objects            ~340–380k, oscilando
sbe_queue total       1 em 99.6% das amostras
executors             0–4 (mediana 1-2)
```

---

## Como interpretar (cheat sheet)

| Sinal observado | Diagnóstico provável |
|---|---|
| `rss` ↑ + `sbe_queue total` ↑ | **#1A confirmado** (consumer SBE travado) |
| `rss` ↑ + `asyncio_tasks` ↑ monotonic | **#2 confirmado** (task leak — fire-and-forget travada) |
| `rss` ↑ + `oldest_task_age` ↑ + task count estável | task antiga retida em I/O sem timeout |
| `rss` ↑ + tracemalloc top em arquivo X | Python heap leak no arquivo X |
| `rss` ↑ + `tm_cur` ESTÁVEL + `gc_objects` estável | **leak NATIVO** (aiohttp/SSL/zlib/glibc) — invisível ao tracemalloc |
| `rss` ↑ + `fd` ↑ | socket leak |
| `rss` ↑ + `threads` ↑ | thread leak |
| `pss << rss` | shared memory inflando RSS (incomum neste bot) |
| `swap` > 0 | servidor sob pressão — não acontece em servidor sem swap (apenas OOM-reboot) |

---

## Gotchas conhecidos

### `tracemalloc.take_snapshot()` bloqueia o event loop
Em heap de ~330k objetos, **`take_snapshot()` leva ~12 s síncronos**. O
watchdog do bot dispara EVENT_LOOP_LAG (threshold 10s) e o `auto_terminate`
mata o processo. Por isso `XEMM_TRACEMALLOC=1` foi observado destruindo o bot
a cada ~30 min (no boundary da snapshot periódica de 300s).

**Mitigação atual**: deixar `XEMM_TRACEMALLOC=0` por padrão.
**Patch futuro (P0)**: `await asyncio.to_thread(tracemalloc.take_snapshot)`
em `_maybe_log_tracemalloc()`.

### `PYTHONTRACEMALLOC=N` na env do interpreter é proibitivo aqui
Foi medido **+490 MB de overhead no boot** com `PYTHONTRACEMALLOC=10`. Para
um bot que opera em servidor 3.8 GB / swap=0 é inviável — o boot dispara
EVENT_LOOP_LAG por simples lentidão de alocação. Use apenas
`XEMM_TRACEMALLOC=1` (que ativa tracing pós-boot).

### O leak do 12:22Z é predominantemente NATIVO
Na única amostra capturada (iter 2 deste hunt), RSS pulou +94 MB enquanto
`tm_cur` cresceu apenas ~1.7 MB. **88% do crescimento foi em alocação
NATIVA** (aiohttp/SSL receive buffers, JSON parse, glibc malloc arenas).
Próxima instrumentação útil é introspectar:

- Tamanho do connection pool aiohttp
- Total bytes WS recv/send acumulados (por connector)
- `malloc_info()` via ctypes
- SSL session count

---

## Quando algo der errado

1. **Bot ainda vivo, RSS subindo**:
   ```bash
   grep '\[mem\]' logs/logs_conf_xemm_lead_lag_sbe.log | tail -10
   grep '\[sbe_queue\]' logs/logs_conf_xemm_lead_lag_sbe.log | tail -5
   tail -50 logs/monitor_heartbeat.log
   ```
   Usar cheat sheet acima para classificar.

2. **Bot morreu por EVENT_LOOP_LAG**:
   ```bash
   grep -B 30 -A 5 EVENT_LOOP_LAG logs/logs_conf_xemm_lead_lag_sbe.log | tail -50
   grep '\[mem\]' logs/logs_conf_xemm_lead_lag_sbe.log | tail -5
   ```
   Olhar **última `[mem]` antes do lag** — se RSS já estava alto, é leak;
   se RSS pulou de uma vez no lag, é alocação síncrona pesada (suspeito:
   tracemalloc snapshot ou parse de JSON grande).

3. **Memory guard L1/L2 disparou**:
   ```bash
   tail -100 logs/xemm_lead_lag/memguard_snapshots.log
   ```
   Dump contém `/proc/$PID/status` + `smaps_rollup` + `fd_count` +
   últimas 200 linhas do log — material suficiente para abrir investigação.

---

## Referências

- Patch original da instrumentação: commit que adicionou esta doc
- Conventions do projeto: [`CLAUDE.md`](../CLAUDE.md)
- Operação/debug em runtime: [`OPERATIONS.md`](../OPERATIONS.md)
- Mapa técnico: [`docs/PROJECT_MAP.md`](PROJECT_MAP.md)
