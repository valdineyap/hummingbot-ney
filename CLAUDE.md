# XEMM Lead-Lag BTC-BRL bot

Market-making cross-exchange (BitPreco maker × Binance taker) com sinal lead-lag
sintético. Branch ativa: `claude/xemm-leadlag`.

## Onde procurar

- **Operação e debug em runtime** → [`OPERATIONS.md`](OPERATIONS.md)
  Monitoramento rápido (`state.json`, `trades.jsonl`, `last_fill.touch`),
  troubleshooting, ciclo de vida do bot, safety mechanisms.
- **Arquitetura e estado do projeto** → [`DEVELOPMENT_STATUS.md`](DEVELOPMENT_STATUS.md)
- **Histórico do swap Bybit→BitPreco** → [`HANDOFF_BITPRECO_SWAP.md`](HANDOFF_BITPRECO_SWAP.md)

## Convenções rápidas

- **Conda env**: `hummingbot`
- **Kill switch** (pausa imediata): `touch /tmp/xemm_lead_lag_pause`
- **Bot vivo?** `pgrep -fa hummingbot_quickstart | grep -v grep`

## "Ligar o monitoramento" — protocolo obrigatório

Quando o usuário pedir para monitorar a cada N minutos (ex: "rode o robo e
monitore", "ligue o monitoramento"), **NÃO** use `CronCreate` do Claude
sozinho nem `ScheduleWakeup` em loop manual — esses são best-effort
idle-only e perdem ticks em conversas longas. Já falharam várias vezes.

Use o protocolo das **3 camadas** documentado em
[`OPERATIONS.md §8`](OPERATIONS.md#8-monitoramento-contínuo-durante-sessões-claude):
cron OS (determinístico) + Monitor persistent (visibilidade contínua no
chat) + CronCreate paralelo (visual no painel). Validado em prod
2026-05-11. Script de heartbeat: `tools/monitor_heartbeat.sh`.
