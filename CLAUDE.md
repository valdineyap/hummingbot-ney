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

## Antes de implementar: ofereça a opção simples

Para qualquer mudança não-trivial (refactor, nova feature, correção de bug
que envolva mais de um arquivo), **proponha duas soluções antes de codar**:

1. **Solução simples** — menos código, menos estado, menos abstração. Cite o
   custo (ex: "mais lento em X", "menos preciso em Y", "não cobre o caso Z").
2. **Solução performática/robusta** — a "ideal" técnica. Cite o custo em
   complexidade, superfície de teste e risco de manutenção.

Sempre deixe o usuário decidir qual caminho seguir. **Default = simples**
quando o ganho da versão complexa for marginal ou não-medido.

Motivo: o refactor do audit (2026-05-11/12) começou com 3 camadas de defesa
(timer in-flight + 3-strike + killed-mode passive audit) quando uma barreira
stop-the-world resolveria tudo. A solução simples só apareceu quando o
usuário perguntou "será que não estamos complicando?". Isso não pode
depender da intuição do usuário — tem que vir no primeiro plano.

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
