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
