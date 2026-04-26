# PMM Lead-Lag — Estado da implementação

## ✅ Fase 1 — PMM puro (sem skew, sem regime) — concluída

Commits: `5d6b1efc` (initial), `1992a90a` (gap fixes vs §2.2/2.3/2.4)

### A — pmm_lead_lag_utils.py
- [x] dataclasses (InventoryState, VolState, LeadState, RegimeState, SkewState, SidePermissions, OrderParams)
- [x] compute_inventory_state()
- [x] compute_size_factors() — 3 zonas conforme §2.3
- [x] compute_vol_state()
- [x] compute_lead_state() — stub neutro (Phase 5)
- [x] compute_regime_state() — stub "normal" (Phase 4)
- [x] compute_skew_state()
- [x] compute_side_permissions() — hysteresis assimétrica hard_cap→hard_band
- [x] compute_order_params()

### B — pmm_lead_lag_skew.py (Phase 1)
- [x] PMMLeadLagSkewConfig — 4 bandas (soft/hard/cap/kill) + validators
- [x] PMMLeadLagSkewController.__init__
- [x] update_processed_data() — Phase 1 (sem skew)
- [x] get_levels_to_execute() — filtro sides_enabled
- [x] get_executor_config()
- [x] to_format_status()

### C/D/E — entregáveis Phase 1 — todos OK

## ✅ Fase 2 — Inventory + size factor + CSV — em andamento

### Phase 2 A — live balance fetch
- [x] _split_pair() — BTC-BRL → ('BTC', 'BRL')
- [x] _get_balances() — connectors[name].get_balance(asset)
- [x] update_processed_data integra balances reais

### Phase 2 B — hard cap em BRL absoluto (§2.5)
- [x] net_exposure_quote = base_balance * mid
- [x] over_max_net_position flag em processed_data
- [x] (kill switch fica para Phase 4 conforme plano)

### Phase 2 C — CSV logging
- [x] _csv_log_signals() escreve em logs/pmm_lead_lag/signals.csv
- [x] header + 21 colunas + uma linha por update_processed_data

### Phase 2 D — testes
- [x] balance fetch via mock connector
- [x] inv_pct calculado de balances reais
- [x] over_max_net_position flag
- [x] size_factor=0 → get_executor_config retorna None
- [x] one-sided trigger em hard_cap
- [x] CSV escreve header + linhas
- [x] band ordering enforced

## ✅ Fase 3 — Skew por inventário + volatilidade — em andamento

### Phase 3 A — volatilidade
- [x] compute_volatility_from_prices() — σ de log-returns puro
- [x] CandlesConfig + get_candles_config() (BTC-BRL 1m)
- [x] _compute_vol_from_candles() (σ_short = 30 candles, σ_ref = 360 candles)
- [x] vol_state real → spread_multiplier propagado para order_params

### Phase 3 B — skew ativo
- [x] skew_max_bps=2 default (Fase 3); teto absoluto 8
- [x] compute_order_params combina vol_mult * regime_mult (§5.3)

### Phase 3 C — freios de churn (§5.7)
- [x] min_requote_bps em executors_to_refresh (suprime refresh natural com delta < 1bps)
- [x] force_requote_bps em executors_to_early_stop (cancela orders staled em mercado rápido)

### Phase 3 D — clamp de distância (§5.6)
- [x] _clamp_to_mid no get_executor_config (bid nunca ≥ mid, ask nunca ≤ mid)

### Phase 3 E — testes
- [x] 5 testes vol (constant, increasing, capped, fallback)
- [x] 3 testes clamp (buy/sell cross + safe stay)
- [x] 1 teste skew long → ref < mid
- [x] 5 testes requote brakes (min skip/allow, force stop/no-stop/skip-trading)

## ⏳ Próximas fases

### Fase 4 — Regime + kill switch
- [ ] L1/L1.5/L2/L3 state machine
- [ ] vol_pause_threshold trigger
- [ ] adverse_fill_ratio_10s métrica
- [ ] max_session_drawdown_quote → kill
- [ ] max_net_position_quote → kill (move flag de Phase 2 para gatilho)

### Fase 5 — Lead-lag (sintético + micro)
- [ ] CandlesFeed BTC-USDT + USDT-BRL
- [ ] s_lead_micro (1-5s, BTCUSDT puro)
- [ ] s_lead_regime (30s-5min, sintético com EWM + deadband + 3-leg staleness)
- [ ] w_lead=0.1 inicial, validação A/B
