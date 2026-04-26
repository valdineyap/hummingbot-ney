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

## ⏳ Próximas fases

### Fase 3 — Skew por inventário + vol
- [ ] σ rolante via candles BTC-BRL (HistoricalVolatility)
- [ ] s_vol → spread_multiplier
- [ ] w_inv=1.0, skew_max_bps=2 (Fase 3 inicial)
- [ ] min_requote_bps freio de churn
- [ ] force_requote_bps recotação imediata em delta grande

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
