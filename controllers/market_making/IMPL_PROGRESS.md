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

## ✅ Fase 4 — Regime + kill switch — concluída

### Phase 4 A — máquina de regime (§4.3)
- [x] `_evaluate_regime()` — L1/L1.5/L2/L3 state machine no controller
- [x] L3 latching: kill por `inv_kill`, `max_net_position_quote`, `max_session_drawdown_quote`
- [x] L2 paused: vol_ratio > vol_pause OR |basis| > pause_basis_bps
- [x] L2 dwell: pause_release_sec após cessar condição
- [x] L1.5 safe: l1_duration > safe_mode_entry_sec OR thrash ≥ 2
- [x] L1.5 dwell: 2 × pause_release_sec após cessar condição (sticky)
- [x] L1 degraded: vol_ratio > vol_degraded_threshold_mult

### Phase 4 B — config + state vars
- [x] PMMLeadLagSkewConfig: 8 novos campos (vol_degraded/pause_threshold_mult, pause_basis_bps, pause_release_sec, safe_mode_entry_sec, safe_thrash_window_sec, max_session_drawdown_quote, critical_error_threshold)
- [x] field_validator: vol_pause > vol_degraded
- [x] `__init__`: _l1_entry_time, _safe_entry_time, _last_l2_time(-inf), _l2_timestamps, _is_killed, _regime_cause, _seen_executor_ids, _session_pnl
- [x] Helpers: _update_session_pnl, _trigger_kill, _record_l2_transition, _l2_transitions_in_window

### Phase 4 C — orquestração no update_processed_data
- [x] over_max_net_position + net_exposure_quote setados ANTES de _evaluate_regime
- [x] _update_session_pnl acumula net_pnl_quote de executors completos via _seen_executor_ids
- [x] regime evaluado ANTES de skew → skew zerado em safe/paused/killed
- [x] regime → spread_multiplier (normal=1, degraded=1.5, safe=2.5, paused=1, killed=1) via RegimeState
- [x] processed_data novo: regime_cause, session_pnl

### Phase 4 D — efeitos do regime nos hooks do controller
- [x] get_levels_to_execute: paused/killed → []; safe → 1 nível por lado
- [x] get_executor_config: paused/killed → None; degraded → ×0.5; safe → ×0.25
- [x] executors_to_early_stop: paused/killed → cancela tudo; safe → cancela > buy_0/sell_0; mantém force-requote
- [x] to_format_status + get_custom_info: regime_cause, session_pnl, is_killed
- [x] CSV: 23 colunas (adicionadas regime_cause e session_pnl)

### Phase 4 E — testes (22 novos testes)
- [x] regime states: normal/degraded/paused/safe/killed (10)
- [x] kill triggers: inv_kill, max_net_position, session_drawdown, latch (4)
- [x] level filter: paused→[], killed→[], safe→1+1, safe→shift=0 (4)
- [x] size factors: degraded ×0.5, safe ×0.25 (2)
- [x] cancel-all: paused/killed/safe(deeper) (3)
- [x] telemetria: regime_cause, session_pnl, custom_info (2)
- [x] testes Fase 1-3 atualizados: setUp Phase 3 com max_net_position=10000; vol_capped_at_3 com novo upper bound

## ⏳ Próximas fases

### Fase 5 — Lead-lag (sintético + micro)
- [ ] CandlesFeed BTC-USDT + USDT-BRL
- [ ] s_lead_micro (1-5s, BTCUSDT puro)
- [ ] s_lead_regime (30s-5min, sintético com EWM + deadband + 3-leg staleness)
- [ ] w_lead=0.1 inicial, validação A/B
