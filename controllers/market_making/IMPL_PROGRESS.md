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

## ✅ Fase 5 — Lead-lag (sintético + micro) — concluída

### Phase 5 A — pure functions em pmm_lead_lag_utils.py
- [x] `ewm_step()` — EWM contínuo com halflife em segundos
- [x] `compute_lag_micro()` — lag em bps, pause_buy/pause_sell (BTCUSDT puro)
- [x] `compute_lag_regime()` — z_lead com sigma adaptativo + tanh + deadband
- [x] `compute_lead_state()` — composto (micro + regime + basis)
- [x] LeadState estendido com `pause_buy`/`pause_sell`

### Phase 5 B — config (PMMLeadLagSkewConfig)
- [x] `leader_connector` / `leader_trading_pair` (BTC-USDT)
- [x] `quote_rate_connector` / `quote_rate_trading_pair` (USDT-BRL)
- [x] Micro: `lead_micro_window_sec`, `lead_micro_threshold_bps`, `lead_micro_dwell_sec`
- [x] Regime: `lead_lag_short_window_sec`, `lead_lag_ewm_halflife_sec`, `lead_lag_z_window_sec`
- [x] `validate_default=True` no `leader_connector` / `quote_rate_connector` /
      `candles_connector` / `candles_trading_pair` para que defaults fluam

### Phase 5 C — controller wiring
- [x] `__init__`: history deque, EWM state (`_fair_brl_smooth`), `_lag_samples`,
      staleness ts, `_pause_buy_until` / `_pause_sell_until`
- [x] `get_candles_config()` — adiciona BTC-USDT e USDT-BRL (de-duplica)
- [x] `_read_latest_close()` / `_read_leg_prices()` / `_record_lead_history()` /
      `_lookup_past()` helpers
- [x] `_compute_lead_signals()` — orquestrador completo (EWM, sigma adaptativo,
      staleness, past lookups, dwell latching)
- [x] `update_processed_data()` — usa `_compute_lead_signals` real;
      latch de `_pause_*_until` baseado em `lead_micro_dwell_sec`
- [x] `processed_data` exposto: `s_lead_micro`, `s_lead_regime`, `basis_bps`,
      `micro_stale`, `regime_stale`, `pause_buy`, `pause_sell`

### Phase 5 D — efeitos do micro pause nos hooks
- [x] `get_levels_to_execute()` — filtra side em pause até `_pause_*_until`
- [x] `executors_to_early_stop()` — §3.6 critério 1: cancela ordem ativa quando
      `|s_lead_micro| ≥ tightest_side_spread_bps` E `pause_*` está ativo
- [x] `to_format_status()` — linha extra com `s_lead_micro`, `s_lead_regime`,
      `basis_bps`, `Stale:`/`Pause:` flags
- [x] `get_custom_info()` — 7 novos campos lead-lag + staleness
- [x] CSV: 27 colunas (adicionadas micro_stale, regime_stale, pause_buy, pause_sell)

### Phase 5 E — testes
- [x] 6 testes `compute_lag_micro` (stale, USDT↑/↓, threshold, brl-acompanha, inválido)
- [x] 6 testes `compute_lag_regime` (stale, deadband, sinal, clip, inválido)
- [x] 7 testes `compute_lead_state` composto (neutral, stale isolado, deadband, sinal, micro buy/sell)
- [x] 5 testes `ewm_step` (warmup, halflife decay, dt=0, inválido)
- [x] 12 testes controller Phase 5 (pause filter, dwell latch, lag≥spread cancel,
      staleness, w_lead skew, telemetria, candles_config 3 pares)
- [x] Phase 4 tests atualizados — `_neutral_lead_state()` helper para isolar lead-lag

## ✅ Pós-Fase 5 — refator de arquitetura + gatilhos extras

Endereça achados de revisão externa pré-paper trading.

### A — Modularização do regime (plan §1.7)
- [x] `RegimeContext` dataclass — carry-state (l1/safe/l2 timestamps, is_killed, critical_error_count)
- [x] `RegimeThresholds` dataclass — bundle de thresholds passados à função pura
- [x] `compute_regime_state()` real em utils — lógica completa L1/L1.5/L2/L3
- [x] Controller `_evaluate_regime` reduzido a wrapper de delegação (~30 linhas)
- [x] Controller mantém apenas `_regime_ctx`, `_regime_cause`; helpers redundantes
      (`_record_l2_transition`, `_l2_transitions_in_window`, `_trigger_kill`) removidos

### B — Gatilhos completados
- [x] `regime_stale` (feed BTC-USDT velho > max_leader_staleness_sec) → L2 trigger
- [x] `_feed_stale_for_regime_pause()` separa warmup (sem trigger) de stale real
- [x] `critical_error_count` consumido pelo L3 em `compute_regime_state`
- [x] Auto-trigger: `_track_sustained_feed_stale` bumpa o counter em stale > 6×
      (proxy para "websocket loss > 30s" do plano §4.1 L3)
- [x] Hook manual `_record_critical_error(source)` para integrações externas

### C — Observabilidade ampliada
- [x] `orders.csv` (8 colunas) — uma linha por executor criado em `get_executor_config`
      colunas: ts, level_id, side, price, amount, distance_from_mid_bps, shift_bps, regime

### D — Itens deferidos para Fase 6 (após dados de paper)
- [ ] Trend filter (z_trend = ema_fast - ema_slow / σ) — feature nova ~80 linhas
- [ ] Adverse cluster (3 fills consecutivos do mesmo lado, mid adverso > 5bps em 1s)
- [ ] `fills.csv` com adverse_mid_1s e adverse_mid_10s (requer scheduler async)

### E — testes (20 novos)
- [x] 14 testes `compute_regime_state` em utils (normal/L1/L2/L1.5/L3 + ctx mutations)
- [x] 4 testes `_record_critical_error` + `_track_sustained_feed_stale` (counter,
      threshold-triggered kill, episode bump, recovery reset)
- [x] 2 testes `orders.csv` (escrita ativa/desativada)
- Total: 150 testes passando (78 utils + 72 controller)

## ✅ Pós-Fase 5 — Paper trade bootstrap (commits 9f4407ea + ee7e6980)

### Patch V1/V2 compat (`scripts/v2_pmm_lead_lag.py`)
- [x] `_patch_executor_base_for_paper_trade()` — monkey-patch em `ExecutorBase` aplicado
      na importação do script; sem modificar nenhum arquivo de `strategy_v2/executors/`
- [x] `.trading_rules` → retorna `TradingRule(trading_pair)` com defaults permissivos
      (min_order_size=0, min_notional_size=0); evita crash no `__init__` do executor
- [x] `._order_tracker` → retorna `None`; `TrackedOrder` lida com None de forma segura
      (is_done=False, executed_amount=0)

### Configs de paper trade
- [x] `conf/scripts/conf_v2_pmm_lead_lag.yml` — aponta para `conf_pmm_lead_lag_skew_paper.yml`,
      `max_global_drawdown_quote: 10.0 BRL`, `max_controller_drawdown_quote: null`
- [x] `conf/controllers/conf_pmm_lead_lag_skew_paper.yml` — connector `binance_paper_trade`,
      candles/leader/quote_rate_connector → `binance` (CandlesFactory não suporta paper_trade),
      `max_net_position_quote: 999999` (saldo paper 1 BTC + 500k BRL viola cap live de R$60),
      `w_lead: 0.0` (Semana 1 baseline Phase 4)

### Limitações conhecidas do paper trade
- **Fill tracking zerado**: `PaperTradeExchange` (Cython V1) não completa ciclo de vida do
  executor V2 → `session_pnl` permanece 0 durante todo o paper trade
- **Workaround de PnL**: monitorar `base_bal * mid + quote_bal` via `signals.csv`
  (colunas `base_balance`, `mid_brl`, `quote_balance` ou equivalente na signals)
- Regime, spread, inv_pct, signals.csv e colocação de ordens funcionam corretamente

### Como iniciar
```bash
conda activate hummingbot
cd ~/hummingbot-ney
conda run -n hummingbot python bin/hummingbot_quickstart.py \
  --v2 conf_v2_pmm_lead_lag.yml -p Senha123 --headless &

tail -f logs/pmm_lead_lag/signals.csv
tail -f logs/logs_conf_v2_pmm_lead_lag.log | grep -v MQTT
```

## ⏳ Próximas fases

### Fase 6 — Validação extensiva pré-capital
- [ ] Paper trading 2 semanas (incl. fim-de-semana) — **EM ANDAMENTO** (paper bootstrap feito)
- [ ] Backtest replay 30 dias
- [ ] Calibração thresholds com p95 vol observada
- [ ] Dry-run R$200 / 3 dias
- [ ] Aumento progressivo: 500 → 1k → 3k → alvo

### Notas pós-Fase 5
- `w_lead=0.1` é o default conservador (plan §5.4); subir só com evidência A/B
- Limitação atual: `_read_latest_close` usa candle 1m → resolução de micro fica
  presa a 1m enquanto não houver bookTicker subscription dedicada para BTC-USDT
- TODO Fase 6: medir `cancel_rate` Fase 5 vs Fase 4; se subir > 30% sem
  ganho de edge, reduzir `lead_micro_threshold_bps` ou zerar `w_lead`
- TODO Fase 6: `session_pnl` sempre zero em paper → usar `base_bal * mid + quote_bal`
  da signals.csv como proxy de PnL; implementar fill tracking real só no dry-run
