# PMM Lead-Lag — Estado da implementação

## Fase 1 — PMM puro (sem skew, sem regime)

### A — pmm_lead_lag_utils.py
- [x] imports + dataclasses (InventoryState, VolState, LeadState, RegimeState, SkewState, SidePermissions, OrderParams)
- [x] compute_inventory_state()
- [x] compute_size_factors()
- [x] compute_vol_state()
- [x] compute_lead_state() — stub neutro (Phase 5)
- [x] compute_regime_state() — stub "normal" (Phase 4)
- [x] compute_skew_state()
- [x] compute_side_permissions()
- [x] compute_order_params()

### B — pmm_lead_lag_skew.py
- [x] PMMLeadLagSkewConfig
- [x] PMMLeadLagSkewController.__init__ + _fetch_prices
- [x] update_processed_data() — Phase 1 (sem skew)
- [x] get_levels_to_execute() — filtro sides_enabled
- [x] get_executor_config()
- [x] to_format_status()

### C — scripts/v2_pmm_lead_lag.py
- [x] boot script com drawdown global

### D — conf/controllers/conf_pmm_lead_lag_skew.yml
- [x] YAML Phase 1

### E — testes
- [x] test_pmm_lead_lag_utils.py (inventory, size_factors, skew, sides)
- [x] test_pmm_lead_lag_skew.py (creates 4 actions, side filter)

## Commits
- [ ] feat(utils): A — all pure functions, Phase 1 stubs
- [ ] feat(controller): B — Phase 1 skeleton, bilateral PMM, no skew
- [ ] feat(script): C — boot script com drawdown global
- [ ] conf: D — YAML inicial Phase 1
- [ ] test: E — Phase 1 unit tests
