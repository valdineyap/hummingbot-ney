# Handoff: Trocar Bybit → BitPreco (próximo chat)

## Contexto rápido

Você está retomando trabalho num bot XEMM Lead-Lag para BTC-BRL no Hummingbot. O bot está **rodando em live** (Bybit maker + Binance taker/hedge). A tarefa do próximo chat é **substituir a Bybit pela BitPreco** como exchange maker. O conector da BitPreco já foi merged na branch — verifique seu local com `find hummingbot/connector -iname "*preco*"`.

## Branch

`claude/xemm-leadlag` (do repo `hummingbot-ney` em `/home/ubuntu/hummingbot-ney`)

## Estado atual do bot

- **PID 11951**, ativo desde 2026-05-04 08:15
- Maker: **Bybit** BTC-BRL (LIMIT_MAKER) — *será trocada por BitPreco*
- Taker: **Binance** BTC-BRL (MARKET hedge)
- Sinal: Binance BTC-USDT × USDT-BRL
- Config: `conf/controllers/xemm_lead_lag_btc_brl.yml`
- Log ativo: `logs/logs_conf_xemm_lead_lag_shadow.log`
- CSV: `logs/xemm_lead_lag/xemm_lead_lag_xemm_lead_lag_btcbrl_v1_*.csv`

## Arquitetura essencial (ler antes de modificar)

```
Controller (controllers/generic/xemm_lead_lag.py)
    ├── XEMMLeadLagExecutor (maker LIMIT_MAKER + taker hedge)
    └── LeadLagArbitrageExecutor (taker:taker pure arb — desligado por enquanto)

Pre-cleanup: tools/precleanup.py (REST direto antes do bot subir)
Startup:     start_xemm_lead_lag.sh (graceful shutdown + log rotation)
```

## Onde Bybit/Binance estão hardcoded (PRECISA ADAPTAR)

### 1. `controllers/generic/xemm_lead_lag.py`
- **Linha 495-498**: dispatch condicional
  ```python
  if connector_name == "bybit":
      await self._bybit_cancel_open_orders(...)
  elif connector_name == "binance":
      await self._binance_cancel_open_orders(...)
  ```
- **Linhas 522-579**: `_bybit_cancel_open_orders()` — REST `/v5/order/realtime` + `/v5/order/cancel` (Bybit V5 API)
- **Linhas 581-626**: `_binance_cancel_open_orders()` — REST `/openOrders` DELETE batch

### 2. `tools/precleanup.py`
- Constantes `BYBIT_REST_URL`, `BINANCE_REST_URL`
- Funções `_bybit_sign_get/post`, `_binance_sign`
- Funções `bybit_cancel_open_orders`, `binance_cancel_open_orders`
- Dispatch map `DISPATCH = {"bybit": ..., "binance": ...}`

### 3. `conf/controllers/xemm_lead_lag_btc_brl.yml`
- `maker_connector: bybit` → trocar para `bitpreco`
- `taker_connector: binance` (mantém)
- `signal_connector: binance` (mantém)

## O que NÃO precisa mudar (já é genérico via Hummingbot)

- `XEMMLeadLagExecutor` — usa abstrações do framework, zero hardcoding
- `LeadLagArbitrageExecutor` — idem
- Lógica de pricing, regime, lead-lag signal — toda genérica
- O resto do controller (auto_rebalance VWAP, SIGTERM handler, etc.) — funciona com qualquer exchange

## Plano de implementação (decisão do usuário: usar abordagem simples)

> *"eu pensei em algo mais simples, apenas irmos adicionando if connector_name == 'xxx' para cada exchange que quisermos adicionar"*

**Não criar abstração, apenas duplicar o padrão existente:**

1. Em `xemm_lead_lag.py`:
   - Adicionar `elif connector_name == "bitpreco": await self._bitpreco_cancel_open_orders(...)`
   - Implementar `_bitpreco_cancel_open_orders()` (~60 linhas) seguindo o padrão de `_bybit_cancel_open_orders()`
   - Verificar a doc REST da BitPreco para endpoints e signing

2. Em `tools/precleanup.py`:
   - Adicionar `BITPRECO_REST_URL`
   - Adicionar `_bitpreco_sign_*()` functions
   - Adicionar `bitpreco_cancel_open_orders()`
   - Adicionar `"bitpreco": bitpreco_cancel_open_orders` no DISPATCH map

3. YAML:
   - `maker_connector: bitpreco`
   - Verificar credenciais BitPreco em `conf/connectors/bitpreco.yml` (o conector merged deve ter um exemplo)

4. Validação:
   - Confirmar que `min_taker_base_for_sell_hedge`, `min_taker_quote_for_buy_hedge`, `total_amount_quote` e `order_amount` fazem sentido com a liquidez/fees da BitPreco
   - **Confirmar fee tier real** da BitPreco — pode mudar `min/target/max_profitability` (atualmente 7/10/15 bps NET assumindo ~11 bps round-trip)

## Recursos implementados recentemente neste chat (já em produção)

| Recurso | Status |
|---------|--------|
| **SIGTERM/SIGINT handler** (`_setup_sigterm_handler`, `_graceful_shutdown`) | ✅ Ativo — cancela ordens antes de encerrar |
| **Fix `_has_inflight_activity()`** — só checa `_last_fill_time < 10s` agora | ✅ Ativo — auditoria não mais bloqueada |
| **`auto_rebalance` com VWAP + book depth** (`_rebalance_evaluate_exchange`, `_execute_pending_rebalances`) | ✅ Ativo — `on_drift_action: "auto_rebalance"` |
| Tiered polling 200ms + fingerprint | ✅ Ativo |
| Book-aware pricing (improve/join) | ✅ Ativo |
| Inventory audit com boot-paused mode | ✅ Ativo |

## Convenções do projeto

- **Tests**: `test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py` (95 testes)
  - Rodar com: `conda run -n hummingbot python -m pytest test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py -v`
- **Idioma dos comentários**: misto pt-BR/en (segue o que já existe no arquivo)
- **Não criar arquivos .md sem pedir** — exceto `HANDOFF_*` e `DEVELOPMENT_STATUS.md`
- **Logs rotativos**: o `start_xemm_lead_lag.sh` renomeia o log anterior com timestamp UTC
- **Senha do Hummingbot**: `Senha123` (passada via CLI no script)

## Comandos operacionais

```bash
# Restart com graceful shutdown
bash start_xemm_lead_lag.sh Senha123

# Pause via kill switch (suave)
touch /tmp/xemm_lead_lag_pause

# Verificar status
ps aux | grep -E "[h]ummingbot_quickstart"
tail -f logs/logs_conf_xemm_lead_lag_shadow.log

# Verificar audit/balance no CSV
LATEST=$(ls -t logs/xemm_lead_lag/*.csv | head -1)
tail -1 "$LATEST" | awk -F, '{print "regime="$3" combined_btc="$36" audit_drift="$52" inflight="$53}'
```

## Riscos a observar na troca

1. **Tick size / min_notional / min_order_size** da BitPreco podem ser diferentes — verificar `trading_rules` antes de subir
2. **LIMIT_MAKER**: a BitPreco precisa suportar (rejeição quando cruzaria livro). Se não suportar, terá fallback para LIMIT puro — risco de maker→taker conversion
3. **Liquidez BTC-BRL na BitPreco** pode ser menor que na Bybit — VWAP-based detection vai filtrar arbs falsas, mas o `order_amount` (0.0002 BTC) pode precisar diminuir
4. **Fee tier**: confirmar via "My Fee Rate" ou equivalente. Se for >5 bps round-trip a mais, ajustar `min_profitability` para cima
5. **Auto_rebalance** já funciona com qualquer exchange — só vai precisar revalidar o capital_check com saldos reais da BitPreco

## Checklist sugerido para o novo chat

- [ ] Confirmar que conector BitPreco está em `hummingbot/connector/exchange/bitpreco/`
- [ ] Ler doc REST da BitPreco (signing, endpoints de cancel)
- [ ] Implementar `_bitpreco_cancel_open_orders()` no controller
- [ ] Implementar `bitpreco_cancel_open_orders()` no precleanup.py
- [ ] Adicionar nos dispatch maps
- [ ] Atualizar YAML (`maker_connector: bitpreco`)
- [ ] Configurar credenciais BitPreco
- [ ] Confirmar trading rules (tick, min_notional)
- [ ] Confirmar fee tier e ajustar profitability se necessário
- [ ] Rodar testes: `pytest test/hummingbot/strategy_v2/controllers/test_xemm_lead_lag.py`
- [ ] Pause graceful do bot atual: `touch /tmp/xemm_lead_lag_pause`
- [ ] Restart com nova config: `bash start_xemm_lead_lag.sh Senha123`
- [ ] Monitorar log por 1h: `tail -f logs/logs_conf_xemm_lead_lag_shadow.log`
- [ ] Atualizar `DEVELOPMENT_STATUS.md` com a troca

## Arquivos-chave a abrir primeiro no novo chat

1. `DEVELOPMENT_STATUS.md` — visão geral
2. `controllers/generic/xemm_lead_lag.py` linhas 495-626 — dispatch + cancel REST
3. `tools/precleanup.py` — REST + signing
4. `conf/controllers/xemm_lead_lag_btc_brl.yml` — config live
5. `hummingbot/connector/exchange/bitpreco/` (após confirmar que existe) — pode ter exemplos de signing reaproveitáveis

