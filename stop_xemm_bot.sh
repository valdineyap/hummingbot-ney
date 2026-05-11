#!/bin/bash

# Stop script para XEMM Lead-Lag bot
# Uso: ./stop_xemm_bot.sh [--force]
# --force: parada imediata (kill); padrão: parada graciosa (pause file + timeout)

FORCE=${1:-""}
BOT_PID=$(pgrep -f "hummingbot_quickstart.py" | tail -1)
LOG_FILE="logs/logs_conf_xemm_lead_lag_shadow.log"
PAUSE_FILE="/tmp/xemm_lead_lag_pause"

echo "=== XEMM Lead-Lag Bot Stop Script ==="
echo ""

if [ -z "$BOT_PID" ]; then
    echo "❌ Bot não está rodando (PID não encontrado)"
    exit 1
fi

echo "✓ Bot PID: $BOT_PID"
echo ""

if [ "$FORCE" == "--force" ]; then
    echo "🛑 Executando parada IMEDIATA (--force)..."
    kill -9 $BOT_PID
    sleep 1
    if ! kill -0 $BOT_PID 2>/dev/null; then
        echo "✓ Processo finalizado"
        echo ""
        echo "Últimas 5 linhas do log:"
        tail -5 "$LOG_FILE"
        exit 0
    else
        echo "❌ Falha ao encerrar processo"
        exit 1
    fi
else
    echo "🛑 Executando parada GRACIOSA (padrão)..."
    echo "   → Criando pause file e aguardando cancelamento de ordens..."
    touch "$PAUSE_FILE"

    # Aguardar até 30s pelo encerramento gracioso
    for i in {1..30}; do
        if ! kill -0 $BOT_PID 2>/dev/null; then
            echo "✓ Processo finalizado graciosamente após $i segundos"
            rm -f "$PAUSE_FILE"
            echo ""
            echo "Últimas 5 linhas do log:"
            tail -5 "$LOG_FILE"
            exit 0
        fi
        echo -n "."
        sleep 1
    done

    # Se ainda estiver rodando após 30s, kill
    echo ""
    echo "⚠️  Timeout (30s) — executando kill forçado..."
    kill -9 $BOT_PID
    sleep 1
    rm -f "$PAUSE_FILE"

    if ! kill -0 $BOT_PID 2>/dev/null; then
        echo "✓ Processo finalizado via kill após timeout"
        echo ""
        echo "Últimas 5 linhas do log:"
        tail -5 "$LOG_FILE"
        exit 0
    else
        echo "❌ Falha ao encerrar processo mesmo após kill"
        exit 1
    fi
fi
