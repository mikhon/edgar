#!/bin/bash
# QuestDB lifecycle management script (local binary via Homebrew)
#
# Usage:
#   ./scripts/etl/questdb_ctl.sh install   - Install QuestDB via Homebrew
#   ./scripts/etl/questdb_ctl.sh start     - Start QuestDB daemon
#   ./scripts/etl/questdb_ctl.sh stop      - Stop QuestDB daemon
#   ./scripts/etl/questdb_ctl.sh status    - Check if QuestDB is running
#   ./scripts/etl/questdb_ctl.sh restart   - Restart QuestDB
#   ./scripts/etl/questdb_ctl.sh web       - Open QuestDB web console

set -euo pipefail

QUESTDB_DATA_DIR="${QUESTDB_DATA_DIR:-$HOME/.questdb}"

cmd_install() {
    if command -v questdb &>/dev/null; then
        echo "✅ QuestDB is already installed: $(questdb version 2>/dev/null || echo 'version unknown')"
        return 0
    fi

    echo "📦 Installing QuestDB via Homebrew..."
    brew install questdb

    echo "✅ QuestDB installed successfully"
    echo "   Data directory: $QUESTDB_DATA_DIR"
    echo "   Web console:    http://localhost:9000"
    echo "   ILP port:       9009"
    echo "   PostgreSQL:     localhost:8812"
}

# Check if QuestDB is actually listening on its HTTP port.
# `questdb status` exits 0 whether running or not ("Not running" vs "Running, PID=...").
# We grep for "pid" which only appears in the "Running, PID=..." output.
_is_running() {
    questdb status -d "$QUESTDB_DATA_DIR" 2>/dev/null | grep -qi "pid"
}

cmd_start() {
    if _is_running; then
        echo "⚠️  QuestDB is already running"
        return 0
    fi

    echo "🚀 Starting QuestDB..."
    questdb start -d "$QUESTDB_DATA_DIR"

    # Wait up to 15s for HTTP port to be reachable
    echo -n "   Waiting for QuestDB to be ready"
    for i in $(seq 1 15); do
        sleep 1
        echo -n "."
        if curl -sf http://localhost:9000/status &>/dev/null; then
            echo ""
            echo "✅ QuestDB started and ready"
            echo "   Web console: http://localhost:9000"
            return 0
        fi
    done

    echo ""
    echo "❌ QuestDB started but port 9000 is not responding after 15s."
    echo "   Check logs at $QUESTDB_DATA_DIR/log/"
    return 1
}

cmd_stop() {
    if ! _is_running; then
        echo "⚠️  QuestDB is not running"
        return 0
    fi

    echo "🛑 Stopping QuestDB..."
    questdb stop -d "$QUESTDB_DATA_DIR"
    echo "✅ QuestDB stopped"
}

cmd_restart() {
    cmd_stop
    sleep 1
    cmd_start
}

cmd_status() {
    if _is_running; then
        echo "✅ QuestDB is running"
        echo "   Data directory: $QUESTDB_DATA_DIR"
        echo "   Web console:    http://localhost:9000"
        echo "   ILP port:       9009"
        echo "   PostgreSQL:     localhost:8812"
    else
        echo "❌ QuestDB is not running"
        echo "   Start with: $0 start"
    fi
}

cmd_web() {
    open "http://localhost:9000"
}

# Main
case "${1:-help}" in
    install)  cmd_install ;;
    start)    cmd_start ;;
    stop)     cmd_stop ;;
    restart)  cmd_restart ;;
    status)   cmd_status ;;
    web)      cmd_web ;;
    *)
        echo "Usage: $0 {install|start|stop|restart|status|web}"
        echo ""
        echo "Commands:"
        echo "  install   Install QuestDB via Homebrew"
        echo "  start     Start QuestDB daemon"
        echo "  stop      Stop QuestDB daemon"
        echo "  restart   Restart QuestDB"
        echo "  status    Check if QuestDB is running"
        echo "  web       Open QuestDB web console"
        exit 1
        ;;
esac
