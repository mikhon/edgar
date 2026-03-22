#!/bin/bash
# Script to load all Edgar data into QuestDB
#
# Prerequisites:
#   - QuestDB running locally (./scripts/etl/questdb_ctl.sh start)
#   - Company facts downloaded to ~/.edgar/companyfacts/
#   - pip install questdb yfinance

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🚀 Starting full Edgar data load into QuestDB..."
echo ""

# Check QuestDB is running
if ! "$SCRIPT_DIR/questdb_ctl.sh" status &>/dev/null; then
    echo "❌ QuestDB is not running. Start it first:"
    echo "   $SCRIPT_DIR/questdb_ctl.sh start"
    exit 1
fi

# Activate virtual environment if present
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

echo "📊 Step 1/2: Loading financial data (company facts)..."
echo "   This will process all companies and may take 30-60 minutes."
echo ""
python "$SCRIPT_DIR/build_questdb.py" "$@"

echo ""
echo "📈 Step 2/2: Loading daily price data..."
echo "   This will download prices from Yahoo Finance."
echo ""
python "$SCRIPT_DIR/build_daily_prices.py" "$@"

echo ""
echo "✅ Full data load complete!"
echo "🌐 View your data at: http://localhost:9000"
echo ""
echo "   Try these queries:"
echo "   SELECT * FROM financial WHERE concept = 'revenue' LIMIT 10;"
echo "   SELECT * FROM daily_price WHERE ticker = 'AAPL' ORDER BY ts DESC LIMIT 10;"
