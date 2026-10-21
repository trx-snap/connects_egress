#!/usr/bin/env bash
# Connects Egress LL-HLS Web Player Launcher
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

PORT="${PORT:-3050}"
EGRESS_ORIGIN="${EGRESS_ORIGIN:-http://127.0.0.1:18080}"

echo "=================================================================="
echo "    Launching Connects Egress Sub-2s LL-HLS Web Player"
echo "=================================================================="
echo " Upstream Egress: $EGRESS_ORIGIN"
echo " Local Web UI:    http://localhost:$PORT"
echo "=================================================================="

# Check if Node is available
if command -v node >/dev/null 2>&1; then
  echo "Starting Node.js server..."
  node server.js &
  SERVER_PID=$!
elif command -v go >/dev/null 2>&1; then
  echo "Starting Go server..."
  go run server.go &
  SERVER_PID=$!
else
  echo "Error: Neither Node.js nor Go runtime was found."
  exit 1
fi

sleep 1

# Open in default macOS browser
if command -v open >/dev/null 2>&1; then
  open "http://localhost:$PORT"
fi

wait $SERVER_PID
