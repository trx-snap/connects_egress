#!/usr/bin/env bash
# Connects Egress LL-HLS Web Player — HTTP/3 Stop Script
PID_FILE="/tmp/connects-webplayer-h3.pid"

if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE" 2>/dev/null || true)
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    echo "Stopping HTTP/3 nginx server (PID $PID)..."
    kill -QUIT "$PID" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  echo "Stopped."
else
  echo "No PID file found at $PID_FILE."
fi
