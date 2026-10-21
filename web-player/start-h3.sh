#!/usr/bin/env bash
# Connects Egress LL-HLS Web Player — HTTP/3 (QUIC) Launcher
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

CONF="$DIR/nginx-h3.conf"
PID_FILE="/tmp/connects-webplayer-h3.pid"
NGINX_BIN="/opt/homebrew/bin/nginx"

if [ ! -x "$NGINX_BIN" ]; then
  NGINX_BIN="$(which nginx 2>/dev/null || true)"
fi

if [ -z "$NGINX_BIN" ] || [ ! -x "$NGINX_BIN" ]; then
  echo "Error: nginx executable with HTTP/3 support not found."
  exit 1
fi

# Ensure SSL certificates exist for QUIC / TLS 1.3
if [ ! -f "$DIR/certs/cert.pem" ] || [ ! -f "$DIR/certs/key.pem" ]; then
  echo "Generating local TLS certificates for HTTP/3..."
  mkdir -p "$DIR/certs"
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$DIR/certs/key.pem" \
    -out "$DIR/certs/cert.pem" \
    -days 365 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:192.168.0.136"
fi

# Stop previous instance if running
if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE" 2>/dev/null || true)
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Stopping existing HTTP/3 nginx instance (PID $OLD_PID)..."
    kill -QUIT "$OLD_PID" 2>/dev/null || true
    sleep 0.5
  fi
  rm -f "$PID_FILE"
fi

echo "=================================================================="
echo "    CONNECTS EGRESS HTTP/3 (QUIC) LOW-LATENCY HLS WEB PLAYER     "
echo "=================================================================="
echo " Protocol:        HTTP/3 (QUIC over UDP) + TLS 1.3"
echo " Nginx Binary:    $NGINX_BIN"
echo " Web Player UI:   https://localhost:3053"
echo " Egress Upstream: http://127.0.0.1:18080"
echo " Live Stream URL: https://localhost:3053/live/room_android_smoke/master.m3u8"
echo "=================================================================="

# Test configuration
"$NGINX_BIN" -t -c "$CONF"

# Start nginx
"$NGINX_BIN" -c "$CONF"
echo "HTTP/3 Web Player server is RUNNING on port 3053 (UDP & TCP)."

# Open browser if available
if command -v open >/dev/null 2>&1; then
  sleep 0.5
  open "https://localhost:3053"
fi
