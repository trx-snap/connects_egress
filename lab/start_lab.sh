#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-1v1}"

echo "=========================================================="
echo "          STARTING CONNECTS-EGRESS LAB (${MODE})          "
echo "=========================================================="

# 1. Start LiveKit Server if not running
if ! nc -z 127.0.0.1 7880 >/dev/null 2>&1; then
  echo "Starting LiveKit server on 127.0.0.1:7880..."
  livekit-server --dev --bind 127.0.0.1 --node-ip 127.0.0.1 > "${SCRIPT_DIR}/.logs/livekit-server.log" 2>&1 &
  sleep 1
  for i in {1..20}; do
    if nc -z 127.0.0.1 7880 >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
  echo "✓ LiveKit Server started."
else
  echo "✓ LiveKit Server already running on 127.0.0.1:7880"
fi

# 2. Ensure test fixtures exist
"${SCRIPT_DIR}/generate_fixtures.sh"

# 3. Create rooms
echo "Ensuring test rooms exist..."
lk room create --name room_a --dev >/dev/null 2>&1 || true
lk room create --name room_b --dev >/dev/null 2>&1 || true

if [[ "${MODE}" == "quadro" || "${MODE}" == "tryon" ]]; then
  lk room create --name room_c --dev >/dev/null 2>&1 || true
  lk room create --name room_d --dev >/dev/null 2>&1 || true
fi

# 4. Stop any existing publishers
"${SCRIPT_DIR}/stop_lab.sh"

# 5. Launch publishers
echo "Launching synthetic publishers..."
"${SCRIPT_DIR}/publishers/stream_host.sh" host_a room_a 9101 9102 smpte 440
"${SCRIPT_DIR}/publishers/stream_host.sh" host_b room_b 9103 9104 ball 880

if [[ "${MODE}" == "quadro" || "${MODE}" == "tryon" ]]; then
  "${SCRIPT_DIR}/publishers/stream_host.sh" host_c room_c 9105 9106 pinwheel 660
  "${SCRIPT_DIR}/publishers/stream_host.sh" host_d room_d 9107 9108 spokes 550
fi

# Brief wait to settle
sleep 2

# 6. Display status
"${SCRIPT_DIR}/status_lab.sh"
