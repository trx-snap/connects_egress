#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="${SCRIPT_DIR}/publishers/.pids"

echo "=========================================================="
echo "          CONNECTS-EGRESS LAB ENVIRONMENT STATUS          "
echo "=========================================================="

# 1. LiveKit Server Check
if nc -z 127.0.0.1 7880 >/dev/null 2>&1; then
  echo "✓ LiveKit Server: ONLINE (ws://127.0.0.1:7880, devkey/secret)"
else
  echo "✗ LiveKit Server: OFFLINE"
  exit 1
fi

# 2. Rooms and Participants
echo ""
echo "--- Active LiveKit Rooms ---"
lk room list --dev 2>/dev/null || echo "Unable to list rooms"

# 3. Active Publishers
echo ""
echo "--- Active Lab Publishers ---"
if [[ -d "${PID_DIR}" ]] && compgen -G "${PID_DIR}/*.pid" > /dev/null; then
  for pid_file in "${PID_DIR}"/*.pid; do
    name="$(basename "${pid_file}" .pid)"
    pid="$(cat "${pid_file}")"
    if kill -0 "${pid}" 2>/dev/null; then
      echo "  [RUNNING] ${name} (PID ${pid})"
    else
      echo "  [DEAD]    ${name} (Stale PID ${pid})"
    fi
  done
else
  echo "  No local publisher processes recorded in ${PID_DIR}"
fi
echo "=========================================================="
