#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STOP_SERVER="${1:-}"

echo "=== Stopping Connects Egress Lab Publishers ==="
for host in host_a host_b host_c host_d; do
  "${SCRIPT_DIR}/publishers/stop_host.sh" "${host}"
done

if [[ "${STOP_SERVER}" == "--all" ]]; then
  echo "=== Stopping LiveKit Server ==="
  pkill -f "livekit-server" || true
  echo "✓ LiveKit Server stopped."
fi

echo "=== Lab Publishers Stopped ==="
