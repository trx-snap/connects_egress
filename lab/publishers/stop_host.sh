#!/usr/bin/env bash
set -euo pipefail

HOST_ID="${1:-host_a}"
PID_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.pids"

for kind in lk audio video; do
  pid_file="${PID_DIR}/${HOST_ID}_${kind}.pid"
  if [[ -f "${pid_file}" ]]; then
    pid="$(cat "${pid_file}")"
    if kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      echo "Stopped ${HOST_ID} ${kind} (PID ${pid})"
    fi
    rm -f "${pid_file}"
  fi
done
