#!/usr/bin/env bash
# restart_local_egress.sh — Run from macOS to restart the local connects-egress stack
set -e

echo "=== Restarting Connects-Egress on local Linux container (egress-linux-arm64) ==="
ssh egress-linux-arm64 "/home/foxy/restart_egress.sh"
