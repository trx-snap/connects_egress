#!/usr/bin/env bash
# lab/stop_lab.sh [--all] — stop the lab-owned publishers; with --all also stop
# the lab-owned livekit-server and clear the manifest once everything is
# confirmed gone. Only recorded, identity-validated PIDs are signalled (TERM);
# survivors and unknown records are reported and retained.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "${SCRIPT_DIR}/lib/common.sh"

ALL=""
case $# in
  0) ;;
  1) [ "$1" = "--all" ] || { echo "usage: stop_lab.sh [--all]" >&2; exit 2; }; ALL=1 ;;
  *) echo "usage: stop_lab.sh [--all]" >&2; exit 2 ;;
esac
lab_require_tools jq ps || exit 2

if ! lab_records_present; then
  echo "no lab-owned records in ${LAB_STATE_DIR}; nothing to stop"
  exit 0
fi

FAILS=0
echo "=== Stopping lab publishers ==="
for host in host_a host_b host_c host_d; do
  "${SCRIPT_DIR}/publishers/stop_host.sh" "$host" || FAILS=1
done

RELAY_RECORD="$(lab_record_path relay relay)"
if [ -f "$RELAY_RECORD" ]; then
  echo "=== Stopping lab UDP relay ==="
  pid="$(jq -r '.pid // "?"' "$RELAY_RECORD" 2>/dev/null || echo "?")"
  rc=0
  lab_record_stop "$RELAY_RECORD" || rc=$?
  case $rc in
    0) echo "stopped udp-relay (pid ${pid})" ;;
    1) echo "SURVIVOR udp-relay pid ${pid}: still running after TERM; record retained"; FAILS=1 ;;
    *) echo "UNKNOWN udp-relay pid ${pid}: identity not confirmed; not signalled; record retained"; FAILS=1 ;;
  esac
  if [ -f "$LAB_MANIFEST" ]; then
    cfg_dir="$(jq -r '.config // empty' "$LAB_MANIFEST" 2>/dev/null || true)"
    case "$cfg_dir" in
      /tmp/connects-egress-relay-*) rm -rf "$cfg_dir" ;;
    esac
  fi
fi

if [ -f "$LAB_MANIFEST" ] && [ "$(lab_manifest_mode)" = "android" ]; then
  if [ $FAILS -eq 0 ] && [ -z "$(lab_record_files)" ]; then
    rm -f "$LAB_MANIFEST"
    echo "=== Android lab stopped; records cleared ==="
    exit 0
  fi
fi

if [ -n "$ALL" ]; then
  echo "=== Stopping lab-owned LiveKit server ==="
  SERVER_RECORD="$(lab_record_path server server)"
  if [ -f "$SERVER_RECORD" ]; then
    pid="$(jq -r '.pid // "?"' "$SERVER_RECORD" 2>/dev/null || echo "?")"
    rc=0
    lab_record_stop "$SERVER_RECORD" || rc=$?
    case $rc in
      0) echo "stopped livekit-server (pid ${pid})" ;;
      1) echo "SURVIVOR livekit-server pid ${pid}: still running after TERM; record retained"; FAILS=1 ;;
      *) echo "UNKNOWN livekit-server pid ${pid}: identity not confirmed; not signalled; record retained"; FAILS=1 ;;
    esac
  else
    echo "no lab-owned server record (a foreign listener on :${LAB_SERVER_PORT} is never stopped)"
  fi
  if [ $FAILS -eq 0 ] && [ -z "$(lab_record_files)" ]; then
    rm -f "$LAB_MANIFEST"
    echo "=== Lab stopped; records cleared ==="
    exit 0
  fi
fi

if [ -n "$(lab_record_files)" ]; then
  echo "records remaining in ${LAB_PID_DIR}:"
  for f in "$LAB_PID_DIR"/*.json; do [ -f "$f" ] && echo "  $(basename "$f" .json): $(lab_record_describe "$f")"; done
fi
[ $FAILS -eq 0 ] && { echo "=== Lab publishers stopped ==="; exit 0; }
echo "=== Stop incomplete; see remaining records above ===" >&2
exit 1
