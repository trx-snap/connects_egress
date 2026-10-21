#!/usr/bin/env bash
# lab/publishers/stop_host.sh HOST — stop the lab-owned processes recorded for
# one host (lk, audio, video). Each recorded PID is validated (uid, start time,
# command) before a single TERM; unknown identity is reported, not signalled.
# Records are removed only when the exit is confirmed. No SIGKILL, no pkill.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

if [ $# -ne 1 ] || ! lab_host_valid "$1"; then
  echo "usage: stop_host.sh host_a|host_b|host_c|host_d" >&2
  exit 2
fi
HOST_ID="$1"
lab_require_tools jq ps || exit 2

FAILS=0
FOUND=0
for kind in lk audio video; do
  f="$(lab_record_path "$HOST_ID" "$kind")"
  [ -f "$f" ] || continue
  FOUND=1
  pid="$(jq -r '.pid // "?"' "$f" 2>/dev/null || echo "?")"
  rc=0
  lab_record_stop "$f" || rc=$?
  case $rc in
    0) echo "stopped ${HOST_ID} ${kind} (pid ${pid})" ;;
    1) echo "SURVIVOR ${HOST_ID} ${kind} pid ${pid}: still running after TERM; record retained"; FAILS=1 ;;
    *) echo "UNKNOWN ${HOST_ID} ${kind} pid ${pid}: identity not confirmed; not signalled; record retained"; FAILS=1 ;;
  esac
done
[ $FOUND -eq 1 ] || echo "no lab-owned records for ${HOST_ID} in ${LAB_PID_DIR} (legacy ${LAB_LEGACY_PID_DIR} is never used)"
exit $FAILS
