#!/usr/bin/env bash
# lab/status_lab.sh — report the lab-owned processes by actually checking them:
# each recorded PID is RUNNING (identity matches), GONE or UNKNOWN. Nothing is
# signalled or cleaned. This is process status, not media proof.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "${SCRIPT_DIR}/lib/common.sh"

[ $# -eq 0 ] || { echo "usage: status_lab.sh" >&2; exit 2; }
lab_require_tools jq ps nc || exit 2

echo "=========================================================="
echo "          CONNECTS-EGRESS LAB STATUS (process checks)     "
echo "=========================================================="
if [ -d "$LAB_LEGACY_PID_DIR" ] && ls "$LAB_LEGACY_PID_DIR"/*.pid >/dev/null 2>&1; then
  echo "note: legacy pid files in ${LAB_LEGACY_PID_DIR} are ignored (never adopted or cleaned)"
fi

if lab_tcp_open "$LAB_SERVER_PORT"; then
  echo "tcp/${LAB_SERVER_PORT}: accepting ($(lab_tcp_listener "$LAB_SERVER_PORT"))"
else
  echo "tcp/${LAB_SERVER_PORT}: not accepting"
fi

if ! lab_records_present; then
  echo "no lab-owned records in ${LAB_STATE_DIR}"
  echo "LAB_STATUS=NO_LAB_RECORDS"
  exit 0
fi

EXIT_CODE=0
if [ -f "$LAB_MANIFEST" ]; then
  echo "manifest: mode=$(lab_manifest_mode) hosts=$(lab_manifest_hosts | tr '\n' ' ')launcher_pid=$(jq -r '.launcher_pid // "?"' "$LAB_MANIFEST") started_at=$(jq -r '.started_at // "?"' "$LAB_MANIFEST")"
else
  echo "manifest: missing (records without manifest)"
  EXIT_CODE=1
fi

echo "--- Recorded processes ---"
for f in "$LAB_PID_DIR"/*.json; do
  [ -f "$f" ] || continue
  name="$(basename "$f" .json)"
  desc="$(lab_record_describe "$f")"
  echo "  ${name}: ${desc}  ($(jq -r '.command // "identity unconfirmed"' "$f" 2>/dev/null | cut -c1-90))"
  case "$desc" in RUNNING*) ;; *) EXIT_CODE=1 ;; esac
done

if [ -f "$LAB_MANIFEST" ] && [ "$(lab_manifest_mode)" = "android" ]; then
  echo "--- Android Lab Endpoints ---"
  if nc -z -w 1 127.0.0.1 17880; then echo "  tcp/17880 (LiveKit Signaling): accepting"; else echo "  tcp/17880 (LiveKit Signaling): not accepting"; EXIT_CODE=1; fi
  if nc -z -w 1 127.0.0.1 17883; then echo "  tcp/17883 (Token Issuer): accepting"; else echo "  tcp/17883 (Token Issuer): not accepting"; EXIT_CODE=1; fi
  if nc -z -w 1 127.0.0.1 18080; then echo "  tcp/18080 (HLS Origin): accepting"; else echo "  tcp/18080 (HLS Origin): not accepting"; EXIT_CODE=1; fi
fi

if [ $EXIT_CODE -eq 0 ]; then
  echo "LAB_STATUS=ALL_RECORDED_PROCESSES_RUNNING (process checks only; no media proof)"
else
  echo "LAB_STATUS=ATTENTION some records are GONE/UNKNOWN or the manifest is missing (never auto-repaired)"
fi
exit $EXIT_CODE
