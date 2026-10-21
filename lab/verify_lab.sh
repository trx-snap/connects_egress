#!/usr/bin/env bash
# lab/verify_lab.sh — metadata verification of the running lab. Every host
# expected by the manifest MODE must have running, identity-matching processes
# and be a participant in its room with at least one unmuted H.264 video track
# and one unmuted Opus audio track (omitted protobuf defaults handled).
# Missing hosts/tracks, malformed JSON or CLI failures fail. A PASS proves
# publication metadata only, not media delivery.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "${SCRIPT_DIR}/lib/common.sh"

[ $# -eq 0 ] || { echo "usage: verify_lab.sh" >&2; exit 2; }
lab_require_tools lk jq ps nc || exit 2

FAILED=0
pass() { echo "✓ [PASS] $*"; }
fail() { echo "✗ [FAIL] $*"; FAILED=1; }
finish() {
  echo "=========================================================="
  if [ $FAILED -eq 0 ]; then
    echo "VERIFY_RESULT=PASS scope=METADATA_ONLY (publication metadata; not proof of media delivery)"
    exit 0
  fi
  echo "VERIFY_RESULT=FAIL scope=METADATA_ONLY"
  exit 1
}

echo "=========================================================="
echo "          CONNECTS-EGRESS LAB VERIFICATION (metadata)     "
echo "=========================================================="

MODE="$(lab_manifest_mode)"
if [ -z "$MODE" ]; then
  fail "no valid lab manifest in ${LAB_STATE_DIR} (start with lab/start_lab.sh)"
  finish
fi

if [ "$MODE" = "android" ]; then
  echo "mode=android expected host: android_host in room_android_smoke"

  # 1. UDP relay record
  rc=0
  RELAY_RECORD="$(lab_record_path relay relay)"
  lab_record_check "$RELAY_RECORD" || rc=$?
  if [ $rc -eq 0 ]; then
    pass "udp-relay record running, identity matches (pid $(jq -r '.pid' "$RELAY_RECORD"))"
  else
    fail "udp-relay record not running (rc=${rc})"
  fi

  # 2. Server accepting
  if nc -z -w 1 127.0.0.1 17880; then
    pass "LiveKit server accepting at 127.0.0.1:17880"
  else
    fail "LiveKit server not accepting at 127.0.0.1:17880"
    finish
  fi

  # 3. Check participant metadata
  echo "--- android_host in room_android_smoke ---"
  rc=0
  PJSON="$(lk --url http://127.0.0.1:17880 --dev --quiet room participants get --room room_android_smoke --identity android_host 2>/dev/null)" || rc=$?
  if [ $rc -ne 0 ] || [ -z "$PJSON" ] || ! printf '%s' "$PJSON" | jq -e . >/dev/null 2>&1; then
    fail "android_host participant metadata unavailable or not connected in room_android_smoke"
  elif lab_participant_media_ok "$PJSON" "android_host"; then
    v_dims="$(printf '%s' "$PJSON" | jq -r '[.tracks[]? | select((.type // "AUDIO") == "VIDEO") | "\(.width // "?")x\(.height // "?")"] | join(",")' 2>/dev/null || echo "?")"
    pass "android_host publishes unmuted H.264 video (${v_dims}) and unmuted Opus audio: $(lab_track_summary "$PJSON")"
  else
    fail "android_host lacks required unmuted publications in room_android_smoke: $(lab_track_summary "$PJSON")"
  fi
  finish
fi

if ! lab_mode_valid "$MODE"; then
  fail "no valid lab manifest in ${LAB_STATE_DIR} (start with lab/start_lab.sh)"
  finish
fi
echo "mode=${MODE} expected hosts: $(lab_mode_hosts "$MODE" | awk '{printf "%s ", $1}')"

# Server: lab-owned record running and accepting
rc=0
lab_record_check "$(lab_record_path server server)" || rc=$?
if [ $rc -eq 0 ]; then pass "livekit-server record running, identity matches"; else fail "livekit-server record not running (rc=${rc}: 1=gone, 2=unknown)"; fi
if lab_tcp_open "$LAB_SERVER_PORT"; then
  pass "server accepting at ${LAB_SERVER_URL}"
else
  fail "server not accepting at ${LAB_SERVER_URL}"
  finish
fi

# Hosts derived from MODE
while read -r host room _vp _ap _pat _fr; do
  [ -n "$host" ] || continue
  echo "--- ${host} in ${room} ---"
  for kind in video audio lk; do
    rc=0
    f="$(lab_record_path "$host" "$kind")"
    lab_record_check "$f" || rc=$?
    if [ $rc -eq 0 ]; then
      pass "${host} ${kind} running (pid $(jq -r '.pid' "$f"))"
    else
      fail "${host} ${kind} not running (rc=${rc}: 1=gone, 2=unknown/no record)"
    fi
  done
  rc=0
  PJSON="$(lab_lk_json room participants get --room "$room" --identity "$host" 2>/dev/null)" || rc=$?
  if [ $rc -ne 0 ]; then
    fail "${host} participant metadata unavailable or malformed in ${room}"
  elif lab_participant_media_ok "$PJSON" "$host"; then
    pass "${host} publishes unmuted H.264 video and unmuted Opus audio: $(lab_track_summary "$PJSON")"
  else
    fail "${host} lacks required unmuted publications in ${room}: $(lab_track_summary "$PJSON")"
  fi
done <<EOF_HOSTS
$(lab_mode_hosts "$MODE")
EOF_HOSTS

finish
