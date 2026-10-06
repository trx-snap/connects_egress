#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo "          CONNECTS-EGRESS LAB VERIFICATION SUITE          "
echo "=========================================================="

FAILED=0

# 1. Server check
if nc -z 127.0.0.1 7880 >/dev/null 2>&1; then
  echo "✓ [PASS] LiveKit Server reachable at ws://127.0.0.1:7880"
else
  echo "✗ [FAIL] LiveKit Server is not reachable"
  exit 1
fi

check_host() {
  local room="$1"
  local host="$2"
  echo ""
  echo "--- Checking ${room} (${host}) ---"
  local raw_json
  raw_json="$(lk room participants get --room "${room}" --identity "${host}" --dev 2>/dev/null | sed -n '/^{/,$p' || true)"
  if [[ -z "${raw_json}" ]] || ! echo "${raw_json}" | jq -e '.isPublisher' >/dev/null 2>&1; then
    echo "✗ [FAIL] ${host} is not active in ${room}"
    FAILED=1
    return
  fi

  echo "✓ [PASS] ${host} is ACTIVE publisher in ${room}"
  while IFS= read -r line; do
    echo "         - ${line}"
  done < <(echo "${raw_json}" | jq -r '.tracks[] | "\(.mimeType): sid=\(.sid) type=\(.type // "AUDIO")"')

  local has_video has_audio
  has_video="$(echo "${raw_json}" | jq -r '[.tracks[] | select(.mimeType=="video/H264")] | length')"
  has_audio="$(echo "${raw_json}" | jq -r '[.tracks[] | select(.mimeType=="audio/opus")] | length')"

  if [[ "${has_video}" -ge 1 && "${has_audio}" -ge 1 ]]; then
    echo "✓ [PASS] Both H.264 video and Opus audio tracks confirmed active"
  else
    echo "✗ [FAIL] Missing required tracks (video count: ${has_video}, audio count: ${has_audio})"
    FAILED=1
  fi
}

check_host "room_a" "host_a"
check_host "room_b" "host_b"

echo ""
echo "=========================================================="
if [[ "${FAILED}" -eq 0 ]]; then
  echo "  >>> ALL LAB VERIFICATIONS PASSED: LAB IS READY <<<      "
else
  echo "  >>> LAB VERIFICATION FAILED <<<                         "
  exit 1
fi
echo "=========================================================="
