#!/usr/bin/env bash
# lab/publishers/stream_host.sh HOST ROOM VIDEO_PORT AUDIO_PORT PATTERN FREQ
#
# Launches one live synthetic host: a 720x1280 @ 30 fps H.264 video source and
# a 48 kHz Opus audio source (GStreamer, tcpserversink) plus one lk publisher,
# records each PID immediately under lab/.logs/lab/pids, waits for the source
# sockets, then returns. Normally invoked by start_lab.sh after the server is up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

usage() {
  cat >&2 <<'EOF_USAGE'
usage: stream_host.sh HOST ROOM VIDEO_PORT AUDIO_PORT PATTERN FREQ
Arguments must match the frozen host table (see lab/lib/common.sh).
EOF_USAGE
  exit 2
}

[ $# -eq 6 ] || usage
HOST_ID="$1" ROOM_NAME="$2" VPORT="$3" APORT="$4" PATTERN="$5" FREQ="$6"
lab_host_valid "$HOST_ID" || { lab_err "unknown host: ${HOST_ID}"; usage; }
SPEC="$(lab_host_spec "$HOST_ID")"
if [ "$SPEC" != "${HOST_ID} ${ROOM_NAME} ${VPORT} ${APORT} ${PATTERN} ${FREQ}" ]; then
  lab_err "arguments do not match the frozen mapping for ${HOST_ID}: expected '${SPEC}'"
  usage
fi
lab_require_tools gst-launch-1.0 lk jq nc ps || exit 2

V_RECORD="$(lab_record_path "$HOST_ID" video)"
A_RECORD="$(lab_record_path "$HOST_ID" audio)"
LK_RECORD="$(lab_record_path "$HOST_ID" lk)"
for f in "$V_RECORD" "$A_RECORD" "$LK_RECORD"; do
  if [ -f "$f" ]; then
    lab_err "record already present for ${HOST_ID}: ${f} ($(lab_record_describe "$f")); stop it first"
    exit 1
  fi
done
for p in "$VPORT" "$APORT"; do
  if lab_tcp_open "$p"; then
    lab_err "PORT_IN_USE tcp/${p}: $(lab_tcp_listener "$p"); existing processes are never adopted or stopped"
    exit 1
  fi
done
lab_ensure_dirs
V_LOG="${LAB_LOG_DIR}/${HOST_ID}_video.log"
A_LOG="${LAB_LOG_DIR}/${HOST_ID}_audio.log"
LK_LOG="${LAB_LOG_DIR}/${HOST_ID}_lk.log"

# On failure, stop only what this invocation launched; keep records of survivors.
DONE=""
host_fail() {
  local rc=0 f
  trap - INT TERM EXIT
  lab_err "${HOST_ID}: $1"
  for f in "$LK_RECORD" "$A_RECORD" "$V_RECORD"; do
    [ -f "$f" ] || continue
    lab_record_stop "$f" || rc=1
  done
  [ $rc -eq 0 ] || lab_err "${HOST_ID}: cleanup incomplete; remaining records retained in ${LAB_PID_DIR}"
  exit 1
}
trap 'host_fail "interrupted"' INT TERM
trap '[ -n "$DONE" ] || host_fail "unexpected exit"' EXIT

echo "=== Launching publisher ${HOST_ID} -> ${ROOM_NAME} (video :${VPORT}, audio :${APORT}, ${PATTERN}, ${FREQ} Hz) ==="

# 1. Live 720x1280 30fps H.264 video source
gst-launch-1.0 \
  videotestsrc is-live=true pattern="${PATTERN}" ! \
  video/x-raw,format=I420,width=720,height=1280,framerate=30/1 ! \
  timeoverlay valignment=top halignment=left font-desc="Sans 32" ! \
  textoverlay text="${HOST_ID} [720x1280]" valignment=center halignment=center font-desc="Sans 42" ! \
  vtenc_h264 realtime=true max-keyframe-interval=30 allow-frame-reordering=false bitrate=2500 ! \
  h264parse config-interval=-1 ! \
  video/x-h264,stream-format=byte-stream ! \
  tcpserversink host=127.0.0.1 port="${VPORT}" </dev/null >>"${V_LOG}" 2>&1 &
V_PID=$!
lab_record_write "$V_RECORD" "$V_PID" video "$HOST_ID" gst-launch-1.0 \
  || host_fail "video source pid ${V_PID} did not start; see ${V_LOG}"

# 2. Live 48kHz Opus audio source in Ogg
gst-launch-1.0 \
  audiotestsrc is-live=true freq="${FREQ}" ! \
  audio/x-raw,rate=48000,channels=1 ! \
  opusenc bitrate=64000 ! \
  oggmux ! \
  tcpserversink host=127.0.0.1 port="${APORT}" </dev/null >>"${A_LOG}" 2>&1 &
A_PID=$!
lab_record_write "$A_RECORD" "$A_PID" audio "$HOST_ID" gst-launch-1.0 \
  || host_fail "audio source pid ${A_PID} did not start; see ${A_LOG}"

# 3. Source sockets accepting (short elapsed-time waits)
lab_tcp_wait "$VPORT" 3 || host_fail "video socket :${VPORT} not accepting within 3s; see ${V_LOG}"
lab_tcp_wait "$APORT" 3 || host_fail "audio socket :${APORT} not accepting within 3s; see ${A_LOG}"

# 4. lk publisher: explicit loopback URL, --dev credentials, 30 fps
lk --url "$LAB_SERVER_URL" --dev room join \
  --identity "${HOST_ID}" \
  --publish "h264://127.0.0.1:${VPORT}" \
  --publish "opus://127.0.0.1:${APORT}" \
  --fps 30 \
  "${ROOM_NAME}" </dev/null >>"${LK_LOG}" 2>&1 &
LK_PID=$!
lab_record_write "$LK_RECORD" "$LK_PID" lk "$HOST_ID" "room join" \
  || host_fail "lk publisher pid ${LK_PID} did not start; see ${LK_LOG}"

DONE=1
echo "✓ ${HOST_ID} launched: video pid ${V_PID} (:${VPORT}), audio pid ${A_PID} (:${APORT}), lk pid ${LK_PID} (metadata not yet verified)"
