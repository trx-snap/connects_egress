#!/usr/bin/env bash
set -euo pipefail

HOST_ID="${1:-host_a}"
ROOM_NAME="${2:-room_a}"
VPORT="${3:-9101}"
APORT="${4:-9102}"
PATTERN="${5:-smpte}"
FREQ="${6:-440}"

PID_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.pids"
LOG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.logs"
mkdir -p "${PID_DIR}" "${LOG_DIR}"

V_LOG="${LOG_DIR}/${HOST_ID}_video.log"
A_LOG="${LOG_DIR}/${HOST_ID}_audio.log"
LK_LOG="${LOG_DIR}/${HOST_ID}_lk.log"

echo "=== Launching Publisher: ${HOST_ID} -> ${ROOM_NAME} (Video: :${VPORT}, Audio: :${APORT}) ==="

# 1. Start GStreamer live 720x1280 30fps H.264 video source
gst-launch-1.0 \
  videotestsrc is-live=true pattern="${PATTERN}" ! \
  video/x-raw,format=I420,width=720,height=1280,framerate=30/1 ! \
  timeoverlay valignment=top halignment=left font-desc="Sans 32" ! \
  textoverlay text="${HOST_ID} [720x1280]" valignment=center halignment=center font-desc="Sans 42" ! \
  vtenc_h264 realtime=true max-keyframe-interval=30 allow-frame-reordering=false bitrate=2500 ! \
  h264parse config-interval=-1 ! \
  video/x-h264,stream-format=byte-stream ! \
  tcpserversink host=127.0.0.1 port="${VPORT}" > "${V_LOG}" 2>&1 &
V_PID=$!
echo "${V_PID}" > "${PID_DIR}/${HOST_ID}_video.pid"

# 2. Start GStreamer live 48kHz Opus audio source in Ogg stream
gst-launch-1.0 \
  audiotestsrc is-live=true freq="${FREQ}" ! \
  audio/x-raw,rate=48000,channels=1 ! \
  opusenc bitrate=64000 ! \
  oggmux ! \
  tcpserversink host=127.0.0.1 port="${APORT}" > "${A_LOG}" 2>&1 &
A_PID=$!
echo "${A_PID}" > "${PID_DIR}/${HOST_ID}_audio.pid"

# Wait for TCP sockets to be open and accepting connections
for port in "${VPORT}" "${APORT}"; do
  for i in {1..30}; do
    if nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
done

# 3. Start LiveKit client to join room and stream both tracks
lk room join --dev \
  --identity "${HOST_ID}" \
  --publish "h264://127.0.0.1:${VPORT}" \
  --publish "opus://127.0.0.1:${APORT}" \
  "${ROOM_NAME}" > "${LK_LOG}" 2>&1 &
LK_PID=$!
echo "${LK_PID}" > "${PID_DIR}/${HOST_ID}_lk.pid"

echo "✓ ${HOST_ID} streaming: Video PID=${V_PID} (: ${VPORT}), Audio PID=${A_PID} (: ${APORT}), LK PID=${LK_PID}"
