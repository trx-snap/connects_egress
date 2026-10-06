#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"
mkdir -p "${FIXTURES_DIR}"

echo "=== Generating Synthetic 720x1280 Fixtures for Connects Egress Lab ==="

# Hosts definition: name pattern freq
HOSTS=(
  "host_a smptebars 440"
  "host_b testsrc 880"
  "host_c smptebars 660"
  "host_d testsrc 550"
)

for entry in "${HOSTS[@]}"; do
  set -- $entry
  name="$1"
  pattern="$2"
  freq="$3"

  v_file="${FIXTURES_DIR}/${name}.h264"
  a_file="${FIXTURES_DIR}/${name}.ogg"

  if [[ -f "${v_file}" && -f "${a_file}" ]]; then
    echo "✓ Fixtures for ${name} already exist."
    continue
  fi

  echo "Generating ${name} (Video: 720x1280 @ 30fps H.264, Audio: 48kHz Opus @ ${freq}Hz)..."

  # Generate 10-second 720x1280 30fps H.264 GOP=30
  ffmpeg -y -f lavfi -i "${pattern}=size=720x1280:rate=30:duration=10" \
    -c:v libx264 -preset ultrafast -tune zerolatency -pix_fmt yuv420p -b:v 2500k \
    -g 30 -keyint_min 30 -sc_threshold 0 \
    -bsf:v h264_mp4toannexb "${v_file}" > /dev/null 2>&1

  # Generate 10-second 48kHz Opus
  ffmpeg -y -f lavfi -i "sine=frequency=${freq}:sample_rate=48000:duration=10" \
    -c:a libopus -b:a 64k "${a_file}" > /dev/null 2>&1

  echo "✓ Completed ${name}: ${v_file}, ${a_file}"
done

echo "=== All Fixtures Ready in ${FIXTURES_DIR} ==="
