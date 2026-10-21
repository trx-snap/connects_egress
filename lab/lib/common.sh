#!/usr/bin/env bash
# lab/lib/common.sh — small shared helpers for the connects-egress macOS lab.
#
# Sourced by the lab scripts (Bash 3.2; jq, lsof, nc, ps only). It owns:
#   - the frozen host/room/port/pattern/frequency mapping and mode -> hosts
#   - the lab-owned state namespace under lab/.logs/lab (never the legacy
#     lab/publishers/.pids or .logs)
#   - PID records with process identity (pid, uid, start time, command) and
#     the checks used before any record is signalled
#   - a few socket and lk helpers
# Records are JSON parsed with jq; they are never evaluated as shell.

if [ -n "${LAB_COMMON_LOADED:-}" ]; then
  return 0 2>/dev/null || exit 0
fi
LAB_COMMON_LOADED=1

LAB_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "${LAB_LIB_DIR}/.." && pwd)"
LAB_STATE_DIR="${LAB_DIR}/.logs/lab"        # lab-owned namespace (git-ignored)
LAB_PID_DIR="${LAB_STATE_DIR}/pids"
LAB_LOG_DIR="${LAB_STATE_DIR}/logs"
LAB_MANIFEST="${LAB_STATE_DIR}/manifest.json"
LAB_CONFIG_FILE="${LAB_DIR}/livekit.yaml"
LAB_LEGACY_PID_DIR="${LAB_DIR}/publishers/.pids"

LAB_SERVER_IP="127.0.0.1"
LAB_SERVER_PORT=7880
LAB_SERVER_URL="http://127.0.0.1:7880"
LAB_STOP_WAIT=5                     # seconds to wait after TERM before giving up

lab_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
lab_info() { printf '%s\n' "$*"; }
lab_err() { printf 'ERROR: %s\n' "$*" >&2; }

lab_trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

lab_is_uint() {
  case "$1" in
    '' | *[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

lab_is_pid() { lab_is_uint "$1" && [ "${#1}" -le 6 ] && [ "$1" -ge 1 ]; }

lab_require_tools() {
  local t missing=""
  for t in "$@"; do
    command -v "$t" >/dev/null 2>&1 || missing="${missing} ${t}"
  done
  [ -z "$missing" ] || { lab_err "missing required tools:${missing}"; return 1; }
}

# ---------------------------------------------------------------------------
# Mode and host mapping (frozen)
# ---------------------------------------------------------------------------

LAB_HOST_TABLE='host_a room_a 9101 9102 smpte 440
host_b room_b 9103 9104 ball 880
host_c room_c 9105 9106 pinwheel 660
host_d room_d 9107 9108 spokes 550'

lab_mode_count() {
  case "$1" in
    solo) echo 1 ;;
    1v1) echo 2 ;;
    tryon) echo 3 ;;
    quadro) echo 4 ;;
    *) return 1 ;;
  esac
}

lab_mode_valid() { lab_mode_count "$1" >/dev/null 2>&1; }

# lab_mode_hosts MODE -> one line per selected host:
#   "host room video_port audio_port pattern frequency"
lab_mode_hosts() {
  local n
  n="$(lab_mode_count "$1")" || return 1
  printf '%s\n' "$LAB_HOST_TABLE" | head -n "$n"
}

lab_host_valid() {
  case "$1" in
    host_a | host_b | host_c | host_d) return 0 ;;
    *) return 1 ;;
  esac
}

# lab_host_spec HOST -> the frozen specification line for HOST
lab_host_spec() {
  lab_host_valid "$1" || return 1
  printf '%s\n' "$LAB_HOST_TABLE" | grep "^$1 "
}

# ---------------------------------------------------------------------------
# Lab-owned state: manifest and PID records
# ---------------------------------------------------------------------------

lab_ensure_dirs() { mkdir -p "$LAB_PID_DIR" "$LAB_LOG_DIR"; }

# lab_write_file PATH CONTENT  (write to a temp file, then rename into place)
lab_write_file() {
  local tmp="$1.tmp.$$"
  printf '%s\n' "$2" >"$tmp" && mv -f "$tmp" "$1"
}

lab_record_path() { # HOST KIND   (server record: lab_record_path server server)
  if [ "$1" = "server" ]; then
    printf '%s/server.json' "$LAB_PID_DIR"
  else
    printf '%s/%s_%s.json' "$LAB_PID_DIR" "$1" "$2"
  fi
}

# lab_records_present -> 0 when the manifest or any PID record exists
lab_records_present() {
  [ -f "$LAB_MANIFEST" ] && return 0
  [ -n "$(lab_record_files)" ]
}

lab_record_files() { ls "$LAB_PID_DIR"/*.json 2>/dev/null || true; }

lab_manifest_mode() { jq -r '.mode // empty' "$LAB_MANIFEST" 2>/dev/null || true; }
lab_manifest_hosts() { jq -r '.hosts[]? // empty' "$LAB_MANIFEST" 2>/dev/null || true; }

# lab_ps_field PID FIELD -> trimmed value; rc 1 when ps reports nothing for PID
lab_ps_field() {
  local v
  v="$(ps -ww -p "$1" -o "$2=" 2>/dev/null)" || return 1
  lab_trim "$v"
}

# ps itself must be working before "no such process" can be trusted
lab_ps_sane() { ps -p "$$" -o pid= >/dev/null 2>&1; }

# lab_record_write PATH PID ROLE HOST EXPECT
# Writes the record with the pid immediately, then fills in uid, start time and
# command once the live command line contains EXPECT (waits up to ~1s for exec).
#   0 identity recorded; 1 process gone or command never matched (pid retained)
lab_record_write() {
  local path="$1" pid="$2" role="$3" host="$4" expect="$5" i=0 cmd="" uid start json
  lab_ensure_dirs
  json="$(jq -n --arg pid "$pid" --arg role "$role" --arg host "$host" --arg at "$(lab_now)" \
    '{pid: ($pid | tonumber), role: $role, host: $host, recorded_at: $at,
      uid: null, start_time: null, command: null}')" || return 1
  lab_write_file "$path" "$json"
  while [ $i -lt 20 ]; do
    cmd="$(lab_ps_field "$pid" command)" || return 1
    case "$cmd" in *"$expect"*) break ;; esac
    i=$((i + 1))
    sleep 0.05
  done
  case "$cmd" in *"$expect"*) ;; *) return 1 ;; esac
  uid="$(lab_ps_field "$pid" uid)" || return 1
  start="$(lab_ps_field "$pid" lstart)" || return 1
  [ -n "$uid" ] && [ -n "$start" ] || return 1
  json="$(jq --arg uid "$uid" --arg start "$start" --arg cmd "$cmd" \
    '.uid = ($uid | tonumber) | .start_time = $start | .command = $cmd' "$path")" || return 1
  lab_write_file "$path" "$json"
}

# lab_record_check PATH -> 0 running and identity matches
#                          1 process confirmed gone
#                          2 unknown: identity unconfirmed/mismatched or inspection failed
lab_record_check() {
  local r="$1" pid uid start cmd lstat luid lstart lcmd
  [ -f "$r" ] || return 2
  pid="$(jq -r '.pid // empty' "$r" 2>/dev/null)" || return 2
  lab_is_pid "$pid" || return 2
  if ! lstat="$(lab_ps_field "$pid" stat)" || [ -z "$lstat" ]; then
    lab_ps_sane && return 1
    return 2
  fi
  case "$lstat" in Z*) return 1 ;; esac
  uid="$(jq -r '.uid // empty' "$r" 2>/dev/null)" || return 2
  start="$(jq -r '.start_time // empty' "$r" 2>/dev/null)" || return 2
  cmd="$(jq -r '.command // empty' "$r" 2>/dev/null)" || return 2
  [ -n "$uid" ] && [ -n "$start" ] && [ -n "$cmd" ] || return 2
  luid="$(lab_ps_field "$pid" uid)" || return 2
  lstart="$(lab_ps_field "$pid" lstart)" || return 2
  lcmd="$(lab_ps_field "$pid" command)" || return 2
  [ "$luid" = "$uid" ] && [ "$lstart" = "$start" ] && [ "$lcmd" = "$cmd" ] || return 2
  return 0
}

# lab_record_describe PATH -> "RUNNING pid" | "GONE pid" | "UNKNOWN pid" | "NONE -"
lab_record_describe() {
  local rc=0 pid
  [ -f "$1" ] || { echo "NONE -"; return 0; }
  pid="$(jq -r '.pid // "?"' "$1" 2>/dev/null || echo "?")"
  lab_record_check "$1" || rc=$?
  case $rc in
    0) echo "RUNNING ${pid}" ;;
    1) echo "GONE ${pid}" ;;
    *) echo "UNKNOWN ${pid}" ;;
  esac
}

# lab_record_stop PATH [WAIT]
# Validates identity, sends TERM once, waits up to WAIT seconds (elapsed) for
# the process to be gone. Never SIGKILL. Removes the record only when the exit
# is confirmed; otherwise the record is retained.
#   0 confirmed gone (record removed); 1 still running after WAIT;
#   2 identity unknown/mismatched or inspection failed (not signalled)
lab_record_stop() {
  local r="$1" wait_secs="${2:-$LAB_STOP_WAIT}" rc=0 pid t0
  [ -f "$r" ] || return 0
  pid="$(jq -r '.pid // "?"' "$r" 2>/dev/null || echo "?")"
  lab_record_check "$r" || rc=$?
  case $rc in
    1) rm -f "$r"; return 0 ;;
    2) lab_err "not signalling pid ${pid}: identity unknown or mismatched; record retained ($r)"; return 2 ;;
  esac
  if ! kill -TERM "$pid" 2>/dev/null; then
    rc=0
    lab_record_check "$r" || rc=$?
    [ $rc -eq 1 ] && { rm -f "$r"; return 0; }
    lab_err "TERM to pid ${pid} failed; record retained ($r)"
    return 2
  fi
  t0=$SECONDS
  while [ $((SECONDS - t0)) -le "$wait_secs" ]; do
    rc=0
    lab_record_check "$r" || rc=$?
    case $rc in
      1) rm -f "$r"; return 0 ;;
      2) lab_err "pid ${pid} identity became unknown after TERM; record retained ($r)"; return 2 ;;
    esac
    sleep 0.1
  done
  lab_err "pid ${pid} still running ${wait_secs}s after TERM (no SIGKILL); record retained ($r)"
  return 1
}

# ---------------------------------------------------------------------------
# Sockets and LiveKit CLI
# ---------------------------------------------------------------------------

lab_tcp_open() { nc -z -w 1 "$LAB_SERVER_IP" "$1" >/dev/null 2>&1; }

# lab_tcp_wait PORT SECONDS -> 0 when the port accepts within SECONDS (elapsed)
lab_tcp_wait() {
  local t0=$SECONDS
  while :; do
    lab_tcp_open "$1" && return 0
    [ $((SECONDS - t0)) -lt "$2" ] || return 1
    sleep 0.1
  done
}

# lab_tcp_listener PORT -> "pid command" of a visible listener, or a note
lab_tcp_listener() {
  local pid
  pid="$(lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | head -n 1 || true)"
  if [ -n "$pid" ]; then
    printf 'pid %s (%s)' "$pid" "$(lab_ps_field "$pid" command 2>/dev/null || echo '?')"
  else
    printf 'listener pid not visible to lsof'
  fi
}

# Every lk call uses the explicit loopback URL and --dev credentials.
lab_lk() { lk --url "$LAB_SERVER_URL" --dev "$@"; }

# lab_lk_json ARGS... -> strict JSON on stdout; 1 on CLI failure or malformed JSON
lab_lk_json() {
  local out
  out="$(lk --url "$LAB_SERVER_URL" --dev --quiet "$@")" || return 1
  printf '%s' "$out" | jq -e . >/dev/null 2>&1 || return 1
  printf '%s\n' "$out"
}

# lab_participant_media_ok JSON IDENTITY
# 0 when identity matches and there is at least one unmuted H.264 VIDEO track and
# one unmuted Opus AUDIO track. Protobuf JSON omits defaults: missing .type is
# AUDIO and missing .muted is false.
lab_participant_media_ok() {
  printf '%s' "$1" | jq -e --arg id "$2" '
    def unmuted: ((.muted // false) == false);
    def mt: ((.mimeType // "") | ascii_downcase);
    def ttype: (.type // "AUDIO");
    (type == "object") and (.identity == $id)
    and (([.tracks[]? | select(ttype == "VIDEO" and mt == "video/h264" and unmuted)] | length) >= 1)
    and (([.tracks[]? | select(ttype == "AUDIO" and mt == "audio/opus" and unmuted)] | length) >= 1)
  ' >/dev/null 2>&1
}

lab_track_summary() {
  printf '%s' "$1" | jq -r '
    [.tracks[]? | "\(.type // "AUDIO"):\(.mimeType // "?"):muted=\(.muted // false)"]
    | if length == 0 then "no tracks" else join(" ") end' 2>/dev/null || echo "unparseable"
}
