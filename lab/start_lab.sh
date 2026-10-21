#!/usr/bin/env bash
# lab/start_lab.sh [MODE] — start the local LiveKit server, create the mode's
# rooms, launch the live publishers, check readiness, then return.
#
# Modes: solo=host_a; 1v1=host_a,host_b (default); tryon=+host_c; quadro=+host_d.
# Everything launched is recorded under lab/.logs/lab (pids/, logs/, manifest).
# Existing listeners cause a clear failure; nothing is adopted, stopped or
# replaced. A failed or interrupted start stops only what it launched.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "${SCRIPT_DIR}/lib/common.sh"

usage() {
  echo "usage: start_lab.sh [solo|1v1|tryon|quadro|android]   (default 1v1)" >&2
  exit 2
}

[ $# -le 1 ] || usage
MODE="${1:-1v1}"
if [ "$MODE" = "android" ]; then
  lab_require_tools lk jq nc ps python3 || exit 2
else
  lab_mode_valid "$MODE" || { lab_err "unknown mode: ${MODE}"; usage; }
  lab_require_tools lk livekit-server gst-launch-1.0 jq nc ps || exit 2
  [ -f "$LAB_CONFIG_FILE" ] || { lab_err "missing server config ${LAB_CONFIG_FILE}"; exit 2; }
fi

echo "=========================================================="
echo "          STARTING CONNECTS-EGRESS LAB (${MODE})          "
echo "=========================================================="

if lab_records_present; then
  lab_err "lab-owned records already exist in ${LAB_STATE_DIR}:"
  for f in "$LAB_PID_DIR"/*.json; do [ -f "$f" ] && echo "  $(basename "$f" .json): $(lab_record_describe "$f")" >&2; done
  lab_err "inspect with lab/status_lab.sh and clear with lab/stop_lab.sh --all before starting again"
  exit 1
fi

if [ "$MODE" = "android" ]; then
  # 1. Verify backend services in Linux guest
  if ! nc -z -w 1 127.0.0.1 17880; then
    lab_err "LiveKit server not accepting on 127.0.0.1:17880"
    exit 1
  fi
  if ! nc -z -w 1 127.0.0.1 17883; then
    lab_err "Token server not accepting on 127.0.0.1:17883"
    exit 1
  fi
  if ! nc -z -w 1 127.0.0.1 18080; then
    lab_err "Supervisor not accepting on 127.0.0.1:18080"
    exit 1
  fi

  # 2. Check if UDP 17882 is already bound on LAN IP
  if lsof -nP -iUDP:17882 2>/dev/null | grep -q "192.168.0.136:17882"; then
    lab_err "PORT_IN_USE udp/17882 on 192.168.0.136: $(lsof -nP -iUDP:17882 2>/dev/null | head -n 1)"
    exit 1
  fi

  lab_ensure_dirs
  RELAY_DIR="/tmp/connects-egress-relay-$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$RELAY_DIR"
  cat << 'EOF_RELAY' > "$RELAY_DIR/udp_relay.py"
#!/usr/bin/env python3
import socket, select, sys, time, signal

LAN_IP = sys.argv[1] if len(sys.argv) > 1 else "192.168.0.136"
LAN_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 17882
DEST_IP = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
DEST_PORT = int(sys.argv[4]) if len(sys.argv) > 4 else 17882

sock_lan = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_lan.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock_lan.bind((LAN_IP, LAN_PORT))

client_sockets = {}
loop_sockets = {}

def term_handler(signum, frame):
    sys.exit(0)

signal.signal(signal.SIGTERM, term_handler)
signal.signal(signal.SIGINT, term_handler)
signal.signal(signal.SIGHUP, signal.SIG_IGN)

print(f"[UDP_RELAY] Listening on {LAN_IP}:{LAN_PORT} <-> {DEST_IP}:{DEST_PORT}", flush=True)

try:
    while True:
        readers = [sock_lan] + [s for (_, s) in loop_sockets.values()]
        r, _, _ = select.select(readers, [], [], 1.0)
        now = time.time()

        if sock_lan in r:
            data, client_addr = sock_lan.recvfrom(65535)
            if client_addr not in client_sockets:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                client_sockets[client_addr] = (s, now)
                loop_sockets[s.fileno()] = (client_addr, s)
                print(f"[UDP_RELAY] New peer connection from {client_addr}", flush=True)
            else:
                s, _ = client_sockets[client_addr]
                client_sockets[client_addr] = (s, now)
            s.sendto(data, (DEST_IP, DEST_PORT))

        for fd in [sock.fileno() for sock in r if sock is not sock_lan]:
            if fd in loop_sockets:
                c_addr, s = loop_sockets[fd]
                try:
                    data, _ = s.recvfrom(65535)
                    sock_lan.sendto(data, c_addr)
                except Exception:
                    pass

        # Clean idle sockets after 60s
        for c_addr in list(client_sockets.keys()):
            s, last_time = client_sockets[c_addr]
            if now - last_time > 60:
                print(f"[UDP_RELAY] Cleaning up peer connection from {c_addr}", flush=True)
                del loop_sockets[s.fileno()]
                del client_sockets[c_addr]
                s.close()
finally:
    sock_lan.close()
    for _, s in loop_sockets.values():
        s.close()
EOF_RELAY
  chmod +x "$RELAY_DIR/udp_relay.py"

  RELAY_RECORD="$(lab_record_path relay relay)"
  RELAY_LOG="${LAB_LOG_DIR}/udp-relay.log"

  fail_android_startup() {
    trap - INT TERM EXIT
    lab_err "startup failed: $1"
    [ -f "$RELAY_RECORD" ] && lab_record_stop "$RELAY_RECORD" || true
    rm -rf "$RELAY_DIR" "$LAB_MANIFEST"
    exit 1
  }
  trap 'fail_android_startup "interrupted"' INT TERM
  trap 'fail_android_startup "unexpected exit"' EXIT

  echo "Starting LiveKit UDP relay on 192.168.0.136:17882..."
  nohup python3 -u "$RELAY_DIR/udp_relay.py" "192.168.0.136" "17882" "127.0.0.1" "17882" </dev/null >>"$RELAY_LOG" 2>&1 &
  RELAY_PID=$!
  disown $RELAY_PID 2>/dev/null || true
  lab_record_write "$RELAY_RECORD" "$RELAY_PID" relay android udp_relay.py \
    || fail_android_startup "udp_relay.py pid ${RELAY_PID} did not start; see ${RELAY_LOG}"

  sleep 1
  rc=0
  lab_record_check "$RELAY_RECORD" || rc=$?
  [ $rc -eq 0 ] || fail_android_startup "udp_relay.py not running (check rc=${rc}); see ${RELAY_LOG}"

  # Ensure room exists
  lk room create --url http://127.0.0.1:17880 --dev room_android_smoke >/dev/null 2>&1 || true

  lab_write_file "$LAB_MANIFEST" "$(jq -n --arg mode "$MODE" --arg pid "$$" --arg at "$(lab_now)" \
    --arg url "http://127.0.0.1:17880" --arg cfg "$RELAY_DIR" --arg hosts "android_host" \
    '{mode: $mode, hosts: ["android_host"], launcher_pid: ($pid | tonumber),
      started_at: $at, server_url: $url, config: $cfg}')"

  DONE=1
  trap - INT TERM EXIT
  echo "=========================================================="
  echo "LAB_STARTED mode=android hosts=android_host relay_pid=${RELAY_PID}"
  echo "records: ${LAB_PID_DIR}   logs: ${LAB_LOG_DIR}"
  echo "LiveKit UDP relay active on 192.168.0.136:17882"
  echo "Stage ready for Android publisher in room_android_smoke."
  echo "Next: start Android publisher, then lab/verify_lab.sh"
  exit 0
fi

HOST_SPECS="$(lab_mode_hosts "$MODE")"
HOST_LIST="$(lab_trim "$(printf '%s\n' "$HOST_SPECS" | awk '{printf "%s ", $1}')")"

# Ports must be free: existing listeners are reported, never adopted or stopped.
BUSY=0
for p in "$LAB_SERVER_PORT" $(printf '%s\n' "$HOST_SPECS" | awk '{print $3; print $4}'); do
  if lab_tcp_open "$p"; then
    lab_err "PORT_IN_USE tcp/${p}: $(lab_tcp_listener "$p")"
    BUSY=1
  fi
done
[ $BUSY -eq 0 ] || { lab_err "stop the listed processes manually, then retry"; exit 1; }

lab_ensure_dirs
lab_write_file "$LAB_MANIFEST" "$(jq -n --arg mode "$MODE" --arg pid "$$" --arg at "$(lab_now)" \
  --arg url "$LAB_SERVER_URL" --arg cfg "$LAB_CONFIG_FILE" --arg hosts "$HOST_LIST" \
  '{mode: $mode, hosts: ($hosts | split(" ")), launcher_pid: ($pid | tonumber),
    started_at: $at, server_url: $url, config: $cfg}')"
SERVER_RECORD="$(lab_record_path server server)"
SERVER_LOG="${LAB_LOG_DIR}/livekit-server.log"

# Failure/interruption: stop what this invocation launched (publishers, then
# server). Unconfirmed exits keep their records and are reported.
DONE=""
fail_startup() {
  local host kind f rc=0
  trap - INT TERM EXIT
  lab_err "startup failed: $1"
  echo "stopping processes launched by this invocation..." >&2
  for host in $HOST_LIST; do
    for kind in lk audio video; do
      f="$(lab_record_path "$host" "$kind")"
      [ -f "$f" ] || continue
      lab_record_stop "$f" || rc=1
    done
  done
  if [ -f "$SERVER_RECORD" ]; then
    lab_record_stop "$SERVER_RECORD" || rc=1
  fi
  if [ $rc -eq 0 ] && [ -z "$(lab_record_files)" ]; then
    rm -f "$LAB_MANIFEST"
    echo "cleanup confirmed; no lab-owned processes remain (logs kept in ${LAB_LOG_DIR})" >&2
  else
    echo "CLEANUP_INCOMPLETE records retained in ${LAB_PID_DIR}:" >&2
    for f in "$LAB_PID_DIR"/*.json; do [ -f "$f" ] && echo "  $(basename "$f" .json): $(lab_record_describe "$f")" >&2; done
  fi
  exit 1
}
trap 'fail_startup "interrupted"' INT TERM
trap '[ -n "$DONE" ] || fail_startup "unexpected exit"' EXIT

# 1. LiveKit server (lab-owned, loopback config)
echo "Starting LiveKit server on 127.0.0.1:${LAB_SERVER_PORT}..."
livekit-server --dev --config "$LAB_CONFIG_FILE" </dev/null >>"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
lab_record_write "$SERVER_RECORD" "$SERVER_PID" server - livekit-server \
  || fail_startup "livekit-server pid ${SERVER_PID} did not start; see ${SERVER_LOG}"
lab_tcp_wait "$LAB_SERVER_PORT" 5 \
  || fail_startup "livekit-server not accepting on 127.0.0.1:${LAB_SERVER_PORT} within 5s; see ${SERVER_LOG}"
echo "✓ LiveKit server started (pid ${SERVER_PID})"

# 2. Rooms (positional room-name syntax, explicit loopback URL, --dev)
echo "Creating rooms..."
LK_LOG="${LAB_LOG_DIR}/lk-rooms.log"
for room in $(printf '%s\n' "$HOST_SPECS" | awk '{print $2}'); do
  lab_lk room create "$room" >>"$LK_LOG" 2>&1 || fail_startup "lk room create ${room} failed; see ${LK_LOG}"
  echo "✓ room ${room}"
done

# 3. Publishers (live sources; no fixtures)
echo "Launching publishers..."
while read -r host room vp ap pat fr; do
  [ -n "$host" ] || continue
  "${SCRIPT_DIR}/publishers/stream_host.sh" "$host" "$room" "$vp" "$ap" "$pat" "$fr" \
    || fail_startup "publisher ${host} failed to launch"
done <<EOF_HOSTS
$HOST_SPECS
EOF_HOSTS

# 4. Readiness: every recorded process still running after a short settle
sleep 2
for f in "$LAB_PID_DIR"/*.json; do
  [ -f "$f" ] || continue
  rc=0
  lab_record_check "$f" || rc=$?
  [ $rc -eq 0 ] || fail_startup "$(basename "$f" .json) is not running (check rc=${rc}); see ${LAB_LOG_DIR}"
done
lab_tcp_open "$LAB_SERVER_PORT" || fail_startup "server stopped accepting on :${LAB_SERVER_PORT}"

DONE=1
trap - INT TERM EXIT
echo "=========================================================="
echo "LAB_STARTED mode=${MODE} hosts=$(printf '%s' "$HOST_LIST" | tr ' ' ',') server_pid=${SERVER_PID}"
echo "records: ${LAB_PID_DIR}   logs: ${LAB_LOG_DIR}"
echo "Readiness checked: processes running and sockets accepting (no media proof)."
echo "Next: lab/verify_lab.sh (metadata), lab/status_lab.sh, lab/stop_lab.sh [--all]"
