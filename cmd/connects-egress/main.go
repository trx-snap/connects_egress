// Command connects-egress is the unified media packager and egress engine: one binary with a
// role switch. The supervisor role serves the rooms the backend starts through its control API,
// each by a worker it starts as a transient systemd unit of this same binary's worker role; both
// are the solo path of internal/supervisor and internal/worker on Linux. The bridge role is not
// implemented yet.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/supervisor"
	"github.com/connectsapp/connects-egress/internal/systemd"
	"github.com/connectsapp/connects-egress/internal/worker"
)

const version = "0.1.0-dev"

const (
	defaultStateDir    = "/var/lib/connects-egress"
	defaultSocketDir   = "/run/connects-egress"
	defaultSocket      = defaultSocketDir + "/supervisor.sock"
	defaultControlAddr = "127.0.0.1:8081"
	defaultCapacity    = 32 << 20 // bytes per track memfd: about a minute of the hi rung
	controlTokenEnv    = "CONNECTS_EGRESS_CONTROL_TOKEN"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	role := os.Args[1]
	switch role {
	case "supervisor":
		os.Exit(runSupervisor(os.Args[2:]))
	case "worker":
		os.Exit(runWorker(os.Args[2:]))
	case "bridge":
		runBridge(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Printf("connects-egress v%s\n", version)
	default:
		fmt.Fprintf(os.Stderr, "unknown role: %s\n\n", role)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `connects-egress - Unified Media Packager & Egress Engine

Usage:
  connects-egress <role> [options]

Roles:
  supervisor   Runs node supervisor (memfd Part Store, Room Timelines, LL-HLS origin)
  worker       Runs media pipeline worker for a source (ingest, compose, encode, mix)
  bridge       Runs WebRTC stage bridge for cross-node / cross-region relay

Use "connects-egress <role> --help" for role-specific options.
`)
}

// signalContext returns a context cancelled by SIGINT or SIGTERM.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// soloConfig is the one configuration both roles must agree on.
func soloConfig(capacity int64) ipc.Config {
	return ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: capacity}
}

func runSupervisor(args []string) int {
	fs := flag.NewFlagSet("supervisor", flag.ExitOnError)
	httpPort := fs.Int("http-port", 8080, "HTTP origin port for LL-HLS")
	controlAddr := fs.String("control-addr", defaultControlAddr, "control API listen address: a loopback, private or link-local IP and port")
	publicBase := fs.String("public-base-url", "", "base URL the stable playlist URLs are formed under (default http://127.0.0.1:<http-port>)")
	socketDir := fs.String("socket-dir", defaultSocketDir, "directory of the per-run worker IPC sockets")
	stateDir := fs.String("state-dir", defaultStateDir, "state directory for epoch directories and room journals")
	workerEnv := fs.String("worker-env-file", "", "absolute path of the EnvironmentFile every worker unit reads LIVEKIT_API_KEY and LIVEKIT_API_SECRET from (required)")
	workerSlice := fs.String("worker-slice", "", "systemd slice the worker units run in (default: the system default)")
	workerUID := fs.Int("worker-uid", -1, "uid the worker units run as (default: this process's uid)")
	capacity := fs.Int64("capacity", defaultCapacity, "bytes per track memfd; every worker is started with the same value")
	fs.Parse(args)
	if *workerEnv == "" {
		fmt.Fprintln(os.Stderr, "error: --worker-env-file is required")
		return 2
	}
	token := os.Getenv(controlTokenEnv)
	if token == "" {
		fmt.Fprintf(os.Stderr, "error: %s must be set to a nonempty bearer token\n", controlTokenEnv)
		return 2
	}
	if *workerUID < 0 {
		*workerUID = os.Geteuid()
	}
	if *publicBase == "" {
		*publicBase = fmt.Sprintf("http://127.0.0.1:%d", *httpPort)
	}
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: this executable: %v\n", err)
		return 2
	}
	launcher, err := systemd.NewLauncher(systemd.LauncherOptions{Executable: exe, EnvironmentFile: *workerEnv, UID: *workerUID, Slice: *workerSlice})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 2
	}
	logger := log.New(os.Stderr, "[supervisor] ", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	logger.Printf("starting connects-egress supervisor v%s on port %d (control: %s, sockets: %s)", version, *httpPort, *controlAddr, *socketDir)
	ctx, stop := signalContext()
	defer stop()
	err = supervisor.Run(ctx, supervisor.RuntimeOptions{
		StateDir:     *stateDir,
		SocketDir:    *socketDir,
		HTTPAddr:     fmt.Sprintf(":%d", *httpPort),
		ControlAddr:  *controlAddr,
		ControlToken: token,
		PlaylistBase: *publicBase,
		NotifySocket: os.Getenv("NOTIFY_SOCKET"),
		WorkerUID:    *workerUID,
		Config:       soloConfig(*capacity),
		Launcher:     workerLauncher{launcher},
		Log:          logger,
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Printf("exiting: %v", err)
		return 1
	}
	logger.Printf("stopped")
	return 0
}

// workerLauncher starts each worker as a transient unit running this binary's worker role with
// the run's fixed argv; every value is passed as --flag=value so none is read as a flag.
type workerLauncher struct {
	l *systemd.Launcher
}

func (w workerLauncher) Start(ctx context.Context, spec supervisor.WorkerSpec) (supervisor.WorkerUnit, error) {
	args := []string{"worker", "--source-id=" + spec.Run, "--mode=" + spec.Config.Mode, "--ipc-socket=" + spec.Socket,
		"--state-dir=" + spec.StateDir, "--run=" + spec.Run, "--url=" + spec.URL, "--room=" + spec.Room, "--room-id=" + spec.RoomID,
		"--host=" + spec.Host, "--supervisor-uid=" + strconv.Itoa(spec.SupervisorUID), "--capacity=" + strconv.FormatInt(spec.Config.Capacity, 10)}
	u, err := w.l.Start(ctx, spec.Run, spec.LaunchID, args)
	if u == nil {
		return nil, err // never a typed nil behind the interface
	}
	return u, err
}

func (w workerLauncher) Adopt(ctx context.Context, run, launchID string) (supervisor.WorkerUnit, bool, error) {
	u, present, err := w.l.Adopt(ctx, run, launchID)
	if u == nil {
		return nil, false, err
	}
	return u, present, err
}

func runWorker(args []string) int {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	sourceID := fs.String("source-id", "", "Unique source ID (solo room or match)")
	mode := fs.String("mode", "solo", "Media mode: solo | 1v1 | tryon | quadro")
	ipcSocket := fs.String("ipc-socket", defaultSocket, "Supervisor Unix socket")
	stateDir := fs.String("state-dir", defaultStateDir, "the supervisor's state directory, where the presentation is written")
	run := fs.String("run", "", "run identity the supervisor serves (required)")
	url := fs.String("url", "", "LiveKit server URL (required)")
	room := fs.String("room", "", "LiveKit room to join (required)")
	roomID := fs.String("room-id", "", "room identity the presentation names and the supervisor serves at /live/<room-id>/ (default: the LiveKit room)")
	host := fs.String("host", "", "identity of the host participant to ingest (required)")
	supervisorUID := fs.Int("supervisor-uid", -1, "uid the supervisor's socket peer must have (default: this process's uid)")
	capacity := fs.Int64("capacity", defaultCapacity, "bytes per track memfd; must match the supervisor")
	fs.Parse(args)

	if *sourceID == "" {
		fmt.Fprintln(os.Stderr, "error: --source-id is required")
		return 2
	}
	if *run == "" || *url == "" || *room == "" || *host == "" {
		fmt.Fprintln(os.Stderr, "error: --run, --url, --room and --host are required")
		return 2
	}
	if *mode != ipc.ModeSolo {
		fmt.Fprintf(os.Stderr, "error: mode %q is not implemented; only solo is\n", *mode)
		return 2
	}
	key, secret := os.Getenv("LIVEKIT_API_KEY"), os.Getenv("LIVEKIT_API_SECRET")
	if key == "" || secret == "" {
		fmt.Fprintln(os.Stderr, "error: LIVEKIT_API_KEY and LIVEKIT_API_SECRET must be set")
		return 2
	}
	if *supervisorUID < 0 {
		*supervisorUID = os.Geteuid()
	}
	if *roomID == "" {
		*roomID = *room
	}
	logger := log.New(os.Stderr, "[worker] ", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	logger.Printf("starting connects-egress worker v%s for source %s (mode: %s, IPC: %s, room %s)", version, *sourceID, *mode, *ipcSocket, *roomID)
	ctx, stop := signalContext()
	defer stop()
	err := worker.Run(ctx, worker.RunnerOptions{
		Socket:        *ipcSocket,
		Run:           *run,
		Source:        *sourceID,
		Room:          *room,
		RoomID:        *roomID,
		Host:          *host,
		URL:           *url,
		APIKey:        key,
		APISecret:     secret,
		SupervisorUID: *supervisorUID,
		Config:        soloConfig(*capacity),
		EpochDir: func(epoch uint64) string {
			return supervisor.EpochDir(*stateDir, *run, *sourceID, epoch)
		},
		Log: logger,
	})
	if err != nil {
		logger.Printf("exiting: %v", err)
		return 1
	}
	logger.Printf("room ended cleanly")
	return 0
}

func runBridge(args []string) {
	fs := flag.NewFlagSet("bridge", flag.ExitOnError)
	matchID := fs.String("match-id", "", "Match ID to bridge")
	anchorURL := fs.String("anchor-url", "", "Anchor LiveKit SFU URL")
	spokeURL := fs.String("spoke-url", "", "Spoke LiveKit SFU URL")
	fs.Parse(args)

	if *matchID == "" {
		fmt.Fprintln(os.Stderr, "error: --match-id is required")
		os.Exit(1)
	}

	fmt.Printf("[bridge] starting connects-egress bridge for match %s (anchor: %s, spoke: %s)...\n", *matchID, *anchorURL, *spokeURL)
	// TODO: Phase 1 implementation per plan §17
}
