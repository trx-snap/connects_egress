//go:build linux

package supervisor

// Runtime: the supervisor role's production path. It owns the systemd notifier, the Node that
// registers and serves runs (node_linux.go), the LL-HLS origin over the node's rooms and the
// authenticated control API the backend starts and stops runs through (control_linux.go). Each
// run's worker is started by the node as its own transient unit and admitted into the run's one
// source epoch through the run's own IPC2 socket. Across a supervisor restart the descriptors
// systemd passes back from its store are handed to the node, which re-adopts every recorded
// run's epoch, unit and room and listens again for its worker; the unit must be a notify
// service with a file-descriptor store large enough for every live epoch's tracks. Pure Go, no
// Cgo.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
)

// RuntimeOptions configure Run. StateDir holds the epoch directories (EpochDir) and the room
// journals (RoomDir); SocketDir holds one IPC2 unixpacket socket per run; HTTPAddr is the
// origin's listen address and ControlAddr the control API's, a loopback or private address
// (CheckControlAddr); ControlToken is the bearer token the control API requires; PlaylistBase
// is the public base URL the stable playlist URLs are formed under; NotifySocket the value of
// NOTIFY_SOCKET, required because every epoch stores its descriptors with systemd; WorkerUID
// the uid every worker process runs as, which must be this process's uid because the worker
// writes presentation.json into the epoch directory and LoadPresentation requires the file to
// be this uid's; Config the solo configuration every worker must name; Launcher starts the
// workers.
type RuntimeOptions struct {
	StateDir     string
	SocketDir    string
	HTTPAddr     string
	ControlAddr  string
	ControlToken string
	PlaylistBase string
	NotifySocket string
	WorkerUID    int
	Config       ipc.Config
	Policy       partstore.Policy
	Launcher     WorkerLauncher
	Log          *log.Logger
}

const (
	maintainInterval = 500 * time.Millisecond
	inheritedFDLimit = 256 // descriptors taken back from the store: four per live epoch
	shutdownGrace    = 2 * time.Second
	pidfdPollMS      = 500
)

// EpochDir is the private directory of one source epoch under stateDir: the supervisor creates
// it before the epoch's Source, and the worker writes presentation.json into it. Both roles
// derive it from the same identities, path-escaped.
func EpochDir(stateDir, run, source string, epoch uint64) string {
	return filepath.Join(stateDir, "sources", url.PathEscape(run), url.PathEscape(source), strconv.FormatUint(epoch, 10))
}

// RoomDir is the private directory of one room's timeline journal under stateDir, per run.
func RoomDir(stateDir, run, room string) string {
	return filepath.Join(stateDir, "rooms", url.PathEscape(run), url.PathEscape(room))
}

// Run serves runs until ctx ends or a fatal error occurs: the node's controller, the origin or
// the control API failing. It returns ctx's error on a clean stop.
func Run(ctx context.Context, o RuntimeOptions) error {
	if o.Log == nil {
		o.Log = log.New(os.Stderr, "[supervisor] ", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	}
	if err := checkRuntimeOptions(o); err != nil {
		return err
	}
	n, err := systemd.NewNotifier(o.NotifySocket)
	if err != nil {
		return fmt.Errorf("supervisor: NOTIFY_SOCKET: %w (run as a systemd notify service with a file-descriptor store)", err)
	}
	inherited, err := systemd.ConsumeInheritedFDs(inheritedFDLimit)
	if err != nil {
		return err
	}
	node, err := NewNode(NodeOptions{StateDir: o.StateDir, SocketDir: o.SocketDir, WorkerUID: o.WorkerUID, Config: o.Config,
		Policy: o.Policy, Launcher: o.Launcher, Inherited: inherited, Log: o.Log}, n)
	if err != nil {
		for _, fd := range inherited {
			closeFD(fd)
		}
		return err
	}
	if len(inherited) > 0 {
		o.Log.Printf("inherited %d descriptors from the store for recovery", len(inherited))
	}
	control, err := NewControl(node, o.ControlToken, o.PlaylistBase)
	if err != nil {
		return err
	}
	controlLn, err := net.Listen("tcp", o.ControlAddr)
	if err != nil {
		return fmt.Errorf("supervisor: control listen %s: %w", o.ControlAddr, err)
	}
	defer controlLn.Close()
	originLn, err := net.Listen("tcp", o.HTTPAddr)
	if err != nil {
		return fmt.Errorf("supervisor: origin listen %s: %w", o.HTTPAddr, err)
	}
	defer originLn.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	fatal := make(chan error, 3)
	origin := &http.Server{Handler: NewRoomOrigin(node.ctrl, node), ReadHeaderTimeout: 5 * time.Second}
	controlSrv := &http.Server{Handler: control, ReadHeaderTimeout: 5 * time.Second}
	go func() { fatal <- fmt.Errorf("supervisor: node: %w", node.Run(ctx)) }()
	go func() {
		if err := origin.Serve(originLn); !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("supervisor: origin: %w", err)
		}
	}()
	go func() {
		if err := controlSrv.Serve(controlLn); !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("supervisor: control: %w", err)
		}
	}()
	if err := n.Ready(ctx); err != nil {
		o.Log.Printf("READY=1 not sent: %v", err)
	}
	o.Log.Printf("serving: control %s, origin %s, sockets in %s, state in %s", controlLn.Addr(), originLn.Addr(), o.SocketDir, o.StateDir)

	var cause error
	select {
	case <-ctx.Done():
		cause = ctx.Err()
	case err := <-fatal:
		cause = err
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), shutdownGrace)
	controlSrv.Shutdown(shutdownCtx)
	origin.Shutdown(shutdownCtx)
	stop()
	return cause
}

func checkRuntimeOptions(o RuntimeOptions) error {
	switch {
	case !filepath.IsAbs(o.StateDir):
		return fmt.Errorf("%w: state directory %q is not absolute", ErrInvalidOptions, o.StateDir)
	case !filepath.IsAbs(o.SocketDir):
		return fmt.Errorf("%w: socket directory %q is not absolute", ErrInvalidOptions, o.SocketDir)
	case o.HTTPAddr == "":
		return fmt.Errorf("%w: empty origin address", ErrInvalidOptions)
	case o.Launcher == nil:
		return fmt.Errorf("%w: no worker launcher", ErrInvalidOptions)
	}
	if err := CheckControlAddr(o.ControlAddr); err != nil {
		return err
	}
	if _, err := effectivePolicy(o.Policy); err != nil {
		return err
	}
	return nil
}

// ensurePrivateDir creates dir as a private 0700 directory of this uid, or accepts an existing
// one that openPrivateDir will accept.
func ensurePrivateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%w: create %s: %v", ErrInvalidOptions, dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("%w: chmod %s: %v", ErrInvalidOptions, dir, err)
	}
	fd, err := openPrivateDir(dir)
	if err != nil {
		return err
	}
	closeFD(fd)
	return nil
}

// nextEpoch returns one past the highest epoch directory under root, 1 when there is none.
func nextEpoch(root string) (uint64, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, fmt.Errorf("%w: read %s: %v", ErrInvalidOptions, root, err)
	}
	var high uint64
	for _, e := range entries {
		n, err := strconv.ParseUint(e.Name(), 10, 64)
		if err != nil || !e.IsDir() || strconv.FormatUint(n, 10) != e.Name() {
			continue
		}
		high = max(high, n)
	}
	if high == ^uint64(0) {
		return 0, fmt.Errorf("%w: epoch numbers exhausted under %s", ErrInvalidOptions, root)
	}
	return high + 1, nil
}

// listenPacket listens on an IPC2 socket path, replacing a stale socket file and nothing else.
func listenPacket(path string) (*net.UnixListener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("%w: socket directory: %v", ErrInvalidOptions, err)
	}
	if st, err := os.Lstat(path); err == nil {
		if st.Mode()&os.ModeType != os.ModeSocket {
			return nil, fmt.Errorf("%w: %s exists and is not a socket", ErrInvalidOptions, path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("%w: remove stale socket: %v", ErrInvalidOptions, err)
		}
	}
	ln, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		return nil, fmt.Errorf("supervisor: listen %s: %w", path, err)
	}
	ln.SetUnlinkOnClose(true)
	return ln, nil
}
