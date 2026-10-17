//go:build linux

package supervisor

// Node: the supervisor's run registry over every room it serves. A run is one broadcast of one
// room by one worker: the backend starts it through the control API with a unique run_id that
// is also the source identity, and the node gives it a durable run record, its own room
// journal, IPC2 socket and transient worker unit carrying an unpredictable launch id. The node
// admits into the run's one source epoch exactly the process systemd names as that unit's main
// process, binds the room to the epoch once its start is durable and serves the stable room
// playlists from it. A verified worker terminal ends the run, a worker gone without one fails
// it, a stop request stops it; a run is retired in one order, playlists first, then the
// journal, the timeline, the socket, the terminal record, and the room is freed last, once
// that record is durable and the unit is gone. A run that ends cleanly, on a verified terminal,
// keeps serving its final playlist, ended with EXT-X-ENDLIST, and the media it references for a
// bounded grace after the end, so that a player reading it stops rather than retries; a newer
// run reserved for the room supersedes it for good the moment it is reserved, whatever becomes
// of that newer run, and a run that fails serves nothing. The
// room is freed as described, once
// that record is durable and the unit is gone, so a later run for the room is a new run with
// new media, no cleanup can touch it and no worker of the old run is alive beside its own. A
// run that is over owes its unit a stop until the stop is taken or the unit is gone, and the
// epoch is released only then; its descriptors are asked out of the store until every removal
// is sent. A start whose outcome the launcher cannot establish is held in starting, owning its
// room, until the unit of its name is gone or its worker connects: it is never rolled back
// into a retry that could start a second worker. After a supervisor restart the node recovers
// every run from its record, re-adopts the unit by launch id, reopens the epoch from the
// inherited descriptors and the room from its journal, and listens again on the run's socket
// for the worker's reconnect; a run that is over keeps its room until its unit is gone as
// well. Pure Go, no Cgo.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
	"golang.org/x/sys/unix"
)

var (
	// ErrRunUnknown reports a run identity the node does not hold.
	ErrRunUnknown = errors.New("supervisor: run unknown")

	// ErrRunConflict reports a run_id reused with other parameters, or a room that already has a
	// run that has not ended.
	ErrRunConflict = errors.New("supervisor: run conflict")

	// ErrRunLaunch reports a run whose worker could not be started; its registration was rolled
	// back and the identity may be retried.
	ErrRunLaunch = errors.New("supervisor: worker not started")

	// ErrRunLaunchUncertain reports a run whose worker start has no established outcome: a unit
	// of the run's name may exist. The run is registered and held in starting with the reason,
	// its room stays owned, every Start of it returns this again, and it settles once the unit
	// is gone or its worker publishes.
	ErrRunLaunchUncertain = errors.New("supervisor: worker launch outcome uncertain")

	// ErrRunStop reports a stop that could not be issued to the run's unit: the run is unchanged,
	// still exposed if it was, and the stop may be retried. A run that is over but still owes its
	// unit a stop reports it too, with its terminal state.
	ErrRunStop = errors.New("supervisor: worker not stopped")

	// ErrNodeClosed reports a node that no longer starts or stops runs.
	ErrNodeClosed = errors.New("supervisor: node closed")
)

// RunState is where a run is in its life. Ended, failed and stopped are terminal.
type RunState string

const (
	RunStarting RunState = "starting" // registered and its worker unit started; no media yet
	RunRunning  RunState = "running"  // the room is bound to the worker's epoch: playlists serve
	RunStopping RunState = "stopping" // a stop was issued; the worker is ending
	RunEnded    RunState = "ended"    // the worker published a verified terminal
	RunFailed   RunState = "failed"   // the worker is gone without a verified terminal
	RunStopped  RunState = "stopped"  // the run was stopped on request
)

// Terminal reports whether s is final.
func (s RunState) Terminal() bool { return s == RunEnded || s == RunFailed || s == RunStopped }

// RunSpec is what the backend starts a run with. RunID is the idempotency key, the run and
// source identity, the unit name part and the socket name; RoomID is the room of the stable
// playlist URL, the presentation slot and the timeline; LiveKitRoom and HostIdentity are what
// the worker joins and ingests from LiveKitURL.
type RunSpec struct {
	RunID        string
	RoomID       string
	LiveKitRoom  string
	HostIdentity string
	LiveKitURL   string
}

// RunStatus is the observable state of a run. Reason is bounded and set with a terminal state
// or while the run is held; Uncertain is set while the run is held because its launch has no
// established outcome.
type RunStatus struct {
	Spec      RunSpec
	State     RunState
	Reason    string
	Uncertain bool
	CreatedAt time.Time
	StartedAt time.Time // running entered
	EndedAt   time.Time // a terminal state entered
}

// WorkerSpec is what a worker is started with: the run it publishes into and the launch id its
// unit carries, its IPC2 socket, the state directory holding its epoch directory, the LiveKit
// room, host and URL it ingests, the room identity it presents, the uid of the supervisor and
// the configuration both must agree on.
type WorkerSpec struct {
	Run           string
	LaunchID      string
	Socket        string
	StateDir      string
	URL           string
	Room          string // the LiveKit room to join
	RoomID        string // the presentation and playlist room
	Host          string
	SupervisorUID int
	Config        ipc.Config
}

// WorkerLauncher starts and re-adopts workers in their own transient units. Start returns once
// the unit is started or refused; a failure that wraps systemd.ErrLaunchUncertain returns the
// unit of the run's name as well, which may exist without the launcher being able to prove or
// undo it. Adopt returns the handle of the unit of a recovered run and whether it is loaded with
// the run's launch id; a unit that cannot be proven or read is systemd.ErrLaunchUncertain with
// the handle.
type WorkerLauncher interface {
	Start(ctx context.Context, spec WorkerSpec) (WorkerUnit, error)
	Adopt(ctx context.Context, run, launchID string) (WorkerUnit, bool, error)
}

// WorkerUnit is the handle of one worker unit by name and launch id. Owns reports whether pid
// is the main process of the unit carrying the launch id; Stop asks for its end without waiting
// and refuses a unit it does not own; Wait returns once the unit is gone, with how it ended, or
// ctx's error.
type WorkerUnit interface {
	Name() string
	LaunchID() string
	Owns(ctx context.Context, pid int) (bool, error)
	Stop(ctx context.Context) error
	Wait(ctx context.Context) (systemd.Exit, error)
}

// NodeOptions configure a Node. StateDir holds the run records (RunDir), the epoch directories
// (EpochDir) and the room journals (RoomDir); SocketDir holds one IPC2 socket per run;
// WorkerUID is the uid every worker runs as, this process's; Config is the solo configuration
// every worker must name; Launcher starts and adopts the workers; Inherited are the descriptors
// systemd passed back from its store, by name, which Run consumes: adopted into the recovered
// epochs, every other one closed and removed from the store.
type NodeOptions struct {
	StateDir  string
	SocketDir string
	WorkerUID int
	Config    ipc.Config
	Policy    partstore.Policy
	Launcher  WorkerLauncher
	Inherited map[string]int
	Log       *log.Logger
}

const (
	maxReason      = 256
	maxLiveKitURL  = 1024
	launchBudget   = 30 * time.Second // registration to the worker unit started
	unitStopBudget = 10 * time.Second // a stop issued from inside the node
	unitStopRetry  = 2 * time.Second  // between attempts to issue a stop the run owes its unit
	unitWatchRetry = 2 * time.Second  // between attempts to watch a unit systemd could not report on
	recoverBudget  = 20 * time.Second // one run's recovery at startup

	// finalPlaylistGrace is how long a run that ended cleanly keeps serving its final playlist
	// and the media it references: three of a player's two-second polls.
	finalPlaylistGrace = 6 * time.Second

	runFile      = "run.json"
	runFormat    = 2 // format 1 lacked the reserved epoch; such a record is refused, never guessed at
	maxRunRecord = 16 * 1024
	unixPathMax  = 108 // sizeof(sockaddr_un.sun_path)
)

// RunDir is the private directory of one run's record under stateDir.
func RunDir(stateDir, run string) string {
	return filepath.Join(stateDir, "runs", url.PathEscape(run))
}

// runRecord is the durable record of a run: what it was started with, the launch id its unit
// carries and the epoch reserved for its worker, so a restarted supervisor can find and
// re-adopt it before or after the worker has connected, and, once the run is over, how it
// ended, so a restarted supervisor reports it as it was, and whether it still owes its unit a
// stop, written before that stop is attempted.
type runRecord struct {
	Format        int      `json:"format"`
	RunID         string   `json:"run_id"`
	RoomID        string   `json:"room_id"`
	LiveKitRoom   string   `json:"livekit_room"`
	HostIdentity  string   `json:"host_identity"`
	LiveKitURL    string   `json:"livekit_url"`
	LaunchID      string   `json:"launch_id"`
	Epoch         uint64   `json:"epoch"`
	CreatedUnixNS int64    `json:"created_unix_ns"`
	StartedUnixNS int64    `json:"started_unix_ns,omitempty"`
	EndedUnixNS   int64    `json:"ended_unix_ns,omitempty"`
	FinalState    RunState `json:"final_state,omitempty"`
	FinalReason   string   `json:"final_reason,omitempty"`
	StopPending   bool     `json:"stop_pending,omitempty"`
}

func (rec runRecord) spec() RunSpec {
	return RunSpec{RunID: rec.RunID, RoomID: rec.RoomID, LiveKitRoom: rec.LiveKitRoom, HostIdentity: rec.HostIdentity, LiveKitURL: rec.LiveKitURL}
}

// writeRunRecord makes rec durable in dir, a private directory, replacing any record there.
func writeRunRecord(dir string, rec runRecord) error {
	if err := ensurePrivateDir(dir); err != nil {
		return err
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("supervisor: encode %s: %w", runFile, err)
	}
	tmp := filepath.Join(dir, runFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("supervisor: write %s: %w", runFile, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("supervisor: write %s: %w", runFile, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("supervisor: sync %s: %w", runFile, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("supervisor: close %s: %w", runFile, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, runFile)); err != nil {
		return fmt.Errorf("supervisor: publish %s: %w", runFile, err)
	}
	// The rename is durable only once the directory is.
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("supervisor: open %s: %w", dir, err)
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return fmt.Errorf("supervisor: sync %s: %w", dir, err)
	}
	return nil
}

// readRunRecord reads and strictly decodes the record in dir: exactly one JSON object of the
// current format, without unknown fields or trailing bytes, describing a valid run.
func readRunRecord(dir string) (runRecord, error) {
	var rec runRecord
	data, err := os.ReadFile(filepath.Join(dir, runFile))
	if err != nil {
		return rec, err
	}
	if len(data) > maxRunRecord {
		return rec, fmt.Errorf("%s of %d bytes exceeds %d", runFile, len(data), maxRunRecord)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return rec, fmt.Errorf("%s: %w", runFile, err)
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return rec, fmt.Errorf("%s: trailing bytes after the record", runFile)
	}
	if rec.Format != runFormat {
		return rec, fmt.Errorf("%s: format %d, this supervisor reads format %d only", runFile, rec.Format, runFormat)
	}
	if err := checkRunSpec(rec.spec()); err != nil {
		return rec, fmt.Errorf("%s: %w", runFile, err)
	}
	if err := systemd.CheckLaunchID(rec.LaunchID); err != nil {
		return rec, fmt.Errorf("%s: %w", runFile, err)
	}
	if rec.Epoch == 0 {
		return rec, fmt.Errorf("%s: no reserved epoch", runFile)
	}
	if rec.FinalState != "" && !rec.FinalState.Terminal() {
		return rec, fmt.Errorf("%s: final state %q is not terminal", runFile, rec.FinalState)
	}
	if rec.StopPending && rec.FinalState == "" {
		return rec, fmt.Errorf("%s: stop pending without a final state", runFile)
	}
	return rec, nil
}

// Node is the run registry and the owner of every run's record, socket, epoch, timeline and
// unit. See the file comment.
type Node struct {
	o        NodeOptions
	log      *log.Logger
	notifier *systemd.Notifier
	ctrl     *PlaybackController

	mu        sync.Mutex      // may be held while a run's mu is taken (Start superseding a final run); never the reverse
	ctx       context.Context // the Run lifetime; nil before Run
	ready     chan struct{}   // closed once Run has recovered the previous incarnation's runs
	readyOnce sync.Once
	closed    bool
	runs      map[string]*run // every run known, by run id
	rooms     map[string]*run // room id to its run that has not been retired
	final     map[string]*run // room id to its run that ended cleanly and still serves its final playlist
	wg        sync.WaitGroup
}

// NewNode validates o and returns a node that serves no room yet. n stores every epoch's
// descriptors.
func NewNode(o NodeOptions, n *systemd.Notifier) (*Node, error) {
	if o.Log == nil {
		o.Log = log.New(os.Stderr, "[supervisor] ", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	}
	switch {
	case n == nil:
		return nil, fmt.Errorf("%w: nil notifier", ErrInvalidOptions)
	case o.Launcher == nil:
		return nil, fmt.Errorf("%w: nil worker launcher", ErrInvalidOptions)
	case !filepath.IsAbs(o.StateDir):
		return nil, fmt.Errorf("%w: state directory %q is not absolute", ErrInvalidOptions, o.StateDir)
	case !filepath.IsAbs(o.SocketDir):
		return nil, fmt.Errorf("%w: socket directory %q is not absolute", ErrInvalidOptions, o.SocketDir)
	case o.WorkerUID != os.Geteuid():
		return nil, fmt.Errorf("%w: worker uid %d differs from this process's uid %d; the presentation handoff requires the same uid", ErrInvalidOptions, o.WorkerUID, os.Geteuid())
	case o.Config.Mode != ipc.ModeSolo:
		return nil, fmt.Errorf("%w: mode %q; this node serves solo rooms", ErrInvalidOptions, o.Config.Mode)
	}
	hello := ipc.Hello{Version: ipc.Version, Run: "run", Source: "run", PID: os.Getpid(), Config: o.Config}
	if err := ipc.ValidatePacket(ipc.Packet{Type: ipc.TypeHello, Hello: &hello}); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidOptions, err)
	}
	if _, err := effectivePolicy(o.Policy); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(o.SocketDir, 0o750); err != nil {
		return nil, fmt.Errorf("%w: socket directory: %v", ErrInvalidOptions, err)
	}
	node := &Node{o: o, log: o.Log, notifier: n, ready: make(chan struct{}), runs: map[string]*run{}, rooms: map[string]*run{}, final: map[string]*run{}}
	ctrl, err := NewPlaybackController(node.resolve, nil)
	if err != nil {
		return nil, err
	}
	node.ctrl = ctrl
	return node, nil
}

// Run drives the node until ctx ends or the playback controller fails: it recovers the runs of
// a previous incarnation from their records and the inherited descriptors, runs the controller
// and the retention maintenance, then stops every listener, joins every goroutine and closes
// every timeline and source, leaving the stored descriptors in place for the next incarnation.
// Worker units are left running. Runs may be started only while Run is active.
func (n *Node) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n.mu.Lock()
	if n.ctx != nil || n.closed {
		n.mu.Unlock()
		return fmt.Errorf("%w: node already ran", ErrInvalidOptions)
	}
	n.ctx = ctx
	n.mu.Unlock()
	n.recover(ctx)
	n.readyOnce.Do(func() { close(n.ready) })
	fatal := make(chan error, 1)
	go func() { fatal <- n.ctrl.Run(ctx) }()
	n.wg.Add(1)
	go n.maintain(ctx)
	var cause error
	select {
	case <-ctx.Done():
		cause = ctx.Err()
	case err := <-fatal:
		cause = fmt.Errorf("supervisor: controller: %w", err)
	}
	cancel()
	n.mu.Lock()
	n.closed = true
	runs := n.snapshot()
	n.mu.Unlock()
	launched := runs[:0]
	for _, r := range runs {
		<-r.ready // a launch in flight settles against the cancelled context
		if r.launchErr == nil {
			launched = append(launched, r)
			r.closeListener()
		}
	}
	n.wg.Wait()
	for _, r := range launched {
		if r.tl != nil {
			r.tl.Close()
		}
		r.mu.Lock()
		ep := r.ep
		r.mu.Unlock()
		if ep != nil && !ep.closed.Load() {
			if err := ep.src.Close(); err != nil {
				n.log.Printf("run %q: close epoch %d: %v", r.spec.RunID, ep.key.Epoch, err)
			}
		}
	}
	return cause
}

// awaitReady waits until the recovery of the previous incarnation's runs is done, so that no
// request sees a run as unknown that is about to be recovered.
func (n *Node) awaitReady(ctx context.Context) error {
	select {
	case <-n.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// snapshot returns every run, under mu.
func (n *Node) snapshot() []*run {
	out := make([]*run, 0, len(n.runs))
	for _, r := range n.runs {
		out = append(out, r)
	}
	return out
}

// checkRunSpec validates a run before any side effect: the run id names a unit and a socket,
// the rooms and host are presentation identities, the URL is a LiveKit endpoint.
func checkRunSpec(s RunSpec) error {
	if err := systemd.CheckUnitBase(s.RunID); err != nil {
		return fmt.Errorf("%w: run_id: %v", ErrInvalidOptions, err)
	}
	if err := checkRoomID(s.RoomID); err != nil {
		return fmt.Errorf("%w: room_id: %v", ErrInvalidOptions, err)
	}
	if err := checkRoomID(s.LiveKitRoom); err != nil {
		return fmt.Errorf("%w: livekit_room: %v", ErrInvalidOptions, err)
	}
	if err := checkRoomID(s.HostIdentity); err != nil {
		return fmt.Errorf("%w: host_identity: %v", ErrInvalidOptions, err)
	}
	u, err := url.Parse(s.LiveKitURL)
	switch {
	case s.LiveKitURL == "" || len(s.LiveKitURL) > maxLiveKitURL || !utf8.ValidString(s.LiveKitURL) || strings.ContainsFunc(s.LiveKitURL, unicode.IsControl):
		return fmt.Errorf("%w: livekit_url is empty, too long or malformed", ErrInvalidOptions)
	case err != nil:
		return fmt.Errorf("%w: livekit_url: %v", ErrInvalidOptions, err)
	case u.Scheme != "ws" && u.Scheme != "wss" && u.Scheme != "http" && u.Scheme != "https":
		return fmt.Errorf("%w: livekit_url scheme %q is not ws, wss, http or https", ErrInvalidOptions, u.Scheme)
	case u.Host == "":
		return fmt.Errorf("%w: livekit_url has no host", ErrInvalidOptions)
	}
	return nil
}

// Start starts the run described by spec, or returns the run already started with exactly spec,
// and reports whether it created one. A run_id held with other parameters or a room with a run
// that has not ended is ErrRunConflict; a room whose run is over but not retired, its terminal
// record not yet durable or its unit not yet gone, is waited for, bounded by ctx, and the run
// then starts. A worker that cannot be started is ErrRunLaunch:
// the registration and the record are rolled back and nothing of the run remains. A start
// without an established outcome is ErrRunLaunchUncertain together with the run, which is held
// (see ErrRunLaunchUncertain) rather than rolled back, and every Start of a held run returns the
// same. Concurrent identical requests return the same outcome.
func (n *Node) Start(ctx context.Context, spec RunSpec) (RunStatus, bool, error) {
	if err := checkRunSpec(spec); err != nil {
		return RunStatus{}, false, err
	}
	socket := filepath.Join(n.o.SocketDir, spec.RunID+".sock")
	if len(socket) >= unixPathMax {
		return RunStatus{}, false, fmt.Errorf("%w: run_id: socket path %q does not fit sockaddr_un", ErrInvalidOptions, socket)
	}
	if err := n.awaitReady(ctx); err != nil {
		return RunStatus{}, false, err
	}
	n.mu.Lock()
	if n.closed || n.ctx == nil {
		n.mu.Unlock()
		return RunStatus{}, false, ErrNodeClosed
	}
	if r := n.runs[spec.RunID]; r != nil {
		n.mu.Unlock()
		if r.spec != spec {
			return RunStatus{}, false, fmt.Errorf("%w: run %q exists with other parameters", ErrRunConflict, spec.RunID)
		}
		return r.await(ctx)
	}
	if other := n.rooms[spec.RoomID]; other != nil {
		n.mu.Unlock()
		if st := other.status(); st.State.Terminal() {
			// The run is over and its retirement is in flight: the room is free once its
			// terminal state is durable, which maintain keeps retrying, and its unit is gone,
			// which its watcher reports; no run starts beside a worker that may be alive.
			select {
			case <-other.retiredCh:
				return n.Start(ctx, spec)
			case <-ctx.Done():
				return RunStatus{}, false, ctx.Err()
			}
		} else {
			return RunStatus{}, false, fmt.Errorf("%w: room %q has run %q in state %s", ErrRunConflict, spec.RoomID, other.spec.RunID, st.State)
		}
	}
	r := &run{n: n, spec: spec, socket: socket, state: RunStarting, created: time.Now(), ready: make(chan struct{}), retiredCh: make(chan struct{})}
	n.runs[spec.RunID], n.rooms[spec.RoomID] = r, r
	// The room's final run, if one still serves its final playlist, is superseded by this
	// reservation for good: whatever becomes of this run, the room never shows the older one.
	// The mark is made before mu is released, so no lookup or media resolution that starts
	// after the reservation selects the older run; one that already selected it finishes. Its
	// timeline is closed once mu is released.
	final := n.final[spec.RoomID]
	var finalTL *Timeline
	if final != nil {
		delete(n.final, spec.RoomID)
		finalTL = final.markSuperseded()
	}
	lifetime := n.ctx
	n.mu.Unlock()
	if final != nil {
		if finalTL != nil {
			finalTL.Close()
		}
		n.log.Printf("run %q room %q: final playlist withdrawn: a newer run was reserved for the room", final.spec.RunID, final.spec.RoomID)
	}

	launchCtx, cancel := context.WithTimeout(lifetime, launchBudget)
	err := r.launch(launchCtx)
	cancel()
	if errors.Is(err, ErrRunLaunchUncertain) {
		close(r.ready)
		n.log.Printf("run %q room %q: held in starting, unit %s watched: %v", spec.RunID, spec.RoomID, r.unit.Name(), err)
		return r.status(), true, err
	}
	if err != nil {
		n.mu.Lock()
		delete(n.runs, spec.RunID)
		if n.rooms[spec.RoomID] == r {
			delete(n.rooms, spec.RoomID)
		}
		n.mu.Unlock()
		r.launchErr = fmt.Errorf("%w: %v", ErrRunLaunch, err)
		close(r.ready)
		n.log.Printf("run %q room %q: not started: %v", spec.RunID, spec.RoomID, err)
		return RunStatus{}, false, r.launchErr
	}
	close(r.ready)
	n.log.Printf("run %q room %q: started unit %s (launch %s) for LiveKit room %q host %q: ipc %s", spec.RunID, spec.RoomID,
		r.unit.Name(), r.launchID, spec.LiveKitRoom, spec.HostIdentity, r.socket)
	return r.status(), true, nil
}

// Status returns the run's status once its launch has settled: a run being started is awaited,
// bounded by ctx, and one rolled back is unknown.
func (n *Node) Status(ctx context.Context, runID string) (RunStatus, error) {
	if err := n.awaitReady(ctx); err != nil {
		return RunStatus{}, err
	}
	r, err := n.lookupRun(runID)
	if err != nil {
		return RunStatus{}, err
	}
	st, _, err := r.await(ctx)
	switch {
	case errors.Is(err, ErrRunLaunch) && !errors.Is(err, ErrRunLaunchUncertain):
		return RunStatus{}, fmt.Errorf("%w: %q", ErrRunUnknown, runID)
	case errors.Is(err, ErrRunLaunchUncertain):
		return st, nil
	}
	return st, err
}

// Stop requests the run's end once its launch has settled: its unit is asked to stop, and only
// then do its playlists become unavailable and the run turn stopping; it settles stopped once
// the worker is gone. A stop the unit refuses or systemd cannot take is ErrRunStop with the run
// unchanged, so the request can be retried. A run already ending or ended is returned as it is,
// except that one still owing its unit a stop issues it again: ErrRunStop with the run when
// that is refused too.
func (n *Node) Stop(ctx context.Context, runID string) (RunStatus, error) {
	if err := n.awaitReady(ctx); err != nil {
		return RunStatus{}, err
	}
	r, err := n.lookupRun(runID)
	if err != nil {
		return RunStatus{}, err
	}
	if _, _, err := r.await(ctx); err != nil && !errors.Is(err, ErrRunLaunchUncertain) {
		if errors.Is(err, ErrRunLaunch) {
			return RunStatus{}, fmt.Errorf("%w: %q", ErrRunUnknown, runID)
		}
		return RunStatus{}, err
	}
	return r.requestStop(ctx)
}

// lookupRun returns the run of runID or ErrRunUnknown; a run being rolled back is still found
// here and reported unknown by its launch result.
func (n *Node) lookupRun(runID string) (*run, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	r := n.runs[runID]
	if r == nil {
		return nil, fmt.Errorf("%w: %q", ErrRunUnknown, runID)
	}
	return r, nil
}

// Lookup implements RoomIndex: the timeline of the room's running run, or, while no run owns the
// room and no newer run was reserved for it, the final timeline of its run that ended cleanly
// within the last finalPlaylistGrace. A request during the recovery at startup waits for it.
func (n *Node) Lookup(room string) (*Timeline, bool) {
	<-n.ready
	n.mu.Lock()
	r := n.rooms[room]
	if r == nil {
		r = n.final[room]
	}
	n.mu.Unlock()
	if r == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.state == RunRunning:
		return r.tl, true
	case !r.superseded && r.tl != nil && !r.tlClosed && !r.finalUntil.IsZero() && time.Now().Before(r.finalUntil):
		return r.tl, true // the final playlist of a run that ended cleanly
	}
	return nil, false
}

// Inactive implements RoomIndex: a room any run of this node has served or is about to.
func (n *Node) Inactive(room string) bool {
	<-n.ready
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, r := range n.runs {
		if r.spec.RoomID == room {
			return true
		}
	}
	return false
}

// resolve is the EpochResolver of the controller and every timeline: an epoch of a run this
// node holds and has not released.
func (n *Node) resolve(key EpochKey) (*Source, error) {
	n.mu.Lock()
	r := n.runs[key.Run]
	n.mu.Unlock()
	if r != nil {
		r.mu.Lock()
		ep, superseded := r.ep, r.superseded
		r.mu.Unlock()
		if ep != nil && ep.key == key && !ep.closed.Load() && !superseded {
			return ep.src, nil
		}
	}
	return nil, fmt.Errorf("%w: %+v", ErrLostEpoch, key)
}

// maintain rolls every open epoch's retention on a fixed cadence, retries the retirement of
// runs whose terminal record could not be written and the stop of a unit a run still owes one,
// withdraws the final playlist of a run that ended cleanly once its grace is over, and releases
// the epochs of retired runs, after that grace, asking again for a descriptor removal the store
// could not be asked for. A stop is retried in a goroutine of its own, so a systemd that does
// not answer never stalls the maintenance of other runs, and only while the run owes it: the
// unit of a run that is not over is never stopped from here.
func (n *Node) maintain(ctx context.Context) {
	defer n.wg.Done()
	t := time.NewTicker(maintainInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now, wall := monotonicNow(), time.Now()
		n.mu.Lock()
		runs := n.snapshot()
		n.mu.Unlock()
		for _, r := range runs {
			r.mu.Lock()
			ep, released, pending, done := r.ep, r.released, r.pending, r.retired
			stop := r.stopPending && !r.gone && !r.stopBusy && !wall.Before(r.stopRetryAt)
			if stop {
				r.stopBusy = true
			}
			graceOver := !r.finalUntil.IsZero() && !wall.Before(r.finalUntil)
			withdraw := graceOver && r.tl != nil && !r.tlClosed
			if withdraw {
				r.tlClosed = true
			}
			done = done && (r.finalUntil.IsZero() || graceOver) // the media stays through the grace
			r.mu.Unlock()
			if pending {
				r.complete()
			}
			if withdraw {
				r.tl.Close()
				n.mu.Lock()
				if n.final[r.spec.RoomID] == r {
					delete(n.final, r.spec.RoomID)
				}
				n.mu.Unlock()
				n.log.Printf("run %q room %q: final playlist withdrawn after %v", r.spec.RunID, r.spec.RoomID, finalPlaylistGrace)
			}
			if stop {
				n.wg.Add(1)
				go func() {
					defer n.wg.Done()
					r.issueStop(ctx)
					r.mu.Lock()
					r.stopBusy = false
					r.mu.Unlock()
				}()
			}
			if ep == nil || released {
				continue
			}
			if done {
				r.release(ctx)
				continue
			}
			err := ep.src.Maintain(now)
			ep.mu.Lock()
			if err != nil && !ep.maintainErr && !errors.Is(err, ErrClosed) {
				ep.maintainErr = true
				n.log.Printf("run %q: epoch %d: maintain: %v", r.spec.RunID, ep.key.Epoch, err)
			}
			ep.mu.Unlock()
		}
	}
}

// recover rebuilds the runs of a previous incarnation from their records at startup, adopting
// the inherited descriptors into their epochs; what no run adopts is closed and removed from
// the store.
func (n *Node) recover(ctx context.Context) {
	inherited := n.o.Inherited
	n.o.Inherited = nil
	defer func() {
		for name, fd := range inherited {
			closeFD(fd)
			if err := n.notifier.RemoveFD(ctx, name); err != nil && ctx.Err() == nil {
				n.log.Printf("inherited descriptor %q: not adopted; remove from the store: %v", name, err)
			}
		}
		if len(inherited) > 0 {
			n.log.Printf("dropped %d inherited descriptors no recovered run adopted", len(inherited))
		}
	}()
	entries, err := os.ReadDir(filepath.Join(n.o.StateDir, "runs"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			n.log.Printf("recovery: run records: %v", err)
		}
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(n.o.StateDir, "runs", e.Name())
		rec, err := readRunRecord(dir)
		if err != nil {
			n.log.Printf("recovery: %s: %v; left as it is", dir, err)
			continue
		}
		if url.PathEscape(rec.RunID) != e.Name() {
			n.log.Printf("recovery: %s names run %q; left as it is", dir, rec.RunID)
			continue
		}
		n.adopt(ctx, rec, inherited)
	}
}

// adopt recovers one run from rec: its reserved epoch from the inherited descriptors, its unit
// by launch id, its room from the journal. The run ends up running, starting, held, or already
// over, exactly as the durable state and systemd say; durable state that does not describe one
// consistent run fails the recovery (failRecovery).
func (n *Node) adopt(ctx context.Context, rec runRecord, inherited map[string]int) {
	ctx, cancel := context.WithTimeout(ctx, recoverBudget)
	defer cancel()
	spec := rec.spec()
	id := spec.RunID
	r := &run{n: n, spec: spec, socket: filepath.Join(n.o.SocketDir, id+".sock"), launchID: rec.LaunchID, state: RunStarting,
		recordDir: RunDir(n.o.StateDir, id), created: time.Unix(0, rec.CreatedUnixNS), ready: make(chan struct{}),
		retiredCh: make(chan struct{}), adopted: true}
	close(r.ready)
	if rec.StartedUnixNS != 0 {
		r.started = time.Unix(0, rec.StartedUnixNS)
	}
	if rec.FinalState != "" {
		n.adoptFinished(ctx, r, rec, inherited)
		return
	}
	// The record's reserved epoch is the one the worker publishes into. A directory under it
	// means the worker has connected before the restart and the epoch holds its bytes; any
	// other directory, or a reserved epoch that cannot be reopened, means the durable state
	// does not describe one run any more, and the recovery fails rather than attach bytes from
	// another epoch.
	r.epoch = rec.Epoch
	epochErr := n.recoverEpoch(ctx, r, rec.Epoch, inherited)
	unit, present, uerr := n.o.Launcher.Adopt(ctx, id, rec.LaunchID)
	r.unit = unit
	n.mu.Lock()
	n.runs[id] = r
	n.mu.Unlock()
	lifetime := n.lifetime()
	switch {
	case uerr != nil && epochErr != nil:
		// The unit cannot be established and the reserved epoch cannot be recovered: whatever
		// the unit turns out to be, the run cannot go on, so it fails now with that reason,
		// keeping its room until the unit is gone and owing it a stop that is issued only once
		// its state proves it this launch's.
		n.failRecovery(r, lifetime, epochErr.Error())
	case uerr != nil:
		r.held, r.heldErr = true, fmt.Errorf("%w: %v", ErrRunLaunchUncertain, uerr)
		r.reason = boundReason(r.heldErr.Error())
		n.mu.Lock()
		n.rooms[spec.RoomID] = r
		n.mu.Unlock()
		n.wg.Add(1)
		go r.waitUnit(lifetime)
		n.log.Printf("run %q room %q: recovered held in starting: %v", id, spec.RoomID, uerr)
	case !present:
		verified := false
		if r.ep != nil {
			if snap, err := r.ep.src.Snapshot(); err == nil && snap.Terminal != nil {
				verified, r.reason = snap.Terminal.Verified, boundReason(snap.Terminal.Reason)
			}
		}
		switch {
		case verified:
			r.state = RunEnded
		case epochErr != nil:
			r.state, r.reason = RunFailed, boundReason(epochErr.Error())
		default:
			r.state = RunFailed
			if r.reason == "" {
				r.reason = "worker unit gone across restart"
			}
		}
		r.ended = time.Now()
		if tl, err := OpenTimeline(ctx, TimelineOptions{Directory: RoomDir(n.o.StateDir, id, spec.RoomID), Room: spec.RoomID}, n.resolve); err == nil {
			switch {
			case verified:
				if err := tl.End(); err != nil && !errors.Is(err, ErrTimelineInvalid) {
					n.log.Printf("run %q: end room: %v", id, err)
				}
			case r.ep != nil:
				if err := tl.FailSource(r.ep.key, r.reason, monotonicNow()); err != nil && !errors.Is(err, ErrTimelineInvalid) {
					n.log.Printf("run %q: fail source: %v", id, err)
				}
			}
			tl.Close()
		} else {
			n.log.Printf("run %q: recovered journal: %v", id, err)
		}
		r.gone = true
		r.complete() // frees the room: the unit is gone
		if r.ep != nil {
			r.release(ctx)
		}
		n.log.Printf("run %q room %q: recovered %s: %s", id, spec.RoomID, r.state, r.reason)
	case epochErr != nil:
		n.failRecovery(r, lifetime, epochErr.Error())
	default:
		n.adoptLive(ctx, r, lifetime)
	}
}

// recoverEpoch reopens the run's reserved epoch from the inherited descriptors when its
// directory exists: the worker connected before the restart. No directory means the worker
// has not connected yet and there is nothing to open. A directory of another epoch, a
// directory listing that cannot be read, or a reserved epoch whose owner record or
// descriptors are missing or do not match is an error: the recovery must fail rather than
// serve media from an epoch the record did not reserve.
func (n *Node) recoverEpoch(ctx context.Context, r *run, reserved uint64, inherited map[string]int) error {
	id := r.spec.RunID
	latest, err := latestEpoch(filepath.Dir(EpochDir(n.o.StateDir, id, id, 1)))
	switch {
	case err != nil:
		return fmt.Errorf("epoch directories across restart: %v", err)
	case latest == 0:
		return nil
	case latest != reserved:
		return fmt.Errorf("epoch directory %d exists, the record reserved epoch %d; not adopted", latest, reserved)
	}
	src, err := openRecoveredSource(ctx, EpochDir(n.o.StateDir, id, id, reserved), inherited)
	if err != nil {
		return fmt.Errorf("epoch %d unusable across restart: %v", reserved, err)
	}
	r.ep = &epochState{key: EpochKey{Run: id, Source: id, Epoch: reserved}, src: src, owner: src.owner}
	return nil
}

// failRecovery settles a run whose recovery cannot continue, through the ordinary end of a
// run: nothing is served, the room stays owned until the terminal record is durable and the
// unit is gone, the unit is owed a stop, issued once its state proves it this launch's and
// retried until taken, and watched until it is gone, and the epoch, if one was opened, is
// released then.
func (n *Node) failRecovery(r *run, lifetime context.Context, reason string) {
	n.mu.Lock()
	if n.rooms[r.spec.RoomID] == nil {
		n.rooms[r.spec.RoomID] = r
	}
	n.mu.Unlock()
	r.finish(false, "recovery failed: "+reason, true)
	n.wg.Add(1)
	go r.waitUnit(lifetime)
	n.log.Printf("run %q room %q: recovery failed: %s; unit %s asked to stop and watched", r.spec.RunID, r.spec.RoomID, reason, r.unit.Name())
}

// adoptFinished recovers a run whose record says it is over: it is reported as it ended, and
// the reserved epoch, when its directory exists, is reopened from the inherited descriptors so
// that it can be released. A unit of its name confirmed absent settles it at once: the epoch
// is released and the room is free. A unit still loaded with its launch id, or one whose state
// cannot be established, keeps the room owned and the epoch held: the run owes the unit a
// stop, recorded, issued here when the unit's state proves it this launch's and retried by
// maintain until it is taken, and the unit is watched until it is gone, which frees the room
// and releases the epoch. Descriptors of any other epoch are left for recover to drop.
func (n *Node) adoptFinished(ctx context.Context, r *run, rec runRecord, inherited map[string]int) {
	id := r.spec.RunID
	r.state, r.reason, r.ended, r.durable, r.epoch, r.stopPending = rec.FinalState, rec.FinalReason, time.Unix(0, rec.EndedUnixNS), true, rec.Epoch, rec.StopPending
	switch latest, err := latestEpoch(filepath.Dir(EpochDir(n.o.StateDir, id, id, 1))); {
	case err != nil:
		n.log.Printf("run %q: epoch directories across restart: %v", id, err)
	case latest == rec.Epoch:
		if src, err := openRecoveredSource(ctx, EpochDir(n.o.StateDir, id, id, rec.Epoch), inherited); err == nil {
			r.ep = &epochState{key: EpochKey{Run: id, Source: id, Epoch: rec.Epoch}, src: src, owner: src.owner}
		} else {
			n.log.Printf("run %q: epoch %d not reopened for release: %v", id, rec.Epoch, err)
		}
	case latest != 0:
		n.log.Printf("run %q: epoch directory %d exists, the record reserved epoch %d; its descriptors are not this run's", id, latest, rec.Epoch)
	}
	unit, present, uerr := n.o.Launcher.Adopt(ctx, id, rec.LaunchID)
	r.unit = unit
	n.mu.Lock()
	n.runs[id] = r
	n.mu.Unlock()
	if uerr == nil && !present {
		r.mu.Lock()
		r.gone = true
		r.mu.Unlock()
		r.clearOwedStop() // a record that still says the stop is owed is rewritten
		r.freeRoom()
		if r.ep != nil {
			r.release(ctx)
		}
		n.log.Printf("run %q room %q: recovered %s; unit gone", id, r.spec.RoomID, r.state)
		return
	}
	n.mu.Lock()
	n.rooms[r.spec.RoomID] = r
	n.mu.Unlock()
	r.mu.Lock()
	r.stopPending = true
	r.mu.Unlock()
	if !rec.StopPending {
		if err := r.persist(); err != nil {
			n.log.Printf("%v", err)
		}
	}
	r.issueStop(ctx)
	n.wg.Add(1)
	go r.waitUnit(n.lifetime())
	if uerr != nil {
		n.log.Printf("run %q room %q: recovered %s; unit %s not established, room owned and unit watched: %v", id, r.spec.RoomID, r.state, unit.Name(), uerr)
		return
	}
	n.log.Printf("run %q room %q: recovered %s; unit %s still loaded, asked to stop and watched", id, r.spec.RoomID, r.state, unit.Name())
}

// adoptLive finishes the recovery of a run whose unit is loaded with its launch id and whose
// reserved epoch is either reopened or not yet created: the room's journal is reopened and
// the run registered where the journal left it, running only for a live generation of the
// reserved epoch that is neither ended nor failed, starting when the room was never bound,
// and the run's socket is listened on again for the worker's reconnect. A journal that cannot
// be opened or observed, a journal bound to an epoch this incarnation does not hold, a socket
// that cannot be listened on or a room that cannot be registered fails the recovery.
func (n *Node) adoptLive(ctx context.Context, r *run, lifetime context.Context) {
	id, spec := r.spec.RunID, r.spec
	tl, err := OpenTimeline(ctx, TimelineOptions{Directory: RoomDir(n.o.StateDir, id, spec.RoomID), Room: spec.RoomID}, n.resolve)
	if err != nil {
		n.failRecovery(r, lifetime, "room journal across restart: "+err.Error())
		return
	}
	r.tl = tl
	snap, _, err := tl.Observe()
	if err != nil {
		n.failRecovery(r, lifetime, "room journal across restart: "+err.Error())
		return
	}
	// The room is this run's from here on, whatever the journal says: a run that is over keeps
	// it until its unit is gone (freeRoom).
	n.mu.Lock()
	n.rooms[spec.RoomID] = r
	n.mu.Unlock()
	// The journal is the durable truth of a run that is over before its record says so: an
	// ended room ended the run, a failed or frozen live generation failed it, and neither is
	// ever served again. The worker, if still running, is asked to stop.
	switch {
	case snap.Ended:
		r.finish(true, "ended before the restart", true)
	case snap.Failed, snap.Live != nil && snap.Live.Frozen():
		reason := "timeline failed before the restart"
		if snap.FailReason != "" {
			reason = snap.FailReason
		}
		r.finish(false, reason, true)
	case snap.Live != nil && (r.ep == nil || snap.Live.Key != r.ep.key):
		n.failRecovery(r, lifetime, fmt.Sprintf("room journal is bound to epoch %d, which this incarnation does not hold", snap.Live.Key.Epoch))
		return
	}
	if st := r.status(); st.State.Terminal() {
		n.wg.Add(1)
		go r.waitUnit(lifetime)
		n.log.Printf("run %q room %q: recovered %s (%s); unit %s asked to stop and watched", id, spec.RoomID, st.State, st.Reason, r.unit.Name())
		return
	}
	ln, err := listenPacket(r.socket)
	if err != nil {
		n.failRecovery(r, lifetime, "socket across restart: "+err.Error())
		return
	}
	r.ln = ln
	if snap.Live != nil {
		r.ep.mu.Lock()
		r.ep.exposed = true
		r.ep.mu.Unlock()
		if err := n.ctrl.Register(tl); err != nil {
			n.failRecovery(r, lifetime, "room registration across restart: "+err.Error())
			return
		}
		r.exposed, r.state = true, RunRunning
		if r.started.IsZero() { // the record normally carries the original
			r.started = time.Now()
		}
	}
	n.wg.Add(2)
	go r.accept(lifetime)
	go r.waitUnit(lifetime)
	if r.ep != nil {
		n.wg.Add(1)
		go r.follow(lifetime, r.ep)
	}
	n.log.Printf("run %q room %q: recovered %s (epoch %d, unit %s launch %s): ipc %s", id, spec.RoomID, r.state, r.epoch, r.unit.Name(),
		r.launchID, r.socket)
}

// lifetime returns the Run context.
func (n *Node) lifetime() context.Context {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ctx
}

// latestEpoch returns the highest epoch directory under root, 0 when there is none.
func latestEpoch(root string) (uint64, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var high uint64
	for _, e := range entries {
		n, err := strconv.ParseUint(e.Name(), 10, 64)
		if err != nil || !e.IsDir() || strconv.FormatUint(n, 10) != e.Name() {
			continue
		}
		high = max(high, n)
	}
	return high, nil
}

// openRecoveredSource reopens the epoch in dir from the inherited descriptors its owner record
// names, taking them out of inherited; ErrLostEpoch when the record or a descriptor is missing.
func openRecoveredSource(ctx context.Context, dir string, inherited map[string]int) (*Source, error) {
	dirfd, err := openPrivateDir(dir)
	if err != nil {
		return nil, err
	}
	state, err := readOwnerState(dirfd)
	closeFD(dirfd)
	if err != nil {
		return nil, err
	}
	fds := make(map[string]int, len(state.Tracks))
	for _, id := range state.Tracks {
		fd, ok := inherited[id.Name]
		if !ok {
			return nil, fmt.Errorf("%w: no inherited descriptor named %q", ErrLostEpoch, id.Name)
		}
		fds[id.Name] = fd
	}
	for name := range fds {
		delete(inherited, name)
	}
	return OpenSource(ctx, dir, fds) // consumes fds on every path
}

// epochState is one admitted worker epoch and what the node has done with it.
type epochState struct {
	key      EpochKey
	src      *Source
	owner    WorkerOwner
	exited   atomic.Bool // the worker process is gone
	watching atomic.Bool // a pidfd of the worker is being watched
	closed   atomic.Bool // the source is closed for release; read by resolve without mu

	mu           sync.Mutex
	exposed      bool     // Bind issued, or given up on
	maintainErr  bool     // a Maintain failure was logged
	removals     []string // descriptors whose removal from the store has not been sent yet
	removeLogged bool     // a removal that could not be sent was logged
}

// run is one registered run.
type run struct {
	n         *Node
	spec      RunSpec
	socket    string
	launchID  string // the launch id its unit carries
	epoch     uint64 // the epoch its worker is admitted into
	recordDir string
	ln        *net.UnixListener
	tl        *Timeline
	tlClosed  bool // the timeline is closed; under mu
	unit      WorkerUnit
	ready     chan struct{} // closed once the launch has succeeded, been held or been rolled back
	launchErr error         // set before ready closes on a rollback
	adopted   bool          // recovered from a record rather than started here
	retiredCh chan struct{} // closed once retire has freed the room

	admit    sync.Mutex // serializes epoch creation
	recordMu sync.Mutex // serializes record writes; the record is built under mu inside it

	mu          sync.Mutex
	state       RunState
	reason      string
	held        bool  // the launch outcome is not established; heldErr says why
	heldErr     error // wraps ErrRunLaunchUncertain
	created     time.Time
	started     time.Time
	ended       time.Time
	ep          *epochState
	exposed     bool // the timeline is registered with the controller
	gone        bool // the unit is gone: its watcher read its absence or its end back from systemd
	durable     bool // the terminal state is durable in the record
	pending     bool // the record is older than the state; maintain rewrites it (complete)
	retired     bool // durable and gone: the room is freed and retiredCh closed
	stopPending bool // the run owes its unit a stop not taken yet; maintain retries it
	stopBusy    bool // a retry of the owed stop is in flight
	stopRetryAt time.Time
	stopErrText string // the last refusal of the owed stop logged
	released    bool   // the source is closed and every removal of its descriptors has been sent
	lnClosed    bool
	finalUntil  time.Time // the final playlist of a clean end is served until then; zero otherwise
	superseded  bool      // a newer run was reserved for the room: nothing of this run is served again
}

// await waits for the run's launch to settle and returns it: the launch error of a rolled-back
// run, ErrRunLaunchUncertain with the status while the run is held, the status otherwise.
func (r *run) await(ctx context.Context) (RunStatus, bool, error) {
	select {
	case <-r.ready:
	case <-ctx.Done():
		return RunStatus{}, false, ctx.Err()
	}
	if r.launchErr != nil {
		return RunStatus{}, false, r.launchErr
	}
	r.mu.Lock()
	held, herr := r.held, r.heldErr
	r.mu.Unlock()
	if held {
		return r.status(), false, herr
	}
	return r.status(), false, nil
}

func (r *run) status() RunStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RunStatus{Spec: r.spec, State: r.state, Reason: r.reason, Uncertain: r.held, CreatedAt: r.created, StartedAt: r.started, EndedAt: r.ended}
}

// launch gives the run its directories, record, timeline, socket and unit, in that order, and
// undoes everything on a failure so that nothing of the run remains in memory, on disk, on the
// socket path or in systemd. A start without an established outcome keeps everything, records
// the reason and watches the unit the launcher returned with it; it is ErrRunLaunchUncertain.
func (r *run) launch(ctx context.Context) (err error) {
	n := r.n
	id := r.spec.RunID
	srcRoot := filepath.Dir(EpochDir(n.o.StateDir, id, id, 1))
	roomDir := RoomDir(n.o.StateDir, id, r.spec.RoomID)
	for _, d := range []string{srcRoot, roomDir} {
		if err := ensurePrivateDir(d); err != nil {
			return err
		}
	}
	if r.epoch, err = nextEpoch(srcRoot); err != nil {
		return err
	}
	if r.launchID, err = systemd.NewLaunchID(); err != nil {
		return err
	}
	recordDir := RunDir(n.o.StateDir, id)
	r.recordDir = recordDir
	if err := r.persist(); err != nil { // the reserved epoch and launch id are durable before the unit exists
		return err
	}
	tl, err := OpenTimeline(ctx, TimelineOptions{Directory: roomDir, Room: r.spec.RoomID}, n.resolve)
	if err != nil {
		r.dropRecord(recordDir)
		return err
	}
	ln, err := listenPacket(r.socket)
	if err != nil {
		tl.Close()
		r.dropRecord(recordDir)
		return err
	}
	unit, err := n.o.Launcher.Start(ctx, WorkerSpec{Run: id, LaunchID: r.launchID, Socket: r.socket, StateDir: n.o.StateDir, URL: r.spec.LiveKitURL,
		Room: r.spec.LiveKitRoom, RoomID: r.spec.RoomID, Host: r.spec.HostIdentity, SupervisorUID: n.o.WorkerUID, Config: n.o.Config})
	switch {
	case err == nil:
	case errors.Is(err, systemd.ErrLaunchUncertain) && unit != nil:
		err = fmt.Errorf("%w: %v", ErrRunLaunchUncertain, err)
		r.mu.Lock()
		r.held, r.heldErr, r.reason = true, err, boundReason(err.Error())
		r.mu.Unlock()
	default:
		ln.Close() // unlinks the socket
		tl.Close()
		r.dropRecord(recordDir)
		return err
	}
	r.tl, r.ln, r.unit = tl, ln, unit
	lifetime := n.lifetime()
	n.wg.Add(2)
	go r.accept(lifetime)
	go r.waitUnit(lifetime)
	return err
}

// record returns the run's durable record as it stands, under mu.
func (r *run) record() runRecord {
	rec := runRecord{Format: runFormat, RunID: r.spec.RunID, RoomID: r.spec.RoomID, LiveKitRoom: r.spec.LiveKitRoom,
		HostIdentity: r.spec.HostIdentity, LiveKitURL: r.spec.LiveKitURL, LaunchID: r.launchID, Epoch: r.epoch,
		CreatedUnixNS: r.created.UnixNano()}
	if !r.started.IsZero() {
		rec.StartedUnixNS = r.started.UnixNano()
	}
	if r.state.Terminal() {
		rec.FinalState, rec.FinalReason, rec.EndedUnixNS, rec.StopPending = r.state, r.reason, r.ended.UnixNano(), r.stopPending
	}
	return rec
}

// persist rewrites the run's record with its state as it stands when the write is made. Writes
// are serialized, and each one reads the state inside the critical section, so overlapping
// writes cannot leave an older state on disk: once the run is terminal every later write
// records that. The record is what a restarted supervisor reports the run from.
func (r *run) persist() error {
	r.recordMu.Lock()
	defer r.recordMu.Unlock()
	r.mu.Lock()
	rec := r.record()
	r.mu.Unlock()
	if err := writeRunRecord(r.recordDir, rec); err != nil {
		return fmt.Errorf("run %q: record: %w", r.spec.RunID, err)
	}
	return nil
}

// dropRecord removes the run's record after a rolled-back launch.
func (r *run) dropRecord(dir string) {
	if err := os.Remove(filepath.Join(dir, runFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		r.n.log.Printf("run %q: remove record: %v", r.spec.RunID, err)
	}
	_ = os.Remove(dir)
}

// closeListener closes the socket once; the path is unlinked with it.
func (r *run) closeListener() {
	r.mu.Lock()
	closed, ln := r.lnClosed, r.ln
	r.lnClosed = true
	r.mu.Unlock()
	if !closed && ln != nil {
		ln.Close()
	}
}

// accept admits connections on the run's socket until it closes.
func (r *run) accept(ctx context.Context) {
	defer r.n.wg.Done()
	for {
		conn, err := r.ln.AcceptUnix()
		if err != nil {
			return
		}
		r.n.wg.Add(1)
		go func() {
			defer r.n.wg.Done()
			r.admitConn(ctx, conn)
		}()
	}
}

// admitConn authenticates one connection as the main process of the run's unit, finds or
// creates the run's epoch for it and serves it. The hello is left unread for ServeConn.
func (r *run) admitConn(ctx context.Context, conn *net.UnixConn) {
	n := r.n
	id, err := ipc.PeerProcessIdentity(conn)
	if err != nil {
		n.log.Printf("run %q: connection refused: %v", r.spec.RunID, err)
		conn.Close()
		return
	}
	refuse := func(why string) {
		// A reject, rather than a bare close, makes the worker fail at once instead of retrying.
		n.log.Printf("run %q: pid %d not admitted: %s", r.spec.RunID, id.PID, why)
		closeFD(id.Pidfd)
		_ = ipc.WritePublicationMsg(conn, rejectPacket(reasonRefused), nil, time.Now().Add(netDeadline))
		conn.Close()
	}
	if id.UID != n.o.WorkerUID {
		refuse(fmt.Sprintf("peer uid %d, want uid %d", id.UID, n.o.WorkerUID))
		return
	}
	switch owns, err := r.unit.Owns(ctx, id.PID); {
	case err != nil:
		refuse("the unit's main process cannot be read: " + err.Error())
		return
	case !owns:
		refuse(fmt.Sprintf("not the main process of unit %s with launch %s", r.unit.Name(), r.unit.LaunchID()))
		return
	}
	owner := WorkerOwner{UID: id.UID, PID: id.PID, Ino: id.Ino}
	ep, err := r.epochFor(ctx, owner, id.Pidfd) // consumes the pidfd on every path
	if err != nil {
		n.log.Printf("run %q: worker pid %d not admitted: %v", r.spec.RunID, id.PID, err)
		_ = ipc.WritePublicationMsg(conn, rejectPacket(reasonRefused), nil, time.Now().Add(netDeadline))
		conn.Close()
		return
	}
	switch err := ep.src.ServeConn(ctx, conn); {
	case err == nil, errors.Is(err, ErrBusy), ctx.Err() != nil:
	default:
		n.log.Printf("run %q: epoch %d session ended: %v", r.spec.RunID, ep.key.Epoch, err)
	}
}

// epochFor returns the run's epoch for owner, creating it for the first process while the run
// has not ended; any other process, or a run that has, is refused. The pidfd is kept by the
// exit watcher of the epoch's first connection and closed on every other path.
func (r *run) epochFor(ctx context.Context, owner WorkerOwner, pidfd int) (*epochState, error) {
	n := r.n
	r.admit.Lock()
	defer r.admit.Unlock()
	r.mu.Lock()
	ep, state := r.ep, r.state
	r.mu.Unlock()
	switch {
	case ep != nil && ep.owner == owner && !ep.exited.Load():
		if ep.watching.CompareAndSwap(false, true) {
			n.wg.Add(1)
			go r.watch(ctx, ep, pidfd)
		} else {
			closeFD(pidfd)
		}
		return ep, nil
	case ep != nil:
		closeFD(pidfd)
		return nil, fmt.Errorf("the run's worker is pid %d", ep.owner.PID)
	case state != RunStarting && state != RunRunning:
		closeFD(pidfd)
		return nil, fmt.Errorf("the run is %s", state)
	}
	id := r.spec.RunID
	dir := EpochDir(n.o.StateDir, id, id, r.epoch)
	if err := os.Mkdir(dir, 0o700); err != nil {
		closeFD(pidfd)
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		closeFD(pidfd)
		return nil, err
	}
	src, err := CreateSource(ctx, SourceOptions{Directory: dir, Run: id, Source: id, Epoch: r.epoch, Config: n.o.Config,
		Policy: n.o.Policy, Owner: owner}, n.notifier)
	if err != nil {
		closeFD(pidfd)
		return nil, err
	}
	ep = &epochState{key: EpochKey{Run: id, Source: id, Epoch: r.epoch}, src: src, owner: owner}
	ep.watching.Store(true)
	r.mu.Lock()
	if state = r.state; state.Terminal() {
		r.mu.Unlock()
		closeFD(pidfd)
		r.dropSource(ctx, ep)
		return nil, fmt.Errorf("the run is %s", state)
	}
	r.ep = ep
	r.mu.Unlock()
	n.log.Printf("run %q: epoch %d created for worker pid %d (pidfs inode %d) in %s", id, r.epoch, owner.PID, owner.Ino, dir)
	n.wg.Add(2)
	go r.watch(ctx, ep, pidfd)
	go r.follow(ctx, ep)
	return ep, nil
}

// watch owns the worker's pidfd until the process exits or ctx ends, then closes it. The exit
// settles the run unless a verified terminal did.
func (r *run) watch(ctx context.Context, ep *epochState, pidfd int) {
	defer r.n.wg.Done()
	defer closeFD(pidfd)
	for ctx.Err() == nil {
		fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
		cnt, err := unix.Poll(fds, pidfdPollMS)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			r.n.log.Printf("run %q: epoch %d: pidfd poll: %v", r.spec.RunID, ep.key.Epoch, err)
			return
		}
		if cnt > 0 && fds[0].Revents != 0 {
			break
		}
	}
	if ctx.Err() != nil {
		return
	}
	ep.exited.Store(true)
	r.workerGone(fmt.Sprintf("worker pid %d exited", ep.owner.PID))
}

// waitUnit watches the run's unit until it is gone, then settles the run unless a verified
// terminal did, and frees the room of a run whose terminal record is durable. A watch systemd
// cannot answer is retried: the run must not be left unwatched while its unit may exist.
func (r *run) waitUnit(ctx context.Context) {
	defer r.n.wg.Done()
	for {
		exit, err := r.unit.Wait(ctx)
		if err == nil {
			r.mu.Lock()
			r.gone = true
			r.mu.Unlock()
			r.clearOwedStop()
			r.workerGone(fmt.Sprintf("worker unit %s exited: %s", r.unit.Name(), exit))
			r.freeRoom()
			return
		}
		if ctx.Err() != nil {
			return
		}
		r.n.log.Printf("run %q: unit %s: wait: %v; retrying in %v", r.spec.RunID, r.unit.Name(), err, unitWatchRetry)
		select {
		case <-ctx.Done():
			return
		case <-time.After(unitWatchRetry):
		}
	}
}

// workerGone settles the run after its worker is gone: a verified terminal the worker committed
// before leaving ends it (or stops it, when a stop was requested); anything else fails it.
func (r *run) workerGone(reason string) {
	verified := false
	r.mu.Lock()
	ep := r.ep
	r.mu.Unlock()
	if ep != nil {
		if snap, err := ep.src.Snapshot(); err == nil && snap.Terminal != nil {
			verified, reason = snap.Terminal.Verified, snap.Terminal.Reason
		}
	}
	r.finish(verified, reason, false)
}

// follow drives the room from the epoch's publications: it binds the room to the epoch once its
// start is durable and settles the run on the worker's terminal.
func (r *run) follow(ctx context.Context, ep *epochState) {
	defer r.n.wg.Done()
	for {
		obs, err := ep.src.Observe()
		if err != nil {
			r.n.log.Printf("run %q: epoch %d: observe: %v", r.spec.RunID, ep.key.Epoch, err)
			return
		}
		if obs.Closed {
			return
		}
		r.expose(ep, obs.Snapshot)
		if t := obs.Snapshot.Terminal; t != nil {
			r.finish(t.Verified, t.Reason, true)
			return
		}
		select {
		case <-obs.Changed:
		case <-ctx.Done():
			return
		}
	}
}

// expose binds the room to ep once its presentation, inits and first segment are durable, then
// registers the timeline so the playlists serve and the run is running. A refusal other than
// "not ready" fails the run. A run that is no longer starting is left unexposed.
func (r *run) expose(ep *epochState, snap partstore.RingSnapshot) {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	if ep.exposed || ep.exited.Load() {
		return
	}
	ready, err := startDurable(ep.src, snap, r.spec.RoomID)
	if err != nil {
		ep.exposed = true
		r.finish(false, "cannot be exposed: "+err.Error(), true)
		return
	}
	if !ready {
		return
	}
	r.mu.Lock()
	state := r.state
	r.mu.Unlock()
	if state != RunStarting {
		ep.exposed = true
		return
	}
	switch err := r.tl.Bind(ep.key, 1); {
	case errors.Is(err, ErrTimelineNotReady):
		return
	case err != nil:
		ep.exposed = true
		r.finish(false, "cannot be exposed: "+err.Error(), true)
		return
	}
	ep.exposed = true
	r.mu.Lock()
	if r.state != RunStarting {
		r.mu.Unlock()
		return
	}
	if err := r.n.ctrl.Register(r.tl); err != nil {
		r.mu.Unlock()
		r.finish(false, "cannot be exposed: "+err.Error(), true)
		return
	}
	r.exposed, r.state, r.started = true, RunRunning, time.Now()
	r.held, r.heldErr, r.reason = false, nil, ""
	r.mu.Unlock()
	if err := r.persist(); err != nil {
		r.n.log.Printf("%v", err)
	}
	r.n.log.Printf("run %q room %q: running: epoch %d exposed", r.spec.RunID, r.spec.RoomID, ep.key.Epoch)
}

// unexpose takes the timeline out of the controller once: the playlists are unavailable.
func (r *run) unexpose() {
	r.mu.Lock()
	exposed := r.exposed
	r.exposed = false
	r.mu.Unlock()
	if exposed {
		r.n.ctrl.Remove(r.spec.RoomID)
	}
}

// requestStop asks the run's unit to stop and, once the request is taken, makes the playlists
// unavailable and moves the run to stopping; it settles stopped once the worker is gone. A
// request the unit refuses or systemd cannot take leaves the run exactly as it was and is
// ErrRunStop. A run already ending is returned unchanged; a stop already requested is issued
// again. A run that is over is returned as it is, except that one still owing its unit a stop
// issues it again: ErrRunStop with the run when that is refused too, and nothing once the unit
// is gone.
func (r *run) requestStop(ctx context.Context) (RunStatus, error) {
	r.mu.Lock()
	if r.state.Terminal() {
		owed := r.stopPending && !r.gone
		r.mu.Unlock()
		if owed {
			if err := r.issueStop(ctx); err != nil {
				return r.status(), fmt.Errorf("%w: %v", ErrRunStop, err)
			}
		}
		return r.status(), nil
	}
	r.mu.Unlock()
	if err := r.unit.Stop(ctx); err != nil {
		r.n.log.Printf("run %q: unit %s: stop not issued: %v", r.spec.RunID, r.unit.Name(), err)
		return r.status(), fmt.Errorf("%w: %v", ErrRunStop, err)
	}
	r.mu.Lock()
	if !r.state.Terminal() && r.state != RunStopping {
		r.state = RunStopping
		r.held, r.heldErr = false, nil
		r.n.log.Printf("run %q room %q: stopping on request", r.spec.RunID, r.spec.RoomID)
	}
	r.mu.Unlock()
	r.unexpose()
	return r.status(), nil
}

// finish settles the run once in a terminal state: stopped when a stop was requested, ended on a
// verified end, failed otherwise. A settled run is left as it is. stopUnit makes the run owe its
// unit a stop, for a worker that may still be alive: the debt is recorded with the terminal
// state, before the stop is attempted, and stays until the stop is taken or the unit is gone.
func (r *run) finish(verified bool, reason string, stopUnit bool) {
	reason = boundReason(reason)
	r.mu.Lock()
	if r.state.Terminal() {
		r.mu.Unlock()
		return
	}
	switch {
	case r.state == RunStopping:
		r.state = RunStopped
	case verified:
		r.state = RunEnded
	default:
		r.state = RunFailed
	}
	r.reason, r.ended = reason, time.Now()
	r.held, r.heldErr = false, nil
	if stopUnit && !r.gone {
		r.stopPending = true
	}
	state, ep := r.state, r.ep
	r.mu.Unlock()
	r.retire(state, verified, reason, ep)
	r.mu.Lock()
	owed := r.stopPending && !r.gone // a stop requested meanwhile may have taken it
	r.mu.Unlock()
	if owed {
		r.issueStop(context.Background())
	}
}

// issueStop asks the run's unit for the stop the run owes it, with a budget of its own. A stop
// the unit takes, or a unit no longer loaded, settles the debt, recorded again when the run is
// over; a stop the unit refuses, because its state does not prove it this launch's, or systemd
// cannot take keeps the debt, and maintain asks again after unitStopRetry until it is taken or
// the unit is gone. A refusal is logged when it is the first or differs from the last logged.
func (r *run) issueStop(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, unitStopBudget)
	err := r.unit.Stop(ctx)
	cancel()
	r.mu.Lock()
	if err == nil {
		retried, terminal := r.stopErrText != "", r.state.Terminal()
		r.stopPending, r.stopRetryAt, r.stopErrText = false, time.Time{}, ""
		r.mu.Unlock()
		if retried {
			r.n.log.Printf("run %q: unit %s: stop taken after retries", r.spec.RunID, r.unit.Name())
		}
		if terminal {
			if err := r.persist(); err != nil {
				r.n.log.Printf("%v", err)
			}
		}
		return nil
	}
	r.stopRetryAt = time.Now().Add(unitStopRetry)
	logged := parent.Err() == nil && err.Error() != r.stopErrText
	if logged {
		r.stopErrText = err.Error()
	}
	r.mu.Unlock()
	if logged {
		r.n.log.Printf("run %q: unit %s: stop not taken: %v; the stop stays owed and is retried", r.spec.RunID, r.unit.Name(), err)
	}
	return err
}

// clearOwedStop settles the stop the run owed its unit once the unit is gone: nothing is owed
// any more, and a terminal record that carried the debt is rewritten so that it no longer
// does. A write that fails leaves the record dirty, retried by maintain through complete; the
// room's reuse does not wait for it, the terminal state being durable already.
func (r *run) clearOwedStop() {
	r.mu.Lock()
	owed, dirty := r.stopPending, r.pending
	r.stopPending, r.stopRetryAt, r.stopErrText = false, time.Time{}, ""
	r.mu.Unlock()
	if !owed || dirty { // nothing recorded as owed, or the record is being rewritten anyway
		return
	}
	if err := r.persist(); err != nil {
		r.mu.Lock()
		r.pending = true
		r.mu.Unlock()
		r.n.log.Printf("%v; the record still says the stop is owed and is rewritten by maintenance", err)
	}
}

// retire runs once, after the run entered state, in a fixed order: the playlists are taken out
// of the controller, the journal records the end or the failure, the timeline and the socket
// are closed, the terminal state is made durable in the record (complete), and only then, and
// once the unit is gone, is the room freed for a later run (freeRoom). A room the verified
// terminal ends cleanly is not closed here: its final playlist, ended, stays served through
// Lookup for finalPlaylistGrace, and maintain closes it then. The epoch stays open for the
// worker's last publications, and through that grace, until it is released after the unit is
// gone.
func (r *run) retire(state RunState, verified bool, reason string, ep *epochState) {
	n := r.n
	r.unexpose()
	if r.tl != nil {
		clean := false
		switch {
		case verified:
			switch err := r.tl.End(); {
			case err == nil:
				clean = true
			case !errors.Is(err, ErrTimelineInvalid):
				n.log.Printf("run %q: end room: %v", r.spec.RunID, err)
			}
		case ep != nil:
			if err := r.tl.FailSource(ep.key, reason, monotonicNow()); err != nil && !errors.Is(err, ErrTimelineInvalid) {
				n.log.Printf("run %q: fail source: %v", r.spec.RunID, err)
			}
		}
		if clean {
			r.mu.Lock()
			r.finalUntil = time.Now().Add(finalPlaylistGrace)
			r.mu.Unlock()
			n.mu.Lock()
			n.final[r.spec.RoomID] = r
			n.mu.Unlock()
			n.log.Printf("run %q room %q: %s: final playlist served for %v", r.spec.RunID, r.spec.RoomID, state, finalPlaylistGrace)
		} else {
			r.mu.Lock()
			r.tlClosed = true
			r.mu.Unlock()
			r.tl.Close()
		}
	}
	r.closeListener()
	r.complete()
}

// complete finishes a retirement: the terminal record must be durable before the run counts as
// over for its room. While the record cannot be written the room stays owned and the retirement
// stays pending, retried by maintain; a run that is over but not retired is never reused. The
// room itself is freed by freeRoom, once the unit is gone as well. A record left dirty after the
// terminal state was durable (clearOwedStop) is rewritten here too.
func (r *run) complete() {
	n := r.n
	if err := r.persist(); err != nil {
		r.mu.Lock()
		first := !r.pending
		r.pending = true
		r.mu.Unlock()
		if first {
			n.log.Printf("%v; the room stays owned until the terminal state is durable", err)
		}
		return
	}
	r.mu.Lock()
	r.pending = false
	if r.durable {
		r.mu.Unlock()
		return
	}
	r.durable = true
	state, reason := r.state, r.reason
	r.mu.Unlock()
	n.log.Printf("run %q room %q: %s: %s", r.spec.RunID, r.spec.RoomID, state, reason)
	r.freeRoom()
}

// freeRoom frees the run's room once the run is over on both sides: its terminal record is
// durable and its unit is gone. Until both hold the room stays owned, so no run of another id
// starts in it beside a worker that may still be alive, and a Start for it waits.
func (r *run) freeRoom() {
	r.mu.Lock()
	if r.retired || !r.durable || !r.gone {
		r.mu.Unlock()
		return
	}
	r.retired = true
	r.mu.Unlock()
	n := r.n
	n.mu.Lock()
	if n.rooms[r.spec.RoomID] == r {
		delete(n.rooms, r.spec.RoomID)
	}
	n.mu.Unlock()
	close(r.retiredCh)
	n.log.Printf("run %q room %q: retired: record durable and unit gone; the room is free", r.spec.RunID, r.spec.RoomID)
}

// markSuperseded marks the run superseded because a newer run was reserved for its room: its
// final-playlist grace is over, nothing of it is looked up or resolved again, and its epoch is
// released by maintain as soon as the run is retired, under the ordering every release follows.
// Start calls it with n.mu held, so the mark is part of the reservation; only the run's own mu
// is taken here, nothing blocking is done, and the timeline returned, when the run still had
// one open, is closed by the caller once n.mu is released. The newer run's timeline and epoch
// are its own and untouched. Safe to repeat: nil the second time.
func (r *run) markSuperseded() *Timeline {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.superseded {
		return nil
	}
	r.superseded, r.finalUntil = true, time.Time{}
	if r.tl == nil || r.tlClosed {
		return nil
	}
	r.tlClosed = true
	return r.tl
}

// release closes the epoch's source and asks the store to drop its descriptors, once each: the
// source is closed on the first call, and a removal the store could not be asked for is asked
// for again on the next (maintain). The epoch counts as released only once every removal has
// been sent; a sent removal is a request systemd processes on its own, which nothing here
// confirms.
func (r *run) release(ctx context.Context) {
	r.mu.Lock()
	ep, released := r.ep, r.released
	r.mu.Unlock()
	if ep == nil || released {
		return
	}
	if ep.closed.CompareAndSwap(false, true) {
		info := ep.src.Info()
		ep.mu.Lock()
		for _, id := range info.Tracks {
			ep.removals = append(ep.removals, id.Name)
		}
		ep.mu.Unlock()
		if err := ep.src.Close(); err != nil {
			r.n.log.Printf("run %q: epoch %d: close: %v", r.spec.RunID, ep.key.Epoch, err)
		}
	}
	ep.mu.Lock()
	kept := ep.removals[:0]
	for _, name := range ep.removals {
		if err := r.n.notifier.RemoveFD(ctx, name); err != nil {
			kept = append(kept, name)
			if ctx.Err() == nil && !ep.removeLogged {
				ep.removeLogged = true
				r.n.log.Printf("run %q: epoch %d: remove %s from the store: %v; asked again until sent", r.spec.RunID, ep.key.Epoch, name, err)
			}
		}
	}
	ep.removals = kept
	sent := len(kept) == 0
	ep.mu.Unlock()
	if !sent {
		return
	}
	r.mu.Lock()
	r.released = true
	r.mu.Unlock()
	r.n.log.Printf("run %q: epoch %d released: every removal from the store sent", r.spec.RunID, ep.key.Epoch)
}

// dropSource closes ep's source and asks the store once to drop its descriptors, for an epoch
// created for a run that ended meanwhile and was never served; a removal that cannot be sent is
// logged.
func (r *run) dropSource(ctx context.Context, ep *epochState) {
	info := ep.src.Info()
	if err := ep.src.Close(); err != nil {
		r.n.log.Printf("run %q: epoch %d: close: %v", r.spec.RunID, ep.key.Epoch, err)
	}
	for _, id := range info.Tracks {
		if err := r.n.notifier.RemoveFD(ctx, id.Name); err != nil && ctx.Err() == nil {
			r.n.log.Printf("run %q: epoch %d: remove %s from the store: %v", r.spec.RunID, ep.key.Epoch, id.Name, err)
		}
	}
}

// boundReason bounds a reason for a status: control characters become spaces and the text is
// cut to maxReason bytes at a rune boundary.
func boundReason(s string) string {
	s = strings.Map(func(c rune) rune {
		if unicode.IsControl(c) {
			return ' '
		}
		return c
	}, strings.ToValidUTF8(s, "?"))
	if len(s) <= maxReason {
		return s
	}
	cut := maxReason
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// startDurable reports whether the Source holds everything Bind needs: a presentation naming
// room, an init on every room track and the first part of segment 1 on every room track, the
// video one independent at a fragment start. A presentation that cannot name the room, or a
// first segment that cannot start the room, is an error.
func startDurable(src *Source, snap partstore.RingSnapshot, room string) (bool, error) {
	pres, err := LoadPresentation(src)
	if errors.Is(err, ErrTimelineNotReady) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	named := false
	for _, s := range pres.Slots {
		named = named || s.RoomID == room
	}
	if !named {
		return false, fmt.Errorf("%w: presentation names no slot for room %q", ErrTimelineInvalid, room)
	}
	for trk := 0; trk < RoomTracks; trk++ {
		if trk >= len(snap.Tracks) || snap.Tracks[trk].Init == nil {
			return false, nil
		}
		seg := findSegment(snap, trk, 1)
		if seg == nil || seg.Dropped || len(seg.Parts) == 0 {
			return false, nil
		}
		if first := seg.Parts[0]; trk < ipc.TrackAudioA && !(first.Independent && first.FragmentStart) {
			return false, fmt.Errorf("%w: track %d segment 1 does not start with an independent fragment", ErrTimelineInvalid, trk)
		}
	}
	return true, nil
}
