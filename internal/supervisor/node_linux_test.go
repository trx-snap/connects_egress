//go:build linux

package supervisor

// The node against a fake worker launcher and a fd store stand-in that keeps what it is given:
// the worker is this test process publishing through worker.Publisher into the run's socket (or
// a child process where the exit watcher matters), the unit's end, ownership and stop outcomes
// are the fake's, a restart is a second node over the same directories and the stand-in's
// descriptors, and the stand-in can be paused so that the store cannot be reached. Nothing here
// proves the systemd boundary; internal/systemd's launcher is tested on its own against fake
// executables.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
	"github.com/connectsapp/connects-egress/internal/worker"
	"golang.org/x/sys/unix"
)

const (
	nodeTestWait     = 5 * time.Second
	nodeTestURL      = "ws://127.0.0.1:7880"
	nodeHelperSocket = "CONNECTS_EGRESS_NODE_TEST_SOCKET"
	nodeHelperRun    = "CONNECTS_EGRESS_NODE_TEST_RUN"
)

var nodeTestCfg = ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 4096}

// nodeTestNotifier stands in for the systemd fd store: stored descriptors are kept by name,
// removals close them, a barrier's pipe is closed at once (which answers the barrier), and
// inherit hands duplicates of everything kept to a restarted node as LISTEN_FDS would.
type nodeTestNotifier struct {
	addr string
	conn *net.UnixConn
	done chan struct{}

	mu      sync.Mutex
	stored  map[string]int
	removed int
}

func newNodeTestNotifier(t *testing.T) *nodeTestNotifier {
	t.Helper()
	addr := fmt.Sprintf("@node-%d-%d", os.Getpid(), pubTestSeq.Add(1))
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		t.Fatalf("ListenUnixgram %q: %v", addr, err)
	}
	m := &nodeTestNotifier{addr: addr, conn: conn, done: make(chan struct{}), stored: map[string]int{}}
	go m.loop(conn, m.done)
	t.Cleanup(m.Close)
	return m
}

func (m *nodeTestNotifier) loop(conn *net.UnixConn, done chan struct{}) {
	defer close(done)
	buf, oob := make([]byte, 512), make([]byte, 256)
	for {
		n, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
		if err != nil {
			return
		}
		var fds []int
		if oobn > 0 {
			if scms, err := unix.ParseSocketControlMessage(oob[:oobn]); err == nil {
				for _, scm := range scms {
					got, _ := unix.ParseUnixRights(&scm)
					fds = append(fds, got...)
				}
			}
		}
		var name string
		store, remove := false, false
		for _, line := range strings.Split(strings.TrimSpace(string(buf[:n])), "\n") {
			switch {
			case line == "FDSTORE=1":
				store = true
			case line == "FDSTOREREMOVE=1":
				remove = true
			case strings.HasPrefix(line, "FDNAME="):
				name = strings.TrimPrefix(line, "FDNAME=")
			}
		}
		m.mu.Lock()
		switch {
		case store && len(fds) == 1:
			if old, dup := m.stored[name]; dup {
				_ = unix.Close(old)
			}
			m.stored[name] = fds[0]
			fds = nil
		case remove:
			if fd, ok := m.stored[name]; ok {
				_ = unix.Close(fd)
				delete(m.stored, name)
				m.removed++
			}
		}
		m.mu.Unlock()
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}
}

func (m *nodeTestNotifier) counts() (stored, removed int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.stored), m.removed
}

// inherit returns duplicates of every stored descriptor by name, the store keeping its own.
func (m *nodeTestNotifier) inherit(t *testing.T) map[string]int {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.stored))
	for name, fd := range m.stored {
		dup, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			t.Fatalf("inherit %s: %v", name, err)
		}
		out[name] = dup
	}
	return out
}

// pause stops answering the address: every notification sent to it fails, as it does when the
// store cannot be reached. What is stored is kept.
func (m *nodeTestNotifier) pause() {
	m.mu.Lock()
	conn, done := m.conn, m.done
	m.conn = nil
	m.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
		<-done
	}
}

// resume listens on the address again.
func (m *nodeTestNotifier) resume(t *testing.T) {
	t.Helper()
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: m.addr, Net: "unixgram"})
	if err != nil {
		t.Fatalf("ListenUnixgram %q again: %v", m.addr, err)
	}
	done := make(chan struct{})
	m.mu.Lock()
	m.conn, m.done = conn, done
	m.mu.Unlock()
	go m.loop(conn, done)
}

func (m *nodeTestNotifier) Close() {
	m.pause()
	m.mu.Lock()
	for name, fd := range m.stored {
		_ = unix.Close(fd)
		delete(m.stored, name)
	}
	m.mu.Unlock()
}

// fakeLauncher stands in for the transient unit launcher: Start records the spec and hands out
// the fakeUnit under the run's name, whose ownership, stop outcome and end the test decides, or,
// for an uncertain start, a handle for the run's launch id over a unit that carries another;
// Adopt finds the unit under the run's name as a restarted supervisor would: the unit itself
// when it carries the launch id, absent once it is gone, and an unestablished handle over it,
// with ErrLaunchUncertain, when it carries another.
type fakeLauncher struct {
	mu             sync.Mutex
	started        []WorkerSpec
	units          map[string]*fakeUnit
	startErr       error
	startUncertain bool          // Start returns ErrLaunchUncertain with an unowned handle
	gate           chan struct{} // when set, Start blocks until it is closed
}

const foreignLaunch = "ffffffffffffffffffffffffffffffff"

func (f *fakeLauncher) Start(ctx context.Context, spec WorkerSpec) (WorkerUnit, error) {
	f.mu.Lock()
	gate := f.gate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return nil, f.startErr
	}
	f.started = append(f.started, spec)
	u := &fakeUnit{name: "fake-" + spec.Run + ".service", launch: spec.LaunchID, spec: spec, done: make(chan struct{})}
	f.units[spec.Run] = u
	if f.startUncertain {
		u.launch = foreignLaunch // the unit under the name carries another launch
		return u.handle(spec.LaunchID), fmt.Errorf("%w: %w: %s carries another launch; left untouched", systemd.ErrLaunch, systemd.ErrLaunchUncertain, u.name)
	}
	return u, nil
}

func (f *fakeLauncher) Adopt(_ context.Context, run, launchID string) (WorkerUnit, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := f.units[run]
	if u == nil {
		gone := &fakeUnit{name: "fake-" + run + ".service", launch: launchID, done: make(chan struct{})}
		close(gone.done)
		return gone, false, nil
	}
	select {
	case <-u.done:
		return u.handle(launchID), false, nil
	default:
	}
	if h := u.handle(launchID); !h.carried() {
		return h, false, fmt.Errorf("%w: %w: %s carries another launch", systemd.ErrLaunch, systemd.ErrLaunchUncertain, u.name)
	}
	return u, true, nil
}

func (f *fakeLauncher) starts() []WorkerSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]WorkerSpec(nil), f.started...)
}

func (f *fakeLauncher) unit(run string) *fakeUnit {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.units[run]
}

// fakeUnit is the unit under a run's name, or, with target set, a handle for a launch id over
// that unit, which may or may not carry it: ownership, stops and the end are read from the unit
// under the name, as systemd would report them, so a handle acts only on a unit that carries its
// launch id and sees the unit under the name go.
type fakeUnit struct {
	name   string
	launch string    // the id the handle is for; for the unit under the name, the id it carries
	target *fakeUnit // the unit under the name, for a handle over it; nil for the unit itself
	spec   WorkerSpec
	done   chan struct{}
	once   sync.Once
	exit   systemd.Exit
	stops  atomic.Int32

	mu      sync.Mutex
	owns    func(pid int) bool // nil: this process is the worker
	stopErr error
}

func (u *fakeUnit) Name() string     { return u.name }
func (u *fakeUnit) LaunchID() string { return u.launch }

// handle returns a handle for launch over the unit under this unit's name.
func (u *fakeUnit) handle(launch string) *fakeUnit {
	return &fakeUnit{name: u.name, launch: launch, target: u}
}

// unit returns the unit under the name.
func (u *fakeUnit) unit() *fakeUnit {
	if u.target != nil {
		return u.target
	}
	return u
}

// carried reports whether the unit under the name carries this handle's launch id.
func (u *fakeUnit) carried() bool {
	t := u.unit()
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.launch == u.launch
}

func (u *fakeUnit) Owns(_ context.Context, pid int) (bool, error) {
	if !u.carried() {
		return false, nil
	}
	t := u.unit()
	t.mu.Lock()
	owns := t.owns
	t.mu.Unlock()
	if owns != nil {
		return owns(pid), nil
	}
	return pid == os.Getpid(), nil
}

func (u *fakeUnit) Stop(context.Context) error {
	t := u.unit()
	t.mu.Lock()
	err := t.stopErr
	t.mu.Unlock()
	if err != nil {
		return err
	}
	if !u.carried() {
		return fmt.Errorf("%w: %s", systemd.ErrNotOwned, u.name)
	}
	t.stops.Add(1)
	return nil
}

func (u *fakeUnit) Wait(ctx context.Context) (systemd.Exit, error) {
	t := u.unit()
	select {
	case <-t.done:
		return t.exit, nil
	case <-ctx.Done():
		return systemd.Exit{}, ctx.Err()
	}
}

func (u *fakeUnit) setOwns(f func(pid int) bool) {
	t := u.unit()
	t.mu.Lock()
	t.owns = f
	t.mu.Unlock()
}

func (u *fakeUnit) setStopErr(err error) {
	t := u.unit()
	t.mu.Lock()
	t.stopErr = err
	t.mu.Unlock()
}

// setLaunch makes the unit under the name carry launch, as systemd would report it.
func (u *fakeUnit) setLaunch(launch string) {
	t := u.unit()
	t.mu.Lock()
	t.launch = launch
	t.mu.Unlock()
}

// gone ends the unit under the name with e.
func (u *fakeUnit) gone(e systemd.Exit) {
	t := u.unit()
	t.once.Do(func() {
		t.exit = e
		close(t.done)
	})
}

// syncBuffer is a log sink the test reads back.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// nodeRig is a running node with a fake launcher, a fd store stand-in and an origin over it.
type nodeRig struct {
	t         *testing.T
	n         *Node
	l         *fakeLauncher
	fds       *nodeTestNotifier
	logs      *syncBuffer
	stateDir  string
	socketDir string
	origin    *Origin
	cancel    context.CancelFunc
	done      chan error
	stopped   bool
}

func newNodeRig(t *testing.T) *nodeRig {
	t.Helper()
	base := pubTestTempDir(t)
	return newNodeRigAt(t, base, &fakeLauncher{units: map[string]*fakeUnit{}}, newNodeTestNotifier(t), nil)
}

// newNodeRigAt runs a node over base with launcher, fds and inherited descriptors: a restart
// reuses the directories, the launcher's units and the stand-in's descriptors.
func newNodeRigAt(t *testing.T, base string, l *fakeLauncher, fds *nodeTestNotifier, inherited map[string]int) *nodeRig {
	t.Helper()
	r := &nodeRig{t: t, l: l, fds: fds, logs: &syncBuffer{}, stateDir: filepath.Join(base, "state"), socketDir: filepath.Join(base, "s"),
		done: make(chan error, 1)}
	notifier, err := systemd.NewNotifier(fds.addr)
	if err != nil {
		t.Fatalf("NewNotifier: %v", err)
	}
	n, err := NewNode(NodeOptions{StateDir: r.stateDir, SocketDir: r.socketDir, WorkerUID: os.Geteuid(), Config: nodeTestCfg,
		Policy: partstore.Policy{Window: 6 * time.Second, MaxSegment: 2 * time.Second}, Launcher: l, Inherited: inherited,
		Log: log.New(r.logs, "", 0)}, notifier)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	r.n, r.origin = n, NewRoomOrigin(n.ctrl, n)
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() { r.done <- n.Run(ctx) }()
	select {
	case <-n.ready:
	case <-time.After(nodeTestWait):
		t.Fatalf("Run did not finish its recovery")
	}
	t.Cleanup(r.teardown)
	return r
}

func (r *nodeRig) teardown() {
	if r.stopped {
		return
	}
	r.stopped = true
	r.cancel()
	select {
	case err := <-r.done:
		if !errors.Is(err, context.Canceled) {
			r.t.Errorf("Run returned %v", err)
		}
	case <-time.After(nodeTestWait):
		r.t.Errorf("Run did not return after cancellation")
	}
}

func (r *nodeRig) spec(id, room string) RunSpec {
	return RunSpec{RunID: id, RoomID: room, LiveKitRoom: "lk-" + room, HostIdentity: "host_a", LiveKitURL: nodeTestURL}
}

func (r *nodeRig) start(spec RunSpec) RunStatus {
	r.t.Helper()
	st, created, err := r.n.Start(context.Background(), spec)
	if err != nil || !created || st.State != RunStarting {
		r.t.Fatalf("Start %s: %+v, created %v, %v", spec.RunID, st, created, err)
	}
	return st
}

func (r *nodeRig) status(id string) RunStatus {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), nodeTestWait)
	defer cancel()
	st, err := r.n.Status(ctx, id)
	if err != nil {
		r.t.Fatalf("Status %s: %v", id, err)
	}
	return st
}

// waitState returns the run's status once it is in state, failing after nodeTestWait.
func (r *nodeRig) waitState(id string, state RunState) RunStatus {
	r.t.Helper()
	deadline := time.Now().Add(nodeTestWait)
	for {
		st := r.status(id)
		if st.State == state {
			return st
		}
		if st.State.Terminal() && !state.Terminal() || time.Now().After(deadline) {
			r.t.Fatalf("run %s is %s (%q), want %s", id, st.State, st.Reason, state)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// epochOf reports the run's epoch, nil before its worker was admitted.
func (r *nodeRig) epochOf(id string) *epochState {
	r.n.mu.Lock()
	run := r.n.runs[id]
	r.n.mu.Unlock()
	if run == nil {
		return nil
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.ep
}

func (r *nodeRig) socket(id string) string { return filepath.Join(r.socketDir, id+".sock") }

// waitStops returns once the run's unit has been asked to stop want times, which follows the
// run's terminal state, failing after nodeTestWait.
func (r *nodeRig) waitStops(id string, want int32) {
	r.t.Helper()
	deadline := time.Now().Add(nodeTestWait)
	for {
		if got := r.l.unit(id).stops.Load(); got == want {
			return
		} else if time.Now().After(deadline) {
			r.t.Fatalf("unit of %s stopped %d times, want %d", id, got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitStore returns once the stand-in holds stored descriptors and has removed removed, failing
// after nodeTestWait.
func (r *nodeRig) waitStore(stored, removed int) {
	r.t.Helper()
	deadline := time.Now().Add(nodeTestWait)
	for {
		s, d := r.fds.counts()
		if s == stored && d == removed {
			return
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("fd store: %d stored, %d removed, want %d and %d", s, d, stored, removed)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitRecord returns the run's record once ok accepts it, failing after nodeTestWait.
func (r *nodeRig) waitRecord(id string, ok func(runRecord) bool) runRecord {
	r.t.Helper()
	deadline := time.Now().Add(nodeTestWait)
	for {
		rec, err := readRunRecord(RunDir(r.stateDir, id))
		if err == nil && ok(rec) {
			return rec
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("record of %s: %+v, %v", id, rec, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitLog returns once the node's log holds want, failing after nodeTestWait.
func (r *nodeRig) waitLog(want string) {
	r.t.Helper()
	deadline := time.Now().Add(nodeTestWait)
	for !strings.Contains(r.logs.String(), want) {
		if time.Now().After(deadline) {
			r.t.Fatalf("log lacks %q:\n%s", want, r.logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// roomOwnedBy requires room to be owned by run id.
func (r *nodeRig) roomOwnedBy(room, id string) {
	r.t.Helper()
	r.n.mu.Lock()
	owner := r.n.rooms[room]
	r.n.mu.Unlock()
	if owner == nil || owner.spec.RunID != id {
		r.t.Fatalf("room %s is owned by %v, want run %s", room, owner, id)
	}
}

// startWaits requires a Start of spec to wait, for a room whose run is over but not retired.
func (r *nodeRig) startWaits(spec RunSpec) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if st, created, err := r.n.Start(ctx, spec); !errors.Is(err, context.DeadlineExceeded) {
		r.t.Fatalf("Start of %s in the owned room %s: %+v, %v, %v", spec.RunID, spec.RoomID, st, created, err)
	}
	if n := len(r.l.starts()); n > 0 && r.l.starts()[n-1].Run == spec.RunID {
		r.t.Fatalf("a worker was launched for %s while the room was owned", spec.RunID)
	}
}

// worker returns a publisher connecting as the run's worker from this process.
func (r *nodeRig) worker(id string) *worker.Publisher {
	r.t.Helper()
	pub, err := worker.NewPublisher(worker.PublisherOptions{Socket: r.socket(id), Run: id, Source: id, SupervisorUID: os.Geteuid(), Config: nodeTestCfg})
	if err != nil {
		r.t.Fatalf("NewPublisher: %v", err)
	}
	r.t.Cleanup(func() { _ = pub.Close() })
	return pub
}

// publishStart publishes what binding needs: an init on every track and the audio phases, the
// presentation naming the run's room, then segment 1 part 0 on every track.
func (r *nodeRig) publishStart(pub *worker.Publisher, spec RunSpec) {
	r.t.Helper()
	r.publishInits(pub)
	r.writePresentation(spec, spec.RoomID)
	r.publishPart(pub, 1, 0)
}

func (r *nodeRig) publishInits(pub *worker.Publisher) {
	r.t.Helper()
	for trk := 0; trk < nodeTestCfg.Tracks; trk++ {
		codecs := "avc1.64001f"
		if trk >= ipc.TrackAudioA {
			codecs = "mp4a.40.2"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := pub.Publish(ctx, ipc.Entry{Kind: ipc.KindInit, Track: trk, Codecs: codecs}, bytes.Repeat([]byte{byte(0x10 + trk)}, 16))
		cancel()
		if err != nil {
			r.t.Fatalf("publish init %d: %v", trk, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pub.Publish(ctx, ipc.Entry{Kind: ipc.KindPhases, Track: ipc.TrackAudioA, PhaseVideo: 1, PhaseAudio: 1}, nil); err != nil {
		r.t.Fatalf("publish phases: %v", err)
	}
}

// writePresentation writes the presentation as the worker does, with the slot naming room.
func (r *nodeRig) writePresentation(spec RunSpec, room string) {
	r.t.Helper()
	clocks := make([]ipc.TrackClock, nodeTestCfg.Tracks)
	for i := range clocks {
		clocks[i] = ipc.TrackClock{Track: i}
	}
	pres := ipc.Presentation{Version: ipc.PresentationVersion, Run: spec.RunID, Source: spec.RunID, Epoch: 1, Config: nodeTestCfg,
		PipelineUTCNS: 1_700_000_000_000_000_000, UncertaintyNS: 10_000_000, CropAnchor: 0.5, Clocks: clocks,
		Slots: []ipc.PresentationSlot{{HostID: spec.HostIdentity, RoomID: room}}}
	if err := worker.WritePresentation(EpochDir(r.stateDir, spec.RunID, spec.RunID, 1), pres); err != nil {
		r.t.Fatalf("WritePresentation: %v", err)
	}
}

// publishPart publishes part idx of segment msn on every track: §7.5 durations, video
// independent at every third part.
func (r *nodeRig) publishPart(pub *worker.Publisher, msn, idx int) {
	r.t.Helper()
	for trk := 0; trk < nodeTestCfg.Tracks; trk++ {
		dur, payload := otVideoPart, 10
		if trk >= ipc.TrackAudioA {
			dur, payload = otAudioPart, 15
		}
		k := (msn-1)*otParts + idx
		pts := int64(k) * dur
		switchPublishPart(r.t, pub, trk, msn, idx, uint64(msn), k*payload, pts, pts, dur, payload, idx == 0, trk < ipc.TrackAudioA && idx%3 == 0)
	}
}

func (r *nodeRig) publishTerminal(pub *worker.Publisher, verified bool, reason string) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pub.Publish(ctx, ipc.Entry{Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, Verified: verified, Reason: reason}, nil); err != nil {
		r.t.Fatalf("publish terminal: %v", err)
	}
}

// running starts spec, connects its worker and publishes until the run is running.
func (r *nodeRig) running(spec RunSpec) *worker.Publisher {
	r.t.Helper()
	r.start(spec)
	pub := r.worker(spec.RunID)
	r.publishStart(pub, spec)
	r.waitState(spec.RunID, RunRunning)
	return pub
}

func (r *nodeRig) get(path string) *httptest.ResponseRecorder {
	return otRequest(r.origin, http.MethodGet, path)
}

func (r *nodeRig) expect(path string, code int) string {
	r.t.Helper()
	rec := r.get(path)
	if rec.Code != code {
		r.t.Fatalf("GET %s = %d %q, want %d", path, rec.Code, rec.Body.String(), code)
	}
	return rec.Body.String()
}

// waitGet returns once GET path answers code, failing after nodeTestWait.
func (r *nodeRig) waitGet(path string, code int) string {
	r.t.Helper()
	deadline := time.Now().Add(nodeTestWait)
	for {
		rec := r.get(path)
		if rec.Code == code {
			return rec.Body.String()
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("GET %s = %d, want %d", path, rec.Code, code)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func mediaPath(run, file string) string { return "/media/" + run + "/" + run + "/1/" + file }

func TestNodeRunEndsOnVerifiedTerminal(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-1", "room_a")
	st := r.start(spec)
	if st.CreatedAt.IsZero() || !st.StartedAt.IsZero() || !st.EndedAt.IsZero() || st.Uncertain {
		t.Fatalf("starting status %+v", st)
	}
	starts := r.l.starts()
	want := WorkerSpec{Run: "run-1", LaunchID: starts[0].LaunchID, Socket: r.socket("run-1"), StateDir: r.stateDir, URL: nodeTestURL,
		Room: "lk-room_a", RoomID: "room_a", Host: "host_a", SupervisorUID: os.Geteuid(), Config: nodeTestCfg}
	if len(starts) != 1 || starts[0] != want || systemd.CheckLaunchID(starts[0].LaunchID) != nil {
		t.Fatalf("launcher started %+v, want %+v", starts, want)
	}
	rec, err := readRunRecord(RunDir(r.stateDir, "run-1"))
	if err != nil || rec.spec() != spec || rec.LaunchID != starts[0].LaunchID || rec.Epoch != 1 || rec.FinalState != "" {
		t.Fatalf("run record %+v, %v", rec, err)
	}
	if st, err := os.Stat(r.socket("run-1")); err != nil || st.Mode()&os.ModeSocket == 0 {
		t.Fatalf("run socket: %v %v", st, err)
	}
	r.expect("/live/room_a/master.m3u8", http.StatusServiceUnavailable) // starting: no media yet, never 404
	r.expect("/live/other/master.m3u8", http.StatusNotFound)

	pub := r.worker("run-1")
	r.publishInits(pub)
	r.writePresentation(spec, spec.RoomID)
	r.publishPart(pub, 1, 0)
	st = r.waitState("run-1", RunRunning)
	if st.StartedAt.IsZero() {
		t.Fatalf("running without StartedAt: %+v", st)
	}
	otContains(t, r.expect("/live/room_a/master.m3u8", http.StatusOK), "vhi.m3u8")
	vhi := r.expect("/live/room_a/vhi.m3u8", http.StatusOK)
	otContains(t, vhi, `URI="`+mediaPath("run-1", "vhi/init.mp4")+`"`)
	otContains(t, vhi, `URI="`+mediaPath("run-1", "vhi/1.0.m4s")+`"`)
	r.expect(mediaPath("run-1", "vhi/init.mp4"), http.StatusOK)
	r.publishPart(pub, 1, 1)
	r.waitGet("/live/room_a/vhi.m3u8?_HLS_msn=1&_HLS_part=1", http.StatusOK)

	r.publishTerminal(pub, true, "host left")
	st = r.waitState("run-1", RunEnded)
	if st.Reason != "host left" || st.EndedAt.IsZero() {
		t.Fatalf("ended status %+v", st)
	}
	r.expect("/live/room_a/master.m3u8", http.StatusServiceUnavailable)
	r.expect("/live/room_a/vhi.m3u8", http.StatusServiceUnavailable)
	if _, ok := r.n.Lookup("room_a"); ok || !r.n.Inactive("room_a") {
		t.Fatalf("room_a still looked up, or not inactive")
	}
	r.waitStops("run-1", 1) // the verified terminal asks the unit to stop, once
	if _, err := os.Stat(r.socket("run-1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("run socket after the end: %v", err)
	}
	// The epoch stays readable until the worker is gone, then is released with its descriptors.
	r.expect(mediaPath("run-1", "vhi/init.mp4"), http.StatusOK)
	_ = pub.Close()
	r.l.unit("run-1").gone(systemd.Exit{})
	r.waitGet(mediaPath("run-1", "vhi/init.mp4"), http.StatusNotFound)
	r.waitStore(0, nodeTestCfg.Tracks)
	// Settled once: the late unit exit changed nothing.
	if st := r.status("run-1"); st.State != RunEnded || st.Reason != "host left" {
		t.Fatalf("after the unit exit: %+v", st)
	}
	if _, err := r.n.resolve(EpochKey{Run: "run-1", Source: "run-1", Epoch: 1}); !errors.Is(err, ErrLostEpoch) {
		t.Fatalf("released epoch resolves: %v", err)
	}
}

func TestNodeWorkerUnitExitFails(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-2", "room_b")
	pub := r.running(spec)
	r.expect("/live/room_b/vhi.m3u8", http.StatusOK)
	_ = pub.Close()
	r.l.unit("run-2").gone(systemd.Exit{Known: true, Result: "exit-code", Code: "exited", Status: 1})
	st := r.waitState("run-2", RunFailed)
	if !strings.Contains(st.Reason, "exit-code") || !strings.Contains(st.Reason, "fake-run-2.service") {
		t.Fatalf("failed reason %q", st.Reason)
	}
	r.expect("/live/room_b/vhi.m3u8", http.StatusServiceUnavailable)
	if u := r.l.unit("run-2"); u.stops.Load() != 0 {
		t.Fatalf("a gone unit was stopped %d times", u.stops.Load())
	}
	r.waitGet(mediaPath("run-2", "vhi/init.mp4"), http.StatusNotFound)

	// An unverified terminal fails the run as well, with the worker's reason.
	spec3 := r.spec("run-3", "room_c")
	pub3 := r.running(spec3)
	r.publishTerminal(pub3, false, "decoder error")
	if st := r.waitState("run-3", RunFailed); st.Reason != "decoder error" {
		t.Fatalf("failed reason %q", st.Reason)
	}
	r.waitStops("run-3", 1)
}

func TestNodeStopSettlesStopped(t *testing.T) {
	r := newNodeRig(t)
	// A stop systemd cannot take leaves the run exactly as it was, exposed, and is retryable.
	spec := r.spec("run-4", "room_d")
	pub := r.running(spec)
	r.l.unit("run-4").setStopErr(fmt.Errorf("%w: stop fake-run-4.service: Access denied", systemd.ErrUnitControl))
	st, err := r.n.Stop(context.Background(), "run-4")
	if !errors.Is(err, ErrRunStop) || st.State != RunRunning || !strings.Contains(err.Error(), "Access denied") {
		t.Fatalf("Stop refused: %+v, %v", st, err)
	}
	otContains(t, r.expect("/live/room_d/vhi.m3u8", http.StatusOK), mediaPath("run-4", "vhi/1.0.m4s"))
	if u := r.l.unit("run-4"); u.stops.Load() != 0 {
		t.Fatalf("unit stopped %d times through a refused stop", u.stops.Load())
	}
	r.l.unit("run-4").setStopErr(nil)
	// From running: the unit is asked to stop, then the playlists go; the worker's verified
	// terminal on SIGTERM settles stopped, never ended.
	st, err = r.n.Stop(context.Background(), "run-4")
	if err != nil || st.State != RunStopping {
		t.Fatalf("Stop: %+v, %v", st, err)
	}
	r.expect("/live/room_d/vhi.m3u8", http.StatusServiceUnavailable)
	if u := r.l.unit("run-4"); u.stops.Load() != 1 {
		t.Fatalf("unit stopped %d times", u.stops.Load())
	}
	if st, err := r.n.Stop(context.Background(), "run-4"); err != nil || st.State != RunStopping {
		t.Fatalf("second Stop: %+v, %v", st, err)
	}
	if _, _, err := r.n.Start(context.Background(), r.spec("run-5", "room_d")); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("a new run for a stopping room: %v", err)
	}
	r.publishTerminal(pub, true, "stop requested")
	st = r.waitState("run-4", RunStopped)
	if st.Reason != "stop requested" {
		t.Fatalf("stopped reason %q", st.Reason)
	}
	_ = pub.Close()
	r.l.unit("run-4").gone(systemd.Exit{})
	if st, err := r.n.Stop(context.Background(), "run-4"); err != nil || st.State != RunStopped {
		t.Fatalf("Stop after stopped: %+v, %v", st, err)
	}

	// From starting, with no worker connected: the unit's exit settles stopped.
	r.start(r.spec("run-6", "room_e"))
	if st, err := r.n.Stop(context.Background(), "run-6"); err != nil || st.State != RunStopping {
		t.Fatalf("Stop from starting: %+v, %v", st, err)
	}
	r.l.unit("run-6").gone(systemd.Exit{Known: true, Result: "success", Code: "exited"})
	r.waitState("run-6", RunStopped)
	if _, err := r.n.Stop(context.Background(), "run-none"); !errors.Is(err, ErrRunUnknown) {
		t.Fatalf("Stop of an unknown run: %v", err)
	}
}

func TestNodeStopCompletionRace(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-7", "room_f")
	pub := r.running(spec)
	r.publishTerminal(pub, true, "host left")
	r.waitState("run-7", RunEnded)
	r.waitStops("run-7", 1)
	st, err := r.n.Stop(context.Background(), "run-7")
	if err != nil || st.State != RunEnded || st.Reason != "host left" {
		t.Fatalf("Stop after the end: %+v, %v", st, err)
	}
	time.Sleep(20 * time.Millisecond)
	if u := r.l.unit("run-7"); u.stops.Load() != 1 {
		t.Fatalf("unit stopped %d times, want the one stop of the end", u.stops.Load())
	}
}

func TestNodeStopStartRaceAndFreshRun(t *testing.T) {
	r := newNodeRig(t)
	spec1 := r.spec("run-8", "room_g")
	pub1 := r.running(spec1)
	old := r.expect("/live/room_g/vhi.m3u8", http.StatusOK)
	otContains(t, old, mediaPath("run-8", "vhi/1.0.m4s"))
	if _, err := r.n.Stop(context.Background(), "run-8"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	spec2 := r.spec("run-9", "room_g")
	if _, _, err := r.n.Start(context.Background(), spec2); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("Start while stopping: %v", err)
	}
	_ = pub1.Close()
	r.l.unit("run-8").gone(systemd.Exit{Known: true, Result: "success", Code: "exited"})
	r.waitState("run-8", RunStopped)
	r.expect("/live/room_g/vhi.m3u8", http.StatusServiceUnavailable)

	// The fresh run: a new id, record, journal, socket and unit; the stable URL serves only its
	// media, and the old run's late events touch nothing of it.
	r.start(spec2)
	r.expect("/live/room_g/vhi.m3u8", http.StatusServiceUnavailable)
	if _, err := os.Stat(RoomDir(r.stateDir, "run-9", "room_g")); err != nil {
		t.Fatalf("run-9 room journal directory: %v", err)
	}
	pub2 := r.worker("run-9")
	r.publishStart(pub2, spec2)
	r.waitState("run-9", RunRunning)
	fresh := r.expect("/live/room_g/vhi.m3u8", http.StatusOK)
	otContains(t, fresh, mediaPath("run-9", "vhi/1.0.m4s"))
	if strings.Contains(fresh, "run-8") {
		t.Fatalf("the fresh playlist names the old run:\n%s", fresh)
	}
	otContains(t, fresh, "#EXT-X-MEDIA-SEQUENCE:1\n")
	if starts := r.l.starts(); len(starts) != 2 || starts[1].Socket != r.socket("run-9") || starts[1].LaunchID == starts[0].LaunchID {
		t.Fatalf("launches %+v", starts)
	}
	r.waitGet(mediaPath("run-8", "vhi/init.mp4"), http.StatusNotFound)
	r.expect(mediaPath("run-9", "vhi/init.mp4"), http.StatusOK)
	r.n.mu.Lock()
	old8 := r.n.runs["run-8"]
	r.n.mu.Unlock()
	old8.finish(false, "late", false) // a stale settlement of the retired run
	old8.workerGone("late exit")
	otContains(t, r.expect("/live/room_g/vhi.m3u8", http.StatusOK), mediaPath("run-9", "vhi/1.0.m4s"))
	if st := r.status("run-9"); st.State != RunRunning {
		t.Fatalf("the newer run after the old run's late events: %+v", st)
	}
	if tl, ok := r.n.ctrl.Lookup("room_g"); !ok || tl != r.n.runs["run-9"].tl {
		t.Fatal("the newer run's registration was removed")
	}
}

func TestNodeStartFailureRollsBack(t *testing.T) {
	r := newNodeRig(t)
	r.l.mu.Lock()
	r.l.startErr = errors.New("Failed to start transient service unit: Access denied")
	r.l.mu.Unlock()
	spec := r.spec("run-10", "room_h")
	_, _, err := r.n.Start(context.Background(), spec)
	if !errors.Is(err, ErrRunLaunch) || errors.Is(err, ErrRunLaunchUncertain) || !strings.Contains(err.Error(), "Access denied") {
		t.Fatalf("Start = %v, want ErrRunLaunch with the reason", err)
	}
	if _, err := r.n.Status(context.Background(), "run-10"); !errors.Is(err, ErrRunUnknown) {
		t.Fatalf("Status after the rollback: %v", err)
	}
	if _, err := os.Stat(r.socket("run-10")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket left behind: %v", err)
	}
	if _, err := os.Stat(filepath.Join(RunDir(r.stateDir, "run-10"), runFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("run record left behind: %v", err)
	}
	if _, ok := r.n.ctrl.Lookup("room_h"); ok {
		t.Fatal("timeline registered after the rollback")
	}
	r.expect("/live/room_h/master.m3u8", http.StatusNotFound)
	r.l.mu.Lock()
	r.l.startErr = nil
	r.l.mu.Unlock()
	if _, created, err := r.n.Start(context.Background(), spec); err != nil || !created {
		t.Fatalf("Start after the rollback: created %v, %v", created, err)
	}
}

func TestNodeHeldLaunchIsNeverRetriedIntoASecondWorker(t *testing.T) {
	r := newNodeRig(t)
	r.l.mu.Lock()
	r.l.startUncertain = true
	r.l.mu.Unlock()
	spec := r.spec("run-11", "room_i")
	st, created, err := r.n.Start(context.Background(), spec)
	if !errors.Is(err, ErrRunLaunchUncertain) || !created || st.State != RunStarting || !st.Uncertain || !strings.Contains(st.Reason, "another launch") {
		t.Fatalf("uncertain start: %+v, created %v, %v", st, created, err)
	}
	r.l.mu.Lock()
	r.l.startUncertain = false
	r.l.mu.Unlock()
	// Every retry returns the same uncertain outcome; the room is owned; GET reports it held.
	for i := 0; i < 3; i++ {
		if st, created, err := r.n.Start(context.Background(), spec); !errors.Is(err, ErrRunLaunchUncertain) || created || !st.Uncertain {
			t.Fatalf("retry %d: %+v, %v, %v", i, st, created, err)
		}
	}
	if _, _, err := r.n.Start(context.Background(), r.spec("run-12", "room_i")); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("a second run for the held room: %v", err)
	}
	if st := r.status("run-11"); !st.Uncertain || st.State != RunStarting {
		t.Fatalf("held status %+v", st)
	}
	if n := len(r.l.starts()); n != 1 {
		t.Fatalf("%d launches, want 1", n)
	}
	r.expect("/live/room_i/master.m3u8", http.StatusServiceUnavailable)
	// Its socket admits nobody: the handle owns no process.
	pub := r.worker("run-11")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = pub.Publish(ctx, ipc.Entry{Kind: ipc.KindInit, Track: 0, Codecs: "avc1.64001f"}, bytes.Repeat([]byte{0x10}, 16))
	cancel()
	if !errors.Is(err, worker.ErrFatal) || r.epochOf("run-11") != nil {
		t.Fatalf("a peer was admitted into a held run: %v, epoch %v", err, r.epochOf("run-11"))
	}
	// A stop cannot be issued to a unit the run does not own; the run stays held and truthful.
	if st, err := r.n.Stop(context.Background(), "run-11"); !errors.Is(err, ErrRunStop) || st.State != RunStarting || !st.Uncertain {
		t.Fatalf("Stop of a held run: %+v, %v", st, err)
	}
	// The unit of the name going away settles the run failed and frees the room.
	r.l.unit("run-11").gone(systemd.Exit{})
	if st := r.waitState("run-11", RunFailed); st.Uncertain {
		t.Fatalf("failed status still uncertain: %+v", st)
	}
	if st, created, err := r.n.Start(context.Background(), r.spec("run-12", "room_i")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run after the held run settled: %+v, %v, %v", st, created, err)
	}
	if st, _, err := r.n.Start(context.Background(), spec); err != nil || st.State != RunFailed {
		t.Fatalf("the settled run's retry: %+v, %v", st, err)
	}
}

func TestNodeIdempotencyAndConflicts(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-13", "room_j")
	r.start(spec)
	st, created, err := r.n.Start(context.Background(), spec)
	if err != nil || created || st.Spec != spec || st.State != RunStarting {
		t.Fatalf("exact retry: %+v, created %v, %v", st, created, err)
	}
	other := spec
	other.HostIdentity = "host_b"
	if _, _, err := r.n.Start(context.Background(), other); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("reused id with other parameters: %v", err)
	}
	if _, _, err := r.n.Start(context.Background(), r.spec("run-14", "room_j")); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("second run for the room: %v", err)
	}
	// The same LiveKit room may be ingested for another presentation room.
	sameLK := r.spec("run-15", "room_k")
	sameLK.LiveKitRoom = "lk-room_j"
	if _, created, err := r.n.Start(context.Background(), sameLK); err != nil || !created {
		t.Fatalf("second run ingesting the LiveKit room: created %v, %v", created, err)
	}
	if len(r.l.starts()) != 2 {
		t.Fatalf("%d launches", len(r.l.starts()))
	}

	// Concurrent identical starts create one run; concurrent starts of one room admit one.
	const workers = 8
	var wg sync.WaitGroup
	var createdN, okN, conflictN atomic.Int32
	same := r.spec("run-16", "room_l")
	for i := 0; i < workers; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, created, err := r.n.Start(context.Background(), same)
			if err == nil {
				okN.Add(1)
			}
			if created {
				createdN.Add(1)
			}
		}()
		go func(i int) {
			defer wg.Done()
			_, _, err := r.n.Start(context.Background(), r.spec(fmt.Sprintf("run-17-%d", i), "room_m"))
			if errors.Is(err, ErrRunConflict) {
				conflictN.Add(1)
			} else if err != nil {
				t.Errorf("room_m start: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if okN.Load() != workers || createdN.Load() != 1 {
		t.Fatalf("identical starts: %d ok, %d created", okN.Load(), createdN.Load())
	}
	if conflictN.Load() != workers-1 {
		t.Fatalf("room_m starts: %d conflicts, want %d", conflictN.Load(), workers-1)
	}
	if n := len(r.l.starts()); n != 4 {
		t.Fatalf("%d launches, want 4", n)
	}
}

func TestNodeStatusAndStopWaitForTheLaunch(t *testing.T) {
	r := newNodeRig(t)
	gate := make(chan struct{})
	r.l.mu.Lock()
	r.l.gate = gate
	r.l.mu.Unlock()
	spec := r.spec("run-18", "room_n")
	var wg sync.WaitGroup
	var statusErr, stopErr error
	var statusSt, stopSt, startSt RunStatus
	wg.Add(3)
	go func() { defer wg.Done(); startSt, _, _ = r.n.Start(context.Background(), spec) }()
	time.Sleep(50 * time.Millisecond) // the launch is in flight: the record and socket exist
	go func() { defer wg.Done(); statusSt, statusErr = r.n.Status(context.Background(), "run-18") }()
	go func() { defer wg.Done(); stopSt, stopErr = r.n.Stop(context.Background(), "run-18") }()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if _, err := r.n.Status(ctx, "run-18"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Status during the launch returned before it settled: %v", err)
	}
	cancel()
	if _, _, err := r.n.Start(context.Background(), r.spec("run-19", "room_n")); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("another run for a room being started: %v", err)
	}
	close(gate)
	wg.Wait()
	if startSt.State != RunStarting || statusErr != nil || statusSt.State == "" || stopErr != nil || stopSt.State != RunStopping {
		t.Fatalf("after the launch settled: start %+v, status %+v %v, stop %+v %v", startSt, statusSt, statusErr, stopSt, stopErr)
	}
	if st := r.status("run-18"); st.State != RunStopping {
		t.Fatalf("status %+v", st)
	}
}

func TestNodeRoomsAreIndependent(t *testing.T) {
	r := newNodeRig(t)
	specA, specB := r.spec("run-20", "room_o"), r.spec("run-21", "room_p")
	pubA := r.running(specA)
	pubB := r.running(specB)
	r.expect("/live/room_o/vhi.m3u8", http.StatusOK)
	r.expect("/live/room_p/vhi.m3u8", http.StatusOK)
	r.publishTerminal(pubA, true, "host left")
	r.waitState("run-20", RunEnded)
	r.expect("/live/room_o/vhi.m3u8", http.StatusServiceUnavailable)
	otContains(t, r.expect("/live/room_p/vhi.m3u8", http.StatusOK), mediaPath("run-21", "vhi/1.0.m4s"))
	if st := r.status("run-21"); st.State != RunRunning {
		t.Fatalf("room_p run %+v", st)
	}
	_ = pubB.Close()
	r.l.unit("run-21").gone(systemd.Exit{Known: true, Result: "signal", Code: "killed", Status: 9})
	r.waitState("run-21", RunFailed)
	r.expect("/live/room_p/vhi.m3u8", http.StatusServiceUnavailable)
}

func TestNodeRoomIdentityIsDistinctFromLiveKitRoom(t *testing.T) {
	r := newNodeRig(t)
	spec := RunSpec{RunID: "run-22", RoomID: "room_q", LiveKitRoom: "livekit-session-77", HostIdentity: "host_a", LiveKitURL: nodeTestURL}
	r.start(spec)
	if starts := r.l.starts(); starts[0].Room != "livekit-session-77" || starts[0].RoomID != "room_q" {
		t.Fatalf("worker spec rooms %+v", starts[0])
	}
	pub := r.worker("run-22")
	r.publishInits(pub)
	r.writePresentation(spec, "room_q") // the emitted slot names the presentation room, not the LiveKit room
	r.publishPart(pub, 1, 0)
	r.waitState("run-22", RunRunning)
	tl, ok := r.n.Lookup("room_q")
	if !ok {
		t.Fatal("room_q not served")
	}
	snap, _, _ := tl.Observe()
	if snap.Room != "room_q" || snap.Live == nil || snap.Live.Presentation.Slots[snap.Live.Slot].RoomID != "room_q" {
		t.Fatalf("timeline room %q, live %+v", snap.Room, snap.Live)
	}
	vhi := r.expect("/live/room_q/vhi.m3u8", http.StatusOK)
	otContains(t, vhi, `X-SLOTS="0:host_a:room_q:0,0,1,1"`)
	if strings.Contains(vhi, "livekit-session-77") {
		t.Fatalf("the playlist names the LiveKit room:\n%s", vhi)
	}
	r.expect("/live/livekit-session-77/vhi.m3u8", http.StatusNotFound)
	if _, err := os.Stat(RoomDir(r.stateDir, "run-22", "room_q")); err != nil {
		t.Fatalf("room journal under the presentation room: %v", err)
	}

	// A presentation whose slot names the LiveKit room instead cannot expose the run.
	spec2 := RunSpec{RunID: "run-23", RoomID: "room_r", LiveKitRoom: "livekit-session-78", HostIdentity: "host_a", LiveKitURL: nodeTestURL}
	r.start(spec2)
	pub2 := r.worker("run-23")
	r.publishInits(pub2)
	r.writePresentation(spec2, "livekit-session-78")
	r.publishPart(pub2, 1, 0)
	if st := r.waitState("run-23", RunFailed); !strings.Contains(st.Reason, `no slot for room "room_r"`) {
		t.Fatalf("failed reason %q", st.Reason)
	}
}

func TestNodeRejectsPeerThatIsNotTheUnitsMainProcess(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-24", "room_s")
	r.start(spec)
	u := r.l.unit("run-24")
	u.setOwns(func(int) bool { return false }) // a same-uid process that is not the unit's MainPID
	rogue := r.worker("run-24")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := rogue.Publish(ctx, ipc.Entry{Kind: ipc.KindInit, Track: 0, Codecs: "avc1.64001f"}, bytes.Repeat([]byte{0x10}, 16))
	cancel()
	if !errors.Is(err, worker.ErrFatal) {
		t.Fatalf("rogue publish = %v, want a fatal reject", err)
	}
	if r.epochOf("run-24") != nil {
		t.Fatal("a rogue peer created the run's epoch")
	}
	if _, err := os.Stat(EpochDir(r.stateDir, "run-24", "run-24", 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("epoch directory created for a rogue peer: %v", err)
	}
	if !strings.Contains(r.logs.String(), "not the main process of unit fake-run-24.service") {
		t.Fatalf("logs:\n%s", r.logs.String())
	}
	if st := r.status("run-24"); st.State != RunStarting {
		t.Fatalf("after the rogue peer: %+v", st)
	}
	u.setOwns(nil) // this process is the unit's main process
	pub := r.worker("run-24")
	r.publishStart(pub, spec)
	r.waitState("run-24", RunRunning)
}

func TestNodeExposureRefusalFails(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-25", "room_t")
	r.start(spec)
	pub := r.worker("run-25")
	r.publishInits(pub)
	r.writePresentation(spec, "another-room")
	r.publishPart(pub, 1, 0)
	st := r.waitState("run-25", RunFailed)
	if !strings.Contains(st.Reason, "cannot be exposed") || !strings.Contains(st.Reason, `no slot for room "room_t"`) {
		t.Fatalf("failed reason %q", st.Reason)
	}
	r.waitStops("run-25", 1)
}

func TestNodeRefusesInvalidSpecs(t *testing.T) {
	r := newNodeRig(t)
	good := r.spec("run-26", "room_u")
	for _, c := range []struct {
		name string
		mod  func(*RunSpec)
	}{
		{"empty run id", func(s *RunSpec) { s.RunID = "" }},
		{"run id with a slash", func(s *RunSpec) { s.RunID = "a/b" }},
		{"run id with a space", func(s *RunSpec) { s.RunID = "a b" }},
		{"run id with a dot", func(s *RunSpec) { s.RunID = "a.b" }},
		{"run id starting with a dash", func(s *RunSpec) { s.RunID = "-a" }},
		{"long run id", func(s *RunSpec) { s.RunID = strings.Repeat("a", 65) }},
		{"empty room", func(s *RunSpec) { s.RoomID = "" }},
		{"room with a control character", func(s *RunSpec) { s.RoomID = "a\nb" }},
		{"empty LiveKit room", func(s *RunSpec) { s.LiveKitRoom = "" }},
		{"empty host", func(s *RunSpec) { s.HostIdentity = "" }},
		{"host with a control character", func(s *RunSpec) { s.HostIdentity = "h\x00" }},
		{"empty url", func(s *RunSpec) { s.LiveKitURL = "" }},
		{"url scheme", func(s *RunSpec) { s.LiveKitURL = "file:///etc/passwd" }},
		{"url without host", func(s *RunSpec) { s.LiveKitURL = "ws:///x" }},
		{"url with a control character", func(s *RunSpec) { s.LiveKitURL = "ws://h\x7f" }},
		{"long url", func(s *RunSpec) { s.LiveKitURL = "ws://" + strings.Repeat("h", 1100) }},
	} {
		s := good
		c.mod(&s)
		if _, _, err := r.n.Start(context.Background(), s); !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("%s: Start = %v, want ErrInvalidOptions", c.name, err)
		}
	}
	if n := len(r.l.starts()); n != 0 {
		t.Fatalf("%d launches for invalid specs", n)
	}
	entries, err := os.ReadDir(r.socketDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("socket directory after invalid specs: %v %v", entries, err)
	}
	for _, d := range []string{"sources", "runs"} {
		if _, err := os.Stat(filepath.Join(r.stateDir, d)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s written for invalid specs: %v", d, err)
		}
	}
	if st, _, err := r.n.Start(context.Background(), good); err != nil || st.State != RunStarting {
		t.Fatalf("the valid spec: %+v, %v", st, err)
	}
}

// TestNodeWorkerProcessHelper is the body of a child process: it connects as the run's worker,
// publishes one init and exits, so the parent's exit watcher sees a process leave.
func TestNodeWorkerProcessHelper(t *testing.T) {
	socket := os.Getenv(nodeHelperSocket)
	if socket == "" {
		return
	}
	run := os.Getenv(nodeHelperRun)
	pub, err := worker.NewPublisher(worker.PublisherOptions{Socket: socket, Run: run, Source: run, SupervisorUID: os.Geteuid(), Config: nodeTestCfg})
	if err != nil {
		t.Fatalf("helper: NewPublisher: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pub.Publish(ctx, ipc.Entry{Kind: ipc.KindInit, Track: 0, Codecs: "avc1.64001f"}, bytes.Repeat([]byte{0x10}, 16)); err != nil {
		t.Fatalf("helper: publish: %v", err)
	}
	_ = pub.Close()
}

func TestNodeWorkerProcessExitFails(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-27", "room_v")
	r.start(spec)
	r.l.unit("run-27").setOwns(func(pid int) bool { return pid != os.Getpid() }) // the child is the unit's main process
	cmd := exec.Command(os.Args[0], "-test.run=^TestNodeWorkerProcessHelper$", "-test.count=1")
	cmd.SysProcAttr = &unix.SysProcAttr{Pdeathsig: unix.SIGKILL}
	cmd.Env = append(os.Environ(), nodeHelperSocket+"="+r.socket("run-27"), nodeHelperRun+"=run-27")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper: %v\n%s", err, out.String())
	}
	st := r.waitState("run-27", RunFailed)
	if !strings.Contains(st.Reason, fmt.Sprintf("worker pid %d exited", cmd.Process.Pid)) {
		t.Fatalf("failed reason %q", st.Reason)
	}
	if ep := r.epochOf("run-27"); ep == nil || !ep.exited.Load() || ep.owner.PID != cmd.Process.Pid {
		t.Fatalf("epoch after the helper's exit: %+v", ep)
	}
	// The process is gone but the unit is not: the epoch and its descriptors are kept and the
	// room stays owned until systemd reports the unit gone. The run was settled by the exit, so
	// the unit is not asked to stop.
	r.waitRecord("run-27", func(rec runRecord) bool { return rec.FinalState == RunFailed })
	r.expect(mediaPath("run-27", "vhi/init.mp4"), http.StatusOK)
	if stored, removed := r.fds.counts(); stored != nodeTestCfg.Tracks || removed != 0 {
		t.Fatalf("fd store after the process exit: %d stored, %d removed", stored, removed)
	}
	r.roomOwnedBy("room_v", "run-27")
	r.startWaits(r.spec("run-27-next", "room_v"))
	if u := r.l.unit("run-27"); u.stops.Load() != 0 {
		t.Fatalf("unit stopped %d times after its process exited", u.stops.Load())
	}
	r.l.unit("run-27").gone(systemd.Exit{Known: true, Result: "success", Code: "exited"})
	r.waitGet(mediaPath("run-27", "vhi/init.mp4"), http.StatusNotFound)
	r.waitStore(0, nodeTestCfg.Tracks)
	if st, created, err := r.n.Start(context.Background(), r.spec("run-27-next", "room_v")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for the room after the unit exit: %+v, %v, %v", st, created, err)
	}
	if st := r.status("run-27"); st.State != RunFailed || !strings.Contains(st.Reason, "worker pid") {
		t.Fatalf("after the unit exit: %+v", st)
	}
}

func TestNodeShutdownLeavesUnitsAndDescriptors(t *testing.T) {
	r := newNodeRig(t)
	pub := r.running(r.spec("run-28", "room_w"))
	r.teardown()
	if u := r.l.unit("run-28"); u.stops.Load() != 0 {
		t.Fatalf("shutdown stopped the unit %d times", u.stops.Load())
	}
	if stored, removed := r.fds.counts(); stored != nodeTestCfg.Tracks || removed != 0 {
		t.Fatalf("shutdown left %d descriptors stored and removed %d, want %d and 0", stored, removed, nodeTestCfg.Tracks)
	}
	if _, err := os.Stat(r.socket("run-28")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket after shutdown: %v", err)
	}
	if _, err := readRunRecord(RunDir(r.stateDir, "run-28")); err != nil {
		t.Fatalf("run record after shutdown: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := pub.Publish(ctx, ipc.Entry{Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, Verified: true}, nil); err == nil {
		t.Fatal("a publication after shutdown succeeded")
	}
	if _, _, err := r.n.Start(context.Background(), r.spec("run-29", "room_x")); !errors.Is(err, ErrNodeClosed) {
		t.Fatalf("Start after shutdown: %v", err)
	}
}

func TestNodeRestartReadoptsLiveWorker(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	spec := r1.spec("run-30", "room_y")
	pub := r1.running(spec)
	started := r1.status("run-30").StartedAt
	r1.publishPart(pub, 1, 1)
	before := r1.expect("/live/room_y/vhi.m3u8", http.StatusOK)
	otContains(t, before, mediaPath("run-30", "vhi/1.1.m4s"))
	r1.teardown() // the worker's unit keeps running; its publisher keeps dialling the socket

	// The worker publishes through the restart: its next publication blocks on the dial until
	// the new incarnation listens again.
	published := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), nodeTestWait)
		defer cancel()
		_, err := pub.Publish(ctx, ipc.Entry{Kind: ipc.KindPart, Track: 0, MSN: 1, Index: 2, Sequence: 1, FirstAU: 20, PTS: 2 * otVideoPart,
			DTS: 2 * otVideoPart, Duration: otVideoPart, Payload: 10}, bytes.Repeat([]byte{0x50}, 64))
		published <- err
	}()
	time.Sleep(150 * time.Millisecond)
	select {
	case err := <-published:
		t.Fatalf("a publication completed with no supervisor: %v", err)
	default:
	}

	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	st := r2.waitState("run-30", RunRunning)
	if st.Uncertain || st.Reason != "" || st.CreatedAt.IsZero() || !st.StartedAt.Equal(started) {
		t.Fatalf("recovered status %+v, want StartedAt %v preserved", st, started)
	}
	if !strings.Contains(r2.logs.String(), "recovered running (epoch 1, unit fake-run-30.service") {
		t.Fatalf("logs:\n%s", r2.logs.String())
	}
	after := r2.expect("/live/room_y/vhi.m3u8", http.StatusOK)
	otContains(t, after, mediaPath("run-30", "vhi/1.1.m4s"))
	otContains(t, after, "#EXT-X-MEDIA-SEQUENCE:1\n")
	otContains(t, after, "#EXT-X-DISCONTINUITY-SEQUENCE:0\n")
	if strings.Contains(after, "#EXT-X-DISCONTINUITY\n") {
		t.Fatalf("a discontinuity after the restart:\n%s", after)
	}
	r2.expect(mediaPath("run-30", "vhi/init.mp4"), http.StatusOK)
	r2.expect(mediaPath("run-30", "vhi/1.0.m4s"), http.StatusOK)
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("publication across the restart: %v", err)
		}
	case <-time.After(nodeTestWait):
		t.Fatal("the publication did not complete after the restart")
	}
	for trk := 1; trk < nodeTestCfg.Tracks; trk++ {
		dur, payload := otVideoPart, 10
		if trk >= ipc.TrackAudioA {
			dur, payload = otAudioPart, 15
		}
		switchPublishPart(t, pub, trk, 1, 2, 1, 2*payload, 2*dur, 2*dur, dur, payload, false, false)
	}
	otContains(t, r2.waitGet("/live/room_y/vhi.m3u8?_HLS_msn=1&_HLS_part=2", http.StatusOK), mediaPath("run-30", "vhi/1.2.m4s"))
	if ep := r2.epochOf("run-30"); ep == nil || ep.key.Epoch != 1 || ep.owner.PID != os.Getpid() || !ep.watching.Load() {
		t.Fatalf("recovered epoch %+v", ep)
	}
	// The same run is known to the control plane as the one run it is.
	if st, created, err := r2.n.Start(context.Background(), spec); err != nil || created || st.State != RunRunning {
		t.Fatalf("Start of the recovered run: %+v, %v, %v", st, created, err)
	}
	if _, _, err := r2.n.Start(context.Background(), r2.spec("run-31", "room_y")); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("a second run for the recovered room: %v", err)
	}
	if stored, _ := fds.counts(); stored != nodeTestCfg.Tracks {
		t.Fatalf("%d descriptors stored after the recovery, want %d", stored, nodeTestCfg.Tracks)
	}
	r2.publishTerminal(pub, true, "host left")
	r2.waitState("run-30", RunEnded)
	r2.expect("/live/room_y/vhi.m3u8", http.StatusServiceUnavailable)
	_ = pub.Close()
	l.unit("run-30").gone(systemd.Exit{})
	r2.waitGet(mediaPath("run-30", "vhi/init.mp4"), http.StatusNotFound)
	r2.waitStore(0, nodeTestCfg.Tracks)
	r2.teardown()

	// A third incarnation finds the run over: ended, served to the control plane, nothing adopted.
	r3 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	if st := r3.status("run-30"); st.State != RunEnded || st.Reason != "host left" {
		t.Fatalf("run after a restart with its unit gone: %+v", st)
	}
	r3.expect("/live/room_y/vhi.m3u8", http.StatusServiceUnavailable)
	if _, _, err := r3.n.Start(context.Background(), r3.spec("run-32", "room_y")); err != nil {
		t.Fatalf("a new run for the room after the recovery: %v", err)
	}
}

func TestNodeRestartBeforeFirstConnection(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	spec := r1.spec("run-36", "room_aa")
	r1.start(spec)
	rec, err := readRunRecord(RunDir(r1.stateDir, "run-36"))
	if err != nil || rec.Epoch != 1 {
		t.Fatalf("record before any connection: %+v, %v", rec, err)
	}
	if _, err := os.Stat(EpochDir(r1.stateDir, "run-36", "run-36", 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("epoch directory before any connection: %v", err)
	}
	r1.teardown()

	// No epoch directory and no descriptors exist; the reserved epoch comes from the record.
	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	if st := r2.status("run-36"); st.State != RunStarting || st.Uncertain || st.Reason != "" {
		t.Fatalf("recovered status %+v", st)
	}
	if !strings.Contains(r2.logs.String(), "recovered starting (epoch 1, unit fake-run-36.service") {
		t.Fatalf("logs:\n%s", r2.logs.String())
	}
	r2.expect("/live/room_aa/master.m3u8", http.StatusServiceUnavailable)
	pub := r2.worker("run-36")
	r2.publishStart(pub, spec)
	r2.waitState("run-36", RunRunning)
	if ep := r2.epochOf("run-36"); ep == nil || ep.key.Epoch != 1 {
		t.Fatalf("epoch after the first connection: %+v", ep)
	}
	otContains(t, r2.expect("/live/room_aa/vhi.m3u8", http.StatusOK), mediaPath("run-36", "vhi/1.0.m4s"))
	if _, created, err := r2.n.Start(context.Background(), spec); err != nil || created {
		t.Fatalf("Start of the recovered run: %v, %v", created, err)
	}
}

func TestNodeRecoveryOfFailedTimelineWithStaleRecord(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	spec := r1.spec("run-37", "room_ab")
	pub := r1.running(spec)
	r1.n.mu.Lock()
	run := r1.n.runs["run-37"]
	r1.n.mu.Unlock()
	for deadline := time.Now().Add(nodeTestWait); ; time.Sleep(5 * time.Millisecond) { // the running record follows the state
		if rec, err := readRunRecord(RunDir(r1.stateDir, "run-37")); err == nil && rec.StartedUnixNS != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the running record was not written")
		}
	}
	// The timeline fails durably, as the controller does on a lost or refusing source, and the
	// supervisor dies before the run settles: the record still says the run is running.
	if err := run.tl.FailSource(run.ep.key, "worker died", monotonicNow()); err != nil {
		t.Fatalf("FailSource: %v", err)
	}
	if rec, err := readRunRecord(RunDir(r1.stateDir, "run-37")); err != nil || rec.FinalState != "" || rec.StartedUnixNS == 0 {
		t.Fatalf("stale record %+v, %v", rec, err)
	}
	r1.teardown()

	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	st := r2.status("run-37")
	if st.State != RunFailed || st.Reason != "worker died" || st.Uncertain || st.EndedAt.IsZero() {
		t.Fatalf("recovered status %+v", st)
	}
	r2.expect("/live/room_ab/vhi.m3u8", http.StatusServiceUnavailable)
	if _, ok := r2.n.Lookup("room_ab"); ok {
		t.Fatal("a failed timeline is served")
	}
	if _, ok := r2.n.ctrl.Lookup("room_ab"); ok {
		t.Fatal("a failed timeline is registered")
	}
	if rec, err := readRunRecord(RunDir(r2.stateDir, "run-37")); err != nil || rec.FinalState != RunFailed || rec.FinalReason != "worker died" {
		t.Fatalf("record after the recovery %+v, %v", rec, err)
	}
	// The worker is asked to stop, and the room stays owned until its unit is gone.
	r2.waitStops("run-37", 1)
	r2.roomOwnedBy("room_ab", "run-37")
	r2.startWaits(r2.spec("run-38", "room_ab"))
	l.unit("run-37").gone(systemd.Exit{Known: true, Result: "success", Code: "exited"})
	if _, created, err := r2.n.Start(context.Background(), r2.spec("run-38", "room_ab")); err != nil || !created {
		t.Fatalf("a new run for the room: %v, %v", created, err)
	}
	_ = pub.Close()
}

func TestNodeRecordWritesSerialize(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-39", "room_ac")
	r.start(spec)
	r.n.mu.Lock()
	run := r.n.runs["run-39"]
	r.n.mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				if err := run.persist(); err != nil {
					t.Errorf("persist: %v", err)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		run.finish(false, "boom", false)
	}()
	wg.Wait()
	rec, err := readRunRecord(RunDir(r.stateDir, "run-39"))
	if err != nil || rec.FinalState != RunFailed || rec.FinalReason != "boom" || rec.EndedUnixNS == 0 {
		t.Fatalf("record after overlapping writes: %+v, %v", rec, err)
	}
	if st := r.status("run-39"); st.State != RunFailed {
		t.Fatalf("status %+v", st)
	}
	run.mu.Lock()
	durable, pending, retired := run.durable, run.pending, run.retired
	run.mu.Unlock()
	if !durable || pending || retired { // durable, but the unit is not gone: the room is still owned
		t.Fatalf("durable %v, pending %v, retired %v", durable, pending, retired)
	}
	if _, err := os.Stat(filepath.Join(RunDir(r.stateDir, "run-39"), runFile+".tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary record left behind: %v", err)
	}
}

func TestNodeRetirementWaitsForDurableRecord(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-40", "room_ad")
	r.start(spec)
	block := filepath.Join(RunDir(r.stateDir, "run-40"), runFile+".tmp")
	if err := os.Mkdir(block, 0o700); err != nil { // the record cannot be written while this exists
		t.Fatal(err)
	}
	r.l.unit("run-40").gone(systemd.Exit{Known: true, Result: "exit-code", Code: "exited", Status: 1})
	st := r.waitState("run-40", RunFailed)
	if st.EndedAt.IsZero() {
		t.Fatalf("status %+v", st)
	}
	// Terminal, but not retired: the room stays owned and a start for it waits.
	r.n.mu.Lock()
	owner := r.n.rooms["room_ad"]
	r.n.mu.Unlock()
	if owner == nil || owner.spec.RunID != "run-40" {
		t.Fatal("the room was freed without a durable terminal record")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, _, err := r.n.Start(ctx, r.spec("run-41", "room_ad"))
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start while the retirement is pending: %v", err)
	}
	if rec, err := readRunRecord(RunDir(r.stateDir, "run-40")); err != nil || rec.FinalState != "" {
		t.Fatalf("a terminal record was written through the block: %+v, %v", rec, err)
	}
	if !strings.Contains(r.logs.String(), "the room stays owned until the terminal state is durable") {
		t.Fatalf("logs:\n%s", r.logs.String())
	}
	if err := os.Remove(block); err != nil {
		t.Fatal(err)
	}
	// maintain retries the retirement; the room is freed once the record is durable.
	ctx, cancel = context.WithTimeout(context.Background(), nodeTestWait)
	defer cancel()
	if st, created, err := r.n.Start(ctx, r.spec("run-41", "room_ad")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("Start after the record became durable: %+v, %v, %v", st, created, err)
	}
	if rec, err := readRunRecord(RunDir(r.stateDir, "run-40")); err != nil || rec.FinalState != RunFailed {
		t.Fatalf("record after the retry %+v, %v", rec, err)
	}
	r.n.mu.Lock()
	run := r.n.runs["run-40"]
	r.n.mu.Unlock()
	run.mu.Lock()
	retired, pending := run.retired, run.pending
	run.mu.Unlock()
	if !retired || pending {
		t.Fatalf("retired %v, pending %v", retired, pending)
	}
}

func TestReadRunRecordStrict(t *testing.T) {
	dir := pubTestTempDir(t)
	good := runRecord{Format: runFormat, RunID: "run-x", RoomID: "room", LiveKitRoom: "lk", HostIdentity: "h", LiveKitURL: nodeTestURL,
		LaunchID: "0123456789abcdef0123456789abcdef", Epoch: 3, CreatedUnixNS: 1}
	if err := writeRunRecord(dir, good); err != nil {
		t.Fatal(err)
	}
	if got, err := readRunRecord(dir); err != nil || got != good {
		t.Fatalf("round trip %+v, %v", got, err)
	}
	data, _ := json.Marshal(good)
	old := good
	old.Format, old.Epoch = 1, 0
	oldData, _ := json.Marshal(old)
	noEpoch := good
	noEpoch.Epoch = 0
	noEpochData, _ := json.Marshal(noEpoch)
	for name, body := range map[string]string{
		"trailing brace":   string(data) + "}",
		"trailing bracket": string(data) + "]",
		"second value":     string(data) + " {}",
		"trailing text":    string(data) + "\nx",
		"unknown field":    strings.TrimSuffix(string(data), "}") + `,"extra":1}`,
		"older format":     string(oldData),
		"no epoch":         string(noEpochData),
		"empty":            "",
	} {
		if err := os.WriteFile(filepath.Join(dir, runFile), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readRunRecord(dir); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, runFile), append(data, " \n\t"...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRunRecord(dir); err != nil {
		t.Fatalf("trailing whitespace refused: %v", err)
	}

	// A record with trailing bytes is left as it is at recovery: the run is unknown, the record
	// untouched, and the reason logged.
	base := pubTestTempDir(t)
	bad := RunDir(filepath.Join(base, "state"), "run-bad")
	if err := writeRunRecord(bad, good); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, runFile), append(data, '}'), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newNodeRigAt(t, base, &fakeLauncher{units: map[string]*fakeUnit{}}, newNodeTestNotifier(t), nil)
	if _, err := r.n.Status(context.Background(), "run-bad"); !errors.Is(err, ErrRunUnknown) {
		t.Fatalf("Status of a run with a corrupt record: %v", err)
	}
	if !strings.Contains(r.logs.String(), "trailing bytes after the record; left as it is") {
		t.Fatalf("logs:\n%s", r.logs.String())
	}
	if got, err := os.ReadFile(filepath.Join(bad, runFile)); err != nil || string(got) != string(data)+"}" {
		t.Fatalf("corrupt record rewritten: %q, %v", got, err)
	}
}

func TestNodeRestartWithWorkerGoneFailsTheRun(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	pub := r1.running(r1.spec("run-33", "room_z"))
	r1.teardown()
	_ = pub.Close()
	l.unit("run-33").gone(systemd.Exit{Known: true, Result: "signal", Code: "killed", Status: 9})

	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	st := r2.status("run-33")
	if st.State != RunFailed || !strings.Contains(st.Reason, "gone across restart") || st.EndedAt.IsZero() {
		t.Fatalf("recovered status %+v", st)
	}
	r2.expect("/live/room_z/vhi.m3u8", http.StatusServiceUnavailable)
	r2.expect(mediaPath("run-33", "vhi/init.mp4"), http.StatusNotFound)
	r2.waitStore(0, nodeTestCfg.Tracks)
	if _, err := os.Stat(r2.socket("run-33")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket of a run that is over: %v", err)
	}
	if _, created, err := r2.n.Start(context.Background(), r2.spec("run-34", "room_z")); err != nil || !created {
		t.Fatalf("a new run for the freed room: %v, %v", created, err)
	}

	// A record whose unit now carries another launch is held, not adopted and not touched.
	r2.teardown()
	l.unit("run-34").setLaunch(foreignLaunch)
	r3 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	if st := r3.status("run-34"); st.State != RunStarting || !st.Uncertain || !strings.Contains(st.Reason, "another launch") {
		t.Fatalf("held recovery %+v", st)
	}
	if _, _, err := r3.n.Start(context.Background(), r3.spec("run-35", "room_z")); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("a new run for a held room: %v", err)
	}
	if u := l.unit("run-34"); u.stops.Load() != 0 {
		t.Fatalf("a foreign unit was stopped %d times", u.stops.Load())
	}
}

func TestNodeRecoveredTerminalRunStopsItsLiveUnit(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	spec := r1.spec("run-50", "room_ba")
	pub := r1.running(spec)
	r1.publishTerminal(pub, true, "host left")
	r1.waitState("run-50", RunEnded)
	r1.waitStops("run-50", 1)
	r1.waitRecord("run-50", func(rec runRecord) bool { return rec.FinalState == RunEnded && !rec.StopPending })
	r1.teardown() // the unit is still loaded: the worker has not left yet
	_ = pub.Close()

	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	if st := r2.status("run-50"); st.State != RunEnded || st.Reason != "host left" || st.Uncertain {
		t.Fatalf("recovered status %+v", st)
	}
	r2.waitStops("run-50", 2) // the recovery asks the loaded unit to stop
	r2.waitRecord("run-50", func(rec runRecord) bool { return rec.FinalState == RunEnded && !rec.StopPending })
	if !strings.Contains(r2.logs.String(), "unit fake-run-50.service still loaded, asked to stop and watched") {
		t.Fatalf("logs:\n%s", r2.logs.String())
	}
	// The room and the epoch are held until the unit is gone: no other run starts in the room,
	// the descriptors stay stored, nothing is served, and the run is still the one run it is.
	r2.expect("/live/room_ba/master.m3u8", http.StatusServiceUnavailable)
	r2.expect(mediaPath("run-50", "vhi/init.mp4"), http.StatusOK)
	r2.roomOwnedBy("room_ba", "run-50")
	r2.startWaits(r2.spec("run-51", "room_ba"))
	if stored, removed := fds.counts(); stored != nodeTestCfg.Tracks || removed != 0 {
		t.Fatalf("fd store while the unit is loaded: %d stored, %d removed", stored, removed)
	}
	if st, created, err := r2.n.Start(context.Background(), spec); err != nil || created || st.State != RunEnded {
		t.Fatalf("Start of the recovered run: %+v, %v, %v", st, created, err)
	}
	if st, err := r2.n.Stop(context.Background(), "run-50"); err != nil || st.State != RunEnded {
		t.Fatalf("Stop of the recovered run: %+v, %v", st, err)
	}
	l.unit("run-50").gone(systemd.Exit{})
	r2.waitGet(mediaPath("run-50", "vhi/init.mp4"), http.StatusNotFound)
	r2.waitStore(0, nodeTestCfg.Tracks)
	if st, created, err := r2.n.Start(context.Background(), r2.spec("run-51", "room_ba")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for the room after the unit is gone: %+v, %v, %v", st, created, err)
	}
	if n := l.unit("run-50").stops.Load(); n != 2 {
		t.Fatalf("unit stopped %d times, want 2", n)
	}
}

func TestNodeRecoveredTerminalRunWithUnitGoneReleases(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	spec := r1.spec("run-52", "room_bb")
	pub := r1.running(spec)
	r1.publishTerminal(pub, true, "host left")
	r1.waitState("run-52", RunEnded)
	r1.waitStops("run-52", 1)
	r1.teardown() // the descriptors stay stored across the restart
	_ = pub.Close()
	l.unit("run-52").gone(systemd.Exit{})
	if stored, _ := fds.counts(); stored != nodeTestCfg.Tracks {
		t.Fatalf("%d descriptors stored before the restart", stored)
	}

	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	if st := r2.status("run-52"); st.State != RunEnded || st.Reason != "host left" {
		t.Fatalf("recovered status %+v", st)
	}
	r2.waitStore(0, nodeTestCfg.Tracks)
	r2.expect(mediaPath("run-52", "vhi/init.mp4"), http.StatusNotFound)
	r2.expect("/live/room_bb/master.m3u8", http.StatusServiceUnavailable)
	if n := l.unit("run-52").stops.Load(); n != 1 {
		t.Fatalf("an absent unit was stopped: %d stops", n)
	}
	if st, created, err := r2.n.Start(context.Background(), r2.spec("run-53", "room_bb")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for the freed room: %+v, %v, %v", st, created, err)
	}
}

func TestNodeRecoveredTerminalRunWithUncertainUnit(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	specA, specB := r1.spec("run-54", "room_bc"), r1.spec("run-55", "room_bd")
	pubA, pubB := r1.running(specA), r1.running(specB)
	r1.publishTerminal(pubA, true, "host left")
	r1.publishTerminal(pubB, true, "host left")
	r1.waitState("run-54", RunEnded)
	r1.waitState("run-55", RunEnded)
	r1.waitStops("run-54", 1)
	r1.waitStops("run-55", 1)
	r1.teardown()
	_, _ = pubA.Close(), pubB.Close()
	launchA := l.unit("run-54").spec.LaunchID
	l.unit("run-54").setLaunch(foreignLaunch) // the units under the names carry another launch
	l.unit("run-55").setLaunch(foreignLaunch)

	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	for _, id := range []string{"run-54", "run-55"} {
		if st := r2.status(id); st.State != RunEnded || st.Reason != "host left" {
			t.Fatalf("recovered status of %s: %+v", id, st)
		}
		if n := l.unit(id).stops.Load(); n != 1 {
			t.Fatalf("a unit not proven this launch's was stopped: %d stops of %s", n, id)
		}
		if !strings.Contains(r2.logs.String(), "unit fake-"+id+".service not established, room owned and unit watched") {
			t.Fatalf("logs:\n%s", r2.logs.String())
		}
	}
	// Nothing is gone, released or reused while the units are not established.
	time.Sleep(50 * time.Millisecond)
	if stored, removed := fds.counts(); stored != 2*nodeTestCfg.Tracks || removed != 0 {
		t.Fatalf("fd store while the units are uncertain: %d stored, %d removed", stored, removed)
	}
	r2.roomOwnedBy("room_bc", "run-54")
	r2.roomOwnedBy("room_bd", "run-55")
	r2.startWaits(r2.spec("run-56", "room_bc"))
	r2.startWaits(r2.spec("run-57", "room_bd"))
	if st, err := r2.n.Stop(context.Background(), "run-54"); !errors.Is(err, ErrRunStop) || st.State != RunEnded {
		t.Fatalf("Stop of the run whose unit is not established: %+v, %v", st, err)
	}

	// The unit under A's name turns out to carry its launch: the owed stop is taken by a retry,
	// and the unit's end then frees the room and releases the epoch.
	l.unit("run-54").setLaunch(launchA)
	r2.waitStops("run-54", 2)
	r2.roomOwnedBy("room_bc", "run-54")
	l.unit("run-54").gone(systemd.Exit{})
	if st, created, err := r2.n.Start(context.Background(), r2.spec("run-56", "room_bc")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for room_bc: %+v, %v, %v", st, created, err)
	}
	r2.waitGet(mediaPath("run-54", "vhi/init.mp4"), http.StatusNotFound)
	r2.waitStore(nodeTestCfg.Tracks, nodeTestCfg.Tracks)

	// The unit under B's name goes away without ever being established: stable absence settles
	// the run, with nothing stopped.
	l.unit("run-55").gone(systemd.Exit{})
	if st, created, err := r2.n.Start(context.Background(), r2.spec("run-57", "room_bd")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for room_bd: %+v, %v, %v", st, created, err)
	}
	r2.waitStore(0, 2*nodeTestCfg.Tracks)
	if n := l.unit("run-55").stops.Load(); n != 1 {
		t.Fatalf("unit of run-55 stopped %d times, want the one stop of its end", n)
	}
}

func TestNodeRecoveryWithUnusableEpochAndUncertainUnitFails(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	spec := r1.spec("run-58", "room_be")
	pub := r1.running(spec)
	r1.teardown()
	_ = pub.Close()
	// A directory of another epoch and a unit that cannot be established.
	if err := os.Mkdir(EpochDir(r1.stateDir, "run-58", "run-58", 2), 0o700); err != nil {
		t.Fatal(err)
	}
	l.unit("run-58").setLaunch(foreignLaunch)

	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	st := r2.status("run-58")
	if st.State != RunFailed || st.Uncertain || !strings.Contains(st.Reason, "recovery failed: epoch directory 2 exists, the record reserved epoch 1") {
		t.Fatalf("recovered status %+v", st)
	}
	r2.waitRecord("run-58", func(rec runRecord) bool { return rec.FinalState == RunFailed && rec.StopPending })
	r2.expect("/live/room_be/master.m3u8", http.StatusServiceUnavailable)
	r2.expect(mediaPath("run-58", "vhi/init.mp4"), http.StatusNotFound)
	// The room stays owned and nothing is stopped while the unit is not proven this launch's.
	r2.roomOwnedBy("room_be", "run-58")
	r2.startWaits(r2.spec("run-59", "room_be"))
	if n := l.unit("run-58").stops.Load(); n != 0 {
		t.Fatalf("a unit not proven this launch's was stopped %d times", n)
	}
	l.unit("run-58").gone(systemd.Exit{})
	if st, created, err := r2.n.Start(context.Background(), r2.spec("run-59", "room_be")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for the room after the unit is gone: %+v, %v, %v", st, created, err)
	}
	if st := r2.status("run-58"); st.State != RunFailed || !strings.Contains(st.Reason, "recovery failed") {
		t.Fatalf("after the unit is gone: %+v", st)
	}
}

func TestNodeOwedStopIsRetried(t *testing.T) {
	r := newNodeRig(t)
	specA, specB := r.spec("run-60", "room_bf"), r.spec("run-61", "room_bg")
	pubA, pubB := r.running(specA), r.running(specB)
	refused := fmt.Errorf("%w: stop fake-run-60.service: Access denied", systemd.ErrUnitControl)
	r.l.unit("run-60").setStopErr(refused)
	r.publishTerminal(pubA, true, "host left")
	r.waitState("run-60", RunEnded)
	// The debt is recorded before the stop is attempted, the refusal logged, the room owned.
	r.waitRecord("run-60", func(rec runRecord) bool { return rec.FinalState == RunEnded && rec.StopPending })
	r.waitLog("stop not taken: systemd: unit control failed: stop fake-run-60.service: Access denied; the stop stays owed")
	if n := r.l.unit("run-60").stops.Load(); n != 0 {
		t.Fatalf("a refused stop counted %d times", n)
	}
	r.roomOwnedBy("room_bf", "run-60")
	r.startWaits(r.spec("run-62", "room_bf"))
	// A repeated stop retries it while it is owed and reports the refusal with the ended run.
	if st, err := r.n.Stop(context.Background(), "run-60"); !errors.Is(err, ErrRunStop) || st.State != RunEnded || !strings.Contains(err.Error(), "Access denied") {
		t.Fatalf("Stop while the stop is owed: %+v, %v", st, err)
	}
	// Maintenance goes on: the healthy run is served and its unit is never asked to stop.
	otContains(t, r.expect("/live/room_bg/vhi.m3u8", http.StatusOK), mediaPath("run-61", "vhi/1.0.m4s"))
	r.publishPart(pubB, 1, 1)
	r.waitGet("/live/room_bg/vhi.m3u8?_HLS_msn=1&_HLS_part=1", http.StatusOK)
	if st := r.status("run-61"); st.State != RunRunning {
		t.Fatalf("the healthy run: %+v", st)
	}
	// Once systemd takes it, the retry from maintain settles the debt, durably.
	r.l.unit("run-60").setStopErr(nil)
	r.waitStops("run-60", 1)
	r.waitRecord("run-60", func(rec runRecord) bool { return rec.FinalState == RunEnded && !rec.StopPending })
	if !strings.Contains(r.logs.String(), "unit fake-run-60.service: stop taken after retries") {
		t.Fatalf("logs:\n%s", r.logs.String())
	}
	if st, err := r.n.Stop(context.Background(), "run-60"); err != nil || st.State != RunEnded {
		t.Fatalf("Stop after the stop was taken: %+v, %v", st, err)
	}
	time.Sleep(20 * time.Millisecond)
	if n := r.l.unit("run-60").stops.Load(); n != 1 {
		t.Fatalf("unit stopped %d times, want 1", n)
	}
	if n := r.l.unit("run-61").stops.Load(); n != 0 {
		t.Fatalf("the healthy run's unit was stopped %d times", n)
	}
	r.roomOwnedBy("room_bf", "run-60") // until the unit is gone
	_ = pubA.Close()
	r.l.unit("run-60").gone(systemd.Exit{})
	if st, created, err := r.n.Start(context.Background(), r.spec("run-62", "room_bf")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for the room: %+v, %v, %v", st, created, err)
	}
	if st, err := r.n.Stop(context.Background(), "run-60"); err != nil || st.State != RunEnded {
		t.Fatalf("Stop after the unit is gone: %+v, %v", st, err)
	}
	if n := r.l.unit("run-60").stops.Load(); n != 1 {
		t.Fatalf("unit stopped %d times after it was gone", n)
	}

	// A repeated stop that systemd then takes settles the debt itself.
	specC := r.spec("run-63", "room_bh")
	pubC := r.running(specC)
	r.l.unit("run-63").setStopErr(refused)
	r.publishTerminal(pubC, true, "host left")
	r.waitState("run-63", RunEnded)
	r.waitLog("unit fake-run-63.service: stop not taken")
	r.l.unit("run-63").setStopErr(nil)
	if st, err := r.n.Stop(context.Background(), "run-63"); err != nil || st.State != RunEnded {
		t.Fatalf("Stop retrying the owed stop: %+v, %v", st, err)
	}
	r.waitStops("run-63", 1)
	time.Sleep(20 * time.Millisecond)
	if n := r.l.unit("run-63").stops.Load(); n != 1 {
		t.Fatalf("unit stopped %d times, want 1", n)
	}
}

func TestNodeReleaseRetriesDescriptorRemoval(t *testing.T) {
	r := newNodeRig(t)
	spec := r.spec("run-64", "room_bi")
	pub := r.running(spec)
	r.publishTerminal(pub, true, "host left")
	r.waitState("run-64", RunEnded)
	r.waitStops("run-64", 1)
	_ = pub.Close()
	r.fds.pause() // the store cannot be reached
	r.l.unit("run-64").gone(systemd.Exit{})
	r.waitLog("from the store: ")
	r.waitLog("asked again until sent")
	r.n.mu.Lock()
	run := r.n.runs["run-64"]
	r.n.mu.Unlock()
	run.mu.Lock()
	released, retired := run.released, run.retired
	run.mu.Unlock()
	if released || !retired {
		t.Fatalf("released %v, retired %v after removals that could not be sent", released, retired)
	}
	if stored, removed := r.fds.counts(); stored != nodeTestCfg.Tracks || removed != 0 {
		t.Fatalf("fd store: %d stored, %d removed", stored, removed)
	}
	r.expect(mediaPath("run-64", "vhi/init.mp4"), http.StatusNotFound) // the source is closed
	if strings.Contains(r.logs.String(), "released") {
		t.Fatalf("the epoch was reported released:\n%s", r.logs.String())
	}
	r.fds.resume(t)
	r.waitStore(0, nodeTestCfg.Tracks)
	for deadline := time.Now().Add(nodeTestWait); ; time.Sleep(5 * time.Millisecond) {
		run.mu.Lock()
		released = run.released
		run.mu.Unlock()
		if released {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the epoch was not released after the removals were sent")
		}
	}
	if !strings.Contains(r.logs.String(), "epoch 1 released: every removal from the store sent") {
		t.Fatalf("logs:\n%s", r.logs.String())
	}
}

// ownedStopRefusedForGood ends spec's run with a verified terminal while its unit refuses every
// stop, so that the terminal record carries the owed stop, and returns the publisher.
func (r *nodeRig) ownedStopRefusedForGood(spec RunSpec) *worker.Publisher {
	r.t.Helper()
	pub := r.running(spec)
	r.l.unit(spec.RunID).setStopErr(fmt.Errorf("%w: stop %s: Access denied", systemd.ErrUnitControl, r.l.unit(spec.RunID).name))
	r.publishTerminal(pub, true, "host left")
	r.waitState(spec.RunID, RunEnded)
	r.waitRecord(spec.RunID, func(rec runRecord) bool { return rec.FinalState == RunEnded && rec.StopPending })
	r.waitLog("unit " + r.l.unit(spec.RunID).name + ": stop not taken")
	return pub
}

// recordDirty reports the run's dirty-record and retired flags.
func (r *nodeRig) recordDirty(id string) (pending, retired bool) {
	r.n.mu.Lock()
	run := r.n.runs[id]
	r.n.mu.Unlock()
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.pending, run.retired
}

func TestNodeUnitGoneClearsTheOwedStopDurably(t *testing.T) {
	r := newNodeRig(t)
	// The unit goes away while the stop it was owed is still refused: the record no longer says
	// the stop is owed, the room is free, and nothing else is stopped.
	spec := r.spec("run-65", "room_bj")
	pub := r.ownedStopRefusedForGood(spec)
	_ = pub.Close()
	r.l.unit("run-65").gone(systemd.Exit{})
	r.waitRecord("run-65", func(rec runRecord) bool {
		return rec.FinalState == RunEnded && rec.FinalReason == "host left" && !rec.StopPending
	})
	if st, created, err := r.n.Start(context.Background(), r.spec("run-66", "room_bj")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for the room: %+v, %v, %v", st, created, err)
	}
	if pending, retired := r.recordDirty("run-65"); pending || !retired {
		t.Fatalf("pending %v, retired %v", pending, retired)
	}
	if n := r.l.unit("run-65").stops.Load(); n != 0 {
		t.Fatalf("unit stopped %d times", n)
	}

	// The record cannot be rewritten when the unit goes: it stays dirty, the room is free all the
	// same, its terminal state being durable, and maintenance rewrites it once it can.
	spec2 := r.spec("run-67", "room_bk")
	pub2 := r.ownedStopRefusedForGood(spec2)
	block := filepath.Join(RunDir(r.stateDir, "run-67"), runFile+".tmp")
	if err := os.Mkdir(block, 0o700); err != nil {
		t.Fatal(err)
	}
	_ = pub2.Close()
	r.l.unit("run-67").gone(systemd.Exit{})
	r.waitLog("the record still says the stop is owed and is rewritten by maintenance")
	if rec, err := readRunRecord(RunDir(r.stateDir, "run-67")); err != nil || rec.FinalState != RunEnded || !rec.StopPending {
		t.Fatalf("record through the block: %+v, %v", rec, err)
	}
	if st, created, err := r.n.Start(context.Background(), r.spec("run-68", "room_bk")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for the room with a dirty record: %+v, %v, %v", st, created, err)
	}
	if pending, retired := r.recordDirty("run-67"); !pending || !retired {
		t.Fatalf("pending %v, retired %v", pending, retired)
	}
	time.Sleep(2 * maintainInterval) // the retries through the block change nothing
	if rec, err := readRunRecord(RunDir(r.stateDir, "run-67")); err != nil || !rec.StopPending {
		t.Fatalf("record through the block after retries: %+v, %v", rec, err)
	}
	if err := os.Remove(block); err != nil {
		t.Fatal(err)
	}
	r.waitRecord("run-67", func(rec runRecord) bool { return rec.FinalState == RunEnded && !rec.StopPending })
	for deadline := time.Now().Add(nodeTestWait); ; time.Sleep(5 * time.Millisecond) {
		if pending, _ := r.recordDirty("run-67"); !pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the record is still dirty after it was rewritten")
		}
	}
	if st := r.status("run-67"); st.State != RunEnded {
		t.Fatalf("status %+v", st)
	}
}

func TestNodeRecoveredTerminalRunWithUnitGoneClearsTheOwedStop(t *testing.T) {
	base := pubTestTempDir(t)
	l := &fakeLauncher{units: map[string]*fakeUnit{}}
	fds := newNodeTestNotifier(t)
	r1 := newNodeRigAt(t, base, l, fds, nil)
	specA, specB := r1.spec("run-69", "room_bl"), r1.spec("run-70", "room_bm")
	pubA := r1.ownedStopRefusedForGood(specA)
	pubB := r1.ownedStopRefusedForGood(specB)
	r1.teardown()
	_, _ = pubA.Close(), pubB.Close()
	l.unit("run-69").gone(systemd.Exit{})
	l.unit("run-70").gone(systemd.Exit{})
	blockB := filepath.Join(RunDir(r1.stateDir, "run-70"), runFile+".tmp")
	if err := os.Mkdir(blockB, 0o700); err != nil { // B's record cannot be rewritten at recovery
		t.Fatal(err)
	}

	r2 := newNodeRigAt(t, base, l, fds, fds.inherit(t))
	// A: the unit is confirmed absent, the record no longer says the stop is owed.
	if st := r2.status("run-69"); st.State != RunEnded || st.Reason != "host left" {
		t.Fatalf("recovered status of run-69: %+v", st)
	}
	if rec, err := readRunRecord(RunDir(r2.stateDir, "run-69")); err != nil || rec.FinalState != RunEnded || rec.StopPending {
		t.Fatalf("record of run-69 after the recovery: %+v, %v", rec, err)
	}
	if pending, retired := r2.recordDirty("run-69"); pending || !retired {
		t.Fatalf("run-69: pending %v, retired %v", pending, retired)
	}
	if st, created, err := r2.n.Start(context.Background(), r2.spec("run-71", "room_bl")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for room_bl: %+v, %v, %v", st, created, err)
	}
	// B: the record stays dirty, the room is free all the same, and maintenance rewrites it.
	if st := r2.status("run-70"); st.State != RunEnded {
		t.Fatalf("recovered status of run-70: %+v", st)
	}
	r2.waitLog("the record still says the stop is owed and is rewritten by maintenance")
	if rec, err := readRunRecord(RunDir(r2.stateDir, "run-70")); err != nil || rec.FinalState != RunEnded || !rec.StopPending {
		t.Fatalf("record of run-70 through the block: %+v, %v", rec, err)
	}
	if pending, retired := r2.recordDirty("run-70"); !pending || !retired {
		t.Fatalf("run-70: pending %v, retired %v", pending, retired)
	}
	if st, created, err := r2.n.Start(context.Background(), r2.spec("run-72", "room_bm")); err != nil || !created || st.State != RunStarting {
		t.Fatalf("a new run for room_bm: %+v, %v, %v", st, created, err)
	}
	if err := os.Remove(blockB); err != nil {
		t.Fatal(err)
	}
	r2.waitRecord("run-70", func(rec runRecord) bool {
		return rec.FinalState == RunEnded && rec.FinalReason == "host left" && !rec.StopPending
	})
	for deadline := time.Now().Add(nodeTestWait); ; time.Sleep(5 * time.Millisecond) {
		if pending, _ := r2.recordDirty("run-70"); !pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run-70's record is still dirty after it was rewritten")
		}
	}
	r2.waitStore(0, 2*nodeTestCfg.Tracks) // both epochs released: the units are gone
	for _, id := range []string{"run-69", "run-70"} {
		if n := l.unit(id).stops.Load(); n != 0 {
			t.Fatalf("an absent unit of %s was stopped %d times", id, n)
		}
	}
}

func TestBoundReason(t *testing.T) {
	if got := boundReason("a\nb\x00c"); got != "a b c" {
		t.Fatalf("control characters: %q", got)
	}
	long := strings.Repeat("é", 200)
	if got := boundReason(long); len(got) != maxReason || !strings.HasSuffix(got, "é") {
		t.Fatalf("cut to %d bytes ending %q", len(got), got[len(got)-2:])
	}
	if got := boundReason("ok\xff"); got != "ok?" {
		t.Fatalf("invalid UTF-8: %q", got)
	}
}
