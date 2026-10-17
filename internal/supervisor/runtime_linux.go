//go:build linux

package supervisor

// Runtime: the supervisor role's continuous single-room production path. It owns the IPC2
// listening socket, one PlaybackController, one room Timeline and the LL-HLS origin, and admits
// one worker process at a time into one source epoch: the first connection of a new worker
// process allocates the next epoch for the configured run and source, records that process as
// the epoch's owner, and every connection of that process is served by the epoch's Source. The
// room is bound to an epoch once its presentation, every init and the first part of its first
// segment are durable, switched to a later epoch the same way, ended on a verified terminal and
// failed when the worker process exits without one. Epochs are never re-adopted after a
// supervisor restart here: a worker that outlives the supervisor is refused on its reconnect
// hello and restarts as a new epoch. Pure Go, no Cgo.

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
	"sync"
	"sync/atomic"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
	"golang.org/x/sys/unix"
)

// RuntimeOptions configure Run. StateDir holds the epoch directories (EpochDir) and the room
// journal (RoomDir); Socket is the absolute path of the IPC2 unixpacket socket; HTTPAddr the
// origin's listen address; NotifySocket the value of NOTIFY_SOCKET, required because every epoch
// stores its descriptors with systemd. Run, Source and Config are the one source every worker
// must name in its hello; Room the room identity the presentation slot must name; WorkerUID the
// uid every worker process must run as, which must be this process's uid because the worker
// writes presentation.json into the epoch directory and LoadPresentation requires the file to be
// this uid's.
type RuntimeOptions struct {
	StateDir     string
	Socket       string
	HTTPAddr     string
	NotifySocket string
	Run          string
	Source       string
	Room         string
	WorkerUID    int
	Config       ipc.Config
	Policy       partstore.Policy
	Log          *log.Logger
}

const (
	maintainInterval = 500 * time.Millisecond
	inheritedFDLimit = 64
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

// epochState is one admitted worker epoch and what the runtime has done with it.
type epochState struct {
	key    EpochKey
	src    *Source
	owner  WorkerOwner
	exited atomic.Bool // the worker process is gone

	mu          sync.Mutex
	exposed     bool // Bind or PlanSwitch issued, or given up on
	ended       bool // End issued
	maintainErr bool // a Maintain failure was logged
}

type roomRuntime struct {
	o        RuntimeOptions
	log      *log.Logger
	notifier *systemd.Notifier
	tl       *Timeline

	mu     sync.Mutex
	epochs map[EpochKey]*epochState
	owners map[WorkerOwner]*epochState
	next   uint64
	wg     sync.WaitGroup
}

// Run serves the configured room until ctx ends or a fatal error occurs: the controller or the
// origin failing, or the listener breaking. It returns ctx's error on a clean stop.
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
	if err := dropInheritedFDs(ctx, n, o.Log); err != nil {
		return err
	}
	srcRoot := filepath.Dir(EpochDir(o.StateDir, o.Run, o.Source, 1))
	roomDir := RoomDir(o.StateDir, o.Run, o.Room)
	for _, d := range []string{srcRoot, roomDir} {
		if err := ensurePrivateDir(d); err != nil {
			return err
		}
	}
	next, err := nextEpoch(srcRoot)
	if err != nil {
		return err
	}
	r := &roomRuntime{o: o, log: o.Log, notifier: n, epochs: map[EpochKey]*epochState{}, owners: map[WorkerOwner]*epochState{}, next: next}
	ctrl, err := NewPlaybackController(r.resolve, nil)
	if err != nil {
		return err
	}
	tl, err := OpenTimeline(ctx, TimelineOptions{Directory: roomDir, Room: o.Room}, r.resolve)
	if err != nil {
		return err
	}
	defer tl.Close()
	r.tl = tl
	if err := ctrl.Register(tl); err != nil {
		return err
	}
	ln, err := listenPacket(o.Socket)
	if err != nil {
		return err
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	fatal := make(chan error, 3)
	srv := &http.Server{Addr: o.HTTPAddr, Handler: NewOrigin(ctrl), ReadHeaderTimeout: 5 * time.Second}
	go func() { fatal <- fmt.Errorf("supervisor: controller: %w", ctrl.Run(ctx)) }()
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("supervisor: origin: %w", err)
		}
	}()
	go func() { fatal <- r.accept(ctx, ln) }()
	r.wg.Add(1)
	go r.maintain(ctx)
	if err := n.Ready(ctx); err != nil {
		r.log.Printf("READY=1 not sent: %v", err)
	}
	r.log.Printf("serving run %q source %q room %q: ipc %s, origin %s, epochs from %d", o.Run, o.Source, o.Room, o.Socket, o.HTTPAddr, next)

	var cause error
	select {
	case <-ctx.Done():
		cause = ctx.Err()
	case err := <-fatal:
		cause = err
	}
	cancel()
	ln.Close()
	shutdownCtx, stop := context.WithTimeout(context.Background(), shutdownGrace)
	srv.Shutdown(shutdownCtx)
	stop()
	r.wg.Wait()
	r.mu.Lock()
	eps := make([]*epochState, 0, len(r.epochs))
	for _, ep := range r.epochs {
		eps = append(eps, ep)
	}
	r.mu.Unlock()
	for _, ep := range eps {
		if err := ep.src.Close(); err != nil {
			r.log.Printf("epoch %d: close: %v", ep.key.Epoch, err)
		}
	}
	return cause
}

func checkRuntimeOptions(o RuntimeOptions) error {
	switch {
	case !filepath.IsAbs(o.StateDir):
		return fmt.Errorf("%w: state directory %q is not absolute", ErrInvalidOptions, o.StateDir)
	case !filepath.IsAbs(o.Socket):
		return fmt.Errorf("%w: socket %q is not absolute", ErrInvalidOptions, o.Socket)
	case o.HTTPAddr == "":
		return fmt.Errorf("%w: empty origin address", ErrInvalidOptions)
	case o.WorkerUID != os.Geteuid():
		return fmt.Errorf("%w: worker uid %d differs from this process's uid %d; the presentation handoff requires the same uid", ErrInvalidOptions, o.WorkerUID, os.Geteuid())
	}
	if err := checkRoomID(o.Room); err != nil {
		return err
	}
	hello := ipc.Hello{Version: ipc.Version, Run: o.Run, Source: o.Source, PID: os.Getpid(), Config: o.Config}
	if err := ipc.ValidatePacket(ipc.Packet{Type: ipc.TypeHello, Hello: &hello}); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidOptions, err)
	}
	if o.Config.Mode != ipc.ModeSolo {
		return fmt.Errorf("%w: mode %q; this path serves one solo room", ErrInvalidOptions, o.Config.Mode)
	}
	_, err := effectivePolicy(o.Policy)
	return err
}

// dropInheritedFDs consumes whatever systemd passed from the file-descriptor store and closes
// it: epochs are not re-adopted here, and the stored names are released so the store does not
// fill over restarts.
func dropInheritedFDs(ctx context.Context, n *systemd.Notifier, l *log.Logger) error {
	fds, err := systemd.ConsumeInheritedFDs(inheritedFDLimit)
	if err != nil {
		return err
	}
	for name, fd := range fds {
		closeFD(fd)
		if err := n.RemoveFD(ctx, name); err != nil {
			l.Printf("inherited descriptor %q: remove from the store: %v", name, err)
		}
	}
	if len(fds) > 0 {
		l.Printf("dropped %d inherited descriptors: epochs are not re-adopted", len(fds))
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

// listenPacket listens on the IPC2 socket path, replacing a stale socket file and nothing else.
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

// accept admits connections until the listener closes.
func (r *roomRuntime) accept(ctx context.Context, ln *net.UnixListener) error {
	for {
		conn, err := ln.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("supervisor: accept: %w", err)
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.admit(ctx, conn)
		}()
	}
}

// admit authenticates one connection, finds or creates its process's epoch and serves it. The
// hello is left unread for ServeConn.
func (r *roomRuntime) admit(ctx context.Context, conn *net.UnixConn) {
	id, err := ipc.PeerProcessIdentity(conn)
	if err != nil {
		r.log.Printf("connection refused: %v", err)
		conn.Close()
		return
	}
	if id.UID != r.o.WorkerUID {
		r.log.Printf("connection refused: peer uid %d pid %d, want uid %d", id.UID, id.PID, r.o.WorkerUID)
		closeFD(id.Pidfd)
		conn.Close()
		return
	}
	owner := WorkerOwner{UID: id.UID, PID: id.PID, Ino: id.Ino}
	ep, err := r.epochFor(ctx, owner, id.Pidfd) // consumes the pidfd on every path
	if err != nil {
		// A reject, rather than a bare close, makes the worker fail at once instead of retrying.
		r.log.Printf("worker pid %d: epoch not created: %v", id.PID, err)
		_ = ipc.WritePublicationMsg(conn, rejectPacket(reasonRefused), nil, time.Now().Add(netDeadline))
		conn.Close()
		return
	}
	switch err := ep.src.ServeConn(ctx, conn); {
	case err == nil, errors.Is(err, ErrBusy), ctx.Err() != nil:
	default:
		r.log.Printf("epoch %d session ended: %v", ep.key.Epoch, err)
	}
}

// epochFor returns the epoch owned by owner, creating the next epoch when it has none. On
// creation the pidfd is kept by the exit watcher; otherwise it is closed here.
func (r *roomRuntime) epochFor(ctx context.Context, owner WorkerOwner, pidfd int) (*epochState, error) {
	r.mu.Lock()
	if ep := r.owners[owner]; ep != nil {
		r.mu.Unlock()
		closeFD(pidfd)
		return ep, nil
	}
	epoch := r.next
	r.next++
	r.mu.Unlock()
	dir := EpochDir(r.o.StateDir, r.o.Run, r.o.Source, epoch)
	if err := os.Mkdir(dir, 0o700); err != nil {
		closeFD(pidfd)
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		closeFD(pidfd)
		return nil, err
	}
	src, err := CreateSource(ctx, SourceOptions{Directory: dir, Run: r.o.Run, Source: r.o.Source, Epoch: epoch,
		Config: r.o.Config, Policy: r.o.Policy, Owner: owner}, r.notifier)
	if err != nil {
		closeFD(pidfd)
		return nil, err
	}
	ep := &epochState{key: EpochKey{Run: r.o.Run, Source: r.o.Source, Epoch: epoch}, src: src, owner: owner}
	r.mu.Lock()
	r.epochs[ep.key] = ep
	r.owners[owner] = ep
	r.mu.Unlock()
	r.log.Printf("epoch %d created for worker pid %d (pidfs inode %d) in %s", epoch, owner.PID, owner.Ino, dir)
	r.wg.Add(2)
	go r.watch(ctx, ep, pidfd)
	go r.follow(ctx, ep)
	return ep, nil
}

// resolve is the EpochResolver of the controller and the timeline: epochs this runtime does not
// hold are lost.
func (r *roomRuntime) resolve(key EpochKey) (*Source, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ep := r.epochs[key]
	if ep == nil {
		return nil, fmt.Errorf("%w: %+v", ErrLostEpoch, key)
	}
	return ep.src, nil
}

// watch owns the worker's pidfd until the process exits or ctx ends, then closes it. An exit
// without a verified terminal fails the epoch's generation in the room; the Source stays open
// for whatever the room still references.
func (r *roomRuntime) watch(ctx context.Context, ep *epochState, pidfd int) {
	defer r.wg.Done()
	defer closeFD(pidfd)
	for ctx.Err() == nil {
		fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, pidfdPollMS)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			r.log.Printf("epoch %d: pidfd poll: %v", ep.key.Epoch, err)
			return
		}
		if n > 0 && fds[0].Revents != 0 {
			break
		}
	}
	if ctx.Err() != nil {
		return
	}
	ep.exited.Store(true)
	r.mu.Lock()
	if r.owners[ep.owner] == ep {
		delete(r.owners, ep.owner)
	}
	r.mu.Unlock()
	if snap, err := ep.src.Snapshot(); err == nil && snap.Terminal != nil && snap.Terminal.Verified {
		r.log.Printf("epoch %d: worker pid %d exited after its verified terminal", ep.key.Epoch, ep.owner.PID)
		return
	}
	r.log.Printf("epoch %d: worker pid %d exited without a verified terminal", ep.key.Epoch, ep.owner.PID)
	if err := r.tl.FailSource(ep.key, "worker process exited", monotonicNow()); err != nil && !errors.Is(err, ErrTimelineInvalid) {
		r.log.Printf("epoch %d: fail source: %v", ep.key.Epoch, err)
	}
}

// follow drives the room from one epoch's publications: it binds or switches the room to the
// epoch once its start is durable and ends the room on a verified terminal.
func (r *roomRuntime) follow(ctx context.Context, ep *epochState) {
	defer r.wg.Done()
	for {
		obs, err := ep.src.Observe()
		if err != nil {
			r.log.Printf("epoch %d: observe: %v", ep.key.Epoch, err)
			return
		}
		if obs.Closed {
			return
		}
		r.expose(ep, obs.Snapshot)
		if t := obs.Snapshot.Terminal; t != nil {
			if t.Verified {
				r.end(ep)
			} else {
				r.log.Printf("epoch %d: worker reported failure: %s", ep.key.Epoch, t.Reason)
			}
			return
		}
		select {
		case <-obs.Changed:
		case <-ctx.Done():
			return
		}
	}
}

// expose binds or switches the room to ep once its presentation, inits and first segment are
// durable. A refusal other than "not ready" gives the epoch up.
func (r *roomRuntime) expose(ep *epochState, snap partstore.RingSnapshot) {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	if ep.exposed || ep.exited.Load() {
		return
	}
	ready, err := startDurable(ep.src, snap, r.o.Room)
	if err != nil {
		r.log.Printf("epoch %d: cannot be exposed: %v", ep.key.Epoch, err)
		ep.exposed = true
		return
	}
	if !ready {
		return
	}
	tsnap, _, err := r.tl.Observe()
	if err != nil {
		return
	}
	now := monotonicNow()
	switch {
	case tsnap.Ended:
		err = fmt.Errorf("room %q has ended", r.o.Room)
	case tsnap.Pending != nil:
		return // a switch is in flight; the next source change tries again
	case tsnap.Live == nil:
		err = r.tl.Bind(ep.key, 1)
	default:
		err = r.tl.PlanSwitch(ep.key, 1, now)
	}
	switch {
	case errors.Is(err, ErrTimelineNotReady):
		return
	case err != nil:
		r.log.Printf("epoch %d: cannot be exposed: %v", ep.key.Epoch, err)
	default:
		r.log.Printf("epoch %d: exposed in room %q", ep.key.Epoch, r.o.Room)
	}
	ep.exposed = true
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

// end ends the room after ep's verified terminal, once its exposure has caught up with the
// closed segments. Only the live generation ends the room.
func (r *roomRuntime) end(ep *epochState) {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	if ep.ended {
		return
	}
	ep.ended = true
	snap, _, err := r.tl.Observe()
	if err != nil || snap.Live == nil || snap.Live.Key != ep.key || snap.Ended {
		r.log.Printf("epoch %d: verified terminal while not live; room left as it is", ep.key.Epoch)
		return
	}
	if err := r.tl.Refresh(monotonicNow()); err != nil {
		r.log.Printf("epoch %d: refresh before end: %v", ep.key.Epoch, err)
	}
	if err := r.tl.End(); err != nil {
		r.log.Printf("epoch %d: end room: %v", ep.key.Epoch, err)
		return
	}
	r.log.Printf("epoch %d: room %q ended cleanly", ep.key.Epoch, r.o.Room)
}

// maintain rolls every open Source's retention on a fixed cadence and closes the epochs whose
// worker is gone and whose bytes the room no longer references, releasing their stored
// descriptors.
func (r *roomRuntime) maintain(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(maintainInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := monotonicNow()
		referenced := map[EpochKey]bool{}
		for _, key := range r.tl.References(now) {
			referenced[key] = true
		}
		r.mu.Lock()
		eps := make([]*epochState, 0, len(r.epochs))
		for _, ep := range r.epochs {
			eps = append(eps, ep)
		}
		r.mu.Unlock()
		for _, ep := range eps {
			if ep.exited.Load() && !referenced[ep.key] {
				r.release(ctx, ep)
				continue
			}
			err := ep.src.Maintain(now)
			ep.mu.Lock()
			if err != nil && !ep.maintainErr && !errors.Is(err, ErrClosed) {
				ep.maintainErr = true
				r.log.Printf("epoch %d: maintain: %v", ep.key.Epoch, err)
			}
			ep.mu.Unlock()
		}
	}
}

// release closes an unreferenced epoch of an exited worker and removes its descriptors from the
// systemd store.
func (r *roomRuntime) release(ctx context.Context, ep *epochState) {
	r.mu.Lock()
	delete(r.epochs, ep.key)
	r.mu.Unlock()
	info := ep.src.Info()
	if err := ep.src.Close(); err != nil {
		r.log.Printf("epoch %d: close: %v", ep.key.Epoch, err)
	}
	for _, id := range info.Tracks {
		if err := r.notifier.RemoveFD(ctx, id.Name); err != nil && ctx.Err() == nil {
			r.log.Printf("epoch %d: remove %s from the store: %v", ep.key.Epoch, id.Name, err)
		}
	}
	r.log.Printf("epoch %d released", ep.key.Epoch)
}
