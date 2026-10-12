//go:build linux

// Package supervisor owns the publication side of one source epoch in the supervisor role: the
// epoch's memfds, its journalled Ring, its durable owner record and the IPC2 session with the one
// worker process allowed to publish into it. A Source is created once per epoch, when the epoch
// is allocated, and reopened after a supervisor restart from the inherited descriptors and the
// journal; it never allocates a replacement for a lost epoch. The pure-Go supervisor role has no
// Cgo and this package has none.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
)

var (
	// ErrInvalidOptions reports SourceOptions, a directory or an argument the package refuses
	// before touching any descriptor.
	ErrInvalidOptions = errors.New("supervisor: invalid options")

	// ErrLostEpoch reports an epoch that cannot be reopened: owner.json or journal.bin missing or
	// corrupt, written in another boot, or inherited descriptors that are not exactly the epoch's.
	// The epoch's history is gone; nothing is allocated in its place here.
	ErrLostEpoch = errors.New("supervisor: epoch lost")

	// ErrClosed reports an operation on a closed Source.
	ErrClosed = errors.New("supervisor: source closed")

	// ErrRefusing reports that the Source refuses mutations and admissions after an ambiguous
	// persistence or compaction outcome. Retained reads still work.
	ErrRefusing = errors.New("supervisor: source refuses mutations")

	// ErrUnauthorized reports a connection whose peer is not the epoch's worker process.
	ErrUnauthorized = errors.New("supervisor: unauthorized peer")

	// ErrBusy reports a connection refused because the Source still serves an earlier one. The
	// active session is left untouched; the worker retries once it has given up the old one.
	ErrBusy = errors.New("supervisor: source busy")

	// ErrProtocol reports a session the worker broke: a packet the session state does not admit,
	// a mismatch with the epoch, or a request the ring refuses. The connection is closed.
	ErrProtocol = errors.New("supervisor: protocol violation")
)

// WorkerOwner is the kernel identity of the one worker process allowed to publish into an epoch:
// its credentials and the pidfs inode of its pidfd, which no other process shares.
type WorkerOwner struct {
	UID int
	PID int
	Ino uint64
}

// SourceOptions describe the epoch to create. Directory is the epoch's private 0700 directory,
// which must exist and hold neither owner.json nor journal.bin. Policy fields that are zero take
// the partstore defaults.
type SourceOptions struct {
	Directory string
	Run       string
	Source    string
	Epoch     uint64
	Config    ipc.Config
	Policy    partstore.Policy
	Owner     WorkerOwner
}

// Source is one source epoch: the store, its journalled ring and the publication session. All
// mutations of the ring (publication and maintenance) are serialized by op; reads go to the ring
// directly and stay available while the Source refuses mutations.
type Source struct {
	dir    string
	run    string
	source string
	epoch  uint64
	cfg    ipc.Config
	policy partstore.Policy // effective
	ids    []ipc.FDIdentity
	owner  WorkerOwner

	store   *partstore.EpochStore
	journal *partstore.Journal
	ring    *partstore.Ring

	op       sync.Mutex             // serializes publication and maintenance
	high     uint64                 // entry high-water, mirrors the ring
	fresh    bool                   // no record persisted yet: a setup hello may be answered again
	pending  *partstore.Reservation // the single in-flight grant, for entry high+1
	refusing atomic.Bool
	changed  chan struct{}                 // closed and replaced under op on every observable change
	pins     map[*RetentionPin]map[int]int // retention floors by pin, under op

	mu       sync.Mutex // admission and the active connection
	closed   bool
	active   *session
	done     chan struct{} // closed once the first Close has finished its cleanup
	closeErr error         // that cleanup's result, shared by every Close caller

	// hook, nil by default, runs at "reserve_durable" and "commit_durable" immediately before the
	// respective reply, under op, for later crash proofs. It must not call back into the Source.
	hook func(string)
}

// CreateSource allocates the epoch, records its owner durably, creates its journal and stores
// borrowed duplicates of its descriptors in the systemd fd store under their canonical names, then
// returns the Source, which may welcome its worker from here on. On failure after allocation the
// journal and store are closed and whatever reached the directory is left as evidence; nothing is
// replaced and nothing is retried.
func CreateSource(ctx context.Context, o SourceOptions, n *systemd.Notifier) (*Source, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n == nil {
		return nil, fmt.Errorf("%w: nil notifier", ErrInvalidOptions)
	}
	pol, err := effectivePolicy(o.Policy)
	if err != nil {
		return nil, err
	}
	if err := checkOwner(o.Owner); err != nil {
		return nil, err
	}
	dirfd, err := openPrivateDir(o.Directory)
	if err != nil {
		return nil, err
	}
	defer closeFD(dirfd)
	for _, name := range []string{ownerFile, journalFile} {
		if err := absent(dirfd, name); err != nil {
			return nil, err
		}
	}
	store, err := partstore.AllocateEpoch(o.Run, o.Source, o.Epoch, o.Config)
	if err != nil {
		return nil, err
	}
	ids, err := store.Identities()
	if err != nil {
		store.Close()
		return nil, err
	}
	boot, err := currentBootID()
	if err != nil {
		store.Close()
		return nil, err
	}
	state := ownerState{Format: ownerFormat, WireMajor: ipc.PublicationVersion, BootID: boot,
		Run: o.Run, Source: o.Source, Epoch: o.Epoch, Config: o.Config,
		Policy: policyState{WindowNS: int64(pol.Window), MaxSegmentNS: int64(pol.MaxSegment)},
		Worker: workerState{UID: o.Owner.UID, PID: o.Owner.PID, Ino: o.Owner.Ino}, Tracks: ids}
	if err := writeOwnerState(dirfd, state); err != nil {
		store.Close()
		return nil, err
	}
	j, r, err := partstore.CreateJournal(o.Directory, store, o.Run, o.Source, o.Epoch, o.Config, o.Policy)
	if err != nil {
		store.Close()
		return nil, err
	}
	if err := storeDescriptors(ctx, n, store, ids); err != nil {
		j.Close()
		store.Close()
		return nil, err
	}
	s := newSource(o.Directory, o.Run, o.Source, o.Epoch, o.Config, pol, ids, o.Owner, store, j, r)
	if err := s.load(); err != nil {
		j.Close()
		store.Close()
		return nil, err
	}
	return s, nil
}

// storeDescriptors asks systemd to keep a duplicate of every track under its canonical name and
// waits for the datagrams to be processed. The duplicates are closed before returning.
func storeDescriptors(ctx context.Context, n *systemd.Notifier, store *partstore.EpochStore, ids []ipc.FDIdentity) error {
	dups, err := store.DupFDs()
	if err != nil {
		return err
	}
	defer closeUnique(dups)
	for i, fd := range dups {
		if err := n.StoreFD(ctx, ids[i].Name, fd); err != nil {
			return fmt.Errorf("supervisor: store track %d: %w", i, err)
		}
	}
	if err := n.Barrier(ctx); err != nil {
		return fmt.Errorf("supervisor: fd store barrier: %w", err)
	}
	return nil
}

// OpenSource reopens the epoch in dir after a supervisor restart from fds, the inherited
// descriptors by fd store name. It consumes every supplied descriptor on every path: the set must
// be exactly the epoch's canonical names as owner.json records them, each is verified against the
// recorded identity, and the journal is restored. A missing or corrupt owner record, journal or
// descriptor set is ErrLostEpoch; nothing is allocated afresh.
func OpenSource(ctx context.Context, dir string, fds map[string]int) (*Source, error) {
	offered := make([]int, 0, len(fds))
	for _, fd := range fds {
		offered = append(offered, fd)
	}
	consumed := false
	defer func() {
		if !consumed {
			closeUnique(offered)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dirfd, err := openPrivateDir(dir)
	if err != nil {
		return nil, err
	}
	defer closeFD(dirfd)
	state, err := readOwnerState(dirfd)
	if err != nil {
		return nil, err
	}
	// Prove, before any ownership transfer, that the recorded names are distinct, that each one
	// was supplied, and that nothing else was: then list is exactly the offered set, so AdoptEpoch
	// consuming list consumes every offered descriptor.
	list := make([]int, len(state.Tracks))
	named := make(map[string]bool, len(state.Tracks))
	for i, id := range state.Tracks {
		if named[id.Name] {
			return nil, fmt.Errorf("%w: %s records descriptor name %q twice", ErrLostEpoch, ownerFile, id.Name)
		}
		named[id.Name] = true
		fd, ok := fds[id.Name]
		if !ok {
			return nil, fmt.Errorf("%w: no inherited descriptor named %q", ErrLostEpoch, id.Name)
		}
		list[i] = fd
	}
	if len(fds) != len(list) {
		return nil, fmt.Errorf("%w: %d inherited descriptors, %s records %d", ErrLostEpoch, len(fds), ownerFile, len(list))
	}
	consumed = true // AdoptEpoch owns the exact set from here, on every path
	pol := partstore.Policy{Window: time.Duration(state.Policy.WindowNS), MaxSegment: time.Duration(state.Policy.MaxSegmentNS)}
	store, err := partstore.AdoptEpoch(state.Run, state.Source, state.Epoch, state.Config, list, state.Tracks)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLostEpoch, err)
	}
	j, r, err := partstore.OpenJournal(dir, store, state.Run, state.Source, state.Epoch, state.Config, pol)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("%w: %v", ErrLostEpoch, err)
	}
	owner := WorkerOwner{UID: state.Worker.UID, PID: state.Worker.PID, Ino: state.Worker.Ino}
	s := newSource(dir, state.Run, state.Source, state.Epoch, state.Config, pol, state.Tracks, owner, store, j, r)
	if err := s.load(); err != nil {
		j.Close()
		store.Close()
		return nil, err
	}
	return s, nil
}

func newSource(dir, run, source string, epoch uint64, cfg ipc.Config, pol partstore.Policy, ids []ipc.FDIdentity,
	owner WorkerOwner, store *partstore.EpochStore, j *partstore.Journal, r *partstore.Ring) *Source {
	return &Source{dir: dir, run: run, source: source, epoch: epoch, cfg: cfg, policy: pol, ids: ids, owner: owner,
		store: store, journal: j, ring: r, changed: make(chan struct{}), pins: map[*RetentionPin]map[int]int{}}
}

// SourceInfo is the immutable description of a Source: its scope, configuration, effective
// retention policy and the kernel identities of its descriptors in track order.
type SourceInfo struct {
	Directory string
	Run       string
	Source    string
	Epoch     uint64
	Config    ipc.Config
	Policy    partstore.Policy
	Tracks    []ipc.FDIdentity
}

// Info returns an owned copy of the Source's description.
func (s *Source) Info() SourceInfo {
	return SourceInfo{Directory: s.dir, Run: s.run, Source: s.source, Epoch: s.epoch, Config: s.cfg, Policy: s.policy,
		Tracks: append([]ipc.FDIdentity(nil), s.ids...)}
}

// SourceObservation is one consistent view of a Source: an owned ring snapshot, a channel that is
// closed at the next observable change after it (a committed entry, maintenance, refusal or
// Close), and whether the Source was already closed or refusing. Snapshot is zero once the Source
// is closed.
type SourceObservation struct {
	Snapshot partstore.RingSnapshot
	Changed  <-chan struct{}
	Closed   bool
	Refusing bool
}

// Observe returns the current observation. The snapshot and the channel are taken together under
// the mutation lock, so every change after the snapshot closes the channel. No goroutine is
// started per waiter and nothing calls back into the caller.
func (s *Source) Observe() (SourceObservation, error) {
	s.op.Lock()
	defer s.op.Unlock()
	obs := SourceObservation{Changed: s.changed, Closed: s.isClosed(), Refusing: s.refusing.Load()}
	snap, err := s.ring.Snapshot()
	switch {
	case errors.Is(err, partstore.ErrClosed):
		obs.Closed = true
	case err != nil:
		return SourceObservation{}, err
	default:
		obs.Snapshot = snap
	}
	return obs, nil
}

// notify wakes every observer, under op.
func (s *Source) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// RetentionPin keeps segments retained: Maintain never drops a pinned track's segments at or
// above the pin's floor MSN. Pins of several holders merge by the minimum floor per track.
type RetentionPin struct {
	src *Source
}

// PinWindow installs a pin with floors, a map of track to the lowest segment MSN (positive) that
// must stay retained on that track; tracks absent from the map are unconstrained. An unconfigured
// track or a non-positive MSN is ErrInvalidOptions and installs nothing.
func (s *Source) PinWindow(floors map[int]int) (*RetentionPin, error) {
	s.op.Lock()
	defer s.op.Unlock()
	if err := s.checkFloors(floors); err != nil {
		return nil, err
	}
	p := &RetentionPin{src: s}
	s.pins[p] = copyFloors(floors)
	return p, nil
}

// Update replaces the pin's floors with the same validation as PinWindow; a closed pin is
// ErrInvalidOptions.
func (p *RetentionPin) Update(floors map[int]int) error {
	s := p.src
	s.op.Lock()
	defer s.op.Unlock()
	if _, ok := s.pins[p]; !ok {
		return fmt.Errorf("%w: pin is closed", ErrInvalidOptions)
	}
	if err := s.checkFloors(floors); err != nil {
		return err
	}
	s.pins[p] = copyFloors(floors)
	return nil
}

// Close removes the pin; a second Close changes nothing.
func (p *RetentionPin) Close() {
	s := p.src
	s.op.Lock()
	delete(s.pins, p)
	s.op.Unlock()
}

func (s *Source) checkFloors(floors map[int]int) error {
	for trk, msn := range floors {
		switch {
		case trk < 0 || trk >= s.cfg.Tracks:
			return fmt.Errorf("%w: pin on track %d, %d configured", ErrInvalidOptions, trk, s.cfg.Tracks)
		case msn <= 0:
			return fmt.Errorf("%w: pin floor %d on track %d is not positive", ErrInvalidOptions, msn, trk)
		}
	}
	return nil
}

func copyFloors(floors map[int]int) map[int]int {
	c := make(map[int]int, len(floors))
	for k, v := range floors {
		c[k] = v
	}
	return c
}

// floor returns the lowest pinned MSN of trk, or 0 when no pin constrains it. Under op.
func (s *Source) floor(trk int) int {
	f := 0
	for _, floors := range s.pins {
		if msn, ok := floors[trk]; ok && (f == 0 || msn < f) {
			f = msn
		}
	}
	return f
}

// load takes the session position from the ring: the entry high-water, whether any record exists
// and the single pending grant. More than one pending grant is not a history this Source wrote.
func (s *Source) load() error {
	snap, err := s.ring.Snapshot()
	if err != nil {
		return err
	}
	if len(snap.Pending) > 1 {
		return fmt.Errorf("%w: %d grants pending, this source keeps at most one", ErrLostEpoch, len(snap.Pending))
	}
	s.high, s.fresh = snap.EntryHigh, snap.RecordHigh == 0
	if len(snap.Pending) == 1 {
		g := snap.Pending[0]
		if s.high == math.MaxUint64 {
			return fmt.Errorf("%w: entry sequence exhausted with a grant pending", ErrLostEpoch)
		}
		s.pending = &g
	}
	return nil
}

// Snapshot returns an owned copy of the ring's retained state.
func (s *Source) Snapshot() (partstore.RingSnapshot, error) { return s.ring.Snapshot() }

// ReadInit returns a digest-checked copy of the track's init.
func (s *Source) ReadInit(trk int) ([]byte, error) { return s.ring.ReadInit(trk) }

// ReadPart returns a digest-checked copy of part index of segment msn.
func (s *Source) ReadPart(trk, msn, index int) ([]byte, error) {
	return s.ring.ReadPart(trk, msn, index)
}

// ReadSegment returns the digest-checked parts of closed segment msn concatenated.
func (s *Source) ReadSegment(trk, msn int) ([]byte, error) { return s.ring.ReadSegment(trk, msn) }

// Maintain retires what the window no longer needs, at monotonic time nowMonoNS: on every track it
// drops the oldest closed segments wholly outside the effective window, oldest first, retires the
// dropped segments whose deadline has passed, and compacts the journal when anything changed. It
// never evicts for a full track and never touches a grant. nowMonoNS must not precede any clock
// the ring has seen.
func (s *Source) Maintain(nowMonoNS int64) error {
	if nowMonoNS < 0 {
		return fmt.Errorf("%w: negative clock %d", ErrInvalidOptions, nowMonoNS)
	}
	s.op.Lock()
	defer s.op.Unlock()
	if s.refusing.Load() {
		return ErrRefusing
	}
	snap, err := s.ring.Snapshot()
	if err != nil {
		return err
	}
	changed := false
	defer func() {
		if changed || s.refusing.Load() {
			s.notify()
		}
	}()
	for _, t := range snap.Tracks {
		floor := s.floor(t.Track)
		for _, seg := range t.Segments {
			if seg.Dropped {
				continue
			}
			if !seg.Closed || seg.End > t.End-int64(s.policy.Window) || (floor != 0 && seg.MSN >= floor) {
				break
			}
			if err := s.ring.DropSegment(t.Track, seg.MSN, nowMonoNS); err != nil {
				return s.mutationErr(err)
			}
			changed = true
		}
		for _, seg := range t.Segments {
			if !seg.Dropped {
				break
			}
			if seg.DeadlineNS > nowMonoNS {
				continue
			}
			if err := s.ring.RetireSegment(t.Track, seg.MSN, nowMonoNS); err != nil {
				return s.mutationErr(err)
			}
			changed = true
		}
	}
	if !changed {
		return nil
	}
	s.fresh = false
	if err := s.journal.Compact(); err != nil {
		s.refusing.Store(true)
		return fmt.Errorf("%w: compact: %w", ErrRefusing, err)
	}
	return nil
}

// mutationErr maps a ring error after a failed mutation: an ambiguous outcome makes the Source
// refuse from now on, anything else is reported as it is.
func (s *Source) mutationErr(err error) error {
	if errors.Is(err, partstore.ErrPoisoned) {
		s.refusing.Store(true)
		s.notify()
		return fmt.Errorf("%w: %w", ErrRefusing, err)
	}
	return err
}

// Maintain's documented contract above is extended by pins: on every track the drop loop stops
// at the first segment at or above the merged pin floor (PinWindow), so a referenced segment is
// never dropped however old it is.

// Close stops admissions, closes the active connection and joins its ServeConn, then closes the
// journal (which disables the ring) and the store. The first caller performs the cleanup; every
// concurrent or later caller waits for it to finish and receives the same result. Grants,
// descriptors in the fd store and files on disk are left exactly as they are.
func (s *Source) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.done
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	s.done = make(chan struct{})
	active, done := s.active, s.done
	s.mu.Unlock()
	s.op.Lock()
	s.notify() // observers see Closed at once, before the session join
	s.op.Unlock()
	if active != nil {
		_ = active.conn.Close() // net.ErrClosed is benign: the session may already be ending
		<-active.done
	}
	s.op.Lock()
	err := errors.Join(s.journal.Close(), s.store.Close())
	s.op.Unlock()
	s.closeErr = err
	close(done)
	return err
}

// effectivePolicy applies the partstore defaults and refuses what the ring would refuse, so that
// the owner record names exactly the policy the journal will carry.
func effectivePolicy(p partstore.Policy) (partstore.Policy, error) {
	if p.Window == 0 {
		p.Window = partstore.DefaultWindow
	}
	if p.MaxSegment == 0 {
		p.MaxSegment = partstore.DefaultMaxSegment
	}
	switch {
	case p.Window < 0 || p.MaxSegment < 0:
		return partstore.Policy{}, fmt.Errorf("%w: policy window %v and max segment %v must be positive", ErrInvalidOptions, p.Window, p.MaxSegment)
	case p.MaxSegment > math.MaxInt64/3 || p.Window > math.MaxInt64-3*p.MaxSegment:
		return partstore.Policy{}, fmt.Errorf("%w: policy window %v and max segment %v overflow the grace period", ErrInvalidOptions, p.Window, p.MaxSegment)
	}
	return p, nil
}

// session is one admitted connection; done is closed when its ServeConn has returned.
type session struct {
	conn *net.UnixConn
	done chan struct{}
}
