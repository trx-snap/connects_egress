//go:build linux

package partstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

// segment is one media segment of a track: its parts in index order and its checked
// cumulative timing. start is the cumulative end of the previous segment.
type segment struct {
	msn       int
	start     int64
	duration  int64
	end       int64
	closed    bool
	dropped   bool
	droppedAt int64
	deadline  int64
	parts     []ipc.Entry
}

// track is one track's retained state. last and end survive retirement so that fragment
// continuity and the window are judged against the full published timeline.
type track struct {
	init     *ipc.Entry
	phases   *ipc.Entry
	pending  *Reservation
	segments []*segment // retained, MSN ascending, dropped included until retired
	last     *ipc.Entry // last committed part
	end      int64      // cumulative end of last
	cursor   int64      // next first-fit start
}

// Ring is the publication state of one source epoch over a borrowed, open EpochStore: which
// spans are granted, committed, live, dropped and free. It never closes the store, never
// frees or signals anything to the writer, and is the exclusive allocation owner of the
// epoch. All mutations are validated, persisted and then applied under one write lock; reads
// copy and digest-check under a read lock. The raw writable descriptor cannot stop a writer
// from touching committed bytes, so every returned copy is checked against its digest.
type Ring struct {
	mu       sync.RWMutex
	store    *EpochStore
	run      string
	source   string
	epoch    uint64
	cfg      ipc.Config
	policy   Policy
	persist  Persist
	closed   bool
	poisoned bool
	recSeq   uint64 // record high-water
	high     uint64 // entry high-water
	clock    int64  // latest publication or removal monotonic clock
	terminal *ipc.Entry
	retained map[uint64]ipc.Entry // retained entries by seq
	tracks   []*track
}

// NewRing opens a fresh ring over store, which the caller must have allocated or adopted for
// exactly run, source, epoch and cfg and must keep open while the ring is used. The store
// must be fresh: no record has yet been persisted for it. No record is written until the
// first mutation. persist is mandatory. This is not a journal: the caller owns durability.
func NewRing(store *EpochStore, run, source string, epoch uint64, cfg ipc.Config, policy Policy, persist Persist) (*Ring, error) {
	return RestoreRing(store, run, source, epoch, cfg, policy, nil, persist)
}

// RestoreRing rebuilds a ring over store from records, which must be the complete durable
// record history of that store in order; an empty history yields a fresh ring. Every record
// is replayed through the same validation as a live mutation without persisting, and the
// digest of every retained init and part is then checked against the store's bytes before
// the ring is exposed. Pending grants are restored as occupied; they are released only by
// CancelAfterQuiescence. On any error the borrowed store is untouched.
func RestoreRing(store *EpochStore, run, source string, epoch uint64, cfg ipc.Config, policy Policy, records []Record, persist Persist) (*Ring, error) {
	r, err := newRing(store, run, source, epoch, cfg, policy, persist)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		if err := r.mutate(rec, false); err != nil {
			return nil, fmt.Errorf("record %d: %w", rec.Seq, err)
		}
	}
	if err := r.verifyRetained(); err != nil {
		return nil, err
	}
	return r, nil
}

// newRing validates the scope, config, policy and persist of a ring and checks that store is
// exactly the epoch's store, then returns an empty ring over it. Nothing is read or written.
func newRing(store *EpochStore, run, source string, epoch uint64, cfg ipc.Config, policy Policy, persist Persist) (*Ring, error) {
	names, err := setup(run, source, epoch, cfg)
	if err != nil {
		return nil, err
	}
	pol, err := policy.effective()
	if err != nil {
		return nil, err
	}
	if persist == nil {
		return nil, fmt.Errorf("%w: persist is nil", ErrInvalidConfig)
	}
	ids, err := store.Identities()
	if err != nil {
		return nil, err
	}
	if len(ids) != len(names) {
		return nil, fmt.Errorf("%w: store has %d tracks, config %d", ErrIdentityMismatch, len(ids), len(names))
	}
	for i, id := range ids {
		if id.Track != i || id.Name != names[i] || id.Size != cfg.Capacity {
			return nil, fmt.Errorf("%w: store track %d is %q size %d, want %q size %d", ErrIdentityMismatch, i, id.Name, id.Size, names[i], cfg.Capacity)
		}
	}
	r := &Ring{store: store, run: run, source: source, epoch: epoch, cfg: cfg, policy: pol, persist: persist,
		retained: make(map[uint64]ipc.Entry), tracks: make([]*track, cfg.Tracks)}
	for i := range r.tracks {
		r.tracks[i] = &track{}
	}
	return r, nil
}

// verifyRetained digest-checks every retained init and part against the store's bytes.
func (r *Ring) verifyRetained() error {
	for _, t := range r.tracks {
		if t.init != nil {
			if _, err := r.checked(*t.init); err != nil {
				return err
			}
		}
		for _, seg := range t.segments {
			for _, p := range seg.parts {
				if _, err := r.checked(p); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Reserve grants the writer a contiguous extent of length bytes in track for one init or
// part. Init is pinned at offset 0 and granted once; a part needs the committed init. The
// extent is the first fit from the track's cursor, wrapping once, among the bytes not held by
// the init, a retained part or a pending grant; nothing is split or evicted. A second pending
// grant on the track is ErrReservation; no fit is ErrFull.
func (r *Ring) Reserve(kind string, trk int, length int64) (Reservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutable(); err != nil {
		return Reservation{}, err
	}
	res := Reservation{ID: r.recSeq + 1, Kind: kind, Track: trk, Length: length}
	offset, err := r.place(res)
	if err != nil {
		return Reservation{}, err
	}
	res.Offset = offset
	if err := r.mutate(r.record(OpReserve, res, ipc.Entry{}, trk, 0, 0), true); err != nil {
		return Reservation{}, err
	}
	return res, nil
}

// Commit publishes e, an init or part the writer has finished writing into exactly the
// pending grant res. The bytes are digest-checked before the commit record is persisted.
func (r *Ring) Commit(res Reservation, e ipc.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutable(); err != nil {
		return err
	}
	if err := r.matchPending(res); err != nil {
		return err
	}
	return r.mutate(r.record(OpCommit, res, e, e.Track, e.MSN, 0), true)
}

// Apply publishes a phases or terminal entry; new bytes never enter without a grant. An exact
// duplicate of a retained entry is a no-op, an old sequence no longer retained is
// ErrNotFound, and a different entry at a retained sequence is ErrInvalidPublication.
func (r *Ring) Apply(e ipc.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutable(); err != nil {
		return err
	}
	if e.Seq != 0 && e.Seq <= r.high {
		old, ok := r.retained[e.Seq]
		switch {
		case !ok:
			return fmt.Errorf("%w: entry seq %d is retired", ErrNotFound, e.Seq)
		case old != e:
			return fmt.Errorf("%w: entry seq %d conflicts with the retained entry", ErrInvalidPublication, e.Seq)
		}
		return nil
	}
	if e.Kind != ipc.KindPhases && e.Kind != ipc.KindTerminal {
		return fmt.Errorf("%w: apply of %q entry needs a grant", ErrInvalidPublication, e.Kind)
	}
	return r.mutate(r.record(OpMeta, Reservation{}, e, e.Track, 0, 0), true)
}

// CancelAfterQuiescence releases the pending grant res. The caller must already know the
// writer will never touch it again, from its acknowledgement or its death; the ring never
// infers that and never times a grant out.
func (r *Ring) CancelAfterQuiescence(res Reservation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutable(); err != nil {
		return err
	}
	if err := r.matchPending(res); err != nil {
		return err
	}
	return r.mutate(r.record(OpCancel, res, ipc.Entry{}, res.Track, 0, 0), true)
}

// DropSegment removes segment msn of track from live visibility at monotonic time
// nowMonoNS. It must be the track's oldest still-visible segment, closed and wholly outside
// the window, and nowMonoNS must not precede any publication or removal clock seen so far.
// Its bytes stay readable until RetireSegment.
func (r *Ring) DropSegment(trk, msn int, nowMonoNS int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutable(); err != nil {
		return err
	}
	return r.mutate(r.record(OpDrop, Reservation{}, ipc.Entry{}, trk, msn, nowMonoNS), true)
}

// RetireSegment frees the extents of a dropped segment once nowMonoNS has reached its
// deadline, removing all its parts at once. The init is never retired.
func (r *Ring) RetireSegment(trk, msn int, nowMonoNS int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutable(); err != nil {
		return err
	}
	return r.mutate(r.record(OpRetire, Reservation{}, ipc.Entry{}, trk, msn, nowMonoNS), true)
}

// ReadInit returns a digest-checked copy of the track's init.
func (r *Ring) ReadInit(trk int) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, err := r.readable(trk)
	if err != nil {
		return nil, err
	}
	if t.init == nil {
		return nil, fmt.Errorf("%w: track %d has no init", ErrNotFound, trk)
	}
	return r.checked(*t.init)
}

// ReadPart returns a digest-checked copy of part index of segment msn, dropped or not.
func (r *Ring) ReadPart(trk, msn, index int) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, err := r.readable(trk)
	if err != nil {
		return nil, err
	}
	seg := t.find(msn)
	if seg == nil {
		return nil, fmt.Errorf("%w: track %d segment %d", ErrNotFound, trk, msn)
	}
	if index < 0 || index >= len(seg.parts) {
		return nil, fmt.Errorf("%w: track %d segment %d part %d", ErrNotFound, trk, msn, index)
	}
	return r.checked(seg.parts[index])
}

// ReadSegment returns the digest-checked parts of closed segment msn concatenated in index
// order; the parts need not be contiguous in the track. An open segment is ErrNotReady.
func (r *Ring) ReadSegment(trk, msn int) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, err := r.readable(trk)
	if err != nil {
		return nil, err
	}
	seg := t.find(msn)
	if seg == nil {
		return nil, fmt.Errorf("%w: track %d segment %d", ErrNotFound, trk, msn)
	}
	if !seg.closed {
		return nil, fmt.Errorf("%w: track %d segment %d is open", ErrNotReady, trk, msn)
	}
	var out []byte
	for _, p := range seg.parts {
		b, err := r.checked(p)
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	return out, nil
}

// Snapshot returns an owned deep copy of the retained state, dropped segments included.
func (r *Ring) Snapshot() (RingSnapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return RingSnapshot{}, ErrClosed
	}
	s := RingSnapshot{RecordHigh: r.recSeq, EntryHigh: r.high, Terminal: copyEntry(r.terminal)}
	for i, t := range r.tracks {
		if t.pending != nil {
			s.Pending = append(s.Pending, *t.pending)
		}
		ts := TrackSnapshot{Track: i, Init: copyEntry(t.init), Phases: copyEntry(t.phases), End: t.end}
		for _, seg := range t.segments {
			ts.Segments = append(ts.Segments, SegmentSnapshot{MSN: seg.msn, Duration: seg.duration, End: seg.end,
				Closed: seg.closed, Dropped: seg.dropped, DroppedAtNS: seg.droppedAt, DeadlineNS: seg.deadline,
				Parts: append([]ipc.Entry(nil), seg.parts...)})
		}
		s.Tracks = append(s.Tracks, ts)
	}
	return s, nil
}

// Close disables every operation of the ring. It is idempotent and frees nothing: the store,
// its bytes and any pending grant stay as they are, and the writer is not signalled. A later
// owner must restore from the complete record history before reusing the store.
func (r *Ring) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

// mutable reports whether a mutation may proceed.
func (r *Ring) mutable() error {
	switch {
	case r.closed:
		return ErrClosed
	case r.poisoned:
		return ErrPoisoned
	}
	return nil
}

// readable returns the track for a read.
func (r *Ring) readable(trk int) (*track, error) {
	if r.closed {
		return nil, ErrClosed
	}
	if trk < 0 || trk >= len(r.tracks) {
		return nil, fmt.Errorf("%w: track %d", ErrNotFound, trk)
	}
	return r.tracks[trk], nil
}

// record builds the next live record of the ring.
func (r *Ring) record(op string, res Reservation, e ipc.Entry, trk, msn int, at int64) Record {
	return Record{Version: RecordVersion, Run: r.run, Source: r.source, Epoch: r.epoch, Seq: r.recSeq + 1,
		Config: r.cfg, Policy: r.policy, Op: op, Reservation: res, Entry: e, Track: trk, MSN: msn, AtNS: at}
}

// matchPending requires res to be exactly the pending grant of its track.
func (r *Ring) matchPending(res Reservation) error {
	if res.Track < 0 || res.Track >= len(r.tracks) {
		return fmt.Errorf("%w: track %d", ErrReservation, res.Track)
	}
	p := r.tracks[res.Track].pending
	if p == nil || *p != res {
		return fmt.Errorf("%w: grant %d is not the pending grant of track %d", ErrReservation, res.ID, res.Track)
	}
	return nil
}

// checked copies the span of e from the store and verifies its digest.
func (r *Ring) checked(e ipc.Entry) ([]byte, error) {
	b, err := r.store.ReadSpan(e.Track, e.Offset, e.Length)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != e.SHA256 {
		return nil, fmt.Errorf("%w: track %d span %d+%d digest mismatch", ErrInvalidPublication, e.Track, e.Offset, e.Length)
	}
	return b, nil
}

// place validates a reserve request and returns the offset it would be granted.
func (r *Ring) place(res Reservation) (int64, error) {
	if res.Track < 0 || res.Track >= len(r.tracks) {
		return 0, fmt.Errorf("%w: track %d", ErrInvalidPublication, res.Track)
	}
	t := r.tracks[res.Track]
	switch {
	case r.terminal != nil:
		return 0, fmt.Errorf("%w: no grant after terminal", ErrInvalidPublication)
	case t.pending != nil:
		return 0, fmt.Errorf("%w: track %d already has grant %d pending", ErrReservation, res.Track, t.pending.ID)
	case res.Length <= 0:
		return 0, fmt.Errorf("%w: length %d", ErrInvalidPublication, res.Length)
	case res.Length > r.cfg.Capacity:
		return 0, fmt.Errorf("%w: length %d exceeds capacity %d", ErrFull, res.Length, r.cfg.Capacity)
	}
	switch res.Kind {
	case ipc.KindInit:
		if t.init != nil {
			return 0, fmt.Errorf("%w: track %d init already committed", ErrInvalidPublication, res.Track)
		}
		return 0, nil
	case ipc.KindPart:
		if t.init == nil {
			return 0, fmt.Errorf("%w: track %d has no init", ErrInvalidPublication, res.Track)
		}
	default:
		return 0, fmt.Errorf("%w: cannot reserve %q", ErrInvalidPublication, res.Kind)
	}
	occ := t.occupied()
	for _, from := range []int64{t.cursor, 0} {
		if off, ok := fit(occ, from, r.cfg.Capacity, res.Length); ok {
			return off, nil
		}
	}
	return 0, fmt.Errorf("%w: no %d contiguous free bytes in track %d", ErrFull, res.Length, res.Track)
}

type extent struct{ off, end int64 }

// occupied lists the track's held extents sorted by offset.
func (t *track) occupied() []extent {
	var occ []extent
	if t.init != nil {
		occ = append(occ, extent{t.init.Offset, t.init.Offset + t.init.Length})
	}
	for _, seg := range t.segments {
		for _, p := range seg.parts {
			occ = append(occ, extent{p.Offset, p.Offset + p.Length})
		}
	}
	if t.pending != nil {
		occ = append(occ, extent{t.pending.Offset, t.pending.Offset + t.pending.Length})
	}
	sort.Slice(occ, func(i, j int) bool { return occ[i].off < occ[j].off })
	return occ
}

// fit returns the first start at or after from where length bytes are free below capacity.
func fit(occ []extent, from, capacity, length int64) (int64, bool) {
	start := from
	for start <= capacity-length {
		blocked := false
		for _, x := range occ {
			if x.off < start+length && start < x.end {
				start, blocked = x.end, true
				break
			}
		}
		if !blocked {
			return start, true
		}
	}
	return 0, false
}

// find returns the retained segment msn, or nil.
func (t *track) find(msn int) *segment {
	for _, seg := range t.segments {
		if seg.msn == msn {
			return seg
		}
	}
	return nil
}

func copyEntry(e *ipc.Entry) *ipc.Entry {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}
