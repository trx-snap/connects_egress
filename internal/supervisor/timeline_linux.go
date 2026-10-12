//go:build linux

package supervisor

// Timeline: the durable room playback timeline (plan §5.3, §9.5). It maps the segments of borrowed
// Sources onto one continuous room numbering, exposes only what is durable in the Source and in
// its own journal, pins what it references so Source maintenance never drops it, and performs
// source switches at segment boundaries of the incoming source. Every mutation runs under one room
// lock: the journal record is synced first, then the immutable view is replaced and observers are
// woken. Nothing here starts a goroutine or calls back into a caller; the controller drives it
// with Refresh and the Sources' own change channels.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
)

// EpochResolver returns the Source of key, borrowed: the timeline never closes or signals it. An
// epoch whose history is gone is ErrLostEpoch; any other error is fatal to the caller.
type EpochResolver func(EpochKey) (*Source, error)

// genRuntime is the process-local state of one referenced generation.
type genRuntime struct {
	src  *Source
	pin  *RetentionPin
	segs map[uint64]RoomSegment // exposed segments by room MSN
}

// Timeline is one room's timeline. See the package comment of this file.
type Timeline struct {
	opts     TimelineOptions
	resolver EpochResolver
	boot     string

	mu         sync.Mutex // the room lock
	st         roomState
	j          *roomJournal
	gens       map[uint64]*genRuntime
	cutReached bool
	view       TimelineSnapshot
	changed    chan struct{}
	poison     error
	closed     bool
}

// OpenTimeline opens or creates the room's journal in opts.Directory, replays it, resolves every
// referenced epoch, trims the head through any lost epoch, reinstalls retention pins before any
// maintenance can run and resumes a pending switch. A corrupt journal, a scope or identity
// mismatch or a resolver error other than ErrLostEpoch is fatal.
func OpenTimeline(ctx context.Context, opts TimelineOptions, resolver EpochResolver) (*Timeline, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resolver == nil {
		return nil, fmt.Errorf("%w: nil resolver", ErrTimelineInvalid)
	}
	if err := checkRoomID(opts.Room); err != nil {
		return nil, err
	}
	if opts.Window == 0 {
		opts.Window = DefaultRoomWindow
	}
	if opts.Window < 0 {
		return nil, fmt.Errorf("%w: window %v is negative", ErrTimelineInvalid, opts.Window)
	}
	boot, err := currentBootID()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTimelineInvalid, err)
	}
	fresh := roomState{Version: stateVersion, Room: opts.Room, BootID: boot, WindowNS: int64(opts.Window), NextGeneration: 1}
	j, st, err := openRoomJournal(opts.Directory, fresh)
	if err != nil {
		return nil, err
	}
	t := &Timeline{opts: opts, resolver: resolver, boot: boot, st: st, j: j, gens: map[uint64]*genRuntime{}, changed: make(chan struct{})}
	if st.WindowNS != int64(opts.Window) {
		j.close()
		return nil, fmt.Errorf("%w: journal window %dns, options say %v", ErrTimelineInvalid, st.WindowNS, opts.Window)
	}
	if st.BootID != boot {
		// Another boot: leases and holds expire, counters never do.
		t.st.BootID, t.st.Grace = boot, nil
		if t.st.Pending != nil {
			t.st.Pending.HoldStartMonoNS, t.st.Pending.HoldBootID = 0, ""
		}
		if err := j.rewrite(t.st); err != nil {
			j.close()
			return nil, err
		}
	}
	if err := t.recover(); err != nil {
		t.releasePins()
		j.close()
		return nil, err
	}
	t.publish()
	return t, nil
}

// checkRoomID requires a nonempty valid UTF-8 room identity without control characters.
func checkRoomID(room string) error {
	if room == "" || len(room) > ipc.MaxPresentationIdentity || !utf8.ValidString(room) {
		return fmt.Errorf("%w: room identity is empty, too long or not UTF-8", ErrTimelineInvalid)
	}
	for _, r := range room {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: room identity contains a control character", ErrTimelineInvalid)
		}
	}
	return nil
}

// recover resolves the referenced epochs, trims the head through lost ones, resumes the pending
// switch, rebuilds exposure and installs pins.
func (t *Timeline) recover() error {
	var lost uint64
	for i := range t.st.Bindings {
		b := &t.st.Bindings[i]
		src, err := t.resolve(*b)
		switch {
		case errors.Is(err, ErrLostEpoch):
			lost = max(lost, b.Generation)
		case err != nil:
			return err
		default:
			t.gens[b.Generation] = &genRuntime{src: src, segs: map[uint64]RoomSegment{}}
		}
	}
	if lost > 0 {
		head := t.st.LastAdvance + 1
		for _, s := range t.st.Segments {
			if s.Generation > lost {
				head = s.RoomMSN
				break
			}
		}
		dsn := t.st.DSN
		for _, s := range t.st.Segments {
			if s.RoomMSN < head && s.Discontinuity {
				dsn++
			}
		}
		w := windowRecord{Head: head, DSN: dsn}
		for _, g := range t.st.Grace {
			if g.Generation > lost {
				w.Grace = append(w.Grace, g)
			}
		}
		for _, b := range t.st.Bindings {
			if b.Generation <= lost {
				w.RemoveGenerations = append(w.RemoveGenerations, b.Generation)
				delete(t.gens, b.Generation)
			}
		}
		if err := t.commit(roomRecord{Op: opWindow, Window: &w}); err != nil {
			return err
		}
	}
	if p := t.st.Pending; p != nil {
		src, err := t.resolve(p.Incoming)
		switch {
		case errors.Is(err, ErrLostEpoch):
			if err := t.commit(roomRecord{Op: opCancel}); err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			t.gens[p.Incoming.Generation] = &genRuntime{src: src, segs: map[uint64]RoomSegment{}}
		}
	}
	if err := t.repin(); err != nil {
		return err
	}
	for i := range t.st.Bindings {
		if b := &t.st.Bindings[i]; b.Frozen() {
			t.rebuildFrozen(b)
		}
	}
	return t.exposeLive()
}

// resolve asks the resolver for b's epoch and verifies that it is exactly the epoch b was bound
// with: scope, configuration and descriptor identities.
func (t *Timeline) resolve(b RoomBinding) (*Source, error) {
	src, err := t.resolver(b.Key)
	if err != nil {
		if errors.Is(err, ErrLostEpoch) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: resolve %+v: %v", ErrTimelineInvalid, b.Key, err)
	}
	if src == nil {
		return nil, fmt.Errorf("%w: resolver returned no source for %+v", ErrTimelineInvalid, b.Key)
	}
	info := src.Info()
	if info.Run != b.Key.Run || info.Source != b.Key.Source || info.Epoch != b.Key.Epoch ||
		info.Config != b.Config || !slices.Equal(info.Tracks, b.Tracks) {
		return nil, fmt.Errorf("%w: source %+v does not match its binding", ErrTimelineInvalid, b.Key)
	}
	return src, nil
}

// Bind binds the room's first generation (or its first after a lost epoch) to key, mapping the
// source's startMSN to the next room MSN. The presentation and every room track's init must exist.
func (t *Timeline) Bind(key EpochKey, startMSN int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.mutable(); err != nil {
		return err
	}
	switch {
	case t.st.live() != nil:
		return fmt.Errorf("%w: room is bound to generation %d; use PlanSwitch", ErrTimelineInvalid, t.st.live().Generation)
	case t.st.Pending != nil:
		return fmt.Errorf("%w: a switch is pending", ErrTimelineInvalid)
	case t.st.Ended:
		return fmt.Errorf("%w: room has ended", ErrTimelineInvalid)
	}
	b, src, err := t.prepareBinding(key, startMSN)
	if err != nil {
		return err
	}
	inits, err := t.inits(src, b)
	if err != nil {
		return err
	}
	b.Inits, b.FirstRoomMSN, b.Discontinuity = inits, t.st.LastAdvance+1, t.st.LastAdvance > 0
	t.gens[b.Generation] = &genRuntime{src: src, segs: map[uint64]RoomSegment{}}
	if err := t.commit(roomRecord{Op: opBind, Bind: &b}); err != nil {
		delete(t.gens, b.Generation)
		return err
	}
	defer t.publish()
	if err := t.repin(); err != nil {
		return err
	}
	return t.exposeLive()
}

// prepareBinding resolves key, loads its presentation, finds the room's slot and builds the
// binding of the next generation, without inits or room numbering.
func (t *Timeline) prepareBinding(key EpochKey, startMSN int) (RoomBinding, *Source, error) {
	if startMSN < 1 {
		return RoomBinding{}, nil, fmt.Errorf("%w: start msn %d is below 1", ErrTimelineInvalid, startMSN)
	}
	src, err := t.resolver(key)
	if err != nil {
		return RoomBinding{}, nil, fmt.Errorf("%w: resolve %+v: %v", ErrTimelineInvalid, key, err)
	}
	if src == nil {
		return RoomBinding{}, nil, fmt.Errorf("%w: resolver returned no source for %+v", ErrTimelineInvalid, key)
	}
	info := src.Info()
	if info.Run != key.Run || info.Source != key.Source || info.Epoch != key.Epoch {
		return RoomBinding{}, nil, fmt.Errorf("%w: resolver returned another epoch for %+v", ErrTimelineInvalid, key)
	}
	pres, err := LoadPresentation(src)
	if err != nil {
		return RoomBinding{}, nil, err
	}
	slot := slices.IndexFunc(pres.Slots, func(s ipc.PresentationSlot) bool { return s.RoomID == t.opts.Room })
	if slot < 0 {
		return RoomBinding{}, nil, fmt.Errorf("%w: presentation of %+v has no slot for room %q", ErrTimelineInvalid, key, t.opts.Room)
	}
	b := RoomBinding{Generation: t.st.NextGeneration, Key: key, Config: info.Config,
		WindowNS: int64(info.Policy.Window), MaxSegmentNS: int64(info.Policy.MaxSegment), Tracks: info.Tracks,
		Presentation: pres, Slot: slot, AudioTrack: ipc.TrackAudioA + slot, StartMSN: startMSN}
	return b, src, nil
}

// inits returns the init of every room track of b from src, or ErrTimelineNotReady.
func (t *Timeline) inits(src *Source, b RoomBinding) ([]ipc.Entry, error) {
	obs, err := src.Observe()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTimelineInvalid, err)
	}
	if obs.Closed {
		return nil, fmt.Errorf("%w: source %+v is closed", ErrTimelineInvalid, b.Key)
	}
	inits := make([]ipc.Entry, RoomTracks)
	for i := range inits {
		trk := b.SourceTrack(i)
		if trk >= len(obs.Snapshot.Tracks) || obs.Snapshot.Tracks[trk].Init == nil {
			return nil, fmt.Errorf("%w: track %d of %+v has no init", ErrTimelineNotReady, trk, b.Key)
		}
		inits[i] = *obs.Snapshot.Tracks[trk].Init
	}
	return inits, nil
}

// PlanSwitch plans a switch of the live generation to key at the incoming source's startMSN. The
// cut is the incoming video anchor plus SegmentNominal per segment before startMSN; a cut behind
// media the room has already exposed is refused. The plan is durable before it takes effect.
func (t *Timeline) PlanSwitch(key EpochKey, startMSN int, nowMonoNS int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.mutable(); err != nil {
		return err
	}
	live := t.st.live()
	switch {
	case nowMonoNS < 0:
		return fmt.Errorf("%w: negative clock %d", ErrTimelineInvalid, nowMonoNS)
	case live == nil:
		return fmt.Errorf("%w: room is not bound; use Bind", ErrTimelineInvalid)
	case t.st.Pending != nil:
		return fmt.Errorf("%w: a switch is already pending", ErrTimelineInvalid)
	case t.st.Ended:
		return fmt.Errorf("%w: room has ended", ErrTimelineInvalid)
	case live.Key == key:
		return fmt.Errorf("%w: %+v is already live", ErrTimelineInvalid, key)
	}
	b, src, err := t.prepareBinding(key, startMSN)
	if err != nil {
		return err
	}
	anchor, err := ipc.PresentationUTC(b.Presentation, ipc.TrackVideoHi, b.Presentation.Clocks[ipc.TrackVideoHi].RawPTSOriginNS)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTimelineInvalid, err)
	}
	span := int64(SegmentNominal) * int64(startMSN-1)
	if int64(startMSN-1) > math.MaxInt64/int64(SegmentNominal) || anchor > math.MaxInt64-span {
		return fmt.Errorf("%w: cut time overflows", ErrTimelineInvalid)
	}
	cut := anchor + span
	for i, end := range t.ends(live) {
		if end.utc > cut {
			return fmt.Errorf("%w: cut %d rewinds behind exposed end %d of room track %d", ErrTimelineInvalid, cut, end.utc, i)
		}
	}
	b.Discontinuity = true
	t.gens[b.Generation] = &genRuntime{src: src, segs: map[uint64]RoomSegment{}}
	if err := t.commit(roomRecord{Op: opRequest, AtMonoNS: nowMonoNS, Request: &PendingSwitch{Incoming: b, CutUTCNS: cut}}); err != nil {
		delete(t.gens, b.Generation)
		return err
	}
	defer t.publish()
	return t.refresh(nowMonoNS, true)
}

// CancelSwitch cancels the pending switch; the outgoing generation resumes.
func (t *Timeline) CancelSwitch() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.mutable(); err != nil {
		return err
	}
	if t.st.Pending == nil {
		return fmt.Errorf("%w: no switch is pending", ErrTimelineInvalid)
	}
	defer t.publish()
	return t.cancelPending()
}

// cancelPending persists the cancellation and releases the incoming generation.
func (t *Timeline) cancelPending() error {
	gen := t.st.Pending.Incoming.Generation
	if err := t.commit(roomRecord{Op: opCancel}); err != nil {
		return err
	}
	if rt := t.gens[gen]; rt != nil && rt.pin != nil {
		rt.pin.Close()
	}
	delete(t.gens, gen)
	t.cutReached = false
	return t.exposeLive()
}

// Refresh advances exposure from the Sources, runs the pending switch (hold, commit or cancel at
// the hold limit) and rolls the window, at monotonic time nowMonoNS.
func (t *Timeline) Refresh(nowMonoNS int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.mutable(); err != nil {
		return err
	}
	if nowMonoNS < 0 {
		return fmt.Errorf("%w: negative clock %d", ErrTimelineInvalid, nowMonoNS)
	}
	defer t.publish()
	return t.refresh(nowMonoNS, true)
}

// FailSource reports that key's source failed: a live generation freezes at exactly its exposed
// prefix (never an end), a pending incoming is cancelled, a referenced old generation keeps its
// bytes.
func (t *Timeline) FailSource(key EpochKey, reason string, nowMonoNS int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.mutable(); err != nil {
		return err
	}
	if nowMonoNS < 0 {
		return fmt.Errorf("%w: negative clock %d", ErrTimelineInvalid, nowMonoNS)
	}
	defer t.publish()
	if live := t.st.live(); live != nil && live.Key == key {
		if live.Frozen() {
			return nil
		}
		if err := t.freeze(live, true, reason); err != nil {
			return err
		}
		return t.refresh(nowMonoNS, true)
	}
	if p := t.st.Pending; p != nil && p.Incoming.Key == key {
		return t.cancelPending()
	}
	for _, b := range t.st.Bindings {
		if b.Key == key {
			return nil
		}
	}
	return fmt.Errorf("%w: %+v is not referenced by this room", ErrTimelineInvalid, key)
}

// End ends the room: the live generation freezes at its exposed prefix and the playlists become
// final. A pending switch must be cancelled or committed first.
func (t *Timeline) End() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.mutable(); err != nil {
		return err
	}
	live := t.st.live()
	switch {
	case live == nil:
		return fmt.Errorf("%w: room is not bound", ErrTimelineInvalid)
	case t.st.Pending != nil:
		return fmt.Errorf("%w: a switch is pending", ErrTimelineInvalid)
	case t.st.Ended:
		return nil
	}
	defer t.publish()
	if !live.Frozen() {
		if err := t.freeze(live, false, ""); err != nil {
			return err
		}
	}
	return t.commit(roomRecord{Op: opEnd})
}

// Observe returns the current view and a channel closed at the next change.
func (t *Timeline) Observe() (TimelineSnapshot, <-chan struct{}, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return t.copyView(), t.changed, ErrTimelineClosed
	}
	return t.copyView(), t.changed, nil
}

// References returns the epochs whose bytes this room still references at monotonic time
// nowMonoNS: the live and pending generations, every generation with a segment in the window and
// every generation whose removed segments are still within their grace. Only an epoch absent from
// this list may be closed by the controller.
func (t *Timeline) References(nowMonoNS int64) []EpochKey {
	t.mu.Lock()
	defer t.mu.Unlock()
	var keys []EpochKey
	add := func(k EpochKey) {
		if !slices.Contains(keys, k) {
			keys = append(keys, k)
		}
	}
	if live := t.st.live(); live != nil {
		add(live.Key)
	}
	if p := t.st.Pending; p != nil {
		add(p.Incoming.Key)
	}
	for _, s := range t.st.Segments {
		if b := t.st.binding(s.Generation); b != nil {
			add(b.Key)
		}
	}
	for _, g := range t.st.Grace {
		if g.ReleaseMonoNS > nowMonoNS {
			add(g.Key)
		}
	}
	return keys
}

// Close releases the timeline's own pins, journal and lock and wakes observers. It joins no
// worker and ends no room; every caller shares the result.
func (t *Timeline) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	t.releasePins()
	t.j.close()
	t.publish()
	return nil
}

func (t *Timeline) releasePins() {
	for _, rt := range t.gens {
		if rt.pin != nil {
			rt.pin.Close()
			rt.pin = nil
		}
	}
}

// mutable reports whether a mutation may proceed.
func (t *Timeline) mutable() error {
	switch {
	case t.closed:
		return ErrTimelineClosed
	case t.poison != nil:
		return fmt.Errorf("%w: %v", ErrTimelineRefusing, t.poison)
	}
	return nil
}

// commit makes rec durable, applies it and compacts when due. An error poisons the timeline.
func (t *Timeline) commit(rec roomRecord) error {
	if t.poison != nil {
		return fmt.Errorf("%w: %v", ErrTimelineRefusing, t.poison)
	}
	rec.Version, rec.Room, rec.Seq = stateVersion, t.opts.Room, t.st.RecordHigh+1
	if rec.Op == opHold {
		rec.BootID = t.boot
	}
	if err := t.j.append(rec); err != nil {
		t.poison = err
		return err
	}
	if err := t.st.apply(rec); err != nil {
		t.poison = err
		return fmt.Errorf("%w: %v", ErrTimelineRefusing, err)
	}
	if t.j.count >= compactAfter {
		if err := t.j.rewrite(t.st); err != nil {
			t.poison = err
			return err
		}
	}
	return nil
}

// refresh is the body of Refresh under the room lock; timed is false when no clock is known.
func (t *Timeline) refresh(now int64, timed bool) error {
	if err := t.exposeLive(); err != nil {
		return err
	}
	if err := t.switching(now, timed); err != nil {
		return err
	}
	if timed {
		if err := t.roll(now); err != nil {
			return err
		}
	}
	return t.repin()
}

// snapshotOf returns the ring snapshot of generation gen, false when it cannot be read.
func (t *Timeline) snapshotOf(gen uint64) (partstore.RingSnapshot, bool) {
	rt := t.gens[gen]
	if rt == nil || rt.src == nil {
		return partstore.RingSnapshot{}, false
	}
	obs, err := rt.src.Observe()
	if err != nil || obs.Closed {
		return partstore.RingSnapshot{}, false
	}
	return obs.Snapshot, true
}

// exposeLive advances the live generation's exposure: every allocated segment's prefix is
// recomputed from the Source and new segments are allocated durably before their first part is
// exposed. Parts ending beyond a pending cut are withheld.
func (t *Timeline) exposeLive() error {
	t.cutReached = false
	b := t.st.live()
	if b == nil || b.Frozen() {
		t.cutReached = b != nil
		return nil
	}
	rt := t.gens[b.Generation]
	snap, ok := t.snapshotOf(b.Generation)
	if !ok {
		return nil
	}
	if snap.Terminal != nil && !snap.Terminal.Verified {
		return t.freeze(b, true, "terminal: "+snap.Terminal.Reason)
	}
	var cut *int64
	if t.st.Pending != nil {
		cut = &t.st.Pending.CutUTCNS
	}
	next := b.StartMSN
	for _, s := range t.st.Segments {
		if s.Generation != b.Generation {
			continue
		}
		seg, ready, err := t.build(b, snap, s, cut)
		if err != nil {
			return t.freeze(b, true, err.Error())
		}
		if !ready {
			return t.freeze(b, true, fmt.Sprintf("segment %d vanished from the source", s.SourceMSN))
		}
		rt.segs[s.RoomMSN] = seg
		next = s.SourceMSN + 1
	}
	for {
		cand := segState{RoomMSN: t.st.LastAdvance + 1, Generation: b.Generation, SourceMSN: next,
			Discontinuity: b.Discontinuity && next == b.StartMSN}
		seg, ready, err := t.build(b, snap, cand, cut)
		if err != nil {
			return t.freeze(b, true, err.Error())
		}
		if !ready {
			return nil
		}
		if err := t.commit(roomRecord{Op: opAdvance, Advance: &cand}); err != nil {
			return err
		}
		rt.segs[cand.RoomMSN] = seg
		next++
	}
}

// build computes the exposed prefix of room segment s of b from snap. ready is false when the
// segment cannot be exposed yet: a selected track lacks its first part, or the segment opens the
// generation without an aligned IDR start. cut, when set, withholds parts ending after it.
func (t *Timeline) build(b *RoomBinding, snap partstore.RingSnapshot, s segState, cut *int64) (RoomSegment, bool, error) {
	seg := RoomSegment{MSN: s.RoomMSN, Generation: b.Generation, Key: b.Key, SourceMSN: s.SourceMSN, Discontinuity: s.Discontinuity}
	var video [3]*partstore.SegmentSnapshot
	for v := range video {
		video[v] = findSegment(snap, v, s.SourceMSN)
		if video[v] == nil {
			return seg, false, nil
		}
	}
	n := len(video[0].Parts)
	for v := 1; v < 3; v++ {
		n = min(n, len(video[v].Parts))
	}
	for i := 0; i < n; i++ {
		for v := 1; v < 3; v++ {
			if !samePart(video[0].Parts[i], video[v].Parts[i]) {
				return seg, false, fmt.Errorf("video tracks diverge at segment %d part %d", s.SourceMSN, i)
			}
		}
	}
	closed := true
	for _, vs := range video {
		closed = closed && vs.Closed
	}
	if closed {
		for _, vs := range video {
			if len(vs.Parts) != n {
				return seg, false, fmt.Errorf("video tracks diverge in length at segment %d", s.SourceMSN)
			}
		}
	}
	audio := findSegment(snap, b.AudioTrack, s.SourceMSN)
	if n == 0 || audio == nil || len(audio.Parts) == 0 {
		return seg, false, nil
	}
	if s.SourceMSN == b.StartMSN && !(video[0].Parts[0].Independent && video[0].Parts[0].FragmentStart) {
		return seg, false, fmt.Errorf("segment %d does not start with an independent fragment", s.SourceMSN)
	}
	for i := 0; i < RoomTracks; i++ {
		parts, isClosed := audio.Parts, audio.Closed
		if i < 3 {
			parts, isClosed = video[i].Parts[:n], closed
		}
		rtk, ready, err := t.track(b, b.SourceTrack(i), parts, isClosed, cut)
		if err != nil {
			return seg, false, err
		}
		if !ready {
			t.cutReached = true
			return seg, false, nil
		}
		seg.Tracks[i] = rtk
	}
	return seg, true, nil
}

// track builds one room track from parts, withholding parts ending after cut.
func (t *Timeline) track(b *RoomBinding, trk int, parts []ipc.Entry, closed bool, cut *int64) (RoomTrack, bool, error) {
	rt := RoomTrack{Track: trk, Last: -1}
	for _, e := range parts {
		utc, err := ipc.PresentationUTC(b.Presentation, trk, e.PTS)
		if err != nil {
			return rt, false, err
		}
		if utc > math.MaxInt64-e.Duration {
			return rt, false, fmt.Errorf("part end overflows")
		}
		end := utc + e.Duration
		if cut != nil && end > *cut {
			t.cutReached = true
			closed = false
			break
		}
		if len(rt.Parts) == 0 {
			rt.UTCNS = utc
		}
		rt.Parts = append(rt.Parts, e)
		rt.EndUTCNS, rt.DurationNS = end, rt.DurationNS+e.Duration
	}
	if len(rt.Parts) == 0 {
		return rt, false, nil
	}
	if closed {
		rt.Closed, rt.Last = true, len(rt.Parts)-1
	}
	return rt, true, nil
}

// findSegment returns segment msn of track trk in snap, dropped or not, nil when absent.
func findSegment(snap partstore.RingSnapshot, trk, msn int) *partstore.SegmentSnapshot {
	if trk < 0 || trk >= len(snap.Tracks) {
		return nil
	}
	for i := range snap.Tracks[trk].Segments {
		if s := &snap.Tracks[trk].Segments[i]; s.MSN == msn {
			return s
		}
	}
	return nil
}

// samePart reports whether two video parts of different rungs describe the same media interval.
func samePart(a, b ipc.Entry) bool {
	return a.Index == b.Index && a.PTS == b.PTS && a.DTS == b.DTS && a.Duration == b.Duration && a.Sequence == b.Sequence &&
		a.FirstAU == b.FirstAU && a.Payload == b.Payload && a.Independent == b.Independent && a.FragmentStart == b.FragmentStart
}

// trackEnd is the exposed end of one room track of a generation.
type trackEnd struct {
	ref PartRef
	utc int64
}

// ends returns, per room track, the last exposed part of b and its end UTC.
func (t *Timeline) ends(b *RoomBinding) [RoomTracks]trackEnd {
	var out [RoomTracks]trackEnd
	rt := t.gens[b.Generation]
	if rt == nil {
		return out
	}
	for _, s := range t.st.Segments {
		seg, ok := rt.segs[s.RoomMSN]
		if !ok || s.Generation != b.Generation {
			continue
		}
		for i := range out {
			if n := len(seg.Tracks[i].Parts); n > 0 {
				out[i] = trackEnd{ref: PartRef{MSN: s.SourceMSN, Index: n - 1}, utc: seg.Tracks[i].EndUTCNS}
			}
		}
	}
	return out
}

// freeze persists b's exact exposed prefix as its final exposure.
func (t *Timeline) freeze(b *RoomBinding, failed bool, reason string) error {
	ends := make([]PartRef, RoomTracks)
	for i, e := range t.ends(b) {
		ends[i] = e.ref
	}
	gen := b.Generation
	if err := t.commit(roomRecord{Op: opCut, Cut: &cutRecord{Generation: gen, Ends: ends, Failed: failed, Reason: reason}}); err != nil {
		return err
	}
	if rt := t.gens[gen]; rt != nil {
		for msn, seg := range rt.segs {
			for i := range seg.Tracks {
				seg.Tracks[i].Closed, seg.Tracks[i].Last = true, len(seg.Tracks[i].Parts)-1
			}
			rt.segs[msn] = seg
		}
	}
	t.cutReached = true
	return nil
}

// rebuildFrozen recomputes the exposure of a frozen generation from its Source at exactly Ends.
func (t *Timeline) rebuildFrozen(b *RoomBinding) {
	rt := t.gens[b.Generation]
	if rt == nil {
		return
	}
	snap, ok := t.snapshotOf(b.Generation)
	if !ok {
		return
	}
	for _, s := range t.st.Segments {
		if s.Generation != b.Generation {
			continue
		}
		seg := RoomSegment{MSN: s.RoomMSN, Generation: b.Generation, Key: b.Key, SourceMSN: s.SourceMSN, Discontinuity: s.Discontinuity}
		for i := 0; i < RoomTracks; i++ {
			trk := b.SourceTrack(i)
			var parts []ipc.Entry
			if ss := findSegment(snap, trk, s.SourceMSN); ss != nil {
				switch end := b.Ends[i]; {
				case s.SourceMSN < end.MSN:
					parts = ss.Parts
				case s.SourceMSN == end.MSN && end.Index < len(ss.Parts):
					parts = ss.Parts[:end.Index+1]
				}
			}
			rtk, _, err := t.track(b, trk, parts, true, nil)
			if err != nil {
				rtk = RoomTrack{Track: trk, Last: -1}
			}
			rtk.Closed, rtk.Last = true, len(rtk.Parts)-1
			seg.Tracks[i] = rtk
		}
		rt.segs[s.RoomMSN] = seg
	}
}

// switching runs the pending switch: the hold begins when the outgoing generation has reached the
// cut; the switch commits once the incoming start is ready without per-track overlap; a hold older
// than SwitchHoldLimit cancels the plan.
func (t *Timeline) switching(now int64, timed bool) error {
	p := t.st.Pending
	if p == nil {
		return nil
	}
	if t.cutReached && p.HoldStartMonoNS == 0 && timed {
		if err := t.commit(roomRecord{Op: opHold, AtMonoNS: now}); err != nil {
			return err
		}
	}
	if p.HoldStartMonoNS == 0 {
		return nil
	}
	ready, inits, err := t.incomingReady(p)
	if err != nil {
		return t.cancelPending()
	}
	if ready {
		out := t.st.live()
		if out != nil && !out.Frozen() {
			if err := t.freeze(out, false, ""); err != nil {
				return err
			}
		}
		b := p.Incoming
		b.Inits, b.FirstRoomMSN, b.Discontinuity = inits, t.st.LastAdvance+1, true
		if err := t.commit(roomRecord{Op: opBind, Bind: &b}); err != nil {
			return err
		}
		return t.exposeLive()
	}
	if timed && now-p.HoldStartMonoNS >= int64(SwitchHoldLimit) {
		return t.cancelPending()
	}
	return nil
}

// incomingReady reports whether the incoming generation can start: inits for every room track, an
// aligned independent first video part and a first audio part of its start segment, and no
// per-track UTC overlap with the outgoing generation's exposed ends.
func (t *Timeline) incomingReady(p *PendingSwitch) (bool, []ipc.Entry, error) {
	b := p.Incoming
	snap, ok := t.snapshotOf(b.Generation)
	if !ok {
		return false, nil, nil
	}
	if snap.Terminal != nil && !snap.Terminal.Verified {
		return false, nil, fmt.Errorf("incoming source failed")
	}
	inits := make([]ipc.Entry, RoomTracks)
	for i := range inits {
		trk := b.SourceTrack(i)
		if trk >= len(snap.Tracks) || snap.Tracks[trk].Init == nil {
			return false, nil, nil
		}
		inits[i] = *snap.Tracks[trk].Init
	}
	seg, ready, err := t.build(&b, snap, segState{SourceMSN: b.StartMSN}, nil)
	if err != nil {
		return false, nil, err
	}
	if !ready {
		return false, nil, nil
	}
	if out := t.st.live(); out != nil {
		for i, end := range t.ends(out) {
			if end.ref.MSN != 0 && seg.Tracks[i].UTCNS < end.utc {
				return false, nil, fmt.Errorf("incoming room track %d starts at %d before outgoing end %d", i, seg.Tracks[i].UTCNS, end.utc)
			}
		}
	}
	return true, inits, nil
}

// roll removes head segments the window no longer needs, keeping their bytes referenced for the
// grace period, releases expired grace references and unreferenced generations.
func (t *Timeline) roll(now int64) error {
	type dur struct {
		d   int64
		gen uint64
	}
	ds := make([]dur, len(t.st.Segments))
	var total int64
	for i, s := range t.st.Segments {
		if rt := t.gens[s.Generation]; rt != nil {
			if seg, ok := rt.segs[s.RoomMSN]; ok {
				ds[i] = dur{d: seg.Tracks[0].DurationNS, gen: s.Generation}
			}
		}
		total += ds[i].d
	}
	h := 0
	for h < len(ds)-1 && total-ds[h].d >= t.st.WindowNS {
		total -= ds[h].d
		h++
	}
	var grace []graceRef
	for _, g := range t.st.Grace {
		if g.ReleaseMonoNS > now {
			grace = append(grace, g)
		}
	}
	w := windowRecord{Head: t.st.LastAdvance + 1, DSN: t.st.DSN, Grace: grace}
	if h < len(t.st.Segments) {
		w.Head = t.st.Segments[h].RoomMSN
	}
	for i := 0; i < h; i++ {
		s := t.st.Segments[i]
		if s.Discontinuity {
			w.DSN++
		}
		b := t.st.binding(s.Generation)
		if b == nil {
			continue
		}
		release := int64(math.MaxInt64)
		if g := b.WindowNS + 2*b.MaxSegmentNS + ds[i].d; g >= 0 && now <= math.MaxInt64-g {
			release = now + g
		}
		w.Grace = append(w.Grace, graceRef{Generation: s.Generation, Key: b.Key, SourceMSN: s.SourceMSN, ReleaseMonoNS: release})
	}
	referenced := map[uint64]bool{}
	for _, s := range t.st.Segments[h:] {
		referenced[s.Generation] = true
	}
	for _, g := range w.Grace {
		referenced[g.Generation] = true
	}
	if live := t.st.live(); live != nil {
		referenced[live.Generation] = true
	}
	if p := t.st.Pending; p != nil {
		referenced[p.Incoming.Generation] = true
	}
	for _, b := range t.st.Bindings {
		if !referenced[b.Generation] {
			w.RemoveGenerations = append(w.RemoveGenerations, b.Generation)
		}
	}
	if h == 0 && len(grace) == len(t.st.Grace) && len(w.RemoveGenerations) == 0 {
		return nil
	}
	if err := t.commit(roomRecord{Op: opWindow, AtMonoNS: now, Window: &w}); err != nil {
		return err
	}
	for _, gen := range w.RemoveGenerations {
		if rt := t.gens[gen]; rt != nil {
			if rt.pin != nil {
				rt.pin.Close()
			}
			delete(t.gens, gen)
		}
	}
	for _, rt := range t.gens {
		for msn := range rt.segs {
			if msn < w.Head {
				delete(rt.segs, msn)
			}
		}
	}
	return nil
}

// repin installs, on every referenced Source, the lowest segment MSN per track the room still
// references, so Source maintenance never drops it. Floors only advance after the durable window
// change that released the segments.
func (t *Timeline) repin() error {
	floors := map[uint64]map[int]int{}
	lower := func(gen uint64, trk, msn int) {
		m := floors[gen]
		if m == nil {
			m = map[int]int{}
			floors[gen] = m
		}
		if cur, ok := m[trk]; !ok || msn < cur {
			m[trk] = msn
		}
	}
	pin := func(gen uint64, msn int) {
		if b := t.st.binding(gen); b != nil {
			for i := 0; i < RoomTracks; i++ {
				lower(gen, b.SourceTrack(i), msn)
			}
		}
	}
	for _, s := range t.st.Segments {
		pin(s.Generation, s.SourceMSN)
	}
	for _, g := range t.st.Grace {
		pin(g.Generation, g.SourceMSN)
	}
	if live := t.st.live(); live != nil && !live.Frozen() {
		pin(live.Generation, live.StartMSN)
	}
	if p := t.st.Pending; p != nil {
		b := p.Incoming
		for i := 0; i < RoomTracks; i++ {
			lower(b.Generation, b.SourceTrack(i), b.StartMSN)
		}
	}
	for gen, rt := range t.gens {
		f := floors[gen]
		switch {
		case rt.src == nil:
		case len(f) == 0 && rt.pin != nil:
			rt.pin.Close()
			rt.pin = nil
		case len(f) == 0:
		case rt.pin == nil:
			p, err := rt.src.PinWindow(f)
			if err != nil {
				return fmt.Errorf("%w: pin generation %d: %v", ErrTimelineInvalid, gen, err)
			}
			rt.pin = p
		default:
			if err := rt.pin.Update(f); err != nil {
				return fmt.Errorf("%w: pin generation %d: %v", ErrTimelineInvalid, gen, err)
			}
		}
	}
	return nil
}

// publish replaces the immutable view and wakes observers, under the room lock.
func (t *Timeline) publish() {
	v := TimelineSnapshot{Room: t.opts.Room, Bindings: slices.Clone(t.st.Bindings), DSN: t.st.DSN,
		NextMSN: t.st.LastAdvance + 1, Ended: t.st.Ended, Refusing: t.poison != nil, Closed: t.closed}
	if live := t.st.live(); live != nil {
		l := *live
		v.Live, v.Failed, v.FailReason = &l, live.Failed, live.FailReason
	}
	if p := t.st.Pending; p != nil {
		c := *p
		v.Pending = &c
	}
	for _, s := range t.st.Segments {
		if rt := t.gens[s.Generation]; rt != nil {
			if seg, ok := rt.segs[s.RoomMSN]; ok {
				v.Segments = append(v.Segments, seg)
			}
		}
	}
	t.view = v
	close(t.changed)
	t.changed = make(chan struct{})
}

// copyView returns an owned copy of the view.
func (t *Timeline) copyView() TimelineSnapshot {
	v := t.view
	v.Bindings = slices.Clone(v.Bindings)
	for i := range v.Bindings {
		v.Bindings[i].Inits = slices.Clone(v.Bindings[i].Inits)
		v.Bindings[i].Ends = slices.Clone(v.Bindings[i].Ends)
	}
	if v.Live != nil {
		l := *v.Live
		l.Inits, l.Ends = slices.Clone(l.Inits), slices.Clone(l.Ends)
		v.Live = &l
	}
	if v.Pending != nil {
		p := *v.Pending
		v.Pending = &p
	}
	v.Segments = slices.Clone(v.Segments)
	for i := range v.Segments {
		for j := range v.Segments[i].Tracks {
			v.Segments[i].Tracks[j].Parts = slices.Clone(v.Segments[i].Tracks[j].Parts)
		}
	}
	return v
}
