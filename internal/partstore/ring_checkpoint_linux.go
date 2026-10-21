//go:build linux

package partstore

import (
	"fmt"
	"sort"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

// ExportCheckpoint returns an owned deep copy of exactly the retained state RestoreCheckpoint
// needs, under the read lock; nothing in the result aliases the ring. A closed ring is
// ErrClosed and a poisoned one ErrPoisoned. It must not be called from a Persist callback,
// which runs under the ring's write lock. The caller owns durability: the checkpoint must be
// durable before the record prefix through RecordHigh is removed, and every later record is
// the suffix RestoreCheckpoint replays.
func (r *Ring) ExportCheckpoint() (Checkpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.mutable(); err != nil {
		return Checkpoint{}, err
	}
	cp := Checkpoint{Version: CheckpointVersion, Run: r.run, Source: r.source, Epoch: r.epoch, Config: r.cfg, Policy: r.policy,
		RecordHigh: r.recSeq, EntryHigh: r.high, ClockNS: r.clock, Terminal: copyEntry(r.terminal),
		Tracks: make([]TrackCheckpoint, len(r.tracks))}
	for i, t := range r.tracks {
		tc := TrackCheckpoint{Track: i, Init: copyEntry(t.init), Phases: copyEntry(t.phases), Cursor: t.cursor}
		if t.pending != nil {
			grant := *t.pending
			tc.Pending = &grant
		}
		for _, seg := range t.segments {
			tc.Segments = append(tc.Segments, SegmentSnapshot{MSN: seg.msn, Duration: seg.duration, End: seg.end,
				Closed: seg.closed, Dropped: seg.dropped, DroppedAtNS: seg.droppedAt, DeadlineNS: seg.deadline,
				Parts: append([]ipc.Entry(nil), seg.parts...)})
		}
		cp.Tracks[i] = tc
	}
	return cp, nil
}

// RestoreCheckpoint rebuilds a ring over store from cp and suffix, the records persisted after
// cp.RecordHigh in order. The checkpoint is validated structurally against the ring's own
// rules before any state is set, the suffix is then replayed through the same validation as a
// live mutation without persisting, and the digest of every finally retained init and part is
// checked against the store's bytes: the suffix may legitimately retire and overwrite bytes
// the checkpoint still retained. Config, policy and store identity errors are those of
// RestoreRing; an inconsistent checkpoint is ErrInvalidPublication. The checkpoint is copied,
// never aliased, and on any error the borrowed store is untouched.
func RestoreCheckpoint(store *EpochStore, cp Checkpoint, suffix []Record, persist Persist) (*Ring, error) {
	if cp.Version != CheckpointVersion {
		return nil, fmt.Errorf("%w: checkpoint version %d, want %d", ErrInvalidPublication, cp.Version, CheckpointVersion)
	}
	r, err := newRing(store, cp.Run, cp.Source, cp.Epoch, cp.Config, cp.Policy, persist)
	if err != nil {
		return nil, err
	}
	if err := r.loadCheckpoint(cp); err != nil {
		return nil, err
	}
	for _, rec := range suffix {
		if err := r.mutate(rec, false); err != nil {
			return nil, fmt.Errorf("record %d: %w", rec.Seq, err)
		}
	}
	if err := r.verifyRetained(); err != nil {
		return nil, err
	}
	return r, nil
}

// checkpointLoad accumulates the validated state of one checkpoint before it is applied.
type checkpointLoad struct {
	r        *Ring
	cp       Checkpoint
	retained map[uint64]ipc.Entry // every distinct entry by seq
	grants   map[uint64]struct{}  // every pending grant id
	high     uint64               // max retained seq
	tracks   []*track
}

// loadCheckpoint validates cp against the ring's config and policy and, only when every
// check passed, installs the rebuilt tracks, retained map, high-waters, clock and terminal.
func (r *Ring) loadCheckpoint(cp Checkpoint) error {
	switch {
	case cp.Policy != r.policy:
		return fmt.Errorf("%w: checkpoint policy %+v is not the effective policy %+v", ErrInvalidPublication, cp.Policy, r.policy)
	case cp.ClockNS < 0:
		return fmt.Errorf("%w: negative checkpoint clock %d", ErrInvalidPublication, cp.ClockNS)
	case len(cp.Tracks) != r.cfg.Tracks:
		return fmt.Errorf("%w: checkpoint has %d tracks, config %d", ErrInvalidPublication, len(cp.Tracks), r.cfg.Tracks)
	}
	l := &checkpointLoad{r: r, cp: cp, retained: make(map[uint64]ipc.Entry), grants: make(map[uint64]struct{}),
		tracks: make([]*track, len(cp.Tracks))}
	for i, tc := range cp.Tracks {
		t, err := l.loadTrack(i, tc)
		if err != nil {
			return err
		}
		l.tracks[i] = t
	}
	if cp.Terminal != nil {
		if err := l.loadTerminal(*cp.Terminal); err != nil {
			return err
		}
	}
	switch {
	case cp.EntryHigh != l.high:
		return fmt.Errorf("%w: checkpoint entry high %d, retained entries reach %d", ErrInvalidPublication, cp.EntryHigh, l.high)
	case cp.RecordHigh < cp.EntryHigh:
		return fmt.Errorf("%w: checkpoint record high %d below entry high %d", ErrInvalidPublication, cp.RecordHigh, cp.EntryHigh)
	}
	r.tracks, r.retained = l.tracks, l.retained
	r.high, r.recSeq, r.clock = cp.EntryHigh, cp.RecordHigh, cp.ClockNS
	r.terminal = copyEntry(cp.Terminal)
	return nil
}

// entry validates one checkpoint entry's fields, kind and track, bounds its clock, and
// records it as retained with a globally unique seq.
func (l *checkpointLoad) entry(e ipc.Entry, kind string, trk int) error {
	if err := l.r.checkEntryFields(e); err != nil {
		return err
	}
	switch {
	case e.Kind != kind || e.Track != trk:
		return fmt.Errorf("%w: entry seq %d is %q on track %d, want %q on track %d", ErrInvalidPublication, e.Seq, e.Kind, e.Track, kind, trk)
	case e.PublishedMonoNS > l.cp.ClockNS:
		return fmt.Errorf("%w: entry seq %d published at %d after checkpoint clock %d", ErrInvalidPublication, e.Seq, e.PublishedMonoNS, l.cp.ClockNS)
	}
	if _, dup := l.retained[e.Seq]; dup {
		return fmt.Errorf("%w: entry seq %d retained twice", ErrInvalidPublication, e.Seq)
	}
	l.retained[e.Seq] = e
	if e.Seq > l.high {
		l.high = e.Seq
	}
	return nil
}

// loadTrack validates track row i and rebuilds its state; last and end derive from the newest
// segment.
func (l *checkpointLoad) loadTrack(i int, tc TrackCheckpoint) (*track, error) {
	capacity := l.r.cfg.Capacity
	switch {
	case tc.Track != i:
		return nil, fmt.Errorf("%w: checkpoint track row %d names track %d", ErrInvalidPublication, i, tc.Track)
	case tc.Cursor < 0 || tc.Cursor >= capacity:
		return nil, fmt.Errorf("%w: track %d cursor %d outside capacity %d", ErrInvalidPublication, i, tc.Cursor, capacity)
	}
	t := &track{cursor: tc.Cursor}
	var occ []extent
	var floor uint64 // every part seq must exceed the init and phases seqs
	if tc.Init != nil {
		e := *tc.Init
		if err := l.entry(e, ipc.KindInit, i); err != nil {
			return nil, err
		}
		if e.Offset != 0 {
			return nil, fmt.Errorf("%w: track %d init at offset %d, want 0", ErrInvalidPublication, i, e.Offset)
		}
		t.init = copyEntry(&e)
		occ = append(occ, extent{e.Offset, e.Offset + e.Length})
		floor = e.Seq
	}
	if tc.Phases != nil {
		e := *tc.Phases
		if err := l.entry(e, ipc.KindPhases, i); err != nil {
			return nil, err
		}
		if i != ipc.TrackAudioA && i != ipc.TrackAudioB {
			return nil, fmt.Errorf("%w: phases on video track %d", ErrInvalidPublication, i)
		}
		t.phases = copyEntry(&e)
		if e.Seq > floor {
			floor = e.Seq
		}
	}
	if n := len(tc.Segments); n > 0 {
		switch {
		case t.init == nil:
			return nil, fmt.Errorf("%w: track %d has parts but no init", ErrInvalidPublication, i)
		case i >= ipc.TrackAudioA && t.phases == nil:
			return nil, fmt.Errorf("%w: audio track %d has parts but no phases", ErrInvalidPublication, i)
		}
		trackEnd := tc.Segments[n-1].End
		var prev *segment
		var prevPart *ipc.Entry
		for k, ss := range tc.Segments {
			seg, err := l.segment(i, t.phases, ss, prev, prevPart, floor, trackEnd, k == n-1)
			if err != nil {
				return nil, err
			}
			for _, p := range seg.parts {
				occ = append(occ, extent{p.Offset, p.Offset + p.Length})
			}
			t.segments = append(t.segments, seg)
			prev, prevPart = seg, copyEntry(&seg.parts[len(seg.parts)-1])
		}
		t.last, t.end = prevPart, prev.end
	}
	if tc.Pending != nil {
		g := *tc.Pending
		switch {
		case l.cp.Terminal != nil:
			return nil, fmt.Errorf("%w: grant %d pending on track %d after terminal", ErrInvalidPublication, g.ID, i)
		case g.ID == 0 || g.ID > l.cp.RecordHigh:
			return nil, fmt.Errorf("%w: grant %d on track %d outside record high %d", ErrInvalidPublication, g.ID, i, l.cp.RecordHigh)
		case g.Track != i:
			return nil, fmt.Errorf("%w: grant %d on track row %d names track %d", ErrInvalidPublication, g.ID, i, g.Track)
		case g.Kind != ipc.KindInit && g.Kind != ipc.KindPart:
			return nil, fmt.Errorf("%w: grant %d reserves %q", ErrInvalidPublication, g.ID, g.Kind)
		case g.Offset < 0 || g.Length <= 0 || g.Offset > capacity-g.Length:
			return nil, fmt.Errorf("%w: grant %d extent %d+%d outside capacity %d", ErrInvalidPublication, g.ID, g.Offset, g.Length, capacity)
		case g.Kind == ipc.KindInit && (t.init != nil || g.Offset != 0):
			return nil, fmt.Errorf("%w: grant %d reserves an init at %d on track %d", ErrInvalidPublication, g.ID, g.Offset, i)
		case g.Kind == ipc.KindPart && t.init == nil:
			return nil, fmt.Errorf("%w: grant %d reserves a part on track %d without init", ErrInvalidPublication, g.ID, i)
		}
		if _, dup := l.grants[g.ID]; dup {
			return nil, fmt.Errorf("%w: grant %d pending twice", ErrInvalidPublication, g.ID)
		}
		l.grants[g.ID] = struct{}{}
		next := g.Offset + g.Length
		if next >= capacity {
			next = 0
		}
		if tc.Cursor != next {
			return nil, fmt.Errorf("%w: track %d cursor %d, grant %d ends at %d", ErrInvalidPublication, i, tc.Cursor, g.ID, next)
		}
		t.pending = &g
		occ = append(occ, extent{g.Offset, g.Offset + g.Length})
	}
	sort.Slice(occ, func(a, b int) bool { return occ[a].off < occ[b].off })
	for k, x := range occ {
		switch {
		case x.off < 0 || x.end > capacity:
			return nil, fmt.Errorf("%w: track %d extent %d..%d outside capacity %d", ErrInvalidPublication, i, x.off, x.end, capacity)
		case k > 0 && x.off < occ[k-1].end:
			return nil, fmt.Errorf("%w: track %d extents %d..%d and %d..%d overlap", ErrInvalidPublication, i, occ[k-1].off, occ[k-1].end, x.off, x.end)
		}
	}
	return t, nil
}

// segment validates one retained segment of track trk after prev and prevPart, which are nil
// for the oldest retained segment. floor is the seq every part must exceed, trackEnd the
// newest segment's end and last whether ss is that newest segment.
func (l *checkpointLoad) segment(trk int, phases *ipc.Entry, ss SegmentSnapshot, prev *segment, prevPart *ipc.Entry, floor uint64, trackEnd int64, last bool) (*segment, error) {
	pol := l.r.policy
	switch {
	case ss.MSN < 1:
		return nil, fmt.Errorf("%w: track %d segment msn %d", ErrInvalidPublication, trk, ss.MSN)
	case prev != nil && ss.MSN <= prev.msn:
		return nil, fmt.Errorf("%w: track %d segment %d after segment %d", ErrInvalidPublication, trk, ss.MSN, prev.msn)
	case ss.Duration <= 0 || ss.Duration > int64(pol.MaxSegment):
		return nil, fmt.Errorf("%w: track %d segment %d duration %d exceeds %v or is not positive", ErrInvalidPublication, trk, ss.MSN, ss.Duration, pol.MaxSegment)
	case ss.End < ss.Duration:
		return nil, fmt.Errorf("%w: track %d segment %d end %d precedes its duration %d", ErrInvalidPublication, trk, ss.MSN, ss.End, ss.Duration)
	case len(ss.Parts) == 0:
		return nil, fmt.Errorf("%w: track %d segment %d has no parts", ErrInvalidPublication, trk, ss.MSN)
	}
	seg := &segment{msn: ss.MSN, start: ss.End - ss.Duration, duration: ss.Duration, end: ss.End,
		closed: ss.Closed, dropped: ss.Dropped, droppedAt: ss.DroppedAtNS, deadline: ss.DeadlineNS}
	contiguous := prev != nil && ss.MSN == prev.msn+1
	switch {
	case ss.MSN == 1 && seg.start != 0:
		return nil, fmt.Errorf("%w: track %d segment 1 starts at %d, want 0", ErrInvalidPublication, trk, seg.start)
	case contiguous && seg.start != prev.end:
		return nil, fmt.Errorf("%w: track %d segment %d starts at %d, segment %d ends at %d", ErrInvalidPublication, trk, ss.MSN, seg.start, prev.msn, prev.end)
	case prev != nil && !contiguous && (seg.start < prev.end || !prev.dropped):
		return nil, fmt.Errorf("%w: track %d segments %d and %d leave a gap outside the dropped prefix", ErrInvalidPublication, trk, prev.msn, ss.MSN)
	case ss.Dropped && prev != nil && !prev.dropped:
		return nil, fmt.Errorf("%w: track %d segment %d dropped before segment %d", ErrInvalidPublication, trk, ss.MSN, prev.msn)
	case !ss.Closed && (!last || l.cp.Terminal != nil):
		return nil, fmt.Errorf("%w: track %d segment %d is open", ErrInvalidPublication, trk, ss.MSN)
	}
	if ss.Dropped {
		switch {
		case !ss.Closed:
			return nil, fmt.Errorf("%w: track %d dropped segment %d is open", ErrInvalidPublication, trk, ss.MSN)
		case ss.End > trackEnd-int64(pol.Window):
			return nil, fmt.Errorf("%w: track %d dropped segment %d is inside the window", ErrInvalidPublication, trk, ss.MSN)
		case ss.DroppedAtNS < 0 || ss.DroppedAtNS > l.cp.ClockNS:
			return nil, fmt.Errorf("%w: track %d segment %d dropped at %d outside checkpoint clock %d", ErrInvalidPublication, trk, ss.MSN, ss.DroppedAtNS, l.cp.ClockNS)
		case prev != nil && ss.DroppedAtNS < prev.droppedAt:
			return nil, fmt.Errorf("%w: track %d segment %d dropped at %d before segment %d at %d", ErrInvalidPublication, trk, ss.MSN, ss.DroppedAtNS, prev.msn, prev.droppedAt)
		}
		grace, ok := addNS(int64(pol.Window+2*pol.MaxSegment), ss.Duration)
		deadline, ok2 := addNS(ss.DroppedAtNS, grace)
		if !ok || !ok2 || deadline != ss.DeadlineNS {
			return nil, fmt.Errorf("%w: track %d segment %d deadline %d, want %d", ErrInvalidPublication, trk, ss.MSN, ss.DeadlineNS, deadline)
		}
	} else if ss.DroppedAtNS != 0 || ss.DeadlineNS != 0 {
		return nil, fmt.Errorf("%w: track %d visible segment %d carries a drop clock", ErrInvalidPublication, trk, ss.MSN)
	}
	video := trk < ipc.TrackAudioA
	var sum int64
	for j, e := range ss.Parts {
		if err := l.entry(e, ipc.KindPart, trk); err != nil {
			return nil, err
		}
		switch {
		case e.MSN != ss.MSN || e.Index != j:
			return nil, fmt.Errorf("%w: track %d segment %d part %d is entry msn %d part %d", ErrInvalidPublication, trk, ss.MSN, j, e.MSN, e.Index)
		case e.FragmentStart != (j == 0):
			return nil, fmt.Errorf("%w: track %d segment %d part %d fragment start %t", ErrInvalidPublication, trk, ss.MSN, j, e.FragmentStart)
		case video && j == 0 && !e.Independent:
			return nil, fmt.Errorf("%w: video segment %d must start independent", ErrInvalidPublication, ss.MSN)
		case e.Sequence != uint64(ss.MSN):
			return nil, fmt.Errorf("%w: track %d segment %d sequence %d", ErrInvalidPublication, trk, ss.MSN, e.Sequence)
		case e.Seq <= floor:
			return nil, fmt.Errorf("%w: track %d part seq %d does not follow its init and phases", ErrInvalidPublication, trk, e.Seq)
		}
		closeSeg, err := checkPartBoundary(video, phases, e)
		if err != nil {
			return nil, err
		}
		if closeSeg && j != len(ss.Parts)-1 {
			return nil, fmt.Errorf("%w: track %d segment %d continues after final part %d", ErrInvalidPublication, trk, ss.MSN, j)
		}
		if prevPart == nil {
			if ss.MSN == 1 && e.FirstAU != 0 {
				return nil, fmt.Errorf("%w: track %d segment 1 starts at first_au %d, want 0", ErrInvalidPublication, trk, e.FirstAU)
			}
		} else {
			next := prevPart.FirstAU + prevPart.Payload
			switch {
			case e.Seq <= prevPart.Seq:
				return nil, fmt.Errorf("%w: track %d part seq %d does not follow %d", ErrInvalidPublication, trk, e.Seq, prevPart.Seq)
			case e.PTS <= prevPart.PTS:
				return nil, fmt.Errorf("%w: pts %d does not increase past %d", ErrInvalidPublication, e.PTS, prevPart.PTS)
			case e.DTS < prevPart.DTS:
				return nil, fmt.Errorf("%w: dts %d goes back from %d", ErrInvalidPublication, e.DTS, prevPart.DTS)
			case (j > 0 || contiguous) && e.FirstAU != next:
				return nil, fmt.Errorf("%w: first_au %d does not follow %d+%d", ErrInvalidPublication, e.FirstAU, prevPart.FirstAU, prevPart.Payload)
			case e.FirstAU < next:
				return nil, fmt.Errorf("%w: first_au %d overlaps %d+%d", ErrInvalidPublication, e.FirstAU, prevPart.FirstAU, prevPart.Payload)
			}
		}
		var ok bool
		if sum, ok = addNS(sum, e.Duration); !ok {
			return nil, fmt.Errorf("%w: track %d segment %d duration overflows", ErrInvalidPublication, trk, ss.MSN)
		}
		if closeSeg {
			seg.closed = true
		}
		seg.parts = append(seg.parts, e)
		prevPart = copyEntry(&e)
	}
	if sum != ss.Duration {
		return nil, fmt.Errorf("%w: track %d segment %d duration %d, parts sum to %d", ErrInvalidPublication, trk, ss.MSN, ss.Duration, sum)
	}
	return seg, nil
}

// loadTerminal validates the terminal against the rebuilt tracks; it must be the newest entry.
func (l *checkpointLoad) loadTerminal(e ipc.Entry) error {
	if err := l.entry(e, ipc.KindTerminal, ipc.TrackTerminal); err != nil {
		return err
	}
	if e.Seq != l.high {
		return fmt.Errorf("%w: terminal seq %d is not the newest entry %d", ErrInvalidPublication, e.Seq, l.high)
	}
	for i, t := range l.tracks {
		switch {
		case e.Verified && (t.init == nil || t.last == nil):
			return fmt.Errorf("%w: verified terminal but track %d has no init and part", ErrInvalidPublication, i)
		case e.Verified && (i == ipc.TrackAudioA || i == ipc.TrackAudioB) && t.phases == nil:
			return fmt.Errorf("%w: verified terminal but audio track %d has no phases", ErrInvalidPublication, i)
		}
	}
	return nil
}
