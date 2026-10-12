//go:build linux

package partstore

import (
	"fmt"
	"math"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

// mutate validates rec against the current state, persists it when live, and only then
// applies it. A replayed record (live false) skips persistence and the byte check; retained
// bytes are digest-checked once after the whole history. The caller holds the write lock.
func (r *Ring) mutate(rec Record, live bool) error {
	if err := r.checkScope(rec); err != nil {
		return err
	}
	var apply func()
	var err error
	switch rec.Op {
	case OpReserve:
		apply, err = r.prepareReserve(rec)
	case OpCommit:
		apply, err = r.prepareCommit(rec, live)
	case OpMeta:
		apply, err = r.prepareMeta(rec)
	case OpCancel:
		apply, err = r.prepareCancel(rec)
	case OpDrop:
		apply, err = r.prepareDrop(rec)
	case OpRetire:
		apply, err = r.prepareRetire(rec)
	default:
		err = fmt.Errorf("%w: unknown record op %q", ErrInvalidPublication, rec.Op)
	}
	if err != nil {
		return err
	}
	if live {
		if err := r.persist(rec); err != nil {
			r.poisoned = true
			return fmt.Errorf("%w: persist record %d: %w", ErrPoisoned, rec.Seq, err)
		}
	}
	apply()
	r.recSeq = rec.Seq
	return nil
}

// checkScope requires rec to be the next record of exactly this ring, config and policy.
func (r *Ring) checkScope(rec Record) error {
	switch {
	case rec.Version != RecordVersion:
		return fmt.Errorf("%w: record version %d, want %d", ErrInvalidPublication, rec.Version, RecordVersion)
	case rec.Run != r.run || rec.Source != r.source || rec.Epoch != r.epoch:
		return fmt.Errorf("%w: record belongs to another run, source or epoch", ErrInvalidPublication)
	case rec.Seq == 0 || r.recSeq == math.MaxUint64 || rec.Seq != r.recSeq+1:
		return fmt.Errorf("%w: record seq %d, want %d after %d", ErrInvalidPublication, rec.Seq, r.recSeq+1, r.recSeq)
	case rec.Config != r.cfg:
		return fmt.Errorf("%w: record config %+v differs from ring config %+v", ErrInvalidPublication, rec.Config, r.cfg)
	case rec.Policy != r.policy:
		return fmt.Errorf("%w: record policy %+v differs from ring policy %+v", ErrInvalidPublication, rec.Policy, r.policy)
	}
	return nil
}

// unused rejects non-zero fields that rec's op does not use.
func unused(rec Record, reservation, entry, msn, at bool) error {
	switch {
	case reservation && rec.Reservation != (Reservation{}):
		return fmt.Errorf("%w: %s record carries a reservation", ErrInvalidPublication, rec.Op)
	case entry && rec.Entry != (ipc.Entry{}):
		return fmt.Errorf("%w: %s record carries an entry", ErrInvalidPublication, rec.Op)
	case msn && rec.MSN != 0:
		return fmt.Errorf("%w: %s record carries an msn", ErrInvalidPublication, rec.Op)
	case at && rec.AtNS != 0:
		return fmt.Errorf("%w: %s record carries a clock", ErrInvalidPublication, rec.Op)
	}
	return nil
}

func (r *Ring) prepareReserve(rec Record) (func(), error) {
	if err := unused(rec, false, true, true, true); err != nil {
		return nil, err
	}
	res := rec.Reservation
	if res.ID != rec.Seq || res.Track != rec.Track {
		return nil, fmt.Errorf("%w: reserve record %d names grant %d on track %d", ErrInvalidPublication, rec.Seq, res.ID, res.Track)
	}
	offset, err := r.place(res)
	if err != nil {
		return nil, err
	}
	if offset != res.Offset {
		return nil, fmt.Errorf("%w: reserve record %d offset %d, first fit is %d", ErrInvalidPublication, rec.Seq, res.Offset, offset)
	}
	t := r.tracks[res.Track]
	return func() {
		grant := res
		t.pending = &grant
		t.cursor = res.Offset + res.Length
		if t.cursor >= r.cfg.Capacity {
			t.cursor = 0
		}
	}, nil
}

func (r *Ring) prepareCommit(rec Record, live bool) (func(), error) {
	if err := unused(rec, false, false, false, true); err != nil {
		return nil, err
	}
	res, e := rec.Reservation, rec.Entry
	if err := r.matchPending(res); err != nil {
		return nil, err
	}
	if err := r.checkEntry(e); err != nil {
		return nil, err
	}
	switch {
	case e.Kind != ipc.KindInit && e.Kind != ipc.KindPart:
		return nil, fmt.Errorf("%w: commit of %q entry", ErrInvalidPublication, e.Kind)
	case e.Kind != res.Kind || e.Track != res.Track || e.Offset != res.Offset || e.Length != res.Length:
		return nil, fmt.Errorf("%w: entry seq %d does not fill grant %d", ErrInvalidPublication, e.Seq, res.ID)
	case rec.Track != e.Track || rec.MSN != e.MSN:
		return nil, fmt.Errorf("%w: commit record track/msn differ from the entry", ErrInvalidPublication)
	}
	t := r.tracks[e.Track]
	var apply func()
	if e.Kind == ipc.KindInit {
		if t.init != nil {
			return nil, fmt.Errorf("%w: track %d init already committed", ErrInvalidPublication, e.Track)
		}
		apply = func() { t.init = copyEntry(&e) }
	} else {
		var err error
		if apply, err = r.preparePart(t, e); err != nil {
			return nil, err
		}
	}
	if live {
		if _, err := r.checked(e); err != nil {
			return nil, err
		}
	}
	return func() {
		apply()
		t.pending = nil
		r.publish(e)
	}, nil
}

// preparePart checks fragment continuity and timing of part e on t.
func (r *Ring) preparePart(t *track, e ipc.Entry) (func(), error) {
	if t.init == nil {
		return nil, fmt.Errorf("%w: track %d has no init", ErrInvalidPublication, e.Track)
	}
	video := e.Track < ipc.TrackAudioA
	switch {
	case !video && t.phases == nil:
		return nil, fmt.Errorf("%w: audio track %d has no phases", ErrInvalidPublication, e.Track)
	case video && e.FragmentStart && !e.Independent:
		return nil, fmt.Errorf("%w: video segment %d must start independent", ErrInvalidPublication, e.MSN)
	case e.FirstAU > math.MaxInt-e.Payload:
		return nil, fmt.Errorf("%w: first_au %d plus payload %d overflows", ErrInvalidPublication, e.FirstAU, e.Payload)
	}
	prev := t.last
	var open *segment
	if n := len(t.segments); n > 0 && !t.segments[n-1].closed {
		open = t.segments[n-1]
	}
	if prev == nil {
		if e.MSN != 1 || e.Index != 0 || !e.FragmentStart || e.Sequence != 1 || e.FirstAU != 0 {
			return nil, fmt.Errorf("%w: first part of track %d must be msn 1 part 0 sequence 1 at fragment start", ErrInvalidPublication, e.Track)
		}
	} else {
		switch {
		case e.MSN == prev.MSN:
			if e.Index != prev.Index+1 || e.FragmentStart || e.Sequence != prev.Sequence || open == nil || open.msn != e.MSN {
				return nil, fmt.Errorf("%w: part %d of segment %d does not continue part %d", ErrInvalidPublication, e.Index, e.MSN, prev.Index)
			}
		case e.MSN == prev.MSN+1:
			if e.Index != 0 || !e.FragmentStart || prev.Sequence == math.MaxUint64 || e.Sequence != prev.Sequence+1 {
				return nil, fmt.Errorf("%w: segment %d must start at part 0 with sequence %d+1", ErrInvalidPublication, e.MSN, prev.Sequence)
			}
		default:
			return nil, fmt.Errorf("%w: segment %d after segment %d", ErrInvalidPublication, e.MSN, prev.MSN)
		}
		switch {
		case e.PTS <= prev.PTS:
			return nil, fmt.Errorf("%w: pts %d does not increase past %d", ErrInvalidPublication, e.PTS, prev.PTS)
		case e.DTS < prev.DTS:
			return nil, fmt.Errorf("%w: dts %d goes back from %d", ErrInvalidPublication, e.DTS, prev.DTS)
		case prev.FirstAU > math.MaxInt-prev.Payload || e.FirstAU != prev.FirstAU+prev.Payload:
			return nil, fmt.Errorf("%w: first_au %d does not follow %d+%d", ErrInvalidPublication, e.FirstAU, prev.FirstAU, prev.Payload)
		}
	}
	start, dur := t.end, int64(0)
	if open != nil && open.msn == e.MSN {
		start, dur = open.start, open.duration
	}
	dur, ok := addNS(dur, e.Duration)
	if !ok || dur > int64(r.policy.MaxSegment) {
		return nil, fmt.Errorf("%w: segment %d duration exceeds %v", ErrInvalidPublication, e.MSN, r.policy.MaxSegment)
	}
	end, ok := addNS(start, dur)
	if !ok {
		return nil, fmt.Errorf("%w: segment %d end overflows", ErrInvalidPublication, e.MSN)
	}
	return func() {
		seg := open
		if seg == nil || seg.msn != e.MSN {
			if open != nil {
				open.closed = true
			}
			seg = &segment{msn: e.MSN, start: start}
			t.segments = append(t.segments, seg)
		}
		seg.duration, seg.end = dur, end
		seg.parts = append(seg.parts, e)
		t.last, t.end = copyEntry(&e), end
	}, nil
}

func (r *Ring) prepareMeta(rec Record) (func(), error) {
	if err := unused(rec, true, false, true, true); err != nil {
		return nil, err
	}
	e := rec.Entry
	if err := r.checkEntry(e); err != nil {
		return nil, err
	}
	if rec.Track != e.Track {
		return nil, fmt.Errorf("%w: meta record track differs from the entry", ErrInvalidPublication)
	}
	switch e.Kind {
	case ipc.KindPhases:
		t := r.tracks[e.Track]
		switch {
		case e.Track != ipc.TrackAudioA && e.Track != ipc.TrackAudioB:
			return nil, fmt.Errorf("%w: phases on video track %d", ErrInvalidPublication, e.Track)
		case t.phases != nil:
			return nil, fmt.Errorf("%w: track %d phases already published", ErrInvalidPublication, e.Track)
		case t.last != nil:
			return nil, fmt.Errorf("%w: track %d phases after its parts", ErrInvalidPublication, e.Track)
		}
		return func() { t.phases = copyEntry(&e); r.publish(e) }, nil
	case ipc.KindTerminal:
		for i, t := range r.tracks {
			switch {
			case t.pending != nil:
				return nil, fmt.Errorf("%w: terminal while grant %d is pending on track %d", ErrInvalidPublication, t.pending.ID, i)
			case e.Verified && (t.init == nil || t.last == nil):
				return nil, fmt.Errorf("%w: verified terminal but track %d has no init and part", ErrInvalidPublication, i)
			case e.Verified && (i == ipc.TrackAudioA || i == ipc.TrackAudioB) && t.phases == nil:
				return nil, fmt.Errorf("%w: verified terminal but audio track %d has no phases", ErrInvalidPublication, i)
			}
		}
		return func() {
			for _, t := range r.tracks {
				if n := len(t.segments); n > 0 {
					t.segments[n-1].closed = true
				}
			}
			r.terminal = copyEntry(&e)
			r.publish(e)
		}, nil
	}
	return nil, fmt.Errorf("%w: meta record carries a %q entry", ErrInvalidPublication, e.Kind)
}

func (r *Ring) prepareCancel(rec Record) (func(), error) {
	if err := unused(rec, false, true, true, true); err != nil {
		return nil, err
	}
	res := rec.Reservation
	if err := r.matchPending(res); err != nil {
		return nil, err
	}
	if rec.Track != res.Track {
		return nil, fmt.Errorf("%w: cancel record track differs from the grant", ErrInvalidPublication)
	}
	t := r.tracks[res.Track]
	return func() { t.pending = nil }, nil
}

func (r *Ring) prepareDrop(rec Record) (func(), error) {
	t, seg, err := r.removal(rec)
	if err != nil {
		return nil, err
	}
	switch {
	case seg.dropped:
		return nil, fmt.Errorf("%w: track %d segment %d already dropped", ErrInvalidPublication, rec.Track, rec.MSN)
	case !seg.closed:
		return nil, fmt.Errorf("%w: track %d segment %d is open", ErrNotReady, rec.Track, rec.MSN)
	case seg.end > t.end-int64(r.policy.Window):
		return nil, fmt.Errorf("%w: track %d segment %d is inside the window", ErrNotReady, rec.Track, rec.MSN)
	}
	for _, s := range t.segments {
		if !s.dropped {
			if s != seg {
				return nil, fmt.Errorf("%w: segment %d is older and still visible on track %d", ErrInvalidPublication, s.msn, rec.Track)
			}
			break
		}
	}
	grace, ok := addNS(int64(r.policy.Window+2*r.policy.MaxSegment), seg.duration)
	deadline, ok2 := addNS(rec.AtNS, grace)
	if !ok || !ok2 {
		return nil, fmt.Errorf("%w: retirement deadline overflows", ErrInvalidPublication)
	}
	return func() {
		seg.dropped, seg.droppedAt, seg.deadline = true, rec.AtNS, deadline
		r.clock = rec.AtNS
	}, nil
}

func (r *Ring) prepareRetire(rec Record) (func(), error) {
	t, seg, err := r.removal(rec)
	if err != nil {
		return nil, err
	}
	switch {
	case !seg.dropped:
		return nil, fmt.Errorf("%w: track %d segment %d is not dropped", ErrNotReady, rec.Track, rec.MSN)
	case rec.AtNS < seg.deadline:
		return nil, fmt.Errorf("%w: track %d segment %d deadline %d not reached at %d", ErrNotReady, rec.Track, rec.MSN, seg.deadline, rec.AtNS)
	}
	return func() {
		kept := t.segments[:0]
		for _, s := range t.segments {
			if s != seg {
				kept = append(kept, s)
			}
		}
		t.segments = kept
		for _, p := range seg.parts {
			delete(r.retained, p.Seq)
		}
		r.clock = rec.AtNS
	}, nil
}

// removal checks the common fields of a drop or retire record and finds its segment.
func (r *Ring) removal(rec Record) (*track, *segment, error) {
	if err := unused(rec, true, true, false, false); err != nil {
		return nil, nil, err
	}
	switch {
	case rec.Track < 0 || rec.Track >= len(r.tracks):
		return nil, nil, fmt.Errorf("%w: track %d", ErrNotFound, rec.Track)
	case rec.MSN < 1:
		return nil, nil, fmt.Errorf("%w: msn %d", ErrInvalidPublication, rec.MSN)
	case rec.AtNS < 0 || rec.AtNS < r.clock:
		return nil, nil, fmt.Errorf("%w: clock %d precedes the latest clock %d", ErrInvalidPublication, rec.AtNS, r.clock)
	}
	t := r.tracks[rec.Track]
	seg := t.find(rec.MSN)
	if seg == nil {
		return nil, nil, fmt.Errorf("%w: track %d segment %d", ErrNotFound, rec.Track, rec.MSN)
	}
	return t, seg, nil
}

// checkEntry validates e as the next entry of this ring's stream.
func (r *Ring) checkEntry(e ipc.Entry) error {
	if err := r.checkEntryFields(e); err != nil {
		return err
	}
	switch {
	case e.Seq != r.high+1:
		return fmt.Errorf("%w: entry seq %d, want %d", ErrInvalidPublication, e.Seq, r.high+1)
	case r.terminal != nil:
		return fmt.Errorf("%w: entry after terminal", ErrInvalidPublication)
	}
	return nil
}

// checkEntryFields validates the fields of e independently of the ring's position: ipc
// validity, this ring's scope, a configured track and non-negative publication clocks.
func (r *Ring) checkEntryFields(e ipc.Entry) error {
	if err := ipc.ValidatePacket(ipc.Packet{Type: ipc.TypeEntry, Entry: &e}); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPublication, err)
	}
	switch {
	case e.Run != r.run || e.Source != r.source || e.Epoch != r.epoch:
		return fmt.Errorf("%w: entry belongs to another run, source or epoch", ErrInvalidPublication)
	case e.Kind != ipc.KindTerminal && e.Track >= r.cfg.Tracks:
		return fmt.Errorf("%w: track %d is not configured", ErrInvalidPublication, e.Track)
	case e.PublishedNS < 0 || e.PublishedMonoNS < 0:
		return fmt.Errorf("%w: negative publication clock", ErrInvalidPublication)
	}
	return nil
}

// publish records e as retained and advances the entry high-water and clock.
func (r *Ring) publish(e ipc.Entry) {
	r.retained[e.Seq] = e
	r.high = e.Seq
	if e.PublishedMonoNS > r.clock {
		r.clock = e.PublishedMonoNS
	}
}

// addNS adds two non-negative ns values, reporting overflow.
func addNS(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}
