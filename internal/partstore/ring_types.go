package partstore

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

// RecordVersion is the version of the Ring's durable Record, independent of ipc.Version.
const RecordVersion = 1

// Record operations.
const (
	OpReserve = "reserve"
	OpCommit  = "commit"
	OpMeta    = "meta"
	OpCancel  = "cancel"
	OpDrop    = "drop"
	OpRetire  = "retire"
)

// Policy defaults, used for a zero Window or MaxSegment.
const (
	DefaultWindow     = 24 * time.Second
	DefaultMaxSegment = 3 * time.Second
)

var (
	// ErrInvalidPublication reports an entry, record or transition the ring refuses: it fails
	// ipc validation, names another scope, config or policy, breaks sequence or fragment
	// continuity, overflows, conflicts with retained state, or its bytes do not match the
	// published digest.
	ErrInvalidPublication = errors.New("partstore: invalid publication")

	// ErrFull reports that no contiguous extent of the requested length is free in the track.
	ErrFull = errors.New("partstore: track full")

	// ErrReservation reports a grant that is not the track's exact pending grant, or a reserve
	// while one is already pending on the track.
	ErrReservation = errors.New("partstore: reservation mismatch")

	// ErrNotFound reports an init, segment, part or entry sequence the ring does not retain.
	ErrNotFound = errors.New("partstore: not found")

	// ErrNotReady reports a segment that is still open, inside the window, not dropped or
	// before its retirement deadline.
	ErrNotReady = errors.New("partstore: not ready")

	// ErrPoisoned reports a ring whose persistence outcome is ambiguous: a Persist call
	// returned an error, so every further mutation is refused while reads keep the old state.
	ErrPoisoned = errors.New("partstore: ring poisoned")
)

// Policy is the retention policy of a Ring: the live window and the longest segment, both
// positive. Zero fields take the defaults.
type Policy struct {
	Window     time.Duration
	MaxSegment time.Duration
}

// effective returns p with defaults applied and rejects non-positive or overflowing values.
func (p Policy) effective() (Policy, error) {
	if p.Window == 0 {
		p.Window = DefaultWindow
	}
	if p.MaxSegment == 0 {
		p.MaxSegment = DefaultMaxSegment
	}
	switch {
	case p.Window < 0 || p.MaxSegment < 0:
		return Policy{}, fmt.Errorf("%w: policy window %v and max segment %v must be positive", ErrInvalidConfig, p.Window, p.MaxSegment)
	case p.MaxSegment > math.MaxInt64/3 || p.Window > math.MaxInt64-3*p.MaxSegment:
		return Policy{}, fmt.Errorf("%w: policy window %v and max segment %v overflow the grace period", ErrInvalidConfig, p.Window, p.MaxSegment)
	}
	return p, nil
}

// Reservation is a pending writable grant: an exact byte extent of one track that the writer
// may fill with exactly one init or part. ID is the Seq of the reserve Record that created it.
type Reservation struct {
	ID     uint64
	Kind   string
	Track  int
	Offset int64
	Length int64
}

// Record is one durable mutation of a Ring, scoped to its run, source and epoch and numbered
// gap-free from 1. Config and Policy are the ring's exact effective values, repeated in every
// record so that recovery is bound to the original capacity, tracks, codec and retention.
// Each Op uses only its own fields and leaves the others zero: reserve and cancel carry
// Reservation and Track; commit carries Reservation, Entry, Track and MSN; meta carries Entry
// and Track; drop and retire carry Track, MSN and the caller's monotonic clock in AtNS.
type Record struct {
	Version     int
	Run         string
	Source      string
	Epoch       uint64
	Seq         uint64
	Config      ipc.Config
	Policy      Policy
	Op          string
	Reservation Reservation
	Entry       ipc.Entry
	Track       int
	MSN         int
	AtNS        int64
}

// Persist makes one Record durable. It is called once per mutation, under the ring's
// mutation lock, after full validation and before any state change; it must not call back
// into the Ring. A nil result means the record is durably committed. An error means the
// outcome is unknown: the ring poisons itself and never retries.
type Persist func(Record) error

// RingSnapshot is an owned copy of the ring's retained state.
type RingSnapshot struct {
	RecordHigh uint64
	EntryHigh  uint64
	Pending    []Reservation
	Terminal   *ipc.Entry
	Tracks     []TrackSnapshot
}

// TrackSnapshot is one track's retained state. End is the cumulative end of its last
// committed part in ns, including retired segments.
type TrackSnapshot struct {
	Track    int
	Init     *ipc.Entry
	Phases   *ipc.Entry
	End      int64
	Segments []SegmentSnapshot
}

// SegmentSnapshot is one retained segment, dropped ones included. Live playlist callers
// filter Dropped; byte readers may still read a dropped segment until it is retired at or
// after DeadlineNS.
type SegmentSnapshot struct {
	MSN         int
	Duration    int64
	End         int64
	Closed      bool
	Dropped     bool
	DroppedAtNS int64
	DeadlineNS  int64
	Parts       []ipc.Entry
}
