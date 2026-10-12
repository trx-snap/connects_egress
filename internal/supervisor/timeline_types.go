package supervisor

// Public types of the room playback timeline (plan §5.3, §9.5): the durable, continuously numbered
// view one room's playlists are rendered from. A Timeline borrows Sources through an EpochResolver
// and never closes or signals them; it owns only its own journal, pins and view.

import (
	"errors"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

var (
	// ErrTimelineInvalid reports options, arguments, a journal, a presentation or a Source the
	// timeline refuses: it does not describe this room, breaks a rule, or rewinds exposed media.
	ErrTimelineInvalid = errors.New("supervisor: timeline invalid")

	// ErrTimelineNotReady reports a mutation whose preconditions the Source does not yet meet: no
	// presentation, no init, or a start segment that has not been published.
	ErrTimelineNotReady = errors.New("supervisor: timeline not ready")

	// ErrTimelineRefusing reports a timeline whose persistence outcome is ambiguous: mutations are
	// refused, already known references and counters stay observable.
	ErrTimelineRefusing = errors.New("supervisor: timeline refuses mutations")

	// ErrTimelineClosed reports an operation on a closed Timeline.
	ErrTimelineClosed = errors.New("supervisor: timeline closed")
)

const (
	// DefaultRoomWindow is the room's live window of coherent media duration.
	DefaultRoomWindow = 24 * time.Second

	// SwitchHoldLimit bounds, in monotonic time, how long a room waits at a cut for the incoming
	// source before the pending switch is cancelled and the outgoing source resumes.
	SwitchHoldLimit = 3 * time.Second

	// SegmentNominal is the nominal segment duration a cut time is derived from (plan §7.5).
	SegmentNominal = 2 * time.Second

	// RoomTracks is the number of tracks a room presents: three video rungs and its own audio mix.
	RoomTracks = 4
)

// EpochKey names one source epoch.
type EpochKey struct {
	Run    string `json:"run"`
	Source string `json:"source"`
	Epoch  uint64 `json:"epoch"`
}

// TimelineOptions describe the room: Directory is its private 0700 directory holding the journal,
// Room its identity (the room_id of a presentation slot), Window the live window of media
// duration, DefaultRoomWindow when zero.
type TimelineOptions struct {
	Directory string
	Room      string
	Window    time.Duration
}

// PartRef names the last exposed part of one track: segment MSN and part index in the source's
// numbering. A zero MSN means nothing was exposed on that track.
type PartRef struct {
	MSN   int `json:"msn"`
	Index int `json:"index"`
}

// RoomBinding is one generation of the room: the epoch it plays, the scope, configuration,
// retention policy and descriptor identities it was bound with, the presentation (layout and
// clocks), the room's slot and audio track, the init of every room track and the mapping of the
// source's StartMSN to the room's FirstRoomMSN. Ends is set once the generation is frozen by a
// cut, a failure or End: exactly the last exposed part of each room track.
type RoomBinding struct {
	Generation    uint64           `json:"generation"`
	Key           EpochKey         `json:"key"`
	Config        ipc.Config       `json:"config"`
	WindowNS      int64            `json:"window_ns"`
	MaxSegmentNS  int64            `json:"max_segment_ns"`
	Tracks        []ipc.FDIdentity `json:"tracks"`
	Presentation  ipc.Presentation `json:"presentation"`
	Slot          int              `json:"slot"`
	AudioTrack    int              `json:"audio_track"`
	Inits         []ipc.Entry      `json:"inits"`
	StartMSN      int              `json:"start_msn"`
	FirstRoomMSN  uint64           `json:"first_room_msn"`
	Discontinuity bool             `json:"discontinuity"`
	Ends          []PartRef        `json:"ends,omitempty"`
	Failed        bool             `json:"failed"`
	FailReason    string           `json:"fail_reason,omitempty"`
}

// Frozen reports whether the generation exposes nothing further.
func (b *RoomBinding) Frozen() bool { return b.Ends != nil }

// SourceTrack returns the source track behind room track i.
func (b *RoomBinding) SourceTrack(i int) int {
	if i < ipc.TrackAudioA {
		return i
	}
	return b.AudioTrack
}

// PendingSwitch is a planned switch: the incoming binding (its Inits are filled at commit), the
// cut time in UTC ns derived from the incoming video anchor, and the monotonic time the hold
// began, zero while the outgoing source has not reached the cut.
type PendingSwitch struct {
	Incoming        RoomBinding `json:"incoming"`
	CutUTCNS        int64       `json:"cut_utc_ns"`
	HoldStartMonoNS int64       `json:"hold_start_mono_ns"`
	HoldBootID      string      `json:"hold_boot_id,omitempty"`
}

// RoomTrack is the exposed prefix of one track of one room segment: the source track, the exposed
// parts in index order, whether the prefix is final, the UTC of its first part, the UTC end of its
// last exposed part and its exposed duration. Last is the final part index when Closed, else -1; a
// whole-segment reference always names it.
type RoomTrack struct {
	Track      int
	Parts      []ipc.Entry
	Closed     bool
	Last       int
	UTCNS      int64
	EndUTCNS   int64
	DurationNS int64
}

// RoomSegment is one room segment: its continuous room MSN, the generation and source segment it
// maps to, whether it opens a discontinuity, and its RoomTracks tracks.
type RoomSegment struct {
	MSN           uint64
	Generation    uint64
	Key           EpochKey
	SourceMSN     int
	Discontinuity bool
	Tracks        [RoomTracks]RoomTrack
}

// TimelineSnapshot is an owned, immutable view of the room. Segments are the exposed segments in
// the window in MSN order; DSN is the discontinuity sequence of the first; NextMSN the room MSN
// the next segment receives. Bindings are the generations still referenced (Live is the last
// non-pending one, nil when the room has no live generation).
type TimelineSnapshot struct {
	Room       string
	Bindings   []RoomBinding
	Live       *RoomBinding
	Segments   []RoomSegment
	DSN        uint64
	NextMSN    uint64
	Pending    *PendingSwitch
	Ended      bool
	Failed     bool
	FailReason string
	Refusing   bool
	Closed     bool
}
