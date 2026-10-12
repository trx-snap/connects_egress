package partstore

import "github.com/connectsapp/connects-egress/internal/ipc"

// CheckpointVersion is the version of the Ring's Checkpoint, independent of RecordVersion.
const CheckpointVersion = 1

// Checkpoint is a self-contained, owned copy of exactly the retained state a Ring needs to
// resume: scope, effective config and policy, both high-waters, the latest clock, the terminal
// and every track. It carries no record history and makes no claim about the store's bytes;
// RestoreCheckpoint digest-checks them. The owner that compacts durable history must make the
// checkpoint durable atomically before removing the record prefix it replaces, and must keep
// every record after RecordHigh as the suffix: a checkpoint without its suffix, or a prefix
// removed before the checkpoint is durable, loses mutations.
type Checkpoint struct {
	Version    int
	Run        string
	Source     string
	Epoch      uint64
	Config     ipc.Config
	Policy     Policy
	RecordHigh uint64
	EntryHigh  uint64
	ClockNS    int64
	Terminal   *ipc.Entry
	Tracks     []TrackCheckpoint
}

// TrackCheckpoint is one track's retained state: its init, phases, pending grant, first-fit
// cursor and retained segments in MSN order, dropped ones included. The track's last part and
// cumulative end are derived from the newest segment on restore.
type TrackCheckpoint struct {
	Track    int
	Init     *ipc.Entry
	Phases   *ipc.Entry
	Pending  *Reservation
	Cursor   int64
	Segments []SegmentSnapshot
}
