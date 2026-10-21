//go:build linux

package supervisor

// The room timeline's durable state: one private append journal per room, timeline.journal in the
// room's 0700 directory, flocked through .timeline.lock. The file is the ASCII prefix "EGRT" and a
// big-endian uint32 version, then frames: one kind byte, the big-endian uint32 length of a JSON
// payload, the SHA-256 of kind, length and payload, and the payload. The first frame is a
// checkpoint (the whole roomState); every later frame is one roomRecord numbered gap-free from the
// checkpoint's RecordHigh+1. Compaction after compactAfter records rewrites the file as one fresh
// checkpoint through a synced temporary file, rename and directory sync. Only an incomplete final
// frame is ever trimmed; anything else wrong is fatal.

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"golang.org/x/sys/unix"
)

const (
	timelineFile    = "timeline.journal"
	timelineLock    = ".timeline.lock"
	timelineMagic   = "EGRT"
	timelineVersion = 1
	stateVersion    = 1

	tlFrameCheckpoint byte = 1
	tlFrameRecord     byte = 2
	tlFrameHead            = 1 + 4 + sha256.Size

	maxTimelineRecord = 1 << 20
	maxTimelineFile   = 64 << 20
	compactAfter      = 128
)

// Record operations.
const (
	opBind    = "bind"
	opAdvance = "advance"
	opRequest = "request"
	opHold    = "hold"
	opCancel  = "cancel"
	opCut     = "cut"
	opEnd     = "end"
	opWindow  = "window"
)

// roomState is the checkpoint: everything the room needs to resume. Bindings are the referenced
// generations in order; Segments the allocated room segments from the head, in order; Grace the
// byte references of segments already removed from the head.
type roomState struct {
	Version        int            `json:"version"`
	Room           string         `json:"room"`
	BootID         string         `json:"boot_id"`
	WindowNS       int64          `json:"window_ns"`
	RecordHigh     uint64         `json:"record_high"`
	NextGeneration uint64         `json:"next_generation"`
	LastAdvance    uint64         `json:"last_advance"`
	DSN            uint64         `json:"dsn"`
	Bindings       []RoomBinding  `json:"bindings"`
	Segments       []segState     `json:"segments"`
	Grace          []graceRef     `json:"grace"`
	Pending        *PendingSwitch `json:"pending,omitempty"`
	Ended          bool           `json:"ended"`
}

// segState is one allocated room segment.
type segState struct {
	RoomMSN       uint64 `json:"room_msn"`
	Generation    uint64 `json:"generation"`
	SourceMSN     int    `json:"source_msn"`
	Discontinuity bool   `json:"discontinuity"`
	StartUTCNS    *int64 `json:"start_utc_ns,omitempty"`
}

// graceRef keeps the bytes of a segment removed from the head referenced until ReleaseMonoNS of
// the boot it was removed in.
type graceRef struct {
	Generation    uint64   `json:"generation"`
	Key           EpochKey `json:"key"`
	SourceMSN     int      `json:"source_msn"`
	ReleaseMonoNS int64    `json:"release_mono_ns"`
}

// roomRecord is one durable mutation. Each op uses only its own payload.
type roomRecord struct {
	Version  int            `json:"version"`
	Room     string         `json:"room"`
	Seq      uint64         `json:"seq"`
	Op       string         `json:"op"`
	AtMonoNS int64          `json:"at_mono_ns"`
	BootID   string         `json:"boot_id,omitempty"`
	Bind     *RoomBinding   `json:"bind,omitempty"`
	Advance  *segState      `json:"advance,omitempty"`
	Request  *PendingSwitch `json:"request,omitempty"`
	Cut      *cutRecord     `json:"cut,omitempty"`
	Window   *windowRecord  `json:"window,omitempty"`
}

// cutRecord freezes a generation at exactly Ends; Failed marks a failure rather than a cut.
type cutRecord struct {
	Generation uint64    `json:"generation"`
	Ends       []PartRef `json:"ends"`
	Failed     bool      `json:"failed"`
	Reason     string    `json:"reason,omitempty"`
}

// windowRecord is a durable window change: segments below Head are removed, DSN and Grace are
// replaced, and the listed generations are no longer referenced.
type windowRecord struct {
	Head              uint64     `json:"head"`
	DSN               uint64     `json:"dsn"`
	Grace             []graceRef `json:"grace"`
	RemoveGenerations []uint64   `json:"remove_generations"`
}

func copyInt64Ptr(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// live returns the live generation: the last binding, or nil.
func (st *roomState) live() *RoomBinding {
	if len(st.Bindings) == 0 {
		return nil
	}
	return &st.Bindings[len(st.Bindings)-1]
}

func (st *roomState) binding(gen uint64) *RoomBinding {
	for i := range st.Bindings {
		if st.Bindings[i].Generation == gen {
			return &st.Bindings[i]
		}
	}
	return nil
}

// apply replays one record over the state; a record the state does not admit is invalid.
func (st *roomState) apply(rec roomRecord) error {
	switch {
	case rec.Version != stateVersion || rec.Room != st.Room:
		return fmt.Errorf("%w: record %d names version %d room %q", ErrTimelineInvalid, rec.Seq, rec.Version, rec.Room)
	case rec.Seq != st.RecordHigh+1:
		return fmt.Errorf("%w: record %d after %d", ErrTimelineInvalid, rec.Seq, st.RecordHigh)
	}
	switch rec.Op {
	case opBind:
		// A pending switch has reserved its incoming generation (NextGeneration already passed it);
		// the bind must consume exactly that generation and identity. With no pending switch the
		// bind must not reuse any generation already handed out.
		switch {
		case rec.Bind == nil:
			return fmt.Errorf("%w: bind record %d", ErrTimelineInvalid, rec.Seq)
		case st.Pending != nil:
			if rec.Bind.Generation != st.Pending.Incoming.Generation || rec.Bind.Key != st.Pending.Incoming.Key {
				return fmt.Errorf("%w: bind record %d does not consume the pending switch", ErrTimelineInvalid, rec.Seq)
			}
		case rec.Bind.Generation < st.NextGeneration:
			return fmt.Errorf("%w: bind record %d", ErrTimelineInvalid, rec.Seq)
		}
		b := *rec.Bind
		b.StartUTCNS = copyInt64Ptr(b.StartUTCNS)
		st.Bindings = append(st.Bindings, b)
		st.NextGeneration = rec.Bind.Generation + 1
		st.Pending = nil
	case opAdvance:
		b := (*RoomBinding)(nil)
		if rec.Advance != nil {
			b = st.binding(rec.Advance.Generation)
		}
		if rec.Advance == nil || rec.Advance.RoomMSN != st.LastAdvance+1 || b == nil {
			return fmt.Errorf("%w: advance record %d", ErrTimelineInvalid, rec.Seq)
		}
		if rec.Advance.StartUTCNS != nil {
			if rec.Advance.SourceMSN != b.StartMSN {
				return fmt.Errorf("%w: advance record %d sets start utc on segment %d, want %d", ErrTimelineInvalid, rec.Seq, rec.Advance.SourceMSN, b.StartMSN)
			}
			if b.StartUTCNS != nil && *b.StartUTCNS != *rec.Advance.StartUTCNS {
				return fmt.Errorf("%w: advance record %d changes start utc of generation %d", ErrTimelineInvalid, rec.Seq, b.Generation)
			}
			b.StartUTCNS = copyInt64Ptr(rec.Advance.StartUTCNS)
		}
		adv := *rec.Advance
		adv.StartUTCNS = copyInt64Ptr(adv.StartUTCNS)
		st.Segments = append(st.Segments, adv)
		st.LastAdvance = rec.Advance.RoomMSN
	case opRequest:
		if rec.Request == nil || st.Pending != nil || rec.Request.Incoming.Generation < st.NextGeneration {
			return fmt.Errorf("%w: request record %d", ErrTimelineInvalid, rec.Seq)
		}
		p := *rec.Request
		st.Pending = &p
		st.NextGeneration = p.Incoming.Generation + 1
	case opHold:
		if st.Pending == nil {
			return fmt.Errorf("%w: hold record %d without a pending switch", ErrTimelineInvalid, rec.Seq)
		}
		st.Pending.HoldStartMonoNS, st.Pending.HoldBootID = rec.AtMonoNS, rec.BootID
	case opCancel:
		if st.Pending == nil {
			return fmt.Errorf("%w: cancel record %d without a pending switch", ErrTimelineInvalid, rec.Seq)
		}
		st.Pending = nil
	case opCut:
		b := (*RoomBinding)(nil)
		if rec.Cut != nil {
			b = st.binding(rec.Cut.Generation)
		}
		if b == nil || b.Frozen() || len(rec.Cut.Ends) != RoomTracks {
			return fmt.Errorf("%w: cut record %d", ErrTimelineInvalid, rec.Seq)
		}
		b.Ends = slices.Clone(rec.Cut.Ends)
		b.Failed, b.FailReason = rec.Cut.Failed, rec.Cut.Reason
	case opEnd:
		st.Ended = true
	case opWindow:
		w := rec.Window
		if w == nil {
			return fmt.Errorf("%w: window record %d", ErrTimelineInvalid, rec.Seq)
		}
		st.Segments = slices.DeleteFunc(st.Segments, func(s segState) bool { return s.RoomMSN < w.Head })
		st.DSN, st.Grace = w.DSN, slices.Clone(w.Grace)
		st.Bindings = slices.DeleteFunc(st.Bindings, func(b RoomBinding) bool { return slices.Contains(w.RemoveGenerations, b.Generation) })
	default:
		return fmt.Errorf("%w: record %d op %q", ErrTimelineInvalid, rec.Seq, rec.Op)
	}
	st.RecordHigh = rec.Seq
	return nil
}

// roomJournal is the open journal file, its lock and directory.
type roomJournal struct {
	dirfd  int
	lockfd int
	fd     int
	size   int64
	count  int // records since the checkpoint
}

// openRoomJournal locks the room directory and opens or creates its journal, returning the state
// it holds. A fresh journal starts from fresh.
func openRoomJournal(dir string, fresh roomState) (*roomJournal, roomState, error) {
	dirfd, err := openPrivateDir(dir)
	if err != nil {
		return nil, roomState{}, fmt.Errorf("%w: %v", ErrTimelineInvalid, err)
	}
	j := &roomJournal{dirfd: dirfd, lockfd: -1, fd: -1}
	j.lockfd, err = unix.Openat(dirfd, timelineLock, unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		j.close()
		return nil, roomState{}, fmt.Errorf("%w: open lock: %v", ErrTimelineInvalid, err)
	}
	if err := unix.Flock(j.lockfd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		j.close()
		return nil, roomState{}, fmt.Errorf("%w: journal busy: %v", ErrTimelineInvalid, err)
	}
	j.fd, err = unix.Openat(dirfd, timelineFile, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		if err := j.rewrite(fresh); err != nil {
			j.close()
			return nil, roomState{}, err
		}
		return j, fresh, nil
	}
	if err != nil {
		j.close()
		return nil, roomState{}, fmt.Errorf("%w: open journal: %v", ErrTimelineInvalid, err)
	}
	st, err := j.load(fresh.Room)
	if err != nil {
		j.close()
		return nil, roomState{}, err
	}
	return j, st, nil
}

// load reads and replays the journal, trimming only an incomplete final record frame.
func (j *roomJournal) load(room string) (roomState, error) {
	var st unix.Stat_t
	if err := unix.Fstat(j.fd, &st); err != nil {
		return roomState{}, fmt.Errorf("%w: fstat journal: %v", ErrTimelineInvalid, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0o777 != 0o600 || st.Uid != uint32(unix.Geteuid()) || st.Nlink != 1 {
		return roomState{}, fmt.Errorf("%w: journal is not a private regular file", ErrTimelineInvalid)
	}
	if st.Size > maxTimelineFile {
		return roomState{}, fmt.Errorf("%w: journal of %d bytes exceeds %d", ErrTimelineInvalid, st.Size, maxTimelineFile)
	}
	buf := make([]byte, st.Size)
	if err := preadFull(j.fd, buf, 0); err != nil {
		return roomState{}, fmt.Errorf("%w: read journal: %v", ErrTimelineInvalid, err)
	}
	if len(buf) < 8 || string(buf[:4]) != timelineMagic || binary.BigEndian.Uint32(buf[4:8]) != timelineVersion {
		return roomState{}, fmt.Errorf("%w: journal prefix", ErrTimelineInvalid)
	}
	payload, pos, err := tlDecodeFrame(buf, 8, tlFrameCheckpoint)
	if err != nil {
		return roomState{}, fmt.Errorf("%w: checkpoint: %v", ErrTimelineInvalid, err)
	}
	var state roomState
	if err := tlDecodeJSON(payload, &state); err != nil {
		return roomState{}, err
	}
	if state.Version != stateVersion || state.Room != room {
		return roomState{}, fmt.Errorf("%w: checkpoint names version %d room %q", ErrTimelineInvalid, state.Version, state.Room)
	}
	good := pos
	for pos < len(buf) {
		payload, next, err := tlDecodeFrame(buf, pos, tlFrameRecord)
		if errors.Is(err, errTLIncomplete) {
			break
		}
		if err != nil {
			return roomState{}, fmt.Errorf("%w: record frame: %v", ErrTimelineInvalid, err)
		}
		var rec roomRecord
		if err := tlDecodeJSON(payload, &rec); err != nil {
			return roomState{}, err
		}
		if err := state.apply(rec); err != nil {
			return roomState{}, err
		}
		pos, good = next, next
		j.count++
	}
	if good < len(buf) {
		if err := unix.Ftruncate(j.fd, int64(good)); err != nil {
			return roomState{}, fmt.Errorf("%w: trim incomplete frame: %v", ErrTimelineInvalid, err)
		}
		if err := unix.Fsync(j.fd); err != nil {
			return roomState{}, fmt.Errorf("%w: sync trimmed journal: %v", ErrTimelineInvalid, err)
		}
	}
	j.size = int64(good)
	return state, nil
}

// append makes rec durable at the end of the journal.
func (j *roomJournal) append(rec roomRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("%w: encode record: %v", ErrTimelineRefusing, err)
	}
	if len(payload) > maxTimelineRecord {
		return fmt.Errorf("%w: record of %d bytes exceeds %d", ErrTimelineInvalid, len(payload), maxTimelineRecord)
	}
	frame := tlEncodeFrame(tlFrameRecord, payload)
	if err := pwriteFull(j.fd, frame, j.size); err != nil {
		return fmt.Errorf("%w: write record: %v", ErrTimelineRefusing, err)
	}
	if err := unix.Fsync(j.fd); err != nil {
		return fmt.Errorf("%w: sync record: %v", ErrTimelineRefusing, err)
	}
	j.size += int64(len(frame))
	j.count++
	return nil
}

// rewrite replaces the journal with one checkpoint of st: a private temporary file is written and
// synced, renamed over the journal and the directory synced; then the open descriptor is swapped.
func (j *roomJournal) rewrite(st roomState) error {
	payload, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("%w: encode checkpoint: %v", ErrTimelineRefusing, err)
	}
	if len(payload) > maxTimelineRecord {
		return fmt.Errorf("%w: checkpoint of %d bytes exceeds %d", ErrTimelineInvalid, len(payload), maxTimelineRecord)
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Errorf("%w: random temp name: %v", ErrTimelineRefusing, err)
	}
	name := ".timeline." + hex.EncodeToString(b[:]) + ".tmp"
	fd, err := unix.Openat(j.dirfd, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("%w: create checkpoint: %v", ErrTimelineRefusing, err)
	}
	data := make([]byte, 8)
	copy(data, timelineMagic)
	binary.BigEndian.PutUint32(data[4:], timelineVersion)
	data = append(data, tlEncodeFrame(tlFrameCheckpoint, payload)...)
	err = pwriteFull(fd, data, 0)
	if err == nil {
		err = unix.Fsync(fd)
	}
	if err == nil {
		err = unix.Renameat(j.dirfd, name, j.dirfd, timelineFile)
	}
	if err != nil {
		unix.Close(fd)
		_ = unix.Unlinkat(j.dirfd, name, 0)
		return fmt.Errorf("%w: publish checkpoint: %v", ErrTimelineRefusing, err)
	}
	if err := unix.Fsync(j.dirfd); err != nil {
		unix.Close(fd)
		return fmt.Errorf("%w: sync directory: %v", ErrTimelineRefusing, err)
	}
	if j.fd >= 0 {
		unix.Close(j.fd)
	}
	j.fd, j.size, j.count = fd, int64(len(data)), 0
	return nil
}

func (j *roomJournal) close() {
	for _, fd := range []int{j.fd, j.lockfd, j.dirfd} {
		if fd >= 0 {
			unix.Close(fd)
		}
	}
	j.fd, j.lockfd, j.dirfd = -1, -1, -1
}

var errTLIncomplete = errors.New("incomplete frame")

func tlEncodeFrame(kind byte, payload []byte) []byte {
	out := make([]byte, tlFrameHead+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	h := sha256.New()
	h.Write(out[:5])
	h.Write(payload)
	copy(out[5:tlFrameHead], h.Sum(nil))
	copy(out[tlFrameHead:], payload)
	return out
}

func tlDecodeFrame(buf []byte, pos int, kind byte) ([]byte, int, error) {
	rest := buf[pos:]
	if len(rest) > 0 && rest[0] != kind {
		return nil, 0, fmt.Errorf("frame kind %d at %d, want %d", rest[0], pos, kind)
	}
	if len(rest) >= 5 {
		if n := binary.BigEndian.Uint32(rest[1:5]); n > maxTimelineRecord {
			return nil, 0, fmt.Errorf("frame at %d is %d bytes, limit %d", pos, n, maxTimelineRecord)
		}
	}
	if len(rest) < tlFrameHead {
		return nil, 0, errTLIncomplete
	}
	n := int(binary.BigEndian.Uint32(rest[1:5]))
	if len(rest)-tlFrameHead < n {
		return nil, 0, errTLIncomplete
	}
	payload := rest[tlFrameHead : tlFrameHead+n]
	h := sha256.New()
	h.Write(rest[:5])
	h.Write(payload)
	if !bytes.Equal(h.Sum(nil), rest[5:tlFrameHead]) {
		return nil, 0, fmt.Errorf("frame at %d checksum mismatch", pos)
	}
	return payload, pos + tlFrameHead + n, nil
}

func tlDecodeJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: decode payload: %v", ErrTimelineInvalid, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: data after the payload", ErrTimelineInvalid)
	}
	return nil
}
