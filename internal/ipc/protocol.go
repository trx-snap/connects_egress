// Package ipc is the wire contract between the connects-egress worker and supervisor roles:
// packet types, validation and JSON serialization. These portable wire definitions open no
// sockets and touch no file descriptors; FDIdentity only describes a descriptor. The Linux
// transport in transport_linux.go is what sends and receives packets and descriptors.
//
// The contract is contracts/ipc-v1.schema.json. It starts from the G-R spike's envelope and field
// names but is a distinct protocol family, so spike version numbers mean nothing here. A packet is
// one JSON object of at most MaxPacketSize bytes. A worker opens every connection with a hello. On
// its setup connection, before it holds any descriptor, the hello names epoch 0 and the
// supervisor's welcome assigns the epoch and carries that epoch's descriptors; on a reconnect the
// hello names the assigned epoch and its descriptors, and the welcome carries the last sequence the
// supervisor holds for it. The worker then streams entries. Sequences, high-water marks and
// applied counts are per epoch. Checking a packet against the session it arrives in (a welcome
// against its hello, sequence continuity) belongs to the session owner, not to this package.
//
// Two layers share this package. The version 1 definitions (Hello, Welcome, Entry, Config,
// FDIdentity and their codec) are the canonical data contract and are unchanged; the version 1
// Packet envelope that streams entries without grants was never deployed. The deployed session
// protocol is IPC2 (publication.go, contracts/ipc-v2.schema.json): a version 2 envelope that
// carries the version 1 hello, welcome and entry unchanged and adds reserve, grant, publish and
// ack. IPC2 is the first deployed baseline, so there is no previous deployed major to accept.
package ipc

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

const (
	// Version is the data major carried by every hello, welcome and entry, inside the version 1
	// envelope and inside the deployed IPC2 envelope alike (see PublicationVersion). It is the
	// canonical data contract; 0 and unknown future majors are rejected. The deployed session
	// protocol is IPC2 and its envelope version 2 is what rolling deployments will one day
	// carry alongside a successor (plan §9.6, current plus previous deployed major); the version
	// 1 envelope was never deployed and takes no part in that.
	Version = 1

	// ContractVersion is the version of contracts/ipc-v1.schema.json.
	ContractVersion = "1.0.0"

	// MaxPacketSize bounds one encoded packet, descriptors included.
	MaxPacketSize = 16 * 1024
)

// Packet types.
const (
	TypeHello   = "hello"
	TypeWelcome = "welcome"
	TypeEntry   = "entry"
	TypeReject  = "reject"
)

// Entry kinds. Terminal ends a source's stream; there is no separate end-of-stream packet.
const (
	KindInit     = "init"
	KindPhases   = "phases"
	KindPart     = "part"
	KindTerminal = "terminal"
)

// Modes and codecs a Config may name.
const (
	ModeSolo = "solo"
	Mode1v1  = "1v1"

	CodecH264 = "h264"
	CodecHEVC = "hevc"
)

// Track counts: solo carries three video rungs and one audio mix, 1v1 adds the second audio mix.
const (
	TracksSolo = 4
	Tracks1v1  = 5
)

// Track ids. TrackTerminal is only valid on a terminal entry.
const (
	TrackVideoHi  = 0
	TrackVideoMid = 1
	TrackVideoLo  = 2
	TrackAudioA   = 3
	TrackAudioB   = 4 // 1v1 only

	TrackTerminal = -1
)

var (
	// ErrPacketTooLarge reports a packet, encoded or received, above MaxPacketSize.
	ErrPacketTooLarge = errors.New("ipc: packet too large")

	// ErrUnsupportedVersion reports a payload whose protocol version is not Version.
	ErrUnsupportedVersion = errors.New("ipc: unsupported protocol version")

	// ErrInvalidPacket reports a packet that is malformed or breaks the contract.
	ErrInvalidPacket = errors.New("ipc: invalid packet")
)

// FDIdentity describes one memfd: its track, its name and the identity and size the kernel reports
// for it. It is a description only; the portable wire definitions never receive, close or inspect
// descriptors, which is the Linux transport's work.
type FDIdentity struct {
	Track int    `json:"track"`
	Name  string `json:"name"`
	Dev   uint64 `json:"dev"`
	Ino   uint64 `json:"ino"`
	Size  int64  `json:"size"`
}

// Config is what the handshake compares: both sides must describe the same presentation. Tracks is
// TracksSolo for solo and Tracks1v1 for 1v1. Capacity is the size in bytes of each track's memfd,
// chosen by the caller.
type Config struct {
	Mode     string `json:"mode"`
	Codec    string `json:"codec"`
	Tracks   int    `json:"tracks"`
	Capacity int64  `json:"capacity"`
}

// Hello opens every worker connection. Epoch 0 marks the setup connection, which carries no
// descriptors; any other epoch is the assigned one and Tracks must then describe every configured
// track. High is the entry high-water sequence when the hello was built: the supervisor has
// recovered once it has applied through it.
type Hello struct {
	Version int          `json:"version"`
	Run     string       `json:"run"`
	Source  string       `json:"source"`
	Epoch   uint64       `json:"epoch"`
	PID     int          `json:"pid"`
	Config  Config       `json:"config"`
	Tracks  []FDIdentity `json:"tracks"`
	High    uint64       `json:"high"`
}

// Welcome answers an accepted hello. Epoch is the epoch assigned to or confirmed for the worker.
// Setup is true on the connection whose welcome carries that epoch's descriptors (Tracks, 4 or 5 of
// them); otherwise Tracks is empty. Applied is the last sequence of the epoch the supervisor
// holds; the worker streams from the next one.
type Welcome struct {
	Version int          `json:"version"`
	Run     string       `json:"run"`
	Source  string       `json:"source"`
	Epoch   uint64       `json:"epoch"`
	Setup   bool         `json:"setup"`
	Applied uint64       `json:"applied"`
	Tracks  []FDIdentity `json:"tracks"`
}

// Packet is one IPC message; exactly one payload is set, by Type. A reject carries only Reason.
type Packet struct {
	Type    string   `json:"type"` // hello, welcome, entry or reject
	Hello   *Hello   `json:"hello,omitempty"`
	Welcome *Welcome `json:"welcome,omitempty"`
	Entry   *Entry   `json:"entry,omitempty"`
	Reason  string   `json:"reason,omitempty"` // reject
}

// Entry is one record of a source's stream, immutable once published. Every entry identifies the
// protocol version, run, source, epoch and its sequence within the epoch (1-based).
// PublishedNS is the worker's own clock since its origin; PublishedMonoNS the host's
// CLOCK_MONOTONIC at the same moment. Numbers are never omitted so that a replayed entry decodes to
// exactly the value that was sent. Each kind uses only its own group of fields below and leaves
// the others zero.
type Entry struct {
	Version         int    `json:"version"`
	Run             string `json:"run"`
	Source          string `json:"source"`
	Epoch           uint64 `json:"epoch"`
	Seq             uint64 `json:"seq"`
	Kind            string `json:"kind"`  // init, phases, part or terminal
	Track           int    `json:"track"` // -1 for terminal
	PublishedNS     int64  `json:"published_ns"`
	PublishedMonoNS int64  `json:"published_mono_ns"`

	// init and part: the byte span in the track's memfd and its digest
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	SHA256 string `json:"sha256,omitempty"`
	Codecs string `json:"codecs,omitempty"` // init: CODECS derived from the actual configuration record

	// phases: P and the track's own A in ns of pipeline running time
	PhaseVideo int64 `json:"phase_video_ns"`
	PhaseAudio int64 `json:"phase_audio_ns"`

	// part
	MSN           int    `json:"msn"`
	Index         int    `json:"part"`
	PTS           int64  `json:"pts_ns"`
	DTS           int64  `json:"dts_ns"`
	Duration      int64  `json:"duration_ns"`
	Sequence      uint64 `json:"sequence"` // moof sequence number
	FragmentStart bool   `json:"fragment_start"`
	Independent   bool   `json:"independent"`
	Payload       int    `json:"payload_buffers"`
	FirstAU       int    `json:"first_au"`

	// terminal: the worker's verdict, never its native arrays
	Verified bool   `json:"verified"`
	Reason   string `json:"reason,omitempty"`
}

// ValidatePacket reports whether p is a well-formed packet of this contract: version 1, a
// recognized type, exactly the payload that type names, and valid fields. Errors wrap
// ErrUnsupportedVersion or ErrInvalidPacket.
func ValidatePacket(p Packet) error {
	n := 0
	for _, set := range []bool{p.Hello != nil, p.Welcome != nil, p.Entry != nil, p.Reason != ""} {
		if set {
			n++
		}
	}
	var own bool
	switch p.Type {
	case TypeHello:
		own = p.Hello != nil
	case TypeWelcome:
		own = p.Welcome != nil
	case TypeEntry:
		own = p.Entry != nil
	case TypeReject:
		own = p.Reason != ""
	default:
		return invalidf("unknown packet type %q", p.Type)
	}
	if !own || n != 1 {
		return invalidf("%s packet must carry exactly its own payload", p.Type)
	}
	switch p.Type {
	case TypeHello:
		return p.Hello.validate()
	case TypeWelcome:
		return p.Welcome.validate()
	case TypeEntry:
		return p.Entry.validate()
	}
	return checkText("reject reason", p.Reason, true)
}

func (h *Hello) validate() error {
	if h.Version != Version {
		return unsupported("hello", h.Version)
	}
	if err := checkIdentity("hello", h.Run, h.Source); err != nil {
		return err
	}
	if h.PID <= 0 {
		return invalidf("hello pid %d is not positive", h.PID)
	}
	if err := h.Config.validate(); err != nil {
		return err
	}
	if h.Epoch == 0 {
		if len(h.Tracks) != 0 {
			return invalidf("setup hello (epoch 0) carries %d descriptors", len(h.Tracks))
		}
		return nil
	}
	if err := validateDescriptors("hello", h.Tracks, h.Config.Tracks); err != nil {
		return err
	}
	for _, d := range h.Tracks {
		if d.Size != h.Config.Capacity {
			return invalidf("hello track %d size %d, want capacity %d", d.Track, d.Size, h.Config.Capacity)
		}
	}
	return nil
}

func (w *Welcome) validate() error {
	if w.Version != Version {
		return unsupported("welcome", w.Version)
	}
	if err := checkIdentity("welcome", w.Run, w.Source); err != nil {
		return err
	}
	if w.Epoch == 0 {
		return invalidf("welcome epoch must be positive")
	}
	if !w.Setup {
		if len(w.Tracks) != 0 {
			return invalidf("welcome without setup carries %d descriptors", len(w.Tracks))
		}
		return nil
	}
	if n := len(w.Tracks); n != TracksSolo && n != Tracks1v1 {
		return invalidf("setup welcome carries %d descriptors, want %d or %d", n, TracksSolo, Tracks1v1)
	}
	return validateDescriptors("welcome", w.Tracks, len(w.Tracks))
}

func (c Config) validate() error {
	var want int
	switch c.Mode {
	case ModeSolo:
		want = TracksSolo
	case Mode1v1:
		want = Tracks1v1
	default:
		return invalidf("unsupported mode %q", c.Mode)
	}
	switch {
	case c.Codec != CodecH264 && c.Codec != CodecHEVC:
		return invalidf("unsupported codec %q", c.Codec)
	case c.Tracks != want:
		return invalidf("mode %s has %d tracks, config says %d", c.Mode, want, c.Tracks)
	case c.Capacity <= 0:
		return invalidf("config capacity %d is not positive", c.Capacity)
	}
	return nil
}

// validateDescriptors requires ds to describe each of the count tracks exactly once, with a name
// and a positive size. count is 4 or 5.
func validateDescriptors(what string, ds []FDIdentity, count int) error {
	if len(ds) != count {
		return invalidf("%s carries %d descriptors, want %d", what, len(ds), count)
	}
	var seen [Tracks1v1]bool
	for _, d := range ds {
		switch {
		case d.Track < 0 || d.Track >= count:
			return invalidf("%s descriptor track %d is outside 0..%d", what, d.Track, count-1)
		case seen[d.Track]:
			return invalidf("%s describes track %d twice", what, d.Track)
		case d.Size <= 0:
			return invalidf("%s track %d size %d is not positive", what, d.Track, d.Size)
		}
		seen[d.Track] = true
		if err := checkText(fmt.Sprintf("%s track %d name", what, d.Track), d.Name, true); err != nil {
			return err
		}
	}
	return nil
}

// Groups of kind-specific entry fields.
const (
	fSpan = 1 << iota // offset, length, sha256
	fCodecs
	fPhases
	fPart
	fVerdict
)

// kindFields returns the field groups an entry kind may use, and whether the kind is supported.
func kindFields(kind string) (int, bool) {
	switch kind {
	case KindInit:
		return fSpan | fCodecs, true
	case KindPhases:
		return fPhases, true
	case KindPart:
		return fSpan | fPart, true
	case KindTerminal:
		return fVerdict, true
	}
	return 0, false
}

// fields returns the groups of kind-specific fields that e sets to a non-zero value.
func (e *Entry) fields() (set int) {
	if e.Offset != 0 || e.Length != 0 || e.SHA256 != "" {
		set |= fSpan
	}
	if e.Codecs != "" {
		set |= fCodecs
	}
	if e.PhaseVideo != 0 || e.PhaseAudio != 0 {
		set |= fPhases
	}
	if e.MSN != 0 || e.Index != 0 || e.PTS != 0 || e.DTS != 0 || e.Duration != 0 || e.Sequence != 0 ||
		e.FragmentStart || e.Independent || e.Payload != 0 || e.FirstAU != 0 {
		set |= fPart
	}
	if e.Verified || e.Reason != "" {
		set |= fVerdict
	}
	return set
}

func (e *Entry) validate() error {
	if e.Version != Version {
		return unsupported("entry", e.Version)
	}
	if err := checkIdentity("entry", e.Run, e.Source); err != nil {
		return err
	}
	allowed, supported := kindFields(e.Kind)
	switch {
	case e.Epoch == 0:
		return invalidf("entry epoch must be positive")
	case e.Seq == 0:
		return invalidf("entry seq must be positive")
	case !supported:
		return invalidf("unsupported entry kind %q", e.Kind)
	case e.Kind == KindTerminal && e.Track != TrackTerminal:
		return invalidf("terminal entry track is %d, want %d", e.Track, TrackTerminal)
	case e.Kind != KindTerminal && (e.Track < TrackVideoHi || e.Track > TrackAudioB):
		return invalidf("%s entry track %d is not a track id", e.Kind, e.Track)
	case e.fields()&^allowed != 0:
		return invalidf("%s entry sets fields that belong to another kind", e.Kind)
	}
	switch e.Kind {
	case KindInit:
		if err := e.validateSpan(); err != nil {
			return err
		}
		return checkText("init codecs", e.Codecs, true)
	case KindPhases:
		if e.PhaseVideo < 0 || e.PhaseAudio < 0 {
			return invalidf("phases %d/%d must not be negative", e.PhaseVideo, e.PhaseAudio)
		}
	case KindPart:
		if err := e.validateSpan(); err != nil {
			return err
		}
		switch {
		case e.MSN < 1:
			return invalidf("part msn %d is below 1", e.MSN)
		case e.Index < 0:
			return invalidf("part index %d is negative", e.Index)
		case e.FirstAU < 0:
			return invalidf("part first_au %d is negative", e.FirstAU)
		case e.Payload <= 0:
			return invalidf("part payload_buffers %d is not positive", e.Payload)
		case e.Duration <= 0:
			return invalidf("part duration %d is not positive", e.Duration)
		case e.PTS < 0 || e.DTS < 0:
			return invalidf("part pts/dts %d/%d must not be negative", e.PTS, e.DTS)
		}
	case KindTerminal:
		return checkText("terminal reason", e.Reason, false)
	}
	return nil
}

// validateSpan checks the byte span and digest of an init or part entry. The span is not checked
// against any memfd capacity: that belongs to whoever holds the descriptors.
func (e *Entry) validateSpan() error {
	switch {
	case e.Offset < 0:
		return invalidf("%s offset %d is negative", e.Kind, e.Offset)
	case e.Length <= 0:
		return invalidf("%s length %d is not positive", e.Kind, e.Length)
	case e.Offset > math.MaxInt64-e.Length:
		return invalidf("%s span %d+%d overflows", e.Kind, e.Offset, e.Length)
	case !validSHA256(e.SHA256):
		return invalidf("%s sha256 is not 64 lowercase hexadecimal characters", e.Kind)
	}
	return nil
}

func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func checkIdentity(what, run, source string) error {
	if err := checkText(what+" run", run, true); err != nil {
		return err
	}
	return checkText(what+" source", source, true)
}

// checkText rejects an empty string when one is required, and any string that is not valid UTF-8:
// JSON would silently rewrite it, and an identity must survive the wire unchanged.
func checkText(what, s string, required bool) error {
	switch {
	case required && s == "":
		return invalidf("%s is empty", what)
	case !utf8.ValidString(s):
		return invalidf("%s is not valid UTF-8", what)
	}
	return nil
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPacket, fmt.Sprintf(format, args...))
}

func unsupported(what string, version int) error {
	return fmt.Errorf("%w: %s version %d, want %d", ErrUnsupportedVersion, what, version, Version)
}
