package ipc

// Presentation: the immutable presentation mapping of one source epoch, written once by the
// producer before its first part and read by the supervisor's room timelines. It maps every
// track's raw PTS to pipeline running time and the pipeline's running time origin to UTC, and
// names which host occupies which slot of the atlas and which room that host belongs to. It is a
// mapping declared by the producer, not a capture or lip-sync proof: nothing here is ever derived
// from publication time. The contract is contracts/presentation-v1.schema.json.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode"
	"unicode/utf8"
)

const (
	// PresentationVersion is the version of the Presentation document.
	PresentationVersion = 1

	// PresentationContractVersion is the version of contracts/presentation-v1.schema.json.
	PresentationContractVersion = "1.0.0"

	// MaxPresentationSize bounds one encoded presentation.
	MaxPresentationSize = 16 * 1024

	// MaxPresentationIdentity bounds a host or room identity in bytes.
	MaxPresentationIdentity = 256
)

// ErrInvalidPresentation reports a presentation that breaks the contract.
var ErrInvalidPresentation = errors.New("ipc: invalid presentation")

// TrackClock maps one track's raw PTS to pipeline running time: a sample at raw PTS p has running
// time RunningTimeOriginNS + (p - RawPTSOriginNS). The first video part's running time is P and
// an audio track's first part's running time is its own A (the phases entry), when the producer
// publishes phases.
type TrackClock struct {
	Track               int   `json:"track"`
	RawPTSOriginNS      int64 `json:"raw_pts_origin_ns"`
	RunningTimeOriginNS int64 `json:"running_time_origin_ns"`
}

// PresentationSlot names the host in one atlas slot and the room whose audio mix is that slot's
// own; the slot is the index in Presentation.Slots.
type PresentationSlot struct {
	HostID string `json:"host_id"`
	RoomID string `json:"room_id"`
}

// Presentation is the document. PipelineUTCNS is the UTC time of pipeline running time zero and
// UncertaintyNS its stated uncertainty; CropAnchor is the atlas crop anchor in [0,1]. Clocks has
// exactly one entry per configured track in track order and the three video clocks are
// identical. Slots has one entry for solo and two for 1v1.
type Presentation struct {
	Version       int                `json:"version"`
	Run           string             `json:"run"`
	Source        string             `json:"source"`
	Epoch         uint64             `json:"epoch"`
	Config        Config             `json:"config"`
	PipelineUTCNS int64              `json:"pipeline_utc_ns"`
	UncertaintyNS int64              `json:"uncertainty_ns"`
	CropAnchor    float64            `json:"crop_anchor"`
	Clocks        []TrackClock       `json:"clocks"`
	Slots         []PresentationSlot `json:"slots"`
}

var (
	presentationProps = []string{"version", "run", "source", "epoch", "config", "pipeline_utc_ns",
		"uncertainty_ns", "crop_anchor", "clocks", "slots"}
	clockProps = []string{"track", "raw_pts_origin_ns", "running_time_origin_ns"}
	slotProps  = []string{"host_id", "room_id"}
)

// ValidatePresentation reports whether p is a well-formed presentation of this contract.
func ValidatePresentation(p Presentation) error {
	if p.Version != PresentationVersion {
		return fmt.Errorf("%w: version %d, want %d", ErrInvalidPresentation, p.Version, PresentationVersion)
	}
	if err := checkIdentity("presentation", p.Run, p.Source); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPresentation, err)
	}
	if p.Epoch == 0 {
		return presentationf("epoch must be positive")
	}
	if err := p.Config.validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPresentation, err)
	}
	switch {
	case p.PipelineUTCNS <= 0:
		return presentationf("pipeline_utc_ns %d is not positive", p.PipelineUTCNS)
	case p.UncertaintyNS < 0:
		return presentationf("uncertainty_ns %d is negative", p.UncertaintyNS)
	case math.IsNaN(p.CropAnchor) || math.IsInf(p.CropAnchor, 0) || p.CropAnchor < 0 || p.CropAnchor > 1:
		return presentationf("crop_anchor %v is not in [0,1]", p.CropAnchor)
	case len(p.Clocks) != p.Config.Tracks:
		return presentationf("%d clocks, config has %d tracks", len(p.Clocks), p.Config.Tracks)
	}
	for i, c := range p.Clocks {
		switch {
		case c.Track != i:
			return presentationf("clock %d names track %d", i, c.Track)
		case c.RawPTSOriginNS < 0 || c.RunningTimeOriginNS < 0:
			return presentationf("clock %d origins %d/%d must not be negative", i, c.RawPTSOriginNS, c.RunningTimeOriginNS)
		case i <= TrackVideoLo && i > TrackVideoHi && c != (TrackClock{Track: i,
			RawPTSOriginNS: p.Clocks[TrackVideoHi].RawPTSOriginNS, RunningTimeOriginNS: p.Clocks[TrackVideoHi].RunningTimeOriginNS}):
			return presentationf("video clock %d differs from the video hi clock", i)
		}
	}
	want := 1
	if p.Config.Mode == Mode1v1 {
		want = 2
	}
	if len(p.Slots) != want {
		return presentationf("%d slots, mode %s has %d", len(p.Slots), p.Config.Mode, want)
	}
	hosts, rooms := map[string]bool{}, map[string]bool{}
	for i, s := range p.Slots {
		if err := checkPresentationIdentity(fmt.Sprintf("slot %d host_id", i), s.HostID); err != nil {
			return err
		}
		if err := checkPresentationIdentity(fmt.Sprintf("slot %d room_id", i), s.RoomID); err != nil {
			return err
		}
		if hosts[s.HostID] || rooms[s.RoomID] {
			return presentationf("slot %d repeats a host or room identity", i)
		}
		hosts[s.HostID], rooms[s.RoomID] = true, true
	}
	return nil
}

// checkPresentationIdentity requires a nonempty valid UTF-8 identity of at most
// MaxPresentationIdentity bytes without control characters.
func checkPresentationIdentity(what, s string) error {
	switch {
	case s == "":
		return presentationf("%s is empty", what)
	case len(s) > MaxPresentationIdentity:
		return presentationf("%s exceeds %d bytes", what, MaxPresentationIdentity)
	case !utf8.ValidString(s):
		return presentationf("%s is not valid UTF-8", what)
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return presentationf("%s contains a control character", what)
		}
	}
	return nil
}

// EncodePresentation validates p and returns its canonical JSON encoding: the snake_case
// properties in contract order, no insignificant whitespace, at most MaxPresentationSize bytes.
func EncodePresentation(p Presentation) ([]byte, error) {
	if err := ValidatePresentation(p); err != nil {
		return nil, err
	}
	data, err := json.Marshal(p)
	if err != nil {
		return nil, presentationf("encode: %v", err)
	}
	if len(data) > MaxPresentationSize {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", ErrInvalidPresentation, len(data), MaxPresentationSize)
	}
	return data, nil
}

// DecodePresentation parses one canonical presentation and validates it: valid UTF-8 of at most
// MaxPresentationSize bytes, exactly the schema's properties, nothing after the value and bytes
// identical to the canonical encoding of the decoded document. On any error the result is zero.
func DecodePresentation(data []byte) (Presentation, error) {
	switch {
	case len(data) == 0:
		return Presentation{}, presentationf("empty input")
	case len(data) > MaxPresentationSize:
		return Presentation{}, fmt.Errorf("%w: %d bytes exceeds %d", ErrInvalidPresentation, len(data), MaxPresentationSize)
	case !utf8.Valid(data):
		return Presentation{}, presentationf("input is not valid UTF-8")
	}
	if err := checkPresentationWire(data); err != nil {
		return Presentation{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var p Presentation
	if err := dec.Decode(&p); err != nil {
		return Presentation{}, presentationf("decode: %v", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Presentation{}, presentationf("data after the presentation")
	}
	canon, err := EncodePresentation(p)
	if err != nil {
		return Presentation{}, err
	}
	if !bytes.Equal(canon, data) {
		return Presentation{}, presentationf("input is not the canonical encoding")
	}
	return p, nil
}

// checkPresentationWire verifies the property shape of the document before typed decoding.
func checkPresentationWire(data []byte) error {
	root, err := wireObject(data, "presentation", presentationProps)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPresentation, err)
	}
	if _, err := wireObject(root["config"], "presentation config", configProps); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPresentation, err)
	}
	for _, list := range []struct {
		name  string
		props []string
	}{{"clocks", clockProps}, {"slots", slotProps}} {
		raw := root[list.name]
		if firstByte(raw) != '[' {
			return presentationf("%s is not an array", list.name)
		}
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return presentationf("%s: %v", list.name, err)
		}
		for i, item := range items {
			if _, err := wireObject(item, fmt.Sprintf("%s[%d]", list.name, i), list.props); err != nil {
				return fmt.Errorf("%w: %v", ErrInvalidPresentation, err)
			}
		}
	}
	return nil
}

// PresentationUTC returns the UTC time in ns of a sample of track trk at raw PTS pts:
// PipelineUTCNS + RunningTimeOriginNS + (pts - RawPTSOriginNS), with every step checked for
// overflow. p must be valid.
func PresentationUTC(p Presentation, trk int, pts int64) (int64, error) {
	if trk < 0 || trk >= len(p.Clocks) {
		return 0, presentationf("track %d has no clock", trk)
	}
	c := p.Clocks[trk]
	rel, ok := subNS(pts, c.RawPTSOriginNS)
	if !ok {
		return 0, presentationf("track %d pts %d relative to origin overflows", trk, pts)
	}
	rt, ok := addNS(c.RunningTimeOriginNS, rel)
	if !ok {
		return 0, presentationf("track %d running time overflows", trk)
	}
	utc, ok := addNS(p.PipelineUTCNS, rt)
	if !ok {
		return 0, presentationf("track %d utc overflows", trk)
	}
	return utc, nil
}

func addNS(a, b int64) (int64, bool) {
	s := a + b
	return s, (b >= 0 && s >= a) || (b < 0 && s < a)
}

func subNS(a, b int64) (int64, bool) {
	d := a - b
	return d, (b >= 0 && d <= a) || (b < 0 && d > a)
}

func presentationf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPresentation, fmt.Sprintf(format, args...))
}
