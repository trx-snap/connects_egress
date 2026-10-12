package ipc

// These tests exercise the wire contract only: validation and JSON serialization. No descriptor is
// transferred or inspected here, so nothing in this file is evidence about SCM_RIGHTS or memfd
// behavior on Linux.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	testCapacity = 4 << 20
	testSHA      = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func descriptors(n int) []FDIdentity {
	ds := make([]FDIdentity, n)
	for i := range ds {
		ds[i] = FDIdentity{Track: i, Name: fmt.Sprintf("run-1.match-7.e2.t%d", i), Dev: 64, Ino: uint64(1000 + i), Size: testCapacity}
	}
	return ds
}

// hello builds a valid hello: a setup hello for epoch 0, else a reconnect hello with descriptors.
func hello(mode, codec string, epoch uint64) Packet {
	tracks := TracksSolo
	if mode == Mode1v1 {
		tracks = Tracks1v1
	}
	h := &Hello{
		Version: Version, Run: "run-1", Source: "match-7", Epoch: epoch, PID: 4242,
		Config: Config{Mode: mode, Codec: codec, Tracks: tracks, Capacity: testCapacity},
		Tracks: []FDIdentity{}, High: 17,
	}
	if epoch > 0 {
		h.Tracks = descriptors(tracks)
	}
	return Packet{Type: TypeHello, Hello: h}
}

// welcome builds a valid welcome: a setup welcome carrying n descriptors, else a plain one.
func welcome(setup bool, n int) Packet {
	w := &Welcome{Version: Version, Run: "run-1", Source: "match-7", Epoch: 2, Setup: setup, Tracks: []FDIdentity{}}
	if setup {
		w.Tracks = descriptors(n)
	} else {
		w.Applied = 41
	}
	return Packet{Type: TypeWelcome, Welcome: w}
}

func entry(kind string) Packet {
	e := &Entry{
		Version: Version, Run: "run-1", Source: "match-7", Epoch: 3, Seq: 9, Kind: kind,
		Track: TrackVideoMid, PublishedNS: 5_000, PublishedMonoNS: 123_456_789,
	}
	switch kind {
	case KindInit:
		e.Length, e.SHA256, e.Codecs = 812, testSHA, "avc1.64001f"
	case KindPhases:
		e.Track, e.PhaseVideo, e.PhaseAudio = TrackAudioA, 40_000_000, 21_333_333
	case KindPart:
		e.Offset, e.Length, e.SHA256 = 812, 65_536, testSHA
		e.MSN, e.Index, e.PTS, e.DTS, e.Duration, e.Sequence = 4, 2, 8_000_000_000, 7_960_000_000, 200_000_000, 17
		e.FragmentStart, e.Independent, e.Payload, e.FirstAU = true, true, 6, 12
	case KindTerminal:
		e.Track, e.Verified, e.Reason = TrackTerminal, true, "end of source"
	}
	return Packet{Type: TypeEntry, Entry: e}
}

func reject() Packet { return Packet{Type: TypeReject, Reason: "unsupported config"} }

type namedPacket struct {
	name string
	p    Packet
}

func validPackets() []namedPacket {
	return []namedPacket{
		{"hello setup solo", hello(ModeSolo, CodecH264, 0)},
		{"hello setup 1v1", hello(Mode1v1, CodecH264, 0)},
		{"hello reconnect solo", hello(ModeSolo, CodecH264, 2)},
		{"hello reconnect 1v1", hello(Mode1v1, CodecHEVC, 2)},
		{"welcome setup solo", welcome(true, TracksSolo)},
		{"welcome setup 1v1", welcome(true, Tracks1v1)},
		{"welcome reconnect", welcome(false, 0)},
		{"entry init", entry(KindInit)},
		{"entry phases", entry(KindPhases)},
		{"entry part", entry(KindPart)},
		{"entry terminal", entry(KindTerminal)},
		{"reject", reject()},
	}
}

// invalidCase is a valid packet and the one edit that makes it break the contract.
type invalidCase struct {
	name string
	p    Packet
	edit func(*Packet)
}

func invalidPackets() []invalidCase {
	h := func(f func(*Hello)) func(*Packet) { return func(p *Packet) { f(p.Hello) } }
	w := func(f func(*Welcome)) func(*Packet) { return func(p *Packet) { f(p.Welcome) } }
	e := func(f func(*Entry)) func(*Packet) { return func(p *Packet) { f(p.Entry) } }
	return []invalidCase{
		// envelope and payload mismatches
		{"empty type", reject(), func(p *Packet) { p.Type = "" }},
		{"unknown type", reject(), func(p *Packet) { p.Type = "bye" }},
		{"type case", hello(ModeSolo, CodecH264, 0), func(p *Packet) { p.Type = "HELLO" }},
		{"hello without payload", hello(ModeSolo, CodecH264, 0), func(p *Packet) { p.Hello = nil }},
		{"welcome without payload", welcome(false, 0), func(p *Packet) { p.Welcome = nil }},
		{"entry without payload", entry(KindInit), func(p *Packet) { p.Entry = nil }},
		{"reject without reason", reject(), func(p *Packet) { p.Reason = "" }},
		{"hello type with entry payload", hello(ModeSolo, CodecH264, 0), func(p *Packet) { p.Hello, p.Entry = nil, entry(KindInit).Entry }},
		{"hello with welcome too", hello(ModeSolo, CodecH264, 0), func(p *Packet) { p.Welcome = welcome(false, 0).Welcome }},
		{"hello with reason", hello(ModeSolo, CodecH264, 0), func(p *Packet) { p.Reason = "x" }},
		{"entry with reason", entry(KindInit), func(p *Packet) { p.Reason = "x" }},
		{"reject with hello", reject(), func(p *Packet) { p.Hello = hello(ModeSolo, CodecH264, 0).Hello }},

		// config
		{"mode empty", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Mode = "" })},
		{"mode tryon", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Mode = "tryon" })},
		{"mode quadro", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Mode = "quadro" })},
		{"codec av1", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Codec = "av1" })},
		{"codec empty", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Codec = "" })},
		{"solo with 5 tracks", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Tracks = 5 })},
		{"1v1 with 4 tracks", hello(Mode1v1, CodecH264, 0), h(func(h *Hello) { h.Config.Tracks = 4 })},
		{"zero tracks", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Tracks = 0 })},
		{"capacity zero", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Capacity = 0 })},
		{"capacity negative", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Config.Capacity = -1 })},

		// hello
		{"hello run empty", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Run = "" })},
		{"hello source empty", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Source = "" })},
		{"hello pid zero", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.PID = 0 })},
		{"setup hello with descriptors", hello(ModeSolo, CodecH264, 0), h(func(h *Hello) { h.Tracks = descriptors(4) })},
		{"reconnect hello without descriptors", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks = nil })},
		{"solo hello with 3 descriptors", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks = h.Tracks[:3] })},
		{"solo hello with 5 descriptors", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks = descriptors(5) })},
		{"1v1 hello with 4 descriptors", hello(Mode1v1, CodecH264, 2), h(func(h *Hello) { h.Tracks = descriptors(4) })},
		{"hello duplicate track, one missing", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks[1].Track = 0 })},
		{"hello track beyond solo", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks[3].Track = 4 })},
		{"hello track negative", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks[0].Track = -1 })},
		{"hello descriptor name empty", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks[2].Name = "" })},
		{"hello descriptor size zero", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks[2].Size = 0 })},
		{"hello descriptor size negative", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks[2].Size = -4 })},
		{"hello descriptor size not capacity", hello(ModeSolo, CodecH264, 2), h(func(h *Hello) { h.Tracks[2].Size++ })},

		// welcome
		{"welcome run empty", welcome(false, 0), w(func(w *Welcome) { w.Run = "" })},
		{"welcome source empty", welcome(false, 0), w(func(w *Welcome) { w.Source = "" })},
		{"welcome epoch zero", welcome(false, 0), w(func(w *Welcome) { w.Epoch = 0 })},
		{"welcome without setup carries descriptors", welcome(false, 0), w(func(w *Welcome) { w.Tracks = descriptors(4) })},
		{"setup welcome without descriptors", welcome(true, 4), w(func(w *Welcome) { w.Tracks = nil })},
		{"setup welcome with 3 descriptors", welcome(true, 4), w(func(w *Welcome) { w.Tracks = descriptors(3) })},
		{"setup welcome with 6 descriptors", welcome(true, 4), w(func(w *Welcome) { w.Tracks = descriptors(6) })},
		{"welcome duplicate track, one missing", welcome(true, 5), w(func(w *Welcome) { w.Tracks[4].Track = 0 })},
		{"welcome track beyond 4 descriptors", welcome(true, 4), w(func(w *Welcome) { w.Tracks[3].Track = 4 })},
		{"welcome descriptor name empty", welcome(true, 4), w(func(w *Welcome) { w.Tracks[0].Name = "" })},
		{"welcome descriptor size zero", welcome(true, 4), w(func(w *Welcome) { w.Tracks[0].Size = 0 })},

		// entry envelope
		{"entry run empty", entry(KindInit), e(func(e *Entry) { e.Run = "" })},
		{"entry source empty", entry(KindInit), e(func(e *Entry) { e.Source = "" })},
		{"entry epoch zero", entry(KindInit), e(func(e *Entry) { e.Epoch = 0 })},
		{"entry seq zero", entry(KindInit), e(func(e *Entry) { e.Seq = 0 })},
		{"entry kind eos", entry(KindInit), e(func(e *Entry) { e.Kind = "eos" })},
		{"entry kind empty", entry(KindInit), e(func(e *Entry) { e.Kind = "" })},
		{"init track terminal", entry(KindInit), e(func(e *Entry) { e.Track = TrackTerminal })},
		{"part track 5", entry(KindPart), e(func(e *Entry) { e.Track = 5 })},
		{"phases track -2", entry(KindPhases), e(func(e *Entry) { e.Track = -2 })},
		{"terminal track 0", entry(KindTerminal), e(func(e *Entry) { e.Track = 0 })},
		{"terminal track -2", entry(KindTerminal), e(func(e *Entry) { e.Track = -2 })},

		// init
		{"init offset negative", entry(KindInit), e(func(e *Entry) { e.Offset = -1 })},
		{"init length zero", entry(KindInit), e(func(e *Entry) { e.Length = 0 })},
		{"init length negative", entry(KindInit), e(func(e *Entry) { e.Length = -1 })},
		{"init sha empty", entry(KindInit), e(func(e *Entry) { e.SHA256 = "" })},
		{"init sha uppercase", entry(KindInit), e(func(e *Entry) { e.SHA256 = strings.ToUpper(testSHA) })},
		{"init sha 63 chars", entry(KindInit), e(func(e *Entry) { e.SHA256 = testSHA[1:] })},
		{"init sha 65 chars", entry(KindInit), e(func(e *Entry) { e.SHA256 += "0" })},
		{"init sha not hex", entry(KindInit), e(func(e *Entry) { e.SHA256 = "g" + testSHA[1:] })},
		{"init codecs empty", entry(KindInit), e(func(e *Entry) { e.Codecs = "" })},
		{"init with part fields", entry(KindInit), e(func(e *Entry) { e.Duration = 1 })},
		{"init with phases", entry(KindInit), e(func(e *Entry) { e.PhaseAudio = 1 })},
		{"init with verdict", entry(KindInit), e(func(e *Entry) { e.Verified = true })},

		// phases
		{"phases video negative", entry(KindPhases), e(func(e *Entry) { e.PhaseVideo = -1 })},
		{"phases audio negative", entry(KindPhases), e(func(e *Entry) { e.PhaseAudio = -1 })},
		{"phases with span", entry(KindPhases), e(func(e *Entry) { e.Offset = 1 })},
		{"phases with sha", entry(KindPhases), e(func(e *Entry) { e.SHA256 = testSHA })},
		{"phases with codecs", entry(KindPhases), e(func(e *Entry) { e.Codecs = "avc1" })},
		{"phases with part fields", entry(KindPhases), e(func(e *Entry) { e.Independent = true })},

		// part
		{"part offset negative", entry(KindPart), e(func(e *Entry) { e.Offset = -1 })},
		{"part length zero", entry(KindPart), e(func(e *Entry) { e.Length = 0 })},
		{"part sha uppercase", entry(KindPart), e(func(e *Entry) { e.SHA256 = strings.ToUpper(testSHA) })},
		{"part msn zero", entry(KindPart), e(func(e *Entry) { e.MSN = 0 })},
		{"part index negative", entry(KindPart), e(func(e *Entry) { e.Index = -1 })},
		{"part first_au negative", entry(KindPart), e(func(e *Entry) { e.FirstAU = -1 })},
		{"part payload zero", entry(KindPart), e(func(e *Entry) { e.Payload = 0 })},
		{"part duration zero", entry(KindPart), e(func(e *Entry) { e.Duration = 0 })},
		{"part pts negative", entry(KindPart), e(func(e *Entry) { e.PTS = -1 })},
		{"part dts negative", entry(KindPart), e(func(e *Entry) { e.DTS = -1 })},
		{"part with codecs", entry(KindPart), e(func(e *Entry) { e.Codecs = "avc1" })},
		{"part with phases", entry(KindPart), e(func(e *Entry) { e.PhaseVideo = 1 })},
		{"part with verdict", entry(KindPart), e(func(e *Entry) { e.Reason = "x" })},

		// terminal
		{"terminal with span", entry(KindTerminal), e(func(e *Entry) { e.Offset = 1 })},
		{"terminal with sha", entry(KindTerminal), e(func(e *Entry) { e.SHA256 = testSHA })},
		{"terminal with part fields", entry(KindTerminal), e(func(e *Entry) { e.MSN = 1 })},
		{"terminal with phases", entry(KindTerminal), e(func(e *Entry) { e.PhaseVideo = 1 })},
	}
}

func mustEncode(t *testing.T, p Packet) []byte {
	t.Helper()
	data, err := EncodePacket(p)
	if err != nil {
		t.Fatalf("EncodePacket: %v", err)
	}
	return data
}

func TestRoundTrip(t *testing.T) {
	for _, tc := range validPackets() {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidatePacket(tc.p); err != nil {
				t.Fatalf("ValidatePacket: %v", err)
			}
			data := mustEncode(t, tc.p)
			got, err := DecodePacket(data)
			if err != nil {
				t.Fatalf("DecodePacket(%s): %v", data, err)
			}
			if !reflect.DeepEqual(got, tc.p) {
				t.Fatalf("round trip changed the packet:\n got %+v\nwant %+v", got, tc.p)
			}
			if again := mustEncode(t, got); !bytes.Equal(again, data) {
				t.Fatalf("re-encoding differs:\n%s\n%s", again, data)
			}
		})
	}
}

func TestConfigCombinations(t *testing.T) {
	for _, mode := range []string{ModeSolo, Mode1v1} {
		for _, codec := range []string{CodecH264, CodecHEVC} {
			for _, epoch := range []uint64{0, 1} {
				p := hello(mode, codec, epoch)
				p.Hello.Source = "source-" + mode + "-" + codec // callers choose the source
				got, err := DecodePacket(mustEncode(t, p))
				if err != nil || !reflect.DeepEqual(got, p) {
					t.Errorf("%s/%s epoch %d: got %+v, %v", mode, codec, epoch, got, err)
				}
			}
		}
	}
}

func TestUint64Exact(t *testing.T) {
	for _, v := range []uint64{1<<53 + 1, 1<<63 + 1, math.MaxUint64} {
		h := hello(ModeSolo, CodecH264, v)
		h.Hello.High = v
		for i := range h.Hello.Tracks {
			h.Hello.Tracks[i].Dev, h.Hello.Tracks[i].Ino = v, v-1
		}
		w := welcome(true, TracksSolo)
		w.Welcome.Epoch, w.Welcome.Applied = v, v
		e := entry(KindPart)
		e.Entry.Epoch, e.Entry.Seq, e.Entry.Sequence = v, v, v
		for _, p := range []Packet{h, w, e} {
			data := mustEncode(t, p)
			if !bytes.Contains(data, []byte(strconv.FormatUint(v, 10))) {
				t.Errorf("%d missing from %s", v, data)
			}
			got, err := DecodePacket(data)
			if err != nil || !reflect.DeepEqual(got, p) {
				t.Errorf("%d: %s did not round trip: %v", v, p.Type, err)
			}
		}
	}
}

func TestNilDescriptorListsEncodeAsArrays(t *testing.T) {
	h, w := hello(ModeSolo, CodecH264, 0), welcome(false, 0)
	h.Hello.Tracks, w.Welcome.Tracks = nil, nil
	for _, p := range []Packet{h, w} {
		data := mustEncode(t, p)
		if !bytes.Contains(data, []byte(`"tracks":[]`)) || bytes.Contains(data, []byte("null")) {
			t.Errorf("%s: want an empty array, got %s", p.Type, data)
		}
		if _, err := DecodePacket(data); err != nil {
			t.Errorf("%s: %v", p.Type, err)
		}
	}
	if h.Hello.Tracks != nil || w.Welcome.Tracks != nil {
		t.Error("EncodePacket modified the caller's packet")
	}
}

func TestInvalidPackets(t *testing.T) {
	for _, tc := range invalidPackets() {
		t.Run(tc.name, func(t *testing.T) {
			tc.edit(&tc.p)
			if err := ValidatePacket(tc.p); !errors.Is(err, ErrInvalidPacket) {
				t.Fatalf("ValidatePacket = %v, want ErrInvalidPacket", err)
			}
			if data, err := EncodePacket(tc.p); !errors.Is(err, ErrInvalidPacket) || data != nil {
				t.Fatalf("EncodePacket = %q, %v, want nil and ErrInvalidPacket", data, err)
			}
			// Bypass EncodePacket to prove decoding validates too.
			data, err := json.Marshal(tc.p)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := DecodePacket(data); !errors.Is(err, ErrInvalidPacket) || !reflect.DeepEqual(got, Packet{}) {
				t.Fatalf("DecodePacket(%s) = %+v, %v, want zero packet and ErrInvalidPacket", data, got, err)
			}
		})
	}
}

// JSON would silently rewrite invalid UTF-8, so no string may carry it onto the wire.
func TestInvalidUTF8(t *testing.T) {
	const bad = "run-\xff"
	for _, tc := range []invalidCase{
		{"hello run", hello(ModeSolo, CodecH264, 0), func(p *Packet) { p.Hello.Run = bad }},
		{"hello descriptor name", hello(ModeSolo, CodecH264, 2), func(p *Packet) { p.Hello.Tracks[0].Name = bad }},
		{"welcome source", welcome(false, 0), func(p *Packet) { p.Welcome.Source = bad }},
		{"entry run", entry(KindPart), func(p *Packet) { p.Entry.Run = bad }},
		{"init codecs", entry(KindInit), func(p *Packet) { p.Entry.Codecs = bad }},
		{"terminal reason", entry(KindTerminal), func(p *Packet) { p.Entry.Reason = bad }},
		{"reject reason", reject(), func(p *Packet) { p.Reason = bad }},
	} {
		tc.edit(&tc.p)
		if err := ValidatePacket(tc.p); !errors.Is(err, ErrInvalidPacket) {
			t.Errorf("%s: ValidatePacket = %v, want ErrInvalidPacket", tc.name, err)
		}
	}
}

func TestSpans(t *testing.T) {
	spans := []struct {
		offset, length int64
		ok             bool
	}{
		{0, 1, true},
		{0, math.MaxInt64, true},
		{math.MaxInt64 - 1, 1, true},
		{math.MaxInt64, 1, false},
		{1, math.MaxInt64, false},
		{math.MaxInt64 - 1, 2, false},
		{math.MaxInt64, math.MaxInt64, false},
		{math.MaxInt64 / 2, math.MaxInt64/2 + 2, false},
		{-1, 1, false},
		{0, 0, false},
		{0, -1, false},
		{0, math.MinInt64, false},
		{math.MinInt64, 1, false},
	}
	for _, kind := range []string{KindInit, KindPart} {
		for _, s := range spans {
			p := entry(kind)
			p.Entry.Offset, p.Entry.Length = s.offset, s.length
			err := ValidatePacket(p)
			if s.ok != (err == nil) || (err != nil && !errors.Is(err, ErrInvalidPacket)) {
				t.Errorf("%s span %d+%d: ok=%v, got %v", kind, s.offset, s.length, s.ok, err)
			}
		}
	}
}

func TestUnsupportedVersion(t *testing.T) {
	set := func(p *Packet, v int) {
		switch p.Type {
		case TypeHello:
			p.Hello.Version = v
		case TypeWelcome:
			p.Welcome.Version = v
		case TypeEntry:
			p.Entry.Version = v
		}
	}
	for _, v := range []int{0, 2, -1, 1 << 40} {
		for _, tc := range validPackets() {
			if tc.p.Type == TypeReject {
				continue // a reject carries no version of its own
			}
			set(&tc.p, v)
			if err := ValidatePacket(tc.p); !errors.Is(err, ErrUnsupportedVersion) || errors.Is(err, ErrInvalidPacket) {
				t.Errorf("%s version %d: ValidatePacket = %v, want ErrUnsupportedVersion", tc.name, v, err)
			}
			if data, err := EncodePacket(tc.p); !errors.Is(err, ErrUnsupportedVersion) || data != nil {
				t.Errorf("%s version %d: EncodePacket = %q, %v", tc.name, v, data, err)
			}
		}
	}

	// The version is checked before anything else in the payload, which may differ in another major.
	p := hello(ModeSolo, CodecH264, 0)
	p.Hello.Version, p.Hello.Run = 2, ""
	if err := ValidatePacket(p); !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("version 2 with an empty run: %v, want ErrUnsupportedVersion", err)
	}

	// The spike's version 2 is not a production major.
	for _, tc := range validPackets() {
		if tc.p.Type == TypeReject {
			continue
		}
		data := mustEncode(t, tc.p)
		for _, v := range []string{"0", "2"} {
			changed := bytes.Replace(data, []byte(`"version":1`), []byte(`"version":`+v), 1)
			if got, err := DecodePacket(changed); !errors.Is(err, ErrUnsupportedVersion) || !reflect.DeepEqual(got, Packet{}) {
				t.Errorf("%s decoded with version %s: %+v, %v", tc.name, v, got, err)
			}
		}
	}
}

func TestDecodeRejectsMalformedInput(t *testing.T) {
	good := `{"type":"reject","reason":"no"}`
	helloWire := string(mustEncode(t, hello(ModeSolo, CodecH264, 0)))
	welcomeWire := string(mustEncode(t, welcome(false, 0)))
	inputs := []struct{ name, in string }{
		{"empty", ""},
		{"whitespace", " \n\t"},
		{"null", "null"},
		{"not json", "hello"},
		{"truncated", good[:len(good)-1]},
		{"array", "[]"},
		{"number", "1"},
		{"trailing object", good + good},
		{"trailing garbage", good + "x"},
		{"trailing comma", good + ","},
		{"trailing null", good + " null"},
		{"trailing brace", good + "}"},
		{"unknown top-level field", `{"type":"reject","reason":"no","extra":1}`},
		{"unknown payload field", strings.Replace(helloWire, `"version":1`, `"bogus":true,"version":1`, 1)},
		{"unknown nested field", strings.Replace(helloWire, `"mode":"solo"`, `"mode":"solo","bogus":1`, 1)},
		{"wrong field type", `{"type":"reject","reason":5}`},
		{"reason null", `{"type":"reject","reason":null}`},
		{"missing payload", `{"type":"reject"}`},
		{"negative epoch", strings.Replace(welcomeWire, `"epoch":2`, `"epoch":-2`, 1)},
		{"fractional epoch", strings.Replace(welcomeWire, `"epoch":2`, `"epoch":2.5`, 1)},
		{"exponent epoch", strings.Replace(welcomeWire, `"epoch":2`, `"epoch":2e0`, 1)},
		{"string epoch", strings.Replace(welcomeWire, `"epoch":2`, `"epoch":"2"`, 1)},
		{"epoch above uint64", strings.Replace(welcomeWire, `"epoch":2`, `"epoch":18446744073709551616`, 1)},
	}
	for _, tc := range inputs {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := DecodePacket([]byte(tc.in)); !errors.Is(err, ErrInvalidPacket) || !reflect.DeepEqual(got, Packet{}) {
				t.Fatalf("DecodePacket(%q) = %+v, %v, want zero packet and ErrInvalidPacket", tc.in, got, err)
			}
		})
	}
	if _, err := DecodePacket(nil); !errors.Is(err, ErrInvalidPacket) {
		t.Errorf("DecodePacket(nil) = %v, want ErrInvalidPacket", err)
	}
	if got, err := DecodePacket([]byte(good + " \n")); err != nil || got.Reason != "no" {
		t.Errorf("trailing whitespace: %+v, %v", got, err)
	}
}

func TestPacketSizeLimit(t *testing.T) {
	base := mustEncode(t, Packet{Type: TypeReject, Reason: "x"})
	atLimit := Packet{Type: TypeReject, Reason: strings.Repeat("x", MaxPacketSize-len(base)+1)}
	data := mustEncode(t, atLimit)
	if len(data) != MaxPacketSize {
		t.Fatalf("encoded %d bytes, want exactly %d", len(data), MaxPacketSize)
	}
	if got, err := DecodePacket(data); err != nil || got.Reason != atLimit.Reason {
		t.Fatalf("a packet of exactly MaxPacketSize bytes must decode: %v", err)
	}

	over := Packet{Type: TypeReject, Reason: atLimit.Reason + "x"}
	if data, err := EncodePacket(over); !errors.Is(err, ErrPacketTooLarge) || data != nil {
		t.Errorf("EncodePacket over the limit = %d bytes, %v, want nil and ErrPacketTooLarge", len(data), err)
	}
	for name, in := range map[string][]byte{
		"one byte over": append(data[:len(data):len(data)], ' '),
		"zeroes":        make([]byte, MaxPacketSize+1),
		"huge":          bytes.Repeat([]byte("{"), 1<<20),
	} {
		if got, err := DecodePacket(in); !errors.Is(err, ErrPacketTooLarge) || !reflect.DeepEqual(got, Packet{}) {
			t.Errorf("%s: DecodePacket = %+v, %v, want zero packet and ErrPacketTooLarge", name, got, err)
		}
	}
}

// TestSchemaMatchesGoTypes keeps contracts/ipc-v1.schema.json aligned with the Go wire types: the
// same JSON field names, the same required fields, the same enumerations and versions.
func TestSchemaMatchesGoTypes(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/ipc-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("schema is not JSON: %v", err)
	}
	if got := schema["x-contract-version"]; got != ContractVersion {
		t.Errorf("x-contract-version = %v, want %s", got, ContractVersion)
	}
	if got := schema["x-max-packet-size"]; got != float64(MaxPacketSize) {
		t.Errorf("x-max-packet-size = %v, want %d", got, MaxPacketSize)
	}

	at := func(path ...string) map[string]any {
		cur := schema
		for _, key := range path {
			next, ok := cur[key].(map[string]any)
			if !ok {
				t.Fatalf("schema has no %s", strings.Join(path, "/"))
			}
			cur = next
		}
		return cur
	}
	keys := func(m map[string]any) []string {
		var ks []string
		for k := range m {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		return ks
	}
	strs := func(v any) []string {
		var ss []string
		for _, x := range v.([]any) {
			ss = append(ss, x.(string))
		}
		sort.Strings(ss)
		return ss
	}
	// tags returns the JSON names of a struct's fields, and those that are never omitted.
	tags := func(v any) (all, required []string) {
		typ := reflect.TypeOf(v)
		for i := 0; i < typ.NumField(); i++ {
			name, opts, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
			all = append(all, name)
			if !strings.Contains(opts, "omitempty") {
				required = append(required, name)
			}
		}
		sort.Strings(all)
		sort.Strings(required)
		return all, required
	}

	for name, v := range map[string]any{
		"hello": Hello{}, "welcome": Welcome{}, "entry": Entry{}, "config": Config{}, "fdIdentity": FDIdentity{},
	} {
		def := at("$defs", name)
		all, required := tags(v)
		if got := keys(def["properties"].(map[string]any)); !reflect.DeepEqual(got, all) {
			t.Errorf("%s properties = %v, Go fields %v", name, got, all)
		}
		if got := strs(def["required"]); !reflect.DeepEqual(got, required) {
			t.Errorf("%s required = %v, Go always-present fields %v", name, got, required)
		}
	}

	packetProps := map[string]any{}
	for _, name := range []string{"helloPacket", "welcomePacket", "entryPacket", "rejectPacket"} {
		for k := range at("$defs", name, "properties") {
			packetProps[k] = true
		}
	}
	if all, _ := tags(Packet{}); !reflect.DeepEqual(keys(packetProps), all) {
		t.Errorf("packet properties = %v, Go fields %v", keys(packetProps), all)
	}

	for _, name := range []string{"hello", "welcome", "entry"} {
		if got := at("$defs", name, "properties", "version")["const"]; got != float64(Version) {
			t.Errorf("%s version const = %v, want %d", name, got, Version)
		}
	}
	for _, c := range []struct {
		def, field string
		want       []string
	}{
		{"config", "mode", []string{ModeSolo, Mode1v1}},
		{"config", "codec", []string{CodecH264, CodecHEVC}},
		{"entry", "kind", []string{KindInit, KindPhases, KindPart, KindTerminal}},
	} {
		sort.Strings(c.want)
		if got := strs(at("$defs", c.def, "properties", c.field)["enum"]); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s.%s enum = %v, want %v", c.def, c.field, got, c.want)
		}
	}
}
