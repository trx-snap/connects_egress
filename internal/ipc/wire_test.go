package ipc

// Regression tests for the wire shape DecodePacket enforces before typed decoding: exact
// case-sensitive property names, required presence, no null, and the properties each entry kind
// carries. They cover the contract only; no descriptor is transferred, so they are no evidence
// about SCM_RIGHTS or memfd behavior on Linux.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

// wireEdit decodes a wire packet into generic maps and arrays, applies edit and re-encodes it.
// Numbers pass through float64, so it is only for the small values of the test packets.
func wireEdit(t *testing.T, raw []byte, edit func(doc map[string]any)) []byte {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	edit(doc)
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// wireAt follows path through v: a string selects an object property, an int an array element.
func wireAt(v any, path ...any) any {
	for _, step := range path {
		switch step := step.(type) {
		case string:
			v = v.(map[string]any)[step]
		case int:
			v = v.([]any)[step]
		}
	}
	return v
}

// wireDrop removes the property at path, whose last element names it.
func wireDrop(t *testing.T, raw []byte, path ...any) []byte {
	t.Helper()
	return wireEdit(t, raw, func(doc map[string]any) {
		delete(wireAt(doc, path[:len(path)-1]...).(map[string]any), path[len(path)-1].(string))
	})
}

// wirePut sets the property at path to value; a nil value encodes as JSON null.
func wirePut(t *testing.T, raw []byte, value any, path ...any) []byte {
	t.Helper()
	return wireEdit(t, raw, func(doc map[string]any) {
		wireAt(doc, path[:len(path)-1]...).(map[string]any)[path[len(path)-1].(string)] = value
	})
}

// wireRename gives the property at path the name, keeping its value.
func wireRename(t *testing.T, raw []byte, name string, path ...any) []byte {
	t.Helper()
	return wireEdit(t, raw, func(doc map[string]any) {
		obj := wireAt(doc, path[:len(path)-1]...).(map[string]any)
		old := path[len(path)-1].(string)
		obj[name] = obj[old]
		delete(obj, old)
	})
}

// wirePaths lists the path of every JSON object in v, depth first, v itself first.
func wirePaths(v any, path []any) [][]any {
	var paths [][]any
	switch v := v.(type) {
	case map[string]any:
		paths = append(paths, path)
		names := make([]string, 0, len(v))
		for name := range v {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			paths = append(paths, wirePaths(v[name], append(slices.Clone(path), name))...)
		}
	case []any:
		for i, item := range v {
			paths = append(paths, wirePaths(item, append(slices.Clone(path), i))...)
		}
	}
	return paths
}

func wantWireRejected(t *testing.T, name string, raw []byte) {
	t.Helper()
	if got, err := DecodePacket(raw); !errors.Is(err, ErrInvalidPacket) || !reflect.DeepEqual(got, Packet{}) {
		t.Errorf("%s: DecodePacket(%s) = %+v, %v, want zero packet and ErrInvalidPacket", name, raw, got, err)
	}
}

// TestWireAuditRegressions holds the 55 inputs of the IPC wire-contract audit, which the typed
// decode alone accepted (all but the reconnect hello without tracks).
func TestWireAuditRegressions(t *testing.T) {
	helloSetup := mustEncode(t, hello(ModeSolo, CodecH264, 0))
	helloRecv := mustEncode(t, hello(ModeSolo, CodecH264, 2))
	welcomeRecv := mustEncode(t, welcome(false, 0))
	entryInit := mustEncode(t, entry(KindInit))
	entryPhases := mustEncode(t, entry(KindPhases))
	entryPart := mustEncode(t, entry(KindPart))
	entryTerminal := mustEncode(t, entry(KindTerminal))
	rejectWire := mustEncode(t, reject())

	cases := []struct {
		name string
		raw  []byte
	}{
		// 1. Required properties deleted or null
		{"delete hello.epoch (setup hello)", wireDrop(t, helloSetup, "hello", "epoch")},
		{"null hello.epoch (setup hello)", wirePut(t, helloSetup, nil, "hello", "epoch")},
		{"delete hello.high", wireDrop(t, helloSetup, "hello", "high")},
		{"null hello.high", wirePut(t, helloSetup, nil, "hello", "high")},
		{"delete hello.tracks (setup hello)", wireDrop(t, helloSetup, "hello", "tracks")},
		{"null hello.tracks (setup hello)", wirePut(t, helloSetup, nil, "hello", "tracks")},
		{"delete hello.tracks (reconnect hello)", wireDrop(t, helloRecv, "hello", "tracks")},
		{"delete welcome.setup", wireDrop(t, welcomeRecv, "welcome", "setup")},
		{"null welcome.setup", wirePut(t, welcomeRecv, nil, "welcome", "setup")},
		{"delete welcome.applied", wireDrop(t, welcomeRecv, "welcome", "applied")},
		{"null welcome.applied", wirePut(t, welcomeRecv, nil, "welcome", "applied")},
		{"delete entry.phase_video_ns (phases entry)", wireDrop(t, entryPhases, "entry", "phase_video_ns")},
		{"delete entry.phase_audio_ns (phases entry)", wireDrop(t, entryPhases, "entry", "phase_audio_ns")},
		{"delete both phases (phases entry)", wireDrop(t, wireDrop(t, entryPhases, "entry", "phase_video_ns"), "entry", "phase_audio_ns")},
		{"delete phase_video_ns from init entry", wireDrop(t, entryInit, "entry", "phase_video_ns")},
		{"delete entry.first_au (part entry)", wireDrop(t, entryPart, "entry", "first_au")},
		{"delete entry.first_au (init entry)", wireDrop(t, entryInit, "entry", "first_au")},
		{"delete descriptor.track (reconnect hello track 0)", wireDrop(t, helloRecv, "hello", "tracks", 0, "track")},
		{"delete descriptor.dev (reconnect hello track 0)", wireDrop(t, helloRecv, "hello", "tracks", 0, "dev")},
		{"delete descriptor.ino (reconnect hello track 0)", wireDrop(t, helloRecv, "hello", "tracks", 0, "ino")},
		{"null descriptor.dev (reconnect hello track 0)", wirePut(t, helloRecv, nil, "hello", "tracks", 0, "dev")},
		{"null descriptor.ino (reconnect hello track 0)", wirePut(t, helloRecv, nil, "hello", "tracks", 0, "ino")},

		// 2. Case variants of exact property names
		{"type -> TYPE (hello packet)", wireRename(t, helloSetup, "TYPE", "type")},
		{"hello -> Hello (hello packet)", wireRename(t, helloSetup, "Hello", "hello")},
		{"version -> Version (hello payload)", wireRename(t, helloSetup, "Version", "hello", "version")},
		{"run -> Run (hello payload)", wireRename(t, helloSetup, "Run", "hello", "run")},
		{"source -> Source (hello payload)", wireRename(t, helloSetup, "Source", "hello", "source")},
		{"epoch -> Epoch (hello payload)", wireRename(t, helloSetup, "Epoch", "hello", "epoch")},
		{"mode -> Mode (config)", wireRename(t, helloSetup, "Mode", "hello", "config", "mode")},
		{"codec -> Codec (config)", wireRename(t, helloSetup, "Codec", "hello", "config", "codec")},
		{"tracks -> Tracks (config)", wireRename(t, helloSetup, "Tracks", "hello", "config", "tracks")},
		{"capacity -> Capacity (config)", wireRename(t, helloSetup, "Capacity", "hello", "config", "capacity")},
		{"type -> TYPE (welcome packet)", wireRename(t, welcomeRecv, "TYPE", "type")},
		{"welcome -> Welcome (welcome packet)", wireRename(t, welcomeRecv, "Welcome", "welcome")},
		{"type -> TYPE (entry packet)", wireRename(t, entryPart, "TYPE", "type")},
		{"entry -> Entry (entry packet)", wireRename(t, entryPart, "Entry", "entry")},
		{"kind -> Kind (entry payload)", wireRename(t, entryPart, "Kind", "entry", "kind")},
		{"seq -> Seq (entry payload)", wireRename(t, entryPart, "Seq", "entry", "seq")},
		{"reason -> Reason (reject packet)", wireRename(t, rejectWire, "Reason", "reason")},

		// 3. Unwanted null envelope payloads
		{"hello packet with welcome: null", wirePut(t, helloSetup, nil, "welcome")},
		{"hello packet with entry: null", wirePut(t, helloSetup, nil, "entry")},
		{"welcome packet with hello: null", wirePut(t, welcomeRecv, nil, "hello")},
		{"welcome packet with entry: null", wirePut(t, welcomeRecv, nil, "entry")},
		{"entry packet with hello: null", wirePut(t, entryPart, nil, "hello")},
		{"entry packet with welcome: null", wirePut(t, entryPart, nil, "welcome")},
		{"reject packet with hello: null", wirePut(t, rejectWire, nil, "hello")},

		// 4. Empty or null properties an entry kind forbids
		{"phases entry with codecs: empty string", wirePut(t, entryPhases, "", "entry", "codecs")},
		{"phases entry with codecs: null", wirePut(t, entryPhases, nil, "entry", "codecs")},
		{"part entry with codecs: empty string", wirePut(t, entryPart, "", "entry", "codecs")},
		{"part entry with codecs: null", wirePut(t, entryPart, nil, "entry", "codecs")},
		{"terminal entry with codecs: empty string", wirePut(t, entryTerminal, "", "entry", "codecs")},
		{"init entry with reason: empty string", wirePut(t, entryInit, "", "entry", "reason")},
		{"part entry with reason: empty string", wirePut(t, entryPart, "", "entry", "reason")},
		{"phases entry with sha256: empty string", wirePut(t, entryPhases, "", "entry", "sha256")},
		{"terminal entry with sha256: empty string", wirePut(t, entryTerminal, "", "entry", "sha256")},
	}
	if len(cases) != 55 {
		t.Fatalf("%d audit cases, want 55", len(cases))
	}
	for _, tc := range cases {
		wantWireRejected(t, tc.name, tc.raw)
	}
}

// Every property of every object of every kind of valid packet is required except a terminal
// entry's reason: deleting it, making it null or giving it a case variant of its name must fail.
func TestWireRejectsMissingNullAndCaseVariantProperties(t *testing.T) {
	for _, tc := range validPackets() {
		raw := mustEncode(t, tc.p)
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		for _, objPath := range wirePaths(doc, nil) {
			for name := range wireAt(doc, objPath...).(map[string]any) {
				path := append(slices.Clone(objPath), name)
				where := fmt.Sprintf("%s %v", tc.name, path)
				wantWireRejected(t, "null "+where, wirePut(t, raw, nil, path...))
				wantWireRejected(t, "case variant "+where, wireRename(t, raw, strings.ToUpper(name), path...))

				dropped := wireDrop(t, raw, path...)
				if tc.name == "entry terminal" && name == "reason" {
					if _, err := DecodePacket(dropped); err != nil {
						t.Errorf("terminal entry without a reason: %v", err)
					}
					continue
				}
				wantWireRejected(t, "missing "+where, dropped)
			}
		}
	}
}

func TestWireRejectsExtraEnvelopePayloads(t *testing.T) {
	own := map[string]string{TypeHello: "hello", TypeWelcome: "welcome", TypeEntry: "entry", TypeReject: "reason"}
	for _, tc := range validPackets() {
		raw := mustEncode(t, tc.p)
		for _, key := range []string{"hello", "welcome", "entry", "reason"} {
			if key == own[tc.p.Type] {
				continue
			}
			for _, value := range []any{nil, map[string]any{}} {
				wantWireRejected(t, fmt.Sprintf("%s with %s: %v", tc.name, key, value), wirePut(t, raw, value, key))
			}
		}
	}
}

// An entry kind requires, allows or forbids each of sha256, codecs and reason, and a property
// that is present is never null or, where allowed, empty.
func TestWireEntryKindProperties(t *testing.T) {
	const required, optional, forbidden = "required", "optional", "forbidden"
	kinds := []struct {
		kind                   string
		sha256, codecs, reason string
	}{
		{KindInit, required, required, forbidden},
		{KindPhases, forbidden, forbidden, forbidden},
		{KindPart, required, forbidden, forbidden},
		{KindTerminal, forbidden, forbidden, optional},
	}
	values := map[string]string{"sha256": testSHA, "codecs": "avc1.64001f", "reason": "end of source"}
	for _, k := range kinds {
		raw := mustEncode(t, entry(k.kind))
		for name, rule := range map[string]string{"sha256": k.sha256, "codecs": k.codecs, "reason": k.reason} {
			for _, state := range []string{"absent", "null", "empty", "value"} {
				var edited []byte
				var want bool
				switch state {
				case "absent":
					edited, want = wireDrop(t, raw, "entry", name), rule != required
				case "null":
					edited = wirePut(t, raw, nil, "entry", name)
				case "empty":
					edited = wirePut(t, raw, "", "entry", name)
				case "value":
					edited, want = wirePut(t, raw, values[name], "entry", name), rule != forbidden
				}
				_, err := DecodePacket(edited)
				if want != (err == nil) {
					t.Errorf("%s entry, %s %s (%s): accepted=%v, want %v (%v)", k.kind, rule, name, state, err == nil, want, err)
				}
			}
		}
	}
}

// Explicit zeros, false, empty arrays and uint64 beyond 2^53 are values, not absences: they are
// accepted and survive unchanged, however the JSON is spaced.
func TestWireAcceptsExplicitZeroValues(t *testing.T) {
	setupHello := hello(ModeSolo, CodecH264, 0)
	setupHello.Hello.High = 0
	plainWelcome := welcome(false, 0)
	plainWelcome.Welcome.Applied = 0
	phases := entry(KindPhases)
	phases.Entry.PhaseVideo, phases.Entry.PhaseAudio = 0, 0
	part := entry(KindPart)
	part.Entry.Offset, part.Entry.Index, part.Entry.PTS, part.Entry.DTS, part.Entry.FirstAU = 0, 0, 0, 0, 0
	part.Entry.FragmentStart, part.Entry.Independent = false, false
	terminal := entry(KindTerminal)
	terminal.Entry.Verified, terminal.Entry.Reason = false, ""
	big := hello(ModeSolo, CodecH264, 1<<53+1)
	big.Hello.High = math.MaxUint64
	for i := range big.Hello.Tracks {
		big.Hello.Tracks[i].Dev, big.Hello.Tracks[i].Ino = 1<<63+1, math.MaxUint64
	}

	for _, tc := range []struct {
		name string
		p    Packet
		wire []string
	}{
		{"setup hello", setupHello, []string{`"epoch":0`, `"high":0`, `"tracks":[]`}},
		{"welcome without setup", plainWelcome, []string{`"setup":false`, `"applied":0`, `"tracks":[]`}},
		{"phases entry", phases, []string{`"phase_video_ns":0`, `"phase_audio_ns":0`, `"offset":0`, `"length":0`, `"verified":false`}},
		{"part entry", part, []string{`"offset":0`, `"part":0`, `"pts_ns":0`, `"dts_ns":0`, `"first_au":0`, `"fragment_start":false`, `"independent":false`}},
		{"terminal entry", terminal, []string{`"verified":false`}},
		{"uint64 above 2^53", big, []string{`"epoch":9007199254740993`, `"high":18446744073709551615`, `"dev":9223372036854775809`, `"ino":18446744073709551615`}},
	} {
		data := mustEncode(t, tc.p)
		var indented bytes.Buffer
		if err := json.Indent(&indented, data, "", "  "); err != nil {
			t.Fatal(err)
		}
		for _, in := range [][]byte{data, indented.Bytes()} {
			got, err := DecodePacket(in)
			if err != nil || !reflect.DeepEqual(got, tc.p) {
				t.Errorf("%s: DecodePacket(%s) = %+v, %v, want the packet unchanged", tc.name, in, got, err)
			}
		}
		for _, want := range tc.wire {
			if !bytes.Contains(data, []byte(want)) {
				t.Errorf("%s: %s missing from %s", tc.name, want, data)
			}
		}
	}
}

// The property lists DecodePacket enforces are the schema's required lists.
func TestWireKeyListsMatchSchema(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/ipc-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs map[string]struct {
			Required   []string                   `json:"required"`
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	sorted := func(s []string) []string {
		s = slices.Clone(s)
		slices.Sort(s)
		return s
	}
	for name, got := range map[string][]string{
		"hello": helloProps, "welcome": welcomeProps, "config": configProps, "fdIdentity": fdProps, "entry": entryProps,
	} {
		if want := sorted(schema.Defs[name].Required); !reflect.DeepEqual(sorted(got), want) {
			t.Errorf("%s properties = %v, schema requires %v", name, sorted(got), want)
		}
	}
	var optional []string
	for name := range schema.Defs["entry"].Properties {
		if !slices.Contains(entryProps, name) {
			optional = append(optional, name)
		}
	}
	if want := []string{"codecs", "reason", "sha256"}; !reflect.DeepEqual(sorted(optional), want) {
		t.Errorf("optional entry properties = %v, want %v", sorted(optional), want)
	}
}
