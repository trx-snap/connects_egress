package ipc

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func mustEncodePub(t *testing.T, p PublicationPacket) []byte {
	t.Helper()
	data, err := EncodePublicationPacket(p)
	if err != nil {
		t.Fatalf("EncodePublicationPacket: %v", err)
	}
	return data
}

func wantPubWireRejected(t *testing.T, name string, raw []byte) {
	t.Helper()
	if got, err := DecodePublicationPacket(raw); err == nil || !reflect.DeepEqual(got, PublicationPacket{}) {
		t.Errorf("%s: DecodePublicationPacket(%s) = %+v, %v, want zero packet and error", name, raw, got, err)
	}
}

func pubHello(mode, codec string, epoch uint64) PublicationPacket {
	h := hello(mode, codec, epoch).Hello
	return PublicationPacket{Version: PublicationVersion, Type: TypeHello, Hello: h}
}

func pubWelcome(setup bool, tracks int, applied uint64, pending *Grant) PublicationPacket {
	w := welcome(setup, tracks).Welcome
	if !setup {
		w.Applied = applied
	}
	pw := &PublicationWelcome{State: *w, Pending: pending}
	return PublicationPacket{Version: PublicationVersion, Type: TypeWelcome, Welcome: pw}
}

func pubReserve(seq uint64, kind string, track int, length int64) PublicationPacket {
	return PublicationPacket{
		Version: PublicationVersion,
		Type:    TypeReserve,
		Reserve: &ReserveRequest{
			Seq:    seq,
			Kind:   kind,
			Track:  track,
			Length: length,
		},
	}
}

func pubGrant(seq uint64, id uint64, kind string, track int, offset, length int64) PublicationPacket {
	return PublicationPacket{
		Version: PublicationVersion,
		Type:    TypeGrant,
		Grant: &Grant{
			Seq:    seq,
			ID:     id,
			Kind:   kind,
			Track:  track,
			Offset: offset,
			Length: length,
		},
	}
}

func pubPublish(e Entry, g *Grant) PublicationPacket {
	return PublicationPacket{
		Version: PublicationVersion,
		Type:    TypePublish,
		Publish: &Publication{
			Entry: e,
			Grant: g,
		},
	}
}

func pubAck(seq uint64) PublicationPacket {
	return PublicationPacket{
		Version: PublicationVersion,
		Type:    TypeAck,
		Ack:     &Ack{Seq: seq},
	}
}

func pubReject(reason string) PublicationPacket {
	return PublicationPacket{
		Version: PublicationVersion,
		Type:    TypeReject,
		Reason:  reason,
	}
}

type namedPubPacket struct {
	name string
	p    PublicationPacket
}

func validPubPackets() []namedPubPacket {
	initEntry := *entry(KindInit).Entry
	initGrant := &Grant{
		Seq:    initEntry.Seq,
		ID:     101,
		Kind:   initEntry.Kind,
		Track:  initEntry.Track,
		Offset: initEntry.Offset,
		Length: initEntry.Length,
	}

	partEntry := *entry(KindPart).Entry
	partGrant := &Grant{
		Seq:    partEntry.Seq,
		ID:     102,
		Kind:   partEntry.Kind,
		Track:  partEntry.Track,
		Offset: partEntry.Offset,
		Length: partEntry.Length,
	}

	phasesEntry := *entry(KindPhases).Entry
	terminalEntry := *entry(KindTerminal).Entry

	pendingGrant := &Grant{
		Seq:    42,
		ID:     201,
		Kind:   KindInit,
		Track:  TrackVideoHi,
		Offset: 0,
		Length: 812,
	}

	return []namedPubPacket{
		{"hello setup solo h264", pubHello(ModeSolo, CodecH264, 0)},
		{"hello setup 1v1 hevc", pubHello(Mode1v1, CodecHEVC, 0)},
		{"hello reconnect solo h264", pubHello(ModeSolo, CodecH264, 2)},
		{"hello reconnect 1v1 hevc", pubHello(Mode1v1, CodecHEVC, 2)},
		{"welcome setup solo", pubWelcome(true, TracksSolo, 0, nil)},
		{"welcome setup 1v1", pubWelcome(true, Tracks1v1, 0, nil)},
		{"welcome reconnect without pending", pubWelcome(false, 0, 41, nil)},
		{"welcome reconnect with pending", pubWelcome(false, 0, 41, pendingGrant)},
		{"reserve init", pubReserve(1, KindInit, TrackVideoHi, 812)},
		{"reserve part", pubReserve(2, KindPart, TrackVideoMid, 65536)},
		{"grant init", pubGrant(1, 101, KindInit, TrackVideoHi, 0, 812)},
		{"grant part", pubGrant(2, 102, KindPart, TrackVideoMid, 812, 65536)},
		{"publish init", pubPublish(initEntry, initGrant)},
		{"publish part", pubPublish(partEntry, partGrant)},
		{"publish phases", pubPublish(phasesEntry, nil)},
		{"publish terminal", pubPublish(terminalEntry, nil)},
		{"ack", pubAck(9)},
		{"reject", pubReject("invalid options")},
	}
}

func TestPublicationValidPacketsRoundTrip(t *testing.T) {
	for _, tc := range validPubPackets() {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidatePublicationPacket(tc.p); err != nil {
				t.Fatalf("ValidatePublicationPacket: %v", err)
			}
			raw := mustEncodePub(t, tc.p)
			got, err := DecodePublicationPacket(raw)
			if err != nil {
				t.Fatalf("DecodePublicationPacket: %v", err)
			}
			if !reflect.DeepEqual(got, tc.p) {
				t.Fatalf("roundtrip mismatch:\ngot:  %+v\nwant: %+v", got, tc.p)
			}
		})
	}
}

func TestPublicationUint64Precision(t *testing.T) {
	largeVals := []uint64{
		1 << 54,
		1 << 62,
		math.MaxUint64 - 100,
	}

	for i, v := range largeVals {
		t.Run(fmt.Sprintf("large-%d", i), func(t *testing.T) {
			// 1. Reserve with large Seq
			resPkt := pubReserve(v, KindInit, TrackVideoHi, 812)
			decRes, err := DecodePublicationPacket(mustEncodePub(t, resPkt))
			if err != nil {
				t.Fatalf("Decode reserve: %v", err)
			}
			if decRes.Reserve.Seq != v {
				t.Errorf("Reserve.Seq = %d, want %d", decRes.Reserve.Seq, v)
			}

			// 2. Grant with large Seq and ID
			grantPkt := pubGrant(v, v+1, KindInit, TrackVideoHi, 0, 812)
			decGrant, err := DecodePublicationPacket(mustEncodePub(t, grantPkt))
			if err != nil {
				t.Fatalf("Decode grant: %v", err)
			}
			if decGrant.Grant.Seq != v || decGrant.Grant.ID != v+1 {
				t.Errorf("Grant Seq/ID = %d/%d, want %d/%d", decGrant.Grant.Seq, decGrant.Grant.ID, v, v+1)
			}

			// 3. Ack with large Seq
			ackPkt := pubAck(v)
			decAck, err := DecodePublicationPacket(mustEncodePub(t, ackPkt))
			if err != nil {
				t.Fatalf("Decode ack: %v", err)
			}
			if decAck.Ack.Seq != v {
				t.Errorf("Ack.Seq = %d, want %d", decAck.Ack.Seq, v)
			}

			// 4. Welcome with pending large Seq and ID
			if v < math.MaxUint64 {
				pending := &Grant{
					Seq:    v + 1,
					ID:     v + 2,
					Kind:   KindInit,
					Track:  TrackVideoHi,
					Offset: 0,
					Length: 812,
				}
				welPkt := pubWelcome(false, 0, v, pending)
				decWel, err := DecodePublicationPacket(mustEncodePub(t, welPkt))
				if err != nil {
					t.Fatalf("Decode welcome: %v", err)
				}
				if decWel.Welcome.State.Applied != v || decWel.Welcome.Pending.Seq != v+1 || decWel.Welcome.Pending.ID != v+2 {
					t.Errorf("Welcome applied/pending = %d/%+v, want %d/%+v", decWel.Welcome.State.Applied, decWel.Welcome.Pending, v, pending)
				}
			}

			// 5. Publish Part with large PTS, DTS, MonoNS
			e := *entry(KindPart).Entry
			e.Seq = v
			e.PTS = int64(v & 0x7fffffffffffffff)
			e.DTS = int64(v & 0x7fffffffffffffff)
			e.PublishedMonoNS = int64(v & 0x7fffffffffffffff)
			g := &Grant{
				Seq:    e.Seq,
				ID:     v + 1,
				Kind:   e.Kind,
				Track:  e.Track,
				Offset: e.Offset,
				Length: e.Length,
			}
			pubPkt := pubPublish(e, g)
			decPub, err := DecodePublicationPacket(mustEncodePub(t, pubPkt))
			if err != nil {
				t.Fatalf("Decode publish: %v", err)
			}
			if decPub.Publish.Entry.Seq != v || decPub.Publish.Entry.PTS != e.PTS || decPub.Publish.Grant.ID != v+1 {
				t.Errorf("Publish mismatch with large uint64 values")
			}
		})
	}
}

func TestPublicationVersions(t *testing.T) {
	// Top-level envelope must be version 2
	for _, badVer := range []int{0, 1, 3, 99} {
		p := pubAck(1)
		p.Version = badVer
		if err := ValidatePublicationPacket(p); !errors.Is(err, ErrUnsupportedVersion) {
			t.Errorf("ValidatePublicationPacket(version %d) = %v, want ErrUnsupportedVersion", badVer, err)
		}

		raw := []byte(fmt.Sprintf(`{"version":%d,"type":"ack","ack":{"seq":1}}`, badVer))
		if got, err := DecodePublicationPacket(raw); !errors.Is(err, ErrUnsupportedVersion) || !reflect.DeepEqual(got, PublicationPacket{}) {
			t.Errorf("DecodePublicationPacket(version %d) = %+v, %v, want zero packet and ErrUnsupportedVersion", badVer, got, err)
		}
	}

	// Inner canonical contracts must be version 1
	// 1. Hello inner version != 1
	{
		h := hello(ModeSolo, CodecH264, 0).Hello
		h.Version = 2
		p := PublicationPacket{Version: PublicationVersion, Type: TypeHello, Hello: h}
		if err := ValidatePublicationPacket(p); !errors.Is(err, ErrUnsupportedVersion) {
			t.Errorf("hello inner version 2: ValidatePublicationPacket = %v, want ErrUnsupportedVersion", err)
		}
	}

	// 2. Welcome State inner version != 1
	{
		w := welcome(true, TracksSolo).Welcome
		w.Version = 2
		p := PublicationPacket{Version: PublicationVersion, Type: TypeWelcome, Welcome: &PublicationWelcome{State: *w}}
		if err := ValidatePublicationPacket(p); !errors.Is(err, ErrUnsupportedVersion) {
			t.Errorf("welcome state inner version 2: ValidatePublicationPacket = %v, want ErrUnsupportedVersion", err)
		}
	}

	// 3. Publish Entry inner version != 1
	{
		e := *entry(KindInit).Entry
		e.Version = 2
		g := &Grant{Seq: e.Seq, ID: 1, Kind: e.Kind, Track: e.Track, Offset: e.Offset, Length: e.Length}
		p := pubPublish(e, g)
		if err := ValidatePublicationPacket(p); !errors.Is(err, ErrUnsupportedVersion) {
			t.Errorf("entry inner version 2: ValidatePublicationPacket = %v, want ErrUnsupportedVersion", err)
		}
	}
}

func TestPublicationRejectsMissingNullAndCaseVariants(t *testing.T) {
	for _, tc := range validPubPackets() {
		raw := mustEncodePub(t, tc.p)
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		for _, objPath := range wirePaths(doc, nil) {
			for name := range wireAt(doc, objPath...).(map[string]any) {
				path := append(slices.Clone(objPath), name)
				where := fmt.Sprintf("%s %v", tc.name, path)

				// Null check
				wantPubWireRejected(t, "null "+where, wirePut(t, raw, nil, path...))

				// Case variant check
				wrongName := strings.ToUpper(name[:1]) + name[1:]
				if wrongName == name {
					wrongName = strings.ToLower(name)
				}
				wantPubWireRejected(t, "case variant "+where, wireRename(t, raw, wrongName, path...))

				// Missing property check
				dropped := wireDrop(t, raw, path...)
				if tc.name == "publish terminal" && len(path) == 2 && path[0] == "publish" && name == "grant" {
					// grant is omitted on terminal entry
					continue
				}
				if strings.HasPrefix(tc.name, "welcome") && len(path) == 2 && path[0] == "welcome" && name == "pending" {
					// pending is optional on welcome
					continue
				}
				if tc.name == "publish terminal" && len(path) == 3 && path[0] == "publish" && path[1] == "entry" && name == "reason" {
					// reason is optional on terminal entry
					continue
				}
				wantPubWireRejected(t, "missing "+where, dropped)
			}
		}
	}
}

func TestPublicationOptionalAbsentVsNull(t *testing.T) {
	// 1. Welcome pending: absent is valid, null is rejected
	{
		welAbs := pubWelcome(false, 0, 41, nil)
		rawAbs := mustEncodePub(t, welAbs)
		if _, err := DecodePublicationPacket(rawAbs); err != nil {
			t.Fatalf("welcome pending absent: %v", err)
		}
		rawNull := wirePut(t, rawAbs, nil, "welcome", "pending")
		wantPubWireRejected(t, "welcome pending null", rawNull)
	}

	// 2. Publish grant on phases: absent is valid, null is rejected
	{
		pubAbs := pubPublish(*entry(KindPhases).Entry, nil)
		rawAbs := mustEncodePub(t, pubAbs)
		if _, err := DecodePublicationPacket(rawAbs); err != nil {
			t.Fatalf("publish phases grant absent: %v", err)
		}
		rawNull := wirePut(t, rawAbs, nil, "publish", "grant")
		wantPubWireRejected(t, "publish phases grant null", rawNull)
	}

	// 3. Publish grant on terminal: absent is valid, null is rejected
	{
		pubAbs := pubPublish(*entry(KindTerminal).Entry, nil)
		rawAbs := mustEncodePub(t, pubAbs)
		if _, err := DecodePublicationPacket(rawAbs); err != nil {
			t.Fatalf("publish terminal grant absent: %v", err)
		}
		rawNull := wirePut(t, rawAbs, nil, "publish", "grant")
		wantPubWireRejected(t, "publish terminal grant null", rawNull)
	}

	// 4. Hello / Welcome tracks: empty list [] is valid, null is rejected
	{
		hSetup := pubHello(ModeSolo, CodecH264, 0)
		rawH := mustEncodePub(t, hSetup)
		rawNull := wirePut(t, rawH, nil, "hello", "tracks")
		wantPubWireRejected(t, "hello tracks null", rawNull)

		wSetup := pubWelcome(true, TracksSolo, 0, nil)
		rawW := mustEncodePub(t, wSetup)
		rawWNull := wirePut(t, rawW, nil, "welcome", "state", "tracks")
		wantPubWireRejected(t, "welcome state tracks null", rawWNull)
	}
}

func TestPublicationExtraKeys(t *testing.T) {
	own := map[string]string{
		TypeHello:   "hello",
		TypeWelcome: "welcome",
		TypeReserve: "reserve",
		TypeGrant:   "grant",
		TypePublish: "publish",
		TypeAck:     "ack",
		TypeReject:  "reason",
	}

	allPayloads := []string{"hello", "welcome", "reserve", "grant", "publish", "ack", "reason"}

	for _, tc := range validPubPackets() {
		raw := mustEncodePub(t, tc.p)

		// Extra top-level payload keys
		for _, key := range allPayloads {
			if key == own[tc.p.Type] {
				continue
			}
			val := any(map[string]any{})
			if key == "reason" {
				val = "extra reason"
			}
			wantPubWireRejected(t, fmt.Sprintf("%s with payload %s", tc.name, key), wirePut(t, raw, val, key))
		}

		// Unknown top-level key and nested object extra keys
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		for _, objPath := range wirePaths(doc, nil) {
			path := append(slices.Clone(objPath), "unknown_extra_property")
			wantPubWireRejected(t, fmt.Sprintf("%s extra key at %v", tc.name, objPath), wirePut(t, raw, "extra_value", path...))
		}
	}
}

func TestPublicationDecodeErrorsReturnZeroPacket(t *testing.T) {
	// 1. Empty input
	if got, err := DecodePublicationPacket([]byte{}); err == nil || !reflect.DeepEqual(got, PublicationPacket{}) {
		t.Errorf("empty input: got %+v, %v, want zero packet and error", got, err)
	}

	// 2. Trailing data after JSON
	rawValid := mustEncodePub(t, pubAck(1))
	trailing := append(slices.Clone(rawValid), []byte(" trailing")...)
	if got, err := DecodePublicationPacket(trailing); err == nil || !reflect.DeepEqual(got, PublicationPacket{}) {
		t.Errorf("trailing input: got %+v, %v, want zero packet and error", got, err)
	}

	// 3. Non-UTF-8 input
	nonUTF8 := []byte{0xff, 0xfe, 0xfd}
	if got, err := DecodePublicationPacket(nonUTF8); err == nil || !reflect.DeepEqual(got, PublicationPacket{}) {
		t.Errorf("non-UTF-8 input: got %+v, %v, want zero packet and error", got, err)
	}

	// 4. Oversized input
	oversized := make([]byte, MaxPacketSize+1)
	if got, err := DecodePublicationPacket(oversized); !errors.Is(err, ErrPacketTooLarge) || !reflect.DeepEqual(got, PublicationPacket{}) {
		t.Errorf("oversized input: got %+v, %v, want zero packet and ErrPacketTooLarge", got, err)
	}

	// 5. Type mismatch in fields
	typeCases := [][]byte{
		[]byte(`{"version":"2","type":"ack","ack":{"seq":1}}`),
		[]byte(`{"version":2,"type":123,"ack":{"seq":1}}`),
		[]byte(`{"version":2,"type":"ack","ack":{"seq":"not-an-int"}}`),
		[]byte(`{"version":2,"type":"reserve","reserve":{"seq":1,"kind":123,"track":0,"length":10}}`),
		[]byte(`{"version":2,"type":"grant","grant":{"seq":1,"id":1,"kind":"init","track":"zero","offset":0,"length":10}}`),
	}
	for i, tc := range typeCases {
		if got, err := DecodePublicationPacket(tc); err == nil || !reflect.DeepEqual(got, PublicationPacket{}) {
			t.Errorf("type error case %d: got %+v, %v, want zero packet and error", i, got, err)
		}
	}
}

func TestPublicationSemanticRejections(t *testing.T) {
	// 1. Setup welcome with pending grant
	{
		pending := &Grant{Seq: 1, ID: 10, Kind: KindInit, Track: TrackVideoHi, Offset: 0, Length: 812}
		p := pubWelcome(true, TracksSolo, 0, pending)
		if err := ValidatePublicationPacket(p); err == nil || !strings.Contains(err.Error(), "setup welcome carries a pending grant") {
			t.Errorf("setup welcome with pending: got %v, want 'setup welcome carries a pending grant'", err)
		}
	}

	// 2. Pending grant sequence mismatch (Pending.Seq != State.Applied + 1)
	{
		pending := &Grant{Seq: 43, ID: 10, Kind: KindInit, Track: TrackVideoHi, Offset: 0, Length: 812} // Applied is 41, want 42
		p := pubWelcome(false, 0, 41, pending)
		if err := ValidatePublicationPacket(p); err == nil || !strings.Contains(err.Error(), "welcome pending grant is for seq 43, applied is 41") {
			t.Errorf("welcome pending seq mismatch: got %v, want seq mismatch error", err)
		}
	}

	// 3. Byte publication missing grant
	{
		pInit := pubPublish(*entry(KindInit).Entry, nil)
		if err := ValidatePublicationPacket(pInit); err == nil || !strings.Contains(err.Error(), "init publication requires its grant") {
			t.Errorf("init without grant: got %v, want 'init publication requires its grant'", err)
		}

		pPart := pubPublish(*entry(KindPart).Entry, nil)
		if err := ValidatePublicationPacket(pPart); err == nil || !strings.Contains(err.Error(), "part publication requires its grant") {
			t.Errorf("part without grant: got %v, want 'part publication requires its grant'", err)
		}
	}

	// 4. Byte publication with mismatched grant
	{
		e := *entry(KindInit).Entry
		mismatchCases := []struct {
			name  string
			grant Grant
		}{
			{"seq mismatch", Grant{Seq: e.Seq + 1, ID: 1, Kind: e.Kind, Track: e.Track, Offset: e.Offset, Length: e.Length}},
			{"kind mismatch", Grant{Seq: e.Seq, ID: 1, Kind: KindPart, Track: e.Track, Offset: e.Offset, Length: e.Length}},
			{"track mismatch", Grant{Seq: e.Seq, ID: 1, Kind: e.Kind, Track: e.Track + 1, Offset: e.Offset, Length: e.Length}},
			{"offset mismatch", Grant{Seq: e.Seq, ID: 1, Kind: e.Kind, Track: e.Track, Offset: e.Offset + 10, Length: e.Length}},
			{"length mismatch", Grant{Seq: e.Seq, ID: 1, Kind: e.Kind, Track: e.Track, Offset: e.Offset, Length: e.Length + 10}},
		}
		for _, mc := range mismatchCases {
			g := mc.grant
			p := pubPublish(e, &g)
			if err := ValidatePublicationPacket(p); err == nil || !strings.Contains(err.Error(), "does not match entry") {
				t.Errorf("%s: got %v, want mismatch error", mc.name, err)
			}
		}
	}

	// 5. Metadata publication carrying a grant
	{
		g := &Grant{Seq: 9, ID: 1, Kind: KindInit, Track: TrackAudioA, Offset: 0, Length: 812}

		pPhases := pubPublish(*entry(KindPhases).Entry, g)
		if err := ValidatePublicationPacket(pPhases); err == nil || !strings.Contains(err.Error(), "phases publication must not carry a grant") {
			t.Errorf("phases with grant: got %v, want 'phases publication must not carry a grant'", err)
		}

		pTerm := pubPublish(*entry(KindTerminal).Entry, g)
		if err := ValidatePublicationPacket(pTerm); err == nil || !strings.Contains(err.Error(), "terminal publication must not carry a grant") {
			t.Errorf("terminal with grant: got %v, want 'terminal publication must not carry a grant'", err)
		}
	}
}
