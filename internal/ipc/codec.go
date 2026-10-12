package ipc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
)

// EncodePacket validates p and returns its JSON encoding. A nil descriptor list is encoded as an
// empty array. It fails with ErrPacketTooLarge when the encoding exceeds MaxPacketSize.
func EncodePacket(p Packet) ([]byte, error) {
	if err := ValidatePacket(p); err != nil {
		return nil, err
	}
	data, err := json.Marshal(p.withEmptyLists())
	if err != nil {
		return nil, invalidf("encode: %v", err)
	}
	if len(data) > MaxPacketSize {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", ErrPacketTooLarge, len(data), MaxPacketSize)
	}
	return data, nil
}

// DecodePacket parses one packet and validates it. It rejects empty or oversized input, a packet
// whose shape breaks the schema (see checkWire), unknown fields and anything after the packet's
// JSON value; on any error the returned Packet is zero.
func DecodePacket(data []byte) (Packet, error) {
	switch {
	case len(data) == 0:
		return Packet{}, invalidf("empty input")
	case len(data) > MaxPacketSize:
		return Packet{}, fmt.Errorf("%w: %d bytes exceeds %d", ErrPacketTooLarge, len(data), MaxPacketSize)
	}
	if err := checkWire(data); err != nil {
		return Packet{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var p Packet
	if err := dec.Decode(&p); err != nil {
		return Packet{}, invalidf("decode: %v", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Packet{}, invalidf("data after the packet")
	}
	if err := ValidatePacket(p); err != nil {
		return Packet{}, err
	}
	return p, nil
}

// The properties every object of the wire format must carry, as the schema's required lists. An
// entry also carries sha256, codecs and reason according to its kind (see wireEntry).
var (
	helloProps   = []string{"version", "run", "source", "epoch", "pid", "config", "tracks", "high"}
	welcomeProps = []string{"version", "run", "source", "epoch", "setup", "applied", "tracks"}
	configProps  = []string{"mode", "codec", "tracks", "capacity"}
	fdProps      = []string{"track", "name", "dev", "ino", "size"}
	entryProps   = []string{
		"version", "run", "source", "epoch", "seq", "kind", "track", "published_ns", "published_mono_ns",
		"offset", "length", "phase_video_ns", "phase_audio_ns",
		"msn", "part", "pts_ns", "dts_ns", "duration_ns", "sequence",
		"fragment_start", "independent", "payload_buffers", "first_au", "verified",
	}
)

// checkWire verifies the shape of one packet's JSON against the schema before typed decoding, which
// cannot see it: a missing property, a null and a case-variant name all decode silently to a zero
// value or to the right field. It checks exact case-sensitive names, required presence, null and
// the object and array structure of the envelope, payload, config and descriptors. Scalar types,
// values and cross-field rules are left to the typed decode and ValidatePacket.
func checkWire(data []byte) error {
	top, err := wireObject(data, "packet", []string{"type"}, "hello", "welcome", "entry", "reason")
	if err != nil {
		return err
	}
	var typ string
	if err := json.Unmarshal(top["type"], &typ); err != nil {
		return invalidf("packet type is not a string")
	}
	payload := typ // the property that carries the payload: the type's own name, except for reject
	switch typ {
	case TypeHello, TypeWelcome, TypeEntry:
	case TypeReject:
		payload = "reason"
	default:
		return invalidf("unknown packet type %q", typ)
	}
	if _, ok := top[payload]; !ok || len(top) != 2 {
		return invalidf("%s packet must carry exactly its own payload", typ)
	}
	switch typ {
	case TypeHello:
		h, err := wireObject(top["hello"], "hello", helloProps)
		if err != nil {
			return err
		}
		if _, err := wireObject(h["config"], "hello config", configProps); err != nil {
			return err
		}
		return wireDescriptors("hello", h["tracks"])
	case TypeWelcome:
		w, err := wireObject(top["welcome"], "welcome", welcomeProps)
		if err != nil {
			return err
		}
		return wireDescriptors("welcome", w["tracks"])
	case TypeEntry:
		return wireEntry(top["entry"])
	}
	return nil
}

// wireObject parses raw as a JSON object that carries every required property, may carry the
// allowed ones and nothing else. Names match case-sensitively and no value is null.
func wireObject(raw json.RawMessage, what string, required []string, allowed ...string) (map[string]json.RawMessage, error) {
	if firstByte(raw) != '{' {
		return nil, invalidf("%s is not a JSON object", what)
	}
	var props map[string]json.RawMessage
	if err := json.Unmarshal(raw, &props); err != nil {
		return nil, invalidf("%s: %v", what, err)
	}
	for name, value := range props {
		switch {
		case !slices.Contains(required, name) && !slices.Contains(allowed, name):
			return nil, invalidf("%s has unexpected property %q", what, name)
		case firstByte(value) == 'n':
			return nil, invalidf("%s property %q is null", what, name)
		}
	}
	for _, name := range required {
		if _, ok := props[name]; !ok {
			return nil, invalidf("%s lacks property %q", what, name)
		}
	}
	return props, nil
}

// wireDescriptors requires raw to be an array of descriptor objects.
func wireDescriptors(what string, raw json.RawMessage) error {
	var items []json.RawMessage
	if firstByte(raw) != '[' {
		return invalidf("%s tracks is not an array", what)
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return invalidf("%s tracks: %v", what, err)
	}
	for i, item := range items {
		if _, err := wireObject(item, fmt.Sprintf("%s tracks[%d]", what, i), fdProps); err != nil {
			return err
		}
	}
	return nil
}

// How an entry kind treats one of the optional properties sha256, codecs and reason.
type presence uint8

const (
	propForbidden presence = iota // must be absent, even as an empty string
	propOptional                  // may be absent; when present, a nonempty string
	propRequired                  // must be present
)

// wireEntry requires raw to be an entry object with the optional properties its kind calls for.
func wireEntry(raw json.RawMessage) error {
	e, err := wireObject(raw, "entry", entryProps, "sha256", "codecs", "reason")
	if err != nil {
		return err
	}
	var kind string
	if err := json.Unmarshal(e["kind"], &kind); err != nil {
		return invalidf("entry kind is not a string")
	}
	var rule [3]presence // sha256, codecs, reason
	switch kind {
	case KindInit:
		rule = [3]presence{propRequired, propRequired, propForbidden}
	case KindPhases:
		rule = [3]presence{propForbidden, propForbidden, propForbidden}
	case KindPart:
		rule = [3]presence{propRequired, propForbidden, propForbidden}
	case KindTerminal:
		rule = [3]presence{propForbidden, propForbidden, propOptional}
	default:
		return nil // an unsupported kind is for ValidatePacket to reject, after the version
	}
	for i, name := range [3]string{"sha256", "codecs", "reason"} {
		value, present := e[name]
		switch {
		case rule[i] == propRequired && !present:
			return invalidf("%s entry lacks %s", kind, name)
		case rule[i] == propForbidden && present:
			return invalidf("%s entry must not carry %s", kind, name)
		case rule[i] == propOptional && present:
			var s string
			if json.Unmarshal(value, &s) != nil || s == "" {
				return invalidf("%s entry %s must be a nonempty string", kind, name)
			}
		}
	}
	return nil
}

// firstByte returns the first byte of the JSON value raw, past any leading space, or 0 if none.
func firstByte(raw []byte) byte {
	if raw = bytes.TrimSpace(raw); len(raw) == 0 {
		return 0
	}
	return raw[0]
}

// withEmptyLists returns p with nil descriptor lists replaced by empty ones, which encode as []
// rather than null. The payloads of p itself are left untouched.
func (p Packet) withEmptyLists() Packet {
	if h := p.Hello; h != nil && h.Tracks == nil {
		c := *h
		c.Tracks = []FDIdentity{}
		p.Hello = &c
	}
	if w := p.Welcome; w != nil && w.Tracks == nil {
		c := *w
		c.Tracks = []FDIdentity{}
		p.Welcome = &c
	}
	return p
}
