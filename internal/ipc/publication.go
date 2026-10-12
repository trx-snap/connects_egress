package ipc

// IPC2: the deployed publication-session protocol. It is the first wire major that ships; the
// version 1 Packet envelope (hello, welcome, a stream of entries) was never deployed and exists
// only as the canonical data contract whose hello, welcome and entry this protocol carries
// unchanged, still stamped Version 1. Every IPC2 packet is a JSON object with the envelope
// version 2, a type and exactly that type's payload; its contract is contracts/ipc-v2.schema.json.
//
// A worker opens every connection with a hello and the supervisor answers with a welcome whose
// state is the version 1 welcome and whose pending grant, when one exists, is the grant the worker
// must finish for sequence state.applied+1. Bytes enter the Part Store only through a grant: the
// worker reserves an extent for one init or part, the supervisor answers with a grant or a reject,
// the worker writes exactly that extent, then publishes the entry together with the grant; a
// phases or terminal entry is published without one. Every accepted publication is answered with
// an ack naming the entry's sequence. Which requests a session accepts at which point (sequence
// continuity, the single in-flight grant, duplicates) is the session owner's to judge, not this
// package's.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

// PublicationVersion is the envelope version of every IPC2 packet. The hello, welcome state and
// entry it carries keep their canonical Version 1.
const PublicationVersion = 2

// IPC2 packet types beyond hello, welcome and reject.
const (
	TypeReserve = "reserve"
	TypeGrant   = "grant"
	TypePublish = "publish"
	TypeAck     = "ack"
)

// ReserveRequest asks for a writable extent of Length bytes in Track for the init or part the
// worker will publish as entry Seq.
type ReserveRequest struct {
	Seq    uint64 `json:"seq"`
	Kind   string `json:"kind"` // init or part
	Track  int    `json:"track"`
	Length int64  `json:"length"`
}

// Grant answers a reserve: the extent Offset..Offset+Length of Track that entry Seq, of Kind, must
// fill exactly. ID is the supervisor's durable identity of the grant.
type Grant struct {
	Seq    uint64 `json:"seq"`
	ID     uint64 `json:"id"`
	Kind   string `json:"kind"`
	Track  int    `json:"track"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// PublicationWelcome answers an accepted hello. State is the canonical welcome; Pending is the
// grant still open for entry State.Applied+1, present only when one really exists and never on a
// setup welcome.
type PublicationWelcome struct {
	State   Welcome `json:"state"`
	Pending *Grant  `json:"pending,omitempty"`
}

// Publication publishes Entry. An init or part carries the grant it fills, which the entry's
// kind, track and span must match; a phases or terminal entry carries none.
type Publication struct {
	Entry Entry  `json:"entry"`
	Grant *Grant `json:"grant,omitempty"`
}

// Ack acknowledges the publication of entry Seq.
type Ack struct {
	Seq uint64 `json:"seq"`
}

// PublicationPacket is one IPC2 message: Version is PublicationVersion and exactly one payload is
// set, by Type. A reject carries only Reason.
type PublicationPacket struct {
	Version int                 `json:"version"`
	Type    string              `json:"type"`
	Hello   *Hello              `json:"hello,omitempty"`
	Welcome *PublicationWelcome `json:"welcome,omitempty"`
	Reserve *ReserveRequest     `json:"reserve,omitempty"`
	Grant   *Grant              `json:"grant,omitempty"`
	Publish *Publication        `json:"publish,omitempty"`
	Ack     *Ack                `json:"ack,omitempty"`
	Reason  string              `json:"reason,omitempty"`
}

// EncodePublicationPacket validates p and returns its JSON encoding. Nil descriptor lists encode
// as empty arrays. It fails with ErrPacketTooLarge above MaxPacketSize.
func EncodePublicationPacket(p PublicationPacket) ([]byte, error) {
	if err := ValidatePublicationPacket(p); err != nil {
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

// DecodePublicationPacket parses one IPC2 packet and validates it. It rejects empty, oversized or
// non-UTF-8 input, a packet whose shape breaks the schema (see checkPublicationWire), unknown
// fields and anything after the packet's JSON value; on any error the returned packet is zero.
func DecodePublicationPacket(data []byte) (PublicationPacket, error) {
	switch {
	case len(data) == 0:
		return PublicationPacket{}, invalidf("empty input")
	case len(data) > MaxPacketSize:
		return PublicationPacket{}, fmt.Errorf("%w: %d bytes exceeds %d", ErrPacketTooLarge, len(data), MaxPacketSize)
	case !utf8.Valid(data):
		return PublicationPacket{}, invalidf("input is not valid UTF-8")
	}
	if err := checkPublicationWire(data); err != nil {
		return PublicationPacket{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var p PublicationPacket
	if err := dec.Decode(&p); err != nil {
		return PublicationPacket{}, invalidf("decode: %v", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return PublicationPacket{}, invalidf("data after the packet")
	}
	if err := ValidatePublicationPacket(p); err != nil {
		return PublicationPacket{}, err
	}
	return p, nil
}

// ValidatePublicationPacket reports whether p is a well-formed IPC2 packet: envelope version 2, a
// recognized type, exactly the payload that type names, and valid fields. The hello, welcome state
// and entry are validated by the canonical Version 1 rules. Errors wrap ErrUnsupportedVersion or
// ErrInvalidPacket.
func ValidatePublicationPacket(p PublicationPacket) error {
	if p.Version != PublicationVersion {
		return fmt.Errorf("%w: publication packet version %d, want %d", ErrUnsupportedVersion, p.Version, PublicationVersion)
	}
	n := 0
	for _, set := range []bool{p.Hello != nil, p.Welcome != nil, p.Reserve != nil, p.Grant != nil, p.Publish != nil, p.Ack != nil, p.Reason != ""} {
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
	case TypeReserve:
		own = p.Reserve != nil
	case TypeGrant:
		own = p.Grant != nil
	case TypePublish:
		own = p.Publish != nil
	case TypeAck:
		own = p.Ack != nil
	case TypeReject:
		own = p.Reason != ""
	default:
		return invalidf("unknown publication packet type %q", p.Type)
	}
	if !own || n != 1 {
		return invalidf("%s packet must carry exactly its own payload", p.Type)
	}
	switch p.Type {
	case TypeHello:
		return p.Hello.validate()
	case TypeWelcome:
		return p.Welcome.validate()
	case TypeReserve:
		return p.Reserve.validate()
	case TypeGrant:
		return p.Grant.validate("grant")
	case TypePublish:
		return p.Publish.validate()
	case TypeAck:
		if p.Ack.Seq == 0 {
			return invalidf("ack seq must be positive")
		}
		return nil
	}
	return checkText("reject reason", p.Reason, true)
}

func (w *PublicationWelcome) validate() error {
	if err := w.State.validate(); err != nil {
		return err
	}
	if w.Pending == nil {
		return nil
	}
	if w.State.Setup {
		return invalidf("setup welcome carries a pending grant")
	}
	if err := w.Pending.validate("welcome pending"); err != nil {
		return err
	}
	if w.State.Applied == math.MaxUint64 || w.Pending.Seq != w.State.Applied+1 {
		return invalidf("welcome pending grant is for seq %d, applied is %d", w.Pending.Seq, w.State.Applied)
	}
	return nil
}

func (r *ReserveRequest) validate() error {
	switch {
	case r.Seq == 0:
		return invalidf("reserve seq must be positive")
	case r.Kind != KindInit && r.Kind != KindPart:
		return invalidf("reserve of %q, want init or part", r.Kind)
	case r.Track < TrackVideoHi || r.Track > TrackAudioB:
		return invalidf("reserve track %d is not a track id", r.Track)
	case r.Length <= 0:
		return invalidf("reserve length %d is not positive", r.Length)
	}
	return nil
}

func (g *Grant) validate(what string) error {
	switch {
	case g.Seq == 0:
		return invalidf("%s seq must be positive", what)
	case g.ID == 0:
		return invalidf("%s id must be positive", what)
	case g.Kind != KindInit && g.Kind != KindPart:
		return invalidf("%s of %q, want init or part", what, g.Kind)
	case g.Track < TrackVideoHi || g.Track > TrackAudioB:
		return invalidf("%s track %d is not a track id", what, g.Track)
	case g.Offset < 0:
		return invalidf("%s offset %d is negative", what, g.Offset)
	case g.Length <= 0:
		return invalidf("%s length %d is not positive", what, g.Length)
	case g.Offset > math.MaxInt64-g.Length:
		return invalidf("%s span %d+%d overflows", what, g.Offset, g.Length)
	}
	return nil
}

func (pb *Publication) validate() error {
	e := &pb.Entry
	if err := e.validate(); err != nil {
		return err
	}
	if e.Kind != KindInit && e.Kind != KindPart {
		if pb.Grant != nil {
			return invalidf("%s publication must not carry a grant", e.Kind)
		}
		return nil
	}
	g := pb.Grant
	if g == nil {
		return invalidf("%s publication requires its grant", e.Kind)
	}
	if err := g.validate("publication grant"); err != nil {
		return err
	}
	if g.Seq != e.Seq || g.Kind != e.Kind || g.Track != e.Track || g.Offset != e.Offset || g.Length != e.Length {
		return invalidf("publication grant %d does not match entry seq %d", g.ID, e.Seq)
	}
	return nil
}

// The required properties of the IPC2 payload objects, as the schema's required lists.
var (
	reserveProps = []string{"seq", "kind", "track", "length"}
	grantProps   = []string{"seq", "id", "kind", "track", "offset", "length"}
	ackProps     = []string{"seq"}
)

// checkPublicationWire verifies the shape of one IPC2 packet's JSON against the schema before
// typed decoding, as checkWire does for the version 1 envelope: exact names, required presence,
// null, and the object and array structure of the envelope and every payload. The envelope
// version is judged first so that a foreign major is ErrUnsupportedVersion, not a shape error.
func checkPublicationWire(data []byte) error {
	top, err := wireObject(data, "publication packet", []string{"version", "type"},
		"hello", "welcome", "reserve", "grant", "publish", "ack", "reason")
	if err != nil {
		return err
	}
	var version int
	if err := json.Unmarshal(top["version"], &version); err != nil {
		return invalidf("publication packet version is not an integer")
	}
	if version != PublicationVersion {
		return fmt.Errorf("%w: publication packet version %d, want %d", ErrUnsupportedVersion, version, PublicationVersion)
	}
	var typ string
	if err := json.Unmarshal(top["type"], &typ); err != nil {
		return invalidf("publication packet type is not a string")
	}
	payload := typ
	switch typ {
	case TypeHello, TypeWelcome, TypeReserve, TypeGrant, TypePublish, TypeAck:
	case TypeReject:
		payload = "reason"
	default:
		return invalidf("unknown publication packet type %q", typ)
	}
	if _, ok := top[payload]; !ok || len(top) != 3 {
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
		w, err := wireObject(top["welcome"], "welcome", []string{"state"}, "pending")
		if err != nil {
			return err
		}
		st, err := wireObject(w["state"], "welcome state", welcomeProps)
		if err != nil {
			return err
		}
		if err := wireDescriptors("welcome state", st["tracks"]); err != nil {
			return err
		}
		if pending, ok := w["pending"]; ok {
			_, err = wireObject(pending, "welcome pending", grantProps)
		}
		return err
	case TypeReserve:
		_, err := wireObject(top["reserve"], "reserve", reserveProps)
		return err
	case TypeGrant:
		_, err := wireObject(top["grant"], "grant", grantProps)
		return err
	case TypePublish:
		pb, err := wireObject(top["publish"], "publish", []string{"entry"}, "grant")
		if err != nil {
			return err
		}
		if err := wireEntry(pb["entry"]); err != nil {
			return err
		}
		if grant, ok := pb["grant"]; ok {
			_, err = wireObject(grant, "publication grant", grantProps)
		}
		return err
	case TypeAck:
		_, err := wireObject(top["ack"], "ack", ackProps)
		return err
	}
	return nil
}

// withEmptyLists returns p with nil descriptor lists replaced by empty ones, which encode as []
// rather than null. The payloads of p itself are left untouched.
func (p PublicationPacket) withEmptyLists() PublicationPacket {
	if h := p.Hello; h != nil && h.Tracks == nil {
		c := *h
		c.Tracks = []FDIdentity{}
		p.Hello = &c
	}
	if w := p.Welcome; w != nil && w.State.Tracks == nil {
		c := *w
		c.State.Tracks = []FDIdentity{}
		p.Welcome = &c
	}
	return p
}
