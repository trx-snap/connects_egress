//go:build linux

package partstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

// Journal file layout: the ASCII prefix "EGRJ" and a big-endian uint32 version, then frames.
// A frame is one kind byte, the big-endian uint32 length of its JSON payload, the SHA-256 of
// kind, length and payload, and the payload. The first frame is the checkpoint header; every
// later frame is one Record in sequence order.
const (
	journalFile    = "journal.bin"
	journalLock    = ".journal.lock"
	journalMagic   = "EGRJ"
	journalVersion = 1
	bootIDPath     = "/proc/sys/kernel/random/boot_id"

	frameCheckpoint byte = 1
	frameRecord     byte = 2

	prefixLen    = 8
	frameHeadLen = 1 + 4 + sha256.Size

	maxHeaderJSON    = 4 << 20
	maxRecordJSON    = 16 << 10
	maxJournalFile   = 64 << 20
	maxSuffixRecords = 32768
)

var (
	// ErrJournalInvalid reports a journal file that is not a complete, valid representation of
	// this epoch: bad prefix, kind, checksum, JSON, sequence, scope, boot or a complete record
	// that cannot be parsed. Only an incomplete final record frame is recovered by truncation.
	ErrJournalInvalid = errors.New("partstore: journal invalid")

	// ErrJournalBusy reports that another journal holds the directory's lock.
	ErrJournalBusy = errors.New("partstore: journal busy")

	// ErrJournalLimit reports a header, record, file or suffix beyond the fixed journal limits.
	ErrJournalLimit = errors.New("partstore: journal limit")
)

// journalHeader is the JSON payload of the first frame: the boot the memfds belong to, their
// exact kernel identities and the checkpoint every later record is replayed over.
type journalHeader struct {
	BootID     string           `json:"boot_id"`
	Identities []ipc.FDIdentity `json:"identities"`
	Checkpoint Checkpoint       `json:"checkpoint"`
}

// journalPrefix returns the fixed file prefix.
func journalPrefix() []byte {
	p := make([]byte, prefixLen)
	copy(p, journalMagic)
	binary.BigEndian.PutUint32(p[4:], journalVersion)
	return p
}

// encodeFrame builds one complete frame of kind around payload.
func encodeFrame(kind byte, payload []byte) []byte {
	out := make([]byte, frameHeadLen+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	h := sha256.New()
	h.Write(out[:5])
	h.Write(payload)
	copy(out[5:frameHeadLen], h.Sum(nil))
	copy(out[frameHeadLen:], payload)
	return out
}

// errIncomplete marks a frame cut short by the end of buf; the caller decides whether it may
// be dropped. Anything else wrong with a frame is fatal.
var errIncomplete = errors.New("incomplete frame")

// decodeFrame parses the frame at buf[pos:], requiring kind and a payload of at most maxLen
// bytes, and returns the payload and the position after the frame. The length is bounded
// before the payload is touched; a complete frame with a bad checksum is ErrJournalInvalid.
func decodeFrame(buf []byte, pos int, kind byte, maxLen int) ([]byte, int, error) {
	rest := buf[pos:]
	if len(rest) > 0 && rest[0] != kind {
		return nil, 0, fmt.Errorf("%w: frame kind %d at %d, want %d", ErrJournalInvalid, rest[0], pos, kind)
	}
	if len(rest) >= 5 {
		if n := binary.BigEndian.Uint32(rest[1:5]); uint64(n) > uint64(maxLen) {
			return nil, 0, fmt.Errorf("%w: frame at %d is %d bytes, limit %d", ErrJournalLimit, pos, n, maxLen)
		}
	}
	if len(rest) < frameHeadLen {
		return nil, 0, errIncomplete
	}
	n := int(binary.BigEndian.Uint32(rest[1:5]))
	if len(rest)-frameHeadLen < n {
		return nil, 0, errIncomplete
	}
	payload := rest[frameHeadLen : frameHeadLen+n]
	h := sha256.New()
	h.Write(rest[:5])
	h.Write(payload)
	if !bytes.Equal(h.Sum(nil), rest[5:frameHeadLen]) {
		return nil, 0, fmt.Errorf("%w: frame at %d checksum mismatch", ErrJournalInvalid, pos)
	}
	return payload, pos + frameHeadLen + n, nil
}

// decodeStrict parses exactly one JSON object into v: valid UTF-8, no unknown fields and
// nothing after the value.
func decodeStrict(data []byte, v any) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("%w: payload is not valid UTF-8", ErrJournalInvalid)
	}
	if trimmed := bytes.TrimLeft(data, " \t\r\n"); len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("%w: payload is not a JSON object", ErrJournalInvalid)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: decode: %v", ErrJournalInvalid, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return fmt.Errorf("%w: data after the JSON value", ErrJournalInvalid)
	}
	return nil
}

// currentBootID returns the kernel's boot id, read with a bounded buffer.
func currentBootID() (string, error) {
	f, err := os.Open(bootIDPath)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrJournalInvalid, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 128))
	if err != nil {
		return "", fmt.Errorf("%w: read boot id: %v", ErrJournalInvalid, err)
	}
	id := strings.TrimSuffix(string(b), "\n")
	if id == "" || len(b) == 128 || strings.ContainsAny(id, "\n\r\t ") || !utf8.ValidString(id) {
		return "", fmt.Errorf("%w: unusable boot id", ErrJournalInvalid)
	}
	return id, nil
}

// checkRecord requires rec to be record want of exactly this journal's scope, config and
// effective policy. Its operation and fields are the Ring's to validate.
func (j *Journal) checkRecord(rec Record, want uint64) error {
	switch {
	case rec.Version != RecordVersion:
		return fmt.Errorf("%w: record version %d, want %d", ErrJournalInvalid, rec.Version, RecordVersion)
	case rec.Run != j.run || rec.Source != j.source || rec.Epoch != j.epoch:
		return fmt.Errorf("%w: record %d belongs to another run, source or epoch", ErrJournalInvalid, rec.Seq)
	case rec.Seq != want:
		return fmt.Errorf("%w: record seq %d, want %d", ErrJournalInvalid, rec.Seq, want)
	case rec.Config != j.cfg:
		return fmt.Errorf("%w: record %d config differs from the journal config", ErrJournalInvalid, rec.Seq)
	case rec.Policy != j.policy:
		return fmt.Errorf("%w: record %d policy differs from the effective policy", ErrJournalInvalid, rec.Seq)
	}
	return nil
}

// checkHeader requires hdr to describe this boot, exactly the store's memfds and a checkpoint
// of this journal's scope, config and effective policy. Its retained state is
// RestoreCheckpoint's to validate.
func (j *Journal) checkHeader(hdr journalHeader) error {
	cp := hdr.Checkpoint
	switch {
	case hdr.BootID != j.bootID:
		return fmt.Errorf("%w: journal was written in boot %q, now %q", ErrJournalInvalid, hdr.BootID, j.bootID)
	case len(hdr.Identities) != len(j.ids):
		return fmt.Errorf("%w: journal lists %d memfds, store has %d", ErrIdentityMismatch, len(hdr.Identities), len(j.ids))
	case cp.Version != CheckpointVersion:
		return fmt.Errorf("%w: checkpoint version %d, want %d", ErrJournalInvalid, cp.Version, CheckpointVersion)
	case cp.Run != j.run || cp.Source != j.source || cp.Epoch != j.epoch:
		return fmt.Errorf("%w: checkpoint belongs to another run, source or epoch", ErrJournalInvalid)
	case cp.Config != j.cfg:
		return fmt.Errorf("%w: checkpoint config differs from the expected config", ErrJournalInvalid)
	case cp.Policy != j.policy:
		return fmt.Errorf("%w: checkpoint policy differs from the effective policy", ErrJournalInvalid)
	}
	for i, id := range hdr.Identities {
		if id != j.ids[i] {
			return fmt.Errorf("%w: journal memfd %d is %+v, store has %+v", ErrIdentityMismatch, i, id, j.ids[i])
		}
	}
	return nil
}

// encodeHeader builds the checkpoint header frame for cp, within the header limit.
func (j *Journal) encodeHeader(cp Checkpoint) ([]byte, error) {
	data, err := json.Marshal(journalHeader{BootID: j.bootID, Identities: j.ids, Checkpoint: cp})
	if err != nil {
		return nil, fmt.Errorf("%w: encode header: %v", ErrJournalInvalid, err)
	}
	if len(data) > maxHeaderJSON {
		return nil, fmt.Errorf("%w: header of %d bytes exceeds %d", ErrJournalLimit, len(data), maxHeaderJSON)
	}
	return encodeFrame(frameCheckpoint, data), nil
}

// parseHeader parses the prefix and the complete, valid checkpoint header that every journal
// must start with, and returns the header and the position of the first record frame.
func (j *Journal) parseHeader(buf []byte) (journalHeader, int, error) {
	if len(buf) < prefixLen || !bytes.Equal(buf[:prefixLen], journalPrefix()) {
		return journalHeader{}, 0, fmt.Errorf("%w: bad prefix", ErrJournalInvalid)
	}
	payload, pos, err := decodeFrame(buf, prefixLen, frameCheckpoint, maxHeaderJSON)
	if err != nil {
		return journalHeader{}, 0, fmt.Errorf("%w: header: %w", ErrJournalInvalid, err)
	}
	var hdr journalHeader
	if err := decodeStrict(payload, &hdr); err != nil {
		return journalHeader{}, 0, fmt.Errorf("header: %w", err)
	}
	if err := j.checkHeader(hdr); err != nil {
		return journalHeader{}, 0, err
	}
	return hdr, pos, nil
}

// parseRecords parses the record frames in buf[pos:] numbered from prev+1 and returns them
// with the frame start of each and the position after the last complete one. A complete frame
// that is wrong in any way is fatal; only an incomplete final frame is reported, by a
// position short of len(buf), for the caller to judge.
func (j *Journal) parseRecords(buf []byte, pos int, prev uint64) ([]Record, []int64, int, error) {
	var recs []Record
	var starts []int64
	for pos < len(buf) {
		if len(recs) >= maxSuffixRecords {
			return nil, nil, 0, fmt.Errorf("%w: more than %d suffix records", ErrJournalLimit, maxSuffixRecords)
		}
		payload, next, err := decodeFrame(buf, pos, frameRecord, maxRecordJSON)
		if errors.Is(err, errIncomplete) {
			return recs, starts, pos, nil
		}
		if err != nil {
			return nil, nil, 0, fmt.Errorf("%w: record at %d: %w", ErrJournalInvalid, pos, err)
		}
		var rec Record
		if err := decodeStrict(payload, &rec); err != nil {
			return nil, nil, 0, fmt.Errorf("record at %d: %w", pos, err)
		}
		if prev == ^uint64(0) {
			return nil, nil, 0, fmt.Errorf("%w: record sequence exhausted", ErrJournalLimit)
		}
		if err := j.checkRecord(rec, prev+1); err != nil {
			return nil, nil, 0, err
		}
		recs, starts, prev, pos = append(recs, rec), append(starts, int64(pos)), rec.Seq, next
	}
	return recs, starts, pos, nil
}
