//go:build linux

package partstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

type journalFixture struct {
	*testFixture
	dir string
	j   *Journal
	r   *Ring
}

func mustChmod(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

func mustWriteFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st
}

func mustSymlink(t *testing.T, oldname, newname string) {
	t.Helper()
	if err := os.Symlink(oldname, newname); err != nil {
		t.Fatalf("symlink %s -> %s: %v", newname, oldname, err)
	}
}

func mustLink(t *testing.T, oldname, newname string) {
	t.Helper()
	if err := os.Link(oldname, newname); err != nil {
		t.Fatalf("link %s -> %s: %v", newname, oldname, err)
	}
}

func mustSnapshot(t *testing.T, r *Ring) RingSnapshot {
	t.Helper()
	snap, err := r.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return snap
}

func newJournalFixture(t *testing.T, mode, codec string, capSize int64, pol Policy) *journalFixture {
	t.Helper()
	f := newTestFixture(t, mode, codec, capSize, pol)
	if err := f.ring.Close(); err != nil {
		t.Fatalf("close initial ring: %v", err)
	}

	dir := t.TempDir()
	mustChmod(t, dir, 0o700)

	j, r, err := CreateJournal(dir, f.store, f.run, f.source, f.epoch, f.cfg, pol)
	if err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	return &journalFixture{testFixture: f, dir: dir, j: j, r: r}
}

func (jf *journalFixture) commitInit(trk int, data []byte) ipc.Entry {
	jf.t.Helper()
	res, err := jf.r.Reserve(ipc.KindInit, trk, int64(len(data)))
	if err != nil {
		jf.t.Fatalf("Reserve init: %v", err)
	}
	mustPwrite(jf.t, jf.dups[trk], data, res.Offset)
	cStr := "avc1.64001f"
	if jf.cfg.Codec == ipc.CodecHEVC {
		cStr = "hvc1.1.6.L93.B0"
	}
	snap := mustSnapshot(jf.t, jf.r)
	entry := ipc.Entry{
		Version: ipc.Version, Run: jf.run, Source: jf.source, Epoch: jf.epoch,
		Seq: snap.EntryHigh + 1, Kind: ipc.KindInit, Track: trk,
		PublishedNS: 1e9, PublishedMonoNS: 1e9,
		Offset: res.Offset, Length: int64(len(data)),
		SHA256: testSHA(data), Codecs: cStr,
	}
	if err := jf.r.Commit(res, entry); err != nil {
		jf.t.Fatalf("Commit init: %v", err)
	}
	return entry
}

func (jf *journalFixture) commitPart(trk int, data []byte, msn int, pts, mono int64) ipc.Entry {
	jf.t.Helper()
	res, err := jf.r.Reserve(ipc.KindPart, trk, int64(len(data)))
	if err != nil {
		jf.t.Fatalf("Reserve part MSN %d: %v", msn, err)
	}
	mustPwrite(jf.t, jf.dups[trk], data, res.Offset)
	snap := mustSnapshot(jf.t, jf.r)
	entry := ipc.Entry{
		Version: ipc.Version, Run: jf.run, Source: jf.source, Epoch: jf.epoch,
		Seq: snap.EntryHigh + 1, Kind: ipc.KindPart, Track: trk,
		PublishedNS: pts, PublishedMonoNS: mono,
		Offset: res.Offset, Length: int64(len(data)),
		SHA256: testSHA(data),
		MSN:    msn, Index: 0, PTS: pts, DTS: pts, Duration: 1e9,
		Sequence: uint64(msn), FragmentStart: true, Independent: true, Payload: 1, FirstAU: msn - 1,
	}
	if err := jf.r.Commit(res, entry); err != nil {
		jf.t.Fatalf("Commit part MSN %d: %v", msn, err)
	}
	return entry
}

func (jf *journalFixture) closeJournal() {
	jf.t.Helper()
	if err := jf.j.Close(); err != nil {
		jf.t.Fatalf("Close journal: %v", err)
	}
}

func (jf *journalFixture) reopen() (*Journal, *Ring) {
	jf.t.Helper()
	j, r, err := OpenJournal(jf.dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, jf.policy)
	if err != nil {
		jf.t.Fatalf("OpenJournal: %v", err)
	}
	jf.t.Cleanup(func() { _ = j.Close() })
	jf.j, jf.r = j, r
	return j, r
}

func (jf *journalFixture) record(seq uint64, op string) Record {
	return Record{
		Version: RecordVersion, Run: jf.run, Source: jf.source, Epoch: jf.epoch,
		Seq: seq, Config: jf.cfg, Policy: Policy{Window: DefaultWindow, MaxSegment: DefaultMaxSegment},
		Op: op, Reservation: Reservation{ID: seq, Kind: ipc.KindPart, Track: 0, Offset: 64, Length: 80}, Track: 0,
	}
}

func (jf *journalFixture) recordFrame(rec Record) []byte {
	return encodeFrame(frameRecord, mustMarshal(rec))
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestJournalMatrixAndLifecycle(t *testing.T) {
	for _, mode := range []string{ipc.ModeSolo, ipc.Mode1v1} {
		for _, codec := range []string{ipc.CodecH264, ipc.CodecHEVC} {
			t.Run(fmt.Sprintf("%s_%s", mode, codec), func(t *testing.T) {
				jf := newJournalFixture(t, mode, codec, 4096, Policy{})

				missDir := t.TempDir()
				mustChmod(t, missDir, 0o700)
				if _, _, err := OpenJournal(missDir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("OpenJournal missing want ErrNotExist, got %v", err)
				}
				if _, err := os.Stat(filepath.Join(missDir, journalFile)); !os.IsNotExist(err) {
					t.Fatalf("OpenJournal missing must not bootstrap %s", journalFile)
				}

				if _, _, err := CreateJournal(jf.dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalBusy) {
					t.Fatalf("CreateJournal held lock want ErrJournalBusy, got %v", err)
				}

				snapEmpty := mustSnapshot(t, jf.r)
				if snapEmpty.RecordHigh != 0 || snapEmpty.EntryHigh != 0 {
					t.Fatalf("snapEmpty high-waters want 0, got rec=%d entry=%d", snapEmpty.RecordHigh, snapEmpty.EntryHigh)
				}

				initData := bytes.Repeat([]byte{0xAA}, 64)
				jf.commitInit(0, initData)
				partData := bytes.Repeat([]byte{0xBB}, 100)
				jf.commitPart(0, partData, 1, 1e9, 1e9)

				snapBefore := mustSnapshot(t, jf.r)
				if snapBefore.RecordHigh != 4 || snapBefore.EntryHigh != 2 {
					t.Fatalf("snapshot want RecordHigh 4 EntryHigh 2, got rec=%d entry=%d", snapBefore.RecordHigh, snapBefore.EntryHigh)
				}

				if err := jf.j.Compact(); err != nil {
					t.Fatalf("Compact: %v", err)
				}

				readIBefore, err := jf.r.ReadInit(0)
				if err != nil {
					t.Fatalf("ReadInit before close: %v", err)
				}
				if !bytes.Equal(readIBefore, initData) {
					t.Fatalf("ReadInit before close bytes mismatch: got %x, want %x", readIBefore, initData)
				}

				readPBefore, err := jf.r.ReadPart(0, 1, 0)
				if err != nil {
					t.Fatalf("ReadPart before close: %v", err)
				}
				if !bytes.Equal(readPBefore, partData) {
					t.Fatalf("ReadPart before close bytes mismatch: got %x, want %x", readPBefore, partData)
				}

				jf.closeJournal()

				jPath := filepath.Join(jf.dir, journalFile)
				bytesBefore := mustReadFile(t, jPath)
				if _, _, err := CreateJournal(jf.dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, os.ErrExist) {
					t.Fatalf("CreateJournal on existing closed dir want ErrExist, got %v", err)
				}
				bytesAfter := mustReadFile(t, jPath)
				if !bytes.Equal(bytesBefore, bytesAfter) {
					t.Fatalf("CreateJournal on existing modified journal.bin bytes")
				}

				j2, r2 := jf.reopen()
				snapAfter := mustSnapshot(t, r2)
				if !reflect.DeepEqual(snapBefore, snapAfter) {
					t.Fatalf("snapshot mismatch after reopen: before=%+v after=%+v", snapBefore, snapAfter)
				}

				readIAfter, err := r2.ReadInit(0)
				if err != nil {
					t.Fatalf("ReadInit after reopen: %v", err)
				}
				if !bytes.Equal(readIAfter, initData) {
					t.Fatalf("ReadInit after reopen bytes mismatch: got %x, want %x", readIAfter, initData)
				}

				readPAfter, err := r2.ReadPart(0, 1, 0)
				if err != nil {
					t.Fatalf("ReadPart after reopen: %v", err)
				}
				if !bytes.Equal(readPAfter, partData) {
					t.Fatalf("ReadPart after reopen bytes mismatch: got %x, want %x", readPAfter, partData)
				}

				var nilJ *Journal
				_ = nilJ.Close()
				if err := nilJ.Compact(); !errors.Is(err, ErrClosed) {
					t.Fatalf("nil Compact want ErrClosed, got %v", err)
				}
				zeroJ := &Journal{}
				_ = zeroJ.Close()
				if err := zeroJ.Compact(); !errors.Is(err, ErrClosed) {
					t.Fatalf("zero Compact want ErrClosed, got %v", err)
				}
				_ = j2.Close()
				if err := j2.Compact(); !errors.Is(err, ErrClosed) {
					t.Fatalf("closed Compact want ErrClosed, got %v", err)
				}
				if _, err := r2.Reserve(ipc.KindPart, 0, 32); !errors.Is(err, ErrClosed) {
					t.Fatalf("closed Ring want ErrClosed, got %v", err)
				}
				if _, err := jf.store.Identities(); err != nil {
					t.Fatalf("borrowed store closed: %v", err)
				}
			})
		}
	}
}

func TestPendingGrantDurability(t *testing.T) {
	jf := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})

	res, err := jf.r.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve init: %v", err)
	}

	if _, err := jf.r.Reserve(ipc.KindInit, 0, 64); !errors.Is(err, ErrReservation) {
		t.Fatalf("second Reserve want ErrReservation, got %v", err)
	}

	jf.closeJournal()

	_, r2 := jf.reopen()
	snap2 := mustSnapshot(t, r2)
	if len(snap2.Pending) != 1 || snap2.Pending[0] != res {
		t.Fatalf("pending grant want %+v, got %+v", res, snap2.Pending)
	}

	if _, err := r2.Reserve(ipc.KindInit, 0, 64); !errors.Is(err, ErrReservation) {
		t.Fatalf("reopened second Reserve want ErrReservation, got %v", err)
	}

	if err := r2.CancelAfterQuiescence(res); err != nil {
		t.Fatalf("CancelAfterQuiescence: %v", err)
	}

	snapCanceled := mustSnapshot(t, r2)
	if len(snapCanceled.Pending) != 0 {
		t.Fatalf("pending after cancel want 0, got %d", len(snapCanceled.Pending))
	}

	res2, err := r2.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve after cancel: %v", err)
	}
	if res2.ID <= res.ID {
		t.Fatalf("res2.ID %d not strictly greater than res.ID %d", res2.ID, res.ID)
	}
	if err := r2.CancelAfterQuiescence(res2); err != nil {
		t.Fatalf("cleanup cancel: %v", err)
	}
}

func TestPrefixRetiredByteProof(t *testing.T) {
	pol := Policy{Window: time.Second, MaxSegment: time.Second}
	jf := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 384, pol)

	initBytes := bytes.Repeat([]byte{0x10}, 64)
	jf.commitInit(0, initBytes)

	for msn := 1; msn <= 4; msn++ {
		partData := bytes.Repeat([]byte{byte(msn * 10)}, 80)
		jf.commitPart(0, partData, msn, int64(msn)*1e9, int64(msn)*1e9)
	}

	if _, err := jf.r.Reserve(ipc.KindPart, 0, 80); !errors.Is(err, ErrFull) {
		t.Fatalf("expected ErrFull on full track, got %v", err)
	}

	if err := jf.j.Compact(); err != nil {
		t.Fatalf("Compact before drop/retire: %v", err)
	}

	if err := jf.r.DropSegment(0, 1, 6e9); err != nil {
		t.Fatalf("DropSegment MSN1: %v", err)
	}

	if err := jf.r.RetireSegment(0, 1, 10e9); err != nil {
		t.Fatalf("RetireSegment MSN1 at 10s: %v", err)
	}

	res5, err := jf.r.Reserve(ipc.KindPart, 0, 80)
	if err != nil {
		t.Fatalf("Reserve MSN5: %v", err)
	}
	if res5.Offset != 64 {
		t.Fatalf("MSN5 expected offset 64 in retired extent, got %d", res5.Offset)
	}

	partData5 := bytes.Repeat([]byte{0x55}, 80)
	mustPwrite(t, jf.dups[0], partData5, res5.Offset)
	snap := mustSnapshot(t, jf.r)
	if err := jf.r.Commit(res5, ipc.Entry{
		Version: ipc.Version, Run: jf.run, Source: jf.source, Epoch: jf.epoch, Seq: snap.EntryHigh + 1,
		Kind: ipc.KindPart, Track: 0,
		PublishedNS: 5e9, PublishedMonoNS: 11e9,
		Offset: res5.Offset, Length: 80, SHA256: testSHA(partData5),
		MSN: 5, Index: 0, PTS: 5e9, DTS: 5e9, Duration: 1e9,
		Sequence: 5, FragmentStart: true, Independent: true, Payload: 1, FirstAU: 4,
	}); err != nil {
		t.Fatalf("Commit part MSN5: %v", err)
	}

	jf.closeJournal()

	_, r2 := jf.reopen()

	if _, err := r2.ReadSegment(0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MSN1 ReadSegment want ErrNotFound, got %v", err)
	}
	if _, err := r2.ReadPart(0, 1, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MSN1 ReadPart want ErrNotFound, got %v", err)
	}

	read5, err := r2.ReadPart(0, 5, 0)
	if err != nil {
		t.Fatalf("MSN5 ReadPart: %v", err)
	}
	if !bytes.Equal(read5, partData5) {
		t.Fatalf("MSN5 bytes mismatch: got %x, want %x", read5, partData5)
	}
}

func TestFramingTableAndTailTruncation(t *testing.T) {
	jf := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})
	jf.commitInit(0, bytes.Repeat([]byte{0x77}, 64))

	validCP, err := jf.r.ExportCheckpoint()
	if err != nil {
		t.Fatalf("ExportCheckpoint: %v", err)
	}
	validIDs, err := jf.store.Identities()
	if err != nil {
		t.Fatalf("Identities: %v", err)
	}
	currentBoot, err := currentBootID()
	if err != nil {
		t.Fatalf("currentBootID: %v", err)
	}

	jf.closeJournal()

	goodBytes := mustReadFile(t, filepath.Join(jf.dir, journalFile))

	tailCases := []struct {
		name string
		tail []byte
	}{
		{name: "kind_only", tail: []byte{frameRecord}},
		{name: "partial_header", tail: append([]byte{frameRecord, 0, 0, 0, 10}, bytes.Repeat([]byte{0xEE}, 10)...)},
		{name: "partial_payload", tail: func() []byte {
			f := jf.recordFrame(jf.record(3, OpReserve))
			return f[:len(f)-5]
		}()},
	}

	for _, tc := range tailCases {
		t.Run("recover_"+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			mustChmod(t, dir, 0o700)
			jPath := filepath.Join(dir, journalFile)
			corruptContent := append(append([]byte(nil), goodBytes...), tc.tail...)
			mustWriteFile(t, jPath, corruptContent, 0o600)

			jRec, rRec, err := OpenJournal(dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{})
			if err != nil {
				t.Fatalf("OpenJournal should succeed for incomplete final tail %s: %v", tc.name, err)
			}
			t.Cleanup(func() { _ = jRec.Close() })

			st := mustStat(t, jPath)
			if st.Size() != int64(len(goodBytes)) {
				t.Fatalf("size after recovery want %d, got %d", len(goodBytes), st.Size())
			}

			resNew, err := rRec.Reserve(ipc.KindPart, 0, 50)
			if err != nil {
				t.Fatalf("Reserve after recovery: %v", err)
			}
			if err := rRec.CancelAfterQuiescence(resNew); err != nil {
				t.Fatalf("Cancel after recovery: %v", err)
			}

			if err := jRec.Close(); err != nil {
				t.Fatalf("Close after append: %v", err)
			}

			jRec2, _, err := OpenJournal(dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{})
			if err != nil {
				t.Fatalf("Reopen after append: %v", err)
			}
			_ = jRec2.Close()
		})
	}

	rec3Valid := jf.record(3, OpReserve)
	rec3Valid.Reservation = Reservation{ID: 3, Kind: ipc.KindPart, Track: 0, Offset: 64, Length: 80}
	f3Valid := jf.recordFrame(rec3Valid)

	rec4Valid := jf.record(4, OpCancel)
	rec4Valid.Reservation = Reservation{ID: 3, Kind: ipc.KindPart, Track: 0, Offset: 64, Length: 80}
	f4Valid := jf.recordFrame(rec4Valid)

	t.Run("valid_unmutated_control", func(t *testing.T) {
		dir := t.TempDir()
		mustChmod(t, dir, 0o700)
		jPath := filepath.Join(dir, journalFile)
		validContent := append(append(append([]byte(nil), goodBytes...), f3Valid...), f4Valid...)
		mustWriteFile(t, jPath, validContent, 0o600)

		jCtrl, rCtrl, err := OpenJournal(dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{})
		if err != nil {
			t.Fatalf("OpenJournal valid control: %v", err)
		}
		snap := mustSnapshot(t, rCtrl)
		if snap.RecordHigh != 4 || len(snap.Pending) != 0 {
			_ = jCtrl.Close()
			t.Fatalf("valid control state mismatch: rec=%d pending=%d", snap.RecordHigh, len(snap.Pending))
		}
		if err := jCtrl.Close(); err != nil {
			t.Fatalf("Close valid control: %v", err)
		}
	})

	badFrame := append([]byte(nil), f3Valid...)
	badFrame[5] ^= 0xFF

	var mUnknown map[string]json.RawMessage
	if err := json.Unmarshal(mustMarshal(rec3Valid), &mUnknown); err != nil {
		t.Fatalf("unmarshal rec3Valid: %v", err)
	}
	mUnknown["unknown_prop"] = json.RawMessage(`123`)

	foreignRec := jf.record(3, OpReserve)
	foreignRec.Run = "other-run"

	badBootHdr := mustMarshal(journalHeader{BootID: "foreign-boot-id", Identities: validIDs, Checkpoint: validCP})

	badIDs := append([]ipc.FDIdentity(nil), validIDs...)
	badIDs[0].Ino += 99999
	badIDsHdr := mustMarshal(journalHeader{BootID: currentBoot, Identities: badIDs, Checkpoint: validCP})

	overHead := make([]byte, frameHeadLen)
	overHead[0] = frameRecord
	binary.BigEndian.PutUint32(overHead[1:5], uint32(maxRecordJSON+100))

	failCases := []struct {
		name string
		data []byte
		want error
	}{
		{name: "complete_last_bad_checksum", data: append(append([]byte(nil), goodBytes...), badFrame...), want: ErrJournalInvalid},
		{name: "complete_interior_bad_checksum", data: append(append(append([]byte(nil), goodBytes...), badFrame...), f4Valid...), want: ErrJournalInvalid},
		{name: "valid_checksum_bad_json", data: append(append([]byte(nil), goodBytes...), encodeFrame(frameRecord, []byte("{not valid json"))...), want: ErrJournalInvalid},
		{name: "valid_checksum_unknown_property", data: append(append([]byte(nil), goodBytes...), encodeFrame(frameRecord, mustMarshal(mUnknown))...), want: ErrJournalInvalid},
		{name: "valid_checksum_non_utf8", data: append(append([]byte(nil), goodBytes...), encodeFrame(frameRecord, []byte("{\"Run\":\"\xff\xfe\"}"))...), want: ErrJournalInvalid},
		{name: "valid_checksum_gapped_sequence", data: append(append([]byte(nil), goodBytes...), jf.recordFrame(jf.record(99, OpReserve))...), want: ErrJournalInvalid},
		{name: "valid_checksum_foreign_scope", data: append(append([]byte(nil), goodBytes...), jf.recordFrame(foreignRec)...), want: ErrJournalInvalid},
		{name: "valid_checksum_bad_op", data: append(append([]byte(nil), goodBytes...), jf.recordFrame(jf.record(3, "bogus_op"))...), want: ErrInvalidPublication},
		{name: "partial_first_header", data: goodBytes[:15], want: ErrJournalInvalid},
		{name: "bad_boot", data: append(journalPrefix(), encodeFrame(frameCheckpoint, badBootHdr)...), want: ErrJournalInvalid},
		{name: "bad_identities", data: append(journalPrefix(), encodeFrame(frameCheckpoint, badIDsHdr)...), want: ErrIdentityMismatch},
		{name: "oversized_frame", data: append(append([]byte(nil), goodBytes...), overHead...), want: ErrJournalLimit},
	}

	for _, fc := range failCases {
		t.Run("fail_"+fc.name, func(t *testing.T) {
			dir := t.TempDir()
			mustChmod(t, dir, 0o700)
			jPath := filepath.Join(dir, journalFile)

			mustWriteFile(t, jPath, fc.data, 0o600)

			_, _, err := OpenJournal(dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{})
			if !errors.Is(err, fc.want) {
				t.Fatalf("%s want %v, got %v", fc.name, fc.want, err)
			}

			afterBytes := mustReadFile(t, jPath)
			if !bytes.Equal(fc.data, afterBytes) {
				t.Fatalf("file bytes modified on failure for %s", fc.name)
			}
		})
	}

	t.Run("sparse_oversized_file", func(t *testing.T) {
		dir := t.TempDir()
		mustChmod(t, dir, 0o700)
		jPath := filepath.Join(dir, journalFile)
		fSparse, err := os.OpenFile(jPath, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatalf("create sparse: %v", err)
		}
		const oversize = int64(maxJournalFile + 10)
		if err := fSparse.Truncate(oversize); err != nil {
			_ = fSparse.Close()
			t.Fatalf("truncate sparse: %v", err)
		}
		if err := fSparse.Close(); err != nil {
			t.Fatalf("close sparse: %v", err)
		}

		_, _, err = OpenJournal(dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{})
		if !errors.Is(err, ErrJournalLimit) {
			t.Fatalf("sparse file want ErrJournalLimit, got %v", err)
		}
		st := mustStat(t, jPath)
		if st.Size() != oversize {
			t.Fatalf("sparse size changed: want %d, got %d", oversize, st.Size())
		}
	})

	t.Run("corrupt_memfd_plus_torn_tail", func(t *testing.T) {
		jfTorn := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})
		jfTorn.commitInit(0, bytes.Repeat([]byte{0x44}, 64))
		jfTorn.closeJournal()

		jPath := filepath.Join(jfTorn.dir, journalFile)
		fAppend, err := os.OpenFile(jPath, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatalf("open append: %v", err)
		}
		if _, err := fAppend.Write([]byte{frameRecord}); err != nil {
			_ = fAppend.Close()
			t.Fatalf("write torn byte: %v", err)
		}
		if err := fAppend.Close(); err != nil {
			t.Fatalf("close append: %v", err)
		}

		statBefore := mustStat(t, jPath)
		sizeBefore := statBefore.Size()

		mustPwrite(t, jfTorn.dups[0], bytes.Repeat([]byte{0x99}, 64), 0)

		_, _, err = OpenJournal(jfTorn.dir, jfTorn.store, jfTorn.run, jfTorn.source, jfTorn.epoch, jfTorn.cfg, Policy{})
		if err == nil {
			t.Fatalf("OpenJournal should fail on corrupt memfd digest")
		}

		statAfter := mustStat(t, jPath)
		if statAfter.Size() != sizeBefore {
			t.Fatalf("torn tail was trimmed despite corrupt memfd: before=%d after=%d", sizeBefore, statAfter.Size())
		}
	})
}

func TestHookFailures(t *testing.T) {
	for _, hookPoint := range []string{"append_written", "append_synced"} {
		t.Run("hook_"+hookPoint, func(t *testing.T) {
			jf := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})

			jf.j.hook = func(pt string) error {
				if pt == hookPoint {
					return fmt.Errorf("injected %s hook failure", hookPoint)
				}
				return nil
			}

			res, err := jf.r.Reserve(ipc.KindInit, 0, 64)
			if !errors.Is(err, ErrPoisoned) || res != (Reservation{}) {
				t.Fatalf("Reserve want ErrPoisoned and zero res, got err=%v res=%+v", err, res)
			}

			snap := mustSnapshot(t, jf.r)
			if len(snap.Pending) != 0 || snap.RecordHigh != 0 {
				t.Fatalf("state advanced in poisoned ring: pending=%d high=%d", len(snap.Pending), snap.RecordHigh)
			}

			if _, err := jf.r.Reserve(ipc.KindInit, 0, 64); !errors.Is(err, ErrPoisoned) {
				t.Fatalf("subsequent mutation want ErrPoisoned, got %v", err)
			}

			jf.closeJournal()

			_, r2 := jf.reopen()
			snap2 := mustSnapshot(t, r2)
			if len(snap2.Pending) != 1 || snap2.Pending[0].ID != 1 || snap2.RecordHigh != 1 {
				t.Fatalf("restored unacknowledged record want ID 1 high 1, got pending %+v high %d", snap2.Pending, snap2.RecordHigh)
			}
		})
	}

	for _, tc := range []struct {
		point string
		base  uint64
	}{
		{"compact_temp_synced", 0}, {"compact_before_rename", 0},
		{"compact_renamed", 4}, {"compact_dir_synced", 4},
	} {
		t.Run("hook_"+tc.point, func(t *testing.T) {
			jf := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})
			iData := bytes.Repeat([]byte{0x12}, 64)
			jf.commitInit(0, iData)
			pData := bytes.Repeat([]byte{0x34}, 80)
			jf.commitPart(0, pData, 1, 1e9, 1e9)

			jf.j.hook = func(pt string) error {
				if pt == tc.point {
					return fmt.Errorf("injected %s hook failure", tc.point)
				}
				return nil
			}

			if err := jf.j.Compact(); !errors.Is(err, ErrPoisoned) {
				t.Fatalf("Compact want ErrPoisoned, got %v", err)
			}
			jf.closeJournal()

			j2, r2 := jf.reopen()
			snap2 := mustSnapshot(t, r2)
			if snap2.RecordHigh != 4 || snap2.EntryHigh != 2 || j2.base != tc.base {
				t.Fatalf("reopened state mismatch: rec=%d entry=%d base=%d wantBase=%d", snap2.RecordHigh, snap2.EntryHigh, j2.base, tc.base)
			}
			readI, err := r2.ReadInit(0)
			if err != nil || !bytes.Equal(readI, iData) {
				t.Fatalf("ReadInit mismatch: %v", err)
			}
			readP, err := r2.ReadPart(0, 1, 0)
			if err != nil || !bytes.Equal(readP, pData) {
				t.Fatalf("ReadPart mismatch: %v", err)
			}
		})
	}

	t.Run("prefix_only_noop_and_clean_suffix", func(t *testing.T) {
		jf := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})

		if err := jf.j.Compact(); err != nil || jf.j.base != 0 {
			t.Fatalf("empty Compact want nil err base 0, got err=%v base=%d", err, jf.j.base)
		}

		jf.commitInit(0, bytes.Repeat([]byte{0x33}, 64))
		jf.commitPart(0, bytes.Repeat([]byte{0x44}, 80), 1, 1e9, 1e9)

		snapBeforeCompact := mustSnapshot(t, jf.r)
		if snapBeforeCompact.RecordHigh != 4 || snapBeforeCompact.EntryHigh != 2 {
			t.Fatalf("before compact want RecordHigh 4, EntryHigh 2; got rec=%d entry=%d",
				snapBeforeCompact.RecordHigh, snapBeforeCompact.EntryHigh)
		}

		if err := jf.j.Compact(); err != nil || jf.j.base != 4 {
			t.Fatalf("after compact want base 4, got err=%v base=%d", err, jf.j.base)
		}

		if err := jf.j.Compact(); err != nil || jf.j.base != 4 {
			t.Fatalf("second Compact no-op want base 4, got err=%v base=%d", err, jf.j.base)
		}

		resSuffix, err := jf.r.Reserve(ipc.KindPart, 0, 80)
		if err != nil || resSuffix.ID != 5 {
			t.Fatalf("Reserve suffix want ID 5, got err=%v id=%d", err, resSuffix.ID)
		}

		if err := jf.j.Compact(); err != nil || jf.j.base != 5 {
			t.Fatalf("after suffix compact want base 5, got err=%v base=%d", err, jf.j.base)
		}
	})
}

func TestLockPermissionsAndIdentities(t *testing.T) {
	jf := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})

	t.Run("lock_contention", func(t *testing.T) {
		if _, _, err := OpenJournal(jf.dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalBusy) {
			t.Fatalf("OpenJournal on held lock want ErrJournalBusy, got %v", err)
		}
	})

	createClosedDir := func(tb *testing.T) string {
		tb.Helper()
		d := tb.TempDir()
		mustChmod(tb, d, 0o700)
		j, _, err := CreateJournal(d, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{})
		if err != nil {
			tb.Fatalf("CreateJournal: %v", err)
		}
		if err := j.Close(); err != nil {
			tb.Fatalf("Close: %v", err)
		}
		return d
	}

	t.Run("unsafe_permissions", func(t *testing.T) {
		badDir := t.TempDir()
		mustChmod(t, badDir, 0o755)
		if _, _, err := CreateJournal(badDir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalInvalid) {
			t.Fatalf("CreateJournal 0755 dir want ErrJournalInvalid, got %v", err)
		}

		dir := createClosedDir(t)
		jPath := filepath.Join(dir, journalFile)
		mustChmod(t, jPath, 0o644)
		if _, _, err := OpenJournal(dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalInvalid) {
			t.Fatalf("OpenJournal 0644 journal.bin want ErrJournalInvalid, got %v", err)
		}
		mustChmod(t, jPath, 0o600)

		lPath := filepath.Join(dir, journalLock)
		mustChmod(t, lPath, 0o644)
		if _, _, err := OpenJournal(dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalInvalid) {
			t.Fatalf("OpenJournal 0644 .journal.lock want ErrJournalInvalid, got %v", err)
		}
	})

	t.Run("symlinks", func(t *testing.T) {
		dir := createClosedDir(t)

		symDir := filepath.Join(t.TempDir(), "symlink-dir")
		mustSymlink(t, dir, symDir)
		if _, _, err := OpenJournal(symDir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalInvalid) {
			t.Fatalf("OpenJournal symlink dir want ErrJournalInvalid, got %v", err)
		}

		targetFile := filepath.Join(dir, journalFile)
		targetBytes := mustReadFile(t, targetFile)
		dirFileSym := t.TempDir()
		mustChmod(t, dirFileSym, 0o700)
		mustSymlink(t, targetFile, filepath.Join(dirFileSym, journalFile))
		if _, _, err := OpenJournal(dirFileSym, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalInvalid) {
			t.Fatalf("OpenJournal symlink journal.bin want ErrJournalInvalid, got %v", err)
		}
		afterFileBytes := mustReadFile(t, targetFile)
		if !bytes.Equal(targetBytes, afterFileBytes) {
			t.Fatalf("target file modified through symlink")
		}

		targetLock := filepath.Join(t.TempDir(), "target.lock")
		lockBytes := []byte("lock content")
		mustWriteFile(t, targetLock, lockBytes, 0o600)
		lockDir := createClosedDir(t)
		lockPath := filepath.Join(lockDir, journalLock)
		if err := os.Remove(lockPath); err != nil {
			t.Fatalf("remove lockPath: %v", err)
		}
		mustSymlink(t, targetLock, lockPath)
		if _, _, err := OpenJournal(lockDir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalInvalid) {
			t.Fatalf("OpenJournal symlink .journal.lock want ErrJournalInvalid, got %v", err)
		}
		afterLockBytes := mustReadFile(t, targetLock)
		if !bytes.Equal(lockBytes, afterLockBytes) {
			t.Fatalf("target lock modified through symlink")
		}
	})

	t.Run("hardlinked_data", func(t *testing.T) {
		dir := createClosedDir(t)
		jPath := filepath.Join(dir, journalFile)
		hardLink := filepath.Join(dir, "journal.bin.hard")
		mustLink(t, jPath, hardLink)
		defer os.Remove(hardLink)
		if _, _, err := OpenJournal(dir, jf.store, jf.run, jf.source, jf.epoch, jf.cfg, Policy{}); !errors.Is(err, ErrJournalInvalid) {
			t.Fatalf("OpenJournal hardlinked journal.bin want ErrJournalInvalid, got %v", err)
		}
	})

	t.Run("foreign_identities_refusal", func(t *testing.T) {
		dir := createClosedDir(t)
		jPath := filepath.Join(dir, journalFile)
		bytesBefore := mustReadFile(t, jPath)

		fForeign := newTestFixture(t, jf.cfg.Mode, jf.cfg.Codec, jf.cfg.Capacity, Policy{})
		if err := fForeign.ring.Close(); err != nil {
			t.Fatalf("close foreign ring: %v", err)
		}

		if _, _, err := OpenJournal(dir, fForeign.store, fForeign.run, fForeign.source, fForeign.epoch, fForeign.cfg, Policy{}); !errors.Is(err, ErrIdentityMismatch) {
			t.Fatalf("OpenJournal foreign store want ErrIdentityMismatch, got %v", err)
		}
		bytesAfter := mustReadFile(t, jPath)
		if !bytes.Equal(bytesBefore, bytesAfter) {
			t.Fatalf("journal file modified on identity mismatch")
		}
	})
}

func TestBoundedConcurrency(t *testing.T) {
	jf := newJournalFixture(t, ipc.ModeSolo, ipc.CodecH264, 4096, Policy{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errCh := make(chan error, 32)
	compactDone := make(chan struct{}, 1)
	var compactCount atomic.Int32
	var closing atomic.Bool
	var wg sync.WaitGroup

	t.Cleanup(func() {
		cancel()
		closing.Store(true)
		_ = jf.j.Close()
		wg.Wait()
	})

	sendErr := func(format string, args ...any) {
		select {
		case errCh <- fmt.Errorf(format, args...):
		default:
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				res, err := jf.r.Reserve(ipc.KindInit, 0, 32)
				if err != nil {
					if errors.Is(err, ErrClosed) && closing.Load() {
						return
					}
					sendErr("mutation reserve: %w", err)
					return
				}
				if err := jf.r.CancelAfterQuiescence(res); err != nil {
					if errors.Is(err, ErrClosed) && closing.Load() {
						return
					}
					sendErr("mutation cancel: %w", err)
					return
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := jf.j.Compact(); err != nil {
					if errors.Is(err, ErrClosed) && closing.Load() {
						return
					}
					sendErr("compact: %w", err)
					return
				}
				if compactCount.Add(1) >= 2 {
					select {
					case compactDone <- struct{}{}:
					default:
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if _, err := jf.r.Snapshot(); err != nil {
					if errors.Is(err, ErrClosed) && closing.Load() {
						return
					}
					sendErr("snapshot: %w", err)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	select {
	case <-compactDone:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for 2 compacts under 2s ceiling")
	}

	closing.Store(true)
	if err := jf.j.Close(); err != nil {
		t.Fatalf("coordinator j.Close: %v", err)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("worker unexpected error: %v", err)
	}
}
