//go:build linux

package partstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

func cloneCheckpoint(cp Checkpoint) Checkpoint {
	b, err := json.Marshal(cp)
	if err != nil {
		panic(err)
	}
	var out Checkpoint
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return out
}

func noopPersist(Record) error { return nil }

func commitInitEntry(t *testing.T, f *testFixture, trk int, seq uint64, codec string, data []byte) {
	t.Helper()
	res, err := f.ring.Reserve(ipc.KindInit, trk, int64(len(data)))
	if err != nil {
		t.Fatalf("Reserve init trk %d: %v", trk, err)
	}
	mustPwrite(t, f.dups[trk], data, 0)
	cStr := "avc1.64001f"
	if codec == ipc.CodecHEVC && trk < ipc.TrackAudioA {
		cStr = "hvc1.1.6.L93.B0"
	} else if trk >= ipc.TrackAudioA {
		cStr = "mp4a.40.2"
	}
	if err := f.ring.Commit(res, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: seq,
		Kind: ipc.KindInit, Track: trk, PublishedNS: 1e9, PublishedMonoNS: 1e9,
		Offset: 0, Length: int64(len(data)), SHA256: testSHA(data), Codecs: cStr,
	}); err != nil {
		t.Fatalf("Commit init trk %d: %v", trk, err)
	}
}

func commitPartEntry(t *testing.T, f *testFixture, trk int, seq uint64, msn int, dur int64, data []byte) Reservation {
	t.Helper()
	res, err := f.ring.Reserve(ipc.KindPart, trk, int64(len(data)))
	if err != nil {
		t.Fatalf("Reserve part trk %d msn %d: %v", trk, msn, err)
	}
	mustPwrite(t, f.dups[trk], data, res.Offset)
	if err := f.ring.Commit(res, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: seq,
		Kind: ipc.KindPart, Track: trk, PublishedNS: 2e9, PublishedMonoNS: 2e9,
		Offset: res.Offset, Length: int64(len(data)), SHA256: testSHA(data),
		MSN: msn, Index: 0, PTS: int64(msn) * 1e9, DTS: int64(msn) * 1e9, Duration: dur,
		Sequence: uint64(msn), FragmentStart: true, Independent: true, Payload: 1, FirstAU: msn - 1,
	}); err != nil {
		t.Fatalf("Commit part trk %d msn %d: %v", trk, msn, err)
	}
	return res
}

func closeAllSegments(cp *Checkpoint) {
	for trkIdx := range cp.Tracks {
		for segIdx := range cp.Tracks[trkIdx].Segments {
			cp.Tracks[trkIdx].Segments[segIdx].Closed = true
		}
	}
}

func TestCheckpointExportRestoreMatrix(t *testing.T) {
	for _, mode := range []string{ipc.ModeSolo, ipc.Mode1v1} {
		for _, codec := range []string{ipc.CodecH264, ipc.CodecHEVC} {
			t.Run(fmt.Sprintf("%s_%s", mode, codec), func(t *testing.T) {
				f := newTestFixture(t, mode, codec, 4096, Policy{})

				cpEmpty, err := f.ring.ExportCheckpoint()
				if err != nil || cpEmpty.RecordHigh != 0 || len(cpEmpty.Tracks) != f.cfg.Tracks {
					t.Fatalf("ExportCheckpoint empty: %v, %+v", err, cpEmpty)
				}
				recovEmpty, err := RestoreCheckpoint(f.store, cpEmpty, nil, noopPersist)
				if err != nil {
					t.Fatalf("RestoreCheckpoint empty: %v", err)
				}
				cpRecovEmpty, err := recovEmpty.ExportCheckpoint()
				if err != nil {
					t.Fatalf("recovEmpty.ExportCheckpoint: %v", err)
				}
				snapEmpty, err := f.ring.Snapshot()
				if err != nil {
					t.Fatalf("Snapshot empty: %v", err)
				}
				snapRecovEmpty, err := recovEmpty.Snapshot()
				if err != nil {
					t.Fatalf("recovEmpty.Snapshot: %v", err)
				}
				if !reflect.DeepEqual(cpEmpty, cpRecovEmpty) || !reflect.DeepEqual(snapEmpty, snapRecovEmpty) {
					t.Fatalf("empty restore mismatch")
				}
				if err := recovEmpty.Close(); err != nil {
					t.Fatalf("recovEmpty.Close: %v", err)
				}

				var seq uint64 = 1
				inits, parts := make([][]byte, f.cfg.Tracks), make([][]byte, f.cfg.Tracks)
				for trk := 0; trk < f.cfg.Tracks; trk++ {
					inits[trk] = bytes.Repeat([]byte{byte(0x10 + trk)}, 64)
					commitInitEntry(t, f, trk, seq, codec, inits[trk])
					seq++
				}
				for trk := ipc.TrackAudioA; trk < f.cfg.Tracks; trk++ {
					if err := f.ring.Apply(ipc.Entry{
						Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: seq,
						Kind: ipc.KindPhases, Track: trk, PublishedNS: 15e8, PublishedMonoNS: 15e8,
						PhaseVideo: 100, PhaseAudio: 100,
					}); err != nil {
						t.Fatalf("Apply phases trk %d: %v", trk, err)
					}
					seq++
				}
				for trk := 0; trk < f.cfg.Tracks; trk++ {
					parts[trk] = bytes.Repeat([]byte{byte(0x20 + trk)}, 64)
					dur := int64(333333333)
					if trk >= ipc.TrackAudioA {
						dur = 320000000
					}
					commitPartEntry(t, f, trk, seq, 1, dur, parts[trk])
					seq++
				}

				cpPop, err := f.ring.ExportCheckpoint()
				if err != nil {
					t.Fatalf("ExportCheckpoint populated: %v", err)
				}
				snapPop, err := f.ring.Snapshot()
				if err != nil {
					t.Fatalf("Snapshot populated: %v", err)
				}

				recovPop, err := RestoreCheckpoint(f.store, cpPop, nil, noopPersist)
				if err != nil {
					t.Fatalf("RestoreCheckpoint populated: %v", err)
				}
				t.Cleanup(func() { _ = recovPop.Close() })

				cpRecovPop, err := recovPop.ExportCheckpoint()
				if err != nil {
					t.Fatalf("recovPop.ExportCheckpoint: %v", err)
				}
				snapRecovPop, err := recovPop.Snapshot()
				if err != nil {
					t.Fatalf("recovPop.Snapshot: %v", err)
				}
				if !reflect.DeepEqual(cpPop, cpRecovPop) || !reflect.DeepEqual(snapPop, snapRecovPop) {
					t.Fatalf("populated restore deep-equal mismatch")
				}
				for trk := 0; trk < f.cfg.Tracks; trk++ {
					bInit, err := recovPop.ReadInit(trk)
					if err != nil || !bytes.Equal(bInit, inits[trk]) {
						t.Fatalf("ReadInit trk %d mismatch: %v", trk, err)
					}
					bPart, err := recovPop.ReadPart(trk, 1, 0)
					if err != nil || !bytes.Equal(bPart, parts[trk]) {
						t.Fatalf("ReadPart trk %d mismatch: %v", trk, err)
					}
				}

				fPart := newTestFixture(t, mode, codec, 2048, Policy{})
				commitInitEntry(t, fPart, 0, 1, codec, inits[0])
				commitPartEntry(t, fPart, 0, 2, 1, 333333333, parts[0])
				if err := fPart.ring.Apply(ipc.Entry{
					Version: ipc.Version, Run: fPart.run, Source: fPart.source, Epoch: fPart.epoch, Seq: 3,
					Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, PublishedNS: 3e9, PublishedMonoNS: 3e9,
					Verified: false, Reason: "partial failure",
				}); err != nil {
					t.Fatalf("Apply unverified terminal: %v", err)
				}
				cpPart, err := fPart.ring.ExportCheckpoint()
				if err != nil {
					t.Fatalf("ExportCheckpoint partial terminal: %v", err)
				}
				recovPart, err := RestoreCheckpoint(fPart.store, cpPart, nil, noopPersist)
				if err != nil {
					t.Fatalf("RestoreCheckpoint partial terminal: %v", err)
				}
				snapPart, err := recovPart.Snapshot()
				if err != nil {
					t.Fatalf("recovPart.Snapshot: %v", err)
				}
				if snapPart.Terminal == nil || snapPart.Terminal.Verified || !snapPart.Tracks[0].Segments[0].Closed {
					t.Fatalf("partial terminal snapshot invalid: %+v", snapPart.Terminal)
				}
				if err := recovPart.Close(); err != nil {
					t.Fatalf("recovPart.Close: %v", err)
				}

				badVerified := cloneCheckpoint(cpPart)
				badVerified.Terminal.Verified = true
				if _, err := RestoreCheckpoint(fPart.store, badVerified, nil, noopPersist); !errors.Is(err, ErrInvalidPublication) {
					t.Fatalf("verified terminal on partial tracks got %v, want ErrInvalidPublication", err)
				}

				if err := f.ring.Apply(ipc.Entry{
					Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: seq,
					Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, PublishedNS: 4e9, PublishedMonoNS: 4e9,
					Verified: true, Reason: "clean shutdown",
				}); err != nil {
					t.Fatalf("Apply verified terminal: %v", err)
				}
				cpTerm, err := f.ring.ExportCheckpoint()
				if err != nil {
					t.Fatalf("ExportCheckpoint full terminal: %v", err)
				}
				recovTerm, err := RestoreCheckpoint(f.store, cpTerm, nil, noopPersist)
				if err != nil {
					t.Fatalf("RestoreCheckpoint full terminal: %v", err)
				}
				snapTerm, err := recovTerm.Snapshot()
				if err != nil {
					t.Fatalf("recovTerm.Snapshot: %v", err)
				}
				if snapTerm.Terminal == nil || !snapTerm.Terminal.Verified {
					t.Fatalf("verified terminal missing in snapshot")
				}
				for trkIdx, trkSnap := range snapTerm.Tracks {
					if len(trkSnap.Segments) == 0 || !trkSnap.Segments[0].Closed {
						t.Fatalf("track %d segment not closed by terminal", trkIdx)
					}
				}
				if err := recovTerm.Close(); err != nil {
					t.Fatalf("recovTerm.Close: %v", err)
				}
			})
		}
	}
}

func TestCheckpointWrapAndRetirementCase(t *testing.T) {
	policy := Policy{Window: 1 * time.Second, MaxSegment: 1 * time.Second}
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 384, policy)

	initData := bytes.Repeat([]byte{0xaa}, 64)
	commitInitEntry(t, f, 0, 1, ipc.CodecH264, initData)

	durs := []int64{1e9, 5e8, 1e9, 1e9}
	expectedOffsets := []int64{64, 144, 224, 304}
	partDataList := make([][]byte, 4)

	for msn := 1; msn <= 4; msn++ {
		pData := bytes.Repeat([]byte{byte(0x10 * msn)}, 80)
		partDataList[msn-1] = pData
		res := commitPartEntry(t, f, 0, uint64(msn+1), msn, durs[msn-1], pData)
		if res.Offset != expectedOffsets[msn-1] {
			t.Fatalf("MSN %d offset %d, want %d", msn, res.Offset, expectedOffsets[msn-1])
		}
	}

	cpPrefix, err := f.ring.ExportCheckpoint()
	if err != nil || cpPrefix.Tracks[0].Cursor != 0 {
		t.Fatalf("ExportCheckpoint prefix failed or cursor %d != 0: %v", cpPrefix.Tracks[0].Cursor, err)
	}
	prefixRecordsLen := len(f.records)

	if err := f.ring.DropSegment(0, 1, 6e9); err != nil {
		t.Fatalf("DropSegment 1: %v", err)
	}
	if err := f.ring.DropSegment(0, 2, 6e9); err != nil {
		t.Fatalf("DropSegment 2: %v", err)
	}
	if err := f.ring.RetireSegment(0, 2, 95e8); err != nil {
		t.Fatalf("RetireSegment 2: %v", err)
	}

	resPart5, err := f.ring.Reserve(ipc.KindPart, 0, 80)
	if err != nil || resPart5.Offset != 144 {
		t.Fatalf("Reserve part 5 offset %d, want 144: %v", resPart5.Offset, err)
	}
	p5Data := bytes.Repeat([]byte{0x55}, 80)
	mustPwrite(t, f.dups[0], p5Data, 144)

	cpCurrent, err := f.ring.ExportCheckpoint()
	if err != nil || cpCurrent.Tracks[0].Cursor != 224 || cpCurrent.Tracks[0].Pending == nil {
		t.Fatalf("ExportCheckpoint current: %v, %+v", err, cpCurrent.Tracks[0])
	}
	snapCurrent, err := f.ring.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot current: %v", err)
	}

	var persistCalled bool
	recordPersist := func(Record) error { persistCalled = true; return nil }

	recovCurrent, err := RestoreCheckpoint(f.store, cpCurrent, nil, recordPersist)
	if err != nil || persistCalled {
		t.Fatalf("RestoreCheckpoint current: %v, persistCalled=%t", err, persistCalled)
	}
	snapRecov1, err := recovCurrent.Snapshot()
	if err != nil {
		t.Fatalf("recovCurrent.Snapshot: %v", err)
	}
	if !reflect.DeepEqual(snapCurrent, snapRecov1) {
		t.Fatalf("recovCurrent snapshot mismatch")
	}
	if _, err := recovCurrent.ReadPart(0, 2, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadPart 2 got %v, want ErrNotFound", err)
	}
	for _, m := range []int{1, 3, 4} {
		if b, err := recovCurrent.ReadPart(0, m, 0); err != nil || !bytes.Equal(b, partDataList[m-1]) {
			t.Fatalf("ReadPart %d corrupted: %v", m, err)
		}
	}
	if _, err := recovCurrent.Reserve(ipc.KindPart, 0, 80); !errors.Is(err, ErrReservation) {
		t.Fatalf("Reserve while pending got %v, want ErrReservation", err)
	}
	if err := recovCurrent.Close(); err != nil {
		t.Fatalf("recovCurrent.Close: %v", err)
	}

	suffix := f.records[prefixRecordsLen:]
	persistCalled = false
	recovSuffix, err := RestoreCheckpoint(f.store, cpPrefix, suffix, recordPersist)
	if err != nil || persistCalled {
		t.Fatalf("RestoreCheckpoint suffix: %v, persistCalled=%t", err, persistCalled)
	}
	snapRecov2, err := recovSuffix.Snapshot()
	if err != nil {
		t.Fatalf("recovSuffix.Snapshot: %v", err)
	}
	if !reflect.DeepEqual(snapCurrent, snapRecov2) {
		t.Fatalf("recovSuffix snapshot mismatch")
	}
	if _, err := recovSuffix.ReadPart(0, 2, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadPart 2 got %v, want ErrNotFound", err)
	}
	if _, err := recovSuffix.Reserve(ipc.KindPart, 0, 80); !errors.Is(err, ErrReservation) {
		t.Fatalf("Reserve pending on recovSuffix got %v, want ErrReservation", err)
	}

	if err := f.ring.Close(); err != nil {
		t.Fatalf("f.ring.Close: %v", err)
	}

	if err := recovSuffix.CancelAfterQuiescence(*cpCurrent.Tracks[0].Pending); err != nil {
		t.Fatalf("CancelAfterQuiescence: %v", err)
	}
	res5Recovered, err := recovSuffix.Reserve(ipc.KindPart, 0, 80)
	if err != nil || res5Recovered.Offset != 144 {
		t.Fatalf("Reserve part 5 on recovered offset %d, want 144: %v", res5Recovered.Offset, err)
	}
	if err := recovSuffix.Commit(res5Recovered, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: cpCurrent.EntryHigh + 1,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 10e9, PublishedMonoNS: 10e9,
		Offset: 144, Length: 80, SHA256: testSHA(p5Data),
		MSN: 5, Index: 0, PTS: 5e9, DTS: 5e9, Duration: 5e8, Sequence: 5,
		FragmentStart: true, Independent: true, Payload: 1, FirstAU: 4,
	}); err != nil {
		t.Fatalf("Commit part 5 on recovered: %v", err)
	}

	cpAfter, err := recovSuffix.ExportCheckpoint()
	if err != nil {
		t.Fatalf("ExportCheckpoint after: %v", err)
	}
	if cpAfter.RecordHigh != cpCurrent.RecordHigh+3 || cpAfter.EntryHigh != cpCurrent.EntryHigh+1 || cpAfter.Tracks[0].Cursor != 224 || cpAfter.Tracks[0].Pending != nil {
		t.Fatalf("checkpoint invariants violated after mutations: %+v", cpAfter)
	}
	if err := recovSuffix.Close(); err != nil {
		t.Fatalf("recovSuffix.Close: %v", err)
	}
}

func TestCheckpointDeepCopies(t *testing.T) {
	setupFixture := func(t *testing.T) (*testFixture, Reservation) {
		f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})
		commitInitEntry(t, f, 0, 1, ipc.CodecH264, bytes.Repeat([]byte{0x11}, 64))
		commitInitEntry(t, f, 3, 2, ipc.CodecH264, bytes.Repeat([]byte{0x33}, 64))
		if err := f.ring.Apply(ipc.Entry{
			Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 3,
			Kind: ipc.KindPhases, Track: 3, PublishedNS: 200, PublishedMonoNS: 200, PhaseVideo: 100, PhaseAudio: 100,
		}); err != nil {
			t.Fatalf("Apply phases: %v", err)
		}
		commitPartEntry(t, f, 0, 4, 1, 333333333, bytes.Repeat([]byte{0x22}, 64))
		resPending, err := f.ring.Reserve(ipc.KindPart, 0, 64)
		if err != nil {
			t.Fatalf("Reserve pending: %v", err)
		}
		return f, resPending
	}

	t.Run("pending", func(t *testing.T) {
		f, _ := setupFixture(t)
		cp, err := f.ring.ExportCheckpoint()
		if err != nil {
			t.Fatalf("ExportCheckpoint: %v", err)
		}
		expected := cloneCheckpoint(cp)
		baseline, err := f.ring.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot baseline: %v", err)
		}

		cp.Tracks[0].Init.Offset = 9999
		cp.Tracks[3].Phases.PhaseVideo = 9999
		cp.Tracks[0].Segments[0].Parts[0].Offset = 9999
		cp.Tracks[0].Pending.Offset = 9999
		cp.Tracks[0].Segments = append(cp.Tracks[0].Segments, SegmentSnapshot{MSN: 99})
		cp.Tracks = append(cp.Tracks, TrackCheckpoint{Track: 99})

		newCP, err := f.ring.ExportCheckpoint()
		if err != nil {
			t.Fatalf("new ExportCheckpoint: %v", err)
		}
		newSnap, err := f.ring.Snapshot()
		if err != nil {
			t.Fatalf("new Snapshot: %v", err)
		}
		if !reflect.DeepEqual(expected, newCP) || !reflect.DeepEqual(baseline, newSnap) {
			t.Fatal("original ring mutated by exported checkpoint mutation")
		}

		freshInput, err := f.ring.ExportCheckpoint()
		if err != nil {
			t.Fatalf("freshInput Export: %v", err)
		}
		if err := f.ring.Close(); err != nil {
			t.Fatalf("Close original: %v", err)
		}
		recov, err := RestoreCheckpoint(f.store, freshInput, nil, noopPersist)
		if err != nil {
			t.Fatalf("RestoreCheckpoint: %v", err)
		}
		t.Cleanup(func() { _ = recov.Close() })
		recovCP, err := recov.ExportCheckpoint()
		if err != nil {
			t.Fatalf("recov ExportCheckpoint: %v", err)
		}
		recovSnap, err := recov.Snapshot()
		if err != nil {
			t.Fatalf("recov Snapshot: %v", err)
		}

		freshInput.Tracks[0].Init.Offset = 8888
		freshInput.Tracks[3].Phases.PhaseVideo = 8888
		freshInput.Tracks[0].Segments[0].Parts[0].Offset = 8888
		freshInput.Tracks[0].Pending.Offset = 8888
		freshInput.Tracks[0].Segments = append(freshInput.Tracks[0].Segments, SegmentSnapshot{MSN: 88})
		freshInput.Tracks = append(freshInput.Tracks, TrackCheckpoint{Track: 88})

		newRecovCP, err := recov.ExportCheckpoint()
		if err != nil {
			t.Fatalf("newRecov ExportCheckpoint: %v", err)
		}
		newRecovSnap, err := recov.Snapshot()
		if err != nil {
			t.Fatalf("newRecov Snapshot: %v", err)
		}
		if !reflect.DeepEqual(recovCP, newRecovCP) || !reflect.DeepEqual(recovSnap, newRecovSnap) {
			t.Fatal("recovered ring mutated by caller mutation of input checkpoint")
		}
		bPart, err := recov.ReadPart(0, 1, 0)
		if err != nil || !bytes.Equal(bPart, bytes.Repeat([]byte{0x22}, 64)) {
			t.Fatalf("ReadPart byte mismatch: %v", err)
		}
	})

	t.Run("terminal", func(t *testing.T) {
		f, resPending := setupFixture(t)
		if err := f.ring.CancelAfterQuiescence(resPending); err != nil {
			t.Fatalf("CancelAfterQuiescence: %v", err)
		}
		term := ipc.Entry{
			Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 5,
			Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, PublishedNS: 300, PublishedMonoNS: 300,
			Verified: false, Reason: "failed terminal deep copy",
		}
		if err := f.ring.Apply(term); err != nil {
			t.Fatalf("Apply terminal: %v", err)
		}

		cp, err := f.ring.ExportCheckpoint()
		if err != nil {
			t.Fatalf("ExportCheckpoint: %v", err)
		}
		expected := cloneCheckpoint(cp)
		baseline, err := f.ring.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot baseline: %v", err)
		}

		cp.Terminal.Reason = "mutated_terminal"
		cp.Tracks[0].Init.Offset = 9999
		cp.Tracks[3].Phases.PhaseVideo = 9999
		cp.Tracks[0].Segments[0].Parts[0].Offset = 9999
		cp.Tracks[0].Segments = append(cp.Tracks[0].Segments, SegmentSnapshot{MSN: 99})
		cp.Tracks = append(cp.Tracks, TrackCheckpoint{Track: 99})

		newCP, err := f.ring.ExportCheckpoint()
		if err != nil {
			t.Fatalf("new ExportCheckpoint: %v", err)
		}
		newSnap, err := f.ring.Snapshot()
		if err != nil {
			t.Fatalf("new Snapshot: %v", err)
		}
		if !reflect.DeepEqual(expected, newCP) || !reflect.DeepEqual(baseline, newSnap) {
			t.Fatal("original ring mutated by exported checkpoint mutation")
		}

		freshInput, err := f.ring.ExportCheckpoint()
		if err != nil {
			t.Fatalf("freshInput Export: %v", err)
		}
		if err := f.ring.Close(); err != nil {
			t.Fatalf("Close original: %v", err)
		}
		recov, err := RestoreCheckpoint(f.store, freshInput, nil, noopPersist)
		if err != nil {
			t.Fatalf("RestoreCheckpoint: %v", err)
		}
		t.Cleanup(func() { _ = recov.Close() })
		recovCP, err := recov.ExportCheckpoint()
		if err != nil {
			t.Fatalf("recov ExportCheckpoint: %v", err)
		}
		recovSnap, err := recov.Snapshot()
		if err != nil {
			t.Fatalf("recov Snapshot: %v", err)
		}

		freshInput.Terminal.Reason = "mutated_terminal_after"
		freshInput.Tracks[0].Init.Offset = 8888
		freshInput.Tracks[3].Phases.PhaseVideo = 8888
		freshInput.Tracks[0].Segments[0].Parts[0].Offset = 8888
		freshInput.Tracks[0].Segments = append(freshInput.Tracks[0].Segments, SegmentSnapshot{MSN: 88})
		freshInput.Tracks = append(freshInput.Tracks, TrackCheckpoint{Track: 88})

		newRecovCP, err := recov.ExportCheckpoint()
		if err != nil {
			t.Fatalf("newRecov ExportCheckpoint: %v", err)
		}
		newRecovSnap, err := recov.Snapshot()
		if err != nil {
			t.Fatalf("newRecov Snapshot: %v", err)
		}
		if !reflect.DeepEqual(recovCP, newRecovCP) || !reflect.DeepEqual(recovSnap, newRecovSnap) {
			t.Fatal("recovered ring mutated by caller mutation of input checkpoint")
		}
		bPart, err := recov.ReadPart(0, 1, 0)
		if err != nil || !bytes.Equal(bPart, bytes.Repeat([]byte{0x22}, 64)) {
			t.Fatalf("ReadPart byte mismatch: %v", err)
		}
	})
}

func TestCheckpointMalformedTableDriven(t *testing.T) {
	policy := Policy{Window: 1 * time.Second, MaxSegment: 1 * time.Second}
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, policy)

	initData := bytes.Repeat([]byte{0x11}, 64)
	commitInitEntry(t, f, 0, 1, ipc.CodecH264, initData)

	p0Data := bytes.Repeat([]byte{0x22}, 64)
	resP0, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p0: %v", err)
	}
	mustPwrite(t, f.dups[0], p0Data, resP0.Offset)
	if err := f.ring.Commit(resP0, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 2,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 1e9, PublishedMonoNS: 1e9,
		Offset: resP0.Offset, Length: 64, SHA256: testSHA(p0Data),
		MSN: 1, Index: 0, PTS: 1e9, DTS: 1e9, Duration: 5e8, Sequence: 1,
		FragmentStart: true, Independent: true, Payload: 1, FirstAU: 0,
	}); err != nil {
		t.Fatalf("Commit p0: %v", err)
	}

	p1Data := bytes.Repeat([]byte{0x33}, 64)
	resP1, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p1: %v", err)
	}
	mustPwrite(t, f.dups[0], p1Data, resP1.Offset)
	if err := f.ring.Commit(resP1, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 3,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 15e8, PublishedMonoNS: 15e8,
		Offset: resP1.Offset, Length: 64, SHA256: testSHA(p1Data),
		MSN: 1, Index: 1, PTS: 2e9, DTS: 2e9, Duration: 5e8, Sequence: 1,
		FragmentStart: false, Independent: false, Payload: 1, FirstAU: 1,
	}); err != nil {
		t.Fatalf("Commit p1: %v", err)
	}

	p2Data := bytes.Repeat([]byte{0x44}, 64)
	resP2, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p2: %v", err)
	}
	mustPwrite(t, f.dups[0], p2Data, resP2.Offset)
	if err := f.ring.Commit(resP2, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 4,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 3e9, PublishedMonoNS: 3e9,
		Offset: resP2.Offset, Length: 64, SHA256: testSHA(p2Data),
		MSN: 2, Index: 0, PTS: 3e9, DTS: 3e9, Duration: 1e9, Sequence: 2,
		FragmentStart: true, Independent: true, Payload: 1, FirstAU: 2,
	}); err != nil {
		t.Fatalf("Commit p2: %v", err)
	}

	p3Data := bytes.Repeat([]byte{0x55}, 64)
	resP3, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p3: %v", err)
	}
	mustPwrite(t, f.dups[0], p3Data, resP3.Offset)
	if err := f.ring.Commit(resP3, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 5,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 4e9, PublishedMonoNS: 4e9,
		Offset: resP3.Offset, Length: 64, SHA256: testSHA(p3Data),
		MSN: 3, Index: 0, PTS: 4e9, DTS: 4e9, Duration: 1e9, Sequence: 3,
		FragmentStart: true, Independent: true, Payload: 1, FirstAU: 3,
	}); err != nil {
		t.Fatalf("Commit p3: %v", err)
	}

	baseCP, err := f.ring.ExportCheckpoint()
	if err != nil {
		t.Fatalf("ExportCheckpoint baseline: %v", err)
	}

	if err := f.ring.Close(); err != nil {
		t.Fatalf("f.ring.Close: %v", err)
	}
	rDrop, err := RestoreCheckpoint(f.store, baseCP, nil, noopPersist)
	if err != nil {
		t.Fatalf("RestoreCheckpoint rDrop: %v", err)
	}
	if err := rDrop.DropSegment(0, 1, 6e9); err != nil {
		t.Fatalf("rDrop.DropSegment: %v", err)
	}
	cpDropped, err := rDrop.ExportCheckpoint()
	if err != nil {
		t.Fatalf("rDrop.ExportCheckpoint: %v", err)
	}
	if err := rDrop.Close(); err != nil {
		t.Fatalf("rDrop.Close: %v", err)
	}

	idsBefore, err := f.store.Identities()
	if err != nil {
		t.Fatalf("Identities before: %v", err)
	}
	spansBefore := make([][]byte, f.cfg.Tracks)
	for trk := 0; trk < f.cfg.Tracks; trk++ {
		span, err := f.store.ReadSpan(trk, 0, f.cfg.Capacity)
		if err != nil {
			t.Fatalf("ReadSpan before track %d: %v", trk, err)
		}
		spansBefore[trk] = span
	}

	tests := []struct {
		name string
		mut  func(cp *Checkpoint)
		err  error
	}{
		{"invalid_version", func(cp *Checkpoint) { cp.Version = 99 }, ErrInvalidPublication},
		{"scope_run_mismatch", func(cp *Checkpoint) { cp.Run = "bad-run" }, ErrIdentityMismatch},
		{"scope_source_mismatch", func(cp *Checkpoint) { cp.Source = "bad-source" }, ErrIdentityMismatch},
		{"scope_epoch_mismatch", func(cp *Checkpoint) { cp.Epoch = 999 }, ErrIdentityMismatch},
		{"invalid_config_tracks", func(cp *Checkpoint) { cp.Config.Tracks = 3 }, ErrInvalidConfig},
		{"invalid_config_capacity", func(cp *Checkpoint) { cp.Config.Capacity = 2048 }, ErrIdentityMismatch},
		{"invalid_policy_negative_window", func(cp *Checkpoint) { cp.Policy.Window = -1 }, ErrInvalidConfig},
		{"invalid_policy_negative_max_segment", func(cp *Checkpoint) { cp.Policy.MaxSegment = -1 }, ErrInvalidConfig},
		{"zero_policy_window_rejected", func(cp *Checkpoint) { cp.Policy.Window = 0 }, ErrInvalidPublication},
		{"zero_policy_max_segment_rejected", func(cp *Checkpoint) { cp.Policy.MaxSegment = 0 }, ErrInvalidPublication},
		{"track_count_mismatch", func(cp *Checkpoint) { cp.Tracks = cp.Tracks[:1] }, ErrInvalidPublication},
		{"track_order_mismatch", func(cp *Checkpoint) { cp.Tracks[0].Track = 1 }, ErrInvalidPublication},
		{"kind_track_mismatch", func(cp *Checkpoint) { cp.Tracks[0].Init.Track = 1 }, ErrInvalidPublication},
		{"init_offset_nonzero", func(cp *Checkpoint) { cp.Tracks[0].Init.Offset = 64 }, ErrInvalidPublication},
		{"video_phases_forbidden", func(cp *Checkpoint) {
			cp.Tracks[0].Phases = &ipc.Entry{Version: ipc.Version, Run: cp.Run, Source: cp.Source, Epoch: cp.Epoch, Seq: 99, Kind: ipc.KindPhases, Track: 0}
			cp.EntryHigh, cp.RecordHigh = 99, 99
		}, ErrInvalidPublication},
		{"entry_high_mismatch", func(cp *Checkpoint) { cp.EntryHigh++ }, ErrInvalidPublication},
		{"record_high_below_entry_high", func(cp *Checkpoint) { cp.RecordHigh = cp.EntryHigh - 1 }, ErrInvalidPublication},
		{"duplicate_global_entry_seq", func(cp *Checkpoint) {
			cp.Tracks[1].Init = copyEntry(cp.Tracks[0].Init)
			cp.Tracks[1].Init.Track = 1
		}, ErrInvalidPublication},
		{"negative_clock", func(cp *Checkpoint) { cp.ClockNS = -1 }, ErrInvalidPublication},
		{"publication_above_clock", func(cp *Checkpoint) { cp.Tracks[0].Init.PublishedMonoNS = cp.ClockNS + 1000 }, ErrInvalidPublication},
		{"cursor_negative", func(cp *Checkpoint) { cp.Tracks[0].Cursor = -1 }, ErrInvalidPublication},
		{"cursor_outside_capacity", func(cp *Checkpoint) { cp.Tracks[0].Cursor = cp.Config.Capacity }, ErrInvalidPublication},
		{"pending_id_zero", func(cp *Checkpoint) {
			cp.Tracks[0].Pending = &Reservation{ID: 0, Kind: ipc.KindPart, Track: 0, Offset: 320, Length: 64}
			cp.Tracks[0].Cursor = 384
		}, ErrInvalidPublication},
		{"pending_id_above_record_high", func(cp *Checkpoint) {
			cp.Tracks[0].Pending = &Reservation{ID: cp.RecordHigh + 1, Kind: ipc.KindPart, Track: 0, Offset: 320, Length: 64}
			cp.Tracks[0].Cursor = 384
		}, ErrInvalidPublication},
		{"duplicate_pending_grant_id", func(cp *Checkpoint) {
			cp.Tracks[0].Pending = &Reservation{ID: cp.RecordHigh, Kind: ipc.KindPart, Track: 0, Offset: 320, Length: 64}
			cp.Tracks[0].Cursor = 384
			cp.Tracks[1].Init = copyEntry(cp.Tracks[0].Init)
			cp.Tracks[1].Init.Track, cp.Tracks[1].Init.Seq = 1, 10
			cp.Tracks[1].Pending = &Reservation{ID: cp.RecordHigh, Kind: ipc.KindPart, Track: 1, Offset: 64, Length: 64}
			cp.Tracks[1].Cursor = 128
			cp.EntryHigh, cp.RecordHigh = 10, 10
		}, ErrInvalidPublication},
		{"pending_cursor_mismatch", func(cp *Checkpoint) {
			cp.Tracks[0].Pending = &Reservation{ID: cp.RecordHigh, Kind: ipc.KindPart, Track: 0, Offset: 320, Length: 64}
			cp.Tracks[0].Cursor = 400
		}, ErrInvalidPublication},
		{"pending_overlap", func(cp *Checkpoint) {
			cp.Tracks[0].Pending = &Reservation{ID: cp.RecordHigh, Kind: ipc.KindPart, Track: 0, Offset: 0, Length: 64}
			cp.Tracks[0].Cursor = 64
		}, ErrInvalidPublication},
		{"part_index_mismatch", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].Parts[0].Index = 1 }, ErrInvalidPublication},
		{"part_fragment_start_mismatch", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].Parts[0].FragmentStart = false }, ErrInvalidPublication},
		{"part_sequence_mismatch", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].Parts[0].Sequence = 99 }, ErrInvalidPublication},
		{"part_first_au_mismatch", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].Parts[0].FirstAU = 1 }, ErrInvalidPublication},
		{"part_pts_backwards", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].Parts[1].PTS = 50 }, ErrInvalidPublication},
		{"part_dts_backwards", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].Parts[1].DTS = 50 }, ErrInvalidPublication},
		{"duration_sum_mismatch", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].Duration += 100 }, ErrInvalidPublication},
		{"segment_end_precedes_duration", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].End = cp.Tracks[0].Segments[0].Duration - 1 }, ErrInvalidPublication},
		{"nonfinal_open_segment", func(cp *Checkpoint) { cp.Tracks[0].Segments[0].Closed = false }, ErrInvalidPublication},
		{"interior_visible_gap", func(cp *Checkpoint) {
			cp.Tracks[0].Segments = []SegmentSnapshot{baseCP.Tracks[0].Segments[0], baseCP.Tracks[0].Segments[2]}
		}, ErrInvalidPublication},
		{"dropped_after_visible", func(cp *Checkpoint) {
			grace := int64(policy.Window+2*policy.MaxSegment) + cp.Tracks[0].Segments[1].Duration
			cp.Tracks[0].Segments[1].Dropped = true
			cp.Tracks[0].Segments[1].DroppedAtNS = cp.ClockNS
			cp.Tracks[0].Segments[1].DeadlineNS = cp.ClockNS + grace
		}, ErrInvalidPublication},
		{"dropped_clock_invalid", func(cp *Checkpoint) {
			*cp = cloneCheckpoint(cpDropped)
			cp.Tracks[0].Segments[0].DroppedAtNS = cp.ClockNS + 1000
			grace := int64(policy.Window+2*policy.MaxSegment) + cp.Tracks[0].Segments[0].Duration
			cp.Tracks[0].Segments[0].DeadlineNS = cp.Tracks[0].Segments[0].DroppedAtNS + grace
		}, ErrInvalidPublication},
		{"dropped_deadline_invalid", func(cp *Checkpoint) {
			*cp = cloneCheckpoint(cpDropped)
			cp.Tracks[0].Segments[0].DeadlineNS = 12345
		}, ErrInvalidPublication},
		{"verified_terminal_lacking_tracks", func(cp *Checkpoint) {
			closeAllSegments(cp)
			cp.Terminal = &ipc.Entry{
				Version: ipc.Version, Run: cp.Run, Source: cp.Source, Epoch: cp.Epoch, Seq: cp.EntryHigh + 1,
				Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, PublishedNS: cp.ClockNS, PublishedMonoNS: cp.ClockNS, Verified: true, Reason: "clean",
			}
			cp.EntryHigh++
			cp.RecordHigh++
		}, ErrInvalidPublication},
		{"pending_after_terminal", func(cp *Checkpoint) {
			closeAllSegments(cp)
			cp.Terminal = &ipc.Entry{
				Version: ipc.Version, Run: cp.Run, Source: cp.Source, Epoch: cp.Epoch, Seq: cp.EntryHigh + 1,
				Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, PublishedNS: cp.ClockNS, PublishedMonoNS: cp.ClockNS, Verified: false, Reason: "abort",
			}
			cp.EntryHigh++
			cp.RecordHigh++
			cp.Tracks[0].Pending = &Reservation{ID: cp.RecordHigh, Kind: ipc.KindPart, Track: 0, Offset: 320, Length: 64}
			cp.Tracks[0].Cursor = 384
		}, ErrInvalidPublication},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cpClone := cloneCheckpoint(baseCP)
			tc.mut(&cpClone)
			var persistCalled bool
			r, err := RestoreCheckpoint(f.store, cpClone, nil, func(Record) error {
				persistCalled = true
				return nil
			})
			if r != nil || !errors.Is(err, tc.err) || persistCalled {
				if r != nil {
					_ = r.Close()
				}
				t.Fatalf("RestoreCheckpoint got r=%v err=%v, want %v", r, err, tc.err)
			}
			idsAfter, err := f.store.Identities()
			if err != nil || !reflect.DeepEqual(idsBefore, idsAfter) {
				t.Fatalf("%s: store identities changed: %v", tc.name, err)
			}
			for trk := 0; trk < f.cfg.Tracks; trk++ {
				spanAfter, err := f.store.ReadSpan(trk, 0, f.cfg.Capacity)
				if err != nil || !bytes.Equal(spansBefore[trk], spanAfter) {
					t.Fatalf("%s: store bytes changed on track %d", tc.name, trk)
				}
			}
		})
	}

	t.Run("positive_changed_policy_control", func(t *testing.T) {
		cpChanged := cloneCheckpoint(baseCP)
		cpChanged.Policy = Policy{Window: 5 * time.Second, MaxSegment: 2 * time.Second}
		r, err := RestoreCheckpoint(f.store, cpChanged, nil, noopPersist)
		if err != nil {
			t.Fatalf("RestoreCheckpoint with changed policy: %v", err)
		}
		defer r.Close()
		cpRestored, err := r.ExportCheckpoint()
		if err != nil {
			t.Fatalf("ExportCheckpoint: %v", err)
		}
		if cpRestored.Policy != cpChanged.Policy {
			t.Fatalf("restored policy %+v, want %+v", cpRestored.Policy, cpChanged.Policy)
		}
	})
}

func TestCheckpointSuffixAndCorruption(t *testing.T) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})
	initData := bytes.Repeat([]byte{0x44}, 64)
	commitInitEntry(t, f, 0, 1, ipc.CodecH264, initData)

	cp, err := f.ring.ExportCheckpoint()
	if err != nil {
		t.Fatalf("ExportCheckpoint: %v", err)
	}
	cpRecordsLen := len(f.records)

	resPart, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve part: %v", err)
	}
	partData := bytes.Repeat([]byte{0x55}, 64)
	mustPwrite(t, f.dups[0], partData, resPart.Offset)
	pEntry := ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 2,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 2e9, PublishedMonoNS: 2e9,
		Offset: resPart.Offset, Length: 64, SHA256: testSHA(partData),
		MSN: 1, Index: 0, PTS: 1e9, DTS: 1e9, Duration: 333333333, Sequence: 1,
		FragmentStart: true, Independent: true, Payload: 1, FirstAU: 0,
	}
	if err := f.ring.Commit(resPart, pEntry); err != nil {
		t.Fatalf("Commit part: %v", err)
	}

	validSuffix := append([]Record(nil), f.records[cpRecordsLen:]...)

	t.Run("legitimate_suffix_accepted", func(t *testing.T) {
		recov, err := RestoreCheckpoint(f.store, cp, validSuffix, noopPersist)
		if err != nil {
			t.Fatalf("RestoreCheckpoint suffix: %v", err)
		}
		if err := recov.Close(); err != nil {
			t.Fatalf("recov.Close: %v", err)
		}
	})

	for _, tc := range []struct {
		name string
		mut  func(s []Record)
	}{
		{"duplicate_seq", func(s []Record) { s[0].Seq = cp.RecordHigh }},
		{"seq_gap", func(s []Record) { s[0].Seq = cp.RecordHigh + 2 }},
		{"wrong_scope", func(s []Record) { s[0].Run = "bad-run" }},
		{"wrong_config", func(s []Record) { s[0].Config.Capacity = 4096 }},
		{"wrong_policy", func(s []Record) { s[0].Policy.Window = 100 * time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badSuffix := append([]Record(nil), validSuffix...)
			tc.mut(badSuffix)
			if _, err := RestoreCheckpoint(f.store, cp, badSuffix, noopPersist); !errors.Is(err, ErrInvalidPublication) {
				t.Fatalf("%s got %v, want ErrInvalidPublication", tc.name, err)
			}
		})
	}

	t.Run("retained_byte_corruption", func(t *testing.T) {
		mustPwrite(t, f.dups[0], []byte{0xff}, 0)
		t.Cleanup(func() { mustPwrite(t, f.dups[0], initData[:1], 0) })
		if _, err := RestoreCheckpoint(f.store, cp, nil, noopPersist); !errors.Is(err, ErrInvalidPublication) {
			t.Fatalf("corrupted bytes got %v, want ErrInvalidPublication", err)
		}
	})

	t.Run("export_closed", func(t *testing.T) {
		fClosed := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 512, Policy{})
		if err := fClosed.ring.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := fClosed.ring.ExportCheckpoint(); !errors.Is(err, ErrClosed) {
			t.Fatalf("ExportCheckpoint closed got %v, want ErrClosed", err)
		}
	})

	t.Run("export_poisoned", func(t *testing.T) {
		cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 512}
		store, err := AllocateEpoch("run-p", "src-p", 301, cfg)
		if err != nil {
			t.Fatalf("AllocateEpoch: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		rPoison, err := NewRing(store, "run-p", "src-p", 301, cfg, Policy{}, func(Record) error {
			return errors.New("simulated disk persist failure")
		})
		if err != nil {
			t.Fatalf("NewRing: %v", err)
		}
		t.Cleanup(func() { _ = rPoison.Close() })
		if _, err := rPoison.Reserve(ipc.KindInit, 0, 64); !errors.Is(err, ErrPoisoned) {
			t.Fatalf("Reserve got %v, want ErrPoisoned", err)
		}
		if _, err := rPoison.ExportCheckpoint(); !errors.Is(err, ErrPoisoned) {
			t.Fatalf("ExportCheckpoint got %v, want ErrPoisoned", err)
		}
	})
}

func TestCheckpointBoundedConcurrentExportClose(t *testing.T) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})
	commitInitEntry(t, f, 0, 1, ipc.CodecH264, bytes.Repeat([]byte{0x11}, 64))

	const numWorkers = 4
	errCh := make(chan error, 1000)
	stopCh := make(chan struct{})
	var preCloseCount int64
	var postCloseErrClosed int64
	var ringClosed int32

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				cp, err := f.ring.ExportCheckpoint()
				if err == nil {
					atomic.AddInt64(&preCloseCount, 1)
					if cp.Version != CheckpointVersion || len(cp.Tracks) != f.cfg.Tracks {
						select {
						case errCh <- fmt.Errorf("inconsistent checkpoint: %+v", cp):
						default:
						}
						return
					}
				} else if errors.Is(err, ErrClosed) {
					if atomic.LoadInt32(&ringClosed) == 0 {
						select {
						case errCh <- fmt.Errorf("ErrClosed before ring.Close()"):
						default:
						}
					} else {
						atomic.AddInt64(&postCloseErrClosed, 1)
					}
					return
				} else {
					select {
					case errCh <- fmt.Errorf("unexpected error: %w", err):
					default:
					}
					return
				}
			}
		}()
	}

	t.Cleanup(func() {
		select {
		case <-stopCh:
		default:
			close(stopCh)
		}
		wg.Wait()
	})

	start := time.Now()
	for atomic.LoadInt64(&preCloseCount) < 20 {
		if time.Since(start) > 2*time.Second {
			t.Fatalf("timed out waiting for 20 pre-close exports, got %d", atomic.LoadInt64(&preCloseCount))
		}
		time.Sleep(1 * time.Millisecond)
	}

	atomic.StoreInt32(&ringClosed, 1)
	if err := f.ring.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		close(stopCh)
		wg.Wait()
		t.Fatal("workers failed to exit after Close within deadline")
	}

	close(errCh)
	for err := range errCh {
		t.Errorf("worker error: %v", err)
	}

	if pre := atomic.LoadInt64(&preCloseCount); pre < 20 {
		t.Fatalf("expected >= 20 pre-close exports, got %d", pre)
	}
	if post := atomic.LoadInt64(&postCloseErrClosed); post == 0 {
		t.Fatal("expected at least one post-close ErrClosed observation")
	}
}
