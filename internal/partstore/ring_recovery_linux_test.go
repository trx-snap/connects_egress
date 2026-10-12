//go:build linux

package partstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

// 5. Persistence errors at each operation: capture count/snapshot before, require ErrPoisoned;
// no exposure/grant return/state advance, no retry or later mutation, old reads remain.
// Durable record Config/effectivePolicy match; replay no Persist, pending quarantined;
// reject foreign/gaps/conflicts/unused fields/overlap/incorrect transitions/config/policy and retained digest mismatch.
func TestPersistenceErrorPoisoning(t *testing.T) {
	operations := []string{OpReserve, OpCommit, OpMeta, OpCancel, OpDrop, OpRetire}

	for _, targetOp := range operations {
		t.Run("poison_on_"+targetOp, func(t *testing.T) {
			run := "run-poison"
			source := "src-poison"
			epoch := uint64(501)
			cfg := ipc.Config{
				Mode:     ipc.ModeSolo,
				Codec:    ipc.CodecH264,
				Tracks:   ipc.TracksSolo,
				Capacity: 2048,
			}
			pol := Policy{Window: 1 * time.Second, MaxSegment: 500 * time.Millisecond}

			store, err := AllocateEpoch(run, source, epoch, cfg)
			if err != nil {
				t.Fatalf("AllocateEpoch: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })

			dups, err := store.DupFDs()
			if err != nil {
				t.Fatalf("DupFDs: %v", err)
			}
			t.Cleanup(func() {
				for _, fd := range dups {
					_ = unix.Close(fd)
				}
			})

			var failOp string
			var persistCalls int
			persist := func(rec Record) error {
				persistCalls++
				if rec.Op == failOp {
					return errors.New("simulated durable I/O failure")
				}
				return nil
			}

			ring, err := NewRing(store, run, source, epoch, cfg, pol, persist)
			if err != nil {
				t.Fatalf("NewRing: %v", err)
			}
			t.Cleanup(func() { _ = ring.Close() })

			// Setup base state: Init on track 0
			resInit, err := ring.Reserve(ipc.KindInit, 0, 64)
			if err != nil {
				t.Fatalf("setup Reserve init: %v", err)
			}
			initData := bytes.Repeat([]byte{0x10}, 64)
			mustPwrite(t, dups[0], initData, 0)
			initEntry := ipc.Entry{
				Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: 1,
				Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
				Offset: 0, Length: 64, SHA256: testSHA(initData), Codecs: "avc1.64001f",
			}
			if err := ring.Commit(resInit, initEntry); err != nil {
				t.Fatalf("setup Commit init: %v", err)
			}

			// Add a part so we have a segment for Drop/Retire
			resP0, err := ring.Reserve(ipc.KindPart, 0, 64)
			if err != nil {
				t.Fatalf("setup Reserve p0: %v", err)
			}
			p0Data := bytes.Repeat([]byte{0x20}, 64)
			mustPwrite(t, dups[0], p0Data, resP0.Offset)
			p0Entry := ipc.Entry{
				Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: 2,
				Kind: ipc.KindPart, Track: 0, PublishedNS: 1_000_000_000, PublishedMonoNS: 1_000_000_000,
				Offset: resP0.Offset, Length: 64, SHA256: testSHA(p0Data),
				MSN: 1, Index: 0, PTS: 1_000_000_000, DTS: 1_000_000_000,
				Duration: 500_000_000, Sequence: 1, FragmentStart: true, Independent: true,
				Payload: 1, FirstAU: 0,
			}
			if err := ring.Commit(resP0, p0Entry); err != nil {
				t.Fatalf("setup Commit p0: %v", err)
			}

			// Close segment 1 by starting segment 2
			resP1, err := ring.Reserve(ipc.KindPart, 0, 64)
			if err != nil {
				t.Fatalf("setup Reserve p1: %v", err)
			}
			p1Data := bytes.Repeat([]byte{0x30}, 64)
			mustPwrite(t, dups[0], p1Data, resP1.Offset)
			p1Entry := ipc.Entry{
				Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: 3,
				Kind: ipc.KindPart, Track: 0, PublishedNS: 2_000_000_000, PublishedMonoNS: 2_000_000_000,
				Offset: resP1.Offset, Length: 64, SHA256: testSHA(p1Data),
				MSN: 2, Index: 0, PTS: 2_000_000_000, DTS: 2_000_000_000,
				Duration: 500_000_000, Sequence: 2, FragmentStart: true, Independent: true,
				Payload: 1, FirstAU: 1,
			}
			if err := ring.Commit(resP1, p1Entry); err != nil {
				t.Fatalf("setup Commit p1: %v", err)
			}

			// Advance past window to make seg 1 drop eligible
			resP2, err := ring.Reserve(ipc.KindPart, 0, 64)
			if err != nil {
				t.Fatalf("setup Reserve p2: %v", err)
			}
			p2Data := bytes.Repeat([]byte{0x40}, 64)
			mustPwrite(t, dups[0], p2Data, resP2.Offset)
			p2Entry := ipc.Entry{
				Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: 4,
				Kind: ipc.KindPart, Track: 0, PublishedNS: 3_000_000_000, PublishedMonoNS: 3_000_000_000,
				Offset: resP2.Offset, Length: 64, SHA256: testSHA(p2Data),
				MSN: 3, Index: 0, PTS: 3_000_000_000, DTS: 3_000_000_000,
				Duration: 500_000_000, Sequence: 3, FragmentStart: true, Independent: true,
				Payload: 1, FirstAU: 2,
			}
			if err := ring.Commit(resP2, p2Entry); err != nil {
				t.Fatalf("setup Commit p2: %v", err)
			}

			if targetOp == OpRetire {
				if err := ring.DropSegment(0, 1, 4_000_000_000); err != nil {
					t.Fatalf("setup DropSegment: %v", err)
				}
			}

			// Capture snapshot immediately before the failed operation
			snapBefore, err := ring.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot before: %v", err)
			}

			// ARM the failure
			failOp = targetOp

			var opErr error
			switch targetOp {
			case OpReserve:
				res, err := ring.Reserve(ipc.KindPart, 0, 64)
				opErr = err
				if res != (Reservation{}) {
					t.Fatalf("Reserve returned non-zero reservation on poison: %+v", res)
				}
			case OpCommit:
				res, err := ring.Reserve(ipc.KindPart, 0, 64)
				if err != nil {
					t.Fatalf("Reserve for commit test: %v", err)
				}
				// Re-capture snapBefore now that grant is pending
				snapBefore, err = ring.Snapshot()
				if err != nil {
					t.Fatalf("Snapshot before commit: %v", err)
				}
				pData := bytes.Repeat([]byte{0x99}, 64)
				mustPwrite(t, dups[0], pData, res.Offset)
				opErr = ring.Commit(res, ipc.Entry{
					Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: snapBefore.EntryHigh + 1,
					Kind: ipc.KindPart, Track: 0, PublishedNS: 4_000_000_000, PublishedMonoNS: 4_000_000_000,
					Offset: res.Offset, Length: 64, SHA256: testSHA(pData),
					MSN: 4, Index: 0, PTS: 4_000_000_000, DTS: 4_000_000_000,
					Duration: 500_000_000, Sequence: 4, FragmentStart: true, Independent: true,
					Payload: 1, FirstAU: 3,
				})
			case OpMeta:
				opErr = ring.Apply(ipc.Entry{
					Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: snapBefore.EntryHigh + 1,
					Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, PublishedNS: 4_000_000_000, PublishedMonoNS: 4_000_000_000,
					Verified: false, Reason: "testing poison",
				})
			case OpCancel:
				res, err := ring.Reserve(ipc.KindPart, 0, 64)
				if err != nil {
					t.Fatalf("Reserve for cancel test: %v", err)
				}
				snapBefore, err = ring.Snapshot()
				if err != nil {
					t.Fatalf("Snapshot before cancel: %v", err)
				}
				opErr = ring.CancelAfterQuiescence(res)
			case OpDrop:
				opErr = ring.DropSegment(0, 1, 4_000_000_000)
			case OpRetire:
				snap, err := ring.Snapshot()
				if err != nil {
					t.Fatalf("Snapshot before retire: %v", err)
				}
				deadline := snap.Tracks[0].Segments[0].DeadlineNS
				opErr = ring.RetireSegment(0, 1, deadline)
			}

			// Verify ErrPoisoned is returned
			if !errors.Is(opErr, ErrPoisoned) {
				t.Fatalf("expected ErrPoisoned on %s failure, got %v", targetOp, opErr)
			}

			// Full snapshot equality check: snapshot must be completely identical before and after
			snapAfter, err := ring.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot after: %v", err)
			}
			if !reflect.DeepEqual(snapBefore, snapAfter) {
				t.Fatalf("Snapshot modified on poisoned %s: before %+v, after %+v", targetOp, snapBefore, snapAfter)
			}

			// In OpCommit, verify the grant remains in snapAfter.Pending
			if targetOp == OpCommit {
				if len(snapAfter.Pending) != 1 {
					t.Fatal("pending grant was dropped on failed Commit persist")
				}
			}

			// Subsequent mutation must return ErrPoisoned immediately without retry
			callCountBefore := persistCalls
			if _, err := ring.Reserve(ipc.KindPart, 0, 32); !errors.Is(err, ErrPoisoned) {
				t.Fatalf("subsequent mutation got %v, want ErrPoisoned", err)
			}
			if persistCalls != callCountBefore {
				t.Fatalf("persist was retried on poisoned ring")
			}

			// Old reads remain valid: previously committed init and parts must still be readable
			bInit, err := ring.ReadInit(0)
			if err != nil || !bytes.Equal(bInit, initData) {
				t.Fatalf("ReadInit after poison failed: %v", err)
			}
			bP0, err := ring.ReadPart(0, 1, 0)
			if err != nil || !bytes.Equal(bP0, p0Data) {
				t.Fatalf("ReadPart after poison failed: %v", err)
			}
		})
	}
}

func TestRealScratchAppendSyncSmoke(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "ring_persist_smoke.log")

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()

	recCount := 0
	persist := func(rec Record) error {
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		data = append(data, '\n')
		if _, err := file.Write(data); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
		recCount++
		return nil
	}

	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 1024}
	store, err := AllocateEpoch("run-smoke", "src-smoke", 1, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	defer store.Close()

	ring, err := NewRing(store, "run-smoke", "src-smoke", 1, cfg, Policy{}, persist)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	defer ring.Close()

	res, err := ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if recCount != 1 {
		t.Fatalf("expected 1 record persisted, got %d", recCount)
	}

	// Verify file has data
	st, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Size() == 0 {
		t.Fatal("persisted log file is empty after Sync")
	}
	if res.ID != 1 || res.Offset != 0 || res.Length != 64 {
		t.Fatalf("unexpected res: %+v", res)
	}
}

func TestReplayRecordValidation(t *testing.T) {
	run := "run-replay"
	source := "src-replay"
	epoch := uint64(99)
	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 1024}
	pol := Policy{Window: 24 * time.Second, MaxSegment: 3 * time.Second}

	baseRecord := Record{
		Version: RecordVersion, Run: run, Source: source, Epoch: epoch, Seq: 1,
		Config: cfg, Policy: pol, Op: OpReserve,
		Reservation: Reservation{ID: 1, Kind: ipc.KindInit, Track: 0, Offset: 0, Length: 64},
		Track:       0,
	}

	tests := []struct {
		name string
		rec  Record
	}{
		{
			name: "wrong_version",
			rec: func() Record {
				r := baseRecord
				r.Version = 99
				return r
			}(),
		},
		{
			name: "foreign_run",
			rec: func() Record {
				r := baseRecord
				r.Run = "other-run"
				return r
			}(),
		},
		{
			name: "foreign_source",
			rec: func() Record {
				r := baseRecord
				r.Source = "other-source"
				return r
			}(),
		},
		{
			name: "foreign_epoch",
			rec: func() Record {
				r := baseRecord
				r.Epoch = 888
				return r
			}(),
		},
		{
			name: "seq_gap",
			rec: func() Record {
				r := baseRecord
				r.Seq = 2 // gap: expected 1
				return r
			}(),
		},
		{
			name: "config_mismatch",
			rec: func() Record {
				r := baseRecord
				r.Config.Capacity = 2048
				return r
			}(),
		},
		{
			name: "policy_mismatch",
			rec: func() Record {
				r := baseRecord
				r.Policy.Window = 10 * time.Second
				return r
			}(),
		},
		{
			name: "unused_fields_set",
			rec: func() Record {
				r := baseRecord
				r.MSN = 10 // Reserve op must not have MSN
				return r
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := AllocateEpoch(run, source, epoch, cfg)
			if err != nil {
				t.Fatalf("AllocateEpoch: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })

			_, err = RestoreRing(store, run, source, epoch, cfg, pol, []Record{tc.rec}, func(Record) error {
				t.Fatal("Persist should never be called during RestoreRing")
				return nil
			})
			if !errors.Is(err, ErrInvalidPublication) {
				t.Fatalf("expected ErrInvalidPublication on %s, got %v", tc.name, err)
			}
		})
	}

	// Conflicting replay and overlapping replay
	t.Run("conflicting_and_overlapping_replay", func(t *testing.T) {
		store, err := AllocateEpoch(run, source, epoch, cfg)
		if err != nil {
			t.Fatalf("AllocateEpoch: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		// Record 1: reserve init at 0..64
		rec1 := baseRecord
		// Record 2: conflicting reserve on track 0 while grant 1 is still pending
		rec2Conflict := Record{
			Version: RecordVersion, Run: run, Source: source, Epoch: epoch, Seq: 2,
			Config: cfg, Policy: pol, Op: OpReserve,
			Reservation: Reservation{ID: 2, Kind: ipc.KindPart, Track: 0, Offset: 64, Length: 64},
			Track:       0,
		}
		_, err = RestoreRing(store, run, source, epoch, cfg, pol, []Record{rec1, rec2Conflict}, func(Record) error {
			return nil
		})
		if !errors.Is(err, ErrReservation) && !errors.Is(err, ErrInvalidPublication) {
			t.Fatalf("conflicting replay got %v, want ErrReservation or ErrInvalidPublication", err)
		}

		// Overlapping replay: record 1 committed at 0..64, record 3 claims offset 0 overlapping committed init
		dups, err := store.DupFDs()
		if err != nil {
			t.Fatalf("DupFDs: %v", err)
		}
		for _, fd := range dups {
			defer unix.Close(fd)
		}
		initData := bytes.Repeat([]byte{0x11}, 64)
		if err := pwriteErr(dups[0], initData, 0); err != nil {
			t.Fatalf("pwrite init: %v", err)
		}
		rec2Commit := Record{
			Version: RecordVersion, Run: run, Source: source, Epoch: epoch, Seq: 2,
			Config: cfg, Policy: pol, Op: OpCommit,
			Reservation: rec1.Reservation,
			Entry: ipc.Entry{
				Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: 1,
				Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
				Offset: 0, Length: 64, SHA256: testSHA(initData), Codecs: "avc1.64001f",
			},
			Track: 0,
		}
		rec3Overlap := Record{
			Version: RecordVersion, Run: run, Source: source, Epoch: epoch, Seq: 3,
			Config: cfg, Policy: pol, Op: OpReserve,
			Reservation: Reservation{ID: 3, Kind: ipc.KindPart, Track: 0, Offset: 0, Length: 64}, // overlaps init at 0
			Track:       0,
		}
		_, err = RestoreRing(store, run, source, epoch, cfg, pol, []Record{rec1, rec2Commit, rec3Overlap}, func(Record) error {
			return nil
		})
		if !errors.Is(err, ErrInvalidPublication) {
			t.Fatalf("overlapping replay got %v, want ErrInvalidPublication", err)
		}
	})

	// Known defect: ring_records_linux.go:58 record seq+1 may wrap MaxUint64 into 0
	t.Run("seq_wrap_max_uint64_defect_reproduce", func(t *testing.T) {
		store, err := AllocateEpoch(run, source, epoch, cfg)
		if err != nil {
			t.Fatalf("AllocateEpoch: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		ring, err := NewRing(store, run, source, epoch, cfg, pol, func(Record) error { return nil })
		if err != nil {
			t.Fatalf("NewRing: %v", err)
		}
		t.Cleanup(func() { _ = ring.Close() })

		// Synthetic boundary control: set private r.recSeq to MaxUint64
		ring.recSeq = math.MaxUint64

		// A record with Seq = 0 would be accepted if r.recSeq + 1 wraps without checked arithmetic!
		recSeq0 := Record{
			Version: RecordVersion, Run: run, Source: source, Epoch: epoch, Seq: 0,
			Config: cfg, Policy: pol, Op: OpReserve,
			Reservation: Reservation{ID: 0, Kind: ipc.KindInit, Track: 0, Offset: 0, Length: 64},
			Track:       0,
		}
		err = ring.checkScope(recSeq0)
		if err == nil {
			t.Fatalf("DEFECT ring_records_linux.go:58: recSeq wrapped MaxUint64 into 0 and checkScope accepted it")
		}
	})
}

func TestRetainedCorruptionRestoreFailure(t *testing.T) {
	run := "run-corrupt"
	source := "src-corrupt"
	epoch := uint64(701)
	cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: ipc.TracksSolo, Capacity: 1024}
	pol := Policy{Window: 24 * time.Second, MaxSegment: 3 * time.Second}

	// 1. Create a ring, commit init and part
	store, err := AllocateEpoch(run, source, epoch, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	dups, err := store.DupFDs()
	if err != nil {
		t.Fatalf("DupFDs: %v", err)
	}
	t.Cleanup(func() {
		for _, fd := range dups {
			_ = unix.Close(fd)
		}
	})

	var records []Record
	ring, err := NewRing(store, run, source, epoch, cfg, pol, func(r Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	t.Cleanup(func() { _ = ring.Close() })

	resInit, err := ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve init: %v", err)
	}
	initData := bytes.Repeat([]byte{0xaa}, 64)
	mustPwrite(t, dups[0], initData, 0)
	if err := ring.Commit(resInit, ipc.Entry{
		Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: 1,
		Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
		Offset: 0, Length: 64, SHA256: testSHA(initData), Codecs: "avc1.64001f",
	}); err != nil {
		t.Fatalf("Commit init: %v", err)
	}

	resP0, err := ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p0: %v", err)
	}
	p0Data := bytes.Repeat([]byte{0xbb}, 64)
	mustPwrite(t, dups[0], p0Data, resP0.Offset)
	if err := ring.Commit(resP0, ipc.Entry{
		Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: 2,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 200, PublishedMonoNS: 200,
		Offset: resP0.Offset, Length: 64, SHA256: testSHA(p0Data),
		MSN: 1, Index: 0, PTS: 100, DTS: 100, Duration: 100_000_000, Sequence: 1,
		FragmentStart: true, Independent: true, Payload: 1, FirstAU: 0,
	}); err != nil {
		t.Fatalf("Commit p0: %v", err)
	}

	// 2. Corrupt one byte of retained part 0 in the memfd
	corruptByte := []byte{0xff}
	mustPwrite(t, dups[0], corruptByte, resP0.Offset+10)

	// 3. Replaying these records against the corrupted store MUST fail during digest check
	_, err = RestoreRing(store, run, source, epoch, cfg, pol, records, func(Record) error {
		return nil
	})
	if !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("RestoreRing on corrupted retained part got %v, want ErrInvalidPublication", err)
	}
}

// 6. Deterministic seeded operation sequence, every durable prefix reconstructed against the memfd image AT THAT PREFIX,
// not current overwritten bytes. During Persist record capture copy entire small memfds via EpochStore (never call Ring from callback).
// Assert recovered pending + retained extents no overlap, copies correct, eligible retirement only, record/entry highwater continuity.
// Include retire+reuse so old retired hashes NOT checked. Bounded <=100 prefixes, no endless fuzz.
func TestDeterministicPrefixRecovery(t *testing.T) {
	run := "run-deterministic"
	source := "src-deterministic"
	epoch := uint64(801)
	// Reduced capacity to 384: Init (64) + 5 parts (5*64 = 320) = 384. Track is 100% full!
	// Retiring MSN 1 frees 64..128. New part wraps to 0, finds hole, grants offset 64!
	capSize := int64(384)
	cfg := ipc.Config{
		Mode:     ipc.ModeSolo,
		Codec:    ipc.CodecH264,
		Tracks:   ipc.TracksSolo,
		Capacity: capSize,
	}
	pol := Policy{Window: 1 * time.Second, MaxSegment: 500 * time.Millisecond}

	store, err := AllocateEpoch(run, source, epoch, cfg)
	if err != nil {
		t.Fatalf("AllocateEpoch: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	dups, err := store.DupFDs()
	if err != nil {
		t.Fatalf("DupFDs: %v", err)
	}
	t.Cleanup(func() {
		for _, fd := range dups {
			_ = unix.Close(fd)
		}
	})

	type prefixSnapshot struct {
		record       Record
		memfds       [][]byte
		expectedSnap RingSnapshot
	}

	var captured []prefixSnapshot

	// Persist callback copies entire small memfds via EpochStore (never call Ring from callback)
	persist := func(rec Record) error {
		snap := make([][]byte, cfg.Tracks)
		for trk := 0; trk < cfg.Tracks; trk++ {
			b, err := store.ReadSpan(trk, 0, capSize)
			if err != nil {
				return fmt.Errorf("ReadSpan track %d: %w", trk, err)
			}
			snap[trk] = b
		}
		captured = append(captured, prefixSnapshot{record: rec, memfds: snap})
		return nil
	}

	ring, err := NewRing(store, run, source, epoch, cfg, pol, persist)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	t.Cleanup(func() { _ = ring.Close() })

	mustSnap := func() RingSnapshot {
		s, err := ring.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		return s
	}

	rng := rand.New(rand.NewSource(42))

	// Step 1: Init on video track 0
	resInit, err := ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve init: %v", err)
	}
	captured[len(captured)-1].expectedSnap = mustSnap()

	initData := make([]byte, 64)
	rng.Read(initData)
	mustPwrite(t, dups[0], initData, 0)
	initEntry := ipc.Entry{
		Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: 1,
		Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
		Offset: 0, Length: 64, SHA256: testSHA(initData), Codecs: "avc1.64001f",
	}
	if err := ring.Commit(resInit, initEntry); err != nil {
		t.Fatalf("Commit init: %v", err)
	}
	captured[len(captured)-1].expectedSnap = mustSnap()

	// Step 2: 5 segments of 1 part each -> fills capacity (64 + 5*64 = 384)
	msn := 1
	seq := uint64(2)
	clock := int64(1_000_000_000)

	for i := 0; i < 5; i++ {
		res, err := ring.Reserve(ipc.KindPart, 0, 64)
		if err != nil {
			t.Fatalf("Reserve step %d: %v", i, err)
		}
		captured[len(captured)-1].expectedSnap = mustSnap()

		pData := make([]byte, 64)
		rng.Read(pData)
		mustPwrite(t, dups[0], pData, res.Offset)
		pEntry := ipc.Entry{
			Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: seq,
			Kind: ipc.KindPart, Track: 0, PublishedNS: clock, PublishedMonoNS: clock,
			Offset: res.Offset, Length: 64, SHA256: testSHA(pData),
			MSN: msn, Index: 0, PTS: clock, DTS: clock,
			Duration: 500_000_000, Sequence: uint64(msn), FragmentStart: true, Independent: true,
			Payload: 1, FirstAU: i,
		}
		if err := ring.Commit(res, pEntry); err != nil {
			t.Fatalf("Commit step %d: %v", i, err)
		}
		captured[len(captured)-1].expectedSnap = mustSnap()

		seq++
		msn++
		clock += 1_000_000_000
	}

	// Step 3: Advance clock and drop MSN 1
	dropClock := clock + 2_000_000_000
	if err := ring.DropSegment(0, 1, dropClock); err != nil {
		t.Fatalf("DropSegment: %v", err)
	}
	captured[len(captured)-1].expectedSnap = mustSnap()

	// Step 4: Retire MSN 1 at its deadline
	snap := mustSnap()
	seg1Deadline := snap.Tracks[0].Segments[0].DeadlineNS
	if err := ring.RetireSegment(0, 1, seg1Deadline); err != nil {
		t.Fatalf("RetireSegment: %v", err)
	}
	captured[len(captured)-1].expectedSnap = mustSnap()

	// Step 5: Reuse freed space: reserve a new part
	// Track was full (0..384). Retiring MSN 1 freed 64..128. Cursor is at 384.
	// Cursor wraps to 0 and finds fit at 64!
	resReuse, err := ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve reuse: %v", err)
	}
	if resReuse.Offset != 64 {
		t.Fatalf("expected wrap reuse at offset 64, got %d", resReuse.Offset)
	}
	captured[len(captured)-1].expectedSnap = mustSnap()

	reuseData := make([]byte, 64)
	rng.Read(reuseData)
	mustPwrite(t, dups[0], reuseData, resReuse.Offset)
	reuseEntry := ipc.Entry{
		Version: ipc.Version, Run: run, Source: source, Epoch: epoch, Seq: seq,
		Kind: ipc.KindPart, Track: 0, PublishedNS: seg1Deadline + 100, PublishedMonoNS: seg1Deadline + 100,
		Offset: resReuse.Offset, Length: 64, SHA256: testSHA(reuseData),
		MSN: msn, Index: 0, PTS: seg1Deadline + 100, DTS: seg1Deadline + 100,
		Duration: 500_000_000, Sequence: uint64(msn), FragmentStart: true, Independent: true,
		Payload: 1, FirstAU: 5,
	}
	if err := ring.Commit(resReuse, reuseEntry); err != nil {
		t.Fatalf("Commit reuse: %v", err)
	}
	captured[len(captured)-1].expectedSnap = mustSnap()

	totalPrefixes := len(captured)
	if totalPrefixes > 100 {
		t.Fatalf("expected <= 100 prefixes, got %d", totalPrefixes)
	}

	// Reconstruct every durable prefix against the memfd image AT THAT PREFIX
	for prefixLen := 1; prefixLen <= totalPrefixes; prefixLen++ {
		prefixLen := prefixLen
		t.Run(fmt.Sprintf("prefix_%d", prefixLen), func(t *testing.T) {
			prefixRecords := make([]Record, prefixLen)
			for k := 0; k < prefixLen; k++ {
				prefixRecords[k] = captured[k].record
			}
			memfdImage := captured[prefixLen-1].memfds
			expectedSnap := captured[prefixLen-1].expectedSnap

			// Fresh store for this prefix with immediate cleanup registration
			pStore, err := AllocateEpoch(run, source, epoch, cfg)
			if err != nil {
				t.Fatalf("prefix %d AllocateEpoch: %v", prefixLen, err)
			}
			t.Cleanup(func() { _ = pStore.Close() })

			pDups, err := pStore.DupFDs()
			if err != nil {
				t.Fatalf("prefix %d DupFDs: %v", prefixLen, err)
			}
			t.Cleanup(func() {
				for _, fd := range pDups {
					_ = unix.Close(fd)
				}
			})

			// Write memfd image AT THAT PREFIX into the fresh store
			for trk := 0; trk < cfg.Tracks; trk++ {
				mustPwrite(t, pDups[trk], memfdImage[trk], 0)
			}

			dummyPersist := func(Record) error {
				return errors.New("restore must not persist")
			}

			restoredRing, err := RestoreRing(pStore, run, source, epoch, cfg, pol, prefixRecords, dummyPersist)
			if err != nil {
				t.Fatalf("prefix %d RestoreRing failed: %v", prefixLen, err)
			}
			t.Cleanup(func() { _ = restoredRing.Close() })

			rSnap, err := restoredRing.Snapshot()
			if err != nil {
				t.Fatalf("prefix %d Snapshot failed: %v", prefixLen, err)
			}

			// Full recovered snapshot comparison for EVERY durable prefix
			if !reflect.DeepEqual(rSnap, expectedSnap) {
				t.Fatalf("prefix %d snapshot mismatch:\ngot:  %+v\nwant: %+v", prefixLen, rSnap, expectedSnap)
			}

			// Restored pending quarantine: extents do not overlap
			for trkIdx, trkSnap := range rSnap.Tracks {
				type span struct{ start, end int64 }
				var spans []span
				if trkSnap.Init != nil {
					spans = append(spans, span{trkSnap.Init.Offset, trkSnap.Init.Offset + trkSnap.Init.Length})
				}
				for _, seg := range trkSnap.Segments {
					for _, p := range seg.Parts {
						spans = append(spans, span{p.Offset, p.Offset + p.Length})
					}
				}
				for _, pend := range rSnap.Pending {
					if pend.Track == trkIdx {
						spans = append(spans, span{pend.Offset, pend.Offset + pend.Length})
					}
				}
				for a := 0; a < len(spans); a++ {
					for b := a + 1; b < len(spans); b++ {
						if spans[a].start < spans[b].end && spans[b].start < spans[a].end {
							t.Fatalf("prefix %d track %d overlap between [%d..%d] and [%d..%d]",
								prefixLen, trkIdx, spans[a].start, spans[a].end, spans[b].start, spans[b].end)
						}
					}
				}
			}

			// Retained copies verification: ReadInit and ReadPart on every retained entry
			for trkIdx, trkSnap := range rSnap.Tracks {
				if trkSnap.Init != nil {
					bInit, err := restoredRing.ReadInit(trkIdx)
					if err != nil {
						t.Fatalf("prefix %d ReadInit track %d: %v", prefixLen, trkIdx, err)
					}
					if testSHA(bInit) != trkSnap.Init.SHA256 {
						t.Fatalf("prefix %d SHA mismatch on restored init track %d", prefixLen, trkIdx)
					}
				}
				for _, seg := range trkSnap.Segments {
					for partIdx, p := range seg.Parts {
						b, err := restoredRing.ReadPart(trkIdx, seg.MSN, partIdx)
						if err != nil {
							t.Fatalf("prefix %d ReadPart track %d msn %d part %d: %v", prefixLen, trkIdx, seg.MSN, partIdx, err)
						}
						if testSHA(b) != p.SHA256 {
							t.Fatalf("prefix %d SHA mismatch on restored part track %d msn %d part %d", prefixLen, trkIdx, seg.MSN, partIdx)
						}
					}
				}
			}
		})
	}
}
