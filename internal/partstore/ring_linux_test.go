//go:build linux

package partstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

func testSHA(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func mustPwrite(t *testing.T, fd int, b []byte, offset int64) {
	t.Helper()
	n, err := unix.Pwrite(fd, b, offset)
	if err != nil {
		t.Fatalf("pwrite offset %d length %d: %v", offset, len(b), err)
	}
	if n != len(b) {
		t.Fatalf("pwrite short write: %d, want %d", n, len(b))
	}
}

func pwriteErr(fd int, b []byte, offset int64) error {
	n, err := unix.Pwrite(fd, b, offset)
	if err != nil {
		return fmt.Errorf("pwrite offset %d len %d: %w", offset, len(b), err)
	}
	if n != len(b) {
		return fmt.Errorf("short pwrite: %d != %d", n, len(b))
	}
	return nil
}

type testFixture struct {
	t       *testing.T
	run     string
	source  string
	epoch   uint64
	cfg     ipc.Config
	policy  Policy
	store   *EpochStore
	dups    []int
	records []Record
	ring    *Ring
}

func newTestFixture(t *testing.T, mode, codec string, capacity int64, policy Policy) *testFixture {
	t.Helper()
	run := "run-fixture"
	source := "src-fixture"
	epoch := uint64(101)

	tracks := ipc.TracksSolo
	if mode == ipc.Mode1v1 {
		tracks = ipc.Tracks1v1
	}
	cfg := ipc.Config{
		Mode:     mode,
		Codec:    codec,
		Tracks:   tracks,
		Capacity: capacity,
	}

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

	f := &testFixture{
		t:      t,
		run:    run,
		source: source,
		epoch:  epoch,
		cfg:    cfg,
		policy: policy,
		store:  store,
		dups:   dups,
	}

	persist := func(rec Record) error {
		f.records = append(f.records, rec)
		return nil
	}

	ring, err := NewRing(store, run, source, epoch, cfg, policy, persist)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	t.Cleanup(func() { _ = ring.Close() })
	f.ring = ring

	return f
}

// 1. Fixtures for 4/5 tracks, H264/HEVC, tiny cap 128..4096, zero-policy defaults 24s/3s and invalid constructor/persist/scope.
func TestRingAllocationMatrix(t *testing.T) {
	modes := []string{ipc.ModeSolo, ipc.Mode1v1}
	codecs := []string{ipc.CodecH264, ipc.CodecHEVC}
	caps := []int64{128, 512, 1024, 4096}

	for _, m := range modes {
		for _, c := range codecs {
			for _, capSize := range caps {
				testName := fmt.Sprintf("%s_%s_%d", m, c, capSize)
				t.Run(testName, func(t *testing.T) {
					f := newTestFixture(t, m, c, capSize, Policy{})
					snap, err := f.ring.Snapshot()
					if err != nil {
						t.Fatalf("Snapshot: %v", err)
					}
					if len(snap.Tracks) != f.cfg.Tracks {
						t.Fatalf("Tracks len %d, want %d", len(snap.Tracks), f.cfg.Tracks)
					}
				})
			}
		}
	}
}

func TestConstructorAndPolicyValidation(t *testing.T) {
	t.Run("zero_policy_defaults", func(t *testing.T) {
		pol, err := (Policy{}).effective()
		if err != nil {
			t.Fatalf("Policy{}.effective(): %v", err)
		}
		if pol.Window != DefaultWindow || pol.MaxSegment != DefaultMaxSegment {
			t.Fatalf("got window %v max %v, want defaults %v / %v", pol.Window, pol.MaxSegment, DefaultWindow, DefaultMaxSegment)
		}
	})

	t.Run("negative_policy", func(t *testing.T) {
		_, err := (Policy{Window: -1, MaxSegment: 1 * time.Second}).effective()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("negative window got %v, want ErrInvalidConfig", err)
		}
		_, err = (Policy{Window: 1 * time.Second, MaxSegment: -1}).effective()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("negative max segment got %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("policy_true_overflow", func(t *testing.T) {
		_, err := (Policy{Window: time.Duration(math.MaxInt64 - 100), MaxSegment: 1000 * time.Second}).effective()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("overflow policy got %v, want ErrInvalidConfig", err)
		}
	})

	// Known defect: ring_types.go:76 MaxSegment>MaxInt64/4 wrongly rejects non-overflowing Window+3*MaxSegment
	t.Run("policy_valid_boundary_defect_reproduce", func(t *testing.T) {
		validBoundaryPolicy := Policy{
			Window:     10 * time.Second,
			MaxSegment: time.Duration(math.MaxInt64/4 + 100),
		}
		eff, err := validBoundaryPolicy.effective()
		if err != nil {
			t.Fatalf("DEFECT ring_types.go:76: valid boundary policy (Window+3*MaxSegment < MaxInt64) rejected: %v", err)
		}
		if eff.MaxSegment != validBoundaryPolicy.MaxSegment {
			t.Fatalf("effective MaxSegment mismatch")
		}
	})

	t.Run("invalid_constructor_inputs", func(t *testing.T) {
		cfg := ipc.Config{Mode: ipc.ModeSolo, Codec: ipc.CodecH264, Tracks: 4, Capacity: 1024}
		store, err := AllocateEpoch("run", "src", 1, cfg)
		if err != nil {
			t.Fatalf("AllocateEpoch: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })

		dummyPersist := func(Record) error { return nil }

		// nil store
		if _, err := NewRing(nil, "run", "src", 1, cfg, Policy{}, dummyPersist); err == nil {
			t.Fatal("NewRing nil store expected error")
		}

		// nil persist
		if _, err := NewRing(store, "run", "src", 1, cfg, Policy{}, nil); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewRing nil persist got %v, want ErrInvalidConfig", err)
		}

		// epoch 0
		if _, err := NewRing(store, "run", "src", 0, cfg, Policy{}, dummyPersist); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewRing epoch 0 got %v, want ErrInvalidConfig", err)
		}

		// invalid cfg tracks
		badCfg := cfg
		badCfg.Tracks = 3
		if _, err := NewRing(store, "run", "src", 1, badCfg, Policy{}, dummyPersist); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewRing bad tracks got %v, want ErrInvalidConfig", err)
		}

		// store identity mismatch: store capacity does not match config
		mismatchCfg := cfg
		mismatchCfg.Capacity = 2048
		if _, err := NewRing(store, "run", "src", 1, mismatchCfg, Policy{}, dummyPersist); !errors.Is(err, ErrIdentityMismatch) {
			t.Fatalf("NewRing identity mismatch got %v, want ErrIdentityMismatch", err)
		}
	})

	t.Run("borrowed_store_survives_ring_close", func(t *testing.T) {
		f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})
		if err := f.ring.Close(); err != nil {
			t.Fatalf("Ring.Close: %v", err)
		}
		// Borrowed store should still be readable and open
		ids, err := f.store.Identities()
		if err != nil {
			t.Fatalf("store.Identities after Ring.Close: %v", err)
		}
		if len(ids) != f.cfg.Tracks {
			t.Fatalf("store.Identities len %d, want %d", len(ids), f.cfg.Tracks)
		}
		b, err := f.store.ReadSpan(0, 0, 16)
		if err != nil {
			t.Fatalf("store.ReadSpan after Ring.Close: %v", err)
		}
		if len(b) != 16 {
			t.Fatalf("read %d bytes", len(b))
		}
	})
}

// 2. Grants pinned init 0, one pending/track, overlaps never, invalid/forged/oversize no state/record change;
// canceled grant remains occupied until explicit CancelAfterQuiescence; ErrFull no eviction.
// Wrap/canceled-hole control: free gap straddling old cursor must fit; extents must remain contiguous.
// Initialization cannot be overwritten.
func TestGrantsAndPlacement(t *testing.T) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 512, Policy{})

	// Pinned init at 0
	resInit, err := f.ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve init: %v", err)
	}
	if resInit.Offset != 0 || resInit.Length != 64 {
		t.Fatalf("init grant offset %d len %d, want 0/64", resInit.Offset, resInit.Length)
	}

	// Second pending grant on same track returns ErrReservation
	if _, err := f.ring.Reserve(ipc.KindPart, 0, 32); !errors.Is(err, ErrReservation) {
		t.Fatalf("second pending grant got %v, want ErrReservation", err)
	}

	// Another track can have a pending grant concurrently
	resInit1, err := f.ring.Reserve(ipc.KindInit, 1, 64)
	if err != nil {
		t.Fatalf("Reserve init track 1: %v", err)
	}
	if resInit1.Offset != 0 {
		t.Fatalf("track 1 init offset %d, want 0", resInit1.Offset)
	}

	// Oversize grant returns ErrFull
	if _, err := f.ring.Reserve(ipc.KindInit, 2, 1024); !errors.Is(err, ErrFull) {
		t.Fatalf("oversize grant got %v, want ErrFull", err)
	}

	// Negative / zero length
	if _, err := f.ring.Reserve(ipc.KindInit, 2, 0); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("zero length got %v, want ErrInvalidPublication", err)
	}
	if _, err := f.ring.Reserve(ipc.KindInit, 2, -10); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("negative length got %v, want ErrInvalidPublication", err)
	}

	// Reserving part before init is committed on track 2
	if _, err := f.ring.Reserve(ipc.KindPart, 2, 32); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("part before init got %v, want ErrInvalidPublication", err)
	}

	// Commit init on track 0
	initData := bytes.Repeat([]byte{0xaa}, 64)
	mustPwrite(t, f.dups[0], initData, 0)
	initEntry := ipc.Entry{
		Version:         ipc.Version,
		Run:             f.run,
		Source:          f.source,
		Epoch:           f.epoch,
		Seq:             1,
		Kind:            ipc.KindInit,
		Track:           0,
		PublishedNS:     1000,
		PublishedMonoNS: 1000,
		Offset:          0,
		Length:          64,
		SHA256:          testSHA(initData),
		Codecs:          "avc1.64001f",
	}
	if err := f.ring.Commit(resInit, initEntry); err != nil {
		t.Fatalf("Commit init: %v", err)
	}

	// Second init reservation on track 0 refused
	if _, err := f.ring.Reserve(ipc.KindInit, 0, 64); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("second init reserve got %v, want ErrInvalidPublication", err)
	}

	// Canceled grant remains occupied until CancelAfterQuiescence
	resPart1, err := f.ring.Reserve(ipc.KindPart, 0, 100)
	if err != nil {
		t.Fatalf("Reserve part 1: %v", err)
	}
	if resPart1.Offset != 64 {
		t.Fatalf("resPart1 offset %d, want 64", resPart1.Offset)
	}

	// CancelAfterQuiescence releases it
	if err := f.ring.CancelAfterQuiescence(resPart1); err != nil {
		t.Fatalf("CancelAfterQuiescence: %v", err)
	}

	// Wrap / canceled-hole reuse: next reservation fits into the 64..164 hole or cursor
	resPart2, err := f.ring.Reserve(ipc.KindPart, 0, 80)
	if err != nil {
		t.Fatalf("Reserve part 2: %v", err)
	}
	if resPart2.Offset != 164 {
		t.Fatalf("resPart2 offset %d, want 164", resPart2.Offset)
	}

	// Commit part 2
	p2Data := bytes.Repeat([]byte{0xbb}, 80)
	mustPwrite(t, f.dups[0], p2Data, resPart2.Offset)
	p2Entry := ipc.Entry{
		Version:         ipc.Version,
		Run:             f.run,
		Source:          f.source,
		Epoch:           f.epoch,
		Seq:             2,
		Kind:            ipc.KindPart,
		Track:           0,
		PublishedNS:     2000,
		PublishedMonoNS: 2000,
		Offset:          resPart2.Offset,
		Length:          80,
		SHA256:          testSHA(p2Data),
		MSN:             1,
		Index:           0,
		PTS:             1000,
		DTS:             1000,
		Duration:        333333333,
		Sequence:        1,
		FragmentStart:   true,
		Independent:     true,
		Payload:         1,
		FirstAU:         0,
	}
	if err := f.ring.Commit(resPart2, p2Entry); err != nil {
		t.Fatalf("Commit part 2: %v", err)
	}

	// Reserve another part to near capacity
	// Capacity is 512. Committed: 0..64 (init), 164..244 (p2). Hole: 64..164 (100 bytes).
	// Cursor is at 244. Reserve 200 bytes -> 244..444.
	resPart3, err := f.ring.Reserve(ipc.KindPart, 0, 200)
	if err != nil {
		t.Fatalf("Reserve part 3: %v", err)
	}
	if resPart3.Offset != 244 {
		t.Fatalf("resPart3 offset %d, want 244", resPart3.Offset)
	}
	p3Data := bytes.Repeat([]byte{0xcc}, 200)
	mustPwrite(t, f.dups[0], p3Data, resPart3.Offset)
	p3Entry := ipc.Entry{
		Version:         ipc.Version,
		Run:             f.run,
		Source:          f.source,
		Epoch:           f.epoch,
		Seq:             3,
		Kind:            ipc.KindPart,
		Track:           0,
		PublishedNS:     3000,
		PublishedMonoNS: 3000,
		Offset:          resPart3.Offset,
		Length:          200,
		SHA256:          testSHA(p3Data),
		MSN:             1,
		Index:           1,
		PTS:             2000,
		DTS:             2000,
		Duration:        333333333,
		Sequence:        1,
		FragmentStart:   false,
		Independent:     false,
		Payload:         1,
		FirstAU:         1,
	}
	if err := f.ring.Commit(resPart3, p3Entry); err != nil {
		t.Fatalf("Commit part 3: %v", err)
	}

	// Now cursor is at 444. Remaining at end: 512 - 444 = 68 bytes.
	// Request 80 bytes: cannot fit at 444..512. Must wrap and fit into the 64..164 hole!
	resPart4, err := f.ring.Reserve(ipc.KindPart, 0, 80)
	if err != nil {
		t.Fatalf("Reserve part 4 wrapped into hole: %v", err)
	}
	if resPart4.Offset != 64 {
		t.Fatalf("resPart4 wrapped offset %d, want 64", resPart4.Offset)
	}

	// Init cannot be overwritten: init is at 0..64. Even after wrapping, fit cannot allocate at 0.
	if resPart4.Offset < 64 {
		t.Fatalf("grant overlapped init at 0..64")
	}
}

// 3. Write complete then commit only; wrong digest/token/span/scope/seq/timing/order leaves snapshots unchanged and grant pending.
// Valid metadata/terminal; terminal pending rejected; failed partial terminal allowed; verified all tracks requires audio phases.
// Read copies/snapshot deep-copy isolation, open segment ErrNotReady, segment concatenation after noncontiguous wrap, post-Close errors.
func TestCommitValidationAndReplayGate(t *testing.T) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})

	// Reserve init track 0
	resInit, err := f.ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve init: %v", err)
	}

	initData := bytes.Repeat([]byte{0x11}, 64)
	mustPwrite(t, f.dups[0], initData, 0)
	initEntry := ipc.Entry{
		Version:         ipc.Version,
		Run:             f.run,
		Source:          f.source,
		Epoch:           f.epoch,
		Seq:             1,
		Kind:            ipc.KindInit,
		Track:           0,
		PublishedNS:     1000,
		PublishedMonoNS: 1000,
		Offset:          0,
		Length:          64,
		SHA256:          testSHA(initData),
		Codecs:          "avc1.64001f",
	}

	// Wrong digest leaves snapshot unchanged and grant pending
	badDigestEntry := initEntry
	badDigestEntry.SHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	snapBefore, err := f.ring.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot before: %v", err)
	}
	if err := f.ring.Commit(resInit, badDigestEntry); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("commit bad digest got %v, want ErrInvalidPublication", err)
	}
	snapAfter, err := f.ring.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot after: %v", err)
	}
	if snapAfter.Tracks[0].Init != nil {
		t.Fatal("init was committed despite bad digest")
	}
	if len(snapAfter.Pending) != 1 {
		t.Fatal("pending grant was dropped on failed commit")
	}
	if !reflect.DeepEqual(snapBefore, snapAfter) {
		t.Fatalf("snapshot changed on bad digest commit: before %+v, after %+v", snapBefore, snapAfter)
	}

	// Wrong token/reservation ID
	forgedRes := resInit
	forgedRes.ID = 9999
	if err := f.ring.Commit(forgedRes, initEntry); !errors.Is(err, ErrReservation) {
		t.Fatalf("commit forged res got %v, want ErrReservation", err)
	}

	// Commit valid init
	if err := f.ring.Commit(resInit, initEntry); err != nil {
		t.Fatalf("Commit valid init: %v", err)
	}

	// Replay gate test (Known defect ring_linux.go:177-188)
	// Contract: Apply on an exact duplicate of a retained entry is a no-op (return nil).
	t.Run("apply_retained_init_replay_defect_reproduce", func(t *testing.T) {
		err := f.ring.Apply(initEntry)
		if err != nil {
			t.Fatalf("DEFECT ring_linux.go:177-188: Apply replay of retained init entry rejected: %v", err)
		}
	})

	// Part 0 on track 0
	resP0, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p0: %v", err)
	}
	p0Data := bytes.Repeat([]byte{0x22}, 64)
	mustPwrite(t, f.dups[0], p0Data, resP0.Offset)
	p0Entry := ipc.Entry{
		Version:         ipc.Version,
		Run:             f.run,
		Source:          f.source,
		Epoch:           f.epoch,
		Seq:             2,
		Kind:            ipc.KindPart,
		Track:           0,
		PublishedNS:     2000,
		PublishedMonoNS: 2000,
		Offset:          resP0.Offset,
		Length:          64,
		SHA256:          testSHA(p0Data),
		MSN:             1,
		Index:           0,
		PTS:             1000,
		DTS:             1000,
		Duration:        333333333,
		Sequence:        1,
		FragmentStart:   true,
		Independent:     true,
		Payload:         1,
		FirstAU:         0,
	}
	if err := f.ring.Commit(resP0, p0Entry); err != nil {
		t.Fatalf("Commit p0: %v", err)
	}

	t.Run("apply_retained_part_replay_defect_reproduce", func(t *testing.T) {
		err := f.ring.Apply(p0Entry)
		if err != nil {
			t.Fatalf("DEFECT ring_linux.go:177-188: Apply replay of retained part entry rejected: %v", err)
		}
	})

	// Video fragment start independence on segment 2 (Known defect ring_records_linux.go:163-179)
	// Contract: EVERY video fragment start must be independent per plan §7.5.
	t.Run("video_fragment_start_independent_defect_reproduce", func(t *testing.T) {
		resP1, err := f.ring.Reserve(ipc.KindPart, 0, 64)
		if err != nil {
			t.Fatalf("Reserve p1: %v", err)
		}
		p1Data := bytes.Repeat([]byte{0x33}, 64)
		mustPwrite(t, f.dups[0], p1Data, resP1.Offset)
		// MSN 2, Part 0, FragmentStart true, but Independent FALSE
		p1NonIndepEntry := ipc.Entry{
			Version:         ipc.Version,
			Run:             f.run,
			Source:          f.source,
			Epoch:           f.epoch,
			Seq:             3,
			Kind:            ipc.KindPart,
			Track:           0,
			PublishedNS:     3000,
			PublishedMonoNS: 3000,
			Offset:          resP1.Offset,
			Length:          64,
			SHA256:          testSHA(p1Data),
			MSN:             2,
			Index:           0,
			PTS:             2000,
			DTS:             2000,
			Duration:        333333333,
			Sequence:        2,
			FragmentStart:   true,
			Independent:     false, // Non-independent video fragment start!
			Payload:         1,
			FirstAU:         1,
		}
		err = f.ring.Commit(resP1, p1NonIndepEntry)
		if err == nil {
			t.Fatalf("DEFECT ring_records_linux.go:163-179: non-independent video fragment start MSN 2 was admitted")
		}
		if !errors.Is(err, ErrInvalidPublication) {
			t.Fatalf("got %v, want ErrInvalidPublication", err)
		}
	})
}

func TestAudioPhasesGateAndCheckedArithmetic(t *testing.T) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})

	// Commit init on audio track (TrackAudioA = 3)
	resInitA, err := f.ring.Reserve(ipc.KindInit, ipc.TrackAudioA, 64)
	if err != nil {
		t.Fatalf("Reserve audio init: %v", err)
	}
	initAData := bytes.Repeat([]byte{0x44}, 64)
	mustPwrite(t, f.dups[ipc.TrackAudioA], initAData, 0)
	initAEntry := ipc.Entry{
		Version:         ipc.Version,
		Run:             f.run,
		Source:          f.source,
		Epoch:           f.epoch,
		Seq:             1,
		Kind:            ipc.KindInit,
		Track:           ipc.TrackAudioA,
		PublishedNS:     1000,
		PublishedMonoNS: 1000,
		Offset:          0,
		Length:          64,
		SHA256:          testSHA(initAData),
		Codecs:          "mp4a.40.2",
	}
	if err := f.ring.Commit(resInitA, initAEntry); err != nil {
		t.Fatalf("Commit audio init: %v", err)
	}

	// Audio part without phases (Known defect ring_records_linux.go:154-158)
	// Contract requires phases before audio parts.
	t.Run("audio_part_without_phases_defect_reproduce", func(t *testing.T) {
		resAudioPart, err := f.ring.Reserve(ipc.KindPart, ipc.TrackAudioA, 64)
		if err != nil {
			t.Fatalf("Reserve audio part: %v", err)
		}
		partData := bytes.Repeat([]byte{0x55}, 64)
		mustPwrite(t, f.dups[ipc.TrackAudioA], partData, resAudioPart.Offset)
		audioPartEntry := ipc.Entry{
			Version:         ipc.Version,
			Run:             f.run,
			Source:          f.source,
			Epoch:           f.epoch,
			Seq:             2,
			Kind:            ipc.KindPart,
			Track:           ipc.TrackAudioA,
			PublishedNS:     2000,
			PublishedMonoNS: 2000,
			Offset:          resAudioPart.Offset,
			Length:          64,
			SHA256:          testSHA(partData),
			MSN:             1,
			Index:           0,
			PTS:             1000,
			DTS:             1000,
			Duration:        320000000,
			Sequence:        1,
			FragmentStart:   true,
			Independent:     true,
			Payload:         1,
			FirstAU:         0,
		}
		err = f.ring.Commit(resAudioPart, audioPartEntry)
		if err == nil {
			t.Fatalf("DEFECT ring_records_linux.go:154-158: audio part admitted without prior phases")
		}
		if !errors.Is(err, ErrInvalidPublication) {
			t.Fatalf("got %v, want ErrInvalidPublication", err)
		}
	})

	// Checked arithmetic: FirstAU + Payload overflow in current entry
	// Establish valid predecessor with Payload = MaxInt - 5, then contiguous next FirstAU with Payload = 10.
	t.Run("first_au_payload_overflow_defect_reproduce", func(t *testing.T) {
		f2 := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})
		resInitV, err := f2.ring.Reserve(ipc.KindInit, 0, 64)
		if err != nil {
			t.Fatalf("Reserve init: %v", err)
		}
		vInitData := bytes.Repeat([]byte{0x12}, 64)
		mustPwrite(t, f2.dups[0], vInitData, 0)
		if err := f2.ring.Commit(resInitV, ipc.Entry{
			Version: ipc.Version, Run: f2.run, Source: f2.source, Epoch: f2.epoch, Seq: 1,
			Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
			Offset: 0, Length: 64, SHA256: testSHA(vInitData), Codecs: "avc1.64001f",
		}); err != nil {
			t.Fatalf("Commit init: %v", err)
		}

		// Valid predecessor: part 0 with FirstAU=0, Payload=math.MaxInt - 5
		resP0, err := f2.ring.Reserve(ipc.KindPart, 0, 64)
		if err != nil {
			t.Fatalf("Reserve p0: %v", err)
		}
		p0Data := bytes.Repeat([]byte{0x34}, 64)
		mustPwrite(t, f2.dups[0], p0Data, resP0.Offset)
		if err := f2.ring.Commit(resP0, ipc.Entry{
			Version: ipc.Version, Run: f2.run, Source: f2.source, Epoch: f2.epoch, Seq: 2,
			Kind: ipc.KindPart, Track: 0, PublishedNS: 200, PublishedMonoNS: 200,
			Offset: resP0.Offset, Length: 64, SHA256: testSHA(p0Data),
			MSN: 1, Index: 0, PTS: 100, DTS: 100, Duration: 100_000_000, Sequence: 1,
			FragmentStart: true, Independent: true,
			FirstAU: 0,
			Payload: math.MaxInt - 5,
		}); err != nil {
			t.Fatalf("Commit valid predecessor: %v", err)
		}

		// Contiguous next entry: FirstAU follows 0 + (MaxInt-5) = MaxInt-5, but FirstAU + Payload (MaxInt-5 + 10) overflows math.MaxInt
		resP1, err := f2.ring.Reserve(ipc.KindPart, 0, 64)
		if err != nil {
			t.Fatalf("Reserve p1: %v", err)
		}
		p1Data := bytes.Repeat([]byte{0x56}, 64)
		mustPwrite(t, f2.dups[0], p1Data, resP1.Offset)
		overflowEntry := ipc.Entry{
			Version: ipc.Version, Run: f2.run, Source: f2.source, Epoch: f2.epoch, Seq: 3,
			Kind: ipc.KindPart, Track: 0, PublishedNS: 300, PublishedMonoNS: 300,
			Offset: resP1.Offset, Length: 64, SHA256: testSHA(p1Data),
			MSN: 1, Index: 1, PTS: 200, DTS: 200, Duration: 100_000_000, Sequence: 1,
			FragmentStart: false, Independent: false,
			FirstAU: math.MaxInt - 5,
			Payload: 10, // MaxInt - 5 + 10 overflows math.MaxInt!
		}
		err = f2.ring.Commit(resP1, overflowEntry)
		if err == nil {
			t.Fatalf("DEFECT ring_records_linux.go:188: current entry FirstAU+Payload overflow was admitted without checked arithmetic")
		}
	})

	// Synthetic prior fragment Sequence = MaxUint64 -> next 0 rejection, state/Persist unchanged
	t.Run("sequence_wrap_max_uint64_defect_reproduce", func(t *testing.T) {
		f3 := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})
		resInitV, err := f3.ring.Reserve(ipc.KindInit, 0, 64)
		if err != nil {
			t.Fatalf("Reserve init: %v", err)
		}
		vInitData := bytes.Repeat([]byte{0x99}, 64)
		mustPwrite(t, f3.dups[0], vInitData, 0)
		if err := f3.ring.Commit(resInitV, ipc.Entry{
			Version: ipc.Version, Run: f3.run, Source: f3.source, Epoch: f3.epoch, Seq: 1,
			Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
			Offset: 0, Length: 64, SHA256: testSHA(vInitData), Codecs: "avc1.64001f",
		}); err != nil {
			t.Fatalf("Commit init: %v", err)
		}

		resP0, err := f3.ring.Reserve(ipc.KindPart, 0, 64)
		if err != nil {
			t.Fatalf("Reserve p0: %v", err)
		}
		p0Data := bytes.Repeat([]byte{0xaa}, 64)
		mustPwrite(t, f3.dups[0], p0Data, resP0.Offset)
		if err := f3.ring.Commit(resP0, ipc.Entry{
			Version: ipc.Version, Run: f3.run, Source: f3.source, Epoch: f3.epoch, Seq: 2,
			Kind: ipc.KindPart, Track: 0, PublishedNS: 200, PublishedMonoNS: 200,
			Offset: resP0.Offset, Length: 64, SHA256: testSHA(p0Data),
			MSN: 1, Index: 0, PTS: 100, DTS: 100, Duration: 100_000_000, Sequence: 1,
			FragmentStart: true, Independent: true, FirstAU: 0, Payload: 1,
		}); err != nil {
			t.Fatalf("Commit p0: %v", err)
		}

		// Synthetic boundary control: set prior fragment sequence to MaxUint64
		f3.ring.tracks[0].last.Sequence = math.MaxUint64

		resNextSeg, err := f3.ring.Reserve(ipc.KindPart, 0, 64)
		if err != nil {
			t.Fatalf("Reserve next segment: %v", err)
		}
		nextData := bytes.Repeat([]byte{0xbb}, 64)
		mustPwrite(t, f3.dups[0], nextData, resNextSeg.Offset)

		// Next segment sequence wraps to 0 if unchecked: Sequence = 0
		recCountBefore := len(f3.records)
		nextWrappedEntry := ipc.Entry{
			Version: ipc.Version, Run: f3.run, Source: f3.source, Epoch: f3.epoch, Seq: 3,
			Kind: ipc.KindPart, Track: 0, PublishedNS: 300, PublishedMonoNS: 300,
			Offset: resNextSeg.Offset, Length: 64, SHA256: testSHA(nextData),
			MSN: 2, Index: 0, PTS: 200, DTS: 200, Duration: 100_000_000,
			Sequence:      0, // Wraps around MaxUint64 + 1
			FragmentStart: true, Independent: true, FirstAU: 1, Payload: 1,
		}
		err = f3.ring.Commit(resNextSeg, nextWrappedEntry)
		if err == nil {
			t.Fatalf("DEFECT: fragment sequence wrapped MaxUint64 into 0 and was admitted")
		}
		if len(f3.records) != recCountBefore {
			t.Fatal("Persist was called despite invalid sequence overflow")
		}
	})
}

func TestTerminalEntryRules(t *testing.T) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})

	// Reserve on track 0 and leave pending
	_, err := f.ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	// Terminal while grant pending -> rejected
	termEntry := ipc.Entry{
		Version:         ipc.Version,
		Run:             f.run,
		Source:          f.source,
		Epoch:           f.epoch,
		Seq:             1,
		Kind:            ipc.KindTerminal,
		Track:           ipc.TrackTerminal,
		PublishedNS:     1000,
		PublishedMonoNS: 1000,
		Verified:        false,
		Reason:          "stream ended",
	}
	if err := f.ring.Apply(termEntry); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("terminal while grant pending got %v, want ErrInvalidPublication", err)
	}

	// Clean fixture for failed partial terminal
	f2 := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})
	termPartial := ipc.Entry{
		Version:         ipc.Version,
		Run:             f2.run,
		Source:          f2.source,
		Epoch:           f2.epoch,
		Seq:             1,
		Kind:            ipc.KindTerminal,
		Track:           ipc.TrackTerminal,
		PublishedNS:     1000,
		PublishedMonoNS: 1000,
		Verified:        false,
		Reason:          "worker error",
	}
	if err := f2.ring.Apply(termPartial); err != nil {
		t.Fatalf("failed partial terminal should be allowed: %v", err)
	}

	// After terminal, further grants or entries rejected
	if _, err := f2.ring.Reserve(ipc.KindInit, 0, 64); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("grant after terminal got %v, want ErrInvalidPublication", err)
	}

	// Full verified terminal across all 5 tracks with audio phases (Mode1v1)
	f3 := newTestFixture(t, ipc.Mode1v1, ipc.CodecH264, 2048, Policy{})
	seq := uint64(1)

	// Step 1: Init on all 5 tracks
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		res, err := f3.ring.Reserve(ipc.KindInit, trk, 64)
		if err != nil {
			t.Fatalf("Reserve init track %d: %v", trk, err)
		}
		data := bytes.Repeat([]byte{byte(trk + 1)}, 64)
		mustPwrite(t, f3.dups[trk], data, 0)
		codec := "avc1.64001f"
		if trk >= ipc.TrackAudioA {
			codec = "mp4a.40.2"
		}
		if err := f3.ring.Commit(res, ipc.Entry{
			Version: ipc.Version, Run: f3.run, Source: f3.source, Epoch: f3.epoch, Seq: seq,
			Kind: ipc.KindInit, Track: trk, PublishedNS: 100, PublishedMonoNS: 100,
			Offset: 0, Length: 64, SHA256: testSHA(data), Codecs: codec,
		}); err != nil {
			t.Fatalf("Commit init track %d: %v", trk, err)
		}
		seq++
	}

	// Step 2: Phases on audio tracks 3 and 4
	for trk := ipc.TrackAudioA; trk <= ipc.TrackAudioB; trk++ {
		if err := f3.ring.Apply(ipc.Entry{
			Version: ipc.Version, Run: f3.run, Source: f3.source, Epoch: f3.epoch, Seq: seq,
			Kind: ipc.KindPhases, Track: trk, PublishedNS: 200, PublishedMonoNS: 200,
			PhaseVideo: 1000, PhaseAudio: 1000,
		}); err != nil {
			t.Fatalf("Apply phases track %d: %v", trk, err)
		}
		seq++
	}

	// Step 3: Part on all 5 tracks
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		res, err := f3.ring.Reserve(ipc.KindPart, trk, 64)
		if err != nil {
			t.Fatalf("Reserve part track %d: %v", trk, err)
		}
		data := bytes.Repeat([]byte{byte(trk + 10)}, 64)
		mustPwrite(t, f3.dups[trk], data, res.Offset)
		dur := int64(333333333)
		if trk >= ipc.TrackAudioA {
			dur = 320000000
		}
		if err := f3.ring.Commit(res, ipc.Entry{
			Version: ipc.Version, Run: f3.run, Source: f3.source, Epoch: f3.epoch, Seq: seq,
			Kind: ipc.KindPart, Track: trk, PublishedNS: 300, PublishedMonoNS: 300,
			Offset: res.Offset, Length: 64, SHA256: testSHA(data),
			MSN: 1, Index: 0, PTS: 100, DTS: 100, Duration: dur, Sequence: 1,
			FragmentStart: true, Independent: true, Payload: 1, FirstAU: 0,
		}); err != nil {
			t.Fatalf("Commit part track %d: %v", trk, err)
		}
		seq++
	}

	// Step 4: Valid verified terminal across all tracks
	termVerified := ipc.Entry{
		Version: ipc.Version, Run: f3.run, Source: f3.source, Epoch: f3.epoch, Seq: seq,
		Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, PublishedNS: 400, PublishedMonoNS: 400,
		Verified: true, Reason: "clean shutdown",
	}
	if err := f3.ring.Apply(termVerified); err != nil {
		t.Fatalf("Apply verified terminal failed: %v", err)
	}

	snap, err := f3.ring.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Terminal == nil || !snap.Terminal.Verified {
		t.Fatal("terminal not recorded as verified in snapshot")
	}
	for i, trkSnap := range snap.Tracks {
		if len(trkSnap.Segments) == 0 || !trkSnap.Segments[0].Closed {
			t.Fatalf("track %d segment 0 not closed by terminal", i)
		}
	}
}

func TestReadsAndSnapshotIsolation(t *testing.T) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})

	// Init
	resInit, err := f.ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve init: %v", err)
	}
	initData := bytes.Repeat([]byte{0x77}, 64)
	mustPwrite(t, f.dups[0], initData, 0)
	initEntry := ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 1,
		Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
		Offset: 0, Length: 64, SHA256: testSHA(initData), Codecs: "avc1.64001f",
	}
	if err := f.ring.Commit(resInit, initEntry); err != nil {
		t.Fatalf("Commit init: %v", err)
	}

	// Part 0
	resP0, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p0: %v", err)
	}
	p0Data := bytes.Repeat([]byte{0x88}, 64)
	mustPwrite(t, f.dups[0], p0Data, resP0.Offset)
	p0Entry := ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 2,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 200, PublishedMonoNS: 200,
		Offset: resP0.Offset, Length: 64, SHA256: testSHA(p0Data),
		MSN: 1, Index: 0, PTS: 100, DTS: 100, Duration: 333333333, Sequence: 1,
		FragmentStart: true, Independent: true, Payload: 1, FirstAU: 0,
	}
	if err := f.ring.Commit(resP0, p0Entry); err != nil {
		t.Fatalf("Commit p0: %v", err)
	}

	// ReadInit: owned copy test
	bInit, err := f.ring.ReadInit(0)
	if err != nil || !bytes.Equal(bInit, initData) {
		t.Fatalf("ReadInit failed: %v", err)
	}
	bInit[0] = 0x00 // mutate caller slice
	bInit2, err := f.ring.ReadInit(0)
	if err != nil || bInit2[0] == 0x00 {
		t.Fatal("ReadInit returned unowned shared slice")
	}

	// ReadPart: owned copy test
	bP0, err := f.ring.ReadPart(0, 1, 0)
	if err != nil || !bytes.Equal(bP0, p0Data) {
		t.Fatalf("ReadPart failed: %v", err)
	}
	bP0[0] = 0x00
	bP0_2, err := f.ring.ReadPart(0, 1, 0)
	if err != nil || bP0_2[0] == 0x00 {
		t.Fatal("ReadPart returned unowned shared slice")
	}

	// Open segment ReadSegment returns ErrNotReady
	if _, err := f.ring.ReadSegment(0, 1); !errors.Is(err, ErrNotReady) {
		t.Fatalf("ReadSegment on open segment got %v, want ErrNotReady", err)
	}

	// Snapshot deep-copy isolation
	snap1, err := f.ring.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	snap1.Tracks[0].Segments[0].Parts[0].SHA256 = "mutated"
	snap2, err := f.ring.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}
	if snap2.Tracks[0].Segments[0].Parts[0].SHA256 == "mutated" {
		t.Fatal("Snapshot returned shallow copy; internal state mutated")
	}

	// Successful noncontiguous closed-segment concatenation
	// Create a noncontiguous gap in track 0 by reserving and canceling a hole
	resGap, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve gap: %v", err)
	}
	if err := f.ring.CancelAfterQuiescence(resGap); err != nil {
		t.Fatalf("CancelAfterQuiescence gap: %v", err)
	}

	// Part 1 of segment 1 on track 0 placed noncontiguously (after the canceled gap)
	resP1, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p1: %v", err)
	}
	if resP1.Offset <= resP0.Offset+resP0.Length {
		t.Fatalf("expected noncontiguous offset > %d, got %d", resP0.Offset+resP0.Length, resP1.Offset)
	}
	p1Data := bytes.Repeat([]byte{0x99}, 64)
	mustPwrite(t, f.dups[0], p1Data, resP1.Offset)
	p1Entry := ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 3,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 300, PublishedMonoNS: 300,
		Offset: resP1.Offset, Length: 64, SHA256: testSHA(p1Data),
		MSN: 1, Index: 1, PTS: 200, DTS: 200, Duration: 333333333, Sequence: 1,
		FragmentStart: false, Independent: false, Payload: 1, FirstAU: 1,
	}
	if err := f.ring.Commit(resP1, p1Entry); err != nil {
		t.Fatalf("Commit p1: %v", err)
	}

	// Close segment 1 by starting segment 2
	resP2, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve p2: %v", err)
	}
	p2Data := bytes.Repeat([]byte{0xaa}, 64)
	mustPwrite(t, f.dups[0], p2Data, resP2.Offset)
	p2Entry := ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 4,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 400, PublishedMonoNS: 400,
		Offset: resP2.Offset, Length: 64, SHA256: testSHA(p2Data),
		MSN: 2, Index: 0, PTS: 300, DTS: 300, Duration: 333333333, Sequence: 2,
		FragmentStart: true, Independent: true, Payload: 1, FirstAU: 2,
	}
	if err := f.ring.Commit(resP2, p2Entry); err != nil {
		t.Fatalf("Commit p2: %v", err)
	}

	// ReadSegment on closed segment 1 returns concatenated parts
	segBytes, err := f.ring.ReadSegment(0, 1)
	if err != nil {
		t.Fatalf("ReadSegment: %v", err)
	}
	expectedConcatenated := append(p0Data, p1Data...)
	if !bytes.Equal(segBytes, expectedConcatenated) {
		t.Fatal("ReadSegment concatenated bytes mismatch")
	}

	// Owned copy on ReadSegment
	segBytes[0] = 0x00
	segBytes2, err := f.ring.ReadSegment(0, 1)
	if err != nil || segBytes2[0] == 0x00 {
		t.Fatal("ReadSegment returned unowned shared slice")
	}
}

// 4. Drop oldest visible CLOSED/outside Window only; dropped bytes still readable;
// deadline=dropAt+Window+2*MaxSegment+actualDuration. Early Retire/open/live/pending forbidden;
// deadline-exact retire removes metadata/bytes from lookup and permits reuse.
// No automatic time-based grant release. Checked monotonic clocks/deadline overflow.
func TestDropAndRetireLifecycle(t *testing.T) {
	pol := Policy{Window: 2 * time.Second, MaxSegment: 1 * time.Second}
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 2048, pol)

	// Init
	resInit, err := f.ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve init: %v", err)
	}
	initData := bytes.Repeat([]byte{0x01}, 64)
	mustPwrite(t, f.dups[0], initData, 0)
	if err := f.ring.Commit(resInit, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 1,
		Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
		Offset: 0, Length: 64, SHA256: testSHA(initData), Codecs: "avc1.64001f",
	}); err != nil {
		t.Fatalf("Commit init: %v", err)
	}

	// Seg 1 (MSN 1): 1 second duration
	resS1, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve s1: %v", err)
	}
	s1Data := bytes.Repeat([]byte{0x02}, 64)
	mustPwrite(t, f.dups[0], s1Data, resS1.Offset)
	if err := f.ring.Commit(resS1, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 2,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 1_000_000_000, PublishedMonoNS: 1_000_000_000,
		Offset: resS1.Offset, Length: 64, SHA256: testSHA(s1Data),
		MSN: 1, Index: 0, PTS: 1_000_000_000, DTS: 1_000_000_000,
		Duration: 1_000_000_000, Sequence: 1, FragmentStart: true, Independent: true,
		Payload: 1, FirstAU: 0,
	}); err != nil {
		t.Fatalf("Commit s1: %v", err)
	}

	// Seg 1 is open -> cannot drop
	if err := f.ring.DropSegment(0, 1, 1_000_000_000); !errors.Is(err, ErrNotReady) {
		t.Fatalf("drop open segment got %v, want ErrNotReady", err)
	}

	// Close seg 1 by starting seg 2 (MSN 2)
	resS2, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve s2: %v", err)
	}
	s2Data := bytes.Repeat([]byte{0x03}, 64)
	mustPwrite(t, f.dups[0], s2Data, resS2.Offset)
	if err := f.ring.Commit(resS2, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 3,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 2_000_000_000, PublishedMonoNS: 2_000_000_000,
		Offset: resS2.Offset, Length: 64, SHA256: testSHA(s2Data),
		MSN: 2, Index: 0, PTS: 2_000_000_000, DTS: 2_000_000_000,
		Duration: 1_000_000_000, Sequence: 2, FragmentStart: true, Independent: true,
		Payload: 1, FirstAU: 1,
	}); err != nil {
		t.Fatalf("Commit s2: %v", err)
	}

	// Seg 1 is now closed, but seg 1 end is 1s, timeline end is 2s. Window is 2s.
	if err := f.ring.DropSegment(0, 1, 2_000_000_000); !errors.Is(err, ErrNotReady) {
		t.Fatalf("drop segment inside window got %v, want ErrNotReady", err)
	}

	// Advance timeline with seg 3 (MSN 3) duration 1s -> timeline end becomes 3s
	resS3, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve s3: %v", err)
	}
	s3Data := bytes.Repeat([]byte{0x04}, 64)
	mustPwrite(t, f.dups[0], s3Data, resS3.Offset)
	if err := f.ring.Commit(resS3, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 4,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 3_000_000_000, PublishedMonoNS: 3_000_000_000,
		Offset: resS3.Offset, Length: 64, SHA256: testSHA(s3Data),
		MSN: 3, Index: 0, PTS: 3_000_000_000, DTS: 3_000_000_000,
		Duration: 1_000_000_000, Sequence: 3, FragmentStart: true, Independent: true,
		Payload: 1, FirstAU: 2,
	}); err != nil {
		t.Fatalf("Commit s3: %v", err)
	}

	// Advance timeline with seg 4 (MSN 4) duration 1s -> timeline end becomes 4s
	resS4, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve s4: %v", err)
	}
	s4Data := bytes.Repeat([]byte{0x05}, 64)
	mustPwrite(t, f.dups[0], s4Data, resS4.Offset)
	if err := f.ring.Commit(resS4, ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 5,
		Kind: ipc.KindPart, Track: 0, PublishedNS: 4_000_000_000, PublishedMonoNS: 4_000_000_000,
		Offset: resS4.Offset, Length: 64, SHA256: testSHA(s4Data),
		MSN: 4, Index: 0, PTS: 4_000_000_000, DTS: 4_000_000_000,
		Duration: 1_000_000_000, Sequence: 4, FragmentStart: true, Independent: true,
		Payload: 1, FirstAU: 3,
	}); err != nil {
		t.Fatalf("Commit s4: %v", err)
	}

	// Oldest-only drop: attempting to drop MSN 2 while MSN 1 is still visible returns ErrInvalidPublication
	if err := f.ring.DropSegment(0, 2, 4_000_000_000); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("drop MSN 2 while MSN 1 visible got %v, want ErrInvalidPublication", err)
	}

	dropClock := int64(4_000_000_000)
	if err := f.ring.DropSegment(0, 1, dropClock); err != nil {
		t.Fatalf("DropSegment seg 1: %v", err)
	}

	// Dropped bytes still readable!
	bS1, err := f.ring.ReadPart(0, 1, 0)
	if err != nil || !bytes.Equal(bS1, s1Data) {
		t.Fatalf("ReadPart on dropped segment failed: %v", err)
	}

	// Deadline calculation: dropClock (4s) + Window (2s) + 2*MaxSegment (2*1s=2s) + duration (1s) = 9s
	expectedDeadline := dropClock + int64(pol.Window+2*pol.MaxSegment) + 1_000_000_000
	snap, _ := f.ring.Snapshot()
	segSnap := snap.Tracks[0].Segments[0]
	if !segSnap.Dropped || segSnap.DeadlineNS != expectedDeadline {
		t.Fatalf("deadline %d, want %d", segSnap.DeadlineNS, expectedDeadline)
	}

	// Early retire (< deadline) forbidden
	if err := f.ring.RetireSegment(0, 1, expectedDeadline-1); !errors.Is(err, ErrNotReady) {
		t.Fatalf("early retire got %v, want ErrNotReady", err)
	}

	// Exact deadline retire succeeds
	if err := f.ring.RetireSegment(0, 1, expectedDeadline); err != nil {
		t.Fatalf("exact deadline retire: %v", err)
	}

	// After retire, metadata & bytes removed from lookup
	if _, err := f.ring.ReadPart(0, 1, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadPart on retired part got %v, want ErrNotFound", err)
	}

	// Retired part Apply ErrNotFound
	// Contract: an entry sequence that is no longer retained is ErrNotFound
	t.Run("retired_part_apply_not_found_defect_reproduce", func(t *testing.T) {
		retiredPartEntry := ipc.Entry{
			Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 2,
			Kind: ipc.KindPart, Track: 0, PublishedNS: 1_000_000_000, PublishedMonoNS: 1_000_000_000,
			Offset: resS1.Offset, Length: 64, SHA256: testSHA(s1Data),
			MSN: 1, Index: 0, PTS: 1_000_000_000, DTS: 1_000_000_000,
			Duration: 1_000_000_000, Sequence: 1, FragmentStart: true, Independent: true,
			Payload: 1, FirstAU: 0,
		}
		err := f.ring.Apply(retiredPartEntry)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("DEFECT ring_linux.go:177-188: Apply of retired part got %v, want ErrNotFound", err)
		}
	})
}

// 7. Concurrent compliant pending writer vs published readers/cancel/retire/Close:
// explicit start barriers, channel/error collection, guaranteed cancel+join Cleanup, no sleep-only races.
// Deliberate stray write committed bytes yields nil result + digest error; never label it prevention.
type concurrentTestContext struct {
	done chan struct{}
	once sync.Once
}

func newConcurrentTestContext() *concurrentTestContext {
	return &concurrentTestContext{done: make(chan struct{})}
}

func (c *concurrentTestContext) Cancel() {
	c.once.Do(func() { close(c.done) })
}

func (c *concurrentTestContext) Done() <-chan struct{} {
	return c.done
}

func TestConcurrentWriterAndReaders(t *testing.T) {
	// 1. Concurrent test coordinator creates valid init plus five 64-byte parts,
	// capacity 384 (full), Window 1s / MaxSegment 500ms, durations 500ms.
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 384, Policy{
		Window:     1 * time.Second,
		MaxSegment: 500 * time.Millisecond,
	})

	// Valid init (64 bytes)
	resInit, err := f.ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("setup Reserve init: %v", err)
	}
	if resInit.Offset != 0 || resInit.Length != 64 {
		t.Fatalf("setup resInit offset=%d len=%d, want 0, 64", resInit.Offset, resInit.Length)
	}
	initData := bytes.Repeat([]byte{0x10}, 64)
	mustPwrite(t, f.dups[0], initData, 0)
	initEntry := ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 1,
		Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
		Offset: 0, Length: 64, SHA256: testSHA(initData), Codecs: "avc1.64001f",
	}
	if err := f.ring.Commit(resInit, initEntry); err != nil {
		t.Fatalf("setup Commit init: %v", err)
	}

	// Five 64-byte parts, capacity 384 (full), durations 500ms
	var msn1Data, msn2Data []byte
	for i := 0; i < 5; i++ {
		msn := i + 1
		seq := uint64(i + 2)
		clock := int64(msn) * 1_000_000_000 // 1s, 2s, 3s, 4s, 5s
		res, err := f.ring.Reserve(ipc.KindPart, 0, 64)
		if err != nil {
			t.Fatalf("setup Reserve part msn %d: %v", msn, err)
		}
		expectedOffset := int64(64 + i*64)
		if res.Offset != expectedOffset {
			t.Fatalf("setup part msn %d offset=%d, want %d", msn, res.Offset, expectedOffset)
		}
		pData := bytes.Repeat([]byte{byte(0x20 + i)}, 64)
		if msn == 1 {
			msn1Data = bytes.Clone(pData)
		} else if msn == 2 {
			msn2Data = bytes.Clone(pData)
		}
		mustPwrite(t, f.dups[0], pData, res.Offset)
		pEntry := ipc.Entry{
			Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: seq,
			Kind: ipc.KindPart, Track: 0, PublishedNS: clock, PublishedMonoNS: clock,
			Offset: res.Offset, Length: 64, SHA256: testSHA(pData),
			MSN: msn, Index: 0, PTS: clock, DTS: clock,
			Duration: 500_000_000, Sequence: uint64(msn), FragmentStart: true, Independent: true,
			Payload: 1, FirstAU: i,
		}
		if err := f.ring.Commit(res, pEntry); err != nil {
			t.Fatalf("setup Commit part msn %d: %v", msn, err)
		}
	}

	// Capture owned ReadPart(MSN1) copy
	msn1Read, err := f.ring.ReadPart(0, 1, 0)
	if err != nil {
		t.Fatalf("ReadPart MSN 1: %v", err)
	}
	if !bytes.Equal(msn1Read, msn1Data) {
		t.Fatalf("ReadPart MSN 1 mismatch with msn1Data")
	}
	msn1Copy := bytes.Clone(msn1Read)

	// Drop closed/outside-window MSN1 at monotonic8s
	if err := f.ring.DropSegment(0, 1, 8_000_000_000); err != nil {
		t.Fatalf("DropSegment MSN 1 at monotonic8s: %v", err)
	}

	// Reserve must ErrFull while dropped, including before grace
	if _, err := f.ring.Reserve(ipc.KindPart, 0, 64); !errors.Is(err, ErrFull) {
		t.Fatalf("Reserve while MSN 1 dropped got %v, want ErrFull", err)
	}

	// Inspect deadline from snapshot
	snapBeforeRetire, err := f.ring.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot before retire: %v", err)
	}
	seg1Deadline := snapBeforeRetire.Tracks[0].Segments[0].DeadlineNS

	// Early retire at deadline-1 ErrNotReady
	if err := f.ring.RetireSegment(0, 1, seg1Deadline-1); !errors.Is(err, ErrNotReady) {
		t.Fatalf("RetireSegment at deadline-1 got %v, want ErrNotReady", err)
	}

	// Reserve must still ErrFull before retirement
	if _, err := f.ring.Reserve(ipc.KindPart, 0, 64); !errors.Is(err, ErrFull) {
		t.Fatalf("Reserve before deadline got %v, want ErrFull", err)
	}

	// Retire exactly deadline
	if err := f.ring.RetireSegment(0, 1, seg1Deadline); err != nil {
		t.Fatalf("RetireSegment at deadline: %v", err)
	}

	// Reserve next part must reuse retired offset64
	resPending, err := f.ring.Reserve(ipc.KindPart, 0, 64)
	if err != nil {
		t.Fatalf("Reserve reuse: %v", err)
	}
	if resPending.Offset != 64 {
		t.Fatalf("Reserve reuse offset = %d, want 64", resPending.Offset)
	}

	// 2. Writer owns only this pending grant; never commits it.
	// Reader checks pinned init and retained MSN2 bytes/digests, pending reservation occupancy,
	// ReadPart(MSN6,0)==ErrNotFound. Use 20 explicit write->read->ack rounds.
	ctx := newConcurrentTestContext()
	errCh := make(chan error, 50)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		case <-ctx.Done():
		}
	}

	writerWroteCh := make(chan struct{})
	writerAckCh := make(chan struct{})
	readerReqCh := make(chan struct{})
	readerRespCh := make(chan struct{})
	closeBarrier := make(chan struct{})

	var writerWg sync.WaitGroup
	var readerWg sync.WaitGroup

	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for round := 0; round < 20; round++ {
			writeData := bytes.Repeat([]byte{byte(0x50 + round)}, int(resPending.Length))
			if err := pwriteErr(f.dups[0], writeData, resPending.Offset); err != nil {
				reportErr(fmt.Errorf("writer pwrite round %d: %w", round, err))
				return
			}
			select {
			case <-ctx.Done():
				return
			case writerWroteCh <- struct{}{}:
			}
			select {
			case <-ctx.Done():
				return
			case <-writerAckCh:
			}
		}
	}()

	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-readerReqCh:
				// 1. Pinned init check
				bInit, err := f.ring.ReadInit(0)
				if err != nil {
					reportErr(fmt.Errorf("reader ReadInit: %w", err))
					return
				}
				if !bytes.Equal(bInit, initData) || testSHA(bInit) != initEntry.SHA256 {
					reportErr(errors.New("reader ReadInit corrupted or digest mismatch"))
					return
				}

				// 2. Retained MSN 2 check
				bMsn2, err := f.ring.ReadPart(0, 2, 0)
				if err != nil {
					reportErr(fmt.Errorf("reader ReadPart MSN 2: %w", err))
					return
				}
				if !bytes.Equal(bMsn2, msn2Data) || testSHA(bMsn2) != testSHA(msn2Data) {
					reportErr(errors.New("reader ReadPart MSN 2 corrupted or digest mismatch"))
					return
				}

				// 3. Pending reservation occupancy check
				snap, err := f.ring.Snapshot()
				if err != nil {
					reportErr(fmt.Errorf("reader Snapshot: %w", err))
					return
				}
				if len(snap.Pending) != 1 {
					reportErr(fmt.Errorf("reader snap.Pending len=%d, want 1", len(snap.Pending)))
					return
				}
				if snap.Pending[0] != resPending {
					reportErr(fmt.Errorf("reader snap.Pending[0]=%+v, want %+v", snap.Pending[0], resPending))
					return
				}
				for _, trk := range snap.Tracks {
					for _, seg := range trk.Segments {
						for _, p := range seg.Parts {
							if p.Offset == resPending.Offset {
								reportErr(fmt.Errorf("pending offset %d exposed in published part %d of MSN %d", p.Offset, p.Index, seg.MSN))
								return
							}
						}
					}
				}

				// 4. ReadPart(MSN6, 0) == ErrNotFound
				if _, err := f.ring.ReadPart(0, 6, 0); !errors.Is(err, ErrNotFound) {
					reportErr(fmt.Errorf("reader ReadPart MSN 6 got %v, want ErrNotFound", err))
					return
				}

				select {
				case <-ctx.Done():
					return
				case readerRespCh <- struct{}{}:
				}

			case <-closeBarrier:
				// Reader vs Close barrier
				b, err := f.ring.ReadPart(0, 2, 0)
				if err != nil && !errors.Is(err, ErrClosed) {
					reportErr(fmt.Errorf("reader vs close got unexpected error: %w", err))
					return
				}
				if err == nil && !bytes.Equal(b, msn2Data) {
					reportErr(errors.New("reader vs close yielded malformed data"))
					return
				}
				return
			}
		}
	}()

	// 4. One context cancellation + both joins is registered immediately after goroutine creation
	// and before any possible Fatal. Ring/FD/store cleanup remains later in LIFO cleanup.
	t.Cleanup(func() {
		ctx.Cancel()
		writerWg.Wait()
		readerWg.Wait()
	})

	// 20 explicit write -> read -> ack rounds
	for round := 0; round < 20; round++ {
		select {
		case <-ctx.Done():
			t.Fatal("coordinator context canceled")
		case <-writerWroteCh:
		case err := <-errCh:
			t.Fatalf("error before round %d write: %v", round, err)
		}

		select {
		case <-ctx.Done():
			t.Fatal("coordinator context canceled")
		case readerReqCh <- struct{}{}:
		case err := <-errCh:
			t.Fatalf("error requesting read round %d: %v", round, err)
		}

		select {
		case <-ctx.Done():
			t.Fatal("coordinator context canceled")
		case <-readerRespCh:
		case err := <-errCh:
			t.Fatalf("error in reader round %d: %v", round, err)
		}

		select {
		case <-ctx.Done():
			t.Fatal("coordinator context canceled")
		case writerAckCh <- struct{}{}:
		case err := <-errCh:
			t.Fatalf("error acking writer round %d: %v", round, err)
		}
	}

	// 3. After writer finishes, join it BEFORE CancelAfterQuiescence.
	writerWg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("writer failed: %v", err)
	default:
	}

	if err := f.ring.CancelAfterQuiescence(resPending); err != nil {
		t.Fatalf("CancelAfterQuiescence: %v", err)
	}

	// Exercise reader vs Close with one simultaneous-start barrier
	close(closeBarrier)
	if err := f.ring.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	readerWg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("reader vs close failed: %v", err)
	default:
	}

	// After both complete all public operations closed
	if _, err := f.ring.Reserve(ipc.KindPart, 0, 64); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reserve after Close got %v, want ErrClosed", err)
	}
	if err := f.ring.Commit(Reservation{}, ipc.Entry{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Commit after Close got %v, want ErrClosed", err)
	}
	if err := f.ring.Apply(ipc.Entry{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Apply after Close got %v, want ErrClosed", err)
	}
	if err := f.ring.CancelAfterQuiescence(Reservation{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("CancelAfterQuiescence after Close got %v, want ErrClosed", err)
	}
	if err := f.ring.DropSegment(0, 2, 20_000_000_000); !errors.Is(err, ErrClosed) {
		t.Fatalf("DropSegment after Close got %v, want ErrClosed", err)
	}
	if err := f.ring.RetireSegment(0, 2, 20_000_000_000); !errors.Is(err, ErrClosed) {
		t.Fatalf("RetireSegment after Close got %v, want ErrClosed", err)
	}
	if _, err := f.ring.ReadInit(0); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadInit after Close got %v, want ErrClosed", err)
	}
	if _, err := f.ring.ReadPart(0, 2, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadPart after Close got %v, want ErrClosed", err)
	}
	if _, err := f.ring.ReadSegment(0, 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadSegment after Close got %v, want ErrClosed", err)
	}
	if _, err := f.ring.Snapshot(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Snapshot after Close got %v, want ErrClosed", err)
	}

	// Previously owned MSN1 copy must remain unchanged despite reuse and Close
	if !bytes.Equal(msn1Copy, msn1Data) {
		t.Fatalf("owned MSN1 copy modified: got %x, want %x", msn1Copy, msn1Data)
	}
}

func TestDeliberateStrayWriteDetection(t *testing.T) {
	f := newTestFixture(t, ipc.ModeSolo, ipc.CodecH264, 1024, Policy{})

	// Reserve and commit valid init
	resInit, err := f.ring.Reserve(ipc.KindInit, 0, 64)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	initData := bytes.Repeat([]byte{0x5a}, 64)
	mustPwrite(t, f.dups[0], initData, 0)
	initEntry := ipc.Entry{
		Version: ipc.Version, Run: f.run, Source: f.source, Epoch: f.epoch, Seq: 1,
		Kind: ipc.KindInit, Track: 0, PublishedNS: 100, PublishedMonoNS: 100,
		Offset: 0, Length: 64, SHA256: testSHA(initData), Codecs: "avc1.64001f",
	}
	if err := f.ring.Commit(resInit, initEntry); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify reader can read it
	b, err := f.ring.ReadInit(0)
	if err != nil || !bytes.Equal(b, initData) {
		t.Fatalf("ReadInit: %v", err)
	}

	// Deliberate stray write: corrupt committed bytes via the duplicated writable descriptor
	corruptData := bytes.Repeat([]byte{0xff}, 16)
	mustPwrite(t, f.dups[0], corruptData, 10)

	// Reader reads corrupt bytes: raw memfd does not prevent the write,
	// but reader detects the corruption via SHA digest check and returns nil, ErrInvalidPublication
	bAfter, errAfter := f.ring.ReadInit(0)
	if bAfter != nil {
		t.Fatalf("expected nil result on digest mismatch, got %d bytes", len(bAfter))
	}
	if !errors.Is(errAfter, ErrInvalidPublication) {
		t.Fatalf("expected ErrInvalidPublication on stray write detection, got %v", errAfter)
	}
}
