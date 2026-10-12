//go:build linux

package supervisor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
	"github.com/connectsapp/connects-egress/internal/worker"
)

func TestPublicationHealthyMatrix(t *testing.T) {
	matrixCases := []struct {
		name        string
		mode        string
		codec       string
		tracks      int
		audioTracks int
	}{
		{"solo h264", ipc.ModeSolo, ipc.CodecH264, ipc.TracksSolo, 1},
		{"solo hevc", ipc.ModeSolo, ipc.CodecHEVC, ipc.TracksSolo, 1},
		{"1v1 h264", ipc.Mode1v1, ipc.CodecH264, ipc.Tracks1v1, 2},
		{"1v1 hevc", ipc.Mode1v1, ipc.CodecHEVC, ipc.Tracks1v1, 2},
	}

	for _, tc := range matrixCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := pubTestTempDir(t)
			owner := pubTestSelfOwner(t)

			var (
				mock      *pubTestMockFDStore
				s         *Source
				srv       *pubTestServer
				pub       *worker.Publisher
				pubCancel context.CancelFunc
				pubJoined chan struct{}
			)

			// Strict cleanup order: gates-release -> cancel/join publishers -> Publisher.Close -> server cancel/join -> Source.Close -> mock.Close
			cleanup := func() {
				if pubCancel != nil {
					pubCancel()
				}
				if pubJoined != nil {
					select {
					case <-pubJoined:
					case <-time.After(2 * time.Second):
						t.Errorf("cleanup: publisher goroutine failed to join")
					}
				}
				if pub != nil {
					_ = pub.Close()
				}
				if srv != nil {
					srv.Close()
				}
				if s != nil {
					_ = s.Close()
				}
				if mock != nil {
					mock.Close()
				}
			}
			defer cleanup()

			mock = pubTestStartMockNotifier(t, tc.tracks)

			n, err := systemd.NewNotifier(mock.Address())
			if err != nil {
				t.Fatalf("NewNotifier: %v", err)
			}

			cfg := ipc.Config{
				Mode:     tc.mode,
				Codec:    tc.codec,
				Tracks:   tc.tracks,
				Capacity: 4096,
			}
			pol := partstore.Policy{
				Window:     6 * time.Second,
				MaxSegment: 2 * time.Second,
			}

			opts := SourceOptions{
				Directory: dir,
				Run:       "run-matrix",
				Source:    "src-matrix",
				Epoch:     1,
				Config:    cfg,
				Policy:    pol,
				Owner:     owner,
			}

			s, err = CreateSource(context.Background(), opts, n)
			if err != nil {
				t.Fatalf("CreateSource: %v", err)
			}

			mock.WaitStored(2 * time.Second)

			srv = pubTestStartServer(t, s, dir)

			pub, err = worker.NewPublisher(worker.PublisherOptions{
				Socket:        srv.Socket(),
				Run:           "run-matrix",
				Source:        "src-matrix",
				SupervisorUID: os.Geteuid(),
				Config:        cfg,
			})
			if err != nil {
				t.Fatalf("NewPublisher: %v", err)
			}

			// 1. Publish 16-byte deterministic init each track with bounded 2s context
			videoCodec := "avc1.64001f"
			if tc.codec == ipc.CodecHEVC {
				videoCodec = "hvc1.1.6.L93.B0"
			}
			audioCodec := "mp4a.40.2"

			initPayloads := make([][]byte, tc.tracks)
			for trk := 0; trk < tc.tracks; trk++ {
				cStr := videoCodec
				if trk >= ipc.TrackAudioA {
					cStr = audioCodec
				}
				initData := bytes.Repeat([]byte{byte(0x10 + trk)}, 16)
				initPayloads[trk] = initData

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := pub.Publish(ctx, ipc.Entry{
					Kind:   ipc.KindInit,
					Track:  trk,
					Codecs: cStr,
				}, initData)
				cancel()
				if err != nil {
					t.Fatalf("Publish init track %d: %v", trk, err)
				}
			}

			// 2. Phases P=A=0 for each audio track with bounded 2s context
			for audioTrk := ipc.TrackAudioA; audioTrk < tc.tracks; audioTrk++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := pub.Publish(ctx, ipc.Entry{
					Kind:       ipc.KindPhases,
					Track:      audioTrk,
					PhaseVideo: 0,
					PhaseAudio: 0,
				}, nil)
				cancel()
				if err != nil {
					t.Fatalf("Publish phases track %d: %v", audioTrk, err)
				}
			}

			// 3. One 64-byte part each with bounded 2s context
			partPayloads := make([][]byte, tc.tracks)
			for trk := 0; trk < tc.tracks; trk++ {
				isVideo := trk < ipc.TrackAudioA
				duration := int64(320000000)
				payload := 15
				independent := false
				if isVideo {
					duration = 333333333
					payload = 10
					independent = true
				}
				partData := bytes.Repeat([]byte{byte(0x40 + trk)}, 64)
				partPayloads[trk] = partData

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := pub.Publish(ctx, ipc.Entry{
					Kind:          ipc.KindPart,
					Track:         trk,
					MSN:           1,
					Index:         0,
					Sequence:      1,
					FirstAU:       0,
					PTS:           0,
					DTS:           0,
					Duration:      duration,
					Payload:       payload,
					FragmentStart: true,
					Independent:   independent,
				}, partData)
				cancel()
				if err != nil {
					t.Fatalf("Publish part track %d: %v", trk, err)
				}
			}

			// 4. Verified terminal with bounded 2s context
			ctxTerm, cancelTerm := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = pub.Publish(ctxTerm, ipc.Entry{
				Kind:     ipc.KindTerminal,
				Track:    ipc.TrackTerminal,
				Verified: true,
				Reason:   "verified terminal finish",
			}, nil)
			cancelTerm()
			if err != nil {
				t.Fatalf("Publish terminal: %v", err)
			}

			// 5. Compare Source ReadInit/ReadPart/ReadSegment bytes
			for trk := 0; trk < tc.tracks; trk++ {
				gotInit, err := s.ReadInit(trk)
				if err != nil {
					t.Fatalf("ReadInit track %d: %v", trk, err)
				}
				if !bytes.Equal(gotInit, initPayloads[trk]) {
					t.Fatalf("ReadInit track %d mismatch: got %x, want %x", trk, gotInit, initPayloads[trk])
				}

				gotPart, err := s.ReadPart(trk, 1, 0)
				if err != nil {
					t.Fatalf("ReadPart track %d: %v", trk, err)
				}
				if !bytes.Equal(gotPart, partPayloads[trk]) {
					t.Fatalf("ReadPart track %d mismatch: got %x, want %x", trk, gotPart, partPayloads[trk])
				}

				gotSeg, err := s.ReadSegment(trk, 1)
				if err != nil {
					t.Fatalf("ReadSegment track %d: %v", trk, err)
				}
				if !bytes.Equal(gotSeg, partPayloads[trk]) {
					t.Fatalf("ReadSegment track %d mismatch: got %x, want %x", trk, gotSeg, partPayloads[trk])
				}
			}

			// 6. Check snapshot high waters, pending, and capture original descriptors
			snap, err := s.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if len(snap.Pending) != 0 {
				t.Fatalf("snap.Pending not empty: %+v", snap.Pending)
			}
			wantEntryHigh := uint64(2*tc.tracks + tc.audioTracks + 1)
			wantRecordHigh := uint64(4*tc.tracks + tc.audioTracks + 1)
			if snap.EntryHigh != wantEntryHigh {
				t.Fatalf("EntryHigh = %d, want %d", snap.EntryHigh, wantEntryHigh)
			}
			if snap.RecordHigh != wantRecordHigh {
				t.Fatalf("RecordHigh = %d, want %d", snap.RecordHigh, wantRecordHigh)
			}

			origIDs := append([]ipc.FDIdentity(nil), s.ids...)

			// 7. Close publisher and source, then OpenSource from original mock-held duplicates
			if err := pub.Close(); err != nil {
				t.Fatalf("pub.Close: %v", err)
			}
			pub = nil
			if err := s.Close(); err != nil {
				t.Fatalf("s.Close: %v", err)
			}
			s = nil
			srv.Close()
			srv = nil

			heldFDs := mock.DupStored()
			s2, err := OpenSource(context.Background(), dir, heldFDs)
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			defer s2.Close()

			// Compare original descriptors
			reopenedIDs := append([]ipc.FDIdentity(nil), s2.ids...)
			if !reflect.DeepEqual(reopenedIDs, origIDs) {
				t.Fatalf("descriptor identities changed on reopen:\ngot:  %+v\nwant: %+v", reopenedIDs, origIDs)
			}

			// Compare complete snapshot after reopen
			snap2, err := s2.Snapshot()
			if err != nil {
				t.Fatalf("s2.Snapshot: %v", err)
			}
			if snap2.EntryHigh != snap.EntryHigh || snap2.RecordHigh != snap.RecordHigh {
				t.Fatalf("reopened snapshot high water mismatch: got entry=%d rec=%d, want entry=%d rec=%d",
					snap2.EntryHigh, snap2.RecordHigh, snap.EntryHigh, snap.RecordHigh)
			}
			if len(snap2.Pending) != 0 {
				t.Fatalf("reopened snap.Pending not empty: %+v", snap2.Pending)
			}
			if len(snap2.Tracks) != len(snap.Tracks) {
				t.Fatalf("reopened tracks count = %d, want %d", len(snap2.Tracks), len(snap.Tracks))
			}
			for i := range snap.Tracks {
				t1, t2 := snap.Tracks[i], snap2.Tracks[i]
				if t1.Track != t2.Track || t1.End != t2.End || len(t1.Segments) != len(t2.Segments) {
					t.Fatalf("reopened track %d mismatch: got %+v, want %+v", i, t2, t1)
				}
				if !reflect.DeepEqual(t1.Init, t2.Init) || !reflect.DeepEqual(t1.Phases, t2.Phases) {
					t.Fatalf("reopened track %d init/phases mismatch", i)
				}
			}

			for trk := 0; trk < tc.tracks; trk++ {
				gotInit2, err := s2.ReadInit(trk)
				if err != nil {
					t.Fatalf("s2.ReadInit track %d: %v", trk, err)
				}
				if !bytes.Equal(gotInit2, initPayloads[trk]) {
					t.Fatalf("s2.ReadInit track %d mismatch: got %x, want %x", trk, gotInit2, initPayloads[trk])
				}

				gotPart2, err := s2.ReadPart(trk, 1, 0)
				if err != nil {
					t.Fatalf("s2.ReadPart track %d: %v", trk, err)
				}
				if !bytes.Equal(gotPart2, partPayloads[trk]) {
					t.Fatalf("s2.ReadPart track %d mismatch: got %x, want %x", trk, gotPart2, partPayloads[trk])
				}

				gotSeg2, err := s2.ReadSegment(trk, 1)
				if err != nil {
					t.Fatalf("s2.ReadSegment track %d: %v", trk, err)
				}
				if !bytes.Equal(gotSeg2, partPayloads[trk]) {
					t.Fatalf("s2.ReadSegment track %d mismatch: got %x, want %x", trk, gotSeg2, partPayloads[trk])
				}
			}
		})
	}
}

func TestPublicationRetentionFullRetry(t *testing.T) {
	dir := pubTestTempDir(t)
	owner := pubTestSelfOwner(t)

	var (
		mock      *pubTestMockFDStore
		s         *Source
		srv       *pubTestServer
		pub       *worker.Publisher
		pubCancel context.CancelFunc
		pubJoined chan struct{}
	)

	cleanup := func() {
		if pubCancel != nil {
			pubCancel()
		}
		if pubJoined != nil {
			select {
			case <-pubJoined:
			case <-time.After(2 * time.Second):
				t.Errorf("cleanup: publisher goroutine failed to join")
			}
		}
		if pub != nil {
			_ = pub.Close()
		}
		if srv != nil {
			srv.Close()
		}
		if s != nil {
			_ = s.Close()
		}
		if mock != nil {
			mock.Close()
		}
	}
	defer cleanup()

	mock = pubTestStartMockNotifier(t, ipc.TracksSolo)

	n, err := systemd.NewNotifier(mock.Address())
	if err != nil {
		t.Fatalf("NewNotifier: %v", err)
	}

	cfg := ipc.Config{
		Mode:     ipc.ModeSolo,
		Codec:    ipc.CodecH264,
		Tracks:   ipc.TracksSolo,
		Capacity: 256,
	}
	pol := partstore.Policy{
		Window:     1 * time.Second,
		MaxSegment: 1 * time.Second,
	}

	opts := SourceOptions{
		Directory: dir,
		Run:       "run-retention",
		Source:    "src-retention",
		Epoch:     1,
		Config:    cfg,
		Policy:    pol,
		Owner:     owner,
	}

	s, err = CreateSource(context.Background(), opts, n)
	if err != nil {
		t.Fatalf("CreateSource: %v", err)
	}

	mock.WaitStored(2 * time.Second)

	srv = pubTestStartServer(t, s, dir)

	pub, err = worker.NewPublisher(worker.PublisherOptions{
		Socket:        srv.Socket(),
		Run:           "run-retention",
		Source:        "src-retention",
		SupervisorUID: os.Geteuid(),
		Config:        cfg,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	// 1. Init: 16 bytes on video-hi (track 0) with bounded 2s context
	initData := bytes.Repeat([]byte{0xaa}, 16)
	ctxInit, cancelInit := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = pub.Publish(ctxInit, ipc.Entry{
		Kind:   ipc.KindInit,
		Track:  0,
		Codecs: "avc1.64001f",
	}, initData)
	cancelInit()
	if err != nil {
		t.Fatalf("Publish init: %v", err)
	}

	// 2. Three 64-byte video-hi parts (MSN 1..3, one part/segment, PTS 0, 1s, 2s) with bounded 2s context
	part1Data := bytes.Repeat([]byte{0x11}, 64)
	ctxP1, cancelP1 := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = pub.Publish(ctxP1, ipc.Entry{
		Kind:            ipc.KindPart,
		Track:           0,
		MSN:             1,
		Index:           0,
		Sequence:        1,
		FirstAU:         0,
		PTS:             0,
		DTS:             0,
		Duration:        1_000_000_000,
		Payload:         1,
		FragmentStart:   true,
		Independent:     true,
		PublishedMonoNS: 1_000_000_000,
	}, part1Data)
	cancelP1()
	if err != nil {
		t.Fatalf("Publish part 1: %v", err)
	}

	part2Data := bytes.Repeat([]byte{0x22}, 64)
	ctxP2, cancelP2 := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = pub.Publish(ctxP2, ipc.Entry{
		Kind:            ipc.KindPart,
		Track:           0,
		MSN:             2,
		Index:           0,
		Sequence:        2,
		FirstAU:         1,
		PTS:             1_000_000_000,
		DTS:             1_000_000_000,
		Duration:        1_000_000_000,
		Payload:         1,
		FragmentStart:   true,
		Independent:     true,
		PublishedMonoNS: 2_000_000_000,
	}, part2Data)
	cancelP2()
	if err != nil {
		t.Fatalf("Publish part 2: %v", err)
	}

	part3Data := bytes.Repeat([]byte{0x33}, 64)
	ctxP3, cancelP3 := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = pub.Publish(ctxP3, ipc.Entry{
		Kind:            ipc.KindPart,
		Track:           0,
		MSN:             3,
		Index:           0,
		Sequence:        3,
		FirstAU:         2,
		PTS:             2_000_000_000,
		DTS:             2_000_000_000,
		Duration:        1_000_000_000,
		Payload:         1,
		FragmentStart:   true,
		Independent:     true,
		PublishedMonoNS: 3_000_000_000,
	}, part3Data)
	cancelP3()
	if err != nil {
		t.Fatalf("Publish part 3: %v", err)
	}

	// 3. Fourth Publish (MSN 4, 64 bytes) waits on full
	part4Data := bytes.Repeat([]byte{0x44}, 64)
	part4Entry := ipc.Entry{
		Kind:            ipc.KindPart,
		Track:           0,
		MSN:             4,
		Index:           0,
		Sequence:        4,
		FirstAU:         3,
		PTS:             3_000_000_000,
		DTS:             3_000_000_000,
		Duration:        1_000_000_000,
		Payload:         1,
		FragmentStart:   true,
		Independent:     true,
		PublishedMonoNS: 4_000_000_000,
	}

	type fourthRes struct {
		entry ipc.Entry
		err   error
	}
	fourthCh := make(chan fourthRes, 1)
	fourthCtx, cancelFourth := context.WithTimeout(context.Background(), 5*time.Second)
	pubCancel = cancelFourth
	pubJoined = make(chan struct{})

	go func() {
		defer close(pubJoined)
		e, err := pub.Publish(fourthCtx, part4Entry, part4Data)
		fourthCh <- fourthRes{entry: e, err: err}
	}()

	// Verify fourth publish waits on full
	select {
	case res := <-fourthCh:
		t.Fatalf("fourth publish returned early unexpectedly: %+v", res)
	case <-time.After(150 * time.Millisecond):
		// Expected: waiter blocked
	}

	// 4. Maintain at synthetic mono 10s: drops 1/2 but cannot reuse; dropped bytes still readable
	if err := s.Maintain(10_000_000_000); err != nil {
		t.Fatalf("Maintain(10s): %v", err)
	}

	// Dropped bytes still readable
	p1, err := s.ReadPart(0, 1, 0)
	if err != nil || !bytes.Equal(p1, part1Data) {
		t.Fatalf("ReadPart(1) after drop: %v, got %x, want %x", err, p1, part1Data)
	}
	p2, err := s.ReadPart(0, 2, 0)
	if err != nil || !bytes.Equal(p2, part2Data) {
		t.Fatalf("ReadPart(2) after drop: %v, got %x, want %x", err, p2, part2Data)
	}

	// Latest 3 and init immutable
	p3, err := s.ReadPart(0, 3, 0)
	if err != nil || !bytes.Equal(p3, part3Data) {
		t.Fatalf("ReadPart(3) after drop: %v, got %x, want %x", err, p3, part3Data)
	}
	pi, err := s.ReadInit(0)
	if err != nil || !bytes.Equal(pi, initData) {
		t.Fatalf("ReadInit after drop: %v, got %x, want %x", err, pi, initData)
	}

	// Waiter is still waiting because dropped bytes are not yet retired
	select {
	case res := <-fourthCh:
		t.Fatalf("fourth publish returned after drop (before retire): %+v", res)
	case <-time.After(150 * time.Millisecond):
		// Expected: waiter still blocked
	}

	// 5. Maintain at synthetic mono 14s: retires 1/2; waiter completes same seq
	if err := s.Maintain(14_000_000_000); err != nil {
		t.Fatalf("Maintain(14s): %v", err)
	}

	// Waiter completes
	var fourthOut fourthRes
	select {
	case fourthOut = <-fourthCh:
		if fourthOut.err != nil {
			t.Fatalf("fourth publish failed after retirement: %v", fourthOut.err)
		}
		if fourthOut.entry.Seq != 5 {
			t.Fatalf("fourth publish seq = %d, want 5", fourthOut.entry.Seq)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("fourth publish failed to unblock within 2s after retirement")
	}

	// Retired reads return NotFound
	if _, err := s.ReadPart(0, 1, 0); !errors.Is(err, partstore.ErrNotFound) {
		t.Fatalf("ReadPart(1) after retire = %v, want ErrNotFound", err)
	}
	if _, err := s.ReadPart(0, 2, 0); !errors.Is(err, partstore.ErrNotFound) {
		t.Fatalf("ReadPart(2) after retire = %v, want ErrNotFound", err)
	}

	// Latest 3, 4 and init intact
	p3After, err := s.ReadPart(0, 3, 0)
	if err != nil || !bytes.Equal(p3After, part3Data) {
		t.Fatalf("ReadPart(3) after retire: %v, got %x, want %x", err, p3After, part3Data)
	}
	p4After, err := s.ReadPart(0, 4, 0)
	if err != nil || !bytes.Equal(p4After, part4Data) {
		t.Fatalf("ReadPart(4) after retire: %v, got %x, want %x", err, p4After, part4Data)
	}
	piAfter, err := s.ReadInit(0)
	if err != nil || !bytes.Equal(piAfter, initData) {
		t.Fatalf("ReadInit after retire: %v, got %x, want %x", err, piAfter, initData)
	}

	t.Logf("retention scope: synthetic mono 10s dropped MSN 1-2; synthetic mono 14s retired MSN 1-2; unblocked MSN 4 at seq %d", fourthOut.entry.Seq)
}

func TestPublicationCanceledReservation(t *testing.T) {
	dir := pubTestTempDir(t)
	owner := pubTestSelfOwner(t)

	var (
		mock        *pubTestMockFDStore
		s           *Source
		srv         *pubTestServer
		pub         *worker.Publisher
		pubCancel   context.CancelFunc
		pubJoined   chan struct{}
		hookRelease = make(chan struct{})
		releaseOnce sync.Once
	)

	releaseGate := func() {
		releaseOnce.Do(func() {
			close(hookRelease)
		})
	}

	cleanup := func() {
		// 1. gates-release
		releaseGate()
		// 2. cancel/join publishers
		if pubCancel != nil {
			pubCancel()
		}
		if pubJoined != nil {
			select {
			case <-pubJoined:
			case <-time.After(2 * time.Second):
				t.Errorf("cleanup: publisher goroutine failed to join")
			}
		}
		// 3. Publisher.Close
		if pub != nil {
			_ = pub.Close()
		}
		// 4. server cancel/join
		if srv != nil {
			srv.Close()
		}
		// 5. Source.Close
		if s != nil {
			_ = s.Close()
		}
		// 6. mock.Close
		if mock != nil {
			mock.Close()
		}
	}
	defer cleanup()

	mock = pubTestStartMockNotifier(t, ipc.TracksSolo)

	n, err := systemd.NewNotifier(mock.Address())
	if err != nil {
		t.Fatalf("NewNotifier: %v", err)
	}

	cfg := ipc.Config{
		Mode:     ipc.ModeSolo,
		Codec:    ipc.CodecH264,
		Tracks:   ipc.TracksSolo,
		Capacity: 4096,
	}
	pol := partstore.Policy{
		Window:     6 * time.Second,
		MaxSegment: 2 * time.Second,
	}

	opts := SourceOptions{
		Directory: dir,
		Run:       "run-cancel",
		Source:    "src-cancel",
		Epoch:     1,
		Config:    cfg,
		Policy:    pol,
		Owner:     owner,
	}

	s, err = CreateSource(context.Background(), opts, n)
	if err != nil {
		t.Fatalf("CreateSource: %v", err)
	}

	mock.WaitStored(2 * time.Second)

	hookReached := make(chan struct{})
	var hookOnce sync.Once

	s.hook = func(pt string) {
		if pt == "reserve_durable" {
			hookOnce.Do(func() {
				close(hookReached)
			})
			<-hookRelease
		}
	}

	srv = pubTestStartServer(t, s, dir)

	pub, err = worker.NewPublisher(worker.PublisherOptions{
		Socket:        srv.Socket(),
		Run:           "run-cancel",
		Source:        "src-cancel",
		SupervisorUID: os.Geteuid(),
		Config:        cfg,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	pubCtx, pubCancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
	pubCancel = pubCancelFunc
	pubJoined = make(chan struct{})

	type pubRes struct {
		entry ipc.Entry
		err   error
	}
	pubCh := make(chan pubRes, 1)

	go func() {
		defer close(pubJoined)
		e, err := pub.Publish(pubCtx, ipc.Entry{
			Kind:   ipc.KindInit,
			Track:  0,
			Codecs: "avc1.64001f",
		}, bytes.Repeat([]byte{0x11}, 16))
		pubCh <- pubRes{entry: e, err: err}
	}()

	// Wait for reserve_durable hook to be gated
	select {
	case <-hookReached:
	case <-time.After(2 * time.Second):
		t.Fatalf("reserve_durable hook not reached within 2s")
	}

	// Cancel Publish while gated
	pubCancelFunc()

	// Cancellation returns fatal within 2s
	select {
	case res := <-pubCh:
		if !errors.Is(res.err, context.Canceled) && !errors.Is(res.err, worker.ErrFatal) {
			t.Fatalf("Publish error = %v, want context.Canceled or ErrFatal", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Publish did not abort within 2s of cancellation")
	}

	// Release hook and join
	releaseGate()

	// Gated pending init must remain ReadInit=ErrNotFound
	if _, err := s.ReadInit(0); !errors.Is(err, partstore.ErrNotFound) {
		t.Fatalf("ReadInit(0) while grant pending = %v, want ErrNotFound", err)
	}

	// Pending reservation remains in supervisor snapshot
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Pending) != 1 {
		t.Fatalf("Snapshot pending grants = %d, want 1", len(snap.Pending))
	}

	// Future Publish is fatal
	ctxFuture, cancelFuture := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFuture()
	_, err = pub.Publish(ctxFuture, ipc.Entry{
		Kind:   ipc.KindInit,
		Track:  0,
		Codecs: "avc1.64001f",
	}, bytes.Repeat([]byte{0x11}, 16))
	if !errors.Is(err, worker.ErrFatal) {
		t.Fatalf("future Publish = %v, want ErrFatal", err)
	}
}
