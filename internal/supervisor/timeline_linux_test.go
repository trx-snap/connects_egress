//go:build linux

package supervisor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
	"github.com/connectsapp/connects-egress/internal/worker"
)

func TestLoadPresentation(t *testing.T) {
	srcDir := pubTestTempDir(t)
	owner := pubTestSelfOwner(t)

	var (
		s    *Source
		mock *pubTestMockFDStore
	)
	defer func() {
		if s != nil {
			_ = s.Close()
		}
		if mock != nil {
			mock.Close()
		}
	}()

	mock = pubTestStartMockNotifier(t, ipc.Tracks1v1)
	n, err := systemd.NewNotifier(mock.Address())
	if err != nil {
		t.Fatalf("NewNotifier: %v", err)
	}

	cfg := ipc.Config{
		Mode:     ipc.Mode1v1,
		Codec:    ipc.CodecH264,
		Tracks:   ipc.Tracks1v1,
		Capacity: 4096,
	}
	pol := partstore.Policy{
		Window:     6 * time.Second,
		MaxSegment: 2 * time.Second,
	}
	opts := SourceOptions{
		Directory: srcDir,
		Run:       "run-load-pres",
		Source:    "src-load-pres",
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

	// Before presentation.json exists, LoadPresentation(src) must return an error matching ErrTimelineNotReady.
	_, err = LoadPresentation(s)
	if !errors.Is(err, ErrTimelineNotReady) {
		t.Fatalf("LoadPresentation before write: got %v, want ErrTimelineNotReady", err)
	}

	// A canonical, valid presentation matching SourceInfo must load and compare exactly.
	info := s.Info()
	clocks := make([]ipc.TrackClock, info.Config.Tracks)
	for i := range clocks {
		clocks[i] = ipc.TrackClock{
			Track:               i,
			RawPTSOriginNS:      0,
			RunningTimeOriginNS: 0,
		}
	}
	pres := ipc.Presentation{
		Version:       ipc.PresentationVersion,
		Run:           info.Run,
		Source:        info.Source,
		Epoch:         info.Epoch,
		Config:        info.Config,
		PipelineUTCNS: 1_700_000_000_000_000_000,
		UncertaintyNS: 10_000_000,
		CropAnchor:    0.4,
		Clocks:        clocks,
		Slots: []ipc.PresentationSlot{
			{HostID: "host_a", RoomID: "room_a"},
			{HostID: "host_b", RoomID: "room_b"},
		},
	}

	if err := worker.WritePresentation(info.Directory, pres); err != nil {
		t.Fatalf("WritePresentation: %v", err)
	}

	got, err := LoadPresentation(s)
	if err != nil {
		t.Fatalf("LoadPresentation: %v", err)
	}
	if !reflect.DeepEqual(got, pres) {
		t.Fatalf("LoadPresentation got %+v, want %+v", got, pres)
	}

	// A canonical presentation with mismatched Run or Config must be rejected with ErrTimelineInvalid.
	presPath := filepath.Join(info.Directory, presentationFile)

	badRunPres := pres
	badRunPres.Run = "other-run"
	badRunData, err := ipc.EncodePresentation(badRunPres)
	if err != nil {
		t.Fatalf("EncodePresentation bad run: %v", err)
	}
	if err := os.WriteFile(presPath, badRunData, 0600); err != nil {
		t.Fatalf("write bad run presentation: %v", err)
	}
	if _, err := LoadPresentation(s); !errors.Is(err, ErrTimelineInvalid) {
		t.Fatalf("LoadPresentation mismatched Run: got %v, want ErrTimelineInvalid", err)
	}

	badCfgPres := pres
	badCfgPres.Config.Codec = ipc.CodecHEVC
	badCfgData, err := ipc.EncodePresentation(badCfgPres)
	if err != nil {
		t.Fatalf("EncodePresentation bad config: %v", err)
	}
	if err := os.WriteFile(presPath, badCfgData, 0600); err != nil {
		t.Fatalf("write bad config presentation: %v", err)
	}
	if _, err := LoadPresentation(s); !errors.Is(err, ErrTimelineInvalid) {
		t.Fatalf("LoadPresentation mismatched Config: got %v, want ErrTimelineInvalid", err)
	}

	// A presentation.json changed to mode 0644 must be rejected with ErrTimelineInvalid.
	validData, err := ipc.EncodePresentation(pres)
	if err != nil {
		t.Fatalf("EncodePresentation valid: %v", err)
	}
	if err := os.WriteFile(presPath, validData, 0600); err != nil {
		t.Fatalf("restore valid presentation: %v", err)
	}
	if err := os.Chmod(presPath, 0644); err != nil {
		t.Fatalf("chmod 0644: %v", err)
	}
	if _, err := LoadPresentation(s); !errors.Is(err, ErrTimelineInvalid) {
		t.Fatalf("LoadPresentation mode 0644: got %v, want ErrTimelineInvalid", err)
	}
}

func TestTimelineBindObserveAndRecover(t *testing.T) {
	srcDir := pubTestTempDir(t)
	tlDir := pubTestTempDir(t)
	owner := pubTestSelfOwner(t)

	var (
		tl2  *Timeline
		tl   *Timeline
		pub  *worker.Publisher
		srv  *pubTestServer
		s    *Source
		mock *pubTestMockFDStore
	)
	defer func() {
		if tl2 != nil {
			_ = tl2.Close()
		}
		if tl != nil {
			_ = tl.Close()
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
	}()

	mock = pubTestStartMockNotifier(t, ipc.Tracks1v1)
	n, err := systemd.NewNotifier(mock.Address())
	if err != nil {
		t.Fatalf("NewNotifier: %v", err)
	}

	cfg := ipc.Config{
		Mode:     ipc.Mode1v1,
		Codec:    ipc.CodecH264,
		Tracks:   ipc.Tracks1v1,
		Capacity: 4096,
	}
	pol := partstore.Policy{
		Window:     6 * time.Second,
		MaxSegment: 2 * time.Second,
	}
	epochKey := EpochKey{
		Run:    "run-timeline-test",
		Source: "src-timeline-test",
		Epoch:  1,
	}
	opts := SourceOptions{
		Directory: srcDir,
		Run:       epochKey.Run,
		Source:    epochKey.Source,
		Epoch:     epochKey.Epoch,
		Config:    cfg,
		Policy:    pol,
		Owner:     owner,
	}

	s, err = CreateSource(context.Background(), opts, n)
	if err != nil {
		t.Fatalf("CreateSource: %v", err)
	}
	mock.WaitStored(2 * time.Second)

	info := s.Info()
	clocks := make([]ipc.TrackClock, info.Config.Tracks)
	for i := range clocks {
		clocks[i] = ipc.TrackClock{
			Track:               i,
			RawPTSOriginNS:      0,
			RunningTimeOriginNS: 0,
		}
	}
	pres := ipc.Presentation{
		Version:       ipc.PresentationVersion,
		Run:           info.Run,
		Source:        info.Source,
		Epoch:         info.Epoch,
		Config:        info.Config,
		PipelineUTCNS: 1_700_000_000_000_000_000,
		UncertaintyNS: 10_000_000,
		CropAnchor:    0.4,
		Clocks:        clocks,
		Slots: []ipc.PresentationSlot{
			{HostID: "host_a", RoomID: "room_a"},
			{HostID: "host_b", RoomID: "room_b"},
		},
	}
	if err := worker.WritePresentation(srcDir, pres); err != nil {
		t.Fatalf("WritePresentation: %v", err)
	}

	srv = pubTestStartServer(t, s, srcDir)

	pub, err = worker.NewPublisher(worker.PublisherOptions{
		Socket:        srv.Socket(),
		Run:           info.Run,
		Source:        info.Source,
		SupervisorUID: os.Geteuid(),
		Config:        cfg,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	// Publish init for tracks 0..4
	for trk := 0; trk < info.Config.Tracks; trk++ {
		cStr := "avc1.64001f"
		if trk >= ipc.TrackAudioA {
			cStr = "mp4a.40.2"
		}
		initData := bytes.Repeat([]byte{byte(0x20 + trk)}, 16)
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

	// Publish phases for audio tracks 3 and 4
	for audioTrk := ipc.TrackAudioA; audioTrk < info.Config.Tracks; audioTrk++ {
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

	// Publish exactly one valid first part on each track
	for trk := 0; trk < info.Config.Tracks; trk++ {
		isVideo := trk < ipc.TrackAudioA
		duration := int64(320000000)
		payload := 15
		independent := false
		if isVideo {
			duration = 333333333
			payload = 10
			independent = true
		}
		partData := bytes.Repeat([]byte{byte(0x50 + trk)}, 64)
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

	resolver := func(k EpochKey) (*Source, error) {
		if k == epochKey {
			return s, nil
		}
		return nil, ErrLostEpoch
	}

	// Open Timeline for room_b
	ctxOpen, cancelOpen := context.WithTimeout(context.Background(), 5*time.Second)
	tl, err = OpenTimeline(ctxOpen, TimelineOptions{
		Directory: tlDir,
		Room:      "room_b",
		Window:    DefaultRoomWindow,
	}, resolver)
	cancelOpen()
	if err != nil {
		t.Fatalf("OpenTimeline: %v", err)
	}

	// Observe before Bind
	snapBefore, chBefore, err := tl.Observe()
	if err != nil {
		t.Fatalf("Observe before bind: %v", err)
	}
	if snapBefore.Live != nil {
		t.Fatalf("snapBefore.Live != nil before Bind")
	}

	// Bind source epoch at source MSN 1
	if err := tl.Bind(epochKey, 1); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	// Assert the observed change channel closes
	select {
	case <-chBefore:
	case <-time.After(2 * time.Second):
		t.Fatalf("observed change channel did not close after Bind")
	}

	// Assert snapshot properties
	snapAfter, _, err := tl.Observe()
	if err != nil {
		t.Fatalf("Observe after bind: %v", err)
	}
	if snapAfter.NextMSN != 2 {
		t.Fatalf("snapAfter.NextMSN = %d, want 2", snapAfter.NextMSN)
	}
	if len(snapAfter.Segments) != 1 {
		t.Fatalf("len(snapAfter.Segments) = %d, want 1", len(snapAfter.Segments))
	}
	seg := snapAfter.Segments[0]
	if seg.MSN != 1 {
		t.Fatalf("seg.MSN = %d, want 1", seg.MSN)
	}
	if snapAfter.Live == nil {
		t.Fatalf("snapAfter.Live is nil")
	}
	if snapAfter.Live.AudioTrack != 4 {
		t.Fatalf("Live.AudioTrack = %d, want 4", snapAfter.Live.AudioTrack)
	}
	if seg.Tracks[3].Track != 4 {
		t.Fatalf("seg.Tracks[3].Track = %d, want 4", seg.Tracks[3].Track)
	}
	for i := 0; i < RoomTracks; i++ {
		rtk := seg.Tracks[i]
		if len(rtk.Parts) != 1 {
			t.Fatalf("track %d has %d parts, want 1", i, len(rtk.Parts))
		}
		expectedEnd := rtk.UTCNS + rtk.Parts[0].Duration
		if rtk.EndUTCNS != expectedEnd {
			t.Fatalf("track %d EndUTCNS = %d, want %d", i, rtk.EndUTCNS, expectedEnd)
		}
	}

	// Close and reopen the Timeline journal with a resolver returning the same Source.
	if err := tl.Close(); err != nil {
		t.Fatalf("tl.Close: %v", err)
	}

	ctxReopen, cancelReopen := context.WithTimeout(context.Background(), 5*time.Second)
	tl2, err = OpenTimeline(ctxReopen, TimelineOptions{
		Directory: tlDir,
		Room:      "room_b",
		Window:    DefaultRoomWindow,
	}, resolver)
	cancelReopen()
	if err != nil {
		t.Fatalf("OpenTimeline reopen: %v", err)
	}

	// Assert room MSN, track mapping, part counts and UTC/end values are unchanged.
	snapReopen, _, err := tl2.Observe()
	if err != nil {
		t.Fatalf("tl2.Observe: %v", err)
	}
	if snapReopen.NextMSN != snapAfter.NextMSN {
		t.Fatalf("snapReopen.NextMSN = %d, want %d", snapReopen.NextMSN, snapAfter.NextMSN)
	}
	if len(snapReopen.Segments) != len(snapAfter.Segments) {
		t.Fatalf("len(snapReopen.Segments) = %d, want %d", len(snapReopen.Segments), len(snapAfter.Segments))
	}
	seg2 := snapReopen.Segments[0]
	if seg2.MSN != seg.MSN {
		t.Fatalf("seg2.MSN = %d, want %d", seg2.MSN, seg.MSN)
	}
	if snapReopen.Live == nil || snapReopen.Live.AudioTrack != 4 {
		t.Fatalf("snapReopen.Live audio track = %v, want 4", snapReopen.Live)
	}
	for i := 0; i < RoomTracks; i++ {
		rtk := seg2.Tracks[i]
		orig := seg.Tracks[i]
		if rtk.Track != orig.Track {
			t.Fatalf("track %d source track = %d, want %d", i, rtk.Track, orig.Track)
		}
		if len(rtk.Parts) != len(orig.Parts) {
			t.Fatalf("track %d part count = %d, want %d", i, len(rtk.Parts), len(orig.Parts))
		}
		if rtk.UTCNS != orig.UTCNS {
			t.Fatalf("track %d UTCNS = %d, want %d", i, rtk.UTCNS, orig.UTCNS)
		}
		if rtk.EndUTCNS != orig.EndUTCNS {
			t.Fatalf("track %d EndUTCNS = %d, want %d", i, rtk.EndUTCNS, orig.EndUTCNS)
		}
		if rtk.DurationNS != orig.DurationNS {
			t.Fatalf("track %d DurationNS = %d, want %d", i, rtk.DurationNS, orig.DurationNS)
		}
		expectedEnd := rtk.UTCNS + rtk.Parts[0].Duration
		if rtk.EndUTCNS != expectedEnd {
			t.Fatalf("track %d EndUTCNS = %d, want %d", i, rtk.EndUTCNS, expectedEnd)
		}
	}
}

// TestRepinAdvancesLiveWindowRetentionFloor checks that an unfrozen live binding does not pin its
// StartMSN once that segment has rolled out of the room window: only window segments, grace
// references and a pending switch's start media hold Source bytes.
func TestRepinAdvancesLiveWindowRetentionFloor(t *testing.T) {
	newSrc := func() *Source {
		return &Source{cfg: ipc.Config{Tracks: ipc.TracksSolo}, pins: map[*RetentionPin]map[int]int{}}
	}
	wantFloors := func(step string, src *Source, want int) {
		t.Helper()
		for trk := 0; trk < ipc.TracksSolo; trk++ {
			if got := src.floor(trk); got != want {
				t.Errorf("%s: track %d floor = %d, want %d", step, trk, got, want)
			}
		}
	}

	src := newSrc()
	live := RoomBinding{Generation: 1, AudioTrack: ipc.TrackAudioA, StartMSN: 1, FirstRoomMSN: 1}
	tl := &Timeline{
		st: roomState{
			Bindings: []RoomBinding{live},
			Segments: []segState{
				{RoomMSN: 7, Generation: 1, SourceMSN: 7},
				{RoomMSN: 8, Generation: 1, SourceMSN: 8},
			},
		},
		gens: map[uint64]*genRuntime{1: {src: src, segs: map[uint64]RoomSegment{}}},
	}
	if err := tl.repin(); err != nil {
		t.Fatalf("repin (window only): %v", err)
	}
	wantFloors("window only", src, 7)

	// A grace reference below the window still lowers the floor.
	tl.st.Grace = []graceRef{{Generation: 1, Key: live.Key, SourceMSN: 5, ReleaseMonoNS: 1}}
	if err := tl.repin(); err != nil {
		t.Fatalf("repin (grace): %v", err)
	}
	wantFloors("grace", src, 5)

	// A pending switch pins the incoming generation at its StartMSN without touching the live one.
	src2 := newSrc()
	incoming := RoomBinding{Generation: 2, AudioTrack: ipc.TrackAudioA, StartMSN: 3}
	tl.st.Pending = &PendingSwitch{Incoming: incoming}
	tl.gens[2] = &genRuntime{src: src2, segs: map[uint64]RoomSegment{}}
	if err := tl.repin(); err != nil {
		t.Fatalf("repin (pending): %v", err)
	}
	wantFloors("pending incoming", src2, 3)
	wantFloors("live with pending", src, 5)

	// Once grace is released the live floor advances to the window again.
	tl.st.Grace = nil
	if err := tl.repin(); err != nil {
		t.Fatalf("repin (grace released): %v", err)
	}
	wantFloors("grace released", src, 7)
}
