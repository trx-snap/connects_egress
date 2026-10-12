//go:build linux

package supervisor

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
	"github.com/connectsapp/connects-egress/internal/systemd"
	"github.com/connectsapp/connects-egress/internal/worker"
)

type switchTestRig struct {
	t        *testing.T
	mock1    *pubTestMockFDStore
	mock2    *pubTestMockFDStore
	s1       *Source
	s2       *Source
	srv1     *pubTestServer
	srv2     *pubTestServer
	pub1     *worker.Publisher
	pub2     *worker.Publisher
	tl       *Timeline
	tl2      *Timeline
	key1     EpochKey
	key2     EpochKey
	srcDir1  string
	srcDir2  string
	tlDir    string
	resolver EpochResolver
	room     string
	baseUTC  int64
	cutUTC   int64
}

func (r *switchTestRig) teardown() {
	// Reverse ownership order:
	// 1. Timelines newest-first
	if r.tl2 != nil {
		_ = r.tl2.Close()
		r.tl2 = nil
	}
	if r.tl != nil {
		_ = r.tl.Close()
		r.tl = nil
	}
	// 2. Publishers
	if r.pub2 != nil {
		_ = r.pub2.Close()
		r.pub2 = nil
	}
	if r.pub1 != nil {
		_ = r.pub1.Close()
		r.pub1 = nil
	}
	// 3. Mock IPC servers
	if r.srv2 != nil {
		r.srv2.Close()
		r.srv2 = nil
	}
	if r.srv1 != nil {
		r.srv1.Close()
		r.srv1 = nil
	}
	// 4. Sources
	if r.s2 != nil {
		_ = r.s2.Close()
		r.s2 = nil
	}
	if r.s1 != nil {
		_ = r.s1.Close()
		r.s1 = nil
	}
	// 5. Mock notifiers
	if r.mock2 != nil {
		r.mock2.Close()
		r.mock2 = nil
	}
	if r.mock1 != nil {
		r.mock1.Close()
		r.mock1 = nil
	}
}

func newSwitchTestRig(t *testing.T) *switchTestRig {
	t.Helper()
	rig := &switchTestRig{
		t:       t,
		room:    "room_b",
		baseUTC: 1_700_000_000_000_000_000,
		cutUTC:  1_700_000_002_000_000_000,
		key1: EpochKey{
			Run:    "run-switch-out",
			Source: "src-switch-out",
			Epoch:  1,
		},
		key2: EpochKey{
			Run:    "run-switch-in",
			Source: "src-switch-in",
			Epoch:  1,
		},
		srcDir1: pubTestTempDir(t),
		srcDir2: pubTestTempDir(t),
		tlDir:   pubTestTempDir(t),
	}

	owner := pubTestSelfOwner(t)
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

	// 1. Outgoing source and mock notifier
	rig.mock1 = pubTestStartMockNotifier(t, ipc.Tracks1v1)
	n1, err := systemd.NewNotifier(rig.mock1.Address())
	if err != nil {
		rig.teardown()
		t.Fatalf("NewNotifier 1: %v", err)
	}
	opts1 := SourceOptions{
		Directory: rig.srcDir1,
		Run:       rig.key1.Run,
		Source:    rig.key1.Source,
		Epoch:     rig.key1.Epoch,
		Config:    cfg,
		Policy:    pol,
		Owner:     owner,
	}
	rig.s1, err = CreateSource(context.Background(), opts1, n1)
	if err != nil {
		rig.teardown()
		t.Fatalf("CreateSource 1: %v", err)
	}
	rig.mock1.WaitStored(2 * time.Second)

	// 2. Incoming source and mock notifier
	rig.mock2 = pubTestStartMockNotifier(t, ipc.Tracks1v1)
	n2, err := systemd.NewNotifier(rig.mock2.Address())
	if err != nil {
		rig.teardown()
		t.Fatalf("NewNotifier 2: %v", err)
	}
	opts2 := SourceOptions{
		Directory: rig.srcDir2,
		Run:       rig.key2.Run,
		Source:    rig.key2.Source,
		Epoch:     rig.key2.Epoch,
		Config:    cfg,
		Policy:    pol,
		Owner:     owner,
	}
	rig.s2, err = CreateSource(context.Background(), opts2, n2)
	if err != nil {
		rig.teardown()
		t.Fatalf("CreateSource 2: %v", err)
	}
	rig.mock2.WaitStored(2 * time.Second)

	// 3. Presentations with deterministic PipelineUTCNS and per-track clocks
	slots := []ipc.PresentationSlot{
		{HostID: "host_a", RoomID: "room_a"},
		{HostID: "host_b", RoomID: "room_b"},
	}
	clocks1 := make([]ipc.TrackClock, cfg.Tracks)
	clocks2 := make([]ipc.TrackClock, cfg.Tracks)
	for i := range clocks1 {
		clocks1[i] = ipc.TrackClock{Track: i, RawPTSOriginNS: 0, RunningTimeOriginNS: 0}
		clocks2[i] = ipc.TrackClock{Track: i, RawPTSOriginNS: 0, RunningTimeOriginNS: 0}
	}
	pres1 := ipc.Presentation{
		Version:       ipc.PresentationVersion,
		Run:           rig.key1.Run,
		Source:        rig.key1.Source,
		Epoch:         rig.key1.Epoch,
		Config:        cfg,
		PipelineUTCNS: rig.baseUTC,
		UncertaintyNS: 10_000_000,
		CropAnchor:    0.4,
		Clocks:        clocks1,
		Slots:         slots,
	}
	if err := worker.WritePresentation(rig.srcDir1, pres1); err != nil {
		rig.teardown()
		t.Fatalf("WritePresentation 1: %v", err)
	}

	pres2 := ipc.Presentation{
		Version:       ipc.PresentationVersion,
		Run:           rig.key2.Run,
		Source:        rig.key2.Source,
		Epoch:         rig.key2.Epoch,
		Config:        cfg,
		PipelineUTCNS: rig.cutUTC,
		UncertaintyNS: 10_000_000,
		CropAnchor:    0.4,
		Clocks:        clocks2,
		Slots:         slots,
	}
	if err := worker.WritePresentation(rig.srcDir2, pres2); err != nil {
		rig.teardown()
		t.Fatalf("WritePresentation 2: %v", err)
	}

	// 4. Mock IPC servers and worker publishers
	rig.srv1 = pubTestStartServer(t, rig.s1, rig.srcDir1)
	rig.pub1, err = worker.NewPublisher(worker.PublisherOptions{
		Socket:        rig.srv1.Socket(),
		Run:           rig.key1.Run,
		Source:        rig.key1.Source,
		SupervisorUID: os.Geteuid(),
		Config:        cfg,
	})
	if err != nil {
		rig.teardown()
		t.Fatalf("NewPublisher 1: %v", err)
	}

	rig.srv2 = pubTestStartServer(t, rig.s2, rig.srcDir2)
	rig.pub2, err = worker.NewPublisher(worker.PublisherOptions{
		Socket:        rig.srv2.Socket(),
		Run:           rig.key2.Run,
		Source:        rig.key2.Source,
		SupervisorUID: os.Geteuid(),
		Config:        cfg,
	})
	if err != nil {
		rig.teardown()
		t.Fatalf("NewPublisher 2: %v", err)
	}

	// 5. Inits and phases for both publishers
	switchPublishInitsAndPhases(t, rig.pub1, cfg.Tracks)
	switchPublishInitsAndPhases(t, rig.pub2, cfg.Tracks)

	// 6. Source resolver
	rig.resolver = func(k EpochKey) (*Source, error) {
		switch k {
		case rig.key1:
			return rig.s1, nil
		case rig.key2:
			return rig.s2, nil
		default:
			return nil, ErrLostEpoch
		}
	}

	return rig
}

func switchPublishInitsAndPhases(t *testing.T, pub *worker.Publisher, numTracks int) {
	t.Helper()
	for trk := 0; trk < numTracks; trk++ {
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
			t.Fatalf("publish init track %d: %v", trk, err)
		}
	}
	for audioTrk := ipc.TrackAudioA; audioTrk < numTracks; audioTrk++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := pub.Publish(ctx, ipc.Entry{
			Kind:       ipc.KindPhases,
			Track:      audioTrk,
			PhaseVideo: 0,
			PhaseAudio: 0,
		}, nil)
		cancel()
		if err != nil {
			t.Fatalf("publish phases track %d: %v", audioTrk, err)
		}
	}
}

func switchPublishPart(t *testing.T, pub *worker.Publisher, trk, msn, idx int, seq uint64, firstAU int, pts, dts, dur int64, payload int, fragStart, indep bool) {
	t.Helper()
	partData := bytes.Repeat([]byte{byte(0x50 + trk)}, 64)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := pub.Publish(ctx, ipc.Entry{
		Kind:          ipc.KindPart,
		Track:         trk,
		MSN:           msn,
		Index:         idx,
		Sequence:      seq,
		FirstAU:       firstAU,
		PTS:           pts,
		DTS:           dts,
		Duration:      dur,
		Payload:       payload,
		FragmentStart: fragStart,
		Independent:   indep,
	}, partData)
	if err != nil {
		t.Fatalf("switchPublishPart trk %d msn %d idx %d: %v", trk, msn, idx, err)
	}
}

func TestTimelineSwitchCommitHealthy(t *testing.T) {
	rig := newSwitchTestRig(t)
	defer rig.teardown()

	const partDur = int64(1_000_000_000) // 1 second per part

	// 1. Publish outgoing segment 1 part 0 for all tracks (ends at baseUTC + 1s)
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 1, 0, 1, 0, 0, 0, partDur, 10, true, true)
		} else {
			switchPublishPart(t, rig.pub1, trk, 1, 0, 1, 0, 0, 0, partDur, 15, true, false)
		}
	}

	// 2. Open Timeline and bind outgoing source at source MSN 1
	ctxOpen, cancelOpen := context.WithTimeout(context.Background(), 5*time.Second)
	var err error
	rig.tl, err = OpenTimeline(ctxOpen, TimelineOptions{
		Directory: rig.tlDir,
		Room:      rig.room,
		Window:    DefaultRoomWindow,
	}, rig.resolver)
	cancelOpen()
	if err != nil {
		t.Fatalf("OpenTimeline: %v", err)
	}

	if err := rig.tl.Bind(rig.key1, 1); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	snapBound, chBound, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe after bind: %v", err)
	}
	if snapBound.NextMSN != 2 {
		t.Fatalf("snapBound.NextMSN = %d, want 2", snapBound.NextMSN)
	}
	if len(snapBound.Segments) != 1 {
		t.Fatalf("len(snapBound.Segments) = %d, want 1", len(snapBound.Segments))
	}
	if snapBound.Live == nil || snapBound.Live.Key != rig.key1 {
		t.Fatalf("snapBound.Live = %v, want key1", snapBound.Live)
	}
	// Verify exposed ends are at baseUTC + 1s
	expectedOutEnd1 := rig.baseUTC + partDur
	for i := 0; i < RoomTracks; i++ {
		rtk := snapBound.Segments[0].Tracks[i]
		if rtk.EndUTCNS != expectedOutEnd1 {
			t.Fatalf("room track %d EndUTCNS = %d, want %d", i, rtk.EndUTCNS, expectedOutEnd1)
		}
	}

	// 3. PlanSwitch to incoming source at startMSN 1.
	// Cut UTC is derived as incoming anchor: rig.cutUTC = rig.baseUTC + 2s (after exposed end baseUTC + 1s).
	const planMonoNS = int64(1_000_000_000)
	if err := rig.tl.PlanSwitch(rig.key2, 1, planMonoNS); err != nil {
		t.Fatalf("PlanSwitch: %v", err)
	}

	snapPlanned, _, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe after PlanSwitch: %v", err)
	}
	if snapPlanned.Pending == nil {
		t.Fatalf("snapPlanned.Pending is nil")
	}
	if snapPlanned.Pending.CutUTCNS != rig.cutUTC {
		t.Fatalf("snapPlanned.Pending.CutUTCNS = %d, want %d", snapPlanned.Pending.CutUTCNS, rig.cutUTC)
	}
	if snapPlanned.Pending.HoldStartMonoNS != 0 {
		t.Fatalf("HoldStartMonoNS before cut = %d, want 0", snapPlanned.Pending.HoldStartMonoNS)
	}
	if snapPlanned.Live == nil || snapPlanned.Live.Key != rig.key1 {
		t.Fatalf("Live before cut = %v, want key1", snapPlanned.Live)
	}

	// 4. Publish outgoing parts through the cut:
	// - segment 1 part 1: ends at baseUTC + 2s == cutUTC
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 1, 1, 1, 10, partDur, partDur, partDur, 10, false, false)
		} else {
			switchPublishPart(t, rig.pub1, trk, 1, 1, 1, 15, partDur, partDur, partDur, 15, false, false)
		}
	}
	// - segment 2 part 0: ends at baseUTC + 3s > cutUTC (crosses through the cut)
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 2, 0, 2, 20, 2*partDur, 2*partDur, partDur, 10, true, true)
		} else {
			switchPublishPart(t, rig.pub1, trk, 2, 0, 2, 30, 2*partDur, 2*partDur, partDur, 15, true, false)
		}
	}

	// 5. Call Refresh with deterministic monotonic time; assert hold is recorded while outgoing remains live
	const holdMonoNS = int64(2_000_000_000)
	if err := rig.tl.Refresh(holdMonoNS); err != nil {
		t.Fatalf("Refresh hold: %v", err)
	}

	snapHold, chHold, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe hold: %v", err)
	}
	if snapHold.Pending == nil {
		t.Fatalf("snapHold.Pending is nil after Refresh through cut")
	}
	if snapHold.Pending.HoldStartMonoNS != holdMonoNS {
		t.Fatalf("snapHold.Pending.HoldStartMonoNS = %d, want %d", snapHold.Pending.HoldStartMonoNS, holdMonoNS)
	}
	if snapHold.Live == nil || snapHold.Live.Key != rig.key1 {
		t.Fatalf("snapHold.Live = %v, want key1 (outgoing remains live)", snapHold.Live)
	}
	if snapHold.Live.Frozen() {
		t.Fatalf("outgoing live generation must remain unfrozen during hold")
	}
	// Verify outgoing exposed ends on all four selected room tracks end at exact cutUTC
	if len(snapHold.Segments) != 1 {
		t.Fatalf("len(snapHold.Segments) = %d, want 1", len(snapHold.Segments))
	}
	for i := 0; i < RoomTracks; i++ {
		rtk := snapHold.Segments[0].Tracks[i]
		if rtk.EndUTCNS != rig.cutUTC {
			t.Fatalf("held outgoing room track %d EndUTCNS = %d, want %d (cutUTC)", i, rtk.EndUTCNS, rig.cutUTC)
		}
		if len(rtk.Parts) != 2 {
			t.Fatalf("held outgoing room track %d parts = %d, want 2", i, len(rtk.Parts))
		}
	}

	// 6. Publish incoming start-segment parts for three aligned video rungs (IDR/independent fragment start)
	// and the room's selected audio track at exact cut UTC.
	for vTrk := 0; vTrk < 3; vTrk++ {
		switchPublishPart(t, rig.pub2, vTrk, 1, 0, 1, 0, 0, 0, partDur, 10, true, true)
	}
	// Room 'room_b' is slot 1 -> AudioTrack is ipc.TrackAudioB (track 4)
	switchPublishPart(t, rig.pub2, ipc.TrackAudioB, 1, 0, 1, 0, 0, 0, partDur, 15, true, false)
	// Also publish audio track A for complete producer consistency
	switchPublishPart(t, rig.pub2, ipc.TrackAudioA, 1, 0, 1, 0, 0, 0, partDur, 15, true, false)

	// 7. Refresh to commit switch
	const commitMonoNS = holdMonoNS + 100_000_000
	if err := rig.tl.Refresh(commitMonoNS); err != nil {
		t.Fatalf("Refresh commit: %v", err)
	}

	// 8. Assertions:
	// - Observer change notification closes
	select {
	case <-chHold:
	default:
		t.Fatalf("observer change notification did not close on commit")
	}

	snapCommit, _, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe after commit: %v", err)
	}
	// - Pending is cleared
	if snapCommit.Pending != nil {
		t.Fatalf("snapCommit.Pending = %+v, want nil", snapCommit.Pending)
	}
	// - Incoming generation is live
	if snapCommit.Live == nil || snapCommit.Live.Key != rig.key2 {
		t.Fatalf("snapCommit.Live = %+v, want key2", snapCommit.Live)
	}
	// - Room MSN is continuous (outgoing was 1, incoming is 2, NextMSN is 3)
	if snapCommit.NextMSN != 3 {
		t.Fatalf("snapCommit.NextMSN = %d, want 3", snapCommit.NextMSN)
	}
	if len(snapCommit.Segments) != 2 {
		t.Fatalf("len(snapCommit.Segments) = %d, want 2", len(snapCommit.Segments))
	}
	seg2 := snapCommit.Segments[1]
	if seg2.MSN != 2 {
		t.Fatalf("seg2.MSN = %d, want 2", seg2.MSN)
	}
	if seg2.Generation != 2 {
		t.Fatalf("seg2.Generation = %d, want 2", seg2.Generation)
	}
	// - New generation's first room segment has Discontinuity=true
	if !seg2.Discontinuity {
		t.Fatalf("seg2.Discontinuity = false, want true")
	}
	// - New generation's first room segment uses correct room-specific audio track (track 4)
	if seg2.Tracks[3].Track != ipc.TrackAudioB {
		t.Fatalf("seg2.Tracks[3].Track = %d, want %d (TrackAudioB)", seg2.Tracks[3].Track, ipc.TrackAudioB)
	}
	// Verify incoming start UTC exactly equals cutUTC (and outgoing end UTC)
	for i := 0; i < RoomTracks; i++ {
		if seg2.Tracks[i].UTCNS != rig.cutUTC {
			t.Fatalf("seg2 track %d UTCNS = %d, want %d", i, seg2.Tracks[i].UTCNS, rig.cutUTC)
		}
	}
	// - Outgoing generation is frozen with its exact exposed Ends: {MSN: 1, Index: 1}
	if len(snapCommit.Bindings) < 2 {
		t.Fatalf("len(snapCommit.Bindings) = %d, want >= 2", len(snapCommit.Bindings))
	}
	bOut := snapCommit.Bindings[0]
	if !bOut.Frozen() {
		t.Fatalf("outgoing generation binding not frozen")
	}
	if len(bOut.Ends) != RoomTracks {
		t.Fatalf("len(bOut.Ends) = %d, want %d", len(bOut.Ends), RoomTracks)
	}
	for i := 0; i < RoomTracks; i++ {
		expectedEnd := PartRef{MSN: 1, Index: 1}
		if bOut.Ends[i] != expectedEnd {
			t.Fatalf("bOut.Ends[%d] = %+v, want %+v", i, bOut.Ends[i], expectedEnd)
		}
	}
	_ = chBound
}

func TestTimelineSwitchHoldTimeoutCancellation(t *testing.T) {
	rig := newSwitchTestRig(t)
	defer rig.teardown()

	const partDur = int64(1_000_000_000)

	// 1. Publish outgoing segment 1 part 0 for all tracks
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 1, 0, 1, 0, 0, 0, partDur, 10, true, true)
		} else {
			switchPublishPart(t, rig.pub1, trk, 1, 0, 1, 0, 0, 0, partDur, 15, true, false)
		}
	}

	// 2. Open Timeline and bind outgoing source
	ctxOpen, cancelOpen := context.WithTimeout(context.Background(), 5*time.Second)
	var err error
	rig.tl, err = OpenTimeline(ctxOpen, TimelineOptions{
		Directory: rig.tlDir,
		Room:      rig.room,
		Window:    DefaultRoomWindow,
	}, rig.resolver)
	cancelOpen()
	if err != nil {
		t.Fatalf("OpenTimeline: %v", err)
	}

	if err := rig.tl.Bind(rig.key1, 1); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	// 3. PlanSwitch to incoming source
	const planMonoNS = int64(1_000_000_000)
	if err := rig.tl.PlanSwitch(rig.key2, 1, planMonoNS); err != nil {
		t.Fatalf("PlanSwitch: %v", err)
	}

	// 4. Drive outgoing timeline to the cut/hold
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 1, 1, 1, 10, partDur, partDur, partDur, 10, false, false)
		} else {
			switchPublishPart(t, rig.pub1, trk, 1, 1, 1, 15, partDur, partDur, partDur, 15, false, false)
		}
	}
	// Cross cut with segment 2 part 0
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 2, 0, 2, 20, 2*partDur, 2*partDur, partDur, 10, true, true)
		} else {
			switchPublishPart(t, rig.pub1, trk, 2, 0, 2, 30, 2*partDur, 2*partDur, partDur, 15, true, false)
		}
	}

	const holdMonoNS = int64(2_000_000_000)
	if err := rig.tl.Refresh(holdMonoNS); err != nil {
		t.Fatalf("Refresh hold: %v", err)
	}

	snapHold, _, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe hold: %v", err)
	}
	if snapHold.Pending == nil || snapHold.Pending.HoldStartMonoNS != holdMonoNS {
		t.Fatalf("snapHold.Pending = %+v, want HoldStartMonoNS %d", snapHold.Pending, holdMonoNS)
	}
	incomingGen := snapHold.Pending.Incoming.Generation

	// Verify incoming pin was installed on source 2
	rig.s2.op.Lock()
	initialPinsOnS2 := len(rig.s2.pins)
	rig.s2.op.Unlock()
	if initialPinsOnS2 == 0 {
		t.Fatalf("expected pin on source 2 while pending switch, got 0")
	}

	// 5. Leave incoming without a usable start segment.
	// Call Refresh at hold-start + SwitchHoldLimit (matching source comparison: now - holdStart >= SwitchHoldLimit)
	timeoutMonoNS := holdMonoNS + int64(SwitchHoldLimit)
	if err := rig.tl.Refresh(timeoutMonoNS); err != nil {
		t.Fatalf("Refresh timeout: %v", err)
	}

	// 6. Assertions after timeout cancellation:
	snapCancelled, _, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe cancelled: %v", err)
	}
	// - Pending clears
	if snapCancelled.Pending != nil {
		t.Fatalf("snapCancelled.Pending = %+v, want nil", snapCancelled.Pending)
	}
	// - Outgoing remains live and unfrozen
	if snapCancelled.Live == nil || snapCancelled.Live.Key != rig.key1 {
		t.Fatalf("snapCancelled.Live = %+v, want key1", snapCancelled.Live)
	}
	if snapCancelled.Live.Frozen() {
		t.Fatalf("outgoing live generation must remain unfrozen after cancellation")
	}
	// - Incoming generation is no longer retained in Timeline.gens
	rig.tl.mu.Lock()
	_, existsInGens := rig.tl.gens[incomingGen]
	rig.tl.mu.Unlock()
	if existsInGens {
		t.Fatalf("incoming generation %d still exists in Timeline.gens after cancellation", incomingGen)
	}
	// - Incoming generation pin is released
	rig.s2.op.Lock()
	remainingPinsOnS2 := len(rig.s2.pins)
	rig.s2.op.Unlock()
	if remainingPinsOnS2 != 0 {
		t.Fatalf("expected 0 pins on source 2 after cancellation, got %d", remainingPinsOnS2)
	}

	// 7. Publish outgoing media past the cut and refresh: room resumes with continuous MSN and no new discontinuity
	// Segment 2 part 0 was already published and unblocked by cancellation. Now publish segment 2 part 1:
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 2, 1, 2, 30, 3*partDur, 3*partDur, partDur, 10, false, false)
		} else {
			switchPublishPart(t, rig.pub1, trk, 2, 1, 2, 45, 3*partDur, 3*partDur, partDur, 15, false, false)
		}
	}

	if err := rig.tl.Refresh(timeoutMonoNS + 100_000_000); err != nil {
		t.Fatalf("Refresh post-cancellation: %v", err)
	}

	snapResume, _, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe resume: %v", err)
	}
	if snapResume.NextMSN != 3 {
		t.Fatalf("snapResume.NextMSN = %d, want 3", snapResume.NextMSN)
	}
	if len(snapResume.Segments) != 2 {
		t.Fatalf("len(snapResume.Segments) = %d, want 2", len(snapResume.Segments))
	}
	seg2 := snapResume.Segments[1]
	if seg2.MSN != 2 {
		t.Fatalf("seg2.MSN = %d, want 2", seg2.MSN)
	}
	if seg2.Generation != 1 {
		t.Fatalf("seg2.Generation = %d, want 1 (outgoing)", seg2.Generation)
	}
	if seg2.Discontinuity {
		t.Fatalf("seg2.Discontinuity = true, want false (no new discontinuity)")
	}
}

func TestTimelineSwitchRecoveryDuringHold(t *testing.T) {
	rig := newSwitchTestRig(t)
	defer rig.teardown()

	const partDur = int64(1_000_000_000)

	// 1. Publish outgoing segment 1 part 0 for all tracks
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 1, 0, 1, 0, 0, 0, partDur, 10, true, true)
		} else {
			switchPublishPart(t, rig.pub1, trk, 1, 0, 1, 0, 0, 0, partDur, 15, true, false)
		}
	}

	// 2. Open Timeline and bind outgoing source
	ctxOpen, cancelOpen := context.WithTimeout(context.Background(), 5*time.Second)
	var err error
	rig.tl, err = OpenTimeline(ctxOpen, TimelineOptions{
		Directory: rig.tlDir,
		Room:      rig.room,
		Window:    DefaultRoomWindow,
	}, rig.resolver)
	cancelOpen()
	if err != nil {
		t.Fatalf("OpenTimeline: %v", err)
	}

	if err := rig.tl.Bind(rig.key1, 1); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	// 3. PlanSwitch to incoming source
	const planMonoNS = int64(1_000_000_000)
	if err := rig.tl.PlanSwitch(rig.key2, 1, planMonoNS); err != nil {
		t.Fatalf("PlanSwitch: %v", err)
	}

	// 4. Drive outgoing media through the cut to reach a durable pending hold
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 1, 1, 1, 10, partDur, partDur, partDur, 10, false, false)
		} else {
			switchPublishPart(t, rig.pub1, trk, 1, 1, 1, 15, partDur, partDur, partDur, 15, false, false)
		}
	}
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		if trk < ipc.TrackAudioA {
			switchPublishPart(t, rig.pub1, trk, 2, 0, 2, 20, 2*partDur, 2*partDur, partDur, 10, true, true)
		} else {
			switchPublishPart(t, rig.pub1, trk, 2, 0, 2, 30, 2*partDur, 2*partDur, partDur, 15, true, false)
		}
	}

	const holdMonoNS = int64(10_000_000_000) // known monotonic AtMonoNS
	if err := rig.tl.Refresh(holdMonoNS); err != nil {
		t.Fatalf("Refresh hold: %v", err)
	}

	snapHold, _, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe hold: %v", err)
	}
	if snapHold.Pending == nil || snapHold.Pending.HoldStartMonoNS != holdMonoNS {
		t.Fatalf("snapHold.Pending = %+v, want HoldStartMonoNS %d", snapHold.Pending, holdMonoNS)
	}

	// 5. Close ONLY the Timeline (sources, publishers, and servers remain live)
	if err := rig.tl.Close(); err != nil {
		t.Fatalf("tl.Close: %v", err)
	}

	// 6. Reopen from the same journal and same live Source resolver
	ctxReopen, cancelReopen := context.WithTimeout(context.Background(), 5*time.Second)
	rig.tl2, err = OpenTimeline(ctxReopen, TimelineOptions{
		Directory: rig.tlDir,
		Room:      rig.room,
		Window:    DefaultRoomWindow,
	}, rig.resolver)
	cancelReopen()
	if err != nil {
		t.Fatalf("OpenTimeline reopen: %v", err)
	}

	// 7. Assert pending incoming key, cut and HoldStartMonoNS survive unchanged; room MSN and outgoing binding preserved
	snapReopen, _, err := rig.tl2.Observe()
	if err != nil {
		t.Fatalf("tl2.Observe: %v", err)
	}
	if snapReopen.Pending == nil {
		t.Fatalf("snapReopen.Pending is nil after same-boot recovery")
	}
	if snapReopen.Pending.Incoming.Key != rig.key2 {
		t.Fatalf("reopened pending incoming key = %+v, want %+v", snapReopen.Pending.Incoming.Key, rig.key2)
	}
	if snapReopen.Pending.CutUTCNS != rig.cutUTC {
		t.Fatalf("reopened pending cut = %d, want %d", snapReopen.Pending.CutUTCNS, rig.cutUTC)
	}
	if snapReopen.Pending.HoldStartMonoNS != holdMonoNS {
		t.Fatalf("reopened pending HoldStartMonoNS = %d, want %d", snapReopen.Pending.HoldStartMonoNS, holdMonoNS)
	}
	if snapReopen.Live == nil || snapReopen.Live.Key != rig.key1 {
		t.Fatalf("reopened Live = %+v, want key1", snapReopen.Live)
	}
	if snapReopen.NextMSN != 2 {
		t.Fatalf("reopened NextMSN = %d, want 2", snapReopen.NextMSN)
	}

	// 8. Refresh before the 3s limit keeps it pending
	refreshBeforeLimit := holdMonoNS + int64(SwitchHoldLimit) - 1
	if err := rig.tl2.Refresh(refreshBeforeLimit); err != nil {
		t.Fatalf("Refresh before limit: %v", err)
	}

	snapBefore, _, err := rig.tl2.Observe()
	if err != nil {
		t.Fatalf("Observe before limit: %v", err)
	}
	if snapBefore.Pending == nil {
		t.Fatalf("expected switch to stay pending before 3s limit, got nil")
	}
	if snapBefore.Pending.HoldStartMonoNS != holdMonoNS {
		t.Fatalf("HoldStartMonoNS before limit = %d, want %d", snapBefore.Pending.HoldStartMonoNS, holdMonoNS)
	}
	if snapBefore.Live == nil || snapBefore.Live.Key != rig.key1 {
		t.Fatalf("Live before limit = %+v, want key1", snapBefore.Live)
	}

	// 9. Refresh at the 3s limit: cancels the pending switch
	refreshAtLimit := holdMonoNS + int64(SwitchHoldLimit)
	if err := rig.tl2.Refresh(refreshAtLimit); err != nil {
		t.Fatalf("Refresh at limit: %v", err)
	}

	snapAtLimit, _, err := rig.tl2.Observe()
	if err != nil {
		t.Fatalf("Observe at limit: %v", err)
	}
	if snapAtLimit.Pending != nil {
		t.Fatalf("expected pending to be cancelled at 3s limit, got %+v", snapAtLimit.Pending)
	}
	// Monotonic room MSN and outgoing binding preserved
	if snapAtLimit.Live == nil || snapAtLimit.Live.Key != rig.key1 {
		t.Fatalf("Live at limit = %+v, want key1", snapAtLimit.Live)
	}
	if snapAtLimit.Live.Frozen() {
		t.Fatalf("outgoing live generation must remain unfrozen after cancellation")
	}
	// Room MSN 1 is exposed, segment 2 is exposed past the cut
	if snapAtLimit.NextMSN < 2 {
		t.Fatalf("NextMSN at limit = %d, want >= 2", snapAtLimit.NextMSN)
	}
}
