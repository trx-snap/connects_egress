//go:build linux

package supervisor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ctRig is a switch rig with the outgoing source bound into room_b's timeline and a controller
// running over a fake clock.
type ctRig struct {
	*switchTestRig
	now    atomic.Int64
	c      *PlaybackController
	cancel context.CancelFunc
	done   chan error
}

func newCtRig(t *testing.T) *ctRig {
	t.Helper()
	r := &ctRig{switchTestRig: newSwitchTestRig(t), done: make(chan error, 1)}
	r.now.Store(otBaseMono)
	otPublish(t, r.pub1, 1, 0)
	r.tl = otOpen(t, r.switchTestRig, "room_b")
	if err := r.tl.Bind(r.key1, 1); err != nil {
		r.teardown()
		t.Fatalf("Bind: %v", err)
	}
	c, err := NewPlaybackController(r.resolver, r.now.Load)
	if err != nil {
		r.teardown()
		t.Fatalf("NewPlaybackController: %v", err)
	}
	if err := c.Register(r.tl); err != nil {
		r.teardown()
		t.Fatalf("Register: %v", err)
	}
	r.c = c
	return r
}

// run starts the controller; stop cancels it and returns Run's result.
func (r *ctRig) run() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() { r.done <- r.c.Run(ctx) }()
}

func (r *ctRig) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		return err
	case <-time.After(otWaitLimit):
		t.Fatalf("Run did not return after cancellation")
		return nil
	}
}

// holdSwitch publishes the outgoing source through segment 1, plans the switch and drives the
// outgoing source beyond the cut so the controller records the hold.
func (r *ctRig) holdSwitch(t *testing.T) {
	t.Helper()
	for idx := 1; idx < otParts; idx++ {
		otPublish(t, r.pub1, 1, idx)
	}
	otWait(t, r.tl, "segment 1 parts", func(s TimelineSnapshot) bool {
		return len(s.Segments) == 1 && len(s.Segments[0].Tracks[0].Parts) == otParts && len(s.Segments[0].Tracks[3].Parts) == otParts
	})
	if err := r.tl.PlanSwitch(r.key2, 1, r.now.Load()); err != nil {
		t.Fatalf("PlanSwitch: %v", err)
	}
	otPublish(t, r.pub1, 2, 0)
	snap := otWait(t, r.tl, "hold", func(s TimelineSnapshot) bool {
		return s.Pending != nil && s.Pending.HoldStartMonoNS != 0
	})
	if snap.Pending.HoldStartMonoNS != otBaseMono || snap.Live == nil || snap.Live.Key != r.key1 || snap.Live.Frozen() || snap.NextMSN != 2 {
		t.Fatalf("held view: pending %+v live %+v next %d", snap.Pending, snap.Live, snap.NextMSN)
	}
}

func TestPlaybackControllerDrivesSwitch(t *testing.T) {
	r := newCtRig(t)
	defer r.teardown()
	baseline := runtime.NumGoroutine()
	r.run()
	r.holdSwitch(t)

	// A playlist waiter on room MSN 2 wakes on the committed switch, not on a publication alone.
	o := NewOrigin(r.c)
	waiter := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		o.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/live/room_b/vhi.m3u8?_HLS_msn=2", nil))
		waiter <- rec
	}()
	otPublish(t, r.pub2, 1, 0)
	snap := otWait(t, r.tl, "commit", func(s TimelineSnapshot) bool { return s.Live != nil && s.Live.Key == r.key2 })
	if snap.Pending != nil || snap.NextMSN != 3 || len(snap.Segments) != 2 || !snap.Segments[1].Discontinuity ||
		snap.Segments[1].SourceMSN != 1 || snap.Segments[1].Tracks[3].Track != 4 || !snap.Bindings[0].Frozen() || snap.Bindings[0].Failed {
		t.Fatalf("committed view: %+v", snap)
	}
	for i := range snap.Bindings[0].Ends {
		if snap.Bindings[0].Ends[i] != (PartRef{MSN: 1, Index: otParts - 1}) {
			t.Fatalf("outgoing ends = %+v", snap.Bindings[0].Ends)
		}
	}
	select {
	case rec := <-waiter:
		if rec.Code != http.StatusOK {
			t.Fatalf("waiter = %d %s", rec.Code, rec.Body.String())
		}
		otContains(t, rec.Body.String(), "#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\""+otInBase+"vhi/init.mp4\"\n")
		otContains(t, rec.Body.String(), `URI="`+otInBase+`vhi/1.0.m4s",INDEPENDENT=YES`)
	case <-time.After(otWaitLimit):
		t.Fatalf("waiter did not wake on the switch")
	}
	// The incoming source keeps advancing the room.
	otPublish(t, r.pub2, 1, 1)
	otWait(t, r.tl, "incoming part", func(s TimelineSnapshot) bool { return len(s.Segments[1].Tracks[0].Parts) == 2 })

	if err := r.stop(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	otWaitGoroutines(t, baseline)
	// A second Run after the first returned is allowed; a cancelled context returns at once.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.c.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run with cancelled context = %v", err)
	}
}

func TestPlaybackControllerHoldDeadlineCancels(t *testing.T) {
	r := newCtRig(t)
	defer r.teardown()
	r.run()
	r.holdSwitch(t)

	// Before the limit nothing changes; a publication at the limit cancels the plan and the
	// outgoing source resumes with continuous room numbering and no discontinuity.
	r.now.Store(otBaseMono + int64(SwitchHoldLimit)/2)
	otPublish(t, r.pub1, 2, 1)
	time.Sleep(200 * time.Millisecond)
	if snap, _, _ := r.tl.Observe(); snap.Pending == nil || snap.NextMSN != 2 {
		t.Fatalf("switch cancelled before the hold limit: %+v", snap)
	}
	r.now.Store(otBaseMono + int64(SwitchHoldLimit))
	otPublish(t, r.pub1, 2, 2)
	snap := otWait(t, r.tl, "cancellation", func(s TimelineSnapshot) bool {
		return s.Pending == nil && s.NextMSN == 3 && len(s.Segments[1].Tracks[0].Parts) == 3
	})
	if snap.Live == nil || snap.Live.Key != r.key1 || snap.Live.Frozen() || snap.Segments[1].Discontinuity ||
		snap.Segments[1].Key != r.key1 || snap.Segments[1].SourceMSN != 2 || len(snap.Segments[1].Tracks[0].Parts) != 3 {
		t.Fatalf("resumed view: %+v", snap)
	}
	if err := r.stop(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

func TestPlaybackControllerHoldDeadlineTimer(t *testing.T) {
	r := newCtRig(t)
	defer r.teardown()
	r.run()
	r.holdSwitch(t)
	// With nothing published, the controller's own timer fires after the real limit elapses on
	// the fake clock advanced in step.
	start := time.Now()
	r.now.Store(otBaseMono + int64(SwitchHoldLimit))
	otWait(t, r.tl, "timer cancellation", func(s TimelineSnapshot) bool { return s.Pending == nil })
	if elapsed := time.Since(start); elapsed < SwitchHoldLimit-200*time.Millisecond || elapsed > SwitchHoldLimit+2*time.Second {
		t.Fatalf("cancelled after %v, want about %v", elapsed, SwitchHoldLimit)
	}
	if err := r.stop(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

func TestPlaybackControllerSourceCloseAndRefusal(t *testing.T) {
	r := newCtRig(t)
	defer r.teardown()
	r.run()
	r.holdSwitch(t)

	// The pending incoming source closes: the plan is cancelled, the outgoing source resumes.
	if err := r.s2.Close(); err != nil {
		t.Fatalf("close incoming: %v", err)
	}
	snap := otWait(t, r.tl, "incoming failure", func(s TimelineSnapshot) bool { return s.Pending == nil })
	if snap.Live == nil || snap.Live.Key != r.key1 || snap.Live.Frozen() || snap.NextMSN != 3 {
		t.Fatalf("after incoming close: %+v", snap)
	}
	// The live source refuses: the live generation freezes at exactly its exposed prefix.
	r.s1.refusing.Store(true)
	r.s1.op.Lock()
	r.s1.notify()
	r.s1.op.Unlock()
	snap = otWait(t, r.tl, "live refusal", func(s TimelineSnapshot) bool { return s.Failed })
	if !snap.Live.Frozen() || snap.FailReason != "source refusing" || snap.Live.Ends[0] != (PartRef{MSN: 2, Index: 0}) {
		t.Fatalf("after refusal: live %+v reason %q", snap.Live, snap.FailReason)
	}
	o := NewOrigin(r.c)
	rec := httptest.NewRecorder()
	o.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/live/room_b/vhi.m3u8", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "#EXTINF:0.333,\n"+otOutBase+"vhi/2.m4s\n") || strings.Contains(rec.Body.String(), "PRELOAD-HINT") {
		t.Fatalf("frozen playlist = %d:\n%s", rec.Code, rec.Body.String())
	}
	// The live source closes: nothing more changes and Run keeps serving.
	if err := r.s1.Close(); err != nil {
		t.Fatalf("close outgoing: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := r.stop(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if _, _, err := r.tl.Observe(); err != nil {
		t.Fatalf("timeline after Run: %v", err)
	}
}

func TestPlaybackControllerRegistry(t *testing.T) {
	r := newCtRig(t)
	defer r.teardown()
	r.tl2 = otOpen(t, r.switchTestRig, "room_a")
	if err := r.tl2.Bind(r.key1, 1); err != nil {
		t.Fatalf("Bind room_a: %v", err)
	}
	if err := r.c.Register(r.tl); !errors.Is(err, ErrTimelineInvalid) {
		t.Fatalf("duplicate Register = %v", err)
	}
	if err := r.c.Register(nil); !errors.Is(err, ErrTimelineInvalid) {
		t.Fatalf("nil Register = %v", err)
	}
	if _, err := NewPlaybackController(nil, nil); !errors.Is(err, ErrTimelineInvalid) {
		t.Fatalf("nil resolver = %v", err)
	}
	if r.c.Remove("room_a") {
		t.Fatalf("Remove of an unregistered room reported true")
	}
	r.run()
	for running := false; !running; time.Sleep(time.Millisecond) {
		r.c.mu.Lock()
		running = r.c.running
		r.c.mu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.c.Run(ctx); !errors.Is(err, ErrTimelineInvalid) {
		t.Fatalf("concurrent Run = %v", err)
	}

	// Registry mutation and lookups race with the loop while the source publishes.
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = r.c.Register(r.tl2)
				_, _ = r.c.Lookup("room_a")
				_, _ = r.c.Lookup("room_b")
				_ = r.c.Remove("room_a")
			}
		}()
	}
	for idx := 1; idx < otParts; idx++ {
		otPublish(t, r.pub1, 1, idx)
	}
	wg.Wait()
	if err := r.c.Register(r.tl2); err != nil && !errors.Is(err, ErrTimelineInvalid) {
		t.Fatalf("final Register: %v", err)
	}
	complete := func(s TimelineSnapshot) bool {
		return len(s.Segments) == 1 && len(s.Segments[0].Tracks[0].Parts) == otParts
	}
	otWait(t, r.tl, "room_b parts", complete)
	otWait(t, r.tl2, "room_a parts", complete)
	if tl, ok := r.c.Lookup("room_a"); !ok || tl != r.tl2 {
		t.Fatalf("Lookup room_a = %v %v", tl, ok)
	}
	// A removed room's closed timeline never fails the loop; a registered closed one is skipped.
	if !r.c.Remove("room_a") {
		t.Fatalf("Remove room_a reported false")
	}
	if err := r.tl2.Close(); err != nil {
		t.Fatalf("close room_a: %v", err)
	}
	if err := r.c.Register(r.tl2); err != nil {
		t.Fatalf("Register closed room_a: %v", err)
	}
	otPublish(t, r.pub1, 2, 0)
	otWait(t, r.tl, "room_b segment 2", func(s TimelineSnapshot) bool { return s.NextMSN == 3 })
	if err := r.stop(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}
