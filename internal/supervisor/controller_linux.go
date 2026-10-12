//go:build linux

package supervisor

// PlaybackController: the one loop that drives every registered room Timeline (plan §5.3, §9.5).
// It borrows the Timelines and, through the EpochResolver, their Sources: it never opens, closes,
// ends or signals any of them. Owners register and remove rooms at any time. Run observes each
// room's live and pending Sources, reports a closed, refusing or lost epoch with FailSource,
// refreshes the room on every Source or Timeline change and at the SwitchHoldLimit deadline of a
// pending switch, and returns when ctx ends. Everything runs on the caller's goroutine: Run starts
// none and waits on the change channels directly, so nothing outlives it.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// PlaybackController is the room registry and its driver. See the file comment.
type PlaybackController struct {
	resolver EpochResolver
	clock    func() int64 // monotonic ns

	mu      sync.Mutex
	rooms   map[string]*Timeline
	running bool
	wake    chan struct{} // one pending registry change
}

// NewPlaybackController returns a controller resolving epochs with resolver. clock, when not nil,
// replaces CLOCK_MONOTONIC; timelines record its values durably, so production passes nil.
func NewPlaybackController(resolver EpochResolver, clock func() int64) (*PlaybackController, error) {
	if resolver == nil {
		return nil, fmt.Errorf("%w: nil resolver", ErrTimelineInvalid)
	}
	if clock == nil {
		clock = monotonicNow
	}
	return &PlaybackController{resolver: resolver, clock: clock, rooms: map[string]*Timeline{}, wake: make(chan struct{}, 1)}, nil
}

// monotonicNow returns CLOCK_MONOTONIC in ns.
func monotonicNow() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		panic("supervisor: clock_gettime: " + err.Error())
	}
	return ts.Nano()
}

// Register adds t under its room; a room may be registered once at a time.
func (c *PlaybackController) Register(t *Timeline) error {
	if t == nil {
		return fmt.Errorf("%w: nil timeline", ErrTimelineInvalid)
	}
	room := t.opts.Room
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, dup := c.rooms[room]; dup {
		return fmt.Errorf("%w: room %q is already registered", ErrTimelineInvalid, room)
	}
	c.rooms[room] = t
	c.notify()
	return nil
}

// Remove removes the room's timeline and reports whether it was registered. The timeline is
// handed back untouched: the owner closes it.
func (c *PlaybackController) Remove(room string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.rooms[room]
	delete(c.rooms, room)
	if ok {
		c.notify()
	}
	return ok
}

// Lookup returns the room's timeline.
func (c *PlaybackController) Lookup(room string) (*Timeline, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.rooms[room]
	return t, ok
}

// Resolve returns the borrowed Source of key through the controller's resolver.
func (c *PlaybackController) Resolve(key EpochKey) (*Source, error) { return c.resolver(key) }

// notify wakes Run after a registry change, under mu.
func (c *PlaybackController) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// snapshot returns the registered timelines.
func (c *PlaybackController) snapshot() []*Timeline {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Timeline, 0, len(c.rooms))
	for _, t := range c.rooms {
		out = append(out, t)
	}
	return out
}

// Run drives the registered rooms until ctx ends and returns ctx's error. A resolver error other
// than ErrLostEpoch, a Source observation error or a timeline refusing mutations ends Run with that
// error. Only one Run may be active at a time.
func (c *PlaybackController) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("%w: controller is already running", ErrTimelineInvalid)
	}
	c.running = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cases := []reflect.SelectCase{recvCase(ctx.Done()), recvCase((<-chan struct{})(c.wake))}
		var deadline int64 = -1
		for _, t := range c.snapshot() {
			chans, d, err := c.step(t)
			if errors.Is(err, ErrTimelineClosed) {
				continue // the owner closed it and will remove it
			}
			if err != nil {
				return err
			}
			cases = append(cases, chans...)
			if d >= 0 && (deadline < 0 || d < deadline) {
				deadline = d
			}
		}
		var timer *time.Timer
		if deadline >= 0 {
			timer = time.NewTimer(max(time.Duration(deadline-c.clock()), time.Millisecond))
			cases = append(cases, recvCase(timer.C))
		}
		chosen, _, _ := reflect.Select(cases)
		if timer != nil {
			timer.Stop()
		}
		if chosen == 0 {
			return ctx.Err()
		}
	}
}

func recvCase[T any](ch <-chan T) reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
}

// step observes the live and pending Sources of t, fails what is closed, refusing or lost,
// refreshes t and returns the channels to wait on and the hold deadline of a pending switch in
// clock ns, -1 when none.
func (c *PlaybackController) step(t *Timeline) ([]reflect.SelectCase, int64, error) {
	now := c.clock()
	snap, _, err := t.Observe()
	if err != nil {
		return nil, -1, err
	}
	var keys []EpochKey
	if snap.Live != nil {
		keys = append(keys, snap.Live.Key)
	}
	if snap.Pending != nil {
		keys = append(keys, snap.Pending.Incoming.Key)
	}
	var cases []reflect.SelectCase
	for _, key := range keys {
		reason := ""
		src, err := c.resolver(key)
		switch {
		case errors.Is(err, ErrLostEpoch):
			reason = "epoch lost"
		case err != nil:
			return nil, -1, fmt.Errorf("%w: resolve %+v: %v", ErrTimelineInvalid, key, err)
		case src == nil:
			return nil, -1, fmt.Errorf("%w: resolver returned no source for %+v", ErrTimelineInvalid, key)
		default:
			obs, err := src.Observe()
			switch {
			case err != nil:
				return nil, -1, err
			case obs.Closed:
				reason = "source closed"
			case obs.Refusing:
				reason = "source refusing"
			default:
				cases = append(cases, recvCase(obs.Changed))
			}
		}
		if reason == "" {
			continue
		}
		// The key may have been cancelled or rolled out by the owner since the observation: that
		// is ErrTimelineInvalid and not a failure of this loop.
		if err := t.FailSource(key, reason, now); err != nil && !errors.Is(err, ErrTimelineInvalid) {
			return nil, -1, err
		}
	}
	if err := t.Refresh(now); err != nil {
		return nil, -1, err
	}
	snap, changed, err := t.Observe()
	if err != nil {
		return nil, -1, err
	}
	cases = append(cases, recvCase(changed))
	var deadline int64 = -1
	if p := snap.Pending; p != nil && p.HoldStartMonoNS > 0 {
		deadline = p.HoldStartMonoNS + int64(SwitchHoldLimit)
	}
	return cases, deadline, nil
}
