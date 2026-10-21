//go:build linux && cgo

package worker

// Regression tests of the pipeline's key-unit schedule and end of stream, run against the real
// GStreamer pipeline of describe, cmafmux included; nothing is mocked. The fixture is H.264
// access units and Opus packets encoded once per test binary with gst-launch-1.0. Each case
// feeds the pipeline as the ingest would, sends EOS the way generate does when the room ends,
// drains it under drainGrace and judges the end the way generate does: a verified clean end
// when the pipeline reached EOS without error, every reader drained and every track published,
// a contained failure otherwise. The media drained is checked the way publishPart and the
// supervisor would: checkPart on every chunk, ten access units per video part except the last,
// a fragment start every sixth, an IDR at every third, the three rungs identical, and nothing
// from a withheld tail on.
//
// How many access units EOS leaves behind depends on videorate: GStreamer 1.28 pushes a frame
// on arrival when it lands on its grid and then flushes the held frame once more at EOS, so the
// stream ends with one duplicate of its last frame at the next grid point; an older videorate
// ends with the last frame itself. The cases therefore judge the schedule against the access
// units the pipeline actually emitted, which must be the frames fed or one more.
//
// The tests need gst-launch-1.0 and the plugins the pipeline uses, cmafmux included
// (GST_PLUGIN_PATH when it is not installed system-wide). Without them they fail; they never
// pass vacuously.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	fixtureFrames  = 400 // H.264 access units encoded at fixtureRate
	fixturePackets = 720 // Opus packets encoded, 20 ms each: longer than the video
	fixtureRate    = 30
	opusPacket     = 20 * time.Millisecond
	catchUp        = 3 * time.Second // the feed's bounded wait for the pipeline before EOS
)

type fixture struct{ video, audio [][]byte }

var (
	fixtureOnce sync.Once
	fixtureData *fixture
	fixtureErr  error
)

// loadFixture encodes the fixture once per test binary.
func loadFixture(t *testing.T) *fixture {
	t.Helper()
	fixtureOnce.Do(func() { fixtureData, fixtureErr = encodeFixture() })
	if fixtureErr != nil {
		t.Fatalf("fixture: %v", fixtureErr)
	}
	return fixtureData
}

// encodeFixture runs gst-launch-1.0 twice, one file per access unit through multifilesink, and
// reads the files back in order. The video is 30 fps with an IDR every 30 frames, which the
// pipeline decodes and re-encodes anyway; the audio is 20 ms Opus packets.
func encodeFixture() (*fixture, error) {
	launch, err := exec.LookPath("gst-launch-1.0")
	if err != nil {
		return nil, fmt.Errorf("gst-launch-1.0 is required to encode the fixture: %w", err)
	}
	dir, err := os.MkdirTemp("", "ge-fixture-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	runs := []struct {
		prefix string
		args   []string
		dst    *[][]byte
		want   int
	}{
		{"v", []string{"videotestsrc", fmt.Sprintf("num-buffers=%d", fixtureFrames), "pattern=ball", "!",
			fmt.Sprintf("video/x-raw,format=I420,width=640,height=360,framerate=%d/1", fixtureRate), "!",
			"x264enc", "tune=zerolatency", "speed-preset=ultrafast", "bitrate=600", "key-int-max=30", "!",
			"h264parse", "!", "video/x-h264,stream-format=byte-stream,alignment=au", "!",
			"multifilesink", "location=" + filepath.Join(dir, "v%05d")}, nil, fixtureFrames},
		{"a", []string{"audiotestsrc", fmt.Sprintf("num-buffers=%d", fixturePackets), "samplesperbuffer=960", "wave=sine", "!",
			"audio/x-raw,format=S16LE,rate=48000,channels=2", "!", "opusenc", "frame-size=20", "bitrate=64000", "!",
			"multifilesink", "location=" + filepath.Join(dir, "a%05d")}, nil, fixturePackets - 1},
	}
	f := &fixture{}
	runs[0].dst, runs[1].dst = &f.video, &f.audio
	for _, run := range runs {
		if out, err := exec.Command(launch, run.args...).CombinedOutput(); err != nil {
			return nil, fmt.Errorf("gst-launch-1.0 %s: %w\n%s", strings.Join(run.args, " "), err, out)
		}
		names, err := filepath.Glob(filepath.Join(dir, run.prefix+"*"))
		if err != nil {
			return nil, err
		}
		sort.Strings(names)
		for _, name := range names {
			b, err := os.ReadFile(name)
			if err != nil {
				return nil, err
			}
			*run.dst = append(*run.dst, b)
		}
		if len(*run.dst) < run.want {
			return nil, fmt.Errorf("fixture %s: %d units encoded, want at least %d", run.prefix, len(*run.dst), run.want)
		}
	}
	return f, nil
}

// grid is the PTS of normalized frame n relative to the first one, as videorate stamps it.
func grid(n int) int64 {
	return int64(n) * int64(time.Second) / fixtureRate
}

// framePTS is the PTS of normalized frame n relative to the first one as the tee passed it on:
// on the grid, or 1 ns early at every multiple of early when the case has that.
func framePTS(n int, early uint) int64 {
	if early > 0 && n > 0 && n%int(early) == 0 {
		return grid(n) - 1
	}
	return grid(n)
}

// endCase is one end of a room: EOS after frames video access units, with key-unit request
// withhold (0 none, n the one for frame 30 n) kept from rung withholdRung, every rung when
// negative, and every frame at a multiple of early (0 none) entering the tee 1 ns early.
type endCase struct {
	name         string
	frames       int
	withhold     uint
	withholdRung int
	early        uint
}

// trackEnd is what one reader drained from its appsink.
type trackEnd struct {
	inits  int
	chunks []header
	phases bool // the pull that carried the first audio chunk knew both phases
	eos    bool
	err    error
	parts  atomic.Int64 // len(chunks), readable while the reader runs
}

// endResult is how the room's end went: cause and the verdict as generate would form them,
// the media every track drained, the tail the pipeline withheld, if any, and the key-unit
// requests it sent.
type endResult struct {
	tracks   [rungs + 1]trackEnd
	cause    error
	verified bool
	reason   string
	tailAU   int64
	tail     bool
	requests uint
}

// endRoom runs the real pipeline through one room end the way generate does: readers and the
// bus poller first, the feed, EOS, the drain under drainGrace, then NULL and the verdict.
func endRoom(t *testing.T, f *fixture, c endCase) *endResult {
	t.Helper()
	if c.frames > len(f.video) {
		t.Fatalf("%d frames wanted, %d in the fixture", c.frames, len(f.video))
	}
	p, err := openPipeline()
	if err != nil {
		t.Fatalf("openPipeline: %v", err)
	}
	defer p.close()
	if c.withhold > 0 {
		p.withholdKeyUnit(c.withhold, c.withholdRung)
	}
	if c.early > 0 {
		p.earlyFrames(c.early)
	}
	if err := p.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	res := &endResult{}
	readerDone := make(chan int, rungs+1)
	for trk := 0; trk <= rungs; trk++ {
		go func() {
			te := &res.tracks[trk]
			te.err = readTrack(p, trk, te)
			readerDone <- trk
		}()
	}
	busDone, quit := make(chan error, 1), make(chan struct{})
	go func() { busDone <- pollBus(p, quit) }()
	feedDone, stop := make(chan error, 1), make(chan struct{})
	go func() { feedDone <- feed(p, f, c.frames, &res.tracks[0], stop) }()

	// Until the feed is done or the first failure, as generate runs the room.
	readers, busLive := rungs+1, true
	var cause error
	fed := false
	for cause == nil && !fed {
		select {
		case err := <-feedDone:
			fed, cause = true, err
		case err := <-busDone:
			busLive = false
			if cause = err; err == nil {
				cause = errors.New("the pipeline reached EOS before the room ended")
			}
		case trk := <-readerDone:
			readers--
			if cause = res.tracks[trk].err; cause == nil {
				cause = errors.New("a reader drained before EOS was sent")
			}
		}
	}
	if !fed {
		close(stop)
		<-feedDone
	}
	// The room's end: EOS and the drain, as generate does them.
	if cause == nil {
		cause = p.endOfStream()
	}
	busEOS := false
	drain := time.NewTimer(drainGrace)
	for cause == nil && !(busEOS && readers == 0) {
		select {
		case err := <-busDone:
			busLive = false
			if err != nil {
				cause = fmt.Errorf("pipeline error: %w", err)
			} else {
				busEOS = true
			}
		case trk := <-readerDone:
			readers--
			if err := res.tracks[trk].err; err != nil {
				cause = err
			}
		case <-drain.C:
			cause = fmt.Errorf("EOS did not drain within %v", drainGrace)
		}
	}
	drain.Stop()
	close(quit)
	p.stop()
	for ; readers > 0; readers-- {
		trk := <-readerDone
		if err := res.tracks[trk].err; err != nil && cause == nil {
			cause = err
		}
	}
	if busLive {
		<-busDone
	}
	res.cause = cause
	res.tailAU, res.tail = p.tail()
	res.requests = p.keyUnitRequests()
	var tracks [rungs + 1]trackState
	for trk := range tracks {
		tracks[trk] = trackState{parts: len(res.tracks[trk].chunks), phases: res.tracks[trk].phases}
	}
	res.verified, res.reason = cause == nil, "room ended"
	if res.verified {
		if why := incompleteTracks(tracks); why != "" {
			res.verified, res.reason = false, why
		}
	} else {
		res.reason = cause.Error()
	}
	t.Logf("%s: verified=%v reason=%q parts=%v tail=%v/%d requests=%d", c.name, res.verified, res.reason,
		[]int{len(res.tracks[0].chunks), len(res.tracks[1].chunks), len(res.tracks[2].chunks), len(res.tracks[3].chunks)}, res.tail, res.tailAU, res.requests)
	return res
}

// readTrack drains one appsink into te with the checks read makes before publishing: no stray
// buffer, every video chunk classified, checkPart on every chunk.
func readTrack(p *pipeline, trk int, te *trackEnd) error {
	for {
		l, eos, err := p.pull(trk, pollStep)
		if err != nil {
			return fmt.Errorf("%s reader: %w", trackLabels[trk], err)
		}
		if eos {
			te.eos = true
			return nil
		}
		if l == nil {
			continue
		}
		if l.stray != 0 || l.overflow != 0 {
			return fmt.Errorf("%s reader: sample holds %d stray buffers and %d headers beyond the bound", trackLabels[trk], l.stray, l.overflow)
		}
		for _, h := range l.headers {
			if h.isInit {
				te.inits++
				continue
			}
			if trk < trackAudio && h.keyUnit < 0 {
				return fmt.Errorf("%s reader: the chunk at access unit %d was emitted before that access unit was classified", trackLabels[trk], h.firstAU)
			}
			if trk == trackAudio && len(te.chunks) == 0 {
				te.phases = l.haveVideo && l.haveAudio
			}
			if err := checkPart(trk, h); err != nil {
				return err
			}
			te.chunks = append(te.chunks, h)
			te.parts.Store(int64(len(te.chunks)))
		}
	}
}

// feed pushes frames video access units at fixtureRate from the pipeline's running time, as the
// ingest places them, and the Opus packets covering them from P on, as the audio feeder anchors
// them once P exists. It then gives the hi rung up to catchUp to publish all but the last part,
// so that EOS follows a pipeline that has caught up, as it has when a room ends (a withheld tail
// leaves a part open, so the wait is bounded, not required); stop ends the wait when the room
// has failed meanwhile.
func feed(p *pipeline, f *fixture, frames int, hi *trackEnd, stop <-chan struct{}) error {
	offset, err := p.runningTime()
	if err != nil {
		return err
	}
	videoPTS := func(n int) time.Duration { return offset + time.Duration(grid(n)) }
	audioStart, next := time.Duration(-1), 0
	audioUpTo := func(end time.Duration) error { // the packets covering P to end
		for ; audioStart+time.Duration(next)*opusPacket <= end; next++ {
			if next >= len(f.audio) {
				return errors.New("the fixture has too few audio packets")
			}
			pts := audioStart + time.Duration(next)*opusPacket
			if dropped, err := p.push(inputAudio, f.audio[next], pts, opusPacket); err != nil || dropped {
				return fmt.Errorf("audio push %d: dropped=%v err=%v", next, dropped, err)
			}
		}
		return nil
	}
	// The parser and videorate each hold a frame back, so P exists only a few frames in; the
	// video is never held up for it, the audio joins once it is there.
	for n := 0; n < frames; n++ {
		if dropped, err := p.push(inputVideo, f.video[n], videoPTS(n), time.Second/fixtureRate); err != nil || dropped {
			return fmt.Errorf("video push %d: dropped=%v err=%v", n, dropped, err)
		}
		if audioStart < 0 {
			v, _, have, _ := p.phases()
			if !have {
				continue
			}
			audioStart = time.Duration(v)
		}
		if err := audioUpTo(videoPTS(n)); err != nil {
			return err
		}
	}
	if audioStart < 0 {
		deadline := time.Now().Add(5 * time.Second)
		for {
			v, _, have, _ := p.phases()
			if have {
				audioStart = time.Duration(v)
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("P was not recorded within 5s of the last of %d frames", frames)
			}
			select {
			case <-stop:
				return nil
			case <-time.After(5 * time.Millisecond):
			}
		}
		if err := audioUpTo(videoPTS(frames - 1)); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(catchUp)
	for hi.parts.Load() < int64(frames/10-1) && time.Now().Before(deadline) {
		select {
		case <-stop:
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil
}

// emittedAUs is how many video access units the hi rung's parts cover: the frames fed, plus
// the one videorate may add at EOS.
func emittedAUs(res *endResult) int {
	chunks := res.tracks[0].chunks
	if len(chunks) == 0 {
		return 0
	}
	last := chunks[len(chunks)-1]
	return int(last.firstAU) + last.payload
}

// samePart reports whether two chunks of different rungs describe the same media interval, as
// the supervisor compares the rungs' parts.
func samePart(a, b header) bool {
	return a.firstAU == b.firstAU && a.payload == b.payload && a.pts == b.pts && a.dts == b.dts && a.duration == b.duration &&
		a.sequence == b.sequence && a.fragmentStart == b.fragmentStart && a.keyUnit == b.keyUnit
}

// checkVideo checks the video of every rung against the schedule for published access units:
// one init, parts of ten access units from access unit 0, the last one shorter, each lasting
// exactly its frames (the muxer sums the durations videorate stamped, so an early frame moves
// a part's PTS, never its duration), a fragment start every sixth, an IDR at every third and
// nowhere else, and the three rungs identical.
func checkVideo(t *testing.T, res *endResult, published int) {
	t.Helper()
	want := (published + 9) / 10
	for trk := 0; trk < rungs; trk++ {
		te := &res.tracks[trk]
		label := trackLabels[trk]
		if te.inits != 1 {
			t.Errorf("%s: %d inits, want 1", label, te.inits)
		}
		if len(te.chunks) != want {
			t.Errorf("%s: %d parts, want %d for %d access units", label, len(te.chunks), want, published)
		}
		for j, h := range te.chunks {
			payload := min(10, published-10*j)
			if h.firstAU != int64(10*j) || h.payload != payload {
				t.Errorf("%s part %d: access units %d+%d, want %d+%d", label, j, h.firstAU, h.payload, 10*j, payload)
			}
			if dur := grid(10*j+payload) - grid(10*j); h.duration != dur {
				t.Errorf("%s part %d: duration %d ns, want %d ns", label, j, h.duration, dur)
			}
			if h.fragmentStart != (j%6 == 0) {
				t.Errorf("%s part %d: fragment start %v", label, j, h.fragmentStart)
			}
			if (h.keyUnit == 1) != (j%3 == 0) {
				t.Errorf("%s part %d: key unit %d", label, j, h.keyUnit)
			}
			if trk > 0 && j < len(res.tracks[0].chunks) && !samePart(h, res.tracks[0].chunks[j]) {
				t.Errorf("%s part %d: %+v differs from vhi's %+v", label, j, h, res.tracks[0].chunks[j])
			}
		}
	}
}

// checkAudio checks the mix: one init, the phases with its first chunk, a fragment start first,
// 15 access units per chunk except the last of a segment, and no chunk over its part target.
func checkAudio(t *testing.T, res *endResult) {
	t.Helper()
	te := &res.tracks[trackAudio]
	if len(te.chunks) == 0 {
		t.Errorf("a: no part")
		return
	}
	if te.inits != 1 || !te.phases || !te.chunks[0].fragmentStart {
		t.Errorf("a: %d inits, phases %v, first fragment start %v", te.inits, te.phases, te.chunks[0].fragmentStart)
	}
	for j, h := range te.chunks {
		last := j+1 == len(te.chunks) || te.chunks[j+1].fragmentStart
		if h.payload <= 0 || h.payload > 15 || (!last && h.payload != 15) {
			t.Errorf("a part %d: %d access units, segment-last %v", j, h.payload, last)
		}
		if h.duration > (320 * time.Millisecond).Nanoseconds() {
			t.Errorf("a part %d: duration %d ns over the part target", j, h.duration)
		}
	}
}

// cleanEnd asserts a verified clean end with published video access units and no withheld tail.
func cleanEnd(t *testing.T, res *endResult, published int) {
	t.Helper()
	if !res.verified || res.cause != nil {
		t.Fatalf("verified=%v cause=%v, want a verified clean end", res.verified, res.cause)
	}
	for trk := 0; trk <= rungs; trk++ {
		if !res.tracks[trk].eos {
			t.Errorf("%s reader did not reach EOS", trackLabels[trk])
		}
	}
	checkVideo(t, res, published)
	checkAudio(t, res)
}

// TestPipelineEndAroundBoundaries ends the room one frame before, at and one frame after a part
// boundary, a key-unit boundary and a segment boundary (with videorate's extra frame, one
// further on as well), and after six whole segments: every end is verified and clean, and every
// part published is whole and on schedule except the last, which closes the stream.
func TestPipelineEndAroundBoundaries(t *testing.T) {
	f := loadFixture(t)
	for _, c := range []endCase{
		{name: "before a part boundary", frames: 99},
		{name: "at a part boundary", frames: 100},
		{name: "after a part boundary", frames: 101},
		{name: "before a key-unit boundary", frames: 119},
		{name: "at a key-unit boundary", frames: 120},
		{name: "after a key-unit boundary", frames: 121},
		{name: "before a segment boundary", frames: 179},
		{name: "at a segment boundary", frames: 180},
		{name: "after a segment boundary", frames: 181},
		{name: "after six segments", frames: 365},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := endRoom(t, f, c)
			emitted := emittedAUs(res)
			if emitted != c.frames && emitted != c.frames+1 {
				t.Errorf("%d access units emitted for %d frames", emitted, c.frames)
			}
			cleanEnd(t, res, emitted)
			if res.tail {
				t.Errorf("tail withheld from access unit %d, want none", res.tailAU)
			}
		})
	}
}

// TestPipelineKeyUnitsOnEarlyFrames is the reported timing defect: every frame a key unit is
// due on enters the tee with its running time 1 ns short of P + k s, as a nanosecond of
// rounding in the pipeline's timestamps leaves it. The schedule counts frames, so each request
// still goes out on frame 30 k and every rung's IDR lands on access unit 30 k, across six
// key-unit intervals and a segment boundary; the parts stay whole, on schedule and within
// their target, and the end is verified and clean. The parts' own PTS prove the frames did
// enter early.
func TestPipelineKeyUnitsOnEarlyFrames(t *testing.T) {
	f := loadFixture(t)
	c := endCase{name: "every key-unit frame 1 ns early", frames: 191, early: 30}
	res := endRoom(t, f, c)
	emitted := emittedAUs(res)
	if emitted != c.frames && emitted != c.frames+1 {
		t.Errorf("%d access units emitted for %d frames", emitted, c.frames)
	}
	cleanEnd(t, res, emitted)
	if res.tail {
		t.Errorf("tail withheld from access unit %d, want none", res.tailAU)
	}
	if want := uint((emitted - 1) / 30); res.requests != want {
		t.Errorf("%d key-unit requests sent, want %d for %d access units", res.requests, want, emitted)
	}
	for trk := 0; trk < rungs; trk++ {
		chunks := res.tracks[trk].chunks
		for j := 1; j < len(chunks); j++ {
			if got, want := chunks[j].pts-chunks[0].pts, framePTS(10*j, c.early); got != want {
				t.Errorf("%s part %d starts %d ns after part 0, want %d", trackLabels[trk], j, got, want)
			}
		}
	}
}

// TestPipelineEndWithheldTail is the reported condition: the stream ends right after a boundary
// at a multiple of three whose access unit is not an IDR (key-unit request 4, the one for access
// unit 120, never reaches the encoders). The end is clean: the twelve whole parts before the
// boundary are published, the frames from it on are withheld on every rung, and the pipeline
// reports the tail.
func TestPipelineEndWithheldTail(t *testing.T) {
	f := loadFixture(t)
	for _, c := range []endCase{
		{name: "the boundary access unit ends the stream", frames: 120, withhold: 4, withholdRung: -1},
		{name: "a partial part follows the boundary", frames: 128, withhold: 4, withholdRung: -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := endRoom(t, f, c)
			cleanEnd(t, res, 120)
			if !res.tail || res.tailAU != 120 {
				t.Errorf("tail withheld %v from access unit %d, want from 120", res.tail, res.tailAU)
			}
		})
	}
}

// TestPipelineActiveBoundaryViolationFails proves the omission stays fatal while the stream
// runs: with the same key unit missing, an access unit at the next boundary fails the pipeline
// with the reason, whether it arrives while the room runs or with the frames EOS flushes;
// nothing from the boundary on is published, and the end is not verified. A key unit missing
// on one rung only is caught as the rungs disagreeing, before either publishes different media.
func TestPipelineActiveBoundaryViolationFails(t *testing.T) {
	f := loadFixture(t)
	for _, c := range []struct {
		endCase
		reason string
	}{
		{endCase{name: "the stream reaches the next boundary", frames: 131, withhold: 4, withholdRung: -1}, "not an IDR, and the stream went on to access unit 130"},
		{endCase{name: "the stream runs on", frames: 240, withhold: 4, withholdRung: -1}, "not an IDR, and the stream went on to access unit 130"},
		{endCase{name: "the tail reaches the next boundary at EOS", frames: 130, withhold: 4, withholdRung: -1}, "not an IDR, and the stream went on to access unit 130"},
		{endCase{name: "one rung lacks the key unit", frames: 150, withhold: 4, withholdRung: 2}, "another rung"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := endRoom(t, f, c.endCase)
			if res.verified || res.cause == nil {
				t.Fatalf("verified=%v cause=%v, want a contained failure", res.verified, res.cause)
			}
			if !strings.Contains(res.cause.Error(), c.reason) {
				t.Errorf("cause %q, want the pipeline's reason mentioning %q", res.cause, c.reason)
			}
			for trk := 0; trk < rungs; trk++ {
				for j, h := range res.tracks[trk].chunks {
					if h.firstAU >= 120 {
						t.Errorf("%s part %d at access unit %d was published past the boundary", trackLabels[trk], j, h.firstAU)
					}
					if h.keyUnit == 1 != (j%3 == 0) || h.fragmentStart != (j%6 == 0) || h.payload != 10 {
						t.Errorf("%s part %d: %+v off the schedule", trackLabels[trk], j, h)
					}
				}
			}
		})
	}
}
