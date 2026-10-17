//go:build linux

package worker

// Runner: the worker role's continuous solo production path. One host's H.264 and Opus tracks
// are subscribed from the configured LiveKit room (ingest_linux.go), decoded, encoded into the
// solo ladder and one AAC mix and cut into CMAF parts by the native pipeline
// (pipeline_cgo_linux.go), and every init, phases and part is published through the IPC2
// Publisher with no duration cap. The presentation is written into the epoch directory the
// supervisor created, before the first part; the room's clean end (the host leaving, the room
// closing or a stop request) drains the pipeline and publishes a verified terminal; any other
// failure publishes an unverified terminal, or none when the publisher is already poisoned.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"golang.org/x/sys/unix"
)

// Outputs of the solo pipeline are the ipc tracks: the three rungs, then the audio mix. Inputs
// are the host's video and audio.
const (
	inputVideo = 0
	inputAudio = 1
	rungs      = 3
	trackAudio = ipc.TrackAudioA

	pollStep    = 100 * time.Millisecond // bound of every native wait, so cancellation is noticed
	drainGrace  = 5 * time.Second        // appsrc EOS to bus EOS with the readers drained
	readerGrace = 5 * time.Second        // pipeline stopped to readers returned, before the publisher is cancelled
	originWait  = 10 * time.Second       // first part to both clock origins known
	terminalMax = 5 * time.Second

	audioCodecs     = "mp4a.40.2"
	videoQueueBytes = 2 << 20 // appsrc backlog over which a video sample is rejected
	audioQueueBytes = 256 << 10
	segmentLogEvery = 30 // segments between progress lines per track
)

// ladder is the solo ladder, hi first; the rung index is the ipc track.
var ladder = [rungs]struct {
	name          string
	width, height int
	kbps          int
}{{"hi", 720, 1280, 2500}, {"mid", 576, 1024, 1600}, {"lo", 360, 640, 800}}

var trackLabels = [rungs + 1]string{"vhi", "vmid", "vlo", "a"}

// header is one HEADER buffer of a pulled sample: the initialization segment or a chunk with the
// payload that followed it. Its bytes are data[off:off+hdrLen+payLen] of the pull.
type header struct {
	isInit              bool
	pts, dts, duration  int64 // ns as stamped, -1 unknown
	sequence            uint64
	fragmentStart       bool
	payload             int
	firstAU             int64
	keyUnit             int // 1 an IDR picture, 0 another picture, -1 not derived
	off, hdrLen, payLen uint64
}

// pulled is one appsink sample copied into Go memory with the phases as recorded when it was
// described: P and, for the audio output, A.
type pulled struct {
	data                   []byte
	headers                []header
	stray, overflow        int
	codecData              []byte // with a video initialization segment: the parser's codec_data
	phaseVideo, phaseAudio int64
	haveVideo, haveAudio   bool
}

// origins are the clock origins of the presentation: the raw PTS and running time of the first
// video access unit and of the first audio access unit.
type origins struct {
	videoPTS, videoRT, audioPTS, audioRT int64
	haveVideo, haveAudio                 bool
}

// RunnerOptions configure Run. Room is the LiveKit room to join and RoomID the room identity the
// presentation's slot names, which the supervisor serves the playlists under; they are distinct
// identities that may share a value. EpochDir maps the epoch the supervisor assigns to the
// directory the presentation is written into.
type RunnerOptions struct {
	Socket        string
	Run           string
	Source        string
	Room          string
	RoomID        string
	Host          string
	URL           string
	APIKey        string
	APISecret     string
	SupervisorUID int
	Config        ipc.Config
	EpochDir      func(epoch uint64) string
	Log           *log.Logger
}

type trackState struct {
	msn, index int
	parts      int
	phases     bool
}

type runner struct {
	o      RunnerOptions
	log    *log.Logger
	pub    *Publisher
	pipe   *pipeline
	origin time.Time
	utc0   int64 // UTC of pipeline running time zero
	uncert int64

	pubCtx    context.Context
	pubCancel context.CancelFunc

	mu        sync.Mutex
	epoch     uint64
	presented bool
	tracks    [rungs + 1]trackState
}

// Run runs the worker until the room ends cleanly (nil) or the media fails.
func Run(ctx context.Context, o RunnerOptions) error {
	if o.Log == nil {
		o.Log = log.New(os.Stderr, "[worker] ", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	}
	if err := checkRunnerOptions(o); err != nil {
		return err
	}
	pub, err := NewPublisher(PublisherOptions{Socket: o.Socket, Run: o.Run, Source: o.Source, Config: o.Config, SupervisorUID: o.SupervisorUID})
	if err != nil {
		return err
	}
	defer pub.Close()
	pipe, err := openPipeline()
	if err != nil {
		return err
	}
	defer pipe.close()
	r := &runner{o: o, log: o.Log, pub: pub, pipe: pipe, origin: time.Now()}
	r.pubCtx, r.pubCancel = context.WithCancel(context.Background())
	defer r.pubCancel()
	if err := pipe.start(); err != nil {
		return fmt.Errorf("worker: pipeline: %w", err)
	}
	if err := r.measureOrigin(); err != nil {
		return err
	}
	r.log.Printf("pipeline playing for run %q source %q: LiveKit room %q host %q presented as room %q, pipeline utc origin %d ns ± %d ns", o.Run, o.Source, o.Room, o.Host, o.RoomID, r.utc0, r.uncert)
	return r.generate(ctx)
}

func checkRunnerOptions(o RunnerOptions) error {
	switch {
	case !filepath.IsAbs(o.Socket):
		return fmt.Errorf("%w: socket %q is not absolute", ErrInvalidOptions, o.Socket)
	case o.Room == "" || o.RoomID == "" || o.Host == "" || o.URL == "":
		return fmt.Errorf("%w: room, room id, host and url are required", ErrInvalidOptions)
	case o.APIKey == "" || o.APISecret == "":
		return fmt.Errorf("%w: LiveKit api key and secret are required", ErrInvalidOptions)
	case o.EpochDir == nil:
		return fmt.Errorf("%w: no epoch directory mapping", ErrInvalidOptions)
	case o.Config.Mode != ipc.ModeSolo || o.Config.Codec != ipc.CodecH264:
		return fmt.Errorf("%w: config %+v; this worker produces solo h264", ErrInvalidOptions, o.Config)
	}
	return nil
}

// measureOrigin fixes the UTC of pipeline running time zero from the wall clock read around one
// running-time read; the stated uncertainty is half that read's span plus a millisecond, since
// nothing here knows the host clock's discipline.
func (r *runner) measureOrigin() error {
	t1 := time.Now()
	rt, err := r.pipe.runningTime()
	t2 := time.Now()
	if err != nil {
		return fmt.Errorf("worker: pipeline clock: %w", err)
	}
	span := t2.Sub(t1).Nanoseconds()
	r.utc0 = t1.UnixNano() + span/2 - rt.Nanoseconds()
	r.uncert = span/2 + int64(time.Millisecond)
	if r.utc0 <= 0 {
		return fmt.Errorf("worker: pipeline utc origin %d is not positive", r.utc0)
	}
	return nil
}

// generate runs the room from the join to its end: the feeders, the readers and the bus are
// serviced until the room ends cleanly or the first failure, then everything is torn down in
// order and the terminal is published.
func (r *runner) generate(ctx context.Context) error {
	in := newIngest(ingestOptions{URL: r.o.URL, Room: r.o.Room, Host: r.o.Host, APIKey: r.o.APIKey, APISecret: r.o.APISecret,
		Identity: fmt.Sprintf("connects-egress-%s-%d", r.o.Source, os.Getpid())}, r.pipe, r.log)
	if err := in.prepare(); err != nil {
		return err
	}
	readerDone := make(chan error, rungs+1)
	for trk := 0; trk <= rungs; trk++ {
		go func() { readerDone <- r.read(trk) }()
	}
	busDone, quit := make(chan error, 1), make(chan struct{})
	go func() { busDone <- pollBus(r.pipe, quit) }()
	joining := in.startJoin()
	readers, busLive := rungs+1, true
	var cause error
	clean, endReason := false, ""
	for cause == nil && !clean {
		select {
		case <-ctx.Done():
			clean, endReason = true, "stop requested"
		case err := <-in.joined:
			joining--
			if err != nil {
				cause = err
			} else {
				r.log.Printf("joined room %q, waiting for host %q", r.o.Room, r.o.Host)
			}
		case reason := <-in.ended:
			clean, endReason = true, reason
		case <-in.ctx.Done():
			cause = context.Cause(in.ctx)
		case err := <-busDone:
			busLive = false
			if cause = err; err == nil {
				cause = errors.New("the pipeline reached EOS before the room ended")
			}
		case err := <-readerDone:
			readers--
			if cause = err; err == nil {
				cause = errors.New("a reader drained before EOS was sent")
			}
		}
	}
	if clean {
		r.log.Printf("room ending: %s", endReason)
		in.stopFeeding()
		cause = r.pipe.endOfStream()
		drain := time.NewTimer(drainGrace)
		busEOS := false
		for cause == nil && !(busEOS && readers == 0) {
			select {
			case err := <-busDone:
				busLive = false
				if err != nil {
					cause = fmt.Errorf("pipeline error: %w", err)
				} else {
					busEOS = true
				}
			case err := <-readerDone:
				readers--
				if err != nil {
					cause = err
				}
			case <-drain.C:
				cause = fmt.Errorf("EOS did not drain within %v", drainGrace)
			}
		}
		drain.Stop()
	}
	// Every terminal path: no new tracks, no more reads, NULL, every native user joined.
	in.closeAdmissions()
	if cause != nil {
		in.fatal(cause)
	}
	in.cancel(errEnded)
	close(quit)
	r.pipe.stop()
	in.feeders.Wait()
	grace := time.NewTimer(readerGrace)
	for readers > 0 {
		select {
		case err := <-readerDone:
			readers--
			if err != nil && cause == nil {
				cause = err
			}
		case <-grace.C:
			r.pubCancel() // a reader blocked in Publish returns poisoned; nothing more could be published anyway
		}
	}
	grace.Stop()
	if busLive {
		<-busDone
	}
	for ; joining > 0; joining-- {
		<-in.joined // the cancellation has closed their sockets
	}
	in.disconnect()
	verified, reason := cause == nil, endReason
	if verified {
		if why := r.incomplete(); why != "" {
			verified, reason = false, why
		}
	} else {
		reason = cause.Error()
	}
	if err := r.terminal(verified, reason); err != nil {
		r.log.Printf("terminal not published: %v", err)
		if cause == nil {
			cause = err
		}
	}
	r.mu.Lock()
	tracks := r.tracks
	r.mu.Unlock()
	r.log.Printf("media ended: verified=%v reason=%q parts=%v feeders=%s elapsed=%v", verified, reason,
		[]int{tracks[0].parts, tracks[1].parts, tracks[2].parts, tracks[3].parts}, in.stats(), time.Since(r.origin).Round(time.Millisecond))
	return cause
}

// read drains the appsink of track into the publisher until it reports EOS: every init, the
// presentation before the first part, the audio phases before the first audio part, then every
// chunk as the next part of its track.
func (r *runner) read(trk int) error {
	for {
		l, eos, err := r.pipe.pull(trk, pollStep)
		if err != nil {
			return fmt.Errorf("%s reader: %w", trackLabels[trk], err)
		}
		if eos {
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
				if err := r.publishInit(trk, l.data[h.off:h.off+h.hdrLen], l.codecData); err != nil {
					return err
				}
				continue
			}
			if trk < trackAudio && h.keyUnit < 0 {
				return fmt.Errorf("%s reader: the chunk at access unit %d was emitted before that access unit was classified", trackLabels[trk], h.firstAU)
			}
			if err := r.ensurePresentation(); err != nil {
				return err
			}
			if trk == trackAudio {
				if err := r.publishPhases(l); err != nil {
					return err
				}
			}
			if err := r.publishPart(trk, h, l.data[h.off:h.off+h.hdrLen+h.payLen]); err != nil {
				return err
			}
		}
	}
}

// stamp sets the publication clocks of e.
func (r *runner) stamp(e *ipc.Entry) {
	e.PublishedNS, e.PublishedMonoNS = time.Since(r.origin).Nanoseconds(), monotonicNow()
}

func (r *runner) publishInit(trk int, data, codecData []byte) error {
	codecs := audioCodecs
	if trk < trackAudio {
		var err error
		if codecs, err = videoCodecs(codecData); err != nil {
			return fmt.Errorf("%s init: %w", trackLabels[trk], err)
		}
	}
	e := ipc.Entry{Kind: ipc.KindInit, Track: trk, Codecs: codecs}
	r.stamp(&e)
	out, err := r.pub.Publish(r.pubCtx, e, data)
	if err != nil {
		return fmt.Errorf("%s init: %w", trackLabels[trk], err)
	}
	r.mu.Lock()
	if r.epoch == 0 {
		r.epoch = out.Epoch
	}
	r.mu.Unlock()
	r.log.Printf("%s init published: epoch %d seq %d, %d bytes, codecs %s", trackLabels[trk], out.Epoch, out.Seq, len(data), codecs)
	return nil
}

// ensurePresentation writes presentation.json once, before the first part of any track, once
// the epoch is known from an init's acknowledgement and both clock origins are recorded.
func (r *runner) ensurePresentation() error {
	r.mu.Lock()
	done, epoch := r.presented, r.epoch
	r.mu.Unlock()
	if done {
		return nil
	}
	if epoch == 0 {
		return errors.New("worker: a part preceded every init acknowledgement")
	}
	var og origins
	deadline := time.Now().Add(originWait)
	for {
		og = r.pipe.origins()
		if og.haveVideo && og.haveAudio {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("worker: clock origins not recorded within %v (video %v, audio %v)", originWait, og.haveVideo, og.haveAudio)
		}
		if err := r.pubCtx.Err(); err != nil {
			return fmt.Errorf("worker: cancelled while waiting for the clock origins: %w", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if og.videoPTS < 0 || og.videoRT < 0 || og.audioPTS < 0 || og.audioRT < 0 {
		return fmt.Errorf("worker: clock origins without timestamps: %+v", og)
	}
	cfg := r.o.Config
	clocks := make([]ipc.TrackClock, cfg.Tracks)
	for i := range clocks {
		clocks[i] = ipc.TrackClock{Track: i, RawPTSOriginNS: og.videoPTS, RunningTimeOriginNS: og.videoRT}
	}
	clocks[trackAudio] = ipc.TrackClock{Track: trackAudio, RawPTSOriginNS: og.audioPTS, RunningTimeOriginNS: og.audioRT}
	pres := ipc.Presentation{Version: ipc.PresentationVersion, Run: r.o.Run, Source: r.o.Source, Epoch: epoch, Config: cfg,
		PipelineUTCNS: r.utc0, UncertaintyNS: r.uncert, CropAnchor: 0.5, Clocks: clocks,
		Slots: []ipc.PresentationSlot{{HostID: r.o.Host, RoomID: r.o.RoomID}}}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.presented {
		return nil
	}
	dir := r.o.EpochDir(epoch)
	if err := WritePresentation(dir, pres); err != nil {
		return fmt.Errorf("worker: presentation: %w", err)
	}
	r.presented = true
	r.log.Printf("presentation written to %s: video origin pts %d rt %d, audio origin pts %d rt %d", dir, og.videoPTS, og.videoRT, og.audioPTS, og.audioRT)
	return nil
}

// publishPhases publishes P and A on the audio track once, before its first part.
func (r *runner) publishPhases(l *pulled) error {
	r.mu.Lock()
	done := r.tracks[trackAudio].phases
	r.mu.Unlock()
	if done {
		return nil
	}
	if !l.haveVideo || !l.haveAudio {
		return errors.New("worker: an audio chunk was emitted before both phases were recorded")
	}
	e := ipc.Entry{Kind: ipc.KindPhases, Track: trackAudio, PhaseVideo: l.phaseVideo, PhaseAudio: l.phaseAudio}
	r.stamp(&e)
	out, err := r.pub.Publish(r.pubCtx, e, nil)
	if err != nil {
		return fmt.Errorf("phases: %w", err)
	}
	r.mu.Lock()
	r.tracks[trackAudio].phases = true
	r.mu.Unlock()
	r.log.Printf("phases published: seq %d, P %d ns, A %d ns", out.Seq, l.phaseVideo, l.phaseAudio)
	return nil
}

// publishPart publishes one chunk as the next part of its track: a fragment start opens the
// next segment. An audio chunk has no DTS; its PTS is used.
func (r *runner) publishPart(trk int, h header, data []byte) error {
	switch {
	case h.duration <= 0:
		return fmt.Errorf("%s: the chunk at access unit %d has duration %d ns", trackLabels[trk], h.firstAU, h.duration)
	case h.pts < 0:
		return fmt.Errorf("%s: the chunk at access unit %d has no PTS", trackLabels[trk], h.firstAU)
	}
	dts := h.dts
	if dts < 0 {
		dts = h.pts
	}
	r.mu.Lock()
	t := &r.tracks[trk]
	if h.fragmentStart {
		t.msn++
		t.index = 0
	} else if t.msn == 0 {
		r.mu.Unlock()
		return fmt.Errorf("%s: the first chunk is not a fragment start", trackLabels[trk])
	} else {
		t.index++
	}
	msn, index, parts := t.msn, t.index, t.parts
	r.mu.Unlock()
	e := ipc.Entry{Kind: ipc.KindPart, Track: trk, MSN: msn, Index: index, PTS: h.pts, DTS: dts, Duration: h.duration,
		Sequence: h.sequence, FragmentStart: h.fragmentStart, Independent: trk < trackAudio && h.keyUnit == 1,
		Payload: h.payload, FirstAU: int(h.firstAU)}
	r.stamp(&e)
	out, err := r.pub.Publish(r.pubCtx, e, data)
	if err != nil {
		return fmt.Errorf("%s part %d.%d: %w", trackLabels[trk], msn, index, err)
	}
	r.mu.Lock()
	t.parts++
	r.mu.Unlock()
	if parts == 0 || (h.fragmentStart && msn%segmentLogEvery == 0) {
		r.log.Printf("%s part %d.%d published: seq %d, pts %d ns, %d bytes, %d parts so far", trackLabels[trk], msn, index, out.Seq, h.pts, len(data), parts+1)
	}
	return nil
}

// incomplete names what a verified terminal would still need, "" when the ring would accept it.
func (r *runner) incomplete() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for trk, t := range r.tracks {
		if t.parts == 0 {
			return fmt.Sprintf("ended before %s published a part", trackLabels[trk])
		}
	}
	if !r.tracks[trackAudio].phases {
		return "ended before the audio phases were published"
	}
	return ""
}

// terminal publishes the worker's verdict with a bounded fresh context: the run is over either
// way, and a publisher poisoned by the deadline changes nothing further.
func (r *runner) terminal(verified bool, reason string) error {
	e := ipc.Entry{Kind: ipc.KindTerminal, Track: ipc.TrackTerminal, Verified: verified, Reason: reason}
	r.stamp(&e)
	ctx, cancel := context.WithTimeout(context.Background(), terminalMax)
	defer cancel()
	out, err := r.pub.Publish(ctx, e, nil)
	if err != nil {
		return err
	}
	r.log.Printf("terminal published: seq %d verified=%v", out.Seq, verified)
	return nil
}

// pollBus polls the bus until EOS (nil), an error, or quit.
func pollBus(p *pipeline, quit <-chan struct{}) error {
	for {
		select {
		case <-quit:
			return errors.New("bus polling ended")
		default:
		}
		eos, err := p.poll(pollStep)
		if err != nil {
			return err
		}
		if eos {
			return nil
		}
	}
}

// videoCodecs derives the RFC 6381 CODECS entry from the leading bytes of the avcC record the
// parser announced: profile, compatibility and level. Nothing is guessed.
func videoCodecs(d []byte) (string, error) {
	switch {
	case len(d) == 0:
		return "", errors.New("the video parser announced no codec_data")
	case d[0] != 1:
		return "", fmt.Errorf("codec_data has configurationVersion %d, want 1", d[0])
	case len(d) < 4:
		return "", fmt.Errorf("avcC holds %d bytes, want at least 4", len(d))
	}
	return fmt.Sprintf("avc1.%02x%02x%02x", d[1], d[2], d[3]), nil
}

// monotonicNow returns CLOCK_MONOTONIC in ns, the clock the supervisor's maintenance compares
// publication clocks against.
func monotonicNow() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		panic("worker: clock_gettime: " + err.Error())
	}
	return ts.Nano()
}
