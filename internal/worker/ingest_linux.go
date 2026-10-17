//go:build linux

package worker

// Ingest: one LiveKit room joined with a token minted here and auto-subscribe off, the host's
// first H.264 and first Opus publication subscribed, and one feeder per track that rebuilds
// samples with Pion's samplebuilder and pushes the compressed bytes into its appsrc with
// wrap-safe RTP deltas. Video admits nothing before its first SPS or IDR, asking for a key
// unit again every second meanwhile; it opens the timeline at the pipeline's running time.
// Audio admits nothing before the video phase P exists and anchors its first admitted sample to
// P itself: arrival alignment of the tracks on one clock, not capture synchronization. What
// arrives earlier is discarded and counted. The host leaving or the room closing is the room's
// clean end; a lost connection, a foreign codec or a rejected sample is a failure.
//
// The SDK's first WebSocket read cannot be interrupted by context cancellation alone, so the
// dialer it uses is replaced once, before any SDK worker starts, by a clone whose sockets the
// run's cancellation closes.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/auth"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
)

const (
	maxBuffering = 150 * time.Millisecond // samplebuilder bound, in RTP time
	readTimeout  = 200 * time.Millisecond // bounds each RTP read so cancellation is noticed
	pliInterval  = time.Second            // key-unit requests while no decodable sample has arrived
)

var errEnded = errors.New("worker: generation ended")

type ingestOptions struct {
	URL, Room, Host, Identity, APIKey, APISecret string
}

// media is one track of the host, feeding one appsrc input.
type media struct {
	input     int
	name      string
	mime      string
	kind      lksdk.TrackKind
	clockRate uint32
	maxLate   uint16
	depack    func() rtp.Depacketizer
	limit     int
	pushed    atomic.Uint64
	discarded atomic.Uint64

	// guarded by ingest.mu
	subscribed bool
	reading    bool
}

func newMedia(input int, video bool) *media {
	m := &media{input: input, name: "audio", mime: "audio/opus", kind: lksdk.TrackKindAudio, clockRate: 48000, maxLate: 128,
		depack: func() rtp.Depacketizer { return &codecs.OpusPacket{} }, limit: audioQueueBytes}
	if video {
		m.name, m.mime, m.kind, m.clockRate, m.maxLate, m.limit = "video", "video/H264", lksdk.TrackKindVideo, 90000, 512, videoQueueBytes
		m.depack = func() rtp.Depacketizer { return &codecs.H264Packet{} }
	}
	return m
}

type ingest struct {
	o      ingestOptions
	ctx    context.Context
	cancel context.CancelCauseFunc
	pipe   *pipeline
	log    *log.Logger
	lk     *lksdk.Room
	token  string
	video  *media
	audio  *media
	joined chan error  // the join worker's result; a slot for it, so no send blocks
	ended  chan string // the room's clean end, once

	mu      sync.Mutex // guards closed, the media fields noted above and every feeders.Add
	closed  bool       // no new track work is admitted
	stopped atomic.Bool
	ending  atomic.Bool
	endOnce sync.Once
	feeders sync.WaitGroup
}

func newIngest(o ingestOptions, pipe *pipeline, log *log.Logger) *ingest {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &ingest{o: o, ctx: ctx, cancel: cancel, pipe: pipe, log: log, video: newMedia(inputVideo, true), audio: newMedia(inputAudio, false),
		joined: make(chan error, 1), ended: make(chan string, 1)}
}

// fatal ends the run with err unless the run is already ending; the first cause is kept.
func (in *ingest) fatal(err error) {
	if in.ctx.Err() == nil {
		in.cancel(err)
	}
}

// end reports the room's clean end once.
func (in *ingest) end(reason string) {
	in.endOnce.Do(func() {
		in.ending.Store(true)
		in.ended <- reason
	})
}

// closeAdmissions stops every new subscription and feeder; feeders.Wait is final afterwards.
func (in *ingest) closeAdmissions() {
	in.mu.Lock()
	in.closed = true
	in.mu.Unlock()
}

func (in *ingest) isClosed() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.closed
}

// stopFeeding closes admissions, stops the feeders and waits for them, so EOS can follow.
func (in *ingest) stopFeeding() {
	in.closeAdmissions()
	in.stopped.Store(true)
	in.feeders.Wait()
}

// prepare mints the token, installs the dialer and constructs the room, all synchronously;
// nothing talks to the server until startJoin.
func (in *ingest) prepare() error {
	no, yes := false, true
	grant := &auth.VideoGrant{RoomJoin: true, Room: in.o.Room, CanPublish: &no, CanSubscribe: &yes, Hidden: true}
	token, err := auth.NewAccessToken(in.o.APIKey, in.o.APISecret).SetVideoGrant(grant).SetIdentity(in.o.Identity).ToJWT()
	if err != nil {
		return fmt.Errorf("worker: token for %s: %w", in.o.Room, err)
	}
	in.token = token
	in.installDialer()
	cb := &lksdk.RoomCallback{
		OnDisconnectedWithReason: func(reason lksdk.DisconnectionReason) {
			if in.isClosed() {
				return // our own disconnect at teardown is expected
			}
			switch reason {
			case lksdk.RoomClosed, lksdk.ParticipantRemoved, lksdk.LeaveRequested:
				in.end("room " + in.o.Room + ": " + string(reason))
			default:
				in.fatal(fmt.Errorf("disconnected from room %s: %s", in.o.Room, reason))
			}
		},
		OnParticipantDisconnected: func(rp *lksdk.RemoteParticipant) {
			if rp.Identity() == in.o.Host && !in.isClosed() {
				in.end("host " + in.o.Host + " left room " + in.o.Room)
			}
		},
		ParticipantCallback: lksdk.ParticipantCallback{
			OnTrackPublished: func(pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
				if rp.Identity() == in.o.Host {
					in.subscribe(pub)
				}
			},
			OnTrackSubscribed: func(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
				in.onSubscribed(track, rp)
			},
		},
	}
	in.lk = lksdk.NewRoom(cb)
	return nil
}

// installDialer replaces the package dialer the SDK uses for its signal WebSockets with a
// process-private clone, once, before any SDK worker starts. Every socket it opens is closed
// when the run's context ends, so a join blocked in its first read returns.
func (in *ingest) installDialer() {
	d := *websocket.DefaultDialer
	dial := d.NetDialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	d.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &runConn{Conn: conn, stop: context.AfterFunc(in.ctx, func() { conn.Close() })}, nil
	}
	websocket.DefaultDialer = &d
}

// runConn is a socket the run's cancellation closes; closing it normally drops that registration.
type runConn struct {
	net.Conn
	stop func() bool
}

func (c *runConn) Close() error {
	c.stop()
	return c.Conn.Close()
}

// startJoin launches the tracked join worker and returns its count, 1: the SDK join without
// auto-subscribe, then the subscriptions of tracks published before the join. The result
// arrives on joined; the teardown collects it before anything is freed.
func (in *ingest) startJoin() int {
	go func() { in.joined <- in.joinRoom() }()
	return 1
}

func (in *ingest) joinRoom() error {
	if err := in.lk.JoinWithContextAndToken(in.ctx, in.o.URL, in.token, lksdk.WithAutoSubscribe(false)); err != nil {
		return fmt.Errorf("worker: join %s: %w", in.o.Room, err)
	}
	if rp := in.lk.GetParticipantByIdentity(in.o.Host); rp != nil {
		for _, p := range rp.TrackPublications() {
			if pub, ok := p.(*lksdk.RemoteTrackPublication); ok {
				in.subscribe(pub)
			}
		}
	}
	return nil
}

// disconnect leaves the room; admissions are closed first, so the callbacks stay quiet.
func (in *ingest) disconnect() {
	if in.lk != nil {
		in.lk.Disconnect()
	}
}

// subscribe requests the host's first video and first audio publication, once each. The codec
// is checked when the track arrives: a publication's mime type may not be known yet when it is
// announced.
func (in *ingest) subscribe(pub *lksdk.RemoteTrackPublication) {
	m := in.video
	switch pub.Kind() {
	case in.video.kind:
	case in.audio.kind:
		m = in.audio
	default:
		return
	}
	in.mu.Lock()
	skip := in.closed || m.subscribed
	if !skip {
		m.subscribed = true
	}
	in.mu.Unlock()
	if skip {
		return
	}
	if err := pub.SetSubscribed(true); err != nil {
		in.fatal(fmt.Errorf("subscribe %s %s: %w", m.name, pub.SID(), err))
	}
}

// onSubscribed runs on an SDK goroutine: it only admits the track, requests a key unit and
// starts the feeder, under the mutex that closeAdmissions takes.
func (in *ingest) onSubscribed(track *webrtc.TrackRemote, rp *lksdk.RemoteParticipant) {
	if rp.Identity() != in.o.Host {
		return
	}
	m := in.video
	if track.Kind() == webrtc.RTPCodecTypeAudio {
		m = in.audio
	}
	codec := track.Codec()
	if !strings.EqualFold(codec.MimeType, m.mime) {
		in.fatal(fmt.Errorf("host %s %s track is %s, want %s", in.o.Host, m.name, codec.MimeType, m.mime))
		return
	}
	in.mu.Lock()
	admit := !in.closed && !m.reading
	if admit {
		m.reading = true
		in.feeders.Add(1)
	}
	in.mu.Unlock()
	if !admit {
		return
	}
	in.log.Printf("subscribed to %s %s (%s, %d Hz)", in.o.Host, m.name, codec.MimeType, codec.ClockRate)
	if m == in.video {
		rp.WritePLI(track.SSRC())
	}
	go in.feed(m, rp, track)
}

// feed runs one feeder to its end: a failure ends the run; a stop or the room's end is quiet.
func (in *ingest) feed(m *media, rp *lksdk.RemoteParticipant, track *webrtc.TrackRemote) {
	defer in.feeders.Done()
	if err := in.feedTrack(m, rp, track); err != nil {
		in.fatal(err)
	}
}

// quiet reports that the feeders are expected to stop: the run is cancelled, stopped or ending.
func (in *ingest) quiet() bool {
	return in.ctx.Err() != nil || in.stopped.Load() || in.ending.Load()
}

// feedTrack turns the RTP packets of one track into encoded samples and pushes them until the
// run ends, stops or the room ends.
func (in *ingest) feedTrack(m *media, rp *lksdk.RemoteParticipant, track *webrtc.TrackRemote) error {
	rate := track.Codec().ClockRate
	if rate == 0 {
		rate = m.clockRate
	}
	sb := samplebuilder.New(m.maxLate, m.depack(), rate, samplebuilder.WithMaxTimeDelay(maxBuffering))
	clk := rtpClock{rate: rate}
	video := m.kind == lksdk.TrackKindVideo
	waitKey := video // nothing is fed before the first SPS/IDR
	placed := false
	var offset time.Duration
	lastPLI := time.Now()
	for !in.quiet() {
		if waitKey && time.Since(lastPLI) >= pliInterval {
			rp.WritePLI(track.SSRC())
			lastPLI = time.Now()
		}
		if err := track.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return fmt.Errorf("%s read deadline: %w", m.name, err)
		}
		pkt, _, err := track.ReadRTP()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			if in.quiet() {
				return nil
			}
			return fmt.Errorf("%s read: %w", m.name, err)
		}
		sb.Push(pkt)
		for s := sb.Pop(); s != nil; s = sb.Pop() {
			if waitKey && !startsDecoding(s.Data) {
				m.discarded.Add(1)
				continue
			}
			waitKey = false
			if !placed {
				if video {
					if offset, err = in.pipe.runningTime(); err != nil {
						return fmt.Errorf("%s timeline: %w", m.name, err)
					}
				} else {
					vphase, _, havePhase, _ := in.pipe.phases()
					if !havePhase { // audio joins only once P exists
						m.discarded.Add(1)
						continue
					}
					offset = time.Duration(vphase) // and is anchored to P itself: arrival alignment, not lip-sync
				}
				placed = true
				in.log.Printf("%s admitted at running time %v after %d discarded samples", m.name, offset, m.discarded.Load())
			}
			pts := offset + clk.pts(s.PacketTimestamp)
			dropped, err := in.pipe.push(m.input, s.Data, pts, s.Duration)
			switch {
			case err != nil:
				return fmt.Errorf("%s push: %w", m.name, err)
			case dropped:
				return fmt.Errorf("%s backlog over %d bytes: a sample was rejected", m.name, m.limit)
			}
			m.pushed.Add(1)
		}
	}
	return nil
}

// stats summarizes both feeders for the log.
func (in *ingest) stats() string {
	return fmt.Sprintf("video pushed=%d discarded=%d, audio pushed=%d discarded=%d",
		in.video.pushed.Load(), in.video.discarded.Load(), in.audio.pushed.Load(), in.audio.discarded.Load())
}

// rtpClock maps 32-bit RTP timestamps to a wrap-safe offset from the first one seen.
type rtpClock struct {
	rate    uint32
	started bool
	last    uint32
	ticks   int64
}

func (c *rtpClock) pts(ts uint32) time.Duration {
	if !c.started {
		c.started, c.last = true, ts
	}
	c.ticks += int64(int32(ts - c.last))
	c.last = ts
	if c.ticks < 0 {
		return 0
	}
	rate := int64(c.rate)
	return time.Duration(c.ticks/rate)*time.Second + time.Duration(c.ticks%rate)*time.Second/time.Duration(rate)
}

// startsDecoding reports whether an Annex-B chunk holds an SPS or an IDR slice, the earliest
// point at which a decoder can start without errors.
func startsDecoding(b []byte) bool {
	for i := 0; i+3 < len(b); i++ {
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 1 {
			if t := b[i+3] & 0x1f; t == 5 || t == 7 {
				return true
			}
			i += 2
		}
	}
	return false
}
