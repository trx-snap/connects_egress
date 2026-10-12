//go:build linux

package supervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/worker"
)

// §7.5 timing of the test media: six 0.333 s video parts and six 0.320 s audio parts per segment.
const (
	otVideoPart  = int64(333_333_333)
	otAudioPart  = int64(320_000_000)
	otParts      = 6
	otBaseMono   = int64(1_000_000_000)
	otOutBase    = "/media/run-switch-out/src-switch-out/1/"
	otInBase     = "/media/run-switch-in/src-switch-in/1/"
	otWaitLimit  = 5 * time.Second
	otAtlas1v1   = `X-SLOTS="0:host_a:room_a:0,0,0.5,1;1:host_b:room_b:0.5,0,0.5,1",X-CROP-ANCHOR=0.4`
	otAtlasClass = `CLASS="com.connectsapp.atlas"`
)

// otPublish publishes part idx of source segment msn on every track of pub, video independent at
// every GOP of three parts; the part bytes are switchPublishPart's 64 bytes of 0x50+track.
func otPublish(t *testing.T, pub *worker.Publisher, msn, idx int) {
	t.Helper()
	for trk := 0; trk < ipc.Tracks1v1; trk++ {
		dur, payload := otVideoPart, 10
		if trk >= ipc.TrackAudioA {
			dur, payload = otAudioPart, 15
		}
		k := (msn-1)*otParts + idx
		pts := int64(k) * dur
		switchPublishPart(t, pub, trk, msn, idx, uint64(msn), k*payload, pts, pts, dur, payload, idx == 0, trk < ipc.TrackAudioA && idx%3 == 0)
	}
}

// otOpen opens a timeline of room in a fresh directory.
func otOpen(t *testing.T, rig *switchTestRig, room string) *Timeline {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tl, err := OpenTimeline(ctx, TimelineOptions{Directory: pubTestTempDir(t), Room: room, Window: DefaultRoomWindow}, rig.resolver)
	if err != nil {
		t.Fatalf("OpenTimeline %s: %v", room, err)
	}
	return tl
}

// otWait returns the first timeline view satisfying pred, failing after otWaitLimit.
func otWait(t *testing.T, tl *Timeline, what string, pred func(TimelineSnapshot) bool) TimelineSnapshot {
	t.Helper()
	deadline := time.After(otWaitLimit)
	for {
		snap, ch, err := tl.Observe()
		if err != nil {
			t.Fatalf("Observe while waiting for %s: %v", what, err)
		}
		if pred(snap) {
			return snap
		}
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// otWaitGoroutines fails unless the goroutine count drops to limit within a few seconds.
func otWaitGoroutines(t *testing.T, limit int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > limit {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines, want at most %d", runtime.NumGoroutine(), limit)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func otRequest(o *Origin, method, target string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	o.ServeHTTP(rec, req)
	return rec
}

func otGet(t *testing.T, o *Origin, target string, want int) *httptest.ResponseRecorder {
	t.Helper()
	rec := otRequest(o, http.MethodGet, target)
	if rec.Code != want {
		t.Fatalf("GET %s = %d %q, want %d", target, rec.Code, rec.Body.String(), want)
	}
	return rec
}

func otContains(t *testing.T, body, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Fatalf("playlist lacks %q:\n%s", want, body)
	}
}

// otController returns a controller over rig's resolver with a fixed clock, and its origin.
func otController(t *testing.T, rig *switchTestRig, tl *Timeline) (*PlaybackController, *Origin) {
	t.Helper()
	c, err := NewPlaybackController(rig.resolver, func() int64 { return otBaseMono })
	if err != nil {
		t.Fatalf("NewPlaybackController: %v", err)
	}
	if err := c.Register(tl); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return c, NewOrigin(c)
}

func TestOriginMasterAndRenditionPlaylists(t *testing.T) {
	rig := newSwitchTestRig(t)
	defer rig.teardown()
	for idx := 0; idx < otParts; idx++ {
		otPublish(t, rig.pub1, 1, idx)
	}
	otPublish(t, rig.pub1, 2, 0) // closes segment 1
	rig.tl = otOpen(t, rig, "room_b")
	if err := rig.tl.Bind(rig.key1, 1); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	rig.tl2 = otOpen(t, rig, "room_a")
	if err := rig.tl2.Bind(rig.key1, 1); err != nil {
		t.Fatalf("Bind room_a: %v", err)
	}
	c, o := otController(t, rig, rig.tl)
	if err := c.Register(rig.tl2); err != nil {
		t.Fatalf("Register room_a: %v", err)
	}

	master := otGet(t, o, "/live/room_b/master.m3u8", http.StatusOK)
	if ct := master.Header().Get("Content-Type"); ct != playlistMIME {
		t.Fatalf("master Content-Type = %q", ct)
	}
	wantMaster := strings.Join([]string{
		"#EXTM3U", "#EXT-X-VERSION:9", "#EXT-X-INDEPENDENT-SEGMENTS",
		`#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="AAC",DEFAULT=YES,AUTOSELECT=YES,URI="a.m3u8"`,
		`#EXT-X-STREAM-INF:BANDWIDTH=2628000,CODECS="avc1.64001f,mp4a.40.2",RESOLUTION=720x1280,AUDIO="aud"`, "vhi.m3u8",
		`#EXT-X-STREAM-INF:BANDWIDTH=1728000,CODECS="avc1.64001f,mp4a.40.2",RESOLUTION=576x1024,AUDIO="aud"`, "vmid.m3u8",
		`#EXT-X-STREAM-INF:BANDWIDTH=928000,CODECS="avc1.64001f,mp4a.40.2",RESOLUTION=360x640,AUDIO="aud"`, "vlo.m3u8", ""}, "\n")
	if got := master.Body.String(); got != wantMaster {
		t.Fatalf("master playlist:\n%s\nwant:\n%s", got, wantMaster)
	}

	v := otOutBase + "vhi/"
	wantVHi := strings.Join([]string{
		"#EXTM3U", "#EXT-X-VERSION:9", "#EXT-X-TARGETDURATION:2",
		"#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=1.0,CAN-SKIP-UNTIL=12.0",
		"#EXT-X-PART-INF:PART-TARGET=0.334", "#EXT-X-MEDIA-SEQUENCE:1", "#EXT-X-DISCONTINUITY-SEQUENCE:0",
		`#EXT-X-DATERANGE:ID="atlas-1",` + otAtlasClass + `,START-DATE="2023-11-14T22:13:20.000Z",X-MODE="1v1",X-ATLAS-VERSION="1",` + otAtlas1v1,
		`#EXT-X-MAP:URI="` + v + `init.mp4"`,
		"#EXT-X-PROGRAM-DATE-TIME:2023-11-14T22:13:20.000Z",
		`#EXT-X-PART:DURATION=0.333,URI="` + v + `1.0.m4s",INDEPENDENT=YES`,
		`#EXT-X-PART:DURATION=0.333,URI="` + v + `1.1.m4s"`,
		`#EXT-X-PART:DURATION=0.333,URI="` + v + `1.2.m4s"`,
		`#EXT-X-PART:DURATION=0.333,URI="` + v + `1.3.m4s",INDEPENDENT=YES`,
		`#EXT-X-PART:DURATION=0.333,URI="` + v + `1.4.m4s"`,
		`#EXT-X-PART:DURATION=0.333,URI="` + v + `1.5.m4s"`,
		"#EXTINF:1.999,", v + "1.m4s",
		"#EXT-X-PROGRAM-DATE-TIME:2023-11-14T22:13:21.999Z",
		`#EXT-X-PART:DURATION=0.333,URI="` + v + `2.0.m4s",INDEPENDENT=YES`,
		`#EXT-X-PRELOAD-HINT:TYPE=PART,URI="` + v + `2.1.m4s"`,
		`#EXT-X-RENDITION-REPORT:URI="vmid.m3u8",LAST-MSN=2,LAST-PART=0`,
		`#EXT-X-RENDITION-REPORT:URI="vlo.m3u8",LAST-MSN=2,LAST-PART=0`,
		`#EXT-X-RENDITION-REPORT:URI="a.m3u8",LAST-MSN=2,LAST-PART=0`, ""}, "\n")
	vhi := otGet(t, o, "/live/room_b/vhi.m3u8", http.StatusOK)
	if got := vhi.Body.String(); got != wantVHi {
		t.Fatalf("vhi playlist:\n%s\nwant:\n%s", got, wantVHi)
	}
	if cc := vhi.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("playlist Cache-Control = %q", cc)
	}
	otContains(t, otGet(t, o, "/live/room_b/vlo.m3u8", http.StatusOK).Body.String(), `#EXT-X-MAP:URI="`+otOutBase+`vlo/init.mp4"`)

	// The stable a.m3u8 maps to the room's own source audio track: a1 for room_b, a0 for room_a.
	a := otGet(t, o, "/live/room_b/a.m3u8", http.StatusOK).Body.String()
	otContains(t, a, "#EXT-X-PART-INF:PART-TARGET=0.320\n")
	otContains(t, a, `#EXT-X-MAP:URI="`+otOutBase+`a1/init.mp4"`)
	otContains(t, a, `#EXT-X-PART:DURATION=0.320,URI="`+otOutBase+`a1/1.0.m4s"`+"\n")
	otContains(t, a, "#EXTINF:1.920,\n"+otOutBase+"a1/1.m4s\n#EXT-X-PROGRAM-DATE-TIME:2023-11-14T22:13:21.920Z\n")
	otContains(t, a, `#EXT-X-PRELOAD-HINT:TYPE=PART,URI="`+otOutBase+`a1/2.1.m4s"`)
	otContains(t, a, `#EXT-X-RENDITION-REPORT:URI="vhi.m3u8",LAST-MSN=2,LAST-PART=0`)
	if strings.Contains(a, "INDEPENDENT") || strings.Contains(a, "/a0/") {
		t.Fatalf("room_b audio playlist:\n%s", a)
	}
	aA := otGet(t, o, "/live/room_a/a.m3u8", http.StatusOK).Body.String()
	otContains(t, aA, `#EXT-X-MAP:URI="`+otOutBase+`a0/init.mp4"`)
	if strings.Contains(aA, "/a1/") {
		t.Fatalf("room_a audio playlist names a1:\n%s", aA)
	}
	// Video bytes are shared: both rooms name the same objects.
	otContains(t, otGet(t, o, "/live/room_a/vhi.m3u8", http.StatusOK).Body.String(), `URI="`+v+`1.0.m4s"`)

	// Routing and validation.
	for _, bad := range []struct {
		target string
		code   int
	}{
		{"/live/room_b/", http.StatusBadRequest},
		{"/live//vhi.m3u8", http.StatusBadRequest},
		{"/live/room_b/vhi.m3u8/", http.StatusBadRequest},
		{"/live/nope/master.m3u8", http.StatusNotFound},
		{"/live/room_b/v.m3u8", http.StatusNotFound},
		{"/live/room_b", http.StatusNotFound},
		{"/other", http.StatusNotFound},
		{otOutBase + "a2/init.mp4", http.StatusNotFound},
		{otOutBase + "vhi/init.m4s", http.StatusNotFound},
		{otOutBase + "vhi/01.m4s", http.StatusNotFound},
		{otOutBase + "vhi/1.-1.m4s", http.StatusNotFound},
		{"/media/run-switch-out/src-switch-out/0/vhi/init.mp4", http.StatusNotFound},
		{"/media/run-switch-out/src-switch-out/2/vhi/init.mp4", http.StatusNotFound},
	} {
		otGet(t, o, bad.target, bad.code)
	}
	if rec := otRequest(o, http.MethodPost, "/live/room_b/master.m3u8"); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST = %d Allow %q", rec.Code, rec.Header().Get("Allow"))
	}
	if got := mediaBase(EpochKey{Run: "run/x", Source: "src y", Epoch: 7}, ipc.TrackAudioB); got != "/media/run%2Fx/src%20y/7/a1/" {
		t.Fatalf("mediaBase = %q", got)
	}
	var got EpochKey
	stub, err := NewPlaybackController(func(k EpochKey) (*Source, error) { got = k; return nil, ErrLostEpoch }, nil)
	if err != nil {
		t.Fatalf("NewPlaybackController: %v", err)
	}
	otGet(t, NewOrigin(stub), "/media/run%2Fx/src%20y%25/7/vhi/init.mp4", http.StatusNotFound)
	if want := (EpochKey{Run: "run/x", Source: "src y%", Epoch: 7}); got != want {
		t.Fatalf("resolved key = %+v, want %+v", got, want)
	}
}

// otSwitch drives rig through a committed switch at the start of the outgoing source's segment 2:
// room MSN 1 is the outgoing source's segment 1, room MSN 2 the incoming's segment 1 and room MSN
// 3 the incoming's open segment 2. Refreshes are explicit and timed by now.
func otSwitch(t *testing.T, rig *switchTestRig, now *int64) {
	t.Helper()
	tick := func() int64 { *now += 10_000_000; return *now }
	for idx := 0; idx < otParts; idx++ {
		otPublish(t, rig.pub1, 1, idx)
	}
	rig.tl = otOpen(t, rig, "room_b")
	if err := rig.tl.Bind(rig.key1, 1); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := rig.tl.PlanSwitch(rig.key2, 1, tick()); err != nil {
		t.Fatalf("PlanSwitch: %v", err)
	}
	otPublish(t, rig.pub1, 2, 0) // beyond the cut: withheld, segment 1 closes, the hold begins
	if err := rig.tl.Refresh(tick()); err != nil {
		t.Fatalf("Refresh hold: %v", err)
	}
	snap, _, _ := rig.tl.Observe()
	if snap.Pending == nil || snap.Pending.HoldStartMonoNS == 0 {
		t.Fatalf("hold not recorded: %+v", snap.Pending)
	}
	otPublish(t, rig.pub2, 1, 0)
	if err := rig.tl.Refresh(tick()); err != nil {
		t.Fatalf("Refresh commit: %v", err)
	}
	for idx := 1; idx < otParts; idx++ {
		otPublish(t, rig.pub2, 1, idx)
	}
	otPublish(t, rig.pub2, 2, 0)
	if err := rig.tl.Refresh(tick()); err != nil {
		t.Fatalf("Refresh after switch: %v", err)
	}
}

func TestOriginMediaBytesAcrossSwitch(t *testing.T) {
	rig := newSwitchTestRig(t)
	defer rig.teardown()
	now := otBaseMono
	otSwitch(t, rig, &now)
	_, o := otController(t, rig, rig.tl)

	snap, _, err := rig.tl.Observe()
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if len(snap.Segments) != 3 || snap.Live == nil || snap.Live.Key != rig.key2 {
		t.Fatalf("after switch: %d segments, live %+v", len(snap.Segments), snap.Live)
	}
	for i, want := range []struct {
		key   EpochKey
		src   int
		disc  bool
		parts int
	}{{rig.key1, 1, false, otParts}, {rig.key2, 1, true, otParts}, {rig.key2, 2, false, 1}} {
		seg := snap.Segments[i]
		if seg.MSN != uint64(i+1) || seg.Key != want.key || seg.SourceMSN != want.src || seg.Discontinuity != want.disc || len(seg.Tracks[0].Parts) != want.parts {
			t.Fatalf("room segment %d = %+v", i+1, seg)
		}
	}

	v := otGet(t, o, "/live/room_b/vhi.m3u8", http.StatusOK).Body.String()
	otContains(t, v, "#EXT-X-MEDIA-SEQUENCE:1\n#EXT-X-DISCONTINUITY-SEQUENCE:0\n")
	otContains(t, v, `#EXT-X-DATERANGE:ID="atlas-1",`+otAtlasClass+`,START-DATE="2023-11-14T22:13:20.000Z",`)
	otContains(t, v, `#EXT-X-DATERANGE:ID="atlas-2",`+otAtlasClass+`,START-DATE="2023-11-14T22:13:22.000Z",X-MODE="1v1",X-ATLAS-VERSION="1",`+otAtlas1v1+"\n")
	otContains(t, v, "#EXTINF:1.999,\n"+otOutBase+"vhi/1.m4s\n#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\""+otInBase+"vhi/init.mp4\"\n"+
		"#EXT-X-PROGRAM-DATE-TIME:2023-11-14T22:13:22.000Z\n#EXT-X-PART:DURATION=0.333,URI=\""+otInBase+"vhi/1.0.m4s\",INDEPENDENT=YES\n")
	otContains(t, v, "#EXTINF:1.999,\n"+otInBase+"vhi/1.m4s\n#EXT-X-PROGRAM-DATE-TIME:2023-11-14T22:13:23.999Z\n")
	otContains(t, v, `#EXT-X-PRELOAD-HINT:TYPE=PART,URI="`+otInBase+`vhi/2.1.m4s"`)
	otContains(t, v, `#EXT-X-RENDITION-REPORT:URI="a.m3u8",LAST-MSN=3,LAST-PART=0`)
	if strings.Count(v, "#EXT-X-DISCONTINUITY\n") != 1 || strings.Count(v, "#EXT-X-MAP:") != 2 || strings.Count(v, "#EXT-X-PROGRAM-DATE-TIME:") != 3 {
		t.Fatalf("tag counts:\n%s", v)
	}
	a := otGet(t, o, "/live/room_b/a.m3u8", http.StatusOK).Body.String()
	otContains(t, a, "#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\""+otInBase+"a1/init.mp4\"\n")

	// Immutable media bytes, digest-checked against the entries the timeline exposes.
	check := func(target string, mime string, want []byte, sum string) {
		t.Helper()
		rec := otGet(t, o, target, http.StatusOK)
		if rec.Header().Get("Content-Type") != mime || rec.Header().Get("Cache-Control") != mediaCacheControl {
			t.Fatalf("%s headers: %v", target, rec.Header())
		}
		if !bytes.Equal(rec.Body.Bytes(), want) {
			t.Fatalf("%s: %d bytes %x, want %d bytes", target, rec.Body.Len(), rec.Body.Bytes(), len(want))
		}
		if got := sha256.Sum256(rec.Body.Bytes()); hex.EncodeToString(got[:]) != sum {
			t.Fatalf("%s: sha256 %x, want %s", target, got, sum)
		}
	}
	part := func(trk int) []byte { return bytes.Repeat([]byte{byte(0x50 + trk)}, 64) }
	sum := func(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
	out1, in1 := snap.Segments[0], snap.Segments[1]
	check(otOutBase+"vhi/init.mp4", "video/mp4", bytes.Repeat([]byte{0x20}, 16), snap.Bindings[0].Inits[0].SHA256)
	check(otOutBase+"a1/init.mp4", "audio/mp4", bytes.Repeat([]byte{0x24}, 16), snap.Bindings[0].Inits[3].SHA256)
	check(otOutBase+"vhi/1.3.m4s", "video/mp4", part(0), out1.Tracks[0].Parts[3].SHA256)
	check(otOutBase+"vlo/1.5.m4s", "video/mp4", part(2), out1.Tracks[2].Parts[5].SHA256)
	check(otOutBase+"a1/1.0.m4s", "audio/mp4", part(4), out1.Tracks[3].Parts[0].SHA256)
	check(otOutBase+"vhi/1.m4s", "video/mp4", bytes.Repeat(part(0), otParts), sum(bytes.Repeat(part(0), otParts)))
	check(otOutBase+"a1/1.m4s", "audio/mp4", bytes.Repeat(part(4), otParts), sum(bytes.Repeat(part(4), otParts)))
	check(otInBase+"vhi/init.mp4", "video/mp4", bytes.Repeat([]byte{0x20}, 16), snap.Live.Inits[0].SHA256)
	check(otInBase+"vhi/1.0.m4s", "video/mp4", part(0), in1.Tracks[0].Parts[0].SHA256)
	check(otInBase+"vhi/1.m4s", "video/mp4", bytes.Repeat(part(0), otParts), sum(bytes.Repeat(part(0), otParts)))
	check(otInBase+"vmid/2.0.m4s", "video/mp4", part(1), snap.Segments[2].Tracks[1].Parts[0].SHA256)
	// Withheld beyond the cut but published: immutable bytes stay servable. Unpublished and open: 404.
	check(otOutBase+"vhi/2.0.m4s", "video/mp4", part(0), sum(part(0)))
	otGet(t, o, otInBase+"vhi/2.m4s", http.StatusNotFound)
	otGet(t, o, otInBase+"vhi/2.1.m4s", http.StatusNotFound)
	otGet(t, o, otInBase+"vhi/3.m4s", http.StatusNotFound)
	otGet(t, o, otOutBase+"vhi/3.0.m4s", http.StatusNotFound)

	head := otRequest(o, http.MethodHead, otOutBase+"vhi/init.mp4")
	if head.Code != http.StatusOK || head.Header().Get("Content-Length") != "16" || head.Body.Len() != 0 {
		t.Fatalf("HEAD = %d, Content-Length %q, %d body bytes", head.Code, head.Header().Get("Content-Length"), head.Body.Len())
	}
	rng := otRequest(o, http.MethodGet, otOutBase+"vhi/1.m4s", "Range", "bytes=64-127")
	if rng.Code != http.StatusPartialContent || !bytes.Equal(rng.Body.Bytes(), part(0)) || rng.Header().Get("Content-Range") != "bytes 64-127/384" {
		t.Fatalf("range = %d %q %d bytes", rng.Code, rng.Header().Get("Content-Range"), rng.Body.Len())
	}
}

// otWriter records whether anything was written.
type otWriter struct {
	http.ResponseWriter
	wrote atomic.Bool
}

func (w *otWriter) WriteHeader(code int) {
	w.wrote.Store(true)
	w.ResponseWriter.WriteHeader(code)
}

func (w *otWriter) Write(b []byte) (int, error) {
	w.wrote.Store(true)
	return w.ResponseWriter.Write(b)
}

func TestOriginBlockingReload(t *testing.T) {
	rig := newSwitchTestRig(t)
	defer rig.teardown()
	otPublish(t, rig.pub1, 1, 0)
	otPublish(t, rig.pub1, 1, 1)
	rig.tl = otOpen(t, rig, "room_b")
	if err := rig.tl.Bind(rig.key1, 1); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	c, err := NewPlaybackController(rig.resolver, nil)
	if err != nil {
		t.Fatalf("NewPlaybackController: %v", err)
	}
	if err := c.Register(rig.tl); err != nil {
		t.Fatalf("Register: %v", err)
	}
	o := NewOrigin(c)
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Immediate: the segment and part exist.
	otContains(t, otGet(t, o, "/live/room_b/vhi.m3u8?_HLS_msn=1", http.StatusOK).Body.String(), `URI="`+otOutBase+`vhi/1.1.m4s"`)
	otGet(t, o, "/live/room_b/vhi.m3u8?_HLS_msn=1&_HLS_part=1", http.StatusOK)
	otGet(t, o, "/live/room_b/vhi.m3u8?_HLS_msn=0&_HLS_part=5", http.StatusOK)

	// Blocking on a part: wakes when the controller exposes the publication.
	type result struct {
		rec     *httptest.ResponseRecorder
		elapsed time.Duration
	}
	fetch := func(target string) chan result {
		ch := make(chan result, 1)
		go func() {
			start := time.Now()
			rec := otRequest(o, http.MethodGet, target)
			ch <- result{rec, time.Since(start)}
		}()
		return ch
	}
	waiting := fetch("/live/room_b/vhi.m3u8?_HLS_msn=1&_HLS_part=2")
	select {
	case res := <-waiting:
		t.Fatalf("part request answered early: %d %s", res.rec.Code, res.rec.Body.String())
	case <-time.After(200 * time.Millisecond):
	}
	otPublish(t, rig.pub1, 1, 2)
	res := <-waiting
	if res.rec.Code != http.StatusOK {
		t.Fatalf("blocked part request = %d %s", res.rec.Code, res.rec.Body.String())
	}
	otContains(t, res.rec.Body.String(), `URI="`+otOutBase+`vhi/1.2.m4s"`)

	// Blocking on a segment by room MSN: wakes when room segment 2 is exposed.
	for idx := 3; idx < otParts; idx++ {
		otPublish(t, rig.pub1, 1, idx)
	}
	waiting = fetch("/live/room_b/a.m3u8?_HLS_msn=2")
	select {
	case res := <-waiting:
		t.Fatalf("segment request answered early: %d %s", res.rec.Code, res.rec.Body.String())
	case <-time.After(200 * time.Millisecond):
	}
	otPublish(t, rig.pub1, 2, 0)
	res = <-waiting
	if res.rec.Code != http.StatusOK {
		t.Fatalf("blocked segment request = %d %s", res.rec.Code, res.rec.Body.String())
	}
	otContains(t, res.rec.Body.String(), "#EXTINF:1.920,\n"+otOutBase+"a1/1.m4s\n")
	otContains(t, res.rec.Body.String(), `URI="`+otOutBase+`a1/2.0.m4s"`)
	// A part past the end of a complete segment is answered by the next segment.
	otGet(t, o, "/live/room_b/vhi.m3u8?_HLS_msn=1&_HLS_part=8", http.StatusOK)

	// Timeout: three part targets, then 503.
	res = <-fetch("/live/room_b/vhi.m3u8?_HLS_msn=3")
	if res.rec.Code != http.StatusServiceUnavailable || res.elapsed < 3*videoPartTarget || res.elapsed > 3*time.Second {
		t.Fatalf("future segment request = %d after %v", res.rec.Code, res.elapsed)
	}

	// Client cancellation: the handler returns without writing.
	reqCtx, reqCancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/live/room_b/vhi.m3u8?_HLS_msn=3&_HLS_part=0", nil).WithContext(reqCtx)
	w := &otWriter{ResponseWriter: httptest.NewRecorder()}
	returned := make(chan struct{})
	go func() { o.ServeHTTP(w, req); close(returned) }()
	time.Sleep(100 * time.Millisecond)
	reqCancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatalf("cancelled request did not return")
	}
	if w.wrote.Load() {
		t.Fatalf("cancelled request wrote a response")
	}

	// Malformed, overflowing and future directives.
	for _, q := range []string{
		"_HLS_part=1", "_HLS_msn=", "_HLS_msn=abc", "_HLS_msn=-1", "_HLS_msn=+1", "_HLS_msn=01", "_HLS_msn=1.0",
		"_HLS_msn=99999999999999999999", "_HLS_msn=1&_HLS_part=99999999999", "_HLS_msn=1&_HLS_msn=2",
		"_HLS_msn=5", "_HLS_msn=2&_HLS_part=9", "_HLS_skip=v2", "_HLS_skip=NO", "_HLS_msn=%zz",
	} {
		otGet(t, o, "/live/room_b/vhi.m3u8?"+q, http.StatusBadRequest)
	}
	otGet(t, o, "/live/room_b/a.m3u8?_HLS_msn=2&_HLS_part=10", http.StatusBadRequest)
	otGet(t, o, "/live/room_b/a.m3u8?_HLS_msn=1&_HLS_part=9", http.StatusOK)
	otGet(t, o, "/live/room_b/vhi.m3u8?_HLS_msn=4", http.StatusServiceUnavailable)
	// A delta is asked for but the window is shorter than CAN-SKIP-UNTIL: the full playlist.
	full := otGet(t, o, "/live/room_b/vhi.m3u8?_HLS_skip=YES&_HLS_msn=2", http.StatusOK).Body.String()
	if strings.Contains(full, "#EXT-X-SKIP") {
		t.Fatalf("delta skipped inside the skip boundary:\n%s", full)
	}

	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	otWaitGoroutines(t, baseline)
}

// otSynthetic builds a solo room view of n two-second segments, the last one open with a single
// part when open is set.
func otSynthetic(n int, open bool) TimelineSnapshot {
	key := EpochKey{Run: "r", Source: "s", Epoch: 1}
	bind := RoomBinding{Generation: 1, Key: key, AudioTrack: ipc.TrackAudioA, Inits: make([]ipc.Entry, RoomTracks),
		Presentation: ipc.Presentation{Config: ipc.Config{Mode: ipc.ModeSolo}, CropAnchor: 0.4,
			Slots: []ipc.PresentationSlot{{HostID: "h", RoomID: "r"}}}}
	live := bind
	snap := TimelineSnapshot{Room: "r", Bindings: []RoomBinding{bind}, Live: &live, NextMSN: uint64(n + 1)}
	for i := 1; i <= n; i++ {
		seg := RoomSegment{MSN: uint64(i), Generation: 1, Key: key, SourceMSN: i}
		closed := !(open && i == n)
		for t := range seg.Tracks {
			trk := RoomTrack{Track: bind.SourceTrack(t), Closed: closed, Last: -1}
			parts := otParts
			if !closed {
				parts = 1
			}
			for p := 0; p < parts; p++ {
				trk.Parts = append(trk.Parts, ipc.Entry{Index: p, Duration: otVideoPart, Independent: p == 0})
				trk.DurationNS += otVideoPart
			}
			if closed {
				trk.Last = parts - 1
			}
			seg.Tracks[t] = trk
		}
		snap.Segments = append(snap.Segments, seg)
	}
	return snap
}

func TestRenderMediaDeltaAndEnd(t *testing.T) {
	vhi := renditions[0]
	full := string(renderMedia(otSynthetic(8, true), vhi, false))
	otContains(t, full, `X-MODE="solo",X-ATLAS-VERSION="1",X-SLOTS="0:h:r:0,0,1,1",X-CROP-ANCHOR=0.4`)
	otContains(t, full, "/media/r/s/1/vhi/1.m4s\n")
	otContains(t, full, `#EXT-X-PRELOAD-HINT:TYPE=PART,URI="/media/r/s/1/vhi/8.1.m4s"`)
	if strings.Contains(full, "#EXT-X-SKIP") || strings.Contains(full, "#EXT-X-ENDLIST") {
		t.Fatalf("full playlist:\n%s", full)
	}
	// Seven closed segments and one open part: the segments after the first already span more
	// than CAN-SKIP-UNTIL, so exactly one segment is skipped and the first kept one gets its MAP.
	delta := string(renderMedia(otSynthetic(8, true), vhi, true))
	otContains(t, delta, "#EXT-X-MEDIA-SEQUENCE:1\n")
	otContains(t, delta, "#EXT-X-SKIP:SKIPPED-SEGMENTS=1\n#EXT-X-MAP:URI=\"/media/r/s/1/vhi/init.mp4\"\n#EXT-X-PROGRAM-DATE-TIME:")
	if strings.Contains(delta, "/media/r/s/1/vhi/1.m4s\n") || !strings.Contains(delta, "/media/r/s/1/vhi/2.m4s\n") || strings.Count(delta, "#EXT-X-MAP:") != 1 {
		t.Fatalf("delta playlist:\n%s", delta)
	}
	if got := skipCount(otSynthetic(7, false).Segments, 0); got != 0 {
		t.Fatalf("skipCount of 14 s = %d, want 0", got)
	}
	// Ended: ENDLIST and no hint. Frozen live or a closed last segment under a pending switch: no hint.
	ended := otSynthetic(2, false)
	ended.Ended = true
	if s := string(renderMedia(ended, vhi, false)); !strings.HasSuffix(s, "#EXT-X-ENDLIST\n") || strings.Contains(s, "PRELOAD-HINT") {
		t.Fatalf("ended playlist:\n%s", s)
	}
	frozen := otSynthetic(2, true)
	frozen.Live.Ends = make([]PartRef, RoomTracks)
	if s := string(renderMedia(frozen, vhi, false)); strings.Contains(s, "PRELOAD-HINT") {
		t.Fatalf("frozen playlist hints:\n%s", s)
	}
	pending := otSynthetic(2, false)
	pending.Pending = &PendingSwitch{}
	if s := string(renderMedia(pending, vhi, false)); strings.Contains(s, "PRELOAD-HINT") {
		t.Fatalf("pending playlist hints:\n%s", s)
	}
	otContains(t, string(renderMedia(otSynthetic(2, false), vhi, false)), `#EXT-X-PRELOAD-HINT:TYPE=PART,URI="/media/r/s/1/vhi/3.0.m4s"`)
	if _, ok := renderMaster(TimelineSnapshot{}); ok {
		t.Fatalf("master rendered without a live binding")
	}
}
