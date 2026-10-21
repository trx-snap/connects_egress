//go:build linux

package supervisor

// Origin: the LL-HLS HTTP origin of the supervisor (plan §5.2, §7.5), an http.Handler over a
// PlaybackController. Routes:
//
//	/live/{room}/master.m3u8, vhi.m3u8, vmid.m3u8, vlo.m3u8, a.m3u8
//	/media/{run}/{source}/{epoch}/{rendition}/init.mp4, {source_msn}.m4s, {source_msn}.{part}.m4s
//
// Playlists are rendered from an owned TimelineSnapshot. _HLS_msn and _HLS_part are evaluated
// against room MSNs and hold the request, without a goroutine, until the room exposes that segment
// or part, the room ends, three part targets pass (503) or the client leaves (nothing is written).
// _HLS_skip=YES asks for a delta playlist. A room is resolved through a RoomIndex: a room the
// index knows without an active run is 503, never the media of an earlier run, and an unknown
// room is 404; the index may resolve a room whose run ended cleanly to its final, ended
// timeline for a while, whose playlists carry EXT-X-ENDLIST, and a timeline closed meanwhile is
// 503 as well. Media bytes are resolved by their full epoch key and source track, served
// immutable with GET, HEAD and byte ranges; what the Source does not retain, and an open segment
// asked for whole, is 404. A malformed route or query is 400.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/connectsapp/connects-egress/internal/ipc"
	"github.com/connectsapp/connects-egress/internal/partstore"
)

const mediaCacheControl = "public, max-age=31536000, immutable"

// RoomIndex resolves the room of a /live/ request. Lookup returns the timeline serving the
// room's active run; Inactive reports a room the index knows that has no active run, whose
// playlists are unavailable rather than unknown.
type RoomIndex interface {
	Lookup(room string) (*Timeline, bool)
	Inactive(room string) bool
}

// Origin serves one controller's rooms. See the file comment.
type Origin struct {
	c     *PlaybackController
	rooms RoomIndex
}

// NewOrigin returns the origin of c: its registered timelines are the rooms, every other room
// is unknown.
func NewOrigin(c *PlaybackController) *Origin { return &Origin{c: c, rooms: registry{c}} }

// NewRoomOrigin returns an origin resolving rooms through rooms and media through c.
func NewRoomOrigin(c *PlaybackController, rooms RoomIndex) *Origin {
	return &Origin{c: c, rooms: rooms}
}

// registry is the RoomIndex of a controller's registered timelines alone.
type registry struct{ c *PlaybackController }

func (r registry) Lookup(room string) (*Timeline, bool) { return r.c.Lookup(room) }
func (registry) Inactive(string) bool                   { return false }

// ServeHTTP implements http.Handler.
func (o *Origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	segs, ok := splitPath(r.URL.EscapedPath())
	switch {
	case !ok:
		http.Error(w, "malformed path", http.StatusBadRequest)
	case len(segs) == 3 && segs[0] == "live":
		o.playlist(w, r, segs[1], segs[2])
	case len(segs) == 6 && segs[0] == "media":
		o.media(w, r, segs[1:])
	default:
		http.NotFound(w, r)
	}
}

// splitPath splits an escaped request path into its unescaped segments: an absolute path of
// nonempty segments, each valid UTF-8 without control characters once unescaped.
func splitPath(escaped string) ([]string, bool) {
	rest, ok := strings.CutPrefix(escaped, "/")
	if !ok {
		return nil, false
	}
	segs := strings.Split(rest, "/")
	for i, s := range segs {
		u, err := url.PathUnescape(s)
		if err != nil || u == "" || !utf8.ValidString(u) || strings.ContainsFunc(u, unicode.IsControl) {
			return nil, false
		}
		segs[i] = u
	}
	return segs, true
}

// playlist serves one playlist of room.
func (o *Origin) playlist(w http.ResponseWriter, r *http.Request, room, name string) {
	tl, ok := o.rooms.Lookup(room)
	if !ok {
		if o.rooms.Inactive(room) {
			http.Error(w, "the room has no active run", http.StatusServiceUnavailable)
			return
		}
		http.NotFound(w, r)
		return
	}
	if name == "master.m3u8" {
		snap, _, err := tl.Observe()
		if closedRoom(w, err) {
			return
		}
		body, ok := renderMaster(snap)
		if err != nil || !ok {
			http.NotFound(w, r)
			return
		}
		servePlaylist(w, r, body)
		return
	}
	rend, ok := renditionByPlaylist(name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	d, err := parseDirectives(r.URL.RawQuery, rend.partTarget)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	snap, changed, err := tl.Observe()
	if closedRoom(w, err) {
		return
	}
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if d.wait && d.msn > snap.NextMSN+1 { // beyond the last segment plus two
		http.Error(w, "_HLS_msn is too far ahead", http.StatusBadRequest)
		return
	}
	if d.wait {
		timer := time.NewTimer(3 * rend.partTarget)
		defer timer.Stop()
		for !snap.Ended && !d.satisfied(snap, rend.track) {
			select {
			case <-changed:
			case <-timer.C:
				http.Error(w, "the requested part did not arrive in time", http.StatusServiceUnavailable)
				return
			case <-r.Context().Done():
				return
			}
			if snap, changed, err = tl.Observe(); err != nil {
				if !closedRoom(w, err) {
					http.NotFound(w, r)
				}
				return
			}
		}
	}
	servePlaylist(w, r, renderMedia(snap, rend, d.skip))
}

// closedRoom answers a timeline closed between its lookup and its observation, which is a room
// without an active run, and reports whether it did.
func closedRoom(w http.ResponseWriter, err error) bool {
	if !errors.Is(err, ErrTimelineClosed) {
		return false
	}
	http.Error(w, "the room has no active run", http.StatusServiceUnavailable)
	return true
}

func servePlaylist(w http.ResponseWriter, r *http.Request, body []byte) {
	w.Header().Set("Content-Type", playlistMIME)
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(body))
}

// directives is a validated set of delivery directives: the room segment, or its part when part
// is not negative, to wait for, and whether a delta playlist is wanted.
type directives struct {
	msn  uint64
	part int
	wait bool
	skip bool
}

// parseDirectives applies RFC 8216bis 6.2.5.2 to the query: _HLS_msn and _HLS_part are canonical
// decimal integers, _HLS_part needs _HLS_msn and may not reach further than three part targets
// into a segment, _HLS_skip is YES. Other parameters are ignored.
func parseDirectives(rawQuery string, partTarget time.Duration) (directives, error) {
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return directives{}, err
	}
	var d directives
	msn, hasMSN, err := directiveValue(q, "_HLS_msn", 64)
	if err != nil {
		return directives{}, err
	}
	part, hasPart, err := directiveValue(q, "_HLS_part", 31)
	if err != nil {
		return directives{}, err
	}
	switch skip := q["_HLS_skip"]; {
	case len(skip) > 1 || (len(skip) == 1 && skip[0] != "YES"):
		return directives{}, errors.New("_HLS_skip must be YES")
	case len(skip) == 1:
		d.skip = true
	}
	switch {
	case hasPart && !hasMSN:
		return directives{}, errors.New("_HLS_part needs _HLS_msn")
	case hasPart && part > uint64(3*time.Second/partTarget):
		return directives{}, errors.New("_HLS_part is too far ahead")
	}
	d.msn, d.part, d.wait = msn, -1, hasMSN
	if hasPart {
		d.part = int(part)
	}
	return d, nil
}

// directiveValue reads one directive of at most bits bits: a repeated, empty, signed,
// non-canonical or overflowing value is an error.
func directiveValue(q url.Values, key string, bits int) (uint64, bool, error) {
	v := q[key]
	switch {
	case len(v) == 0:
		return 0, false, nil
	case len(v) > 1:
		return 0, false, fmt.Errorf("%s given %d times", key, len(v))
	}
	n, err := strconv.ParseUint(v[0], 10, bits)
	if err != nil || strconv.FormatUint(n, 10) != v[0] {
		return 0, false, fmt.Errorf("%s=%q is not a canonical non-negative decimal integer", key, v[0])
	}
	return n, true, nil
}

// satisfied reports whether snap already answers d on room track trk: the segment has rolled out
// of the window, a later segment exists, or the segment exposes the part. Every exposed segment
// has at least one part on every room track, so a later segment answers any part of a complete
// earlier one.
func (d directives) satisfied(snap TimelineSnapshot, trk int) bool {
	last := snap.NextMSN - 1
	if len(snap.Segments) == 0 {
		return d.msn <= last
	}
	if d.msn < snap.Segments[0].MSN || d.msn < last {
		return true
	}
	if d.msn > last {
		return false
	}
	t := snap.Segments[len(snap.Segments)-1].Tracks[trk]
	return d.part < len(t.Parts)
}

// media serves one media object: segs are run, source, epoch, rendition and file name.
func (o *Origin) media(w http.ResponseWriter, r *http.Request, segs []string) {
	epoch, err := strconv.ParseUint(segs[2], 10, 64)
	trk, okTrack := mediaTrack(segs[3])
	kind, msn, idx, okName := parseMediaName(segs[4])
	if err != nil || strconv.FormatUint(epoch, 10) != segs[2] || epoch == 0 || !okTrack || !okName {
		http.NotFound(w, r)
		return
	}
	key := EpochKey{Run: segs[0], Source: segs[1], Epoch: epoch}
	src, err := o.c.Resolve(key)
	switch {
	case errors.Is(err, ErrLostEpoch):
		http.NotFound(w, r)
		return
	case err != nil || src == nil:
		http.Error(w, "epoch resolution failed", http.StatusInternalServerError)
		return
	}
	info := src.Info()
	if info.Run != key.Run || info.Source != key.Source || info.Epoch != key.Epoch {
		http.Error(w, "epoch resolution failed", http.StatusInternalServerError)
		return
	}
	if trk >= info.Config.Tracks {
		http.NotFound(w, r)
		return
	}
	var data []byte
	switch kind {
	case mediaInit:
		data, err = src.ReadInit(trk)
	case mediaSegment:
		data, err = src.ReadSegment(trk, msn)
	default:
		data, err = o.readPart(r.Context(), key, src, trk, msn, idx)
	}
	switch {
	case errors.Is(err, errClientGone):
		return
	case errors.Is(err, errPartTimeout):
		http.Error(w, "the requested part did not arrive in time", http.StatusServiceUnavailable)
		return
	case errors.Is(err, partstore.ErrNotFound), errors.Is(err, partstore.ErrNotReady), errors.Is(err, partstore.ErrClosed):
		http.NotFound(w, r)
		return
	case err != nil:
		http.Error(w, "media read failed", http.StatusInternalServerError)
		return
	}
	mime := "video/mp4"
	if trk >= ipc.TrackAudioA {
		mime = "audio/mp4"
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Cache-Control", mediaCacheControl)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
}

var (
	errPartTimeout = errors.New("part timeout")
	errClientGone  = errors.New("client gone")
)

// readPart serves a committed media part immediately, or waits up to three part targets when the
// requested part is an active preload hint on a healthy source and live room timeline.
func (o *Origin) readPart(ctx context.Context, key EpochKey, src *Source, trk, msn, idx int) ([]byte, error) {
	data, err := src.ReadPart(trk, msn, idx)
	if !errors.Is(err, partstore.ErrNotFound) {
		return data, err
	}
	obs, obsErr := src.Observe()
	if obsErr != nil {
		return nil, obsErr
	}
	if data, err = src.ReadPart(trk, msn, idx); !errors.Is(err, partstore.ErrNotFound) {
		return data, err
	}
	ok, tlCh0, tlCh1 := o.activePartHint(key, obs, trk, msn, idx)
	if !ok {
		return nil, partstore.ErrNotFound
	}
	target := videoPartTarget
	if trk >= ipc.TrackAudioA {
		target = audioPartTarget
	}
	timer := time.NewTimer(3 * target)
	defer timer.Stop()
	for {
		select {
		case <-obs.Changed:
		case <-tlCh0:
		case <-tlCh1:
		case <-timer.C:
			if data, err = src.ReadPart(trk, msn, idx); !errors.Is(err, partstore.ErrNotFound) {
				return data, err
			}
			if obs, obsErr = src.Observe(); obsErr == nil {
				if ok, _, _ = o.activePartHint(key, obs, trk, msn, idx); !ok {
					return nil, partstore.ErrNotFound
				}
			}
			return nil, errPartTimeout
		case <-ctx.Done():
			return nil, errClientGone
		}
		obs, obsErr = src.Observe()
		if obsErr != nil {
			return nil, obsErr
		}
		if data, err = src.ReadPart(trk, msn, idx); !errors.Is(err, partstore.ErrNotFound) {
			return data, err
		}
		if ok, tlCh0, tlCh1 = o.activePartHint(key, obs, trk, msn, idx); !ok {
			return nil, partstore.ErrNotFound
		}
	}
}

// activePartHint reports whether (trk, msn, idx) is the next part of key's healthy source and is
// actively preload-hinted by at least one registered room timeline, returning up to two timeline
// change channels to wake on room state transitions.
func (o *Origin) activePartHint(key EpochKey, obs SourceObservation, trk, msn, idx int) (bool, <-chan struct{}, <-chan struct{}) {
	if obs.Closed || obs.Refusing || obs.Snapshot.Terminal != nil || trk < 0 || trk >= len(obs.Snapshot.Tracks) {
		return false, nil, nil
	}
	ts := obs.Snapshot.Tracks[trk]
	if ts.Init == nil || len(ts.Segments) == 0 || (trk >= ipc.TrackAudioA && ts.Phases == nil) {
		return false, nil, nil
	}
	last := ts.Segments[len(ts.Segments)-1]
	if !last.Closed {
		if (trk < ipc.TrackAudioA && len(last.Parts) >= 6) || msn != last.MSN || idx != len(last.Parts) {
			return false, nil, nil
		}
	} else if msn != last.MSN+1 || idx != 0 {
		return false, nil, nil
	}
	roomTrk := trk
	if trk >= ipc.TrackAudioA {
		roomTrk = RoomTracks - 1
	}
	var ch0, ch1 <-chan struct{}
	matched := false
	for _, tl := range o.c.snapshot() {
		tlSnap, tlCh, err := tl.Observe()
		if err != nil || tlSnap.Closed || tlSnap.Ended || tlSnap.Live == nil || tlSnap.Live.Key != key {
			continue
		}
		if trk >= ipc.TrackAudioA && tlSnap.Live.AudioTrack != trk {
			continue
		}
		if tlSnap.Pending != nil && tlSnap.Pending.HoldStartMonoNS != 0 {
			continue
		}
		hMSN, hIdx, _, ok := nextPart(tlSnap, roomTrk)
		if !ok || hMSN != msn || hIdx != idx {
			continue
		}
		matched = true
		if ch0 == nil {
			ch0 = tlCh
		} else if ch1 == nil {
			ch1 = tlCh
		}
	}
	return matched, ch0, ch1
}

const (
	mediaInit = iota
	mediaSegment
	mediaPart
)

// parseMediaName accepts init.mp4, {msn}.m4s and {msn}.{part}.m4s with canonical decimals, msn
// from 1 and part from 0.
func parseMediaName(name string) (kind, msn, idx int, ok bool) {
	if name == "init.mp4" {
		return mediaInit, 0, 0, true
	}
	stem, ok := strings.CutSuffix(name, ".m4s")
	if !ok {
		return 0, 0, 0, false
	}
	f := strings.Split(stem, ".")
	switch len(f) {
	case 1:
		msn = number(f[0])
		return mediaSegment, msn, 0, msn >= 1
	case 2:
		msn, idx = number(f[0]), number(f[1])
		return mediaPart, msn, idx, msn >= 1 && idx >= 0
	}
	return 0, 0, 0, false
}

// number parses a canonical non-negative decimal, -1 otherwise.
func number(s string) int {
	n, err := strconv.ParseInt(s, 10, 31)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != s {
		return -1
	}
	return int(n)
}
