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
// _HLS_skip=YES asks for a delta playlist. Media bytes are resolved by their full epoch key and
// source track, served immutable with GET, HEAD and byte ranges; what the Source does not retain,
// and an open segment asked for whole, is 404. A malformed route or query is 400.

import (
	"bytes"
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

// Origin serves one controller's rooms. See the file comment.
type Origin struct {
	c *PlaybackController
}

// NewOrigin returns the origin of c.
func NewOrigin(c *PlaybackController) *Origin { return &Origin{c: c} }

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
	tl, ok := o.c.Lookup(room)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if name == "master.m3u8" {
		snap, _, err := tl.Observe()
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
				http.NotFound(w, r)
				return
			}
		}
	}
	servePlaylist(w, r, renderMedia(snap, rend, d.skip))
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
		data, err = src.ReadPart(trk, msn, idx)
	}
	switch {
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
