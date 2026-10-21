//go:build linux

package supervisor

// Playlist rendering of the LL-HLS origin (plan §4.5, §5.2, §5.3, §7.5): the master and the four
// room rendition playlists, rendered from one owned TimelineSnapshot and nothing else. Room MSNs
// number the segments; every media URI names the source epoch, source track and source MSN behind
// the room segment, so rooms sharing a source share its video bytes. The audio playlist is always
// a.m3u8; its media URIs carry a0 or a1 for the source audio track the room's binding selects.

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/connectsapp/connects-egress/internal/ipc"
)

const (
	playlistMIME = "application/vnd.apple.mpegurl"

	// §7.5 packaging parameters. Apple's HLS authoring specification requires PART-HOLD-BACK to
	// be at least three times PART-TARGET: 3 × videoPartTarget, the larger part target.
	targetDuration   = 2
	videoPartTarget  = 334 * time.Millisecond
	audioPartTarget  = 320 * time.Millisecond
	holdBackAttr        = "6.0"
	partHoldBack        = "1.002"
	startTimeOffsetAttr = "-1.2"
	canSkipUntil        = 12 * time.Second
	canSkipUntilAttr = "12.0"

	// atlasVersion is the initial version of the atlas DATERANGE contract (§4.5).
	atlasVersion = "1"
	atlasClass   = "com.connectsapp.atlas"

	// audioKbps is the provisional AAC bitrate of the G-E2E ladder.
	audioKbps = 128
)

// rendition is one of the four room playlists: its playlist name, the room track it renders and
// the part target of that track.
type rendition struct {
	name       string
	track      int
	partTarget time.Duration
}

var renditions = [RoomTracks]rendition{
	{"vhi", ipc.TrackVideoHi, videoPartTarget},
	{"vmid", ipc.TrackVideoMid, videoPartTarget},
	{"vlo", ipc.TrackVideoLo, videoPartTarget},
	{"a", RoomTracks - 1, audioPartTarget},
}

// videoLadder is the provisional ladder: the fixed Solo RESOLUTION of each tier (§8.5) and the
// G-E2E target bitrate in kbps. They are planning targets, not measured production rates.
var videoLadder = [3]struct{ width, height, kbps int }{{720, 1280, 2500}, {576, 1024, 1600}, {360, 640, 800}}

// mediaRenditions are the rendition names of media URIs by source track.
var mediaRenditions = [ipc.Tracks1v1]string{"vhi", "vmid", "vlo", "a0", "a1"}

// renditionByPlaylist returns the rendition of a playlist file name such as vhi.m3u8.
func renditionByPlaylist(name string) (rendition, bool) {
	for _, r := range renditions {
		if name == r.name+".m3u8" {
			return r, true
		}
	}
	return rendition{}, false
}

// mediaTrack returns the source track of a media rendition name such as a1.
func mediaTrack(name string) (int, bool) {
	for trk, n := range mediaRenditions {
		if name == n {
			return trk, true
		}
	}
	return 0, false
}

// mediaBase is the URI prefix of the media objects of source track trk of epoch key:
// /media/{run}/{source}/{epoch}/{rendition}/ with the identities path-escaped.
func mediaBase(key EpochKey, trk int) string {
	return "/media/" + url.PathEscape(key.Run) + "/" + url.PathEscape(key.Source) + "/" +
		strconv.FormatUint(key.Epoch, 10) + "/" + mediaRenditions[trk] + "/"
}

// renderMaster renders the master playlist of the room from the live binding: the audio group on
// the stable a.m3u8, three video variants with CODECS from the actual inits, the fixed Solo
// RESOLUTION of each tier and the provisional BANDWIDTH. false without a live binding.
func renderMaster(snap TimelineSnapshot) ([]byte, bool) {
	live := snap.Live
	if live == nil || len(live.Inits) != RoomTracks {
		return nil, false
	}
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString("#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"AAC\",DEFAULT=YES,AUTOSELECT=YES,URI=\"a.m3u8\"\n")
	for i, tier := range videoLadder {
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:BANDWIDTH=%d,CODECS=\"%s,%s\",RESOLUTION=%dx%d,AUDIO=\"aud\"\n%s.m3u8\n",
			(tier.kbps+audioKbps)*1000, live.Inits[i].Codecs, live.Inits[RoomTracks-1].Codecs, tier.width, tier.height, renditions[i].name)
	}
	return []byte(b.String()), true
}

// renderMedia renders rendition r of the room from snap: a delta playlist when delta is set and
// the window allows skipping (§7.5 CAN-SKIP-UNTIL), the full playlist otherwise.
func renderMedia(snap TimelineSnapshot, r rendition, delta bool) []byte {
	segs := snap.Segments
	var b strings.Builder
	fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:%d\n", targetDuration)
	fmt.Fprintf(&b, "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=%s,PART-HOLD-BACK=%s,CAN-SKIP-UNTIL=%s\n", holdBackAttr, partHoldBack, canSkipUntilAttr)
	fmt.Fprintf(&b, "#EXT-X-PART-INF:PART-TARGET=%s\n", seconds(int64(r.partTarget)))
	fmt.Fprintf(&b, "#EXT-X-START:TIME-OFFSET=%s,PRECISE=YES\n", startTimeOffsetAttr)
	first := snap.NextMSN
	if len(segs) > 0 {
		first = segs[0].MSN
	}
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-DISCONTINUITY-SEQUENCE:%d\n", first, snap.DSN)
	for i, seg := range segs {
		if i == 0 || seg.Generation != segs[i-1].Generation {
			if bd := bindingOf(snap, seg.Generation); bd != nil {
				writeAtlas(&b, bd, seg)
			}
		}
	}
	skipped := 0
	if delta {
		skipped = skipCount(segs, r.track)
	}
	if skipped > 0 {
		fmt.Fprintf(&b, "#EXT-X-SKIP:SKIPPED-SEGMENTS=%d\n", skipped)
	}
	for i := skipped; i < len(segs); i++ {
		seg := segs[i]
		trk := seg.Tracks[r.track]
		base := mediaBase(seg.Key, trk.Track)
		if seg.Discontinuity {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if i == skipped || seg.Generation != segs[i-1].Generation {
			fmt.Fprintf(&b, "#EXT-X-MAP:URI=\"%sinit.mp4\"\n", base)
		}
		fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", utcString(trk.UTCNS))
		for _, p := range trk.Parts {
			fmt.Fprintf(&b, "#EXT-X-PART:DURATION=%s,URI=\"%s%d.%d.m4s\"", seconds(p.Duration), base, seg.SourceMSN, p.Index)
			if p.Independent {
				b.WriteString(",INDEPENDENT=YES")
			}
			b.WriteByte('\n')
		}
		if trk.Closed {
			fmt.Fprintf(&b, "#EXTINF:%s,\n%s%d.m4s\n", seconds(trk.DurationNS), base, seg.SourceMSN)
		}
	}
	if msn, idx, base, ok := nextPart(snap, r.track); ok {
		fmt.Fprintf(&b, "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"%s%d.%d.m4s\"\n", base, msn, idx)
	}
	if n := len(segs); n > 0 {
		last := segs[n-1]
		for _, o := range renditions {
			if o.track != r.track {
				fmt.Fprintf(&b, "#EXT-X-RENDITION-REPORT:URI=\"%s.m3u8\",LAST-MSN=%d,LAST-PART=%d\n", o.name, last.MSN, len(last.Tracks[o.track].Parts)-1)
			}
		}
	}
	if snap.Ended {
		b.WriteString("#EXT-X-ENDLIST\n")
	}
	return []byte(b.String())
}

// bindingOf returns the binding of generation gen in snap, nil when it is no longer referenced.
func bindingOf(snap TimelineSnapshot, gen uint64) *RoomBinding {
	for i := range snap.Bindings {
		if snap.Bindings[i].Generation == gen {
			return &snap.Bindings[i]
		}
	}
	return nil
}

// writeAtlas writes the §4.5 atlas DATERANGE of binding bd, started at the actual first video IDR
// segment UTC of the generation. When a legacy binding lacks StartUTCNS and its start segment has
// already rolled out of the window, no DATERANGE is emitted rather than inventing a timestamp.
// X-SLOTS lists slot:host_id:room_id:x,y,w,h per slot, separated by semicolons, identities
// query-escaped, rectangles normalised to 0..1 of the atlas.
func writeAtlas(b *strings.Builder, bd *RoomBinding, seg RoomSegment) {
	startUTC, ok := atlasStartUTC(bd, seg)
	if !ok {
		return
	}
	pres := bd.Presentation
	var slots []string
	for i, s := range pres.Slots {
		rect := "0,0,1,1"
		if pres.Config.Mode == ipc.Mode1v1 {
			rect = []string{"0,0,0.5,1", "0.5,0,0.5,1"}[i]
		}
		slots = append(slots, fmt.Sprintf("%d:%s:%s:%s", i, url.QueryEscape(s.HostID), url.QueryEscape(s.RoomID), rect))
	}
	fmt.Fprintf(b, "#EXT-X-DATERANGE:ID=\"atlas-%d\",CLASS=\"%s\",START-DATE=\"%s\",X-MODE=\"%s\",X-ATLAS-VERSION=\"%s\",X-SLOTS=\"%s\",X-CROP-ANCHOR=%s\n",
		bd.Generation, atlasClass, utcString(startUTC), pres.Config.Mode, atlasVersion,
		strings.Join(slots, ";"), strconv.FormatFloat(pres.CropAnchor, 'f', -1, 64))
}

func atlasStartUTC(bd *RoomBinding, seg RoomSegment) (int64, bool) {
	if bd.StartUTCNS != nil {
		return *bd.StartUTCNS, true
	}
	startMSN := bd.StartMSN
	if startMSN == 0 {
		startMSN = 1
	}
	if seg.SourceMSN == startMSN && len(seg.Tracks[ipc.TrackVideoHi].Parts) > 0 {
		return seg.Tracks[ipc.TrackVideoHi].UTCNS, true
	}
	return 0, false
}

// skipCount returns how many leading segments a delta playlist may skip on track trk: a segment
// is skippable only when the segments after it already span CAN-SKIP-UNTIL.
func skipCount(segs []RoomSegment, trk int) int {
	var after int64
	for i := len(segs) - 1; i >= 0; i-- {
		if after >= int64(canSkipUntil) {
			return i + 1
		}
		after += segs[i].Tracks[trk].DurationNS
	}
	return 0
}

// nextPart returns the part to preload-hint on room track trk: the next part of the open last
// segment, or part 0 of the live generation's next segment while no switch is pending; nothing
// once the live generation is frozen, the room has ended, or the last segment is not the live
// generation's.
func nextPart(snap TimelineSnapshot, trk int) (msn, idx int, base string, ok bool) {
	n := len(snap.Segments)
	live := snap.Live
	if n == 0 || live == nil || live.Frozen() || snap.Ended {
		return 0, 0, "", false
	}
	last := snap.Segments[n-1]
	if last.Generation != live.Generation {
		return 0, 0, "", false
	}
	t := last.Tracks[trk]
	base = mediaBase(last.Key, t.Track)
	if !t.Closed {
		if len(t.Parts) == 0 || (trk < ipc.TrackAudioA && (len(t.Parts) >= 6 || t.Parts[len(t.Parts)-1].Index >= 5)) {
			return 0, 0, "", false
		}
		return last.SourceMSN, len(t.Parts), base, true
	}
	if snap.Pending != nil {
		return 0, 0, "", false
	}
	return last.SourceMSN + 1, 0, base, true
}

// seconds formats a non-negative duration in ns as seconds with three decimals, truncated so a
// part never renders longer than its target.
func seconds(ns int64) string {
	return fmt.Sprintf("%d.%03d", ns/int64(time.Second), ns%int64(time.Second)/int64(time.Millisecond))
}

// utcString formats a UTC time in ns as an ISO 8601 date-time with milliseconds.
func utcString(ns int64) string {
	return time.Unix(0, ns).UTC().Format("2006-01-02T15:04:05.000Z")
}
