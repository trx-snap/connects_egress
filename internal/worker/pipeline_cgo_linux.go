//go:build linux && cgo

package worker

// The native solo pipeline, built with gst_parse_launch. Two non-live appsrcs take pre-stamped
// compressed samples: the video is parsed, decoded single-threaded, converted and normalized by
// videorate to a 30/1 cadence on the 720x1280 canvas entering the tee vt, whose three branches
// each scale to one rung, encode H.264 (scene cut off, no B-frames), parse to access units, mux
// into CMAF in manual-split mode and drain through an appsink that never drops; the audio is
// decoded, made contiguous by audiorate on a 48 kHz stereo sample clock, encoded to AAC-LC,
// parsed, muxed and drained the same way.
//
// Pad probes installed before anything flows do the scheduling, exactly as the G-R proof did:
// the tee's sink pad records the phase P and counts the normalized frames, and before frame
// 30 k, k >= 1, sends one identical key-unit request to each rung's encoder; each rung's parser src pad
// classifies every access unit by its NALs and splits before access unit 10 i, counted rather
// than timed (a fragment at every sixth, an IDR required at every third); the mix's
// encoder sink pad checks the PCM clock against the sample count; the mix's parser src pad
// records the phase A and splits every 15 access units and at the segment end nearest to
// P + 2(k+1) s. A boundary that cannot be honoured posts a pipeline error, so nothing invalid is
// published silently, with one exception at the stream's end: a boundary that lacks its key unit
// withholds the access units after it, at most a part of them, and only a further boundary turns
// that into the error (ge_probe_vparse).
//
// Unlike the proof, nothing here is sized for a run length. The only per-access-unit record is
// each rung's IDR classification, kept in a ring indexed by access unit so that ge_pull can
// mark a chunk independent: a slot is written only while every access unit it could still
// describe has been taken by the reader (the write fails the pipeline otherwise), so metadata
// still in flight is never overwritten, and the reader finds its chunk's entry as long as it
// is fewer than GE_AU_RING access units behind the parser, which the appsink and queue bounds
// guarantee. Counters are 64-bit; the boundary arithmetic is scaled without overflow.

/*
#cgo pkg-config: gstreamer-1.0 gstreamer-app-1.0 gstreamer-audio-1.0 gstreamer-video-1.0
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <gst/gst.h>
#include <gst/app/app.h>
#include <gst/audio/audio.h>
#include <gst/video/video.h>

#define GE_IN_V 0
#define GE_IN_A 1
#define GE_INPUTS 2
#define GE_RUNGS 3
#define GE_OUT_A 3
#define GE_OUTPUTS 4
#define GE_AU_RING 1024    // access-unit classifications kept per rung; see the file comment
#define GE_VIDEO_PART_AUS 10 // access units per video part: three parts a second at the 30 fps the pipeline normalizes to
#define GE_SEGMENT_PARTS 6 // boundaries per video segment
#define GE_KEY_PARTS 3     // boundaries per key unit
#define GE_AAC_RATE 48000
#define GE_AAC_FRAME 1024
#define GE_AAC_PART 15     // access units per audio chunk
#define GE_AAC_SEGMENT_S 2
#define GE_AVCC_DATA 5     // leading avcC bytes retained: configurationVersion, profile, compatibility, level, NAL length-field size
#define GE_HEADERS 8       // header buffers described per pulled sample

// ge_header describes one HEADER buffer of a pulled sample and, for a chunk header, the
// payload buffers that followed it in the list; the bytes start at off in the copy ge_take
// makes, the header first and the payload right after it.
typedef struct {
	int is_init;                // DISCONT|HEADER: the initialization segment
	guint64 pts, dts, dur, seq; // the header buffer's PTS, DTS, DURATION and OFFSET (moof sequence number)
	int fragment_start;         // header without DELTA_UNIT
	int payload;                // non-header buffers after it in the same list
	guint64 first_au;           // index of the chunk's first access unit: the payload buffers taken before it
	int key_unit;               // 1 if that access unit is an IDR picture, 0 another picture, -1 not derived
	guint64 off, hdr_len, pay_len;
} ge_header;

typedef struct {
	int nheaders, overflow, stray; // headers described, headers beyond GE_HEADERS, non-header buffers outside a chunk
	ge_header h[GE_HEADERS];
	guint64 total;                 // bytes of every buffer in the sample
	guint8 codec_data[GE_AVCC_DATA];
	int codec_data_len;            // with a video initialization segment: the parser's codec_data so far
	guint64 phase_v, phase_a;      // P and, for the audio output, A as recorded when the sample was described
	int have_v, have_a;
} ge_list;

typedef struct ge_pipe ge_pipe;

// ge_ref is the user data of a probe that serves one rung or the mix.
typedef struct {
	ge_pipe *p;
	int i;
} ge_ref;

// ge_track is one output: its appsink, its parser's src pad and the encoder pad it uses: a rung
// keeps the encoder's src pad for the key-unit requests, the mix probes the encoder's sink pad.
typedef struct {
	GstElement *sink;
	GstPad *enc_sink, *enc_src, *parse_src;
	gulong probe_enc, probe_parse;
	GstSample *pending; // described by the last ge_pull, owned until ge_take releases it
	guint64 au_taken;   // meta: payload buffers of every chunk taken so far
} ge_track;

// ge_rung is one video rendition: what the probe on its parser's src pad records.
typedef struct {
	GstSegment parse_segment;
	int have_parse_segment;
	guint8 codec_data[GE_AVCC_DATA]; // meta
	int codec_data_len;              // meta
	int nal_length;                  // NAL length-field size avcC byte 4 declares, 0 unknown
	guint64 au_count;                // meta: access units that left the parser
	guint8 idr[GE_AU_RING];          // meta: classification of access unit i at i % GE_AU_RING, valid while i >= au_taken
	guint64 first_pts, first_rt;     // meta: the first access unit's raw PTS and running time
	int have_first;                  // meta
	guint64 last_rt;
	int have_last;
	guint64 nsplit; // meta: boundaries honoured so far
} ge_rung;

// ge_mix is the AAC mix: its probe contexts, phase, counters and schedule.
typedef struct {
	// ge_probe_aenc: PCM entering the encoder, checked against the sample count
	GstSegment asrc_segment;
	int have_asrc_segment;
	GstAudioInfo pcm;
	int have_pcm;
	guint64 pcm_samples; // samples per channel
	guint64 pcm_t0;      // running time of the first PCM buffer
	int have_pcm_t0;

	// ge_probe_aparse: AAC access units leaving the parser, the phase and the scheduled splits
	GstSegment aenc_segment;
	int have_aenc_segment;
	guint64 phase_a, first_pts; // meta: running time and raw PTS of the first access unit
	int have_phase_a;           // meta
	guint64 au_count;
	guint64 seg_start, seg_end; // access units: where the open segment started and where it ends
	guint64 nseg;               // segment ends computed so far
} ge_mix;

struct ge_pipe {
	GstElement *pipe;
	GstBus *bus;
	GstElement *in[GE_INPUTS]; // appsrcs
	ge_track t[GE_OUTPUTS];
	ge_ref ref_rung[GE_RUNGS], ref_mix;

	// meta guards the fields marked so: the probes write them in the streaming threads while
	// ge_pull, ge_phases and ge_origins read them. It is held for those writes and reads only,
	// never across an event push, a pull or a wait.
	GMutex meta;
	int failed; // meta: a probe has posted the pipeline error

	// ge_probe_vt: frames entering the tee, the phase and the key-unit requests every rung receives
	GstPad *vt_sink;
	gulong probe_vt;
	GstSegment vt_segment;
	int have_vt_segment;
	guint64 phase_v;  // meta
	int have_phase_v; // meta
	guint nreq;         // key-unit requests sent: one before every frame 30 k, k >= 1
	guint withhold_req; // regression tests only: the key-unit request not sent, 0 none
	int withhold_rung;  // regression tests only: the rung it is not sent to, -1 every rung
	guint early_period; // regression tests only: every frame at a multiple of it enters the tee 1 ns early, 0 none
	guint64 vt_frame_count; // normalized frames entered so far: the key-unit schedule counts them

	// ge_probe_vparse: the stream's end, cut short of a boundary that lacked its key unit
	guint64 tail_au; // meta: the first access unit withheld from every rung
	int have_tail;   // meta

	ge_rung rung[GE_RUNGS]; // rung k is output k
	ge_mix a;               // output GE_OUT_A
};

static const char *const ge_rung_names[GE_RUNGS][3] = {
	{"venc_hi", "vparse_hi", "vsink_hi"}, {"venc_mid", "vparse_mid", "vsink_mid"}, {"venc_lo", "vparse_lo", "vsink_lo"}};

static void ge_segment(GstEvent *ev, GstSegment *seg, int *have) {
	const GstSegment *s;
	if (GST_EVENT_TYPE(ev) != GST_EVENT_SEGMENT) {
		return;
	}
	gst_event_parse_segment(ev, &s);
	gst_segment_copy_into(s, seg);
	*have = s->format == GST_FORMAT_TIME;
}

static guint64 ge_running_time(const GstSegment *seg, int have, GstBuffer *buf) {
	if (!have || !GST_BUFFER_PTS_IS_VALID(buf)) {
		return GST_CLOCK_TIME_NONE;
	}
	return gst_segment_to_running_time(seg, GST_FORMAT_TIME, GST_BUFFER_PTS(buf));
}

// ge_fail posts msg as the pipeline's error once; later failures are dropped. The bus poller
// turns it into the run's failure.
static void ge_fail(ge_pipe *p, const char *msg) {
	int first;
	g_mutex_lock(&p->meta);
	first = !p->failed;
	p->failed = 1;
	g_mutex_unlock(&p->meta);
	if (first) {
		GST_ELEMENT_ERROR(p->pipe, STREAM, FAILED, ("%s", msg), (NULL));
	}
}

static void ge_failf(ge_pipe *p, const char *fmt, ...) G_GNUC_PRINTF(2, 3);
static void ge_failf(ge_pipe *p, const char *fmt, ...) {
	va_list ap;
	gchar *msg;
	va_start(ap, fmt);
	msg = g_strdup_vprintf(fmt, ap);
	va_end(ap);
	ge_fail(p, msg);
	g_free(msg);
}

static int ge_failed(ge_pipe *p) {
	int failed;
	g_mutex_lock(&p->meta);
	failed = p->failed;
	g_mutex_unlock(&p->meta);
	return failed;
}

// ge_codec_data copies the leading GE_AVCC_DATA bytes of the codec_data buffer a CAPS event
// carries into dst, len bytes. The caller holds meta.
static void ge_codec_data(GstEvent *ev, guint8 *dst, int *len) {
	GstCaps *caps;
	const GValue *v;
	GstBuffer *b;
	GstMapInfo map;
	gst_event_parse_caps(ev, &caps);
	if (caps == NULL || gst_caps_get_size(caps) == 0) {
		return;
	}
	v = gst_structure_get_value(gst_caps_get_structure(caps, 0), "codec_data");
	if (v == NULL || !GST_VALUE_HOLDS_BUFFER(v) || (b = gst_value_get_buffer(v)) == NULL) {
		return;
	}
	if (!gst_buffer_map(b, &map, GST_MAP_READ)) {
		return;
	}
	*len = (int)MIN(map.size, (gsize)GE_AVCC_DATA);
	memcpy(dst, map.data, (size_t)*len);
	gst_buffer_unmap(b, &map);
}

// ge_split_now pushes one serialized FMP4MuxSplitNow event through pad: the muxer splits
// between the access unit before it and the one after it.
static int ge_split_now(GstPad *pad, gboolean chunk) {
	GstEvent *ev = gst_event_new_custom(GST_EVENT_CUSTOM_DOWNSTREAM,
		gst_structure_new("FMP4MuxSplitNow", "chunk", G_TYPE_BOOLEAN, chunk, NULL));
	return gst_pad_push_event(pad, ev) ? 1 : 0; // takes ownership of ev
}

// ge_aseg_end is the end of audio segment k in access units from A: the absolute mark
// P + 2(k+1) s rounded to the nearest unit, ties upward; 0 when the mark is not after A. The
// caller knows both phases.
static guint64 ge_aseg_end(const ge_pipe *p, const ge_mix *m, guint64 k) {
	guint64 mark = p->phase_v + (k + 1) * GE_AAC_SEGMENT_S * GST_SECOND;
	if (mark <= m->phase_a) {
		return 0;
	}
	return gst_util_uint64_scale_round(mark - m->phase_a, GE_AAC_RATE, (guint64)GE_AAC_FRAME * GST_SECOND);
}

// ge_probe_vt runs on the tee's sink pad, after videorate: the first normalized frame with a
// running time fixes the phase P; afterwards every frame 30 k, k >= 1, counted from that one,
// gets one upstream force-key-unit request carrying its own running time sent to every rung's
// encoder before it enters the tee. The frames are counted, not timed: every rung encodes each
// of them as one access unit, so frame 30 k is access unit 30 k, the boundary ge_probe_vparse
// requires an IDR at. videorate stamps the frames P + k s apart, but comparing a frame's running
// time against that mark let a nanosecond of rounding move the request to the next frame, and
// the IDR off its boundary. No lock is held while the events are sent. The regression tests can
// have request withhold_req kept from one rung or all of them: a key unit that goes missing,
// which ge_probe_vparse must then catch; and can have every frame at a multiple of early_period
// enter 1 ns early: the rounding the schedule must not see.
static GstPadProbeReturn ge_probe_vt(GstPad *pad, GstPadProbeInfo *info, gpointer user) {
	ge_pipe *p = user;
	GstPadProbeType type = GST_PAD_PROBE_INFO_TYPE(info);
	GstBuffer *buf;
	guint64 rt, frame_idx;
	int k, send_ok[GE_RUNGS];
	if ((type & GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM) != 0) {
		ge_segment(GST_PAD_PROBE_INFO_EVENT(info), &p->vt_segment, &p->have_vt_segment);
		return GST_PAD_PROBE_OK;
	}
	if ((type & GST_PAD_PROBE_TYPE_BUFFER) == 0) {
		return GST_PAD_PROBE_OK;
	}
	buf = GST_PAD_PROBE_INFO_BUFFER(info);
	frame_idx = p->vt_frame_count++;
	if (p->early_period != 0 && frame_idx != 0 && frame_idx % p->early_period == 0 && GST_BUFFER_PTS_IS_VALID(buf)) {
		buf = gst_buffer_make_writable(buf); // the probe holds the reference info carries
		GST_BUFFER_PTS(buf) -= 1;
		GST_PAD_PROBE_INFO_DATA(info) = buf;
	}
	rt = ge_running_time(&p->vt_segment, p->have_vt_segment, buf);
	if (rt == GST_CLOCK_TIME_NONE) {
		ge_fail(p, "a normalized frame entered the tee without running time");
		return GST_PAD_PROBE_OK;
	}
	if (!p->have_phase_v) { // this thread is the only writer
		g_mutex_lock(&p->meta);
		p->phase_v = rt;
		p->have_phase_v = 1;
		g_mutex_unlock(&p->meta);
		return GST_PAD_PROBE_OK;
	}
	if (frame_idx % (GE_KEY_PARTS * GE_VIDEO_PART_AUS) != 0) {
		return GST_PAD_PROBE_OK;
	}
	p->nreq++;
	for (k = 0; k < GE_RUNGS; k++) {
		GstEvent *ev;
		send_ok[k] = -1;
		if (p->nreq == p->withhold_req && (p->withhold_rung < 0 || p->withhold_rung == k)) {
			send_ok[k] = 0;
			continue;
		}
		ev = gst_video_event_new_upstream_force_key_unit(rt, TRUE, p->nreq);
		if (!gst_pad_send_event(p->t[k].enc_src, ev)) {
			send_ok[k] = 0;
			ge_failf(p, "key-unit request at %" G_GUINT64_FORMAT " ns was refused by the encoder of rung %d", rt, k);
		} else {
			send_ok[k] = 1;
		}
	}
	g_printerr("[diag] force-key-unit: req=%u frame=%" G_GUINT64_FORMAT " rt=%" G_GUINT64_FORMAT " pad_accepted_hi=%d pad_accepted_mid=%d pad_accepted_lo=%d\n",
		p->nreq, frame_idx, rt, send_ok[0], send_ok[1], send_ok[2]);
	return GST_PAD_PROBE_OK;
}

// ge_classify walks the length-prefixed NALs of one AVC access unit, nal_length bytes per
// length field: 1 when its slices are all IDR (type 5), 0 when they are all other pictures
// (types 1 to 4), -1 when it is malformed: no length-field size, a length field that overruns
// the buffer, a NAL shorter than its header, a forbidden bit set, IDR and non-IDR slices mixed,
// or no slice at all (parameter sets, SEI and delimiters are not pictures).
static int ge_classify(GstBuffer *buf, int nal_length) {
	GstMapInfo map;
	gsize pos = 0;
	guint64 len;
	int idr = 0, other = 0, type, k, rc = -1;
	if (nal_length < 1 || nal_length > 4 || !gst_buffer_map(buf, &map, GST_MAP_READ)) {
		return -1;
	}
	while (pos < map.size) {
		if (map.size - pos < (gsize)nal_length) {
			goto out;
		}
		for (len = 0, k = 0; k < nal_length; k++) {
			len = (len << 8) | map.data[pos + k];
		}
		pos += (gsize)nal_length;
		if (len < 1 || len > map.size - pos || (map.data[pos] & 0x80) != 0) {
			goto out;
		}
		type = map.data[pos] & 0x1f;
		if (type == 5) {
			idr++;
		} else if (type >= 1 && type <= 4) {
			other++;
		}
		pos += (gsize)len;
	}
	if (idr > 0 && other == 0) {
		rc = 1;
	} else if (other > 0 && idr == 0) {
		rc = 0;
	}
out:
	gst_buffer_unmap(buf, &map);
	return rc;
}

// ge_probe_vparse runs on one rung's parser src pad (user is the rung's ge_ref): it keeps the
// parser's codec_data and the NAL length-field size the avcC declares, classifies every access
// unit by its NALs into the rung's ring (under meta, refusing to overwrite a slot the reader
// has not taken), records the first access unit's PTS and running time and, before access unit
// GE_VIDEO_PART_AUS * i, pushes one split event with the lock released: chunk=false at
// multiples of six, chunk=true otherwise. Boundaries are counted, not timed. Every rung
// receives the same normalized frames in the same order, so access unit 10 i is the same frame
// on each rung and every part holds ten access units; comparing a running time against
// P + i/3 s let a nanosecond of rounding in the parser's timestamps move a boundary by a whole
// frame, which made a part of eleven. The key units are requested by the same count, before
// frame 30 k; the IDR check below catches the two disagreeing. An access unit
// without running time, a malformed one, a running time going backwards, a refused event or a
// reader too far behind fails the pipeline.
//
// A boundary at a multiple of three whose access unit is not an IDR is the one case decided
// later: whether it fails depends on whether the stream was still running. The access unit and
// those after it are dropped before the muxer, on every rung (tail_au is shared, and a rung that
// passed that boundary, or finds an IDR there, fails the pipeline: the rungs would otherwise
// publish different media). If the stream ends within that part, every chunk muxed is a whole,
// scheduled part and the key unit the boundary lacked would only have served media that never
// came: the end is clean, and the Go side reports what was withheld. An access unit at the next
// boundary proves the stream was active, and the omission fails the pipeline with its reason.
static GstPadProbeReturn ge_probe_vparse(GstPad *pad, GstPadProbeInfo *info, gpointer user) {
	ge_ref *r = user;
	ge_pipe *p = r->p;
	ge_rung *g = &p->rung[r->i];
	GstPadProbeType type = GST_PAD_PROBE_INFO_TYPE(info);
	GstBuffer *buf;
	GstEvent *ev;
	guint64 idx, taken, rt, i, tail;
	int have_phase, cls, overflow, have_tail, disagree, k;
	gboolean chunk;
	if ((type & GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM) != 0) {
		ev = GST_PAD_PROBE_INFO_EVENT(info);
		ge_segment(ev, &g->parse_segment, &g->have_parse_segment);
		if (GST_EVENT_TYPE(ev) == GST_EVENT_CAPS) {
			g_mutex_lock(&p->meta);
			ge_codec_data(ev, g->codec_data, &g->codec_data_len);
			g->nal_length = g->codec_data_len >= GE_AVCC_DATA ? (g->codec_data[4] & 3) + 1 : 0;
			g_mutex_unlock(&p->meta);
		}
		return GST_PAD_PROBE_OK;
	}
	if ((type & GST_PAD_PROBE_TYPE_BUFFER) == 0) {
		return GST_PAD_PROBE_OK;
	}
	buf = GST_PAD_PROBE_INFO_BUFFER(info);
	rt = ge_running_time(&g->parse_segment, g->have_parse_segment, buf);
	cls = ge_classify(buf, g->nal_length); // this thread wrote nal_length
	g_mutex_lock(&p->meta);
	idx = g->au_count;
	taken = p->t[r->i].au_taken;
	overflow = idx >= taken + GE_AU_RING;
	if (!overflow) {
		g->idr[idx % GE_AU_RING] = cls == 1 ? 1 : 0;
		g->au_count = idx + 1;
		if (!g->have_first && rt != GST_CLOCK_TIME_NONE) {
			g->first_pts = GST_BUFFER_PTS(buf);
			g->first_rt = rt;
			g->have_first = 1;
		}
	}
	have_phase = p->have_phase_v;
	g_mutex_unlock(&p->meta);
	{
		guint64 rem = idx % (GE_KEY_PARTS * GE_VIDEO_PART_AUS);
		if (cls == 1 || rem <= 2 || rem >= (GE_KEY_PARTS * GE_VIDEO_PART_AUS - 2)) {
			guint64 pts = GST_BUFFER_PTS(buf);
			guint64 dts = GST_BUFFER_DTS(buf);
			g_printerr("[diag] au: rung=%d idx=%" G_GUINT64_FORMAT " cls=%d (%s) pts=%" G_GUINT64_FORMAT " dts=%" G_GUINT64_FORMAT " rt=%" G_GUINT64_FORMAT "\n",
				r->i, idx, cls, cls == 1 ? "IDR" : "non-IDR", pts, dts, rt);
		}
	}
	if (overflow) {
		ge_failf(p, "rung %d access-unit tracking overflowed: the reader is %" G_GUINT64_FORMAT " access units behind", r->i, idx - taken);
		return GST_PAD_PROBE_OK;
	}
	if (ge_failed(p)) {
		return GST_PAD_PROBE_OK;
	}
	if (!have_phase || rt == GST_CLOCK_TIME_NONE) {
		ge_failf(p, "rung %d access unit %" G_GUINT64_FORMAT " has no running time", r->i, idx);
		return GST_PAD_PROBE_OK;
	}
	if (cls < 0) {
		ge_failf(p, "rung %d access unit %" G_GUINT64_FORMAT " is malformed, holds no picture or has no NAL length-field size", r->i, idx);
		return GST_PAD_PROBE_OK;
	}
	if (g->have_last && rt < g->last_rt) {
		ge_failf(p, "rung %d access unit %" G_GUINT64_FORMAT " at %" G_GUINT64_FORMAT " ns is before the previous one at %" G_GUINT64_FORMAT " ns",
			r->i, idx, rt, g->last_rt);
		return GST_PAD_PROBE_OK;
	}
	g->last_rt = rt;
	g->have_last = 1;
	i = g->nsplit + 1;
	if (idx < i * GE_VIDEO_PART_AUS) {
		return GST_PAD_PROBE_OK;
	}
	g_mutex_lock(&p->meta);
	have_tail = p->have_tail;
	tail = p->tail_au;
	g_mutex_unlock(&p->meta);
	if (have_tail && tail <= idx) { // withheld on some rung; this rung is at or past it
		g_printerr("[diag] au-withheld-drop: rung=%d idx=%" G_GUINT64_FORMAT " tail=%" G_GUINT64_FORMAT " cls=%d\n", r->i, idx, tail, cls);
		if (tail < i * GE_VIDEO_PART_AUS) {
			ge_failf(p, "rung %d published access unit %" G_GUINT64_FORMAT ", withheld on another rung as the stream's tail", r->i, tail);
		} else if (idx == tail && cls == 1) {
			ge_failf(p, "rung %d: access unit %" G_GUINT64_FORMAT " is an IDR, withheld on another rung as a non-IDR", r->i, idx);
		} else if (idx >= tail + GE_VIDEO_PART_AUS) {
			ge_failf(p, "rung %d: boundary %" G_GUINT64_FORMAT " falls on access unit %" G_GUINT64_FORMAT ", not an IDR, and the stream went on to access unit %" G_GUINT64_FORMAT,
				r->i, tail / GE_VIDEO_PART_AUS, tail, idx);
		}
		return GST_PAD_PROBE_DROP;
	}
	if (i % GE_KEY_PARTS == 0 && cls != 1) {
		int set_tail = 0;
		disagree = 0;
		g_mutex_lock(&p->meta);
		for (k = 0; k < GE_RUNGS; k++) {
			disagree = disagree || p->rung[k].nsplit >= i; // that rung split here, on an IDR
		}
		if (!p->have_tail) {
			p->tail_au = idx;
			p->have_tail = 1;
			set_tail = 1;
		} else if (p->tail_au != idx) {
			disagree = 1;
		}
		g_mutex_unlock(&p->meta);
		if (set_tail) {
			g_printerr("[diag] withheld-tail-set: rung=%d au=%" G_GUINT64_FORMAT " boundary=%" G_GUINT64_FORMAT " rt=%" G_GUINT64_FORMAT "\n",
				r->i, idx, i, rt);
		}
		if (disagree) {
			ge_failf(p, "rung %d: boundary %" G_GUINT64_FORMAT " falls on access unit %" G_GUINT64_FORMAT ", not an IDR, which another rung passed", r->i, i, idx);
		}
		return GST_PAD_PROBE_DROP;
	}
	chunk = i % GE_SEGMENT_PARTS != 0;
	if (!ge_split_now(pad, chunk)) {
		ge_failf(p, "rung %d: split %" G_GUINT64_FORMAT " before access unit %" G_GUINT64_FORMAT " was refused", r->i, i, idx);
		return GST_PAD_PROBE_OK;
	}
	g_mutex_lock(&p->meta);
	g->nsplit = i;
	g_mutex_unlock(&p->meta);
	return GST_PAD_PROBE_OK;
}

// ge_probe_aenc runs on the mix's encoder sink pad: it keeps the negotiated PCM layout, counts
// the samples per channel of every buffer entering the encoder and fails the pipeline when a
// buffer's running time strays from the sample count by more than half an AAC frame.
static GstPadProbeReturn ge_probe_aenc(GstPad *pad, GstPadProbeInfo *info, gpointer user) {
	ge_ref *r = user;
	ge_pipe *p = r->p;
	ge_mix *m = &p->a;
	GstPadProbeType type = GST_PAD_PROBE_INFO_TYPE(info);
	GstEvent *ev;
	GstCaps *caps;
	GstBuffer *buf;
	guint64 rt, expected, diff, samples;
	int rate;
	if ((type & GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM) != 0) {
		ev = GST_PAD_PROBE_INFO_EVENT(info);
		ge_segment(ev, &m->asrc_segment, &m->have_asrc_segment);
		if (GST_EVENT_TYPE(ev) == GST_EVENT_CAPS) {
			gst_event_parse_caps(ev, &caps);
			m->have_pcm = gst_audio_info_from_caps(&m->pcm, caps) && GST_AUDIO_INFO_BPF(&m->pcm) > 0 && GST_AUDIO_INFO_RATE(&m->pcm) > 0;
		}
		return GST_PAD_PROBE_OK;
	}
	if ((type & GST_PAD_PROBE_TYPE_BUFFER) == 0) {
		return GST_PAD_PROBE_OK;
	}
	buf = GST_PAD_PROBE_INFO_BUFFER(info);
	if (!m->have_pcm) {
		ge_fail(p, "a PCM buffer entered the encoder before usable caps");
		return GST_PAD_PROBE_OK;
	}
	rate = GST_AUDIO_INFO_RATE(&m->pcm);
	samples = gst_buffer_get_size(buf) / GST_AUDIO_INFO_BPF(&m->pcm);
	rt = ge_running_time(&m->asrc_segment, m->have_asrc_segment, buf);
	if (rt == GST_CLOCK_TIME_NONE) {
		ge_fail(p, "a PCM buffer entered the encoder without running time");
	} else if (!m->have_pcm_t0) {
		m->pcm_t0 = rt;
		m->have_pcm_t0 = 1;
	} else {
		expected = m->pcm_t0 + gst_util_uint64_scale(m->pcm_samples, GST_SECOND, (guint64)rate);
		diff = rt > expected ? rt - expected : expected - rt;
		if (diff > gst_util_uint64_scale(GE_AAC_FRAME / 2, GST_SECOND, (guint64)rate)) {
			ge_failf(p, "PCM at %" G_GUINT64_FORMAT " ns after %" G_GUINT64_FORMAT " samples, expected %" G_GUINT64_FORMAT " ns",
				rt, m->pcm_samples, expected);
		}
	}
	m->pcm_samples += samples;
	return GST_PAD_PROBE_OK;
}

// ge_probe_aparse runs on the mix's parser src pad: it fixes the phase A and the first raw PTS
// at the first access unit (under meta) and, with the lock released, pushes one split event
// before the access unit that ends the open segment (chunk=false; the end is ge_aseg_end of P
// and A) and before every 15th unit within it (chunk=true). Audio before P, a unit without
// running time or a segment end that does not advance fails the pipeline.
static GstPadProbeReturn ge_probe_aparse(GstPad *pad, GstPadProbeInfo *info, gpointer user) {
	ge_ref *r = user;
	ge_pipe *p = r->p;
	ge_mix *m = &p->a;
	GstPadProbeType type = GST_PAD_PROBE_INFO_TYPE(info);
	GstBuffer *buf;
	guint64 idx, rt, end;
	int have_v, first = 0;
	gboolean chunk;
	if ((type & GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM) != 0) {
		ge_segment(GST_PAD_PROBE_INFO_EVENT(info), &m->aenc_segment, &m->have_aenc_segment);
		return GST_PAD_PROBE_OK;
	}
	if ((type & GST_PAD_PROBE_TYPE_BUFFER) == 0) {
		return GST_PAD_PROBE_OK;
	}
	buf = GST_PAD_PROBE_INFO_BUFFER(info);
	rt = ge_running_time(&m->aenc_segment, m->have_aenc_segment, buf);
	idx = m->au_count++;
	g_mutex_lock(&p->meta);
	have_v = p->have_phase_v;
	if (!m->have_phase_a && have_v && rt != GST_CLOCK_TIME_NONE) {
		m->phase_a = rt;
		m->first_pts = GST_BUFFER_PTS(buf);
		m->have_phase_a = 1;
		first = 1;
	}
	g_mutex_unlock(&p->meta);
	if (ge_failed(p)) {
		return GST_PAD_PROBE_OK;
	}
	if (!have_v) {
		ge_fail(p, "an audio access unit left the parser before the video phase");
		return GST_PAD_PROBE_OK;
	}
	if (rt == GST_CLOCK_TIME_NONE) {
		ge_failf(p, "audio access unit %" G_GUINT64_FORMAT " has no running time", idx);
		return GST_PAD_PROBE_OK;
	}
	if (first) {
		end = ge_aseg_end(p, m, 0);
		if (end == 0) {
			ge_fail(p, "the first audio segment would end before its first access unit");
			return GST_PAD_PROBE_OK;
		}
		m->seg_start = 0;
		m->seg_end = end;
		m->nseg = 1;
	}
	if (idx == m->seg_end) {
		chunk = FALSE;
		end = ge_aseg_end(p, m, m->nseg);
		if (end <= idx) {
			ge_failf(p, "audio segment %" G_GUINT64_FORMAT " would end at access unit %" G_GUINT64_FORMAT ", not after %" G_GUINT64_FORMAT,
				m->nseg + 1, end, idx);
			return GST_PAD_PROBE_OK;
		}
		m->seg_start = idx;
		m->seg_end = end;
		m->nseg++;
	} else if (idx > m->seg_start && (idx - m->seg_start) % GE_AAC_PART == 0) {
		chunk = TRUE;
	} else {
		return GST_PAD_PROBE_OK;
	}
	if (!ge_split_now(pad, chunk)) {
		ge_failf(p, "audio split before access unit %" G_GUINT64_FORMAT " was refused", idx);
	}
	return GST_PAD_PROBE_OK;
}

// ge_pull waits up to timeout_ns for one sample from the appsink of output track: 1 a sample,
// described in out and retained as pending until ge_take; 0 nothing within the timeout; -1
// EOS (the appsink is drained, which a stopped appsink reports too); -2 a sample is still
// pending. In the sample's buffer list (or its single buffer) a DISCONT|HEADER buffer is the
// initialization segment, a HEADER buffer a chunk header, the non-header buffers after a chunk
// header its payload; whatever else is counted as stray. A chunk's first access unit is the
// running payload count of the track, and for a rung its IDR classification is read from the
// ring under meta, as are the phases copied into out.
static int ge_pull(ge_pipe *p, int track, guint64 timeout_ns, ge_list *out) {
	ge_track *t = &p->t[track];
	ge_rung *g = track < GE_OUT_A ? &p->rung[track] : NULL;
	GstAppSink *sink = GST_APP_SINK(t->sink);
	GstSample *sample;
	GstBufferList *list;
	GstBuffer *single = NULL;
	ge_header *h = NULL;
	guint i, n = 0;
	guint64 taken;
	if (t->pending != NULL) {
		return -2;
	}
	sample = gst_app_sink_try_pull_sample(sink, timeout_ns);
	if (sample == NULL) {
		return gst_app_sink_is_eos(sink) ? -1 : 0;
	}
	memset(out, 0, sizeof(*out));
	list = gst_sample_get_buffer_list(sample);
	if (list != NULL) {
		n = gst_buffer_list_length(list);
	} else if ((single = gst_sample_get_buffer(sample)) != NULL) {
		n = 1;
	}
	g_mutex_lock(&p->meta);
	taken = t->au_taken;
	for (i = 0; i < n; i++) {
		GstBuffer *b = list != NULL ? gst_buffer_list_get(list, i) : single;
		guint64 off = out->total, size = gst_buffer_get_size(b);
		out->total += size;
		if (!GST_BUFFER_FLAG_IS_SET(b, GST_BUFFER_FLAG_HEADER)) {
			if (h != NULL && !h->is_init) {
				h->payload++;
				h->pay_len += size;
				taken++;
			} else {
				out->stray++;
			}
			continue;
		}
		if (out->nheaders >= GE_HEADERS) {
			out->overflow++;
			h = NULL;
			continue;
		}
		h = &out->h[out->nheaders++];
		h->off = off;
		h->hdr_len = size;
		h->pts = GST_BUFFER_PTS(b);
		h->dts = GST_BUFFER_DTS(b);
		h->dur = GST_BUFFER_DURATION(b);
		h->key_unit = -1;
		if (GST_BUFFER_FLAG_IS_SET(b, GST_BUFFER_FLAG_DISCONT)) {
			h->is_init = 1;
			if (g != NULL) {
				out->codec_data_len = g->codec_data_len;
				memcpy(out->codec_data, g->codec_data, sizeof(out->codec_data));
			}
			continue;
		}
		h->seq = GST_BUFFER_OFFSET(b);
		h->fragment_start = GST_BUFFER_FLAG_IS_SET(b, GST_BUFFER_FLAG_DELTA_UNIT) ? 0 : 1;
		h->first_au = taken;
		if (g != NULL && taken < g->au_count) {
			h->key_unit = g->idr[taken % GE_AU_RING];
		}
	}
	t->au_taken = taken;
	out->phase_v = p->phase_v;
	out->have_v = p->have_phase_v;
	if (g == NULL) {
		out->phase_a = p->a.phase_a;
		out->have_a = p->a.have_phase_a;
	}
	g_mutex_unlock(&p->meta);
	t->pending = sample;
	return 1;
}

// ge_take copies the bytes of the pending sample of track, in list order, into the len bytes
// at dst and releases the sample: 0 done, -1 nothing pending, -2 a buffer could not be mapped,
// -3 the bytes do not add up to len. The sample is released on every return.
static int ge_take(ge_pipe *p, int track, guint8 *dst, guint64 len) {
	ge_track *t = &p->t[track];
	GstSample *sample = t->pending;
	GstBufferList *list;
	GstBuffer *single = NULL;
	guint i, n = 0;
	guint64 done = 0;
	int rc = 0;
	if (sample == NULL) {
		return -1;
	}
	t->pending = NULL;
	list = gst_sample_get_buffer_list(sample);
	if (list != NULL) {
		n = gst_buffer_list_length(list);
	} else if ((single = gst_sample_get_buffer(sample)) != NULL) {
		n = 1;
	}
	for (i = 0; i < n && rc == 0; i++) {
		GstBuffer *b = list != NULL ? gst_buffer_list_get(list, i) : single;
		GstMapInfo map;
		if (!gst_buffer_map(b, &map, GST_MAP_READ)) {
			rc = -2;
			break;
		}
		if (done + map.size > len) {
			rc = -3;
		} else {
			memcpy(dst + done, map.data, map.size);
			done += map.size;
		}
		gst_buffer_unmap(b, &map);
	}
	if (rc == 0 && done != len) {
		rc = -3;
	}
	gst_sample_unref(sample);
	return rc;
}

// ge_phases copies P and A as recorded.
static void ge_phases(ge_pipe *p, guint64 *v, guint64 *a, int *have_v, int *have_a) {
	g_mutex_lock(&p->meta);
	*v = p->phase_v;
	*have_v = p->have_phase_v;
	*a = p->a.phase_a;
	*have_a = p->a.have_phase_a;
	g_mutex_unlock(&p->meta);
}

// ge_origins copies the raw PTS and running time of the hi rung's and the mix's first access
// unit, the presentation's clock origins. Every rung sees the same frames on the same clock, so
// the hi rung's pair describes all three.
static void ge_origins(ge_pipe *p, guint64 *vpts, guint64 *vrt, int *have_v, guint64 *apts, guint64 *art, int *have_a) {
	g_mutex_lock(&p->meta);
	*vpts = p->rung[0].first_pts;
	*vrt = p->rung[0].first_rt;
	*have_v = p->rung[0].have_first;
	*apts = p->a.first_pts;
	*art = p->a.phase_a;
	*have_a = p->a.have_phase_a;
	g_mutex_unlock(&p->meta);
}

// ge_tail reports the video withheld at the stream's end: 1 and the first access unit withheld
// when a boundary lacked its key unit and the stream ended within that part, 0 otherwise.
static int ge_tail(ge_pipe *p, guint64 *au) {
	int have;
	g_mutex_lock(&p->meta);
	have = p->have_tail;
	*au = p->tail_au;
	g_mutex_unlock(&p->meta);
	return have;
}

// ge_withhold_key_unit serves the regression tests only, before ge_start: key-unit request n,
// counted from 1, is not sent to the encoder of rung k, or of any rung when k is negative.
static void ge_withhold_key_unit(ge_pipe *p, guint n, int k) {
	p->withhold_req = n;
	p->withhold_rung = k;
}

// ge_early_frames serves the regression tests only, before ge_start: every normalized frame
// at a multiple of period, the first aside, enters the tee with its PTS 1 ns early.
static void ge_early_frames(ge_pipe *p, guint period) {
	p->early_period = period;
}

// ge_key_unit_requests is how many key-unit requests the tee's probe has sent, read after
// ge_stop, when no streaming thread writes it.
static guint ge_key_unit_requests(ge_pipe *p) {
	return p->nreq;
}

// ge_now stores the pipeline's running time (clock minus base time) in ns: 0 done, -1 no
// clock, -2 no usable base time.
static int ge_now(ge_pipe *p, guint64 *ns) {
	GstClock *clk = gst_element_get_clock(p->pipe);
	GstClockTime base = gst_element_get_base_time(p->pipe);
	GstClockTime now;
	if (clk == NULL) {
		return -1;
	}
	now = gst_clock_get_time(clk);
	gst_object_unref(clk);
	if (base == GST_CLOCK_TIME_NONE || now == GST_CLOCK_TIME_NONE || now < base) {
		return -2;
	}
	*ns = now - base;
	return 0;
}

// ge_push copies len bytes into a new GstBuffer stamped pts (and dur when positive) and queues
// it on the appsrc of input: 0 queued, 1 rejected because the backlog would exceed max_bytes
// (nothing allocated), -1 allocation failed, -2 the push failed, -3 no such input.
static int ge_push(ge_pipe *p, int input, const void *data, gsize len, guint64 pts, guint64 dur, guint64 max_bytes) {
	GstAppSrc *src;
	GstBuffer *buf;
	if (input < 0 || input >= GE_INPUTS || p->in[input] == NULL) {
		return -3;
	}
	src = GST_APP_SRC(p->in[input]);
	if (gst_app_src_get_current_level_bytes(src) + len > max_bytes) {
		return 1;
	}
	buf = gst_buffer_new_allocate(NULL, len, NULL);
	if (buf == NULL) {
		return -1;
	}
	gst_buffer_fill(buf, 0, data, len);
	GST_BUFFER_PTS(buf) = pts;
	if (dur > 0) {
		GST_BUFFER_DURATION(buf) = dur;
	}
	// takes ownership of buf, also on failure
	return gst_app_src_push_buffer(src, buf) == GST_FLOW_OK ? 0 : -2;
}

// ge_eos sends EOS into every appsrc: 0 done, otherwise one plus the index of the first input
// that refused it.
static int ge_eos(ge_pipe *p) {
	int i;
	for (i = 0; i < GE_INPUTS; i++) {
		if (gst_app_src_end_of_stream(GST_APP_SRC(p->in[i])) != GST_FLOW_OK) {
			return i + 1;
		}
	}
	return 0;
}

// ge_bus_poll waits up to timeout_ns for an ERROR or EOS message: 1 EOS, 0 nothing, -1 ERROR
// described in buf. Other messages are discarded, so the bus queue never grows.
static int ge_bus_poll(ge_pipe *p, guint64 timeout_ns, char *buf, size_t n) {
	GstMessage *msg = gst_bus_timed_pop_filtered(p->bus, timeout_ns, GST_MESSAGE_ERROR | GST_MESSAGE_EOS);
	GError *err = NULL;
	gchar *dbg = NULL;
	int rc = 1;
	if (msg == NULL) {
		return 0;
	}
	if (GST_MESSAGE_TYPE(msg) == GST_MESSAGE_ERROR) {
		gst_message_parse_error(msg, &err, &dbg);
		g_snprintf(buf, n, "%s: %s", GST_MESSAGE_SRC(msg) != NULL ? GST_OBJECT_NAME(GST_MESSAGE_SRC(msg)) : "?",
			err != NULL ? err->message : "unknown error");
		g_clear_error(&err);
		g_free(dbg);
		rc = -1;
	}
	gst_message_unref(msg);
	return rc;
}

// ge_stop takes the pipeline to NULL: the streaming threads end, so no probe runs afterwards,
// and pending pulls return EOS. Safe to repeat.
static void ge_stop(ge_pipe *p) {
	if (p->pipe != NULL) {
		gst_element_set_state(p->pipe, GST_STATE_NULL);
	}
}

static void ge_release_pad(GstPad *pad, gulong probe) {
	if (pad == NULL) {
		return;
	}
	if (probe != 0) {
		gst_pad_remove_probe(pad, probe);
	}
	gst_object_unref(pad);
}

// ge_close stops the pipeline and releases every handle ge_open acquired, pending samples
// included, also after a partial ge_open. No feeder or reader may still use it.
static void ge_close(ge_pipe *p) {
	int i;
	ge_stop(p);
	for (i = 0; i < GE_OUTPUTS; i++) {
		ge_track *t = &p->t[i];
		ge_release_pad(t->enc_sink, t->probe_enc);
		ge_release_pad(t->parse_src, t->probe_parse);
		if (t->enc_src != NULL) {
			gst_object_unref(t->enc_src);
		}
		if (t->pending != NULL) {
			gst_sample_unref(t->pending);
		}
		if (t->sink != NULL) {
			gst_object_unref(t->sink);
		}
	}
	for (i = 0; i < GE_INPUTS; i++) {
		if (p->in[i] != NULL) {
			gst_object_unref(p->in[i]);
		}
	}
	ge_release_pad(p->vt_sink, p->probe_vt);
	if (p->bus != NULL) {
		gst_object_unref(p->bus);
	}
	if (p->pipe != NULL) {
		gst_object_unref(p->pipe);
	}
	g_mutex_clear(&p->meta);
	g_free(p);
}

// ge_pad resolves the static pad name of the element called elem, or NULL with err set.
static GstPad *ge_pad(ge_pipe *p, const char *elem, const char *name, char *err, size_t n) {
	GstElement *el = gst_bin_get_by_name(GST_BIN(p->pipe), elem);
	GstPad *pad;
	if (el == NULL) {
		g_snprintf(err, n, "pipeline has no element %s", elem);
		return NULL;
	}
	pad = gst_element_get_static_pad(el, name);
	gst_object_unref(el);
	if (pad == NULL) {
		g_snprintf(err, n, "element %s has no %s pad", elem, name);
	}
	return pad;
}

// ge_probe installs one buffer-and-downstream-event probe: the id, 0 with err set.
static gulong ge_probe(GstPad *pad, GstPadProbeCallback cb, gpointer user, const char *what, char *err, size_t n) {
	gulong id = gst_pad_add_probe(pad, GST_PAD_PROBE_TYPE_BUFFER | GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM, cb, user, NULL);
	if (id == 0) {
		g_snprintf(err, n, "a probe could not be installed on %s", what);
	}
	return id;
}

// ge_open builds the pipeline from desc, resolves the fixed element names and installs every
// probe. It leaves the pipeline NULL; ge_start sets it PLAYING. On failure everything acquired
// so far is released.
static ge_pipe *ge_open(const char *desc, char *err, size_t n) {
	GError *e = NULL;
	ge_pipe *p = g_new0(ge_pipe, 1);
	int i;
	g_mutex_init(&p->meta);
	gst_segment_init(&p->vt_segment, GST_FORMAT_UNDEFINED);
	for (i = 0; i < GE_RUNGS; i++) {
		gst_segment_init(&p->rung[i].parse_segment, GST_FORMAT_UNDEFINED);
		p->ref_rung[i].p = p;
		p->ref_rung[i].i = i;
	}
	gst_segment_init(&p->a.asrc_segment, GST_FORMAT_UNDEFINED);
	gst_segment_init(&p->a.aenc_segment, GST_FORMAT_UNDEFINED);
	p->ref_mix.p = p;
	p->ref_mix.i = 0;
	p->withhold_rung = -1;
	p->pipe = gst_parse_launch(desc, &e);
	if (p->pipe == NULL || e != NULL) {
		g_snprintf(err, n, "%s", e != NULL ? e->message : "gst_parse_launch failed");
		g_clear_error(&e);
		goto fail;
	}
	if ((p->in[GE_IN_V] = gst_bin_get_by_name(GST_BIN(p->pipe), "vsrc")) == NULL ||
		(p->in[GE_IN_A] = gst_bin_get_by_name(GST_BIN(p->pipe), "asrc")) == NULL) {
		g_snprintf(err, n, "pipeline has no appsrc vsrc or asrc");
		goto fail;
	}
	for (i = 0; i < GE_RUNGS; i++) {
		ge_track *t = &p->t[i];
		const char *enc = ge_rung_names[i][0], *parse = ge_rung_names[i][1], *sink = ge_rung_names[i][2];
		if ((t->sink = gst_bin_get_by_name(GST_BIN(p->pipe), sink)) == NULL) {
			g_snprintf(err, n, "pipeline has no element %s", sink);
			goto fail;
		}
		if ((t->parse_src = ge_pad(p, parse, "src", err, n)) == NULL || (t->enc_src = ge_pad(p, enc, "src", err, n)) == NULL) {
			goto fail;
		}
		if ((t->probe_parse = ge_probe(t->parse_src, ge_probe_vparse, &p->ref_rung[i], parse, err, n)) == 0) {
			goto fail;
		}
	}
	{
		ge_track *t = &p->t[GE_OUT_A];
		if ((t->sink = gst_bin_get_by_name(GST_BIN(p->pipe), "asink")) == NULL) {
			g_snprintf(err, n, "pipeline has no element asink");
			goto fail;
		}
		if ((t->parse_src = ge_pad(p, "aparse", "src", err, n)) == NULL || (t->enc_sink = ge_pad(p, "aenc", "sink", err, n)) == NULL) {
			goto fail;
		}
		t->probe_enc = ge_probe(t->enc_sink, ge_probe_aenc, &p->ref_mix, "aenc", err, n);
		t->probe_parse = ge_probe(t->parse_src, ge_probe_aparse, &p->ref_mix, "aparse", err, n);
		if (t->probe_enc == 0 || t->probe_parse == 0) {
			goto fail;
		}
	}
	if ((p->vt_sink = ge_pad(p, "vt", "sink", err, n)) == NULL) {
		goto fail;
	}
	if ((p->probe_vt = ge_probe(p->vt_sink, ge_probe_vt, p, "vt", err, n)) == 0) {
		goto fail;
	}
	p->bus = gst_element_get_bus(p->pipe);
	if (p->bus == NULL) {
		g_snprintf(err, n, "pipeline has no bus");
		goto fail;
	}
	return p;
fail:
	ge_close(p);
	return NULL;
}

// ge_start sets the pipeline PLAYING: 0 done, -1 failure described in err.
static int ge_start(ge_pipe *p, char *err, size_t n) {
	if (gst_element_set_state(p->pipe, GST_STATE_PLAYING) == GST_STATE_CHANGE_FAILURE) {
		if (ge_bus_poll(p, 0, err, n) != -1) {
			g_snprintf(err, n, "pipeline cannot enter PLAYING");
		}
		return -1;
	}
	return 0;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	soloWidth, soloHeight = 720, 1280
	aacRate               = 48000
	aacFrameSamples       = 1024
)

type pipeline struct {
	p *C.ge_pipe
}

var gstOnce sync.Once

// describe returns the launch description of the solo pipeline; see the file comment.
func describe() string {
	parts := []string{
		fmt.Sprintf("appsrc name=vsrc is-live=false format=time do-timestamp=false block=false max-bytes=%d "+
			`caps="video/x-h264,stream-format=(string)byte-stream,alignment=(string)nal" ! `+
			"h264parse ! avdec_h264 max-threads=1 ! videoconvert ! videoscale ! videorate skip-to-first=true ! "+
			"video/x-raw,format=I420,width=%d,height=%d,framerate=30/1 ! tee name=vt",
			videoQueueBytes, soloWidth, soloHeight),
	}
	for _, r := range ladder {
		parts = append(parts, fmt.Sprintf("vt. ! queue leaky=no max-size-buffers=30 max-size-bytes=0 max-size-time=0 ! "+
			"videoscale add-borders=false ! video/x-raw,format=I420,width=%d,height=%d,framerate=30/1,pixel-aspect-ratio=1/1 ! "+
			`x264enc name=venc_%s bitrate=%d speed-preset=superfast tune=zerolatency bframes=0 key-int-max=60 option-string="scenecut=0" ! `+
			"h264parse name=vparse_%s ! video/x-h264,stream-format=avc,alignment=au ! "+
			"cmafmux name=vmux_%s manual-split=true send-force-keyunit=false fragment-duration=2000000000 "+
			"chunk-duration=333333333 chunk-mode=duration ! "+
			"appsink name=vsink_%s buffer-list=true sync=false async=false max-buffers=8 drop=false wait-on-eos=false",
			r.width, r.height, r.name, r.kbps, r.name, r.name, r.name))
	}
	// audiorate acts on a timestamp deviation above half an AAC frame, the deviation
	// ge_probe_aenc rejects on the encoder sink pad, instead of its 40 ms default.
	parts = append(parts, fmt.Sprintf("appsrc name=asrc is-live=false format=time do-timestamp=false block=false max-bytes=%d "+
		`caps="audio/x-opus,rate=(int)48000,channels=(int)2,channel-mapping-family=(int)0" ! `+
		"opusdec ! audioconvert ! audioresample ! audiorate skip-to-first=true tolerance=%d ! "+
		"audio/x-raw,format=S16LE,layout=interleaved,rate=%d,channels=2 ! "+
		"voaacenc name=aenc bitrate=128000 perfect-timestamp=true ! aacparse name=aparse ! "+
		"audio/mpeg,mpegversion=4,stream-format=raw ! "+
		"cmafmux name=amux manual-split=true send-force-keyunit=false fragment-duration=2000000000 "+
		"chunk-duration=320000000 chunk-mode=duration ! "+
		"appsink name=asink buffer-list=true sync=false async=false max-buffers=8 drop=false wait-on-eos=false",
		audioQueueBytes, int64(aacFrameSamples)*int64(time.Second)/aacRate/2, aacRate))
	return strings.Join(parts, " ")
}

// openPipeline builds the solo pipeline with every probe in place, without starting it.
func openPipeline() (*pipeline, error) {
	gstOnce.Do(func() { C.gst_init(nil, nil) })
	desc := C.CString(describe())
	defer C.free(unsafe.Pointer(desc))
	var msg [512]C.char
	p := C.ge_open(desc, &msg[0], C.size_t(len(msg)))
	if p == nil {
		return nil, fmt.Errorf("worker: pipeline: %s", C.GoString(&msg[0]))
	}
	return &pipeline{p: p}, nil
}

// clockTime converts a GStreamer clock time to ns, -1 for GST_CLOCK_TIME_NONE.
func clockTime(t C.guint64) int64 {
	if uint64(t) == math.MaxUint64 {
		return -1
	}
	return int64(t)
}

// start sets the pipeline PLAYING. The feeders may push and the readers pull once it returns nil.
func (p *pipeline) start() error {
	var msg [512]C.char
	if C.ge_start(p.p, &msg[0], C.size_t(len(msg))) != 0 {
		return errors.New(C.GoString(&msg[0]))
	}
	return nil
}

// runningTime returns the pipeline's running time: the clock time elapsed since it went
// PLAYING. The first admitted video sample is stamped with it.
func (p *pipeline) runningTime() (time.Duration, error) {
	var ns C.guint64
	if rc := C.ge_now(p.p, &ns); rc != 0 {
		return 0, fmt.Errorf("pipeline clock unavailable (%d)", int(rc))
	}
	return time.Duration(ns), nil
}

// phases returns P and A as recorded so far.
func (p *pipeline) phases() (video, audio int64, haveVideo, haveAudio bool) {
	var v, a C.guint64
	var hv, ha C.int
	C.ge_phases(p.p, &v, &a, &hv, &ha)
	return int64(v), int64(a), hv != 0, ha != 0
}

// origins returns the presentation's clock origins as recorded so far.
func (p *pipeline) origins() origins {
	var vpts, vrt, apts, art C.guint64
	var hv, ha C.int
	C.ge_origins(p.p, &vpts, &vrt, &hv, &apts, &art, &ha)
	return origins{videoPTS: clockTime(vpts), videoRT: clockTime(vrt), audioPTS: clockTime(apts), audioRT: clockTime(art),
		haveVideo: hv != 0, haveAudio: ha != 0}
}

// tail reports the video withheld at the stream's end, if any: the first access unit from
// which every rung dropped frames because the boundary before it lacked its key unit and the
// stream ended within that part. See ge_probe_vparse.
func (p *pipeline) tail() (au int64, withheld bool) {
	var a C.guint64
	if C.ge_tail(p.p, &a) == 0 {
		return 0, false
	}
	return int64(a), true
}

// withholdKeyUnit serves the regression tests only, before start: key-unit request n, the one
// at P + n s, is not sent to the encoder of rung, or of any rung when rung is negative.
func (p *pipeline) withholdKeyUnit(n uint, rung int) {
	C.ge_withhold_key_unit(p.p, C.guint(n), C.int(rung))
}

// earlyFrames serves the regression tests only, before start: every normalized frame at a
// multiple of period, the first aside, enters the tee with its PTS 1 ns early, the rounding
// the key-unit schedule must not move by a frame.
func (p *pipeline) earlyFrames(period uint) {
	C.ge_early_frames(p.p, C.guint(period))
}

// keyUnitRequests is how many key-unit requests the tee's probe sent, for the regression
// tests after stop.
func (p *pipeline) keyUnitRequests() uint {
	return uint(C.ge_key_unit_requests(p.p))
}

// push copies data into a GStreamer-owned buffer stamped with pts and queues it on the appsrc
// of input. dropped reports that the backlog bound was hit and nothing was queued.
func (p *pipeline) push(input int, data []byte, pts, dur time.Duration) (dropped bool, err error) {
	if len(data) == 0 {
		return false, nil
	}
	limit := audioQueueBytes
	if input == inputVideo {
		limit = videoQueueBytes
	}
	pts, dur = max(pts, 0), max(dur, 0)
	switch rc := C.ge_push(p.p, C.int(input), unsafe.Pointer(&data[0]), C.gsize(len(data)),
		C.guint64(pts), C.guint64(dur), C.guint64(limit)); rc {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("appsrc push failed (%d)", int(rc))
	}
}

// endOfStream sends EOS into every appsrc; the feeders must have returned.
func (p *pipeline) endOfStream() error {
	if rc := C.ge_eos(p.p); rc != 0 {
		return fmt.Errorf("appsrc %d refused EOS", int(rc)-1)
	}
	return nil
}

// pull waits up to timeout for one sample from the appsink of output track and returns it
// copied. nil, false, nil means nothing arrived; eos reports a drained appsink, which a stopped
// appsink reports too. One goroutine per track.
func (p *pipeline) pull(track int, timeout time.Duration) (l *pulled, eos bool, err error) {
	var out C.ge_list
	switch rc := C.ge_pull(p.p, C.int(track), C.guint64(timeout.Nanoseconds()), &out); rc {
	case 0:
		return nil, false, nil
	case -1:
		return nil, true, nil
	case 1:
	default:
		return nil, false, fmt.Errorf("sample could not be pulled (%d)", int(rc))
	}
	l = &pulled{data: make([]byte, int(out.total)+1), stray: int(out.stray), overflow: int(out.overflow), // +1: never empty
		phaseVideo: int64(out.phase_v), phaseAudio: int64(out.phase_a), haveVideo: out.have_v != 0, haveAudio: out.have_a != 0}
	rc := C.ge_take(p.p, C.int(track), (*C.guint8)(unsafe.Pointer(&l.data[0])), out.total)
	l.data = l.data[:out.total]
	if rc != 0 {
		return nil, false, fmt.Errorf("sample bytes could not be copied (%d)", int(rc))
	}
	for i := 0; i < int(out.nheaders); i++ {
		h := out.h[i]
		l.headers = append(l.headers, header{isInit: h.is_init != 0, pts: clockTime(h.pts), dts: clockTime(h.dts),
			duration: clockTime(h.dur), sequence: uint64(h.seq), fragmentStart: h.fragment_start != 0, payload: int(h.payload),
			firstAU: int64(h.first_au), keyUnit: int(h.key_unit), off: uint64(h.off), hdrLen: uint64(h.hdr_len), payLen: uint64(h.pay_len)})
	}
	if out.codec_data_len > 0 {
		l.codecData = C.GoBytes(unsafe.Pointer(&out.codec_data[0]), out.codec_data_len)
	}
	return l, false, nil
}

// poll waits up to timeout for the pipeline's ERROR or EOS message.
func (p *pipeline) poll(timeout time.Duration) (eos bool, err error) {
	var msg [512]C.char
	switch C.ge_bus_poll(p.p, C.guint64(timeout.Nanoseconds()), &msg[0], C.size_t(len(msg))) {
	case 1:
		return true, nil
	case -1:
		return false, errors.New(C.GoString(&msg[0]))
	}
	return false, nil
}

// stop takes the pipeline to NULL, after which no probe runs and pending pulls return EOS. It
// keeps every handle; close releases them.
func (p *pipeline) stop() {
	C.ge_stop(p.p)
}

// close stops the pipeline and releases every native handle. No feeder or reader may still
// use it.
func (p *pipeline) close() {
	if p.p != nil {
		C.ge_close(p.p)
		p.p = nil
	}
}
