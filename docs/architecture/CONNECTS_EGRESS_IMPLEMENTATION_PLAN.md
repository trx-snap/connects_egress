# Connects Egress (formerly "Vanguard Unified Live Packager") — Implementation Plan

**Document Version:** 2.3.0 (supersedes 2.2.0, 2.1.0, 2.0.0, 1.0.0 and `VANGUARD_MEDIA_ENGINE_5_YEAR_FUTURE_PROOF_SPEC.md` v1.0.0)
**Date:** 2026-10-06
**Canonical location:** `/Users/foxy/connects-egress/docs/architecture/CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md` (copies elsewhere are non-authoritative snapshots)
**v2.3 change:** Tryon is encoded on the **same 720×656 2×2 atlas as Quadro**, with the 4th cell black (D-C, §4.3); the 1080×328 strip is withdrawn. All ladder rungs are mod16 (`hi`/`mid`) or mod8 (`lo`) (§8.5). The engine, binary and repository are named **`connects-egress`** (D-D, §17.0).
**v2.1 change:** the fixed 720×1280 atlas canvas and the fixed-pixel screen bands are replaced by a **tight atlas** (only the visible video box is encoded) and a **responsive screen layout** measured from TikTok references (§4).
**v2.2 change:** Tryon uses the Quadro-style 2×2 screen with one empty cell, so every audience sees its own host top-left (D-C); three-way scoring is defined (§10.1); the engine lives in a separate repository (D-D, §17.0); final-review fixes to process supervision, Part Store recovery, bridge visibility, audio framing, deploys, recording and the lifecycle table.
**Deliverable:** One Go binary, `connects-egress`, running in three isolated process roles: `supervisor`, `worker`, `bridge`
**Replaces:** LiveKit Egress → RTMP loopback → OvenMediaEngine (multi-container, double encode)
**Status:** Architecture ratified; every performance number is `NOT_VERIFIED` until the Phase 0 gates in §16 pass.

**Evidence labels:** `VERIFIED_SOURCE` (primary source checked in this revision) · `NOT_VERIFIED` (claim or target that still needs a source or a measurement) · `TARGET` (engineering goal, not a fact).

---

## 0. Ratified Product Decisions (2026-10-06)

| ID | Decision | Engineering consequence |
| :--- | :--- | :--- |
| **D-A** | **Each audience sees its own host first** (TikTok behaviour). CPU must be spent on encoding as little as possible. | The server encodes **one perspective-neutral video per match** (the *Atlas Canvas*, §4). The **player** arranges tiles per audience. Audio is mixed **per room** (cheap), so one-sided "mute opponent" works. |
| **D-B** | **Build the cross-server bridge now**; the platform will grow to many servers in many countries. | `connects-egress bridge` relays host media between LiveKit nodes and regions (§6). Hosts never reconnect when a battle starts or ends. The same path serves same-region multi-node and cross-region battles. |
| **D-C** | **Tryon = Quadro-style 2×2 with one empty cell, three-way scoring.** Every audience sees its own creator on the left. | Atlas is the Quadro 720×656 2×2 with 3 host tiles and one black cell (§4.3, v2.3); the player places own host top-left, opponents in the right column, and a Flutter panel over the empty cell (§4.4). Free-for-all scoring (§10.1). |
| **D-D** | **The engine is built as a separate project and repository**, outside `connects_app`, with its own lab. | Repo, lab and contract rules in §17.0. Only versioned contracts and the app-side integration land in ConnectsApp. |

---

## 1. Goals and Non-Goals

**Goals**
1. One binary replaces Egress + OME and encodes the master once (no double encode).
2. All four modes (Solo, 1v1, Tryon, Quadro) run on one media contract, so modes 3–4 only add layout and battle logic.
3. A spectator's playback URL never changes for the life of a room: solo → battle → solo, worker crash, region bridge.
4. A worker failure affects exactly one stream and recovers without spectator action.
5. Multi-region from day one, with no client release needed to add a region (D3 routing contract).

**Non-Goals (this plan)**
- Client-side simulcast (still forbidden; single 720×1280 publisher invariant).
- DVR / rewind.
- MoQ delivery (tracked as a future option, §14).

---

## 2. Vocabulary

| Term | Meaning |
| :--- | :--- |
| **Room** | A creator's audience channel. It owns the stable playback URL `/live/{room_id}/…`, its Centrifugo channel and its chat. One per creator broadcast. |
| **Match** | A battle joining 2–4 rooms (1v1, Tryon, Quadro). |
| **Source** | One running `worker` encode job. A solo room has one source. A match has **one** source shared by all its rooms. |
| **Anchor region / node** | Where a match's source runs. Rule for Phase 1: the inviting host's region and node. |
| **Stage Bridge** | A process that relays a host's WebRTC media from their home SFU into the anchor SFU, and back. |
| **Room Playback Timeline** | The supervisor-owned, per-room view of HLS playlists. It splices sources together while keeping sequence numbers continuous. |

---

## 3. System Architecture

```
                         ┌─────────────── CONTROL PLANE ───────────────┐
                         │ Laravel API · Match Orchestrator · Region    │
                         │ Registry (D3) · Centrifugo · Redis · TigerBeetle│
                         └──────────────┬───────────────────┬──────────┘
                                        │ match/room cmds   │ events (server_ts)
 REGION R1 (e.g. Frankfurt) — ANCHOR    ▼                   ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ LiveKit SFU node(s)          connects-egress supervisor (pure Go, no Cgo)   │
│  ├─ room_a (Host A)           ├─ Part Store (shared memory, owns all parts) │
│  └─ bridged tracks of Host B  ├─ Room Playback Timelines (room_a, room_b*)  │
│         ▲          │          ├─ HTTP origin (LL-HLS) behind NGINX          │
│         │          ▼          ├─ Worker lifecycle, watchdog, admission      │
│  connects-egress bridge connects-egress worker (per source, Cgo+GStreamer)  │
│  (per match, pure Go)   ├─ ingest N hosts (LiveKit Go SDK, §7.1)            │
│         ▲               ├─ decode → tight Atlas compose (video box, §4)     │
│         │               ├─ ONE video ladder (HEVC 3 rungs, §8)              │
│         │               ├─ N per-room audio mixes (AAC, §5)                 │
│         │               └─ writes CMAF parts → supervisor Part Store (IPC)  │
└─────────┼──────────────────────────────────────────────────────────────────┘
          │ server-to-server WebRTC (RTP passthrough, no transcode)
┌─────────┼──────────────────────────────────────────────────────────────────┐
│ REGION R2 (e.g. Singapore) — SPOKE                                          │
│  LiveKit SFU: room_b (Host B) + bridged tracks of Host A                    │
│  supervisor: Room Playback Timeline room_b → mirrors anchor parts (§5.4)    │
│  NGINX edge cache → Room B spectators in Asia                               │
└────────────────────────────────────────────────────────────────────────────┘
```

**Process roles (one binary, `connects-egress <role>`):**

| Role | Language boundary | Count | Crash blast radius |
| :--- | :--- | :--- | :--- |
| `supervisor` | Pure Go, **no Cgo** | 1 per node (systemd) | Playlist serving on that node pauses; workers keep encoding and are re-adopted on restart (§9.4). |
| `worker` | Go + Cgo GStreamer | 1 per **source** (solo room or match) | Exactly one source. |
| `bridge` | Pure Go (LiveKit Go SDK) | 1 per **match** that needs relaying | Interactive media for one match; the spectator stream shows a placeholder tile (§6.5). |

Keeping Cgo out of the supervisor means a native crash can never take down HTTP serving or the timelines.

---

## 4. The Tight Atlas Contract and Screen Layout (Decision D-A)

### 4.1 Principle (v2.1 — replaces the fixed 720×1280 canvas)
- The worker encodes **only the video box the viewer actually sees**. No black regions or unused pixels, except Tryon's single static black cell (§4.3).
- One shared encode per match is kept (D-A). It contains every pixel any audience needs; the **player** arranges tiles for its own audience.
- Header, score bar, names, badges, timers and gift animations stay Flutter overlays.
- Every atlas tile is the **exact pixel size** it is shown at on a 720-wide reference screen, so the player places tiles 1:1 and never scales or letterboxes them. In every mode the atlas aspect also equals the video box aspect (Tryon shares the Quadro 2×2 box, v2.3).

Why v2.0's fixed 720×1280 canvas is withdrawn:
- It wasted ~50 % of encoded pixels in 1v1/Quadro and ~25 % in Tryon (black regions). The compose, scale and memory work for those pixels is still paid.
- It assumed a 9:16, 720×1280 screen with fixed pixel bands. Current phones are about 19.5:9, and TikTok places the score bar **above** the video, not below.
- The cost of the tight atlas: a mode change changes resolution, so it needs a new init segment and a discontinuity at a segment boundary (§4.5). Non-anchor rooms already take that discontinuity, so the extra cost is one discontinuity per battle in the anchor room.

### 4.2 Reference Measurements (TikTok, user screenshots 2026-10-06)
Screens are 472×1024 px (≈ 19.5:9). Values are normalised by screen width **W** (`VERIFIED_SOURCE`: measured from the four reference screenshots, ±2 %).

| Mode | Video box (W × h) | Tiles | Notes |
| :--- | :--- | :--- | :--- |
| Solo | Full screen | 1 | Video is full-bleed, aspect-fill; all UI is overlaid on the video |
| 1v1 | 1.00W × 0.867W | 2 × (0.5W × 0.867W) | Own host left, opponent right |
| Tryon (TikTok reference only) | 1.00W × 0.905W | Left: 0.5W × 0.905W; Right: 2 × (0.5W × 0.45W) | **Not adopted** (D-C): ConnectsApp uses the Quadro 2×2 box for Tryon |
| Quadro | 1.00W × 0.905W | 4 × (0.5W × 0.45W) | 2 × 2 grid |

Common to all battle modes: the score bar (≈ 0.036W thick) sits **directly on top** of the video box. The video box top is ≈ 0.50W from the screen top (≈ 0.23H). Below the video come the contributor row, chat and the input bar.

### 4.3 Atlas Geometry (server side)
All battle canvases are strictly **720 pixels wide**, matching the screen width baseline. No 1080-wide scaling. Every atlas dimension is mod16, so no encoder padding is needed. The only unused area is Tryon's black 4th cell (below).

| Mode | Atlas | Tiles (x, y, w, h) | Tile content |
| :--- | :--- | :--- | :--- |
| **Solo** | 720×1280 | T0 = (0,0,720,1280) | Host, full 9:16 frame |
| **1v1** | **720×624** | T0 = (0,0,360,624), T1 = (360,0,360,624) | Host scaled to 360×640, crop 8 px top and bottom |
| **Tryon** | **720×656** | T0 = (0,0,360,328), T1 = (360,0,360,328), T2 = (0,328,360,328), T3 = (360,328,360,328) **black** | Same 2×2 as Quadro. 3 hosts in T0–T2 (scaled to 360×640, then a 360×328 crop at the crop anchor); T3 is static black |
| **Quadro** | **720×656** | 2 × 2 of 360×328 | Host scaled to 360×640, then a 360×328 crop. 4 equal tiles. |

```
1v1 (720×624)            TRYON (720×656)            QUADRO (720×656)
┌─────────┬─────────┐    ┌─────────┬─────────┐      ┌─────────┬─────────┐
│ T0 A    │ T1 B    │    │ T0 A    │ T1 B    │ 328  │ T0 A    │ T1 B    │ 328
│ 360×624 │ 360×624 │    ├─────────┼─────────┤      ├─────────┼─────────┤
│         │         │    │ T2 C    │ T3 black│ 328  │ T2 C    │ T3 D    │ 328
└─────────┴─────────┘    └─────────┴─────────┘      └─────────┴─────────┘
   no unused pixels       1 static black cell        no unused pixels
```

- **Small-tile crop anchor:** default 40 % from the top of the host frame, because faces sit in the upper part of a portrait frame (`TARGET`, tuned in G-P). It is configurable per match; face-aware anchoring is a future option.
- **Tile-to-host binding** is fixed for the life of a match. Slot order is the order in which hosts accepted. A host who leaves and rejoins gets the same slot back.
- Pixels encoded per mode, relative to Solo (921,600 px): 1v1 49 %, Tryon 51 %, Quadro 51 %.
- **Why Tryon uses 720×656 (v2.3, replaces the 1080×328 strip):** Tryon and Quadro share one geometry, one ladder and one pipeline configuration (Tryon binds 3 inputs instead of 4), every dimension is mod16, and the player shows the atlas 1:1 like Quadro. The cost: the static black cell is coded as skipped macroblocks after the IDR, so its **bitrate** is near zero, but compose/encode **CPU** is still spent on 25 % of the canvas (`NOT_VERIFIED`; measured in G-C).

### 4.4 Player Perspective (what each audience sees)
| Mode | Audience of room X sees | Player work |
| :--- | :--- | :--- |
| Solo | T0 full-bleed (`BoxFit.cover`) | None |
| 1v1 | **Own host left**, opponent right | Swap the two halves for one of the two rooms |
| Tryon | **Own host top-left.** The two opponents fill the right column (slot order). The bottom-left cell is empty video space, filled by a Flutter panel (default: live three-way standings; content is a product choice) | Re-order the 4 cells of the 720×656 atlas so the own host is top-left and the black cell is bottom-left; a Flutter panel covers the black cell |
| Quadro | **Own team left column** (own host top-left), opposing team right column | Re-order 4 equal tiles |

- **Near-zero waste in every mode:** viewers download only host pixels plus, in Tryon, a black cell that costs almost no bits; every host pixel downloaded is displayed.
- Decoder footprint stays at **1 hardware decoder** per spectator. The 2-decoder swipe ceiling is unaffected.
- **Player work package (app side, separate slice):** render decoded texture into the target layout. Lead reference: [`app_vanguard_video_player_view.dart`](file:///Users/foxy/connects_app/connectsapp_app/lib/app/shared/media/playback/vanguard/app_vanguard_video_player_view.dart). Gate **G-P** decides (`NOT_VERIFIED`).
- The host's own battle screen uses the **same layout spec** (§4.6), fed by WebRTC tracks instead of HLS.

### 4.5 Layout Signalling and Mode Changes
- Every source playlist carries `EXT-X-DATERANGE` with `CLASS="com.connectsapp.atlas"`. Attributes: `X-MODE`, `X-ATLAS-VERSION`, `X-SLOTS` and `X-CROP-ANCHOR`.
  - `X-SLOTS` maps slot → host_id → room_id → tile rectangle **normalised to 0–1** of the atlas. The player computes crops from fractions, so ABR rung switches (different pixel sizes) never affect layout.
  - `START-DATE` is aligned to the IDR frame where the new atlas begins.
- A mode change takes effect **only at a GOP boundary**. The worker creates new encoder branches for the new geometry (it does not renegotiate caps on running encoders). Output gets new init segments; every room timeline inserts `EXT-X-DISCONTINUITY` + a new `EXT-X-MAP` (§5.3).
- Transition target: no gap longer than 1 GOP (`TARGET`, Gate G-A).
- Centrifugo sends a `layout_hint` event early so the player can prepare. The **video-aligned DATERANGE is authoritative**: the player switches layout when the playhead crosses `START-DATE`.

### 4.6 Screen Layout Spec (player, responsive)
Sizes are in dp or fractions of screen width W. Fixed pixel bands from v2.0 are withdrawn.

| Element | Rule (battle modes) |
| :--- | :--- |
| Header row 1 | Below the safe area. Creator pill + Follow/Join; top-viewer avatars + viewer count; minimise; close |
| Header row 2 | Chips: league/ranking, gallery/goal progress, event banner |
| Battle band | Space between header and score bar. Holds transient banners (round, win streak). It collapses first on short screens |
| Score bar | Full width, ≈ 0.036W thick, **directly on top of the video box**. 1v1: own side (red) left, opponent (blue) right. Tryon: three segments, one per host (§10.1). Quadro: own team left, other team right |
| Video box | Full width. Height: 0.867W (1v1), 0.911W (Tryon and Quadro, both 2×2 of 360×328). Thin dark gutter between tiles |
| Tile overlays | Tier badge top-left; gift counter bottom-left; name + follow "+" bottom-right on other hosts' tiles; timer pill top-centre on the score-bar seam; VS/result animation centred; team contributor avatars on the row seam (Quadro) |
| Contributor row | Directly below the video. Own side top-3 left, opponent top-3 right |
| Chat | **Flexible**: fills everything between the contributor row and the input bar; newest at bottom. Like-hearts float at the right edge |
| Input bar | Bottom, above the safe area: "Type…" field, emoji, rose quick-gift, gift, share with count |

**Solo:** the video is full-bleed (`BoxFit.cover` over the whole screen). The header, chat (lower-left, ≈ 0.6W wide), like-hearts and input bar are overlaid, with a readability gradient behind the chat. On a 19.5:9 screen, cover crops ≈ 18 % of the width of a 9:16 frame, so the host's camera preview must show the same safe frame.

**Height adaptation (all modes):** header, score bar, video box and input bar keep their sizes. On tall screens the chat grows; on short screens (16:9) the battle band collapses to 0 first, then the chat shrinks to a 3-line minimum (`TARGET`). The video box never shrinks, because it is what was encoded.

### 4.7 Edge Cases
| Situation | Server | Player |
| :--- | :--- | :--- |
| Host media missing > 500 ms (`TARGET`) | Tile shows the last frame; after 2 s, a branded "reconnecting" still | Overlays the host avatar and status |
| Host leaves the match | If ≥ 2 hosts remain, a mode change to the smaller geometry at the next GOP boundary (e.g. Tryon → 1v1); otherwise the match ends (§11) | Re-layout per §4.4 at the DATERANGE |
| Host switches camera ↔ image (Slice I1/I2) | Input caps may change; the decoder renegotiates; tile size unchanged | Nothing |

---

## 5. Audio, Renditions and the Room Playback Timeline

### 5.1 Per-Room Audio Mix Matrix
The worker produces **one AAC mix per room** in the match:

`mix(room_X) = host_X + Σ host_Y   for every Y ≠ X not muted by host_X`

- "Mute opponent" by Host X changes **only** `mix(room_X)`. Each host's own WebRTC monitoring (what the hosts hear) is untouched.
- Cost per extra room = 1 audio mix + 1 AAC encode. That is orders of magnitude cheaper than a second video ladder (`NOT_VERIFIED` magnitude; measured in G-C).

### 5.2 HLS Structure per Room (demuxed audio)
```
/live/{room}/master.m3u8                     ← stable, fetched once
  #EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",URI="a.m3u8",DEFAULT=YES
  #EXT-X-STREAM-INF:...CODECS="hvc1.1.6.L93.B0,mp4a.40.2",AUDIO="aud"  → vhi.m3u8
  #EXT-X-STREAM-INF:...                                               → vmid.m3u8
  #EXT-X-STREAM-INF:...                                               → vlo.m3u8
  (#EXT-X-STREAM-INF: H.264 web rung, only when enabled — §8.4)
/live/{room}/vhi.m3u8   ← room timeline; part URIs point at shared source parts
/live/{room}/a.m3u8     ← room timeline for this room's audio mix
/media/{source_id}/{epoch}/{rendition}/{msn}.{part}.m4s   ← immutable, shared
```
**Shared video bytes:** the video part URIs of every room in a match point at the **same** `/media/{source_id}/…` objects. Edge caches store the video once per match, not once per room. Only the small audio objects differ per room.

**Variants are quality tiers, not resolutions (v2.1):** the room's master playlist is fetched once, but the atlas resolution changes with the mode (§4.3). Each tier (`hi`/`mid`/`lo`) therefore keeps its variant URI for the life of the room. Its resolution changes only across a discontinuity, with a new `EXT-X-MAP`. `BANDWIDTH` per tier is the peak across all geometries the room can enter, and per-tier bitrates are tuned to stay in one band across modes (§8.5). `RESOLUTION` is set to the Solo geometry. The player takes the real size from the init segment and the layout from `X-SLOTS` fractions. Player behaviour across in-variant resolution change is Gate **G-L** (`NOT_VERIFIED`).

### 5.3 Room Playback Timeline (stable URL, continuous numbering)
For each room the supervisor keeps: `room_msn`, `discontinuity_sequence`, and the active `(source_id, epoch, msn_offset)`.
- **Source switch** (solo → match, match → solo, worker restart = new epoch): the next room segment carries `EXT-X-DISCONTINUITY` and a new `EXT-X-MAP` if the init segment changed. `room_msn` keeps increasing; `EXT-X-DISCONTINUITY-SEQUENCE` increments as the window rolls (RFC 8216bis rules).
- Switches happen **only at segment boundaries** of the incoming source, which always start with an IDR frame.
- This one mechanism provides crash continuity (§9.2), solo↔battle transitions and cross-region spectating (§5.4). Spectators never change URL.
- **Player requirement:** LL-HLS discontinuity handling on Media3 and AVPlayer, including in demuxed audio. Gate **G-L** (`NOT_VERIFIED`).

### 5.4 Cross-Region Spectating
Room B's timeline lives on Room B's home-region supervisor (R2). During a match anchored in R1:
- The R2 supervisor **mirrors** the anchor source's parts (blocking-reload pull from the R1 origin over the server-to-server path) into its local Part Store. It then serves room_b's timeline locally.
- Asian spectators keep hitting the R2 edge. The long-haul pull happens **once per part per region**, not once per viewer.
- The added spectator latency equals the R1→R2 part fetch time (`TARGET` ≤ 250 ms; measured in G-B).

---

## 6. The Stage Bridge (Decision D-B)

### 6.1 Why it is required
Self-hosted LiveKit pins each room to a single node. Splitting one room across nodes is not supported in open-source deployments (`VERIFIED_SOURCE`, LiveKit distributed-setup docs, §20). A battle between hosts on different nodes or regions therefore needs a media relay.

### 6.2 Topology: hub-and-spoke through the anchor
```
 Host B ──► SFU R2 (room_b) ──bridge leg B→A──► SFU R1 (room_a, anchor) ◄── Host A
 Host B ◄── SFU R2 (room_b) ◄─bridge leg A→B─── SFU R1 (room_a, anchor)
 Tryon/Quadro: every spoke host is relayed into the anchor; the anchor relays every
 other host back out to each spoke. Legs grow O(N), not O(N²).
```
- Hosts **stay connected to their home SFU** for the whole broadcast. Battle start and end only add or remove bridged tracks, so there is never a host reconnect.
- The worker subscribes only to the **anchor** room. Local and bridged hosts look identical to it.
- Same-node battles need no bridge legs. Same-region, different-node battles use the same bridge code over the LAN.

### 6.3 Leg Mechanics
- Each leg is one bridge-owned participant subscribing on the source SFU and one publishing on the destination SFU. Grants are limited to publish/subscribe of the relayed track IDs only.
- **Visibility (open risk):** a `hidden` publisher may not be delivered to host clients, which would make the opponent's video unsubscribable (`NOT_VERIFIED`). G-B tests this with the real Flutter host client. Fallback: a visible participant with a reserved identity prefix and `kind`/attribute `vanguard_bridge`, which the app filters out of participant UIs.
- **RTP passthrough only. No decode or re-encode.** The codec is pinned to H.264 on publishers (§7.4), so the bridge never transcodes.
- RTCP handling: the bridge answers NACKs on the destination leg from its own retransmission buffer. Keyframe requests (PLI/FIR) arriving on the destination leg are forwarded upstream, rate-limited to 1 per 500 ms per track (`TARGET`).
- Track metadata carries `origin_host_id`, `origin_room_id` and `match_id` so the worker and clients can map slots.

### 6.4 Latency Budget (host ↔ host interactivity)
| Segment | Budget (`TARGET`, to measure in G-B) |
| :--- | :--- |
| Host B uplink → SFU R2 | ≤ 60 ms |
| Bridge R2 → R1 (server-to-server, one way) | ≤ 100 ms (FRA–SIN path `NOT_VERIFIED`) |
| SFU R1 → Host A downlink + jitter buffer | ≤ 120 ms |
| **Total one-way glass-to-glass host↔host** | **≤ 350 ms** (matches the interactive-stage target) |

### 6.5 Bridge Failure Behaviour
| Event | Bridge | Worker / spectators | Match |
| :--- | :--- | :--- | :--- |
| Leg packet loss > 10 % for 5 s (`TARGET`) | Request a keyframe; report degraded state | Tile may freeze, then show placeholder | Continue |
| Leg disconnect | Reconnect with backoff 0.5 → 1 → 2 → 4 s (cap 8 s) | Placeholder tile; audio for that host absent from mixes | Continue; `opponent_reconnecting` event |
| Not restored within 20 s (`TARGET`, product-tunable) | Tear down legs | Atlas drops the slot at the next segment | Match ends `connection_lost`; gift scoring frozen at last authoritative Redis state |
| Bridge process crash | Supervisor restarts it (backoff, §9.3) | As for leg disconnect | As for leg disconnect |

---

## 7. Media Pipeline (worker)

### 7.1 Ingest — Gate G-I decides between two options
| Option | Path | Pros | Risks |
| :--- | :--- | :--- | :--- |
| **A (default)** | LiveKit Go SDK (pion) → Go jitter buffer / sample builder → **whole access units** pushed to GStreamer `appsrc` | Same Go↔GStreamer pattern LiveKit Egress uses in production; shares code with the bridge; Go owns PLI/NACK policy | Go↔C crossing per access unit (~30 video + ~50 audio per second per host) — negligible by design, confirmed in G-I |
| **B** | `livekitwebrtcsrc` (gst-plugins-rs) fully inside GStreamer | No Go↔C media crossing (`VERIFIED_SOURCE`: element exists with a LiveKit signaller) | Rust plugin build and packaging; less control over subscription and PLI; separate code path from the bridge |

Correction from v1: the claim that "frames never cross Go↔C" is impossible under Option A and is withdrawn. Option A pushes **access units, not RTP packets**, so crossings are low-rate. The v1 "5,000 Cgo calls/sec stalls the scheduler" claim had no source and is withdrawn.

### 7.2 Jitter, Timing and Synchronisation
- **Per-host jitter buffering** happens at ingest (Option A: Go sample builder with a bounded delay; Option B: `rtpjitterbuffer`). The v1 "`queue max-size-time` as jitter buffer" is withdrawn; a queue is not a jitter buffer.
- **Lip sync within one host:** RTP timestamps are mapped to sender wall clock using RTCP Sender Reports (NTP), and audio/video PTS derive from the same mapping.
- **Across hosts:** compose **on arrival** (minimum latency), not on capture time. Hosts already interact live over WebRTC, so the spectator view matches what the hosts experience. This is a documented choice; capture-time alignment would add the worst host's delay to everyone.
- **Non-blocking compositor:** GStreamer `compositor` in live mode on the pipeline clock at 30 fps. The aggregator `latency` property (starting at 150 ms, tuned in G-I) sets the deadline; a pad that misses it re-uses its last frame. Exact properties (`latency`, `ignore-inactive-pads`, version minimum) are confirmed in G-I (`NOT_VERIFIED`). Audio uses `audiomixer` with the same latency policy.

### 7.3 Video Ladder and Keyframe Alignment (P0 correction)
`key-int-max` only caps the GOP. Scene-cut detection can add extra keyframes, which de-align the rungs and break ABR switching. Rules:
1. **Scene-cut disabled** on every software encoder (x265 `scenecut=0`, x264 `scenecut=0`; hardware encoders: the equivalent setting, verified per backend).
2. **One keyframe clock:** the worker sends a GStreamer force-key-unit event to **all** encoder branches at every 1.000 s of pipeline running time. Encoders run with `key-int-max` ≥ 31 as a safety cap only.
3. Acceptance: identical IDR PTS across all rungs for 1 hour of continuous encoding (Gate G-A).

### 7.4 Publisher Codec Pinning
LiveKit clients may publish VP8 unless configured otherwise. Vanguard publishers **must** publish H.264 only (bridge passthrough and the worker decoder path depend on it). The supervisor rejects or flags non-H.264 tracks at subscribe time with a structured error.

### 7.5 LL-HLS Packaging (corrected)
| Parameter | Value | Note |
| :--- | :--- | :--- |
| GOP | 1.000 s (30 frames), aligned (§7.3) | |
| Part target | 0.333 s (10 frames) | Parts start on frame boundaries; first part of each GOP flagged `INDEPENDENT=YES` |
| Segment | 2.000 s (2 GOPs), starts on an IDR | |
| `EXT-X-SERVER-CONTROL` | `CAN-BLOCK-RELOAD=YES, PART-HOLD-BACK=1.0, CAN-SKIP-UNTIL=12.0` | PART-HOLD-BACK ≥ 3× part target |
| `EXT-X-PRELOAD-HINT` | Next part | |
| `EXT-X-RENDITION-REPORT` | Every other rendition of the room | Needed for fast ABR switching |
| `EXT-X-PROGRAM-DATE-TIME` | Every segment, from an NTP-disciplined clock (chrony) | Drives the event sync in §10 |
| Window | 24 s (12 segments), configurable | Must exceed `CAN-SKIP-UNTIL` so delta updates actually skip |
| Container | CMAF fMP4; HEVC sample entry **`hvc1`** (parameter sets in sample description) | `hvc1` required for Apple playback (`NOT_VERIFIED` here; checked in G-L with Apple's validator) |
| `PART-TARGET` | 0.334 | Video parts are 0.3333 s; rounding must never exceed the target |
| Audio framing | AAC-LC 48 kHz, 1024-sample frames (21.33 ms) | A 0.333 s part is not a whole number of AAC frames. Audio parts = **15 frames (0.320 s)**. Each audio segment ends on the AAC frame nearest the video segment end (± 1 frame), and audio PDT/`tfdt` come from the running sample count, so drift is zero. Validated in G-L |

**Blocking reload semantics (corrects v1 §4.3):** a request with `_HLS_msn`/`_HLS_part` for a **playlist** blocks until that part exists, then returns the **playlist**, never part bytes. Part bytes are served only from part URIs. If the part never arrives within a server timeout (`TARGET` 3× part target), the server answers with an error status per RFC 8216bis guidance, not a stale playlist.

**NGINX in front of the supervisor:** the cache key includes `_HLS_msn`, `_HLS_part` and `_HLS_skip`; `proxy_cache_lock on` collapses concurrent requests. Parts are immutable and cached long. Playlists are cached for their blocking lifetime only. The upstream hop is HTTP/1.1 keep-alive (NGINX `proxy_pass` does not speak HTTP/2 upstream); clients still get HTTP/2 from NGINX.

---

## 8. Codec Strategy (H.265 target, gated)

### 8.1 Decision
**HEVC is the target primary codec** for the mobile ladder. It ships as primary only after Gates **G-C** (quality/CPU) and **G-D** (device decode) pass. If either gate fails, the identical pipeline runs with x264 rungs (pure configuration change, §8.3).

### 8.2 Corrections to v1 claims
| v1 claim | Correction |
| :--- | :--- |
| "99%+ of Android devices decode HEVC in **hardware** (CDD mandate)" | **Withdrawn.** The Android CDD requires HEVC Main Profile Level 3 *support* but does not require hardware; budget devices may use a software decoder (`VERIFIED_SOURCE`, CDD §5). G-D must check `MediaCodecInfo.isHardwareAccelerated()` (API 29+) on target low-end Unisoc/MediaTek devices plus SM-A566B, including 30-minute thermal runs. |
| "100% of iPhones decode HEVC in hardware" | `NOT_VERIFIED`; G-D covers the iOS minimum supported device. |
| "~41% bandwidth saving; 950/450/220 kbps" | **Planning target only.** HEVC efficiency gains come from slower presets; `ultrafast`/`zerolatency` gives back part of the gain and costs more CPU than x264 `veryfast`. G-C sets the final bitrates. |
| "100% royalty-free for free streaming" | **Withdrawn.** Pool terms for content distribution must be confirmed by counsel, including licensors outside the pools. Added to Legal Gate D8. x265 is GPLv2/commercial; server-side use without distribution goes to counsel under D8. |

### 8.3 Encoder Backend Factory
`EncoderBackend` interface selected at worker start from a probed capability list. On a mid-stream backend failure (session error), the worker falls back to the next backend at the next GOP boundary via a new epoch (a discontinuity, §5.3).

| Priority | Backend | Notes |
| :--- | :--- | :--- |
| 1 | NVENC (`nvh265enc` / `nvh264enc`) | Needs an NVIDIA GPU. Decode (NVDEC) and compose (GPU compositor) must also move to the GPU, otherwise CPU stays the bottleneck. |
| 2 | VAAPI (`vah265enc` / `vah264enc`) | AMD/Intel GPUs. Availability on the target server is `NOT_VERIFIED`. |
| 3 | Software (`x265enc` / `x264enc`) | Launch baseline on Hetzner AX102 (AMD, no discrete GPU). |

Each backend has its **own tuned rate-control profile**. The v1 "zero changes" claim is withdrawn: switching backends means re-tuning, but not changing pipeline or business code.

### 8.4 H.264 Web Rung — lazy
The H.264 web rung is **off by default** and starts only when a client that cannot play HEVC requests the room (player capability query parameter). It stops 60 s after the last such client leaves. This saves the 4th encode for mobile-only audiences. Start-up cost: about one GOP plus a playlist cycle (`TARGET` ≤ 3 s).

### 8.5 Per-Geometry Ladders (v2.3)
Each tier scales the atlas to roughly 100 % / 80 % / 50 % per side. **Invariant:** `hi` and `mid` dimensions are mod16; `lo` dimensions are at least mod8 (avoids hardware-encoder rejection and chroma edge artefacts).

| Geometry | `hi` (mod16) | `mid` (mod16) | `lo` (mod8) |
| :--- | :--- | :--- | :--- |
| Solo 720×1280 | 720×1280 | 576×1024 | 360×640 |
| 1v1 720×624 | 720×624 | 576×496 | 360×312 |
| Tryon & Quadro 720×656 | 720×656 | 576×528 | 360×328 |

- Bitrate per tier stays in one band across geometries (§5.2), so a mode change never forces an ABR down-switch. Battle geometries get more bits per pixel than Solo at the same tier; G-C decides whether to lower them.
- Final dimensions and bitrates are set by Gate G-C (`NOT_VERIFIED`).

---

## 9. Reliability Engineering

### 9.1 Process Model
- systemd runs `supervisor` with `Restart=always`. `setsid` alone does **not** move a child out of the supervisor's cgroup, and systemd's default `KillMode=control-group` kills the whole cgroup on stop/restart (systemd.kill(5); `NOT_VERIFIED` in this revision, checked in G-R).
- Therefore every worker and bridge is started as its **own transient systemd unit** (D-Bus `StartTransientUnit`, e.g. `vanguard-worker-<source_id>.service`) inside `vanguard-media.slice`. Each unit has its own CPU quota, `MemoryMax` (OOM contained to one source) and `TasksMax`. A supervisor restart never touches them.

### 9.2 Part Store (crash continuity)
- The **supervisor owns** all media part memory: `memfd`-backed ring buffers per source and rendition, mapped read-write into the worker and read-only into HTTP serving.
- **Recovery after a supervisor restart:** each memfd is also stored in systemd's file-descriptor store (`FileDescriptorStoreMax=`, `sd_pid_notify_with_fds`, named by `source_id/epoch/rendition`). A restarted supervisor receives the fds back, re-maps them, and re-handshakes with workers over their Unix sockets. If an fd is missing, the affected source restarts as a new epoch (DISCONTINUITY) rather than serving unknown memory.
- The worker writes a part, then publishes `(source, epoch, rendition, msn, part, offset, len, flags, pts, pdt)` over a Unix socket. A part becomes visible only after that publish (no torn reads).
- **Worker crash:** already-published parts stay servable. Timelines stop advancing; blocking requests wait. The supervisor restarts the worker as a **new epoch**, and room timelines append `EXT-X-DISCONTINUITY` (§5.3).
- Restart target: gap between the last part before the crash and the first after ≤ 3 s (`TARGET`, Gate G-R). The v1 "300 ms" figure had no source and is withdrawn; rejoin + keyframe request + decoder/encoder warm-up dominate.

### 9.3 Failure Detection and Containment
| Failure | Detection | Action |
| :--- | :--- | :--- |
| Worker crash | `SIGCHLD` / pidfd | Restart as new epoch |
| Worker **hang** (deadlock, stalled pipeline) | Heartbeat: published-parts counter must advance every ≤ 1.5 s (`TARGET`) | `SIGKILL`, then restart |
| Crash loop (e.g. a host sending poison NAL units) | ≥ 3 restarts in 60 s | **Circuit breaker:** stop restarting; drop the offending host's slot if identifiable (per-host decoder error counters), otherwise end the stream with `media_error`; alert |
| Bridge crash / hang | Same heartbeat model (RTP forward counters) | Restart with backoff |
| Supervisor crash | systemd | On start, re-adopt live workers via their Unix sockets and rebuild timelines from the Part Store index and persisted timeline state (§9.5) |

### 9.4 Admission Control and Overload Policy
- Each node advertises **encode budget units** (measured per mode in G-C) to the Region Registry. The orchestrator places sources only where budget remains, with ≥ 15 % headroom (`TARGET`).
- Overload degradation order (per node, automatic):
  1. disable lazy H.264 web rungs,
  2. step the software encoder preset one notch faster,
  3. refuse new matches (solo still admitted),
  4. refuse new sources; route to another node or region.
- Never degrade by dropping frames from running streams.

### 9.5 Persisted State
Room timeline state (`room_msn`, discontinuity sequence, active source/epoch/offset) is written to a local append-only file on every source switch and once per segment. A supervisor restart therefore resumes numbering exactly, never resetting a room's MSN.

### 9.6 Deploys
One supervisor per node (it owns the origin port). A deploy is an **in-place supervisor restart** using the same re-adoption path as a crash (§9.2, §9.3). Workers and bridges are versioned binaries: running ones finish with the version they started with; new sources start the new version. Versioned IPC protocol (`ipc_version` in the handshake); the supervisor supports the current and previous major version, and refuses anything else.

### 9.7 Node Failure (explicit launch risk)
The two-server launch has **no origin redundancy within a region**. If a node dies, its sources die; hosts reconnect and new sources start on the other node or region (stream URL is stable, spectators see a long gap). Registered in the risk register (§18). It is removed when each region has ≥ 2 media nodes.

---

## 10. Gift / Score ↔ Video Synchronisation
Spectator video is 2–4 s behind real time, but Centrifugo events arrive in under 400 ms. Unless corrected, score bars and gift animations would play **before** the host reacts on screen.
- Every gift/score/timer event carries `server_ts` (NTP-disciplined).
- The player maps its playhead to wall clock using `EXT-X-PROGRAM-DATE-TIME`, and renders an event when `playhead_pdt ≥ server_ts`. Cap: render anyway after 5 s.
- **Exception:** the gifting user's own optimistic echo is instant (unchanged D4 behaviour).
- Battle timers display relative to playhead PDT, so all audiences see the same countdown aligned with the video.
- Acceptance: HUD-to-video skew ≤ 200 ms (`TARGET`, Gate G-S).

### 10.1 Battle Scoring Models
| Mode | Model | Score bar | Winner |
| :--- | :--- | :--- | :--- |
| 1v1 | Two-way | Two segments, proportional; own host left | Higher score |
| **Tryon** | **Three-way free-for-all** (no teams) | Three segments proportional to each host's score. Each host keeps one fixed colour for the whole match (tile badge = bar colour). In each room the own host's segment is leftmost | Highest score. Live rank badge (1st/2nd/3rd) on each tile. Tie for first at the end: default draw between the tied hosts (product-tunable) |
| Quadro | Two teams | `team_red = A + C`, `team_blue = B + D`; own team left | Higher team score |

- Scores live in Redis (authoritative per match); diamonds are always credited to the individual host in TigerBeetle, in every mode.
- Top contributors are tracked per host; the contributor row shows the own host's top 3 on the left and the leading opponent's top 3 on the right (Tryon).

---

## 11. Match Lifecycle — Terminal-State Table

| State / Event | Orchestrator | Bridge | Worker (anchor) | Room timelines | Spectator player |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Invite accepted (start)** | Pick anchor (inviter's node); reserve encode budget | Create legs if hosts are on different nodes | Anchor room's solo worker keeps running (same source, same epoch) and starts new encoder branches for the match geometry at the next GOP boundary (§4.5) | Anchor room: DISCONTINUITY + new `EXT-X-MAP` (resolution change). Other rooms: switch to the anchor source at the same boundary (DISCONTINUITY) | Layout switches at DATERANGE; audio continues |
| **Anchor node out of budget** | Try the invitee's node as anchor; if neither has budget, reject the invite with `capacity_unavailable` (no media change) | — | Unchanged | Unchanged | Inviter sees "battle unavailable, try again" |
| **Active** | Score/timer authority (Redis) | Relay; monitor | Compose + 1 ladder + N mixes | Advance | Per-room perspective |
| **Host mutes opponent** | Persist flag | — | Rebuild `mix(room_X)` only | — | Hears updated mix |
| **Host media lost** | `opponent_reconnecting` event | Reconnect (§6.5) | Placeholder tile | — | Avatar overlay |
| **Bridge irrecoverable** | End match `connection_lost` | Tear down | Drop slot at next segment | Spoke rooms switch back to their own solo source | Back to solo layout |
| **Worker crash** | — | Unaffected | Restarted as new epoch | DISCONTINUITY | Brief stall, auto-recovers |
| **Mode change (1v1 → Tryon)** | Add host; re-check budget (one more decode and audio mix) | Add legs | New atlas version + new encoder branches at a GOP boundary | All rooms: DISCONTINUITY + new `EXT-X-MAP`; new room joins the source | Re-layout at DATERANGE |
| **Match end (timer) / cancel** | Final score; settle ephemeral state | Tear down legs | Back to the Solo geometry at a GOP boundary | Anchor room: DISCONTINUITY + new `EXT-X-MAP`. Non-anchor rooms switch to their own new solo sources (DISCONTINUITY) | Solo layout |
| **Host ends broadcast mid-match** | Host's room closes; match continues if ≥ 2 hosts remain, else ends | Remove legs | Drop slot | Closed room's timeline ends (`EXT-X-ENDLIST`) | That audience sees "live ended" |
| **Node drain / shutdown** | Stop placing sources | Legs finish with their match | Run to completion, or hand off at match end | — | — |

Non-anchor rooms need their own solo source again after a match. To make the return fast, the orchestrator **pre-starts** that solo worker during the final 10 s of the match (`TARGET`).

---

## 12. Security
- Playback URLs carry a short-lived signed token for private or paid rooms. NGINX validates the token and **strips it from the cache key**, so authorised viewers share cache entries.
- Bridge, worker and supervisor LiveKit tokens use the least privilege needed (hidden, subscribe-only or specific-track publish).
- Supervisor ↔ worker IPC runs over Unix sockets with filesystem permissions; the HTTP origin binds to loopback behind NGINX.

## 13. Observability
Per source: input fps per host, decode errors per host, compositor late-frame count, encoder fps per rung, IDR alignment check, part publish latency (encoder output → visible), Part Store occupancy.
Per bridge leg: RTT, loss, NACK/PLI rates, forward bitrate.
Per room: blocking-request wait p50/p99, MSN, discontinuity count.
Initial SLOs (`TARGET`): part publish latency p99 ≤ 50 ms; unplanned discontinuities ≤ 1 per stream-hour; host↔host one-way ≤ 350 ms p95.
All logs structured JSON keyed by `room_id`, `match_id`, `source_id`, `epoch`.

## 14. Memory and Future Delivery
- **GC (corrects v1 Pillar 6):** Go's stop-the-world pauses are typically sub-millisecond, so the v1 "20–50 ms GC pauses" claim is withdrawn. `sync.Pool` may drop items at any time and is not a fixed slab. Part memory now lives in the supervisor's `memfd` Part Store (§9.2), which removes per-part heap churn by design. Go heap tuning (`GOGC`, `GOMEMLIMIT`) is set from profiling, not assumed.
- **MoQ (Media over QUIC):** speculative. The CMAF parts in the Part Store are the natural input for a future MoQ publisher (MoQ uses QUIC streams). Not on the roadmap until the IETF specs and client stacks stabilise; no latency claims are made.

---

## 15. Capacity Model (replaces v1 tables)
v1 room counts assumed x264 and one output per battle, so they are **withdrawn**. Planning formula:

```
cost(source) = C_ladder(codec, backend)              ← one video ladder per source
             + N_hosts × C_decode(720p H.264)
             + C_compose(mode)                        ← scales with atlas pixels (§4.3)
             + N_rooms × C_audio(mix + AAC)
             + [C_h264_web if lazily enabled]
```
- Under D-A, a 1v1 match costs about **one** video ladder plus one extra decode and one extra audio mix, not two ladders.
- With the tight atlas, `C_ladder` scales with encoded pixels: a 1v1 ladder encodes ≈ 49 % of a Solo ladder's pixels, Tryon and Quadro ≈ 51 % (§4.3). CPU per pixel is not exactly linear; G-C measures it.
- Node budget units and per-mode costs are filled in by Gate G-C on the actual AX102. The master architecture's admission cap of "20–25 rooms" is replaced by **measured budget units**.

---

## 16. Phase 0 — Spike Gates (must pass before Phase 1 code is frozen)

| Gate | Proves | Acceptance (`TARGET` unless stated) |
| :--- | :--- | :--- |
| **G-I** Ingest | Option A vs B; compositor late-frame policy | 4 hosts × 2 h with 15 % loss on one host: other tiles show 0 stalls; Go↔C overhead < 1 % CPU (Option A) |
| **G-C** Codec/CPU | x265 (ultrafast/superfast) vs x264 (veryfast) on recorded Solo, 1v1, Quadro and Tryon atlas content (§4.3 geometries) | VMAF per tier at candidate bitrates; CPU per source per mode → budget units; final per-geometry ladders (§8.5) |
| **G-D** Device decode | HEVC hardware decode + 30-min thermals on SM-A566B, ≥ 2 low-end Unisoc/MediaTek devices, minimum iPhone | `isHardwareAccelerated()==true` (Android); no thermal throttle below 30 fps; 1 decoder in use |
| **G-A** ABR alignment | Forced-key-unit design; mode-change branch swap | Identical IDR PTS across rungs for 1 h; 50 forced rung switches without a stall; Solo ↔ 1v1 ↔ Tryon swaps with no gap > 1 GOP |
| **G-L** LL-HLS conformance | Playlists, demuxed audio, audio framing (§7.5), discontinuities, `hvc1`, in-variant resolution change | Apple `mediastreamvalidator` clean; audio/video PDT skew ≤ 1 AAC frame over 1 h; Media3 + AVPlayer play through source switches, worker restart and 50 Solo ↔ battle resolution changes without a stall or ABR reset |
| **G-R** Crash continuity | Part Store + epochs + process supervision | `kill -9` worker: gap ≤ 3 s. `systemctl restart` and `kill -9` of the supervisor: workers keep running, memfds recovered from the fd store, no discontinuity, MSN monotonic. Players recover without user action |
| **G-B** Bridge | FRA↔SIN legs | Host↔host one-way ≤ 350 ms p95; leg loss recovers per §6.5; grant scope verified; **real Flutter host client can see and subscribe to the bridged track** (decides hidden vs filtered-visible participant, §6.3) |
| **G-P** Player atlas layout | Multi-crop rendering + §4.6 screen spec | All 4 perspectives correct; 1 hardware decoder; ≥ 30 fps render on low-end device; layout checked side by side with the TikTok references on 19.5:9, 20:9 and 16:9 screens; crop anchor tuned |
| **G-S** Event sync | PDT-aligned HUD | HUD-to-video skew ≤ 200 ms |

---

## 17. Delivery Roadmap

### 17.0 Repository, Lab and Contract Boundary (Decision D-D)
- **Separate repository** `connects-egress` at `/Users/foxy/connects-egress`, branch `main`, outside `/Users/foxy/connects_app`. Its own `AGENTS.md` forbids any change to `connects_app`.
- **The server code stays in its own repo permanently.** It is a server binary with its own build, CI and deploy cycle, and never merges into the Flutter app. What lands in ConnectsApp at the end is only the integration: player slice, orchestrator/API calls, Centrifugo events.
- **Contracts are the single source of truth** and live in `connects-egress/contracts/`, versioned with semver: playlist/DATERANGE `X-SLOTS` schema, room/match control API, event JSON schemas, IPC protocol. ConnectsApp pins a contract version; a breaking change bumps the major version.
- **Lab:** a local macOS lab exists in `lab/` (native `livekit-server --dev` on `127.0.0.1:7880`, GStreamer + `lk` synthetic publishers at 720×1280 @ 30 fps H.264 + 48 kHz Opus, `start_lab.sh` / `verify_lab.sh`). It covers ingest, compose and encode spikes. Gates that need Linux kernel features (memfd, cgroup v2, systemd fd store, `tc netem`: G-R and the loss parts of G-I) run on a Linux lab host (an AX102 or a cheaper dedicated box).
- **Players for gates:** use the existing Vanguard engine (`packages/vanguard_media_engine`) example app pointed at the lab origin, plus Apple `mediastreamvalidator`. Do not fork the player.
- **Spike code is throwaway:** `spikes/<gate>/` is never imported by `internal/`. Production packages start only after the gate that decides them passes.
- Licensing: keeping x265 (GPLv2) in a server-only repo simplifies Legal Gate D8 review; it is not distributed in the app.

Suggested layout:
```
connects-egress/
  cmd/connects-egress/        # one binary, role switch: supervisor | worker | bridge
  internal/supervisor/        # partstore, timeline, origin, admission, units (systemd)
  internal/worker/            # ingest, compose, encode, package (Cgo/GStreamer)
  internal/bridge/
  internal/ipc/               # versioned protocol
  contracts/                  # schemas consumed by ConnectsApp
  lab/                        # docker-compose, netem, synthetic publishers, fixtures
  spikes/                     # Phase 0 gate code (throwaway)
  docs/                       # this plan, gate reports
```

**Phase 1 — Solo + 1v1 (including bridge)**
1. `supervisor`: Part Store, Room Playback Timelines, LL-HLS origin, watchdog, admission, persisted state.
2. `worker`: ingest (G-I choice), Solo + 1v1 atlas, aligned ladder (G-C codec), per-room audio mixes, CMAF parts.
3. `bridge`: same-region multi-node and cross-region legs (hub-and-spoke).
4. Orchestrator integration: match lifecycle per §11, Region Registry, encode budget units.
5. Player slice (separate app manifest): atlas perspective layout (G-P), PDT event sync (G-S), discontinuity handling (G-L).
6. On-demand recording: remux the `hi` tier + room audio from the Part Store with no extra encode. Because resolution changes between modes, the recording is written as **one MP4 per geometry run** (e.g. `solo_1.mp4`, `1v1_1.mp4`, `solo_2.mp4`) plus a manifest; joining them into one file is a later, optional re-encode job.

**Phase 2 — Tryon + Quadro.** Adds atlas geometries (§4.3), perspective tables (§4.4), hub fan-out for 3–4 hosts, Tryon three-way scoring and Quadro team scoring (§10.1). No new media mechanism is required.

---

## 18. Risk Register

| Risk | Severity | Mitigation / Gate |
| :--- | :--- | :--- |
| HEVC software decode on budget Android → battery/thermal | High | G-D; x264 fallback ladder by configuration |
| x265 CPU too high for planned density | High | G-C; preset tuning; GPU backend later |
| Discontinuity handling bugs in Media3/AVPlayer LL-HLS | High | G-L; player fixes in Vanguard engine |
| Single media node per region (no origin redundancy) | High | Accepted for launch; add ≥ 2 nodes per region |
| Cross-region bridge latency above budget on some routes | Medium | G-B; anchor selection by measured RTT (future) |
| Flutter multi-texture crop performance on low-end devices | Medium | G-P Option P2 (native crop) |
| Player stalls or ABR resets on in-variant resolution change (tight atlas) | Medium | G-L; fallback: separate variant sets per geometry announced via a master-playlist reload |
| HEVC / x265 licensing interpretation | Medium | Legal Gate D8 |
| Contract drift between the `connects-egress` repo and ConnectsApp | Medium | Versioned `contracts/`; app pins a version; lab-generated fixture streams used by app player tests |
| Bridge publisher not visible to host clients if `hidden` | Medium | G-B; filtered-visible participant fallback (§6.3) |

---

## 19. Previous Text Withdrawn by This Revision
1. v1 ingest code that hard-coded API keys and room names (illustrative only; never ship credentials in code).
2. v1 §4.3 blocking-reload sample that returned part bytes on a playlist request (§7.5 is authoritative).
3. v1 §6 per-mode room counts and the "20+ rooms, sub-2.0 s" hardware-ceiling statement (§15 replaces).
4. Server-side final-layout composition at canvas y=100 (replaced by the Atlas Canvas and player perspective, §4).
5. The 5-Year spec's process-restart, GC, licensing, CDD and "80+ rooms" claims (§8.2, §9.2, §14).
6. v2.0's **fixed 720×1280 atlas canvas** with black unused regions, and the fixed-pixel screen bands ("video 720×640 at y=100, score bar 740–800, chat 800–1280"). Replaced by the tight atlas (§4.3) and the responsive screen spec (§4.6).
7. v2.1's three-column 1080×656 Tryon atlas (33 % client-crop discard), and the interim 720×656 Tryon where only Creator A was shown big. Replaced by D-C: Quadro-style 2×2 with an empty cell (§4.3, §4.4).
8. v2.1's `setsid`-only worker survival, blue/green deploy on one node, hidden-grant bridge assumption, `≥ 12 s` window with `CAN-SKIP-UNTIL=12.0`, and single-MP4 recording across mode changes (§9.1, §9.6, §6.3, §7.5, §17).
9. v2.2's 1080×328 three-tile Tryon strip, its 864×262 / 540×164 rungs, and the non-mod16 `mid` rungs 576×500 / 576×524. Replaced by the shared 720×656 Tryon/Quadro atlas and the mod16 ladder (§4.3, §8.5).
10. The working names `vanguard-live` / `vanguard-live-packager`. Replaced by `connects-egress`.

---

## 20. Sources & Verification Log

| Claim | Source | Status |
| :--- | :--- | :--- |
| Self-hosted LiveKit hosts each room on a single node; multi-node scales room count, not room size | LiveKit docs — Distributed setup: https://docs.livekit.io/home/self-hosting/distributed/ | `VERIFIED_SOURCE` (2026-10-06) |
| `livekitwebrtcsrc` exists in gst-plugins-rs (`rswebrtc`) with a LiveKit signaller | GStreamer docs: https://gstreamer.freedesktop.org/documentation/rswebrtc/ | `VERIFIED_SOURCE` (2026-10-06) |
| Android CDD requires HEVC Main Profile L3 support, not specifically hardware | Android CDD §5 (Multimedia): https://source.android.com/docs/compatibility/cdd | `VERIFIED_SOURCE` (2026-10-06) |
| LL-HLS tags and blocking-reload semantics | draft-pantos-hls-rfc8216bis: https://datatracker.ietf.org/doc/draft-pantos-hls-rfc8216bis/ | Cited; re-verify exact clauses in G-L |
| `sync.Pool` items may be removed at any time | https://pkg.go.dev/sync#Pool | Cited; re-verify |
| Go GC pause characteristics | https://go.dev/doc/gc-guide | Cited; re-verify |
| LiveKit Egress uses Go + GStreamer `appsrc` | https://github.com/livekit/egress | Cited; re-verify source path in G-I |
| NGINX `proxy_pass` upstream is HTTP/1.x | https://nginx.org/en/docs/http/ngx_http_proxy_module.html | Cited; re-verify |
| Apple requires `hvc1` sample entry for HEVC in HLS | Apple HLS Authoring Specification | `NOT_VERIFIED` → G-L |
| FRA↔SIN server RTT; all latency/CPU/bitrate figures | — | `NOT_VERIFIED` → G-B / G-C |
