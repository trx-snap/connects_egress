# ConnectsApp: TikTok-Style Livestreaming Architecture Specification

**Document Version:** 5.1.0 (Two-Server Launch Topology, Unified Single-Binary Media Engine `connects-egress` & 4-Mode Media Lifecycle; v5.1 aligns with implementation plan v2.3.0)  
**Canonical location:** `/Users/foxy/connects-egress/docs/architecture/CONNECTSAPP_TIKTOK_STYLE_LIVESTREAM_MASTER_ARCHITECTURE.md` (copies elsewhere are non-authoritative snapshots)  
**System Target:** ConnectsApp Livestreaming Architecture  
**Core Model:** Two-Tier Hybrid Architecture (WebRTC Interactive Stage + Metered HLS/LL-HLS Mass Delivery)  
**Launch Topology:** Two Servers Only (Frankfurt Dedicated Origin/Ingress + Singapore Cloud Edge/Ingress)  
**Core Technologies:** Vanguard Media Engine + LiveKit WebRTC SFU + Unified Live Packager (`connects-egress`) + Centrifugo (WebSockets) + TigerBeetle (Financial Ledger) + Redis (Realtime Game State) + PostgreSQL + Flutter  
**Target Latency:** Interactive Stage 180–350ms (cellular/cross-region), 120–200ms (local Wi-Fi); Mass Audience 2–4s (Launch Baseline; sub-2s stretch goal); Chat/Events up to ~400ms end-to-end with instant optimistic local echo  

---

## 1. Executive Summary & Design Principles

This document defines the production architecture for **ConnectsApp Livestreaming**. It specifies the media delivery, gamified battle mechanics, interactive co-hosting, financial ledger pipelines, and mobile silicon protection required to achieve TikTok-style feature parity while controlling operational costs and ensuring legal compliance.

### Core Architecture Principles

1. **Strictly Two-Tier Delivery:** WebRTC is strictly reserved for the broadcaster, co-host, PK battle opponent, and host-approved multi-guests (<10 active publishers). Mass spectators never connect to WebRTC; they consume cost-effective HLS/LL-HLS streams.
2. **Two-Server Launch Topology & Launch Budget Target (Product Owner Decision D1):**
   * **Launch Candidate Assumption (< €300/mo):** The initial deployment validates a budget-conscious two-server topology targeting under €300/month (planning assumption based on dated source prices, subject to current quote and traffic validation), evaluated using Hetzner Frankfurt dedicated bare metal and Hetzner Singapore Cloud. This provider and cost profile is an assumption to validate in Phase 0 / launch trials, not a permanent architectural lock or product constraint.
   * **Node 1: Frankfurt (Master Origin, 10G NIC):** Master Media Origin, EU/US/Central Asia/MidEast Ingress, Unified Packager (`connects-egress`), RAM-Disk Origin Shield (initial launch candidate: Hetzner Dedicated AX102).
   * **Node 2: Singapore (East Edge / Ingress):** Asia-Pacific & Oceania Ingress/Edge, Southeast Asia / Bangladesh / Australia / NZ Spectator Delivery, Local Packager & Edge Cache (initial launch candidate: Hetzner Cloud CCX).
   * **Future Infrastructure Separation (D2):** Bunny.net Karachi, Gcore Tashkent/Almaty, Sydney, North America, and in-country regional data centres are explicitly deferred to the *Future Regional Infrastructure* roadmap.
3. **Unified Single-Binary Media Engine (`connects-egress`, plan v2.3.0):**
   * Replaces LiveKit Egress + RTMP loopback + OvenMediaEngine with **one Go binary running three isolated process roles**: `supervisor` (pure Go: Part Store, Room Playback Timelines, LL-HLS origin), `worker` (one per source: ingest, compose, one ABR ladder, per-room audio) and `bridge` (cross-node / cross-region host relay).
   * Authoritative spec: [`CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md`](CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md) v2.3.0. Built in the separate `connects-egress` repository at `/Users/foxy/connects-egress` (plan §17.0). All performance figures are `NOT_VERIFIED` until its Phase 0 gates pass.
4. **4 Future-Proof Media Modes (Decision D-A: each audience sees its own host first; one video encode per match):**
   * The worker encodes one perspective-neutral **tight Atlas** per match (only the visible video box; the sole black area is Tryon's empty cell); the Vanguard player crops and arranges tiles per audience. Audio is mixed per room, so one-sided "mute opponent" works.
   * **Mode 1 (Solo):** full-frame 720×1280, shown full-bleed with overlaid UI.
   * **Mode 2 (1v1):** atlas 720×624 (2× 360×624); each audience sees its own host on the left.
   * **Mode 3 (Tryon, three-way free-for-all):** atlas 720×656 (same 2×2 as Quadro: 3× 360×328 host tiles + 1 black cell); each audience sees own host top-left, opponents in the right column and the empty cell bottom-left (covered by a Flutter panel).
   * **Mode 4 (Quadro 2v2):** atlas 720×656 (4× 360×328); each audience sees its own team in the left column.
   * **Decision D-B:** the `bridge` relays host media between LiveKit nodes and regions (self-hosted LiveKit pins each room to one node), so battles work across servers and countries without hosts reconnecting.
5. **Vendor-Neutral Architecture & Hardware Portability (D3):** Software is architected so new regional edges, CDNs, or private data centres are registered and routed via backend configuration APIs without requiring client mobile app updates. Portability across Linux amd64 and arm64 architectures is an explicit design and validation target (cross-platform reliability is not assumed proven); hardware acceleration (e.g. VAAPI, NVENC) is capability-probed and optional, with CPU software x264 encoding serving as the initial baseline and fallback (with x265 deferred for future evaluation) when required GStreamer plugins are available (runtime package availability and performance must be validated per target; not all distributions include these plugins). Capacity is treated as an empirical measurement for whichever node model is deployed; switching host vendors or chip models requires recalibrating capacity (budget units), not rewriting product code.
6. **Vanguard Dedicated Player Playback (D6):** Spectator playback is powered exclusively by the in-house **Vanguard streaming engine** (native GPU pipeline: Media3/ExoPlayer on Android, AVPlayer + CADisplayLink on iOS via `connectsapp_app/lib/app/shared/media/playback/vanguard/`), not stock Flutter `video_player`.
7. **Fixed Resolution & Frame Rate Baseline:** Broadcaster cameras, GPU processing, and hardware encoders are locked to **true 720×1280 portrait at 30 fps** (Slice E4 contract invariant). Client-side simulcast is strictly forbidden.
8. **On-Demand Recording Only (D7):** Livestreams are written to persistent storage **only when the creator explicitly initiates recording**. Unrecorded live streams leave zero persistent footprint beyond the in-memory rolling buffer.
9. **Authoritative Double-Entry Financial Integrity:** TigerBeetle is the sole authority for financial balances (Coins, Diamonds, Revenue) using same-ledger linked transfers. Redis stores only ephemeral game state (battle score bars, combo streaks, timers).
10. **Real-Time Event Engine (D4):** Centrifugo is the unified real-time WebSocket event bus for livestream chat, gifts, and battle scores (<400ms end-to-end; optimistic sender echo). Soketi is removed from the live streaming stack (retained solely for legacy 1:1 direct messages).
11. **Legal & Compliance Gate (D8):** High-risk regional routing, sanctions compliance, App Store gambling guidelines (Guideline 5.3 / 3.1.1), and hosting ToS are governed by an explicit compliance gate. All "bypass DPI" wording is removed.

---

## 2. End-to-End Livestreaming System Architecture

The following diagram illustrates the decoupled architecture separating the Financial/Control Plane from the Media Plane, powered by the **Unified Single-Binary Media Engine**:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CONTROL & FINANCIAL PLANE (Dedicated Virtual Instances / Managed Clusters)                      │
│ ├── Authoritative Ledger: TigerBeetle (6-Replica Production VSR Quorum / 3-Replica Staging)     │
│ ├── Financial Gateway Sidecar (Go / Node.js gRPC Microservice bridging Laravel API)             │
│ ├── Relational Database: PostgreSQL (Primary + Read Replica + Continuous WAL Archiving)          │
│ ├── Real-Time Event Bus: Centrifugo Cluster (WebSockets / HTTP/2, Redis Engine)                 │
│ ├── Ephemeral Gameplay State: Redis Cluster (PK battle scores, gloves, tapping counters)        │
│ └── Application API: Laravel Core API (User profiles, stream metadata, auth tokens)            │
└─────────────────────────────────┬───────────────────────────────────┬───────────────────────────┘
                                  │ Token Issuance & Edge Directory   │ Event Publishing
                                  ▼                                   ▼
┌─────────────────────────────────────────────────┐ ┌─────────────────────────────────────────────┐
│ MEDIA PLANE — NODE 1: FRANKFURT (Master Origin) │ │ MEDIA PLANE — NODE 2: SINGAPORE (East Edge) │
│ (Launch Candidate: Hetzner AX102 / 10G NIC)     │ │ (Launch Candidate: Hetzner Cloud CCX)       │
│                                                 │ │                                             │
│ [ Broadcasters (EU / MidEast / US) ]            │ │ [ Broadcasters (SEA / ANZ) ]                │
│   │ Vanguard Ingress (720x1280 @ 30fps, 1.8M)   │ │   │ Vanguard Ingress (720x1280 @ 30fps)     │
│   ▼                                             │ │   ▼                                         │
│ [ LiveKit WebRTC SFU (Frankfurt) ]              │ │ [ LiveKit WebRTC SFU (Singapore) ]          │
│   ├── Host, Co-Hosts, PK Battle Stage           │ │   ├── Host, Co-Hosts, PK Battle Stage       │
│   ▼ (In-Memory RTP WebRTC Subscription)         │ │   ▼ (In-Memory RTP WebRTC Subscription)     │
│ [ CONNECTS-EGRESS (Single Go Binary) ]          │ │ [ CONNECTS-EGRESS (Single Go Binary) ]       │
│   ├── Ingress: livekit/server-sdk-go            │ │   ├── Ingress: livekit/server-sdk-go        │
│   ├── Compositor: Solo, 1v1 PK, Tryon, Quadro   │ │   ├── Compositor: Solo, 1v1 PK, Tryon, Quad │
│   ├── Encoder: hi/mid/lo ladder (GOP = 1.0s)    │ │   ├── Encoder: hi/mid/lo ladder             │
│   ├── In-Memory Part Store (memfd, 24 s window) │ │   ├── In-Memory Part Store (memfd, 24 s)    │
│   └── Embedded HTTP/2 Server (_HLS_msn blocking)│ │   └── Embedded HTTP/2 Server                │
│   ▼                                             │ │   ▼                                         │
│ [ NGINX Origin Shield (Port 443 SNI Mux) ]      │ │ [ NGINX Edge Cache (Port 443 SNI Mux) ]     │
│   │ proxy_cache_lock on                         │ │   │ Pulls from Frankfurt on Cache-Miss      │
│   ▼                                             │ │   ▼                                         │
│ [ Spectator Audience: EU / US / Central Asia ]  │ │ [ Spectator Audience: SEA / ANZ / BD ]      │
│   (Vanguard Player: LL-HLS 2–4s Latency)        │ │   (Vanguard Player: LL-HLS 2–4s Latency)    │
└─────────────────────────────────────────────────┘ └─────────────────────────────────────────────┘
```

---

## 3. Two-Tier Delivery, Edge Routing & Unified Media Engine

### 3.1 Two-Tier Audience Model & Latency Grounding

* **Tier 1: Interactive Stage (LiveKit WebRTC):**
  * **Participants:** Broadcaster, co-hosts, PK opponents, and approved stage guests (<10 publishers).
  * **Protocol:** WebRTC over UDP (DTLS/SRTP) with fallback to TCP/TLS 443 TURN.
  * **Verified Latency Reality:** Glass-to-glass latency is **180 ms – 350 ms** across typical mobile cellular (4G/5G) connections and cross-region hops (e.g. Middle East or US to Frankfurt). Sub-200 ms is achieved on same-region high-speed Wi-Fi.
  * **Bandwidth Usage:** ~1.8 Mbps upstream per publisher.
* **Tier 2: Mass Spectators (LL-HLS / HLS):**
  * **Participants:** 100 to 20,000+ viewers per dedicated origin node (scaled to larger audiences via CDN fallback).
  * **Protocol:** Apple Low-Latency HLS (LL-HLS) with 333ms partial segments (`EXT-X-PART`) over HTTP/2, falling back to 2s standard segments.
  * **Target Latency (Product Owner Decision D5):** **2.0 to 4.0 seconds** perceived video latency at launch; sub-2.0s is a secondary stretch goal.
  * **Interactive Illusion:** Centrifugo WebSocket events (chat, hearts, battle scores) deliver updates in **<400 ms** (Product Owner Decision D4), with instant optimistic rendering on the sending client.

### 3.2 Vendor-Neutral Edge/Region Routing Contract (Product Owner Decision D3)

To ensure ConnectsApp can add regional edges, local data centres, or third-party CDNs without releasing new mobile client updates, all playback URL resolution is server-driven:

```
[ Mobile Spectator Client ]
  │
  ├──► 1. GET /api/v1/livestream/{stream_id}/playback-route (Client IP, Geo headers)
  │
[ Server-Side Edge Directory Service ]
  │
  ├──► Evaluates Client Region, Edge Health, Active Connections, Server Capacity
  │
  └──◄ Returns Playback Routing Packet:
         {
           "stream_id": "live_user_123",
           "protocol": "ll-hls",
           "primary_edge": {
             "endpoint_url": "https://hls-sg.connectsapp.com/live/live_user_123/master.m3u8",
             "region": "ap-southeast-1",
             "provider": "self_hosted_singapore"
           },
           "fallback_edges": [
             {
               "endpoint_url": "https://hls-fra.connectsapp.com/live/live_user_123/master.m3u8",
               "region": "eu-central-1",
               "provider": "self_hosted_frankfurt"
             },
             {
               "endpoint_url": "https://stream.connectsapp.com/cdn/live_user_123/master.m3u8",
               "region": "global_cdn",
               "provider": "commercial_cdn"
             }
           ],
           "token": "jwt_signed_playback_token",
           "part_duration_ms": 333,
           "drain_status": "active"
         }
```

#### Invariants of the Edge/Region Routing Contract
1. **Ordered Failover List:** The client player attempts playback on `primary_edge`. If 3 consecutive segment/playlist fetches fail (HTTP 5xx, timeout >2.5s), the Vanguard controller steps to the first entry in `fallback_edges` without tearing down UI state.
2. **Edge Health & Capacity Reporting:** Edge nodes run a lightweight telemetry daemon reporting CPU load, active connections, and 10G port egress bandwidth to Redis every 5 seconds. If an edge reaches **85% capacity**, the directory service marks it `draining` and assigns subsequent viewers to the secondary edge or commercial CDN.
3. **Origin Shield & On-Demand Pull:** The Singapore edge node does not ingest broadcasts from creators directly unless the creator is in Southeast Asia. For Frankfurt broadcasts watched by Asian viewers, Singapore acts as an **Origin Shield Edge Cache**: it pulls manifests and segments on-demand from the Frankfurt origin using NGINX `proxy_cache` with `proxy_cache_lock on;`, collapsing thousands of local requests into a single trans-continental transit stream.
4. **LL-HLS Query-String Handling Invariant:** Any future edge, reverse proxy, or CDN **must strictly preserve query strings** in cache keys (`$uri$is_args$args`). Apple LL-HLS mandates query parameters `_HLS_msn` and `_HLS_part`. Dropping them destroys LL-HLS, causing players to fall back to standard 6-second latency.
5. **Zero-App-Update Regional Drainage:** When decommissioning or maintenance is required on an edge, changing its status to `draining` in the backend database seamlessly diverts new incoming viewers within 30 seconds.

### 3.3 Future Regional Infrastructure (Product Owner Decision D2)

The following locations and services are **explicitly removed from the launch topology** and deferred to future expansion milestones once traffic warrants dedicated regional infrastructure:
* **Pakistan Regional Edge:** Bunny.net Karachi PoP ($0.03/GB).
* **Central Asia Regional Edges:** Gcore Tashkent (Uzbekistan) & Almaty (Kazakhstan) PoPs (€0.01/GB).
* **Oceania Regional Edge:** Sydney, Australia dedicated server or local edge.
* **North America Regional Hub:** Ashburn / Hillsboro dedicated servers.
* **In-Country Data Centres:** Sovereign on-premise infrastructure in target markets.

### 3.4 Updated Cloudflare Policy & Object Storage Terms

> [!WARNING]
> **Cloudflare Video Terms & Storage Economics (Verified Source):**
> 1. **Updated CDN Service Terms (May 2023 Update):** Cloudflare retired the historic "Section 2.8" blanket restriction. Under current terms (Section 2.2 and Service-Specific Terms), video delivery is explicitly permitted **only when using Cloudflare's designated media products (Cloudflare Stream, Cloudflare Images, Cloudflare R2)**. Routing external, unapproved high-volume video streams through standard Free/Pro/Business CDN caching violates service terms.
> 2. **Cloudflare R2 Economics Trap:** While R2 offers zero egress fees, Class B read operations on live 333ms–500ms chunks cost **$0.36 per million operations**. At 10,000 viewers fetching 2 chunks per second, uncached live playback incurs over 72 million operations per hour (**~$26/hour or ~$780/month** for a single active room).
> 3. **Architectural Guardrail:** Active rolling live chunks are held **strictly in the in-memory Part Store (memfd / RAM)** on origin/edge media nodes (such as the initial budget-conscious Hetzner launch candidate instances, not a permanent constraint). Cloudflare R2 is used **strictly for completed on-demand VOD recordings, permanent highlight clips, and static UI assets**.

### 3.5 Broadcaster Ingress & The Unified `connects-egress` Engine

* **Single Master Ingress Invariant:** Broadcaster mobile clients upload strictly **ONE master video stream** (`720×1280` portrait @ 30 fps, 1.8 Mbps H.264 / AAC-LC: 1,600 kbps video + 128 kbps audio). WebRTC client-side simulcast is forbidden.
* **The Unified Packager Architecture (`connects-egress`, plan v2.3.0):**
  Instead of daisy-chaining containers over RTMP, a per-source `worker` ingests host tracks from LiveKit (ingest path chosen by Gate G-I), composes the Atlas Canvas, and encodes **one** ABR ladder with keyframes force-aligned across rungs every 1.000 s (scene-cut disabled). Parts are written to the supervisor-owned Part Store and served as LL-HLS (part 0.333 s, segment 2.0 s) via per-room timelines with demuxed per-room audio.

Solo ladder shown below. Battle modes use the same tiers on their own geometry (1v1: 720×624 / 576×496 / 360×312; Tryon & Quadro: 720×656 / 576×528 / 360×328). **Invariant:** `hi`/`mid` dimensions are mod16, `lo` at least mod8 (plan §8.5).

| Tier | Resolution (Solo) | Frame Rate | Video Bitrate (`TARGET`, set by Gate G-C) | Audio Bitrate | Target Network / Silicon |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`hi` (H.264)** | 720 × 1280 | 30 fps | `NOT_VERIFIED` (candidate `TARGET` ~1,600 kbps) | 128 kbps | High-speed Wi-Fi, 5G, modern devices |
| **`mid` (H.264)** | 576 × 1024 | 30 fps | `NOT_VERIFIED` (candidate `TARGET` ~800–1,000 kbps) | 64 kbps | Standard 4G/LTE mobile networks |
| **`lo` Data-Saver (H.264)** | 360 × 640 | 30 fps | `NOT_VERIFIED` (candidate `TARGET` ~400–500 kbps) | 48 kbps | 3G, prepaid cellular |

H.264 is the near-term primary/shipping ladder, and `x264` software encoding is the initial baseline across targets, subject to Gate G-C calibration (quality, candidate bitrates, preset tuning, and CPU capacity) and target-environment validation. The obsolete concept of a lazy H.264 web fallback is removed; all mobile and web clients consume the single 3-tier H.264 ladder. HEVC is deferred as an optional future evaluation path, which would require passing Gate G-C (efficiency verification on real atlas content under live low-latency constraints), deferred Gate G-D (device hardware decode and 30-minute thermal qualification on low-end Android and minimum iOS devices), and Legal Gate D8 (review of patent pool and licensing terms). Prior HEVC planning targets (~950 / 450 / 220 kbps) and directional test data remain non-decisive historical references. Portability across Linux amd64 and arm64 is an architectural design and validation target: software encoding (`x264enc`, with `x265enc` deferred as an optional future backend) serves as the intended baseline and fallback when required GStreamer plugins are available (runtime package availability and performance must be validated per target distribution and architecture, without assuming all distributions include these plugins or that cross-platform reliability is already proven), avoiding discrete GPU dependency.

*(Authoritative spec: [`CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md`](CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md) v2.3.0).*

---

## 4. VIP Ranking, Prestige & Stage Admission Boundaries

### 4.1 VIP Prestige Hierarchy

ConnectsApp separates visual prestige from stage broadcast permissions:
* **Level 40+ Emperor:** Custom neon screen entrance banner, gold username in chat, VIP chat flair, priority queue position when requesting stage access.
* **Top 3 Room Contributors:** Avatars pinned on the stream HUD in gold, silver, and bronze slots; special badge in chat feed.
* **Monthly VIP Subscribers:** Dedicated badge, subscriber-only chat modes, exclusive emotes.
* **Regular Viewers:** Standard chat, tapping hearts, gifting from standard catalog.

### 4.2 Gated Stage Admission Engine (4-Gate Verification)

High spending never grants automatic camera or microphone broadcast rights. Admitting a user to the WebRTC stage requires passing all four gates:
1. **Gate 1: Viewer Request:** The viewer explicitly taps "Request to Join Stage".
2. **Gate 2: Safety & Eligibility Check:** Backend automated check validates user age (18+), account standing (no active strikes), and device readiness.
3. **Gate 3: Broadcaster Approval:** The host receives an in-app prompt and manually accepts the request.
4. **Gate 4: Viewer Mutual Consent:** The viewer confirms camera/mic permissions before publishing their track to LiveKit.

### 4.3 Seamless Stage Promotion (Hot-Swap)

When an approved spectator transitions from spectator to stage guest:
1. Flutter client receives `room:promoted_to_stage` over Centrifugo WebSocket.
2. App requests a scoped LiveKit participant token with `canPublish: true`.
3. WebRTC client connects and acquires local camera/microphone.
4. Vanguard spectator player fades audio over 150ms and unbinds its native surface texture.
5. LiveKit `RTCVideoRenderer` mounts smoothly without reloading the room or dropping chat connection.

---

## 5. Video Pipeline, Thermal Management & Bitrate Optimization

### 5.1 Native Camera & Pipeline Invariant: 720p @ 30 fps End-to-End

* **Hardware Baseline:** Mobile camera sensors in ConnectsApp are physically negotiated and locked to **true 720×1280 portrait at 30 fps** (Slice E4 contract invariant).
* **Strict 1:1 Cadence Across the Entire Stack:**
  1. **Camera Sensor Capture:** 720×1280 @ 30 fps (rotation=0).
  2. **Vanguard GPU Engine:** Renders beauty retouching, green screen, and overlays at exactly 30 fps on native GPU textures.
  3. **Hardware Encoder:** VideoToolbox (iOS) or MediaCodec (Android) encodes frames directly from the GPU surface. Target bitrate: **1.8 Mbps** (1,600 kbps video + 128 kbps AAC audio).
  4. **LiveKit SFU:** Relays 30 fps RTP packet streams without server-side transcoding.

### 5.2 Thermal State Machine & App-Level Thermal Guard

Device thermal metrics are evaluated using platform-native APIs mapped to a unified 4-tier state machine:
* **iOS Integration:** Monitored via `ProcessInfo.processInfo.thermalState` (values: `.nominal`, `.fair`, `.serious`, `.critical`).
* **Android Integration:** Monitored via `BatteryManager.EXTRA_TEMPERATURE` (reported in tenths of a degree Celsius) and Android 10+ `PowerManager.getThermalHeadroom`.

| Tier | iOS State | Android Battery Temp | Operational Action |
| :--- | :--- | :--- | :--- |
| **Tier 0 (Normal)** | `thermalState == .nominal` | Temp < 39.0°C | Full 720p @ 30 fps (1.8 Mbps), full PAG gift animations enabled. |
| **Tier 1 (Moderate)** | `thermalState == .fair` | 39.0°C ≤ Temp < 43.0°C | Maintain 720p @ 30 fps (1.4 Mbps), throttle particle animations by 50%, max 1 concurrent gift animation. |
| **Tier 2 (Severe)** | `thermalState == .serious` | 43.0°C ≤ Temp < 46.0°C | Downscale capture to 540p @ 30 fps (800 kbps), disable full-screen PAG animations, static badges only. |
| **Tier 3 (Critical)** | `thermalState == .critical`| Temp ≥ 46.0°C | Drop to audio-only or standby card, suspend camera hardware to prevent OS thermal shutdown. |

### 5.3 Client-Side Whale Gift Animations: PAG / libpag Guardrails

* **Rendering Engine:** Tencent libpag provides high-performance vector/B-frame animations exported from Adobe After Effects.
* **Client-Side Overlay Only:** Gift animations are rendered locally on the client viewport as transparent overlays; they are never composited into the broadcaster's video encoder stream.
* **Performance Guardrails:** Max 40 MB texture memory budget, max 1 full-screen animation active concurrently, and emergency server-side remote kill-switch feature flag.

---

## 6. Financial Ledger & Realtime Engagement Engine

### 6.1 System Responsibility Map: TigerBeetle vs. Redis vs. PostgreSQL

| Responsibility Dimension | TigerBeetle (Authoritative Financial Core) | Redis Cluster (Realtime Ephemeral Gameplay) | PostgreSQL (Relational Data Store) |
| :--- | :--- | :--- | :--- |
| **Primary Role** | Financial balances, double-entry ledgers, transaction immutability. | Ephemeral game state, battle scores, combo streaks, timers. | User profiles, gift catalogs, stream metadata, historical reporting. |
| **Data Handled** | User Coin balances, Creator Diamond liabilities, Platform revenue. | PK match scores, MVP rankings, 30s glove multipliers, tap counters. | Purchase receipts, transaction audit logs, moderation records. |
| **Overdraft Protection** | Enforced via `flags.debits_must_not_exceed_credits` (100% ACID). | None (Ephemeral in-memory cache; resets after match ends). | Relational integrity and constraints. |
| **Throughput Target** | Thousands of financial transfers per second; deterministic execution. | Sub-millisecond latency for WebSocket pushes. | Batched asynchronous writes. |

### 6.2 Authoritative TigerBeetle Linked-Transfer Exchange Flow

> [!IMPORTANT]
> **TigerBeetle Invariant: Same-Ledger Transfers Only:**
> TigerBeetle strictly enforces that debit and credit accounts in a transfer must belong to the **exact same ledger** (`accounts_must_have_the_same_ledger`).
> Virtual gifting is an economic exchange of user Coins for creator Diamonds and platform revenue executed as a **three-leg linked transfer** across two distinct ledgers:
> * **Ledger 1 (`LEDGER_COINS = 1`):** Denominated in Virtual Coins.
> * **Ledger 2 (`LEDGER_DIAMONDS = 2`):** Denominated in Creator Diamonds (Cash-Out Liability).

```
[ Viewer Sends 1,000 Coin Gift ($10.00 Value) ]
  │
  ├──► 1. Client UI performs optimistic bounce.
  │
  ├──► 2. Backend Financial Sidecar submits 3-Leg Linked Transfer to TigerBeetle:
  │
  │      ┌── Leg 1 (Ledger 1 - Coins):
  │      │   Debit:  Viewer Coin Account (1,000 Coins)
  │      │   Credit: Platform Coin Clearing Account (1,000 Coins)
  │      │   Flag:   `flags.linked = true`
  │      │
  │      ├── Leg 2 (Ledger 1 - Coins):
  │      │   Debit:  Platform Coin Clearing Account (500 Coins)
  │      │   Credit: Platform Coin Revenue Account (500 Coins)
  │      │   Flag:   `flags.linked = true`
  │      │
  │      └── Leg 3 (Ledger 2 - Diamonds):
  │          Debit:  Platform Diamond Treasury / Issuance Account (500 Diamonds)
  │          Credit: Creator Diamond Account (500 Diamonds)
  │          Flag:   `flags.linked = false` (Terminal Transfer)
  │
  │      * All 3 transfers are evaluated atomically in a single batch.
  │      * If viewer has <1,000 coins, ALL 3 LEGS FAIL ATOMICALLY with zero state change.
  │      * Transfer IDs are 128-bit UUIDv7 deterministic hashes guaranteeing idempotency.
  │
  ├──► 3A. On TigerBeetle Success:
  │         • Redis increments battle score bar and MVP points.
  │         • Centrifugo broadcasts `room:gift_event` to all spectators (<400ms).
  │         • Async worker writes receipt to PostgreSQL for creator analytics.
  │
  └──► 3B. On Insufficient Funds / Failure:
            • TigerBeetle rejects with `exceeds_debits` or specific error code.
            • Client rolls back optimistic UI; displays coin top-up sheet.
```

---

## 7. Real-Time Chat & The 4 Future-Proof Media Modes

### 7.1 Real-Time Chat & Event Engine (Product Owner Decision D4)

* **Engine:** Centrifugo (high-performance real-time messaging server in Go, using WebSockets, HTTP/2, and Protobuf/JSON).
* **Latency Budget:** Up to **~400 ms end-to-end** is acceptable for spectator audience chat, provided the sending user experiences an **instant optimistic local echo**.
* **Optimistic Local Echo & Rejection Reconciliation:**
  1. Viewer taps "Send". The chat message immediately renders in the local chat HUD with an `in_flight` status indicator.
  2. Message is dispatched via Centrifugo client RPC.
  3. If accepted by server-side content filter, Centrifugo broadcasts to the room channel; sender reconciles status to `confirmed`.
  4. If rejected, server returns an error code; client fades out the unconfirmed bubble and displays a subtle error toast.
* **Chat Channel Isolation Invariant:** Audiences in Room A subscribe strictly to Centrifugo channel `room:host_a`. Audiences in Room B subscribe strictly to `room:host_b`. Spectators never see the opposing room's chat feed, preventing harassment, spam, and cultural clashes.

### 7.2 The 4 Future-Proof Media Modes (Tight Atlas + Player Perspective)

**Decision D-A:** each audience sees its own host first (TikTok behaviour), while the server encodes **one** video per match. The `worker` encodes a **tight Atlas**: only the on-screen video box, with every host at the largest size any audience needs and no unused pixels (except Tryon's one black cell). The Vanguard player crops and positions tiles to build its own room's perspective (1 hardware decoder). Header, score bar, names and gifts remain Flutter overlays.

**Screen layout (measured from TikTok references, responsive):** header rows at the top; the score bar sits **directly on top of** the full-width video box; below it the contributor row, a **flexible** chat that absorbs the remaining height, and the input bar. Solo is full-bleed video with all UI overlaid. Full spec: plan v2.3.0 §4.6. (The v2.0 "720×640 at y=100, score bar 740–800, chat 800–1280" bands are withdrawn.)

| Mode | Server atlas | Video box on screen | What the audience of room X sees |
| :--- | :--- | :--- | :--- |
| **1 Solo** | 720×1280 | Full screen (cover) | Host full-bleed, UI overlaid |
| **2 1v1** | 720×624 (2× 360×624) | W × 0.867W | Own host left, opponent right |
| **3 Tryon** | 720×656 (3× 360×328 + 1 black cell) | W × 0.911W (2×2) | Own host top-left; opponents right column; empty cell bottom-left (Flutter panel); three-way score bar |
| **4 Quadro 2v2** | 720×656 (4× 360×328) | W × 0.911W | Own team left column (own host top-left), opposing team right column |

* **Mode changes change resolution:** each switch happens at a GOP boundary with `EXT-X-DISCONTINUITY` + a new `EXT-X-MAP`. Variant URIs are quality tiers (`hi`/`mid`/`lo`), not resolutions, so the master playlist stays valid (plan §5.2, §8.5; Gate G-L).

* **Per-room audio (one-sided mute):** the worker mixes one AAC track per room: `mix(room_X) = host_X + other hosts not muted by host_X`. Each room's playlist pairs the **shared** video renditions with its own audio rendition. Edge caches store the video once per match.
* **Stable playback URL:** every room keeps `/live/{room_id}/master.m3u8` through solo → battle → solo, worker restarts and cross-region battles. The supervisor's Room Playback Timeline splices sources with `EXT-X-DISCONTINUITY` and continuous sequence numbers.
* **Layout timing:** mode/atlas changes happen at an IDR segment boundary and are signalled in-stream via `EXT-X-DATERANGE` (`CLASS="com.connectsapp.atlas"`), so the player re-lays-out exactly when the new frames appear.
* **Cross-server battles (Decision D-B):** hosts stay on their home LiveKit SFU. The `bridge` relays each remote host's media (RTP passthrough, no transcode) into the match's anchor SFU and back, hub-and-spoke. Remote-region spectators keep their regional edge; their supervisor mirrors the anchor's parts once per region.
* **Battle state:** 1v1 two-colour bar; Tryon three-colour bar; Quadro `team_red = A + C`, `team_blue = B + D` in Redis. Diamonds are always credited to the individual host in TigerBeetle. Gift/score/timer events carry `server_ts` and render when the playhead's `PROGRAM-DATE-TIME` reaches them (gifter's own echo stays instant).

#### 7.2.1 Phased Implementation Roadmap
* **Phase 0:** plan v2.3.0 §16 gates (ingest, H.264 codec/CPU calibration, deferred HEVC device decode re-entry criteria, ABR alignment, LL-HLS conformance, crash continuity, bridge latency, player atlas layout, event sync).
* **Phase 1:** Solo + 1v1, including same-region multi-node and cross-region bridging.
* **Phase 2:** Tryon + Quadro — atlas geometries, perspective tables, bridge fan-out and team logic only; no new media mechanism.

---

## 8. Safety, Compliance, & Moderation Architecture

* **Age Verification (COPPA / GDPR-K):** Broadcasters must be verified 18+. Spectators under 18 have gifting features locked.
* **In-Memory Rolling Moderation Buffer (Reconciled with D7):**
  * Origin nodes maintain a **rolling 60-second in-memory buffer (`/dev/shm/mod_buffer/`)** of video frames.
  * If a user or moderator submits an abuse report, the active 60-second buffer is exported and saved to secure cold storage for compliance review.
  * If no report is filed, frames are overwritten and permanently lost upon window expiration.
* **Automated AI Moderation Sampling:** Egress compositor extracts 1 frame every 3 seconds, pushing to an asynchronous visual safety worker for nudity, violence, and hate-speech detection.

---

## 9. On-Demand Recording Lifecycle & Origin Shield

### 9.1 Zero-Copy In-Memory RAM-Disk Origin

1. **Direct In-Memory Slicing:** `connects-egress` writes CMAF parts directly into the supervisor's in-memory Part Store (memfd), keeping a 24-second playlist window (12 segments, plan §7.5).
2. **NGINX Origin Shield:** NGINX fronts the RAM disk with `proxy_cache_lock on;`, collapsing thousands of simultaneous spectator requests into single memory reads.

### 9.2 On-Demand Recording Lifecycle (Product Owner Decision D7)

> [!IMPORTANT]
> **TikTok LIVE Replay Reality vs. ConnectsApp Policy:**
> * **How TikTok Actually Works (Verified Source):** TikTok automatically generates a full server-side "LIVE Replay" for every completed broadcast by default (stored in TikTok Studio for 30 to 90 days for creator review/download/clipping, with a settings toggle to disable). TikTok does **not** have an in-stream start/stop recording button.
> * **ConnectsApp Policy Decision D7:** ConnectsApp explicitly implements **On-Demand Recording Only**. Livestreams are written to persistent storage **only when the creator explicitly starts recording**. No recording = nothing persisted beyond the ephemeral rolling live window.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ ON-DEMAND RECORDING STATE MACHINE                                                      │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ [ Creator Live: Not Recording ]                                                        │
│   ├── Video exists ONLY in the in-memory Part Store (24 s rolling window).             │
│   │                                                                                    │
│   ├──► Creator taps "Start Recording" (Explicit Consent Logged)                        │
│   │      │                                                                             │
│   │      ▼                                                                             │
│ [ Recording Active: Segment 1 ]                                                        │
│   ├── Unified packager file sink writes contiguous segmented MP4 to /var/recordings/   │
│   ├── Red recording HUD indicator pulses on creator and spectator screens.             │
│   │                                                                                    │
│   ├──► Creator taps "Pause/Stop Recording"                                             │
│   │      │                                                                             │
│   │      ▼                                                                             │
│ [ Recording Paused / Segment Closed ]                                                  │
│   ├── Segment 1 finalized with valid Moov atom; queued for background upload.          │
│   │                                                                                    │
│   ├──► Creator taps "Resume Recording" ──► Starts Segment 2 (New isolated MP4)        │
│   │                                                                                    │
│   └──► Stream Ends While Recording (or Server Crash / Abrupt Disconnect):              │
│          ├── Crash-recovery hook executes fast remux to salvage unfinalized MP4 chunks.│
│          ├── Completed segments uploaded asynchronously to Cloudflare R2 / Storage Box.│
│          └── Segments linked to creator account; 30-day retention window begins.       │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. Security, TURN Port 443 Coexistence & DDoS Posture

### 10.1 LiveKit TURN TLS Coexistence with NGINX on Port 443 (Resolving P2)

All external spectator traffic and LiveKit TURN TLS share port `443` using NGINX Layer 4 SNI stream multiplexing (`ssl_preread on;`):

```nginx
# /etc/nginx/nginx.conf
stream {
    map $ssl_preread_server_name $backend_target {
        turn.connectsapp.com      livekit_turn_tls_backend;
        livekit.connectsapp.com   internal_nginx_https;
        hls.connectsapp.com       internal_nginx_https;
        api.connectsapp.com       internal_nginx_https;
        default                   internal_nginx_https;
    }

    upstream livekit_turn_tls_backend {
        server 127.0.0.1:5349; # LiveKit embedded TURN TLS listener
    }

    upstream internal_nginx_https {
        server 127.0.0.1:8443; # Internal loopback for standard NGINX HTTP/S
    }

    server {
        listen 443 reuseport;
        listen [::]:443 reuseport;
        proxy_pass $backend_target;
        ssl_preread on;
        proxy_connect_timeout 5s;
        proxy_timeout 3600s;
    }
}
```

---

## 11. Monitoring, High Availability & Disaster Recovery

### 11.1 Infrastructure Separation (Resolving P0-4)
* **Financial & Control Plane:** TigerBeetle cluster, PostgreSQL primary/replica, and Redis cluster are hosted independently of the high-throughput media transcoding servers.
* **No "Auto-Scaling" on Fixed Hardware:** Fixed dedicated servers have strict physical CPU ceilings. Instead of unrealistic auto-scaling claims on fixed bare metal, each server enforces a **hard admission cap expressed in measured encode budget units** (per-mode costs set by plan v2.3.0 Gate G-C; the earlier 20–25-room figure assumed x264 and one output per battle and is withdrawn) plus a spectator egress cap derived from the actual provisioned NIC. When a node reaches 85% of either budget, new sources and spectators are routed to another node, region or the Mode A CDN.
* **Empirical Capacity Calibration per Node Model:** Server capacity is an empirical property of the deployed node model, not a hardcoded software constant. Deploying on alternative hosting providers or hardware models requires recalibrating budget units via Gate G-C, not rewriting product or pipeline code.

---

## 12. Scalability, Capacity & Cost Model

### 12.1 Launch Candidate Infrastructure Baseline & Pricing Reality (Resolving P0-5)

To establish an economically viable baseline, Hetzner Dedicated Frankfurt (AX102) and Hetzner Cloud Singapore (CCX33) are evaluated as the **budget-conscious initial launch candidate topology** (an initial candidate option, not a permanent product constraint). The estimated spend under €300/month (~€227/month baseline) represents a planning assumption based on dated source prices, subject to current quote and traffic validation.

> [!NOTE]
> **Launch Assumptions vs. Portability Design Targets:**
> Hardware configurations, pricing terms, and quota calculations (including the < €300/month planning target and ~€227/month baseline) represent initial launch candidate assumptions based on dated source prices, subject to current quote and traffic validation during Phase 0 trials rather than an architectural guarantee or permanent product constraint. Portability across Linux (amd64 and arm64) is an explicit design and validation target, not an assumed proven state. GPU/hardware acceleration is optional and capability-probed, while software encoding (`x264enc`, with `x265enc` deferred as an optional future backend) is the intended baseline and fallback when required GStreamer plugins are available; runtime package availability and encoding performance must be validated per target distribution and architecture (not all distributions bundle these plugins, and cross-platform reliability is not assumed proven). Deploying on alternative hosting vendors or chip models requires recalibrating capacity (budget units), not rewriting product code.

* **Frankfurt Dedicated Launch Candidate (Hetzner AX102):**
  * Hardware: AMD Ryzen 9 7950X3D (16 Cores / 32 Threads), 128 GB DDR5 RAM, 2x 1.92 TB NVMe Datacenter Edition.
  * Base Price: ~€114 / month + 10G NIC Add-on: ~€33 / month = **~€147 / month**.
  * **10G Uplink Traffic Terms:** Includes **20 TB outbound traffic per month**. Overage is billed at **€1.00 per TB (excluding VAT)**.
* **Singapore Cloud Edge Launch Candidate (Hetzner Cloud):**
  * Dedicated root servers are NOT offered in Singapore (EU/US only).
  * In Singapore, Hetzner provides **Hetzner Cloud** (e.g. CCX33 dedicated vCPU instance: 8 vCPUs, 32 GB RAM at ~€80 / month).
* **Broadcast Room Capacity under `connects-egress` (`NOT_VERIFIED`):**
  * Eliminating the double encode (Egress $\to$ OME) and encoding one ladder per match (D-A) lowers CPU per room, but no per-room figure is ratified.
  * Room capacity per node is expressed in **encode budget units measured by Gate G-C** on the actual deployed node model (initially calibrated on the budget-conscious AX102 launch candidate as an initial baseline rather than a permanent product constraint; plan §15). The earlier "~1.1–1.4 threads per room" and "20 to 25 simultaneous live rooms" figures are withdrawn. Switching node models or hosting providers requires recalibrating budget units, not code modifications.
* **Bandwidth & Viewer Limits on a 10G Port:**
  * Practical safe saturation ceiling (80%): **8,000 Mbps (8 Gbps)**.
  * Average spectator bitrate across ladder: ~1,000 kbps (1 Mbps).
  * **Maximum Concurrent Spectators on Single 10G Port = 8,000 Concurrent Viewers.** Beyond 8,000 concurrent viewers on Frankfurt, a commercial CDN (Mode A) or additional edge servers become mandatory.

### 12.2 Rebuilt Monthly Cost Model (Two-Server Launch Candidate Topology)

*The cost model below reflects planning assumptions for the budget-conscious initial launch candidate topology (Hetzner Frankfurt AX102 + Singapore CCX33) based on dated source prices, subject to current quote and traffic validation to evaluate the < €300/month target (~€227/month baseline) before scale (an initial launch candidate, not a permanent product constraint):*

| Tier | Concurrent Viewers | Monthly Outbound Data | Frankfurt AX102 (10G Dedicated) | Singapore Cloud (CCX33 Edge) | Bandwidth Overage Cost | Total Monthly Infrastructure Cost | Commercial CDN Comparison (Cloudflare Stream) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Launch** | 500 – 1,500 | 6.75 TB – 20.25 TB | €147 / mo | €80 / mo | €0 (Within 20 TB quota) | **~€227 / mo** | $900 – $2,700 / mo |
| **Growth** | 2,000 – 5,000 | 27.0 TB – 67.5 TB | €147 / mo | €80 / mo | €7 – €47 / mo (€1/TB) | **~€234 – €274 / mo** | $3,600 – $9,000 / mo |
| **Edge Limit** | 5,000 – 8,000 | 67.5 TB – 108.0 TB | €147 / mo | €80 / mo | €47 – €88 / mo (€1/TB) | **~€274 – €315 / mo** | $9,000 – $14,400 / mo |
| **CDN Scale** | 10,000 – 50,000+ | 135 TB – 675 TB | €147 / mo | €80 / mo | Mode A CDN Engaged ($0.01/GB) | **~€1,577 – €6,977 / mo** | $18,000 – $90,000 / mo |

---

## 13. Legal, Sanctions & App Store Regulatory Compliance Gate (Product Owner Decision D8)

### 13.1 Sanctions, Hosting ToS & Jurisdiction Restrictions
* **International Sanctions (OFAC / EU Regulations):** Providing paid financial services, in-app coin transactions, or commercial hosting in sanctioned territories (e.g. Iran) is strictly regulated.
* **Hosting Terms Compliance:** Infrastructure providers (including Hetzner for the initial launch candidate) and cloud partners require strict adherence to EU/German export controls.
* **App Store Distribution:** Apple does not operate an App Store in Iran; Google Play billing services are unavailable.
* **Legal Policy Invariant:** ConnectsApp does not employ "censorship bypass" or "DPI evasion" techniques. Delivery relies exclusively on standard HTTPS (port 443), compliant TLS 1.3 encryption, and server-driven IP failover.

### 13.2 App Store Review Guidelines: Loot Boxes & Gambling (Guideline 5.3 / 3.1.1)
* **Treasure Boxes:** Must be **100% free to open for all viewers in the room** (no consideration) to prevent classification as illegal gambling under Guideline 5.3.
* **Odds Disclosure (Guideline 3.1.1):** The probability distribution of winning coin quantities must be displayed in an on-screen rules sheet.
* **Booster Gloves (30% Critical Hit Chance):** Replaced with a **deterministic, predictable multiplier** (flat 1.5x boost for 30s) in jurisdictions with strict loot-box regulations.

---

## 14. Vanguard Spectator Playback & Pre-Warming Specification

### 14.1 Native Vanguard Streaming Engine Ownership (Product Owner Decision D6)
Spectator video playback is owned exclusively by the Vanguard native streaming engine:
* **Android Native:** `HttpAdaptivePlaybackAdapter.kt` and `AndroidDagStreamingPlaybackCoordinator.kt` driving Flutter's `TextureRegistry` / `SurfaceProducer`.
* **iOS Native:** `VGStreamingPlaybackCoordinator.swift` driving `AVPlayer`, `AVPlayerItemVideoOutput`, and a `CADisplayLink` frame pump.
* **Application Controller:** `AppVanguardVideoPlayerController` and `AppVanguardVideoPlayerView` (`connectsapp_app/lib/app/shared/media/playback/vanguard/`).

### 14.2 Active Source Code Capabilities vs. Deferred Engineering

| Subsystem / Feature | Active Workspace Implementation | Verification Status | Architectural Requirement |
| :--- | :--- | :---: | :--- |
| **Android Media3 Adaptive Adapter** | `HttpAdaptivePlaybackAdapter.kt:76-120` | `VERIFIED_CODE` | Drives HLS/LL-HLS playback on private `HandlerThread`. |
| **Android Live Offset Policy** | `AdaptiveStreamLiveOffsetPolicy.kt:34-67` | `VERIFIED_CODE` | Evaluates target (3s), min (1.5s), max (6s) live offset speed adjustments (0.97x–1.03x). |
| **Android Cache Prewarm Engine** | `AndroidDagPlaybackPrewarmEngine.kt:40-100` | `VERIFIED_CODE` | Single-thread background `CacheWriter` execution. |
| **iOS AVPlayer Frame Pump** | `VGStreamingPlaybackCoordinator.swift:23-120` | `VERIFIED_CODE` | `CADisplayLink` driving `FlutterTextureRegistry` pixel buffer updates. |
| **BehindLiveWindowException Recovery** | None in `HttpAdaptivePlaybackAdapter.kt` | **`NOT_PROVEN / TO_BUILD`** | Must catch Media3 eviction and seek cleanly to live edge. |
| **WSOLA Pitch Preservation in Playback** | Exists in Duet recorder; absent from streaming player | **`NOT_PROVEN / TO_BUILD`** | Live speed adjustments must preserve pitch without distortion. |
| **Hardware Decoder Ceiling (Max 2 Decoders)** | Contract requirement; absent from playback wrapper | **`NOT_PROVEN / TO_BUILD`** | Must enforce strict 2-decoder ceiling during feed swiping. |
| **Stream N+1 Paused 1s Live Buffer** | Contract requirement; absent from playback wrapper | **`NOT_PROVEN / TO_BUILD`** | Must pre-buffer 1 second of live frames without consuming 2nd audio session. |

---

## 15. Execution Roadmap: Proof Gates & Spikes

| Proof Gate / Spike | Subsystem Under Test | Success Criteria & Verification Seam | Status |
| :--- | :--- | :--- | :---: |
| **Spike-1** | **`connects-egress` Phase 0 gates (Solo + 1v1 + Bridge)** | Plan v2.3.0 §16 gates G-I, G-C, G-D (deferred), G-A, G-L, G-R, G-B, G-P, G-S: ingest choice, H.264 codec/CPU budget units, deferred HEVC device decode criteria, aligned IDRs, LL-HLS conformance with demuxed audio and discontinuities, crash continuity ≤ 3 s, cross-region bridge ≤ 350 ms host↔host, player atlas layout on 1 decoder, HUD↔video skew ≤ 200 ms. | **`NOT_PROVEN`** |
| **Spike-2** | **Multi-Host Expansion (Tryon & Quadro)** | Add Tryon/Quadro atlas geometries, player perspective tables, hub-and-spoke bridge fan-out for 3–4 hosts and team scoring; no new media mechanism. | **`NOT_PROVEN`** |
| **Spike-3** | **TigerBeetle Linked-Transfer Exchange Spike** | Go sidecar executing 3-leg linked transfers across Coin and Diamond ledgers under 5,000 simulated concurrent gift events with zero overdraft. | **`NOT_PROVEN`** |
| **Gate 1** | **Vanguard LL-HLS Playback Proof** | `AppVanguardVideoPlayerView` playing live LL-HLS stream on physical Android (`SM-A566B`) and iOS device with 2–4s live latency. | **`NOT_PROVEN`** |
| **Gate 2** | **Edge Directory Route Resolution Proof** | Client fetches `/playback-route`; smoothly falls back from primary to secondary edge upon simulated HTTP 500 failure. | **`NOT_PROVEN`** |
| **Gate 3** | **Centrifugo Chat & Optimistic Echo Proof** | 1,000 concurrent room participants chatting; sender sees instant local bubble; server rejection triggers rollback. | **`NOT_PROVEN`** |
| **Gate 4** | **On-Demand Recording & 60s Buffer Proof** | Creator starts/stops recording; MP4 finalized and uploaded; unrecorded live stream leaves zero persistent disk traces. | **`NOT_PROVEN`** |
| **Gate 5** | **Vanguard Decoder GC & Swipe Proof** | Rapid swiping across 10 live streams; zero `MediaCodec NO_MEMORY` crashes; hardware decoder count never exceeds 2. | **`NOT_PROVEN`** |
| **Gate 6** | **TURN Port 443 Coexistence Proof** | WebRTC stream successfully traverses NGINX SNI stream multiplexer on port 443 with UDP blocked on client. | **`NOT_PROVEN`** |
| **Gate 7** | **Legal & App Store Compliance Gate** | Formal legal review of sanctions, ToS, and App Store Guideline 5.3/3.1.1 odds disclosure documentation. | **`NOT_PROVEN`** |

---

## 16. Sources & Verification Log (Mandatory Primary Sources)

| Item / Claim | Primary Source URL | Access Date | Direct Source Quote / Verified Metric | Status Classification |
| :--- | :--- | :---: | :--- | :---: |
| **LiveKit Egress Open Source** | [github.com/livekit/egress](https://github.com/livekit/egress) | 2026-10-06 | "LiveKit Egress is 100% open source under Apache 2.0. Written in Go with go-gst bindings." | `VERIFIED_SOURCE` |
| **LiveKit Go Server SDK** | [github.com/livekit/server-sdk-go](https://github.com/livekit/server-sdk-go) | 2026-10-06 | "Official Go SDK for LiveKit. Allows building custom server-side participants, room bots, and media pipes." | `VERIFIED_SOURCE` |
| **OvenMediaEngine Architecture** | [github.com/AirenSoft/OvenMediaEngine](https://github.com/AirenSoft/OvenMediaEngine) | 2026-10-06 | "Written in C++17. Relies on FFmpeg (libavcodec, libswscale) for core transcoding and packaging." | `VERIFIED_SOURCE` |
| **TigerBeetle Same-Ledger Rule** | [tigerbeetle.com/docs/reference/operations/create_transfers](https://tigerbeetle.com/docs/reference/operations/create_transfers) | 2026-10-06 | `accounts_must_have_the_same_ledger`: "The debit and credit accounts must belong to the same ledger." Direct transfers between different ledgers are prohibited. | `VERIFIED_SOURCE` |
| **Hetzner 10G Uplink Terms (Launch Candidate)** | [hetzner.com/dedicated-rootserver/matrix-ax/](https://www.hetzner.com/dedicated-rootserver/matrix-ax/) | 2026-10-06 | Dedicated 10G port includes 20 TB monthly outbound traffic. Overage is billed at €1.00 per TB (excl. VAT). Evaluated for initial launch candidate based on dated source pricing (2026-10-06), subject to current quote and traffic validation. | `VERIFIED_SOURCE` |
| **Hetzner Singapore Availability (Launch Candidate)** | [hetzner.com/cloud/](https://www.hetzner.com/cloud/) | 2026-10-06 | Dedicated root servers are NOT offered in Singapore (EU/US only). Singapore presence is Hetzner Cloud only (CCX/CPX) with tiered traffic. Evaluated for initial launch candidate based on dated source pricing (2026-10-06), subject to current quote validation. | `VERIFIED_SOURCE` |
| **Cloudflare CDN Video Terms** | [cloudflare.com/terms/](https://www.cloudflare.com/terms/) | 2026-10-06 | Section 2.8 retired May 2023. Video caching on standard CDN restricted unless using Cloudflare Stream or R2. R2 Class B reads: $0.36/M. | `VERIFIED_SOURCE` |
| **TikTok LIVE Replay Behavior** | [support.tiktok.com](https://support.tiktok.com/en/live-gifts-wallet/tiktok-live/live-recordings) | 2026-10-06 | TikTok automatically generates LIVE Replays stored in TikTok Studio for 30 days. No in-stream start/stop recording button exists. | `VERIFIED_SOURCE` |
| **NGINX TURN Port 443 SNI Mux** | [nginx.org/en/docs/stream/ngx_stream_ssl_preread_module.html](https://nginx.org/en/docs/stream/ngx_stream_ssl_preread_module.html) | 2026-10-06 | `ssl_preread on;` extracts SNI hostname from Client Hello, allowing raw Layer 4 routing to LiveKit TURN (5349) alongside HTTPS. | `VERIFIED_SOURCE` |
| **iOS Thermal State API** | [developer.apple.com/documentation/foundation/processinfo/thermalstate](https://developer.apple.com/documentation/foundation/processinfo/thermalstate) | 2026-10-06 | iOS exposes 4 states: `nominal`, `fair`, `serious`, `critical`. Does not expose temperature in °C. | `VERIFIED_SOURCE` |
| **Apple App Store Guideline 5.3 & 3.1.1** | [developer.apple.com/app-store/review/guidelines/](https://developer.apple.com/app-store/review/guidelines/) | 2026-10-06 | Guideline 5.3 strictly regulates gambling/lotteries. Guideline 3.1.1 requires odds disclosure prior to purchase for randomized virtual items. | `VERIFIED_SOURCE` |
| **Vanguard Media3 Playback Engine** | `HttpAdaptivePlaybackAdapter.kt:76-120` | 2026-10-06 | Android Media3 adapter running on dedicated `HandlerThread` driving surface producer. | `VERIFIED_CODE` |
| **Vanguard Live Offset Speed Policy** | `AdaptiveStreamLiveOffsetPolicy.kt:34-67` | 2026-10-06 | Deterministic speed adjustment metadata policy (0.97x–1.03x) for live offset recovery. | `VERIFIED_CODE` |
| **Vanguard iOS DisplayLink Coordinator** | `VGStreamingPlaybackCoordinator.swift:23-120` | 2026-10-06 | AVPlayer + CADisplayLink frame pump driving texture registry updates on iOS. | `VERIFIED_CODE` |
| **BehindLiveWindowException Handling** | Workspace Codebase Search | 2026-10-06 | Present only as test case description; zero catch/recovery implementation in code. | `NOT_PROVEN / TO_BUILD` |
| **WSOLA Pitch Preservation in Player** | Workspace Codebase Search | 2026-10-06 | Exists in Duet recorder; zero implementation in adaptive streaming playback engine. | `NOT_PROVEN / TO_BUILD` |
| **Mobile 2-Decoder Ceiling Enforcement** | Workspace Codebase Search | 2026-10-06 | Contractual requirement; zero pool/ceiling enforcement code in playback wrapper. | `NOT_PROVEN / TO_BUILD` |
