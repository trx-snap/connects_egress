# Connects Egress — LL-HLS Sub-2s Web Player

High-performance, ultra low-latency web player designed specifically to consume **Connects Egress** unified CMAF LL-HLS streams and maintain steady glass-to-glass/live-edge playback strictly **under 2.0 seconds**.

---

## 1. Quick Start

### Option A: Native HTTP/3 (QUIC over UDP) — Recommended
```bash
cd web-player
./start-h3.sh
```
Launches the HTTP/3 Nginx gateway on `https://localhost:3053` with native QUIC, TLS 1.3 0-RTT, and automatic CORS proxying to `connects-egress`!

### Option B: Node.js Dev Server (HTTP/1.1 & HTTP/2)
```bash
cd web-player
npm start
# Or directly:
./start.sh
```
Opens in your browser at `http://localhost:3050`.

### Option C: Go Server (Zero Dependencies)
```bash
cd web-player
go run server.go -port 3050 -upstream http://127.0.0.1:18080
```

---

## 2. Architectural Overview

```
 ┌────────────────────────┐      HTTP / GET (LL-HLS)       ┌────────────────────────────────┐
 │   connects-egress      │ ◄────────────────────────────► │     web-player (Proxy/Server)  │
 │   Supervisor / Origin  │                                │  Node.js (3000) or Go Server   │
 │   (:18080 or :8080)    │                                └──────────────┬─────────────────┘
 └────────────────────────┘                                               │ CORS-enabled
                                                                          │ Chunked Stream
                                                                          ▼
                                                           ┌────────────────────────────────┐
                                                           │   Modern Web Browser           │
                                                           │   Hls.js Low-Latency Engine    │
                                                           │   - Part streaming (333ms)     │
                                                           │   - Latency: ~1.1s – 1.4s      │
                                                           │   - Catch-up rate clock        │
                                                           └────────────────────────────────┘
```

### Why a Built-in Smart Proxy?
Standard browser security enforces the Cross-Origin Resource Sharing (CORS) specification. Because `connects-egress` supervisor origin serves native HLS without origin pinning, requests from `http://localhost:3000` directly to `http://127.0.0.1:18080` would be blocked by CORS in desktop and mobile browsers.

The built-in `server.js` and `server.go`:
1. Transparently proxies `/live/*` and `/media/*` with streaming chunked transfer (no buffering latency).
2. Attaches `Access-Control-Allow-Origin: *` and all necessary CORS headers.
3. Allows changing the target egress server dynamically from the UI without restarting.

---

## 3. How Sub-2-Second LL-HLS Playback is Achieved

Standard HLS operates on full segments (e.g. 2-6 seconds each) with a 3-segment target duration holdback, yielding **6 to 18 seconds of delay**.

This web player leverages Apple's RFC 8216bis Low-Latency HLS specification with `connects-egress` packaging parameters:

| Parameter | Value in Connects Egress | Role in Achieving < 2s Latency |
| :--- | :--- | :--- |
| **Video Part Target** | `334 ms` (10 frames @ 30fps) | Chunks video into ~1/3-second atomic units. |
| **Audio Part Target** | `320 ms` (15 AAC frames @ 48kHz) | Clean non-fractional audio packaging. |
| **GOP Keyframe Interval** | `1.000 s` (30 frames) | Fast IDR random-access synchronization. |
| **Server Hold-Back** | `PART-HOLD-BACK = 1.002 s` | Origin allows clients to request parts 1.0s behind live head. |
| **Preload Hints** | `EXT-X-PRELOAD-HINT: TYPE=PART` | Browser pre-requests the next part before it finishes encoding. |
| **Client Live Sync** | `liveSyncDuration: 1.25 s` | Anchors playhead at ~1.25s behind the encoder. |
| **Catch-Up Clock** | `1.12×` playback rate | When latency drifts toward 1.8s, the player accelerates to pull back to 1.2s. |

---

## 4. UI & Real-Time Telemetry Dashboard

The web player includes a high-tech dashboard displaying:
- **Live Latency Gauge**: Displays current latency with real-time color feedback:
  - 🟢 **Green**: `< 1.5 s` (Optimal LL-HLS performance)
  - 🟡 **Amber**: `1.5 s – 2.0 s` (Within sub-2s threshold)
  - 🔴 **Red**: `> 2.0 s` (Exceeding; automatic catch-up triggered)
- **Live Latency History Chart**: HTML5 canvas graph plotting the latency curve against a hard **2.0s red ceiling line**.
- **Catch-Up Clock**: Visualizes player rate adjustments (`1.00× Synced`, `1.12× Accelerating`, `0.95× Braking`).
- **Constrained Forward Buffer**: Real-time buffer measurement (maintained tightly between 0.6s and 1.5s).
- **ABR Ladder Switcher**: Mod16 rendition selector (`vhi` 720×1280 @ 2.5 Mbps, `vmid` 576×1024 @ 1.6 Mbps, `vlo` 360×640 @ 800 kbps, or `AUTO`).
- **Event Terminal**: Real-time logging of individual CMAF part loads (`N.M.m4s`), segment loads, and buffer updates.
- **One-Click Live Edge Jump**: Instant recovery to live edge if playback is paused or tab is backgrounded.

---

## 5. Verification & Testing

To test against the active egress instance:
1. Ensure the supervisor is running (e.g. `http://127.0.0.1:18080`).
2. Run `./start.sh` or `node server.js`.
3. Select preset `Local Container (room_android_smoke)` or type your custom room name.
4. Click **Connect & Play**.
5. Observe the latency card and sparkline holding steady at **~1.1s – 1.4s** (strictly below the 2.0s ceiling).
