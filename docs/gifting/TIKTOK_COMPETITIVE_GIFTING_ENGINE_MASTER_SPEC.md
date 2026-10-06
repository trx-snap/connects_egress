# ConnectsApp: TikTok-Competitive Gifting Engine
## Master Engineering Blueprint & Technical Architecture

**Classification:** Core System Architecture Specification  
**Licensing Requirement:** 100% Commercially Permissive Open-Source (MIT, Apache-2.0, BSD)  
**Security Standard:** Banking-Grade ACID Double-Entry Ledger, Zero-Overdraft, Anti-Money Laundering (AML)  
**Performance Target:** Sub-50ms Room Fan-out, 60fps Native Hardware-Accelerated Visuals, 500,000 Concurrent Viewers / Room  

---

## 1. System Philosophy & TikTok Feature Parity

To genuinely compete with TikTok, the gifting engine must be more than a tip jar; it is a **gamified real-time social economy**. The architecture must deliver complete parity across five core pillars:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        TIKTOK-PARITY GIFT TAXONOMY                     │
├───────────────────┬───────────────────┬────────────────────────────────┤
│ Category          │ Coin Range        │ Visual & Social Experience     │
├───────────────────┼───────────────────┼────────────────────────────────┤
│ 1. Micro-Gifts    │ 1 – 10 Coins      │ Rapid Combo Taps (x1 ... x99), │
│    (Rose, Heart)  │ ($0.01 – $0.10)   │ Floating screen bubbles, chat  │
├───────────────────┼───────────────────┼────────────────────────────────┤
│ 2. Mid-Tier       │ 50 – 500 Coins    │ Center-screen 2-3s animation,  │
│    (Fireworks)    │ ($0.50 – $5.00)   │ Sound FX, chat banner highlight│
├───────────────────┼───────────────────┼────────────────────────────────┤
│ 3. Mega-Gifts     │ 1,000 – 10,000    │ Full-screen 3D showstopper,    │
│    (Sports Car)   │ ($10 – $100)      │ Dynamic sender avatar inside,  │
│                   │                   │ Room-wide screen shake/sound   │
├───────────────────┼───────────────────┼────────────────────────────────┤
│ 4. Universe-Tier  │ 20,000 – 40,000+  │ App-Wide Global Banner Push    │
│    (Lion, Falcon) │ ($200 – $500+)    │ (drags viewers from other rooms)│
├───────────────────┼───────────────────┼────────────────────────────────┤
│ 5. Vanguard AR    │ 500 – 5,000 Coins │ Tracks Creator's Face/Stage:   │
│    (Crown, Stage) │ ($5.00 – $50.00)  │ Crown on head, Green Screen swap│
└───────────────────┴───────────────────┴────────────────────────────────┘
```

---

## 2. Permissive Open-Source Bill of Materials (Commercial Friendly)

All selected components are strictly non-copyleft (MIT, Apache 2.0, BSD-3) to ensure 100% legal commercial ownership without licensing risks:

| Layer | Component | License | Role in Gifting Engine |
| :--- | :--- | :--- | :--- |
| **Financial Ledger** | [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle) / Redis Cluster | Apache-2.0 / BSD | Banking-grade double-entry ledger; 1M+ transfers/sec; zero balance drift. |
| **Real-Time Gateway**| [Soketi](https://github.com/soketi/soketi) (`uWebSockets.js`) | MIT | High-concurrency room broadcast & client duplex socket handling. |
| **Low-Latency Transport** | [LiveKit DataChannels](https://livekit.io) | Apache-2.0 | Peer-to-edge WebRTC data stream for sub-30ms in-room animation sync. |
| **Event Highway** | [NATS.io](https://nats.io) (JetStream) | Apache-2.0 | Ultra-fast message bus connecting API, Ledger, and Soketi clusters. |
| **App Graphics Engine** | [Tencent PAG](https://github.com/Tencent/libpag) / [SVGA](https://github.com/svga) | Apache-2.0 / BSD | Hardware-accelerated 60fps vector/3D effects with dynamic avatar injection. |
| **Camera & AR Pipeline**| **Vanguard Media Engine** | Proprietary | Native C++/Metal/Vulkan engine for Face Tracking AR gifts & Scene swaps. |
| **Backend Orchestrator**| **Laravel 10/11** + Go Worker | MIT / BSD | API Gateway, Stripe payment reconciliation, and user session auth. |

---

## 3. High-Level Master Architecture

```
                                  ┌───────────────────────────────┐
                                  │      Flutter Client App       │
                                  └───────┬───────────────┬───────┘
                                          │               │
       (Duplex / HTTP SendGift: <20ms)    │               │ (LiveKit WebRTC Media: 720p 30fps)
                                          ▼               ▼
                       ┌────────────────────────────┐ ┌───────────────────────┐
                       │     Soketi API Gateway     │ │     LiveKit Server    │
                       └──────────────┬─────────────┘ └───────────┬───────────┘
                                      │                           │
                                      ▼                           │
                       ┌────────────────────────────┐             │
                       │   Risk & Velocity Engine   │             │
                       └──────────────┬─────────────┘             │
                                      │                           │
                    ┌─────────────────┴─────────────────┐         │
                    ▼                                   ▼         │
    ┌──────────────────────────────┐    ┌──────────────────────────────┐
    │    Core Financial Ledger     │    │   NATS Real-Time Event Bus   │
    │  (TigerBeetle / Redis Lua)   │    │  (JetStream In-Memory PubSub)│
    │                              │    └───────────────┬──────────────┘
    │ • Atomic Idempotency Check   │                    │
    │ • Double-Entry Coin Debit    │                    ├────────────────────────┐
    │ • Creator Diamond Accumulate │                    ▼                        ▼
    └───────────────┬──────────────┘    ┌──────────────────────────────┐ ┌──────────────┐
                    │                   │    Soketi Broadcast Edge     │ │ LiveKit Data │
                    │ (Confirmed Tx)    │    (WebSockets to 500k Viewers) Channel Edge │
                    └──────────────────►└───────────────┬──────────────┘ └──────┬───────┘
                                                        │                       │
                                                        └───────────┬───────────┘
                                                                    │
                                                                    ▼
                                                    ┌───────────────────────────────┐
                                                    │  All Viewers in Stream Room   │
                                                    │                               │
                                                    │ • PAG/SVGA 60fps Animation    │
                                                    │ • Dynamic Sender Name/Avatar  │
                                                    │ • Combo Streak Counter        │
                                                    │ • Live Leaderboard Bump       │
                                                    └───────────────────────────────┘
```

---

## 4. The Banking-Standard Financial Ledger

To satisfy institutional financial standards, virtual coins and creator diamonds are treated like bank deposits:

### 4.1 Invariants (Strict Rules of the Ledger)
1. **No Overdrafts:** An account balance cannot transition below zero under any concurrency condition.
2. **Double-Entry Principle:** Every transaction consists of at least two balanced entries: `Sum(Debits) == Sum(Credits)`.
3. **Immutable History:** Balances are never modified by `UPDATE` queries. The current balance is mathematically derived from the journal history or an append-only WAL snapshot.
4. **Idempotency Guarantee:** Every gift action carries a unique UUID generated by the client. Retries (e.g. from network drops) are guaranteed zero duplicate debits.

### 4.2 The Double-Entry Transaction Model
When a viewer gifts a **100-Coin Gift** with a 50% platform revenue cut:

```
Transaction ID: tx_9f82c1_combo_14
Timestamp:      2026-09-28T15:30:00.124Z

Entries:
  1. DEBIT  Account:Viewer:Coins           Amount: 100  (Asset reduced)
  2. CREDIT Account:Platform:GrossRevenue  Amount:  50  (Liability to platform)
  3. CREDIT Account:Creator:PendingDiamonds Amount: 50  (Liability to creator)

Total Debits: 100 | Total Credits: 100 (Balanced: Δ = 0)
```

### 4.3 High-Throughput Engine Implementation (TigerBeetle / Redis Lua)
In high-frequency PK battles, relational databases (MySQL/PostgreSQL) crash due to row-lock contention on the creator's wallet. The ledger operates in two decoupled tiers:

1. **Tier 1: Sub-Millisecond Execution (In-Memory Atomic State Machine)**
   - Executed via TigerBeetle or a deterministic Redis Lua script.
   - Debits the viewer's balance immediately.
   - Increments an **in-memory room accumulator** for the creator's diamonds (`room:{id}:diamonds`).
   - Returns a cryptographically signed ACK to the sender in `< 10ms`.

2. **Tier 2: Asynchronous Settlement & Cold Persistence**
   - The transaction is pushed into an append-only event stream (`stream:ledger_transactions`).
   - Background Go/Laravel workers read the stream and persist the immutable journal records to PostgreSQL in batched bulk inserts (1,000 records per query).
   - Creator diamonds are flushed from the accumulator to their persistent earnings account every 30 seconds or upon stream termination.

---

## 5. Visual Pipeline: Vanguard Engine vs. Hardware App Overlay

To achieve TikTok's 4K visual crispness without degrading WebRTC video bitrate, visual responsibilities are cleanly bifurcated:

```
┌────────────────────────────────────────────────────────────────────────┐
│                         VISUAL PIPELINE SPLIT                          │
├───────────────────────────────────┬────────────────────────────────────┤
│ A. VANGUARD ENGINE (GPU / C++)    │ B. HARDWARE APP OVERLAY (Flutter)  │
│ (Baked into the Camera Texture)   │ (Rendered locally on Viewer Phone) │
├───────────────────────────────────┼────────────────────────────────────┤
│ • AR Face-Tracking Accessories    │ • Full-Screen Showstoppers         │
│   (3D Crown, Sunglasses, Masks)   │   (PAG/SVGA Dragon, Car, Rocket)   │
│ • Green Screen Stage Swaps        │ • Dynamic Sender Profile Badges    │
│   (Swap background for 10s)       │ • Interactive Combo Rings (x99)    │
│ • Facial Morphing & Beauty Shaders│ • Chat Banner Slides & Text Glow   │
│ • WebRTC Texture Tap (LiveKit)    │ • Spatial Sound FX (Stereo Audio)  │
└───────────────────────────────────┴────────────────────────────────────┘
```

### 5.1 The App Overlay Engine: Tencent PAG vs. SVGA
* **Why Tencent PAG (`libpag`):**
  - Used directly by WeChat, Tencent Live, and Douyin clones.
  - Hardware-accelerated on iOS (Metal) and Android (Vulkan / OpenGL).
  - Supports **Dynamic Image Replacement**: The animation template contains a placeholder for the sender's avatar and username. The phone renders the sender's avatar inside the driver's seat of a moving 3D sports car in real time.
  - File size: 50–80% smaller than Lottie or MP4 alpha channels with superior rendering efficiency.

### 5.2 The Vanguard Engine AR Gifts
When an AR Gift (e.g. "Royal Crown") is sent:
1. The socket sends a targeted command to the **Creator's Broadcaster Client**:  
   `{ type: "AR_GIFT", effect_id: "royal_crown_v3", duration_ms: 12000 }`
2. The Creator app commands Vanguard:  
   `VanguardEngine.activateAREffect("royal_crown_v3")`
3. Vanguard's GPU pipeline detects facial landmarks (eyes, forehead, chin) and renders the 3D crown directly onto the creator's video texture before encoding to LiveKit.
4. All remote viewers see the creator wearing the crown directly within the video feed.

---

## 6. Real-Time Distribution & Socket Architecture

### 6.1 Multi-Tier Fan-Out Strategy
When 500,000 viewers are in a single stream, sending individual WebSocket messages causes edge bandwidth saturation. The fan-out uses a multi-tier strategy:

1. **Individual Delivery (Micro-Gifts):** Pushed down the Soketi room channel (`presence-livestream.{roomId}`).
2. **Combo Aggregation Window (50ms Throttling):** If 5,000 viewers tap roses at once, the edge aggregator combines them into a single broadcast packet:
   ```json
   {
     "type": "BATCH_GIFT_TICK",
     "room_id": "stream_104",
     "totals": { "rose": 1420, "heart": 850 },
     "top_gifters": [{ "user_id": 99, "name": "Sarah", "points": 5000 }]
   }
   ```
3. **App-Wide Broadcast (Mega/Universe Gifts):**
   - When a user sends a 40,000-coin gift, a high-priority push goes to `global-announcements`.
   - Every active user in the app (even those watching other streams or browsing the home feed) sees a sliding gold banner at the top of their screen:  
     `"Alex sent a Golden Dragon in Sarah's Live! [TAP TO WATCH]"`  
   - This replicates TikTok's famous traffic-funneling mechanism.

---

## 7. Data Contracts & Wire Protocol

### 7.1 Client Send Gift (`client-send-gift`)
Sent by the viewer over the duplex WebSocket connection:
```json
{
  "event": "client-send-gift",
  "data": {
    "gift_id": 802,
    "room_id": "room_live_4491",
    "creator_id": 105,
    "combo_index": 7,
    "idempotency_key": "c7a8f102-4821-4f93-b6d2-e562148b8120",
    "timestamp_ms": 1727539200145
  }
}
```

### 7.2 Fast Acknowledgment (`gift-ack`)
Returned to sender in `< 15ms`:
```json
{
  "event": "gift-ack",
  "data": {
    "status": "APPROVED",
    "idempotency_key": "c7a8f102-4821-4f93-b6d2-e562148b8120",
    "gift_id": 802,
    "combo_index": 7,
    "coins_deducted": 10,
    "remaining_balance": 1450,
    "server_time_ms": 1727539200158
  }
}
```

### 7.3 Room Fan-Out Broadcast (`App\Events\GiftBroadcastEvent`)
Broadcast to all viewers in `presence-livestream.{roomId}`:
```json
{
  "event": "App\\Events\\GiftBroadcastEvent",
  "data": {
    "sender": {
      "id": 84,
      "name": "Jordan",
      "avatar_url": "https://cdn.connectsweb.com/avatars/u84.webp",
      "vip_level": 4
    },
    "gift": {
      "id": 802,
      "name": "Cyber Supercar",
      "tier": "MEGA",
      "pag_url": "https://cdn.connectsweb.com/gifts/pag/supercar_v2.pag",
      "sound_url": "https://cdn.connectsweb.com/gifts/audio/engine_rev.aac",
      "display_duration_ms": 4500
    },
    "combo": {
      "count": 7,
      "is_final": false
    },
    "stream_stats": {
      "total_diamonds": 184500,
      "top_contributor_id": 84
    }
  }
}
```

---

## 8. Anti-Fraud & Risk Architecture

Because virtual coins represent real financial value, the platform enforces banking-grade fraud prevention:

1. **Velocity Rate Limiting:** Max 30 gift actions per second per user token. Bursts exceeding this trigger automated temporary rate-limits to prevent bot attacks.
2. **Coin Age & Taint Tracking:** Coins bought via high-risk credit cards carry a 72-hour settlement taint. Tainted coins cannot be cashed out by creators immediately.
3. **Collusion & Laundering Detection:**
   - Detects when User A deposits $1,000, immediately gifts all $1,000 to Creator B in an empty private stream, and Creator B attempts instant withdrawal.
   - Flags accounts with high gift velocity originating from the same IP, device fingerprint, or subnet.
4. **Mandatory T+7 Escrow Window:** Creator Diamonds remain in `PENDING_CLEARANCE` for 7 days before entering `AVAILABLE_CASHOUT`. Any credit card chargebacks during this window automatically claw back the pending diamonds.

---

## 9. Phased Execution Roadmap

### Phase 1: Core Financial State Machine (Backend)
- Deploy TigerBeetle or the single-threaded Redis Lua ledger on the server.
- Set up the double-entry SQL journal schema (`ledger_journal_entries`).
- Implement the Stripe webhook listener for direct coin top-ups (zero IAP fees).

### Phase 2: High-Speed Duplex Socket Wiring
- Wire `client-send-gift` and `gift-ack` into the existing Soketi server.
- Establish the broadcast channel `presence-livestream.{roomId}`.
- Implement in-memory room diamond accumulators to eliminate MySQL row locks.

### Phase 3: Hardware-Accelerated Visual Overlay (Flutter App)
- Integrate `libpag` or `svgaplayer_flutter` into `connectsapp_app`.
- Build the `GiftAnimationOverlay` widget as a transparent stack layer above `LivestreamProofView`.
- Build the responsive combo ring with 3.0s auto-reset countdown.

### Phase 4: Vanguard AR Effects & Global Funnel
- Hook AR gift commands to Vanguard's native C++ Face Tracking and Green Screen shaders.
- Enable the app-wide top banner (`global-announcements`) for mega-gifts.
