# TikTok-Grade Gifting Engine: Product & Architecture Specification

**Project:** ConnectsApp Live Stream Gifting  
**Target Stack:** Flutter (Client) + Soketi (WebSockets) + Laravel (API) + Redis (In-Memory Atomic Engine) + LiveKit (Video)  
**Objective:** Deliver a high-concurrency, low-latency gifting experience with banking-grade virtual currency security, without excessive distributed systems engineering.

---

## 1. Executive Product Overview

### 1.1 The Economy Model (Direct Payment / Non-IAP)
Since coins are **not** purchased via Apple/Google In-App Purchases:
- **Payment Method:** Direct Web Billing (Stripe / Adyen / Local Gateway) or In-App Webview checkout.
- **Financial Benefit:** Zero 30% app-store fee deduction. 97%+ of gross coin purchase value is retained by the platform (minus ~2.9% payment processing fees).
- **Coin Purchasing Flow:**
  1. In the Flutter app, tapping "+ Top Up" opens a secure native Stripe Sheet or a localized checkout session.
  2. Stripe webhook notifies Laravel backend: `charge.succeeded`.
  3. Laravel atomically credits the user's `wallet:user:{id}:coins` in Redis and records a double-entry journal entry.
- **Diamonds (Creator Earnings):** When gifted, creators receive **Diamonds** (e.g., 50% revenue share).
- **Cashout:** Creators withdraw to bank/PayPal after a fraud/chargeback holding window (T+7 days).

```
[Fiat Cash] ──► (Apple / Google / Stripe) ──► [Viewer Coins]
                                                    │
                                         (Send Gift in Live Stream)
                                                    ▼
                       ┌────────────────────────────┴───────────────────────────┐
                       ▼                                                        ▼
         [Platform Revenue Escrow] (50%)                       [Creator Diamonds] (50%)
                                                                                │
                                                                   (After T+7 Hold Period)
                                                                                ▼
                                                                     [Creator Cashout / Bank]
```

### 1.2 User Experience Features
1. **Interactive Gift Tray:** Bottom sheet with tabbed categories (Popular, Premium, Exclusive, Effects). Shows live coin balance with a "+ Top Up" button.
2. **Combo Streak Button:** When tapping a low-cost gift (1–10 coins), a 3.0-second combo countdown ring appears. Rapid tapping increments the streak counter (`x2`, `x3` ... `x99`).
3. **Multi-Tier Visual Presentation:**
   - *Tier 1 (Small gifts, 1–10 coins):* Floating micro-animation above the chat feed + combo badge.
   - *Tier 2 (Medium gifts, 50–500 coins):* Mid-screen animated banner with sound effect.
   - *Tier 3 (Mega gifts, 1000+ coins):* Full-screen hardware-accelerated SVG/PAG animation (e.g., animated sports car or dragon) with dynamic sender name and creator avatar embedded.
4. **Live Stream Leaderboard:** Top 3 gifters shown as avatar badges in the top bar beside the creator.

---

## 2. Pragmatic Architecture: Integrating with Your Existing Stack

Rather than introducing complex C++ or distributed consensus layers from scratch, this architecture leverages the tools already running in your repository:

| Functional Layer | Selected Open-Source Component | Why It Fits Without Heavy Engineering |
| :--- | :--- | :--- |
| **Mobile Client** | **Flutter** + [`svgaplayer_flutter`](https://pub.dev/packages/svgaplayer_flutter) | Already your app stack. SVGA handles complex gift animations with zero video frame drops. |
| **Real-time Edge** | **Soketi** (Pusher Protocol) | Already in production (`wsHost: b.connectsweb.com`). Handles 50k+ socket connections per node. |
| **Atomic Financial Ledger** | **Redis Lua Scripts** | Atomic single-thread execution in memory (<1ms). Prevents double-spend and database deadlocks. |
| **Persistent Ledger & Business Logic**| **Laravel 10/11** + PostgreSQL/MySQL | Existing backend. Double-entry accounting tables populated via async batch workers. |
| **Media Egress** | **LiveKit** | Already contracted in `VANGUARD_LIVEKIT_CREATOR_STACK_CONTRACT.md`. WebRTC media remains untouched. |

---

## 3. End-to-End System Workflow

```
[Flutter Client] 
   │
   │ 1. Duplex Socket Event: `client-send-gift`
   │    { gift_id: 101, room_id: "live_45", combo_seq: 14, idempotency_key: "c7b2-..." }
   ▼
[Soketi Gateway]
   │
   │ 2. Webhook / Fast IPC to Laravel Gifting Engine
   ▼
[Redis Atomic Layer (Single Lua Script)]
   │
   ├─► Step A: Check Idempotency Key (Return cached ACK if already processed)
   ├─► Step B: Check User Balance: if (user_coins < cost) return ERR_INSUFFICIENT_FUNDS
   ├─► Step C: DECRBY user_coins by cost
   ├─► Step D: INCRBY room_creator_diamonds by (cost * share_ratio)
   ├─► Step E: Push transaction record into Redis Stream: `stream:gift_transactions`
   ▼
[Soketi Real-Time Broadcast]
   │
   ├─► 3a. Duplex Fast ACK to Sender (Updates sender wallet UI & increments combo ring)
   │
   └─► 3b. Room Broadcast on Channel `presence-livestream.{roomId}`:
           Event: `App\Events\GiftBroadcastEvent`
           { sender_name, gift_id, combo_seq, anim_asset, diamond_count }
                 │
                 ▼
          [All Viewers & Streamer]
          (Flutter renders SVGA animation overlay)

   (Background Asynchronous Pipeline)
   ▼
[Laravel Queue Worker] (Every 1-2 seconds)
   │
   └─► Reads batch from `stream:gift_transactions`
   └─► Executes bulk INSERT into MySQL `ledger_journal_entries`
```

---

## 4. Financial Security & Invariant Rules

### Rule 1: Compile-Time & Atomic Balance Invariant
* Never deduct balances via raw SQL `UPDATE users SET balance = balance - X`.
* All balance validations and deductions happen inside an **atomic Redis Lua script**. Because Redis is single-threaded, race conditions and overdrafts are mathematically impossible.

#### Redis Lua Script (`deduct_gift.lua`):
```lua
-- KEYS[1]: user_coin_key (e.g. "wallet:user:101:coins")
-- KEYS[2]: room_creator_diamonds_key (e.g. "room:45:diamonds")
-- KEYS[3]: idempotency_key (e.g. "idemp:c7b2-45a1...")
-- ARGV[1]: gift_cost
-- ARGV[2]: creator_diamonds
-- ARGV[3]: idempotency_ttl (seconds, e.g. 86400)

-- 1. Idempotency Check
if redis.call('EXISTS', KEYS[3]) == 1 then
    return {1, redis.call('GET', KEYS[1])} -- Already processed, return success + current balance
end

-- 2. Balance Check
local current_balance = tonumber(redis.call('GET', KEYS[1]) or 0)
local cost = tonumber(ARGV[1])

if current_balance < cost then
    return {-1, current_balance} -- Insufficient funds
end

-- 3. Atomic Debit & Room Increment
local new_balance = redis.call('DECRBY', KEYS[1], cost)
redis.call('INCRBY', KEYS[2], tonumber(ARGV[2]))

-- 4. Mark Idempotency
redis.call('SETEX', KEYS[3], tonumber(ARGV[3]), "1")

return {0, new_balance}
```

### Rule 2: Immutable Double-Entry Ledger Schema
In MySQL/Postgres, money is never stored as an editable integer alone. Every transaction writes balanced debit and credit entries:

```sql
CREATE TABLE ledger_journal_entries (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL, -- Idempotency UUID
    user_id BIGINT NOT NULL,
    account_type ENUM('USER_COIN', 'CREATOR_DIAMOND', 'PLATFORM_REVENUE'),
    direction ENUM('DEBIT', 'CREDIT'),
    amount BIGINT NOT NULL,
    reference_type VARCHAR(32), -- 'GIFT_PURCHASE', 'PAYOUT', 'REFUND'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_account (user_id, account_type),
    UNIQUE KEY uk_tx_direction (transaction_id, account_type, direction)
);
```

### Rule 3: Eliminating Creator Hotspot Locks
During a PK battle with 10,000 gifts/minute, **never** lock the creator's row in MySQL. 
- Diamonds are accumulated in Redis memory (`room:{id}:diamonds`).
- When the live stream ends, or every 60 seconds, a batch worker sums the accumulated room diamonds and credits the creator's persistent wallet in **one single write**.

---

## 5. Client Integration Specification (Flutter)

### 5.1 Architecture Stack in View
In `LivestreamProofView` or the viewer screen:
```
[LiveKit Video Viewfinder (WebRTC)] (Bottom Layer)
  └── [Interactive UI Overlay] (Chat, Top Bar, Bottom Bar)
        └── [Gift Animation Layer] (SVGA / PAG Overlay Layer)
              └── [Combo Tap Interaction Button]
```

### 5.2 SVGA Animation Player Setup
Using [`svgaplayer_flutter`](https://pub.dev/packages/svgaplayer_flutter) ensures 60fps animations without causing WebRTC video jitter:

```dart
// Gift Animation Queue & Player Widget
class GiftAnimationOverlay extends StatefulWidget {
  final Stream<GiftBroadcastModel> giftEventStream;
  const GiftAnimationOverlay({super.key, required this.giftEventStream});

  @override
  State<GiftAnimationOverlay> createState() => _GiftAnimationOverlayState();
}

class _GiftAnimationOverlayState extends State<GiftAnimationOverlay> 
    with SingleTickerProviderStateMixin {
  SVGAAnimationController? _svgaController;
  final Queue<GiftBroadcastModel> _animationQueue = Queue();
  bool _isPlaying = false;

  @override
  void initState() {
    super.initState();
    _svgaController = SVGAAnimationController(vsync: this);
    widget.giftEventStream.listen(_onNewGiftReceived);
  }

  void _onNewGiftReceived(GiftBroadcastModel gift) {
    if (gift.tier == GiftTier.mega) {
      _animationQueue.add(gift);
      if (!_isPlaying) _playNextInQueue();
    }
  }

  Future<void> _playNextInQueue() async {
    if (_animationQueue.isEmpty) {
      _isPlaying = false;
      return;
    }
    _isPlaying = true;
    final gift = _animationQueue.removeFirst();
    final videoItem = await SVGAParser.shared.decodeFromURL(gift.svgaUrl);
    _svgaController?.videoItem = videoItem;
    _svgaController?.reset();
    await _svgaController?.forward();
    _playNextInQueue();
  }

  @override
  Widget build(BuildContext context) {
    return IgnorePointer(
      child: _svgaController?.videoItem != null
          ? SVGAImage(_svgaController!)
          : const SizedBox.shrink(),
    );
  }
}
```

### 5.3 Turnkey Asset Solution (No Dedicated Designers Required)

If your team does not have 3D motion designers, you do not need to create `.svga` files from scratch. You can immediately launch with three proven sources:

1. **Free Open-Source SVGA Live Gift Packs:**
   - The SVGA open-source community maintains public collections of pre-rendered live-streaming gifts (including Sports Car, Rocket, Rose, Heart, Crown, Treasure Box).
   - These are licensed for free commercial use and can be hosted directly on your CDN (`https://b.connectsweb.com/gifts/rose.svga`).
   - Repository: [SVGA-Samples](https://github.com/yyued/SVGA-Samples) and community collections on GitHub.

2. **LottieFiles (Royalty-Free / Free Commercial Tier):**
   - Thousands of ready-to-use, high-end vector gift animations exist on [LottieFiles](https://lottiefiles.com) (search: "Diamond", "Firework", "Gift Box", "Rocket").
   - Can be loaded using the standard `lottie` Flutter package for Tier 1 & Tier 2 gifts.

3. **Native Flutter Canvas Particle & Combo Streaks (Zero Asset Files):**
   - For combo streaks (`x1`, `x2`... `x99`), use Flutter's built-in `TweenAnimationBuilder` and `CustomPainter` to generate glowing particle bursts, scale-bounce counters, and animated gradient badges.
   - This provides the instant, snappy "TikTok feel" without downloading any external asset files.


---

## 6. Socket Data Contracts

### 6.1 Client → Server (`client-send-gift`)
Sent by the viewer over the existing duplex Soketi channel:
```json
{
  "event": "client-send-gift",
  "data": {
    "gift_id": 401,
    "room_id": "stream_984",
    "combo_seq": 5,
    "idempotency_key": "user9_g401_seq5_1727520000"
  }
}
```

### 6.2 Server → Sender ACK (`gift-ack`)
Returned to the sender in <15ms:
```json
{
  "event": "gift-ack",
  "data": {
    "status": "SUCCESS",
    "idempotency_key": "user9_g401_seq5_1727520000",
    "remaining_coins": 1820,
    "combo_seq": 5
  }
}
```

### 6.3 Server → Room Broadcast (`App\Events\GiftBroadcastEvent`)
Broadcast to `presence-livestream.{roomId}` via Soketi:
```json
{
  "event": "App\\Events\\GiftBroadcastEvent",
  "data": {
    "sender_id": 9,
    "sender_name": "Alex",
    "sender_avatar": "https://b.connectsweb.com/img/u9.jpg",
    "gift_id": 401,
    "gift_name": "Supercar",
    "combo_seq": 5,
    "svga_url": "https://b.connectsweb.com/gifts/supercar.svga",
    "tier": "mega",
    "stream_points": 25000
  }
}
```

---

## 7. Implementation Checklist

- [ ] **Phase 1: In-Memory Ledger (Laravel + Redis)**
  - Add Redis Lua script `deduct_gift.lua` into Laravel service.
  - Implement Redis Stream `stream:gift_transactions` for async batch logging.
- [ ] **Phase 2: Soketi Duplex Event Binding**
  - Bind `client-send-gift` on `initialization.dart` in Flutter.
  - Set up server-side event listener verifying token + executing the Lua script.
- [ ] **Phase 3: Client Animation Overlay**
  - Add `svgaplayer_flutter` to `connectsapp_app/pubspec.yaml`.
  - Add `GiftAnimationOverlay` to `livestream_proof_view.dart` and viewer screen.
- [ ] **Phase 4: Async Financial Settlement & Audit**
  - Create `ledger_journal_entries` migration.
  - Set up Laravel scheduled task (running every minute) to reconcile Redis room diamonds to creator earnings.
