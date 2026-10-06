# connects-egress — Documentation Index

This folder is the **canonical home** for the ConnectsApp livestream media contracts.
Copies in agent "brain" folders or chat history are non-authoritative snapshots.

## Precedence (highest first)

When two documents disagree, the higher one wins. Fix the lower one; never code against it.

| # | Document | Version | Status | Scope |
| :- | :--- | :--- | :--- | :--- |
| 1 | [architecture/CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md](architecture/CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md) | 2.3.0 | **Authoritative** for the media engine | Engine roles, tight atlas, ladders, LL-HLS, bridge, reliability, Phase 0 gates |
| 2 | [architecture/CONNECTSAPP_TIKTOK_STYLE_LIVESTREAM_MASTER_ARCHITECTURE.md](architecture/CONNECTSAPP_TIKTOK_STYLE_LIVESTREAM_MASTER_ARCHITECTURE.md) | 5.1.0 | **Authoritative** for the system | Topology, routing contract, ledger, chat, compliance, cost; defers media detail to #1 |
| 3 | [test-plans/VANGUARD_HLS_LLHLS_LAB_AND_EDGE_CASE_TEST_PLAN.md](test-plans/VANGUARD_HLS_LLHLS_LAB_AND_EDGE_CASE_TEST_PLAN.md) | — | Active (player side) | Vanguard player HLS/LL-HLS edge-case battery |
| 4 | [gifting/TIKTOK_COMPETITIVE_GIFTING_ENGINE_MASTER_SPEC.md](gifting/TIKTOK_COMPETITIVE_GIFTING_ENGINE_MASTER_SPEC.md) | — | **Needs reconciliation** | Mentions Soketi; master v5.1 decision D4 makes Centrifugo the live event bus |
| — | [`../AGENTS.md`](../AGENTS.md) | — | Repo rules | Isolation from `connects_app`, invariants, git safety |

## Frozen invariants (from #1 and #2)

- Single publisher: 720×1280 portrait @ 30 fps, no client simulcast.
- Atlases: Solo 720×1280 · 1v1 720×624 · Tryon & Quadro 720×656 (Tryon = 3 hosts + 1 black cell).
- Ladder: `hi`/`mid` mod16, `lo` ≥ mod8 — Solo 720×1280/576×1024/360×640 · 1v1 720×624/576×496/360×312 · Tryon/Quadro 720×656/576×528/360×328.
- LL-HLS: part 0.333 s, segment 2.0 s, GOP 1.000 s force-aligned, window 24 s.
- All performance, CPU, bitrate and latency numbers are `NOT_VERIFIED` until the Phase 0 gates pass.

## Archive (superseded, kept for history; do not implement from these)

| Document | Superseded by |
| :--- | :--- |
| [archive/VANGUARD_MEDIA_ENGINE_5_YEAR_FUTURE_PROOF_SPEC.md](archive/VANGUARD_MEDIA_ENGINE_5_YEAR_FUTURE_PROOF_SPEC.md) | #1 |
| [archive/gifting_engine_architecture.md](archive/gifting_engine_architecture.md) | #2 §6 and #4 (Soketi/Redis-ledger design) |
| [archive/ARCHITECTURE_AUDIT_v1.md](archive/ARCHITECTURE_AUDIT_v1.md) | Audit of master v1.0.0; findings folded into v5.x |
| [archive/CONNECTSAPP_TIKTOK_STYLE_LIVESTREAM_ARCHITECTURE.pdf](archive/CONNECTSAPP_TIKTOK_STYLE_LIVESTREAM_ARCHITECTURE.pdf) | PDF render of an older master version |

## Change rule

Any change to an invariant above bumps the document version, adds a line to the plan's
"Previous Text Withdrawn" section, and updates this index in the same change.
