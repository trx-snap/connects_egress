# Vanguard Unified Live Packager — 5-Year Future-Proofing Spec

**Status:** SUPERSEDED on 2026-10-06 by [CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md](../architecture/CONNECTS_EGRESS_IMPLEMENTATION_PLAN.md) (now v2.3.0).

All future-proofing content now lives in that one plan, so two documents can't drift apart. Where each pillar went:

| v1 Pillar | Now in Plan v2.0.0 | Change |
| :--- | :--- | :--- |
| 1. Process isolation | §3, §9 | Ring buffer moved out of the worker into a supervisor-owned shared-memory Part Store. Added a restart epoch with `EXT-X-DISCONTINUITY`, hang watchdog, circuit breaker, cgroups, admission control and supervisor re-adoption. "300 ms restart" withdrawn (target ≤ 3 s, Gate G-R). |
| 2. Cgo / concurrency | §7.1 | "Frames never cross Go↔C" withdrawn (impossible with Go SDK ingest). Ingest is now Gate G-I: Go SDK + `appsrc` pushing access units, vs `livekitwebrtcsrc`. Unsourced scheduler-stall numbers withdrawn. |
| 3. Codec evolution | §8 | HEVC kept as **gated** target (G-C, G-D). CDD "hardware mandate", "100% royalty-free" and fixed 41% claims withdrawn. H.264 web rung made lazy. |
| 4. Hardware acceleration | §8.3 | Backends reconciled (NVENC / VAAPI / software); per-backend tuning; mid-stream fallback. "80+ rooms" and "zero changes" withdrawn. |
| 5. Jitter / sync | §7.2 | Correct jitter-buffer mechanism; compositor `latency` deadline; RTCP SR lip sync; arrival-aligned cross-host composition. |
| 6. Zero-GC memory | §14 | "20–50 ms GC pauses" withdrawn; `sync.Pool` described accurately; Part Store removes per-part heap churn. |
| 7. MoQ | §14 | Marked speculative; MoQ uses QUIC streams; no latency claims. |

New in v2.0.0 (not in this spec): Atlas Canvas with player-side perspective (Decision D-A), per-room audio renditions, Room Playback Timeline, Stage Bridge (Decision D-B), ABR keyframe alignment, match terminal-state table, gift/score ↔ video sync, Phase 0 gates, risk register.
