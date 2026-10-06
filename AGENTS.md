# Connects Egress Workspace Instructions (AGENTS.md)

> Local workflow and safety rulebook for `connects-egress`.

---

## 1. Scope & Repository Boundaries
- **This repository:** `/Users/foxy/connects-egress` on branch `main`.
- **Hard Isolation:** Agents working in this repository are strictly **FORBIDDEN** from modifying, staging, or committing any files in `/Users/foxy/connects_app`, including `connectsapp_app/` and `packages/`.
- **Contracts as Single Source of Truth:** API and playlist contracts live under `contracts/`. ConnectsApp mobile clients consume versioned contracts; breaking changes require major version bumps.

---

## 2. Architectural Invariants
1. **Three Roles, One Binary (`connects-egress <role>`):**
   - `supervisor`: Pure Go, **NO Cgo**. Owns `memfd` Part Store, Room Playback Timelines, LL-HLS HTTP origin, watchdog, and systemd units.
   - `worker`: Go + Cgo GStreamer (1 per source/match). Ingests WebRTC tracks, composes tight Atlas, encodes 1 video ladder, mixes per-room AAC.
   - `bridge`: Pure Go (LiveKit Go SDK). Relays host media across LiveKit nodes/regions. **RTP passthrough only**—no transcoding.
2. **Strict Mod16 Geometries:**
   - Solo: `720 × 1280`
   - 1v1: `720 × 624`
   - Tryon & Quadro: `720 × 656`
   - Intermediate ladder rungs must strictly adhere to mod16 / mod8 constraints.
3. **Phase-Gated Spike Rule:**
   - Spikes live in `spikes/<gate>/` and are throwaway.
   - Production packages in `internal/` must not import spike code.
   - Production code for a subsystem is frozen only after the corresponding Phase 0 gate passes.

---

## 3. Command & Git Safety
- Never run `git checkout`, `git restore`, `git reset`, `git clean`, destructive rebases, or any equivalent command that discards, overwrites, or reconstructs files from Git history.
- Do not use Git to remove an unwanted diff. Manually edit the exact file until the diff is empty, then re-check with `git diff`.
- Read-only Git inspection is allowed when relevant: `git status`, `git diff`, `git log`, `git show`, and file-specific history commands.
- Commits require explicit user approval and an exact staged file list shown before committing.
- Preserve all unrelated modifications and untracked files.
- External-agent prompts for this repository must include a "Hard forbidden commands" block naming the destructive Git commands above.
