# Server-owned Pivot Column Order Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make wide pivot ordering correct and efficient across viewport, clipboard, and export without sending a full permutation on each scroll.

**Architecture:** The backend resolves stable pivot identities into one display order and slices that order. V1 requests carry a layout revision, a manifest only when changed or missing on a worker, and separately bounded pinned identities. All responses and caches bind to the revision; the frontend rejects stale responses.

**Tech Stack:** Python/FastAPI/Dash runtime, TypeScript/React, JSON Schema, pytest, Selenium.

**Spec:** `docs/superpowers/specs/2026-10-23-server-owned-pivot-order.md`

## Global Constraints

- Keep runtime `schemaVersion` at 1 and the public Dash/React/Streamlit APIs compatible.
- Saved pivot identities derive from pivot path and measure key, never labels or viewport indices.
- A missing saved identity retains its rank without consuming a visible slot.
- Whole pivot groups move across the center; leaves move only within their group; totals remain last.
- Redis remains optional; any worker can recover through a manifest retry.
- Editing stays excluded from public release qualification.

## Review Focus

- Same display index after a filter changes the pivot values: reject stale revision data.
- Two workers with separate local caches: a token-only request gets a retry, then succeeds with the manifest.
- A pinned column outside the scroll window: fetch it once, alongside the scrolling slice.
- Hidden and unauthorized IDs in saved state: do not leak values or schema; preserve only safe client preference.
- Export while reordering: use the resolver snapshot, not a later layout revision.

---

### Task 1: Pure identity and reconciliation resolver

**Files:** Create `pivot_engine/runtime/column_layout.py`; test in `tests/test_runtime_column_layout.py`.

**Interfaces:** `resolve_column_layout(default_columns, saved_ids, *, allowed_ids=None)` returns ordered columns and a deterministic revision. Columns carry structured `pivot_path`, `measure_key`, and existing `cell_id`.

- [ ] Write failing tests for colliding labels, duplicates, missing/returning identities, new columns, group and leaf movement, totals, unauthorized IDs, and bounds.
- [ ] Run focused pytest and confirm the expected failures.
- [ ] Implement the pure resolver and run focused pytest.
- [ ] Commit the resolver and tests.

### Task 2: Versioned V1 layout handshake and cache ownership

**Files:** Modify request/response JSON Schemas, generated types, `runtime/models.py`, `runtime/router.py`, `adapter_viewport_cache.py`; add protocol tests.

**Interfaces:** Data/export state accepts `layoutRevision`, optional `layoutManifest`, and bounded `pinnedColumnIds`; response includes `layoutRevision` or `layout_required`. A validated worker cache uses principal/dataset/query/revision as its key.

- [ ] Write failing protocol and two-worker cache-miss tests.
- [ ] Implement validation, revision binding, retry, and cache invalidation.
- [ ] Run protocol/security tests and commit.

### Task 3: Server window, pinning, clipboard and export

**Files:** Modify `tanstack_adapter.py`, `runtime/router.py`, export/clipboard commands and their tests.

**Interfaces:** One resolver supplies display-ordered column IDs; viewport selects the display slice plus pinned IDs; copy/export apply visibility/scope options to the same order.

- [ ] Write failing tests for reordered sparse windows, pinned off-window columns, grouped headers, copied/exported order, totals, and authorization.
- [ ] Implement the single resolver path, run focused tests, and commit.

### Task 4: Frontend layout lifecycle

**Files:** Modify typed runtime state, column-order utilities, viewport transport and response acceptance; add type/unit tests.

**Interfaces:** Send manifest on change/miss, token on ordinary scroll, pinned IDs separately; discard responses with a different layout revision and retry `layout_required` once with the manifest.

- [ ] Write failing request/response and stale-revision tests.
- [ ] Implement frontend state and retry; run typecheck and focused tests.
- [ ] Commit.

### Task 5: Release-bound qualification

**Files:** Extend the wide fixture and browser journeys; update the parity contract and evidence report.

- [ ] Write browser assertions for group/leaf reorder, pinned off-window values, refresh, copy/export, and three-host request/revision traces.
- [ ] Run unit, typecheck, full Python suite, clean package builds, and three-host plus AG Grid browser qualification.
- [ ] Bind the final commit and artifact hashes; commit source and report the remaining release limits.
