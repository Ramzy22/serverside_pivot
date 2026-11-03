# Unified Target Architecture For Edit Propagation

Status: recommended target architecture

Inputs:

- existing design: `EDIT_SYSTEM_ARCHITECTURE.md`
- replacement design: `docs/edit-propagation-clean-architecture.md`

Goal:

- keep the current table fast
- keep the current patch-refresh and virtualized rendering model
- keep the good modular frontend ideas
- remove the semantic flaws around propagation, revert, overlap, and parent-only pseudo-edits

## 1. What To Keep From Each Design

### 1.1 Keep From `EDIT_SYSTEM_ARCHITECTURE.md`

These parts are strong and should stay:

1. Thin hot-path cell rendering with O(1) `Map.get` lookups.
2. `useEditEngine` extraction from the giant component.
3. Unified `CellEditStore` for render-time marker/original-value state.
4. Separate `OptimisticValues` tracker for in-flight requests.
5. Single `editSerializer` path for persistence.
6. `PropagationGate` as a UI state machine.
7. Patch refresh instead of full viewport refresh when possible.
8. Incremental migration plan that does not rewrite the whole table at once.

### 1.2 Keep From `edit-propagation-clean-architecture.md`

These parts fix the conceptual flaws:

1. Aggregate cells are intent targets, not persisted records.
2. Propagation planning is server-owned.
3. Conflict detection is server-owned.
4. Undo/redo are event-backed, not client-synthesized.
5. Ancestor/descendant overlap is forbidden by default.
6. Grand total editing is not a normal inline edit.
7. Parent-only override is not part of the real data-edit pipeline.
8. Patch computation should be driven by impacted scopes, not visible descendant guesses.

## 2. Comparison

### 2.1 Existing Architecture Strengths

- Excellent frontend modularization plan.
- Good performance discipline.
- Correct instinct to use server `inverseTransaction` instead of client revert math.
- Good extraction of `treeWalker`, `CellEditStore`, and `OptimisticValues`.
- Good use of patch refresh in the current adapter/runtime path.

### 2.2 Existing Architecture Weaknesses

- It still treats the client as partially authoritative for affected descendants.
- It still models aggregate edits too much like cell edits.
- It still tolerates overlapping edit regions through client counters.
- It still keeps `propagationStrategy: 'none'` as if it were a real data edit.
- It still allows no-history fallback paths that produce approximate semantics.

### 2.3 Replacement Architecture Strengths

- Correct semantic model.
- Clean separation between intent, plan, event, and projection.
- Clean parent/child conflict policy.
- Clean handling of publish/undo/redo.
- Eliminates fake parent-only data edits.

### 2.4 Replacement Architecture Weaknesses

- Too large a jump if applied literally in one phase.
- Too abstract unless anchored to the current table transport.
- If implemented naively, it could slow down the current UX.

## 3. Final Recommendation

The best system is a hybrid:

- frontend shell from `EDIT_SYSTEM_ARCHITECTURE.md`
- server authority and scope rules from `edit-propagation-clean-architecture.md`

In short:

1. Keep the current fast table.
2. Keep a client edit engine.
3. Make that client engine a rendering/orchestration layer only.
4. Move propagation truth, overlap truth, inverse truth, and patch truth to the server.

## 4. Final Target Model

### 4.1 Core Principle

There are two kinds of state:

1. UI overlay state
2. authoritative edit session state

The client owns only UI overlay state.
The server owns authoritative edit session state.

### 4.2 Authoritative Server Session

Each table instance gets an edit session:

```text
EditSession {
  session_id
  base_snapshot_version
  session_version
  status: active | published | discarded
}
```

This is not a full table copy.

It is:

- base data snapshot version
- overlay of changed leaf rows
- event log
- scope locks

That keeps the architecture clean without paying the cost of duplicating the full table.

### 4.3 Frontend Engine

Keep `useEditEngine`, but narrow its job.

It should own:

- `CellEditStore`
- `OptimisticValues`
- `PropagationGate`
- `EditSerializer`
- panel state
- request dispatch/pending state

It should not own:

- descendant truth
- propagation math
- inverse construction
- overlap legality
- authoritative revert planning

## 5. Final Layering

### 5.1 Frontend Layer: `useEditEngine`

Responsibilities:

- collect edit intent
- stage propagation method choice
- apply optimistic overlay
- show direct/propagated markers from server-confirmed metadata
- render patch results
- persist lightweight UI edit state

### 5.2 Adapter Layer

Keep the current `handle_transaction` transport contract and patch refresh pipeline, but enrich it.

The adapter should carry:

- `eventId`
- `sessionVersion`
- `scopeLocks`
- `affectedCells`
- `impactedScopeIds`
- `patchPayload`
- `inverseTransaction` and `redoTransaction` only as a compatibility envelope during migration

### 5.3 Server Edit Core

Add these backend services:

1. `CellTargetResolver`
2. `ScopeIndex`
3. `PropagationPlanner`
4. `ConflictManager`
5. `EditSessionManager`
6. `PatchPlanner`

## 6. Merged Data Model

### 6.1 Keep `CellEditStore`, But Change Its Meaning

Keep the old document's store shape because it is good for rendering:

```text
CellEditEntry {
  rowId
  colId
  originalValue
  directCount
  propagatedCount
}
```

But:

- this store is now a UI projection cache
- it is not the source of truth for revert semantics
- it is not used to infer hidden descendants

That is the key merge.

### 6.2 Keep `OptimisticValues`

Keep the existing optimistic overlay idea because it is good for UX and performance.

But optimistic values must be:

- request-scoped
- cleared by server confirmation
- overwritten by authoritative patch payloads

### 6.3 Replace Client History With Event-Backed History

The client can still maintain stacks, but entries must be server event-backed:

```text
HistoryEntry {
  eventId
  sessionVersion
  source
  affectedCells
  patchSummary
  inverseTransaction   // compatibility phase only
  redoTransaction      // compatibility phase only
}
```

Final target:

- `undo(eventId)`
- `redo(eventId)`
- `replace(eventId, newPolicy)`

Short-term compatibility:

- keep using `inverseTransaction`/`redoTransaction` emitted by the server
- but treat them as server-generated artifacts, not client-owned logic

## 7. Parent/Child Rule

This is the most important decision.

### 7.1 Final Rule

If a parent aggregate edit is active in the session, child edits under that parent are blocked.

Likewise:

- if a child edit is active, editing its ancestor aggregate is blocked
- same-scope replacement is allowed
- sibling edits are allowed

### 7.2 Why This Is The Right Merge

This preserves the clean semantics from the replacement architecture while keeping the fast client shell from the old one.

The old design tried to survive overlap with counters.
That is useful for rendering but not sufficient for correctness.

Use counters for UI markers.
Use locks for legality.

### 7.3 Lock Model

```text
ScopeLock {
  scope_id
  measure_id
  lock_mode: exact_scope | subtree
  owner_event_id
}
```

Rules:

- leaf edit: exact scope lock
- aggregate edit: subtree lock
- ancestor/descendant overlap: reject
- same scope + same measure + replace action: allow

## 8. Propagation Policies

### 8.1 Real Data Policies

Supported in the real edit pipeline:

- `equal_delta`
- `proportional_scale`
- `weighted_shift`
- `manual_allocation`

### 8.2 Remove `none` From Real Data Editing

Do not keep `none` as a real persisted edit policy.

If you keep it in the UI, rename it and split it:

- `scenario_override`

That becomes a separate overlay feature with separate semantics.

It must not share the same history, publish, or revert path as real data edits.

This is one of the biggest corrections relative to the current architecture.

## 9. Patch Refresh

This is where the merge matters for performance.

### 9.1 Keep Current Patch Refresh Infrastructure

Keep:

- adapter patch-refresh mode
- visible row path transport
- visible center column ids
- virtualized rendering

These are already valuable and performant.

### 9.2 Change Patch Authority

The current design partially infers descendants from visible rows.
The final design must not.

Instead:

1. server resolves authoritative `impactedScopeIds`
2. adapter intersects with current visible row paths
3. adapter recomputes only impacted visible scopes
4. adapter emits `patchPayload`
5. client renders it

So:

- keep the current patch path
- replace the current patch targeting logic with scope-aware server planning

## 10. Hot Path Performance Requirements

These must remain true:

1. Per-cell render remains O(1).
2. Cell display remains:
   - base value lookup
   - optimistic overlay lookup
   - marker lookup
3. No full-session scans on scroll.
4. No React state explosion.
5. No full viewport reload for normal aggregate edits when patch refresh is possible.

### 10.1 Concrete Frontend Performance Contract

Keep from the old architecture:

- `Map`-based stores
- epoch counters
- frozen marker sentinels
- serialized store shape
- hook extraction

### 10.2 Concrete Backend Performance Contract

The server must not recompute the whole pivot tree for every edit.

For each edit:

1. resolve target scope
2. compute leaf changes
3. collect impacted ancestor scopes
4. recompute only impacted visible scopes
5. fall back to viewport refresh only when impact is too wide

## 11. Undo, Redo, Replace, Revert

### 11.1 Undo/Redo

Keep the old design's UX.
Adopt the replacement design's semantics.

That means:

- undo/redo buttons remain in the table
- server remains the authority
- client stack is only a cache of server-confirmed events

### 11.2 Replace Propagation

Keep the old insight:

- changing propagation is semantically undo + reapply

But execute it on the server as:

```text
replace(eventId, new_policy)
```

The server may optimize internally into one atomic operation.

The client must stop synthesizing descendant reverts for this.

### 11.3 Revert Selected / Revert Cell

For the final system:

- `revertCell` means revert the event that directly owns that cell
- `revertSelected` means revert the unique owning events for the selected cells
- deduplicate by `eventId`

This keeps the good selection UX from the old design.

### 11.4 No Approximate No-History Fallback For Server-Side Mode

This is an important correction.

For server-side mode:

- do not fall back to approximate revert with `propagationStrategy: 'none'`
- if history is missing, the session is incomplete and revert must be rejected or the session must be rehydrated from the server

Approximate revert is acceptable only in client-only mode.

## 12. Persistence

### 12.1 Client Persistence

Keep the old serializer idea.

Persist:

- panel state
- display mode
- lightweight edited-cell overlay metadata
- propagation log

Do not rely on client persistence as the only source of edit truth.

### 12.2 Server Persistence

Persist:

- session
- events
- leaf changes
- locks
- session version

This removes the need for approximate revert after reload.

## 13. Grand Total

Keep the old document's warning instinct.
Keep the replacement architecture's stricter rule.

Final rule:

- grand total is not editable inline by default

If enabled:

- open a dedicated bulk-edit flow
- show affected row count
- require explicit confirmation
- require policy choice
- allow only on bounded row counts

## 14. Final Unified Architecture

### 14.1 What The User Sees

- same fast table
- same side panel
- same propagation picker UX
- same undo/redo UX
- same patch refresh responsiveness

### 14.2 What Changes Under The Hood

- server owns propagation truth
- server owns overlap truth
- server owns revert/replace truth
- client becomes a fast shell, not a second edit engine

### 14.3 What We Explicitly Reject

- client-authored descendant truth
- parent-only fake persisted edits
- overlapping ancestor/descendant edits in one session
- approximate server-side revert after persistence restore

## 15. Recommended Build Order

This is the safest way to get there without hurting the current table.

### Phase 1

Implement the old document's frontend extraction:

- `useEditEngine`
- `CellEditStore`
- `OptimisticValues`
- `EditSerializer`
- `PropagationGate`

No major behavior change yet.

### Phase 2

Add server event ids and session versions to transaction responses.

Keep current `inverseTransaction`/`redoTransaction` for compatibility.

### Phase 3

Add backend `ScopeIndex` and `ConflictManager`.

Block overlapping parent/child edits on the server.

### Phase 4

Move reapply/revert ownership fully to server events.

Delete client-generated descendant revert logic.

### Phase 5

Replace patch targeting with server-driven `impactedScopeIds`.

Keep the existing patch transport path.

### Phase 6

Split `none` into a separate scenario feature or remove it.

### Phase 7

Persist full server session state so reload keeps exact history.

## 16. Final Answer

The best architecture is not either document alone.

The correct merged system is:

- `EDIT_SYSTEM_ARCHITECTURE.md` for frontend structure, hot-path performance, and migration discipline
- `edit-propagation-clean-architecture.md` for server authority, overlap policy, and semantic correctness

If you want one sentence:

keep the current fast client shell, but make the server the single source of truth for propagation plans, scope locks, event history, and patch impact.
