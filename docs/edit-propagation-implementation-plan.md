# Edit Propagation Implementation Plan

Status: concrete repo plan

References:

- [EDIT_SYSTEM_ARCHITECTURE.md](C:/Users/ramzy/Downloads/serverside_pivot/EDIT_SYSTEM_ARCHITECTURE.md)
- [edit-propagation-clean-architecture.md](C:/Users/ramzy/Downloads/serverside_pivot/docs/edit-propagation-clean-architecture.md)
- [edit-propagation-unified-target-architecture.md](C:/Users/ramzy/Downloads/serverside_pivot/docs/edit-propagation-unified-target-architecture.md)

Goal:

- preserve the current table's performance characteristics
- migrate toward server-authoritative propagation and history
- avoid a destabilizing rewrite

## 1. Target Repo Layout

### 1.1 New Backend Package

Create:

```text
dash_tanstack_pivot/pivot_engine/pivot_engine/editing/
  __init__.py
  models.py
  target_resolver.py
  scope_index.py
  conflict_manager.py
  propagation_planner.py
  patch_planner.py
  session_manager.py
  event_store.py
  service.py
```

Rationale:

- keep edit domain logic out of `scalable_pivot_controller.py`
- make the adapter delegate to a focused service
- isolate scope/event/session logic for testing

### 1.2 New Frontend Hook Package

Create:

```text
dash_tanstack_pivot/src/lib/hooks/useEditEngine/
  index.js
  constants.js
  cellEditStore.js
  optimisticValues.js
  transactionHistory.js
  propagationGate.js
  editSerializer.js
  treeWalker.js
  selectors.js
```

`treeWalker.js` remains a UI helper only.
It must never be used as the source of truth for hidden descendants or revert legality.

## 2. Existing Files In Scope

### 2.1 Backend Files To Modify

- `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/models.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py`

### 2.2 Frontend Files To Modify

- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js`
- `dash_tanstack_pivot/src/lib/components/Table/EditSidePanel.js`
- `dash_tanstack_pivot/src/lib/hooks/useColumnDefs.js`
- `dash_tanstack_pivot/src/lib/components/Table/EditableCell.js`
- `dash_tanstack_pivot/src/lib/components/Table/PivotTableBody.js`
- `dash_tanstack_pivot/src/lib/utils/editing.js`
- `dash_tanstack_pivot/src/lib/index.js`

### 2.3 Generated Dash Artifacts To Regenerate After Frontend Prop Changes

- `dash_tanstack_pivot/dash_tanstack_pivot/DashTanstackPivot.py`
- `dash_tanstack_pivot/dash_tanstack_pivot/PivotAppBar.py`
- `dash_tanstack_pivot/dash_tanstack_pivot/_imports_.py`
- `dash_tanstack_pivot/dash_tanstack_pivot/metadata.json`
- `dash_tanstack_pivot/dash_tanstack_pivot/dash_tanstack_pivot.min.js`
- `dash_tanstack_pivot/dash_tanstack_pivot/package-info.json`

## 3. Test Files In Scope

### 3.1 Existing Tests To Extend

- `tests/test_runtime_service.py`
- `tests/test_editing_e2e.py`
- `tests/test_frontend_contract.py`
- `tests/test_dash_runtime_callbacks.py`
- `tests/test_multi_instance_isolation.py`
- `tests/test_session_request_gate.py`

### 3.2 New Unit Tests To Add

- `tests/test_edit_target_resolver.py`
- `tests/test_edit_scope_index.py`
- `tests/test_edit_conflict_manager.py`
- `tests/test_edit_session_manager.py`
- `tests/test_edit_patch_planner.py`

## 4. Phase Plan

## Phase 0: Baseline Freeze

Objective:

- freeze current behavior before moving logic

Backend changes:

- no production behavior changes

Frontend changes:

- no production behavior changes

Tests:

- add focused regression tests to `tests/test_runtime_service.py` for:
  - aggregate edit inverse correctness
  - patch refresh after parent edit
  - proportional aggregate edit
  - count rejection
- add focused E2E assertions to `tests/test_editing_e2e.py` for:
  - inline aggregate edit
  - undo/redo round trip
  - propagation picker
  - reapply propagation current behavior

Acceptance criteria:

- current edit suite is green
- current behavior is explicitly captured before refactor begins

## Phase 1: Frontend Extraction Only

Objective:

- move edit code out of `DashTanstackPivot.react.js` without changing semantics

Create:

- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/index.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/constants.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/cellEditStore.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/optimisticValues.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/transactionHistory.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/propagationGate.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/editSerializer.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/treeWalker.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/selectors.js`

Modify:

- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js`
- `dash_tanstack_pivot/src/lib/components/Table/EditSidePanel.js`

Frontend work:

1. Move current refs/state into `useEditEngine`.
2. Preserve current prop surface and current runtime behavior.
3. Move all inline edit-panel handlers into named engine methods.
4. Keep `inverseTransaction`/`redoTransaction` flow exactly as-is for now.
5. Replace repeated descendant/ancestor scan code with `treeWalker.js`.

Backend work:

- none

Tests:

- extend `tests/test_frontend_contract.py` to assert the component consumes `useEditEngine`
- keep all runtime and E2E tests green

Acceptance criteria:

- zero backend behavior change
- `DashTanstackPivot.react.js` loses the bulk of edit logic
- render performance is unchanged

## Phase 2: Transaction Envelope Upgrade

Objective:

- introduce server-backed event/session metadata without breaking existing transaction flow

Create:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/models.py`

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/models.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py`

Modify frontend:

- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/transactionHistory.js`
- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js`

Backend work:

1. Extend transaction responses with:
   - `eventId`
   - `sessionVersion`
   - `history.captureable`
   - `affectedCells` placeholder
   - `impactedScopeIds` placeholder
2. Keep existing:
   - `inverseTransaction`
   - `redoTransaction`
   - `patchPayload`
3. Pass these fields through runtime models and service response payloads unchanged.

Frontend work:

1. Store `eventId` and `sessionVersion` in pending/confirmed history entries.
2. Keep old undo/redo behavior using `inverseTransaction` and `redoTransaction`.
3. Do not change revert semantics yet.

Tests:

- add runtime tests for response envelope shape in `tests/test_runtime_service.py`
- add callback transport tests in `tests/test_dash_runtime_callbacks.py`

Acceptance criteria:

- old clients still work
- new metadata arrives end-to-end

## Phase 3: Backend Edit Domain Extraction

Objective:

- remove propagation/domain reasoning from `scalable_pivot_controller.py` and centralize it

Create:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/target_resolver.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/scope_index.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/propagation_planner.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py`

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`

Backend work:

1. Move aggregate-target resolution into `target_resolver.py`.
2. Build stable scope identities in `scope_index.py`.
3. Move propagation planning for:
   - `sum`
   - `avg`
   - `weighted_avg`
   into `propagation_planner.py`.
4. Let `tanstack_adapter.handle_transaction()` delegate aggregate edits into `editing/service.py`.
5. Keep leaf-row mutation execution in the controller for now, but drive it from planner outputs rather than inline aggregate logic.

Frontend work:

- none beyond wiring any response shape changes

Tests:

- add unit tests:
  - `tests/test_edit_target_resolver.py`
  - `tests/test_edit_scope_index.py`
- keep `tests/test_runtime_service.py` green for aggregate edits

Acceptance criteria:

- aggregate semantics live outside `scalable_pivot_controller.py`
- behavior is unchanged for supported edit types

## Phase 4: Scope Locks And Parent/Child Blocking

Objective:

- enforce the final ancestor/descendant conflict rule on the server

Create:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/conflict_manager.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/session_manager.py`

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py`

Modify frontend:

- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/index.js`
- `dash_tanstack_pivot/src/lib/components/Table/EditSidePanel.js`
- `dash_tanstack_pivot/src/lib/hooks/useColumnDefs.js`

Backend work:

1. Introduce session-local scope locks:
   - leaf edit => exact scope lock
   - aggregate edit => subtree lock
2. Reject overlapping ancestor/descendant edits for same measure.
3. Allow same-scope replacement path.
4. Include lock/conflict reason in transaction warnings.

Frontend work:

1. Surface server lock rejections as non-destructive notifications.
2. Optionally show disabled editing affordance for blocked cells in view.
3. Keep marker counters for UI display only.

Tests:

- add `tests/test_edit_conflict_manager.py`
- add runtime coverage in `tests/test_runtime_service.py` for:
  - parent edit then child edit => rejected
  - child edit then parent edit => rejected
  - sibling edit => allowed
  - same-scope replace => allowed

Acceptance criteria:

- overlapping parent/child edits are impossible in server-side mode

## Phase 5: Server-Owned Revert And Replace

Objective:

- stop synthesizing descendant revert/reapply logic in the browser

Create:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/event_store.py`

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`

Modify frontend:

- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/transactionHistory.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/index.js`
- `dash_tanstack_pivot/src/lib/components/Table/EditSidePanel.js`
- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js`

Backend work:

1. Persist exact event ownership:
   - `eventId`
   - `scopeId`
   - `measureId`
   - affected leaf changes
2. Add operations:
   - `undo(eventId)` or strict `undoLast(sessionId)`
   - `redo(eventId)` or strict `redoLast(sessionId)`
   - `replace(eventId, newPolicy)`
   - `revertEvents([eventId...])`
3. Continue emitting compatibility `inverseTransaction` and `redoTransaction`.

Frontend work:

1. Replace current client-generated descendant revert logic.
2. `reapplyPropagation` becomes a server `replace` call.
3. `revertCell` and `revertSelected` deduplicate by `eventId`.
4. Keep client-only mode fallback path separate.

Tests:

- add `tests/test_edit_session_manager.py`
- extend `tests/test_editing_e2e.py` for:
  - revert selected deduplication
  - replace propagation
  - undo after replace

Acceptance criteria:

- no server-side revert or reapply path depends on client descendant scans

## Phase 6: Server-Driven Patch Planning

Objective:

- keep current patch-refresh speed, but make patch impact authoritative

Create:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/patch_planner.py`

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py`

Modify frontend:

- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/index.js`

Backend work:

1. Replace current transaction patch target resolution with server-produced `impactedScopeIds`.
2. Adapter intersects `impactedScopeIds` with `visibleRowPaths`.
3. Adapter recomputes only impacted visible scopes.
4. Fall back to viewport refresh when patch breadth is too large.

Frontend work:

1. Continue consuming patch payload exactly as today.
2. Remove any remaining assumptions that client marker plans define patch impact.

Tests:

- add `tests/test_edit_patch_planner.py`
- extend `tests/test_runtime_service.py` patch cases for:
  - hidden descendants
  - impacted ancestors
  - viewport fallback threshold

Acceptance criteria:

- patch refresh path stays
- visible-row scans are no longer authoritative

## Phase 7: Remove `none` From Real Edit Path

Objective:

- remove the biggest semantic flaw without harming UX

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/propagation_planner.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py`

Modify frontend:

- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/propagationGate.js`
- `dash_tanstack_pivot/src/lib/components/Table/EditSidePanel.js`
- `dash_tanstack_pivot/src/lib/utils/editing.js`

Backend work:

1. Remove `none` as a valid real aggregate propagation policy.
2. Reject persisted aggregate edits that request `none`.
3. Optionally introduce a separate non-persistent `scenario_override` path later.

Frontend work:

1. Remove `None` from real propagation choices.
2. If needed, add a clearly separate scenario-only UI path later.

Tests:

- update runtime tests to reject `none` in server-side real edit mode
- update E2E propagation picker assertions

Acceptance criteria:

- no real edit can claim success while changing zero child rows

## Phase 8: Grand Total Bulk Flow

Objective:

- stop treating grand total like a normal inline edit

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/target_resolver.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py`

Modify frontend:

- `dash_tanstack_pivot/src/lib/components/Table/EditSidePanel.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/propagationGate.js`
- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js`

Backend work:

1. Disable inline grand total edits by default.
2. If enabled by config, require dedicated bulk-edit semantics with row-count guard.

Frontend work:

1. Remove current grand-total inline assumption.
2. Route to confirmation flow if enabled.

Tests:

- runtime tests for blocked grand total inline edits
- optional tests for allowed bounded bulk flow

Acceptance criteria:

- no accidental `WHERE TRUE` mass edit from normal inline editing

## Phase 9: Server Session Persistence

Objective:

- make reload-safe exact history possible

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/session_manager.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/event_store.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py`

Modify frontend:

- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/editSerializer.js`
- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/index.js`
- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js`

Backend work:

1. Persist server edit session state:
   - session
   - events
   - locks
   - session version
2. Add session rehydrate endpoint/response path via transaction envelope.

Frontend work:

1. On restore, rehydrate from server session metadata when available.
2. Keep local serializer only for UI/panel state and lightweight overlay hints.
3. Remove approximate server-side revert fallback.

Tests:

- reload-resume coverage in `tests/test_editing_e2e.py`
- multi-instance session isolation in `tests/test_multi_instance_isolation.py`

Acceptance criteria:

- after reload, server-side revert and undo stay exact

## Phase 10: Cleanup And Contract Tightening

Objective:

- remove migration scaffolding and lock in the new contract

Modify backend:

- `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py`

Modify frontend:

- `dash_tanstack_pivot/src/lib/hooks/useEditEngine/transactionHistory.js`
- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js`
- generated Dash artifacts

Work:

1. Decide whether `inverseTransaction`/`redoTransaction` remain public compatibility fields or become internal-only.
2. Remove dead fallback logic.
3. Simplify panel and history APIs around `eventId`.
4. Regenerate Dash component metadata/bundle artifacts.

Tests:

- full regression suite
- contract tests updated to new response shape

Acceptance criteria:

- architecture matches the unified target document
- no major logic duplication remains between client and server

## 5. Recommended Execution Order

Run phases in this order:

1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 4
6. Phase 5
7. Phase 6
8. Phase 7
9. Phase 8
10. Phase 9
11. Phase 10

Do not start Phase 5 before Phase 4 is stable.
Do not remove `none` before server-owned replace/revert exists.
Do not remove server-side revert fallback before session persistence exists.

## 6. First Implementation Slice

If starting immediately, the highest-value first slice is:

1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 4

Why:

- Phase 1 reduces frontend complexity fast
- Phase 2 gives us event/session metadata without breaking the app
- Phase 4 fixes the worst correctness issue: overlapping parent/child edits

## 7. Definition Of Done

The system is done when all of these are true:

1. The table still uses patch refresh and stays fast under virtualized scrolling.
2. Aggregate propagation is server-owned.
3. Parent/child overlapping edits are blocked server-side.
4. Revert and replace do not use client descendant math.
5. `none` is no longer a fake real edit.
6. Reload-safe exact history exists for server-side mode.
7. Client stores are render caches, not authoritative semantic state.
