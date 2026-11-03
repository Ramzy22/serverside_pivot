# Clean Architecture For Hierarchical Edit Propagation

Status: proposed replacement architecture

Scope: server-side editing of hierarchical and pivot aggregates, propagation to leaves, undo/redo, selective revert, and parent/child conflict handling.

## 1. Executive Summary

The correct model is:

1. Users never truly edit aggregate cells.
2. Users submit an edit intent against an aggregate cell.
3. The server resolves that intent into leaf-row mutations inside an edit session branch.
4. The UI renders `base snapshot + branch overlay`.
5. Undo/redo/revert operate on server-stored events, never on client-reconstructed marker plans.

The most important policy decision:

- Recommended default: forbid overlapping edits on ancestor/descendant scopes inside the same edit session.
- That means if a parent scope has an active propagated edit, no child scope under it can be edited until the parent edit is reverted, replaced, or the session is published.

This is the clean rule because aggregate propagation is not commutative. Allowing parent and child edits to stack on overlapping scopes requires event rebase/transform logic. That is possible, but it is not the right baseline architecture.

## 2. Non-Negotiable Invariants

These rules should drive the design:

1. Only leaf rows are persisted. Parent, subtotal, and grand total rows are derived views.
2. Aggregate edits must always resolve to leaf mutations or be rejected.
3. The client must never infer affected descendants from visible rows.
4. The server must own propagation planning, conflict detection, inverse generation, and patch computation.
5. Undo/redo must operate on server event ids, not client-built synthetic transactions.
6. Every editable cell must resolve to a stable semantic target:
   - leaf target
   - aggregate scope target
   - measure
   - aggregation type
7. If an aggregate cannot be mapped to deterministic leaf mutations, it is not editable.

## 3. Product Rules

### 3.1 Supported Edit Types

- Leaf value edit: direct mutation of one or more source rows.
- Aggregate target edit: set a target aggregate value and choose a propagation policy.
- Explicit child allocation edit: user directly specifies new child targets for a parent scope.

### 3.2 Not Real Data Edits

These should not be modeled as normal edits:

- Parent-only override without touching children
- Temporary override of a subtotal that disappears on refresh
- Formula/window/count/distinct-count edits without a dedicated planner

If you want parent-only overrides, they belong in a separate scenario/what-if system, not in the persisted data-edit pipeline.

### 3.3 Aggregate Editability Matrix

Editable by default:

- `sum`
- `avg`
- `weighted_avg`

Not editable by default:

- `count`
- `count_distinct`
- `min`
- `max`
- calculated formulas
- window functions
- ratios built from multiple measures

These can be added later only if they get a dedicated propagation planner with exact inverse semantics.

## 4. Recommended Parent/Child Policy

### 4.1 Baseline Rule

Inside an active edit session:

- if scope `A` has an active aggregate edit,
- no edit may target any ancestor or descendant of `A`,
- unless the new edit is an explicit replacement of the same scope+measure edit.

This is the default rule I recommend.

### 4.2 Why This Rule Is Correct

A parent edit on `North` and a later child edit on `North|||USA` are not independent operations:

- `equal` propagation is additive
- `proportional` propagation is multiplicative
- custom distributions can be arbitrary

If both are allowed, then:

- selective revert of the older parent edit becomes a rebase problem
- changing propagation method becomes a transform problem
- audit becomes harder because one user action no longer maps cleanly to one scope outcome

The clean architecture avoids that by making scope overlap illegal inside a draft branch.

### 4.3 Practical Workflow

The user can still do both actions, but sequentially:

1. edit parent scope
2. publish session or explicitly replace/revert that edit
3. edit child scope on the new snapshot

That preserves user intent without introducing overlapping-scope ambiguity.

### 4.4 Optional Advanced Mode

If you later want child-after-parent editing, implement it as an advanced feature:

- allow overlap only on leaf targets
- require full event sourcing
- require server-side rebase of descendant events when ancestor events are reverted or replaced
- disable selective revert of non-tip events unless rebase succeeds

Do not build v1 around this.

## 5. Architecture Overview

The system should be split into six server-owned layers.

### 5.1 Cell Address Resolver

Input:

- row identity
- column identity
- current view definition

Output:

```text
ResolvedCellTarget {
  target_kind: leaf | aggregate
  scope_id: stable scope key
  parent_scope_id: stable scope key | null
  hierarchy_depth: int
  measure_id: stable measure key
  aggregation_fn: string | null
  leaf_predicate: canonical predicate over source rows
  editable: bool
  reason_if_not_editable: string | null
}
```

This resolver must use stable ids, never display labels.

### 5.2 Scope Index

The server needs a canonical hierarchy index:

```text
ScopeNode {
  scope_id
  parent_scope_id
  depth
  dimension_values
  leaf_predicate
  leaf_count
}
```

Requirements:

- stable across viewport changes
- independent of which rows are currently visible
- able to answer:
  - ancestors of scope
  - descendants of scope
  - leaf rows belonging to scope

Implementation options:

- materialized path index
- parent-child scope table
- precomputed scope registry per request snapshot

The exact storage can vary. The contract cannot.

### 5.3 Edit Session Branch

Every user edit happens inside a session branch.

```text
EditSession {
  session_id
  base_snapshot_version
  status: active | published | discarded
  created_at
  updated_at
}
```

The branch holds:

- command journal
- current overlay on leaf rows
- active scope locks

The UI always reads from:

```text
projection = base_snapshot + overlay
```

This is the key separation that keeps editing sane.

### 5.4 Propagation Planner

This service converts an aggregate target edit into deterministic leaf changes.

Input:

```text
AggregateEditIntent {
  session_id
  scope_id
  measure_id
  requested_value
  propagation_policy
  expected_session_version
}
```

Output:

```text
PropagationPlan {
  affected_leaf_rows: [leaf_row_id...]
  before_values: map[leaf_row_id] -> value
  after_values: map[leaf_row_id] -> value
  impacted_scopes: [scope_id...]
  inverse_payload
}
```

Supported policies:

- `equal_delta`
- `proportional_scale`
- `weighted_shift`
- `manual_allocation`

The planner must reject impossible cases, for example:

- proportional edit from zero base
- integer-constrained target that cannot be represented exactly
- unsupported aggregation type

### 5.5 Conflict Manager

The conflict manager owns edit legality.

```text
ScopeLock {
  session_id
  scope_id
  measure_id
  lock_mode: exact_scope | subtree
  owner_command_id
}
```

Rules:

- leaf edit locks exact leaf rows for that measure
- aggregate edit locks the subtree of its scope for that measure
- new command is rejected if it overlaps an existing incompatible lock

Recommended lock behavior:

- same scope + same measure + same session: replace allowed
- sibling scopes: allowed
- ancestor/descendant overlap: rejected
- grand total scope: disabled by default or routed to dedicated bulk-edit flow

### 5.6 Projection Engine

After a plan is accepted:

1. update overlay leaf values
2. recompute impacted scopes only
3. return:
   - updated visible rows
   - changed summary cells
   - session version
   - command metadata

The client should receive a patch already computed from authoritative scope ids, not build one from visible descendants.

## 6. Command And Event Model

Use commands for intent and events for committed branch state.

### 6.1 Commands

```text
EditCommand {
  command_id
  session_id
  type: leaf_set | aggregate_target | explicit_allocation | undo | redo | replace
  target_scope_id
  measure_id
  payload
  created_by
  created_at
}
```

### 6.2 Events

```text
EditEvent {
  event_id
  session_id
  command_id
  target_scope_id
  measure_id
  event_kind
  propagation_policy
  affected_leaf_count
  impacted_scope_ids
  before_snapshot_ref
  after_snapshot_ref
  session_version
  created_at
}
```

Important:

- store exact before/after state for the affected leaf rows or a lossless compressed equivalent
- never ask the client to reconstruct an inverse

## 7. Undo, Redo, Revert, Replace

### 7.1 Undo/Redo

Undo/redo are server operations on event ids:

- `undo(last_event_id)`
- `redo(last_undone_event_id)`

Recommended default:

- strict LIFO undo/redo inside a session

This is the correct tradeoff. It keeps semantics exact and removes rebase complexity.

### 7.2 Replace Aggregate Edit

Changing the propagation method or target value for the same scope should be modeled as:

1. undo the existing scope event
2. apply a new event on the same scope

This can be optimized server-side into one atomic replace operation.

### 7.3 Selective Revert

Selective revert of arbitrary older events should not be part of the baseline architecture.

Support it only if you also support:

- event dependency graph
- overlap analysis
- automatic rebase of newer dependent events

Without that, selective revert will produce edge cases that never fully close.

## 8. Grand Total Policy

Grand total edit is not a normal inline edit.

Recommended rule:

- disable grand total inline editing by default

If product insists on it:

- route it through a dedicated bulk-edit dialog
- show affected row count before confirmation
- require explicit propagation policy
- require threshold guard
- require publish/commit confirmation

Do not treat grand total like a regular cell.

## 9. Client Responsibilities

The client should be intentionally dumb.

The client may do:

- collect user intent
- show pending spinner
- render server patch
- show edit event list
- call undo/redo/replace endpoints by event id

The client must not do:

- descendant discovery
- ancestor propagation math
- inverse construction
- revert synthesis
- overlap resolution
- authoritative marker bookkeeping

The client should display server-derived metadata:

```text
EditOverlaySummary {
  session_id
  session_version
  active_events: [...]
  blocked_scopes: [...]
  edited_cells_in_view: [...]
}
```

## 10. API Contract

### 10.1 Apply Edit

```json
POST /edit-sessions/{session_id}/commands
{
  "type": "aggregate_target",
  "target": {
    "scopeId": "region=North",
    "measureId": "sales_sum"
  },
  "payload": {
    "requestedValue": 300,
    "propagationPolicy": "proportional_scale"
  },
  "expectedSessionVersion": 12
}
```

Response:

```json
{
  "accepted": true,
  "sessionVersion": 13,
  "eventId": "evt_123",
  "patch": {
    "rows": [],
    "cells": []
  },
  "overlaySummary": {},
  "warnings": []
}
```

### 10.2 Undo

```json
POST /edit-sessions/{session_id}/undo
{
  "expectedSessionVersion": 13
}
```

### 10.3 Replace Scope Edit

```json
POST /edit-sessions/{session_id}/commands
{
  "type": "replace",
  "target": {
    "scopeId": "region=North",
    "measureId": "sales_sum"
  },
  "payload": {
    "requestedValue": 300,
    "propagationPolicy": "equal_delta"
  },
  "expectedSessionVersion": 13
}
```

### 10.4 Publish

```json
POST /edit-sessions/{session_id}/publish
{
  "expectedSessionVersion": 13
}
```

Publish should:

1. apply overlay leaf mutations to source tables in one transaction
2. write audit record
3. clear session branch
4. advance base snapshot version

## 11. Persistence Model

Minimal durable tables:

- `edit_sessions`
- `edit_commands`
- `edit_events`
- `edit_event_leaf_changes`
- `edit_scope_locks`
- `edit_publish_audit`

If storage volume is a concern:

- store leaf changes compressed by row ranges or predicate blocks
- keep exact values for undo safety
- archive old sessions

## 12. Patch Refresh Model

Patch refresh must be driven by impacted scopes, not by visible descendant scans.

Algorithm:

1. command produces `impacted_scope_ids`
2. intersect those with viewport scope ids
3. recompute only that set
4. send rows/cells patch
5. if overlap is too large, fall back to viewport refresh

This makes patch correctness independent of what happened to be expanded when the edit was created.

## 13. Concurrency Model

Support three versions:

- `base_snapshot_version`
- `session_version`
- `viewport_version`

Rules:

- viewport staleness does not corrupt edits because scope resolution uses session/base versions
- apply edit rejects if `expected_session_version` is stale
- publish rejects if base snapshot changed incompatibly unless rebase succeeds

## 14. Why This Is Better Than A Client-Heavy Design

This architecture avoids the typical failure modes:

- no visible-row-dependent propagation
- no fake parent-only persisted edits
- no client-synthesized inverse transactions
- no split-brain between optimistic state, markers, original values, and history
- no ambiguous ancestor/descendant overlap

## 15. Implementation Order

Build it in this order:

1. Stable scope identity and cell target resolution
2. Edit session branch and session versioning
3. Server-side propagation planner for `sum`
4. Scope lock manager with ancestor/descendant blocking
5. Event store with exact before/after leaf snapshots
6. Undo/redo by event id
7. Patch engine driven by impacted scopes
8. `avg` and `weighted_avg` planners
9. Publish flow
10. Optional advanced rebase support

## 16. Final Recommendation

If the goal is correctness first, the architecture should adopt these decisions:

- aggregate cells are intent targets, not mutable records
- propagation is server-only
- parent-only aggregate edits are removed from the data-edit path
- grand total editing is disabled by default
- ancestor/descendant overlapping edits are forbidden inside one active session
- undo/redo are strict server-owned event operations

That is the clean baseline. Everything more flexible than this is an advanced feature, not a foundation.
