# Master / Detail And Tree Architecture

Status: implemented MVP architecture  
Scope: `DashTanstackPivot`, runtime transport, backend execution, frontend row/detail surfaces

## Goal

Add two first-class capabilities without fighting the current server-side pivot design:

- `tree` as a real row model, not a pivot workaround
- `detail` as a row surface, not only a drill-through modal

This should stay native to the current architecture:

- server-side data execution
- unified `runtimeRequest` / `runtimeResponse`
- TanStack-based frontend
- Dash adapter on top

## Design Principles

1. `pivot`, `tree`, and `table` are different view modes.
2. `detail` is orthogonal to row expansion.
3. `drill` and `detail` are not the same feature.
4. Row metadata must use one normalized hierarchy contract across modes.
5. New work should extend the unified runtime contract, not reintroduce separate callback APIs.
6. The backend should stay mode-aware and transport-neutral.

## Product Model

### View Modes

- `table`: flat records grid
- `pivot`: grouped and aggregated analytical hierarchy
- `tree`: first-class hierarchical records

### Detail Modes

- `none`
- `inline`
- `sidepanel`
- `drawer`

### Important Rule

In `pivot` and `tree`:

- row expander controls children
- detail button controls detail surface

Do not overload one expander to do both.

## Current Repo Reality

These parts already exist and should be reused:

- unified runtime router: [service.py](../dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py)
- unified Dash callback transport: [dash_callbacks.py](../dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py)
- server-side pivot adapter: [tanstack_adapter.py](../dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py)
- grouped hierarchy row metadata: `_path`, `_pathFields`, `_has_children`, `_is_expanded`
- drill-through transport path: [service.py](../dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py)
- current detail surfaces: [InlineDetailPanel.js](../dash_tanstack_pivot/src/lib/components/Table/InlineDetailPanel.js)
- current hierarchy/grid shell: [DashTanstackPivot.react.js](../dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js)
- current pivot/table columns: [useColumnDefs.js](../dash_tanstack_pivot/src/lib/hooks/useColumnDefs.js)

## Implemented State

The current implementation now ships:

- `viewMode`: `pivot | report | tree | table`
- `detailMode`: `none | inline | sidepanel | drawer`
- `treeConfig`
- `detailConfig`
- unified `runtimeRequest` / `runtimeResponse` support for `detail`
- first-class tree row rendering with normalized row metadata
- lazy detail surfaces separate from row expansion

Current caveat:

- tree mode semantics are complete, but backend tree loading still materializes the filtered source rows in Python before building the visible tree. It is correct and server-driven, but not yet branch-paged at the database level.

## Target Architecture

### Layer Split

#### 1. Core Execution

- pivot execution
- tree execution
- detail payload resolution

This layer should not know about Dash UI details.

#### 2. Runtime Transport

- request parsing
- response normalization
- session gating
- request kind routing

This is the contract boundary.

#### 3. Dash Adapter

- Dash callback wiring
- prop marshalling
- Dash wrapper component exposure

#### 4. React UI

- grid shell
- view-mode-specific row handling
- detail surfaces

## Public API

Add to `DashTanstackPivot`:

```python
viewMode="pivot"      # pivot | report | tree | table
detailMode="none"     # none | inline | sidepanel | drawer
treeConfig=None
detailConfig=None
```

Default behavior:

- `viewMode="pivot"`
- `detailMode="none"`

Backward compatibility:

- keep current behavior as default
- keep legacy saved views readable by mapping stored `pivotMode` values to `viewMode`
- `viewMode="report"` is the public report-mode entry point; `pivotMode` is no longer part of the public API

## Tree Config

```python
treeConfig = {
    "sourceType": "path",      # path | adjacency | nested
    "idField": "id",
    "parentIdField": "parent_id",
    "pathField": "path",
    "childrenField": "children",
    "labelField": "name",
    "valueFields": ["sales", "cost"],
    "sortBy": "name",
    "sortDir": "asc",
}
```

Rules:

- `path`: use `pathField`
- `adjacency`: use `idField` + `parentIdField`
- `nested`: flatten once at load/controller layer, not in the browser

## Detail Config

```python
detailConfig = {
    "enabled": True,
    "defaultKind": "records",   # records | grid | pivot | chart
    "allowPerRowKind": True,
    "inlineHeight": 280,
    "sidepanelWidth": 480,
}
```

Longer term, allow per-row descriptors:

```python
{
    "kind": "pivot",
    "title": "Country Breakdown",
    "request": {...}
}
```

## Normalized Row Metadata Contract

Keep underscore-prefixed transport keys for compatibility, but treat this as the canonical model:

```json
{
  "_rowKey": "stable-row-key",
  "_parentKey": "stable-parent-key",
  "_path": "North|||USA",
  "_pathFields": ["region", "country"],
  "_depth": 1,
  "_has_children": true,
  "_is_expanded": false,
  "_can_detail": true,
  "_detail_kind": "records"
}
```

Rules:

- every hierarchical mode emits the same metadata shape
- `_path` remains the primary row identity for grouped/tree navigation
- `_rowKey` should become the stable frontend key when introduced
- `detail` should not depend on guessing row identity from display values

## Runtime Contract

Keep one transport:

- `runtimeRequest`
- `runtimeResponse`

### Request Kinds

- `data`
- `filter_options`
- `chart`
- `drill`
- `detail`

### Data Request

```json
{
  "kind": "data",
  "requestId": "abc",
  "payload": {
    "viewMode": "tree",
    "treeConfig": {...},
    "expanded": {...},
    "startRow": 0,
    "endRow": 99,
    "colStart": 0,
    "colEnd": 20,
    "sorting": [],
    "filters": {}
  }
}
```

### Detail Request

```json
{
  "kind": "detail",
  "requestId": "abc",
  "payload": {
    "viewMode": "pivot",
    "rowPath": "North|||USA",
    "rowFields": ["region", "country"],
    "detailKind": "records",
    "page": 0,
    "pageSize": 100
  }
}
```

### Response Shapes

`data`:

```json
{
  "kind": "data",
  "status": "data",
  "payload": {
    "rows": [...],
    "columns": [...],
    "rowCount": 1234,
    "dataOffset": 0,
    "dataVersion": 7
  }
}
```

`detail`:

```json
{
  "kind": "detail",
  "status": "data",
  "payload": {
    "rowPath": "North|||USA",
    "detailKind": "records",
    "title": "Source Rows",
    "rows": [...],
    "columns": [...],
    "rowCount": 42
  }
}
```

## Feature Semantics

### Drill

`drill` means:

- raw underlying source rows
- pagination
- raw sort/filter inside the drill surface
- export-compatible result set

### Detail

`detail` means:

- lazy child surface for a row
- can be records, grid, pivot, or chart
- lives inline or in a side surface

### Tree

`tree` means:

- hierarchical records
- root-only initial load
- children loaded only on expand
- sibling-level sorting and filtering
- no pivot/group aggregation unless explicitly requested

## Backend Implementation Map

### Keep

- [runtime/service.py](../dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py)
- [runtime/models.py](../dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/models.py)
- [runtime/dash_callbacks.py](../dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py)
- [tanstack_adapter.py](../dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py)

### Add

- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/tree_service.py`
- `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/detail_service.py`
- optionally `dash_tanstack_pivot/pivot_engine/pivot_engine/tree_controller.py`

### Responsibility Split

`service.py`

- route by `kind`
- route by `viewMode`
- keep session-gate behavior

`tree_service.py`

- normalize tree requests
- fetch roots
- fetch child slices
- return tree-mode row metadata

`detail_service.py`

- resolve one row into a detail payload
- support `records` first
- later support `grid`, `pivot`, `chart`

`tanstack_adapter.py`

- stay focused on pivot/table execution
- should not become the only home for tree/detail logic

## Frontend Implementation Map

### Keep

- [DashTanstackPivot.react.js](../dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js)
- [useServerSideRowModel.js](../dash_tanstack_pivot/src/lib/hooks/useServerSideRowModel.js)
- [useColumnDefs.js](../dash_tanstack_pivot/src/lib/hooks/useColumnDefs.js)

### Add

- `dash_tanstack_pivot/src/lib/components/Table/InlineDetailPanel.js`
- `dash_tanstack_pivot/src/lib/components/Table/DetailSidePanel.js`
- `dash_tanstack_pivot/src/lib/components/Table/DetailDrawer.js`
- `dash_tanstack_pivot/src/lib/components/Table/DetailSurfaceContent.js`

### Frontend Rules

- `pivot`, `table`, and `tree` currently share the main server-side row model
- tree mode is rendered via normalized hierarchical rows instead of a dedicated hook
- detail state should be managed separately from row expansion
- current modal drill UI remains available as a fallback renderer

## Rollout Plan

### Phase 1. Normalize Meta Contract

- formalize row metadata for all hierarchical rows
- keep compatibility fields
- no UI behavior change

Status: completed

### Phase 2. Add Public Props

- add `viewMode`
- add `detailMode`
- add `treeConfig`
- add `detailConfig`

Status: completed

### Phase 3. Add `detail` Runtime Kind

- route `detail` in runtime service
- keep `drill` separate
- return canonical detail payloads

Status: completed

### Phase 4. Replace Modal-Only Drill UX

- add inline detail row host
- keep modal as fallback for one release

Status: completed

### Phase 5. Add First-Class Tree Backend

- implement `tree_service.py`
- support `path` and `adjacency`
- root-only initial load
- child-on-expand fetch

Status: completed for MVP semantics

### Phase 6. Add Frontend Tree Mode

- implement `useTreeRowModel`
- render tree node column
- wire tree expansion to `runtimeRequest(kind="data")`

Status: completed

### Phase 7. Add Richer Detail Surfaces

- side panel
- drawer
- optional per-row detail kinds

Status: completed

### Phase 8. Cleanup

- deprecate modal-first drill UX
- later retire old naming where needed

Status: completed

Current state:

- raw record drill-through now reuses the detail surfaces instead of a separate modal
- internal components may still derive a local `pivotMode` flag from `viewMode` during the transition, but the public API is `viewMode` only

## MVP Recommendation

Build this first:

- `viewMode="tree"`
- `treeConfig.sourceType in {"path", "adjacency"}`
- `detailMode="inline"`
- `detailKind="records"` only

Do not start with:

- nested custom detail renderers
- tree charts
- pivot-inside-detail
- nested tree-in-tree

That MVP already gives:

- real tree mode
- real master/detail shape
- no modal dependency
- no architecture fork

## Non-Goals

Do not:

- make `detail` share the same expander state as children
- model tree mode as fake pivot rows
- put all tree/detail code directly into `tanstack_adapter.py`
- keep detail forever as only a modal

## Acceptance Criteria

### Tree

- initial tree request renders roots only
- child rows appear only when parent expands
- fast expand/collapse uses session gate and windowing correctly
- branch-paged backend loading is still a future optimization

### Detail

- detail loads lazily per row
- detail does not interfere with child expansion
- inline detail can be opened and closed without resetting main grid state
- drill remains available as raw-record detail

### Architecture

- no new transport callback split
- row metadata is consistent across view modes
- backend responsibilities are separated by mode, not mixed into one giant adapter path

## First Implementation Tasks

1. Add props to React + Python wrapper.
2. Add canonical row metadata helper in runtime/backend.
3. Add `detail` request kind in runtime service.
4. Build inline detail host in frontend.
5. Route current drill payload through inline detail as the first renderer.
6. Add tree request normalization and root/child loading service.
7. Add `useTreeRowModel`.

## Session Handoff Notes

When resuming work:

1. Start from this document.
2. Treat this as the target architecture unless explicitly changed.
3. Prefer phased delivery, not one large rewrite.
4. Keep current pivot behavior stable while adding tree/detail incrementally.

Current implementation notes:

1. Tree adjacency paths now use normalized id chains such as `1|||2|||4`, not display labels. This avoids collisions and fixes nullable numeric parent ids (`1.0`, `nan`) from dataframe execution backends.
2. Tree rows still display the label via `_id`; the path is now an internal stable identity.
3. Detail requests for tree rows should use `rowPath` / `rowKey` based on that normalized path contract.
4. Tree mode now uses branch-filtered backend loading for adjacency trees and prefix-filtered subtree loading for path trees, instead of materializing the full filtered row set for every data request.
