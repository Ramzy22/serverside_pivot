# Current Implementation Audit

Date: 2026-04-26  
Scope: current workspace implementation of `DashTanstackPivot`, the runtime transport, server-side pivot execution, export/copy paths, packaging, and test/build posture.

This is a static and targeted implementation audit, not a full runtime profiling report. It includes issues found while fixing the copy/export ordering bug, plus a broader codebase pass across frontend, backend, packaging, and tests.

## Executive Summary

The implementation has a strong direction: a unified Dash runtime transport, a server-side TanStack adapter, Ibis/DuckDB execution, and client-side viewport caching. The main risk is that this direction has been implemented through accumulated feature layers rather than cleanly separated subsystems. The result is several very large modules, duplicated request shaping logic, multiple cache layers with different invalidation rules, and fallback paths that can hide real defects.

Highest priority risks:

- Export/copy still has a separate backend path from the normal runtime service. The ordering bug came from that divergence.
- Large data export/copy is memory-heavy on both server and client because it materializes all rows into strings/base64 before delivery.
- The frontend and backend both have "god modules" that own too many responsibilities, which increases regression risk and makes stale-state bugs more likely.
- Server-side viewport caching and request cancellation are complex and partially duplicated across client, runtime service, adapter, and controller.
- Build/package generation can succeed while docgen emits parser errors, which can leave generated metadata stale.
- Several legacy shims, fallback paths, and duplicated helpers make behavior harder to reason about.

## Fix Progress

Implemented on 2026-04-26:

- Export/copy no longer uses the old direct backend export branch. Export is routed through `PivotRuntimeService` with the same runtime request state, ordering, expansion, and column-window semantics as the visible table.
- Export delivery no longer emits base64 through Dash props. Runtime export writes CSV/TSV to a temporary file, stores a payload reference, and the Flask payload endpoint streams the file with download headers.
- Clipboard writes now have a user-facing error path for hotkeys and context-menu copy actions, with a fallback to `document.execCommand("copy")` where available.
- Adapter-local mutable caches now go through a lock-backed abstraction for lookup/store/invalidation, center-column metadata, and prefetch in-flight keys.
- `npm run build:py` now runs a strict wrapper that fails when Dash/react-docgen reports parser errors instead of silently accepting stale generated metadata.
- Hierarchy row construction, expanded-path normalization, grand-total detection/placement, color-scale stats, and legacy viewport fallback slicing now share `hierarchy_rows.py` instead of separate adapter/controller implementations.
- Server-side runtime stale gating and in-process request cancellation now share `RuntimeRequestCoordinator`; the old controller table-scoped cancellation registry was removed; profiling includes session/client/epoch/window/abort/lane metadata for viewport investigations.

Still open:

- Clipboard copy still has to materialize the selected text in the browser because the Clipboard API accepts text payloads, not a stream.
- Export query execution still materializes the selected row window returned by the adapter before serialization. The base64/Dash-prop and generated-string duplication are removed, but true backend row streaming would require deeper adapter/export-service work.
- The existing docgen parser errors remain and now intentionally fail the Python build step until the underlying component parser issues are fixed.

## Architecture Findings

### ARCH-01: Frontend root component is too large

Evidence:

- `dash_tanstack_pivot/src/lib/components/DashTanstackPivot.react.js` is about 12k lines.
- It owns runtime request normalization, Dash prop sync, state history, viewport request metadata, copy/export, chart pane orchestration, editing, detail, report/tree modes, context menus, persistence, notifications, and large UI sections.

Impact:

- Any change to transport, export, charts, editing, or layout risks stale closures and unrelated regressions.
- Testing tends to assert source strings because behavior is hard to isolate.
- Refactors are expensive because many concerns share local refs and state setters.

Recommendation:

- Extract runtime transport, export/copy orchestration, chart pane state, layout history, and selection/context-menu behavior behind focused hooks or services.
- Keep `DashTanstackPivot.react.js` as orchestration only.

### ARCH-02: Backend service/adapter/controller are god objects

Evidence:

- `runtime/service.py` is about 1.8k lines and handles request gating, cancellation, edit/update/detail/drill/chart/report logic, request construction, profiling, and response shaping.
- `tanstack_adapter.py` is about 3.7k lines and handles TanStack conversion, formula normalization, hierarchy, virtual scroll, edit propagation, caching, fallback behavior, and metadata.
- `scalable_pivot_controller.py` is about 3.8k lines and handles planning, execution, hierarchy materialization, sparse pivots, mutation, export, cache invalidation, and CDC-adjacent concerns.

Impact:

- Boundaries are unclear. For example, hierarchy behavior exists in the adapter, controller, `HierarchyQueryService`, `HierarchicalVirtualScrollManager`, and `tree.py`.
- Bugs can be fixed in one path while a fallback or legacy path keeps old behavior.
- Performance improvements are hard to validate because caches and execution paths are spread across layers.

Recommendation:

- Split backend into explicit services: request builder, hierarchy query service, export service, mutation/edit service, formula service, and cache coordinator.
- Make adapter responsibilities transport-only where possible.

### ARCH-03: Export/copy bypasses the runtime service path

Evidence:

- `runtime/dash_callbacks.py:713` has a dedicated `request_kind == "export"` branch.
- That branch calls `adapter.handle_request(export_context.request)` directly at `runtime/dash_callbacks.py:737`.
- The normal data path builds state and context later in the same callback and then routes through `PivotRuntimeService`.

Impact:

- Export/copy can diverge from visible table behavior. The recently fixed copy/export ordering bug happened because export built a separate request without the same effective sorting/state metadata.
- Any future runtime behavior, such as request gates, stale checks, profiling, payload externalization, or column visibility logic, must be duplicated or export will drift again.

Recommendation:

- Move export into a dedicated service that shares the exact same request builder as the runtime data path.
- Treat export as a runtime intent with a different serializer, not as a separate callback branch.

### ARCH-04: Package import shims hide packaging complexity

Evidence:

- `pivot_engine/__init__.py` is a repo-local package shim that rewrites `__path__`.
- `dash_tanstack_pivot/pivot_engine/__init__.py:15` rewrites `__path__`.
- `dash_tanstack_pivot/pivot_engine/__init__.py:19-20` opens and `exec`s the inner package `__init__.py`.

Impact:

- Static analysis, packaging tools, IDEs, and import-time security assumptions are weaker.
- Local test imports may pass while installed package imports behave differently.
- `exec` complicates auditing and can mask path errors.

Recommendation:

- Collapse to one canonical package layout.
- Remove `exec` bootstrap behavior and rely on normal package discovery.

### ARCH-05: Hierarchy implementation is fragmented

Evidence:

- Hierarchy logic appears in `hierarchical_scroll_manager.py`, `hierarchy_query_service.py`, `scalable_pivot_controller.py`, `tanstack_adapter.py`, and `tree.py`.
- `scalable_pivot_controller.py:203` wraps `HierarchyQueryService(LegacyHierarchyAdapter(self))`.
- `tanstack_adapter.py:3674-3724` falls back from virtual scroll to hierarchical load.

Impact:

- Different paths can disagree on row order, expanded path filtering, grand total placement, and count ownership.
- Legacy fallback paths can preserve behavior that no longer matches the primary runtime contract.

Recommendation:

- Make `HierarchyQueryService` the single owner of hierarchy execution semantics.
- Keep compatibility adapters thin and temporary, with tests proving equivalent ordering/count/total behavior.

### ARCH-06: Charting is lazy-loaded but still tightly coupled and large

Evidence:

- `DashTanstackPivot.react.js:203-215` lazy-loads `PivotCharts`.
- `PivotCharts.js` is about 5.5k lines.
- Generated artifacts include `559.dash_tanstack_pivot.min.js` at about 1.8 MB and `dash_tanstack_pivot.min.js` at about 1.2 MB.
- `package.json` includes `echarts`, `echarts-for-react`, and `echarts-gl`.

Impact:

- Lazy loading prevents initial inclusion of the chart UI, but the async chart chunk remains large.
- Main component still owns significant chart request and pane state.

Recommendation:

- Keep chart state and server request construction inside a chart subsystem.
- Consider splitting heavy chart types or `echarts-gl` behind feature-level dynamic imports.

## Performance And Scalability Findings

### PERF-01: Export/copy materializes very large datasets in memory

Evidence:

- Export request construction uses `pageSize: 10_000_000` in `runtime/dash_callbacks.py:241`.
- Export serializes through `io.StringIO()` at `runtime/dash_callbacks.py:830`.
- The result is base64 encoded at `runtime/dash_callbacks.py:837`.
- The frontend decodes the full payload with `atob` at `DashTanstackPivot.react.js:10601`.
- Large copy then splits the entire text into lines at `DashTanstackPivot.react.js:10602`.

Impact:

- Server memory grows with full exported row count and column count.
- Base64 adds about 33 percent overhead.
- Client memory doubles or triples during `atob`, `Uint8Array`, `Blob`, and line-splitting operations.
- Large copy/export can freeze the Dash worker or browser tab.

Recommendation:

- Implement streaming export with chunks written directly to a response body or temporary file.
- Avoid base64 for large export payloads.
- Keep copy bounded by user-selected chunks and warn when clipboard payloads exceed a practical size.

### PERF-02: Bundle size has no enforced budget

Evidence:

- Current generated JS artifacts are about 1.2 MB for the main bundle and 1.8 MB for the chart async chunk.
- `webpack.config.js` uses split chunks, but there is no explicit performance budget or CI failure threshold.

Impact:

- Bundle growth can go unnoticed.
- Large async chart chunk still causes delayed first chart interaction.

Recommendation:

- Add a CI bundle-size report and threshold.
- Track main bundle and async chart chunk separately.

### PERF-03: Multiple cache layers have independent invalidation rules

Evidence:

- `RuntimePayloadStore` has an in-process TTL/LRU store in `runtime/payload_store.py`.
- `TanStackPivotAdapter` keeps center column, pivot column catalog, response window, row block, and grand total caches at `tanstack_adapter.py:221-227`.
- `ScalablePivotController` keeps hierarchy view/root/grand-total caches at `scalable_pivot_controller.py:185-196`.
- Client row cache logic exists in `useServerSideRowModel.js`.
- Mutation invalidation in `scalable_pivot_controller.py:4091-4125` clears some controller caches and query caches, but adapter cache invalidation is handled separately.

Impact:

- It is hard to know which cache owns correctness for a given request.
- Mutation, sorting, expansion, custom dimensions, or value config changes can invalidate one layer but leave another stale if the cache key misses a dimension.
- Memory usage is distributed and hard to cap globally.

Recommendation:

- Introduce a cache coordinator with explicit namespaces, key dimensions, invalidation events, size limits, and metrics.
- Add tests for cache invalidation after sort, edit, expand/collapse, filter, and value config changes.

### PERF-04: Per-table execution locking can limit concurrency

Evidence:

- `scalable_pivot_controller.py:127-131` initializes planning/execution/hierarchy locks.
- `scalable_pivot_controller.py:215-222` creates one execution lock per table.
- `scalable_pivot_controller.py:2176` runs execution under `_execution_lock_for_table`.

Impact:

- This protects shared Ibis/DuckDB state, but serializes requests for the same table.
- Viewport, chart, export, and edit refreshes may queue behind each other for heavy tables.
- Existing capability documentation claims "no global lock", which is only partly true because same-table work is still serialized.

Recommendation:

- Document the concurrency model explicitly.
- Measure lock wait time in runtime profiling.
- Separate read-only queries from mutation or metadata-sensitive operations if backend safety permits.

### PERF-05: Viewport request lifecycle is complex and expensive to reason about

Status: fixed for the current runtime boundary on 2026-04-27. Broader client/controller cache consolidation remains intentionally out of scope.

Evidence:

- `useServerSideRowModel.js` tracks `stateEpoch`, `abortGeneration`, inflight windows, duplicate suppression, stale windows, orphaned blocks, column-window urgency, and delayed flushes.
- `RuntimeRequestCoordinator` now owns server-side stale gating plus active request cancellation per session/client/lane.
- `ScalablePivotController.run_pivot_async` no longer carries a table-scoped `_running_queries` registry that can cancel unrelated sessions using the same table.
- `AdapterViewportCache` now owns adapter-local viewport response, row-block, grand-total, pivot-catalog, center-column, prefetch, lock, and generation state.
- `tanstack_adapter.py:716-737` schedules background prefetch tasks.

Impact:

- The system has several independent stale-response protections. That is necessary for correctness, but bugs become hard to reproduce.
- A small mismatch between client epochs, server cancellation, adapter cache keys, or prefetch can cause blank rows, duplicate requests, or stale data.

Recommendation:

- Keep `RUNTIME_REQUEST_LIFECYCLE.md` as the contract for request identity, lifecycle lanes, stale gates, cancellation, cache ownership, and profiling fields.
- Keep request IDs, epoch, abort generation, client cache key, adapter response cache key, lifecycle lane, and cancellation outcome in profiling logs when profiling is enabled.

Implemented:

- Added `runtime/request_coordinator.py` as the single server-side owner for request registration, response freshness checks, and in-process supersession cancellation.
- `PivotRuntimeService` now delegates lifecycle checks to the coordinator instead of carrying its own active-task maps.
- Removed controller-level table-scoped cancellation so request supersession is no longer split between runtime and controller.
- Added `adapter_viewport_cache.py` so adapter viewport cache invalidation, TTL lookup/storage, generation bumping, and prefetch de-duplication have one owner.
- Runtime profiling now emits `sessionId`, `clientInstance`, `stateEpoch`, `windowSeq`, `abortGeneration`, and `lifecycleLane`.
- Runtime profiling now also emits client `cacheKey`, adapter `responseCacheKey`, adapter `cacheGeneration`, and normalized `cancellationOutcome`.
- Added `RUNTIME_REQUEST_LIFECYCLE.md` as the lifecycle contract and tied tests to the required profiling fields.
- Added coordinator tests for stale registration, superseded task cancellation, and client-instance isolation; added cache-owner tests for adapter cache invalidation.

Remaining:

- Client row-cache scheduling and controller hierarchy-result caches are still separate by design. They now have clearer server lifecycle boundaries, but a full end-to-end cache-coordinator refactor would require a broader client hook and hierarchy-controller cache redesign.

### PERF-06: Logging uses raw `print` across backend hot paths

Evidence:

- Many backend files use `print(...)`, including `controller.py`, `hierarchical_scroll_manager.py`, CDC modules, backends, `scalable_pivot_controller.py`, `tanstack_adapter.py`, and `runtime/service.py`.
- Frontend has unconditional or broad console calls, including `DashTanstackPivot.react.js:5163`.

Impact:

- Production logs become noisy and hard to filter.
- Printing from hot paths can add latency under heavy request volume.
- Errors can be swallowed into fallback behavior without structured context.

Recommendation:

- Replace backend `print` with module loggers and level-controlled structured fields.
- Gate frontend console output behind existing debug flags.

## Implementation Bugs And Risks

### BUG-01: Copy/export ordering regression exposed duplicate request construction

Status: fixed in the current workspace, but the underlying architecture risk remains.

Evidence:

- The export path had independent sorting and effective state handling from visible table requests.
- The fix added shared sorting normalization and export-context state override handling, but export is still a separate branch in `dash_callbacks.py`.

Impact:

- Future table-visible semantics can regress in copy/export unless the request builder is shared.

Recommendation:

- Add a browser-level copy/export ordering test.
- Move export request construction behind the same runtime request builder as viewport data.

### BUG-02: Export headers depend on first returned row

Evidence:

- `runtime/dash_callbacks.py:772-779` builds `field_order` from the first row.
- Empty exports set `field_order = []`.
- `colIds` can restore requested order, but only for requested visible column IDs and only after mapping to available fields.

Impact:

- Empty exports can omit headers.
- Sparse row shapes can omit columns that exist later in the result.
- Metadata-only columns or dynamically computed fields can be lost if not present in the first row.

Recommendation:

- Build export schema from `response.columns`, `col_schema`, visible leaf column IDs, and value config metadata instead of row keys.
- Add tests for empty export, sparse row export, hidden columns, reordered columns, and hierarchy-only export.

### BUG-03: Clipboard API has no error handling or fallback

Evidence:

- `DashTanstackPivot.react.js:7125-7126` calls `navigator.clipboard.writeText(text)` directly.

Impact:

- Clipboard writes can fail on non-secure origins, permission denial, iframe restrictions, or older browsers.
- UI can show success even when copy fails.

Recommendation:

- Return and await the clipboard promise.
- Show failure notification on rejection.
- Provide a fallback textarea copy path where supported.

### BUG-04: Adapter local caches are not visibly synchronized

Evidence:

- `tanstack_adapter.py:221-227` stores several mutable dict/`OrderedDict` caches.
- `tanstack_adapter.py:716-737` schedules background prefetch tasks that can touch the same caches.
- Runtime requests can be processed concurrently by Dash callbacks.

Impact:

- Concurrent cache mutation can create race conditions, especially when prefetch overlaps visible viewport requests or edits invalidate caches.
- Python dict operations are individually safe under the GIL, but compound cache get/store/evict sequences are not a correctness guarantee across tasks or threads.

Recommendation:

- Add an adapter cache lock or move cache access into an async-safe cache abstraction.
- Add stress tests for simultaneous viewport requests, prefetch, and edit invalidation.

### BUG-05: Fallback paths can hide primary path failures

Evidence:

- `tanstack_adapter.py:3674` logs "Virtual scroll failed" and falls back to hierarchical load.
- `tanstack_adapter.py:3762` reports a `hierarchical_fallback` profile path.
- `scalable_pivot_controller.py:2216` prints an error for legacy query format rather than surfacing a typed failure.

Impact:

- User-visible data may still render but with different ordering, paging, or performance.
- Regressions can be masked until a larger dataset makes the fallback path unacceptable.

Recommendation:

- Treat fallback as an explicit degraded mode in the response profile.
- Add alerting or test assertions when primary paths unexpectedly fall back.

### BUG-06: Development auth fallback is safe-tested but still operationally risky

Evidence:

- `security.py:35` defines `DEFAULT_JWT_SECRET_KEY = "dev-jwt-secret"`.
- `security.py:59-65` supports development auth fallback.
- `tests/test_security_config.py` verifies production/staging rejection behavior.

Impact:

- The tests are useful, but deployment safety still depends on environment configuration.

Recommendation:

- Keep the tests.
- Add deployment documentation and startup logging that states the active auth mode.

### BUG-07: Build can pass while generated component metadata is incomplete

Evidence:

- `package.json:9-10` runs `webpack` and Dash component generation.
- Recent `npm run build` completed successfully while Dash docgen emitted parser errors for modern JS constructs.
- Generated files such as `metadata.json`, Python component stubs, and minified JS are committed.

Impact:

- CI can pass while Python component metadata is stale or incomplete.
- Generated files can drift from source if build warnings are ignored.

Recommendation:

- Make docgen parser errors fail CI.
- Add a clean-tree check after build to detect stale generated assets.

### BUG-08: Source comments contain mojibake artifacts

Evidence:

- Comments in files such as `tanstack_adapter.py` and `dash_callbacks.py` contain corrupted arrow mojibake sequences.

Impact:

- Not a runtime defect, but it signals encoding drift and reduces maintainability.

Recommendation:

- Normalize comments to ASCII or valid UTF-8 consistently.

## Redundancy And Maintainability Findings

### RED-01: Formula reference normalization is duplicated

Evidence:

- Backend formula reference normalization exists in `tanstack_adapter.py:34` and `runtime/service.py:874`.
- Frontend formula reference helpers exist in `SidebarPanel.js` and related normalization utilities.

Impact:

- Frontend/backend formula semantics can drift.
- Bugs in formula IDs or labels may be fixed in one layer only.

Recommendation:

- Define one formula reference contract and test vectors.
- Share generated test fixtures across Python and JS tests.

### RED-02: Export serialization exists in both frontend and backend

Evidence:

- Backend export serialization is in `runtime/dash_callbacks.py:713-842`.
- Frontend client-side export utilities live in `src/lib/utils/exportUtils.js`.
- Frontend export/copy response handling lives in `DashTanstackPivot.react.js:10587-10635`.

Impact:

- Client-side and server-side export can differ in header formatting, row order, column order, null handling, formula display, and merged-header behavior.

Recommendation:

- Define a single export schema contract.
- Keep serializers separate only where necessary, and test both against the same fixtures.

### RED-03: State override extraction is repeated

Evidence:

- `_state_override_value(...)` is used in export context, filter options, and the generic runtime path in `runtime/dash_callbacks.py`.
- Similar effective state calculations appear around `dash_callbacks.py:184-203`, `597-602`, and `884-936`.

Impact:

- New state fields can be added to one path and missed in another.
- This is exactly the class of drift that broke copy/export ordering.

Recommendation:

- Introduce one `RuntimeEffectiveState` builder and use it for all request kinds.

### RED-04: Hierarchy and virtual scroll fallback implementations overlap

Status: partially fixed on 2026-04-27.

Evidence:

- `hierarchical_scroll_manager.py`, `HierarchyQueryService`, `ScalablePivotController`, and `TanStackPivotAdapter` all participate in hierarchy execution.
- Tests include many regressions for duplicate rows, stale paths, and grand total placement.

Impact:

- Fixes require broad awareness of all paths.
- Fallback behavior can become a second implementation with different guarantees.

Recommendation:

- Consolidate hierarchy row construction, visible filtering, total placement, and path normalization.

Implemented:

- Added shared hierarchy row policy in `pivot_engine/hierarchy_rows.py`.
- `ScalablePivotController` now delegates hierarchy flattening, path normalization, grand-total detection, and color-scale stats to the shared policy.
- `TanStackPivotAdapter` now delegates hierarchy finalization and legacy fallback viewport slicing to the shared policy.

Remaining:

- `HierarchicalVirtualScrollManager` still exists as a legacy compatibility path. The active adapter prefers `run_hierarchy_view`; the manager should be formally deprecated or moved behind a separate legacy adapter in a later cleanup.

### RED-05: Legacy and auxiliary server paths remain in the package

Evidence:

- Files such as `complete_rest_api.py`, `flight_server.py`, streaming modules, CDC modules, and old controller paths remain alongside the Dash runtime.

Impact:

- If these are active public APIs, they need parity tests.
- If they are not active, they add audit and maintenance surface.

Recommendation:

- Mark each path as active, deprecated, or experimental.
- Remove or quarantine unused legacy modules from the primary package surface.

### RED-06: Tests rely heavily on source-string assertions

Evidence:

- `tests/test_frontend_contract.py` reads JS source files and asserts literal snippets in many places.

Impact:

- These tests can catch accidental removals, but they also lock implementation details and can pass without proving behavior.
- They make refactors more expensive.

Recommendation:

- Keep a small number of structural source checks.
- Move high-value behavior into JS unit tests or browser-level integration tests.

## Documentation And Packaging Drift

### DOC-01: Existing documentation disagrees about implemented features

Evidence:

- `KNOWN_ISSUES.md` says master/detail grids are not yet implemented.
- `docs/master-detail-tree-architecture.md` says the MVP is implemented.
- `CAPABILITIES.md` says concurrent execution uses "no global lock", while code uses same-table execution locks.

Impact:

- Users and maintainers cannot trust documentation for current capability or risk.

Recommendation:

- Add one generated or manually curated capability matrix with status: shipped, partial, experimental, deprecated.
- Update `KNOWN_ISSUES.md` after each implementation milestone.

### DOC-02: Generated package files duplicate source package metadata

Evidence:

- There are `package.json` and `package-lock.json` files under both `dash_tanstack_pivot/` and `dash_tanstack_pivot/dash_tanstack_pivot/`.
- Generated Python component stubs and JS bundles are committed.

Impact:

- It is easy to edit or review generated copies accidentally.
- Build drift creates noisy diffs.

Recommendation:

- Document which files are source of truth.
- Add a clean build verification step that fails when generated files are stale.

## Recommended Remediation Plan

### P0: Correctness and reliability

- Move export/copy to a shared runtime request builder and add end-to-end ordering tests.
- Replace large base64 export with streaming or payload-store-backed downloads.
- Add clipboard error handling.
- Add locks or a cache abstraction around adapter mutable caches.
- Make docgen parser errors fail CI.

### P1: Architectural cleanup

- Extract runtime transport and export orchestration from `DashTanstackPivot.react.js`.
- Introduce `RuntimeEffectiveState` in Python and use it for data, filter options, export, chart, and detail requests.
- Consolidate hierarchy semantics behind `HierarchyQueryService`.
- Replace backend `print` calls with structured logging.

### P2: Maintainability and performance hygiene

- Add bundle-size budgets and report main/chart chunks separately.
- Create a cache ownership document and metrics for hit/miss/eviction/lock wait.
- Remove package shims and `exec` imports.
- Update documentation to reflect current shipped, partial, experimental, and deprecated features.
- Reduce source-string tests in favor of behavior tests where practical.

## Immediate Test Gaps To Add

- Browser copy test: sorted visible pivot order equals copied TSV row order.
- Browser export test: sorted visible pivot order equals downloaded CSV order.
- Export schema test: empty result still includes visible headers.
- Export schema test: sparse first row does not drop later columns.
- Large export test: export does not base64-inline payloads above a configured threshold.
- Concurrency test: viewport request, prefetch, and edit invalidation cannot serve stale cached rows.
- Build test: `npm run build` fails on docgen parser errors and leaves no generated diff.
