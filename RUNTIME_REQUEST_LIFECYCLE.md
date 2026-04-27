# Runtime Request Lifecycle

This is the contract for server-side viewport, structural, chart, and detail requests.

## Identity

Every profiled runtime request should carry these identifiers end to end:

- `requestId`: unique browser-generated request id for correlating client queue, Dash callback, runtime service, and adapter profile entries.
- `sessionId`: browser tab/session id.
- `clientInstance`: component instance id, so two grids in one session do not supersede each other.
- `stateEpoch`: structural data/view epoch. Field/filter/grouping changes advance it.
- `windowSeq`: viewport request sequence inside the epoch.
- `abortGeneration`: hard-reset generation. Older generations are rejected.
- `cacheKey`: client row-cache key for the structural view that emitted the request.
- `adapter.responseCacheKey`: adapter-local response-window cache key for the exact row/column/expansion window.

## Lanes

`RuntimeRequestCoordinator.active_request_key()` maps requests to server cancellation lanes:

- `data`: viewport and structural data refreshes.
- `chart`: chart requests.
- `None`: export, drill, update, and other operations that must not be superseded as viewport work.

The key is `(sessionId, clientInstance, lifecycleLane)`. Requests from a different component instance or lane must not cancel each other.

## Registration And Freshness

`RuntimeRequestCoordinator.register_request()` is the first server gate for non-export requests. It delegates to `SessionRequestGate` and rejects requests that are already older than the current `(stateEpoch, abortGeneration, windowSeq)` for the same session/client/intent.

After backend execution, `RuntimeRequestCoordinator.response_is_current()` is checked again. A response that was current when started can still become stale if a newer epoch/window/generation was registered while it was running.

## In-Process Cancellation

Before data execution, `PivotRuntimeService` calls `replace_active_request_task()`. A newer task in the same `(sessionId, clientInstance, lane)` cancels the older in-process task. If the older task receives `CancelledError` and `consume_superseded_cancel()` returns true, the service returns `status="stale"` instead of surfacing cancellation as an error.

## Cache Ownership

Client row-block scheduling remains in `useServerSideRowModel.js`; adapter response-window, row-block, grand-total, pivot-catalog, center-column, prefetch, lock, and generation state are owned by `AdapterViewportCache`.

The runtime service is the boundary between those caches. It must profile both the client `cacheKey` and the adapter `responseCacheKey` so stale-row or duplicate-request bugs can be traced across the boundary.

## Profiling Contract

When profiling is enabled, `profile.request` must include:

- `requestId`
- `sessionId`
- `clientInstance`
- `stateEpoch`
- `windowSeq`
- `abortGeneration`
- `cacheKey`
- `lifecycleLane`
- `cancellationOutcome`

`cancellationOutcome` values:

- `not_cancelled`: request was not rejected or superseded by the runtime lifecycle.
- `stale_registration_rejected`: the server gate rejected the request before backend execution.
- `superseded_cancelled`: a newer same-lane request cancelled this task in process.
- `stale_response_dropped`: backend work completed, but the response failed the final freshness check.

When adapter profiling is present, `profile.adapter.responseCacheKey` and `profile.adapter.cacheGeneration` must also be present.
