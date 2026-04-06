# Pivot Profiler

The grid now has an opt-in request profiler that records:

- request queue time on the client
- Dash callback time
- runtime service time
- response-to-commit render time

## Enable

From the browser console:

```js
localStorage.setItem('pivot-profile', '1');
localStorage.setItem('pivot-profile-console', '1'); // optional
location.reload();
```

Or for one session without local storage:

```js
window.__PIVOT_PROFILE__ = true;
window.__PIVOT_PROFILE_CONSOLE__ = true; // optional
```

## Inspect

The profiler writes to `window.__pivotProfiler`.

Examples:

```js
window.__pivotProfiler.latest('pivot-grid');
window.__pivotProfiler.getHistory('pivot-grid');
window.__pivotProfiler.summary('pivot-grid');
window.__pivotProfiler.clear('pivot-grid');
```

## What You Get

Each entry includes:

- `requestId`
- `kind`
- `status`
- `queuedAt`
- `emittedAt`
- `responseReceivedAt`
- `finishedAt`
- `profile.callback`
- `profile.service`
- `profile.adapter`
- `profile.controller`
- `profile.controllerPivot`
- `derived.queueMs`
- `derived.responseMs`
- `derived.renderMs`
- `derived.totalMs`

Interpretation:

- `queueMs`: client delay before the request actually left the browser
- `responseMs`: request/response span after dispatch
- `renderMs`: response received to committed paint
- `profile.callback.totalMs`: Dash callback wall time
- `profile.service.totalMs`: runtime service wall time inside the callback
- `profile.adapter.totalMs`: adapter-side virtual scroll handling
- `profile.controller.totalMs`: hierarchy/controller work inside the adapter
- `profile.controllerPivot.rootQuery.planner.totalMs`: root query planning/execution on paged collapsed-root requests

For wide server-side windows, the most useful nested fields are usually:

- `profile.adapter.columnCatalogMs`
- `profile.adapter.hierarchyViewMs`
- `profile.controller.pageKeyFetchMs`
- `profile.controller.rootQueryMs`
- `profile.controllerPivot.rootQuery.planner.planningMs`
- `profile.controllerPivot.rootQuery.plannerExecution.columnDiscoveryMs`
- `profile.controllerPivot.rootQuery.plannerExecution.pivotQueryMs`

Use `summary()` to see whether the next win is:

- client scheduling
- backend execution
- post-response render work
