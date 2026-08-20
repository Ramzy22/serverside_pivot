# Capability Architecture

This document is the dependency contract for the capability refactor that starts
from commit `bb2c6eb`.

## Compatibility boundary

The following remain stable throughout the refactor:

- Dash component name, props, defaults, metadata, and generated Python API.
- `TanStackPivotAdapter`, `TanStackRequest`, `TanStackResponse`,
  `TanStackOperation`, and `create_tanstack_adapter` import locations.
- `ScalablePivotController` and `PivotRuntimeService` constructors and public
  methods.
- Runtime response envelopes, row/path identities, profiler phase names, and
  stale/cancellation behavior.

Compatibility methods may delegate to new capability owners, but must not keep
a second copy of mutable state or business logic.

## Dependency direction

Backend dependencies flow in one direction:

```text
Dash callback transport
  -> runtime lifecycle and router
    -> request handlers
      -> TanStack transport facade
        -> query / hierarchy / editing services
          -> planner / backend / cache storage
```

Frontend dependencies flow in one direction:

```text
Dash metadata bridge
  -> TypeScript composition root
    -> capability controllers
      -> pure models, reducers, selectors, and browser adapters
```

Lower layers never import runtime or UI modules. Capability implementations do
not reach into another capability's mutable cache or state.

## Capability ownership

| Capability | Owns | Does not own |
| --- | --- | --- |
| Runtime lifecycle | request registration, lanes, freshness, cancellation, timeout, circuit breaker, request profile | query construction, export rendering |
| Runtime routing | command classification and handler selection | capability algorithms |
| View/layout | fields, filters, sorting, expansion, layout, totals, pinning, persistence reconciliation | network scheduling |
| Selection/clipboard | ranges, keyboard state, clipboard documents, paste/fill commands | edit history, browser downloads |
| Editing | sessions, optimistic values, propagation, history, inverse patches | transport envelopes, cache storage |
| Export | export document and format writers | React state, query planning |
| Chart workspace | pane layout, lock/live data, chart models, title/legend, chart export | pivot request lifecycle |
| Hierarchy execution | path identity, expansion, hierarchy strategies, flattening, totals layout | frontend request parsing |
| Query planning | normalized logical plans and physical strategy selection | runtime freshness, UI state |
| Cache policy | namespaces, key builders, TTL/capacity policy, invalidation events | another capability's cache container |

## Enforcement

- Public compatibility facades contain delegation and construction only.
- Python architecture tests reject upward imports and cycles.
- TypeScript uses strict checking and the React hooks dependency lint rule.
- Authored production modules warn above 800 lines and fail above 1,500 lines;
  generated assets and declarative schemas are excluded.
- Existing private helpers used by diagnostics or tests receive explicit
  forwarding shims and are tracked for later removal.

