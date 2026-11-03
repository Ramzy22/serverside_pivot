# Cache Coordinator Contract

Date: 2026-04-27

## Purpose

`pivot_engine.cache_coordinator.CacheCoordinator` is the shared contract for cache ownership, invalidation events, and metrics. It does not replace the backing caches; it registers each cache namespace with key dimensions, size/TTL limits, live size getters, invalidators, and counters.

## Namespaces

| Namespace | Owner | Correctness key dimensions |
| --- | --- | --- |
| `runtime.payload` | Runtime payload store | payload token, content type, request id, state epoch |
| `controller.query` | Scalable controller query cache | table, compiled SQL, pivot spec, delta epoch |
| `controller.hierarchy_view` | Controller hierarchy cache | table, spec fingerprint, expanded paths, row window |
| `controller.hierarchy_root_count` | Controller hierarchy cache | table, rows, filters |
| `controller.hierarchy_root_page` | Controller hierarchy cache | table, root count key, sort, row window |
| `controller.hierarchy_grand_total` | Controller hierarchy cache | table, spec fingerprint, filters, measures, custom dimensions |
| `adapter.center_columns` | TanStack adapter viewport cache | table, request structure, column window, cache generation |
| `adapter.pivot_catalog` | TanStack adapter viewport cache | table, columns, filters, custom dimensions, column sort options, cache generation |
| `adapter.response_window` | TanStack adapter viewport cache | request structure, expanded paths, row window, column window, grand total, cache generation |
| `adapter.row_block` | TanStack adapter viewport cache | request structure, expanded paths, block index, column window, cache generation |
| `adapter.grand_total` | TanStack adapter viewport cache | request structure, expanded paths, column window, cache generation |
| `adapter.prefetch_pending` | TanStack adapter viewport cache | response window key |

## Invalidation Events

| Event | Scope | Behavior |
| --- | --- | --- |
| `mutation` | Controller query, hierarchy view, hierarchy grand total; structural mutations also clear root count/page caches | Keeps table-scoped query invalidation while avoiding unrelated cache clears. |
| `data_load` | All registered namespaces on the controller coordinator | Full reload invalidates controller caches and registered adapter viewport caches. |
| `adapter.invalidate_all` | Adapter viewport namespaces | Bumps adapter cache generation and clears window, block, catalog, grand-total, center-column, and prefetch state. |

## Metrics

Each namespace exposes `generation`, `hits`, `misses`, `stores`, `evictions`, `expirations`, `invalidations`, `deletes`, `entries`, and `bytes` where applicable. Use `cache_coordinator.snapshot(include_events=True)` for a diagnostic view with recent invalidation events.
