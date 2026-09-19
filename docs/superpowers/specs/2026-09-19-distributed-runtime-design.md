# Distributed Runtime Support Design

## Status

Approved design scope: distributed response delivery, request ordering, and
cache invalidation. Distributed editing is deliberately **not** part of the
next release.

## Goal

Any authenticated runtime request may be served by any application worker.
Payload retrieval must remain available after the producing worker exits, and
workers must not return locally cached data after a mutation or CDC event has
invalidated it.

## Target topology

The complete distributed architecture uses two external services:

- Redis, using authenticated TLS in production, is the authority for
  short-lived payload bytes, payload metadata, request-ordering state,
  cache-generation counters, and invalidation events.
- PostgreSQL is reserved as the durable authority for a future distributed
  edit-session/event subsystem. It is not a requirement for the next release
  while editing is disabled.

The next release requires Redis only. PostgreSQL becomes a required production
dependency when distributed editing is released.

Application memory is an L1 optimization only. It must never be required to
retrieve a payload or establish the current request/cache generation.

## Release scope

### Included

- A shared Redis payload store with opaque, high-entropy tokens, byte-safe
  storage, per-entry TTL, and atomic expiry.
- Server-owned payload ownership metadata (principal subject, tenant scope,
  session binding, and content type). Payload download validates this metadata
  on every worker before returning bytes.
- Redis-backed request ordering using atomic Lua operations and expiring
  session keys; the existing `SessionRequestGate` Redis mode becomes a
  production-supported path.
- Redis monotonic generation counters and Pub/Sub invalidation. A worker that
  misses a Pub/Sub notification compares the generation before using an L1
  entry and therefore still evicts stale state.
- Coordinated expiry/cleanup through Redis TTL rather than per-worker LRU
  eviction.
- Configuration validation: multi-worker mode fails fast without a secure
  Redis URL and namespace prefix.

### Explicitly excluded

- Distributed edit sessions, undo/redo, overlays, and edit conflict handling.
- PostgreSQL edit-event tables and their retention worker.

When distributed mode is enabled, edit transaction and history commands must
be rejected with a stable `editing_not_available_in_distributed_mode` error.
This prevents the existing in-memory edit stores from presenting unreliable
multi-worker behavior. A future edit release will add PostgreSQL-backed
session/event records, optimistic `session_version` checks, and a configurable
active undo window while retaining audit events according to policy.

## Data flow

1. A worker builds a runtime response and serializes the payload once.
2. It writes bytes plus JSON metadata to a Redis key under a deployment
   namespace, with one TTL applied atomically.
3. The runtime response contains only the opaque token/reference.
4. A later download request can reach any worker. That worker reads the shared
   entry, validates the authenticated caller against stored binding metadata,
   then streams the stored bytes with the recorded content type.
5. Mutations/CDC increment the table generation atomically and publish a small
   invalidation event. Every worker checks the generation on L1 reads, making
   missed broadcasts safe.

Payload bytes are never encoded using pickle. Redis values use explicit
versioned byte/JSON formats only.

## Failure handling

- Redis unavailable at startup in distributed mode: fail closed; do not fall
  back to process-local storage.
- Redis unavailable during a request: return a generic retryable runtime
  failure without backend exception text.
- Missing or expired token: return the existing not-found/expired response;
  never re-run an unauthenticated request to reconstruct it.
- Restart: payloads, gate state, and generations remain until their configured
  Redis TTL. L1 caches repopulate safely.
- Redis restart/flush: short-lived payloads and request ordering are lost by
  design; downloads receive an expired response. This release does not claim
  restart recovery for edit history because editing is unavailable in this
  topology.

## Configuration contract

`PIVOT_DISTRIBUTED_RUNTIME=1` enables the mode. It requires:

- `PIVOT_RUNTIME_REDIS_URL=rediss://user:password@host:port/db`
- certificate verification enabled
- `PIVOT_RUNTIME_REDIS_PREFIX`, unique per deployment/environment
- a positive payload TTL and bounded payload size

The existing `PIVOT_SESSION_REDIS_URL` will be consolidated into the runtime
Redis configuration so payloads, session ordering, and invalidation cannot
accidentally target different stores.

## Verification and acceptance criteria

- Two independently constructed workers sharing Redis can create and retrieve
  the same payload token.
- Retrieval remains valid after the originating worker instance is discarded.
- A caller from another tenant, user, or session cannot retrieve a bound
  payload token.
- Expiry is observable consistently from either worker.
- Competing request sequence registration produces one deterministic winner.
- An invalidation emitted by worker A prevents worker B from serving an L1
  entry generated before that invalidation, including when B missed Pub/Sub.
- Distributed mode rejects edit, undo, redo, and history operations before
  touching their in-memory stores.
- Unit tests use a Redis-compatible fake; integration tests run against a real
  ephemeral Redis service and route requests across separate runtime instances.

## Future distributed editing phase

PostgreSQL will contain immutable edit events and session snapshots keyed by
table, tenant, owner, session, and client instance. Commands will use expected
session versions and transactionally append events; conflicts return the
current version without overwriting. Active undo state expires on a configured
window, while audit events follow the retention policy selected by operators.
