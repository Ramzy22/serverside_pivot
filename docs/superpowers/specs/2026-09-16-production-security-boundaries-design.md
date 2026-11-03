# Production Security Boundaries Design

## Purpose

Make every externally reachable Pivot transport fail closed for tenant-scoped
data, bind edit history to the authenticated subject, remove executable Redis
deserialization, and provide a platform-neutral production security baseline.

## Evidence and scope

The disposable certification probe at
`investigation_2026_09_14/security_certification_probe_2026_09_16.py`
currently demonstrates these failures with in-memory data:

- A user who is authorized for a dataset but has no tenant claim receives both
  tenants and can update tenant B.
- A same-tenant attacker who knows a victim's event and browser session values
  can undo the victim's change.
- A shared Flight bearer token executes a pivot without a subject, dataset
  authorization, or row filter.
- `RedisCache.get()` calls `pickle.loads()` for unrecognised bytes.

The scope includes the legacy TanStack endpoint, V1 runtime HTTP transport,
Dash-facing server adapters, Arrow Flight, Redis cache serialization, and
deployment artifacts. It also includes authorization of the currently
viewer-accessible admin-dashboard and expansion-state routes discovered while
running the probe.

## Goals

1. A tenant-scoped table must never be read, exported, mutated, subscribed to,
   or queried through Flight unless the principal supplies valid server-trusted
   row claims.
2. Browser payload fields must not select another principal's edit-history
   session or events.
3. Flight must authenticate a principal per call, apply the same table and row
   security policy as HTTP, and bind result tickets to that principal's scope.
4. Redis cache values must use only non-executable, versioned Arrow IPC or JSON
   codecs; old pickle values become cache misses.
5. Production adoption must have executable checks and operator evidence for
   TLS, headers, CSRF, rate limiting, signing-key rotation, dependency/SAST
   scans, penetration-test exercises, incident response, backups, and release
   verification.

## Non-goals

- The package cannot issue a real TLS certificate, configure an organization's
  backup account, perform an independent penetration test, or operate an
  incident response team. It will fail closed when its required configuration
  is absent, provide deployable proxy configuration, and provide checklists
  and verification tooling that produce evidence during deployment.
- This change does not make an untrusted direct controller/adapter call safe.
  The public HTTP, Dash, and Flight boundaries are the enforced boundaries.

## Design decisions

### 1. Explicit row-scope policy

Every table exposed through an external transport has an explicit
`rowSecurity` declaration in the server-owned dataset policy. There is no
implicit "no claims means all rows" path.

```python
dataset_policy = {
    "sales": {
        "roles": {"viewer", "editor", "admin"},
        "operations": {"read", "export", "mutate", "subscribe"},
        "rowSecurity": {
            "mode": "claims",
            "claims": {"tenant": "tenant"},
        },
    },
    "currency_reference": {
        "roles": {"viewer", "editor", "admin"},
        "operations": {"read"},
        "rowSecurity": {"mode": "global"},
    },
}
```

`claims` maps a trusted principal attribute to the physical table field. The
mapping may contain several fields. In `claims` mode every mapped principal
claim must be present and contain one or more scalar values; a missing, empty,
or nested value raises `RowScopeAuthorizationError` before querying or
mutating. `global` is an intentional, auditable opt-out. A production table
policy without `rowSecurity` is invalid rather than silently global.

`RuntimeDatasetAuthorizer` will construct an immutable `RowScopeGrant` with
the principal subject, table, field predicates, and a stable scope fingerprint.
The same grant is used to:

- replace browser-supplied predicates for protected fields when building a
  `PivotSpec`;
- populate every mutation key predicate and validate insert/upsert field
  values;
- filter row-level CDC/realtime events;
- authorize export and filter-option queries; and
- bind Flight action execution and result tickets.

The existing `apply_rls_to_spec` helper remains an internal conversion point,
but external transports must obtain a `RowScopeGrant` first. It must not treat
an arbitrary non-empty attributes mapping as a policy declaration.

### 2. Server-owned edit-history identity

The runtime context remains the source of identity:

```text
session_id      = "principal:<authenticated subject>"
client_instance = request client-instance for request ordering only
history key     = table + session_id + client_instance
```

The router will overwrite, rather than merge, `session_id` and
`client_instance` in every transaction payload with the values it created from
the authenticated context. The edit service and event store will have
session-aware lookup methods that reject events whose stored `session_key` is
not the current server-computed key. Event records will also retain the owning
subject as defence in depth and reject mismatches.

Consequently, an `eventId`, `sessionId`, or `clientInstance` submitted by a
browser is an opaque hint at most; it can never select another subject's undo,
redo, revert, or replace history.

### 3. Principal-scoped Arrow Flight

Flight uses a host-neutral synchronous `FlightPrincipalResolver` protocol:

```python
class FlightPrincipalResolver(Protocol):
    def resolve(self, headers: Mapping[str, Sequence[str]]) -> User: ...
```

The Flight middleware resolves the principal for every RPC and stores it in the
call middleware. `PivotFlightServer` receives the resolver plus the same
`RuntimeDatasetAuthorizer` used by HTTP. On `pivot` it authorizes the requested
table, builds a `RowScopeGrant`, rewrites the parsed `PivotSpec`, then calls
the controller. `clear_cache` requires an admin principal; unauthenticated
Flight is available only under an explicit local-development switch.

Result cache entries store the table, principal subject, and row-scope
fingerprint alongside the Arrow table. `do_get` resolves the caller again and
returns a ticket only when all three values match. Random ticket knowledge or
a shared service token cannot transfer data between principals.

The old `PIVOT_FLIGHT_AUTH_TOKEN` shared-token constructor path will be
deprecated and rejected in production because it carries no end-user identity.

### 4. Safe Redis codecs and transport configuration

`RedisCache` stores explicit wire formats:

```text
PIVOT_ARROW_V1 + Arrow IPC stream
PIVOT_JSON_V1  + UTF-8 JSON
```

Only Arrow tables and JSON values are accepted. Unsupported objects raise
`TypeError` on write. Unknown, malformed, and legacy pickle values return a
cache miss without deserializing or executing anything. This safely flushes
old pickle entries as they expire or are invalidated.

Production Redis configuration moves to a URL form and requires a `rediss://`
endpoint with authentication and certificate verification. Plain Redis is
permitted only under the explicit local-development profile.

### 5. Platform-neutral production baseline

The package will expose a production settings validator and an installation
function for FastAPI/ASGI hosts. They require, in production:

- a public HTTPS origin and explicit trusted hosts/proxy configuration;
- strict transport headers (HSTS, `nosniff`, frame-ancestors, referrer policy,
  permissions policy, and a configured CSP);
- Origin/CSRF validation for cookie-authenticated unsafe requests;
- a shared rate-limit backend for externally reachable write, export, and
  authentication routes;
- JWT key-ring configuration with a `kid`, active key, previous verification
  keys, issuer, audience, expiry, and algorithm allow-list.

The repository will also provide a reverse-proxy reference configuration. It
terminates TLS, forwards only trusted proxy headers, applies request-size and
rate limits, and rejects cleartext traffic. Hosts that use a cloud load
balancer can translate the same requirements to their provider.

CI will generate a CycloneDX SBOM, run dependency auditing and SAST, run the
security regression suite, and retain reports as build artifacts. A release
check command will validate deployment configuration and probe a supplied
HTTPS endpoint for TLS, headers, redirect behavior, and rate-limit responses.

An operator runbook will contain evidence templates and cadence for
third-party penetration testing, incident response exercises, encrypted backup
restore tests, signing-key rotation, and release/deployment approval. These
items are explicitly required gates, not claims that the library performed
them.

## Error handling and compatibility

- Missing required row claims result in a generic `403`; no table, tenant, or
  row-existence details are disclosed.
- Unauthorized history events use the same generic `403`/not-found semantics
  as other protected resources.
- A pre-existing policy must be migrated by adding `rowSecurity`. This is a
  deliberate security-breaking production change; local development can opt
  into an explicitly documented compatibility profile only.
- Legacy Redis pickle entries are intentionally unreadable after upgrade.
- Existing Flight users must supply a resolver and policy before production
  startup. This is deliberate because a shared token is insufficient authority
  for tenant data.

## Test and evidence requirements

The test suite must include negative end-to-end tests for both legacy and V1
HTTP reads and writes without claims; history session/event forgery between
same-tenant subjects; Flight cross-principal actions and ticket replay;
malicious pickle payloads; malformed row policies; production configuration
startup failures; security headers/CSRF/rate limiting; and key-ring rotation.

The deployment verifier and CI reports are additional evidence. They do not
replace a scheduled independent penetration test or a successful encrypted
backup restore test recorded by the deployment operator.
