# Production Security Boundaries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every confirmed cross-tenant and unsafe-deserialization path, then make production adoption objectively verifiable across Dash, React, Streamlit, HTTP, and Arrow Flight.

**Architecture:** Server-owned table policies produce immutable row-scope grants. HTTP, legacy routes, Dash adapters, mutations, realtime, and Flight consume the same grant rather than trusting browser fields or arbitrary principal attributes. Edit-history identity is derived from the authenticated context, Redis uses framed non-executable codecs, and production middleware/configuration plus CI/deployment artifacts supply operational controls.

**Tech Stack:** Python 3.10+, FastAPI/Starlette, Flask/Dash integration, PyArrow Flight, Redis, python-jose, pytest, GitHub Actions, Nginx reference configuration.

**Spec:** `docs/superpowers/specs/2026-09-16-production-security-boundaries-design.md`

## Global Constraints

- Every public read, write, export, subscribe, and Flight action is default-deny without a server-owned table policy.
- Every tenant-scoped table declares `rowSecurity`; only an explicit `mode: global` can omit a row filter.
- Missing, empty, nested, or malformed required claims fail with a generic 403 before a query or mutation reaches the controller.
- Client `sessionId`, `session_id`, `clientInstance`, and `client_instance` never select an edit-history owner.
- Production never accepts shared Flight bearer tokens, plain Redis, executable cache codecs, JWTs without `exp`/`iss`/`aud`/`kid`, or unconfigured security middleware.
- Do not claim TLS issuance, penetration testing, backup restoration, or incident response occurred; provide configuration gates, probes, and an operator evidence workflow instead.
- Preserve the existing Dash, React, Streamlit, legacy TanStack, and V1 response shapes except where a generic authorization failure is required.
- The shared worktree contains unrelated changes. Stage or commit only files named in a completed task; do not reset, clean, or reformat unrelated work.

---

### Task 1: Immutable explicit row-scope policy

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dataset_authorization.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/mutation_authorization.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/security.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/__init__.py`
- Modify: `tests/test_runtime_dataset_authorization.py`
- Modify: `tests/test_runtime_http_transport.py`
- Modify: `tests/test_security_config.py`

**Interfaces:**
- Produces `RowSecurityPolicy.from_value(value) -> RowSecurityPolicy` and `RowScopeGrant.from_principal(policy, table, principal) -> RowScopeGrant`.
- Produces `RuntimeDatasetAuthorizer.authorize(...) -> RowScopeGrant`; callers that only need permission may ignore the returned immutable grant.
- Produces `RowScopeGrant.apply_to_filters(filters) -> dict`, `RowScopeGrant.apply_to_spec(spec) -> PivotSpec`, and `RowScopeGrant.scope_fingerprint: str`.
- Changes `RuntimeMutationAuthorizer.authorize(..., row_scope: RowScopeGrant) -> MutationGrant` so `MutationGrant.row_constraints` comes only from policy-declared scope fields.
- `apply_rls_to_spec(spec, user, row_scope=None)` accepts the explicit grant. Public hosts pass the grant; a direct trusted in-process caller remains responsible for its own authorization.

- [ ] **Step 1: Write focused failing policy tests**

Add these tests before modifying production code:

```python
def test_claim_scoped_dataset_rejects_a_principal_missing_the_required_claim():
    authorizer = RuntimeDatasetAuthorizer({
        "sales": {"roles": {"viewer"}, "operations": {"read"},
                  "rowSecurity": {"mode": "claims", "claims": {"tenant": "tenant"}}}
    })
    with pytest.raises(RowScopeAuthorizationError):
        authorizer.authorize(principal=User(id="u", username="u", role="viewer", attributes={}),
                             table="sales", operation="read")


def test_claim_scoped_dataset_replaces_a_forged_filter_with_the_server_grant():
    grant = RuntimeDatasetAuthorizer({
        "sales": {"roles": {"viewer"}, "operations": {"read"},
                  "rowSecurity": {"mode": "claims", "claims": {"tenant": "tenant"}}}
    }).authorize(principal=User(id="u", username="u", role="viewer", attributes={"tenant": "A"}),
                table="sales", operation="read")
    assert grant.apply_to_filters({"tenant": {"type": "eq", "value": "B"}}) == {
        "tenant": {"type": "eq", "value": "A"}
    }
```

Also add legacy and V1 integration cases with `A` and `B` data where a viewer/editor with `attributes={}` receives 403 and neither observes nor changes B. Add malformed-policy tests for omitted `rowSecurity`, an empty claims map, duplicate destination fields, and an explicit `global` policy.

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```powershell
pytest -q tests/test_runtime_dataset_authorization.py tests/test_runtime_http_transport.py tests/test_security_config.py
```

Expected: tests for `RowScopeAuthorizationError`, `rowSecurity`, and the no-claim HTTP paths fail because neither grant type nor required-claim enforcement exists.

- [ ] **Step 3: Implement policy parsing and grant construction**

In `dataset_authorization.py`, add frozen dataclasses:

```python
@dataclass(frozen=True)
class RowSecurityPolicy:
    mode: str
    claim_to_field: Mapping[str, str]


@dataclass(frozen=True)
class RowScopeGrant:
    table: str
    subject: str
    field_values: Mapping[str, tuple[Any, ...]]
    scope_fingerprint: str
```

Require exactly `mode="claims"` with a non-empty one-to-one scalar claim map or `mode="global"` with no claim map. Normalise strings, reject ambiguous fields, and construct an immutable mapping whose values are non-empty scalar tuples. `RuntimeDatasetAuthorizer.authorize` keeps its role/operation checks and returns the corresponding grant.

In `mutation_authorization.py`, remove the direct conversion of all `principal.attributes`. Require a `row_scope` from the dataset boundary and copy only `row_scope.field_values` into `MutationGrant.row_constraints`. A mutation without a matching dataset grant fails before controller execution.

In `security.py`, route protected-filter replacement through the grant when supplied. Preserve the existing helper only for explicitly trusted embedded calls, and document that it is not an HTTP authorization boundary.

- [ ] **Step 4: Migrate existing policy fixtures and run GREEN**

Add an explicit `rowSecurity: {"mode": "global"}` to fixtures intentionally exercising global datasets. Add `rowSecurity: {"mode": "claims", "claims": {"tenant": "tenant"}}` to tenant data fixtures and give their test principals a tenant attribute. Export the new public grant/error types from `runtime/__init__.py`.

Run:

```powershell
pytest -q tests/test_runtime_dataset_authorization.py tests/test_runtime_http_transport.py tests/test_security_config.py tests/test_legacy_tanstack_authorization.py
```

Expected: PASS. The test requests with missing tenant claims must stop at 403 before data/controller assertions; global-policy fixtures retain their intentional access.

- [ ] **Step 5: Commit the isolated policy boundary**

```powershell
git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dataset_authorization.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/mutation_authorization.py dash_tanstack_pivot/pivot_engine/pivot_engine/security.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/__init__.py tests/test_runtime_dataset_authorization.py tests/test_runtime_http_transport.py tests/test_security_config.py
git commit -m "fix: require explicit row scope grants"
```

### Task 2: Consume the row-scope grant at every HTTP, legacy, Dash, and realtime boundary

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/complete_rest_api.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/dash_integration.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py`
- Modify: `tests/test_runtime_http_transport.py`
- Modify: `tests/test_runtime_dataset_authorization.py`
- Modify: `tests/test_legacy_tanstack_authorization.py`
- Modify: `tests/test_realtime_authorization.py`
- Modify: `tests/test_dash_runtime_callbacks.py`

**Interfaces:**
- `RuntimeHttpGateway._state_from_request(request, principal, row_scope)` applies only `row_scope` predicates.
- `RuntimeHttpGateway._context_from_request(request, principal, row_scope)` attaches `row_scope` to `PivotRequestContext`.
- `CompletePivotAPI._authorize_dataset(...) -> RowScopeGrant` and `_scope_spec(spec, grant) -> PivotSpec` are the only route helpers allowed to run externally supplied specs.
- `PivotRequestContext` gains immutable `row_scope: Optional[RowScopeGrant]`.

- [ ] **Step 1: Write endpoint regressions before changing request construction**

Add parametrized tests for `/pivot/tanstack` and `/v1/runtime` that use a `claims` policy and an authorized viewer/editor with no attributes. Assert HTTP 403, no response data, and unchanged A/B rows. Add a test where client filters name tenant B while claims specify A; assert only A appears. Add an export/filter-options case and a realtime subscription case to prove each operation uses the same scope.

Add a Dash callback regression where the principal provider returns `{}` for a claims-scoped table and assert the callback returns a controlled authorization error rather than data.

- [ ] **Step 2: Run endpoint regressions and verify RED**

Run:

```powershell
pytest -q tests/test_runtime_http_transport.py tests/test_runtime_dataset_authorization.py tests/test_legacy_tanstack_authorization.py tests/test_realtime_authorization.py tests/test_dash_runtime_callbacks.py
```

Expected: at least the V1 request still returns tenant rows because `_state_from_request` presently uses arbitrary attributes and no explicit grant.

- [ ] **Step 3: Bind grants before state/cache/controller construction**

At `RuntimeHttpGateway.process`, authorize `table` and requested operation first, retain the returned `row_scope`, build state using `row_scope.apply_to_filters`, and pass it into the request context. For transaction/update, pass that same grant into `RuntimeMutationAuthorizer.authorize` and the context; never derive mutation constraints a second time from the principal.

In `CompletePivotAPI`, make `_authorize_dataset` return a grant and replace every route-local `apply_rls_to_spec(pivot_spec, user)` with the grant-aware helper. Add a `user` dependency and authorization to `/admin/dashboard` and `/pivot/expansion-state`; bind expansion state to a table/scope-aware server key or reject it from unauthorised users.

In the legacy adapter and Dash callback paths, require a grant prior to creating a query `PivotSpec`, export request, or realtime subscription. Scope CDC visibility using policy fields, not unrelated principal attributes.

- [ ] **Step 4: Run GREEN and check route coverage**

Run the Step 2 command plus:

```powershell
pytest -q tests/test_features_impl.py tests/test_runtime_service.py tests/test_editing_e2e.py
```

Expected: PASS. Search results must show every externally reachable spec-construction path calls `authorize` then applies a grant:

```powershell
rg -n "apply_rls_to_spec\(|run_pivot|handle_request" dash_tanstack_pivot/pivot_engine/pivot_engine
```

- [ ] **Step 5: Commit transport-wide grant consumption**

```powershell
git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py dash_tanstack_pivot/pivot_engine/pivot_engine/complete_rest_api.py dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py dash_tanstack_pivot/pivot_engine/pivot_engine/dash_integration.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py tests/test_runtime_http_transport.py tests/test_runtime_dataset_authorization.py tests/test_legacy_tanstack_authorization.py tests/test_realtime_authorization.py tests/test_dash_runtime_callbacks.py
git commit -m "fix: scope every external pivot read and write"
```

### Task 3: Make edit history principal-owned

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/models.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/session_manager.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/event_store.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py`
- Modify: `tests/test_runtime_http_transport.py`
- Modify: `tests/test_editing_e2e.py`
- Modify: `investigation_2026_09_14/security_certification_probe_2026_09_16.py`

**Interfaces:**
- `EditSessionState.owner_subject: str` and `SessionEventRecord.owner_subject: str` are immutable owner values.
- `EditSessionManager.get_event_for_session(session, event_id, owner_subject) -> Optional[SessionEventRecord]` returns only an event with matching session key and subject.
- Router transaction payload always contains `session_id=context.session_id` and `client_instance=context.client_instance` without reading their client-supplied counterparts.

- [ ] **Step 1: Write a cross-subject same-tenant forgery test**

Add an end-to-end V1 test that:

1. uses an editor `victim` with tenant A to create an edit in `victim-tab`;
2. switches the dependency override to an editor `attacker` with the same tenant A;
3. submits `undo` with the victim event id plus `session_id="principal:victim"` and `client_instance="victim-tab"`; and
4. asserts generic 403/no applicable history event and that the A value remains edited.

Add direct `EditSessionManager` tests proving a known event id cannot be loaded through a different session key or owner subject.

- [ ] **Step 2: Run the history tests and verify RED**

Run:

```powershell
pytest -q tests/test_runtime_http_transport.py -k "history or session" tests/test_editing_e2e.py
```

Expected: the forged undo currently applies because the router prefers the client `session_id` and `client_instance` fields.

- [ ] **Step 3: Replace browser session selection with context identity**

Replace the `first_present(...)` transaction session merge in `runtime/router.py` with unconditional server values. Add `owner_subject` when creating edit sessions and recording events. Ensure `_resolve_undo_target_event`, redo, revert, replace, finalisation, and every `EventStore.get` used for an event action call the session-aware lookup. A mismatch must behave as no event; do not disclose whether an event id exists for someone else.

- [ ] **Step 4: Run GREEN and rerun the certification proof**

Run:

```powershell
pytest -q tests/test_runtime_http_transport.py tests/test_editing_e2e.py tests/test_edit_session_architecture.py
python investigation_2026_09_14\security_certification_probe_2026_09_16.py
```

Expected: all tests pass and the probe's `same_tenant_history_session_override` reports a rejected attacker action with the victim value unchanged.

- [ ] **Step 5: Commit history ownership**

```powershell
git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py dash_tanstack_pivot/pivot_engine/pivot_engine/editing/models.py dash_tanstack_pivot/pivot_engine/pivot_engine/editing/session_manager.py dash_tanstack_pivot/pivot_engine/pivot_engine/editing/event_store.py dash_tanstack_pivot/pivot_engine/pivot_engine/editing/service.py tests/test_runtime_http_transport.py tests/test_editing_e2e.py investigation_2026_09_14/security_certification_probe_2026_09_16.py
git commit -m "fix: bind edit history to authenticated subject"
```

### Task 4: Replace shared-token Arrow Flight with principal and scope authorization

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/flight_server.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/security.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dataset_authorization.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/__init__.py`
- Modify: `tests/test_analysis_report_fixes.py`
- Create: `tests/test_flight_authorization.py`

**Interfaces:**
- `FlightPrincipalResolver` is a synchronous protocol with `resolve(headers) -> User`.
- `PivotFlightServer(..., principal_resolver, dataset_authorizer)` requires both arguments when authentication is enabled.
- Internal `_FlightResult` stores `table`, `owner_subject`, `scope_fingerprint`, `table_data`, and `expires_at`.
- `PivotFlightServer.do_action` applies a `RowScopeGrant` to the parsed `PivotSpec`; `do_get` reauthenticates and validates ticket owner/scope.

- [ ] **Step 1: Write Flight authorization tests**

Create tests with two resolver-issued principals for tenants A and B. Verify:

```python
def test_flight_pivot_applies_principal_row_scope():
    # Tenant A resolver receives only the A aggregate from a table containing A and B.
    ...

def test_flight_ticket_cannot_be_replayed_by_a_different_subject_or_scope():
    # A ticket created by tenant A raises FlightUnauthorizedError for tenant B.
    ...

def test_flight_rejects_shared_token_configuration_in_production(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    with pytest.raises(RuntimeError, match="principal_resolver"):
        PivotFlightServer(controller, location=None, auth_token="legacy-token")
```

Add tests that missing row claims are denied, `clear_cache` needs admin, unauthenticated local mode is explicit, and a ticket expires as a normal not-found response.

- [ ] **Step 2: Run Flight tests and verify RED**

Run:

```powershell
pytest -q tests/test_analysis_report_fixes.py -k flight tests/test_flight_authorization.py
```

Expected: the new module fails to import and the current shared-token implementation returns unfiltered A/B results.

- [ ] **Step 3: Implement resolver middleware and scoped ticket cache**

Replace `_BearerTokenMiddlewareFactory` with a middleware factory that normalises headers, calls `FlightPrincipalResolver.resolve`, and holds the `User` on the per-call middleware. Retrieve it from `context.get_middleware("pivot_auth")`. Parse `pivot` bodies to `PivotSpec`, call dataset authorization for `read`, apply the row grant, then execute `run_pivot_arrow`.

Store a frozen result record rather than a raw table tuple. On `do_get`, resolve the principal again, rebuild its grant for the cached table, and compare subject and fingerprint with constant-time equality before returning an Arrow stream. Restrict `clear_cache` to admin. In production, reject the old `auth_token` constructor path; allow unauthenticated Flight only when the explicit local-development flag is present.

- [ ] **Step 4: Run GREEN**

Run:

```powershell
pytest -q tests/test_analysis_report_fixes.py -k flight tests/test_flight_authorization.py tests/test_runtime_dataset_authorization.py
```

Expected: tenant A receives only A data; ticket replay and shared-token production configuration are rejected.

- [ ] **Step 5: Commit Flight isolation**

```powershell
git add dash_tanstack_pivot/pivot_engine/pivot_engine/flight_server.py dash_tanstack_pivot/pivot_engine/pivot_engine/security.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dataset_authorization.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/__init__.py tests/test_analysis_report_fixes.py tests/test_flight_authorization.py
git commit -m "fix: scope Arrow Flight requests and tickets"
```

### Task 5: Remove Redis pickle and require secure Redis transport

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/cache/redis_cache.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/config.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/controller.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py`
- Modify: `tests/test_cache.py`
- Modify: `tests/test_config_from_env.py`
- Modify: `tests/test_config_main.py`

**Interfaces:**
- `RedisCache.set(key, value, ttl=None)` accepts `pa.Table` or JSON serialisable values only and raises `TypeError` otherwise.
- Redis payloads begin with `b"PIVOT_ARROW_V1\\0"` or `b"PIVOT_JSON_V1\\0"`.
- `RedisCache.get(key)` returns `None` for an unknown, invalid, or legacy payload and never imports or calls `pickle`.
- `ScalablePivotConfig.redis_config` accepts `url`, `username`, `password`, `ssl_cert_reqs`; production uses an authenticated `rediss://` URL.

- [ ] **Step 1: Write non-executable codec tests**

Add tests asserting Arrow and JSON round-trip through the framed values, a tuple write raises `TypeError`, and an injected malicious pickle object whose `__reduce__` would mutate a sentinel is returned as `None` without calling the sentinel. Add config tests for production rejection of `redis://`, missing credentials, and disabled certificate verification; retain an explicit local `redis://` test.

- [ ] **Step 2: Run cache/config tests and verify RED**

Run:

```powershell
pytest -q tests/test_cache.py tests/test_config_from_env.py tests/test_config_main.py
```

Expected: the tuple/pickle test currently passes and demonstrates that pickle is used; production Redis validation does not yet exist.

- [ ] **Step 3: Implement framed codecs and safe Redis setup**

Remove the `pickle` import and all fallback logic. Prefix every encoded value; decode only the matching Arrow IPC or UTF-8 JSON body. A decode error must be treated as a cache miss, optionally deleting the corrupt key without raising application data. Instantiate Redis from a URL or explicit TLS options with `decode_responses=False` and certificate verification enabled.

Make `ScalablePivotConfig.validate()` call the production security settings validator for Redis configuration. Update controller construction so it passes the normalized URL/TLS options unchanged to `RedisCache`.

- [ ] **Step 4: Run GREEN**

Run:

```powershell
pytest -q tests/test_cache.py tests/test_config_from_env.py tests/test_config_main.py tests/test_controller.py -k "redis or cache"
```

Expected: all supported cache values round-trip; all pickle/unknown payloads are safe misses; production configuration cannot select plaintext Redis.

- [ ] **Step 5: Commit safe Redis codecs**

```powershell
git add dash_tanstack_pivot/pivot_engine/pivot_engine/cache/redis_cache.py dash_tanstack_pivot/pivot_engine/pivot_engine/config.py dash_tanstack_pivot/pivot_engine/pivot_engine/controller.py dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py tests/test_cache.py tests/test_config_from_env.py tests/test_config_main.py
git commit -m "fix: remove executable Redis cache serialization"
```

### Task 6: Production configuration, middleware, key rotation, CI evidence, and operations runbook

**Files:**
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/production_security.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/security.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/complete_rest_api.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/__init__.py`
- Create: `deploy/nginx/serverside-pivot.conf`
- Create: `scripts/verify_production_deployment.py`
- Create: `.github/workflows/security.yml`
- Create: `security/requirements-security.txt`
- Create: `docs/operations/production-security-runbook.md`
- Modify: `README.md`
- Modify: `dash_tanstack_pivot/pivot_engine/README.md`
- Modify: `SECURITY.md`
- Modify: `dash_tanstack_pivot/pyproject.toml`
- Create: `tests/test_production_security.py`
- Modify: `tests/test_security_config.py`

**Interfaces:**
- `ProductionSecuritySettings.from_env()` and `.validate()` report every missing production control in one `RuntimeError`.
- `install_asgi_security(app, settings)` installs trusted-host, HTTPS/forwarded-proto, CSP/header, Origin/CSRF, request-size, and distributed fixed-window rate-limit middleware.
- `install_flask_security(server, settings)` supplies equivalent headers and cookie-origin checks for Dash hosts.
- `JwtKeyRing.from_env()` resolves active and verification keys by `kid`; `authenticate_user_credentials` requires `iss`, `aud`, `exp`, and a known `kid` in production.
- `scripts/verify_production_deployment.py --base-url https://...` exits nonzero if TLS/redirect/headers/deployment checks fail.

- [ ] **Step 1: Write failing production controls and JWT rotation tests**

Add production environment tests that require HTTPS public origin, hosts, CSP, rate-limit Redis URL, key-ring with active `kid`, issuer, audience, and secure Redis. Test that a valid active-key JWT succeeds, a previous verification key succeeds during rotation, an unknown `kid`/audience fails, and removal of the previous key rejects the old token.

Using `TestClient`, test that `install_asgi_security` supplies HSTS, CSP, `X-Content-Type-Options`, `Referrer-Policy`, and frame-ancestors; rejects a cookie-authenticated cross-origin POST without a matching CSRF header; allows a matching same-origin request; and returns 429 after its configured limit. Add Flask test-client equivalents for headers and Origin checks.

- [ ] **Step 2: Run production-security tests and verify RED**

Run:

```powershell
pytest -q tests/test_production_security.py tests/test_security_config.py
```

Expected: the new production module/imports fail; current JWT decoding has a single static secret and no audience or `kid` validation.

- [ ] **Step 3: Implement validation, middleware, and rotating JWT key ring**

Create immutable settings parsed only from environment/configuration. In production reject missing or unsafe values before serving. Implement ASGI/Flask hooks with no browser-specific dependency: trusted proxy headers are accepted only from configured proxy networks, unsafe cookie requests require configured Origin plus constant-time double-submit CSRF token comparison, and rate windows use Redis `INCR`/expiry so multi-worker deployments share limits.

Replace direct `JWT_SECRET_KEY` decode selection with a key ring. Read an unverified JWT header only to choose an allowed `kid`, then perform normal signature, issuer, audience, expiry, and algorithm validation. Keep the documented local single-secret compatibility setting outside production. Wire the FastAPI installer through `CompletePivotAPI` and export both installers for host-neutral React/Streamlit services and Dash integration.

- [ ] **Step 4: Add deployable evidence artifacts**

Add an Nginx configuration that redirects HTTP to HTTPS, configures TLS 1.2+/OCSP/HSTS, limits request size/rate, forwards only needed headers, and applies the documented CSP. Add a deployment verifier that uses only the standard library to require `https`, follow/check redirects, inspect required headers, and emit JSON evidence.

Add a security GitHub workflow that installs pinned security tools from `security/requirements-security.txt`, runs `pip-audit`, Bandit, CycloneDX SBOM generation, the targeted security suite, and uploads JSON/SBOM reports. Update package dev extras as needed so CI tooling has declared provenance.

Write the operator runbook with mandatory evidence attachments and owner/cadence for certificate renewal, CSP review, CSRF/rate-limit verification, key rotation, vulnerability triage, third-party penetration test, incident tabletop/response, encrypted backup restore, and release sign-off. Update public READMEs and `SECURITY.md` with the secure startup contract and reporting contact/process.

- [ ] **Step 5: Run GREEN and check artifacts**

Run:

```powershell
pytest -q tests/test_production_security.py tests/test_security_config.py tests/test_runtime_http_transport.py tests/test_dash_runtime_callbacks.py
python scripts/verify_production_deployment.py --help
python -m compileall -q dash_tanstack_pivot/pivot_engine/pivot_engine
```

Expected: all tests pass, verifier help exits 0, and compilation succeeds. Review the workflow for pinned action versions and artifact upload paths.

- [ ] **Step 6: Commit production baseline and evidence tooling**

```powershell
git add dash_tanstack_pivot/pivot_engine/pivot_engine/production_security.py dash_tanstack_pivot/pivot_engine/pivot_engine/security.py dash_tanstack_pivot/pivot_engine/pivot_engine/complete_rest_api.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/__init__.py deploy/nginx/serverside-pivot.conf scripts/verify_production_deployment.py .github/workflows/security.yml security/requirements-security.txt docs/operations/production-security-runbook.md README.md dash_tanstack_pivot/pivot_engine/README.md SECURITY.md dash_tanstack_pivot/pyproject.toml tests/test_production_security.py tests/test_security_config.py
git commit -m "feat: add production security verification baseline"
```

### Task 7: Full certification and release evidence

**Files:**
- Modify: `investigation_2026_09_14/security_certification_probe_2026_09_16.py`
- Create: `investigation_2026_09_14/security-certification-after-remediation.json`
- Modify: `docs/operations/production-security-runbook.md`

**Interfaces:**
- The certification probe reports rejected no-claim reads/writes, rejected cross-subject history, Flight A-only results and ticket replay rejection, no pickle source/behavior, and all implemented local controls.
- The JSON output identifies itself as disposable local evidence and never claims external TLS, independent pentesting, or backup completion.

- [ ] **Step 1: Update expected security probe behavior**

Replace the confirmed-vulnerability interpretations with assertions that the unsafe requests are denied and data is unchanged. Add a malicious pickle injection check that does not execute the sentinel. Add Flight resolver/ticket replay checks. Retain the explicit fields for operator-only controls as `not_verified_locally` rather than fabricating success.

- [ ] **Step 2: Run the probe and verify it fails before final compatibility fixes**

Run:

```powershell
python investigation_2026_09_14\security_certification_probe_2026_09_16.py
```

Expected before all preceding tasks are complete: one or more unsafe behavior assertions fail. Do not record a certification file until they are fixed.

- [ ] **Step 3: Run final verification and generate local evidence**

Run:

```powershell
python investigation_2026_09_14\security_certification_probe_2026_09_16.py
pytest -q
cd dash_tanstack_pivot; npm run typecheck; npm run build:js; npm run build:py
cd ..; git diff --check
```

Save only the probe's JSON result as `security-certification-after-remediation.json`. Do not include tokens, production URLs, customer data, or credentials.

- [ ] **Step 4: Verify requirements against the spec**

Check each spec goal explicitly:

1. no-claim legacy/V1/Flight read and write rejected;
2. cross-subject undo/redo/revert/replace rejected;
3. Flight ticket replay rejected and scoped data returned;
4. no `pickle` import or `pickle.loads` remains in cache code;
5. settings validator, proxy config, CI reports, deployment verifier, and runbook exist.

If a goal cannot be demonstrated locally, list it in the runbook as an operator deployment gate rather than calling the release certified.

- [ ] **Step 5: Commit certification artifacts**

```powershell
git add investigation_2026_09_14/security_certification_probe_2026_09_16.py investigation_2026_09_14/security-certification-after-remediation.json docs/operations/production-security-runbook.md
git commit -m "test: certify security boundary remediation"
```
