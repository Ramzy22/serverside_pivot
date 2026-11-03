# Production Assurance Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fail closed for production security configuration and request boundaries, block releases with unreviewed security findings, and provide an OWASP ASVS v5.0.0 Level 2 evidence assessment without fabricating operator-owned proof.

**Architecture:** `ProductionSecuritySettings` becomes the single source of truth for production state and feeds ASGI, Flask/Dash, JWT, and Flight startup. A buffered ASGI body gate rejects chunked oversized requests before dispatch, while central error mapping keeps backend messages in server logs. A shared scanner gate and evidence validator turn release assurance into reproducible, machine-checked contracts.

**Tech Stack:** Python 3.10+, FastAPI/Starlette, Flask/Dash, PyArrow Flight, python-jose, pytest, GitHub Actions, pip-audit, Bandit, CycloneDX.

**Spec:** `docs/superpowers/specs/2026-09-17-production-assurance-closure-design.md`

## Global Constraints

- Production means canonical `production`/`prod`; conflicting non-empty environment aliases fail before a public service is configured.
- Do not expose backend exception strings, stack traces, table names, tenant values, or credentials in public error responses.
- `require_auth=False` is rejected whenever the canonical environment is production.
- TLS issuance, edge deployment, IdP setup, penetration tests, backup recovery, and incident exercises remain operator evidence—not repository claims.
- The security baseline is OWASP ASVS **v5.0.0 Level 2**; matrix entries must pin the version with `v5.0.0-...` identifiers.
- The shared worktree has unrelated changes. Do not reset, clean, reformat unrelated files, or commit unless explicitly requested.

---

### Task 1: Canonical production state, strong JWT keys, and Flight fail-closed startup

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/production_security.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/security.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/dash_integration.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/flight_server.py`
- Modify: `tests/test_production_security.py`
- Modify: `tests/test_security_config.py`
- Modify: `tests/test_flight_authorization.py`

**Interfaces:**
- Produces `resolve_runtime_environment(environ: Optional[Mapping[str, str]] = None) -> str`, which returns a normalized canonical environment or raises `RuntimeError` for non-empty disagreement among `ENV`, `APP_ENV`, `FLASK_ENV`, and `DASH_ENV`.
- `ProductionSecuritySettings.from_env()` consumes that function; `is_production` is the only production decision used in migration targets.
- `JwtKeyRing.from_env()` rejects production HS verification keys shorter than 32 UTF-8 bytes and invalid public-key configuration before decode.
- `PivotFlightServer(..., require_auth=False)` raises in canonical production even if `PIVOT_FLIGHT_ALLOW_UNAUTHENTICATED=1` is set.

- [ ] **Step 1: Write failing state/configuration tests**

Add to `tests/test_production_security.py`:

```python
def test_conflicting_environment_aliases_are_a_startup_error(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("DASH_ENV", "development")
    with pytest.raises(RuntimeError, match="Conflicting environment"):
        ProductionSecuritySettings.from_env()
```

Add to `tests/test_security_config.py` a production key-ring case using
`{"current": "short"}` and assert `JwtKeyRing.from_env()` raises a weak-key
configuration error. Add to `tests/test_flight_authorization.py`:

```python
def test_flight_cannot_disable_authentication_in_production(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    with pytest.raises(RuntimeError, match="cannot be disabled in production"):
        PivotFlightServer(_Controller(), location=None, require_auth=False)
```

- [ ] **Step 2: Verify RED**

Run:

```powershell
pytest -q tests/test_production_security.py tests/test_security_config.py tests/test_flight_authorization.py -k "conflicting or weak or disable"
```

Expected: conflicting aliases choose one value, the short key is accepted, and Flight permits `require_auth=False`.

- [ ] **Step 3: Implement canonical environment resolution**

In `production_security.py`, normalize `prod -> production`, `dev/local -> development`, collect all non-empty alias values, and raise when the normalized set has more than one item. Parse this before building `ProductionSecuritySettings`.

Replace bespoke production checks in `security.py`, `dash_integration.py`, and
`runtime/dash_callbacks.py` with the canonical resolver/settings result. Do not
accept a production flag merely because another alias says development.

In `JwtKeyRing.from_env`, enforce minimum 32-byte UTF-8 secret length for
HS256/384/512 keys. Retain the accepted algorithm allowlist and support long
asymmetric public verification keys.

In `flight_server.py`, resolve canonical production state before honoring the
local unauthenticated flag; raise a controlled `RuntimeError` if production
and `require_auth` is false.

- [ ] **Step 4: Verify GREEN**

Run the Step 2 command and:

```powershell
pytest -q tests/test_security_config.py tests/test_flight_authorization.py tests/test_dash_runtime_callbacks.py
```

Expected: production aliases cannot disagree, weak HS key rings are rejected,
local explicit unauthenticated Flight still works, and production Flight cannot
turn off authentication.

---

### Task 2: Stream-safe ASGI request limits and non-disclosing HTTP failures

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/production_security.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/complete_rest_api.py`
- Modify: `tests/test_production_security.py`
- Modify: `tests/test_runtime_dataset_authorization.py`

**Interfaces:**
- Produces a private ASGI body-limiting middleware which buffers at most `max_request_bytes`, then replays valid request events to downstream middleware; it returns `413 {"detail": "Request body is too large."}` for fixed-length and chunked payloads.
- `CompletePivotAPI._internal_error(request, exc) -> HTTPException` emits a generated correlation ID, server-side structured error event, and public `{"code": "internal_error", "correlationId": ...}` detail.

- [ ] **Step 1: Write failing chunked-body and error-disclosure tests**

Use a minimal ASGI app plus manually supplied `receive` events in
`tests/test_production_security.py`:

```python
async def test_asgi_security_rejects_chunked_body_before_endpoint_runs():
    called = False
    async def endpoint(scope, receive, send):
        nonlocal called
        called = True
    response = await invoke_asgi_with_chunks(app, [b"a" * 8, b"b" * 8])
    assert response.status == 413
    assert called is False
```

Set the configured maximum to 12 bytes and omit `Content-Length`. Add a route
whose controller method raises `RuntimeError("postgres password=secret")` and
assert a 500 response includes `internal_error` and a correlation ID but none
of the exception text.

- [ ] **Step 2: Verify RED**

Run:

```powershell
pytest -q tests/test_production_security.py -k "chunked or internal_error"
```

Expected: the endpoint executes for chunked data, and the response contains
the backend message.

- [ ] **Step 3: Implement body buffering and generic errors**

Install a pure ASGI body-gate middleware before the existing
`BaseHTTPMiddleware` security policy. It must read `http.request` messages to
completion before invoking downstream code, track cumulative `body` bytes,
return one 413 response upon overflow, replay buffered events in order for a
valid request, preserve `http.disconnect`, and never call the route after a
limit failure. Keep existing `Content-Length` rejection as a cheaper early
path.

Add a per-request UUID correlation ID to unexpected route failures. Replace
all `HTTPException(status_code=500, detail=str(exc))` branches in
`CompletePivotAPI` with one helper that logs `exception_type`, endpoint, and
correlation ID using the server logger, chains the exception, and returns only
the generic detail object. Preserve explicitly handled 400/403/404/413 errors
and existing runtime error envelopes.

- [ ] **Step 4: Verify GREEN**

Run the Step 2 command plus:

```powershell
pytest -q tests/test_runtime_dataset_authorization.py tests/test_runtime_http_transport.py
rg -n "detail=str\(" dash_tanstack_pivot/pivot_engine/pivot_engine
```

Expected: chunked requests are rejected pre-dispatch, expected client errors
retain their behavior, no public `detail=str(...)` source remains, and no
cross-tenant regression occurs.

---

### Task 3: Install Flask/Dash production hardening at the host boundary

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/production_security.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/dash_integration.py`
- Modify: `tests/test_production_security.py`
- Modify: `tests/test_dash_runtime_callbacks.py`
- Modify: `docs/operations/production-security-runbook.md`

**Interfaces:**
- `install_flask_security(server, settings, *, rate_limiter=None) -> Any` is idempotent per Flask server and installs secure cookies, body limit, host/HTTPS/proxy-aware origin checks, headers, and shared rate limiting.
- `register_pivot_app(...)` calls `install_flask_security(app.server, ProductionSecuritySettings.from_env())` before registering public routes whenever canonical production is active.

- [ ] **Step 1: Write failing Flask integration tests**

Build a `Dash`/Flask host in `tests/test_dash_runtime_callbacks.py` under the
complete `_production_env` fixture. Register the pivot app and assert the
server has the installation marker, response HSTS/CSP headers, secure cookie
settings, and same-origin CSRF acceptance/cross-origin rejection. Register a
second pivot instance and assert it does not duplicate Flask hooks.

Add a Flask `MAX_CONTENT_LENGTH` regression in
`tests/test_production_security.py` proving an oversized callback-style POST
is 413 before route processing.

- [ ] **Step 2: Verify RED**

Run:

```powershell
pytest -q tests/test_production_security.py tests/test_dash_runtime_callbacks.py -k "flask or production"
```

Expected: `register_pivot_app` does not install the Flask middleware and
duplicate registration creates duplicate hooks or no marker.

- [ ] **Step 3: Implement idempotent Flask installation**

Add an internal marker attribute on the Flask server. Configure
`MAX_CONTENT_LENGTH` from `max_request_bytes`, reject malformed/oversized
declared body lengths, enforce host and trusted-proxy-aware HTTPS policy,
perform constant-time double-submit CSRF checks, apply the injected or Redis
fixed-window limiter for unsafe methods, and attach the existing headers.

In `register_pivot_app`, resolve settings once, call `validate()` in production,
install before the drill and runtime endpoints, and leave non-production hosts
unchanged. Update the runbook to state that Nginx/managed edge still enforces
the authoritative streamed-body limit for WSGI hosts.

- [ ] **Step 4: Verify GREEN**

Run the Step 2 command plus:

```powershell
pytest -q tests/test_dash_runtime_callbacks.py tests/test_runtime_http_transport.py
```

Expected: Dash receives production headers and CSRF protections automatically;
the integration remains idempotent and callback security behavior is intact.

---

### Task 4: Gate dependency findings and release publication

**Files:**
- Create: `security/dependency-exceptions.json`
- Create: `scripts/ci/verify_dependency_audit.py`
- Modify: `.github/workflows/security.yml`
- Modify: `.github/workflows/release.yml`
- Modify: `security/requirements-security.txt`
- Modify: `tests/test_production_security.py`
- Modify: `docs/operations/production-security-runbook.md`

**Interfaces:**
- `python scripts/ci/verify_dependency_audit.py --audit report.json --exceptions security/dependency-exceptions.json --as-of YYYY-MM-DD` returns zero only when every audit vulnerability is remediated or matched by a non-expired, exact package/version exception.
- Exception schema: `{ "exceptions": [{"id", "package", "version", "owner", "ticket", "rationale", "expires"}] }`; an empty list is valid.
- Release workflow runs pip-audit, Bandit, CycloneDX, security regressions, dependency gate, and report upload before the PyPI publish action.

- [ ] **Step 1: Write failing audit-gate tests**

Add fixture audit JSON with one vulnerability in
`tests/test_production_security.py`. Assert unmatched findings fail, an exact
unexpired exception passes, an expired exception fails, and an exception with
missing owner/ticket/rationale fails. Assert the release workflow source places
the audit gate before `Publish to PyPI` and does not use `continue-on-error`
for the release scanner commands.

- [ ] **Step 2: Verify RED**

Run:

```powershell
pytest -q tests/test_production_security.py -k "dependency or release"
```

Expected: the verifier does not exist and release publication has no scanner
dependency.

- [ ] **Step 3: Implement audit gate and workflow wiring**

Implement a standard-library JSON validator accepting pip-audit’s list shape.
Validate all exception fields, parse `expires` as ISO date, require it be
strictly future relative to `--as-of`, and reject duplicate exception keys.
Print a redacted actionable summary containing package, version, vulnerability
ID, and exception ticket only.

Commit an empty exception list. Run the script after pip-audit in both
workflows. In `release.yml`, install the pinned security requirements, produce
the three reports, run the full targeted security suite and dependency gate,
upload reports with `if: always()`, then allow the existing publish action. Pin
new GitHub Actions by full commit SHA.

- [ ] **Step 4: Verify GREEN**

Run:

```powershell
pytest -q tests/test_production_security.py
python scripts/ci/verify_dependency_audit.py --help
```

Expected: exception handling is expiry-bound and exact; the static workflow
contract demonstrates that publishing cannot occur after a failed scanner gate.

---

### Task 5: ASVS evidence matrix and release-evidence validator

**Files:**
- Create: `security/certification-matrix.json`
- Create: `security/release-evidence.example.json`
- Create: `scripts/verify_security_release_evidence.py`
- Modify: `docs/operations/production-security-runbook.md`
- Modify: `README.md`
- Modify: `SECURITY.md`
- Modify: `tests/test_production_security.py`

**Interfaces:**
- Matrix entry schema: `{ "id", "asvs", "title", "state", "owner", "cadence", "localEvidence", "operatorEvidence" }`, where `state` is `verified_locally`, `operator_evidence_required`, or `not_applicable`.
- Release evidence schema has `{ "release", "generatedAt", "attachments" }`; each attachment has `id`, `path`, `owner`, `expiresAt`, and `sha256`.
- `python scripts/verify_security_release_evidence.py --matrix ... --evidence ... --as-of YYYY-MM-DD` validates paths, SHA-256 digests, current evidence, accountable owners, and every required matrix item without opening the attachment contents.

- [ ] **Step 1: Write failing evidence-validator tests**

Create a temporary matrix/evidence directory in
`tests/test_production_security.py`. Assert valid local-only rows pass, a
missing required external attachment fails, an expired attachment fails, a
digest mismatch fails, and `verified_locally` cannot cite a non-existent test
or probe path. Assert every ASVS reference begins with `v5.0.0-` and every
operator-evidence control has an owner/cadence.

- [ ] **Step 2: Verify RED**

Run:

```powershell
pytest -q tests/test_production_security.py -k "evidence or matrix"
```

Expected: the matrix and verifier do not exist.

- [ ] **Step 3: Implement evidence contract and matrix**

Implement the validator with only `argparse`, `datetime`, `hashlib`, `json`,
and `pathlib`. Reject unknown states, duplicate IDs, empty mandatory fields,
relative paths escaping the supplied evidence directory, expired attachments,
and SHA mismatches. Do not read or print evidence contents.

Create matrix rows for authorization/tenant isolation, JWT and Flight
authentication, safe cache codecs, request limits, error handling/audit
logging, TLS/network, IdP configuration, dependency scanning/SBOM,
penetration testing, backup restore, incident response, and release sign-off.
Mark only code/test-backed items `verified_locally`; mark external controls
`operator_evidence_required` with the accountable owner and cadence. Provide a
non-success example evidence file and update the runbook/READMEs to reference
the exact validator command.

- [ ] **Step 4: Verify GREEN**

Run:

```powershell
pytest -q tests/test_production_security.py
python scripts/verify_security_release_evidence.py --help
```

Expected: the repository can prove the shape and freshness of supplied release
evidence while explicitly refusing to claim missing operator work.

---

### Task 6: End-to-end certification and build verification

**Files:**
- Modify: `investigation_2026_09_14/security_certification_probe_2026_09_16.py`
- Modify: `investigation_2026_09_14/security-certification-after-remediation.json`
- Modify: `docs/operations/production-security-runbook.md`

- [ ] **Step 1: Extend the failing local probe expectations**

Add checks for a conflicting environment rejection, weak production key
rejection, unauthenticated production Flight rejection, generic backend-error
response, and body-gate rejection. Keep external controls explicitly
`not_verified_locally`; do not generate evidence before all local checks pass.

- [ ] **Step 2: Verify RED**

Run:

```powershell
python investigation_2026_09_14\security_certification_probe_2026_09_16.py
```

Expected before Tasks 1–5 are complete: at least one new security check fails.

- [ ] **Step 3: Generate evidence and run release-equivalent checks**

Run:

```powershell
python investigation_2026_09_14\security_certification_probe_2026_09_16.py
pytest -q
cd dash_tanstack_pivot; npm run typecheck; npm run build:js; npm run build:py
cd ..; python -m compileall -q dash_tanstack_pivot\pivot_engine\pivot_engine
git diff --check
```

Review that the generated JSON remains synthetic/local and the certification
matrix has no external control labelled verified.

- [ ] **Step 4: Requirement-by-requirement review**

Confirm each reported gap:

1. fixed/chunked limits are pre-dispatch; conflicting flags cannot disable middleware; Dash installs Flask security;
2. unexpected backend text is never public and has a correlation ID server-side;
3. weak JWT secrets and unauthenticated production Flight are rejected;
4. scanners gate release and only unexpired reviewed exceptions can pass;
5. external TLS/network/IdP/tenant/audit/backup/incident evidence remains an attachment gate;
6. ASVS v5.0.0 Level 2 matrix and evidence validator give an honest certification assessment.

