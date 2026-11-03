# Production Assurance Closure Design

## Goal

Close the remaining in-repository security enforcement and release-assurance
gaps without claiming that organization-owned deployment controls have been
performed. Production configuration must fail closed, public errors must not
contain backend details, release publishing must be security-gated, and a
machine-checkable evidence assessment must distinguish verified local controls
from required operator evidence.

## Scope and non-goals

This design covers the FastAPI runtime, Flask/Dash registration, Arrow Flight,
JWT/Redis configuration validation, release workflows, and security evidence
artifacts. It does not issue a certificate, configure a customer network,
perform an external penetration test, restore a production backup, or run an
incident response exercise. Those controls remain release evidence supplied by
the deployment operator.

## Security baseline

The certification matrix uses OWASP ASVS v5.0.0 Level 2 as the
application-security baseline. Each relevant control maps to one of three
states:

- `verified_locally`: executable test or probe evidence is checked in;
- `operator_evidence_required`: a release attachment is required and is never
  fabricated by repository tooling;
- `not_applicable`: an owner and written rationale are required.

The matrix must name the requirement, implementation/test evidence, owner,
cadence, and release gate. It is an evidence assessment, not a claim of an
independent ASVS certification.

## Architecture

### Canonical production environment

`ProductionSecuritySettings` becomes the sole environment-state parser for
production-sensitive components. It reads all supported environment aliases
(`ENV`, `APP_ENV`, `FLASK_ENV`, `DASH_ENV`), rejects conflicting non-empty
values, and exposes one immutable production result. The security module,
Dash integration, and runtime callback registration consume this canonical
result rather than maintaining separate production checks.

The settings validator rejects weak symmetric JWT verification keys in a
production key ring. A key must be non-empty after trimming and meet a minimum
32-byte UTF-8 secret length for HS algorithms; asymmetric algorithms continue
to require an explicitly configured public key. Production Flight ignores no
environment escape hatch: authentication is always required, and construction
requires both a per-principal resolver and dataset authorizer.

### HTTP and Dash request boundary

The ASGI middleware enforces the body ceiling incrementally by wrapping the
ASGI receive channel. It counts every `http.request` body chunk and returns a
413 response before the wrapped application can process an oversized payload.
`Content-Length` remains an early rejection optimization, not the authority.
The wrapper avoids consuming a second response and preserves a normal request
stream for valid bodies.

`register_pivot_app` installs Flask security on the Dash server when canonical
production settings are active. Installation is idempotent per Flask server.
The Flask middleware matches ASGI host, HTTPS/proxy, request-size, Origin/CSRF,
rate-limit, secure-cookie, and response-header policy. Its rate limiter uses
the configured Redis implementation so Dash workers share a limit.

FastAPI routes return a stable generic `internal_error` envelope for unexpected
exceptions. The server emits a structured event with a generated correlation
identifier, endpoint, status, and exception class; exception messages and
tracebacks never enter a client response. Expected validation, authorization,
and explicit user-facing limit errors retain their controlled response shapes.

### Release security gate

`security/requirements-security.txt` remains version-pinned. A security gate
script parses `pip-audit` JSON and compares each finding to an explicit
`security/dependency-exceptions.json` entry containing the vulnerability ID,
affected package/version, non-empty rationale, owner, ticket, and future
expiry. Unmatched or expired findings fail. Bandit findings fail directly
unless an inline, reviewed nosec rationale is present in source.

The release workflow runs the pinned scanner tooling, SBOM generation,
security regressions, and the dependency gate before package publication. The
PyPI publish step runs only after those steps succeed and security reports are
uploaded regardless of outcome. The standalone scheduled security workflow
uses the same gate.

### Certification and evidence

`security/certification-matrix.json` is a structured list of ASVS-derived
controls. `scripts/verify_security_release_evidence.py` validates its schema,
local evidence paths, required operator-evidence identifiers, and prohibited
success claims. The script accepts a release-evidence directory only when all
operator-owned evidence entries are present; it does not inspect secrets or
customer data. A release fails if a required attachment is absent, expired, or
has no accountable owner.

The operations runbook describes acceptable evidence for TLS/network controls,
identity configuration, tenant-isolation verification, audit logging, backup
restore, incident procedures, penetration testing, and release sign-off. It
links each item to the certification matrix.

## Data flow

```text
environment aliases -> canonical settings -> ASGI / Flask / Flight startup gate
HTTP chunks -> size-counting receive wrapper -> application route -> generic error mapper + server audit event
security scan JSON + exception manifest -> dependency gate -> release workflow -> publish
local tests/probe + operator release attachments -> evidence validator -> ASVS matrix assessment
```

## Failure handling

- Conflicting environment aliases, weak production keys, absent Flight
  authentication dependencies, invalid exception manifests, and missing
  release evidence are startup/gate failures.
- Oversized fixed-length and chunked HTTP requests receive 413 without being
  dispatched to route code.
- Unexpected backend failures receive a correlation ID and a generic 500;
  their detailed context is server-side only.
- Evidence tooling treats unrecognized, stale, or fabricated state values as
  invalid rather than permitting release.

## Verification

Tests must prove chunked-body rejection, environment conflict rejection,
Flask automatic installation and headers, production Flight rejection of an
unauthenticated setting, weak-key rejection, generic unexpected errors,
dependency exception expiry/matching, workflow release gating, and
certification matrix/evidence validation. The full Python suite, TypeScript
typecheck, production JavaScript build, strict metadata build, and the
existing security certification probe remain required.
