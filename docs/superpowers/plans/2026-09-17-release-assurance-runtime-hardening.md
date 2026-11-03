# Release Assurance and Runtime Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make security scanning, ASVS evidence, HTTP controls, and production Flight transport fail closed before a release can publish.

**Architecture:** The matrix validates exact requirements from a versioned local catalog. CI generates and validates Python and JavaScript audit reports, Bandit results, and protected operator evidence before the PyPI action. Runtime middleware consumes rate tokens before dispatch and protects cookie-based writes; Flight validates TLS at construction.

**Tech Stack:** Python, pytest, FastAPI/Starlette, Flask/Dash, PyArrow Flight, GitHub Actions, pip-audit, npm audit, Bandit.

**Spec:** `docs/superpowers/specs/2026-09-17-release-assurance-runtime-hardening-design.md`

## Global Constraints

- The shared worktree is dirty; preserve unrelated changes and do not commit, reset, clean, or publish.
- Operator-owned evidence must never be generated or represented as verified by repository code.
- Keep public APIs backward-compatible outside production enforcement.
- Use the OWASP ASVS v5.0.0 released catalog; do not invent ASVS identifiers or titles.

---

### Task 1: Make evidence mappings and manifests fail closed

**Files:**
- Create: `security/asvs-5.0.0-catalog.json`
- Modify: `security/certification-matrix.json`
- Modify: `scripts/verify_security_release_evidence.py`
- Test: `tests/test_production_security.py`

**Interfaces:** `verify(matrix_path, evidence_path, repository_root, as_of)` rejects a zero-control matrix and unknown `{id, title}` ASVS catalog references.

- [ ] **Step 1: Write failing tests** for an empty matrix, a wrong catalog title, and a valid canonical mapping.
- [ ] **Step 2: Run** `pytest -q tests/test_production_security.py -k "matrix or evidence"` and confirm the empty/wrong cases currently pass.
- [ ] **Step 3: Implement** a strict catalog loader, nonempty control requirement, and exact requirement-reference validation; update the checked-in matrix from official catalog entries.
- [ ] **Step 4: Run** the focused tests and `python scripts/verify_security_release_evidence.py --help`.

### Task 2: Gate Python/JavaScript scans and protected evidence before publishing

**Files:**
- Create: `scripts/ci/verify_npm_audit.py`
- Create: `security/npm-dependency-exceptions.json`
- Modify: `scripts/ci/verify_dependency_audit.py`
- Modify: `.github/workflows/release.yml`
- Modify: `.github/workflows/security.yml`
- Modify: `.gitignore`
- Modify: `docs/operations/production-security-runbook.md`
- Test: `tests/test_production_security.py`

**Interfaces:** Python audit reports containing a skipped dependency are invalid. `verify_npm_audit.py --audit report.json` exits nonzero on a malformed, skipped, or vulnerable production report. A release obtains `PIVOT_RELEASE_EVIDENCE_PATH` from its protected environment and validates it before `Publish to PyPI`.

- [ ] **Step 1: Write failing tests** for a `skip_reason` Python dependency, an npm v2 vulnerability object, and release workflow ordering/evidence-path requirements.
- [ ] **Step 2: Run** `pytest -q tests/test_production_security.py -k "audit or release"` and confirm the new policy is not implemented.
- [ ] **Step 3: Implement** strict parsers, remove permissive scanner commands, add `npm audit --omit=dev --json`, run scanners/gates before publish, and require the non-example protected evidence file.
- [ ] **Step 4: Run** focused tests, the verifier CLIs, and inspect that every gate precedes publishing.

### Task 3: Resolve Bandit findings rather than report them

**Files:**
- Modify: findings reported by `bandit -r dash_tanstack_pivot/pivot_engine/pivot_engine`
- Modify: `tests/test_production_security.py` or narrowly associated unit tests
- Test: `tests/test_production_security.py`

**Interfaces:** production source returns no unreviewed Bandit findings; every justified `# nosec` identifies its rule and safe boundary.

- [ ] **Step 1: Capture a failing Bandit report** and add a regression where an unsafe hash / unchecked SQL identifier can be demonstrated without weakening real protections.
- [ ] **Step 2: Run** the narrow failing test and `bandit -r ...` to establish the baseline.
- [ ] **Step 3: Implement** SHA-256 substitutions and safe identifier/exception handling. Only suppress a proven false-positive with an adjacent rule-specific rationale.
- [ ] **Step 4: Run** Bandit and the related regression suite until they are clean.

### Task 4: Harden all HTTP request classes and proxy trust

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/production_security.py`
- Modify: host integration request code if needed for the CSRF header
- Modify: `docs/operations/production-security-runbook.md`
- Test: `tests/test_production_security.py`

**Interfaces:** `install_asgi_security` and `install_flask_security` call `limiter.check()` for every request; unsafe requests with cookies require same-origin and an exact configured CSRF cookie/header pair. `ProductionSecuritySettings.validate()` rejects `0.0.0.0/0` and `::/0` trusted proxies.

- [ ] **Step 1: Write failing tests** for GET exhaustion, missing/mismatched cookie CSRF, valid double-submit CSRF, and universal proxy networks.
- [ ] **Step 2: Run** `pytest -q tests/test_production_security.py -k "rate or csrf or proxy"` and confirm the insecure requests are accepted.
- [ ] **Step 3: Implement** a shared request-policy helper, CSRF cookie seeding, strict cookie-client checks, and universal-CIDR rejection for ASGI and Flask.
- [ ] **Step 4: Run** focused security and Dash callback tests to ensure supported hosts can use the documented header contract.

### Task 5: Require Flight TLS in production and complete verification

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/flight_server.py`
- Modify: `docs/operations/production-security-runbook.md`
- Test: `tests/test_flight_authorization.py`

**Interfaces:** `PivotFlightServer` raises `RuntimeError` in production unless location is `grpc+tls://...`, TLS certificates are supplied, authentication is required, and per-principal authorization dependencies exist.

- [ ] **Step 1: Write failing tests** for `grpc://`, a TLS location without certificates, and valid TLS/auth configuration.
- [ ] **Step 2: Run** `pytest -q tests/test_flight_authorization.py -k "tls or production"` and confirm plaintext construction succeeds.
- [ ] **Step 3: Implement** transport validation before `FlightServerBase` initialization and document certificate provisioning.
- [ ] **Step 4: Run** targeted tests, `pytest -q`, `npm run typecheck`, production builds, security scanners, `python -m compileall -q ...`, and `git diff --check`.

## Coverage review

- ASVS mappings/nonempty validator: Task 1.
- Scanner, release, and operator-evidence gating: Tasks 2–3.
- GET rate limits, CSRF, and proxy trust: Task 4.
- Flight transport security: Task 5.
- Deployment execution remains operator-evidence-only and is fail-closed at Task 2.
