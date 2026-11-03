# Security Boundary Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make release evidence, runtime authorization auditability, edge validation, WebSocket identity, and formula evaluation fail closed.

**Architecture:** Release assurance stays outside runtime code but is bound to immutable workflow values. Runtime boundaries share a narrow structured security-event helper. Formula limits stay in the safe AST parser, and WebSocket identity stays server-owned.

**Tech Stack:** Python 3.11, FastAPI/Starlette, Flask/Dash, PyArrow Flight, pytest, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-18-security-boundary-closure-design.md`

## Global Constraints

- Do not log credentials, row values, raw claims, or exception text in security events.
- A release is valid only when the attestation matches its tag, source SHA, environment, origin, matrix digest, and all matrix controls.
- Local tests may prove code; operational evidence remains required for a real deployment.

---

### Task 1: Scope-bound release attestations

**Files:** `scripts/verify_security_release_evidence.py`, `.github/workflows/release.yml`, `security/release-evidence.example.json`, `tests/test_production_security.py`

- [ ] Write tests that reject mismatched tag/commit/environment/origin, matrix digest, and omitted control IDs.
- [ ] Run the focused evidence tests and observe rejection under the prior schema.
- [ ] Require the release object and scope object with exact expected command-line values; pass immutable GitHub tag/SHA and protected environment variables from the release workflow.
- [ ] Run focused tests and update the runbook/example manifest.

### Task 2: Authorization-denial events

**Files:** `pivot_engine/security_events.py`, REST/runtime/Flight/Dash boundaries, authorization tests.

- [ ] Write caplog tests for a denied REST/runtime action, asserting stable metadata and absence of sensitive data.
- [ ] Run focused tests and observe the missing event.
- [ ] Add the shared warning logger and call it only at denial boundaries.
- [ ] Run focused boundary tests.

### Task 3: Effective edge validation

**Files:** `scripts/verify_production_deployment.py`, `tests/test_production_security.py`

- [ ] Write fake-edge tests for zero HSTS, wildcard framing, unsafe referrer policy, and cross-origin redirects.
- [ ] Run them and observe the current false success.
- [ ] Parse directives and reject ineffective values.
- [ ] Run focused verifier tests.

### Task 4: Server-owned realtime identity and bounded formulas

**Files:** `complete_rest_api.py`, `planner/expression_parser.py`, realtime/formula tests.

- [ ] Write tests that prove a supplied WebSocket path token is not used as the registry key and that expensive formula shapes are rejected.
- [ ] Run focused tests and observe the current replacement/unbounded behavior.
- [ ] Generate a registry key server-side and enforce deterministic AST/power/function limits.
- [ ] Run focused tests and then the complete suite.
