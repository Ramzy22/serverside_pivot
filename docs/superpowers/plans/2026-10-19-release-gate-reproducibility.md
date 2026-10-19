# Reproducible Read-Only Release Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make clean-checkout read-only qualification produce and verify current-run browser evidence for installed Dash, React, Streamlit, and the pinned AG Grid reference, without treating absent evidence as a pass.

**Architecture:** Keep the reference app evaluation-only. Add a managed reference service and an adapter from its raw browser observations to the existing bound-case envelope, then run both complete mapped browser suites through the installed-artifact qualifier. Separately remove clean-checkout dependencies on local demo files, an ignored lockfile, and an ignored runbook.

**Tech Stack:** Python, pytest, Selenium/Chrome, Node/npm, TypeScript/Vite, Dash, React, Streamlit, Git-tracked release artifacts.

**Spec:** `docs/superpowers/specs/2026-10-19-release-gate-reproducibility-design.md` (supplements `docs/superpowers/specs/2026-10-13-read-only-parity-qualification-design.md`).

## Global Constraints

- Editing is excluded from the read-only release promise.
- The comparison target is AG Grid Enterprise **36.2.0** with its Server-Side Row Model; do not silently change version, fixture, or configuration.
- The reference app and its built Enterprise assets remain evaluation-only and are not shipped in the wheel or npm tarball.
- Every mandatory mapped workflow needs real current-run evidence; missing, skipped, blocked, failed, duplicate, or stale cases fail qualification.
- `--cancellable-exports` is an explicitly partial nine-case qualification, never a full release/parity pass.
- Raw screenshots, traces, parsed clipboard/downloads, and exception details remain private; public output is sanitized.
- A local runbook or verifier does not constitute production TLS, network, backup, penetration-test, or incident-response attestation.
- Do not add `node_modules`, reference `dist`, optional Yahoo demo data, or generated browser evidence to Git.

## Review Focus

- An occupied reference port must not be killed or mistaken for the service started by this run (Task 4 service test).
- A reference case with the right run ID but a stale screenshot or mismatched fixture hash must fail (Task 5 binding tests).
- A green targeted export subset must not yield a green complete read-only release (Task 7 qualifier test).
- A normal `pytest` run without a reference server must skip only live-reference cases and still execute deterministic contract tests (Task 3 tests).
- A clean checkout missing a required tracked asset must fail before qualification rather than borrow a developer-local file (Tasks 1, 2, and 8 checks).

---

## File Map and Task Boundaries

- `tests/test_drill_through.py`: deterministic Arrow-backed endpoint fixture; no production app substitution.
- `dash_tanstack_pivot/package-lock.json`: committed root component dependency resolution.
- `docs/operations/production-security-runbook.md`: committed existing operator procedure, without fabricated sign-off.
- `tests/flagship_reference_support.py` and its tests: raw reference observations and current-run envelope conversion.
- `tests/test_flagship_ag_grid_reference.py`: live-test opt-in and mapped browser assertions.
- `scripts/flagship_reference_runner.py` (new) and tests: pinned install/build, loopback lifecycle, config/fixture readiness.
- `scripts/certify_flagship.py` and tests: orchestrate complete product/reference suites and evaluate their separate JUnit/envelopes.
- `scripts/flagship_manifest.py`: retain fail-closed evaluator; change only if a new test exposes a genuine missing rejection.

### Task 1: Remove optional demo files from drill-through endpoint tests

**Files:** Modify `tests/test_drill_through.py`; test the same module.

**Interfaces:** The module-scoped `client` fixture yields the real `app.server.test_client()`. It registers a deterministic `pyarrow.Table` under `nasdaq_trader_demo_history` in the real controller, with `desk`, `strategy`, `day_pnl`, and `symbol` values sufficient for two five-row pages, `Consumer Tactical` / `Event Driven`, and `TSLA` filtering. No production application path changes.

- [ ] **Step 1: Capture RED.** Run `python -m pytest tests/test_drill_through.py -q --tb=short` in a checkout without the optional trader parquet; confirm fixture setup fails on missing demo data.
- [ ] **Step 2: Write fixture assertions.** Add a fixture-level check that the registered Arrow table has at least 11 matching root rows and the existing endpoint tests still assert pagination, sorting, text filtering, and pivot coordinates against deterministic expected rows.
- [ ] **Step 3: Replace only the fixture's eager demo load.** Use the existing controller registration API and a small inline Arrow table; patch app data loading at setup before `get_adapter()`, restore module/global state on teardown, and do not generate Yahoo data.
- [ ] **Step 4: Verify GREEN.** Run `python -m pytest tests/test_drill_through.py -q`; expect all five endpoint tests to pass with no demo files.
- [ ] **Step 5: Commit.** Commit only the test fixture change as `test: make drill-through endpoint fixture deterministic`.

### Task 2: Track the root lockfile and authored security runbook

**Files:** Add `dash_tanstack_pivot/package-lock.json`; add `docs/operations/production-security-runbook.md`; test `tests/test_packaging_smoke.py`, `tests/test_production_security.py`.

**Interfaces:** `npm ci` at `dash_tanstack_pivot/` consumes the tracked lockfile matching its `package.json`, including `react-docgen`. The security test reads the existing runbook path; the document describes operator controls but asserts no locally verified production evidence.

- [ ] **Step 1: Capture RED.** Run `python -m pytest tests/test_packaging_smoke.py::test_package_lock_matches_package_json_dev_dependencies tests/test_production_security.py::test_production_security_evidence_artifacts_cover_the_deployment_boundary -q`; confirm absent lockfile/runbook failures.
- [ ] **Step 2: Preserve the authored document.** Bring the existing main-checkout runbook into this worktree without changing claims; inspect it for honest `not_verified_locally`/operator-evidence boundaries. Do not create a replacement attestation.
- [ ] **Step 3: Generate a reproducible lockfile.** From `dash_tanstack_pivot/`, run `npm install --package-lock-only`, inspect the diff and `react-docgen` entry, then use `git add -f` for both locally excluded paths.
- [ ] **Step 4: Verify GREEN and clean install.** Run the two Python tests above and `npm ci` in `dash_tanstack_pivot/`; confirm `git ls-files` includes both paths and excludes `node_modules`.
- [ ] **Step 5: Commit.** Commit exactly the lockfile and runbook as `build: track clean-checkout release prerequisites`.

### Task 3: Make live reference tests explicit opt-in

**Files:** Modify `tests/test_flagship_ag_grid_reference.py`, `tests/test_flagship_reference_support.py`; test both modules.

**Interfaces:** `AG_GRID_REFERENCE_LIVE=1` enables live reference tests only under the managed qualification; its absence skips browser-only tests. Deterministic support tests remain runnable. A qualifier receiving skips still fails its JUnit/case-completeness gate.

- [ ] **Step 1: Write RED tests.** Assert that ordinary collection marks all `tests/test_flagship_ag_grid_reference.py` browser cases skipped without `AG_GRID_REFERENCE_LIVE`, while `test_live_browser_ui_timeout_records_failed_result` is skipped and other support-unit tests execute; assert the opt-in removes those skips.
- [ ] **Step 2: Confirm RED.** Run `python -m pytest tests/test_flagship_ag_grid_reference.py tests/test_flagship_reference_support.py -q --tb=short` without a server; expect the current connection-refused failure.
- [ ] **Step 3: Add one explicit skip gate.** Apply it at the live test module and the one live support test, not to deterministic reference helpers. Never set the opt-in in global `conftest.py`.
- [ ] **Step 4: Verify GREEN.** Repeat the command without a server; expect live cases skipped and support-unit cases passed. Check `AG_GRID_REFERENCE_LIVE=1` restores active live collection (it may fail without a managed server, as intended).
- [ ] **Step 5: Commit.** `test: gate live AG Grid reference cases explicitly`.

### Task 4: Manage the pinned reference service safely

**Files:** Create `scripts/flagship_reference_runner.py`, `tests/test_flagship_reference_runner.py`; modify `reference/ag_grid_36_2/README.md` only if its run instructions conflict.

**Interfaces:** Provide `@contextmanager start_reference_server(source_root: Path, evidence_root: Path) -> Iterator[ReferenceServer]`; `ReferenceServer` has `url: str`, `config_sha256: str`, `fixture_sha256: str`, and `versions: Mapping[str, str]`. It runs `npm ci` and the production build in `reference/ag_grid_36_2`, starts its server bound to an available loopback port via `REFERENCE_PORT`, waits for `/reference-config.json`, verifies the pinned version/config/fixture hashes, yields only after readiness, and terminates only its own process in `finally`.

- [ ] **Step 1: Write RED tests.** With a tiny fake command/server launcher, assert install/build/start order, dynamic loopback URL, exact config/fixture hashes, readiness timeout failure, child cleanup on success/failure, and non-interference with a pre-existing listener.
- [ ] **Step 2: Run `python -m pytest tests/test_flagship_reference_runner.py -q`; expect missing import/function failure.**
- [ ] **Step 3: Implement the context manager.** Keep command output in the private evidence tree; reject a mismatched `/reference-config.json` or non-36.2.0 package configuration before yielding. Do not require or modify a manually running port 4179 service.
- [ ] **Step 4: Verify GREEN.** Run the unit tests and `npm ci && npm run build` in `reference/ag_grid_36_2` using PowerShell-equivalent sequential commands; confirm generated assets remain untracked.
- [ ] **Step 5: Commit.** `test: manage pinned AG Grid reference service`.

### Task 5: Bind raw AG Grid observations to the current release

**Files:** Modify `tests/flagship_reference_support.py`; test `tests/test_flagship_reference_support.py`, `tests/test_flagship_manifest.py`.

**Interfaces:** Add `write_reference_case_envelope(raw_root: Path, output_path: Path, binding: QualificationBinding) -> None`. Read `load_case_results(raw_root, binding.run_id)`; preserve each raw result status and test ID, calculate SHA-256 for every evidence path, copy the exact `binding.run_id`, `binding.source_commit`, and complete `binding.sha256` to the envelope and each record. The output must load through existing `load_case_envelope(output_path, binding)` without relaxing the evaluator. A passing reference record still needs its live SSRM trace and fixture/config hash as the evaluator requires.

- [ ] **Step 1: Write RED tests.** A current-run raw case with observation and screenshot becomes one loadable `BoundCaseResult`; wrong run ID, missing file, changed file bytes, fixture mismatch, duplicate requirement, and `blocked`/`failed` raw cases cannot become a passing case.
- [ ] **Step 2: Run `python -m pytest tests/test_flagship_reference_support.py tests/test_flagship_manifest.py -q`; expect the new envelope tests to fail.**
- [ ] **Step 3: Implement the adapter.** Write atomically into the current run's private tree, bind but never reclassify raw statuses, and reject duplicate IDs rather than last-write-wins. Keep observation/clipboard/download payloads out of public reports.
- [ ] **Step 4: Verify GREEN.** Re-run the two modules; explicitly load the produced envelope through `load_case_envelope` and evaluate a fixture-mismatched case as failed.
- [ ] **Step 5: Commit.** `test: bind live reference evidence to release artifacts`.

### Task 6: Run every mapped AG Grid browser case in the qualifier

**Files:** Modify `scripts/certify_flagship.py`; test `tests/test_flagship_certification.py`, `tests/test_flagship_ag_grid_reference.py`.

**Interfaces:** Add `_run_reference_suite(artifact_dir: Path, environment: Mapping[str, str], junit_path: Path) -> int`, invoked inside `start_reference_server(...)` with `AG_GRID_REFERENCE_LIVE=1`, `AG_GRID_REFERENCE_URL`, `AG_GRID_REFERENCE_RUN_ID`, and `AG_GRID_REFERENCE_EVIDENCE` set to the same invocation's private tree. On exit, validate exact mapped test IDs/count, no skips/failures/errors, then call Task 5's envelope writer. Keep a separate reference JUnit path from product JUnit.

- [ ] **Step 1: Write RED orchestration tests.** Assert a managed reference run is attempted, a missing server/bad config/nonzero pytest/incomplete JUnit leaves `ag_grid` invalid and parity failed, and cleanup occurs even if pytest raises.
- [ ] **Step 2: Run those tests; confirm failure because the qualifier never starts the reference service.**
- [ ] **Step 3: Integrate the service and suite.** Select all committed AG Grid-mapped browser test IDs, not just a hand-picked subset; record reference JUnit issues separately and generate the envelope only from the just-finished raw run.
- [ ] **Step 4: Verify GREEN.** Run focused qualifier tests and the managed AG Grid live suite locally; require actual Chrome/SSRM observations. Any unavailable browser remains a qualification failure, not a substituted pass.
- [ ] **Step 5: Commit.** `test: execute current-run AG Grid reference qualification`.

### Task 7: Execute complete mapped product browser coverage

**Files:** Modify `scripts/certify_flagship.py`, `tests/test_flagship_certification.py`, `tests/test_flagship_workflows.py` only where needed to map existing browser cases; do not weaken `tests/flagship_requirements.json`.

**Interfaces:** Full `--installed-release` selects every required product browser `test_id` for Dash, React, and Streamlit from the committed contract, and checks exact parametrized JUnit identities and bound product envelope records. `--cancellable-exports` keeps its nine expected cases and explicitly reports partial coverage, never full release qualification.

- [ ] **Step 1: Write RED coverage tests.** A fake green three-case product JUnit plus a valid reference envelope is rejected for missing mapped product requirements; a skipped required host case fails; all correctly mapped case identities can pass coverage; export-only nine-case evidence still cannot pass the complete release gate.
- [ ] **Step 2: Run focused tests; confirm present subset selection fails the new full-coverage assertions.**
- [ ] **Step 3: Derive selection and expected identities from the committed requirement map.** Reuse existing browser journeys where their observed assertion matches the map; add only genuinely absent product browser tests. Never synthesize evidence or change a required mapping solely to make the gate green.
- [ ] **Step 4: Verify GREEN.** Run `python -m pytest tests/test_flagship_certification.py tests/test_flagship_workflows.py -q` and a real installed-release browser run; enumerate any still-missing or failing requirement IDs without claiming release pass.
- [ ] **Step 5: Commit.** `test: require complete installed-host browser coverage`.

### Task 8: Clean-checkout and final qualification audit

**Files:** Modify release/qualification documentation only if necessary; retain raw evidence under ignored `artifacts/`. Add a clean-checkout smoke test in `tests/test_packaging_smoke.py` only if current coverage does not detect a missing tracked prerequisite.

**Interfaces:** The final artifact manifest identifies source commit, wheel/npm/bundle/config/fixture hashes, browser and host versions, per-case results, explicit skips/failures, and `not_verified_locally` for protected operator evidence. It is not a production sign-off.

- [ ] **Step 1: Reproduce from a fresh checkout/worktree of the final commit.** Verify `git ls-files` contains the root lockfile, runbook, reference lockfile, and fixture, and no local-only inputs are required.
- [ ] **Step 2: Run deterministic gates.** Run `npm ci`, component typecheck, production build and metadata generation, pinned reference unit/build checks, and `python -m pytest -q`; record exact counts and failures.
- [ ] **Step 3: Run `python -m scripts.certify_flagship --installed-release --artifact-dir <new-private-evidence-dir>`.** Inspect both JUnit files, both bound envelopes, parsed browser outputs, hashes, and the sanitized report. A failed/blocked required case remains red and is reported by requirement ID.
- [ ] **Step 4: Review release scope.** Confirm no generated Enterprise assets or raw evidence are staged; list operator-owned TLS/network/identity/audit/restore/incident/penetration attachments still needed for production sign-off.
- [ ] **Step 5: Commit any test/doc corrections separately, then request a fresh whole-branch review.** Rerun affected checks after corrections and report exact final commit/artifact hashes; do not mark release qualified unless both browser gate and protected operator gate are genuinely satisfied.

## Self-Review

- Spec coverage: managed reference, opt-in ordinary tests, current-run binding, complete product coverage, deterministic drill fixture, tracked lockfile/runbook, sanitized reporting, and external operator boundary each have an owning task.
- Interface order: Task 4 provides `ReferenceServer`; Task 5 provides `write_reference_case_envelope`; Task 6 consumes both; Task 7 broadens product selection without changing Task 6's reference contract.
- The review-focus conditions are asserted in Tasks 1–5 and 7–8. No task treats an environmental skip or missing evidence as success.
