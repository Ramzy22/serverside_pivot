# Cancellable Large Exports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make large CSV/XLSX export progress truthful and cancellation server-acknowledged in Dash, React, and Streamlit, with optional Redis cross-worker coordination and safe interrupted-retry after restart.

**Architecture:** Add `start/status/cancel` actions to the existing V1 export command. A bounded worker-local executor owns native query cancellation, temporary output, and one export admission lease. A small job registry is local by default and Redis-backed only in distributed mode; completed files still use the authenticated payload store.

**Tech Stack:** Python/FastAPI/Flask, Redis optional, React/TypeScript and checked JavaScript, pytest, Node tests, Selenium/Chromium.

**Spec:** `docs/superpowers/specs/2026-10-18-cancellable-large-exports-design.md`

## Global Constraints

- Keep `schemaVersion: 1`; do not create a second runtime protocol or a host-specific export implementation.
- Redis is optional for one worker and required only when cross-worker operation is enabled. S3-compatible storage is deferred and optional.
- A worker restart means `interrupted` and fresh retry, not resumption.
- Do not report `cancelled` or release an export slot before native execution and writer cleanup complete.
- Retain dataset/subject authorization, trusted accounting account, row/byte budgets, authenticated payload-token delivery, and CSV/XLSX fidelity.
- No fabricated percent, partial downloadable output, or successful response after cancellation wins the terminal-state race.

## Review Focus

1. Cancel immediately after Start, before the worker claims the job: Task 2 test must prove no execution starts and admission is released exactly once.
2. Cancel versus successful publish at the same instant: Task 3 test must prove one terminal state and no published payload after cancellation wins.
3. Cross-tenant guessed job ID: Task 4 test must prove status/cancel reveal neither table nor progress and emit authorization-denial evidence.
4. Native interruption unavailable or stuck: Task 3 test must prove `cancelling` stays visible and reservation stays charged until actual cleanup; no false acknowledgement.
5. Redis lease loss while an old owner is still running: Task 2 test must prove a fenced old owner cannot publish success or overwrite `interrupted`.

---

### Task 1: V1 export job contract and state machine

**Files:**
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/export_jobs.py` (immutable job snapshot and transition rules only)
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/runtime-contract.schema.json`, `runtime-response.schema.json`, `contracts.py`
- Regenerate: `dash_tanstack_pivot/src/lib/runtime/runtime-contract.generated.ts`, `runtime-response-contract.generated.ts`
- Modify: `dash_tanstack_pivot/src/lib/runtime/runtimeContract.ts`
- Test: `tests/test_runtime_export_jobs.py`, `tests/test_runtime_contracts.py`

**Interfaces:** `ExportJobSnapshot(job_id, subject, accounting_account_id, table, phase, rows_written, bytes_written, total_rows, owner_epoch, payload_ref)`; `transition_export_job(snapshot, event) -> ExportJobSnapshot`. `accounting_account_id` is resolved from the authenticated principal, never copied from the envelope. V1 `kind="export"` payload action `start|status|cancel`, with `jobId` required for status/cancel and server-generated for start. Job response payload includes `jobId`, `phase`, observed rows/bytes, nullable total, and `payloadRef` only after completion.

- [ ] Write tests for accepted/rejected V1 action envelopes, omitted/forged job IDs, and transitions `queued→running→cancelling→cancelled`, `running→completed|failed|interrupted`; assert terminal states cannot be overwritten, observed counters never fall, unknown total stays null.
- [ ] Run `python -m pytest tests/test_runtime_export_jobs.py tests/test_runtime_contracts.py -q`; verify RED for missing action/state model.
- [ ] Add the model and schema/codec changes, regenerate TS types with `npm run generate:runtime-contract-types` from `dash_tanstack_pivot`, and update the hand-written command union without widening unrelated kinds.
- [ ] Rerun the two Python files and `npm run typecheck`; require exit 0.
- [ ] Commit only Task 1 files as `feat: define V1 export job lifecycle`.

### Task 2: Local and Redis job registry with fencing

**Files:**
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/export_job_store.py` (interface and local implementation)
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/redis_export_job_store.py` (atomic Redis implementation)
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/distributed.py` (namespaced store construction)
- Test: `tests/test_runtime_export_job_store.py`, `tests/test_distributed_runtime.py`

**Interfaces:** `ExportJobStore.create(snapshot)`, `get(job_id, subject, table)`, `claim(job_id, owner_id)`, `request_cancel(job_id)`, `advance(job_id, owner_epoch, event)`, `heartbeat(job_id, owner_epoch)`, `interrupt_expired(now)`. Unknown and unauthorized lookups have the same public result; owner epoch fences all worker writes. Local and Redis stores expose identical behavior.

- [ ] Write local-store tests for create/claim, replay, monotonic progress, cancel-before-claim, cancel/completion race, TTL, and exact-once terminal transition; write fakeredis tests for cross-instance cancel, atomic owner fencing, lease expiry/restart to `interrupted`, and Redis outage fail-closed.
- [ ] Run `python -m pytest tests/test_runtime_export_job_store.py tests/test_distributed_runtime.py -q`; verify RED for missing stores.
- [ ] Implement local and Redis stores using JSON-only state and atomic CAS/Lua where a transition crosses multiple Redis keys; do not store exported bytes in job records.
- [ ] Rerun those tests and the existing distributed-runtime suite; require exit 0.
- [ ] Commit only Task 2 files as `feat: coordinate export jobs across workers`.

### Task 3: Bounded execution, native cancellation, and output publication

**Files:**
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/export_job_runner.py` (executor, progress, cancellation, cleanup)
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/lifecycle.py`, `router.py`, `export_service.py`, `payload_store.py`
- Test: `tests/test_runtime_export_job_runner.py`, `tests/test_database_cancellation.py`, `tests/test_export_qualification_runtime.py`, `tests/test_bounded_export_streaming.py`

**Interfaces:** `ExportJobRunner.start(job, context, state) -> None`, `signal_cancel(job_id) -> None`, `close() -> None`; one trusted `ResourceLease` is acquired before scheduling and passed through the export lifecycle without a second reservation. The runner owns lease release after native query, writer, temp-file, and publication cleanup. `publish_completed(job_id, content_path, metadata)` stages the authenticated payload reference, then atomically wins the fenced transition to `completed`; if cancellation wins or registration fails, it revokes the staged reference and removes output.

- [ ] Write a slow real-DuckDB export test: Start holds one export slot charged to the server-resolved accounting account; Cancel from another thread/worker enters `cancelling`; native query stops; connection is reusable; temp file is gone; slot releases only after execution ends; a smaller retry succeeds. Add tests for cancel-before-claim, writer-batch cancellation, cancellation refusal, stop/restart orphan cleanup, byte/row cap, CSV/XLSX value/header fidelity, and cancel/publish race.
- [ ] Run the four targeted Python files; verify RED for missing runner and lease handoff.
- [ ] Implement a bounded executor and dedicated temp directory. Reuse existing native cancellation and file-format writers; instrument measured rows/bytes, with unknown totals remaining indeterminate. Add a lifecycle path accepting the runner-owned export lease so admission is not double-counted. Stage a completed payload, commit the fenced `completed` transition, then expose its reference; revoke it if the transition loses, and clean failures/cancellation in `finally`.
- [ ] Rerun target files plus `tests/test_runtime_resource_admission.py`; require exit 0 and observed slot/byte lifetime assertions.
- [ ] Commit only Task 3 files as `feat: execute cancellable bounded exports`.

### Task 4: Authenticated host-neutral gateway

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py`, `dash_callbacks.py`
- Test: `tests/test_runtime_http_transport.py`, `tests/test_runtime_dataset_authorization.py`, `tests/test_runtime_payload_store.py`

**Interfaces:** `RuntimeHttpGateway.process()` handles V1 export start/status/cancel through the store/runner. All actions use `RuntimeDatasetAuthorizer(..., operation="export")`, server-resolved subject/account, and one public response shape; Dash callback forwards the same envelope semantics. Existing synchronous small export remains compatible until the shared frontend switches.

- [ ] Write FastAPI and Dash-boundary tests for authenticated start/status/cancel, a different worker handling cancel, guessed cross-subject/tenant job IDs, alternate table, expired job, Redis outage, and completed payload retrieval; require authorization denial to be logged without leaking job metadata.
- [ ] Run the three targeted files; verify RED for missing gateway action dispatch.
- [ ] Wire gateway and Dash callback to store/runner; status and cancel must never regenerate a view or trust browser-supplied tenant/account values. Map resource exhaustion and missing IDs to stable V1 errors.
- [ ] Rerun target files plus `tests/test_runtime_contracts.py`; require exit 0.
- [ ] Commit only Task 4 files as `feat: expose authenticated export job controls`.

### Task 5: Shared frontend job flow and truthful overlay

**Files:**
- Modify: `dash_tanstack_pivot/src/lib/runtime/createRuntimeExportCommands.js`, `useRuntimeResponseLifecycle.js`, `useRuntimeExportState.js`, `useViewportRuntimeStage.tsx`, `runtimeDomains.ts`
- Modify: `dash_tanstack_pivot/src/lib/components/PivotWorkspaceShell.js`
- Test: `tests/frontend_export_part_lifecycle.cjs`, new `tests/frontend_export_job_lifecycle.cjs`, `tests/test_frontend_contract.py`

**Interfaces:** Shared commands `startExportJob(format, scope)`, `pollExportJob(jobId)`, `cancelExportJob(jobId)` dispatch V1 export actions via the existing host transport port. Presentation state is `queued|running|cancelling|cancelled|completed|failed|interrupted`; the terminal completed response alone may trigger one download. Polling timers and in-flight requests are cleaned on unmount and retry.

- [ ] Add failing Node tests for real request envelopes, monotonic measured progress, unknown-total indeterminate UI, Cancel→Cancelling→Cancelled acknowledgement, cancel/completed race, late-response suppression, interrupted retry, no duplicate download, and current-table/part-export compatibility.
- [ ] Run `node tests/frontend_export_job_lifecycle.cjs` and existing frontend export tests; verify RED.
- [ ] Implement the shared command/state machine and overlay; reuse the current transport dispatch so Dash, React, and Streamlit do not diverge. Do not change copy behavior unless its separate size threshold requires the same server cleanup guarantee.
- [ ] Rerun Node tests and `npm run typecheck` under `dash_tanstack_pivot`; require exit 0.
- [ ] Commit only Task 5 files as `feat: show server-acknowledged export cancellation`.

### Task 6: Installed-host qualification and release contract

**Files:**
- Modify: `tests/flagship_browser_support.py`, `tests/test_flagship_workflows.py`, `tests/flagship_requirements.json`
- Modify: `docs/flagship-functionality-certification.md`, `docs/operations/distributed-runtime.md`
- Test: `tests/test_flagship_contract.py`, `tests/test_flagship_manifest.py`

**Interfaces:** Produce bound `browser.large-delivery.progress`, `browser.large-delivery.cancel-retry`, and `browser.journey-recovery.cancel-new-work` cases for each host. Observations include request/job correlation, measured progress, cancellation acknowledgement, executor/lease lifetime, no partial download, and successful numeric retry; a skipped/blocked case never passes the release gate.

- [ ] Add failing installed-host browser journeys for Dash, React, and Streamlit using a deterministic slow export. Check status/network evidence, visible phases, exact parsed CSV/XLSX output after retry, cleanup metrics, and absence of cancelled file; add multi-process Redis kill/restart/other-worker cancel integration test. Bind source commit, wheel/npm hashes, browser version, and private evidence.
- [ ] Run the targeted browser journeys with the installed-artifact runner; verify RED against the prior UI-only Cancel path.
- [ ] Complete any narrowly identified host transport/fixture changes, update the contract and operational docs with single-worker vs optional Redis deployment rules and deferred S3 extension, and preserve fail-closed manifest semantics.
- [ ] Commit Task 6 files as `test: qualify cancellable exports in all hosts` before the final installed-artifact run.
- [ ] Rerun Node/typecheck, targeted Python suites, production builds/metadata, and the three-host installed browser journeys against that exact commit. If any fix requires another commit, rerun against the new commit. Run the full Python suite and report unrelated failures explicitly; do not call the release gate green unless all required cases pass.

## Self-review / release decision

Every spec requirement maps to a task: protocol/auth (1,4), local/Redis coordination and restart (2), execution/progress/budgets/cleanup (3), shared hosts (5), browser/release evidence (6), future S3 boundary (1,2,6 docs only). No S3 implementation, editing, or resumable export is smuggled into this plan. A final independent review must verify no cancellation acknowledgement or governor release precedes native cleanup, and no pass is inferred from a browser-only overlay.
