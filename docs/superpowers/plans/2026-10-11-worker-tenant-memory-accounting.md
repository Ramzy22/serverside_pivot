# Worker and Tenant Memory Accounting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply one worker and one trusted accounting-tenant budget to runtime work and retained stores, while atomically bounding durable CDC state across Redis jobs.

**Architecture:** A process registry supplies the default worker governor; hosts bind a trusted workload account to each request. Leases check worker and tenant capacity together and persist for the actual lifetime of work or retained data. Local CDC uses governor leases, while Redis CDC maintains durable namespace and tenant totals in the same atomic operation as snapshot replacement.

**Tech Stack:** Python 3.11, FastAPI/Dash, DuckDB, Redis/fakeredis, pytest.

**Spec:** `docs/superpowers/specs/2026-10-11-worker-tenant-memory-accounting-design.md`

## Global Constraints

- Accounting identity comes only from authentication or an authorized host resolver; row-security claims and protocol fields cannot select it.
- A cross-tenant query charges one accounting tenant in full.
- Worker and tenant admission is one atomic operation. Rejection leaves both ledgers unchanged.
- `system` is a capped internal bucket for unassigned administrative, service and direct-engine work.
- Redis durable CDC totals and worker transient reservations are separate budgets.
- Native memory remains bounded by RSS, DuckDB and deployment controls outside application byte accounting.
- Preserve user-owned untracked `artifacts/`, `dash_tanstack_pivot/dist/`, `investigation_2026_09_14/` and `security/reports/`.

## Review Focus

- A missing or malformed accounting claim charges the capped system bucket; it never creates an unmetered tenant.
- An envelope field named `accountingTenant` cannot alter the authenticated accounting tenant.
- Replacing an entry under pressure preserves the old object and lease if new admission fails.
- Two Redis writers replacing different jobs cannot both spend the same remaining namespace or tenant bytes.
- Cleanup and recovery cannot undercount durable CDC state after job expiry or worker restart.

## File structure

| File | Responsibility |
| --- | --- |
| `resource_governor.py` | Worker/tenant limits, lease accounting, registry and safe aggregate metrics. |
| `runtime/accounting.py` | Trusted workload-account capability and context propagation. |
| `runtime/service.py`, `runtime/lifecycle.py`, `runtime/http_transport.py`, `runtime/dash_callbacks.py` | Bind account and use one governor for request admission. |
| `scalable_pivot_controller.py`, `controller.py`, `hierarchy_cache_registry.py`, `runtime/payload_store.py` | Inject one governor and retain charged cache/payload entries. |
| `streaming/exact_state.py`, `streaming/streaming_processor.py` | Local aggregate CDC leases, Redis atomic durable totals and transient reservations. |
| `admin_api.py`, `docs/OPERATIONS.md` | Publish safe aggregate metrics and configuration. |

### Task 1: Worker registry and tenant-aware leases

**Files:** `resource_governor.py`, `admin_api.py`, `tests/test_resource_governor.py`.

**Interfaces:** Produce `get_worker_resource_governor() -> ResourceGovernor`, `WorkloadAccount`, `ResourceGovernor.reserve(kind, estimated_bytes, *, account=SYSTEM_ACCOUNT)`, and lease `resize(new_bytes)`/`release()`.

- [ ] Write failing tests: two default resolutions return the same governor; tenant A exhaustion does not consume B capacity; worker exhaustion still rejects B; slot and byte rejection leave both ledgers unchanged; resize shrink/release return both capacities; system bucket is capped; snapshot has aggregate tenant metrics without tenant IDs.
- [ ] Run `python -m pytest tests/test_resource_governor.py -q` and verify the new cases fail for missing behavior.
- [ ] Implement one process registry, validated tenant/system byte and slot configuration, atomic worker+tenant checks, lease resize, and aggregate metrics. Explicit test governors remain injectable. No tenant IDs appear in snapshots or errors.
- [ ] Re-run the module and commit `feat: add worker and tenant resource accounting`.

### Task 2: Trusted request identity and shared runtime ownership

**Files:** `runtime/accounting.py`, `runtime/models.py`, `runtime/service.py`, `runtime/lifecycle.py`, `runtime/http_transport.py`, `runtime/dash_callbacks.py`, `dash_integration.py`, `complete_rest_api.py`, `tests/test_runtime_resource_admission.py`, `tests/test_runtime_http_transport.py`.

**Interfaces:** Consume Task 1's governor and account; produce a server-owned `PivotRequestContext.accounting_account` and an account context used by cache/payload writes. A host resolver may return an authorized account only after authentication.

- [ ] Write failing tests: two default services share one governor and competing query slots; trusted principal account is charged; an envelope tenant field cannot change the charge; an admin without an account uses the capped system bucket; a multi-data-tenant row scope still charges one account; cancellation retains both leases through native cleanup.
- [ ] Run the focused runtime modules and verify these cases fail.
- [ ] Resolve trusted account at the host boundary, carry it through request context, pass it to lifecycle admission, and use the worker registry for every default service/fallback. Reject a composed adapter/controller bound to a different explicit governor.
- [ ] Run focused tests and commit `feat: bind trusted workload accounts to runtime admission`.

### Task 3: Controller caches and payload retention

**Files:** `controller.py`, `scalable_pivot_controller.py`, `hierarchy_cache_registry.py`, `cache/memory_cache.py`, `runtime/payload_store.py`, `tanstack_adapter.py`, `tests/test_hierarchy_cache_registry.py`, `tests/test_memory_cache_budgets.py`, `tests/test_runtime_payload_store.py`.

**Interfaces:** Consume Task 1's governor and Task 2's account context. All local retained bytes use a `CACHE` or `PAYLOAD` lease for their lifetime; background work uses `SYSTEM_ACCOUNT`.

- [ ] Write failing tests: query and hierarchy caches on two controllers share the worker cache budget; tenant cache entries keep their original charge after another tenant reads them; replacement failure keeps the old entry; eviction/expiry/clear release the charge; two payload stores share the same budget and tenant caps.
- [ ] Run focused cache/payload modules and verify the new cases fail.
- [ ] Inject the same governor into base and hierarchy caches and local payload stores, charge on retention, preserve old entries when replacement admission fails, and release on every removal path.
- [ ] Run focused tests and commit `fix: charge controller and payload retention to worker budgets`.

### Task 4: Aggregate local CDC state and transient reservations

**Files:** `streaming/exact_state.py`, `streaming/streaming_processor.py`, `scalable_pivot_controller.py`, `tests/test_exact_cdc_state.py`, `tests/test_streaming_incremental.py`.

**Interfaces:** Consume Task 1's `CDC` lease and account. `MemoryCDCStateStore` charges all retained snapshots across jobs; both stores reserve local clone/serialization memory separately from durable bytes.

- [ ] Write failing tests: two individually legal jobs exceed the aggregate CDC budget; CAS growth/shrink changes the charge by the size delta; a conflict or rejection keeps the previous snapshot/charge; deletion/expiry release it; transient load/clone/serialization can reject even when Redis has durable capacity.
- [ ] Run the focused CDC modules and verify the new cases fail.
- [ ] Add aggregate local state leases, safe replacement and delete/expire methods, worker transient reservations, and processor invalidation on budget exhaustion.
- [ ] Run focused tests and commit `fix: bound aggregate local CDC state`.

### Task 5: Atomic Redis CDC totals, lifecycle and recovery

**Files:** `streaming/exact_state.py`, `runtime/distributed.py` if a namespaced key helper is required, `tests/test_exact_cdc_state.py`, `tests/test_streaming_incremental.py`.

**Interfaces:** Consume trusted account and `CDCStateLimits`; produce atomic compare-and-swap over snapshot, owner/size metadata, namespace total, tenant total and expiry index. `PIVOT_CDC_TENANT_MAX_BYTES` defaults to 64 MiB; namespace ceiling is `PIVOT_CDC_MAX_BYTES`.

- [ ] Write failing fakeredis tests for multi-job aggregate rejection, tenant isolation, conflicting CAS, concurrent writers, replacement growth/shrink, deletion, configured expiry, recreated store recovery, and inconsistent metadata failing closed.
- [ ] Run focused Redis CDC tests and verify the new cases fail.
- [ ] Implement one Redis atomic transaction/script for writes, delete and indexed expiry cleanup. State keys never expire independently. Reconciliation validates indexed metadata and reconstructs totals atomically before admitting writes; owner changes are rejected.
- [ ] Run focused tests and commit `fix: account Redis CDC jobs atomically`.

### Task 6: Operations contract and integrated verification

**Files:** `docs/OPERATIONS.md`, `tests/test_docs_examples_contract.py`, `tests/test_memory_protection_benchmark.py`, `scripts/benchmark_pivot_runtime.py` if needed for a two-tenant workload.

**Interfaces:** Consume the completed accounting interfaces and publish exact default/env policy and the per-worker versus Redis durable distinction.

- [ ] Write failing documentation and benchmark contract tests for the new policy and a two-tenant admission workload.
- [ ] Run them to verify the missing contract fails; document configuration, limits, metrics and failure behavior; extend the benchmark only if its existing workload cannot demonstrate tenant rejection and aggregate CDC pressure.
- [ ] Run focused tests, `python -m pytest -q`, `bandit -r dash_tanstack_pivot/pivot_engine/pivot_engine`, and the opt-in memory benchmark under a stated RSS ceiling. Commit `docs: qualify worker and tenant memory accounting`.

## Self-review

Each spec requirement maps to Tasks 1-6. Tasks 2-5 consume the account and governor interfaces from Task 1; Task 3 also consumes Task 2's trusted context. Local CDC and Redis CDC are separate reviewable persistence changes. The review focus is covered by named failing tests. The full suite and benchmark supply integrated evidence after the final task.
