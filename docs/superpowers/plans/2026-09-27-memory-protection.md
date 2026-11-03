# Memory Protection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound per-worker application memory and concurrent work so overload is rejected predictably rather than exhausting a worker.

**Architecture:** A process-local `ResourceGovernor` owns fail-fast slots and accountable-byte leases. Caches, payloads, runtime work, exports, and CDC state use narrow governor interfaces. DuckDB allocation limits and deployment worker limits remain independent safeguards because Python cannot account for all native memory.

**Tech Stack:** Python 3.11, asyncio, threading, PyArrow, DuckDB, Ibis, Redis runtime state, FastAPI/Dash runtime, psutil, pytest.

**Spec:** `docs/superpowers/specs/2026-09-27-memory-protection-design.md`

## Global Constraints

- Accounted application bytes are not a claim about all native DuckDB, Arrow, CPython, allocator, or kernel memory.
- Local defaults: 1 GiB total, 256 MiB headroom/cache/query, 128 MiB payload/CDC, 32 MiB export batch, and 8 MiB download buffer.
- Production explicitly configures all budgets, worker hard/soft limits, DuckDB memory limit, and deployment hard memory limit.
- Capacity exhaustion is fail-fast: `resource_exhausted`, bounded retry metadata, no queue, and no sensitive response data.
- Release each resource lease after success, timeout, error, cancellation, and database cancellation cleanup.
- Do not cache unsized values; reject a single oversize item rather than evict unrelated values merely to admit it.
- Full exports require record-batch streaming. Reject a backend without a bounded path before materializing a full result.
- CDC state over budget invalidates as `cdc_state_budget_exceeded` and recomputes authoritatively; it never returns approximate data.
- Admission is worker-global and retains existing tenant authorization. Tenant quotas and cross-worker native-memory accounting are non-goals.

## Review Focus

- Cancellation racing admission holds a query lease until database cleanup; Task 3 tests it.
- Cache-key replacement credits the old size exactly once; Task 2 tests it.
- Redis payload expiry/delete cannot leak or make namespace bytes negative; Task 2 tests it.
- A full-table-only export backend is rejected before invocation; Task 4 tests it.
- Low-group CDC state with unbounded distinct extrema invalidates at its extrema limit; Task 5 tests it.

---

### Task 1: Resource-governor primitives and worker-memory policy

**Files:**
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/resource_governor.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/config.py`
- Modify: `dash_tanstack_pivot/pivot_engine/requirements.txt`
- Test: `tests/test_resource_governor.py`

**Interfaces:**
- Consumes: positive environment parsing from `pivot_engine/export_limits.py` and `psutil` RSS readings.
- Produces: `ResourceKind`, `ResourceLimits`, `ResourceGovernor`, `ResourceLease`, `ResourceExhaustedError`, `estimate_value_bytes`, and `rss_bytes` for Tasks 2–6.

- [ ] **Step 1: Write failing governor tests**

```python
def test_resource_governor_rejects_exhausted_bytes_or_slots():
    governor = ResourceGovernor(ResourceLimits.for_testing(total_bytes=100, headroom_bytes=10, query_bytes=80, max_queries=1))
    lease = governor.reserve(ResourceKind.QUERY, 80)
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        governor.reserve(ResourceKind.QUERY, 1)
    lease.release()
    assert governor.reserve(ResourceKind.QUERY, 80).kind is ResourceKind.QUERY
```

Test defaults, malformed values, RSS soft-limit rejection, idempotent release,
lease growth, metrics, Arrow/bytes/text sizing, unsupported objects returning
`None`, and an inconsistent DuckDB/worker envelope.

- [ ] **Step 2: Run RED verification**

Run: `pytest tests/test_resource_governor.py -v`

Expected: FAIL because `pivot_engine.resource_governor` does not exist.

- [ ] **Step 3: Implement the isolated resource policy**

```python
class ResourceExhaustedError(RuntimeError):
    code = "resource_exhausted"
    resource_kind: ResourceKind
    retry_after_ms: int

class ResourceKind(StrEnum):
    CACHE = "cache"; PAYLOAD = "payload"; QUERY = "query"; EXPORT = "export"; DOWNLOAD = "download"; CDC = "cdc"

@dataclass(frozen=True)
class ResourceLimits:
    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> "ResourceLimits": ...
    @classmethod
    def for_testing(cls, **overrides: int) -> "ResourceLimits": ...

class ResourceLease:
    kind: ResourceKind
    reserved_bytes: int
    def grow(self, additional_bytes: int) -> None: ...
    def release(self) -> None: ...

class ResourceGovernor:
    def __init__(self, limits: ResourceLimits, *, rss_bytes_getter: Callable[[], int] = rss_bytes) -> None: ...
    def reserve(self, kind: ResourceKind, estimated_bytes: int) -> ResourceLease: ...
    def snapshot(self) -> dict[str, Any]: ...
```

Use one `threading.RLock` shared by cache and asyncio callers. Slots are
fail-fast counters, not queues. `grow()` validates global/class capacity
before changing charge. Declare `psutil>=5.9` if absent and expose policy
fields from `ScalablePivotConfig` without a second environment parser.

- [ ] **Step 4: Run focused verification**

Run: `pytest tests/test_resource_governor.py tests/test_config_main.py tests/test_config_from_env.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add dash_tanstack_pivot/pivot_engine/pivot_engine/resource_governor.py dash_tanstack_pivot/pivot_engine/pivot_engine/config.py dash_tanstack_pivot/pivot_engine/requirements.txt tests/test_resource_governor.py tests/test_config_main.py tests/test_config_from_env.py && git commit -m "feat: add runtime resource governor"`

### Task 2: Byte-bound cache and payload residency

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/cache/memory_cache.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/hierarchy_cache_registry.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/payload_store.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/distributed.py`
- Test: `tests/test_memory_cache_budgets.py`
- Test: `tests/test_runtime_payload_store.py`
- Test: `tests/test_hierarchy_cache_registry.py`

**Interfaces:**
- Consumes: Task 1 `ResourceGovernor.reserve`, `ResourceLease.release`, and `estimate_value_bytes`.
- Produces: exact resident-byte cache/payload behavior for Tasks 3 and 6.

- [ ] **Step 1: Write failing cache/payload tests**

```python
def test_memory_cache_evicts_lru_by_total_bytes_and_rejects_one_oversize_value():
    cache = MemoryCache(max_size=10, max_bytes=10)
    assert cache.set("old", b"123456") is True
    assert cache.set("new", b"abcdef") is True
    assert cache.get("old") is None
    assert cache.set("oversize", b"01234567890") is False

def test_payload_store_rejects_a_single_value_larger_than_total_budget():
    store = RuntimePayloadStore(max_entries=4, max_bytes=8)
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        store.put_bytes(b"123456789")
```

Add tests for cache replacement, expiry/delete/clear release, Arrow sizing,
unsized values, file metadata charging, and two Redis backend instances
contending for one atomic aggregate counter.

- [ ] **Step 2: Run RED verification**

Run: `pytest tests/test_memory_cache_budgets.py tests/test_runtime_payload_store.py -v`

Expected: FAIL because cache writes have no byte result and one local payload
can exceed its budget.

- [ ] **Step 3: Implement byte-aware stores**

Add keyword-only `max_bytes` and `governor` to `MemoryCache`; make
`set(key, value, ttl=None) -> bool`. Store each value's charge with its expiry
and release its old lease exactly once on replace, expiry, eviction, delete,
and clear. Rework hierarchy cache write helpers and namespace snapshots to use
the same sizing/leases while retaining OrderedDict compatibility aliases.

Reject an over-budget payload before insertion. Add
`payload_total_max_bytes` to distributed settings. `RedisPayloadBackend` must
atomically reserve/release the namespace counter during write, delete, and
expiry reconciliation; capacity failure raises `ResourceExhaustedError`.

- [ ] **Step 4: Run affected suites**

Run: `pytest tests/test_memory_cache_budgets.py tests/test_runtime_payload_store.py tests/test_hierarchy_cache_registry.py tests/test_cache_coordinator.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add dash_tanstack_pivot/pivot_engine/pivot_engine/cache/memory_cache.py dash_tanstack_pivot/pivot_engine/pivot_engine/hierarchy_cache_registry.py dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/payload_store.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/distributed.py tests/test_memory_cache_budgets.py tests/test_runtime_payload_store.py tests/test_hierarchy_cache_registry.py tests/test_cache_coordinator.py && git commit -m "feat: bound cache and payload memory"`

### Task 3: Runtime admission, overload responses, and cancellation release

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/lifecycle.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/contracts.py`
- Test: `tests/test_runtime_resource_admission.py`
- Test: `tests/test_runtime_http_transport.py`
- Test: `tests/test_runtime_service.py`

**Interfaces:**
- Consumes: Task 1 governor and Task 2 byte-bounded payload references.
- Produces: `RuntimeRequestLifecycle.run_backend_operation(..., resource_kind: ResourceKind, reservation_bytes: int)` and safe public overload responses for Task 4.

- [ ] **Step 1: Write failing runtime-admission tests**

```python
@pytest.mark.asyncio
async def test_runtime_rejects_a_second_query_when_shared_capacity_is_full():
    governor = ResourceGovernor(ResourceLimits.for_testing(max_queries=1, query_bytes=64))
    lifecycle = RuntimeRequestLifecycle(resource_governor=governor)
    first = asyncio.create_task(lifecycle.run_backend_operation(context, "data", blocking_operation, resource_kind=ResourceKind.QUERY, reservation_bytes=64))
    await started.wait()
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        await lifecycle.run_backend_operation(context, "data", immediate_operation, resource_kind=ResourceKind.QUERY, reservation_bytes=1)
    release.set()
    await first
```

Test a 503 HTTP response with `Retry-After` and stable code, RSS soft-limit
rejection, non-tripping circuit breaker, and cancellation/native-timeout
release only after database cleanup completes.

- [ ] **Step 2: Run RED verification**

Run: `pytest tests/test_runtime_resource_admission.py tests/test_runtime_http_transport.py -v`

Expected: FAIL because lifecycle has no governor lease and HTTP has no
overload mapping.

- [ ] **Step 3: Wire shared runtime admission**

Construct one governor in `PivotRuntimeService`; inject it into lifecycle,
router/export service, and payload-store construction. Extend
`run_backend_operation` with keyword-only `resource_kind` and
`reservation_bytes`, acquiring before operation invocation and releasing in
the outermost `finally` after native cancellation cleanup.

Map `ResourceExhaustedError` to a V1 response containing only code, resource
class, and bounded retry milliseconds, then to HTTP 503 plus `Retry-After`.
Route data/detail/chart/drill/filter through `QUERY`, export through `EXPORT`,
and payload retrieval through `DOWNLOAD`. Do not record this deterministic
rejection as a circuit-breaker failure.

- [ ] **Step 4: Run runtime regressions**

Run: `pytest tests/test_runtime_resource_admission.py tests/test_runtime_capabilities.py tests/test_runtime_http_transport.py tests/test_runtime_service.py tests/test_database_cancellation.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/lifecycle.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/contracts.py tests/test_runtime_resource_admission.py tests/test_runtime_http_transport.py tests/test_runtime_service.py tests/test_runtime_capabilities.py && git commit -m "feat: admit runtime work by resource budget"`

### Task 4: Bounded record-batch export paths

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/backends/ibis_backend.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/export_executor.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/export_service.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py`
- Test: `tests/test_bounded_export_streaming.py`
- Test: `tests/test_export.py`
- Test: `tests/test_runtime_service.py`

**Interfaces:**
- Consumes: Task 3 `ResourceLease` for `EXPORT` and `ExportLimits`.
- Produces: `execute_record_batches(query, *, max_batch_bytes, max_batch_rows)` and a runtime streamed-export writer without a full-table fallback.

- [ ] **Step 1: Write failing bounded-export tests**

```python
@pytest.mark.asyncio
async def test_export_writes_each_batch_without_full_table_materialization():
    payload = await export_service.build_streamed_export([[{"region": "A", "sales": 1}], [{"region": "B", "sales": 2}]], state, {"format": "csv"})
    assert payload["rows"] == 2
    assert governor.snapshot()["classes"]["export"]["reservedBytes"] == 0

@pytest.mark.asyncio
async def test_full_table_only_export_is_rejected_before_full_execute():
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        await executor.stream(spec, export_format="csv")
    backend.execute.assert_not_called()
```

Cover temporary-file cleanup on over-budget output, Ibis/DuckDB byte/row batch
ceilings, and existing CSV/TSV/HTML/XLSX header/style compatibility.

- [ ] **Step 2: Run RED verification**

Run: `pytest tests/test_bounded_export_streaming.py -v`

Expected: FAIL because Ibis can slice a completed table and runtime exports
consume materialized `TanStackResponse.data`.

- [ ] **Step 3: Implement streaming-only large exports**

Implement `execute_record_batches` via native driver batch APIs; it must not
call full `execute()->Table` then slice. Preserve `execute_streaming` as a
compatibility wrapper.

Implement `RuntimeExportService.build_streamed_export` to write a single batch
at a time, reserve only bounded buffer bytes, and validate cumulative output
before the next batch. Route all-record/full export through this capability.
For adapters without it, return `resource_exhausted` before full response
execution. Current-table/viewport export retains an explicit materialization
ceiling before presentation projection.

- [ ] **Step 4: Run export/runtime regressions**

Run: `pytest tests/test_bounded_export_streaming.py tests/test_export.py tests/test_runtime_service.py tests/test_runtime_http_transport.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add dash_tanstack_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py dash_tanstack_pivot/pivot_engine/pivot_engine/backends/ibis_backend.py dash_tanstack_pivot/pivot_engine/pivot_engine/export_executor.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/export_service.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py tests/test_bounded_export_streaming.py tests/test_export.py tests/test_runtime_service.py && git commit -m "feat: stream exports within memory budgets"`

### Task 5: Bounded exact CDC state with authoritative invalidation

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/exact_state.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/streaming_processor.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py`
- Test: `tests/test_exact_cdc_state.py`
- Test: `tests/test_streaming_incremental.py`
- Test: `tests/test_cdc.py`

**Interfaces:**
- Consumes: Task 1 policy values and current exact snapshot persistence boundaries.
- Produces: `CDCStateLimits`, `CDCStateBudgetExceeded`, and `cdc_state_budget_exceeded` invalidation.

- [ ] **Step 1: Write failing CDC-budget tests**

```python
@pytest.mark.asyncio
async def test_exact_cdc_invalidates_when_a_new_group_exceeds_its_budget():
    processor = StreamAggregationProcessor(cdc_state_limits=CDCStateLimits(max_groups=1, max_bytes=4096, max_extrema_values=10))
    await processor.bootstrap_job("job", [{"region": "A", "sales": 1}], checkpoints={"0": (0, "bootstrap")})
    assert await processor.process_change(insert_change("B", 2)) == {"exact": 0, "invalidated": 1, "ignored": 0}
    assert (await processor.get_job_result("job")).reason == "cdc_state_budget_exceeded"
```

Cover bootstrap, updates, duplicate extrema, encoded memory/Redis overflow,
CAS conflicts, replays, and no persisted partial state.

- [ ] **Step 2: Run RED verification**

Run: `pytest tests/test_exact_cdc_state.py tests/test_streaming_incremental.py -v`

Expected: FAIL because state has no group, extrema, or encoded-size budget.

- [ ] **Step 3: Enforce exact state limits**

```python
@dataclass(frozen=True)
class CDCStateLimits:
    max_groups: int
    max_bytes: int
    max_extrema_values: int

class CDCStateBudgetExceeded(ExactCDCStateError):
    code = "cdc_state_budget_exceeded"
```

Check new group keys/new encoded extrema before mutating a clone; validate
encoded size in memory and Redis `compare_and_swap`. Catch only
`CDCStateBudgetExceeded` in bootstrap/change processing, persist an
invalidated snapshot with the stable reason, and route existing controller
refresh to authoritative recomputation. Keep current reasons for all other
exactness failures.

- [ ] **Step 4: Run CDC regressions**

Run: `pytest tests/test_exact_cdc_state.py tests/test_streaming_incremental.py tests/test_cdc.py tests/test_config_from_env.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/exact_state.py dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/streaming_processor.py dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py tests/test_exact_cdc_state.py tests/test_streaming_incremental.py tests/test_cdc.py tests/test_config_from_env.py && git commit -m "feat: bound exact CDC state growth"`

### Task 6: Native-memory validation, operations, and workload evidence

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/backends/ibis_backend.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/admin_api.py`
- Modify: `scripts/benchmark_pivot_runtime.py`
- Modify: `tests/test_performance_benchmark.py`
- Create: `tests/test_memory_protection_benchmark.py`
- Modify: `docs/OPERATIONS.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: Tasks 1–5 resource metrics and safeguards.
- Produces: DuckDB/worker envelope validation and opt-in 500k/1M RSS evidence.

- [ ] **Step 1: Write failing native-limit and benchmark tests**

```python
def test_duckdb_rejects_limit_outside_declared_worker_envelope():
    with pytest.raises(ValueError, match="worker memory"):
        DuckDBBackend(":memory:", memory_limit="900MB", resource_limits=ResourceLimits.for_testing(worker_hard_limit_bytes=1024**3, total_bytes=512*1024**2, native_headroom_bytes=128*1024**2))

def test_memory_benchmark_reports_rss_and_overload_rejections():
    report = run_memory_benchmark(rows=500_000, concurrent_requests=4, max_rss_bytes=2 * 1024**3)
    assert report["memory"]["peakRssBytes"] > 0
    assert report["resourceGovernor"]["rejections"] > 0
```

Add CLI coverage for `--memory-workload`, `--rows 1000000`,
`--concurrent-requests`, `--max-rss-bytes`, and JSON-only output without
running expensive workloads in ordinary unit tests.

- [ ] **Step 2: Run RED verification**

Run: `pytest tests/test_memory_protection_benchmark.py tests/test_performance_benchmark.py tests/test_duckdb_backend_config.py -v`

Expected: FAIL because backend memory is not reconciled with worker envelope
and benchmark output has no RSS/admission data.

- [ ] **Step 3: Implement layered safeguards and evidence**

Accept optional `resource_limits` in direct/Ibis DuckDB constructors. Resolve
`PIVOT_DUCKDB_MEMORY_LIMIT`, validate DuckDB allocation plus application
budget plus native headroom below `PIVOT_WORKER_MEMORY_HARD_LIMIT_BYTES`, and
configure DuckDB. Normalize memory failures through the existing safe backend
error boundary.

Expose current/high-water RSS and governor metrics in authenticated admin
health. Add a `--memory-workload` mode to `benchmark_pivot_runtime.py`:
deterministic 500k/1M data; concurrent viewport, bounded export, payload,
cache-pressure, and CDC-cardinality work; JSON latency, throughput, RSS, and
admission output. Fail only for caller-provided RSS ceiling or missing expected
overload rejection. Document Docker, Kubernetes, systemd, DuckDB, and release
evidence commands.

- [ ] **Step 4: Run focused verification and 500k evidence**

Run: `pytest tests/test_memory_protection_benchmark.py tests/test_performance_benchmark.py tests/test_duckdb_backend_config.py -v && python scripts/benchmark_pivot_runtime.py --memory-workload --rows 500000 --concurrent-requests 4 --json`

Expected: Tests PASS; benchmark JSON has positive `peakRssBytes`, rejections,
and no worker termination.

- [ ] **Step 5: Commit**

Run: `git add dash_tanstack_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py dash_tanstack_pivot/pivot_engine/pivot_engine/backends/ibis_backend.py dash_tanstack_pivot/pivot_engine/pivot_engine/admin_api.py scripts/benchmark_pivot_runtime.py tests/test_performance_benchmark.py tests/test_memory_protection_benchmark.py docs/OPERATIONS.md README.md && git commit -m "feat: measure and validate worker memory limits"`

## Final verification

- [ ] Run `pytest -q`.
- [ ] Run `python scripts/benchmark_pivot_runtime.py --memory-workload --rows 500000 --concurrent-requests 4 --json` and retain the JSON evidence.
- [ ] Run the 1M workload in the target worker/container profile with `--max-rss-bytes`; retain JSON and deployment configuration.
- [ ] Run package/build checks for Dash, React, and Streamlit from a clean checkout and preserve host-runtime contracts.
- [ ] Review every public error path for stable errors only and no claim that accounted bytes equal total process memory.
