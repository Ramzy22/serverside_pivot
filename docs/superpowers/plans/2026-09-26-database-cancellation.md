# Database cancellation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make request cancellation and configured query deadlines stop the active DuckDB statement, safely retire its lease, and expose only stable application outcomes.

**Architecture:** Add a small backend execution primitive that owns one statement’s ID, physical lease, cancellation request, completion acknowledgement, and terminal disposition. Direct DuckDB and Ibis-on-DuckDB will use that primitive; Ibis expressions go through the Ibis backend facade rather than controller-owned executor calls. The runtime lifecycle remains the request deadline owner, but its cancellation reaches the database facade and does not complete until worker cleanup has completed.

**Tech Stack:** Python 3.11, asyncio, threading, DuckDB 0.8–1.x, Ibis 12, PyArrow, pytest.

**Spec:** `docs/superpowers/specs/2026-09-26-database-cancellation-design.md`

## Global Constraints

- Never claim that cancelling an asyncio waiter terminates a database statement.
- A cancellation targets only the exact statement cursor/connection lease; no shared global connection interrupt is allowed.
- A cancelled/failed lease must complete a health check before reuse or be discarded.
- The supported default is DuckDB, direct and Ibis-backed; a hard deadline on an unsupported Ibis driver fails closed.
- Client-visible database failures use only `query_cancelled`, `query_timeout`, `cancellation_unavailable`, or `database_execution_failed`; no backend exception text is exposed.
- Preserve current Arrow schemas and normal successful execution behavior.

## Review Focus

- Cancellation before a worker has obtained a lease must still prevent that leased statement from running normally; cover it in Task 1.
- A DuckDB interrupt that leaves the cursor healthy must return that *same* cursor safely, while an unhealthy cursor must be replaced; cover both dispositions in Task 2.
- Ibis DuckDB Arrow conversion must preserve Ibis logical types, not raw DuckDB decimal inference; cover it in Task 3.
- An outer request timeout must wait for backend cleanup before producing the timeout result; cover it in Task 4.
- Repeated cancel calls for one query must be idempotent and must not increase active workers or pool capacity; cover it in Task 2.

---

### Task 1: Statement execution primitives

**Files:**
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/backends/execution.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py:90-180`
- Test: `tests/test_database_cancellation.py`

**Interfaces:**
- Produces `DatabaseExecution`, `DatabaseExecutionError`, `QueryCancelledError`, `QueryTimeoutError`, and `CancellationUnavailableError` from `pivot_engine.backends.execution`.
- Produces `ConnectionPool.checkout() -> Any`, `ConnectionPool.checkin(connection: Any, *, discard: bool = False) -> None`, and `ConnectionPool.discard(connection: Any) -> None` for a statement owner.

- [ ] **Step 1: Write the failing statement-state tests**

```python
def test_database_execution_cancels_the_lease_acquired_after_cancel_request():
    execution = DatabaseExecution("query-1")
    execution.request_cancel("client_cancelled")
    execution.attach_lease(cursor_with_interrupt)
    assert cursor_with_interrupt.interrupt_calls == 1
    assert execution.reason == "client_cancelled"


def test_connection_pool_checkin_discards_an_unhealthy_lease():
    pool = ConnectionPool(factory, max_size=1)
    lease = pool.checkout()
    pool.checkin(lease, discard=True)
    assert factory.created == 2
```

- [ ] **Step 2: Run the statement-state tests to verify they fail**

Run: `pytest tests/test_database_cancellation.py -k 'database_execution or pool_checkin' -v`

Expected: FAIL because the execution owner and explicit pool lease API do not exist.

- [ ] **Step 3: Implement the execution owner and explicit lease API**

Implement `DatabaseExecution(query_id: str)` with a thread-safe `attach_lease`, idempotent `request_cancel(reason: Literal["client_cancelled", "query_timeout"])`, `finish`, and `wait_for_completion` contract. It must call only the attached lease’s native `interrupt()`/`cancel()` capability, retain the first cancellation reason, and expose a completion event. Refactor `ConnectionPool` so only the worker/handle decides whether a checked-out connection returns or is discarded; retain `get_connection()` as a compatibility wrapper over the new API.

- [ ] **Step 4: Run the statement-state tests to verify they pass**

Run: `pytest tests/test_database_cancellation.py -k 'database_execution or pool_checkin' -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/backends/execution.py dash_tanstack_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py tests/test_database_cancellation.py
git commit -m "feat: add statement execution ownership"
```

### Task 2: Cancellable direct DuckDB execution

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py:170-270, 355-365`
- Modify: `tests/test_duckdb_backend_config.py`
- Modify: `tests/test_database_cancellation.py`

**Interfaces:**
- Consumes: `DatabaseExecution` and explicit `ConnectionPool` leases from Task 1.
- Produces `DuckDBBackend.execute_async(query: Dict[str, Any], *, timeout_seconds: Optional[float] = None) -> pa.Table`, `DuckDBBackend.cancel_query(query_id: str) -> bool`, and `DuckDBBackend.execution_metrics() -> Dict[str, int]`.

- [ ] **Step 1: Write the failing real-DuckDB cancellation tests**

```python
@pytest.mark.asyncio
async def test_duckdb_async_cancel_interrupts_the_running_cursor_and_reuses_or_replaces_it():
    backend = DuckDBBackend(":memory:", max_connections=1)
    task = asyncio.create_task(backend.execute_async(LONG_DUCKDB_QUERY))
    query_id = await wait_for_running_query(backend)
    assert await backend.cancel_query(query_id) is True
    with pytest.raises(QueryCancelledError, match="query_cancelled"):
        await task
    assert backend.execute({"sql": "SELECT 1 AS healthy"}).to_pylist() == [{"healthy": 1}]


@pytest.mark.asyncio
async def test_duckdb_async_timeout_interrupts_database_and_does_not_leak_worker_capacity():
    backend = DuckDBBackend(":memory:", max_connections=1)
    with pytest.raises(QueryTimeoutError, match="query_timeout"):
        await backend.execute_async(LONG_DUCKDB_QUERY, timeout_seconds=0.01)
    assert backend.execution_metrics()["running"] == 0
    assert backend.execute({"sql": "SELECT 1"}).num_rows == 1
```

Use `SELECT sum(i) FROM range(10000000000) AS t(i)` and a started-event/registry poll so the test only interrupts after the DuckDB statement begins. Add a repeated-cancel test that verifies one interrupt and no pool growth beyond `max_connections`.

- [ ] **Step 2: Run the direct-DuckDB cancellation tests to verify they fail**

Run: `pytest tests/test_database_cancellation.py -k duckdb -v`

Expected: FAIL because async execution has no query registry, targets the global connection, and does not translate native interrupts.

- [ ] **Step 3: Implement cancellable `DuckDBBackend` execution**

Create a query ID before scheduling each worker, attach the exact pooled cursor in that worker, execute and Arrow-convert on it, and always health-check/check-in or discard that cursor in the worker’s `finally`. On explicit cancellation or the locally configured deadline, signal the handle, shield/wait for worker cleanup, then raise `QueryCancelledError` or `QueryTimeoutError`. Translate DuckDB’s native interrupt only when the handle has a cancellation reason; propagate other failures as `DatabaseExecutionError("database_execution_failed")` without backend text. Replace the existing global `interrupt()` implementation with a compatibility operation that cancels active statement handles rather than `self.con.interrupt()`.

- [ ] **Step 4: Run the direct-DuckDB cancellation tests to verify they pass**

Run: `pytest tests/test_database_cancellation.py -k duckdb -v; pytest tests/test_duckdb_backend_config.py -v`

Expected: PASS, including parameterized, empty-schema, batch, streaming, and normal async behavior.

- [ ] **Step 5: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/backends/duckdb_backend.py tests/test_duckdb_backend_config.py tests/test_database_cancellation.py
git commit -m "feat: interrupt direct DuckDB statements"
```

### Task 3: Ibis DuckDB facade and controller routing

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/backends/ibis_backend.py:25-220`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py:766-787`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/controller.py:235-315`
- Modify: `tests/test_ibis_backend_transaction.py`
- Modify: `tests/test_query_execution_service.py`
- Modify: `tests/test_database_cancellation.py`

**Interfaces:**
- Consumes: direct DuckDB execution semantics and stable errors from Tasks 1–2.
- Produces `IbisBackend.execute_async(query, params=None, return_arrow=True, *, timeout_seconds=None) -> Any`, `IbisBackend.cancel_query(query_id: str) -> bool`, and `IbisBackend.cancellation_status() -> Dict[str, Any]`.
- Produces controller expression execution through `backend.execute_async({"ibis_expr": expr})` whenever the backend is an `IbisBackend`.

- [ ] **Step 1: Write the failing Ibis facade and controller-routing tests**

```python
@pytest.mark.asyncio
async def test_ibis_duckdb_cancel_stops_compiled_expression_and_keeps_arrow_schema():
    backend = IbisBackend(connection_uri=":memory:")
    expression = backend.con.sql(LONG_DUCKDB_QUERY)
    task = asyncio.create_task(backend.execute_async({"ibis_expr": expression}))
    assert await backend.cancel_query(await wait_for_running_query(backend))
    with pytest.raises(QueryCancelledError, match="query_cancelled"):
        await task
    assert backend.execute({"ibis_expr": backend.con.sql("SELECT 1 AS value")}).schema.field("value").type == pa.int64()


@pytest.mark.asyncio
async def test_scalable_controller_uses_ibis_backend_async_facade(monkeypatch):
    controller = configured_controller()
    spy = AsyncMock(return_value=pa.table({"value": [1]}))
    monkeypatch.setattr(controller.backend, "execute_async", spy)
    await controller._execute_ibis_expr_async(expression, "sales")
    spy.assert_awaited_once_with({"ibis_expr": expression})
```

Add a capability-double assertion that `IbisBackend(connection=unsupported, query_timeout_seconds=1)` raises `CancellationUnavailableError`, while an unsupported backend without a hard deadline reports `available=False` from `cancellation_status()`.

- [ ] **Step 2: Run the Ibis facade and routing tests to verify they fail**

Run: `pytest tests/test_ibis_backend_transaction.py tests/test_database_cancellation.py tests/test_query_execution_service.py -k 'ibis or controller' -v`

Expected: FAIL because Ibis cancellation only cancels an asyncio future and controller paths invoke expression methods directly.

- [ ] **Step 3: Implement the DuckDB-aware Ibis adapter and route async controllers through it**

For an Ibis DuckDB backend, compile `expr.as_table()`, execute it on a leased `self.con.con.cursor()`, and use Ibis’s DuckDB Arrow mapper to return the same logical Arrow result as `expr.to_pyarrow()`. Reuse the Task 1 execution owner and Task 2 cleanup policy; do not use `self.con.con.interrupt()` globally. For non-DuckDB Ibis connections, detect a documented exact-connection `cancel()` capability; otherwise reject a supplied hard deadline and report cancellation unavailable. Replace direct controller `run_in_executor(..., expr.to_pyarrow)` request execution with the Ibis facade; preserve existing non-Ibis fallback behavior and scalar/non-request paths.

- [ ] **Step 4: Run the Ibis facade and routing tests to verify they pass**

Run: `pytest tests/test_ibis_backend_transaction.py tests/test_database_cancellation.py tests/test_query_execution_service.py -k 'ibis or controller' -v`

Expected: PASS. `rg 'run_in_executor\(None, .*to_pyarrow' dash_tanstack_pivot/pivot_engine/pivot_engine/controller.py dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py` returns no request-path match.

- [ ] **Step 5: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/backends/ibis_backend.py dash_tanstack_pivot/pivot_engine/pivot_engine/controller.py dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py tests/test_ibis_backend_transaction.py tests/test_query_execution_service.py tests/test_database_cancellation.py
git commit -m "feat: route Ibis pivots through cancellable execution"
```

### Task 4: Runtime deadline/error boundary and release evidence

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/lifecycle.py:94-132`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/resilience.py:10-18`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py:649-697`
- Modify: `tests/test_runtime_capabilities.py`
- Modify: `tests/test_runtime_service.py`
- Modify: `docs/OPERATIONS.md`

**Interfaces:**
- Consumes: `QueryTimeoutError`, `QueryCancelledError`, `CancellationUnavailableError`, and `DatabaseExecutionError` from Tasks 1–3.
- Produces stable runtime status/message pairs: `timeout` / `query_timeout`, `error` / `cancellation_unavailable`, and `error` / `database_execution_failed`.

- [ ] **Step 1: Write the failing lifecycle and transport-boundary tests**

```python
@pytest.mark.asyncio
async def test_lifecycle_timeout_waits_for_native_cleanup_before_returning_timeout():
    cleanup_complete = asyncio.Event()
    async def operation():
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cleanup_complete.set()
            raise
    with pytest.raises(PivotRequestTimeout, match="query_timeout"):
        await lifecycle.run_backend_operation(context, "data", operation)
    assert cleanup_complete.is_set()


def test_runtime_router_hides_database_exception_text():
    response = process_backend_error(DatabaseExecutionError("server secret"))
    assert response.message == "database_execution_failed"
    assert "secret" not in response.message
```

Also use a real `IbisBackend` through the runtime operation in a 10 ms deadline test to prove the request returns only after `execution_metrics()["running"] == 0`.

- [ ] **Step 2: Run the runtime deadline tests to verify they fail**

Run: `pytest tests/test_runtime_capabilities.py tests/test_runtime_service.py tests/test_database_cancellation.py -k 'timeout or database_exception' -v`

Expected: FAIL because the outer wait can report a timeout before a worker has been cleaned up and router error text is raw.

- [ ] **Step 3: Implement stable deadline and error translation**

Keep `RuntimeRequestLifecycle`’s request timeout as the deadline authority, but let cancellation propagate into the facade and await its cleanup. Translate its completed cancellation to `PivotRequestTimeout("query_timeout")`; translate stable backend exceptions at the router boundary without interpolating raw database text. Document `QUERY_TIMEOUT`/`PIVOT_REQUEST_TIMEOUT_SECONDS`, DuckDB support, unsupported-backend fail-closed behavior, metrics, and the PostgreSQL opt-in integration prerequisite in `docs/OPERATIONS.md`.

- [ ] **Step 4: Run the release verification suite**

Run: `pytest tests/test_database_cancellation.py tests/test_duckdb_backend_config.py tests/test_ibis_backend_transaction.py tests/test_runtime_capabilities.py tests/test_runtime_service.py tests/test_query_execution_service.py -v; pytest -q`

Expected: PASS. The focused suite proves native interruption, healthy reuse/discard, Ibis/controller routing, timeouts, stable errors, and repeated-cancel bounds; the complete Python suite has no regression.

- [ ] **Step 5: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/lifecycle.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/resilience.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py tests/test_runtime_capabilities.py tests/test_runtime_service.py tests/test_database_cancellation.py docs/OPERATIONS.md
git commit -m "fix: enforce database cancellation deadlines"
```

## Self-Review

1. **Spec coverage:** Tasks 1–2 implement the per-statement handle, exact DuckDB interruption, cleanup, health/discard, metrics, deadlines and idempotency. Task 3 supplies Ibis DuckDB parity, unsupported capability behavior, and removes controller-owned request execution. Task 4 proves runtime cleanup ordering, stable client errors, and documents the production contract. PostgreSQL remains opt-in by the approved spec and is explicitly documented rather than falsely generalized.
2. **Step scan:** Each task starts red, runs the narrow failing target, implements named interfaces, runs green, then commits. No step delegates an undefined API or asks for a generic edge-case pass.
3. **Type consistency:** Both direct and Ibis facades expose `execute_async(..., timeout_seconds=None)`, `cancel_query(query_id)`, and metrics/status. Controllers consume only the Ibis facade’s existing query envelope.
4. **Review focus:** Each of the five listed failure modes is assigned to and tested in Tasks 1–4.
5. **Proportion:** The plan defines interfaces, concrete files, and proof cases without copying implementation bodies.
