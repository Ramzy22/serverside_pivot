# Exact CDC Aggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Deliver exact event-speed CDC aggregates for trusted row-image streams, with safe invalidation and authoritative refresh for incomplete change sources.

**Architecture:** Normalize every provider event into a validated Change envelope before it reaches a consumer. StreamAggregationProcessor uses a durable per-job/per-group accumulator: counted multisets plus heaps make min and max reversible, while sum/count state makes other aggregates constant-time. PivotCDCManager routes trusted events to exact consumers and invalidating events to explicit freshness invalidation.

**Tech Stack:** Python 3.10+, asyncio, Ibis, PyArrow, Redis/fakeredis, pytest, pytest-asyncio.

**Spec:** docs/superpowers/specs/2026-09-19-cdc-exactness-design.md

## Global Constraints

- Exact CDC accepts INSERT, UPDATE, and DELETE events only with complete row images, stable source identity, event ID, and monotonic partition sequence.
- UPDATE removes its old image and adds its new image; filters and grouping membership are evaluated for each image independently.
- min and max use counted, type-preserving scalar multisets. No branch may retain a stale extremum after deletion or update.
- Duplicate events at or below a durable source checkpoint are ignored; out-of-order events invalidate rather than mutate state.
- State serialization uses versioned JSON/binary only. No path uses pickle or the legacy generic cache serializer.
- Polling changes without row images invalidate derived results and refresh authoritatively before output becomes fresh.
- Consumer, persistence, and refresh failures leave results invalidated and use safe structured logs.
- Browser editing, edit sessions, undo/redo, and history remain out of scope.

## Review Focus

- Decimal, date/time, None, and duplicate scalar values must preserve type identity and ordering in min/max state; Task 2 covers typed round trips and duplicate extrema.
- A CDC update crossing both a filter and grouping boundary must remove only the old contribution and add only the new one; Task 3 covers it.
- A failing consumer must record a failure instead of disappearing under a broad exception; Task 4 covers it.
- Restart followed by a replay must not double count; Task 2 covers store recreation and checkpoint behavior.
- An HTTP push event without ordering metadata must be invalidating, never exact; Task 5 covers this route-level contract.

---

### Task 1: Classify exact and invalidating CDC changes

**Files:**
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/cdc/models.py
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/cdc/database_change_detector.py
- Modify: tests/test_cdc.py

**Interfaces:**
- Produces Change.is_exact() -> bool and Change.is_invalidating() -> bool.
- Produces Change.invalidation(table: str, reason: str, *, source: str = "polling") -> Change.
- Extends Change with source, event_id, partition, sequence, row_identity, row_images_complete, and invalidation_reason without breaking table/type/old_row/new_row construction.
- PollingCDCProvider.detect_changes(table_name) returns one invalidating Change for a changed snapshot, never invented row-level changes.

- [ ] **Step 1: Write the failing tests**

~~~python
def test_change_requires_images_identity_and_order_for_exact_processing():
    exact = Change(
        table="sales", type="UPDATE",
        old_row={"id": 1, "amount": 10}, new_row={"id": 1, "amount": 25},
        source="postgres", event_id="wal-101", partition="0",
        sequence=101, row_identity="1",
    )
    incomplete = Change(table="sales", type="DELETE", old_row={"id": 1})

    assert exact.is_exact() is True
    assert incomplete.is_exact() is False
    assert incomplete.is_invalidating() is True


@pytest.mark.asyncio
async def test_polling_change_is_one_invalidating_event_not_placeholder_rows():
    provider = PollingCDCProvider(MutableBackend(rows))
    await provider.start_tracking_table("sales")
    rows.pop()

    changes = await provider.detect_changes("sales")

    assert len(changes) == 1
    assert changes[0].is_invalidating()
    assert changes[0].invalidation_reason == "polling_snapshot_changed"
    assert changes[0].old_row is None and changes[0].new_row is None
~~~

- [ ] **Step 2: Run tests to verify RED**

Run: python -m pytest -q tests/test_cdc.py -k "exact_processing or invalidating_event"

Expected: FAIL because Change has no quality contract and polling synthesizes row-level records.

- [ ] **Step 3: Implement event-quality classification**

~~~python
@dataclass
class Change:
    table: str
    type: str
    new_row: Optional[Dict[str, Any]] = None
    old_row: Optional[Dict[str, Any]] = None
    source: str = "unknown"
    event_id: Optional[str] = None
    partition: str = "0"
    sequence: Optional[int] = None
    row_identity: Optional[str] = None
    row_images_complete: Optional[bool] = None
    invalidation_reason: Optional[str] = None

    def is_exact(self) -> bool:
        operation = str(self.type or "").upper()
        images_ok = (
            operation == "INSERT" and isinstance(self.new_row, dict)
        ) or (
            operation == "DELETE" and isinstance(self.old_row, dict)
        ) or (
            operation == "UPDATE" and isinstance(self.old_row, dict)
            and isinstance(self.new_row, dict)
        )
        return bool(
            images_ok and self.row_images_complete is not False
            and self.source and self.event_id and self.row_identity
            and self.sequence is not None
        )

    def is_invalidating(self) -> bool:
        return bool(self.invalidation_reason) or not self.is_exact()
~~~

Make PollingCDCProvider emit Change.invalidation(table_name, "polling_snapshot_changed") whenever a snapshot fingerprint differs. Remove row-count-derived INSERT/DELETE fabrication and synthetic UPDATE images.

- [ ] **Step 4: Run the CDC provider suite**

Run: python -m pytest -q tests/test_cdc.py

Expected: PASS. Existing initialization, checksum order-independence, cache invalidation, and new event-quality tests pass.

- [ ] **Step 5: Commit**

~~~bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/cdc/models.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/cdc/database_change_detector.py \
  tests/test_cdc.py
git commit -m "feat: classify exact and invalidating CDC events"
~~~

### Task 2: Create reversible aggregate state and checkpoints

**Files:**
- Create: dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/exact_state.py
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/state_store.py
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/__init__.py
- Test: tests/test_exact_cdc_state.py

**Interfaces:**
- Produces CanonicalScalarCodec.encode(value: Any) -> str and decode(encoded: str) -> Any.
- Produces ExactMeasureAccumulator(aggregation: str), with add(value), remove(value), result(), is_empty(), to_payload(), and from_payload().
- Produces ExactGroupState.apply(row, sign, measures) and result().
- Produces CDCJobSnapshot with version, groups, invalidation status, checkpoint(partition), accept(partition, sequence, event_id), and advance(change).
- Produces async CDCStateStore.load(job_id) and compare_and_swap(job_id, expected_version, snapshot).
- Produces MemoryCDCStateStore and RedisCDCStateStore(runtime_state: RedisRuntimeState).

- [ ] **Step 1: Write the failing accumulator and persistence tests**

~~~python
def test_minimum_removal_uses_counted_multiset_for_duplicate_values():
    accumulator = ExactMeasureAccumulator("min")
    for value in (10, 10, 20, 30):
        accumulator.add(value)
    accumulator.remove(10)
    assert accumulator.result() == 10
    accumulator.remove(10)
    assert accumulator.result() == 20


def test_update_extremum_is_exact_after_remove_then_add():
    accumulator = ExactMeasureAccumulator("max")
    for value in (10, 20, 30):
        accumulator.add(value)
    accumulator.remove(30)
    accumulator.add(25)
    assert accumulator.result() == 25


@pytest.mark.asyncio
async def test_redis_snapshot_persists_checkpoint_across_store_recreation(fake_runtime_state):
    first = RedisCDCStateStore(fake_runtime_state)
    snapshot = await first.load("job-sales")
    persisted = await first.compare_and_swap(
        "job-sales", snapshot.version,
        snapshot.with_checkpoint("0", 4, "evt-4"),
    )

    second = RedisCDCStateStore(fake_runtime_state)

    assert (await second.load("job-sales")).checkpoint("0") == (4, "evt-4")
    assert persisted.version == 1
~~~

- [ ] **Step 2: Run tests to verify RED**

Run: python -m pytest -q tests/test_exact_cdc_state.py

Expected: FAIL during import because exact-state types do not exist.

- [ ] **Step 3: Implement exact, serializable state**

Use heapq and per-value reference counts for min/max. remove(value) must reject a negative contribution with ExactCDCStateError; it must not silently clamp. Use the canonical codec to encode bool, int, float, Decimal, str, bytes, date, datetime, and None with a version/type tag. Reject unsupported or non-orderable values for min/max.

Store a versioned JSON envelope through RedisRuntimeState.client at runtime_state.key("cdc", "job", job_id). Implement Redis compare-and-swap through WATCH/MULTI retry or one Lua script; persist the state and its partition checkpoints atomically. Implement MemoryCDCStateStore with asyncio.Lock and identical version conflict behavior.

- [ ] **Step 4: Run exact state tests**

Run: python -m pytest -q tests/test_exact_cdc_state.py

Expected: PASS. Duplicate min/max, extrema replacement, empty accumulator, scalar codec, CAS conflict, and recreated-store checkpoint tests pass.

- [ ] **Step 5: Commit**

~~~bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/exact_state.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/state_store.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/__init__.py \
  tests/test_exact_cdc_state.py
git commit -m "feat: add reversible CDC aggregate state"
~~~

### Task 3: Apply trusted changes to affected streaming groups

**Files:**
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/streaming_processor.py
- Modify: tests/test_streaming_incremental.py
- Modify: tests/test_exact_cdc_state.py

**Interfaces:**
- Extends StreamAggregationProcessor.__init__(kafka_config=None, state_store: CDCStateStore | None = None).
- Produces process_change(change: Change) -> dict[str, int] with exact, invalidated, and ignored counts.
- Produces StreamingJobResult(status, values, reason=None) from get_job_result(job_id); legacy fresh callers retain dictionary-style values.
- Produces invalidate_table(table_name, reason) -> int.
- Retains process_stream_update only as a compatibility wrapper that creates an invalidating event unless exact envelope metadata is supplied.

- [ ] **Step 1: Write failing streaming tests**

~~~python
@pytest.mark.asyncio
async def test_stream_job_removes_old_group_and_adds_new_group_for_exact_update():
    processor = StreamAggregationProcessor(state_store=MemoryCDCStateStore())
    job_id = await processor.create_real_time_aggregation_job(
        _spec(rows=["region"], filters=[{"field": "active", "op": "=", "value": True}])
    )
    await processor.process_change(
        _insert("evt-1", 1, {"id": 1, "region": "North", "active": True, "sales": 10})
    )
    await processor.process_change(
        _update(
            "evt-2", 2,
            {"id": 1, "region": "North", "active": True, "sales": 10},
            {"id": 1, "region": "South", "active": True, "sales": 25},
        )
    )

    result = await processor.get_job_result(job_id)
    assert result.status == "fresh"
    assert result.values["groups"] == {"South": {"total_sales": 25}}


@pytest.mark.asyncio
async def test_incomplete_change_invalidates_job_and_never_returns_previous_extremum():
    processor = StreamAggregationProcessor(state_store=MemoryCDCStateStore())
    job_id = await processor.create_real_time_aggregation_job(_min_spec())
    await processor.process_change(_insert("evt-1", 1, {"id": 1, "sales": 10}))
    await processor.process_change(Change(table="sales", type="DELETE", old_row={"id": 1}))

    result = await processor.get_job_result(job_id)

    assert result.status == "invalidated"
    assert result.values == {}
~~~

- [ ] **Step 2: Run tests to verify RED**

Run: python -m pytest -q tests/test_streaming_incremental.py tests/test_exact_cdc_state.py -k "removes_old_group or incomplete_change"

Expected: FAIL because the processor accepts flattened records and exposes stale raw state.

- [ ] **Step 3: Implement group-local exact transitions**

For all jobs on change.table, process trusted INSERT as +new_row, DELETE as -old_row, and UPDATE as -old_row then +new_row. Evaluate saved filters separately for every image. Encode a grouping tuple through CanonicalScalarCodec and update only its old/new groups. Use the stored checkpoint to ignore replays and invalidate on an out-of-order position.

~~~python
async def process_change(self, change: Change) -> dict[str, int]:
    if change.is_invalidating():
        return {
            "exact": 0,
            "invalidated": await self.invalidate_table(
                change.table, change.invalidation_reason or "incomplete_change"
            ),
            "ignored": 0,
        }
    for job in self._jobs_for_table(change.table):
        snapshot = await self.state_store.load(job["job_id"])
        decision = snapshot.accept(change.partition, change.sequence, change.event_id)
        if decision == "replay":
            ignored += 1
            continue
        if decision == "out_of_order":
            await self._invalidate_job(job["job_id"], "out_of_order_source_event")
            invalidated += 1
            continue
        next_snapshot = self._apply_exact_transition(snapshot, job, change)
        await self.state_store.compare_and_swap(
            job["job_id"], snapshot.version, next_snapshot.advance(change)
        )
~~~

Delete _apply_aggregation_reverse and its pass branch. The accumulator is the only reverse path. A failed compare-and-swap reloads once and repeats the pure transition; a second conflict raises a safe state error and invalidates the job.

- [ ] **Step 4: Run streaming regression tests**

Run: python -m pytest -q tests/test_streaming_incremental.py tests/test_exact_cdc_state.py

Expected: PASS. Deleted extrema, duplicate extrema, updated extrema, filter/group transitions, empty groups, replays, out-of-order events, and freshness results pass.

- [ ] **Step 5: Commit**

~~~bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/streaming_processor.py \
  tests/test_streaming_incremental.py tests/test_exact_cdc_state.py
git commit -m "feat: apply exact CDC changes to streaming jobs"
~~~

### Task 4: Repair materialized-view routing and invalidation

**Files:**
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/streaming_processor.py
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/cdc/cdc_manager.py
- Modify: tests/test_streaming_incremental.py
- Modify: tests/test_cdc.py

**Interfaces:**
- Produces IncrementalMaterializedViewManager.process_change(change: Change) -> None.
- Produces IncrementalMaterializedViewManager.invalidate_table(table_name, reason) -> int.
- Produces one ExactGroupState snapshot per registered view; get_view_data renders
  that snapshot rather than returning a stale physical-table cache.
- Produces PivotCDCManager.register_change_consumer(table_name, consumer) -> None; register_materialized_view_manager remains a compatibility wrapper.
- Produces CDCProcessingError after setting checkpoints[table_name]["last_error"] to "cdc_consumer_failed".

- [ ] **Step 1: Write failing materialized-view and error tests**

~~~python
@pytest.mark.asyncio
async def test_materialized_view_refreshes_authoritatively_after_invalidating_change(duckdb_backend):
    manager = IncrementalMaterializedViewManager(duckdb_backend)
    view_name = await manager.create_incremental_view(_sales_min_spec())
    await manager.process_change(Change.invalidation("sales", "polling_snapshot_changed"))

    assert await manager.get_view_data(view_name) == [{"region": "North", "minimum": 20}]
    assert manager.views[view_name]["freshness"] == "fresh"


@pytest.mark.asyncio
async def test_cdc_manager_records_and_raises_consumer_failure(cdc_manager):
    class BrokenConsumer:
        async def process_change(self, change):
            raise RuntimeError("backend secret")
        async def invalidate_table(self, table_name, reason):
            raise AssertionError("exact event must use process_change")

    cdc_manager.register_change_consumer("sales", BrokenConsumer())

    with pytest.raises(CDCProcessingError):
        await cdc_manager._process_change(_insert("evt-1", 1, {"id": 1, "sales": 1}))

    assert cdc_manager.checkpoints["sales"]["last_error"] == "cdc_consumer_failed"
~~~

- [ ] **Step 2: Run tests to verify RED**

Run: python -m pytest -q tests/test_cdc.py tests/test_streaming_incremental.py -k "refreshes_authoritatively or consumer_failure"

Expected: FAIL because CDC calls an incompatible manager method and catches consumer errors.

- [ ] **Step 3: Use named views and explicit consumer operations**

The manager records each saved view name by source table. At view creation it
bootstraps one ExactGroupState snapshot from the filtered source rows and uses
that snapshot to render view_data. Exact events invoke process_change, apply
the old/new row transition to only its affected view groups, then replace the
rendered groups in view_data. This is the event-speed materialized result path;
it never reads the old aggregated row to guess a new min/max.

Invalidating events set freshness to invalidated, clear view_data, rebuild the
source snapshot from the saved Ibis PivotSpec, render it, and become fresh only
after that rebuild succeeds. Do not use a constructed "mv_<table>" key. Mark
the legacy physical Ibis table non-fresh until its coalesced writer has caught
up, so no consumer can select it as a current result ahead of the exact snapshot.

~~~python
async def _route_change(self, change: Change) -> None:
    for consumer in tuple(self.change_consumers.get(change.table, ())):
        try:
            if change.is_invalidating():
                await consumer.invalidate_table(
                    change.table, change.invalidation_reason or "incomplete_change"
                )
            else:
                await consumer.process_change(change)
        except Exception as exc:
            self._record_consumer_failure(change.table, exc)
            raise CDCProcessingError("cdc_consumer_failed") from exc
~~~

Keep table-cache invalidation for every change. Structured logs may include source, table, event ID, and stable code but never backend exception text in an HTTP response.

- [ ] **Step 4: Run CDC/materialized-view tests**

Run: python -m pytest -q tests/test_cdc.py tests/test_streaming_incremental.py

Expected: PASS. Real DuckDB/Ibis refresh after an invalidating deleted min, correct saved-view routing, visible consumer failure, and cache invalidation pass.

- [ ] **Step 5: Commit**

~~~bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/cdc/cdc_manager.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/streaming/streaming_processor.py \
  tests/test_cdc.py tests/test_streaming_incremental.py
git commit -m "fix: make CDC materialized views authoritative"
~~~

### Task 5: Wire controller and HTTP push ingestion safely

**Files:**
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/complete_rest_api.py
- Modify: dash_tanstack_pivot/pivot_engine/pivot_engine/config.py
- Modify: tests/test_cdc.py
- Modify: tests/test_features_impl.py
- Modify: tests/test_runtime_dataset_authorization.py

**Interfaces:**
- run_streaming_aggregation(spec) registers StreamAggregationProcessor as a CDC consumer for spec.table.
- push_change_event(table_name, change_dict) preserves event metadata and derives row_identity only from an explicit identifier or a present id.
- The HTTP route reports mode "accepted_exact" or "accepted_invalidating" without exposing backend exception text.
- ScalablePivotConfig gains cdc_exact_state_store: Literal["memory", "redis"] and production validation rejects redis mode without a validated RedisRuntimeState.

- [ ] **Step 1: Write failing controller/API tests**

~~~python
@pytest.mark.asyncio
async def test_streaming_job_registered_for_cdc_receives_exact_push(controller):
    job = await controller.run_streaming_aggregation(_sales_min_spec())
    await controller.setup_push_cdc("sales")
    await controller.push_change_event(
        "sales", _push_insert("evt-1", 1, {"id": 1, "sales": 10})
    )

    result = await controller.streaming_processor.get_job_result(job["job_id"])
    assert result.values["minimum"] == 10


def test_incomplete_push_is_not_advertised_as_exact_cdc(api_client):
    response = api_client.post(
        "/pivot/cdc/push/sales",
        json={"type": "DELETE", "old_row": {"id": 1}},
    )

    assert response.status_code == 200
    assert response.json()["data"]["mode"] == "accepted_invalidating"
~~~

- [ ] **Step 2: Run tests to verify RED**

Run: python -m pytest -q tests/test_cdc.py tests/test_features_impl.py tests/test_runtime_dataset_authorization.py -k "registered_for_cdc or advertised_as_exact"

Expected: FAIL because controller streaming jobs and CDC routing are disconnected and the HTTP response does not expose event quality.

- [ ] **Step 3: Connect exact consumers after authorization**

Register stream processor once per active table in setup_cdc and setup_push_cdc. Register IncrementalMaterializedViewManager only when it exists. Apply dataset/row-scope authorization before quality classification and dispatch. Request Redis CDC state through injected RedisRuntimeState or validated distributed settings; production redis mode must fail closed and never fall back to memory.

- [ ] **Step 4: Run controller/API and authorization tests**

Run: python -m pytest -q tests/test_cdc.py tests/test_features_impl.py tests/test_runtime_dataset_authorization.py

Expected: PASS. Authorized exact events update streaming state, incomplete events invalidate, and viewer/wrong-tenant events remain denied before dispatch.

- [ ] **Step 5: Commit**

~~~bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/complete_rest_api.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/config.py tests/test_cdc.py \
  tests/test_features_impl.py tests/test_runtime_dataset_authorization.py
git commit -m "feat: route exact CDC through controller"
~~~

### Task 6: Document the high-performance CDC contract and verify release output

**Files:**
- Create: docs/operations/exact-cdc.md
- Modify: dash_tanstack_pivot/pivot_engine/README.md
- Modify: dash_tanstack_pivot/pivot_engine/examples/example_scalable_pivot.py
- Modify: tests/test_docs_examples_contract.py
- Modify: .github/workflows/ci.yml

**Interfaces:**
- Documents exact fields, native/push CDC requirements, polling invalidation semantics, supported aggregates, Redis recovery, and freshness status.
- CI creates a second processor over the same Redis state to execute duplicate-min, update, replay, and restart coverage.

- [ ] **Step 1: Write the failing documentation test**

~~~python
def test_exact_cdc_operations_documentation_declares_safety_and_performance_contract():
    text = Path("docs/operations/exact-cdc.md").read_text(encoding="utf-8")
    for required in (
        "event_id", "sequence", "row_identity", "old_row", "new_row",
        "PollingCDCProvider", "invalidated", "PIVOT_RUNTIME_REDIS_URL",
    ):
        assert required in text
~~~

- [ ] **Step 2: Run test to verify RED**

Run: python -m pytest -q tests/test_docs_examples_contract.py -k exact_cdc

Expected: FAIL with FileNotFoundError for docs/operations/exact-cdc.md.

- [ ] **Step 3: Publish the operational boundary**

Document that polling is correctness-safe but not the event-speed path; native/push sources need complete images and ordered offsets. Update the example to send a complete Change envelope and show freshness. Configure CI Redis coverage to recreate the processor and prove that event replay does not alter restored state.

- [ ] **Step 4: Run release verification**

Run: python -m pytest -q

Run: python -m build dash_tanstack_pivot --outdir $env:TEMP\pivot-cdc-dist

Run: python scripts/ci/verify_wheel_contract.py (Get-ChildItem $env:TEMP\pivot-cdc-dist\*.whl | Select-Object -First 1 -ExpandProperty FullName)

Expected: tests pass, distribution builds, and wheel contract verification passes.

- [ ] **Step 5: Commit**

~~~bash
git add -f docs/operations/exact-cdc.md docs/superpowers/plans/2026-09-19-exact-cdc-aggregation.md
git add dash_tanstack_pivot/pivot_engine/README.md \
  dash_tanstack_pivot/pivot_engine/examples/example_scalable_pivot.py \
  tests/test_docs_examples_contract.py .github/workflows/ci.yml
git commit -m "docs: publish exact CDC performance contract"
~~~
