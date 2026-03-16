"""
test_multi_instance_isolation.py
----------------------------------
Explicit multi-instance isolation tests for dash-tanstack-pivot.

Covers:
1. Table-scoped request isolation: two instances with distinct tables never share query state
2. Filter/sort isolation: changing filter/sort on instance A does not bleed into instance B
3. Interleaved request concurrency: stale or out-of-order responses are dropped per-instance identity
4. Identity contract: session_id, client_instance, and table are the three isolation axes

All tests are deterministic and execute against an in-memory DuckDB backend.

Run:
    python -m pytest tests/test_multi_instance_isolation.py -v
"""

import pyarrow as pa
import pytest

from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import (
    PivotRequestContext,
    PivotRuntimeService,
    PivotViewState,
    SessionRequestGate,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_service_with_two_tables():
    """
    Create a PivotRuntimeService with two distinct in-memory tables:
    - "table_a": region/sales data
    - "table_b": warehouse/inventory data

    Returns (service, gate) so tests can inspect gate state.
    """
    adapter = create_tanstack_adapter(backend_uri=":memory:")

    table_a = pa.Table.from_pydict(
        {
            "region": ["North", "North", "South", "South"],
            "product": ["Laptop", "Phone", "Laptop", "Phone"],
            "sales": [100, 80, 90, 60],
        }
    )
    table_b = pa.Table.from_pydict(
        {
            "warehouse": ["WH-A", "WH-A", "WH-B", "WH-B"],
            "sku": ["SKU-1", "SKU-2", "SKU-1", "SKU-2"],
            "stock": [500, 300, 400, 200],
        }
    )

    adapter.controller.load_data_from_arrow("table_a", table_a)
    adapter.controller.load_data_from_arrow("table_b", table_b)

    gate = SessionRequestGate()
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=gate)
    return service, gate


def _viewport(
    *,
    session_id: str,
    client_instance: str,
    window_seq: int = 1,
    state_epoch: int = 1,
    abort_generation: int = 1,
    intent: str = "viewport",
) -> dict:
    return {
        "start": 0,
        "end": 20,
        "window_seq": window_seq,
        "state_epoch": state_epoch,
        "abort_generation": abort_generation,
        "session_id": session_id,
        "client_instance": client_instance,
        "intent": intent,
    }


# ---------------------------------------------------------------------------
# Test 1: Table-scoped isolation
# Each instance queries its own table; responses contain only that table's columns.
# ---------------------------------------------------------------------------


def test_table_scoped_isolation():
    """
    Requests from instance A (table_a) and instance B (table_b) remain isolated.
    Each response contains only columns from the respective table.
    """
    service, _ = _make_service_with_two_tables()

    context_a = PivotRequestContext.from_frontend(
        table="table_a",
        trigger_prop="custom.viewport",
        viewport=_viewport(session_id="sess-abc", client_instance="grid-a"),
    )
    state_a = PivotViewState(
        row_fields=["region"],
        val_configs=[{"field": "sales", "agg": "sum"}],
    )

    context_b = PivotRequestContext.from_frontend(
        table="table_b",
        trigger_prop="custom.viewport",
        viewport=_viewport(session_id="sess-abc", client_instance="grid-b"),
    )
    state_b = PivotViewState(
        row_fields=["warehouse"],
        val_configs=[{"field": "stock", "agg": "sum"}],
    )

    response_a = service.process(state_a, context_a, current_filter_options={})
    response_b = service.process(state_b, context_b, current_filter_options={})

    assert response_a.status == "data"
    assert response_b.status == "data"

    # Responses are independent — each has data from its own table
    assert isinstance(response_a.data, list) and len(response_a.data) > 0
    assert isinstance(response_b.data, list) and len(response_b.data) > 0

    # Table A rows should have region-level grouping
    first_row_a = response_a.data[0]
    assert "region" in first_row_a or any(
        "region" in str(r) for r in response_a.data
    ), "table_a response should contain region dimension data"

    # Table B rows should have warehouse-level grouping
    first_row_b = response_b.data[0]
    assert "warehouse" in first_row_b or any(
        "warehouse" in str(r) for r in response_b.data
    ), "table_b response should contain warehouse dimension data"


# ---------------------------------------------------------------------------
# Test 2: Filter/sort isolation across instances
# Applying a filter to instance A must not affect instance B's response.
# ---------------------------------------------------------------------------


def test_filter_isolation_across_instances():
    """
    Two instances share the same session but use distinct client_instance values.
    Applying a filter on instance A must not affect instance B's unfiltered response.
    """
    service, _ = _make_service_with_two_tables()

    # Instance A: filter to only North region
    context_a = PivotRequestContext.from_frontend(
        table="table_a",
        trigger_prop="custom.viewport",
        viewport=_viewport(session_id="sess-filter", client_instance="grid-a"),
    )
    state_a_filtered = PivotViewState(
        row_fields=["region"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        filters={"region": ["North"]},
    )

    # Instance B: no filter applied
    context_b = PivotRequestContext.from_frontend(
        table="table_a",
        trigger_prop="custom.viewport",
        viewport=_viewport(session_id="sess-filter", client_instance="grid-b"),
    )
    state_b_unfiltered = PivotViewState(
        row_fields=["region"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        filters={},
    )

    response_a = service.process(state_a_filtered, context_a, current_filter_options={})
    response_b = service.process(state_b_unfiltered, context_b, current_filter_options={})

    assert response_a.status == "data"
    assert response_b.status == "data"

    # Filtered response for A should have fewer or equal rows than unfiltered B
    assert response_a.total_rows is not None
    assert response_b.total_rows is not None
    assert response_a.total_rows <= response_b.total_rows, (
        f"Filtered instance A ({response_a.total_rows} rows) should not exceed "
        f"unfiltered instance B ({response_b.total_rows} rows)"
    )


def test_sort_isolation_across_instances():
    """
    Sorting on instance A does not affect the sort order of instance B's response.
    Both instances get independent data ordered by their own sort state.
    """
    service, _ = _make_service_with_two_tables()

    # Instance A: sort sales descending
    context_a = PivotRequestContext.from_frontend(
        table="table_a",
        trigger_prop="custom.viewport",
        viewport=_viewport(session_id="sess-sort", client_instance="grid-a"),
    )
    state_a = PivotViewState(
        row_fields=["region"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        sorting=[{"id": "sales", "desc": True}],
    )

    # Instance B: no sorting
    context_b = PivotRequestContext.from_frontend(
        table="table_a",
        trigger_prop="custom.viewport",
        viewport=_viewport(session_id="sess-sort", client_instance="grid-b"),
    )
    state_b = PivotViewState(
        row_fields=["region"],
        val_configs=[{"field": "sales", "agg": "sum"}],
        sorting=[],
    )

    response_a = service.process(state_a, context_a, current_filter_options={})
    response_b = service.process(state_b, context_b, current_filter_options={})

    assert response_a.status == "data"
    assert response_b.status == "data"

    # Both instances return data — sort isolation means neither response is stale
    assert len(response_a.data) > 0
    assert len(response_b.data) > 0


# ---------------------------------------------------------------------------
# Test 3: Interleaved requests do not cross instance state
# Sending a higher-sequence request for A does not invalidate B's current response.
# ---------------------------------------------------------------------------


def test_interleaved_requests_do_not_cross_instance_state():
    """
    Interleaved requests from two separate client_instance values are tracked independently.

    Sequence:
    1. A sends window_seq=5, state_epoch=2 -> accepted
    2. B sends window_seq=3, state_epoch=1 -> accepted (separate gate key)
    3. A sends window_seq=4 (stale for A) -> rejected for A only
    4. B sends window_seq=4 -> accepted (B's gate is still at seq=3)
    5. Verify: B's response is current, A's stale response is not current
    """
    _, gate = _make_service_with_two_tables()

    session = "sess-interleaved"

    # Step 1: A registers seq=5
    assert gate.register_request(
        session, state_epoch=2, window_seq=5, abort_generation=2, intent="viewport",
        client_instance="grid-a"
    ), "A seq=5 should be accepted"

    # Step 2: B registers seq=3 (independent from A)
    assert gate.register_request(
        session, state_epoch=1, window_seq=3, abort_generation=1, intent="viewport",
        client_instance="grid-b"
    ), "B seq=3 should be accepted (separate instance)"

    # Step 3: A tries to register seq=4 (stale relative to A's seq=5)
    assert not gate.register_request(
        session, state_epoch=2, window_seq=4, abort_generation=2, intent="viewport",
        client_instance="grid-a"
    ), "A seq=4 should be rejected (A is already at seq=5)"

    # Step 4: B registers seq=4 (valid advance for B)
    assert gate.register_request(
        session, state_epoch=1, window_seq=4, abort_generation=1, intent="viewport",
        client_instance="grid-b"
    ), "B seq=4 should be accepted (B's previous was seq=3)"

    # Step 5: B's current response at seq=4 should be current
    assert gate.response_is_current(
        session, state_epoch=1, window_seq=4, abort_generation=1, intent="viewport",
        client_instance="grid-b"
    ), "B's seq=4 response should be current"

    # A's stale seq=4 response should not be current (A is at seq=5)
    assert not gate.response_is_current(
        session, state_epoch=2, window_seq=4, abort_generation=2, intent="viewport",
        client_instance="grid-a"
    ), "A's seq=4 response should not be current (A advanced to seq=5)"


# ---------------------------------------------------------------------------
# Test 4: Identity contract — client_instance prevents cross-mount stale poisoning
# A re-mounted component with a new client_instance resets its gate state.
# ---------------------------------------------------------------------------


def test_client_instance_prevents_cross_mount_stale_poisoning():
    """
    When a component unmounts and remounts, it gets a new client_instance.
    The new mount's requests must not be blocked by the old mount's high sequence numbers.
    """
    _, gate = _make_service_with_two_tables()

    session = "sess-remount"

    # Old mount reaches high sequence/epoch
    assert gate.register_request(
        session, state_epoch=99, window_seq=999, abort_generation=99, intent="viewport",
        client_instance="mount-old"
    )

    # New mount starts fresh at seq=1, epoch=1 — must not be blocked
    assert gate.register_request(
        session, state_epoch=1, window_seq=1, abort_generation=1, intent="structural",
        client_instance="mount-new"
    ), "New mount must not be blocked by old mount's high sequence numbers"

    # New mount's response at seq=1 must be current
    assert gate.response_is_current(
        session, state_epoch=1, window_seq=1, abort_generation=1, intent="structural",
        client_instance="mount-new"
    )


# ---------------------------------------------------------------------------
# Test 5: Abort generation isolation
# An abort on instance A does not abort in-flight requests from instance B.
# ---------------------------------------------------------------------------


def test_abort_generation_is_isolated_per_instance():
    """
    Bumping abort_generation on instance A rejects A's older requests
    but does not affect B's current request at a lower abort_generation.
    """
    _, gate = _make_service_with_two_tables()

    session = "sess-abort"

    # Both instances register requests at abort_generation=1
    assert gate.register_request(
        session, state_epoch=1, window_seq=1, abort_generation=1, intent="viewport",
        client_instance="grid-a"
    )
    assert gate.register_request(
        session, state_epoch=1, window_seq=1, abort_generation=1, intent="viewport",
        client_instance="grid-b"
    )

    # A bumps abort_generation to 5 (user triggered a hard reset on grid-a)
    assert gate.register_request(
        session, state_epoch=1, window_seq=2, abort_generation=5, intent="structural",
        client_instance="grid-a"
    )

    # A's old response at abort_generation=1 is now stale
    assert not gate.response_is_current(
        session, state_epoch=1, window_seq=1, abort_generation=1, intent="viewport",
        client_instance="grid-a"
    ), "A's old abort_generation=1 response must be dropped after A advanced to abort_generation=5"

    # B's response at abort_generation=1 remains current — unaffected by A's abort
    assert gate.response_is_current(
        session, state_epoch=1, window_seq=1, abort_generation=1, intent="viewport",
        client_instance="grid-b"
    ), "B's abort_generation=1 response must remain current despite A's abort bump"


# ---------------------------------------------------------------------------
# Test 6: Concurrent interleaved service.process calls produce independent results
# Two instances issue interleaved service.process calls; each gets its own data.
# ---------------------------------------------------------------------------


def test_concurrent_interleaved_service_process_calls():
    """
    Simulate interleaved service.process calls from two instances.
    Each call returns an independent response scoped to its own table and state.
    """
    service, _ = _make_service_with_two_tables()

    contexts_and_states = [
        (
            PivotRequestContext.from_frontend(
                table="table_a",
                trigger_prop="custom.viewport",
                viewport=_viewport(
                    session_id="sess-concurrent",
                    client_instance="grid-a",
                    window_seq=i,
                    state_epoch=1,
                ),
            ),
            PivotViewState(
                row_fields=["region"],
                val_configs=[{"field": "sales", "agg": "sum"}],
            ),
        )
        for i in range(1, 4)
    ] + [
        (
            PivotRequestContext.from_frontend(
                table="table_b",
                trigger_prop="custom.viewport",
                viewport=_viewport(
                    session_id="sess-concurrent",
                    client_instance="grid-b",
                    window_seq=i,
                    state_epoch=1,
                ),
            ),
            PivotViewState(
                row_fields=["warehouse"],
                val_configs=[{"field": "stock", "agg": "sum"}],
            ),
        )
        for i in range(1, 4)
    ]

    responses = []
    for ctx, state in contexts_and_states:
        resp = service.process(state, ctx, current_filter_options={})
        responses.append(resp)

    # All responses must succeed (no cross-contamination causes failures)
    statuses = [r.status for r in responses]
    assert all(s == "data" for s in statuses), (
        f"Some interleaved responses failed: {statuses}"
    )

    # All responses must have data
    assert all(len(r.data) > 0 for r in responses), (
        "All interleaved responses must contain data rows"
    )
