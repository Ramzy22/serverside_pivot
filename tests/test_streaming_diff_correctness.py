"""Comprehensive 30-scenario release-gate test suite for Live Streaming & Analytical Diff Engine."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, List

import pyarrow as pa
import pytest

from pivot_engine import (
    PivotRuntimeService,
    RuntimeDatasetAuthorizer,
    create_tanstack_adapter,
    register_pivot_app,
    register_runtime_http_transport,
)
from pivot_engine.diff.baseline_comparison_service import BaselineComparisonService
from pivot_engine.diff.snapshot_provider import (
    SnapshotAuthorizationError,
    SnapshotExpiredError,
    SnapshotProvider,
    SnapshotStrategy,
)
from pivot_engine.resource_governor import (
    ResourceExhaustedError,
    ResourceGovernor,
    ResourceLimits,
)
from pivot_engine.streaming.exact_measure_engine import (
    CanonicalPathCodec,
    ExactMeasureEngine,
    GRAND_TOTAL_CANONICAL_PATH,
    compute_measure_diff,
    decode_wire_value,
    encode_wire_value,
)
from pivot_engine.streaming.reactive_query_coordinator import (
    ExecutionStrategy,
    ReactiveQueryCoordinator,
)
from pivot_engine.streaming.subscription_broker import (
    PauseMode,
    SubscriptionBroker,
    ViewportWindow,
)
from pivot_engine.streaming.versioned_source_adapter import (
    DuckDBSingleOwnerError,
    SourceCapabilityTier,
    SourceChangeProvider,
    VersionedSourceAdapter,
)
from pivot_engine.types.pivot_spec import Measure, PivotSpec


@dataclass
class _Principal:
    id: str
    role: str = "trader"
    attributes: Dict[str, Any] = None

    def __post_init__(self):
        if self.attributes is None:
            self.attributes = {}


def _make_stack(arrow_table: pa.Table, table_name: str = "trades", *, backend_uri: str = ":memory:", resource_governor=None, authorizer=None):
    adapter = create_tanstack_adapter(backend_uri=backend_uri)
    if resource_governor is not None:
        adapter.controller.resource_governor = resource_governor
    adapter.controller.load_data_from_arrow(table_name, arrow_table)
    source = VersionedSourceAdapter(adapter.controller)
    source.register_immutable_arrow_source(table_name, arrow_table, revision=1)
    snapshots = SnapshotProvider(
        adapter.controller,
        source,
        resource_governor=resource_governor or adapter.controller.resource_governor,
    )
    comparison = BaselineComparisonService(adapter.controller, snapshots)
    coordinator = ReactiveQueryCoordinator(
        adapter.controller,
        source,
        snapshots,
        comparison,
        resource_governor=resource_governor or adapter.controller.resource_governor,
    )
    broker = SubscriptionBroker(
        source_adapter=source,
        snapshot_provider=snapshots,
        coordinator=coordinator,
        authorizer=authorizer,
    )
    return adapter, source, snapshots, comparison, coordinator, broker


def _base_trades_table() -> pa.Table:
    return pa.table(
        {
            "id": [1, 2, 3, 4],
            "desk": ["Equities", "Equities", "Rates", "Rates"],
            "book": ["StatArb", "Index", "UST", "UST"],
            "quarter": ["Q1", "Q1", "Q1", "Q2"],
            "pnl": [100.0, 200.0, 50.0, 150.0],
            "weight": [2.0, 4.0, 1.0, 3.0],
            "revenue": [500.0, 1000.0, 250.0, 750.0],
        }
    )


# 1. Correction 1A: Weighted AVG with NULL value and non-NULL weight
def test_weighted_average_null_value_non_null_weight_parity():
    tbl = pa.table(
        {
            "id": [1, 2],
            "book": ["A", "A"],
            "x": pa.array([10.0, None], type=pa.float64()),
            "w": pa.array([1.0, 100.0], type=pa.float64()),
        }
    )
    adapter, source, _, _, coordinator, _ = _make_stack(tbl, "wavg_test")
    spec = PivotSpec(
        table="wavg_test",
        rows=["book"],
        measures=[Measure(field="x", agg="wavg", alias="x_wavg", weighted_field="w")],
    )
    live_q = coordinator.acquire_live_query(spec)
    rows, _ = live_q.materialize_hierarchy_rows()
    assert rows[0]["x_wavg"] == pytest.approx(10.0)

    # Now update row 2 from NULL -> 20.0 (weight still 100.0): (10*1 + 20*100) / 101 = 2010/101
    batch = source.apply_mutation_batch("wavg_test", [{"id": 2, "x": 20.0}], mode="update", key_fields=["id"])
    live_q.apply_batch(batch)
    rows2, _ = live_q.materialize_hierarchy_rows()
    assert rows2[0]["x_wavg"] == pytest.approx(2010.0 / 101.0)

    # Revert row 2 back to NULL -> must return cleanly to 10.0!
    batch2 = source.apply_mutation_batch("wavg_test", [{"id": 2, "x": None}], mode="update", key_fields=["id"])
    live_q.apply_batch(batch2)
    rows3, _ = live_q.materialize_hierarchy_rows()
    assert rows3[0]["x_wavg"] == pytest.approx(10.0)


# 2. Weighted AVG zero denominator and incremental avg initialization
def test_weighted_average_zero_denominator_and_incremental_init():
    tbl = pa.table(
        {
            "id": [1, 2],
            "book": ["A", "A"],
            "x": [10.0, 20.0],
            "w": [5.0, -5.0],
        }
    )
    _, source, _, _, coordinator, _ = _make_stack(tbl, "wavg_zero")
    spec = PivotSpec(
        table="wavg_zero",
        rows=["book"],
        measures=[
            Measure(field="x", agg="avg", alias="x_avg"),
            Measure(field="x", agg="wavg", alias="x_wavg", weighted_field="w"),
        ],
    )
    live_q = coordinator.acquire_live_query(spec)
    rows, _ = live_q.materialize_hierarchy_rows()
    assert rows[0]["x_avg"] == pytest.approx(15.0)
    assert rows[0]["x_wavg"] is None  # weight_sum == 0 -> NULL, no ZeroDivisionError

    # Update Row 1 from 10 -> 30: avg must become 25.0 (proves sufficient stats sum/count initialized!)
    batch = source.apply_mutation_batch("wavg_zero", [{"id": 1, "x": 30.0, "w": 10.0}], mode="update", key_fields=["id"])
    live_q.apply_batch(batch)
    rows2, _ = live_q.materialize_hierarchy_rows()
    assert rows2[0]["x_avg"] == pytest.approx(25.0)
    assert rows2[0]["x_wavg"] == pytest.approx((30.0 * 10.0 + 20.0 * -5.0) / 5.0)


# 3. Grouped MIN/MAX initialization (single-pass SQL) and tied extrema Tier B fallback
def test_grouped_min_max_init_and_tied_extrema_tier_b_fallback():
    tbl = pa.table(
        {
            "id": [1, 2, 3, 4],
            "book": ["A", "A", "A", "A"],
            "px": pa.array([10.0, 10.0, 25.0, None], type=pa.float64()),
        }
    )
    _, source, _, _, coordinator, _ = _make_stack(tbl, "extrema_t")
    spec = PivotSpec(
        table="extrema_t",
        rows=["book"],
        measures=[
            Measure(field="px", agg="min", alias="px_min"),
            Measure(field="px", agg="max", alias="px_max"),
        ],
    )
    live_q = coordinator.acquire_live_query(spec)
    rows, _ = live_q.materialize_hierarchy_rows()
    assert rows[0]["px_min"] == 10.0
    assert rows[0]["px_max"] == 25.0

    # Monotonic improvement (insert 5.0) stays Tier A
    b1 = source.apply_mutation_batch("extrema_t", [{"id": 5, "book": "A", "px": 5.0}], mode="insert")
    d1 = live_q.apply_batch(b1)
    assert d1.strategy_used == "tier_a_incremental"
    rows, _ = live_q.materialize_hierarchy_rows()
    assert rows[0]["px_min"] == 5.0

    # Deleting current min (5.0) triggers Tier B targeted SQL and restores 10.0
    b2 = source.apply_mutation_batch("extrema_t", [{"id": 5}], mode="delete", key_fields=["id"])
    d2 = live_q.apply_batch(b2)
    assert d2.strategy_used == "tier_b_targeted_sql"
    rows, _ = live_q.materialize_hierarchy_rows()
    assert rows[0]["px_min"] == 10.0

    # Deleting one tied 10.0 triggers Tier B and keeps 10.0; deleting second restores 25.0
    b3 = source.apply_mutation_batch("extrema_t", [{"id": 1}], mode="delete", key_fields=["id"])
    live_q.apply_batch(b3)
    assert live_q.materialize_hierarchy_rows()[0][0]["px_min"] == 10.0

    b4 = source.apply_mutation_batch("extrema_t", [{"id": 2}], mode="delete", key_fields=["id"])
    live_q.apply_batch(b4)
    assert live_q.materialize_hierarchy_rows()[0][0]["px_min"] == 25.0


# 4. Blocker 1A: Tier B revision race (rev 102 commits while Tier B for rev 101 runs -> never applied twice!)
def test_tier_b_revision_race_no_double_apply():
    tbl = pa.table({"id": [1, 2], "book": ["A", "A"], "pnl": [10.0, 50.0]})
    _, source, _, _, coordinator, _ = _make_stack(tbl, "race_t")
    spec = PivotSpec(
        table="race_t",
        rows=["book"],
        measures=[
            Measure(field="pnl", agg="min", alias="pnl_min"),
            Measure(field="pnl", agg="sum", alias="pnl_sum"),
        ],
    )
    live_q = coordinator.acquire_live_query(spec)
    assert live_q.last_applied_revision == 1

    # Rev 2 deletes current min (id=1, pnl=10), triggering Tier B SQL
    b_rev2 = source.apply_mutation_batch("race_t", [{"id": 1}], mode="delete", key_fields=["id"])
    assert b_rev2.source_revision == 2

    concurrent_batch_holder: List[Any] = []

    def _commit_rev3_before_tier_b_sql():
        # Rev 3 inserts id=3 (pnl=100) BEFORE Tier B SQL reads DuckDB!
        b_rev3 = source.apply_mutation_batch("race_t", [{"id": 3, "book": "A", "pnl": 100.0}], mode="insert")
        concurrent_batch_holder.append(b_rev3)

    delta2 = live_q.apply_batch(b_rev2, on_before_tier_b_sql=_commit_rev3_before_tier_b_sql)
    assert delta2.strategy_used == "tier_b_targeted_sql"
    assert delta2.source_revision == 3  # Tier B SQL already read rev 3!

    # Now replay rev 3 incrementally: Blocker 1A must skip it so pnl_sum stays 150.0 (not 250.0!)
    b_rev3 = concurrent_batch_holder[0]
    live_q.apply_batch(b_rev3)
    rows, gt = live_q.materialize_hierarchy_rows()
    assert rows[0]["pnl_min"] == 50.0
    assert rows[0]["pnl_sum"] == pytest.approx(150.0)
    assert gt["pnl_sum"] == pytest.approx(150.0)


# 5. Bootstrap race: viewport at rev 1, mutation 2 during subscribe, mutation 3 after
def test_bootstrap_race_revision_100_101_102():
    _, source, _, _, coordinator, broker = _make_stack(_base_trades_table())
    spec = PivotSpec(
        table="trades",
        rows=["desk"],
        measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")],
    )
    # Initial revision = 1
    assert source.read_coherent_revision("trades")[0] == 1
    # Commit rev 2 before subscribe finishes (simulating mutation between initial render & subscribe)
    source.apply_mutation_batch("trades", [{"id": 1, "pnl": 150.0}], mode="update", key_fields=["id"])

    sub, snap = broker.subscribe(
        spec=spec,
        viewport=ViewportWindow(start_row=0, end_row=10),
        known_revision=1,
    )
    assert snap["sourceRevision"] == 2
    eq_row = next(r for r in snap["rows"] if r["_id"] == "Equities")
    assert eq_row["pnl_sum"] == pytest.approx(350.0)  # 150 + 200

    # Commit rev 3 after subscribe
    source.apply_mutation_batch("trades", [{"id": 1, "pnl": 200.0}], mode="update", key_fields=["id"])
    patch = broker.poll_updates(sub.subscription_id, ack_seq=snap["seq"])
    assert patch["type"] == "PATCH"
    assert patch["sourceRevision"] == 3
    assert patch["cellPatches"][0]["value"] == pytest.approx(400.0)


# 6. Blocker 1B: Bootstrap under continuous concurrent writes (lock-free handoff)
def test_bootstrap_under_continuous_writes_lock_free_handoff():
    _, source, _, _, _, broker = _make_stack(_base_trades_table())
    spec = PivotSpec(
        table="trades",
        rows=["desk"],
        measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")],
    )
    stop_flag = threading.Event()
    writes_done = []

    def _writer():
        for i in range(1, 26):
            batch = source.apply_mutation_batch(
                "trades", [{"id": 1, "pnl": 100.0 + i * 10.0}], mode="update", key_fields=["id"]
            )
            writes_done.append(batch.source_revision)
            time.sleep(0.001)
        stop_flag.set()

    t = threading.Thread(target=_writer)
    t.start()
    time.sleep(0.005)
    sub, snap = broker.subscribe(spec=spec, viewport=ViewportWindow(start_row=0, end_row=10))
    t.join()

    # Drain any post-handoff updates
    last_frame = snap
    for _ in range(5):
        frame = broker.poll_updates(sub.subscription_id, ack_seq=last_frame.get("seq"))
        if frame["type"] in {"PATCH", "SNAPSHOT"}:
            last_frame = frame

    # Final state must equal exact SQL sum after all 25 writes: 100+250=350 for id=1, +200 for id=2 = 550!
    assert sub.live_query.last_applied_revision == writes_done[-1]
    rows, _ = sub.live_query.materialize_hierarchy_rows()
    eq_row = next(r for r in rows if r["_id"] == "Equities")
    assert eq_row["pnl_sum"] == pytest.approx(550.0)


# 7. Issue 6: Off-screen update invalidates visible % of Grand Total
def test_offscreen_update_invalidates_visible_percent_of_grand_total():
    tbl = pa.table({"id": [1, 2], "book": ["BookA", "BookZ"], "pnl": [10.0, 90.0]})
    _, source, _, _, _, broker = _make_stack(tbl, "pct_t")
    spec = PivotSpec(
        table="pct_t",
        rows=["book"],
        measures=[
            Measure(field="pnl", agg="sum", alias="pnl_sum"),
            Measure(field="pnl", agg="sum", alias="pnl_pct", window_func="percent_of_grand_total"),
        ],
    )
    # Viewport shows ONLY row 0 (BookA); BookZ is off-screen!
    sub, snap = broker.subscribe(spec=spec, viewport=ViewportWindow(start_row=0, end_row=0))
    assert len(snap["rows"]) == 1
    assert snap["rows"][0]["_id"] == "BookA"
    assert snap["rows"][0]["pnl_pct"] == pytest.approx(0.10)

    # Off-screen BookZ increases 90 -> 190 (Grand Total 100 -> 200)
    source.apply_mutation_batch("pct_t", [{"id": 2, "pnl": 190.0}], mode="update", key_fields=["id"])
    patch = broker.poll_updates(sub.subscription_id, ack_seq=snap["seq"])
    assert patch["type"] == "PATCH"
    pct_patch = next(p for p in patch["cellPatches"] if p["rowId"] == "BookA" and p["columnId"] == "pnl_pct")
    assert pct_patch["value"] == pytest.approx(0.05)
    assert patch["grandTotalPatch"]["pnl_sum"]["value"] == pytest.approx(200.0)


# 8. Issue 7 & 2.8: Deleting last member of visible group in live vs. baseline mode
def test_delete_last_member_of_visible_hierarchy_group_live_vs_baseline():
    tbl = pa.table(
        {
            "id": [1, 2],
            "region": ["EMEA", "AMER"],
            "country": ["France", "USA"],
            "pnl": [500.0, 300.0],
            "price": [100.0, 200.0],
        }
    )
    _, source, snapshots, _, _, broker = _make_stack(tbl, "grp_del")
    base = snapshots.capture_snapshot("grp_del", label="T0")
    spec = PivotSpec(
        table="grp_del",
        rows=["region", "country"],
        measures=[
            Measure(field="pnl", agg="sum", alias="pnl_sum"),
            Measure(field="price", agg="avg", alias="price_avg"),
        ],
    )
    sub_live, snap_live = broker.subscribe(spec=spec, viewport=ViewportWindow(start_row=0, end_row=10))
    sub_base, snap_base = broker.subscribe(
        spec=spec, viewport=ViewportWindow(start_row=0, end_row=10), baseline_id=base.snapshot_id
    )

    # Delete only trade in EMEA / France
    source.apply_mutation_batch("grp_del", [{"id": 1}], mode="delete", key_fields=["id"])

    # Live mode removes EMEA & EMEA|||France
    upd_live = broker.poll_updates(sub_live.subscription_id, ack_seq=snap_live["seq"])
    assert upd_live["type"] in {"PATCH", "SNAPSHOT"}
    if upd_live["type"] == "SNAPSHOT":
        assert all("France" not in r["_path"] for r in upd_live["rows"])
    else:
        assert any(op["op"] == "remove" and "France" in op["rowId"] for op in upd_live["rowOps"])

    # Baseline mode keeps EMEA|||France with _diffStatus="removed", SUM->0/-500, AVG->None/None!
    upd_base = broker.poll_updates(sub_base.subscription_id, ack_seq=snap_base["seq"])
    snap_after = broker.modify_viewport(sub_base.subscription_id, ViewportWindow(start_row=0, end_row=10))
    fr_row = next(r for r in snap_after["rows"] if r["_path"] == "EMEA|||France")
    assert fr_row["_diffStatus"] == "removed"
    assert fr_row["pnl_sum"] == 0.0
    assert fr_row["_delta"]["pnl_sum"] == pytest.approx(-500.0)
    assert fr_row["_pctDelta"]["pnl_sum"] == pytest.approx(-1.0)
    assert fr_row["price_avg"] is None
    assert fr_row["_delta"]["price_avg"] is None


# 9. Issue 8: Upsert moves row across hierarchy group AND pivot column (discovers Q4, removes Q2)
def test_upsert_moves_row_across_hierarchy_group_and_pivot_column():
    _, source, _, _, _, broker = _make_stack(_base_trades_table())
    spec = PivotSpec(
        table="trades",
        rows=["desk", "book"],
        columns=["quarter"],
        measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")],
    )
    sub, snap = broker.subscribe(spec=spec, viewport=ViewportWindow(start_row=0, end_row=20))
    assert snap["colKeys"] == ["Q1", "Q2"]

    # Move row 4 from (Rates, UST, Q2) -> (FX, Spot, Q4): Q2 empties, Q4 appears!
    source.apply_mutation_batch(
        "trades",
        [{"id": 4, "desk": "FX", "book": "Spot", "quarter": "Q4", "pnl": 400.0}],
        mode="upsert",
        key_fields=["id"],
    )
    upd = broker.poll_updates(sub.subscription_id, ack_seq=snap["seq"])
    assert upd["type"] == "SNAPSHOT"
    assert upd["colKeys"] == ["Q1", "Q4"]
    fx_row = next(r for r in upd["rows"] if r["_path"] == "FX|||Spot")
    assert fx_row["Q4_pnl_sum"] == pytest.approx(400.0)


# 10 & 11. Problem B: Live sort off-screen group enters Rank 1 vs. in_place Sort Lock
def test_live_sort_offscreen_group_crosses_viewport_boundary_and_sort_lock():
    n = 60
    tbl = pa.table(
        {
            "id": list(range(1, n + 1)),
            "book": [f"Book_{i:03d}" for i in range(1, n + 1)],
            "pnl": [float(1000 - i * 10) for i in range(1, n + 1)],
        }
    )
    _, source, _, _, _, broker = _make_stack(tbl, "rank_t")
    spec = PivotSpec(
        table="rank_t",
        rows=["book"],
        measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")],
    )
    # Live sort subscription (top 5 rows)
    sub_live, snap_live = broker.subscribe(
        spec=spec,
        viewport=ViewportWindow(
            start_row=0,
            end_row=4,
            sort_specs=({"field": "pnl_sum", "order": "desc"},),
            sort_mode="live_sort",
        ),
    )
    # In-place locked subscription (top 5 rows)
    sub_lock, snap_lock = broker.subscribe(
        spec=spec,
        viewport=ViewportWindow(
            start_row=0,
            end_row=4,
            sort_specs=({"field": "pnl_sum", "order": "desc"},),
            sort_mode="in_place",
        ),
    )
    assert snap_live["rows"][0]["_id"] == "Book_001"
    assert snap_lock["rows"][0]["_id"] == "Book_001"

    # Off-screen Book_060 jumps from 400 -> 500000!
    source.apply_mutation_batch("rank_t", [{"id": 60, "pnl": 500000.0}], mode="update", key_fields=["id"])

    upd_live = broker.poll_updates(sub_live.subscription_id, ack_seq=snap_live["seq"])
    assert upd_live["type"] in {"PATCH", "SNAPSHOT"}
    live_top = broker.modify_viewport(sub_live.subscription_id, sub_live.viewport)
    assert live_top["rows"][0]["_id"] == "Book_060"

    # In-place subscription keeps Book_001 at index 0 while updating Grand Total!
    upd_lock = broker.poll_updates(sub_lock.subscription_id, ack_seq=snap_lock["seq"])
    assert upd_lock["type"] == "PATCH"
    assert upd_lock["grandTotalPatch"]["pnl_sum"]["value"] > 500000.0
    assert sub_lock.last_emitted_rows[0]["_id"] == "Book_001"


# 12. Correction 1B & Issue 3: Honest snapshot strategies + cross-connection DuckDB visibility
def test_baseline_snapshot_cross_connection_visibility_and_strategies(tmp_path):
    db_file = tmp_path / "snap_test.duckdb"
    tbl = _base_trades_table()
    adapter, source, snapshots, _, _, _ = _make_stack(tbl, "trades", backend_uri=f"duckdb:///{db_file}")
    try:
        # Unmutated Arrow source -> true zero-copy IMMUTABLE_ARROW_REF
        d1 = snapshots.capture_snapshot("trades", label="ZeroCopy")
        assert d1.strategy == SnapshotStrategy.IMMUTABLE_ARROW_REF
        assert d1.immutable_verified is True

        # Mutate in-place -> now physical DuckDB table -> DUCKDB_PERSISTENT_SNAPSHOT
        source.apply_mutation_batch("trades", [{"id": 1, "pnl": 999.0}], mode="update", key_fields=["id"])
        d2 = snapshots.capture_snapshot("trades", label="PhysicalSnap")
        assert d2.strategy == SnapshotStrategy.DUCKDB_PERSISTENT_SNAPSHOT
        assert d2.physical_table_name is not None

        # Query physical snapshot from a separate DuckDB cursor on the same connection
        raw_con = source._raw_duckdb()
        cur2 = raw_con.cursor()
        cnt = cur2.execute(f"SELECT COUNT(*) FROM {d2.physical_table_name}").fetchone()[0]
        assert cnt == 4
    finally:
        snapshots.close()
        source.close()
        adapter.controller.close()


# 13. Issue 4: Two-tier LiveQueryKey shares one SharedLiveQuery across different baselines
def test_two_tier_shared_live_query_key_across_different_baselines():
    _, source, snapshots, _, coordinator, broker = _make_stack(_base_trades_table())
    b1 = snapshots.capture_snapshot("trades", label="B1")
    source.apply_mutation_batch("trades", [{"id": 1, "pnl": 120.0}], mode="update", key_fields=["id"])
    b2 = snapshots.capture_snapshot("trades", label="B2")

    spec = PivotSpec(table="trades", rows=["desk"], measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")])
    sub1, _ = broker.subscribe(spec=spec, viewport=ViewportWindow(), baseline_id=b1.snapshot_id)
    sub2, _ = broker.subscribe(spec=spec, viewport=ViewportWindow(), baseline_id=b2.snapshot_id)
    assert sub1.live_query is sub2.live_query
    assert coordinator.active_live_query_count() == 1


# 14. Issue 5: Process crash / restart serverEpochId discontinuity
def test_crash_recovery_server_epoch_discontinuity():
    _, source, _, _, _, broker = _make_stack(_base_trades_table())
    spec = PivotSpec(table="trades", rows=["desk"], measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")])
    sub, snap = broker.subscribe(spec=spec, viewport=ViewportWindow())
    frame = broker.poll_updates(sub.subscription_id, client_server_epoch_id="old_crashed_epoch_uuid")
    assert frame["type"] == "INVALIDATE"
    assert frame["reason"] == "server_epoch_changed"


# 15. RLS isolation on shared PivotSpec
def test_rls_isolation_on_shared_pivot_spec():
    authorizer = RuntimeDatasetAuthorizer(
        {
            "trades": {
                "roles": {"trader"},
                "operations": {"read"},
                "rowSecurity": {"mode": "claims", "claims": {"allowed_desk": "desk"}},
            }
        }
    )
    _, _, _, _, coordinator, broker = _make_stack(
        _base_trades_table(),
        authorizer=lambda principal, table: authorizer.authorize(principal=principal, table=table, operation="read"),
    )
    spec = PivotSpec(table="trades", rows=["desk"], measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")])
    p_eq = _Principal(id="u_eq", role="trader", attributes={"allowed_desk": "Equities"})
    p_rt = _Principal(id="u_rt", role="trader", attributes={"allowed_desk": "Rates"})

    sub_eq, snap_eq = broker.subscribe(spec=spec, viewport=ViewportWindow(), principal=p_eq)
    sub_rt, snap_rt = broker.subscribe(spec=spec, viewport=ViewportWindow(), principal=p_rt)
    assert sub_eq.live_query is not sub_rt.live_query
    assert coordinator.active_live_query_count() == 2
    assert [r["_id"] for r in snap_eq["rows"]] == ["Equities"]
    assert [r["_id"] for r in snap_rt["rows"]] == ["Rates"]


# 16 & 17. Blocker 1C: Freeze Snapshot historical consistency + fail-closed expiry + Pause Live scroll resume
def test_frozen_historical_export_vs_pause_live_and_expiry():
    _, source, snapshots, _, _, broker = _make_stack(_base_trades_table())
    spec = PivotSpec(table="trades", rows=["desk"], measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")])
    sub, snap = broker.subscribe(spec=spec, viewport=ViewportWindow(start_row=0, end_row=10))

    # Freeze snapshot at rev 1 (Equities = 300)
    frozen = broker.set_pause_mode(sub.subscription_id, "freeze_snapshot", ttl_seconds=0.15)
    assert frozen["pauseMode"] == "freeze_snapshot"
    assert frozen["sourceRevision"] == 1

    # Advance live table to rev 2 & 3 (Equities = 900)
    source.apply_mutation_batch("trades", [{"id": 1, "pnl": 400.0}], mode="update", key_fields=["id"])
    source.apply_mutation_batch("trades", [{"id": 2, "pnl": 500.0}], mode="update", key_fields=["id"])

    # Scrolling / querying frozen subscription still returns exact rev 1 data!
    scrolled = broker.modify_viewport(sub.subscription_id, ViewportWindow(start_row=0, end_row=10))
    assert scrolled["sourceRevision"] == 1
    assert next(r for r in scrolled["rows"] if r["_id"] == "Equities")["pnl_sum"] == pytest.approx(300.0)

    # Wait for snapshot TTL expiry -> must fail closed with SnapshotExpiredError!
    time.sleep(0.2)
    with pytest.raises(SnapshotExpiredError):
        broker.poll_updates(sub.subscription_id)

    # Now test Pause Live -> scroll -> resume resync
    sub2, snap2 = broker.subscribe(spec=spec, viewport=ViewportWindow(start_row=0, end_row=0))
    broker.set_pause_mode(sub2.subscription_id, "pause_live")
    source.apply_mutation_batch("trades", [{"id": 1, "pnl": 600.0}], mode="update", key_fields=["id"])
    hb = broker.poll_updates(sub2.subscription_id)
    assert hb["type"] == "HEARTBEAT"
    assert hb["bufferedUpdates"] >= 1
    # Modifying viewport while paused unpauses and returns fresh SNAPSHOT at latest rev!
    resumed = broker.modify_viewport(sub2.subscription_id, ViewportWindow(start_row=0, end_row=10))
    assert resumed["type"] == "SNAPSHOT"
    assert resumed["sourceRevision"] == 4


# 18. Correction 2.6: Unacked SSE conflation & structural upgrade to SNAPSHOT
def test_unacked_sse_conflation_and_structural_snapshot_upgrade():
    _, source, _, _, _, broker = _make_stack(_base_trades_table())
    spec = PivotSpec(table="trades", rows=["desk"], measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")])
    sub, snap = broker.subscribe(spec=spec, viewport=ViewportWindow(start_row=0, end_row=10))
    sub.acknowledge(snap["seq"])

    # Update 1 without client ACK
    source.apply_mutation_batch("trades", [{"id": 1, "pnl": 150.0}], mode="update", key_fields=["id"])
    p1 = broker.poll_updates(sub.subscription_id)
    assert p1["type"] == "PATCH"
    assert p1["cellPatches"][0]["value"] == pytest.approx(350.0)

    # Update 2 still unacked -> value patch uses absolute final value & prevValue relative to lastAckSeq (300.0)!
    source.apply_mutation_batch("trades", [{"id": 1, "pnl": 200.0}], mode="update", key_fields=["id"])
    p2 = broker.poll_updates(sub.subscription_id)
    assert p2["type"] == "PATCH"
    assert p2["cellPatches"][0]["value"] == pytest.approx(400.0)
    assert p2["cellPatches"][0]["prevValue"] == pytest.approx(300.0)

    # Structural insert while still unacked -> must upgrade to authoritative SNAPSHOT!
    source.apply_mutation_batch(
        "trades",
        [{"id": 5, "desk": "Crypto", "book": "BTC", "quarter": "Q1", "pnl": 50.0, "weight": 1.0, "revenue": 100.0}],
        mode="insert",
    )
    p3 = broker.poll_updates(sub.subscription_id)
    assert p3["type"] == "SNAPSHOT"


# 19. Correction 2: Mid-stream authorization revocation emits AUTH_REVOKED and closes stream
def test_midstream_sse_security_revocation_and_403_reconnect():
    allowed = {"active": True}

    def _dynamic_auth(principal, table):
        if not allowed["active"]:
            raise PermissionError("revoked")
        return None

    _, source, _, _, _, broker = _make_stack(_base_trades_table(), authorizer=_dynamic_auth)
    spec = PivotSpec(table="trades", rows=["desk"], measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")])
    sub, snap = broker.subscribe(spec=spec, viewport=ViewportWindow())
    assert snap["type"] == "SNAPSHOT"

    # Revoke mid-stream
    allowed["active"] = False
    revoked = broker.poll_updates(sub.subscription_id)
    assert revoked["type"] == "AUTH_REVOKED"
    assert broker.get_subscription(sub.subscription_id) is None


# 20 & 21. Exact Decimal128 precision + CanonicalPathCodec delimiters/NULLs/types
def test_decimal128_mixed_precision_and_canonical_path_codec():
    val1 = Decimal("123456789012345678.123456")
    val2 = Decimal("0.000001")
    tbl = pa.table(
        {
            "id": [1, 2],
            "grp": ["A|||B", "A|||B"],
            "amt": pa.array([val1, val2], type=pa.decimal128(38, 6)),
            "w": pa.array([Decimal("2.500000"), Decimal("-0.500000")], type=pa.decimal128(18, 6)),
        }
    )
    _, source, _, _, coordinator, _ = _make_stack(tbl, "dec_t")
    spec = PivotSpec(
        table="dec_t",
        rows=["grp"],
        measures=[
            Measure(field="amt", agg="sum", alias="amt_sum"),
            Measure(field="amt", agg="wavg", alias="amt_wavg", weighted_field="w"),
        ],
    )
    live_q = coordinator.acquire_live_query(spec)
    rows, _ = live_q.materialize_hierarchy_rows()
    assert rows[0]["amt_sum"] == Decimal("123456789012345678.123457")
    wire = encode_wire_value(rows[0]["amt_sum"])
    assert wire == {"__decimal__": "123456789012345678.123457", "scale": 6}
    assert decode_wire_value(wire) == Decimal("123456789012345678.123457")

    # CanonicalPathCodec collision immunity
    p_delim = CanonicalPathCodec.encode(["A|||B", "C"])
    p_split = CanonicalPathCodec.encode(["A", "B|||C"])
    p_null = CanonicalPathCodec.encode([None])
    p_str_null = CanonicalPathCodec.encode(["null"])
    p_empty = CanonicalPathCodec.encode([""])
    p_int = CanonicalPathCodec.encode([10])
    p_str_int = CanonicalPathCodec.encode(["10"])
    assert len({p_delim, p_split, p_null, p_str_null, p_empty, p_int, p_str_int}) == 7
    assert CanonicalPathCodec.decode(p_delim) == ("A|||B", "C")
    assert CanonicalPathCodec.decode(p_null) == (None,)


# 22. Snapshot resource exhaustion rollback leaves zero orphan tables or leaked governor bytes
def test_snapshot_resource_exhaustion_rollback_no_orphans():
    tiny_gov = ResourceGovernor(
        ResourceLimits(
            total_bytes=2 * 1024 * 1024,
            headroom_bytes=256 * 1024,
            cache_bytes=16 * 1024,  # Smaller than _MIN_RESERVATION_BYTES (64 KiB)!
            payload_bytes=512 * 1024,
            cdc_bytes=512 * 1024,
        )
    )
    _, source, snapshots, _, _, _ = _make_stack(_base_trades_table(), resource_governor=tiny_gov)
    source.apply_mutation_batch("trades", [{"id": 1, "pnl": 101.0}], mode="update", key_fields=["id"])
    raw_con = source._raw_duckdb()
    tables_before = {r[0] for r in raw_con.execute("SHOW TABLES").fetchall()}

    with pytest.raises(ResourceExhaustedError):
        snapshots.capture_snapshot("trades", label="TooBig")

    assert snapshots.list_snapshots("trades") == []
    tables_after = {r[0] for r in raw_con.execute("SHOW TABLES").fetchall()}
    assert not any(t.startswith("__pivot_snap_") for t in (tables_after - tables_before))
    assert tiny_gov.snapshot()["reserved_bytes"] == 0


# 23. Concurrent schema evolution invalidates stale state and bumps queryEpoch
def test_concurrent_schema_evolution_invalidates_and_bumps_query_epoch():
    _, source, _, _, _, broker = _make_stack(_base_trades_table())
    spec = PivotSpec(table="trades", rows=["desk"], measures=[Measure(field="pnl", agg="sum", alias="pnl_sum")])
    sub, snap = broker.subscribe(spec=spec, viewport=ViewportWindow())
    initial_epoch = snap["queryEpoch"]

    # Alter schema on DuckDB and notify schema change
    source._raw_duckdb().execute("ALTER TABLE trades ADD COLUMN desk_tier VARCHAR DEFAULT 'Core'")
    source.notify_schema_change("trades")

    upd = broker.poll_updates(sub.subscription_id, ack_seq=snap["seq"])
    assert upd["type"] == "SNAPSHOT"
    assert upd["queryEpoch"] == initial_epoch + 1
    assert upd["schemaVersion"] != snap["schemaVersion"]


# 24, 25, 26. Multi-process file-backed DuckDB guard, honest capability tiers, & Redis failure recovery
def test_single_owner_guard_capability_tiers_and_redis_reconcile(tmp_path):
    db_file = tmp_path / "owned.duckdb"
    a1 = create_tanstack_adapter(backend_uri=f"duckdb:///{db_file}")
    a1.controller.load_data_from_arrow("t", _base_trades_table())
    s1 = VersionedSourceAdapter(a1.controller)
    try:
        with pytest.raises(DuckDBSingleOwnerError):
            VersionedSourceAdapter(a1.controller)
    finally:
        s1.close()
        a1.controller.close()

    # Honest SourceChangeProvider capability tiers
    provider = SourceChangeProvider()
    cap_ext = provider.resolve_capabilities("ext_tbl", backend_name="postgres", managed_local_duckdb=False)
    assert cap_ext.tier == SourceCapabilityTier.TIER_4_PERIODIC_REFRESH
    assert cap_ext.supports_exact_row_deltas is False

    provider.register_change_token_provider("token_tbl", lambda _t: "snap-42")
    cap_tok = provider.resolve_capabilities("token_tbl", backend_name="snowflake", managed_local_duckdb=False, supports_time_travel=True)
    assert cap_tok.tier == SourceCapabilityTier.TIER_2_CHANGE_TOKEN

    # Redis publish failure does not corrupt sourceRevision and recovers on reconcile
    class _FailingRedisClient:
        def __init__(self):
            self.fail = True
            self.events = []

        def xadd(self, key, fields, **kwargs):
            if self.fail:
                raise ConnectionError("redis down")
            self.events.append((key, fields))

    class _FakeDist:
        def __init__(self):
            self.client = _FailingRedisClient()
            self.settings = type("S", (), {"key_prefix": "test"})()

    dist = _FakeDist()
    a2 = create_tanstack_adapter()
    a2.controller.load_data_from_arrow("redis_trades", _base_trades_table())
    s2 = VersionedSourceAdapter(a2.controller, distributed_runtime=dist)
    b = s2.apply_mutation_batch("redis_trades", [{"id": 1, "pnl": 111.0}], mode="update", key_fields=["id"])
    assert b.source_revision == 1
    assert "redis_trades" in s2._pending_redis_reconciliations

    dist.client.fail = False
    recovered = s2.reconcile_pending_observations()
    assert recovered == ["redis_trades"]
    assert len(dist.client.events) == 1
    s2.close()


# 27 & 28. Post-formula Margin % pp diff + Top-3 PnL Explain drivers + Admission policy downgrade
def test_baseline_post_formula_margin_pp_pnl_explain_and_admission():
    tbl = pa.table(
        {
            "id": [1, 2],
            "desk": ["Equities", "Equities"],
            "book": ["NVDA", "AAPL"],
            "profit": [10.0, 10.0],
            "revenue": [50.0, 50.0],
        }
    )
    _, source, snapshots, _, coordinator, broker = _make_stack(tbl, "pnl_t")
    base = snapshots.capture_snapshot("pnl_t", label="T0")  # Margin = 20/100 = 20%

    # NVDA profit jumps 10 -> 20, revenue 50 -> 70 => Total Profit=30, Revenue=120 => Margin=25% (+5 pp!)
    source.apply_mutation_batch(
        "pnl_t", [{"id": 1, "profit": 20.0, "revenue": 70.0}], mode="update", key_fields=["id"]
    )
    spec = PivotSpec(
        table="pnl_t",
        rows=["desk", "book"],
        measures=[
            Measure(field="profit", agg="sum", alias="profit_sum"),
            Measure(field="revenue", agg="sum", alias="revenue_sum"),
            Measure(field="margin", agg="formula", alias="margin", expression="[profit_sum] / [revenue_sum]", ratio_format="percentage"),
        ],
    )
    live_q = coordinator.acquire_live_query(spec)
    comp = coordinator.compute_comparison_for_live_query(live_q, baseline_id=base.snapshot_id)
    eq_row = next(r for r in comp.rows if r.display_path == "Equities")
    # Independent post-formula diff: 0.25 - 0.20 = +0.05 (NOT 10/20 = 0.50!)
    assert eq_row.diffs["margin"].delta == pytest.approx(0.05)
    assert eq_row.diffs["margin"].delta_unit == "pp"
    # Top-3 PnL Explain child driver identifies NVDA
    assert eq_row.variance_drivers["profit_sum"][0]["label"] == "NVDA"
    assert eq_row.variance_drivers["profit_sum"][0]["pctShare"] == pytest.approx(1.0)

    # 0 -> 0 relative percentage diff is None (undefined denominator)
    z = compute_measure_diff(0.0, 0.0, agg="sum")
    assert z.delta == 0.0
    assert z.pct_delta is None

    # Admission policy downgrade when max_incremental_bytes is tiny
    tiny_coord = ReactiveQueryCoordinator(
        coordinator._controller,
        source,
        snapshots,
        coordinator._comparison_service,
        max_incremental_bytes_per_query=1,  # forces QUERY_ONLY_DIFF
    )
    q_downgraded = tiny_coord.acquire_live_query(spec)
    assert q_downgraded.strategy == ExecutionStrategy.QUERY_ONLY_DIFF


# 29 & 30. FastAPI HTTP/SSE endpoints + Dash same-origin fast-lane endpoint
def test_fastapi_and_dash_subscription_endpoints():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from dash import Dash, html
    from serverside_pivot import DashTanstackPivot

    adapter = create_tanstack_adapter()
    adapter.controller.load_data_from_arrow("trades", _base_trades_table())
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    auth_policy = RuntimeDatasetAuthorizer(
        {"trades": {"roles": {"trader"}, "operations": {"read", "export"}, "rowSecurity": {"mode": "global"}}}
    )
    principal = _Principal(id="alice", role="trader")

    # FastAPI endpoints
    fapp = FastAPI()
    register_runtime_http_transport(
        fapp,
        lambda: service,
        current_user_dependency=lambda: principal,
        dataset_authorizer=auth_policy,
    )
    client = TestClient(fapp)
    r_sub = client.post(
        "/v1/runtime/subscription",
        json={
            "table": "trades",
            "action": "subscribe",
            "state": {
                "rowFields": ["desk"],
                "valConfigs": [{"field": "pnl", "agg": "sum", "alias": "pnl_sum"}],
            },
            "payload": {"start": 0, "end": 10},
        },
    )
    assert r_sub.status_code == 200
    snap = r_sub.json()
    assert snap["type"] == "SNAPSHOT"
    sub_id = snap["subscriptionId"]

    adapter.push_stream_changes("trades", [{"id": 1, "pnl": 250.0}], mode="update", key_fields=["id"])
    r_poll = client.post(
        "/v1/runtime/subscription",
        json={"table": "trades", "action": "poll", "payload": {"subscriptionId": sub_id, "ackSeq": snap["seq"]}},
    )
    assert r_poll.status_code == 200
    assert r_poll.json()["type"] == "PATCH"

    # Dash same-origin fast-lane endpoint
    dapp = Dash(__name__)
    dapp.layout = html.Div([DashTanstackPivot(id="p1", table="trades", serverSide=True)])
    register_pivot_app(
        dapp,
        adapter_getter=lambda: adapter,
        pivot_id="p1",
        runtime_dataset_authorizer=auth_policy,
        runtime_principal_getter=lambda _req: principal,
    )
    flask_client = dapp.server.test_client()
    r_dash = flask_client.post(
        "/_serverside_pivot/subscription",
        json={
            "pivotId": "p1",
            "table": "trades",
            "action": "capture_baseline",
            "payload": {"label": "DashEOD"},
        },
    )
    assert r_dash.status_code == 200
    body = r_dash.get_json()
    assert body["type"] == "BASELINE_CAPTURED"
    assert body["snapshot"]["label"] == "DashEOD"


def test_concurrent_expand_collapse_filter_sort_with_live_updates():
    """Verify concurrent expand/collapse/filter/sort actions and live stream ticks never race or drop."""
    from pivot_engine.runtime.models import PivotRequestContext, PivotViewState
    from pivot_engine.runtime.subscription_service import RuntimeSubscriptionService

    adapter = create_tanstack_adapter()
    adapter.controller.load_data_from_arrow("trades", _base_trades_table())
    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    sub_service = RuntimeSubscriptionService(adapter_getter=lambda: adapter)

    base = adapter.capture_baseline("trades", label="EOD")
    base_state = {
        "rowFields": ["desk", "book"],
        "colFields": ["quarter"],
        "valConfigs": [{"id": "pnl_sum", "field": "pnl", "agg": "sum"}],
        "filters": {},
        "sorting": [],
        "expanded": {},
    }
    sub_snap = sub_service.handle_command(
        table="trades",
        action="subscribe",
        state=base_state,
        payload={"startRow": 0, "endRow": 50, "baselineId": base.snapshot_id},
    )
    assert sub_snap["type"] == "SNAPSHOT"
    sub_id = sub_snap["subscriptionId"]
    # Collapsed initially: only root desk rows ("Equities", "Rates")
    assert [r["_path"] for r in sub_snap["rows"]] == ["Equities", "Rates"]

    errors = []

    def _writer():
        for i in range(12):
            try:
                adapter.push_stream_changes(
                    "trades",
                    [{"id": 1, "pnl": 100.0 + i * 5.0}, {"id": 3, "pnl": 200.0 - i * 3.0}],
                    mode="update",
                    key_fields=["id"],
                )
            except Exception as exc:
                errors.append(exc)

    def _actions_and_polls():
        for step in range(6):
            try:
                # 1. Expand Equities
                exp_state = PivotViewState(
                    row_fields=["desk", "book"],
                    col_fields=["quarter"],
                    val_configs=[{"id": "pnl_sum", "field": "pnl", "agg": "sum"}],
                    filters={},
                    sorting=[],
                    expanded={"Equities": True},
                )
                ctx_exp = PivotRequestContext.from_frontend(
                    table="trades",
                    trigger_prop="p.runtimeRequest",
                    viewport={
                        "start": 0,
                        "end": 50,
                        "window_seq": step * 4 + 1,
                        "state_epoch": step + 1,
                        "session_id": "s1",
                        "client_instance": "c1",
                        "intent": "expansion",
                    },
                )
                res_exp = service.process(exp_state, ctx_exp)
                assert res_exp.status == "data"
                assert any(r.get("_path") == "Equities|||Index" for r in (res_exp.data or []))

                # Sync expanded viewport to subscription
                mod_frame = sub_service.handle_command(
                    table="trades",
                    action="modify_viewport",
                    state={**base_state, "expanded": {"Equities": True}},
                    payload={"subscriptionId": sub_id, "startRow": 0, "endRow": 50, "baselineId": base.snapshot_id},
                )
                assert mod_frame["type"] == "SNAPSHOT"
                assert "Equities|||Index" in [r["_path"] for r in mod_frame["rows"]]

                # 2. Poll live updates while expanded
                poll_frame = sub_service.handle_command(
                    table="trades",
                    action="poll",
                    state={**base_state, "expanded": {"Equities": True}},
                    payload={"subscriptionId": sub_id, "ackSeq": mod_frame["seq"]},
                )
                assert poll_frame["type"] in {"PATCH", "HEARTBEAT", "SNAPSHOT"}

                # 3. Collapse Equities & apply filter
                flt_state = PivotViewState(
                    row_fields=["desk", "book"],
                    col_fields=["quarter"],
                    val_configs=[{"id": "pnl_sum", "field": "pnl", "agg": "sum"}],
                    filters={"desk": {"conditions": [{"type": "eq", "value": "Equities"}]}},
                    sorting=[{"id": "Q1_pnl_sum", "desc": True}],
                    expanded={},
                )
                ctx_flt = PivotRequestContext.from_frontend(
                    table="trades",
                    trigger_prop="p.runtimeRequest",
                    viewport={
                        "start": 0,
                        "end": 50,
                        "window_seq": step * 4 + 2,
                        "state_epoch": step + 2,
                        "session_id": "s1",
                        "client_instance": "c1",
                        "intent": "structural",
                    },
                )
                res_flt = service.process(flt_state, ctx_flt)
                assert res_flt.status == "data"
            except Exception as exc:
                errors.append(exc)

    t1 = threading.Thread(target=_writer)
    t2 = threading.Thread(target=_actions_and_polls)
    t1.start()
    t2.start()
    t1.join(timeout=15)
    t2.join(timeout=15)
    assert errors == []

