"""Live (incremental) results equal the core over the same committed revision (REQ-LIVE-002).

The reactive coordinator maintains aggregates incrementally (Tier A) with
targeted (Tier B) and full (Tier C) re-queries.  After every committed batch
its visible tree must equal a fresh core evaluation of the current table:
same nodes (by canonical typed identity) and the same values.
"""

from __future__ import annotations

import math

import ibis
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pivot_engine.core.exec import execute
from pivot_engine.core.identity import CanonicalPathCodec
from pivot_engine.core.ir import MeasureRef, PivotQuery
from pivot_engine.types.pivot_spec import Measure, PivotSpec

from ..test_streaming_diff_correctness import _make_stack

DIM = st.sampled_from(["A", "B", "a", "", None])
VAL = st.one_of(st.none(), st.integers(-9, 9).map(float))
ROW = st.fixed_dictionaries({"r": DIM, "c": DIM, "v": VAL})
AGGS = ["sum", "count", "avg", "min", "max"]

MUTATION = st.one_of(
    st.tuples(st.just("insert"), ROW),
    st.tuples(st.just("update"), st.integers(0, 9), st.sampled_from(["r", "c", "v"]), st.one_of(DIM, VAL)),
    st.tuples(st.just("delete"), st.integers(0, 9)),
)


def _same(a, b):
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)


def _table(rows):
    return pa.table({
        "id": pa.array([r["id"] for r in rows], pa.int64()),
        "r": pa.array([r["r"] for r in rows], pa.string()),
        "c": pa.array([r["c"] for r in rows], pa.string()),
        "v": pa.array([r["v"] for r in rows], pa.float64()),
    })


def _core_nodes(current: pa.Table, aggs):
    con = ibis.duckdb.connect()
    con.create_table("lt", current)
    q = PivotQuery(dataset="lt", rows=("r", "c"), measures=tuple(MeasureRef(f"v_{a}", a, "v") for a in aggs))
    return {CanonicalPathCodec.encode(list(rec["path"])): rec["values"]
            for rec in execute(q, con.table).records() if rec["path"]}


def _live_nodes(live, aggs):
    rows, _ = live.materialize_hierarchy_rows()
    return {r["_canonicalPath"]: {f"v_{a}": r.get(f"v_{a}") for a in aggs} for r in rows if not r.get("_isTotal")}


@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
@given(initial=st.lists(ROW, min_size=1, max_size=8), mutations=st.lists(MUTATION, max_size=6),
       aggs=st.lists(st.sampled_from(AGGS), min_size=1, max_size=3, unique=True))
def test_live_tree_equals_core_after_every_batch(initial, mutations, aggs):  # REQ-LIVE-002
    rows = [{"id": i, **r} for i, r in enumerate(initial)]
    adapter, source, _, _, coordinator, _ = _make_stack(_table(rows), "lt")
    spec = PivotSpec(table="lt", rows=["r", "c"], measures=[Measure(field="v", agg=a, alias=f"v_{a}") for a in aggs])
    live = coordinator.acquire_live_query(spec)
    next_id = len(rows)
    for step, m in enumerate(mutations):
        live_ids = [r["id"] for r in rows]
        if m[0] == "insert":
            payload, mode = [{"id": next_id, **m[1]}], "insert"
            rows.append(dict(payload[0]))
            next_id += 1
        elif not live_ids:
            continue
        elif m[0] == "update":
            target = live_ids[m[1] % len(live_ids)]
            field, value = m[2], m[3]
            if field == "v" and isinstance(value, str) or field != "v" and isinstance(value, float):
                continue  # keep column types
            payload, mode = [{"id": target, field: value}], "update"
            next(r for r in rows if r["id"] == target)[field] = value
        else:
            target = live_ids[m[1] % len(live_ids)]
            payload, mode = [{"id": target}], "delete"
            rows = [r for r in rows if r["id"] != target]
        live.apply_batch(source.apply_mutation_batch("lt", payload, mode=mode, key_fields=["id"]))
        current = adapter.controller.resolve_table("lt").to_pyarrow()
        assert sorted(current.to_pylist(), key=lambda r: r["id"]) == sorted(rows, key=lambda r: r["id"])
        expected, got = _core_nodes(current, aggs), _live_nodes(live, aggs)
        assert set(got) == set(expected), f"step {step}: live nodes differ from the core"
        for key, values in expected.items():
            bad = {k: (got[key][k], v) for k, v in values.items() if not _same(got[key][k], v)}
            assert not bad, f"step {step} {CanonicalPathCodec.decode(key)}: (live, core) {bad}"


@pytest.mark.xfail(strict=True, reason="F-LIVE-NULL-ORDER: the live tree orders the NULL member first (REQ-BACK-001)")
def test_live_tree_orders_null_members_last():
    rows = [{"id": 0, "r": None, "c": "x", "v": 1.0}, {"id": 1, "r": "A", "c": "x", "v": 2.0}]
    _, _, _, _, coordinator, _ = _make_stack(_table(rows), "lt")
    live = coordinator.acquire_live_query(
        PivotSpec(table="lt", rows=["r"], measures=[Measure(field="v", agg="sum", alias="v_sum")]))
    order = [r.get("r") for r in live.materialize_hierarchy_rows()[0] if not r.get("_isTotal")]
    assert order == ["A", None]
