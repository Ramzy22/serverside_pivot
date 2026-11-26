"""Physical cube preparation and cache-provider contracts.

Coverage:
* test_provider_uses_e0_and_restricted_child_cubes: 3 backends x 2 totals
  modes x filters present/absent; null parents, expansion, pivot sorts,
  provider deduplication and no raw-table dependencies after materialization.
* test_grain_reaches_filters_below_collapsed_nodes_and_member_sorts: grain
  selection, including filter levels below visible depth and member prefixes.
* test_cube_reuses_scalar_minima_after_sort_change_and_ignores_window:
  3 backends; sort-independent minima, whole-tree API and cube-only graph.
* test_global_partial_has_zero_count_without_phantom_members: 3 backends x
  empty/nonempty input x true global/root1-normalized provider; count-star,
  nullable count, sum, no phantom member and rejection of a rowless plan.

These are physical-expression checks, not persistent cache lifecycle or
latency benchmarks. The companion strategy-equivalence module directly
checks materialized cubes against both the Python oracle and union_all.
"""
from dataclasses import replace

import ibis
import ibis.expr.operations as ops
import pyarrow as pa
import pytest
from ibis.common.graph import Graph
from pivot_engine.core.ir import MeasureFilter, MeasureRef, PivotQuery, SortKey, Window
from pivot_engine.core.logical import plan as logical_plan
from pivot_engine.core.physical.fetch import to_pyarrow  # the core's one result boundary
from pivot_engine.core.physical import leaf_partials, union_all

from .test_core_semantics import _assert_equal, _oracle
from .test_cross_backend import _connect


def _rows(expr, query, backend):
    return [(tuple(r[k] for k in query.rows[:r["__depth"]]),
             {k: r[k] for k in query.value_columns()})
            for r in to_pyarrow(expr).to_pylist()]


@pytest.mark.parametrize("backend", ["duckdb", "sqlite", "polars"])
@pytest.mark.parametrize("totals", ["visible", "all_data"])
@pytest.mark.parametrize("filtered", [False, True])
def test_provider_uses_e0_and_restricted_child_cubes(backend, totals, filtered):
    records = [{"r": r, "c": c, "p": p, "v": v, "k": k}
               for r, c, p, v, k in [
                   ("A", "x", "a", 8, 4), ("A", "y", "b", -5, 1),
                   ("B", "x", "b", 2, 3), (None, "x", "a", 9, None),
               ]]
    con = _connect(backend, pa.Table.from_pylist(records))
    q = PivotQuery(dataset="t", rows=("r", "c"), columns=("p",), column_members=(("a",),),
                   measures=(MeasureRef("m", "sum", "v"), MeasureRef("a", "avg", "v"),
                             MeasureRef("n", "count", None)),
                   expanded=frozenset({("A",), (None,)}), totals=totals,
                   measure_filters=(MeasureFilter(2, "m", ">", 0), MeasureFilter(1, "m", ">", 0)),
                   sort=(SortKey("m", desc=True, member=("b",)), SortKey("r", by="k")))
    if not filtered:
        q = replace(q, measure_filters=())
    plan = logical_plan(q)
    calls = []

    def provider(p, e0, keys, restriction):
        calls.append((tuple(keys), restriction))
        if filtered:
            assert set(keys) >= {"r", "c", "p"}
        # Materialized memtables deliberately sever all raw fact dependencies.
        part = leaf_partials.partials(union_all.restrict(e0, restriction, e0.schema()), p, keys)
        return ibis.memtable(to_pyarrow(part))

    expr = leaf_partials.lower(plan, con.table("t"), partial_provider=provider)
    assert not any(isinstance(node, ops.DatabaseTable) for node in Graph.from_bfs(expr.op()))
    _assert_equal(_rows(expr, q, con), _oracle(records, q))
    assert any(restriction is not None for _, restriction in calls)
    assert any(restriction is None for _, restriction in calls)
    assert len(calls) == len(set(calls))


def test_grain_reaches_filters_below_collapsed_nodes_and_member_sorts():
    q = PivotQuery(dataset="t", rows=("r", "c", "d"), columns=("p",),
                   measures=(MeasureRef("m", "sum", "v"),), expanded=frozenset(),
                   measure_filters=(MeasureFilter(3, "m", ">", 0),))
    assert leaf_partials.grain(logical_plan(q), ("r",)) == ["r", "c", "d", "p"]
    member = replace(q, measure_filters=(), sort=(SortKey("d", by="k"),))
    assert leaf_partials.grain(logical_plan(member)) == ["r", "c", "d", "p"]


@pytest.mark.parametrize("backend", ["duckdb", "sqlite", "polars"])
def test_cube_carries_its_member_key_minima_and_ignores_window(backend):
    """A member-key sort (REQ-SORT-003) is served from the cube alone: it carries that key's partial MIN,
    and only the minima its sorts name (every other column's MIN would double the build)."""
    records = [{"r": "A", "v": 3, "k": 8}, {"r": "B", "v": 2, "k": 1}]
    con = _connect(backend, pa.Table.from_pylist(records))
    q = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("m", "sum", "v"),))
    sorted_q = replace(q, sort=(SortKey("r", by="k"),), window=Window(1, 1))
    cube = leaf_partials.partials(con.table("t"), logical_plan(sorted_q))
    schema = con.table("t").schema()
    assert set(cube.columns) - {"r"} == {"__s0", *leaf_partials.scalar_minima(schema, ["k"]).values()}
    assert tuple(leaf_partials.partials(con.table("t"), logical_plan(q)).columns) == ("r", "__s0")  # unsorted: no minima
    con.create_table("cube", to_pyarrow(cube))
    expr = leaf_partials.from_partials(logical_plan(sorted_q), con.table("t"), con.table("cube"))
    assert {node.name for node in Graph.from_bfs(expr.op()) if isinstance(node, ops.DatabaseTable)} == {"cube"}
    _assert_equal(_rows(expr, sorted_q, con), _oracle(records, replace(sorted_q, window=Window())))

@pytest.mark.parametrize("backend", ["duckdb", "sqlite", "polars"])
@pytest.mark.parametrize("normalize_root", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_global_partial_has_zero_count_without_phantom_members(backend, normalize_root, empty):
    records = [] if empty else [{"r": "A", "v": None}, {"r": None, "v": 3}]
    table = pa.table({"r": pa.array([r["r"] for r in records], pa.string()),
                      "v": pa.array([r["v"] for r in records], pa.int64())})
    con = _connect(backend, table)
    q = PivotQuery(dataset="t", rows=("r",), measures=(
        MeasureRef("n", "count", None), MeasureRef("nf", "count", "v"), MeasureRef("s", "sum", "v")))
    plan = logical_plan(q)
    raw = con.table("t")
    global_rows = to_pyarrow(leaf_partials.partials(raw, plan, keys=())).to_pylist()
    assert len(global_rows) == 1
    assert global_rows[0]["__c0"] == len(records)
    assert global_rows[0]["__c1"] == (0 if empty else 1)
    if empty:
        assert global_rows[0]["__s2"] is None
        assert to_pyarrow(leaf_partials.partials(raw, plan, keys=("r",))).num_rows == 0

    def provider(p, e0, keys, restriction):
        requested = keys or (("r",) if normalize_root else ())
        return ibis.memtable(to_pyarrow(leaf_partials.partials(e0, p, keys=requested)))

    result = _rows(leaf_partials.lower(plan, raw, partial_provider=provider), q, con)
    _assert_equal(result, _oracle(records, q))
    if empty:
        assert result == [((), {"n": 0, "nf": 0, "s": None})]
    assert not leaf_partials.supports(logical_plan(replace(q, rows=())), raw.schema())
