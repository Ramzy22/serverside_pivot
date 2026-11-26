"""M5 cross-backend certification (REQ-BACK-004): the core gives the oracle's answer on every Ibis backend it can reach.

Each backend either returns exactly the oracle's rows or refuses the plan with
a typed ``BackendCapabilityError`` before running it. A wrong number, a raw
driver error or a partial result fails. DuckDB (the reference) never refuses.
Backends that are not installed are skipped. Server backends (Postgres, Oracle,
cloud warehouses) are not exercised yet: see docs/gates/M5.md, carried over.
"""

from __future__ import annotations

import ibis
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings

from pivot_engine.core.exec import execute
from pivot_engine.core.ir import Expr, MeasureFilter, MeasureRef, PivotQuery, Window
from pivot_engine.core.physical import BackendCapabilityError

from .test_core_semantics import (
    _ARROW, RECORDS, VECTORS, _assert_equal, _oracle, _table, examples, queries,
)

BACKENDS = ["duckdb", "sqlite", "polars"]


def _connect(name: str, table: pa.Table):
    try:
        con = getattr(ibis, name).connect()
    except Exception as exc:  # noqa: BLE001 - backend extra not installed
        pytest.skip(f"ibis {name} backend unavailable: {exc}")
    con.create_table("t", table)
    return con


def _disconnect(con) -> None:
    disconnect = getattr(con, "disconnect", None)
    if callable(disconnect):
        try:
            disconnect()
        except Exception:  # noqa: BLE001 - best-effort test cleanup
            pass


def _run(name, table, query):
    con = _connect(name, table)
    try:
        return [(r["path"], r["values"]) for r in execute(query, con.table).records()]
    except BackendCapabilityError as exc:
        assert name != "duckdb", f"the reference backend refused: {exc}"
        assert exc.backend == name and exc.operations
        return None
    finally:
        _disconnect(con)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("vector", VECTORS, ids=[v["id"] for v in VECTORS])
def test_every_backend_conforms_to_the_vectors(backend, vector):  # REQ-AGG-001
    cols = vector["data"]["columns"]
    table = pa.table({k: pa.array(c["values"], _ARROW[c["type"]]) for k, c in cols.items()})
    q = vector["query"]
    query = PivotQuery(
        dataset="t", rows=tuple(q["rows"]),
        measures=tuple(MeasureRef(m["alias"], m["agg"], m["field"]) for m in q["measures"]),
        measure_filters=tuple(MeasureFilter(f["level"], f["measure"], f["op"], f["value"], f.get("cells", False))
                              for f in q["measureFilters"]),
        totals=q["totals"],
    )
    got = _run(backend, table, query)
    if got is not None:
        _assert_equal(got, [(tuple(r["path"]), r["values"]) for r in vector["expected"]["rows"]])


@pytest.mark.parametrize("backend", BACKENDS)
def test_every_backend_equals_the_oracle(backend):
    @settings(max_examples=examples(60), deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(records=RECORDS, query=queries())
    def check(records, query):
        got = _run(backend, _table(records), query)
        if got is not None:
            _assert_equal(got, _oracle(records, query))

    check()


@pytest.mark.parametrize("backend", BACKENDS)
def test_count_distinct_ignores_nulls_on_every_backend(backend):  # REQ-AGG-003
    table = pa.table({"r": ["A", "A", "A", "B"], "v": pa.array([1, None, 1, None], pa.int64())})
    query = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("d", "count_distinct", "v"),))
    got = dict(_run(backend, table, query))
    assert {p: v["d"] for p, v in got.items()} == {(): 1, ("A",): 1, ("B",): 0}


def test_a_missing_aggregation_is_a_typed_refusal_before_execution():
    con = _connect("sqlite", pa.table({"r": ["A"], "v": pa.array([1], pa.int64())}))
    try:
        query = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("m", "median", "v"),))
        with pytest.raises(BackendCapabilityError) as info:
            execute(query, con.table)
        assert info.value.code == "unsupported_query" and "Median" in info.value.operations
    finally:
        _disconnect(con)


@pytest.mark.parametrize("backend, measure, gap", [
    ("polars", MeasureRef("f", "first", "v", order_by="k"), "ordered aggregate"),
    ("sqlite", MeasureRef("f", "variance", "v"), "variance"),  # finalize raises on a one-value group
    ("sqlite", MeasureRef("f", "stddev", "v"), "stddev"),
])
def test_a_known_execution_gap_is_refused_before_it_runs(backend, measure, gap):
    table = pa.table({"r": ["A", "A", "B"], "k": ["x", "y", "z"], "v": pa.array([None, 2, 5], pa.int64())})
    con = _connect(backend, table)
    try:
        query = PivotQuery(dataset="t", rows=("r",), measures=(measure,))
        with pytest.raises(BackendCapabilityError) as info:
            execute(query, con.table)
        assert gap in info.value.operations[0]
    finally:
        _disconnect(con)


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_formula_over_constants_is_folded_on_every_backend(backend):  # REQ-AGG-010
    """max(min(0, 0), m) once panicked Polars 1.3 inside an aggregation; constants are now folded."""
    def num(x):
        return Expr("num", (x,))

    table = pa.table({"r": ["A", "A", "B"], "v": pa.array([-3, None, 5], pa.int64())})
    formula = Expr("max", (Expr("min", (num(0), num(0))), Expr("ref", ("m",))))
    over_null = Expr("+", (Expr("null", ()), num(1)))
    by_zero = Expr("/", (num(1), Expr("-", (num(2), num(2)))))
    query = PivotQuery(dataset="t", rows=("r",), measures=(
        MeasureRef("m", "max", "v"),
        MeasureRef("f", "formula", None, expr=formula),
        MeasureRef("n", "formula", None, expr=over_null),
        MeasureRef("z", "formula", None, expr=by_zero),
        MeasureRef("p", "formula", None, expr=Expr("pow", (num(-8), 0.5))),
    ))
    rows = _run(backend, table, query)
    if rows is None:  # SQLite has no isnan/isinf, so every formula is a typed refusal there
        assert backend == "sqlite"
        return
    got = {path: (v["f"], v["n"], v["z"], v["p"]) for path, v in rows}
    assert got == {(): (5.0, None, None, None), ("A",): (0.0, None, None, None), ("B",): (5.0, None, None, None)}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("window", [Window(0, 2), Window(2, 3), Window(1, None), Window(9, 4)])
def test_a_window_reads_the_whole_trees_total_and_grand_total(backend, window):
    """One statement where the backend has window functions, else page + count + grand total: same frame."""
    table = pa.table({"r": ["A", "A", "B", "C"], "c": ["x", "y", "x", "z"], "v": pa.array([1, 2, 4, 8], pa.int64())})
    query = PivotQuery(dataset="t", rows=("r", "c"), measures=(MeasureRef("s", "sum", "v"),), window=window)
    con = _connect(backend, table)
    try:
        whole = execute(PivotQuery(dataset="t", rows=("r", "c"), measures=(MeasureRef("s", "sum", "v"),)), con.table)
        frame = execute(query, con.table, grand_total=True)
        end = None if window.limit is None else window.offset + window.limit
        assert [r["path"] for r in frame.records()] == [r["path"] for r in whole.records()][window.offset:end]
        assert frame.total == whole.table.num_rows == 8
        assert frame.grand_total_record()["values"] == {"s": 15}
    finally:
        _disconnect(con)


def test_a_percentile_cell_on_polars_is_a_typed_refusal():
    """Polars evaluates a percentile cell's condition at the measure axis's length: refused, never wrong."""
    from pivot_engine.core.ir import Unpivot, UnpivotMember

    table = pa.table({"r": ["A", "A", "B"], "k": ["x", "y", "x"], "v": pa.array([1, None, 5], pa.int64()),
                      "w": pa.array([2, 3, None], pa.int64())})
    axis = Unpivot("mn", "mv", "mk", (UnpivotMember("V", "v", 0), UnpivotMember("W", "w", 1)), False)
    query = PivotQuery(dataset="t", rows=("mn", "r"), measures=(MeasureRef("p", "percentile", "mv", q=0.0),),
                       unpivot=axis, columns=("k",), column_members=(("x",),))
    con = _connect("polars", table)
    try:
        with pytest.raises(BackendCapabilityError) as info:
            execute(query, con.table)
        assert "percentile pivot cell over the measure axis" in info.value.operations
    finally:
        _disconnect(con)
