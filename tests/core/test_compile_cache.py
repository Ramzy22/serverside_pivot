"""Plan tier of the core cache: compiled trees and SQL are reused only when exact (REQ-SEC-004)."""

from __future__ import annotations

import ibis
import pyarrow as pa
import pytest
from ibis.common.graph import Graph

from pivot_engine.core.authority.scope import scoped_execution
from pivot_engine.core.exec import clear_compile_cache, compile_query, compiled_sql, execute
from pivot_engine.core.ir import IRValidationError, MeasureRef, PivotQuery, Window
from pivot_engine.runtime.dataset_authorization import RowScopeGrant


def _grant(tenant, fp=None):
    return RowScopeGrant(table="t", subject="u", field_values={"tenant": (tenant,)}, scope_fingerprint=fp or tenant)


def _con():
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"tenant": ["A", "A", "B"], "r": ["x", "y", "x"], "v": [1, 2, 40]}))
    return con


def _query(window=Window()):
    return PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("s", "sum", "v"),), window=window)


def _values(frame):
    return {r["path"]: r["values"]["s"] for r in frame.records()}


@pytest.fixture(autouse=True)
def _fresh():
    clear_compile_cache()
    yield
    clear_compile_cache()


def test_a_repeated_query_reuses_its_compiled_tree():
    con = _con()
    assert compile_query(_query(), con.table) is compile_query(_query(), con.table)


def test_every_page_is_cut_from_the_one_tree_and_equals_a_fresh_compile():
    con = _con()
    whole = compile_query(_query(), con.table)
    for offset, limit in [(0, 1), (1, 1), (1, None), (0, 2)]:
        page = compile_query(_query(Window(offset, limit)), con.table)
        assert whole.op().parent in Graph.from_bfs(page.op())  # the page is cut from the cached tree
        clear_compile_cache()
        fresh = compile_query(_query(Window(offset, limit)), con.table)
        assert page.to_pyarrow().equals(fresh.to_pyarrow())


def test_tenants_never_share_a_tree_even_when_their_fingerprints_collide():
    con = _con()
    with scoped_execution(_grant("A", fp="same"), connections=[con]):
        assert _values(execute(_query(), con.table)) == {(): 3, ("x",): 1, ("y",): 2}
    with scoped_execution(_grant("B", fp="same"), connections=[con]):  # same key, other scope filter
        assert _values(execute(_query(), con.table)) == {(): 40, ("x",): 40}


def test_a_changed_schema_recompiles():
    con = _con()
    before = compile_query(_query(), con.table)
    con.create_table("t", pa.table({"tenant": ["A"], "r": ["z"], "v": [7], "extra": [1.0]}), overwrite=True)
    after = compile_query(_query(), con.table)
    assert after is not before
    assert _values(execute(_query(), con.table)) == {(): 7, ("z",): 7}


def test_validation_still_runs_on_a_hit():
    con = _con()
    compile_query(_query(), con.table)
    with pytest.raises(IRValidationError):
        compile_query(_query(Window(-1, 1)), con.table)


def test_cached_sql_equals_the_backends_own_and_differs_per_page():
    con = _con()
    first = compile_query(_query(Window(0, 1)), con.table)
    second = compile_query(_query(Window(1, 1)), con.table)
    assert compiled_sql(con, first) == con.compile(first)
    assert compiled_sql(con, second) == con.compile(second) != compiled_sql(con, first)
    assert compiled_sql(con, first) is compiled_sql(con, first)


def test_connections_that_ibis_calls_equal_never_share_a_tree():
    """Ibis compares in-memory DuckDB connections by parameters: equal, yet holding different data."""
    first, second = _con(), ibis.duckdb.connect()
    second.create_table("t", pa.table({"tenant": ["C"], "r": ["w"], "v": [9]}))
    assert first.table("t").op() == second.table("t").op()
    assert _values(execute(_query(), first.table)) == {(): 43, ("x",): 41, ("y",): 2}
    assert _values(execute(_query(), second.table)) == {(): 9, ("w",): 9}


def test_a_measure_axis_formula_member_is_part_of_the_plan_identity():
    from pivot_engine.core.exec import fingerprint
    from pivot_engine.core.ir import Expr, Unpivot, UnpivotMember

    def query(op):
        formula = Expr(op, (Expr("ref", ("a",)), Expr("ref", ("b",))))
        axis = Unpivot("mn", "mv", "mk", (UnpivotMember("V", "v", 0, "sum"),
                                         UnpivotMember("F", None, 1, "formula", expr=formula)))
        return PivotQuery(dataset="t", rows=("mn",), unpivot=axis, measures=(
            MeasureRef("m", "member", "mv"), MeasureRef("a", "sum", "v"), MeasureRef("b", "count", "v")))

    assert fingerprint(query("-")) != fingerprint(query("/"))
    con = _con()
    minus, ratio = (execute(query(op), con.table).records() for op in ("-", "/"))
    member = {tuple(r["path"]): r["values"]["m"] for r in minus}, {tuple(r["path"]): r["values"]["m"] for r in ratio}
    assert member[0][("F",)] == 43 - 3 and member[1][("F",)] == 43 / 3
