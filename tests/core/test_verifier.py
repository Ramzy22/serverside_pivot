"""Plan verifier (REQ-SEC-001 defence in depth)."""

from __future__ import annotations

import ibis
import pyarrow as pa
import pytest

from pivot_engine.backends.table_catalog import IbisTableCatalog
from pivot_engine.core.authority.scope import scoped_execution, unguarded_table
from pivot_engine.core.authority.verifier import PlanVerificationError, verify_scoped
from pivot_engine.core.exec import compile_query, execute
from pivot_engine.core.ir import MeasureFilter, MeasureRef, PivotQuery
from pivot_engine.runtime.dataset_authorization import RowScopeGrant

GRANT = RowScopeGrant(table="t", subject="u", field_values={"tenant": ("A",)}, scope_fingerprint="A")


def _con():
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"tenant": ["A", "a", "B"], "r": ["x", "x", "y"], "v": [1, 10, 100]}))
    return con


def test_core_query_under_scope_verifies_and_is_scoped():
    con = _con()
    catalog = IbisTableCatalog(con)
    q = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("v_sum", "sum", "v"),),
                   measure_filters=(MeasureFilter(1, "v_sum", ">", 0),))
    with scoped_execution(GRANT, connections=[con]):
        frame = execute(q, catalog.resolve_table)
    assert [(r["path"], r["values"]["v_sum"]) for r in frame.records()] == [((), 1), (("x",), 1)]


def test_union_with_the_raw_table_is_rejected():
    con = _con()
    with scoped_execution(GRANT, connections=[con]):
        scoped = con.table("t")
        bypass = scoped.union(unguarded_table(con, "t"))
        with pytest.raises(PlanVerificationError):
            verify_scoped(bypass)


def test_self_join_reaching_the_raw_table_is_rejected():
    con = _con()
    with scoped_execution(GRANT, connections=[con]):
        scoped = con.table("t")
        raw = unguarded_table(con, "t")
        joined = scoped.join(raw, scoped.r == raw.r).select(scoped.r, raw.v)
        with pytest.raises(PlanVerificationError):
            verify_scoped(joined)


def test_compile_rejects_a_resolver_that_returns_the_raw_table():
    con = _con()
    q = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("v_sum", "sum", "v"),))
    with scoped_execution(GRANT, connections=[con]):
        with pytest.raises(PlanVerificationError):
            compile_query(q, lambda name: unguarded_table(con, name))
