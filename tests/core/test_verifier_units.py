"""Plan verifier unit behaviour (REQ-SEC-001 defence in depth)."""

from __future__ import annotations

import ibis
import pyarrow as pa
import pytest

from pivot_engine.core.authority import verifier as V
from pivot_engine.core.authority.scope import compile_scope_predicate, scoped_execution, unguarded_table
from pivot_engine.runtime.dataset_authorization import RowScopeGrant

A = RowScopeGrant(table="t", subject="u", field_values={"tenant": ("A",)}, scope_fingerprint="A")


def _con():
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"tenant": ["A", "B"], "v": [1, 2]}))
    con.create_table("other", pa.table({"tenant": ["A"], "w": [5]}))
    return con


@pytest.mark.parametrize("name, bare", [('"Main"."T"', "t"), ("db.t", "t"), ("T", "t"), ('"t"', "t")])
def test_bare(name, bare):
    assert V._bare(name) == bare


def test_nothing_to_verify_without_protection():
    con = _con()
    V.verify_scoped(unguarded_table(con, "t"))  # no grant, nothing declared protected


def test_declared_protected_without_a_grant_is_rejected():
    con = _con()
    with pytest.raises(V.PlanVerificationError, match=r"^Plan verification failed: t: protected but no grant is active$"):
        V.verify_scoped(unguarded_table(con, "t"), protected=['"main"."T"'])
    V.verify_scoped(unguarded_table(con, "other"), protected=["t"])  # other tables are not concerned


def test_raw_read_under_a_grant_is_rejected_with_reason():
    con = _con()
    with scoped_execution(A):
        with pytest.raises(V.PlanVerificationError, match=r"^Plan verification failed: t: read without its scope filter$"):
            V.verify_scoped(unguarded_table(con, "t").select("v"))


def test_scope_filter_with_extra_predicates_is_accepted():
    con = _con()
    with scoped_execution(A):
        raw = unguarded_table(con, "t")
        ok = raw.filter([compile_scope_predicate(raw, A), raw.v > 0])
        V.verify_scoped(ok.group_by("tenant").aggregate(s=ok.v.sum()))


def test_a_different_predicate_is_not_the_scope_filter():
    con = _con()
    with scoped_execution(A):
        raw = unguarded_table(con, "t")
        with pytest.raises(V.PlanVerificationError, match="read without its scope filter"):
            V.verify_scoped(raw.filter(raw.tenant == "B"))
        with pytest.raises(V.PlanVerificationError, match="read without its scope filter"):
            V.verify_scoped(raw.filter(raw.tenant.isin(["A", "B"])))


def test_scoped_plus_raw_reference_names_the_offending_operation():
    con = _con()
    with scoped_execution(A):
        raw = unguarded_table(con, "t")
        scoped = raw.filter(compile_scope_predicate(raw, A))
        leak = scoped.union(raw)
        with pytest.raises(V.PlanVerificationError, match=r"t: referenced by Union outside the scope filter"):
            V.verify_scoped(leak)


def test_unprotected_tables_in_the_same_plan_are_fine():
    con = _con()
    with scoped_execution(A, connections=[con]):
        t, o = con.table("t"), con.table("other")
        V.verify_scoped(t.join(o, "tenant").select(t.v, o.w))


def test_error_is_a_path_denial():
    from pivot_engine.core.authority.scope import PathNotProvenError

    assert issubclass(V.PlanVerificationError, PathNotProvenError)
