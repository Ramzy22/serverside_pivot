"""Unit tests for core.authority.scope (REQ-SEC-001..004)."""

from __future__ import annotations

import concurrent.futures

import ibis
import pyarrow as pa
import pytest

from pivot_engine.backends.table_catalog import IbisTableCatalog
from pivot_engine.core.authority.scope import (
    PathNotProvenError,
    ScopeColumnError,
    UnscopedAccessError,
    require_proven_path,
    scoped_execution,
    submit_with_scope,
)
from pivot_engine.runtime.dataset_authorization import RowScopeGrant


def _con():
    con = ibis.duckdb.connect()
    con.create_table(
        "t",
        pa.table({"tenant": ["A", "a", "B", None], "amount": [1, 10, 100, 1000], "n": [1, 2, 3, 4]}),
    )
    return con


def _grant(**fields):
    return RowScopeGrant(
        table="t",
        subject="u",
        field_values={k: tuple(v) for k, v in fields.items()},
        scope_fingerprint=repr(sorted(fields.items())),
    )


def _sum(expr):
    return int(expr.amount.sum().execute())


def test_scope_is_exact_and_case_sensitive():  # REQ-SEC-003
    con = _con()
    with scoped_execution(_grant(tenant=["A"]), connections=[con]):
        assert _sum(con.table("t")) == 1


def test_scope_with_null_value_matches_only_null():
    con = _con()
    with scoped_execution(_grant(tenant=[None]), connections=[con]):
        assert _sum(con.table("t")) == 1000


def test_integer_scope_is_typed_exactly():
    con = _con()
    grant = RowScopeGrant(table="t", subject="u", field_values={"n": ("2",)}, scope_fingerprint="n2")
    with scoped_execution(grant, connections=[con]):
        assert _sum(con.table("t")) == 10


def test_missing_scope_column_denies():  # REQ-SEC-002
    con = _con()
    with scoped_execution(_grant(org=["A"]), connections=[con]):
        with pytest.raises(ScopeColumnError):
            con.table("t")


def test_protected_table_without_grant_fails_closed():  # REQ-SEC-001
    con = _con()
    with scoped_execution(_grant(tenant=["A"]), connections=[con]):
        pass
    with pytest.raises(UnscopedAccessError):
        con.table("t")


def test_thread_hop_without_context_fails_closed_and_with_scope_propagates():
    con = _con()
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        with scoped_execution(_grant(tenant=["A"]), connections=[con]):
            lost = pool.submit(lambda: con.table("t"))
            with pytest.raises(UnscopedAccessError):
                lost.result()
            kept = submit_with_scope(pool, lambda: _sum(con.table("t")))
            assert kept.result() == 1


def test_catalog_cache_never_carries_one_grant_to_another():  # REQ-SEC-004
    con = _con()
    catalog = IbisTableCatalog(con, schema_cache_ttl=300)
    with scoped_execution(_grant(tenant=["A"]), connections=[con]):
        assert _sum(catalog.resolve_table("t")) == 1
    with scoped_execution(_grant(tenant=["B"]), connections=[con]):
        assert _sum(catalog.resolve_table("t")) == 100


def test_nested_scope_cannot_replace_active_grant():
    con = _con()
    with scoped_execution(_grant(tenant=["A"]), connections=[con]):
        with pytest.raises(UnscopedAccessError):
            with scoped_execution(_grant(tenant=["B"]), connections=[con]):
                pass


def test_raw_sql_over_protected_table_is_denied():
    con = _con()
    with scoped_execution(_grant(tenant=["A"]), connections=[con]):
        with pytest.raises(UnscopedAccessError):
            con.sql("SELECT * FROM t")
        with pytest.raises(UnscopedAccessError):
            con.raw_sql("select sum(amount) from T")


def test_unprotected_dataset_is_unaffected():
    con = _con()
    assert _sum(con.table("t")) == 1111
    with scoped_execution(None, connections=[con]):
        assert _sum(con.table("t")) == 1111


def test_unproven_path_denied_only_under_protecting_scope():
    require_proven_path("pruned_hierarchical")  # no scope: allowed
    con = _con()
    with scoped_execution(_grant(tenant=["A"]), connections=[con]):
        with pytest.raises(PathNotProvenError):
            require_proven_path("pruned_hierarchical")


def test_unguarded_table_is_not_used_by_production_code():
    import pathlib

    import pivot_engine

    root = pathlib.Path(pivot_engine.__file__).parent
    offenders = [
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if p.name != "scope.py" and "unguarded_table" in p.read_text(encoding="utf-8", errors="ignore")
    ]
    assert offenders == []


def test_dash_requires_authorization_by_default(monkeypatch):  # REQ-SEC-006
    from pivot_engine.runtime.dash_callbacks import resolve_dash_authorization_requirement

    monkeypatch.delenv("PIVOT_DEV_UNAUTHENTICATED", raising=False)
    assert resolve_dash_authorization_requirement() is True
    assert resolve_dash_authorization_requirement(dev_unauthenticated=True) is False
    monkeypatch.setenv("PIVOT_DEV_UNAUTHENTICATED", "1")
    assert resolve_dash_authorization_requirement() is False
    monkeypatch.setenv("PIVOT_ENV", "production")
    monkeypatch.setenv("ENVIRONMENT", "production")
    from pivot_engine.runtime import dash_callbacks

    monkeypatch.setattr(dash_callbacks, "_is_production_environment", lambda: True)
    assert resolve_dash_authorization_requirement(dev_unauthenticated=True) is True


def test_snapshot_where_sql_is_exact_and_null_aware():
    from pivot_engine.diff.snapshot_provider import _build_rls_where_sql

    where, params = _build_rls_where_sql(_grant(tenant=["A", None]))
    assert where == ' WHERE ("tenant" IN (?) OR "tenant" IS NULL)'
    assert params == ["A"]
    con = _con()
    raw = con.con
    assert raw.execute("SELECT sum(amount) FROM t" + where, params).fetchone()[0] == 1001
