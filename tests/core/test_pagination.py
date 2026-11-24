"""Pagination modes and binding cursors (REQ-PAGE-001..003, F-PAGE-DEEP)."""

from __future__ import annotations

from dataclasses import replace

import ibis
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pivot_engine.backends.table_catalog import IbisTableCatalog
from pivot_engine.core.authority.scope import scoped_execution
from pivot_engine.core.cache import CacheManager
from pivot_engine.core.exec import canonical, execute, fingerprint
from pivot_engine.core.ir import MeasureRef, PivotQuery, SortKey, Window
from pivot_engine.core.pagination import CursorRejectedError, MaterializationTooLargeError, Pager, _drop, _held_rows
from pivot_engine.runtime.dataset_authorization import RowScopeGrant

from .test_core_semantics import _con, _table, examples, queries

RECORDS = [
    {"r": r, "c": c, "v": v}
    for r, c, v in [("A", "x", 1), ("A", "y", 2), ("B", "x", 3), (None, "z", 4), ("C", None, 5), ("B", "w", 6)]
]
Q = PivotQuery(dataset="t", rows=("r", "c"), measures=(MeasureRef("s", "sum", "v"),), window=Window(0, 3))


def _paths(frame):
    return [r["path"] for r in frame.records()]


def _walk(pager, con, query, **kw):
    pages, cursor = [], None
    for _ in range(100):
        page = pager.page(query, con.table, cursor=cursor, **kw)
        pages.append(page)
        cursor = page.next_cursor
        if cursor is None:
            return pages
    raise AssertionError("pagination did not terminate")


@pytest.mark.parametrize("mode", ["restart_on_change", "materialized"])
def test_pages_concatenate_to_the_unpaged_tree(mode):
    con = _con(_table(RECORDS))
    full = _paths(execute(replace(Q, window=Window()), con.table))
    pages = _walk(Pager(materialize_after=0), con, Q, mode=mode)
    assert [p for page in pages for p in _paths(page.frame)] == full
    assert [page.offset for page in pages] == list(range(0, len(full), 3))
    if mode == "materialized":
        assert {page.total_rows for page in pages} == {len(full)}
    else:
        assert {page.total_rows for page in pages} == {None}


@settings(max_examples=examples(30), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(records=st.lists(st.fixed_dictionaries({"r": st.sampled_from(["A", "a", None, "x|y"]),
                                               "c": st.sampled_from(["A", "", None]),
                                               "v": st.one_of(st.none(), st.integers(-5, 5))}), max_size=12),
       query=queries(), size=st.integers(1, 4))
def test_materialized_pages_equal_direct_execution(records, query, size):
    con = _con(_table(records))
    unpaged = replace(query, window=Window())
    expected = [(r["path"], r["values"]) for r in execute(unpaged, con.table).records()]
    pager = Pager(materialize_after=1)  # first page direct, then held: both must agree (paths and values)
    got = [(r["path"], r["values"])
           for page in _walk(pager, con, replace(unpaged, window=Window(0, size)), mode="materialized")
           for r in page.frame.records()]
    assert got == expected


def test_materialization_is_reused_for_the_same_revision_and_rebuilt_for_a_new_one():
    con = _con(_table(RECORDS))
    pager = Pager(materialize_after=0)
    first = pager.page(Q, con.table, mode="materialized", revision="1")
    second = pager.page(Q, con.table, mode="materialized", revision="1")
    third = pager.page(Q, con.table, mode="materialized", revision="2")
    assert first.frame.fingerprint == second.frame.fingerprint != third.frame.fingerprint
    assert pager.total_rows(Q, con.table, revision="1") == first.total_rows


def test_restart_on_change_rejects_a_moved_revision():
    con = _con(_table(RECORDS))
    pager = Pager()
    cursor = pager.page(Q, con.table, revision="1").next_cursor
    pager.page(Q, con.table, revision="1", cursor=cursor)
    with pytest.raises(CursorRejectedError, match=r"^Cursor rejected: revision changed\.$"):
        pager.page(Q, con.table, revision="2", cursor=cursor)


def test_materialized_cursor_survives_a_revision_move_but_not_eviction():
    con = _con(_table(RECORDS))
    pager = Pager(CacheManager(budgets={"snapshot": 10_000}, sizeof=_held_rows, on_evict=_drop),
                  materialize_after=0)
    first = pager.page(Q, con.table, mode="materialized", revision="1")
    table = first.frame.fingerprint.split(":", 1)[1]
    assert table in con.list_tables()
    nxt = pager.page(Q, con.table, mode="materialized", revision="2", cursor=first.next_cursor)
    assert nxt.offset == 3
    pager.cache.invalidate(dataset="t")
    assert table not in con.list_tables()  # eviction drops the server-held result
    with pytest.raises(CursorRejectedError, match="materialized result expired"):
        pager.page(Q, con.table, mode="materialized", revision="2", cursor=first.next_cursor)


@pytest.mark.parametrize("change, reason", [
    (dict(sort=(SortKey("r", True),)), "fingerprint changed"),
    (dict(expanded=frozenset({("A",)})), "fingerprint changed"),
    (dict(measures=(MeasureRef("s", "max", "v"),)), "fingerprint changed"),
    (dict(totals="all_data"), "fingerprint changed"),
])
def test_cursor_for_a_different_query_is_rejected(change, reason):
    con = _con(_table(RECORDS))
    pager = Pager()
    cursor = pager.page(Q, con.table).next_cursor
    with pytest.raises(CursorRejectedError, match=reason):
        pager.page(replace(Q, **change), con.table, cursor=cursor)
    pager.page(replace(Q, window=Window(99, 3)), con.table, cursor=cursor)  # the window itself is not bound


def test_cursor_mode_and_epoch_and_signature_are_bound():
    con = _con(_table(RECORDS))
    pager = Pager()
    cursor = pager.page(Q, con.table).next_cursor
    with pytest.raises(CursorRejectedError, match="mode changed"):
        pager.page(Q, con.table, mode="materialized", cursor=cursor)
    with pytest.raises(CursorRejectedError, match="signature mismatch"):
        Pager().page(Q, con.table, cursor=cursor)  # another server epoch / secret
    payload, sig = cursor.split(".")
    with pytest.raises(CursorRejectedError, match="signature mismatch"):
        pager.page(Q, con.table, cursor=payload[:-2] + "AA." + sig)
    for junk in ("", "abc", "a.b.c", "!!!.???"):
        with pytest.raises(CursorRejectedError):
            pager.page(Q, con.table, cursor=junk)
    same_secret = Pager(epoch="other", secret=pager._secret)
    with pytest.raises(CursorRejectedError, match="epoch changed"):
        same_secret.page(Q, con.table, cursor=cursor)


def test_snapshot_mode_binds_the_snapshot_not_the_revision():
    con = _con(_table(RECORDS))
    pager = Pager()
    with pytest.raises(ValueError, match="requires a snapshot id"):
        pager.page(Q, con.table, mode="snapshot")
    cursor = pager.page(Q, con.table, mode="snapshot", snapshot_id="s1", revision="1").next_cursor
    pager.page(Q, con.table, mode="snapshot", snapshot_id="s1", revision="9", cursor=cursor)
    with pytest.raises(CursorRejectedError, match="snapshot_id changed"):
        pager.page(Q, con.table, mode="snapshot", snapshot_id="s2", cursor=cursor)


def test_unknown_mode_and_missing_limit():
    con = _con(_table(RECORDS))
    with pytest.raises(ValueError, match="unknown pagination mode"):
        Pager().page(Q, con.table, mode="eventual")
    with pytest.raises(ValueError, match="needs a window limit"):
        Pager().page(replace(Q, window=Window()), con.table)


def _tenant_con():
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({"tenant": ["A", "A", "B"], "r": ["x", "y", "z"], "v": [1, 2, 3]}))
    return con


def _grant(value):
    return RowScopeGrant(table="t", subject=value, field_values={"tenant": (value,)}, scope_fingerprint=value)


def test_materialized_results_and_cursors_are_per_tenant():
    con = _tenant_con()
    catalog = IbisTableCatalog(con)
    q = PivotQuery(dataset="t", rows=("r",), measures=(MeasureRef("s", "sum", "v"),), window=Window(0, 2))
    pager = Pager(materialize_after=0)
    with scoped_execution(_grant("A"), connections=[con]):
        a = pager.page(q, catalog.resolve_table, mode="materialized")
        assert _paths(a.frame) == [(), ("x",)] and a.total_rows == 3
    with scoped_execution(_grant("B"), connections=[con]):
        b = pager.page(q, catalog.resolve_table, mode="materialized")
        assert _paths(b.frame) == [(), ("z",)] and b.total_rows == 2
        assert b.frame.fingerprint != a.frame.fingerprint
        with pytest.raises(CursorRejectedError, match="scope changed"):
            pager.page(q, catalog.resolve_table, mode="materialized", cursor=a.next_cursor)


def test_canonical_fingerprint_is_order_independent_and_meaning_sensitive():
    paths = [("b",), ("a",), (None,), ("A",)]
    q1 = replace(Q, expanded=frozenset(paths))
    q2 = replace(Q, expanded=frozenset(reversed(paths)))
    assert canonical(q1) == canonical(q2) and fingerprint(q1) == fingerprint(q2)
    variants = [replace(Q, rows=("c", "r")), replace(Q, totals="all_data"), replace(Q, window=Window(1, 3)),
                replace(Q, expanded=frozenset({("1",)})), replace(Q, expanded=frozenset({(1,)}))]
    assert len({fingerprint(v) for v in variants + [Q]}) == len(variants) + 1
    unscoped = fingerprint(Q)
    with scoped_execution(_grant("A")):
        assert fingerprint(Q) != unscoped  # the grant is part of plan identity


def test_tree_too_large_to_hold_is_a_typed_error_and_leaves_nothing_behind():
    con = _con(_table(RECORDS))
    before = set(con.list_tables())
    with pytest.raises(MaterializationTooLargeError, match="use restart_on_change"):
        Pager(max_held_rows=3, materialize_after=0).page(Q, con.table, mode="materialized")
    assert set(con.list_tables()) == before


def test_held_trees_are_evicted_by_row_budget():
    con = _con(_table(RECORDS))
    pager = Pager(max_held_rows=12, materialize_after=0)  # the tree has 11 rows: one fits, two do not
    a = pager.page(Q, con.table, mode="materialized", revision="1")
    pager.page(Q, con.table, mode="materialized", revision="2")
    assert a.frame.fingerprint.split(":", 1)[1] not in con.list_tables()
    with pytest.raises(CursorRejectedError, match="expired"):
        pager.page(Q, con.table, mode="materialized", revision="1", cursor=a.next_cursor)


def test_materialized_mode_serves_direct_pages_until_a_scroll_is_under_way():
    con = _con(_table(RECORDS))
    pager = Pager(materialize_after=2, materialize_offset=6)
    p0 = pager.page(Q, con.table, mode="materialized", revision="1")
    p1 = pager.page(Q, con.table, mode="materialized", revision="1", cursor=p0.next_cursor)
    assert p0.total_rows is None and p1.total_rows is None  # direct
    with pytest.raises(CursorRejectedError, match="revision changed"):  # direct pages bind the revision
        pager.page(Q, con.table, mode="materialized", revision="2", cursor=p1.next_cursor)
    p2 = pager.page(Q, con.table, mode="materialized", revision="1", cursor=p1.next_cursor)
    assert p2.total_rows == 11  # third page: built and held
    p3 = pager.page(Q, con.table, mode="materialized", revision="2", cursor=p2.next_cursor)
    assert p3.offset == 9  # held cursors survive revision moves
    jump = Pager(materialize_after=5, materialize_offset=6).page(
        replace(Q, window=Window(6, 3)), con.table, mode="materialized")
    assert jump.total_rows == 11  # a deep jump materializes at once


def test_one_materialization_never_blocks_other_trees_and_is_built_once():
    import threading

    con = _con(_table(RECORDS))
    pager = Pager(materialize_after=0)
    other = replace(Q, rows=("c",))
    real_build, real_slice = pager._build, pager._slice
    release, started, builds = threading.Event(), threading.Event(), []
    db = threading.Lock()  # one DuckDB connection is not safe across threads: serialize the statements only

    def slow_build(query, resolve, strategy, mat_id):
        builds.append(query.rows)
        if query.rows == Q.rows:
            started.set()
            assert release.wait(30)  # blocked holding no lock of the Pager's (nor the database)
        with db:
            return real_build(query, resolve, strategy, mat_id)

    def serialized_slice(*args):
        with db:
            return real_slice(*args)

    pager._build, pager._slice = slow_build, serialized_slice
    results = {}

    def page_q(name):
        try:
            results[name] = pager.page(Q, con.table, mode="materialized")
        except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
            results[name] = exc

    first = threading.Thread(target=page_q, args=("first",))
    first.start()
    assert started.wait(30)
    second = threading.Thread(target=page_q, args=("second",))  # same tree: waits for that one build
    second.start()
    assert pager.page(other, con.table, mode="materialized").frame.table.num_rows > 0  # not blocked by Q's build
    release.set()
    first.join(30)
    second.join(30)
    assert not [r for r in results.values() if isinstance(r, BaseException)], results
    assert builds.count(Q.rows) == 1
    assert results["first"].frame.fingerprint == results["second"].frame.fingerprint
