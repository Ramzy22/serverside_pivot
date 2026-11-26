"""The controller's cube tier serves interactions without reading the facts."""

from dataclasses import replace

import ibis.expr.operations as ops
import pyarrow as pa
import pytest

import pivot_engine.core.exec as engine
from pivot_engine.core.authority.scope import scoped_execution
from pivot_engine.core.ir import MeasureRef, PivotQuery, SortKey, Window
from pivot_engine.runtime.dataset_authorization import RowScopeGrant
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.transport.core_bridge import core_locked_call


@pytest.fixture
def controller():
    c = ScalablePivotController(backend_uri=":memory:")
    c.load_data_from_arrow("facts", pa.table({"r": ["A", "A", "B", "B", None],
        "c": ["x", "y", "x", "z", "z"], "p": ["P1", "P2", "P1", "P2", "P1"], "v": [1, 2, 4, 8, 16]}))
    yield c
    c.close()


def query():
    return PivotQuery(dataset="facts", rows=("r", "c"), columns=("p",), column_members=(("P1",), ("P2",)),
                      measures=(MeasureRef("s", "sum", "v"),), expanded=frozenset(), window=Window(0, 2))


def read(c, q):
    return core_locked_call(c, "facts", lambda: engine.execute(q, c.resolve_table, strategy="leaf_partials"))


def trace(monkeypatch, controller):
    scanned = []
    real = engine.materialize

    def materialize(expr):
        if any(t.name == "facts" for t in expr.op().find(ops.DatabaseTable)):
            scanned.append(str(controller.con.compile(expr)))
        return real(expr)

    monkeypatch.setattr(engine, "materialize", materialize)
    return scanned


def test_scroll_sort_and_column_window_read_only_the_cube(controller, monkeypatch):
    scanned = trace(monkeypatch, controller)
    q = query()
    read(controller, q)
    assert len(scanned) == 1
    for changed in [replace(q, window=Window(1, 2)), replace(q, column_members=(("P2",),)),
                    replace(q, sort=(SortKey("s", member=("P2",), desc=True),))]:
        got = read(controller, changed)
        expected = engine.execute(changed, controller.resolve_table, strategy="union_all")
        assert got.records() == expected.records()
        scanned.pop()  # the independent reference intentionally reads the facts
    assert len(scanned) == 1
    assert controller.core_cache.used_bytes("cube") > 0


def test_expand_reads_only_new_parents_then_reexpand_reads_no_facts(controller, monkeypatch):
    scanned = trace(monkeypatch, controller)
    q = query()
    read(controller, q)
    read(controller, replace(q, expanded=frozenset({("A",)})))
    assert len(scanned) == 2
    assert "'A'" in scanned[-1] and "WHERE" in scanned[-1]
    read(controller, replace(q, expanded=frozenset({("A",), ("B",)})))
    assert len(scanned) == 3
    assert "'B'" in scanned[-1] and "'A'" not in scanned[-1]
    read(controller, q)
    read(controller, replace(q, expanded=frozenset({("A",)})))
    assert len(scanned) == 3


def test_load_and_mutation_invalidate_cached_partials(controller):
    q = query()
    read(controller, q)
    controller._clear_mutation_caches("facts")
    assert controller.core_cache.used_bytes("cube") == 0
    read(controller, q)
    controller.load_data_from_arrow("facts", pa.table({"r": ["A"], "c": ["x"], "p": ["P1"], "v": [99]}))
    assert controller.core_cache.used_bytes("cube") == 0
    assert read(controller, q).records()[0]["values"]["s"] == 99


def test_warm_cache_does_not_bypass_tenant_authority(controller):
    q = query()
    for tenant, value in [("A", 3), ("B", 12), ("A", 3)]:
        grant = RowScopeGrant(table="facts", subject=tenant, field_values={"r": (tenant,)}, scope_fingerprint=tenant)
        with scoped_execution(grant, connections=[controller.con]):
            result = read(controller, q)
            assert result.records()[0]["values"]["s"] == value
    with pytest.raises(Exception, match="grant|scope|protected"):
        read(controller, q)


def test_same_scope_fingerprint_cannot_alias_different_predicates(controller):
    for tenant, value in [("A", 3), ("B", 12)]:
        grant = RowScopeGrant(table="facts", subject=tenant, field_values={"r": (tenant,)},
                              scope_fingerprint="collision")
        with scoped_execution(grant, connections=[controller.con]):
            assert read(controller, query()).records()[0]["values"]["s"] == value


def test_scrolling_reuses_the_compiled_cube_program(controller, monkeypatch):
    from pivot_engine.core.physical import leaf_partials

    q = query()
    calls = []
    real = leaf_partials.lower

    def lower(*args, **kwargs):
        calls.append(True)
        return real(*args, **kwargs)

    monkeypatch.setattr(leaf_partials, "lower", lower)
    read(controller, q)
    read(controller, replace(q, window=Window(2, 2)))
    assert len(calls) == 1
    assert not list(controller.con._memtables)  # registrations end with the request, programs own only Arrow
    controller.clear_cache()
    assert controller.core_cache.used_bytes("plan") == 0
