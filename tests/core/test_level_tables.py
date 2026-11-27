"""Native level blocks are exact, reusable, scoped and bounded."""
from dataclasses import replace

import ibis
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from pivot_engine.core import exec as core
from pivot_engine.core.ir import MeasureRef, PivotQuery, Predicate, SortKey, Window
from .test_core_semantics import _assert_equal, _oracle, examples
from .test_cross_backend import _connect, _disconnect


def test_level_tables_are_an_in_process_capability():
    from pivot_engine.core.physical import capabilities
    con = ibis.duckdb.connect()
    assert capabilities.level_tables(con)
    _disconnect(con)
    assert not capabilities.level_tables(type('Server', (), {'name': 'postgres'})())


def test_remote_and_read_only_duckdb_connections_never_enable_native_writes(monkeypatch):
    from pivot_engine.core.physical.capabilities import level_tables
    from pivot_engine.core.physical.level_tables import LevelTableStore
    con = ibis.duckdb.connect()
    for options in ({'database': 'md:remote'}, {'read_only': True}):
        monkeypatch.setattr(con, '_con_kwargs', options)
        assert not level_tables(con)
        with pytest.raises(ValueError):
            LevelTableStore(con)
    _disconnect(con)


def _facts():
    return pa.table({'r': ['A', 'A', 'B', None], 'c': ['x', 'y', 'x', 'z'], 'v': [1, 7, 9, 3]})


def _query():
    return PivotQuery(dataset='t', rows=('r', 'c'), measures=(MeasureRef('m', 'median', 'v'),),
                      expanded=frozenset())


def test_new_expansions_bind_parents_and_reuse_sql_without_memtables():
    from pivot_engine.core.physical.level_tables import BoundStatement, store_for
    con = _connect('duckdb', _facts())
    statements = []
    def run(statement):
        statements.append(statement)
        return statement.to_pyarrow()
    with core.statement_runner(run):
        core.execute(_query(), con.table)
        for path in [('A',), ('B',), (None,), ('A',)]:
            core.execute(replace(_query(), expanded=frozenset({path})), con.table)
    reads = [s for s in statements if isinstance(s, BoundStatement) and s.kind == 'read']
    assert len({s.sql for s in reads}) == 1
    assert '$parents_2' in reads[0].sql
    assert len({str(s.params) for s in reads}) > 1
    writes = [s for s in statements if isinstance(s, BoundStatement) and s.kind == 'insert']
    assert len(writes) == 4  # level 1 + total in one scan, three distinct parents (NULL included)
    assert len({s.sql for s in writes}) == 2
    assert all('GROUPING SETS' in s.sql for s in writes)
    assert all(not s.expr.op().find(__import__('ibis').expr.operations.InMemoryTable) for s in reads + writes)
    assert store_for(con).cache.used_bytes('level') > 0
    _disconnect(con)


@pytest.mark.parametrize('backend', ['duckdb', 'sqlite', 'polars'])
@pytest.mark.parametrize('pivoted', [False, True])
@settings(max_examples=examples(40), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(values=st.lists(st.tuples(st.sampled_from(['A', 'B', None]), st.sampled_from(['x', 'y', None]),
                                st.one_of(st.none(), st.integers(-10, 10))), min_size=0, max_size=25),
       order=st.lists(st.sampled_from(['expand', 'collapse', 'scroll', 'horizontal', 'sort', 'filter']), min_size=1, max_size=10))
def test_interaction_sequences_equal_union_all_and_oracle(backend, pivoted, values, order):
    table = pa.table({'r': pa.array([v[0] for v in values], pa.string()),
                      'c': pa.array([v[1] for v in values], pa.string()),
                      'v': pa.array([v[2] for v in values], pa.int64()),
                      'p': pa.array(['x' if i % 2 else 'y' for i, _ in enumerate(values)], pa.string())})
    records = table.to_pylist()
    con = _connect(backend, table)
    q = replace(_query(), measures=(MeasureRef('m', 'sum', 'v'), MeasureRef('d', 'count_distinct', 'v')))
    if pivoted:
        q = replace(q, columns=('p',), column_members=(('x',), ('y',), (None,)))
    try:
        for action in order:
            if action == 'expand':
                q = replace(q, expanded=frozenset({('A',), (None,)}))
            elif action == 'collapse':
                q = replace(q, expanded=frozenset())
            elif action == 'scroll':
                q = replace(q, window=Window(2, 3))
            elif action == 'sort':
                q = replace(q, sort=(SortKey('m', desc=True, member=('x',) if pivoted else None),))
            elif action == 'horizontal':
                if pivoted:
                    q = replace(q, column_members=(('y',),))
            else:
                q = replace(q, filters=(Predicate('v', '>', 0),))
            actual = core.execute(q, con.table, grand_total=True)
            reference = core.execute(q, con.table, strategy='union_all', grand_total=True)
            rows = lambda frame: [(r['path'], r['values']) for r in frame.records()]
            _assert_equal(rows(actual), rows(reference))
            _assert_equal(rows(actual), _oracle(records, q))
            assert actual.total == reference.total
            assert actual.grand_total.equals(reference.grand_total)
    finally:
        _disconnect(con)


def test_level_table_scope_binding_rejects_other_grants_even_with_colliding_fingerprints():
    from pivot_engine.core.authority.scope import scoped_execution
    from pivot_engine.core.authority.verifier import PlanVerificationError, verify_scoped
    from pivot_engine.core.physical.level_tables import store_for
    from .test_compile_cache import _grant
    con = _connect('duckdb', _facts().append_column('tenant', pa.array(['A', 'B', 'A', 'B'])))
    with scoped_execution(_grant('A', fp='collision'), connections=[con]):
        core.execute(_query(), con.table)
        blocks = list(store_for(con).blocks.values())
        expr = blocks[0].relation
        verify_scoped(expr)
    with scoped_execution(_grant('B', fp='collision'), connections=[con]):
        with pytest.raises(PlanVerificationError):
            verify_scoped(expr)
        actual = core.execute(_query(), con.table)
        reference = core.execute(_query(), con.table, strategy='union_all')
        _assert_equal([(r['path'], r['values']) for r in actual.records()],
                      [(r['path'], r['values']) for r in reference.records()])
    with pytest.raises(PlanVerificationError):
        verify_scoped(expr)


def test_controller_invalidation_drops_native_tables_and_allows_fresh_data():
    from pivot_engine import create_tanstack_adapter
    from pivot_engine.core.physical.level_tables import store_for
    adapter = create_tanstack_adapter(backend_uri=':memory:')
    adapter.load_data(_facts(), 't')
    controller = adapter.controller
    core.execute(_query(), controller.resolve_table)
    store = store_for(controller.con)
    names = [b.name for b in store.blocks.values()]
    assert names
    adapter.load_data(pa.table({'r': ['A'], 'c': ['x'], 'v': [99]}), 't')
    assert not store.blocks
    assert all(name not in controller.con.list_tables(database=store.schema) for name in names)
    assert core.execute(_query(), controller.resolve_table).records()[0]['values']['m'] == 99


def test_level_budget_evicts_whole_tables_and_missing_blocks_are_rebuilt():
    from pivot_engine.core.physical.level_tables import LevelTableStore
    con = _connect('duckdb', _facts())
    store = LevelTableStore(con, budget=1)
    object.__setattr__(con, '_pivot_level_store', store)
    actual = core.execute(replace(_query(), expanded=None), con.table)
    reference = core.execute(replace(_query(), expanded=None), con.table, strategy='union_all')
    _assert_equal([(r['path'], r['values']) for r in actual.records()],
                  [(r['path'], r['values']) for r in reference.records()])
    assert store.cache.used_bytes('level') <= 1
    assert not store.blocks
    assert con.list_tables(database=store.schema) == []
