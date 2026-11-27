"""Long blocks aggregate their own records; only the response edge widens cells."""
from dataclasses import replace

import pyarrow as pa
import pytest

from pivot_engine.core import exec as core
from pivot_engine.core.ir import MeasureRef, PivotQuery, SortKey, Window
from .test_core_semantics import _assert_equal, _oracle
from .test_cross_backend import _connect, _disconnect


def facts():
    return pa.table({'r': ['A', 'A', 'A', 'B', 'B', None],
                     'child': ['a', 'b', 'b', 'a', 'c', 'a'],
                     'period': ['x', 'y', 'x', 'y', None, 'x'],
                     'v': [1, 9, 8, 7, 3, None]})


def query():
    return PivotQuery('t', ('r', 'child'),
                      (MeasureRef('m', 'median', 'v'), MeasureRef('d', 'count_distinct', 'v')),
                      columns=('period',), column_members=(('x',), ('y',), (None,)), expanded=frozenset())


@pytest.mark.parametrize('backend', ['duckdb', 'sqlite', 'polars'])
def test_long_blocks_equal_reference_and_oracle_through_column_and_row_interactions(backend):
    from pivot_engine.core.physical.long_form import read
    con = _connect(backend, facts())
    # SQLite has no median: retain the exact distinct-count holistic measure.
    q = query() if backend != 'sqlite' else replace(query(), measures=(MeasureRef('d', 'count_distinct', 'v'),))
    try:
        for q in (q, replace(q, expanded=frozenset({('A',)})),
                  replace(q, column_members=(('y',),), sort=(SortKey('d', desc=True, member=('x',)),)),
                  replace(q, window=Window(2, 2)), replace(q, subtotals='none')):
            actual = core._frame([read(q, con.table('t'), True)], q, True)
            _assert_equal([(r['path'], r['values']) for r in actual.records()], _oracle(facts().to_pylist(), q))
            reference = core.execute(q, con.table, strategy='union_all', grand_total=True)
            assert actual.total == reference.total
    finally:
        _disconnect(con)


def test_native_storage_is_long_and_horizontal_scroll_never_reaggregates():
    from pivot_engine.core.physical.level_tables import BoundStatement, store_for
    con = _connect('duckdb', facts())
    statements = []
    def run(s):
        statements.append(s)
        return s.to_pyarrow()
    try:
        with core.statement_runner(run):
            core.execute(query(), con.table)
            first_writes = len([s for s in statements if isinstance(s, BoundStatement) and s.kind == 'insert'])
            core.execute(replace(query(), column_members=(('y',),)), con.table)
        assert len([s for s in statements if isinstance(s, BoundStatement) and s.kind == 'insert']) == first_writes
        for b in store_for(con).blocks.values():
            assert 'period' in b.relation.columns
            assert not any(c.startswith('__cell_') for c in b.relation.columns)
        writes = [s for s in statements if isinstance(s, BoundStatement) and s.kind == 'insert']
        assert all('GROUPING SETS' in s.sql for s in writes)
        assert len(writes) == 1  # level 1 and grand total: one scan
    finally:
        _disconnect(con)


def test_totals_off_prunes_grand_total_grouping_sets():
    from pivot_engine.core.physical.level_tables import store_for
    con = _connect('duckdb', facts())
    try:
        actual = core.execute(query(), con.table, prune_total=True)
        b = next(iter(store_for(con).blocks.values()))
        assert con.con.execute(f'SELECT count(*) FROM "{store_for(con).schema}"."{b.name}" WHERE __depth=0').fetchone()[0] == 0
        assert [(r['path'], r['values']['d']) for r in actual.records() if r['path']] == [(('A',), 3), (('B',), 2), ((None,), 0)]
    finally:
        _disconnect(con)


def test_sorts_are_bound_to_the_same_view_statement_and_never_rescan_facts():
    from pivot_engine.core.physical.parameters import BoundStatement
    con = _connect('duckdb', facts())
    seen = []
    def run(s):
        seen.append(s)
        return s.to_pyarrow()
    try:
        with core.statement_runner(run):
            for sort in ((), (SortKey('d', desc=True),), (SortKey('r', desc=True),),
                         (SortKey('d', desc=True, member=('x',)),),
                         (SortKey('d', desc=True, member=('y',)),),
                         (SortKey('r', by='v'),)):
                q = replace(query(), sort=sort)
                actual = core.execute(q, con.table)
                _assert_equal([(r['path'], r['values']) for r in actual.records()], _oracle(facts().to_pylist(), q))
        reads = [s for s in seen if isinstance(s, BoundStatement) and s.kind == 'read']
        cell_sort_reads = [s for s in reads if '$sm0_0' in s.sql]
        assert len(cell_sort_reads) == 2
        assert len({s.sql for s in cell_sort_reads}) == 1
        inserts = [s for s in seen if isinstance(s, BoundStatement) and s.kind == 'insert']
        assert len(inserts) == 2  # level 1 + lazy member-key lookup table only; never rescans level blocks
        assert sum('GROUPING SETS' in s.sql for s in inserts) == 1
    finally:
        _disconnect(con)
