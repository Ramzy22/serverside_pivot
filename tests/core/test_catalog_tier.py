"""Catalogs share a revision and scope boundary with native levels."""
from dataclasses import replace

import pyarrow as pa

from pivot_engine.core import exec as core
from pivot_engine.core.ir import MeasureRef, PivotQuery, Predicate
from .test_cross_backend import _connect, _disconnect


def test_catalog_windows_share_one_read_and_case_fold_resolves_all_exact_variants():
    from pivot_engine.core.catalog import store_for
    con = _connect('duckdb', pa.table({'r': ['A', 'a', 'B'], 'c': ['x', 'y', 'z'], 'v': [1, 3, 8]}))
    q = PivotQuery('t', ('r',), (MeasureRef('s', 'sum', 'v'),), columns=('c',))
    try:
        assert core.column_members(q, con.table, limit=1) == (('x',),)
        store = store_for(con)
        misses = store.cache.misses
        assert core.column_members(q, con.table, offset=1) == (('y',), ('z',))
        assert store.cache.misses == misses
        f = replace(q, filters=(Predicate('r', '=', 'a', case_sensitive=False),))
        actual = core.execute(f, con.table)
        assert [(r['path'], r['values']['s']) for r in actual.records()] == [((), 4), (('A',), 1), (('a',), 3)]
        lowered = store.lower_filters(f, con.table('t'))
        assert lowered.filters[0].op == 'in'
        assert set(lowered.filters[0].value) == {'A', 'a'}
    finally:
        _disconnect(con)
