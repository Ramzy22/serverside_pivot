"""Warm response hits reuse the converted result and the schema superset."""
import pyarrow as pa
import pytest

from pivot_engine import create_tanstack_adapter
from pivot_engine.types.pivot_spec import PivotSpec


def test_warm_dict_hit_never_reconverts_cells_and_caller_mutation_is_isolated(monkeypatch):
    adapter = create_tanstack_adapter(backend_uri=':memory:')
    adapter.load_data(pa.table({'r': ['A', 'B'], 'v': [1, 2]}), 't')
    c = adapter.controller
    spec = PivotSpec.from_dict({'table': 't', 'rows': ['r'], 'measures': [{'field': 'v', 'agg': 'sum', 'alias': 's'}]})
    first = c.run_pivot(spec, return_format='dict')
    first['rows'][0][1] = 999
    def refuse(*args, **kwargs):
        raise AssertionError('a cached dict was converted again')
    monkeypatch.setattr(c, '_convert_table_to_dict', refuse)
    second = c.run_pivot(spec, return_format='dict')
    assert second['rows'] == [['A', 1], ['B', 2]]
    c.close()


@pytest.mark.asyncio
async def test_viewport_schema_superset_hit_needs_no_read_and_keeps_original_schema(monkeypatch):
    from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest

    adapter = create_tanstack_adapter(backend_uri=':memory:')
    adapter.load_data(pa.table({'r': ['A', 'B'], 'v': [1, 2]}), 't')
    request = TanStackRequest(operation=TanStackOperation.GET_DATA, table='t',
                             columns=[{'id': 'r'}, {'id': 's', 'aggregationField': 'v', 'aggregationFn': 'sum'}],
                             filters={}, sorting=[], grouping=['r'], aggregations=[], totals=True)
    first = await adapter.handle_virtual_scroll_request(request, 0, 10, needs_col_schema=True,
                                                        include_grand_total=True, _allow_prefetch=False)
    assert first.col_schema is not None
    adapter._viewport_cache.row_block_cache.clear()
    def refuse(*args, **kwargs):
        raise AssertionError('schema-superset response should serve this viewport')
    monkeypatch.setattr(adapter, '_assemble_cached_row_block_window', refuse)
    second = await adapter.handle_virtual_scroll_request(request, 0, 10, needs_col_schema=False,
                                                         include_grand_total=True, _allow_prefetch=False)
    assert second.data == first.data and second.total_rows == first.total_rows
    assert second.col_schema is None
    again = await adapter.handle_virtual_scroll_request(request, 0, 10, needs_col_schema=True,
                                                        include_grand_total=True, _allow_prefetch=False)
    assert again.col_schema == first.col_schema
    adapter.controller.close()
