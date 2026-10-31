"""Authoritative source values for clipboard Unicode/null/empty qualification."""
import json
from pathlib import Path
import pyarrow as pa
import pytest
from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import PivotRuntimeService, SessionRequestGate
from tests.test_flagship_shared_runtime import _response

from tests.flagship_clipboard_values import FIELDS, EXPECTED, assert_source_rows


@pytest.mark.parametrize("aggregation", ["first", "last"])
def test_shared_clipboard_source_values(aggregation):
    rows = json.loads(Path(__file__).with_name("data").joinpath("flagship_parity_rows.json").read_text(encoding="utf-8"))["rows"]
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("flagship_shared", pa.Table.from_pylist(rows))
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())
    response = _response(service, {}, 1, row_fields=["rowId"], col_fields=[],
                         val_configs=[{"field": field, "agg": aggregation} for field in FIELDS],
                         show_row_totals=False, show_col_totals=False, needs_col_schema=True)
    assert response.status == "data", response.message
    assert_source_rows(response.data, aggregation)
    columns = response.col_schema["columns"]
    assert [column["id"] for column in columns] == [f"{field}_{aggregation}" for field in FIELDS]
    assert [column["index"] for column in columns] == list(range(4))
    assert response.layout_revision == response.col_schema.get("layoutRevision")  # no sparse-pivot catalog in this layout
