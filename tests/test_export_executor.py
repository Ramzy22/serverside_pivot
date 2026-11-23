"""Bounded streaming exports: core pages of at most ``batch_size`` rows, written as CSV/Parquet."""

import asyncio
import copy

import pyarrow as pa
import pytest

from pivot_engine.export_executor import PivotExportExecutor
from pivot_engine.export_limits import ExportLimitError
from pivot_engine.resource_governor import ResourceExhaustedError
from pivot_engine.runtime.export_service import RuntimeExportService
from pivot_engine.runtime.models import PivotViewState
from pivot_engine.types.pivot_spec import PivotSpec

ROWS = [{"region": "North", "sales": 4}, {"region": "South", "sales": 7}, {"region": "West", "sales": 9}]


class _PagedController:
    """Serves the export rows as core pages; records every (offset, limit) it is asked for."""

    def __init__(self, rows=ROWS, result=None):
        self.rows = rows
        self.result = result
        self.windows = []

    def _normalize_spec(self, spec):
        return PivotSpec.from_dict(spec) if isinstance(spec, dict) else copy.copy(spec)

    async def run_pivot_async(self, spec, return_format="arrow"):
        assert return_format == "arrow"
        self.windows.append((spec.offset, spec.limit))
        if self.result is not None:
            return self.result
        return pa.Table.from_pylist(self.rows[spec.offset:spec.offset + spec.limit])


async def _collect(stream):
    return [chunk async for chunk in stream]


def _stream(controller, spec=None, fmt="csv", **limits):
    executor = PivotExportExecutor(controller, **limits)
    return asyncio.run(_collect(executor.stream(spec or {"table": "sales"}, fmt)))


def test_csv_header_is_written_once_across_pages():
    text = b"".join(_stream(_PagedController(), batch_size=2)).decode("utf-8")
    assert text.count('"region","sales"') == 1
    assert '"North",4' in text and '"South",7' in text and '"West",9' in text


def test_every_read_is_a_bounded_page_and_pages_partition_the_rows():
    controller = _PagedController()
    _stream(controller, batch_size=2)
    assert controller.windows == [(0, 2), (2, 2)]  # a short page ends the export


def test_spec_offset_and_limit_bound_the_export():
    controller = _PagedController()
    text = b"".join(_stream(controller, {"table": "sales", "offset": 1, "limit": 1}, batch_size=2)).decode()
    assert '"South",7' in text and "North" not in text and "West" not in text
    assert controller.windows == [(1, 1)]


def test_parquet_export_round_trips():
    import io

    import pyarrow.parquet as pq

    payload = b"".join(_stream(_PagedController(), fmt="parquet", batch_size=2))
    assert pq.read_table(io.BytesIO(payload)).to_pylist() == ROWS


def test_an_oversized_row_set_stops_before_emitting():
    with pytest.raises(ExportLimitError, match="Export contains"):
        _stream(_PagedController(), max_rows=1)


def test_the_byte_budget_is_enforced_before_yielding():
    # a row wider than the budget is refused by the batch splitter or the byte limit: nothing is yielded
    with pytest.raises((ExportLimitError, ResourceExhaustedError)):
        _stream(_PagedController(), max_bytes=8)


def test_a_non_streaming_format_is_rejected_before_any_read():
    controller = _PagedController()
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        _stream(controller, fmt="xlsx")
    assert controller.windows == []


def test_a_non_arrow_page_is_rejected():
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        _stream(_PagedController(result={"rows": []}))


def test_runtime_streamed_export_writes_batches_and_bounds_output(tmp_path):
    service = RuntimeExportService(max_rows=10, max_bytes=1024)
    state = PivotViewState()
    batches = [
        pa.RecordBatch.from_pylist([{"region": "A", "sales": 1}]),
        pa.RecordBatch.from_pylist([{"region": "B", "sales": 2}]),
    ]

    payload = asyncio.run(
        service.build_streamed_export(
            batches,
            state,
            {"format": "csv", "filename": "stream.csv"},
        )
    )
    try:
        assert payload["rows"] == 2
        assert payload["contentLength"] <= 1024
        with open(payload["contentPath"], encoding="utf-8-sig") as exported:
            lines = exported.read().splitlines()
        assert lines == [
            "Region,Sales",
            "A,1",
            "B,2",
        ]
    finally:
        import os
        if os.path.exists(payload["contentPath"]):
            os.remove(payload["contentPath"])


def test_runtime_streamed_xlsx_rejects_before_consuming_batches():
    consumed = False

    def batches():
        nonlocal consumed
        consumed = True
        yield pa.RecordBatch.from_pylist([{"value": 1}])

    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        asyncio.run(
            RuntimeExportService().build_streamed_export(
                batches(), PivotViewState(), {"format": "xlsx"},
            )
        )
    assert consumed is False
