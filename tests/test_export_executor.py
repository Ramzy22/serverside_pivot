import asyncio

import pytest

from pivot_engine.export_executor import PivotExportExecutor
from pivot_engine.export_limits import ExportLimitError


class _Planner:
    def plan(self, spec):
        return {"queries": ["planned-query"]}


class _StreamingBackend:
    def execute_streaming(self, query, batch_size):
        assert query == "planned-query"
        assert batch_size == 5000
        return [[{"region": "North", "sales": 4}], [{"region": "South", "sales": 7}]]


class _StreamingController:
    planner = _Planner()
    backend = _StreamingBackend()

    def _normalize_spec(self, spec):
        return {"normalized": spec}


def test_streaming_export_executor_preserves_csv_headers_once():
    async def collect():
        executor = PivotExportExecutor(_StreamingController())
        return [chunk async for chunk in executor.stream({"table": "sales"}, "csv")]

    chunks = asyncio.run(collect())
    export_text = b"".join(chunks).decode("utf-8")
    assert export_text.count('"region","sales"') == 1
    assert '"North",4' in export_text
    assert '"South",7' in export_text


def test_streaming_export_executor_stops_before_emitting_an_oversized_row_set():
    async def collect():
        executor = PivotExportExecutor(_StreamingController(), max_rows=1)
        return [chunk async for chunk in executor.stream({"table": "sales"}, "csv")]

    with pytest.raises(ExportLimitError, match="Export contains 2 rows"):
        asyncio.run(collect())


def test_streaming_export_executor_enforces_the_byte_budget_before_yielding():
    async def collect():
        executor = PivotExportExecutor(_StreamingController(), max_bytes=8)
        return [chunk async for chunk in executor.stream({"table": "sales"}, "csv")]

    with pytest.raises(ExportLimitError, match="server limit is 8"):
        asyncio.run(collect())
