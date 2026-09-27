"""Regression tests for bounded, record-batch export execution."""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from pivot_engine.export_executor import PivotExportExecutor
from pivot_engine.resource_governor import ResourceExhaustedError
from pivot_engine.runtime.export_service import RuntimeExportService
from pivot_engine.runtime.models import PivotViewState


class _BatchBackend:
    def __init__(self, *, full_only: bool = False):
        self.full_only = full_only
        self.execute_called = False

    def execute_record_batches(self, query, *, max_batch_bytes, max_batch_rows):
        assert max_batch_bytes > 0
        assert max_batch_rows > 0
        yield pa.RecordBatch.from_pylist([{"region": "A", "sales": 1}])
        yield pa.RecordBatch.from_pylist([{"region": "B", "sales": 2}])

    def execute(self, query):
        self.execute_called = True
        raise AssertionError("full table execution is not allowed for a bounded export")


class _FullTableOnlyBackend:
    def __init__(self):
        self.execute_called = False

    def execute(self, query):
        self.execute_called = True
        raise AssertionError("full table backend must be rejected before invocation")


class _Controller:
    def __init__(self, backend):
        self.backend = backend
        self.planner = self

    def _normalize_spec(self, spec):
        return spec

    def plan(self, spec):
        return {"queries": [{"sql": "SELECT region, sales FROM sales"}]}

    async def run_pivot_async(self, spec, return_format="arrow"):
        raise AssertionError("full-table fallback must not run")


def test_export_writes_each_record_batch_without_full_table_materialization():
    backend = _BatchBackend()
    executor = PivotExportExecutor(_Controller(backend), batch_size=1)

    chunks = asyncio.run(_collect(executor.stream(object(), export_format="csv")))

    output = b"".join(chunks)
    assert b'"region","sales"' in output
    assert b'"A",1' in output
    assert b'"B",2' in output
    assert backend.execute_called is False


def test_full_table_only_backend_is_rejected_before_execution():
    backend = _FullTableOnlyBackend()
    executor = PivotExportExecutor(_Controller(backend))

    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        asyncio.run(_collect(executor.stream(object(), export_format="csv")))

    assert backend.execute_called is False


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


async def _collect(iterator):
    return [chunk async for chunk in iterator]
