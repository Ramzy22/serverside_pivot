import os

import pytest

from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState
from pivot_engine.runtime.export_service import ExportLimitError, RuntimeExportService
from pivot_engine.runtime.request_factory import RuntimeRequestFactory
from pivot_engine.runtime.session_gate import SessionRequestGate


def test_export_pagination_uses_configured_limit_when_no_page_size_is_supplied():
    pagination = RuntimeRequestFactory.build_export_pagination({}, max_rows=25)

    assert pagination["pageSize"] == 25
    assert pagination["startRow"] == 0
    assert pagination["endRow"] == 24


def test_export_pagination_rejects_explicit_oversized_ranges():
    with pytest.raises(ValueError, match="limited to 25 rows"):
        RuntimeRequestFactory.build_export_pagination(
            {"pageSize": 26},
            max_rows=25,
        )


def test_export_service_deletes_an_oversized_generated_file(tmp_path):
    content_path = tmp_path / "oversized.csv"
    content_path.write_bytes(b"x" * 32)
    service = RuntimeExportService(max_rows=10, max_bytes=16)
    service._build_export_payload = lambda *_args: {
        "contentPath": str(content_path),
        "contentLength": content_path.stat().st_size,
        "rows": 1,
    }

    with pytest.raises(ExportLimitError, match="server limit is 16"):
        service.build_export_payload(None, None, None)

    assert not content_path.exists()


def test_export_service_rejects_materialized_rows_before_creating_a_document():
    service = RuntimeExportService(max_rows=1)

    class Response:
        data = [{"region": "North"}, {"region": "South"}]

    with pytest.raises(ExportLimitError, match="Export contains 2 rows"):
        service.build_export_payload(Response(), None, None)


def test_runtime_router_rejects_oversized_export_before_adapter_execution(monkeypatch):
    monkeypatch.setenv("PIVOT_EXPORT_MAX_ROWS", "2")

    class Adapter:
        async def handle_virtual_scroll_request(self, *_args, **_kwargs):
            raise AssertionError("oversized exports must not execute")

    service = PivotRuntimeService(
        adapter_getter=Adapter,
        session_gate=SessionRequestGate(),
    )
    response = service.process(
        PivotViewState(
            export_request={"format": "csv", "pageSize": 3},
        ),
        PivotRequestContext(
            table="sales",
            trigger_prop="pivot.runtimeRequest",
            session_id="export-limit-session",
            client_instance="export-limit-client",
        ),
    )

    assert response.status == "error"
    assert "limited to 2 rows" in response.message
