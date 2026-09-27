import os

import pytest

from pivot_engine.runtime.export_service import RuntimeExportService
from tests.export_qualification_fixture import (
    EXPECTED_ORDER,
    expected_export,
    qualification_response,
    qualification_state,
    read_csv_export,
    read_html_export,
    read_xlsx_export,
)


@pytest.mark.parametrize("requested, expected", [(None, "csv"), ("xls", "xlsx"), ("TSV", "tsv")])
def test_runtime_export_normalizes_supported_format(requested, expected):
    assert RuntimeExportService.normalize_export_format(requested) == expected


def test_runtime_export_rejects_unknown_format_without_creating_a_file():
    with pytest.raises(ValueError, match="Unsupported export format"):
        RuntimeExportService().build_export_payload(
            qualification_response(), qualification_state(), {"format": "pdf"}
        )


@pytest.mark.parametrize("export_format", ["csv", "tsv", "html", "xlsx"])
def test_runtime_exports_fixture_with_order_filter_totals_numeric_and_unicode(export_format):
    payload = RuntimeExportService().build_export_payload(
        qualification_response(),
        qualification_state(),
        {"format": export_format, "colIds": EXPECTED_ORDER},
    )
    try:
        if export_format == "csv":
            document = read_csv_export(payload["contentPath"])
        elif export_format == "tsv":
            document = read_csv_export(payload["contentPath"], delimiter="\t")
        elif export_format == "html":
            document = read_html_export(payload["contentPath"])
        else:
            document = read_xlsx_export(payload["contentPath"])
        assert document == expected_export(export_format)
    finally:
        if os.path.exists(payload["contentPath"]):
            os.remove(payload["contentPath"])
