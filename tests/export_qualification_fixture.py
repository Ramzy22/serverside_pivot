"""Literal runtime-export qualification inputs and document readers.

The expected values in this module are deliberately hand-calculated rather
than derived through the exporter under test.
"""

from __future__ import annotations

import csv
from html.parser import HTMLParser
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile

from pivot_engine.runtime import PivotViewState
from pivot_engine.tanstack_adapter import TanStackResponse


# The response represents the filtered `region in {Éclair, München}` result.
# Its literal total is 100.25 + 49.75 = 150.00 and 2 + 3 = 5.
QUALIFICATION_ROWS = [
    {"_id": "Éclair", "sales_sum": 100.25, "units_sum": 2},
    {"_id": "München", "sales_sum": 49.75, "units_sum": 3},
    {"_id": "Grand Total", "sales_sum": 150.0, "units_sum": 5, "_isTotal": True},
]

EXPECTED_ORDER = ["hierarchy", "sales_sum", "units_sum"]
EXPECTED_TABLE = [
    ["Region", "Sales", "Units"],
    ["Éclair", "100.25", "2"],
    ["München", "49.75", "3"],
    ["Grand Total", "150.0", "5"],
]


def qualification_state() -> PivotViewState:
    return PivotViewState(
        row_fields=["region"],
        val_configs=[
            {"field": "sales", "agg": "sum", "label": "Sales"},
            {"field": "units", "agg": "sum", "label": "Units"},
        ],
        filters={"region": {"operator": "in", "values": ["Éclair", "München"]}},
    )


def qualification_response() -> TanStackResponse:
    return TanStackResponse(
        data=QUALIFICATION_ROWS,
        columns=[
            {"id": "hierarchy", "header": "Region"},
            {"id": "sales_sum", "header": "Sales"},
            {"id": "units_sum", "header": "Units"},
        ],
        total_rows=3,
    )


def expected_export(export_format: str = "csv") -> list[list[str]]:
    if export_format == "html":
        # HTML uses the runtime's default whole-number display formatting.
        return [
            ["Region", "Sales", "Units"],
            ["Éclair", "100", "2"],
            ["München", "50", "3"],
            ["Grand Total", "150", "5"],
        ]
    return EXPECTED_TABLE


def read_csv_export(path: str | Path, delimiter: str = ",") -> list[list[str]]:
    with open(path, encoding="utf-8-sig", newline="") as export_file:
        return list(csv.reader(export_file, delimiter=delimiter))


class _TableReader(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._row = []
        elif tag in {"th", "td"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self._cell is not None and self._row is not None:
            self._row.append("".join(self._cell))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None


def read_html_export(path: str | Path) -> list[list[str]]:
    reader = _TableReader()
    reader.feed(Path(path).read_text(encoding="utf-8"))
    return reader.rows


def read_xlsx_export(path: str | Path) -> list[list[str]]:
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as workbook:
        sheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
    rows: list[list[str]] = []
    for row in sheet.findall(".//x:sheetData/x:row", namespace):
        values: list[str] = []
        for cell in row.findall("x:c", namespace):
            inline = cell.find("x:is/x:t", namespace)
            numeric = cell.find("x:v", namespace)
            values.append(inline.text if inline is not None and inline.text is not None else numeric.text if numeric is not None and numeric.text is not None else "")
        rows.append(values)
    return rows
