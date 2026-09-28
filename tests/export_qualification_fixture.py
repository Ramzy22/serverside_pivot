"""Literal runtime-export qualification inputs and document readers.

The expected values in this module are deliberately hand-calculated rather
than derived through the exporter under test.
"""

from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
import io
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


# Browser qualification uses these literal source records in every flagship
# host.  The fifth row is intentionally outside the configured ``scope``
# filter.  Expected totals below are hand-calculated from the first four rows:
# Q1: 150.00 sales, 5 units, -8.25 variance
# Q2:  50.00 sales, 5 units,  1.50 variance
# all: 200.00 sales, 10 units, -6.75 variance
FLAGSHIP_QUALIFICATION_SOURCE_ROWS = [
    {"region": "Éclair", "account": "東京", "scope": "Included", "period": "Q1", "ventes_€": 100.25, "unités": 2, "écart": -12.5},
    {"region": "Éclair", "account": "東京", "scope": "Included", "period": "Q2", "ventes_€": 20.0, "unités": 1, "écart": 2.5},
    {"region": "München", "account": "القاهرة", "scope": "Included", "period": "Q1", "ventes_€": 49.75, "unités": 3, "écart": 4.25},
    {"region": "München", "account": "القاهرة", "scope": "Included", "period": "Q2", "ventes_€": 30.0, "unités": 4, "écart": -1.0},
    {"region": "São Paulo", "account": "北京", "scope": "Excluded", "period": "Q1", "ventes_€": 999999.5, "unités": 11, "écart": 99.0},
]

FLAGSHIP_FILTER = {
    "scope": {
        "operator": "AND",
        "conditions": [{"type": "eq", "value": "Included", "caseSensitive": False}],
    }
}
FLAGSHIP_ROW_FIELDS = ["region", "account"]
FLAGSHIP_COL_FIELDS = ["period"]
FLAGSHIP_VALUE_CONFIGS = [
    {"field": "ventes_€", "agg": "sum", "label": "Ventes (€)"},
    {"field": "unités", "agg": "sum", "label": "Unités"},
    {"field": "écart", "agg": "sum", "label": "Écart"},
]
FLAGSHIP_EXPECTED_TOTALS = {
    "sales": ["150", "50", "200"],
    "units": ["5", "5", "10"],
    "variance": ["-8.25", "1.5", "-6.75"],
}
FLAGSHIP_INCLUDED_TEXT = ("Éclair", "München", "東京", "القاهرة")
FLAGSHIP_EXCLUDED_TEXT = ("São Paulo", "北京", "999999.5")


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


def read_html_text(document: str) -> list[list[str]]:
    reader = _TableReader()
    reader.feed(document)
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


def read_xlsx_document(path: str | Path) -> dict[str, object]:
    """Return values and type/merge metadata without a spreadsheet library."""
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as workbook:
        sheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
    rows: list[list[str]] = []
    numeric_cells: list[dict[str, str]] = []
    for row in sheet.findall(".//x:sheetData/x:row", namespace):
        values: list[str] = []
        for cell in row.findall("x:c", namespace):
            inline = cell.find("x:is/x:t", namespace)
            numeric = cell.find("x:v", namespace)
            value = inline.text if inline is not None and inline.text is not None else numeric.text if numeric is not None and numeric.text is not None else ""
            values.append(value)
            if numeric is not None and cell.get("t") is None:
                numeric_cells.append({"ref": cell.get("r", ""), "value": value})
        rows.append(values)
    merged_ranges = [
        element.get("ref", "")
        for element in sheet.findall(".//x:mergeCells/x:mergeCell", namespace)
    ]
    return {"rows": rows, "numericCells": numeric_cells, "mergedRanges": merged_ranges}


def _flatten(rows: list[list[str]]) -> list[str]:
    return [value for row in rows for value in row]


def _numbers(values: list[str]) -> set[Decimal]:
    numbers: set[Decimal] = set()
    for value in values:
        try:
            numbers.add(Decimal(value))
        except InvalidOperation:
            continue
    return numbers


def _expected_total_numbers() -> set[Decimal]:
    return {Decimal(value) for values in FLAGSHIP_EXPECTED_TOTALS.values() for value in values}


def _assert_qualification_membership(rows: list[list[str]], *, all_records: bool) -> None:
    values = _flatten(rows)
    joined = "\n".join(values)
    assert "Éclair" in joined
    assert "München" in joined
    if all_records:
        assert "東京" in joined
        assert "القاهرة" in joined
    else:
        assert "東京" not in joined
        assert "القاهرة" not in joined
    assert not any(value in joined for value in FLAGSHIP_EXCLUDED_TEXT)


def assert_flagship_download(path: str | Path, export_format: str) -> dict[str, object]:
    """Assert literal filtered/order/numeric/Unicode export behavior."""
    if export_format == "csv":
        rows = read_csv_export(path)
        numeric_cells: list[dict[str, str]] = []
        merged_ranges: list[str] = []
    elif export_format == "xlsx":
        document = read_xlsx_document(path)
        rows = document["rows"]
        numeric_cells = document["numericCells"]
        merged_ranges = document["mergedRanges"]
        assert merged_ranges
        numeric_values = _numbers([cell["value"] for cell in numeric_cells])
        assert _expected_total_numbers().issubset(numeric_values)
    else:
        raise AssertionError(f"Unsupported flagship download format: {export_format}")

    assert len(rows) > 2
    header_rows = rows[:2] if export_format == "xlsx" else rows[:1]
    header_text = " | ".join(_flatten(header_rows))
    assert all(label in header_text for label in ("Unités", "Ventes", "€", "Écart"))
    order_header_text = " | ".join(rows[1] if export_format == "xlsx" else rows[0])
    assert order_header_text.index("Unités") < order_header_text.index("Ventes")
    _assert_qualification_membership(rows, all_records=True)
    assert _expected_total_numbers().issubset(_numbers(_flatten(rows)))
    return {
        "format": export_format,
        "rows": len(rows),
        "columns": max((len(row) for row in rows), default=0),
        "numericCellCount": len(numeric_cells),
        "mergedHeaderCount": len(merged_ranges),
        "unicodeVerified": True,
        "filterVerified": True,
        "columnOrderVerified": True,
        "totalsVerified": True,
    }


def assert_flagship_clipboard(document: str, representation: str, *, all_records: bool) -> dict[str, object]:
    """Assert clipboard MIME representations without returning cell contents."""
    assert "ERROR:" not in document
    if representation == "tsv":
        rows = list(csv.reader(io.StringIO(document), delimiter="\t"))
    elif representation == "html":
        assert "<thead>" in document and "<th " in document
        rows = read_html_text(document)
    else:
        raise AssertionError(f"Unsupported clipboard representation: {representation}")
    assert rows
    header_text = " | ".join(rows[0])
    assert all(label in header_text for label in ("Unités", "Ventes", "€", "Écart"))
    assert header_text.index("Unités") < header_text.index("Ventes")
    _assert_qualification_membership(rows, all_records=all_records)
    return {
        "representation": representation,
        "scope": "all-records" if all_records else "current-table",
        "rows": len(rows),
        "columns": max((len(row) for row in rows), default=0),
        "unicodeVerified": True,
        "filterVerified": True,
        "columnOrderVerified": True,
    }
