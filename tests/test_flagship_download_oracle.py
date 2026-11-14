"""Negative controls for exact browser-download qualification."""
import csv
from pathlib import Path

import pytest
from openpyxl import Workbook

from tests.export_qualification_fixture import assert_exact_flagship_download


HEADERS = ["Region > Account"] + [f"{period} {measure}"
    for period in ("Q1", "Q2", "Total") for measure in ("Unit\u00e9s", "Ventes (\u20ac)", "\u00c9cart")]
ROWS = [
    ["M\u00fcnchen", 3, 49.75, 4.25, 4, 30, -1, 7, 79.75, 3.25],
    ["\u0627\u0644\u0642\u0627\u0647\u0631\u0629", 3, 49.75, 4.25, 4, 30, -1, 7, 79.75, 3.25],
    ["\u00c9clair", 2, 100.25, -12.5, 1, 20, 2.5, 3, 120.25, -10],
    ["\u6771\u4eac", 2, 100.25, -12.5, 1, 20, 2.5, 3, 120.25, -10],
    ["Grand Total", 5, 150, -8.25, 5, 50, 1.5, 10, 200, -6.75],
]


def write_document(path, kind, *, rows=None, bad_merge=False, strings=False, bad_headers=False):
    rows = [list(row) for row in (ROWS if rows is None else rows)]
    headers = list(HEADERS)
    if bad_headers:
        headers[1], headers[2] = headers[2], headers[1]
    if kind == "csv":
        headers = [headers[0], *[label.replace(" (\u20ac)", "") for label in headers[1:7]],
                   *headers[7:]]
        with path.open("w", encoding="utf-8-sig", newline="") as stream:
            csv.writer(stream).writerows([headers, *rows])
    else:
        workbook = Workbook()
        sheet = workbook.active
        measures = ["Unit\u00e9s", "Ventes (\u20ac)", "\u00c9cart"]
        total_measures = [f"Total {measure}" for measure in measures]
        sheet.append([headers[0], "Q1", None, None, "Q2", None, None, *total_measures])
        leaf_headers = measures * 2
        if bad_headers:
            leaf_headers[0], leaf_headers[1] = leaf_headers[1], leaf_headers[0]
        sheet.append([None, *leaf_headers, None, None, None])
        sheet.merge_cells("A1:A2")
        sheet.merge_cells("B1:E1" if bad_merge else "B1:D1")
        sheet.merge_cells("E1:G1")
        for span in ("H1:H2", "I1:I2", "J1:J2"):
            sheet.merge_cells(span)
        for row in rows:
            sheet.append([row[0], *[str(value) if strings else value for value in row[1:]]])
        workbook.save(path)
        workbook.close()


@pytest.mark.parametrize("kind", ["csv", "xlsx"])
def test_literal_complete_download_passes(tmp_path, kind):
    path = tmp_path / f"export.{kind}"
    write_document(path, kind)
    result = assert_exact_flagship_download(path, kind)
    assert result["rows"] == 5 and result["columns"] == 10
    assert result["totalsVerified"] and result["unicodeVerified"]
    assert result["numericCellCount"] == 45


@pytest.mark.parametrize("kind", ["csv", "xlsx"])
@pytest.mark.parametrize("damage", ["wrong-grand-total", "missing-child", "duplicate-parent", "wrong-unicode", "wrong-headers", "swapped-period-values"])
def test_download_oracle_rejects_wrong_cells_rows_and_headers(tmp_path, kind, damage):
    rows = [list(row) for row in ROWS]
    if damage == "wrong-grand-total":
        rows[-1][8] = 400
    elif damage == "missing-child":
        rows.pop(1)
    elif damage == "duplicate-parent":
        rows[1] = list(rows[0])
    elif damage == "wrong-unicode":
        rows[3][0] = "Tokyo"
    elif damage == "swapped-period-values":
        for row in rows:
            row[2], row[5] = row[5], row[2]
    path = tmp_path / f"bad.{kind}"
    write_document(path, kind, rows=rows, bad_headers=damage == "wrong-headers")
    with pytest.raises(AssertionError):
        assert_exact_flagship_download(path, kind)


@pytest.mark.parametrize("damage", ["bad-merge", "string-numbers"])
def test_xlsx_oracle_rejects_wrong_merge_and_numeric_types(tmp_path, damage):
    path = tmp_path / "bad.xlsx"
    write_document(path, "xlsx", bad_merge=damage == "bad-merge", strings=damage == "string-numbers")
    with pytest.raises(AssertionError):
        assert_exact_flagship_download(path, "xlsx")


def test_csv_oracle_rejects_swapped_period_headers(tmp_path):
    path = tmp_path / "bad-period.csv"
    write_document(path, "csv")
    text = path.read_text(encoding="utf-8-sig")
    path.write_text(text.replace("Q1", "QX").replace("Q2", "Q1").replace("QX", "Q2"),
                    encoding="utf-8-sig")
    with pytest.raises(AssertionError):
        assert_exact_flagship_download(path, "csv")


@pytest.mark.parametrize("damage", ["hierarchy", "leaf-period", "total-label", "missing-merge"])
def test_xlsx_oracle_rejects_corrupted_header_cells(tmp_path, damage):
    from openpyxl import load_workbook

    path = tmp_path / "bad-header.xlsx"
    write_document(path, "xlsx")
    workbook = load_workbook(path)
    sheet = workbook.active
    if damage == "hierarchy":
        sheet["A1"] = "wrong Region > Account"
    elif damage == "leaf-period":
        sheet["B2"] = "Q2 Unit\u00e9s (Sum) garbage"
    elif damage == "total-label":
        sheet["H1"] = "Wrong Unit\u00e9s (Sum)"
    else:
        sheet.unmerge_cells("H1:H2")
    workbook.save(path)
    workbook.close()
    with pytest.raises(AssertionError):
        assert_exact_flagship_download(path, "xlsx")


def delivery_trace():
    return [
        dict(phase="request", at=2, requestId="start", action="start", format="csv"),
        dict(phase="response", at=3, requestId="start", status="export_job", jobId="job", jobPhase="queued", payloadUrl=None),
        dict(phase="request", at=4, requestId="poll", action="status", jobId="job"),
        dict(phase="response", at=5, requestId="poll", status="export_job", jobId="job", jobPhase="completed", payloadUrl="http://test/payload/token"),
        dict(phase="delivery", at=6, url="http://test/payload/token", status=200, sha256="digest", bytes=100),
        dict(phase="blob", at=7, url="blob:test", sha256="digest", bytes=100),
        dict(phase="download", at=8, url="blob:test", filename="pivot.csv"),
    ]


@pytest.mark.parametrize("damage", [None, "missing-response", "wrong-request", "wrong-job", "error-response",
                                  "wrong-url", "wrong-digest", "wrong-blob", "duplicate-download"])
def test_exact_export_delivery_requires_one_correlated_job_and_file(damage):
    from tests.flagship_browser_support import assert_exact_export_delivery

    trace = delivery_trace()
    if damage == "missing-response":
        trace.pop(1)
    elif damage == "wrong-request":
        trace[3]["requestId"] = "unrelated"
    elif damage == "wrong-job":
        trace[3]["jobId"] = "other"
    elif damage == "error-response":
        trace[3]["status"] = "error"
    elif damage == "wrong-url":
        trace[4]["url"] = "http://test/payload/other"
    elif damage == "wrong-digest":
        trace[4]["sha256"] = "other"
    elif damage == "wrong-blob":
        trace[5]["sha256"] = "other"
    elif damage == "duplicate-download":
        trace.append(dict(trace[-1]))
    if damage:
        with pytest.raises(AssertionError):
            assert_exact_export_delivery(trace, "csv", 1, "digest", "pivot.csv", 100)
    else:
        assert assert_exact_export_delivery(trace, "csv", 1, "digest", "pivot.csv", 100) == "job"


@pytest.mark.parametrize("extra", [None, "pivot (1).csv", "pivot.csv.crdownload"])
def test_exact_download_rejects_duplicate_and_partial_files(tmp_path, extra):
    from tests.flagship_browser_support import assert_single_exact_download

    selected = tmp_path / "pivot.csv"
    selected.write_text("content")
    if extra:
        (tmp_path / extra).write_text("extra")
        with pytest.raises(AssertionError):
            assert_single_exact_download(tmp_path, selected)
    else:
        assert_single_exact_download(tmp_path, selected)
