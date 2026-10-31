"""Literal clipboard rectangle negative controls; no generated output oracle."""
import pytest
from tests.flagship_clipboard_values import UNICODE, assert_clipboard_values


@pytest.mark.parametrize("damage", [None, "unicode", "normalization", "null", "undefined", "trailing", "interior", "extra-row", "extra-column", "header"])
def test_clipboard_values_require_codepoints_and_exact_empty_fields(damage):
    text = UNICODE + "\t\tq2-munchen\t"
    if damage == "unicode": text = text.replace("\U0001f680", "\ufffd")
    elif damage == "normalization": text = text.replace("\u00c9", "E\u0301")
    elif damage == "null": text = text.replace("\t\t", "\tnull\t")
    elif damage == "undefined": text = text.replace("\t\t", "\tundefined\t")
    elif damage == "trailing": text = text[:-1]
    elif damage == "interior": text = text.replace("\t\t", "\t")
    elif damage == "extra-row": text += "\nextra\t\trow\t"
    elif damage == "extra-column": text += "\textra"
    elif damage == "header": text = "Unicode\tNull\tID\tEmpty\n" + text
    if damage is None:
        assert assert_clipboard_values(text) == [[UNICODE, "", "q2-munchen", ""]]
    else:
        with pytest.raises(AssertionError): assert_clipboard_values(text)


@pytest.mark.parametrize("damage", [None, "request-field", "aggregation", "filter", "identity", "stale-response", "schema", "indices", "revision", "raw-null", "raw-empty", "raw-unicode", "duplicate-row", "display"])
def test_source_snapshot_requires_actual_source_and_current_model(monkeypatch, damage):
    from tests import flagship_clipboard_values as clipboard
    from tests import flagship_browser_support as support
    rows = [{"_id": row_id, **dict(zip(clipboard.COLUMNS, values))} for row_id, values in clipboard.EXPECTED.items()]
    request = {"phase": "request", "requestId": "current", "at": 2, "rowFields": ["rowId"], "colFields": [],
               "valueAggregations": [{"field": field, "agg": "first"} for field in clipboard.FIELDS],
               "filters": {"scope": {"conditions": [{"value": "Included"}]}}}
    reply = {"phase": "response", "requestId": "current", "at": 3, "status": "data", "rows": rows,
             "colSchema": {"columns": [{"id": column, "index": index} for index, column in enumerate(clipboard.COLUMNS)]}}
    cells = [{"rowid": "q2-munchen", "colid": column, "text": value} for column, value in zip(clipboard.COLUMNS, [clipboard.UNICODE, "", "q2-munchen", ""])]
    if damage == "request-field": request["rowFields"] = ["region"]
    elif damage == "aggregation": request["valueAggregations"][0]["agg"] = "sum"
    elif damage == "filter": request["filters"] = {}
    elif damage == "identity": reply["requestId"] = "unrelated"
    elif damage == "stale-response": reply["at"] = 1
    elif damage == "schema": reply["colSchema"]["columns"][0]["id"] = "wrong"
    elif damage == "indices": reply["colSchema"]["columns"][0]["index"] = 99
    elif damage == "revision": reply["layoutRevision"] = "unrelated"
    elif damage == "raw-null": rows[-1]["copyNull_first"] = ""
    elif damage == "raw-empty": rows[-1]["copyEmpty_first"] = None
    elif damage == "raw-unicode": rows[-1]["copyUnicode_first"] = "wrong"
    elif damage == "duplicate-row": rows.append(dict(rows[-1]))
    elif damage == "display": cells[0]["text"] = "wrong"
    class Driver:
        def execute_script(self, _): return [request, reply]
    monkeypatch.setattr(support, "_navigation_cells", lambda _: cells)
    assert bool(clipboard.clipboard_source_snapshot(Driver())) is (damage is None)
