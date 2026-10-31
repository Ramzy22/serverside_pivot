"""Negative controls for numeric, displayed and correlated axis-swap evidence."""

import copy
import pytest

from tests.flagship_axis_swap import assert_axis_rows, axis_dom_matches, axis_expected_cells, axis_snapshot


def rows_and_cells(transposed):
    rows, cells = {}, []
    for (row, column), value in axis_expected_cells(transposed).items():
        rows.setdefault(row, {"_id": row})[column] = value
        cells.append({"rowid": "__grand_total__" if row == "Grand Total" else row, "colid": column, "text": f"{value:.2f}"})
    return list(rows.values()), cells


@pytest.mark.parametrize("transposed", [False, True])
def test_axis_oracles_require_every_unique_exact_cell(transposed):
    rows, cells = rows_and_cells(transposed)
    assert_axis_rows(rows, transposed=transposed)
    assert axis_dom_matches(cells, transposed=transposed)
    for damage in ("value", "row", "duplicate", "column"):
        damaged = copy.deepcopy(rows)
        if damage == "value":
            key = next(key for key in damaged[0] if key.endswith("_sum"))
            damaged[0][key] = 999
        elif damage == "row":
            damaged[0]["_id"] = "Wrong axis"
        elif damage == "duplicate":
            damaged.append(copy.deepcopy(damaged[0]))
        else:
            damaged[0]["forbidden_sum"] = 1
        with pytest.raises(AssertionError):
            assert_axis_rows(damaged, transposed=transposed)
    assert not axis_dom_matches(cells + [dict(cells[0])], transposed=transposed)
    assert not axis_dom_matches(cells[:-1], transposed=transposed)
    assert not axis_dom_matches(cells, transposed=not transposed)
    cells[0]["text"] = "999.00"
    assert not axis_dom_matches(cells, transposed=transposed)


def test_axis_oracle_accepts_only_one_defined_dash_stats_sidecar():
    rows, _ = rows_and_cells(False)
    sidecar = {"_path": "__color_scale_stats__", "_colorScaleStats": {"byCol": {}}}
    assert_axis_rows([sidecar, *rows], transposed=False)
    for extra in ([sidecar, sidecar], [{"_path": "unknown"}],
                  [{**sidecar, "hidden_sum": 123}], [{**sidecar, "_id": "hidden"}]):
        with pytest.raises(AssertionError):
            assert_axis_rows([*extra, *rows], transposed=False)


@pytest.mark.parametrize("damage", [None, "axes", "identity", "filter", "revision", "old-request", "old-response", "display",
                                  "old-schema", "schema-indices", "schema-layout-ids", "duplicate-schema"])
def test_axis_snapshot_requires_fresh_correlated_response_and_visible_values(monkeypatch, damage):
    from tests import flagship_browser_support as support

    rows, cells = rows_and_cells(True)
    request = dict(phase="request", requestId="new", at=2, rowFields=["period"], colFields=["region", "account"],
                   filters={"scope": {"conditions": [{"value": "Included"}]}})
    columns = list(dict.fromkeys(column for _, column in axis_expected_cells(True)))
    reply = dict(phase="response", requestId="new", at=3, status="data", rows=rows,
                 layoutRevision="revision", colSchema={"layoutRevision": "revision", "columns": [
                     {"id": column, "index": index, "layoutId": f"layout-{index}"} for index, column in enumerate(columns)]})
    if damage == "axes": request["rowFields"] = ["region"]
    elif damage == "identity": reply["requestId"] = "unrelated"
    elif damage == "filter": request["filters"] = {}
    elif damage == "revision": reply["layoutRevision"] = "other"
    elif damage == "old-request": request["at"] = 0
    elif damage == "old-response": reply["at"] = 0
    elif damage == "display": cells[0]["text"] = "999.00"
    elif damage == "old-schema": reply["colSchema"]["columns"][0]["id"] = "Q1_ventes_\u20ac_sum"
    elif damage == "schema-indices": reply["colSchema"]["columns"][0]["index"] = 999
    elif damage == "schema-layout-ids": reply["colSchema"]["columns"][0]["layoutId"] = "layout-1"
    elif damage == "duplicate-schema": reply["colSchema"]["columns"].append(dict(reply["colSchema"]["columns"][0]))

    class Driver:
        def execute_script(self, _): return [request, reply]

    monkeypatch.setattr(support, "_navigation_cells", lambda _: cells)
    monkeypatch.setattr(support, "_navigation_rows", lambda _: [(row["_id"], row["_id"]) for row in rows])
    monkeypatch.setattr(support, "_navigation_columns", lambda _: [cell["colid"] for cell in cells[:9]])
    observed = axis_snapshot(Driver(), 1, transposed=True)
    assert bool(observed) is (damage is None)


@pytest.mark.parametrize("transposed", [False, True])
@pytest.mark.parametrize("count", [1, 2])
def test_axis_oracles_reject_extra_or_missing_measures_in_history_states(transposed, count):
    from tests.flagship_axis_swap import MEASURES
    measures = MEASURES[:count]
    complete_rows, complete_cells = rows_and_cells(transposed)
    expected = {key: value for key, value in axis_expected_cells(transposed).items()
                if any(key[1].endswith(f"_{measure}_sum") for measure in measures)}
    rows = [{key: value for key, value in row.items() if key == "_id" or any(key.endswith(f"_{measure}_sum") for measure in measures)}
            for row in complete_rows]
    cells = [cell for cell in complete_cells if any(cell["colid"].endswith(f"_{measure}_sum") for measure in measures)]
    assert axis_expected_cells(transposed, measures=measures) == expected
    assert_axis_rows(rows, transposed=transposed, measures=measures)
    assert axis_dom_matches(cells, transposed=transposed, measures=measures)
    with pytest.raises(AssertionError):
        assert_axis_rows(complete_rows, transposed=transposed, measures=measures)
    assert not axis_dom_matches(complete_cells, transposed=transposed, measures=measures)
    assert not axis_dom_matches(cells[:-1], transposed=transposed, measures=measures)
