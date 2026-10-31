"""Reject layout labels without matching data, geometry and cell presentation."""

import copy
import pytest

from tests.flagship_axis_swap import axis_expected_cells
from tests.flagship_layout_modes import ACCOUNTS, PATHS, assert_layout_rows, layout_dom_matches, layout_expected_cells


def fixture(mode):
    roots = {}
    for (owner, column), value in axis_expected_cells(False).items():
        roots.setdefault(owner, {})[column] = value
    rows = []
    for region in sorted(ACCOUNTS):
        account = ACCOUNTS[region]
        parent = {"_id": region, "_path": region, "region": region, "depth": 0, **roots[region]}
        child = {"_id": account, "_path": region + "|||" + account, "region": region, "account": account, "depth": 1, **roots[region]}
        rows.extend((child, parent) if mode == "tabular" else (parent, child))
    rows.append({"_id": "Grand Total", "_path": "__grand_total__", "depth": 0, **roots["Grand Total"]})
    positions = {row["_path"]: index * 44 for index, row in enumerate(rows)}
    cells = [{"rowid": path, "colid": column, "text": text, "top": positions[path], "bottom": positions[path] + 44,
              "indent": 16 if "|||" in path else 0}
             for (path, column), text in layout_expected_cells(mode).items()]
    return rows, cells


@pytest.mark.parametrize("mode", ["hierarchy", "outline", "tabular"])
def test_layout_oracles_require_exact_keys_values_and_presentation(mode):
    rows, cells = fixture(mode)
    assert_layout_rows(rows, mode)
    assert layout_dom_matches(cells, mode)
    for damage in ("value", "dimension", "duplicate", "order"):
        bad = copy.deepcopy(rows)
        if damage == "value": bad[1]["__RowTotal__ventes_\u20ac_sum"] = 999
        elif damage == "dimension": bad[1]["region"] = "Wrong region"
        elif damage == "duplicate": bad.append(copy.deepcopy(bad[0]))
        else: bad[0], bad[1] = bad[1], bad[0]
        with pytest.raises(AssertionError):
            assert_layout_rows(bad, mode)
    bad = copy.deepcopy(cells)
    bad[0]["text"] = "Wrong visible value"
    assert not layout_dom_matches(bad, mode)
    assert not layout_dom_matches(cells + [dict(cells[0])], mode)
    assert not layout_dom_matches(cells[:-1], mode)


@pytest.mark.parametrize("mode", ["hierarchy", "outline", "tabular"])
def test_layout_oracle_rejects_overlapping_groups_and_wrong_display_mode(mode):
    _, cells = fixture(mode)
    parent = sorted(ACCOUNTS)
    positions = {cell["rowid"]: cell["top"] for cell in cells}
    bad = copy.deepcopy(cells)
    for cell in bad:
        if cell["rowid"] == parent[1]: cell["top"] = positions[parent[0]]
        elif cell["rowid"] == parent[1] + "|||" + ACCOUNTS[parent[1]]:
            cell["top"] = positions[parent[0] + "|||" + ACCOUNTS[parent[0]]]
    assert not layout_dom_matches(bad, mode)
    other = "tabular" if mode != "tabular" else "outline"
    assert not layout_dom_matches(cells, other)


def test_hierarchy_requires_actual_indentation():
    _, cells = fixture("hierarchy")
    for cell in cells: cell["indent"] = 0
    assert not layout_dom_matches(cells, "hierarchy")


@pytest.mark.parametrize("mode", ["hierarchy", "outline", "tabular"])
@pytest.mark.parametrize("damage", ["overlapping-pitch", "interleaved-groups"])
def test_layout_oracle_rejects_overlapping_rows_and_interleaved_groups(mode, damage):
    _, cells = fixture(mode)
    parents = sorted(ACCOUNTS)
    child = lambda region: region + "|||" + ACCOUNTS[region]
    if damage == "overlapping-pitch":
        order = sorted({cell["rowid"] for cell in cells}, key=lambda row: next(cell["top"] for cell in cells if cell["rowid"] == row))
        positions = {row: index * 2 for index, row in enumerate(order)}
    else:
        order = ([child(region) for region in parents] + parents) if mode == "tabular" else (
            parents + [child(region) for region in parents])
        positions = {row: index * 44 for index, row in enumerate([*order, "__grand_total__"])}
    for cell in cells:
        cell["top"] = positions[cell["rowid"]]
        cell["bottom"] = cell["top"] + 44
    assert not layout_dom_matches(cells, mode)
