import pytest

from pivot_engine.hierarchy_rows import build_hierarchy_row_window
from pivot_engine.runtime.models import PivotRequestContext, PivotViewState
from pivot_engine.runtime.tree_service import TreeRuntimeService
from pivot_engine.tanstack_adapter import TanStackOperation, TanStackRequest, create_tanstack_adapter
from pivot_engine.types.pivot_spec import Measure, PivotSpec


@pytest.mark.asyncio
async def test_server_side_group_allow_unbalanced_ragged_hierarchy():
    pytest.importorskip("duckdb")
    import pyarrow as pa
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    conn = adapter.controller.backend.con
    conn.create_table(
        "pnl_ragged",
        pa.table(
            {
                "level_1": ["Root", "Root", "Root", "DirectRoot", ""],
                "level_2": ["Desk", None, "Desk", None, "OrphanDesk"],
                "level_3": [None, None, "SubDesk", "", None],
                "hedge": ["Hedge A", "Hedge B", "Hedge C", None, "Hedge D"],
                "ytd_pnl": [100.0, -250.0, 50.0, 30.0, 15.0],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="pnl_ragged",
        columns=[
            {"id": "level_1"},
            {"id": "level_2"},
            {"id": "level_3"},
            {"id": "hedge"},
            {"id": "ytd_pnl_sum", "aggregationField": "ytd_pnl", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[{"id": "ytd_pnl_sum", "desc": True, "absoluteSort": True}],
        grouping=["level_1", "level_2", "level_3", "hedge"],
        aggregations=[],
        totals=True,
        include_subtotals=True,
        group_allow_unbalanced=True,
    )

    res = await adapter.handle_virtual_scroll_request(
        request,
        start_row=0,
        end_row=50,
        expanded_paths=[["__ALL__"]],
        include_grand_total=True,
    )

    rows = res.data
    ids = [r.get("_id") for r in rows]
    assert None not in ids
    assert "" not in ids

    by_id = {r["_id"]: r for r in rows}
    assert by_id["Root"]["depth"] == 0
    assert by_id["Root"]["ytd_pnl_sum"] == pytest.approx(-100.0)
    assert by_id["Root"]["_has_children"] is True

    assert by_id["Hedge B"]["depth"] == 1
    assert by_id["Hedge B"]["_path"] == "Root|||Hedge B"
    assert by_id["Hedge B"]["_has_children"] is False

    assert by_id["Desk"]["depth"] == 1
    assert by_id["Hedge A"]["depth"] == 2
    assert by_id["Hedge A"]["_path"] == "Root|||Desk|||Hedge A"
    assert by_id["SubDesk"]["depth"] == 2
    assert by_id["Hedge C"]["depth"] == 3

    assert by_id["DirectRoot"]["depth"] == 0
    assert by_id["DirectRoot"]["_has_children"] is False

    assert by_id["OrphanDesk"]["depth"] == 0
    assert by_id["Hedge D"]["depth"] == 1
    assert by_id["Grand Total"]["ytd_pnl_sum"] == pytest.approx(-55.0)

    # Check sorting across different source depths under Root (|Hedge B|=250 before |Desk|=150)
    root_children = [r["_id"] for r in rows if r["depth"] == 1 and str(r["_path"]).startswith("Root|||")]
    assert root_children == ["Hedge B", "Desk"]

    # Collapsed view only returns roots + Grand Total while preserving accurate _has_children
    res_collapsed = await adapter.handle_virtual_scroll_request(
        request,
        start_row=0,
        end_row=50,
        expanded_paths=[],
        include_grand_total=True,
    )
    collapsed_by_id = {r["_id"]: r for r in res_collapsed.data}
    assert set(collapsed_by_id.keys()) == {"Root", "DirectRoot", "OrphanDesk", "Grand Total"}
    assert collapsed_by_id["Root"]["_has_children"] is True
    assert collapsed_by_id["DirectRoot"]["_has_children"] is False
    assert collapsed_by_id["OrphanDesk"]["_has_children"] is True


def test_tree_service_sorting_absolute_sort_and_show_grand_total():
    service = TreeRuntimeService()
    source_rows = [
        {"path": "Root", "ytd_pnl": -100.0},
        {"path": "Root/Desk A", "ytd_pnl": 40.0},
        {"path": "Root/Desk B", "ytd_pnl": -140.0},
        {"path": "Alpha", "ytd_pnl": 50.0},
    ]
    state = PivotViewState(
        view_mode="tree",
        tree_config={
            "sourceType": "path",
            "pathField": "path",
            "pathSeparator": "/",
            "valueFields": ["ytd_pnl"],
            "showGrandTotal": True,
        },
        sorting=[{"id": "ytd_pnl", "desc": True, "absoluteSort": True}],
        expanded={"Root": True},
        show_grand_total=True,
    )
    tree_config = service._normalize_tree_config(state)
    detail_config = service._normalize_detail_config(state)
    nodes = service._materialize_nodes(source_rows, tree_config)
    visible_rows = service._build_visible_rows(
        nodes,
        [["Root"]],
        state,
        tree_config,
        detail_config,
    )
    assert [r["_id"] for r in visible_rows] == ["Root", "Desk B", "Desk A", "Alpha", "Grand Total"]
    gt = visible_rows[-1]
    assert gt["_isTotal"] is True
    assert gt["_path"] == "__grand_total__"
    assert gt["ytd_pnl"] == pytest.approx(-50.0)


def test_hierarchy_rows_unbalanced_values_sentinel():
    spec = PivotSpec(
        table="t",
        rows=["l1", "l2", "hedge"],
        measures=[Measure(field="val", agg="sum", alias="val_sum")],
        group_allow_unbalanced=True,
        unbalanced_values=["-", "N/A"],
    )
    hierarchy_result = {
        "": [{"l1": "Root", "val_sum": 30}],
        "Root": [
            {"l1": "Root", "l2": "-", "hedge": "H1", "val_sum": 10},
            {"l1": "Root", "l2": "N/A", "hedge": "H2", "val_sum": 20},
        ],
    }
    window = build_hierarchy_row_window(spec, hierarchy_result, [["Root"]])
    assert [r["_id"] for r in window["rows"]] == ["Root", "H1", "H2"]
    assert [r["depth"] for r in window["rows"]] == [0, 1, 1]
    assert [r["_path"] for r in window["rows"]] == ["Root", "Root|||H1", "Root|||H2"]


def test_export_service_boost_compact_scale_neg_zero_and_theme_tokens():
    from pivot_engine.runtime.export_service import RuntimeExportService
    from pivot_engine.runtime.models import PivotViewState

    state = PivotViewState(
        row_fields=["book"],
        val_configs=[
            {"field": "pnl", "agg": "sum", "format": "compact", "valueScale": 1000, "unitSuffix": "k"}
        ],
    )
    style_profile = {
        "theme": {
            "negativeText": "#b3362c",
            "positiveText": "#0f6e78",
            "groupBg": ["#e9f1f2", "#f3f7f8"],
            "groupFontWeight": [700, 600],
            "leafBg": "#ffffff",
            "leafFontWeight": 400,
            "totalFontWeight": 800,
            "treeIndent": 18,
            "rowBorder": "#eef1f3",
            "headerFontWeight": 700,
            "headerFontSize": 11,
            "headerBorderBottom": "#d5dde2",
        },
        "decimalPlaces": 1,
    }
    row_neg_zero = {"_id": "Tiny", "depth": 1, "pnl_sum": -4.0}
    formatted = RuntimeExportService._formatted_export_value("pnl_sum", -4.0, row_neg_zero, state, style_profile)
    assert formatted == "0.0k"

    neg_zero_style = RuntimeExportService._export_cell_style("pnl_sum", -4.0, row_neg_zero, "PnL", style_profile, state)
    assert neg_zero_style["color"] != "#b3362c"
    assert neg_zero_style["background"] == "#ffffff"
    assert neg_zero_style["fontWeight"] == 400

    group_row = {"_id": "Root", "depth": 0, "_has_children": True, "pnl_sum": -2_500_000.0}
    group_formatted = RuntimeExportService._formatted_export_value("pnl_sum", -2_500_000.0, group_row, state, style_profile)
    assert group_formatted == "-2.5Kk"
    group_style = RuntimeExportService._export_cell_style("pnl_sum", -2_500_000.0, group_row, "PnL", style_profile, state)
    assert group_style["background"] == "#e9f1f2"
    assert group_style["fontWeight"] == 700
    assert group_style["color"] == "#b3362c"

    hier_style = RuntimeExportService._export_cell_style("_id", "Tiny", row_neg_zero, "Hierarchy", style_profile, state)
    assert hier_style["padding"] == "4px 10px 4px 28px"

    hdr_style = RuntimeExportService._export_header_style("pnl_sum", "PnL", style_profile, state)
    assert hdr_style["fontSize"] == "11px"
    assert hdr_style["border"] == "1px solid #d5dde2"


def test_xlsx_export_passes_state_for_scaled_negative_zero_color(tmp_path):
    import os
    import xml.etree.ElementTree as ET
    import zipfile
    from pivot_engine.runtime.export_service import RuntimeExportService
    from pivot_engine.tanstack_adapter import TanStackResponse

    state = PivotViewState(
        row_fields=["book"],
        val_configs=[
            {"field": "pnl", "agg": "sum", "format": "compact", "valueScale": 1000}
        ],
    )
    response = TanStackResponse(
        data=[{"_id": "Tiny", "depth": 1, "pnl_sum": -4.0}],
        columns=[{"id": "hierarchy", "header": "Book"}, {"id": "pnl_sum", "header": "PnL"}],
        total_rows=1,
    )
    payload = RuntimeExportService().build_export_payload(
        response,
        state,
        {
            "format": "xlsx",
            "colIds": ["hierarchy", "pnl_sum"],
            "style": {
                "decimalPlaces": 1,
                "theme": {"negativeText": "#b3362c", "textSec": "#374151"},
            },
        },
    )
    content_path = payload["contentPath"]
    try:
        with zipfile.ZipFile(content_path) as workbook:
            styles_text = workbook.read("xl/styles.xml").decode("utf-8")
            sheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
        assert "FFB3362C" not in styles_text
        assert "FF374151" in styles_text
        ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        assert sheet.find(".//x:c[@r='B2']/x:v", ns).text == "-4.0"
    finally:
        if os.path.exists(content_path):
            os.remove(content_path)


def test_export_style_helpers_tolerate_missing_profile_and_state():
    from pivot_engine.runtime.export_service import RuntimeExportService

    row = {"_id": "North", "depth": 0, "sales_sum": 100.0}
    assert RuntimeExportService._matching_value_config_for_export("sales_sum", None, None) is None
    cell_style = RuntimeExportService._export_cell_style("sales_sum", 100.0, row, "Sales", None, None)
    header_style = RuntimeExportService._export_header_style("sales_sum", "Sales", None, None)
    assert cell_style["textAlign"] == "right"
    assert header_style["textAlign"] == "center"
