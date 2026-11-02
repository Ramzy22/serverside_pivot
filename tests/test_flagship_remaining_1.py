"""Mutation checks for presentation evidence oracles and real fixture queries."""
import copy
import json
from pathlib import Path
import pytest

from tests.flagship_remaining_1 import (
    REQUIREMENTS, SCALE_IDS, DATA_BAR_IDS, SALES, assert_data_bars, assert_decimal_cells,
    assert_density, assert_number_column, assert_scale, assert_zoom, starting_state,
)


def test_native_theme_picker_edits_blue_with_trusted_input_on_local_fixture():
    """Opt-in, isolated picker regression; never contacts application hosts."""
    import os
    from urllib.parse import quote
    if os.environ.get("PIVOT_RUN_NATIVE_COLOR_FIXTURE") != "1":
        pytest.skip("Enable the isolated native color fixture explicitly")
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.flagship_remaining_1 import _choose_native_theme_override

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1680,1200")
    driver = webdriver.Chrome(options=options)
    try:
        for top in (8, 400, 800, 1100):
            driver.get("data:text/html," + quote(f"""
              <div style="height:1500px"><input id="color" type="color"
                value="#0a0c10" style="position:absolute;top:{top}px"></div>
              <script>
                window.inputs=[];
                document.querySelector('input').addEventListener('input',event=>
                  inputs.push({{value:event.target.value,trusted:event.isTrusted}}));
              </script>
            """))
            field = driver.find_element("id", "color")
            _choose_native_theme_override(driver, field, WebDriverWait(driver, 5))
            assert field.get_attribute("value") == "#0a0c0f", f"Picker position {top}"
            events = driver.execute_script("return window.inputs")
            assert events and events[-1] == {"value": "#0a0c0f", "trusted": True}
    finally:
        driver.quit()


def test_actual_frontend_format_scale_and_data_bar_runtime():
    import shutil
    import subprocess
    node = shutil.which("node")
    assert node, "Node is required to test actual presentation runtime"
    result = subprocess.run([node, str(Path(__file__).with_name("frontend_flagship_presentation.cjs"))], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "flagship presentation runtime passed" in result.stdout


def test_all_presentation_contract_cases_have_executable_host_mappings():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements
    plan = _product_browser_plan(load_requirements(Path(__file__).with_name("flagship_requirements.json")))
    for requirement in REQUIREMENTS:
        for host in ("dash", "react", "streamlit"):
            assert plan[(requirement, host, "browser." + requirement)] == (
                f"tests/test_flagship_workflows.py::test_flagship_presentation[{requirement}-{host}]")


def test_number_column_checks_order_deduplication_measure_invariance_and_selection():
    before = [{"rowid": row, "colid": "sales", "text": value, "top": index * 44, "selected": index == 0}
              for index, (row, value) in enumerate((("Group", "150"), ("Other", "50"), ("__grand_total__", "200")))]
    shown = before + [{"rowid": cell["rowid"], "colid": "__row_number__", "text": str(index + 1), "top": cell["top"], "selected": False}
                      for index, cell in enumerate(before)]
    assert_number_column(before, shown, before)
    for damage in ("zero-based", "wrong-order", "duplicate", "data", "selection"):
        bad = copy.deepcopy(shown)
        if damage == "zero-based": bad[-3]["text"] = "0"
        elif damage == "wrong-order": bad[-3]["text"], bad[-2]["text"] = "2", "1"
        elif damage == "duplicate": bad.append(dict(bad[-1]))
        elif damage == "data": bad[0]["text"] = "999"
        else: bad[0]["selected"] = False
        with pytest.raises(AssertionError):
            assert_number_column(before, bad, before)


@pytest.mark.parametrize("height", [36, 44, 56])
def test_density_asserts_every_actual_row_and_preserves_ids_values(height):
    before = [{"rowid": row, "colid": "sales", "text": "100", "rowHeight": 44, "height": 44} for row in ("a", "b")]
    after = [{**cell, "rowHeight": height, "height": height} for cell in before]
    assert_density(before, after, height)
    for damage in ("row", "cell", "value", "id", "missing"):
        bad = copy.deepcopy(after)
        if damage == "row": bad[1]["rowHeight"] += 2
        elif damage == "cell": bad[1]["height"] += 2
        elif damage == "value": bad[1]["text"] = "200"
        elif damage == "id": bad[1]["rowid"] = "c"
        else: bad.pop()
        with pytest.raises(AssertionError): assert_density(before, bad, height)


def test_decimal_oracle_checks_negative_ties_selected_scope_and_controls():
    raw = {("r", "sales"): 100.25, ("r", "second_sales"): 20, ("r", "edge"): -100.25, ("r", "control"): 2}
    cells = [{"rowid": "r", "colid": column, "text": text} for column, text in
             (("sales", "100.3"), ("second_sales", "20.0"), ("edge", "-100.3"), ("control", "2.00"))]
    columns = ("sales", "second_sales", "edge")
    assert_decimal_cells(cells, raw, columns, places=1)
    for index, text in ((0, "100.2"), (1, "20"), (2, "-100.2"), (3, "2.0")):
        bad = copy.deepcopy(cells); bad[index]["text"] = text
        with pytest.raises(AssertionError): assert_decimal_cells(bad, raw, columns, places=1)
    with pytest.raises(AssertionError): assert_decimal_cells(cells[:-1], raw, columns, places=1)


def scale_cells():
    result = []
    for row, value, color in zip(SCALE_IDS, (0, 100, 200), ("rgba(248, 105, 107, 0.82)", "rgba(248, 105, 107, 0.06)", "rgba(99, 190, 123, 0.82)")):
        result.append({"rowid": row, "colid": "scale_value_sum", "text": str(value), "background": color, "color": "rgb(0, 0, 0)",
                       "barPercent": value / 2, "barWidth": value / 2, "usableWidth": 100, "barBehindText": True})
        result.append({"rowid": row, "colid": "scale_control_sum", "text": "10", "background": "rgb(255, 255, 255)", "color": "rgb(0, 0, 0)", "barWidth": 0})
    return result


@pytest.mark.parametrize("damage", ["control", "stop", "value", "missing"])
def test_scale_requires_exact_colors_and_unselected_control_preservation(damage):
    before = scale_cells(); after = copy.deepcopy(before)
    assert_scale(before, after)
    if damage == "control": after[1]["background"] = "red"
    elif damage == "stop": after[2]["background"] = "rgba(248, 105, 107, 0.07)"
    elif damage == "value": after[0]["text"] = "1"
    else: after.pop(0)
    with pytest.raises((AssertionError, StopIteration)): assert_scale(before, after)


def data_bar_cells():
    result = scale_cells()
    for row, value, percent in zip(DATA_BAR_IDS[3:], (300, -100), (100, 50)):
        result.append({**result[0], "rowid": row, "text": str(value), "barWidth": percent, "barPercent": percent})
        result.append({**result[1], "rowid": row})
    for cell in result:
        if cell["colid"] == "scale_value_sum":
            cell["barColor"] = "rgba(220, 38, 38, 0.12)" if cell["text"].startswith("-") else "rgba(37, 99, 235, 0.13)"
    return result


@pytest.mark.parametrize("damage", ["geometry", "zero", "hidden-text", "control", "upper-clamp", "negative", "negative-tint", "control-style", "missing-outlier"])
def test_data_bars_use_measured_width_including_zero_and_unselected_columns(damage):
    before = data_bar_cells(); after = copy.deepcopy(before)
    assert_data_bars(before, after)
    if damage == "geometry": after[2]["barWidth"] = 53
    elif damage == "zero": after[0]["barWidth"] = 2
    elif damage == "hidden-text": after[2]["barBehindText"] = False
    elif damage == "control": after[1]["barWidth"] = 10
    elif damage == "upper-clamp": after[6]["barWidth"] = 150
    elif damage == "negative": after[8]["barWidth"] = 0
    elif damage == "negative-tint": after[8]["barColor"] = "rgba(37, 99, 235, 0.13)"
    elif damage == "control-style": after[1]["background"] = "red"
    else: after.pop(8)
    with pytest.raises(AssertionError): assert_data_bars(before, after)


@pytest.mark.parametrize("damage", [None, "zoom", "width", "row", "scroll", "window", "origin-before", "origin-zoomed"])
def test_zoom_round_trip_requires_real_geometry_and_restored_viewport(damage):
    before = {"zoom": 1.15, "cellWidth": 115, "rowHeight": 50.6, "scrollLeft": 30, "scrollTop": 8}
    zoomed = {"zoom": 1.25, "cellWidth": 125, "rowHeight": 55, "scrollLeft": 30, "scrollTop": 8, "windowX": 0, "windowY": 100}
    restored = dict(zoomed)
    if damage == "zoom": restored["zoom"] = 1.15
    elif damage == "width": zoomed["cellWidth"] = 115
    elif damage == "row": restored["rowHeight"] = 44
    elif damage == "scroll": restored["scrollLeft"] = 0
    elif damage == "window": restored["windowY"] = 0
    elif damage == "origin-before": before.update(scrollLeft=0, scrollTop=0)
    elif damage == "origin-zoomed":
        zoomed.update(scrollLeft=0, scrollTop=0)
        restored.update(scrollLeft=0, scrollTop=0)
    if damage is None: assert_zoom(before, zoomed, restored)
    else:
        with pytest.raises(AssertionError): assert_zoom(before, zoomed, restored)


def test_presentation_fixture_preserves_five_sales_rows_and_adds_read_only_edges(monkeypatch):
    from examples import flagship_demo_data as demo
    rows = json.loads(Path(__file__).with_name("data").joinpath("flagship_parity_rows.json").read_text(encoding="utf-8"))["rows"]
    assert len(rows) == 5 and sum(row[SALES] for row in rows if row["scope"] == "Included") == 200
    assert [row["format_edge"] for row in rows] == [-100.25, 0, 0, 0, 0]
    assert [row["scale_value"] for row in rows] == [0, 100, 200, 300, -100]
    monkeypatch.setenv("PIVOT_FLAGSHIP_PARITY_DATA", "1")
    assert {"format_edge", "scale_value", "scale_control"} <= set(demo.workspace_config("react")["availableFieldList"])


def test_presentation_seed_is_executable_against_real_runtime_values():
    import pyarrow as pa
    from pivot_engine import create_tanstack_adapter
    from pivot_engine.runtime import PivotRuntimeService, SessionRequestGate, PivotRequestContext, PivotViewState
    rows = json.loads(Path(__file__).with_name("data").joinpath("flagship_parity_rows.json").read_text(encoding="utf-8"))["rows"]
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("flagship_shared", pa.Table.from_pylist(rows))
    service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())
    for index, requirement in enumerate(("number-format.decimals", "color-scales.scale", "color-scales.data-bars"), 1):
        state = starting_state(requirement)
        # Execute the exact public state without the generic helper's implicit
        # Included filter, which would silently drop the negative source row.
        reply = service.process(PivotViewState(
            row_fields=state["rowFields"], col_fields=state["colFields"],
            val_configs=state["valConfigs"], filters=state["filters"], expanded={}, sorting=[],
            show_row_totals=state["showRowTotals"], show_col_totals=state["showColTotals"]),
            PivotRequestContext(table="flagship_shared", trigger_prop="pivot.viewport",
                                session_id="presentation-fixture", client_instance="presentation-client",
                                state_epoch=index, window_seq=index, viewport_active=True,
                                start_row=0, end_row=100, needs_col_schema=True))
        assert reply.status == "data", reply.message
        data = {row["_path"]: row for row in reply.data if "_id" in row}
        if requirement == "number-format.decimals":
            assert data["__grand_total__"][f"__RowTotal__{SALES}_sum"] == 200
            assert data["\u00c9clair"]["Q1_format_edge_sum"] == -100.25
        elif requirement == "color-scales.scale":
            assert set(data) == set(SCALE_IDS)
            assert [data[row]["scale_value_sum"] for row in SCALE_IDS] == [0, 100, 200]
            assert [data[row]["scale_control_sum"] for row in SCALE_IDS] == [10, 20, 30]
        else:
            assert not state["filters"], "Data-bar bounds must include the real Excluded outlier"
            assert set(data) == set(DATA_BAR_IDS)
            assert [data[row]["scale_value_sum"] for row in DATA_BAR_IDS] == [0, 100, 200, 300, -100]
            assert [data[row]["scale_control_sum"] for row in DATA_BAR_IDS] == [10, 20, 30, 40, 50]
