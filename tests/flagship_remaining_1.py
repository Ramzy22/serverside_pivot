"""Installed-artifact presentation journeys. Configuration seeding is setup only.

All actions under qualification use Selenium input on the actual public controls;
DOM scripts only read geometry, styles and focus. Transport probes observe real
requests and replies and never replace handlers, responses or clipboard APIs.
"""

from decimal import Decimal, ROUND_HALF_UP
import json
import os
from pathlib import Path
import traceback

from tests import flagship_browser_support as support
from tests.flagship_axis_swap import AXIS_PROBE, assert_axis_rows

REQUIREMENTS = (
    "row-numbers.toggle", "density.cycle", "number-format.decimals",
    "cell-format.apply-close", "color-scales.scale", "color-scales.data-bars",
    "themes.select-override", "zoom-immersive.round-trip",
)
SALES = "ventes_\u20ac"
UNITS = "unit\u00e9s"
ECLAIR = "\u00c9clair"
SCALE_IDS = ("q1-eclair", "q2-eclair", "q1-munchen")


def starting_state(requirement):
    """Public v1 saved-view setup, consumed by the real restore implementation."""
    state = {
        "rowFields": ["region", "account"], "colFields": ["period"],
        "valConfigs": [{"field": field, "agg": "sum"} for field in (SALES, UNITS, "\u00e9cart")],
        "filters": {"scope": {"operator": "AND", "conditions": [{"type": "eq", "value": "Included"}]}},
        "expanded": {}, "sorting": [], "columnOrder": ["hierarchy", f"Q1_{SALES}_sum", f"Q2_{SALES}_sum"], "columnPinning": {"left": [], "right": []},
        "showRowTotals": True, "showColTotals": True, "showSubtotals": True,
        "showRowNumbers": False, "spacingMode": 1, "zoomLevel": 100,
        "decimalPlaces": 2, "defaultValueFormat": "", "numberGroupSeparator": "none",
        "columnDecimalOverrides": {}, "columnFormatOverrides": {}, "columnGroupSeparatorOverrides": {},
        "cellFormatRules": {}, "colorScaleMode": "off", "colorPalette": "redGreen", "dataBarsColumns": [],
        "themeName": "light", "themeOverrides": {}, "layoutMode": "hierarchy", "sidebarOpen": False,
    }
    if requirement == "number-format.decimals":
        state["valConfigs"] = [{"field": field, "agg": "sum"} for field in (SALES, "format_edge", UNITS)]
        state["columnOrder"].append("Q1_format_edge_sum")
    if requirement.startswith("color-scales."):
        state.update(rowFields=["rowId"], colFields=[], showRowTotals=False, showColTotals=False, showSubtotals=False,
                     valConfigs=[{"field": "scale_value", "agg": "sum", "label": "Sales"},
                                 {"field": "scale_control", "agg": "sum", "label": "Control"}])
        state["filters"]["rowId"] = {"operator": "OR", "conditions": [{"type": "eq", "value": row} for row in SCALE_IDS]}
        state["decimalPlaces"] = 0
    if requirement == "zoom-immersive.round-trip":
        state["zoomLevel"] = 115  # Native toolbar advances by 10 percentage points.
    return state


def cells(driver):
    return driver.execute_script("""
      const result=[]; const visit=root=>{
        for(const cell of root.querySelectorAll('[role="gridcell"][data-rowid][data-colid]')) {
          if(!cell.getClientRects().length) continue;
          const box=cell.getBoundingClientRect(), style=getComputedStyle(cell);
          const row=cell.closest('[role="row"]');
          const bar=[...cell.children].find(child=>child.style.position==='absolute' && child.style.zIndex==='0' && child.style.width.endsWith('%'));
          result.push({rowid:cell.dataset.rowid,colid:cell.dataset.colid,text:cell.innerText.trim(),
            top:box.top,left:box.left,width:box.width,height:box.height,
            rowHeight:row?.getBoundingClientRect().height ?? box.height,
            background:style.backgroundColor,color:style.color,selected:cell.getAttribute('aria-selected')==='true',
            usableWidth:cell.clientWidth,barWidth:bar?.getBoundingClientRect().width ?? 0,
            barPercent:bar ? parseFloat(bar.style.width) : null,
            barColor:bar ? getComputedStyle(bar).backgroundColor : null,
            barBehindText:bar ? getComputedStyle(bar).pointerEvents==='none' && getComputedStyle(bar).zIndex==='0' : null});
        }
        for(const el of root.querySelectorAll('*')) if(el.shadowRoot) visit(el.shadowRoot);
      }; visit(document); return result;
    """)


def logical_values(snapshot):
    result = {(cell["rowid"], cell["colid"]): cell["text"] for cell in snapshot if cell["colid"] != "__row_number__"}
    assert len(result) == sum(cell["colid"] != "__row_number__" for cell in snapshot), "Duplicate logical cells"
    return result


def assert_number_column(before, shown, restored):
    assert logical_values(before) == logical_values(shown) == logical_values(restored), "Row numbers changed measure/dimension values"
    assert not any(cell["colid"] == "__row_number__" for cell in before + restored)
    numbered = [cell for cell in shown if cell["colid"] == "__row_number__"]
    row_ids = sorted({cell["rowid"] for cell in shown}, key=lambda row: min(cell["top"] for cell in shown if cell["rowid"] == row))
    assert len(numbered) == len(row_ids) and len({cell["rowid"] for cell in numbered}) == len(row_ids)
    assert {cell["rowid"]: cell["text"] for cell in numbered} == {row: str(index + 1) for index, row in enumerate(row_ids)}
    selections = lambda snapshot: {(cell["rowid"], cell["colid"]) for cell in snapshot if cell["selected"]}
    assert selections(before) and selections(before) == selections(shown) == selections(restored), "Selection lost on row-number toggle"


def assert_density(before, after, height):
    assert logical_values(before) == logical_values(after), "Density changed logical rows/values"
    assert after and all(abs(cell["rowHeight"] - height) <= 1 and abs(cell["height"] - height) <= 1 for cell in after), "Density geometry misses CSS-pixel target"


def decimal_text(value, places):
    return f"{Decimal(str(value)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP):.{places}f}"


def assert_decimal_cells(snapshot, raw, selected_columns=(), places=2):
    by_key = {(cell["rowid"], cell["colid"]): cell["text"] for cell in snapshot}
    assert len(by_key) == len(snapshot), "Duplicated formatted cells"
    assert raw and set(raw) <= set(by_key), "Missing raw numeric cell"
    for key, value in raw.items():
        precision = places if not selected_columns or key[1] in selected_columns else 2
        assert by_key[key] == decimal_text(value, precision), f"Wrong formatted text: {key}: {by_key[key]}"


def assert_scale(before, after):
    assert logical_values(before) == logical_values(after)
    expected = dict(zip(SCALE_IDS, ("rgba(248, 105, 107, 0.82)", "rgba(248, 105, 107, 0.06)", "rgba(99, 190, 123, 0.82)")))
    values = {"q1-eclair": "0", "q2-eclair": "100", "q1-munchen": "200"}
    for row, background in expected.items():
        cell = next(cell for cell in after if cell["rowid"] == row and cell["colid"] == "scale_value_sum")
        assert cell["background"] == background and cell["text"] == values[row], "Scale stop/background differs"
    controls = lambda snapshot: {(cell["rowid"], cell["colid"]): (cell["background"], cell["color"]) for cell in snapshot if cell["colid"] != "scale_value_sum"}
    assert controls(before) and controls(after) == controls(before), "Color scale changed unselected columns"


def assert_data_bars(before, after):
    assert logical_values(before) == logical_values(after)
    for row, target in zip(SCALE_IDS, (0, .5, 1)):
        cell = next(cell for cell in after if cell["rowid"] == row and cell["colid"] == "scale_value_sum")
        assert cell["barPercent"] is not None and abs(cell["barPercent"] / 100 - target) <= .01
        assert abs(cell["barWidth"] / cell["usableWidth"] - target) <= .01, "Actual data-bar width misses target"
        assert cell["barBehindText"] and cell["text"] == str(int(target * 200))
    assert not any(cell["barWidth"] for cell in after if cell["colid"] != "scale_value_sum"), "Unselected column received bars"


def assert_zoom(before, zoomed, restored):
    for key in ("cellWidth", "rowHeight"):
        assert abs(zoomed[key] - before[key] * 125 / 115) <= 2, f"125% zoom did not scale {key}"
        assert abs(restored[key] - zoomed[key]) <= 2, f"Exit changed zoomed {key}"
    assert before["zoom"] == 1.15 and zoomed["zoom"] == restored["zoom"] == 1.25
    assert abs(restored["scrollLeft"] - zoomed["scrollLeft"]) <= 2 and abs(restored["scrollTop"] - zoomed["scrollTop"]) <= 2
    assert abs(restored["windowX"] - zoomed["windowX"]) <= 2 and abs(restored["windowY"] - zoomed["windowY"]) <= 2


def _element(driver, selector):
    from tests.test_flagship_certification import _wait_for_visible_workspace_element
    return _wait_for_visible_workspace_element(driver, selector)


def _section(driver, name, control):
    from tests.test_flagship_certification import _workspace_elements
    if not any(el.is_displayed() for el in _workspace_elements(driver, control)):
        _element(driver, f'[data-testid="pivot-toolbar-{name}"]').click()
    return _element(driver, control)


def _select(driver, keys):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.flagship_embedded_clipboard import _cell
    for index, (row, column) in enumerate(keys):
        element = _cell(driver, row, column)
        assert element is not None, f"Missing selection cell {row}/{column}"
        action = ActionChains(driver)
        if index:
            action.key_down(Keys.CONTROL)
        action.move_to_element(element).click()
        if index:
            action.key_up(Keys.CONTROL)
        action.perform()
    WebDriverWait(driver, 8).until(lambda current: set(support._selection_ids(current)) == set(keys))


def _seed(driver, host, requirement):
    state = starting_state(requirement)
    snapshot = {"version": 1, "table": "revenue_operations", "state": state}
    # Public persisted configuration is installed before navigation. This script
    # does not access React or mutate the state under test after startup.
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source":
        f"localStorage.setItem({json.dumps(f'flagship-{host}-pivot-savedView')},{json.dumps(json.dumps(snapshot))});"})
    return state


def _raw_snapshot(driver, state):
    trace = driver.execute_script("return window.__pivotAxisTrace || []")
    requests = {entry["requestId"]: entry for entry in trace if entry["phase"] == "request"
                and entry.get("rowFields") == state["rowFields"] and entry.get("colFields") == state["colFields"]
                and entry.get("valueAggregations") == [{"field": value["field"], "agg": value["agg"]} for value in state["valConfigs"]]}
    for reply in reversed(trace):
        if reply["phase"] != "response" or reply.get("status") != "data" or reply["requestId"] not in requests:
            continue
        actual_filters = requests[reply["requestId"]].get("filters") or {}
        if any([(item.get("type"), item.get("value")) for item in actual_filters.get(field, {}).get("conditions", [])]
               != [(item.get("type"), item.get("value")) for item in wanted["conditions"]]
               for field, wanted in state["filters"].items()):
            continue
        rows = [row for row in reply.get("rows") or [] if "_id" in row]
        if state["rowFields"] == ["rowId"]:
            expected = {row: (value, control) for row, value, control in zip(SCALE_IDS, (0, 100, 200), (10, 20, 30))}
            assert len(rows) == 3 and {row["_path"] for row in rows} == set(expected)
            for row in rows:
                assert (row["scale_value_sum"], row["scale_control_sum"]) == expected[row["_path"]]
        elif any(value["field"] == "format_edge" for value in state["valConfigs"]):
            assert len(rows) == 3
            grand = next(row for row in rows if row.get("_path") == "__grand_total__")
            assert grand[f"__RowTotal__{SALES}_sum"] == 200
            edge = next(row for row in rows if row.get("_path") == ECLAIR)
            assert edge[f"Q1_{SALES}_sum"] == 100.25 and edge[f"Q2_{SALES}_sum"] == 20
            assert edge["Q1_format_edge_sum"] == -100.25
        else:
            assert_axis_rows(rows, transposed=False)
        numeric = {(row["_path"], column): value for row in rows for column, value in row.items()
                   if column.endswith("_sum") and isinstance(value, (int, float)) and not isinstance(value, bool)}
        actual = logical_values(cells(driver))
        if numeric and set(numeric) <= set(actual):
            return {"requestId": reply["requestId"], "rawRows": rows, "numeric": numeric, "layoutRevision": reply.get("layoutRevision")}
    return False


def _geometry(driver):
    grid = _element(driver, '[data-pivot-scroll-container]')
    cell = _element(driver, f'[role="gridcell"][data-rowid="{ECLAIR}"][data-colid="Q1_{SALES}_sum"]')
    return driver.execute_script("""
      const grid=arguments[0], cell=arguments[1], rect=grid.getBoundingClientRect();
      let parent=grid; while(parent && !parent.style.zoom) parent=parent.parentElement;
      return {zoom:Number(parent?.style.zoom),cellWidth:cell.getBoundingClientRect().width,
        rowHeight:cell.getBoundingClientRect().height,viewportWidth:rect.width,viewportHeight:rect.height,
        scrollLeft:grid.scrollLeft,scrollTop:grid.scrollTop,windowX:scrollX,windowY:scrollY};
    """, grid, cell)


def _copy_selected(driver, expected):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _clipboard_text
    ActionChains(driver).key_down(Keys.CONTROL).send_keys('c').key_up(Keys.CONTROL).perform()
    text = WebDriverWait(driver, 8).until(lambda current: (value if (value := _clipboard_text(current)) == expected else False))
    assert text == expected
    return text


def _number_dialog(driver, places):
    from selenium.webdriver.common.keys import Keys
    from tests.test_flagship_certification import _workspace_elements
    _section(driver, "format", '[title="Open number format options"]').click()
    dialog = _element(driver, '[role="dialog"][aria-label="Format Cells"]')
    next(button for button in dialog.find_elements("css selector", "button") if button.text == "Number").click()
    field = dialog.find_element("css selector", 'input[type="number"]')
    field.send_keys(Keys.CONTROL, "a")
    field.send_keys(str(places), Keys.TAB)
    checkbox = dialog.find_element("css selector", 'input[type="checkbox"]')
    if checkbox.is_selected():
        checkbox.click()
    next(button for button in dialog.find_elements("css selector", "button") if button.text == "OK").click()
    assert not any(el.is_displayed() for el in _workspace_elements(driver, '[role="dialog"][aria-label="Format Cells"]'))


def _run(driver, host, requirement, state, wait, observation):
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import Select
    from tests.test_flagship_certification import _switch_to_workspace, _workspace_elements
    raw = wait.until(lambda current: _raw_snapshot(current, state))
    observation["rawBefore"] = {key: value for key, value in raw.items() if key != "numeric"}
    before = cells(driver)
    observation["before"] = before
    target = (ECLAIR, f"Q1_{SALES}_sum")
    if requirement == "row-numbers.toggle":
        _select(driver, [target])
        before = cells(driver)
        _section(driver, "view", '[title="Toggle row numbers"]').click()
        shown = wait.until(lambda current: (snapshot if logical_values(snapshot := cells(current)) == logical_values(before)
            and any(cell["colid"] == "__row_number__" for cell in snapshot) else False))
        observation["shown"] = shown
        _element(driver, '[title="Toggle row numbers"]').click()
        restored = wait.until(lambda current: (snapshot if logical_values(snapshot := cells(current)) == logical_values(before)
            and not any(cell["colid"] == "__row_number__" for cell in snapshot) else False))
        observation["restored"] = restored
        assert_number_column(before, shown, restored)
        observation.update(shown=shown, restored=restored, numberingPolicy="Visible groups and grand total consume sequential displayed indices; first displayed group is 1")
    elif requirement == "density.cycle":
        assert_density(before, before, 44)
        observation["densityStates"] = []
        for label, height in (("Loose", 56), ("Compact", 36), ("Normal", 44)):
            _section(driver, "view", '[title="Cycle row density"]').click()
            wait.until(lambda current: all(abs(cell["rowHeight"] - height) <= 1 for cell in cells(current)))
            after = cells(driver)
            assert_density(before, after, height)
            assert _element(driver, '[title="Cycle row density"]').text.strip() == label
            observation["densityStates"].append({"name": label, "height": height, "cells": after})
    elif requirement == "number-format.decimals":
        observation["scopes"] = []
        for scope in ("default", "selected-columns"):
            if scope == "selected-columns":
                # Reconsume the public initial configuration to isolate scopes.
                _switch_to_workspace(driver, host)
                raw = wait.until(lambda current: _raw_snapshot(current, state))
            assert_decimal_cells(cells(driver), raw["numeric"], places=2)
            columns = (f"Q1_{SALES}_sum", f"Q2_{SALES}_sum", "Q1_format_edge_sum") if scope == "selected-columns" else ()
            if columns:
                _select(driver, [(ECLAIR, column) for column in columns])
            _number_dialog(driver, 1)
            def formatted(current):
                try:
                    snapshot = cells(current)
                    assert_decimal_cells(snapshot, raw["numeric"], columns, places=1)
                    return snapshot
                except AssertionError:
                    return False
            after = wait.until(formatted)
            observation["scopes"].append({"scope": scope, "selectedColumns": columns, "cells": after})
    elif requirement == "cell-format.apply-close":
        keys = [target, (ECLAIR, f"Q1_{UNITS}_sum")]
        _select(driver, keys)
        before = cells(driver)
        _section(driver, "format", '[title="Format selected cells"]').click()
        for label, value in (("Background", "#123456"), ("Text color", "#ffffff")):
            field = _element(driver, f'[data-cell-format-popover] input[aria-label="{label}"]')
            field.send_keys(Keys.CONTROL, "a")
            field.send_keys(value, Keys.TAB)
        popover = _element(driver, '[data-cell-format-popover]')
        next(button for button in popover.find_elements("css selector", "button") if button.text == "Apply to 2").click()
        _element(driver, '[aria-label="Close cell formatting"]').click()
        assert not any(el.is_displayed() for el in _workspace_elements(driver, '[data-cell-format-popover]'))
        focused = driver.execute_script("let el=document.activeElement;while(el?.shadowRoot?.activeElement)el=el.shadowRoot.activeElement;return !!el?.closest('[role=grid]');")
        assert focused, "Closing cell formatting did not restore grid focus"
        observation.update(gridFocused=focused, clipboard=_copy_selected(driver, "100.25\t2.00"))
        # The selected-cell overlay deliberately covers fill colors. Remove it
        # through the public clear control before sampling the applied fill.
        clear = _element(driver, '[data-pivot-status-action="clear-selection"]')
        driver.execute_script("arguments[0].scrollIntoView({block:'center'})", clear)
        clear.click()
        wait.until(lambda current: not support._selection_ids(current))
        after = cells(driver)
        observation["after"] = after
        assert logical_values(before) == logical_values(after)
        formatted = [cell for cell in after if (cell["rowid"], cell["colid"]) in keys]
        assert len(formatted) == 2 and all(cell["background"] == "rgb(18, 52, 86)" and cell["color"] == "rgb(255, 255, 255)" for cell in formatted)
        controls = lambda snapshot: {(cell["rowid"], cell["colid"]): (cell["color"], cell["background"]) for cell in snapshot if (cell["rowid"], cell["colid"]) not in keys}
        assert controls(before) and controls(before) == controls(after), "Formatting escaped selected range"
    elif requirement.startswith("color-scales."):
        _select(driver, [(row, "scale_value_sum") for row in SCALE_IDS])
        before = cells(driver)
        if requirement == "color-scales.scale":
            Select(_section(driver, "format", 'select[title="Color scale"]')).select_by_value("col")
            palette = next(el for el in _workspace_elements(driver, "select") if el.is_displayed() and any(option.get_attribute("value") == "redGreen" for option in el.find_elements("css selector", "option")))
            Select(palette).select_by_value("redGreen")
            clear = _element(driver, '[data-pivot-status-action="clear-selection"]')
            driver.execute_script("arguments[0].scrollIntoView({block:'center'})", clear)
            clear.click()
            wait.until(lambda current: not support._selection_ids(current))
            wait.until(lambda current: any(cell["background"] == "rgba(99, 190, 123, 0.82)" for cell in cells(current)))
            after = cells(driver)
            observation["after"] = after  # Preserve evidence even when control-column isolation fails.
            assert_scale(before, after)
        else:
            _section(driver, "format", '[title="Toggle data bars for selected columns"]').click()
            wait.until(lambda current: any(cell["barWidth"] > 0 for cell in cells(current)))
            after = cells(driver)
            observation["after"] = after
            assert_data_bars(before, after)
            observation.update(bounds=[0, 200], negativePolicy="Absolute value uses negative tint; runtime derives bounds from column statistics", clampingPolicy="Absolute value / max(abs(min),abs(max)), clamped to 100%; no public fixed-bound control")
    elif requirement == "themes.select-override":
        _section(driver, "theme", '[title="Edit theme colors"]')
        theme_select = next(el for el in _workspace_elements(driver, "select") if el.is_displayed() and any(option.get_attribute("value") == "dark" for option in el.find_elements("css selector", "option")))
        root = _element(driver, f'[id="flagship-{host}-pivot"]')
        appearance = lambda: driver.execute_script("const s=getComputedStyle(arguments[0]);return {background:s.backgroundColor,color:s.color}", root)
        light = appearance()
        Select(theme_select).select_by_value("dark")
        wait.until(lambda current: appearance() == {"background": "rgb(10, 12, 16)", "color": "rgb(248, 250, 252)"})
        dark = appearance()
        assert light != dark
        _element(driver, '[title="Edit theme colors"]').click()
        field = driver.execute_script("""
          const visit=root=>{ for(const label of root.querySelectorAll('label')) if(label.textContent.trim()==='Page Bg') return label.parentElement.querySelector('input[type=color]');
            for(const el of root.querySelectorAll('*')) if(el.shadowRoot){const found=visit(el.shadowRoot);if(found)return found;} return null;};return visit(document);
        """)
        assert field is not None
        # WebDriver's color send_keys changes the DOM value without a trusted
        # picker event. Use the real picker and keyboard instead: one upward
        # step from the dark theme's #0a0c10 yields #0a0c0f in Chrome.
        driver.execute_script("window.__pivotThemeColorInputs=[];arguments[0].addEventListener('input',e=>window.__pivotThemeColorInputs.push({value:e.target.value,trusted:e.isTrusted}),true)", field)
        driver.execute_script("arguments[0].scrollIntoView({block:'center'})", field)
        field.click()
        ActionChains(driver).send_keys(Keys.ARROW_UP, Keys.ENTER).perform()
        wait.until(lambda current: appearance()["background"] == "rgb(10, 12, 15)")
        events = driver.execute_script("return window.__pivotThemeColorInputs")
        assert events and events[-1] == {"value": "#0a0c0f", "trusted": True}
        # The popover can cover its anchor. Use its own real Done control.
        done = next(button for button in _workspace_elements(driver, 'button') if button.is_displayed() and button.text == 'Done')
        driver.execute_script("arguments[0].scrollIntoView({block:'center'})", done)
        done.click()
        from selenium.webdriver.support import expected_conditions as EC
        wait.until(EC.staleness_of(field))
        _select(driver, [target])
        _section(driver, "view", '[title="Cycle row density"]').click()
        wait.until(lambda current: all(abs(cell["height"] - 56) <= 1 for cell in cells(current)))
        assert logical_values(before) == logical_values(cells(driver))
        observation.update(light=light, dark=dark, override=appearance(), chosenOverride="#0a0c0f", nativeColorInputs=events)
        # White text over the chosen dark background exceeds WCAG normal-text contrast.
        assert appearance()["color"] == "rgb(248, 250, 252)"
    else:
        before_geometry = _geometry(driver)
        _element(driver, '[title="Zoom in"]').click()
        wait.until(lambda current: _geometry(current)["zoom"] == 1.25)
        zoomed = _geometry(driver)
        _element(driver, '[title="Immersive Mode — hide all controls and show only the table"]').click()
        wait.until(lambda current: bool(_workspace_elements(current, '[data-immersive-exit-overlay]')))
        immersive = _geometry(driver)
        assert immersive["viewportHeight"] > zoomed["viewportHeight"] + 2 or immersive["viewportWidth"] > zoomed["viewportWidth"] + 2, "Immersive workspace did not expand"
        _select(driver, [target])
        copied = _copy_selected(driver, "100.25")
        overlay = _element(driver, '[data-immersive-exit-overlay]')
        ActionChains(driver).move_to_element(overlay).pause(.25).click().perform()
        wait.until(lambda current: not _workspace_elements(current, '[data-immersive-exit-overlay]'))
        restored = _geometry(driver)
        assert_zoom(before_geometry, zoomed, restored)
        assert logical_values(before) == logical_values(cells(driver))
        observation.update(beforeGeometry=before_geometry, zoomed=zoomed, immersive=immersive, restored=restored, clipboard=copied)
    final_raw = _raw_snapshot(driver, state)
    assert final_raw and final_raw["numeric"] == raw["numeric"], "Presentation changed authoritative measure values"
    observation["rawAfter"] = {key: value for key, value in final_raw.items() if key != "numeric"}


def qualify_presentation(host, requirement):
    import pytest
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (
        _qualification_chrome_options, _switch_to_workspace, _verify_release_asset_batch, _host_url, _origin,
    )
    assert requirement in REQUIREMENTS
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    binding_path = os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING")
    if not binding_path:
        pytest.fail("Installed artifact binding is required")
    binding = load_artifact_binding(Path(binding_path))
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "presentation" / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "setup": "Public version-1 saved-view configuration", "locale": "en-GB", "browserZoomPercent": 100}
    driver, paths = None, []
    try:
        options = _qualification_chrome_options(downloads)
        options.add_argument("--lang=en-GB")
        driver = webdriver.Chrome(options=options)
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}", infrastructureUnavailable=False)
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Browser.grantPermissions", {"origin": _origin(_host_url(host)), "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": AXIS_PROBE})
        state = _seed(driver, host, requirement)
        observation["startingState"] = state
        _switch_to_workspace(driver, host)
        _run(driver, host, requirement, state, WebDriverWait(driver, 20), observation)
        result.update(result="passed", actionCompleted=True, logicalValuesPreserved=True, v1RequestsCorrelated=True)
        observation["diagnostic"] = ""
    except Exception:
        observation["diagnostic"] = traceback.format_exc()
    finally:
        if driver is not None:
            try:
                observation["finalCells"] = cells(driver)
                observation["trace"] = driver.execute_script("return window.__pivotAxisTrace || []")
                observation["nativeColorInputs"] = driver.execute_script("return window.__pivotThemeColorInputs || []")
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                screenshot = directory / "final.png"
                driver.save_screenshot(str(screenshot))
                paths.append(str(screenshot))
            except Exception:
                result["result"] = "blocked"
                observation["evidenceDiagnostic"] = traceback.format_exc()
            finally:
                driver.quit()
        observation.update(result={key: value for key, value in result.items() if key != "loadedAssetSha256"}, loadedAssetSha256=result.get("loadedAssetSha256", {}))
        path = directory / "observation.json"
        path.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(path))
        result["evidence_paths"] = paths
        support.record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {observation.get('diagnostic', '')}; evidence: {directory}"
