"""Installed chart journeys: native controls, real model CSV and SVG downloads.

CSV is the product's actual model serialization, not a reconstructed chart.
DOM scripts below only observe rendered elements, geometry and attributes.
"""
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import traceback
import xml.etree.ElementTree as ET

from tests import flagship_browser_support as support
from tests.flagship_axis_swap import AXIS_PROBE
from tests.flagship_remaining_1 import SALES, UNITS, ECLAIR, starting_state, _element, _section, _raw_snapshot, cells, logical_values

MUNCHEN = "M\u00fcnchen"
REQUIREMENTS = ("range-chart.source", "range-chart.locked-source", "range-chart.live-source",
                "chart-pane.lifecycle", "chart-export.artifact")
COLUMNS = (f"Q1_{SALES}_sum", f"Q1_{UNITS}_sum")
INITIAL_ROWS = [[ECLAIR, 100.25, 2], [MUNCHEN, 49.75, 3]]


def parse_chart_csv(content):
    rows = list(csv.reader(io.StringIO(content.decode("utf-8-sig"))))
    indices = [i for i, row in enumerate(rows) if row and row[0] == "Category"]
    assert len(indices) == 1, "Missing or duplicate chart source header"
    index = indices[0]
    header = rows[index]
    assert len(header) == 3, "Chart has missing or extra series"
    assert "ventes" in header[1].lower() and "unit\u00e9s" in header[2].lower(), "Sales/units series order differs"
    data = rows[index + 1:]
    assert data and all(len(row) == 3 for row in data), "Ragged/empty chart source"
    parsed = [[row[0], float(row[1]), float(row[2])] for row in data]
    assert len({row[0] for row in parsed}) == len(parsed), "Duplicate chart source rows"
    return {"header": header, "rows": parsed, "metadata": rows[:index]}


def assert_chart_source(source, expected, original_header=None):
    assert source["rows"] == expected, f"Chart source differs: {source['rows']}"
    if original_header is not None:
        assert source["header"] == original_header, "Reselection/docking changed series identity/order"


def assert_svg_image(content, live, source):
    assert b"<!DOCTYPE" not in content and b"<!ENTITY" not in content
    root = ET.fromstring(content)
    assert root.tag == "{http://www.w3.org/2000/svg}svg", "Download is not an SVG image"
    width, height = float(root.attrib["width"]), float(root.attrib["height"])
    viewbox = [float(value) for value in root.attrib["viewBox"].replace(",", " ").split()]
    assert viewbox == [0, 0, width, height] and width > 0 and height > 0
    assert width == live["viewBox"][2], "Export width differs from declared chart width"
    # Product declares a title band and one or more measured legend rows.
    used, rows = 0, 0
    for label in live["legend"]:
        item_width = max(48, len(label) * 6.4 + 30)
        if used and used + item_width > max(80, width - 24):
            rows += 1
            used = 0
        used += item_width
    if used:
        rows += 1
    expected_height = live["viewBox"][3] + (42 if live["title"] else 0) + (rows * 22 + 14 if rows else 0)
    assert height == expected_height, "Export height differs from declared plot/title/legend dimensions"
    text = ["".join(node.itertext()) for node in root.iter("{http://www.w3.org/2000/svg}text")]
    assert all(row[0] in text for row in source["rows"]), "Image omits selected category labels"
    assert all(label in text for label in source["header"][1:]), "Image omits selected series legend"
    assert live["title"] in text, "Image omits chart title"
    marks = [node for node in root.iter() if "data-tip-cat" in node.attrib]
    expected_marks = {(row[0], label) for row in source["rows"] for label in source["header"][1:]}
    assert len(marks) == len(expected_marks) and {(node.attrib["data-tip-cat"], node.attrib["data-tip-ser"]) for node in marks} == expected_marks
    assert all(node.tag == "{http://www.w3.org/2000/svg}rect" for node in marks), "Chosen chart is not rendered as bars"
    assert not any(node.tag.endswith("script") or node.tag.endswith("image") for node in root.iter()), "Image unexpectedly depends on executable/external content"
    return {"format": "svg", "width": width, "height": height, "viewBox": viewbox,
            "labels": text, "marks": [dict(node.attrib) for node in marks], "sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content)}


def _visible(driver, selector):
    from tests.test_flagship_certification import _workspace_elements
    return [element for element in _workspace_elements(driver, selector) if element.is_displayed()]


def _button(driver, text, prefix=False):
    from selenium.common.exceptions import StaleElementReferenceException
    from selenium.webdriver.support.ui import WebDriverWait
    def find(current):
        # One synchronous observation avoids keeping hundreds of remote button
        # handles across the preview's asynchronous renderer mount. The caller
        # still performs the real Selenium click on the returned DOM element.
        return current.execute_script("""
          const text=arguments[0], prefix=arguments[1];
          const visit=root=>{
            for(const el of root.querySelectorAll('button')) {
              const value=el.innerText.trim(), style=getComputedStyle(el);
              if(!el.disabled && el.getClientRects().length && style.visibility!=='hidden'
                && (prefix ? value.startsWith(text) : value===text)) return el;
            }
            for(const el of root.querySelectorAll('*')) if(el.shadowRoot) {
              const found=visit(el.shadowRoot); if(found) return found;
            }
            return null;
          };return visit(document);
        """, text, prefix)
    return WebDriverWait(driver, 12, ignored_exceptions=(StaleElementReferenceException,)).until(find)


def _pane_count(driver):
    return len(_visible(driver, 'button[title="Close chart pane"]'))


def _live_chart(driver):
    surface = _element(driver, '[data-chart-surface-main="true"]')
    return driver.execute_script("""
      const surface=arguments[0], svg=surface.querySelector('svg[viewBox]');
      if(!svg) return null;
      return {viewBox:svg.getAttribute('viewBox').split(/[ ,]+/).map(Number),
        legend:[...surface.querySelectorAll('[data-chart-export-legend-item="true"]')].map(el=>el.getAttribute('data-chart-export-label')),
        title:surface.querySelector('input[placeholder="Chart title"]')?.value || '',
        marks:[...svg.querySelectorAll('[data-tip-cat][data-tip-ser]')].map(el=>({tag:el.localName,category:el.dataset.tipCat,series:el.dataset.tipSer,value:el.dataset.tipVal})),
        deviceScale:devicePixelRatio};
    """, surface)


def _download(driver, directory, format, observation, label):
    from selenium.webdriver.support.ui import WebDriverWait
    before = set(directory.iterdir())
    _element(driver, '[title="Export chart"]').click()
    _button(driver, {"csv": "CSV data", "svg": "SVG image"}[format], prefix=True).click()
    def new_file(_):
        new = set(directory.iterdir()) - before
        complete = [path for path in new if path.suffix == "." + format]
        return complete[0] if len(new) == len(complete) == 1 and complete[0].stat().st_size > 0 else False
    path = WebDriverWait(driver, 20).until(new_file)
    observation.setdefault("downloads", []).append({"checkpoint": label, "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size})
    return path


def _source(driver, directory, observation, label, expected, header=None):
    path = _download(driver, directory, "csv", observation, label)
    source = parse_chart_csv(path.read_bytes())
    assert_chart_source(source, expected, header)
    live = _live_chart(driver)
    titles = [row[1] for row in source["metadata"] if len(row) == 2 and row[0] == "Title"]
    assert len(titles) == 1 and titles[0], "Chart has no declared export title"
    live["title"] = titles[0]
    assert live and live["legend"] == source["header"][1:], "Rendered series differ from source model"
    expected_marks = {(row[0], series) for row in expected for series in source["header"][1:]}
    assert len(live["marks"]) == len(expected_marks)
    assert {(mark["category"], mark["series"]) for mark in live["marks"]} == expected_marks
    assert all(mark["tag"] == "rect" for mark in live["marks"]), "Chart is not a bar chart"
    observation.setdefault("sources", []).append({"checkpoint": label, **source, "rendered": live})
    return source


def _seed(driver, host):
    state = starting_state("range-chart.source")
    state.update(columnOrder=["hierarchy", *COLUMNS], sorting=[{"id": COLUMNS[0], "desc": True}], chartCanvasPanes=[])
    snapshot = {"version": 1, "table": "revenue_operations", "state": state}
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source":
        f"localStorage.setItem({json.dumps(f'flagship-{host}-pivot-savedView')},{json.dumps(json.dumps(snapshot))});"})
    return state


def _create(driver, wait, observation):
    assert _pane_count(driver) == 0, "Unexpected persisted chart pane"
    order = support._selection_column_order(driver)
    assert order.index(COLUMNS[1]) == order.index(COLUMNS[0]) + 1
    rows = support._selection_rows(driver)
    assert [row["label"] for row in rows] == [ECLAIR, MUNCHEN]
    observation["selectedIds"] = support._select_rectangle(driver, [row["id"] for row in rows], COLUMNS)
    _section(driver, "charts", '[data-add-chart-pane="true"]').click()
    _button(driver, "Selection").click()
    # Explicit public orientation preserves measure identities as row count changes.
    _button(driver, "By Rows").click()
    _button(driver, "Bar / Column").click()
    _button(driver, "Open in Panel").click()
    wait.until(lambda current: _pane_count(current) == 1)
    # Settings can widen panes and hide grid cells; close through the actual UI.
    _element(driver, '[title="Close settings"]').click()
    assert len(_visible(driver, '[aria-label="Lock chart to the current data selection"]')) == 1
    return [row["id"] for row in rows]


def _run(driver, host, requirement, state, wait, directory, observation):
    from selenium.webdriver.common.action_chains import ActionChains
    raw = wait.until(lambda current: _raw_snapshot(current, state))
    observation["rawBefore"] = {key: value for key, value in raw.items() if key != "numeric"}
    before = logical_values(cells(driver))
    row_ids = _create(driver, wait, observation)
    downloads = directory / "downloads"
    initial = _source(driver, downloads, observation, "initial", INITIAL_ROWS)
    if requirement in ("range-chart.locked-source", "range-chart.live-source"):
        locked = requirement == "range-chart.locked-source"
        if locked:
            _element(driver, '[aria-label="Lock chart to the current data selection"]').click()
            _element(driver, '[aria-label="Unlock chart data and resume live updates"]')
        observation["nextSelectedIds"] = support._select_rectangle(driver, [row_ids[1]], COLUMNS)
        assert set(observation["nextSelectedIds"]) == {(row_ids[1], column) for column in COLUMNS}
        # A real event loop turn must apply the new selection before export.
        if not locked:
            wait.until(lambda current: (live := _live_chart(current)) and {mark["category"] for mark in live["marks"]} == {MUNCHEN})
        expected = INITIAL_ROWS if locked else [INITIAL_ROWS[1]]
        _source(driver, downloads, observation, "after-reselection", expected, initial["header"])
        assert len(_visible(driver, '[aria-label="Unlock chart data and resume live updates"]' if locked else '[aria-label="Lock chart to the current data selection"]')) == 1
    elif requirement == "chart-pane.lifecycle":
        pane = _element(driver, '[data-docked-chart-pane]')
        first = driver.execute_script("return arguments[0].getBoundingClientRect().width", pane)
        handle = _element(driver, '[title="Resize workspace panes"]')
        ActionChains(driver).move_to_element(handle).click_and_hold().move_by_offset(-40, 0).pause(.2).release().perform()
        def resized(current):
            width = current.execute_script("return arguments[0].getBoundingClientRect().width", _element(current, '[data-docked-chart-pane]'))
            return width if abs(width - first - 40) <= 2 else False
        after = wait.until(resized)
        assert abs(after - first - 40) <= 2
        observation["resize"] = {"before": first, "after": after, "requestedDelta": 40}
        _element(driver, '[title="Float chart over table"]').click()
        wait.until(lambda current: not _visible(current, '[data-docked-chart-pane]'))
        assert _pane_count(driver) == 1
        _source(driver, downloads, observation, "floating", INITIAL_ROWS, initial["header"])
        _element(driver, '[title="Dock chart panel"]').click()
        wait.until(lambda current: len(_visible(current, '[data-docked-chart-pane]')) == 1)
        _source(driver, downloads, observation, "redocked", INITIAL_ROWS, initial["header"])
        _element(driver, '[title="Close chart pane"]').click()
        wait.until(lambda current: _pane_count(current) == 0 and not _visible(current, '[data-chart-surface-main="true"]'))
        observation["selectionAfterClose"] = support._select_rectangle(driver, [row_ids[1]], COLUMNS)
        copy_observation = {"clipboardLineSeparator": "\r\n" if str(driver.capabilities.get("platformName", "")).lower() == "windows" else "\n"}
        support._copy_selected(driver, support._selection_cell(driver, row_ids[1], COLUMNS[0]),
                               [["49.75", "3.00"]], copy_observation)
        observation["copyAfterClose"] = copy_observation
        assert copy_observation["clipboardText"] == "49.75\t3.00"
    elif requirement == "chart-export.artifact":
        live = _live_chart(driver)
        live["title"] = next(row[1] for row in initial["metadata"] if row and row[0] == "Title")
        path = _download(driver, downloads, "svg", observation, "selected-image")
        observation["image"] = assert_svg_image(path.read_bytes(), live, initial)
        observation["image"]["deviceScale"] = live["deviceScale"]
        assert live["deviceScale"] == 1
        assert _pane_count(driver) == 1
        _source(driver, downloads, observation, "after-image-export", INITIAL_ROWS, initial["header"])
    assert _pane_count(driver) == (0 if requirement == "chart-pane.lifecycle" else 1)
    # Docking narrows the grid and may unmount horizontal virtual columns. The
    # authoritative reply must remain unchanged, and every mounted logical cell
    # must retain its independently checked pre-chart value.
    trace = driver.execute_script("return window.__pivotAxisTrace || []")
    replies = [entry for entry in trace if entry["phase"] == "response" and entry.get("status") == "data" and entry.get("rows")]
    assert replies and [row for row in replies[-1]["rows"] if "_id" in row] == raw["rawRows"], "Chart action replaced authoritative grid data"
    current = logical_values(cells(driver))
    assert current and set(current) <= set(before) and all(before[key] == value for key, value in current.items()), "Chart action changed grid values"
    assert all((row, column) in current for row in row_ids for column in COLUMNS), "Selected grid values vanished"
    observation["rawAfter"] = replies[-1]


def qualify_charts(host, requirement):
    import pytest
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import (_qualification_chrome_options, _switch_to_workspace,
        _verify_release_asset_batch, _host_url, _origin)
    assert requirement in REQUIREMENTS
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    binding_path = os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING")
    assert binding_path, "Installed artifact binding is required"
    binding = load_artifact_binding(Path(binding_path))
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "charts" / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "setup": "Public v1 saved-view setup; all chart actions use native controls"}
    driver, paths = None, []
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}", infrastructureUnavailable=False)
        driver._pivot_artifact_binding, driver._pivot_artifact_host = binding, host
        driver._pivot_loaded_assets = {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        if requirement == "chart-pane.lifecycle":
            driver.execute_cdp_cmd("Browser.grantPermissions", {"origin": _origin(_host_url(host)), "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"]})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": AXIS_PROBE})
        state = _seed(driver, host)
        observation["startingState"] = state
        _switch_to_workspace(driver, host)
        _run(driver, host, requirement, state, WebDriverWait(driver, 25), directory, observation)
        result.update(result="passed", actionCompleted=True, logicalValuesPreserved=True, v1RequestsCorrelated=True)
        observation["diagnostic"] = ""
    except Exception:
        observation["diagnostic"] = traceback.format_exc()
    finally:
        if driver is not None:
            try:
                observation["trace"] = driver.execute_script("return window.__pivotAxisTrace || []")
                observation["finalCells"] = cells(driver)
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
        paths.extend(str(path) for path in downloads.iterdir() if path.is_file())
        observation.update(result={key: value for key, value in result.items() if key != "loadedAssetSha256"}, loadedAssetSha256=result.get("loadedAssetSha256", {}))
        path = directory / "observation.json"
        path.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(path))
        result["evidence_paths"] = paths
        support.record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {observation.get('diagnostic', '')}; evidence: {directory}"
