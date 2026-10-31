"""Native, installed-artifact qualification of derived models and layout.

The observer records unmodified V1 envelopes. Public saved-view seeding is only
startup setup; all creation, removal, moves and restoration use native controls.
"""
from decimal import Decimal
import json
import os
from pathlib import Path
import traceback

from tests import flagship_browser_support as support
from tests.flagship_remaining_1 import SALES, ECLAIR, starting_state as presentation_state

REQUIREMENTS = ("layout-field-zones.move", "formula-columns.add-remove",
                "custom-dimensions.add-remove", "report-mode.restore-definition")
MUNICH = "M\u00fcnchen"
TOKYO = "\u6771\u4eac"
CAIRO = "\u0627\u0644\u0642\u0627\u0647\u0631\u0629"

MODEL_PROBE = r"""
if (!window.__pivotModelTrace) {
  const trace=window.__pivotModelTrace=[];
  const visit=(value,phase)=>{
    if(typeof value==='string'){try{value=JSON.parse(value)}catch(_){return}}
    if(!value || typeof value!=='object') return;
    if(value.schemaVersion===1 && value.kind==='data' && value.requestId){
      trace.push({phase,at:performance.now(),...value});return;
    }
    Object.values(value).forEach(child=>visit(child,phase));
  };
  const fetch=window.fetch.bind(window);
  window.fetch=(...args)=>{visit(args[1]?.body,'request');return fetch(...args).then(response=>{
    response.clone().text().then(text=>visit(text,'response')).catch(()=>{});return response;
  })};
  const send=XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send=function(body){visit(body,'request');
    this.addEventListener('load',()=>visit(this.responseText,'response'));return send.call(this,body)};
}
"""


def starting_state(requirement):
    assert requirement in REQUIREMENTS
    state = presentation_state("row-numbers.toggle")
    state.update(valConfigs=[{"field": SALES, "agg": "sum", "label": "Sales"}],
                 sidebarOpen=True, sidebarTab="fields", customDimensions=[],
                 viewMode="pivot", savedReports=[], activeReportId=None,
                 reportDef={"levels": []}, columnOrder=[])
    if requirement == "layout-field-zones.move":
        state["rowFields"] = ["region", "account", "scope"]
    return state


def expected_sales(kind="base", formula_field=None):
    """Literal independent oracle, including sparse intersections and totals."""
    values = {ECLAIR: (100.25, 20, 120.25), MUNICH: (49.75, 30, 79.75),
              "__grand_total__": (150, 50, 200)}
    if kind == "custom":
        values = {"Included cities": (150, 50, 200), "__grand_total__": (150, 50, 200)}
    if kind == "accounts":
        values = {TOKYO: (100.25, 20, 120.25), CAIRO: (49.75, 30, 79.75),
                  "__grand_total__": (150, 50, 200)}
    result = {}
    for row, numbers in values.items():
        for prefix, value in zip(("Q1_", "Q2_", "__RowTotal__"), numbers):
            if kind == "moved" and prefix != "__RowTotal__":
                for account in (TOKYO, CAIRO):
                    owner = ECLAIR if account == TOKYO else MUNICH
                    actual = (values[owner][0 if prefix == "Q1_" else 1]
                              if row == "__grand_total__" else value if row == owner else None)
                    result[(row, f"{prefix[:-1]}|{account}_{SALES}_sum")] = actual
            else:
                result[(row, f"{prefix}{SALES}_sum")] = value
            if formula_field:
                result[(row, f"{prefix}{formula_field}")] = value * 2
    return result


def assert_model_values(rows, expected):
    data = [row for row in rows if "_id" in row]
    by_path = {row["_path"]: row for row in data}
    assert len(data) == len(by_path) and set(by_path) == {row for row, _ in expected}, "Wrong logical groups"
    expected_columns = {column for _, column in expected}
    for rowid, row in by_path.items():
        # All non-metadata value cells must belong to the declared schema.
        actual_columns = {key for key in row if key.endswith("_sum") or key.startswith(("Q1_formula_", "Q2_formula_", "__RowTotal__formula_"))}
        assert actual_columns == expected_columns, "Missing, extra, or stale measure"
        for column in expected_columns:
            value = expected[(rowid, column)]
            if value is None:
                assert row[column] is None
            else:
                assert isinstance(row[column], (float, int)) and not isinstance(row[column], bool)
                assert Decimal(str(row[column])) == Decimal(str(value)), f"Wrong {rowid}/{column}"


def assert_model_dom(cells, expected):
    gears = [cell for cell in cells if cell["colid"] == "__report_config__"]
    if gears:
        assert len(gears) == len({row for row, _ in expected})
        assert {cell["rowid"] for cell in gears} == {row for row, _ in expected}
        assert all(cell["text"] == "" for cell in gears), "Report gear metadata contains unexpected values"
    actual = [(cell["rowid"], cell["colid"], cell["text"]) for cell in cells if cell["colid"] not in ("hierarchy", "__report_config__")]
    assert len(actual) == len(expected) and len({(row, col) for row, col, _ in actual}) == len(expected), "Missing or duplicate visible cells"
    wanted = {key: "" if value is None else f"{value:.2f}" for key, value in expected.items()}
    assert {(row, col): text for row, col, text in actual} == wanted, "Displayed cells differ from independent oracle"


def _elements(driver, selector):
    from tests.test_flagship_certification import _workspace_elements
    return [element for element in _workspace_elements(driver, selector) if element.is_displayed()]


def _element(driver, selector):
    from selenium.webdriver.support.ui import WebDriverWait
    return WebDriverWait(driver, 10).until(lambda current: next(iter(_elements(current, selector)), False))


def _button(driver, text, root=None):
    candidates = root.find_elements("css selector", "button") if root else _elements(driver, "button")
    return next(button for button in candidates if button.is_displayed() and button.text.strip() == text)


def _type(element, text):
    from selenium.webdriver.common.keys import Keys
    element.send_keys(Keys.CONTROL, "a")
    element.send_keys(text, Keys.TAB)


def _move(driver, label, source, target):
    from selenium.webdriver.common.keys import Keys
    prefix = f'[data-sidebar-drop-zone="{source}"] ' if source != "pool" else '[data-sidebar-field-chip="available"]'
    selector = (prefix + f'[aria-label="Move {label}"]') if source == "pool" else prefix + f'[data-sidebar-field-chip="zone"][aria-label="Move {label}"]'
    chip = _element(driver, selector)
    chip.send_keys(Keys.SPACE)
    assert chip.get_attribute("aria-grabbed") == "true", "Native keyboard pickup did not start"
    _element(driver, f'[data-sidebar-drop-zone="{target}"]').send_keys(Keys.ENTER)


def _zone_labels(driver):
    return {zone: [el.get_attribute("aria-label").removeprefix("Move ") for el in
                   _elements(driver, f'[data-sidebar-drop-zone="{zone}"] [data-sidebar-field-chip="zone"]')]
            for zone in ("rows", "cols", "filter", "vals")}


def assert_zones(zones, *, moved, restored=False):
    assert zones["rows"] == (["Region"] if moved else ["Region", "Account"] + (["Scope"] if restored else []))
    assert zones["cols"] == (["Period", "Account"] if moved else ["Period"])
    assert zones["filter"] == ["Scope"]
    assert sum(label == "Account" for labels in zones.values() for label in labels) == 1
    if moved:
        assert sum(label == "Scope" for labels in zones.values() for label in labels) == 1


def snapshot(driver, checkpoint, expected, *, row_fields=None, col_fields=None,
             formula=None, custom=None, view_mode="pivot", report_root=None):
    cells = support._navigation_cells(driver)
    try:
        assert_model_dom(cells, expected)
    except AssertionError:
        return False
    trace = driver.execute_script("return window.__pivotModelTrace || []")
    requests = {item["requestId"]: item for item in trace if item["phase"] == "request" and item["at"] > checkpoint}
    for reply in reversed(trace):
        if reply["phase"] != "response" or reply.get("status") != "data" or reply["requestId"] not in requests:
            continue
        request = requests[reply["requestId"]]
        state = request.get("state") or {}
        if reply["at"] < request["at"] or state.get("viewMode", "pivot") != view_mode:
            continue
        if row_fields is not None and state.get("rowFields") != row_fields:
            continue
        if col_fields is not None and state.get("colFields") != col_fields:
            continue
        if not any(item.get("value") == "Included" for item in state.get("filters", {}).get("scope", {}).get("conditions", [])):
            continue
        configs = state.get("valConfigs") or []
        if formula is False and (len(configs) != 1 or any(configs[0].get(key) != value for key, value in
                {"field": SALES, "agg": "sum", "label": "Sales"}.items())):
            continue
        if isinstance(formula, dict) and not any(all(config.get(key) == value for key, value in formula.items()) for config in configs):
            continue
        if custom is not None and bool(state.get("customDimensions")) != custom:
            continue
        if report_root is not None and (state.get("reportDef") or {}).get("root", {}).get("field") != report_root:
            continue
        payload = reply.get("payload") or {}
        schema = payload.get("colSchema") or {}
        columns = schema.get("columns") or []
        if (not payload.get("layoutRevision") or payload["layoutRevision"] != schema.get("layoutRevision")
                or {column.get("id") for column in columns} != {column for _, column in expected}
                or len(columns) != len({column for _, column in expected})
                or len({column.get("layoutId") for column in columns}) != len(columns)):
            continue
        try:
            assert_model_values(payload.get("data") or [], expected)
        except AssertionError:
            continue
        return {"request": request, "response": reply, "visibleCells": cells,
                "visibleColumns": support._navigation_columns(driver)}
    return False


def _layout(driver, wait, observation):
    original = ["region", "account", "scope"]
    observation["before"] = wait.until(lambda current: snapshot(current, -1, expected_sales(), row_fields=original, col_fields=["period"]))
    checkpoint = driver.execute_script("return performance.now()")
    _move(driver, "Account", "rows", "cols")
    _move(driver, "Scope", "rows", "filter")
    observation["moved"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales("moved"), row_fields=["region"], col_fields=["period", "account"]))
    observation["movedZones"] = _zone_labels(driver)
    assert_zones(observation["movedZones"], moved=True)
    checkpoint = driver.execute_script("return performance.now()")
    _move(driver, "Account", "cols", "rows")
    # The filter is retained while Scope is restored through the public pool.
    _move(driver, "Scope", "pool", "rows")
    observation["restored"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales(), row_fields=original, col_fields=["period"]))
    observation["restoredZones"] = _zone_labels(driver)
    assert_zones(observation["restoredZones"], moved=False, restored=True)


def _formula(driver, wait, observation):
    _button(driver, "+ Add Formula").click()
    label = _element(driver, 'input[placeholder="Formula label"]')
    _type(label, "double_sales")
    _type(_element(driver, 'input[placeholder="formula1"]'), "double_sales")
    _type(_element(driver, "textarea"), f"2 * [{SALES}]")
    checkpoint = driver.execute_script("return performance.now()")
    _button(driver, "Apply").click()
    wait.until(lambda current: not _elements(current, 'input[placeholder="Formula label"]'))
    # The generated internal field is observed from the actual native edit.
    def authored(current):
        trace = current.execute_script("return window.__pivotModelTrace || []")
        return next((config for item in reversed(trace) if item["phase"] == "request" and item["at"] > checkpoint
                     for config in item.get("state", {}).get("valConfigs", [])
                     if config.get("label") == config.get("formulaRef") == "double_sales" and config.get("agg") == "formula"), False)
    config = wait.until(authored)
    assert config["formula"] == f"2 * [{SALES}]" and config.get("formulaScope", "measures") == "measures"
    observation["formulaConfig"] = config
    observation["created"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales(formula_field=config["field"]), formula=config))
    assert any("double sales" in el.text.casefold().replace("_", " ") for el in _elements(driver, '[role="columnheader"]'))
    checkpoint = driver.execute_script("return performance.now()")
    chip = _element(driver, '[data-sidebar-drop-zone="vals"] [data-sidebar-field-chip="zone"][aria-label="Move double_sales"]')
    chip.find_element("css selector", '[title="Select formula"] > div:nth-child(2) > span:last-child').click()
    observation["removed"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales(), formula=False))
    assert not any("double sales" in el.text.casefold().replace("_", " ") for el in _elements(driver, '[role="columnheader"]'))
    assert not _elements(driver, '[aria-label="Move double_sales"]')
    observation["aggregationStage"] = "Value formula evaluates 2 * [sales] after sum aggregation; Q1=300,Q2=100,total=400"


def _custom(driver, wait, observation):
    from selenium.webdriver.support.ui import Select
    _button(driver, "+ Category").click()
    dialog = _element(driver, '[role="dialog"][aria-label="Add custom category"]')
    _type(dialog.find_element("css selector", 'input[placeholder="Example: Priority Segment"]'), "IncludedGroup")
    _type(dialog.find_element("css selector", 'input[placeholder="Category value"]'), "Included cities")
    selects = dialog.find_elements("css selector", "select")
    field = next(element for element in selects if any(option.get_attribute("value") == "region" for option in element.find_elements("css selector", "option")))
    Select(field).select_by_value("region")
    # Two native Equals clauses avoid the list editor's immediate delimiter
    # normalization swallowing a comma while WebDriver types one key at a time.
    condition = next(element for element in selects if {option.get_attribute("value") for option in element.find_elements("css selector", "option")} == {"AND", "OR"})
    Select(condition).select_by_value("OR")
    _type(dialog.find_element("css selector", 'input[placeholder="Value"]'), ECLAIR)
    _button(driver, "+ Condition", dialog).click()
    fields = [element for element in dialog.find_elements("css selector", "select") if any(option.get_attribute("value") == "region" for option in element.find_elements("css selector", "option"))]
    Select(fields[1]).select_by_value("region")
    _type(dialog.find_elements("css selector", 'input[placeholder="Value"]')[1], MUNICH)
    _button(driver, "Apply Category", dialog).click()
    wait.until(lambda current: not _elements(current, '[role="dialog"][aria-label="Add custom category"]'))
    checkpoint = driver.execute_script("return performance.now()")
    # Remove the two original row fields with their native close controls.
    for label in ("Account", "Region"):
        chip = _element(driver, f'[data-sidebar-drop-zone="rows"] [aria-label="Move {label}"]')
        chip.find_element("css selector", 'span[style*="cursor: pointer"]').click()
    _move(driver, "IncludedGroup", "pool", "rows")
    observation["created"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales("custom"), custom=True))
    dimensions = observation["created"]["request"]["state"]["customDimensions"]
    assert len(dimensions) == 1
    dimension = dimensions[0]
    assert dimension["name"] == "IncludedGroup"
    assert dimension["rules"][0]["label"] == "Included cities"
    condition = dimension["rules"][0]["condition"]
    assert condition["op"] == "OR" and len(condition["clauses"]) == 2
    assert {(clause["field"], clause["operator"], clause["value"]) for clause in condition["clauses"]} == {
        ("region", "eq", ECLAIR), ("region", "eq", MUNICH)}
    assert observation["created"]["request"]["state"]["rowFields"] == [dimension["field"]]
    # The category card's native delete button is the second card button.
    edit = _button(driver, "Edit")
    card = edit.find_element("xpath", "..")
    assert "IncludedGroup" in card.text
    checkpoint = driver.execute_script("return performance.now()")
    card.find_elements("css selector", "button")[1].click()
    _move(driver, "Region", "pool", "rows")
    _move(driver, "Account", "pool", "rows")
    observation["removed"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales(), row_fields=["region", "account"], col_fields=["period"], custom=False))
    assert not _elements(driver, '[aria-label="Move IncludedGroup"]')
    observation["baseDatasetUnchanged"] = "Original region keys and every original period/total sales value restored from the same live dataset"


def _report(driver, host, wait, observation):
    from selenium.webdriver.support.ui import Select
    support._click_totals_control(driver, "Report")
    levels = _element(driver, '[data-report-section="report-levels"]')
    choices = levels.find_elements("css selector", "select")
    if choices:
        Select(choices[0]).select_by_value("region")
    wait.until(lambda current: snapshot(current, -1, expected_sales(), col_fields=["period"], view_mode="report", report_root="region"))
    _type(_element(driver, 'input[placeholder="Report name..."]'), "Complete207 Sales")
    _button(driver, "Save", _element(driver, '[data-report-section="save-report"]')).click()
    storage_key = f"flagship-{host}-pivot-savedReports"
    reports = wait.until(lambda current: current.execute_script("return JSON.parse(localStorage.getItem(arguments[0]))", storage_key))
    saved = next(report for report in reports if report["name"] == "Complete207 Sales")
    assert saved["reportDef"]["root"]["field"] == "region" and saved["id"] and saved["createdAt"]
    assert saved["layout"] == {"colFields": ["period"], "valConfigs": [{"field": SALES, "agg": "sum", "label": "Sales"}]}
    observation["storedReport"] = saved
    # Change the active definition through the selected-level inspector.
    levels = _element(driver, '[data-report-section="report-levels"]')
    levels.find_element("css selector", 'button[draggable="true"]').click()
    inspector = _element(driver, '[data-report-section="selected-level"]')
    inspector.find_element("css selector", '[data-report-level-collapsible] > button').click()
    field = next(element for element in inspector.find_elements("css selector", "select") if any(option.get_attribute("value") == "account" for option in element.find_elements("css selector", "option")))
    checkpoint = driver.execute_script("return performance.now()")
    Select(field).select_by_value("account")
    _button(driver, "Apply", inspector).click()
    observation["changed"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales("accounts"),
        col_fields=["period"], view_mode="report", report_root="account"))
    checkpoint = driver.execute_script("return performance.now()")
    editor = _element(driver, '[data-report-section="save-report"]').find_element("xpath", "..")
    title = next(element for element in editor.find_elements("css selector", "div")
                 if element.text.strip() == "Complete207 Sales" and not element.find_elements("css selector", "div"))
    title.click()
    observation["restored"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales(),
        col_fields=["period"], formula=False, view_mode="report", report_root="region"))
    assert observation["restored"]["request"]["state"]["reportDef"] == saved["reportDef"]
    assert current_reports(driver, storage_key) == reports
    assert any("Q1" in el.text for el in _elements(driver, '[role="columnheader"]'))
    assert any("Q2" in el.text for el in _elements(driver, '[role="columnheader"]'))
    checkpoint = driver.execute_script("return performance.now()")
    support._click_totals_control(driver, "Pivot")
    observation["pivot"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales(), row_fields=["region", "account"], col_fields=["period"]))
    # A subsequent native layout move proves pivot mode remains interactive.
    _element(driver, '[data-testid="pivot-toggle-sidebar"]').click() if not _elements(driver, '[data-sidebar-drop-zone="rows"]') else None
    checkpoint = driver.execute_script("return performance.now()")
    _move(driver, "Account", "rows", "cols")
    observation["interactivePivot"] = wait.until(lambda current: snapshot(current, checkpoint, expected_sales("moved"), row_fields=["region"], col_fields=["period", "account"]))


def current_reports(driver, key):
    return driver.execute_script("return JSON.parse(localStorage.getItem(arguments[0]))", key)


def qualify_models(host, requirement):
    import pytest
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from scripts.flagship_artifacts import load_artifact_binding
    from tests.test_flagship_certification import _qualification_chrome_options, _switch_to_workspace, _verify_release_asset_batch
    assert requirement in REQUIREMENTS
    if os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") != "1":
        pytest.skip("Enable installed flagship browser qualification explicitly")
    if not os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_BINDING"):
        pytest.fail("Installed artifact binding is required")
    binding = load_artifact_binding(Path(os.environ["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]))
    run_id = os.environ["PIVOT_FLAGSHIP_RUN_ID"]
    directory = Path(os.environ["PIVOT_FLAGSHIP_EVIDENCE_ROOT"]) / "derived-models" / requirement / host
    assert ("runs", run_id) in tuple(zip(directory.parts, directory.parts[1:]))
    directory.mkdir(parents=True, exist_ok=True)
    downloads = directory / "downloads"
    downloads.mkdir(exist_ok=True)
    result = {"result": "blocked", "browser": "unavailable", "infrastructureUnavailable": True}
    observation = {"requirement": requirement, "setup": "Public version-1 saved view; native controls thereafter"}
    paths, driver = [], None
    try:
        driver = webdriver.Chrome(options=_qualification_chrome_options(downloads))
        result.update(result="failed", browser=f"{driver.capabilities['browserName']} {driver.capabilities['browserVersion']}", infrastructureUnavailable=False)
        driver._pivot_artifact_binding, driver._pivot_artifact_host, driver._pivot_loaded_assets = binding, host, {}
        driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": MODEL_PROBE})
        state = starting_state(requirement)
        snapshot_value = {"version": 1, "table": "revenue_operations", "state": state}
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": f"localStorage.setItem({json.dumps(f'flagship-{host}-pivot-savedView')},{json.dumps(json.dumps(snapshot_value))});"})
        observation["startingState"] = state
        _switch_to_workspace(driver, host)
        wait = WebDriverWait(driver, 25)
        observation["baseline"] = wait.until(lambda current: snapshot(current, -1, expected_sales(), row_fields=state["rowFields"], col_fields=["period"]))
        {"layout-field-zones.move": lambda: _layout(driver, wait, observation),
         "formula-columns.add-remove": lambda: _formula(driver, wait, observation),
         "custom-dimensions.add-remove": lambda: _custom(driver, wait, observation),
         "report-mode.restore-definition": lambda: _report(driver, host, wait, observation)}[requirement]()
        assert not _elements(driver, '[role="alert"]')
        result.update(result="passed", actionCompleted=True, logicalValuesPreserved=True, v1RequestsCorrelated=True)
        observation["diagnostic"] = ""
    except Exception:
        observation["diagnostic"] = traceback.format_exc()
    finally:
        if driver is not None:
            try:
                observation["trace"] = driver.execute_script("return window.__pivotModelTrace || []")
                observation["finalCells"] = support._navigation_cells(driver)
                observation["finalZones"] = _zone_labels(driver)
                _verify_release_asset_batch(driver, require_assets=True)
                result["loadedAssetSha256"] = dict(driver._pivot_loaded_assets)
                screenshot = directory / "final.png"
                driver.save_screenshot(str(screenshot)); paths.append(str(screenshot))
            except Exception:
                result["result"] = "blocked"
                observation["evidenceDiagnostic"] = traceback.format_exc()
            finally:
                driver.quit()
        observation.update(result={key: value for key, value in result.items() if key != "loadedAssetSha256"}, loadedAssetSha256=result.get("loadedAssetSha256", {}))
        path = directory / "observation.json"
        path.write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(path)); result["evidence_paths"] = paths
        support.record_case(requirement, host, result, directory)
    assert result["result"] == "passed", f"{host} {requirement}: {observation.get('diagnostic', '')}; evidence: {directory}"
