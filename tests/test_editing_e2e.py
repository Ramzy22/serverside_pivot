import asyncio
import os
import socket
import sys
import threading
import time

import pyarrow as pa
import pytest
import requests
from dash import Dash, html
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from werkzeug.serving import make_server

sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "pivot_engine"))
sys.path.append(os.path.join(os.getcwd(), "dash_tanstack_pivot"))

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app


class _DashServerThread(threading.Thread):
    def __init__(self, app, host, port):
        super().__init__(daemon=True)
        self._server = make_server(host, port, app.server, threaded=True)

    def run(self):
        self._server.serve_forever()

    def shutdown(self):
        self._server.shutdown()
        self._server.server_close()


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _build_editing_app(
    transaction_delay_seconds=0.0,
    view_state=None,
    val_configs=None,
    editing_config=None,
    component_id="pivot-grid",
    persistence=None,
):
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "North", "South"],
                "country": ["USA", "USA", "Canada", "Brazil"],
                "sales": [100.0, 20.0, 80.0, 120.0],
                "cost": [10.0, 2.0, 8.0, 12.0],
            }
        ),
    )
    if transaction_delay_seconds > 0:
        original_apply_row_transaction = adapter.controller.apply_row_transaction

        async def delayed_apply_row_transaction(*args, **kwargs):
            await asyncio.sleep(transaction_delay_seconds)
            return await original_apply_row_transaction(*args, **kwargs)

        adapter.controller.apply_row_transaction = delayed_apply_row_transaction

    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "12px"},
        children=[
            DashTanstackPivot(
                id=component_id,
                style={"height": "560px", "width": "100%"},
                table="sales_data",
                serverSide=True,
                rowFields=["region", "country"],
                colFields=[],
                valConfigs=val_configs or [{"field": "sales", "agg": "sum"}],
                filters={},
                sorting=[],
                expanded={"North": True, "South": True},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "country", "sales", "cost"],
                editingConfig=editing_config,
                validationRules={
                    "sales_sum": [{"type": "numeric"}, {"type": "min", "value": 0}],
                    "cost_sum": [{"type": "numeric"}, {"type": "min", "value": 0}],
                },
                persistence=persistence,
                viewState=view_state,
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id=component_id, debug=False)
    return app


def _build_chart_app():
    months = [f"2024-{index:02d}" for index in range(1, 37)] + [f"2025-{index:02d}" for index in range(1, 37)]
    data = []
    for region_index, region in enumerate(["North", "South", "West"]):
        for month_index, month in enumerate(months):
            data.append(
                {
                    "region": region,
                    "month": month,
                    "sales": float((month_index + 1) * (region_index + 2) * 11),
                    "cost": float((month_index + 1) * (region_index + 1) * 4),
                }
            )

    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "12px"},
        children=[
            DashTanstackPivot(
                id="pivot-grid",
                style={"height": "680px", "width": "100%"},
                data=data,
                serverSide=False,
                rowFields=["region"],
                colFields=["month"],
                valConfigs=[{"field": "sales", "agg": "sum"}],
                filters={},
                sorting=[],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "month", "sales", "cost"],
                chartCanvasPanes=[
                    {
                        "id": "chart-pane-1",
                        "name": "Chart Pane 1",
                        "chartTitle": "Chart Pane 1",
                        "source": "pivot",
                        "chartType": "bar",
                        "barLayout": "grouped",
                        "axisMode": "vertical",
                        "orientation": "rows",
                        "hierarchyLevel": "all",
                        "rowLimit": 24,
                        "columnLimit": 24,
                        "width": 430,
                        "chartHeight": 280,
                        "sortMode": "natural",
                        "interactionMode": "focus",
                        "serverScope": "viewport",
                        "size": 1,
                        "floating": False,
                    }
                ],
                tableCanvasSize=1.15,
            )
        ],
    )
    return app


def _build_sparkline_chart_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "chart_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South", "West", "West"],
                "month": ["Jan", "Feb", "Jan", "Feb", "Jan", "Feb"],
                "sales": [120.0, 150.0, 180.0, 190.0, 140.0, 160.0],
                "cost": [50.0, 60.0, 65.0, 70.0, 58.0, 61.0],
            }
        ),
    )
    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "12px"},
        children=[
            DashTanstackPivot(
                id="pivot-grid",
                style={"height": "620px", "width": "100%"},
                table="chart_data",
                serverSide=True,
                rowFields=["region"],
                colFields=["month"],
                valConfigs=[{"field": "sales", "agg": "sum"}, {"field": "cost", "agg": "sum"}],
                filters={},
                sorting=[],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "month", "sales", "cost"],
                chartCanvasPanes=[
                    {
                        "id": "chart-pane-1",
                        "name": "Chart Pane 1",
                        "chartTitle": "Chart Pane 1",
                        "source": "pivot",
                        "chartType": "bar",
                        "barLayout": "grouped",
                        "axisMode": "vertical",
                        "orientation": "rows",
                        "hierarchyLevel": "all",
                        "rowLimit": 12,
                        "columnLimit": 12,
                        "width": 430,
                        "chartHeight": 280,
                        "sortMode": "natural",
                        "interactionMode": "focus",
                        "serverScope": "viewport",
                        "size": 1,
                        "floating": False,
                    }
                ],
                tableCanvasSize=1.2,
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def _build_cell_sparkline_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sparkline_cells",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "North", "North", "South", "South", "South", "South"],
                "month": ["2024-01", "2024-02", "2024-03", "2024-04", "2024-01", "2024-02", "2024-03", "2024-04"],
                "sales": [120.0, 150.0, 132.0, 180.0, 90.0, 110.0, 108.0, 140.0],
            }
        ),
    )
    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "12px"},
        children=[
            DashTanstackPivot(
                id="pivot-grid",
                style={"height": "560px", "width": "100%"},
                table="sparkline_cells",
                serverSide=True,
                rowFields=["region"],
                colFields=["month"],
                valConfigs=[{
                    "field": "sales",
                    "agg": "sum",
                    "sparkline": {
                        "type": "line",
                        "showCurrentValue": True,
                        "showDelta": True,
                    },
                }],
                filters={},
                sorting=[],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "month", "sales"],
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


@pytest.fixture
def editing_e2e_server():
    app = _build_editing_app(transaction_delay_seconds=0.85)
    host = "127.0.0.1"
    port = _find_free_port()
    server = _DashServerThread(app, host, port)
    server.start()

    base_url = f"http://{host}:{port}"
    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                break
        except Exception as exc:  # pragma: no cover - diagnostic only
            last_error = exc
        time.sleep(0.2)
    else:
        server.shutdown()
        raise RuntimeError(f"Dash test server did not start: {last_error}")

    try:
        yield base_url
    finally:
        server.shutdown()


@pytest.fixture
def row_editing_e2e_server():
    app = _build_editing_app(
        transaction_delay_seconds=0.85,
        val_configs=[{"field": "sales", "agg": "sum"}, {"field": "cost", "agg": "sum"}],
        editing_config={
            "mode": "hybrid",
            "rowActions": True,
            "columns": {
                "sales_sum": {"editor": "number", "step": 1, "min": 0},
                "cost_sum": {"editor": "number", "step": 1, "min": 0},
            },
        },
    )
    host = "127.0.0.1"
    port = _find_free_port()
    server = _DashServerThread(app, host, port)
    server.start()

    base_url = f"http://{host}:{port}"
    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                break
        except Exception as exc:  # pragma: no cover - diagnostic only
            last_error = exc
        time.sleep(0.2)
    else:
        server.shutdown()
        raise RuntimeError(f"Dash test server did not start: {last_error}")

    try:
        yield base_url
    finally:
        server.shutdown()


@pytest.fixture
def persisted_editing_e2e_server():
    app = _build_editing_app(
        transaction_delay_seconds=0.85,
        component_id="pivot-grid-persisted-edit-state",
        persistence=True,
    )
    host = "127.0.0.1"
    port = _find_free_port()
    server = _DashServerThread(app, host, port)
    server.start()

    base_url = f"http://{host}:{port}"
    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                break
        except Exception as exc:  # pragma: no cover - diagnostic only
            last_error = exc
        time.sleep(0.2)
    else:
        server.shutdown()
        raise RuntimeError(f"Dash test server did not start: {last_error}")

    try:
        yield base_url
    finally:
        server.shutdown()


@pytest.fixture
def formatted_editing_e2e_server():
    app = _build_editing_app(
        transaction_delay_seconds=0.85,
        view_state={
            "cellFormatRules": {
                "North:::sales_sum": {"bg": "#FFFFFF", "color": "#111827"},
                "North|||USA:::sales_sum": {"bg": "#FFFFFF", "color": "#111827"},
            }
        },
    )
    host = "127.0.0.1"
    port = _find_free_port()
    server = _DashServerThread(app, host, port)
    server.start()

    base_url = f"http://{host}:{port}"
    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                break
        except Exception as exc:  # pragma: no cover - diagnostic only
            last_error = exc
        time.sleep(0.2)
    else:
        server.shutdown()
        raise RuntimeError(f"Dash test server did not start: {last_error}")

    try:
        yield base_url
    finally:
        server.shutdown()


@pytest.fixture
def themed_editing_e2e_server():
    app = _build_editing_app(
        transaction_delay_seconds=0.85,
        view_state={
            "themeName": "flash",
            "themeOverrides": {
                "editedCellBg": "#FCE7A8",
            },
        },
    )
    host = "127.0.0.1"
    port = _find_free_port()
    server = _DashServerThread(app, host, port)
    server.start()

    base_url = f"http://{host}:{port}"
    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                break
        except Exception as exc:  # pragma: no cover - diagnostic only
            last_error = exc
        time.sleep(0.2)
    else:
        server.shutdown()
        raise RuntimeError(f"Dash test server did not start: {last_error}")

    try:
        yield base_url
    finally:
        server.shutdown()


@pytest.fixture
def chart_e2e_server():
    app = _build_chart_app()
    host = "127.0.0.1"
    port = _find_free_port()
    server = _DashServerThread(app, host, port)
    server.start()

    base_url = f"http://{host}:{port}"
    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                break
        except Exception as exc:  # pragma: no cover - diagnostic only
            last_error = exc
        time.sleep(0.2)
    else:
        server.shutdown()
        raise RuntimeError(f"Dash test server did not start: {last_error}")

    try:
        yield base_url
    finally:
        server.shutdown()


@pytest.fixture
def sparkline_chart_e2e_server():
    app = _build_sparkline_chart_app()
    host = "127.0.0.1"
    port = _find_free_port()
    server = _DashServerThread(app, host, port)
    server.start()

    base_url = f"http://{host}:{port}"
    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                break
        except Exception as exc:  # pragma: no cover - diagnostic only
            last_error = exc
        time.sleep(0.2)
    else:
        server.shutdown()
        raise RuntimeError(f"Dash test server did not start: {last_error}")

    try:
        yield base_url
    finally:
        server.shutdown()


@pytest.fixture
def cell_sparkline_e2e_server():
    app = _build_cell_sparkline_app()
    host = "127.0.0.1"
    port = _find_free_port()
    server = _DashServerThread(app, host, port)
    server.start()

    base_url = f"http://{host}:{port}"
    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                break
        except Exception as exc:  # pragma: no cover - diagnostic only
            last_error = exc
        time.sleep(0.2)
    else:
        server.shutdown()
        raise RuntimeError(f"Dash test server did not start: {last_error}")

    try:
        yield base_url
    finally:
        server.shutdown()


@pytest.fixture
def chrome_driver():
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1680,1200")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--force-device-scale-factor=1")

    try:
        driver = webdriver.Chrome(options=options)
    except WebDriverException as exc:
        pytest.skip(f"Chrome WebDriver is not available in this environment: {exc}")
        return

    driver.set_page_load_timeout(30)
    try:
        yield driver
    finally:
        driver.quit()


def _cell_selector(row_id, col_id):
    return f'[role="gridcell"][data-rowid="{row_id}"][data-colid="{col_id}"]'


def _wait_for_cell_text(driver, row_id, col_id, expected_text, timeout=20):
    selector = _cell_selector(row_id, col_id)
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))

    def _matches(_driver):
        element = _driver.find_element(By.CSS_SELECTOR, selector)
        text = " ".join(element.text.split())
        return element if text == expected_text else False

    return wait.until(_matches)


def _wait_for_cell_absent(driver, row_id, col_id, timeout=20):
    selector = _cell_selector(row_id, col_id)
    wait = WebDriverWait(driver, timeout)
    return wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, selector)))


def _get_cell_css_value(driver, row_id, col_id, property_name):
    selector = _cell_selector(row_id, col_id)
    element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
    )
    return element.value_of_css_property(property_name)


def _get_cell_surface_css_value(driver, row_id, col_id, property_name):
    selector = f'{_cell_selector(row_id, col_id)} [data-display-rowid="{row_id}"][data-display-colid="{col_id}"]'
    element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
    )
    return element.value_of_css_property(property_name)


def _wait_for_button_enabled(driver, title_text, timeout=20):
    selector = f'button[title="{title_text}"]'
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))

    def _enabled(_driver):
        button = _driver.find_element(By.CSS_SELECTOR, selector)
        disabled = button.get_attribute("disabled")
        return button if disabled in (None, "false") else False

    return wait.until(_enabled)


def _wait_for_value_mode_enabled(driver, mode, timeout=20):
    selector = f'button[data-edit-value-mode="{mode}"]'
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))

    def _enabled(_driver):
        button = _driver.find_element(By.CSS_SELECTOR, selector)
        disabled = button.get_attribute("disabled")
        return button if disabled in (None, "false") else False

    return wait.until(_enabled)


def _wait_for_value_mode_pressed(driver, mode, pressed, timeout=20):
    selector = f'button[data-edit-value-mode="{mode}"]'
    wait = WebDriverWait(driver, timeout)

    def _pressed(_driver):
        button = _driver.find_element(By.CSS_SELECTOR, selector)
        return button if button.get_attribute("aria-pressed") == ("true" if pressed else "false") else False

    return wait.until(_pressed)


def _assert_no_loading_indicator(driver):
    indicators = driver.find_elements(By.CSS_SELECTOR, '[data-pivot-loading-indicator]')
    visible_indicators = [indicator for indicator in indicators if indicator.is_displayed()]
    assert visible_indicators == []


def _edit_cell_value(driver, row_id, col_id, next_value, propagation_formula=None):
    input_selector = f'input[data-edit-rowid="{row_id}"][data-edit-colid="{col_id}"]'
    input_elements = driver.find_elements(By.CSS_SELECTOR, input_selector)
    if input_elements:
        input_el = input_elements[0]
    else:
        display_selector = f'{_cell_selector(row_id, col_id)} [data-display-rowid="{row_id}"][data-display-colid="{col_id}"]'
        cell_display = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, display_selector))
        )
        ActionChains(driver).double_click(cell_display).perform()
        input_el = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, input_selector))
      )
    input_el.send_keys(Keys.CONTROL, "a")
    input_el.send_keys(str(next_value))
    input_el.send_keys(Keys.ENTER)
    # Propagation method is selected via the EditSidePanel instead of browser prompt.
    # Only look for the panel if we expect propagation (aggregate edit).
    try:
        panel = WebDriverWait(driver, 2.0).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-pivot-edit-panel="true"] input[name="propagation-method"]'))
        )
        panel_root = driver.find_element(By.CSS_SELECTOR, '[data-pivot-edit-panel="true"]')
        if propagation_formula == "proportional":
            radio = panel_root.find_element(By.CSS_SELECTOR, 'input[value="proportional"]')
            radio.click()
        for btn in panel_root.find_elements(By.CSS_SELECTOR, 'button'):
            if 'Apply' in btn.text:
                btn.click()
                break
    except TimeoutException:
        pass


def _click_cell(driver, row_id, col_id):
    cell = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, _cell_selector(row_id, col_id)))
    )
    cell.click()


def _toggle_hierarchy_row(driver, row_id):
    button_selector = f'{_cell_selector(row_id, "hierarchy")} button'
    button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, button_selector))
    )
    button.click()


def _click_row_edit_action(driver, row_id, action, timeout=20):
    selector = f'button[data-row-edit-action="{action}"][data-row-edit-rowid="{row_id}"]'
    button = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
    )
    button.click()
    return button


def _click_button_by_text(driver, text, timeout=20):
    xpath = f'//button[normalize-space()="{text}"]'
    button = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )
    button.click()
    return button


def _open_chart_pane(driver):
    WebDriverWait(driver, 20).until(
        lambda current_driver: current_driver.execute_script(
            """
            const toggle = document.querySelector('button[data-toolbar-section-toggle="charts"]')
                || Array.from(document.querySelectorAll('button')).find((button) => button.textContent.trim() === 'Charts');
            if (!toggle) return false;
            toggle.click();
            return true;
            """
        )
    )
    WebDriverWait(driver, 20).until(
        lambda current_driver: current_driver.execute_script(
            """
            const addButton = document.querySelector('button[data-add-chart-pane="true"]')
                || Array.from(document.querySelectorAll('button')).find((button) => {
                    const title = button.getAttribute('title') || '';
                    return title.includes('Add a resizable chart pane') || button.textContent.includes('New Chart Pane');
                });
            if (!addButton) return false;
            addButton.click();
            return true;
            """
        )
    )
    return WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-docked-chart-pane]'))
    )


def _visible_header_ids(driver):
    return driver.execute_script(
        """
        return Array.from(document.querySelectorAll('[role="columnheader"]'))
            .filter((el) => {
                const style = window.getComputedStyle(el);
                return style.display !== 'none'
                    && style.visibility !== 'hidden'
                    && el.getClientRects().length > 0;
            })
            .map((el, index) => el.getAttribute('data-header-column-id') || `header-${index}`);
        """
    )


def _element_width(driver, selector):
    return driver.execute_script(
        """
        const node = document.querySelector(arguments[0]);
        if (!node) return null;
        return node.getBoundingClientRect().width;
        """,
        selector,
    )


def test_inline_aggregate_edit_propagates_and_undo_redo_round_trips(editing_e2e_server, chrome_driver):
    chrome_driver.get(editing_e2e_server)

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")
    initial_leaf_background = _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color")
    initial_parent_background = _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-color")
    edited_leaf_background = None
    edited_parent_background = None

    _edit_cell_value(chrome_driver, "North|||USA", "sales_sum", 180)

    for _ in range(3):
        time.sleep(0.08)
        assert chrome_driver.find_element(By.CSS_SELECTOR, _cell_selector("North|||USA", "sales_sum")).text.strip() == "180"
        _assert_no_loading_indicator(chrome_driver)
        edited_leaf_background = _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color")
        assert edited_leaf_background != initial_leaf_background

    _click_cell(chrome_driver, "South", "sales_sum")

    for _ in range(5):
        time.sleep(0.12)
        assert chrome_driver.find_element(By.CSS_SELECTOR, _cell_selector("North|||USA", "sales_sum")).text.strip() == "180"
        assert chrome_driver.find_element(By.CSS_SELECTOR, _cell_selector("North", "sales_sum")).text.strip() == "260"
        _assert_no_loading_indicator(chrome_driver)
        edited_leaf_background = _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color")
        edited_parent_background = _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-color")
        assert edited_leaf_background != initial_leaf_background
        assert edited_parent_background != initial_parent_background

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "180")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")
    _assert_no_loading_indicator(chrome_driver)
    edited_leaf_background = _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color")
    edited_parent_background = _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-color")
    assert edited_leaf_background != initial_leaf_background
    assert edited_parent_background != initial_parent_background
    for _ in range(4):
        time.sleep(0.12)
        assert _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color") == edited_leaf_background
        assert _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-color") == edited_parent_background

    undo_button = _wait_for_button_enabled(
        chrome_driver,
        "Undo the last edit or layout change (Ctrl/Cmd+Z)",
    )
    undo_button.click()

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")
    _click_cell(chrome_driver, "South", "sales_sum")
    assert _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color") != edited_leaf_background
    assert _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-color") != edited_parent_background

    redo_button = _wait_for_button_enabled(
        chrome_driver,
        "Redo the last edit or layout change (Ctrl+Y or Cmd/Ctrl+Shift+Z)",
    )
    redo_button.click()

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "180")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")
    _click_cell(chrome_driver, "South", "sales_sum")
    assert _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color") == edited_leaf_background
    assert _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-color") == edited_parent_background


def test_row_mode_double_click_edit_saves_without_explicit_start_button(row_editing_e2e_server, chrome_driver):
    chrome_driver.get(row_editing_e2e_server)

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North|||USA", "cost_sum", "12")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")
    _wait_for_cell_text(chrome_driver, "North", "cost_sum", "20")
    assert chrome_driver.find_elements(By.CSS_SELECTOR, 'button[data-row-edit-action="start"]') == []

    _edit_cell_value(chrome_driver, "North|||USA", "sales_sum", 150)
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "150")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "230")
    _assert_no_loading_indicator(chrome_driver)

    _edit_cell_value(chrome_driver, "North|||USA", "cost_sum", 18)
    _wait_for_cell_text(chrome_driver, "North|||USA", "cost_sum", "18")
    _wait_for_cell_text(chrome_driver, "North", "cost_sum", "26")
    _assert_no_loading_indicator(chrome_driver)

    undo_button = _wait_for_button_enabled(
        chrome_driver,
        "Undo the last edit or layout change (Ctrl/Cmd+Z)",
    )
    undo_button.click()

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "150")
    _wait_for_cell_text(chrome_driver, "North|||USA", "cost_sum", "12")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "230")
    _wait_for_cell_text(chrome_driver, "North", "cost_sum", "20")

    undo_button = _wait_for_button_enabled(
        chrome_driver,
        "Undo the last edit or layout change (Ctrl/Cmd+Z)",
    )
    undo_button.click()

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")

    redo_button = _wait_for_button_enabled(
        chrome_driver,
        "Redo the last edit or layout change (Ctrl+Y or Cmd/Ctrl+Shift+Z)",
    )
    redo_button.click()

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "150")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "230")

    redo_button = _wait_for_button_enabled(
        chrome_driver,
        "Redo the last edit or layout change (Ctrl+Y or Cmd/Ctrl+Shift+Z)",
    )
    redo_button.click()

    _wait_for_cell_text(chrome_driver, "North|||USA", "cost_sum", "18")
    _wait_for_cell_text(chrome_driver, "North", "cost_sum", "26")


def test_expand_collapse_undo_redo_round_trips(editing_e2e_server, chrome_driver):
    chrome_driver.get(editing_e2e_server)

    _wait_for_cell_text(chrome_driver, "North", "hierarchy", "North")
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")

    _toggle_hierarchy_row(chrome_driver, "North")
    _wait_for_cell_absent(chrome_driver, "North|||USA", "sales_sum")

    undo_button = _wait_for_button_enabled(
        chrome_driver,
        "Undo the last edit or layout change (Ctrl/Cmd+Z)",
    )
    undo_button.click()

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")

    redo_button = _wait_for_button_enabled(
        chrome_driver,
        "Redo the last edit or layout change (Ctrl+Y or Cmd/Ctrl+Shift+Z)",
    )
    redo_button.click()

    _wait_for_cell_absent(chrome_driver, "North|||USA", "sales_sum")


def test_parent_edit_does_not_mark_children_as_edited(editing_e2e_server, chrome_driver):
    chrome_driver.get(editing_e2e_server)

    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North|||Canada", "sales_sum", "80")

    initial_parent_overlay = _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-image")
    initial_usa_overlay = _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image")
    initial_canada_overlay = _get_cell_css_value(chrome_driver, "North|||Canada", "sales_sum", "background-image")

    _edit_cell_value(chrome_driver, "North", "sales_sum", 260)
    _click_cell(chrome_driver, "South", "sales_sum")

    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "160")
    _wait_for_cell_text(chrome_driver, "North|||Canada", "sales_sum", "100")
    _assert_no_loading_indicator(chrome_driver)

    parent_overlay = _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-image")
    usa_overlay = _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image")
    canada_overlay = _get_cell_css_value(chrome_driver, "North|||Canada", "sales_sum", "background-image")

    assert parent_overlay != initial_parent_overlay
    assert parent_overlay != "none"
    assert usa_overlay == initial_usa_overlay
    assert canada_overlay == initial_canada_overlay
    assert usa_overlay == "none"
    assert canada_overlay == "none"


def test_edited_overlay_survives_existing_cell_formatting(formatted_editing_e2e_server, chrome_driver):
    chrome_driver.get(formatted_editing_e2e_server)

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")

    assert _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color") == "rgba(255, 255, 255, 1)"
    assert _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-color") == "rgba(255, 255, 255, 1)"
    assert _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image") == "none"
    assert _get_cell_surface_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image") == "none"

    _edit_cell_value(chrome_driver, "North|||USA", "sales_sum", 180)
    _click_cell(chrome_driver, "South", "sales_sum")

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "180")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")

    leaf_overlay = _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image")
    parent_overlay = _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-image")
    leaf_surface_overlay = _get_cell_surface_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image")
    assert leaf_overlay != "none"
    assert parent_overlay != "none"
    assert leaf_surface_overlay == "none"

    for _ in range(4):
        time.sleep(0.12)
        assert _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image") == leaf_overlay
        assert _get_cell_css_value(chrome_driver, "North", "sales_sum", "background-image") == parent_overlay
        assert _get_cell_surface_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image") == leaf_surface_overlay


def test_custom_edited_theme_background_updates_outer_cell_without_stale_inner_overlay(themed_editing_e2e_server, chrome_driver):
    chrome_driver.get(themed_editing_e2e_server)

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")

    _edit_cell_value(chrome_driver, "North|||USA", "sales_sum", 180)
    _click_cell(chrome_driver, "South", "sales_sum")

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "180")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")

    expected_background = "rgba(252, 231, 168, 1)"
    expected_surface_background = "rgba(0, 0, 0, 0)"
    for _ in range(8):
        time.sleep(0.18)
        assert _get_cell_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color") == expected_background
        assert _get_cell_surface_css_value(chrome_driver, "North|||USA", "sales_sum", "background-color") == expected_surface_background
        assert _get_cell_surface_css_value(chrome_driver, "North|||USA", "sales_sum", "background-image") == "none"


def test_original_edited_value_switch_restores_parent_and_visible_children(editing_e2e_server, chrome_driver):
    chrome_driver.get(editing_e2e_server)

    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North|||Canada", "sales_sum", "80")

    _wait_for_value_mode_pressed(chrome_driver, "original", True)
    _wait_for_value_mode_pressed(chrome_driver, "edited", False)

    _edit_cell_value(chrome_driver, "North", "sales_sum", 260)
    _click_cell(chrome_driver, "South", "sales_sum")

    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "160")
    _wait_for_cell_text(chrome_driver, "North|||Canada", "sales_sum", "100")
    _wait_for_value_mode_pressed(chrome_driver, "edited", True)


def test_reload_preserves_original_vs_edited_baseline_for_saved_values(persisted_editing_e2e_server, chrome_driver):
    chrome_driver.get(persisted_editing_e2e_server)
    chrome_driver.execute_script(
        """
        const prefix = 'pivot-grid-persisted-edit-state-';
        Object.keys(window.localStorage).forEach((key) => {
            if (key.startsWith(prefix)) {
                window.localStorage.removeItem(key);
            }
        });
        """
    )
    chrome_driver.refresh()

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")
    _wait_for_value_mode_pressed(chrome_driver, "original", True)

    _edit_cell_value(chrome_driver, "North|||USA", "sales_sum", 180)
    _click_cell(chrome_driver, "South", "sales_sum")

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "180")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")
    _wait_for_value_mode_pressed(chrome_driver, "edited", True)

    time.sleep(0.3)
    chrome_driver.refresh()

    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "180")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")
    _wait_for_value_mode_enabled(chrome_driver, "original").click()
    _wait_for_value_mode_pressed(chrome_driver, "original", True)
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")

    _wait_for_value_mode_enabled(chrome_driver, "edited").click()
    _wait_for_value_mode_pressed(chrome_driver, "edited", True)
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "180")
    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")


def test_parent_aggregate_edit_prompt_accepts_proportional_formula(editing_e2e_server, chrome_driver):
    chrome_driver.get(editing_e2e_server)

    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "200")
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "120")
    _wait_for_cell_text(chrome_driver, "North|||Canada", "sales_sum", "80")

    _edit_cell_value(chrome_driver, "North", "sales_sum", 260, propagation_formula="proportional")
    _click_cell(chrome_driver, "South", "sales_sum")

    _wait_for_cell_text(chrome_driver, "North", "sales_sum", "260")
    _wait_for_cell_text(chrome_driver, "North|||USA", "sales_sum", "156")
    _wait_for_cell_text(chrome_driver, "North|||Canada", "sales_sum", "104")


def test_chart_settings_open_immediately_and_table_refills_after_close(chart_e2e_server, chrome_driver):
    chrome_driver.get(chart_e2e_server)

    WebDriverWait(chrome_driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-docked-chart-pane]'))
    )
    initial_table_width = _element_width(chrome_driver, '[data-docked-table-canvas]')
    initial_chart_pane_width = _element_width(chrome_driver, '[data-docked-chart-pane]')
    initial_chart_surface_width = _element_width(chrome_driver, '[data-chart-surface-main="true"]')
    assert initial_table_width is not None
    assert initial_chart_pane_width is not None
    assert initial_chart_surface_width is not None

    settings_toggle = WebDriverWait(chrome_driver, 20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-chart-settings-toggle="true"]'))
    )
    settings_toggle.click()

    settings_pane = WebDriverWait(chrome_driver, 20).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, '[data-chart-settings-pane="true"]'))
    )
    assert settings_pane.text.find("Chart Settings") >= 0
    settings_pane_width = _element_width(chrome_driver, '[data-chart-settings-pane="true"]')
    assert settings_pane_width >= 300
    assert len(chrome_driver.find_elements(By.CSS_SELECTOR, '[data-chart-settings-pane="true"]')) == 1
    time.sleep(0.8)
    expanded_chart_pane_width = _element_width(chrome_driver, '[data-docked-chart-pane]')
    expanded_chart_surface_width = _element_width(chrome_driver, '[data-chart-surface-main="true"]')
    assert expanded_chart_pane_width >= initial_chart_pane_width + (settings_pane_width * 0.75)
    assert expanded_chart_surface_width >= initial_chart_surface_width - 24
    settings_scroll = chrome_driver.find_element(By.CSS_SELECTOR, '[data-chart-settings-scroll="true"]')
    chart_surface_scroll = chrome_driver.find_element(By.CSS_SELECTOR, '[data-chart-surface-scroll="true"]')
    docked_chart_pane = chrome_driver.find_element(By.CSS_SELECTOR, '[data-docked-chart-pane]')
    initial_chart_surface_scroll_top = chrome_driver.execute_script("return arguments[0].scrollTop;", chart_surface_scroll)
    initial_docked_chart_scroll_top = chrome_driver.execute_script("return arguments[0].scrollTop;", docked_chart_pane)
    scroll_metrics = chrome_driver.execute_script(
        "arguments[0].scrollTop = 260; return {top: arguments[0].scrollTop, max: arguments[0].scrollHeight - arguments[0].clientHeight};",
        settings_scroll,
    )
    assert scroll_metrics["max"] > 0
    WebDriverWait(chrome_driver, 20).until(
        lambda driver: driver.execute_script("return arguments[0].scrollTop;", settings_scroll) > 0
    )
    assert chrome_driver.execute_script("return arguments[0].scrollTop;", chart_surface_scroll) == initial_chart_surface_scroll_top
    assert chrome_driver.execute_script("return arguments[0].scrollTop;", docked_chart_pane) == initial_docked_chart_scroll_top

    close_button = WebDriverWait(chrome_driver, 20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-docked-chart-pane] button[title="Close chart pane"]'))
    )
    close_button.click()

    WebDriverWait(chrome_driver, 20).until(
        lambda driver: len(driver.find_elements(By.CSS_SELECTOR, '[data-docked-chart-pane]')) == 0
    )
    restored_table_width = WebDriverWait(chrome_driver, 20).until(
        lambda driver: _element_width(driver, '[data-docked-table-canvas]')
        if (_element_width(driver, '[data-docked-table-canvas]') or 0) > initial_table_width + 120
        else False
    )
    assert restored_table_width > initial_table_width


def test_chart_sparkline_mode_renders_multi_series_board(sparkline_chart_e2e_server, chrome_driver):
    chrome_driver.get(sparkline_chart_e2e_server)

    WebDriverWait(chrome_driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-docked-chart-pane]'))
    )
    WebDriverWait(chrome_driver, 20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-chart-settings-toggle="true"]'))
    ).click()

    WebDriverWait(chrome_driver, 20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-chart-type="sparkline"]'))
    ).click()

    WebDriverWait(chrome_driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-chart-sparkline-board="true"]'))
    )
    assert len(chrome_driver.find_elements(By.CSS_SELECTOR, '[data-docked-chart-pane]')) == 1
    assert len(chrome_driver.find_elements(By.CSS_SELECTOR, '[data-chart-sparkline-surface="true"]')) == 1
    sparkline_cards = chrome_driver.find_elements(By.CSS_SELECTOR, '[data-chart-sparkline-card]')
    assert len(sparkline_cards) >= 2


def test_pivot_cell_sparkline_summary_column_renders_row_trend(cell_sparkline_e2e_server, chrome_driver):
    chrome_driver.get(cell_sparkline_e2e_server)

    sparkline_selector = f'{_cell_selector("North", "__sparkline__sales_sum")} [data-pivot-sparkline-cell="true"]'
    sparkline_cell = WebDriverWait(chrome_driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, sparkline_selector))
    )

    assert sparkline_cell.get_attribute("data-pivot-sparkline-type") == "line"
    assert sparkline_cell.get_attribute("data-pivot-sparkline-points") == "4"

    current_value = chrome_driver.find_element(
        By.CSS_SELECTOR,
        f'{sparkline_selector} [data-pivot-sparkline-current="true"]'
    ).text.strip()
    delta_value = chrome_driver.find_element(
        By.CSS_SELECTOR,
        f'{sparkline_selector} [data-pivot-sparkline-delta="true"]'
    ).text.strip()

    ordered_visible_values = chrome_driver.execute_script(
        """
        return Array.from(document.querySelectorAll('[role="gridcell"][data-rowid="North"]'))
            .filter((cell) => {
                const columnId = cell.getAttribute('data-colid') || '';
                return columnId.endsWith('_sales_sum')
                    && !columnId.startsWith('__sparkline__')
                    && !columnId.startsWith('__RowTotal__');
            })
            .map((cell) => Number(cell.textContent.trim()))
            .filter((value) => Number.isFinite(value));
        """
    )

    assert len(ordered_visible_values) == 4
    assert current_value == str(int(ordered_visible_values[-1]))
    assert delta_value == f"{ordered_visible_values[-1] - ordered_visible_values[-2]:+g}"
