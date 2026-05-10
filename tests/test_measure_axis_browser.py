import os
import re
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
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
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


def _build_measure_axis_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "measure_axis_browser",
        pa.Table.from_pydict(
            {
                "portfolio": ["Book", "Book", "Hedge", "Hedge"],
                "Metric": ["physical-metric-a", "physical-metric-a", "physical-metric-b", "physical-metric-b"],
                "delta": [100.0, 20.0, -80.0, -20.0],
                "gamma": [25.0, 35.0, -10.0, -20.0],
                "price": [2.0, 10.0, 4.0, 8.0],
                "quantity": [1.0, 3.0, 2.0, 2.0],
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
                table="measure_axis_browser",
                serverSide=True,
                rowFields=["Measure Name"],
                colFields=["portfolio"],
                valConfigs=[
                    {"field": "delta", "agg": "sum", "alias": "delta_sum", "label": "Delta Sum"},
                    {"field": "gamma", "agg": "avg", "alias": "gamma_avg", "label": "Gamma Avg"},
                    {
                        "field": "price",
                        "agg": "weighted_avg",
                        "alias": "weighted_price",
                        "weightField": "quantity",
                        "label": "Weighted Price",
                    },
                ],
                measureAxis={
                    "placement": "rows",
                    "labelField": "Measure Name",
                    "valueField": "Amount",
                    "members": [
                        {"measureAlias": "delta_sum", "label": "Delta Sum", "order": 0},
                        {"measureAlias": "gamma_avg", "label": "Gamma Avg", "order": 1},
                        {"measureAlias": "weighted_price", "label": "Weighted Price", "order": 2},
                    ],
                },
                filters={},
                sorting=[],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["portfolio", "delta", "gamma", "price", "quantity", "Metric"],
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


@pytest.fixture
def measure_axis_browser_server():
    app = _build_measure_axis_app()
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
        except Exception as exc:
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
    options.set_capability("goog:loggingPrefs", {"browser": "ALL"})

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


def _numeric_text_value(text):
    compact = " ".join(str(text or "").split())
    match = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", compact)
    if not match:
        return None
    return float(match.group(0).replace(",", ""))


def _wait_for_numeric_cell(driver, row_id, col_id, expected, timeout=30):
    selector = _cell_selector(row_id, col_id)
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))

    def _matches(_driver):
        element = _driver.find_element(By.CSS_SELECTOR, selector)
        value = _numeric_text_value(element.text)
        return element if value is not None and abs(value - expected) < 1e-9 else False

    return wait.until(_matches)


def _severe_browser_logs(driver):
    try:
        entries = driver.get_log("browser")
    except (ValueError, WebDriverException):
        return []
    return [
        entry
        for entry in entries
        if str(entry.get("level", "")).upper() in {"SEVERE", "ERROR"}
    ]


def test_measure_axis_rows_browser_renders_aggregate_first_pivot(measure_axis_browser_server, chrome_driver):
    chrome_driver.get(measure_axis_browser_server)

    values_as_select = WebDriverWait(chrome_driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Values placement"]'))
    )
    assert values_as_select.get_attribute("value") == "rows"

    _wait_for_numeric_cell(chrome_driver, "Delta Sum", "Book_Amount", 120.0)
    _wait_for_numeric_cell(chrome_driver, "Delta Sum", "Hedge_Amount", -100.0)
    _wait_for_numeric_cell(chrome_driver, "Gamma Avg", "Book_Amount", 30.0)
    _wait_for_numeric_cell(chrome_driver, "Gamma Avg", "Hedge_Amount", -15.0)
    _wait_for_numeric_cell(chrome_driver, "Weighted Price", "Book_Amount", 8.0)
    _wait_for_numeric_cell(chrome_driver, "Weighted Price", "Hedge_Amount", 6.0)

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []
