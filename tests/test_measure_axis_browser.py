import os
import re
import socket
import sys
import threading
import time

import pyarrow as pa
import pytest
import requests
from dash import Dash, Input, Output, callback_context, dcc, html
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, WebDriverException
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


def _build_measure_axis_hierarchy_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "measure_axis_hierarchy_browser",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "desk": ["Rates", "Credit", "Rates", "Credit"],
                "sales": [100.0, 50.0, 80.0, 20.0],
                "cost": [40.0, 10.0, 30.0, 5.0],
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
                table="measure_axis_hierarchy_browser",
                serverSide=True,
                rowFields=["region", "Measure Name", "desk"],
                colFields=[],
                valConfigs=[
                    {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
                    {"field": "cost", "agg": "sum", "alias": "cost_sum", "label": "Cost"},
                ],
                measureAxis={
                    "placement": "rows",
                    "labelField": "Measure Name",
                    "valueField": "Amount",
                    "members": [
                        {"measureAlias": "sales_sum", "label": "Sales", "order": 0},
                        {"measureAlias": "cost_sum", "label": "Cost", "order": 1},
                    ],
                },
                filters={},
                sorting=[
                    {
                        "id": "Measure Name",
                        "desc": False,
                        "sortKeyField": "__sortkey__Measure Name",
                    }
                ],
                expanded={"North": True, "North|||Sales": True},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "desk", "sales", "cost"],
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def _build_measure_axis_hierarchy_columns_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "measure_axis_hierarchy_columns_browser",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "desk": ["Rates", "Credit", "Rates", "Credit"],
                "portfolio": ["Book", "Hedge", "Book", "Hedge"],
                "sales": [100.0, 50.0, 80.0, 20.0],
                "cost": [40.0, 10.0, 30.0, 5.0],
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
                table="measure_axis_hierarchy_columns_browser",
                serverSide=True,
                rowFields=["region", "Measure Name", "desk"],
                colFields=["portfolio"],
                valConfigs=[
                    {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
                    {"field": "cost", "agg": "sum", "alias": "cost_sum", "label": "Cost"},
                ],
                measureAxis={
                    "placement": "rows",
                    "labelField": "Measure Name",
                    "valueField": "Amount",
                    "members": [
                        {"measureAlias": "sales_sum", "label": "Sales", "order": 0},
                        {"measureAlias": "cost_sum", "label": "Cost", "order": 1},
                    ],
                },
                filters={},
                sorting=[
                    {
                        "id": "Measure Name",
                        "desc": False,
                        "sortKeyField": "__sortkey__Measure Name",
                    }
                ],
                expanded={"North": True, "North|||Sales": True},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "desk", "portfolio", "sales", "cost"],
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def _build_measure_axis_hierarchy_internal_state_app(include_columns=False):
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "measure_axis_hierarchy_internal_browser",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "desk": ["Rates", "Credit", "Rates", "Credit"],
                "portfolio": ["Book", "Hedge", "Book", "Hedge"],
                "sales": [100.0, 50.0, 80.0, 20.0],
                "cost": [40.0, 10.0, 30.0, 5.0],
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
                table="measure_axis_hierarchy_internal_browser",
                serverSide=True,
                rowFields=["region", "Measure Name", "desk"],
                colFields=["portfolio"] if include_columns else [],
                valConfigs=[
                    {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
                    {"field": "cost", "agg": "sum", "alias": "cost_sum", "label": "Cost"},
                ],
                filters={},
                sorting=[
                    {
                        "id": "Measure Name",
                        "desc": False,
                        "sortKeyField": "__sortkey__Measure Name",
                    }
                ],
                expanded={"North": True, "North|||Sales": True},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "desk", "portfolio", "sales", "cost"],
                data=[],
                persistence=True,
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def _build_measure_axis_drag_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "measure_axis_drag_browser",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "desk": ["Rates", "Credit", "Rates", "Credit"],
                "portfolio": ["Book", "Hedge", "Book", "Hedge"],
                "sales": [100.0, 50.0, 80.0, 20.0],
                "cost": [40.0, 10.0, 30.0, 5.0],
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
                table="measure_axis_drag_browser",
                serverSide=True,
                rowFields=["region", "desk"],
                colFields=[],
                valConfigs=[
                    {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
                    {"field": "cost", "agg": "sum", "alias": "cost_sum", "label": "Cost"},
                ],
                filters={},
                sorting=[
                    {
                        "id": "Measure Name",
                        "desc": False,
                        "sortKeyField": "__sortkey__Measure Name",
                    }
                ],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "desk", "portfolio", "sales", "cost"],
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def _build_measure_axis_array_drag_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    point_type = pa.struct([("x", pa.string()), ("y", pa.float64())])
    dates = [f"d{index:02d}" for index in range(1, 21)]
    instruments = []
    portfolios = []
    price_points = []
    pnl_points = []
    volume_points = []
    for instrument, offset in (("AAA", 0.0), ("BBB", 100.0)):
        for index, label in enumerate(dates):
            instruments.append(instrument)
            portfolios.append("Book" if index % 2 == 0 else "Hedge")
            price_points.append({"x": label, "y": offset + index + 1.0})
            pnl_points.append({"x": label, "y": offset + (index + 1.0) * 10.0})
            volume_points.append({"x": label, "y": offset + (index + 1.0) * 100.0})
    adapter.controller.load_data_from_arrow(
        "measure_axis_array_drag_browser",
        pa.Table.from_pydict(
            {
                "instrument": instruments,
                "portfolio": portfolios,
                "price_20d": pa.array(price_points, type=point_type),
                "pnl_20d": pa.array(pnl_points, type=point_type),
                "volume_20d": pa.array(volume_points, type=point_type),
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
                table="measure_axis_array_drag_browser",
                serverSide=True,
                rowFields=["instrument"],
                colFields=[],
                valConfigs=[
                    {
                        "field": "price_20d",
                        "agg": "array_agg",
                        "alias": "price_series",
                        "label": "Price 20D",
                        "sparkline": {
                            "source": "field",
                            "displayMode": "trend",
                            "type": "line",
                            "metric": "last",
                            "showCurrentValue": True,
                            "showDelta": True,
                        },
                    },
                    {
                        "field": "pnl_20d",
                        "agg": "array_agg",
                        "alias": "pnl_series",
                        "label": "PnL 20D",
                        "sparkline": {
                            "source": "field",
                            "displayMode": "trend",
                            "type": "area",
                            "metric": "last",
                            "showCurrentValue": True,
                            "showDelta": True,
                        },
                    },
                    {
                        "field": "volume_20d",
                        "agg": "array_agg",
                        "alias": "volume_series",
                        "label": "Volume 20D",
                        "sparkline": {
                            "source": "field",
                            "displayMode": "trend",
                            "type": "column",
                            "metric": "sum",
                            "showCurrentValue": True,
                            "showDelta": True,
                        },
                    },
                ],
                filters={},
                sorting=[
                    {
                        "id": "Measure Name",
                        "desc": False,
                        "sortKeyField": "__sortkey__Measure Name",
                    }
                ],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["instrument", "portfolio", "price_20d", "pnl_20d", "volume_20d"],
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def _build_formula_dependency_toggle_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "formula_dependency_browser",
        pa.Table.from_pydict(
            {
                "region": ["North", "South"],
                "sales": [100.0, 200.0],
                "cost": [80.0, 150.0],
            }
        ),
    )

    hidden_cost_values = [
        {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
        {
            "field": "formula_1",
            "agg": "formula",
            "label": "Margin",
            "formula": "sales - cost",
            "formulaRef": "margin",
        },
    ]
    visible_cost_values = [
        {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
        {"field": "cost", "agg": "sum", "alias": "cost_sum", "label": "Cost"},
        {
            "field": "formula_1",
            "agg": "formula",
            "label": "Margin",
            "formula": "sales - cost",
            "formulaRef": "margin",
        },
    ]

    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "12px"},
        children=[
            dcc.Store(id="hidden-cost-values", data=hidden_cost_values),
            dcc.Store(id="visible-cost-values", data=visible_cost_values),
            html.Button("Remove cost", id="remove-cost", n_clicks=0),
            html.Button("Restore cost", id="restore-cost", n_clicks=0),
            html.Div("hidden", id="formula-mode-label"),
            DashTanstackPivot(
                id="pivot-grid",
                style={"height": "520px", "width": "100%"},
                table="formula_dependency_browser",
                serverSide=True,
                rowFields=["region"],
                colFields=[],
                valConfigs=hidden_cost_values,
                filters={},
                sorting=[],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "sales", "cost"],
                data=[],
            ),
        ],
    )

    @app.callback(
        Output("pivot-grid", "valConfigs"),
        Output("formula-mode-label", "children"),
        Input("remove-cost", "n_clicks"),
        Input("restore-cost", "n_clicks"),
        Input("hidden-cost-values", "data"),
        Input("visible-cost-values", "data"),
    )
    def _toggle_formula_dependency(_remove_clicks, _restore_clicks, hidden_values, visible_values):
        trigger = callback_context.triggered[0]["prop_id"].split(".")[0] if callback_context.triggered else ""
        if trigger == "restore-cost":
            return visible_values, "visible"
        return hidden_values, "hidden"

    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def _build_value_removal_expand_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "value_removal_expand_browser",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "desk": ["Rates", "Credit", "Rates", "Credit"],
                "sales": [100.0, 50.0, 80.0, 20.0],
                "cost": [40.0, 10.0, 30.0, 5.0],
            }
        ),
    )

    sales_only_values = [
        {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
    ]
    full_values = [
        {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
        {"field": "cost", "agg": "sum", "alias": "cost_sum", "label": "Cost"},
    ]

    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "12px"},
        children=[
            dcc.Store(id="sales-only-values", data=sales_only_values),
            dcc.Store(id="full-values", data=full_values),
            html.Button("Remove cost", id="remove-cost-value", n_clicks=0),
            html.Button("Restore cost", id="restore-cost-value", n_clicks=0),
            html.Div("full", id="value-mode-label"),
            DashTanstackPivot(
                id="pivot-grid",
                style={"height": "560px", "width": "100%"},
                table="value_removal_expand_browser",
                serverSide=True,
                rowFields=["region", "desk"],
                colFields=[],
                valConfigs=full_values,
                filters={},
                sorting=[],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "desk", "sales", "cost"],
                data=[],
            ),
        ],
    )

    @app.callback(
        Output("pivot-grid", "valConfigs"),
        Output("value-mode-label", "children"),
        Input("remove-cost-value", "n_clicks"),
        Input("restore-cost-value", "n_clicks"),
        Input("sales-only-values", "data"),
        Input("full-values", "data"),
    )
    def _toggle_values(_remove_clicks, _restore_clicks, sales_values, visible_values):
        trigger = callback_context.triggered[0]["prop_id"].split(".")[0] if callback_context.triggered else ""
        if trigger == "remove-cost-value":
            return sales_values, "sales-only"
        return visible_values, "full"

    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def _build_measure_axis_value_removal_expand_app():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "measure_axis_value_removal_expand_browser",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "desk": ["Rates", "Credit", "Rates", "Credit"],
                "sales": [100.0, 50.0, 80.0, 20.0],
                "cost": [40.0, 10.0, 30.0, 5.0],
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
                table="measure_axis_value_removal_expand_browser",
                serverSide=True,
                rowFields=["region", "Measure Name", "desk"],
                colFields=[],
                valConfigs=[
                    {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
                    {"field": "cost", "agg": "sum", "alias": "cost_sum", "label": "Cost"},
                ],
                measureAxis={
                    "placement": "rows",
                    "labelField": "Measure Name",
                    "valueField": "Amount",
                    "members": [
                        {"measureAlias": "sales_sum", "label": "Sales", "order": 0},
                        {"measureAlias": "cost_sum", "label": "Cost", "order": 1},
                    ],
                },
                filters={},
                sorting=[
                    {
                        "id": "Measure Name",
                        "desc": False,
                        "sortKeyField": "__sortkey__Measure Name",
                    }
                ],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "desk", "sales", "cost"],
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
def measure_axis_hierarchy_browser_server():
    app = _build_measure_axis_hierarchy_app()
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
def measure_axis_hierarchy_columns_browser_server():
    app = _build_measure_axis_hierarchy_columns_app()
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
def measure_axis_drag_browser_server():
    app = _build_measure_axis_drag_app()
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
def measure_axis_array_drag_browser_server():
    app = _build_measure_axis_array_drag_app()
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
def formula_dependency_toggle_browser_server():
    app = _build_formula_dependency_toggle_app()
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
def value_removal_expand_browser_server():
    app = _build_value_removal_expand_app()
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
def measure_axis_value_removal_expand_browser_server():
    app = _build_measure_axis_value_removal_expand_app()
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
def measure_axis_internal_hierarchy_browser_server():
    app = _build_measure_axis_hierarchy_internal_state_app(include_columns=False)
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
def measure_axis_internal_hierarchy_columns_browser_server():
    app = _build_measure_axis_hierarchy_internal_state_app(include_columns=True)
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


def _wait_for_cell_absent(driver, row_id, col_id, timeout=30):
    selector = _cell_selector(row_id, col_id)
    return WebDriverWait(driver, timeout).until(
        lambda _driver: len(_driver.find_elements(By.CSS_SELECTOR, selector)) == 0
    )


def _find_sidebar_chip(driver, selector, text, timeout=30):
    wait = WebDriverWait(driver, timeout)

    def _match(_driver):
        for element in _driver.find_elements(By.CSS_SELECTOR, selector):
            if " ".join(element.text.split()) == text:
                return element
        return False

    return wait.until(_match)


def _remove_sidebar_value_chip(driver, label):
    result = driver.execute_script(
        """
        const label = String(arguments[0] || '').trim().toLowerCase();
        const chips = Array.from(document.querySelectorAll('[data-sidebar-drop-zone="vals"] [data-sidebar-field-chip="zone"]'));
        const chip = chips.find((element) => element.innerText.toLowerCase().includes(label));
        if (!chip) {
            return {ok: false, chips: chips.map((element) => element.innerText)};
        }
        const spans = Array.from(chip.querySelectorAll('span'));
        const removeButton = spans[spans.length - 1];
        if (!removeButton) {
            return {ok: false, reason: 'missing remove button', text: chip.innerText};
        }
        removeButton.click();
        return {ok: true};
        """,
        label,
    )
    assert result["ok"], result


def _html5_drag_start(driver, source):
    driver.execute_script(
        """
        const source = arguments[0];
        const rect = source.getBoundingClientRect();
        window.__pivotTestDragDataTransfer = new DataTransfer();
        source.dispatchEvent(new DragEvent('dragstart', {
            bubbles: true,
            cancelable: true,
            dataTransfer: window.__pivotTestDragDataTransfer,
            clientX: rect.left + rect.width / 2,
            clientY: rect.top + rect.height / 2
        }));
        """,
        source,
    )


def _html5_drag_over(driver, target, *, y_fraction=0.5):
    driver.execute_script(
        """
        const target = arguments[0];
        const yFraction = arguments[1];
        const rect = target.getBoundingClientRect();
        target.dispatchEvent(new DragEvent('dragover', {
            bubbles: true,
            cancelable: true,
            dataTransfer: window.__pivotTestDragDataTransfer,
            clientX: rect.left + rect.width / 2,
            clientY: rect.top + rect.height * yFraction
        }));
        """,
        target,
        y_fraction,
    )


def _html5_drop(driver, target):
    driver.execute_script(
        """
        const target = arguments[0];
        const rect = target.getBoundingClientRect();
        target.dispatchEvent(new DragEvent('drop', {
            bubbles: true,
            cancelable: true,
            dataTransfer: window.__pivotTestDragDataTransfer,
            clientX: rect.left + rect.width / 2,
            clientY: rect.top + rect.height / 2
        }));
        window.__pivotTestDragDataTransfer = null;
        """,
        target,
    )


def _drag_sidebar_chip_before_row_chip(driver, source, target_row_chip):
    rows_zone = driver.find_element(By.CSS_SELECTOR, '[data-sidebar-drop-zone="rows"]')
    _html5_drag_start(driver, source)
    time.sleep(0.2)
    _html5_drag_over(driver, target_row_chip, y_fraction=0.1)
    time.sleep(0.2)
    _html5_drop(driver, rows_zone)


def _drag_sidebar_chip_to_zone(driver, source, zone_id):
    drop_zone = driver.find_element(By.CSS_SELECTOR, f'[data-sidebar-drop-zone="{zone_id}"]')
    _html5_drag_start(driver, source)
    time.sleep(0.2)
    _html5_drag_over(driver, drop_zone)
    time.sleep(0.2)
    _html5_drop(driver, drop_zone)


def _activate_persisted_measure_axis(chrome_driver, url):
    chrome_driver.get(url)
    WebDriverWait(chrome_driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Values placement"]'))
    )
    chrome_driver.execute_script(
        """
        window.localStorage.setItem('pivot-grid-measureAxis', JSON.stringify({
            placement: 'rows',
            labelField: 'Measure Name',
            valueField: 'Amount',
            members: [
                {measureAlias: 'sales_sum', label: 'Sales', order: 0},
                {measureAlias: 'cost_sum', label: 'Cost', order: 1}
            ]
        }));
        """
    )
    chrome_driver.refresh()
    values_as_select = WebDriverWait(chrome_driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Values placement"]'))
    )
    assert values_as_select.get_attribute("value") == "rows"


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


def _wait_for_sparkline_cell(driver, row_id, col_id, expected_points, timeout=30):
    selector = f'{_cell_selector(row_id, col_id)} [data-pivot-sparkline-cell="true"]'
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))

    def _matches(_driver):
        try:
            element = _driver.find_element(By.CSS_SELECTOR, selector)
            points = element.get_attribute("data-pivot-sparkline-points")
            return element if points == str(expected_points) else False
        except StaleElementReferenceException:
            return False

    return wait.until(_matches)


def _click_row_expander(driver, row_id, timeout=30):
    selector = f'{_cell_selector(row_id, "hierarchy")} button'
    button = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
    )
    button.click()
    return button


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


def test_measure_axis_rows_browser_renders_hierarchy_levels(measure_axis_hierarchy_browser_server, chrome_driver):
    chrome_driver.get(measure_axis_hierarchy_browser_server)

    values_as_select = WebDriverWait(chrome_driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Values placement"]'))
    )
    assert values_as_select.get_attribute("value") == "rows"

    _wait_for_numeric_cell(chrome_driver, "North", "Amount", 200.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Amount", 150.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Rates", "Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Credit", "Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "South", "Amount", 135.0)

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []


def test_measure_axis_rows_browser_renders_second_level_hierarchy_with_column_fields(
    measure_axis_hierarchy_columns_browser_server,
    chrome_driver,
):
    chrome_driver.get(measure_axis_hierarchy_columns_browser_server)

    values_as_select = WebDriverWait(chrome_driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Values placement"]'))
    )
    assert values_as_select.get_attribute("value") == "rows"

    _wait_for_numeric_cell(chrome_driver, "North", "Book_Amount", 140.0)
    _wait_for_numeric_cell(chrome_driver, "North", "Hedge_Amount", 60.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Book_Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Hedge_Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Rates", "Book_Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Credit", "Hedge_Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Book_Amount", 40.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Hedge_Amount", 10.0)
    _wait_for_numeric_cell(chrome_driver, "South", "Book_Amount", 110.0)
    _wait_for_numeric_cell(chrome_driver, "South", "Hedge_Amount", 25.0)

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []


def test_formula_dependency_toggle_browser_keeps_grid_populated(
    formula_dependency_toggle_browser_server,
    chrome_driver,
):
    chrome_driver.get(formula_dependency_toggle_browser_server)

    _wait_for_numeric_cell(chrome_driver, "North", "sales_sum", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North", "formula_1", 20.0)
    _wait_for_cell_absent(chrome_driver, "North", "cost_sum")

    chrome_driver.find_element(By.ID, "restore-cost").click()
    WebDriverWait(chrome_driver, 30).until(
        EC.text_to_be_present_in_element((By.ID, "formula-mode-label"), "visible")
    )
    _wait_for_numeric_cell(chrome_driver, "North", "cost_sum", 80.0)
    _wait_for_numeric_cell(chrome_driver, "North", "formula_1", 20.0)

    chrome_driver.find_element(By.ID, "remove-cost").click()
    WebDriverWait(chrome_driver, 30).until(
        EC.text_to_be_present_in_element((By.ID, "formula-mode-label"), "hidden")
    )
    _wait_for_numeric_cell(chrome_driver, "North", "sales_sum", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North", "formula_1", 20.0)
    _wait_for_cell_absent(chrome_driver, "North", "cost_sum")

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []


def test_value_removal_then_expand_browser_keeps_visible_measure_populated(
    value_removal_expand_browser_server,
    chrome_driver,
):
    chrome_driver.get(value_removal_expand_browser_server)

    _wait_for_numeric_cell(chrome_driver, "North", "sales_sum", 150.0)
    _wait_for_numeric_cell(chrome_driver, "North", "cost_sum", 50.0)

    chrome_driver.find_element(By.ID, "remove-cost-value").click()
    WebDriverWait(chrome_driver, 30).until(
        EC.text_to_be_present_in_element((By.ID, "value-mode-label"), "sales-only")
    )
    _wait_for_numeric_cell(chrome_driver, "North", "sales_sum", 150.0)
    _wait_for_cell_absent(chrome_driver, "North", "cost_sum")

    _click_row_expander(chrome_driver, "North")
    _wait_for_numeric_cell(chrome_driver, "North|||Rates", "sales_sum", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Credit", "sales_sum", 50.0)
    _wait_for_cell_absent(chrome_driver, "North|||Rates", "cost_sum")
    _wait_for_cell_absent(chrome_driver, "North|||Credit", "cost_sum")

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []


def test_measure_axis_value_removal_and_restore_keep_hierarchy_populated(
    measure_axis_value_removal_expand_browser_server,
    chrome_driver,
):
    chrome_driver.get(measure_axis_value_removal_expand_browser_server)

    values_as_select = WebDriverWait(chrome_driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Values placement"]'))
    )
    assert values_as_select.get_attribute("value") == "rows"

    _wait_for_numeric_cell(chrome_driver, "North", "Amount", 200.0)

    _remove_sidebar_value_chip(chrome_driver, "Cost")
    _wait_for_numeric_cell(chrome_driver, "North", "Amount", 150.0)

    _click_row_expander(chrome_driver, "North")
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Amount", 150.0)
    _wait_for_cell_absent(chrome_driver, "North|||Cost", "Amount", timeout=5)

    _click_row_expander(chrome_driver, "North|||Sales")
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Rates", "Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Credit", "Amount", 50.0)

    cost_chip = _find_sidebar_chip(
        chrome_driver,
        '[data-sidebar-field-chip="available"]',
        "Cost",
    )
    _drag_sidebar_chip_to_zone(chrome_driver, cost_chip, "vals")

    _wait_for_numeric_cell(chrome_driver, "North", "Amount", 200.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Amount", 150.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Amount", 50.0)

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []


def test_measure_axis_rows_browser_uses_internal_state_for_second_level_hierarchy_without_columns(
    measure_axis_internal_hierarchy_browser_server,
    chrome_driver,
):
    _activate_persisted_measure_axis(chrome_driver, measure_axis_internal_hierarchy_browser_server)

    _wait_for_numeric_cell(chrome_driver, "North", "Amount", 200.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Amount", 150.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Rates", "Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Credit", "Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Amount", 50.0)

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []


def test_measure_axis_rows_browser_uses_internal_state_for_second_level_hierarchy_with_columns(
    measure_axis_internal_hierarchy_columns_browser_server,
    chrome_driver,
):
    _activate_persisted_measure_axis(chrome_driver, measure_axis_internal_hierarchy_columns_browser_server)

    _wait_for_numeric_cell(chrome_driver, "North", "Book_Amount", 140.0)
    _wait_for_numeric_cell(chrome_driver, "North", "Hedge_Amount", 60.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Book_Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Hedge_Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Rates", "Book_Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Credit", "Hedge_Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Book_Amount", 40.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Hedge_Amount", 10.0)

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []


def test_measure_axis_rows_browser_drag_measure_names_to_second_row_position_then_add_columns(
    measure_axis_drag_browser_server,
    chrome_driver,
):
    chrome_driver.get(measure_axis_drag_browser_server)
    WebDriverWait(chrome_driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Values placement"]'))
    )

    measure_names_chip = _find_sidebar_chip(
        chrome_driver,
        '[data-sidebar-field-chip="measure-axis-label"]',
        "Measure Names",
    )
    desk_row_chip = _find_sidebar_chip(
        chrome_driver,
        '[data-sidebar-drop-zone="rows"] [data-sidebar-field-chip="zone"]',
        "Desk",
    )
    _drag_sidebar_chip_before_row_chip(chrome_driver, measure_names_chip, desk_row_chip)

    WebDriverWait(chrome_driver, 30).until(
        lambda driver: _find_sidebar_chip(
            driver,
            '[data-sidebar-drop-zone="rows"] [data-sidebar-field-chip="zone"]',
            "Measure Name",
            timeout=1,
        )
    )
    row_chip_labels = [
        " ".join(element.text.split())
        for element in chrome_driver.find_elements(
            By.CSS_SELECTOR,
            '[data-sidebar-drop-zone="rows"] [data-sidebar-field-chip="zone"]',
        )
    ]
    assert row_chip_labels == ["Region", "Measure Name", "Desk"]
    values_as_select = chrome_driver.find_element(By.CSS_SELECTOR, 'select[title="Values placement"]')
    assert values_as_select.get_attribute("value") == "rows"

    _wait_for_numeric_cell(chrome_driver, "North", "Amount", 200.0)
    _click_row_expander(chrome_driver, "North")
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Amount", 150.0)
    _click_row_expander(chrome_driver, "North|||Sales")
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Rates", "Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Credit", "Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Amount", 50.0)

    portfolio_chip = _find_sidebar_chip(
        chrome_driver,
        '[data-sidebar-field-chip="available"]',
        "Portfolio",
    )
    _drag_sidebar_chip_to_zone(chrome_driver, portfolio_chip, "cols")

    _wait_for_numeric_cell(chrome_driver, "North", "Book_Amount", 140.0)
    _wait_for_numeric_cell(chrome_driver, "North", "Hedge_Amount", 60.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Book_Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales", "Hedge_Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Rates", "Book_Amount", 100.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Sales|||Credit", "Hedge_Amount", 50.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Book_Amount", 40.0)
    _wait_for_numeric_cell(chrome_driver, "North|||Cost", "Hedge_Amount", 10.0)

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []


def test_measure_axis_rows_browser_drag_measure_names_renders_array_agg_sparklines(
    measure_axis_array_drag_browser_server,
    chrome_driver,
):
    chrome_driver.get(measure_axis_array_drag_browser_server)
    WebDriverWait(chrome_driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[title="Values placement"]'))
    )

    measure_names_chip = _find_sidebar_chip(
        chrome_driver,
        '[data-sidebar-field-chip="measure-axis-label"]',
        "Measure Names",
    )
    instrument_row_chip = _find_sidebar_chip(
        chrome_driver,
        '[data-sidebar-drop-zone="rows"] [data-sidebar-field-chip="zone"]',
        "Instrument",
    )
    rows_zone = chrome_driver.find_element(By.CSS_SELECTOR, '[data-sidebar-drop-zone="rows"]')
    _html5_drag_start(chrome_driver, measure_names_chip)
    time.sleep(0.2)
    _html5_drag_over(chrome_driver, instrument_row_chip, y_fraction=0.9)
    time.sleep(0.2)
    _html5_drop(chrome_driver, rows_zone)

    WebDriverWait(chrome_driver, 30).until(
        lambda driver: _find_sidebar_chip(
            driver,
            '[data-sidebar-drop-zone="rows"] [data-sidebar-field-chip="zone"]',
            "Measure Name",
            timeout=1,
        )
    )
    row_chip_labels = [
        " ".join(element.text.split())
        for element in chrome_driver.find_elements(
            By.CSS_SELECTOR,
            '[data-sidebar-drop-zone="rows"] [data-sidebar-field-chip="zone"]',
        )
    ]
    assert row_chip_labels == ["Instrument", "Measure Name"]
    values_as_select = chrome_driver.find_element(By.CSS_SELECTOR, 'select[title="Values placement"]')
    assert values_as_select.get_attribute("value") == "rows"

    _click_row_expander(chrome_driver, "AAA")
    _wait_for_sparkline_cell(chrome_driver, "AAA|||Price 20D", "Amount", 20)
    _wait_for_sparkline_cell(chrome_driver, "AAA|||PnL 20D", "Amount", 20)
    _wait_for_sparkline_cell(chrome_driver, "AAA|||Volume 20D", "Amount", 20)

    portfolio_chip = _find_sidebar_chip(
        chrome_driver,
        '[data-sidebar-field-chip="available"]',
        "Portfolio",
    )
    _drag_sidebar_chip_to_zone(chrome_driver, portfolio_chip, "cols")

    _wait_for_sparkline_cell(chrome_driver, "AAA|||Price 20D", "Book_Amount", 10)
    _wait_for_sparkline_cell(chrome_driver, "AAA|||Price 20D", "Hedge_Amount", 10)
    _wait_for_sparkline_cell(chrome_driver, "AAA|||PnL 20D", "Book_Amount", 10)

    severe_logs = _severe_browser_logs(chrome_driver)
    assert severe_logs == []
