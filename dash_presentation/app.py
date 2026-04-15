"""
app.py - Enterprise Grade Server-Side Pivot Table
Integrates DashTanstackPivot (serverside-pivot) with pivot-engine backend.
"""
import json
import os
import sys
import threading
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Ensure the local pivot_engine package takes priority over any stale installs.
_PE_DIR = _REPO_ROOT / "dash_tanstack_pivot" / "pivot_engine"
if _PE_DIR.is_dir() and str(_PE_DIR) not in sys.path:
    sys.path.insert(0, str(_PE_DIR))

import pyarrow as pa
import pyarrow.parquet as pq
from dash import Dash, Input, Output, dcc, html, no_update

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app

_DEBUG_OUTPUT = os.environ.get("PIVOT_DEBUG_OUTPUT", "1").lower() in {"1", "true", "yes"}
_EAGER_LOAD_ON_START = os.environ.get("PIVOT_EAGER_LOAD", "1").lower() in {"1", "true", "yes"}
_DATA_DIR = Path(__file__).resolve().parent / "data"
_TRADER_HISTORY_PATH = _DATA_DIR / "nasdaq_trader_demo_history.parquet"
_TRADER_SNAPSHOT_PATH = _DATA_DIR / "nasdaq_trader_demo_snapshot.parquet"
_TRADER_METADATA_PATH = _DATA_DIR / "nasdaq_trader_demo_metadata.json"


def _load_trader_dataset_metadata():
    if not _TRADER_METADATA_PATH.exists():
        return {}
    try:
        return json.loads(_TRADER_METADATA_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


_TRADER_AVAILABLE_FIELDS = [
    "trade_date",
    "desk",
    "strategy",
    "asset_class",
    "instrument_type",
    "sector",
    "symbol",
    "underlier",
    "instrument",
    "expiry",
    "option_right",
    "strike",
    "multiplier",
    "position_qty",
    "price",
    "prev_price",
    "day_return",
    "price_norm_20d",
    "volume",
    "adv20_shares",
    "realized_vol_20d",
    "market_value",
    "gross_exposure",
    "net_exposure",
    "day_pnl",
    "mtd_pnl",
    "cum_pnl_20d",
    "price_20d",
    "day_pnl_20d",
    "volume_20d",
    "market_value_20d",
    "pnl_20d",
    "delta_usd",
    "gamma_usd",
    "vega_usd",
    "theta_usd",
    "beta_adj_exposure",
    "adv_pct",
    "scenario_pnl_1pct",
]
_TRADER_LOOKBACK_DAYS = 20
_TRADER_LATEST_DATE = "latest"
_TRADER_TIMELINE_META = f"Frozen Yahoo snapshot · {_TRADER_LOOKBACK_DAYS} sessions ending {_TRADER_LATEST_DATE}"
_TRADER_CURRENT_META = f"Latest book snapshot · {_TRADER_LATEST_DATE}"

_TRADER_SERIES_POINT_FIELDS = [
    ("price_20d", "price"),
    ("day_pnl_20d", "day_pnl"),
    ("volume_20d", "volume"),
    ("market_value_20d", "market_value"),
    ("pnl_20d", "cum_pnl_20d"),
]

_TRADER_SPARKLINE_FIELDS = {
    "price_20d": {"source": "field", "displayMode": "trend", "type": "line", "metric": "last", "showCurrentValue": True, "showDelta": True},
    "pnl_20d": {"source": "field", "displayMode": "trend", "type": "area", "metric": "last", "showCurrentValue": True, "showDelta": True},
    "volume_20d": {"source": "field", "displayMode": "trend", "type": "column", "metric": "sum", "showCurrentValue": True, "showDelta": True},
    "market_value_20d": {"source": "field", "displayMode": "value", "type": "line", "metric": "last", "showCurrentValue": True, "showDelta": True},
    "day_pnl_20d": {"source": "field", "displayMode": "trend", "type": "area", "metric": "last", "showCurrentValue": True, "showDelta": True},
}

_TRADER_PRIMARY_CHART_PANES = [
    {
        "id": "chart-pane-momentum",
        "name": "Momentum Board",
        "chartTitle": "Momentum Board",
        "source": "pivot",
        "chartType": "sparkline",
        "barLayout": "grouped",
        "axisMode": "vertical",
        "orientation": "rows",
        "hierarchyLevel": "all",
        "rowLimit": 14,
        "columnLimit": 10,
        "width": 450,
        "chartHeight": 260,
        "sortMode": "natural",
        "interactionMode": "focus",
        "serverScope": "viewport",
        "size": 1,
        "floating": True,
        "floatingRect": {"left": 36, "top": 34, "width": 470, "height": 430},
    },
    {
        "id": "chart-pane-risk-tree",
        "name": "Risk Tree",
        "chartTitle": "Risk Tree",
        "source": "pivot",
        "chartType": "icicle",
        "barLayout": "grouped",
        "axisMode": "vertical",
        "orientation": "rows",
        "hierarchyLevel": "all",
        "rowLimit": 18,
        "columnLimit": 12,
        "width": 470,
        "chartHeight": 260,
        "sortMode": "natural",
        "interactionMode": "focus",
        "serverScope": "viewport",
        "size": 1,
        "floating": True,
        "floatingRect": {"left": 532, "top": 34, "width": 500, "height": 430},
    },
    {
        "id": "chart-pane-pnl-drivers",
        "name": "PnL Drivers",
        "chartTitle": "PnL Drivers",
        "source": "pivot",
        "chartType": "waterfall",
        "barLayout": "grouped",
        "axisMode": "vertical",
        "orientation": "rows",
        "hierarchyLevel": "all",
        "rowLimit": 12,
        "columnLimit": 10,
        "width": 520,
        "chartHeight": 250,
        "sortMode": "natural",
        "interactionMode": "focus",
        "serverScope": "viewport",
        "size": 1,
        "floating": True,
        "floatingRect": {"left": 220, "top": 458, "width": 560, "height": 400},
    },
]


# --- 2. Data Loading (Simulation) ---
def load_initial_data(adapter):
    if _DEBUG_OUTPUT:
        print(f"Loading frozen trader datasets from {_DATA_DIR}...")
    if not _TRADER_HISTORY_PATH.exists() or not _TRADER_SNAPSHOT_PATH.exists():
        raise FileNotFoundError(
            "Missing trader presentation datasets. Run "
            "`python scripts/generate_nasdaq_trader_demo_dataset.py` first."
        )

    trader_history_table = pq.read_table(_TRADER_HISTORY_PATH)
    trader_snapshot_table = pq.read_table(_TRADER_SNAPSHOT_PATH)
    trader_series_table = trader_history_table.sort_by([("symbol", "ascending"), ("trade_date", "ascending")])
    sparkline_point_type = pa.struct([("x", pa.string()), ("y", pa.float64())])
    for series_field, value_field in _TRADER_SERIES_POINT_FIELDS:
        series_points = pa.array(
            [
                {"x": row.get("trade_date"), "y": row.get(value_field)}
                for row in trader_series_table.select(["trade_date", value_field]).to_pylist()
            ],
            type=sparkline_point_type,
        )
        trader_series_table = trader_series_table.append_column(series_field, series_points)
    adapter.controller.load_data_from_arrow("nasdaq_trader_demo_history", trader_history_table)
    adapter.controller.load_data_from_arrow("nasdaq_trader_demo_pnl_series", trader_series_table)
    adapter.controller.load_data_from_arrow("nasdaq_trader_demo_snapshot", trader_snapshot_table)
    # Keep the original example table name for the sparkline contract/demo surface.
    adapter.controller.load_data_from_arrow("sparkline_demo_data", trader_history_table)
    if _DEBUG_OUTPUT:
        print(
            "Trader datasets loaded: "
            f"{trader_history_table.num_rows} history rows, "
            f"{trader_snapshot_table.num_rows} snapshot rows."
        )

    tenor_table = pa.Table.from_pydict(
        {
            "book": ["FI", "FI", "FI", "FI", "FI", "FX", "FX", "FX", "FX", "FX"],
            "tenor": ["1M", "2W", "1D", "6Y", "3M", "1M", "2W", "1D", "6Y", "10Y"],
            "rate": [0.0310, 0.0280, 0.0250, 0.0450, 0.0340, 0.0290, 0.0270, 0.0240, 0.0430, 0.0510],
            "notional": [120.0, 80.0, 150.0, 60.0, 100.0, 90.0, 110.0, 130.0, 70.0, 50.0],
        }
    )
    adapter.controller.load_data_from_arrow("tenor_data", tenor_table)

    curve_pillar_table = pa.Table.from_pydict(
        {
            "desk": ["Rates", "Rates", "Rates", "Rates", "Rates", "Credit", "Credit", "Credit", "Credit", "Credit"],
            "Curve Pillar": ["1M", "2W", "1D", "6Y", "3M", "1M", "2W", "1D", "6Y", "10Y"],
            "__sortkey__Curve Pillar": [30, 14, 1, 2190, 90, 30, 14, 1, 2190, 3650],
            "pv01": [0.12, 0.05, 0.01, 1.80, 0.35, 0.10, 0.04, 0.02, 1.60, 2.10],
            "dv01": [0.08, 0.03, 0.005, 1.20, 0.22, 0.07, 0.025, 0.01, 1.05, 1.40],
        }
    )
    adapter.controller.load_data_from_arrow("curve_data", curve_pillar_table)

    # The field-array sparkline demos use nasdaq_trader_demo_pnl_series.
    # *_20d fields are point structs; agg="array_agg" returns [{x, y}, ...].


_adapter = None
_adapter_lock = threading.Lock()


def get_adapter():
    global _adapter
    if _adapter is not None:
        return _adapter

    with _adapter_lock:
        if _adapter is None:
            adapter = create_tanstack_adapter(backend_uri=":memory:")
            load_initial_data(adapter)
            _adapter = adapter
    return _adapter


# --- 3. Dash App ---
app = Dash(
    __name__,
    suppress_callback_exceptions=False,  # Fix L5: catch future layout mistakes
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
    ],
)

_PAGE_STYLE = {
    "fontFamily": "'Inter', ui-sans-serif, system-ui, sans-serif",
    "background": "#FAFAFA",
    "minHeight": "100vh",
    "margin": 0,
    "padding": 0,
}

_SECTION_HEADER_STYLE = {
    "fontSize": "11px",
    "fontWeight": "700",
    "letterSpacing": "0.06em",
    "textTransform": "uppercase",
    "color": "#94A3B8",
    "padding": "0 0 10px 0",
}

_SECTION_CARD_STYLE = {"padding": "0"}

_MAIN_CONTENT_STYLE = {
    "padding": "24px",
    "display": "flex",
    "flexDirection": "column",
    "gap": "20px",
    "maxWidth": "1680px",
    "margin": "0 auto",
}

_PANEL_STYLE = {
    "background": "#FFFFFF",
    "border": "1px solid #E5E7EB",
    "borderRadius": "18px",
    "boxShadow": "0 10px 24px rgba(15,23,42,0.06), 0 2px 8px rgba(15,23,42,0.04)",
    "padding": "18px",
}

_PANEL_HEADER_STYLE = {
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "space-between",
    "gap": "12px",
    "marginBottom": "14px",
}

_PANEL_TITLE_STYLE = {
    "fontSize": "18px",
    "fontWeight": "600",
    "color": "#111827",
}

_PANEL_META_STYLE = {
    "fontSize": "12px",
    "fontWeight": "500",
    "color": "#64748B",
    "background": "#F8FAFC",
    "border": "1px solid #E5E7EB",
    "borderRadius": "999px",
    "padding": "6px 10px",
}

_LAZY_PLACEHOLDER_STYLE = {
    "display": "flex",
    "flexDirection": "column",
    "alignItems": "flex-start",
    "gap": "12px",
    "padding": "12px 4px 2px 4px",
}

_LAZY_COPY_STYLE = {
    "fontSize": "13px",
    "lineHeight": "1.5",
    "color": "#64748B",
    "maxWidth": "720px",
}

_LAZY_LOAD_BTN_STYLE = {
    "display": "inline-flex",
    "alignItems": "center",
    "justifyContent": "center",
    "gap": "8px",
    "minHeight": "38px",
    "padding": "0 14px",
    "borderRadius": "10px",
    "border": "1px solid #CBD5E1",
    "background": "#F8FAFC",
    "color": "#0F172A",
    "fontSize": "13px",
    "fontWeight": "600",
    "cursor": "pointer",
}


def build_panel(section_label, title, meta, component):
    return html.Div(
        style=_SECTION_CARD_STYLE,
        children=[
            html.Div(section_label, style=_SECTION_HEADER_STYLE),
            html.Div(
                style=_PANEL_STYLE,
                children=[
                    html.Div(
                        style=_PANEL_HEADER_STYLE,
                        children=[
                            html.Div(title, style=_PANEL_TITLE_STYLE),
                            html.Div(meta, style=_PANEL_META_STYLE),
                        ],
                    ),
                    component,
                ],
            ),
        ],
    )


def build_lazy_panel_content(button_id, button_label, description):
    return html.Div(
        style=_LAZY_PLACEHOLDER_STYLE,
        children=[
            html.Div(description, style=_LAZY_COPY_STYLE),
            html.Button(button_label, id=button_id, style=_LAZY_LOAD_BTN_STYLE),
        ],
    )


app.layout = html.Div(
    style={
        **_PAGE_STYLE,
        "width": "100%",
        "height": "100vh",
        "minHeight": "100vh",
        "overflowX": "hidden",
        "overflowY": "auto",
        "background": "#F4F7FB",
    },
    children=[
        dcc.Store(id="saved-view-store"),
        html.Main(
            style=_MAIN_CONTENT_STYLE,
            children=[
                build_panel(
                    "Trader Monitor",
                    "NASDAQ 20D PnL Series",
                    "Each value is a 20-session {x, y} series. Open Sparkline Column settings and switch Cell display.",
                    DashTanstackPivot(
                        id="pivot-grid",
                        style={"height": "860px", "width": "100%"},
                        table="nasdaq_trader_demo_pnl_series",
                        serverSide=True,
                        rowFields=["instrument"],
                        colFields=[],
                        valConfigs=[
                            {
                                "field": "price_20d",
                                "agg": "array_agg",
                                "format": "fixed:2",
                                "label": "Price 20D",
                                "sparkline": {
                                    "source": "field",
                                    "displayMode": "trend",
                                    "type": "line",
                                    "metric": "last",
                                    "header": "Price 20D",
                                    "showCurrentValue": True,
                                    "showDelta": True,
                                },
                            },
                            {
                                "field": "pnl_20d",
                                "agg": "array_agg",
                                "format": "fixed:0",
                                "label": "PnL 20D",
                                "sparkline": {
                                    "source": "field",
                                    "displayMode": "trend",
                                    "type": "area",
                                    "metric": "last",
                                    "header": "PnL 20D",
                                    "showCurrentValue": True,
                                    "showDelta": True,
                                },
                            },
                            {
                                "field": "volume_20d",
                                "agg": "array_agg",
                                "format": "fixed:0",
                                "label": "Volume 20D",
                                "sparkline": {
                                    "source": "field",
                                    "displayMode": "trend",
                                    "type": "column",
                                    "metric": "sum",
                                    "header": "Volume 20D",
                                    "showCurrentValue": True,
                                    "showDelta": True,
                                },
                            },
                            {
                                "field": "market_value_20d",
                                "agg": "array_agg",
                                "format": "fixed:0",
                                "label": "Market Value 20D",
                                "sparkline": {
                                    "source": "field",
                                    "displayMode": "value",
                                    "type": "line",
                                    "metric": "last",
                                    "header": "Market Value 20D",
                                    "showCurrentValue": True,
                                    "showDelta": True,
                                },
                            },
                        ],
                        filters={},
                        sorting=[],
                        expanded={},
                        showRowTotals=False,
                        showColTotals=False,
                        availableFieldList=_TRADER_AVAILABLE_FIELDS,
                        sparklineFields=_TRADER_SPARKLINE_FIELDS,
                        defaultTheme="flash",
                        data=[],
                    ),
                ),
                build_panel(
                    "Performance Baseline",
                    "NASDAQ Snapshot — No Trendlines",
                    "Plain numeric pivot with a displayed-column formula: Cash Equity Day PnL minus ETF Day PnL.",
                    DashTanstackPivot(
                        id="perf-baseline-grid",
                        style={"height": "860px", "width": "100%"},
                        table="nasdaq_trader_demo_snapshot",
                        serverSide=True,
                        rowFields=["desk", "strategy"],
                        colFields=["asset_class"],
                        valConfigs=[
                            {"field": "day_pnl", "agg": "sum", "format": "fixed:0", "label": "Day PnL"},
                            {"field": "market_value", "agg": "sum", "format": "fixed:0", "label": "Mkt Val"},
                            {"field": "net_exposure", "agg": "sum", "format": "fixed:0", "label": "Net Exp"},
                            {"field": "position_qty", "agg": "sum", "format": "fixed:0", "label": "Qty"},
                            {
                                "field": "formula_1",
                                "agg": "formula",
                                "format": "fixed:0",
                                "label": "Cash - ETF PnL",
                                "formula": "[Cash Equity_day_pnl_sum] - [ETF_day_pnl_sum]",
                                "formulaRef": "cash_vs_etf_pnl",
                                "formulaScope": "columns",
                            },
                        ],
                        filters={},
                        sorting=[],
                        expanded={},
                        showRowTotals=True,
                        showColTotals=True,
                        availableFieldList=_TRADER_AVAILABLE_FIELDS,
                        defaultTheme="flash",
                        data=[],
                    ),
                ),
                build_panel(
                    "Linked Sparkline Columns",
                    "Values Plus Trend",
                    "Numeric value columns stay available; each metric also gets a linked trend column.",
                    html.Div(
                        id="sparkline-demo-slot",
                        children=build_lazy_panel_content(
                            "load-sparkline-demo-btn",
                            "Load Linked Trend Demo",
                            "Adds linked trend columns for price, PnL, and volume while keeping the date value columns numeric.",
                        ),
                    ),
                ),
                build_panel(
                    "Field-Array Sparkline Demo",
                    "Symbol PnL History - no column pivot",
                    "Each instrument stores its full 20-day PnL series as an array. "
                    "Switch the series column between trendline and value in the sidebar.",
                    html.Div(
                        id="field-sparkline-demo-slot",
                        children=build_lazy_panel_content(
                            "load-field-sparkline-btn",
                            "Load Field-Array Sparkline Demo",
                            "Shows price, PnL, and volume as series-valued columns. Open Sparkline Column settings and use Cell display.",
                        ),
                    ),
                ),
                build_panel(
                    "Curve Demo",
                    "Curve Pillar",
                    "Custom backend sort key",
                    html.Div(
                        id="curve-demo-slot",
                        children=build_lazy_panel_content(
                            "load-curve-demo-btn",
                            "Load Curve Demo",
                            "Deferred to keep the first page load focused on the primary pivot. Load this panel only when you need the custom sort-key example.",
                        ),
                    ),
                ),
                build_panel(
                    "Rates Demo",
                    "Tenor Sort · Weighted Average",
                    "Semantic tenor ordering",
                    html.Div(
                        id="tenor-demo-slot",
                        children=build_lazy_panel_content(
                            "load-tenor-demo-btn",
                            "Load Rates Demo",
                            "Deferred to keep the initial app responsive. Load this panel when you want the weighted-average and semantic tenor sort example.",
                        ),
                    ),
                ),
            ],
        ),
    ],
)


# Capture saved view emitted by the component into Dash state (Store).
@app.callback(
    Output("saved-view-store", "data"),
    Input("pivot-grid", "savedView"),
    prevent_initial_call=True,
)
def persist_saved_view(saved_view):
    if not saved_view:
        return no_update
    return saved_view


@app.callback(
    Output("sparkline-demo-slot", "children"),
    Input("load-sparkline-demo-btn", "n_clicks"),
    prevent_initial_call=True,
)
def mount_sparkline_demo(_clicks):
    """Load linked trend columns without replacing the numeric pivot value cells."""
    return DashTanstackPivot(
        id="sparkline-modes-pivot-grid",
        style={"height": "560px", "width": "100%"},
        table="nasdaq_trader_demo_history",
        serverSide=True,
        rowFields=["desk", "sector", "symbol"],
        colFields=["trade_date"],
        valConfigs=[
            {
                "field": "price_norm_20d",
                "agg": "avg",
                "format": "fixed:1",
                "sparkline": {
                    "type": "line",
                    "header": "20D Price Trend",
                    "showCurrentValue": True,
                    "showDelta": True,
                    "hideColumns": False,
                    "placement": "before",
                },
            },
            {
                "field": "day_pnl",
                "agg": "sum",
                "format": "fixed:0",
                "sparkline": {
                    "type": "area",
                    "header": "20D PnL Trend",
                    "showCurrentValue": True,
                    "showDelta": True,
                    "hideColumns": False,
                    "placement": "before",
                },
            },
            {
                "field": "volume",
                "agg": "sum",
                "format": "fixed:0",
                "sparkline": {
                    "type": "column",
                    "header": "20D Volume Trend",
                    "showCurrentValue": True,
                    "showDelta": True,
                    "hideColumns": False,
                    "placement": "before",
                },
            },
        ],
        filters={},
        sorting=[],
        expanded={},
        showRowTotals=False,
        showColTotals=False,
        availableFieldList=_TRADER_AVAILABLE_FIELDS,
        defaultTheme="flash",
        data=[],
    )


@app.callback(
    Output("field-sparkline-demo-slot", "children"),
    Input("load-field-sparkline-btn", "n_clicks"),
    prevent_initial_call=True,
)
def mount_field_sparkline_demo(_clicks):
    # array_agg collects each point struct into [{x, y}, ...].
    # The frontend reads that field as a series with source="field".
    return DashTanstackPivot(
        id="field-sparkline-pivot",
        style={"height": "520px", "width": "100%"},
        table="nasdaq_trader_demo_pnl_series",
        serverSide=True,
        rowFields=["instrument"],
        colFields=[],
        valConfigs=[
            {
                "field": "price_20d",
                "agg": "array_agg",
                "format": "fixed:2",
                "label": "Price 20D",
                "sparkline": {
                    "source": "field",
                    "displayMode": "value",
                    "type": "line",
                    "metric": "last",
                    "header": "Price 20D",
                    "showCurrentValue": True,
                    "showDelta": True,
                },
            },
            {
                "field": "day_pnl_20d",
                "agg": "array_agg",
                "format": "fixed:0",
                "label": "Day PnL 20D",
                "sparkline": {
                    "source": "field",
                    "displayMode": "trend",
                    "type": "area",
                    "metric": "sum",
                    "header": "Day PnL 20D",
                    "showCurrentValue": True,
                    "showDelta": True,
                },
            },
            {
                "field": "volume_20d",
                "agg": "array_agg",
                "format": "fixed:0",
                "label": "Volume 20D",
                "sparkline": {
                    "source": "field",
                    "displayMode": "trend",
                    "type": "column",
                    "metric": "sum",
                    "header": "Volume 20D",
                    "showCurrentValue": True,
                    "showDelta": True,
                },
            },
            {
                "field": "pnl_20d",
                "agg": "array_agg",
                "format": "fixed:0",
                "label": "PnL 20D",
                "sparkline": {
                    "source": "field",
                    "displayMode": "trend",
                    "type": "area",
                    "metric": "last",
                    "header": "PnL 20D",
                    "showCurrentValue": True,
                    "showDelta": True,
                },
            },
        ],
        filters={},
        sorting=[],
        expanded={},
        showRowTotals=False,
        showColTotals=False,
        availableFieldList=[
            "desk", "sector", "asset_class", "symbol", "instrument",
            "price_20d", "day_pnl_20d", "volume_20d", "pnl_20d",
        ],
        defaultTheme="flash",
        data=[],
    )


@app.callback(
    Output("curve-demo-slot", "children"),
    Input("load-curve-demo-btn", "n_clicks"),
    prevent_initial_call=True,
)
def mount_curve_demo(_clicks):
    return DashTanstackPivot(
        id="curve-pivot-grid",
        style={"height": "420px", "width": "100%"},
        table="curve_data",
        serverSide=True,
        rowFields=["Curve Pillar"],
        colFields=[],
        valConfigs=[
            {"field": "pv01", "agg": "sum", "format": "fixed:4"},
            {"field": "dv01", "agg": "sum", "format": "fixed:4"},
        ],
        sorting=[{"id": "Curve Pillar", "desc": False}],
        sortOptions={
            "columnOptions": {
                "Curve Pillar": {
                    "sortKeyField": "__sortkey__Curve Pillar",
                }
            }
        },
        filters={},
        expanded={},
        showRowTotals=False,
        showColTotals=False,
        availableFieldList=["desk", "Curve Pillar", "pv01", "dv01"],
        data=[],
    )


@app.callback(
    Output("tenor-demo-slot", "children"),
    Input("load-tenor-demo-btn", "n_clicks"),
    prevent_initial_call=True,
)
def mount_tenor_demo(_clicks):
    return DashTanstackPivot(
        id="tenor-pivot-grid",
        style={"height": "420px", "width": "100%"},
        table="tenor_data",
        serverSide=True,
        rowFields=["tenor"],
        colFields=[],
        valConfigs=[
            {"field": "notional", "agg": "sum", "format": "fixed:2"},
            {
                "field": "rate",
                "agg": "weighted_avg",
                "weightField": "notional",
                "format": "fixed:4",
            },
        ],
        sorting=[{"id": "tenor", "desc": False, "semanticType": "tenor"}],
        filters={},
        expanded={},
        showRowTotals=False,
        showColTotals=False,
        availableFieldList=["book", "tenor", "rate", "notional"],
        data=[],
    )


app.validation_layout = html.Div(
    children=[
        app.layout,
        html.Div(
            style={"display": "none"},
            children=[
                mount_sparkline_demo(None),
                mount_field_sparkline_demo(None),
                mount_curve_demo(None),
                mount_tenor_demo(None),
            ],
        ),
    ]
)


# --- 4. Pivot wiring (one line) ---
# All pivots share a single adapter singleton.  register_pivot_app wires
# each pivot_id to the same transport callback; the runtime callback uses
# a shared PivotRuntimeService so session state is consistent across grids.
_service = None
_service_lock = threading.Lock()


def _get_service():
    global _service
    if _service is not None:
        return _service
    with _service_lock:
        if _service is None:
            from pivot_engine.runtime import PivotRuntimeService, SessionRequestGate
            _service = PivotRuntimeService(
                adapter_getter=get_adapter,
                session_gate=SessionRequestGate(),
            )
    return _service


register_pivot_app(app, adapter_getter=get_adapter, pivot_id="pivot-grid")
register_pivot_app(app, adapter_getter=get_adapter, pivot_id="perf-baseline-grid")
register_pivot_app(app, adapter_getter=get_adapter, pivot_id="sparkline-modes-pivot-grid")
register_pivot_app(app, adapter_getter=get_adapter, pivot_id="curve-pivot-grid")
register_pivot_app(app, adapter_getter=get_adapter, pivot_id="tenor-pivot-grid")
register_pivot_app(app, adapter_getter=get_adapter, pivot_id="field-sparkline-pivot")

if __name__ == "__main__":
    if _EAGER_LOAD_ON_START:
        if _DEBUG_OUTPUT:
            print("Preloading pivot datasets before starting Dash...")
        get_adapter()
    app.run(debug=True, use_reloader=False, port=8050)
