"""
app_sensi.py — Sensitivity Explain Dashboard
Demonstrates measureAxis rows + colFields period pivot.
Run: python app_sensi.py
"""
import random
import sys
import threading
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_PE_DIR = _REPO_ROOT / "dash_tanstack_pivot" / "pivot_engine"
if _PE_DIR.is_dir() and str(_PE_DIR) not in sys.path:
    sys.path.insert(0, str(_PE_DIR))

import pandas as pd
import pyarrow as pa
from dash import Dash, html

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app
import pivot_engine.tanstack_adapter as _ta

_ta._LOCAL_CACHE_TTL_SECONDS = 120.0

# ---------------------------------------------------------------------------
# Data generation — wide format
# ---------------------------------------------------------------------------

random.seed(42)

_DESKS      = ["Equity L/S", "Equity Deriv", "Rates", "Credit", "FX", "Commodity"]
_STRATEGIES = ["Market Making", "Relative Value", "Directional", "Statistical Arb"]
_BOOKS      = ["Book A", "Book B", "Book C"]
_SECTORS    = ["Technology", "Financials", "Energy", "Healthcare", "Industrials"]
_CURRENCIES = ["USD", "EUR", "GBP", "JPY"]


def _rn(mu: float, sigma: float) -> float:
    return random.gauss(mu, sigma)


def _generate_wide(n: int = 2000) -> pa.Table:
    """Generate wide-format sensi data: one row per position, 15 measure×period cols."""
    rows = []
    for _ in range(n):
        desk     = random.choice(_DESKS)
        strategy = random.choice(_STRATEGIES)
        book     = random.choice(_BOOKS)
        sector   = random.choice(_SECTORS)
        currency = random.choice(_CURRENCIES)

        # Simulate correlated today/mtd/ytd for each measure
        delta_today = _rn(0, 5_000)
        gamma_today = _rn(0, 800)
        theta_today = _rn(-200, 100)
        vega_today  = _rn(0, 1_200)
        total_today = delta_today + gamma_today + theta_today + vega_today
        sensi_today = total_today * random.uniform(0.85, 1.15)

        def mtd(x): return x * random.uniform(4, 8)
        def ytd(x): return x * random.uniform(15, 35)

        rows.append({
            "desk":     desk,
            "strategy": strategy,
            "book":     book,
            "sector":   sector,
            "currency": currency,

            "delta_explain_today": round(delta_today, 2),
            "delta_explain_mtd":   round(mtd(delta_today), 2),
            "delta_explain_ytd":   round(ytd(delta_today), 2),

            "gamma_explain_today": round(gamma_today, 2),
            "gamma_explain_mtd":   round(mtd(gamma_today), 2),
            "gamma_explain_ytd":   round(ytd(gamma_today), 2),

            "theta_explain_today": round(theta_today, 2),
            "theta_explain_mtd":   round(mtd(theta_today), 2),
            "theta_explain_ytd":   round(ytd(theta_today), 2),

            "vega_explain_today":  round(vega_today, 2),
            "vega_explain_mtd":    round(mtd(vega_today), 2),
            "vega_explain_ytd":    round(ytd(vega_today), 2),

            "total_explain_today": round(total_today, 2),
            "total_explain_mtd":   round(mtd(total_today), 2),
            "total_explain_ytd":   round(ytd(total_today), 2),

            "sensi_pnl_today":     round(sensi_today, 2),
            "sensi_pnl_mtd":       round(mtd(sensi_today), 2),
            "sensi_pnl_ytd":       round(ytd(sensi_today), 2),
        })

    return pa.Table.from_pydict({k: [r[k] for r in rows] for k in rows[0]})


# ---------------------------------------------------------------------------
# Wide → Long transform
# ---------------------------------------------------------------------------

# Maps each wide column → (measure label shown in the table, period label)
_MEASURE_PERIOD_MAP = [
    ("delta_explain_today", "Delta Explain", "Today"),
    ("delta_explain_mtd",   "Delta Explain", "MTD"),
    ("delta_explain_ytd",   "Delta Explain", "YTD"),
    ("gamma_explain_today", "Gamma Explain", "Today"),
    ("gamma_explain_mtd",   "Gamma Explain", "MTD"),
    ("gamma_explain_ytd",   "Gamma Explain", "YTD"),
    ("theta_explain_today", "Theta Explain", "Today"),
    ("theta_explain_mtd",   "Theta Explain", "MTD"),
    ("theta_explain_ytd",   "Theta Explain", "YTD"),
    ("vega_explain_today",  "Vega Explain",  "Today"),
    ("vega_explain_mtd",    "Vega Explain",  "MTD"),
    ("vega_explain_ytd",    "Vega Explain",  "YTD"),
    ("total_explain_today", "Total Explain", "Today"),
    ("total_explain_mtd",   "Total Explain", "MTD"),
    ("total_explain_ytd",   "Total Explain", "YTD"),
    ("sensi_pnl_today",     "Sensi PnL",     "Today"),
    ("sensi_pnl_mtd",       "Sensi PnL",     "MTD"),
    ("sensi_pnl_ytd",       "Sensi PnL",     "YTD"),
]

_DIM_COLS = ["desk", "strategy", "book", "sector", "currency"]

# Preserve a stable display order for measures in the table
_MEASURE_ORDER = {
    "Delta Explain": 0,
    "Gamma Explain": 1,
    "Theta Explain": 2,
    "Vega Explain":  3,
    "Total Explain": 4,
    "Sensi PnL":     5,
}

_PERIOD_ORDER = {"Today": 0, "MTD": 1, "YTD": 2}


def wide_to_long(wide: pa.Table) -> pa.Table:
    df = wide.to_pandas()
    chunks = []
    for col_name, measure_label, period_label in _MEASURE_PERIOD_MAP:
        chunk = df[_DIM_COLS].copy()
        chunk["measure"]        = measure_label
        chunk["period"]         = period_label
        chunk["measure_order"]  = _MEASURE_ORDER[measure_label]
        chunk["period_order"]   = _PERIOD_ORDER[period_label]
        chunk["value"]          = df[col_name]
        chunks.append(chunk)

    long_df = pd.concat(chunks, ignore_index=True)
    return pa.Table.from_pandas(long_df, preserve_index=False)


# ---------------------------------------------------------------------------
# Adapter singleton
# ---------------------------------------------------------------------------

_adapter = None
_adapter_lock = threading.Lock()


def get_adapter():
    global _adapter
    if _adapter is not None:
        return _adapter
    with _adapter_lock:
        if _adapter is None:
            a = create_tanstack_adapter(backend_uri=":memory:")
            wide = _generate_wide(2000)
            long = wide_to_long(wide)
            a.controller.load_data_from_arrow("sensi", long)
            _adapter = a
    return _adapter


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = Dash(
    __name__,
    suppress_callback_exceptions=False,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
    ],
)

_PAGE = {
    "fontFamily": "'Inter', ui-sans-serif, system-ui, sans-serif",
    "background": "#0D1117",
    "minHeight": "100vh",
    "color": "#E6EDF3",
    "display": "flex",
    "flexDirection": "column",
}

_HEADER = {
    "background": "linear-gradient(135deg, #161B22 0%, #0D1117 100%)",
    "borderBottom": "1px solid #21262D",
    "padding": "18px 24px",
    "display": "flex",
    "alignItems": "center",
    "gap": "16px",
}

_TITLE = {
    "fontSize": "20px",
    "fontWeight": "700",
    "color": "#E6EDF3",
    "letterSpacing": "-0.02em",
    "margin": 0,
}

_SUBTITLE = {"fontSize": "12px", "color": "#7D8590"}

_MAIN = {"padding": "20px 16px", "flex": "1"}

_CARD = {
    "background": "#161B22",
    "border": "1px solid #21262D",
    "borderRadius": "12px",
    "overflow": "hidden",
}

_CARD_HEADER = {
    "padding": "12px 18px",
    "borderBottom": "1px solid #21262D",
    "background": "#0D1117",
    "display": "flex",
    "flexDirection": "column",
    "gap": "2px",
}

_ALL_DIMS = ["desk", "strategy", "book", "sector", "currency", "measure", "period"]

app.layout = html.Div(
    style=_PAGE,
    children=[
        html.Header(
            style=_HEADER,
            children=[
                html.Div([
                    html.H1("Sensitivity Explain", style=_TITLE),
                    html.Div(
                        "Desk · Strategy · Book  ×  Delta / Gamma / Theta / Vega / Total / Sensi  ×  Today / MTD / YTD",
                        style=_SUBTITLE,
                    ),
                ]),
            ],
        ),

        html.Main(
            style=_MAIN,
            children=[
                html.Div(
                    style=_CARD,
                    children=[
                        html.Div(
                            style=_CARD_HEADER,
                            children=[
                                html.Div("PnL Explain by Desk × Strategy",
                                         style={"fontSize": "14px", "fontWeight": "600", "color": "#E6EDF3"}),
                                html.Div("Drag 'period' between rows/columns to flip the layout",
                                         style={"fontSize": "11px", "color": "#7D8590"}),
                            ],
                        ),
                        DashTanstackPivot(
                            id="pivot-sensi",
                            style={"height": "820px", "width": "100%"},
                            table="sensi",
                            serverSide=True,

                            # Row dimensions
                            rowFields=["desk", "strategy"],

                            # Period becomes the column pivot — Today | MTD | YTD
                            colFields=["period"],

                            # Measures stack as sub-rows under each desk/strategy
                            measureAxis={
                                "placement": "rows",
                                "labelField": "measure",
                                "valueField": "value",
                            },

                            # Single valConfig — the engine UNIONs per measure internally
                            valConfigs=[
                                {"field": "value", "agg": "sum", "format": "fixed:0", "label": ""},
                            ],

                            filters={},
                            sorting=[],
                            expanded={},
                            showRowTotals=True,
                            showColTotals=False,
                            availableFieldList=_ALL_DIMS,
                            defaultTheme="bloomberg",
                            data=[],
                        ),
                    ],
                ),
            ],
        ),
    ],
)

register_pivot_app(app, adapter_getter=get_adapter, pivot_id="pivot-sensi")

if __name__ == "__main__":
    print("Preloading sensi data...")
    get_adapter()
    print("Ready. Open http://localhost:8050")
    app.run(debug=True, use_reloader=False, port=8050)
