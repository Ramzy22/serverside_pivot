"""
app_trading.py — Trading Book Dashboard
Self-contained example for serverside-pivot. No parquet files needed.
Run: python app_trading.py
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

import pyarrow as pa
from dash import Dash, Input, Output, dcc, html, no_update

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app
import pivot_engine.tanstack_adapter as _ta
_ta._LOCAL_CACHE_TTL_SECONDS = 120.0  # default 10s is too short; raise to 2 min for demo

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

random.seed(42)

_DESKS = ["Equity L/S", "Equity Deriv", "Rates", "Credit", "FX", "Commodity", "EM Debt", "Convertibles"]
_STRATEGIES = ["Market Making", "Relative Value", "Directional", "Statistical Arb", "Hedging", "Long/Short"]
_ASSET_CLASSES = ["Equity", "Fixed Income", "FX", "Commodity", "Derivative"]
_INSTRUMENT_TYPES = ["Stock", "ETF", "Bond", "Swap", "Option", "Future", "FX Spot", "FX Forward", "CDS", "Convertible"]
_SECTORS = ["Technology", "Healthcare", "Financials", "Energy", "Consumer Disc", "Industrials", "Materials", "Utilities", "Real Estate", "Communication"]
_OPTION_RIGHTS = ["Call", "Put"]
_EXPIRIES = ["2025-06", "2025-09", "2025-12", "2026-03", "2026-06", "2026-12", "2027-06"]
_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "SEK"]
_RATINGS = ["AAA", "AA+", "AA", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "B+", "NR"]

# ~120 realistic symbols per asset class
_EQUITY_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "BRK.B", "JPM", "V",
    "JNJ", "WMT", "XOM", "MA", "PG", "UNH", "HD", "CVX", "LLY", "ABBV",
    "MRK", "PEP", "KO", "AVGO", "COST", "ADBE", "CRM", "TMO", "ACN", "MCD",
    "BAC", "NEE", "CSCO", "DHR", "IBM", "TXN", "AMGN", "INTC", "QCOM", "HON",
    "GS", "MS", "BLK", "SCHW", "AXP", "USB", "WFC", "C", "PNC", "TFC",
    "SPY", "QQQ", "IWM", "DIA", "VTI", "EFA", "EEM", "GLD", "TLT", "HYG",
]
_FI_SYMBOLS = [
    "UST2Y", "UST5Y", "UST10Y", "UST30Y", "BUND2Y", "BUND10Y", "GILT10Y",
    "IG_CORP_5Y", "HY_CORP_5Y", "EM_SOVER_10Y", "TIPS5Y", "TIPS10Y",
    "CDX_IG_S42", "CDX_HY_S42", "ITRAXX_MAIN", "ITRAXX_XOVER",
]
_FX_SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "EURGBP", "EURJPY", "GBPJPY", "NZDUSD"]
_CMDTY_SYMBOLS = ["WTI_CLZ5", "BRENT_COZ5", "GOLD_GCZ5", "SILVER_SIZ5", "COPPER_HGZ5", "NATGAS_NGZ5", "CORN_CZ5", "WHEAT_WZ5"]

# Desk → (asset_classes, instrument_types, symbols) mapping
_DESK_CONFIG = {
    "Equity L/S":    (["Equity"],                          ["Stock", "ETF"],                    _EQUITY_SYMBOLS[:40]),
    "Equity Deriv":  (["Derivative", "Equity"],            ["Option", "Future", "Stock"],        _EQUITY_SYMBOLS[20:50]),
    "Rates":         (["Fixed Income", "Derivative"],      ["Bond", "Swap", "Future"],           _FI_SYMBOLS),
    "Credit":        (["Fixed Income", "Derivative"],      ["Bond", "CDS", "Swap"],              _FI_SYMBOLS[4:]),
    "FX":            (["FX", "Derivative"],                ["FX Spot", "FX Forward", "Option"],  _FX_SYMBOLS),
    "Commodity":     (["Commodity", "Derivative"],         ["Future", "Option"],                 _CMDTY_SYMBOLS),
    "EM Debt":       (["Fixed Income", "Equity"],          ["Bond", "ETF", "Stock"],             _FI_SYMBOLS[6:] + _EQUITY_SYMBOLS[40:50]),
    "Convertibles":  (["Fixed Income", "Derivative"],      ["Convertible", "Option"],            _EQUITY_SYMBOLS[30:45]),
}
_DESK_SECTOR_MAP = {
    "Equity L/S":   ["Technology", "Healthcare", "Consumer Disc", "Financials", "Industrials"],
    "Equity Deriv": ["Technology", "Financials", "Energy", "Consumer Disc"],
    "Rates":        ["Financials", "Utilities", "Real Estate"],
    "Credit":       ["Financials", "Energy", "Industrials", "Materials", "Communication"],
    "FX":           ["Financials"],
    "Commodity":    ["Energy", "Materials"],
    "EM Debt":      ["Financials", "Communication", "Energy", "Materials"],
    "Convertibles": ["Technology", "Healthcare", "Communication", "Industrials"],
}


def _rn(mu: float, sigma: float) -> float:
    return random.gauss(mu, sigma)


def _generate_positions(n: int = 4000) -> pa.Table:
    rows = []
    for _ in range(n):
        desk = random.choice(_DESKS)
        asset_classes, instr_types, symbols = _DESK_CONFIG[desk]
        asset_class = random.choice(asset_classes)
        instrument_type = random.choice(instr_types)
        symbol = random.choice(symbols)
        strategy = random.choice(_STRATEGIES)
        sector = random.choice(_DESK_SECTOR_MAP[desk])
        currency = random.choice(_CURRENCIES[:4])  # mostly USD/EUR/GBP/JPY
        rating = random.choice(_RATINGS)

        is_option = instrument_type == "Option"
        option_right = random.choice(_OPTION_RIGHTS) if is_option else "N/A"
        expiry = random.choice(_EXPIRIES) if instrument_type in ("Option", "Future", "FX Forward") else "N/A"
        strike = round(_rn(200, 80), 2) if is_option else 0.0

        # Position & pricing
        long_short = 1 if random.random() > 0.35 else -1
        qty = int(abs(_rn(5000, 8000))) * long_short
        price = max(0.5, _rn(150, 90))
        book_value = abs(qty) * max(0.5, price + _rn(0, 10)) * long_short
        market_value = qty * price

        unrealized_pnl = market_value - book_value
        realized_pnl = _rn(0, abs(market_value) * 0.02)
        day_pnl = _rn(0, abs(market_value) * 0.008)
        mtd_pnl = day_pnl + _rn(0, abs(market_value) * 0.03)
        ytd_pnl = mtd_pnl + _rn(0, abs(market_value) * 0.12)

        gross_exposure = abs(market_value)
        net_exposure = market_value
        beta = max(0.2, _rn(1.0, 0.5))
        beta_adj = net_exposure * beta

        # Greeks (meaningful for options/derivatives, small for cash)
        deriv_scale = 1.0 if is_option else 0.05
        delta_usd = net_exposure * beta * deriv_scale * _rn(0.6, 0.3)
        gamma_usd = gross_exposure * deriv_scale * abs(_rn(0, 0.002))
        vega_usd = gross_exposure * deriv_scale * abs(_rn(0, 0.005))
        theta_usd = -gross_exposure * deriv_scale * abs(_rn(0, 0.003))

        realized_vol = max(0.05, _rn(0.22, 0.12))
        adv_pct = max(0.001, abs(_rn(0.02, 0.03)))

        rows.append({
            "desk": desk,
            "strategy": strategy,
            "asset_class": asset_class,
            "instrument_type": instrument_type,
            "sector": sector,
            "symbol": symbol,
            "currency": currency,
            "rating": rating,
            "option_right": option_right,
            "expiry": expiry,
            "strike": round(strike, 2),
            "position_qty": qty,
            "price": round(price, 4),
            "book_value": round(book_value, 2),
            "market_value": round(market_value, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "realized_pnl": round(realized_pnl, 2),
            "day_pnl": round(day_pnl, 2),
            "mtd_pnl": round(mtd_pnl, 2),
            "ytd_pnl": round(ytd_pnl, 2),
            "gross_exposure": round(gross_exposure, 2),
            "net_exposure": round(net_exposure, 2),
            "delta_usd": round(delta_usd, 2),
            "gamma_usd": round(gamma_usd, 4),
            "vega_usd": round(vega_usd, 2),
            "theta_usd": round(theta_usd, 2),
            "beta_adj_exposure": round(beta_adj, 2),
            "realized_vol_20d": round(realized_vol, 4),
            "adv_pct": round(adv_pct, 4),
        })

    schema = {
        "desk": pa.string(), "strategy": pa.string(), "asset_class": pa.string(),
        "instrument_type": pa.string(), "sector": pa.string(), "symbol": pa.string(),
        "currency": pa.string(), "rating": pa.string(), "option_right": pa.string(),
        "expiry": pa.string(), "strike": pa.float64(), "position_qty": pa.int64(),
        "price": pa.float64(), "book_value": pa.float64(), "market_value": pa.float64(),
        "unrealized_pnl": pa.float64(), "realized_pnl": pa.float64(),
        "day_pnl": pa.float64(), "mtd_pnl": pa.float64(), "ytd_pnl": pa.float64(),
        "gross_exposure": pa.float64(), "net_exposure": pa.float64(),
        "delta_usd": pa.float64(), "gamma_usd": pa.float64(),
        "vega_usd": pa.float64(), "theta_usd": pa.float64(),
        "beta_adj_exposure": pa.float64(), "realized_vol_20d": pa.float64(),
        "adv_pct": pa.float64(),
    }
    arrays = {k: pa.array([r[k] for r in rows], type=v) for k, v in schema.items()}
    return pa.table(arrays)


_ALL_FIELDS = [
    "desk", "strategy", "asset_class", "instrument_type", "sector", "symbol",
    "currency", "rating", "option_right", "expiry", "strike",
    "position_qty", "price", "book_value", "market_value",
    "unrealized_pnl", "realized_pnl", "day_pnl", "mtd_pnl", "ytd_pnl",
    "gross_exposure", "net_exposure", "delta_usd", "gamma_usd",
    "vega_usd", "theta_usd", "beta_adj_exposure", "realized_vol_20d", "adv_pct",
]

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
            tbl = _generate_positions(4000)
            a.controller.load_data_from_arrow("positions", tbl)
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

# --- Styles ------------------------------------------------------------------

_PAGE = {
    "fontFamily": "'Inter', ui-sans-serif, system-ui, sans-serif",
    "background": "#0D1117",
    "minHeight": "100vh",
    "color": "#E6EDF3",
}

_HEADER = {
    "background": "linear-gradient(135deg, #161B22 0%, #0D1117 100%)",
    "borderBottom": "1px solid #21262D",
    "padding": "20px 32px",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "space-between",
    "gap": "24px",
}

_HEADER_LEFT = {"display": "flex", "flexDirection": "column", "gap": "4px"}

_APP_TITLE = {
    "fontSize": "22px",
    "fontWeight": "700",
    "color": "#E6EDF3",
    "letterSpacing": "-0.02em",
    "margin": 0,
}

_APP_SUBTITLE = {
    "fontSize": "12px",
    "color": "#7D8590",
    "fontWeight": "400",
}

_BADGE = {
    "display": "inline-flex",
    "alignItems": "center",
    "gap": "6px",
    "padding": "4px 10px",
    "borderRadius": "6px",
    "fontSize": "11px",
    "fontWeight": "600",
    "letterSpacing": "0.03em",
}

_BADGE_LIVE = {**_BADGE, "background": "rgba(35,197,94,0.12)", "color": "#3FB950", "border": "1px solid rgba(35,197,94,0.25)"}
_BADGE_INFO = {**_BADGE, "background": "rgba(88,166,255,0.10)", "color": "#58A6FF", "border": "1px solid rgba(88,166,255,0.20)"}

_HEADER_BADGES = {"display": "flex", "alignItems": "center", "gap": "10px"}

_MAIN = {
    "padding": "20px 16px",
    "display": "flex",
    "flexDirection": "column",
    "gap": "24px",
}

_SECTION_LABEL = {
    "fontSize": "10px",
    "fontWeight": "700",
    "letterSpacing": "0.08em",
    "textTransform": "uppercase",
    "color": "#7D8590",
    "marginBottom": "10px",
}

_CARD = {
    "background": "#161B22",
    "border": "1px solid #21262D",
    "borderRadius": "12px",
    "overflow": "hidden",
}

_CARD_HEADER = {
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "space-between",
    "padding": "14px 18px",
    "borderBottom": "1px solid #21262D",
    "background": "#0D1117",
}

_CARD_TITLE = {
    "fontSize": "14px",
    "fontWeight": "600",
    "color": "#E6EDF3",
    "margin": 0,
}

_CARD_META = {
    "fontSize": "11px",
    "color": "#7D8590",
    "fontWeight": "400",
}

_CARD_BODY = {"padding": "0"}

# --- Layout helpers ----------------------------------------------------------

def stat_chip(label, value, color="#58A6FF"):
    return html.Div(
        style={
            "display": "flex",
            "flexDirection": "column",
            "alignItems": "center",
            "padding": "8px 16px",
            "background": "rgba(33,38,45,0.8)",
            "border": "1px solid #21262D",
            "borderRadius": "8px",
            "gap": "2px",
        },
        children=[
            html.Div(value, style={"fontSize": "16px", "fontWeight": "700", "color": color}),
            html.Div(label, style={"fontSize": "10px", "color": "#7D8590", "fontWeight": "500", "letterSpacing": "0.04em"}),
        ],
    )


def card(section_label, title, meta, body, extra_header=None):
    header_children = [
        html.Div(style={"display": "flex", "flexDirection": "column", "gap": "2px"}, children=[
            html.Div(title, style=_CARD_TITLE),
            html.Div(meta, style=_CARD_META),
        ])
    ]
    if extra_header:
        header_children.append(extra_header)
    return html.Div(
        children=[
            html.Div(section_label, style=_SECTION_LABEL),
            html.Div(
                style=_CARD,
                children=[
                    html.Div(style=_CARD_HEADER, children=header_children),
                    html.Div(style=_CARD_BODY, children=[body]),
                ],
            ),
        ]
    )


# --- Component factories -----------------------------------------------------

def book_pnl_pivot():
    return DashTanstackPivot(
        id="pivot-book-pnl",
        style={"height": "780px", "width": "100%"},
        table="positions",
        serverSide=True,
        rowFields=["desk", "strategy"],
        colFields=["asset_class"],
        valConfigs=[
            {"field": "day_pnl",   "agg": "sum", "format": "fixed:0", "label": "Day PnL"},
            {"field": "mtd_pnl",   "agg": "sum", "format": "fixed:0", "label": "MTD PnL"},
            {"field": "ytd_pnl",   "agg": "sum", "format": "fixed:0", "label": "YTD PnL"},
            {"field": "market_value", "agg": "sum", "format": "fixed:0", "label": "Mkt Val"},
            {"field": "net_exposure", "agg": "sum", "format": "fixed:0", "label": "Net Exp"},
        ],
        filters={},
        sorting=[{"id": "day_pnl", "desc": True}],
        expanded={},
        showRowTotals=True,
        showColTotals=False,
        availableFieldList=_ALL_FIELDS,
        defaultTheme="bloomberg",
        data=[],
    )


def position_monitor_pivot():
    return DashTanstackPivot(
        id="pivot-positions",
        style={"height": "780px", "width": "100%"},
        table="positions",
        serverSide=True,
        rowFields=["desk", "asset_class", "symbol"],
        colFields=[],
        valConfigs=[
            {"field": "position_qty",   "agg": "sum",  "format": "fixed:0",  "label": "Qty"},
            {"field": "price",          "agg": "avg",  "format": "fixed:4",  "label": "Avg Price"},
            {"field": "market_value",   "agg": "sum",  "format": "fixed:0",  "label": "Mkt Val"},
            {"field": "unrealized_pnl", "agg": "sum",  "format": "fixed:0",  "label": "Unreal PnL"},
            {"field": "realized_pnl",   "agg": "sum",  "format": "fixed:0",  "label": "Real PnL"},
            {"field": "day_pnl",        "agg": "sum",  "format": "fixed:0",  "label": "Day PnL"},
            {"field": "gross_exposure", "agg": "sum",  "format": "fixed:0",  "label": "Gross Exp"},
        ],
        filters={},
        sorting=[{"id": "gross_exposure", "desc": True}],
        expanded={},
        showRowTotals=False,
        showColTotals=False,
        availableFieldList=_ALL_FIELDS,
        defaultTheme="bloomberg",
        data=[],
    )


# --- Layout ------------------------------------------------------------------

app.layout = html.Div(
    style=_PAGE,
    children=[
        dcc.Store(id="saved-view-store"),

        # Header
        html.Header(
            style=_HEADER,
            children=[
                html.Div(
                    style=_HEADER_LEFT,
                    children=[
                        html.H1("Trading Book Dashboard", style=_APP_TITLE),
                        html.Div("Multi-desk · serverside-pivot · 4 000 positions · DuckDB in-memory", style=_APP_SUBTITLE),
                    ],
                ),
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "16px"},
                    children=[
                        html.Div(style={"display": "flex", "gap": "8px"}, children=[
                            stat_chip("DESKS", "8",    "#58A6FF"),
                            stat_chip("POSITIONS", "4 000", "#3FB950"),
                            stat_chip("SYMBOLS", "90+", "#F0883E"),
                            stat_chip("STRATEGIES", "6",  "#D2A8FF"),
                        ]),
                        html.Div(style=_HEADER_BADGES, children=[
                            html.Span(style=_BADGE_LIVE, children=["● LIVE"]),
                            html.Span(style=_BADGE_INFO, children=["DuckDB"]),
                        ]),
                    ],
                ),
            ],
        ),

        # Main content
        html.Main(
            style=_MAIN,
            children=[
                card(
                    "PnL Overview",
                    "Book PnL by Desk × Strategy",
                    "Columns pivoted on asset class · Day / MTD / YTD / Market Value",
                    book_pnl_pivot(),
                ),
                card(
                    "Position Book",
                    "Full Position Monitor — Desk › Asset Class › Symbol",
                    "Qty · Avg Price · Market Value · Unrealized / Realized / Day PnL · Gross Exposure",
                    position_monitor_pivot(),
                ),
            ],
        ),
    ],
)


# --- Callbacks ---------------------------------------------------------------

@app.callback(
    Output("saved-view-store", "data"),
    Input("pivot-book-pnl", "savedView"),
    prevent_initial_call=True,
)
def persist_saved_view(sv):
    return sv or no_update


# --- Register server-side callbacks ------------------------------------------

for _pid in ["pivot-book-pnl", "pivot-positions"]:
    register_pivot_app(app, adapter_getter=get_adapter, pivot_id=_pid)


if __name__ == "__main__":
    print("Preloading 4 000 positions into DuckDB...")
    get_adapter()
    print("Ready. Open http://localhost:8050")
    app.run(debug=True, use_reloader=False, port=8050)
