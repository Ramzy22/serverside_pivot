"""Industry-specific showcase datasets and adapter builders for serverside-pivot.

Provides high-cardinality, realistic synthetic datasets generated deterministically via DuckDB:
1. Trading & Risk Portfolio (Equities, Rates, FX, Commodities, Crypto, VaR, DV01, PnL)
2. People Analytics & Workforce (Headcount, Salary Bands, Rating, Location, Tenure, Attrition Risk)
3. B2B SaaS Intelligence (ARR, MRR, Cohorts, NRR, CAC, LTV, Net Retention)

Pre-configured with advanced visual superpowers:
- Inline Chart Cells (Area & Trend Sparklines in table cells)
- Real-time Docked Chart Canvas Panes (Bar, Donut, Area charts)
- Deeply Expanded Tree Hierarchies for immediate virtual scrolling
- Data Bars on key volumetric columns
- Heatmap Color Scales & Conditional Formatting
- Pinned Hierarchy Columns
- Pre-applied Multi-condition Filters
- Immersive High-Density View Mode
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any
import duckdb
import pyarrow as pa

from pivot_engine import create_tanstack_adapter


# ==============================================================================
# 1. TRADING & PORTFOLIO RISK DATASET
# ==============================================================================

TRADER_TABLE_NAME = "trading_portfolio_risk"
TRADER_DEFAULT_ROWS = 50_000

TRADER_FIELDS = [
    "asset_class",
    "desk",
    "book",
    "trader",
    "ticker",
    "currency",
    "risk_tier",
    "tenor",
    "notional_usd",
    "daily_pnl",
    "unrealized_pnl",
    "var_95",
    "dv01",
    "trade_count",
    "sharpe_ratio",
]

def build_trader_table(row_count: int = TRADER_DEFAULT_ROWS) -> pa.Table:
    """Build deterministic multi-asset trading portfolio risk data using DuckDB."""
    con = duckdb.connect()
    query = f"""
    WITH raw AS (
        SELECT 
            range as id,
            (abs(hash(range, 11)) % 5) as asset_class_id,
            (abs(hash(range, 22)) % 7) as desk_id,
            (abs(hash(range, 33)) % 12) as book_id,
            (abs(hash(range, 44)) % 8) as trader_id,
            (abs(hash(range, 55)) % 16) as ticker_id,
            (abs(hash(range, 66)) % 5) as currency_id,
            (abs(hash(range, 77)) % 3) as tier_id,
            (abs(hash(range, 88)) % 6) as tenor_id,
            (sin(range * 0.17) * 0.5 + 0.5) as r1,
            (cos(range * 0.31) * 0.5 + 0.5) as r2,
            (sin(range * 0.43 + 1.2) * 0.5 + 0.5) as r3
        FROM range({row_count})
    )
    SELECT
        CASE asset_class_id
            WHEN 0 THEN 'Equities'
            WHEN 1 THEN 'Fixed Income'
            WHEN 2 THEN 'Foreign Exchange'
            WHEN 3 THEN 'Commodities'
            ELSE 'Digital Assets'
        END AS asset_class,

        CASE desk_id
            WHEN 0 THEN 'Global Macro'
            WHEN 1 THEN 'Delta One'
            WHEN 2 THEN 'Volatility Arbitrage'
            WHEN 3 THEN 'Flow Credit'
            WHEN 4 THEN 'Energy & Metals'
            WHEN 5 THEN 'Emerging Markets'
            ELSE 'Quantitative Systematic'
        END AS desk,

        CASE book_id
            WHEN 0 THEN 'Alpha-1 Master'
            WHEN 1 THEN 'Apex Liquid'
            WHEN 2 THEN 'Convergence Core'
            WHEN 3 THEN 'Nexus Vol'
            WHEN 4 THEN 'Horizon Rates'
            WHEN 5 THEN 'Solar Energy'
            WHEN 6 THEN 'Starlight Yield'
            WHEN 7 THEN 'Vanguard FX'
            WHEN 8 THEN 'Meridian Metals'
            WHEN 9 THEN 'Quantum Micro'
            WHEN 10 THEN 'Pacific Carry'
            ELSE 'Titan Tactical'
        END AS book,

        CASE trader_id
            WHEN 0 THEN 'Elena Rostova'
            WHEN 1 THEN 'Marcus Sterling'
            WHEN 2 THEN 'Chen Wei'
            WHEN 3 THEN 'Amira Al-Mansoor'
            WHEN 4 THEN 'Liam O''Connor'
            WHEN 5 THEN 'Sophia Laurent'
            WHEN 6 THEN 'Kenji Sato'
            ELSE 'Mateo Fernandez'
        END AS trader,

        CASE ticker_id
            WHEN 0 THEN 'NVDA'
            WHEN 1 THEN 'AAPL'
            WHEN 2 THEN 'MSFT'
            WHEN 3 THEN 'TSLA'
            WHEN 4 THEN 'US10Y'
            WHEN 5 THEN 'GER10Y'
            WHEN 6 THEN 'UKGILT'
            WHEN 7 THEN 'EUR/USD'
            WHEN 8 THEN 'GBP/USD'
            WHEN 9 THEN 'USD/JPY'
            WHEN 10 THEN 'BRENT_CRUDE'
            WHEN 11 THEN 'GOLD_OZ'
            WHEN 12 THEN 'COPPER_LBS'
            WHEN 13 THEN 'BTC/USD'
            WHEN 14 THEN 'ETH/USD'
            ELSE 'SOL/USD'
        END AS ticker,

        CASE currency_id
            WHEN 0 THEN 'USD'
            WHEN 1 THEN 'EUR'
            WHEN 2 THEN 'GBP'
            WHEN 3 THEN 'JPY'
            ELSE 'CHF'
        END AS currency,

        CASE tier_id
            WHEN 0 THEN 'Tier 1 (High Flow)'
            WHEN 1 THEN 'Tier 2 (Core Hedged)'
            ELSE 'Tier 3 (Tactical Vol)'
        END AS risk_tier,

        CASE tenor_id
            WHEN 0 THEN '1M'
            WHEN 1 THEN '3M'
            WHEN 2 THEN '6M'
            WHEN 3 THEN '1Y'
            WHEN 4 THEN '5Y+'
            ELSE 'Spot'
        END AS tenor,

        ROUND((
            (1800000 + asset_class_id * 950000 + desk_id * 420000 + tenor_id * 310000)
            * (0.65 + r1 * 0.85)
        ), 2) AS notional_usd,
        ROUND((
            (sin(asset_class_id * 1.3 + desk_id * 0.9 + tenor_id * 0.85 + ticker_id * 0.4) * 95000)
            + ((asset_class_id - 1.6) * 28000)
            + ((tenor_id - 2.0) * (desk_id - 2.5) * 14000)
            + ((r2 - 0.48) * 45000)
        ), 2) AS daily_pnl,
        ROUND(((r3 - 0.44) * 3800000), 2) AS unrealized_pnl,
        ROUND((
            (65000 + asset_class_id * 48000 + desk_id * 22000 + tenor_id * 15000)
            * (0.7 + r1 * 0.6)
        ), 2) AS var_95,
        ROUND((r2 * 14500 + 850 + asset_class_id * 2400), 2) AS dv01,
        CAST(ROUND(r3 * 180 + 12 + desk_id * 15) AS INTEGER) AS trade_count,
        ROUND((r1 * 2.8 - 0.4), 2) AS sharpe_ratio
    FROM raw;
    """
    tbl = con.execute(query).arrow().read_all()
    con.close()
    return tbl


@lru_cache(maxsize=1)
def get_trader_adapter():
    """Create in-memory DuckDB adapter for trading demo."""
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(TRADER_TABLE_NAME, build_trader_table())
    return adapter


def trader_workspace_config(host: str = "dash") -> dict[str, Any]:
    """Public workspace configuration for Trading Demo."""
    return {
        "id": f"trader-{host}-pivot",
        "table": TRADER_TABLE_NAME,
        "pivotTitle": "Institutional Trading Desk · Portfolio Risk & Real-Time PnL",
        "serverSide": True,
        "rowFields": ["asset_class", "desk", "ticker"],
        "colFields": ["tenor"],
        "valConfigs": [
            {
                "field": "daily_pnl",
                "agg": "sum",
                "label": "Daily PnL",
                "format": "currency",
                "sparkline": {
                    "enabled": True,
                    "type": "area",
                    "color": "#10B981",
                    "positiveColor": "#10B981",
                    "negativeColor": "#EF4444",
                    "showCurrentValue": True,
                    "showDelta": True,
                    "displayMode": "trend",
                },
            },
            {
                "field": "notional_usd",
                "agg": "sum",
                "label": "Notional",
                "format": "currency",
            },
            {
                "field": "var_95",
                "agg": "sum",
                "label": "VaR 95%",
                "format": "currency",
            },
        ],
        "availableFieldList": TRADER_FIELDS,
        # Pre-applied multi-condition filter showing filter badge
        "filters": {
            "risk_tier": {
                "operator": "AND",
                "conditions": [
                    {"type": "neq", "value": "Tier 3 (Tactical Vol)", "caseSensitive": False}
                ],
            }
        },
        "sorting": [],
        # Multi-level pre-expanded hierarchy for instant virtual scroll exploration
        "expanded": {
            "Equities": True,
            "Equities|||Global Macro": True,
            "Fixed Income": True,
            "Fixed Income|||Delta One": True,
        },
        "columnPinning": {
            "left": ["hierarchy"],
            "right": [],
        },
        "showRowTotals": True,
        "showColTotals": True,
        "showSubtotals": True,
        "grandTotalPosition": "bottom",
        "defaultTheme": "dark",
        "persistence": False,
        "performanceConfig": {
            "cacheBlockSize": 150,
            "rowOverscan": 16,
            "columnOverscan": 6,
        },
        # Docked Live Chart Pane in Workspace
        "chartCanvasPanes": [
            {
                "id": "pane-trader-risk",
                "name": "Daily PnL by Asset Class",
                "chartTitle": "Daily PnL & Exposure by Asset Class ($)",
                "source": "pivot",
                "chartType": "bar",
                "barLayout": "grouped",
                "axisMode": "vertical",
                "orientation": "rows",
                "hierarchyLevel": 1,
                "width": 440,
                "chartHeight": 360,
                "floating": False,
                "chartSettings": {
                    "showDataLabels": True,
                    "showGradient": True,
                    "showAnimations": False,
                },
            }
        ],
        # Pre-configured viewState with Heatmap ("col") & Data Bars
        "viewState": {
            "colorScaleMode": "col",
            "colorPalette": "redGreen",
            "dataBarsColumns": [
                "1M_notional_usd_sum",
                "3M_notional_usd_sum",
                "__RowTotal__notional_usd_sum",
            ],
            "chartPanelOpen": True,
            "sidebarOpen": True,
        },
        "conditionalFormatting": [
            {"condition": ">", "value": 0, "style": {"color": "#10B981", "fontWeight": "700"}},
            {"condition": "<", "value": 0, "style": {"color": "#F43F5E", "fontWeight": "700"}},
        ],
        "uiConfig": {
            "showToolbar": True,
            "showSidebar": True,
            "showCharts": True,
        },
        "style": {"height": "780px", "width": "100%"},
    }


# ==============================================================================
# 2. PEOPLE ANALYTICS & HR WORKFORCE DATASET
# ==============================================================================

HR_TABLE_NAME = "workforce_people_analytics"
HR_DEFAULT_ROWS = 35_000

HR_FIELDS = [
    "department",
    "sub_team",
    "office_location",
    "seniority_level",
    "performance_rating",
    "employment_type",
    "fiscal_year",
    "quarter",
    "headcount",
    "base_salary",
    "annual_bonus",
    "equity_grant",
    "total_compensation",
    "tenure_years",
    "engagement_score",
    "attrition_risk_pct",
]

def build_hr_table(row_count: int = HR_DEFAULT_ROWS) -> pa.Table:
    """Build deterministic workforce and compensation data using DuckDB."""
    con = duckdb.connect()
    query = f"""
    WITH raw AS (
        SELECT 
            range as id,
            (abs(hash(concat(range, '_d'))) % 6) as dept_id,
            (abs(hash(concat(range, '_t'))) % 12) as team_id,
            (abs(hash(concat(range, '_loc'))) % 8) as location_id,
            (abs(hash(concat(range, '_lvl'))) % 5) as level_id,
            (abs(hash(concat(range, '_rat'))) % 4) as rating_id,
            (abs(hash(concat(range, '_emp'))) % 3) as emp_id,
            (abs(hash(concat(range, '_yr'))) % 3) as year_id,
            (abs(hash(concat(range, '_qtr'))) % 4) as quarter_id,
            (sin(range * 0.23) * 0.5 + 0.5) as r1,
            (cos(range * 0.37) * 0.5 + 0.5) as r2,
            (sin(range * 0.49 + 2.1) * 0.5 + 0.5) as r3
        FROM range({row_count})
    )
    SELECT
        CASE dept_id
            WHEN 0 THEN 'Engineering & AI'
            WHEN 1 THEN 'Product & Design'
            WHEN 2 THEN 'Enterprise Sales'
            WHEN 3 THEN 'Marketing & Growth'
            WHEN 4 THEN 'Customer Success'
            ELSE 'People & Operations'
        END AS department,

        CASE team_id
            WHEN 0 THEN 'Core Infrastructure'
            WHEN 1 THEN 'AI & LLM Platform'
            WHEN 2 THEN 'Frontend Experience'
            WHEN 3 THEN 'Cloud Systems'
            WHEN 4 THEN 'Strategic Accounts'
            WHEN 5 THEN 'Mid-Market Sales'
            WHEN 6 THEN 'Product Management'
            WHEN 7 THEN 'Design Systems'
            WHEN 8 THEN 'Brand & Content'
            WHEN 9 THEN 'Performance Marketing'
            WHEN 10 THEN 'Solutions Engineering'
            ELSE 'People Operations'
        END AS sub_team,

        CASE location_id
            WHEN 0 THEN 'San Francisco (HQ)'
            WHEN 1 THEN 'New York'
            WHEN 2 THEN 'London'
            WHEN 3 THEN 'Berlin'
            WHEN 4 THEN 'Tokyo'
            WHEN 5 THEN 'Paris'
            WHEN 6 THEN 'Singapore'
            ELSE 'Remote (Global)'
        END AS office_location,

        CASE level_id
            WHEN 0 THEN 'L1 Associate'
            WHEN 1 THEN 'L2 Intermediate'
            WHEN 2 THEN 'L3 Senior'
            WHEN 3 THEN 'L4 Staff / Principal'
            ELSE 'L5 Executive / Director'
        END AS seniority_level,

        CASE rating_id
            WHEN 0 THEN 'Exceeds Expectations'
            WHEN 1 THEN 'Consistently Meets'
            WHEN 2 THEN 'Exemplary Leadership'
            ELSE 'Needs Coaching'
        END AS performance_rating,

        CASE emp_id
            WHEN 0 THEN 'Full-Time Regular'
            WHEN 1 THEN 'Full-Time Regular'
            ELSE 'Strategic Contractor'
        END AS employment_type,

        CASE year_id
            WHEN 0 THEN 'FY2024'
            WHEN 1 THEN 'FY2025'
            ELSE 'FY2026'
        END AS fiscal_year,

        CASE quarter_id
            WHEN 0 THEN 'Q1'
            WHEN 1 THEN 'Q2'
            WHEN 2 THEN 'Q3'
            ELSE 'Q4'
        END AS quarter,

        CAST(ROUND(1 + (dept_id * 0.4) + (quarter_id * 0.55) + (sin(dept_id * 1.4 + team_id * 0.8 + quarter_id * 1.1) * 0.7 + 0.7)) AS INTEGER) AS headcount,
        ROUND((92000 + (level_id * 34000) + (dept_id * 6500) + (quarter_id * 3200) + (r1 * 28000)), 2) AS base_salary,
        ROUND(((14000 + (level_id * 14000) + (dept_id * 2200)) * (0.8 + r2 * 0.45)), 2) AS annual_bonus,
        ROUND(((22000 + (level_id * 42000)) * (0.75 + r3 * 0.5)), 2) AS equity_grant,
        ROUND((
            (92000 + (level_id * 34000) + (dept_id * 6500) + (quarter_id * 3200) + (r1 * 28000)) +
            ((14000 + (level_id * 14000) + (dept_id * 2200)) * (0.8 + r2 * 0.45)) +
            ((22000 + (level_id * 42000)) * (0.75 + r3 * 0.5))
        ), 2) AS total_compensation,
        ROUND((r1 * 8.5 + 0.5), 1) AS tenure_years,
        ROUND((68.0 + (dept_id * 2.4) + (quarter_id * 1.8) + (r2 * 18.0)), 1) AS engagement_score,
        ROUND((0.03 + (r3 * 0.14)), 4) AS attrition_risk_pct
    FROM raw;
    """
    tbl = con.execute(query).arrow().read_all()
    con.close()
    return tbl


@lru_cache(maxsize=1)
def get_hr_adapter():
    """Create in-memory DuckDB adapter for HR People Analytics demo."""
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(HR_TABLE_NAME, build_hr_table())
    return adapter


def hr_workspace_config(host: str = "dash") -> dict[str, Any]:
    """Public workspace configuration for HR Demo."""
    return {
        "id": f"hr-{host}-pivot",
        "table": HR_TABLE_NAME,
        "pivotTitle": "Global Workforce Planning & Executive Compensation Intelligence",
        "serverSide": True,
        "rowFields": ["department", "sub_team", "seniority_level"],
        "colFields": ["quarter"],
        "valConfigs": [
            {
                "field": "headcount",
                "agg": "sum",
                "label": "Headcount",
                "format": "fixed:0",
                "sparkline": {
                    "enabled": True,
                    "type": "column",
                    "color": "#4F46E5",
                    "showCurrentValue": True,
                    "showDelta": True,
                    "displayMode": "trend",
                },
            },
            {
                "field": "base_salary",
                "agg": "avg",
                "label": "Avg Salary",
                "format": "currency",
            },
            {
                "field": "total_compensation",
                "agg": "avg",
                "label": "Total Comp",
                "format": "currency",
            },
        ],
        "availableFieldList": HR_FIELDS,
        # Active filter excluding contractors/needs coaching
        "filters": {
            "performance_rating": {
                "operator": "AND",
                "conditions": [
                    {"type": "neq", "value": "Needs Coaching", "caseSensitive": False}
                ],
            }
        },
        "sorting": [],
        # Multi-level pre-expanded hierarchy
        "expanded": {
            "Engineering & AI": True,
            "Engineering & AI|||Core Infrastructure": True,
            "Product & Design": True,
            "Product & Design|||Design Systems": True,
        },
        "columnPinning": {
            "left": ["hierarchy"],
            "right": [],
        },
        "showRowTotals": True,
        "showColTotals": True,
        "showSubtotals": True,
        "grandTotalPosition": "bottom",
        "defaultTheme": "flash",
        "persistence": False,
        "performanceConfig": {
            "cacheBlockSize": 150,
            "rowOverscan": 16,
            "columnOverscan": 6,
        },
        # Docked Donut Chart Pane in Workspace
        "chartCanvasPanes": [
            {
                "id": "pane-hr-headcount",
                "name": "Headcount by Department",
                "chartTitle": "Workforce Distribution by Department",
                "source": "pivot",
                "chartType": "donut",
                "orientation": "rows",
                "hierarchyLevel": 1,
                "width": 430,
                "chartHeight": 360,
                "floating": False,
                "chartSettings": {
                    "showDataLabels": True,
                    "showGradient": True,
                    "showAnimations": False,
                },
            }
        ],
        # Pre-configured viewState with Heatmap ("table") & Data Bars
        "viewState": {
            "colorScaleMode": "table",
            "colorPalette": "yellowBlue",
            "dataBarsColumns": [
                "Q1_headcount_sum",
                "Q2_headcount_sum",
                "Q3_headcount_sum",
                "Q4_headcount_sum",
                "__RowTotal__headcount_sum",
            ],
            "chartPanelOpen": True,
            "sidebarOpen": True,
        },
        "uiConfig": {
            "showToolbar": True,
            "showSidebar": True,
            "showCharts": True,
        },
        "style": {"height": "780px", "width": "100%"},
    }


# ==============================================================================
# 3. B2B SAAS METRICS & COHORT REVENUE DATASET
# ==============================================================================

SAAS_TABLE_NAME = "b2b_saas_metrics"
SAAS_DEFAULT_ROWS = 40_000

SAAS_FIELDS = [
    "customer_segment",
    "subscription_tier",
    "region",
    "acquisition_channel",
    "billing_cycle",
    "cohort_year",
    "cohort_quarter",
    "account_health",
    "mrr",
    "arr",
    "net_expansion",
    "churn_amount",
    "net_revenue_retention",
    "cac",
    "ltv",
    "ltv_cac_ratio",
    "active_seats",
]

def build_saas_table(row_count: int = SAAS_DEFAULT_ROWS) -> pa.Table:
    """Build deterministic B2B SaaS revenue, MRR/ARR & cohort retention data."""
    con = duckdb.connect()
    query = f"""
    WITH raw AS (
        SELECT 
            range as id,
            (abs(hash(concat(range, '_seg'))) % 4) as segment_id,
            (abs(hash(concat(range, '_tier'))) % 4) as tier_id,
            (abs(hash(concat(range, '_reg'))) % 5) as region_id,
            (abs(hash(concat(range, '_ch'))) % 4) as channel_id,
            (abs(hash(concat(range, '_cyc'))) % 3) as cycle_id,
            (abs(hash(concat(range, '_cy'))) % 3) as cohort_y_id,
            (abs(hash(concat(range, '_cq'))) % 4) as cohort_q_id,
            (abs(hash(concat(range, '_hlth'))) % 3) as health_id,
            (sin(range * 0.19) * 0.5 + 0.5) as r1,
            (cos(range * 0.29) * 0.5 + 0.5) as r2,
            (sin(range * 0.41 + 1.7) * 0.5 + 0.5) as r3
        FROM range({row_count})
    )
    SELECT
        CASE segment_id
            WHEN 0 THEN 'Enterprise ($100k+)'
            WHEN 1 THEN 'Upper Mid-Market'
            WHEN 2 THEN 'Growth SMB'
            ELSE 'Self-Serve Starter'
        END AS customer_segment,

        CASE tier_id
            WHEN 0 THEN 'Enterprise Dedicated'
            WHEN 1 THEN 'Scale Plus'
            WHEN 2 THEN 'Team Pro'
            ELSE 'Starter Essential'
        END AS subscription_tier,

        CASE region_id
            WHEN 0 THEN 'North America (East)'
            WHEN 1 THEN 'North America (West)'
            WHEN 2 THEN 'EMEA Core'
            WHEN 3 THEN 'APAC Emerging'
            ELSE 'LATAM'
        END AS region,

        CASE channel_id
            WHEN 0 THEN 'Inbound Content'
            WHEN 1 THEN 'Direct Outbound'
            WHEN 2 THEN 'Partner Ecosystem'
            ELSE 'Product-Led Growth (PLG)'
        END AS acquisition_channel,

        CASE cycle_id
            WHEN 0 THEN 'Annual Upfront'
            WHEN 1 THEN 'Multi-Year (3-Yr)'
            ELSE 'Monthly Recurring'
        END AS billing_cycle,

        CASE cohort_y_id
            WHEN 0 THEN '2024'
            WHEN 1 THEN '2025'
            ELSE '2026'
        END AS cohort_year,

        CASE cohort_q_id
            WHEN 0 THEN 'Q1'
            WHEN 1 THEN 'Q2'
            WHEN 2 THEN 'Q3'
            ELSE 'Q4'
        END AS cohort_quarter,

        CASE health_id
            WHEN 0 THEN 'Champion (Green)'
            WHEN 1 THEN 'Stable (Yellow)'
            ELSE 'At Risk (Red)'
        END AS account_health,

        ROUND((
            (CASE segment_id
                WHEN 0 THEN 9500
                WHEN 1 THEN 4200
                WHEN 2 THEN 1450
                ELSE 380
            END)
            * (1.0 + cohort_q_id * 0.16 + tier_id * 0.08 + sin(segment_id * 1.2 + tier_id * 0.9 + cohort_q_id * 0.7) * 0.22)
            * (0.75 + r1 * 0.5)
        ), 2) AS mrr,

        ROUND((
            (CASE segment_id
                WHEN 0 THEN 9500
                WHEN 1 THEN 4200
                WHEN 2 THEN 1450
                ELSE 380
            END)
            * 12.0
            * (1.0 + cohort_q_id * 0.16 + tier_id * 0.08 + sin(segment_id * 1.2 + tier_id * 0.9 + cohort_q_id * 0.7) * 0.22)
            * (0.75 + r1 * 0.5)
        ), 2) AS arr,

        ROUND((r2 * 1850 + cohort_q_id * 420 + (3 - segment_id) * 650), 2) AS net_expansion,
        ROUND((r3 * 450), 2) AS churn_amount,
        ROUND((1.08 + (3 - segment_id) * 0.05 + cohort_q_id * 0.03 + ((r1 - 0.35) * 0.24)), 4) AS net_revenue_retention,
        ROUND((1500 + ((3 - segment_id) * 4500) + (r2 * 3500)), 2) AS cac,
        ROUND((18000 + ((3 - segment_id) * 65000) + (r1 * 42000)), 2) AS ltv,
        ROUND((4.8 + (3 - segment_id) * 1.4 + cohort_q_id * 0.6 + (r3 * 4.2)), 1) AS ltv_cac_ratio,
        CAST(ROUND(18 + ((3 - segment_id) * 95) + (cohort_q_id * 28) + (r1 * 80)) AS INTEGER) AS active_seats
    FROM raw;
    """
    tbl = con.execute(query).arrow().read_all()
    con.close()
    return tbl


@lru_cache(maxsize=1)
def get_saas_adapter():
    """Create in-memory DuckDB adapter for SaaS Metrics demo."""
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(SAAS_TABLE_NAME, build_saas_table())
    return adapter


def saas_workspace_config(host: str = "dash") -> dict[str, Any]:
    """Public workspace configuration for SaaS Demo with IMMERSIVE MODE."""
    return {
        "id": f"saas-{host}-pivot",
        "table": SAAS_TABLE_NAME,
        "pivotTitle": "B2B SaaS Revenue Intelligence · Cohorts, Retention & Net ARR",
        "serverSide": True,
        "immersiveMode": False,
        "rowFields": ["customer_segment", "subscription_tier", "acquisition_channel"],
        "colFields": ["cohort_quarter"],
        "valConfigs": [
            {
                "field": "arr",
                "agg": "sum",
                "label": "Annual ARR",
                "format": "currency",
                "sparkline": {
                    "enabled": True,
                    "type": "area",
                    "color": "#8B5CF6",
                    "positiveColor": "#34D399",
                    "negativeColor": "#F43F5E",
                    "showCurrentValue": True,
                    "showDelta": True,
                    "displayMode": "trend",
                },
            },
            {
                "field": "net_revenue_retention",
                "agg": "avg",
                "label": "Net NRR %",
                "format": "percent",
            },
            {
                "field": "active_seats",
                "agg": "sum",
                "label": "Active Seats",
                "format": "fixed:0",
            },
        ],
        "availableFieldList": SAAS_FIELDS,
        # Active filter excluding churn risk
        "filters": {
            "account_health": {
                "operator": "AND",
                "conditions": [
                    {"type": "neq", "value": "At Risk (Red)", "caseSensitive": False}
                ],
            }
        },
        "sorting": [],
        # Multi-level pre-expanded hierarchy
        "expanded": {
            "Enterprise ($100k+)": True,
            "Enterprise ($100k+)|||Enterprise Dedicated": True,
            "Upper Mid-Market": True,
            "Upper Mid-Market|||Scale Plus": True,
        },
        "columnPinning": {
            "left": ["hierarchy"],
            "right": [],
        },
        "showRowTotals": True,
        "showColTotals": True,
        "showSubtotals": True,
        "grandTotalPosition": "bottom",
        "defaultTheme": "dark",
        "persistence": False,
        "performanceConfig": {
            "cacheBlockSize": 150,
            "rowOverscan": 16,
            "columnOverscan": 6,
        },
        # Docked Live Cohort Growth Area Chart
        "chartCanvasPanes": [
            {
                "id": "pane-saas-arr",
                "name": "Cohort ARR Expansion",
                "chartTitle": "ARR Growth Across Cohort Quarters ($)",
                "source": "pivot",
                "chartType": "area",
                "orientation": "rows",
                "hierarchyLevel": 1,
                "width": 440,
                "chartHeight": 360,
                "floating": False,
                "chartSettings": {
                    "showDataLabels": False,
                    "showGradient": True,
                    "showAnimations": False,
                },
            }
        ],
        # Pre-configured viewState with Heatmap ("table") & Data Bars + Collapsed Sidebar (Immersive-like clean canvas)
        "viewState": {
            "colorScaleMode": "table",
            "colorPalette": "greenRed",
            "dataBarsColumns": [
                "__RowTotal__active_seats_sum",
                "Q1_active_seats_sum",
                "Q2_active_seats_sum",
                "Q3_active_seats_sum",
            ],
            "chartPanelOpen": True,
            "sidebarOpen": False,
        },
        "uiConfig": {
            "showToolbar": True,
            "showSidebar": True,
            "showCharts": True,
        },
        "style": {"height": "800px", "width": "100%"},
    }
