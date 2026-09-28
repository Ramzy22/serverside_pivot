"""Shared, deterministic data and workspace configuration for flagship demos.

The examples deliberately use one data model across Dash, React, and
Streamlit. It makes the host-neutral V1 protocol visible without tying a
published example to a database, credentials, or a generated data file.
"""

from __future__ import annotations

from functools import lru_cache
import os
from random import Random
from typing import Any

import pyarrow as pa

from pivot_engine import create_tanstack_adapter


TABLE_NAME = "revenue_operations"
DEMO_ROW_COUNT = 12_000

AVAILABLE_FIELDS = [
    "region",
    "segment",
    "team",
    "product",
    "channel",
    "quarter",
    "owner",
    "revenue",
    "gross_margin",
    "budget",
    "units",
    "margin_pct",
    "variance",
]

QUALIFICATION_SOURCE_ROWS = [
    {"region": "Éclair", "account": "東京", "scope": "Included", "period": "Q1", "ventes_€": 100.25, "unités": 2, "écart": -12.5},
    {"region": "Éclair", "account": "東京", "scope": "Included", "period": "Q2", "ventes_€": 20.0, "unités": 1, "écart": 2.5},
    {"region": "München", "account": "القاهرة", "scope": "Included", "period": "Q1", "ventes_€": 49.75, "unités": 3, "écart": 4.25},
    {"region": "München", "account": "القاهرة", "scope": "Included", "period": "Q2", "ventes_€": 30.0, "unités": 4, "écart": -1.0},
    {"region": "São Paulo", "account": "北京", "scope": "Excluded", "period": "Q1", "ventes_€": 999999.5, "unités": 11, "écart": 99.0},
]


def _qualification_data_enabled() -> bool:
    return os.environ.get("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA") == "1"


def build_demo_table(row_count: int = DEMO_ROW_COUNT) -> pa.Table:
    """Build a stable analytics dataset with useful categorical cardinality."""
    if _qualification_data_enabled():
        return pa.Table.from_pylist(QUALIFICATION_SOURCE_ROWS)

    random = Random(20260912)
    regions = ("North America", "EMEA", "APAC", "Latin America")
    segments = ("Enterprise", "Mid-market", "Commercial")
    teams = ("Strategic", "Growth", "Digital", "Partners")
    products = ("Atlas", "Beacon", "Canvas", "Delta", "Echo", "Flux")
    channels = ("Direct", "Partner", "Marketplace", "Expansion")
    quarters = ("Q1", "Q2", "Q3", "Q4")
    owners = ("Avery Chen", "Jordan Blake", "Morgan Lee", "Samira Patel", "Riley Stone")

    records: list[dict[str, Any]] = []
    for index in range(max(1, int(row_count))):
        region = regions[index % len(regions)]
        segment = segments[(index // 3) % len(segments)]
        product = products[(index // 11) % len(products)]
        channel = channels[(index // 17) % len(channels)]
        quarter = quarters[(index // 23) % len(quarters)]
        team = teams[(index // 41) % len(teams)]
        owner = owners[(index // 67) % len(owners)]
        product_multiplier = 1.0 + (products.index(product) * 0.11)
        region_multiplier = 1.0 + (regions.index(region) * 0.06)
        seasonal_multiplier = 0.88 + (quarters.index(quarter) * 0.09)
        units = random.randint(12, 460)
        unit_price = random.uniform(240, 1_850) * product_multiplier * region_multiplier
        revenue = round(units * unit_price * seasonal_multiplier, 2)
        margin_rate = 0.24 + random.random() * 0.38
        gross_margin = round(revenue * margin_rate, 2)
        budget = round(revenue * random.uniform(0.82, 1.16), 2)
        records.append(
            {
                "region": region,
                "segment": segment,
                "team": team,
                "product": product,
                "channel": channel,
                "quarter": quarter,
                "owner": owner,
                "revenue": revenue,
                "gross_margin": gross_margin,
                "budget": budget,
                "units": units,
                "margin_pct": round(margin_rate, 4),
                "variance": round(revenue - budget, 2),
            }
        )
    return pa.Table.from_pylist(records)


@lru_cache(maxsize=1)
def get_demo_adapter():
    """Create the in-memory adapter once per demo process."""
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(TABLE_NAME, build_demo_table())
    return adapter


def workspace_config(host: str) -> dict[str, Any]:
    """Return public workspace props shared by all three flagship hosts."""
    if _qualification_data_enabled():
        return {
            "id": f"flagship-{host}-pivot",
            "table": TABLE_NAME,
            "pivotTitle": "Export Qualification / تصدير",
            "serverSide": True,
            "rowFields": ["region", "account"],
            "colFields": ["period"],
            "valConfigs": [
                {"field": "ventes_€", "agg": "sum", "label": "Ventes (€)"},
                {"field": "unités", "agg": "sum", "label": "Unités"},
                {"field": "écart", "agg": "sum", "label": "Écart"},
            ],
            "availableFieldList": ["region", "account", "scope", "period", "ventes_€", "unités", "écart"],
            "filters": {
                "scope": {
                    "operator": "AND",
                    "conditions": [{"type": "eq", "value": "Included", "caseSensitive": False}],
                }
            },
            "sorting": [],
            "expanded": {},
            "columnOrder": [
                "hierarchy",
                "Q1_unités_sum", "Q1_ventes_€_sum", "Q1_écart_sum",
                "Q2_unités_sum", "Q2_ventes_€_sum", "Q2_écart_sum",
                "__RowTotal__unités_sum", "__RowTotal__ventes_€_sum", "__RowTotal__écart_sum",
            ],
            "showRowTotals": True,
            "showColTotals": True,
            "showSubtotals": True,
            "grandTotalPosition": "bottom",
            "defaultTheme": "dark",
            "persistence": False,
            "performanceConfig": {
                "serverSideBlockSize": 50,
                "rowOverscan": 4,
                "columnOverscan": 4,
            },
            "uiConfig": {
                "showToolbar": True,
                "showSidebar": True,
                "showCharts": True,
            },
            "style": {"height": "760px", "width": "100%"},
        }
    return {
        "id": f"flagship-{host}-pivot",
        "table": TABLE_NAME,
        "pivotTitle": "Revenue Operations Explorer",
        "serverSide": True,
        "rowFields": ["region", "segment", "product"],
        "colFields": ["quarter"],
        "valConfigs": [
            {"field": "revenue", "agg": "sum", "label": "Revenue", "format": "currency"},
            {"field": "gross_margin", "agg": "sum", "label": "Gross Margin", "format": "currency"},
            {"field": "units", "agg": "sum", "label": "Units", "format": "fixed:0"},
            {
                "field": "margin_pct",
                "agg": "weighted_avg",
                "weightField": "revenue",
                "label": "Margin %",
                "format": "percent",
            },
        ],
        "availableFieldList": AVAILABLE_FIELDS,
        "filters": {},
        "sorting": [],
        "expanded": {},
        "showRowTotals": True,
        "showColTotals": True,
        "showSubtotals": True,
        "grandTotalPosition": "bottom",
        "defaultTheme": "dark",
        "persistence": True,
        "persistence_type": "local",
        "performanceConfig": {
            "serverSideBlockSize": 200,
            "rowOverscan": 16,
            "columnOverscan": 6,
        },
        "uiConfig": {
            "showToolbar": True,
            "showSidebar": True,
            "showCharts": True,
        },
        "style": {"height": "760px", "width": "100%"},
    }
