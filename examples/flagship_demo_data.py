"""Shared, deterministic data and workspace configuration for flagship demos.

The examples deliberately use one data model across Dash, React, and
Streamlit. It makes the host-neutral V1 protocol visible without tying a
published example to a database, credentials, or a generated data file.
"""

from __future__ import annotations

from functools import lru_cache
import json
import os
from pathlib import Path
from random import Random
import threading
from typing import Any

import pyarrow as pa

from pivot_engine import create_tanstack_adapter
from pivot_engine.backends.execution import DatabaseExecutionError


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


def _parity_data_enabled() -> bool:
    return os.environ.get("PIVOT_FLAGSHIP_PARITY_DATA") == "1"


def _parity_rows() -> list[dict[str, Any]]:
    fixture_path = Path(__file__).resolve().parents[1] / "tests" / "data" / "flagship_parity_rows.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))["rows"]


def build_demo_table(row_count: int = DEMO_ROW_COUNT) -> pa.Table:
    """Build a stable analytics dataset with useful categorical cardinality."""
    if os.environ.get("PIVOT_FLAGSHIP_EXPORT_JOB_DATA") == "1":
        return pa.table({"region": [f"R{i:04d} 東京" for i in range(512)],
                         "sales": [i + .25 for i in range(512)]})
    if _parity_data_enabled():
        return pa.Table.from_pylist(_parity_rows())
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
    if os.environ.get("PIVOT_FLAGSHIP_EXPORT_JOB_DATA") == "1":
        _instrument_export_qualification(adapter)
    if (_parity_data_enabled() and os.environ.get("PIVOT_FLAGSHIP_TEST_FAULT_FILE")
            and os.environ.get("PIVOT_FLAGSHIP_EXPORT_JOB_DATA") != "1"):
        return _OneShotFaultAdapter(adapter)
    return adapter


def _instrument_export_qualification(adapter):
    """Private fixture instrumentation; no public route or request can arm it."""
    import asyncio
    import time
    from uuid import uuid4
    from pivot_engine.backends.duckdb_backend import DuckDBBackend
    from pivot_engine.runtime.export_service import current_export_write

    root_value = os.environ.get("PIVOT_FLAGSHIP_EVIDENCE_ROOT")
    if not root_value:
        return
    root = Path(root_value).resolve()
    root.mkdir(parents=True, exist_ok=True)
    original = adapter.controller.run_pivot_async
    native = DuckDBBackend(":memory:", max_connections=1)
    checkin = native.pool.checkin
    def slow_checkin(*args, **kwargs):
        time.sleep(.25)  # expose native cleanup while the export lease is charged
        return checkin(*args, **kwargs)
    native.pool.checkin = slow_checkin
    controls = {}

    async def query(spec, **kwargs):
        control = current_export_write.get()
        if control is not None:
            key = id(control)
            if key not in controls:
                marker = root / "slow-next-export"
                try:
                    marker.unlink()
                    armed = True
                except FileNotFoundError:
                    armed = False
                controls[key] = (control, armed)
                path = root / f"export-worker-{os.getpid()}-{uuid4().hex}.jsonl"
                def sample():
                    with path.open("w", encoding="utf-8") as output:
                        while True:
                            released = control.lease._released
                            output.write(json.dumps({
                                "at": time.time(), "executorRunning": not released,
                                "nativeRunning": native.execution_metrics()["running"],
                                "nativeCancellationAttempts": native.execution_metrics()["cancellation_attempts"],
                                "cancelRequested": control.cancelled.is_set(),
                                "reservedBytes": 0 if released else control.lease.reserved_bytes,
                                "activeSlots": control.lease._governor.snapshot()["active_slots"]["export"],
                                "temporaryFiles": [p for p in control.paths if Path(p).exists()],
                            }) + "\n")
                            output.flush()
                            if released:
                                return
                            time.sleep(.01)
                threading.Thread(target=sample, daemon=True).start()
            if controls[key][1] and (spec.offset or 0) >= 128:
                await native.execute_async({"sql": "SELECT sum(i) FROM range(10000000000) t(i)"})
            else:
                await asyncio.sleep(.35)
        return await original(spec, **kwargs)
    adapter.controller.run_pivot_async = query


class _OneShotFaultAdapter:
    """Test-only data fault; no browser request can arm it or choose its path."""

    def __init__(self, adapter):
        self._adapter = adapter

    def __getattr__(self, name):
        return getattr(self._adapter, name)

    async def handle_virtual_scroll_request(self, *args, **kwargs):
        evidence_root = os.environ.get("PIVOT_FLAGSHIP_EVIDENCE_ROOT")
        marker_path = os.environ.get("PIVOT_FLAGSHIP_TEST_FAULT_FILE")
        if not evidence_root or not marker_path:
            return await self._adapter.handle_virtual_scroll_request(*args, **kwargs)
        root = Path(evidence_root).resolve()
        marker = Path(marker_path)
        if marker.name != "fail-next-data-request" or marker.resolve() != root / "fail-next-data-request":
            return await self._adapter.handle_virtual_scroll_request(*args, **kwargs)
        try:
            armed = marker.read_bytes() == b"one-shot"
        except FileNotFoundError:
            armed = False
        if not armed:
            return await self._adapter.handle_virtual_scroll_request(*args, **kwargs)
        claimed = marker.with_name(f"{marker.name}.claimed-{os.getpid()}-{threading.get_ident()}")
        try:
            marker.rename(claimed)
        except FileNotFoundError:
            pass
        else:
            claimed.unlink()
            raise DatabaseExecutionError("PIVOT_QUALIFICATION_PRIVATE_SENTINEL")
        return await self._adapter.handle_virtual_scroll_request(*args, **kwargs)


def workspace_config(host: str) -> dict[str, Any]:
    """Return public workspace props shared by all three flagship hosts."""
    if os.environ.get("PIVOT_FLAGSHIP_EXPORT_JOB_DATA") == "1":
        return {
            "id": f"flagship-{host}-pivot", "table": TABLE_NAME,
            "pivotTitle": "Bounded export qualification", "serverSide": True,
            "rowFields": ["region"], "colFields": [],
            "valConfigs": [{"field": "sales", "agg": "sum", "label": "Sales"}],
            "availableFieldList": ["region", "sales"], "filters": {}, "sorting": [],
            "expanded": {}, "layoutMode": "tabular", "showSubtotals": False,
            "showRowTotals": False, "showColTotals": False, "persistence": False,
            "style": {"height": "760px", "width": "100%"},
        }
    if _qualification_data_enabled() or _parity_data_enabled():
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
            "availableFieldList": [
                "region", "account", "scope", "period", "ventes_€", "unités", "écart",
                *(["rowId", "extrema", "copyTab", "copyLF", "copyCRLF", "copyUnicode", "copyNull", "copyEmpty"] if _parity_data_enabled() else []),
            ],
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
