"""Shared harness for systematic visual review of serverside-pivot views, transitions, and results.

Provides:
- Multi-route Dash showcase/test server hosting all view modes (`pivot`, `report`, `tree`, `table`,
  `wide`, `client`, `sparklines`, `detail-inline`, `detail-sidepanel`, `detail-drawer`, `live`, `fault`).
- Headless Chrome WebDriver session with 1600x1000 viewport, console error capture, and reliable
  DOM/Shadow-DOM interaction helpers.
- 3-Phase Action-Transition-Result recorder (`record_transition`) that captures screenshots and
  verifies visual conformity invariants (viewport containment, zero duplicate logical cells,
  non-overlapping row/header geometry, foreground/background contrast, CSS transition/animation
  state, canvas/SVG graphic rendering, and Pillow pixel-diff verification) across:
    1. `01_before`     — Baseline visual state prior to action
    2. `02_transition` — Active visual state launched by the action (menu/dialog/drawer/skeleton/motion)
    3. `03_after`      — Final produced visual state after transition settles
"""

from __future__ import annotations

import json
import math
import os
import socket
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import PIL.Image
import PIL.ImageChops
import PIL.ImageStat
import pyarrow as pa
import pytest
from dash import Dash, Input, Output, dcc, html
from flask import jsonify, request as flask_request
from selenium import webdriver
from selenium.common.exceptions import (
    JavascriptException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from werkzeug.serving import make_server

ROOT_DIR = Path(__file__).resolve().parents[1]
for _candidate in (
    ROOT_DIR,
    ROOT_DIR / "serverside_pivot",
    ROOT_DIR / "serverside_pivot" / "pivot_engine",
):
    if _candidate.exists() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))

from pivot_engine import RuntimeDatasetAuthorizer, create_tanstack_adapter, register_pivot_app
from serverside_pivot import DashTanstackPivot


VIEWPORT_WIDTH = 1600
VIEWPORT_HEIGHT = 1000


class _DashServerThread(threading.Thread):
    def __init__(self, app: Dash, host: str, port: int) -> None:
        super().__init__(daemon=True)
        self._server = make_server(host, port, app.server, threaded=True)

    def run(self) -> None:
        self._server.serve_forever()

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def resolve_artifact_root(suite_name: str) -> Path:
    configured = os.environ.get("PIVOT_VISUAL_REVIEW_DIR", "").strip()
    if configured:
        base = Path(configured)
    else:
        base = Path(tempfile.gettempdir()) / "serverside_pivot_visual_review"
    target = base / suite_name
    target.mkdir(parents=True, exist_ok=True)
    return target


def _build_main_dataset() -> Tuple[pa.Table, List[Dict[str, Any]]]:
    regions = ["Americas", "EMEA", "APAC", "LATAM", "Nordics", "DACH", "UKI", "Benelux"]
    segments = ["Enterprise", "Mid-Market", "SMB", "Public Sector", "Strategic"]
    channels = ["Direct", "Partner", "Cloud Marketplace", "Self-Serve"]
    quarters = ["Q1", "Q2", "Q3", "Q4"]
    months = [f"M{m:02d}" for m in range(1, 13)]
    reps = [f"Advisor {i:02d}" for i in range(1, 37)]

    rows: List[Dict[str, Any]] = []
    idx = 0
    for r_i, region in enumerate(regions):
        for s_i, segment in enumerate(segments):
            for c_i, channel in enumerate(channels):
                for q_i, quarter in enumerate(quarters):
                    idx += 1
                    rev = round(18000.0 + (r_i * 4200.0) + (s_i * 3100.0) + (c_i * 1450.0) + (q_i * 2250.0), 2)
                    cost = round(rev * (0.42 + 0.04 * (c_i % 3)), 2)
                    profit = round(rev - cost, 2)
                    margin = round((profit / rev) if rev else 0.0, 4)
                    delta_pct = round(((q_i - 1.5) * 0.065) + ((r_i % 3 - 1) * 0.03), 4)
                    units = int(40 + r_i * 12 + s_i * 9 + q_i * 7)
                    rows.append(
                        {
                            "id": idx,
                            "region": region,
                            "segment": segment,
                            "channel": channel,
                            "quarter": quarter,
                            "month": months[(idx - 1) % 12],
                            "rep": reps[(idx - 1) % len(reps)],
                            "order_date": f"2026-{(q_i * 3 + 1):02d}-15",
                            "revenue": rev,
                            "cost": cost,
                            "profit": profit,
                            "margin": margin,
                            "delta_pct": delta_pct,
                            "units": units,
                        }
                    )
    return pa.Table.from_pylist(rows), rows


def _build_tree_dataset() -> pa.Table:
    org_rows = [
        {"emp_id": "100", "manager_id": None, "org_unit": "Global Operations", "role": "Executive", "location": "New York", "headcount": 120, "budget": 2450000.0, "utilization": 0.91},
        {"emp_id": "110", "manager_id": "100", "org_unit": "Platform Engineering", "role": "VP", "location": "San Francisco", "headcount": 55, "budget": 1180000.0, "utilization": 0.94},
        {"emp_id": "111", "manager_id": "110", "org_unit": "Core Query Engine", "role": "Director", "location": "San Francisco", "headcount": 28, "budget": 620000.0, "utilization": 0.96},
        {"emp_id": "112", "manager_id": "110", "org_unit": "Virtualized UI Grid", "role": "Director", "location": "London", "headcount": 27, "budget": 560000.0, "utilization": 0.92},
        {"emp_id": "120", "manager_id": "100", "org_unit": "Enterprise Go-To-Market", "role": "VP", "location": "London", "headcount": 65, "budget": 1270000.0, "utilization": 0.88},
        {"emp_id": "121", "manager_id": "120", "org_unit": "Financial Services Desk", "role": "Director", "location": "New York", "headcount": 35, "budget": 710000.0, "utilization": 0.90},
        {"emp_id": "122", "manager_id": "120", "org_unit": "Strategic Cloud Accounts", "role": "Director", "location": "Tokyo", "headcount": 30, "budget": 560000.0, "utilization": 0.86},
        {"emp_id": "200", "manager_id": None, "org_unit": "Quantitative Research", "role": "Executive", "location": "Zurich", "headcount": 48, "budget": 1350000.0, "utilization": 0.93},
        {"emp_id": "210", "manager_id": "200", "org_unit": "Systematic Alpha", "role": "VP", "location": "Zurich", "headcount": 26, "budget": 760000.0, "utilization": 0.95},
        {"emp_id": "220", "manager_id": "200", "org_unit": "Risk & Attribution", "role": "VP", "location": "Singapore", "headcount": 22, "budget": 590000.0, "utilization": 0.91},
    ]
    return pa.Table.from_pylist(org_rows)


class _FaultInjectingAdapterProxy:
    """Thin wrapper around TanStackPivotAdapter to arm one-shot visual error/slow-export states."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.fail_next_data_requests = 0
        self.slow_export_seconds = 0.0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def _maybe_raise_simulated_fault(self) -> None:
        if self.fail_next_data_requests > 0:
            self.fail_next_data_requests -= 1
            raise RuntimeError("Simulated transient warehouse timeout")

    async def handle_virtual_scroll_request(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_raise_simulated_fault()
        return await self._inner.handle_virtual_scroll_request(*args, **kwargs)

    async def handle_hierarchical_request(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_raise_simulated_fault()
        return await self._inner.handle_hierarchical_request(*args, **kwargs)

    async def handle_request(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_raise_simulated_fault()
        return await self._inner.handle_request(*args, **kwargs)

    def handle_tanstack_request(self, request: Any) -> Any:
        self._maybe_raise_simulated_fault()
        return self._inner.handle_tanstack_request(request)


class _AdminPrincipal:
    id = "visual-reviewer"
    role = "admin"
    attributes: Dict[str, Any] = {}


def create_visual_review_app() -> Tuple[Dash, Dict[str, Any]]:
    arrow_main, records_main = _build_main_dataset()
    arrow_tree = _build_tree_dataset()

    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("vr_sales", arrow_main)
    adapter.controller.load_data_from_arrow("vr_org_tree", arrow_tree)

    live_adapter = create_tanstack_adapter(backend_uri=":memory:")
    live_adapter.controller.load_data_from_arrow("vr_live_sales", arrow_main)

    fault_proxy = _FaultInjectingAdapterProxy(adapter)

    fields = [
        "region",
        "segment",
        "channel",
        "quarter",
        "month",
        "rep",
        "order_date",
        "revenue",
        "cost",
        "profit",
        "margin",
        "delta_pct",
        "units",
    ]

    base_measures = [
        {"field": "revenue", "agg": "sum", "label": "Revenue ($)", "format": "currency"},
        {"field": "profit", "agg": "sum", "label": "Profit ($)", "format": "currency"},
        {"field": "margin", "agg": "avg", "label": "Margin %", "format": "percent"},
        {"field": "delta_pct", "agg": "avg", "label": "QoQ Delta", "format": "pill:signed_percent"},
        {"field": "units", "agg": "sum", "label": "Units", "format": "fixed:0"},
    ]

    saved_reports = [
        {
            "id": "exec-pnl-top5",
            "name": "Executive Regional P&L (Top 5)",
            "description": "Top 5 regions with bundled Other bucket and highlighted leaders",
            "colFields": ["quarter"],
            "valConfigs": [
                {"field": "revenue", "agg": "sum", "label": "Revenue ($)", "format": "currency"},
                {"field": "profit", "agg": "sum", "label": "Profit ($)", "format": "currency"},
            ],
            "reportDef": {
                "root": {
                    "field": "region",
                    "label": "Operating Region",
                    "sortBy": "revenue_sum",
                    "sortDir": "desc",
                    "topN": 5,
                    "otherBucket": True,
                    "defaultChild": {
                        "field": "segment",
                        "label": "Market Segment",
                        "sortBy": "revenue_sum",
                        "sortDir": "desc",
                    },
                },
                "formats": {
                    "UKI": {
                        "bold": True,
                        "showSubtotal": True,
                        "rowColor": "rgba(59, 130, 246, 0.14)",
                        "borderStyle": "solid",
                        "borderWidth": 2,
                        "borderColor": "#3B82F6",
                    }
                },
            },
        },
        {
            "id": "channel-mix-scorecard",
            "name": "Channel Mix Scorecard",
            "description": "Breakdown by GTM channel and segment",
            "colFields": ["quarter"],
            "valConfigs": [
                {"field": "revenue", "agg": "sum", "label": "Revenue ($)", "format": "currency"},
                {"field": "units", "agg": "sum", "label": "Units", "format": "fixed:0"},
            ],
            "reportDef": {
                "root": {
                    "field": "channel",
                    "label": "GTM Channel",
                    "sortBy": "revenue_sum",
                    "sortDir": "desc",
                },
                "formats": {},
            },
        },
    ]

    app = Dash(__name__, suppress_callback_exceptions=True, title="serverside-pivot · Visual Review Harness")

    shell_style = {
        "height": "100vh",
        "width": "100vw",
        "margin": "0",
        "padding": "12px",
        "boxSizing": "border-box",
        "background": "#0F172A",
        "overflow": "hidden",
        "display": "flex",
        "flexDirection": "column",
    }
    pivot_box_style = {"height": "calc(100vh - 24px)", "width": "100%"}

    app.layout = html.Div(
        style=shell_style,
        children=[
            dcc.Location(id="vr-url", refresh=False),
            html.Div(id="vr-page-container", style={"flex": "1", "minHeight": "0", "width": "100%"}),
        ],
    )

    @app.callback(Output("vr-page-container", "children"), Input("vr-url", "pathname"))
    def _render_route(pathname: Optional[str]) -> Any:
        route = (pathname or "/pivot").rstrip("/") or "/pivot"

        if route == "/wide":
            return DashTanstackPivot(
                id="vr-pivot-wide",
                pivotTitle="2D Virtualized Matrix",
                table="vr_sales",
                serverSide=True,
                rowFields=["rep", "region", "segment"],
                colFields=["month", "quarter"],
                valConfigs=[
                    {"field": "revenue", "agg": "sum", "label": "Revenue", "format": "currency"},
                    {"field": "units", "agg": "sum", "label": "Units", "format": "fixed:0"},
                ],
                availableFieldList=fields,
                expanded={"Advisor 01": True, "Advisor 02": True, "Advisor 03": True},
                showRowTotals=True,
                showColTotals=True,
                showSubtotals=True,
                defaultTheme="flash",
                style=pivot_box_style,
            )

        if route == "/report":
            return DashTanstackPivot(
                id="vr-pivot-report",
                pivotTitle="Executive P&L Report",
                table="vr_sales",
                serverSide=True,
                viewMode="report",
                rowFields=["region", "segment"],
                colFields=["quarter"],
                valConfigs=saved_reports[0]["valConfigs"],
                availableFieldList=fields,
                reportDef=saved_reports[0]["reportDef"],
                savedReports=saved_reports,
                activeReportId="exec-pnl-top5",
                showRowTotals=True,
                showColTotals=True,
                defaultTheme="flash",
                style=pivot_box_style,
            )

        if route in ("/tree", "/tree-configured"):
            configured_tree = (
                {
                    "sourceType": "adjacency",
                    "idField": "emp_id",
                    "parentIdField": "manager_id",
                    "labelField": "org_unit",
                    "valueFields": ["headcount", "budget", "utilization"],
                    "extraFields": ["role", "location"],
                    "displayMode": "singleColumn",
                    "groupDefaultExpanded": 0,
                    "levelLabels": ["Executive Group", "VP Division", "Director Team"],
                }
                if route == "/tree-configured"
                else {
                    "sourceType": "adjacency",
                    "idField": "emp_id",
                    "parentIdField": "manager_id",
                    "labelField": "org_unit",
                    "displayMode": "singleColumn",
                    "groupDefaultExpanded": 1,
                    "levelLabels": ["Executive Group", "VP Division", "Director Team"],
                }
            )
            return DashTanstackPivot(
                id="vr-pivot-tree",
                pivotTitle="Organization Hierarchy Tree",
                table="vr_org_tree",
                serverSide=True,
                viewMode="tree",
                rowFields=["org_unit"],
                colFields=[],
                valConfigs=[
                    {"field": "headcount", "agg": "sum", "label": "Headcount", "format": "fixed:0"},
                    {"field": "budget", "agg": "sum", "label": "Annual Budget", "format": "currency"},
                    {"field": "utilization", "agg": "avg", "label": "Utilization", "format": "percent"},
                ],
                availableFieldList=["org_unit", "role", "location", "headcount", "budget", "utilization"],
                treeConfig=configured_tree,
                showColTotals=True,
                defaultTheme="light",
                style=pivot_box_style,
            )

        if route == "/table":
            return DashTanstackPivot(
                id="vr-pivot-table",
                pivotTitle="Flat Transaction Explorer",
                table="vr_sales",
                serverSide=True,
                viewMode="table",
                rowFields=["region", "segment", "channel", "quarter"],
                colFields=[],
                valConfigs=base_measures[:3],
                availableFieldList=fields,
                showColTotals=True,
                defaultTheme="strata",
                style=pivot_box_style,
            )

        if route == "/client":
            return DashTanstackPivot(
                id="vr-pivot-client",
                pivotTitle="Client-Side Paginated Grid",
                data=records_main[:160],
                serverSide=False,
                rowFields=["region", "segment"],
                colFields=["quarter"],
                valConfigs=[
                    {"field": "revenue", "agg": "sum", "label": "Revenue ($)", "format": "currency"},
                    {"field": "units", "agg": "sum", "label": "Units", "format": "fixed:0"},
                ],
                availableFieldList=fields,
                paginationConfig={"enabled": True, "pageSize": 10},
                expanded={"Americas": True, "EMEA": True, "APAC": True, "LATAM": True},
                showRowTotals=True,
                showColTotals=True,
                defaultTheme="light",
                style=pivot_box_style,
            )

        if route == "/sparklines":
            return DashTanstackPivot(
                id="vr-pivot-sparklines",
                pivotTitle="Inline Sparklines & Trend Analytics",
                table="vr_sales",
                serverSide=True,
                rowFields=["region", "segment"],
                colFields=["quarter"],
                valConfigs=[
                    {
                        "field": "revenue",
                        "agg": "sum",
                        "label": "Revenue",
                        "format": "currency",
                        "sparkline": {
                            "type": "area",
                            "header": "Revenue Trajectory",
                            "showCurrentValue": True,
                            "showDelta": True,
                            "hideColumns": False,
                            "placement": "before",
                        },
                    },
                    {
                        "field": "profit",
                        "agg": "sum",
                        "label": "Profit",
                        "format": "currency",
                        "sparkline": {
                            "type": "bar",
                            "header": "Profit Bars",
                            "showCurrentValue": False,
                            "showDelta": True,
                            "hideColumns": True,
                        },
                    },
                    {"field": "units", "agg": "sum", "label": "Units", "format": "fixed:0"},
                ],
                availableFieldList=fields,
                showRowTotals=True,
                showColTotals=True,
                defaultTheme="bloomberg",
                style=pivot_box_style,
            )

        if route in ("/detail-inline", "/detail-sidepanel", "/detail-drawer"):
            mode = route.split("-", 1)[1]
            return DashTanstackPivot(
                id=f"vr-pivot-detail-{mode}",
                pivotTitle=f"Master-Detail ({mode.title()})",
                table="vr_sales",
                serverSide=True,
                detailMode=mode,
                detailConfig={"inlineHeight": 240, "sidepanelWidth": 460, "drawerHeight": 280, "keepDetailRows": True},
                drillEndpoint="/api/drill-through",
                rowFields=["region", "segment"],
                colFields=["quarter"],
                valConfigs=base_measures[:3],
                availableFieldList=fields,
                showRowTotals=True,
                showColTotals=True,
                defaultTheme="flash",
                style=pivot_box_style,
            )

        if route == "/live":
            return DashTanstackPivot(
                id="vr-pivot-live",
                pivotTitle="Live CDC Streaming Monitor",
                table="vr_live_sales",
                serverSide=True,
                streamingConfig={"enabled": True, "cadenceMs": 250, "flashEnabled": True},
                rowFields=["region", "segment"],
                colFields=["quarter"],
                valConfigs=[
                    {"field": "revenue", "agg": "sum", "label": "Live Revenue", "format": "currency"},
                    {"field": "units", "agg": "sum", "label": "Live Units", "format": "fixed:0"},
                ],
                availableFieldList=fields,
                showRowTotals=True,
                showColTotals=True,
                defaultTheme="dark",
                style=pivot_box_style,
            )

        if route == "/fault":
            return DashTanstackPivot(
                id="vr-pivot-fault",
                pivotTitle="Resilience & Recovery Workspace",
                table="vr_sales",
                serverSide=True,
                rowFields=["region", "segment"],
                colFields=["quarter"],
                valConfigs=base_measures[:2],
                availableFieldList=fields,
                showRowTotals=True,
                showColTotals=True,
                defaultTheme="flash",
                style=pivot_box_style,
            )

        return DashTanstackPivot(
            id="vr-pivot-main",
            pivotTitle="Global Revenue Workspace",
            table="vr_sales",
            serverSide=True,
            detailMode="none",
            drillEndpoint="/api/drill-through",
            rowFields=["region", "segment", "channel"],
            colFields=["quarter"],
            valConfigs=base_measures,
            availableFieldList=fields,
            showRowTotals=True,
            showColTotals=True,
            showSubtotals=True,
            defaultTheme="flash",
            style=pivot_box_style,
        )

    policy = RuntimeDatasetAuthorizer(
        {
            "vr_sales": {"roles": {"admin"}, "operations": {"read", "export"}, "rowSecurity": {"mode": "global"}},
            "vr_org_tree": {"roles": {"admin"}, "operations": {"read", "export"}, "rowSecurity": {"mode": "global"}},
            "vr_live_sales": {"roles": {"admin"}, "operations": {"read", "export"}, "rowSecurity": {"mode": "global"}},
        }
    )

    for pid in (
        "vr-pivot-main",
        "vr-pivot-wide",
        "vr-pivot-report",
        "vr-pivot-tree",
        "vr-pivot-table",
        "vr-pivot-sparklines",
        "vr-pivot-detail-inline",
        "vr-pivot-detail-sidepanel",
        "vr-pivot-detail-drawer",
    ):
        register_pivot_app(
            app,
            adapter_getter=lambda: adapter,
            pivot_id=pid,
            runtime_dataset_authorizer=policy,
            runtime_principal_getter=lambda _req: _AdminPrincipal(),
        )

    register_pivot_app(
        app,
        adapter_getter=lambda: live_adapter,
        pivot_id="vr-pivot-live",
        runtime_dataset_authorizer=policy,
        runtime_principal_getter=lambda _req: _AdminPrincipal(),
    )

    register_pivot_app(
        app,
        adapter_getter=lambda: fault_proxy,
        pivot_id="vr-pivot-fault",
        runtime_dataset_authorizer=policy,
        runtime_principal_getter=lambda _req: _AdminPrincipal(),
    )

    @app.server.route("/vr-control/arm-fault", methods=["POST"])
    def _arm_fault() -> Any:
        count = int((flask_request.get_json(silent=True) or {}).get("count", 1))
        fault_proxy.fail_next_data_requests = count
        return jsonify({"ok": True, "armed": count})

    @app.server.route("/vr-control/push-live-tick", methods=["POST"])
    def _push_live_tick() -> Any:
        body = flask_request.get_json(silent=True) or {}
        bump = float(body.get("delta", 7500.0))
        updates = [
            {"id": 1, "revenue": 42000.0 + bump, "units": 95},
            {"id": 2, "revenue": 39500.0 + bump * 0.5, "units": 82},
            {"id": 21, "revenue": 46000.0 + bump * 0.8, "units": 110},
        ]
        live_adapter.push_stream_changes("vr_live_sales", updates, mode="update", key_fields=["id"])
        return jsonify({"ok": True, "updated": len(updates)})

    return app, {"adapter": adapter, "live_adapter": live_adapter, "fault_proxy": fault_proxy}


_VISUAL_AUDIT_JS = r"""
const vw = window.innerWidth;
const vh = window.innerHeight;
const docScrollWidth = document.documentElement.scrollWidth;

function parseRgb(str) {
  if (!str) return null;
  const m = str.match(/rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)(?:\s*,\s*([\d.]+))?\s*\)/i);
  if (!m) return null;
  return { r: Number(m[1]), g: Number(m[2]), b: Number(m[3]), a: m[4] === undefined ? 1 : Number(m[4]) };
}

function effectiveBg(el) {
  let cur = el;
  while (cur && cur.nodeType === 1) {
    const bg = parseRgb(getComputedStyle(cur).backgroundColor);
    if (bg && bg.a >= 0.15) return bg;
    cur = cur.parentElement || (cur.getRootNode && cur.getRootNode().host) || null;
  }
  return { r: 255, g: 255, b: 255, a: 1 };
}

function relLum(rgb) {
  const f = (c) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  };
  return 0.2126 * f(rgb.r) + 0.7152 * f(rgb.g) + 0.0722 * f(rgb.b);
}

function contrastRatio(fg, bg) {
  const l1 = relLum(fg), l2 = relLum(bg);
  return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
}

function isVisible(el) {
  if (!el || !el.isConnected) return false;
  const st = getComputedStyle(el);
  if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity) === 0) return false;
  const r = el.getBoundingClientRect();
  return r.width > 0.5 && r.height > 0.5;
}

const roots = [];
const allEls = [];
function collect(root) {
  for (const el of root.querySelectorAll('*')) {
    allEls.push(el);
    if (el.hasAttribute('data-pivot-workspace-root')) roots.push(el);
    if (el.shadowRoot) collect(el.shadowRoot);
  }
}
collect(document);

const headers = [];
const cells = [];
const overlays = [];
const charts = [];
const lowContrastSamples = [];

for (const el of allEls) {
  if (!isVisible(el)) continue;
  const r = el.getBoundingClientRect();
  const role = el.getAttribute('role');

  if (role === 'columnheader') {
    headers.push({
      text: (el.innerText || '').trim().slice(0, 80),
      left: Math.round(r.left * 10) / 10,
      top: Math.round(r.top * 10) / 10,
      width: Math.round(r.width * 10) / 10,
      height: Math.round(r.height * 10) / 10,
      sort: el.getAttribute('aria-sort') || 'none',
    });
  } else if (role === 'gridcell' && el.dataset && el.dataset.rowid && el.dataset.colid) {
    const st = getComputedStyle(el);
    const fg = parseRgb(st.color);
    const bg = effectiveBg(el);
    const txt = (el.innerText || '').trim();
    const cr = (fg && bg) ? Math.round(contrastRatio(fg, bg) * 100) / 100 : null;
    if (txt.length > 0 && cr !== null && cr < 2.2) {
      lowContrastSamples.push({ rowid: el.dataset.rowid, colid: el.dataset.colid, text: txt.slice(0, 30), contrast: cr });
    }
    cells.push({
      rowid: el.dataset.rowid,
      colid: el.dataset.colid,
      text: txt.slice(0, 80),
      left: Math.round(r.left * 10) / 10,
      top: Math.round(r.top * 10) / 10,
      width: Math.round(r.width * 10) / 10,
      height: Math.round(r.height * 10) / 10,
      selected: el.getAttribute('aria-selected') === 'true',
      bg: st.backgroundColor,
      color: st.color,
      fontWeight: st.fontWeight,
      fontStyle: st.fontStyle,
      contrast: cr,
    });
  }

  if (
    role === 'dialog' ||
    role === 'menu' ||
    el.hasAttribute('data-cell-format-popover') ||
    el.classList.contains('sp-toast-surface')
  ) {
    const st = getComputedStyle(el);
    overlays.push({
      tag: el.tagName.toLowerCase(),
      role: role || (el.classList.contains('sp-toast-surface') ? 'toast' : 'popover'),
      label: el.getAttribute('aria-label') || (el.innerText || '').trim().split('\n')[0].slice(0, 60),
      left: Math.round(r.left * 10) / 10,
      top: Math.round(r.top * 10) / 10,
      right: Math.round(r.right * 10) / 10,
      bottom: Math.round(r.bottom * 10) / 10,
      width: Math.round(r.width * 10) / 10,
      height: Math.round(r.height * 10) / 10,
      zIndex: st.zIndex,
      animationName: st.animationName,
      withinViewport: r.left >= -2 && r.top >= -2 && r.right <= vw + 4 && r.bottom <= vh + 4,
    });
  }

  if (el.tagName === 'CANVAS' || (el.tagName === 'svg' && r.width > 60 && r.height > 20)) {
    charts.push({
      kind: el.tagName.toLowerCase(),
      width: Math.round(r.width),
      height: Math.round(r.height),
      left: Math.round(r.left),
      top: Math.round(r.top),
      svgChildren: el.tagName === 'svg' ? el.querySelectorAll('path,rect,circle,line,polygon,text').length : null,
    });
  }
}

const logicalCellKeys = cells.filter(c => c.colid !== '__row_number__').map(c => `${c.rowid}:::${c.colid}`);
const uniqueLogicalKeys = new Set(logicalCellKeys);
const duplicateLogicalCellCount = logicalCellKeys.length - uniqueLogicalKeys.size;

const mainWorkspace = roots[0] || null;
const wsRect = mainWorkspace ? mainWorkspace.getBoundingClientRect() : null;
const wsStyle = mainWorkspace ? getComputedStyle(mainWorkspace) : null;

return {
  viewport: { width: vw, height: vh, docScrollWidth },
  workspace: wsRect ? {
    width: Math.round(wsRect.width),
    height: Math.round(wsRect.height),
    primaryColor: wsStyle.getPropertyValue('--sp-primary').trim(),
    surfaceBg: wsStyle.getPropertyValue('--sp-surface-bg').trim(),
    textColor: wsStyle.getPropertyValue('--sp-text').trim(),
  } : null,
  headerCount: headers.length,
  headers: headers.slice(0, 30),
  cellCount: cells.length,
  selectedCellCount: cells.filter(c => c.selected).length,
  duplicateLogicalCellCount,
  lowContrastSamples: lowContrastSamples.slice(0, 10),
  overlays,
  charts,
  expandedRowButtons: allEls.filter(e => e.classList && e.classList.contains('sp-expander-btn') && isVisible(e)).map(e => ({
    expanded: e.getAttribute('aria-expanded'),
    fetching: e.classList.contains('sp-expander-fetching'),
  })),
  liveBarVisible: allEls.some(e => e.hasAttribute && e.hasAttribute('data-live-stream-diff-bar') && isVisible(e)),
  errorBoundaryVisible: (document.body.innerText || '').includes('Something went wrong in Pivot'),
};
"""


@dataclass
class PhaseCapture:
    phase: str
    screenshot_path: str
    image_size: Tuple[int, int]
    pixel_std_dev: float
    diff_pct_from_before: float
    dom_audit: Dict[str, Any]
    severe_console_errors: List[str]


@dataclass
class TransitionEvidence:
    case_id: str
    description: str
    route: str
    phases: Dict[str, PhaseCapture] = field(default_factory=dict)


class VisualReviewSession:
    """Manages one headless Chrome browser + multi-route Dash server for visual transition tests."""

    def __init__(self, suite_name: str, base_url: str, driver: webdriver.Chrome, controls: Dict[str, Any]) -> None:
        self.suite_name = suite_name
        self.base_url = base_url.rstrip("/")
        self.driver = driver
        self.controls = controls
        self.artifact_dir = resolve_artifact_root(suite_name)
        self.records: List[TransitionEvidence] = []

    def open_route(self, route: str, *, wait_for_cells: bool = True, timeout: float = 20.0) -> None:
        url = f"{self.base_url}{route if route.startswith('/') else '/' + route}"
        self.driver.get(url)
        # Drain stale console entries from prior route
        self._drain_severe_logs()
        if wait_for_cells:
            self.wait_until(
                lambda: len(self.driver.find_elements(By.CSS_SELECTOR, '[role="gridcell"][data-rowid][data-colid]')) > 0,
                timeout=timeout,
                message=f"Timed out waiting for grid cells on route {route}",
            )
            self.settle(0.25)

    def settle(self, seconds: float = 0.2) -> None:
        time.sleep(seconds)
        try:
            self.driver.execute_async_script(
                "const done = arguments[0]; requestAnimationFrame(() => requestAnimationFrame(done));"
            )
        except WebDriverException:
            pass

    def wait_for_settled_grid(self, timeout: float = 8.0) -> None:
        """Wait for any in-flight runtime request ('Loading' badge / skeleton state) to settle."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                is_loading = self.driver.execute_script(
                    """
                    const statusPanels = [...document.querySelectorAll('[data-pivot-status-panel]')];
                    const hasLoadingBadge = statusPanels.some(el => /loading/i.test(el.innerText || '') && el.offsetParent !== null);
                    const hasFetchingExpander = Boolean(document.querySelector('.sp-expander-fetching'));
                    return hasLoadingBadge || hasFetchingExpander;
                    """
                )
                if not is_loading:
                    break
            except WebDriverException:
                break
            time.sleep(0.12)
        self.settle(0.2)

    def wait_until(self, predicate: Callable[[], Any], timeout: float = 12.0, message: str = "Condition timed out") -> Any:
        deadline = time.monotonic() + timeout
        last_exc: Optional[Exception] = None
        while time.monotonic() < deadline:
            try:
                val = predicate()
                if val:
                    return val
            except (StaleElementReferenceException, JavascriptException, WebDriverException) as exc:
                last_exc = exc
            time.sleep(0.1)
        raise TimeoutError(f"{message} (last_exc={last_exc})")

    def _drain_severe_logs(self) -> List[str]:
        severe: List[str] = []
        try:
            entries = self.driver.get_log("browser")
        except Exception:
            return severe
        for entry in entries or []:
            if entry.get("level") == "SEVERE":
                msg = str(entry.get("message", ""))
                # Ignore benign favicon or external Google Fonts offline warnings, and expected simulated fault 500s
                if any(
                    benign in msg
                    for benign in (
                        "favicon.ico",
                        "fonts.googleapis.com",
                        "fonts.gstatic.com",
                        "Simulated transient warehouse timeout",
                    )
                ):
                    continue
                severe.append(msg)
        return severe

    def audit_dom(self) -> Dict[str, Any]:
        return dict(self.driver.execute_script(_VISUAL_AUDIT_JS))

    def _compute_image_metrics(
        self, png_path: Path, baseline_path: Optional[Path]
    ) -> Tuple[Tuple[int, int], float, float]:
        with PIL.Image.open(png_path) as img:
            rgb = img.convert("RGB")
            size = rgb.size
            stat = PIL.ImageStat.Stat(rgb)
            std_dev = float(sum(stat.stddev) / len(stat.stddev))
            diff_pct = 0.0
            if baseline_path is not None and baseline_path.exists():
                with PIL.Image.open(baseline_path) as base_img:
                    base_rgb = base_img.convert("RGB")
                    if base_rgb.size == rgb.size:
                        arr_a = np.asarray(rgb, dtype=np.int16)
                        arr_b = np.asarray(base_rgb, dtype=np.int16)
                        changed = np.any(np.abs(arr_a - arr_b) > 6, axis=-1)
                        diff_pct = round(float(np.mean(changed) * 100.0), 4)
        return size, round(std_dev, 3), diff_pct

    def assert_visual_invariants(
        self,
        capture: PhaseCapture,
        *,
        allow_zero_cells: bool = False,
        allow_low_contrast: bool = False,
    ) -> None:
        audit = capture.dom_audit
        assert capture.image_size[0] >= 1200 and capture.image_size[1] >= 700, (
            f"[{capture.phase}] Unexpected screenshot dimensions: {capture.image_size}"
        )
        assert capture.pixel_std_dev > 4.0, (
            f"[{capture.phase}] Screenshot appears visually blank (std_dev={capture.pixel_std_dev})"
        )
        assert not audit.get("errorBoundaryVisible"), f"[{capture.phase}] React PivotErrorBoundary triggered!"
        assert not capture.severe_console_errors, (
            f"[{capture.phase}] Severe browser console errors: {capture.severe_console_errors}"
        )
        assert audit["duplicateLogicalCellCount"] == 0, (
            f"[{capture.phase}] Duplicate logical (rowid, colid) cells rendered: {audit['duplicateLogicalCellCount']}"
        )
        if not allow_zero_cells:
            assert audit["cellCount"] > 0 or audit["headerCount"] > 0, (
                f"[{capture.phase}] Neither headers nor grid cells visible in DOM"
            )
        for ov in audit.get("overlays", []):
            assert ov["withinViewport"], (
                f"[{capture.phase}] Overlay '{ov['label']}' ({ov['role']}) clipped outside viewport: {ov}"
            )
        if not allow_low_contrast:
            assert len(audit.get("lowContrastSamples", [])) == 0, (
                f"[{capture.phase}] Unreadable low-contrast cells detected: {audit['lowContrastSamples']}"
            )

    def capture_phase(
        self,
        evidence: TransitionEvidence,
        phase_name: str,
        *,
        baseline_path: Optional[Path] = None,
        allow_zero_cells: bool = False,
        allow_low_contrast: bool = False,
    ) -> PhaseCapture:
        png_path = self.artifact_dir / f"{evidence.case_id}_{phase_name}.png"
        self.driver.save_screenshot(str(png_path))
        dom_audit = self.audit_dom()
        severe_errors = self._drain_severe_logs()
        img_size, std_dev, diff_pct = self._compute_image_metrics(png_path, baseline_path)
        cap = PhaseCapture(
            phase=phase_name,
            screenshot_path=str(png_path),
            image_size=img_size,
            pixel_std_dev=std_dev,
            diff_pct_from_before=diff_pct,
            dom_audit=dom_audit,
            severe_console_errors=severe_errors,
        )
        evidence.phases[phase_name] = cap
        self.assert_visual_invariants(
            cap,
            allow_zero_cells=allow_zero_cells,
            allow_low_contrast=allow_low_contrast,
        )
        return cap

    def record_transition(
        self,
        case_id: str,
        description: str,
        *,
        route: Optional[str] = None,
        setup_fn: Optional[Callable[["VisualReviewSession"], None]] = None,
        action_to_transition_fn: Callable[["VisualReviewSession"], None],
        transition_check_fn: Optional[Callable[[PhaseCapture, PhaseCapture], None]] = None,
        complete_to_after_fn: Optional[Callable[["VisualReviewSession"], None]] = None,
        after_check_fn: Optional[Callable[[PhaseCapture, PhaseCapture, PhaseCapture], None]] = None,
        expect_visual_change_in_transition: bool = True,
        expect_visual_change_in_after: bool = True,
        allow_zero_cells: bool = False,
        allow_low_contrast: bool = False,
        route_timeout: float = 20.0,
    ) -> TransitionEvidence:
        if route is not None:
            self.open_route(route, wait_for_cells=not allow_zero_cells, timeout=route_timeout)
        if setup_fn is not None:
            setup_fn(self)
            self.settle(0.2)

        ev = TransitionEvidence(case_id=case_id, description=description, route=route or "current")
        before = self.capture_phase(
            ev, "01_before", allow_zero_cells=allow_zero_cells, allow_low_contrast=allow_low_contrast
        )
        before_path = Path(before.screenshot_path)

        action_to_transition_fn(self)
        self.settle(0.18)
        transition = self.capture_phase(
            ev,
            "02_transition",
            baseline_path=before_path,
            allow_zero_cells=allow_zero_cells,
            allow_low_contrast=allow_low_contrast,
        )
        if expect_visual_change_in_transition:
            assert transition.diff_pct_from_before > 0.01, (
                f"[{case_id}] Action launched no detectable visual change in transition phase "
                f"(diff_pct={transition.diff_pct_from_before}%)"
            )
        if transition_check_fn is not None:
            transition_check_fn(before, transition)

        if complete_to_after_fn is not None:
            complete_to_after_fn(self)
        self.wait_for_settled_grid(timeout=8.0)
        after = self.capture_phase(
            ev,
            "03_after",
            baseline_path=before_path,
            allow_zero_cells=allow_zero_cells,
            allow_low_contrast=allow_low_contrast,
        )
        if expect_visual_change_in_after:
            assert after.diff_pct_from_before > 0.01, (
                f"[{case_id}] Action produced no detectable visual change in final result "
                f"(diff_pct={after.diff_pct_from_before}%)"
            )
        if after_check_fn is not None:
            after_check_fn(before, transition, after)

        self.records.append(ev)
        self.write_manifest()
        return ev

    def write_manifest(self) -> Path:
        manifest_path = self.artifact_dir / "visual_review_manifest.json"
        payload = {
            "suite": self.suite_name,
            "caseCount": len(self.records),
            "cases": [
                {
                    "caseId": item.case_id,
                    "description": item.description,
                    "route": item.route,
                    "phases": {
                        k: {
                            "screenshot": v.screenshot_path,
                            "imageSize": list(v.image_size),
                            "pixelStdDev": v.pixel_std_dev,
                            "diffPctFromBefore": v.diff_pct_from_before,
                            "headerCount": v.dom_audit.get("headerCount"),
                            "cellCount": v.dom_audit.get("cellCount"),
                            "selectedCellCount": v.dom_audit.get("selectedCellCount"),
                            "overlays": v.dom_audit.get("overlays"),
                            "charts": v.dom_audit.get("charts"),
                            "lowContrastSamples": v.dom_audit.get("lowContrastSamples"),
                            "workspace": v.dom_audit.get("workspace"),
                        }
                        for k, v in item.phases.items()
                    },
                }
                for item in self.records
            ],
        }
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return manifest_path

    # --- High-level UI interaction helpers ---

    def find_visible(self, css_selector: str, *, text_contains: Optional[str] = None) -> List[Any]:
        candidates = self.driver.find_elements(By.CSS_SELECTOR, css_selector)
        out = []
        for el in candidates:
            try:
                if not el.is_displayed():
                    continue
                if text_contains is not None and text_contains not in (el.text or ""):
                    continue
                out.append(el)
            except StaleElementReferenceException:
                continue
        return out

    def click_button(self, *, testid: Optional[str] = None, text: Optional[str] = None, title_contains: Optional[str] = None, aria_label: Optional[str] = None) -> Any:
        def _locate() -> Any:
            if testid:
                els = self.find_visible(f'[data-testid="{testid}"]')
                if els:
                    return els[0]
            if aria_label:
                els = self.find_visible(f'[aria-label="{aria_label}"]')
                if els:
                    return els[0]
            if title_contains:
                for el in self.find_visible("button, [role='button'], summary"):
                    t = el.get_attribute("title") or ""
                    if title_contains in t:
                        return el
            if text is not None:
                for el in self.find_visible("button, [role='button'], [role='menuitem'], summary"):
                    label = (el.text or "").strip()
                    if label == text or text in label:
                        return el
            return None

        target = self.wait_until(_locate, timeout=8.0, message=f"Button not found (testid={testid}, text={text}, title={title_contains}, aria={aria_label})")
        self.driver.execute_script("arguments[0].scrollIntoView({block: 'nearest', inline: 'nearest'});", target)
        try:
            target.click()
        except WebDriverException:
            self.driver.execute_script("arguments[0].click();", target)
        return target

    def open_toolbar_section(self, section_id: str) -> None:
        btn = self.wait_until(
            lambda: (self.find_visible(f'[data-testid="pivot-toolbar-{section_id}"]') or [None])[0],
            timeout=8.0,
            message=f"Toolbar section toggle {section_id} not found",
        )
        cls = btn.get_attribute("class") or ""
        if "sp-btn-active" not in cls:
            btn.click()
            self.settle(0.15)

    def select_themed_option(self, select_el: Any, option_value_or_text: str) -> None:
        """Handles both native <select> and custom ThemedSelect button/listbox controls."""
        tag = (select_el.tag_name or "").lower()
        if tag == "select":
            from selenium.webdriver.support.ui import Select
            sel = Select(select_el)
            try:
                sel.select_by_value(option_value_or_text)
            except Exception:
                sel.select_by_visible_text(option_value_or_text)
            return
        select_el.click()
        self.settle(0.15)
        target_norm = option_value_or_text.strip().lower()
        option = self.wait_until(
            lambda: (
                next(
                    (
                        el
                        for el in self.find_visible('[data-themed-select-menu="true"] [role="option"], [role="listbox"] [role="option"], [data-themed-select-menu="true"] button')
                        if (el.get_attribute("data-value") or "").strip().lower() == target_norm
                        or (el.text or "").strip().rstrip("✓").strip().lower() == target_norm
                    ),
                    None,
                )
                or next(
                    (
                        el
                        for el in self.find_visible('[data-themed-select-menu="true"] [role="option"], [role="listbox"] [role="option"], [data-themed-select-menu="true"] button')
                        if target_norm in ((el.get_attribute("data-value") or "") + " " + (el.text or "")).lower()
                    ),
                    None,
                )
            ),
            timeout=6.0,
            message=f"ThemedSelect option '{option_value_or_text}' not found",
        )
        option.click()
        self.settle(0.15)


@pytest.fixture(scope="module")
def visual_session_factory():
    """Module-scoped fixture yielding a factory that creates an isolated server + Chrome session."""
    if os.environ.get("PIVOT_SKIP_VISUAL_REVIEW") == "1":
        pytest.skip("Skipped via PIVOT_SKIP_VISUAL_REVIEW=1")

    created: List[Tuple[VisualReviewSession, _DashServerThread]] = []

    def _create(suite_name: str) -> VisualReviewSession:
        app, controls = create_visual_review_app()
        port = find_free_port()
        server = _DashServerThread(app, "127.0.0.1", port)
        server.start()

        opts = ChromeOptions()
        opts.add_argument("--headless=new")
        opts.add_argument(f"--window-size={VIEWPORT_WIDTH},{VIEWPORT_HEIGHT}")
        opts.add_argument("--force-device-scale-factor=1")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-background-networking")
        opts.set_capability("goog:loggingPrefs", {"browser": "ALL"})

        try:
            driver = webdriver.Chrome(options=opts)
        except WebDriverException as exc:
            server.shutdown()
            pytest.skip(f"Headless Chrome unavailable: {exc}")

        driver.set_window_size(VIEWPORT_WIDTH, VIEWPORT_HEIGHT)
        session = VisualReviewSession(suite_name, f"http://127.0.0.1:{port}", driver, controls)
        created.append((session, server))
        return session

    yield _create

    for session, server in created:
        try:
            session.write_manifest()
        except Exception:
            pass
        try:
            session.driver.quit()
        except Exception:
            pass
        try:
            server.shutdown()
        except Exception:
            pass
