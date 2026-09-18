#!/usr/bin/env python3
"""Benchmark the direct V1 transport used by the flagship React hosts.

This is deliberately narrower than a browser benchmark. It measures the
JSON-over-HTTP path from a local client through the configured endpoint and
back, including the runtime gateway and JSON serialization, but excludes DOM
layout and React paint. That separation makes it useful when comparing the
React dev proxy with Streamlit's direct endpoint without overstating it as a
browser-to-paint result.

Start ``examples/launch_flagship_demos.ps1`` first, then run::

    python scripts/benchmark_flagship_transport.py --requests 30
    python scripts/benchmark_flagship_transport.py --json
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from typing import Any, Iterable

import requests


DEFAULT_ENDPOINTS = {
    "React (Vite dev proxy)": "http://127.0.0.1:5174/v1/runtime",
    "Streamlit (direct V1)": "http://127.0.0.1:8765/v1/runtime",
}


def percentile(values: Iterable[float], percentile_value: float) -> float | None:
    """Return the nearest-rank percentile, appropriate for small p95 samples."""
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return None
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * percentile_value) - 1))
    return round(ordered[index], 3)


def flagship_request(sequence: int) -> dict[str, Any]:
    """Build the same initial structural V1 request as the published demo."""
    return {
        "schemaVersion": 1,
        "kind": "data",
        "requestId": f"flagship-transport-{sequence}",
        "table": "revenue_operations",
        "sessionId": "flagship-transport-benchmark",
        "clientInstance": "flagship-transport-benchmark",
        "stateEpoch": 1,
        "windowSeq": sequence,
        "state": {
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
            "filters": {},
            "sorting": [],
            "expanded": {},
            "showRowTotals": True,
            "showColTotals": True,
            "showSubtotals": True,
        },
        "payload": {
            "intent": "structural",
            "start": 0,
            "end": 199,
            "count": 200,
            "version": sequence,
            "window_seq": sequence,
            "state_epoch": 1,
            "needs_col_schema": True,
            "include_grand_total": True,
            "layout_mode": "hierarchy",
            "show_subtotal_footers": True,
        },
    }


def _gateway_ms(body: dict[str, Any]) -> float | None:
    profile = body.get("profile") if isinstance(body.get("profile"), dict) else {}
    transport = profile.get("transport") if isinstance(profile.get("transport"), dict) else {}
    value = transport.get("gatewayMs")
    return float(value) if isinstance(value, (int, float)) and math.isfinite(value) else None


def measure_endpoint(
    endpoint: str,
    *,
    samples: int,
    warmup: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Warm an endpoint then collect sequential structural request timings."""
    if samples < 1:
        raise ValueError("samples must be at least 1")
    if warmup < 0:
        raise ValueError("warmup cannot be negative")

    session = requests.Session()
    sequence = 0

    def post_once() -> tuple[float, int, dict[str, Any]]:
        nonlocal sequence
        sequence += 1
        started_at = time.perf_counter()
        response = session.post(endpoint, json=flagship_request(sequence), timeout=timeout_seconds)
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        response.raise_for_status()
        body = response.json()
        if body.get("schemaVersion") != 1 or body.get("status") != "data":
            raise RuntimeError(f"{endpoint} returned an unexpected runtime response: {body!r}")
        return elapsed_ms, len(response.content), body

    for _ in range(warmup):
        post_once()

    elapsed_values: list[float] = []
    byte_values: list[float] = []
    gateway_values: list[float] = []
    row_counts: list[int] = []
    column_counts: list[int] = []
    for _ in range(samples):
        elapsed_ms, byte_count, body = post_once()
        payload = body.get("payload") if isinstance(body.get("payload"), dict) else {}
        elapsed_values.append(elapsed_ms)
        byte_values.append(float(byte_count))
        gateway_ms = _gateway_ms(body)
        if gateway_ms is not None:
            gateway_values.append(gateway_ms)
        row_counts.append(int(payload.get("rowCount", 0)))
        columns = payload.get("columns")
        column_counts.append(len(columns) if isinstance(columns, list) else 0)

    return {
        "completedSamples": len(elapsed_values),
        "configuredWarmup": warmup,
        "rowCount": row_counts[-1] if row_counts else None,
        "columnCount": column_counts[-1] if column_counts else None,
        "timingsMs": {
            "httpP50": percentile(elapsed_values, 0.5),
            "httpP95": percentile(elapsed_values, 0.95),
            "runtimeGatewayP50": percentile(gateway_values, 0.5),
            "runtimeGatewayP95": percentile(gateway_values, 0.95),
        },
        "responseBytes": {
            "p50": percentile(byte_values, 0.5),
            "p95": percentile(byte_values, 0.95),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure the flagship direct V1 transport paths.")
    parser.add_argument("--requests", type=int, default=30, help="Measured requests per endpoint (default: 30).")
    parser.add_argument("--warmup", type=int, default=5, help="Unreported warmup requests per endpoint (default: 5).")
    parser.add_argument("--timeout", type=float, default=15, help="Per-request timeout in seconds (default: 15).")
    parser.add_argument("--react-endpoint", default=DEFAULT_ENDPOINTS["React (Vite dev proxy)"], help="React runtime URL.")
    parser.add_argument("--streamlit-endpoint", default=DEFAULT_ENDPOINTS["Streamlit (direct V1)"], help="Streamlit runtime URL.")
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.requests < 20:
        raise ValueError("requests must be at least 20 for a meaningful p95")

    endpoints = {
        "React (Vite dev proxy)": args.react_endpoint,
        "Streamlit (direct V1)": args.streamlit_endpoint,
    }
    report = {
        "scope": "local warmed V1 HTTP transport; excludes browser scheduling and paint",
        "requestsPerEndpoint": args.requests,
        "hosts": {
            label: {
                "endpoint": endpoint,
                **measure_endpoint(
                    endpoint,
                    samples=args.requests,
                    warmup=args.warmup,
                    timeout_seconds=args.timeout,
                ),
            }
            for label, endpoint in endpoints.items()
        },
    }
    if args.json:
        print(json.dumps(report, sort_keys=True))
        return 0

    print("Scope: local warmed V1 HTTP transport; excludes browser scheduling and paint")
    for label, host in report["hosts"].items():
        timings = host["timingsMs"]
        response_bytes = host["responseBytes"]
        print(f"\n{label}: {host['endpoint']}")
        print(f"  Samples: {host['completedSamples']} (after {host['configuredWarmup']} warmup)")
        print(f"  Payload: {host['rowCount']} rows, {host['columnCount']} columns; {response_bytes['p50']} / {response_bytes['p95']} bytes p50/p95")
        print(f"  HTTP p50/p95: {timings['httpP50']} / {timings['httpP95']} ms")
        print(f"  Gateway p50/p95: {timings['runtimeGatewayP50']} / {timings['runtimeGatewayP95']} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
