#!/usr/bin/env python3
"""Measure real browser-to-Dash viewport latency at a repeatable data scale.

Unlike ``benchmark_pivot_runtime.py``, this launches the actual Dash
component in headless Chrome.  Each sample is a server-side viewport request
caused by a real scroll event and includes browser dispatch, the HTTP/Dash
callback, server query work, response delivery, React commit, and paint-ready
layout.  Results are observational because browser and machine speed vary.

Examples:

    python scripts/benchmark_browser_runtime.py
    python scripts/benchmark_browser_runtime.py --rows 500000 --requests 40
    python scripts/benchmark_browser_runtime.py --max-p95-ms 500 --json

Chrome plus a compatible ChromeDriver are required.  This is intentionally a
developer/release benchmark rather than a fixed CI time gate.
"""

from __future__ import annotations

import argparse
import json
import math
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = REPOSITORY_ROOT / "dash_tanstack_pivot"
ENGINE_ROOT = PACKAGE_ROOT / "pivot_engine"
for import_root in (REPOSITORY_ROOT, PACKAGE_ROOT, ENGINE_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import pyarrow as pa
import requests
from dash import Dash, html
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from werkzeug.serving import WSGIRequestHandler, make_server

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter, register_pivot_app


class QuietRequestHandler(WSGIRequestHandler):
    """Keep benchmark output to the measurements rather than HTTP access logs."""

    def log(self, _type: str, _message: str, *args: Any) -> None:
        return


class DashServerThread(threading.Thread):
    """Own a local Dash server for the duration of a benchmark run."""

    def __init__(self, app: Dash, host: str, port: int) -> None:
        super().__init__(daemon=True)
        self._server = make_server(
            host,
            port,
            app.server,
            threaded=True,
            request_handler=QuietRequestHandler,
        )

    def run(self) -> None:
        self._server.serve_forever()

    def shutdown(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def percentile(values: Iterable[float], percentile_value: float) -> float | None:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return None
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * percentile_value) - 1))
    return round(ordered[index], 3)


def build_sales_table(row_count: int) -> pa.Table:
    """Build a deterministic 100k+-row analytical table without I/O."""
    if row_count < 10_000:
        raise ValueError("rows must be at least 10,000 to exercise viewport paging")
    return pa.table(
        {
            # 5,000 top-level members provide enough independently fetched
            # blocks for a 20+ sample p95 even at the minimum row count.
            "region": [f"Region {index % 5_000:04d}" for index in range(row_count)],
            "desk": [f"Desk {(index // 5_000) % 20:02d}" for index in range(row_count)],
            "quarter": [f"Q{index % 4 + 1}" for index in range(row_count)],
            "sales": [float((index * 37) % 10_000) / 10 for index in range(row_count)],
            "units": [index % 19 + 1 for index in range(row_count)],
        }
    )


def build_app(row_count: int) -> Dash:
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("browser_performance", build_sales_table(row_count))

    app = Dash(__name__)
    app.layout = html.Div(
        style={"padding": "12px"},
        children=[
            DashTanstackPivot(
                id="pivot-grid",
                style={"height": "760px", "width": "1420px"},
                table="browser_performance",
                serverSide=True,
                rowFields=["region", "desk"],
                colFields=["quarter"],
                valConfigs=[
                    {"field": "sales", "agg": "sum", "alias": "sales_sum", "label": "Sales"},
                    {"field": "units", "agg": "sum", "alias": "units_sum", "label": "Units"},
                ],
                filters={},
                sorting=[],
                expanded={},
                showRowTotals=False,
                showColTotals=False,
                availableFieldList=["region", "desk", "quarter", "sales", "units"],
                performanceConfig={
                    "cacheBlockSize": 48,
                    "maxBlocksInCache": 500,
                    "rowOverscan": 8,
                },
                data=[],
            )
        ],
    )
    register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id="pivot-grid", debug=False)
    return app


def wait_for_server(base_url: str, server: DashServerThread) -> None:
    deadline = time.monotonic() + 20
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(base_url, timeout=1.5)
            if response.ok:
                return
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(0.2)
    server.shutdown()
    raise RuntimeError(f"Dash performance server did not start: {last_error}")


def create_driver() -> webdriver.Chrome:
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1680,1200")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--force-device-scale-factor=1")
    try:
        driver = webdriver.Chrome(options=options)
    except WebDriverException as exc:
        raise RuntimeError(f"Chrome WebDriver is not available: {exc}") from exc
    # The profiler reads this during component initialization, so it must be
    # installed before navigating to Dash rather than after the first render.
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "window.__PIVOT_PROFILE__ = true;"},
    )
    driver.set_page_load_timeout(30)
    return driver


def profiler_history(driver: webdriver.Chrome) -> list[dict[str, Any]]:
    history = driver.execute_script(
        """
        const profiler = window.__pivotProfiler;
        return profiler && typeof profiler.getHistory === 'function'
            ? profiler.getHistory('pivot-grid')
            : [];
        """
    )
    return history if isinstance(history, list) else []


def scroll_to_fraction(driver: webdriver.Chrome, fraction: float) -> int:
    return int(
        driver.execute_script(
            """
            const container = document.querySelector('[data-pivot-scroll-container="true"]');
            if (!container) return -1;
            const maximum = Math.max(0, container.scrollHeight - container.clientHeight);
            container.scrollTop = Math.round(maximum * arguments[0]);
            container.dispatchEvent(new Event('scroll', { bubbles: true }));
            return container.scrollTop;
            """,
            fraction,
        )
    )


def prepare_scroll_samples(driver: webdriver.Chrome) -> list[dict[str, Any]]:
    """Wait for hydration and reset the profiler before interactive samples."""
    wait = WebDriverWait(driver, 15)
    wait.until(expected_conditions.presence_of_element_located((By.CSS_SELECTOR, '[data-pivot-scroll-container="true"]')))
    # Ensure initial hydration completed, then remove it so this report only
    # contains user-like viewport changes.
    wait.until(lambda current_driver: len(profiler_history(current_driver)) >= 1)
    # A structural request may issue a second data response after the first
    # visible viewport. Wait for the profiler to be quiet before clearing it;
    # otherwise that response can be misclassified as the first scroll.
    quiet_since = time.monotonic()
    observed_count = len(profiler_history(driver))
    quiet_deadline = quiet_since + 2
    while time.monotonic() < quiet_deadline:
        time.sleep(0.05)
        current_count = len(profiler_history(driver))
        if current_count != observed_count:
            observed_count = current_count
            quiet_since = time.monotonic()
        elif time.monotonic() - quiet_since >= 0.2:
            break
    initial_history = profiler_history(driver)
    driver.execute_script("window.__pivotProfiler.clear('pivot-grid');")
    return initial_history


def collect_scroll_samples(
    driver: webdriver.Chrome, request_count: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    initial_history = prepare_scroll_samples(driver)

    for index in range(request_count):
        before = len(profiler_history(driver))
        # Keep every scroll outside the prior block and away from the exact
        # endpoint, both of which can be cache hits/no-op scroll events.
        fraction = (index + 1) / (request_count + 2)
        scroll_to_fraction(driver, fraction)
        try:
            WebDriverWait(driver, 8).until(
                lambda current_driver: len(profiler_history(current_driver)) > before
            )
        except TimeoutException as exc:
            raise RuntimeError(
                f"viewport scroll {index + 1}/{request_count} did not produce a completed runtime sample"
            ) from exc

    return initial_history, profiler_history(driver)


def collect_concurrent_scroll_samples(
    drivers: list[webdriver.Chrome], request_count: int
) -> tuple[list[list[dict[str, Any]]], list[list[dict[str, Any]]]]:
    """Generate independently positioned scrolls from multiple real browsers.

    Drivers hydrate one at a time to make first-viewport timing observable.
    Every measured scroll then starts behind a barrier, so the server receives
    overlapping Dash callbacks rather than a sequence of cached requests.
    """
    initial_histories = [prepare_scroll_samples(driver) for driver in drivers]
    client_count = len(drivers)
    if client_count < 2:
        return initial_histories, [profiler_history(driver) for driver in drivers]

    with ThreadPoolExecutor(max_workers=client_count) as executor:
        for request_index in range(request_count):
            start_barrier = threading.Barrier(client_count, timeout=15)

            def scroll_one(driver: webdriver.Chrome, client_index: int) -> None:
                before = len(profiler_history(driver))
                # Offset each client within a logical scroll step. This avoids
                # measuring a single identical response-window cache key while
                # keeping all requests inside the interactive viewport range.
                fraction = (request_index + 1 + (client_index / client_count)) / (request_count + 2)
                start_barrier.wait()
                scroll_to_fraction(driver, fraction)
                WebDriverWait(driver, 8).until(
                    lambda current_driver: len(profiler_history(current_driver)) > before
                )

            futures = [
                executor.submit(scroll_one, driver, client_index)
                for client_index, driver in enumerate(drivers)
            ]
            for future in futures:
                try:
                    future.result()
                except TimeoutException as exc:
                    raise RuntimeError(
                        f"concurrent viewport scroll {request_index + 1}/{request_count} did not complete"
                    ) from exc

    return initial_histories, [profiler_history(driver) for driver in drivers]


def initial_viewport_timing(history: list[dict[str, Any]]) -> dict[str, float | None]:
    """Report the completed hydration sample separately from scroll p95s.

    Viewport scrolling is intentionally measured after hydration so it reflects
    an interactive user session. Keeping the initial request in the report
    prevents a warm-scroll improvement from hiding a first-render regression.
    """
    completed = [entry for entry in history if isinstance(entry.get("derived"), dict)]
    entry = completed[-1] if completed else {}
    derived = entry.get("derived") if isinstance(entry, dict) else {}
    profile = entry.get("profile") if isinstance(entry, dict) and isinstance(entry.get("profile"), dict) else {}
    callback = profile.get("callback") if isinstance(profile.get("callback"), dict) else {}
    service = profile.get("service") if isinstance(profile.get("service"), dict) else {}

    def finite_or_none(value: Any) -> float | None:
        return round(float(value), 3) if isinstance(value, (int, float)) and math.isfinite(value) else None

    return {
        "endToEnd": finite_or_none(derived.get("totalMs")),
        "networkAndServer": finite_or_none(derived.get("responseMs")),
        "dashCallback": finite_or_none(callback.get("totalMs")),
        "pivotService": finite_or_none(service.get("totalMs")),
        "reactCommit": finite_or_none(derived.get("renderMs")),
    }


def summarize(history: list[dict[str, Any]], input_rows: int, requested_samples: int) -> dict[str, Any]:
    completed = [entry for entry in history if isinstance(entry.get("derived"), dict)]

    def values(name: str) -> list[float]:
        result: list[float] = []
        for entry in completed:
            value = entry["derived"].get(name)
            if isinstance(value, (int, float)) and math.isfinite(value):
                result.append(float(value))
        return result

    def profile_values(group: str) -> list[float]:
        result: list[float] = []
        for entry in completed:
            profile = entry.get("profile") if isinstance(entry.get("profile"), dict) else {}
            section = profile.get(group) if isinstance(profile.get(group), dict) else {}
            value = section.get("totalMs")
            if isinstance(value, (int, float)) and math.isfinite(value):
                result.append(float(value))
        return result

    def nested_profile_values(*path: str) -> list[float]:
        """Return one numeric profiling stage from completed runtime samples."""
        result: list[float] = []
        for entry in completed:
            current: Any = entry.get("profile")
            for segment in path:
                current = current.get(segment) if isinstance(current, dict) else None
            if isinstance(current, (int, float)) and math.isfinite(current):
                result.append(float(current))
        return result

    adapter_paths: dict[str, int] = {}
    for entry in completed:
        profile = entry.get("profile") if isinstance(entry.get("profile"), dict) else {}
        adapter = profile.get("adapter") if isinstance(profile.get("adapter"), dict) else {}
        path = adapter.get("path")
        if isinstance(path, str) and path:
            adapter_paths[path] = adapter_paths.get(path, 0) + 1

    total = values("totalMs")
    response = values("responseMs")
    render = values("renderMs")
    callback = profile_values("callback")
    service = profile_values("service")
    return {
        "inputRows": input_rows,
        "requestedSamples": requested_samples,
        "completedSamples": len(completed),
        "timingsMs": {
            "endToEndP50": percentile(total, 0.5),
            "endToEndP95": percentile(total, 0.95),
            "networkAndServerP50": percentile(response, 0.5),
            "networkAndServerP95": percentile(response, 0.95),
            "dashCallbackP50": percentile(callback, 0.5),
            "dashCallbackP95": percentile(callback, 0.95),
            "pivotServiceP50": percentile(service, 0.5),
            "pivotServiceP95": percentile(service, 0.95),
            "reactCommitP50": percentile(render, 0.5),
            "reactCommitP95": percentile(render, 0.95),
        },
        # These values make the benchmark actionable: they distinguish a
        # browser regression from a Dash transport, adapter, or query-stage
        # regression without collecting request data or cache keys.
        "runtimePaths": {
            "adapter": adapter_paths,
        },
        "serverStagesMs": {
            "adapterHierarchyViewP50": percentile(
                nested_profile_values("adapter", "hierarchyViewMs"), 0.5
            ),
            "adapterHierarchyViewP95": percentile(
                nested_profile_values("adapter", "hierarchyViewMs"), 0.95
            ),
            "controllerPageKeyFetchP50": percentile(
                nested_profile_values("controller", "pageKeyFetchMs"), 0.5
            ),
            "controllerPageKeyFetchP95": percentile(
                nested_profile_values("controller", "pageKeyFetchMs"), 0.95
            ),
            "controllerRootQueryP50": percentile(
                nested_profile_values("controller", "rootQueryMs"), 0.5
            ),
            "controllerRootQueryP95": percentile(
                nested_profile_values("controller", "rootQueryMs"), 0.95
            ),
            "controllerRootCountP50": percentile(
                nested_profile_values("controller", "rootCountMs"), 0.5
            ),
            "controllerRootCountP95": percentile(
                nested_profile_values("controller", "rootCountMs"), 0.95
            ),
        },
    }


def print_human(report: dict[str, Any]) -> None:
    timings = report["timingsMs"]
    stages = report["serverStagesMs"]
    print(f"Input rows:              {report['inputRows']:,}")
    concurrency = report.get("concurrency")
    if isinstance(concurrency, dict):
        print(
            "Concurrent clients:       "
            f"{concurrency.get('clients', 1)} × {concurrency.get('requestsPerClient', report['requestedSamples'])} scrolls"
        )
    print(f"Initial viewport:        {report['initialViewportMs']}")
    print(f"Completed scroll samples: {report['completedSamples']}/{report['requestedSamples']}")
    print(f"End-to-end p50/p95:      {timings['endToEndP50']} / {timings['endToEndP95']} ms")
    print(f"Network + server p50/p95:{timings['networkAndServerP50']} / {timings['networkAndServerP95']} ms")
    print(f"Dash callback p50/p95:   {timings['dashCallbackP50']} / {timings['dashCallbackP95']} ms")
    print(f"Pivot service p50/p95:   {timings['pivotServiceP50']} / {timings['pivotServiceP95']} ms")
    print(f"React commit p50/p95:    {timings['reactCommitP50']} / {timings['reactCommitP95']} ms")
    print(f"Adapter paths:           {report['runtimePaths']['adapter']}")
    print(f"Root keys p50/p95:       {stages['controllerPageKeyFetchP50']} / {stages['controllerPageKeyFetchP95']} ms")
    print(f"Root query p50/p95:      {stages['controllerRootQueryP50']} / {stages['controllerRootQueryP95']} ms")
    print(f"Root count p50/p95:      {stages['controllerRootCountP50']} / {stages['controllerRootCountP95']} ms")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark real browser/Dash viewport p50 and p95 latency.")
    parser.add_argument("--rows", type=int, default=100_000, help="Synthetic Arrow rows to load (default: 100,000).")
    parser.add_argument("--requests", type=int, default=30, help="Completed scroll requests to measure (default: 30).")
    parser.add_argument(
        "--clients",
        type=int,
        default=1,
        help="Independent headless browsers issuing overlapping scrolls (default: 1; max: 4).",
    )
    parser.add_argument("--max-p95-ms", type=float, default=None, help="Optional fail budget for end-to-end p95.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.requests < 20:
        raise ValueError("requests must be at least 20 for a meaningful p95")
    if not 1 <= args.clients <= 4:
        raise ValueError("clients must be between 1 and 4 for this local benchmark")

    app = build_app(args.rows)
    host = "127.0.0.1"
    port = find_free_port()
    server = DashServerThread(app, host, port)
    server.start()
    base_url = f"http://{host}:{port}"
    drivers: list[webdriver.Chrome] = []
    try:
        wait_for_server(base_url, server)
        for _client_index in range(args.clients):
            driver = create_driver()
            drivers.append(driver)
            driver.get(base_url)
        if args.clients == 1:
            initial_history, scroll_history = collect_scroll_samples(drivers[0], args.requests)
            initial_histories = [initial_history]
            scroll_histories = [scroll_history]
        else:
            initial_histories, scroll_histories = collect_concurrent_scroll_samples(
                drivers, args.requests
            )
        report = summarize(
            [entry for history in scroll_histories for entry in history],
            args.rows,
            args.requests * args.clients,
        )
        report["concurrency"] = {
            "clients": args.clients,
            "requestsPerClient": args.requests,
        }
        report["initialViewportMs"] = initial_viewport_timing(initial_histories[0])
    finally:
        for driver in drivers:
            driver.quit()
        server.shutdown()

    if args.json:
        print(json.dumps(report, sort_keys=True))
    else:
        print_human(report)
    p95 = report["timingsMs"]["endToEndP95"]
    if args.max_p95_ms is not None and (p95 is None or p95 > args.max_p95_ms):
        raise SystemExit(f"end-to-end p95 {p95} ms exceeds budget {args.max_p95_ms} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
