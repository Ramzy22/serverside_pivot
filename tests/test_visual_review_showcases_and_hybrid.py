"""Visual Review Suite 7: Industry Showcase Hub & Hybrid DuckDB-Wasm/Server Execution Switcher.

Covers 3-phase visual verification (`01_before` -> `02_transition` -> `03_after`) for:
- VR-701: Unified Industry Showcase Hub (`examples/demo_showcase_hub.py`):
          Trading & Risk (50k rows, Bloomberg dark, docked charts, sparklines)
          -> People Analytics (35k rows, HR compensation & sparklines)
          -> B2B SaaS Growth (40k rows, ARR/NRR cohorts & docked analytics)
- VR-702: Hybrid Client-Side DuckDB-Wasm & Server-Side Showcase (`examples/demo_clientside_version.py`):
          Auto Client mode (pill badges + underbar data bars + telemetry strip)
          -> Execution Mode dropdown transition -> Force Server-Side Python Ibis parity
"""

from __future__ import annotations

import os
import pytest
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By

from tests.visual_review_support import (
    VIEWPORT_HEIGHT,
    VIEWPORT_WIDTH,
    VisualReviewSession,
    _DashServerThread,
    find_free_port,
)


def _start_dash_app_session(app, suite_name: str):
    if os.environ.get("PIVOT_SKIP_VISUAL_REVIEW") == "1":
        pytest.skip("Skipped via PIVOT_SKIP_VISUAL_REVIEW=1")

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
    session = VisualReviewSession(suite_name, f"http://127.0.0.1:{port}", driver, {})
    return session, server


def test_vr701_industry_showcase_hub_tabs() -> None:
    from examples.demo_showcase_hub import app as hub_app

    session, server = _start_dash_app_session(hub_app, "suite7_showcases_and_hybrid")
    try:
        def switch_to_hr_tab(s: VisualReviewSession) -> None:
            hr_tab = s.wait_until(
                lambda: next(
                    (el for el in s.find_visible('[role="tab"], .tab') if "People Analytics" in (el.text or "")),
                    None,
                ),
                timeout=12.0,
                message="People Analytics tab not found in Showcase Hub",
            )
            hr_tab.click()
            s.wait_until(
                lambda: "TOTAL WORKFORCE" in (s.driver.find_element(By.TAG_NAME, "body").text or ""),
                timeout=15.0,
                message="HR KPI cards did not render",
            )
            s.wait_until(
                lambda: len(s.driver.find_elements(By.CSS_SELECTOR, '[role="gridcell"][data-rowid][data-colid]')) > 0,
                timeout=15.0,
                message="HR pivot cells did not populate",
            )
            s.settle(0.4)

        def switch_to_saas_tab(s: VisualReviewSession) -> None:
            saas_tab = next(
                el for el in s.find_visible('[role="tab"], .tab') if "B2B SaaS" in (el.text or "")
            )
            saas_tab.click()
            s.wait_until(
                lambda: "ANNUAL RUN-RATE" in (s.driver.find_element(By.TAG_NAME, "body").text or ""),
                timeout=15.0,
                message="SaaS KPI cards did not render",
            )
            s.wait_until(
                lambda: len(s.driver.find_elements(By.CSS_SELECTOR, '[role="gridcell"][data-rowid][data-colid]')) > 0,
                timeout=15.0,
                message="SaaS pivot cells did not populate",
            )
            s.settle(0.4)

        session.record_transition(
            "VR_701_showcase_hub_industry_tabs",
            "Showcase Hub Trading & Risk -> switch to People Analytics tab -> switch to B2B SaaS Growth tab",
            route="/",
            route_timeout=45.0,
            action_to_transition_fn=switch_to_hr_tab,
            complete_to_after_fn=switch_to_saas_tab,
            allow_low_contrast=True,
        )
    finally:
        session.write_manifest()
        session.driver.quit()
        server.shutdown()


def test_vr702_hybrid_clientside_wasm_to_server_switcher() -> None:
    from examples.demo_clientside_version import create_app

    hybrid_app = create_app(row_count=400)
    session, server = _start_dash_app_session(hybrid_app, "suite7_showcases_and_hybrid")
    try:
        def open_execution_mode_dropdown(s: VisualReviewSession) -> None:
            dd = s.wait_until(
                lambda: (s.find_visible("#hybrid-execution-mode-select") or [None])[0],
                timeout=10.0,
                message="Hybrid execution mode dropdown not found",
            )
            dd.click()
            s.settle(0.25)

        def select_force_server_mode(s: VisualReviewSession) -> None:
            opts = s.find_visible(".VirtualizedSelectOption, .Select-option, [role='option']")
            srv_opt = next((o for o in opts if "Force Server-Side" in (o.text or "")), None)
            if srv_opt:
                srv_opt.click()
            s.wait_until(
                lambda: "TanStackPivotAdapter" in (s.driver.find_element(By.ID, "hybrid-telemetry-strip").text or "")
                or "server" in (s.driver.find_element(By.ID, "hybrid-telemetry-strip").text or "").lower(),
                timeout=12.0,
                message="Telemetry strip did not reflect server-side execution switch",
            )
            s.wait_until(
                lambda: len(s.driver.find_elements(By.CSS_SELECTOR, '[role="gridcell"][data-rowid][data-colid]')) > 0,
                timeout=12.0,
            )
            s.settle(0.35)

        session.record_transition(
            "VR_702_hybrid_client_to_server_switch",
            "Hybrid Client Mode (pill badges + underbar bars) -> open Execution Mode dropdown -> Force Server-Side",
            route="/",
            action_to_transition_fn=open_execution_mode_dropdown,
            complete_to_after_fn=select_force_server_mode,
        )
    finally:
        session.write_manifest()
        session.driver.quit()
        server.shutdown()
