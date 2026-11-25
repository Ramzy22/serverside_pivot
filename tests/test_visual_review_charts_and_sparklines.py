"""Visual Review Suite 5: Visual Analytics, Chart Workspace Docking/Floating, Chart Families & Sparklines.

Covers 3-phase visual verification (`01_before` -> `02_transition` -> `03_after`) for:
- VR-501: `New Chart Pane` Preview Modal (`isPreview`) -> `Open in Panel` Docked Chart -> Splitter Resize
- VR-502: Docked Chart Pane -> Transition to Floating / Bottom Dock Layout -> Close Cleanly
- VR-503: Cell Selection -> `Range Chart` Activation & Launch -> `By Columns` Orientation
- VR-504: Systematic Multi-Family Chart Gallery Sweep (`bar`, `line`, `area`, `donut`, `scatter`,
          `waterfall`, `heatmap`, `radar`, `sunburst`, `sankey`, `combo`) + Chart Export Menu
- VR-505: Inline Sparklines (`area` & `bar` cells on `/sparklines`) -> Double-Click `SparklineDetailGraph` Modal
"""

from __future__ import annotations

import pytest
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from tests.visual_review_support import VisualReviewSession, visual_session_factory  # noqa: F401


@pytest.fixture(scope="module")
def vr_session(visual_session_factory) -> VisualReviewSession:
    return visual_session_factory("suite5_charts_and_sparklines")


def _open_and_confirm_docked_chart(s: VisualReviewSession) -> None:
    s.open_toolbar_section("charts")
    s.click_button(title_contains="Add a resizable chart pane")
    s.wait_until(
        lambda: any((b.text or "").strip() == "Open in Panel" for b in s.find_visible("button")),
        timeout=10.0,
        message="New Chart Pane preview modal did not open",
    )
    s.settle(0.25)
    s.click_button(text="Open in Panel")
    s.wait_until(
        lambda: len(s.find_visible("[data-docked-chart-pane]")) > 0,
        timeout=10.0,
        message="Docked chart pane did not mount after clicking Open in Panel",
    )
    s.settle(0.3)


def test_vr501_docked_chart_pane_preview_open_and_resize(vr_session: VisualReviewSession) -> None:
    def launch_chart_preview_modal(s: VisualReviewSession) -> None:
        s.open_toolbar_section("charts")
        s.click_button(title_contains="Add a resizable chart pane")
        s.wait_until(
            lambda: any((b.text or "").strip() == "Open in Panel" for b in s.find_visible("button")),
            timeout=10.0,
            message="New Chart Pane preview modal did not open",
        )
        s.settle(0.3)

    def confirm_into_docked_panel_and_resize(s: VisualReviewSession) -> None:
        s.click_button(text="Open in Panel")
        s.wait_until(
            lambda: len(s.find_visible("[data-docked-chart-pane]")) > 0,
            timeout=10.0,
            message="Docked chart pane did not appear after Open in Panel",
        )
        s.settle(0.3)
        handles = s.find_visible('[title="Resize workspace panes"]')
        if handles:
            ActionChains(s.driver).click_and_hold(handles[0]).move_by_offset(-80, 0).release().perform()
            s.settle(0.25)
        stacked_btns = [b for b in s.find_visible("button") if (b.text or "").strip() == "Stacked"]
        if stacked_btns:
            stacked_btns[0].click()
            s.settle(0.25)

    def verify_chart_rendered(_b, trans, after) -> None:
        assert len(trans.dom_audit["charts"]) >= 1 or len(after.dom_audit["charts"]) >= 1
        assert len(after.dom_audit["charts"]) >= 1

    vr_session.record_transition(
        "VR_501_docked_chart_open_and_resize",
        "Click New Chart Pane -> full Preview Modal -> click 'Open in Panel' to dock & resize splitter",
        route="/pivot",
        action_to_transition_fn=launch_chart_preview_modal,
        complete_to_after_fn=confirm_into_docked_panel_and_resize,
        after_check_fn=verify_chart_rendered,
    )


def test_vr502_chart_dock_positions_and_floating_window(vr_session: VisualReviewSession) -> None:
    def setup_docked(s: VisualReviewSession) -> None:
        _open_and_confirm_docked_chart(s)

    def float_or_redock_chart_pane(s: VisualReviewSession) -> None:
        pane = s.find_visible("[data-docked-chart-pane]")[0]
        float_btns = [
            b
            for b in pane.find_elements(By.TAG_NAME, "button")
            if b.is_displayed()
            and any(
                kw in ((b.get_attribute("title") or "") + " " + (b.get_attribute("aria-label") or "") + " " + (b.text or ""))
                for kw in ("Float", "Undock", "Floating", "Bottom", "Horizontal")
            )
        ]
        if float_btns:
            float_btns[0].click()
            s.settle(0.3)
        else:
            horiz_btns = [b for b in s.find_visible("button") if (b.text or "").strip() == "Horizontal"]
            if horiz_btns:
                horiz_btns[0].click()
                s.settle(0.3)

    def close_chart_pane_cleanly(s: VisualReviewSession) -> None:
        close_btns = [
            b
            for b in s.find_visible("button")
            if any(
                kw in ((b.get_attribute("title") or "") + " " + (b.get_attribute("aria-label") or ""))
                for kw in ("Close", "Remove")
            )
        ]
        if close_btns:
            close_btns[-1].click()
            s.settle(0.25)

    vr_session.record_transition(
        "VR_502_chart_float_or_redock_and_close",
        "Docked chart pane -> transition to Floating / Horizontal layout -> close pane",
        route="/pivot",
        setup_fn=setup_docked,
        action_to_transition_fn=float_or_redock_chart_pane,
        complete_to_after_fn=close_chart_pane_cleanly,
        expect_visual_change_in_after=False,
    )


def test_vr503_selection_range_chart(vr_session: VisualReviewSession) -> None:
    def select_range_and_launch_range_chart(s: VisualReviewSession) -> None:
        expanders = [e for e in s.find_visible(".sp-expander-btn") if e.get_attribute("aria-expanded") == "false"]
        if expanders:
            expanders[0].click()
            s.settle(0.3)

        cells = [
            el
            for el in s.find_visible('[role="gridcell"][data-rowid][data-colid]')
            if (el.get_attribute("data-colid") or "") not in ("hierarchy", "__row_number__")
            and (el.get_attribute("data-rowid") or "") != "__grand_total__"
        ]
        assert len(cells) >= 4
        ActionChains(s.driver).click_and_hold(cells[0]).move_to_element(cells[min(5, len(cells) - 1)]).release().perform()
        s.settle(0.2)
        s.open_toolbar_section("charts")
        s.click_button(title_contains="Create a range chart")
        s.wait_until(
            lambda: len(s.audit_dom()["charts"]) > 0 or len(s.find_visible('[role="dialog"]')) > 0,
            timeout=10.0,
            message="Range chart did not render",
        )

    def switch_orientation_by_columns(s: VisualReviewSession) -> None:
        by_col_btns = [b for b in s.find_visible("button") if (b.text or "").strip() == "By Columns"]
        if by_col_btns:
            by_col_btns[0].click()
            s.settle(0.3)

    vr_session.record_transition(
        "VR_503_selection_range_chart",
        "Select numeric cell range -> launch Range Chart -> toggle 'By Columns' orientation",
        route="/pivot",
        action_to_transition_fn=select_range_and_launch_range_chart,
        complete_to_after_fn=switch_orientation_by_columns,
    )


def test_vr504_chart_families_gallery_and_export_menu(vr_session: VisualReviewSession) -> None:
    target_chart_labels = [
        "Line",
        "Area",
        "Donut",
        "Scatter",
        "Waterfall",
        "Heatmap",
        "Radar",
        "Sunburst",
        "Sankey",
        "Combo",
    ]

    def open_docked_chart_and_switch_line(s: VisualReviewSession) -> None:
        _open_and_confirm_docked_chart(s)
        pane = s.find_visible("[data-docked-chart-pane]")[0]
        for b in pane.find_elements(By.CSS_SELECTOR, "button, [role='button']"):
            if b.is_displayed() and "line" in ((b.text or "") + " " + (b.get_attribute("title") or "")).lower():
                b.click()
                break
        s.settle(0.3)

    def sweep_chart_families_and_open_export(s: VisualReviewSession) -> None:
        pane = s.find_visible("[data-docked-chart-pane]")[0]
        for label in target_chart_labels:
            switched = False
            for b in pane.find_elements(By.CSS_SELECTOR, "button, [role='button']"):
                if not b.is_displayed():
                    continue
                meta = f"{(b.text or '').strip()} {b.get_attribute('title') or ''} {b.get_attribute('aria-label') or ''}"
                if label.lower() in meta.lower():
                    b.click()
                    switched = True
                    break
            if not switched:
                selects = [
                    el for el in pane.find_elements(By.CSS_SELECTOR, '[aria-haspopup="listbox"], select') if el.is_displayed()
                ]
                for trig in selects[:2]:
                    try:
                        s.select_themed_option(trig, label)
                        switched = True
                        break
                    except Exception:
                        ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
            if switched:
                s.settle(0.28)
                snap = s.artifact_dir / f"VR_504_chart_family_{label.lower()}.png"
                s.driver.save_screenshot(str(snap))
                audit = s.audit_dom()
                assert not audit["errorBoundaryVisible"], f"Error boundary in chart type {label}"

        export_btns = [
            b
            for b in pane.find_elements(By.TAG_NAME, "button")
            if b.is_displayed()
            and any(
                k in ((b.text or "") + " " + (b.get_attribute("title") or "") + " " + (b.get_attribute("aria-label") or ""))
                for k in ("Export", "Download")
            )
        ]
        if export_btns:
            export_btns[0].click()
            s.settle(0.2)

    vr_session.record_transition(
        "VR_504_chart_families_sweep",
        "Dock Chart Pane & switch to Line -> systematically render & screenshot 10 chart families + Export menu",
        route="/pivot",
        action_to_transition_fn=open_docked_chart_and_switch_line,
        complete_to_after_fn=sweep_chart_families_and_open_export,
    )


def test_vr505_inline_sparklines_and_detail_modal(vr_session: VisualReviewSession) -> None:
    def double_click_sparkline_cell(s: VisualReviewSession) -> None:
        opened = s.driver.execute_script(
            """
            const targets = [...document.querySelectorAll('[title="Double-click to view data"]')];
            // Pick a non-Grand-Total row if available
            const target = targets.find(el => {
                const cell = el.closest('[role="gridcell"]');
                return cell && cell.dataset.rowid !== '__grand_total__';
            }) || targets[0];
            if (!target) return false;
            target.scrollIntoView({ block: 'center', inline: 'center' });
            target.dispatchEvent(new MouseEvent('dblclick', { bubbles: true, cancelable: true, view: window }));
            return true;
            """
        )
        assert opened, "Could not locate inline sparkline target element"
        s.wait_until(
            lambda: len(s.find_visible('[data-pivot-sparkline-detail-table="true"], [aria-label="Close trend data dialog"]')) > 0,
            timeout=8.0,
            message="SparklineDetailGraph modal did not open on dblclick",
        )

    def close_sparkline_detail_modal(s: VisualReviewSession) -> None:
        s.click_button(aria_label="Close trend data dialog")
        s.wait_until(
            lambda: len(s.find_visible('[data-pivot-sparkline-detail-table="true"]')) == 0,
            timeout=6.0,
            message="SparklineDetailGraph modal did not close",
        )

    vr_session.record_transition(
        "VR_505_inline_sparklines_and_detail_modal",
        "Render inline Area & Bar sparkline cells -> double-click to open SparklineDetailGraph modal -> close",
        route="/sparklines",
        action_to_transition_fn=double_click_sparkline_cell,
        complete_to_after_fn=close_sparkline_detail_modal,
        expect_visual_change_in_after=False,
    )
