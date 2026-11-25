"""Visual Review Suite 1: View Modes, Layout Modes, Structural Toggles, Zoom/Immersive & Pagination.

Covers 3-phase visual verification (`01_before` -> `02_transition` -> `03_after`) for:
- VR-101: View toolbar drawer expand -> Layout Mode `Hierarchy` -> `Outline` -> `Tabular`
- VR-102: Totals & Structural Toggles (`Row #`, `Filters` floating bar, `Row Total`, `Col Total`, `Subtotals`)
- VR-103: Row Density Segmented Control (`Compact` -> `Normal` -> `Comfortable` / `Spacious`)
- VR-104: Axis Transpose (`rowFields` <-> `colFields` swap transition & result)
- VR-105: Zoom Controls (`-` / `+`) & Immersive Full-Table Mode enter/exit round-trip
- VR-106: Pivot <-> Report Mode Sliding Pill Switch & Saved Report Selector (`/report`)
- VR-107: Hierarchical Tree View Mode (`/tree`) expand/collapse & hierarchy indentation
- VR-108: Flat Table View Mode (`/table`) & Client-Side `<PaginationBar />` (`/client`) page size + navigation
"""

from __future__ import annotations

import pytest
from selenium.webdriver.common.by import By

from tests.visual_review_support import VisualReviewSession, visual_session_factory  # noqa: F401


@pytest.fixture(scope="module")
def vr_session(visual_session_factory) -> VisualReviewSession:
    return visual_session_factory("suite1_views_and_layouts")


def test_vr101_layout_modes_hierarchy_outline_tabular(vr_session: VisualReviewSession) -> None:
    def open_view_drawer_and_outline(s: VisualReviewSession) -> None:
        s.open_toolbar_section("view")
        outline_btn = next(
            el for el in s.find_visible('[aria-label="Layout mode"] button') if "Outline" in (el.text or "")
        )
        outline_btn.click()

    def switch_to_tabular(s: VisualReviewSession) -> None:
        tabular_btn = next(
            el for el in s.find_visible('[aria-label="Layout mode"] button') if "Tabular" in (el.text or "")
        )
        tabular_btn.click()
        s.wait_until(
            lambda: tabular_btn.get_attribute("aria-pressed") == "true",
            message="Tabular layout button did not become active",
        )

    def check_transition(_before, trans) -> None:
        assert trans.dom_audit["headerCount"] >= 4

    def check_after(before, _trans, after) -> None:
        assert after.dom_audit["headerCount"] >= before.dom_audit["headerCount"]

    vr_session.record_transition(
        "VR_101_layout_modes",
        "View toolbar drawer -> Outline layout transition -> Tabular multi-column layout result",
        route="/pivot",
        action_to_transition_fn=open_view_drawer_and_outline,
        transition_check_fn=check_transition,
        complete_to_after_fn=switch_to_tabular,
        after_check_fn=check_after,
    )


def test_vr102_structural_toggles_row_numbers_floating_filters_and_totals(vr_session: VisualReviewSession) -> None:
    def setup_view(s: VisualReviewSession) -> None:
        s.open_toolbar_section("view")

    def toggle_row_numbers_and_floating_filters(s: VisualReviewSession) -> None:
        s.click_button(text="Row #")
        s.click_button(text="Filters")
        s.wait_until(
            lambda: len(s.driver.find_elements(By.CSS_SELECTOR, '[data-colid="__row_number__"]')) > 0,
            message="Row number cells did not appear",
        )

    def toggle_totals_off_and_on(s: VisualReviewSession) -> None:
        s.click_button(text="Row Total")
        s.click_button(text="Col Total")
        s.settle(0.3)

    def check_transition(_before, trans) -> None:
        row_num_cells = vr_session.driver.find_elements(By.CSS_SELECTOR, '[data-colid="__row_number__"]')
        assert len(row_num_cells) > 0, "Row # column should be visible during transition check"

    vr_session.record_transition(
        "VR_102_structural_toggles",
        "Toggle Row #, Floating Filters bar, and Row/Column Totals",
        route="/pivot",
        setup_fn=setup_view,
        action_to_transition_fn=toggle_row_numbers_and_floating_filters,
        transition_check_fn=check_transition,
        complete_to_after_fn=toggle_totals_off_and_on,
    )


def test_vr103_row_density_cycling(vr_session: VisualReviewSession) -> None:
    measured_heights = {}

    def _sample_row_height(s: VisualReviewSession) -> float:
        return float(
            s.driver.execute_script(
                """
                const cell = document.querySelector('[role="gridcell"][data-rowid][data-colid]');
                return cell ? cell.getBoundingClientRect().height : 0;
                """
            )
        )

    def setup_view(s: VisualReviewSession) -> None:
        s.open_toolbar_section("view")
        measured_heights["initial"] = _sample_row_height(s)

    def switch_first_density(s: VisualReviewSession) -> None:
        buttons = s.find_visible('[aria-label="Row density"] button')
        assert len(buttons) >= 2
        # Click whichever density index is not currently pressed
        target = buttons[0] if buttons[0].get_attribute("aria-pressed") != "true" else buttons[-1]
        target.click()
        s.settle(0.2)
        measured_heights["transition"] = _sample_row_height(s)

    def switch_last_density(s: VisualReviewSession) -> None:
        buttons = s.find_visible('[aria-label="Row density"] button')
        buttons[-1].click()
        s.settle(0.2)
        measured_heights["after"] = _sample_row_height(s)

    def verify_heights(_b, _t, _a) -> None:
        assert measured_heights["after"] > 0 and measured_heights["transition"] > 0
        assert len({round(v, 1) for v in measured_heights.values()}) >= 2, (
            f"Expected row height to change across density modes, got {measured_heights}"
        )

    vr_session.record_transition(
        "VR_103_row_density_cycle",
        "Cycle row density segmented control from compact to spacious and verify row geometry",
        route="/pivot",
        setup_fn=setup_view,
        action_to_transition_fn=switch_first_density,
        complete_to_after_fn=switch_last_density,
        after_check_fn=verify_heights,
    )


def test_vr104_transpose_axes(vr_session: VisualReviewSession) -> None:
    def setup_view(s: VisualReviewSession) -> None:
        s.open_toolbar_section("view")

    def trigger_transpose(s: VisualReviewSession) -> None:
        s.click_button(title_contains="Swap row fields and column fields")
        s.wait_until(
            lambda: any(
                any(tok in h["text"].lower() for tok in ("americas", "apac", "emea", "benelux", "dach", "uki"))
                for h in s.audit_dom()["headers"]
            ),
            timeout=12.0,
            message="Expected transposed region headers to appear",
        )

    def transpose_round_trip_expand(s: VisualReviewSession) -> None:
        expanders = s.find_visible(".sp-expander-btn")
        if expanders:
            expanders[0].click()
            s.settle(0.3)

    vr_session.record_transition(
        "VR_104_transpose_axes",
        "Transpose rows and columns via View toolbar and verify swapped headers & cells",
        route="/pivot",
        setup_fn=setup_view,
        action_to_transition_fn=trigger_transpose,
        complete_to_after_fn=transpose_round_trip_expand,
    )


def test_vr105_zoom_and_immersive_mode_roundtrip(vr_session: VisualReviewSession) -> None:
    def zoom_in_and_enter_immersive(s: VisualReviewSession) -> None:
        s.click_button(title_contains="Zoom in")
        s.click_button(title_contains="Zoom in")
        s.settle(0.15)
        s.click_button(title_contains="Immersive Mode")
        s.wait_until(
            lambda: len(s.find_visible('[data-testid="pivot-toolbar-view"]')) == 0,
            timeout=8.0,
            message="Expected toolbar to hide in Immersive Mode",
        )

    def exit_immersive(s: VisualReviewSession) -> None:
        # Exit via floating exit control or Escape
        exit_btns = s.find_visible('button[title*="Exit"], button[aria-label*="Exit"], button[title*="Immersive"]')
        if exit_btns:
            exit_btns[0].click()
        else:
            from selenium.webdriver.common.action_chains import ActionChains
            from selenium.webdriver.common.keys import Keys
            ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
        s.wait_until(
            lambda: len(s.find_visible('[data-testid="pivot-toolbar-view"]')) > 0,
            timeout=8.0,
            message="Expected toolbar to reappear after exiting Immersive Mode",
        )

    vr_session.record_transition(
        "VR_105_zoom_and_immersive",
        "Zoom to 120%, enter Immersive full-grid mode, then exit back to zoomed workspace",
        route="/pivot",
        action_to_transition_fn=zoom_in_and_enter_immersive,
        complete_to_after_fn=exit_immersive,
    )


def test_vr106_report_mode_and_saved_report_switching(vr_session: VisualReviewSession) -> None:
    def open_saved_report_dropdown(s: VisualReviewSession) -> None:
        trigger = s.wait_until(
            lambda: next(
                (el for el in s.find_visible('[title="Switch saved report"], button[aria-haspopup="listbox"]') if "Executive" in (el.text or "") or "Select report" in (el.text or "")),
                None,
            ),
            timeout=10.0,
            message="Saved report selector trigger not found",
        )
        trigger.click()
        s.wait_until(
            lambda: len(s.find_visible('[data-themed-select-menu="true"], [role="listbox"]')) > 0,
            timeout=6.0,
            message="Saved report dropdown menu did not open",
        )

    def select_second_report_and_toggle_pivot_mode(s: VisualReviewSession) -> None:
        opt = next(
            el
            for el in s.find_visible('[data-themed-select-menu="true"] [role="option"], [role="listbox"] [role="option"]')
            if "Channel Mix" in (el.text or "")
        )
        opt.click()
        s.wait_until(
            lambda: "Direct" in (s.driver.find_element(By.TAG_NAME, "body").text or "")
            or "Partner" in (s.driver.find_element(By.TAG_NAME, "body").text or ""),
            timeout=10.0,
            message="Channel Mix report rows did not load",
        )

    vr_session.record_transition(
        "VR_106_report_mode_saved_switch",
        "Executive P&L Report Mode -> open Saved Report menu -> switch to Channel Mix Scorecard",
        route="/report",
        action_to_transition_fn=open_saved_report_dropdown,
        complete_to_after_fn=select_second_report_and_toggle_pivot_mode,
    )


def test_vr107_tree_view_mode_hierarchy_expansion(vr_session: VisualReviewSession) -> None:
    # 107a: Document default treeConfig (groupDefaultExpanded=1 without explicit valueFields) transition behavior
    def click_level1_child_on_default_tree(s: VisualReviewSession) -> None:
        collapsed = [
            el for el in s.find_visible(".sp-expander-btn") if el.get_attribute("aria-expanded") == "false"
        ]
        assert collapsed, "Expected collapsed level-1 child node on /tree"
        collapsed[0].click()
        s.settle(0.4)

    def expand_root_after_default_reset(s: VisualReviewSession) -> None:
        collapsed = [
            el for el in s.find_visible(".sp-expander-btn") if el.get_attribute("aria-expanded") == "false"
        ]
        if collapsed:
            collapsed[0].click()
            s.settle(0.4)

    vr_session.record_transition(
        "VR_107a_tree_default_config_anomaly",
        "Tree Mode (groupDefaultExpanded=1, omitted valueFields) -> click level-1 expander -> expand root",
        route="/tree",
        action_to_transition_fn=click_level1_child_on_default_tree,
        complete_to_after_fn=expand_root_after_default_reset,
    )

    # 107b: Explicit valueFields + groupDefaultExpanded=0 clean 3-level expansion
    def expand_root_division(s: VisualReviewSession) -> None:
        collapsed = [
            el for el in s.find_visible(".sp-expander-btn") if el.get_attribute("aria-expanded") == "false"
        ]
        assert collapsed
        collapsed[0].click()
        s.wait_until(
            lambda: sum(1 for el in s.find_visible(".sp-expander-btn") if el.get_attribute("aria-expanded") == "true") >= 1,
            timeout=10.0,
            message="Root division did not expand on /tree-configured",
        )
        s.settle(0.3)

    def expand_vp_into_director_team(s: VisualReviewSession) -> None:
        collapsed = [
            el for el in s.find_visible(".sp-expander-btn") if el.get_attribute("aria-expanded") == "false"
        ]
        assert collapsed
        collapsed[0].click()
        s.settle(0.4)

    vr_session.record_transition(
        "VR_107b_tree_configured_3level_expand",
        "Tree Mode (explicit valueFields & extraFields) -> expand Executive Group -> expand VP Division to Director Team",
        route="/tree-configured",
        action_to_transition_fn=expand_root_division,
        complete_to_after_fn=expand_vp_into_director_team,
    )


def test_vr108_table_mode_and_clientside_pagination(vr_session: VisualReviewSession) -> None:
    # First verify flat table route renders cleanly
    vr_session.open_route("/table")
    flat_ev = vr_session.record_transition(
        "VR_108a_flat_table_sort",
        "Flat Table View Mode -> click column header sort ascending -> sort descending",
        route="/table",
        action_to_transition_fn=lambda s: s.find_visible('[role="columnheader"]')[0].click(),
        complete_to_after_fn=lambda s: s.find_visible('[role="columnheader"]')[0].click(),
    )
    assert flat_ev.phases["03_after"].dom_audit["cellCount"] > 0

    # Now test Client-Side Mode with PaginationBar transitions
    def open_rows_per_page_menu(s: VisualReviewSession) -> None:
        trigger = s.wait_until(
            lambda: (s.find_visible('[aria-label="Rows per page"]') or [None])[0],
            timeout=8.0,
            message="PaginationBar 'Rows per page' control not found",
        )
        trigger.click()
        s.settle(0.15)

    def navigate_next_page(s: VisualReviewSession) -> None:
        # Close menu by choosing 10 or pressing Escape, then click Next ›
        opts = s.find_visible('[data-themed-select-menu="true"] [role="option"]')
        if opts:
            opts[0].click()
            s.settle(0.15)
        next_btn = next(
            el for el in s.find_visible("button") if (el.text or "").strip() == "›"
        )
        next_btn.click()
        s.wait_until(
            lambda: "2 /" in (s.driver.find_element(By.TAG_NAME, "body").text or ""),
            timeout=6.0,
            message="PaginationBar did not advance to page 2",
        )

    vr_session.record_transition(
        "VR_108b_clientside_pagination",
        "Client-Side Mode PaginationBar -> open Rows-per-page dropdown -> advance to Page 2",
        route="/client",
        action_to_transition_fn=open_rows_per_page_menu,
        complete_to_after_fn=navigate_next_page,
    )
