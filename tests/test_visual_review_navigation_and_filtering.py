"""Visual Review Suite 2: Hierarchy Motion, 2D Virtual Scroll, Pinning, Resizing, Sorting & Filtering.

Covers 3-phase visual verification (`01_before` -> `02_transition` -> `03_after`) for:
- VR-201: Single Hierarchy Row Expand -> Child Indentation -> Collapse Motion
- VR-202: Context Menu `Expand All Children` & Row Pinning (`Pin Row Top` / `Unpin Row`)
- VR-203: 2D Virtual Scrolling (Vertical + Horizontal scroll on `/wide` matrix, sticky header & zero gaps)
- VR-204: Column Pinning Left & Right across horizontal scroll (zero duplicate logical cells)
- VR-205: Column Drag Resize (active resize pill highlight) & Toolbar `Auto Size`
- VR-206: Multi-Column Sorting (`Asc` -> `Desc` + Shift-Click secondary sort indicator)
- VR-207: Global Search Focus Ring, Debounced Filter Transition & Clear Restoration
- VR-208: Column Header `FilterPopover` Open (viewport-clamped glass dialog) -> Apply Filter -> Badge
"""

from __future__ import annotations

import time

import pytest
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from tests.visual_review_support import VisualReviewSession, visual_session_factory  # noqa: F401


@pytest.fixture(scope="module")
def vr_session(visual_session_factory) -> VisualReviewSession:
    return visual_session_factory("suite2_navigation_and_filtering")


def test_vr201_hierarchy_row_expand_and_collapse(vr_session: VisualReviewSession) -> None:
    def expand_first_region(s: VisualReviewSession) -> None:
        btn = s.wait_until(
            lambda: next(
                (el for el in s.find_visible(".sp-expander-btn") if el.get_attribute("aria-expanded") == "false"),
                None,
            ),
            message="No collapsed expander button found",
        )
        btn.click()
        s.wait_until(
            lambda: any(el.get_attribute("aria-expanded") == "true" for el in s.find_visible(".sp-expander-btn")),
            timeout=10.0,
            message="Row did not transition to aria-expanded=true",
        )

    def expand_second_level_then_collapse(s: VisualReviewSession) -> None:
        # Expand a child segment to verify 2nd-level depth indentation visually
        collapsed = [
            el for el in s.find_visible(".sp-expander-btn") if el.get_attribute("aria-expanded") == "false"
        ]
        assert collapsed
        collapsed[0].click()
        s.settle(0.35)

    def check_expanded(_b, trans, after) -> None:
        assert trans.dom_audit["cellCount"] > 0
        assert after.dom_audit["cellCount"] >= trans.dom_audit["cellCount"]

    vr_session.record_transition(
        "VR_201_hierarchy_expand_depth",
        "Expand root Region row -> reveal indented Segment rows -> expand 2nd-level Channel rows",
        route="/pivot",
        action_to_transition_fn=expand_first_region,
        complete_to_after_fn=expand_second_level_then_collapse,
        after_check_fn=check_expanded,
    )


def test_vr202_row_pinning_via_context_menu(vr_session: VisualReviewSession) -> None:
    def open_context_menu_on_row(s: VisualReviewSession) -> None:
        cells = [
            el
            for el in s.find_visible('[role="gridcell"][data-rowid][data-colid]')
            if (el.get_attribute("data-rowid") or "") != "__grand_total__"
        ]
        assert cells, "Expected non-total grid cell"
        ActionChains(s.driver).context_click(cells[2]).perform()
        s.wait_until(
            lambda: len(s.find_visible('[role="menu"]')) > 0,
            timeout=6.0,
            message="Context menu did not appear on right-click",
        )

    def pin_row_top(s: VisualReviewSession) -> None:
        s.click_button(text="Pin Row Top")
        s.settle(0.3)

    def check_menu_overlay(_b, trans) -> None:
        menus = [o for o in trans.dom_audit["overlays"] if o["role"] == "menu"]
        assert len(menus) >= 1 and menus[0]["withinViewport"]

    vr_session.record_transition(
        "VR_202_context_menu_pin_row_top",
        "Right-click grid cell -> open glassmorphic ContextMenu -> Pin Row Top sticky container",
        route="/pivot",
        action_to_transition_fn=open_context_menu_on_row,
        transition_check_fn=check_menu_overlay,
        complete_to_after_fn=pin_row_top,
    )


def test_vr203_2d_virtual_scrolling_wide_matrix(vr_session: VisualReviewSession) -> None:
    scroll_positions = {}

    def scroll_diagonally_midway(s: VisualReviewSession) -> None:
        pos = s.driver.execute_script(
            """
            const sc = document.querySelector('[data-pivot-scroll-container]');
            if (!sc) return null;
            sc.scrollTop = Math.min(420, Math.max(120, sc.scrollHeight * 0.35));
            sc.scrollLeft = Math.min(500, Math.max(180, sc.scrollWidth * 0.35));
            sc.dispatchEvent(new Event('scroll', { bubbles: true }));
            return { top: sc.scrollTop, left: sc.scrollLeft, maxTop: sc.scrollHeight, maxLeft: sc.scrollWidth };
            """
        )
        scroll_positions["mid"] = pos
        s.settle(0.4)

    def scroll_far_corner(s: VisualReviewSession) -> None:
        pos = s.driver.execute_script(
            """
            const sc = document.querySelector('[data-pivot-scroll-container]');
            if (!sc) return null;
            sc.scrollTop = Math.max(0, sc.scrollHeight - sc.clientHeight - 40);
            sc.scrollLeft = Math.max(0, sc.scrollWidth - sc.clientWidth - 40);
            sc.dispatchEvent(new Event('scroll', { bubbles: true }));
            return { top: sc.scrollTop, left: sc.scrollLeft };
            """
        )
        scroll_positions["far"] = pos
        s.settle(0.45)

    def verify_scroll(_b, trans, after) -> None:
        assert scroll_positions.get("mid") is not None
        assert trans.dom_audit["cellCount"] > 0
        assert after.dom_audit["cellCount"] > 0
        assert after.dom_audit["duplicateLogicalCellCount"] == 0

    vr_session.record_transition(
        "VR_203_2d_virtual_scroll_matrix",
        "2D virtual scroll midway diagonally -> far bottom-right window with sticky hierarchy & headers",
        route="/wide",
        action_to_transition_fn=scroll_diagonally_midway,
        complete_to_after_fn=scroll_far_corner,
        after_check_fn=verify_scroll,
    )


def test_vr204_column_pinning_left_and_right_with_scroll(vr_session: VisualReviewSession) -> None:
    def open_header_context_menu(s: VisualReviewSession) -> None:
        all_headers = s.find_visible('[role="columnheader"]')
        headers = [
            el
            for el in all_headers
            if any(tok in (el.text or "").lower() for tok in ("reve", "units", "q1", "q2", "q3", "q4", "m0"))
        ] or all_headers[1:]
        assert headers, "Expected measure column header"
        # Click the '...' menu button inside header if present, or right-click header
        menu_btns = headers[-1].find_elements(By.CSS_SELECTOR, "button")
        visible_btns = [b for b in menu_btns if b.is_displayed()]
        if visible_btns:
            visible_btns[-1].click()
        else:
            ActionChains(s.driver).context_click(headers[-1]).perform()
        s.settle(0.25)

    def pin_column_and_scroll_horizontally(s: VisualReviewSession) -> None:
        pin_items = [
            el
            for el in s.find_visible('[role="menuitem"], button')
            if "Pin" in (el.text or "") and ("Right" in (el.text or "") or "Left" in (el.text or ""))
        ]
        if pin_items:
            pin_items[0].click()
        else:
            ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
        s.settle(0.25)
        s.driver.execute_script(
            """
            const sc = document.querySelector('[data-pivot-scroll-container]');
            if (sc) { sc.scrollLeft = 360; sc.dispatchEvent(new Event('scroll', { bubbles: true })); }
            """
        )
        s.settle(0.35)

    vr_session.record_transition(
        "VR_204_column_pinning_and_horizontal_scroll",
        "Open measure header menu -> Pin column -> scroll horizontally and verify zero duplicate cells",
        route="/wide",
        action_to_transition_fn=open_header_context_menu,
        complete_to_after_fn=pin_column_and_scroll_horizontally,
    )


def test_vr205_column_drag_resize_and_autosize(vr_session: VisualReviewSession) -> None:
    widths = {}

    def _first_header_width(s: VisualReviewSession) -> float:
        return float(
            s.driver.execute_script(
                """
                const h = document.querySelector('[role="columnheader"]');
                return h ? h.getBoundingClientRect().width : 0;
                """
            )
        )

    def drag_resize_handle(s: VisualReviewSession) -> None:
        widths["before"] = _first_header_width(s)
        handles = s.driver.find_elements(By.CSS_SELECTOR, ".sp-resize-handle")
        visible_handle = next((h for h in handles if h.is_displayed()), None)
        assert visible_handle is not None, "Expected visible .sp-resize-handle on column header"
        ActionChains(s.driver).click_and_hold(visible_handle).move_by_offset(95, 0).release().perform()
        s.settle(0.25)
        widths["resized"] = _first_header_width(s)

    def trigger_auto_size(s: VisualReviewSession) -> None:
        s.open_toolbar_section("view")
        s.click_button(text="Auto Size")
        s.settle(0.3)
        widths["autosized"] = _first_header_width(s)

    vr_session.record_transition(
        "VR_205_column_resize_and_autosize",
        "Drag column resize handle +95px -> trigger View toolbar Auto Size",
        route="/pivot",
        action_to_transition_fn=drag_resize_handle,
        complete_to_after_fn=trigger_auto_size,
    )


def test_vr206_sorting_and_multisort(vr_session: VisualReviewSession) -> None:
    def sort_first_measure_desc(s: VisualReviewSession) -> None:
        all_headers = s.find_visible('[role="columnheader"]')
        leaf_headers = [
            el for el in all_headers if any(tok in (el.text or "").lower() for tok in ("reve", "units"))
        ] or all_headers[1:]
        assert leaf_headers
        leaf_headers[0].click()
        s.settle(0.35)

    def shift_sort_second_measure(s: VisualReviewSession) -> None:
        all_headers = s.find_visible('[role="columnheader"]')
        profit_headers = [
            el for el in all_headers if any(tok in (el.text or "").lower() for tok in ("prof", "mar"))
        ] or all_headers[2:]
        if profit_headers:
            ActionChains(s.driver).key_down(Keys.SHIFT).click(profit_headers[0]).key_up(Keys.SHIFT).perform()
        s.settle(0.35)

    vr_session.record_transition(
        "VR_206_column_sort_and_multisort",
        "Click Revenue header to sort -> Shift+Click Profit header for multi-column sort",
        route="/pivot",
        action_to_transition_fn=sort_first_measure_desc,
        complete_to_after_fn=shift_sort_second_measure,
    )


def test_vr207_global_search_focus_filter_and_clear(vr_session: VisualReviewSession) -> None:
    def focus_and_filter_americas(s: VisualReviewSession) -> None:
        inp = s.wait_until(
            lambda: (s.find_visible('[data-testid="pivot-global-search"]') or [None])[0],
            message="Global search input not found",
        )
        inp.click()
        inp.send_keys("Americas")
        inp.send_keys(Keys.ENTER)
        s.wait_until(
            lambda: "EMEA" not in (s.driver.find_element(By.CSS_SELECTOR, '[data-pivot-scroll-container]').text or ""),
            timeout=10.0,
            message="Expected global search 'Americas' to filter out EMEA",
        )

    def clear_global_search(s: VisualReviewSession) -> None:
        clear_btns = s.find_visible('button[aria-label*="Clear search"], button[title*="Clear search"]')
        if clear_btns:
            clear_btns[0].click()
        else:
            inp = s.find_visible('[data-testid="pivot-global-search"]')[0]
            inp.click()
            inp.send_keys(Keys.CONTROL, "a")
            inp.send_keys(Keys.BACKSPACE)
            inp.send_keys(Keys.ENTER)
        s.wait_until(
            lambda: "EMEA" in (s.driver.find_element(By.CSS_SELECTOR, '[data-pivot-scroll-container]').text or ""),
            timeout=10.0,
            message="Expected EMEA to return after clearing global search",
        )

    vr_session.record_transition(
        "VR_207_global_search_and_clear",
        "Focus Global Search (focus ring) & filter 'Americas' -> Clear search to restore full hierarchy",
        route="/pivot",
        action_to_transition_fn=focus_and_filter_americas,
        complete_to_after_fn=clear_global_search,
        expect_visual_change_in_after=False,  # Restores back to clean pre-search baseline!
    )


def test_vr208_column_filter_popover_and_badge(vr_session: VisualReviewSession) -> None:
    def open_filter_popover(s: VisualReviewSession) -> None:
        # Hover first columnheader to reveal .sp-header-control filter button if hover-gated
        hdr = s.find_visible('[role="columnheader"]')[0]
        ActionChains(s.driver).move_to_element(hdr).perform()
        s.settle(0.15)
        filter_btns = s.driver.find_elements(
            By.CSS_SELECTOR,
            '[role="columnheader"] button[title*="Filter"], [role="columnheader"] button[aria-label*="Filter"], [role="columnheader"] .sp-header-control',
        )
        clicked = False
        for b in filter_btns:
            # Drag handles share .sp-header-control but open nothing (and sit under the label).
            if not b.is_displayed() or "drag" in (b.get_attribute("title") or "").lower():
                continue
            try:
                b.click()
            except ElementClickInterceptedException:
                continue
            clicked = True
            opened_by = time.monotonic() + 1.0
            while time.monotonic() < opened_by and not s.find_visible('[aria-label="Filter column"]'):
                time.sleep(0.1)
            if s.find_visible('[aria-label="Filter column"]'):
                break
        if not clicked or len(s.find_visible('[aria-label="Filter column"]')) == 0:
            # Fallback: open sidebar and click filter icon on sidebar chip
            s.click_button(testid="pivot-toggle-sidebar")
            s.settle(0.2)
            chip_filters = s.find_visible('button[title*="Filter"], [aria-label*="Filter"]')
            assert chip_filters, "Expected filter button in header or sidebar"
            chip_filters[0].click()

        s.wait_until(
            lambda: len(s.find_visible('[aria-label="Filter column"], [role="dialog"]')) > 0,
            timeout=8.0,
            message="FilterPopover dialog did not open",
        )
        # Anchored below the header whenever there is comfortable room below it.
        geometry = s.driver.execute_script(
            """
            const popover = document.querySelector('[aria-label="Filter column"]');
            const anchor = document.activeElement && document.activeElement.closest
                ? document.activeElement.closest('[role="columnheader"]') : null;
            const header = anchor || document.querySelector('[role="columnheader"]');
            if (!popover || !header) return null;
            const p = popover.getBoundingClientRect(), h = header.getBoundingClientRect();
            return {top: p.top, bottom: p.bottom, headerBottom: h.bottom, spaceBelow: innerHeight - h.bottom};
            """
        )
        if geometry and geometry["spaceBelow"] >= 260:
            assert geometry["top"] >= geometry["headerBottom"] - 2, f"Filter popover opened above its header: {geometry}"

    def apply_or_dismiss_popover(s: VisualReviewSession) -> None:
        # Type in filter input if available or click Apply / Escape
        dialogs = s.find_visible('[aria-label="Filter column"]')
        if dialogs:
            inputs = dialogs[0].find_elements(By.CSS_SELECTOR, "input[type='text'], input[type='number']")
            visible_inputs = [i for i in inputs if i.is_displayed()]
            if visible_inputs:
                visible_inputs[-1].send_keys("A")
                s.settle(0.2)
            apply_btns = [
                b for b in dialogs[0].find_elements(By.TAG_NAME, "button") if "Apply" in (b.text or "")
            ]
            if apply_btns:
                apply_btns[0].click()
            else:
                ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
        s.settle(0.35)

    def verify_popover_transition(_b, trans) -> None:
        assert any(o["withinViewport"] for o in trans.dom_audit["overlays"]), (
            f"Expected viewport-contained FilterPopover overlay, got {trans.dom_audit['overlays']}"
        )

    vr_session.record_transition(
        "VR_208_header_filter_popover",
        "Open column FilterPopover dialog -> verify viewport containment -> apply condition",
        route="/pivot",
        action_to_transition_fn=open_filter_popover,
        transition_check_fn=verify_popover_transition,
        complete_to_after_fn=apply_or_dismiss_popover,
    )
