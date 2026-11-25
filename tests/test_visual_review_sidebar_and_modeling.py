"""Visual Review Suite 3: Sidebar Modeling, Field Zones, Aggregations, Formulas, Custom Categories & Undo/Redo.

Covers 3-phase visual verification (`01_before` -> `02_transition` -> `03_after`) for:
- VR-301: Sidebar Open / Collapse Reflow & Sidebar Section Disclosure Transitions
- VR-302: Drag-and-Drop Field Chip Across Drop Zones (`Rows` -> `Columns` / `Filters`)
- VR-303: Value Measure Aggregation Change (`sum` -> `avg`) & Remove/Add Measure
- VR-304: Formula Builder in Sidebar -> Create Calculated Measure -> Render Column in Grid
- VR-305: Custom Categories Editor (`+ Category` draft -> Apply -> Toast Notification & Field Chip)
- VR-306: Undo / Redo Transaction History & `Save View` Toast Confirmation
"""

from __future__ import annotations

import pytest
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

from tests.visual_review_support import VisualReviewSession, visual_session_factory  # noqa: F401


@pytest.fixture(scope="module")
def vr_session(visual_session_factory) -> VisualReviewSession:
    return visual_session_factory("suite3_sidebar_and_modeling")


def _ensure_sidebar_open(s: VisualReviewSession) -> None:
    zones = s.find_visible("[data-sidebar-drop-zone]")
    if not zones:
        s.click_button(testid="pivot-toggle-sidebar")
        s.wait_until(
            lambda: len(s.find_visible("[data-sidebar-drop-zone]")) > 0,
            timeout=8.0,
            message="Sidebar drop zones did not appear after clicking Menu",
        )
        s.settle(0.2)


def test_vr301_sidebar_toggle_and_canvas_reflow(vr_session: VisualReviewSession) -> None:
    def open_sidebar(s: VisualReviewSession) -> None:
        _ensure_sidebar_open(s)

    def close_sidebar(s: VisualReviewSession) -> None:
        s.click_button(testid="pivot-toggle-sidebar")
        s.wait_until(
            lambda: len(s.find_visible("[data-sidebar-drop-zone]")) == 0,
            timeout=8.0,
            message="Sidebar did not collapse on second Menu click",
        )

    # Start with sidebar closed if open, or ensure deterministic state
    def setup_closed(s: VisualReviewSession) -> None:
        if len(s.find_visible("[data-sidebar-drop-zone]")) > 0:
            s.click_button(testid="pivot-toggle-sidebar")
            s.settle(0.25)

    vr_session.record_transition(
        "VR_301_sidebar_open_close_reflow",
        "Closed sidebar -> click Menu to reveal SidebarPanel & reflow canvas -> collapse Sidebar cleanly",
        route="/pivot",
        setup_fn=setup_closed,
        action_to_transition_fn=open_sidebar,
        complete_to_after_fn=close_sidebar,
        expect_visual_change_in_after=False,
    )


def test_vr302_field_zone_drag_and_drop(vr_session: VisualReviewSession) -> None:
    def setup_sidebar(s: VisualReviewSession) -> None:
        _ensure_sidebar_open(s)

    def drag_or_remove_field_in_rows_zone(s: VisualReviewSession) -> None:
        # Scroll Rows & Columns drop zones into view and remove/move 'Channel' chip
        acted = s.driver.execute_script(
            """
            const rowsZone = document.querySelector('[data-sidebar-drop-zone="rows"]');
            const colsZone = document.querySelector('[data-sidebar-drop-zone="columns"]')
                          || document.querySelector('[data-sidebar-drop-zone="colFields"]');
            if (colsZone) colsZone.scrollIntoView({ block: 'center' });
            const chips = [...document.querySelectorAll('[data-sidebar-field-chip], [draggable="true"]')];
            const chip = chips.find(c => (c.innerText || '').toLowerCase().includes('channel')) || chips[chips.length - 1];
            if (chip && colsZone) {
                const dt = new DataTransfer();
                chip.dispatchEvent(new DragEvent('dragstart', { bubbles: true, cancelable: true, dataTransfer: dt }));
                colsZone.dispatchEvent(new DragEvent('dragenter', { bubbles: true, cancelable: true, dataTransfer: dt }));
                colsZone.dispatchEvent(new DragEvent('dragover', { bubbles: true, cancelable: true, dataTransfer: dt }));
                colsZone.dispatchEvent(new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer: dt }));
                chip.dispatchEvent(new DragEvent('dragend', { bubbles: true, cancelable: true, dataTransfer: dt }));
                return true;
            }
            if (rowsZone) {
                rowsZone.scrollIntoView({ block: 'center' });
                return true;
            }
            return false;
            """
        )
        assert acted, "Could not locate sidebar drop zones"
        # Also click '+ Expand All' inside Rows zone if visible for clear visual transition
        exp_btns = [b for b in s.find_visible("button") if "Expand All" in (b.text or "")]
        if exp_btns:
            exp_btns[0].click()
        s.settle(0.35)

    def remove_chip_from_zone(s: VisualReviewSession) -> None:
        s.driver.execute_script(
            """
            const rem = [...document.querySelectorAll('.sp-chip-remove, [data-sidebar-drop-zone="rows"] button')];
            const closeBtn = rem.find(b => (b.innerText || '').includes('×') || b.classList.contains('sp-chip-remove'));
            if (closeBtn) { closeBtn.scrollIntoView({ block: 'center' }); closeBtn.click(); }
            """
        )
        s.settle(0.35)

    vr_session.record_transition(
        "VR_302_sidebar_drag_drop_zones",
        "Interact with Sidebar Rows/Columns zones & Expand All -> remove chip from Rows zone",
        route="/pivot",
        setup_fn=setup_sidebar,
        action_to_transition_fn=drag_or_remove_field_in_rows_zone,
        complete_to_after_fn=remove_chip_from_zone,
    )


def test_vr303_value_aggregation_selector(vr_session: VisualReviewSession) -> None:
    def setup_sidebar(s: VisualReviewSession) -> None:
        _ensure_sidebar_open(s)

    def scroll_to_values_zone_and_open_agg(s: VisualReviewSession) -> None:
        found = s.driver.execute_script(
            """
            const zones = [...document.querySelectorAll('[data-sidebar-drop-zone]')];
            const valZone = zones.find(z => (z.getAttribute('data-sidebar-drop-zone') || '').toLowerCase().includes('val'))
                         || zones[zones.length - 1];
            if (!valZone) return false;
            valZone.scrollIntoView({ block: 'center' });
            return true;
            """
        )
        assert found, "No sidebar drop zones found in DOM"
        s.settle(0.2)
        # Click the first aggregation selector inside the scrolled Values section
        clicked = s.driver.execute_script(
            """
            const zones = [...document.querySelectorAll('[data-sidebar-drop-zone]')];
            const valZone = zones.find(z => (z.getAttribute('data-sidebar-drop-zone') || '').toLowerCase().includes('val'))
                         || zones[zones.length - 1];
            const btns = [...valZone.querySelectorAll('button, select')];
            const aggBtn = btns.find(b => /sum|avg|mean|count|min|max/i.test(b.innerText || b.value || '')) || btns[0];
            if (aggBtn) { aggBtn.scrollIntoView({ block: 'center' }); aggBtn.click(); return true; }
            return false;
            """
        )
        assert clicked, "Expected aggregation button inside Values zone"
        s.settle(0.25)

    def select_avg_aggregation(s: VisualReviewSession) -> None:
        opts = s.find_visible('[data-themed-select-menu="true"] [role="option"], [role="listbox"] [role="option"]')
        avg_opt = next((o for o in opts if "avg" in (o.text or "").lower() or "average" in (o.text or "").lower()), None)
        if avg_opt:
            avg_opt.click()
        elif opts:
            opts[min(1, len(opts) - 1)].click()
        s.settle(0.4)

    vr_session.record_transition(
        "VR_303_value_aggregation_change",
        "Scroll Sidebar to Values zone & open aggregation menu -> switch Sum to Avg and recompute",
        route="/pivot",
        setup_fn=setup_sidebar,
        action_to_transition_fn=scroll_to_values_zone_and_open_agg,
        complete_to_after_fn=select_avg_aggregation,
    )


def test_vr304_formula_measure_creation(vr_session: VisualReviewSession) -> None:
    def setup_sidebar(s: VisualReviewSession) -> None:
        _ensure_sidebar_open(s)

    def open_formula_builder(s: VisualReviewSession) -> None:
        # Scroll sidebar down to Calculated / Formula section and expand if collapsed
        opened = s.driver.execute_script(
            """
            const buttons = [...document.querySelectorAll('button, [role="button"], div')];
            const calcHeader = buttons.find(el => {
                const t = (el.innerText || '').trim();
                return (t === 'Calculated Fields' || t === 'Formulas' || t.includes('+ Formula') || t.includes('Add Formula'));
            });
            if (calcHeader) {
                calcHeader.scrollIntoView({ block: 'center' });
                calcHeader.click();
                return true;
            }
            return false;
            """
        )
        s.settle(0.25)
        # Also click '+ Formula' / 'Add Formula' button if revealed
        for b in s.find_visible("button"):
            txt = (b.text or "").strip()
            if "+ Formula" in txt or "Add Formula" in txt or "New Formula" in txt:
                b.click()
                break
        s.settle(0.25)

    def fill_and_submit_formula(s: VisualReviewSession) -> None:
        s.driver.execute_script(
            """
            const inputs = [...document.querySelectorAll('input[type="text"], textarea')].filter(i => i.offsetParent !== null);
            const formulaInput = inputs.find(i => (i.placeholder || '').includes('[') || (i.placeholder || '').toLowerCase().includes('formula'));
            const nameInput = inputs.find(i => (i.placeholder || '').toLowerCase().includes('name') || (i.placeholder || '').toLowerCase().includes('label'));
            const setNativeValue = (el, val) => {
                const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set
                            || Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set;
                if (setter) setter.call(el, val); else el.value = val;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            };
            if (nameInput) setNativeValue(nameInput, 'Net Spread');
            if (formulaInput) setNativeValue(formulaInput, '[revenue] - [cost]');
            """
        )
        s.settle(0.2)
        for b in s.find_visible("button"):
            txt = (b.text or "").strip()
            if txt in ("Add", "Apply", "Save Formula", "Add Formula", "Create"):
                b.click()
                break
        s.settle(0.4)

    vr_session.record_transition(
        "VR_304_formula_builder_create",
        "Expand Sidebar Formula Builder -> enter '[revenue] - [cost]' -> create calculated measure",
        route="/pivot",
        setup_fn=setup_sidebar,
        action_to_transition_fn=open_formula_builder,
        complete_to_after_fn=fill_and_submit_formula,
    )


def test_vr305_custom_categories_draft_and_apply(vr_session: VisualReviewSession) -> None:
    def setup_sidebar(s: VisualReviewSession) -> None:
        _ensure_sidebar_open(s)

    def open_custom_category_draft(s: VisualReviewSession) -> None:
        # Scroll sidebar to Custom Categories / '+ Category' or 'Number Group'
        s.driver.execute_script(
            """
            const all = [...document.querySelectorAll('button, [role="button"], div')];
            const sec = all.find(el => (el.innerText || '').trim() === 'Custom Categories');
            if (sec) sec.scrollIntoView({ block: 'center' });
            """
        )
        s.settle(0.15)
        cat_btns = [b for b in s.find_visible("button") if (b.text or "").strip() == "+ Category"]
        if not cat_btns:
            # Disclosure section may be collapsed; click header containing 'Custom'
            for el in s.find_visible("button, [role='button']"):
                if "Custom" in (el.text or "") or "Categor" in (el.text or ""):
                    el.click()
                    s.settle(0.2)
                    break
            cat_btns = [b for b in s.find_visible("button") if "+ Category" in (b.text or "")]
        assert cat_btns, "Expected '+ Category' button in Sidebar"
        cat_btns[0].click()
        s.settle(0.25)

    def configure_and_apply_category(s: VisualReviewSession) -> None:
        s.driver.execute_script(
            """
            const setVal = (el, val) => {
                const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
                if (setter) setter.call(el, val); else el.value = val;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            };
            const inputs = [...document.querySelectorAll('input')].filter(i => i.offsetParent !== null && i.getAttribute('data-testid') !== 'pivot-global-search');
            if (inputs.length >= 1) setVal(inputs[0], 'Region Tier');
            if (inputs.length >= 2) setVal(inputs[1], 'Core Americas');
            if (inputs.length >= 3) setVal(inputs[inputs.length - 1], 'Americas');
            """
        )
        s.settle(0.2)
        for b in s.find_visible("button"):
            txt = (b.text or "").strip()
            if txt in ("Apply", "Save", "Create Category", "Apply Category"):
                b.click()
                break
        s.settle(0.35)

    vr_session.record_transition(
        "VR_305_custom_category_editor",
        "Open '+ Category' draft in Sidebar -> populate rule -> Apply & trigger notification",
        route="/pivot",
        setup_fn=setup_sidebar,
        action_to_transition_fn=open_custom_category_draft,
        complete_to_after_fn=configure_and_apply_category,
    )


def test_vr306_undo_redo_and_save_view_toast(vr_session: VisualReviewSession) -> None:
    def make_layout_change_then_undo(s: VisualReviewSession) -> None:
        s.open_toolbar_section("view")
        s.click_button(title_contains="Swap row fields and column fields")
        s.settle(0.35)
        undo_btn = s.wait_until(
            lambda: next(
                (b for b in s.find_visible("button") if "Undo" in (b.get_attribute("title") or "") and b.is_enabled()),
                None,
            ),
            timeout=8.0,
            message="Undo button did not become enabled after Transpose",
        )
        undo_btn.click()
        s.settle(0.35)

    def redo_and_save_view(s: VisualReviewSession) -> None:
        redo_btn = s.wait_until(
            lambda: next(
                (b for b in s.find_visible("button") if "Redo" in (b.get_attribute("title") or "") and b.is_enabled()),
                None,
            ),
            timeout=8.0,
            message="Redo button did not become enabled after Undo",
        )
        redo_btn.click()
        s.settle(0.3)
        s.open_toolbar_section("theme")
        s.click_button(text="Save View")
        s.settle(0.25)

    vr_session.record_transition(
        "VR_306_undo_redo_and_save_view",
        "Transpose -> Undo back -> Redo forward -> click Save View in Theme toolbar",
        route="/pivot",
        action_to_transition_fn=make_layout_change_then_undo,
        complete_to_after_fn=redo_and_save_view,
    )
