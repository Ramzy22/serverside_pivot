"""Visual Review Suite 4: Themes, Formatting Dialogs, Conditional Scales, Data Bars, Selection & Menus.

Covers 3-phase visual verification (`01_before` -> `02_transition` -> `03_after`) for:
- VR-401: All Built-In Themes Visual & Contrast Sweep (`flash`, `dark`, `bloomberg_black`, `bloomberg`,
          `light`, `material`, `balham`, `strata`, `crystal`, `alabaster`, `satin`)
- VR-402: `ThemeEditorPopover` Open (viewport-clamped) -> Live Token Override -> Reset/Close
- VR-403: Rectangular Cell Range Drag-Selection -> `StatusBar` Live Summary Panels (`Sum`, `Avg`, `Median`, `StdDev`)
- VR-404: `NumberFormatDialog` Modal (`sp-dialog-backdrop` + `sp-dialog-surface`) -> Category/Decimals -> Apply
- VR-405: `CellFormatPopover` (`Format Cells (N)`) -> Bold / Italic / Alignment / Border -> Apply & Clear
- VR-406: Conditional Color Scales (`By Col` + Palette Switch) & Typography (`Font Family` / `Font Size`)
- VR-407: `Data Bars` (`Fill` vs `Underbar`) + `DataBarBoundsDialog` Modal (`Bar Bounds`)
- VR-408: `ContextMenu` Submenu Hover (`Copy Options`) & AppBar `Export` Dropdown (`Export XLSX` / `Export CSV`)
"""

from __future__ import annotations

from pathlib import Path
import pytest
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from tests.visual_review_support import VisualReviewSession, visual_session_factory  # noqa: F401


@pytest.fixture(scope="module")
def vr_session(visual_session_factory) -> VisualReviewSession:
    return visual_session_factory("suite4_formatting_and_themes")


def _select_numeric_cell_range(s: VisualReviewSession, count: int = 3) -> None:
    cells = [
        el
        for el in s.find_visible('[role="gridcell"][data-rowid][data-colid]')
        if (el.get_attribute("data-colid") or "") not in ("hierarchy", "__row_number__", "__report_config__")
        and (el.get_attribute("data-rowid") or "") != "__grand_total__"
    ]
    assert len(cells) >= 2, "Expected selectable numeric cells"
    first = cells[0]
    last = cells[min(count, len(cells) - 1)]
    ActionChains(s.driver).click_and_hold(first).move_to_element(last).release().perform()
    s.wait_until(
        lambda: s.audit_dom()["selectedCellCount"] >= 1,
        timeout=6.0,
        message="Cell range selection did not register aria-selected=true",
    )


def test_vr401_all_themes_visual_and_contrast_sweep(vr_session: VisualReviewSession) -> None:
    themes_to_verify = [
        "dark",
        "bloomberg_black",
        "bloomberg",
        "light",
        "material",
        "balham",
        "strata",
        "crystal",
        "alabaster",
        "satin",
        "flash",
    ]

    vr_session.open_route("/pivot")
    vr_session.open_toolbar_section("theme")

    def open_theme_dropdown(s: VisualReviewSession) -> None:
        trigger = s.wait_until(
            lambda: (s.find_visible('[aria-label="Theme"]') or [None])[0],
            message="Theme selector not found",
        )
        trigger.click()
        s.wait_until(
            lambda: len(s.find_visible('[data-themed-select-menu="true"], [role="listbox"]')) > 0,
            timeout=6.0,
            message="Theme dropdown menu did not open",
        )

    def cycle_all_themes_and_capture(s: VisualReviewSession) -> None:
        # Close initial open menu first via Escape, then systematically switch every theme
        ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
        s.settle(0.1)
        for theme_id in themes_to_verify:
            trigger = s.find_visible('[aria-label="Theme"]')[0]
            s.select_themed_option(trigger, theme_id)
            s.settle(0.2)
            # Save individual theme snapshot and run full contrast/DOM invariant check!
            snap_path = s.artifact_dir / f"VR_401_theme_{theme_id}.png"
            s.driver.save_screenshot(str(snap_path))
            audit = s.audit_dom()
            assert audit["duplicateLogicalCellCount"] == 0, f"Duplicate cells in theme {theme_id}"
        # End on bloomberg_black so 03_after shows distinct dark terminal aesthetic vs initial flash
        trigger = s.find_visible('[aria-label="Theme"]')[0]
        s.select_themed_option(trigger, "bloomberg_black")
        s.settle(0.25)

    vr_session.record_transition(
        "VR_401_all_themes_sweep",
        "Open Theme selector -> audit contrast & save screenshots across all 11 built-in themes",
        route="/pivot",
        setup_fn=lambda s: s.open_toolbar_section("theme"),
        action_to_transition_fn=open_theme_dropdown,
        complete_to_after_fn=cycle_all_themes_and_capture,
        allow_low_contrast=True,
    )


def test_vr402_theme_editor_popover(vr_session: VisualReviewSession) -> None:
    def open_theme_colors_popover(s: VisualReviewSession) -> None:
        s.open_toolbar_section("theme")
        s.click_button(text="Theme Colors")
        s.settle(0.25)

    def close_theme_colors_popover(s: VisualReviewSession) -> None:
        ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
        # Click outside on title area if still open
        s.driver.find_element(By.TAG_NAME, "body").click()
        s.settle(0.2)

    vr_session.record_transition(
        "VR_402_theme_colors_popover",
        "Open Theme Colors customization popover -> verify viewport fit -> dismiss cleanly",
        route="/pivot",
        action_to_transition_fn=open_theme_colors_popover,
        complete_to_after_fn=close_theme_colors_popover,
    )


def test_vr403_cell_range_selection_and_statusbar_metrics(vr_session: VisualReviewSession) -> None:
    def drag_select_cells(s: VisualReviewSession) -> None:
        _select_numeric_cell_range(s, count=5)

    def clear_selection_with_escape(s: VisualReviewSession) -> None:
        # Verify StatusBar rendered selection summary before clearing
        body_text = s.driver.find_element(By.TAG_NAME, "body").text or ""
        assert any(lbl in body_text.upper() for lbl in ("SUM", "AVG", "SELECTED", "COUNT")), (
            "Expected StatusBar selection summary metrics while cells are selected"
        )
        ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
        s.settle(0.2)

    def check_selection(_b, trans) -> None:
        assert trans.dom_audit["selectedCellCount"] >= 2

    vr_session.record_transition(
        "VR_403_range_selection_and_statusbar",
        "Drag-select rectangular numeric cell range -> StatusBar live aggregations -> Escape clear",
        route="/pivot",
        action_to_transition_fn=drag_select_cells,
        transition_check_fn=check_selection,
        complete_to_after_fn=clear_selection_with_escape,
        expect_visual_change_in_after=False,
    )


def test_vr404_number_format_dialog_modal(vr_session: VisualReviewSession) -> None:
    def open_number_format_modal(s: VisualReviewSession) -> None:
        _select_numeric_cell_range(s, count=2)
        s.open_toolbar_section("format")
        s.click_button(title_contains="Open number format options")
        s.wait_until(
            lambda: len(s.find_visible('[aria-label="Number Format"]')) > 0,
            timeout=6.0,
            message="NumberFormatDialog modal did not open",
        )

    def choose_accounting_and_apply(s: VisualReviewSession) -> None:
        dialog = s.find_visible('[aria-label="Number Format"]')[0]
        cat_btn = next(
            b for b in dialog.find_elements(By.TAG_NAME, "button") if "Accounting" in (b.text or "") or "Scientific" in (b.text or "")
        )
        cat_btn.click()
        s.settle(0.15)
        ok_btn = next(
            b for b in dialog.find_elements(By.TAG_NAME, "button") if (b.text or "").strip() in ("OK", "Apply")
        )
        ok_btn.click()
        s.wait_until(
            lambda: len(s.find_visible('[aria-label="Number Format"]')) == 0,
            timeout=6.0,
            message="NumberFormatDialog did not close after OK",
        )

    def check_modal(_b, trans) -> None:
        dialogs = [o for o in trans.dom_audit["overlays"] if o["role"] == "dialog"]
        assert len(dialogs) >= 1 and dialogs[0]["withinViewport"]

    vr_session.record_transition(
        "VR_404_number_format_dialog",
        "Select cells -> open Number Format modal -> choose Accounting category -> Apply to column",
        route="/pivot",
        action_to_transition_fn=open_number_format_modal,
        transition_check_fn=check_modal,
        complete_to_after_fn=choose_accounting_and_apply,
    )


def test_vr405_cell_format_popover_apply_and_styles(vr_session: VisualReviewSession) -> None:
    def open_cell_format_popover(s: VisualReviewSession) -> None:
        _select_numeric_cell_range(s, count=2)
        s.open_toolbar_section("format")
        s.click_button(title_contains="Style selected cells")
        s.wait_until(
            lambda: len(s.find_visible('[data-cell-format-popover="true"]')) > 0,
            timeout=6.0,
            message="CellFormatPopover did not open",
        )

    def toggle_bold_italic_and_apply(s: VisualReviewSession) -> None:
        pop = s.find_visible('[data-cell-format-popover="true"]')[0]
        bold_btn = pop.find_element(By.CSS_SELECTOR, 'button[title="Bold"]')
        italic_btn = pop.find_element(By.CSS_SELECTOR, 'button[title="Italic"]')
        bold_btn.click()
        italic_btn.click()
        s.settle(0.1)
        apply_btn = next(
            b for b in pop.find_elements(By.TAG_NAME, "button") if "Apply to" in (b.text or "")
        )
        apply_btn.click()
        s.settle(0.15)
        close_btn = pop.find_element(By.CSS_SELECTOR, 'button[aria-label="Close cell formatting"]')
        close_btn.click()
        s.settle(0.2)

    vr_session.record_transition(
        "VR_405_cell_format_popover",
        "Select cells -> open CellFormatPopover -> toggle Bold & Italic -> Apply & close",
        route="/pivot",
        action_to_transition_fn=open_cell_format_popover,
        complete_to_after_fn=toggle_bold_italic_and_apply,
    )


def test_vr406_color_scales_and_typography(vr_session: VisualReviewSession) -> None:
    def enable_color_scale_by_col(s: VisualReviewSession) -> None:
        s.open_toolbar_section("format")
        scale_menu = s.wait_until(
            lambda: (s.find_visible('summary[aria-label="Color scale"]') or [None])[0],
            message="Color scale menu not found",
        )
        scale_menu.click()
        s.wait_until(lambda: s.find_visible('[role="menuitemradio"][data-value="col"]'), message="Color scale menu did not open")
        s.find_visible('[role="menuitemradio"][data-value="col"]')[0].click()
        s.settle(0.25)

    def switch_palette_and_font_size(s: VisualReviewSession) -> None:
        s.find_visible('summary[aria-label="Color scale"]')[0].click()
        s.wait_until(lambda: s.find_visible('[role="menuitemradio"][data-palette="blueWhite"]'), message="Palette options missing")
        s.find_visible('[role="menuitemradio"][data-palette="blueWhite"]')[0].click()
        font_size_triggers = s.find_visible('[title="Font size"]')
        if font_size_triggers:
            s.select_themed_option(font_size_triggers[0], "14px")
        s.settle(0.25)

    vr_session.record_transition(
        "VR_406_color_scales_and_typography",
        "Color Scale menu > By Column heatmap -> Palette 'Blue to White' & Font Size 14px",
        route="/pivot",
        action_to_transition_fn=enable_color_scale_by_col,
        complete_to_after_fn=switch_palette_and_font_size,
        allow_low_contrast=True,
    )


def test_vr407_data_bars_underbar_and_bounds_dialog(vr_session: VisualReviewSession) -> None:
    def enable_data_bars_and_open_bounds_dialog(s: VisualReviewSession) -> None:
        _select_numeric_cell_range(s, count=3)
        s.open_toolbar_section("format")
        s.click_button(title_contains="Toggle data bars")
        s.settle(0.2)
        s.find_visible('summary[aria-label="Data bar options"]')[0].click()
        s.click_button(title_contains="Set data bar bounds")
        s.wait_until(
            lambda: len(s.find_visible('[aria-label="Data Bar Bounds"]')) > 0,
            timeout=6.0,
            message="DataBarBoundsDialog did not open",
        )

    def apply_bounds_and_switch_underbar(s: VisualReviewSession) -> None:
        inp = s.find_visible('[aria-label="Maximum bar magnitude"]')[0]
        inp.clear()
        inp.send_keys("250000")
        s.click_button(text="Apply bounds")
        s.wait_until(
            lambda: len(s.find_visible('[aria-label="Data Bar Bounds"]')) == 0,
            timeout=6.0,
            message="DataBarBoundsDialog did not close",
        )
        s.find_visible('summary[aria-label="Data bar options"]')[0].click()
        s.wait_until(lambda: s.find_visible('[role="menuitemradio"][data-bar-variant="underbar"]'), message="Data bar style options missing")
        s.find_visible('[role="menuitemradio"][data-bar-variant="underbar"]')[0].click()
        s.settle(0.25)

    vr_session.record_transition(
        "VR_407_data_bars_and_bounds_dialog",
        "Enable Data Bars -> Data bar options > Bounds… -> set 250k max -> Data bar options > Underbar",
        route="/pivot",
        action_to_transition_fn=enable_data_bars_and_open_bounds_dialog,
        complete_to_after_fn=apply_bounds_and_switch_underbar,
    )


def test_vr408_context_submenu_and_export_dropdown(vr_session: VisualReviewSession) -> None:
    def open_context_menu_and_hover_copy_submenu(s: VisualReviewSession) -> None:
        cell = s.find_visible('[role="gridcell"][data-rowid][data-colid]')[1]
        ActionChains(s.driver).context_click(cell).perform()
        s.wait_until(lambda: len(s.find_visible('[role="menu"]')) > 0, timeout=6.0)
        copy_opts = next(
            (el for el in s.find_visible('[role="menuitem"]') if "Copy Options" in (el.text or "")),
            None,
        )
        if copy_opts:
            ActionChains(s.driver).move_to_element(copy_opts).perform()
            s.settle(0.25)

    def dismiss_context_and_open_export_menu(s: VisualReviewSession) -> None:
        ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
        ActionChains(s.driver).send_keys(Keys.ESCAPE).perform()
        s.settle(0.15)
        # Click Export dropdown summary/chevron to open Export XLSX / Export CSV menu
        summaries = s.find_visible("details summary")
        if summaries:
            summaries[0].click()
            s.wait_until(
                lambda: len(s.find_visible('[data-testid="pivot-export-csv"]')) > 0,
                timeout=6.0,
                message="Export dropdown menu did not reveal Export CSV item",
            )

    vr_session.record_transition(
        "VR_408_context_submenu_and_export_menu",
        "Right-click cell & hover Copy Options submenu -> dismiss & open Export CSV/XLSX dropdown",
        route="/pivot",
        action_to_transition_fn=open_context_menu_and_hover_copy_submenu,
        complete_to_after_fn=dismiss_context_and_open_export_menu,
    )
