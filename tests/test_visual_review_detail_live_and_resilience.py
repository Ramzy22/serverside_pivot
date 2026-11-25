"""Visual Review Suite 6: Master-Detail Surfaces, Drill-Through, Live CDC Streaming & Error Recovery.

Covers 3-phase visual verification (`01_before` -> `02_transition` -> `03_after`) for:
- VR-601: Master-Detail `inline` (`InlineDetailPanel` sliding row card, sort & close)
- VR-602: Master-Detail `sidepanel` (`DetailSidePanel` right drawer, filter & reflow)
- VR-603: Master-Detail `drawer` (`DetailDrawer` bottom sheet) & Context Menu `Drill Through`
- VR-604: Live CDC Streaming (`LiveStreamDiffBar` status pill, live tick push, `Val`/`Δ`/`%Δ`/`Split`
          modes, `Pause` -> `PAUSED`, `Freeze` -> `FROZEN` & `Explain` panel)
- VR-605: Resilience & Fault Recovery (`RuntimeDataError` alert banner on transient failure -> `Retry` recovery)
"""

from __future__ import annotations

import json
import urllib.request
import pytest
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from tests.visual_review_support import VisualReviewSession, visual_session_factory  # noqa: F401


@pytest.fixture(scope="module")
def vr_session(visual_session_factory) -> VisualReviewSession:
    return visual_session_factory("suite6_detail_live_and_resilience")


def _post_json(url: str, payload: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _click_non_total_show_detail(s: VisualReviewSession) -> None:
    clicked = s.driver.execute_script(
        """
        const btns = [...document.querySelectorAll('button[title="Show detail"]')];
        const nonTotal = btns.find(b => {
            const cell = b.closest('[role="gridcell"]');
            return !cell || cell.dataset.rowid !== '__grand_total__';
        }) || btns[btns.length - 1];
        if (!nonTotal) return false;
        nonTotal.scrollIntoView({ block: 'center' });
        nonTotal.click();
        return true;
        """
    )
    assert clicked, "No non-total 'Show detail' button found"


def test_vr601_master_detail_inline_surface(vr_session: VisualReviewSession) -> None:
    def open_inline_detail(s: VisualReviewSession) -> None:
        _click_non_total_show_detail(s)
        s.wait_until(
            lambda: len(s.find_visible('input[placeholder="Filter rows..."]')) > 0,
            timeout=10.0,
            message="InlineDetailPanel did not open with detail filter input",
        )
        s.wait_until(
            lambda: "Loading detail..." not in (s.driver.find_element(By.TAG_NAME, "body").text or ""),
            timeout=10.0,
            message="InlineDetailPanel stayed stuck in Loading detail...",
        )

    def sort_and_close_inline_detail(s: VisualReviewSession) -> None:
        # Sort inside detail table then close
        detail_ths = s.find_visible("table thead th")
        if detail_ths:
            detail_ths[0].click()
            s.settle(0.25)
        s.click_button(title_contains="Close detail")
        s.wait_until(
            lambda: len(s.find_visible('input[placeholder="Filter rows..."]')) == 0,
            timeout=6.0,
            message="InlineDetailPanel did not close",
        )

    vr_session.record_transition(
        "VR_601_detail_inline_panel",
        "Click row 'Show detail' eye icon -> slide open InlineDetailPanel with source records -> sort & close",
        route="/detail-inline",
        action_to_transition_fn=open_inline_detail,
        complete_to_after_fn=sort_and_close_inline_detail,
        expect_visual_change_in_after=False,
    )


def test_vr602_master_detail_sidepanel_surface(vr_session: VisualReviewSession) -> None:
    def open_sidepanel_detail(s: VisualReviewSession) -> None:
        _click_non_total_show_detail(s)
        s.wait_until(
            lambda: len(s.find_visible('input[placeholder="Filter rows..."]')) > 0,
            timeout=10.0,
            message="DetailSidePanel did not open",
        )
        s.wait_until(
            lambda: "Loading detail..." not in (s.driver.find_element(By.TAG_NAME, "body").text or ""),
            timeout=10.0,
        )

    def filter_inside_sidepanel(s: VisualReviewSession) -> None:
        inp = s.find_visible('input[placeholder="Filter rows..."]')[0]
        inp.click()
        inp.send_keys("Direct")
        inp.send_keys(Keys.ENTER)
        s.settle(0.35)

    vr_session.record_transition(
        "VR_602_detail_sidepanel_drawer",
        "Open right-hand DetailSidePanel -> verify side-by-side table reflow -> filter source rows by 'Direct'",
        route="/detail-sidepanel",
        action_to_transition_fn=open_sidepanel_detail,
        complete_to_after_fn=filter_inside_sidepanel,
    )


def test_vr603_master_detail_bottom_drawer_and_context_drill_through(vr_session: VisualReviewSession) -> None:
    def trigger_context_menu_drill_through(s: VisualReviewSession) -> None:
        cells = [
            el
            for el in s.find_visible('[role="gridcell"][data-rowid][data-colid]')
            if (el.get_attribute("data-rowid") or "") != "__grand_total__"
            and (el.get_attribute("data-colid") or "") != "hierarchy"
        ]
        assert cells
        ActionChains(s.driver).context_click(cells[0]).perform()
        s.wait_until(lambda: len(s.find_visible('[role="menu"]')) > 0, timeout=6.0)
        s.click_button(text="Drill Through")
        s.wait_until(
            lambda: len(s.find_visible('input[placeholder="Filter rows..."]')) > 0,
            timeout=10.0,
            message="Bottom DetailDrawer did not open from context-menu Drill Through",
        )
        s.wait_until(
            lambda: "Loading detail..." not in (s.driver.find_element(By.TAG_NAME, "body").text or ""),
            timeout=10.0,
        )

    def close_bottom_drawer(s: VisualReviewSession) -> None:
        s.click_button(title_contains="Close detail")
        s.wait_until(
            lambda: len(s.find_visible('input[placeholder="Filter rows..."]')) == 0,
            timeout=6.0,
        )

    vr_session.record_transition(
        "VR_603_context_drill_through_bottom_drawer",
        "Right-click measure cell -> click 'Drill Through' -> open bottom DetailDrawer -> close cleanly",
        route="/detail-drawer",
        action_to_transition_fn=trigger_context_menu_drill_through,
        complete_to_after_fn=close_bottom_drawer,
        expect_visual_change_in_after=False,
    )


def test_vr604_live_cdc_streaming_diff_bar_and_modes(vr_session: VisualReviewSession) -> None:
    def setup_live(s: VisualReviewSession) -> None:
        s.wait_until(
            lambda: s.audit_dom()["liveBarVisible"],
            timeout=12.0,
            message="LiveStreamDiffBar did not mount on /live route",
        )
        s.settle(0.35)

    def push_tick_and_switch_to_split_diff_mode(s: VisualReviewSession) -> None:
        _post_json(f"{s.base_url}/vr-control/push-live-tick", {"delta": 12400.0})
        s.settle(0.55)
        split_btns = [b for b in s.find_visible('[data-live-stream-diff-bar="true"] button') if (b.text or "").strip() == "Split"]
        if split_btns:
            split_btns[0].click()
            s.settle(0.25)

    def pause_stream_and_open_explain_if_present(s: VisualReviewSession) -> None:
        bar_btns = s.find_visible('[data-live-stream-diff-bar="true"] button')
        pause_btn = next(
            (b for b in bar_btns if "Pause" in ((b.text or "") + " " + (b.get_attribute("title") or ""))),
            None,
        )
        if pause_btn:
            pause_btn.click()
            s.settle(0.25)
        explain_btn = next(
            (b for b in bar_btns if "Explain" in ((b.text or "") + " " + (b.get_attribute("title") or ""))),
            None,
        )
        if explain_btn:
            explain_btn.click()
            s.settle(0.25)

    vr_session.record_transition(
        "VR_604_live_cdc_stream_bar_and_diff_modes",
        "Live CDC Streaming -> push live delta tick & switch to 'Split' variance mode -> Pause stream & Explain",
        route="/live",
        setup_fn=setup_live,
        action_to_transition_fn=push_tick_and_switch_to_split_diff_mode,
        complete_to_after_fn=pause_stream_and_open_explain_if_present,
    )


def test_vr605_runtime_error_banner_and_retry_recovery(vr_session: VisualReviewSession) -> None:
    def arm_fault_and_trigger_failed_request(s: VisualReviewSession) -> None:
        _post_json(f"{s.base_url}/vr-control/arm-fault", {"count": 1})
        # Trigger a fresh server data request by expanding a region
        expander = s.wait_until(
            lambda: next(
                (el for el in s.find_visible(".sp-expander-btn") if el.get_attribute("aria-expanded") == "false"),
                None,
            ),
            message="Expected expander on /fault route",
        )
        expander.click()
        s.wait_until(
            lambda: any("Retry" in (b.text or "") for b in s.find_visible("button")),
            timeout=10.0,
            message="Expected RuntimeDataError banner with Retry button after simulated fault",
        )

    def click_retry_and_verify_recovery(s: VisualReviewSession) -> None:
        s.click_button(text="Retry")
        s.wait_until(
            lambda: not any("Retry" in (b.text or "") for b in s.find_visible("button")),
            timeout=10.0,
            message="RuntimeDataError banner did not clear after clicking Retry",
        )
        s.settle(0.3)

    def verify_no_raw_secret_leak(_b, trans) -> None:
        body_text = vr_session.driver.find_element(By.TAG_NAME, "body").text or ""
        assert "Traceback" not in body_text, "Raw Python traceback leaked into UI error banner"

    vr_session.record_transition(
        "VR_605_runtime_error_and_retry_recovery",
        "Simulate transient backend failure -> display RuntimeDataError banner with Retry -> click Retry to recover",
        route="/fault",
        action_to_transition_fn=arm_fault_and_trigger_failed_request,
        transition_check_fn=verify_no_raw_secret_leak,
        complete_to_after_fn=click_retry_and_verify_recovery,
    )
