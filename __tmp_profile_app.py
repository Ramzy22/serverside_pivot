from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.edge.options import Options as EdgeOptions
import json
import os
import tempfile
import time


def install_fetch_probe(driver):
    driver.execute_script(
        """
        window.__dashFetchLog = [];
        if (!window.__dashFetchProbeInstalled) {
            const originalFetch = window.fetch.bind(window);
            window.fetch = async (...args) => {
                const [resource, init] = args;
                const url = typeof resource === 'string' ? resource : String(resource && resource.url || '');
                const bodyText = init && typeof init.body === 'string' ? init.body : '';
                const entry = {
                    url,
                    method: (init && init.method) || 'GET',
                    bodyText,
                    startedAt: performance.now(),
                };
                const shouldTrack = url.includes('/_dash-update-component');
                if (shouldTrack) {
                    window.__dashFetchLog.push(entry);
                }
                try {
                    const response = await originalFetch(...args);
                    entry.status = response.status;
                    entry.finishedAt = performance.now();
                    entry.durationMs = entry.finishedAt - entry.startedAt;
                    if (shouldTrack) {
                        response.clone().text().then((text) => {
                            entry.responseText = text.slice(0, 4000);
                        }).catch(() => {});
                    }
                    return response;
                } catch (error) {
                    entry.error = String(error);
                    entry.finishedAt = performance.now();
                    entry.durationMs = entry.finishedAt - entry.startedAt;
                    throw error;
                }
            };
            window.__dashFetchProbeInstalled = true;
        }
        """
    )


def profile_inline_edit(driver, wait):
    driver.execute_script("window.__dashFetchLog = [];")
    display_selector = (
        '#pivot-grid [role="gridcell"][data-rowid="North"][data-colid="sales_sum"] '
        '[data-display-rowid="North"][data-display-colid="sales_sum"]'
    )
    wait.until(lambda d: d.find_element(By.CSS_SELECTOR, display_selector))
    current_value = driver.execute_script(
        """
        const node = document.querySelector(arguments[0]);
        if (!node) return null;
        const numericText = String(node.textContent || '').replace(/[^0-9.-]/g, '');
        return numericText ? Number(numericText) : null;
        """,
        display_selector,
    )
    if current_value is None:
        return {"error": "Could not resolve current root aggregate value."}

    next_value = current_value + 10
    element = driver.find_element(By.CSS_SELECTOR, display_selector)
    ActionChains(driver).double_click(element).perform()
    input_selector = 'input[data-edit-rowid="North"][data-edit-colid="sales_sum"]'
    wait.until(lambda d: d.find_element(By.CSS_SELECTOR, input_selector))
    input_el = driver.find_element(By.CSS_SELECTOR, input_selector)
    input_el.send_keys(Keys.CONTROL, "a")
    input_el.send_keys(str(int(next_value)))
    input_el.send_keys(Keys.ENTER)

    wait.until(
        lambda d: d.execute_script(
            """
            const node = document.querySelector(arguments[0]);
            if (!node) return false;
            return String(node.textContent || '').replace(/[^0-9.-]/g, '') === String(arguments[1]);
            """,
            display_selector,
            int(next_value),
        )
    )
    optimistic_value = driver.execute_script(
        """
        const node = document.querySelector(arguments[0]);
        return node ? String(node.textContent || '').trim() : null;
        """,
        display_selector,
    )

    fetch_entry = wait.until(
        lambda d: d.execute_script(
            """
            const entries = Array.from(window.__dashFetchLog || []);
            return entries.find((entry) =>
                entry.bodyText
                && entry.bodyText.includes('"source":"inline-edit"')
                && entry.bodyText.includes('"rowId":"North"')
                && entry.finishedAt
            ) || null;
            """
        )
    )
    time.sleep(1.0)
    final_value = driver.execute_script(
        """
        const node = document.querySelector(arguments[0]);
        return node ? String(node.textContent || '').trim() : null;
        """,
        display_selector,
    )
    return {
        "fromValue": current_value,
        "toValue": next_value,
        "optimisticText": optimistic_value,
        "finalText": final_value,
        "request": fetch_entry,
    }


def main():
    user_data = tempfile.mkdtemp(prefix="edge-prof-")
    opts = EdgeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1200")
    opts.add_argument(f"--user-data-dir={user_data}")
    driver = webdriver.Edge(options=opts)
    wait = WebDriverWait(driver, 90)
    try:
        url = os.environ.get("PIVOT_PROFILE_URL", "http://127.0.0.1:8050/")
        driver.get(url)
        driver.execute_script(
            "localStorage.setItem('pivot-profile','1'); "
            "localStorage.setItem('pivot-profile-console','0');"
        )
        driver.get(url)
        install_fetch_probe(driver)
        wait.until(
            lambda d: d.execute_script(
                "return !!document.querySelector('#pivot-grid [role=grid]')"
            )
        )
        time.sleep(1.0)
        initial = driver.execute_script(
            "return window.__pivotProfiler.latest('pivot-grid');"
        )
        clicked = driver.execute_script(
            """
            const btns = Array.from(document.querySelectorAll('#pivot-grid button'));
            let target = btns.find((b) => /expand/i.test((b.getAttribute('aria-label')||'') + ' ' + (b.getAttribute('title')||'')));
            if (!target) target = btns.find((b) => ['+', '▸', '▶'].includes((b.textContent || '').trim()));
            if (!target && btns.length) target = btns[0];
            if (target) { target.click(); return true; }
            return false;
            """
        )
        if clicked:
            time.sleep(1.5)
        expansion = driver.execute_script(
            "return window.__pivotProfiler.latest('pivot-grid');"
        )
        edit_profile = profile_inline_edit(driver, wait)
        driver.find_element(By.ID, "load-curve-demo-btn").click()
        wait.until(
            lambda d: d.execute_script(
                "return !!document.querySelector('#curve-pivot-grid [role=grid]')"
            )
        )
        time.sleep(1.0)
        curve = driver.execute_script(
            "return window.__pivotProfiler.latest('curve-pivot-grid');"
        )
        payload = {
            "initial": initial,
            "expansion_clicked": clicked,
            "expansion": expansion,
            "edit_root_aggregate": edit_profile,
            "curve_load": curve,
            "summary_pivot": driver.execute_script(
                "return window.__pivotProfiler.summary('pivot-grid');"
            ),
            "summary_curve": driver.execute_script(
                "return window.__pivotProfiler.summary('curve-pivot-grid');"
            ),
        }
        print(json.dumps(payload, indent=2))
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
