import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "brag-output"
ASSETS_DIR = OUT_DIR / "composition" / "assets" / "screens"
OUT_DIR.mkdir(parents=True, exist_ok=True)
ASSETS_DIR.mkdir(parents=True, exist_ok=True)

opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--window-size=1920,1080")
opts.add_argument("--force-device-scale-factor=2")
opts.add_argument("--hide-scrollbars")
opts.add_argument("--disable-gpu")
opts.add_argument("--no-sandbox")

driver = webdriver.Chrome(options=opts)

demos = [
    ("trader", "http://127.0.0.1:8051", "trader-dash-pivot"),
    ("hr", "http://127.0.0.1:8052", "hr-dash-pivot"),
    ("saas", "http://127.0.0.1:8053", "saas-dash-pivot"),
]

def wait_for_pivot_ready(driver, comp_id, timeout=18.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = driver.execute_script("""
            const el = document.getElementById(arguments[0]);
            if (!el) return { ready: false, reason: 'no_el' };
            const text = el.innerText || '';
            const retryBtn = Array.from(el.querySelectorAll('button')).find(b => b.innerText.trim() === 'Retry');
            if (retryBtn) {
                retryBtn.click();
                return { ready: false, reason: 'clicked_retry' };
            }
            if (text.includes('We could not load the grid data')) {
                return { ready: false, reason: 'error_banner' };
            }
            if (text.includes('Loading')) {
                return { ready: false, reason: 'still_loading' };
            }
            const hasGrandTotal = text.includes('Grand Total') || text.includes('ROWS ');
            const hasChartIssue = text.includes('No visible rows') || text.includes('does not have visible data');
            return { ready: hasGrandTotal && !hasChartIssue, textLen: text.length };
        """, comp_id)
        if state and state.get("ready"):
            time.sleep(1.2)
            return True
        time.sleep(0.5)
    return False

try:
    for name, url, comp_id in demos:
        print(f"Capturing {name} from {url} ...")
        driver.get(url)
        ok = wait_for_pivot_ready(driver, comp_id)
        print(f"  Initial load ready={ok}")

        # Save full top view
        full_path = OUT_DIR / f"shot_{name}_full.png"
        driver.save_screenshot(str(full_path))
        (ASSETS_DIR / f"shot_{name}_full.png").write_bytes(full_path.read_bytes())

        # Scroll smoothly so the Pivot Workspace Card fills the 1920x1080 frame
        driver.execute_script("""
            const el = document.getElementById(arguments[0]);
            if (el) {
                const section = el.closest('section') || el;
                const rect = section.getBoundingClientRect();
                window.scrollTo({ top: window.scrollY + rect.top - 16, behavior: 'instant' });
            }
        """, comp_id)
        time.sleep(1.0)
        ok_scroll = wait_for_pivot_ready(driver, comp_id)
        print(f"  Scrolled workspace ready={ok_scroll}")

        ws_path = OUT_DIR / f"shot_{name}.png"
        driver.save_screenshot(str(ws_path))
        (ASSETS_DIR / f"shot_{name}.png").write_bytes(ws_path.read_bytes())
        print(f"  -> Saved {full_path.name} and {ws_path.name}")
finally:
    driver.quit()

print("Done capturing verified screenshots!")
