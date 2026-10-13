"""Private, browser-derived AG Grid reference evidence for the flagship contract."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
import json
import os
from pathlib import Path
import re
from typing import Iterator

from tests.flagship_contract import CaseResult, load_requirements


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "tests" / "flagship_requirements.json"
DEFAULT_EVIDENCE = ROOT / "artifacts" / "flagship-ag-grid-reference"


def comparable_requirements():
    return tuple(
        requirement for requirement in load_requirements(CONTRACT)
        if requirement.ag_grid.get("result") != "not_applicable"
    )


@dataclass
class LiveCase:
    requirement_id: str
    driver: object
    evidence_root: Path
    observed: str = ""
    parsed_output: object = field(default_factory=dict)

    def save(self, result: str, diagnostic: str = "") -> CaseResult:
        requirements = {item.id: item for item in comparable_requirements()}
        requirement = requirements[self.requirement_id]
        safe_id = re.sub(r"[^a-zA-Z0-9._-]", "_", self.requirement_id)
        case_dir = self.evidence_root / safe_id
        case_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        browser = "unavailable"
        snapshot = {"observed": self.observed, "diagnostic": diagnostic}
        unexpected_console = []
        if self.driver is not None:
            browser = f"{self.driver.capabilities.get('browserName', 'Chrome')} {self.driver.capabilities.get('browserVersion', 'unknown')}"
            screenshot = case_dir / "screen.png"
            self.driver.save_screenshot(str(screenshot))
            paths.append(str(screenshot))
            snapshot.update(self.driver.execute_script("""
                return {
                  config: window.referenceConfig ?? null,
                  trace: window.referenceEvidence?.trace() ?? [],
                  visibleCells: window.referenceEvidence?.visibleCells() ?? [],
                  visibleHeaders: [...document.querySelectorAll('[role=columnheader]')]
                    .filter(e => e.getClientRects().length).map(e => e.innerText),
                  state: window.referenceGrid?.getState() ?? null,
                };
            """))
            try:
                snapshot["console"] = self.driver.get_log("browser")
                def expected_license(entry):
                    message = entry.get("message", "")
                    if "ag-grid-enterprise.js" not in message:
                        return False
                    return (any(token in message for token in (
                        "AG Grid and AG Charts Enterprise License", "License Key Not Found",
                        "All AG Grid and AG Charts Enterprise features are unlocked for trial",
                        "If you want to hide the watermark",
                    )) or bool(re.search(r'"\*{10,}"', message)))

                unexpected_console = [entry for entry in snapshot["console"]
                                      if entry.get("level") == "SEVERE" and not expected_license(entry)]
                snapshot["unexpected_console"] = unexpected_console
            except Exception as error:  # Browser-specific log support is diagnostic only.
                snapshot["console_log_error"] = type(error).__name__
        if result == "passed" and unexpected_console:
            result = "failed"
            diagnostic = "Unexpected browser console error"
            snapshot["diagnostic"] = diagnostic
        if result == "passed" and (not self.observed or not snapshot.get("trace")):
            raise AssertionError("A passing AG Grid result requires observed state and a live SSRM trace")
        snapshot["parsed_output"] = self.parsed_output
        evidence = case_dir / "observation.json"
        evidence.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(evidence))
        case_result = CaseResult(
            requirement_id=requirement.id,
            system="ag_grid",
            host=None,
            test_id=requirement.ag_grid["test_id"],
            result=result,
            observed="; ".join(part for part in (self.observed, diagnostic) if part),
            browser=browser,
            evidence_paths=tuple(paths),
        )
        manifest = self.evidence_root / "case-results.json"
        previous = json.loads(manifest.read_text(encoding="utf-8")) if manifest.exists() else []
        previous = [item for item in previous if item["requirement_id"] != self.requirement_id]
        previous.append(asdict(case_result))
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(previous, ensure_ascii=False, indent=2), encoding="utf-8")
        if unexpected_console and diagnostic == "Unexpected browser console error":
            raise AssertionError(f"Unexpected browser console error: {unexpected_console}")
        return case_result


@contextmanager
def reference_case(requirement_id: str) -> Iterator[LiveCase]:
    """Open the real Chrome reference and retain failure evidence as well as passes."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait

    if requirement_id not in {item.id for item in comparable_requirements()}:
        raise ValueError(f"Unknown or non-comparable requirement: {requirement_id}")
    evidence_root = Path(os.environ.get("AG_GRID_REFERENCE_EVIDENCE", DEFAULT_EVIDENCE)).resolve()
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1680,1200")
    options.add_argument("--force-device-scale-factor=1")
    options.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    driver = None
    case = LiveCase(requirement_id, None, evidence_root)
    try:
        driver = webdriver.Chrome(options=options)
        case.driver = driver
        driver.get(os.environ.get("AG_GRID_REFERENCE_URL", "http://127.0.0.1:4179/"))
        WebDriverWait(driver, 20).until(
            lambda browser: browser.execute_script("return window.referenceEvidence?.ready === true")
        )
        yield case
    except AssertionError as error:
        case.save("failed", str(error))
        raise
    except Exception as error:
        case.save("blocked", f"{type(error).__name__}: {error}")
        raise
    else:
        case.save("passed")
    finally:
        if driver is not None:
            driver.quit()
