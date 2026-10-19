"""Private, browser-derived AG Grid reference evidence for the flagship contract."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Iterator
from uuid import uuid4
from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.parse import urlsplit

from selenium.common.exceptions import TimeoutException

from tests.flagship_contract import CaseResult, load_requirements
from scripts.flagship_manifest import QualificationBinding, load_case_envelope


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "tests" / "flagship_requirements.json"
DEFAULT_EVIDENCE = ROOT / "artifacts" / "flagship-ag-grid-reference"
MANIFEST_SCHEMA = 1


class ReferenceWorkflowUnavailable(RuntimeError):
    """The pinned reference exposes no UI path for the requested workflow."""


def comparable_requirements():
    return tuple(
        requirement for requirement in load_requirements(CONTRACT)
        if requirement.ag_grid.get("result") != "not_applicable"
    )


def start_reference_run(evidence_root: Path, run_id: str | None = None) -> str:
    """Create a fresh manifest for this invocation, retaining older raw evidence separately."""
    run_id = run_id or uuid4().hex
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", run_id):
        raise ValueError("run id must contain only letters, digits, underscore or hyphen")
    evidence_root = Path(evidence_root)
    evidence_root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": MANIFEST_SCHEMA,
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "results": [],
    }
    (evidence_root / "case-results.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return run_id


def load_case_results(evidence_root: Path, expected_run_id: str) -> tuple[CaseResult, ...]:
    """Require the caller's run binding; an old manifest cannot count as this run."""
    manifest = json.loads((Path(evidence_root) / "case-results.json").read_text(encoding="utf-8"))
    if (not isinstance(manifest, dict) or manifest.get("schema_version") != MANIFEST_SCHEMA
            or manifest.get("run_id") != expected_run_id):
        raise ValueError("reference case-results run id or schema does not match")
    return tuple(CaseResult(**{**record, "evidence_paths": tuple(record.get("evidence_paths", ()))})
                 for record in manifest["results"])


def write_reference_case_envelope(
    raw_root: Path, output_path: Path, binding: QualificationBinding,
) -> None:
    """Bind current private observations without upgrading their raw outcomes.

    Content validity remains the evaluator's responsibility. Hashes freeze the
    captured bytes so later replacements cannot qualify, while path checks stop
    retained evidence from an earlier invocation being rebound here.
    """
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", binding.run_id):
        raise ValueError("invalid reference run id")
    run_root = (Path(raw_root) / "runs" / binding.run_id).resolve()
    output_path = Path(output_path).resolve()
    if not output_path.is_relative_to(run_root):
        raise ValueError("reference envelope must remain in the current-run private tree")
    provenance = {
        "run_id": binding.run_id,
        "source_commit": binding.source_commit,
        "sha256": dict(binding.sha256),
    }
    records = []
    seen = set()
    for case in load_case_results(raw_root, binding.run_id):
        if case.requirement_id in seen:
            raise ValueError("duplicate reference requirement id")
        seen.add(case.requirement_id)
        if case.system != "ag_grid" or case.host is not None:
            raise ValueError("reference case must describe AG Grid")
        evidence_hashes = {}
        for name in case.evidence_paths:
            path = Path(name).resolve(strict=True)
            if not path.is_relative_to(run_root) or path == output_path:
                raise ValueError("reference evidence must remain in the current-run private tree")
            if name in evidence_hashes:
                raise ValueError("duplicate reference evidence path")
            evidence_hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        records.append({**asdict(case), **provenance, "loaded_assets": {},
                        "evidence_sha256": evidence_hashes})
    envelope = {"schema_version": MANIFEST_SCHEMA, **provenance, "results": records}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output_path.parent,
                                         prefix=".reference-envelope-", suffix=".json", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(envelope, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        load_case_envelope(temporary, binding)
        os.replace(temporary, output_path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def classify_reference_exception(error: Exception, *, page_loaded: bool) -> str:
    """A visible UI timeout is a failed behavior; unavailable infrastructure is blocked."""
    if isinstance(error, ReferenceWorkflowUnavailable):
        return "blocked"
    return "failed" if page_loaded and isinstance(error, TimeoutException) else "blocked"


@dataclass
class LiveCase:
    requirement_id: str
    driver: object
    evidence_root: Path
    run_id: str
    observed: str = ""
    parsed_output: object = field(default_factory=dict)

    @property
    def download_dir(self) -> Path:
        safe_id = re.sub(r"[^a-zA-Z0-9._-]", "_", self.requirement_id)
        return self.evidence_root / "runs" / self.run_id / safe_id / "downloads"

    def save(self, result: str, diagnostic: str = "") -> CaseResult:
        requirements = {item.id: item for item in comparable_requirements()}
        requirement = requirements[self.requirement_id]
        safe_id = re.sub(r"[^a-zA-Z0-9._-]", "_", self.requirement_id)
        case_dir = self.evidence_root / "runs" / self.run_id / safe_id
        case_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        browser = "unavailable"
        snapshot = {"observed": self.observed, "diagnostic": diagnostic,
                    "provenance": {
                        "config_sha256": hashlib.sha256((ROOT / "reference" / "ag_grid_36_2" / "reference-config.json").read_bytes()).hexdigest(),
                        "fixture_sha256": hashlib.sha256((ROOT / "tests" / "data" / "flagship_parity_rows.json").read_bytes()).hexdigest(),
                    }}
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
                  displayedRows: window.referenceEvidence?.displayedRows?.() ?? [],
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
        snapshot["downloads"] = []
        for download in self.download_dir.glob("*"):
            if not download.is_file() or download.name.endswith(".crdownload"):
                continue
            snapshot["downloads"].append({
                "path": str(download), "bytes": download.stat().st_size,
                "sha256": hashlib.sha256(download.read_bytes()).hexdigest(),
            })
            paths.append(str(download))
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
        current = json.loads(manifest.read_text(encoding="utf-8"))
        if current.get("schema_version") != MANIFEST_SCHEMA or current.get("run_id") != self.run_id:
            raise ValueError("reference case-results run id or schema does not match")
        current["results"] = [item for item in current["results"] if item["requirement_id"] != self.requirement_id]
        current["results"].append(asdict(case_result))
        manifest.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        if unexpected_console and diagnostic == "Unexpected browser console error":
            raise AssertionError(f"Unexpected browser console error: {unexpected_console}")
        return case_result


@contextmanager
def reference_case(requirement_id: str, run_id: str, evidence_root: Path | None = None) -> Iterator[LiveCase]:
    """Open the real Chrome reference and retain failure evidence as well as passes."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait

    if requirement_id not in {item.id for item in comparable_requirements()}:
        raise ValueError(f"Unknown or non-comparable requirement: {requirement_id}")
    evidence_root = Path(evidence_root or os.environ.get("AG_GRID_REFERENCE_EVIDENCE", DEFAULT_EVIDENCE)).resolve()
    url = os.environ.get("AG_GRID_REFERENCE_URL", "http://127.0.0.1:4179/")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1680,1200")
    options.add_argument("--force-device-scale-factor=1")
    options.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    driver = None
    page_loaded = False
    case = LiveCase(requirement_id, None, evidence_root, run_id)
    case.download_dir.mkdir(parents=True, exist_ok=True)
    options.add_experimental_option("prefs", {
        "download.default_directory": str(case.download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    })
    try:
        with urlopen(url, timeout=5) as response:
            if response.status != 200:
                raise OSError(f"Reference HTTP status {response.status}")
        driver = webdriver.Chrome(options=options)
        case.driver = driver
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
            "behavior": "allow", "downloadPath": str(case.download_dir),
        })
        origin = urlsplit(url)
        driver.execute_cdp_cmd("Browser.grantPermissions", {
            "origin": f"{origin.scheme}://{origin.netloc}",
            "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"],
        })
        driver.get(url)
        page_loaded = True
        WebDriverWait(driver, 20).until(
            lambda browser: browser.execute_script("return window.referenceEvidence?.ready === true")
        )
        yield case
    except AssertionError as error:
        case.save("failed", str(error))
        raise
    except Exception as error:
        case.save(classify_reference_exception(error, page_loaded=page_loaded),
                  f"{type(error).__name__}: {error}")
        raise
    else:
        case.save("passed")
    finally:
        if driver is not None:
            driver.quit()
