"""Build and certify the published Dash, React, and Streamlit showcases.

Use --installed-release for fresh commit-bound wheel/npm artifact hosts.
The legacy path supports local diagnostics; --export-qualification retains
its separate publication and redaction rules. Browser runs leave auditable
screenshots, browser logs, network traces, clipboard/download checks, and the
feature matrix in one artifact directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Iterator, Mapping
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "dash_tanstack_pivot"
DEFAULT_ARTIFACT_DIR = ROOT / "artifacts" / "flagship-certification"
EXPORT_QUALIFICATION_ARTIFACT_DIR = ROOT / "artifacts" / "export-qualification"
EXPORT_PUBLICATION_MARKER = ".pivot-export-qualification-publication-v1"
QUALIFICATION_HOSTS = ("dash", "react", "streamlit")
QUALIFICATION_PHASES = ("normal", "constrained")
QUALIFICATION_TEST_COUNTS = {"normal": 12, "constrained": 6}
SAFE_LIMIT_CODES = {"PIVOT_EXPORT_MAX_ROWS", "PIVOT_EXPORT_MAX_BYTES"}

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _run(command: list[str], *, cwd: Path = ROOT) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def _wait_for(url: str, *, process: subprocess.Popen[str], timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Flagship process exited before becoming ready: {url}")
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if 200 <= response.status < 500:
                    return
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last_error = error
        time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {url}: {last_error}")


@contextmanager
def _process(command: list[str], *, name: str, artifact_dir: Path, environment: dict[str, str]) -> Iterator[subprocess.Popen[str]]:
    stdout = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False)
    stderr = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False)
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=environment,
        stdout=stdout,
        stderr=stderr,
        text=True,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
        stdout.close()
        stderr.close()
        metadata = {
            "process": name,
            "returnCode": process.poll(),
            "stdoutLineCount": _line_count(Path(stdout.name)),
            "stderrLineCount": _line_count(Path(stderr.name)),
        }
        (artifact_dir / f"{name}.process.json").write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
        )
        Path(stdout.name).unlink(missing_ok=True)
        Path(stderr.name).unlink(missing_ok=True)


def _line_count(path: Path) -> int:
    try:
        with path.open(encoding="utf-8", errors="replace") as stream:
            return sum(1 for _ in stream)
    except OSError:
        return 0


def _write_matrix(artifact_dir: Path, *, result: str, started_at: datetime, completed_at: datetime) -> None:
    # ``tests`` is a local package solely to share this immutable release
    # contract with the runner; it is not application code.
    from tests.flagship_capabilities import CAPABILITIES, HOSTS, markdown_matrix

    report = {
        "startedAt": started_at.isoformat(),
        "completedAt": completed_at.isoformat(),
        "result": result,
        "hosts": list(HOSTS),
        "capabilities": [
            {
                "id": item.id,
                "group": item.group,
                "workflow": item.workflow,
                "verification": item.verification,
                "supported": item.supported,
                "disabledReason": item.disabled_reason,
            }
            for item in CAPABILITIES
        ],
    }
    (artifact_dir / "flagship-certification.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    (artifact_dir / "flagship-functionality-matrix.md").write_text(markdown_matrix(), encoding="utf-8")


def _install_package(wheel: Path) -> None:
    # Streamlit loads its frontend from the installed Python distribution.
    # Reinstall even when builds are skipped so the browser suite never uses a
    # stale manifest or hashed asset left behind by an earlier local install.
    wheel = wheel.resolve()
    if not wheel.is_file() or wheel.suffix.lower() != ".whl":
        raise FileNotFoundError(f"Prebuilt wheel is required: {wheel}")
    _run([
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-deps",
        "--force-reinstall",
        str(wheel),
    ], cwd=ROOT)


def _build() -> None:
    npm = shutil.which("npm") or "npm"
    _run([npm, "run", "build:js"], cwd=PACKAGE)
    _run([npm, "run", "build:react"], cwd=PACKAGE)
    _run([npm, "run", "build:streamlit"], cwd=PACKAGE)
    with tempfile.TemporaryDirectory(prefix="pivot-certification-wheel-") as wheel_dir:
        _run([
            sys.executable, "-m", "build", "--wheel", "--outdir", wheel_dir, ".",
        ], cwd=PACKAGE)
        wheels = list(Path(wheel_dir).glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"Expected one built wheel, found {len(wheels)}")
        _install_package(wheels[0])


def _run_browser_suite(
    artifact_dir: Path,
    environment: dict[str, str],
    *,
    junit_path: Path | None = None,
    selection: str | None = None,
) -> int:
    child_environment = {
        **environment,
        "PIVOT_RUN_FLAGSHIP_BROWSER_TESTS": "1",
        "PIVOT_FLAGSHIP_ARTIFACT_DIR": str(artifact_dir),
    }
    suite = ("tests/test_flagship_workflows.py" if environment.get("PIVOT_FLAGSHIP_EXPORT_JOB_DATA") == "1"
             else "tests/test_flagship_certification.py")
    command = [sys.executable, "-m", "pytest", suite, "-m", "flagship", "-q"]
    if selection:
        command.extend(["-k", selection])
    if junit_path is not None:
        command.append(f"--junitxml={junit_path}")
    completed = subprocess.run(command, cwd=ROOT, env=child_environment)
    return completed.returncode


def _run_with_hosts(
    artifact_dir: Path,
    environment: dict[str, str],
    browser_suite: Callable[[], int],
) -> int:
    node = shutil.which("node") or "node"
    streamlit = shutil.which("streamlit") or "streamlit"
    with ExitStack() as stack:
        runtime = stack.enter_context(_process(
            [sys.executable, "-m", "uvicorn", "examples.flagship_runtime_server:app", "--host", "127.0.0.1", "--port", "8765"],
            name="runtime", artifact_dir=artifact_dir, environment=environment,
        ))
        _wait_for("http://127.0.0.1:8765/health", process=runtime)
        dash = stack.enter_context(_process(
            [sys.executable, "examples/flagship_dash.py"],
            name="dash", artifact_dir=artifact_dir, environment=environment,
        ))
        _wait_for("http://127.0.0.1:8050", process=dash)
        react = stack.enter_context(_process(
            [node, "dash_tanstack_pivot/node_modules/vite/bin/vite.js", "--config", "examples/flagship_react/vite.config.ts", "--host", "127.0.0.1", "--port", "5174"],
            name="react", artifact_dir=artifact_dir, environment=environment,
        ))
        _wait_for("http://127.0.0.1:5174", process=react)
        streamlit_process = stack.enter_context(_process(
            [streamlit, "run", "examples/flagship_streamlit.py", "--server.address", "127.0.0.1", "--server.port", "8502", "--server.headless", "true", "--browser.gatherUsageStats", "false"],
            name="streamlit", artifact_dir=artifact_dir, environment=environment,
        ))
        _wait_for("http://127.0.0.1:8502/_stcore/health", process=streamlit_process)
        return browser_suite()


def _run_reference_suite(artifact_dir: Path, environment: Mapping[str, str], junit_path: Path) -> int:
    """Execute every committed reference browser case with private raw output."""
    child_environment = {
        **environment,
        "AG_GRID_REFERENCE_LIVE": "1",
        "AG_GRID_REFERENCE_EVIDENCE": str(artifact_dir / "private"),
    }
    command = [sys.executable, "-m", "pytest", "tests/test_flagship_ag_grid_reference.py",
               "-m", "flagship", "-q", "-o", "junit_family=xunit1", f"--junitxml={junit_path}"]
    return subprocess.run(command, cwd=ROOT, env=child_environment).returncode


def _reference_junit_issues(path: Path, expected_ids: set[str]) -> list[str]:
    """A green subset, duplicate identity, or unlabelled case cannot qualify."""
    from scripts.flagship_manifest import apply_junit_result

    report = {"systemIssues": {"ag_grid": []}}
    apply_junit_result(report, path, system="ag_grid")
    issues = report["systemIssues"]["ag_grid"]
    try:
        cases = list(ET.parse(path).getroot().iter("testcase"))
        identities = [case.findall("./properties/property[@name='ag_grid_test_id']") for case in cases]
        ids = [properties[0].get("value") for properties in identities if len(properties) == 1]
        if (not expected_ids or len(ids) != len(cases) or len(ids) != len(expected_ids)
                or set(ids) != expected_ids):
            issues.append("junit_invalid")
    except (OSError, ET.ParseError):
        pass  # apply_junit_result already records the missing/malformed report.
    return sorted(set(issues))


def _run_qualification_phase(
    phase: str,
    artifact_dir: Path,
    environment: dict[str, str],
) -> int:
    phase_dir = artifact_dir / phase
    phase_dir.mkdir(parents=True, exist_ok=True)
    junit_path = phase_dir / "junit.xml"
    raw_handle, raw_name = tempfile.mkstemp(prefix=f"pivot-{phase}-", suffix=".xml")
    os.close(raw_handle)
    raw_junit_path = Path(raw_name)
    raw_junit_path.unlink(missing_ok=True)
    selection = (
        "flagship_qualified_download or flagship_qualified_clipboard"
        if phase == "normal"
        else "flagship_export_limit_rejection_creates_no_file"
    )

    def run_browser_suite() -> int:
        return _run_browser_suite(
            phase_dir,
            environment,
            junit_path=raw_junit_path,
            selection=selection,
        )

    try:
        return _run_with_hosts(phase_dir, environment, run_browser_suite)
    finally:
        if raw_junit_path.is_file():
            _sanitize_junit_report(raw_junit_path, junit_path)
        raw_junit_path.unlink(missing_ok=True)


def _sanitize_junit_report(source: Path, destination: Path) -> None:
    """Publish result counts without test names, captured output, or exception text."""
    try:
        source_root = ET.parse(source).getroot()
    except (ET.ParseError, OSError):
        return
    source_cases = list(source_root.iter("testcase"))
    suite = ET.Element("testsuite")
    for index, source_case in enumerate(source_cases):
        case = ET.SubElement(suite, "testcase", classname="export-qualification", name=f"case-{index + 1}")
        for outcome in ("skipped", "failure", "error"):
            if source_case.find(outcome) is not None:
                ET.SubElement(case, outcome)
                break
    suite.set("tests", str(len(source_cases)))
    suite.set("skipped", str(sum(case.find("skipped") is not None for case in source_cases)))
    suite.set("failures", str(sum(case.find("failure") is not None for case in source_cases)))
    suite.set("errors", str(sum(case.find("error") is not None for case in source_cases)))
    destination.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(destination, encoding="utf-8", xml_declaration=True)


def _junit_summary(path: Path) -> dict[str, int]:
    summary = {"tests": 0, "skipped": 0, "failures": 0, "errors": 0}
    if not path.is_file():
        return summary
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError):
        return summary
    cases = list(root.iter("testcase"))
    summary["tests"] = len(cases)
    summary["skipped"] = sum(case.find("skipped") is not None for case in cases)
    summary["failures"] = sum(case.find("failure") is not None for case in cases)
    summary["errors"] = sum(case.find("error") is not None for case in cases)
    return summary


def _relative_evidence_path(path: Path, artifact_dir: Path) -> str:
    return path.relative_to(artifact_dir).as_posix()


def _validate_host_evidence(
    phase: str,
    host: str,
    artifact_dir: Path,
) -> tuple[dict[str, object], list[str]]:
    host_dir = artifact_dir / phase / host
    downloads = host_dir / "downloads"
    evidence: list[Path] = []
    issues: list[str] = []

    if phase == "normal":
        for export_format in ("csv", "xlsx"):
            required = [
                host_dir / f"qualified-{export_format}.download.json",
                host_dir / f"qualified-{export_format}.parsed.json",
                host_dir / f"qualified-{export_format}.png",
                host_dir / f"qualified-{export_format}.browser.json",
                host_dir / f"qualified-{export_format}.network.json",
            ]
            for path in required:
                if path.is_file():
                    evidence.append(path)
                else:
                    issues.append(f"missing_{path.name}")
        for representation in ("tsv", "html"):
            for suffix in ("parsed.json", "png", "browser.json", "network.json"):
                path = host_dir / f"qualified-clipboard-{representation}.{suffix}"
                if path.is_file():
                    evidence.append(path)
                else:
                    issues.append(f"missing_{path.name}")
        if not downloads.is_dir():
            issues.append("missing_download_directory")
        elif any(path.is_file() for path in downloads.iterdir()):
            issues.append("raw_download_present")
    else:
        for export_format in ("csv", "xlsx"):
            for suffix in ("parsed.json", "png", "browser.json", "network.json"):
                path = host_dir / f"qualified-limit-{export_format}.{suffix}"
                if path.is_file():
                    evidence.append(path)
                else:
                    issues.append(f"missing_{path.name}")
            proof_path = host_dir / f"qualified-limit-{export_format}.parsed.json"
            if proof_path.is_file() and not _valid_limit_proof(proof_path, export_format):
                issues.append(f"invalid_limit_proof_{export_format}")
        if not downloads.is_dir():
            issues.append("missing_download_directory")
        elif any(path.is_file() for path in downloads.iterdir()):
            issues.append("limit_created_download")

    report = {
        "result": "passed" if not issues else "failed",
        "evidence": [_relative_evidence_path(path, artifact_dir) for path in evidence],
        "issues": issues,
    }
    return report, issues


def _valid_limit_proof(path: Path, export_format: str) -> bool:
    try:
        proof = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return False
    return _valid_limit_proof_value(proof, export_format)


def _valid_limit_proof_value(proof: object, export_format: object) -> bool:
    return (
        isinstance(proof, dict)
        and export_format in {"csv", "xlsx"}
        and proof.get("format") == export_format
        and proof.get("limitRejected") is True
        and proof.get("schemaVersion") == 1
        and proof.get("kind") == "export"
        and proof.get("runtimeStatus") == "error"
        and isinstance(proof.get("requestId"), str)
        and bool(proof.get("requestId"))
        and proof.get("requestId") == proof.get("clickedRequestId")
        and proof.get("limitCode") in SAFE_LIMIT_CODES
        and proof.get("stableError") == "Export limit exceeded"
        and proof.get("newCompletedFiles") == 0
        and proof.get("newPartialFiles") == 0
    )


def _validate_qualification_phase(
    phase: str,
    artifact_dir: Path,
    browser_exit_code: int,
) -> dict[str, object]:
    junit_path = artifact_dir / phase / "junit.xml"
    junit = _junit_summary(junit_path)
    issues: list[str] = []
    if browser_exit_code != 0:
        issues.append("browser_exit_nonzero")
    if not junit_path.is_file():
        issues.append("junit_missing")
    if junit["tests"] != QUALIFICATION_TEST_COUNTS[phase]:
        issues.append("junit_case_count")
    for field in ("skipped", "failures", "errors"):
        if junit[field]:
            issues.append(f"junit_{field}")

    hosts: dict[str, object] = {}
    for host in QUALIFICATION_HOSTS:
        host_report, host_issues = _validate_host_evidence(phase, host, artifact_dir)
        hosts[host] = host_report
        if host_issues:
            issues.append(f"host_{host}_evidence")

    return {
        "result": "passed" if not issues else "failed",
        "browserExitCode": browser_exit_code,
        "junit": _relative_evidence_path(junit_path, artifact_dir),
        "tests": junit,
        "hosts": hosts,
        "issues": issues,
    }


def _qualification_environment(environment: dict[str, str], phase: str) -> dict[str, str]:
    phase_environment = {
        **environment,
        "PIVOT_RUN_EXPORT_QUALIFICATION": "1",
        "PIVOT_REQUIRE_BROWSER_EVIDENCE": "1",
        "PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA": "1",
        "PIVOT_EXPORT_QUALIFICATION_PHASE": phase,
    }
    phase_environment.pop("PIVOT_EXPORT_MAX_ROWS", None)
    phase_environment.pop("PIVOT_EXPORT_MAX_BYTES", None)
    if phase == "constrained":
        phase_environment["PIVOT_EXPORT_MAX_ROWS"] = "1"
        phase_environment["PIVOT_EXPORT_MAX_BYTES"] = "64"
    return phase_environment


def _validate_publication_target(artifact_dir: Path) -> Path:
    """Accept only a new directory whose ancestors are real directories."""
    target = artifact_dir.absolute()
    if target.exists() or target.is_symlink():
        raise ValueError(f"Publication target must not already exist: {target}")
    if not target.parent.is_dir():
        raise ValueError(f"Publication target parent must already exist: {target.parent}")
    for parent in target.parents:
        if parent.is_symlink():
            raise ValueError(f"Publication target cannot traverse a symlink: {parent}")
    resolved_parent = target.parent.resolve(strict=True)
    resolved = resolved_parent / target.name
    package = PACKAGE.resolve()
    git_dir = (ROOT / ".git").resolve()
    if resolved.is_relative_to(package) or resolved.is_relative_to(git_dir):
        raise ValueError(f"Unsafe export qualification publication target: {resolved}")
    return resolved


def _prepare_export_artifact_dir(artifact_dir: Path) -> Path:
    resolved = _validate_publication_target(artifact_dir)
    resolved.mkdir()
    (resolved / EXPORT_PUBLICATION_MARKER).write_text(
        "owned export qualification publication directory\n", encoding="utf-8"
    )
    return resolved


def _non_negative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _safe_count_map(value: object) -> bool:
    return (
        isinstance(value, dict)
        and all(
            isinstance(key, str)
            and re.fullmatch(r"[A-Za-z0-9_.:/ -]{1,80}", key) is not None
            and _non_negative_int(count)
            for key, count in value.items()
        )
    )


def _load_safe_summary(path: Path) -> dict[str, object] | None:
    """Reconstruct a publication summary from one explicitly known raw schema."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return None
    if not isinstance(value, dict):
        return None

    name = path.name
    if name.endswith(".browser.json"):
        expected = {"entryCount", "levelCounts", "sourceCounts", "relevantErrorCount"}
        if (
            set(value) != expected
            or not _non_negative_int(value["entryCount"])
            or not _non_negative_int(value["relevantErrorCount"])
            or not _safe_count_map(value["levelCounts"])
            or not _safe_count_map(value["sourceCounts"])
        ):
            return None
        return {
            "entryCount": value["entryCount"],
            "relevantErrorCount": value["relevantErrorCount"],
        }

    if name.endswith(".network.json"):
        expected = {"entryCount", "methodCounts", "statusCounts", "resourceTypeCounts"}
        if (
            set(value) != expected
            or not _non_negative_int(value["entryCount"])
            or not all(
                _safe_count_map(value[field])
                for field in ("methodCounts", "statusCounts", "resourceTypeCounts")
            )
        ):
            return None
        return {"entryCount": value["entryCount"]}

    if name.endswith(".download.json"):
        expected = {"format", "byteLength", "documentValidated"}
        if (
            set(value) != expected
            or value["format"] not in {"csv", "xlsx"}
            or not _non_negative_int(value["byteLength"])
            or value["documentValidated"] is not True
        ):
            return None
        return {key: value[key] for key in ("format", "byteLength", "documentValidated")}

    if name.startswith("qualified-clipboard-") and name.endswith(".parsed.json"):
        if set(value) != {"currentTable", "allRecords"}:
            return None
        safe: dict[str, object] = {}
        for key, scope in (("currentTable", "current-table"), ("allRecords", "all-records")):
            item = value[key]
            expected = {
                "representation", "scope", "rows", "columns", "unicodeVerified",
                "filterVerified", "columnOrderVerified",
            }
            if (
                not isinstance(item, dict)
                or set(item) != expected
                or item["representation"] not in {"tsv", "html"}
                or item["scope"] != scope
                or not _non_negative_int(item["rows"])
                or not _non_negative_int(item["columns"])
                or any(item[field] is not True for field in (
                    "unicodeVerified", "filterVerified", "columnOrderVerified",
                ))
            ):
                return None
            safe[key] = {field: item[field] for field in sorted(expected)}
        return safe

    if name.startswith("qualified-limit-") and name.endswith(".parsed.json"):
        if not _valid_limit_proof_value(value, value.get("format")):
            return None
        keys = (
            "format", "limitRejected", "schemaVersion", "kind", "runtimeStatus",
            "requestId", "clickedRequestId", "limitCode", "stableError",
            "newCompletedFiles", "newPartialFiles",
        )
        if set(value) != set(keys):
            return None
        return {key: value[key] for key in keys}

    if name.startswith("qualified-") and name.endswith(".parsed.json"):
        expected = {
            "format", "rows", "columns", "numericCellCount", "mergedHeaderCount",
            "unicodeVerified", "filterVerified", "columnOrderVerified", "totalsVerified",
        }
        if (
            set(value) != expected
            or value["format"] not in {"csv", "xlsx"}
            or any(not _non_negative_int(value[field]) for field in (
                "rows", "columns", "numericCellCount", "mergedHeaderCount",
            ))
            or any(value[field] is not True for field in (
                "unicodeVerified", "filterVerified", "columnOrderVerified", "totalsVerified",
            ))
        ):
            return None
        return {field: value[field] for field in sorted(expected)}
    return None


def _raw_expected_files() -> tuple[set[str], set[str]]:
    expected: set[str] = set()
    summaries: set[str] = set()
    for phase in QUALIFICATION_PHASES:
        expected.add(f"{phase}/junit.xml")
        for process in ("runtime", "dash", "react", "streamlit"):
            expected.add(f"{phase}/{process}.process.json")
        for host in QUALIFICATION_HOSTS:
            prefix = f"{phase}/{host}"
            if phase == "normal":
                for export_format in ("csv", "xlsx"):
                    for suffix in ("download.json", "parsed.json", "png", "browser.json", "network.json"):
                        relative = f"{prefix}/qualified-{export_format}.{suffix}"
                        expected.add(relative)
                        if suffix != "png":
                            summaries.add(relative)
                for representation in ("tsv", "html"):
                    for suffix in ("parsed.json", "png", "browser.json", "network.json"):
                        relative = f"{prefix}/qualified-clipboard-{representation}.{suffix}"
                        expected.add(relative)
                        if suffix != "png":
                            summaries.add(relative)
            else:
                for export_format in ("csv", "xlsx"):
                    for suffix in ("parsed.json", "png", "browser.json", "network.json"):
                        relative = f"{prefix}/qualified-limit-{export_format}.{suffix}"
                        expected.add(relative)
                        if suffix != "png":
                            summaries.add(relative)
    return expected, summaries


def _write_public_junit(destination: Path, tests: dict[str, int]) -> None:
    suite = ET.Element("testsuite")
    outcomes = (
        ["failure"] * tests["failures"]
        + ["error"] * tests["errors"]
        + ["skipped"] * tests["skipped"]
    )
    for index in range(tests["tests"]):
        case = ET.SubElement(
            suite, "testcase", classname="export-qualification", name=f"case-{index + 1}"
        )
        if index < len(outcomes):
            ET.SubElement(case, outcomes[index])
    for key in ("tests", "skipped", "failures", "errors"):
        suite.set(key, str(tests[key]))
    destination.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(destination, encoding="utf-8", xml_declaration=True)


def _publish_safe_evidence(
    raw_dir: Path,
    publication_dir: Path,
    phases: dict[str, object],
) -> tuple[set[str], dict[str, set[str]]]:
    """Build a new publication from an explicit summary-only allowlist."""
    expected, summary_paths = _raw_expected_files()
    issues = {phase: set() for phase in QUALIFICATION_PHASES}
    for path in raw_dir.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(raw_dir).as_posix()
        if relative not in expected:
            phase = relative.split("/", 1)[0]
            issues[phase if phase in issues else "normal"].add("unexpected_raw_artifact")

    published: set[str] = set()
    for relative in sorted(summary_paths):
        source = raw_dir / relative
        if not source.is_file():
            continue
        safe = _load_safe_summary(source)
        phase = relative.split("/", 1)[0]
        if safe is None:
            issues[phase].add("invalid_safe_summary")
            continue
        destination = publication_dir / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(safe, indent=2) + "\n", encoding="utf-8")
        published.add(relative)

    for phase in QUALIFICATION_PHASES:
        report = phases.get(phase)
        if not isinstance(report, dict):
            continue
        junit = report.get("tests")
        if (
            isinstance(junit, dict)
            and set(junit) == {"tests", "skipped", "failures", "errors"}
            and all(_non_negative_int(junit[key]) for key in junit)
        ):
            relative = f"{phase}/junit.xml"
            _write_public_junit(publication_dir / relative, junit)
            published.add(relative)
    return published, issues


def _failed_qualification_phases(issue: str) -> dict[str, object]:
    return {
        phase: {
            "result": "failed",
            "browserExitCode": None,
            "junit": f"{phase}/junit.xml",
            "tests": {"tests": 0, "skipped": 0, "failures": 0, "errors": 0},
            "hosts": {},
            "issues": [issue],
        }
        for phase in QUALIFICATION_PHASES
    }


def _safe_issue_code(issue: object) -> str:
    exact = {
        "browser_exit_nonzero", "junit_missing", "junit_case_count", "junit_skipped",
        "junit_failures", "junit_errors", "raw_download_present", "limit_created_download",
        "missing_download_directory", "build_failed", "reuse_running_not_supported",
        "qualification_interrupted", "unexpected_raw_artifact", "invalid_safe_summary",
    }
    if isinstance(issue, str) and issue in exact:
        return issue
    if isinstance(issue, str) and re.fullmatch(
        r"host_(dash|react|streamlit)_evidence|invalid_limit_proof_(csv|xlsx)", issue
    ):
        return issue
    expected, _ = _raw_expected_files()
    allowed_names = {Path(relative).name for relative in expected}
    if isinstance(issue, str) and issue.startswith("missing_") and issue[8:] in allowed_names:
        return issue
    return "qualification_issue"


def _publication_phase_reports(
    phases: dict[str, object], published: set[str]
) -> dict[str, object]:
    safe_phases: dict[str, object] = {}
    for phase in QUALIFICATION_PHASES:
        source = phases.get(phase, {})
        if not isinstance(source, dict):
            source = {}
        raw_tests = source.get("tests", {})
        tests = {
            key: raw_tests.get(key, 0) if isinstance(raw_tests, dict) and _non_negative_int(raw_tests.get(key, 0)) else 0
            for key in ("tests", "skipped", "failures", "errors")
        }
        hosts: dict[str, object] = {}
        raw_hosts = source.get("hosts", {})
        for host in QUALIFICATION_HOSTS:
            raw_host = raw_hosts.get(host, {}) if isinstance(raw_hosts, dict) else {}
            evidence = raw_host.get("evidence", []) if isinstance(raw_host, dict) else []
            host_issues = raw_host.get("issues", []) if isinstance(raw_host, dict) else []
            hosts[host] = {
                "result": "passed" if isinstance(raw_host, dict) and raw_host.get("result") == "passed" else "failed",
                "evidence": [
                    item for item in evidence
                    if isinstance(item, str) and item in published
                ],
                "issues": sorted({_safe_issue_code(item) for item in host_issues}),
            }
        issues = source.get("issues", [])
        safe_phases[phase] = {
            "result": "passed" if source.get("result") == "passed" else "failed",
            "browserExitCode": source.get("browserExitCode")
            if isinstance(source.get("browserExitCode"), int) else None,
            "junit": f"{phase}/junit.xml" if f"{phase}/junit.xml" in published else None,
            "tests": tests,
            "hosts": hosts,
            "issues": sorted({_safe_issue_code(item) for item in issues}),
        }
    return safe_phases


def _write_export_qualification_manifest(
    artifact_dir: Path,
    *,
    result: str,
    started_at: datetime,
    phases: dict[str, object],
    build_status: str,
) -> None:
    manifest = {
        "schemaVersion": 1,
        "startedAt": started_at.isoformat(),
        "completedAt": datetime.now(UTC).isoformat(),
        "result": result,
        "build": build_status,
        "phases": phases,
    }
    (artifact_dir / "export-qualification.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


def _certify_export_qualification(
    artifact_dir: Path,
    environment: dict[str, str],
    *,
    skip_build: bool,
    reuse_running: bool,
    wheel: Path | None,
    started_at: datetime,
) -> int:
    phases: dict[str, object] = {}
    build_failed = False
    interrupted = False
    with tempfile.TemporaryDirectory(
        prefix="pivot-export-qualification-raw-", ignore_cleanup_errors=True
    ) as raw_name:
        raw_dir = Path(raw_name)
        try:
            if reuse_running:
                phases = _failed_qualification_phases("reuse_running_not_supported")
                build_failed = True
            else:
                try:
                    if not skip_build:
                        _build()
                    elif wheel is None:
                        raise FileNotFoundError("--skip-build requires --wheel")
                    else:
                        _install_package(wheel)
                except Exception:
                    build_failed = True

            if not build_failed:
                for phase in QUALIFICATION_PHASES:
                    phase_environment = _qualification_environment(environment, phase)
                    try:
                        code = _run_qualification_phase(
                            phase,
                            raw_dir,
                            phase_environment,
                        )
                    except Exception:
                        code = 1
                    phases[phase] = _validate_qualification_phase(phase, raw_dir, code)
            elif not phases:
                phases = _failed_qualification_phases("build_failed")
        except KeyboardInterrupt:
            interrupted = True
            phases = _failed_qualification_phases("qualification_interrupted")
        finally:
            published, publication_issues = _publish_safe_evidence(
                raw_dir, artifact_dir, phases
            )
            for phase, issue_codes in publication_issues.items():
                if not issue_codes:
                    continue
                report = phases.setdefault(
                    phase, _failed_qualification_phases("qualification_issue")[phase]
                )
                report["issues"].extend(sorted(issue_codes))
                report["issues"] = sorted(set(report["issues"]))
                report["result"] = "failed"

            passed = not build_failed and not interrupted and all(
                isinstance(phase_report, dict) and phase_report.get("result") == "passed"
                for phase_report in phases.values()
            )
            result = "passed" if passed else "failed"
            public_phases = _publication_phase_reports(phases, published)
            _write_export_qualification_manifest(
                artifact_dir,
                result=result,
                started_at=started_at,
                phases=public_phases,
                build_status="failed" if build_failed else "skipped" if skip_build else "passed",
            )
            _write_matrix(
                artifact_dir,
                result=result,
                started_at=started_at,
                completed_at=datetime.now(UTC),
            )
    if interrupted:
        raise KeyboardInterrupt
    return 0 if passed else 1


INSTALLED_BROWSER_CASES = (
    "test_flagship_startup_navigation_and_controls",
    "test_flagship_copy_current_table_all_records_and_export_xlsx",
    "test_flagship_sidebar_filter_report_and_chart_entrypoints",
)


def _installed_browser_report_passed(path: Path, *, export_jobs: bool = False) -> bool:
    try:
        report = ET.parse(path).getroot()
    except (OSError, ET.ParseError):
        return False
    cases = report.findall(".//testcase")
    expected = ({f"test_flagship_export_jobs[{requirement}-{host}]"
                 for requirement in ("large-delivery.progress", "large-delivery.cancel-retry", "journey-recovery.cancel-new-work")
                 for host in QUALIFICATION_HOSTS} if export_jobs else
                {f"{name}[{host}]" for name in INSTALLED_BROWSER_CASES for host in QUALIFICATION_HOSTS})
    names = [case.get("name") for case in cases]
    try:
        return (
            len(names) == len(expected) and set(names) == expected
            and not any(case.find(tag) is not None for case in cases for tag in ("skipped", "failure", "error"))
            and not any(int(suite.get(tag, "0")) for suite in report.iter("testsuite") for tag in ("skipped", "failures", "errors"))
        )
    except ValueError:
        return False


def _certify_installed_release(artifact_dir: Path, started_at: datetime, *, build_wheelhouse: Path | None = None,
                               export_jobs: bool = False) -> int:
    from scripts.flagship_artifacts import ArtifactBinding, build_artifacts, start_installed_hosts, cleanup_python_environment
    from scripts.flagship_manifest import (
        QualificationBinding, apply_junit_result, evaluate_manifest,
        load_case_envelope, publish_safe_manifest, read_committed_contract, load_committed_requirements,
    )
    from scripts.flagship_reference_runner import start_reference_server
    from tests.flagship_reference_support import load_case_results, write_reference_case_envelope

    result = "failed"
    code = 1
    completed = False
    # A new input location prevents a partial rerun inheriting case files from
    # a retained prior invocation, even when its artifact directory is reused.
    run_id = uuid4().hex
    private = artifact_dir / "private" / "runs" / run_id
    private.mkdir(parents=True)
    envelopes = {system: private / f"{system}-case-results.json" for system in ("product", "ag_grid")}
    binding = QualificationBinding(run_id=run_id)
    junit_path = private / "installed-browser.junit.xml"
    reference_junit_path = private / "ag-grid-reference.junit.xml"
    reference_issues: list[str] = []
    try:
        build_options = {"build_wheelhouse": build_wheelhouse} if build_wheelhouse is not None else {}
        artifacts = build_artifacts(ROOT, artifact_dir / "release-artifacts", **build_options)
        values = {key: getattr(artifacts, key) for key in ArtifactBinding.__dataclass_fields__}
        values["sha256"] = dict(artifacts.sha256)
        values["paths"] = {"source_repository": ROOT, **artifacts.paths}
        binding = QualificationBinding(**values, run_id=run_id)
        try:
            contract_bytes = read_committed_contract(binding)
            binding.sha256["contract"] = hashlib.sha256(contract_bytes).hexdigest()
            (private / "source-contract.json").write_bytes(contract_bytes)
        except (OSError, ValueError, subprocess.CalledProcessError):
            # A missing source object cannot qualify; retain the browser exit
            # code and let the manifest report the invalid contract binding.
            pass
        binding_path = artifact_dir / "artifact-binding.json"
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        environment["PIVOT_FLAGSHIP_ARTIFACT_BINDING"] = str(binding_path)
        environment["PIVOT_REQUIRE_BROWSER_EVIDENCE"] = "1"
        environment["PIVOT_FLAGSHIP_RUN_ID"] = run_id
        environment["PIVOT_FLAGSHIP_CASE_RESULTS"] = str(envelopes["product"])
        environment["PIVOT_FLAGSHIP_REFERENCE_CASE_RESULTS"] = str(envelopes["ag_grid"])
        environment["PIVOT_FLAGSHIP_EVIDENCE_ROOT"] = str(private)
        if export_jobs:
            environment["PIVOT_FLAGSHIP_EXPORT_JOB_DATA"] = "1"
        options = {"environment": environment} if export_jobs else {}
        with start_installed_hosts(artifacts, artifact_dir / "installed-hosts", **options) as urls:
            binding.host_origins.update(urls)
            binding_path.write_text(json.dumps({
                "runId": run_id,
                "sourceCommit": binding.source_commit,
                "sha256": binding.sha256,
                "versions": binding.versions,
                "expectedLoadedAssets": binding.expected_loaded_assets,
                "hostOrigins": binding.host_origins,
            }, indent=2) + "\n", encoding="utf-8")
            for host, url in urls.items():
                environment[f"PIVOT_FLAGSHIP_{host.upper()}_URL"] = url
            code = _run_browser_suite(
                artifact_dir, environment, junit_path=junit_path,
                selection="test_flagship_export_jobs" if export_jobs else " or ".join(INSTALLED_BROWSER_CASES),
            )
            if code == 0 and not _installed_browser_report_passed(junit_path, export_jobs=export_jobs):
                code = 1
        if code == 0:
            cleanup_python_environment(artifacts)
        try:
            reference_requirements = load_committed_requirements(binding)
            expected_reference = {(item.id, item.ag_grid["test_id"]) for item in reference_requirements
                                  if item.ag_grid.get("result") != "not_applicable"}
            if hashlib.sha256((ROOT / "tests/data/flagship_parity_rows.json").read_bytes()).hexdigest() != binding.sha256.get("fixture"):
                raise ValueError("reference fixture differs from the built artifact fixture")
            with start_reference_server(ROOT, private) as reference:
                reference_environment = {
                    **environment,
                    "AG_GRID_REFERENCE_LIVE": "1",
                    "AG_GRID_REFERENCE_URL": reference.url,
                    "AG_GRID_REFERENCE_RUN_ID": run_id,
                    "AG_GRID_REFERENCE_EVIDENCE": str(artifact_dir / "private"),
                }
                reference_code = _run_reference_suite(artifact_dir, reference_environment, reference_junit_path)
            reference_issues.extend(_reference_junit_issues(reference_junit_path, {test for _, test in expected_reference}))
            if reference_code != 0:
                reference_issues.append("browser_exit_nonzero")
            if not set(reference_issues) & {"junit_missing", "junit_invalid", "junit_empty"}:
                raw_cases = load_case_results(artifact_dir / "private", run_id)
                actual_reference = [(case.requirement_id, case.test_id) for case in raw_cases]
                if len(actual_reference) != len(expected_reference) or set(actual_reference) != expected_reference:
                    raise ValueError("reference observations do not cover the exact committed map")
                write_reference_case_envelope(artifact_dir / "private", envelopes["ag_grid"], binding)
        except Exception as error:
            reference_issues.append("qualification_incomplete")
            (private / "reference-error.json").write_text(json.dumps({
                "type": type(error).__name__, "reason": str(error),
            }, indent=2) + "\n", encoding="utf-8")
        completed = True
    finally:
        try:
            requirements = load_committed_requirements(binding)
        except (OSError, ValueError, TypeError, subprocess.CalledProcessError):
            requirements = ()
        cases = []
        envelope_issues = []
        for system, path in envelopes.items():
            try:
                loaded = load_case_envelope(path, binding)
                if any(case.system != system for case in loaded):
                    raise ValueError("case envelope system does not match its input")
                cases.extend(loaded)
            except (OSError, ValueError, TypeError):
                envelope_issues.append(system)
        report = evaluate_manifest(requirements, binding, cases)
        for system in envelope_issues:
            report["systemIssues"][system].append("case_envelope_invalid")
            report["parityResult"] = "failed"
            if system == "product":
                report["releaseResult"] = "failed"
        apply_junit_result(report, junit_path, system="product")
        apply_junit_result(report, reference_junit_path, system="ag_grid")
        if reference_issues:
            report["systemIssues"]["ag_grid"] = sorted(set(report["systemIssues"]["ag_grid"]) | set(reference_issues))
            report["parityResult"] = "failed"
        if code != 0 or not completed:
            report["issues"].append("browser_exit_nonzero" if completed else "qualification_incomplete")
            report["releaseResult"] = report["parityResult"] = "failed"
        # Raw reasons and evidence paths are retained only in the private tree.
        (private / "read-only-qualification.raw.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        publish_safe_manifest(report, artifact_dir / "read-only-qualification.json")
        result = "passed" if report["releaseResult"] == report["parityResult"] == "passed" else "failed"
        _write_matrix(artifact_dir, result=result, started_at=started_at, completed_at=datetime.now(UTC))
    return code if code != 0 else 0 if result == "passed" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument("--reuse-running", action="store_true", help="Use already-running local flagship services.")
    parser.add_argument("--skip-build", action="store_true", help="Do not rebuild bundles before certification.")
    parser.add_argument("--wheel", type=Path, help="Prebuilt wheel to reinstall when --skip-build is used.")
    parser.add_argument("--installed-release", action="store_true", help="Build fresh committed wheel/npm artifacts and run browsers against their installed hosts; retain private evidence.")
    parser.add_argument("--cancellable-exports", action="store_true", help="Installed-release only: qualify the three export job journeys per host against the supported flat fixture. The complete release/parity gate remains fail-closed.")
    parser.add_argument("--build-wheelhouse", type=Path, help="Installed-release only: build dependencies from hash-pinned wheels plus wheelhouse-sha256.json; isolation stays enabled, runtime installation is unchanged.")
    parser.add_argument(
        "--export-qualification",
        action="store_true",
        help="Require normal and constrained export evidence from every flagship host.",
    )
    args = parser.parse_args(argv)

    if args.cancellable_exports and not args.installed_release:
        print("Cancellable export qualification requires --installed-release", file=sys.stderr)
        return 1

    if args.build_wheelhouse is not None and not args.installed_release:
        print("Build wheelhouse requires --installed-release", file=sys.stderr)
        return 1

    if args.installed_release and (args.reuse_running or args.skip_build or args.export_qualification or args.wheel):
        print("Installed release requires a fresh build and cannot combine with legacy/export flags", file=sys.stderr)
        return 1

    default_artifact_dir = (
        EXPORT_QUALIFICATION_ARTIFACT_DIR
        if args.export_qualification
        else DEFAULT_ARTIFACT_DIR
    )
    artifact_dir = (args.artifact_dir or default_artifact_dir).absolute()
    if args.export_qualification:
        if args.reuse_running:
            print("Export qualification does not support --reuse-running", file=sys.stderr)
            return 1
        if args.skip_build:
            if args.wheel is None:
                print("Export qualification --skip-build requires --wheel", file=sys.stderr)
                return 1
            wheel = args.wheel.resolve()
            if not wheel.is_file() or wheel.suffix.lower() != ".whl":
                print(f"Prebuilt wheel is required: {wheel}", file=sys.stderr)
                return 1
        try:
            artifact_dir = _prepare_export_artifact_dir(artifact_dir)
        except ValueError as error:
            print(str(error), file=sys.stderr)
            return 1
    else:
        artifact_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    if args.installed_release:
        options = {"export_jobs": True} if args.cancellable_exports else {}
        return _certify_installed_release(artifact_dir, started_at, build_wheelhouse=args.build_wheelhouse, **options)
    if not args.export_qualification:
        print("Legacy diagnostic mode: results are not current installed-release qualification", file=sys.stderr)
    environment = os.environ.copy()
    python_paths = [str(ROOT), str(PACKAGE), str(PACKAGE / "pivot_engine")]
    environment["PYTHONPATH"] = os.pathsep.join(python_paths + [environment.get("PYTHONPATH", "")])

    if args.export_qualification:
        return _certify_export_qualification(
            artifact_dir,
            environment,
            skip_build=args.skip_build,
            reuse_running=args.reuse_running,
            wheel=args.wheel.resolve() if args.wheel else None,
            started_at=started_at,
        )

    result = "failed"
    try:
        if not args.skip_build:
            _build()
        if args.reuse_running:
            code = _run_browser_suite(artifact_dir, environment)
        else:
            code = _run_with_hosts(
                artifact_dir,
                environment,
                lambda: _run_browser_suite(artifact_dir, environment),
            )
        result = "passed" if code == 0 else "failed"
        return code
    finally:
        _write_matrix(artifact_dir, result=result, started_at=started_at, completed_at=datetime.now(UTC))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
