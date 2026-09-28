"""Build and certify the published Dash, React, and Streamlit showcases.

This is intentionally a release command rather than a developer convenience:
it exercises the built workspace bundles in browsers and leaves auditable
screenshots, browser logs, network traces, clipboard/download checks, and the
feature matrix in one artifact directory.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Iterator


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "dash_tanstack_pivot"
DEFAULT_ARTIFACT_DIR = ROOT / "artifacts" / "flagship-certification"
EXPORT_QUALIFICATION_ARTIFACT_DIR = ROOT / "artifacts" / "export-qualification"
QUALIFICATION_HOSTS = ("dash", "react", "streamlit")
QUALIFICATION_PHASES = ("normal", "constrained")
QUALIFICATION_TEST_COUNTS = {"normal": 12, "constrained": 6}

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
    stdout = (artifact_dir / f"{name}.stdout.log").open("w", encoding="utf-8")
    stderr = (artifact_dir / f"{name}.stderr.log").open("w", encoding="utf-8")
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


def _install_package() -> None:
    # Streamlit loads its frontend from the installed Python distribution.
    # Reinstall even when builds are skipped so the browser suite never uses a
    # stale manifest or hashed asset left behind by an earlier local install.
    _run([
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-deps",
        "--force-reinstall",
        "--no-build-isolation",
        ".",
    ], cwd=PACKAGE)


def _build() -> None:
    npm = shutil.which("npm") or "npm"
    _run([npm, "run", "build:js"], cwd=PACKAGE)
    _run([npm, "run", "build:react"], cwd=PACKAGE)
    _run([npm, "run", "build:streamlit"], cwd=PACKAGE)
    _install_package()


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
    command = [sys.executable, "-m", "pytest", "tests/test_flagship_certification.py", "-m", "flagship", "-q"]
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


def _run_qualification_phase(
    phase: str,
    artifact_dir: Path,
    environment: dict[str, str],
    reuse_running: bool,
) -> int:
    phase_dir = artifact_dir / phase
    phase_dir.mkdir(parents=True, exist_ok=True)
    junit_path = phase_dir / "junit.xml"
    selection = (
        "flagship_qualified_download or flagship_qualified_clipboard"
        if phase == "normal"
        else "flagship_export_limit_rejection_creates_no_file"
    )

    def run_browser_suite() -> int:
        return _run_browser_suite(
            phase_dir,
            environment,
            junit_path=junit_path,
            selection=selection,
        )

    if reuse_running:
        return run_browser_suite()
    return _run_with_hosts(phase_dir, environment, run_browser_suite)


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
                host_dir / f"qualified-{export_format}.parsed.json",
                host_dir / f"qualified-{export_format}.png",
                host_dir / f"qualified-{export_format}.browser.json",
                host_dir / f"qualified-{export_format}.network.json",
            ]
            completed = sorted(downloads.glob(f"*.{export_format}")) if downloads.is_dir() else []
            if not completed:
                issues.append(f"missing_{export_format}_download")
            else:
                evidence.append(completed[-1])
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
        if downloads.is_dir() and any(
            path.is_file() and path.suffix.lower() in {".crdownload", ".part", ".tmp"}
            for path in downloads.iterdir()
        ):
            issues.append("partial_download_present")
    else:
        for export_format in ("csv", "xlsx"):
            for suffix in ("parsed.json", "png", "browser.json", "network.json"):
                path = host_dir / f"qualified-limit-{export_format}.{suffix}"
                if path.is_file():
                    evidence.append(path)
                else:
                    issues.append(f"missing_{path.name}")
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
    started_at: datetime,
) -> int:
    phases: dict[str, object] = {}
    build_failed = False
    try:
        if not skip_build:
            _build()
        else:
            _install_package()
    except Exception:
        build_failed = True

    if not build_failed:
        for phase in QUALIFICATION_PHASES:
            phase_environment = _qualification_environment(environment, phase)
            try:
                code = _run_qualification_phase(
                    phase,
                    artifact_dir,
                    phase_environment,
                    reuse_running,
                )
            except Exception:
                code = 1
            phases[phase] = _validate_qualification_phase(phase, artifact_dir, code)
    else:
        for phase in QUALIFICATION_PHASES:
            phases[phase] = {
                "result": "failed",
                "browserExitCode": None,
                "junit": f"{phase}/junit.xml",
                "tests": {"tests": 0, "skipped": 0, "failures": 0, "errors": 0},
                "hosts": {},
                "issues": ["build_failed"],
            }

    passed = not build_failed and all(
        phase_report.get("result") == "passed" for phase_report in phases.values()
    )
    result = "passed" if passed else "failed"
    _write_export_qualification_manifest(
        artifact_dir,
        result=result,
        started_at=started_at,
        phases=phases,
        build_status="failed" if build_failed else "skipped" if skip_build else "passed",
    )
    _write_matrix(
        artifact_dir,
        result=result,
        started_at=started_at,
        completed_at=datetime.now(UTC),
    )
    return 0 if passed else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument("--reuse-running", action="store_true", help="Use already-running local flagship services.")
    parser.add_argument("--skip-build", action="store_true", help="Do not rebuild bundles before certification.")
    parser.add_argument(
        "--export-qualification",
        action="store_true",
        help="Require normal and constrained export evidence from every flagship host.",
    )
    args = parser.parse_args(argv)

    default_artifact_dir = (
        EXPORT_QUALIFICATION_ARTIFACT_DIR
        if args.export_qualification
        else DEFAULT_ARTIFACT_DIR
    )
    artifact_dir = (args.artifact_dir or default_artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    environment = os.environ.copy()
    python_paths = [str(ROOT), str(PACKAGE), str(PACKAGE / "pivot_engine")]
    environment["PYTHONPATH"] = os.pathsep.join(python_paths + [environment.get("PYTHONPATH", "")])

    if args.export_qualification:
        return _certify_export_qualification(
            artifact_dir,
            environment,
            skip_build=args.skip_build,
            reuse_running=args.reuse_running,
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
