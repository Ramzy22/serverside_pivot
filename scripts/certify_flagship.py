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
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "dash_tanstack_pivot"
DEFAULT_ARTIFACT_DIR = ROOT / "artifacts" / "flagship-certification"

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


def _build() -> None:
    npm = shutil.which("npm") or "npm"
    _run([npm, "run", "build:js"], cwd=PACKAGE)
    _run([npm, "run", "build:react"], cwd=PACKAGE)
    _run([npm, "run", "build:streamlit"], cwd=PACKAGE)
    # Streamlit loads its frontend from the installed Python distribution.
    # Reinstall after the Vite build so the browser suite never certifies a
    # stale component bundle left behind by an earlier local install.
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


def _run_browser_suite(artifact_dir: Path, environment: dict[str, str]) -> int:
    child_environment = {
        **environment,
        "PIVOT_RUN_FLAGSHIP_BROWSER_TESTS": "1",
        "PIVOT_FLAGSHIP_ARTIFACT_DIR": str(artifact_dir),
    }
    command = [sys.executable, "-m", "pytest", "tests/test_flagship_certification.py", "-m", "flagship", "-q"]
    completed = subprocess.run(command, cwd=ROOT, env=child_environment)
    return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    parser.add_argument("--reuse-running", action="store_true", help="Use already-running local flagship services.")
    parser.add_argument("--skip-build", action="store_true", help="Do not rebuild bundles before certification.")
    args = parser.parse_args()

    artifact_dir = args.artifact_dir.resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    environment = os.environ.copy()
    python_paths = [str(ROOT), str(PACKAGE), str(PACKAGE / "pivot_engine")]
    environment["PYTHONPATH"] = os.pathsep.join(python_paths + [environment.get("PYTHONPATH", "")])

    result = "failed"
    try:
        if not args.skip_build:
            _build()
        if args.reuse_running:
            code = _run_browser_suite(artifact_dir, environment)
        else:
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
                code = _run_browser_suite(artifact_dir, environment)
        result = "passed" if code == 0 else "failed"
        return code
    finally:
        _write_matrix(artifact_dir, result=result, started_at=started_at, completed_at=datetime.now(UTC))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
