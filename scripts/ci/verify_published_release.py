#!/usr/bin/env python3
"""Verify published PyPI and npm artifacts in clean, isolated environments.

Polls both public registries until the target version is visible, checks
commit provenance where available, installs both packages from the public
registries into fresh temporary directories, and executes runtime smoke checks.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.sync_version import read_authoritative_version  # noqa: E402


def _fetch_json(url: str, timeout: float = 15.0) -> dict | None:
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "serverside-pivot-release-verifier/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return None
        raise
    except (urllib.error.URLError, TimeoutError):
        return None


def wait_for_registry_versions(
    *,
    pypi_package: str,
    npm_package: str,
    version: str,
    timeout_seconds: int = 180,
    poll_interval: int = 10,
) -> tuple[dict, dict]:
    """Wait until both PyPI and npm expose ``version``."""
    pypi_url = f"https://pypi.org/pypi/{pypi_package}/{version}/json"
    npm_url = f"https://registry.npmjs.org/{npm_package}/{version}"

    deadline = time.monotonic() + timeout_seconds
    pypi_data: dict | None = None
    npm_data: dict | None = None

    while True:
        if pypi_data is None:
            pypi_data = _fetch_json(pypi_url)
        if npm_data is None:
            npm_data = _fetch_json(npm_url)
        if pypi_data is not None and npm_data is not None:
            return pypi_data, npm_data
        if time.monotonic() >= deadline:
            missing = []
            if pypi_data is None:
                missing.append(f"PyPI {pypi_package}=={version} ({pypi_url})")
            if npm_data is None:
                missing.append(f"npm {npm_package}@{version} ({npm_url})")
            raise TimeoutError(
                f"Timed out after {timeout_seconds}s waiting for published artifacts: {', '.join(missing)}"
            )
        time.sleep(poll_interval)


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def verify_clean_python_install(
    *,
    pypi_package: str,
    version: str,
    work_dir: Path,
) -> dict[str, str]:
    """Create an isolated venv, install from PyPI, and verify version + imports."""
    venv_dir = work_dir / "py-clean-venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
    py = _venv_python(venv_dir)

    subprocess.run(
        [str(py), "-m", "pip", "install", "--quiet", "--no-cache-dir", f"{pypi_package}=={version}"],
        cwd=work_dir,
        check=True,
    )
    probe = (
        "import importlib.metadata as md, json, serverside_pivot, pivot_engine; "
        "from serverside_pivot import DashTanstackPivot; "
        "from pivot_engine.runtime import PivotServiceResponse; "
        "print(json.dumps({"
        "'dist_version': md.version('serverside-pivot'), "
        "'dash_version': serverside_pivot.__version__, "
        "'component': DashTanstackPivot.__name__, "
        "'runtime': PivotServiceResponse.__name__"
        "}))"
    )
    result = subprocess.run(
        [str(py), "-c", probe],
        cwd=work_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    info = json.loads(result.stdout.strip())
    if info["dist_version"] != version:
        raise AssertionError(
            f"Installed PyPI distribution version {info['dist_version']!r} != {version!r}"
        )
    if info["dash_version"] != version:
        raise AssertionError(
            f"Installed serverside_pivot.__version__ {info['dash_version']!r} != {version!r}"
        )
    return info


def verify_clean_npm_install(
    *,
    npm_package: str,
    version: str,
    work_dir: Path,
) -> dict[str, str]:
    """Create an isolated Node project, install from npm, and verify exports."""
    npm_dir = work_dir / "npm-clean-project"
    npm_dir.mkdir(parents=True, exist_ok=True)
    npm_cmd = shutil.which("npm") or "npm"
    node_cmd = shutil.which("node") or "node"

    subprocess.run([npm_cmd, "init", "--yes"], cwd=npm_dir, check=True, capture_output=True)
    subprocess.run(
        [npm_cmd, "install", "--ignore-scripts", f"{npm_package}@{version}"],
        cwd=npm_dir,
        check=True,
    )

    smoke_script = ROOT / "scripts" / "ci" / "smoke_installed_react_runtime.mjs"
    shutil.copy2(smoke_script, npm_dir / "smoke_installed_react_runtime.mjs")
    subprocess.run([node_cmd, "smoke_installed_react_runtime.mjs"], cwd=npm_dir, check=True)

    probe = (
        f"import fs from 'node:fs'; "
        f"const pkg = JSON.parse(fs.readFileSync('node_modules/{npm_package}/package.json', 'utf8')); "
        f"const mod = await import('{npm_package}'); "
        f"const reactMod = await import('{npm_package}/react'); "
        f"console.log(JSON.stringify({{version: pkg.version, exports: Object.keys(mod).length, reactExports: Object.keys(reactMod).length}}));"
    )
    result = subprocess.run(
        [node_cmd, "--input-type=module", "-e", probe],
        cwd=npm_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    info = json.loads(result.stdout.strip())
    if info["version"] != version:
        raise AssertionError(
            f"Installed npm package version {info['version']!r} != {version!r}"
        )
    return info


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default=None, help="Expected version (defaults to VERSION).")
    parser.add_argument("--pypi-package", default="serverside-pivot")
    parser.add_argument("--npm-package", default="serverside_pivot")
    parser.add_argument("--expected-commit", default=None, help="Expected release commit SHA.")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--poll-interval", type=int, default=10)
    parser.add_argument("--output", type=Path, default=None, help="Optional JSON report path.")
    args = parser.parse_args(argv)

    version = (
        args.version.lstrip("v")
        if args.version
        else read_authoritative_version(ROOT)
    )

    pypi_data, npm_data = wait_for_registry_versions(
        pypi_package=args.pypi_package,
        npm_package=args.npm_package,
        version=version,
        timeout_seconds=args.timeout_seconds,
        poll_interval=args.poll_interval,
    )

    npm_git_head = npm_data.get("gitHead")
    if args.expected_commit and npm_git_head and npm_git_head != args.expected_commit:
        raise AssertionError(
            f"npm gitHead {npm_git_head!r} does not match expected commit {args.expected_commit!r}"
        )

    with tempfile.TemporaryDirectory(prefix="verify-published-release-") as tmp:
        work_dir = Path(tmp)
        py_info = verify_clean_python_install(
            pypi_package=args.pypi_package,
            version=version,
            work_dir=work_dir,
        )
        npm_info = verify_clean_npm_install(
            npm_package=args.npm_package,
            version=version,
            work_dir=work_dir,
        )

    report = {
        "version": version,
        "expected_commit": args.expected_commit,
        "pypi": {
            "package": args.pypi_package,
            "version": pypi_data.get("info", {}).get("version"),
            "installed": py_info,
        },
        "npm": {
            "package": args.npm_package,
            "version": npm_data.get("version"),
            "gitHead": npm_git_head,
            "installed": npm_info,
        },
    }
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(
        f"verify_published_release: OK — {args.pypi_package}=={version} (PyPI) "
        f"and {args.npm_package}@{version} (npm) installed and verified in clean environments."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
