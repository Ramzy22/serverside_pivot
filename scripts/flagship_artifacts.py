"""Bind installed flagship hosts to the artifacts from one clean source tree."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
import venv
import zipfile
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit


@dataclass(frozen=True)
class ArtifactBinding:
    source_commit: str = ""
    sha256: dict[str, str] = field(default_factory=dict)
    paths: dict[str, Path] = field(default_factory=dict)
    versions: dict[str, str] = field(default_factory=dict)
    expected_loaded_assets: dict[str, dict[str, str]] = field(default_factory=dict)


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _run(command: list[str], cwd: Path) -> str:
    completed = subprocess.run(command, cwd=cwd, check=True, text=True, capture_output=True)
    return completed.stdout.strip()


def _snapshot(source: Path, output: Path) -> tuple[str, Path]:
    commit = _run(["git", "rev-parse", "HEAD"], source)
    snapshot = output / "source"
    archive = output / "source.tar"
    subprocess.run(["git", "archive", "--format=tar", f"--output={archive}", commit], cwd=source, check=True)
    snapshot.mkdir()
    with tarfile.open(archive) as contents:
        for member in contents.getmembers():
            target = (snapshot / member.name).resolve()
            if not target.is_relative_to(snapshot.resolve()) or member.issym() or member.islnk():
                raise ValueError("Unsafe source archive entry")
        contents.extractall(snapshot, filter="data")
    archive.unlink()
    return commit, snapshot


def _python_in(environment: Path) -> Path:
    return environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _file_hashes(paths: list[Path], base: Path, url_prefix: str = "") -> dict[str, str]:
    return {
        f"{url_prefix}/{path.relative_to(base).as_posix()}": _digest(path.read_bytes())
        for path in sorted(paths)
    }


def _wheel_assets(wheel: Path) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {"dash": {}, "streamlit": {}}
    with zipfile.ZipFile(wheel) as archive:
        for name in archive.namelist():
            if name.startswith("dash_tanstack_pivot/") and name.endswith((".js", ".css")):
                result["dash"][f"/_dash-component-suites/{name}"] = _digest(archive.read(name))
            if name.startswith("streamlit_pivot/frontend/build/") and name.endswith((".js", ".css")):
                result["streamlit"][f"/{name}"] = _digest(archive.read(name))
    if not result["dash"] or not result["streamlit"]:
        raise RuntimeError("Wheel omits Dash or Streamlit production assets")
    return result


def _installed_module_paths(python: Path, cwd: Path) -> dict[str, str]:
    probe = (
        "import json,dash_tanstack_pivot,streamlit_pivot,pivot_engine;"
        "print(json.dumps({'dash':dash_tanstack_pivot.__file__,"
        "'streamlit':streamlit_pivot.__file__,'engine':pivot_engine.__file__}))"
    )
    return json.loads(_run([str(python), "-c", probe], cwd))


def build_artifacts(source: Path, output: Path) -> ArtifactBinding:
    """Build and install every host from one committed, isolated source snapshot."""
    source = source.resolve(strict=True)
    output = output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"Artifact output must be empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    commit, snapshot = _snapshot(source, output)
    package = snapshot / "dash_tanstack_pivot"
    npm = shutil.which("npm")
    if npm is None:
        raise RuntimeError("npm is required to build release artifacts")
    _run([npm, "install", "--ignore-scripts", "--legacy-peer-deps", "--no-audit", "--no-fund"], package)
    _run([npm, "run", "build:js"], package)
    _run([npm, "run", "build:react"], package)
    frontend = package / "streamlit_pivot" / "frontend"
    _run([npm, "install", "--ignore-scripts", "--legacy-peer-deps", "--no-audit", "--no-fund"], frontend)
    _run([npm, "run", "build"], frontend)

    wheel_dir = output / "wheels"
    wheel_dir.mkdir()
    _run([sys.executable, "-m", "build", "--wheel", "--outdir", str(wheel_dir), "."], package)
    wheels = list(wheel_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"Expected one wheel, found {len(wheels)}")
    wheel = wheels[0]
    _run([npm, "pack", "--ignore-scripts", "--pack-destination", str(output)], package)
    tarballs = list(output.glob("*.tgz"))
    if len(tarballs) != 1:
        raise RuntimeError(f"Expected one npm tarball, found {len(tarballs)}")
    npm_tarball = tarballs[0]

    environment = output / "python-env"
    venv.EnvBuilder(with_pip=True, system_site_packages=True).create(environment)
    python = _python_in(environment)
    _run([str(python), "-m", "pip", "install", "--no-deps", "--force-reinstall", str(wheel)], output)

    app = output / "installed-app"
    (app / "examples").mkdir(parents=True)
    for name in ("__init__.py", "flagship_demo_data.py", "flagship_dash.py", "flagship_runtime_server.py", "flagship_streamlit.py"):
        candidate = snapshot / "examples" / name
        if candidate.is_file():
            shutil.copy2(candidate, app / "examples" / name)
    fixture = snapshot / "tests" / "data" / "flagship_parity_rows.json"
    if not fixture.is_file():
        raise FileNotFoundError(f"Required parity fixture missing: {fixture}")
    (app / "tests" / "data").mkdir(parents=True)
    shutil.copy2(fixture, app / "tests" / "data" / fixture.name)
    react_app = app / "examples" / "flagship_react"
    shutil.copytree(snapshot / "examples" / "flagship_react", react_app, ignore=shutil.ignore_patterns("node_modules", "dist", "release-package.tgz"))
    shutil.copy2(npm_tarball, react_app / "release-package.tgz")
    _run([npm, "install", "--package-lock-only", "--ignore-scripts", "--legacy-peer-deps", "--no-audit", "--no-fund"], react_app)
    _run([npm, "ci", "--ignore-scripts", "--legacy-peer-deps", "--no-audit", "--no-fund"], react_app)
    installed_package = react_app / "node_modules" / "dash_tanstack_pivot" / "dist" / "react" / "index.mjs"
    if not installed_package.is_file() or installed_package.read_bytes() != (package / "dist" / "react" / "index.mjs").read_bytes():
        raise RuntimeError("React example did not resolve the packed package")
    _run([npm, "run", "build"], react_app)

    origins = _installed_module_paths(python, app)
    site_packages = (environment / ("Lib/site-packages" if os.name == "nt" else f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages")).resolve()
    if any(not Path(origin).resolve().is_relative_to(site_packages) for origin in origins.values()):
        raise RuntimeError(f"Host imported Python package outside installed wheel: {origins}")

    react_dist = react_app / "dist"
    assets = _wheel_assets(wheel)
    assets["react"] = _file_hashes([path for path in react_dist.rglob("*") if path.is_file()], react_dist)
    versions = {
        "python": _run([str(python), "--version"], app),
        "node": _run([shutil.which("node") or "node", "--version"], app),
        "npm": _run([npm, "--version"], app),
        "dash": _run([str(python), "-c", "import dash; print(dash.__version__)"], app),
        "streamlit": _run([str(python), "-c", "import streamlit; print(streamlit.__version__)"], app),
        "react": _run([shutil.which("node") or "node", "-p", "require('./node_modules/react/package.json').version"], react_app),
    }
    hashes = {
        "source_commit": _digest(commit.encode()),
        "wheel": _digest(wheel.read_bytes()),
        "npm_tarball": _digest(npm_tarball.read_bytes()),
        "fixture": _digest(fixture.read_bytes()),
        **{f"react:{name}": digest for name, digest in assets["react"].items()},
        **{f"dash:{name}": digest for name, digest in assets["dash"].items()},
        **{f"streamlit:{name}": digest for name, digest in assets["streamlit"].items()},
    }
    return ArtifactBinding(
        source_commit=commit,
        sha256=hashes,
        paths={"source": snapshot, "wheel": wheel, "npm_tarball": npm_tarball, "python": python, "app": app, "react_dist": react_dist},
        versions=versions,
        expected_loaded_assets=assets,
    )


def _wait_for(url: str, process: subprocess.Popen[bytes], timeout: float = 60) -> None:
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if process.poll() is not None:
            raise RuntimeError(f"Installed host exited before startup: {url}")
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.25)
    raise RuntimeError(f"Installed host did not start: {url}")


@contextmanager
def start_installed_hosts(binding: ArtifactBinding, output: Path) -> Iterator[dict[str, str]]:
    """Serve copied examples with the isolated wheel and packed React build."""
    app = binding.paths["app"]
    python = binding.paths["python"]
    react_app = app / "examples" / "flagship_react"
    npm = shutil.which("npm")
    if npm is None:
        raise RuntimeError("npm is required to serve the installed React build")
    output.mkdir(parents=True, exist_ok=True)
    child_env = os.environ.copy()
    child_env.pop("PYTHONPATH", None)
    child_env["PYTHONPATH"] = str(app)
    child_env["PIVOT_FLAGSHIP_PARITY_DATA"] = "1"
    commands = (
        ("runtime", [str(python), "-m", "uvicorn", "examples.flagship_runtime_server:app", "--host", "127.0.0.1", "--port", "8765"], app, "http://127.0.0.1:8765/health"),
        ("dash", [str(python), "examples/flagship_dash.py"], app, "http://127.0.0.1:8050"),
        ("react", [npm, "run", "preview"], react_app, "http://127.0.0.1:5174"),
        ("streamlit", [str(python), "-m", "streamlit", "run", "examples/flagship_streamlit.py", "--server.address", "127.0.0.1", "--server.port", "8502", "--server.headless", "true", "--browser.gatherUsageStats", "false"], app, "http://127.0.0.1:8502/_stcore/health"),
    )
    with ExitStack() as stack:
        for name, command, cwd, health in commands:
            log = stack.enter_context((output / f"{name}.log").open("wb"))
            process = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=log, stderr=subprocess.STDOUT)
            stack.callback(_stop_process, process)
            _wait_for(health, process)
        yield {"dash": "http://127.0.0.1:8050", "react": "http://127.0.0.1:5174", "streamlit": "http://127.0.0.1:8502"}


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def verify_loaded_asset(binding: ArtifactBinding, host: str, url: str, body: bytes) -> str:
    """Reject an unrecorded or changed production asset returned by a host."""
    path = urlsplit(url).path
    if path == "/@vite/client" or path.startswith("/@vite/"):
        raise ValueError("Vite development client is forbidden in a release host")
    expected = binding.expected_loaded_assets.get(host, {}).get(path)
    if expected is None:
        raise ValueError(f"unbound asset: {host} {path}")
    actual = hashlib.sha256(body).hexdigest()
    if actual != expected:
        raise ValueError(f"asset hash mismatch: {host} {path}")
    return actual
