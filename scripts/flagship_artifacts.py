"""Bind installed flagship hosts to the artifacts from one clean source tree."""

from __future__ import annotations

import hashlib
import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
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
    try:
        completed = subprocess.run(command, cwd=cwd, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as error:
        # The caller retains private build output. Do not discard the installer
        # or bundler diagnostics behind CalledProcessError's command-only text.
        if error.stdout:
            print(error.stdout, file=sys.stderr, end="")
        if error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        raise
    return completed.stdout.strip()


def _snapshot(source: Path, output: Path) -> tuple[str, Path]:
    commit = _run(["git", "rev-parse", "HEAD"], source)
    snapshot = output / "source"
    archive = output / "source.tar"
    subprocess.run(["git", "-c", "core.autocrlf=false", "archive", "--format=tar", f"--output={archive}", commit], cwd=source, check=True)
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


def _create_python_environment(environment: Path) -> Path:
    venv.EnvBuilder(with_pip=True, system_site_packages=False).create(environment)
    return _python_in(environment)


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
    # setuptools' configured ../.setuptools-build can exceed Windows MAX_PATH
    # when pytest or a release job chooses a deeply nested evidence directory.
    with tempfile.TemporaryDirectory(prefix="pf-") as short_root:
        commit, snapshot = _snapshot(source, Path(short_root))
        return _build_from_snapshot(commit, snapshot, output)


def _build_from_snapshot(commit: str, snapshot: Path, output: Path) -> ArtifactBinding:
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
    python = _create_python_environment(environment)
    _run([str(python), "-m", "pip", "--isolated", "install", "--force-reinstall", f"{wheel}[streamlit,api]"], output)

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
    # Registry dependencies stay locked. The local tarball is rebuilt for this
    # commit and bound separately by SHA-256, so its lock entry has no stale
    # integrity from an earlier release.
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
        paths={"wheel": wheel, "npm_tarball": npm_tarball, "python": python, "app": app, "react_dist": react_dist},
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
                    if not _owns_listener(process, url):
                        raise RuntimeError(f"Installed host listener is not owned by the launched process: {url}")
                    return
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.25)
    raise RuntimeError(f"Installed host did not start: {url}")


def _owns_listener(process: subprocess.Popen, url: str) -> bool:
    import psutil

    endpoint = urlsplit(url)
    try:
        root = psutil.Process(process.pid)
        for owner in (root, *root.children(recursive=True)):
            try:
                if any(connection.status == psutil.CONN_LISTEN
                       and connection.laddr.port == endpoint.port
                       and connection.laddr.ip in (endpoint.hostname, "0.0.0.0", "::")
                       for connection in (getattr(owner, "net_connections", None) or owner.connections)(kind="tcp")):
                    return True
            except psutil.NoSuchProcess:
                continue
    except psutil.NoSuchProcess:
        pass
    return False


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
    with ExitStack() as stack:
        reservations = {}
        ports = {}
        for name in ("runtime", "dash", "react", "streamlit"):
            listener = stack.enter_context(socket.socket())
            if os.name == "nt":
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            port = 8502 if name == "streamlit" else 0
            try:
                listener.bind(("127.0.0.1", port))
            except OSError as error:
                raise RuntimeError(f"occupied installed host port: {port}") from error
            reservations[name] = listener
            ports[name] = listener.getsockname()[1]
        # Streamlit keeps its configured demo CORS origin; all reservations are
        # checked before any process is started and listener ownership is verified.
        child_env["PIVOT_RUNTIME_UPSTREAM"] = f"http://127.0.0.1:{ports['runtime']}"
        child_env["PIVOT_RUNTIME_ENDPOINT"] = f"http://127.0.0.1:{ports['runtime']}/v1/runtime"
        commands = (
            ("runtime", [str(python), "-m", "uvicorn", "examples.flagship_runtime_server:app", "--host", "127.0.0.1", "--port", str(ports["runtime"])], app, f"http://127.0.0.1:{ports['runtime']}/health"),
            ("dash", [str(python), "-c", f"from examples.flagship_dash import app; app.run(debug=False, use_reloader=False, host='127.0.0.1', port={ports['dash']})"], app, f"http://127.0.0.1:{ports['dash']}"),
            ("react", [npm, "run", "preview", "--", "--port", str(ports["react"])], react_app, f"http://127.0.0.1:{ports['react']}"),
            ("streamlit", [str(python), "-m", "streamlit", "run", "examples/flagship_streamlit.py", "--server.address", "127.0.0.1", "--server.port", str(ports["streamlit"]), "--server.headless", "true", "--browser.gatherUsageStats", "false"], app, f"http://127.0.0.1:{ports['streamlit']}/_stcore/health"),
        )
        for name, command, cwd, health in commands:
            log = stack.enter_context((output / f"{name}.log").open("wb"))
            reservations[name].close()
            process = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=log, stderr=subprocess.STDOUT)
            stack.callback(_stop_process, process)
            (output / f"{name}.pid").write_text(str(process.pid), encoding="ascii")
            _wait_for(health, process)
        yield {name: f"http://127.0.0.1:{ports[name]}" for name in ("dash", "react", "streamlit")}


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    import psutil

    try:
        root = psutil.Process(process.pid)
        owned = [*reversed(root.children(recursive=True)), root]
    except psutil.NoSuchProcess:
        return
    for child in owned:
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
    _, survivors = psutil.wait_procs(owned, timeout=5)
    for child in survivors:
        try:
            child.kill()
        except psutil.NoSuchProcess:
            pass
    _, survivors = psutil.wait_procs(survivors, timeout=5)
    process.wait(timeout=5)
    if survivors:
        raise RuntimeError("Installed host process tree did not stop")


def verify_loaded_asset(binding: ArtifactBinding, host: str, url: str, body: bytes) -> str:
    """Reject an unrecorded or changed production asset returned by a host."""
    path = urlsplit(url).path
    if path == "/@vite/client" or path.startswith("/@vite/"):
        raise ValueError("Vite development client is forbidden in a release host")
    expected = binding.expected_loaded_assets.get(host, {}).get(path)
    if expected is None and host == "dash":
        from dash.fingerprint import check_fingerprint

        original_path, fingerprinted = check_fingerprint(path)
        if fingerprinted:
            expected = binding.expected_loaded_assets.get(host, {}).get(original_path)
    if expected is None and host == "streamlit":
        # Streamlit serves wheel component files below its own versioned URL.
        # Their hashed filenames are unique within the installed build.
        matches = [
            digest for bound_path, digest in binding.expected_loaded_assets.get(host, {}).items()
            if Path(bound_path).name == Path(path).name
        ]
        if len(matches) == 1:
            expected = matches[0]
    if expected is None:
        raise ValueError(f"unbound asset: {host} {path}")
    actual = hashlib.sha256(body).hexdigest()
    if actual != expected:
        raise ValueError(f"asset hash mismatch: {host} {path}")
    return actual


def load_artifact_binding(path: Path) -> ArtifactBinding:
    record = json.loads(path.read_text(encoding="utf-8"))
    assets = record["expectedLoadedAssets"]
    if not all(assets.get(host) for host in ("dash", "react", "streamlit")):
        raise ValueError("Artifact binding omits a host's production assets")
    return ArtifactBinding(
        source_commit=record["sourceCommit"], sha256=record["sha256"],
        versions=record["versions"], expected_loaded_assets=assets,
    )


def verify_browser_assets(binding: ArtifactBinding, host: str, driver, entries: list[dict] | None = None) -> dict[str, str]:
    """Hash actual CDP response bodies before their performance batch is discarded."""
    loaded = {}
    for entry in driver.get_log("performance") if entries is None else entries:
        event = json.loads(entry["message"])["message"]
        params = event.get("params", {})
        resource = params.get("response", params.get("request", {})).get("url", "")
        path = urlsplit(resource).path
        if path == "/@vite/client" or path.startswith("/@vite/"):
            raise ValueError("Vite development client is forbidden in a release host")
        if event["method"] != "Network.responseReceived":
            continue
        own_asset = (
            host == "react" and path.startswith("/assets/")
            or host == "dash" and path.startswith("/_dash-component-suites/dash_tanstack_pivot/")
            or host == "streamlit" and (
                path.startswith("/_stcore/bidi-components/serverside-pivot.")
                or path.startswith("/_stcore/component/serverside-pivot/")
                or Path(path).name in {Path(name).name for name in binding.expected_loaded_assets[host]}
            )
        )
        if own_asset and path.endswith((".js", ".mjs", ".css")):
            response = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": params["requestId"]})
            body = base64.b64decode(response["body"]) if response.get("base64Encoded") else response["body"].encode()
            loaded[path] = verify_loaded_asset(binding, host, resource, body)
    return loaded
