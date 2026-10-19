"""Own a private, pinned production reference server for one qualification run."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from typing import Iterator, Mapping
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener


READINESS_TIMEOUT = 30.0
PINNED_VERSIONS = {
    "ag-grid-community": "36.2.0",
    "ag-grid-enterprise": "36.2.0",
    "ag-charts-enterprise": "14.2.0",
}


@dataclass(frozen=True)
class ReferenceServer:
    url: str
    config_sha256: str
    fixture_sha256: str
    versions: Mapping[str, str]


@contextmanager
def start_reference_server(source_root: Path, evidence_root: Path) -> Iterator[ReferenceServer]:
    """Build, validate and own a loopback server; preserve logs even on failure.

    Node is launched directly (without an npm or tsx wrapper) so the process
    handle used for cleanup is the server itself. No port-based process lookup
    or termination is performed. The supplied evidence root must be private.
    """
    source_root = Path(source_root).resolve()
    app = source_root / "reference" / "ag_grid_36_2"
    config_path = app / "reference-config.json"
    fixture_path = source_root / "tests" / "data" / "flagship_parity_rows.json"
    config_bytes = config_path.read_bytes()
    config = json.loads(config_bytes)
    fixture_sha256 = hashlib.sha256(fixture_path.read_bytes()).hexdigest()
    config_sha256 = hashlib.sha256(config_bytes).hexdigest()
    package = json.loads((app / "package.json").read_bytes())
    if config.get("version") != "36.2.0" or any(
        package.get("dependencies", {}).get(name) != version
        for name, version in PINNED_VERSIONS.items()
    ):
        raise ValueError("reference packages/config must pin AG Grid 36.2.0 and AG Charts 14.2.0")

    evidence_root = Path(evidence_root).resolve()
    evidence_root.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="reference-", dir=evidence_root))
    npm = shutil.which("npm.cmd" if os.name == "nt" else "npm") or "npm"
    node = shutil.which("node") or "node"
    for label, args in (("install", ["ci"]), ("build", ["run", "build"])):
        with (logs / f"{label}.log").open("wb") as output:
            subprocess.run([npm, *args], cwd=app, check=True, stdout=output,
                           stderr=subprocess.STDOUT)
    versions = {
        name: json.loads((app / "node_modules" / name / "package.json").read_bytes())["version"]
        for name in PINNED_VERSIONS
    }
    if versions != PINNED_VERSIONS:
        raise ValueError("installed reference packages do not match pinned versions")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    url = f"http://127.0.0.1:{port}/"
    env = {**os.environ, "REFERENCE_PORT": str(port), "NODE_ENV": "production"}
    # Bypass environment HTTP proxies for this strictly local readiness probe.
    opener = build_opener(ProxyHandler({}))
    with (logs / "server.log").open("wb") as output:
        child = subprocess.Popen([node, "--import", "tsx", "src/server.ts"],
                                 cwd=app, env=env, stdout=output, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + READINESS_TIMEOUT
            while True:
                if child.poll() is not None:
                    raise RuntimeError(f"reference server exited before readiness; see {logs}")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"reference server readiness timed out; see {logs}")
                try:
                    with opener.open(url + "reference-config.json", timeout=min(1.0, remaining)) as response:
                        served = json.load(response)
                except (URLError, OSError):
                    time.sleep(min(0.05, max(0, deadline - time.monotonic())))
                    continue
                if not isinstance(served, dict) or served.get("fixtureSha256") != fixture_sha256:
                    raise ValueError("reference fixture hash mismatch")
                served_config = {key: value for key, value in served.items()
                                 if key not in {"fixtureSha256", "filterValues"}}
                if served_config != config:
                    raise ValueError("reference config mismatch")
                if (hashlib.sha256(config_path.read_bytes()).hexdigest() != config_sha256
                        or hashlib.sha256(fixture_path.read_bytes()).hexdigest() != fixture_sha256):
                    raise ValueError("reference source hashes changed during startup")
                if child.poll() is not None:
                    raise RuntimeError("reference server exited during readiness")
                break
            yield ReferenceServer(url, config_sha256, fixture_sha256, versions)
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
