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
import uuid
import email
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
    host_origins: dict[str, str] = field(default_factory=dict)


_OWNED_ENVIRONMENTS: dict[Path, tuple[str, Path]] = {}

# Do not retain the interpreter's old ensurepip seed in installed release hosts.
# Review this exact wheel and hash when advancing the release installer.
_RELEASE_PIP_VERSION = "26.2"
_RELEASE_PIP_URL = "https://files.pythonhosted.org/packages/62/36/a3aed958d60531cb442b7ab4596cda7b3621cfb916f8ae1d6769795c7dc1/pip-26.2-py3-none-any.whl"
_RELEASE_PIP_SHA256 = "931c303696af6fa3417112103b1cad26890e5a07eccb5b99783700e33f2b8aad"


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _run(command: list[str], cwd: Path, *, env: dict[str, str] | None = None) -> str:
    try:
        completed = subprocess.run(command, cwd=cwd, env=env, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as error:
        # The caller retains private build output. Do not discard the installer
        # or bundler diagnostics behind CalledProcessError's command-only text.
        if error.stdout:
            print(error.stdout, file=sys.stderr, end="")
        if error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        raise
    return completed.stdout.strip()


def _prepare_build_wheelhouse(wheelhouse: Path, package: Path, output: Path) -> dict[str, str]:
    """Validate hash-pinned wheels and stage only these build-time inputs."""
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10
        import tomli as tomllib
    from packaging.requirements import Requirement
    from packaging.tags import sys_tags
    from packaging.utils import canonicalize_name, parse_wheel_filename
    from packaging.specifiers import SpecifierSet

    try:
        wheelhouse = wheelhouse.resolve(strict=True)
        manifest = json.loads((wheelhouse / "wheelhouse-sha256.json").read_text(encoding="utf-8"))
        wheels = {path.name: path for path in wheelhouse.glob("*.whl")}
        if not isinstance(manifest, dict) or not manifest or set(manifest) != set(wheels):
            raise ValueError("manifest must name every wheel exactly")
        candidates = []
        supported = set(sys_tags())
        for filename, expected in manifest.items():
            path = wheels[filename]
            if path.is_symlink() or not isinstance(expected, str) or _digest(path.read_bytes()) != expected:
                raise ValueError(f"hash mismatch: {filename}")
            name, version, _, tags = parse_wheel_filename(filename)
            if not tags.intersection(supported):
                raise ValueError(f"incompatible wheel: {filename}")
            with zipfile.ZipFile(path) as archive:
                metadata_names = [name for name in archive.namelist() if name.count("/") == 1 and name.endswith(".dist-info/METADATA")]
                if len(metadata_names) != 1:
                    raise ValueError(f"invalid metadata: {filename}")
                metadata = email.message_from_bytes(archive.read(metadata_names[0]))
                if canonicalize_name(metadata.get("Name", "")) != name or metadata.get("Version") != str(version):
                    raise ValueError(f"metadata mismatch: {filename}")
                requires_python = metadata.get("Requires-Python")
                if requires_python and not SpecifierSet(requires_python).contains(".".join(map(str, sys.version_info[:3]))):
                    raise ValueError(f"incompatible Python: {filename}")
            candidates.append((name, version))
        requirements = tomllib.loads((package / "pyproject.toml").read_text(encoding="utf-8"))["build-system"]["requires"]
        for text in requirements:
            requirement = Requirement(text)
            if requirement.marker is not None and not requirement.marker.evaluate():
                continue
            if requirement.url or not any(name == canonicalize_name(requirement.name) and version in requirement.specifier for name, version in candidates):
                raise ValueError(f"missing required wheel: {text}")
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        raise ValueError(f"Invalid build wheelhouse: {error}") from error

    staged = output / "build-wheelhouse"
    staged.mkdir(parents=True)
    for filename, source in wheels.items():
        shutil.copyfile(source, staged / filename)
        if _digest((staged / filename).read_bytes()) != manifest[filename]:
            raise ValueError(f"Invalid build wheelhouse: changed while staging {filename}")
    (output / "build-wheelhouse.json").write_text(json.dumps({
        "source": str(wheelhouse), "staged": str(staged), "sha256": manifest,
        "buildSystemRequires": requirements,
    }, indent=2), encoding="utf-8")
    return {**os.environ, "PIP_NO_INDEX": "1", "PIP_FIND_LINKS": str(staged)}


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
    record = environment.with_name(environment.name + ".location.json").resolve()
    record.parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="pf-env-")).resolve()
    if os.name == "nt" and len(str(root)) > 80:
        root.rmdir()
        raise ValueError("Temporary directory is too long for the release environment")
    token = uuid.uuid4().hex
    _OWNED_ENVIRONMENTS[root] = (token, record)
    ownership = {"environment": str(root), "ownershipId": token, "record": str(record)}
    (root / ".pivot-flagship-owned-env.json").write_text(json.dumps(ownership), encoding="utf-8")
    private_record = {**ownership, "python": str(_python_in(root)), "state": "created"}
    record.write_text(json.dumps(private_record, indent=2), encoding="utf-8")
    venv.EnvBuilder(with_pip=True, system_site_packages=False).create(root)
    try:
        with urllib.request.urlopen(_RELEASE_PIP_URL, timeout=60) as response:
            installer = response.read()
        if _digest(installer) != _RELEASE_PIP_SHA256:
            raise ValueError("Release pip bootstrap wheel hash mismatch")
        with tempfile.TemporaryDirectory(prefix="pf-pip-") as download:
            wheel = Path(download) / f"pip-{_RELEASE_PIP_VERSION}-py3-none-any.whl"
            wheel.write_bytes(installer)
            _run([str(_python_in(root)), "-m", "pip", "--isolated", "install", "--no-index", "--no-deps", "--upgrade", str(wheel)], root)
        installed_pip = _run([str(_python_in(root)), "-c", "import importlib.metadata; print(importlib.metadata.version('pip'))"], root)
        if installed_pip != _RELEASE_PIP_VERSION:
            raise RuntimeError("Release pip bootstrap version mismatch")
        private_record["pipBootstrap"] = {"version": installed_pip, "url": _RELEASE_PIP_URL, "sha256": _RELEASE_PIP_SHA256}
        private_record["state"] = "ready"
        record.write_text(json.dumps(private_record, indent=2), encoding="utf-8")
    except Exception as error:
        # No binding reaches the caller when bootstrap fails. Clean up through
        # the same marker/token/path guard used by normal release teardown.
        try:
            cleanup_python_environment(ArtifactBinding(paths={
                "python": _python_in(root), "environment": root,
                "environment_record": record,
            }))
        except Exception as cleanup_error:
            raise error from cleanup_error
        raise
    return _python_in(root)


def cleanup_python_environment(binding: ArtifactBinding) -> None:
    """Remove only a short environment allocated and recorded by this process."""
    root = binding.paths["environment"].resolve()
    record = binding.paths["environment_record"].resolve()
    ownership = _OWNED_ENVIRONMENTS.get(root)
    if ownership is None or ownership[1] != record:
        raise ValueError("Refusing cleanup of an unowned environment")
    marker = json.loads((root / ".pivot-flagship-owned-env.json").read_text(encoding="utf-8"))
    info = json.loads(record.read_text(encoding="utf-8"))
    expected = {"environment": str(root), "ownershipId": ownership[0], "record": str(record)}
    if (root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith("pf-env-")
            or marker != expected or any(info.get(key) != value for key, value in expected.items())
            or binding.paths["python"].absolute() != _python_in(root).absolute()):
        raise ValueError("Refusing cleanup of an unowned environment")
    shutil.rmtree(root)
    info["state"] = "cleaned"
    record.write_text(json.dumps(info, indent=2), encoding="utf-8")
    del _OWNED_ENVIRONMENTS[root]


def _file_hashes(paths: list[Path], base: Path, url_prefix: str = "") -> dict[str, str]:
    return {
        f"{url_prefix}/{path.relative_to(base).as_posix()}": _digest(path.read_bytes())
        for path in sorted(paths)
    }


def _wheel_assets(wheel: Path) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {"dash": {}, "streamlit": {}}
    with zipfile.ZipFile(wheel) as archive:
        for name in archive.namelist():
            if name.startswith("serverside_pivot/") and name.endswith((".js", ".css")):
                result["dash"][f"/_dash-component-suites/{name}"] = _digest(archive.read(name))
            if name.startswith("streamlit_pivot/frontend/build/") and name.endswith((".js", ".css")):
                result["streamlit"][f"/{name}"] = _digest(archive.read(name))
    if not result["dash"] or not result["streamlit"]:
        raise RuntimeError("Wheel omits Dash or Streamlit production assets")
    return result


def _installed_module_paths(python: Path, cwd: Path) -> dict[str, str]:
    probe = (
        "import json,serverside_pivot,streamlit_pivot,pivot_engine;"
        "print(json.dumps({'dash':serverside_pivot.__file__,"
        "'streamlit':streamlit_pivot.__file__,'engine':pivot_engine.__file__}))"
    )
    isolated_env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    return json.loads(_run([str(python), "-I", "-c", probe], cwd, env=isolated_env))


def _copy_flagship_fixtures(snapshot: Path, app: Path) -> dict[str, str]:
    """Stage and hash qualification oracles from the frozen snapshot."""
    names = {"fixture": "flagship_parity_rows.json", "scroll_fixture": "flagship_scroll_rows.json",
             "parts_fixture": "flagship_parts_manifest.json"}
    files = {key: snapshot / "tests" / "data" / name for key, name in names.items()}
    for path in files.values():
        if not path.is_file():
            raise FileNotFoundError(f"Required flagship fixture missing: {path}")
    target = app / "tests" / "data"
    target.mkdir(parents=True)
    for path in files.values():
        shutil.copy2(path, target / path.name)
    return {key: _digest(path.read_bytes()) for key, path in files.items()}


def build_artifacts(source: Path, output: Path, *, build_wheelhouse: Path | None = None) -> ArtifactBinding:
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
        return _build_from_snapshot(commit, snapshot, output, build_wheelhouse=build_wheelhouse)


def _build_from_snapshot(commit: str, snapshot: Path, output: Path, *, build_wheelhouse: Path | None = None) -> ArtifactBinding:
    package = snapshot / "serverside_pivot"
    build_environment = _prepare_build_wheelhouse(build_wheelhouse, package, output) if build_wheelhouse is not None else None
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
    build_command = [sys.executable, "-m", "build", "--wheel", "--outdir", str(wheel_dir), "."]
    if build_environment is None:
        _run(build_command, package)
    else:
        _run(build_command, package, env=build_environment)
    wheels = list(wheel_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"Expected one wheel, found {len(wheels)}")
    wheel = wheels[0]
    _run([npm, "pack", "--ignore-scripts", "--pack-destination", str(output)], package)
    tarballs = list(output.glob("*.tgz"))
    if len(tarballs) != 1:
        raise RuntimeError(f"Expected one npm tarball, found {len(tarballs)}")
    npm_tarball = tarballs[0]

    python = _create_python_environment(output / "python-env")
    environment = python.parent.parent
    environment_record = output / "python-env.location.json"
    private_record = json.loads(environment_record.read_text(encoding="utf-8"))
    private_record.update(sourceCommit=commit, wheelSha256=_digest(wheel.read_bytes()))
    environment_record.write_text(json.dumps(private_record, indent=2), encoding="utf-8")
    _run([str(python), "-m", "pip", "--isolated", "install", "--force-reinstall", f"{wheel}[streamlit,api,qualification]"], output)

    app = output / "installed-app"
    (app / "examples").mkdir(parents=True)
    for name in ("__init__.py", "flagship_demo_data.py", "flagship_dash.py", "flagship_runtime_server.py", "flagship_streamlit.py"):
        candidate = snapshot / "examples" / name
        if candidate.is_file():
            shutil.copy2(candidate, app / "examples" / name)
    fixture_hashes = _copy_flagship_fixtures(snapshot, app)
    react_app = app / "examples" / "flagship_react"
    shutil.copytree(snapshot / "examples" / "flagship_react", react_app, ignore=shutil.ignore_patterns("node_modules", "dist", "release-package.tgz"))
    shutil.copy2(npm_tarball, react_app / "release-package.tgz")
    # Registry dependencies stay locked. Update only the local tarball's
    # integrity in the copied lockfile to match the tarball built for this commit.
    lock_path = react_app / "package-lock.json"
    if lock_path.is_file():
        lock_data = json.loads(lock_path.read_text(encoding="utf-8"))
        local_entry = (lock_data.get("packages") or {}).get("node_modules/serverside_pivot")
        if isinstance(local_entry, dict):
            tarball_sri = "sha512-" + base64.b64encode(
                hashlib.sha512(npm_tarball.read_bytes()).digest()
            ).decode("ascii")
            local_entry["integrity"] = tarball_sri
            lock_path.write_text(json.dumps(lock_data, indent=2) + "\n", encoding="utf-8")
    _run([npm, "ci", "--ignore-scripts", "--legacy-peer-deps", "--no-audit", "--no-fund"], react_app)
    installed_package = react_app / "node_modules" / "serverside_pivot" / "dist" / "react" / "index.mjs"
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
        "duckdb": _run([str(python), "-c", "import duckdb; print(duckdb.__version__)"], app),
        "ibis": _run([str(python), "-c", "import ibis; print(ibis.__version__)"], app),
    }
    hashes = {
        "source_commit": _digest(commit.encode()),
        "wheel": _digest(wheel.read_bytes()),
        "npm_tarball": _digest(npm_tarball.read_bytes()),
        **fixture_hashes,
        "export_job_fixture": _digest((app / "examples" / "flagship_demo_data.py").read_bytes()),
        **{f"react:{name}": digest for name, digest in assets["react"].items()},
        **{f"dash:{name}": digest for name, digest in assets["dash"].items()},
        **{f"streamlit:{name}": digest for name, digest in assets["streamlit"].items()},
    }
    if build_wheelhouse is not None:
        hashes.update({f"build-input:{name}": digest for name, digest in json.loads((output / "build-wheelhouse.json").read_text(encoding="utf-8"))["sha256"].items()})
    return ArtifactBinding(
        source_commit=commit,
        sha256=hashes,
        paths={"wheel": wheel, "npm_tarball": npm_tarball, "python": python, "environment": environment, "environment_record": environment_record, "app": app, "react_dist": react_dist},
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
    if not isinstance(process, OwnedHostProcess):
        return False
    for owner in process.members():
        try:
            if any(connection.status == psutil.CONN_LISTEN
                   and connection.laddr.port == endpoint.port
                   and connection.laddr.ip in (endpoint.hostname, "0.0.0.0", "::")
                   for connection in (getattr(owner, "net_connections", None) or owner.connections)(kind="tcp")):
                return True
        except psutil.NoSuchProcess:
            continue
    return False


class _WindowsJob:
    """Kernel-owned process membership survives launcher exit and PID reuse."""

    def __init__(self):
        import ctypes
        from ctypes import wintypes

        self.ctypes = ctypes
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel.CreateJobObjectW.argtypes = (wintypes.LPVOID, wintypes.LPCWSTR)
        self.kernel.CreateJobObjectW.restype = wintypes.HANDLE
        self.kernel.SetInformationJobObject.argtypes = (wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD)
        self.kernel.SetInformationJobObject.restype = wintypes.BOOL
        self.kernel.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
        self.kernel.AssignProcessToJobObject.restype = wintypes.BOOL
        self.kernel.QueryInformationJobObject.argtypes = (wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD, wintypes.LPVOID)
        self.kernel.QueryInformationJobObject.restype = wintypes.BOOL
        self.kernel.TerminateJobObject.argtypes = (wintypes.HANDLE, wintypes.UINT)
        self.kernel.TerminateJobObject.restype = wintypes.BOOL
        self.kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        self.kernel.CloseHandle.restype = wintypes.BOOL

        class BasicLimits(ctypes.Structure):
            _fields_ = [("process_time", ctypes.c_longlong), ("job_time", ctypes.c_longlong),
                        ("flags", wintypes.DWORD), ("min_working", ctypes.c_size_t),
                        ("max_working", ctypes.c_size_t), ("active_limit", wintypes.DWORD),
                        ("affinity", ctypes.c_size_t), ("priority", wintypes.DWORD),
                        ("scheduling", wintypes.DWORD)]
        class ExtendedLimits(ctypes.Structure):
            _fields_ = [("basic", BasicLimits), ("io", ctypes.c_ulonglong * 6),
                        ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                        ("peak_process", ctypes.c_size_t), ("peak_job", ctypes.c_size_t)]
        self.handle = self.kernel.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = ExtendedLimits()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.kernel.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            self.close()
            raise ctypes.WinError(ctypes.get_last_error())

    def assign(self, process):
        if not self.kernel.AssignProcessToJobObject(self.handle, int(process._handle)):
            raise self.ctypes.WinError(self.ctypes.get_last_error())

    def pids(self):
        count = 128
        while True:
            buffer = self.ctypes.create_string_buffer(8 + count * self.ctypes.sizeof(self.ctypes.c_size_t))
            if self.kernel.QueryInformationJobObject(self.handle, 3, buffer, len(buffer), None):
                returned = self.ctypes.c_ulong.from_buffer(buffer, 4).value
                return list((self.ctypes.c_size_t * returned).from_buffer(buffer, 8))
            if self.ctypes.get_last_error() != 234:  # ERROR_MORE_DATA
                raise self.ctypes.WinError(self.ctypes.get_last_error())
            count *= 2

    def stop(self):
        if not self.kernel.TerminateJobObject(self.handle, 1):
            raise self.ctypes.WinError(self.ctypes.get_last_error())
        deadline = time.monotonic() + 5
        while self.pids():
            if time.monotonic() >= deadline:
                raise RuntimeError("Owned Windows job did not stop")
            time.sleep(0.02)
        self.close()

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


@dataclass
class OwnedHostProcess:
    process: subprocess.Popen
    created_at: float
    job: _WindowsJob | None = None
    stopped: bool = False

    @property
    def pid(self):
        return self.process.pid

    @property
    def stdout(self):
        return self.process.stdout

    def poll(self):
        return self.process.poll()

    def wait(self, timeout=None):
        return self.process.wait(timeout=timeout)

    def members(self):
        import psutil
        if self.stopped:
            return []
        if self.job is not None:
            members = []
            for pid in self.job.pids():
                try:
                    members.append(psutil.Process(pid))
                except psutil.NoSuchProcess:
                    pass
            return members
        try:
            if psutil.Process(self.pid).create_time() != self.created_at:
                raise RuntimeError("Host process ownership changed (PID reused)")
        except psutil.NoSuchProcess:
            pass
        # POSIX launch uses a new process group. A reused group-leader PID is
        # rejected above; psutil also rechecks each captured identity before kill.
        members = []
        for owner in psutil.process_iter():
            try:
                if os.name != "nt" and os.getpgid(owner.pid) == self.pid:
                    owner.create_time()
                    members.append(owner)
            except (psutil.NoSuchProcess, ProcessLookupError):
                continue
        return members


def _launch_host_process(command: list[str], **kwargs) -> OwnedHostProcess:
    import psutil

    job = _WindowsJob() if os.name == "nt" else None
    bootstrap = "import json,subprocess,sys; command=json.loads(sys.stdin.readline()); sys.exit(subprocess.call(command))"
    try:
        process = subprocess.Popen(
            [sys.executable, "-I", "-S", "-c", bootstrap], stdin=subprocess.PIPE,
            start_new_session=os.name != "nt", **kwargs,
        )
        created_at = psutil.Process(process.pid).create_time()
        if job is not None:
            job.assign(process)
        owned = OwnedHostProcess(process, created_at, job)
        payload = json.dumps(command) + "\n"
        process.stdin.write(payload if kwargs.get("text") else payload.encode())
        process.stdin.close()
        return owned
    except BaseException:
        if "process" in locals():
            process.terminate()
            process.wait(timeout=5)
        if job is not None:
            job.close()
        raise


@contextmanager
def start_installed_hosts(binding: ArtifactBinding, output: Path, *, environment: dict[str, str] | None = None) -> Iterator[dict[str, str]]:
    """Serve copied examples with the isolated wheel and packed React build."""
    app = binding.paths["app"]
    python = binding.paths["python"]
    react_app = app / "examples" / "flagship_react"
    npm = shutil.which("npm")
    if npm is None:
        raise RuntimeError("npm is required to serve the installed React build")
    output.mkdir(parents=True, exist_ok=True)
    child_env = dict(os.environ if environment is None else environment)
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
            bind_deadline = time.monotonic() + (10.0 if port else 0.0)
            while True:
                try:
                    listener.bind(("127.0.0.1", port))
                    break
                except OSError as error:
                    if port and time.monotonic() < bind_deadline:
                        time.sleep(0.25)
                        continue
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
            process = _launch_host_process(command, cwd=cwd, env=child_env, stdout=log, stderr=subprocess.STDOUT)
            stack.callback(_stop_process, process)
            (output / f"{name}.pid").write_text(str(process.pid), encoding="ascii")
            _wait_for(health, process)
        urls = {name: f"http://127.0.0.1:{ports[name]}" for name in ("dash", "react", "streamlit")}
        binding.host_origins.update(urls)
        yield urls


def _stop_process(process: OwnedHostProcess) -> None:
    import psutil

    if process.stopped:
        return
    if process.job is not None:
        process.job.stop()
        process.wait(timeout=5)
        process.stopped = True
        return
    owned = process.members()
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
    process.stopped = True
    if survivors:
        raise RuntimeError("Installed host process tree did not stop")


def resolve_loaded_asset(binding: ArtifactBinding, host: str, url: str) -> tuple[str, str]:
    """Resolve a served URL to its canonical bound path and expected SHA-256."""
    _verify_asset_origin(binding, host, url)
    path = urlsplit(url).path
    if path == "/@vite/client" or path.startswith("/@vite/"):
        raise ValueError("Vite development client is forbidden in a release host")
    canonical = path
    expected = binding.expected_loaded_assets.get(host, {}).get(canonical)
    if expected is None and host == "dash":
        from dash.fingerprint import check_fingerprint

        original_path, fingerprinted = check_fingerprint(path)
        if fingerprinted:
            canonical = original_path
            expected = binding.expected_loaded_assets.get(host, {}).get(canonical)
    if expected is None and host == "streamlit":
        # Streamlit serves wheel component files below its own versioned URL.
        # Their hashed filenames are unique within the installed build.
        matches = [
            (bound_path, digest) for bound_path, digest in binding.expected_loaded_assets.get(host, {}).items()
            if Path(bound_path).name == Path(path).name
        ]
        if len(matches) == 1:
            canonical, expected = matches[0]
    if expected is None:
        raise ValueError(f"unbound asset: {host} {path}")
    return canonical, expected


def verify_loaded_asset(binding: ArtifactBinding, host: str, url: str, body: bytes) -> str:
    """Reject an unrecorded or changed production asset returned by a host."""
    _, expected = resolve_loaded_asset(binding, host, url)
    actual = hashlib.sha256(body).hexdigest()
    if actual != expected:
        raise ValueError(f"asset hash mismatch: {host} {urlsplit(url).path}")
    return actual


def _verify_asset_origin(binding: ArtifactBinding, host: str, url: str) -> None:
    parsed = urlsplit(url)
    expected = binding.host_origins.get(host)
    if parsed.netloc:
        if not expected:
            raise ValueError(f"Missing installed host origin: {host}")
        wanted = urlsplit(expected)
        def origin(value):
            return value.scheme.lower(), value.hostname, value.port or {"http": 80, "https": 443}.get(value.scheme)
        if parsed.username or parsed.password or origin(parsed) != origin(wanted):
            raise ValueError(f"foreign origin for installed {host} asset")
    elif expected:
        raise ValueError(f"Installed host origin is required for {host} asset")


def load_artifact_binding(path: Path) -> ArtifactBinding:
    record = json.loads(path.read_text(encoding="utf-8"))
    assets = record["expectedLoadedAssets"]
    origins = record["hostOrigins"]
    if not all(assets.get(host) and origins.get(host) for host in ("dash", "react", "streamlit")):
        raise ValueError("Artifact binding omits a host's production assets")
    return ArtifactBinding(
        source_commit=record["sourceCommit"], sha256=record["sha256"],
        versions=record["versions"], expected_loaded_assets=assets, host_origins=origins,
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
            or host == "dash" and path.startswith("/_dash-component-suites/serverside_pivot/")
            or host == "streamlit" and (
                path.startswith("/_stcore/bidi-components/serverside-pivot.")
                or path.startswith("/_stcore/component/serverside-pivot/")
                or Path(path).name in {Path(name).name for name in binding.expected_loaded_assets[host]}
            )
        )
        if own_asset and path.endswith((".js", ".mjs", ".css")):
            _verify_asset_origin(binding, host, resource)
            response = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": params["requestId"]})
            body = base64.b64decode(response["body"]) if response.get("base64Encoded") else response["body"].encode()
            loaded[path] = verify_loaded_asset(binding, host, resource, body)
    return loaded
