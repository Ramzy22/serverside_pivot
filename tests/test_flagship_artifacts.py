"""Release artifact binding for the three installed flagship hosts."""

from __future__ import annotations

import hashlib
import email
import json
import os
import shutil
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import subprocess
import urllib.request
import zipfile
from contextlib import contextmanager
from pathlib import Path

import pytest

from scripts.flagship_artifacts import (
    ArtifactBinding,
    build_artifacts,
    start_installed_hosts,
    verify_loaded_asset,
    _snapshot,
)


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("requirement", ["psutil>=5.9", "sqlglot<30"])
def test_wheel_metadata_declares_engine_runtime_dependencies(tmp_path, requirement):
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10
        import tomli as tomllib
    from packaging.requirements import Requirement
    # Generate actual backend metadata from the production project config,
    # without npm/bundles or importing the engine into the test runner.
    package = tmp_path / "package"
    package.mkdir()
    for filename in ("pyproject.toml", "README.md", "setup.py"):
        shutil.copy2(ROOT / "serverside_pivot" / filename, package / filename)
    configuration = tomllib.loads((package / "pyproject.toml").read_text(encoding="utf-8"))
    for name in configuration["tool"]["setuptools"]["packages"]:
        relative = name.replace(".", "/")
        if name.startswith("pivot_engine"):
            relative = "pivot_engine/" + relative
        (package / relative).mkdir(parents=True, exist_ok=True)
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()
    subprocess.run([sys.executable, "-c", "from setuptools.build_meta import prepare_metadata_for_build_wheel; prepare_metadata_for_build_wheel(" + repr(str(metadata_dir)) + ")"],
                   cwd=package, check=True, capture_output=True, text=True)
    metadata = email.message_from_bytes(next(metadata_dir.glob("*.dist-info/METADATA")).read_bytes())
    dependencies = [Requirement(value) for value in metadata.get_all("Requires-Dist")]
    expected = Requirement(requirement)
    assert any(dependency.name == expected.name and dependency.marker is None
               and dependency.specifier == expected.specifier for dependency in dependencies)


@pytest.mark.skipif(not os.environ.get("PIVOT_FLAGSHIP_INSTALLED_PYTHON"), reason="isolated installed wheel is opt-in")
def test_installed_wheel_bootstraps_arrow_without_source_imports(tmp_path):
    python = Path(os.environ["PIVOT_FLAGSHIP_INSTALLED_PYTHON"]).resolve(strict=True)
    environment = python.parent.parent
    assert "include-system-site-packages = false" in (environment / "pyvenv.cfg").read_text().lower()
    # An empty cwd and isolated Python prevent the checkout from satisfying
    # imports; a one-row Arrow table catches memtable cleanup compatibility.
    completed = subprocess.run([str(python), "-I", "-c", """
import json
import pyarrow as pa
import pivot_engine
from pivot_engine import create_tanstack_adapter
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.controller.load_data_from_arrow("bootstrap", pa.table({"x": [1]}), register_checkpoint=False)
rows = adapter.controller.planner.con.table("bootstrap").to_pyarrow().to_pylist()
assert rows == [{"x": 1}], rows
print(json.dumps({"engine": pivot_engine.__file__, "rows": rows}))
"""], cwd=tmp_path, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    result = json.loads(completed.stdout)
    assert Path(result["engine"]).is_relative_to(environment)
    assert "site-packages" in Path(result["engine"]).parts


def _test_build_wheelhouse(path):
    path.mkdir()
    hashes = {}
    for name, version in (("setuptools", "84.0.0"), ("wheel", "0.45.1")):
        wheel = path / f"{name}-{version}-py3-none-any.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            prefix = f"{name}-{version}.dist-info/"
            archive.writestr(prefix + "METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\nRequires-Python: >=3.8\n")
            archive.writestr(prefix + "WHEEL", "Wheel-Version: 1.0\nTag: py3-none-any\n")
        hashes[wheel.name] = hashlib.sha256(wheel.read_bytes()).hexdigest()
    (path / "wheelhouse-sha256.json").write_text(json.dumps(hashes))
    return path


def test_build_wheelhouse_ignores_vendored_dependency_metadata(tmp_path):
    from scripts import flagship_artifacts
    wheelhouse = _test_build_wheelhouse(tmp_path / "wheelhouse")
    target = wheelhouse / "setuptools-84.0.0-py3-none-any.whl"
    with zipfile.ZipFile(target, "a") as archive:
        archive.writestr("setuptools/_vendor/packaging-26.0.dist-info/METADATA", "Name: packaging\nVersion: 26.0\n")
    manifest = wheelhouse / "wheelhouse-sha256.json"
    hashes = json.loads(manifest.read_text())
    hashes[target.name] = hashlib.sha256(target.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(hashes))
    output = tmp_path / "output"
    env = flagship_artifacts._prepare_build_wheelhouse(wheelhouse, ROOT / "serverside_pivot", output)
    assert (Path(env["PIP_FIND_LINKS"]) / target.name).read_bytes() == target.read_bytes()


@pytest.mark.parametrize("damage", ["missing", "altered", "manifest-missing", "metadata", "required-wheel-missing"])
def test_build_wheelhouse_rejects_missing_or_altered_requirements(tmp_path, damage):
    from scripts import flagship_artifacts
    wheelhouse = _test_build_wheelhouse(tmp_path / "wheelhouse")
    target = wheelhouse / "wheel-0.45.1-py3-none-any.whl"
    if damage in ("missing", "required-wheel-missing"):
        target.unlink()
        if damage == "required-wheel-missing":
            manifest = wheelhouse / "wheelhouse-sha256.json"
            hashes = json.loads(manifest.read_text())
            del hashes[target.name]
            manifest.write_text(json.dumps(hashes))
    elif damage == "altered":
        with target.open("ab") as stream:
            stream.write(b"altered after manifest")
    elif damage == "manifest-missing":
        (wheelhouse / "wheelhouse-sha256.json").unlink()
    else:
        with zipfile.ZipFile(target, "w") as archive:
            archive.writestr("wheel-0.45.1.dist-info/METADATA", "Name: foreign\nVersion: 0.45.1\n")
        hashes = json.loads((wheelhouse / "wheelhouse-sha256.json").read_text())
        hashes[target.name] = hashlib.sha256(target.read_bytes()).hexdigest()
        (wheelhouse / "wheelhouse-sha256.json").write_text(json.dumps(hashes))
    with pytest.raises(ValueError, match="wheelhouse"):
        flagship_artifacts._prepare_build_wheelhouse(wheelhouse, ROOT / "serverside_pivot", tmp_path / "output")


@pytest.mark.parametrize("offline", [False, True])
def test_build_wheelhouse_environment_does_not_leak_to_npm_or_runtime_install(tmp_path, monkeypatch, offline):
    from scripts import flagship_artifacts
    wheelhouse = _test_build_wheelhouse(tmp_path / "wheelhouse")
    package = tmp_path / "snapshot" / "serverside_pivot"
    package.mkdir(parents=True)
    (package / "pyproject.toml").write_text('[build-system]\nrequires=["setuptools>=61.0", "wheel"]\n')
    output = tmp_path / "output"
    output.mkdir()
    calls = []
    before = {name: os.environ.get(name) for name in ("PIP_NO_INDEX", "PIP_FIND_LINKS")}

    def command(command, cwd, *, env=None):
        calls.append((command, env))
        if command[1:4] == ["-m", "build", "--wheel"]:
            (output / "wheels" / "release.whl").write_bytes(b"built wheel")
        elif "pack" in command:
            (output / "release.tgz").write_bytes(b"packed npm")
        elif "pip" in command:
            raise RuntimeError("runtime installation reached")
        return ""

    def environment(path):
        (output / "python-env.location.json").write_text("{}")
        return tmp_path / "short" / "Scripts" / "python.exe"

    monkeypatch.setattr(flagship_artifacts, "_run", command)
    monkeypatch.setattr(flagship_artifacts, "_create_python_environment", environment)
    with pytest.raises(RuntimeError, match="runtime installation reached"):
        flagship_artifacts._build_from_snapshot("a" * 40, package.parent, output, build_wheelhouse=wheelhouse if offline else None)
    build = [env for command, env in calls if command[1:4] == ["-m", "build", "--wheel"]]
    assert len(build) == 1
    if offline:
        assert build[0]["PIP_NO_INDEX"] == "1"
        staged = Path(build[0]["PIP_FIND_LINKS"])
        assert staged != wheelhouse and staged.is_dir()
        for name, digest in json.loads((wheelhouse / "wheelhouse-sha256.json").read_text()).items():
            assert hashlib.sha256((staged / name).read_bytes()).hexdigest() == digest
        record = json.loads((output / "build-wheelhouse.json").read_text())
        assert record["sha256"] == json.loads((wheelhouse / "wheelhouse-sha256.json").read_text())
    else:
        assert build[0] is None
        assert not (output / "build-wheelhouse.json").exists()
    assert all(env is None for command, env in calls if command[1:4] != ["-m", "build", "--wheel"])
    assert {name: os.environ.get(name) for name in before} == before


def test_release_command_environment_is_subprocess_local(tmp_path, monkeypatch):
    from scripts.flagship_artifacts import _run
    monkeypatch.delenv("PIVOT_TEST_BUILD_ENV", raising=False)
    assert _run([sys.executable, "-c", "import os; print(os.environ['PIVOT_TEST_BUILD_ENV'])"], tmp_path,
                env={**os.environ, "PIVOT_TEST_BUILD_ENV": "build-only"}) == "build-only"
    assert "PIVOT_TEST_BUILD_ENV" not in os.environ


def test_installed_cli_passes_wheelhouse_only_to_artifact_build(tmp_path, monkeypatch):
    from scripts import certify_flagship, flagship_artifacts
    wheelhouse = _test_build_wheelhouse(tmp_path / "wheelhouse")
    def build(source, output, *, build_wheelhouse=None):
        assert build_wheelhouse == wheelhouse
        raise RuntimeError("build option forwarded")
    monkeypatch.setattr(flagship_artifacts, "build_artifacts", build)
    with pytest.raises(RuntimeError, match="build option forwarded"):
        certify_flagship.main(["--installed-release", "--build-wheelhouse", str(wheelhouse), "--artifact-dir", str(tmp_path / "evidence")])


def test_build_wheelhouse_cli_rejects_legacy_mode(tmp_path):
    from scripts import certify_flagship
    assert certify_flagship.main(["--build-wheelhouse", str(tmp_path), "--artifact-dir", str(tmp_path / "evidence")]) == 1
    assert not (tmp_path / "evidence").exists()


def test_failed_release_command_retains_stdout_and_stderr(tmp_path, capsys):
    from scripts.flagship_artifacts import _run
    with pytest.raises(subprocess.CalledProcessError):
        _run([sys.executable, "-c", "import sys; print('private stdout proof'); print('private stderr proof',file=sys.stderr); sys.exit(3)"], tmp_path)
    diagnostic = capsys.readouterr().err
    assert "private stdout proof" in diagnostic
    assert "private stderr proof" in diagnostic


def test_source_snapshot_preserves_commit_bytes_despite_git_autocrlf(tmp_path: Path) -> None:
    source = tmp_path / "repository"
    source.mkdir()
    subprocess.run(["git", "init", str(source)], check=True, capture_output=True)
    for key, value in (("user.name", "Artifact Test"), ("user.email", "artifact-test@example.invalid"), ("core.autocrlf", "true")):
        subprocess.run(["git", "config", key, value], cwd=source, check=True)
    (source / "fixture.json").write_bytes(b'{"row":1}\n{"row":2}\n')
    subprocess.run(["git", "add", "fixture.json"], cwd=source, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "fixture"], cwd=source, check=True, capture_output=True)
    output = tmp_path / "snapshot"
    output.mkdir()
    commit, snapshot = _snapshot(source, output)
    assert (snapshot / "fixture.json").read_bytes() == b'{"row":1}\n{"row":2}\n'
    assert commit == subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()


def test_loaded_asset_rejects_bytes_other_than_the_bound_release(tmp_path: Path) -> None:
    asset = tmp_path / "assets" / "entry.js"
    asset.parent.mkdir()
    asset.write_bytes(b"known production bundle")
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    binding = ArtifactBinding(expected_loaded_assets={"react": {"/assets/entry.js": digest}})

    assert verify_loaded_asset(binding, "react", "/assets/entry.js", b"known production bundle") == digest
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_loaded_asset(binding, "react", "/assets/entry.js", b"changed bundle")


def test_loaded_asset_rejects_vite_development_client() -> None:
    binding = ArtifactBinding(expected_loaded_assets={"react": {"/assets/entry.js": "0" * 64}})
    with pytest.raises(ValueError, match="Vite development client"):
        verify_loaded_asset(binding, "react", "/@vite/client", b"dev")


def test_loaded_asset_requires_a_recorded_path() -> None:
    binding = ArtifactBinding(expected_loaded_assets={"react": {"/assets/entry.js": "0" * 64}})
    with pytest.raises(ValueError, match="unbound asset"):
        verify_loaded_asset(binding, "react", "/assets/other.js", b"anything")


def test_dash_fingerprinted_asset_verifies_the_wheel_bytes() -> None:
    digest = hashlib.sha256(b"installed Dash component").hexdigest()
    binding = ArtifactBinding(expected_loaded_assets={
        "dash": {"/_dash-component-suites/serverside_pivot/serverside_pivot.min.js": digest},
    })
    url = "/_dash-component-suites/serverside_pivot/serverside_pivot.v0_0_88mabc123.min.js"
    assert verify_loaded_asset(binding, "dash", url, b"installed Dash component") == digest
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_loaded_asset(binding, "dash", url, b"stale component")


def test_streamlit_component_url_verifies_the_wheel_asset_bytes() -> None:
    digest = hashlib.sha256(b"installed component").hexdigest()
    binding = ArtifactBinding(expected_loaded_assets={
        "streamlit": {"/streamlit_pivot/frontend/build/pivot-workspace-123.js": digest},
    })
    assert verify_loaded_asset(
        binding, "streamlit", "/_stcore/component/serverside-pivot/pivot-workspace-123.js", b"installed component"
    ) == digest
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_loaded_asset(
            binding, "streamlit", "/_stcore/component/serverside-pivot/pivot-workspace-123.js", b"changed"
        )


@pytest.mark.parametrize("host,canonical,alias", [
    ("dash", "/_dash-component-suites/serverside_pivot/serverside_pivot.min.js",
     "/_dash-component-suites/serverside_pivot/serverside_pivot.v0_0_88mabc123.min.js"),
    ("streamlit", "/streamlit_pivot/frontend/build/pivot-workspace-123.js",
     "/_stcore/component/serverside-pivot/pivot-workspace-123.js"),
])
def test_asset_resolver_returns_canonical_paths_and_enforces_the_existing_origin_gate(host, canonical, alias):
    from scripts.flagship_artifacts import resolve_loaded_asset

    body = b"bound release component"
    digest = hashlib.sha256(body).hexdigest()
    binding = ArtifactBinding(expected_loaded_assets={host: {canonical: digest}},
        host_origins={host: "http://127.0.0.1:39001"})
    for path in (canonical, alias):
        url = "http://127.0.0.1:39001" + path
        assert resolve_loaded_asset(binding, host, url) == (canonical, digest)
        assert verify_loaded_asset(binding, host, url, body) == digest
    with pytest.raises(ValueError, match="foreign origin"):
        resolve_loaded_asset(binding, host, "http://foreign.invalid" + alias)


@pytest.mark.parametrize("conflicting_flag", ["--reuse-running", "--skip-build", "--export-qualification"])
def test_installed_release_rejects_flags_that_bypass_its_fresh_build(tmp_path: Path, conflicting_flag: str) -> None:
    from scripts import certify_flagship

    assert certify_flagship.main([
        "--installed-release", conflicting_flag, "--artifact-dir", str(tmp_path / "evidence"),
    ]) == 1
    assert not (tmp_path / "evidence").exists()


@pytest.fixture
def mocked_browser_harness(tmp_path, monkeypatch):
    """Isolate orchestration tests whose artifact build and browser are mocked.

    Committed archive verification is exercised with real repositories in
    test_flagship_shared_client; fake build bindings cannot satisfy that check.
    """
    from scripts import flagship_harness

    @contextmanager
    def harness(binding, evidence_root):
        yield flagship_harness.BrowserHarness(tmp_path, evidence_root / "browser-harness.json")

    monkeypatch.setattr(flagship_harness, "committed_browser_harness", harness)


def test_installed_release_binds_browser_environment_and_preserves_failure(tmp_path: Path, monkeypatch, mocked_browser_harness) -> None:
    from scripts import certify_flagship
    from scripts import flagship_artifacts

    binding = ArtifactBinding(source_commit="a" * 40, sha256={"fixture": "b" * 64})
    monkeypatch.setattr(flagship_artifacts, "build_artifacts", lambda source, output: binding)

    @contextmanager
    def installed_hosts(actual_binding, output, **kwargs):
        assert actual_binding is binding
        yield {"dash": "http://127.0.0.1:39001", "react": "http://127.0.0.1:39002", "streamlit": "http://127.0.0.1:8502"}

    monkeypatch.setattr(flagship_artifacts, "start_installed_hosts", installed_hosts)

    def browser_suite(evidence, environment, **kwargs):
        assert environment["PIVOT_REQUIRE_BROWSER_EVIDENCE"] == "1"
        assert environment["PIVOT_FLAGSHIP_REACT_URL"] == "http://127.0.0.1:39002"
        assert environment["PIVOT_FLAGSHIP_DASH_URL"] == "http://127.0.0.1:39001"
        assert environment["PIVOT_FLAGSHIP_STREAMLIT_URL"] == "http://127.0.0.1:8502"
        assert "PYTHONPATH" not in environment
        record = json.loads(Path(environment["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]).read_text())
        assert record["sourceCommit"] == "a" * 40
        assert record["sha256"]["fixture"] == "b" * 64
        assert "paths" not in record
        return 7

    monkeypatch.setattr(certify_flagship, "_run_browser_suite", browser_suite)
    evidence = tmp_path / "evidence"
    assert certify_flagship.main(["--installed-release", "--artifact-dir", str(evidence)]) == 7
    assert json.loads((evidence / "flagship-certification.json").read_text())["result"] == "failed"


@pytest.mark.parametrize("report", [None, '<testsuite><testcase name="browser"><skipped/></testcase></testsuite>', '<testsuite/>'])
def test_installed_release_rejects_zero_exit_with_skipped_missing_or_empty_browser_report(tmp_path, monkeypatch, report, mocked_browser_harness):
    from scripts import certify_flagship, flagship_artifacts

    monkeypatch.setattr(flagship_artifacts, "build_artifacts", lambda source, output: ArtifactBinding())
    @contextmanager
    def hosts(binding, output, **kwargs):
        yield {"dash": "http://127.0.0.1:1", "react": "http://127.0.0.1:2", "streamlit": "http://127.0.0.1:8502"}
    monkeypatch.setattr(flagship_artifacts, "start_installed_hosts", hosts)
    def suite(evidence, environment, **kwargs):
        if report is not None:
            (kwargs.get("junit_path") or evidence / "installed-browser.junit.xml").write_text(report)
        return 0
    monkeypatch.setattr(certify_flagship, "_run_browser_suite", suite)
    evidence = tmp_path / "evidence"
    assert certify_flagship.main(["--installed-release", "--artifact-dir", str(evidence)]) == 1
    assert json.loads((evidence / "flagship-certification.json").read_text())["result"] == "failed"


@pytest.mark.parametrize("browser_code", [0, 7])
def test_installed_release_records_cleanup_failure_as_failed(tmp_path, monkeypatch, browser_code, mocked_browser_harness):
    from scripts import certify_flagship, flagship_artifacts
    binding = ArtifactBinding(paths={"environment": tmp_path / "owned-env",
                                    "environment_record": tmp_path / "owned-env.location.json"})
    monkeypatch.setattr(flagship_artifacts, "build_artifacts", lambda source, output: binding)
    @contextmanager
    def hosts(binding, output, **kwargs):
        yield {"dash": "http://127.0.0.1:1", "react": "http://127.0.0.1:2", "streamlit": "http://127.0.0.1:8502"}
    monkeypatch.setattr(flagship_artifacts, "start_installed_hosts", hosts)
    def suite(evidence, environment, **kwargs):
        names = ("test_flagship_startup_navigation_and_controls", "test_flagship_copy_current_table_all_records_and_export_xlsx", "test_flagship_sidebar_filter_report_and_chart_entrypoints")
        kwargs["junit_path"].write_text("<testsuite>" + "".join(f'<testcase name="{name}[{host}]"/>' for name in names for host in ("dash", "react", "streamlit")) + "</testsuite>")
        return browser_code
    monkeypatch.setattr(certify_flagship, "_run_browser_suite", suite)
    def cleanup(binding):
        raise RuntimeError("owned environment cleanup failed")
    monkeypatch.setattr(flagship_artifacts, "cleanup_python_environment", cleanup)
    evidence = tmp_path / "evidence"
    assert certify_flagship.main(["--installed-release", "--artifact-dir", str(evidence)]) == (browser_code or 1)
    assert json.loads((evidence / "flagship-certification.json").read_text())["result"] == "failed"
    diagnostic, = (evidence / "private/runs").glob("*/cleanup-error.json")
    assert json.loads(diagnostic.read_text())["reason"] == "owned environment cleanup failed"


@pytest.mark.parametrize("browser_code", [0, 7])
@pytest.mark.parametrize("evidence_error", ["unknown-encoding", "unsupported-encoding", "aggregate-write", "cleanup-diagnostic-write"])
def test_installed_release_always_cleans_owned_environment_when_reporting_fails(
    tmp_path, monkeypatch, browser_code, evidence_error, mocked_browser_harness,
):
    import xml.etree.ElementTree as ET
    from scripts import certify_flagship, flagship_artifacts

    binding = ArtifactBinding(paths={"environment": tmp_path / "owned-env",
                                    "environment_record": tmp_path / "owned-env.location.json"})
    monkeypatch.setattr(flagship_artifacts, "build_artifacts", lambda source, output: binding)
    events = []

    @contextmanager
    def hosts(binding, output, **kwargs):
        try:
            yield {"dash": "http://127.0.0.1:1", "react": "http://127.0.0.1:2", "streamlit": "http://127.0.0.1:8502"}
        finally:
            events.append("hosts-stopped")

    def browser(evidence, environment, **kwargs):
        report = kwargs["junit_path"]
        if evidence_error in {"unknown-encoding", "unsupported-encoding"}:
            encoding = "invalid-encoding" if evidence_error == "unknown-encoding" else "UTF-7"
            report.write_bytes(f'<?xml version="1.0" encoding="{encoding}"?><testsuite/>'.encode("ascii"))
        else:
            report.write_text('<testsuite><testcase name="observed"/></testsuite>')
        return browser_code

    original_xml_write = ET.ElementTree.write

    def xml_write(tree, destination, *args, **kwargs):
        if evidence_error == "aggregate-write" and Path(destination).name == "installed-browser.junit.xml":
            events.append("aggregate-write-failed")
            raise OSError("aggregate output unavailable")
        return original_xml_write(tree, destination, *args, **kwargs)

    original_text_write = Path.write_text

    def text_write(path, *args, **kwargs):
        if evidence_error == "cleanup-diagnostic-write" and path.name == "cleanup-error.json":
            raise OSError("cleanup diagnostic unavailable")
        if path.name == "read-only-qualification.raw.json":
            events.append("final-report")
        return original_text_write(path, *args, **kwargs)

    def cleanup(actual):
        assert actual is binding
        events.append("cleanup")
        if evidence_error == "cleanup-diagnostic-write":
            raise RuntimeError("cleanup failed")

    monkeypatch.setattr(flagship_artifacts, "start_installed_hosts", hosts)
    monkeypatch.setattr(flagship_artifacts, "cleanup_python_environment", cleanup)
    monkeypatch.setattr(certify_flagship, "_run_browser_suite", browser)
    monkeypatch.setattr(ET.ElementTree, "write", xml_write)
    monkeypatch.setattr(Path, "write_text", text_write)
    evidence = tmp_path / "evidence"
    assert certify_flagship.main(["--installed-release", "--artifact-dir", str(evidence)]) == (browser_code or 1)
    assert events.count("cleanup") == 1
    assert events.index("hosts-stopped") < events.index("cleanup") < events.index("final-report")
    raw, = (evidence / "private/runs").glob("*/read-only-qualification.raw.json")
    report = json.loads(raw.read_text())
    assert report["releaseResult"] == "failed"
    issue = "junit_invalid" if evidence_error in {"unknown-encoding", "unsupported-encoding"} else "qualification_incomplete"
    assert issue in report["systemIssues"]["product"]


@pytest.mark.parametrize("body", [b"good release", b"altered release", None])
def test_certification_suite_checks_actual_loaded_assets_before_saving_evidence(tmp_path, body):
    from tests import test_flagship_certification as suite
    digest = hashlib.sha256(b"good release").hexdigest()

    class Browser:
        _pivot_artifact_binding = ArtifactBinding(expected_loaded_assets={"react": {"/assets/index.js": digest}}, host_origins={"react": "http://127.0.0.1:39002"})
        _pivot_artifact_host = "react"
        _pivot_loaded_assets = {}
        def get_log(self, kind):
            if kind != "performance" or body is None:
                return []
            return [{"message": json.dumps({"message": {"method": "Network.responseReceived", "params": {
                "requestId": "asset-1", "type": "Script", "response": {
                    "url": "http://127.0.0.1:39002/assets/index.js", "status": 200, "mimeType": "text/javascript",
                },
            }}})}]
        def execute_cdp_cmd(self, command, arguments):
            assert command == "Network.getResponseBody" and arguments == {"requestId": "asset-1"}
            return {"body": body.decode(), "base64Encoded": False}
        def execute_script(self, script):
            return None
        def save_screenshot(self, path):
            return True

    if body == b"good release":
        suite._save_evidence(Browser(), tmp_path, "asset-consumer")
        record = json.loads((tmp_path / "asset-consumer.network.json").read_text())
        assert record["loadedAssetSha256"] == {"/assets/index.js": digest}
    else:
        with pytest.raises(ValueError, match="hash mismatch|No installed .*assets"):
            suite._save_evidence(Browser(), tmp_path, "asset-consumer")


def test_installed_hosts_reject_occupied_streamlit_port_before_launch(tmp_path, monkeypatch):
    from scripts import flagship_artifacts
    with socket.socket() as listener:
        if os.name == "nt":
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        listener.bind(("127.0.0.1", 8502))
        listener.listen()
        def forbidden_launch(*args, **kwargs):
            raise AssertionError("Launched a host before rejecting the foreign listener")
        monkeypatch.setattr(flagship_artifacts.subprocess, "Popen", forbidden_launch)
        binding = ArtifactBinding(paths={"app": tmp_path, "python": Path(sys.executable)})
        with pytest.raises(RuntimeError, match="occupied.*8502"):
            with start_installed_hosts(binding, tmp_path / "hosts"):
                pass
        assert not list((tmp_path / "hosts").glob("*.pid"))


def test_health_200_from_foreign_process_cannot_satisfy_installed_host_startup():
    from scripts.flagship_artifacts import _wait_for
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        with pytest.raises(RuntimeError, match="not owned"):
            _wait_for(f"http://127.0.0.1:{server.server_port}", process, timeout=2)
    finally:
        process.terminate()
        process.wait(timeout=5)
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_release_environment_does_not_inherit_global_distribution_metadata(tmp_path):
    from scripts import flagship_artifacts
    environment = tmp_path / "isolated-env"
    python = flagship_artifacts._create_python_environment(environment)
    try:
        installed_pip = subprocess.check_output([str(python), "-c", "import importlib.metadata; print(importlib.metadata.version('pip'))"], text=True).strip()
        assert installed_pip == "26.2"
        provenance = json.loads((tmp_path / "isolated-env.location.json").read_text())
        assert provenance["pipBootstrap"]["version"] == installed_pip
        assert provenance["pipBootstrap"]["sha256"] == "931c303696af6fa3417112103b1cad26890e5a07eccb5b99783700e33f2b8aad"
        paths = json.loads(subprocess.check_output([str(python), "-c", "import json,sys; print(json.dumps(sys.path))"], text=True))
        import sysconfig
        assert str(Path(sysconfig.get_path("purelib")).resolve()) not in {str(Path(path).resolve()) for path in paths if path}
    finally:
        flagship_artifacts.cleanup_python_environment(ArtifactBinding(paths={
            "python": python, "environment": python.parent.parent,
            "environment_record": tmp_path / "isolated-env.location.json",
        }))


@pytest.mark.skipif(os.name != "nt", reason="Windows installer path bound")
def test_short_environment_installs_long_wheel_member_and_records_cleanup(tmp_path):
    from scripts import flagship_artifacts
    nested = tmp_path / "nested-evidence-directory-for-path-regression" / "release"
    nested.mkdir(parents=True)
    member = "path_probe/backends/impala/tests/snapshots/test_bucket_histogram/test_bucket_to_case/close_extreme_false_include_under_include_over/snapshot.txt"
    assert len(str(nested / "python-env/Lib/site-packages" / member)) > 260
    wheel = tmp_path / "path_probe-0.0.0-py3-none-any.whl"
    metadata = "path_probe-0.0.0.dist-info"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(member, b"installed long-path snapshot")
        archive.writestr(metadata + "/METADATA", "Metadata-Version: 2.1\nName: path-probe\nVersion: 0.0.0\n")
        archive.writestr(metadata + "/WHEEL", "Wheel-Version: 1.0\nGenerator: path-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        archive.writestr(metadata + "/RECORD", "\n".join(name + ",," for name in archive.namelist()))
    python = flagship_artifacts._create_python_environment(nested / "python-env")
    record = nested / "python-env.location.json"
    binding = ArtifactBinding(paths={"python": python, "environment": python.parent.parent, "environment_record": record})
    try:
        subprocess.run([str(python), "-m", "pip", "--isolated", "install", "--no-index", "--no-deps", str(wheel)], check=True, capture_output=True, text=True)
        installed = python.parent.parent / "Lib/site-packages" / member
        assert installed.read_bytes() == b"installed long-path snapshot"
        assert len(str(installed)) < 260
        assert json.loads(record.read_text())["environment"] == str(python.parent.parent)
        assert not python.parent.parent.is_relative_to(nested)
        flagship_artifacts.cleanup_python_environment(binding)
        assert not python.parent.parent.exists()
        assert json.loads(record.read_text())["state"] == "cleaned"
    finally:
        if hasattr(flagship_artifacts, "cleanup_python_environment") and python.parent.parent.exists():
            flagship_artifacts.cleanup_python_environment(binding)


def test_environment_cleanup_rejects_unowned_directory(tmp_path):
    from scripts import flagship_artifacts
    unrelated = tmp_path / "user-data"
    unrelated.mkdir()
    (unrelated / "keep.txt").write_text("preserve unrelated data")
    binding = ArtifactBinding(paths={"python": unrelated / "Scripts/python.exe", "environment": unrelated, "environment_record": tmp_path / "fake.json"})
    with pytest.raises(ValueError, match="owned"):
        flagship_artifacts.cleanup_python_environment(binding)
    assert (unrelated / "keep.txt").read_text() == "preserve unrelated data"


@pytest.mark.parametrize("failure", ["download", "hash", "install", "version", "record"])
def test_release_bootstrap_failure_cleans_only_allocated_environment(tmp_path, monkeypatch, failure):
    import io
    from types import SimpleNamespace
    from scripts import flagship_artifacts

    # Exercise real allocation, ownership records, and guarded cleanup. Only
    # the slow interpreter seed and external downloader/installer are replaced.
    monkeypatch.setattr(flagship_artifacts.venv, "EnvBuilder", lambda **kwargs: SimpleNamespace(create=lambda root: None))
    wheel_bytes = b"test installer download"
    if failure != "hash":
        monkeypatch.setattr(flagship_artifacts, "_RELEASE_PIP_SHA256", hashlib.sha256(wheel_bytes).hexdigest())
    def download(*args, **kwargs):
        if failure == "download":
            raise urllib.error.URLError("bootstrap unavailable")
        return io.BytesIO(wheel_bytes)
    def install(command, cwd, **kwargs):
        if "install" in command and failure == "install":
            raise subprocess.CalledProcessError(1, command)
        return "unexpected-version" if failure == "version" else "26.2"
    original_write = Path.write_text
    def write_record(path, text, *args, **kwargs):
        if failure == "record" and path.name == "failed-env.location.json" and json.loads(text)["state"] == "ready":
            raise OSError("bootstrap provenance unavailable")
        return original_write(path, text, *args, **kwargs)
    monkeypatch.setattr(flagship_artifacts.urllib.request, "urlopen", download)
    monkeypatch.setattr(flagship_artifacts, "_run", install)
    monkeypatch.setattr(Path, "write_text", write_record)
    expected = {"download": urllib.error.URLError, "hash": ValueError,
                "install": subprocess.CalledProcessError, "version": RuntimeError, "record": OSError}[failure]
    unrelated = tmp_path / "keep.txt"
    unrelated.write_text("unrelated user data")
    with pytest.raises(expected):
        flagship_artifacts._create_python_environment(tmp_path / "failed-env")
    record = json.loads((tmp_path / "failed-env.location.json").read_text())
    assert record["state"] == "cleaned"
    assert not Path(record["environment"]).exists()
    assert Path(record["environment"]) not in flagship_artifacts._OWNED_ENVIRONMENTS
    assert unrelated.read_text() == "unrelated user data"


@pytest.mark.parametrize("url", ["http://127.0.0.1:39002/assets/index.js", "http://127.0.0.1:39003/assets/index.js", "https://foreign.example/assets/index.js"])
def test_loaded_asset_requires_bound_host_origin_even_when_bytes_match(url):
    body = b"exact release bytes"
    binding = ArtifactBinding(expected_loaded_assets={"react": {"/assets/index.js": hashlib.sha256(body).hexdigest()}}, host_origins={"react": "http://127.0.0.1:39002"})
    if url.startswith("http://127.0.0.1:39002/"):
        assert verify_loaded_asset(binding, "react", url, body) == hashlib.sha256(body).hexdigest()
    else:
        with pytest.raises(ValueError, match="foreign origin"):
            verify_loaded_asset(binding, "react", url, body)


def test_host_cleanup_retains_ownership_after_launcher_exits():
    import psutil
    from scripts import flagship_artifacts
    command = [sys.executable, "-c", "import subprocess,sys; child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)']); print(child.pid,flush=True)"]
    owned = flagship_artifacts._launch_host_process(command, stdout=subprocess.PIPE, text=True)
    child = psutil.Process(int(owned.stdout.readline()))
    try:
        owned.wait(timeout=10)
        assert child.is_running()
        flagship_artifacts._stop_process(owned)
        # Job termination and psutil process visibility need not be atomic.
        gone, alive = psutil.wait_procs([child], timeout=5)
        assert child in gone and not alive
    finally:
        if child.is_running():
            child.kill()
            child.wait(timeout=5)
        owned.stdout.close()


def test_cleanup_refuses_reused_pid_and_preserves_unrelated_process():
    import psutil
    from scripts import flagship_artifacts
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        stale = flagship_artifacts.OwnedHostProcess(unrelated, psutil.Process(unrelated.pid).create_time() - 1)
        with pytest.raises(RuntimeError, match="ownership"):
            flagship_artifacts._stop_process(stale)
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_host_cleanup_terminates_spawned_descendants():
    import psutil
    from scripts.flagship_artifacts import _stop_process, _launch_host_process
    process = _launch_host_process([sys.executable, "-c", (
        "import subprocess,sys,time; "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)']); "
        "print(child.pid,flush=True); time.sleep(120)"
    )], stdout=subprocess.PIPE, text=True)
    child = psutil.Process(int(process.stdout.readline()))
    try:
        _stop_process(process)
        with pytest.raises(psutil.NoSuchProcess):
            child.status()
    finally:
        if child.is_running():
            child.kill()
            child.wait(timeout=5)
        if process.poll() is None:
            _stop_process(process)
        process.stdout.close()


def _assert_browser_loaded_bound_assets(binding: ArtifactBinding, urls: dict[str, str], evidence: Path) -> None:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from tests.test_flagship_certification import _save_evidence

    options = Options()
    for argument in ("--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-background-networking", "--disable-component-update", "--window-size=1680,1200"):
        options.add_argument(argument)
    options.set_capability("goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"})
    observations = {}
    for host, url in urls.items():
        driver = webdriver.Chrome(options=options)
        try:
            driver._pivot_artifact_binding = binding
            driver._pivot_artifact_host = host
            driver._pivot_loaded_assets = {}
            driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
            driver.get(url)
            cells = WebDriverWait(driver, 45).until(lambda browser: browser.execute_script("""
                const find = root => {
                    const cells = [...root.querySelectorAll('[role="gridcell"]')];
                    for (const element of root.querySelectorAll('*')) {
                        if (element.shadowRoot) cells.push(...find(element.shadowRoot));
                    }
                    return cells;
                };
                return find(document).map(cell => cell.textContent).filter(text => text.trim());
            """))
            _save_evidence(driver, evidence, f"{host}-installed-assets")
            loaded = driver._pivot_loaded_assets
            observations[host] = {"browserVersion": driver.capabilities["browserVersion"], "cellCount": len(cells), "loadedAssetSha256": loaded}
            (evidence / "browser-assets.json").write_text(json.dumps({
                "sourceCommit": binding.source_commit, "sha256": binding.sha256, "versions": binding.versions,
                "observations": observations,
            }, indent=2), encoding="utf-8")
        finally:
            driver.quit()
    (evidence / "browser-assets.json").write_text(json.dumps({
        "sourceCommit": binding.source_commit, "sha256": binding.sha256, "versions": binding.versions,
        "observations": observations,
    }, indent=2), encoding="utf-8")


@pytest.mark.skipif(os.environ.get("PIVOT_RUN_ARTIFACT_TESTS") != "1", reason="real release build is opt-in")
def test_clean_artifacts_serve_installed_hosts_and_match_loaded_bytes(tmp_path: Path) -> None:
    wheelhouse = os.environ.get("PIVOT_FLAGSHIP_BUILD_WHEELHOUSE")
    binding = build_artifacts(ROOT, tmp_path / "release", build_wheelhouse=Path(wheelhouse) if wheelhouse else None)
    assert binding.source_commit and len(binding.source_commit) == 40
    assert binding.sha256["wheel"] and binding.sha256["npm_tarball"]
    committed_fixture = subprocess.check_output(
        ["git", "show", f"{binding.source_commit}:tests/data/flagship_parity_rows.json"], cwd=ROOT,
    )
    assert binding.sha256["fixture"] == hashlib.sha256(committed_fixture).hexdigest()
    assert binding.expected_loaded_assets["dash"]
    assert binding.expected_loaded_assets["react"]
    assert binding.expected_loaded_assets["streamlit"]

    with zipfile.ZipFile(binding.paths["wheel"]) as wheel:
        names = wheel.namelist()
        assert any(name.startswith("streamlit_pivot/frontend/build/") and name.endswith(".js") for name in names)
        assert any(name.startswith("serverside_pivot/") and name.endswith(".min.js") for name in names)

    installed_origin = subprocess.check_output(
        [str(binding.paths["python"]), "-c", "import streamlit_pivot; print(streamlit_pivot.__file__)"],
        cwd=binding.paths["app"], text=True,
    ).strip()
    assert str(binding.paths["app"]) not in installed_origin
    assert "site-packages" in installed_origin
    installed_streamlit = Path(installed_origin).parent
    for bound_path, digest in binding.expected_loaded_assets["streamlit"].items():
        installed_asset = installed_streamlit / bound_path.removeprefix("/streamlit_pivot/")
        assert installed_asset.is_file()
        assert hashlib.sha256(installed_asset.read_bytes()).hexdigest() == digest

    with start_installed_hosts(binding, tmp_path / "hosts") as urls:
        for host in ("dash", "react", "streamlit"):
            with urllib.request.urlopen(urls[host], timeout=10) as response:
                assert response.status == 200
        with urllib.request.urlopen(urls["react"], timeout=10) as response:
            index = response.read()
        assert b"/@vite/client" not in index
        dash_path = next(path for path in binding.expected_loaded_assets["dash"] if path.endswith(".js"))
        with urllib.request.urlopen(urls["dash"] + dash_path, timeout=10) as response:
            verify_loaded_asset(binding, "dash", urls["dash"] + dash_path, response.read())
        react_path = next(path for path in binding.expected_loaded_assets["react"] if path.endswith(".js"))
        with urllib.request.urlopen(urls["react"] + react_path, timeout=10) as response:
            verify_loaded_asset(binding, "react", urls["react"] + react_path, response.read())
        with urllib.request.urlopen(urls["react"] + "/v1/workspace-config", timeout=10) as response:
            assert json.loads(response.read())["table"]
        _assert_browser_loaded_bound_assets(binding, urls, tmp_path)
    from scripts.flagship_artifacts import cleanup_python_environment
    cleanup_python_environment(binding)
