"""Built-artifact acceptance checks for every published flagship host.

The browser cases are intentionally opt-in for normal unit-test runs.  The
release command enables them after building and launching the three examples.
They are never a substitute for their lower-level runtime tests: these cases
verify what a user can actually copy, download, scroll, and control.
"""

from __future__ import annotations

import importlib.util
import json
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from tests.flagship_capabilities import CAPABILITIES, HOSTS, capability_ids, markdown_matrix
from tests.export_qualification_fixture import (
    FLAGSHIP_COL_FIELDS,
    FLAGSHIP_FILTER,
    FLAGSHIP_QUALIFICATION_SOURCE_ROWS,
    FLAGSHIP_ROW_FIELDS,
    FLAGSHIP_VALUE_CONFIGS,
    assert_flagship_clipboard,
    assert_flagship_download,
)


ROOT = Path(__file__).resolve().parents[1]
RUN_BROWSER_CERTIFICATION = os.environ.get("PIVOT_RUN_FLAGSHIP_BROWSER_TESTS") == "1"
RUN_EXPORT_QUALIFICATION = os.environ.get("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA") == "1"
EXPORT_QUALIFICATION_PHASE = os.environ.get("PIVOT_EXPORT_QUALIFICATION_PHASE", "")
REQUIRE_BROWSER_EVIDENCE = os.environ.get("PIVOT_REQUIRE_BROWSER_EVIDENCE") == "1"
ARTIFACT_ROOT = Path(os.environ.get("PIVOT_FLAGSHIP_ARTIFACT_DIR", ROOT / "artifacts" / "flagship-certification"))
_EXPORT_RESPONSE_PROBE_SCRIPT = """
if (!window.__PIVOT_EXPORT_PROBE_INSTALLED__) {
    window.__PIVOT_EXPORT_PROBE_INSTALLED__ = true;
    window.__PIVOT_EXPORT_PROOFS__ = [];
    const requestIds = new Set();
    window.__PIVOT_EXPORT_REQUEST_IDS__ = requestIds;
    const parsed = value => {
        if (typeof value !== 'string') return value;
        try { return JSON.parse(value); } catch (_) { return null; }
    };
    const visitRequest = rawValue => {
        const value = parsed(rawValue);
        if (!value || typeof value !== 'object') return;
        if (Array.isArray(value)) { value.forEach(visitRequest); return; }
        if (
            value.kind === 'export'
            && typeof value.requestId === 'string'
            && value.requestId.length > 0
            && value.payload
            && typeof value.payload === 'object'
            && !Array.isArray(value.payload)
        ) {
            requestIds.add(value.requestId);
        }
        Object.values(value).forEach(visitRequest);
    };
    const visitResponse = rawValue => {
        const value = parsed(rawValue);
        if (!value || typeof value !== 'object') return;
        if (Array.isArray(value)) { value.forEach(visitResponse); return; }
        if (
            value.schemaVersion === 1
            && value.kind === 'export'
            && value.status === 'error'
            && typeof value.requestId === 'string'
            && requestIds.has(value.requestId)
            && value.payload
            && typeof value.payload === 'object'
            && !Array.isArray(value.payload)
            && ['PIVOT_EXPORT_MAX_ROWS', 'PIVOT_EXPORT_MAX_BYTES'].includes(value.payload.limitCode)
        ) {
            window.__PIVOT_EXPORT_PROOFS__.push({
                schemaVersion: 1,
                kind: 'export',
                status: 'error',
                requestId: value.requestId,
                limitCode: value.payload.limitCode,
            });
        }
        Object.values(value).forEach(visitResponse);
    };
    const originalFetch = window.fetch.bind(window);
    window.fetch = (...args) => {
        if (args[1] && args[1].body) visitRequest(args[1].body);
        if (args[0] instanceof Request) {
            args[0].clone().text().then(visitRequest).catch(() => {});
        }
        return originalFetch(...args).then(response => {
        response.clone().text().then(visitResponse).catch(() => {});
        return response;
        });
    };
    const originalOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function(...args) {
        this.addEventListener('load', () => {
            visitResponse(this.responseText);
        });
        return originalOpen.apply(this, args);
    };
    const originalSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send = function(body) {
        visitRequest(body);
        return originalSend.call(this, body);
    };
    window.__PIVOT_RESET_EXPORT_PROBE__ = () => {
        requestIds.clear();
        window.__PIVOT_EXPORT_PROOFS__ = [];
    };
}
"""


def _load_certification_runner():
    path = ROOT / "scripts" / "certify_flagship.py"
    spec = importlib.util.spec_from_file_location("certify_flagship_for_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_junit_report(
    path: Path,
    *,
    tests: int,
    skipped: int = 0,
    failures: int = 0,
    errors: int = 0,
) -> None:
    suite = ET.Element(
        "testsuite",
        tests=str(tests),
        skipped=str(skipped),
        failures=str(failures),
        errors=str(errors),
    )
    for index in range(tests):
        case = ET.SubElement(
            suite,
            "testcase",
            classname="qualification",
            name=f"browser evidence {index}",
        )
        if index == 0 and skipped:
            ET.SubElement(case, "skipped", message="SECRET backend exception: raw-cell-value")
        if index == 0 and failures:
            ET.SubElement(case, "failure", message="SECRET backend exception: raw-cell-value")
        if index == 0 and errors:
            ET.SubElement(case, "error", message="SECRET backend exception: raw-cell-value")
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def _write_phase_evidence(phase_dir: Path, phase: str, *, missing: tuple[str, str] | None = None) -> None:
    for host in HOSTS:
        host_dir = phase_dir / host
        downloads = host_dir / "downloads"
        downloads.mkdir(parents=True, exist_ok=True)
        if phase == "normal":
            for export_format in ("csv", "xlsx"):
                for suffix in ("download.json", "parsed.json", "png", "browser.json", "network.json"):
                    path = host_dir / f"qualified-{export_format}.{suffix}"
                    if missing != (host, path.name):
                        if suffix == "png":
                            path.write_bytes(b"png")
                        elif suffix == "download.json":
                            path.write_text(json.dumps({
                                "format": export_format,
                                "byteLength": 389,
                                "documentValidated": True,
                            }), encoding="utf-8")
                        elif suffix == "parsed.json":
                            path.write_text(json.dumps({
                                "format": export_format,
                                "rows": 5,
                                "columns": 7,
                                "numericCellCount": 10 if export_format == "xlsx" else 0,
                                "mergedHeaderCount": 2 if export_format == "xlsx" else 0,
                                "unicodeVerified": True,
                                "filterVerified": True,
                                "columnOrderVerified": True,
                                "totalsVerified": True,
                            }), encoding="utf-8")
                        elif suffix == "browser.json":
                            path.write_text(json.dumps({
                                "entryCount": 1,
                                "levelCounts": {"INFO": 1},
                                "sourceCounts": {"console-api": 1},
                                "relevantErrorCount": 0,
                            }), encoding="utf-8")
                        else:
                            path.write_text(json.dumps({
                                "entryCount": 1,
                                "methodCounts": {"Network.responseReceived": 1},
                                "statusCounts": {"200": 1},
                                "resourceTypeCounts": {"Fetch": 1},
                            }), encoding="utf-8")
            for representation in ("tsv", "html"):
                for suffix in ("parsed.json", "png", "browser.json", "network.json"):
                    path = host_dir / f"qualified-clipboard-{representation}.{suffix}"
                    if suffix == "png":
                        path.write_bytes(b"png")
                    elif suffix == "parsed.json":
                        def clipboard_summary(scope, rows):
                            return {
                                "representation": representation,
                                "scope": scope,
                                "rows": rows,
                                "columns": 7,
                                "unicodeVerified": True,
                                "filterVerified": True,
                                "columnOrderVerified": True,
                            }
                        path.write_text(json.dumps({
                            "currentTable": clipboard_summary("current-table", 3),
                            "allRecords": clipboard_summary("all-records", 5),
                        }), encoding="utf-8")
                    elif suffix == "browser.json":
                        path.write_text(json.dumps({
                            "entryCount": 1,
                            "levelCounts": {"INFO": 1},
                            "sourceCounts": {"console-api": 1},
                            "relevantErrorCount": 0,
                        }), encoding="utf-8")
                    else:
                        path.write_text(json.dumps({
                            "entryCount": 1,
                            "methodCounts": {"Network.responseReceived": 1},
                            "statusCounts": {"200": 1},
                            "resourceTypeCounts": {"Fetch": 1},
                        }), encoding="utf-8")
        else:
            for export_format in ("csv", "xlsx"):
                for suffix in ("parsed.json", "png", "browser.json", "network.json"):
                    path = host_dir / f"qualified-limit-{export_format}.{suffix}"
                    if suffix == "parsed.json":
                        path.write_text(json.dumps({
                            "format": export_format,
                            "limitRejected": True,
                            "schemaVersion": 1,
                            "kind": "export",
                            "runtimeStatus": "error",
                            "requestId": f"export:test:{host}:{export_format}",
                            "clickedRequestId": f"export:test:{host}:{export_format}",
                            "limitCode": "PIVOT_EXPORT_MAX_ROWS",
                            "stableError": "Export limit exceeded",
                            "newCompletedFiles": 0,
                            "newPartialFiles": 0,
                        }), encoding="utf-8")
                    elif suffix == "png":
                        path.write_bytes(b"png")
                    elif suffix == "browser.json":
                        path.write_text(json.dumps({
                            "entryCount": 1,
                            "levelCounts": {"INFO": 1},
                            "sourceCounts": {"console-api": 1},
                            "relevantErrorCount": 0,
                        }), encoding="utf-8")
                    else:
                        path.write_text(json.dumps({
                            "entryCount": 1,
                            "methodCounts": {"Network.responseReceived": 1},
                            "statusCounts": {"429": 1},
                            "resourceTypeCounts": {"Fetch": 1},
                        }), encoding="utf-8")


def _assert_published_tree_is_sanitized(root: Path) -> None:
    probes = (
        "raw-cell-value",
        "SECRET backend exception",
        "Bearer test-token",
        "session-cookie-value",
        "token-value-123",
        "access-token-value-456",
        "password-value-789",
        "credential-value-abc",
        "secret-value-def",
        "arbitrary backend/raw string",
    )
    prohibited_keys = {
        "postdata", "headers", "requestheaders", "responseheaders",
        "cookie", "cookies", "authorization", "set-cookie",
        "token", "accesstoken", "password", "credential", "secret",
    }
    prohibited_suffixes = {".log", ".csv", ".xlsx", ".crdownload", ".part", ".tmp", ".har"}

    def visit(value):
        if isinstance(value, dict):
            assert not ({str(key).lower() for key in value} & prohibited_keys)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        assert path.suffix.lower() not in prohibited_suffixes, path
        text = path.read_text(encoding="utf-8")
        assert not any(probe in text for probe in probes), path
        if path.suffix.lower() == ".json":
            visit(json.loads(text))


@pytest.mark.parametrize(
    ("scenario", "normal_code", "normal_skips", "normal_tests", "missing"),
    [
        pytest.param("skipped-case", 0, 1, 12, None, id="skipped-junit-case"),
        pytest.param(
            "missing-artifact",
            0,
            0,
            12,
            ("react", "qualified-xlsx.parsed.json"),
            id="missing-required-host-artifact",
        ),
        pytest.param("browser-failure", 7, 0, 12, None, id="nonzero-browser-result"),
        pytest.param("missing-cases", 0, 0, 1, None, id="incomplete-junit-case-count"),
    ],
)
def test_export_qualification_runner_fails_closed_and_writes_sanitized_manifest(
    tmp_path, monkeypatch, scenario, normal_code, normal_skips, normal_tests, missing
):
    """A release cannot qualify from a green-looking but incomplete browser run."""
    runner = _load_certification_runner()

    wheel = tmp_path / "serverside_pivot-0.0.88-py3-none-any.whl"
    wheel.write_bytes(b"wheel")

    def fake_phase(phase, artifact_dir, environment):
        assert environment["PIVOT_RUN_EXPORT_QUALIFICATION"] == "1"
        assert environment["PIVOT_REQUIRE_BROWSER_EVIDENCE"] == "1"
        assert environment["PIVOT_EXPORT_QUALIFICATION_PHASE"] == phase
        if phase == "constrained":
            assert environment["PIVOT_EXPORT_MAX_ROWS"] == "1"
            assert int(environment["PIVOT_EXPORT_MAX_BYTES"]) < 389
        else:
            assert "PIVOT_EXPORT_MAX_ROWS" not in environment
            assert "PIVOT_EXPORT_MAX_BYTES" not in environment
        phase_dir = artifact_dir / phase
        _write_phase_evidence(phase_dir, phase, missing=missing if phase == "normal" else None)
        _write_junit_report(
            phase_dir / "junit.xml",
            tests=normal_tests if phase == "normal" else 6,
            skipped=normal_skips if phase == "normal" else 0,
        )
        return normal_code if phase == "normal" else 0

    monkeypatch.setattr(runner, "_run_qualification_phase", fake_phase, raising=False)
    monkeypatch.setattr(runner, "_install_package", lambda wheel_path: None)
    artifact_dir = tmp_path / scenario
    code = runner.main([
        "--export-qualification",
        "--skip-build",
        "--wheel",
        str(wheel),
        "--artifact-dir",
        str(artifact_dir),
    ])

    assert code != 0
    manifest = json.loads((artifact_dir / "export-qualification.json").read_text(encoding="utf-8"))
    assert manifest["result"] == "failed"
    assert set(manifest["phases"]) == {"normal", "constrained"}
    serialized = json.dumps(manifest)
    assert "raw-cell-value" not in serialized
    assert "backend exception" not in serialized
    assert "SECRET" not in serialized


def test_export_qualification_skip_build_reinstalls_only_a_prebuilt_wheel(
    tmp_path, monkeypatch
):
    runner = _load_certification_runner()
    install_calls = []
    wheel = tmp_path / "serverside_pivot-0.0.88-py3-none-any.whl"
    wheel.write_bytes(b"wheel")

    def fake_install_package(wheel_path):
        install_calls.append(wheel_path)

    def fake_phase(phase, artifact_dir, environment):
        phase_dir = artifact_dir / phase
        _write_phase_evidence(phase_dir, phase)
        _write_junit_report(phase_dir / "junit.xml", tests=12 if phase == "normal" else 6)
        return 0

    monkeypatch.setattr(runner, "_install_package", fake_install_package, raising=False)
    monkeypatch.setattr(runner, "_run_qualification_phase", fake_phase)
    artifact_dir = tmp_path / "qualification"
    code = runner.main([
        "--export-qualification",
        "--skip-build",
        "--wheel",
        str(wheel),
        "--artifact-dir",
        str(artifact_dir),
    ])

    assert code == 0
    assert install_calls == [wheel.resolve()]
    manifest = json.loads((artifact_dir / "export-qualification.json").read_text(encoding="utf-8"))
    assert manifest["result"] == "passed"
    assert manifest["build"] == "skipped"


def test_install_package_command_never_invokes_a_source_build(tmp_path, monkeypatch):
    runner = _load_certification_runner()
    wheel = tmp_path / "serverside_pivot-0.0.88-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    calls = []
    monkeypatch.setattr(runner, "_run", lambda command, cwd=runner.ROOT: calls.append((command, cwd)))

    runner._install_package(wheel)

    assert len(calls) == 1
    command, cwd = calls[0]
    assert command[-1] == str(wheel.resolve())
    assert "." not in command
    assert "--no-build-isolation" not in command
    assert cwd == runner.ROOT


def test_export_qualification_rejects_reuse_running_before_any_phase(tmp_path, monkeypatch):
    runner = _load_certification_runner()
    called = []
    monkeypatch.setattr(runner, "_run_qualification_phase", lambda *args: called.append(args))
    artifact_dir = tmp_path / "qualification"
    artifact_dir.mkdir()
    sentinel = artifact_dir / "caller-owned.txt"
    sentinel.write_text("preserve", encoding="utf-8")

    code = runner.main([
        "--export-qualification",
        "--skip-build",
        "--reuse-running",
        "--artifact-dir",
        str(artifact_dir),
    ])

    assert code != 0
    assert called == []
    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_export_qualification_missing_wheel_does_not_mutate_destination(tmp_path, monkeypatch):
    runner = _load_certification_runner()
    artifact_dir = tmp_path / "qualification"
    artifact_dir.mkdir()
    sentinel = artifact_dir / "caller-owned.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    called = []
    monkeypatch.setattr(runner, "_run_qualification_phase", lambda *args: called.append(args))

    code = runner.main([
        "--export-qualification",
        "--skip-build",
        "--artifact-dir",
        str(artifact_dir),
    ])

    assert code != 0
    assert called == []
    assert sentinel.read_text(encoding="utf-8") == "preserve"


@pytest.mark.parametrize("target_name", ["root", "package", "git", "external"])
def test_export_publication_target_rejects_existing_caller_owned_paths(
    tmp_path, target_name
):
    runner = _load_certification_runner()
    targets = {
        "root": runner.ROOT,
        "package": runner.PACKAGE,
        "git": runner.ROOT / ".git",
        "external": tmp_path / "caller-owned",
    }
    target = targets[target_name]
    if target_name == "external":
        target.mkdir()
        (target / "sentinel.txt").write_text("preserve", encoding="utf-8")

    with pytest.raises(ValueError, match="must not already exist"):
        runner._validate_publication_target(target)

    if target_name == "external":
        assert (target / "sentinel.txt").read_text(encoding="utf-8") == "preserve"


def test_export_publication_target_rejects_symlink_without_touching_target(tmp_path):
    runner = _load_certification_runner()
    target = tmp_path / "target"
    target.mkdir()
    sentinel = target / "sentinel.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    link = tmp_path / "publication-link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are unavailable")

    with pytest.raises(ValueError, match="must not already exist"):
        runner._validate_publication_target(link)

    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_constrained_evidence_rejects_arbitrary_export_failure(tmp_path):
    runner = _load_certification_runner()
    artifact_dir = tmp_path / "qualification"
    phase_dir = artifact_dir / "constrained"
    _write_phase_evidence(phase_dir, "constrained")
    _write_junit_report(phase_dir / "junit.xml", tests=6)
    summary = phase_dir / "react" / "qualified-limit-csv.parsed.json"
    summary.write_text(json.dumps({
        "format": "csv",
        "limitRejected": True,
        "runtimeStatus": "error",
        "stableError": "Export failed",
        "newCompletedFiles": 0,
        "newPartialFiles": 0,
    }), encoding="utf-8")

    report = runner._validate_qualification_phase("constrained", artifact_dir, 0)

    assert report["result"] == "failed"
    assert "host_react_evidence" in report["issues"]
    assert "invalid_limit_proof_csv" in report["hosts"]["react"]["issues"]


@pytest.mark.parametrize(
    "mutation",
    [
        {"schemaVersion": 2},
        {"kind": "data"},
        {"runtimeStatus": "timeout"},
        {"requestId": "export:other-request"},
        {"clickedRequestId": "export:other-request"},
        {"limitCode": "PIVOT_EXPORT_MAX_ROWS_FAKE"},
        {"limitCode": None, "stableError": "PIVOT_EXPORT_MAX_ROWS Export failed"},
        {"stableError": "Export failed"},
    ],
)
def test_constrained_limit_proof_requires_exact_correlated_v1_envelope(
    tmp_path, mutation
):
    runner = _load_certification_runner()
    proof = {
        "format": "csv",
        "limitRejected": True,
        "schemaVersion": 1,
        "kind": "export",
        "runtimeStatus": "error",
        "requestId": "export:clicked-request",
        "clickedRequestId": "export:clicked-request",
        "limitCode": "PIVOT_EXPORT_MAX_ROWS",
        "stableError": "Export limit exceeded",
        "newCompletedFiles": 0,
        "newPartialFiles": 0,
    }
    proof.update(mutation)
    path = tmp_path / "proof.json"
    path.write_text(json.dumps(proof), encoding="utf-8")

    assert not runner._valid_limit_proof(path, "csv")


def test_browser_export_probe_correlates_outbound_request_to_exact_v1_response():
    assert "XMLHttpRequest.prototype.send" in _EXPORT_RESPONSE_PROBE_SCRIPT
    assert "schemaVersion === 1" in _EXPORT_RESPONSE_PROBE_SCRIPT
    assert "value.payload.limitCode" in _EXPORT_RESPONSE_PROBE_SCRIPT
    assert "requestIds.has(value.requestId)" in _EXPORT_RESPONSE_PROBE_SCRIPT
    assert "detail.includes" not in _EXPORT_RESPONSE_PROBE_SCRIPT


def test_qualification_chrome_options_disable_background_component_downloads(tmp_path, monkeypatch):
    class FakeOptions:
        def __init__(self):
            self.arguments = []
            self.experimental = {}
            self.capabilities = {}

        def add_argument(self, argument):
            self.arguments.append(argument)

        def add_experimental_option(self, key, value):
            self.experimental[key] = value

        def set_capability(self, key, value):
            self.capabilities[key] = value

    monkeypatch.setitem(
        globals(), "_selenium",
        lambda: (None, None, None, FakeOptions, None, None, None, None),
    )

    options = _qualification_chrome_options(tmp_path)

    assert "--disable-background-networking" in options.arguments
    assert "--disable-component-update" in options.arguments
    assert "--disable-default-apps" in options.arguments
    assert "--disable-sync" in options.arguments
    feature_flag = next(argument for argument in options.arguments if argument.startswith("--disable-features="))
    assert "OptimizationHints" in feature_flag
    assert "OptimizationGuideModelDownloading" in feature_flag


def test_validated_download_removal_retries_a_transient_browser_file_lock(
    tmp_path, monkeypatch
):
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    export = downloads / "pivot_export.csv"
    export.write_text("validated export", encoding="utf-8")
    unrelated = downloads / "unrelated.txt"
    unrelated.write_text("preserve", encoding="utf-8")
    real_unlink = Path.unlink
    attempts = 0

    def transiently_locked_unlink(path, *args, **kwargs):
        nonlocal attempts
        if path == export and attempts < 2:
            attempts += 1
            raise PermissionError("browser still owns the completed download")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", transiently_locked_unlink)

    _remove_validated_download(export, timeout=1)

    assert not export.exists()
    assert unrelated.read_text(encoding="utf-8") == "preserve"


def test_export_publication_is_new_allowlisted_tree_separate_from_raw_evidence(
    tmp_path, monkeypatch
):
    runner = _load_certification_runner()
    wheel = tmp_path / "serverside_pivot-0.0.88-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    artifact_dir = tmp_path / "qualification"
    raw_roots = []

    def fake_phase(phase, output_dir, environment):
        assert output_dir != artifact_dir
        assert not output_dir.is_relative_to(artifact_dir)
        raw_roots.append(output_dir)
        phase_dir = output_dir / phase
        _write_phase_evidence(phase_dir, phase)
        _write_junit_report(phase_dir / "junit.xml", tests=12 if phase == "normal" else 6)
        if phase == "normal":
            (phase_dir / "runtime.stdout.log").write_text(
                "SECRET backend exception: raw-cell-value", encoding="utf-8"
            )
            (phase_dir / "dash" / "downloads" / "unrelated.txt").write_text(
                "token=token-value-123; arbitrary backend/raw string", encoding="utf-8"
            )
            (phase_dir / "dash" / "leak.network.json").write_text(json.dumps({
                "accessToken": "access-token-value-456",
                "nested": {
                    "password": "password-value-789",
                    "credential": "credential-value-abc",
                    "secret": "secret-value-def",
                },
            }), encoding="utf-8")
            (phase_dir / "dash" / "unknown.bin").write_bytes(
                b"credential-value-abc\x00secret-value-def"
            )
            network_summary = phase_dir / "dash" / "qualified-xlsx.network.json"
            value = json.loads(network_summary.read_text(encoding="utf-8"))
            value["token"] = "token-value-123"
            network_summary.write_text(json.dumps(value), encoding="utf-8")
            (phase_dir / "react" / "qualified-csv.browser.json").write_text(
                "{invalid json", encoding="utf-8"
            )
        return 0

    monkeypatch.setattr(runner, "_install_package", lambda wheel_path: None)
    monkeypatch.setattr(runner, "_run_qualification_phase", fake_phase)
    code = runner.main([
        "--export-qualification",
        "--skip-build",
        "--wheel",
        str(wheel),
        "--artifact-dir",
        str(artifact_dir),
    ])

    assert code != 0
    assert raw_roots
    assert all(not raw_root.exists() for raw_root in raw_roots)
    _assert_published_tree_is_sanitized(artifact_dir)
    assert not list(artifact_dir.rglob("*.png"))
    assert not list(artifact_dir.rglob("*.process.json"))
    assert not (artifact_dir / "normal" / "dash" / "downloads" / "unrelated.txt").exists()
    manifest = json.loads((artifact_dir / "export-qualification.json").read_text(encoding="utf-8"))
    assert manifest["result"] == "failed"
    assert "unexpected_raw_artifact" in manifest["phases"]["normal"]["issues"]
    assert "invalid_safe_summary" in manifest["phases"]["normal"]["issues"]


def test_export_publication_remains_safe_when_qualification_is_interrupted(tmp_path, monkeypatch):
    runner = _load_certification_runner()
    wheel = tmp_path / "serverside_pivot-0.0.88-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    artifact_dir = tmp_path / "qualification"
    raw_roots = []

    def interrupting_phase(phase, output_dir, environment):
        raw_roots.append(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "secret.log").write_text(
            "password=password-value-789", encoding="utf-8"
        )
        raise KeyboardInterrupt

    monkeypatch.setattr(runner, "_install_package", lambda wheel_path: None)
    monkeypatch.setattr(runner, "_run_qualification_phase", interrupting_phase)

    with pytest.raises(KeyboardInterrupt):
        runner.main([
            "--export-qualification",
            "--skip-build",
            "--wheel",
            str(wheel),
            "--artifact-dir",
            str(artifact_dir),
        ])

    assert all(not raw_root.exists() for raw_root in raw_roots)
    _assert_published_tree_is_sanitized(artifact_dir)
    manifest = json.loads((artifact_dir / "export-qualification.json").read_text(encoding="utf-8"))
    assert manifest["result"] == "failed"
    assert all(
        "qualification_interrupted" in report["issues"]
        for report in manifest["phases"].values()
    )


def test_flagship_capability_matrix_covers_every_published_workflow():
    """Keep the release promise explicit, including intentionally disabled work."""
    capability_set = capability_ids()
    assert len(capability_set) == len(CAPABILITIES)
    assert {item.group for item in CAPABILITIES} == {
        "Startup", "Navigation", "Model", "Filtering", "Table", "Selection",
        "Delivery", "Presentation", "Advanced sidebar", "Saved work", "Analysis",
        "Host integration", "Explicitly unavailable",
    }
    unsupported = [item for item in CAPABILITIES if not item.supported]
    assert [(item.id, item.disabled_reason) for item in unsupported] == [
        ("editing", "Editing remains outside the public component contract."),
    ]


def test_flagship_certification_document_is_generated_from_the_matrix_contract():
    document = (ROOT / "docs" / "flagship-functionality-certification.md").read_text(encoding="utf-8")
    assert "tests/flagship_capabilities.py" in document
    assert "python scripts/certify_flagship.py" in document
    matrix = markdown_matrix()
    assert matrix.count("| required | required | required |") == len([item for item in CAPABILITIES if item.supported])


def test_flagship_qualification_data_is_environment_gated(monkeypatch):
    from examples import flagship_demo_data

    monkeypatch.delenv("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA", raising=False)
    default_table = flagship_demo_data.build_demo_table(3)
    default_config = flagship_demo_data.workspace_config("dash")
    assert default_table.num_rows == 3
    assert default_config["rowFields"] == ["region", "segment", "product"]

    monkeypatch.setenv("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA", "1")
    qualification_table = flagship_demo_data.build_demo_table(3)
    qualification_config = flagship_demo_data.workspace_config("dash")
    assert qualification_table.to_pylist() == FLAGSHIP_QUALIFICATION_SOURCE_ROWS
    assert qualification_config["rowFields"] == FLAGSHIP_ROW_FIELDS
    assert qualification_config["colFields"] == FLAGSHIP_COL_FIELDS
    assert qualification_config["valConfigs"] == FLAGSHIP_VALUE_CONFIGS
    assert qualification_config["filters"] == FLAGSHIP_FILTER
    assert qualification_config["columnOrder"] != []


def test_flagship_react_host_authorizes_and_receives_shared_workspace_config():
    """The HTTP-only hosts use the same explicit demo contract as Dash."""
    runtime_server = (ROOT / "examples" / "flagship_runtime_server.py").read_text(encoding="utf-8")
    react_source = (ROOT / "examples" / "flagship_react" / "src" / "main.tsx").read_text(encoding="utf-8")

    assert "RuntimeDatasetAuthorizer" in runtime_server
    assert '"read", "export"' in runtime_server
    assert 'role: str = "viewer"' in runtime_server
    assert 'workspace_config("react")' in runtime_server
    assert '"/v1/workspace-config"' in runtime_server
    assert "fetch('/v1/workspace-config')" in react_source
    assert "<PivotWorkspace {...workspaceConfig} transport={transport} />" in react_source


pytestmark = pytest.mark.flagship


def _selenium():
    try:
        import selenium
    except ImportError:
        if REQUIRE_BROWSER_EVIDENCE:
            pytest.fail("Selenium is required for browser evidence")
        pytest.skip("Selenium is not installed")
    from selenium import webdriver
    from selenium.common.exceptions import WebDriverException
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    return selenium, webdriver, WebDriverException, ChromeOptions, ActionChains, By, EC, WebDriverWait


def _host_url(host: str) -> str:
    defaults = {
        "dash": "http://127.0.0.1:8050",
        "react": "http://127.0.0.1:5174",
        "streamlit": "http://127.0.0.1:8502",
    }
    return os.environ.get(f"PIVOT_FLAGSHIP_{host.upper()}_URL", defaults[host])


def _origin(url: str) -> str:
    from urllib.parse import urlsplit

    parsed = urlsplit(url)
    return f"{parsed.scheme}://{parsed.netloc}"


@pytest.fixture(params=HOSTS, ids=HOSTS)
def flagship_host(request):
    if not RUN_BROWSER_CERTIFICATION:
        pytest.skip("set PIVOT_RUN_FLAGSHIP_BROWSER_TESTS=1 to run built-host certification")
    return request.param


@pytest.fixture
def flagship_artifacts(flagship_host):
    directory = ARTIFACT_ROOT / flagship_host
    directory.mkdir(parents=True, exist_ok=True)
    return directory


@pytest.fixture
def flagship_driver(flagship_host, flagship_artifacts):
    _, webdriver, WebDriverException, ChromeOptions, _, _, _, _ = _selenium()
    downloads = flagship_artifacts / "downloads"
    downloads.mkdir(exist_ok=True)
    options = _qualification_chrome_options(downloads)
    try:
        driver = webdriver.Chrome(options=options)
    except WebDriverException as exc:
        if REQUIRE_BROWSER_EVIDENCE:
            pytest.fail("Chrome WebDriver is required for browser evidence")
        pytest.skip(f"Chrome WebDriver is not available: {exc}")
    driver.set_page_load_timeout(30)
    driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
        "behavior": "allow", "downloadPath": str(downloads), "eventsEnabled": True,
    })
    # Chrome denies clipboard reads by default in headless mode.  Granting the
    # permission lets this test verify the user-visible clipboard contents,
    # rather than accepting a misleading toast.
    driver.execute_cdp_cmd("Browser.grantPermissions", {
        "origin": _origin(_host_url(flagship_host)),
        "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"],
    })
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": _EXPORT_RESPONSE_PROBE_SCRIPT,
    })
    try:
        yield driver
    finally:
        driver.quit()


def _qualification_chrome_options(downloads: Path):
    _, _, _, ChromeOptions, _, _, _, _ = _selenium()
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1680,1200")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--force-device-scale-factor=1")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-component-update")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-features=OptimizationHints,OptimizationGuideModelDownloading")
    options.add_experimental_option("prefs", {
        "download.default_directory": str(downloads),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })
    options.set_capability("goog:loggingPrefs", {"browser": "ALL", "performance": "ALL"})
    return options


def _switch_to_workspace(driver, host: str, timeout: int = 30):
    _, _, _, _, _, By, EC, WebDriverWait = _selenium()
    driver.get(_host_url(host))
    wait = WebDriverWait(driver, timeout)
    if host != "streamlit":
        driver._pivot_workspace_shadow = False
        return wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[role="gridcell"]')))

    # Streamlit V2 isolates a component in an open ShadowRoot (rather than
    # the iframe used by the older component API). Selenium's ordinary CSS
    # lookups stop at that boundary, so retain the document context and query
    # through open roots explicitly.
    driver._pivot_workspace_shadow = True
    return wait.until(lambda current: _workspace_element(current, '[role="gridcell"]', required=False))


def _workspace_elements(driver, selector: str):
    if not getattr(driver, "_pivot_workspace_shadow", False):
        _, _, _, _, _, By, _, _ = _selenium()
        return driver.find_elements(By.CSS_SELECTOR, selector)
    return driver.execute_script(
        """
        const selector = arguments[0];
        const collect = (root, values) => {
            values.push(...root.querySelectorAll(selector));
            for (const element of root.querySelectorAll('*')) {
                if (element.shadowRoot) collect(element.shadowRoot, values);
            }
        };
        const values = [];
        collect(document, values);
        return values;
        """,
        selector,
    )


def _workspace_element(driver, selector: str, *, required: bool = True):
    matches = _workspace_elements(driver, selector)
    if matches:
        return matches[0]
    if required:
        raise AssertionError(f"Workspace element not found: {selector}")
    return False


def _wait_for_workspace_element(driver, selector: str, timeout: int = 15):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    return WebDriverWait(driver, timeout).until(lambda current: _workspace_element(current, selector, required=False))


def _wait_for_visible_workspace_element(driver, selector: str, timeout: int = 15):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    return WebDriverWait(driver, timeout).until(lambda current: next((
        element for element in _workspace_elements(current, selector)
        if element.is_displayed() and element.is_enabled()
    ), False))


def _workspace_has_exact_text(driver, selector: str, text: str) -> bool:
    return bool(driver.execute_script(
        """
        const [selector, expected] = arguments;
        const visit = root => {
            for (const element of root.querySelectorAll(selector)) {
                if ((element.textContent || '').trim() === expected) return true;
            }
            for (const element of root.querySelectorAll('*')) {
                if (element.shadowRoot && visit(element.shadowRoot)) return true;
            }
            return false;
        };
        return visit(document);
        """,
        selector,
        text,
    ))


def _save_evidence(driver, directory: Path, test_name: str):
    browser_logs = driver.get_log("browser")
    performance_logs = driver.get_log("performance")
    driver.execute_script(
        """
        const apply = root => {
            const style = document.createElement('style');
            style.dataset.pivotEvidenceRedaction = 'true';
            style.textContent = '[role="gridcell"] { color: transparent !important; text-shadow: none !important; }';
            (root === document ? document.head : root).appendChild(style);
            for (const element of root.querySelectorAll('*')) {
                if (element.shadowRoot) apply(element.shadowRoot);
            }
        };
        apply(document);
        """
    )
    try:
        driver.save_screenshot(str(directory / f"{test_name}.png"))
    finally:
        driver.execute_script(
            """
            const remove = root => {
                for (const style of root.querySelectorAll('style[data-pivot-evidence-redaction]')) style.remove();
                for (const element of root.querySelectorAll('*')) {
                    if (element.shadowRoot) remove(element.shadowRoot);
                }
            };
            remove(document);
            """
        )

    browser_summary = {
        "entryCount": len(browser_logs),
        "levelCounts": {},
        "sourceCounts": {},
        "relevantErrorCount": len(_relevant_browser_errors(browser_logs)),
    }
    for entry in browser_logs:
        level = str(entry.get("level", "UNKNOWN"))
        source = str(entry.get("source", "unknown"))
        browser_summary["levelCounts"][level] = browser_summary["levelCounts"].get(level, 0) + 1
        browser_summary["sourceCounts"][source] = browser_summary["sourceCounts"].get(source, 0) + 1

    network_summary = {"entryCount": len(performance_logs), "methodCounts": {}, "statusCounts": {}, "resourceTypeCounts": {}}
    for entry in performance_logs:
        try:
            event = json.loads(entry.get("message", "{}"))["message"]
        except (KeyError, TypeError, json.JSONDecodeError):
            continue
        method = str(event.get("method", "unknown"))
        network_summary["methodCounts"][method] = network_summary["methodCounts"].get(method, 0) + 1
        response = event.get("params", {}).get("response", {})
        if isinstance(response, dict) and "status" in response:
            status = str(response["status"])
            network_summary["statusCounts"][status] = network_summary["statusCounts"].get(status, 0) + 1
        resource_type = event.get("params", {}).get("type")
        if resource_type:
            resource_type = str(resource_type)
            network_summary["resourceTypeCounts"][resource_type] = network_summary["resourceTypeCounts"].get(resource_type, 0) + 1

    (directory / f"{test_name}.browser.json").write_text(json.dumps(browser_summary, indent=2), encoding="utf-8")
    (directory / f"{test_name}.network.json").write_text(json.dumps(network_summary, indent=2), encoding="utf-8")
    return browser_logs


def _install_export_response_probe(driver) -> None:
    driver.execute_script(_EXPORT_RESPONSE_PROBE_SCRIPT)
    driver.execute_script("window.__PIVOT_RESET_EXPORT_PROBE__();")


def _export_limit_proof(driver):
    return driver.execute_script(
        "return (window.__PIVOT_EXPORT_PROOFS__ || []).find(item => item && item.status === 'error' && item.limitCode) || null;"
    )


def _relevant_browser_errors(logs):
    ignored = ("favicon.ico",)
    return [
        entry["message"] for entry in logs
        if entry.get("level") in {"SEVERE", "ERROR"}
        and not any(token in entry.get("message", "") for token in ignored)
    ]


def _clipboard_text(driver):
    return driver.execute_async_script(
        "const done = arguments[0]; navigator.clipboard.readText().then(done, error => done(`ERROR:${error.message}`));"
    )


def _clipboard_html(driver):
    return driver.execute_async_script(
        """
        const done = arguments[0];
        navigator.clipboard.read().then(async (items) => {
            const item = items.find(candidate => candidate.types.includes('text/html'));
            if (!item) return done('');
            const blob = await item.getType('text/html');
            done(await blob.text());
        }, error => done(`ERROR:${error.message}`));
        """
    )


def _wait_for_download(directory: Path, export_format: str = "xlsx", timeout: int = 20, started_at: float | None = None) -> Path:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        completed = [
            path for path in directory.glob(f"*.{export_format}")
            if not path.with_suffix(path.suffix + ".crdownload").exists()
            and (started_at is None or path.stat().st_mtime >= started_at)
        ]
        if completed:
            return max(completed, key=lambda path: path.stat().st_mtime)
        time.sleep(0.2)
    raise AssertionError(f"No completed {export_format.upper()} download in {directory}")


def _remove_validated_download(path: Path, *, timeout: float = 5) -> None:
    """Remove one parsed export once Chrome releases its completed file handle."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            path.unlink()
            return
        except PermissionError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(0.05, remaining))


def _click_menu_item(driver, cell, label: str):
    _, _, _, _, ActionChains, _, _, WebDriverWait = _selenium()
    ActionChains(driver).context_click(cell).perform()
    return WebDriverWait(driver, 15).until(lambda current: next((
        element for element in _workspace_elements(current, '[role="menuitem"]')
        if element.text.strip() == label and element.is_enabled()
    ), False))


def _assert_workspace_controls(driver):
    for selector in (
        "[data-testid='pivot-toggle-sidebar']",
        "[data-testid='pivot-toolbar-view']",
        "[data-testid='pivot-toolbar-format']",
        "[data-testid='pivot-toolbar-charts']",
        "[data-testid='pivot-toolbar-theme']",
        "[data-testid='pivot-export-xlsx']",
        "[data-testid='pivot-export-csv']",
    ):
        _wait_for_workspace_element(driver, selector)


def _click_export(driver, export_format: str):
    if export_format == "csv":
        _wait_for_visible_workspace_element(driver, "[aria-label='Choose export format']").click()
    _wait_for_visible_workspace_element(driver, f"[data-testid='pivot-export-{export_format}']").click()


def _qualification_leaf_order(driver) -> list[str]:
    return [
        element.get_attribute("data-header-column-id")
        for element in _workspace_elements(driver, "[role='columnheader'][data-header-column-id]")
        if element.get_attribute("data-header-column-id")
    ]


def _apply_qualification_column_order(driver):
    _, _, _, _, ActionChains, _, _, WebDriverWait = _selenium()
    from selenium.webdriver.common.keys import Keys

    units_id = "Q1_unités_sum"
    sales_id = "Q1_ventes_€_sum"
    order = _qualification_leaf_order(driver)
    if order.index(units_id) > order.index(sales_id):
        units_header = _wait_for_visible_workspace_element(
            driver, f"[role='columnheader'][data-header-column-id='{units_id}']"
        )
        ActionChains(driver).click(units_header).key_down(Keys.CONTROL).send_keys(
            Keys.ARROW_LEFT
        ).key_up(Keys.CONTROL).perform()
    WebDriverWait(driver, 15).until(
        lambda current: _qualification_leaf_order(current).index(units_id)
        < _qualification_leaf_order(current).index(sales_id)
    )


def test_flagship_startup_navigation_and_controls(flagship_host, flagship_driver, flagship_artifacts):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    _switch_to_workspace(flagship_driver, flagship_host)
    _assert_workspace_controls(flagship_driver)
    assert len(_workspace_elements(flagship_driver, '[role="gridcell"]')) > 0

    # Open/close the main control bands and move the main horizontal scroller.
    for selector in ("[data-testid='pivot-toolbar-view']", "[data-testid='pivot-toolbar-format']", "[data-testid='pivot-toolbar-theme']"):
        _workspace_element(flagship_driver, selector).click()
    flagship_driver.execute_script(
        "const visit = root => root.querySelector('[role=gridcell]') || Array.from(root.querySelectorAll('*')).map(node => node.shadowRoot && visit(node.shadowRoot)).find(Boolean);"
        "const grid = visit(document);"
        "let parent = grid; while (parent && parent.scrollWidth <= parent.clientWidth) parent = parent.parentElement;"
        "if (parent) parent.scrollLeft = Math.min(400, parent.scrollWidth);"
    )
    WebDriverWait(flagship_driver, 15).until(lambda current: _workspace_element(current, '[role="gridcell"]', required=False))
    logs = _save_evidence(flagship_driver, flagship_artifacts, "startup-navigation-controls")
    assert not _relevant_browser_errors(logs)


@pytest.mark.skipif(RUN_EXPORT_QUALIFICATION, reason="ordinary showcase-data certification")
def test_flagship_copy_current_table_all_records_and_export_xlsx(flagship_host, flagship_driver, flagship_artifacts):
    _, _, _, _, ActionChains, _, _, WebDriverWait = _selenium()
    from selenium.webdriver.common.keys import Keys

    first_cell = _switch_to_workspace(flagship_driver, flagship_host)

    current_table_action = _click_menu_item(flagship_driver, first_cell, "Copy Current Table")
    current_table_action.click()
    WebDriverWait(flagship_driver, 20).until(lambda driver: "\t" in str(_clipboard_text(driver)))
    current_table = _clipboard_text(flagship_driver)
    assert "ERROR:" not in current_table
    assert "\t" in current_table and "\n" in current_table
    current_header = current_table.splitlines()[0]
    assert "Revenue" in current_header
    assert not current_header.startswith("APAC")
    current_table_html = _clipboard_html(flagship_driver)
    assert "ERROR:" not in current_table_html
    assert "<thead>" in current_table_html
    assert "<th " in current_table_html
    assert "Revenue" in current_table_html

    # The keyboard equivalent of Copy Current Table must be labelled too.
    flagship_driver.execute_async_script(
        "const done = arguments[0]; navigator.clipboard.writeText('').then(done, done);"
    )
    first_cell.click()
    ActionChains(flagship_driver).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).perform()
    ActionChains(flagship_driver).key_down(Keys.CONTROL).send_keys("c").key_up(Keys.CONTROL).perform()
    WebDriverWait(flagship_driver, 20).until(
        lambda driver: str(_clipboard_text(driver)).splitlines()[:1] == [current_header]
    )
    first_cell = _switch_to_workspace(flagship_driver, flagship_host)

    all_records_action = _click_menu_item(flagship_driver, first_cell, "Copy All Records")
    all_records_action.click()
    WebDriverWait(flagship_driver, 20).until(
        lambda driver: len(str(_clipboard_text(driver)).splitlines()) > len(str(current_table).splitlines())
    )
    all_records = _clipboard_text(flagship_driver)
    assert "ERROR:" not in all_records
    assert len(all_records.splitlines()) > len(current_table.splitlines())
    assert all_records.splitlines()[0] == current_header

    download_started_at = time.time()
    WebDriverWait(flagship_driver, 15).until(lambda current: _workspace_element(current, "[data-testid='pivot-export-xlsx']", required=False)).click()
    export = _wait_for_download(flagship_artifacts / "downloads", started_at=download_started_at)
    assert export.stat().st_size > 1024
    assert export.read_bytes()[:4] == b"PK\x03\x04"
    with zipfile.ZipFile(export) as workbook:
        sheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    assert sheet.findall(".//x:mergeCells/x:mergeCell", namespace)
    logs = _save_evidence(flagship_driver, flagship_artifacts, "copy-current-all-and-export")
    assert not _relevant_browser_errors(logs)


@pytest.mark.skipif(
    not RUN_EXPORT_QUALIFICATION or EXPORT_QUALIFICATION_PHASE == "constrained",
    reason="normal export qualification phase",
)
@pytest.mark.parametrize("export_format", ["csv", "xlsx"])
def test_flagship_qualified_download(flagship_host, flagship_driver, flagship_artifacts, export_format):
    _switch_to_workspace(flagship_driver, flagship_host)
    _assert_workspace_controls(flagship_driver)
    _apply_qualification_column_order(flagship_driver)

    download_started_at = time.time()
    _click_export(flagship_driver, export_format)
    export = _wait_for_download(
        flagship_artifacts / "downloads",
        export_format,
        started_at=download_started_at,
    )
    summary = assert_flagship_download(export, export_format)
    (flagship_artifacts / f"qualified-{export_format}.parsed.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    (flagship_artifacts / f"qualified-{export_format}.download.json").write_text(
        json.dumps({
            "format": export_format,
            "byteLength": export.stat().st_size,
            "documentValidated": True,
        }, indent=2) + "\n",
        encoding="utf-8",
    )
    _remove_validated_download(export)
    logs = _save_evidence(flagship_driver, flagship_artifacts, f"qualified-{export_format}")
    assert not _relevant_browser_errors(logs)


@pytest.mark.skipif(
    not RUN_EXPORT_QUALIFICATION or EXPORT_QUALIFICATION_PHASE == "constrained",
    reason="normal export qualification phase",
)
@pytest.mark.parametrize("representation", ["tsv", "html"])
def test_flagship_qualified_clipboard(flagship_host, flagship_driver, flagship_artifacts, representation):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    _switch_to_workspace(flagship_driver, flagship_host)
    _apply_qualification_column_order(flagship_driver)
    first_cell = _workspace_element(flagship_driver, '[role="gridcell"]')

    current_action = _click_menu_item(flagship_driver, first_cell, "Copy Current Table")
    current_action.click()
    WebDriverWait(flagship_driver, 20).until(lambda driver: "\t" in str(_clipboard_text(driver)))
    current_document = _clipboard_text(flagship_driver) if representation == "tsv" else _clipboard_html(flagship_driver)
    current_summary = assert_flagship_clipboard(current_document, representation, all_records=False)

    first_cell = _workspace_element(flagship_driver, '[role="gridcell"]')
    all_action = _click_menu_item(flagship_driver, first_cell, "Copy All Records")
    all_action.click()
    WebDriverWait(flagship_driver, 20).until(
        lambda driver: len(str(_clipboard_text(driver)).splitlines()) > current_summary["rows"]
    )
    all_document = _clipboard_text(flagship_driver) if representation == "tsv" else _clipboard_html(flagship_driver)
    all_summary = assert_flagship_clipboard(all_document, representation, all_records=True)
    assert all_summary["rows"] > current_summary["rows"]

    summary = {"currentTable": current_summary, "allRecords": all_summary}
    (flagship_artifacts / f"qualified-clipboard-{representation}.parsed.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    logs = _save_evidence(flagship_driver, flagship_artifacts, f"qualified-clipboard-{representation}")
    assert not _relevant_browser_errors(logs)


@pytest.mark.skipif(
    not RUN_EXPORT_QUALIFICATION or EXPORT_QUALIFICATION_PHASE != "constrained",
    reason="constrained export qualification phase",
)
@pytest.mark.parametrize("export_format", ["csv", "xlsx"])
def test_flagship_export_limit_rejection_creates_no_file(
    flagship_host, flagship_driver, flagship_artifacts, export_format
):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    _switch_to_workspace(flagship_driver, flagship_host)
    _assert_workspace_controls(flagship_driver)
    _install_export_response_probe(flagship_driver)
    downloads = flagship_artifacts / "downloads"
    before = {
        path.name: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in downloads.iterdir()
        if path.is_file()
    }

    _click_export(flagship_driver, export_format)
    proof = WebDriverWait(flagship_driver, 20).until(_export_limit_proof)
    assert proof["schemaVersion"] == 1
    assert proof["kind"] == "export"
    assert proof["status"] == "error"
    assert proof["limitCode"] in {"PIVOT_EXPORT_MAX_ROWS", "PIVOT_EXPORT_MAX_BYTES"}
    assert proof["requestId"]
    WebDriverWait(flagship_driver, 20).until(
        lambda current: _workspace_element(
            current,
            f"[data-testid='pivot-export-{export_format}']",
            required=False,
        ).is_enabled()
    )

    deadline = time.monotonic() + 2
    after = before
    while time.monotonic() < deadline:
        after = {
            path.name: (path.stat().st_size, path.stat().st_mtime_ns)
            for path in downloads.iterdir()
            if path.is_file()
        }
        assert after == before
        time.sleep(0.1)

    summary = {
        "format": export_format,
        "limitRejected": True,
        "schemaVersion": proof["schemaVersion"],
        "kind": proof["kind"],
        "runtimeStatus": proof["status"],
        "requestId": proof["requestId"],
        "clickedRequestId": proof["requestId"],
        "limitCode": proof["limitCode"],
        "stableError": "Export limit exceeded",
        "newCompletedFiles": 0,
        "newPartialFiles": 0,
    }
    (flagship_artifacts / f"qualified-limit-{export_format}.parsed.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    logs = _save_evidence(flagship_driver, flagship_artifacts, f"qualified-limit-{export_format}")
    assert not _relevant_browser_errors(logs)
    assert after == before


def test_flagship_sidebar_filter_report_and_chart_entrypoints(flagship_host, flagship_driver, flagship_artifacts):
    _, _, _, _, _, _, _, WebDriverWait = _selenium()
    _switch_to_workspace(flagship_driver, flagship_host)
    wait = WebDriverWait(flagship_driver, 15)
    toggle = _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-toggle-sidebar']")
    toggle.click()
    if not _workspace_element(flagship_driver, '[aria-label="Tool Panel"]', required=False):
        toggle.click()
    wait.until(lambda current: _workspace_element(current, '[aria-label="Tool Panel"]', required=False))

    search = _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-global-search']")
    search.clear()
    search.send_keys("APAC")
    wait.until(lambda current: _workspace_element(current, '[role="gridcell"]', required=False))
    search.clear()

    if not _workspace_element(flagship_driver, "[data-add-chart-pane='true']", required=False):
        _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-toolbar-charts']").click()
    _wait_for_workspace_element(flagship_driver, "[data-add-chart-pane='true']").click()
    wait.until(lambda current: next((element for element in _workspace_elements(current, '*') if element.text.strip() == 'New Chart Pane'), False))
    wait.until(lambda current: next((
        element for element in _workspace_elements(current, 'button') if element.text.strip() == 'Cancel'
    ), False)).click()
    _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-mode-report']").click()
    _wait_for_workspace_element(flagship_driver, "[data-testid='pivot-mode-pivot']").click()
    logs = _save_evidence(flagship_driver, flagship_artifacts, "sidebar-filter-report-chart")
    assert not _relevant_browser_errors(logs)
