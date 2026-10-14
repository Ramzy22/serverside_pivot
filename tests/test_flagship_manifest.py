"""Qualification rejects missing, stale and unsafe evidence independently per gate."""

from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path

import pytest

from scripts import flagship_manifest as manifest
from tests.flagship_contract import CaseResult, Requirement


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


@pytest.fixture
def qualification(tmp_path):
    binding = manifest.QualificationBinding(
        source_commit="a" * 40, run_id="current-run",
        sha256={"source_commit": digest("a" * 40), "wheel": digest("wheel"),
                "npm_tarball": digest("npm"), "fixture": digest("fixture"),
                **{f"{host}:/entry.js": digest(host) for host in ("dash", "react", "streamlit")}},
        expected_loaded_assets={host: {"/entry.js": digest(host)} for host in ("dash", "react", "streamlit")},
        versions={"python": "3.13", "node": "24", "dash": "3.2", "react": "19", "streamlit": "1.50"},
    )
    requirements = (
        Requirement("copy.cells", "selection", "Copy cells", "Exact cells", {
            "version": "36.2.0", "edition": "Enterprise", "row_model": "SSRM",
            "test_id": "reference.copy", "result": "unverified"}, "Exact cells", "unverified",
            ("Loaded fixture",), ("dash", "react", "streamlit"), True,
            ("unit.copy",), {host: ("browser.copy",) for host in ("dash", "react", "streamlit")}),
        Requirement("dash.transport", "dash-host", "Callback", "Callback response", {
            "result": "not_applicable", "reason": "Dash integration only"}, "Response", "not_comparable",
            ("Dash only",), ("dash",), True, ("unit.transport",), {"dash": ("browser.transport",)}),
    )
    cases = []
    for requirement in requirements:
        expected = [("product", host, test) for host in requirement.hosts
                    for test in (*requirement.shared_tests, *requirement.browser_tests[host])]
        if requirement.ag_grid.get("test_id"):
            expected.append(("ag_grid", None, requirement.ag_grid["test_id"]))
        for index, (system, host, test_id) in enumerate(expected):
            evidence = tmp_path / "runs" / binding.run_id / f"{requirement.id}-{index}.json"
            evidence.parent.mkdir(parents=True, exist_ok=True)
            evidence.write_text(json.dumps({
                "observed": "Observed exact output", "trace": [{"request": "getRows"}],
                "config": {"version": "36.2.0", "edition": "Enterprise", "rowModelType": "serverSide",
                           "fixtureSha256": binding.sha256["fixture"]},
            }))
            cases.append(manifest.BoundCaseResult(
                requirement.id, system, host, test_id, "passed", "Observed exact output", "Chrome 140",
                (str(evidence),), run_id=binding.run_id, source_commit=binding.source_commit,
                sha256=dict(binding.sha256), loaded_assets={"/entry.js": digest(host)} if host else {},
                evidence_sha256={str(evidence): hashlib.sha256(evidence.read_bytes()).hexdigest()},
            ))
    return requirements, binding, cases


def test_complete_bound_cases_pass_both_gates_and_declare_host_exclusions(qualification):
    requirements, binding, cases = qualification
    result = manifest.evaluate_manifest(requirements, binding, cases)
    assert result["releaseResult"] == "passed"
    assert result["parityResult"] == "passed"
    assert result["requirements"]["dash.transport"]["hosts"]["react"]["result"] == "not_applicable"
    assert result["requirements"]["dash.transport"]["reference"]["result"] == "not_applicable"
    assert result["requirements"]["copy.cells"]["hosts"]["dash"]["result"] == "passed"


@pytest.mark.parametrize("state", ["missing", "failed", "skipped", "blocked", "not_applicable"])
def test_one_applicable_host_case_cannot_be_hidden_by_other_passes(qualification, state):
    requirements, binding, cases = qualification
    target = next(i for i, case in enumerate(cases) if case.host == "react" and case.test_id == "browser.copy")
    if state == "missing":
        cases.pop(target)
    else:
        cases[target] = replace(cases[target], result=state, observed="private skip or failure reason")
    result = manifest.evaluate_manifest(requirements, binding, cases)
    assert result["releaseResult"] == "failed"
    assert result["parityResult"] == "failed"
    assert result["requirements"]["copy.cells"]["hosts"]["react"]["result"] != "passed"


def test_missing_reference_fails_only_parity_and_does_not_infer_live_pass_from_contract(qualification):
    requirements, binding, cases = qualification
    requirements = (replace(requirements[0], ag_grid={**requirements[0].ag_grid, "result": "passed"}), requirements[1])
    result = manifest.evaluate_manifest(requirements, binding, [case for case in cases if case.system == "product"])
    assert result["releaseResult"] == "passed"
    assert result["parityResult"] == "failed"
    assert result["requirements"]["copy.cells"]["reference"]["result"] == "unverified"


@pytest.mark.parametrize("damage", ["commit", "run", "fixture", "wheel", "npm_tarball", "bundle", "evidence", "unbound", "browser"])
def test_stale_or_unbound_product_passes_fail_release(qualification, damage):
    requirements, binding, cases = qualification
    index = 1  # Browser case; shared calculation tests need no browser assets.
    case = cases[index]
    if damage == "commit":
        case = replace(case, source_commit="b" * 40)
    elif damage == "run":
        case = replace(case, run_id="old-run")
    elif damage in ("fixture", "wheel", "npm_tarball"):
        case = replace(case, sha256={**case.sha256, damage: "0" * 64})
    elif damage == "bundle":
        case = replace(case, loaded_assets={"/entry.js": "0" * 64})
    elif damage == "evidence":
        Path(case.evidence_paths[0]).write_text("replaced private observation")
    elif damage == "browser":
        case = replace(case, browser="unavailable")
    else:
        case = CaseResult(**{key: value for key, value in asdict(case).items() if key in CaseResult.__dataclass_fields__})
    cases[index] = case
    result = manifest.evaluate_manifest(requirements, binding, cases)
    assert result["releaseResult"] == "failed"


@pytest.mark.parametrize("damage", ["missing", "trace", "fixture", "version", "run_path"])
def test_reference_requires_a_current_live_observation(qualification, damage):
    requirements, binding, cases = qualification
    index = next(i for i, case in enumerate(cases) if case.system == "ag_grid")
    case = cases[index]
    evidence = Path(case.evidence_paths[0])
    record = json.loads(evidence.read_text())
    if damage == "missing":
        evidence.unlink()
    elif damage == "run_path":
        old = evidence.parent.parent / "old-run" / evidence.name
        old.parent.mkdir()
        old.write_bytes(evidence.read_bytes())
        evidence = old
    else:
        if damage == "trace":
            record["trace"] = []
        elif damage == "fixture":
            record["config"]["fixtureSha256"] = "0" * 64
        else:
            record["config"]["version"] = "35.0.0"
        evidence.write_text(json.dumps(record))
    cases[index] = replace(case, evidence_paths=(str(evidence),), evidence_sha256={str(evidence): hashlib.sha256(evidence.read_bytes()).hexdigest()} if evidence.exists() else {})
    result = manifest.evaluate_manifest(requirements, binding, cases)
    assert result["releaseResult"] == "passed"
    assert result["parityResult"] == "failed"


def test_duplicate_and_unknown_tests_cannot_replace_required_cases(qualification):
    requirements, binding, cases = qualification
    assert manifest.evaluate_manifest(requirements, binding, [*cases, cases[0]])["releaseResult"] == "failed"
    cases[0] = replace(cases[0], test_id="unmapped-pass")
    assert manifest.evaluate_manifest(requirements, binding, cases)["releaseResult"] == "failed"


def test_case_envelope_rejects_stale_top_level_binding_and_does_not_upgrade_bare_task3_records(qualification, tmp_path):
    _, binding, cases = qualification
    path = tmp_path / "case-results.json"
    envelope = {"schema_version": 1, "run_id": binding.run_id, "source_commit": binding.source_commit,
                "sha256": binding.sha256, "results": [asdict(case) for case in cases]}
    path.write_text(json.dumps(envelope))
    assert len(manifest.load_case_envelope(path, binding)) == 9
    envelope["run_id"] = "historical-run"
    path.write_text(json.dumps(envelope))
    with pytest.raises(ValueError, match="binding"):
        manifest.load_case_envelope(path, binding)
    path.write_text(json.dumps({"schema_version": 1, "run_id": binding.run_id, "results": []}))
    with pytest.raises(ValueError, match="binding"):
        manifest.load_case_envelope(path, binding)


@pytest.mark.parametrize("xml", [
    '<testsuite tests="1" skipped="1"><testcase name="apparently-passed"/></testsuite>',
    '<testsuites><testsuite tests="1" skipped="0"><testcase><skipped/></testcase></testsuite></testsuites>',
    '<testsuite tests="1" errors="1"><testcase/></testsuite>',
    '<testsuite tests="1" failures="bad"><testcase/></testsuite>',
    '<testsuite tests="0"/>',
])
def test_junit_counts_and_children_cannot_be_ignored(qualification, tmp_path, xml):
    requirements, binding, cases = qualification
    report = tmp_path / "junit.xml"
    report.write_text(xml)
    result = manifest.evaluate_manifest(requirements, binding, cases)
    manifest.apply_junit_result(result, report, system="product")
    assert result["releaseResult"] == "failed"
    assert result["parityResult"] == "failed"


def test_clean_reference_junit_preserves_release_independence(qualification, tmp_path):
    requirements, binding, cases = qualification
    result = manifest.evaluate_manifest(requirements, binding, cases)
    report = tmp_path / "reference.xml"
    report.write_text('<testsuite tests="1"><testcase/></testsuite>')
    manifest.apply_junit_result(result, report, system="ag_grid")
    assert result["releaseResult"] == result["parityResult"] == "passed"
    report.write_text('<testsuite tests="1" skipped="1"><testcase/></testsuite>')
    manifest.apply_junit_result(result, report, system="ag_grid")
    assert result["releaseResult"] == "passed"
    assert result["parityResult"] == "failed"


def test_publication_allowlist_never_copies_tokens_rows_paths_or_exception_text(qualification, tmp_path):
    requirements, binding, cases = qualification
    raw = manifest.evaluate_manifest(requirements, binding, cases)
    secret = "Bearer customer-secret-123"
    raw.update({"token": secret, "rows": [[secret]], "failure": secret, "files": [secret]})
    raw["binding"]["paths"] = {"private": secret}
    raw["binding"]["versions"]["secret"] = secret
    raw["requirements"]["copy.cells"]["hosts"]["dash"]["cases"][0].update({"observed": secret, "evidence": [secret], "issues": [secret]})
    destination = tmp_path / "public" / "manifest.json"
    manifest.publish_safe_manifest(raw, destination)
    text = destination.read_text()
    assert secret not in text
    assert str(tmp_path) not in text
    public = json.loads(text)
    assert public["releaseResult"] == "passed"
    assert public["parityResult"] == "passed"
    assert public["binding"]["sha256"]["fixture"] == digest("fixture")
    browser_case = public["requirements"]["copy.cells"]["hosts"]["react"]["cases"][1]
    assert browser_case["browser"] == "Chrome 140"
    assert browser_case["loadedAssetSha256"] == [digest("react")]
    assert "loadedAssetSha256" not in public["binding"]  # Expected assets are not observations.


def test_empty_contract_and_empty_binding_cannot_pass():
    from scripts.flagship_artifacts import ArtifactBinding
    result = manifest.evaluate_manifest((), ArtifactBinding(), [])
    assert result["releaseResult"] == result["parityResult"] == "failed"


def test_not_applicable_reference_requires_a_host_only_contract_exclusion(qualification):
    requirements, binding, cases = qualification
    forged = replace(requirements[0], ag_grid={"result": "not_applicable", "reason": "Avoid missing reference"})
    result = manifest.evaluate_manifest((forged, requirements[1]), binding, [case for case in cases if case.system == "product"])
    assert result["parityResult"] == "failed"


def test_shared_runtime_case_does_not_need_a_browser_observation(qualification):
    requirements, binding, cases = qualification
    cases[0] = replace(cases[0], browser="", loaded_assets={})
    result = manifest.evaluate_manifest(requirements, binding, cases)
    assert result["releaseResult"] == result["parityResult"] == "passed"


def test_excluded_editing_has_no_fictitious_pass(qualification):
    requirements, binding, cases = qualification
    editing = Requirement("editing.excluded", "editing", "Editing unavailable", "Unavailable",
        {"result": "not_applicable", "reason": "Excluded from read-only release"}, "Unavailable",
        "unsupported", ("Read only",), (), False, (), {})
    result = manifest.evaluate_manifest((*requirements, editing), binding, cases)
    assert result["releaseResult"] == result["parityResult"] == "passed"
    assert result["requirements"]["editing.excluded"]["releaseResult"] == "not_applicable"
    assert result["requirements"]["editing.excluded"]["parityResult"] == "not_applicable"


def test_reference_options_must_match_the_declared_configuration(qualification):
    requirements, binding, cases = qualification
    configured = replace(requirements[0], ag_grid={**requirements[0].ag_grid, "options": {"pivotMode": True}})
    result = manifest.evaluate_manifest((configured, requirements[1]), binding, cases)
    assert result["releaseResult"] == "passed"
    assert result["parityResult"] == "failed"


def test_junit_declared_test_count_cannot_hide_missing_testcases(qualification, tmp_path):
    requirements, binding, cases = qualification
    result = manifest.evaluate_manifest(requirements, binding, cases)
    report = tmp_path / "junit.xml"
    report.write_text('<testsuite tests="9"><testcase/></testsuite>')
    manifest.apply_junit_result(result, report, system="product")
    assert result["releaseResult"] == "failed"


def test_installed_runner_publishes_failed_requirement_manifest_even_when_smoke_suite_passes(qualification, tmp_path, monkeypatch):
    from contextlib import contextmanager
    from scripts import certify_flagship as runner, flagship_artifacts

    _, binding, _ = qualification
    monkeypatch.setattr(flagship_artifacts, "build_artifacts", lambda source, output: binding)
    monkeypatch.setattr(flagship_artifacts, "cleanup_python_environment", lambda binding: None)

    @contextmanager
    def hosts(binding, output):
        yield {host: f"http://127.0.0.1:{39001 + index}" for index, host in enumerate(("dash", "react", "streamlit"))}

    monkeypatch.setattr(flagship_artifacts, "start_installed_hosts", hosts)

    def suite(evidence, environment, **kwargs):
        assert environment["PIVOT_FLAGSHIP_RUN_ID"] != "current-run"
        assert environment["PIVOT_FLAGSHIP_CASE_RESULTS"]
        kwargs["junit_path"].write_text('<testsuite tests="9">' + "".join(
            f'<testcase name="{test}[{host}]"/>' for test in runner.INSTALLED_BROWSER_CASES
            for host in ("dash", "react", "streamlit")) + '</testsuite>')
        return 0

    monkeypatch.setattr(runner, "_run_browser_suite", suite)
    evidence = tmp_path / "qualification"
    assert runner.main(["--installed-release", "--artifact-dir", str(evidence)]) == 1
    public = json.loads((evidence / "read-only-qualification.json").read_text())
    assert public["releaseResult"] == public["parityResult"] == "failed"
    assert public["requirements"]["initial-workspace.populated"]["hosts"]["dash"]["result"] == "unverified"
