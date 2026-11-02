"""Release-tooling regressions; synthetic evidence here never qualifies a product."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from scripts.flagship_harness import committed_browser_harness
from scripts.flagship_manifest import BoundCaseResult, QualificationBinding, _case_issues
from scripts.flagship_shared_client import SHARED_CLIENT_ASSERTIONS, shared_client_cases
from scripts.flagship_shared_runner import SHARED_ASSERTIONS, unmapped_shared_ids
from tests.flagship_contract import load_requirements


def digest(data):
    return hashlib.sha256(data if isinstance(data, bytes) else data.encode()).hexdigest()


@pytest.fixture
def reuse(tmp_path):
    repository = tmp_path / "repository"
    (repository / "tests").mkdir(parents=True)
    (repository / "scripts").mkdir()
    (repository / "examples").mkdir()
    (repository / "tests" / "helper.py").write_text("VALUE = 1\n")
    (repository / "scripts" / "coordinator.py").write_text("VALUE = 1\n")
    (repository / "examples" / "host.py").write_text("VALUE = 1\n")
    (repository / "pytest.ini").write_text("[pytest]\n")
    contract = Path(__file__).with_name("flagship_requirements.json").read_bytes()
    (repository / "tests" / "flagship_requirements.json").write_bytes(contract)
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(["git", "-c", "user.name=Tooling test", "-c", "user.email=test@example.invalid",
                    "-c", "commit.gpgsign=false", "commit", "-qm", "Harness"], cwd=repository, check=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    binding = QualificationBinding(run_id="current", source_commit=commit,
        paths={"source_repository": repository},
        sha256={"source_commit": digest(commit), "contract": digest(contract), "fixture": digest("fixture"),
                "wheel": digest("wheel"), "npm_tarball": digest("npm"), "react:/entry.js": digest("entry")},
        host_origins={"react": "http://127.0.0.1:12345"},
        expected_loaded_assets={"react": {"/entry.js": digest("entry")}})
    root = tmp_path / "runs" / binding.run_id
    root.mkdir(parents=True)
    with committed_browser_harness(binding, root, coordinator_root=repository):
        pass
    requirement = next(item for item in load_requirements(repository / "tests" / "flagship_requirements.json")
                       if item.id == "copy-selection.logical-cells")
    requirement = replace(requirement, hosts=("react",), browser_tests={"react": requirement.browser_tests["react"]})
    node = SHARED_CLIENT_ASSERTIONS[requirement.shared_tests[0]].node_template.format(host="react")
    report = root / "product-parity.junit.xml"
    report.write_text('<testsuite tests="1"><testcase classname="tests.test_flagship_workflows" '
                      f'name="{node.split("::")[1]}"/></testsuite>')
    observation = root / "observation.json"
    observation.write_text(json.dumps({"requirement": requirement.id,
        "result": {"result": "passed", "logicalCellsVerified": True, "clipboardBytesVerified": True,
                   "displayOrderVerified": True}, "parsedTSV": [["100.25", "2"], ["49.75", "3"]],
        "loadedAssetSha256": {"/entry.js": digest("entry")}}))
    case = BoundCaseResult(requirement.id, "product", "react", requirement.browser_tests["react"][0],
        "passed", "Real assertion execution (synthetic tooling fixture)", "Chrome 140", (str(observation),),
        run_id=binding.run_id, source_commit=commit, sha256=dict(binding.sha256),
        loaded_assets={"/entry.js": digest("entry")}, evidence_sha256={str(observation): digest(observation.read_bytes())})
    return binding, requirement, case, node, report, root


def project(values, cases=None, nodes=None, reports=None):
    binding, requirement, case, node, report, root = values
    return shared_client_cases(binding, (requirement,), (case,) if cases is None else cases,
        {"parity": (node,)} if nodes is None else nodes,
        {"parity": report} if reports is None else reports, root)


def test_client_inventory_composes_with_task2_python_assertions_for_43_baseline_ids():
    requirements = load_requirements(Path(__file__).with_name("flagship_requirements.json"))
    required = {test for item in requirements if item.required for test in item.shared_tests}
    task2_python = {"shared.layout-field-zones.move", "shared.formula-columns.add-remove",
                    "shared.custom-dimensions.add-remove", "shared.report-mode.restore-definition"}
    assert len(SHARED_CLIENT_ASSERTIONS) == 39
    assert not set(SHARED_CLIENT_ASSERTIONS) & SHARED_ASSERTIONS.keys()
    assert task2_python <= SHARED_ASSERTIONS.keys()
    assert set(SHARED_CLIENT_ASSERTIONS) == required - SHARED_ASSERTIONS.keys()
    assert not unmapped_shared_ids(requirements)


def test_every_shared_client_mapping_is_reviewed_and_matches_executable_host_plan():
    from scripts.certify_flagship import _product_browser_plan, _product_phase
    from tests.flagship_remaining_4 import ACCEPTANCE_CHECKS
    requirements = load_requirements(Path(__file__).with_name('flagship_requirements.json'))
    plan = _product_browser_plan(requirements)
    observed = set()
    for requirement in requirements:
        if not requirement.required:
            continue
        for test_id in requirement.shared_tests:
            assertion = SHARED_CLIENT_ASSERTIONS.get(test_id)
            if assertion is None:
                continue
            assert assertion.reviewed
            for host in requirement.hosts:
                node = plan[(requirement.id, host, assertion.browser_test_id)]
                assert node == assertion.node_template.format(host=host)
                assert _product_phase(node) == assertion.phase
            if requirement.id in ACCEPTANCE_CHECKS:
                assert ACCEPTANCE_CHECKS[requirement.id] in assertion.checks
            observed.add(test_id)
    assert observed == SHARED_CLIENT_ASSERTIONS.keys()


def test_reuse_is_per_host_exact_node_and_explicitly_not_independent(reuse):
    result, = project(reuse)
    assert result.result == "passed" and result.host == "react"
    assert "reused execution" in result.observed and "not an independent execution" in result.observed
    assert result.test_id == "shared.copy-selection.logical-cells"
    assert _case_issues(result, reuse[0], reuse[1]) == []
    assert project(reuse, nodes={"parity": ()}) == ()
    assert project(reuse, cases=()) == ()


@pytest.mark.parametrize("damage", ["run_id", "source_commit", "sha256", "host", "test_id", "loaded_assets",
                                   "foreign_asset", "bad_asset", "evidence", "duplicate", "failed", "skipped"])
def test_reuse_rejects_wrong_case_binding_assets_and_failed_execution(reuse, damage):
    case = reuse[2]
    modifications = {"run_id": "other", "source_commit": "0" * 40, "sha256": {**case.sha256, "fixture": "0" * 64},
        "host": "dash", "test_id": "browser.other", "loaded_assets": {},
        "foreign_asset": {"https://foreign.invalid/entry.js": digest("entry")},
        "bad_asset": {"/entry.js": "0" * 64}, "evidence": {}, "failed": "failed", "skipped": "skipped"}
    if damage == "duplicate":
        values = (case, case)
    else:
        field = {"foreign_asset": "loaded_assets", "bad_asset": "loaded_assets", "evidence": "evidence_sha256",
                 "failed": "result", "skipped": "result"}.get(damage, damage)
        values = (replace(case, **{field: modifications[damage]}),)
    results = project(reuse, cases=values)
    assert not results or all(result.result != "passed" for result in results)


@pytest.mark.parametrize("xml", [None, '<testsuite tests="0"/>', '<testsuite tests="1"><testcase classname="wrong" name="wrong"/></testsuite>',
    '<testsuite tests="1"><testcase classname="tests.test_flagship_workflows" name="{name}"><skipped/></testcase></testsuite>',
    '<testsuite tests="1"><testcase classname="tests.test_flagship_workflows" name="{name}"><failure/></testcase></testsuite>',
    '<testsuite tests="2"><testcase classname="tests.test_flagship_workflows" name="{name}"/><testcase classname="tests.test_flagship_workflows" name="{name}"/></testsuite>'])
def test_missing_foreign_skipped_failed_duplicate_junit_never_passes(reuse, xml):
    if xml is None:
        reuse[4].unlink()
    else:
        reuse[4].write_text(xml.format(name=reuse[3].split("::")[1]))
    assert all(case.result != "passed" for case in project(reuse))


@pytest.mark.parametrize("when,where", [("before", "coordinator"), ("after", "coordinator"), ("after", "archive")])
def test_mutable_transitive_helpers_cannot_acquire_committed_provenance(reuse, when, where):
    binding, _, _, _, _, root = reuse
    repository = binding.paths["source_repository"]
    helper = repository / "tests" / "helper.py"
    if when == "before":
        helper.write_text("VALUE = 2\n")
    with pytest.raises(ValueError, match="harness"):
        with committed_browser_harness(binding, root, coordinator_root=repository) as harness:
            if when == "after":
                (helper if where == "coordinator" else harness.root / "tests" / "helper.py").write_text("VALUE = 2\n")
    assert not (root / "browser-harness.json").exists(), "Failed rerun retained an earlier completion receipt"


def test_manifest_shared_browser_checks_assets_and_reuse_receipt(reuse):
    result, = project(reuse)
    for assets in ({}, {"/entry.js": "0" * 64}, {"https://foreign.invalid/entry.js": digest("entry")}):
        assert "asset_mismatch" in _case_issues(replace(result, loaded_assets=assets), reuse[0], reuse[1])
    assert "browser_unavailable" in _case_issues(replace(result, browser=""), reuse[0], reuse[1])
    assert "evidence_invalid" in _case_issues(replace(result, evidence_paths=reuse[2].evidence_paths,
        evidence_sha256=reuse[2].evidence_sha256), reuse[0], reuse[1])


def test_receipt_and_original_evidence_remain_hash_bound(reuse):
    result, = project(reuse)
    Path(reuse[2].evidence_paths[0]).write_text('{}')
    assert "evidence_invalid" in _case_issues(result, reuse[0], reuse[1])


@pytest.mark.parametrize("key", ["source_commit", "contract", "fixture", "wheel", "npm_tarball"])
def test_each_artifact_or_fixture_digest_is_mandatory(reuse, key):
    case = reuse[2]
    results = project(reuse, cases=(replace(case, sha256={**case.sha256, key: "0" * 64}),))
    assert results and all(result.result == "failed" for result in results)


def test_harness_digest_cannot_hide_changed_transitive_source_manifest(reuse):
    result, = project(reuse)
    harness = reuse[-1] / "browser-harness.json"
    data = json.loads(harness.read_text())
    data["committedHarnessSha256"]["tests/helper.py"] = "0" * 64
    harness.write_text(json.dumps(data))
    receipt_path = Path(result.evidence_paths[-1])
    receipt = json.loads(receipt_path.read_text())
    receipt["harness"]["sha256"] = digest(harness.read_bytes())
    receipt_path.write_text(json.dumps(receipt))
    result = replace(result, evidence_sha256={**result.evidence_sha256,
        str(harness): digest(harness.read_bytes()), str(receipt_path): digest(receipt_path.read_bytes())})
    assert "evidence_invalid" in _case_issues(result, reuse[0], reuse[1])


@pytest.mark.parametrize("field,value", [("node", "tests/foreign.py::test_green"), ("host", "dash"),
    ("runId", "old"), ("sourceCommit", "0" * 40), ("phase", "exports"),
    ("artifactSha256", {"fixture": "0" * 64}), ("sharedTestId", "shared.copy-selection.headers"),
    ("independentExecution", True)])
def test_manifest_rechecks_receipt_identity_even_if_receipt_digest_is_rebound(reuse, field, value):
    result, = project(reuse)
    path = Path(result.evidence_paths[-1])
    data = json.loads(path.read_text())
    data[field] = value
    path.write_text(json.dumps(data))
    result = replace(result, evidence_sha256={**result.evidence_sha256, str(path): digest(path.read_bytes())})
    assert "evidence_invalid" in _case_issues(result, reuse[0], reuse[1])


def test_runner_uses_committed_archive_as_cwd_and_import_root(tmp_path, monkeypatch):
    from scripts import certify_flagship as runner
    from types import SimpleNamespace
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(runner.subprocess, "run", run)
    assert runner._run_browser_suite(tmp_path, {"PIVOT_FLAGSHIP_HARNESS_ROOT": str(tmp_path)},
        node_ids=["tests/test_flagship_workflows.py::test_real[react]"]) == 0
    assert calls[0][1]["cwd"] == tmp_path.resolve()
    assert calls[0][1]["env"]["PYTHONPATH"] == str(tmp_path.resolve())
    assert calls[0][1]["env"]["PYTHONNOUSERSITE"] == "1"
