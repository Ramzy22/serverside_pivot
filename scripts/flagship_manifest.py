"""Fail-closed, current-run qualification and summary-only publication.

CaseResult deliberately contains no release provenance. BoundCaseResult adds
private provenance; bare historical records cannot qualify a current build.
The envelope extends Task 3's schema/run/results format without upgrading its
unbound observations into current release evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Mapping, Sequence
import xml.etree.ElementTree as ET

from scripts.flagship_artifacts import ArtifactBinding
from tests.flagship_capabilities import CAPABILITIES
from tests.flagship_contract import CaseResult, DIFFERENCES, HOSTS, Requirement, RESULTS, load_requirements, validate_requirements


@dataclass(frozen=True)
class QualificationBinding(ArtifactBinding):
    run_id: str = ""


@dataclass(frozen=True)
class BoundCaseResult(CaseResult):
    run_id: str = ""
    source_commit: str = ""
    sha256: Mapping[str, str] = field(default_factory=dict)
    loaded_assets: Mapping[str, str] = field(default_factory=dict)
    evidence_sha256: Mapping[str, str] = field(default_factory=dict)


ISSUES = frozenset({
    "contract_invalid", "binding_invalid", "case_unmapped", "case_duplicate", "case_missing",
    "case_invalid", "case_unbound", "run_mismatch", "commit_mismatch", "hash_mismatch",
    "asset_mismatch", "evidence_invalid", "reference_unobserved", "browser_unavailable",
    "junit_missing", "junit_invalid", "junit_empty", "junit_skipped", "junit_failures", "junit_errors",
    "case_envelope_invalid", "browser_exit_nonzero", "qualification_incomplete", "qualification_issue",
})


def _hash(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _identifier(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", value) is not None


def read_committed_contract(binding: ArtifactBinding) -> bytes:
    """Read the exact contract archived for the build, ignoring worktree drift."""
    if not isinstance(binding.source_commit, str) or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", binding.source_commit) is None:
        raise ValueError("invalid contract source commit")
    repository = binding.paths.get("source_repository", Path(__file__).resolve().parents[1])
    return subprocess.run(
        ["git", "show", f"{binding.source_commit}:tests/flagship_requirements.json"],
        cwd=repository, check=True, capture_output=True,
    ).stdout


def load_committed_requirements(binding: ArtifactBinding) -> tuple[Requirement, ...]:
    """Validate the complete inventory and hash-bound contract from source Git."""
    data = read_committed_contract(binding)
    if hashlib.sha256(data).hexdigest() != binding.sha256.get("contract"):
        raise ValueError("contract hash does not match built source commit")
    with tempfile.TemporaryDirectory(prefix="pf-contract-") as directory:
        path = Path(directory) / "requirements.json"
        path.write_bytes(data)
        requirements = load_requirements(path)
    validate_requirements(requirements, CAPABILITIES)
    return requirements


def _binding_valid(binding: ArtifactBinding) -> bool:
    return (
        isinstance(binding.source_commit, str)
        and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", binding.source_commit) is not None
        and _identifier(getattr(binding, "run_id", ""))
        and all(_hash(binding.sha256.get(key)) for key in ("source_commit", "wheel", "npm_tarball", "fixture"))
        and binding.sha256["source_commit"] == hashlib.sha256(binding.source_commit.encode()).hexdigest()
        and all(isinstance(key, str) and _hash(value) for key, value in binding.sha256.items())
        and all(binding.expected_loaded_assets.get(host) and all(
            isinstance(path, str) and path.startswith("/") and _hash(value)
            and binding.sha256.get(f"{host}:{path}") == value
            for path, value in binding.expected_loaded_assets[host].items()) for host in HOSTS)
        and all(isinstance(binding.versions.get(tool), str) and binding.versions[tool].strip()
                for tool in ("python", "node", "dash", "react", "streamlit"))
    )


def load_case_envelope(path: Path, binding: QualificationBinding) -> tuple[BoundCaseResult, ...]:
    """Read only an explicit envelope for this run, commit and complete hash set.

    Per-case provenance is also required: top-level metadata is never attached
    to old records on the caller's behalf. Malformed JSON/IO failures propagate.
    """
    envelope = json.loads(path.read_text(encoding="utf-8"))
    if (not isinstance(envelope, dict) or envelope.get("schema_version") != 1
            or envelope.get("run_id") != binding.run_id
            or envelope.get("source_commit") != binding.source_commit
            or envelope.get("sha256") != binding.sha256
            or not isinstance(envelope.get("results"), list)):
        raise ValueError("case envelope binding does not match this qualification")
    records = []
    for record in envelope["results"]:
        if not isinstance(record, dict) or not {"run_id", "source_commit", "sha256", "evidence_sha256", "loaded_assets"} <= record.keys():
            raise ValueError("case envelope binding is absent from a record")
        try:
            case = BoundCaseResult(**{**record, "evidence_paths": tuple(record.get("evidence_paths", ()))})
        except TypeError as error:
            raise ValueError("case envelope record does not match schema") from error
        if case.run_id != binding.run_id or case.source_commit != binding.source_commit or case.sha256 != binding.sha256:
            raise ValueError("case envelope binding does not match this qualification")
        records.append(case)
    return tuple(records)


def _case_issues(case: CaseResult, binding: ArtifactBinding, requirement: Requirement) -> list[str]:
    issues = []
    if not isinstance(case, BoundCaseResult):
        return ["case_unbound"]
    if case.run_id != getattr(binding, "run_id", None):
        issues.append("run_mismatch")
    if case.source_commit != binding.source_commit:
        issues.append("commit_mismatch")
    if case.sha256 != binding.sha256:
        issues.append("hash_mismatch")
    if case.result not in RESULTS or case.result == "not_applicable":
        issues.append("case_invalid")
    if case.result == "passed":
        browser_case = case.system == "ag_grid" or case.test_id in requirement.browser_tests.get(case.host, ())
        if browser_case and (not isinstance(case.browser, str) or not case.browser.strip() or case.browser.lower() in {"unavailable", "unknown"}):
            issues.append("browser_unavailable")
        if not isinstance(case.observed, str) or not case.observed.strip():
            issues.append("case_invalid")
        if not case.evidence_paths or not isinstance(case.evidence_sha256, Mapping) or set(case.evidence_sha256) != set(case.evidence_paths):
            issues.append("evidence_invalid")
        observations = []
        for name in case.evidence_paths:
            try:
                path = Path(name).resolve(strict=True)
                # Bind raw files to their current invocation, not a retained old
                # screenshot/observation with a newly attached passed record.
                if ("runs", case.run_id) not in tuple(zip(path.parts, path.parts[1:])):
                    issues.append("evidence_invalid")
                data = path.read_bytes()
                if hashlib.sha256(data).hexdigest() != case.evidence_sha256.get(name):
                    issues.append("evidence_invalid")
                if path.suffix == ".json":
                    observations.append(json.loads(data))
            except (OSError, ValueError, TypeError):
                issues.append("evidence_invalid")
        if case.system == "product" and browser_case:
            expected = binding.expected_loaded_assets.get(case.host, {})
            observed_assets = {}
            for observation in observations:
                if isinstance(observation, dict) and isinstance(observation.get("loadedAssetSha256"), dict):
                    for path, value in observation["loadedAssetSha256"].items():
                        if path in observed_assets and observed_assets[path] != value:
                            issues.append("asset_mismatch")
                        observed_assets[path] = value
            if (not isinstance(case.loaded_assets, Mapping) or not case.loaded_assets
                    or not any(isinstance(path, str) and path.endswith((".js", ".mjs")) for path in case.loaded_assets)
                    or any(expected.get(path) != value for path, value in case.loaded_assets.items())
                    or observed_assets != case.loaded_assets):
                issues.append("asset_mismatch")
        elif case.system == "ag_grid":
            def live(observation):
                if not isinstance(observation, dict):
                    return False
                config = observation.get("config")
                if not isinstance(config, dict):
                    return False
                options = config.get("options", {})
                return (config.get("version") == requirement.ag_grid.get("version")
                        and config.get("edition") == requirement.ag_grid.get("edition")
                        and config.get("fixtureSha256") == binding.sha256.get("fixture")
                        and (config.get("rowModelType") == "serverSide" or isinstance(options, dict) and options.get("rowModelType") == "serverSide")
                        and isinstance(options, dict)
                        and _reference_options_match(config, requirement.ag_grid.get("options", {}))
                        and _live_ssrm_trace(observation.get("trace"), options)
                        and isinstance(observation.get("observed"), str) and bool(observation["observed"].strip()))
            if not any(live(item) for item in observations):
                issues.append("reference_unobserved")
    return sorted(set(issues))


def _reference_options_match(config: Mapping, required: Mapping) -> bool:
    options = config.get("options", {})
    for key, value in required.items():
        if key == "modules":
            # Task 3 records the module registry beside grid options. Extra
            # supporting modules do not weaken the required baseline.
            modules = config.get("modules", options.get("modules", []))
            if (not isinstance(modules, list) or not all(isinstance(module, str) for module in modules)
                    or not isinstance(value, list) or not set(value) <= set(modules)):
                return False
        elif options.get(key) != value:
            return False
    return True


def _live_ssrm_trace(trace: object, options: Mapping) -> bool:
    """Require a datasource request and successful response for this config.

    Reference hooks retain the actual getRows payload and parsed HTTP result.
    A label, empty object or request-only event is not a live observation.
    """
    if not isinstance(trace, list):
        return False
    for entry in trace:
        if not isinstance(entry, dict) or entry.get("error"):
            continue
        request, response = entry.get("request"), entry.get("response")
        if not isinstance(request, dict) or not isinstance(response, dict):
            continue
        start, end = request.get("startRow"), request.get("endRow")
        rows, count = response.get("rowData"), response.get("rowCount")
        if (type(start) is not int or type(end) is not int or start < 0 or end <= start
                or not isinstance(rows, list) or not rows or not all(isinstance(row, dict) and row for row in rows)
                or type(count) is not int or count < len(rows)
                or len(rows) > end - start
                or not isinstance(request.get("groupKeys"), list)
                or not all(isinstance(key, str) for key in request["groupKeys"])
                or not isinstance(request.get("filterModel"), dict)
                or not isinstance(request.get("sortModel"), list)
                or request.get("pivotMode") is not options.get("pivotMode")):
            continue
        columns_match = True
        for key, option in (("rowGroupCols", "rowGroupFields"), ("pivotCols", "pivotFields"), ("valueCols", "valueFields")):
            columns = request.get(key)
            if (not isinstance(columns, list) or not all(isinstance(column, dict) and isinstance(column.get("field"), str) for column in columns)
                    or [column["field"] for column in columns] != options.get(option)):
                columns_match = False
                break
        if columns_match and len(request["groupKeys"]) <= len(request["rowGroupCols"]):
            return True
    return False


def _status(cases: list[dict]) -> str:
    for state in ("failed", "blocked", "skipped", "unverified"):
        if any(case["result"] == state for case in cases):
            return state
    return "passed" if cases and all(case["result"] == "passed" for case in cases) else "unverified"


def evaluate_manifest(requirements: tuple[Requirement, ...], binding: ArtifactBinding,
                      cases: Sequence[CaseResult]) -> dict[str, object]:
    """Evaluate every mapped test per applicable host; reference absence is independent.

    Multiple attempts must be resolved explicitly by a caller before evaluation;
    duplicate records fail instead of a last-write-wins pass hiding a failure.
    """
    requirements = tuple(requirements)
    global_issues = []
    try:
        validate_requirements(requirements, CAPABILITIES)
        if requirements != load_committed_requirements(binding):
            raise ValueError("requirements differ from the built source contract")
    except (OSError, ValueError, TypeError, subprocess.CalledProcessError):
        global_issues.append("contract_invalid")
    if (not requirements or len({item.id for item in requirements}) != len(requirements)
            or not any(item.required for item in requirements)
            or any(not _identifier(item.id) or not set(item.hosts) <= set(HOSTS)
                   or item.ag_grid.get("result") == "not_applicable" and item.hosts
                   and (len(item.hosts) != 1 or item.difference != "not_comparable" or not item.ag_grid.get("reason"))
                   or item.required and (not item.hosts or not item.shared_tests
                      or set(item.browser_tests) != set(item.hosts)
                      or any(not item.browser_tests[host] for host in item.hosts)) for item in requirements)):
        global_issues.append("contract_invalid")
    if not _binding_valid(binding):
        global_issues.append("binding_invalid")
    allowed = {}
    for requirement in requirements:
        for host in requirement.hosts:
            for test in (*requirement.shared_tests, *requirement.browser_tests.get(host, ())):
                allowed[(requirement.id, "product", host, test)] = requirement
        if requirement.ag_grid.get("result") != "not_applicable":
            allowed[(requirement.id, "ag_grid", None, requirement.ag_grid.get("test_id"))] = requirement
    recorded = {}
    system_issues = {"product": [], "ag_grid": []}
    for case in cases:
        system = case.system if case.system in system_issues else "product"
        key = (case.requirement_id, case.system, case.host, case.test_id)
        if key not in allowed:
            # Explicit not_applicable records are accepted only for contract
            # exclusions; they never stand in for an applicable test.
            excluded = next((item for item in requirements if item.id == case.requirement_id), None)
            if (case.result == "not_applicable" and excluded is not None
                    and (case.system == "product" and case.host in HOSTS and case.host not in excluded.hosts
                         or case.system == "ag_grid" and case.host is None
                         and excluded.ag_grid.get("result") == "not_applicable")):
                continue
            system_issues[system].append("case_unmapped")
            continue
        if key in recorded:
            system_issues[system].append("case_duplicate")
            recorded[key]["result"] = "failed"
            recorded[key]["issues"].append("case_duplicate")
            continue
        issues = _case_issues(case, binding, allowed[key])
        recorded[key] = {
            "testId": case.test_id, "result": "failed" if issues else case.result,
            "reportedResult": case.result, "observed": case.observed, "browser": case.browser,
            "evidence": list(case.evidence_paths),
            "runId": getattr(case, "run_id", ""), "sourceCommit": getattr(case, "source_commit", ""),
            "sha256": dict(getattr(case, "sha256", {})),
            "loadedAssetSha256": dict(getattr(case, "loaded_assets", {})),
            "evidenceSha256": dict(getattr(case, "evidence_sha256", {})), "issues": issues,
        }
    rows = {}
    release_ok = not global_issues and not system_issues["product"]
    parity_ok = not global_issues and not any(system_issues.values())
    for requirement in requirements:
        def section(system, host=None):
            excluded = host not in requirement.hosts if system == "product" else requirement.ag_grid.get("result") == "not_applicable"
            if excluded:
                return {"result": "not_applicable", "cases": [], "issues": []}
            expected = (*requirement.shared_tests, *requirement.browser_tests.get(host, ())) if system == "product" else (requirement.ag_grid.get("test_id"),)
            entries = [recorded.get((requirement.id, system, host, test), {
                "testId": test, "result": "unverified", "issues": ["case_missing"], "evidence": [],
            }) for test in expected]
            return {"result": _status(entries), "cases": entries,
                    "issues": sorted({issue for entry in entries for issue in entry["issues"]})}
        hosts = {host: section("product", host) for host in HOSTS}
        reference = section("ag_grid")
        product_ok = all(hosts[host]["result"] == "passed" for host in requirement.hosts)
        reference_ok = reference["result"] in {"passed", "not_applicable"}
        if requirement.required:
            release_ok = release_ok and product_ok
            parity_ok = parity_ok and product_ok and reference_ok
        rows[requirement.id] = {"inventoryId": requirement.inventory_id, "required": requirement.required,
            "difference": requirement.difference, "hosts": hosts, "reference": reference,
            "releaseResult": "not_applicable" if not requirement.hosts else "passed" if product_ok else "failed",
            "parityResult": "not_applicable" if not requirement.hosts else "passed" if product_ok and reference_ok else "failed"}
    return {"schemaVersion": 1, "runId": getattr(binding, "run_id", ""),
        "releaseResult": "passed" if release_ok else "failed", "parityResult": "passed" if parity_ok else "failed",
        "issues": sorted(set(global_issues)), "systemIssues": system_issues,
        "binding": {"sourceCommit": binding.source_commit, "sha256": dict(binding.sha256),
                    "versions": dict(binding.versions), "expectedLoadedAssets": binding.expected_loaded_assets,
                    "paths": {key: str(value) for key, value in binding.paths.items()}},
        "requirements": rows}


def apply_junit_result(raw: dict, path: Path, *, system: str) -> None:
    """Merge process evidence without letting summary skip counts disappear."""
    if system not in {"product", "ag_grid"}:
        raise ValueError("unknown qualification system")
    issues = []
    counts = {"tests": 0, "skipped": 0, "failures": 0, "errors": 0}
    try:
        root = ET.parse(path).getroot()
        if root.tag not in {"testsuite", "testsuites"}:
            raise ValueError("unknown JUnit root")
        cases = list(root.iter("testcase"))
        if not cases:
            issues.append("junit_empty")
        counts["tests"] = len(cases)
        for node in root.iter():
            if node.tag not in {"testsuite", "testsuites"}:
                continue
            for key in counts:
                if key in node.attrib:
                    count = int(node.attrib[key])
                    if count < 0:
                        raise ValueError("negative JUnit count")
                    if key == "tests" and count != len(list(node.iter("testcase"))):
                        raise ValueError("JUnit declared test count does not match records")
                    if key != "tests" and count:
                        issues.append(f"junit_{key}")
                        counts[key] = max(counts[key], count)
        for key, child in (("skipped", "skipped"), ("failures", "failure"), ("errors", "error")):
            count = sum(case.find(child) is not None for case in cases)
            if count:
                counts[key] = max(counts[key], count)
                issues.append(f"junit_{key}")
    except OSError:
        issues.append("junit_missing")
    except (ET.ParseError, ValueError, TypeError):
        issues.append("junit_invalid")
    raw.setdefault("junit", {})[system] = counts
    raw["systemIssues"][system] = sorted(set(raw["systemIssues"][system]) | set(issues))
    if issues:
        raw["parityResult"] = "failed"
        if system == "product":
            raw["releaseResult"] = "failed"


def publish_safe_manifest(raw: Mapping[str, object], destination: Path) -> None:
    """Reconstruct an allowlisted JSON document; never recursively copy raw data.

    Raw paths, outputs, observations, browser logs and exception reasons stay in
    the private report. Public evidence references are content hashes only.
    """
    def result(value):
        return value if value in RESULTS | {"unverified"} else "failed"

    def issues(values):
        return sorted({value if isinstance(value, str) and value in ISSUES else "qualification_issue"
                       for value in values}) if isinstance(values, (list, tuple)) else ["qualification_issue"]

    def section(value):
        if not isinstance(value, Mapping):
            value = {}
        cases = []
        for case in value.get("cases", []):
            if not isinstance(case, Mapping) or not _identifier(case.get("testId")):
                continue
            hashes = case.get("evidenceSha256", {})
            assets = case.get("loadedAssetSha256", {})
            browser = case.get("browser")
            cases.append({"testId": case["testId"], "result": result(case.get("result")),
                "issues": issues(case.get("issues", [])),
                "browser": browser if isinstance(browser, str) and re.fullmatch(r"(?:chrome|chromium|firefox|MicrosoftEdge|safari) [0-9][0-9.]{0,39}", browser, re.IGNORECASE) else None,
                "loadedAssetSha256": sorted({value for value in assets.values() if _hash(value)}) if isinstance(assets, Mapping) else [],
                "evidenceSha256": sorted({value for value in hashes.values() if _hash(value)}) if isinstance(hashes, Mapping) else []})
        return {"result": result(value.get("result")), "issues": issues(value.get("issues", [])), "cases": cases}

    binding = raw.get("binding", {})
    hashes = binding.get("sha256", {})
    versions = binding.get("versions", {})
    safe = {"schemaVersion": 1, "runId": raw.get("runId") if _identifier(raw.get("runId")) else None,
        "releaseResult": result(raw.get("releaseResult")), "parityResult": result(raw.get("parityResult")),
        "issues": issues(raw.get("issues", [])),
        "systemIssues": {system: issues(raw.get("systemIssues", {}).get(system, [])) for system in ("product", "ag_grid")},
        "binding": {"sourceCommit": binding.get("sourceCommit") if isinstance(binding.get("sourceCommit"), str) and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", binding["sourceCommit"]) else None,
            "sha256": {key: hashes[key] for key in ("source_commit", "contract", "wheel", "npm_tarball", "fixture") if _hash(hashes.get(key))},
            "versions": {key: versions[key] for key in ("python", "node", "npm", "dash", "react", "streamlit", "browser", "driver")
                         if isinstance(versions.get(key), str) and re.fullmatch(r"(?:Python |Chrome |chromedriver |v)?[0-9][0-9A-Za-z.+_-]{0,59}", versions[key])},
            "expectedAssetSha256": {host: sorted({value for value in binding.get("expectedLoadedAssets", {}).get(host, {}).values() if _hash(value)}) for host in HOSTS}},
        "requirements": {}}
    for requirement_id, row in raw.get("requirements", {}).items():
        if not _identifier(requirement_id) or not isinstance(row, Mapping):
            continue
        safe["requirements"][requirement_id] = {
            "inventoryId": row.get("inventoryId") if _identifier(row.get("inventoryId")) else None,
            "required": row.get("required") is True,
            "difference": row.get("difference") if row.get("difference") in DIFFERENCES else "unverified",
            "releaseResult": result(row.get("releaseResult")), "parityResult": result(row.get("parityResult")),
            "hosts": {host: section(row.get("hosts", {}).get(host, {})) for host in HOSTS},
            "reference": section(row.get("reference", {})),
        }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(safe, indent=2) + "\n", encoding="utf-8")
