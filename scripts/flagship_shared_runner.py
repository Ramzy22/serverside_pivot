"""Run committed shared assertions against an installed Python release.

This runner deliberately has a small, explicit map. A test which merely checks
that a control or source file exists cannot qualify an observable requirement.
Unmapped cases remain missing in the release manifest.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Mapping, Sequence

from scripts.flagship_manifest import BoundCaseResult, QualificationBinding, load_committed_requirements
from tests.flagship_contract import Requirement


@dataclass(frozen=True)
class SharedAssertion:
    """Committed pytest assertion and all source files needed to execute it."""

    node: str
    sources: tuple[str, ...]


# Each mapped assertion checks a parsed, literal document against an independent
# expected result, including order, totals, numeric values, and Unicode text.
SHARED_ASSERTIONS: Mapping[str, tuple[SharedAssertion, ...]] = {
    "shared.export-csv.exact-document": (
        SharedAssertion(
            "tests/test_export_qualification_runtime.py::test_runtime_exports_fixture_with_order_filter_totals_numeric_and_unicode[csv]",
            ("tests/__init__.py", "tests/export_qualification_fixture.py", "tests/test_export_qualification_runtime.py"),
        ),
    ),
    "shared.export-xlsx.exact-workbook": (
        SharedAssertion(
            "tests/test_export_qualification_runtime.py::test_runtime_exports_fixture_with_order_filter_totals_numeric_and_unicode[xlsx]",
            ("tests/__init__.py", "tests/export_qualification_fixture.py", "tests/test_export_qualification_runtime.py"),
        ),
    ),
}


def unmapped_shared_ids(requirements: Sequence[Requirement]) -> tuple[str, ...]:
    """Return every required shared ID lacking a concrete installed assertion."""
    required = {test_id for requirement in requirements if requirement.required for test_id in requirement.shared_tests}
    return tuple(sorted(required - SHARED_ASSERTIONS.keys()))


def _committed_source(binding: QualificationBinding, relative: str) -> bytes:
    if not relative.startswith("tests/") or ".." in Path(relative).parts:
        raise ValueError("shared assertion source must be under tests/")
    repository = Path(binding.paths.get("source_repository", Path(__file__).resolve().parents[1]))
    return subprocess.run(
        ["git", "show", f"{binding.source_commit}:{relative}"],
        cwd=repository, check=True, capture_output=True,
    ).stdout


def _installed_package_location(python: Path, repository: Path) -> str:
    """Fail closed if the candidate interpreter imports source or lacks pytest."""
    probe = (
        "import importlib.util, json, pathlib; "
        "p=importlib.util.find_spec('pivot_engine'); "
        "q=importlib.util.find_spec('pytest'); "
        "print(json.dumps({'package': str(pathlib.Path(p.origin).resolve()) if p and p.origin else '', "
        "'pytest': bool(q)}))"
    )
    completed = subprocess.run([str(python), "-I", "-c", probe], capture_output=True, text=True, timeout=30)
    if completed.returncode:
        raise RuntimeError(f"installed Python preflight failed: {completed.stderr.strip()}")
    data = json.loads(completed.stdout)
    package = Path(data["package"]).resolve() if data.get("package") else None
    if not package or package.is_relative_to(repository.resolve()):
        raise RuntimeError("candidate Python does not import an installed pivot_engine")
    environment = python.resolve().parent.parent
    if not package.is_relative_to(environment):
        raise RuntimeError("pivot_engine is outside the candidate Python environment")
    if not data.get("pytest"):
        raise RuntimeError("candidate Python lacks declared pytest test dependency")
    return str(package)


def run_shared_qualification(
    binding: QualificationBinding,
    python_executable: str | Path,
    evidence_root: str | Path,
    *,
    requirements: Sequence[Requirement] | None = None,
) -> tuple[tuple[BoundCaseResult, ...], tuple[str, ...]]:
    """Return bound observed records and exact missing mappings.

    ``evidence_root`` must be the current invocation's ``runs/<run_id>``
    directory. The caller must include returned records in its envelope. A
    failure remains a failed case; an unmapped ID produces no invented record.
    Execute before removing the installed candidate environment.
    """
    requirements = tuple(requirements if requirements is not None else load_committed_requirements(binding))
    evidence_root = Path(evidence_root).resolve()
    if evidence_root.name != binding.run_id or evidence_root.parent.name != "runs":
        raise ValueError("shared evidence must be under runs/<current run_id>")
    python = Path(python_executable).resolve(strict=True)
    if "python" not in binding.paths or python != Path(binding.paths["python"]).resolve(strict=True):
        raise ValueError("shared tests must use the bound installed Python")
    if "wheel" not in binding.paths or "wheel" not in binding.sha256:
        raise ValueError("shared tests require a bound wheel")
    if hashlib.sha256(Path(binding.paths["wheel"]).read_bytes()).hexdigest() != binding.sha256["wheel"]:
        raise ValueError("bound wheel hash changed before shared tests")
    if "environment_record" not in binding.paths:
        raise ValueError("shared tests require the installed-environment record")
    environment_record = json.loads(Path(binding.paths["environment_record"]).read_text(encoding="utf-8"))
    if (environment_record.get("sourceCommit") != binding.source_commit
            or environment_record.get("wheelSha256") != binding.sha256["wheel"]):
        raise ValueError("installed-environment record differs from candidate")
    repository = Path(binding.paths.get("source_repository", Path(__file__).resolve().parents[1])).resolve()
    installed = _installed_package_location(python, repository)
    required = {test_id: requirement for requirement in requirements if requirement.required for test_id in requirement.shared_tests}
    unknown_mappings = set(SHARED_ASSERTIONS) - set(required)
    if unknown_mappings:
        raise ValueError(f"shared mapping is not in committed contract: {sorted(unknown_mappings)}")
    results: list[BoundCaseResult] = []
    for test_id in sorted(required.keys() & SHARED_ASSERTIONS.keys()):
        assertions = SHARED_ASSERTIONS[test_id]
        if not assertions or any(not assertion.node or not assertion.sources for assertion in assertions):
            raise ValueError(f"empty shared assertion mapping: {test_id}")
        with tempfile.TemporaryDirectory(prefix="pf-installed-shared-") as temporary:
            temporary_root = Path(temporary)
            source_hashes = {}
            for relative in sorted({source for assertion in assertions for source in assertion.sources}):
                content = _committed_source(binding, relative)
                target = temporary_root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
                source_hashes[relative] = hashlib.sha256(content).hexdigest()
            nodes = [str(temporary_root / assertion.node.split("::", 1)[0]) + "::" + assertion.node.split("::", 1)[1] for assertion in assertions]
            completed = subprocess.run(
                [str(python), "-I", "-m", "pytest", "-q", "--disable-warnings", "--confcutdir", str(temporary_root), *nodes],
                cwd=temporary_root, capture_output=True, text=True, timeout=300,
            )
        observation = {
            "runId": binding.run_id,
            "sourceCommit": binding.source_commit,
            "artifactSha256": dict(binding.sha256),
            "testId": test_id,
            "nodes": [assertion.node for assertion in assertions],
            "committedTestSourceSha256": source_hashes,
            "installedPackage": installed,
            "exitCode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        location = evidence_root / "shared" / (test_id.replace(".", "_") + ".json")
        location.parent.mkdir(parents=True, exist_ok=True)
        content = (json.dumps(observation, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
        location.write_bytes(content)
        evidence = str(location)
        passed = completed.returncode == 0 and re.search(r"\b[1-9][0-9]* passed\b", completed.stdout) is not None
        for host in required[test_id].hosts:
            results.append(BoundCaseResult(
                requirement_id=required[test_id].id,
                system="product", host=host, test_id=test_id,
                result="passed" if passed else "failed",
                observed=f"Installed Python pytest exit {completed.returncode}; {len(assertions)} committed assertion(s); package {installed}",
                browser="", evidence_paths=(evidence,), run_id=binding.run_id,
                source_commit=binding.source_commit, sha256=dict(binding.sha256),
                loaded_assets={}, evidence_sha256={evidence: hashlib.sha256(content).hexdigest()},
            ))
    return tuple(results), unmapped_shared_ids(requirements)
