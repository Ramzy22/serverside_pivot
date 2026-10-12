"""Versioned read-only workflow promises and independent observed results.

The contract is an inventory of acceptance criteria, never evidence that a
workflow passed. Browser execution and artifact qualification are separate.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable, Mapping


HOSTS = ("dash", "react", "streamlit")
DIFFERENCES = frozenset({"equivalent", "deliberately_different", "narrower", "unsupported", "unverified", "not_comparable"})
RESULTS = frozenset({"passed", "failed", "skipped", "blocked", "not_applicable"})


@dataclass(frozen=True)
class Requirement:
    id: str
    inventory_id: str
    task: str
    expected: str
    ag_grid: Mapping
    product_behavior: str
    difference: str
    constraints: tuple[str, ...]
    hosts: tuple[str, ...]
    required: bool
    shared_tests: tuple[str, ...]
    browser_tests: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class CaseResult:
    requirement_id: str
    system: str
    host: str | None
    test_id: str
    result: str
    observed: str
    browser: str
    evidence_paths: tuple[str, ...] = ()


def load_requirements(path: str | Path) -> tuple[Requirement, ...]:
    """Parse schema 1; expand versioned reference configurations per requirement."""
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("unsupported contract schema_version")
    records = document.get("requirements")
    references = document.get("references", {})
    if not isinstance(records, list) or not isinstance(references, dict):
        raise ValueError("contract schema requires requirements array and references object")
    parsed = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("requirement must be an object")
        values = dict(record)
        reference = values.get("ag_grid")
        if not isinstance(reference, dict):
            raise ValueError("requirement reference must be an object")
        reference = dict(reference)
        reference_id = reference.pop("reference", None)
        if reference_id is not None:
            if reference_id not in references or not isinstance(references[reference_id], dict):
                raise ValueError(f"unknown reference configuration: {reference_id}")
            reference = {**references[reference_id], **reference}
        values["ag_grid"] = reference
        for field in ("constraints", "hosts", "shared_tests"):
            if not isinstance(values.get(field), list):
                raise ValueError(f"requirement {field} must be an array")
            values[field] = tuple(values[field])
        if not isinstance(values.get("browser_tests"), dict):
            raise ValueError("requirement browser_tests must be an object")
        if any(not isinstance(ids, list) for ids in values["browser_tests"].values()):
            raise ValueError("browser_tests values must be arrays")
        values["browser_tests"] = {host: tuple(ids) for host, ids in values["browser_tests"].items()}
        try:
            parsed.append(Requirement(**values))
        except TypeError as error:
            raise ValueError("requirement fields do not match schema 1") from error
    return tuple(parsed)


def _text(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _ids(values) -> bool:
    return isinstance(values, (tuple, list)) and bool(values) and all(_text(value) for value in values) and len(values) == len(set(values))


def validate_requirements(requirements: Iterable[Requirement], capabilities: Iterable) -> None:
    """Reject incomplete promises, invented hosts and changes in inventory coverage."""
    requirements = tuple(requirements)
    capabilities = tuple(capabilities)
    inventory = {capability.id: capability for capability in capabilities}
    if len(inventory) != len(capabilities):
        raise ValueError("duplicate inventory id")
    seen = set()
    covered = set()
    excluded = set()
    for requirement in requirements:
        label = requirement.id
        if not _text(label):
            raise ValueError("missing requirement id")
        if label in seen:
            raise ValueError(f"duplicate requirement id: {label}")
        seen.add(label)
        if requirement.inventory_id not in inventory:
            raise ValueError(f"{label}: missing or unknown inventory id")
        capability = inventory[requirement.inventory_id]
        for field in ("task", "expected", "product_behavior"):
            if not _text(getattr(requirement, field)):
                raise ValueError(f"{label}: missing {field}")
        if not _ids(requirement.constraints):
            raise ValueError(f"{label}: missing constraints")
        if requirement.difference not in DIFFERENCES:
            raise ValueError(f"{label}: invalid difference")
        if type(requirement.required) is not bool:
            raise ValueError(f"{label}: required must be boolean")
        declared_hosts = tuple(getattr(capability, "hosts", HOSTS)) if capability.supported else ()
        if not isinstance(requirement.hosts, (list, tuple)) or len(set(requirement.hosts)) != len(requirement.hosts) or any(host not in HOSTS for host in requirement.hosts):
            raise ValueError(f"{label}: invalid host")
        if set(requirement.hosts) != set(declared_hosts):
            raise ValueError(f"{label}: hosts differ from inventory applicability")
        if not isinstance(requirement.browser_tests, Mapping):
            raise ValueError(f"{label}: invalid browser verification mapping")
        if capability.supported:
            if not requirement.hosts:
                raise ValueError(f"{label}: supported workflow has no host")
            if not _ids(requirement.shared_tests) and not requirement.browser_tests:
                raise ValueError(f"{label}: missing verification mapping")
            if set(requirement.browser_tests) != set(requirement.hosts) or any(not _ids(ids) for ids in requirement.browser_tests.values()):
                raise ValueError(f"{label}: missing browser verification for applicable host")
            if not _ids(requirement.shared_tests):
                raise ValueError(f"{label}: missing shared verification mapping")
            covered.add(requirement.inventory_id)
        else:
            if requirement.required or requirement.difference != "unsupported" or requirement.shared_tests or requirement.browser_tests:
                raise ValueError(f"{label}: excluded inventory entry cannot be a release requirement")
            if requirement.inventory_id in excluded:
                raise ValueError(f"{label}: excluded inventory entry must map only once")
            excluded.add(requirement.inventory_id)
        reference = requirement.ag_grid
        if not isinstance(reference, Mapping) or not reference:
            raise ValueError(f"{label}: missing reference")
        host_only = capability.supported and len(declared_hosts) == 1
        if host_only or not capability.supported:
            if reference.get("result") != "not_applicable" or not _text(reference.get("reason")) or reference.get("test_id"):
                raise ValueError(f"{label}: reference must be explicitly not_applicable with reason and no invented test")
            if host_only and requirement.difference != "not_comparable":
                raise ValueError(f"{label}: host-only reference must be not_comparable")
        else:
            for field in ("version", "edition", "row_model", "behavior", "test_id"):
                if not _text(reference.get(field)):
                    raise ValueError(f"{label}: missing reference {field}")
            if (reference["version"], reference["edition"], reference["row_model"]) != ("36.2.0", "Enterprise", "SSRM"):
                raise ValueError(f"{label}: reference baseline differs from pinned SSRM configuration")
            if not isinstance(reference.get("options"), Mapping) or not reference["options"] or not _ids(reference.get("docs")):
                raise ValueError(f"{label}: missing reference configuration or documentation")
            if reference.get("result") not in RESULTS | {"unverified"} or reference.get("result") == "not_applicable":
                raise ValueError(f"{label}: invalid reference result")
            if requirement.difference == "equivalent" and reference.get("result") != "passed":
                raise ValueError(f"{label}: equivalent requires observed reference evidence")
    expected = {capability.id for capability in capabilities if capability.supported}
    expected_excluded = set(inventory) - expected
    if covered != expected or excluded != expected_excluded:
        raise ValueError(f"inventory coverage mismatch: missing {sorted((expected - covered) | (expected_excluded - excluded))}")


def _escape(value) -> str:
    return str(value).replace("|", "\\|").replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")


def render_matrix(requirements: Iterable[Requirement], results: Iterable[CaseResult] | None = None) -> str:
    """Show only contract-bound outcomes; absence of any mapped case is unverified.

    Private paths and observed text are deliberately omitted from this public
    summary. Artifact binding and release gating belong to the run manifest.
    """
    requirements = tuple(requirements)
    inventory = {requirement.id: requirement for requirement in requirements}
    if len(inventory) != len(requirements):
        raise ValueError("duplicate requirement id")
    outcomes = {}
    for result in results or ():
        if result.requirement_id not in inventory or result.system not in {"product", "ag_grid"} or result.result not in RESULTS:
            raise ValueError("result cannot bind to a declared requirement/system/result")
        requirement = inventory[result.requirement_id]
        if result.system == "product":
            if result.host not in HOSTS:
                raise ValueError("product result needs a known host")
            if result.host not in requirement.hosts:
                if result.result == "not_applicable":
                    continue
                raise ValueError("result host is excluded by the requirement")
            allowed = (*requirement.shared_tests, *requirement.browser_tests[result.host])
            if result.result == "not_applicable":
                raise ValueError("applicable host cannot report not_applicable")
        else:
            if result.host is not None:
                raise ValueError("AG Grid result cannot declare a product host")
            if requirement.ag_grid.get("result") == "not_applicable":
                if result.result == "not_applicable":
                    continue
                raise ValueError("excluded reference cannot report observed parity")
            if result.result == "not_applicable":
                raise ValueError("applicable reference cannot report not_applicable")
            allowed = (requirement.ag_grid["test_id"],)
        if result.test_id not in allowed:
            raise ValueError("result test_id is not mapped by the contract")
        key = (result.requirement_id, result.system, result.host, result.test_id)
        if key in outcomes:
            raise ValueError("duplicate result; select a run attempt before rendering")
        outcomes[key] = result.result

    def status(requirement, system, host=None):
        if system == "product":
            if host not in requirement.hosts:
                return "not_applicable"
            tests = (*requirement.shared_tests, *requirement.browser_tests[host])
        else:
            if requirement.ag_grid.get("result") == "not_applicable":
                return "not_applicable"
            tests = (requirement.ag_grid["test_id"],)
        values = [outcomes.get((requirement.id, system, host, test), "unverified") for test in tests]
        for state in ("failed", "blocked", "skipped", "unverified"):
            if state in values:
                return state
        return "passed" if values and all(value == "passed" for value in values) else "unverified"

    rows = [
        "# Flagship functionality certification", "",
        "Contract acceptance criteria are not certification evidence. Missing observations remain unverified.", "",
        "| Requirement | Inventory | Customer task | Expected | Difference | Release | AG Grid | Dash | React | Streamlit | Constraints |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for requirement in requirements:
        release = "mandatory" if requirement.required else "optional" if requirement.hosts else "intentionally unavailable"
        cells = [requirement.id, requirement.inventory_id, requirement.task, requirement.expected, requirement.difference, release,
                 status(requirement, "ag_grid"), *(status(requirement, "product", host) for host in HOSTS), "; ".join(requirement.constraints)]
        rows.append("| " + " | ".join(_escape(cell) for cell in cells) + " |")
    return "\n".join(rows) + "\n"
