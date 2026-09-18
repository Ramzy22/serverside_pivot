"""Fail a release when pip-audit findings lack a current reviewed exception."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys
from typing import Any


_EXCEPTION_FIELDS = frozenset(
    {"id", "package", "version", "owner", "ticket", "rationale", "expires"}
)


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {label} JSON: {path}") from exc


def _findings_from_audit(payload: Any) -> set[tuple[str, str, str]]:
    dependencies = payload
    if isinstance(payload, dict):
        dependencies = payload.get("dependencies", payload.get("packages"))
    if not isinstance(dependencies, list):
        raise ValueError("pip-audit report must be a dependency list.")

    findings: set[tuple[str, str, str]] = set()
    for dependency in dependencies:
        if not isinstance(dependency, dict):
            raise ValueError("pip-audit dependency entries must be objects.")
        if dependency.get("skip_reason"):
            package = dependency.get("name") or "unknown-package"
            raise ValueError(f"pip-audit skipped dependency: {package}")
        package = dependency.get("name")
        version = dependency.get("version")
        if not isinstance(package, str) or not package.strip() or not isinstance(version, str) or not version.strip():
            raise ValueError("pip-audit dependency entries must include name and version.")
        vulnerabilities = dependency.get("vulns", dependency.get("vulnerabilities", []))
        if not isinstance(vulnerabilities, list):
            raise ValueError("pip-audit vulnerability lists must be arrays.")
        for vulnerability in vulnerabilities:
            vulnerability_id = vulnerability.get("id") if isinstance(vulnerability, dict) else None
            if not all(isinstance(value, str) and value.strip() for value in (package, version, vulnerability_id)):
                raise ValueError("pip-audit findings must include package, version, and vulnerability id.")
            findings.add((package.strip(), version.strip(), vulnerability_id.strip()))
    return findings


def _exceptions_from_policy(payload: Any, as_of: date) -> set[tuple[str, str, str]]:
    if not isinstance(payload, dict) or set(payload) != {"exceptions"}:
        raise ValueError('Exception policy must be exactly {"exceptions": [...]}.')
    records = payload["exceptions"]
    if not isinstance(records, list):
        raise ValueError("Exception policy exceptions must be an array.")

    exceptions: set[tuple[str, str, str]] = set()
    for record in records:
        if not isinstance(record, dict) or set(record) != _EXCEPTION_FIELDS:
            raise ValueError(
                "Each dependency exception must contain id, package, version, owner, ticket, rationale, and expires."
            )
        if any(not isinstance(record[field], str) or not record[field].strip() for field in _EXCEPTION_FIELDS):
            raise ValueError("Dependency exception fields must be non-empty strings.")
        try:
            expires = date.fromisoformat(record["expires"])
        except ValueError as exc:
            raise ValueError("Dependency exception expires must be an ISO date.") from exc
        if expires <= as_of:
            raise ValueError(
                f"Dependency exception {record['id']} for {record['package']}@{record['version']} has expired."
            )
        key = (record["package"].strip(), record["version"].strip(), record["id"].strip())
        if key in exceptions:
            raise ValueError("Duplicate dependency exception key: " + "@".join(key))
        exceptions.add(key)
    return exceptions


def verify(*, audit_path: Path, exceptions_path: Path, as_of: date) -> tuple[set[tuple[str, str, str]], set[tuple[str, str, str]]]:
    findings = _findings_from_audit(_load_json(audit_path, "pip-audit report"))
    exceptions = _exceptions_from_policy(_load_json(exceptions_path, "exception policy"), as_of)
    return findings - exceptions, exceptions - findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", type=Path, required=True, help="pip-audit JSON report")
    parser.add_argument("--exceptions", type=Path, required=True, help="reviewed exception policy JSON")
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today(), help="ISO date used for exception expiry")
    args = parser.parse_args(argv)

    try:
        unmatched, unused = verify(
            audit_path=args.audit,
            exceptions_path=args.exceptions,
            as_of=args.as_of,
        )
    except ValueError as exc:
        print(f"dependency-audit gate: {exc}", file=sys.stderr)
        return 2

    if unmatched:
        for package, version, vulnerability_id in sorted(unmatched):
            print(
                f"dependency-audit gate: unreviewed {vulnerability_id} in {package}@{version}",
                file=sys.stderr,
            )
        return 1
    if unused:
        for package, version, vulnerability_id in sorted(unused):
            print(
                f"dependency-audit gate: stale exception {vulnerability_id} for {package}@{version}",
                file=sys.stderr,
            )
        return 1

    print("dependency-audit gate: no unreviewed dependency findings.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
