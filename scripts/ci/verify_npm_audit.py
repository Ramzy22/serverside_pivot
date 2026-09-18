"""Fail closed on malformed or vulnerable production npm audit reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


def _load(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read npm audit JSON: {path}") from exc


def findings(payload: Any) -> list[tuple[str, str]]:
    if not isinstance(payload, dict) or payload.get("auditReportVersion") != 2:
        raise ValueError("npm audit report must use auditReportVersion 2.")
    vulnerabilities = payload.get("vulnerabilities")
    if not isinstance(vulnerabilities, dict):
        raise ValueError("npm audit report must include a vulnerabilities object.")
    found: list[tuple[str, str]] = []
    for package, record in vulnerabilities.items():
        if not isinstance(package, str) or not package.strip() or not isinstance(record, dict):
            raise ValueError("npm audit vulnerability entries are invalid.")
        name = record.get("name")
        severity = record.get("severity")
        via = record.get("via")
        if name != package or not isinstance(severity, str) or not isinstance(via, list) or not via:
            raise ValueError(f"npm audit vulnerability entry is incomplete: {package}")
        found.append((package, severity))
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", type=Path, required=True, help="npm audit --json report")
    args = parser.parse_args(argv)
    try:
        unresolved = findings(_load(args.audit))
    except ValueError as exc:
        print(f"npm-audit gate: {exc}", file=sys.stderr)
        return 2
    if unresolved:
        for package, severity in sorted(unresolved):
            print(f"npm-audit gate: unreviewed {severity} finding in {package}", file=sys.stderr)
        return 1
    print("npm-audit gate: no production dependency findings.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
