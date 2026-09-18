"""Validate release evidence and exact OWASP ASVS mappings without success claims."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any
from urllib.parse import urlparse, urlunparse


_MATRIX_FIELDS = frozenset({"standard", "controls", "operationalControls"})
_STANDARD_FIELDS = frozenset({"name", "version", "catalog"})
_CONTROL_FIELDS = frozenset(
    {"id", "asvs", "title", "state", "owner", "cadence", "localEvidence", "operatorEvidence"}
)
_OPERATIONAL_CONTROL_FIELDS = frozenset({"id", "title", "owner", "cadence", "operatorEvidence"})
_ASVS_REFERENCE_FIELDS = frozenset({"id", "title"})
_CATALOG_FIELDS = frozenset({"standard", "requirements"})
_CATALOG_STANDARD_FIELDS = frozenset({"name", "version", "source", "sourceSha256"})
_CATALOG_REQUIREMENT_FIELDS = frozenset({"id", "level", "title"})
_ATTACHMENT_FIELDS = frozenset({"id", "path", "owner", "expiresAt", "sha256"})
_RELEASE_FIELDS = frozenset({"tag", "commit", "environment", "deploymentOrigin"})
_SCOPE_FIELDS = frozenset({"matrixSha256", "controlIds"})
_EVIDENCE_FIELDS = frozenset({"release", "scope", "generatedAt", "attachments"})
_STATES = frozenset({"verified_locally", "operator_evidence_required", "not_applicable"})
_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
_COMMIT_SHA = re.compile(r"^[0-9a-fA-F]{40,64}$")


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {label}: {path}") from exc


def _require_strings(record: dict[str, Any], fields: tuple[str, ...], label: str) -> None:
    if any(not isinstance(record.get(field), str) or not record[field].strip() for field in fields):
        raise ValueError(f"{label} requires non-empty " + ", ".join(fields) + ".")


def _contained_path(root: Path, value: str, label: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        raise ValueError(f"{label} must be relative.")
    resolved_root = root.resolve()
    resolved = (resolved_root / candidate).resolve()
    if not resolved.is_relative_to(resolved_root):
        raise ValueError(f"{label} escapes its allowed directory.")
    return resolved


def _catalog_requirements(payload: Any, standard: dict[str, Any]) -> dict[str, str]:
    if not isinstance(payload, dict) or set(payload) != _CATALOG_FIELDS:
        raise ValueError('ASVS catalog must contain exactly "standard" and "requirements".')
    catalog_standard = payload["standard"]
    if not isinstance(catalog_standard, dict) or set(catalog_standard) != _CATALOG_STANDARD_FIELDS:
        raise ValueError("ASVS catalog standard metadata is invalid.")
    _require_strings(catalog_standard, ("name", "version", "source", "sourceSha256"), "ASVS catalog")
    if not _SHA256.fullmatch(catalog_standard["sourceSha256"]):
        raise ValueError("ASVS catalog sourceSha256 is invalid.")
    if (catalog_standard["name"], catalog_standard["version"]) != (
        standard["name"],
        standard["version"],
    ):
        raise ValueError("Matrix standard does not match the pinned ASVS catalog.")
    requirements = payload["requirements"]
    if not isinstance(requirements, list) or not requirements:
        raise ValueError("ASVS catalog requirements must be a non-empty array.")
    resolved: dict[str, str] = {}
    for requirement in requirements:
        if not isinstance(requirement, dict) or set(requirement) != _CATALOG_REQUIREMENT_FIELDS:
            raise ValueError("Each ASVS catalog requirement has invalid fields.")
        _require_strings(requirement, ("id", "level", "title"), "ASVS catalog requirement")
        if not requirement["id"].startswith("v5.0.0-V"):
            raise ValueError("ASVS catalog requirement identifiers must pin v5.0.0.")
        if requirement["id"] in resolved:
            raise ValueError("ASVS catalog contains a duplicate requirement id.")
        resolved[requirement["id"]] = requirement["title"]
    return resolved


def _validate_control_evidence(control: dict[str, Any], repository_root: Path) -> None:
    for field in ("localEvidence", "operatorEvidence"):
        if not isinstance(control[field], list) or any(
            not isinstance(value, str) or not value.strip() for value in control[field]
        ):
            raise ValueError(f"Matrix control {control['id']} {field} must be a list of non-empty ids or paths.")
    if control["state"] == "verified_locally":
        if not control["localEvidence"]:
            raise ValueError(f"Locally verified control {control['id']} lacks local evidence.")
        for evidence_path in control["localEvidence"]:
            resolved = _contained_path(repository_root, evidence_path, "Local evidence path")
            if not resolved.is_file():
                raise ValueError(f"Local evidence path does not exist: {evidence_path}")
    if control["state"] == "operator_evidence_required" and not control["operatorEvidence"]:
        raise ValueError(f"Operator-evidence control {control['id']} lacks required attachment ids.")


def _matrix_controls(payload: Any, repository_root: Path) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or set(payload) != _MATRIX_FIELDS:
        raise ValueError('Matrix must contain exactly "standard", "controls", and "operationalControls".')
    standard = payload["standard"]
    if not isinstance(standard, dict) or set(standard) != _STANDARD_FIELDS:
        raise ValueError("Matrix standard metadata is invalid.")
    _require_strings(standard, ("name", "version", "catalog"), "Matrix standard")
    if (standard["name"], standard["version"]) != ("OWASP ASVS", "5.0.0"):
        raise ValueError("Matrix must pin OWASP ASVS 5.0.0.")
    catalog_path = _contained_path(repository_root, standard["catalog"], "ASVS catalog path")
    catalog = _catalog_requirements(_load_json(catalog_path, "ASVS catalog"), standard)

    controls = payload["controls"]
    operational_controls = payload["operationalControls"]
    if not isinstance(controls, list) or not controls:
        raise ValueError("Matrix controls must be a non-empty array.")
    if not isinstance(operational_controls, list):
        raise ValueError("Matrix operationalControls must be an array.")

    known_ids: set[str] = set()
    resolved: list[dict[str, Any]] = []
    for control in controls:
        if not isinstance(control, dict) or set(control) != _CONTROL_FIELDS:
            raise ValueError("Each matrix control must have the documented fields.")
        _require_strings(control, ("id", "title", "state", "owner", "cadence"), "Matrix control")
        if control["id"] in known_ids:
            raise ValueError(f"Duplicate matrix control id: {control['id']}")
        known_ids.add(control["id"])
        if control["state"] not in _STATES:
            raise ValueError(f"Unknown matrix state: {control['state']}")
        references = control["asvs"]
        if not isinstance(references, list) or not references:
            raise ValueError(f"Matrix control {control['id']} must map to at least one ASVS requirement.")
        for reference in references:
            if not isinstance(reference, dict) or set(reference) != _ASVS_REFERENCE_FIELDS:
                raise ValueError(f"Matrix control {control['id']} has an invalid ASVS reference.")
            _require_strings(reference, ("id", "title"), "ASVS reference")
            if catalog.get(reference["id"]) != reference["title"]:
                raise ValueError(f"Matrix control {control['id']} ASVS reference is not in the pinned catalog.")
        _validate_control_evidence(control, repository_root)
        resolved.append(control)

    for control in operational_controls:
        if not isinstance(control, dict) or set(control) != _OPERATIONAL_CONTROL_FIELDS:
            raise ValueError("Each operational control must have the documented fields.")
        _require_strings(control, ("id", "title", "owner", "cadence"), "Operational control")
        if control["id"] in known_ids:
            raise ValueError(f"Duplicate matrix control id: {control['id']}")
        known_ids.add(control["id"])
        evidence = control["operatorEvidence"]
        if not isinstance(evidence, list) or not evidence or any(
            not isinstance(value, str) or not value.strip() for value in evidence
        ):
            raise ValueError(f"Operational control {control['id']} lacks valid attachment ids.")
        resolved.append(control)
    return resolved


def _deployment_origin(value: str, label: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{label} must be a credential-free https origin.")
    return urlunparse(("https", parsed.netloc.lower(), "", "", "", ""))


def _validate_release_binding(release: Any, expected_release: dict[str, str]) -> None:
    if not isinstance(release, dict) or set(release) != _RELEASE_FIELDS:
        raise ValueError("Release evidence release binding has invalid fields.")
    _require_strings(
        release,
        ("tag", "commit", "environment", "deploymentOrigin"),
        "Release evidence release binding",
    )
    if not _COMMIT_SHA.fullmatch(release["commit"]):
        raise ValueError("Release evidence commit must be an immutable Git SHA.")
    supplied_origin = _deployment_origin(release["deploymentOrigin"], "Release evidence deploymentOrigin")
    expected_origin = _deployment_origin(expected_release["deploymentOrigin"], "Expected deployment origin")
    if supplied_origin != expected_origin:
        raise ValueError("Release evidence deployment origin does not match the protected release target.")
    for key in ("tag", "commit", "environment"):
        if release[key] != expected_release[key]:
            raise ValueError(f"Release evidence {key} does not match the protected release target.")


def _validate_scope(scope: Any, *, matrix_sha256: str, control_ids: set[str]) -> None:
    if not isinstance(scope, dict) or set(scope) != _SCOPE_FIELDS:
        raise ValueError("Release evidence scope has invalid fields.")
    matrix_digest = scope.get("matrixSha256")
    if not isinstance(matrix_digest, str) or not _SHA256.fullmatch(matrix_digest):
        raise ValueError("Release evidence scope matrixSha256 is invalid.")
    if matrix_digest.lower() != matrix_sha256.lower():
        raise ValueError("Release evidence scope does not match the current control matrix.")
    scoped_controls = scope.get("controlIds")
    if (
        not isinstance(scoped_controls, list)
        or any(not isinstance(control_id, str) or not control_id.strip() for control_id in scoped_controls)
        or len(set(scoped_controls)) != len(scoped_controls)
        or set(scoped_controls) != control_ids
    ):
        raise ValueError("Release evidence scope must list every and only current matrix control id.")


def _attachments_from_evidence(
    payload: Any,
    evidence_directory: Path,
    as_of: date,
    *,
    expected_release: dict[str, str],
    matrix_sha256: str,
    control_ids: set[str],
) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict) or set(payload) != _EVIDENCE_FIELDS:
        raise ValueError("Release evidence must contain release, scope, generatedAt, and attachments only.")
    _require_strings(payload, ("generatedAt",), "Release evidence")
    try:
        datetime.fromisoformat(payload["generatedAt"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Release evidence generatedAt must be ISO-8601.") from exc
    _validate_release_binding(payload["release"], expected_release)
    _validate_scope(
        payload["scope"],
        matrix_sha256=matrix_sha256,
        control_ids=control_ids,
    )
    if not isinstance(payload["attachments"], list):
        raise ValueError("Release evidence attachments must be an array.")
    attachments: dict[str, dict[str, Any]] = {}
    for attachment in payload["attachments"]:
        if not isinstance(attachment, dict) or set(attachment) != _ATTACHMENT_FIELDS:
            raise ValueError("Each evidence attachment must have the documented fields.")
        _require_strings(attachment, ("id", "path", "owner", "expiresAt", "sha256"), "Evidence attachment")
        if attachment["id"] in attachments:
            raise ValueError(f"Duplicate evidence attachment id: {attachment['id']}")
        if not _SHA256.fullmatch(attachment["sha256"]):
            raise ValueError(f"Evidence attachment {attachment['id']} has an invalid SHA-256.")
        try:
            expires_at = date.fromisoformat(attachment["expiresAt"])
        except ValueError as exc:
            raise ValueError(f"Evidence attachment {attachment['id']} expiresAt must be an ISO date.") from exc
        if expires_at <= as_of:
            raise ValueError(f"Evidence attachment {attachment['id']} has expired.")
        path = _contained_path(evidence_directory, attachment["path"], "Evidence attachment path")
        if not path.is_file():
            raise ValueError(f"Evidence attachment path does not exist: {attachment['path']}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest.lower() != attachment["sha256"].lower():
            raise ValueError(f"Evidence attachment {attachment['id']} SHA-256 does not match.")
        attachments[attachment["id"]] = attachment
    return attachments


def verify(
    *,
    matrix_path: Path,
    evidence_path: Path,
    repository_root: Path,
    as_of: date,
    expected_release: dict[str, str],
) -> None:
    try:
        matrix_bytes = matrix_path.read_bytes()
    except OSError as exc:
        raise ValueError(f"Cannot read certification matrix: {matrix_path}") from exc
    controls = _matrix_controls(_load_json(matrix_path, "certification matrix"), repository_root)
    attachments = _attachments_from_evidence(
        _load_json(evidence_path, "release evidence"),
        evidence_path.parent,
        as_of,
        expected_release=expected_release,
        matrix_sha256=hashlib.sha256(matrix_bytes).hexdigest(),
        control_ids={str(control["id"]) for control in controls},
    )
    required_ids = {
        attachment_id
        for control in controls
        if control.get("state") == "operator_evidence_required" or "state" not in control
        for attachment_id in control["operatorEvidence"]
    }
    missing = required_ids - set(attachments)
    if missing:
        raise ValueError("Required operator evidence is missing: " + ", ".join(sorted(missing)))
    unexpected = set(attachments) - required_ids
    if unexpected:
        raise ValueError("Evidence attachment is not referenced by an operator control: " + ", ".join(sorted(unexpected)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, required=True, help="OWASP ASVS matrix JSON")
    parser.add_argument("--evidence", type=Path, required=True, help="release evidence manifest JSON")
    parser.add_argument("--repository-root", type=Path, default=Path.cwd(), help="repository root for local evidence paths")
    parser.add_argument("--expected-tag", required=True, help="immutable release tag expected by the protected job")
    parser.add_argument("--expected-commit", required=True, help="immutable source commit expected by the protected job")
    parser.add_argument("--expected-environment", required=True, help="protected deployment environment")
    parser.add_argument("--expected-deployment-origin", required=True, help="protected HTTPS deployment origin")
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today(), help="ISO date used for evidence freshness")
    args = parser.parse_args(argv)
    try:
        verify(
            matrix_path=args.matrix,
            evidence_path=args.evidence,
            repository_root=args.repository_root,
            as_of=args.as_of,
            expected_release={
                "tag": args.expected_tag,
                "commit": args.expected_commit,
                "environment": args.expected_environment,
                "deploymentOrigin": args.expected_deployment_origin,
            },
        )
    except ValueError as exc:
        print(f"security-release-evidence: {exc}", file=sys.stderr)
        return 2
    print("security-release-evidence: all required evidence is current and complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
