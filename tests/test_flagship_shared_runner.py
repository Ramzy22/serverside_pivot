"""Fail-closed contracts for the installed-release shared assertion runner."""

from pathlib import Path
import json

from scripts.flagship_manifest import QualificationBinding

import pytest

from scripts.flagship_shared_runner import SHARED_ASSERTIONS, _committed_source, run_shared_qualification, unmapped_shared_ids
from tests.flagship_contract import load_requirements


def test_shared_mapping_is_explicit_and_never_claims_all_requirements():
    requirements = load_requirements(Path(__file__).with_name("flagship_requirements.json"))
    required = {test_id for requirement in requirements if requirement.required for test_id in requirement.shared_tests}
    assert set(SHARED_ASSERTIONS) <= required
    assert set(unmapped_shared_ids(requirements)) == required - set(SHARED_ASSERTIONS)
    assert "shared.copy-selection.headers" in unmapped_shared_ids(requirements)
    assert "shared.export-csv.exact-document" not in unmapped_shared_ids(requirements)
    assert "shared.export-xlsx.exact-workbook" not in unmapped_shared_ids(requirements)


def test_mapped_cases_execute_parsed_exact_document_assertions():
    for case_id, format_name in (
        ("shared.export-csv.exact-document", "csv"),
        ("shared.export-xlsx.exact-workbook", "xlsx"),
    ):
        assertions = SHARED_ASSERTIONS[case_id]
        assert len(assertions) == 1
        assert assertions[0].node.endswith(f"[{format_name}]")
        assert "tests/export_qualification_fixture.py" in assertions[0].sources


def test_run_rejects_evidence_outside_current_run(tmp_path):
    class Binding:
        run_id = "current"

    with pytest.raises(ValueError, match="runs/<current run_id>"):
        run_shared_qualification(Binding(), tmp_path / "python", tmp_path / "runs" / "old", requirements=())


def test_committed_source_rejects_path_traversal():
    with pytest.raises(ValueError, match="under tests"):
        _committed_source(object(), "tests/../secrets.txt")


def test_run_rejects_modified_wheel_before_any_assertion(tmp_path):
    python = tmp_path / "python.exe"
    python.write_bytes(b"candidate")
    wheel = tmp_path / "release.whl"
    wheel.write_bytes(b"modified")
    record = tmp_path / "environment.json"
    record.write_text(json.dumps({"sourceCommit": "abc", "wheelSha256": "original"}), encoding="utf-8")
    binding = QualificationBinding(
        run_id="current", source_commit="abc", sha256={"wheel": "original"},
        paths={"python": python, "wheel": wheel, "environment_record": record},
    )
    with pytest.raises(ValueError, match="wheel hash changed"):
        run_shared_qualification(binding, python, tmp_path / "runs" / "current", requirements=())
