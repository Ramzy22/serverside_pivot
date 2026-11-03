"""Fail-closed contracts for the installed-release shared assertion runner."""

from pathlib import Path
import json

from scripts.flagship_manifest import QualificationBinding
from scripts.flagship_shared_client import SHARED_CLIENT_ASSERTIONS

import pytest

from scripts.flagship_shared_runner import SHARED_ASSERTIONS, _committed_source, _pytest_all_passed, run_shared_qualification, unmapped_shared_ids
from tests.flagship_contract import load_requirements


def test_shared_mapping_is_explicit_and_declared_coverage_does_not_claim_execution():
    requirements = load_requirements(Path(__file__).with_name("flagship_requirements.json"))
    required = {test_id for requirement in requirements if requirement.required for test_id in requirement.shared_tests}
    assert set(SHARED_ASSERTIONS) <= required
    assert set(unmapped_shared_ids(requirements)) == required - (SHARED_ASSERTIONS.keys() | SHARED_CLIENT_ASSERTIONS.keys())
    assert "shared.copy-selection.headers" in SHARED_CLIENT_ASSERTIONS
    assert "shared.copy-selection.headers" not in SHARED_ASSERTIONS
    assert "shared.export-csv.exact-document" not in unmapped_shared_ids(requirements)
    assert "shared.export-xlsx.exact-workbook" not in unmapped_shared_ids(requirements)


@pytest.mark.parametrize("exit_code,output,expected_count,accepted", [
    (0, "... [100%]\n3 passed in 1.20s\n", 3, True),
    (0, ". [100%]\n1 passed, 2 skipped in 1.20s\n", 3, False),
    (0, ".. [100%]\n2 passed in 1.20s\n", 3, False),
    (0, "... [100%]\n3 passed, 12 warnings in 1.20s\n", 3, True),
    (1, "..F [100%]\n2 passed, 1 failed in 1.20s\n", 3, False),
    (0, "... [100%]\n3 passed, 1 deselected in 1.20s\n", 3, False),
])
def test_shared_gate_requires_every_mapped_assertion_to_pass(exit_code, output, expected_count, accepted):
    assert _pytest_all_passed(exit_code, output, expected_count) is accepted


def test_mapped_cases_execute_parsed_exact_document_assertions():
    for case_id, format_name in (
        ("shared.export-csv.exact-document", "csv"),
        ("shared.export-xlsx.exact-workbook", "xlsx"),
    ):
        assertions = SHARED_ASSERTIONS[case_id]
        assert len(assertions) == 1
        assert assertions[0].node.endswith(f"[{format_name}]")
        assert "tests/export_qualification_fixture.py" in assertions[0].sources


def test_table_scope_mappings_execute_real_runtime_service_exports():
    current = SHARED_ASSERTIONS["shared.copy-current-table.visible-scope"]
    all_rows = SHARED_ASSERTIONS["shared.copy-all-records.complete-scope"]
    assert len(current) == 1 and "collapsed_hierarchy_state" in current[0].node
    assert len(all_rows) == 3
    assert {assertion.node.rsplit("[", 1)[-1] for assertion in all_rows} == {
        "hierarchy]", "outline]", "tabular]",
    }
    assert all("tests/test_runtime_service.py" in assertion.sources for assertion in (*current, *all_rows))


def test_custom_pivot_order_mapping_executes_sparse_values_and_pin_assertions():
    assertions = SHARED_ASSERTIONS["shared.custom-pivot-order.sparse-window"]
    assert len(assertions) == 1
    assert assertions[0].node == (
        "tests/test_flagship_shared_runtime.py::test_flagship_custom_order_sparse_window_and_off_window_pin"
    )
    assert "tests/export_qualification_fixture.py" in assertions[0].sources


def test_scroll_mappings_execute_wide_fixture_http_round_trips():
    for axis in ("vertical", "horizontal"):
        assertions = SHARED_ASSERTIONS[f"shared.{axis}-virtual-scroll.window"]
        assert {assertion.node for assertion in assertions} == {
            f"tests/test_flagship_shared_scroll.py::test_{axis}_window_round_trip",
            "tests/test_flagship_shared_scroll.py::test_cached_row_block_preserves_layout_revision",
        }
        assert all("tests/data/flagship_scroll_rows.json" in assertion.sources for assertion in assertions)


def test_aggregation_mapping_covers_secondary_measure_revision_changes():
    assertions = SHARED_ASSERTIONS["shared.layout-values.aggregation"]
    assert {assertion.node.split("::")[-1] for assertion in assertions} == {
        "test_flagship_sum_average_sum_round_trip_preserves_measure_position",
        "test_secondary_aggregation_changes_revision_without_moving_saved_identity",
    }


def test_recovery_mappings_require_real_busy_admission_and_generation_checks():
    for test_id in ("shared.runtime-error.visible", "shared.runtime-error.recovery"):
        assert any(assertion.node.endswith("::test_flagship_busy_rejection_preserves_generation_and_recovers_exact_data")
                   for assertion in SHARED_ASSERTIONS[test_id])


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
