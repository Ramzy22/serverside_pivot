"""Behavioral checks for the parity contract's fail-closed validation."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
import importlib
import importlib.util
import json
from pathlib import Path
import re

import pytest

from tests.flagship_capabilities import CAPABILITIES
from tests.export_qualification_fixture import FLAGSHIP_QUALIFICATION_SOURCE_ROWS


CONTRACT_PATH = Path(__file__).with_name("flagship_requirements.json")
SUPPORTED_IDS = {
    "initial-workspace", "runtime-error", "hierarchy-expand", "hierarchy-expand-all",
    "vertical-virtual-scroll", "horizontal-virtual-scroll", "column-order", "saved-scroll",
    "layout-field-zones", "layout-values", "layout-swap", "global-filter",
    "header-filter", "sorting", "filter-options", "totals", "row-numbers",
    "layout-modes", "density", "column-sizing", "column-pinning", "cell-selection",
    "copy-selection", "copy-current-table", "copy-all-records", "keyboard-copy",
    "export-csv", "export-xlsx", "large-delivery", "number-format", "cell-format",
    "color-scales", "themes", "zoom-immersive", "formula-columns", "custom-dimensions",
    "report-mode", "persistence", "undo-redo", "range-chart", "chart-pane", "chart-export",
    "streamlit-no-rerun", "react-direct-transport", "dash-callback-transport",
}


@pytest.fixture
def contract():
    # A missing implementation must be an explicit assertion failure during RED.
    assert importlib.util.find_spec("tests.flagship_contract") is not None, "contract module is missing"
    return importlib.import_module("tests.flagship_contract")


@pytest.fixture
def requirements(contract):
    return contract.load_requirements(CONTRACT_PATH)


def test_contract_covers_all_supported_workflows_without_promoting_editing():
    assert importlib.util.find_spec("tests.flagship_contract") is not None, "contract module is missing"
    contract = importlib.import_module("tests.flagship_contract")
    requirements = contract.load_requirements(CONTRACT_PATH)
    contract.validate_requirements(requirements, CAPABILITIES)
    assert {r.inventory_id for r in requirements if r.required} == SUPPORTED_IDS
    editing = [r for r in requirements if r.inventory_id == "editing"]
    assert len(editing) == 1
    assert editing[0].required is False
    assert editing[0].hosts == ()
    assert editing[0].difference == "unsupported"


@pytest.mark.parametrize("inventory_id,host", [
    ("streamlit-no-rerun", "streamlit"),
    ("react-direct-transport", "react"),
    ("dash-callback-transport", "dash"),
])
def test_host_only_requirements_have_no_fabricated_reference(contract, requirements, inventory_id, host):
    requirement = next(r for r in requirements if r.inventory_id == inventory_id)
    assert requirement.hosts == (host,)
    assert set(requirement.browser_tests) == {host}
    assert requirement.difference == "not_comparable"
    assert requirement.ag_grid["result"] == "not_applicable"
    assert requirement.ag_grid["reason"].strip()
    assert not requirement.ag_grid.get("test_id")


def test_comparable_requirements_pin_a_reference_without_claiming_observed_parity(requirements):
    for requirement in requirements:
        if requirement.ag_grid["result"] == "not_applicable":
            continue
        assert requirement.ag_grid["version"] == "36.2.0"
        assert requirement.ag_grid["edition"] == "Enterprise"
        assert requirement.ag_grid["row_model"] == "SSRM"
        assert requirement.ag_grid["options"]
        assert requirement.ag_grid["docs"]
        assert requirement.ag_grid["behavior"]
        assert requirement.ag_grid["test_id"]
        assert requirement.ag_grid["result"] == "unverified"
        assert requirement.difference != "equivalent"


def test_selected_copy_has_distinct_edge_contracts(requirements):
    edges = {r.id for r in requirements if r.inventory_id == "copy-selection"}
    assert {
        "copy-selection.logical-cells", "copy-selection.display-order",
        "copy-selection.pinned-columns", "copy-selection.hidden-columns",
        "copy-selection.formatted-values", "copy-selection.tabs",
        "copy-selection.newlines", "copy-selection.unicode", "copy-selection.empty-values",
        "copy-selection.headers", "copy-selection.unloaded-rows",
    } <= edges
    keyboard = next(r for r in requirements if r.id == "keyboard-copy.native-input")
    assert "dialog" in keyboard.expected.lower()


def test_special_text_copy_declares_comparator_difference(requirements):
    by_id = {requirement.id: requirement for requirement in requirements}
    for edge in ("copy-selection.tabs", "copy-selection.newlines"):
        assert by_id[edge].difference == "deliberately_different"
        assert by_id[edge].required
        assert set(by_id[edge].hosts) == {"dash", "react", "streamlit"}


def test_retry_and_superseded_response_have_separate_required_evidence(requirements):
    retry = next(r for r in requirements if r.id == "runtime-error.recovery")
    stale = next(r for r in requirements if r.id == "runtime-error.superseded-response")
    assert retry.required and stale.required
    assert "retry" in retry.expected.lower()
    assert "late response" not in retry.expected.lower()
    assert "older" in stale.expected.lower() and "newer" in stale.expected.lower()
    assert set(retry.hosts) == set(stale.hosts) == {"dash", "react", "streamlit"}
    for host in retry.hosts:
        assert retry.browser_tests[host] == ("browser.runtime-error.recovery",)
        assert stale.browser_tests[host] == ("browser.runtime-error.superseded-response",)


@pytest.mark.parametrize("change,match", [
    ({"id": ""}, "id"),
    ({"inventory_id": ""}, "inventory"),
    ({"inventory_id": "invented"}, "inventory"),
    ({"hosts": ("unknown",)}, "host"),
    ({"expected": " "}, "expected"),
    ({"constraints": ()}, "constraints"),
    ({"task": ""}, "task"),
    ({"product_behavior": ""}, "product_behavior"),
    ({"shared_tests": (), "browser_tests": {}}, "verification"),
    ({"browser_tests": {}}, "browser"),
    ({"difference": "looks-good"}, "difference"),
    ({"ag_grid": {}}, "reference"),
])
def test_validator_rejects_incomplete_requirements(contract, requirements, change, match):
    changed = [replace(requirements[0], **change), *requirements[1:]]
    with pytest.raises(ValueError, match=match):
        contract.validate_requirements(changed, CAPABILITIES)


@pytest.mark.parametrize("field,value", [
    ("version", ""), ("edition", ""), ("row_model", ""), ("options", {}),
    ("docs", []), ("behavior", ""), ("test_id", ""),
])
def test_validator_rejects_missing_reference_fields(contract, requirements, field, value):
    reference = dict(requirements[0].ag_grid)
    reference[field] = value
    with pytest.raises(ValueError, match="reference"):
        contract.validate_requirements([replace(requirements[0], ag_grid=reference), *requirements[1:]], CAPABILITIES)


def test_validator_rejects_duplicate_ids(contract, requirements):
    with pytest.raises(ValueError, match="duplicate"):
        contract.validate_requirements([*requirements, requirements[0]], CAPABILITIES)


def test_validator_rejects_uncovered_inventory(contract, requirements):
    with pytest.raises(ValueError, match="coverage"):
        contract.validate_requirements([r for r in requirements if r.inventory_id != "runtime-error"], CAPABILITIES)


def test_validator_detects_new_supported_inventory_without_requirements(contract, requirements):
    extra = replace(CAPABILITIES[0], id="new-published-workflow")
    with pytest.raises(ValueError, match="coverage"):
        contract.validate_requirements(requirements, (*CAPABILITIES, extra))


@pytest.mark.parametrize("inventory_id", ["initial-workspace", "runtime-error", "copy-selection"])
def test_validator_rejects_supported_inventory_with_only_optional_requirements(contract, requirements, inventory_id):
    changed = [replace(r, required=False) if r.inventory_id == inventory_id else r for r in requirements]
    with pytest.raises(ValueError, match="mandatory coverage"):
        contract.validate_requirements(changed, CAPABILITIES)


def test_validator_allows_optional_supplement_without_weakening_mandatory_coverage(contract, requirements):
    supplement = replace(requirements[0], id="initial-workspace.optional-supplement", required=False)
    contract.validate_requirements([*requirements, supplement], CAPABILITIES)


@pytest.mark.parametrize("requirement_id", [
    "hierarchy-expand.round-trip", "header-filter.apply-clear", "sorting.direction-order",
    "totals.subtotals", "layout-modes.switch",
])
def test_eclair_acceptance_oracle_matches_source_records(requirements, requirement_id):
    # The sum is derived from the fixture, independently of contract text.
    sales = sum(Decimal(str(row["ventes_€"])) for row in FLAGSHIP_QUALIFICATION_SOURCE_ROWS
                if row["region"] == "Éclair" and row["scope"] == "Included")
    assert sales == Decimal("120.25")
    requirement = next(r for r in requirements if r.id == requirement_id)
    for promise in (requirement.expected, requirement.product_behavior):
        numbers = re.findall(r"\b120(?:\.\d+)?\b", promise)
        assert numbers and all(Decimal(number) == sales for number in numbers)


def test_density_case_has_independent_geometry_targets(requirements):
    requirement = next(r for r in requirements if r.id == "density.cycle")
    case = requirement.ag_grid.get("case", {})
    assert case.get("density_sequence") == [
        {"name": "Normal", "height_css_px": 44},
        {"name": "Loose", "height_css_px": 56},
        {"name": "Compact", "height_css_px": 36},
        {"name": "Normal", "height_css_px": 44},
    ]
    assert case["tolerance_css_px"] == 1


def test_decimal_case_has_exact_tie_rounding_outputs(requirements):
    case = next(r for r in requirements if r.id == "number-format.decimals").ag_grid.get("case", {})
    assert case.get("values") == [100.25, 20, -100.25]
    assert case.get("two_decimal_text") == ["100.25", "20.00", "-100.25"]
    assert case.get("one_decimal_text") == ["100.3", "20.0", "-100.3"]
    assert case.get("rounding") == "nearest, ties away from zero"


def test_color_scale_case_has_literal_stops_and_computed_colors(requirements):
    case = next(r for r in requirements if r.id == "color-scales.scale").ag_grid.get("case", {})
    assert case.get("palette") == "redGreen"
    assert case.get("stops") == [
        {"value": 0, "background": "rgba(248, 105, 107, 0.82)"},
        {"value": 100, "background": "rgba(248, 105, 107, 0.06)"},
        {"value": 200, "background": "rgba(99, 190, 123, 0.82)"},
    ]


@pytest.mark.parametrize("requirement_id,locked,expected_rows", [
    ("range-chart.locked-source", True, [["Éclair", 100.25, 2], ["München", 49.75, 3]]),
    ("range-chart.live-source", False, [["München", 49.75, 3]]),
])
def test_chart_source_policy_has_exact_result_after_reselection(requirements, requirement_id, locked, expected_rows):
    matches = [r for r in requirements if r.id == requirement_id]
    assert len(matches) == 1, "source-policy requirement is missing"
    case = matches[0].ag_grid["case"]
    assert case["locked"] is locked
    assert case["next_selected_regions"] == ["München"]
    assert case["expected_rows_after_reselection"] == expected_rows
    assert matches[0].required is True


def test_selected_copy_records_sales_first_reorder_and_exact_body(requirements):
    requirement = next(r for r in requirements if r.id == "copy-selection.logical-cells")
    case = requirement.ag_grid.get("case", {})
    assert case.get("displayed_measure_fields") == ["ventes_€", "unités", "écart"]
    assert case.get("setup_action") == "Move Q1 sales before Q1 units in the displayed column order"
    assert case.get("expected_body") == [[100.25, 2], [49.75, 3]]
    assert requirement.ag_grid["options"]["valueFields"] == ["ventes_€", "unités", "écart"]


def test_validator_rejects_cross_host_mapping_for_host_only_case(contract, requirements):
    index = next(i for i, r in enumerate(requirements) if r.inventory_id == "streamlit-no-rerun")
    changed = list(requirements)
    changed[index] = replace(changed[index], hosts=("streamlit", "dash"))
    with pytest.raises(ValueError, match="host"):
        contract.validate_requirements(changed, CAPABILITIES)


def test_loader_rejects_unsupported_contract_schema(contract, tmp_path):
    path = tmp_path / "contract.json"
    path.write_text(json.dumps({"schema_version": 99, "requirements": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="schema"):
        contract.load_requirements(path)


def test_requirement_and_case_result_are_frozen(contract, requirements):
    with pytest.raises(FrozenInstanceError):
        requirements[0].required = False
    result = contract.CaseResult(requirements[0].id, "product", "dash", "case.startup", "failed", "empty grid", "Chrome")
    with pytest.raises(FrozenInstanceError):
        result.result = "passed"


def test_matrix_keeps_missing_evidence_unverified_and_excludes_other_hosts(contract, requirements):
    matrix = contract.render_matrix(requirements)
    assert "| required | required | required |" not in matrix
    assert "unverified" in matrix
    host_line = next(line for line in matrix.splitlines() if "streamlit-no-rerun.local-interactions" in line)
    assert "| not_applicable | not_applicable | unverified |" in host_line
    assert "intentionally unavailable" in matrix


def test_matrix_shows_reference_and_product_results_independently(contract, requirements):
    requirement = requirements[0]
    results = [
        contract.CaseResult(requirement.id, "ag_grid", None, requirement.ag_grid["test_id"], "passed", "matching fixture", "Chrome"),
        contract.CaseResult(requirement.id, "product", "dash", requirement.browser_tests["dash"][0], "failed", "empty grid", "Chrome"),
    ]
    line = next(line for line in contract.render_matrix(requirements, results).splitlines() if requirement.id in line)
    assert "| passed | failed | unverified | unverified |" in line
    assert "| unverified |" in line  # Classification is not upgraded by observations alone.


def test_matrix_requires_all_mapped_cases_before_showing_pass(contract, requirements):
    requirement = replace(requirements[0], browser_tests={"dash": ("startup.cells", "startup.totals"), "react": ("startup",), "streamlit": ("startup",)})
    result = contract.CaseResult(requirement.id, "product", "dash", "startup.cells", "passed", "cells present", "Chrome")
    line = next(line for line in contract.render_matrix([requirement], [result]).splitlines() if requirement.id in line)
    assert "| unverified | unverified | unverified |" in line


def test_matrix_includes_failed_shared_checks_in_host_outcome(contract, requirements):
    requirement = requirements[0]
    results = [
        contract.CaseResult(requirement.id, "product", "dash", requirement.browser_tests["dash"][0], "passed", "cells present", "Chrome"),
        contract.CaseResult(requirement.id, "product", "dash", requirement.shared_tests[0], "failed", "wrong total", "Chrome"),
    ]
    line = next(line for line in contract.render_matrix([requirement], results).splitlines() if requirement.id in line)
    assert "| unverified | failed | unverified | unverified |" in line


def test_matrix_does_not_treat_a_browser_pass_as_complete_shared_evidence(contract, requirements):
    requirement = requirements[0]
    result = contract.CaseResult(requirement.id, "product", "dash", requirement.browser_tests["dash"][0], "passed", "cells present", "Chrome")
    line = next(line for line in contract.render_matrix([requirement], [result]).splitlines() if requirement.id in line)
    assert "| unverified | unverified | unverified | unverified |" in line


def test_matrix_rejects_result_for_an_excluded_host(contract, requirements):
    requirement = next(r for r in requirements if r.inventory_id == "streamlit-no-rerun")
    result = contract.CaseResult(requirement.id, "product", "dash", "case.invalid", "passed", "claimed", "Chrome")
    with pytest.raises(ValueError, match="host"):
        contract.render_matrix(requirements, [result])


@pytest.mark.parametrize("change", [{"result": "success"}, {"test_id": "invented"}, {"system": "other"}, {"requirement_id": "missing"}])
def test_matrix_rejects_results_that_cannot_bind_to_the_contract(contract, requirements, change):
    requirement = requirements[0]
    result = contract.CaseResult(requirement.id, "product", "dash", requirement.browser_tests["dash"][0], "passed", "matching", "Chrome")
    with pytest.raises(ValueError):
        contract.render_matrix(requirements, [replace(result, **change)])


def test_matrix_escapes_multiline_pipe_text_and_never_publishes_private_evidence(contract, requirements):
    requirement = replace(requirements[0], task="Open | pivot\nworkspace")
    result = contract.CaseResult(requirement.id, "product", "dash", requirement.browser_tests["dash"][0], "failed", "secret observed value", "Chrome", ("private/token.trace",))
    matrix = contract.render_matrix([requirement], [result])
    assert "Open \\| pivot<br>workspace" in matrix
    assert "secret observed value" not in matrix
    assert "private/token.trace" not in matrix
