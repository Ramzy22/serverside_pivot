"""Reviewed per-host shared integration assertions backed by real browser executions.

This registry is dependency-neutral for manifest classification. Future entries
are explicit coverage declarations and remain ineligible until reviewed against
an authored assertion. Neither check flags nor a registry entry execute a test.
"""
from dataclasses import asdict, dataclass, replace
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Mapping
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class SharedClientAssertion:
    browser_test_id: str
    node_template: str
    phase: str
    checks: tuple[str, ...]
    semantics: str
    reviewed: bool = True


def _entry(name, journey, checks, semantics, *, phase="parity", reviewed=True):
    return SharedClientAssertion("browser." + name,
        "tests/test_flagship_workflows.py::" + journey, phase, tuple(checks), semantics, reviewed)


# Each list is intentionally explicit. Do not infer equivalence from ID prefixes.
SHARED_CLIENT_ASSERTIONS: Mapping[str, SharedClientAssertion] = {
    **{"shared." + name: _entry(name, f"test_flagship_selected_clipboard[{name}-{{host}}]", checks, semantics)
       for name, checks, semantics in (
        ("cell-selection.range-clear", ("logicalCellsVerified", "clipboardBytesVerified", "rangeCleared", "clearedCopyAbsent", "clearedClipboardMonitored"), "Exact four logical cells; clear; clipboard sentinel remains stable"),
        ("copy-selection.logical-cells", ("logicalCellsVerified", "clipboardBytesVerified", "displayOrderVerified"), "Sales-first raw 2x2 literal matrix, no extra cells"),
        ("copy-selection.display-order", ("logicalCellsVerified", "clipboardBytesVerified", "displayOrderVerified"), "Displayed descending native region order; units before sales; exact formatted payload"),
        ("copy-selection.pinned-columns", ("logicalCellsVerified", "clipboardBytesVerified", "pinnedNoDuplicates", "horizontalScrollVerified"), "Q1 units pinned left plus sales after physical scroll; no duplicate logical cells"),
        ("copy-selection.hidden-columns", ("logicalCellsVerified", "clipboardBytesVerified", "hiddenColumnExcluded", "headerOptInVerified"), "Hide selected variance; exact two-field body and headers without placeholders"),
        ("copy-selection.formatted-values", ("logicalCellsVerified", "clipboardBytesVerified"), "Installed Format dialog fixed:2; displayed and copied values match"),
        ("copy-selection.headers", ("logicalCellsVerified", "clipboardBytesVerified", "headerOptInVerified"), "Same selection with and without exact selected headers"),
        ("keyboard-copy.selection", ("logicalCellsVerified", "clipboardBytesVerified", "nativeKeyboardCopyVerified", "singleWritePerKeypressVerified"), "Native focused shortcut equals menu representation; exactly one write over interval"),
    )},
    **{"shared." + name: _entry(name, f"test_flagship_column_geometry[{name}-{{host}}]", checks, semantics)
       for name, checks, semantics in (
        ("column-sizing.resize", ("resizeDeltaVerified", "geometryScrollVerified", "logicalValuesPreserved"), "Physical +40px resize; neighboring IDs and values unchanged after scroll"),
        ("column-sizing.auto-size", ("autoSizeFitVerified", "autoSizeIdempotent", "logicalValuesPreserved"), "Visible longest label/value fits; bounded and idempotent within 2px"),
        ("column-pinning.round-trip", ("pinRoundTripVerified", "geometryScrollVerified", "logicalValuesPreserved", "clipboardOrderVerified"), "Stationary pinned geometry, unique cells/export order, restored unpin order"),
    )},
    **{"shared.copy-selection." + name: _entry("copy-selection." + name,
        f"test_flagship_embedded_clipboard[{name}-{{host}}]",
        ("logicalCellsVerified", "clipboardBytesVerified", "preDeliveryBytesVerified"),
        "Actual product serializer before OS delivery and parsed native clipboard; control characters and neighbor preserved")
       for name in ("tabs", "newlines")},
    "shared.keyboard-copy.native-input": _entry("keyboard-copy.native-input", "test_flagship_native_input_copy[{host}]",
        ("clipboardBytesVerified", "nativeKeyboardCopyVerified", "filterInputNativeCopyVerified"),
        "Native selected text in global search, filter input and dialog; grid copy never steals focus"),
    **{"shared." + name: _entry(name, f"test_flagship_export_jobs[{name}-{{host}}]", checks,
        "Correlated real V1 export jobs, parsed literal CSV/XLSX; cancellation cleanup precedes fresh filtered work", phase="exports")
       for name, checks in (
        ("large-delivery.progress", ("measuredProgressVerified", "parsedCsvVerified", "parsedXlsxVerified")),
        ("large-delivery.cancel-retry", ("serverCancellationVerified", "nativeCleanupVerified", "noPartialDownloadVerified", "parsedCsvVerified", "parsedXlsxVerified")),
        ("journey-recovery.cancel-new-work", ("serverCancellationVerified", "nativeCleanupVerified", "noPartialDownloadVerified", "parsedCsvVerified", "parsedXlsxVerified")),
    )},
    **{"shared." + name: _entry(name, f"test_flagship_presentation[{name}-{{host}}]",
        ("actionCompleted", "logicalValuesPreserved", "v1RequestsCorrelated"), semantics)
       for name, semantics in (
        ("row-numbers.toggle", "Dedicated sequential visible numbering; policy, unchanged values and surviving selection after disable"),
        ("density.cycle", "Measured 44/56/36/44px rows, identical IDs and authoritative sales200"),
        ("number-format.decimals", "Default and selected scopes: positive/negative midpoint rounding, two-decimal controls unchanged"),
        ("cell-format.apply-close", "Selected/unselected computed foreground/background, returned grid focus and exact copy"),
        ("color-scales.scale", "Exact redGreen 0/100/200 computed backgrounds; selected column only; unchanged controls"),
        ("color-scales.data-bars", "Actual usable-width bar geometry, zero and bound policy, visible text"),
        ("themes.select-override", "Computed theme tokens and override; readable values and successful controls/copy"),
        ("zoom-immersive.round-trip", "Measured125% geometry, immersive expansion, selection/copy and restored scroll/zoom"),
    )},
    **{"shared." + name: _entry(name, f"test_flagship_charts[{name}-{{host}}]",
        ("actionCompleted", "logicalValuesPreserved", "v1RequestsCorrelated"), semantics)
       for name, semantics in (
        ("range-chart.source", "Actual rendered bar chart and downloaded model CSV; exact two cities, sales-before-units and values"),
        ("range-chart.locked-source", "Actual lock and changed one-row selection; original two-row chart model/series retained"),
        ("range-chart.live-source", "Actual unlocked chart follows changed one-row selection; stale categories/series absent"),
        ("chart-pane.lifecycle", "Physical40px resize, dock/float/redock retain exact source, close then fresh exact grid copy"),
        ("chart-export.artifact", "Real SVG download decoded; declared dimensions/title/legend/marks and scale; grid/pane preserved"),
    )},
    **{"shared." + name: _entry(name, f"test_flagship_combined_journeys[{name}-{{host}}]",
        ("actionCompleted", "v1RequestsCorrelated", check), semantics, phase=phase)
       for name, check, phase, semantics in (
        ("saved-scroll.restore", "savedScrollRestored", "scroll", "Save/reload both nonzero axes; logical first row/column and offsets within2px"),
        ("copy-selection.merged-headers", "mergedSelectionHeadersVerified", "parity", "Actual rich HTML selected Q1/Q2 group labels/spans sum to selected width"),
        ("copy-selection.unloaded-rows", "unloadedSelectionRejectedWithoutWrite", "parts", "Actual unloaded range explicitly unavailable; native clipboard sentinel remains unchanged"),
        ("large-delivery.selected-part", "selectedPartRowsVerified", "parts", "At least3 parts; real part2 download exact5000 rows, headers/boundaries and filtered bounds"),
        ("journey-selection.filter-pin-scroll-copy", "combinedSelectionClipboardVerified", "parity", "Single filter Munchen/pin units/scroll/copy headers journey exact [3,49.75]"),
        ("journey-analysis.aggregate-expand-swap-export", "analysisDisplayAndExportVerified", "parity", "Single average/expand/swap/parsed export journey; displayed Q1grand75 and exact raw-source matrix"),
    )},
    **{"shared." + name: _entry(name, f"test_flagship_host_integration[{{host}}-{name}]",
        ("actionCompleted", "fixtureTotalsVerified", *checks), semantics)
       for name, checks, semantics in (
        ("streamlit-no-rerun.local-interactions", ("scriptRunCounterUnchanged", "nativeScrollVerified", "nativeResizeVerified", "gridStatePreserved", "sizingPermissionErrorsAbsent"), "Real Streamlit session script counter unchanged after physical scroll and chart resize"),
        ("react-direct-transport.v1", ("allRequestsV1", "dashCallbacksAbsent", "consumerUnmountAbortedRequests", "componentListenersReleased"), "Public installed React mount/direct V1 payloads, typed freshness, totals200/10/-6.75, cleanup and stale rejection"),
        ("dash-callback-transport.v1", ("callbackWrappersVerified", "errorRecoveryVerified", "staleCallbackRejected"), "Live installed Dash V1 callbacks, typed freshness, exact totals, stale callback rejection and error recovery"),
    )},
}


def _digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _junit_passed(path, node):
    root = ET.parse(path).getroot()
    if root.tag not in {"testsuite", "testsuites"}:
        return False
    module, name = node.split("::", 1)
    module = module.removesuffix(".py").replace("/", ".")
    matches = [case for case in root.iter("testcase") if (case.get("classname"), case.get("name")) == (module, name)]
    if len(matches) != 1 or any(matches[0].find(tag) is not None for tag in ("failure", "error", "skipped")):
        return False
    # Inconsistent or skipped process evidence cannot become a reused pass.
    for suite in root.iter():
        if suite.tag in {"testsuite", "testsuites"}:
            if "tests" in suite.attrib and int(suite.attrib["tests"]) != len(list(suite.iter("testcase"))):
                return False
            if any(int(suite.get(key, "0")) != 0 for key in ("skipped", "failures", "errors")):
                return False
    return True


def _observation_matches(assertion, case):
    for name in case.evidence_paths:
        if Path(name).suffix != ".json":
            continue
        data = json.loads(Path(name).read_text(encoding="utf-8"))
        result = data.get("result") if isinstance(data, dict) else None
        if not isinstance(result, dict) or result.get("result") != "passed":
            continue
        if not all(result.get(check) is True for check in assertion.checks):
            continue
        if case.test_id == "browser.copy-selection.logical-cells" and data.get("parsedTSV") != [["100.25", "2"], ["49.75", "3"]]:
            continue
        return True
    return False


def validate_shared_reuse(case, binding, requirement, observations):
    """Independently verify private reuse receipt when evaluating a manifest."""
    from scripts.flagship_harness import validate_harness_record
    from scripts.flagship_manifest import BoundCaseResult, _case_issues
    assertion = SHARED_CLIENT_ASSERTIONS[case.test_id]
    if (not assertion.reviewed or case.requirement_id != requirement.id
            or case.test_id not in requirement.shared_tests
            or assertion.browser_test_id not in requirement.browser_tests.get(case.host, ())):
        raise ValueError("shared client mapping awaits authored semantic review")
    receipts = [item for item in observations if isinstance(item, dict) and item.get("sharedClientReuse") is True]
    if len(receipts) != 1:
        raise ValueError("shared client requires exactly one reuse receipt")
    receipt = receipts[0]
    node = assertion.node_template.format(host=case.host)
    if (receipt.get("runId") != binding.run_id or receipt.get("sourceCommit") != binding.source_commit
            or receipt.get("artifactSha256") != binding.sha256 or receipt.get("host") != case.host
            or receipt.get("sharedTestId") != case.test_id or receipt.get("node") != node
            or receipt.get("phase") != assertion.phase or receipt.get("independentExecution") is not False
            or receipt.get("qualificationEvidence") is not True):
        raise ValueError("shared client reuse identity differs")
    original = BoundCaseResult(**{**receipt["originatingCase"], "evidence_paths": tuple(receipt["originatingCase"]["evidence_paths"])})
    if (original.requirement_id != case.requirement_id or original.host != case.host or original.system != "product"
            or original.test_id != assertion.browser_test_id or original.result != "passed"
            or original.browser != case.browser or original.loaded_assets != case.loaded_assets
            or _case_issues(original, binding, requirement) or not _observation_matches(assertion, original)):
        raise ValueError("originating browser case does not qualify shared semantics")
    for field in ("junit", "harness"):
        path = receipt[field]["path"]
        if path not in case.evidence_paths or case.evidence_sha256.get(path) != receipt[field]["sha256"] or _digest(path) != receipt[field]["sha256"]:
            raise ValueError("shared execution proof is not hash bound")
    validate_harness_record(binding, receipt["harness"]["path"])
    if not _junit_passed(receipt["junit"]["path"], node):
        raise ValueError("matching JUnit node did not pass")


def shared_client_cases(binding, requirements, product_cases, executed_nodes, junit_reports, evidence_root):
    """Reuse only explicit reviewed matches; missing execution yields no record."""
    from scripts.flagship_harness import validate_harness_record
    from scripts.flagship_manifest import _case_issues
    root = Path(evidence_root).resolve()
    if root.name != binding.run_id or root.parent.name != "runs":
        raise ValueError("shared client evidence must be under current runs/<run_id>")
    results = []
    for requirement in requirements:
        if not requirement.required:
            continue
        for test_id in requirement.shared_tests:
            assertion = SHARED_CLIENT_ASSERTIONS.get(test_id)
            if assertion is None:
                continue
            for host in requirement.hosts:
                node = assertion.node_template.format(host=host)
                if node not in executed_nodes.get(assertion.phase, ()):
                    continue
                matches = [case for case in product_cases if (case.requirement_id, case.system, case.host, case.test_id)
                           == (requirement.id, "product", host, assertion.browser_test_id)]
                if not matches:
                    continue
                original = matches[0]
                passed = False
                diagnostic = "originating execution did not qualify"
                report, harness = junit_reports.get(assertion.phase), root / "browser-harness.json"
                try:
                    if not assertion.reviewed or len(matches) != 1:
                        raise ValueError("unreviewed mapping or duplicate originating browser records")
                    if original.result != "passed" or _case_issues(original, binding, requirement):
                        raise ValueError("originating browser execution failed provenance/asset/evidence validation")
                    if assertion.browser_test_id not in requirement.browser_tests.get(host, ()) or not _observation_matches(assertion, original):
                        raise ValueError("originating observation lacks exact shared acceptance")
                    validate_harness_record(binding, harness)
                    if not report or not _junit_passed(report, node):
                        raise ValueError("exact matching JUnit execution did not pass")
                    passed = True
                except (OSError, ValueError, TypeError, KeyError, LookupError, ET.ParseError, subprocess.CalledProcessError) as error:
                    diagnostic = str(error)
                receipt = {"sharedClientReuse": True, "qualificationEvidence": True, "independentExecution": False,
                    "runId": binding.run_id, "sourceCommit": binding.source_commit, "artifactSha256": dict(binding.sha256),
                    "host": host, "sharedTestId": test_id, "node": node, "phase": assertion.phase,
                    "semantics": assertion.semantics, "requiredObservationChecks": list(assertion.checks),
                    "originatingCase": asdict(original), "diagnostic": "" if passed else diagnostic,
                    "result": "passed" if passed else "failed"}
                paths = list(original.evidence_paths)
                for field, path in (("junit", report), ("harness", harness)):
                    if path and Path(path).is_file():
                        path = str(Path(path).resolve())
                        receipt[field] = {"path": path, "sha256": _digest(path)}
                        paths.append(path)
                location = root / "shared" / test_id / f"{host}.json"
                location.parent.mkdir(parents=True, exist_ok=True)
                location.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n", encoding="utf-8")
                paths.append(str(location))
                paths = tuple(dict.fromkeys(paths))
                # Copy only a validated pass; every attempted invalid match remains failed.
                results.append(replace(original, test_id=test_id, result="passed" if passed else "failed",
                    observed=f"Shared client-runtime integration; reused execution of {node}; not an independent execution",
                    evidence_paths=paths, evidence_sha256={path: _digest(path) for path in paths if Path(path).is_file()}))
    return tuple(results)
