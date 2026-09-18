"""Run Dash component generation and fail on react-docgen parser errors."""

from __future__ import annotations

import json
from copy import deepcopy
import subprocess
import sys
import tempfile
from pathlib import Path

import dash
from dash.development.component_generator import generate_components, reserved_words


DOCGEN_ERROR_MARKERS = (
    "Error with path",
    "did not recognize object of type",
    "Multiple exported component definitions found",
    "Cannot convert undefined or null to object",
)


def display_docgen_output(output: str) -> None:
    """Keep actionable docgen output while suppressing repetitive TS field notices."""
    visible_lines = [
        line for line in output.splitlines()
        if line and not line.startswith("Description for DashTanstackPivot.react.")
    ]
    visible = "\n".join(visible_lines).strip()
    if visible:
        print(visible)


def extract_typescript_metadata(package_root: Path, public_component: Path) -> dict[str, object]:
    """Run Dash's TS extractor with the optional-object union bug fixed.

    Dash's current extractor discards non-primitive union members. Optional
    interface and array props are represented by TypeScript as
    ``Shape | undefined`` and ``Shape[] | undefined``, so they used to be
    emitted as ``any``.  Patch a temporary copy rather than editing Dash in
    site-packages; the replacement retains every member except ``undefined``.
    """
    dash_extractor = Path(dash.__file__).with_name("extract-meta.js")
    source = dash_extractor.read_text(encoding="utf-8")
    legacy_filter = """return (
                    unionSupport.includes(typeName) ||
                    isArray(checker.typeToString(t))
                );"""
    replacement_filter = """return !(t.getFlags() & ts.TypeFlags.Undefined);"""
    if source.count(legacy_filter) != 1:
        raise RuntimeError("Dash TypeScript extractor no longer has the expected union filter.")
    patched_source = source.replace(legacy_filter, replacement_filter)
    reserved_patterns = "|".join(f"^{word}$" for word in reserved_words)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".js",
            prefix="dash_extract_meta_typed_",
            dir=package_root,
            encoding="utf-8",
            delete=False,
        ) as temporary:
            temporary.write(patched_source)
            temporary_path = Path(temporary.name)
        result = subprocess.run(
            ["node", str(temporary_path), "^_", reserved_patterns, str(public_component)],
            cwd=package_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    diagnostics = result.stderr or ""
    if diagnostics:
        display_docgen_output(diagnostics)
    if result.returncode != 0:
        raise RuntimeError(f"Dash TypeScript metadata extraction failed (status={result.returncode}).")
    if any(marker in diagnostics for marker in DOCGEN_ERROR_MARKERS):
        raise RuntimeError("Dash component docgen emitted parser errors.")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Dash TypeScript metadata extractor returned invalid JSON: {error}") from error

# This is the Dash/Python API contract. TypeScript is now the metadata source,
# so a prop added to (or accidentally removed from) the public interface must
# be an intentional, reviewed API change rather than a side effect of docgen.
EXPECTED_DASH_PROP_NAMES = frozenset({
    'activeReportId', 'availableFieldList', 'cellUpdate', 'cellUpdates',
    'chartCanvasPanes', 'chartDefaults', 'chartDefinitions', 'chartEvent',
    'chartServerWindow', 'colFields', 'columnOrder', 'columnPinned',
    'columnPinning', 'columnSizing', 'columnVisibility', 'conditionalFormatting',
    'customDimensions', 'data', 'decimalPlaces', 'defaultTheme',
    'defaultValueFormat', 'detailConfig', 'detailMode', 'drillEndpoint',
    'editLifecycleEvent', 'editState', 'editingConfig', 'expanded',
    'fieldPanelSizes', 'filters', 'grandTotalPosition', 'headerFormatting',
    'id', 'immersiveMode', 'localeText', 'measureAxis', 'mergedHeaderSeparator',
    'numberGroupSeparator', 'paginationConfig', 'performanceConfig',
    'persistence', 'persistence_type', 'pinningOptions', 'pinningPresets',
    'pivotTitle', 'reportDef', 'reset', 'rowFields', 'rowMove', 'rowPinned',
    'rowPinning', 'rowSelection', 'runtimeRequest', 'runtimeResponse',
    'saveViewTrigger', 'savedReports', 'savedView', 'selectedRows', 'serverSide',
    'setProps', 'showColTotals', 'showRowTotals', 'showSubtotals', 'sortEvent',
    'sortLock', 'sortOptions', 'sorting', 'sparklineFields', 'style', 'table',
    'tableCanvasSize', 'treeConfig', 'uiConfig', 'valConfigs', 'validationRules',
    'viewMode', 'viewState',
})


# Dash's TypeScript extractor currently omits comments on optional interface
# members. Keep this public compatibility policy explicit in both metadata and
# generated Python docstrings instead of allowing legacy edit props to imply a
# supported editing feature.
LEGACY_EDITING_PROP_DESCRIPTIONS = {
    "cellUpdate": "Deprecated compatibility input. Editing is unsupported and this value is ignored.",
    "cellUpdates": "Deprecated compatibility input. Editing is unsupported and this value is ignored.",
    "editingConfig": "Deprecated compatibility input. Editing is unsupported and this value is ignored.",
    "editLifecycleEvent": "Deprecated compatibility input. Editing is unsupported and this value is ignored.",
    "editState": "Deprecated compatibility input. Editing is unsupported and this value is ignored.",
}


def apply_public_contract_descriptions(metadata: dict[str, object]) -> None:
    """Annotate legacy no-op inputs that TS docgen cannot describe itself."""
    for component in metadata.values():
        if not isinstance(component, dict):
            continue
        props = component.get("props")
        if not isinstance(props, dict):
            continue
        for prop_name, description in LEGACY_EDITING_PROP_DESCRIPTIONS.items():
            prop = props.get(prop_name)
            if isinstance(prop, dict):
                prop["description"] = description


def normalize_generated_metadata(package_root: Path, public_component: Path) -> bool:
    metadata_path = package_root / "dash_tanstack_pivot" / "metadata.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if len(metadata) != 1:
            raise ValueError(f"expected one public component, found {len(metadata)}")
        component = next(iter(metadata.values()))
        stable_source_path = public_component.relative_to(package_root).as_posix()
        metadata_path.write_text(
            json.dumps({stable_source_path: component}, separators=(",", ":")),
            encoding="utf-8",
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Invalid generated Dash metadata: {error}", file=sys.stderr)
        return False
    return True


def validate_generated_metadata(package_root: Path) -> bool:
    metadata_path = package_root / "dash_tanstack_pivot" / "metadata.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        component = next(iter(metadata.values()))
        generated_names = frozenset(component.get("props", {}))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Invalid generated Dash metadata: {error}", file=sys.stderr)
        return False
    if generated_names != EXPECTED_DASH_PROP_NAMES:
        missing = sorted(EXPECTED_DASH_PROP_NAMES - generated_names)
        unexpected = sorted(generated_names - EXPECTED_DASH_PROP_NAMES)
        print(
            "Generated Dash metadata changed unexpectedly "
            f"(missing={missing}, unexpected={unexpected}).",
            file=sys.stderr,
        )
        return False
    return True


def main() -> int:
    package_root = Path(__file__).resolve().parents[1]
    public_component = package_root / "src" / "lib" / "components" / "DashTanstackPivot.react.tsx"
    if not public_component.is_file():
        print(f"Missing public Dash component source: {public_component}", file=sys.stderr)
        return 1

    try:
        metadata = extract_typescript_metadata(package_root, public_component)
        apply_public_contract_descriptions(metadata)
        generate_components(
            str(public_component),
            "dash_tanstack_pivot",
            package_info_filename="package-info.json",
            # Dash's Python generator mutates nested metadata as it derives
            # annotations. Keep the TS-extracted document intact for the
            # published metadata contract.
            metadata=deepcopy(metadata),
        )
        metadata_path = package_root / "dash_tanstack_pivot" / "metadata.json"
        metadata_path.write_text(json.dumps(metadata, separators=(",", ":")), encoding="utf-8")
    except (OSError, RuntimeError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    if not normalize_generated_metadata(package_root, public_component):
        return 1
    if not validate_generated_metadata(package_root):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
