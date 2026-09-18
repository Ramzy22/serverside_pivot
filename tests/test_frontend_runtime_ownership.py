"""Structural contracts for the frontend runtime ownership migration."""

from pathlib import Path
import re


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LIB_ROOT = REPOSITORY_ROOT / "dash_tanstack_pivot" / "src" / "lib"


def _source(relative_path: str) -> str:
    return (LIB_ROOT / relative_path).read_text(encoding="utf-8")


def test_public_component_only_depends_on_the_internal_runtime_root():
    source = _source("components/DashTanstackPivot.react.tsx")

    assert "PivotRuntimeRoot" in source
    assert "useCoreRuntimeStage" not in source
    assert "useCommandRuntimeStage" not in source
    assert "useViewportRuntimeStage" not in source
    assert "useWorkspaceRuntimeStage" not in source


def test_typed_public_component_is_the_dash_metadata_source():
    source = _source("components/DashTanstackPivot.react.tsx")
    build_source = (REPOSITORY_ROOT / "dash_tanstack_pivot" / "scripts" / "build_py_strict.py").read_text(encoding="utf-8")

    assert "export interface DashTanstackPivotProps" in source
    assert "DashTanstackPivot.react.tsx" in build_source
    assert "EXPECTED_DASH_PROP_NAMES" in build_source
    assert "validate_generated_metadata" in build_source
    assert "extract_typescript_metadata" in build_source
    assert "TypeFlags.Undefined" in build_source


def test_editing_is_not_a_public_runtime_feature_and_legacy_props_are_noops():
    public_source = _source("components/DashTanstackPivot.react.tsx")
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    domain_source = _source("runtime/runtimeDomains.ts")
    build_source = (REPOSITORY_ROOT / "dash_tanstack_pivot" / "scripts" / "build_py_strict.py").read_text(encoding="utf-8")
    package_readme = (REPOSITORY_ROOT / "dash_tanstack_pivot" / "README.md").read_text(encoding="utf-8")

    for prop_name in ("cellUpdate", "cellUpdates", "editingConfig", "editLifecycleEvent", "editState"):
        assert f"{prop_name}?:" in public_source
    assert public_source.count("Editing is unsupported. This legacy compatibility input is ignored.") == 5
    assert "const EDITING_PUBLIC_FEATURE_SUPPORTED = false;" in public_source
    assert "showEditing: EDITING_PUBLIC_FEATURE_SUPPORTED" in setup_source
    assert "showEditPanel: EDITING_PUBLIC_FEATURE_SUPPORTED" in setup_source
    assert "stage.externalEditingConfig = null;" in setup_source
    assert "stage.externalEditState = null;" in setup_source
    assert "if (!EDITING_PUBLIC_FEATURE_SUPPORTED) return;" in setup_source
    assert "editingConfig?:" not in domain_source
    assert "editState?:" not in domain_source
    assert "LEGACY_EDITING_PROP_DESCRIPTIONS" in build_source
    assert "apply_public_contract_descriptions(metadata)" in build_source
    assert "Cell editing and edit transactions are not a supported public capability" in package_readme


def test_generated_metadata_keeps_optional_nested_types_from_typescript():
    import json

    metadata_path = REPOSITORY_ROOT / "dash_tanstack_pivot" / "dash_tanstack_pivot" / "metadata.json"
    component = next(iter(json.loads(metadata_path.read_text(encoding="utf-8")).values()))
    value_config = component["props"]["valConfigs"]["type"]
    chart_pane = component["props"]["chartCanvasPanes"]["type"]
    chart_settings = chart_pane["value"][0]["value"]["value"]["chartSettings"]

    assert value_config["value"][0]["value"]["name"] == "shape"
    assert chart_pane["value"][0]["value"]["name"] == "shape"
    assert chart_settings["value"][0]["name"] == "shape"


def test_runtime_domains_have_concrete_types_and_private_event_ports():
    source = _source("runtime/runtimeDomains.ts")

    for interface_name in (
        "PivotRuntimeProps",
        "CoreRuntimeDomain",
        "TransportRuntimeDomain",
        "CommandRuntimeDomain",
        "ViewportRuntimeDomain",
        "WorkspaceRuntimeDomain",
        "TransportDispatchPort",
        "ViewportNavigationPort",
    ):
        assert f"interface {interface_name}" in source
    assert "Record<string, unknown>" not in source
    assert "getDisplayRows" not in source
    assert "scrollToRow" in source


def test_migrated_domain_hooks_have_named_dependencies_and_no_env_mutation():
    runtime_root = LIB_ROOT / "runtime"
    migrated = (
        "useCoreRuntime.ts",
        "useTransportRuntime.ts",
        "useCommandRuntime.ts",
        "useViewportRuntime.ts",
        "useWorkspaceRuntime.ts",
    )

    for filename in migrated:
        source = (runtime_root / filename).read_text(encoding="utf-8")
        assert "Object.assign(" not in source
        assert not re.search(r"(?<!process\\.)\\benv\\.", source)
        if filename == "useCoreRuntime.ts":
            assert "return freeze<CoreRuntimeDomain>" in source
        elif filename == "useCommandRuntime.ts":
            assert "return freeze<CommandRuntimeDomain>" in source
        elif filename == "useViewportRuntime.ts":
            assert "return freeze<ViewportRuntimeDomain>" in source
        elif filename == "useWorkspaceRuntime.ts":
            assert "return freeze<WorkspaceRuntimeDomain>" in source
        else:
            assert "return domain" in source
        assert "Object.freeze" in source


def test_runtime_stages_no_longer_use_env_or_object_assign_composition():
    for filename in (
        "runtime/useCoreRuntimeStage.tsx",
        "runtime/useCommandRuntimeStage.tsx",
        "runtime/useViewportRuntimeStage.tsx",
        "runtime/useWorkspaceRuntimeStage.tsx",
    ):
        source = _source(filename)
        assert not re.search(r"(?<!process\\.)\\benv\\.", source)
        assert "Object.assign(" not in source


def test_runtime_stages_do_not_mutate_a_predecessor_domain():
    """Render-pass assembly is local and every exported stage freezes it."""
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    for filename in (
        "runtime/useCommandRuntimeStage.tsx",
        "runtime/useViewportRuntimeStage.tsx",
        "runtime/useWorkspaceRuntimeStage.tsx",
    ):
        source = _source(filename)
        assert "(input," in source or "(input)" in source
        assert not re.search(r"\\binput\\.[A-Za-z_$][A-Za-z0-9_$]*\\s*=", source)
        assert "Object.freeze(" in source

    assert "const stage = { props };" in setup_source
    assert "return Object.freeze(stage);" in setup_source


def test_runtime_domains_supply_frozen_stage_inputs_without_retaining_a_runtime_bag():
    command_source = _source("runtime/useCommandRuntime.ts")
    viewport_source = _source("runtime/useViewportRuntime.ts")
    workspace_source = _source("runtime/useWorkspaceRuntime.ts")

    for source in (command_source, viewport_source, workspace_source):
        assert "const stageInput = freeze({" in source
        assert "const runtime = {" not in source
    assert "useCommandRuntimeStage(stageInput, transport)" in command_source
    assert "useViewportRuntimeStage(stageInput, transport)" in viewport_source
    assert "useWorkspaceRuntimeStage(stageInput)" in workspace_source


def test_runtime_stages_replace_the_old_javascript_hook_shells():
    runtime_root = LIB_ROOT / "runtime"
    old_hook_root = LIB_ROOT / "hooks"

    for old_name, stage_name in (
        ("usePivotRuntimeSetup.js", "useCoreRuntimeStage.tsx"),
        ("usePivotRuntimeInteractions.js", "useCommandRuntimeStage.tsx"),
        ("usePivotRuntimeViewport.js", "useViewportRuntimeStage.tsx"),
        ("usePivotRuntimeWorkspace.js", "useWorkspaceRuntimeStage.tsx"),
    ):
        assert not (old_hook_root / old_name).exists()
        assert (runtime_root / stage_name).exists()

    runtime_sources = "\n".join(path.read_text(encoding="utf-8") for path in runtime_root.glob("*.ts*"))
    assert "usePivotRuntimeSetup" not in runtime_sources
    assert "usePivotRuntimeInteractions" not in runtime_sources
    assert "usePivotRuntimeViewport" not in runtime_sources
    assert "usePivotRuntimeWorkspace" not in runtime_sources


def test_workspace_shell_receives_presentation_not_the_runtime_assembly():
    shell_source = _source("components/PivotWorkspaceShell.js")
    model_source = _source("runtime/workspaceViewModel.ts")

    assert "PivotWorkspaceShell({ workspace })" in shell_source
    assert "const model = workspace.presentation;" in shell_source
    assert "domains.workspace.presentation" in model_source
    assert "PRESENTATION_KEYS" not in model_source
    assert "createWorkspaceViewModel(domains" in model_source


def test_workspace_shell_has_only_rendering_dependencies():
    shell_source = _source("components/PivotWorkspaceShell.js")

    assert "useReactTable" not in shell_source
    assert "useServerSideRowModel" not in shell_source
    assert "useRuntimeTransport" not in shell_source
    assert "usePivotTransactions" not in shell_source
    assert "getPivotProfiler" not in shell_source


def test_workspace_shell_imports_the_chart_column_resolver_it_renders():
    shell_source = _source("components/PivotWorkspaceShell.js")

    assert "resolveChartAvailableColumns," in shell_source
    assert "from '../hooks/usePivotNormalization'" in shell_source
    assert "availableColumns={resolveChartAvailableColumns(" in shell_source


def test_workspace_presentation_is_an_explicit_immutable_renderer_contract():
    presentation_source = _source("runtime/createWorkspacePresentation.ts")
    workspace_source = _source("runtime/useWorkspaceRuntime.ts")
    domains_source = _source("runtime/runtimeDomains.ts")

    assert "createWorkspacePresentation(composed as WorkspacePresentation)" in workspace_source
    assert "...composed" not in workspace_source
    assert "chartRequest: freeze" in workspace_source
    assert "Object.freeze" in presentation_source
    assert "source.renderCell" in presentation_source
    assert "source.handleAddChartCanvasPane" in presentation_source
    assert "readonly renderCell" in domains_source


def test_workspace_presentation_has_no_opaque_unknown_fields():
    domains_source = _source("runtime/runtimeDomains.ts")
    presentation = domains_source.split("export interface WorkspacePresentation {", 1)[1].split("\n}\n\n/** Narrow workspace-to-transport", 1)[0]

    assert ": unknown;" not in presentation
    assert "WorkspaceCollection" in presentation
    assert "WorkspaceRecord" in presentation


def test_core_domain_has_no_hidden_legacy_implementation_association():
    runtime_root = LIB_ROOT / "runtime"
    root_source = _source("runtime/PivotRuntimeRoot.tsx")
    core_source = _source("runtime/useCoreRuntime.ts")
    runtime_sources = "\n".join(path.read_text(encoding="utf-8") for path in runtime_root.glob("*.ts*"))

    assert not (runtime_root / "runtimeKernel.ts").exists()
    assert not (runtime_root / "runtimeImplementation.ts").exists()
    assert "coreImplementationFor" not in runtime_sources
    assert "bindCoreImplementation" not in runtime_sources
    assert "const core = useCoreRuntime(props);" in root_source
    assert "CoreRuntimeBindings" not in runtime_sources
    assert not re.search(r"\bbindings\b", runtime_sources)
    assert "shared mutable record" in core_source


def test_runtime_fragments_are_owned_locally_without_a_shared_publisher():
    runtime_root = LIB_ROOT / "runtime"
    migrated_sources = "\n".join(
        _source(f"runtime/{filename}")
        for filename in (
            "useCoreRuntimeStage.tsx",
            "useCommandRuntimeStage.tsx",
            "useViewportRuntimeStage.tsx",
            "useWorkspaceRuntimeStage.tsx",
        )
    )

    assert not (runtime_root / "publishRuntimeFragment.js").exists()
    assert "publishRuntimeFragment" not in migrated_sources
    assert "Object.assign(" not in migrated_sources
    assert "Object.entries(chartState)" in migrated_sources
    assert "Object.entries(exportCommands)" not in migrated_sources


def test_workspace_controls_receive_bounded_presentation_models():
    shell_source = _source("components/PivotWorkspaceShell.js")
    controls_source = _source("components/PivotWorkspaceControls.js")
    model_source = _source("runtime/workspaceViewModel.ts")

    assert "workspace.controls.toolbar" in shell_source
    assert "workspace.controls.sidebar" in shell_source
    assert "PivotWorkspaceToolbar" in controls_source
    assert "PivotWorkspaceSidebar" in controls_source
    assert "LazySidebarPanel" in controls_source
    assert "domains.workspace.toolbar" in model_source
    assert "domains.workspace.sidebar" in model_source
    assert "TOOLBAR_KEYS" not in model_source
    assert "SIDEBAR_KEYS" not in model_source


def test_workspace_canvas_receives_its_bounded_presentation_model():
    shell_source = _source("components/PivotWorkspaceShell.js")
    canvas_source = _source("components/PivotWorkspaceCanvas.js")
    model_source = _source("runtime/workspaceViewModel.ts")

    assert "<PivotWorkspaceCanvas canvas={workspace.canvas} />" in shell_source
    assert "export function PivotWorkspaceCanvas({ canvas })" in canvas_source
    assert "workspace.presentation" not in canvas_source
    assert "domains.workspace.canvas" in model_source
    assert "CANVAS_KEYS" not in model_source


def test_typed_runtime_root_replaces_the_legacy_bridge():
    root_source = _source("runtime/PivotRuntimeRoot.tsx")

    assert not (LIB_ROOT / "runtime" / "legacyRuntimeBridge.ts").exists()
    assert "useCoreRuntime" in root_source
    assert "useTransportRuntime" in root_source
    assert "useCommandRuntime" in root_source
    assert "useViewportRuntime" in root_source
    assert "useWorkspaceRuntime" in root_source
    assert "createWorkspaceViewModel(domains)" in root_source


def test_runtime_root_flows_frozen_domains_in_one_direction():
    root_source = _source("runtime/PivotRuntimeRoot.tsx")
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    command_source = _source("runtime/useCommandRuntime.ts")
    viewport_source = _source("runtime/useViewportRuntime.ts")
    workspace_source = _source("runtime/useWorkspaceRuntime.ts")
    assert root_source.index("const core =") < root_source.index("const transport =") < root_source.index("const commands =") < root_source.index("const viewport =") < root_source.index("const workspaceDomain =")
    assert root_source.index("useTransportResponseRuntime(core") < root_source.index("useTransportChartRequestDispatch(workspaceDomain")
    assert "return Object.freeze(stage);" in setup_source
    assert "const stageInput = freeze({" in command_source
    assert "const stageInput = freeze({" in viewport_source
    assert "CoreRuntimeBindings" not in command_source + viewport_source + workspace_source


def test_stage_adapters_take_stable_handles_from_their_typed_predecessors():
    transport_source = _source("runtime/useTransportRuntime.ts")
    command_source = _source("runtime/useCommandRuntime.ts")
    viewport_source = _source("runtime/useViewportRuntime.ts")
    workspace_source = _source("runtime/useWorkspaceRuntime.ts")

    # Every stage receives its typed predecessor plus the narrow event ports
    # it needs. No stage can retrieve a separate shared binding object.
    for source in (command_source, viewport_source, workspace_source):
        assert "void core" not in source
    assert "void commands" not in viewport_source
    assert "void transport" not in viewport_source
    assert "savePersistedState: core.persistence.save" in command_source
    assert "emitRuntimeRequest: transport.port.emit" in viewport_source
    assert "recordProfilerQueuedRequest: transport.profiling.recordQueued" in viewport_source
    assert "parentRef: viewport.virtualization.parent" in workspace_source
    assert "table: viewport.table.instance" in workspace_source
    assert "...commands" in viewport_source
    assert "const tableName = core.configuration.tableName" in transport_source


def test_transport_and_profiler_are_composed_before_interactions():
    root_source = _source("runtime/PivotRuntimeRoot.tsx")
    transport_source = _source("runtime/useTransportRuntime.ts")
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    profiling_source = _source("hooks/useRuntimeProfiling.js")

    assert "useRuntimeProfiling" in transport_source
    assert "useRuntimeTransport" in transport_source
    assert root_source.index("const transport =") < root_source.index("const commands =")
    assert "useRuntimeTransport(" not in interactions_source
    assert "export function useRuntimeProfiling" in profiling_source


def test_legacy_consumers_receive_transport_callbacks_only_through_explicit_ports():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    workspace_source = _source("runtime/useWorkspaceRuntimeStage.tsx")
    transport_source = _source("runtime/useTransportRuntime.ts")
    domains_source = _source("runtime/runtimeDomains.ts")

    assert "transportDomain.port" in interactions_source
    assert "export function useViewportRuntimeStage(input, transport)" in viewport_source
    assert "transport.profiling.recordQueued" in viewport_source
    assert "export function useWorkspaceRuntimeStage(input)" in workspace_source
    assert "recordProfilerResponse: transport.profiling.recordResponse" in transport_source
    assert "useRuntimeChartRequestDispatch({" in transport_source
    assert "transport.port.emit" not in workspace_source
    assert "runtime.emitRuntimeRequest" not in workspace_source
    assert "runtime.recordProfilerResponse" not in workspace_source
    assert "runtime.recordProfilerQueuedRequest" not in viewport_source
    assert "recordQueued" in domains_source


def test_chart_model_domain_has_explicit_inputs_and_a_named_result():
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    charts_source = _source("runtime/useRuntimeChartModels.js")

    assert "useRuntimeChartModels({" in viewport_source
    assert "useRuntimeChartModels(env" not in viewport_source
    assert "buildChartRequestBase: stage.buildChartRequestBase" in viewport_source
    assert "export function useRuntimeChartModels({" in charts_source
    assert "return {" in charts_source
    assert "chartCanvasPaneModels" in charts_source


def test_chart_workspace_presentation_is_separate_from_workspace_coordination():
    workspace_source = _source("runtime/useWorkspaceRuntimeStage.tsx")
    charts_source = _source("runtime/useChartWorkspacePresentation.js")

    assert "useChartWorkspacePresentation({" in workspace_source
    assert "useChartWorkspacePresentation(env" not in workspace_source
    assert "export function useChartWorkspacePresentation({" in charts_source
    assert "renderChartCanvasPane" in charts_source
    assert "renderFloatingChartCanvasPane" in charts_source


def test_status_presentation_has_named_inputs_and_no_legacy_environment():
    workspace_source = _source("runtime/useWorkspaceRuntimeStage.tsx")
    status_source = _source("runtime/useWorkspaceStatusPresentation.js")

    assert "useWorkspaceStatusPresentation({" in workspace_source
    assert "useWorkspaceStatusPresentation(env" not in workspace_source
    assert "export function useWorkspaceStatusPresentation({" in status_source
    assert "statusAccessoryActions" in status_source
    assert "statusAccessoryModel" in status_source


def test_chart_request_dispatch_is_a_transport_owned_hook():
    workspace_source = _source("runtime/useWorkspaceRuntimeStage.tsx")
    transport_source = _source("runtime/useTransportRuntime.ts")
    dispatch_source = _source("runtime/useRuntimeChartRequestDispatch.js")

    assert "useRuntimeChartRequestDispatch({" not in workspace_source
    assert "useRuntimeChartRequestDispatch({" in transport_source
    assert "getState: () =>" in transport_source
    assert "export function useRuntimeChartRequestDispatch({ getState })" in dispatch_source
    assert "const {" in dispatch_source
    assert "emitRuntimeRequest('chart'" in dispatch_source


def test_runtime_response_lifecycle_is_owned_by_transport_not_workspace_presentation():
    workspace_source = _source("runtime/useWorkspaceRuntimeStage.tsx")
    transport_source = _source("runtime/useTransportRuntime.ts")
    response_source = _source("runtime/useRuntimeResponseLifecycle.js")

    assert "useRuntimeResponseLifecycle({" not in workspace_source
    assert "useRuntimeResponseLifecycle({" in transport_source
    assert "useRuntimeDataCommit({" in transport_source
    assert "export function useRuntimeResponseLifecycle({" in response_source
    assert "commitRuntimeDataPayload" in response_source
    assert "setTransportFilterOptionsState" in response_source


def test_response_post_commit_effects_are_transport_owned():
    workspace_source = _source("runtime/useWorkspaceRuntimeStage.tsx")
    transport_source = _source("runtime/useTransportRuntime.ts")
    post_commit_source = _source("runtime/useRuntimeResponsePostCommit.js")

    assert "useRuntimeResponsePostCommit({" not in workspace_source
    assert "useRuntimeResponsePostCommit({" in transport_source
    assert "export function useRuntimeResponsePostCommit({" in post_commit_source
    assert "TABLE_OVERLAY_CHART_PANE_ID" in post_commit_source


def test_clipboard_export_presentation_and_export_commands_have_explicit_boundaries():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    workspace_source = _source("runtime/useWorkspaceRuntimeStage.tsx")
    presentation_source = _source("runtime/useRuntimeClipboardExportPresentation.js")
    commands_source = _source("runtime/createRuntimeExportCommands.js")

    assert "useRuntimeClipboardExportPresentation({" not in interactions_source
    assert "useRuntimeClipboardExportPresentation({" in viewport_source
    assert "useRuntimeClipboardExportPresentation(env" not in viewport_source
    assert "createRuntimeExportCommands({" not in workspace_source
    assert "createRuntimeExportCommands({" in viewport_source
    assert "createRuntimeExportCommands(env" not in viewport_source
    assert "export function useRuntimeClipboardExportPresentation({" in presentation_source
    assert "export function createRuntimeExportCommands({" in commands_source
    assert "dispatchServerExportRequest" in commands_source


def test_selection_formatting_is_an_explicit_viewport_domain():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    selection_source = _source("runtime/useRuntimeSelectionFormatting.js")

    assert "useRuntimeSelectionFormatting({" not in interactions_source
    assert "useRuntimeSelectionFormatting({" in viewport_source
    assert "useRuntimeSelectionFormatting(env" not in viewport_source
    assert "export function useRuntimeSelectionFormatting({" in selection_source
    assert "applyFormatToSelection" in selection_source
    assert "buildSelectedClipboardPayload" not in selection_source


def test_rendered_column_measurement_is_isolated_in_the_viewport_domain():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    measurement_source = _source("runtime/useRuntimeColumnMeasurement.js")

    assert "useRuntimeColumnMeasurement({" not in interactions_source
    assert "useRuntimeColumnMeasurement({" in viewport_source
    assert "useRuntimeColumnMeasurement(env" not in viewport_source
    assert "export function useRuntimeColumnMeasurement({" in measurement_source
    assert "queueHeaderFitValidation" in measurement_source


def test_pivot_column_sorting_and_pinning_have_a_command_owner():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    commands_source = _source("runtime/useRuntimeColumnCommands.js")

    assert "useRuntimeColumnCommands({" not in interactions_source
    assert "useRuntimeColumnCommands({" in viewport_source
    assert "useRuntimeColumnCommands(env" not in viewport_source
    assert "export function useRuntimeColumnCommands({" in commands_source
    assert "applyPivotColumnSort" in commands_source


def test_export_state_is_owned_outside_the_general_setup_stage():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    export_state_source = _source("runtime/useRuntimeExportState.js")

    assert "useRuntimeExportState()" in setup_source
    assert "runtime.exportRequestSequenceRef = useRef(0)" not in setup_source
    assert "export function useRuntimeExportState()" in export_state_source
    assert "cancelledExportRequestIdsRef" in export_state_source


def test_layout_history_commands_are_separate_from_general_interactions():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    layout_history_source = _source("runtime/useRuntimeLayoutHistory.js")

    assert "useRuntimeLayoutHistory({" in interactions_source
    assert "useRuntimeLayoutHistory(env" not in interactions_source
    assert "export function useRuntimeLayoutHistory({" in layout_history_source
    assert "applyLayoutHistorySnapshot" in layout_history_source
    assert "setRowPinningWithHistory" in layout_history_source


def test_row_pinning_event_command_has_a_small_explicit_owner():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    row_pin_source = _source("runtime/createRuntimeRowPinCommand.js")

    assert "createRuntimeRowPinCommand({" in interactions_source
    assert "createRuntimeRowPinCommand(env" not in interactions_source
    assert "export function createRuntimeRowPinCommand({" in row_pin_source
    assert "rowPinned" in row_pin_source


def test_structural_sync_owns_request_branching_outside_interactions():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    viewport_domain_source = _source("runtime/useViewportRuntime.ts")
    sync_source = _source("runtime/useRuntimeStructuralSync.js")

    assert "useRuntimeStructuralSync({" in viewport_domain_source
    assert "useRuntimeStructuralSync({" not in interactions_source
    assert "useRuntimeStructuralSync(env" not in interactions_source
    assert "export function useRuntimeStructuralSync({" in sync_source
    assert "isExpansionOnly" in sync_source
    assert "isSortingOnly" in sync_source
    assert "SCHEMA_SHAPE_CHANGE_KEYS" in sync_source


def test_structural_sync_owns_its_own_previous_state_ref():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    viewport_domain_source = _source("runtime/useViewportRuntime.ts")
    sync_source = _source("runtime/useRuntimeStructuralSync.js")

    assert "runtime.lastPropsRef =" not in interactions_source
    assert "lastPropsRef:" not in viewport_domain_source
    assert "initialState: {" in viewport_domain_source
    assert "const lastPropsRef = useRef(initialSyncState);" in sync_source


def test_transport_emits_the_versioned_runtime_envelope_directly():
    transport_source = _source("hooks/useRuntimeTransport.js")

    assert "const runtimeRequest = normalizeOutgoingRuntimeRequest({ kind, payload });" in transport_source
    assert "runtimeRequest: { kind, payload }" not in transport_source


def test_cell_selection_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    selection_source = _source("runtime/useRuntimeCellSelection.js")

    assert "useRuntimeCellSelection({" in viewport_source
    assert "useRuntimeCellSelection({" not in setup_source
    assert "useRuntimeCellSelection(env" not in setup_source
    assert "useRuntimeCellSelection(env" not in viewport_source
    assert "runtime.handleKeyDown =" not in viewport_source
    assert "runtime.selectRange =" not in setup_source
    assert "runtime.handleCellMouseDown =" not in setup_source
    assert "runtime.handleFillMouseUp =" not in setup_source
    assert "export function useRuntimeCellSelection({" in selection_source
    assert "handleKeyDown" in selection_source
    assert "handleFillMouseUp" in selection_source


def test_clipboard_paste_waits_for_the_interaction_and_viewport_dependencies():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")

    assert "useRuntimeClipboardPaste({" not in setup_source
    assert "useRuntimeClipboardPaste({" in viewport_source


def test_column_auto_sizing_has_an_explicit_viewport_owner():
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    sizing_source = _source("runtime/useRuntimeColumnAutoSizing.js")

    assert "useRuntimeColumnAutoSizing({" in viewport_source
    assert "useRuntimeColumnAutoSizing(env" not in viewport_source
    assert "runtime.autoSizeColumn =" not in viewport_source
    assert "export function useRuntimeColumnAutoSizing({" in sizing_source
    assert "handleAutoSizeToolbarClick" in sizing_source


def test_filter_popover_has_an_explicit_viewport_owner_and_v1_request():
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    filters_source = _source("runtime/useRuntimeFilterPopover.js")

    assert "useRuntimeFilterPopover({" in viewport_source
    assert "useRuntimeFilterPopover(env" not in viewport_source
    assert "runtime.requestFilterOptions =" not in viewport_source
    assert "export function useRuntimeFilterPopover({" in filters_source
    assert "createRuntimeRequest({" in filters_source


def test_color_scale_presentation_has_an_explicit_viewport_owner():
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    color_scale_source = _source("runtime/useRuntimeColorScalePresentation.js")

    assert "useRuntimeColorScalePresentation({" in viewport_source
    assert "useRuntimeColorScalePresentation(env" not in viewport_source
    assert "runtime.getConditionalStyle =" not in viewport_source
    assert "export function useRuntimeColorScalePresentation({" in color_scale_source
    assert "getConditionalStyle" in color_scale_source


def test_viewport_table_data_has_an_explicit_runtime_owner():
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    table_data_source = _source("runtime/useRuntimeViewportTableData.js")

    assert "useRuntimeViewportTableData({" in viewport_source
    assert "useRuntimeViewportTableData(env" not in viewport_source
    assert "runtime.rawTableData =" not in viewport_source
    assert "export function useRuntimeViewportTableData({" in table_data_source
    assert "getStableDataRowId" in table_data_source


def test_view_snapshot_is_owned_by_the_viewport_that_supplies_scroll_state():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    snapshot_source = _source("runtime/useRuntimeViewSnapshot.js")

    assert "useRuntimeViewSnapshot({" not in interactions_source
    assert "runtime.buildCurrentViewState =" not in interactions_source
    assert "useRuntimeViewSnapshot({" in viewport_source
    assert "useRuntimeViewSnapshot(env" not in viewport_source
    assert "parentRef: stage.parentRef" in viewport_source
    assert "export function useRuntimeViewSnapshot({" in snapshot_source
    assert "savedView: snapshot" in snapshot_source


def test_display_navigation_and_view_restore_are_viewport_owned():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    navigation_source = _source("runtime/useRuntimeDisplayNavigation.js")
    restore_source = _source("runtime/useRuntimeViewRestoreScroll.js")

    assert "runtime.scrollToDisplayRow =" not in setup_source
    assert "runtime.scrollToDisplayColumn =" not in setup_source
    assert "savedScroll && runtime.parentRef.current" not in setup_source
    assert "useRuntimeDisplayNavigation({" in viewport_source
    assert "useRuntimeDisplayNavigation(env" not in viewport_source
    assert "useRuntimeViewRestoreScroll({" in viewport_source
    assert "parentRef: stage.parentRef" in viewport_source
    assert "export function useRuntimeDisplayNavigation({" in navigation_source
    assert "export function useRuntimeViewRestoreScroll({" in restore_source


def test_horizontal_viewport_projection_is_not_owned_by_workspace():
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    workspace_source = _source("runtime/useWorkspaceRuntimeStage.tsx")

    assert "useColumnVirtualizer({" in viewport_source
    assert "stage.leftHeaderGroups = useMemo" in viewport_source
    assert "useStickyStyles(stage.theme, stage.leftCols, stage.rightCols)" in viewport_source
    assert "useColumnVirtualizer({" not in workspace_source
    assert "useStickyStyles(runtime.theme, runtime.leftCols, runtime.rightCols)" not in workspace_source
    assert "columnVirtualizer: runtime.columnVirtualizer" not in workspace_source


def test_chart_canvas_commands_have_an_explicit_runtime_owner():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    commands_source = _source("runtime/useRuntimeChartCanvasCommands.js")

    assert "useRuntimeChartCanvasCommands({" in interactions_source
    assert "useRuntimeChartCanvasCommands(env" not in interactions_source
    assert "export function useRuntimeChartCanvasCommands({" in commands_source
    assert "handleAddChartCanvasPane" in commands_source
    assert "handleRemoveChartCanvasPane" in commands_source


def test_chart_panel_sizing_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    sizing_source = _source("runtime/useRuntimeChartPanelSizing.js")

    assert "useRuntimeChartPanelSizing({" in setup_source
    assert "useRuntimeChartPanelSizing(env" not in setup_source
    assert "export function useRuntimeChartPanelSizing({" in sizing_source
    assert "clampChartPanelWidth" in sizing_source


def test_chart_canvas_horizontal_resize_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    resize_source = _source("runtime/useRuntimeChartCanvasResize.js")

    assert "useRuntimeChartCanvasResize({" in setup_source
    assert "useRuntimeChartCanvasResize(env" not in setup_source
    assert "export function useRuntimeChartCanvasResize({" in resize_source
    assert "handleStartChartCanvasResize" in resize_source


def test_chart_canvas_vertical_resize_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    resize_source = _source("runtime/useRuntimeChartCanvasVerticalResize.js")

    assert "useRuntimeChartCanvasVerticalResize({" in setup_source
    assert "useRuntimeChartCanvasVerticalResize(env" not in setup_source
    assert "export function useRuntimeChartCanvasVerticalResize({" in resize_source
    assert "handleStartChartCanvasVerticalResize" in resize_source


def test_chart_definition_commands_have_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    commands_source = _source("runtime/useRuntimeChartDefinitionCommands.js")

    assert "useRuntimeChartDefinitionCommands({" in setup_source
    assert "useRuntimeChartDefinitionCommands(env" not in setup_source
    assert "export function useRuntimeChartDefinitionCommands({" in commands_source
    assert "handleDuplicateChartDefinition" in commands_source


def test_chart_floating_interactions_have_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    interactions_source = _source("runtime/useRuntimeChartFloatingInteractions.js")

    assert "useRuntimeChartFloatingInteractions({" in setup_source
    assert "useRuntimeChartFloatingInteractions(env" not in setup_source
    assert "export function useRuntimeChartFloatingInteractions({" in interactions_source
    assert "handleToggleChartPanelFloating" in interactions_source
    assert "handleToggleChartCanvasPaneFloating" in interactions_source


def test_edit_state_and_optimistic_tracking_have_explicit_runtime_owners():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    persistence_source = _source("runtime/useRuntimeEditStatePersistence.js")
    tracking_source = _source("runtime/useRuntimeEditChangeTracking.js")

    assert "useRuntimeEditStatePersistence({" in interactions_source
    assert "useRuntimeEditChangeTracking({" in interactions_source
    assert "useRuntimeEditStatePersistence(env" not in interactions_source
    assert "useRuntimeEditChangeTracking(env" not in interactions_source
    assert "export function useRuntimeEditStatePersistence({" in persistence_source
    assert "export function useRuntimeEditChangeTracking({" in tracking_source
    assert "buildEditedCellMarkerPlan" in tracking_source
    assert "reconcileOptimisticCellValuesWithPayload" in tracking_source
    assert "didRestorePersistedEditComparisonRef" in persistence_source
    assert "lastRestoredViewEditComparisonKeyRef" in persistence_source
    assert "didRestorePersistedEditComparisonRef = useRef" not in interactions_source


def test_edit_dispatch_has_a_dedicated_runtime_owner():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    dispatch_source = _source("runtime/useRuntimeEditDispatch.js")

    assert "useRuntimeEditDispatch({" in interactions_source
    assert "useRuntimeEditDispatch(env" not in interactions_source
    assert "export function useRuntimeEditDispatch({" in dispatch_source
    assert "dispatchServerEditUpdates" in dispatch_source
    assert "resolvePropagationFormulaUpdates" in dispatch_source


def test_row_edit_session_commands_have_an_explicit_runtime_owner():
    interactions_source = _source("runtime/useCommandRuntimeStage.tsx")
    row_editing_source = _source("runtime/useRuntimeRowEditing.js")

    assert "useRuntimeRowEditing({" in interactions_source
    assert "useRuntimeRowEditing(env" not in interactions_source
    assert "export function useRuntimeRowEditing({" in row_editing_source
    assert "saveRowEditSession" in row_editing_source
    assert "renderRowEditActions" in row_editing_source


def test_runtime_request_session_state_is_separate_from_setup():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    session_source = _source("runtime/useRuntimeRequestSession.js")

    assert "useRuntimeRequestSession({" in setup_source
    assert "useRuntimeRequestSession(env" not in setup_source
    assert "export function useRuntimeRequestSession({" in session_source
    assert "latestDataVersionRef" in session_source
    assert "sessionIdRef" in session_source
    assert "optimisticCellValuesRef" in session_source


def test_edit_cell_identity_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    identity_source = _source("runtime/useRuntimeEditCellIdentity.js")

    assert "useRuntimeEditCellIdentity({" in setup_source
    assert "useRuntimeEditCellIdentity(env" not in setup_source
    assert "export function useRuntimeEditCellIdentity({" in identity_source
    assert "resolveDisplayedCellValue" in identity_source
    assert "resolveOptimisticCellValue" in identity_source


def test_edit_comparison_state_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    state_source = _source("runtime/useRuntimeEditComparisonState.js")

    assert "useRuntimeEditComparisonState({" in setup_source
    assert "useRuntimeEditComparisonState(env" not in setup_source
    assert "export function useRuntimeEditComparisonState({" in state_source
    assert "restoreSerializedEditComparisonState" in state_source
    assert "serializedEditComparisonState" in state_source


def test_edit_event_ownership_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    ownership_source = _source("runtime/useRuntimeEditOwnership.js")

    assert "useRuntimeEditOwnership({" in setup_source
    assert "useRuntimeEditOwnership(env" not in setup_source
    assert "export function useRuntimeEditOwnership({" in ownership_source
    assert "resolveCellEditOwnershipState" in ownership_source
    assert "hydrateVisibleEditOverlay" in ownership_source


def test_edit_presentation_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    presentation_source = _source("runtime/useRuntimeEditPresentation.js")

    assert "useRuntimeEditPresentation({" in setup_source
    assert "useRuntimeEditPresentation(env" not in setup_source
    assert "export function useRuntimeEditPresentation({" in presentation_source
    assert "resolveEditorPresentation" in presentation_source
    assert "resolveAggregationConfigForColumnId" in presentation_source


def test_row_edit_sessions_have_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    sessions_source = _source("runtime/useRuntimeRowEditSessions.js")

    assert "useRuntimeRowEditSessions({" in setup_source
    assert "useRuntimeRowEditSessions(env" not in setup_source
    assert "export function useRuntimeRowEditSessions({" in sessions_source
    assert "startRowEditSession" in sessions_source
    assert "updateRowDraftValue" in sessions_source


def test_optimistic_cell_state_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    state_source = _source("runtime/useRuntimeOptimisticCellState.js")

    assert "useRuntimeOptimisticCellState({" in setup_source
    assert "useRuntimeOptimisticCellState(env" not in setup_source
    assert "export function useRuntimeOptimisticCellState({" in state_source
    assert "captureOptimisticCellValues" in state_source
    assert "clearOptimisticCellValuesForRequest" in state_source


def test_row_selection_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    selection_source = _source("runtime/useRuntimeRowSelection.js")

    assert "useRuntimeRowSelection({" in setup_source
    assert "useRuntimeRowSelection(env" not in setup_source
    assert "export function useRuntimeRowSelection({" in selection_source
    assert "handleRowRangeSelect" in selection_source
    assert "handleRowSelect" in selection_source


def test_clipboard_paste_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    viewport_source = _source("runtime/useViewportRuntimeStage.tsx")
    paste_source = _source("runtime/useRuntimeClipboardPaste.js")

    assert "useRuntimeClipboardPaste({" not in setup_source
    assert "useRuntimeClipboardPaste({" in viewport_source
    assert "useRuntimeClipboardPaste(env" not in setup_source
    assert "useRuntimeClipboardPaste(env" not in viewport_source
    assert "export function useRuntimeClipboardPaste({" in paste_source
    assert "dispatchBatchUpdateRequest(updates, 'paste')" in paste_source


def test_cell_presentation_has_an_explicit_runtime_owner():
    setup_source = _source("runtime/useCoreRuntimeStage.tsx")
    presentation_source = _source("runtime/createRuntimeCellPresentation.js")

    assert "createRuntimeCellPresentation({" in setup_source
    assert "createRuntimeCellPresentation(env" not in setup_source
    assert "export function createRuntimeCellPresentation({" in presentation_source
    assert "getRuleBasedStyle" in presentation_source
    assert "validateCell" in presentation_source
