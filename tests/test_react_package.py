"""Public React package boundary checks."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = ROOT / "dash_tanstack_pivot"


def test_react_package_export_has_esm_and_types():
    package = json.loads((PACKAGE_DIR / "package.json").read_text(encoding="utf-8"))
    react_export = package["exports"]["./react"]

    assert react_export == {
        "types": "./dist/react/react.d.ts",
        "import": "./dist/react/index.mjs",
        "default": "./dist/react/index.mjs",
    }
    assert package["exports"]["."] == react_export
    assert package["main"] == "./dist/react/index.mjs"
    assert package["types"] == "./dist/react/react.d.ts"
    assert package["files"] == ["dist/react", "README.md"]
    assert "build:react" in package["scripts"]
    assert "react" in package["peerDependencies"]
    assert "styled-components" in package["peerDependencies"]


def test_streamlit_mounts_the_shared_react_entry_not_a_second_workspace():
    source = (
        PACKAGE_DIR / "streamlit_pivot" / "frontend" / "src" / "index.tsx"
    ).read_text(encoding="utf-8")

    assert "from '@pivot/react'" in source
    assert "PivotWorkspace" in source
    assert "components/PivotWorkspace" not in source


def test_streamlit_state_output_is_opt_in_and_resize_stays_outside_the_rerun_channel():
    workspace_source = (
        PACKAGE_DIR / "src" / "lib" / "components" / "PivotWorkspace.tsx"
    ).read_text(encoding="utf-8")
    streamlit_source = (
        PACKAGE_DIR / "streamlit_pivot" / "frontend" / "src" / "index.tsx"
    ).read_text(encoding="utf-8")

    assert "DURABLE_HOST_STATE_KEYS" in workspace_source
    assert "'chartCanvasPanes'" not in workspace_source.split("const DURABLE_HOST_STATE_KEYS", 1)[1].split("const selectDurableHostState", 1)[0]
    assert "'tableCanvasSize'" not in workspace_source.split("const DURABLE_HOST_STATE_KEYS", 1)[1].split("const selectDurableHostState", 1)[0]
    assert "viewStateEnabled?: boolean" in streamlit_source
    assert "onStateChange: data.viewStateEnabled" in streamlit_source


def test_streamlit_build_emits_a_manifest_for_the_exact_hashed_entry():
    source = (
        PACKAGE_DIR / "streamlit_pivot" / "frontend" / "vite.config.ts"
    ).read_text(encoding="utf-8")
    host_source = (PACKAGE_DIR / "streamlit_pivot" / "__init__.py").read_text(encoding="utf-8")

    assert "manifest: 'pivot-workspace-manifest.json'" in source
    assert 'manifest.get("src/index.tsx")' in host_source


def test_workspace_does_not_reapply_its_own_chart_resize_publication():
    source = (
        PACKAGE_DIR / "src" / "lib" / "runtime" / "useCoreRuntimeStage.tsx"
    ).read_text(encoding="utf-8")

    assert "stage.lastChartDefinitionsPropRef.current = nextJson" in source
    assert "stage.lastChartCanvasPanesPropRef.current = JSON.stringify(nextChartCanvasPanes)" in source
    assert "stage.lastTableCanvasSizePropRef.current = stage.tableCanvasSize" in source


def test_react_workspace_dispatches_the_runtime_owned_initial_request_once():
    source = (
        PACKAGE_DIR / "src" / "lib" / "components" / "PivotWorkspace.tsx"
    ).read_text(encoding="utf-8")

    assert "if (nextProps.runtimeRequest) dispatch(nextProps.runtimeRequest);" in source
    assert "runtimeResponse: response" in source
    assert "bootstrap:" not in source
    assert "bootstrapRef" not in source


def test_http_transport_binds_the_browser_fetch_receiver_for_embedded_hosts():
    source = (
        PACKAGE_DIR / "src" / "lib" / "runtime" / "httpTransport.ts"
    ).read_text(encoding="utf-8")

    assert "globalThis.fetch.bind(globalThis)" in source
    assert "Illegal invocation" in source


def test_http_transport_resolves_runtime_payload_urls_for_cross_origin_hosts():
    source = (
        PACKAGE_DIR / "src" / "lib" / "runtime" / "httpTransport.ts"
    ).read_text(encoding="utf-8")

    assert "private resolvePayloadUrl(url: string): string" in source
    assert "new URL(url, endpointUrl).toString()" in source
    assert "return this.resolvePayloadReference(body);" in source
    assert "this.fetchImpl(this.resolvePayloadUrl(reference.url)" in source


def test_runtime_export_fetches_payload_before_reporting_download_success():
    source = (ROOT / "dash_tanstack_pivot" / "src" / "lib" / "runtime" / "useRuntimeResponseLifecycle.js").read_text(encoding="utf-8")

    assert "const downloadPayloadReference = () =>" in source
    assert "return response.blob();" in source
    assert "downloadPayloadReference().then(() =>" in source
    assert "showNotification(`Downloaded ${payload.rows ?? ''} rows`, 'success');" in source


def test_runtime_response_lifecycle_receives_delivery_callbacks_from_viewport_domain():
    domains = (PACKAGE_DIR / "src" / "lib" / "runtime" / "runtimeDomains.ts").read_text(encoding="utf-8")
    viewport = (PACKAGE_DIR / "src" / "lib" / "runtime" / "useViewportRuntime.ts").read_text(encoding="utf-8")
    transport = (PACKAGE_DIR / "src" / "lib" / "runtime" / "useTransportRuntime.ts").read_text(encoding="utf-8")
    root = (PACKAGE_DIR / "src" / "lib" / "runtime" / "PivotRuntimeRoot.tsx").read_text(encoding="utf-8")

    assert "export interface ViewportDeliveryPort" in domains
    assert "readonly delivery: ViewportDeliveryPort;" in domains
    assert "copy: command(composed.copyRichToClipboard)" in viewport
    assert "download: command(composed.triggerExportDownload)" in viewport
    assert "viewport: ViewportRuntimeDomain" in transport
    assert "copyRichToClipboard: viewport.delivery.copy" in transport
    assert "triggerExportDownload: viewport.delivery.download" in transport
    assert "useTransportResponseRuntime(core, transport, viewport);" in root


def test_chart_preview_uses_consistent_longhand_flex_properties():
    chart_panel = (
        PACKAGE_DIR / "src" / "lib" / "components" / "Charts" / "PivotCharts.js"
    ).read_text(encoding="utf-8")
    chart_workspace = (
        PACKAGE_DIR / "src" / "lib" / "components" / "Charts" / "ChartWorkspace.js"
    ).read_text(encoding="utf-8")

    panel_container = chart_panel.split("const panelContainerStyle", 1)[1].split("useEffect(() =>", 1)[0]
    assert "flex: '1 1 auto'" not in panel_container
    assert "flexGrow: 1" in panel_container
    assert "flexBasis: 'auto'" in panel_container
    assert "flex: `0 0 ${hintedGroupWidth}px`" not in chart_workspace
    assert "flexBasis: `${hintedGroupWidth}px`" in chart_workspace


def test_clipboard_commands_distinguish_current_table_from_all_records():
    commands = (
        PACKAGE_DIR / "src" / "lib" / "runtime" / "createRuntimeExportCommands.js"
    ).read_text(encoding="utf-8")
    viewport = (
        PACKAGE_DIR / "src" / "lib" / "runtime" / "useViewportRuntimeStage.tsx"
    ).read_text(encoding="utf-8")
    selection = (
        PACKAGE_DIR / "src" / "lib" / "runtime" / "useRuntimeCellSelection.js"
    ).read_text(encoding="utf-8")

    assert "const copyCurrentTableToClipboard" in commands
    assert "copyServerTableToClipboard({ allRows: false" in commands
    assert "const copyAllRecordsToClipboard" in commands
    assert "copyServerTableToClipboard({ allRows: true" in commands
    assert "getServerExportColumnTarget({ currentTable: !allRows })" in commands
    assert "label: 'Copy Current Table'" in viewport
    assert "label: 'Copy All Records'" in viewport
    assert "Copy Table'" not in viewport
    assert "copyCurrentTableToClipboard: stage.copyCurrentTableToClipboard" in viewport
    assert "copyCurrentTableToClipboard" in selection
    assert selection.count("copyCurrentTableToClipboard(true, false)") == 2
    assert "copyCurrentTableToClipboard(false, false)" not in selection
