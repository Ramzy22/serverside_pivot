
import pytest
import asyncio
import pyarrow as pa
import sys
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Add root and pivot_engine source to path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'pivot_engine'))
sys.path.append(os.path.join(os.getcwd(), 'dash_tanstack_pivot'))

from pivot_engine.tanstack_adapter import create_tanstack_adapter, TanStackRequest, TanStackOperation
from pivot_engine.types.pivot_spec import PivotSpec, Measure, PivotConfig

# Reuse the data generation logic from app.py
def create_test_data(adapter, rows=1000):
    dates = [f"2023-{m:02d}-{d:02d}" for m in range(1, 13) for d in range(1, 28, 5)]
    data_source = {
        "region": (["North", "South", "East", "West"] * (rows // 4)) + ["North"] * (rows % 4),
        "country": (["USA", "Canada", "Brazil", "UK", "China", "Japan", "Germany", "France"] * (rows // 8)) + ["USA"] * (rows % 8),
        "product": (["Laptop", "Phone", "Tablet", "Monitor", "Headphones"] * (rows // 5)) + ["Laptop"] * (rows % 5),
        "sales": [x % 1000 for x in range(rows)],
        "cost": [x % 800 for x in range(rows)],
        "date": (dates * (rows // len(dates)) + dates[:rows % len(dates)])
    }
    # Ensure all lists are same length
    min_len = min(len(v) for v in data_source.values())
    for k in data_source:
        data_source[k] = data_source[k][:min_len]

    table = pa.Table.from_pydict(data_source)
    adapter.controller.load_data_from_arrow("sales_data", table)

@pytest.fixture
def adapter():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    create_test_data(adapter)
    return adapter


def test_dash_app_import_and_layout_valid():
    from dash_presentation.app import app

    assert app is not None
    assert app.layout is not None


def test_dash_app_registers_local_component_bundle():
    from dash.fingerprint import build_fingerprint
    import dash_tanstack_pivot as component_pkg
    from dash_presentation.app import app

    resources = app._collect_and_register_resources(app.scripts.get_all_scripts())
    component_scripts = [
        resource for resource in resources
        if "dash_tanstack_pivot" in resource and resource.endswith(".min.js")
    ]

    bundle_rel_path = "dash_tanstack_pivot/dash_tanstack_pivot.min.js"
    bundle_path = Path(component_pkg.__file__).resolve().parent / bundle_rel_path
    expected = (
        f"{app.config.requests_pathname_prefix}"
        f"_dash-component-suites/dash_tanstack_pivot/"
        f"{build_fingerprint(bundle_rel_path, component_pkg.__version__, int(bundle_path.stat().st_mtime))}"
    )

    assert Path(component_pkg.__file__).resolve().is_relative_to(
        (Path(os.getcwd()) / "dash_tanstack_pivot").resolve()
    )
    assert component_scripts == [expected]


def test_dash_presentation_app_includes_single_pivot_sparkline_modes_demo():
    app_source = Path(
        os.path.join(os.getcwd(), "dash_presentation", "app.py")
    ).read_text(encoding="utf-8")

    assert 'id="sparkline-modes-pivot-grid"' in app_source
    assert '"Linked Sparkline Columns"' in app_source
    assert '"Values Plus Trend"' in app_source
    assert "app.validation_layout = html.Div" in app_source
    assert "mount_field_sparkline_demo(None)" in app_source
    assert '"sparkline_demo_data"' in app_source
    assert '"type": "line"' in app_source
    assert '"type": "area"' in app_source
    assert '"type": "column"' in app_source
    assert '"hideColumns": False' in app_source
    assert '"nasdaq_trader_demo_pnl_series"' in app_source
    assert '"pnl_20d"' in app_source
    assert '"price_20d"' in app_source
    assert '"day_pnl_20d"' in app_source
    assert '"volume_20d"' in app_source
    assert '"market_value_20d"' in app_source
    assert "_TRADER_SERIES_POINT_FIELDS" in app_source
    assert '"source": "field"' in app_source
    assert '"displayMode": "trend"' in app_source
    assert '"displayMode": "value"' in app_source
    assert '"overflowY": "auto"' in app_source
    assert '"formulaScope": "columns"' in app_source
    assert '"formula": "[Cash Equity_day_pnl_sum] - [ETF_day_pnl_sum]"' in app_source


def test_export_copy_uses_rich_html_clipboard_and_styled_xls_runtime_export():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    export_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "exportUtils.js",
        )
    ).read_text(encoding="utf-8")

    assert "buildStyledHtmlTableExport" in component_source
    assert "writeClipboardPayload" in component_source
    assert "htmlTableToTsv" in component_source
    assert "format: 'html'" in component_source
    assert "format: 'xls'" in component_source
    assert "style: buildExportStyleProfile" in component_source
    assert "ClipboardItem" in export_source
    assert "'text/html'" in export_source
    assert "application/vnd.ms-excel" in export_source
    assert "downloadHtmlTableAsExcel" in export_source


def test_displayed_column_formula_scope_is_exposed_in_sidebar_and_component():
    sidebar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "SidebarPanel.js",
        )
    ).read_text(encoding="utf-8")
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")

    assert "const COLUMN_FORMULA_REFERENCE_RE" in sidebar_source
    assert "formulaScope: normalizeFormulaScope(item.formulaScope)" in sidebar_source
    assert '<option value="columns">Displayed columns</option>' in sidebar_source
    assert "formatColumnFormulaReference(column.id)" in sidebar_source
    assert "displayedColumnOptions={displayedFormulaColumnOptions}" in component_source
    assert "formulaScope: PropTypes.oneOf(['measures', 'columns'])" in component_source


def test_value_formula_editor_brackets_field_names_that_are_not_identifiers():
    sidebar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "SidebarPanel.js",
        )
    ).read_text(encoding="utf-8")

    assert "function formatValueFormulaReference" in sidebar_source
    assert "SIMPLE_FORMULA_REFERENCE_RE.test(token) ? token : `[${token}]`" in sidebar_source
    assert "function extractValueFormulaReferences" in sidebar_source
    assert "knownReferenceList" in sidebar_source
    assert "extractValueFormulaReferences(trimmed, knownReferences)" in sidebar_source
    assert "insertAtCursor(formatValueFormulaReference(config.field))" in sidebar_source
    assert "insertAtCursor(formatValueFormulaReference(f))" in sidebar_source


def test_report_editor_lists_outline_and_uses_row_override_language():
    report_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "ReportEditor.js",
        )
    ).read_text(encoding="utf-8")

    assert "Report Outline" in report_source
    assert "data-report-section={sectionId || title}" in report_source
    assert 'sectionId="report-levels"' in report_source
    assert 'sectionId="report-outline"' in report_source
    assert 'sectionId="selected-level"' in report_source
    assert "borderTop: `2px solid ${theme.textSec || '#334155'}`" in report_source
    assert "linear-gradient" not in report_source
    assert "borderLeft" not in report_source
    assert "Default levels plus every row override" in report_source
    assert "OutlineNodeRow" in report_source
    assert "OutlineBranchRow" in report_source
    assert "selectedItem.type === 'branch'" in report_source
    assert "Row override ${item.badge}" in report_source
    assert "Edit row override rule" in report_source
    assert "Custom children" in report_source
    assert "Default children" in report_source
    assert "Override applies to this row only" in report_source
    assert "Open Custom children" in report_source
    assert "Add Custom children" in report_source
    assert "Edit spl" + "it rule" not in report_source
    assert "Custom " + "path" not in report_source
    assert "Default " + "path" not in report_source


def test_report_mode_supports_formatting_validation_debug_and_collapsed_levels():
    report_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "ReportEditor.js",
        )
    ).read_text(encoding="utf-8")
    column_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnDefs.js",
        )
    ).read_text(encoding="utf-8")
    render_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useRenderHelpers.js",
        )
    ).read_text(encoding="utf-8")
    normalization_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "usePivotNormalization.js",
        )
    ).read_text(encoding="utf-8")
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")

    assert "DEFAULT_REPORT_FORMAT" in report_source
    assert "Manual Report Rows" not in report_source
    assert "Add rows like Strategic Accounts" not in report_source
    assert "Number format override" in report_source
    assert "Show subtotal/other rows" in report_source
    assert "data-report-validation=\"true\"" in report_source
    assert "validateReportDefinition" in report_source
    assert "Circular row override detected" in report_source
    assert "Top-N at" in report_source
    assert "does not match any loaded rows" in report_source
    assert "sourceRowPath" in report_source
    assert "Why Is This Row Here?" in report_source
    assert "function CollapsibleLevelSection" in report_source
    assert 'data-report-level-collapsible="true"' in report_source
    assert "data-report-level-collapsed={open ? 'false' : 'true'}" in report_source
    assert "defaultOpen={false}" in report_source
    assert "Hide row gears" in report_source
    assert "Show row gears" in report_source
    assert "setShowReportConfigColumn((current) => (current === false ? true : false))" in report_source

    assert "_reportFormat" in column_source
    assert "reportFormat.numberFormat" in column_source
    assert "_reportDisplayLabel" in column_source
    assert "const HIERARCHY_INDENT_PX = 32;" in column_source
    assert "depth * HIERARCHY_INDENT_PX" in column_source
    assert "(depth * HIERARCHY_INDENT_PX) + (Number.isFinite(indent)" in column_source
    assert "getReportIndentPx" in column_source
    assert "id.startsWith('_report')" in column_source
    assert "showReportConfigColumn !== false && typeof onConfigureReportLine" in column_source
    assert "reportFormat.borderStyle" in render_source
    assert "reportFormat.rowColor" in render_source
    assert "reportFormat && reportFormat.bold" in render_source
    assert "delete normalized.manualRows" in normalization_source
    assert "debug: rowData._reportDebug || null" in component_source
    assert "reportDef: Object.prototype.hasOwnProperty.call(snapshot, 'reportDef')" in component_source
    assert "showReportConfigColumn: showReportConfigColumn !== false && !immersiveMode" in component_source
    assert (
        "const shouldShowReportConfigGutter = pivotMode === 'report' && "
        "showReportConfigColumn !== false && !immersiveMode"
    ) in component_source


def test_sidebar_field_panels_allow_compact_minimum_sizes():
    field_layout_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "fieldPanelLayout.js",
        )
    ).read_text(encoding="utf-8")
    sidebar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "SidebarPanel.js",
        )
    ).read_text(encoding="utf-8")

    assert "minWidth: 120" in field_layout_source
    assert "minHeight: 44" in field_layout_source
    assert "minHeight: 56" in field_layout_source
    assert "minHeight: 40" in field_layout_source
    assert "minHeight: '24px'" in sidebar_source
    assert "padding: '4px 8px'" in sidebar_source


def test_chart_panel_source_grows_for_settings_instead_of_splitting_canvas():
    chart_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Charts",
            "PivotCharts.js",
        )
    ).read_text(encoding="utf-8")
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    normalization_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "usePivotNormalization.js",
        )
    ).read_text(encoding="utf-8")

    assert "settingsPanelWidth: controlledSettingsPanelWidth" in chart_source
    assert "onSettingsPanelWidthChange" in chart_source
    assert "const visibleSettingsWidth = (!immersiveMode && settingsPaneOpen) ? (settingsPanelWidth + 9) : 0;" in chart_source
    assert "onSettingsWidthBudgetChange" in chart_source
    assert "const [settingsWidthBudget, setSettingsWidthBudget] = useState(null);" in chart_source
    assert "onSettingsWidthBudgetChange={setSettingsWidthBudget}" in chart_source
    assert "floatingInteractionLocked" in chart_source
    assert "Dock chart pane to the top" in chart_source
    assert "Dock chart pane to the bottom" in chart_source
    assert "data-chart-modal-position={position}" in chart_source
    assert "const [chartCanvasPaneWidthHints, setChartCanvasPaneWidthHints] = useState({});" in component_source
    assert "const paneWidthHints = panes.map((pane) => {" in component_source
    assert "const hintedGroupWidth = hasPaneWidthHint" in component_source
    assert "flex: `0 0 ${hintedGroupWidth}px`" in component_source
    assert "width: widthHint === null ? undefined : `${widthHint}px`" in component_source
    assert "width: widthHint === null ? undefined : '100%'" not in component_source
    assert "VALID_CHART_DOCK_POSITIONS = new Set(['left', 'right', 'top', 'bottom'])" in normalization_source
    assert "normalizeChartDockPosition" in component_source
    assert "dockPosition: normalizeChartDockPosition(source.dockPosition || source.dock_position, 'right')" in normalization_source
    assert "data-docked-chart-pane-position={normalizedDockPosition}" in component_source
    assert "renderHorizontalDockGroup(dockedChartCanvasPanesByPosition.left, 'left')" in component_source
    assert "renderVerticalDockGroup(dockedChartCanvasPanesByPosition.top, 'top')" in component_source
    assert "chartModal && chartModalPosition === 'top'" in component_source
    assert "chartModal && chartModalPosition === 'bottom'" in component_source
    assert "handleChartCanvasPaneWidthHintChange" in component_source
    assert "normalizedDockPosition === 'left' || normalizedDockPosition === 'right'" in component_source


def test_chart_panel_settings_budget_survives_parent_rerenders():
    chart_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Charts",
            "PivotCharts.js",
        )
    ).read_text(encoding="utf-8")

    assert "const onSettingsWidthBudgetChangeRef = useRef(onSettingsWidthBudgetChange);" in chart_source
    assert "const lastSettingsWidthBudgetRef = useRef(null);" in chart_source
    assert "onSettingsWidthBudgetChangeRef.current = onSettingsWidthBudgetChange;" in chart_source
    assert "emitSettingsWidthBudget(baseWidthHint + visibleSettingsWidth);" in chart_source
    assert "onSettingsWidthBudgetChange(null);" not in chart_source
    assert "onMouseDown={(event) => event.stopPropagation()}" in chart_source
    assert "setConfigOpen((currentOpen) => !currentOpen);" in chart_source


def test_dash_app_get_adapter_initializes_once_under_concurrency(monkeypatch):
    import dash_presentation.app as app_module

    created = []
    loaded = []

    class StubAdapter:
        pass

    def fake_create_tanstack_adapter(**_kwargs):
        adapter = StubAdapter()
        created.append(adapter)
        return adapter

    def fake_load_initial_data(adapter):
        loaded.append(adapter)
        time.sleep(0.02)

    monkeypatch.setattr(app_module, "_adapter", None)
    monkeypatch.setattr(app_module, "create_tanstack_adapter", fake_create_tanstack_adapter)
    monkeypatch.setattr(app_module, "load_initial_data", fake_load_initial_data)

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(lambda _: app_module.get_adapter(), range(6)))

    assert len(created) == 1
    assert len(loaded) == 1
    assert all(result is created[0] for result in results)

@pytest.mark.asyncio
async def test_initial_load_hierarchy(adapter):
    """Verify initial load with hierarchy returns correct structure"""
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[{"id": "region"}, {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"}],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[]
    )
    
    # Simulate initial load where expanded is False (or empty dict)
    response = await adapter.handle_hierarchical_request(request, [])
    
    assert response.total_rows > 0
    assert len(response.data) > 0
    
    # Check structure of the first row (Grand Total) or first group
    first_row = response.data[0]
    
    # Based on adapter logic, first row should be Grand Total if not filtered
    if first_row.get('_isTotal'):
        assert first_row['_id'] == 'Grand Total'
        assert first_row['depth'] == 0
    else:
        # If no grand total, it should be a region
        assert '_id' in first_row
        assert 'depth' in first_row
        assert first_row['depth'] == 0
    
    # Verify we have aggregated data
    assert 'sales_sum' in response.columns[1]['id']

@pytest.mark.asyncio
async def test_expansion_logic(adapter):
    """Verify expansion returns children"""
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[{"id": "region"}, {"id": "country"}, {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"}],
        filters={},
        sorting=[],
        grouping=["region", "country"],
        aggregations=[]
    )
    
    # Expand "North"
    # The path key logic in adapter is "val" or "val|||val2"
    # We need to find the correct key for North.
    # In the simulated data, "North" is a region.
    
    expanded_paths = [['North']] 
    
    response = await adapter.handle_hierarchical_request(request, expanded_paths)
    
    # We expect to see "North" (depth 0) and its children (depth 1)
    found_north = False
    found_child = False
    
    for row in response.data:
        if row.get('region') == 'North':
            if row['depth'] == 0:
                found_north = True
            elif row['depth'] == 1:
                found_child = True
                assert row['country'] is not None
    
    assert found_north, "Should have returned the parent node 'North'"
    assert found_child, "Should have returned children of 'North'"


@pytest.mark.asyncio
async def test_first_expansion_reuses_collapsed_root_cache(adapter):
    spec = PivotSpec(
        table="sales_data",
        rows=["region", "country"],
        measures=[
            Measure(field="sales", agg="sum", alias="sales_sum"),
            Measure(field="cost", agg="sum", alias="cost_sum"),
        ],
    )

    collapsed = await adapter.controller.run_hierarchy_view(
        spec,
        [],
        start_row=0,
        end_row=20,
        include_grand_total_row=False,
        profiling=True,
    )
    expanded = await adapter.controller.run_hierarchy_view(
        spec,
        [["North"]],
        start_row=0,
        end_row=50,
        include_grand_total_row=False,
        profiling=True,
    )

    assert collapsed["profile"]["controller"]["path"] == "paged_collapsed_root"
    assert expanded["profile"]["controller"]["path"] == "materialized_hierarchy"
    assert expanded["profile"]["controller"]["reusedCache"] is True
    assert any(isinstance(row, dict) and row.get("country") is not None for row in expanded["rows"])


@pytest.mark.asyncio
async def test_wide_materialized_window_uses_sparse_materialized_pivot_path(adapter):
    row_count = 40
    bucket_count = 60
    table = pa.Table.from_pydict(
        {
            "row_id": [row_idx for row_idx in range(row_count) for _ in range(bucket_count)],
            "bucket": [f"B{bucket_idx:03d}" for _ in range(row_count) for bucket_idx in range(bucket_count)],
            "sales": [
                (row_idx * 1000) + bucket_idx
                for row_idx in range(row_count)
                for bucket_idx in range(bucket_count)
            ],
        }
    )
    adapter.controller.load_data_from_arrow("wide_sparse_bench", table)

    spec = PivotSpec(
        table="wide_sparse_bench",
        rows=["row_id"],
        columns=["bucket"],
        measures=[Measure(field="sales", agg="sum", alias="sales_sum")],
        filters=[{"field": "row_id", "op": "in", "value": list(range(10))}],
        sort=[{"field": "row_id", "order": "asc"}],
        limit=0,
        offset=0,
        pivot_config=PivotConfig(
            enabled=True,
            materialized_column_values=[f"B{bucket_idx:03d}" for bucket_idx in range(bucket_count)],
        ),
    )

    profile = {}
    result = await adapter.controller.run_pivot_async(spec, return_format="arrow", force_refresh=True, profile_sink=profile)

    assert profile["planner"]["path"] == "sparse_materialized"
    assert profile["plannerExecution"]["mode"] == "sparse_materialized"
    assert result.num_rows == 10
    first_row = result.slice(0, 1).to_pylist()[0]
    assert first_row["row_id"] == 0
    assert first_row["B000_sales_sum"] == 0
    assert first_row["B059_sales_sum"] == 59

@pytest.mark.asyncio
async def test_filtering(adapter):
    """Verify filtering works"""
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[{"id": "region"}, {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"}],
        filters={"region": {"operator": "AND", "conditions": [{"type": "eq", "value": "North"}]}},
        sorting=[],
        grouping=["region"],
        aggregations=[]
    )
    
    response = await adapter.handle_request(request)
    
    # Should only contain North
    regions = {r['region'] for r in response.data if not r.get('_isTotal')}
    assert "North" in regions
    assert "South" not in regions

@pytest.mark.asyncio
async def test_sorting(adapter):
    """Verify sorting works"""
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[{"id": "region"}, {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"}],
        filters={},
        sorting=[{"id": "sales_sum", "desc": True}],
        grouping=["region"],
        aggregations=[]
    )
    
    response = await adapter.handle_request(request)
    data = [r for r in response.data if not r.get('_isTotal')]
    
    if len(data) > 1:
        # Check if sorted descending
        vals = [r['sales_sum'] for r in data if r['sales_sum'] is not None]
        assert vals == sorted(vals, reverse=True), "Data should be sorted by sales desc"


@pytest.mark.asyncio
async def test_dynamic_columns_refresh_after_data_change(adapter):
    """Repeated requests should refresh discovered dynamic columns after the table changes."""
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    first_response = await adapter.handle_request(request)
    first_dynamic_columns = {
        column["id"]
        for column in first_response.columns
        if column["id"] not in {"region", "product"}
    }
    assert "Laptop_sales_sum" in first_dynamic_columns
    assert "Phone_sales_sum" in first_dynamic_columns

    updated_table = pa.Table.from_pydict(
        {
            "region": ["North", "South"],
            "country": ["USA", "Brazil"],
            "product": ["Laptop", "Monitor"],
            "sales": [10, 30],
            "cost": [1, 3],
            "date": ["2023-01-01", "2023-01-02"],
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", updated_table)

    second_response = await adapter.handle_request(request)
    second_dynamic_columns = {
        column["id"]
        for column in second_response.columns
        if column["id"] not in {"region", "product"}
    }

    assert second_dynamic_columns == {"Laptop_sales_sum", "Monitor_sales_sum"}


@pytest.mark.asyncio
async def test_virtual_scroll_preserves_sort_and_grand_total(adapter):
    """Virtual-scroll responses should preserve the same sort intent and total-row behavior as hierarchical requests."""
    custom_table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "North", "South", "South", "South"],
            "country": ["USA", "Canada", "USA", "Brazil", "UK", "Brazil"],
            "product": ["Laptop", "Phone", "Tablet", "Laptop", "Phone", "Tablet"],
            "sales": [100, 200, 150, 300, 250, 50],
            "cost": [10, 20, 15, 30, 25, 5],
            "date": ["2023-01-01"] * 6,
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", custom_table)

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={"region": {"operator": "AND", "conditions": [{"type": "eq", "value": "North"}]}},
        sorting=[{"id": "sales_sum", "desc": True}],
        grouping=["region", "country"],
        aggregations=[],
    )

    virtual_response = await adapter.handle_virtual_scroll_request(request, 0, 10, [["North"]])

    virtual_children = [row for row in virtual_response.data if row.get("country") is not None]
    virtual_sales = [row["sales_sum"] for row in virtual_children]

    assert virtual_sales == sorted(virtual_sales, reverse=True)
    assert any(row.get("_isTotal") for row in virtual_response.data), "Virtual scroll should preserve a grand total row"


def load_virtual_scroll_fixture(adapter):
    table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "North", "North", "South", "South", "South", "South"],
            "country": ["USA", "Canada", "Mexico", "USA", "Brazil", "Argentina", "Brazil", "Chile"],
            "product": ["Laptop", "Laptop", "Phone", "Tablet", "Laptop", "Phone", "Tablet", "Monitor"],
            "sales": [100, 80, 60, 40, 200, 150, 120, 110],
            "cost": [10, 8, 6, 4, 20, 15, 12, 11],
            "date": ["2023-01-01"] * 8,
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)


def row_paths(rows):
    return [row["_path"] for row in rows]


def assert_unique_non_blank_paths(rows):
    paths = row_paths(rows)
    assert paths
    assert all(path for path in paths)
    assert len(paths) == len(set(paths))


@pytest.mark.asyncio
async def test_virtual_scroll_matches_initial_hierarchical_visibility(adapter):
    """Initial hierarchy load followed by virtual scroll should preserve visible-row identity and metadata."""
    load_virtual_scroll_fixture(adapter)

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[{"id": "sales_sum", "desc": True}],
        grouping=["region", "country"],
        aggregations=[],
    )

    hierarchical_response = await adapter.handle_hierarchical_request(request, [["North"]])
    virtual_response = await adapter.handle_virtual_scroll_request(request, 0, 4, [["North"]])

    assert_unique_non_blank_paths(hierarchical_response.data)
    assert_unique_non_blank_paths(virtual_response.data)
    assert hierarchical_response.total_rows >= len(hierarchical_response.data)
    assert virtual_response.total_rows == hierarchical_response.total_rows

    visible_prefix = row_paths(hierarchical_response.data)[: len(virtual_response.data)]
    assert row_paths(virtual_response.data) == visible_prefix
    assert any(row.get("_isTotal") for row in hierarchical_response.data)


@pytest.mark.asyncio
async def test_expand_collapse_rerequest_preserves_sibling_ordering(adapter):
    """Expand/collapse cycles should not reorder siblings or duplicate visible rows on re-request."""
    load_virtual_scroll_fixture(adapter)

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[{"id": "sales_sum", "desc": True}],
        grouping=["region", "country"],
        aggregations=[],
    )

    expanded_once = await adapter.handle_hierarchical_request(request, [["North"]])
    collapsed = await adapter.handle_hierarchical_request(request, [])
    expanded_twice = await adapter.handle_hierarchical_request(request, [["North"]])

    for response in (expanded_once, collapsed, expanded_twice):
        assert_unique_non_blank_paths(response.data)

    top_level_once = [row["_path"] for row in expanded_once.data if row.get("depth") == 0 and not row.get("_isTotal")]
    top_level_collapsed = [row["_path"] for row in collapsed.data if row.get("depth") == 0 and not row.get("_isTotal")]
    top_level_twice = [row["_path"] for row in expanded_twice.data if row.get("depth") == 0 and not row.get("_isTotal")]
    assert top_level_once == top_level_collapsed == top_level_twice

    north_children_once = [
        row["_path"]
        for row in expanded_once.data
        if row.get("depth") == 1 and row.get("region") == "North"
    ]
    north_children_twice = [
        row["_path"]
        for row in expanded_twice.data
        if row.get("depth") == 1 and row.get("region") == "North"
    ]
    assert north_children_once == ["North|||USA", "North|||Canada", "North|||Mexico"]
    assert north_children_twice == north_children_once


@pytest.mark.asyncio
async def test_repeated_virtual_scroll_requests_do_not_surface_stale_or_blank_rows(adapter):
    """Repeated virtual-scroll windows should remain stable and free of duplicate placeholder rows."""
    load_virtual_scroll_fixture(adapter)

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[{"id": "sales_sum", "desc": True}],
        grouping=["region", "country"],
        aggregations=[],
    )

    first_window = await adapter.handle_virtual_scroll_request(request, 1, 4, [["North"]])
    second_window = await adapter.handle_virtual_scroll_request(request, 1, 4, [["North"]])

    for response in (first_window, second_window):
        assert_unique_non_blank_paths(response.data)
        assert response.total_rows >= len(response.data)
        assert all(row.get("_id") not in (None, "") for row in response.data)

    assert row_paths(first_window.data) == row_paths(second_window.data)
    assert first_window.total_rows == second_window.total_rows


# ---------------------------------------------------------------------------
# Column-window handshake tests
# ---------------------------------------------------------------------------

def make_wide_table(adapter, center_cols=30):
    """Load a table with many pivot columns to exercise column windowing."""
    n_regions = 4
    regions = (["North", "South", "East", "West"] * (100 // n_regions))[:100]
    products = [f"Product_{i % center_cols}" for i in range(100)]
    sales = [float(i % 500) for i in range(100)]
    table = pa.Table.from_pydict({
        "region": regions,
        "product": products,
        "sales": sales,
    })
    adapter.controller.load_data_from_arrow("wide_data", table)


def make_wide_request():
    return TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="wide_data",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )


@pytest.mark.asyncio
async def test_col_schema_present_on_needs_col_schema(adapter):
    """When needs_col_schema=True the response col_schema is populated and contains
    total_center_cols + a columns list with correct index/id entries."""
    make_wide_table(adapter)
    request = make_wide_request()

    response = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], needs_col_schema=True
    )

    assert response.col_schema is not None, "col_schema must be set when needs_col_schema=True"
    schema = response.col_schema
    assert "total_center_cols" in schema
    assert "columns" in schema
    assert schema["total_center_cols"] > 0
    assert 0 < len(schema["columns"]) <= schema["total_center_cols"]

    for i, entry in enumerate(schema["columns"]):
        assert entry["index"] == i
        assert "id" in entry


@pytest.mark.asyncio
async def test_col_schema_absent_when_not_requested(adapter):
    """When needs_col_schema=False the response col_schema is None (no wasted payload)."""
    make_wide_table(adapter)
    request = make_wide_request()

    response = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], needs_col_schema=False
    )

    assert response.col_schema is None, "col_schema must be None when not requested"


@pytest.mark.asyncio
async def test_col_windowing_slices_to_requested_range(adapter):
    """col_start/col_end must slice data rows to only include center columns in the window."""
    make_wide_table(adapter)
    request = make_wide_request()

    # First get full schema
    full_response = await adapter.handle_virtual_scroll_request(
        request, 0, 50, [], needs_col_schema=True
    )
    schema = full_response.col_schema
    assert schema is not None
    total = schema["total_center_cols"]

    if total < 4:
        pytest.skip("Not enough pivot columns to test windowing")

    col_start = 0
    col_end = min(2, total - 1)  # request columns 0–2

    windowed = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], col_start=col_start, col_end=col_end, needs_col_schema=False
    )

    # Determine which IDs are in the window
    windowed_ids = {entry["id"] for entry in schema["columns"][col_start:col_end + 1]}
    row_meta_keys = {
        "_id", "_path", "_isTotal", "_level", "_expanded",
        "_parentPath", "_has_children", "_is_expanded", "depth", "uuid", "subRows",
        "region",  # pinned / grouping col
    }

    for row in windowed.data:
        for key in row:
            if key in row_meta_keys:
                continue
            assert key in windowed_ids, (
                f"Column '{key}' in row but outside requested window [{col_start},{col_end}]"
            )


@pytest.mark.asyncio
async def test_col_windowing_first_column_only(adapter):
    """Windowing must work when only column index 0 is requested (col_start=0, col_end=0)."""
    make_wide_table(adapter)
    request = make_wide_request()

    full_response = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], needs_col_schema=True
    )
    schema = full_response.col_schema
    if schema is None or schema["total_center_cols"] < 2:
        pytest.skip("Need at least 2 center columns")

    windowed = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], col_start=0, col_end=0, needs_col_schema=False
    )

    first_col_id = schema["columns"][0]["id"]
    second_col_id = schema["columns"][1]["id"]
    row_meta_keys = {"_id", "_path", "_isTotal", "_level", "_expanded",
                     "_parentPath", "_has_children", "_is_expanded", "depth", "uuid", "subRows", "region"}

    for row in windowed.data:
        assert second_col_id not in row, (
            f"Column '{second_col_id}' leaked into window [0,0] response"
        )


# ---------------------------------------------------------------------------
# Expansion anchor tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_expansion_anchor_row_appears_in_response(adapter):
    """Expanding a node that is within the viewport window should return the
    expanded node and its children within the requested row range."""
    load_virtual_scroll_fixture(adapter)
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[{"id": "sales_sum", "desc": True}],
        grouping=["region", "country"],
        aggregations=[],
    )

    # Request a window that covers row 0 onward — expand North
    response = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [["North"]]
    )

    paths = row_paths(response.data)
    assert any("North" in p and "|||" not in p for p in paths), (
        "Parent node 'North' should appear in response"
    )
    assert any("North|||" in p for p in paths), (
        "Children of 'North' should appear in response after expansion"
    )


@pytest.mark.asyncio
async def test_expansion_does_not_duplicate_sibling_rows(adapter):
    """Expanding one region must not cause sibling region rows to appear twice."""
    load_virtual_scroll_fixture(adapter)
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region", "country"],
        aggregations=[],
    )

    response = await adapter.handle_virtual_scroll_request(request, 0, 20, [["North"]])

    paths = row_paths(response.data)
    assert len(paths) == len(set(paths)), "Duplicate rows detected after expansion"


@pytest.mark.asyncio
async def test_expansion_total_rows_increases_after_expand(adapter):
    """total_rows must be larger after expanding a node than before."""
    load_virtual_scroll_fixture(adapter)
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region", "country"],
        aggregations=[],
    )

    collapsed = await adapter.handle_virtual_scroll_request(request, 0, 10, [])
    expanded = await adapter.handle_virtual_scroll_request(request, 0, 10, [["North"]])

    assert expanded.total_rows > collapsed.total_rows, (
        f"total_rows should increase after expansion: {collapsed.total_rows} → {expanded.total_rows}"
    )

@pytest.mark.asyncio
async def test_include_grand_total_returns_total_row_even_outside_window(adapter):
    """When include_grand_total=True, virtual windows should include one grand total row
    even if the requested row range does not naturally intersect it."""
    load_virtual_scroll_fixture(adapter)
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["region", "country"],
        aggregations=[],
    )

    no_total = await adapter.handle_virtual_scroll_request(
        request,
        0,
        0,
        [],
        include_grand_total=False,
    )
    with_total = await adapter.handle_virtual_scroll_request(
        request,
        0,
        0,
        [],
        include_grand_total=True,
    )

    assert not any(row.get("_isTotal") for row in no_total.data), (
        "Grand total should not be included when include_grand_total=False for a non-intersecting window"
    )

    total_rows = [row for row in with_total.data if row.get("_isTotal")]
    assert len(total_rows) == 1, (
        f"Expected exactly one grand total row when include_grand_total=True, got {len(total_rows)}"
    )
    assert with_total.total_rows in {no_total.total_rows, no_total.total_rows + 1}


def load_deep_scroll_fixture(adapter):
    """Load enough data so there are >2 top-level groups, each with several children.
    Layout (sorted desc by sales):
      South (4 children), North (4 children) → 2 top-level collapsed rows + grand total
    Expanding South reveals children at rows 1-4; North is the 'pre-anchor' sibling.
    """
    table = pa.Table.from_pydict({
        "region": (["South"] * 4) + (["North"] * 4),
        "country": ["Brazil", "Argentina", "Chile", "Peru",
                    "USA", "Canada", "Mexico", "Cuba"],
        "sales":   [400, 350, 300, 250,   200, 150, 100, 50],
    })
    adapter.controller.load_data_from_arrow("deep_scroll", table)


def make_deep_scroll_request():
    return TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="deep_scroll",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[{"id": "sales_sum", "desc": True}],
        grouping=["region", "country"],
        aggregations=[],
    )


@pytest.mark.asyncio
async def test_deep_scroll_pre_anchor_rows_stable_after_expansion(adapter):
    """Rows above the expanded node (pre-anchor blocks) must not change identity after expansion.

    Scenario: two top-level groups are visible collapsed.  We expand the *second* group
    (North) from a window that starts after the first group's position.  The first group's
    rows (South siblings) must remain identical in both responses.
    """
    load_deep_scroll_fixture(adapter)
    request = make_deep_scroll_request()

    # Collapsed baseline — all top-level groups visible
    collapsed = await adapter.handle_virtual_scroll_request(request, 0, 20, [])
    total_collapsed = collapsed.total_rows

    # Find the position of 'North' in the collapsed list so we can anchor on it
    north_idx = next(
        (i for i, r in enumerate(collapsed.data)
         if r.get("region") == "North" and r.get("depth") == 0 and not r.get("_isTotal")),
        None,
    )
    assert north_idx is not None, "North top-level row must appear in collapsed view"

    # Rows *before* North in the collapsed response — these are the pre-anchor rows
    pre_anchor_paths = [
        r["_path"] for r in collapsed.data[:north_idx] if not r.get("_isTotal")
    ]

    # Now expand North — request a window starting from before North's position
    expanded = await adapter.handle_virtual_scroll_request(
        request, 0, 20, [["North"]]
    )

    # 1. total_rows increased by the number of North's children
    assert expanded.total_rows > total_collapsed, "total_rows must grow after expansion"

    # 2. Pre-anchor rows are unchanged — same paths in the same order
    pre_anchor_in_expanded = [
        r["_path"] for r in expanded.data
        if r["_path"] in set(pre_anchor_paths) and not r.get("_isTotal")
    ]
    assert pre_anchor_in_expanded == pre_anchor_paths, (
        "Pre-anchor rows changed after expansion — anchor block logic may be incorrect"
    )

    # 3. North's children appear in the expanded response
    north_children = [
        r for r in expanded.data
        if r.get("depth") == 1 and r.get("region") == "North"
    ]
    assert len(north_children) > 0, "North's children must appear after expansion"


@pytest.mark.asyncio
async def test_deep_scroll_window_offset_returns_correct_rows(adapter):
    """Fetching a window that starts *after* an expanded node must return the
    correct post-anchor rows without including the node itself."""
    load_deep_scroll_fixture(adapter)
    request = make_deep_scroll_request()

    # Expand South (should have 4 children)
    expanded = await adapter.handle_virtual_scroll_request(request, 0, 20, [["South"]])
    total = expanded.total_rows

    # Find South's position and its last child
    south_children = [
        (i, r) for i, r in enumerate(expanded.data)
        if r.get("depth") == 1 and r.get("region") == "South"
    ]
    assert len(south_children) > 0, "South must have children after expansion"
    last_child_idx = south_children[-1][0]

    # Request a window starting after South's children (post-anchor window)
    post_anchor_start = last_child_idx + 1
    if post_anchor_start >= total:
        pytest.skip("Not enough rows for a post-anchor window in this dataset")

    post_window = await adapter.handle_virtual_scroll_request(
        request, post_anchor_start, min(post_anchor_start + 5, total - 1), [["South"]]
    )

    # No South children should appear in this window
    south_child_rows = [
        r for r in post_window.data
        if r.get("depth") == 1 and r.get("region") == "South"
    ]
    assert len(south_child_rows) == 0, (
        "Post-anchor window should not contain expanded node's children"
    )


@pytest.mark.asyncio
async def test_col_schema_total_center_cols_matches_full_response(adapter):
    """Observed center columns must be a subset of the full schema reported in col_schema."""
    make_wide_table(adapter)
    request = make_wide_request()

    full = await adapter.handle_virtual_scroll_request(
        request, 0, 50, [], needs_col_schema=True
    )
    assert full.col_schema is not None

    row_meta_keys = {
        "_id", "_path", "_isTotal", "_level", "_expanded",
        "_parentPath", "_has_children", "_is_expanded", "depth", "uuid", "subRows", "region",
    }
    observed_center_ids = set()
    for row in full.data:
        for k in row:
            if k not in row_meta_keys:
                observed_center_ids.add(k)

    schema_ids = {column["id"] for column in full.col_schema["columns"]}
    assert observed_center_ids.issubset(schema_ids)
    assert full.col_schema["total_center_cols"] >= len(observed_center_ids)


@pytest.mark.asyncio
async def test_col_windowing_boundary_right_edge(adapter):
    """Requesting col_end = total_center_cols - 1 must not raise and must return
    the last center column without truncating it."""
    make_wide_table(adapter)
    request = make_wide_request()

    schema_resp = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], needs_col_schema=True
    )
    total = schema_resp.col_schema["total_center_cols"]
    right_edge_schema = await adapter.handle_virtual_scroll_request(
        request,
        0,
        10,
        [],
        col_start=max(0, total - 3),
        col_end=total - 1,
        needs_col_schema=True,
    )
    last_col_id = right_edge_schema.col_schema["columns"][-1]["id"]

    windowed = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [],
        col_start=max(0, total - 3),
        col_end=total - 1,
        needs_col_schema=False,
    )

    row_meta_keys = {
        "_id", "_path", "_isTotal", "_level", "_expanded",
        "_parentPath", "_has_children", "_is_expanded", "depth", "uuid", "subRows", "region",
    }
    data_rows = [r for r in windowed.data if not r.get("_isTotal")]
    found = any(last_col_id in r for r in data_rows)
    assert found, f"Last center column '{last_col_id}' missing from right-edge window"


@pytest.mark.asyncio
async def test_col_schema_discovers_more_than_default_fifty_pivot_columns(adapter):
    """Server-side TanStack requests must not silently truncate the pivot column domain."""
    make_wide_table(adapter, center_cols=80)
    request = make_wide_request()

    schema_resp = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], needs_col_schema=True
    )

    assert schema_resp.col_schema is not None
    assert schema_resp.col_schema["total_center_cols"] == 80
    right_edge_schema = await adapter.handle_virtual_scroll_request(
        request,
        0,
        10,
        [],
        col_start=79,
        col_end=79,
        needs_col_schema=True,
    )
    last_col_id = right_edge_schema.col_schema["columns"][-1]["id"]

    windowed = await adapter.handle_virtual_scroll_request(
        request,
        0,
        10,
        [],
        col_start=79,
        col_end=79,
        needs_col_schema=False,
    )

    data_rows = [r for r in windowed.data if not r.get("_isTotal")]
    assert any(last_col_id in row for row in data_rows), (
        "Right-edge window should include pivot columns beyond the old 50-column discovery cap"
    )


@pytest.mark.asyncio
async def test_initial_schema_response_materializes_only_small_center_window(adapter):
    """The first schema-bearing response should expose the full schema without shipping every pivoted cell."""
    make_wide_table(adapter, center_cols=80)
    request = make_wide_request()

    schema_resp = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], needs_col_schema=True
    )

    assert schema_resp.col_schema is not None
    assert schema_resp.col_schema["total_center_cols"] == 80

    row_meta_keys = {
        "_id", "_path", "_isTotal", "_level", "_expanded",
        "_parentPath", "_has_children", "_is_expanded", "depth", "uuid", "subRows", "region",
    }
    observed_center_ids = {
        key
        for row in schema_resp.data
        for key in row.keys()
        if key not in row_meta_keys
    }

    assert 0 < len(observed_center_ids) <= 24, (
        f"Expected only the initial center-column window in data, got {len(observed_center_ids)} columns"
    )


@pytest.mark.asyncio
async def test_pivot_column_catalog_cache_reuses_discovery(adapter, monkeypatch):
    make_wide_table(adapter, center_cols=80)
    request = make_wide_request()
    pivot_spec = adapter.convert_tanstack_request_to_pivot_spec(request)

    original_build_column_values_query = adapter.controller.planner._build_column_values_query
    call_count = {"value": 0}

    def counting_build_column_values_query(*args, **kwargs):
        call_count["value"] += 1
        return original_build_column_values_query(*args, **kwargs)

    monkeypatch.setattr(
        adapter.controller.planner,
        "_build_column_values_query",
        counting_build_column_values_query,
    )

    await adapter._discover_pivot_column_values(pivot_spec)
    await adapter._discover_pivot_column_values(pivot_spec)

    assert call_count["value"] == 1


@pytest.mark.asyncio
async def test_virtual_scroll_response_cache_reuses_identical_window(adapter, monkeypatch):
    make_wide_table(adapter, center_cols=40)
    request = make_wide_request()

    original_run_hierarchy_view = adapter.controller.run_hierarchy_view
    call_count = {"value": 0}

    async def counting_run_hierarchy_view(*args, **kwargs):
        call_count["value"] += 1
        return await original_run_hierarchy_view(*args, **kwargs)

    monkeypatch.setattr(adapter.controller, "run_hierarchy_view", counting_run_hierarchy_view)

    first = await adapter.handle_virtual_scroll_request(
        request, 0, 20, [], needs_col_schema=True, col_start=0, col_end=5
    )
    second = await adapter.handle_virtual_scroll_request(
        request, 0, 20, [], needs_col_schema=True, col_start=0, col_end=5
    )

    assert call_count["value"] == 1
    assert first.total_rows == second.total_rows
    assert first.data == second.data


@pytest.mark.asyncio
async def test_run_hierarchy_view_reuses_cached_batch_for_incremental_expansion(adapter, monkeypatch):
    controller = adapter.controller
    spec = PivotSpec(
        table="sales_data",
        rows=["region", "country"],
        columns=[],
        measures=[Measure(field="sales", agg="sum", alias="sales_sum")],
        filters=[],
        totals=False,
    )

    root_rows = [
        {"region": "North", "sales_sum": 10},
        {"region": "South", "sales_sum": 20},
    ]
    north_rows = [{"region": "North", "country": "USA", "sales_sum": 10}]
    south_rows = [{"region": "South", "country": "Brazil", "sales_sum": 20}]
    call_targets = []

    async def fake_batch_load(spec_dict, target_paths, max_levels=3):
        call_targets.append([list(path) for path in target_paths])
        result = {"": [dict(row) for row in root_rows]}
        if ["North"] in target_paths:
            result["North"] = [dict(row) for row in north_rows]
        if ["South"] in target_paths:
            result["South"] = [dict(row) for row in south_rows]
        return result

    monkeypatch.setattr(controller, "run_hierarchical_pivot_batch_load", fake_batch_load)

    first = await controller.run_hierarchy_view(spec, [["North"]])
    second = await controller.run_hierarchy_view(spec, [["North"], ["South"]])

    assert call_targets[0] == [["North"]]
    assert call_targets[1] == [["South"]]
    assert any(row.get("country") == "USA" for row in second["rows"])
    assert any(row.get("country") == "Brazil" for row in second["rows"])
    assert len(second["rows"]) > len(first["rows"])


@pytest.mark.asyncio
async def test_virtual_scroll_transport_columns_are_trimmed(adapter):
    make_wide_table(adapter, center_cols=30)
    request = make_wide_request()

    response = await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], needs_col_schema=True, col_start=0, col_end=5
    )

    allowed_keys = {"id", "header", "headerVal", "accessorKey", "col_schema"}
    assert response.columns
    for column in response.columns:
        assert set(column.keys()).issubset(allowed_keys)


@pytest.mark.asyncio
async def test_virtual_scroll_schedules_background_prefetch(adapter, monkeypatch):
    make_wide_table(adapter, center_cols=40)
    request = make_wide_request()
    scheduled = []

    class DummyTaskManager:
        def create_task(self, coro, name="task"):
            scheduled.append(name)
            coro.close()
            return None

    monkeypatch.setattr(adapter.controller, "task_manager", DummyTaskManager())
    adapter.viewport_prefetch_enabled = True

    await adapter.handle_virtual_scroll_request(
        request, 0, 10, [], needs_col_schema=True, col_start=0, col_end=5
    )

    assert scheduled, "Viewport requests should schedule at least one background prefetch task"


@pytest.mark.asyncio
async def test_collapsed_root_virtual_scroll_uses_paged_root_query(adapter, monkeypatch):
    rows = 100_000
    table = pa.Table.from_pydict(
        {
            "row_id": list(range(rows)),
            "sales": [100 + (idx % 900) for idx in range(rows)],
            "cost": [50 + (idx % 700) for idx in range(rows)],
        }
    )
    adapter.controller.load_data_from_arrow("bench_100k", table)

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="bench_100k",
        columns=[
            {"id": "row_id"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["row_id"],
        aggregations=[
            {"field": "sales", "agg": "sum", "alias": "sales_sum"},
            {"field": "cost", "agg": "sum", "alias": "cost_sum"},
        ],
        totals=False,
        row_totals=False,
        version=1,
        column_sort_options={},
    )

    original_run_pivot_async = adapter.controller.run_pivot_async
    recorded_specs = []

    async def recording_run_pivot_async(spec, *args, **kwargs):
        root_filter = None
        for filter_spec in (getattr(spec, "filters", None) or []):
            if isinstance(filter_spec, dict) and filter_spec.get("field") == "row_id":
                root_filter = {
                    "op": filter_spec.get("op"),
                    "value_len": len(filter_spec.get("value") or []),
                    "first_value": (filter_spec.get("value") or [None])[0],
                    "last_value": (filter_spec.get("value") or [None])[-1],
                }
                break
        recorded_specs.append(
            {
                "rows": list(getattr(spec, "rows", []) or []),
                "limit": getattr(spec, "limit", None),
                "offset": getattr(spec, "offset", None),
                "force_refresh": kwargs.get("force_refresh", False),
                "root_filter": root_filter,
            }
        )
        return await original_run_pivot_async(spec, *args, **kwargs)

    monkeypatch.setattr(adapter.controller, "run_pivot_async", recording_run_pivot_async)

    first = await adapter.handle_virtual_scroll_request(
        request,
        0,
        99,
        [],
        col_start=0,
        col_end=3,
        needs_col_schema=True,
        include_grand_total=False,
        _allow_prefetch=False,
    )
    deep = await adapter.handle_virtual_scroll_request(
        request,
        49900,
        50099,
        [],
        col_start=0,
        col_end=3,
        needs_col_schema=False,
        include_grand_total=False,
        _allow_prefetch=False,
    )

    assert len(first.data) == 100
    assert len(deep.data) == 200
    assert deep.data[0]["row_id"] == 49900
    assert deep.data[-1]["row_id"] == 50099

    assert any(
        call["rows"] == ["row_id"]
        and call["limit"] is None
        and call["offset"] == 0
        and call["force_refresh"] is True
        and call["root_filter"] == {
            "op": "in",
            "value_len": 200,
            "first_value": 49900,
            "last_value": 50099,
        }
        for call in recorded_specs
    ), "Deep collapsed scroll should aggregate only the requested page root keys instead of rebuilding all root groups"


@pytest.mark.asyncio
async def test_collapsed_root_virtual_scroll_with_grand_total_uses_paged_root_query(adapter, monkeypatch):
    rows = 100_000
    table = pa.Table.from_pydict(
        {
            "row_id": list(range(rows)),
            "sales": [100 + (idx % 900) for idx in range(rows)],
            "cost": [50 + (idx % 700) for idx in range(rows)],
        }
    )
    adapter.controller.load_data_from_arrow("bench_100k_totals", table)

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="bench_100k_totals",
        columns=[
            {"id": "row_id"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["row_id"],
        aggregations=[
            {"field": "sales", "agg": "sum", "alias": "sales_sum"},
            {"field": "cost", "agg": "sum", "alias": "cost_sum"},
        ],
        totals=True,
        row_totals=False,
        version=1,
        column_sort_options={},
    )

    original_run_pivot_async = adapter.controller.run_pivot_async
    recorded_specs = []

    async def recording_run_pivot_async(spec, *args, **kwargs):
        root_filter = None
        for filter_spec in (getattr(spec, "filters", None) or []):
            if isinstance(filter_spec, dict) and filter_spec.get("field") == "row_id":
                root_filter = {
                    "op": filter_spec.get("op"),
                    "value_len": len(filter_spec.get("value") or []),
                    "first_value": (filter_spec.get("value") or [None])[0],
                    "last_value": (filter_spec.get("value") or [None])[-1],
                }
                break
        recorded_specs.append(
            {
                "rows": list(getattr(spec, "rows", []) or []),
                "limit": getattr(spec, "limit", None),
                "offset": getattr(spec, "offset", None),
                "force_refresh": kwargs.get("force_refresh", False),
                "root_filter": root_filter,
            }
        )
        return await original_run_pivot_async(spec, *args, **kwargs)

    monkeypatch.setattr(adapter.controller, "run_pivot_async", recording_run_pivot_async)

    response = await adapter.handle_virtual_scroll_request(
        request,
        49900,
        50099,
        [],
        col_start=0,
        col_end=3,
        needs_col_schema=False,
        include_grand_total=True,
        _allow_prefetch=False,
    )
    second_response = await adapter.handle_virtual_scroll_request(
        request,
        99800,
        99999,
        [],
        col_start=0,
        col_end=3,
        needs_col_schema=False,
        include_grand_total=True,
        _allow_prefetch=False,
    )

    assert response.total_rows == 100001
    assert second_response.total_rows == 100001
    assert any(row.get("_isTotal") for row in response.data)
    assert any(
        call["rows"] == ["row_id"]
        and call["limit"] is None
        and call["offset"] == 0
        and call["force_refresh"] is True
        and call["root_filter"] == {
            "op": "in",
            "value_len": 200,
            "first_value": 49900,
            "last_value": 50099,
        }
        for call in recorded_specs
    ), "Collapsed scroll with a grand total row should still aggregate only the requested root key page"
    assert any(
        call["rows"] == []
        and call["limit"] == 1
        and call["force_refresh"] is True
        for call in recorded_specs
    ), "Grand total should be fetched separately instead of disabling the paged root path"
    assert sum(1 for call in recorded_specs if call["rows"] == [] and call["limit"] == 1) == 1


def test_collapsed_root_cache_keys_ignore_materialized_pivot_window(adapter):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "date"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["date"],
        aggregations=[
            {"field": "sales", "agg": "sum", "alias": "sales_sum"},
            {"field": "cost", "agg": "sum", "alias": "cost_sum"},
        ],
        totals=False,
        row_totals=False,
        version=1,
        column_sort_options={},
    )

    spec_a = adapter.convert_tanstack_request_to_pivot_spec(request)
    spec_b = adapter.convert_tanstack_request_to_pivot_spec(request)

    assert spec_a.pivot_config is not None
    assert spec_b.pivot_config is not None

    spec_a.pivot_config.materialized_column_values = ["USA", "Canada"]
    spec_b.pivot_config.materialized_column_values = ["Japan", "Germany"]

    controller = adapter.controller
    root_sort = controller._build_group_rows_sort(spec_a.rows[:1], spec_a.sort, spec_a.column_sort_options)

    assert controller._collapsed_root_count_cache_key(spec_a) == controller._collapsed_root_count_cache_key(spec_b)
    assert (
        controller._collapsed_root_page_cache_key(spec_a, root_sort, 1000, 100)
        == controller._collapsed_root_page_cache_key(spec_b, root_sort, 1000, 100)
    )


@pytest.mark.asyncio
async def test_topn_query_skips_column_discovery_when_materialized_values_are_present(adapter, monkeypatch):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "date"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["date"],
        aggregations=[
            {"field": "sales", "agg": "sum", "alias": "sales_sum"},
            {"field": "cost", "agg": "sum", "alias": "cost_sum"},
        ],
        totals=False,
        row_totals=False,
        version=1,
        column_sort_options={},
    )

    spec = adapter.convert_tanstack_request_to_pivot_spec(request)
    assert spec.pivot_config is not None
    spec.pivot_config.materialized_column_values = ["USA", "Canada"]

    original_execute = adapter.controller._execute_ibis_expr_async
    execute_calls = []

    async def recording_execute(expr, table_name=None):
        execute_calls.append(expr)
        return await original_execute(expr, table_name)

    monkeypatch.setattr(adapter.controller, "_execute_ibis_expr_async", recording_execute)

    profile_sink = {}
    result = await adapter.controller.run_pivot_async(
        spec,
        return_format="arrow",
        force_refresh=True,
        profile_sink=profile_sink,
    )

    assert isinstance(result, pa.Table)
    assert len(execute_calls) == 1
    assert profile_sink["plannerExecution"]["columnDiscoverySkipped"] is True
    assert profile_sink["plannerExecution"]["materializedColumns"] == 2


def test_row_virtualization_source_has_coalesced_scheduler_and_pinned_cache():
    source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useServerSideRowModel.js",
        )
    ).read_text(encoding="utf-8")
    cache_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useRowCache.js",
        )
    ).read_text(encoding="utf-8")

    assert "enqueueViewportRequest" in source
    assert "viewportFlushTimerRef" in source
    assert "setPinnedRange" in source
    assert "blockJumpDistance" in source
    assert "shouldDispatchImmediately" in source
    assert "lastFastScrollDispatchRef" in source
    assert "lastImmediateViewportRef" in source
    assert "columnRangeUrgencyToken" in source
    assert "recentImmediateViewport.colStart === colStart" in source
    assert "recentImmediateViewport.colEnd === colEnd" in source
    assert "requestUrgentColumnViewport" in source
    assert "getServerSideRowOverscan" in source
    assert "getUrgentJumpRowOverscan" in source
    assert "handleScroll" in source
    assert "VIEWPORT_BLOCK_STALE_MS" in source
    assert "VIEWPORT_DUPLICATE_REQUEST_WINDOW_MS" in source
    assert "pinnedWindowRef" in cache_source
    assert "evictOverflow" in cache_source


def test_server_side_component_source_has_fast_horizontal_dispatch_and_trimmed_row_models():
    source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    hook_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnVirtualizer.js",
        )
    ).read_text(encoding="utf-8")
    viewport_hook_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useServerSideViewportController.js",
        )
    ).read_text(encoding="utf-8")
    body_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Table",
            "PivotTableBody.js",
        )
    ).read_text(encoding="utf-8")

    assert "chartModelingActive" in source
    assert "useServerSideViewportController" in source
    assert "getExpandedRowModel: serverSide ? undefined : getExpandedRowModel()" in source
    assert "getGroupedRowModel: serverSide ? undefined : getGroupedRowModel()" in source
    assert "const serverSideBlockSize = useMemo" in viewport_hook_source
    assert "blockSize: serverSideBlockSize" in source
    assert "lastObservedHorizontalScrollLeftRef" in viewport_hook_source
    assert "lastFastHorizontalRangeRef" in viewport_hook_source
    assert "columnRangeUrgencyToken" in viewport_hook_source
    assert "handleHorizontalScroll" in hook_source
    assert "onHorizontalScrollMetrics" in hook_source
    assert "SERVER_SIDE_HORIZONTAL_METRICS_DEBOUNCE_MS" in hook_source
    assert "scheduleHorizontalMetrics" in hook_source
    assert "pendingHorizontalMetricsRef" in hook_source
    assert "Measuring on every horizontal virtual-index change makes wide pivots" in hook_source
    assert "lastVirtualCenterIndex, parentRef, totalLayoutWidth" not in hook_source
    assert "horizontalOverscan" in hook_source
    assert "totalCenterCols" in hook_source
    assert "columnSizing," in hook_source
    assert "bigJumpThreshold" in viewport_hook_source
    assert "handleHorizontalScrollMetrics" in viewport_hook_source
    assert "resetVisibleColRange" in viewport_hook_source
    assert "preserveRecentUrgentRange" in viewport_hook_source
    assert "edgeSafetyCount" in viewport_hook_source
    assert "largeColumnMode" in viewport_hook_source
    assert "extremeColumnMode" in viewport_hook_source
    assert "deferredHorizontalMetricsRef" in viewport_hook_source
    assert "isRequestPending || isHorizontalColumnRequestPending" in viewport_hook_source
    assert "preserveRecentRightEdgeUrgentRange" in viewport_hook_source
    assert "syncPreciseVisibleColRange" in viewport_hook_source
    assert "centerHeaderRenderPlan" in body_source
    assert "centerColumnWidthPrefix" in body_source
    assert "centerLeafEntry.indices" in body_source
    assert "columnAdvisory" in body_source
    assert "visibleCenterRange" in body_source
    assert "minIdx" in body_source
    assert "maxIdx" in body_source
    assert "renderVirtualColumnCell" in body_source
    assert "immersiveMode={immersiveMode}" in source
    assert "{!immersiveMode && (" in body_source
    assert "_getAllCellsByColumnId" not in body_source


def test_large_column_guardrail_source_warns_without_disabling_features():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    status_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Table",
            "StatusBar.js",
        )
    ).read_text(encoding="utf-8")
    normalization_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "usePivotNormalization.js",
        )
    ).read_text(encoding="utf-8")

    assert "SOFT_CENTER_COLUMN_WARNING_THRESHOLD" in component_source
    assert "HARD_CENTER_COLUMN_WARNING_THRESHOLD" in component_source
    assert "buildLargeColumnAdvisory" in component_source
    assert "bucket numeric fields, roll up dates, or move one field to Rows or Filters" in normalization_source
    assert "columnAdvisory" in status_source
    assert "totalCenterColumns" in status_source


def test_server_side_performance_config_source_exposes_ssrm_style_tuning():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    row_model_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useServerSideRowModel.js",
        )
    ).read_text(encoding="utf-8")
    viewport_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useServerSideViewportController.js",
        )
    ).read_text(encoding="utf-8")
    col_virtualizer_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnVirtualizer.js",
        )
    ).read_text(encoding="utf-8")

    assert "normalizePerformanceConfigValue" in component_source
    assert "performanceConfig: PropTypes.shape" in component_source
    assert "cacheBlockSize" in component_source
    assert "maxBlocksInCache" in component_source
    assert "blockLoadDebounceMs" in component_source
    assert "rowOverscan" in component_source
    assert "columnOverscan" in component_source
    assert "prefetchColumns" in component_source
    assert "maxBlocksInCache" in row_model_source
    assert "blockLoadDebounceMs" in row_model_source
    assert "rowOverscan" in row_model_source
    assert "prefetchColumns" in row_model_source
    assert "performanceConfig" in viewport_source
    assert "columnOverscan" in col_virtualizer_source
    assert "const signature = columns.map((column) => {" in col_virtualizer_source
    assert "[table, columns, columnVisibility, columnPinning, columnSizing]" in col_virtualizer_source


def test_frontend_profiler_source_tracks_request_ids_and_global_history():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    row_model_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useServerSideRowModel.js",
        )
    ).read_text(encoding="utf-8")
    profiler_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "pivotProfiler.js",
        )
    ).read_text(encoding="utf-8")

    assert "getPivotProfiler" in component_source
    assert "recordProfilerResponse" in component_source
    assert "pendingDataProfilerCommitRef" in component_source
    assert "requestId = `viewport:" in row_model_source
    assert "queuedAt" in row_model_source
    assert "window.__pivotProfiler" in profiler_source
    assert "getHistory" in profiler_source
    assert "summary" in profiler_source


def test_frontend_batch_edit_source_uses_runtime_transaction_requests():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")

    assert "const dispatchBatchUpdateRequest = useCallback" in component_source
    assert "emitRuntimeRequest('transaction'" in component_source
    assert "supportsPatchTransactionRefresh" in component_source
    assert "refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart'" in component_source
    assert "visibleRowPaths: visiblePatchRowPathsRef.current" in component_source
    assert "visibleCenterColumnIds: visiblePatchCenterIdsRef.current" in component_source
    assert "applyRuntimePatchEnvelope" in component_source
    assert "silent: !showGlobalLoading" in component_source
    assert "shouldShowTransactionLoading" in component_source
    assert "dispatchBatchUpdateRequest(updates, 'paste')" in component_source
    assert "dispatchBatchUpdateRequest(updates, 'fill')" in component_source
    assert "dispatchBatchUpdateRequest([update], 'inline-edit')" in component_source
    assert "propagationStrategy: formula" in component_source
    assert "pendingPropagationUpdatesRef" in component_source
    assert "handleConfirmPropagation" in component_source
    assert "handleCancelPropagation" in component_source
    assert "inverseTransaction" in component_source
    assert "redoTransaction" in component_source
    assert "describeTransactionPropagation" in component_source
    assert "transactionUndoStackRef" in component_source
    assert "transactionRedoStackRef" in component_source
    assert "handleGlobalEditShortcut" in component_source
    assert "onUndoTransaction={handleUndo}" in component_source
    assert "onRedoTransaction={handleRedo}" in component_source
    assert "requestLayoutHistoryCapture" in component_source
    assert "pushUnifiedHistoryEntry" in component_source
    assert "applyLayoutHistorySnapshot" in component_source
    assert "kind: 'layout'" in component_source
    assert "setRowFieldsWithHistory" in component_source
    assert "setColFieldsWithHistory" in component_source
    assert "setValConfigsWithHistory" in component_source
    assert "setColumnPinningWithHistory" in component_source
    assert "setRowFields={setRowFieldsWithHistory}" in component_source
    assert "setColFields={setColFieldsWithHistory}" in component_source
    assert "setValConfigs={setValConfigsWithHistory}" in component_source


def test_cell_sparkline_source_supports_pivot_summary_and_ag_grid_style_series():
    column_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnDefs.js",
        )
    ).read_text(encoding="utf-8")
    sparkline_utils_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "sparklines.js",
        )
    ).read_text(encoding="utf-8")
    helper_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "helpers.js",
        )
    ).read_text(encoding="utf-8")
    sparkline_cell_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Table",
            "SparklineCell.js",
        )
    ).read_text(encoding="utf-8")
    sidebar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "SidebarPanel.js",
        )
    ).read_text(encoding="utf-8")
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    normalization_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "usePivotNormalization.js",
        )
    ).read_text(encoding="utf-8")
    python_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "dash_tanstack_pivot",
            "DashTanstackPivot.py",
        )
    ).read_text(encoding="utf-8")

    assert "SparklineCell" in column_source
    assert "renderConfiguredInlineSparkline" in column_source
    assert "buildSparklineSummaryColumns" in column_source
    assert "sparklineConfig.source !== 'field'" in column_source
    assert "sparklineConfig.displayMode === 'value'" in column_source
    assert "data-pivot-sparkline-display=\"value\"" in column_source
    assert "renderInlinePivotSparkline" not in column_source
    assert "__sparkline__" in column_source
    assert "isSparklineSummary: true" in column_source
    assert "normalizeSparklineConfig" in sparkline_utils_source
    assert "displayMode:" in sparkline_utils_source
    assert "source.enabled === false" in sparkline_utils_source
    assert "normalizeSparklinePoints" in sparkline_utils_source
    assert "buildPivotSparklinePoints" in sparkline_utils_source
    assert "resolveSparklineMetricValue" in sparkline_utils_source
    assert "formatNonScalarValue" in helper_source
    assert "return `${value.length} points`;" in helper_source
    assert "data-pivot-sparkline-cell=\"true\"" in sparkline_cell_source
    assert "data-pivot-sparkline-current=\"true\"" in sparkline_cell_source
    assert "data-pivot-sparkline-delta=\"true\"" in sparkline_cell_source
    assert "buildSparklineGeometry" in component_source
    assert "data-pivot-sparkline-detail-graph=\"true\"" in component_source
    assert "data-pivot-sparkline-hover-target=\"true\"" in component_source
    assert "data-pivot-sparkline-tooltip=\"true\"" in component_source
    assert "data-pivot-sparkline-detail-point=\"true\"" in component_source
    assert "data-pivot-sparkline-zoom-controls=\"true\"" in component_source
    assert "data-pivot-sparkline-zoom-in=\"true\"" in component_source
    assert "data-pivot-sparkline-zoom-reset=\"true\"" in component_source
    assert "data-pivot-sparkline-detail-table=\"true\"" in component_source
    assert "normalizeSparklineValConfigsForView" in component_source
    assert "getFixedSparklineConfigMap" in component_source
    assert "removesFixedSparkline" in component_source
    assert "fixedSparklineValueKeys" in component_source
    assert "setValConfigs(normalizeSparklineValConfigsForView(restored.valConfigs, normalizedInitialValConfigs))" in component_source
    assert "valConfigs: normalizeSparklineValConfigsForView(valConfigs, normalizedInitialValConfigs)" in component_source
    assert "sparkline: PropTypes.oneOfType([PropTypes.bool, PropTypes.object])" in component_source
    assert "'array_agg'" in component_source
    assert "\"sparkline\": NotRequired[typing.Union[bool, dict]]" in python_source
    assert "- sparkline (boolean | dict; optional)" in python_source
    assert '"array_agg"' in python_source
    assert "if (columnMeta && columnMeta.isSparklineSummary) return null;" in normalization_source
    assert "const placement = sparklineConfig.placement || 'before';" in column_source
    assert "placement: 'before'" in sparkline_utils_source
    assert "placement: 'before'" in sidebar_source
    assert "data-value-sparkline-toggle=\"true\"" in sidebar_source
    assert "data-value-sparkline-settings=\"true\"" in sidebar_source
    assert "data-value-sparkline-fixed=\"true\"" in sidebar_source
    assert "data-value-sparkline-available=" in sidebar_source
    assert "data-value-sparkline-unavailable=\"true\"" in sidebar_source
    assert "getValueSparklineCapabilities" in sidebar_source
    assert "This trend is defined by the app" in sidebar_source
    assert '<option value="array_agg">Series</option>' in sidebar_source
    assert "sanitizeValueSparklineConfig" in sidebar_source
    assert "Cell display" in sidebar_source


def test_sparse_schema_row_totals_do_not_render_blank_placeholders():
    column_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnDefs.js",
        )
    ).read_text(encoding="utf-8")

    row_total_guard = "if (isRowTotalColumnId(entryId))"
    placeholder_emit = "sparseDataCols.push(buildPlaceholderColumn(index, schemaSize));"

    assert "const isRowTotalColumnId = (columnId) => (" in column_source
    assert ".filter(isRowTotalColumnId)" in column_source
    assert "Row totals are rendered as auxiliary columns after the pivot tree." in column_source
    assert row_total_guard in column_source
    assert column_source.index(row_total_guard) < column_source.index(placeholder_emit)
    assert "const pivotCols = flatCols.filter(c => !isRowTotalColumnId(c.id));" in column_source
    assert ".filter(c => isRowTotalColumnId(c.id))" in column_source


def test_frontend_sorting_source_preserves_multisort_priority_and_metadata():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    sorting_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "sorting.js",
        )
    ).read_text(encoding="utf-8")

    assert "updateSortingForColumn({" in component_source
    assert "normalizeSortingState(rawNextSorting, sorting)" in component_source
    assert "Update Priority" in component_source
    assert "mergeSortSpecifierMetadata" in sorting_source
    assert "updatedExisting = true" in sorting_source
    assert "SORT_METADATA_KEYS" in sorting_source


def test_frontend_editable_cells_flow_through_column_metadata_and_transaction_history():
    column_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnDefs.js",
        )
    ).read_text(encoding="utf-8")
    editable_cell_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Table",
            "EditableCell.js",
        )
    ).read_text(encoding="utf-8")

    assert "isEditableMeasureConfig" in column_source
    assert "columnConfig={config}" in column_source
    assert "displayValue={getResolvedCellValue(info)}" in column_source
    assert "onCellEdit={onCellEdit}" in column_source
    assert "theme," in column_source
    assert "defaultColumnWidths," in column_source
    assert "validationRules," in column_source
    assert "renderedOffset," in column_source
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    assert "dispatchBatchUpdateRequest([update], 'inline-edit')" in component_source
    assert "optimisticCellValuesRef" in component_source
    assert "captureOptimisticCellValues(effectiveUpdates, dispatched.requestId);" in component_source
    assert "const setExpandedWithHistory = useCallback" in component_source
    assert "const setColExpandedWithHistory = useCallback" in component_source
    assert "setExpandedWithHistory(newExpanded, 'layout:expanded');" in component_source
    assert "setExpandedWithHistory(shouldExpand ? true : {}, 'layout:expand-all');" in component_source
    assert "setExpandedWithHistory((prev) => {" in component_source
    assert "'layout:expand-subtree'" in component_source
    assert "const resolveCurrentCellValue = useCallback" in component_source
    assert "const resolveDisplayedCellValue = useCallback" in component_source
    assert "useState('original')" in component_source
    assert "deriveStructuralThemeTokens" in component_source
    assert "if (aggregationFn === 'sum')" in component_source
    assert "resolveAggregationConfigForColumnId" in component_source
    assert "buildEditedCellMarkerPlan" in component_source
    assert "applyEditedCellMarkerPlan" in component_source
    assert "buildComparisonValuePlan" in component_source
    assert "applyComparisonValuePlan" in component_source
    assert "serializeEditComparisonState" in component_source
    assert "restoreSerializedEditComparisonState" in component_source
    assert "editComparisonState: serializedEditComparisonState" in component_source
    assert "resolveEditedCellMarker" in component_source
    assert "editValueDisplayMode" in component_source
    assert "resolveCellDisplayValue: resolveDisplayedCellValue" in component_source
    assert "resolveCurrentCellValue={resolveCurrentCellValue}" in component_source
    assert "setEditValueDisplayMode={setEditValueDisplayMode}" in component_source
    assert "normalizedEditingConfig" in component_source
    assert "const resolveEditorPresentation = useCallback" in component_source
    assert "const startRowEditSession = useCallback" in component_source
    assert "const saveRowEditSession = useCallback" in component_source
    assert "const updateRowDraftValue = useCallback" in component_source
    assert "const renderRowEditActions = useCallback" in component_source
    assert "data-row-edit-action=\"start\"" not in component_source
    assert "editLifecycleEvent" in component_source
    assert "emitEditLifecycleEvent({" in component_source
    assert "editorOptionsLoadingState" in component_source
    assert "const requestEditorOptions = useCallback" in component_source
    assert "aggregation =" in editable_cell_source
    assert "source: 'inline-edit'" in editable_cell_source
    assert "rowPath: rowPath || null" in editable_cell_source
    assert "displayValue," in editable_cell_source
    assert "rowEditSession = null" in editable_cell_source
    assert "editorConfig = null" in editable_cell_source
    assert "onRequestRowStart," in editable_cell_source
    assert "requestEditorOptions" in editable_cell_source
    assert "editorOptionsLoading = false" in editable_cell_source
    assert "data-editor-type" in editable_cell_source
    assert "normalizeEditorType" in editable_cell_source
    assert "validateEditorValue" in editable_cell_source
    assert "if (editorType === 'textarea')" in editable_cell_source
    assert "if (editorType === 'checkbox')" in editable_cell_source
    assert "if (editorType === 'select')" in editable_cell_source
    assert "if (editorType === 'richSelect')" in editable_cell_source
    assert "<datalist id={datalistIdRef.current}>" in editable_cell_source
    assert "const commitValue = ({ keepEditing = false, nextValue: nextDraftValue = value } = {}) => {" in editable_cell_source
    assert "commitValue({ nextValue: nextChecked })" in editable_cell_source
    assert "commitValue({ nextValue });" in editable_cell_source
    assert "onDoubleClick={(e) => {" in editable_cell_source
    assert "onMouseDown={(e) => {" not in editable_cell_source
    assert "onClick={(e) => {" not in editable_cell_source
    assert "const EDITING_TEMPORARILY_DISABLED = true;" in component_source
    assert "showEditing: !EDITING_TEMPORARILY_DISABLED && src.showEditing !== false" in component_source
    assert "showEditPanel: !EDITING_TEMPORARILY_DISABLED && src.showEditPanel !== false" in component_source
    assert "if (!editingEnabled) return null;" in component_source
    assert "const getRenderedCellWidthForColumn = useCallback((columnId) => {" in component_source
    assert "const getAutoSizeSampleRows = useCallback((rows) => {" in component_source
    assert "const renderedCellWidth = (columnId !== 'hierarchy' && columnId !== '__row_number__' && columnId !== '__report_config__')" in component_source
    assert "const measuredFromCells = (col.id !== 'hierarchy' && col.id !== '__row_number__' && col.id !== '__report_config__')" in component_source
    assert "const reportDefRef = useRef(reportDef);" in component_source
    assert "reportDefRef.current = reportDef;" in component_source
    assert "buildReportEditorSelectionFromRow(reportDefRef.current, rowData)" in component_source
    assert "Math.min(Math.max(measuredWithCells, autoSizeBounds.minWidth), autoSizeBounds.maxWidth)" in component_source
    assert "Math.ceil(renderedHeaderWidth)" in component_source
    assert "contentWidth + horizontalChrome + autoSizeBounds.cellOverscan" in component_source
    assert "const measurementClone = headerContent.cloneNode(true);" in component_source
    assert "measurementClone.style.width = 'max-content';" in component_source

    app_bar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "PivotAppBar.js",
        )
    ).read_text(encoding="utf-8")
    assert "view: false," in app_bar_source
    assert "format: true," in app_bar_source

    styles_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "styles.js",
        )
    ).read_text(encoding="utf-8")
    assert "export const deriveEditedCellThemeTokens = (baseTheme, explicitOverrides = null) => {" in styles_source
    assert "export const CELL_CONTENT_RESET_STYLE = Object.freeze({" in styles_source
    assert "const glowBaseColor = fillColor || borderColor;" in styles_source
    assert "const explicitFillColor = editedCellFmt && editedCellFmt.bg ? editedCellFmt.bg : null;" in styles_source
    assert "const borderColor = explicitFillColor" in styles_source
    assert "const derivedBorderFromBg = explicitBg" in styles_source
    assert "export const deriveStructuralThemeTokens = (baseTheme, explicitOverrides = null) => {" in styles_source
    assert "export const colorToInputHex = (value, fallback = '#000000') => {" in styles_source
    assert "useOptionalPivotValueDisplay" in editable_cell_source
    assert "const currentCellValue = valueDisplayContext && typeof valueDisplayContext.resolveCurrentCellValue === 'function'" in editable_cell_source
    assert "const contextDisplayValue = valueDisplayContext && typeof valueDisplayContext.resolveCellDisplayValue === 'function'" in editable_cell_source
    assert "const resolvedDisplayValue = contextDisplayValue !== undefined" in editable_cell_source
    assert "const isOriginalMode = Boolean(" in editable_cell_source
    assert "const effectiveEditingDisabled = editingDisabled || !resolvedEditorConfig || resolvedEditorConfig.editable === false;" in editable_cell_source
    assert "if (!isRowEditing && supportsRowEditSession && typeof onRequestRowStart === 'function')" in editable_cell_source
    assert "setValue(toEditorInputValue(baseValue, resolvedEditorConfig));" in editable_cell_source
    assert "setSubmittedDisplayValue(nextValue);" in editable_cell_source
    assert "const effectiveDisplayValue = (" in editable_cell_source
    assert "formatEditorDisplayValue(" in editable_cell_source
    assert "...CELL_CONTENT_RESET_STYLE" in editable_cell_source
    assert "'data-edit-rowid': rowPath" in editable_cell_source
    assert "'data-edit-colid': column.id" in editable_cell_source
    assert "data-display-rowid={rowPath}" in editable_cell_source
    assert "resolveEditorPresentation," in column_source
    assert "getRowEditSession," in column_source
    assert "renderRowEditActions" in column_source
    assert "data-row-edit-action=\"save\"" in component_source
    editing_utils_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "editing.js",
        )
    ).read_text(encoding="utf-8")
    assert "normalizeEditingConfig" in editing_utils_source
    assert "resolveColumnEditSpec" in editing_utils_source
    assert "validateEditorValue" in editing_utils_source
    assert "normalizeEditorOptions" in editing_utils_source
    status_bar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Table",
            "StatusBar.js",
        )
    ).read_text(encoding="utf-8")
    assert "data-pivot-status-bar=\"true\"" in status_bar_source
    assert "const [showDetails, setShowDetails] = React.useState(false);" in status_bar_source
    assert "data-pivot-status-details-open={showDetails ? 'true' : 'false'}" in status_bar_source
    assert "data-pivot-status-details=\"true\"" in status_bar_source
    assert "data-pivot-status-summary=\"true\"" in status_bar_source
    assert "data-pivot-status-actions=\"true\"" in status_bar_source
    assert "data-pivot-status-panel={id}" in status_bar_source
    assert "data-pivot-status-action={id}" in status_bar_source
    assert "label={showDetails ? 'Hide Status' : 'Show Status'}" in status_bar_source
    assert "Detailed panels are off by default." in status_bar_source
    assert "label=\"Mean\"" in status_bar_source
    assert "label=\"Std Dev\"" in status_bar_source
    assert "summarizeSelection" in status_bar_source
    assert "label=\"Selection\"" in status_bar_source
    assert "label=\"Editing\"" in status_bar_source
    assert "label=\"Charts\"" in status_bar_source
    assert "label=\"Range Chart\"" in status_bar_source
    assert "label=\"Refresh View\"" in status_bar_source
    assert "const statusAccessoryModel = useMemo(() => ({" in component_source
    assert "const statusAccessoryActions = useMemo(() => ({" in component_source
    assert "statusModel={statusAccessoryModel}" in component_source
    assert "statusActions={statusAccessoryActions}" in component_source
    assert "data-rowid={row.id}" in Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useRenderHelpers.js",
        )
    ).read_text(encoding="utf-8")


def test_frontend_chart_surface_includes_inline_settings_sparkline_and_resize_reflow_hooks():
    chart_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Charts",
            "PivotCharts.js",
        )
    ).read_text(encoding="utf-8")
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    normalization_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "usePivotNormalization.js",
        )
    ).read_text(encoding="utf-8")
    column_virtualizer_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnVirtualizer.js",
        )
    ).read_text(encoding="utf-8")

    assert "data-chart-settings-toggle=\"true\"" in chart_source
    assert "data-chart-settings-pane=\"true\"" in chart_source
    assert "data-chart-settings-scroll=\"true\"" in chart_source
    assert "data-chart-surface-scroll=\"true\"" in chart_source
    assert "data-chart-settings-resizer=\"true\"" in chart_source
    assert "data-chart-sparkline-board=\"true\"" in chart_source
    assert "data-chart-sparkline-card=" in chart_source
    assert "const PivotChartPanelContent = ({" in chart_source
    assert "export const PivotChartPanel = (props) => {" in chart_source
    assert "if (!props.open) return null;" in chart_source
    assert "return <PivotChartPanelContent {...props} />;" in chart_source
    assert "const handleCopyChart = useCallback(async () => {" in chart_source
    assert "setChartCopyStatus(copied" in chart_source
    assert "const isSparklineChart = chartType === 'sparkline';" in chart_source
    assert "overscrollBehavior: 'contain'" in chart_source
    assert "applyChartPreset" in chart_source
    assert "onChartTypeChange('sparkline')" in chart_source
    assert "chartType === 'sparkline'" in chart_source
    assert "showLegend={!isSparklineChart}" in chart_source
    assert "VALID_CHART_TYPES = new Set([" in normalization_source
    assert "'bar3d'" in normalization_source
    assert "'sparkline'" in normalization_source
    assert "setTableCanvasSize((previousSize) => Math.max(DEFAULT_TABLE_CANVAS_SIZE" in component_source
    assert "const remeasure = () => {" in column_virtualizer_source
    assert "observer.observe(scrollEl);" in column_virtualizer_source
    assert "columnVirtualizer.measure();" in column_virtualizer_source
    assert "data-colid={col.id}" in Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useRenderHelpers.js",
        )
    ).read_text(encoding="utf-8")
    app_bar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "PivotAppBar.js",
        )
    ).read_text(encoding="utf-8")
    render_helper_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useRenderHelpers.js",
        )
    ).read_text(encoding="utf-8")
    formatting_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "utils",
            "formatting.js",
        )
    ).read_text(encoding="utf-8")
    assert "EDITED_CELL_FORMAT_KEY" in app_bar_source
    assert "Edited Cells" in app_bar_source
    assert "Save Edited Style" in app_bar_source
    assert "editedCellBg" in app_bar_source
    assert "editedCellBorder" in app_bar_source
    assert "editedCellText" in app_bar_source
    assert "Structure" in app_bar_source
    assert "Hierarchy Bg" in app_bar_source
    assert "Grand Total Bg" in app_bar_source
    assert "Grand Total Text" in app_bar_source
    assert "resolveEditedCellMarker" in render_helper_source
    assert "buildEditedCellVisualStyle" in render_helper_source
    assert "export const EDITED_CELL_FORMAT_KEY = '__edited_cells__';" in formatting_source
    assert "data-pivot-loading-indicator=\"global\"" in Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Table",
            "PivotTableBody.js",
        )
    ).read_text(encoding="utf-8")
    assert "data-pivot-loading-indicator=\"status\"" in Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Table",
            "StatusBar.js",
        )
    ).read_text(encoding="utf-8")


def test_frontend_toolbar_exposes_transaction_undo_redo_controls():
    app_bar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "PivotAppBar.js",
        )
    ).read_text(encoding="utf-8")

    assert "canUndoTransactions" in app_bar_source
    assert "canRedoTransactions" in app_bar_source
    assert "transactionHistoryPending" in app_bar_source
    assert "onUndoTransaction" in app_bar_source
    assert "onRedoTransaction" in app_bar_source
    assert "data-edit-value-mode=\"edited\"" in app_bar_source
    assert "data-edit-value-mode=\"original\"" in app_bar_source
    assert "aria-pressed={editValueDisplayMode === 'edited'}" in app_bar_source
    assert "aria-pressed={editValueDisplayMode === 'original'}" in app_bar_source
    assert "hasComparedValues" in app_bar_source
    assert "setEditValueDisplayMode" in app_bar_source
    assert "Undo the last edit or layout change (Ctrl/Cmd+Z)" in app_bar_source
    assert "Redo the last edit or layout change (Ctrl+Y or Cmd/Ctrl+Shift+Z)" in app_bar_source


def test_expansion_request_includes_anchor_block_from_overscan_boundary():
    source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")

    assert "viewportAlignedStart" in source
    assert "Math.min(viewportAlignedStart, anchorBlockHint * expansionBlockSize)" in source


def test_runtime_data_drop_resolves_pending_viewport_request():
    source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")

    assert "if (!runtimePayloadCommitIsCurrent(runtimeResponse, payload, resolvedPayload)) {" in source
    assert "resolvePendingRequest(runtimeResponse);" in source
    assert "if (!runtimePayloadCommitIsCurrent(runtimeResponse, payload, payload)) {" in source


def test_expansion_requests_preserve_latest_horizontal_window():
    source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    viewport_hook_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useServerSideViewportController.js",
        )
    ).read_text(encoding="utf-8")

    assert "latestRequestedColumnWindowRef" in source
    assert "resolveStableRequestedColumnWindow" in source
    assert "Math.min(...candidateStarts)" in viewport_hook_source
    assert "Math.max(...candidateEnds)" in viewport_hook_source


def test_structural_requests_restart_from_top_window_after_field_changes():
    source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")

    assert "const structuralWindowSize = Math.max(" in source
    assert "const structuralStart = 0;" in source
    assert "const structuralEnd = structuralWindowSize - 1;" in source


def test_view_state_restore_rehydrates_server_side_runtime_requests():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    callback_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "pivot_engine",
            "pivot_engine",
            "runtime",
            "dash_callbacks.py",
        )
    ).read_text(encoding="utf-8")

    assert "restored.expanded === true" in component_source
    assert "restored.expanded === false" in component_source
    assert "const buildRuntimeRequestStateOverride = useCallback" in component_source
    assert "state_override: requestStateOverride || undefined" in component_source
    assert "const dispatchServerSideRuntimeSetProps = useCallback" in component_source
    assert "setProps: dispatchServerSideRuntimeSetProps" in component_source
    assert "state_override: buildRuntimeRequestStateOverride() || undefined" in component_source
    assert "showSubtotals" in component_source
    assert "reportDef," in component_source
    assert "customDimensions" in component_source

    assert "_extract_request_state_override" in callback_source
    assert "request_state_override = (" in callback_source
    assert 'effective_trigger_prop.endswith(".runtimeRequest")' in callback_source
    assert '_state_override_value(request_state_override, "rowFields", row_fields or [], list)' in callback_source
    assert '_state_override_value(request_state_override, "customDimensions", custom_dimensions or [], list)' in callback_source
    assert '_state_override_value(request_state_override, "expanded", expanded, (dict, bool))' in callback_source
    assert '"showSubtotals"' in callback_source
    assert 'view_mode=effective_view_mode' in callback_source
    assert 'report_def=effective_report_def' in callback_source
    assert 'custom_dimensions=effective_custom_dimensions' in callback_source


def test_tabular_mode_repeats_labels_and_subtotal_toggle_is_exposed():
    column_defs_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnDefs.js",
        )
    ).read_text(encoding="utf-8")
    appbar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "PivotAppBar.js",
        )
    ).read_text(encoding="utf-8")
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    render_helpers_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useRenderHelpers.js",
        )
    ).read_text(encoding="utf-8")

    assert "rowSpan: false" in column_defs_source
    assert "rowSpan: effectiveLayoutMode === 'tabular'" not in column_defs_source
    assert "accessorFn: (rowData) => getTabularRowFieldValue(rowData, field, i)" in column_defs_source
    assert "getTabularPathFieldValue" in column_defs_source
    assert "preserveAccessorDisplayValue: effectiveLayoutMode === 'tabular'" in column_defs_source
    assert "const preserveAccessorDisplayValue = Boolean" in render_helpers_source
    assert "preserveAccessorDisplayValue" in render_helpers_source
    assert "_path.split('|||')" in column_defs_source
    assert "const displayValue = isBlankHierarchyValue(val)" in column_defs_source
    assert "showSubtotals" in appbar_source
    assert "Subtotals" in appbar_source
    assert "setShowSubtotals(prev => !prev)" in appbar_source
    assert "show_subtotal_footers: showSubtotals || undefined" in component_source
    assert "showSubtotals," in component_source
    assert "include_subtotals: layoutMode === 'tabular'" not in component_source
    assert "includeSubtotals: layoutMode !== 'tabular'" not in component_source


def test_client_side_custom_categories_materialize_before_filtering():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    normalization_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "usePivotNormalization.js",
        )
    ).read_text(encoding="utf-8")

    assert "export const evaluateCustomCategoryDimension" in normalization_source
    assert "export const evaluateCustomCategoryCondition" in normalization_source
    assert "export const applyCustomDimensionsToRows" in normalization_source
    assert "if (normalized === 'notin') return 'not_in';" in normalization_source
    assert "if (normalized === 'isnull') return 'is_null';" in normalization_source
    assert "clause.value.split(',')" in normalization_source
    assert "evaluateCustomCategoryDimension(nextRow, dimension)" in normalization_source
    assert "serverSide ? cleanData : applyCustomDimensionsToRows(cleanData, customDimensions)" in component_source
    assert "data={customAwareData}" in component_source
    assert "const filteredData = useFilteredData(customAwareData, filters, serverSide);" in component_source
    assert component_source.index("const filteredData = useFilteredData") < component_source.index("} = useServerSideViewportController({")
    assert "targetZone === 'vals' && isCustomCategoryField(fieldName)" in component_source


def test_custom_categories_are_selectable_report_dimensions():
    report_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "ReportEditor.js",
        )
    ).read_text(encoding="utf-8")

    assert "isCustomCategoryField" in report_source
    assert "!field.startsWith('_') || isCustomCategoryField(field)" in report_source


def test_report_rows_do_not_render_level_label_chips():
    column_defs_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useColumnDefs.js",
        )
    ).read_text(encoding="utf-8")

    assert "Report mode: show level label badge" not in column_defs_source
    assert "row.original._levelLabel" not in column_defs_source
    assert "Top {row.original._levelTopN}" in column_defs_source


def test_custom_category_editor_uses_modal_and_validates_rules():
    sidebar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "SidebarPanel.js",
        )
    ).read_text(encoding="utf-8")

    assert 'role="dialog"' in sidebar_source
    assert "getAllowedCustomCategoryDependencies" in sidebar_source
    assert "Rules can use base fields and categories defined before this one." in sidebar_source
    assert 'A category named "${normalized.name}" already exists.' in sidebar_source
    assert "does not match any current row" in sidebar_source
    assert "createCustomCategoryRule(conditionFieldOptions" in sidebar_source


def test_filter_open_requests_and_responses_are_idempotent():
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")
    filter_popover_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Filters",
            "FilterPopover.js",
        )
    ).read_text(encoding="utf-8")
    column_filter_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Filters",
            "ColumnFilter.js",
        )
    ).read_text(encoding="utf-8")
    date_filter_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Filters",
            "DateRangeFilter.js",
        )
    ).read_text(encoding="utf-8")
    numeric_filter_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Filters",
            "NumericRangeFilter.js",
        )
    ).read_text(encoding="utf-8")
    multi_select_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Filters",
            "MultiSelectFilter.js",
        )
    ).read_text(encoding="utf-8")
    sidebar_filter_item_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "SidebarFilterItem.js",
        )
    ).read_text(encoding="utf-8")
    sidebar_panel_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "SidebarPanel.js",
        )
    ).read_text(encoding="utf-8")

    assert "const handledRuntimeResponseKeysRef = useRef(new Set());" in component_source
    assert "const buildRuntimeResponseProcessingKey = useCallback" in component_source
    assert "const markRuntimeResponseHandled = useCallback" in component_source
    assert "if (responseProcessingKey && !markRuntimeResponseHandled(responseProcessingKey)) return;" in component_source
    assert "Object.prototype.hasOwnProperty.call(filterOptions || {}, columnId)" in component_source
    assert "pendingServerFilterOptionsRef.current = requestKey;" in component_source
    assert "const handleFilterClick = useCallback" in component_source
    assert "setFilterAnchorEl({" in component_source
    assert "getBoundingClientRect: () => ({" in component_source
    assert "requestFilterOptions(colId);" in component_source
    assert "previousOptions.every((value, index) => value === mergedOptions[index])" in component_source
    assert "const collectDataFieldIds = (rows) =>" in component_source
    assert "return collectDataFieldIds(data);" in component_source
    assert "const clientFilterOptionMap = useMemo" in component_source
    assert "buildClientFilterOptionMap(customAwareData, availableFieldsWithCustomDimensions)" in component_source
    assert "if (!serverSide && clientFilterOptionMap[activeFilterCol])" in component_source
    assert "const FILTER_OPTION_PAGE_SIZE = 250;" in component_source
    assert "setTransportFilterOptionMetaState" in component_source
    assert "payload: {" in component_source and "search," in component_source and "offset," in component_source
    assert "state_override: buildRuntimeRequestStateOverride() || undefined" in component_source

    assert "const columnPositionKey =" in filter_popover_source
    assert "[anchorEl, columnAnchorTarget, columnPositionKey]" in filter_popover_source
    assert "typeof target.getBoundingClientRect !== 'function'" in filter_popover_source
    assert "window.addEventListener('resize', updatePosition);" in filter_popover_source
    assert "window.addEventListener('scroll', updatePosition, true);" in filter_popover_source
    assert "background: resolvedTheme.surfaceBg" in filter_popover_source

    assert "const tabAutoInitialized = useRef(false);" in column_filter_source
    assert "const cloneFilterConditions = (filter)" in column_filter_source
    assert "filter.conditions.map(condition => ({ ...condition }))" in column_filter_source
    assert "conditionIndex === index ? { ...condition, [key]: value } : condition" in column_filter_source
    assert "const selectTab = (nextTab) =>" in column_filter_source
    assert "selectTab('condition')" in column_filter_source
    assert "if (tabAutoInitialized.current) return;" in column_filter_source
    assert "[options && options.length, isDate]" in column_filter_source
    assert "DateRangeFilter onFilter={onFilter} currentFilter={currentFilter} theme={resolvedTheme} onClose={onClose}" in column_filter_source
    assert "NumericRangeFilter onFilter={onFilter} currentFilter={currentFilter} theme={resolvedTheme} onClose={onClose}" in column_filter_source
    assert "formatLocalDate" in date_filter_source
    assert "toISOString().split('T')[0]" not in date_filter_source
    assert "onClose) onClose();" in date_filter_source
    assert "Number.isFinite(numericValue)" in numeric_filter_source
    assert "Min must be less than or equal to max." in numeric_filter_source
    assert "onClose) onClose();" in numeric_filter_source
    assert "onSearchOptions={onSearchOptions}" in column_filter_source
    assert "const LOCAL_RENDER_LIMIT = 250;" in multi_select_source
    assert "onSearchOptions(search);" in multi_select_source
    assert "onLoadMoreOptions(search, loadedCount);" in multi_select_source
    assert "Showing first {LOCAL_RENDER_LIMIT}" in multi_select_source
    assert "clientFilterOptionMap && clientFilterOptionMap[columnId]" in sidebar_panel_source
    assert "const closeFilterEditor = () => setExpanded(false);" in sidebar_filter_item_source
    assert "onClose={closeFilterEditor}" in sidebar_filter_item_source


def test_sidebar_group_selection_uses_leaf_selection_state():
    column_tree_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "ColumnTreeItem.js",
        )
    ).read_text(encoding="utf-8")

    assert "const selectionIds = isGroup ? getAllLeafIdsFromColumn(column) : [column.id];" in column_tree_source
    assert "selectionIds.every(id => selectedCols.has(id))" in column_tree_source
    assert "selectionIds.some(id => selectedCols.has(id))" in column_tree_source
    assert "checkboxRef.current.indeterminate = isPartiallySelected;" in column_tree_source
    assert "onClick={(e) => e.stopPropagation()}" in column_tree_source
    assert "onClick={(e) => { e.stopPropagation(); toggleSelection(e); }}" not in column_tree_source


def test_row_cache_merges_rows_by_stable_identity_when_column_windows_shift():
    source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useRowCache.js",
        )
    ).read_text(encoding="utf-8")

    assert "getRowMergeKey" in source
    assert "previousByKey" in source
    assert "rows: mergedRows" in source


def test_frontend_resilience_source_handles_reset_jank_cell_errors_and_keyboard_dnd():
    row_model_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useServerSideRowModel.js",
        )
    ).read_text(encoding="utf-8")
    render_helpers_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "hooks",
            "useRenderHelpers.js",
        )
    ).read_text(encoding="utf-8")
    sidebar_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "Sidebar",
            "SidebarPanel.js",
        )
    ).read_text(encoding="utf-8")
    component_source = Path(
        os.path.join(
            os.getcwd(),
            "dash_tanstack_pivot",
            "src",
            "lib",
            "components",
            "DashTanstackPivot.react.js",
        )
    ).read_text(encoding="utf-8")

    assert "lifecycleResetRef" in row_model_source
    assert "resetViewportRequestState" in row_model_source
    assert "resolveRetentionBufferBlocks" in row_model_source
    assert "recentFastScroll" in row_model_source
    assert "hasFreshInflight" in row_model_source
    assert "const retentionBufferBlocks = resolveRetentionBufferBlocks(2);" in row_model_source
    assert "const blockNeedsViewportRequest = useCallback" in row_model_source
    assert "const dispatchBlocksNeeded = [...new Set(blocksNeeded)]" in row_model_source
    assert "debugLog('skip-obsolete-viewport'" in row_model_source
    assert "blocksNeeded: dispatchBlocksNeeded" in row_model_source

    assert "class CellErrorBoundary extends React.Component" in render_helpers_source
    assert "data-pivot-cell-error=\"true\"" in render_helpers_source
    assert "function SafeCellContent({ render })" in render_helpers_source
    assert "render={() => flexRender(col.columnDef.cell, displayCellLike.getContext())}" in render_helpers_source
    assert "import { useCallback, useEffect, useMemo, useRef } from 'react';" in render_helpers_source
    assert "useEffect(() => { leafColumnsCacheRef.current = new Map(); }" in render_helpers_source
    assert "useMemo(() => { leafColumnsCacheRef.current = new Map(); }" not in render_helpers_source

    assert "const valueSelectStyle = React.useMemo" in sidebar_source
    assert "const handleValueWindowFnChange = React.useCallback" in sidebar_source
    assert "windowFn: nextWindowFn === 'none' ? null : nextWindowFn" in sidebar_source
    assert ".windowFn=e.target.value" not in sidebar_source
    assert "const zoneDescriptors = React.useMemo" in sidebar_source
    assert "const zoneItemsById = React.useMemo" in sidebar_source
    assert "keyboardDragItem" in sidebar_source
    assert "onKeyboardFieldDrop" in sidebar_source
    assert "data-sidebar-field-chip=\"available\"" in sidebar_source
    assert "data-sidebar-drop-zone={zone.id}" in sidebar_source
    assert "aria-grabbed=" in sidebar_source
    assert "handleDraggableKeyDown" in sidebar_source
    assert "handleDropZoneKeyDown" in sidebar_source

    assert "const applyFieldZoneMove = useCallback" in component_source
    assert "source: 'layout:keyboard-drop'" in sidebar_source
    assert "onKeyboardFieldDrop={applyFieldZoneMove}" in component_source


@pytest.mark.asyncio
async def test_virtual_scroll_reuses_cached_adjacent_row_blocks(adapter, monkeypatch):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "sales"},
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["sales", "region", "country"],
        aggregations=[],
        totals=False,
    )

    original_run_hierarchy_view = adapter.controller.run_hierarchy_view
    call_ranges = []

    async def counting_run_hierarchy_view(*args, **kwargs):
        call_ranges.append((args[2], args[3]))
        return await original_run_hierarchy_view(*args, **kwargs)

    monkeypatch.setattr(adapter.controller, "run_hierarchy_view", counting_run_hierarchy_view)

    await adapter.handle_virtual_scroll_request(
        request, 0, 99, [], needs_col_schema=False, include_grand_total=False
    )
    await adapter.handle_virtual_scroll_request(
        request, 100, 199, [], needs_col_schema=False, include_grand_total=False
    )
    assembled = await adapter.handle_virtual_scroll_request(
        request, 0, 199, [], needs_col_schema=False, include_grand_total=False
    )

    assert call_ranges == [(0, 99), (100, 199)]
    assert len(assembled.data) >= 200


@pytest.mark.asyncio
async def test_virtual_scroll_fetches_only_missing_row_blocks_when_adjacent_blocks_cached(adapter, monkeypatch):
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "sales"},
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={},
        sorting=[],
        grouping=["sales", "region", "country"],
        aggregations=[],
        totals=False,
    )

    original_run_hierarchy_view = adapter.controller.run_hierarchy_view
    call_ranges = []

    async def counting_run_hierarchy_view(*args, **kwargs):
        call_ranges.append((args[2], args[3]))
        return await original_run_hierarchy_view(*args, **kwargs)

    monkeypatch.setattr(adapter.controller, "run_hierarchy_view", counting_run_hierarchy_view)

    await adapter.handle_virtual_scroll_request(
        request, 0, 99, [], needs_col_schema=False, include_grand_total=False
    )
    await adapter.handle_virtual_scroll_request(
        request, 200, 299, [], needs_col_schema=False, include_grand_total=False
    )
    assembled = await adapter.handle_virtual_scroll_request(
        request, 0, 299, [], needs_col_schema=False, include_grand_total=False
    )

    assert call_ranges == [(0, 99), (200, 299), (100, 199)]
    assert len(assembled.data) >= 300
