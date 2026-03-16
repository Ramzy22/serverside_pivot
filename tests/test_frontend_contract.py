
import pytest
import asyncio
import pyarrow as pa
import sys
import os

# Add root and pivot_engine source to path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'pivot_engine'))
sys.path.append(os.path.join(os.getcwd(), 'dash_tanstack_pivot'))

from pivot_engine.tanstack_adapter import create_tanstack_adapter, TanStackRequest, TanStackOperation

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
async def test_col_schema_sentinel_present_on_needs_col_schema(adapter):
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
    assert len(schema["columns"]) == schema["total_center_cols"]

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
    assert with_total.total_rows == no_total.total_rows


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
    """total_center_cols in col_schema must equal the number of distinct center columns
    in a full (non-windowed) response."""
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

    assert full.col_schema["total_center_cols"] == len(observed_center_ids), (
        f"col_schema.total_center_cols={full.col_schema['total_center_cols']} "
        f"but observed {len(observed_center_ids)} center columns in data"
    )


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
    last_col_id = schema_resp.col_schema["columns"][-1]["id"]

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
