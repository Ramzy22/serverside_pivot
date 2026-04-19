
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

# Reuse the data generation logic
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


@pytest.mark.asyncio
async def test_server_filter_options_are_paged_searchable_and_need_no_developer_supplied_values(adapter):
    rows = 650
    table = pa.Table.from_pydict(
        {
            "code": [f"code-{idx:03d}" for idx in range(rows)],
            "region": ["North" if idx % 2 == 0 else "South" for idx in range(rows)],
            "sales": list(range(rows)),
        }
    )
    adapter.controller.load_data_from_arrow("complete_filter_data", table)

    request = TanStackRequest(
        operation=TanStackOperation.GET_UNIQUE_VALUES,
        table="complete_filter_data",
        columns=[],
        filters={
            "code": {
                "operator": "AND",
                "conditions": [{"type": "in", "value": ["code-001"]}],
            }
        },
        sorting=[],
        grouping=[],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 250, "offset": 0},
        global_filter="code",
    )

    response = await adapter.handle_request(request)

    options = [row["value"] for row in response.data]
    assert len(options) == 250
    assert options[0] == "code-000"
    assert options[-1] == "code-249"
    assert response.pagination["totalRows"] == rows
    assert response.pagination["hasMore"] is True

    next_page_request = TanStackRequest(
        operation=TanStackOperation.GET_UNIQUE_VALUES,
        table="complete_filter_data",
        columns=[],
        filters={},
        sorting=[],
        grouping=[],
        aggregations=[],
        pagination={"pageIndex": 1, "pageSize": 250, "offset": 250},
        global_filter="code",
    )
    next_page = await adapter.handle_request(next_page_request)
    next_options = [row["value"] for row in next_page.data]
    assert next_options[0] == "code-250"
    assert next_options[-1] == "code-499"

    search_request = TanStackRequest(
        operation=TanStackOperation.GET_UNIQUE_VALUES,
        table="complete_filter_data",
        columns=[],
        filters={},
        sorting=[],
        grouping=[],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 250, "offset": 0, "search": "64"},
        global_filter="code",
    )
    search_response = await adapter.handle_request(search_request)
    search_options = [row["value"] for row in search_response.data]
    assert "code-064" in search_options
    assert "code-640" in search_options
    assert search_response.pagination["hasMore"] is False


@pytest.mark.asyncio
async def test_server_filter_options_include_custom_category_fields(adapter):
    table = pa.Table.from_pydict(
        {
            "region": ["North", "South", "East", "West"],
            "sales": [100, 200, 300, 400],
        }
    )
    adapter.controller.load_data_from_arrow("custom_category_filter_data", table)
    category_field = "__custom_category__market_bucket"

    request = TanStackRequest(
        operation=TanStackOperation.GET_UNIQUE_VALUES,
        table="custom_category_filter_data",
        columns=[],
        filters={},
        sorting=[],
        grouping=[],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 250, "offset": 0},
        global_filter=category_field,
        custom_dimensions=[
            {
                "id": "market_bucket",
                "field": category_field,
                "name": "Market Bucket",
                "fallbackLabel": "Other Markets",
                "rules": [
                    {
                        "id": "north_south",
                        "label": "Core Markets",
                        "condition": {
                            "op": "OR",
                            "clauses": [
                                {"field": "region", "operator": "eq", "value": "North"},
                                {"field": "region", "operator": "eq", "value": "South"},
                            ],
                        },
                    }
                ],
            }
        ],
    )

    response = await adapter.handle_request(request)
    options = [row["value"] for row in response.data]

    assert options == ["Core Markets", "Other Markets"]

@pytest.mark.asyncio
async def test_floating_filter_backend_logic(adapter):
    """
    Verify that the backend correctly handles the filter structure generated 
    by the floating filter inputs (simple 'contains' filter).
    """
    # Simulate the filter object created by the floating filter input:
    # onChange={e => handleHeaderFilter(column.id, {
    #     operator: 'AND',
    #     conditions: [{ type: 'contains', value: e.target.value, caseSensitive: false }]
    # })}
    
    filter_value = "North"
    tanstack_filter = {
        "region": {
            "operator": "AND", 
            "conditions": [{ "type": "contains", "value": filter_value, "caseSensitive": False }]
        }
    }

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[{"id": "region"}, {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"}],
        filters=tanstack_filter,
        sorting=[],
        grouping=["region"],
        aggregations=[]
    )
    
    response = await adapter.handle_request(request)
    
    # Assert that all returned rows match the filter
    for row in response.data:
        if not row.get('_isTotal'):
            assert filter_value in row['region'], f"Row {row} should contain {filter_value}"
    
    # Assert that we filtered out other regions
    regions = {r['region'] for r in response.data if not r.get('_isTotal')}
    assert "South" not in regions
    assert "East" not in regions
    assert "West" not in regions


@pytest.mark.asyncio
async def test_filter_and_sort_state_survives_repeated_requests(adapter):
    """Repeated requests should preserve the same filter and sort intent."""
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        filters={
            "country": {
                "operator": "AND",
                "conditions": [{"type": "contains", "value": "a", "caseSensitive": False}],
            }
        },
        sorting=[{"id": "sales_sum", "desc": True}],
        grouping=["country"],
        aggregations=[],
    )

    first_response = await adapter.handle_request(request)
    second_response = await adapter.handle_request(request)

    for response in (first_response, second_response):
        rows = [row for row in response.data if not row.get('_isTotal')]
        assert rows
        assert all("a" in row["country"].lower() for row in rows)

        values = [row["sales_sum"] for row in rows if row["sales_sum"] is not None]
        assert values == sorted(values, reverse=True)


def load_filter_sequence_fixture(adapter):
    table = pa.Table.from_pydict(
        {
            "region": ["North", "North", "North", "South", "South", "South"],
            "country": ["USA", "Canada", "Mexico", "Brazil", "Argentina", "Chile"],
            "product": ["Laptop", "Phone", "Tablet", "Laptop", "Phone", "Tablet"],
            "sales": [100, 80, 60, 200, 150, 120],
            "cost": [10, 8, 6, 20, 15, 12],
            "date": ["2023-01-01"] * 6,
        }
    )
    adapter.controller.load_data_from_arrow("sales_data", table)


def visible_paths(response):
    return [row["_path"] for row in response.data]


@pytest.mark.asyncio
async def test_virtual_scroll_request_state_updates_cleanly_after_filter_and_sort_change(adapter):
    """A scroll request followed by filter/sort changes should return the new visible state without stale rows."""
    load_filter_sequence_fixture(adapter)

    base_request = dict(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
        ],
        grouping=["region", "country"],
        aggregations=[],
    )

    initial_request = TanStackRequest(
        filters={},
        sorting=[{"id": "sales_sum", "desc": True}],
        **base_request,
    )
    updated_request = TanStackRequest(
        filters={
            "region": {
                "operator": "AND",
                "conditions": [{"type": "eq", "value": "South"}],
            }
        },
        sorting=[{"id": "sales_sum", "desc": False}],
        **base_request,
    )

    initial_window = await adapter.handle_virtual_scroll_request(initial_request, 0, 10, [["South"]])
    updated_window = await adapter.handle_virtual_scroll_request(updated_request, 0, 10, [["South"]])
    repeated_updated_window = await adapter.handle_virtual_scroll_request(updated_request, 0, 10, [["South"]])

    assert any(path.startswith("North") for path in visible_paths(initial_window))

    for response in (updated_window, repeated_updated_window):
        assert response.total_rows >= len(response.data)
        assert visible_paths(response)
        assert len(visible_paths(response)) == len(set(visible_paths(response)))

        rows = [row for row in response.data if not row.get("_isTotal")]
        assert rows
        assert all(row["region"] == "South" for row in rows)

        child_rows = [row for row in rows if row.get("country")]
        child_sales = [row["sales_sum"] for row in child_rows]
        assert child_sales == sorted(child_sales)
        assert visible_paths(response) == ["South", "South|||Chile", "South|||Argentina", "South|||Brazil", "__grand_total__"]

    assert visible_paths(updated_window) == visible_paths(repeated_updated_window)
    assert updated_window.total_rows == repeated_updated_window.total_rows
