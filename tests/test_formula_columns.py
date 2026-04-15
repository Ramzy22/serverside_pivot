import os
import sys

import pyarrow as pa
import pytest

sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "pivot_engine"))
sys.path.append(os.path.join(os.getcwd(), "dash_tanstack_pivot"))

from pivot_engine.runtime import PivotRuntimeService
from pivot_engine.tanstack_adapter import (  # noqa: E402
    TanStackOperation,
    TanStackRequest,
    create_tanstack_adapter,
)


def test_runtime_service_build_request_columns_preserves_formula_metadata():
    columns = PivotRuntimeService._build_request_columns(
        row_fields=["region"],
        col_fields=["product"],
        val_configs=[
            {"field": "sales", "agg": "sum"},
            {
                "field": "formula_1",
                "agg": "formula",
                "label": "Sales x100",
                "formula": "sales * 100",
                "formulaRef": "salesx100",
            },
        ],
    )

    formula_column = next(column for column in columns if column.get("isFormula"))

    assert formula_column == {
        "id": "formula_1",
        "header": "Sales x100",
        "accessorKey": "formula_1",
        "formulaExpr": "sales * 100",
        "formulaRef": "salesx100",
        "formulaLabel": "Sales x100",
        "isFormula": True,
    }


def test_runtime_service_build_request_columns_generates_formula_ref_from_label_when_missing():
    columns = PivotRuntimeService._build_request_columns(
        row_fields=["region"],
        col_fields=[],
        val_configs=[
            {"field": "sales", "agg": "sum"},
            {
                "field": "formula_1",
                "agg": "formula",
                "label": "Formula 1",
                "formula": "sales * 100",
            },
        ],
    )

    formula_column = next(column for column in columns if column.get("isFormula"))
    assert formula_column["formulaRef"] == "formula1"


def test_runtime_service_build_request_columns_preserves_column_formula_scope():
    columns = PivotRuntimeService._build_request_columns(
        row_fields=["region"],
        col_fields=["product"],
        val_configs=[
            {"field": "sales", "agg": "sum"},
            {
                "field": "formula_1",
                "agg": "formula",
                "label": "Laptop minus Phone",
                "formula": "[Laptop_sales_sum] - [Phone_sales_sum]",
                "formulaScope": "columns",
            },
        ],
    )

    formula_column = next(column for column in columns if column.get("isFormula"))
    assert formula_column["formulaScope"] == "columns"
    assert formula_column["formulaExpr"] == "[Laptop_sales_sum] - [Phone_sales_sum]"


@pytest.mark.asyncio
async def test_formula_columns_are_applied_in_flat_mode():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "South"],
                "sales": [100, 200],
                "cost": [80, 150],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)

    rows = {
        row["region"]: row
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    }

    assert rows["North"]["formula_1"] == pytest.approx(20.0)
    assert rows["South"]["formula_1"] == pytest.approx(50.0)
    assert any(
        column.get("id") == "formula_1" and column.get("header") == "Margin"
        for column in response.columns
        if isinstance(column, dict)
    )


@pytest.mark.asyncio
async def test_formula_columns_are_dynamic_in_pivot_mode_without_placeholder_columns():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "product": ["Laptop", "Phone", "Laptop", "Phone"],
                "sales": [100, 60, 90, 50],
                "cost": [70, 20, 30, 10],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)

    north_row = next(
        row for row in response.data
        if isinstance(row, dict) and row.get("region") == "North" and not row.get("_isTotal")
    )

    assert north_row["Laptop_formula_1"] == pytest.approx(30.0)
    assert north_row["Phone_formula_1"] == pytest.approx(40.0)

    response_ids = {
        column.get("id")
        for column in response.columns
        if isinstance(column, dict)
    }

    assert "formula_1" not in response_ids
    assert "Laptop_formula_1" in response_ids
    assert any(
        column.get("id") == "Laptop_formula_1" and column.get("header") == "Margin"
        for column in response.columns
        if isinstance(column, dict)
    )


@pytest.mark.asyncio
async def test_column_scoped_formula_uses_materialized_pivot_columns():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "product": ["Laptop", "Phone", "Laptop", "Phone"],
                "sales": [100, 60, 90, 50],
                "cost": [70, 20, 30, 10],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Laptop minus Phone",
                "accessorKey": "formula_1",
                "formulaExpr": "[Laptop_sales_sum] - [Phone_sales_sum]",
                "formulaLabel": "Laptop minus Phone",
                "formulaScope": "columns",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)
    north_row = next(
        row for row in response.data
        if isinstance(row, dict) and row.get("region") == "North" and not row.get("_isTotal")
    )

    assert north_row["formula_1"] == pytest.approx(40.0)
    assert "Laptop_formula_1" not in north_row
    assert "Phone_formula_1" not in north_row

    response_ids = [
        column.get("id")
        for column in response.columns
        if isinstance(column, dict)
    ]
    assert "formula_1" in response_ids
    assert "Laptop_formula_1" not in response_ids
    assert "Phone_formula_1" not in response_ids


@pytest.mark.asyncio
async def test_column_scoped_formula_survives_virtual_scroll_schema_window():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "product": ["Laptop", "Phone", "Laptop", "Phone"],
                "sales": [100, 60, 90, 50],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Laptop minus Phone",
                "accessorKey": "formula_1",
                "formulaExpr": "[Laptop_sales_sum] - [Phone_sales_sum]",
                "formulaLabel": "Laptop minus Phone",
                "formulaScope": "columns",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        totals=True,
        row_totals=True,
    )

    response = await adapter.handle_virtual_scroll_request(
        request,
        start_row=0,
        end_row=10,
        expanded_paths=True,
        col_start=0,
        col_end=20,
        needs_col_schema=True,
        include_grand_total=True,
    )

    schema_ids = [
        column.get("id")
        for column in (response.col_schema or {}).get("columns", [])
        if isinstance(column, dict)
    ]
    assert "formula_1" in schema_ids
    assert "Laptop_formula_1" not in schema_ids

    north_row = next(
        row for row in response.data
        if isinstance(row, dict) and row.get("region") == "North" and not row.get("_isTotal")
    )
    assert north_row["formula_1"] == pytest.approx(40.0)


@pytest.mark.asyncio
async def test_invalid_formula_evaluates_to_none_without_crashing():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "South"],
                "sales": [100, 200],
                "cost": [80, 150],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Broken",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - unknown_field",
                "formulaLabel": "Broken",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)

    rows = [
        row for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ]

    assert rows
    assert all(row.get("formula_1") is None for row in rows)


@pytest.mark.asyncio
async def test_formula_columns_can_reference_other_formulas_out_of_order():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "South"],
                "sales": [100, 200],
                "cost": [80, 150],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_2",
                "header": "Margin %",
                "accessorKey": "formula_2",
                "formulaExpr": "(margin / sales) * 100",
                "formulaRef": "margin_pct",
                "formulaLabel": "Margin %",
                "isFormula": True,
            },
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaRef": "margin",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)

    rows = {
        row["region"]: row
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    }

    assert rows["North"]["formula_1"] == pytest.approx(20.0)
    assert rows["North"]["formula_2"] == pytest.approx(20.0)
    assert rows["South"]["formula_1"] == pytest.approx(50.0)
    assert rows["South"]["formula_2"] == pytest.approx(25.0)


@pytest.mark.asyncio
async def test_formula_columns_can_reference_normalized_label_alias_when_formula_ref_missing():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "South"],
                "sales": [100, 200],
                "cost": [80, 150],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_2",
                "header": "Formula 2",
                "accessorKey": "formula_2",
                "formulaExpr": "formula1 * 2",
                "formulaLabel": "Formula 2",
                "isFormula": True,
            },
            {
                "id": "formula_1",
                "header": "Formula 1",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaLabel": "Formula 1",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)

    rows = {
        row["region"]: row
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    }

    assert rows["North"]["formula_2"] == pytest.approx(40.0)
    assert rows["South"]["formula_2"] == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_pivot_formula_columns_can_reference_other_formulas_by_alias():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "product": ["Laptop", "Phone", "Laptop", "Phone"],
                "sales": [100, 60, 90, 50],
                "cost": [70, 20, 30, 10],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_2",
                "header": "Margin %",
                "accessorKey": "formula_2",
                "formulaExpr": "(margin / sales) * 100",
                "formulaRef": "margin_pct",
                "formulaLabel": "Margin %",
                "isFormula": True,
            },
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaRef": "margin",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)

    north_row = next(
        row for row in response.data
        if isinstance(row, dict) and row.get("region") == "North" and not row.get("_isTotal")
    )

    assert north_row["Laptop_formula_1"] == pytest.approx(30.0)
    assert north_row["Laptop_formula_2"] == pytest.approx(30.0)
    assert north_row["Phone_formula_1"] == pytest.approx(40.0)
    assert north_row["Phone_formula_2"] == pytest.approx((40.0 / 60.0) * 100)


@pytest.mark.asyncio
async def test_pivot_formula_columns_follow_requested_measure_order_through_windowing():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "North", "South", "South", "South"],
                "product": ["Laptop", "Phone", "Tablet", "Laptop", "Phone", "Tablet"],
                "sales": [100, 110, 120, 90, 95, 105],
                "cost": [60, 70, 80, 50, 55, 65],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "product"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    structural_response = await adapter.handle_virtual_scroll_request(
        request,
        0,
        10,
        [],
        needs_col_schema=True,
    )

    assert structural_response.col_schema is not None
    schema_ids = [
        column["id"]
        for column in structural_response.col_schema["columns"]
        if isinstance(column, dict)
    ]
    prefix_order = [
        column_id[: -len("_sales_sum")]
        for column_id in schema_ids
        if column_id.endswith("_sales_sum")
    ]
    expected_schema_ids = []
    for prefix in prefix_order:
        expected_schema_ids.extend(
            [f"{prefix}_sales_sum", f"{prefix}_cost_sum", f"{prefix}_formula_1"]
        )

    emitted_center_ids = [
        column["id"]
        for column in (structural_response.columns or [])
        if isinstance(column, dict) and column.get("id") != "region"
    ]

    assert schema_ids == expected_schema_ids
    assert emitted_center_ids == expected_schema_ids

    first_window = await adapter.handle_virtual_scroll_request(
        request,
        0,
        10,
        [],
        col_start=0,
        col_end=2,
        needs_col_schema=False,
    )

    north_row = next(
        row
        for row in first_window.data
        if isinstance(row, dict)
        and row.get("region") == "North"
        and not row.get("_isTotal")
    )
    row_meta_keys = {"region", "_id", "depth", "_path", "_isTotal"}
    first_window_ids = [key for key in north_row.keys() if key not in row_meta_keys]

    assert first_window_ids == expected_schema_ids[:3]
    assert first_window_ids[-1].endswith("_formula_1")
    assert north_row[first_window_ids[-1]] == pytest.approx(40.0)


@pytest.mark.asyncio
async def test_pivot_formula_totals_sum_materialized_formula_values():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "country": ["France", "Germany", "France", "Germany"],
                "sales": [100, 100, 80, 120],
                "cost": [60, 80, 20, 90],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin %",
                "accessorKey": "formula_1",
                "formulaExpr": "(sales - cost) / sales",
                "formulaLabel": "Margin %",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        totals=True,
        row_totals=True,
    )

    response = await adapter.handle_virtual_scroll_request(
        request,
        0,
        10,
        [],
        needs_col_schema=True,
        include_grand_total=True,
    )

    schema_ids = [
        column["id"]
        for column in response.col_schema["columns"]
        if isinstance(column, dict)
    ]
    assert "__RowTotal__formula_1" in schema_ids

    north_row = next(
        row
        for row in response.data
        if isinstance(row, dict)
        and row.get("region") == "North"
        and not row.get("_isTotal")
    )
    grand_total_row = next(
        row
        for row in response.data
        if isinstance(row, dict) and row.get("_isTotal")
    )

    # Row totals should sum the rendered pivot formula cells.
    assert north_row["France_formula_1"] == pytest.approx(0.4)
    assert north_row["Germany_formula_1"] == pytest.approx(0.2)
    assert north_row["__RowTotal__formula_1"] == pytest.approx(0.6)

    # Grand totals should also sum the displayed formula values down the column.
    assert grand_total_row["France_formula_1"] == pytest.approx(1.15)
    assert grand_total_row["Germany_formula_1"] == pytest.approx(0.45)
    assert grand_total_row["__RowTotal__formula_1"] == pytest.approx(1.6)


@pytest.mark.asyncio
async def test_formula_sorting_works_in_flat_mode():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "South", "East"],
                "sales": [100, 200, 150],
                "cost": [80, 120, 140],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[{"id": "formula_1", "desc": True}],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)

    ordered_regions = [
        row["region"]
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ]
    assert ordered_regions == ["South", "North", "East"]


@pytest.mark.asyncio
async def test_virtual_scroll_formula_sorting_works_for_materialized_pivot_columns():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South", "East", "East"],
                "country": ["France", "Germany", "France", "Germany", "France", "Germany"],
                "sales": [100, 100, 70, 120, 40, 80],
                "cost": [60, 90, 20, 95, 10, 70],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[{"id": "France_formula_1", "desc": True}],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_virtual_scroll_request(
        request,
        0,
        10,
        [],
        needs_col_schema=True,
        include_grand_total=True,
    )

    ordered_regions = [
        row["region"]
        for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ]
    assert ordered_regions == ["South", "North", "East"]


@pytest.mark.asyncio
async def test_virtual_scroll_grand_total_formula_uses_full_top_level_rollup_not_window_subset():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "North", "South", "South"],
                "country": ["France", "Germany", "France", "Germany"],
                "sales": [100, 100, 80, 120],
                "cost": [60, 80, 20, 90],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin %",
                "accessorKey": "formula_1",
                "formulaExpr": "(sales - cost) / sales",
                "formulaLabel": "Margin %",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        totals=True,
        row_totals=True,
    )

    response = await adapter.handle_virtual_scroll_request(
        request,
        0,
        0,
        [],
        needs_col_schema=True,
        include_grand_total=True,
    )

    rows = [row for row in response.data if isinstance(row, dict)]
    assert len([row for row in rows if not row.get("_isTotal")]) == 1

    grand_total_row = next(
        row for row in rows
        if row.get("_isTotal")
    )

    assert grand_total_row["France_formula_1"] == pytest.approx(1.15)
    assert grand_total_row["Germany_formula_1"] == pytest.approx(0.45)
    assert grand_total_row["__RowTotal__formula_1"] == pytest.approx(1.6)


@pytest.mark.asyncio
async def test_circular_formula_references_evaluate_to_none_without_crashing():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["North", "South"],
                "sales": [100, 200],
            }
        ),
    )

    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Cycle A",
                "accessorKey": "formula_1",
                "formulaExpr": "cycle_b + sales",
                "formulaRef": "cycle_a",
                "formulaLabel": "Cycle A",
                "isFormula": True,
            },
            {
                "id": "formula_2",
                "header": "Cycle B",
                "accessorKey": "formula_2",
                "formulaExpr": "cycle_a * 2",
                "formulaRef": "cycle_b",
                "formulaLabel": "Cycle B",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
    )

    response = await adapter.handle_request(request)

    rows = [
        row for row in response.data
        if isinstance(row, dict) and not row.get("_isTotal")
    ]

    assert rows
    assert all(row.get("formula_1") is None for row in rows)
    assert all(row.get("formula_2") is None for row in rows)


@pytest.mark.asyncio
async def test_structural_formula_response_keeps_columns_and_col_schema_in_sync():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow(
        "sales_data",
        pa.Table.from_pydict(
            {
                "region": ["East", "East", "West", "West"],
                "country": ["France", "Germany", "Italy", "Spain"],
                "sales": [100, 110, 120, 130],
                "cost": [70, 80, 90, 95],
            }
        ),
    )

    initial_request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaRef": "margin",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        totals=True,
    )

    # Prime the adapter the same way the UI does before a structural row-field change.
    await adapter.handle_virtual_scroll_request(
        initial_request,
        0,
        10,
        [],
        needs_col_schema=True,
        include_grand_total=True,
    )

    changed_request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales_data",
        columns=[
            {"id": "region"},
            {"id": "country"},
            {"id": "sales_sum", "aggregationField": "sales", "aggregationFn": "sum"},
            {"id": "cost_sum", "aggregationField": "cost", "aggregationFn": "sum"},
            {
                "id": "formula_1",
                "header": "Margin",
                "accessorKey": "formula_1",
                "formulaExpr": "sales - cost",
                "formulaRef": "margin",
                "formulaLabel": "Margin",
                "isFormula": True,
            },
        ],
        filters={},
        sorting=[],
        grouping=["region", "country"],
        aggregations=[],
        totals=True,
    )

    structural_response = await adapter.handle_virtual_scroll_request(
        changed_request,
        0,
        10,
        [],
        needs_col_schema=True,
        include_grand_total=True,
    )

    assert structural_response.col_schema is not None

    schema_ids = [
        column["id"]
        for column in structural_response.col_schema["columns"]
        if isinstance(column, dict)
    ]
    emitted_center_ids = [
        column["id"]
        for column in (structural_response.columns or [])
        if isinstance(column, dict)
        and column.get("id") not in {"region", "country"}
    ]

    assert emitted_center_ids == schema_ids
    assert schema_ids == ["sales_sum", "cost_sum", "formula_1"]

    east_row = next(
        row for row in structural_response.data
        if isinstance(row, dict)
        and row.get("region") == "East"
        and not row.get("_isTotal")
    )
    assert east_row["formula_1"] == pytest.approx(60.0)

    windowed_response = await adapter.handle_virtual_scroll_request(
        changed_request,
        0,
        10,
        [],
        col_start=0,
        col_end=schema_ids.index("formula_1"),
        needs_col_schema=False,
        include_grand_total=True,
    )

    windowed_row = next(
        row for row in windowed_response.data
        if isinstance(row, dict)
        and row.get("region") == "East"
        and not row.get("_isTotal")
    )
    assert windowed_row["formula_1"] == pytest.approx(60.0)
