"""
RED-phase tests for the drill-through backend data layer used by the export
feature (EXPORT-02 through EXPORT-05) and drill-through pagination/filter
(DRILL-04, DRILL-06 at the Python controller level).

Note: SheetJS-based xlsx/csv export logic (EXPORT-02/03/04/05 JS helpers) is
not directly testable in pytest.  These tests validate the Python layer:
  - get_drill_through_data() pagination correctness (DRILL-04)
  - get_drill_through_data() coordinate-filter correctness (DRILL-06)
  - Placeholder for sort support (DRILL-05 — skipped until Plan 02)

All tests use a small in-memory DuckDB table (20 rows) created via PyArrow so
they run in milliseconds without the 2M-row simulation dataset.
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pivot_engine')))

import asyncio
import pytest
import pyarrow as pa

from pivot_engine.pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.pivot_engine.types.pivot_spec import PivotSpec, Measure


# ---------------------------------------------------------------------------
# Module-scoped fixture: small in-memory controller
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def controller_with_data():
    """Return a ScalablePivotController loaded with a 20-row in-memory table."""
    ctrl = ScalablePivotController()

    # Build a small deterministic dataset
    regions = ['North', 'North', 'North', 'South', 'South',
               'North', 'North', 'South', 'South', 'North',
               'North', 'North', 'South', 'South', 'North',
               'North', 'South', 'South', 'North', 'South']
    countries = ['USA', 'USA', 'Canada', 'USA', 'USA',
                 'Canada', 'USA', 'Canada', 'USA', 'USA',
                 'USA', 'Canada', 'USA', 'Canada', 'USA',
                 'USA', 'USA', 'Canada', 'Canada', 'USA']
    sales = list(range(100, 120))  # 100..119

    table = pa.table({
        'region': pa.array(regions, type=pa.string()),
        'country': pa.array(countries, type=pa.string()),
        'sales': pa.array(sales, type=pa.int64()),
    })

    ctrl.load_data_from_arrow('test_sales', table)
    yield ctrl


def _make_spec(table: str = 'test_sales') -> PivotSpec:
    """Minimal PivotSpec with no measures/rows/filters so get_drill_through_data
    queries the raw table."""
    return PivotSpec(
        table=table,
        rows=[],
        measures=[Measure(field='sales', agg='sum', alias='sales_sum')],
        filters=[],
    )


# ---------------------------------------------------------------------------
# DRILL-04 (Python layer): Pagination returns non-overlapping offsets
# ---------------------------------------------------------------------------

def test_get_drill_through_data_pagination(controller_with_data):
    """asyncio.run(get_drill_through_data(..., limit=5, offset=0)) returns a dict
    with 'rows' (exactly 5) and 'total_rows'; offset=5 returns the next 5 (no overlap).

    Tests the Python controller directly — no HTTP round-trip.
    """
    spec = _make_spec()
    result0 = asyncio.run(
        controller_with_data.get_drill_through_data(spec, [], limit=5, offset=0)
    )
    result1 = asyncio.run(
        controller_with_data.get_drill_through_data(spec, [], limit=5, offset=5)
    )

    assert isinstance(result0, dict), f"Expected dict return, got {type(result0)}"
    assert 'rows' in result0, f"'rows' key missing from result: {result0.keys()}"
    assert 'total_rows' in result0, f"'total_rows' key missing from result: {result0.keys()}"

    page0 = result0['rows']
    page1 = result1['rows']

    assert len(page0) == 5, f"Expected 5 rows for page 0, got {len(page0)}"
    assert len(page1) == 5, f"Expected 5 rows for page 1, got {len(page1)}"

    assert result0['total_rows'] == 20, f"Expected total_rows=20, got {result0['total_rows']}"

    # Extract sales values (unique per row in this dataset) to check non-overlap
    sales0 = {r['sales'] for r in page0}
    sales1 = {r['sales'] for r in page1}
    assert sales0.isdisjoint(sales1), (
        f"Pages overlap — sales values in both pages: {sales0 & sales1}"
    )


# ---------------------------------------------------------------------------
# DRILL-06 (Python layer): Coordinate filters applied correctly
# ---------------------------------------------------------------------------

def test_get_drill_through_data_coord_filters(controller_with_data):
    """filters=[{'field': 'region', 'op': '=', 'value': 'North'}] returns only
    rows where region='North'.

    Tests the Python controller directly.
    """
    spec = _make_spec()
    filters = [{'field': 'region', 'op': '=', 'value': 'North'}]
    result = asyncio.run(
        controller_with_data.get_drill_through_data(spec, filters, limit=20, offset=0)
    )

    rows = result['rows']
    assert len(rows) > 0, "Expected at least one row with region='North'"
    for row in rows:
        assert row['region'] == 'North', (
            f"Expected region='North', got '{row['region']}' in row {row}"
        )


# ---------------------------------------------------------------------------
# DRILL-05 (Python layer): Sort param — skipped until Plan 02 adds sort support
# ---------------------------------------------------------------------------

def test_get_drill_through_data_sort(controller_with_data):
    """Passing sort_col='sales' and sort_dir='desc' returns rows in descending sales order.
    Passing sort_col='sales' and sort_dir='asc' returns rows in ascending sales order.

    Tests Plan 02 extension of get_drill_through_data with sort params.
    """
    spec = _make_spec()
    result_desc = asyncio.run(
        controller_with_data.get_drill_through_data(
            spec, [], limit=5, offset=0, sort_col='sales', sort_dir='desc'
        )
    )
    result_asc = asyncio.run(
        controller_with_data.get_drill_through_data(
            spec, [], limit=5, offset=0, sort_col='sales', sort_dir='asc'
        )
    )

    rows_desc = result_desc['rows']
    rows_asc = result_asc['rows']

    assert len(rows_desc) == 5, f"Expected 5 rows, got {len(rows_desc)}"
    assert len(rows_asc) == 5, f"Expected 5 rows, got {len(rows_asc)}"

    sales_desc = [r['sales'] for r in rows_desc]
    sales_asc = [r['sales'] for r in rows_asc]

    assert sales_desc == sorted(sales_desc, reverse=True), (
        f"Rows not in descending order: {sales_desc}"
    )
    assert sales_asc == sorted(sales_asc), (
        f"Rows not in ascending order: {sales_asc}"
    )
