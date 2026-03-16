
import pytest
import ibis
import pandas as pd
from pivot_engine.planner.ibis_planner import IbisPlanner
from pivot_engine.types.pivot_spec import PivotSpec, Measure

@pytest.fixture
def ibis_conn():
    """Create an in-memory DuckDB Ibis connection for testing."""
    return ibis.connect("duckdb://:memory:")

@pytest.mark.parametrize(
    ("agg", "measure_field", "measure_filter", "expected_west_total", "expected_east_total"),
    [
        ("sum", "sales", 110, 420, 1200),
        ("avg", "sales", 110, 140, 400),
    ],
)
def test_visual_totals_with_measure_filter(
    ibis_conn,
    agg,
    measure_field,
    measure_filter,
    expected_west_total,
    expected_east_total,
):
    """
    Tests that parent totals are correctly recalculated from filtered child groups
    when a measure filter (post-aggregation/HAVING) is applied.

    Scenario:
    - Hierarchy: Region > Country
    - Data is intentionally unbalanced so AVG is wrong if implemented as
      "sum of averages" instead of recomputing from filtered leaf rows.
    - Filter: measure > 110 at the country level
    - Expected:
      - SUM: West = 300 + 120 = 420? No: visual totals must only use filtered
        country aggregates, so West = 300 + 120? Wait, country sums are 300 and 120,
        and the region total should be 420.
      - AVG: West = (100 + 200 + 120) / 3 = 140, not 150 + 120 = 270.
    """
    # 1. Setup: Create test data and load it into the backend
    data = {
        'region': ['West', 'West', 'West', 'East', 'East', 'East'],
        'country': ['France', 'France', 'USA', 'Germany', 'Germany', 'Germany'],
        'sales': [100, 200, 120, 300, 400, 500]
    }
    df = pd.DataFrame(data)
    ibis_conn.create_table('sales', df, overwrite=True)

    # 2. Spec: Define the hierarchical request with a measure filter
    measure_alias = f"sales_{agg}"
    spec = PivotSpec(
        table='sales',
        rows=['region'],
        full_rows=['region', 'country'],  # Provide full hierarchy for Visual Totals
        measures=[Measure(field=measure_field, agg=agg, alias=measure_alias)],
        filters=[{'field': measure_alias, 'op': '>', 'value': measure_filter}]
    )

    # 3. Plan: Generate the Ibis query using the planner
    planner = IbisPlanner(con=ibis_conn)
    plan_result = planner.plan(spec)
    
    query = plan_result['queries'][0]

    # For debugging: print the generated SQL
    try:
        sql = ibis_conn.compile(query)
        print("\n--- Generated SQL for Visual Totals Test ---")
        print(sql)
        print("------------------------------------------\n")
    except Exception as e:
        pytest.fail(f"Ibis query compilation failed: {e}")

    # 4. Execute & Assert
    result = query.execute().to_dict('records')

    # Find the 'West' record and check its total
    west_record = next((r for r in result if r['region'] == 'West'), None)
    
    assert west_record is not None, "Region 'West' not found in results."

    assert west_record[measure_alias] == expected_west_total, (
        f"Visual Totals failed for {agg}. Expected West total to be "
        f"{expected_west_total}, but got {west_record[measure_alias]}."
    )

    # Also check that 'East' record is correct
    east_record = next((r for r in result if r['region'] == 'East'), None)
    assert east_record is not None, "Region 'East' not found in results."
    assert east_record[measure_alias] == expected_east_total, f"East total is incorrect for {agg}."
