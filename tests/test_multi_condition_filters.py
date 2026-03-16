
import pytest
import ibis
import pandas as pd
from pivot_engine.common.ibis_expression_builder import IbisExpressionBuilder

@pytest.fixture
def ibis_conn():
    """Create an in-memory DuckDB Ibis connection for testing."""
    conn = ibis.connect("duckdb://:memory:")
    data = {
        'product': ['Laptop', 'Phone', 'Tablet', 'Headphones'],
        'region': ['North', 'South', 'North', 'South'],
        'sales': [100, 200, 150, 50]
    }
    df = pd.DataFrame(data)
    conn.create_table('products', df, overwrite=True)
    return conn

def test_multi_condition_and_filter(ibis_conn):
    """
    Tests that a composite filter with multiple AND conditions is parsed correctly.
    """
    builder = IbisExpressionBuilder(ibis_conn)
    table = ibis_conn.table('products')

    filters = [
        {
            'op': 'AND',
            'conditions': [
                {'field': 'product', 'op': 'contains', 'value': 'h'},
                {'field': 'region', 'op': 'eq', 'value': 'South'}
            ]
        }
    ]

    expr = builder.build_filter_expression(table, filters)
    
    # Check that we got a valid expression back
    assert expr is not None
    
    # Execute and check results
    # Both 'Phone' and 'Headphones' are in South and contain 'h' (case-insensitive)
    result = table.filter(expr).execute()
    assert len(result) == 2
    assert set(result['product'].tolist()) == {'Phone', 'Headphones'}

def test_single_condition_filter(ibis_conn):
    """Tests that a simple, single-condition filter still works."""
    builder = IbisExpressionBuilder(ibis_conn)
    table = ibis_conn.table('products')
    filters = [{'field': 'sales', 'op': '>', 'value': 120}]

    expr = builder.build_filter_expression(table, filters)
    
    assert expr is not None

    result = table.filter(expr).execute()
    assert len(result) == 2
    assert 'Tablet' in result['product'].tolist() # Tablet 150 > 120
    assert 'Phone' in result['product'].tolist()  # Phone 200 > 120
