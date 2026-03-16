from pivot_engine.controller import PivotController
import pyarrow as pa

def test_cursor_pagination():
    controller = PivotController(backend_uri=':memory:', planner_name='sql')
    
    # Create a larger dataset for pagination
    data = pa.table({
        'city': [f'City {i}' for i in range(5)],
        'sales': [10, 20, 30, 40, 50],
    })
    controller.load_data_from_arrow('sales_pagination', data)

    # --- First Page ---
    spec1 = {
        'table': 'sales_pagination',
        'rows': ['city'],
        'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
        'sort': [{'field': 'city', 'order': 'asc'}],
        'limit': 2,
    }

    result1 = controller.run_pivot(spec1, return_format='dict')

    print('First page result:', result1)
    print('First page rows:', len(result1['rows']))

    # --- Second Page ---
    spec2 = {
        'table': 'sales_pagination',
        'rows': ['city'],
        'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
        'sort': [{'field': 'city', 'order': 'asc'}],
        'limit': 2,
        'cursor': result1['next_cursor'], # Use the cursor from the previous result
    }

    result2 = controller.run_pivot(spec2, return_format='dict')

    print('Second page result rows:', len(result2['rows']))
    print('Expected: 2, Got:', len(result2['rows']))
    print('Second page details:', result2)
    
    # Test assertion
    assert len(result2['rows']) == 2, f"Expected 2 rows, got {len(result2['rows'])}: {result2['rows']}"

try:
    test_cursor_pagination()
    print("Test passed!")
except Exception as e:
    import traceback
    print(f"Test failed with error: {e}")
    traceback.print_exc()