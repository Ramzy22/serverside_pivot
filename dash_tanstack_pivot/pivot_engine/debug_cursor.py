from pivot_engine.controller import PivotController
import pyarrow as pa

print("Creating controller...")
try:
    # Create controller - let's try with SQL planner to avoid ibis issues
    controller = PivotController(backend_uri=':memory:', planner_name='sql')
    print("Controller created successfully")

    # Create a dataset for pagination 
    data = pa.table({
        'city': [f'City {i}' for i in range(5)],
        'sales': [10, 20, 30, 40, 50],
    })
    print("Creating table...")
    controller.load_data_from_arrow('sales_pagination', data)

    # First page: limit 2
    spec1 = {
        'table': 'sales_pagination',
        'rows': ['city'],
        'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
        'sort': [{'field': 'city', 'order': 'asc'}],
        'limit': 2,
    }

    print("Running first query...")
    result1 = controller.run_pivot(spec1, return_format='dict')
    print('First page:', result1)
    print('Next cursor:', result1.get('next_cursor'))

    # Second page: limit 2 with cursor
    spec2 = {
        'table': 'sales_pagination',
        'rows': ['city'],
        'measures': [{'field': 'sales', 'agg': 'sum', 'alias': 'total_sales'}],
        'sort': [{'field': 'city', 'order': 'asc'}],
        'limit': 2,
        'cursor': result1['next_cursor'],
    }

    print("Running second query...")
    result2 = controller.run_pivot(spec2, return_format='dict')
    print('Second page:', result2)
    print('Number of rows in second page:', len(result2.get('rows', [])))

except Exception as e:
    import traceback
    print('Error:', str(e))
    traceback.print_exc()