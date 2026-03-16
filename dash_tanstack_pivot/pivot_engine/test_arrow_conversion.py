#!/usr/bin/env python3

def test_arrow_to_json_conversion():
    import pyarrow as pa
    from pivot_engine.controller import PivotController
    from pivot_engine.types.pivot_spec import PivotSpec
    
    print("Testing Arrow-to-JSON conversion improvements...")
    
    controller = PivotController()
    
    # Create an Arrow table with various data types to test the conversion
    table = pa.table({
        'str_col': ['A', 'B', 'C'],
        'int_col': [1, 2, 3],
        'float_col': [1.1, 2.2, 3.3],
        'null_col': [None, 'value', None],
        'decimal_col': [pa.scalar(10.5), pa.scalar(20.7), pa.scalar(30.9)]  # Using pa.scalar for decimal
    })
    
    # Test the conversion method directly
    spec = PivotSpec(table="test", rows=["str_col"], measures=[{"field": "int_col", "op": "sum"}])
    result = controller._convert_table_to_dict(table, spec)
    
    print(f"Result columns: {result['columns']}")
    print(f"Result rows length: {len(result['rows'])}")
    print(f"Sample row: {result['rows'][0] if result['rows'] else 'No rows'}")
    
    # Verify the conversion worked properly
    assert result['columns'] == table.column_names, "Column names should match"
    assert len(result['rows']) == table.num_rows, f"Row count should be {table.num_rows}"
    
    print("Arrow-to-JSON conversion test passed!")

if __name__ == "__main__":
    test_arrow_to_json_conversion()