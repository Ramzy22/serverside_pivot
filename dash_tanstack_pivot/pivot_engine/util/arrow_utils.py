"""
Utilities for Arrow table handling.
"""
from typing import List, Dict, Any, Optional, Union
try:
    import pyarrow as pa
    import pyarrow.compute as pc
except ImportError:
    pa = None
    pc = None

def ensure_arrow_table(data: Any) -> "pa.Table":
    """
    Ensure the input data is a PyArrow Table.
    
    Args:
        data: Input data (pa.Table, pandas.DataFrame, list of dicts)
        
    Returns:
        pa.Table
    """
    if pa is None:
        raise ImportError("pyarrow is required for arrow_utils")
        
    if isinstance(data, pa.Table):
        return data
    
    # Check for pandas DataFrame without importing pandas if not needed
    if hasattr(data, "to_dict"): 
        # Likely pandas DataFrame
        try:
            return pa.Table.from_pandas(data)
        except Exception:
            pass
            
    if isinstance(data, list):
        if not data:
            return pa.Table.from_pydict({})
        return pa.Table.from_pylist(data)
        
    if isinstance(data, dict):
        return pa.Table.from_pydict(data)
        
    raise ValueError(f"Could not convert {type(data)} to PyArrow Table")

def concat_tables(tables: List["pa.Table"]) -> "pa.Table":
    """
    Concatenate multiple Arrow tables.
    """
    if pa is None:
        raise ImportError("pyarrow is required for arrow_utils")
    
    if not tables:
        return pa.Table.from_pydict({})
        
    return pa.concat_tables(tables)

def estimate_memory_usage(table: "pa.Table") -> int:
    """
    Estimate memory usage of an Arrow table in bytes.
    """
    if pa is None:
        raise ImportError("pyarrow is required for arrow_utils")
        
    return table.nbytes

def slice_table(table: "pa.Table", offset: int, length: int) -> "pa.Table":
    """
    Slice an Arrow table.
    """
    if pa is None:
        raise ImportError("pyarrow is required for arrow_utils")
        
    return table.slice(offset, length)

def filter_table(table: "pa.Table", filters: List[Dict[str, Any]]) -> "pa.Table":
    """
    Filter an Arrow table using a list of filter dictionaries.
    
    Filters format: [{'field': 'col', 'op': '=', 'value': 1}]
    """
    if pa is None:
        raise ImportError("pyarrow is required for arrow_utils")
        
    if not filters:
        return table
        
    mask = None
    
    for f in filters:
        field = f.get('field')
        op = f.get('op', '=')
        value = f.get('value')
        
        if field not in table.column_names:
            continue
            
        col = table[field]
        current_mask = None
        
        # Helper to handle type conversion for comparison if needed
        # (PyArrow compute handles many types automatically)
        
        if op == '=' or op == '==':
            current_mask = pc.equal(col, value)
        elif op == '!=':
            current_mask = pc.not_equal(col, value)
        elif op == '>':
            current_mask = pc.greater(col, value)
        elif op == '>=':
            current_mask = pc.greater_equal(col, value)
        elif op == '<':
            current_mask = pc.less(col, value)
        elif op == '<=':
            current_mask = pc.less_equal(col, value)
        elif op == 'in':
            if isinstance(value, list):
                current_mask = pc.is_in(col, value_set=pa.array(value))
        elif op == 'like':
            # Simple simulation of LIKE
            current_mask = pc.match_substring(col, value)
            
        if current_mask is not None:
            if mask is None:
                mask = current_mask
            else:
                mask = pc.and_(mask, current_mask)
                
    if mask is not None:
        return table.filter(mask)
        
    return table

def sort_table(table: "pa.Table", sort_keys: List[Dict[str, str]]) -> "pa.Table":
    """
    Sort an Arrow table.
    
    Args:
        table: The table to sort
        sort_keys: List of dicts like [{'field': 'col', 'order': 'asc'}]
    """
    if pa is None:
        raise ImportError("pyarrow is required for arrow_utils")
        
    if not sort_keys:
        return table
        
    # PyArrow sort_by takes list of (name, order) tuples
    # order is "ascending" or "descending"
    pa_sort_keys = []
    
    for k in sort_keys:
        field = k.get('field')
        if field not in table.column_names:
            continue
            
        order = k.get('order', 'asc').lower()
        pa_order = "descending" if order == 'desc' else "ascending"
        
        pa_sort_keys.append((field, pa_order))
        
    if not pa_sort_keys:
        return table
        
    # Note: sort_by was added in newer PyArrow versions
    try:
        return table.sort_by(pa_sort_keys)
    except AttributeError:
        # Fallback for older versions if needed (though >=10.0.0 is required)
        indices = pc.sort_indices(table, sort_keys=pa_sort_keys)
        return table.take(indices)

def get_column_values(table: "pa.Table", column: str, distinct: bool = True) -> List[Any]:
    """
    Get values from a specific column, optionally distinct.
    """
    if pa is None:
        raise ImportError("pyarrow is required for arrow_utils")
        
    if column not in table.column_names:
        return []
        
    col_data = table[column]
    
    if distinct:
        return pc.unique(col_data).to_pylist()
    else:
        return col_data.to_pylist()