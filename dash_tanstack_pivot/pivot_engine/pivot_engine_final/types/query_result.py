"""
QueryResult - standardized response from PivotController.run_pivot
"""
from dataclasses import dataclass
from typing import List, Any, Dict

@dataclass
class QueryResult:
    columns: List[str]
    rows: List[List[Any]]
    page: Dict[str, Any]
    stats: Dict[str, Any] = None
