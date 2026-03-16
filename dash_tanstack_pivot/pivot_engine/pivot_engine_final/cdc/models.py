"""
Data models for CDC (Change Data Capture) system
"""
from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class Change:
    """Represents a database change event"""
    table: str
    type: str  # 'INSERT', 'UPDATE', 'DELETE'
    new_row: Optional[Dict[str, Any]] = None
    old_row: Optional[Dict[str, Any]] = None