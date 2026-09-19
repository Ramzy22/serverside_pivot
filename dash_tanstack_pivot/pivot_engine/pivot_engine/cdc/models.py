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
    source: str = "unknown"
    event_id: Optional[str] = None
    partition: str = "0"
    sequence: Optional[int] = None
    row_identity: Optional[str] = None
    row_images_complete: Optional[bool] = None
    invalidation_reason: Optional[str] = None

    @classmethod
    def invalidation(
        cls,
        table: str,
        reason: str,
        *,
        source: str = "polling",
    ) -> "Change":
        """Represent a source change that cannot be safely applied incrementally."""
        return cls(
            table=table,
            type="INVALIDATE",
            source=source,
            row_images_complete=False,
            invalidation_reason=reason,
        )

    def is_exact(self) -> bool:
        """Whether this event includes enough trusted data for reversible state."""
        operation = str(self.type or "").upper()
        images_ok = (
            operation == "INSERT" and isinstance(self.new_row, dict)
        ) or (
            operation == "DELETE" and isinstance(self.old_row, dict)
        ) or (
            operation == "UPDATE"
            and isinstance(self.old_row, dict)
            and isinstance(self.new_row, dict)
        )
        return bool(
            images_ok
            and self.row_images_complete is not False
            and str(self.source or "").strip()
            and str(self.event_id or "").strip()
            and str(self.row_identity or "").strip()
            and isinstance(self.sequence, int)
            and not isinstance(self.sequence, bool)
        )

    def is_invalidating(self) -> bool:
        """Whether consumers must discard derived state instead of applying it."""
        return bool(self.invalidation_reason) or not self.is_exact()
