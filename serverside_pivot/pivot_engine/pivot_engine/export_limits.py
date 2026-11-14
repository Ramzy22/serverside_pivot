"""Shared safety budgets for every pivot export transport."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Optional


DEFAULT_EXPORT_MAX_ROWS = 1_000_000
DEFAULT_EXPORT_MAX_BYTES = 512 * 1024 * 1024
EXPORT_ROW_LIMIT_CODE = "PIVOT_EXPORT_MAX_ROWS"
EXPORT_BYTE_LIMIT_CODE = "PIVOT_EXPORT_MAX_BYTES"


class ExportLimitError(ValueError):
    """Raised when an export would exceed its configured resource budget."""

    def __init__(self, message: str, *, limit_code: Optional[str] = None) -> None:
        super().__init__(message)
        self.limit_code = limit_code


def positive_env_int(name: str, default: int) -> int:
    """Read one positive integer environment setting with a safe fallback."""
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


@dataclass(frozen=True)
class ExportLimits:
    """One immutable row and byte budget shared by Dash and REST exports."""

    max_rows: int
    max_bytes: int

    @classmethod
    def from_environment(
        cls,
        *,
        max_rows: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ) -> "ExportLimits":
        return cls(
            max_rows=max(
                1,
                int(
                    max_rows
                    if max_rows is not None
                    else positive_env_int("PIVOT_EXPORT_MAX_ROWS", DEFAULT_EXPORT_MAX_ROWS)
                ),
            ),
            max_bytes=max(
                1,
                int(
                    max_bytes
                    if max_bytes is not None
                    else positive_env_int("PIVOT_EXPORT_MAX_BYTES", DEFAULT_EXPORT_MAX_BYTES)
                ),
            ),
        )

    def ensure_rows(self, rows: int) -> None:
        if int(rows) > self.max_rows:
            raise ExportLimitError(
                f"Export contains {int(rows):,} rows; the server limit is {self.max_rows:,}. "
                "Narrow the selected range or raise PIVOT_EXPORT_MAX_ROWS.",
                limit_code=EXPORT_ROW_LIMIT_CODE,
            )

    def ensure_bytes(self, content_length: int) -> None:
        if int(content_length) > self.max_bytes:
            raise ExportLimitError(
                f"Export is {int(content_length):,} bytes; the server limit is {self.max_bytes:,}. "
                "Narrow the selected range or raise PIVOT_EXPORT_MAX_BYTES.",
                limit_code=EXPORT_BYTE_LIMIT_CODE,
            )
