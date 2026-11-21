"""Versioning interfaces the core depends on (implemented by the live edges).

The core never imports ``streaming`` or ``diff``; those packages implement
these protocols (``VersionedSourceAdapter`` / ``SourceChangeProvider`` for
versions, ``diff.snapshot_provider.SnapshotProvider`` for snapshots).  The
core only needs a comparable revision to key caches and pagination cursors
(REQ-PAGE-002, REQ-LIVE-001).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol, Tuple, runtime_checkable

__all__ = ["DatasetVersion", "DatasetVersionProvider", "QueryChangeEvent", "SnapshotProvider", "UNVERSIONED"]


@dataclass(frozen=True)
class DatasetVersion:
    table: str
    source_revision: Optional[int]  # None = the source cannot report revisions
    schema_version: str = ""
    server_epoch_id: str = ""

    @property
    def token(self) -> str:
        rev = "unversioned" if self.source_revision is None else str(self.source_revision)
        return f"{self.server_epoch_id}:{self.schema_version}:{rev}"


UNVERSIONED = DatasetVersion(table="", source_revision=None)


@runtime_checkable
class DatasetVersionProvider(Protocol):
    def current_version(self, table: str) -> DatasetVersion: ...


@runtime_checkable
class SnapshotProvider(Protocol):
    def capture_snapshot(self, table: str, **options: Any) -> Any: ...

    def acquire_snapshot(self, snapshot_id: str, **options: Any) -> Any: ...

    def release_snapshot(self, snapshot_id: str) -> None: ...


@dataclass(frozen=True)
class QueryChangeEvent:
    """A committed source change, as the core sees it: which revision, which rows may move."""

    table: str
    from_revision: Optional[int]
    to_revision: int
    schema_changed: bool = False
    row_deltas_complete: bool = True
    touched_paths: Tuple[Tuple[Any, ...], ...] = ()  # empty + complete = values only
