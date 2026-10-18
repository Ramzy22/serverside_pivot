"""Bounded export paging, writer control, and native batch streaming.

Formatting stays behind a small protocol so these resource and cancellation
helpers do not depend on the runtime service or its compatibility facade.
"""

from __future__ import annotations

import asyncio
import csv
import html
import os
import pickle
import tempfile
import threading
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import (
    Any, AsyncIterable, AsyncIterator, BinaryIO, Callable, Dict, Iterable,
    Iterator, List, Optional, Protocol,
)

from ..export_limits import ExportLimits
from ..resource_governor import ResourceExhaustedError, ResourceKind, ResourceLease
from ..tanstack_adapter import TanStackResponse
from .models import PivotViewState, first_present


class ExportCancelledError(RuntimeError):
    """Cooperative writer stop; the runner acknowledges after cleanup."""


@dataclass
class ExportWriteControl:
    directory: str
    lease: ResourceLease
    progress: Callable[[int | None, int | None], Any]
    ensure_bytes: Callable[[int], None] = lambda size: None
    cancelled: threading.Event = field(default_factory=threading.Event)
    paths: set[str] = field(default_factory=set)
    workspace_bytes: int = 0

    def reserve_workspace(self, size: int) -> None:
        if size > self.lease.reserved_bytes:
            self.lease.grow(size - self.lease.reserved_bytes)

    def checkpoint(self, rows: int | None = None, size: int | None = None) -> None:
        if self.cancelled.is_set():
            raise ExportCancelledError()
        if size is not None:
            self.ensure_bytes(size)
        if size is not None:
            self.reserve_workspace(self.workspace_bytes + size)
        if rows is not None or size is not None:
            self.progress(rows, size)


current_export_write: ContextVar[ExportWriteControl | None] = ContextVar("pivot_export_write", default=None)


def _export_checkpoint(rows: int | None = None, size: int | None = None) -> None:
    control = current_export_write.get()
    if control is not None:
        control.checkpoint(rows, size)


def _export_tempfile(prefix: str, suffix: str) -> tuple[int, str]:
    control = current_export_write.get()
    _export_checkpoint()
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=control.directory if control else None)
    if control is not None:
        control.paths.add(path)
    return fd, path


class _DiskExportRows:
    """Trusted internal row spool; only bounded pages enter Python memory."""

    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream
        self.count = 0
        self.first: Dict[str, Any] | None = None

    def append(self, row: Dict[str, Any]) -> None:
        if self.first is None:
            self.first = row
        pickle.dump(row, self.stream, protocol=pickle.HIGHEST_PROTOCOL)
        self.count += 1

    def __len__(self) -> int:
        return self.count

    def __getitem__(self, index: int) -> Dict[str, Any]:
        if index != 0 or self.first is None:
            raise IndexError(index)
        return self.first

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        self.stream.seek(0)
        for _ in range(self.count):
            yield pickle.load(self.stream)


class ExportFormatting(Protocol):
    """Formatting and limit operations consumed by bounded writers."""

    limits: ExportLimits

    def normalize_export_format(self, value: object) -> str: ...

    def build_export_payload(
        self, response: TanStackResponse, state: PivotViewState,
        export_request: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]: ...

    def _project_export_rows_for_layout(
        self, rows: List[Dict[str, Any]], state: PivotViewState,
    ) -> List[Dict[str, Any]]: ...

    def _build_export_header_map(
        self, columns: List[Dict[str, Any]], state: PivotViewState,
    ) -> Dict[str, str]: ...

    def _resolve_export_field_order(
        self, rows: List[Dict[str, Any]], columns: List[Dict[str, Any]],
        col_ids: Any, state: Optional[PivotViewState] = None,
    ) -> List[str]: ...

    def _export_header_for_key(
        self, key: str, header_map: Dict[str, str], state: PivotViewState,
        style_profile: Dict[str, Any],
    ) -> str: ...


async def build_paged_export(
    service: ExportFormatting,
    pages: AsyncIterable[TanStackResponse],
    state: PivotViewState,
    export_request: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Spool validated bounded response pages, then reuse faithful writers.

    The spool is private, anonymous and never deserializes external input.
    It preserves Decimal/date values without retaining all Python rows.
    """
    control = current_export_write.get()
    columns: List[Dict[str, Any]] = []
    with tempfile.TemporaryFile(mode="w+b", dir=control.directory if control else None) as stream:
        rows = _DiskExportRows(stream)
        try:
            async for page in pages:
                _export_checkpoint()
                if not columns:
                    columns = page.columns or []
                projected = service._project_export_rows_for_layout(page.data, state)
                service.limits.ensure_rows(len(rows) + len(projected))
                for row in projected:
                    _export_checkpoint()
                    rows.append(row)
                if control:
                    control.workspace_bytes = stream.tell()
                    control.reserve_workspace(control.workspace_bytes)
            return service.build_export_payload(TanStackResponse(data=rows, columns=columns), state, export_request)
        finally:
            if control:
                control.workspace_bytes = 0


async def build_streamed_export(
    service: ExportFormatting,
    batches: Iterable[Any] | AsyncIterable[Any],
    state: PivotViewState,
    export_request: Optional[Dict[str, Any]],
    *,
    columns: Optional[List[Dict[str, Any]]] = None,
    export_lease: Optional[ResourceLease] = None,
) -> Dict[str, Any]:
    """Write native record batches without retaining the full result."""
    request_payload = export_request if isinstance(export_request, dict) else {}
    export_fmt = service.normalize_export_format(request_payload.get("format"))
    if export_fmt not in {"csv", "tsv", "html"}:
        raise ResourceExhaustedError(
            ResourceKind.EXPORT,
            reason="bounded_format_unavailable",
        )
    delimiter = "\t" if export_fmt == "tsv" else ","
    ext = "tsv" if export_fmt == "tsv" else export_fmt
    content_type = {
        "tsv": "text/tab-separated-values;charset=utf-8",
        "html": "text/html;charset=utf-8",
    }.get(export_fmt, "text/csv;charset=utf-8")
    filename = str(request_payload.get("filename") or f"pivot_export.{ext}")
    include_headers = bool(first_present(request_payload, "includeHeaders", "include_headers", default=True))
    field_order: Optional[List[str]] = None
    header_map = service._build_export_header_map(columns or [], state)
    emitted_rows = 0
    fd, content_path = _export_tempfile(prefix="pivot_stream_", suffix=f".{ext}")

    async def _items() -> AsyncIterator[Any]:
        if hasattr(batches, "__aiter__"):
            async for value in batches:  # type: ignore[union-attr]
                yield value
        else:
            for value in batches:  # type: ignore[union-attr]
                yield value
                await asyncio.sleep(0)
    def _rows_from_batch(batch: Any) -> List[Dict[str, Any]]:
        if hasattr(batch, "to_pylist"):
            batch = batch.to_pylist()
        if not isinstance(batch, list):
            batch = list(batch)
        rows = [row for row in batch if isinstance(row, dict)]
        return service._project_export_rows_for_layout(rows, state)

    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, delimiter=delimiter) if export_fmt != "html" else None
            if export_fmt == "html":
                output.write("<table><thead><tr>")
            async for raw_batch in _items():
                _export_checkpoint()
                rows = _rows_from_batch(raw_batch)
                if not rows:
                    continue
                if field_order is None:
                    field_order = service._resolve_export_field_order(
                        rows, columns or [], request_payload.get("colIds"), state,
                    )
                    if include_headers:
                        labels = [service._export_header_for_key(key, header_map, state, {}) for key in field_order]
                        if export_fmt == "html":
                            output.write("".join(f"<th>{html.escape(str(label))}</th>" for label in labels))
                            output.write("</tr></thead><tbody>")
                        else:
                            writer.writerow(labels)
                    elif export_fmt == "html":
                        output.write("</tr></thead><tbody>")
                service.limits.ensure_rows(emitted_rows + len(rows))
                for row in rows:
                    _export_checkpoint()
                    values = ["" if row.get(key) is None else row.get(key) for key in (field_order or [])]
                    if export_fmt == "html":
                        output.write("<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in values) + "</tr>")
                    else:
                        writer.writerow(values)
                emitted_rows += len(rows)
                output.flush()
                current_bytes = os.path.getsize(content_path)
                service.limits.ensure_bytes(current_bytes)
                _export_checkpoint(emitted_rows, current_bytes)
                if export_lease is not None and current_bytes > export_lease.reserved_bytes:
                    export_lease.grow(current_bytes - export_lease.reserved_bytes)
            if export_fmt == "html":
                output.write("</tbody></table>")
                output.flush()
        content_length = os.path.getsize(content_path)
        service.limits.ensure_bytes(content_length)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.remove(content_path)
        except OSError:
            pass
        raise
    return {
        "contentPath": content_path,
        "contentLength": content_length,
        "contentType": content_type,
        "format": export_fmt,
        "filename": filename,
        "rows": emitted_rows,
        "columns": len(field_order or []),
        "partId": request_payload.get("partId"),
    }
