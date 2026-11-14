"""Streaming pivot-query execution owned independently from the controller façade."""

from __future__ import annotations

import asyncio
import io
import os
import tempfile
from typing import Any, AsyncGenerator, Optional, Protocol

import pyarrow as pa

from .export_limits import ExportLimits
from .resource_governor import ResourceExhaustedError, ResourceKind


def iter_bounded_record_batches(
    batch: Any,
    *,
    max_batch_bytes: int,
    max_batch_rows: int,
):
    """Yield Arrow batches that satisfy both row and byte ceilings.

    Drivers may return a batch that is wider than requested.  Splitting that
    batch before it reaches the formatter keeps the export buffer bounded; a
    single row that cannot fit is rejected explicitly instead of being
    materialized into an unbounded fallback table.
    """
    if max_batch_bytes <= 0 or max_batch_rows <= 0:
        raise ValueError("record-batch limits must be positive")
    if isinstance(batch, pa.Table):
        pieces = batch.to_batches(max_chunksize=max_batch_rows)
    elif isinstance(batch, pa.RecordBatch):
        pieces = [batch]
    else:
        raise TypeError("bounded export drivers must yield Arrow record batches")

    pending = list(pieces)
    while pending:
        current = pending.pop(0)
        rows = int(current.num_rows)
        if rows == 0:
            continue
        if rows <= max_batch_rows and int(getattr(current, "nbytes", 0)) <= max_batch_bytes:
            yield current
            continue
        if rows == 1:
            raise ResourceExhaustedError(
                ResourceKind.EXPORT,
                reason="record_batch_bytes",
            )
        midpoint = rows // 2
        pending.insert(0, current.slice(midpoint))
        pending.insert(0, current.slice(0, midpoint))


class _PivotExportController(Protocol):
    backend: Any
    planner: Any

    def _normalize_spec(self, spec: Any) -> Any:
        ...

    async def run_pivot_async(self, spec: Any, return_format: str = "arrow") -> Any:
        ...


class PivotExportExecutor:
    """Execute and stream a pivot export using the controller's stable query API."""

    def __init__(
        self,
        controller: _PivotExportController,
        *,
        max_rows: Optional[int] = None,
        max_bytes: Optional[int] = None,
        batch_size: int = 5_000,
    ) -> None:
        self._controller = controller
        self.limits = ExportLimits.from_environment(
            max_rows=max_rows,
            max_bytes=max_bytes,
        )
        self.batch_size = max(1, int(batch_size))

    def _validate_requested_rows(self, spec: Any) -> None:
        """Fail before planning when an API caller asks for an unsafe limit."""
        requested_limit = getattr(spec, "limit", None)
        if requested_limit is None:
            return
        try:
            requested_rows = int(requested_limit)
        except (TypeError, ValueError):
            # Existing PivotSpec normalization owns invalid limit coercion.
            return
        self.limits.ensure_rows(requested_rows)

    async def _checked_csv_chunk(
        self,
        rows: list[dict[str, Any]],
        *,
        header_written: bool,
        emitted_rows: int,
        emitted_bytes: int,
    ) -> tuple[bytes, int, int]:
        """Encode one bounded CSV batch without retaining prior output."""
        self.limits.ensure_rows(emitted_rows + len(rows))
        import pyarrow.csv as csv

        batch_table = pa.Table.from_pylist(rows)
        sink = io.BytesIO()
        csv.write_csv(batch_table, sink, csv.WriteOptions(include_header=not header_written))
        payload = sink.getvalue()
        self.limits.ensure_bytes(emitted_bytes + len(payload))
        return payload, emitted_rows + len(rows), emitted_bytes + len(payload)

    async def stream(self, spec: Any, export_format: str = "csv") -> AsyncGenerator[bytes, None]:
        controller = self._controller
        normalized_spec = controller._normalize_spec(spec)
        self._validate_requested_rows(normalized_spec)
        plan_result = await asyncio.get_running_loop().run_in_executor(None, controller.planner.plan, normalized_spec)
        queries = plan_result.get("queries", [])
        if not queries:
            yield b""
            return

        main_query = queries[-1]
        normalized_format = str(export_format or "csv").lower()
        batch_bytes = max(1, min(self.limits.max_bytes, 32 * 1024 * 1024))
        if hasattr(controller.backend, "execute_record_batches"):
            iterator = controller.backend.execute_record_batches(
                main_query,
                max_batch_bytes=batch_bytes,
                max_batch_rows=self.batch_size,
            )
        elif hasattr(controller.backend, "execute_streaming"):
            # Compatibility backends may already expose a bounded row
            # iterator.  They are accepted, but a backend exposing only
            # execute() is rejected below before it can materialize a table.
            iterator = controller.backend.execute_streaming(main_query, batch_size=self.batch_size)
        else:
            raise ResourceExhaustedError(
                ResourceKind.EXPORT,
                reason="bounded_streaming_unavailable",
            )

        if normalized_format == "csv":
            header_written = False
            emitted_rows = 0
            emitted_bytes = 0
            for batch in iterator:
                if isinstance(batch, (pa.RecordBatch, pa.Table)):
                    batches = iter_bounded_record_batches(
                        batch,
                        max_batch_bytes=batch_bytes,
                        max_batch_rows=self.batch_size,
                    )
                else:
                    batches = (batch,)
                for batch_rows in batches:
                    if isinstance(batch_rows, (pa.RecordBatch, pa.Table)):
                        batch_rows = batch_rows.to_pylist()
                    if not batch_rows:
                        continue
                    if not isinstance(batch_rows, list):
                        batch_rows = list(batch_rows)
                    if not all(isinstance(row, dict) for row in batch_rows):
                        raise TypeError("Streaming backends must yield batches of row dictionaries or Arrow batches.")
                    payload, emitted_rows, emitted_bytes = await self._checked_csv_chunk(
                        batch_rows,
                        header_written=header_written,
                        emitted_rows=emitted_rows,
                        emitted_bytes=emitted_bytes,
                    )
                    header_written = True
                    yield payload
                    await asyncio.sleep(0)
            return

        if normalized_format == "parquet":
            import pyarrow.parquet as parquet

            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temporary_file:
                temporary_path = temporary_file.name
            writer = None
            emitted_rows = 0
            try:
                for batch in iterator:
                    if isinstance(batch, (pa.RecordBatch, pa.Table)):
                        batches = iter_bounded_record_batches(
                            batch,
                            max_batch_bytes=batch_bytes,
                            max_batch_rows=self.batch_size,
                        )
                    else:
                        rows = list(batch)
                        batches = iter_bounded_record_batches(
                            pa.Table.from_pylist(rows),
                            max_batch_bytes=batch_bytes,
                            max_batch_rows=self.batch_size,
                        )
                    for bounded_batch in batches:
                        self.limits.ensure_rows(emitted_rows + bounded_batch.num_rows)
                        emitted_rows += bounded_batch.num_rows
                        if writer is None:
                            writer = parquet.ParquetWriter(temporary_path, bounded_batch.schema)
                        writer.write_batch(bounded_batch)
                        await asyncio.sleep(0)
                if writer:
                    writer.close()
                    writer = None
                self.limits.ensure_bytes(os.path.getsize(temporary_path))
                with open(temporary_path, "rb") as export_file:
                    while chunk := export_file.read(64 * 1024):
                        yield chunk
                        await asyncio.sleep(0)
            finally:
                if writer:
                    writer.close()
                if os.path.exists(temporary_path):
                    os.unlink(temporary_path)
            return

        # CSV and parquet are the streaming wire formats. Other formats are
        # intentionally rejected here instead of silently materializing a full
        # table behind a formatter.
        raise ResourceExhaustedError(
            ResourceKind.EXPORT,
            reason="bounded_format_unavailable",
        )
