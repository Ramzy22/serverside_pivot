"""Streaming pivot-query execution owned independently from the controller façade."""

from __future__ import annotations

import asyncio
import io
import os
import tempfile
from typing import Any, AsyncGenerator, Optional, Protocol

import pyarrow as pa

from .export_limits import ExportLimits


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
        if hasattr(controller.backend, "execute_streaming"):
            iterator = controller.backend.execute_streaming(main_query, batch_size=self.batch_size)
            if normalized_format == "csv":
                header_written = False
                emitted_rows = 0
                emitted_bytes = 0
                for batch_rows in iterator:
                    if not batch_rows:
                        continue
                    if not isinstance(batch_rows, list):
                        batch_rows = list(batch_rows)
                    if not all(isinstance(row, dict) for row in batch_rows):
                        raise TypeError("Streaming backends must yield batches of row dictionaries.")
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
                    for batch_rows in iterator:
                        if not batch_rows:
                            continue
                        if not isinstance(batch_rows, list):
                            batch_rows = list(batch_rows)
                        if not all(isinstance(row, dict) for row in batch_rows):
                            raise TypeError("Streaming backends must yield batches of row dictionaries.")
                        self.limits.ensure_rows(emitted_rows + len(batch_rows))
                        emitted_rows += len(batch_rows)
                        batch_table = pa.Table.from_pylist(batch_rows)
                        if writer is None:
                            writer = parquet.ParquetWriter(temporary_path, batch_table.schema)
                        writer.write_table(batch_table)
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

        table = await controller.run_pivot_async(normalized_spec, return_format="arrow")
        self.limits.ensure_rows(int(getattr(table, "num_rows", 0) or 0))
        sink = io.BytesIO()
        if normalized_format == "csv":
            import pyarrow.csv as csv

            csv.write_csv(table, sink)
        else:
            import pyarrow.parquet as parquet

            parquet.write_table(table, sink)
        payload = sink.getvalue()
        self.limits.ensure_bytes(len(payload))
        yield payload
