"""
tanstack_adapter.py - Direct TanStack Table/Query adapter for the scalable pivot engine
This bypasses the REST API and provides direct integration with TanStack components
"""

from typing import Dict, Any, List, Optional, Tuple, Union
import asyncio
import logging
import math
import os
import re
import time
import json
from collections import Counter, OrderedDict
from dataclasses import replace
import hashlib
from .adapter_viewport_cache import AdapterViewportCache, AdapterViewportKeyBuilder
from .scalable_pivot_controller import ScalablePivotController
from .editing import EditDomainService, TransactionAdapterMixin
from .types.pivot_spec import PivotSpec, Measure, MeasureAxisConfig
from .security import User, apply_rls_to_spec
from .core.authority.scope import row_scoped
from .formula_mixin import FormulaEngineMixin
from .hierarchy_rows import (
    GRAND_TOTAL_ID,
    GRAND_TOTAL_PATH,
    finalize_hierarchy_rows,
    is_hierarchy_grand_total,
)
from .transport.contracts import TanStackOperation, TanStackRequest, TanStackResponse
from .transport.request_mapper import TanStackRequestMapper
from .transport.response_mapper import TanStackResponseMapper
from .column_layout import (
    LayoutRevisionStore, PivotColumnIdentity, catalog_from_pivot_values, resolve_column_layout,
)


# Row metadata (never a data column). _rowKey is the canonical typed identity (REQ-ID-001).
ROW_META_KEYS = frozenset({
    "_id", "_path", "_isTotal", "_level", "_expanded", "_parentPath", "_has_children", "_is_expanded",
    "depth", "_depth", "uuid", "subRows", "_pathFields", "_rowKey",
})

_adapter_logger = logging.getLogger("pivot_engine.adapter")
_FORMULA_IDENTIFIER_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
_INITIAL_SCHEMA_WINDOW_CENTER_COLS = 24


def _positive_env_float(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


# These caches are generation-keyed and explicitly invalidated after data changes.
# A 10-second TTL caused periodic full column-catalog and hierarchy recomputation
# during ordinary scrolling, so keep hot viewport state for a realistic session.
_LOCAL_CACHE_TTL_SECONDS = _positive_env_float(
    "PIVOT_VIEWPORT_CACHE_TTL_SECONDS", 300.0
)
_PIVOT_CATALOG_CACHE_SIZE = 32
_RESPONSE_WINDOW_CACHE_SIZE = 128
_ROW_BLOCK_CACHE_SIZE = 1024
_ROW_BLOCK_SIZE = 100


def _normalize_formula_reference_key(value: Any, fallback: Any = "formula") -> str:
    base = str(value or "").strip().lower()
    base = re.sub(r"\s+", "", base)
    base = re.sub(r"[^a-z0-9_]", "", base)
    fallback_base = (
        re.sub(r"[^a-z0-9_]", "", str(fallback or "formula").strip().lower())
        or "formula"
    )
    normalized = base or fallback_base
    return normalized if re.match(r"^[a-z_]", normalized) else f"f_{normalized}"


def _is_missing_value(value: Any) -> bool:
    """Treat both None and NaN as missing values from the engine layer."""
    return value is None or (isinstance(value, float) and math.isnan(value))


def _is_grand_total_row(row: Any) -> bool:
    return is_hierarchy_grand_total(row)


def _has_case_variant_siblings(rows: Any) -> bool:
    seen: Dict[Any, Any] = {}
    for row in rows:
        fields = row.get("_pathFields") or []
        path = tuple(row.get(f) for f in fields)
        if not path or not isinstance(path[-1], str):
            continue
        key = (path[:-1], path[-1].lower())
        if key in seen and seen[key] != path[-1]:
            return True
        seen[key] = path[-1]
    return False


class TanStackPivotAdapter(TransactionAdapterMixin, FormulaEngineMixin):
    """Direct TanStack adapter that bypasses REST API and connects to controller"""

    bounded_pivot_export_capable = True

    def __init__(self, controller: ScalablePivotController, debug: bool = False):
        self.controller = controller
        self.edit_domain = EditDomainService()
        self.hierarchy_state = {}  # Store expansion state
        self._debug = debug
        self.cache_coordinator = getattr(controller, "cache_coordinator", None)
        self._viewport_cache = AdapterViewportCache(
            ttl_seconds=_LOCAL_CACHE_TTL_SECONDS,
            pivot_catalog_size=_PIVOT_CATALOG_CACHE_SIZE,
            response_window_size=_RESPONSE_WINDOW_CACHE_SIZE,
            row_block_cache_size=_ROW_BLOCK_CACHE_SIZE,
            cache_coordinator=self.cache_coordinator,
            generation_guard=getattr(self.cache_coordinator, "generation_guard", None),
        )
        self._viewport_keys = AdapterViewportKeyBuilder(self._cache_generation_value)
        self.cache_coordinator = self._viewport_cache.cache_coordinator
        self._local_cache_lock = self._viewport_cache.lock
        # Compatibility aliases for existing diagnostics/tests. All mutation
        # goes through AdapterViewportCache.
        self._center_col_ids_cache = self._viewport_cache.center_col_ids_cache
        self._pivot_column_catalog_cache = (
            self._viewport_cache.pivot_column_catalog_cache
        )
        self._response_window_cache = self._viewport_cache.response_window_cache
        self._row_block_cache = self._viewport_cache.row_block_cache
        self._grand_total_cache = self._viewport_cache.grand_total_cache
        self._prefetch_request_keys = self._viewport_cache.prefetch_request_keys
        self.viewport_prefetch_enabled: bool = False
        self._request_mapper = TanStackRequestMapper()
        self._response_mapper = TanStackResponseMapper()
        self._layout_revisions = LayoutRevisionStore()

    @property
    def _local_cache_generation(self) -> int:
        return self._viewport_cache.generation_value()

    def _cache_generation_value(self) -> int:
        return self._viewport_cache.generation_value()

    def _locked_cache_lookup(self, cache: OrderedDict, key: str):
        return self._viewport_cache.lookup(cache, key)

    def _locked_cache_store(
        self,
        cache: OrderedDict,
        key: str,
        value: Any,
        max_size: int,
        ttl_seconds: float,
    ):
        self._viewport_cache.store(
            cache,
            key,
            value,
            max_size=max_size,
            ttl_seconds=ttl_seconds,
        )

    def _center_col_ids_cache_get(self, key: Any) -> Optional[List[str]]:
        return self._viewport_cache.center_col_ids_get(key)

    def _center_col_ids_cache_set(self, key: Any, value: List[str]) -> None:
        self._viewport_cache.center_col_ids_set(key, value)

    def _mark_prefetch_request_pending(self, cache_key: str) -> bool:
        return self._viewport_cache.mark_prefetch_request_pending(cache_key)

    def _clear_prefetch_request_pending(self, cache_key: str) -> None:
        self._viewport_cache.clear_prefetch_request_pending(cache_key)

    @staticmethod
    def _profile_ms(start: Optional[float], end: Optional[float]) -> Optional[float]:
        if start is None or end is None:
            return None
        return round((end - start) * 1000, 3)

    def _attach_virtual_scroll_profile(
        self,
        response: "TanStackResponse",
        *,
        started_at: float,
        request: "TanStackRequest",
        start_row: int,
        end_row: int,
        col_start: int,
        col_end: Optional[int],
        needs_col_schema: bool,
        include_grand_total: bool,
        path: str,
        stages: Dict[str, Optional[float]],
        response_cache_key: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> "TanStackResponse":
        response.profile = {
            "adapter": {
                "operation": "virtual_scroll",
                "path": path,
                "table": request.table,
                "responseCacheKey": response_cache_key,
                "cacheGeneration": self._cache_generation_value(),
                "cacheNamespaces": self._viewport_cache.metrics_snapshot().get(
                    "namespaces", {}
                ),
                "rowWindow": [
                    int(start_row or 0),
                    int(end_row if end_row is not None else start_row or 0),
                ],
                "colWindow": [
                    int(col_start or 0),
                    None if col_end is None else int(col_end),
                ],
                "needsColSchema": bool(needs_col_schema),
                "includeGrandTotal": bool(include_grand_total),
                "responseRows": len(response.data or []),
                "responseColumns": len(response.columns or []),
                "totalRows": response.total_rows,
                "totalMs": self._profile_ms(started_at, time.perf_counter()),
                **{
                    key: value
                    for key, value in (stages or {}).items()
                    if value is not None
                },
            }
        }
        if isinstance(extra, dict):
            for key, value in extra.items():
                if value is not None:
                    response.profile[key] = value
        return response

    @staticmethod
    def _stable_json(value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))

    @staticmethod
    def _request_with_row_scope(
        request: "TanStackRequest", row_scope: Any
    ) -> "TanStackRequest":
        """Bind a server-issued row grant into request identity before caching.

        The adapter owns several viewport caches.  A row grant has to be part
        of the immutable request consumed by every one of those caches; adding
        it only to the SQL spec after a cache lookup would allow a response
        built for one tenant to satisfy another tenant's request.
        """
        if row_scope is None:
            return request
        apply_filters = getattr(row_scope, "apply_to_filters", None)
        if not callable(apply_filters):
            raise ValueError("A row-scoped request requires an immutable row grant.")
        scoped_filters = apply_filters(dict(request.filters or {}))
        if not isinstance(scoped_filters, dict):
            raise ValueError("Row grant produced invalid request filters.")
        return replace(request, filters=scoped_filters)

    def _center_column_catalog_cache_key(self, request: "TanStackRequest") -> str:
        return self._viewport_keys.center_column_catalog_key(request)

    def _pivot_column_catalog_cache_key(self, spec: PivotSpec) -> str:
        """Key column discovery by inputs that can change column membership/order.

        Row hierarchy fields and measures after the first one do not affect the
        discovered pivot members. Including them caused a catalog query after
        every deeper row-field change, while omitting ``spec.columns`` could
        incorrectly reuse a catalog for a different pivot dimension.
        """
        return self._viewport_keys.pivot_column_catalog_key(spec)

    @staticmethod
    def _clone_response(
        response: "TanStackResponse", include_profile: bool = False
    ) -> "TanStackResponse":
        if response is None:
            return response
        return TanStackResponse(
            data=[dict(row) for row in (response.data or [])],
            columns=[dict(column) for column in (response.columns or [])],
            pagination=(
                dict(response.pagination)
                if isinstance(response.pagination, dict)
                else response.pagination
            ),
            total_rows=response.total_rows,
            grouping=(
                [
                    dict(item) if isinstance(item, dict) else item
                    for item in (response.grouping or [])
                ]
                if response.grouping
                else response.grouping
            ),
            version=response.version,
            layout_revision=response.layout_revision,
            col_schema=(
                {
                    **response.col_schema,
                    "columns": [
                        dict(column)
                        for column in (response.col_schema.get("columns") or [])
                    ],
                }
                if isinstance(response.col_schema, dict)
                else response.col_schema
            ),
            color_scale_stats=(
                json.loads(json.dumps(response.color_scale_stats, default=str))
                if response.color_scale_stats is not None
                else None
            ),
            profile=(
                json.loads(json.dumps(response.profile, default=str))
                if include_profile and response.profile is not None
                else None
            ),
        )

    def _response_window_cache_key(
        self,
        request: "TanStackRequest",
        start_row: int,
        end_row: int,
        expanded_paths: Union[List[List[str]], bool, None],
        col_start: int,
        col_end: Optional[int],
        needs_col_schema: bool,
        include_grand_total: bool,
        requested_center_ids: Optional[List[str]],
    ) -> str:
        return self._viewport_keys.response_window_key(
            request,
            start_row,
            end_row,
            expanded_paths,
            col_start,
            col_end,
            needs_col_schema,
            include_grand_total,
            requested_center_ids,
        )

    def _row_block_cache_key(
        self,
        request: "TanStackRequest",
        block_index: int,
        expanded_paths: Union[List[List[str]], bool, None],
        col_start: int,
        col_end: Optional[int],
        requested_center_ids: Optional[List[str]],
    ) -> str:
        return self._viewport_keys.row_block_key(
            request,
            block_index,
            expanded_paths,
            col_start,
            col_end,
            requested_center_ids,
        )

    def _grand_total_cache_key(
        self,
        request: "TanStackRequest",
        expanded_paths: Union[List[List[str]], bool, None],
        col_start: int,
        col_end: Optional[int],
        requested_center_ids: Optional[List[str]],
    ) -> str:
        return self._viewport_keys.grand_total_key(
            request,
            expanded_paths,
            col_start,
            col_end,
            requested_center_ids,
        )

    def _store_row_block_window(
        self,
        request: "TanStackRequest",
        response: "TanStackResponse",
        start_row: int,
        end_row: int,
        expanded_paths: Union[List[List[str]], bool, None],
        col_start: int,
        col_end: Optional[int],
        requested_center_ids: Optional[List[str]],
        *,
        _already_cloned: bool = False,
    ) -> None:
        if response is None or not isinstance(response.data, list):
            return

        grand_total_row = None
        regular_rows = []
        gt_path = GRAND_TOTAL_PATH
        gt_id = GRAND_TOTAL_ID
        append = regular_rows.append
        for _row in response.data:
            if type(_row) is not dict and not isinstance(_row, dict):
                continue
            if _row.get("_isTotal") or _row.get("_path") == gt_path or _row.get("_id") == gt_id:
                if grand_total_row is None:
                    grand_total_row = _row if _already_cloned else dict(_row)
            else:
                append(_row if _already_cloned else dict(_row))

        if grand_total_row is not None:
            self._locked_cache_store(
                self._grand_total_cache,
                self._grand_total_cache_key(
                    request, expanded_paths, col_start, col_end, requested_center_ids
                ),
                grand_total_row,
                max_size=_ROW_BLOCK_CACHE_SIZE,
                ttl_seconds=_LOCAL_CACHE_TTL_SECONDS,
            )

        if not regular_rows:
            return

        response_start = int(start_row or 0)
        response_end = response_start + len(regular_rows) - 1
        total_rows = (
            response.total_rows if response.total_rows is not None else response_end + 1
        )
        start_block = response_start // _ROW_BLOCK_SIZE
        end_block = response_end // _ROW_BLOCK_SIZE

        cached_columns = (
            list(response.columns or [])
            if _already_cloned
            else [dict(column) for column in (response.columns or [])]
        )
        cached_pagination = (
            response.pagination
            if _already_cloned
            else (
                dict(response.pagination)
                if isinstance(response.pagination, dict)
                else response.pagination
            )
        )
        cached_grouping = (
            response.grouping
            if _already_cloned
            else (
                [
                    dict(item) if isinstance(item, dict) else item
                    for item in (response.grouping or [])
                ]
                if response.grouping
                else response.grouping
            )
        )

        for block_index in range(start_block, end_block + 1):
            block_start = block_index * _ROW_BLOCK_SIZE
            block_end = block_start + _ROW_BLOCK_SIZE - 1
            abs_start = max(block_start, response_start)
            abs_end = min(block_end, response_end)
            rel_start = abs_start - response_start
            rel_end = abs_end - response_start + 1
            block_rows = regular_rows[rel_start:rel_end]
            expected_rows = max(0, min(block_end + 1, int(total_rows)) - block_start)
            is_complete = (
                abs_start == block_start
                and len(block_rows) >= expected_rows
                and expected_rows > 0
            )
            if not is_complete:
                continue
            self._locked_cache_store(
                self._row_block_cache,
                self._row_block_cache_key(
                    request,
                    block_index,
                    expanded_paths,
                    col_start,
                    col_end,
                    requested_center_ids,
                ),
                {
                    "rows": block_rows[:expected_rows],
                    "total_rows": response.total_rows,
                    "version": response.version,
                    "layout_revision": response.layout_revision,
                    "pagination": cached_pagination,
                    "grouping": cached_grouping,
                    "columns": cached_columns,
                },
                max_size=_ROW_BLOCK_CACHE_SIZE,
                ttl_seconds=_LOCAL_CACHE_TTL_SECONDS,
            )

    def _get_cached_row_block_entries(
        self,
        request: "TanStackRequest",
        start_row: int,
        end_row: int,
        expanded_paths: Union[List[List[str]], bool, None],
        col_start: int,
        col_end: Optional[int],
        requested_center_ids: Optional[List[str]],
    ) -> tuple[Dict[int, Dict[str, Any]], List[int]]:
        if end_row is None:
            return {}, []
        start_block = max(int(start_row or 0), 0) // _ROW_BLOCK_SIZE
        end_block = max(int(end_row), int(start_row or 0)) // _ROW_BLOCK_SIZE
        entries: Dict[int, Dict[str, Any]] = {}
        missing_blocks: List[int] = []
        for block_index in range(start_block, end_block + 1):
            entry = self._locked_cache_lookup(
                self._row_block_cache,
                self._row_block_cache_key(
                    request,
                    block_index,
                    expanded_paths,
                    col_start,
                    col_end,
                    requested_center_ids,
                ),
            )
            if entry is None:
                missing_blocks.append(block_index)
                continue
            entries[block_index] = entry
        return entries, missing_blocks

    def _assemble_cached_row_block_window(
        self,
        request: "TanStackRequest",
        start_row: int,
        end_row: int,
        expanded_paths: Union[List[List[str]], bool, None],
        col_start: int,
        col_end: Optional[int],
        include_grand_total: bool,
        requested_center_ids: Optional[List[str]],
    ) -> Optional["TanStackResponse"]:
        block_entries, missing_blocks = self._get_cached_row_block_entries(
            request,
            start_row,
            end_row,
            expanded_paths,
            col_start,
            col_end,
            requested_center_ids,
        )
        if missing_blocks:
            return None

        grand_total_row = None
        if include_grand_total:
            grand_total_row = self._locked_cache_lookup(
                self._grand_total_cache,
                self._grand_total_cache_key(
                    request, expanded_paths, col_start, col_end, requested_center_ids
                ),
            )
            if grand_total_row is None:
                return None

        start_block = max(int(start_row or 0), 0) // _ROW_BLOCK_SIZE
        end_block = max(int(end_row), int(start_row or 0)) // _ROW_BLOCK_SIZE
        merged_rows: List[Dict[str, Any]] = []
        metadata_source: Optional[Dict[str, Any]] = None
        for block_index in range(start_block, end_block + 1):
            entry = block_entries.get(block_index)
            if entry is None:
                return None
            if metadata_source is None:
                metadata_source = entry
            merged_rows.extend(dict(row) for row in (entry.get("rows") or []))

        trim_from = max(int(start_row or 0) - start_block * _ROW_BLOCK_SIZE, 0)
        trim_to = trim_from + max(
            int(end_row if end_row is not None else start_row or 0)
            - int(start_row or 0)
            + 1,
            0,
        )
        window_rows = merged_rows[trim_from:trim_to]
        expected_rows = max(
            int(end_row if end_row is not None else start_row or 0)
            - int(start_row or 0)
            + 1,
            0,
        )
        if len(window_rows) < expected_rows:
            return None

        if include_grand_total and isinstance(grand_total_row, dict):
            window_rows = [*window_rows, dict(grand_total_row)]

        metadata_source = metadata_source or {}
        return TanStackResponse(
            data=window_rows,
            columns=[dict(column) for column in (metadata_source.get("columns") or [])],
            pagination=(
                dict(metadata_source["pagination"])
                if isinstance(metadata_source.get("pagination"), dict)
                else metadata_source.get("pagination")
            ),
            total_rows=metadata_source.get("total_rows"),
            grouping=(
                [
                    dict(item) if isinstance(item, dict) else item
                    for item in (metadata_source.get("grouping") or [])
                ]
                if metadata_source.get("grouping")
                else metadata_source.get("grouping")
            ),
            version=metadata_source.get("version"),
            layout_revision=metadata_source.get("layout_revision"),
            col_schema=None,
            color_scale_stats=None,
        )

    @staticmethod
    def _trim_transport_columns(
        columns: Optional[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        trimmed_columns: List[Dict[str, Any]] = []
        for column in columns or []:
            if not isinstance(column, dict):
                continue
            column_id = column.get("id")
            if not column_id:
                continue
            if column_id == "__col_schema":
                trimmed_columns.append(
                    {
                        "id": "__col_schema",
                        "col_schema": column.get("col_schema"),
                    }
                )
                continue
            trimmed_column = {"id": column_id}
            for key in ("header", "headerVal", "accessorKey"):
                if column.get(key) is not None:
                    trimmed_column[key] = column.get(key)
            trimmed_columns.append(trimmed_column)
        return trimmed_columns

    def _finalize_windowed_response(
        self, response: "TanStackResponse"
    ) -> "TanStackResponse":
        if response.columns:
            response.columns = self._trim_transport_columns(response.columns)
        return response

    def _get_cached_window_response(
        self, cache_key: str
    ) -> Optional["TanStackResponse"]:
        cached_response = self._locked_cache_lookup(
            self._response_window_cache, cache_key
        )
        if cached_response is None:
            return None
        return self._clone_response(cached_response)

    def _store_window_response(
        self, cache_key: str, response: "TanStackResponse", *, _return_cloned: bool = False
    ) -> Any:
        finalized = self._finalize_windowed_response(response)
        cloned = self._clone_response(finalized)
        self._locked_cache_store(
            self._response_window_cache,
            cache_key,
            cloned,
            max_size=_RESPONSE_WINDOW_CACHE_SIZE,
            ttl_seconds=_LOCAL_CACHE_TTL_SECONDS,
        )
        if _return_cloned:
            return finalized, cloned
        return finalized

    def _schedule_viewport_prefetch(
        self,
        request: "TanStackRequest",
        start_row: int,
        end_row: int,
        expanded_paths: Union[List[List[str]], bool, None],
        col_start: int,
        col_end: Optional[int],
        include_grand_total: bool,
        total_rows: Optional[int],
        total_center_cols: Optional[int],
    ) -> None:
        if end_row is None or total_rows is None:
            return
        if not self.viewport_prefetch_enabled:
            return
        task_manager = getattr(self.controller, "task_manager", None)
        if task_manager is None:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return

        row_window_size = max(int(end_row) - int(start_row or 0) + 1, 1)
        next_row_start = int(end_row) + 1
        next_row_end = min(
            max(int(total_rows) - 1, 0), next_row_start + row_window_size - 1
        )
        prefetch_specs: List[Dict[str, Any]] = []

        if next_row_start <= next_row_end:
            prefetch_specs.append(
                {
                    "start_row": next_row_start,
                    "end_row": next_row_end,
                    "col_start": col_start,
                    "col_end": col_end,
                    "needs_col_schema": False,
                    "label": "rows",
                }
            )

        if (
            total_center_cols is not None
            and col_end is not None
            and col_start is not None
            and int(col_end) < int(total_center_cols) - 1
        ):
            col_window_size = max(int(col_end) - int(col_start) + 1, 1)
            next_col_start = int(col_end) + 1
            next_col_end = min(
                int(total_center_cols) - 1, next_col_start + col_window_size - 1
            )
            if next_col_start <= next_col_end:
                prefetch_specs.append(
                    {
                        "start_row": start_row,
                        "end_row": end_row,
                        "col_start": next_col_start,
                        "col_end": next_col_end,
                        "needs_col_schema": True,
                        "label": "cols",
                    }
                )

        for prefetch_spec in prefetch_specs:
            cache_key = self._response_window_cache_key(
                request,
                prefetch_spec["start_row"],
                prefetch_spec["end_row"],
                expanded_paths,
                prefetch_spec["col_start"],
                prefetch_spec["col_end"],
                prefetch_spec["needs_col_schema"],
                include_grand_total,
                None,
            )
            if not self._mark_prefetch_request_pending(cache_key):
                continue

            async def _run_prefetch(spec=prefetch_spec, prefetch_cache_key=cache_key):
                try:
                    await self.handle_virtual_scroll_request(
                        request,
                        spec["start_row"],
                        spec["end_row"],
                        expanded_paths=expanded_paths,
                        col_start=spec["col_start"],
                        col_end=spec["col_end"],
                        needs_col_schema=spec["needs_col_schema"],
                        include_grand_total=include_grand_total,
                        requested_center_ids=None,
                        _allow_prefetch=False,
                    )
                except Exception as exc:
                    if self._debug:
                        _adapter_logger.debug(
                            "Background prefetch %s failed: %s", spec["label"], exc
                        )
                finally:
                    self._clear_prefetch_request_pending(prefetch_cache_key)

            task_manager.create_task(
                _run_prefetch(),
                name=f"pivot_prefetch_{prefetch_spec['label']}_{cache_key[:8]}",
            )

    def _complete_virtual_scroll_response(
        self,
        response: "TanStackResponse",
        response_cache_key: str,
        request: "TanStackRequest",
        start_row: int,
        end_row: int,
        expanded_paths: Union[List[List[str]], bool, None],
        col_start: int,
        col_end: Optional[int],
        include_grand_total: bool,
        allow_prefetch: bool,
        requested_center_ids: Optional[List[str]] = None,
    ) -> "TanStackResponse":
        stored_response, cloned_response = self._store_window_response(
            response_cache_key, response, _return_cloned=True
        )
        self._store_row_block_window(
            request,
            cloned_response,
            start_row,
            end_row,
            expanded_paths,
            col_start,
            col_end,
            requested_center_ids,
            _already_cloned=True,
        )
        if allow_prefetch:
            total_center_cols = None
            if isinstance(stored_response.col_schema, dict):
                total_center_cols = stored_response.col_schema.get("total_center_cols")
            self._schedule_viewport_prefetch(
                request,
                start_row,
                end_row,
                expanded_paths,
                col_start,
                col_end,
                include_grand_total,
                stored_response.total_rows,
                total_center_cols,
            )
        return stored_response

    def _get_center_col_ids_from_rows(
        self, rows: list, row_meta_keys: set, pinned_ids: set
    ) -> list:
        """Return an ordered list of center (non-pinned, non-meta) column IDs from result rows."""
        seen = []
        all_known = set(row_meta_keys) | set(pinned_ids)
        for row in rows:
            if row.keys() <= all_known:
                continue
            for col_id in row:
                if col_id not in all_known:
                    seen.append(col_id)
                    all_known.add(col_id)
        return seen

    def _get_center_col_ids_from_columns(
        self, columns: list, excluded_ids: set
    ) -> list:
        """Return ordered center column IDs from response column metadata when available."""
        ordered = []
        seen = set()
        for col in columns or []:
            if not isinstance(col, dict):
                continue
            col_id = col.get("id")
            if not isinstance(col_id, str):
                continue
            if col_id in excluded_ids or col_id in seen:
                continue
            ordered.append(col_id)
            seen.add(col_id)
        return ordered

    def _measure_axis_placeholder_center_ids(
        self, request: "TanStackRequest"
    ) -> set[str]:
        """Return request measure placeholders hidden by measure-axis rendering."""
        measure_axis = MeasureAxisConfig.from_dict(
            getattr(request, "measure_axis", None)
        )
        if not measure_axis or measure_axis.placement not in {"rows", "columns"}:
            return set()

        # Only hide columns that are actual measure-axis members.  If the user
        # has configured a partial member list (e.g. ["Sales", "Cost"] out of
        # ["Sales", "Cost", "Delta"]) non-member aggregation columns must remain
        # visible — they are not replaced by the single valueField column.
        member_aliases: set[str] = set()
        for m in measure_axis.members or []:
            if isinstance(m, dict):
                alias = m.get("measureAlias") or m.get("alias") or m.get("id") or ""
                if alias:
                    member_aliases.add(str(alias))

        placeholder_ids: set[str] = set()
        for col in request.columns or []:
            if not isinstance(col, dict) or not col.get("id"):
                continue
            if col.get("aggregationFn") or self._is_measure_formula_column(col):
                col_id = str(col.get("id"))
                # When no members are configured every measure participates; when
                # members are configured only those members are replaced.
                if not member_aliases or col_id in member_aliases:
                    placeholder_ids.add(col_id)
        return placeholder_ids

    @staticmethod
    def _implicit_formula_ref_request_ids(request: "TanStackRequest") -> set[str]:
        return TanStackResponseMapper._implicit_formula_ref_request_ids(request)

    @staticmethod
    def _matches_materialized_request_id(column_id: Any, request_id: Any) -> bool:
        return TanStackResponseMapper._matches_materialized_request_id(
            column_id, request_id
        )

    def _is_implicit_formula_ref_center_id(
        self, column_id: Any, request: "TanStackRequest"
    ) -> bool:
        return self._response_mapper._is_implicit_formula_ref_center_id(
            column_id, request
        )

    def _filter_implicit_formula_ref_columns(
        self,
        columns: Optional[List[Dict[str, Any]]],
        request: "TanStackRequest",
    ) -> List[Dict[str, Any]]:
        return self._response_mapper._filter_implicit_formula_ref_columns(
            columns, request
        )

    def _reorder_materialized_dynamic_ids(
        self,
        observed_ids: list,
        request: "TanStackRequest",
    ) -> list[str]:
        return self._response_mapper._reorder_materialized_dynamic_ids(
            observed_ids, request
        )

    def _synthesize_response_column(
        self,
        col_id: str,
        request_lookup: Dict[str, Dict[str, Any]],
        formula_labels: Dict[str, str],
    ) -> Dict[str, Any]:
        """Build response metadata for a materialized center column id."""
        request_col = request_lookup.get(col_id)
        if isinstance(request_col, dict):
            return dict(request_col)

        if col_id.startswith("__RowTotal__"):
            measure_key = col_id.replace("__RowTotal__", "")
            return {
                "id": col_id,
                "header": f"Total {measure_key.replace('_', ' ').title()}",
                "accessorKey": col_id,
                "isRowTotal": True,
            }

        formula_label = None
        for formula_id, label in formula_labels.items():
            if self._matches_formula_column_id(col_id, formula_id):
                formula_label = label
                break

        synthesized = {
            "id": col_id,
            "header": formula_label or col_id.replace("_", " ").title(),
            "accessorKey": col_id,
        }
        if formula_label:
            synthesized["formulaLabel"] = formula_label
        return synthesized

    def _build_authoritative_response_columns(
        self,
        tanstack_result: "TanStackResponse",
        request: "TanStackRequest",
        row_meta_keys: set,
        excluded_ids: set,
    ) -> tuple[list[str], list[Dict[str, Any]]]:
        """Return the canonical center-column order and response column manifest.

        The backend may start from request placeholders, but the emitted payload
        must reflect the columns that are actually materialized after pivot/window
        processing and formula evaluation. Both response.columns and col_schema are
        derived from this manifest so they cannot drift apart.
        """
        request_lookup = {
            str(col.get("id")): dict(col)
            for col in (request.columns or [])
            if isinstance(col, dict) and col.get("id")
        }
        current_lookup = {
            str(col.get("id")): dict(col)
            for col in (tanstack_result.columns or [])
            if isinstance(col, dict) and col.get("id")
        }
        formula_labels = {
            str(col.get("id")): self._formula_label(col)
            for col in (request.columns or [])
            if isinstance(col, dict) and col.get("isFormula") and col.get("id")
        }

        center_col_ids = self._get_center_col_ids_from_columns(
            tanstack_result.columns, excluded_ids
        )
        seen_center_ids = set(center_col_ids)
        for col_id in self._get_center_col_ids_from_rows(
            tanstack_result.data, row_meta_keys, excluded_ids
        ):
            if col_id not in seen_center_ids:
                center_col_ids.append(col_id)
                seen_center_ids.add(col_id)
        measure_axis_placeholder_ids = self._measure_axis_placeholder_center_ids(
            request
        )
        if measure_axis_placeholder_ids:
            center_col_ids = [
                col_id
                for col_id in center_col_ids
                if col_id not in measure_axis_placeholder_ids
            ]
        center_col_ids = [
            col_id
            for col_id in center_col_ids
            if not self._is_implicit_formula_ref_center_id(col_id, request)
        ]
        center_col_ids = self._reorder_materialized_dynamic_ids(center_col_ids, request)

        authoritative_columns: list[Dict[str, Any]] = []
        seen_column_ids = set()

        for col_id in list(request.grouping or []):
            col = current_lookup.get(col_id) or request_lookup.get(col_id)
            if not isinstance(col, dict):
                col = {
                    "id": col_id,
                    "header": col_id.replace("_", " ").title(),
                    "accessorKey": col_id,
                }
            if col_id not in seen_column_ids:
                authoritative_columns.append(col)
                seen_column_ids.add(col_id)

        for col_id in center_col_ids:
            col = (
                current_lookup.get(col_id)
                or request_lookup.get(col_id)
                or self._synthesize_response_column(
                    col_id, request_lookup, formula_labels
                )
            )
            if col_id not in seen_column_ids:
                authoritative_columns.append(col)
                seen_column_ids.add(col_id)

        return center_col_ids, authoritative_columns

    @staticmethod
    def _dynamic_request_ids(request: "TanStackRequest") -> list[str]:
        return TanStackResponseMapper._dynamic_request_ids(request)

    @staticmethod
    def _column_formula_request_ids(request: "TanStackRequest") -> list[str]:
        return [
            str(col.get("id"))
            for col in (request.columns or [])
            if FormulaEngineMixin._is_column_formula_column(col) and col.get("id")
        ]

    def _resolve_pivot_column_layout(
        self, discovered_values: list[dict[str, Any]], spec: PivotSpec,
        request: "TanStackRequest", user: Optional[User],
    ):
        """Resolve current authorized catalog before interpreting display indices."""
        measure_ids = self._dynamic_request_ids(request)
        active = [column for column in (request.columns or [])
                  if isinstance(column, dict) and column.get("id") in measure_ids
                  and column.get("aggregationFn") and column.get("aggregationField")]
        field_counts = Counter(str(column["aggregationField"]) for column in active)
        # The generated cell ID changes with its aggregate function. The saved
        # display slot represents a unique source measure and must survive that
        # change. Keep the historic sum identity so existing sum layouts remain
        # valid; aliases and duplicate source measures retain their own IDs.
        layout_measure_keys = {
            str(column["id"]): f"{column['aggregationField']}_sum"
            for column in active
            if field_counts[str(column["aggregationField"])] == 1
            and str(column["id"]) == f"{column['aggregationField']}_{column['aggregationFn']}"
        }
        catalog = list(catalog_from_pivot_values(
            discovered_values, list(spec.columns), measure_ids,
            row_totals=bool(request.row_totals),
            layout_measure_keys=layout_measure_keys,
        ))
        for formula_id in self._column_formula_request_ids(request):
            catalog.append(PivotColumnIdentity(
                (("$formula", formula_id),), formula_id, formula_id,
            ))
        order_measure = spec.measures[0] if spec.measures else None
        structural_key = hashlib.sha256(self._stable_json({
            "table": spec.table,
            "pivot_columns": list(spec.columns or []),
            "filters": spec.filters or [],
            "custom_dimensions": spec.custom_dimensions or [],
            "measure_axis": spec.measure_axis.to_dict() if spec.measure_axis else None,
            "column_sort_options": spec.column_sort_options or {},
            "order_measure": vars(order_measure) if order_measure is not None else None,
        }).encode("utf-8")).hexdigest()
        principal = getattr(user, "id", None) or getattr(user, "user_id", None)
        owner_scope = hashlib.sha256(self._stable_json({
            "principal": principal,
            "filters": spec.filters,
            "table": spec.table,
        }).encode("utf-8")).hexdigest()
        if request.layout_manifest is not None:
            saved_ids = request.layout_manifest
        elif request.layout_revision:
            saved_ids = self._layout_revisions.get(
                owner_scope, structural_key, request.layout_revision
            )
        else:
            saved_ids = ()
        resolved = resolve_column_layout(
            catalog, saved_ids, structural_key=structural_key,
        )
        self._layout_revisions.put(
            owner_scope, structural_key, resolved.revision, saved_ids,
        )
        return resolved

    @row_scoped
    async def apply_full_export_layout(
        self, response: "TanStackResponse", request: "TanStackRequest",
        *, user: Optional[User] = None, row_scope: Any = None,
        include_schema: bool = False,
    ) -> "TanStackResponse":
        """Use the viewport's server order for a full hierarchy export."""
        scoped_request = self._request_with_row_scope(request, row_scope)
        spec = self.convert_tanstack_request_to_pivot_spec(scoped_request)
        if user:
            spec = apply_rls_to_spec(spec, user, row_scope=row_scope)
        if not spec.columns or not spec.pivot_config or not spec.pivot_config.enabled:
            return response
        discovered = await self._discover_pivot_column_values(spec)
        resolved = self._resolve_pivot_column_layout(discovered, spec, scoped_request, user)
        by_id = {column.get("id"): column for column in response.columns or []
                 if isinstance(column, dict) and column.get("id")}
        center_ids = {column.cell_id for column in resolved.columns}
        leading = [column for column in response.columns or []
                   if isinstance(column, dict) and column.get("id") not in center_ids]
        ordered_columns = [*leading, *(
            by_id[column.cell_id] for column in resolved.columns if column.cell_id in by_id
        )]
        layout_schema = {
            "layoutRevision": resolved.revision,
            "total_center_cols": len(resolved.columns),
            "columns": [
                {"index": index, "id": column.cell_id,
                 "layoutId": column.layout_id, "groupId": column.group_id}
                for index, column in enumerate(resolved.columns)
            ],
        } if include_schema else response.col_schema
        return replace(response, columns=ordered_columns,
                       col_schema=layout_schema, layout_revision=resolved.revision)

    def _resolve_center_window_ids(
        self,
        center_col_ids: list[str],
        request: "TanStackRequest",
        col_start: int,
        col_end: Optional[int],
        needs_col_schema: bool,
        requested_center_ids: Optional[List[str]] = None,
    ) -> Optional[list[str]]:
        if not center_col_ids:
            return None

        if requested_center_ids:
            requested_set = {column_id for column_id in requested_center_ids if isinstance(column_id, str)}
            requested_order = [
                col_id
                for col_id in center_col_ids
                if col_id in requested_set
            ]
            return requested_order or None

        safe_start = max(0, min(int(col_start or 0), len(center_col_ids) - 1))
        if col_end is not None:
            safe_end = max(safe_start, min(int(col_end), len(center_col_ids) - 1))
            return center_col_ids[safe_start : safe_end + 1]

        if needs_col_schema:
            safe_end = min(
                len(center_col_ids) - 1,
                safe_start + _INITIAL_SCHEMA_WINDOW_CENTER_COLS - 1,
            )
            return center_col_ids[safe_start : safe_end + 1]

        return None

    def _materialized_pivot_values_for_window(
        self,
        window_center_ids: Optional[list[str]],
        request: "TanStackRequest",
        all_center_ids: Optional[list[str]] = None,
    ) -> Optional[list[str]]:
        if not window_center_ids:
            return None

        dynamic_ids = self._dynamic_request_ids(request)
        if not dynamic_ids:
            return None

        ordered_values: list[str] = []
        seen_values: set[str] = set()

        source_center_ids = list(window_center_ids)
        visible_center_ids = set(
            col_id for col_id in window_center_ids if isinstance(col_id, str)
        )
        available_center_ids = set(
            col_id
            for col_id in (all_center_ids or window_center_ids)
            if isinstance(col_id, str)
        )
        # A row sort on a pivoted value remains a query dependency even after
        # horizontal scrolling moves that column off screen. Without it, each
        # column window can produce a different row order and cached rows appear
        # blank or attached to the wrong virtual index.
        for sort_spec in request.sorting or []:
            sort_id = sort_spec.get("id") if isinstance(sort_spec, dict) else None
            if (
                isinstance(sort_id, str)
                and sort_id in available_center_ids
                and sort_id not in visible_center_ids
            ):
                source_center_ids.append(sort_id)

        for col in request.columns or []:
            if not self._is_column_formula_column(col):
                continue
            formula_id = str(col.get("id") or "")
            if not formula_id or formula_id not in visible_center_ids:
                continue
            source_center_ids.extend(
                reference
                for reference in self._extract_column_formula_references(
                    col.get("formulaExpr", "")
                )
                if reference not in visible_center_ids
            )

        for col_id in source_center_ids:
            if not isinstance(col_id, str) or col_id.startswith("__RowTotal__"):
                continue
            for dynamic_id in dynamic_ids:
                suffix = f"_{dynamic_id}"
                if not col_id.endswith(suffix):
                    continue
                raw_value = col_id[: -len(suffix)]
                if raw_value and raw_value not in seen_values:
                    ordered_values.append(raw_value)
                    seen_values.add(raw_value)
                break

        return ordered_values

    async def _discover_pivot_column_values(self, spec: PivotSpec) -> list[dict[str, Any]]:
        return (await self._discover_pivot_columns(spec))[0]

    async def _discover_pivot_columns(
        self, spec: PivotSpec
    ) -> Tuple[list[dict[str, Any]], Tuple[Tuple[Any, ...], ...]]:
        """The column catalog and its typed members (one core read, cached together)."""
        if not spec.columns:
            return [], ()

        local_cache_key = self._pivot_column_catalog_cache_key(spec)
        cached_catalog = self._locked_cache_lookup(
            self._pivot_column_catalog_cache, local_cache_key
        )
        if isinstance(cached_catalog, dict) and isinstance(
            cached_catalog.get("values"), list
        ) and isinstance(cached_catalog.get("members"), tuple):
            return list(cached_catalog["values"]), cached_catalog["members"]

        from .transport.core_bridge import core_column_catalog_members, run_core_read

        # REQ-PIV-001/002: the ordered catalog from the core.
        discovered_values, members = await run_core_read(
            self.controller, spec.table, lambda resolve: core_column_catalog_members(resolve, spec))
        self._locked_cache_store(
            self._pivot_column_catalog_cache,
            local_cache_key,
            {"values": list(discovered_values), "members": tuple(members)},
            max_size=_PIVOT_CATALOG_CACHE_SIZE,
            ttl_seconds=_LOCAL_CACHE_TTL_SECONDS,
        )
        return discovered_values, tuple(members)

    def _apply_col_windowing(
        self,
        tanstack_result: "TanStackResponse",
        request: "TanStackRequest",
        col_start: int,
        col_end: Optional[int],
        needs_col_schema: bool,
        requested_center_ids: Optional[List[str]] = None,
        discovered_center_ids: Optional[List[str]] = None,
        discovered_layout: Optional[dict[str, PivotColumnIdentity]] = None,
        pinned_center_ids: Optional[list[str]] = None,
        layout_revision: Optional[str] = None,
    ) -> "TanStackResponse":
        """Apply column slicing and optionally build col_schema."""
        if (not needs_col_schema and col_end is None and not requested_center_ids
                and discovered_center_ids is None):
            return self._finalize_windowed_response(tanstack_result)

        row_meta_keys = set(ROW_META_KEYS)
        pinned_ids = set(request.grouping or [])
        request_dimension_ids = {
            col.get("id")
            for col in (request.columns or [])
            if isinstance(col, dict)
            and not col.get("aggregationFn")
            and not col.get("isFormula")
            and col.get("id")
        }
        excluded_ids = set(row_meta_keys) | pinned_ids | request_dimension_ids
        cache_key = (
            self._cache_generation_value(),
            request.table,
            tuple(str(field) for field in (request.grouping or [])),
            tuple(sorted(str(column_id) for column_id in excluded_ids)),
            self._center_column_catalog_cache_key(request),
        )

        if discovered_center_ids is not None:
            center_col_ids = [
                str(col_id)
                for col_id in discovered_center_ids
                if isinstance(col_id, str)
            ]
            self._center_col_ids_cache_set(cache_key, center_col_ids)
            if tanstack_result.columns:
                _, authoritative_columns = self._build_authoritative_response_columns(
                    tanstack_result, request, row_meta_keys, excluded_ids
                )
                tanstack_result.columns = authoritative_columns
        else:
            cached_center_col_ids = self._center_col_ids_cache_get(cache_key)

        if discovered_center_ids is None and (
            needs_col_schema or cached_center_col_ids is None or requested_center_ids
        ):
            center_col_ids, authoritative_columns = (
                self._build_authoritative_response_columns(
                    tanstack_result, request, row_meta_keys, excluded_ids
                )
            )
            tanstack_result.columns = authoritative_columns
            self._center_col_ids_cache_set(cache_key, center_col_ids)
        elif discovered_center_ids is None:
            center_col_ids = cached_center_col_ids
            # Always rebuild columns from rows to catch newly-materialized center columns.
            # The cache may have been populated with an empty list before data arrived,
            # so we must re-scan rows even when cache exists.
            _, authoritative_columns = self._build_authoritative_response_columns(
                tanstack_result, request, row_meta_keys, excluded_ids
            )
            tanstack_result.columns = authoritative_columns
            # If the rebuilt center columns differ from cached, update the cache
            fresh_center_col_ids = self._get_center_col_ids_from_columns(
                authoritative_columns, excluded_ids
            )
            if fresh_center_col_ids and fresh_center_col_ids != center_col_ids:
                center_col_ids = self._reorder_materialized_dynamic_ids(
                    fresh_center_col_ids, request
                )
                self._center_col_ids_cache_set(cache_key, center_col_ids)

        effective_window_ids = self._resolve_center_window_ids(
            center_col_ids,
            request,
            col_start,
            col_end,
            needs_col_schema,
            requested_center_ids=requested_center_ids,
        )
        if effective_window_ids is not None and pinned_center_ids:
            effective_window_ids = list(dict.fromkeys(
                [*effective_window_ids, *pinned_center_ids]
            ))
        layout_revision = layout_revision or tanstack_result.layout_revision
        tanstack_result.layout_revision = layout_revision
        if effective_window_ids is None and discovered_center_ids:
            center_set = set(center_col_ids)
            column_lookup = {column.get("id"): column for column in tanstack_result.columns or []
                             if isinstance(column, dict) and column.get("id")}
            leading_columns = [column for column in tanstack_result.columns or []
                               if isinstance(column, dict) and column.get("id") not in center_set]
            tanstack_result.columns = [*leading_columns, *(
                column_lookup[col_id] for col_id in center_col_ids if col_id in column_lookup
            )]

        if needs_col_schema:
            existing_layout_schema = tanstack_result.col_schema if isinstance(tanstack_result.col_schema, dict) else {}
            existing_layout_by_cell = {
                column.get("id"): column
                for column in existing_layout_schema.get("columns", [])
                if isinstance(column, dict) and column.get("id")
            }
            column_lookup = {
                col.get("id"): col
                for col in (tanstack_result.columns or [])
                if isinstance(col, dict) and col.get("id")
            }
            schema_center_ids = effective_window_ids or center_col_ids
            center_indices = {identity: index for index, identity in enumerate(center_col_ids)}
            tanstack_result.col_schema = {
                "total_center_cols": len(center_col_ids),
                **({"layoutRevision": layout_revision} if layout_revision else {}),
                "columns": [
                    {
                        "index": center_indices[col_id],
                        "id": col_id,
                        **({
                            "layoutId": discovered_layout[col_id].layout_id,
                            "groupId": discovered_layout[col_id].group_id,
                        } if discovered_layout and col_id in discovered_layout else {
                            key: existing_layout_by_cell[col_id][key]
                            for key in ("layoutId", "groupId")
                            if col_id in existing_layout_by_cell
                            and key in existing_layout_by_cell[col_id]
                        }),
                        "size": 140,
                        "header": (
                            column_lookup.get(col_id, {}).get("header")
                            or column_lookup.get(col_id, {}).get("accessorKey")
                            or col_id
                        ),
                        "headerVal": (
                            column_lookup.get(col_id, {}).get("headerVal")
                            if column_lookup.get(col_id, {}).get("headerVal")
                            is not None
                            else (
                                column_lookup.get(col_id, {}).get("header")
                                or column_lookup.get(col_id, {}).get("accessorKey")
                                or col_id
                            )
                        ),
                    }
                    for col_id in schema_center_ids
                ],
            }

        if requested_center_ids:
            requested_set = {column_id for column_id in requested_center_ids if isinstance(column_id, str)}
            requested_order = [
                col_id
                for col_id in center_col_ids
                if col_id in requested_set
            ]
            requested_ids = set(requested_order)
            keep_ids = requested_ids | pinned_ids
            keep_all = row_meta_keys | keep_ids

            tanstack_result.data = [
                row if row.keys() <= keep_all else {k: v for k, v in row.items() if k in keep_all}
                for row in tanstack_result.data
            ]

            if tanstack_result.columns:
                column_lookup = {
                    col.get("id"): col
                    for col in tanstack_result.columns
                    if isinstance(col, dict) and col.get("id")
                }
                ordered_columns = []
                seen = set()
                for col_id in list(request.grouping or []) + requested_order:
                    if col_id in column_lookup and col_id not in seen:
                        ordered_columns.append(column_lookup[col_id])
                        seen.add(col_id)
                for col in tanstack_result.columns:
                    if not isinstance(col, dict):
                        continue
                    col_id = col.get("id")
                    if not col_id or col_id in seen:
                        continue
                    if col_id in keep_ids:
                        ordered_columns.append(col)
                        seen.add(col_id)
                tanstack_result.columns = ordered_columns
            return self._finalize_windowed_response(tanstack_result)

        if effective_window_ids:
            window_ids = set(effective_window_ids)
            keep_ids = window_ids | pinned_ids
            keep_all = row_meta_keys | keep_ids

            tanstack_result.data = [
                row if row.keys() <= keep_all else {k: v for k, v in row.items() if k in keep_all}
                for row in tanstack_result.data
            ]

            if tanstack_result.columns:
                column_lookup = {
                    col.get("id"): col
                    for col in tanstack_result.columns
                    if isinstance(col, dict) and col.get("id")
                }
                ordered_columns = []
                seen = set()
                for col_id in list(request.grouping or []) + [
                    col_id for col_id in center_col_ids if col_id in keep_ids
                ]:
                    if col_id in column_lookup and col_id not in seen:
                        ordered_columns.append(column_lookup[col_id])
                        seen.add(col_id)
                tanstack_result.columns = ordered_columns

        return self._finalize_windowed_response(tanstack_result)

    def load_data(self, data, table_name: str) -> None:
        """
        Load data into the pivot engine from any supported source type.

        Accepts pandas DataFrame, polars DataFrame, Ibis table expression,
        connection string dict, or PyArrow Table. Auto-detects the type and
        converts to the engine's native Arrow representation.

        Parameters
        ----------
        data : pd.DataFrame | pl.DataFrame | ibis.Table | dict | pa.Table
            The data source.
        table_name : str
            The name under which the table will be registered. Use the same
            table_name in pivot requests to query this data.

        Raises
        ------
        DataInputError
            If the data type is not supported.
        """
        from .data_input import normalize_data_input  # noqa: PLC0415  (lazy import)

        normalize_data_input(data, table_name, self.controller)

    def _log_request(self, method: str, request: "TanStackRequest", **extra):
        if not self._debug:
            return
        _adapter_logger.debug(
            "adapter_request",
            extra={
                "method": method,
                "table": getattr(request, "table", None),
                "grouping": getattr(request, "grouping", None),
                "totals": getattr(request, "totals", None),
                "filters_keys": list((getattr(request, "filters", None) or {}).keys()),
                **extra,
            },
        )

    def _log_response(self, method: str, rows: list):
        if not self._debug:
            return
        total_rows = [
            r for r in rows if r.get("_isTotal") or r.get("_path") == "__grand_total__"
        ]
        _adapter_logger.debug(
            "adapter_response",
            extra={
                "method": method,
                "row_count": len(rows),
                "total_row_count": len(total_rows),
                "has_grand_total": any(r.get("_id") == "Grand Total" for r in rows),
            },
        )

    # Formula engine and window function methods provided by FormulaEngineMixin

    @staticmethod
    def _derive_measure_axis_members(
        measure_axis: Optional[MeasureAxisConfig],
        measures: List[Measure],
        request_columns: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        return TanStackRequestMapper._derive_measure_axis_members(
            measure_axis, measures, request_columns
        )

    @staticmethod
    def _formula_alias_lookup_from_request_columns(
        request_columns: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        return TanStackRequestMapper._formula_alias_lookup_from_request_columns(
            request_columns
        )

    @staticmethod
    def _canonicalize_measure_formula_expression(
        expression: Any, alias_lookup: Dict[str, str]
    ) -> str:
        return TanStackRequestMapper._canonicalize_measure_formula_expression(
            expression, alias_lookup
        )

    def convert_tanstack_request_to_pivot_spec(
        self, request: TanStackRequest
    ) -> PivotSpec:
        """Convert TanStack request to PivotSpec format"""
        return self._request_mapper.convert_tanstack_request_to_pivot_spec(request)

        # Extract grouping columns as hierarchy

        # Extract measure columns

        # Convert TanStack sorting to PivotSpec sorting.
        # The hierarchy column has id="hierarchy" in the frontend but the backend
        # needs the actual first row dimension field name.

        # Inject default sort specs for custom dimensions with explicit order settings.
        # Only injected when: user hasn't already sorted by that field AND the
        # dimension is actually present in the current grouping (avoids carrying
        # unnecessary hidden sort-key columns through unrelated queries).

        # Handle pagination

        # Parse column_cursor from global_filter

    def _map_tanstack_operator(self, tanstack_op: str) -> str:
        return self._request_mapper._map_tanstack_operator(tanstack_op)

    def convert_pivot_result_to_tanstack_format(
        self,
        pivot_result: Any,
        tanstack_request: TanStackRequest,
        version: Optional[int] = None,
        *,
        _core_normalized: bool = False,
    ) -> TanStackResponse:
        """Convert pivot engine result to TanStack format"""
        return self._response_mapper.convert_pivot_result_to_tanstack_format(
            pivot_result,
            tanstack_request,
            version,
            _core_normalized=_core_normalized,
        )

        # Convert from pivot format to TanStack row format

        # Hidden sort-key fields are transport-only helpers.
        # Keep them available for backend ORDER BY, but never expose them to the frontend.
        # Also sanitize NaN floats → None so JSON serialization produces valid null.

        # Enrich rows for TanStack Hierarchy display

        # Apply pivot window functions (% row/% col/% grand total) after hierarchy
        # metadata is normalized, so grand-total detection is stable.

        # Apply formula columns (post-aggregation calculated fields) after window functions.

        # Calculate pagination info if needed

        # Dynamic Column Generation for Pivot
        # If we have pivot columns, we need to update the response columns

        # In pivot mode (column dimensions present), base measure columns are placeholders.
        # Keep only expanded dynamic pivot columns to avoid showing extra plain measures.

        # Detect dynamic columns using backend schema order first so pivot column
        # sorting survives transport and horizontal windowing.

        # Apply formula columns here so that all call paths (handle_request,
        # handle_hierarchical_request, handle_virtual_scroll_request) evaluate
        # formula columns. handle_request also calls _apply_formula_columns after
        # window functions, but that second pass is idempotent so it is safe.

    async def handle_update(
        self,
        request: TanStackRequest,
        update_payload: Dict[str, Any],
        *,
        user: Optional[User] = None,
        mutation_grant: Any = None,
    ) -> bool:
        """
        Handle a cell update request from the frontend.
        """
        result = await self.handle_updates(
            request,
            [update_payload],
            user=user,
            mutation_grant=mutation_grant,
        )
        return bool(result.get("updated", 0))

    def _invalidate_local_caches(self) -> None:
        self._viewport_cache.invalidate_all()

    @staticmethod
    def _normalize_aggregation_name(value: Any) -> str:
        return str(value or "").strip().lower()

    def _request_column_dimension_ids(self, request: TanStackRequest) -> List[str]:
        grouping_ids = {str(field) for field in (request.grouping or []) if field}
        ordered_dimensions: List[str] = []
        for col in request.columns or []:
            if not isinstance(col, dict):
                continue
            column_id = str(col.get("id") or "").strip()
            if (
                not column_id
                or column_id in grouping_ids
                or col.get("aggregationFn")
                or col.get("isFormula")
            ):
                continue
            if column_id not in ordered_dimensions:
                ordered_dimensions.append(column_id)
        return ordered_dimensions

    def _resolve_request_column_spec(
        self,
        request: TanStackRequest,
        column_id: Any,
    ) -> Optional[Dict[str, Any]]:
        normalized_column_id = str(column_id or "").strip()
        if not normalized_column_id:
            return None

        column_dimensions = self._request_column_dimension_ids(request)
        matched_column = None
        matched_prefix = None

        for col in request.columns or []:
            if not isinstance(col, dict):
                continue
            request_column_id = str(col.get("id") or "").strip()
            accessor_key = str(col.get("accessorKey") or "").strip()
            if (
                request_column_id == normalized_column_id
                or accessor_key == normalized_column_id
            ):
                matched_column = col
                break

        if matched_column is None:
            lowered_column_id = normalized_column_id.lower()
            row_total_prefix = "__RowTotal__"
            row_total_payload = (
                normalized_column_id[len(row_total_prefix) :]
                if normalized_column_id.startswith(row_total_prefix)
                else None
            )
            for col in request.columns or []:
                if not isinstance(col, dict):
                    continue
                request_column_id = str(col.get("id") or "").strip()
                if not request_column_id:
                    continue
                if (
                    row_total_payload
                    and request_column_id.lower() == row_total_payload.lower()
                ):
                    matched_column = col
                    break
                if col.get("isFormula"):
                    if self._matches_formula_column_id(
                        normalized_column_id, request_column_id
                    ):
                        matched_column = col
                        suffix = f"_{request_column_id}"
                        if lowered_column_id.endswith(suffix.lower()) and len(
                            normalized_column_id
                        ) > len(suffix):
                            matched_prefix = normalized_column_id[: -len(suffix)]
                        break
                    continue
                aggregation_field = str(col.get("aggregationField") or "").strip()
                aggregation_fn = self._normalize_aggregation_name(
                    col.get("aggregationFn")
                )
                if not aggregation_field or not aggregation_fn:
                    continue
                suffix = f"_{aggregation_field}_{aggregation_fn}"
                if lowered_column_id.endswith(suffix.lower()):
                    matched_column = col
                    if len(normalized_column_id) > len(suffix):
                        matched_prefix = normalized_column_id[: -len(suffix)]
                        if matched_prefix.endswith("_"):
                            matched_prefix = matched_prefix[:-1]
                    break

        spec = {
            "columnId": normalized_column_id,
            "targetColumn": normalized_column_id,
            "aggregationField": None,
            "aggregationFn": None,
            "windowFn": None,
            "weightField": None,
            "orderBy": None,
            "isFormula": False,
            "isRowTotal": normalized_column_id.startswith("__RowTotal__"),
            "pivotFilters": {},
            "editable": True,
            "reason": None,
        }

        if isinstance(matched_column, dict):
            target_column = (
                matched_column.get("aggregationField")
                or matched_column.get("accessorKey")
                or matched_column.get("id")
            )
            spec.update(
                {
                    "targetColumn": (
                        str(target_column) if target_column else normalized_column_id
                    ),
                    "aggregationField": matched_column.get("aggregationField"),
                    "aggregationFn": self._normalize_aggregation_name(
                        matched_column.get("aggregationFn")
                    ),
                    "windowFn": matched_column.get("windowFn"),
                    "weightField": matched_column.get("weightField"),
                    "orderBy": matched_column.get("orderBy"),
                    "isFormula": bool(matched_column.get("isFormula")),
                }
            )

        if matched_prefix and column_dimensions:
            prefix_parts = matched_prefix.split("|")
            if len(column_dimensions) == 1:
                spec["pivotFilters"] = {column_dimensions[0]: matched_prefix}
            elif len(prefix_parts) == len(column_dimensions):
                spec["pivotFilters"] = {
                    column_dimensions[index]: prefix_parts[index]
                    for index in range(len(column_dimensions))
                }
            else:
                spec["editable"] = False
                spec["reason"] = (
                    "Skipped aggregate edit because the pivot column path could not be resolved."
                )

        aggregation_fn = spec["aggregationFn"]
        if spec["isRowTotal"]:
            spec["editable"] = False
            spec["reason"] = (
                "Row total cells are derived from multiple pivot buckets and are not directly editable."
            )
        elif spec["isFormula"]:
            spec["editable"] = False
            spec["reason"] = "Formula cells are derived and are not directly editable."
        elif spec["windowFn"]:
            spec["editable"] = False
            spec["reason"] = (
                "Window-function cells are derived and are not directly editable."
            )
        elif aggregation_fn in {"count", "count_distinct", "distinct_count"}:
            spec["editable"] = False
            spec["reason"] = (
                "Count-based aggregates cannot be edited because they do not map to one source value."
            )

        return spec




















    async def _drill_routed(
        self,
        spec: Any,
        extra_filters: Any,
        drill_payload: Any,
        *,
        limit: int,
        offset: int,
        sort_col: Optional[str],
        sort_dir: str,
        text_filter: str,
    ) -> Dict[str, Any]:
        """Drill-through records from the core (an unsupported request raises ``CoreUnsupported``)."""
        from .transport.core_bridge import core_drill_through, run_core_read

        return await run_core_read(self.controller, spec.table, lambda resolve: core_drill_through(
            resolve, spec, extra_filters, drill_payload, limit=limit, offset=offset,
            sort_col=sort_col, sort_dir=sort_dir, text_filter=text_filter,
        ))

    async def handle_drill_through(
        self, request: TanStackRequest, drill_payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle a drill through request.
        """
        # 1. Convert request to spec to get base filters
        spec = self.convert_tanstack_request_to_pivot_spec(request)

        # 2. The payload's own filters (the node path is read by the core from rowPath/rowFields)
        payload_filters_raw = dict(drill_payload.get("filters", {}) or {})
        row_path = drill_payload.get("row_path") or drill_payload.get("rowPath") or ""
        row_fields = drill_payload.get("row_fields") or drill_payload.get("pathFields") or []
        if not isinstance(row_fields, list):
            row_fields = []

        # Convert frontend filter format to pivot engine format
        # This mirrors logic in convert_tanstack_request_to_pivot_spec
        def _convert_drill_filters(raw_filters: Dict[str, Any]) -> List[Dict[str, Any]]:
            converted: List[Dict[str, Any]] = []
            for field_name, filter_obj in (raw_filters or {}).items():
                if not isinstance(filter_obj, dict):
                    continue
                conditions = filter_obj["conditions"] if "conditions" in filter_obj else [filter_obj]
                for cond in conditions:
                    converted.append(
                        {
                            "field": field_name,
                            "op": self._map_tanstack_operator(cond.get("type", "eq")),
                            "value": cond.get("value"),
                        }
                    )
            return converted

        page = max(int(drill_payload.get("page", 0) or 0), 0)
        page_size_raw = drill_payload.get(
            "page_size", drill_payload.get("pageSize", 100)
        )
        page_size = min(max(int(page_size_raw or 100), 1), 500)
        sort_col = drill_payload.get("sort_col") or drill_payload.get("sortCol")
        sort_dir = str(
            drill_payload.get("sort_dir") or drill_payload.get("sortDir") or "asc"
        ).lower()
        text_filter = drill_payload.get("filter")
        if text_filter is None:
            text_filter = drill_payload.get("filterText", "")

        # 3. The core answers (dict with 'rows' and 'total_rows')
        result = await self._drill_routed(
            spec,
            _convert_drill_filters(payload_filters_raw),
            drill_payload,
            limit=page_size,
            offset=page * page_size,
            sort_col=sort_col,
            sort_dir="desc" if sort_dir == "desc" else "asc",
            text_filter=str(text_filter or ""),
        )
        return {
            "rows": result.get("rows", []),
            "total_rows": result.get("total_rows", 0),
            "page": page,
            "page_size": page_size,
            "sort_col": sort_col,
            "sort_dir": "desc" if sort_dir == "desc" else "asc",
            "filter": str(text_filter or ""),
            "row_path": row_path,
            "row_fields": row_fields,
        }

    @row_scoped
    async def handle_request(
        self,
        request: TanStackRequest,
        user: Optional[User] = None,
        *,
        row_scope: Any = None,
    ) -> TanStackResponse:
        """Handle a TanStack request directly"""
        request = self._request_with_row_scope(request, row_scope)
        if request.operation == TanStackOperation.GET_UNIQUE_VALUES:
            # Logic for unique values (used by Excel-like filter)
            column_id = (
                request.global_filter
            )  # Overload global_filter to pass the column
            pagination = (
                request.pagination if isinstance(request.pagination, dict) else {}
            )
            page_size = pagination.get("pageSize", pagination.get("page_size", 250))
            page_index = pagination.get("pageIndex", pagination.get("page_index", 0))
            offset = pagination.get("offset")
            try:
                page_size = max(1, min(int(page_size), 500))
            except (TypeError, ValueError):
                page_size = 250
            try:
                page_index = max(0, int(page_index))
            except (TypeError, ValueError):
                page_index = 0
            try:
                offset = (
                    max(0, int(offset))
                    if offset is not None
                    else page_index * page_size
                )
            except (TypeError, ValueError):
                offset = page_index * page_size
            search = str(pagination.get("search") or "").strip()
            unique_values, total_values = await self.get_unique_values(
                request.table,
                column_id,
                request.filters,
                request.custom_dimensions,
                measure_axis=request.measure_axis,
                request_columns=request.columns,
                search=search,
                limit=page_size,
                offset=offset,
                user=user,
                row_scope=row_scope,
            )
            return TanStackResponse(
                data=[{"value": v} for v in unique_values],
                columns=[],
                pagination={
                    "totalRows": total_values,
                    "pageSize": page_size,
                    "pageIndex": page_index,
                    "offset": offset,
                    "hasMore": offset + len(unique_values) < total_values,
                    "search": search,
                },
            )

        # Convert request to pivot spec
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)

        # Apply RLS if user is provided
        if user or row_scope is not None:
            pivot_spec = apply_rls_to_spec(
                pivot_spec, user, row_scope=row_scope
            )

        # Execute pivot operation asynchronously
        pivot_result = await self.controller.run_pivot_async(
            pivot_spec, return_format="dict"
        )

        # Convert result to TanStack format
        tanstack_result = self.convert_pivot_result_to_tanstack_format(
            pivot_result, request
        )

        return tanstack_result

    @row_scoped
    async def handle_hierarchical_request(
        self,
        request: TanStackRequest,
        expanded_paths: Union[List[List[str]], bool],
        user_preferences: Optional[Dict[str, Any]] = None,
        *,
        user: Optional[User] = None,
        row_scope: Any = None,
    ) -> TanStackResponse:
        """Handle hierarchical TanStack request with expansion state"""
        request = self._request_with_row_scope(request, row_scope)
        self._log_request("handle_hierarchical_request", request)
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)
        if user or row_scope is not None:
            pivot_spec = apply_rls_to_spec(
                pivot_spec, user, row_scope=row_scope
            )

        # Handle "Expand All" case
        target_paths = expanded_paths
        if expanded_paths is True:
            # Signal to load all levels/nodes
            target_paths = [["__ALL__"]]


        # The whole visible tree (no window), from the core.
        hierarchy_view = await self._hierarchy_view_routed(
            pivot_spec, target_paths, 0, None,
            include_grand_total=False,
        )
        tanstack_result = self.convert_pivot_result_to_tanstack_format(
            hierarchy_view.get("rows", []), request
        )
        if hierarchy_view.get("total_rows") is not None:
            tanstack_result.total_rows = hierarchy_view["total_rows"]
        tanstack_result.data = finalize_hierarchy_rows(tanstack_result.data)
        self._log_response("handle_hierarchical_request", tanstack_result.data)
        return tanstack_result


    @staticmethod
    def _annotate_flat_group_rows(
        rows: List[Dict[str, Any]], grouping: List[str]
    ) -> None:
        if not grouping:
            return
        max_depth = max(len(grouping) - 1, 0)
        for row in rows or []:
            if not isinstance(row, dict) or _is_grand_total_row(row):
                continue
            path_parts: List[str] = []
            for field in grouping:
                value = row.get(field)
                if _is_missing_value(value):
                    break
                path_parts.append(str(value))
            depth = min(max(len(path_parts) - 1, 0), max_depth)
            row["depth"] = depth
            row["_depth"] = depth
            row["_id"] = (
                row.get(grouping[depth], "") if grouping else row.get("_id", "")
            )
            row["_path"] = (
                "|||".join(path_parts) if path_parts else str(row.get("_id", ""))
            )
            row["_pathFields"] = list(grouping[: len(path_parts)])
            row["_has_children"] = False
            row["_is_expanded"] = False

    # _apply_expansion_state removed as it is now handled by the controller/tree manager logic

    @row_scoped
    async def _core_flat_view(self, pivot_spec: Any, start_row: int, end_row: int,
                              include_grand_total: bool, *,
                              column_members: Optional[Tuple[Tuple[Any, ...], ...]] = None,
                              ) -> Optional[Dict[str, Any]]:
        """The flat tabular view from the core."""
        from .transport.core_bridge import core_hierarchy_view, run_core_read

        return await run_core_read(self.controller, pivot_spec.table, lambda resolve: core_hierarchy_view(
            resolve, pivot_spec, [["__ALL__"]], start_row, end_row,
            include_grand_total_row=include_grand_total, flat=True, column_members=column_members))

    async def hierarchy_view(
        self,
        pivot_spec: Any,
        expanded_paths: Any,
        start_row: int = 0,
        end_row: Optional[int] = None,
        *,
        include_grand_total: bool = False,
    ) -> Dict[str, Any]:
        """The ``run_hierarchy_view`` contract for a scoped spec, from the core."""
        target_paths = [["__ALL__"]] if expanded_paths is True else (expanded_paths or [])
        return await self._hierarchy_view_routed(
            pivot_spec, target_paths, start_row, end_row, include_grand_total=include_grand_total)

    async def _hierarchy_view_routed(
        self,
        pivot_spec: Any,
        target_paths: Any,
        start_row: int,
        end_row: Optional[int],
        *,
        include_grand_total: bool,
        tabular_subtotals: bool = False,
        column_members: Optional[Tuple[Tuple[Any, ...], ...]] = None,
    ) -> Dict[str, Any]:
        """The hierarchy view from the core (an unsupported request raises ``CoreUnsupported``)."""
        from .transport.core_bridge import core_hierarchy_view, run_core_read

        return await run_core_read(self.controller, pivot_spec.table, lambda resolve: core_hierarchy_view(
            resolve, pivot_spec, target_paths, start_row, end_row,
            include_grand_total_row=include_grand_total,
            tabular_subtotals=tabular_subtotals, column_members=column_members,
        ))

    async def handle_virtual_scroll_request(
        self,
        request: TanStackRequest,
        start_row: int,
        end_row: int,
        expanded_paths: Union[List[List[str]], bool] = None,
        user: Optional[User] = None,
        row_scope: Any = None,
        col_start: int = 0,
        col_end: Optional[int] = None,
        needs_col_schema: bool = False,
        include_grand_total: bool = False,
        requested_center_ids: Optional[List[str]] = None,
        _allow_prefetch: bool = True,
        profiling: bool = False,
    ) -> TanStackResponse:
        """Handle virtual scrolling request with start/end row indices"""
        request = self._request_with_row_scope(request, row_scope)
        request_started_at = time.perf_counter()
        cache_lookup_started_at = request_started_at
        response_cache_key = self._response_window_cache_key(
            request,
            start_row,
            end_row,
            expanded_paths,
            col_start,
            col_end,
            needs_col_schema,
            include_grand_total,
            requested_center_ids,
        )
        cached_response = self._get_cached_window_response(response_cache_key)
        if cached_response is None and not needs_col_schema:
            schema_key = self._response_window_cache_key(
                request, start_row, end_row, expanded_paths, col_start, col_end,
                True, include_grand_total, requested_center_ids,
            )
            cached_response = self._get_cached_window_response(schema_key)
            if cached_response is not None:
                cached_response.col_schema = None
        cache_lookup_finished_at = time.perf_counter()
        if cached_response is not None:
            if _allow_prefetch:
                total_center_cols = None
                if isinstance(cached_response.col_schema, dict):
                    total_center_cols = cached_response.col_schema.get(
                        "total_center_cols"
                    )
                elif request.grouping:
                    row_meta_keys = set(ROW_META_KEYS)
                    request_dimension_ids = {
                        col.get("id")
                        for col in (request.columns or [])
                        if isinstance(col, dict)
                        and not col.get("aggregationFn")
                        and not col.get("isFormula")
                        and col.get("id")
                    }
                    excluded_ids = (
                        set(row_meta_keys)
                        | set(request.grouping or [])
                        | request_dimension_ids
                    )
                    center_cache_key = (
                        self._cache_generation_value(),
                        request.table,
                        tuple(str(field) for field in (request.grouping or [])),
                        tuple(sorted(str(column_id) for column_id in excluded_ids)),
                        self._center_column_catalog_cache_key(request),
                    )
                    total_center_cols = (
                        len(self._center_col_ids_cache_get(center_cache_key) or [])
                        or None
                    )
                self._schedule_viewport_prefetch(
                    request,
                    start_row,
                    end_row,
                    expanded_paths,
                    col_start,
                    col_end,
                    include_grand_total,
                    cached_response.total_rows,
                    total_center_cols,
                )
            return (
                self._attach_virtual_scroll_profile(
                    cached_response,
                    started_at=request_started_at,
                    request=request,
                    start_row=start_row,
                    end_row=end_row,
                    col_start=col_start,
                    col_end=col_end,
                    needs_col_schema=needs_col_schema,
                    include_grand_total=include_grand_total,
                    response_cache_key=response_cache_key,
                    path="response_cache_hit",
                    stages={
                        "cacheLookupMs": self._profile_ms(
                            cache_lookup_started_at, cache_lookup_finished_at
                        ),
                    },
                )
                if profiling
                else cached_response
            )
        if not needs_col_schema:
            row_block_lookup_started_at = time.perf_counter()
            cached_block_response = self._assemble_cached_row_block_window(
                request,
                start_row,
                end_row,
                expanded_paths,
                col_start,
                col_end,
                include_grand_total,
                requested_center_ids,
            )
            row_block_lookup_finished_at = time.perf_counter()
            if cached_block_response is not None:
                completed_response = self._complete_virtual_scroll_response(
                    cached_block_response,
                    response_cache_key,
                    request,
                    start_row,
                    end_row,
                    expanded_paths,
                    col_start,
                    col_end,
                    include_grand_total,
                    _allow_prefetch,
                    requested_center_ids,
                )
                return (
                    self._attach_virtual_scroll_profile(
                        completed_response,
                        started_at=request_started_at,
                        request=request,
                        start_row=start_row,
                        end_row=end_row,
                        col_start=col_start,
                        col_end=col_end,
                        needs_col_schema=needs_col_schema,
                        include_grand_total=include_grand_total,
                        response_cache_key=response_cache_key,
                        path="row_block_cache_hit",
                        stages={
                            "cacheLookupMs": self._profile_ms(
                                cache_lookup_started_at, cache_lookup_finished_at
                            ),
                            "rowBlockLookupMs": self._profile_ms(
                                row_block_lookup_started_at,
                                row_block_lookup_finished_at,
                            ),
                        },
                    )
                    if profiling
                    else completed_response
                )

            missing_blocks_lookup_started_at = time.perf_counter()
            cached_blocks, missing_blocks = self._get_cached_row_block_entries(
                request,
                start_row,
                end_row,
                expanded_paths,
                col_start,
                col_end,
                requested_center_ids,
            )
            missing_blocks_lookup_finished_at = time.perf_counter()
            if cached_blocks and missing_blocks:
                missing_start_row = missing_blocks[0] * _ROW_BLOCK_SIZE
                missing_end_row = ((missing_blocks[-1] + 1) * _ROW_BLOCK_SIZE) - 1
                missing_blocks_fetch_started_at = time.perf_counter()
                await self.handle_virtual_scroll_request(
                    request,
                    missing_start_row,
                    missing_end_row,
                    expanded_paths=expanded_paths,
                    user=user,
                    col_start=col_start,
                    col_end=col_end,
                    needs_col_schema=False,
                    include_grand_total=include_grand_total,
                    requested_center_ids=requested_center_ids,
                    _allow_prefetch=False,
                    profiling=False,
                )
                missing_blocks_fetch_finished_at = time.perf_counter()
                row_block_reassemble_started_at = time.perf_counter()
                cached_block_response = self._assemble_cached_row_block_window(
                    request,
                    start_row,
                    end_row,
                    expanded_paths,
                    col_start,
                    col_end,
                    include_grand_total,
                    requested_center_ids,
                )
                row_block_reassemble_finished_at = time.perf_counter()
                if cached_block_response is not None:
                    completed_response = self._complete_virtual_scroll_response(
                        cached_block_response,
                        response_cache_key,
                        request,
                        start_row,
                        end_row,
                        expanded_paths,
                        col_start,
                        col_end,
                        include_grand_total,
                        _allow_prefetch,
                        requested_center_ids,
                    )
                    return (
                        self._attach_virtual_scroll_profile(
                            completed_response,
                            started_at=request_started_at,
                            request=request,
                            start_row=start_row,
                            end_row=end_row,
                            col_start=col_start,
                            col_end=col_end,
                            needs_col_schema=needs_col_schema,
                            include_grand_total=include_grand_total,
                            response_cache_key=response_cache_key,
                            path="row_block_partial_reuse",
                            stages={
                                "cacheLookupMs": self._profile_ms(
                                    cache_lookup_started_at, cache_lookup_finished_at
                                ),
                                "rowBlockLookupMs": self._profile_ms(
                                    missing_blocks_lookup_started_at,
                                    missing_blocks_lookup_finished_at,
                                ),
                                "missingBlocksFetchMs": self._profile_ms(
                                    missing_blocks_fetch_started_at,
                                    missing_blocks_fetch_finished_at,
                                ),
                                "rowBlockReassembleMs": self._profile_ms(
                                    row_block_reassemble_started_at,
                                    row_block_reassemble_finished_at,
                                ),
                            },
                            extra={
                                "rowBlockCache": {
                                    "cachedBlocks": len(cached_blocks),
                                    "missingBlocks": len(missing_blocks),
                                }
                            },
                        )
                        if profiling
                        else completed_response
                    )
        if self._has_formula_sort(request):
            formula_path_started_at = time.perf_counter()
            target_paths = expanded_paths or []
            requires_hierarchy_materialization = bool(request.grouping) and (
                expanded_paths is True
                or any(isinstance(path, list) and path for path in target_paths)
            )
            full_response = (
                await self.handle_hierarchical_request(
                    request,
                    expanded_paths or [],
                    user=user,
                    row_scope=row_scope,
                )
                if requires_hierarchy_materialization
                else await self.handle_request(
                    request, user=user, row_scope=row_scope
                )
            )
            full_response = await self.apply_full_export_layout(
                full_response, request, user=user, row_scope=row_scope,
                include_schema=True,
            )
            formula_layout_columns = (
                full_response.col_schema.get("columns", [])
                if isinstance(full_response.col_schema, dict) else []
            )
            formula_center_ids = [
                column["id"] for column in formula_layout_columns
                if isinstance(column, dict) and isinstance(column.get("id"), str)
            ] or None
            formula_pinned_ids = [
                column["id"] for column in formula_layout_columns
                if isinstance(column, dict)
                and column.get("layoutId") in request.pinned_column_ids
            ]
            full_rows = list(full_response.data or [])
            grand_total_row = None
            regular_rows = []
            for _fr in full_rows:
                if _is_grand_total_row(_fr):
                    if grand_total_row is None:
                        grand_total_row = _fr
                else:
                    regular_rows.append(_fr)

            safe_start = max(int(start_row or 0), 0)
            safe_end = max(
                int(end_row if end_row is not None else safe_start), safe_start
            )
            window_rows = regular_rows[safe_start : safe_end + 1]

            if (
                include_grand_total
                and request.totals
                and isinstance(grand_total_row, dict)
            ):
                window_rows = finalize_hierarchy_rows(
                    [*window_rows, grand_total_row], preserve_window_order=True
                )

            response = TanStackResponse(
                data=window_rows,
                columns=list(full_response.columns or []),
                pagination=full_response.pagination,
                total_rows=full_response.total_rows,
                grouping=full_response.grouping,
                version=full_response.version,
                col_schema=full_response.col_schema,
                color_scale_stats=full_response.color_scale_stats,
            )
            col_window_started_at = time.perf_counter()
            windowed_response = self._apply_col_windowing(
                response,
                request,
                col_start,
                col_end,
                needs_col_schema,
                requested_center_ids=requested_center_ids,
                discovered_center_ids=formula_center_ids,
                pinned_center_ids=formula_pinned_ids,
                layout_revision=full_response.layout_revision,
            )
            col_window_finished_at = time.perf_counter()
            completed_response = self._complete_virtual_scroll_response(
                windowed_response,
                response_cache_key,
                request,
                start_row,
                end_row,
                expanded_paths,
                col_start,
                col_end,
                include_grand_total,
                _allow_prefetch,
                requested_center_ids,
            )
            return (
                self._attach_virtual_scroll_profile(
                    completed_response,
                    started_at=request_started_at,
                    request=request,
                    start_row=start_row,
                    end_row=end_row,
                    col_start=col_start,
                    col_end=col_end,
                    needs_col_schema=needs_col_schema,
                    include_grand_total=include_grand_total,
                    response_cache_key=response_cache_key,
                    path="formula_sort_window",
                    stages={
                        "cacheLookupMs": self._profile_ms(
                            cache_lookup_started_at, cache_lookup_finished_at
                        ),
                        "formulaPathMs": self._profile_ms(
                            formula_path_started_at, time.perf_counter()
                        ),
                        "colWindowingMs": self._profile_ms(
                            col_window_started_at, col_window_finished_at
                        ),
                    },
                    extra=(
                        full_response.profile
                        if isinstance(getattr(full_response, "profile", None), dict)
                        else None
                    ),
                )
                if profiling
                else completed_response
            )

        # Convert request to pivot spec
        pivot_spec_started_at = time.perf_counter()
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)
        pivot_spec_finished_at = time.perf_counter()

        # Apply RLS if user is provided
        if user:
            pivot_spec = apply_rls_to_spec(
                pivot_spec, user, row_scope=row_scope
            )

        # Handle "Expand All" case
        target_paths = expanded_paths or []
        if expanded_paths is True:
            target_paths = [["__ALL__"]]

        # Flat path: tabular layout with subtotals OFF.  Layout-mode is the
        # primary signal; include_subtotals is kept as a backward-compat fallback
        # for callers that predate the layout_mode field.
        _raw_request_layout_mode = getattr(request, "layout_mode", None)
        _request_layout_mode = str(_raw_request_layout_mode or "").strip().lower()
        if _raw_request_layout_mode and _request_layout_mode not in {
            "hierarchy",
            "outline",
            "tabular",
        }:
            _request_layout_mode = "hierarchy"
        _show_subtotal_footers = bool(getattr(request, "show_subtotal_footers", False))
        _use_flat_path = request.grouping and (
            (_request_layout_mode == "tabular" and not _show_subtotal_footers)
            or (not _request_layout_mode and request.include_subtotals is False)
        )
        _hier_show_subtotal_footers = (
            _show_subtotal_footers and _request_layout_mode != "tabular"
        )
        _hier_tabular_subtotals = (
            _show_subtotal_footers and _request_layout_mode == "tabular"
        )
        if _hier_tabular_subtotals:
            # A tabular subtotal view is a flat post-order projection of the full
            # hierarchy. Expansion/collapse is a presentation concern for the
            # hierarchy and outline layouts, so load every descendant here even
            # when the incoming expansion map is empty or contains overrides.
            target_paths = [["__ALL__"]]

        def _apply_discovered_layout(
            values: list[dict[str, Any]],
            spec_in: PivotSpec,
        ) -> Tuple[
            Optional[List[str]],
            Optional[dict[str, PivotColumnIdentity]],
            list[str],
            Optional[str],
            PivotSpec,
        ]:
            if not values:
                return None, None, [], None, spec_in
            resolved_layout = self._resolve_pivot_column_layout(
                values, spec_in, request, user,
            )
            center_ids = [column.cell_id for column in resolved_layout.columns]
            layout_map = {column.cell_id: column for column in resolved_layout.columns}
            layout_rev = resolved_layout.revision
            pinned_id_set = set(request.pinned_column_ids)
            pinned_cids = [
                column.cell_id for column in resolved_layout.columns
                if column.layout_id in pinned_id_set
            ]
            window_center_ids = self._resolve_center_window_ids(
                center_ids,
                request,
                col_start,
                col_end,
                needs_col_schema,
                requested_center_ids=requested_center_ids,
            )
            if window_center_ids is not None:
                window_center_ids = list(dict.fromkeys(
                    [*window_center_ids, *pinned_cids]
                ))
            spec_out = spec_in
            if spec_in.pivot_config is not None:
                mat_vals = self._materialized_pivot_values_for_window(
                    window_center_ids,
                    request,
                    center_ids,
                )
                spec_out = replace(
                    spec_in,
                    pivot_config=replace(
                        spec_in.pivot_config,
                        materialized_column_values=mat_vals,
                    ),
                )
            return center_ids, layout_map, pinned_cids, layout_rev, spec_out

        discovered_center_ids: Optional[List[str]] = None
        discovered_layout: Optional[dict[str, PivotColumnIdentity]] = None
        pinned_center_ids: list[str] = []
        resolved_layout_revision: Optional[str] = None
        catalog_members: Optional[Tuple[Tuple[Any, ...], ...]] = None
        hierarchy_view: Optional[Dict[str, Any]] = None
        column_catalog_started_at = None
        column_catalog_finished_at = None
        hierarchy_view_started_at = None
        hierarchy_view_finished_at = None

        if (
            pivot_spec.columns
            and pivot_spec.pivot_config
            and pivot_spec.pivot_config.enabled
        ):
            column_catalog_started_at = time.perf_counter()
            local_cat_key = self._pivot_column_catalog_cache_key(pivot_spec)
            cached_catalog = self._locked_cache_lookup(
                self._pivot_column_catalog_cache, local_cat_key
            )
            _can_fuse_core = (
                cached_catalog is None
                and not _use_flat_path
                and type(self)._discover_pivot_columns is TanStackPivotAdapter._discover_pivot_columns
                and type(self)._hierarchy_view_routed is TanStackPivotAdapter._hierarchy_view_routed
                and "_discover_pivot_columns" not in self.__dict__
                and "_hierarchy_view_routed" not in self.__dict__
            )
            if _can_fuse_core:
                from .transport.core_bridge import (
                    core_column_catalog_members,
                    core_hierarchy_view,
                    run_core_read,
                )

                def _fused_discover_and_view(resolve: Any) -> Tuple[
                    list[dict[str, Any]],
                    Tuple[Tuple[Any, ...], ...],
                    Optional[List[str]],
                    Optional[dict[str, PivotColumnIdentity]],
                    list[str],
                    Optional[str],
                    Dict[str, Any],
                    float,
                ]:
                    d_vals, d_members = core_column_catalog_members(resolve, pivot_spec)
                    self._locked_cache_store(
                        self._pivot_column_catalog_cache,
                        local_cat_key,
                        {"values": list(d_vals), "members": tuple(d_members)},
                        max_size=_PIVOT_CATALOG_CACHE_SIZE,
                        ttl_seconds=_LOCAL_CACHE_TTL_SECONDS,
                    )
                    cat_done = time.perf_counter()
                    c_ids, l_map, p_cids, l_rev, eff_spec = _apply_discovered_layout(
                        d_vals, pivot_spec
                    )
                    h_view = core_hierarchy_view(
                        resolve,
                        eff_spec,
                        target_paths,
                        start_row,
                        end_row,
                        include_grand_total_row=include_grand_total,
                        tabular_subtotals=_hier_tabular_subtotals,
                        column_members=tuple(d_members),
                    )
                    return d_vals, tuple(d_members), c_ids, l_map, p_cids, l_rev, h_view, cat_done

                (
                    _d_vals,
                    catalog_members,
                    discovered_center_ids,
                    discovered_layout,
                    pinned_center_ids,
                    resolved_layout_revision,
                    hierarchy_view,
                    column_catalog_finished_at,
                ) = await run_core_read(self.controller, pivot_spec.table, _fused_discover_and_view)
                hierarchy_view_started_at = column_catalog_finished_at
                hierarchy_view_finished_at = time.perf_counter()
            else:
                if (
                    isinstance(cached_catalog, dict)
                    and isinstance(cached_catalog.get("values"), list)
                    and isinstance(cached_catalog.get("members"), tuple)
                ):
                    discovered_values = list(cached_catalog["values"])
                    catalog_members = cached_catalog["members"]
                else:
                    discovered_values, catalog_members = await self._discover_pivot_columns(pivot_spec)
                column_catalog_finished_at = time.perf_counter()
                (
                    discovered_center_ids,
                    discovered_layout,
                    pinned_center_ids,
                    resolved_layout_revision,
                    pivot_spec,
                ) = _apply_discovered_layout(discovered_values, pivot_spec)

        if hierarchy_view is None:
            # REQ-LAYOUT-002: the flat tabular layout is served by the core when it can
            # express the request; the hierarchy branch below builds the response from its view.
            _core_flat_view = await self._core_flat_view(
                pivot_spec, start_row, end_row, include_grand_total, column_members=catalog_members,
            ) if _use_flat_path else None
            hierarchy_view_started_at = time.perf_counter()
            hierarchy_view = _core_flat_view if _core_flat_view is not None else await self._hierarchy_view_routed(
                pivot_spec,
                target_paths,
                start_row,
                end_row,
                include_grand_total=include_grand_total,
                # Hierarchy/outline subtotal footers add no rows (REQ-LAYOUT-001): served as the plain tree.
                tabular_subtotals=_hier_tabular_subtotals,
                column_members=catalog_members,
            )
            hierarchy_view_finished_at = time.perf_counter()
        convert_started_at = time.perf_counter()
        _core_fast_meta = (
            hierarchy_view.get("_core_fast_meta")
            if isinstance(hierarchy_view, dict)
            else None
        )
        tanstack_result = self.convert_pivot_result_to_tanstack_format(
            hierarchy_view.get("rows", []),
            request,
            version=request.version,
            _core_normalized=bool(_core_fast_meta and _core_fast_meta.get("normalized")),
        )
        convert_finished_at = time.perf_counter()
        if hierarchy_view.get("total_rows") is not None:
            tanstack_result.total_rows = hierarchy_view["total_rows"]

        if include_grand_total and request.totals:
            grand_total_row = hierarchy_view.get("grand_total_row")
            if isinstance(grand_total_row, dict):
                _req_cols = request.columns or []
                _gt_needs_convert = not (_core_fast_meta and _core_fast_meta.get("gt_normalized")) or any(
                    isinstance(c, dict)
                    and (
                        c.get("isFormula")
                        or c.get("windowFn")
                        or c.get("window_fn")
                        or c.get("pivotWindowFn")
                        or FormulaEngineMixin._is_measure_formula_column(c)
                    )
                    for c in _req_cols
                )
                if _gt_needs_convert:
                    normalized_total = self.convert_pivot_result_to_tanstack_format(
                        [grand_total_row], request, version=request.version
                    ).data
                    if normalized_total:
                        grand_total_row = normalized_total[0]
            if isinstance(grand_total_row, dict):
                existing_rows = tanstack_result.data or []
                if not any(_is_grand_total_row(row) for row in existing_rows):
                    existing_rows.append(grand_total_row)
                    tanstack_result.data = existing_rows
                if not request.totals and tanstack_result.total_rows is not None:
                    tanstack_result.total_rows = max(
                        int(tanstack_result.total_rows) - 1, 0
                    )

        tanstack_result.data = finalize_hierarchy_rows(
            tanstack_result.data, preserve_window_order=True, copy_rows=False
        )
        if hierarchy_view.get("color_scale_stats"):
            tanstack_result.color_scale_stats = hierarchy_view["color_scale_stats"]
        col_window_started_at = time.perf_counter()
        windowed_response = self._apply_col_windowing(
            tanstack_result,
            request,
            col_start,
            col_end,
            needs_col_schema,
            requested_center_ids=requested_center_ids,
            discovered_center_ids=discovered_center_ids,
            discovered_layout=discovered_layout,
            pinned_center_ids=pinned_center_ids,
            layout_revision=resolved_layout_revision,
        )
        col_window_finished_at = time.perf_counter()
        completed_response = self._complete_virtual_scroll_response(
            windowed_response,
            response_cache_key,
            request,
            start_row,
            end_row,
            expanded_paths,
            col_start,
            col_end,
            include_grand_total,
            _allow_prefetch,
            requested_center_ids,
        )
        return (
            self._attach_virtual_scroll_profile(
                completed_response,
                started_at=request_started_at,
                request=request,
                start_row=start_row,
                end_row=end_row,
                col_start=col_start,
                col_end=col_end,
                needs_col_schema=needs_col_schema,
                include_grand_total=include_grand_total,
                response_cache_key=response_cache_key,
                path="hierarchy_view",
                stages={
                    "cacheLookupMs": self._profile_ms(
                        cache_lookup_started_at, cache_lookup_finished_at
                    ),
                    "pivotSpecMs": self._profile_ms(
                        pivot_spec_started_at, pivot_spec_finished_at
                    ),
                    "columnCatalogMs": self._profile_ms(
                        column_catalog_started_at, column_catalog_finished_at
                    ),
                    "hierarchyViewMs": self._profile_ms(
                        hierarchy_view_started_at, hierarchy_view_finished_at
                    ),
                    "convertMs": self._profile_ms(
                        convert_started_at, convert_finished_at
                    ),
                    "colWindowingMs": self._profile_ms(
                        col_window_started_at, col_window_finished_at
                    ),
                },
                extra=(
                    hierarchy_view.get("profile")
                    if isinstance(hierarchy_view.get("profile"), dict)
                    else None
                ),
            )
            if profiling
            else completed_response
        )

    def get_schema_info(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for TanStack column configuration"""
        # Try to use backend's schema discovery first
        try:
            if hasattr(self.controller, "backend") and hasattr(
                self.controller.backend, "get_schema"
            ):
                schema = self.controller.backend.get_schema(table_name)
                if schema:
                    columns_info = []
                    for col_name, col_type in schema.items():
                        columns_info.append(
                            {
                                "id": col_name,
                                "header": col_name.replace("_", " ").title(),
                                "accessorKey": col_name,
                                "type": col_type,
                                "enableSorting": True,
                                "enableFiltering": True,
                            }
                        )

                    return {
                        "table": table_name,
                        "columns": columns_info,
                        "sample_data": [],  # Fetching sample data could be separate if needed
                    }
        except Exception as e:
            print(f"Metadata schema retrieval failed: {e}")

        # Fallback: return mock schema based on the controller's data via sample query
        try:
            # Run a simple query to get column info
            spec = PivotSpec(
                table=table_name, rows=[], measures=[], filters=[]  # No grouping
            )
            sample_result = self.controller.run_pivot(spec, return_format="dict")

            if sample_result and sample_result.get("columns"):
                columns_info = []
                for col_name in sample_result["columns"]:
                    # Determine column type (simplified)
                    col_type = "string"  # Default
                    # In a real implementation, this would analyze the data
                    columns_info.append(
                        {
                            "id": col_name,
                            "header": col_name.replace("_", " ").title(),
                            "accessorKey": col_name,
                            "type": col_type,
                            "enableSorting": True,
                            "enableFiltering": True,
                        }
                    )

                return {
                    "table": table_name,
                    "columns": columns_info,
                    "sample_data": sample_result.get("rows", [])[
                        :5
                    ],  # First 5 rows as sample
                }
        except:  # nosec B110
            pass

        # Return empty schema
        return {"table": table_name, "columns": [], "sample_data": []}

    async def get_grouped_data(self, request: TanStackRequest) -> TanStackResponse:
        """Handle grouped data request (for hierarchical tables)"""
        pivot_spec = self.convert_tanstack_request_to_pivot_spec(request)

        # Use the controller to get grouped data
        pivot_result = await self.controller.run_pivot_async(
            pivot_spec, return_format="dict"
        )

        # Format for TanStack grouping
        tanstack_result = self.convert_pivot_result_to_tanstack_format(
            pivot_result, request
        )

        # Add grouping information
        if request.grouping:
            tanstack_result.grouping = [
                {
                    "id": group_col,
                    "value": None,  # Will be populated with actual grouped data
                }
                for group_col in request.grouping
            ]

        return tanstack_result

    def get_invalidation_events(
        self, table_name: str, change_type: str
    ) -> List[Dict[str, Any]]:
        # ... existing ...
        pass

    @row_scoped
    async def get_unique_values(
        self,
        table_name: str,
        column_id: str,
        filters: Dict[str, Any] = None,
        custom_dimensions: Optional[List[Dict[str, Any]]] = None,
        measure_axis: Optional[Dict[str, Any]] = None,
        request_columns: Optional[List[Dict[str, Any]]] = None,
        search: str = "",
        limit: int = 250,
        offset: int = 0,
        user: Optional[User] = None,
        row_scope: Any = None,
    ) -> tuple[List[Any], int]:
        # Skip virtual columns that don't exist in the underlying table
        if not column_id or column_id in ("__row_number__", "hierarchy"):
            return [], 0
        """Get unique values for a column, potentially filtered"""

        # Convert the filters from the request format to the spec format
        pivot_filters = []
        if filters:
            for field_name, filter_obj in filters.items():
                if not isinstance(filter_obj, dict) or field_name == column_id:
                    continue

                if "conditions" in filter_obj and "operator" in filter_obj:
                    conditions = []
                    for cond in filter_obj["conditions"]:
                        conditions.append(
                            {
                                "field": field_name,
                                "op": self._map_tanstack_operator(
                                    cond.get("type", "eq")
                                ),
                                "value": cond.get("value"),
                            }
                        )
                    if conditions:
                        pivot_filters.append(
                            {"op": filter_obj["operator"], "conditions": conditions}
                        )
                else:
                    pivot_filters.append(
                        {
                            "field": field_name,
                            "op": self._map_tanstack_operator(
                                filter_obj.get("type", "eq")
                            ),
                            "value": filter_obj.get("value"),
                        }
                    )

        try:
            limit = max(1, min(int(limit), 500))
        except (TypeError, ValueError):
            limit = 250
        try:
            offset = max(0, int(offset))
        except (TypeError, ValueError):
            offset = 0

        axis_config = MeasureAxisConfig.from_dict(measure_axis)
        request_measures: List[Measure] = []
        for column in request_columns or []:
            if (
                not isinstance(column, dict)
                or not column.get("aggregationFn")
                or column.get("isFormula")
            ):
                continue
            request_measures.append(
                Measure(
                    field=column.get("aggregationField", column.get("id")),
                    agg=column.get("aggregationFn", "sum"),
                    alias=column.get("id"),
                )
            )
        if axis_config and axis_config.placement != "none":
            axis_config.members = self._derive_measure_axis_members(
                axis_config, request_measures, request_columns or []
            )

        spec = PivotSpec(
            table=table_name,
            rows=[],
            columns=[],
            measures=request_measures,
            filters=pivot_filters,
            custom_dimensions=custom_dimensions or [],
            measure_axis=axis_config,
            limit=limit,
            offset=offset,
        )

        # Filter-option queries are data reads too.  Apply the exact same
        # server-owned RLS transform used by normal TanStack requests *after*
        # client filters are normalized, so a client cannot replace a
        # protected predicate with its own tenant value.
        if user or row_scope is not None:
            spec = apply_rls_to_spec(spec, user, row_scope=row_scope)

        return await self._unique_values_routed(
            spec, column_id, search=str(search or ""), limit=spec.limit, offset=spec.offset
        )

    async def _unique_values_routed(
        self, spec: Any, column_id: str, *, search: str, limit: int, offset: int
    ) -> Optional[tuple]:
        """Filter options from the core (an unsupported request raises ``CoreUnsupported``)."""
        from .transport.core_bridge import core_unique_values, run_core_read

        return await run_core_read(self.controller, spec.table, lambda resolve: core_unique_values(
            resolve, spec, column_id, search=search, limit=limit, offset=offset))

    # ========== Live Streaming & Baseline Diff Facade ==========

    def get_streaming_stack(self) -> Dict[str, Any]:
        """Lazily initialize and return the 5-component streaming & diff stack."""
        stack = getattr(self, "_streaming_stack", None)
        if stack is not None:
            return stack
        from .diff.baseline_comparison_service import BaselineComparisonService
        from .diff.snapshot_provider import SnapshotProvider
        from .streaming.reactive_query_coordinator import ReactiveQueryCoordinator
        from .streaming.subscription_broker import SubscriptionBroker
        from .streaming.versioned_source_adapter import VersionedSourceAdapter

        source_adapter = VersionedSourceAdapter(self.controller)
        snapshot_provider = SnapshotProvider(self.controller, source_adapter)
        comparison_service = BaselineComparisonService(self.controller, snapshot_provider)
        coordinator = ReactiveQueryCoordinator(
            self.controller, source_adapter, snapshot_provider, comparison_service
        )
        broker = SubscriptionBroker(
            source_adapter=source_adapter,
            snapshot_provider=snapshot_provider,
            coordinator=coordinator,
        )
        stack = {
            "source_adapter": source_adapter,
            "snapshot_provider": snapshot_provider,
            "comparison_service": comparison_service,
            "coordinator": coordinator,
            "broker": broker,
        }
        self._streaming_stack = stack
        return stack

    def push_stream_changes(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        *,
        mode: str = "upsert",
        key_fields: Optional[List[str]] = None,
        source: str = "push",
    ):
        """Atomically mutate `table` and publish one committed revision to subscribers."""
        source_adapter = self.get_streaming_stack()["source_adapter"]
        return source_adapter.apply_mutation_batch(
            table, rows, mode=mode, key_fields=key_fields, source=source
        )

    @row_scoped
    def capture_baseline(
        self,
        table: str,
        *,
        label: Optional[str] = None,
        ttl_seconds: Optional[float] = None,
        principal: Any = None,
        row_scope: Any = None,
        external_table: Optional[str] = None,
        time_travel_token: Optional[str] = None,
    ):
        """Capture an immutable T0 baseline snapshot governed by ResourceGovernor."""
        snapshot_provider = self.get_streaming_stack()["snapshot_provider"]
        return snapshot_provider.capture_snapshot(
            table,
            principal=principal,
            row_scope=row_scope,
            label=label,
            kind="baseline",
            ttl_seconds=ttl_seconds,
            external_table=external_table,
            time_travel_token=time_travel_token,
        )

    @row_scoped
    def freeze_snapshot(
        self,
        table: str,
        *,
        label: Optional[str] = None,
        ttl_seconds: Optional[float] = None,
        principal: Any = None,
        row_scope: Any = None,
    ):
        """Capture a retained frozen snapshot for consistent historical scrolling/exports."""
        snapshot_provider = self.get_streaming_stack()["snapshot_provider"]
        return snapshot_provider.capture_snapshot(
            table,
            principal=principal,
            row_scope=row_scope,
            label=label,
            kind="freeze_snapshot",
            ttl_seconds=ttl_seconds,
        )


# Utility function for TanStack integration
def create_tanstack_adapter(
    backend_uri: str = ":memory:",
    debug: bool = False,
    *,
    distributed_runtime: Optional[Any] = None,
) -> TanStackPivotAdapter:
    """Create a TanStack adapter with a configured controller"""
    controller = ScalablePivotController(
        backend_uri=backend_uri,
        enable_streaming=True,
        enable_incremental_views=True,
        cache_ttl=300,
        distributed_runtime=distributed_runtime,
    )
    return TanStackPivotAdapter(controller, debug=debug)


# Example usage functions
async def example_usage():
    """Example of how to use the TanStack adapter"""
    adapter = create_tanstack_adapter()

    # Example TanStack request
    request = TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[
            {"id": "region", "header": "Region", "enableSorting": True},
            {"id": "product", "header": "Product", "enableSorting": True},
            {
                "id": "total_sales",
                "header": "Total Sales",
                "aggregationFn": "sum",
                "aggregationField": "sales",
            },
        ],
        filters=[],
        sorting=[{"id": "total_sales", "desc": True}],
        grouping=["region", "product"],
        aggregations=[],
        pagination={"pageIndex": 0, "pageSize": 100},
    )

    result = await adapter.handle_request(request)
    print(f"Received {len(result.data)} rows from TanStack adapter")

    return result


if __name__ == "__main__":
    asyncio.run(example_usage())
