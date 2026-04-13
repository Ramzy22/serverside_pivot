"""Transport-agnostic runtime service for pivot requests."""

from __future__ import annotations

import asyncio
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from ..tanstack_adapter import TanStackOperation, TanStackRequest, TanStackResponse

from .models import PivotRequestContext, PivotServiceResponse, PivotViewState, first_present, safe_int
from .session_gate import SessionRequestGate
from .detail_service import DetailRuntimeService
from .tree_service import TreeRuntimeService


class PivotRuntimeService:
    """Executes pivot operations using generic state/context contracts."""

    def __init__(
        self,
        adapter_getter: Callable[[], Any],
        session_gate: Optional[SessionRequestGate] = None,
        debug: bool = False,
    ):
        self._adapter_getter = adapter_getter
        self._session_gate = session_gate or SessionRequestGate()
        self._debug = debug
        self._tree_service = TreeRuntimeService(debug=debug)
        self._detail_service = DetailRuntimeService(self._tree_service, debug=debug)
        self._active_request_lock = threading.Lock()
        self._active_request_tasks: Dict[tuple[str, str, str], asyncio.Task] = {}
        self._superseded_tasks: set[asyncio.Task] = set()

    @staticmethod
    def _active_request_key(context: PivotRequestContext) -> Optional[tuple[str, str, str]]:
        if context.intent == "chart":
            lane = "chart"
        elif context.intent in {"viewport", "structural"}:
            lane = "data"
        else:
            return None
        return (
            str(context.session_id or "anonymous"),
            str(context.client_instance or "default"),
            lane,
        )

    def _replace_active_request_task(self, context: PivotRequestContext) -> bool:
        key = self._active_request_key(context)
        current_task = asyncio.current_task()
        if key is None or current_task is None:
            return False
        with self._active_request_lock:
            previous_task = self._active_request_tasks.get(key)
            if previous_task is not None and previous_task is not current_task and not previous_task.done():
                self._superseded_tasks.add(previous_task)
                previous_task.cancel()
            self._active_request_tasks[key] = current_task
        return True

    def _release_active_request_task(self, context: PivotRequestContext) -> None:
        key = self._active_request_key(context)
        current_task = asyncio.current_task()
        if key is None or current_task is None:
            return
        with self._active_request_lock:
            if self._active_request_tasks.get(key) is current_task:
                self._active_request_tasks.pop(key, None)
            self._superseded_tasks.discard(current_task)

    def _consume_superseded_cancel(self) -> bool:
        current_task = asyncio.current_task()
        if current_task is None:
            return False
        with self._active_request_lock:
            if current_task not in self._superseded_tasks:
                return False
            self._superseded_tasks.discard(current_task)
            return True

    async def process_async(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
    ) -> PivotServiceResponse:
        """Process one pivot request and return a transport-neutral response."""
        service_started_at = time.perf_counter()
        trigger_kind = "transaction" if state.transaction_request else (context.trigger_kind or "data")
        profiling_enabled = bool(context.profiling)
        adapter_lookup_started_at = service_started_at
        adapter = self._adapter_getter()
        adapter_lookup_finished_at = time.perf_counter()

        def build_profile(
            *,
            execution_started_at: Optional[float] = None,
            execution_finished_at: Optional[float] = None,
            postprocess_started_at: Optional[float] = None,
            response_rows: Optional[int] = None,
            response_columns: Optional[int] = None,
            extra: Optional[Dict[str, Any]] = None,
        ) -> Optional[Dict[str, Any]]:
            if not profiling_enabled:
                return None

            def ms(start: Optional[float], end: Optional[float]) -> Optional[float]:
                if start is None or end is None:
                    return None
                return round((end - start) * 1000, 3)

            profile = {
                "request": {
                    "requestId": context.request_id,
                    "kind": trigger_kind,
                    "viewMode": state.view_mode,
                    "intent": context.intent,
                    "table": context.table,
                    "rowFields": len(state.row_fields or []),
                    "colFields": len(state.col_fields or []),
                    "valueFields": len(state.val_configs or []),
                    "startRow": context.start_row,
                    "endRow": context.end_row,
                    "colStart": context.col_start,
                    "colEnd": context.col_end,
                    "needsColSchema": context.needs_col_schema,
                    "includeGrandTotal": context.include_grand_total,
                },
                "service": {
                    "adapterLookupMs": ms(adapter_lookup_started_at, adapter_lookup_finished_at),
                    "gateMs": ms(adapter_lookup_finished_at, gate_finished_at),
                    "requestBuildMs": ms(gate_finished_at, request_built_at),
                    "executeMs": ms(execution_started_at, execution_finished_at),
                    "postProcessMs": ms(postprocess_started_at, time.perf_counter()) if postprocess_started_at is not None else None,
                    "totalMs": ms(service_started_at, time.perf_counter()),
                    "responseRows": response_rows,
                    "responseColumns": response_columns,
                },
            }
            if isinstance(extra, dict) and extra:
                for key, value in extra.items():
                    profile[key] = value
            return profile

        if not self._session_gate.register_request(
            session_id=context.session_id,
            state_epoch=context.state_epoch,
            window_seq=context.window_seq,
            abort_generation=context.abort_generation,
            intent=context.intent,
            client_instance=context.client_instance,
        ):
            gate_finished_at = time.perf_counter()
            request_built_at = gate_finished_at
            return PivotServiceResponse(status="stale", profile=build_profile())

        gate_finished_at = time.perf_counter()

        tanstack_sorting = []
        sort_options = state.sort_options if isinstance(state.sort_options, dict) else {}
        column_sort_options = (
            sort_options.get("columnOptions")
            if isinstance(sort_options.get("columnOptions"), dict)
            else {}
        )
        # Auto-inject a default ascending sort for the first row field that has
        # a sortKeyField in columnOptions when the frontend sends no explicit sort.
        effective_sorting = list(state.sorting or [])
        if not effective_sorting and state.row_fields and column_sort_options:
            for rf in (state.row_fields or []):
                col_opts = column_sort_options.get(rf)
                if isinstance(col_opts, dict) and col_opts.get("sortKeyField"):
                    effective_sorting.append({"id": rf, "desc": False})
                    break

        for s in effective_sorting:
            if not isinstance(s, dict) or s.get("id") is None:
                continue

            sort_id = s.get("id")
            sort_item = {"id": sort_id, "desc": bool(s.get("desc", False))}

            # Static per-column sort metadata (from sortOptions) is merged first.
            # Dynamic sorting payload keys override these defaults.
            # The frontend sends id="hierarchy" for the row-group column;
            # resolve to the actual row field for sortOptions lookup by
            # checking all row fields for a matching sortOptions entry.
            lookup_id = sort_id
            if lookup_id == "hierarchy" and state.row_fields and column_sort_options:
                for rf in state.row_fields:
                    if rf in column_sort_options:
                        lookup_id = rf
                        break
            static_column_sort = column_sort_options.get(lookup_id)
            if isinstance(static_column_sort, dict):
                for key in ("semanticType", "sortSemantic", "nulls", "sortType", "sortKeyField", "absoluteSort"):
                    if key in static_column_sort and static_column_sort.get(key) is not None:
                        sort_item[key] = static_column_sort.get(key)

            # Preserve optional semantic hints for backend ordering (e.g. tenor sort)
            # and hidden-key directives for deterministic curve-pillar ordering.
            for key in ("semanticType", "sortSemantic", "nulls", "sortType", "sortKeyField", "absoluteSort"):
                if key in s:
                    sort_item[key] = s.get(key)
            tanstack_sorting.append(sort_item)

        request_columns = self._build_request_columns(state.row_fields, state.col_fields, state.val_configs)
        pagination_info = self._build_pagination(context)

        request = TanStackRequest(
            operation=TanStackOperation.GET_DATA,
            table=context.table,
            columns=request_columns,
            filters=state.filters or {},
            sorting=tanstack_sorting,
            grouping=state.row_fields or [],
            aggregations=[],
            pagination=pagination_info,
            global_filter=(state.filters or {}).get("global")
            if isinstance(state.filters, dict)
            else None,
            totals=state.show_col_totals,
            row_totals=state.show_row_totals,
            version=context.window_seq,
            column_sort_options=column_sort_options or None,
        )
        request_built_at = time.perf_counter()

        def build_edit_overlay(rows: Any) -> Optional[Dict[str, Any]]:
            if not isinstance(rows, list) or not rows:
                return None
            edit_domain = getattr(adapter, "edit_domain", None)
            if edit_domain is None or not hasattr(edit_domain, "build_visible_edit_overlay"):
                return None
            if (
                hasattr(edit_domain, "has_visible_edit_overlay")
                and not edit_domain.has_visible_edit_overlay(
                    request,
                    session_id=context.session_id,
                    client_instance=context.client_instance,
                )
            ):
                return None
            return edit_domain.build_visible_edit_overlay(
                request,
                session_id=context.session_id,
                client_instance=context.client_instance,
                rows=rows,
            )

        if trigger_kind == "detail" and state.detail_request:
            execution_started_at = time.perf_counter()
            try:
                detail_result = await self._detail_service.handle_request(adapter, request, state, context)
            except Exception as exc:  # pragma: no cover - defensive
                execution_finished_at = time.perf_counter()
                if self._debug:
                    print(f"Detail request failed: {exc}")
                return PivotServiceResponse(
                    status="error",
                    message=str(exc),
                    data=[],
                    total_rows=0,
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            execution_finished_at = time.perf_counter()
            if not self._session_gate.response_is_current(
                session_id=context.session_id,
                state_epoch=context.state_epoch,
                window_seq=context.window_seq,
                abort_generation=context.abort_generation,
                intent=context.intent,
                client_instance=context.client_instance,
            ):
                return PivotServiceResponse(
                    status="stale",
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            detail_result.profile = build_profile(
                execution_started_at=execution_started_at,
                execution_finished_at=execution_finished_at,
                response_rows=len(detail_result.detail_payload.get("rows") or []) if isinstance(detail_result.detail_payload, dict) else None,
                response_columns=len(detail_result.detail_payload.get("columns") or []) if isinstance(detail_result.detail_payload, dict) else None,
            )
            return detail_result

        if trigger_kind == "drill" and state.drill_through:
            drill_payload = self._normalize_drill_request_payload(
                state.drill_through if isinstance(state.drill_through, dict) else {}
            )
            execution_started_at = time.perf_counter()
            try:
                drill_result = await adapter.handle_drill_through(request, state.drill_through)
            except Exception as exc:  # pragma: no cover - defensive
                execution_finished_at = time.perf_counter()
                if self._debug:
                    print(f"Drill through failed: {exc}")
                drill_result = {"rows": [], "total_rows": 0}
            else:
                execution_finished_at = time.perf_counter()

            if isinstance(drill_result, dict):
                records = list(drill_result.get("rows") or [])
                drill_response_payload = self._normalize_drill_response_payload(drill_result, drill_payload, records)
            else:
                records = list(drill_result or [])
                drill_response_payload = self._normalize_drill_response_payload({}, drill_payload, records)

            return PivotServiceResponse(
                status="drillthrough",
                drill_records=records,
                drill_payload=drill_response_payload,
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                    response_rows=len(records),
                    response_columns=len(records[0]) if records and isinstance(records[0], dict) else None,
                ),
            )

        transaction_result: Optional[Dict[str, Any]] = None
        transaction_refresh_mode = "viewport"
        transaction_requires_structural_refresh = False

        if state.transaction_request:
            transaction_request_payload = (
                {
                    **(state.transaction_request or {}),
                    "session_id": first_present(state.transaction_request, "session_id", "sessionId", default=context.session_id) or context.session_id,
                    "client_instance": first_present(state.transaction_request, "client_instance", "clientInstance", default=context.client_instance) or context.client_instance,
                }
                if isinstance(state.transaction_request, dict)
                else {}
            )
            execution_started_at = time.perf_counter()
            try:
                if hasattr(adapter, "handle_transaction"):
                    transaction_result = await adapter.handle_transaction(request, transaction_request_payload)
                else:
                    transaction_result = {
                        "status": "unsupported",
                        "message": "Adapter does not support row transactions.",
                    }
            except Exception as exc:  # pragma: no cover - defensive
                execution_finished_at = time.perf_counter()
                if self._debug:
                    print(f"Transaction request failed: {exc}")
                return PivotServiceResponse(
                    status="error",
                    message=str(exc),
                    data=[],
                    total_rows=0,
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            execution_finished_at = time.perf_counter()
            transaction_refresh_mode = str(
                (transaction_result or {}).get("refreshMode")
                or transaction_request_payload.get("refreshMode")
                or transaction_request_payload.get("refresh_mode")
                or "viewport"
            ).strip().lower()
            transaction_requires_structural_refresh = bool(
                (transaction_result or {}).get("requiresStructuralRefresh")
            )
            if transaction_refresh_mode == "none":
                return PivotServiceResponse(
                    status="transaction_applied",
                    transaction_result=transaction_result,
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                        extra={"transaction": transaction_result} if isinstance(transaction_result, dict) else None,
                    ),
                )
            if (
                transaction_refresh_mode == "patch"
                and not transaction_requires_structural_refresh
                and isinstance(transaction_result, dict)
                and isinstance(transaction_result.get("patchPayload"), dict)
            ):
                patch_payload = transaction_result.get("patchPayload") or {}
                return PivotServiceResponse(
                    status="patched",
                    data_version=context.window_seq,
                    data_offset=context.start_row,
                    transaction_result=transaction_result,
                    patch_payload=patch_payload,
                    edit_overlay=build_edit_overlay(list(patch_payload.get("rows") or [])),
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                        extra={"transaction": transaction_result},
                    ),
                )

        if trigger_kind == "update" and (state.cell_update or state.cell_updates):
            update_payloads = []
            if isinstance(state.cell_update, dict):
                update_payloads.append(state.cell_update)
            update_payloads.extend(
                update_payload
                for update_payload in (state.cell_updates or [])
                if isinstance(update_payload, dict)
            )
            try:
                if hasattr(adapter, "handle_updates"):
                    await adapter.handle_updates(request, update_payloads)
                else:
                    for update_payload in update_payloads:
                        await adapter.handle_update(request, update_payload)
            except Exception as exc:  # pragma: no cover - defensive
                if self._debug:
                    print(f"Cell update failed: {exc}")

        expanded_paths = self._parse_expanded_paths(state.expanded)
        effective_needs_col_schema = bool(
            context.needs_col_schema
            or transaction_requires_structural_refresh
            or transaction_refresh_mode in {"structural", "full", "smart_structural"}
        )

        execution_started_at = time.perf_counter()
        active_request_registered = self._replace_active_request_task(context)
        try:
            if state.view_mode == "tree" and trigger_kind != "chart":
                response_state = await self._tree_service.handle_data_request(
                    adapter,
                    request,
                    state,
                    context,
                    expanded_paths,
                )
                response = TanStackResponse(
                    data=list(response_state.data or []),
                    columns=list(response_state.columns or []),
                    total_rows=response_state.total_rows,
                    version=context.window_seq,
                )
            elif (
                trigger_kind != "chart"
                and state.view_mode == "report"
                and self._has_branching_report_root(state.report_def)
            ):
                response = await self._handle_branching_report_request(
                    adapter,
                    request,
                    state,
                    context,
                    expanded_paths,
                )
            elif trigger_kind == "chart":
                requested_series_ids = (
                    [
                        value for value in (state.chart_request or {}).get("series_column_ids", [])
                        if isinstance(value, str) and value
                    ]
                    if isinstance((state.chart_request or {}).get("series_column_ids"), list)
                    else None
                )
                response = await adapter.handle_virtual_scroll_request(
                    request,
                    context.start_row,
                    context.end_row if context.end_row is not None else context.start_row,
                    expanded_paths,
                    col_start=context.col_start,
                    col_end=context.col_end,
                    needs_col_schema=bool((state.chart_request or {}).get("needs_col_schema", False)),
                    include_grand_total=context.include_grand_total,
                    requested_center_ids=requested_series_ids,
                    profiling=profiling_enabled,
                )
            elif context.viewport_active and context.end_row is not None:
                response = await adapter.handle_virtual_scroll_request(
                    request,
                    context.start_row,
                    context.end_row,
                    expanded_paths,
                    col_start=context.col_start,
                    col_end=context.col_end,
                    needs_col_schema=effective_needs_col_schema,
                    include_grand_total=context.include_grand_total,
                    profiling=profiling_enabled,
                )
            else:
                if state.row_fields:
                    initial_end_row = min(request.pagination.get("pageSize", 1000), 100) - 1
                    response = await adapter.handle_virtual_scroll_request(
                        request,
                        0,
                        initial_end_row,
                        expanded_paths,
                        needs_col_schema=True,
                        profiling=profiling_enabled,
                    )
                else:
                    response = await adapter.handle_request(request)
        except asyncio.CancelledError:
            execution_finished_at = time.perf_counter()
            if self._consume_superseded_cancel():
                return PivotServiceResponse(
                    status="stale",
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            raise
        except Exception as exc:
            execution_finished_at = time.perf_counter()
            if self._debug:
                print(f"Pivot execution failed: {exc}")
            return PivotServiceResponse(
                status="error",
                message=str(exc),
                data=[],
                total_rows=0,
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                ),
            )
        finally:
            if active_request_registered:
                self._release_active_request_task(context)
        execution_finished_at = time.perf_counter()

        response_version = context.window_seq if context.window_seq is not None else response.version
        if not self._session_gate.response_is_current(
            session_id=context.session_id,
            state_epoch=context.state_epoch,
            window_seq=context.window_seq,
            abort_generation=context.abort_generation,
            intent=context.intent,
            client_instance=context.client_instance,
        ):
            return PivotServiceResponse(
                status="stale",
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                    response_rows=len(response.data or []),
                    response_columns=len(response.columns or []),
                    extra=(response.profile if isinstance(getattr(response, "profile", None), dict) else None),
                ),
            )

        if trigger_kind == "chart":
            postprocess_started_at = time.perf_counter()
            return PivotServiceResponse(
                status="chart_data",
                chart_data={
                    "rows": list(response.data or []),
                    "columns": list(response.columns or []),
                    "colSchema": response.col_schema,
                    "rowStart": context.start_row,
                    "rowEnd": context.end_row,
                    "colStart": context.col_start,
                    "colEnd": context.col_end,
                    "totalRows": response.total_rows,
                    "dataVersion": response_version,
                    "stateEpoch": context.state_epoch,
                    "abortGeneration": context.abort_generation,
                    "windowSeq": context.window_seq,
                    "paneId": (state.chart_request or {}).get("pane_id"),
                    "requestSignature": (state.chart_request or {}).get("request_signature"),
                },
                data_version=response_version,
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                    postprocess_started_at=postprocess_started_at,
                    response_rows=len(response.data or []),
                    response_columns=len(response.columns or []),
                    extra=(response.profile if isinstance(getattr(response, "profile", None), dict) else None),
                ),
            )

        # --- Report Mode: annotate rows with level metadata ---
        response_data = response.data
        response_total_rows = response.total_rows
        if (
            state.view_mode == "report"
            and state.report_def
            and isinstance(state.report_def, dict)
            and not self._has_branching_report_root(state.report_def)
        ):
            response_data, response_total_rows = self._apply_report_annotations(
                response_data, response_total_rows, state.report_def, state.row_fields or []
            )

        cols_payload: List[Dict[str, Any]] = list(response.columns or [])
        should_emit_columns = (
            effective_needs_col_schema
            or not context.viewport_active
            or (context.intent == "structural" and context.original_intent != "expansion")
        )
        schema_payload = response.col_schema if bool(response.col_schema) and (
            effective_needs_col_schema or should_emit_columns
        ) else None

        postprocess_started_at = time.perf_counter()
        return PivotServiceResponse(
            status="data",
            data=response_data,
            total_rows=response_total_rows,
            columns=cols_payload if should_emit_columns else None,
            col_schema=schema_payload,
            data_offset=context.start_row,
            data_version=response_version,
            color_scale_stats=response.color_scale_stats,
            transaction_result=transaction_result,
            edit_overlay=build_edit_overlay(list(response_data or [])),
            profile=build_profile(
                execution_started_at=execution_started_at,
                execution_finished_at=execution_finished_at,
                postprocess_started_at=postprocess_started_at,
                response_rows=len(response_data or []),
                response_columns=len(cols_payload or []),
                extra={
                    **(response.profile if isinstance(getattr(response, "profile", None), dict) else {}),
                    **({"transaction": transaction_result} if isinstance(transaction_result, dict) else {}),
                } if (
                    isinstance(getattr(response, "profile", None), dict) or isinstance(transaction_result, dict)
                ) else None,
            ),
        )

    def process(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
    ) -> PivotServiceResponse:
        """Sync wrapper for sync transports."""
        return asyncio.run(self.process_async(state, context))

    @staticmethod
    def _normalize_drill_request_payload(drill_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize drill request metadata to one canonical camelCase shape."""
        return {
            "rowPath": first_present(drill_payload, "rowPath", "row_path", default="") or "",
            "rowFields": list(first_present(drill_payload, "rowFields", "row_fields", "pathFields", default=[]) or []),
            "page": safe_int(first_present(drill_payload, "page"), 0),
            "pageSize": safe_int(first_present(drill_payload, "pageSize", "page_size"), 100),
            "sortCol": first_present(drill_payload, "sortCol", "sort_col"),
            "sortDir": first_present(drill_payload, "sortDir", "sort_dir", default="asc") or "asc",
            "filterText": first_present(drill_payload, "filterText", "filter", default="") or "",
        }

    @staticmethod
    def _normalize_drill_response_payload(
        drill_result: Dict[str, Any],
        drill_payload: Dict[str, Any],
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build one canonical drill response payload shape for runtimeResponse."""
        return {
            "rows": records,
            "page": safe_int(first_present(drill_result, "page", default=drill_payload.get("page")), 0),
            "pageSize": safe_int(
                first_present(drill_result, "pageSize", "page_size", default=drill_payload.get("pageSize")),
                safe_int(drill_payload.get("pageSize"), 100),
            ),
            "totalRows": safe_int(first_present(drill_result, "totalRows", "total_rows"), len(records)),
            "sortCol": first_present(drill_result, "sortCol", "sort_col", default=drill_payload.get("sortCol")),
            "sortDir": first_present(drill_result, "sortDir", "sort_dir", default=drill_payload.get("sortDir", "asc")) or "asc",
            "filterText": first_present(
                drill_result,
                "filterText",
                "filter",
                default=drill_payload.get("filterText", ""),
            ) or "",
            "rowPath": first_present(drill_result, "rowPath", "row_path", default=drill_payload.get("rowPath")) or "",
            "rowFields": list(first_present(drill_result, "rowFields", "row_fields", default=drill_payload.get("rowFields", [])) or []),
        }

    @staticmethod
    def _normalize_formula_reference_key(value: Any, fallback: Any = "formula") -> str:
        base = str(value or "").strip().lower()
        base = re.sub(r"\s+", "", base)
        base = re.sub(r"[^a-z0-9_]", "", base)
        fallback_base = re.sub(r"[^a-z0-9_]", "", str(fallback or "formula").strip().lower()) or "formula"
        normalized = base or fallback_base
        return normalized if re.match(r"^[a-z_]", normalized) else f"f_{normalized}"

    @staticmethod
    def _build_request_columns(
        row_fields: Optional[List[str]],
        col_fields: Optional[List[str]],
        val_configs: Optional[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        columns: List[Dict[str, Any]] = []
        for field in (row_fields or []):
            columns.append({"id": field})
        for field in (col_fields or []):
            columns.append({"id": field})
        for measure in (val_configs or []):
            if not isinstance(measure, dict):
                continue
            field = measure.get("field")
            agg = measure.get("agg")
            if not field or not agg:
                continue
            if agg == "formula":
                # Formula columns are computed post-aggregation; pass through as metadata only
                formula_label = measure.get("label") or field
                formula_ref = measure.get("formulaRef") or measure.get("referenceKey") or PivotRuntimeService._normalize_formula_reference_key(formula_label, field)
                columns.append(
                    {
                        "id": field,
                        "header": formula_label,
                        "accessorKey": field,
                        "formulaExpr": measure.get("formula", ""),
                        "formulaRef": formula_ref,
                        "formulaLabel": formula_label,
                        "isFormula": True,
                    }
                )
                continue
            columns.append(
                {
                    "id": f"{field}_{agg}",
                    "aggregationField": field,
                    "aggregationFn": agg,
                    "windowFn": measure.get("windowFn"),
                    "weightField": measure.get("weightField"),
                }
            )
        return columns

    @staticmethod
    def _build_pagination(context: PivotRequestContext) -> Dict[str, Any]:
        pagination = {"pageIndex": 0, "pageSize": 1000}
        if context.viewport_active and context.end_row is not None:
            count = max(context.end_row - context.start_row + 1, 1)
            pagination = {
                "pageIndex": context.start_row // 1000,
                "pageSize": min(count, 1000),
                "startRow": context.start_row,
                "endRow": context.end_row,
            }
        return pagination

    @staticmethod
    def _has_branching_report_root(report_def: Optional[Dict[str, Any]]) -> bool:
        return bool(
            isinstance(report_def, dict)
            and isinstance(report_def.get("root"), dict)
            and report_def.get("root", {}).get("field")
        )

    @staticmethod
    def _report_sort_value(value: Any) -> Any:
        if value is None:
            return (2, "", 0)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value != value:
                return (2, "", 0)
            return (0, "", float(value))
        return (1, str(value).lower(), 0)

    @staticmethod
    def _normalize_branching_report_node(node: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        field = node.get("field")
        if not isinstance(field, str) or not field:
            return None

        normalized = {
            "field": field,
            "label": node.get("label") if isinstance(node.get("label"), str) else "",
            "topN": int(node.get("topN")) if isinstance(node.get("topN"), (int, float)) and node.get("topN", 0) > 0 else None,
            "sortBy": node.get("sortBy") if isinstance(node.get("sortBy"), str) and node.get("sortBy") else None,
            "sortDir": "asc" if node.get("sortDir") == "asc" else "desc",
        }

        default_child = PivotRuntimeService._normalize_branching_report_node(node.get("defaultChild"))
        if default_child:
            normalized["defaultChild"] = default_child

        children_source = node.get("childrenByValue")
        if isinstance(children_source, dict):
            children_by_value = {}
            for key, child_node in children_source.items():
                normalized_child = PivotRuntimeService._normalize_branching_report_node(child_node)
                if normalized_child:
                    children_by_value[str(key)] = normalized_child
            if children_by_value:
                normalized["childrenByValue"] = children_by_value

        return normalized

    @staticmethod
    def _get_branching_report_child(node: Dict[str, Any], raw_value: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        children_by_value = node.get("childrenByValue")
        if isinstance(children_by_value, dict):
            exact_child = children_by_value.get(str(raw_value))
            if isinstance(exact_child, dict) and exact_child.get("field"):
                return exact_child
        default_child = node.get("defaultChild")
        if isinstance(default_child, dict) and default_child.get("field"):
            return default_child
        return None

    @staticmethod
    def _merge_exact_report_filter(filters: Dict[str, Any], field: str, value: Any) -> Dict[str, Any]:
        next_filters = dict(filters or {})
        next_filters[field] = {"type": "eq", "value": value}
        return next_filters

    async def _handle_branching_report_request(
        self,
        adapter: Any,
        base_request: TanStackRequest,
        state: PivotViewState,
        context: PivotRequestContext,
        expanded_paths: List[List[str]],
    ) -> TanStackResponse:
        report_root = self._normalize_branching_report_node((state.report_def or {}).get("root"))
        if not report_root:
            return TanStackResponse(data=[], columns=[], total_rows=0)

        expanded_all = expanded_paths == [["__ALL__"]] or state.expanded is True
        expanded_set = {
            "|||".join(path)
            for path in expanded_paths
            if isinstance(path, list) and path and path != ["__ALL__"]
        }

        emitted_columns: Optional[List[Dict[str, Any]]] = None
        emitted_schema: Optional[Dict[str, Any]] = None
        emitted_stats: Optional[Dict[str, Any]] = None
        base_filters = dict(base_request.filters or {})

        async def fetch_node_rows(
            node: Dict[str, Any],
            active_filters: Dict[str, Any],
            path_values: List[str],
            path_fields: List[str],
            depth: int,
        ) -> List[Dict[str, Any]]:
            nonlocal emitted_columns, emitted_schema, emitted_stats

            node_field = node.get("field")
            if not node_field:
                return []

            top_n = node.get("topN")
            sort_by = node.get("sortBy")
            sort_dir = node.get("sortDir") or "desc"
            branch_request = TanStackRequest(
                operation=TanStackOperation.GET_DATA,
                table=base_request.table,
                columns=self._build_request_columns([node_field], [], state.val_configs),
                filters=active_filters,
                sorting=[{"id": sort_by, "desc": sort_dir != "asc"}] if sort_by else [],
                grouping=[node_field],
                aggregations=[],
                pagination={
                    "pageIndex": 0,
                    "pageSize": 100000,
                },
                global_filter=base_request.global_filter,
                totals=False,
                row_totals=False,
                version=base_request.version,
                column_sort_options=base_request.column_sort_options,
            )
            branch_response = await adapter.handle_request(branch_request)
            if emitted_columns is None:
                emitted_columns = list(branch_response.columns or [])
                emitted_schema = branch_response.col_schema
                emitted_stats = branch_response.color_scale_stats

            visible_rows = [
                row for row in (branch_response.data or [])
                if isinstance(row, dict) and not self._is_report_summary_row(row)
            ]
            if sort_by:
                visible_rows = sorted(
                    visible_rows,
                    key=lambda row: self._report_sort_value(row.get(sort_by)),
                    reverse=(sort_dir != "asc"),
                )

            total_count = branch_response.total_rows if isinstance(branch_response.total_rows, int) else len(visible_rows)
            if top_n and len(visible_rows) > top_n:
                visible_rows = visible_rows[:top_n]

            output_rows: List[Dict[str, Any]] = []
            for row in visible_rows:
                raw_value = row.get(node_field, row.get("_id"))
                path_token = "" if raw_value is None else str(raw_value)
                row_path_values = path_values + [path_token]
                row_path_fields = path_fields + [node_field]
                row_path = "|||".join(row_path_values)
                child_node = self._get_branching_report_child(node, raw_value)
                has_children = bool(child_node and child_node.get("field"))
                is_expanded = expanded_all or row_path in expanded_set

                row_out = dict(row)
                row_out["_id"] = path_token or str(row.get("_id") or "")
                row_out["_path"] = row_path
                row_out["depth"] = depth
                row_out["_levelLabel"] = node.get("label") or node_field
                row_out["_levelField"] = node_field
                row_out["_pathFields"] = list(row_path_fields)
                row_out["_has_children"] = has_children
                row_out["_is_expanded"] = bool(is_expanded and has_children)
                if top_n:
                    row_out["_levelTopN"] = int(top_n)
                    if total_count > len(visible_rows):
                        row_out["_groupTotalCount"] = total_count
                output_rows.append(row_out)

                if has_children and is_expanded:
                    child_filters = self._merge_exact_report_filter(active_filters, node_field, raw_value)
                    child_rows = await fetch_node_rows(
                        child_node,
                        child_filters,
                        row_path_values,
                        row_path_fields,
                        depth + 1,
                    )
                    output_rows.extend(child_rows)

            return output_rows

        visible_report_rows = await fetch_node_rows(report_root, base_filters, [], [], 0)

        if context.include_grand_total and state.show_col_totals:
            grand_total_request = TanStackRequest(
                operation=TanStackOperation.GET_DATA,
                table=base_request.table,
                columns=self._build_request_columns([], [], state.val_configs),
                filters=base_filters,
                sorting=[],
                grouping=[],
                aggregations=[],
                pagination={"pageIndex": 0, "pageSize": 1},
                global_filter=base_request.global_filter,
                totals=False,
                row_totals=False,
                version=base_request.version,
                column_sort_options=base_request.column_sort_options,
            )
            grand_total_response = await adapter.handle_request(grand_total_request)
            if emitted_columns is None:
                emitted_columns = list(grand_total_response.columns or [])
                emitted_schema = grand_total_response.col_schema
                emitted_stats = grand_total_response.color_scale_stats
            grand_total_row = next(
                (row for row in (grand_total_response.data or []) if isinstance(row, dict)),
                None,
            )
            if grand_total_row:
                total_row = dict(grand_total_row)
                total_row["_id"] = "Grand Total"
                total_row["_path"] = "__grand_total__"
                total_row["_isTotal"] = True
                total_row["depth"] = 0
                total_row["_has_children"] = False
                total_row["_is_expanded"] = False
                visible_report_rows.append(total_row)

        total_rows = len(visible_report_rows)
        if context.viewport_active and context.end_row is not None:
            window_start = max(context.start_row, 0)
            window_end = max(context.end_row + 1, window_start)
            windowed_rows = visible_report_rows[window_start:window_end]
        else:
            windowed_rows = visible_report_rows

        return TanStackResponse(
            data=windowed_rows,
            columns=emitted_columns or [],
            total_rows=total_rows,
            version=base_request.version,
            col_schema=emitted_schema,
            color_scale_stats=emitted_stats,
        )

    @staticmethod
    def _resolve_report_level(
        report_def: Dict[str, Any],
        row_fields: List[str],
        depth: int,
        parent_values: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve the LevelRule for a given depth, with conditional children support.

        Walks the levels list from depth 0. At each depth, if the previous level
        has conditionalChildren and the parent value matches a key in that map,
        follow the override; otherwise follow the wildcard "*" or fall through
        to the flat levels list.
        """
        levels = report_def.get("levels") or []
        if not levels:
            return None

        current_rule = levels[0] if levels else None
        for d in range(1, depth + 1):
            if current_rule is None:
                break
            conditional = current_rule.get("conditionalChildren")
            parent_val = parent_values[d - 1] if parent_values and d - 1 < len(parent_values) else None

            if isinstance(conditional, dict) and conditional:
                # Try exact match first, then wildcard
                if parent_val is not None and str(parent_val) in conditional:
                    current_rule = conditional[str(parent_val)]
                elif "*" in conditional:
                    current_rule = conditional["*"]
                elif d < len(levels):
                    current_rule = levels[d]
                else:
                    current_rule = None
            elif d < len(levels):
                current_rule = levels[d]
            else:
                current_rule = None

        return current_rule

    @staticmethod
    def _report_row_depth(row: Dict[str, Any]) -> int:
        depth = row.get("depth", 0)
        if isinstance(depth, str):
            try:
                return int(depth)
            except (ValueError, TypeError):
                return 0
        return int(depth) if isinstance(depth, (int, float)) else 0

    @staticmethod
    def _report_row_path_parts(row: Dict[str, Any]) -> List[str]:
        path_str = row.get("_path", "")
        return path_str.split("|||") if isinstance(path_str, str) and path_str else []

    @staticmethod
    def _is_report_summary_row(row: Dict[str, Any]) -> bool:
        return bool(
            row.get("_isTotal")
            or row.get("_id") == "Grand Total"
            or row.get("_path") == "__grand_total__"
        )

    @staticmethod
    def _count_report_tree_rows(node: Dict[str, Any]) -> int:
        total = 1 + len(node.get("after_rows") or [])
        for child in node.get("children") or []:
            total += PivotRuntimeService._count_report_tree_rows(child)
        return total

    @staticmethod
    def _report_sort_key(value: Any) -> Any:
        if value is None:
            return (2, "", 0)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value != value:
                return (2, "", 0)
            return (0, "", float(value))
        return (1, str(value).lower(), 0)

    @staticmethod
    def _apply_report_annotations(
        data: Optional[List[Dict[str, Any]]],
        total_rows: Optional[int],
        report_def: Dict[str, Any],
        row_fields: List[str],
    ) -> tuple:
        """Annotate rows with report level metadata AND enforce topN.

        Two-pass approach:
        1. Annotate every row with _levelLabel, _levelField, _levelTopN
        2. Enforce topN by grouping children by their parent path and keeping
           only the first N at each depth where a topN rule applies.
        """
        if not data:
            return data, total_rows

        levels = report_def.get("levels") or []
        if not levels:
            return data, total_rows

        # --- Pass 1: annotate ---
        annotated = []
        for row in data:
            if not isinstance(row, dict):
                annotated.append(row)
                continue

            if PivotRuntimeService._is_report_summary_row(row):
                annotated.append(row)
                continue

            depth = PivotRuntimeService._report_row_depth(row)
            parent_values = PivotRuntimeService._report_row_path_parts(row)

            rule = PivotRuntimeService._resolve_report_level(
                report_def, row_fields, depth, parent_values
            )

            if rule and isinstance(rule, dict):
                row = {**row}
                row["_levelLabel"] = rule.get("label") or rule.get("field") or ""
                row["_levelField"] = rule.get("field") or ""
                top_n = rule.get("topN")
                if top_n and isinstance(top_n, (int, float)) and top_n > 0:
                    row["_levelTopN"] = int(top_n)
                sort_by = rule.get("sortBy")
                if isinstance(sort_by, str) and sort_by:
                    row["__reportSortBy"] = sort_by
                row["__reportSortDir"] = "asc" if rule.get("sortDir") == "asc" else "desc"

            annotated.append(row)

        root_prefix_rows: List[Any] = []
        root_suffix_rows: List[Any] = []
        roots: List[Dict[str, Any]] = []
        stack: List[Dict[str, Any]] = []
        saw_regular_row = False

        for row in annotated:
            if not isinstance(row, dict):
                target = root_suffix_rows if saw_regular_row else root_prefix_rows
                target.append(row)
                continue

            if PivotRuntimeService._is_report_summary_row(row):
                target_node = None
                if row.get("_path") != "__grand_total__":
                    row_depth = PivotRuntimeService._report_row_depth(row)
                    for candidate in reversed(stack):
                        if candidate["depth"] <= row_depth:
                            target_node = candidate
                            break
                    if target_node is None and stack:
                        target_node = stack[-1]
                if target_node is None:
                    target = root_suffix_rows if saw_regular_row else root_prefix_rows
                    target.append(row)
                else:
                    target_node["after_rows"].append(row)
                continue

            depth = PivotRuntimeService._report_row_depth(row)
            while stack and stack[-1]["depth"] >= depth:
                stack.pop()

            node = {
                "row": row,
                "depth": depth,
                "children": [],
                "after_rows": [],
            }
            if stack:
                stack[-1]["children"].append(node)
            else:
                roots.append(node)
            stack.append(node)
            saw_regular_row = True

        def _flatten_report_nodes(nodes: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
            if not nodes:
                return [], 0

            working_nodes = list(nodes)
            sort_by = working_nodes[0]["row"].get("__reportSortBy")
            sort_dir = working_nodes[0]["row"].get("__reportSortDir") or "desc"
            if isinstance(sort_by, str) and sort_by:
                sortable_nodes = []
                missing_nodes = []
                for node in working_nodes:
                    value = node["row"].get(sort_by)
                    if value is None or value != value:
                        missing_nodes.append(node)
                    else:
                        sortable_nodes.append(node)
                sortable_nodes = sorted(
                    sortable_nodes,
                    key=lambda node: PivotRuntimeService._report_sort_key(node["row"].get(sort_by)),
                    reverse=(sort_dir != "asc"),
                )
                working_nodes = sortable_nodes + missing_nodes

            top_n = working_nodes[0]["row"].get("_levelTopN")
            total_count = len(working_nodes)
            removed = 0
            if top_n and isinstance(top_n, int) and top_n > 0 and total_count > top_n:
                removed = sum(
                    PivotRuntimeService._count_report_tree_rows(node)
                    for node in working_nodes[top_n:]
                )
                working_nodes = working_nodes[:top_n]

            trimmed_count = len(working_nodes)
            flattened: List[Dict[str, Any]] = []
            for node in working_nodes:
                row = node["row"]
                row_out = {k: v for k, v in row.items() if k not in {"__reportSortBy", "__reportSortDir"}}
                if top_n and total_count > trimmed_count:
                    row_out["_groupTotalCount"] = total_count
                child_rows, child_removed = _flatten_report_nodes(node["children"])
                removed += child_removed
                flattened.append(row_out)
                flattened.extend(child_rows)
                flattened.extend(node["after_rows"])

            return flattened, removed

        filtered_rows, removed_count = _flatten_report_nodes(roots)
        filtered = root_prefix_rows + filtered_rows + root_suffix_rows

        adjusted_total = (total_rows - removed_count) if total_rows is not None and removed_count > 0 else total_rows
        return filtered, adjusted_total

    @staticmethod
    def _parse_expanded_paths(expanded: Any) -> List[List[str]]:
        if expanded is None or (isinstance(expanded, dict) and len(expanded) == 0):
            return []
        if isinstance(expanded, dict):
            return [k.split("|||") for k, v in expanded.items() if v is True]
        if expanded is True:
            return [["__ALL__"]]
        return []
