"""Transport-agnostic runtime service for pivot requests."""

from __future__ import annotations

import asyncio
from decimal import Decimal
import inspect
import os
import re
import threading
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from ..tanstack_adapter import TanStackOperation, TanStackRequest, TanStackResponse

from .models import PivotRequestContext, PivotServiceResponse, PivotViewState, first_present, safe_int
from .resilience import CircuitBreaker, CircuitBreakerOpen, PivotRequestTimeout
from .session_gate import SessionRequestGate
from .detail_service import DetailRuntimeService
from .tree_service import TreeRuntimeService


def _env_float(name: str, default: float) -> float:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


class PivotRuntimeService:
    """Executes pivot operations using generic state/context contracts."""

    def __init__(
        self,
        adapter_getter: Callable[[], Any],
        session_gate: Optional[SessionRequestGate] = None,
        debug: bool = False,
        request_timeout_seconds: Optional[float] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        circuit_breaker_failures: int = 5,
        circuit_breaker_cooldown_seconds: float = 30.0,
    ):
        self._adapter_getter = adapter_getter
        self._session_gate = session_gate or SessionRequestGate()
        self._debug = debug
        self._tree_service = TreeRuntimeService(debug=debug)
        self._detail_service = DetailRuntimeService(self._tree_service, debug=debug)
        try:
            explicit_timeout = float(request_timeout_seconds) if request_timeout_seconds is not None else None
        except (TypeError, ValueError):
            explicit_timeout = None
        self._request_timeout_seconds = explicit_timeout if explicit_timeout and explicit_timeout > 0 else _env_float("PIVOT_REQUEST_TIMEOUT_SECONDS", 300.0)
        self._circuit_breaker = circuit_breaker or CircuitBreaker(
            failure_threshold=circuit_breaker_failures,
            cooldown_seconds=circuit_breaker_cooldown_seconds,
        )
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

    @staticmethod
    def _backend_circuit_key(context: PivotRequestContext, trigger_kind: Optional[str]) -> Tuple[str, str]:
        intent = trigger_kind or context.intent or "structural"
        return (str(context.table or "default"), str(intent))

    async def _run_backend_operation(
        self,
        context: PivotRequestContext,
        trigger_kind: Optional[str],
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        circuit_key = self._backend_circuit_key(context, trigger_kind)
        self._circuit_breaker.before_request(circuit_key)
        try:
            awaitable = operation()
            if not inspect.isawaitable(awaitable):
                self._circuit_breaker.record_success(circuit_key)
                return awaitable
            if self._request_timeout_seconds:
                result = await asyncio.wait_for(awaitable, timeout=self._request_timeout_seconds)
            else:
                result = await awaitable
        except asyncio.TimeoutError as exc:
            self._circuit_breaker.record_failure(circuit_key)
            raise PivotRequestTimeout(
                f"Pivot backend request exceeded {self._request_timeout_seconds:g}s timeout."
            ) from exc
        except asyncio.CancelledError:
            raise
        except Exception:
            self._circuit_breaker.record_failure(circuit_key)
            raise
        else:
            self._circuit_breaker.record_success(circuit_key)
            return result

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
            custom_dimensions=state.custom_dimensions or [],
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
                detail_result = await self._run_backend_operation(
                    context,
                    trigger_kind,
                    lambda: self._detail_service.handle_request(adapter, request, state, context),
                )
            except CircuitBreakerOpen as exc:
                execution_finished_at = time.perf_counter()
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
            except PivotRequestTimeout as exc:
                execution_finished_at = time.perf_counter()
                return PivotServiceResponse(
                    status="timeout",
                    message=str(exc),
                    data=[],
                    total_rows=0,
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
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
                drill_result = await self._run_backend_operation(
                    context,
                    trigger_kind,
                    lambda: adapter.handle_drill_through(request, state.drill_through),
                )
            except CircuitBreakerOpen as exc:
                execution_finished_at = time.perf_counter()
                return PivotServiceResponse(
                    status="error",
                    message=str(exc),
                    drill_records=[],
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            except PivotRequestTimeout as exc:
                execution_finished_at = time.perf_counter()
                return PivotServiceResponse(
                    status="timeout",
                    message=str(exc),
                    drill_records=[],
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
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
                    transaction_result = await self._run_backend_operation(
                        context,
                        trigger_kind,
                        lambda: adapter.handle_transaction(request, transaction_request_payload),
                    )
                else:
                    transaction_result = {
                        "status": "unsupported",
                        "message": "Adapter does not support row transactions.",
                    }
            except CircuitBreakerOpen as exc:
                execution_finished_at = time.perf_counter()
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
            except PivotRequestTimeout as exc:
                execution_finished_at = time.perf_counter()
                return PivotServiceResponse(
                    status="timeout",
                    message=str(exc),
                    data=[],
                    total_rows=0,
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
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
                async def _apply_updates():
                    if hasattr(adapter, "handle_updates"):
                        await adapter.handle_updates(request, update_payloads)
                    else:
                        for update_payload in update_payloads:
                            await adapter.handle_update(request, update_payload)

                await self._run_backend_operation(context, trigger_kind, _apply_updates)
            except (CircuitBreakerOpen, PivotRequestTimeout) as exc:
                if self._debug:
                    print(f"Cell update skipped: {exc}")
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
            async def _execute_data_request():
                if state.view_mode == "tree" and trigger_kind != "chart":
                    response_state = await self._tree_service.handle_data_request(
                        adapter,
                        request,
                        state,
                        context,
                        expanded_paths,
                    )
                    return TanStackResponse(
                        data=list(response_state.data or []),
                        columns=list(response_state.columns or []),
                        total_rows=response_state.total_rows,
                        version=context.window_seq,
                    )
                if (
                    trigger_kind != "chart"
                    and state.view_mode == "report"
                    and self._has_branching_report_root(state.report_def)
                ):
                    return await self._handle_branching_report_request(
                        adapter,
                        request,
                        state,
                        context,
                        expanded_paths,
                    )
                if trigger_kind == "chart":
                    requested_series_ids = (
                        [
                            value for value in (state.chart_request or {}).get("series_column_ids", [])
                            if isinstance(value, str) and value
                        ]
                        if isinstance((state.chart_request or {}).get("series_column_ids"), list)
                        else None
                    )
                    return await adapter.handle_virtual_scroll_request(
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
                if context.viewport_active and context.end_row is not None:
                    return await adapter.handle_virtual_scroll_request(
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
                if state.row_fields:
                    initial_end_row = min(request.pagination.get("pageSize", 1000), 100) - 1
                    return await adapter.handle_virtual_scroll_request(
                        request,
                        0,
                        initial_end_row,
                        expanded_paths,
                        needs_col_schema=True,
                        profiling=profiling_enabled,
                    )
                return await adapter.handle_request(request)

            response = await self._run_backend_operation(context, trigger_kind, _execute_data_request)
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
        except CircuitBreakerOpen as exc:
            execution_finished_at = time.perf_counter()
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
        except PivotRequestTimeout as exc:
            execution_finished_at = time.perf_counter()
            return PivotServiceResponse(
                status="timeout",
                message=str(exc),
                data=[],
                total_rows=0,
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                ),
            )
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

        cols_payload: List[Dict[str, Any]] = [c for c in (response.columns or []) if not (isinstance(c, dict) and c.get("_isImplicitFormulaRef"))]
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
            formula_errors=getattr(response, "formula_errors", None) or None,
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
                formula_column = {
                    "id": field,
                    "header": formula_label,
                    "accessorKey": field,
                    "formulaExpr": measure.get("formula", ""),
                    "formulaRef": formula_ref,
                    "formulaLabel": formula_label,
                    "isFormula": True,
                }
                raw_formula_scope = (
                    measure.get("formulaScope")
                    or measure.get("formula_scope")
                    or measure.get("scope")
                )
                if raw_formula_scope is not None:
                    normalized_scope = str(raw_formula_scope).strip().lower()
                    formula_column["formulaScope"] = (
                        "columns"
                        if normalized_scope in {"columns", "display", "displayed", "displayed_columns", "rendered", "rendered_columns"}
                        else "measures"
                    )
                columns.append(formula_column)
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
        # Auto-inject fields referenced in formulas but not in valConfigs as implicit sum aggregations.
        # This lets formulas reference any available data field without explicitly adding it as a value.
        existing_agg_fields: set = {
            measure.get("field")
            for measure in (val_configs or [])
            if isinstance(measure, dict) and measure.get("field") and measure.get("agg") != "formula"
        }
        formula_output_fields: set = {
            measure.get("field")
            for measure in (val_configs or [])
            if isinstance(measure, dict) and measure.get("agg") == "formula"
        }
        formula_ref_fields: set = {
            measure.get("formulaRef") or ""
            for measure in (val_configs or [])
            if isinstance(measure, dict) and measure.get("agg") == "formula"
        }
        _FORMULA_IDENT_RE = re.compile(r'\b[A-Za-z_][A-Za-z0-9_]*\b')
        _RESERVED = {"True", "False", "None", "and", "or", "not", "if", "else", "in", "is"}
        already_implicit: set = set()
        for measure in (val_configs or []):
            if not isinstance(measure, dict) or measure.get("agg") != "formula":
                continue
            expr = measure.get("formula") or ""
            for ident in _FORMULA_IDENT_RE.findall(str(expr)):
                if (
                    ident in _RESERVED
                    or ident in existing_agg_fields
                    or ident in formula_output_fields
                    or ident in formula_ref_fields
                    or ident in already_implicit
                ):
                    continue
                columns.append({
                    "id": f"{ident}_sum",
                    "aggregationField": ident,
                    "aggregationFn": "sum",
                    "windowFn": None,
                    "weightField": None,
                    "_isImplicitFormulaRef": True,
                })
                already_implicit.add(ident)
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
    def _normalize_report_condition(condition: Any) -> Dict[str, Any]:
        if not isinstance(condition, dict):
            return {"op": "AND", "clauses": []}
        op = "OR" if condition.get("op") == "OR" else "AND"
        clauses = []
        for clause in (condition.get("clauses") or []):
            if not isinstance(clause, dict) or not isinstance(clause.get("field"), str) or not clause["field"]:
                continue
            clauses.append({
                "field": clause["field"],
                "operator": str(clause.get("operator") or "eq"),
                "value": str(clause.get("value", "")),
                "values": [str(v) for v in (clause.get("values") or []) if v is not None],
            })
        return {"op": op, "clauses": clauses}

    @staticmethod
    def _normalize_report_format(format_value: Any) -> Dict[str, Any]:
        source = format_value if isinstance(format_value, dict) else {}

        def _str(key: str) -> str:
            value = source.get(key)
            return value if isinstance(value, str) else ""

        indent_raw = source.get("indent")
        try:
            indent = int(indent_raw)
            if indent < 0:
                indent = None
        except (TypeError, ValueError):
            indent = None

        border_width_raw = source.get("borderWidth")
        try:
            border_width = int(border_width_raw)
            if border_width <= 0:
                border_width = None
        except (TypeError, ValueError):
            border_width = None

        border_style = _str("borderStyle")
        if border_style not in {"", "solid", "dashed", "dotted", "double"}:
            border_style = ""

        normalized: Dict[str, Any] = {
            "bold": source.get("bold") is True,
            "showSubtotal": False if source.get("showSubtotal") is False else True,
            "rowColor": _str("rowColor"),
            "labelPrefix": _str("labelPrefix"),
            "labelSuffix": _str("labelSuffix"),
            "numberFormat": _str("numberFormat"),
            "borderStyle": border_style,
            "borderColor": _str("borderColor"),
        }
        if indent is not None:
            normalized["indent"] = indent
        if border_width is not None:
            normalized["borderWidth"] = border_width
        return normalized

    @staticmethod
    def _report_condition_summary(condition: Optional[Dict[str, Any]]) -> str:
        if not isinstance(condition, dict) or not condition.get("clauses"):
            return ""
        parts = []
        for clause in condition.get("clauses") or []:
            if not isinstance(clause, dict):
                continue
            field = str(clause.get("field") or "")
            operator = str(clause.get("operator") or "eq")
            if operator in {"in", "not_in"}:
                values = clause.get("values") or []
                parts.append(f"{field} {operator} [{', '.join(str(v) for v in values[:3])}]")
            else:
                parts.append(f"{field} {operator} {clause.get('value', '')}")
        return f" {condition.get('op') or 'AND'} ".join(parts)

    @staticmethod
    def _report_display_label(base_label: Any, report_format: Optional[Dict[str, Any]]) -> str:
        text = "" if base_label is None else str(base_label)
        fmt = report_format if isinstance(report_format, dict) else {}
        return f"{fmt.get('labelPrefix') or ''}{text}{fmt.get('labelSuffix') or ''}"

    @staticmethod
    def _decorate_report_row_metadata(
        row_out: Dict[str, Any],
        *,
        node: Dict[str, Any],
        depth: int,
        row_path: str,
        row_path_values: List[str],
        row_path_fields: List[str],
        base_label: Any,
        branch_match: Optional[Dict[str, Any]] = None,
        children_source: str = "Default children",
    ) -> Dict[str, Any]:
        node_format = PivotRuntimeService._normalize_report_format(node.get("format"))
        report_format = dict(node_format)
        row_out["_reportFormat"] = report_format
        row_out["_reportDisplayLabel"] = PivotRuntimeService._report_display_label(base_label, report_format)
        row_out["_reportDebug"] = {
            "matchedLevel": node.get("label") or node.get("field") or "",
            "levelLabel": node.get("label") or node.get("field") or "",
            "levelField": node.get("field") or "",
            "depth": depth,
            "rowPath": row_path,
            "pathValues": list(row_path_values or []),
            "pathFields": list(row_path_fields or []),
            "childrenSource": children_source,
            "overrideApplied": isinstance(branch_match, dict),
            "matchedCondition": PivotRuntimeService._report_condition_summary(
                branch_match.get("condition") if isinstance(branch_match, dict) else None
            ),
        }
        return row_out

    @staticmethod
    def _evaluate_report_clause(clause: Dict[str, Any], row: Dict[str, Any]) -> bool:
        field = clause.get("field", "")
        operator = clause.get("operator", "eq")
        raw_value = row.get(field)
        str_value = "" if raw_value is None else str(raw_value)
        clause_value = str(clause.get("value", ""))
        clause_values = clause.get("values") or []
        if operator == "eq":
            return str_value == clause_value
        if operator == "not_eq":
            return str_value != clause_value
        if operator == "in":
            return str_value in clause_values
        if operator == "not_in":
            return str_value not in clause_values
        if operator == "contains":
            return clause_value.lower() in str_value.lower()
        if operator == "not_contains":
            return clause_value.lower() not in str_value.lower()
        try:
            num_value = float(str_value)
            num_clause = float(clause_value)
        except (ValueError, TypeError):
            return False
        if operator == "gt":
            return num_value > num_clause
        if operator == "gte":
            return num_value >= num_clause
        if operator == "lt":
            return num_value < num_clause
        if operator == "lte":
            return num_value <= num_clause
        return False

    @staticmethod
    def _evaluate_report_condition(condition: Dict[str, Any], row: Dict[str, Any]) -> bool:
        clauses = condition.get("clauses") or []
        if not clauses:
            return True
        if condition.get("op") == "OR":
            return any(PivotRuntimeService._evaluate_report_clause(c, row) for c in clauses)
        return all(PivotRuntimeService._evaluate_report_clause(c, row) for c in clauses)

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
            "format": PivotRuntimeService._normalize_report_format(node.get("format")),
        }

        node_filters_raw = node.get("filters")
        if isinstance(node_filters_raw, dict) and isinstance(node_filters_raw.get("clauses"), list) and node_filters_raw["clauses"]:
            normalized["filters"] = PivotRuntimeService._normalize_report_condition(node_filters_raw)

        default_child = PivotRuntimeService._normalize_branching_report_node(node.get("defaultChild"))
        if default_child:
            normalized["defaultChild"] = default_child

        branches_source = node.get("branches")
        if isinstance(branches_source, list) and branches_source:
            normalized_branches = []
            for branch in branches_source:
                if not isinstance(branch, dict):
                    continue
                norm_child = PivotRuntimeService._normalize_branching_report_node(branch.get("child"))
                normalized_branches.append({
                    "label": branch.get("label", "") if isinstance(branch.get("label"), str) else "",
                    "condition": PivotRuntimeService._normalize_report_condition(branch.get("condition")),
                    "sourceRowPath": [str(v) for v in (branch.get("sourceRowPath") or [])] if isinstance(branch.get("sourceRowPath"), list) else [],
                    "sourcePathFields": [str(v) for v in (branch.get("sourcePathFields") or [])] if isinstance(branch.get("sourcePathFields"), list) else [],
                    "child": norm_child,
                })
            if normalized_branches:
                normalized["branches"] = normalized_branches
        else:
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
    def _get_branching_report_branch(node: Dict[str, Any], row: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict) or row is None:
            return None
        branches = node.get("branches")
        if not isinstance(branches, list) or not branches:
            return None
        for branch in branches:
            if not isinstance(branch, dict):
                continue
            condition = branch.get("condition")
            if isinstance(condition, dict) and PivotRuntimeService._evaluate_report_condition(condition, row):
                return branch
        return None

    @staticmethod
    def _get_branching_report_child(node: Dict[str, Any], raw_value: Any, row: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        branch = PivotRuntimeService._get_branching_report_branch(node, row)
        if isinstance(branch, dict):
            child = branch.get("child")
            if isinstance(child, dict) and child.get("field"):
                return child
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

    @staticmethod
    def _merge_in_report_filter(filters: Dict[str, Any], field: str, values: List[Any]) -> Dict[str, Any]:
        next_filters = dict(filters or {})
        next_filters[field] = {"type": "in", "value": list(values or [])}
        return next_filters

    @staticmethod
    def _is_report_numeric_value(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float, Decimal)):
            try:
                return value == value
            except Exception:
                return False
        return False

    @staticmethod
    def _build_report_other_row_from_rows(
        rows: List[Dict[str, Any]],
        node_field: str,
    ) -> Dict[str, Any]:
        """Build a best-effort aggregate from already-aggregated sibling rows."""
        aggregate: Dict[str, Any] = {}
        ordered_keys: List[str] = []
        seen = set()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    ordered_keys.append(key)

        ignored = {
            "depth",
            "_id",
            "_label",
            "_path",
            "_rowKey",
            "_levelLabel",
            "_levelField",
            "_pathFields",
            "_has_children",
            "_is_expanded",
            "_isTotal",
            "_isOther",
            "_levelTopN",
            "_groupTotalCount",
            "_otherCount",
            "__reportSortBy",
            "__reportSortDir",
        }
        for key in ordered_keys:
            if key == node_field or key in ignored or str(key).startswith("_"):
                continue
            values = [
                row.get(key)
                for row in rows or []
                if isinstance(row, dict) and row.get(key) is not None
            ]
            numeric_values = [
                value for value in values
                if PivotRuntimeService._is_report_numeric_value(value)
            ]
            if values and len(values) == len(numeric_values):
                total = numeric_values[0]
                for value in numeric_values[1:]:
                    total += value
                aggregate[key] = total

        return aggregate

    @staticmethod
    def _decorate_report_other_row(
        base_row: Optional[Dict[str, Any]],
        *,
        node_field: str,
        path_values: List[str],
        path_fields: List[str],
        depth: int,
        level_label: str,
        source_count: int,
    ) -> Dict[str, Any]:
        row_out = dict(base_row or {})
        other_token = "__other__"
        row_path_values = list(path_values or []) + [other_token]
        row_path = "|||".join(row_path_values)
        row_path_fields = list(path_fields or []) + ([node_field] if node_field else [])

        if node_field:
            row_out[node_field] = "Other"
        row_out["_id"] = "Other"
        row_out["_label"] = "Other"
        row_out["_path"] = row_path
        row_out["_rowKey"] = row_path
        row_out["depth"] = depth
        row_out["_levelLabel"] = level_label or node_field or ""
        row_out["_levelField"] = node_field or ""
        row_out["_pathFields"] = row_path_fields
        row_out["_has_children"] = False
        row_out["_is_expanded"] = False
        row_out["_isOther"] = True
        row_out["_otherCount"] = max(int(source_count or 0), 0)
        row_out.pop("_isTotal", None)
        return row_out

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
                custom_dimensions=base_request.custom_dimensions or [],
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
                emitted_columns = [c for c in (branch_response.columns or []) if not (isinstance(c, dict) and c.get("_isImplicitFormulaRef"))]
                emitted_schema = branch_response.col_schema
                emitted_stats = branch_response.color_scale_stats

            visible_rows = [
                row for row in (branch_response.data or [])
                if isinstance(row, dict) and not self._is_report_summary_row(row)
            ]
            if sort_by:
                visible_rows = sorted(
                    visible_rows,
                    key=lambda row: self._report_sort_key(row.get(sort_by)),
                    reverse=(sort_dir != "asc"),
                )

            # Apply level filter; rows that don't match go into Others to preserve totals
            filter_excluded: List[Dict[str, Any]] = []
            node_level_filters = node.get("filters")
            if isinstance(node_level_filters, dict) and (node_level_filters.get("clauses") or []):
                pass_rows: List[Dict[str, Any]] = []
                for _row in visible_rows:
                    if PivotRuntimeService._evaluate_report_condition(node_level_filters, _row):
                        pass_rows.append(_row)
                    else:
                        filter_excluded.append(_row)
                visible_rows = pass_rows

            total_count = branch_response.total_rows if isinstance(branch_response.total_rows, int) else len(visible_rows)
            other_rows: List[Dict[str, Any]] = list(filter_excluded)
            if top_n and len(visible_rows) > top_n:
                other_rows.extend(visible_rows[top_n:])
                visible_rows = visible_rows[:top_n]

            output_rows: List[Dict[str, Any]] = []
            for row in visible_rows:
                raw_value = row.get(node_field, row.get("_id"))
                path_token = "" if raw_value is None else str(raw_value)
                row_path_values = path_values + [path_token]
                row_path_fields = path_fields + [node_field]
                row_path = "|||".join(row_path_values)
                branch_match = self._get_branching_report_branch(node, row)
                child_node = self._get_branching_report_child(node, raw_value, row)
                has_children = bool(child_node and child_node.get("field"))
                is_expanded = expanded_all or row_path in expanded_set

                row_out = dict(row)
                row_out["_id"] = path_token or str(row.get("_id") or "")
                if isinstance(branch_match, dict) and isinstance(branch_match.get("label"), str) and branch_match.get("label"):
                    row_out["_label"] = branch_match["label"]
                row_out["_path"] = row_path
                row_out["depth"] = depth
                row_out["_levelLabel"] = node.get("label") or node_field
                row_out["_levelField"] = node_field
                row_out["_pathFields"] = list(row_path_fields)
                row_out["_has_children"] = has_children
                row_out["_is_expanded"] = bool(is_expanded and has_children)
                base_label = row_out.get("_label", row_out.get("_id", path_token))
                self._decorate_report_row_metadata(
                    row_out,
                    node=node,
                    depth=depth,
                    row_path=row_path,
                    row_path_values=row_path_values,
                    row_path_fields=row_path_fields,
                    base_label=base_label,
                    branch_match=branch_match,
                    children_source="Custom children" if isinstance(branch_match, dict) and isinstance(branch_match.get("child"), dict) else "Default children",
                )
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

            report_format = node.get("format") if isinstance(node.get("format"), dict) else {}
            if other_rows and report_format.get("showSubtotal") is not False:
                other_values = [row.get(node_field, row.get("_id")) for row in other_rows if isinstance(row, dict)]
                other_values = [value for value in other_values if value is not None]
                other_aggregate_row: Optional[Dict[str, Any]] = None
                if other_values:
                    other_request = TanStackRequest(
                        operation=TanStackOperation.GET_DATA,
                        table=base_request.table,
                        columns=self._build_request_columns([], [], state.val_configs),
                        filters=self._merge_in_report_filter(active_filters, node_field, other_values),
                        custom_dimensions=base_request.custom_dimensions or [],
                        sorting=[],
                        grouping=[],
                        aggregations=[],
                        pagination={
                            "pageIndex": 0,
                            "pageSize": 1,
                        },
                        global_filter=base_request.global_filter,
                        totals=False,
                        row_totals=False,
                        version=base_request.version,
                        column_sort_options=base_request.column_sort_options,
                    )
                    other_response = await adapter.handle_request(other_request)
                    other_aggregate_row = next(
                        (candidate for candidate in (other_response.data or []) if isinstance(candidate, dict)),
                        None,
                    )

                if other_aggregate_row is None:
                    other_aggregate_row = self._build_report_other_row_from_rows(other_rows, node_field)

                other_out = self._decorate_report_other_row(
                    other_aggregate_row,
                    node_field=node_field,
                    path_values=path_values,
                    path_fields=path_fields,
                    depth=depth,
                    level_label=node.get("label") or node_field,
                    source_count=len(other_rows),
                )
                self._decorate_report_row_metadata(
                    other_out,
                    node=node,
                    depth=depth,
                    row_path=other_out.get("_path") or "",
                    row_path_values=path_values + ["__other__"],
                    row_path_fields=path_fields + [node_field],
                    base_label="Other",
                    children_source="Subtotal/other row",
                )
                output_rows.append(other_out)

            return output_rows

        visible_report_rows = await fetch_node_rows(report_root, base_filters, [], [], 0)

        if context.include_grand_total and state.show_col_totals:
            grand_total_request = TanStackRequest(
                operation=TanStackOperation.GET_DATA,
                table=base_request.table,
                columns=self._build_request_columns([], [], state.val_configs),
                filters=base_filters,
                custom_dimensions=base_request.custom_dimensions or [],
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
            other_nodes: List[Dict[str, Any]] = []
            if top_n and isinstance(top_n, int) and top_n > 0 and total_count > top_n:
                other_nodes = working_nodes[top_n:]
                omitted_count = sum(
                    PivotRuntimeService._count_report_tree_rows(node)
                    for node in other_nodes
                )
                removed = max(omitted_count - 1, 0)
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

            if other_nodes:
                first_other_row = other_nodes[0].get("row") if isinstance(other_nodes[0], dict) else {}
                first_other_row = first_other_row if isinstance(first_other_row, dict) else {}
                node_field = first_other_row.get("_levelField") or ""
                row_path_parts = PivotRuntimeService._report_row_path_parts(first_other_row)
                parent_path_values = row_path_parts[:-1] if row_path_parts else []
                path_fields = first_other_row.get("_pathFields")
                parent_path_fields = (
                    list(path_fields[:-1])
                    if isinstance(path_fields, list) and path_fields
                    else []
                )
                aggregate_row = PivotRuntimeService._build_report_other_row_from_rows(
                    [node.get("row") for node in other_nodes if isinstance(node, dict)],
                    node_field,
                )
                flattened.append(
                    PivotRuntimeService._decorate_report_other_row(
                        aggregate_row,
                        node_field=node_field,
                        path_values=parent_path_values,
                        path_fields=parent_path_fields,
                        depth=PivotRuntimeService._report_row_depth(first_other_row),
                        level_label=first_other_row.get("_levelLabel") or node_field,
                        source_count=len(other_nodes),
                    )
                )

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
