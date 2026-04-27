"""Transport-agnostic runtime service for pivot requests."""

from __future__ import annotations

import asyncio
import csv
import html
from dataclasses import replace
from decimal import Decimal
import inspect
import math
import os
import re
import tempfile
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from ..tanstack_adapter import TanStackOperation, TanStackRequest, TanStackResponse

from .models import PivotRequestContext, PivotServiceResponse, PivotViewState, first_present, safe_int
from .resilience import CircuitBreaker, CircuitBreakerOpen, PivotRequestTimeout
from .request_coordinator import RuntimeRequestCoordinator
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
        request_coordinator: Optional[RuntimeRequestCoordinator] = None,
    ):
        self._adapter_getter = adapter_getter
        self._request_coordinator = request_coordinator or RuntimeRequestCoordinator(session_gate or SessionRequestGate())
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

    @staticmethod
    def _build_tanstack_sorting(state: PivotViewState) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Normalize frontend sorting with static per-column sort metadata."""
        tanstack_sorting: List[Dict[str, Any]] = []
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
            sort_item: Dict[str, Any] = {"id": sort_id, "desc": bool(s.get("desc", False))}

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

        return tanstack_sorting, column_sort_options

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
        trigger_kind = "export" if state.export_request else ("transaction" if state.transaction_request else (context.trigger_kind or "data"))

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
            cancellation_outcome: str = "not_cancelled",
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
                    "sessionId": context.session_id,
                    "clientInstance": context.client_instance,
                    "stateEpoch": context.state_epoch,
                    "windowSeq": context.window_seq,
                    "abortGeneration": context.abort_generation,
                    "cacheKey": context.cache_key,
                    "lifecycleLane": (
                        RuntimeRequestCoordinator.active_request_key(context) or (None, None, None)
                    )[2],
                    "cancellationOutcome": cancellation_outcome,
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

        if trigger_kind == "export":
            gate_finished_at = time.perf_counter()
        else:
            if not self._request_coordinator.register_request(context):
                gate_finished_at = time.perf_counter()
                request_built_at = gate_finished_at
                return PivotServiceResponse(
                    status="stale",
                    profile=build_profile(cancellation_outcome="stale_registration_rejected"),
                )

            gate_finished_at = time.perf_counter()

        tanstack_sorting, column_sort_options = self._build_tanstack_sorting(state)

        request_columns = self._build_request_columns(state.row_fields, state.col_fields, state.val_configs)
        pagination_info = (
            self._build_export_pagination(state.export_request)
            if trigger_kind == "export"
            else self._build_pagination(context)
        )

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

        if trigger_kind == "export":
            export_context = self._build_export_request_context(context, state.export_request)
            export_request = replace(
                request,
                pagination=self._build_export_pagination(state.export_request),
            )
            export_expanded_paths = self._parse_expanded_paths(state.expanded)
            execution_started_at = time.perf_counter()
            try:
                response = await self._run_backend_operation(
                    export_context,
                    trigger_kind,
                    lambda: self._execute_tanstack_data_request(
                        adapter,
                        export_request,
                        state,
                        export_context,
                        export_expanded_paths,
                        trigger_kind=trigger_kind,
                        effective_needs_col_schema=True,
                        profiling_enabled=profiling_enabled,
                    ),
                )
            except CircuitBreakerOpen as exc:
                execution_finished_at = time.perf_counter()
                return PivotServiceResponse(
                    status="error",
                    message=str(exc),
                    export_payload={},
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
                    export_payload={},
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            except Exception as exc:  # pragma: no cover - defensive
                execution_finished_at = time.perf_counter()
                if self._debug:
                    print(f"Export request failed: {exc}")
                return PivotServiceResponse(
                    status="error",
                    message=str(exc),
                    export_payload={},
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            execution_finished_at = time.perf_counter()
            postprocess_started_at = time.perf_counter()
            export_payload = self._build_export_payload(
                response,
                state,
                state.export_request,
            )
            return PivotServiceResponse(
                status="export",
                export_payload=export_payload,
                total_rows=export_payload.get("rows"),
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                    postprocess_started_at=postprocess_started_at,
                    response_rows=export_payload.get("rows"),
                    response_columns=export_payload.get("columns"),
                    extra={
                        **(response.profile if isinstance(getattr(response, "profile", None), dict) else {}),
                        "export": {
                            "format": export_payload.get("format"),
                            "bytes": int(export_payload.get("contentLength") or len(export_payload.get("content") or b"")),
                            "filename": export_payload.get("filename"),
                        },
                    },
                ),
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
            if not self._request_coordinator.response_is_current(context):
                return PivotServiceResponse(
                    status="stale",
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                        cancellation_outcome="stale_response_dropped",
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
        active_request_registered = self._request_coordinator.replace_active_request_task(context)
        try:
            async def _execute_data_request():
                return await self._execute_tanstack_data_request(
                    adapter,
                    request,
                    state,
                    context,
                    expanded_paths,
                    trigger_kind=trigger_kind,
                    effective_needs_col_schema=effective_needs_col_schema,
                    profiling_enabled=profiling_enabled,
                )

            response = await self._run_backend_operation(context, trigger_kind, _execute_data_request)
        except asyncio.CancelledError:
            execution_finished_at = time.perf_counter()
            if self._request_coordinator.consume_superseded_cancel():
                return PivotServiceResponse(
                    status="stale",
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                        cancellation_outcome="superseded_cancelled",
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
                self._request_coordinator.release_active_request_task(context)
        execution_finished_at = time.perf_counter()

        response_version = context.window_seq if context.window_seq is not None else response.version
        if not self._request_coordinator.response_is_current(context):
            return PivotServiceResponse(
                status="stale",
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                    response_rows=len(response.data or []),
                    response_columns=len(response.columns or []),
                    cancellation_outcome="stale_response_dropped",
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
        _svc_response = PivotServiceResponse(
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
        return _svc_response

    def process(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
    ) -> PivotServiceResponse:
        """Sync wrapper for sync transports."""
        return asyncio.run(self.process_async(state, context))

    async def _execute_tanstack_data_request(
        self,
        adapter: Any,
        request: TanStackRequest,
        state: PivotViewState,
        context: PivotRequestContext,
        expanded_paths: List[List[str]],
        *,
        trigger_kind: str,
        effective_needs_col_schema: bool,
        profiling_enabled: bool,
    ) -> TanStackResponse:
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

    @staticmethod
    def _build_export_pagination(export_request: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        request_payload = export_request if isinstance(export_request, dict) else {}
        row_start = safe_int(first_present(request_payload, "rowStart", "row_start"), 0)
        row_end_raw = first_present(request_payload, "rowEnd", "row_end")
        if row_end_raw is not None:
            row_end = max(row_start + 1, safe_int(row_end_raw, row_start + 1))
            page_size = max(1, row_end - row_start)
        else:
            page_size = max(1, safe_int(first_present(request_payload, "pageSize", "page_size"), 10_000_000))
            row_end = row_start + page_size
        return {
            "pageIndex": row_start // page_size if page_size else 0,
            "pageSize": page_size,
            "startRow": row_start,
            "endRow": row_end - 1,
        }

    @staticmethod
    def _build_export_request_context(
        context: PivotRequestContext,
        export_request: Optional[Dict[str, Any]],
    ) -> PivotRequestContext:
        pagination = PivotRuntimeService._build_export_pagination(export_request)
        payload = export_request if isinstance(export_request, dict) else {}
        return replace(
            context,
            original_intent="export",
            intent="export",
            viewport_active=True,
            start_row=safe_int(pagination.get("startRow"), 0),
            end_row=safe_int(pagination.get("endRow"), 0),
            needs_col_schema=True,
            include_grand_total=bool(first_present(payload, "include_grand_total", "includeGrandTotal", default=context.include_grand_total)),
        )

    @staticmethod
    def _build_export_payload(
        response: TanStackResponse,
        state: PivotViewState,
        export_request: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        request_payload = export_request if isinstance(export_request, dict) else {}
        export_fmt = str(request_payload.get("format") or "csv").strip().lower()
        if export_fmt == "xlsx":
            export_fmt = "xls"
        if export_fmt not in {"csv", "tsv", "html", "xls"}:
            export_fmt = "csv"
        delimiter = "\t" if export_fmt == "tsv" else ","
        ext = {"tsv": "tsv", "html": "html", "xls": "xls"}.get(export_fmt, "csv")
        content_type = {
            "tsv": "text/tab-separated-values",
            "html": "text/html",
            "xls": "application/vnd.ms-excel",
        }.get(export_fmt, "text/csv")
        part_label = request_payload.get("partLabel")
        requested_filename = request_payload.get("filename")
        if isinstance(requested_filename, str) and requested_filename.strip():
            filename = requested_filename.strip()
        else:
            filename = f"pivot_export_{part_label}.{ext}" if part_label else f"pivot_export.{ext}"
        include_headers = bool(first_present(request_payload, "includeHeaders", "include_headers", default=True))

        rows = [row for row in (response.data or []) if isinstance(row, dict)]
        field_order = PivotRuntimeService._resolve_export_field_order(
            rows,
            response.columns or [],
            request_payload.get("colIds"),
        )
        header_map = PivotRuntimeService._build_export_header_map(response.columns or [], state)
        style_profile = (
            request_payload.get("style")
            if isinstance(request_payload.get("style"), dict)
            else request_payload.get("exportStyle")
            if isinstance(request_payload.get("exportStyle"), dict)
            else {}
        )

        value_keys = {key for key in field_order if key != "_id"}

        def _cell(key: str, value: Any) -> Any:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return 0 if key in value_keys else ""
            return value

        fd, content_path = tempfile.mkstemp(prefix="pivot_export_", suffix=f".{ext}")
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as buf:
                if export_fmt in {"html", "xls"}:
                    PivotRuntimeService._write_styled_html_export(
                        buf,
                        rows=rows,
                        field_order=field_order,
                        header_map=header_map,
                        state=state,
                        style_profile=style_profile,
                        include_headers=include_headers,
                    )
                else:
                    writer = csv.writer(buf, delimiter=delimiter)
                    if include_headers and field_order:
                        writer.writerow([PivotRuntimeService._export_header_for_key(key, header_map, state, style_profile) for key in field_order])
                    for row in rows:
                        writer.writerow([_cell(key, row.get(key)) for key in field_order])
            content_length = os.path.getsize(content_path)
        except Exception:
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
            "contentType": f"{content_type};charset=utf-8",
            "format": export_fmt,
            "filename": filename,
            "rows": len(rows),
            "columns": len(field_order),
            "partId": request_payload.get("partId"),
        }

    @staticmethod
    def _export_key_aliases(key: str) -> List[str]:
        text = str(key or "")
        aliases = [text]
        if text == "_id":
            aliases.append("hierarchy")
        elif text == "hierarchy":
            aliases.append("_id")
        return aliases

    @staticmethod
    def _export_header_for_key(
        key: str,
        header_map: Dict[str, str],
        state: PivotViewState,
        style_profile: Dict[str, Any],
    ) -> str:
        labels = style_profile.get("headerLabels") if isinstance(style_profile, dict) else None
        if isinstance(labels, dict):
            for alias in PivotRuntimeService._export_key_aliases(key):
                label = labels.get(alias)
                if label is not None:
                    return str(label)
        return header_map.get(key, PivotRuntimeService._display_export_header(key, state))

    @staticmethod
    def _export_column_width(key: str, style_profile: Dict[str, Any]) -> Optional[int]:
        widths = style_profile.get("columnWidths") if isinstance(style_profile, dict) else None
        if not isinstance(widths, dict):
            return None
        for alias in PivotRuntimeService._export_key_aliases(key):
            try:
                width = int(float(widths.get(alias)))
            except (TypeError, ValueError):
                continue
            if width > 0:
                return min(max(width, 24), 1200)
        return None

    @staticmethod
    def _css_property_name(name: str) -> str:
        return re.sub(r"([A-Z])", lambda match: "-" + match.group(1).lower(), str(name or ""))

    @staticmethod
    def _style_to_css(style: Dict[str, Any]) -> str:
        if not isinstance(style, dict):
            return ""
        unitless = {"fontWeight", "lineHeight", "opacity", "zIndex"}
        parts: List[str] = []
        for prop, raw_value in style.items():
            if raw_value is None or raw_value is False or raw_value == "":
                continue
            if isinstance(raw_value, (int, float)) and math.isfinite(float(raw_value)) and prop not in unitless:
                value = f"{raw_value}px"
            else:
                value = str(raw_value)
            parts.append(f"{PivotRuntimeService._css_property_name(str(prop))}:{value}")
        return ";".join(parts)

    @staticmethod
    def _format_export_number(value: float, fmt: Any, decimal_places: Any, group_separator: Any) -> str:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value)
        if not math.isfinite(numeric):
            return ""

        fmt_text = str(fmt or "").strip()
        decimals: Optional[int]
        try:
            decimals = int(decimal_places) if decimal_places is not None else None
        except (TypeError, ValueError):
            decimals = None
        if fmt_text.startswith("fixed:") and decimals is None:
            try:
                decimals = int(fmt_text.split(":", 1)[1])
            except (IndexError, TypeError, ValueError):
                decimals = 2
        if decimals is None:
            decimals = 2 if fmt_text in {"currency", "accounting", "percent", "scientific"} or fmt_text.startswith(("currency:", "accounting:", "fixed")) else 0

        separator_map = {
            "comma": ",",
            "space": "\u00a0",
            "thin_space": "\u202f",
            "apostrophe": "'",
            "none": "",
        }
        separator = separator_map.get(str(group_separator or "thin_space"), "\u202f")

        def grouped(number: float, places: int) -> str:
            formatted = f"{number:,.{max(0, places)}f}"
            return formatted.replace(",", separator)

        if fmt_text == "percent":
            return f"{grouped(numeric * 100, decimals)}%"
        if fmt_text == "scientific":
            return f"{numeric:.{max(0, decimals)}e}"
        if fmt_text == "currency" or fmt_text.startswith("currency:"):
            symbol = fmt_text.split(":", 1)[1] if ":" in fmt_text else "$"
            sign = "-" if numeric < 0 else ""
            return f"{sign}{symbol}{grouped(abs(numeric), decimals)}"
        if fmt_text == "accounting" or fmt_text.startswith("accounting:"):
            symbol = fmt_text.split(":", 1)[1] if ":" in fmt_text else "$"
            formatted = f"{symbol}{grouped(abs(numeric), decimals)}"
            return f"({formatted})" if numeric < 0 else formatted
        return grouped(numeric, decimals)

    @staticmethod
    def _matching_value_config_for_export(key: str, state: PivotViewState, style_profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        configs = style_profile.get("valConfigs") if isinstance(style_profile.get("valConfigs"), list) else state.val_configs
        normalized_key = str(key or "").lower()
        for config in configs or []:
            if not isinstance(config, dict):
                continue
            field = str(config.get("field") or "").lower()
            agg = str(config.get("agg") or "sum").lower()
            if not field:
                continue
            if agg == "formula":
                if normalized_key == field or normalized_key.endswith(f"_{field}"):
                    return config
            else:
                suffix = f"{field}_{agg}"
                if normalized_key == field or normalized_key == suffix or normalized_key.endswith(f"_{suffix}") or f"_{field}_" in normalized_key:
                    return config
        return None

    @staticmethod
    def _formatted_export_value(
        key: str,
        value: Any,
        row: Dict[str, Any],
        state: PivotViewState,
        style_profile: Dict[str, Any],
    ) -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return ""
        if key == "_id":
            return str(value)
        if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
            decimal_overrides = style_profile.get("columnDecimalOverrides") if isinstance(style_profile, dict) else None
            format_overrides = style_profile.get("columnFormatOverrides") if isinstance(style_profile, dict) else None
            group_overrides = style_profile.get("columnGroupSeparatorOverrides") if isinstance(style_profile, dict) else None
            decimal_places = style_profile.get("decimalPlaces") if isinstance(style_profile, dict) else None
            default_format = style_profile.get("defaultValueFormat") if isinstance(style_profile, dict) else None
            group_separator = style_profile.get("numberGroupSeparator") if isinstance(style_profile, dict) else None
            config = PivotRuntimeService._matching_value_config_for_export(key, state, style_profile)
            for alias in PivotRuntimeService._export_key_aliases(key):
                if isinstance(decimal_overrides, dict) and alias in decimal_overrides:
                    decimal_places = decimal_overrides[alias]
                if isinstance(format_overrides, dict) and alias in format_overrides:
                    default_format = format_overrides[alias]
                if isinstance(group_overrides, dict) and alias in group_overrides:
                    group_separator = group_overrides[alias]
            if (not default_format) and isinstance(config, dict):
                default_format = config.get("format") or default_format
            return PivotRuntimeService._format_export_number(float(value), default_format, decimal_places, group_separator)
        if isinstance(value, (list, tuple)):
            return f"{len(value)} items"
        if isinstance(value, dict):
            return str(value)
        return str(value)

    @staticmethod
    def _export_condition_matches(condition: Any, left_value: float, right_value: Any) -> bool:
        try:
            compare_value = float(right_value)
        except (TypeError, ValueError):
            return False
        if condition == ">":
            return left_value > compare_value
        if condition == "<":
            return left_value < compare_value
        if condition == ">=":
            return left_value >= compare_value
        if condition == "<=":
            return left_value <= compare_value
        if condition in {"==", "="}:
            return left_value == compare_value
        return False

    @staticmethod
    def _conditional_export_style(key: str, value: Any, row: Dict[str, Any], style_profile: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(style_profile, dict):
            return {}
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return {}
        if not math.isfinite(numeric):
            return {}
        style: Dict[str, Any] = {}
        rules = style_profile.get("conditionalFormatting")
        if isinstance(rules, list):
            for rule in rules:
                if not isinstance(rule, dict):
                    continue
                column = rule.get("column")
                if column and str(column) not in PivotRuntimeService._export_key_aliases(key):
                    continue
                if PivotRuntimeService._export_condition_matches(rule.get("condition"), numeric, rule.get("value")):
                    rule_style = rule.get("style")
                    if isinstance(rule_style, dict):
                        style.update(rule_style)

        color_mode = str(style_profile.get("colorScaleMode") or "off")
        if color_mode == "off" or row.get("_isTotal") or row.get("_path") == "__grand_total__" or row.get("_id") == "Grand Total":
            return style
        stats_payload = style_profile.get("colorScaleStats")
        stats = None
        if isinstance(stats_payload, dict):
            if color_mode == "col" and isinstance(stats_payload.get("byCol"), dict):
                for alias in PivotRuntimeService._export_key_aliases(key):
                    if alias in stats_payload["byCol"]:
                        stats = stats_payload["byCol"][alias]
                        break
            elif color_mode == "table":
                stats = stats_payload.get("table")
        if not isinstance(stats, dict):
            return style
        try:
            min_value = float(stats.get("min"))
            max_value = float(stats.get("max"))
        except (TypeError, ValueError):
            return style
        if not math.isfinite(min_value) or not math.isfinite(max_value) or min_value == max_value:
            return style
        palettes = {
            "redGreen": {"low": (248, 105, 107), "high": (99, 190, 123)},
            "greenRed": {"low": (99, 190, 123), "high": (248, 105, 107)},
            "blueWhite": {"low": (65, 105, 225), "high": (255, 255, 255)},
            "yellowBlue": {"low": (255, 220, 0), "high": (30, 90, 200)},
            "orangePurp": {"low": (255, 140, 0), "high": (120, 50, 200)},
        }
        palette = palettes.get(str(style_profile.get("colorPalette") or "redGreen"), palettes["redGreen"])
        if min_value < 0 < max_value:
            pos = 0.5 * (numeric - min_value) / (0 - min_value) if numeric <= 0 else 0.5 + 0.5 * numeric / max_value
        else:
            pos = (numeric - min_value) / (max_value - min_value)
        clamped = max(0.0, min(1.0, pos))
        distance = abs(clamped - 0.5) * 2
        alpha = 0.06 + distance * 0.76
        rgb = palette["low"] if clamped <= 0.5 else palette["high"]
        luminance = (0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]) / 255
        style["background"] = f"rgba({rgb[0]},{rgb[1]},{rgb[2]},{alpha:.3f})"
        if alpha > 0.5:
            style["color"] = "#111827" if luminance > 0.45 else "#ffffff"
        return style

    @staticmethod
    def _export_cell_style(
        key: str,
        value: Any,
        row: Dict[str, Any],
        header_label: str,
        style_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        theme = style_profile.get("theme") if isinstance(style_profile.get("theme"), dict) else {}
        font_family = style_profile.get("fontFamily") or "'Inter', ui-sans-serif, system-ui, sans-serif"
        font_size = style_profile.get("fontSize") or "13px"
        row_height = style_profile.get("rowHeight")
        is_hierarchy = key == "_id"
        is_total_row = bool(row.get("_isTotal") or row.get("__isGrandTotal__") or row.get("_path") == "__grand_total__" or row.get("_id") == "Grand Total")
        is_total_col = str(header_label or "").startswith("Grand Total")
        background = (
            theme.get("totalBgStrong") or theme.get("totalBg") or theme.get("select") or theme.get("background") or "#fff"
            if is_total_row
            else theme.get("totalBg") or theme.get("select") or theme.get("background") or "#fff"
            if is_total_col
            else theme.get("hierarchyBg") or theme.get("surfaceInset") or theme.get("surfaceBg") or theme.get("background") or "#fff"
            if is_hierarchy
            else theme.get("surfaceBg") or theme.get("background") or "#fff"
        )
        try:
            depth = max(0, int(row.get("depth") or 0))
        except (TypeError, ValueError):
            depth = 0
        report_format = row.get("_reportFormat") if isinstance(row.get("_reportFormat"), dict) else {}
        row_id = str(
            row.get("_path")
            or ("__grand_total__" if row.get("_isTotal") or row.get("__isGrandTotal__") or row.get("_id") == "Grand Total" else None)
            or row.get("id")
            or row.get("_id")
            or ""
        )
        cell_rules = style_profile.get("cellFormatRules") if isinstance(style_profile.get("cellFormatRules"), dict) else {}
        cell_format: Dict[str, Any] = {}
        if row_id and isinstance(cell_rules, dict):
            for alias in PivotRuntimeService._export_key_aliases(key):
                candidate = cell_rules.get(f"{row_id}:::{alias}")
                if isinstance(candidate, dict):
                    cell_format = candidate
                    break
        border = (
            f"{max(1, int(float(report_format.get('borderWidth') or 1)))}px {report_format.get('borderStyle')} {report_format.get('borderColor') or theme.get('border') or '#d1d5db'}"
            if report_format.get("borderStyle")
            else f"1px solid {theme.get('border') or '#d1d5db'}"
        )
        style = {
            "fontFamily": font_family,
            "fontSize": font_size,
            "height": row_height,
            "color": cell_format.get("color") or (theme.get("totalText") or theme.get("text") if is_total_row else theme.get("text") if is_hierarchy else theme.get("textSec") or theme.get("text") or "#111827"),
            "background": cell_format.get("bg") or report_format.get("rowColor") or background,
            "border": border,
            "padding": f"4px 10px 4px {max(10, 10 + (depth * 24))}px" if is_hierarchy else "4px 10px",
            "textAlign": "left" if is_hierarchy else "right",
            "whiteSpace": "pre" if is_hierarchy else "nowrap",
            "fontVariantNumeric": None if is_hierarchy else "tabular-nums",
            "fontWeight": 700 if (cell_format.get("bold") or is_total_row or report_format.get("bold")) else 600 if is_total_col else 400,
            "fontStyle": "italic" if cell_format.get("italic") else None,
        }
        style.update(PivotRuntimeService._conditional_export_style(key, value, row, style_profile))
        if cell_format.get("bg"):
            style["background"] = cell_format.get("bg")
        if cell_format.get("color"):
            style["color"] = cell_format.get("color")
        return style

    @staticmethod
    def _export_header_style(key: str, header_label: str, style_profile: Dict[str, Any]) -> Dict[str, Any]:
        theme = style_profile.get("theme") if isinstance(style_profile.get("theme"), dict) else {}
        width = PivotRuntimeService._export_column_width(key, style_profile)
        is_total = str(header_label or "").startswith("Grand Total")
        return {
            "fontFamily": style_profile.get("fontFamily") or "'Inter', ui-sans-serif, system-ui, sans-serif",
            "fontSize": style_profile.get("fontSize") or "13px",
            "fontWeight": 700,
            "color": theme.get("totalText") or theme.get("text") if is_total else theme.get("headerText") or theme.get("text") or "#111827",
            "background": (
                theme.get("totalBgStrong") or theme.get("totalBg") or theme.get("select") or "#e5e7eb"
                if is_total
                else theme.get("headerBg") or theme.get("headerSubtleBg") or theme.get("surfaceBg") or "#f3f4f6"
            ),
            "border": f"1px solid {theme.get('border') or '#d1d5db'}",
            "padding": "6px 10px",
            "textAlign": "center",
            "verticalAlign": "middle",
            "whiteSpace": "nowrap",
            "width": f"{width}px" if width else None,
            "minWidth": f"{width}px" if width else None,
        }

    @staticmethod
    def _write_styled_html_export(
        buf: Any,
        *,
        rows: List[Dict[str, Any]],
        field_order: List[str],
        header_map: Dict[str, str],
        state: PivotViewState,
        style_profile: Dict[str, Any],
        include_headers: bool,
    ) -> None:
        font_family = style_profile.get("fontFamily") if isinstance(style_profile, dict) else None
        font_size = style_profile.get("fontSize") if isinstance(style_profile, dict) else None
        table_style = PivotRuntimeService._style_to_css({
            "borderCollapse": "collapse",
            "borderSpacing": 0,
            "fontFamily": font_family or "'Inter', ui-sans-serif, system-ui, sans-serif",
            "fontSize": font_size or "13px",
        })
        buf.write("<!DOCTYPE html><html><head><meta charset=\"utf-8\" />")
        buf.write("<style>table.pivot-export-table{border-collapse:collapse;border-spacing:0}</style>")
        buf.write("</head><body>")
        buf.write(f"<table class=\"pivot-export-table\" style=\"{html.escape(table_style, quote=True)}\">")
        if include_headers and field_order:
            buf.write("<tr>")
            for key in field_order:
                header_label = PivotRuntimeService._export_header_for_key(key, header_map, state, style_profile)
                css = PivotRuntimeService._style_to_css(PivotRuntimeService._export_header_style(key, header_label, style_profile))
                buf.write(f"<th style=\"{html.escape(css, quote=True)}\">{html.escape(header_label)}</th>")
            buf.write("</tr>")

        for row in rows:
            buf.write("<tr>")
            for key in field_order:
                raw_value = row.get(key)
                if raw_value is None and key == "_id":
                    raw_value = row.get("hierarchy")
                header_label = PivotRuntimeService._export_header_for_key(key, header_map, state, style_profile)
                display_value = PivotRuntimeService._formatted_export_value(key, raw_value, row, state, style_profile)
                css = PivotRuntimeService._style_to_css(PivotRuntimeService._export_cell_style(key, raw_value, row, header_label, style_profile))
                buf.write(f"<td style=\"{html.escape(css, quote=True)}\">{html.escape(display_value)}</td>")
            buf.write("</tr>")
        buf.write("</table></body></html>")

    @staticmethod
    def _resolve_export_field_order(
        rows: List[Dict[str, Any]],
        columns: List[Dict[str, Any]],
        col_ids: Any,
    ) -> List[str]:
        def _field_from_column_id(column_id: Any) -> Optional[str]:
            if column_id is None:
                return None
            text = str(column_id)
            return "_id" if text == "hierarchy" else text

        if isinstance(col_ids, list) and col_ids:
            ordered: List[str] = []
            seen = set()
            for column_id in col_ids:
                field = _field_from_column_id(column_id)
                if field and field not in seen:
                    ordered.append(field)
                    seen.add(field)
            return ordered

        skip = {
            "_path",
            "_isTotal",
            "_has_children",
            "depth",
            "__virtualIndex",
            "__colPending",
            "_parentPath",
            "_isGrandTotal",
            "__isGrandTotal__",
        }
        column_fields: List[str] = []
        for column in columns or []:
            if not isinstance(column, dict) or column.get("_isImplicitFormulaRef"):
                continue
            column_id = column.get("id") or column.get("accessorKey")
            field = _field_from_column_id(column_id)
            if field and field not in skip and not str(field).startswith("_") and field not in column_fields:
                column_fields.append(field)
        if column_fields:
            return (["_id"] if any("_id" in row for row in rows) and "_id" not in column_fields else []) + column_fields

        if not rows:
            return []
        first = rows[0]
        return (
            [key for key in first if key == "_id"]
            + [key for key in first if key not in skip and key != "_id" and not key.startswith("_")]
        )

    @staticmethod
    def _build_export_header_map(
        columns: List[Dict[str, Any]],
        state: PivotViewState,
    ) -> Dict[str, str]:
        header_map: Dict[str, str] = {}
        for column in columns or []:
            if not isinstance(column, dict):
                continue
            column_id = column.get("id") or column.get("accessorKey")
            if column_id is None:
                continue
            field = "_id" if str(column_id) == "hierarchy" else str(column_id)
            header = column.get("header") or column.get("name") or column.get("label")
            if header is not None:
                header_map[field] = str(header)

        agg_labels = {
            "sum": "Sum",
            "avg": "Avg",
            "count": "Cnt",
            "min": "Min",
            "max": "Max",
            "weighted_avg": "Weighted Avg",
        }
        for config in state.val_configs or []:
            if not isinstance(config, dict):
                continue
            field = config.get("field", "")
            agg = config.get("agg", "sum")
            key = f"{field}_{agg}" if agg != "formula" else field
            if key and key not in header_map:
                label = config.get("label") or f"{str(field).replace('_', ' ').title()} ({agg_labels.get(agg, agg)})"
                header_map[key] = str(label)
        return header_map

    @staticmethod
    def _display_export_header(key: str, state: PivotViewState) -> str:
        if key == "_id":
            row_fields = state.row_fields or []
            return "/".join(str(field).replace("_", " ").title() for field in row_fields) or "Row"
        return str(key).replace("_", " ").title()

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
            "sortAbs": node.get("sortAbs") is True,
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
            sort_by_raw = node.get("sortBy")
            sort_by = node_field if sort_by_raw == "__field__" else sort_by_raw
            sort_dir = node.get("sortDir") or "desc"
            sort_abs = node.get("sortAbs") is True
            branch_request = TanStackRequest(
                operation=TanStackOperation.GET_DATA,
                table=base_request.table,
                columns=self._build_request_columns([node_field], [], state.val_configs),
                filters=active_filters,
                custom_dimensions=base_request.custom_dimensions or [],
                sorting=[{"id": sort_by, "desc": sort_dir != "asc"}] if sort_by and sort_by_raw != "__field__" else [],
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
                    key=lambda row: self._report_sort_key(row.get(sort_by), sort_abs),
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
    def _report_sort_key(value: Any, abs_sort: bool = False) -> Any:
        if value is None:
            return (2, "", 0)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value != value:
                return (2, "", 0)
            numeric = abs(float(value)) if abs_sort else float(value)
            return (0, "", numeric)
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
                if sort_by == "__field__":
                    sort_by = rule.get("field") or None
                if isinstance(sort_by, str) and sort_by:
                    row["__reportSortBy"] = sort_by
                row["__reportSortDir"] = "asc" if rule.get("sortDir") == "asc" else "desc"
                if rule.get("sortAbs") is True:
                    row["__reportSortAbs"] = True

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
            sort_abs = working_nodes[0]["row"].get("__reportSortAbs") is True
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
                    key=lambda node: PivotRuntimeService._report_sort_key(node["row"].get(sort_by), sort_abs),
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
                row_out = {k: v for k, v in row.items() if k not in {"__reportSortBy", "__reportSortDir", "__reportSortAbs"}}
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
