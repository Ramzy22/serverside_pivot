"""Runtime command routing and response orchestration."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import time
from typing import Any, Callable, Dict, List, Optional

from ..backends.execution import DatabaseExecutionError
from ..resource_governor import ResourceExhaustedError, ResourceKind
from ..tanstack_adapter import TanStackRequest, TanStackResponse
from ..security import MutationAuthorizationError
from .detail_service import DetailRuntimeService
from .drill_service import RuntimeDrillService
from .export_service import ExportLimitError, RuntimeExportService
from .lifecycle import RuntimeRequestLifecycle
from .models import (
    PivotRequestContext,
    PivotServiceResponse,
    PivotViewState,
    first_present,
    safe_int,
)
from .profile import RuntimeProfileBuilder
from .report_service import RuntimeReportService
from .request_factory import RuntimeRequestFactory
from .resilience import CircuitBreakerOpen, PivotRequestTimeout
from .tree_service import TreeRuntimeService


class DistributedEditingUnavailable(RuntimeError):
    """Stable capability error for the Redis-backed distributed release."""

    code = "editing_not_available_in_distributed_mode"

    def __init__(self) -> None:
        super().__init__(self.code)


class RuntimeRequestRouter:
    """Route a normalized runtime command to its capability handler."""

    def __init__(
        self,
        *,
        adapter_getter: Callable[[], Any],
        lifecycle: RuntimeRequestLifecycle,
        tree_service: TreeRuntimeService,
        detail_service: DetailRuntimeService,
        report_service: RuntimeReportService,
        export_service: RuntimeExportService,
        drill_service: RuntimeDrillService,
        distributed_runtime: Optional[Any] = None,
        debug: bool = False,
    ) -> None:
        self._adapter_getter = adapter_getter
        self.lifecycle = lifecycle
        self.tree_service = tree_service
        self.detail_service = detail_service
        self.report_service = report_service
        self.export_service = export_service
        self.drill_service = drill_service
        self.distributed_runtime = distributed_runtime
        self.debug = debug

    @staticmethod
    def _safe_backend_error_code(exc: BaseException) -> str:
        """Return a stable client error without serializing backend exception text."""
        if isinstance(exc, DatabaseExecutionError):
            return exc.code
        return "database_execution_failed"

    @staticmethod
    def _is_distributed_editing_command(
        state: PivotViewState,
        context: PivotRequestContext,
        trigger_kind: str,
    ) -> bool:
        if trigger_kind in {"update", "transaction", "undo", "redo", "history"}:
            return True
        payload = state.transaction_request
        if not isinstance(payload, dict):
            return False
        actions = (
            payload.get("action"),
            payload.get("eventAction"),
            payload.get("event_action"),
            payload.get("historyAction"),
            payload.get("history_action"),
        )
        return any(
            str(action or "").strip().lower() in {"undo", "redo", "history"}
            for action in actions
        )

    async def process_async(
        self,
        state: PivotViewState,
        context: PivotRequestContext,
    ) -> PivotServiceResponse:
        """Process one pivot request and return a transport-neutral response."""
        service_started_at = time.perf_counter()
        trigger_kind = (
            "export"
            if state.export_request
            else (
                "transaction"
                if state.transaction_request
                else (context.trigger_kind or "data")
            )
        )

        profiling_enabled = bool(context.profiling)
        profile_builder = RuntimeProfileBuilder(
            enabled=profiling_enabled,
            state=state,
            context=context,
            trigger_kind=trigger_kind,
            started_at=service_started_at,
        )
        if (
            self.distributed_runtime is not None
            and self._is_distributed_editing_command(state, context, trigger_kind)
        ):
            return PivotServiceResponse(
                status="error",
                message=DistributedEditingUnavailable.code,
                profile=profile_builder.build(),
            )
        adapter = self._adapter_getter()
        adapter_lookup_finished_at = time.perf_counter()
        profile_builder.mark_adapter_lookup_finished(adapter_lookup_finished_at)

        build_profile = profile_builder.build

        if trigger_kind == "export":
            gate_finished_at = time.perf_counter()
            profile_builder.mark_gate_finished(gate_finished_at)
        else:
            if not self.lifecycle.register_request(context):
                gate_finished_at = time.perf_counter()
                request_built_at = gate_finished_at
                profile_builder.mark_gate_finished(gate_finished_at)
                profile_builder.mark_request_built(request_built_at)
                return PivotServiceResponse(
                    status="stale",
                    profile=build_profile(
                        cancellation_outcome="stale_registration_rejected"
                    ),
                )

            gate_finished_at = time.perf_counter()
            profile_builder.mark_gate_finished(gate_finished_at)

        request = RuntimeRequestFactory.build_request(
            state,
            context,
            export=trigger_kind == "export",
        )
        request_built_at = time.perf_counter()
        profile_builder.mark_request_built(request_built_at)

        def build_edit_overlay(rows: Any) -> Optional[Dict[str, Any]]:
            if not isinstance(rows, list) or not rows:
                return None
            edit_domain = getattr(adapter, "edit_domain", None)
            if edit_domain is None or not hasattr(
                edit_domain, "build_visible_edit_overlay"
            ):
                return None
            if hasattr(
                edit_domain, "has_visible_edit_overlay"
            ) and not edit_domain.has_visible_edit_overlay(
                request,
                session_id=context.session_id,
                client_instance=context.client_instance,
            ):
                return None
            return edit_domain.build_visible_edit_overlay(
                request,
                session_id=context.session_id,
                client_instance=context.client_instance,
                rows=rows,
            )

        if trigger_kind == "export":
            try:
                export_pagination = RuntimeRequestFactory.build_export_pagination(
                    state.export_request,
                    max_rows=self.export_service.max_rows,
                )
            except ExportLimitError as exc:
                return PivotServiceResponse(
                    status="error",
                    message=str(exc),
                    export_payload={"limitCode": exc.limit_code} if exc.limit_code else {},
                    profile=build_profile(),
                )
            except ValueError as exc:
                return PivotServiceResponse(
                    status="error",
                    message=str(exc),
                    export_payload={},
                    profile=build_profile(),
                )
            export_context = RuntimeRequestFactory.build_export_context(
                context,
                state.export_request,
            )
            export_all_rows = bool(
                first_present(
                    state.export_request
                    if isinstance(state.export_request, dict)
                    else {},
                    "allRows",
                    "all_rows",
                    default=False,
                )
            )
            export_request = replace(
                request,
                pagination=export_pagination,
                full_export=export_all_rows,
            )
            # A full export/copy must not be limited by the virtual viewport or
            # the branches currently expanded in the interactive grid.
            export_expanded_paths = (
                [["__ALL__"]]
                if export_all_rows and state.row_fields
                else RuntimeRequestFactory.parse_expanded_paths(state.expanded)
            )
            execution_started_at = time.perf_counter()

            async def execute_export():
                response = await self._execute_tanstack_data_request(
                    adapter,
                    export_request,
                    state,
                    export_context,
                    export_expanded_paths,
                    trigger_kind=trigger_kind,
                    effective_needs_col_schema=True,
                    profiling_enabled=profiling_enabled,
                    max_export_rows=self.export_service.max_rows,
                )
                execution_finished = time.perf_counter()
                payload = self.export_service.build_export_payload(
                    response, state, state.export_request,
                )
                return response, payload, execution_finished

            try:
                response, export_payload, execution_finished_at = await self.lifecycle.run_backend_operation(
                    export_context,
                    trigger_kind,
                    execute_export,
                    resource_kind=ResourceKind.EXPORT,
                    reservation_bytes=self.lifecycle.reservation_bytes(ResourceKind.EXPORT),
                )
            except ResourceExhaustedError:
                raise
            except ExportLimitError as exc:
                return PivotServiceResponse(
                    status="error",
                    message=str(exc),
                    export_payload={"limitCode": exc.limit_code} if exc.limit_code else {},
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=time.perf_counter(),
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
                if self.debug:
                    print(f"Export request failed: {exc}")
                return PivotServiceResponse(
                    status="error",
                    message=self._safe_backend_error_code(exc),
                    export_payload={},
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            postprocess_started_at = execution_finished_at
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
                        **(
                            response.profile
                            if isinstance(getattr(response, "profile", None), dict)
                            else {}
                        ),
                        "export": {
                            "format": export_payload.get("format"),
                            "bytes": int(
                                export_payload.get("contentLength")
                                or len(export_payload.get("content") or b"")
                            ),
                            "filename": export_payload.get("filename"),
                        },
                    },
                ),
            )

        if trigger_kind == "detail" and state.detail_request:
            execution_started_at = time.perf_counter()
            try:
                detail_result = await self.lifecycle.run_backend_operation(
                    context,
                    trigger_kind,
                    lambda: self.detail_service.handle_request(
                        adapter, request, state, context
                    ),
                    resource_kind=ResourceKind.QUERY,
                    reservation_bytes=self.lifecycle.reservation_bytes(ResourceKind.QUERY),
                )
            except ResourceExhaustedError:
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
            except Exception as exc:  # pragma: no cover - defensive
                execution_finished_at = time.perf_counter()
                if self.debug:
                    print(f"Detail request failed: {exc}")
                return PivotServiceResponse(
                    status="error",
                    message=self._safe_backend_error_code(exc),
                    data=[],
                    total_rows=0,
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            execution_finished_at = time.perf_counter()
            if not self.lifecycle.response_is_current(context):
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
                response_rows=(
                    len(detail_result.detail_payload.get("rows") or [])
                    if isinstance(detail_result.detail_payload, dict)
                    else None
                ),
                response_columns=(
                    len(detail_result.detail_payload.get("columns") or [])
                    if isinstance(detail_result.detail_payload, dict)
                    else None
                ),
            )
            return detail_result

        if trigger_kind == "drill" and state.drill_through:
            drill_payload = self.drill_service.normalize_request(
                state.drill_through if isinstance(state.drill_through, dict) else {}
            )
            execution_started_at = time.perf_counter()
            try:
                drill_result = await self.lifecycle.run_backend_operation(
                    context,
                    trigger_kind,
                    lambda: adapter.handle_drill_through(request, state.drill_through),
                    resource_kind=ResourceKind.QUERY,
                    reservation_bytes=self.lifecycle.reservation_bytes(ResourceKind.QUERY),
                )
            except ResourceExhaustedError:
                raise
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
                if self.debug:
                    print(f"Drill through failed: {exc}")
                drill_result = {"rows": [], "total_rows": 0}
            else:
                execution_finished_at = time.perf_counter()

            if isinstance(drill_result, dict):
                records = list(drill_result.get("rows") or [])
                drill_response_payload = self.drill_service.normalize_response(
                    drill_result, drill_payload, records
                )
            else:
                records = list(drill_result or [])
                drill_response_payload = self.drill_service.normalize_response(
                    {}, drill_payload, records
                )

            return PivotServiceResponse(
                status="drillthrough",
                drill_records=records,
                drill_payload=drill_response_payload,
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                    response_rows=len(records),
                    response_columns=(
                        len(records[0])
                        if records and isinstance(records[0], dict)
                        else None
                    ),
                ),
            )

        transaction_result: Optional[Dict[str, Any]] = None
        transaction_refresh_mode = "viewport"
        transaction_requires_structural_refresh = False

        if state.transaction_request:
            transaction_request_payload = (
                {
                    **(state.transaction_request or {}),
                    # Session identity is a server-owned request-context
                    # boundary.  Transaction payload values are browser
                    # input and must never select another user's edit history.
                    "session_id": context.session_id,
                    "client_instance": context.client_instance,
                }
                if isinstance(state.transaction_request, dict)
                else {}
            )
            execution_started_at = time.perf_counter()
            try:
                if hasattr(adapter, "handle_transaction"):
                    transaction_result = await self.lifecycle.run_backend_operation(
                        context,
                        trigger_kind,
                        lambda: adapter.handle_transaction(
                            request,
                            transaction_request_payload,
                            user=context.principal,
                            mutation_grant=context.mutation_grant,
                        ),
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
            except (MutationAuthorizationError, ResourceExhaustedError):
                # The HTTP gateway turns this into a 403. Do not convert it to
                # an apparently successful runtime envelope here.
                raise
            except Exception as exc:  # pragma: no cover - defensive
                execution_finished_at = time.perf_counter()
                if self.debug:
                    print(f"Transaction request failed: {exc}")
                return PivotServiceResponse(
                    status="error",
                    message=self._safe_backend_error_code(exc),
                    data=[],
                    total_rows=0,
                    profile=build_profile(
                        execution_started_at=execution_started_at,
                        execution_finished_at=execution_finished_at,
                    ),
                )
            execution_finished_at = time.perf_counter()
            transaction_refresh_mode = (
                str(
                    (transaction_result or {}).get("refreshMode")
                    or transaction_request_payload.get("refreshMode")
                    or transaction_request_payload.get("refresh_mode")
                    or "viewport"
                )
                .strip()
                .lower()
            )
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
                        extra=(
                            {"transaction": transaction_result}
                            if isinstance(transaction_result, dict)
                            else None
                        ),
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
                    edit_overlay=build_edit_overlay(
                        list(patch_payload.get("rows") or [])
                    ),
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
                        await adapter.handle_updates(
                            request,
                            update_payloads,
                            user=context.principal,
                            mutation_grant=context.mutation_grant,
                        )
                    else:
                        for update_payload in update_payloads:
                            await adapter.handle_update(
                                request,
                                update_payload,
                                user=context.principal,
                                mutation_grant=context.mutation_grant,
                            )

                await self.lifecycle.run_backend_operation(
                    context, trigger_kind, _apply_updates
                )
            except (CircuitBreakerOpen, PivotRequestTimeout) as exc:
                if self.debug:
                    print(f"Cell update skipped: {exc}")
            except (MutationAuthorizationError, ResourceExhaustedError):
                raise
            except Exception as exc:  # pragma: no cover - defensive
                if self.debug:
                    print(f"Cell update failed: {exc}")

        expanded_paths = RuntimeRequestFactory.parse_expanded_paths(state.expanded)
        effective_needs_col_schema = bool(
            context.needs_col_schema
            or transaction_requires_structural_refresh
            or transaction_refresh_mode in {"structural", "full", "smart_structural"}
        )

        execution_started_at = time.perf_counter()
        active_request_registered = self.lifecycle.replace_active_request_task(context)
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

            response = await self.lifecycle.run_backend_operation(
                context, trigger_kind, _execute_data_request,
                resource_kind=ResourceKind.QUERY,
                reservation_bytes=self.lifecycle.reservation_bytes(ResourceKind.QUERY),
            )
        except ResourceExhaustedError:
            raise
        except asyncio.CancelledError:
            execution_finished_at = time.perf_counter()
            if self.lifecycle.consume_superseded_cancel():
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
            if self.debug:
                print(f"Pivot execution failed: {exc}")
            return PivotServiceResponse(
                status="error",
                message=self._safe_backend_error_code(exc),
                data=[],
                total_rows=0,
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                ),
            )
        finally:
            if active_request_registered:
                self.lifecycle.release_active_request_task(context)
        execution_finished_at = time.perf_counter()

        response_version = (
            context.window_seq if context.window_seq is not None else response.version
        )
        if not self.lifecycle.response_is_current(context):
            return PivotServiceResponse(
                status="stale",
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                    response_rows=len(response.data or []),
                    response_columns=len(response.columns or []),
                    cancellation_outcome="stale_response_dropped",
                    extra=(
                        response.profile
                        if isinstance(getattr(response, "profile", None), dict)
                        else None
                    ),
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
                    "requestSignature": (state.chart_request or {}).get(
                        "request_signature"
                    ),
                },
                data_version=response_version,
                profile=build_profile(
                    execution_started_at=execution_started_at,
                    execution_finished_at=execution_finished_at,
                    postprocess_started_at=postprocess_started_at,
                    response_rows=len(response.data or []),
                    response_columns=len(response.columns or []),
                    extra=(
                        response.profile
                        if isinstance(getattr(response, "profile", None), dict)
                        else None
                    ),
                ),
            )

        # --- Report Mode: annotate rows with level metadata ---
        response_data = response.data
        response_total_rows = response.total_rows
        if (
            state.view_mode == "report"
            and state.report_def
            and isinstance(state.report_def, dict)
            and not self.report_service._has_branching_report_root(state.report_def)
        ):
            response_data, response_total_rows = (
                self.report_service._apply_report_annotations(
                    response_data,
                    response_total_rows,
                    state.report_def,
                    state.row_fields or [],
                )
            )

        cols_payload: List[Dict[str, Any]] = [
            c
            for c in (response.columns or [])
            if not (isinstance(c, dict) and c.get("_isImplicitFormulaRef"))
        ]
        should_emit_columns = (
            effective_needs_col_schema
            or not context.viewport_active
            or (
                context.intent == "structural"
                and context.original_intent != "expansion"
            )
        )
        schema_payload = (
            response.col_schema
            if bool(response.col_schema)
            and (effective_needs_col_schema or should_emit_columns)
            else None
        )

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
                extra=(
                    {
                        **(
                            response.profile
                            if isinstance(getattr(response, "profile", None), dict)
                            else {}
                        ),
                        **(
                            {"transaction": transaction_result}
                            if isinstance(transaction_result, dict)
                            else {}
                        ),
                    }
                    if (
                        isinstance(getattr(response, "profile", None), dict)
                        or isinstance(transaction_result, dict)
                    )
                    else None
                ),
            ),
        )
        return _svc_response

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
        max_export_rows: Optional[int] = None,
    ) -> TanStackResponse:
        export_all_rows = bool(
            first_present(
                state.export_request if isinstance(state.export_request, dict) else {},
                "allRows",
                "all_rows",
                default=False,
            )
        )
        if trigger_kind == "export" and export_all_rows:
            is_tabular_without_subtotals = (
                str(getattr(request, "layout_mode", "") or "").strip().lower()
                == "tabular"
                and not bool(getattr(request, "show_subtotal_footers", False))
            )
            if is_tabular_without_subtotals:
                # Query the exact size first, then request that bounded range.
                # This keeps the tabular path flat and avoids an artificial
                # max-int virtual window.
                preflight_context = replace(context, start_row=0, end_row=0)
                preflight = await self._execute_tanstack_data_request(
                    adapter,
                    request,
                    state,
                    preflight_context,
                    expanded_paths,
                    trigger_kind="data",
                    effective_needs_col_schema=effective_needs_col_schema,
                    profiling_enabled=profiling_enabled,
                )
                full_row_count = max(int(preflight.total_rows or 0), 0)
                if max_export_rows is not None and full_row_count > max_export_rows:
                    raise ExportLimitError(
                        f"Export contains {full_row_count:,} rows; the server limit is {max_export_rows:,}. "
                        "Narrow the selected range or raise PIVOT_EXPORT_MAX_ROWS.",
                        limit_code="PIVOT_EXPORT_MAX_ROWS",
                    )
                if full_row_count <= 1:
                    return preflight
                full_context = replace(
                    context, start_row=0, end_row=full_row_count - 1
                )
                return await self._execute_tanstack_data_request(
                    adapter,
                    request,
                    state,
                    full_context,
                    expanded_paths,
                    trigger_kind="data",
                    effective_needs_col_schema=effective_needs_col_schema,
                    profiling_enabled=profiling_enabled,
                    max_export_rows=max_export_rows,
                )
            # Hierarchical full exports deliberately bypass virtual-window
            # execution so the complete filtered hierarchy is produced.
            if state.row_fields and hasattr(adapter, "handle_hierarchical_request"):
                return await adapter.handle_hierarchical_request(
                    request,
                    expanded_paths,
                    user=context.principal,
                    row_scope=context.row_scope,
                )
            return await adapter.handle_request(
                request,
                user=context.principal,
                row_scope=context.row_scope,
            )
        if state.view_mode == "tree" and trigger_kind != "chart":
            response_state = await self.tree_service.handle_data_request(
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
            and self.report_service._has_branching_report_root(state.report_def)
        ):
            return await self.report_service._handle_branching_report_request(
                adapter,
                request,
                state,
                context,
                expanded_paths,
            )
        if trigger_kind == "chart":
            requested_series_ids = (
                [
                    value
                    for value in (state.chart_request or {}).get(
                        "series_column_ids", []
                    )
                    if isinstance(value, str) and value
                ]
                if isinstance(
                    (state.chart_request or {}).get("series_column_ids"), list
                )
                else None
            )
            return await adapter.handle_virtual_scroll_request(
                request,
                context.start_row,
                context.end_row if context.end_row is not None else context.start_row,
                expanded_paths,
                user=context.principal,
                row_scope=context.row_scope,
                col_start=context.col_start,
                col_end=context.col_end,
                needs_col_schema=bool(
                    (state.chart_request or {}).get("needs_col_schema", False)
                ),
                include_grand_total=context.include_grand_total,
                requested_center_ids=requested_series_ids,
                profiling=profiling_enabled,
            )
        if (
            trigger_kind == "export"
            and context.viewport_active
            and context.end_row is not None
        ):
            request_payload = (
                state.export_request if isinstance(state.export_request, dict) else {}
            )
            raw_col_ids = request_payload.get("colIds")
            requested_center_ids = None
            if isinstance(raw_col_ids, list):
                normalized_col_ids = []
                for value in raw_col_ids:
                    if value is None:
                        continue
                    column_id = str(value)
                    if column_id:
                        normalized_col_ids.append(column_id)
                requested_center_ids = normalized_col_ids or None

            payload_col_start = safe_int(
                first_present(
                    request_payload, "colStart", "col_start", default=context.col_start
                ),
                context.col_start,
            )
            payload_col_end_raw = first_present(request_payload, "colEnd", "col_end")
            payload_col_end = (
                safe_int(
                    payload_col_end_raw,
                    context.col_end if context.col_end is not None else 0,
                )
                if payload_col_end_raw is not None
                else context.col_end
            )
            export_needs_col_schema = bool(
                requested_center_ids or payload_col_end is not None
            )
            return await adapter.handle_virtual_scroll_request(
                request,
                context.start_row,
                context.end_row,
                expanded_paths,
                user=context.principal,
                row_scope=context.row_scope,
                col_start=payload_col_start,
                col_end=payload_col_end,
                needs_col_schema=export_needs_col_schema,
                include_grand_total=context.include_grand_total,
                requested_center_ids=requested_center_ids,
                profiling=profiling_enabled,
            )
        if context.viewport_active and context.end_row is not None:
            return await adapter.handle_virtual_scroll_request(
                request,
                context.start_row,
                context.end_row,
                expanded_paths,
                user=context.principal,
                row_scope=context.row_scope,
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
                user=context.principal,
                row_scope=context.row_scope,
                needs_col_schema=True,
                profiling=profiling_enabled,
            )
        return await adapter.handle_request(
            request,
            user=context.principal,
            row_scope=context.row_scope,
        )
