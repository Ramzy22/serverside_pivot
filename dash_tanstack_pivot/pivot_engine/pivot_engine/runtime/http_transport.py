"""Fast, host-neutral HTTP transport for Pivot Runtime Protocol V1.

This module deliberately knows nothing about Dash or Streamlit.  It validates
the one canonical V1 envelope, binds a server-authenticated principal, lets
the runtime lifecycle cancel superseded viewport work, and externalizes large
payloads so UI state is never used as a data channel.
"""

import inspect
import json
import os
import time
from dataclasses import replace
from typing import Any, Callable, Dict, Optional

from ..tanstack_adapter import TanStackOperation, TanStackRequest
from ..security import MutationAuthorizationError
from ..security_events import log_authorization_denied
from .contracts import (
    RUNTIME_SCHEMA_VERSION,
    RuntimeContractError,
    build_runtime_response,
    validate_runtime_request_v1,
    validate_runtime_response_v1,
)
from .models import (
    PivotRequestContext,
    PivotServiceResponse,
    PivotViewState,
    safe_int,
)
from .mutation_authorization import RuntimeMutationAuthorizer
from .dataset_authorization import (
    RuntimeDatasetAuthorizationError,
    RuntimeDatasetAuthorizer,
)
from .payload_store import RuntimePayloadStore, runtime_payload_store_from_env
from .distributed import RedisRuntimeState, distributed_runtime_from_env
from .request_factory import RuntimeRequestFactory


_COMMAND_TRIGGER_PROPS = {
    "data": "runtime.viewport",
    "filter_options": "runtime.filterRequest",
    "chart": "runtime.chartRequest",
    "detail": "runtime.detailRequest",
    "drill": "runtime.drillThrough",
    "update": "runtime.cellUpdates",
    "transaction": "runtime.transaction",
    "export": "runtime.export",
}


def _object(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _list(value: Any) -> list:
    return list(value) if isinstance(value, list) else []


def _inline_limit_bytes() -> int:
    try:
        value = int(os.getenv("PIVOT_RUNTIME_HTTP_INLINE_BYTES", "262144"))
    except (TypeError, ValueError):
        value = 262144
    return max(0, value)


def _json_safe(value: Any) -> Any:
    """Return a JSON-native copy before schema validation or HTTP encoding."""
    return json.loads(json.dumps(value, default=str, separators=(",", ":")))


class RuntimeHttpGateway:
    """Map authenticated V1 envelopes onto :class:`PivotRuntimeService`.

    The browser can provide a client instance for local request ordering, but
    the session namespace and RLS constraints always come from ``principal``.
    """

    def __init__(
        self,
        runtime_service_getter: Callable[[], Any],
        *,
        payload_store: Optional[RuntimePayloadStore] = None,
        payload_url_prefix: str = "/v1/runtime/payload",
        inline_payload_bytes: Optional[int] = None,
        mutation_authorizer: Optional[RuntimeMutationAuthorizer] = None,
        dataset_authorizer: Optional[RuntimeDatasetAuthorizer] = None,
        distributed_runtime: Optional[RedisRuntimeState] = None,
    ) -> None:
        self._runtime_service_getter = runtime_service_getter
        self.distributed_runtime = distributed_runtime or distributed_runtime_from_env()
        self.payload_store = payload_store or runtime_payload_store_from_env(
            runtime_state=self.distributed_runtime
        )
        self.payload_url_prefix = payload_url_prefix.rstrip("/")
        self.inline_payload_bytes = (
            _inline_limit_bytes()
            if inline_payload_bytes is None
            else max(0, int(inline_payload_bytes))
        )
        # No policy is intentionally equivalent to a read-only HTTP runtime.
        # Applications must opt in to edits with a server-owned table policy.
        self.mutation_authorizer = mutation_authorizer or RuntimeMutationAuthorizer()
        # Table access is independent of row predicates and mutation grants.
        # An omitted dataset policy is intentionally deny-by-default.
        self.dataset_authorizer = dataset_authorizer or RuntimeDatasetAuthorizer()

    @staticmethod
    def _principal_id(principal: Any) -> str:
        value = getattr(principal, "id", None)
        return str(value or "anonymous")

    def _state_from_request(
        self,
        request: Dict[str, Any],
        *,
        principal: Any,
        row_scope: Any,
    ) -> PivotViewState:
        snapshot = _object(request.get("state"))
        payload = _object(request.get("payload"))
        filters = _object(snapshot.get("filters"))

        # Bind only policy-declared row claims before a request reaches the
        # adapter cache. A principal attribute alone is not an authorization
        # policy, and a claims-scoped table cannot reach this point without a
        # complete server-derived row scope.
        filters = row_scope.apply_to_filters(filters)

        kind = str(request["kind"])
        transaction_request = payload if kind == "transaction" else None
        updates = _list(payload.get("updates") or payload.get("cellUpdates"))
        return PivotViewState(
            row_fields=[str(item) for item in _list(snapshot.get("rowFields"))],
            col_fields=[str(item) for item in _list(snapshot.get("colFields"))],
            val_configs=[item for item in _list(snapshot.get("valConfigs")) if isinstance(item, dict)],
            filters=filters,
            custom_dimensions=[item for item in _list(snapshot.get("customDimensions")) if isinstance(item, dict)],
            measure_axis=_object(snapshot.get("measureAxis")),
            sorting=[item for item in _list(snapshot.get("sorting")) if isinstance(item, dict)],
            sort_options=_object(snapshot.get("sortOptions")),
            expanded=snapshot.get("expanded", {}),
            immersive_mode=bool(snapshot.get("immersiveMode", False)),
            show_row_totals=bool(snapshot.get("showRowTotals", False)),
            show_col_totals=bool(snapshot.get("showColTotals", True)),
            show_subtotals=bool(snapshot.get("showSubtotals", True)),
            layout_mode=str(payload.get("layoutMode") or payload.get("layout_mode") or "hierarchy"),
            cell_update=(
                _object(payload.get("update"))
                if kind == "update" and isinstance(payload.get("update"), dict)
                else (payload if kind == "update" and payload.get("rowId") is not None else None)
            ),
            cell_updates=[item for item in updates if isinstance(item, dict)],
            transaction_request=transaction_request,
            drill_through=payload if kind == "drill" else None,
            detail_request=payload if kind == "detail" else None,
            export_request=payload if kind == "export" else {},
            viewport=payload,
            chart_request=payload if kind == "chart" else {},
            view_mode=str(snapshot.get("viewMode") or "pivot"),
            detail_mode=str(snapshot.get("detailMode") or "none"),
            tree_config=(snapshot.get("treeConfig") if isinstance(snapshot.get("treeConfig"), dict) else None),
            detail_config=(snapshot.get("detailConfig") if isinstance(snapshot.get("detailConfig"), dict) else None),
            report_def=(snapshot.get("reportDef") if isinstance(snapshot.get("reportDef"), dict) else None),
        )

    def _context_from_request(
        self,
        request: Dict[str, Any],
        *,
        principal: Any,
    ) -> PivotRequestContext:
        table = str(request.get("table") or "").strip()
        if not table:
            raise RuntimeContractError("Runtime request requires table.")
        payload = {
            **_object(request.get("payload")),
            "requestId": request.get("requestId"),
            "clientInstance": request.get("clientInstance") or "default",
            "stateEpoch": request.get("stateEpoch", 0),
            "windowSeq": request.get("windowSeq", 0),
            "abortGeneration": request.get("abortGeneration", 0),
            "cacheKey": request.get("cacheKey"),
        }
        context = PivotRequestContext.from_frontend(
            table=table,
            trigger_prop=_COMMAND_TRIGGER_PROPS[str(request["kind"])],
            viewport=payload,
        )
        return replace(
            context,
            # Do not trust the browser-provided session namespace.
            session_id=f"principal:{self._principal_id(principal)}",
            client_instance=str(request.get("clientInstance") or "default"),
            principal=principal,
        )

    async def _process_filter_options(
        self,
        service: Any,
        state: PivotViewState,
        context: PivotRequestContext,
        payload: Dict[str, Any],
    ) -> PivotServiceResponse:
        column_id = payload.get("columnId") or payload.get("column_id")
        if not column_id:
            return PivotServiceResponse(
                status="error", message="Missing filter request column."
            )
        limit = min(max(safe_int(payload.get("limit"), 250), 1), 500)
        offset = max(safe_int(payload.get("offset"), 0), 0)
        request = TanStackRequest(
            operation=TanStackOperation.GET_UNIQUE_VALUES,
            table=context.table,
            columns=RuntimeRequestFactory.build_columns(
                state.row_fields, state.col_fields, state.val_configs
            ),
            filters=state.filters,
            custom_dimensions=state.custom_dimensions,
            measure_axis=state.measure_axis or None,
            sorting=[],
            grouping=state.row_fields,
            aggregations=[],
            pagination={
                "pageIndex": offset // limit,
                "pageSize": limit,
                "offset": offset,
                "search": str(payload.get("search") or ""),
            },
            global_filter=str(column_id),
        )
        adapter = service._adapter_getter()
        response = await adapter.handle_request(
            request,
            user=context.principal,
            row_scope=context.row_scope,
        )
        pagination = _object(getattr(response, "pagination", None))
        values = [row.get("value") for row in (getattr(response, "data", None) or []) if isinstance(row, dict)]
        return PivotServiceResponse(
            status="filter_options",
            filter_options={
                "columnId": str(column_id),
                "options": values,
                "search": str(payload.get("search") or ""),
                "offset": offset,
                "limit": limit,
                "total": safe_int(pagination.get("totalRows"), len(values)),
                "hasMore": bool(pagination.get("hasMore", False)),
            },
        )

    def _externalize_payload(
        self,
        payload: Dict[str, Any],
        *,
        context: PivotRequestContext,
        request_id: Optional[str],
    ) -> Dict[str, Any]:
        serialized = self.payload_store.serialize_json(payload)
        if not self.inline_payload_bytes or len(serialized) <= self.inline_payload_bytes:
            return payload
        reference = self.payload_store.put_json(
            payload,
            serialized_body=serialized,
            compress=True,
            url_prefix=self.payload_url_prefix,
            metadata={
                "requestId": request_id,
                "clientInstance": context.client_instance,
                "stateEpoch": context.state_epoch,
                "subject": self._principal_id(context.principal),
                "dataVersion": context.window_seq,
            },
        )
        return {"payloadRef": reference}

    def _externalize_export_payload(
        self,
        payload: Dict[str, Any],
        *,
        context: PivotRequestContext,
        request_id: Optional[str],
    ) -> Dict[str, Any]:
        """Turn a server-local export file into an authenticated browser URL.

        Runtime exports are written to a temporary file so large XLSX files do
        not enter a JSON envelope.  Unlike Dash's callback adapter, the HTTP
        gateway previously exposed that local path directly, which leaves
        React and Streamlit with nothing the browser can download.
        """
        if not isinstance(payload, dict):
            return {}

        metadata = {
            "requestId": request_id,
            "clientInstance": context.client_instance,
            "stateEpoch": context.state_epoch,
            "windowSeq": context.window_seq,
            "subject": self._principal_id(context.principal),
            "filename": payload.get("filename"),
            "exportFormat": payload.get("format"),
            "rowCount": payload.get("rows"),
            "partId": payload.get("partId"),
        }
        content_type = str(payload.get("contentType") or "application/octet-stream")
        content_path = payload.get("contentPath") or payload.get("content_path")
        if isinstance(content_path, str) and content_path:
            reference = self.payload_store.put_file(
                content_path,
                content_type=content_type,
                metadata=metadata,
                url_prefix=self.payload_url_prefix,
            )
        else:
            content = payload.get("content")
            if not isinstance(content, (bytes, bytearray)):
                return {
                    key: value
                    for key, value in payload.items()
                    if key not in {"content", "contentPath", "content_path"}
                }
            reference = self.payload_store.put_bytes(
                bytes(content),
                content_type=content_type,
                metadata=metadata,
                url_prefix=self.payload_url_prefix,
            )

        return {
            "payloadRef": reference,
            "payloadInline": False,
            "format": payload.get("format"),
            "filename": payload.get("filename"),
            "rows": payload.get("rows"),
            "columns": payload.get("columns"),
            "partId": payload.get("partId"),
        }

    def _response_from_service(
        self,
        result: PivotServiceResponse,
        *,
        request: Dict[str, Any],
        context: PivotRequestContext,
    ) -> Dict[str, Any]:
        kind = str(request["kind"])
        status = result.status
        payload: Dict[str, Any]

        if status == "data":
            kind = "transaction" if kind == "transaction" and result.transaction_result else kind
            payload = {
                "data": list(result.data or []),
                "rowCount": result.total_rows if result.total_rows is not None else 0,
                "columns": result.columns,
                "colSchema": result.col_schema,
                "dataOffset": result.data_offset,
                "dataVersion": result.data_version,
                "transaction": result.transaction_result,
                "editOverlay": result.edit_overlay,
                "formulaErrors": result.formula_errors,
                "colorScaleStats": result.color_scale_stats,
            }
            payload = self._externalize_payload(
                payload, context=context, request_id=request.get("requestId")
            )
        elif status == "filter_options":
            kind, status, payload = "filter_options", "ok", result.filter_options or {}
        elif status == "drillthrough":
            kind, payload = "drill", result.drill_payload or {"rows": result.drill_records or []}
        elif status == "detail_data":
            kind, payload = "detail", result.detail_payload or {}
        elif status == "chart_data":
            kind, payload = "chart", result.chart_data or {}
        elif status == "export":
            kind, status = "export", "ok"
            payload = self._externalize_export_payload(
                result.export_payload or {},
                context=context,
                request_id=request.get("requestId"),
            )
        elif status == "transaction_applied":
            kind, payload = "transaction", {"transaction": result.transaction_result or {}}
        elif status == "patched":
            kind, payload = "transaction", {
                "patch": result.patch_payload or {},
                "dataOffset": result.data_offset,
                "dataVersion": result.data_version,
                "transaction": result.transaction_result or {},
                "editOverlay": result.edit_overlay,
            }
        else:
            payload = {}

        response = build_runtime_response(
            kind=kind,
            request_id=request.get("requestId"),
            status=status,
            payload=_json_safe(payload),
            table=context.table,
            session_id=context.session_id,
            client_instance=context.client_instance,
            state_epoch=context.state_epoch,
            window_seq=context.window_seq,
            message=result.message,
            profile=_json_safe(result.profile) if isinstance(result.profile, dict) else None,
        )
        return validate_runtime_response_v1(response)

    async def process(self, envelope: Any, *, principal: Any) -> Dict[str, Any]:
        """Validate, authorize, execute, and encode exactly one V1 request."""
        started_at = time.perf_counter()
        request = validate_runtime_request_v1(envelope)
        context = self._context_from_request(request, principal=principal)
        kind = str(request["kind"])
        dataset_operation = (
            "mutate"
            if kind in {"transaction", "update"}
            else ("export" if kind == "export" else "read")
        )
        row_scope = self.dataset_authorizer.authorize(
            principal=principal,
            table=context.table,
            operation=dataset_operation,
        )
        context = replace(context, row_scope=row_scope)
        if kind in {"transaction", "update"}:
            grant = self.mutation_authorizer.authorize(
                principal=principal,
                table=context.table,
                kind=str(request["kind"]),
                payload=_object(request.get("payload")),
                row_scope=row_scope,
            )
            context = replace(context, mutation_grant=grant)
        state = self._state_from_request(
            request,
            principal=principal,
            row_scope=row_scope,
        )
        service = self._runtime_service_getter()
        if request["kind"] == "filter_options":
            result = await self._process_filter_options(
                service, state, context, _object(request.get("payload"))
            )
        else:
            process_async = getattr(service, "process_async", None)
            if callable(process_async):
                result = process_async(state, context)
                result = await result if inspect.isawaitable(result) else result
            else:
                result = service.process(state, context)
                result = await result if inspect.isawaitable(result) else result
        response = self._response_from_service(result, request=request, context=context)
        profile = _object(response.get("profile"))
        profile["transport"] = {
            "protocol": "v1",
            "gatewayMs": round((time.perf_counter() - started_at) * 1000, 3),
            "payloadExternalized": bool(
                _object(response.get("payload")).get("payloadRef")
            ),
        }
        response["profile"] = profile
        return validate_runtime_response_v1(response)


def register_runtime_http_transport(
    app: Any,
    runtime_service_getter: Callable[[], Any],
    *,
    current_user_dependency: Optional[Callable[..., Any]] = None,
    payload_store: Optional[RuntimePayloadStore] = None,
    route_prefix: str = "/v1/runtime",
    inline_payload_bytes: Optional[int] = None,
    mutation_authorizer: Optional[RuntimeMutationAuthorizer] = None,
    dataset_authorizer: Optional[RuntimeDatasetAuthorizer] = None,
    distributed_runtime: Optional[RedisRuntimeState] = None,
) -> RuntimeHttpGateway:
    """Register the canonical V1 HTTP API on a FastAPI application.

    ``current_user_dependency`` is injectable for applications that already
    own authentication.  The default uses the package security dependency.
    """
    try:
        from fastapi import APIRouter, Body, Depends, HTTPException, Request
        from fastapi.responses import FileResponse, JSONResponse, Response
    except ImportError as exc:  # pragma: no cover - optional API dependency
        raise RuntimeError("FastAPI support requires serverside-pivot[api].") from exc

    if current_user_dependency is None:
        from ..security import get_current_user

        current_user_dependency = get_current_user

    normalized_prefix = "/" + route_prefix.strip("/")
    gateway = RuntimeHttpGateway(
        runtime_service_getter,
        payload_store=payload_store,
        payload_url_prefix=f"{normalized_prefix}/payload",
        inline_payload_bytes=inline_payload_bytes,
        mutation_authorizer=mutation_authorizer,
        dataset_authorizer=dataset_authorizer,
        distributed_runtime=distributed_runtime,
    )
    router = APIRouter(prefix=normalized_prefix, tags=["pivot-runtime"])

    @router.post("")
    async def runtime_request(envelope: Any = Body(...), user: Any = Depends(current_user_dependency)):
        try:
            return await gateway.process(envelope, principal=user)
        except (MutationAuthorizationError, RuntimeDatasetAuthorizationError) as exc:
            candidate = envelope if isinstance(envelope, dict) else {}
            kind = str(candidate.get("kind") or "data")
            if kind not in _COMMAND_TRIGGER_PROPS:
                kind = "data"
            operation = (
                "mutate"
                if kind in {"transaction", "update"}
                else ("export" if kind == "export" else "read")
            )
            if isinstance(exc, MutationAuthorizationError):
                log_authorization_denied(
                    principal=user,
                    table=candidate.get("table"),
                    operation=operation,
                    reason="mutation_policy",
                )
            request_id = candidate.get("requestId")
            request_id = str(request_id) if request_id is not None else None
            error_response = build_runtime_response(
                kind=kind,
                request_id=request_id,
                status="error",
                payload={},
                message=str(exc),
            )
            return JSONResponse(
                status_code=403,
                content=validate_runtime_response_v1(error_response),
            )
        except RuntimeContractError as exc:
            candidate = envelope if isinstance(envelope, dict) else {}
            kind = str(candidate.get("kind") or "data")
            if kind not in _COMMAND_TRIGGER_PROPS:
                kind = "data"
            request_id = candidate.get("requestId")
            request_id = str(request_id) if request_id is not None else None
            error_response = build_runtime_response(
                kind=kind,
                request_id=request_id,
                status="error",
                payload={},
                message=str(exc),
            )
            return JSONResponse(
                status_code=422,
                content=validate_runtime_response_v1(error_response),
            )

    @router.get("/payload/{token}")
    async def runtime_payload(token: str, request: Request, user: Any = Depends(current_user_dependency)):
        item = gateway.payload_store.get(token)
        if item is None or item.metadata.get("subject") != gateway._principal_id(user):
            raise HTTPException(status_code=404, detail="Runtime payload was not found.")
        etag = f'W/"{item.token}-{item.size}"'
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={"ETag": etag})
        filename = item.metadata.get("filename") if isinstance(item.metadata, dict) else None
        headers = {
            "Cache-Control": "no-store" if filename else "private, max-age=120",
            "ETag": etag,
            "Vary": "Authorization",
        }
        if item.metadata.get("contentEncoding"):
            headers["Content-Encoding"] = str(item.metadata["contentEncoding"])
        if item.file_path:
            if not os.path.isfile(item.file_path):
                raise HTTPException(status_code=404, detail="Runtime payload was not found.")
            safe_filename = (
                str(filename or "pivot_export")
                .replace("\\", "_")
                .replace("/", "_")
                .replace('"', "")
                .replace("\r", "")
                .replace("\n", "")
            )
            return FileResponse(
                item.file_path,
                media_type=item.content_type,
                filename=safe_filename,
                headers=headers,
            )
        if filename:
            safe_filename = (
                str(filename)
                .replace("\\", "_")
                .replace("/", "_")
                .replace('"', "")
                .replace("\r", "")
                .replace("\n", "")
            )
            headers["Content-Disposition"] = f'attachment; filename="{safe_filename}"'
        return Response(content=item.body, media_type=item.content_type, headers=headers)

    app.include_router(router)
    return gateway
