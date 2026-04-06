"""Lazy row-detail runtime service."""

from __future__ import annotations

from typing import Any, Dict, List

from .models import PivotRequestContext, PivotServiceResponse, PivotViewState, first_present, safe_int
from .tree_service import TreeRuntimeService


class DetailRuntimeService:
    """Resolve row-scoped detail payloads for pivot, tree, or table views."""

    def __init__(self, tree_service: TreeRuntimeService, debug: bool = False):
        self._tree_service = tree_service
        self._debug = debug

    async def handle_request(
        self,
        adapter: Any,
        request: Any,
        state: PivotViewState,
        context: PivotRequestContext,
    ) -> PivotServiceResponse:
        payload = self._normalize_detail_payload(
            state.detail_request if isinstance(state.detail_request, dict) else {}
        )
        if state.view_mode == "tree":
            detail_payload = await self._tree_service.handle_detail_request(
                adapter,
                request,
                state,
                payload,
            )
            return PivotServiceResponse(status="detail_data", detail_payload=detail_payload)

        drill_payload = {
            "row_path": payload["rowPath"],
            "row_fields": payload["rowFields"] or state.row_fields or [],
            "page": payload["page"],
            "page_size": payload["pageSize"],
            "sort_col": payload["sortCol"],
            "sort_dir": payload["sortDir"],
            "filter": payload["filterText"],
        }
        drill_result = await adapter.handle_drill_through(request, drill_payload)
        if isinstance(drill_result, dict):
            rows = list(drill_result.get("rows") or [])
            total_rows = safe_int(first_present(drill_result, "totalRows", "total_rows"), len(rows))
        else:
            rows = list(drill_result or [])
            total_rows = len(rows)

        detail_payload = {
            "detailKind": payload["detailKind"],
            "rowPath": payload["rowPath"],
            "rowFields": payload["rowFields"] or state.row_fields or [],
            "rowKey": payload["rowKey"] or payload["rowPath"],
            "page": payload["page"],
            "pageSize": payload["pageSize"],
            "totalRows": total_rows,
            "sortCol": payload["sortCol"],
            "sortDir": payload["sortDir"],
            "filterText": payload["filterText"],
            "rows": rows,
            "columns": self._build_columns(rows),
            "title": payload["title"] or (payload["rowPath"].rsplit("|||", 1)[-1] if payload["rowPath"] else "Detail"),
        }
        return PivotServiceResponse(status="detail_data", detail_payload=detail_payload)

    @staticmethod
    def _normalize_detail_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "detailKind": str(first_present(payload, "detailKind", "detail_kind", default="records") or "records").strip().lower(),
            "rowPath": first_present(payload, "rowPath", "row_path", default="") or "",
            "rowFields": list(first_present(payload, "rowFields", "row_fields", default=[]) or []),
            "rowKey": first_present(payload, "rowKey", "row_key"),
            "page": safe_int(first_present(payload, "page"), 0),
            "pageSize": max(1, safe_int(first_present(payload, "pageSize", "page_size"), 100)),
            "sortCol": first_present(payload, "sortCol", "sort_col"),
            "sortDir": "desc" if str(first_present(payload, "sortDir", "sort_dir", default="asc")).lower() == "desc" else "asc",
            "filterText": first_present(payload, "filterText", "filter", default="") or "",
            "title": first_present(payload, "title"),
        }

    @staticmethod
    def _build_columns(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        ignore = {key for key in rows[0].keys() if str(key).startswith("_")}
        return [{"id": key} for key in rows[0].keys() if key not in ignore]
