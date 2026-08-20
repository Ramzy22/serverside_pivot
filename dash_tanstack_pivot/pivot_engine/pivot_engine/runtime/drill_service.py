"""Canonical drill-through request and response payloads."""

from __future__ import annotations

from typing import Any, Dict, List

from .models import first_present, safe_int


class RuntimeDrillService:
    """Normalize drill payload aliases at the runtime boundary."""

    @staticmethod
    def normalize_request(drill_payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "rowPath": first_present(drill_payload, "rowPath", "row_path", default="")
            or "",
            "rowFields": list(
                first_present(
                    drill_payload,
                    "rowFields",
                    "row_fields",
                    "pathFields",
                    default=[],
                )
                or []
            ),
            "page": safe_int(first_present(drill_payload, "page"), 0),
            "pageSize": safe_int(
                first_present(drill_payload, "pageSize", "page_size"),
                100,
            ),
            "sortCol": first_present(drill_payload, "sortCol", "sort_col"),
            "sortDir": first_present(
                drill_payload,
                "sortDir",
                "sort_dir",
                default="asc",
            )
            or "asc",
            "filterText": first_present(
                drill_payload,
                "filterText",
                "filter",
                default="",
            )
            or "",
        }

    @staticmethod
    def normalize_response(
        drill_result: Dict[str, Any],
        drill_payload: Dict[str, Any],
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return {
            "rows": records,
            "page": safe_int(
                first_present(
                    drill_result,
                    "page",
                    default=drill_payload.get("page"),
                ),
                0,
            ),
            "pageSize": safe_int(
                first_present(
                    drill_result,
                    "pageSize",
                    "page_size",
                    default=drill_payload.get("pageSize"),
                ),
                safe_int(drill_payload.get("pageSize"), 100),
            ),
            "totalRows": safe_int(
                first_present(drill_result, "totalRows", "total_rows"),
                len(records),
            ),
            "sortCol": first_present(
                drill_result,
                "sortCol",
                "sort_col",
                default=drill_payload.get("sortCol"),
            ),
            "sortDir": first_present(
                drill_result,
                "sortDir",
                "sort_dir",
                default=drill_payload.get("sortDir", "asc"),
            )
            or "asc",
            "filterText": first_present(
                drill_result,
                "filterText",
                "filter",
                default=drill_payload.get("filterText", ""),
            )
            or "",
            "rowPath": first_present(
                drill_result,
                "rowPath",
                "row_path",
                default=drill_payload.get("rowPath"),
            )
            or "",
            "rowFields": list(
                first_present(
                    drill_result,
                    "rowFields",
                    "row_fields",
                    default=drill_payload.get("rowFields", []),
                )
                or []
            ),
        }
