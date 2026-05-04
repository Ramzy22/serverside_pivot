"""Hierarchy query service and adapter contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

from pivot_engine.types.pivot_spec import PivotSpec


@dataclass
class HierarchyQuery:
    spec: PivotSpec
    expanded_paths: List[List[str]] = field(default_factory=list)
    start_row: Optional[int] = None
    end_row: Optional[int] = None
    include_grand_total_row: bool = False
    profiling: bool = False
    show_subtotal_footers: bool = False
    tabular_subtotals: bool = False


@dataclass
class HierarchyResult:
    rows: List[Dict[str, Any]]
    total_rows: int
    grand_total_row: Optional[Dict[str, Any]] = None
    grand_total_formula_source_rows: Optional[List[Dict[str, Any]]] = None
    color_scale_stats: Optional[Dict[str, Any]] = None
    expanded_stats: Optional[Dict[str, Any]] = None
    execution_mode: str = "unknown"
    profile: Optional[Dict[str, Any]] = None

    @classmethod
    def from_response(
        cls,
        response: Dict[str, Any],
        *,
        execution_mode: str,
    ) -> "HierarchyResult":
        rows = response.get("rows") or []
        return cls(
            rows=list(rows) if isinstance(rows, list) else [],
            total_rows=int(response.get("total_rows") or 0),
            grand_total_row=response.get("grand_total_row"),
            grand_total_formula_source_rows=response.get("grand_total_formula_source_rows"),
            color_scale_stats=response.get("color_scale_stats"),
            expanded_stats=response.get("expanded_stats"),
            execution_mode=execution_mode,
            profile=response.get("profile"),
        )

    def to_response(self) -> Dict[str, Any]:
        response: Dict[str, Any] = {
            "rows": self.rows,
            "total_rows": self.total_rows,
            "grand_total_row": self.grand_total_row,
            "grand_total_formula_source_rows": self.grand_total_formula_source_rows,
            "color_scale_stats": self.color_scale_stats,
            "profile": self.profile,
        }
        if self.expanded_stats is not None:
            response["expanded_stats"] = self.expanded_stats
        return response


class HierarchyAdapter(Protocol):
    execution_mode: str

    async def execute(self, query: HierarchyQuery) -> HierarchyResult:
        ...


class LegacyHierarchyAdapter:
    """Adapter over the current controller hierarchy implementation.

    This intentionally owns both row fetching and total-row calculation so
    profiling can distinguish the legacy O(N) path from future SQL-backed
    adapters.
    """

    execution_mode = "legacy_python_hierarchy"

    def __init__(self, controller: Any) -> None:
        self.controller = controller

    async def execute(self, query: HierarchyQuery) -> HierarchyResult:
        response = await self.controller._run_hierarchy_view_legacy(
            query.spec,
            query.expanded_paths,
            start_row=query.start_row,
            end_row=query.end_row,
            include_grand_total_row=query.include_grand_total_row,
            profiling=query.profiling,
            show_subtotal_footers=query.show_subtotal_footers,
            tabular_subtotals=query.tabular_subtotals,
        )
        result = HierarchyResult.from_response(response, execution_mode=self.execution_mode)
        if query.profiling:
            profile = dict(result.profile or {})
            profile["hierarchyService"] = {
                "adapter": self.execution_mode,
                "statsMode": "legacy_adapter_total_rows",
            }
            result.profile = profile
        return result


class HierarchyQueryService:
    def __init__(self, adapter: HierarchyAdapter) -> None:
        self.adapter = adapter

    async def execute(self, query: HierarchyQuery) -> Dict[str, Any]:
        result = await self.adapter.execute(query)
        return result.to_response()
