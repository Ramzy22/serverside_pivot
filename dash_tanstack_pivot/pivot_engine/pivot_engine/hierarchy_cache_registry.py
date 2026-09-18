"""Ownership of controller-local hierarchy cache namespaces and storage."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

from .cache_coordinator import CacheCoordinator, CacheInvalidationEvent, CacheNamespaces


class HierarchyCacheRegistry:
    """Create and invalidate controller-local hierarchy cache namespaces."""

    def __init__(self, coordinator: CacheCoordinator, *, ttl_seconds: float) -> None:
        self.coordinator = coordinator
        self.view: OrderedDict[str, tuple[dict[str, Any], float]] = OrderedDict()
        self.root_count: OrderedDict[str, tuple[int, float]] = OrderedDict()
        self.root_page: OrderedDict[str, tuple[list[Any], float]] = OrderedDict()
        # The bounded materialized result is intentionally separate from the
        # key-page cache: it stores completed root aggregates, not source keys.
        self.root_result: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self.grand_total: OrderedDict[str, tuple[dict[str, Any], float]] = OrderedDict()
        self.view_ttl = ttl_seconds
        self.root_count_ttl = ttl_seconds
        self.root_page_ttl = ttl_seconds
        self.root_result_ttl = ttl_seconds
        self.grand_total_ttl = ttl_seconds
        self.view_size = 32
        self.root_count_size = 64
        self.root_page_size = 128
        self.root_result_size = 8
        self.grand_total_size = 64
        self._register_namespaces()

    def _register_namespaces(self) -> None:
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_VIEW,
            owner="controller.hierarchy",
            key_dimensions=("table", "spec_fingerprint", "expanded_paths", "row_window"),
            max_entries=self.view_size,
            ttl_seconds=self.view_ttl,
            size_getter=lambda: {"entries": len(self.view)},
            invalidator=self._invalidate_view,
        )
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_COUNT,
            owner="controller.hierarchy",
            key_dimensions=("table", "rows", "filters"),
            max_entries=self.root_count_size,
            ttl_seconds=self.root_count_ttl,
            size_getter=lambda: {"entries": len(self.root_count)},
            invalidator=self._invalidate_root_count,
        )
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_PAGE,
            owner="controller.hierarchy",
            key_dimensions=("table", "root_count_key", "sort", "row_window"),
            max_entries=self.root_page_size,
            ttl_seconds=self.root_page_ttl,
            size_getter=lambda: {"entries": len(self.root_page)},
            invalidator=self._invalidate_root_page,
        )
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_ROOT_RESULT,
            owner="controller.hierarchy",
            key_dimensions=("table", "root_spec", "sort"),
            max_entries=self.root_result_size,
            ttl_seconds=self.root_result_ttl,
            size_getter=self._root_result_size_snapshot,
            invalidator=self._invalidate_root_result,
        )
        self.coordinator.register_namespace(
            CacheNamespaces.CONTROLLER_HIERARCHY_GRAND_TOTAL,
            owner="controller.hierarchy",
            key_dimensions=("table", "spec_fingerprint", "filters", "measures", "custom_dimensions"),
            max_entries=self.grand_total_size,
            ttl_seconds=self.grand_total_ttl,
            size_getter=lambda: {"entries": len(self.grand_total)},
            invalidator=self._invalidate_grand_total,
        )

    def _invalidate_view(self, _event: CacheInvalidationEvent) -> None:
        self.view.clear()

    def _invalidate_root_count(self, _event: CacheInvalidationEvent) -> None:
        self.root_count.clear()

    def _invalidate_root_page(self, _event: CacheInvalidationEvent) -> None:
        self.root_page.clear()

    def _root_result_size_snapshot(self) -> dict[str, int]:
        byte_count = 0
        for result, _expires_at in self.root_result.values():
            byte_count += max(int(getattr(result, "nbytes", 0) or 0), 0)
        return {"entries": len(self.root_result), "bytes": byte_count}

    def _invalidate_root_result(self, _event: CacheInvalidationEvent) -> None:
        self.root_result.clear()

    def _invalidate_grand_total(self, _event: CacheInvalidationEvent) -> None:
        self.grand_total.clear()
