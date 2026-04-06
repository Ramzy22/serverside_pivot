"""First-class tree-data runtime service."""

from __future__ import annotations

import asyncio
import math
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .models import PivotRequestContext, PivotServiceResponse, PivotViewState


def _stringify_path(parts: Iterable[Any]) -> str:
    normalized_parts = []
    for part in parts:
        normalized = _normalize_node_key(part)
        if normalized is None:
            continue
        normalized_parts.append(normalized)
    return "|||".join(normalized_parts)


def _sort_value(value: Any) -> Tuple[int, Any]:
    if value is None:
        return (2, "")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (0, float(value))
    return (1, str(value).lower())


def _coalesce_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def _normalize_tree_source_type(value: Any) -> str:
    normalized = _coalesce_text(value).lower()
    if normalized in {"path", "data_path", "datapath"}:
        return "path"
    if normalized in {"parentid", "parent_id", "adjacency"}:
        return "adjacency"
    if normalized in {"nested", "children"}:
        return "nested"
    return "adjacency"


def _normalize_tree_display_mode(value: Any) -> str:
    normalized = _coalesce_text(value).lower()
    if normalized in {"multiplecolumns", "multiple_columns", "multiple", "tabular", "outline"}:
        return "multipleColumns"
    return "singleColumn"


def _normalize_tree_group_default_expanded(value: Any) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 0
    return max(-1, normalized)


def _normalize_default_open_paths(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []

    normalized_paths: List[str] = []
    for entry in value:
        if isinstance(entry, str):
            text = entry.strip()
            if text:
                normalized_paths.append(text)
            continue
        if isinstance(entry, list):
            path = "|||".join(
                part
                for part in (_normalize_node_key(item) for item in entry)
                if part is not None
            )
            if path:
                normalized_paths.append(path)
    return list(dict.fromkeys(normalized_paths))


def _normalize_tree_level_labels(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    labels: List[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        label = item.strip()
        if label:
            labels.append(label)
    return labels


def _normalize_node_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"nan", "none", "null"}:
        return None
    if text.endswith(".0"):
        try:
            return str(int(float(text)))
        except ValueError:
            return text
    return text


class TreeRuntimeService:
    """Materialize visible tree rows for path, adjacency, or nested node tables."""

    def __init__(self, debug: bool = False):
        self._debug = debug

    @staticmethod
    def _normalize_tree_config(state: PivotViewState) -> Dict[str, Any]:
        source = state.tree_config if isinstance(state.tree_config, dict) else {}
        source_type = _normalize_tree_source_type(
            source.get("sourceType")
            or source.get("source_type")
            or source.get("mode")
            or source.get("treeDataMode")
            or source.get("tree_data_mode")
            or "adjacency"
        )
        value_fields = source.get("valueFields") or source.get("value_fields") or []
        extra_fields = source.get("extraFields") or source.get("extra_fields") or []
        open_by_default = source.get("openByDefault")
        if open_by_default is None:
            open_by_default = source.get("open_by_default")
        if open_by_default is not None and not isinstance(open_by_default, list):
            group_default_expanded_source = open_by_default
        elif source.get("groupDefaultExpanded") is not None:
            group_default_expanded_source = source.get("groupDefaultExpanded")
        elif source.get("group_default_expanded") is not None:
            group_default_expanded_source = source.get("group_default_expanded")
        else:
            group_default_expanded_source = source.get("defaultOpenDepth", 0)
        return {
            "sourceType": source_type,
            "idField": _coalesce_text(source.get("idField") or source.get("id_field"), "id"),
            "parentIdField": _coalesce_text(
                source.get("parentIdField")
                or source.get("parent_id_field")
                or source.get("treeDataParentIdField"),
                "parent_id",
            ),
            "pathField": _coalesce_text(
                source.get("pathField")
                or source.get("path_field")
                or source.get("treeDataPathField")
                or source.get("dataPathField"),
                "path",
            ),
            "pathSeparator": _coalesce_text(source.get("pathSeparator") or source.get("path_separator"), "|||"),
            "childrenField": _coalesce_text(
                source.get("childrenField")
                or source.get("children_field")
                or source.get("treeDataChildrenField"),
                "children",
            ),
            "labelField": _coalesce_text(source.get("labelField") or source.get("label_field"), "name"),
            "sortBy": source.get("sortBy") or source.get("sort_by"),
            "sortDir": "desc" if str(source.get("sortDir") or source.get("sort_dir") or "asc").lower() == "desc" else "asc",
            "valueFields": [str(field) for field in value_fields if isinstance(field, str) and field],
            "extraFields": [str(field) for field in extra_fields if isinstance(field, str) and field],
            "displayMode": _normalize_tree_display_mode(
                source.get("displayMode")
                or source.get("display_mode")
                or source.get("treeDataDisplayType")
                or source.get("tree_data_display_type")
            ),
            "groupDefaultExpanded": _normalize_tree_group_default_expanded(group_default_expanded_source),
            "defaultOpenPaths": _normalize_default_open_paths(
                open_by_default if isinstance(open_by_default, list) else (source.get("defaultOpenPaths") or source.get("default_open_paths"))
            ),
            "suppressGroupRowsSticky": bool(
                source.get("suppressGroupRowsSticky")
                or source.get("suppress_group_rows_sticky")
                or source.get("disableStickyGroups")
                or source.get("disable_sticky_groups")
            ),
            "levelLabels": _normalize_tree_level_labels(source.get("levelLabels") or source.get("level_labels")),
        }

    @staticmethod
    def _normalize_detail_config(state: PivotViewState) -> Dict[str, Any]:
        source = state.detail_config if isinstance(state.detail_config, dict) else {}
        default_kind = str(source.get("defaultKind") or source.get("default_kind") or "records").strip().lower() or "records"
        try:
            keep_rows_count = max(1, int(source.get("keepDetailRowsCount") or source.get("keep_detail_rows_count") or 10))
        except (TypeError, ValueError):
            keep_rows_count = 10
        refresh_strategy = str(source.get("refreshStrategy") or source.get("refresh_strategy") or "rows").strip().lower()
        if refresh_strategy not in {"rows", "everything", "nothing"}:
            refresh_strategy = "rows"
        return {
            "enabled": bool(source.get("enabled", True)),
            "defaultKind": default_kind,
            "keepDetailRows": bool(source.get("keepDetailRows") or source.get("keep_detail_rows")),
            "keepDetailRowsCount": keep_rows_count,
            "refreshStrategy": refresh_strategy,
        }

    async def handle_data_request(
        self,
        adapter: Any,
        request: Any,
        state: PivotViewState,
        context: PivotRequestContext,
        expanded_paths: List[List[str]],
    ) -> PivotServiceResponse:
        tree_config = self._normalize_tree_config(state)
        detail_config = self._normalize_detail_config(state)
        if tree_config["sourceType"] == "adjacency":
            nodes, has_children_paths = await self._load_visible_adjacency_nodes(
                adapter,
                request,
                state,
                tree_config,
                expanded_paths,
            )
            visible_rows = self._build_visible_rows(
                nodes,
                expanded_paths,
                state,
                tree_config,
                detail_config,
                has_children_paths=has_children_paths,
            )
        elif tree_config["sourceType"] == "path":
            nodes, has_children_paths = await self._load_visible_path_nodes(
                adapter,
                request,
                state,
                tree_config,
                expanded_paths,
            )
            visible_rows = self._build_visible_rows(
                nodes,
                expanded_paths,
                state,
                tree_config,
                detail_config,
                has_children_paths=has_children_paths,
            )
        else:
            source_rows = await self._load_source_rows(adapter, request, state, tree_config)
            nodes = self._materialize_nodes(source_rows, tree_config)
            visible_rows = self._build_visible_rows(nodes, expanded_paths, state, tree_config, detail_config)

        total_rows = len(visible_rows)
        if context.viewport_active and context.end_row is not None:
            start_row = max(0, int(context.start_row))
            end_row = max(start_row, int(context.end_row))
        else:
            start_row = 0
            end_row = min(max(total_rows - 1, 0), 99)

        page_rows = visible_rows[start_row:end_row + 1] if total_rows > 0 else []
        return PivotServiceResponse(
            status="data",
            data=page_rows,
            total_rows=total_rows,
            columns=self._build_columns(tree_config, page_rows),
            data_offset=start_row,
            data_version=context.window_seq,
        )

    async def handle_detail_request(
        self,
        adapter: Any,
        request: Any,
        state: PivotViewState,
        detail_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        tree_config = self._normalize_tree_config(state)
        source_rows = await self._load_source_rows(adapter, request, state, tree_config, include_extra_fields=True)
        normalized_payload = self._normalize_detail_request_payload(detail_payload, tree_config, state)

        if tree_config["sourceType"] == "nested":
            materialized_nodes = self._materialize_nodes(source_rows, tree_config)
            source_rows = [node["_sourceRow"] for node in materialized_nodes.values() if isinstance(node, dict) and node.get("_sourceRow") is not None]

        rows = self._collect_detail_rows(source_rows, tree_config, normalized_payload)
        if normalized_payload["filterText"]:
            rows = self._apply_text_filter(rows, normalized_payload["filterText"])
        if normalized_payload["sortCol"]:
            rows = self._apply_row_sort(rows, normalized_payload["sortCol"], normalized_payload["sortDir"])
        page = normalized_payload["page"]
        page_size = normalized_payload["pageSize"]
        start = page * page_size
        end = start + page_size
        page_rows = rows[start:end]

        columns = self._build_detail_columns(page_rows)
        return {
            "detailKind": normalized_payload["detailKind"],
            "rowPath": normalized_payload["rowPath"],
            "rowKey": normalized_payload["rowKey"],
            "page": page,
            "pageSize": page_size,
            "totalRows": len(rows),
            "rows": page_rows,
            "columns": columns,
            "title": normalized_payload["title"],
        }

    def _build_filtered_tree_expr(
        self,
        adapter: Any,
        request: Any,
        state: PivotViewState,
        tree_config: Dict[str, Any],
        *,
        include_extra_fields: bool = False,
    ) -> Tuple[Any, Any]:
        controller = getattr(adapter, "controller", None)
        planner = getattr(controller, "planner", None)
        con = getattr(planner, "con", None)
        if con is None:
            return None, None

        spec = adapter.convert_tanstack_request_to_pivot_spec(request)
        filtered_table = con.table(request.table)
        builder = getattr(planner, "builder", None)
        filter_expr = builder.build_filter_expression(filtered_table, spec.filters) if builder and spec.filters else None
        if filter_expr is not None:
            filtered_table = filtered_table.filter(filter_expr)

        requested_columns = self._collect_requested_columns(
            filtered_table,
            tree_config,
            list(request.sorting or []),
            include_extra_fields=include_extra_fields,
        )
        selected_table = filtered_table.select(sorted(requested_columns)) if requested_columns else filtered_table
        return filtered_table, selected_table

    @staticmethod
    def _collect_requested_columns(
        table_expr: Any,
        tree_config: Dict[str, Any],
        sort_specs: List[Dict[str, Any]],
        *,
        include_extra_fields: bool = False,
    ) -> Set[str]:
        requested_columns: Set[str] = set()
        for field_name in (
            tree_config["idField"],
            tree_config["parentIdField"],
            tree_config["pathField"],
            tree_config["labelField"],
            tree_config["childrenField"],
        ):
            if field_name and field_name in table_expr.columns:
                requested_columns.add(field_name)

        for field_name in tree_config["valueFields"]:
            if field_name in table_expr.columns:
                requested_columns.add(field_name)

        for field_name in tree_config["extraFields"]:
            if field_name in table_expr.columns:
                requested_columns.add(field_name)

        if include_extra_fields:
            for field_name in table_expr.columns:
                requested_columns.add(field_name)

        for sort_spec in sort_specs:
            sort_id = sort_spec.get("id") if isinstance(sort_spec, dict) else None
            if sort_id and sort_id in table_expr.columns:
                requested_columns.add(sort_id)

        return requested_columns

    @staticmethod
    async def _execute_rows_async(table_expr: Any) -> List[Dict[str, Any]]:
        loop = asyncio.get_running_loop()

        def execute_rows() -> List[Dict[str, Any]]:
            frame = table_expr.execute()
            if hasattr(frame, "to_dict"):
                return frame.to_dict("records")
            if isinstance(frame, list):
                return frame
            return []

        return await loop.run_in_executor(None, execute_rows)

    def _coerce_values_for_field(
        self,
        table_expr: Any,
        field_name: str,
        values: Iterable[Any],
    ) -> List[Any]:
        schema = table_expr.schema()
        dtype = schema[field_name] if field_name in schema else None
        coerced: List[Any] = []
        seen = set()
        for value in values:
            if value is None:
                continue
            next_value: Any = value
            try:
                if dtype is not None and hasattr(dtype, "is_integer") and dtype.is_integer():
                    next_value = int(float(value))
                elif dtype is not None and hasattr(dtype, "is_floating") and dtype.is_floating():
                    next_value = float(value)
                elif dtype is not None and hasattr(dtype, "is_boolean") and dtype.is_boolean():
                    if isinstance(value, str):
                        lowered = value.strip().lower()
                        next_value = lowered in {"1", "true", "yes", "y", "t"}
                    else:
                        next_value = bool(value)
            except (TypeError, ValueError):
                next_value = value
            cache_key = repr(next_value)
            if cache_key in seen:
                continue
            seen.add(cache_key)
            coerced.append(next_value)
        return coerced

    @staticmethod
    def _normalize_expanded_set(expanded_paths: List[List[str]]) -> Set[str]:
        return {
            _stringify_path(path)
            for path in expanded_paths
            if isinstance(path, list) and path and path != ["__ALL__"]
        }

    @staticmethod
    def _should_use_default_tree_expansion(state: PivotViewState) -> bool:
        return state.expanded is None or (isinstance(state.expanded, dict) and len(state.expanded) == 0)

    @staticmethod
    def _build_adjacency_node(
        row: Dict[str, Any],
        *,
        node_path: str,
        parent_path: str,
        label_field: str,
    ) -> Dict[str, Any]:
        return {
            **row,
            "_nodePath": node_path,
            "_parentPath": parent_path,
            "_label": _coalesce_text(row.get(label_field), node_path.rsplit("|||", 1)[-1]),
            "_sourceRow": row,
        }

    async def _query_adjacency_roots(
        self,
        selected_table: Any,
        tree_config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        parent_field = tree_config["parentIdField"]
        if parent_field not in selected_table.columns:
            return []
        return await self._execute_rows_async(selected_table.filter(selected_table[parent_field].isnull()))

    async def _query_adjacency_children(
        self,
        filtered_table: Any,
        selected_table: Any,
        tree_config: Dict[str, Any],
        parent_ids: List[str],
    ) -> List[Dict[str, Any]]:
        parent_field = tree_config["parentIdField"]
        typed_ids = self._coerce_values_for_field(filtered_table, parent_field, parent_ids)
        if not typed_ids:
            return []
        return await self._execute_rows_async(selected_table.filter(selected_table[parent_field].isin(typed_ids)))

    async def _query_adjacency_child_counts(
        self,
        filtered_table: Any,
        tree_config: Dict[str, Any],
        parent_ids: List[str],
    ) -> Dict[str, int]:
        parent_field = tree_config["parentIdField"]
        typed_ids = self._coerce_values_for_field(filtered_table, parent_field, parent_ids)
        if not typed_ids:
            return {}
        grouped_expr = (
            filtered_table
            .filter(filtered_table[parent_field].isin(typed_ids))
            .group_by(parent_field)
            .aggregate(_child_count=filtered_table[parent_field].count())
        )
        rows = await self._execute_rows_async(grouped_expr)
        child_counts: Dict[str, int] = {}
        for row in rows:
            parent_key = _normalize_node_key(row.get(parent_field))
            if parent_key is None:
                continue
            child_counts[parent_key] = int(row.get("_child_count") or 0)
        return child_counts

    async def _load_visible_adjacency_nodes(
        self,
        adapter: Any,
        request: Any,
        state: PivotViewState,
        tree_config: Dict[str, Any],
        expanded_paths: List[List[str]],
    ) -> Tuple[Dict[str, Dict[str, Any]], Set[str]]:
        filtered_table, selected_table = self._build_filtered_tree_expr(adapter, request, state, tree_config)
        if filtered_table is None or selected_table is None:
            return {}, set()

        id_field = tree_config["idField"]
        label_field = tree_config["labelField"]
        expanded_set = self._normalize_expanded_set(expanded_paths)
        use_default_expansion = self._should_use_default_tree_expansion(state)
        default_open_depth = tree_config.get("groupDefaultExpanded", 0) if use_default_expansion else 0
        default_open_paths = set(tree_config.get("defaultOpenPaths") or []) if use_default_expansion else set()
        root_rows = await self._query_adjacency_roots(selected_table, tree_config)
        root_ids = [
            row_id
            for row_id in (_normalize_node_key(row.get(id_field)) for row in root_rows)
            if row_id is not None
        ]
        child_counts = await self._query_adjacency_child_counts(filtered_table, tree_config, root_ids)

        nodes: Dict[str, Dict[str, Any]] = {}
        has_children_paths: Set[str] = set()
        frontier: List[Tuple[str, str, int]] = []

        for row in root_rows:
            row_id = _normalize_node_key(row.get(id_field))
            if row_id is None:
                continue
            node_path = row_id
            nodes[node_path] = self._build_adjacency_node(
                row,
                node_path=node_path,
                parent_path="",
                label_field=label_field,
            )
            if child_counts.get(row_id, 0) > 0:
                has_children_paths.add(node_path)
                if (
                    node_path in expanded_set
                    or (default_open_depth > 0)
                    or (node_path in default_open_paths)
                ):
                    frontier.append((node_path, row_id, 0))

        visited = set()
        while frontier:
            next_frontier: List[Tuple[str, str, int]] = []
            batch = [(path, row_id, depth) for path, row_id, depth in frontier if path not in visited]
            visited.update(path for path, _, _ in batch)
            if not batch:
                break
            child_rows = await self._query_adjacency_children(
                filtered_table,
                selected_table,
                tree_config,
                [row_id for _, row_id, _ in batch],
            )
            child_ids = [
                row_id
                for row_id in (_normalize_node_key(row.get(id_field)) for row in child_rows)
                if row_id is not None
            ]
            child_counts = await self._query_adjacency_child_counts(filtered_table, tree_config, child_ids)
            grouped_children: Dict[str, List[Dict[str, Any]]] = {}
            for row in child_rows:
                parent_key = _normalize_node_key(row.get(tree_config["parentIdField"]))
                if parent_key is None:
                    continue
                grouped_children.setdefault(parent_key, []).append(row)

            for parent_path, parent_id, depth in batch:
                for row in grouped_children.get(parent_id, []):
                    row_id = _normalize_node_key(row.get(id_field))
                    if row_id is None:
                        continue
                    child_path = _stringify_path([parent_path, row_id])
                    nodes[child_path] = self._build_adjacency_node(
                        row,
                        node_path=child_path,
                        parent_path=parent_path,
                        label_field=label_field,
                    )
                    if child_counts.get(row_id, 0) > 0:
                        has_children_paths.add(child_path)
                        child_depth = depth + 1
                        if (
                            child_path in expanded_set
                            or (default_open_depth > child_depth)
                            or (child_path in default_open_paths)
                        ):
                            next_frontier.append((child_path, row_id, child_depth))
            frontier = next_frontier

        return nodes, has_children_paths

    async def _load_visible_path_nodes(
        self,
        adapter: Any,
        request: Any,
        state: PivotViewState,
        tree_config: Dict[str, Any],
        expanded_paths: List[List[str]],
    ) -> Tuple[Dict[str, Dict[str, Any]], Set[str]]:
        filtered_table, selected_table = self._build_filtered_tree_expr(adapter, request, state, tree_config)
        if filtered_table is None or selected_table is None:
            return {}, set()

        path_field = tree_config["pathField"]
        separator = tree_config["pathSeparator"]
        schema = selected_table.schema()
        path_dtype = schema[path_field] if path_field in schema else None
        if path_dtype is None or not hasattr(path_dtype, "is_string") or not path_dtype.is_string():
            source_rows = await self._load_source_rows(adapter, request, state, tree_config)
            nodes = self._materialize_nodes(source_rows, tree_config)
            has_children_paths = {
                node.get("_parentPath")
                for node in nodes.values()
                if isinstance(node, dict) and node.get("_parentPath")
            }
            return nodes, {path for path in has_children_paths if path}

        try:
            root_rows = await self._execute_rows_async(selected_table.filter(~selected_table[path_field].contains(separator)))
            descendant_marker_rows = await self._execute_rows_async(
                filtered_table
                .filter(filtered_table[path_field].contains(separator))
                .select([path_field])
            )
        except Exception:
            source_rows = await self._load_source_rows(adapter, request, state, tree_config)
            nodes = self._materialize_nodes(source_rows, tree_config)
            has_children_paths = {
                node.get("_parentPath")
                for node in nodes.values()
                if isinstance(node, dict) and node.get("_parentPath")
            }
            return nodes, {path for path in has_children_paths if path}

        expanded_set = self._normalize_expanded_set(expanded_paths)
        use_default_expansion = self._should_use_default_tree_expansion(state)
        default_open_depth = tree_config.get("groupDefaultExpanded", 0) if use_default_expansion else 0
        default_open_paths = set(tree_config.get("defaultOpenPaths") or []) if use_default_expansion else set()
        expanded_root_paths = {
            expanded_path.split(separator, 1)[0]
            for expanded_path in expanded_set
            if expanded_path
        }
        if default_open_depth > 0:
            expanded_root_paths.update(
                _stringify_path(str(row.get(path_field)).split(separator))
                for row in root_rows
                if row.get(path_field) is not None
            )
        if default_open_paths:
            expanded_root_paths.update(
                open_path.split("|||", 1)[0]
                for open_path in default_open_paths
                if open_path
            )
        expanded_root_paths = {path for path in expanded_root_paths if path}

        descendant_rows: List[Dict[str, Any]] = []
        if expanded_root_paths:
            prefix_condition = None
            for root_path in sorted(expanded_root_paths):
                prefix = f"{root_path}{separator}"
                next_condition = selected_table[path_field].startswith(prefix)
                prefix_condition = next_condition if prefix_condition is None else (prefix_condition | next_condition)
            if prefix_condition is not None:
                descendant_rows = await self._execute_rows_async(selected_table.filter(prefix_condition))

        nodes = self._materialize_path_nodes([*root_rows, *descendant_rows], tree_config)
        has_children_paths: Set[str] = set()
        for row in descendant_marker_rows:
            raw_path = row.get(path_field)
            normalized_path = _stringify_path(str(raw_path).split(separator)) if raw_path is not None else ""
            if not normalized_path:
                continue
            path_parts = [part for part in normalized_path.split("|||") if part]
            for depth in range(1, len(path_parts)):
                has_children_paths.add(_stringify_path(path_parts[:depth]))
        return nodes, has_children_paths

    async def _load_source_rows(
        self,
        adapter: Any,
        request: Any,
        state: PivotViewState,
        tree_config: Dict[str, Any],
        *,
        include_extra_fields: bool = False,
    ) -> List[Dict[str, Any]]:
        _filtered_table, selected_table = self._build_filtered_tree_expr(
            adapter,
            request,
            state,
            tree_config,
            include_extra_fields=include_extra_fields,
        )
        if selected_table is None:
            return []
        return await self._execute_rows_async(selected_table)

    def _materialize_nodes(
        self,
        source_rows: List[Dict[str, Any]],
        tree_config: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        source_type = tree_config["sourceType"]
        if source_type == "nested":
            return self._materialize_nested_nodes(source_rows, tree_config)
        if source_type == "path":
            return self._materialize_path_nodes(source_rows, tree_config)
        return self._materialize_adjacency_nodes(source_rows, tree_config)

    def _materialize_path_nodes(
        self,
        source_rows: List[Dict[str, Any]],
        tree_config: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        nodes: Dict[str, Dict[str, Any]] = {}
        path_field = tree_config["pathField"]
        separator = tree_config["pathSeparator"]
        label_field = tree_config["labelField"]

        for row in source_rows:
            raw_path = row.get(path_field)
            if raw_path is None:
                continue
            if isinstance(raw_path, list):
                path_parts = [_coalesce_text(part) for part in raw_path if _coalesce_text(part)]
            else:
                path_parts = [_coalesce_text(part) for part in str(raw_path).split(separator) if _coalesce_text(part)]
            if not path_parts:
                continue
            path = _stringify_path(path_parts)
            parent_path = _stringify_path(path_parts[:-1]) if len(path_parts) > 1 else ""
            label = _coalesce_text(row.get(label_field), path_parts[-1])
            nodes[path] = {
                **row,
                "_nodePath": path,
                "_parentPath": parent_path,
                "_label": label,
                "_sourceRow": row,
            }
        return nodes

    def _materialize_adjacency_nodes(
        self,
        source_rows: List[Dict[str, Any]],
        tree_config: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        id_field = tree_config["idField"]
        parent_field = tree_config["parentIdField"]
        label_field = tree_config["labelField"]
        raw_nodes: Dict[str, Dict[str, Any]] = {}
        path_cache: Dict[str, List[str]] = {}
        label_path_cache: Dict[str, List[str]] = {}

        for row in source_rows:
            row_id = _normalize_node_key(row.get(id_field))
            if row_id is None:
                continue
            raw_nodes[row_id] = {
                **row,
                "_rawId": row_id,
                "_rawParentId": _normalize_node_key(row.get(parent_field)),
                "_label": _coalesce_text(row.get(label_field), row_id),
                "_sourceRow": row,
            }

        def build_path(node_id: str, seen: Optional[set] = None) -> List[str]:
            if node_id in path_cache:
                return path_cache[node_id]
            seen = seen or set()
            if node_id in seen:
                path_cache[node_id] = [node_id]
                return path_cache[node_id]
            seen.add(node_id)
            node = raw_nodes[node_id]
            parent_id = node.get("_rawParentId")
            if parent_id and parent_id in raw_nodes:
                parts = build_path(parent_id, seen) + [node_id]
            else:
                parts = [node_id]
            path_cache[node_id] = parts
            return parts

        def build_label_path(node_id: str, seen: Optional[set] = None) -> List[str]:
            if node_id in label_path_cache:
                return label_path_cache[node_id]
            seen = seen or set()
            if node_id in seen:
                label_path_cache[node_id] = [raw_nodes[node_id]["_label"]]
                return label_path_cache[node_id]
            seen.add(node_id)
            node = raw_nodes[node_id]
            parent_id = node.get("_rawParentId")
            if parent_id and parent_id in raw_nodes:
                parts = build_label_path(parent_id, seen) + [node["_label"]]
            else:
                parts = [node["_label"]]
            label_path_cache[node_id] = parts
            return parts

        nodes: Dict[str, Dict[str, Any]] = {}
        for node_id, node in raw_nodes.items():
            path_parts = build_path(node_id)
            label_parts = build_label_path(node_id)
            path = _stringify_path(path_parts)
            parent_path = _stringify_path(path_parts[:-1]) if len(path_parts) > 1 else ""
            nodes[path] = {
                **node,
                "_nodePath": path,
                "_parentPath": parent_path,
                "_displayPath": _stringify_path(label_parts),
            }
        return nodes

    def _materialize_nested_nodes(
        self,
        source_rows: List[Dict[str, Any]],
        tree_config: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        label_field = tree_config["labelField"]
        id_field = tree_config["idField"]
        children_field = tree_config["childrenField"]
        nodes: Dict[str, Dict[str, Any]] = {}

        def visit(row: Dict[str, Any], parent_path: str = "") -> None:
            label = _coalesce_text(row.get(label_field), _coalesce_text(row.get(id_field), "Node"))
            current_path = _stringify_path([parent_path, label]) if parent_path else label
            node = {
                **row,
                "_nodePath": current_path,
                "_parentPath": parent_path,
                "_label": label,
                "_sourceRow": {k: v for k, v in row.items() if k != children_field},
            }
            nodes[current_path] = node
            children = row.get(children_field)
            if isinstance(children, list):
                for child in children:
                    if isinstance(child, dict):
                        visit(child, current_path)

        for row in source_rows:
            if isinstance(row, dict):
                visit(row)
        return nodes

    def _build_visible_rows(
        self,
        nodes: Dict[str, Dict[str, Any]],
        expanded_paths: List[List[str]],
        state: PivotViewState,
        tree_config: Dict[str, Any],
        detail_config: Dict[str, Any],
        has_children_paths: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        children_by_parent: Dict[str, List[str]] = {}
        roots: List[str] = []

        for path, node in nodes.items():
            parent_path = node.get("_parentPath") or ""
            if parent_path and parent_path in nodes:
                children_by_parent.setdefault(parent_path, []).append(path)
            else:
                roots.append(path)

        sort_field, sort_desc = self._resolve_tree_sort(state, tree_config)
        sort_paths = self._build_sorter(nodes, sort_field, sort_desc)
        roots = sort_paths(roots)
        for parent_path, child_paths in list(children_by_parent.items()):
            children_by_parent[parent_path] = sort_paths(child_paths)

        expanded_all = state.expanded is True or expanded_paths == [["__ALL__"]]
        expanded_set = {
            _stringify_path(path)
            for path in expanded_paths
            if isinstance(path, list) and path and path != ["__ALL__"]
        }
        default_open_paths = set(tree_config.get("defaultOpenPaths") or [])
        use_default_expansion = (
            state.expanded is None
            or (isinstance(state.expanded, dict) and len(state.expanded) == 0)
        )
        default_open_depth = tree_config.get("groupDefaultExpanded", 0) if use_default_expansion else 0
        if use_default_expansion and default_open_depth == -1:
            expanded_all = True
        label_field = tree_config["labelField"] or "tree"
        detail_kind = detail_config["defaultKind"] or "records"
        detail_enabled = detail_config["enabled"]
        visible_rows: List[Dict[str, Any]] = []

        def visit(path: str, depth: int = 0) -> None:
            node = nodes[path]
            child_paths = children_by_parent.get(path, [])
            has_children = bool(child_paths or (has_children_paths and path in has_children_paths))
            is_expanded = bool(
                has_children
                and (
                    expanded_all
                    or path in expanded_set
                    or (use_default_expansion and default_open_depth > 0 and depth < default_open_depth)
                    or (use_default_expansion and path in default_open_paths)
                )
            )
            row = {
                **{k: v for k, v in node.items() if not str(k).startswith("_raw")},
                "_rowKey": path,
                "_parentKey": node.get("_parentPath") or None,
                "_path": path,
                "_pathFields": [label_field] * max(depth + 1, 1),
                "_depth": depth,
                "depth": depth,
                "_id": node.get("_label") or path.rsplit("|||", 1)[-1],
                "_has_children": has_children,
                "_is_expanded": is_expanded,
                "_can_detail": detail_enabled,
                "_detail_kind": detail_kind,
            }
            visible_rows.append(row)
            if is_expanded:
                for child_path in child_paths:
                    visit(child_path, depth + 1)

        for root_path in roots:
            visit(root_path, 0)
        return visible_rows

    def _build_columns(
        self,
        tree_config: Dict[str, Any],
        rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        column_ids: List[str] = []
        seen = set()

        for field_name in tree_config["valueFields"] + tree_config["extraFields"]:
            if field_name and field_name not in seen:
                seen.add(field_name)
                column_ids.append(field_name)

        if not column_ids and rows:
            ignore = {
                tree_config["idField"],
                tree_config["parentIdField"],
                tree_config["pathField"],
                tree_config["childrenField"],
                tree_config["labelField"],
                "_rowKey",
                "_parentKey",
                "_path",
                "_pathFields",
                "_depth",
                "depth",
                "_id",
                "_has_children",
                "_is_expanded",
                "_can_detail",
                "_detail_kind",
                "_label",
                "_nodePath",
                "_parentPath",
                "_sourceRow",
            }
            for key in rows[0].keys():
                if key not in ignore and not str(key).startswith("_"):
                    column_ids.append(key)

        return [{"id": field_name} for field_name in column_ids]

    def _resolve_tree_sort(
        self,
        state: PivotViewState,
        tree_config: Dict[str, Any],
    ) -> Tuple[str, bool]:
        if isinstance(state.sorting, list):
            for sort_spec in state.sorting:
                if not isinstance(sort_spec, dict):
                    continue
                sort_id = sort_spec.get("id")
                if sort_id:
                    if sort_id == "hierarchy":
                        return tree_config["labelField"], bool(sort_spec.get("desc"))
                    return str(sort_id), bool(sort_spec.get("desc"))
        sort_by = tree_config.get("sortBy") or tree_config["labelField"]
        return str(sort_by), bool(tree_config.get("sortDir") == "desc")

    def _build_sorter(self, nodes: Dict[str, Dict[str, Any]], sort_field: str, desc: bool):
        def key_func(path: str):
            node = nodes[path]
            if sort_field in {"hierarchy", "_id"}:
                value = node.get("_label")
            else:
                value = node.get(sort_field)
            return _sort_value(value)

        def sort_paths(paths: List[str]) -> List[str]:
            return sorted(paths, key=key_func, reverse=desc)

        return sort_paths

    @staticmethod
    def _normalize_detail_request_payload(
        detail_payload: Dict[str, Any],
        tree_config: Dict[str, Any],
        state: PivotViewState,
    ) -> Dict[str, Any]:
        detail_kind = str(
            detail_payload.get("detailKind")
            or detail_payload.get("detail_kind")
            or (state.detail_config or {}).get("defaultKind")
            or "records"
        ).strip().lower() or "records"
        row_path = _coalesce_text(detail_payload.get("rowPath") or detail_payload.get("row_path"))
        row_key = _coalesce_text(detail_payload.get("rowKey") or detail_payload.get("row_key") or row_path)
        page = max(0, int(detail_payload.get("page") or 0))
        page_size = max(1, int(detail_payload.get("pageSize") or detail_payload.get("page_size") or 100))
        return {
            "detailKind": detail_kind,
            "rowPath": row_path,
            "rowKey": row_key,
            "page": page,
            "pageSize": page_size,
            "sortCol": detail_payload.get("sortCol") or detail_payload.get("sort_col"),
            "sortDir": "desc" if str(detail_payload.get("sortDir") or detail_payload.get("sort_dir") or "asc").lower() == "desc" else "asc",
            "filterText": _coalesce_text(detail_payload.get("filterText") or detail_payload.get("filter")),
            "title": _coalesce_text(detail_payload.get("title"), row_path.rsplit("|||", 1)[-1] if row_path else "Detail"),
            "separator": tree_config["pathSeparator"],
        }

    def _collect_detail_rows(
        self,
        source_rows: List[Dict[str, Any]],
        tree_config: Dict[str, Any],
        detail_payload: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        source_type = tree_config["sourceType"]
        row_path = detail_payload["rowPath"]
        if source_type == "path":
            path_field = tree_config["pathField"]
            separator = tree_config["pathSeparator"]
            prefix = f"{row_path}{separator}" if row_path else ""
            rows = []
            for row in source_rows:
                raw_path = row.get(path_field)
                if raw_path is None:
                    continue
                if isinstance(raw_path, list):
                    normalized_path = _stringify_path(raw_path)
                else:
                    normalized_path = _stringify_path(str(raw_path).split(separator))
                if normalized_path == row_path or (prefix and normalized_path.startswith(prefix)):
                    rows.append(row)
            return rows

        if source_type == "adjacency":
            id_field = tree_config["idField"]
            parent_field = tree_config["parentIdField"]
            target_id = _normalize_node_key(detail_payload.get("rowKey") or row_path.rsplit("|||", 1)[-1] if row_path else None)
            if target_id is None:
                return []
            children_by_parent: Dict[str, List[Dict[str, Any]]] = {}
            row_lookup: Dict[str, Dict[str, Any]] = {}
            for row in source_rows:
                row_id = _normalize_node_key(row.get(id_field))
                if row_id is None:
                    continue
                row_lookup[row_id] = row
                parent_key = _normalize_node_key(row.get(parent_field))
                children_by_parent.setdefault(parent_key or "", []).append(row)

            collected: List[Dict[str, Any]] = []
            stack = [target_id]
            while stack:
                current_id = stack.pop(0)
                row = row_lookup.get(current_id)
                if row is not None:
                    collected.append(row)
                for child in children_by_parent.get(current_id, []):
                    child_id = _normalize_node_key(child.get(id_field))
                    if child_id is not None:
                        stack.append(child_id)
            return collected

        nodes = self._materialize_nested_nodes(source_rows, tree_config)
        prefix = f"{row_path}|||" if row_path else ""
        return [
            node.get("_sourceRow")
            for path, node in nodes.items()
            if path == row_path or (prefix and path.startswith(prefix))
        ]

    @staticmethod
    def _apply_text_filter(rows: List[Dict[str, Any]], text: str) -> List[Dict[str, Any]]:
        if not text:
            return rows
        lowered = text.lower()
        return [
            row for row in rows
            if any(lowered in str(value).lower() for value in row.values())
        ]

    @staticmethod
    def _apply_row_sort(rows: List[Dict[str, Any]], sort_col: Optional[str], sort_dir: str) -> List[Dict[str, Any]]:
        if not sort_col:
            return rows
        return sorted(
            rows,
            key=lambda row: _sort_value(row.get(sort_col)),
            reverse=sort_dir == "desc",
        )

    @staticmethod
    def _build_detail_columns(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        ignore = {key for key in rows[0].keys() if str(key).startswith("_")}
        return [{"id": key} for key in rows[0].keys() if key not in ignore]
