"""TanStack editing-transaction boundary.

This mixin owns transport normalization, patch refresh construction, transaction
execution, and history metadata.  It intentionally depends on the adapter's
stable query/viewport ports instead of importing the adapter facade.
"""

from __future__ import annotations

import copy
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from ..transport.contracts import TanStackRequest, TanStackResponse
from ..types.pivot_spec import PivotSpec


class TransactionAdapterMixin:
    """Editing transaction capability mixed into the compatibility adapter."""

    def _resolve_request_target_column(
        self, request: TanStackRequest, column_id: Any
    ) -> Optional[str]:
        column_spec = self._resolve_request_column_spec(request, column_id)
        if not isinstance(column_spec, dict):
            return None
        target_column = column_spec.get("targetColumn")
        return str(target_column).strip() if target_column else None

    def _normalize_table_row_data(
        self, request: TanStackRequest, row_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        if not isinstance(row_data, dict):
            return normalized
        for column_id, value in row_data.items():
            raw_column_id = str(column_id or "").strip()
            if not raw_column_id or raw_column_id.startswith("_"):
                continue
            target_column = self._resolve_request_target_column(request, raw_column_id)
            if not target_column or target_column.startswith("_"):
                continue
            normalized[target_column] = value
        return normalized

    @staticmethod
    def _normalize_transaction_refresh_mode(value: Any) -> str:
        normalized = str(value or "smart").strip().lower()
        if normalized in {"none", "viewport", "smart", "structural", "full", "patch"}:
            return normalized
        return "smart"

    @staticmethod
    def _normalize_visible_patch_row_paths(
        transaction_payload: Dict[str, Any],
    ) -> List[str]:
        raw_paths = (
            transaction_payload.get("visibleRowPaths")
            or transaction_payload.get("visible_row_paths")
            or transaction_payload.get("visiblePaths")
            or transaction_payload.get("visible_paths")
        )
        normalized: List[str] = []
        seen: set[str] = set()
        for value in (raw_paths if isinstance(raw_paths, list) else []):
            if value is None:
                continue
            path = str(value).strip()
            if not path or path in seen:
                continue
            seen.add(path)
            normalized.append(path)
        return normalized

    @staticmethod
    def _normalize_visible_patch_center_ids(
        transaction_payload: Dict[str, Any],
    ) -> List[str]:
        raw_ids = (
            transaction_payload.get("visibleCenterColumnIds")
            or transaction_payload.get("visible_center_column_ids")
            or transaction_payload.get("visibleColumnIds")
            or transaction_payload.get("visible_column_ids")
        )
        normalized: List[str] = []
        seen: set[str] = set()
        for value in (raw_ids if isinstance(raw_ids, list) else []):
            column_id = str(value or "").strip()
            if not column_id or column_id == "__col_schema" or column_id in seen:
                continue
            seen.add(column_id)
            normalized.append(column_id)
        return normalized

    @staticmethod
    def _extract_patch_entry_path(
        request: TanStackRequest,
        entry: Dict[str, Any],
    ) -> Optional[str]:
        if not isinstance(entry, dict):
            return None
        raw_path = entry.get("rowPath") or entry.get("row_path") or entry.get("path")
        if raw_path is None:
            raw_path = entry.get("rowId") or entry.get("row_id")
        if raw_path is None and isinstance(entry.get("keys"), dict):
            parts = []
            for field in request.grouping or []:
                if field not in entry["keys"]:
                    parts = []
                    break
                parts.append(str(entry["keys"][field]))
            if parts:
                raw_path = "|||".join(parts)
        if raw_path is None:
            return None
        normalized_parts = [part for part in str(raw_path).split("|||") if part != ""]
        if not normalized_parts:
            return None
        return "|||".join(normalized_parts)

    def _resolve_transaction_patch_row_paths(
        self,
        request: TanStackRequest,
        transaction_payload: Dict[str, Any],
        visible_row_paths: List[str],
    ) -> List[str]:
        if not visible_row_paths:
            return []
        visible_order = [
            str(path) for path in visible_row_paths if str(path or "").strip()
        ]
        normalized_visible = [
            path for path in visible_order if path != "__grand_total__"
        ]
        grouping_fields = list(request.grouping or [])
        if not grouping_fields:
            return visible_order

        update_entries = self._extract_transaction_entries(
            transaction_payload, "update"
        )
        if not update_entries:
            return visible_order

        affected_paths: set[str] = set()
        include_grand_total = "__grand_total__" in visible_order

        for entry in update_entries:
            entry_path = self._extract_patch_entry_path(request, entry)
            if not entry_path:
                return visible_order
            if entry_path == "__grand_total__":
                return visible_order

            path_parts = [part for part in entry_path.split("|||") if part != ""]
            if not path_parts or len(path_parts) > len(grouping_fields):
                return visible_order

            is_group_path = len(path_parts) < len(grouping_fields)
            for visible_path in normalized_visible:
                if visible_path == entry_path or entry_path.startswith(
                    f"{visible_path}|||"
                ):
                    affected_paths.add(visible_path)
                    continue
                if is_group_path and visible_path.startswith(f"{entry_path}|||"):
                    affected_paths.add(visible_path)

            if include_grand_total:
                affected_paths.add("__grand_total__")

        if not affected_paths:
            return visible_order
        return [path for path in visible_order if path in affected_paths]

    def _transaction_requires_deferred_viewport_refresh(
        self,
        request: TanStackRequest,
        transaction_payload: Dict[str, Any],
        visible_row_paths: List[str],
        patch_row_paths: List[str],
    ) -> bool:
        if not visible_row_paths or not patch_row_paths:
            return False
        grouping_fields = list(request.grouping or [])
        if not grouping_fields:
            return False
        patch_path_set = set(str(path) for path in patch_row_paths)
        normalized_visible = [
            str(path)
            for path in visible_row_paths
            if str(path or "").strip() and str(path) != "__grand_total__"
        ]
        for entry in self._extract_transaction_entries(transaction_payload, "update"):
            entry_path = self._extract_patch_entry_path(request, entry)
            if not entry_path or entry_path == "__grand_total__":
                return False
            path_parts = [part for part in entry_path.split("|||") if part != ""]
            if len(path_parts) >= len(grouping_fields):
                continue
            for visible_path in normalized_visible:
                if (
                    visible_path.startswith(f"{entry_path}|||")
                    and visible_path not in patch_path_set
                ):
                    return True
        return False

    @staticmethod
    def _request_supports_patch_refresh(request: TanStackRequest) -> bool:
        if not isinstance(request, TanStackRequest):
            return False
        if not request.grouping:
            return False
        for column in request.columns or []:
            if not isinstance(column, dict):
                continue
            if column.get("isFormula") or column.get("windowFn"):
                return False
        return True

    @staticmethod
    def _build_path_filter_block(
        grouping_fields: List[str], requested_paths: List[List[str]]
    ) -> Optional[Dict[str, Any]]:
        if not grouping_fields or not requested_paths:
            return None
        if len(grouping_fields) == 1:
            return {
                "field": grouping_fields[0],
                "op": "in",
                "value": [path[0] for path in requested_paths if path],
            }

        tuple_conditions: List[Dict[str, Any]] = []
        for path in requested_paths:
            if len(path) != len(grouping_fields):
                continue
            conditions = [
                {"field": grouping_fields[index], "op": "=", "value": path[index]}
                for index in range(len(grouping_fields))
            ]
            if len(conditions) == 1:
                tuple_conditions.append(conditions[0])
            elif conditions:
                tuple_conditions.append({"op": "AND", "conditions": conditions})
        if not tuple_conditions:
            return None
        if len(tuple_conditions) == 1:
            return tuple_conditions[0]
        return {"op": "OR", "conditions": tuple_conditions}

    async def _execute_patch_row_group_query(
        self,
        request: TanStackRequest,
        spec: PivotSpec,
        *,
        depth: int,
        requested_center_ids: Optional[List[str]],
    ) -> TanStackResponse:
        pivot_result = await self.controller.run_pivot_async(
            spec, return_format="dict", force_refresh=True
        )
        pivot_columns = list((pivot_result or {}).get("columns") or [])
        pivot_rows = []
        for raw_row in (pivot_result or {}).get("rows") or []:
            if isinstance(raw_row, dict):
                row_out = dict(raw_row)
            else:
                row_out = {
                    pivot_columns[index]: (
                        raw_row[index] if index < len(raw_row) else None
                    )
                    for index in range(len(pivot_columns))
                }
            row_out["depth"] = max(depth - 1, 0)
            pivot_rows.append(row_out)
        converted = self.convert_pivot_result_to_tanstack_format(
            {"columns": pivot_columns, "rows": pivot_rows},
            request,
            version=request.version,
        )
        return self._apply_col_windowing(
            converted,
            request,
            0,
            None,
            False,
            requested_center_ids=requested_center_ids,
        )

    async def _build_transaction_patch_payload(
        self,
        request: TanStackRequest,
        transaction_payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        visible_row_paths = self._normalize_visible_patch_row_paths(transaction_payload)
        if not visible_row_paths or not self._request_supports_patch_refresh(request):
            return None
        targeted_row_paths = self._resolve_transaction_patch_row_paths(
            request,
            transaction_payload,
            visible_row_paths,
        )
        if not targeted_row_paths:
            return None

        requested_center_ids = self._normalize_visible_patch_center_ids(
            transaction_payload
        )
        base_spec = self.convert_tanstack_request_to_pivot_spec(request)
        base_spec.limit = None
        base_spec.sort = []
        base_spec.totals = False

        if base_spec.columns:
            if not requested_center_ids:
                return None
            materialized_values = self._materialized_pivot_values_for_window(
                requested_center_ids, request
            )
            if not materialized_values:
                return None
            base_spec = copy.deepcopy(base_spec)
            if base_spec.pivot_config:
                base_spec.pivot_config.materialized_column_values = materialized_values

        grouped_paths: "OrderedDict[int, List[List[str]]]" = OrderedDict()
        include_grand_total = False
        for row_path in targeted_row_paths:
            if row_path == "__grand_total__":
                include_grand_total = True
                continue
            path_parts = [part for part in str(row_path).split("|||") if part != ""]
            if not path_parts:
                continue
            depth = len(path_parts)
            if depth > len(request.grouping):
                continue
            grouped_paths.setdefault(depth, []).append(path_parts)

        patch_rows_by_path: Dict[str, Dict[str, Any]] = {}

        if include_grand_total:
            grand_total_spec = copy.deepcopy(base_spec)
            grand_total_spec.rows = []
            grand_total_spec.filters = list(copy.deepcopy(base_spec.filters or []))
            grand_total_response = await self._execute_patch_row_group_query(
                request,
                grand_total_spec,
                depth=0,
                requested_center_ids=requested_center_ids,
            )
            if grand_total_response.data:
                grand_total_row = dict(grand_total_response.data[0])
                grand_total_row["_id"] = "Grand Total"
                grand_total_row["_isTotal"] = True
                grand_total_row["_path"] = "__grand_total__"
                grand_total_row["depth"] = 0
                patch_rows_by_path["__grand_total__"] = grand_total_row

        for depth, requested_paths in grouped_paths.items():
            group_spec = copy.deepcopy(base_spec)
            group_spec.rows = list(request.grouping[:depth])
            group_spec.filters = list(copy.deepcopy(base_spec.filters or []))
            path_filter = self._build_path_filter_block(
                group_spec.rows, requested_paths
            )
            if path_filter:
                group_spec.filters.append(path_filter)
            group_response = await self._execute_patch_row_group_query(
                request,
                group_spec,
                depth=depth,
                requested_center_ids=requested_center_ids,
            )
            for row in group_response.data or []:
                if not isinstance(row, dict):
                    continue
                row_path = str(row.get("_path") or "").strip()
                if not row_path:
                    continue
                patch_rows_by_path[row_path] = dict(row)

        ordered_rows = [
            patch_rows_by_path[row_path]
            for row_path in targeted_row_paths
            if row_path in patch_rows_by_path
        ]
        if not ordered_rows:
            return None

        return {
            "mode": "visible_rows",
            "rows": ordered_rows,
            "requestedRowPaths": list(targeted_row_paths),
            "requestedCenterColumnIds": list(requested_center_ids),
            "deferredViewportRefresh": self._transaction_requires_deferred_viewport_refresh(
                request,
                transaction_payload,
                visible_row_paths,
                targeted_row_paths,
            ),
        }

    def _resolve_transaction_key_fields(
        self,
        request: TanStackRequest,
        transaction_payload: Dict[str, Any],
    ) -> List[str]:
        raw_key_fields = (
            transaction_payload.get("keyFields")
            or transaction_payload.get("key_fields")
            or transaction_payload.get("keyField")
            or transaction_payload.get("key_field")
        )
        if isinstance(raw_key_fields, str):
            raw_key_fields = [raw_key_fields]
        if isinstance(raw_key_fields, list):
            normalized = []
            for field in raw_key_fields:
                field_name = self._resolve_request_target_column(request, field)
                if field_name and field_name not in normalized:
                    normalized.append(field_name)
            if normalized:
                return normalized

        tree_config = transaction_payload.get("treeConfig")
        if isinstance(tree_config, dict):
            id_field = tree_config.get("idField") or tree_config.get("id_field")
            resolved_id_field = self._resolve_request_target_column(request, id_field)
            if resolved_id_field:
                return [resolved_id_field]

        normalized_grouping = []
        for field in request.grouping or []:
            field_name = self._resolve_request_target_column(request, field)
            if field_name and field_name not in normalized_grouping:
                normalized_grouping.append(field_name)
        return normalized_grouping

    @staticmethod
    def _extract_transaction_entries(
        transaction_payload: Dict[str, Any], operation_kind: str
    ) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        direct = transaction_payload.get(operation_kind)
        if isinstance(direct, dict):
            entries.append(direct)
        elif isinstance(direct, list):
            entries.extend([entry for entry in direct if isinstance(entry, dict)])

        alias_map = {
            "add": "insert",
            "remove": "delete",
            "update": "updates",
            "upsert": "merge",
        }
        alias_value = transaction_payload.get(alias_map.get(operation_kind, ""))
        if isinstance(alias_value, dict):
            entries.append(alias_value)
        elif isinstance(alias_value, list):
            entries.extend([entry for entry in alias_value if isinstance(entry, dict)])

        operations = transaction_payload.get("operations")
        if isinstance(operations, list):
            entries.extend(
                entry
                for entry in operations
                if isinstance(entry, dict)
                and str(entry.get("kind") or "").strip().lower() == operation_kind
            )

        return entries

    def _extract_transaction_row_data(
        self, request: TanStackRequest, entry: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not isinstance(entry, dict):
            return {}
        row_source = entry.get("rowData")
        if not isinstance(row_source, dict):
            row_source = entry.get("data")
        if not isinstance(row_source, dict):
            row_source = {
                key: value
                for key, value in entry.items()
                if key
                not in {
                    "kind",
                    "keys",
                    "keyColumns",
                    "key_columns",
                    "values",
                    "updates",
                    "rowId",
                    "row_id",
                }
            }
        return self._normalize_table_row_data(request, row_source)

    def _extract_transaction_key_columns(
        self,
        request: TanStackRequest,
        entry: Dict[str, Any],
        row_data: Dict[str, Any],
        key_fields: List[str],
    ) -> Dict[str, Any]:
        exact_keys = bool(entry.get("exactKeys") or entry.get("exact_keys"))
        explicit_keys = entry.get("keys")
        if not isinstance(explicit_keys, dict):
            explicit_keys = entry.get("keyColumns")
        if not isinstance(explicit_keys, dict):
            explicit_keys = entry.get("key_columns")
        if isinstance(explicit_keys, dict):
            normalized_keys = self._normalize_table_row_data(request, explicit_keys)
            if exact_keys:
                return normalized_keys
            if key_fields:
                normalized_keys = {
                    field: normalized_keys[field]
                    for field in key_fields
                    if field in normalized_keys
                }
            if not key_fields or len(normalized_keys) == len(key_fields):
                return normalized_keys
            return {}

        row_id = entry.get("rowId") or entry.get("row_id")
        if row_id is not None and key_fields:
            parts = str(row_id).split("|||")
            if len(parts) < len(key_fields):
                return {}
            return {field: parts[index] for index, field in enumerate(key_fields)}

        if key_fields:
            candidate = {
                field: row_data[field] for field in key_fields if field in row_data
            }
            if len(candidate) == len(key_fields):
                return candidate

        return {}

    def _normalize_update_operation(
        self,
        request: TanStackRequest,
        update_payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        table_name = request.table
        row_id = update_payload.get("rowId") or update_payload.get("row_id")
        col_id = update_payload.get("colId") or update_payload.get("col_id")
        new_value = update_payload.get("value")

        if not table_name or row_id is None or not col_id:
            return None

        hierarchy_cols = request.grouping or []
        key_columns = {}

        if hierarchy_cols:
            parts = str(row_id).split("|||")
            for i, col in enumerate(hierarchy_cols):
                if i < len(parts):
                    key_columns[col] = parts[i]

        column_spec = self._resolve_request_column_spec(request, col_id)
        if not column_spec:
            return None

        pivot_filters = (
            column_spec.get("pivotFilters")
            if isinstance(column_spec.get("pivotFilters"), dict)
            else {}
        )
        if pivot_filters:
            key_columns.update(pivot_filters)

        if not key_columns:
            return {
                "warning": "Skipped update entry without resolvable row or pivot keys.",
            }

        target_col = column_spec.get("targetColumn")
        if not target_col:
            return None

        if not column_spec.get("editable", True):
            reason = column_spec.get("reason") or f"Column '{col_id}' is not editable."
            return {"warning": reason}

        aggregation_fn = self._normalize_aggregation_name(
            column_spec.get("aggregationFn")
        )
        if aggregation_fn:
            return {
                "key_columns": key_columns,
                "edit_meta": {
                    "rowId": str(row_id),
                    "rowPath": str(update_payload.get("rowPath") or row_id),
                    "colId": str(col_id),
                    "groupingFields": list(request.grouping or []),
                },
                "aggregate_edit": {
                    "column": target_col,
                    "rowId": str(row_id),
                    "rowPath": str(update_payload.get("rowPath") or row_id),
                    "columnId": str(col_id),
                    "aggregationFn": aggregation_fn,
                    "weightField": column_spec.get("weightField"),
                    "windowFn": column_spec.get("windowFn"),
                    "oldValue": update_payload.get("oldValue"),
                    "newValue": new_value,
                    "propagationStrategy": (
                        update_payload.get("propagationStrategy")
                        or update_payload.get("propagation_strategy")
                        or update_payload.get("propagationFormula")
                        or update_payload.get("propagation_formula")
                    ),
                    "pivotFilters": pivot_filters,
                },
            }

        return {
            "key_columns": key_columns,
            "edit_meta": {
                "rowId": str(row_id),
                "rowPath": str(update_payload.get("rowPath") or row_id),
                "colId": str(col_id),
                "groupingFields": list(request.grouping or []),
            },
            "updates": {target_col: new_value},
        }

    async def handle_transaction(
        self, request: TanStackRequest, transaction_payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply a richer-than-AG-Grid row transaction with smart refresh semantics."""
        payload = transaction_payload if isinstance(transaction_payload, dict) else {}
        key_fields = self._resolve_transaction_key_fields(request, payload)
        refresh_mode = self._normalize_transaction_refresh_mode(
            payload.get("refreshMode") if isinstance(payload, dict) else None
        )
        warnings: List[str] = []
        zero_applied_counts = {
            "add": 0,
            "remove": 0,
            "update": 0,
            "upsertUpdated": 0,
            "upsertInserted": 0,
        }

        normalized_transaction = {
            "add": [],
            "remove": [],
            "update": [],
            "upsert": [],
        }

        for entry in self._extract_transaction_entries(payload, "add"):
            row_data = self._extract_transaction_row_data(request, entry)
            if not row_data:
                warnings.append("Skipped add entry without valid row data.")
                continue
            key_columns = self._extract_transaction_key_columns(
                request, entry, row_data, key_fields
            )
            normalized_transaction["add"].append(
                {
                    "row_data": row_data,
                    "key_columns": key_columns,
                }
            )

        for entry in self._extract_transaction_entries(payload, "remove"):
            row_data = self._extract_transaction_row_data(request, entry)
            key_columns = self._extract_transaction_key_columns(
                request, entry, row_data, key_fields
            )
            if not key_columns:
                warnings.append("Skipped remove entry without resolvable key columns.")
                continue
            normalized_transaction["remove"].append({"key_columns": key_columns})

        update_entries = self._extract_transaction_entries(payload, "update")
        if not update_entries and isinstance(payload.get("updates"), list):
            update_entries = [
                entry for entry in payload.get("updates") if isinstance(entry, dict)
            ]
        for entry in update_entries:
            operation = None
            if entry.get("rowId") is not None and entry.get("colId") is not None:
                operation = self._normalize_update_operation(request, entry)
            else:
                row_data = self._extract_transaction_row_data(request, entry)
                key_columns = self._extract_transaction_key_columns(
                    request, entry, row_data, key_fields
                )
                values_source = (
                    entry.get("values")
                    if isinstance(entry.get("values"), dict)
                    else (
                        entry.get("updates")
                        if isinstance(entry.get("updates"), dict)
                        else row_data
                    )
                )
                update_values = self._normalize_table_row_data(request, values_source)
                for field in list(key_columns.keys()):
                    update_values.pop(field, None)
                if key_columns and update_values:
                    operation = {
                        "key_columns": key_columns,
                        "updates": update_values,
                    }
            if isinstance(operation, dict) and operation.get("warning"):
                warnings.append(str(operation.get("warning")))
                continue
            if not operation:
                warnings.append(
                    "Skipped update entry without resolvable keys or values."
                )
                continue
            normalized_transaction["update"].append(operation)

        if normalized_transaction["update"]:
            merged_updates: OrderedDict[str, Dict[str, Any]] = OrderedDict()
            passthrough_updates: List[Dict[str, Any]] = []
            for operation in normalized_transaction["update"]:
                if isinstance(operation.get("aggregate_edit"), dict):
                    passthrough_updates.append(operation)
                    continue
                operation_key = self._stable_json(operation["key_columns"])
                existing_operation = merged_updates.get(operation_key)
                if existing_operation is None:
                    merged_updates[operation_key] = {
                        "key_columns": dict(operation["key_columns"]),
                        "updates": dict(operation["updates"]),
                    }
                else:
                    existing_operation["updates"].update(operation["updates"])
            normalized_transaction["update"] = passthrough_updates + list(
                merged_updates.values()
            )

        for entry in self._extract_transaction_entries(payload, "upsert"):
            row_data = self._extract_transaction_row_data(request, entry)
            key_columns = self._extract_transaction_key_columns(
                request, entry, row_data, key_fields
            )
            merged_row = {**key_columns, **row_data}
            if not key_columns or not merged_row:
                warnings.append(
                    "Skipped upsert entry without resolvable keys or row data."
                )
                continue
            normalized_transaction["upsert"].append(
                {
                    "key_columns": key_columns,
                    "row_data": merged_row,
                }
            )

        prepared_event_action = self.edit_domain.prepare_event_action(
            request, payload, normalized_transaction
        )

        if prepared_event_action is not None:
            normalized_transaction = (
                prepared_event_action.normalized_transaction or normalized_transaction
            )

        requested_counts = {
            kind: len(normalized_transaction.get(kind) or [])
            for kind in ("add", "remove", "update", "upsert")
            if len(normalized_transaction.get(kind) or []) > 0
        }

        if prepared_event_action is None:
            validation = self.edit_domain.validate_transaction(
                request, payload, normalized_transaction
            )
        else:
            validation = self.edit_domain.validate_prepared_event_action(
                request, payload, prepared_event_action
            )
            if not any(requested_counts.values()) and not list(
                validation.get("warnings") or []
            ):
                warnings.append(
                    "Requested event action could not be prepared from the current edit session."
                )
        warnings.extend(list(validation.get("warnings") or []))
        if validation.get("conflicts"):
            response = {
                "kind": "transaction",
                "keyFields": key_fields,
                "requested": requested_counts,
                "applied": dict(zero_applied_counts),
                "warnings": warnings,
                "rowCountDelta": 0,
                "refreshMode": refresh_mode,
                "requiresStructuralRefresh": False,
                "source": payload.get("source"),
                "propagation": [],
                "patchPayload": None,
                "deferredViewportRefresh": False,
                "inverseTransaction": None,
                "redoTransaction": None,
                "history": {
                    "captureable": False,
                    "warnings": [],
                },
                "conflicts": list(validation.get("conflicts") or []),
            }
            return self.edit_domain.enrich_transaction_result(
                request,
                payload,
                normalized_transaction,
                response,
                prepared_event_action=prepared_event_action,
            )

        if warnings and not any(requested_counts.values()):
            response = {
                "kind": "transaction",
                "keyFields": key_fields,
                "requested": requested_counts,
                "applied": dict(zero_applied_counts),
                "warnings": warnings,
                "rowCountDelta": 0,
                "refreshMode": refresh_mode,
                "requiresStructuralRefresh": False,
                "source": payload.get("source"),
                "propagation": [],
                "patchPayload": None,
                "deferredViewportRefresh": False,
                "inverseTransaction": None,
                "redoTransaction": None,
                "history": {
                    "captureable": False,
                    "warnings": [],
                },
            }
            return self.edit_domain.enrich_transaction_result(
                request,
                payload,
                normalized_transaction,
                response,
                prepared_event_action=prepared_event_action,
            )

        apply_result = {
            "requested": {},
            "applied": dict(zero_applied_counts),
            "warnings": [],
            "rowCountDelta": 0,
        }
        if hasattr(self.controller, "apply_row_transaction"):
            apply_result = await self.controller.apply_row_transaction(
                request.table, normalized_transaction
            )
        elif normalized_transaction["update"]:
            updated_count = await self.controller.update_records(
                request.table, normalized_transaction["update"]
            )
            apply_result = {
                "requested": {"update": len(normalized_transaction["update"])},
                "applied": {"update": updated_count},
                "warnings": [],
                "rowCountDelta": 0,
            }

        raw_applied = (
            apply_result.get("applied")
            if isinstance(apply_result.get("applied"), dict)
            else {}
        )
        applied = {
            **zero_applied_counts,
            **raw_applied,
        }
        requested = (
            apply_result.get("requested")
            if isinstance(apply_result.get("requested"), dict)
            else {}
        )
        requires_structural_refresh = bool(
            (applied.get("add") or 0)
            or (applied.get("remove") or 0)
            or (applied.get("upsertInserted") or 0)
            or refresh_mode in {"structural", "full"}
        )
        resolved_refresh_mode = refresh_mode
        if refresh_mode == "smart":
            resolved_refresh_mode = (
                "structural" if requires_structural_refresh else "viewport"
            )
        elif refresh_mode == "patch":
            resolved_refresh_mode = (
                "structural" if requires_structural_refresh else "patch"
            )
        elif refresh_mode == "full":
            resolved_refresh_mode = "structural"

        if any(applied.values()):
            self._invalidate_local_caches()

        patch_payload = None
        if resolved_refresh_mode == "patch" and any(applied.values()):
            patch_payload = await self._build_transaction_patch_payload(
                request, payload
            )
            if patch_payload is None:
                resolved_refresh_mode = "viewport"

        def enrich_history_transaction(transaction: Any) -> Optional[Dict[str, Any]]:
            if not isinstance(transaction, dict):
                return None
            enriched = {
                key: list(value or [])
                for key, value in transaction.items()
                if isinstance(value, list) and value
            }
            if not enriched:
                return None
            enriched["keyFields"] = list(key_fields or [])
            enriched["refreshMode"] = (
                resolved_refresh_mode if resolved_refresh_mode != "none" else "smart"
            )
            return enriched

        history_result = (
            apply_result.get("history")
            if isinstance(apply_result.get("history"), dict)
            else {}
        )

        response = {
            "kind": "transaction",
            "keyFields": key_fields,
            "requested": requested,
            "applied": applied,
            "warnings": warnings + list(apply_result.get("warnings") or []),
            "rowCountDelta": int(apply_result.get("rowCountDelta") or 0),
            "refreshMode": resolved_refresh_mode,
            "requiresStructuralRefresh": requires_structural_refresh,
            "source": payload.get("source"),
            "propagation": list(apply_result.get("propagation") or []),
            "scopeValueChanges": list(apply_result.get("scopeValueChanges") or []),
            "patchPayload": patch_payload,
            "deferredViewportRefresh": bool(
                (patch_payload or {}).get("deferredViewportRefresh")
            ),
            "inverseTransaction": enrich_history_transaction(
                apply_result.get("inverseTransaction")
            ),
            "redoTransaction": enrich_history_transaction(
                apply_result.get("redoTransaction")
            ),
            "history": {
                "captureable": bool(history_result.get("captureable")),
                "warnings": list(history_result.get("warnings") or []),
            },
        }
        if prepared_event_action is not None and prepared_event_action.action in {
            "undo",
            "redo",
            "revert",
        }:
            response["history"] = {
                "captureable": False,
                "warnings": list(history_result.get("warnings") or []),
            }
        if prepared_event_action is not None:
            response = self.edit_domain.finalize_event_action(
                request, payload, prepared_event_action, response
            )
        return self.edit_domain.enrich_transaction_result(
            request,
            payload,
            normalized_transaction,
            response,
            prepared_event_action=prepared_event_action,
        )

    async def handle_updates(
        self, request: TanStackRequest, update_payloads: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Apply one or more cell updates and invalidate adapter caches."""
        transaction_result = await self.handle_transaction(
            request,
            {
                "update": update_payloads,
                "refreshMode": "viewport",
                "source": "cell_updates",
            },
        )
        updated_count = int(
            ((transaction_result.get("applied") or {}).get("update")) or 0
        )
        if updated_count <= 0:
            return {"updated": 0}
        return {
            "updated": updated_count,
            "transaction": transaction_result,
        }
