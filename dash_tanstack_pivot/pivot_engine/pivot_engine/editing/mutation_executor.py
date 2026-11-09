"""Capability-aware SQL mutation and transaction executor for pivot controllers."""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from contextlib import nullcontext
import decimal, json, math, re
from typing import Any, Callable, Dict, List, Optional


class DatabaseMutationExecutor:
    """Executes row/cell mutations and aggregate edits against an Ibis/SQL backend."""

    def __init__(self, controller: Any):
        self.controller = controller

    def _capabilities(self) -> Any:
        return getattr(self.controller, "backend_capabilities", None)

    def _ensure_mutations_supported(self) -> None:
        caps = self._capabilities()
        if caps is not None and not getattr(caps, "supports_sql_mutations", True):
            raise NotImplementedError(
                f"SQL mutations are disabled or unsupported for backend '{getattr(caps, 'backend_name', 'unknown')}'. "
                "Set PIVOT_ALLOW_REMOTE_MUTATIONS=1 (or allow_remote_mutations=True) to enable remote write-back."
            )

    def _supports_rowid(self) -> bool:
        caps = self._capabilities()
        return bool(getattr(caps, "supports_rowid", False)) if caps is not None else True

    def _placeholder(self) -> str:
        caps = self._capabilities()
        style = getattr(caps, "param_style", "qmark") if caps is not None else "qmark"
        return "%s" if style == "pyformat" else "?"

    def _adapt_sql_placeholders(self, sql: str) -> str:
        ph = self._placeholder()
        return sql if ph == "?" else sql.replace("?", ph)

    @staticmethod
    def _stable_json(value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=str)

    @staticmethod
    def _sanitize_column_mapping(
        source: Optional[Dict[str, Any]],
        *,
        allowed_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        if not isinstance(source, dict):
            return {}
        allowed = set(allowed_columns or [])
        excluded = set(exclude_columns or [])
        sanitized: Dict[str, Any] = {}
        for column, value in source.items():
            column_name = str(column or "")
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", column_name):
                continue
            if allowed and column_name not in allowed:
                continue
            if column_name in excluded:
                continue
            sanitized[column_name] = value
        return sanitized

    @staticmethod
    def _build_where_clause(key_columns: Dict[str, Any], placeholder: str = "?") -> tuple[str, List[Any]]:
        where_parts = []
        params: List[Any] = []
        for col, val in key_columns.items():
            if val is None:
                where_parts.append(f"{col} IS NULL")
            elif isinstance(val, (list, tuple, set, frozenset)):
                values = list(val)
                if not values:
                    where_parts.append("1 = 0")
                elif len(values) == 1:
                    where_parts.append(f"{col} = {placeholder}")
                    params.append(values[0])
                else:
                    where_parts.append(
                        f"{col} IN ({', '.join([placeholder] * len(values))})"
                    )
                    params.extend(values)
            else:
                where_parts.append(f"{col} = {placeholder}")
                params.append(val)
        return " AND ".join(where_parts), params

    def _get_table_columns(self, table_name: str) -> List[str]:
        return self.controller._get_table_columns(table_name)

    def _count_matching_rows_sync(self, table_name: str, key_columns: Dict[str, Any]) -> int:
        return self.controller._count_matching_rows_sync(table_name, key_columns)

    def _fetch_matching_rows_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        table_columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        return self.controller._fetch_matching_rows_sync(table_name, key_columns, table_columns=table_columns)

    def _fetch_matching_rows_with_rowid_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        table_columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not self._supports_rowid():
            rows = self._fetch_matching_rows_sync(table_name, key_columns, table_columns=table_columns)
            return [
                {"__rowid__": index, **(row if isinstance(row, dict) else {})}
                for index, row in enumerate(rows)
            ]
        selected_columns = [
            str(column)
            for column in (table_columns or self._get_table_columns(table_name) or [])
            if isinstance(column, str) and column and column != "__rowid__"
        ]
        select_list = ", ".join(["rowid AS __rowid__", *selected_columns]) if selected_columns else "rowid AS __rowid__"
        where_sql, where_params = self.controller._build_where_clause(key_columns)
        rows = self.controller._execute_parameterized_fetchall(
            f"SELECT {select_list} FROM {table_name} WHERE {where_sql}",  # nosec B608
            where_params,
        )
        records: List[Dict[str, Any]] = []
        expected_columns = ["__rowid__", *selected_columns]
        for row in rows:
            if isinstance(row, dict):
                records.append({
                    key: row.get(key)
                    for key in expected_columns
                    if key in row
                })
                continue
            if isinstance(row, (list, tuple)):
                records.append({
                    expected_columns[index]: row[index]
                    for index in range(min(len(expected_columns), len(row)))
                })
        return records

    @staticmethod
    def _resolve_grouping_scope_paths_for_row(row: Dict[str, Any], grouping_fields: List[str]) -> List[str]:
        if not isinstance(row, dict) or not grouping_fields:
            return []
        parts: List[str] = []
        scope_paths: List[str] = []
        for field in grouping_fields:
            if field not in row or row.get(field) is None:
                break
            parts.append(str(row.get(field)))
            scope_paths.append("|||".join(parts))
        return scope_paths

    @staticmethod
    def _new_scope_value_accumulator() -> Dict[str, Any]:
        return {
            "before_sum": 0.0,
            "after_sum": 0.0,
            "before_count": 0,
            "after_count": 0,
            "before_weighted_sum": 0.0,
            "after_weighted_sum": 0.0,
            "before_weight_total": 0.0,
            "after_weight_total": 0.0,
            "before_min": None,
            "after_min": None,
            "before_max": None,
            "after_max": None,
        }

    def _accumulate_scope_value(
        self,
        accumulator: Dict[str, Any],
        row: Dict[str, Any],
        *,
        target_column: str,
        aggregation_fn: str,
        weight_field: Optional[str],
        side: str,
    ) -> None:
        numeric_value = self._coerce_numeric_value(row.get(target_column) if isinstance(row, dict) else None)
        if numeric_value is None:
            return
        if aggregation_fn == "sum":
            accumulator[f"{side}_sum"] += numeric_value
            return
        if aggregation_fn == "avg":
            accumulator[f"{side}_sum"] += numeric_value
            accumulator[f"{side}_count"] += 1
            return
        if aggregation_fn in {"weighted_avg", "wavg", "weighted_mean"}:
            weight_value = self._coerce_numeric_value(row.get(weight_field) if isinstance(row, dict) else None)
            if weight_value is None:
                return
            accumulator[f"{side}_weighted_sum"] += numeric_value * weight_value
            accumulator[f"{side}_weight_total"] += weight_value
            return
        if aggregation_fn == "min":
            current_value = accumulator[f"{side}_min"]
            accumulator[f"{side}_min"] = numeric_value if current_value is None else min(current_value, numeric_value)
            return
        if aggregation_fn == "max":
            current_value = accumulator[f"{side}_max"]
            accumulator[f"{side}_max"] = numeric_value if current_value is None else max(current_value, numeric_value)

    @staticmethod
    def _finalize_scope_value_accumulator(accumulator: Dict[str, Any], aggregation_fn: str, side: str) -> Optional[float]:
        if aggregation_fn == "sum":
            return float(accumulator[f"{side}_sum"])
        if aggregation_fn == "avg":
            count = int(accumulator[f"{side}_count"] or 0)
            return (float(accumulator[f"{side}_sum"]) / count) if count > 0 else None
        if aggregation_fn in {"weighted_avg", "wavg", "weighted_mean"}:
            weight_total = float(accumulator[f"{side}_weight_total"] or 0.0)
            if math.isclose(weight_total, 0.0, abs_tol=1e-12):
                return None
            return float(accumulator[f"{side}_weighted_sum"]) / weight_total
        if aggregation_fn == "min":
            return accumulator[f"{side}_min"]
        if aggregation_fn == "max":
            return accumulator[f"{side}_max"]
        return None

    def _build_scope_value_changes_from_row_pairs(
        self,
        row_pairs: List[Dict[str, Dict[str, Any]]],
        *,
        grouping_fields: List[str],
        direct_scope_id: Optional[str],
        measure_id: str,
        target_column: str,
        aggregation_fn: str,
        weight_field: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not row_pairs or not measure_id or not target_column or not aggregation_fn:
            return []
        scope_accumulators: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        normalized_direct_scope = str(direct_scope_id or "").strip()
        for row_pair in row_pairs:
            before_row = row_pair.get("before_row") if isinstance(row_pair, dict) else None
            after_row = row_pair.get("after_row") if isinstance(row_pair, dict) else None
            row_source = before_row if isinstance(before_row, dict) else after_row if isinstance(after_row, dict) else None
            if not isinstance(row_source, dict):
                continue
            scope_paths = self._resolve_grouping_scope_paths_for_row(row_source, grouping_fields)
            scoped_targets = ["__grand_total__", *scope_paths] if scope_paths else ([normalized_direct_scope] if normalized_direct_scope else [])
            for scope_id in list(dict.fromkeys([scope for scope in scoped_targets if scope])):
                accumulator = scope_accumulators.setdefault(scope_id, self._new_scope_value_accumulator())
                if isinstance(before_row, dict):
                    self._accumulate_scope_value(
                        accumulator,
                        before_row,
                        target_column=target_column,
                        aggregation_fn=aggregation_fn,
                        weight_field=weight_field,
                        side="before",
                    )
                if isinstance(after_row, dict):
                    self._accumulate_scope_value(
                        accumulator,
                        after_row,
                        target_column=target_column,
                        aggregation_fn=aggregation_fn,
                        weight_field=weight_field,
                        side="after",
                    )

        scope_value_changes: List[Dict[str, Any]] = []
        for scope_id, accumulator in scope_accumulators.items():
            before_value = self._finalize_scope_value_accumulator(accumulator, aggregation_fn, "before")
            after_value = self._finalize_scope_value_accumulator(accumulator, aggregation_fn, "after")
            if before_value is None or after_value is None:
                continue
            if math.isclose(float(before_value), float(after_value), rel_tol=1e-9, abs_tol=1e-9):
                continue
            scope_value_changes.append({
                "scopeId": scope_id,
                "measureId": measure_id,
                "beforeValue": before_value,
                "afterValue": after_value,
                "role": "direct" if scope_id == normalized_direct_scope else "propagated",
                "aggregationFn": aggregation_fn,
            })
        return scope_value_changes

    def _build_direct_scope_value_changes(
        self,
        before_rows: List[Dict[str, Any]],
        *,
        row_path: Optional[str],
        measure_id: str,
        target_column: str,
        next_value: Any,
    ) -> List[Dict[str, Any]]:
        if not before_rows or not row_path or not measure_id or not target_column:
            return []
        inverse_values = self._build_inverse_update_values(before_rows, [target_column]) or {}
        before_value = inverse_values.get(target_column)
        if before_value is None or before_value == next_value:
            return []
        return [{
            "scopeId": str(row_path),
            "measureId": str(measure_id),
            "beforeValue": before_value,
            "afterValue": next_value,
            "role": "direct",
            "aggregationFn": "direct",
        }]

    @staticmethod
    def _new_history_transaction() -> Dict[str, List[Dict[str, Any]]]:
        return {"add": [], "remove": [], "update": [], "upsert": []}

    @staticmethod
    def _compact_history_transaction(transaction: Dict[str, List[Dict[str, Any]]]) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        compacted = {
            kind: list(entries or [])
            for kind, entries in (transaction or {}).items()
            if isinstance(entries, list) and entries
        }
        return compacted or None

    @staticmethod
    def _build_inverse_update_values(before_rows: List[Dict[str, Any]], update_columns: List[str]) -> Optional[Dict[str, Any]]:
        if not before_rows or not update_columns:
            return None
        inverse_values: Dict[str, Any] = {}
        for column in update_columns:
            sentinel = object()
            current_value = sentinel
            for row in before_rows:
                if not isinstance(row, dict):
                    return None
                row_value = row.get(column)
                if current_value is sentinel:
                    current_value = row_value
                    continue
                if row_value != current_value:
                    return None
            if current_value is not sentinel:
                inverse_values[column] = current_value
        return inverse_values or None

    @staticmethod
    def _normalize_aggregation_name(value: Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _normalize_aggregate_propagation_strategy(value: Any) -> str:
        normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
        if normalized in {"none", "skip", "parent_only"}:
            return "none"
        if normalized in {"", "equal", "even", "default", "delta", "uniform"}:
            return "equal"
        if normalized in {"proportional", "ratio", "scale", "scaled"}:
            return "proportional"
        return "equal"

    @staticmethod
    def _coerce_numeric_value(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, decimal.Decimal):
            return float(value)
        if isinstance(value, (int, float)):
            return None if (isinstance(value, float) and math.isnan(value)) else float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                parsed = float(stripped)
            except ValueError:
                return None
            return None if math.isnan(parsed) else parsed
        return None

    @staticmethod
    def _coerce_number_for_storage(original_value: Any, numeric_value: float) -> Any:
        if isinstance(original_value, decimal.Decimal):
            return decimal.Decimal(str(numeric_value))
        if isinstance(original_value, int) and not isinstance(original_value, bool):
            rounded = round(numeric_value)
            if math.isclose(numeric_value, rounded, rel_tol=1e-9, abs_tol=1e-9):
                return int(rounded)
        return numeric_value

    def _get_table_column_dtype(self, table_name: str, column_name: str) -> Any:
        return self.controller._get_table_column_dtype(table_name, column_name)

    @staticmethod
    def _is_integer_dtype(dtype: Any) -> bool:
        return bool(callable(getattr(dtype, "is_integer", None)) and dtype.is_integer())

    def execute_parameterized_fetchone(self, sql: str, params: List[Any]) -> Any:
        self._ensure_mutations_supported()
        adapted_sql = self._adapt_sql_placeholders(sql)
        con = self.controller.planner.con
        if hasattr(con, "con"):
            result = con.con.execute(adapted_sql, [*params])
        elif hasattr(con, "execute"):
            result = con.execute(adapted_sql, [*params])
        else:
            raise NotImplementedError("Backend does not support parameterized SQL queries")
        if hasattr(result, "fetchone"):
            return result.fetchone()
        if hasattr(result, "fetchall"):
            rows = result.fetchall()
            return rows[0] if rows else None
        return (result[0] if result else None) if isinstance(result, list) else result

    def execute_parameterized_fetchall(self, sql: str, params: List[Any]) -> List[Any]:
        self._ensure_mutations_supported()
        adapted_sql = self._adapt_sql_placeholders(sql)
        con = self.controller.planner.con
        if hasattr(con, "con"):
            result = con.con.execute(adapted_sql, [*params])
        elif hasattr(con, "execute"):
            result = con.execute(adapted_sql, [*params])
        else:
            raise NotImplementedError("Backend does not support parameterized SQL queries")
        rows = result.fetchall() if hasattr(result, "fetchall") else (result if isinstance(result, list) else [])
        description = getattr(result, "description", None) or []
        columns = [column[0] for column in description] if description else []
        if columns and rows and not isinstance(rows[0], dict):
            return [
                {columns[index]: row[index] for index in range(min(len(columns), len(row)))}
                for row in rows if isinstance(row, (list, tuple))
            ]
        return rows

    def execute_parameterized_mutation(self, sql: str, params: List[Any]) -> None:
        self._ensure_mutations_supported()
        adapted_sql = self._adapt_sql_placeholders(sql)
        con = self.controller.planner.con
        if hasattr(con, "con") and hasattr(con.con, "execute"):
            con.con.execute(adapted_sql, [*params])
        elif hasattr(con, "execute"):
            con.execute(adapted_sql, [*params])
        elif hasattr(con, "raw_sql"):
            underlying = getattr(con, "_con", None) or getattr(con, "con", None)
            if underlying is not None and hasattr(underlying, "execute"):
                underlying.execute(adapted_sql, [*params])
            else:
                raise NotImplementedError(
                    "raw_sql backend does not support parameterized queries; "
                    "use a backend that exposes .execute() or .con.execute()"
                )
        else:
            raise NotImplementedError("Backend does not support parameterized SQL updates")

    def _fetch_aggregate_edit_summary_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        aggregate_edit: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        target_column = str(aggregate_edit.get("column") or "").strip()
        aggregation_fn = self._normalize_aggregation_name(aggregate_edit.get("aggregationFn"))
        weight_field = str(aggregate_edit.get("weightField") or "").strip()

        if not target_column.isidentifier():
            return None
        if aggregation_fn in {"weighted_avg", "wavg", "weighted_mean"} and not weight_field.isidentifier():
            return None

        where_sql, where_params = self.controller._build_where_clause(key_columns)
        predicates = [predicate for predicate in [where_sql, f"{target_column} IS NOT NULL"] if predicate]
        predicate_sql = " AND ".join(predicates) if predicates else "TRUE"

        current_value_sql = ""
        if aggregation_fn == "sum":
            current_value_sql = f"SUM({target_column})"
        elif aggregation_fn == "avg":
            current_value_sql = f"AVG({target_column})"
        elif aggregation_fn in {"weighted_avg", "wavg", "weighted_mean"}:
            current_value_sql = (
                f"SUM(CASE WHEN {weight_field} IS NOT NULL THEN {target_column} * {weight_field} ELSE NULL END)"
                f" / NULLIF(SUM(CASE WHEN {weight_field} IS NOT NULL THEN {weight_field} ELSE NULL END), 0)"
            )
        else:
            return None

        row = self.controller._execute_parameterized_fetchone(
            (
                f"SELECT COUNT({target_column}) AS matched_row_count, "  # nosec B608
                f"{current_value_sql} AS current_value "
                f"FROM {table_name} "
                f"WHERE {predicate_sql}"
            ),
            where_params,
        )
        if not row:
            return None

        matched_row_count = 0
        current_value = None
        if isinstance(row, dict):
            matched_row_count = int(row.get("matched_row_count") or 0)
            current_value = self._coerce_numeric_value(row.get("current_value"))
        elif isinstance(row, (list, tuple)):
            matched_row_count = int(row[0] or 0)
            current_value = self._coerce_numeric_value(row[1] if len(row) > 1 else None)

        return {
            "matchedRowCount": matched_row_count,
            "currentValue": current_value,
        }

    @staticmethod
    def _build_history_aggregate_update_entry(
        aggregate_edit: Dict[str, Any],
        *,
        value: Any,
        old_value: Any,
    ) -> Optional[Dict[str, Any]]:
        row_id = aggregate_edit.get("rowId")
        col_id = aggregate_edit.get("columnId")
        if row_id is None or col_id is None:
            return None
        return {
            "rowId": str(row_id),
            "colId": str(col_id),
            "value": value,
            "oldValue": old_value,
        }

    def _apply_integer_delta_distribution_sync(
        self,
        table_name: str,
        target_column: str,
        where_sql: str,
        where_params: List[Any],
        matched_row_count: int,
        total_delta: int,
    ) -> int:
        if matched_row_count <= 0 or total_delta == 0:
            return 0

        base_delta = int(total_delta / matched_row_count)
        remainder = int(total_delta - (base_delta * matched_row_count))
        remainder_count = abs(remainder)
        remainder_step = 1 if remainder > 0 else -1 if remainder < 0 else 0
        numeric_where = " AND ".join(
            predicate for predicate in [where_sql, f"{target_column} IS NOT NULL"] if predicate
        ) or f"{target_column} IS NOT NULL"

        if remainder_count == 0:
            self.controller._execute_parameterized_mutation(
                f"UPDATE {table_name} SET {target_column} = {target_column} + ? WHERE {numeric_where}",  # nosec B608
                [base_delta, *where_params],
            )
            return matched_row_count if base_delta != 0 else 0

        if not self._supports_rowid():
            raise ValueError("Integer remainder distribution requires rowid support; falling back to row rewrites.")

        self.controller._execute_parameterized_mutation(
            (
                f"WITH target AS ("  # nosec B608
                f"SELECT rowid, row_number() OVER (ORDER BY rowid) AS rn "
                f"FROM {table_name} "
                f"WHERE {numeric_where}"
                f") "
                f"UPDATE {table_name} "
                f"SET {target_column} = {target_column} + ? + CASE WHEN target.rn <= ? THEN ? ELSE 0 END "
                f"FROM target "
                f"WHERE {table_name}.rowid = target.rowid"
            ),
            [*where_params, base_delta, remainder_count, remainder_step],
        )
        return matched_row_count if base_delta != 0 else remainder_count

    def _apply_integer_proportional_sync(
        self,
        table_name: str,
        target_column: str,
        where_sql: str,
        where_params: List[Any],
        matched_row_count: int,
        scale_factor: float,
        target_sum: float,
    ) -> int:
        """Scale integer rows proportionally with remainder correction."""
        if matched_row_count <= 0 or math.isclose(scale_factor, 1.0, rel_tol=1e-12):
            return 0
        if not self._supports_rowid():
            raise ValueError("Integer proportional CTE requires rowid support; falling back to row rewrites.")
        numeric_where = " AND ".join(
            predicate for predicate in [where_sql, f"{target_column} IS NOT NULL"] if predicate
        ) or f"{target_column} IS NOT NULL"
        rows = self.controller._execute_parameterized_fetchall(
            f"SELECT rowid, {target_column} AS val FROM {table_name} WHERE {numeric_where}",  # nosec B608
            where_params,
        )
        if not rows:
            return 0
        scaled = []
        for row in rows:
            rid = row["rowid"] if isinstance(row, dict) else row[0]
            val = row["val"] if isinstance(row, dict) else row[1]
            if val is None:
                continue
            new_val = round(float(val) * scale_factor)
            scaled.append((rid, new_val))
        if not scaled:
            return 0
        rounded_sum = sum(v for _, v in scaled)
        remainder = int(round(target_sum)) - int(rounded_sum)
        if remainder != 0:
            step = 1 if remainder > 0 else -1
            for i in range(abs(remainder)):
                idx = i % len(scaled)
                rid, val = scaled[idx]
                scaled[idx] = (rid, val + step)
        value_rows_sql = ", ".join(["(?, ?)"] * len(scaled))
        update_params: List[Any] = []
        for rid, new_val in scaled:
            update_params.extend([rid, int(new_val)])

        self.controller._execute_parameterized_mutation(
            (
                f"WITH scaled(target_rowid, new_value) AS (VALUES {value_rows_sql}) "  # nosec B608
                f"UPDATE {table_name} "
                f"SET {target_column} = scaled.new_value "
                f"FROM scaled "
                f"WHERE {table_name}.rowid = scaled.target_rowid"
            ),
            update_params,
        )
        return len(scaled)

    def _apply_set_based_aggregate_edit_sync(
        self,
        table_name: str,
        key_columns: Dict[str, Any],
        aggregate_edit: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        target_column = str(aggregate_edit.get("column") or "").strip()
        aggregation_fn = self._normalize_aggregation_name(aggregate_edit.get("aggregationFn"))
        next_value = self._coerce_numeric_value(aggregate_edit.get("newValue"))

        if (
            not target_column.isidentifier()
            or not aggregation_fn
            or next_value is None
            or aggregate_edit.get("rowId") is None
            or not str(aggregate_edit.get("columnId") or "").strip()
        ):
            return None

        if aggregation_fn not in {"sum", "avg", "weighted_avg", "wavg", "weighted_mean"}:
            return None

        summary = self.controller._fetch_aggregate_edit_summary_sync(table_name, key_columns, aggregate_edit)
        if not isinstance(summary, dict):
            return None

        matched_row_count = int(summary.get("matchedRowCount") or 0)
        current_value = self._coerce_numeric_value(summary.get("currentValue"))
        if matched_row_count <= 0:
            return {"warning": "Skipped aggregate edit because no editable source rows were found."}
        if current_value is None:
            return {"warning": "Skipped aggregate edit because the source rows were not numeric."}

        where_sql, where_params = self.controller._build_where_clause(key_columns)
        numeric_where = " AND ".join(
            predicate for predicate in [where_sql, f"{target_column} IS NOT NULL"] if predicate
        ) or f"{target_column} IS NOT NULL"

        if math.isclose(next_value, current_value, rel_tol=1e-9, abs_tol=1e-9):
            return {
                "appliedCount": 0,
                "propagation": {
                    "mode": "aggregate",
                    "aggregationFn": aggregation_fn,
                    "targetColumn": target_column,
                    "weightField": aggregate_edit.get("weightField"),
                    "strategy": "no_op",
                    "execution": "set_based_sql",
                    "matchedRowCount": matched_row_count,
                    "updatedRowCount": 0,
                    "fromValue": current_value,
                    "toValue": next_value,
                    "columnId": aggregate_edit.get("columnId"),
                },
            }

        target_dtype = self.controller._get_table_column_dtype(table_name, target_column)
        integer_storage = self._is_integer_dtype(target_dtype)
        if integer_storage and not self._supports_rowid():
            return None

        updated_row_count = 0
        strategy = ""
        propagation_strategy = self._normalize_aggregate_propagation_strategy(
            aggregate_edit.get("propagationStrategy") or aggregate_edit.get("propagationFormula")
        )

        if propagation_strategy == "none":
            return {
                "warning": (
                    "Aggregate propagation policy 'none' is no longer supported for persisted edits."
                )
            }

        if aggregation_fn == "sum":
            if propagation_strategy == "proportional":
                if math.isclose(current_value, 0.0, rel_tol=1e-9, abs_tol=1e-9):
                    return {
                        "warning": (
                            "Skipped aggregate edit because proportional propagation requires a non-zero current value."
                        )
                    }
                scale_factor = next_value / current_value
                if integer_storage:
                    updated_row_count = self.controller._apply_integer_proportional_sync(
                        table_name, target_column, where_sql, where_params,
                        matched_row_count, scale_factor, next_value,
                    )
                else:
                    self.controller._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} * ? WHERE {numeric_where}",  # nosec B608
                        [scale_factor, *where_params],
                    )
                    updated_row_count = matched_row_count
                strategy = "proportional"
            else:
                total_delta = next_value - current_value
                if integer_storage:
                    rounded_total_delta = round(total_delta)
                    if not math.isclose(total_delta, rounded_total_delta, rel_tol=1e-9, abs_tol=1e-9):
                        return {
                            "warning": (
                                "Skipped aggregate edit because integer source rows cannot exactly represent "
                                "the requested sum change."
                            )
                        }
                    updated_row_count = self.controller._apply_integer_delta_distribution_sync(
                        table_name,
                        target_column,
                        where_sql,
                        where_params,
                        matched_row_count,
                        int(rounded_total_delta),
                    )
                    strategy = "balanced_delta"
                else:
                    delta_per_row = total_delta / matched_row_count
                    self.controller._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} + ? WHERE {numeric_where}",  # nosec B608
                        [delta_per_row, *where_params],
                    )
                    updated_row_count = matched_row_count
                    strategy = "equal_delta"
        elif aggregation_fn == "avg":
            if propagation_strategy == "proportional":
                if math.isclose(current_value, 0.0, rel_tol=1e-9, abs_tol=1e-9):
                    return {
                        "warning": (
                            "Skipped aggregate edit because proportional propagation requires a non-zero current value."
                        )
                    }
                scale_factor = next_value / current_value
                if integer_storage:
                    updated_row_count = self.controller._apply_integer_proportional_sync(
                        table_name, target_column, where_sql, where_params,
                        matched_row_count, scale_factor, next_value * matched_row_count,
                    )
                else:
                    self.controller._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} * ? WHERE {numeric_where}",  # nosec B608
                        [scale_factor, *where_params],
                    )
                    updated_row_count = matched_row_count
                strategy = "proportional"
            else:
                if integer_storage:
                    total_delta = (next_value - current_value) * matched_row_count
                    rounded_total_delta = round(total_delta)
                    if not math.isclose(total_delta, rounded_total_delta, rel_tol=1e-9, abs_tol=1e-9):
                        return {
                            "warning": (
                                "Skipped aggregate edit because integer source rows cannot exactly represent "
                                "the requested average change."
                            )
                        }
                    updated_row_count = self.controller._apply_integer_delta_distribution_sync(
                        table_name,
                        target_column,
                        where_sql,
                        where_params,
                        matched_row_count,
                        int(rounded_total_delta),
                    )
                    strategy = "balanced_shift"
                else:
                    delta_per_row = next_value - current_value
                    self.controller._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} + ? WHERE {numeric_where}",  # nosec B608
                        [delta_per_row, *where_params],
                    )
                    updated_row_count = matched_row_count
                    strategy = "uniform_shift"
        else:
            if propagation_strategy == "proportional":
                if math.isclose(current_value, 0.0, rel_tol=1e-9, abs_tol=1e-9):
                    return {
                        "warning": (
                            "Skipped aggregate edit because proportional propagation requires a non-zero current value."
                        )
                    }
                scale_factor = next_value / current_value
                if integer_storage:
                    updated_row_count = self.controller._apply_integer_proportional_sync(
                        table_name, target_column, where_sql, where_params,
                        matched_row_count, scale_factor, next_value * matched_row_count,
                    )
                else:
                    self.controller._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {target_column} = {target_column} * ? WHERE {numeric_where}",  # nosec B608
                        [scale_factor, *where_params],
                    )
                    updated_row_count = matched_row_count
                strategy = "proportional"
            else:
                delta_per_row = next_value - current_value
                if integer_storage:
                    rounded_delta = round(delta_per_row)
                    if not math.isclose(delta_per_row, rounded_delta, rel_tol=1e-9, abs_tol=1e-9):
                        return {
                            "warning": (
                                "Skipped aggregate edit because integer source rows cannot exactly represent "
                                "the requested weighted-average change."
                            )
                        }
                    delta_per_row = int(rounded_delta)
                self.controller._execute_parameterized_mutation(
                    f"UPDATE {table_name} SET {target_column} = {target_column} + ? WHERE {numeric_where}",  # nosec B608
                    [delta_per_row, *where_params],
                )
                updated_row_count = matched_row_count
                strategy = "uniform_shift"

        return {
            "appliedCount": updated_row_count,
            "propagation": {
                "mode": "aggregate",
                "aggregationFn": aggregation_fn,
                "targetColumn": target_column,
                "weightField": aggregate_edit.get("weightField"),
                "strategy": strategy,
                "execution": "set_based_sql",
                "matchedRowCount": matched_row_count,
                "updatedRowCount": updated_row_count,
                "fromValue": current_value,
                "toValue": next_value,
                "columnId": aggregate_edit.get("columnId"),
            },
            "inverseUpdate": self._build_history_aggregate_update_entry(
                aggregate_edit,
                value=current_value,
                old_value=next_value,
            ),
            "redoUpdate": self._build_history_aggregate_update_entry(
                aggregate_edit,
                value=next_value,
                old_value=current_value,
            ),
        }

    def _compute_aggregate_edit_value(
        self,
        rows: List[Dict[str, Any]],
        column: str,
        aggregation_fn: str,
        weight_field: Optional[str] = None,
    ) -> Optional[float]:
        normalized_agg = self._normalize_aggregation_name(aggregation_fn)
        numeric_rows = []
        weighted_rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            value = self._coerce_numeric_value(row.get(column))
            if value is None:
                continue
            numeric_rows.append(value)
            if weight_field:
                weight_value = self._coerce_numeric_value(row.get(weight_field))
                if weight_value is not None:
                    weighted_rows.append((value, weight_value))
        if normalized_agg == "sum":
            return float(sum(numeric_rows)) if numeric_rows else None
        if normalized_agg == "avg":
            return (float(sum(numeric_rows)) / len(numeric_rows)) if numeric_rows else None
        if normalized_agg in {"weighted_avg", "wavg", "weighted_mean"}:
            total_weight = sum(weight for _, weight in weighted_rows)
            if not weighted_rows or math.isclose(total_weight, 0.0, abs_tol=1e-12):
                return None
            return float(sum(value * weight for value, weight in weighted_rows) / total_weight)
        if normalized_agg == "min":
            return min(numeric_rows) if numeric_rows else None
        if normalized_agg == "max":
            return max(numeric_rows) if numeric_rows else None
        return None

    def _plan_aggregate_edit_rewrites(
        self,
        before_rows: List[Dict[str, Any]],
        aggregate_edit: Dict[str, Any],
    ) -> tuple[List[Dict[str, Dict[str, Any]]], Dict[str, Any]]:
        target_column = str(aggregate_edit.get("column") or "").strip()
        aggregation_fn = self._normalize_aggregation_name(aggregate_edit.get("aggregationFn"))
        weight_field = aggregate_edit.get("weightField")
        next_value = self._coerce_numeric_value(aggregate_edit.get("newValue"))
        if not target_column or not aggregation_fn:
            raise ValueError("Skipped aggregate edit without a valid target column or aggregation.")
        if next_value is None:
            raise ValueError("Skipped aggregate edit because the submitted value was not numeric.")

        current_value = self._compute_aggregate_edit_value(
            before_rows,
            target_column,
            aggregation_fn,
            weight_field=weight_field,
        )
        if current_value is None:
            raise ValueError("Skipped aggregate edit because the source rows were not numeric.")

        numeric_rows = []
        for row in before_rows:
            if not isinstance(row, dict):
                continue
            current_row_value = self._coerce_numeric_value(row.get(target_column))
            if current_row_value is None:
                continue
            numeric_rows.append(
                {
                    "before_row": dict(row),
                    "current_value": current_row_value,
                }
            )
        if not numeric_rows:
            raise ValueError("Skipped aggregate edit because no editable source rows were found.")

        changed_rows: List[Dict[str, Dict[str, Any]]] = []
        strategy = ""

        if aggregation_fn == "sum":
            strategy = "equal_delta"
            delta_per_row = (next_value - current_value) / len(numeric_rows)
            for entry in numeric_rows:
                updated_numeric = entry["current_value"] + delta_per_row
                after_row = dict(entry["before_row"])
                after_row[target_column] = self._coerce_number_for_storage(
                    entry["before_row"].get(target_column),
                    updated_numeric,
                )
                changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
        elif aggregation_fn in {"avg", "weighted_avg", "wavg", "weighted_mean"}:
            strategy = "uniform_shift"
            delta_per_row = next_value - current_value
            for entry in numeric_rows:
                updated_numeric = entry["current_value"] + delta_per_row
                after_row = dict(entry["before_row"])
                after_row[target_column] = self._coerce_number_for_storage(
                    entry["before_row"].get(target_column),
                    updated_numeric,
                )
                changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
        elif aggregation_fn == "min":
            if next_value >= current_value:
                strategy = "raise_floor"
                for entry in numeric_rows:
                    if entry["current_value"] < next_value:
                        after_row = dict(entry["before_row"])
                        after_row[target_column] = self._coerce_number_for_storage(
                            entry["before_row"].get(target_column),
                            next_value,
                        )
                        changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
            else:
                strategy = "lower_floor"
                for entry in numeric_rows:
                    if math.isclose(entry["current_value"], current_value, rel_tol=1e-9, abs_tol=1e-9):
                        after_row = dict(entry["before_row"])
                        after_row[target_column] = self._coerce_number_for_storage(
                            entry["before_row"].get(target_column),
                            next_value,
                        )
                        changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
        elif aggregation_fn == "max":
            if next_value <= current_value:
                strategy = "lower_ceiling"
                for entry in numeric_rows:
                    if entry["current_value"] > next_value:
                        after_row = dict(entry["before_row"])
                        after_row[target_column] = self._coerce_number_for_storage(
                            entry["before_row"].get(target_column),
                            next_value,
                        )
                        changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
            else:
                strategy = "raise_ceiling"
                for entry in numeric_rows:
                    if math.isclose(entry["current_value"], current_value, rel_tol=1e-9, abs_tol=1e-9):
                        after_row = dict(entry["before_row"])
                        after_row[target_column] = self._coerce_number_for_storage(
                            entry["before_row"].get(target_column),
                            next_value,
                        )
                        changed_rows.append({"before_row": entry["before_row"], "after_row": after_row})
        else:
            raise ValueError(f"Skipped aggregate edit because aggregation '{aggregation_fn}' is not supported.")

        filtered_changes = [
            row_change
            for row_change in changed_rows
            if row_change["before_row"].get(target_column) != row_change["after_row"].get(target_column)
        ]

        return filtered_changes, {
            "mode": "aggregate",
            "aggregationFn": aggregation_fn,
            "targetColumn": target_column,
            "weightField": weight_field,
            "strategy": strategy,
            "matchedRowCount": len(numeric_rows),
            "updatedRowCount": len(filtered_changes),
            "fromValue": current_value,
            "toValue": next_value,
            "columnId": aggregate_edit.get("columnId"),
        }

    def _build_insert_statement(self, table_name: str, row_data: Dict[str, Any], table_columns: List[str]) -> Optional[tuple[str, List[Any]]]:
        sanitized_row = self._sanitize_column_mapping(row_data, allowed_columns=table_columns)
        if not sanitized_row:
            return None
        insert_columns = [column for column in table_columns if column in sanitized_row]
        if not insert_columns:
            return None
        placeholders = ", ".join(["?"] * len(insert_columns))
        sql = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})"  # nosec B608
        return sql, [sanitized_row[column] for column in insert_columns]

    async def apply_row_transaction(self, table_name: str, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Apply an atomic add/remove/update/upsert transaction and return a structured result."""
        self._ensure_mutations_supported()
        if not table_name.isidentifier():
            raise ValueError("Invalid table name")

        table_columns = self._get_table_columns(table_name)
        add_operations = [
            operation
            for operation in (transaction.get("add") or [])
            if isinstance(operation, dict)
        ]
        remove_operations = [
            operation
            for operation in (transaction.get("remove") or [])
            if isinstance(operation, dict)
        ]
        update_operations = [
            operation
            for operation in (transaction.get("update") or [])
            if isinstance(operation, dict)
        ]
        upsert_operations = [
            operation
            for operation in (transaction.get("upsert") or [])
            if isinstance(operation, dict)
        ]
        requested = {
            "add": len(add_operations),
            "remove": len(remove_operations),
            "update": len(update_operations),
            "upsert": len(upsert_operations),
        }

        loop = asyncio.get_running_loop()

        def execute_transaction() -> Dict[str, Any]:
            applied = {
                "add": 0,
                "remove": 0,
                "update": 0,
                "upsertInserted": 0,
                "upsertUpdated": 0,
            }
            warnings: List[str] = []
            history_warnings: List[str] = []
            propagation_events: List[Dict[str, Any]] = []
            history_captureable = True
            inverse_transaction = self._new_history_transaction()
            redo_transaction = self._new_history_transaction()
            scope_value_changes: List[Dict[str, Any]] = []
            backend = getattr(self.controller, "backend", None)
            transaction_ctx = backend.transaction() if backend and hasattr(backend, "transaction") else nullcontext()

            with transaction_ctx:
                for operation in remove_operations:
                    key_columns = self._sanitize_column_mapping(operation.get("key_columns"), allowed_columns=table_columns)
                    if not key_columns:
                        warnings.append("Skipped remove operation without valid key columns.")
                        continue
                    matched_rows = self._fetch_matching_rows_sync(table_name, key_columns, table_columns=table_columns)
                    if not matched_rows:
                        continue
                    where_sql, where_params = self.controller._build_where_clause(key_columns)
                    self.controller._execute_parameterized_mutation(
                        f"DELETE FROM {table_name} WHERE {where_sql}",  # nosec B608
                        where_params,
                    )
                    applied["remove"] += len(matched_rows)
                    redo_transaction["remove"].append({"keys": dict(key_columns)})
                    inverse_transaction["add"].extend([dict(row) for row in matched_rows if isinstance(row, dict)])

                for operation in update_operations:
                    key_columns = self._sanitize_column_mapping(operation.get("key_columns"), allowed_columns=table_columns)
                    aggregate_edit = operation.get("aggregate_edit") if isinstance(operation.get("aggregate_edit"), dict) else None
                    edit_meta = operation.get("edit_meta") if isinstance(operation.get("edit_meta"), dict) else {}
                    grouping_fields = [
                        str(field)
                        for field in (edit_meta.get("groupingFields") or [])
                        if isinstance(field, str) and field
                    ]
                    if aggregate_edit:
                        if not key_columns:
                            warnings.append("Skipped aggregate edit without valid row or pivot keys.")
                            continue
                        before_rows_with_rowid = self.controller._fetch_matching_rows_with_rowid_sync(
                            table_name,
                            key_columns,
                            table_columns=table_columns,
                        )
                        set_based_result = self.controller._apply_set_based_aggregate_edit_sync(
                            table_name,
                            key_columns,
                            aggregate_edit,
                        )
                        if isinstance(set_based_result, dict):
                            warning = set_based_result.get("warning")
                            if warning:
                                warnings.append(str(warning))
                                continue
                            applied_count = int(set_based_result.get("appliedCount") or 0)
                            propagation_summary = set_based_result.get("propagation")
                            if propagation_summary:
                                propagation_events.append(propagation_summary)
                            if applied_count > 0:
                                after_rows_with_rowid = self.controller._fetch_matching_rows_with_rowid_sync(
                                    table_name,
                                    key_columns,
                                    table_columns=table_columns,
                                )
                                before_by_rowid = {
                                    row.get("__rowid__"): row
                                    for row in before_rows_with_rowid
                                    if isinstance(row, dict) and row.get("__rowid__") is not None
                                }
                                target_column = str(aggregate_edit.get("column") or "").strip()
                                row_pairs = [
                                    {
                                        "before_row": before_by_rowid.get(after_row.get("__rowid__")),
                                        "after_row": after_row,
                                    }
                                    for after_row in after_rows_with_rowid
                                    if isinstance(after_row, dict)
                                    and after_row.get("__rowid__") in before_by_rowid
                                    and before_by_rowid.get(after_row.get("__rowid__"), {}).get(target_column) != after_row.get(target_column)
                                ]
                                scope_value_changes.extend(
                                    self._build_scope_value_changes_from_row_pairs(
                                        row_pairs,
                                        grouping_fields=grouping_fields,
                                        direct_scope_id=aggregate_edit.get("rowPath") or aggregate_edit.get("rowId"),
                                        measure_id=str(aggregate_edit.get("columnId") or "").strip(),
                                        target_column=target_column,
                                        aggregation_fn=self._normalize_aggregation_name(aggregate_edit.get("aggregationFn")),
                                        weight_field=aggregate_edit.get("weightField"),
                                    )
                                )
                                applied["update"] += applied_count
                                inverse_update = set_based_result.get("inverseUpdate")
                                redo_update = set_based_result.get("redoUpdate")
                                if inverse_update:
                                    inverse_transaction["update"].append(inverse_update)
                                else:
                                    history_captureable = False
                                    history_warnings.append(
                                        "Skipped transaction-level undo capture for an aggregate edit because "
                                        "the aggregate cell identity could not be reconstructed."
                                    )
                                if redo_update:
                                    redo_transaction["update"].append(redo_update)
                                else:
                                    history_captureable = False
                                    history_warnings.append(
                                        "Skipped transaction-level redo capture for an aggregate edit because "
                                        "the aggregate cell identity could not be reconstructed."
                                    )
                            continue
                        before_rows = before_rows_with_rowid
                        if not before_rows:
                            continue
                        try:
                            row_rewrites, propagation_summary = self.controller._plan_aggregate_edit_rewrites(before_rows, aggregate_edit)
                        except ValueError as exc:
                            warnings.append(str(exc))
                            continue
                        if not row_rewrites:
                            warnings.append(
                                f"Aggregate edit for '{aggregate_edit.get('column')}' produced no source-row changes."
                            )
                            continue
                        scope_value_changes.extend(
                            self._build_scope_value_changes_from_row_pairs(
                                row_rewrites,
                                grouping_fields=grouping_fields,
                                direct_scope_id=aggregate_edit.get("rowPath") or aggregate_edit.get("rowId"),
                                measure_id=str(aggregate_edit.get("columnId") or "").strip(),
                                target_column=str(aggregate_edit.get("column") or "").strip(),
                                aggregation_fn=self._normalize_aggregation_name(aggregate_edit.get("aggregationFn")),
                                weight_field=aggregate_edit.get("weightField"),
                            )
                        )

                        grouped_rewrites: OrderedDict[str, Dict[str, Any]] = OrderedDict()
                        unique_before_rows: OrderedDict[str, Dict[str, Any]] = OrderedDict()
                        unique_after_rows: OrderedDict[str, Dict[str, Any]] = OrderedDict()
                        for rewrite in row_rewrites:
                            before_row = self._sanitize_column_mapping(rewrite.get("before_row"), allowed_columns=table_columns)
                            after_row = self._sanitize_column_mapping(rewrite.get("after_row"), allowed_columns=table_columns)
                            if not before_row or not after_row:
                                continue
                            before_key = self._stable_json(before_row)
                            after_key = self._stable_json(after_row)
                            if before_key not in unique_before_rows:
                                unique_before_rows[before_key] = dict(before_row)
                            if after_key not in unique_after_rows:
                                unique_after_rows[after_key] = dict(after_row)
                            grouped_key = f"{before_key}->{after_key}"
                            if grouped_key not in grouped_rewrites:
                                grouped_rewrites[grouped_key] = {
                                    "before_row": dict(before_row),
                                    "after_row": dict(after_row),
                                }

                        if not grouped_rewrites:
                            warnings.append(
                                f"Aggregate edit for '{aggregate_edit.get('column')}' produced no executable row rewrites."
                            )
                            continue

                        for grouped_rewrite in grouped_rewrites.values():
                            before_row = grouped_rewrite["before_row"]
                            after_row = grouped_rewrite["after_row"]
                            updates = {
                                column: after_row[column]
                                for column in table_columns
                                if column in before_row and column in after_row and before_row.get(column) != after_row.get(column)
                            }
                            if not updates:
                                continue
                            where_sql, where_params = self.controller._build_where_clause(before_row)
                            set_parts = [f"{column} = ?" for column in updates.keys()]
                            matched_count = self._count_matching_rows_sync(table_name, before_row)
                            if matched_count <= 0:
                                continue
                            self.controller._execute_parameterized_mutation(
                                f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {where_sql}",  # nosec B608
                                [*updates.values(), *where_params],
                            )

                        applied["update"] += len(row_rewrites)
                        propagation_events.append(propagation_summary)
                        redo_transaction["remove"].extend(
                            {"keys": dict(row), "exactKeys": True}
                            for row in unique_before_rows.values()
                        )
                        redo_transaction["add"].extend(
                            dict(rewrite["after_row"])
                            for rewrite in row_rewrites
                            if isinstance(rewrite.get("after_row"), dict)
                        )
                        inverse_transaction["remove"].extend(
                            {"keys": dict(row), "exactKeys": True}
                            for row in unique_after_rows.values()
                        )
                        inverse_transaction["add"].extend(
                            dict(rewrite["before_row"])
                            for rewrite in row_rewrites
                            if isinstance(rewrite.get("before_row"), dict)
                        )
                        continue

                    updates = self._sanitize_column_mapping(
                        operation.get("updates"),
                        allowed_columns=table_columns,
                        exclude_columns=list(key_columns.keys()),
                    )
                    if not key_columns or not updates:
                        warnings.append("Skipped update operation without valid key columns or updates.")
                        continue
                    before_rows = self._fetch_matching_rows_sync(table_name, key_columns, table_columns=table_columns)
                    if not before_rows:
                        continue
                    if edit_meta.get("rowPath") and edit_meta.get("colId") and len(updates) == 1:
                        target_column = next(iter(updates.keys()))
                        scope_value_changes.extend(
                            self._build_direct_scope_value_changes(
                                before_rows,
                                row_path=str(edit_meta.get("rowPath") or edit_meta.get("rowId") or ""),
                                measure_id=str(edit_meta.get("colId") or ""),
                                target_column=target_column,
                                next_value=updates.get(target_column),
                            )
                        )
                    set_parts = [f"{column} = ?" for column in updates.keys()]
                    where_sql, where_params = self.controller._build_where_clause(key_columns)
                    self.controller._execute_parameterized_mutation(
                        f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {where_sql}",  # nosec B608
                        [*updates.values(), *where_params],
                    )
                    applied["update"] += len(before_rows)
                    redo_transaction["update"].append({
                        "keys": dict(key_columns),
                        "values": dict(updates),
                    })
                    inverse_updates = self._build_inverse_update_values(before_rows, list(updates.keys()))
                    if inverse_updates:
                        inverse_transaction["update"].append({
                            "keys": dict(key_columns),
                            "values": inverse_updates,
                        })
                    else:
                        inverse_transaction["remove"].append({"keys": dict(key_columns)})
                        inverse_transaction["add"].extend([dict(row) for row in before_rows if isinstance(row, dict)])
                        history_warnings.append(
                            "Captured structural inverse for a multi-row update because prior values were not uniform."
                        )

                for operation in upsert_operations:
                    key_columns = self._sanitize_column_mapping(operation.get("key_columns"), allowed_columns=table_columns)
                    row_data = self._sanitize_column_mapping(operation.get("row_data"), allowed_columns=table_columns)
                    merged_row = {**key_columns, **row_data}
                    if not key_columns or not merged_row:
                        warnings.append("Skipped upsert operation without valid keys or row data.")
                        continue
                    before_rows = self._fetch_matching_rows_sync(table_name, key_columns, table_columns=table_columns)
                    if before_rows:
                        updates = self._sanitize_column_mapping(
                            merged_row,
                            allowed_columns=table_columns,
                            exclude_columns=list(key_columns.keys()),
                        )
                        if updates:
                            set_parts = [f"{column} = ?" for column in updates.keys()]
                            where_sql, where_params = self.controller._build_where_clause(key_columns)
                            self.controller._execute_parameterized_mutation(
                                f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {where_sql}",  # nosec B608
                                [*updates.values(), *where_params],
                            )
                            applied["upsertUpdated"] += len(before_rows)
                            redo_transaction["upsert"].append({
                                "keys": dict(key_columns),
                                "rowData": dict(merged_row),
                            })
                            inverse_updates = self._build_inverse_update_values(before_rows, list(updates.keys()))
                            if inverse_updates:
                                inverse_transaction["update"].append({
                                    "keys": dict(key_columns),
                                    "values": inverse_updates,
                                })
                            else:
                                inverse_transaction["remove"].append({"keys": dict(key_columns)})
                                inverse_transaction["add"].extend([dict(row) for row in before_rows if isinstance(row, dict)])
                                history_warnings.append(
                                    "Captured structural inverse for a multi-row upsert update because prior values were not uniform."
                                )
                        continue
                    insert_statement = self.controller._build_insert_statement(table_name, merged_row, table_columns)
                    if insert_statement is None:
                        warnings.append("Skipped upsert insert because no valid row columns were provided.")
                        continue
                    sql, params = insert_statement
                    self.controller._execute_parameterized_mutation(sql, params)
                    applied["upsertInserted"] += 1
                    redo_transaction["upsert"].append({
                        "keys": dict(key_columns),
                        "rowData": dict(merged_row),
                    })
                    if key_columns:
                        inverse_transaction["remove"].append({"keys": dict(key_columns)})
                    else:
                        history_captureable = False
                        history_warnings.append("Undo history skipped for an inserted upsert because no key fields were available.")

                for operation in add_operations:
                    row_source = operation.get("row_data") if isinstance(operation.get("row_data"), dict) else operation
                    row = self._sanitize_column_mapping(
                        row_source,
                        allowed_columns=table_columns,
                    )
                    key_columns = self._sanitize_column_mapping(operation.get("key_columns"), allowed_columns=table_columns)
                    insert_statement = self.controller._build_insert_statement(table_name, row, table_columns)
                    if insert_statement is None:
                        warnings.append("Skipped add operation because no valid row columns were provided.")
                        continue
                    sql, params = insert_statement
                    self.controller._execute_parameterized_mutation(sql, params)
                    applied["add"] += 1
                    redo_transaction["add"].append(dict(row))
                    if key_columns:
                        inverse_transaction["remove"].append({"keys": dict(key_columns)})
                    else:
                        history_captureable = False
                        history_warnings.append("Undo history skipped for an added row because no key fields were available.")

            undo_transaction = self._compact_history_transaction(inverse_transaction)
            replay_transaction = self._compact_history_transaction(redo_transaction)
            if not any(applied.values()):
                history_captureable = False
            if history_captureable and (undo_transaction is None or replay_transaction is None):
                history_captureable = False
                history_warnings.append("Undo history was not captured because the transaction produced no reversible operations.")

            return {
                "requested": requested,
                "applied": applied,
                "warnings": warnings,
                "rowCountDelta": (
                    applied["add"]
                    + applied["upsertInserted"]
                    - applied["remove"]
                ),
                "inverseTransaction": undo_transaction if history_captureable else None,
                "redoTransaction": replay_transaction if history_captureable else None,
                "propagation": propagation_events,
                "scopeValueChanges": scope_value_changes,
                "history": {
                    "captureable": bool(history_captureable),
                    "warnings": history_warnings,
                },
            }

        result = await loop.run_in_executor(None, execute_transaction)
        if any(result["applied"].values()):
            applied_counts = result.get("applied") or {}
            structural_changed = bool(
                (applied_counts.get("add") or 0)
                or (applied_counts.get("remove") or 0)
                or (applied_counts.get("upsertInserted") or 0)
            )
            self.controller._clear_mutation_caches(table_name, structural=structural_changed)
        return result

    async def update_cell(
        self,
        table_name: str,
        row_id: Any,
        column: str,
        value: Any,
        id_column: str = "uuid",
    ) -> Dict[str, Any]:
        self._ensure_mutations_supported()
        if not table_name.isidentifier() or not column.isidentifier() or not id_column.isidentifier():
            raise ValueError("Invalid identifier in update request")

        sql = f"UPDATE {table_name} SET {column} = ? WHERE {id_column} = ?"  # nosec B608
        loop = asyncio.get_running_loop()

        def execute_update():
            self.controller._execute_parameterized_mutation(sql, [value, row_id])

        await loop.run_in_executor(None, execute_update)
        self.controller._clear_mutation_caches(table_name)
        return {"status": "success", "updated": 1}
