"""Bounded, parameterized native AG Grid SSRM DuckDB datasource.

Only identifiers derived from the fixed source schema and declared pivot IDs
enter SQL. Every filter, pivot value, group path and page bound is a parameter.
Unsupported request semantics raise ValueError instead of being downgraded.
"""

from __future__ import annotations

import math

from .million_dataset import GROUP_FIELDS, MEASURES, PIVOT_SEPARATOR, TEXT_FIELDS, periods_for_shape, validate_filters


_REQUEST_KEYS = {"startRow", "endRow", "rowGroupCols", "groupKeys", "pivotMode", "pivotCols", "valueCols", "filterModel", "sortModel"}
_AGGREGATES = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX", "count": "COUNT"}
_COMPARISONS = {"equals": "=", "notEqual": "<>", "lessThan": "<", "lessThanOrEqual": "<=", "greaterThan": ">", "greaterThanOrEqual": ">="}


def _identifier(name):
    # All callers use validated fixed fields or period/measure-derived aliases.
    return '"' + name + '"'


def _columns(value, allowed, *, aggregates=False):
    if not isinstance(value, list):
        raise ValueError("column declarations must be a list")
    result = []
    for col in value:
        if not isinstance(col, dict) or set(col) - {"id", "field", "displayName", "aggFunc"}:
            raise ValueError("unsupported column declaration")
        field = col.get("field")
        if not isinstance(field, str) or field not in allowed or field in [item[0] for item in result]:
            raise ValueError("invalid or duplicate column field")
        if "id" in col and col["id"] != field:
            raise ValueError("custom column IDs are not supported")
        agg = col.get("aggFunc")
        if aggregates and (not isinstance(agg, str) or agg not in _AGGREGATES):
            raise ValueError("unsupported aggregation")
        if not aggregates and agg is not None:
            raise ValueError("group/pivot column cannot aggregate")
        result.append((field, agg))
    return result


def _filter_sql(filters):
    clauses, params = [], []
    for field, spec in filters.items():
        quoted = _identifier(field)
        if spec["filterType"] == "set":
            values = [value for value in spec["values"] if value is not None]
            alternatives = []
            if values:
                alternatives.append(f"{quoted} IN ({', '.join('?' for _ in values)})")
                params.extend(values)
            if None in spec["values"]:
                alternatives.append(f"{quoted} IS NULL")
            clauses.append("(" + " OR ".join(alternatives) + ")" if alternatives else "FALSE")
        elif spec["filterType"] == "number":
            # BIGINT compared with a DOUBLE parameter can collapse distinct
            # integers above 2**53. Lower every accepted threshold into the
            # integer domain before binding, including fractional inequalities.
            value, comparison = spec["filter"], spec["type"]
            lower = math.floor(value)
            if value == lower:
                clauses.append(f"{quoted} {_COMPARISONS[comparison]} ?")
                params.append(lower)
            elif comparison == "equals":
                clauses.append("FALSE")
            elif comparison == "notEqual":
                clauses.append(f"{quoted} IS NOT NULL")
            elif comparison in {"lessThan", "lessThanOrEqual"}:
                clauses.append(f"{quoted} <= ?")
                params.append(lower)
            else:
                clauses.append(f"{quoted} >= ?")
                params.append(math.ceil(value))
        else:
            operator = "="
            clauses.append(f"{quoted} {operator} ?")
            params.append(spec["filter"])
    return clauses, params


def query_ag_grid(connection, request: dict, *, shape: str) -> dict:
    """Return ``rowData``, full-window ``rowCount`` and stable pivot fields.

    Group requests aggregate exactly the next requested grouping level. A path
    consuming every group column requests raw fact rows, ordered by record_id
    as the final tie breaker. Null group paths use SQL IS NOT DISTINCT FROM.
    The native auto-group sort ID maps to the next grouping field; at a leaf
    it maps to record_id, where every requested group key is already fixed.
    """
    periods = periods_for_shape(shape)
    if not isinstance(request, dict) or set(request) - _REQUEST_KEYS:
        raise ValueError("unsupported SSRM request property")
    start, end = request.get("startRow"), request.get("endRow")
    if type(start) is not int or type(end) is not int or start < 0 or end <= start or end - start > 200 or end > 2**63 - 1:
        raise ValueError("request needs a positive window of at most 200 rows")
    groups = _columns(request.get("rowGroupCols"), GROUP_FIELDS)
    pivots = _columns(request.get("pivotCols"), ("period",))
    values = _columns(request.get("valueCols"), MEASURES, aggregates=True)
    if request.get("pivotMode") is not True or pivots != [("period", None)] or not values:
        raise ValueError("benchmark requires a period pivot and value columns")
    keys = request.get("groupKeys")
    if not isinstance(keys, list) or len(keys) > len(groups) or len(groups) > 3 or any(key is not None and not isinstance(key, str) for key in keys):
        raise ValueError("invalid group path")
    filters = validate_filters(request.get("filterModel", {}))
    clauses, where_params = _filter_sql(filters)
    for (field, _), key in zip(groups, keys):
        clauses.append(f"{_identifier(field)} IS NOT DISTINCT FROM ?")
        where_params.append(key)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    leaf = bool(groups) and len(keys) == len(groups)
    next_field = groups[len(keys)][0] if len(keys) < len(groups) else None
    projections, select_params = [], []
    if leaf:
        projections.extend(_identifier(field) for field in ("record_id", *TEXT_FIELDS, *MEASURES))
    elif next_field:
        projections.append(_identifier(next_field))
    if not leaf:
        projections.extend(f"{_AGGREGATES[agg]}({_identifier(field)}) AS {_identifier(field)}" for field, agg in values)
    pivot_identity = {f"{period}{PIVOT_SEPARATOR}{field}": (period, field, agg)
                      for period in periods for field, agg in values}
    pivot_result_fields = list(pivot_identity)
    group_by = f" GROUP BY {_identifier(next_field)}" if next_field and not leaf else ""
    sort_model = request.get("sortModel", [])
    if not isinstance(sort_model, list):
        raise ValueError("sortModel must be a list")
    sortable = {"record_id", *TEXT_FIELDS, *MEASURES, *pivot_result_fields} if leaf else {field for field, _ in values} | set(pivot_result_fields) | ({next_field} if next_field else set())
    order, sorted_fields = [], set()
    for sort in sort_model:
        if not isinstance(sort, dict) or set(sort) != {"colId", "sort"}:
            raise ValueError("invalid sort declaration")
        field, direction = sort["colId"], sort["sort"]
        if field == "ag-Grid-AutoColumn":
            field = "record_id" if leaf else next_field
        if not isinstance(field, str) or field not in sortable or not isinstance(direction, str) or direction not in {"asc", "desc"} or field in sorted_fields:
            raise ValueError("unsupported sort column or direction")
        sorted_fields.add(field)
        order.append(f"{_identifier(field)} {direction.upper()} NULLS LAST")
        if field in pivot_identity:
            period, measure, agg = pivot_identity[field]
            expression = f"{_AGGREGATES[agg]}({_identifier(measure)}) FILTER (WHERE period = ?)"
            if leaf:
                expression = f"CASE WHEN period = ? THEN {_identifier(measure)} END"
                if agg == "count":
                    expression = f"CASE WHEN period = ? AND {_identifier(measure)} IS NOT NULL THEN 1 ELSE 0 END"
            projections.append(f"{expression} AS {_identifier(field)}")
            select_params.append(period)
    tie_field = "record_id" if leaf else next_field
    if tie_field and tie_field not in sorted_fields:
        order.append(f"{_identifier(tie_field)} ASC NULLS LAST")
    order_by = " ORDER BY " + ", ".join(order) if order else ""
    # Count independently of the page so an out-of-range page still has a count.
    if next_field and not leaf:
        count_sql = f"SELECT COUNT(*) FROM (SELECT {_identifier(next_field)} FROM benchmark_facts{where}{group_by}) AS grouped"
    elif leaf:
        count_sql = f"SELECT COUNT(*) FROM benchmark_facts{where}"
    else:
        count_sql = "SELECT 1"
    row_count = connection.execute(count_sql, where_params if next_field or leaf else []).fetchone()[0]
    sql = f"SELECT {', '.join(projections)} FROM benchmark_facts{where}{group_by}{order_by} LIMIT ? OFFSET ?"
    cursor = connection.execute(sql, [*select_params, *where_params, end - start, start])
    names = [item[0] for item in cursor.description]
    rows = [dict(zip(names, row)) for row in cursor.fetchall()]
    for row in rows:
        for field, (_, _, agg) in pivot_identity.items():
            row[field] = 0 if agg == "count" else None
    if leaf:
        for row in rows:
            for field, (period, measure, agg) in pivot_identity.items():
                value = row[measure]
                if row["period"] == period:
                    row[field] = int(value is not None) if agg == "count" else value
    elif rows:
        # Aggregate by period in long form. The query returns at most 200
        # selected groups * 256 periods, avoiding 512 predicate evaluations
        # per fact and unbounded wide intermediate group states.
        long_clauses, long_params = list(clauses), list(where_params)
        if next_field:
            selected = [row[next_field] for row in rows]
            non_null = [key for key in selected if key is not None]
            alternatives = []
            if non_null:
                alternatives.append(f"{_identifier(next_field)} IN ({', '.join('?' for _ in non_null)})")
                long_params.extend(non_null)
            if None in selected:
                alternatives.append(f"{_identifier(next_field)} IS NULL")
            long_clauses.append("(" + " OR ".join(alternatives) + ")")
        long_where = " WHERE " + " AND ".join(long_clauses) if long_clauses else ""
        long_groups = ([next_field] if next_field else []) + ["period"]
        long_projections = [_identifier(field) for field in long_groups]
        long_projections.extend(f"{_AGGREGATES[agg]}({_identifier(field)}) AS {_identifier(field)}" for field, agg in values)
        long_sql = f"SELECT {', '.join(long_projections)} FROM benchmark_facts{long_where} GROUP BY {', '.join(_identifier(field) for field in long_groups)}"
        lookup = {row[next_field] if next_field else None: row for row in rows}
        for result in connection.execute(long_sql, long_params).fetchall():
            key = result[0] if next_field else None
            period_index = 1 if next_field else 0
            period = result[period_index]
            if period not in periods:
                continue
            row = lookup[key]
            for index, (measure, _) in enumerate(values):
                row[f"{period}{PIVOT_SEPARATOR}{measure}"] = result[period_index + 1 + index]
    return {"rowData": rows, "rowCount": row_count, "pivotResultFields": pivot_result_fields}
