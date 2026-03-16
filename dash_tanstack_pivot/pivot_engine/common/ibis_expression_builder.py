"""
Unified Ibis Expression Builder module to eliminate duplication
between IbisPlanner, ProgressiveDataLoader, and HierarchicalVirtualScrollManager
"""
from typing import List, Dict, Any, Optional, Union
import ibis
from ibis import BaseBackend as IbisBaseBackend
from ibis.expr.api import Table as IbisTable, Expr as IbisExpr
from pivot_engine.types.pivot_spec import PivotSpec, Measure


class IbisExpressionBuilder:
    """
    Unified class for building Ibis expressions across all components
    """
    
    def __init__(self, backend: IbisBaseBackend):
        self.backend = backend

    def build_filter_expression(self, table: IbisTable, filters: List[Dict[str, Any]]) -> Optional[IbisExpr]:
        """Converts a list of filter dictionaries into an Ibis boolean expression."""
        if not filters:
            return None

        ibis_filters = []
        for f in filters:
            field = f.get("field")
            op = (f.get("op") or "=").lower()
            value = f.get("value")

            if field not in table.columns:
                print(f"Warning: Filter field '{field}' not found in table during Ibis filter conversion.")
                continue

            col = table[field]

            if op in ["=", "=="]:
                ibis_filters.append(col == value)
            elif op == "!=":
                ibis_filters.append(col != value)
            elif op == "<":
                ibis_filters.append(col < value)
            elif op == "<=":
                ibis_filters.append(col <= value)
            elif op == ">":
                ibis_filters.append(col > value)
            elif op == ">=":
                ibis_filters.append(col >= value)
            elif op == "in":
                if isinstance(value, (list, tuple, set)):
                    ibis_filters.append(col.isin(value))
                else:
                    # Treat single value 'in' as equality
                    ibis_filters.append(col == value)
            elif op == "between":
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    ibis_filters.append(col.between(value[0], value[1]))
            elif op == "like":
                ibis_filters.append(col.like(value))
            elif op == "ilike":
                ibis_filters.append(col.ilike(value))
            elif op == "is null":
                ibis_filters.append(col.isnull())
            elif op == "is not null":
                ibis_filters.append(col.notnull())
            elif op == "starts_with":
                ibis_filters.append(col.like(f"{value}%"))
            elif op == "ends_with":
                ibis_filters.append(col.like(f"%{value}"))
            elif op == "contains":
                ibis_filters.append(col.like(f"%{value}%"))

        if not ibis_filters:
            return None

        combined_filter = ibis_filters[0]
        for f_expr in ibis_filters[1:]:
            combined_filter &= f_expr
        return combined_filter

    def build_sort_expressions(self, table: IbisTable, sort_specs: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Union[ibis.Column, ibis.Expr]]:
        """Converts sort specifications to a list of Ibis sort expressions."""
        if not sort_specs:
            return []

        sort_list = [sort_specs] if isinstance(sort_specs, dict) else sort_specs
        ibis_sorts = []

        for s in sort_list:
            field = s.get("field")
            if not field or field not in table.columns:
                continue

            order = (s.get("order") or "asc").lower()
            nulls = (s.get("nulls") or "").lower()

            col = table[field]
            sort_expr = None

            if order == 'asc':
                sort_expr = col.asc
            elif order == 'desc':
                sort_expr = col.desc

            if sort_expr:
                if nulls == 'first':
                    ibis_sorts.append(sort_expr(nulls_first=True))
                elif nulls == 'last':
                    ibis_sorts.append(sort_expr(nulls_last=True))
                else:
                    ibis_sorts.append(sort_expr())

        return ibis_sorts

    def build_cursor_filter_expression(self, table: IbisTable, spec: PivotSpec) -> Optional[IbisExpr]:
        """Builds an Ibis WHERE clause for cursor-based pagination."""
        if not spec.cursor or not spec.sort:
            return None

        sort_keys = spec.sort if isinstance(spec.sort, list) else [spec.sort]
        
        or_clauses = []

        for i in range(len(sort_keys)):
            current_key = sort_keys[i]
            field = current_key['field']
            order = current_key.get('order', 'asc').lower()
            
            and_clauses = []
            for j in range(i):
                prev_key = sort_keys[j]
                prev_field = prev_key['field']
                prev_val = spec.cursor.get(prev_field)
                if prev_field not in table.columns:
                    return None
                and_clauses.append(table[prev_field] == prev_val)

            if field not in table.columns:
                return None

            col_expr = table[field]
            cursor_value = spec.cursor.get(field)
            if order == 'asc':
                current_clause = col_expr > cursor_value
            else: # order == 'desc'
                current_clause = col_expr < cursor_value
            
            if and_clauses:
                full_clause = and_clauses[0]
                for clause in and_clauses[1:]:
                    full_clause &= clause
                full_clause &= current_clause
            else:
                full_clause = current_clause
            
            or_clauses.append(full_clause)

        if not or_clauses:
            return None
        
        combined_or_filter = or_clauses[0]
        for or_expr in or_clauses[1:]:
            combined_or_filter |= or_expr
        
        return combined_or_filter

    def build_measure_aggregation(self, table: IbisTable, measure: Measure) -> ibis.Scalar:
        """Converts a Measure object into an Ibis aggregation expression."""
        if not measure.alias:
            raise ValueError("Measure must include an alias")

        col = table[measure.field] if measure.field else None
        agg_type = (measure.agg or "sum").strip().lower()

        if measure.expression:
            raise ValueError(f"Custom Ibis expressions are not supported directly in measure '{measure.alias}'.")

        if measure.filter_condition:
            # Handle filtered measure by using Ibis 'where' on the column
            # This is equivalent to conditional aggregation (FILTER clause in SQL)
            if col is not None:
                # Need to convert dictionary filter to Ibis expression
                # This requires access to the table to build the filter
                filter_expr = self.build_filter_expression(table, [measure.filter_condition])
                if filter_expr is not None:
                    col = col.where(filter_expr)

        if agg_type == 'sum':
            return col.sum().name(measure.alias)
        elif agg_type == 'avg':
            return col.mean().name(measure.alias)
        elif agg_type == 'min':
            return col.min().name(measure.alias)
        elif agg_type == 'max':
            return col.max().name(measure.alias)
        elif agg_type == 'count':
            return col.count().name(measure.alias) if measure.field else table.count().name(measure.alias)
        elif agg_type in ['count_distinct', 'distinct_count']:
            return col.nunique().name(measure.alias)
        elif agg_type == 'stddev':
            return col.std().name(measure.alias)
        elif agg_type == 'variance':
            return col.var().name(measure.alias)
        elif agg_type == 'median':
            # Try exact median, then approximate, then quantile
            try:
                return col.median().name(measure.alias)
            except (AttributeError, NotImplementedError):
                try:
                    return col.approx_median().name(measure.alias)
                except (AttributeError, NotImplementedError):
                     return col.quantile(0.5).name(measure.alias)
        elif agg_type == 'percentile':
            if measure.percentile is None:
                raise ValueError("Percentile aggregation requires percentile parameter")
            p = float(measure.percentile)
            try:
                return col.quantile(p).name(measure.alias)
            except (AttributeError, NotImplementedError):
                try:
                    return col.approx_quantile(p).name(measure.alias)
                except (AttributeError, NotImplementedError):
                    raise ValueError(f"Percentile/Quantile not supported by backend for measure {measure.alias}")
        elif agg_type == 'string_agg':
            sep = measure.separator or ','
            return col.group_concat(sep).name(measure.alias)
        elif agg_type == 'array_agg':
            return col.collect().name(measure.alias)
        elif agg_type in {"first", "last"}:
            method = 'first' if agg_type == 'first' else 'last'
            return col.arbitrary(how=method).name(measure.alias)
        else:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")

    def build_aggregated_table(self, 
                               table: IbisTable,
                               group_cols: List[str],
                               measures: List[Measure],
                               sort_specs: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
                               limit: Optional[int] = None,
                               offset: Optional[int] = None) -> IbisTable:
        """
        Builds a standard aggregated table with grouping, sorting, and pagination
        """
        # Build aggregations
        aggregations = []
        for m in measures:
            agg_expr = self.build_measure_aggregation(table, m)
            aggregations.append(agg_expr)

        # Apply grouping and aggregation
        if group_cols:
            result_table = table.group_by(group_cols).aggregate(aggregations)
        else:
            result_table = table.aggregate(aggregations)

        # Apply sorting
        if sort_specs:
            ibis_sorts = self.build_sort_expressions(result_table, sort_specs)
            if ibis_sorts:
                result_table = result_table.order_by(ibis_sorts)

        # Apply limit and offset
        if limit:
            if offset:
                result_table = result_table.limit(limit, offset=offset)
            else:
                result_table = result_table.limit(limit)
        elif offset:
            # If only offset is provided, we need to limit to something large
            result_table = result_table.limit(1000000, offset=offset)  # Large default limit

        return result_table

    def build_base_table_with_filters(self, spec: PivotSpec) -> IbisTable:
        """
        Builds a base table with filters applied
        """
        base_table = self.backend.table(spec.table)

        # Apply filters
        filtered_table = base_table
        if spec.filters:
            filter_expr = self.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                filtered_table = filtered_table.filter(filter_expr)

        return filtered_table