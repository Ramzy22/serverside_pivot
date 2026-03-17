"""
Unified Ibis Expression Builder module to eliminate duplication
between IbisPlanner, ProgressiveDataLoader, and HierarchicalVirtualScrollManager
"""
from typing import List, Dict, Any, Optional, Union
import ibis
from ibis import BaseBackend as IbisBaseBackend
from ibis.expr.api import Table as IbisTable, Expr as IbisExpr, Column as IbisColumn
from pivot_engine.types.pivot_spec import PivotSpec, Measure


class IbisExpressionBuilder:
    """
    Unified class for building Ibis expressions across all components
    """
    
    def __init__(self, backend: IbisBaseBackend):
        self.backend = backend

    def build_filter_expression(self, table: IbisTable, filters: List[Dict[str, Any]], is_post_agg: bool = False) -> Optional[IbisExpr]:
        """Converts a list of filter dictionaries into a single Ibis boolean expression."""
        if not filters:
            return None

        all_expressions = []

        for f in filters:
            # Case 1: This is a composite filter object like {op: 'AND', conditions: [...]}
            if ('op' in f or 'operator' in f) and 'conditions' in f:
                sub_expressions = []
                for sub_cond in f.get('conditions', []):
                    field = sub_cond.get("field")
                    if not field:
                        continue
                    
                    if field not in table.columns:
                        if not is_post_agg:
                            print(f"Warning: Filter field '{field}' not found in table columns during sub-expression build.")
                            continue
                        # For post-agg, we assume the field exists in the aggregated table schema
                        # passed in, or we build a dummy expression if needed, but usually 'table'
                        # here IS the aggregated table for HAVING clause.
                        # If it's truly not in the table, it will fail later, but we shouldn't block it here if is_post_agg is True.
                    
                    expr = self._build_single_filter(
                        table[field],
                        sub_cond.get('op', '='),
                        sub_cond.get('value'),
                        sub_cond.get('caseSensitive', False)
                    )
                    if expr is not None:
                        sub_expressions.append(expr)
                
                if not sub_expressions:
                    continue

                # Combine the sub-expressions with AND or OR
                combined_sub = sub_expressions[0]
                op = (f.get('op') or f.get('operator')).upper()
                if op == 'AND':
                    for expr in sub_expressions[1:]:
                        combined_sub &= expr
                else: # OR
                    for expr in sub_expressions[1:]:
                        combined_sub |= expr
                all_expressions.append(combined_sub)

            # Case 2: This is a simple filter object like {field: ..., op: ..., value: ...}
            elif 'field' in f:
                field = f.get("field")
                if not field or field not in table.columns:
                    if not is_post_agg:
                         print(f"Warning: Filter field '{field}' not found in table columns.")
                    continue

                expr = self._build_single_filter(
                    table[field],
                    f.get('op', '='),
                    f.get('value'),
                    f.get('caseSensitive', False)
                )
                if expr is not None:
                    all_expressions.append(expr)
            else:
                print(f"Warning: Malformed filter object skipped: {f}")


        if not all_expressions:
            return None

        # AND all the top-level expressions together into a single boolean expression
        final_expression = all_expressions[0]
        for expr in all_expressions[1:]:
            final_expression &= expr
            
        return final_expression

    def _build_single_filter(self, col: IbisColumn, op: str, value: Any, case_sensitive: bool = False) -> Optional[IbisExpr]:
        """Internal helper to build a single condition for a column."""
        op = op.lower()
        
        # Helper to cast value to column type for strict comparisons
        def cast_val(v, c):
            if v is None: return v
            try:
                col_type = c.type()
                
                # Integer handling
                if col_type.is_integer():
                    if isinstance(v, (int, float)):
                        return int(v)
                    if isinstance(v, str) and v.strip():
                        # Handle "123.0" -> 123
                        try:
                            return int(float(v))
                        except:
                            return int(v)
                
                # Float/Double/Decimal handling
                elif col_type.is_floating() or col_type.is_decimal():
                    if isinstance(v, (int, float, str)):
                        return float(v)
                
                # Boolean handling
                elif col_type.is_boolean():
                    if isinstance(v, str):
                        return v.lower() in ('true', '1', 't', 'yes', 'y')
                    return bool(v)
                
                # String handling (ensure string)
                elif col_type.is_string() and not isinstance(v, str):
                    return str(v)
                
                # Date/Time handling (let Ibis handle string parsing, but ensure string)
                elif (col_type.is_date() or col_type.is_timestamp()) and not isinstance(v, (str, int, float)):
                     # If it's a python date/datetime object, it's fine.
                     pass

            except Exception:
                # If casting fails, return original value and let backend complain or handle it
                pass
            return v

        if op in ["=", "eq"]:
            val = cast_val(value, col)
            if not case_sensitive and isinstance(value, str):
                try:
                    if col.type().is_string():
                        return col.lower() == str(val).lower()
                except: pass
            return col == val

        if op in ["!=", "ne"]:
            val = cast_val(value, col)
            if not case_sensitive and isinstance(value, str):
                try:
                    if col.type().is_string():
                        return col.lower() != str(val).lower()
                except: pass
            return col != val

        if op in ["<", "lt"]:
            return col < cast_val(value, col)
        if op in ["<=", "lte"]:
            return col <= cast_val(value, col)
        if op in [">", "gt"]:
            return col > cast_val(value, col)
        if op in [">=", "gte"]:
            return col >= cast_val(value, col)
        
        # String/Array operations
        if op == "in":
            if isinstance(value, (list, tuple, set)):
                vals = [cast_val(v, col) for v in value]
                if not case_sensitive:
                    try:
                        if col.type().is_string():
                            # For case-insensitive IN, we can use lower() on col and values
                            return col.lower().isin([str(v).lower() for v in vals])
                    except: pass
                return col.isin(vals)
            return col == cast_val(value, col) # Fallback for single value

        if op == "between":
            if isinstance(value, (list, tuple)) and len(value) == 2:
                return col.between(cast_val(value[0], col), cast_val(value[1], col))
        
        # All subsequent operations require casting to string
        str_col = col.cast('string')
        
        if op == "like":
            return str_col.like(value) if case_sensitive else str_col.ilike(value)
        if op == "ilike":
            return str_col.ilike(value) # Always insensitive
        
        if op == "starts_with":
            pat = f"{value}%"
            return str_col.like(pat) if case_sensitive else str_col.ilike(pat)
        if op == "ends_with":
            pat = f"%{value}"
            return str_col.like(pat) if case_sensitive else str_col.ilike(pat)
        if op == "contains":
            pat = f"%{value}%"
            return str_col.like(pat) if case_sensitive else str_col.ilike(pat)
        
        # Null checks
        if op == "is null":
            return col.isnull()
        if op == "is not null":
            return col.notnull()
        
        return None

    def build_sort_expressions(self, table: IbisTable, sort_specs: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Union[ibis.Column, ibis.Expr]]:
        """Converts sort specifications to a list of Ibis sort expressions."""
        if not sort_specs:
            return []

        sort_list = [sort_specs] if isinstance(sort_specs, dict) else sort_specs
        ibis_sorts = []

        for s in sort_list:
            field = s.get("field")
            if not field:
                continue

            sort_type = str(s.get("sortType") or "").strip().lower()
            sort_key_field = s.get("sortKeyField")
            use_hidden_sort_key = (
                isinstance(sort_key_field, str)
                and sort_key_field
                and sort_key_field in table.columns
            )
            effective_field = sort_key_field if use_hidden_sort_key else field
            if effective_field not in table.columns:
                if field not in table.columns:
                    continue
                effective_field = field
                use_hidden_sort_key = False

            order = (s.get("order") or "asc").lower()
            nulls = (s.get("nulls") or "").lower()
            semantic_type = str(
                s.get("semanticType") or s.get("semantic") or s.get("sortSemantic") or ""
            ).lower()

            col = table[effective_field]
            sort_expr = None

            if semantic_type == "tenor" and not use_hidden_sort_key:
                # Parse text values like 1D, 2W, 1M, 6Y into a numeric key.
                # Valid tenor values are always ordered before non-parsable values.
                tenor_text = col.cast("string")
                pattern = r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z])\s*$"
                numeric_text = tenor_text.re_extract(pattern, 1)
                unit_text = tenor_text.re_extract(pattern, 2).lower()
                numeric_value = numeric_text.nullif("").cast("float64")
                unit_factor = (unit_text == "d").ifelse(
                    1.0,
                    (unit_text == "w").ifelse(
                        7.0,
                        (unit_text == "m").ifelse(
                            30.4375,
                            (unit_text == "y").ifelse(365.25, ibis.null()),
                        ),
                    ),
                )
                tenor_key = numeric_value * unit_factor
                is_valid_tenor = numeric_value.notnull() & unit_factor.notnull()

                ibis_sorts.append(is_valid_tenor.desc())
                ibis_sorts.append(tenor_key.asc() if order == "asc" else tenor_key.desc())
                ibis_sorts.append(tenor_text.asc() if order == "asc" else tenor_text.desc())
                continue

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

        if agg_type in {"weighted_avg", "wavg", "weighted_mean"}:
            if col is None:
                raise ValueError(f"Weighted average requires a value field for measure '{measure.alias}'")
            if not measure.weighted_field:
                raise ValueError(
                    f"Weighted average measure '{measure.alias}' requires 'weighted_field' (or valConfig weightField)"
                )
            if measure.weighted_field not in table.columns:
                raise ValueError(
                    f"Weighted average measure '{measure.alias}' references unknown weight field '{measure.weighted_field}'"
                )

            weight_col = table[measure.weighted_field]
            if measure.filter_condition:
                filter_expr = self.build_filter_expression(table, [measure.filter_condition])
                if filter_expr is not None:
                    weight_col = weight_col.where(filter_expr)

            # Ignore rows where value or weight is null.
            valid_mask = col.notnull() & weight_col.notnull()
            weighted_sum = (col * weight_col).where(valid_mask).sum()
            total_weight = weight_col.where(valid_mask).sum()
            return (weighted_sum / total_weight.nullif(0)).name(measure.alias)

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
