"""
HierarchicalVirtualScrollManager - Optimized virtual scrolling for hierarchical data
"""
from typing import Dict, Any, List, Optional, Union
import pyarrow as pa
import pyarrow.compute as pc
import threading
import hashlib
import json

from pivot_engine.types.pivot_spec import PivotSpec
from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager
from pivot_engine.planner.ibis_planner import IbisPlanner


class HierarchicalVirtualScrollManager:
    def __init__(self, planner: IbisPlanner, cache, materialized_hierarchy_manager: MaterializedHierarchyManager, lock: Optional[threading.Lock] = None):
        self.planner = planner
        self.backend = planner.con  # Use the Ibis connection as the backend
        self.cache = cache
        self.materialized_hierarchy_manager = materialized_hierarchy_manager
        self.cache_ttl = 300  # 5 minutes default
        self._lock = lock if lock is not None else threading.RLock()

    def _debug_context(self, spec: PivotSpec, **extra) -> Dict[str, Any]:
        return {
            "table": spec.table,
            "rows": spec.rows,
            "cols": getattr(spec, "columns", []),
            **extra,
        }

    def _uses_duckdb(self) -> bool:
        return getattr(self.planner.con, "name", "").lower() == "duckdb"

    def _to_pyarrow(self, expr):
        if not self._uses_duckdb():
            return expr.to_pyarrow()
        with self._lock:
            return expr.to_pyarrow()

    def _execute_scalar(self, expr):
        if not self._uses_duckdb():
            return expr.execute()
        with self._lock:
            return expr.execute()

    def _split_filters(self, spec: PivotSpec) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Split filters into pre-aggregation (dimensions) and post-aggregation (measures)"""
        all_measures_aliases = [m.alias for m in spec.measures]
        pivot_col_values = [] # We don't have these here easily for generic check, but we can check suffixes
        
        pre_filters = []
        post_filters = []
        
        if spec.filters:
            for f in spec.filters:
                target_field = f.get('field')
                if not target_field and 'conditions' in f and len(f['conditions']) > 0:
                    target_field = f['conditions'][0].get('field')
                
                is_measure = target_field in all_measures_aliases
                # Check for pivoted measure format "Val_Alias" roughly if we can't be sure
                # Or just assume if it's not in rows/cols it might be measure?
                # Better: Check if it ends with a known measure alias
                if not is_measure:
                     is_measure = any(target_field.endswith(f"_{alias}") for alias in all_measures_aliases)
                
                if is_measure:
                    post_filters.append(f)
                else:
                    pre_filters.append(f)
        return pre_filters, post_filters

    def _get_spec_hash(self, spec: PivotSpec) -> str:
        """Generate a stable hash for the pivot spec (excluding pagination)"""
        spec_dict = spec.to_dict()
        # Exclude pagination/cursor from hash as it doesn't change the data structure
        key_parts = {k: v for k, v in spec_dict.items() if k not in ('limit', 'offset', 'cursor')}
        spec_str = json.dumps(key_parts, sort_keys=True, default=str)
        return hashlib.md5(spec_str.encode()).hexdigest()

    def _get_cache_key(self, spec: PivotSpec, offset: int, limit: int, expanded_paths: List[List[str]]):
        """Generate cache key for a hierarchical query."""
        spec_hash = self._get_spec_hash(spec)
        paths_str = str(sorted(map(str, expanded_paths))) if expanded_paths else ""
        paths_hash = hashlib.md5(paths_str.encode()).hexdigest()
        # Combine the four uniquely-identifying fields directly — no outer hash needed
        # since spec_hash and paths_hash are already fixed-length hex digests.
        return f"hier_scroll_v3:{spec_hash}:{paths_hash}:{offset}:{limit}"

    def get_visible_rows_hierarchical(self, spec: PivotSpec, start_row: int, end_row: int, expanded_paths: List[List[str]]):
        """Get hierarchical rows for virtual scrolling with expansion state"""
        # limit calculation fixed: end_row is inclusive
        limit = end_row - start_row + 1
        offset = start_row

        if limit <= 0:
            return []

        # 1. Cache Check (Outside Lock)
        cache_key = self._get_cache_key(spec, offset, limit, expanded_paths)
        cached_result = self.cache.get(cache_key)
        if cached_result:
            return self._format_for_ui(cached_result, expanded_paths, spec)

        # Check for full hierarchy cache (Optimization for Expand All)
        expand_all = False
        if expanded_paths:
            if expanded_paths == [['__ALL__']] or any(p == ['__ALL__'] for p in expanded_paths):
                expand_all = True
        
        spec_hash = self._get_spec_hash(spec)
        full_cache_key = f"hier_full:{spec_hash}"

        # Removed lock since we are now stateless (self.spec removed)
        # self.spec = spec # Removed shared state
        pivot_col_values = []
        
        # Split filters once for reuse
        pre_filters, post_filters = self._split_filters(spec)
        
        # If pivoting is requested (spec.columns is set), we proceed
        if spec.columns and len(spec.columns) > 0:
            top_n = spec.pivot_config.top_n if spec.pivot_config else 50
            col_order_measure = spec.measures[0] 
            
            try:
                # Only pass dimension filters (pre_filters) to column discovery
                col_query = self.planner._build_column_values_query(
                    spec.table, spec.columns, pre_filters, top_n, col_order_measure, None, spec.column_sort_options
                )
                col_data = self._to_pyarrow(col_query)
                pivot_col_values = col_data['_col_key'].to_pylist()
            except Exception as e:
                print(f"[WARN] Failed to discover pivot columns {self._debug_context(spec, offset=offset, limit=limit)}: {e}")
                pivot_col_values = []

        # --- Grand Total Placement Logic ---
        # Treat grand total as the final visible row, not the first.
        grand_total_table = None
        db_offset = offset
        db_limit = limit

        if spec.totals:
            total_visible = self.get_total_visible_row_count(spec, expanded_paths)
            grand_total_index = max(total_visible - 1, 0)
            includes_grand_total = offset <= grand_total_index < (offset + limit)

            if includes_grand_total:
                try:
                    grand_total_table = self._fetch_grand_total_pyarrow(spec, pivot_col_values)
                except Exception as e:
                    print(f"[WARN] Failed to fetch Grand Total {self._debug_context(spec, offset=offset, limit=limit, includes_grand_total=includes_grand_total)}: {e}")

            # Only reduce the data window if the grand total was successfully fetched.
            # If fetch failed (grand_total_table is None), keep full db_limit so data rows
            # are not silently dropped.
            if grand_total_table is not None:
                db_limit = max(0, min(limit, grand_total_index - offset))
                db_offset = min(offset, grand_total_index)
        
        # --- Fetch Data Rows ---
        result_table = None
        full_table = self.cache.get(full_cache_key) if expand_all else None
        
        if expand_all and not full_table and db_limit > 0:
            try:
                full_table = self._fetch_full_hierarchy_optimized(spec, pivot_col_values)
                if full_table:
                    self.cache.set(full_cache_key, full_table, ttl=self.cache_ttl)
            except Exception as e:
                print(f"[WARN] Optimized fetch failed: {e}. Falling back to standard query.")
        
        if full_table:
            if db_offset < full_table.num_rows:
                result_table = full_table.slice(db_offset, db_limit)
        elif db_limit > 0:
            ibis_expr = self._build_hierarchical_ibis_expr(spec, expanded_paths, db_offset, db_limit, pivot_col_values)
            if ibis_expr is not None:
                try:
                    result_table = self._to_pyarrow(ibis_expr)
                except Exception as e:
                    print(f"[ERROR] Query execution failed {self._debug_context(spec, offset=db_offset, limit=db_limit, expanded_paths=expanded_paths)}: {e}")
                    result_table = None

        # --- Combine Results ---
        final_table = result_table
        
        if grand_total_table:
            if final_table:
                import pyarrow as pa
                
                # Cleanup artifacts from Pandas conversion
                if '__index_level_0__' in final_table.column_names:
                    final_table = final_table.drop(['__index_level_0__'])

                # Check/Cast Schema
                if not final_table.schema.equals(grand_total_table.schema):
                    try:
                        # Promotes GT types (e.g. int64) to Data types (e.g. double)
                        grand_total_table = grand_total_table.cast(final_table.schema)
                    except Exception as e:
                        print(f"[WARN] Schema mismatch during concat: {e}")
                
                try:
                    final_table = pa.concat_tables([final_table, grand_total_table])
                except Exception as e:
                    print(f"[ERROR] Concatenation failed: {e}")
            else:
                final_table = grand_total_table

        if final_table:
            self.cache.set(cache_key, final_table, ttl=self.cache_ttl)

        if final_table:
            return self._format_for_ui(final_table, expanded_paths, spec)
        
        return []

    def _fetch_full_hierarchy_optimized(self, spec: PivotSpec, pivot_col_values: List[str] = None) -> Optional[pa.Table]:
        """Fetch ALL rows for the deepest level and reconstruct hierarchy in memory."""
        import pandas as pd
        import ibis
        
        con = self.planner.con
        source_table = con.table(spec.table)
        
        pre_filters, post_filters = self._split_filters(spec)
        
        if pre_filters:
            f_expr = self.planner.builder.build_filter_expression(source_table, pre_filters)
            if f_expr is not None:
                source_table = source_table.filter(f_expr)
                
        level_dims = spec.rows
        aggs = []
        is_pivot = bool(spec.columns)
        
        if is_pivot and pivot_col_values:
             if len(spec.columns) == 1:
                 match_col_expr = source_table[spec.columns[0]].cast('string')
             else:
                 match_col_expr = ibis.literal('')
                 for col_name in spec.columns:
                     match_col_expr = match_col_expr + source_table[col_name].cast('string') + ibis.literal('|')
                 match_col_expr = match_col_expr.substr(0, match_col_expr.length() - 1)

             base_measures = [m for m in spec.measures if not m.ratio_numerator]
             for val in pivot_col_values:
                 match_expr = match_col_expr == val
                 for m in base_measures:
                     col_expr = source_table[m.field]
                     agg_type = (m.agg or "sum").lower()
                     cond_col = match_expr.ifelse(col_expr, ibis.null())
                     alias = f"{val}_{m.alias}"
                     
                     if agg_type == 'sum': aggs.append(cond_col.sum().name(alias))
                     elif agg_type == 'mean' or agg_type == 'avg': aggs.append(cond_col.mean().name(alias))
                     elif agg_type == 'min': aggs.append(cond_col.min().name(alias))
                     elif agg_type == 'max': aggs.append(cond_col.max().name(alias))
                     elif agg_type == 'count': aggs.append(cond_col.count().name(alias))
                     elif agg_type == 'count_distinct': aggs.append(cond_col.nunique().name(alias))
             
             if spec.pivot_config.include_totals_column:
                 for m in base_measures:
                     col_expr = source_table[m.field]
                     agg_type = (m.agg or "sum").lower()
                     alias = f"__RowTotal__{m.alias}"
                     if agg_type == 'sum': aggs.append(col_expr.sum().name(alias))
                     elif agg_type == 'mean' or agg_type == 'avg': aggs.append(col_expr.mean().name(alias))
                     elif agg_type == 'min': aggs.append(col_expr.min().name(alias))
                     elif agg_type == 'max': aggs.append(col_expr.max().name(alias))
                     elif agg_type == 'count': aggs.append(col_expr.count().name(alias))
                     elif agg_type == 'count_distinct': aggs.append(col_expr.nunique().name(alias))
        else:
             aggs = [self.planner.builder.build_measure_aggregation(source_table, m) for m in spec.measures]

        query = source_table.group_by(level_dims).aggregate(aggs)
        
        # Apply Post-Aggregation Filters
        if post_filters:
            post_filter_expr = self.planner.builder.build_filter_expression(query, post_filters, is_post_agg=True)
            if post_filter_expr is not None:
                query = query.filter(post_filter_expr)

        arrow_table = self._to_pyarrow(query)
        df = arrow_table.to_pandas()
        
        if df.empty:
            return arrow_table
            
        dfs = [df]
        for l in range(len(spec.rows) - 1, 0, -1):
            parent_dims = spec.rows[:l]
            measure_cols = [c for c in df.columns if c not in spec.rows]
            parent_df = df.groupby(parent_dims)[measure_cols].sum().reset_index()
            for missing_dim in spec.rows[l:]:
                parent_df[missing_dim] = None
            dfs.append(parent_df)
            
        full_df = pd.concat(dfs, ignore_index=True)
        sort_fields = spec.rows[:1]
        ascending = [True] * len(sort_fields)

        if spec.sort:
            for sort_spec in (spec.sort if isinstance(spec.sort, list) else [spec.sort]):
                field = sort_spec.get('field')
                if field and field in full_df.columns and field not in sort_fields:
                    sort_fields.append(field)
                    ascending.append((sort_spec.get('order') or 'asc').lower() != 'desc')

        for dim in spec.rows[1:]:
            if dim not in sort_fields:
                sort_fields.append(dim)
                ascending.append(True)

        full_df = full_df.sort_values(by=sort_fields, ascending=ascending, na_position='first')
        
        for dim in spec.rows:
            full_df[dim] = full_df[dim].astype(str).replace('None', None).replace('nan', None)
            
        return pa.Table.from_pandas(full_df, preserve_index=False)

    def _fetch_grand_total_pyarrow(self, spec: PivotSpec, pivot_col_values: List[str] = None):
        """Fetch just the Grand Total row as a PyArrow table"""
        import ibis
        con = self.planner.con
        all_dims = spec.rows
        all_measures_aliases = [m.alias for m in spec.measures]
        
        pre_filters, _ = self._split_filters(spec)

        if pivot_col_values and len(pivot_col_values) > 0:
            source_table = con.table(spec.table)
            if pre_filters:
                f_expr = self.planner.builder.build_filter_expression(source_table, pre_filters)
                if f_expr is not None:
                    source_table = source_table.filter(f_expr)

            aggs = []
            if len(spec.columns) == 1:
                match_col_expr = source_table[spec.columns[0]].cast('string')
            else:
                match_col_expr = ibis.literal('')
                for col_name in spec.columns:
                    match_col_expr = match_col_expr + source_table[col_name].cast('string') + ibis.literal('|')
                match_col_expr = match_col_expr.substr(0, match_col_expr.length() - 1)

            base_measures = [m for m in spec.measures if not m.ratio_numerator]
            for val in pivot_col_values:
                match_expr = match_col_expr == val
                for m in base_measures:
                    col_expr = source_table[m.field]
                    agg_type = (m.agg or "sum").lower()
                    cond_col = match_expr.ifelse(col_expr, ibis.null())
                    alias = f"{val}_{m.alias}"
                    if agg_type == 'sum': aggs.append(cond_col.sum().name(alias))
                    elif agg_type == 'mean' or agg_type == 'avg': aggs.append(cond_col.mean().name(alias))
                    elif agg_type == 'min': aggs.append(cond_col.min().name(alias))
                    elif agg_type == 'max': aggs.append(cond_col.max().name(alias))
                    elif agg_type == 'count': aggs.append(cond_col.count().name(alias))
                    elif agg_type == 'count_distinct': aggs.append(cond_col.nunique().name(alias))

            if spec.pivot_config and spec.pivot_config.include_totals_column:
                for m in base_measures:
                    col_expr = source_table[m.field]
                    agg_type = (m.agg or "sum").lower()
                    alias = f"__RowTotal__{m.alias}"
                    if agg_type == 'sum': aggs.append(col_expr.sum().name(alias))
                    elif agg_type == 'mean' or agg_type == 'avg': aggs.append(col_expr.mean().name(alias))
                    elif agg_type == 'min': aggs.append(col_expr.min().name(alias))
                    elif agg_type == 'max': aggs.append(col_expr.max().name(alias))
                    elif agg_type == 'count': aggs.append(col_expr.count().name(alias))
                    elif agg_type == 'count_distinct': aggs.append(col_expr.nunique().name(alias))

            grand_total_row = source_table.aggregate(aggs)
            pivot_measures = []
            for val in pivot_col_values:
                for m_alias in all_measures_aliases:
                    pivot_measures.append(f"{val}_{m_alias}")
            if spec.pivot_config and spec.pivot_config.include_totals_column:
                 for m_alias in all_measures_aliases:
                     pivot_measures.append(f"__RowTotal__{m_alias}")
            
            gt_projection = []
            for dim in all_dims:
                gt_projection.append(ibis.literal(None, type='string').name(dim))
            for m_alias in pivot_measures:
                if m_alias in grand_total_row.columns:
                    gt_projection.append(grand_total_row[m_alias])
                else:
                    gt_projection.append(ibis.literal(None, type='float64').name(m_alias))

            return self._to_pyarrow(grand_total_row.select(gt_projection))

        source_table = con.table(spec.table)
        if pre_filters:
             f_expr = self.planner.builder.build_filter_expression(source_table, pre_filters)
             if f_expr is not None:
                 source_table = source_table.filter(f_expr)

        grand_total_aggs = [
            self.planner.builder.build_measure_aggregation(source_table, m).name(m.alias)
            for m in spec.measures
            if not m.ratio_numerator
        ]
        if not grand_total_aggs:
            return None

        grand_total_row = source_table.aggregate(grand_total_aggs)
        gt_projection = [ibis.literal(None, type='string').name(dim) for dim in all_dims]
        for m in all_measures_aliases:
            gt_projection.append(grand_total_row[m])

        return self._to_pyarrow(grand_total_row.select(gt_projection))

    def get_total_visible_row_count(self, spec: PivotSpec, expanded_paths: List[List[str]]) -> int:
        """Calculate total number of visible rows for the scrollbar."""
        spec_hash = self._get_spec_hash(spec)
        paths_str = str(sorted(map(str, expanded_paths))) if expanded_paths else ""
        cache_key = f"hier_count:{spec_hash}:{hashlib.md5(paths_str.encode()).hexdigest()}"
        
        cached_count = self.cache.get(cache_key)
        if cached_count is not None:
            return cached_count

        with self._lock:
            total_count = 0
            if spec.totals:
                total_count += 1
            
            level_1_table_name = self.materialized_hierarchy_manager.get_rollup_table_name(spec, 1)
            level_1_success = False

            pre_filters, post_filters = self._split_filters(spec)

            # Only use rollup table when there are no post-filters (measure HAVING filters),
            # because rollup tables may not have the aggregated measure columns needed to
            # evaluate those filters accurately.
            if level_1_table_name and self.backend is not None and not post_filters:
                try:
                     level_1_table = self.planner.con.table(level_1_table_name)
                     if pre_filters:
                         f_expr = self.planner.builder.build_filter_expression(level_1_table, pre_filters)
                         if f_expr is not None: level_1_table = level_1_table.filter(f_expr)

                     total_count += self._execute_scalar(level_1_table.count())
                     level_1_success = True
                except Exception as e:
                    # print(f"[WARN] Error counting rollup: {e}")
                    pass

            if not level_1_success:
                 try:
                     base_table = self.planner.con.table(spec.table)
                     if pre_filters:
                         f_expr = self.planner.builder.build_filter_expression(base_table, pre_filters)
                         if f_expr is not None:
                             base_table = base_table.filter(f_expr)
                     dims = spec.rows[:1]
                     if dims:
                         agg_query = base_table.group_by(dims).aggregate(
                             [self.planner.builder.build_measure_aggregation(base_table, m).name(m.alias)
                              for m in spec.measures if not m.ratio_numerator] or [base_table[dims[0]].count().name('__cnt')]
                         )
                         if post_filters:
                             pf_expr = self.planner.builder.build_filter_expression(agg_query, post_filters, is_post_agg=True)
                             if pf_expr is not None:
                                 agg_query = agg_query.filter(pf_expr)
                         total_count += self._execute_scalar(agg_query.count())
                     else:
                         total_count += self._execute_scalar(base_table.count())
                 except Exception as e:
                    print(f"[ERROR] Error calculating count for level 1 {self._debug_context(spec, expanded_paths=expanded_paths)}: {e}")

            valid_expanded_paths = [p for p in expanded_paths if p and p != ['__ALL__']]
            expand_all = [['__ALL__']] in expanded_paths or expanded_paths == [['__ALL__']]
            
            if expand_all:
                 for lv in range(2, len(spec.rows) + 1):
                     rollup_table_name = self.materialized_hierarchy_manager.get_rollup_table_name(spec, lv)
                     try:
                         if rollup_table_name:
                             rollup_table = self.planner.con.table(rollup_table_name)
                             if pre_filters:
                                 f_expr = self.planner.builder.build_filter_expression(rollup_table, pre_filters)
                                 if f_expr is not None: rollup_table = rollup_table.filter(f_expr)
                             total_count += self._execute_scalar(rollup_table.count())
                         else:
                             base_table = self.planner.con.table(spec.table)
                             if pre_filters:
                                 f_expr = self.planner.builder.build_filter_expression(base_table, pre_filters)
                                 if f_expr is not None:
                                     base_table = base_table.filter(f_expr)
                             dims = spec.rows[:lv]
                             if dims: total_count += self._execute_scalar(base_table.group_by(dims).count().count())
                     except Exception as e:
                         print(f"[ERROR] Error calculating count for level {lv} {self._debug_context(spec, expanded_paths=expanded_paths)}: {e}")

            elif valid_expanded_paths and self.backend is not None:
                 paths_by_level = {}
                 for path in valid_expanded_paths:
                     level = len(path) + 1
                     if level <= len(spec.rows):
                         if level not in paths_by_level: paths_by_level[level] = []
                         paths_by_level[level].append(path)
                
                 for level, paths in paths_by_level.items():
                     rollup_table_name = self.materialized_hierarchy_manager.get_rollup_table_name(spec, level)
                     rollup_success = False
                     if rollup_table_name:
                         try:
                             table_to_query = self.planner.con.table(rollup_table_name)
                             if pre_filters:
                                 f_expr = self.planner.builder.build_filter_expression(table_to_query, pre_filters)
                                 if f_expr is not None: table_to_query = table_to_query.filter(f_expr)
                                 
                             parent_dims = spec.rows[:level-1]
                             or_expr = None
                             for path in paths:
                                 and_expr = None
                                 for dim, val in zip(parent_dims, path):
                                     clause = table_to_query[dim].cast('string') == str(val)
                                     and_expr = clause if and_expr is None else (and_expr & clause)
                                 or_expr = and_expr if or_expr is None else (or_expr | and_expr)
                             if or_expr is not None:
                                 total_count += self._execute_scalar(table_to_query.filter(or_expr).count())
                                 rollup_success = True
                         except Exception as e:
                             # print(f"[WARN] Rollup batch count error: {e}")
                             pass

                     if not rollup_success:
                         try:
                             table_to_query = self.planner.con.table(spec.table)
                             if pre_filters:
                                 f_expr = self.planner.builder.build_filter_expression(table_to_query, pre_filters)
                                 if f_expr is not None: table_to_query = table_to_query.filter(f_expr)
                             parent_dims = spec.rows[:level-1]
                             or_expr = None
                             for path in paths:
                                 and_expr = None
                                 for dim, val in zip(parent_dims, path):
                                     clause = table_to_query[dim].cast('string') == str(val)
                                     and_expr = clause if and_expr is None else (and_expr & clause)
                                 or_expr = and_expr if or_expr is None else (or_expr | and_expr)
                             if or_expr is not None:
                                 filtered_table = table_to_query.filter(or_expr)
                                 dims = spec.rows[:level]
                                 if dims: total_count += self._execute_scalar(filtered_table.group_by(dims).count().count())
                         except Exception as e:
                             print(f"[ERROR] Base batch count error {self._debug_context(spec, level=level, expanded_paths=paths)}: {e}")

            self.cache.set(cache_key, total_count, ttl=self.cache_ttl)
            return total_count

    def _build_hierarchical_ibis_expr(self, spec: PivotSpec, expanded_paths: List[List[str]], offset: int, limit: int, pivot_col_values: List[str] = None):
        """Build a query for a tile of hierarchical data using Ibis expressions with batched optimization."""
        import ibis
        import functools
        import operator
        
        union_expressions = []
        con = self.planner.con
        all_dims = spec.rows
        column_sort_options = spec.column_sort_options or {}
        hidden_sort_fields = []
        for dim in all_dims:
            dim_opts = column_sort_options.get(dim) if isinstance(column_sort_options, dict) else None
            if not isinstance(dim_opts, dict):
                continue
            sort_type = str(dim_opts.get("sortType") or "").strip().lower()
            sort_key_field = dim_opts.get("sortKeyField")
            if (
                sort_type == "curve_pillar_tenor"
                and isinstance(sort_key_field, str)
                and sort_key_field
                and sort_key_field not in hidden_sort_fields
            ):
                hidden_sort_fields.append(sort_key_field)
        # Initial standard measures
        all_measures_aliases = [m.alias for m in spec.measures]

        # Determine if we are pivoting
        is_pivot = bool(spec.columns)
        if pivot_col_values is None:
            pivot_col_values = []
        
        if is_pivot:
            # Rebuild measure aliases to include pivoted columns
            pivot_measures = []
            for val in pivot_col_values:
                for m_alias in all_measures_aliases:
                    pivot_measures.append(f"{val}_{m_alias}")
            
            if spec.pivot_config and spec.pivot_config.include_totals_column:
                 for m_alias in all_measures_aliases:
                     pivot_measures.append(f"__RowTotal__{m_alias}")
            
            all_measures_aliases = pivot_measures

        # Split filters into pre-aggregation (dimensions) and post-aggregation (measures)
        pre_filters, post_filters = self._split_filters(spec)

        # Helper to build aggregation for a specific level with BATCHED paths
        def build_level_query_batched(level, paths=None):
            source_table = None
            if not is_pivot:
                rollup_name = self.materialized_hierarchy_manager.get_rollup_table_name(spec, level)
                if rollup_name:
                    source_table = con.table(rollup_name)
            
            is_using_base = False
            if source_table is None:
                source_table = con.table(spec.table)
                is_using_base = True
                # Apply Pre-Filters (Dimensions) to Base Table
                if pre_filters:
                    f_expr = self.planner.builder.build_filter_expression(source_table, pre_filters)
                    if f_expr is not None:
                        source_table = source_table.filter(f_expr)
            else:
                # Apply Pre-Filters to Rollup Table as well (if columns exist)
                if pre_filters:
                    f_expr = self.planner.builder.build_filter_expression(source_table, pre_filters)
                    if f_expr is not None:
                        source_table = source_table.filter(f_expr)

            # 3. Apply Batched Path Filters (Drill-down paths)
            if paths:
                parent_dims = all_dims[:level-1]
                
                # Optimization: If only 1 parent dim, use ISIN
                if len(parent_dims) == 1:
                    dim = parent_dims[0]
                    vals = [p[0] for p in paths]
                    source_table = source_table.filter(source_table[dim].cast('string').isin(vals))
                elif len(parent_dims) > 1:
                    or_conditions = []
                    for path in paths:
                        conditions = []
                        for dim, val in zip(parent_dims, path):
                            conditions.append(source_table[dim].cast('string') == val)
                        
                        if conditions:
                            and_expr = functools.reduce(operator.and_, conditions)
                            or_conditions.append(and_expr)
                    
                    if or_conditions:
                        or_expr = functools.reduce(operator.or_, or_conditions)
                        source_table = source_table.filter(or_expr)

            # 4. Construct Aggregation
            level_dims = all_dims[:level]
            
            query = None
            
            if is_pivot:
                # Pivot Aggregation Logic
                aggs = []
                if len(spec.columns) == 1:
                    match_col_expr = source_table[spec.columns[0]].cast('string')
                else:
                    match_col_expr = ibis.literal('')
                    for col_name in spec.columns:
                        match_col_expr = match_col_expr + source_table[col_name].cast('string') + ibis.literal('|')
                    match_col_expr = match_col_expr.substr(0, match_col_expr.length() - 1)

                base_measures = [m for m in spec.measures if not m.ratio_numerator]
                for val in pivot_col_values:
                    match_expr = match_col_expr == val
                    for m in base_measures:
                        col_expr = source_table[m.field]
                        agg_type = (m.agg or "sum").lower()
                        cond_col = match_expr.ifelse(col_expr, ibis.null())
                        alias = f"{val}_{m.alias}"
                        if agg_type == 'sum': aggs.append(cond_col.sum().name(alias))
                        elif agg_type == 'mean' or agg_type == 'avg': aggs.append(cond_col.mean().name(alias))
                        elif agg_type == 'min': aggs.append(cond_col.min().name(alias))
                        elif agg_type == 'max': aggs.append(cond_col.max().name(alias))
                        elif agg_type == 'count': aggs.append(cond_col.count().name(alias))
                        elif agg_type == 'count_distinct': aggs.append(cond_col.nunique().name(alias))

                if spec.pivot_config and spec.pivot_config.include_totals_column:
                    for m in base_measures:
                        col_expr = source_table[m.field]
                        agg_type = (m.agg or "sum").lower()
                        alias = f"__RowTotal__{m.alias}"
                        if agg_type == 'sum': aggs.append(col_expr.sum().name(alias))
                        elif agg_type == 'mean' or agg_type == 'avg': aggs.append(col_expr.mean().name(alias))
                        elif agg_type == 'min': aggs.append(col_expr.min().name(alias))
                        elif agg_type == 'max': aggs.append(col_expr.max().name(alias))
                        elif agg_type == 'count': aggs.append(col_expr.count().name(alias))
                        elif agg_type == 'count_distinct': aggs.append(col_expr.nunique().name(alias))

                for sort_key_field in hidden_sort_fields:
                    if sort_key_field in source_table.columns:
                        aggs.append(source_table[sort_key_field].min().name(sort_key_field))

                if level_dims:
                    query = source_table.group_by(level_dims).aggregate(aggs)
                else:
                    query = source_table.aggregate(aggs)

            else:
                # Standard Aggregation Logic
                if is_using_base:
                    aggs = [self.planner.builder.build_measure_aggregation(source_table, m) for m in spec.measures]
                    for sort_key_field in hidden_sort_fields:
                        if sort_key_field in source_table.columns:
                            aggs.append(source_table[sort_key_field].min().name(sort_key_field))
                    if level_dims:
                        query = source_table.group_by(level_dims).aggregate(aggs)
                    else:
                        query = source_table.aggregate(aggs)
                else:
                    # Already aggregated in rollup
                    query = source_table

            # Apply Post-Aggregation Filters (Measure Filters) to the Result Query
            if post_filters:
                post_filter_expr = self.planner.builder.build_filter_expression(query, post_filters, is_post_agg=True)
                if post_filter_expr is not None:
                    query = query.filter(post_filter_expr)

            # 5. Projection to align schemas
            projection = []
            for i, dim in enumerate(all_dims):
                if i < level:
                    projection.append(query[dim].cast('string').name(dim))
                else:
                    projection.append(ibis.literal(None, type='string').name(dim))

            for m_alias in all_measures_aliases:
                if m_alias in query.columns:
                    projection.append(query[m_alias])
                else:
                    projection.append(ibis.literal(None, type='float64').name(m_alias))

            for sort_key_field in hidden_sort_fields:
                if sort_key_field in query.columns:
                    projection.append(query[sort_key_field])
                else:
                    projection.append(ibis.literal(None, type='float64').name(sort_key_field))

            return query.select(projection)

        # 1. Level 1 (Roots) is always fetched
        l1_query = build_level_query_batched(1)
        if l1_query is not None: union_expressions.append(l1_query)

        valid_expanded_paths = [p for p in expanded_paths if p and p != ['__ALL__']]
        expand_all = [['__ALL__']] in expanded_paths or expanded_paths == [['__ALL__']]
        
        if expand_all:
            # If expand all, fetch all levels fully
            for lv in range(2, len(all_dims) + 1):
                q = build_level_query_batched(lv)
                if q is not None: union_expressions.append(q)
        else:
            # Batch expanded paths by level
            # Paths in valid_expanded_paths are PARENT paths.
            # We want to fetch their CHILDREN.
            # Child level = len(parent_path) + 1
            
            paths_by_level = {}
            for path in valid_expanded_paths:
                child_level = len(path) + 1
                if child_level <= len(all_dims):
                    if child_level not in paths_by_level:
                        paths_by_level[child_level] = []
                    paths_by_level[child_level].append(path)
            
            # Generate one query per level
            for level, paths in paths_by_level.items():
                q = build_level_query_batched(level, paths)
                if q is not None: union_expressions.append(q)

        if not union_expressions: return None
        final_expr = union_expressions[0] if len(union_expressions) == 1 else ibis.union(*union_expressions)

        # Hierarchical sort: for each dimension level, sort by dim[0..i] values then put
        # the parent row (dim[i+1] IS NULL) before its children (dim[i+1] IS NOT NULL).
        # Pattern per level i:
        #   dim[0] ASC, ..., dim[i-1] ASC,
        #   (dim[i] IS NULL) DESC  -- NULL rows (parents at level i) come first
        #   dim[i] ASC             -- then alphabetical within the level
        # User sort overrides the leaf-level dimension sort only.
        user_sort_field = None
        user_sort_desc = False
        user_sort_spec = {}
        if spec.sort:
            sort_specs = spec.sort if isinstance(spec.sort, list) else [spec.sort]
            if sort_specs:
                user_sort_spec = sort_specs[0] if isinstance(sort_specs[0], dict) else {}
                user_sort_field = user_sort_spec.get('field')
                user_sort_desc = (user_sort_spec.get('order') or 'asc').lower() == 'desc'

        order_by_cols = []
        available = set(final_expr.columns)
        for i, dim in enumerate(all_dims):
            if dim not in available:
                continue
            # Parent-before-children: rows where the NEXT dim is NULL are parents at this level
            if i + 1 < len(all_dims) and all_dims[i + 1] in available:
                next_col = final_expr[all_dims[i + 1]]
                # NULL next-dim means this row is the parent -> sort it first (DESC on IS NULL)
                order_by_cols.append(next_col.isnull().desc())

            dim_sort = {
                "field": dim,
                "order": "desc" if (user_sort_field == dim and user_sort_desc) else "asc",
            }
            dim_opts = column_sort_options.get(dim) if isinstance(column_sort_options, dict) else None
            if isinstance(dim_opts, dict):
                for key in ("sortType", "sortKeyField", "semanticType", "sortSemantic", "nulls", "absoluteSort"):
                    if key in dim_opts and dim_opts.get(key) is not None:
                        dim_sort[key] = dim_opts.get(key)
            if user_sort_field == dim and isinstance(user_sort_spec, dict):
                for key in ("sortType", "sortKeyField", "semanticType", "sortSemantic", "nulls", "absoluteSort"):
                    if key in user_sort_spec and user_sort_spec.get(key) is not None:
                        dim_sort[key] = user_sort_spec.get(key)

            dim_sort_exprs = self.planner.builder.build_sort_expressions(final_expr, [dim_sort])
            if dim_sort_exprs:
                order_by_cols.extend(dim_sort_exprs)
            else:
                col = final_expr[dim]
                if user_sort_field == dim and user_sort_desc:
                    order_by_cols.append(col.desc())
                else:
                    order_by_cols.append(col.isnull().desc())
                    order_by_cols.append(col.asc())

        if not order_by_cols:
            order_by_cols = []
            for dim in all_dims:
                if dim not in available:
                    continue
                col = final_expr[dim]
                order_by_cols.append(col.isnull().desc())
                order_by_cols.append(col.asc())

        return final_expr.order_by(order_by_cols).limit(limit, offset=offset)
    
    def _format_for_ui(self, data, expanded_paths: List[List[str]], spec: PivotSpec):
        """Format data for UI consumption using Arrow Compute for performance"""
        if data is None or data.num_rows == 0:
            return []
            
        # Ensure we work with Arrow Table
        if not isinstance(data, pa.Table):
            return []

        # 1. Prepare Columns
        dims = spec.rows
        
        # Cast dims to string for path construction
        str_cols = []
        for dim in dims:
            if dim in data.column_names:
                str_cols.append(data[dim].cast('string'))
            else:
                str_cols.append(pa.array([None] * data.num_rows, type='string'))

        # 2. Path Construction (||| joined)
        # Preserve one path entry per row, including all-null grand total rows.
        if str_cols:
            str_cols_py = [col.to_pylist() for col in str_cols]
            path_values = []
            for row_vals in zip(*str_cols_py):
                parts = [value for value in row_vals if value is not None]
                path_values.append("|||".join(parts))
            path_col = pa.array(path_values, type=pa.string())
        else:
            path_col = pa.array([""] * data.num_rows, type=pa.string())

        # 3. Depth Calculation
        # Count non-nulls
        depth_arrays = [pc.if_else(pc.is_valid(c), 1, 0) for c in str_cols]
        import functools
        if depth_arrays:
            raw_depth_col = functools.reduce(pc.add, depth_arrays)
        else:
            raw_depth_col = pa.array([0] * data.num_rows)

        # 4. ID Calculation (Deepest Value)
        # Coalesce selects first non-null. Reverse dims to get deepest.
        if str_cols:
            id_col = pc.coalesce(*reversed(str_cols))
        else:
            id_col = pa.array(["Grand Total"] * data.num_rows)

        # 5. UI Depth
        # depth = raw_depth if spec.totals else max(0, raw_depth - 1)
        if spec.totals:
            ui_depth_col = raw_depth_col
        else:
            # subtract 1, clamp to 0
            sub_1 = pc.subtract(raw_depth_col, 1)
            ui_depth_col = pc.if_else(pc.less(sub_1, 0), 0, sub_1)

        # 6. Expansion State
        expand_all = False
        expanded_set = set()
        if expanded_paths:
            if expanded_paths == [['__ALL__']] or any(p == ['__ALL__'] for p in expanded_paths): expand_all = True
            else:
                for p in expanded_paths: expanded_set.add("|||".join([str(x) for x in p]))
        
        if expand_all:
            is_expanded_col = pa.array([True] * data.num_rows)
        else:
            # Check if path is in expanded_set
            # is_in expects value_set.
            if expanded_set:
                is_expanded_col = pc.is_in(path_col, value_set=pa.array(list(expanded_set)))
            else:
                is_expanded_col = pa.array([False] * data.num_rows)

        # 7. Has Children
        # raw_depth < len(dims)
        has_children_col = pc.less(raw_depth_col, len(dims))

        # 8. Grand Total Handling
        # If raw_depth == 0 (and spec.totals is True logic handled implicitly by depth?)
        # We need to set _id="Grand Total" if raw_depth == 0
        is_gt = pc.equal(raw_depth_col, 0)
        final_id_col = pc.if_else(is_gt, "Grand Total", id_col)
        final_path_col = pc.if_else(is_gt, "__grand_total__", path_col)
        
        # Add Columns to Table
        # We use a dictionary to collect new columns then append
        
        # We can't append easily to Table without creating new one or using combine_chunks
        # Or converting to Pandas? No, we want to stay in Arrow.
        # Table.append_column returns new table.
        
        result_table = data.append_column('_path', final_path_col) \
                           .append_column('_depth', ui_depth_col) \
                           .append_column('depth', ui_depth_col) \
                           .append_column('_id', final_id_col) \
                           .append_column('_is_expanded', is_expanded_col) \
                           .append_column('_has_children', has_children_col)
        
        if spec.totals:
             # Add _isTotal flag
             result_table = result_table.append_column('_isTotal', is_gt)

        # Final conversion to list of dicts (fastest way from Arrow)
        return result_table.to_pylist()

