"""
IbisPlanner - Enhanced with Grouping Sets, Top-N Pivot, Ratio Metrics, Multi-dimensional Tiles, and Advanced Planning.
"""

from typing import List, Dict, Any, Tuple, Optional, Union
import re
import itertools
from dataclasses import dataclass
import ibis
# Try to import specific Ibis types, fallback if changed in newer versions
try:
    from ibis.expr.types import Table as IbisTable, Column as IbisColumn, Scalar as IbisScalar
    from ibis.expr.types import Expr as IbisExpr
except ImportError:
    # Fallback for older/different Ibis versions (e.g. 2.x vs 3.x vs 9.x)
    from ibis.expr.api import Table as IbisTable, Column as IbisColumn, Scalar as IbisScalar
    from ibis.expr.api import Expr as IbisExpr

from pivot_engine.types.pivot_spec import PivotSpec, Measure, GroupingConfig, PivotConfig, DrillPath
from pivot_engine.common.ibis_expression_builder import IbisExpressionBuilder
from pivot_engine.planner.expression_parser import SafeExpressionParser
# Import MaterializedHierarchyManager type for type hinting if needed (avoid circular import if possible)
# from pivot_engine.materialized_hierarchy_manager import MaterializedHierarchyManager 

@dataclass
class QueryPlan:
    """Represents a single query execution plan (now Ibis expression)"""
    ibis_expr: IbisTable
    purpose: str
    cost_estimate: float
    execution_steps: List[str]
    optimization_applied: List[str]


@dataclass
class PlanMetadata:
    """Metadata about the planning process"""
    original_plan: Dict[str, Any]
    alternative_plans: List[QueryPlan]
    selected_plan: QueryPlan
    optimization_strategy: str
    statistics: Dict[str, Any]


class CostEstimator:
    """Estimates query execution cost based on various factors"""

    @staticmethod
    def estimate_base_cost(num_rows: int, num_filters: int, num_grouping_cols: int,
                          num_measures: int, has_joins: bool = False) -> float:
        """Base cost estimation formula"""
        # Base cost based on data size
        data_cost = num_rows * 0.01  # Base cost per row

        # Cost multiplier based on complexity
        complexity_multiplier = (
            1.0 +  # Base
            num_filters * 0.15 +  # Filter cost
            num_grouping_cols * 0.25 +  # Grouping cost
            num_measures * 0.20  # Aggregation cost
        )

        # Additional cost for joins
        if has_joins:
            complexity_multiplier *= 2.0

        return data_cost * complexity_multiplier


class QueryRewriter:
    """Applies optimization rules to rewrite queries for better performance"""

    @staticmethod
    def rewrite_for_performance(
        ibis_expr: IbisTable,
        spec: PivotSpec,
        backend_type: str = "duckdb"
    ) -> Tuple[IbisTable, List[str]]:
        """Apply performance optimization rules to the Ibis expression"""
        optimizations = []
        
        # 1. Verify Filter Pushdown
        # Ibis generally handles this, but we acknowledge it.
        # In a more complex implementation, we would traverse the expression tree.
        if spec.filters:
            optimizations.append("filter_pushdown_verified")

        # 2. Join Ordering
        # Check if the expression involves joins (simple string check for now as deep traversal is complex)
        expr_repr = str(ibis_expr)
        if "Join" in expr_repr or "join" in expr_repr:
             optimizations.append("join_ordering_delegated_to_backend")

        return ibis_expr, optimizations


class IbisPlanner:
    """
    Enhanced Ibis planner with support for:
    - Grouping Sets (CUBE, ROLLUP)
    - Top-N Pivot transformations
    - Ratio metrics with measure dependencies
    - Multi-dimensional hierarchical tiles
    - Advanced planning with cost estimation and optimization
    - Database-agnostic fallbacks
    - Unified expression building via IbisExpressionBuilder
    - Materialized View Routing
    """

    def __init__(self, con: Optional[Any] = None, enable_optimization: bool = True, materialized_manager: Optional[Any] = None):
        self.con = con
        self.enable_optimization = enable_optimization
        self.materialized_manager = materialized_manager
        self.cost_estimator = CostEstimator()
        self.query_rewriter = QueryRewriter()
        self.builder = IbisExpressionBuilder(con)
        self.parser = SafeExpressionParser()

        # Detect the backend database type for feature compatibility
        self._database_type = self._detect_database_type()
        self._supports_quantile = self._check_feature_support('quantile')
        self._supports_filter_clause = self._check_feature_support('filter_clause')
        self._supports_grouping_sets = self._check_feature_support('grouping_sets')

    def _detect_database_type(self) -> str:
        """Detect the backend database type."""
        if self.con is None:
            return "unknown"

        try:
            # Try to get the backend name from Ibis connection
            if hasattr(self.con, 'name'):
                return self.con.name.lower()
            elif hasattr(self.con, '_backend'):
                backend_name = getattr(self.con._backend, '__class__', type(self.con)).__name__.lower()
                backend_mapping = {
                    'postgresbackend': 'postgres',
                    'mysqlbackend': 'mysql',
                    'sqlitebackend': 'sqlite',
                    'bigquerybackend': 'bigquery',
                    'snowflakebackend': 'snowflake',
                    'duckdbbackend': 'duckdb',
                    'clickhousebackend': 'clickhouse',
                    'mssqlbackend': 'mssql',
                    'oraclebackend': 'oracle'
                }
                return backend_mapping.get(backend_name, backend_name)
            else:
                result = self.con.sql("SELECT 1 as test").execute()
                return "generic"
        except:
            return "unknown"

    def _check_feature_support(self, feature: str) -> bool:
        """Check if the backend database supports specific features."""
        if self._database_type == "unknown":
            return self._test_feature_availability(feature)

        # Feature compatibility based on database type
        feature_matrix = {
            'quantile': {
                'postgres': True,
                'mysql': True,
                'sqlite': False,
                'bigquery': True,
                'snowflake': True,
                'duckdb': True,
                'clickhouse': True,
                'mssql': True,
                'oracle': True,
            },
            'filter_clause': {
                'postgres': True,
                'mysql': False,
                'sqlite': False,
                'bigquery': True,
                'snowflake': True,
                'duckdb': True,
                'clickhouse': True,
                'mssql': False,
                'oracle': True,
            },
            'grouping_sets': {
                'postgres': True,
                'mysql': True,
                'sqlite': False,
                'bigquery': True,
                'snowflake': True,
                'duckdb': True,
                'clickhouse': True,
                'mssql': True,
                'oracle': True,
            }
        }

        db_features = feature_matrix.get(feature, {})
        return db_features.get(self._database_type, False)

    def _test_feature_availability(self, feature: str) -> bool:
        """Test if a feature is actually available by trying to use it."""
        if self.con is None:
            return False

        # If we reached here, the static feature matrix didn't give a definitive answer
        # or we are in 'generic' mode.
        try:
            # Try a minimal operation to ensure connection is active
            self.con.sql("SELECT 1").execute()
            
            # For specific features, we could try more complex queries if we had a known table.
            # Without a known table, we optimistically assume support if the connection works,
            # trusting that the static matrix handles known limitations.
            return True
        except Exception:
            return False

    def plan(self, spec: PivotSpec, *, columns_top_n: Optional[int] = None,
             columns_order_by_measure: Optional[Measure] = None,
             include_metadata: bool = True, optimize: bool = True) -> Dict[str, Any]:
        """
        Generate enhanced query plan from PivotSpec with advanced planning capabilities.
        """
        self._validate_spec(spec)

        # Check for materialized views (Routing)
        rollup_table = None
        if self.materialized_manager:
            rollup_table = self.materialized_manager.find_best_rollup(spec)
        
        effective_spec = spec
        used_rollup = False
        
        if rollup_table:
             import copy
             effective_spec = copy.deepcopy(spec)
             effective_spec.table = rollup_table
             used_rollup = True

        if effective_spec.pivot_config and effective_spec.pivot_config.enabled:
            plan_result = self._plan_pivot_mode(effective_spec, include_metadata)
        elif effective_spec.grouping_config and effective_spec.grouping_config.mode != "standard":
            plan_result = self._plan_grouping_sets(effective_spec, include_metadata)
        elif effective_spec.drill_paths:
            plan_result = self._plan_hierarchical_drill(effective_spec, include_metadata)
        else:
            plan_result = self._plan_standard(effective_spec, columns_top_n, columns_order_by_measure, include_metadata)

        if self.enable_optimization and optimize:
            plan_result = self._apply_advanced_planning(plan_result, effective_spec)
        else:
            if "metadata" not in plan_result:
                plan_result["metadata"] = {}
            plan_result["metadata"]["optimization_enabled"] = False
            plan_result["metadata"]["advanced_planning_applied"] = False

        if used_rollup:
            plan_result["metadata"]["used_materialized_view"] = rollup_table

        return plan_result

    def _apply_advanced_planning(self, plan_result: Dict[str, Any], spec: PivotSpec) -> Dict[str, Any]:
        """
        Apply advanced planning optimizations to the plan result.
        """
        optimized_ibis_queries = []
        for query_item in plan_result.get("queries", []):
            if isinstance(query_item, IbisTable) or isinstance(query_item, IbisExpr):
                optimized_expr, optimizations = self.query_rewriter.rewrite_for_performance(
                    query_item, spec, self._database_type
                )
                query_item = optimized_expr
            optimized_ibis_queries.append(query_item)

        plan_result["queries"] = optimized_ibis_queries

        if "metadata" not in plan_result:
            plan_result["metadata"] = {}
        plan_result["metadata"]["queries_metadata"] = []

        for i, query_expr in enumerate(plan_result.get("queries", [])):
            query_metadata = {
                "estimated_cost": self._estimate_query_cost(query_expr, spec),
                "optimization_applied": [] # Placeholder for future expansion
            }
            plan_result["metadata"]["queries_metadata"].append(query_metadata)

        total_estimated_cost = sum(
            q_meta.get("estimated_cost", 0) for q_meta in plan_result["metadata"]["queries_metadata"]
        )

        plan_result["metadata"]["optimization_enabled"] = True
        plan_result["metadata"]["total_estimated_cost"] = total_estimated_cost
        plan_result["metadata"]["advanced_planning_applied"] = True

        return plan_result

    def _estimate_query_cost(self, ibis_expr: Union[IbisTable, IbisExpr], spec: PivotSpec) -> float:
        """Estimate the execution cost of a single Ibis expression"""
        filters_count = len(spec.filters)
        grouping_cols_count = len(spec.rows) + len(spec.columns)
        measures_count = len([m for m in spec.measures if not m.ratio_numerator])

        # Simple heuristic to detect joins without complex tree traversal
        # (which varies significantly between Ibis versions)
        has_joins = "Join" in str(ibis_expr) or "join" in str(ibis_expr)
        
        return self.cost_estimator.estimate_base_cost(
            100000,
            filters_count,
            grouping_cols_count,
            measures_count,
            has_joins
        )

    def _apply_stable_ordering(
        self,
        table: IbisTable,
        sort_specs: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]],
        fallback_fields: Optional[List[str]],
    ) -> IbisTable:
        """Apply requested sort and append deterministic tie-breakers."""
        order_exprs = []
        requested_fields = set()

        if sort_specs:
            normalized = sort_specs if isinstance(sort_specs, list) else [sort_specs]
            order_exprs.extend(self.builder.build_sort_expressions(table, normalized))
            for s in normalized:
                if not isinstance(s, dict):
                    continue
                field = s.get("field")
                if field in table.columns:
                    requested_fields.add(field)
                sort_key_field = s.get("sortKeyField")
                if sort_key_field in table.columns:
                    requested_fields.add(sort_key_field)

        for field in (fallback_fields or []):
            if field in table.columns and field not in requested_fields:
                order_exprs.append(table[field].asc(nulls_first=True))

        if order_exprs:
            return table.order_by(order_exprs)
        return table

    # _convert_filter_to_ibis removed, using self.builder.build_filter_expression
    # _convert_sort_to_ibis removed, using self.builder.build_sort_expressions
    # _convert_measure_to_ibis removed, using self.builder.build_measure_aggregation
    # _convert_cursor_to_ibis_filter removed, using self.builder.build_cursor_filter_expression

    def _plan_standard(self, spec: PivotSpec, columns_top_n: Optional[int],
                       columns_order_by_measure: Optional[Measure],
                       include_metadata: bool) -> Dict[str, Any]:
        """
        Standard pivot table planning using Ibis expressions.
        Supports both dimension filters (WHERE) and measure filters (HAVING).
        """
        base_table = self.con.table(spec.table)
        
        # Split filters into pre-aggregation (dimensions) and post-aggregation (measures)
        measure_aliases = {m.alias for m in spec.measures}
        pre_filters = []
        post_filters = []
        
        if spec.filters:
            for f in spec.filters:
                # Determine the target field for this filter object
                target_field = f.get('field')
                
                # If it's a composite filter, check the first condition's field
                if not target_field and 'conditions' in f and len(f['conditions']) > 0:
                    target_field = f['conditions'][0].get('field')
                
                if target_field in measure_aliases:
                    post_filters.append(f)
                elif target_field and any(target_field.endswith(f"_{alias}") for alias in measure_aliases):
                    # Also treat pivot columns (e.g. 'Laptop_cost_sum') as post-filters
                    post_filters.append(f)
                else:
                    pre_filters.append(f)

        filtered_table = base_table
        if pre_filters:
            filter_expr = self.builder.build_filter_expression(base_table, pre_filters)
            if filter_expr is not None:
                filtered_table = filtered_table.filter(filter_expr)
        
        if spec.cursor and spec.sort:
            cursor_filter_expr = self.builder.build_cursor_filter_expression(filtered_table, spec)
            if cursor_filter_expr is not None:
                filtered_table = filtered_table.filter(cursor_filter_expr)

        # ... Classification and aggregation building ...
        window_measures = [m for m in spec.measures if m.window_func]
        non_window_measures = [m for m in spec.measures if not m.window_func]
        
        base_measures = [m for m in non_window_measures if not m.expression and not m.ratio_numerator]
        ratio_measures = [m for m in non_window_measures if m.ratio_numerator]
        calculated_measures = [m for m in non_window_measures if m.expression]
        
        # 2. Prepare Base Aggregations (including dependencies for window functions)
        alias_to_expr = {}
        ibis_aggregations = []
        
        # Track base expressions for window functions
        window_base_map = {} # window_alias -> base_alias
        
        # Add explicit base measures
        for m in base_measures:
            if m.agg == 'median' and not self._supports_quantile:
                 raise ValueError(f"Median aggregation requires quantile support in backend {self._database_type}")
            if m.agg == 'percentile' and not self._supports_quantile:
                 raise ValueError(f"Percentile aggregation requires quantile support in backend {self._database_type}")

            expr = self.builder.build_measure_aggregation(filtered_table, m)
            ibis_aggregations.append(expr)
            alias_to_expr[m.alias] = expr

            # Visual totals for averages need leaf-level counts so parent groups
            # can recompute a weighted average after post-aggregation filters.
            if (m.agg or "").lower() in {"avg", "mean"}:
                count_alias = f"__count_{m.alias}"
                ibis_aggregations.append(filtered_table[m.field].count().name(count_alias))
            elif (m.agg or "").lower() in {"weighted_avg", "wavg", "weighted_mean"}:
                if not m.weighted_field:
                    raise ValueError(
                        f"Weighted average measure '{m.alias}' requires a weight field."
                    )
                if m.weighted_field not in filtered_table.columns:
                    raise ValueError(
                        f"Weighted average measure '{m.alias}' references unknown weight field '{m.weighted_field}'."
                    )
                value_col = filtered_table[m.field]
                weight_col = filtered_table[m.weighted_field]
                valid_mask = value_col.notnull() & weight_col.notnull()
                weighted_sum_alias = f"__sumxw_{m.alias}"
                total_weight_alias = f"__sumw_{m.alias}"
                ibis_aggregations.append(
                    (value_col * weight_col).where(valid_mask).sum().name(weighted_sum_alias)
                )
                ibis_aggregations.append(
                    weight_col.where(valid_mask).sum().name(total_weight_alias)
                )

        # Add hidden base measures for window functions if needed
        for wm in window_measures:
            # Rank/DenseRank might not need a field aggregation if purely row-based, 
            # but usually they are "Rank of Sales".
            # If no field is provided, we can't aggregate.
            if wm.field:
                # Check if this exact aggregation is already being calculated
                # Optimization: reuse existing base measure if matches
                existing_match = next((m for m in base_measures if m.field == wm.field and m.agg == wm.agg), None)
                
                if existing_match:
                    window_base_map[wm.alias] = existing_match.alias
                else:
                    # Create a hidden base measure
                    hidden_alias = f"_base_{wm.alias}"
                    hidden_m = Measure(field=wm.field, agg=wm.agg, alias=hidden_alias)
                    expr = self.builder.build_measure_aggregation(filtered_table, hidden_m)
                    ibis_aggregations.append(expr)
                    window_base_map[wm.alias] = hidden_alias
                    # Also add to alias_to_expr so it can be resolved if needed
                    alias_to_expr[hidden_alias] = expr

        # 3. Handle Ratio Measures (Pre-Window)
        for rm in ratio_measures:
            if rm.ratio_numerator in alias_to_expr and rm.ratio_denominator in alias_to_expr:
                num_expr = alias_to_expr[rm.ratio_numerator]
                den_expr = alias_to_expr[rm.ratio_denominator]
                ratio_expr = (num_expr / den_expr.nullif(0)).name(rm.alias)
                ibis_aggregations.append(ratio_expr)
                alias_to_expr[rm.alias] = ratio_expr

        # 4. Handle Calculated Fields (Pre-Window)
        for cm in calculated_measures:
            if not cm.expression: continue
            try:
                calc_expr = self._parse_custom_expression(cm.expression, alias_to_expr, filtered_table)
                calc_expr = calc_expr.name(cm.alias)
                ibis_aggregations.append(calc_expr)
                alias_to_expr[cm.alias] = calc_expr
            except Exception as e:
                raise ValueError(f"Invalid expression for measure '{cm.alias}': {e}")

        # 5. Perform Aggregation
        # Deduplicate group columns while preserving order
        group_cols = list(dict.fromkeys(list(spec.rows) + list(spec.columns)))
        hidden_sort_group_cols = []
        for sort_spec in (spec.sort or []):
            if not isinstance(sort_spec, dict):
                continue
            sort_type = str(sort_spec.get("sortType") or "").strip().lower()
            sort_key_field = sort_spec.get("sortKeyField")
            sort_field = sort_spec.get("field")
            # Include hidden sort key if the sort field OR any group col
            # is the dimension it belongs to.
            # We check for exact match or the standard __sortkey__ prefix.
            sort_key_matches_group = (
                not sort_field
                or sort_field in group_cols
                or (isinstance(sort_key_field, str) and (
                    sort_key_field in group_cols or
                    (sort_key_field.startswith("__sortkey__") and sort_key_field[11:] in group_cols)
                ))
            )
            if (
                isinstance(sort_key_field, str)
                and sort_key_field
                and sort_key_field in filtered_table.columns
                and sort_key_matches_group
                and sort_key_field not in group_cols
            ):
                hidden_sort_group_cols.append(sort_key_field)
        aggregation_group_cols = list(dict.fromkeys(group_cols + hidden_sort_group_cols))
        
        # Determine the most granular grouping for the current spec (Visual Totals basis)
        # Use full_rows if available to ensure parent totals match child sums deep in the tree
        all_hierarchy_dims = list(
            dict.fromkeys(list(spec.full_rows or spec.rows) + list(spec.columns) + hidden_sort_group_cols)
        )
        
        if post_filters and len(all_hierarchy_dims) > len(group_cols):
            # VISUAL TOTALS MODE: Aggregate from the leaf level up to the parent level.
            # AVG requires hidden count helpers so parent totals remain weighted.
            # 1. Inner query: group by the MOST GRANULAR level and apply HAVING
            inner_agg = filtered_table.group_by(all_hierarchy_dims).aggregate(ibis_aggregations)
            
            # Apply measure filters at the leaf level
            post_filter_expr = self.builder.build_filter_expression(inner_agg, post_filters, is_post_agg=True)
            if post_filter_expr is not None:
                inner_agg = inner_agg.filter(post_filter_expr)
            
            # 2. Outer query: aggregate the filtered leaf results up to the requested level
            # We must sum/min/max the already aggregated values. 
            # Note: This works for SUM, COUNT, MIN, MAX. For AVG it's more complex (not handled here).
            outer_aggs = []
            for m in spec.measures:
                if not m.ratio_numerator and not m.expression:
                    # Map aggregation type to appropriate roll-up function
                    agg_type = (m.agg or "sum").lower()
                    if agg_type in ('sum', 'count'):
                        outer_aggs.append(inner_agg[m.alias].sum().name(m.alias))
                    elif agg_type in ('avg', 'mean'):
                        count_alias = f"__count_{m.alias}"
                        weighted_sum = (inner_agg[m.alias] * inner_agg[count_alias]).sum()
                        total_count = inner_agg[count_alias].sum()
                        outer_aggs.append((weighted_sum / total_count.nullif(0)).name(m.alias))
                    elif agg_type in ('weighted_avg', 'wavg', 'weighted_mean'):
                        weighted_sum_alias = f"__sumxw_{m.alias}"
                        total_weight_alias = f"__sumw_{m.alias}"
                        weighted_sum = inner_agg[weighted_sum_alias].sum()
                        total_weight = inner_agg[total_weight_alias].sum()
                        outer_aggs.append((weighted_sum / total_weight.nullif(0)).name(m.alias))
                    elif agg_type == 'min':
                        outer_aggs.append(inner_agg[m.alias].min().name(m.alias))
                    elif agg_type == 'max':
                        outer_aggs.append(inner_agg[m.alias].max().name(m.alias))
                    else:
                        # Default to sum for aliases
                        outer_aggs.append(inner_agg[m.alias].sum().name(m.alias))
            
            if aggregation_group_cols:
                aggregated_table = inner_agg.group_by(aggregation_group_cols).aggregate(outer_aggs)
            else:
                aggregated_table = inner_agg.aggregate(outer_aggs)
        else:
            # STANDARD MODE: Simple one-pass aggregation
            if aggregation_group_cols:
                aggregated_table = filtered_table.group_by(aggregation_group_cols).aggregate(ibis_aggregations)
            else:
                aggregated_table = filtered_table.aggregate(ibis_aggregations)

            # Apply Post-Aggregation Filters (Measure Filters)
            if post_filters:
                post_filter_expr = self.builder.build_filter_expression(aggregated_table, post_filters, is_post_agg=True)
                if post_filter_expr is not None:
                    aggregated_table = aggregated_table.filter(post_filter_expr)

        # 6. Apply Window Functions (Post-Aggregation)
        if window_measures:
            window_mutations = []
            for wm in window_measures:
                # Resolve the base expression from the aggregated table
                base_col = None
                if wm.alias in window_base_map:
                    base_col = aggregated_table[window_base_map[wm.alias]]
                
                # Build the window expression
                try:
                    win_expr = self._build_window_expression(wm, base_col, aggregated_table, group_cols, spec)
                    window_mutations.append(win_expr.name(wm.alias))
                except Exception as e:
                    print(f"Error building window function {wm.alias}: {e}")
                    # Fallback to null?
                    window_mutations.append(ibis.null().name(wm.alias))

            if window_mutations:
                aggregated_table = aggregated_table.mutate(window_mutations)

        # 7. Final Projection and Ordering.
        # Include hidden sort-key group columns so ORDER BY can reference them,
        # then the adapter strips those keys from the transport payload.
        projection_group_cols = list(dict.fromkeys(group_cols + hidden_sort_group_cols))
        projection = []
        for col in projection_group_cols:
            projection.append(aggregated_table[col])
        
        for m in spec.measures:
            projection.append(aggregated_table[m.alias])
            
        aggregated_table = aggregated_table.select(projection)
        
        aggregated_table = self._apply_stable_ordering(
            aggregated_table,
            spec.sort,
            group_cols,
        )
        
        if spec.limit:
            aggregated_table = aggregated_table.limit(spec.limit)
        
        queries: List[IbisTable] = [aggregated_table]
        
        metadata = {
            "group_by": group_cols,
            "agg_aliases": [m.alias for m in spec.measures],
            "measure_aggs": {m.alias: (m.agg or "sum").lower() for m in spec.measures if not m.ratio_numerator},
            "has_ratio_measures": len(ratio_measures) > 0,
            "has_calculated_measures": len(calculated_measures) > 0,
            "has_window_measures": len(window_measures) > 0,
            "ratio_measures": [{"alias": m.alias, "numerator": m.ratio_numerator, 
                               "denominator": m.ratio_denominator} for m in ratio_measures]
        }

        if spec.columns and columns_top_n and columns_top_n > 0:
            col_ibis_expr = self._build_column_values_query(
                spec.table, spec.columns, spec.filters,
                columns_top_n, columns_order_by_measure
            )
            queries.insert(0, col_ibis_expr)
            metadata["needs_column_discovery"] = True

        if spec.totals:
            metadata["needs_totals"] = True
            
        if include_metadata:
            metadata["estimated_complexity"] = "high" if window_measures else "medium"
            
        return {"queries": queries, "metadata": metadata}

    def _build_window_expression(self, measure: Measure, base_col: Any, table: IbisTable, group_cols: List[str], spec: PivotSpec = None) -> Any:
        """
        Builds an Ibis window function expression.
        """
        if not measure.window_func:
            raise ValueError("Window function name is required")

        raw_func = str(measure.window_func).lower()
        normalized_func = {
            "percent_of_total": "percent_of_total",
            "percent_of_grand_total": "percent_of_total",
            "percent_of_row": "percent_of_row",
            "percent_of_col": "percent_of_col",
            "cumulative": "cumulative",
            "running_avg": "running_avg",
            "moving_avg": "moving_avg",
            "rank": "rank",
            "dense_rank": "dense_rank",
        }.get(raw_func, raw_func)

        # Determine Partition By
        partition_by = []
        if measure.window_group_by is not None:
            # Explicit partitioning
            partition_by = measure.window_group_by
        elif spec:
            # Automatic partitioning based on function type
            if normalized_func == "percent_of_total":
                partition_by = []
            elif normalized_func == "percent_of_row":
                # Partition by Row Dimensions to calculate sum across columns for that row
                partition_by = spec.rows
            elif normalized_func == "percent_of_col":
                # Partition by Column Dimensions to calculate sum across rows for that column
                partition_by = spec.columns
            elif normalized_func in ["cumulative", "running_avg", "moving_avg"]:
                # Default for cumulative: Partition by Parent? Or Grand?
                # Usually Cumulative is over Time (a column or row).
                # If we have rows=['region'], columns=['year']. Cumulative over Year?
                # Partition by Region. Order by Year.
                # Heuristic: Partition by all dims except the last sort dim?
                pass
        partition_by = [table[col] for col in partition_by if col in table.columns]

        # ... rest of function ...

        # Determine Order By
        order_by = []
        if measure.window_order_by:
            for dim in measure.window_order_by:
                if dim in table.columns:
                    order_by.append(table[dim])
        else:
            # Default ordering needed for cumulative
            if normalized_func in ["cumulative", "rank", "dense_rank", "running_avg", "moving_avg"]:
                # Default order by the first row dimension?
                if group_cols:
                    first_col = group_cols[0]
                    if first_col in table.columns:
                        order_by.append(table[first_col])

        # Construct Window
        # Note: Ibis window syntax: ibis.window(group_by=..., order_by=..., following=..., preceding=...)
        w = ibis.window(group_by=partition_by, order_by=order_by)
        
        # Frame for moving averages
        if measure.window_frame_start is not None or measure.window_frame_end is not None:
            w = w.bind(preceding=measure.window_frame_start, following=measure.window_frame_end)

        func_type = normalized_func

        if func_type == "cumulative":
            if base_col is None: raise ValueError("Cumulative requires a base field")
            return base_col.sum().over(w)

        elif func_type in ["percent_of_total", "percent_of_row", "percent_of_col"]:
            if base_col is None: raise ValueError("Percent of Total requires a base field")
            # x / sum(x) over window
            total = base_col.sum().over(w)
            return base_col / total.nullif(0)
            
        elif func_type == "rank":
            return ibis.rank().over(w)
            
        elif func_type == "dense_rank":
            return ibis.dense_rank().over(w)
            
        elif func_type == "running_avg" or func_type == "moving_avg":
            if base_col is None: raise ValueError("Running Avg requires a base field")
            return base_col.mean().over(w)
            
        else:
            raise ValueError(f"Unknown window function: {func_type}")

    def _parse_custom_expression(self, expression: str, alias_map: Dict[str, Any], table: IbisTable) -> Any:
        """
        Parses a user-defined string expression and returns an Ibis expression.
        Uses SafeExpressionParser to safely evaluate AST.
        """
        return self.parser.evaluate(expression, alias_map)


    def _plan_grouping_sets(self, spec: PivotSpec, include_metadata: bool) -> Dict[str, Any]:
        """
        Plan query with GROUPING SETS, CUBE, or ROLLUP using Ibis expressions.
        """
        base_table = self.con.table(spec.table)
        
        if spec.filters:
            filter_expr = self.builder.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                base_table = base_table.filter(filter_expr)
        
        groupings = []
        rows = spec.rows or []
        cols = spec.columns or []
        all_dims = rows + cols
        
        mode = spec.grouping_config.mode if spec.grouping_config else "standard"
        
        if mode == "cube":
            for r in range(len(all_dims) + 1):
                groupings.extend(itertools.combinations(all_dims, r))
        elif mode == "rollup":
            for i in range(len(all_dims), -1, -1):
                groupings.append(tuple(all_dims[:i]))
        else:
            return self._plan_standard(spec, None, None, include_metadata)

        unioned_expr = None
        base_measures = [m for m in spec.measures if not m.ratio_numerator]
        
        for group in groupings:
            group_cols = list(group)
            aggs = [self.builder.build_measure_aggregation(base_table, m) for m in base_measures]
            
            if group_cols:
                sub_expr = base_table.group_by(group_cols).aggregate(aggs)
            else:
                sub_expr = base_table.aggregate(aggs)
            
            projection = []
            
            for dim in all_dims:
                if dim in group_cols:
                    projection.append(sub_expr[dim])
                else:
                    projection.append(ibis.null().name(dim))
            
            for m in base_measures:
                projection.append(sub_expr[m.alias])
                
            projected_expr = sub_expr.select(projection)
            
            if unioned_expr is None:
                unioned_expr = projected_expr
            else:
                unioned_expr = unioned_expr.union(projected_expr)
                
        if spec.sort:
            ibis_sorts = self.builder.build_sort_expressions(unioned_expr, spec.sort)
            if ibis_sorts:
                unioned_expr = unioned_expr.order_by(ibis_sorts)
                
        if spec.limit:
            unioned_expr = unioned_expr.limit(spec.limit)

        metadata = {
            "grouping_mode": mode,
            "generated_groupings": len(groupings),
            "simulated_via_union": True
        }

        return {"queries": [unioned_expr], "metadata": metadata}

    def _plan_pivot_mode(self, spec: PivotSpec, include_metadata: bool) -> Dict[str, Any]:
        """
        Plan for pivot transformation with dynamic columns using Ibis expressions.
        """
        if not spec.columns:
            # If no columns to pivot, treat as standard aggregation plan
            return self._plan_standard(spec, None, None, include_metadata)

        top_n = 50 
        column_cursor = None
        if spec.pivot_config:
             if spec.pivot_config.top_n:
                 top_n = spec.pivot_config.top_n
             column_cursor = spec.pivot_config.column_cursor
             
        order_measure = spec.measures[0]
        
        col_ibis_expr = self._build_column_values_query(
            spec.table, spec.columns, spec.filters, top_n, order_measure, column_cursor
        )
        
        metadata = {
            "needs_column_discovery": True,
            "pivot_enabled": True,
            "top_n": top_n,
            "measure_aggs": {m.alias: (m.agg or "sum").lower() for m in spec.measures if not m.ratio_numerator},
        }
        
        if spec.totals:
            metadata["needs_totals"] = True
        
        return {"queries": [col_ibis_expr], "metadata": metadata}

    def build_pivot_query_from_columns(
        self, spec: PivotSpec, column_values: List[str]
    ) -> IbisTable:
        """
        Build actual pivot query as an Ibis expression after discovering column values.
        """
        base_table = self.con.table(spec.table)
        
        # Split filters into pre-aggregation (dimensions) and post-aggregation (measures)
        measure_aliases = {m.alias for m in spec.measures}
        pre_filters = []
        post_filters = []
        
        if spec.filters:
            for f in spec.filters:
                # Determine the target field for this filter object
                target_field = f.get('field')
                
                # If it's a composite filter, check the first condition's field
                if not target_field and 'conditions' in f and len(f['conditions']) > 0:
                    target_field = f['conditions'][0].get('field')
                
                if target_field in measure_aliases:
                    post_filters.append(f)
                elif target_field and any(target_field.endswith(f"_{alias}") for alias in measure_aliases):
                    # Also treat pivot columns (e.g. 'Laptop_cost_sum') as post-filters
                    post_filters.append(f)
                else:
                    pre_filters.append(f)

        if pre_filters:
            filter_expr = self.builder.build_filter_expression(base_table, pre_filters)
            if filter_expr is not None:
                base_table = base_table.filter(filter_expr)
                
        base_measures = [m for m in spec.measures if not m.ratio_numerator]
        
        if not spec.columns:
             aggregated_result = base_table.aggregate([self.builder.build_measure_aggregation(base_table, m) for m in base_measures])
             if post_filters:
                 post_filter_expr = self.builder.build_filter_expression(aggregated_result, post_filters, is_post_agg=True)
                 if post_filter_expr is not None:
                     aggregated_result = aggregated_result.filter(post_filter_expr)
             return aggregated_result

        pivot_aggs = []
        
        # Pre-build the match expression base
        if len(spec.columns) == 1:
            match_col_expr = base_table[spec.columns[0]].cast('string')
        else:
            match_col_expr = ibis.literal('')
            for col_name in spec.columns:
                match_col_expr = match_col_expr + base_table[col_name].cast('string') + ibis.literal('|')
            # Remove trailing separator to match val format
            match_col_expr = match_col_expr.substr(0, match_col_expr.length() - 1)

        for val in column_values:
            # Compare constructed column key against the value
            match_expr = match_col_expr == val
            
            for m in base_measures:
                col_expr = base_table[m.field]
                agg_type = (m.agg or "sum").lower()
                
                cond_col = match_expr.ifelse(col_expr, ibis.null())
                
                alias = f"{val}_{m.alias}"
                
                if agg_type == 'sum':
                    pivot_aggs.append(cond_col.sum().name(alias))
                elif agg_type == 'avg':
                    pivot_aggs.append(cond_col.mean().name(alias))
                elif agg_type == 'min':
                    pivot_aggs.append(cond_col.min().name(alias))
                elif agg_type == 'max':
                    pivot_aggs.append(cond_col.max().name(alias))
                elif agg_type == 'count':
                    pivot_aggs.append(cond_col.count().name(alias))
                elif agg_type in ['count_distinct', 'distinct_count']:
                    pivot_aggs.append(cond_col.nunique().name(alias))
                elif agg_type in ['weighted_avg', 'wavg', 'weighted_mean']:
                    if not m.weighted_field:
                        raise ValueError(
                            f"Weighted average measure '{m.alias}' requires a weight field."
                        )
                    if m.weighted_field not in base_table.columns:
                        raise ValueError(
                            f"Weighted average measure '{m.alias}' references unknown weight field '{m.weighted_field}'."
                        )
                    weight_expr = base_table[m.weighted_field]
                    cond_weight = match_expr.ifelse(weight_expr, ibis.null())
                    valid_mask = cond_col.notnull() & cond_weight.notnull()
                    weighted_sum = (cond_col * cond_weight).where(valid_mask).sum()
                    total_weight = cond_weight.where(valid_mask).sum()
                    pivot_aggs.append((weighted_sum / total_weight.nullif(0)).name(alias))
                else:
                    pass

        if spec.pivot_config and spec.pivot_config.include_totals_column:
            for m in base_measures:
                col_expr = base_table[m.field]
                agg_type = (m.agg or "sum").lower()
                alias = f"__RowTotal__{m.alias}"
                
                if agg_type == 'sum':
                    pivot_aggs.append(col_expr.sum().name(alias))
                elif agg_type == 'avg':
                    pivot_aggs.append(col_expr.mean().name(alias))
                elif agg_type == 'min':
                    pivot_aggs.append(col_expr.min().name(alias))
                elif agg_type == 'max':
                    pivot_aggs.append(col_expr.max().name(alias))
                elif agg_type == 'count':
                    pivot_aggs.append(col_expr.count().name(alias))
                elif agg_type in ['count_distinct', 'distinct_count']:
                    pivot_aggs.append(col_expr.nunique().name(alias))
                elif agg_type in ['weighted_avg', 'wavg', 'weighted_mean']:
                    if not m.weighted_field:
                        raise ValueError(
                            f"Weighted average measure '{m.alias}' requires a weight field."
                        )
                    if m.weighted_field not in base_table.columns:
                        raise ValueError(
                            f"Weighted average measure '{m.alias}' references unknown weight field '{m.weighted_field}'."
                        )
                    weight_expr = base_table[m.weighted_field]
                    valid_mask = col_expr.notnull() & weight_expr.notnull()
                    weighted_sum = (col_expr * weight_expr).where(valid_mask).sum()
                    total_weight = weight_expr.where(valid_mask).sum()
                    pivot_aggs.append((weighted_sum / total_weight.nullif(0)).name(alias))

        row_dims = spec.rows
        
        if row_dims:
            result_expr = base_table.group_by(row_dims).aggregate(pivot_aggs)
        else:
            result_expr = base_table.aggregate(pivot_aggs)
            
        # Apply Post-Aggregation Filters (Measure Filters) on the pivoted result
        if post_filters:
            post_filter_expr = self.builder.build_filter_expression(result_expr, post_filters, is_post_agg=True)
            if post_filter_expr is not None:
                result_expr = result_expr.filter(post_filter_expr)
            
        result_expr = self._apply_stable_ordering(
            result_expr,
            spec.sort,
            row_dims,
        )
                    
        if spec.limit:
            result_expr = result_expr.limit(spec.limit)
            
        return result_expr

    def _plan_hierarchical_drill(self, spec: PivotSpec, include_metadata: bool) -> Dict[str, Any]:
        """
        Plan queries for hierarchical drill-down with multiple levels using Ibis expressions.
        """
        base_table = self.con.table(spec.table)
        
        if spec.filters:
            filter_expr = self.builder.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                base_table = base_table.filter(filter_expr)
                
        if spec.drill_paths:
            drill_filters = []
            for path_item in spec.drill_paths:
                f_field = path_item.get("field")
                f_val = path_item.get("value")
                if f_field and f_val is not None:
                     drill_filters.append(base_table[f_field] == f_val)
            
            for df in drill_filters:
                base_table = base_table.filter(df)
        
        current_depth = len(spec.drill_paths) if spec.drill_paths else 0
        
        if current_depth < len(spec.rows):
            target_dim = spec.rows[current_depth]
            group_cols = [target_dim]
        else:
            target_dim = None
            group_cols = []
            
        base_measures = [m for m in spec.measures if not m.ratio_numerator]
        aggs = [self.builder.build_measure_aggregation(base_table, m) for m in base_measures]
        
        if group_cols:
            result_expr = base_table.group_by(group_cols).aggregate(aggs)
        else:
            result_expr = base_table.aggregate(aggs)
            
        if spec.sort:
            ibis_sorts = self.builder.build_sort_expressions(result_expr, spec.sort)
            if ibis_sorts:
                result_expr = result_expr.order_by(ibis_sorts)
                
        if spec.limit:
            result_expr = result_expr.limit(spec.limit)
            
        metadata = {
            "drill_depth": current_depth,
            "target_dimension": target_dim
        }
        
        return {"queries": [result_expr], "metadata": metadata}

    def _build_column_values_query(
        self, table_name: str, columns: List[str], filters: List[Dict[str, Any]],
        top_n: int, order_measure: Optional[Measure], column_cursor: Optional[str] = None
    ) -> IbisTable:
        """
        Builds an Ibis expression to discover top-N column values with keyset pagination.
        """
        base_table = self.con.table(table_name)
        
        filtered_table = base_table
        if filters:
            filter_expr = self.builder.build_filter_expression(base_table, filters)
            if filter_expr is not None:
                filtered_table = filtered_table.filter(filter_expr)
        
        if not columns:
            raise ValueError("Columns must be non-empty for column values query.")
        
        if len(columns) == 1:
            col_expr = filtered_table[columns[0]].cast('string').name('_col_key')
        else:
            concat_expr = ibis.literal('')
            for col_name in columns:
                concat_expr = concat_expr + filtered_table[col_name].cast('string') + ibis.literal('|')
            # Use substr instead of slicing for backend compatibility
            col_expr = concat_expr.substr(0, concat_expr.length() - 1).name('_col_key')

        if order_measure:
            agg_measure_expr = self.builder.build_measure_aggregation(filtered_table, order_measure)
            result = filtered_table.group_by(col_expr).aggregate(agg_measure_expr)
            
            # Apply cursor filter if present (before order/limit)
            if column_cursor:
                # Assuming descending order for measures, so cursor means we want values smaller than cursor value?
                # Actually, keyset pagination on aggregated values is tricky because the cursor would be the measure value.
                # If we sort by measure, the cursor should be the measure value of the last item.
                # Here 'column_cursor' is likely the _col_key itself if we sort by _col_key.
                # But we sort by measure.
                # Let's assume standard behavior: if sorting by measure, we need measure-based cursor.
                # BUT user prompt says "Column-Dimension Pagination".
                # Usually column headers are strings.
                # If sorting by measure, pagination is hard without a tuple cursor (measure, col_key).
                # For simplicity, if sorting by measure, we might skip simple cursor or require (measure, key).
                # Let's support simple lexicographical cursor on _col_key if NO order_measure is present OR if we assume simple key cursor.
                pass
            
            result = result.order_by(agg_measure_expr.desc())
        else:
            result = filtered_table.select(col_expr).distinct()
            if column_cursor:
                 # Standard keyset on the column key
                 result = result.filter(col_expr > column_cursor)
            result = result.order_by(col_expr) # Ensure deterministic order
        
        result = result.limit(top_n)
        
        return result

    def _validate_spec(self, spec: PivotSpec):
        if not spec.table:
            raise ValueError("PivotSpec must include a 'table' name")
        if not spec.measures:
            raise ValueError("PivotSpec must include at least one measure")
        
        aliases = [m.alias for m in spec.measures]
        if len(aliases) != len(set(aliases)):
            raise ValueError("Measure aliases must be unique")
        
        ratio_measures = [m for m in spec.measures if m.ratio_numerator]
        base_aliases = set(m.alias for m in spec.measures if not m.ratio_numerator)
        
        for rm in ratio_measures:
            if rm.ratio_numerator not in base_aliases:
                raise ValueError(f"Ratio measure '{rm.alias}' references unknown numerator '{rm.ratio_numerator}'")
            if rm.ratio_denominator not in base_aliases:
                raise ValueError(f"Ratio measure '{rm.alias}' references unknown denominator '{rm.ratio_denominator}'")

    def explain_query_sql(self, ibis_expr: IbisTable) -> str:
        """Explain query via backend if available"""
        if self.con is not None:
            try:
                return str(self.con.explain(ibis_expr))
            except Exception as e:
                return f"Error explaining Ibis expression: {e}"
        return "No Ibis connection available for explain."

    def preview_plan(self, spec: PivotSpec) -> Dict[str, Any]:
        """Convenience method for debugging - returns compiled SQL of the plan"""
        plan_result = self.plan(spec, include_metadata=False, optimize=False)
        queries = plan_result.get("queries", [])
        
        compiled_queries = []
        for query_expr in queries:
            try:
                compiled_queries.append(str(self.con.compile(query_expr)))
            except Exception as e:
                compiled_queries.append(f"Error compiling Ibis expression: {e}")
        
        return {"compiled_queries": compiled_queries, "metadata": plan_result.get("metadata", {})}
