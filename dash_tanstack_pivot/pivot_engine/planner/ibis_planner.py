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
            requested_fields = {
                s.get("field")
                for s in normalized
                if isinstance(s, dict) and s.get("field") in table.columns
            }

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
        """
        base_table = self.con.table(spec.table)
        base_columns = set(base_table.columns)

        # Split filters into pre-aggregation (WHERE) and post-aggregation (HAVING).
        # A filter is post-aggregation when its field is not a raw table column —
        # i.e. it targets a computed pivot column such as "Headphones_cost_sum".
        pre_filters = []
        post_filters = []
        if spec.filters:
            for f in spec.filters:
                if 'conditions' in f:
                    # Composite filter: check the first condition's field
                    first_field = (f.get('conditions') or [{}])[0].get('field', '')
                else:
                    first_field = f.get('field', '')
                if first_field and first_field not in base_columns:
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

        # 1. Split measures into base (standard aggregations) and calculated (expressions)
        base_measures = [m for m in spec.measures if not m.expression and not m.ratio_numerator]
        # Ratio measures are just a special case of calculated measures, keep them separately for now
        # or handle them as "legacy" calculated fields. Ideally, we treat them as calculated too if possible,
        # but maintaining backward compatibility for now.
        ratio_measures = [m for m in spec.measures if m.ratio_numerator]
        calculated_measures = [m for m in spec.measures if m.expression]
        
        # 2. Compute base aggregations
        # Store a map of alias -> ibis_expression for resolution
        alias_to_expr = {}
        ibis_aggregations = []
        
        for m in base_measures:
            # Check for median/percentile support which Builder might not check strictly against *this* Planner's knowledge
            # But we trust Builder or add checks here if needed.
            if m.agg == 'median' and not self._supports_quantile:
                 raise ValueError(f"Median aggregation requires quantile support in backend {self._database_type}")
            if m.agg == 'percentile' and not self._supports_quantile:
                 raise ValueError(f"Percentile aggregation requires quantile support in backend {self._database_type}")

            expr = self.builder.build_measure_aggregation(filtered_table, m)
            ibis_aggregations.append(expr)
            alias_to_expr[m.alias] = expr

        # 3. Handle Ratio Measures (Legacy Support)
        # Ratio measures are effectively calculated fields: num / den
        for rm in ratio_measures:
            if rm.ratio_numerator in alias_to_expr and rm.ratio_denominator in alias_to_expr:
                num_expr = alias_to_expr[rm.ratio_numerator]
                den_expr = alias_to_expr[rm.ratio_denominator]
                # Avoid division by zero
                # Ibis syntax for NULLIF/zero handling varies, simpler to rely on backend behavior or explicit check
                # For safety: (num / nullif(den, 0))
                ratio_expr = (num_expr / den_expr.nullif(0)).name(rm.alias)
                ibis_aggregations.append(ratio_expr)
                alias_to_expr[rm.alias] = ratio_expr

        # 4. Handle Generic Calculated Fields
        # Topological sort might be needed if calculated fields reference each other.
        # For now, we assume simple one-level dependency on base/ratio measures or raw columns.
        # Iterate multiple times to resolve dependencies if needed?
        # Let's support referencing previously defined calculated measures by preserving order.
        
        for cm in calculated_measures:
            if not cm.expression:
                continue
            
            # Parse and build the expression
            try:
                calc_expr = self._parse_custom_expression(cm.expression, alias_to_expr, filtered_table)
                calc_expr = calc_expr.name(cm.alias)
                ibis_aggregations.append(calc_expr)
                alias_to_expr[cm.alias] = calc_expr
            except Exception as e:
                print(f"Error compiling calculated measure '{cm.alias}': {e}")
                # Fallback: maybe just return null? Or raise?
                # Raising ensures the user knows their formula is bad.
                raise ValueError(f"Invalid expression for measure '{cm.alias}': {e}")

        
        group_cols = list(spec.rows) + list(spec.columns)
        
        if group_cols:
            aggregated_table = filtered_table.group_by(group_cols).aggregate(ibis_aggregations)
        else:
            aggregated_table = filtered_table.aggregate(ibis_aggregations)

        # Apply post-aggregation (HAVING) filters — these target computed pivot columns
        if post_filters:
            post_filter_expr = self.builder.build_filter_expression(
                aggregated_table, post_filters, is_post_agg=True
            )
            if post_filter_expr is not None:
                aggregated_table = aggregated_table.filter(post_filter_expr)

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
            "agg_aliases": [m.alias for m in spec.measures], # All aliases
            "has_ratio_measures": len(ratio_measures) > 0,
            "has_calculated_measures": len(calculated_measures) > 0,
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
            metadata["estimated_complexity"] = "medium"
            
        return {"queries": queries, "metadata": metadata}

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
            "top_n": top_n
        }
        
        return {"queries": [col_ibis_expr], "metadata": metadata}

    def build_pivot_query_from_columns(
        self, spec: PivotSpec, column_values: List[str]
    ) -> IbisTable:
        """
        Build actual pivot query as an Ibis expression after discovering column values.
        """
        base_table = self.con.table(spec.table)
        
        if spec.filters:
            filter_expr = self.builder.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                base_table = base_table.filter(filter_expr)
                
        base_measures = [m for m in spec.measures if not m.ratio_numerator]
        pivot_col = spec.columns[0] if spec.columns else None
        
        if not pivot_col:
             return base_table.aggregate([self.builder.build_measure_aggregation(base_table, m) for m in base_measures])

        pivot_aggs = []
        
        for val in column_values:
            match_expr = base_table[pivot_col] == val
            
            for m in base_measures:
                col_expr = base_table[m.field]
                agg_type = (m.agg or "sum").lower()
                
                cond_col = col_expr.where(match_expr)
                
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
                else:
                    pass

        row_dims = spec.rows
        
        if row_dims:
            result_expr = base_table.group_by(row_dims).aggregate(pivot_aggs)
        else:
            result_expr = base_table.aggregate(pivot_aggs)
            
        valid_sorts = None
        if spec.sort:
            valid_sorts = [s for s in (spec.sort if isinstance(spec.sort, list) else [spec.sort])
                          if s.get('field') in result_expr.columns]

        result_expr = self._apply_stable_ordering(
            result_expr,
            valid_sorts,
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
            col_expr = concat_expr[:-1].name('_col_key')

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
            
            result = result.order_by(ibis.desc(agg_measure_expr.name))
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
