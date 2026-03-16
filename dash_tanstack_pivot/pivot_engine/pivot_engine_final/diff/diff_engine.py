"""
QueryDiffEngine - tile-aware, spec-diff optimized, delta-update capable.

New features:
- Tile-aware diffing for virtual scrolling (row/column tiles)
- Semantic spec diffing (detect pagination-only, filter-only, sort-only changes)
- Delta updates for incremental data ingestion
- Table-level cache invalidation
- Optimized for Arrow/DuckDB integration
"""

import json
import time
import hashlib
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict
from enum import Enum
import pyarrow as pa
import ibis
from ibis import BaseBackend as IbisBaseBackend
from pivot_engine.types.pivot_spec import PivotSpec
from ibis.expr.api import Table as IbisTable, Expr


class SpecChangeType(Enum):
    """Types of changes between pivot specs"""
    IDENTICAL = "identical"
    PAGE_ONLY = "page_only"  # Only pagination changed
    SORT_ONLY = "sort_only"  # Only sort changed
    FILTER_ADDED = "filter_added"  # Filters added (subset of data)
    FILTER_REMOVED = "filter_removed"  # Filters removed (superset needed)
    STRUCTURE_CHANGED = "structure_changed"  # Rows/columns/measures changed
    FULL_REFRESH = "full_refresh"  # Complete recomputation needed


@dataclass
class TileKey:
    """Identifies a specific tile in the result grid"""
    row_start: int
    row_end: int
    col_start: int
    col_end: int

    # NEW for hierarchical dimensions
    dimension_level: Optional[Dict[str, int]] = None  # e.g., {"region": 1, "product": 2}
    drill_path: Optional[List[str]] = None  # e.g., ["USA", "California", "San Francisco"]

    def to_string(self) -> str:
        """Convert tile key to string representation, including hierarchical info"""
        base_part = f"r{self.row_start}-{self.row_end}_c{self.col_start}-{self.col_end}"

        if self.drill_path is not None:
            path_part = ":".join(self.drill_path)
            base_part += f"_path_{path_part}"

        if self.dimension_level is not None:
            level_part = ",".join([f"{k}:{v}" for k, v in self.dimension_level.items()])
            base_part += f"_level_{level_part}"

        return base_part

    @staticmethod
    def from_string(s: str) -> 'TileKey':
        """Parse tile key from string representation including hierarchical info"""
        # Split by underscore but keep track of the hierarchical parts
        # Expected format: rX-Y_cA-B[_path_...][_level_...]

        # First, identify hierarchical parts by looking for _path_ and _level_
        path_part = None
        level_part = None
        base_part = s

        # Extract path part if exists
        if '_path_' in s:
            path_start = s.find('_path_')
            base_part = s[:path_start]
            path_content_start = path_start + 6  # length of '_path_'

            # Find where path content ends (either level starts or end of string)
            level_start = s.find('_level_', path_content_start)
            if level_start != -1:
                path_end = level_start
            else:
                path_end = len(s)

            path_content = s[path_content_start:path_end]
            if path_content:
                path_part = path_content.split(':')

        # Extract level part if exists
        if '_level_' in s:
            level_start = s.find('_level_')
            level_content_start = level_start + 7  # length of '_level_'
            level_content = s[level_content_start:]

            if level_content:
                level_dict = {}
                for item in level_content.split(','):
                    if ':' in item:
                        k, v = item.split(':', 1)  # Split only on first ':'
                        level_dict[k] = int(v)
                level_part = level_dict
        else:
            # If no _level_ was found in original string, use base_part as is
            if '_path_' not in s:
                base_part = s

        # Parse the base part (should be in format rX-Y_cA-B)
        if '_' in base_part:
            row_part = base_part.split('_')[0][1:]  # Skip 'r'
            col_part = base_part.split('_')[1][1:]  # Skip 'c'
            r_start, r_end = map(int, row_part.split('-'))
            c_start, c_end = map(int, col_part.split('-'))
        else:
            # Fallback for unexpected format
            raise ValueError(f"Invalid tile string format: {s}")

        return TileKey(
            row_start=r_start,
            row_end=r_end,
            col_start=c_start,
            col_end=c_end,
            dimension_level=level_part,
            drill_path=path_part
        )


@dataclass
class QueryTile:
    """Represents a cached tile of query results"""
    data: List[Dict[str, Any]]
    timestamp: float
    row_count: int
    spec_hash: str


@dataclass
class DeltaInfo:
    """Information about incremental data changes"""
    table: str
    last_timestamp: float
    last_max_id: Optional[int] = None
    incremental_field: Optional[str] = None  # timestamp or id field


class QueryDiffEngine:
    """
    Advanced diff engine with tile-aware caching and semantic spec analysis.
    """
    
    def __init__(
        self,
        cache,
        default_ttl: int = 300,
        tile_size: int = 100,
        enable_tiles: bool = True,
        enable_delta_updates: bool = True,
        backend: Optional[IbisBaseBackend] = None # Add Ibis backend for true total computation
    ):
        """
        Args:
            cache: Cache object with get/set methods
            default_ttl: Default cache TTL in seconds
            tile_size: Number of rows per tile for tile-aware caching
            enable_tiles: Enable tile-based caching
            enable_delta_updates: Enable incremental delta updates
            backend: The Ibis backend connection for executing Ibis expressions
        """
        self.cache = cache
        self.ttl = default_ttl
        self.tile_size = tile_size
        self.enable_tiles = enable_tiles
        self.enable_delta_updates = enable_delta_updates
        self.backend = backend # Store the Ibis backend
        
        # Track previous spec for diffing
        self._last_spec_hash: Optional[str] = None
        self._last_spec: Optional[Dict[str, Any]] = None
        self._last_plan_digest: Optional[str] = None
        
        # Delta tracking per table
        self._delta_info: Dict[str, DeltaInfo] = {}
        
        # Table invalidation tracking
        self._invalidated_tables: Set[str] = set()
    
    # ========== Public API ==========
    
    def plan(
        self,
        plan_result: Dict[str, Any], # Now expects plan_result from IbisPlanner (queries: List[IbisTable])
        spec: Any,
        force_refresh: bool = False
    ) -> Tuple[List[IbisTable], Dict[str, Any]]: # Now returns List[IbisTable]
        """
        Decide which Ibis expressions to execute based on semantic diff analysis and delta updates.

        Returns:
            (ibis_expressions_to_run, execution_strategy)

        execution_strategy contains:
            - change_type: SpecChangeType
            - can_reuse_tiles: bool
            - tiles_needed: List[TileKey] if applicable
            - cache_hits: int
            - use_delta_updates: bool
        """
        queries = plan_result.get("queries", []) # These are now IbisTable expressions
        if not queries:
            return [], {"change_type": SpecChangeType.IDENTICAL}

        # Normalize spec
        spec_dict = self._normalize_spec_for_hash(spec)
        spec_hash = self._hash_dict(spec_dict) # This hash is for the spec itself

        # Check if table was invalidated
        table_name = spec_dict.get("table")
        if table_name in self._invalidated_tables:
            force_refresh = True
            self._invalidated_tables.discard(table_name)

        # Compute plan digest (now based on Ibis expressions)
        plan_digest = self._digest_plan(plan_result, spec_dict)

        # Analyze spec changes
        change_type = self._analyze_spec_change(spec_dict, self._last_spec)

        # Store current spec for next diff
        self._last_spec = spec_dict
        self._last_spec_hash = spec_hash
        self._last_plan_digest = plan_digest

        execution_strategy = {
            "change_type": change_type,
            "can_reuse_tiles": False,
            "tiles_needed": [],
            "cache_hits": 0,
            "force_refresh": force_refresh,
            "use_delta_updates": False,
            "delta_queries_generated": 0
        }

        # Check if delta updates are applicable for this table
        use_delta_updates = (
            self.enable_delta_updates and
            table_name in self._delta_info and
            change_type not in [SpecChangeType.STRUCTURE_CHANGED, SpecChangeType.FULL_REFRESH] and
            not spec_dict.get("cursor")  # Don't apply deltas to cursor pagination queries
        )

        # Only compute and return delta queries if specifically appropriate
        if use_delta_updates:
            # compute_delta_queries now returns Ibis expressions
            delta_ibis_expressions = self.compute_delta_queries(spec, plan_result)
            if delta_ibis_expressions and table_name in self._delta_info:
                execution_strategy["use_delta_updates"] = True
                execution_strategy["delta_queries_generated"] = len(delta_ibis_expressions)
                return delta_ibis_expressions, execution_strategy

        # Handle different change types
        if force_refresh or change_type == SpecChangeType.FULL_REFRESH:
            return queries, execution_strategy # queries are Ibis expressions

        if change_type == SpecChangeType.IDENTICAL:
            # Check if all queries are cached
            all_cached = all(
                self._is_query_cached(q, spec_dict) for q in queries # q is an IbisTable
            )
            if all_cached:
                execution_strategy["cache_hits"] = len(queries)
                return [], execution_strategy
            # Fall through to execute missing queries

        # Use tile-aware strategy only for virtual scrolling (page/offset-based), not cursor-based pagination
        if change_type == SpecChangeType.PAGE_ONLY and self.enable_tiles and spec_dict.get("cursor") is None:
            # _plan_tile_aware now returns Ibis expressions
            return self._plan_tile_aware(queries, spec_dict, plan_result, execution_strategy)

        if change_type == SpecChangeType.SORT_ONLY:
            # Can reuse aggregate data, just re-sort (Ibis expressions are immutable, so re-sort by modifying expression)
            pass 

        if change_type == SpecChangeType.FILTER_ADDED:
            # More restrictive filters - can potentially filter cached results
            pass

        # Default: determine which queries need execution
        to_run = []
        for q_expr in queries: # q_expr is an IbisTable
            if not self._is_query_cached(q_expr, spec_dict):
                to_run.append(q_expr)
            else:
                execution_strategy["cache_hits"] += 1

        return to_run, execution_strategy
    
    def merge_and_finalize(
        self,
        results: List[pa.Table],
        plan_result: Dict[str, Any], # Now expects plan_result from IbisPlanner (queries: List[IbisTable])
        spec: Any,
        execution_strategy: Dict[str, Any]
    ) -> Optional[pa.Table]:
        """
        Merge executed Arrow tables with cached data into a final Arrow Table.
        """
        spec_dict = self._normalize_spec_for_hash(spec)
        queries = plan_result.get("queries", [])
        use_delta_updates = execution_strategy.get("use_delta_updates", False)

        if use_delta_updates and results:
            table_name = spec_dict.get("table")
            if table_name and table_name in self._delta_info:
                if results:
                    first_query = queries[0] if queries else None
                    if first_query:
                        cache_key = self._cache_key_for_query(first_query, spec_dict) # first_query is IbisTable
                        self.cache.set(cache_key, results[0])
                    return results[0]

        change_type = execution_strategy.get("change_type", SpecChangeType.FULL_REFRESH)

        if results:
            return results[0] if len(results) == 1 else pa.concat_tables(results)
        
        if change_type == SpecChangeType.IDENTICAL and not results:
             cached_tables = []
             for q_expr in queries:
                 key = self._cache_key_for_query(q_expr, spec_dict)
                 cached = self.cache.get(key)
                 if cached:
                     cached_tables.append(cached)
             
             if cached_tables:
                 return cached_tables[0] if len(cached_tables) == 1 else pa.concat_tables(cached_tables)

        return None

    
    def invalidate_cache_for_table(self, table_name: str):
        """
        Invalidate all cached queries for a specific table.
        Called by ETL/ingestion processes when data changes.
        """
        self._invalidated_tables.add(table_name)
        
        # Clear delta info for incremental updates
        if table_name in self._delta_info:
            del self._delta_info[table_name]
    
    def register_delta_checkpoint(
        self,
        table: str,
        timestamp: float,
        max_id: Optional[int] = None,
        incremental_field: str = "updated_at"
    ):
        """
        Register a checkpoint for delta/incremental updates.
        """
        self._delta_info[table] = DeltaInfo(
            table=table,
            last_timestamp=timestamp,
            last_max_id=max_id,
            incremental_field=incremental_field
        )
    
    def compute_delta_queries(
        self,
        spec: Any,
        plan_result: Dict[str, Any] # Now expects plan_result from IbisPlanner (queries: List[IbisTable])
    ) -> Optional[List[IbisTable]]: # Now returns List[IbisTable]
        """
        Generate delta queries for incremental updates as Ibis expressions.
        """
        if not self.enable_delta_updates:
            return None

        spec_dict = self._normalize_spec_for_hash(spec)
        table_name = spec_dict.get("table")

        if table_name not in self._delta_info:
            return None

        delta_info = self._delta_info[table_name]
        queries = plan_result.get("queries", []) # These are IbisTable expressions
        delta_ibis_expressions = []

        for q_expr in queries: # q_expr is an IbisTable
            modified_expr = self._add_delta_filter(q_expr, delta_info)
            if modified_expr is not None:
                delta_ibis_expressions.append(modified_expr)

        return delta_ibis_expressions if delta_ibis_expressions else None

    def apply_delta_updates(
        self,
        spec: Any,
        plan_result: Dict[str, Any], # Now expects plan_result from IbisPlanner (queries: List[IbisTable])
        base_result: Optional[pa.Table],
        backend: Optional[Any] = None 
    ) -> Optional[pa.Table]:
        """
        Apply delta updates to existing cached results to produce updated results.
        """
        if not self.enable_delta_updates:
            return base_result

        spec_dict = self._normalize_spec_for_hash(spec)
        table_name = spec_dict.get("table")

        if table_name not in self._delta_info:
            return base_result

        delta_ibis_expressions = self.compute_delta_queries(spec, plan_result)
        if not delta_ibis_expressions:
            return base_result

        delta_results = []
        for dq_expr in delta_ibis_expressions: # dq_expr is an IbisTable
            try:
                # Execute the delta Ibis expression
                delta_result_table = dq_expr.to_pyarrow()
                if delta_result_table is not None:
                    delta_results.extend(delta_result_table.to_pylist())
            except Exception as e:
                print(f"Warning: Delta execution failed: {e}")
                return base_result

        if base_result is not None and delta_results:
            base_data = base_result.to_pylist() if base_result is not None else []
            merged_data = self.merge_delta_results(base_data, delta_results, spec_dict.get("measures", []))

            if merged_data:
                columns = {}
                for row in merged_data:
                    for col, val in row.items():
                        if col not in columns:
                            columns[col] = []
                        columns[col].append(val)

                arrays = {}
                for col_name, values in columns.items():
                    try:
                        arrays[col_name] = pa.array(values)
                    except:
                        arrays[col_name] = pa.array([str(v) for v in values])

                return pa.table(arrays)

        return base_result

    def _add_delta_filter(
        self,
        ibis_expr: IbisTable, # Now expects an IbisTable
        delta_info: DeltaInfo
    ) -> Optional[IbisTable]:
        """
        Modify Ibis expression to fetch only incremental data.
        Only add delta filter if the incremental field exists in the query's table.
        """
        incremental_field = delta_info.incremental_field
        
        # Check if field exists in columns to suppress unnecessary warnings
        if incremental_field not in ibis_expr.columns:
            return None # Cannot apply delta filter

        # Apply filter to the Ibis expression
        delta_filter_expr = ibis_expr[incremental_field] > delta_info.last_timestamp
        
        return ibis_expr.filter(delta_filter_expr)
    
    # ========== Tile-Aware Methods ==========
    
    def _plan_tile_aware(
        self,
        queries: List[IbisTable], # Now expects IbisTable expressions
        spec_dict: Dict[str, Any],
        plan_result: Dict[str, Any], # Now expects plan_result from IbisPlanner (queries: List[IbisTable])
        strategy: Dict[str, Any]
    ) -> Tuple[List[IbisTable], Dict[str, Any]]: # Now returns List[IbisTable]
        """
        Plan execution using tile-based caching.
        """
        page = spec_dict.get("page", {})
        offset = page.get("offset", 0)
        limit = page.get("limit", 100)
        
        start_tile = offset // self.tile_size
        end_tile = (offset + limit - 1) // self.tile_size
        
        tiles_needed = []
        tiles_cached = []
        
        for tile_idx in range(start_tile, end_tile + 1):
            tile_start = tile_idx * self.tile_size
            tile_end = min(tile_start + self.tile_size, offset + limit)
            
            tile_key = TileKey(
                row_start=tile_start,
                row_end=tile_end,
                col_start=0,  # For now, full column width
                col_end=-1   # -1 means all columns
            )
            
            cache_key = self._cache_key_for_tile(tile_key, spec_dict, queries[0]) # Pass Ibis expression
            
            if self.cache.get(cache_key) is not None:
                tiles_cached.append(tile_key)
            else:
                tiles_needed.append(tile_key)
        
        strategy["can_reuse_tiles"] = len(tiles_cached) > 0
        strategy["tiles_needed"] = [t.to_string() for t in tiles_needed]
        strategy["cache_hits"] = len(tiles_cached)
        
        if not tiles_needed:
            # All tiles cached
            return [], strategy
        
        tile_ibis_expressions = []
        for tile in tiles_needed:
            for q_expr in queries: # q_expr is an IbisTable
                # Only process aggregate queries for tiling
                op_name = getattr(q_expr.op(), 'name', None)
                if op_name == "aggregate": 
                    tile_ibis_expr = self._create_tile_query(q_expr, tile, spec_dict)
                    tile_ibis_expressions.append(tile_ibis_expr)
        
        return tile_ibis_expressions, strategy
    
    def _create_tile_query(
        self,
        base_ibis_expr: IbisTable, # Now expects an IbisTable
        tile: TileKey,
        spec_dict: Dict[str, Any]
    ) -> IbisTable: # Now returns an IbisTable
        """
        Create an Ibis expression modified to fetch a specific tile.
        """
        spec_limit = spec_dict.get("limit", self.tile_size)
        tile_limit = tile.row_end - tile.row_start
        effective_limit = min(tile_limit, spec_limit)

        if hasattr(base_ibis_expr, 'limit'):
            tile_ibis_expr = base_ibis_expr.limit(effective_limit, offset=tile.row_start)
            return tile_ibis_expr
        else:
             return base_ibis_expr    

    def _cache_key_for_tile(
        self,
        tile: TileKey,
        spec_dict: Dict[str, Any],
        ibis_expr: IbisTable # Now pass the Ibis expression for better key
    ) -> str:
        """Generate cache key for a specific tile"""
        spec_no_page = spec_dict.copy()
        spec_no_page.pop("page", None)
        
        base_hash = self._hash_dict(spec_no_page)
        tile_str = tile.to_string()
        
        try:
            ibis_hash = hashlib.sha256(str(self.backend.compile(ibis_expr)).encode('utf-8')).hexdigest()[:16]
            return f"pivot:tile:{base_hash}:{tile_str}:{ibis_hash}"
        except Exception as e:
            return f"pivot:tile:{base_hash}:{tile_str}:{hashlib.sha256(str(ibis_expr).encode('utf-8')).hexdigest()[:16]}"
    
    # ========== Spec Diffing ==========
    
    def _analyze_spec_change(
        self,
        current: Dict[str, Any],
        previous: Optional[Dict[str, Any]]
    ) -> SpecChangeType:
        """
        Analyze semantic differences between specs with enhanced analysis.
        """
        if previous is None:
            return SpecChangeType.FULL_REFRESH

        if current == previous:
            return SpecChangeType.IDENTICAL

        page_changed = current.get("page") != previous.get("page")
        sort_changed = current.get("sort") != previous.get("sort")
        filters_changed = current.get("filters", []) != previous.get("filters", [])
        limits_changed = current.get("limit") != previous.get("limit")
        cursor_changed = current.get("cursor") != previous.get("cursor")

        rows_changed = current.get("rows", []) != previous.get("rows", [])
        columns_changed = current.get("columns", []) != previous.get("columns", [])
        measures_changed = self._compare_measures(current.get("measures", []), previous.get("measures", []))
        table_changed = current.get("table") != previous.get("table")
        drill_paths_changed = current.get("drill_paths", []) != previous.get("drill_paths", [])
        totals_changed = current.get("totals", False) != previous.get("totals", False)

        if (page_changed or limits_changed or cursor_changed) and not any([
            sort_changed, filters_changed, rows_changed, columns_changed,
            measures_changed, table_changed, drill_paths_changed, totals_changed
        ]):
            return SpecChangeType.PAGE_ONLY

        if sort_changed and not any([
            page_changed, filters_changed, rows_changed, columns_changed,
            measures_changed, table_changed, drill_paths_changed, limits_changed,
            cursor_changed, totals_changed
        ]):
            return SpecChangeType.SORT_ONLY

        if filters_changed and not any([
            page_changed, sort_changed, rows_changed, columns_changed,
            measures_changed, table_changed, drill_paths_changed, limits_changed,
            cursor_changed, totals_changed
        ]):
            curr_filters = set(json.dumps(f, sort_keys=True) for f in current.get("filters", []))
            prev_filters = set(json.dumps(f, sort_keys=True) for f in previous.get("filters", []))

            if len(curr_filters) > len(prev_filters) and curr_filters.issuperset(prev_filters):
                return SpecChangeType.FILTER_ADDED
            elif len(prev_filters) > len(curr_filters) and prev_filters.issuperset(curr_filters):
                return SpecChangeType.FILTER_REMOVED
            else:
                return SpecChangeType.FULL_REFRESH

        if any([rows_changed, columns_changed, measures_changed, table_changed]):
            return SpecChangeType.STRUCTURE_CHANGED

        return SpecChangeType.FULL_REFRESH

    def _compare_measures(self, measures1: List[Dict[str, Any]], measures2: List[Dict[str, Any]]) -> bool:
        if len(measures1) != len(measures2):
            return True

        def normalize_measure(m):
            if isinstance(m, dict):
                return tuple(sorted((k, v) for k, v in m.items() if k != 'filter_condition')) 
            return str(m)

        measures1_norm = [normalize_measure(m) for m in measures1]
        measures2_norm = [normalize_measure(m) for m in measures2]

        return sorted(measures1_norm) != sorted(measures2_norm)
    
    def merge_delta_results(
        self,
        base_results: List[Dict[str, Any]],
        delta_results: List[Dict[str, Any]],
        measures: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        base_map = {}
        for row in base_results:
            key_parts = []
            for k, v in row.items():
                if not any(m.get("alias") == k for m in measures):
                    key_parts.append(f"{k}:{v}")
            key = "|".join(key_parts)
            base_map[key] = row
        
        for delta_row in delta_results:
            key_parts = []
            for k, v in delta_row.items():
                if not any(m.get("alias") == k for m in measures):
                    key_parts.append(f"{k}:{v}")
            key = "|".join(key_parts)
            
            if key in base_map:
                base_row = base_map[key]
                for measure in measures:
                    alias = measure.get("alias")
                    agg = measure.get("agg", "sum").lower()
                    
                    if agg in {"sum", "count"}:
                        base_row[alias] = base_row.get(alias, 0) + delta_row.get(alias, 0)
                    elif agg == "min":
                        base_row[alias] = min(base_row.get(alias, float('inf')), delta_row.get(alias, float('inf')))
                    elif agg == "max":
                        base_row[alias] = max(base_row.get(alias, float('-inf')), delta_row.get(alias, float('-inf')))
            else:
                base_map[key] = delta_row
        
        return list(base_map.values())
    
    # ========== Helper Methods ==========
    
    def _is_query_cached(
        self,
        ibis_expr: IbisTable, 
        spec_dict: Dict[str, Any]
    ) -> bool:
        cache_key = self._cache_key_for_query(ibis_expr, spec_dict)
        return self.cache.get(cache_key) is not None
    
    def _cache_key_for_query(
        self,
        ibis_expr: IbisTable, 
        spec_dict: Dict[str, Any]
    ) -> str:
        try:
            compiled_sql = str(self.backend.compile(ibis_expr))
            spec_hash = hashlib.sha256(json.dumps(spec_dict, sort_keys=True, default=str).encode()).hexdigest()[:16]
            key_str = f"{compiled_sql}-{spec_hash}"
            key_hash = hashlib.sha256(key_str.encode()).hexdigest()[:32]
            return f"pivot_ibis:query:{key_hash}"
        except Exception as e:
            return f"pivot_ibis:query_fallback:{hashlib.sha256(str(ibis_expr).encode('utf-8')).hexdigest()[:32]}"
    
    def _digest_plan(
        self,
        plan_result: Dict[str, Any], 
        spec_dict: Dict[str, Any]
    ) -> str:
        queries_digest = ""
        for q_expr in plan_result.get("queries", []):
            try:
                queries_digest += str(self.backend.compile(q_expr))
            except Exception as e:
                queries_digest += str(q_expr) 
        
        plan_summary = {
            "metadata": plan_result.get("metadata"),
            "queries_digest": hashlib.sha256(queries_digest.encode('utf-8')).hexdigest()[:32],
            "spec_hash": self._hash_dict(spec_dict)
        }
        return self._hash_dict(plan_summary)
    
    def _normalize_spec_for_hash(self, spec: Any) -> Dict[str, Any]:
        if hasattr(spec, "to_dict"):
            return spec.to_dict()
        if hasattr(spec, "__dict__"):
            d = dict(spec.__dict__)
            if "page" in d and hasattr(d["page"], "__dict__"):
                d["page"] = dict(d["page"].__dict__)
            return d
        if isinstance(spec, dict):
            return spec
        return {"spec": str(spec)}
    
    def _hash_dict(self, d: Dict[str, Any]) -> str:
        j = json.dumps(d, sort_keys=True, default=str)
        return hashlib.sha256(j.encode("utf-8")).hexdigest()

    async def compute_true_total(self, spec: PivotSpec) -> int:
        """
        Compute the true total row count using an Ibis expression.
        """
        if self.backend is None:
            raise ValueError("Ibis backend must be provided to QueryDiffEngine for computing true total.")
        
        base_table = self.backend.table(spec.table)

        filtered_table = base_table
        if spec.filters:
            from pivot_engine.common.ibis_expression_builder import IbisExpressionBuilder
            builder = IbisExpressionBuilder(self.backend)
            filter_expr = builder.build_filter_expression(base_table, spec.filters)
            if filter_expr is not None:
                filtered_table = filtered_table.filter(filter_expr)

        return filtered_table.count().execute()
