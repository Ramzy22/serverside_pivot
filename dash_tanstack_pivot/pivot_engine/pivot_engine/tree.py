"""
Tree Expansion State Management for Hierarchical Pivots

Manages the expansion state of row hierarchies (e.g., Region > State > City)
for lazy loading of child nodes.
"""

from typing import Dict, Any, List, Optional, Tuple, Set, Callable, Generator
from dataclasses import dataclass, field
import time
import hashlib
import json


@dataclass
class ExpansionState:
    """Tracks expansion state for a tree"""
    expanded_paths: Set[str] = field(default_factory=set)
    timestamp: float = field(default_factory=time.time)

    def path_to_key(self, path: List[str]) -> str:
        """Convert path to string key"""
        return "|||".join(str(v) for v in path)

    def is_expanded(self, path: List[str]) -> bool:
        """Check if path is expanded"""
        return self.path_to_key(path) in self.expanded_paths

    def expand(self, path: List[str]):
        """Mark path as expanded"""
        self.expanded_paths.add(self.path_to_key(path))
        self.timestamp = time.time()

    def collapse(self, path: List[str]):
        """Mark path as collapsed"""
        key = self.path_to_key(path)
        self.expanded_paths.discard(key)
        self.timestamp = time.time()


@dataclass
class BuildContext:
    """Context for building hierarchy levels"""
    base_spec: Dict[str, Any]
    spec_hash: str
    state: ExpansionState  # Now this is defined
    dimension_hierarchy: List[str]
    path_cursor_map: Dict[str, Dict[str, Any]]
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    chunk_size: int = 1000
    prefetch_depth: int = 0
    is_progressive: bool = False
    is_chunked: bool = False
    is_prefetch: bool = False
    node_processor: Optional[Callable[[Dict[str, Any], List[str]], None]] = None


class TreeStateCache:
    """Caches tree expansion states for hierarchical pivots."""

    def __init__(self, cache, ttl: int = 600):
        self.cache = cache
        self.ttl = ttl
        self._expansion_states: Dict[str, ExpansionState] = {}

    def get_expansion_state(self, spec_hash: str) -> ExpansionState:
        if spec_hash not in self._expansion_states:
            cache_key = f"tree:expansion:{spec_hash}"
            cached = self.cache.get(cache_key)
            if cached:
                self._expansion_states[spec_hash] = self._deserialize_state(cached)
            else:
                self._expansion_states[spec_hash] = ExpansionState()
        return self._expansion_states[spec_hash]

    def save_expansion_state(self, spec_hash: str, state: ExpansionState):
        cache_key = f"tree:expansion:{spec_hash}"
        self.cache.set(cache_key, self._serialize_state(state), ttl=self.ttl)

    def toggle_expansion(self, spec_hash: str, path: List[str]) -> Tuple[ExpansionState, bool]:
        state = self.get_expansion_state(spec_hash)
        was_expanded = state.is_expanded(path)
        if was_expanded:
            state.collapse(path)
        else:
            state.expand(path)
        self.save_expansion_state(spec_hash, state)
        return state, not was_expanded

    def _serialize_state(self, state: ExpansionState) -> str:
        return json.dumps({
            "expanded_paths": list(state.expanded_paths),
            "timestamp": state.timestamp
        })

    def _deserialize_state(self, data: str) -> ExpansionState:
        obj = json.loads(data)
        return ExpansionState(
            expanded_paths=set(obj.get("expanded_paths", [])),
            timestamp=obj.get("timestamp", time.time())
        )


class TreeExpansionManager:
    """
    Orchestrates hierarchical pivot operations.
    """

    def __init__(self, controller, tree_cache: Optional[TreeStateCache] = None):
        self.controller = controller
        self.tree_cache = tree_cache or TreeStateCache(controller.cache, ttl=600)

    async def run_hierarchical_pivot(
        self,
        spec: Dict[str, Any],
        path_cursor_map: Optional[Dict[str, Dict[str, Any]]] = None,
        flatten: bool = False,  # New parameter to control output format
        start_row: int = 0,     # For virtual scrolling support
        end_row: int = None,    # For virtual scrolling support
        expanded_paths: List[List[str]] = None  # Explicitly pass expanded paths for virtual scrolling
    ) -> Dict[str, Any]:
        spec_hash = self._hash_spec(spec)
        dimension_hierarchy = spec.get("rows", [])
        if not dimension_hierarchy:
            # Fallback to standard pivot if no hierarchy is defined
            return await self.controller.run_pivot_async(spec, return_format="dict")

        state = self.tree_cache.get_expansion_state(spec_hash)

        # Use provided expanded_paths or get from state
        if expanded_paths is None:
            # Extract expanded paths from state for compatibility
            expanded_paths = [path.split('|||') for path in state.expanded_paths if path]
        else:
            # Update state with provided paths
            for path in expanded_paths:
                state.expand(path)

        # For virtual scrolling with large datasets, delegate to virtual scroll manager
        # if it's available in the controller and start_row/end_row are specified
        if (start_row != 0 or end_row is not None) and hasattr(self.controller, 'virtual_scroll_manager'):
            if self.controller.virtual_scroll_manager:
                # Use the virtual scroll manager for efficient loading of visible rows
                from pivot_engine.types.pivot_spec import PivotSpec
                pivot_spec = PivotSpec.from_dict(spec)

                # Convert string paths back to list format for virtual scroll
                string_paths = [list(map(str, path)) for path in expanded_paths]

                visible_rows = self.controller.virtual_scroll_manager.get_visible_rows_hierarchical(
                    pivot_spec, start_row, end_row or (start_row + 100), string_paths
                )

                # If flatten is requested, ensure proper format (it should already be flat from virtual scroll)
                result_rows = visible_rows

                return {
                    "rows": result_rows,
                    "expansion_state": {
                        "expanded_paths": [state.path_to_key(path) for path in expanded_paths],
                        "timestamp": state.timestamp
                    },
                    "spec_hash": spec_hash,
                    "total_visible_rows": self.controller.virtual_scroll_manager.get_total_visible_row_count(
                        pivot_spec, string_paths
                    ) if hasattr(self.controller.virtual_scroll_manager, 'get_total_visible_row_count') else len(result_rows)
                }

        # Build the tree recursively, starting at the root (for smaller datasets or full view)
        tree = await self._build_level(
            base_spec=spec,
            spec_hash=spec_hash,
            state=state,
            parent_path=[],
            dimension_hierarchy=dimension_hierarchy,
            level=0,
            path_cursor_map=path_cursor_map or {}
        )

        # If flatten is True, convert the nested tree to a flat list
        if flatten:
            flattened_rows = self._flatten_tree(tree, dimension_hierarchy, 0)
            result_rows = flattened_rows
        else:
            result_rows = tree

        return {
            "rows": result_rows,
            "expansion_state": {
                "expanded_paths": list(state.expanded_paths),
                "timestamp": state.timestamp
            },
            "spec_hash": spec_hash,
        }

    def _flatten_tree(self, tree: List[Dict[str, Any]], hierarchy: List[str], current_level: int, parent_depth: int = 0) -> List[Dict[str, Any]]:
        """Convert nested tree structure to flat list with depth indicators."""
        flattened = []

        for node in tree:
            # Create a copy of the node to avoid modifying the original
            flat_node = node.copy()

            # Add depth indicator for UI rendering
            flat_node['depth'] = parent_depth

            # Add indentation indicator for UI (optional, can be used for visual hierarchy)
            flat_node['_indent'] = parent_depth

            # Remove children temporarily to add them after this node
            children = flat_node.pop('children', [])

            # Add the current node to the flattened list
            flattened.append(flat_node)

            # Recursively flatten children if they exist
            if children:
                flattened.extend(
                    self._flatten_tree(children, hierarchy, current_level + 1, parent_depth + 1)
                )

        return flattened

    async def _build_level_base(
        self,
        base_spec: Dict[str, Any],
        spec_hash: str,
        state: ExpansionState,
        parent_path: List[str],
        dimension_hierarchy: List[str],
        level: int,
        path_cursor_map: Dict[str, Dict[str, Any]],
        # Additional parameters for customization
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        chunk_size: int = 1000,
        prefetch_depth: int = 0,
        is_progressive: bool = False,
        is_chunked: bool = False,
        is_prefetch: bool = False,
        # Callback for processing nodes before recursion
        node_processor: Optional[Callable[[Dict[str, Any], List[str]], None]] = None
    ) -> List[Dict[str, Any]]:
        """Base method to build a level of the hierarchy with common logic."""
        # Create BuildContext to reduce parameter passing
        context = BuildContext(
            base_spec=base_spec,
            spec_hash=spec_hash,
            state=state,
            dimension_hierarchy=dimension_hierarchy,
            path_cursor_map=path_cursor_map,
            progress_callback=progress_callback,
            chunk_size=chunk_size,
            prefetch_depth=prefetch_depth,
            is_progressive=is_progressive,
            is_chunked=is_chunked,
            is_prefetch=is_prefetch,
            node_processor=node_processor
        )

        return await self._build_level_with_context(context, parent_path, level)

    async def _build_level_with_context(
        self,
        context: BuildContext,
        parent_path: List[str],
        level: int
    ) -> List[Dict[str, Any]]:
        """Internal method that uses BuildContext to reduce parameter count."""
        if level >= len(context.dimension_hierarchy):
            return []

        current_dimension = context.dimension_hierarchy[level]
        path_key = context.state.path_to_key(parent_path)
        cursor = context.path_cursor_map.get(path_key)

        level_spec = self._create_level_spec(context.base_spec, parent_path, current_dimension, cursor)

        result_dict = await self._get_pivot_result(level_spec)

        # Handle potential None result
        if not result_dict:
            return []

        rows = result_dict.get("rows", [])
        columns = result_dict.get("columns", [])
        nodes = self._results_to_dict(rows, columns)

        # If this level has a next_cursor, add it to a special node
        if result_dict.get("next_cursor"):
            # This can be handled by the UI to show a "Load More" button
            # Using a more structured approach for internal metadata
            nodes.append({
                "_type": "load_more",  # More descriptive than _is_load_more_node
                "next_cursor": result_dict["next_cursor"],
                "parent_path_key": path_key,
            })

        for node in nodes:
            # Check if this is a special metadata node
            if node.get("_type") == "load_more":
                continue

            node_path = parent_path + [node.get(current_dimension)]  # Use get() to be safe
            if not node_path[-1]:  # Skip if the dimension value is None/empty
                continue

            if context.state.is_expanded(node_path):
                # Apply node processor if provided
                if context.node_processor:
                    context.node_processor(node, node_path)

                # Determine the next level call based on the type of build
                if context.is_progressive:
                    # For progressive, create a new context with appropriate flags
                    new_context = BuildContext(
                        base_spec=context.base_spec,
                        spec_hash=context.spec_hash,
                        state=context.state,
                        dimension_hierarchy=context.dimension_hierarchy,
                        path_cursor_map=context.path_cursor_map,
                        progress_callback=context.progress_callback,
                        chunk_size=context.chunk_size,
                        prefetch_depth=context.prefetch_depth,
                        is_progressive=True,
                        is_chunked=False,
                        is_prefetch=False
                    )
                    node["children"] = await self._build_level_with_context(
                        new_context, node_path, level + 1
                    )
                elif context.is_chunked:
                    new_context = BuildContext(
                        base_spec=context.base_spec,
                        spec_hash=context.spec_hash,
                        state=context.state,
                        dimension_hierarchy=context.dimension_hierarchy,
                        path_cursor_map=context.path_cursor_map,
                        chunk_size=context.chunk_size,
                        is_chunked=True
                    )
                    node["children"] = await self._build_level_with_context(
                        new_context, node_path, level + 1
                    )
                elif context.is_prefetch:
                    # If prefetching is enabled and we haven't exceeded prefetch depth
                    if context.prefetch_depth > 0:
                        # Prefetch the next level
                        new_context = BuildContext(
                            base_spec=context.base_spec,
                            spec_hash=context.spec_hash,
                            state=context.state,
                            dimension_hierarchy=context.dimension_hierarchy,
                            path_cursor_map=context.path_cursor_map,
                            prefetch_depth=context.prefetch_depth - 1,
                            is_prefetch=True
                        )
                        node["children"] = await self._build_level_with_context(
                            new_context, node_path, level + 1
                        )
                    else:
                        # Just mark that children exist without loading them
                        node["children"] = []  # Will be loaded on demand
                        node["_has_children"] = True
                else:
                    # Standard recursive call
                    node["children"] = await self._build_level_with_context(
                        context, node_path, level + 1
                    )

                # For prefetch scenario, if current level is not expanded but could have children
                if context.is_prefetch and not context.state.is_expanded(node_path) and level < len(context.dimension_hierarchy) - 1:
                    node["_prefetchable"] = True

        return nodes

    async def _build_level(
        self,
        base_spec: Dict[str, Any],
        spec_hash: str,
        state: ExpansionState,
        parent_path: List[str],
        dimension_hierarchy: List[str],
        level: int,
        path_cursor_map: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Recursively build a level of the hierarchy with pagination."""
        return await self._build_level_base(
            base_spec, spec_hash, state, parent_path,
            dimension_hierarchy, level, path_cursor_map
        )

    def _create_level_spec(
        self,
        base_spec: Dict[str, Any],
        parent_path: List[str],
        dimension: str,
        cursor: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a pivot spec for a specific level of the hierarchy."""
        dimension_hierarchy = base_spec.get("rows", [])
        path_filters = []
        for i, value in enumerate(parent_path):
            path_filters.append({
                "field": dimension_hierarchy[i],
                "op": "=",
                "value": value
            })

        all_filters = base_spec.get("filters", []) + path_filters
        
        # Ensure a stable sort order for pagination
        sort = base_spec.get("sort", [])
        if not sort:
            sort = [{"field": dimension, "order": "asc"}]

        return {
            "table": base_spec["table"],
            "rows": [dimension],
            "full_rows": base_spec.get("rows", []),
            "columns": base_spec.get("columns", []),
            "measures": base_spec.get("measures", []),
            "filters": all_filters,
            "sort": sort,
            "limit": base_spec.get("limit", 100),
            "cursor": cursor,
            "pivot_config": base_spec.get("pivot_config"),
            "grouping_config": base_spec.get("grouping_config"),
        }

    def _results_to_dict(
        self,
        rows: List[List[Any]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        """Convert flat query results into a list of dictionary nodes."""
        nodes = []

        if not rows or not columns:
            return []

        for row_data in rows:
            node = {}

            # Make sure row_data and columns are properly aligned
            if not row_data:
                continue

            # Use min to ensure we don't access beyond the row data length
            for idx, col in enumerate(columns):
                if idx < len(row_data):
                    node[col] = row_data[idx]
                else:
                    # If the row has fewer values than columns, set to None
                    node[col] = None

            nodes.append(node)

        return nodes

    async def _get_pivot_result(self, spec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Safely get pivot result with error handling."""
        try:
            result = await self.controller.run_pivot_async(spec, return_format="dict")

            # Validate that result has required keys
            if not isinstance(result, dict):
                print(f"Warning: pivot result is not a dictionary: {type(result)}")
                return None

            if "rows" not in result or "columns" not in result:
                print(f"Warning: pivot result missing required keys. Got: {list(result.keys())}")
                return None

            return result
        except Exception as e:
            print(f"Error executing pivot: {e}")
            return None

    def _hash_spec(self, spec: Dict[str, Any]) -> str:
        """Generate a consistent hash for a pivot spec."""
        hashable = {k: v for k, v in spec.items() if k not in {"page", "expansion_state"}}
        return hashlib.sha256(
            json.dumps(hashable, sort_keys=True, default=str).encode()
        ).hexdigest()[:32]  # Use 32 characters instead of 16 to reduce collision risk

    def toggle_expansion(self, spec_hash: str, path: List[str]) -> Dict[str, Any]:
        """Toggle the expansion state for a given path."""
        state, is_now_expanded = self.tree_cache.toggle_expansion(spec_hash, path)
        return {
            "expanded": is_now_expanded,
            "path": path,
            "spec_hash": spec_hash,
        }

    async def run_hierarchical_pivot_progressive(
        self,
        spec: Dict[str, Any],
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Dict[str, Any]:
        """Build and return tree progressively with intermediate results"""
        spec_hash = self._hash_spec(spec)
        dimension_hierarchy = spec.get("rows", [])
        if not dimension_hierarchy:
            # Fallback to standard pivot if no hierarchy is defined
            result = await self.controller.run_pivot_async(spec, return_format="dict")
            return result

        state = self.tree_cache.get_expansion_state(spec_hash)

        # Build the tree progressively, starting at the root
        tree = await self._build_level_progressive(
            base_spec=spec,
            spec_hash=spec_hash,
            state=state,
            parent_path=[],
            dimension_hierarchy=dimension_hierarchy,
            level=0,
            path_cursor_map={},
            progress_callback=progress_callback
        )

        result = {
            "rows": tree,
            "expansion_state": {
                "expanded_paths": list(state.expanded_paths),
                "timestamp": state.timestamp
            },
            "spec_hash": spec_hash,
        }

        if progress_callback:
            # Send final result
            progress_callback({"rows": tree, "expansion_state": result["expansion_state"], "spec_hash": spec_hash, "partial": False})

        return result

    async def _build_level_progressive(
        self,
        base_spec: Dict[str, Any],
        spec_hash: str,
        state: ExpansionState,
        parent_path: List[str],
        dimension_hierarchy: List[str],
        level: int,
        path_cursor_map: Dict[str, Dict[str, Any]],
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        current_tree: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """Recursively build a level of the hierarchy with progressive updates."""
        if current_tree is None:
            current_tree = []

        # For progressive loading, we'll use a different approach since we're building into a single tree
        # This method has different logic than the base method, so we keep it separate
        if level >= len(dimension_hierarchy):
            return current_tree

        current_dimension = dimension_hierarchy[level]
        path_key = state.path_to_key(parent_path)

        # Load data for this level
        level_spec = self._create_level_spec(base_spec, parent_path, current_dimension, None)
        result_dict = await self._get_pivot_result(level_spec)

        # Handle potential None result
        if not result_dict:
            return current_tree

        rows = result_dict.get("rows", [])
        columns = result_dict.get("columns", [])
        nodes = self._results_to_dict(rows, columns)

        # Add "Load More" node if there are more results
        if result_dict.get("next_cursor"):
            nodes.append({
                "_type": "load_more",  # More descriptive and consistent
                "next_cursor": result_dict["next_cursor"],
                "parent_path_key": path_key,
            })

        # Add nodes to the current tree
        for node in nodes:
            # Check if this is a special metadata node
            if node.get("_type") == "load_more":
                current_tree.append(node)
            else:
                node_path = parent_path + [node.get(current_dimension)]  # Use .get() for safety

                # Add the node to the tree
                current_tree.append(node)

                # If progress callback is provided, send an update
                if progress_callback and level == 0:  # Send update after each top-level node
                    progress_callback({
                        "rows": current_tree.copy(),
                        "expansion_state": {"expanded_paths": list(state.expanded_paths), "timestamp": state.timestamp},
                        "spec_hash": spec_hash,
                        "partial": True,
                        "level": level,
                        "node_count": len(current_tree)
                    })

                # Process children if this path is expanded
                if state.is_expanded(node_path):
                    children = []
                    # Use the base method for the recursive call
                    children_result = await self._build_level_base(
                        base_spec, spec_hash, state, node_path, dimension_hierarchy,
                        level + 1, path_cursor_map,
                        progress_callback=progress_callback,
                        is_progressive=True
                    )
                    node["children"] = children_result

        return current_tree

    async def _build_tree_chunks(
        self,
        spec: Dict[str, Any],
        path_cursor_map: Optional[Dict[str, Dict[str, Any]]] = None,
        chunk_size: int = 1000
    ) -> Generator[Dict[str, Any], None, None]:
        """Generate tree chunks for streaming"""
        spec_hash = self._hash_spec(spec)
        dimension_hierarchy = spec.get("rows", [])
        if not dimension_hierarchy:
            # Fallback to standard pivot if no hierarchy is defined
            result = await self.controller.run_pivot_async(spec, return_format="dict")
            yield result
            return

        state = self.tree_cache.get_expansion_state(spec_hash)

        # Build the tree in chunks
        chunk = {
            "rows": [],
            "expansion_state": {
                "expanded_paths": list(state.expanded_paths),
                "timestamp": state.timestamp
            },
            "spec_hash": spec_hash,
            "chunk_info": {
                "chunk_size": chunk_size,
                "chunk_number": 0
            }
        }

        # Build the tree level by level, yielding chunks
        rows = await self._build_level_chunked(
            base_spec=spec,
            spec_hash=spec_hash,
            state=state,
            parent_path=[],
            dimension_hierarchy=dimension_hierarchy,
            level=0,
            path_cursor_map=path_cursor_map or {},
            chunk_size=chunk_size
        )

        # Yield the complete chunk for now, in a more advanced implementation,
        # we would yield partial chunks as they fill up
        chunk["rows"] = rows
        yield chunk

    async def _build_level_chunked(
        self,
        base_spec: Dict[str, Any],
        spec_hash: str,
        state: ExpansionState,
        parent_path: List[str],
        dimension_hierarchy: List[str],
        level: int,
        path_cursor_map: Dict[str, Dict[str, Any]],
        chunk_size: int
    ) -> List[Dict[str, Any]]:
        """Build a level of the hierarchy, considering chunk size."""
        # Use the base method with chunked flag
        return await self._build_level_base(
            base_spec, spec_hash, state, parent_path, dimension_hierarchy, level,
            path_cursor_map, chunk_size=chunk_size, is_chunked=True
        )

    async def _build_level_with_prefetch(
        self,
        base_spec: Dict[str, Any],
        spec_hash: str,
        state: ExpansionState,
        parent_path: List[str],
        dimension_hierarchy: List[str],
        level: int,
        path_cursor_map: Dict[str, Dict[str, Any]],
        prefetch_depth: int = 1  # Number of levels to prefetch
    ) -> List[Dict[str, Any]]:
        """Build level with optional prefetching of child levels"""
        # Use the base method with prefetch flag
        return await self._build_level_base(
            base_spec, spec_hash, state, parent_path, dimension_hierarchy,
            level, path_cursor_map, prefetch_depth=prefetch_depth, is_prefetch=True
        )

    async def _load_multiple_levels_batch(
        self,
        base_spec: Dict[str, Any],
        expanded_paths: List[List[str]],
        max_levels: int
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Load multiple levels batching by depth (D queries instead of N).
        This optimizes performance for "millions of rows" by minimizing database round-trips.
        """
        results = {}
        rows = base_spec.get("rows", [])
        if not rows:
            return results

        # Check for "Expand All" wildcard
        load_all = any(p == ['__ALL__'] for p in expanded_paths)

        # 1. Always load root level (Depth 1)
        # Spec for root level
        try:
            root_spec = self._create_level_spec_batch(base_spec, (), rows[0])
            root_spec["totals"] = base_spec.get("totals", True)
            root_spec["full_rows"] = base_spec.get("rows", []) # Ensure full hierarchy is passed
            
            root_result = await self.controller.run_pivot_async(root_spec, return_format="dict")
            if root_result and "rows" in root_result:
                results[""] = self._results_to_dict(root_result["rows"], root_result["columns"])
        except Exception as e:
            print(f"Error loading root level: {e}")

        if load_all:
            # Load ALL levels up to max_levels without filtering by parent path
            # We iterate starting from depth 1 (children of root)
            # depth 0 is root (already loaded).
            # expanded_paths usually implies we want children of these paths.
            # If load_all, we want children of EVERYTHING.
            # So we load depth 1 (children of root), depth 2 (children of depth 1), etc.
            
            for depth in range(1, min(len(rows), max_levels + 1)):
                target_dim = rows[depth]
                parent_dims = rows[:depth]
                
                # Create spec for this entire level
                # Group by all parent dims + target
                group_rows = parent_dims + [target_dim]
                
                # Ensure stable sort
                sort = base_spec.get("sort", [])
                if not sort:
                    sort = [{"field": dim, "order": "asc"} for dim in group_rows]

                level_spec = {
                    "table": base_spec["table"],
                    "rows": group_rows,
                    "full_rows": base_spec.get("rows", []),
                    "columns": base_spec.get("columns", []),
                    "measures": base_spec.get("measures", []),
                    "filters": base_spec.get("filters", []),
                    "sort": sort,
                    "limit": base_spec.get("limit", 100000), # Higher limit for full load
                    "pivot_config": base_spec.get("pivot_config"),
                    "grouping_config": base_spec.get("grouping_config"),
                    "totals": False, 
                }

                try:
                    batch_result = await self.controller.run_pivot_async(level_spec, return_format="dict")
                    if batch_result and "rows" in batch_result:
                        # Distribute with no path validation (accept all)
                        self._distribute_batch_results(batch_result, results, parent_dims, None)
                except Exception as e:
                    print(f"Error loading all nodes at depth {depth}: {e}")

        else:
            # Standard selective expansion logic
            # 2. Organize expanded paths by depth
            paths_by_depth = {}
            for path in expanded_paths:
                depth = len(path)
                # Check if we can go deeper
                if depth < len(rows) and depth < max_levels:
                    if depth not in paths_by_depth:
                        paths_by_depth[depth] = []
                    paths_by_depth[depth].append(path)

            # 3. For each depth, query children of all expanded parents at that depth
            for depth, paths in paths_by_depth.items():
                target_dim = rows[depth]
                
                # Create a spec that fetches children for ALL these parents in one query
                batch_spec = self._create_batch_depth_spec(base_spec, paths, target_dim)
                batch_spec["full_rows"] = base_spec.get("rows", []) # Ensure full hierarchy is passed
                
                try:
                    batch_result = await self.controller.run_pivot_async(batch_spec, return_format="dict")
                    
                    # Distribute results back to their specific parent keys using strict validation
                    if batch_result and "rows" in batch_result:
                        self._distribute_batch_results(batch_result, results, rows[:depth], paths)
                except Exception as e:
                    print(f"Error loading batch depth {depth}: {e}")

        return results

    def _create_batch_depth_spec(
        self,
        base_spec: Dict[str, Any],
        parent_paths: List[List[str]],
        target_dim: str
    ) -> Dict[str, Any]:
        """Create a single pivot spec to fetch children for multiple parents."""
        parent_dims = base_spec["rows"][:len(parent_paths[0])]
        
        # We need a filter that matches (Dim1=Val1 AND Dim2=Val2...) OR (Dim1=Val1b AND Dim2=Val2b...)
        # Ibis handles complex OR expressions well. To represent this in PivotSpec dict:
        # We use a custom 'composite_in' operator if supported, or a list of ORed filters.
        
        if len(parent_dims) == 1:
            values = [p[0] for p in parent_paths]
            batch_filter = {
                "field": parent_dims[0],
                "op": "in",
                "value": values
            }
            filters = base_spec.get("filters", []) + [batch_filter]
        else:
            # Complex composite filtering
            # For now, we'll use the 'in' sets approach but make it stricter in distribution
            # or use a special internal 'paths' filter that IbisPlanner can optimize.
            filters = base_spec.get("filters", []).copy()
            for i, dim in enumerate(parent_dims):
                unique_vals = list(set(p[i] for p in parent_paths))
                filters.append({
                    "field": dim,
                    "op": "in",
                    "value": unique_vals
                })
        
        # CRITICAL: We must group by ALL parent dimensions + target dimension
        # to ensure we can correctly identify which parent each child belongs to.
        group_rows = parent_dims + [target_dim]
        
        # Ensure stable sort order
        sort = base_spec.get("sort", [])
        if not sort:
            sort = [{"field": dim, "order": "asc"} for dim in group_rows]
        
        return {
            "table": base_spec["table"],
            "rows": group_rows,
            "full_rows": base_spec.get("rows", []),
            "columns": base_spec.get("columns", []),
            "measures": base_spec.get("measures", []),
            "filters": filters,
            "sort": sort, 
            "limit": base_spec.get("limit", 1000) * len(parent_paths),
            "pivot_config": base_spec.get("pivot_config"),
            "grouping_config": base_spec.get("grouping_config"),
            "totals": False, # Children queries shouldn't return grand totals
        }

    def _distribute_batch_results(
        self,
        batch_result: Dict[str, Any],
        results: Dict[str, List[Dict[str, Any]]],
        parent_dims: List[str],
        valid_parent_paths: List[List[str]] = None
    ):
        """Distribute flat batch results into per-parent buckets with strict path validation."""
        rows = batch_result.get("rows", [])
        cols = batch_result.get("columns", [])
        col_indices = {name: i for i, name in enumerate(cols)}
        parent_indices = [col_indices[d] for d in parent_dims if d in col_indices]
        
        # Convert valid paths to strings for fast lookup
        valid_keys = set("|||".join(str(v) for v in p) for p in valid_parent_paths) if valid_parent_paths else None
        
        for row in rows:
            parent_key_parts = []
            for idx in parent_indices:
                val = row[idx]
                parent_key_parts.append(str(val))
            
            parent_key = "|||".join(parent_key_parts)
            
            # Strict validation: Only accept if this path was explicitly requested
            if valid_keys and parent_key not in valid_keys:
                continue
                
            if parent_key not in results:
                results[parent_key] = []
            
            node = {}
            for i, col_name in enumerate(cols):
                node[col_name] = row[i]
            
            results[parent_key].append(node)


    async def run_hierarchical_pivot_with_prefetch(
        self,
        spec: Dict[str, Any],
        path_cursor_map: Optional[Dict[str, Dict[str, Any]]] = None,
        prefetch_depth: int = 1
    ) -> Dict[str, Any]:
        """Run hierarchical pivot with prefetching of expanded nodes"""
        spec_hash = self._hash_spec(spec)
        dimension_hierarchy = spec.get("rows", [])
        if not dimension_hierarchy:
            # Fallback to standard pivot if no hierarchy is defined
            return await self.controller.run_pivot_async(spec, return_format="dict")

        state = self.tree_cache.get_expansion_state(spec_hash)

        # Build the tree with prefetching
        tree = await self._build_level_with_prefetch(
            base_spec=spec,
            spec_hash=spec_hash,
            state=state,
            parent_path=[],
            dimension_hierarchy=dimension_hierarchy,
            level=0,
            path_cursor_map=path_cursor_map or {},
            prefetch_depth=prefetch_depth
        )

        return {
            "rows": tree,
            "expansion_state": {
                "expanded_paths": list(state.expanded_paths),
                "timestamp": state.timestamp
            },
            "spec_hash": spec_hash,
        }

    def _create_level_spec_batch(
        self,
        base_spec: Dict[str, Any],
        parent_path: tuple,
        dimension: str,
    ) -> Dict[str, Any]:
        """Create a pivot spec for a specific level of the hierarchy, optimized for batching."""
        dimension_hierarchy = base_spec.get("rows", [])

        # Create filters for the parent path dimensions
        path_filters = []
        for i, value in enumerate(parent_path):
            path_filters.append({
                "field": dimension_hierarchy[i],
                "op": "=",
                "value": value
            })

        all_filters = base_spec.get("filters", []) + path_filters

        # Ensure a stable sort order for pagination
        sort = base_spec.get("sort", [])
        if not sort:
            sort = [{"field": dimension, "order": "asc"}]

        return {
            "table": base_spec["table"],
            "rows": [dimension],
            "full_rows": base_spec.get("rows", []),
            "columns": base_spec.get("columns", []),
            "measures": base_spec.get("measures", []),
            "filters": all_filters,
            "sort": sort,
            "limit": base_spec.get("limit", 1000),  # Increase limit for batch loading
            "pivot_config": base_spec.get("pivot_config"),
            "grouping_config": base_spec.get("grouping_config"),
            "totals": base_spec.get("totals", False),
        }

