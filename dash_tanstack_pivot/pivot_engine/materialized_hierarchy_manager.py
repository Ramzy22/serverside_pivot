"""
MaterializedHierarchyManager - Pre-compute and store hierarchical rollups for common drill paths
"""
import asyncio
import json
import os
import time
from typing import Dict, Any, List, Optional
import ibis
from ibis import BaseBackend as IbisBaseBackend
from pivot_engine.types.pivot_spec import PivotSpec


class MaterializedHierarchyManager:
    def __init__(self, backend: IbisBaseBackend, cache, registry_path: str = "materialized_registry.json"):
        self.backend = backend # Expects an Ibis connection
        self.cache = cache
        self.registry_path = registry_path
        self.rollup_tables = self._load_registry()
        self.jobs = {}  # job_id -> {status, progress, error, result}

    def _load_registry(self) -> Dict[str, str]:
        """Load rollup registry from disk."""
        if os.path.exists(self.registry_path):
            try:
                with open(self.registry_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Failed to load materialized registry: {e}")
        return {}

    def _save_registry(self):
        """Save rollup registry to disk."""
        try:
            with open(self.registry_path, 'w') as f:
                json.dump(self.rollup_tables, f)
        except Exception as e:
            print(f"Failed to save materialized registry: {e}")

    async def create_materialized_hierarchy_async(self, spec: PivotSpec) -> str:
        """
        Start an asynchronous job to create materialized hierarchy.
        Returns a job_id.
        """
        import uuid
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {"status": "pending", "progress": 0, "table": spec.table}
        
        # Run in a separate thread to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, self._create_materialized_hierarchy_sync, spec, job_id)
        
        return job_id

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a materialization job."""
        return self.jobs.get(job_id, {"status": "unknown"})

    def _create_materialized_hierarchy_sync(self, spec: PivotSpec, job_id: Optional[str] = None):
        """Internal synchronous worker to create materialized hierarchy."""
        try:
            if job_id:
                self.jobs[job_id]["status"] = "running"
            
            hierarchy_name = f"hierarchy_{spec.table}_{abs(hash(str(spec.to_dict()))):x}"
            base_table = self.backend.table(spec.table)
            
            total_levels = len(spec.rows)
            
            for level in range(1, total_levels + 1):
                level_dims = spec.rows[:level]
                rollup_table_name = f"{hierarchy_name}_level_{level}"

                # Define aggregations in Ibis
                aggregations = []
                for m in spec.measures:
                    agg_func = getattr(base_table[m.field], m.agg)
                    aggregations.append(agg_func().name(m.alias))

                # Build the Ibis expression for the rollup
                rollup_expr = base_table.group_by(level_dims).aggregate(aggregations)

                # Create the table in the database
                # Note: 'overwrite=True' might not work on all backends, but Ibis handles it generally
                self.backend.create_table(rollup_table_name, rollup_expr, overwrite=True)
                
                # OPTIMIZATION: Create Index on the grouping columns (dimensions)
                # This is crucial for performance with millions of rows
                # We try to access the raw connection or use a helper if available
                self._create_index_safely(rollup_table_name, level_dims)
                
                self.rollup_tables[f"{spec.table}:{level}"] = rollup_table_name
                
                if job_id:
                    self.jobs[job_id]["progress"] = int((level / total_levels) * 100)
            
            self._save_registry()

            if job_id:
                self.jobs[job_id]["status"] = "completed"
                self.jobs[job_id]["hierarchy_name"] = hierarchy_name
                
        except Exception as e:
            print(f"Materialization failed: {e}")
            if job_id:
                self.jobs[job_id]["status"] = "failed"
                self.jobs[job_id]["error"] = str(e)

    def cleanup_unused_rollups(self):
        """
        Cleanup all registered rollup tables.
        In a real implementation, this would use access timestamps or a TTL.
        For now, it clears everything to ensure clean state or manual maintenance.
        """
        for key, table_name in list(self.rollup_tables.items()):
            try:
                if table_name in self.backend.list_tables():
                    self.backend.drop_table(table_name)
                del self.rollup_tables[key]
            except Exception as e:
                print(f"Failed to drop rollup table {table_name}: {e}")
        self._save_registry()
                
    def _create_index_safely(self, table_name: str, columns: List[str]):
        """Helper to create indexes if the backend supports it"""
        # Check if the backend object passed to __init__ has the 'create_index' method
        # (which we added to IbisBackend wrapper, but self.backend here is the raw Ibis connection object usually)
        # Wait, in ScalablePivotController, we pass 'con' which is the raw Ibis connection.
        # But we added 'create_index' to 'IbisBackend' wrapper class.
        # We need to bridge this gap. 
        
        # Approach 1: Try to execute raw SQL on the connection object directly
        try:
            import hashlib
            cols_str = "_".join(columns)
            hash_suffix = hashlib.md5(cols_str.encode()).hexdigest()[:8]
            index_name = f"idx_{table_name}_{hash_suffix}"[:63]
            
            safe_table = table_name
            safe_cols = ", ".join([f'"{c}"' for c in columns])
            sql = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{safe_table}" ({safe_cols})'
            
            if hasattr(self.backend, 'raw_sql'):
                 self.backend.raw_sql(sql)
            elif hasattr(self.backend, 'execute'):
                 self.backend.execute(sql)
        except Exception as e:
            # Index creation is optional/optimization, so we log and continue
            print(f"Index creation skipped for {table_name}: {e}")

    def create_materialized_hierarchy(self, spec: PivotSpec):
        """Legacy synchronous method (keeps backward compatibility)."""
        self._create_materialized_hierarchy_sync(spec)
    
    def find_best_rollup(self, spec: PivotSpec) -> Optional[str]:
        """
        Find the best materialized rollup table for the given query.
        Looks for the most aggregated (smallest) rollup that satisfies:
        1. Contains all grouping dimensions.
        2. Contains all filter dimensions.
        """
        # 1. Identify all required dimensions (grouping + filtering)
        # Note: We assume rollup levels are strictly prefixes of spec.rows defined during materialization.
        # This only works if the query spec uses the same hierarchy definition as the materialization spec.
        # We assume the table name identifies the hierarchy context.
        
        required_dims = set(spec.rows) # Grouping
        if spec.columns:
             # Standard rollups don't include column pivots usually, unless specified
             # If columns are used, we can only use a rollup if it includes those columns as dimensions.
             # Current create_materialized_hierarchy only uses spec.rows.
             # So if spec.columns is not empty, we likely can't use these rollups.
             return None

        # Check filters
        for f in spec.filters:
            field = f.get('field')
            if field:
                required_dims.add(field)
        
        # We need to find a level L such that spec.rows[:L] contains all required_dims.
        # Since spec.rows is ordered, we find the max index of any required dimension in spec.rows.
        
        max_idx = -1
        for dim in required_dims:
            try:
                idx = spec.rows.index(dim)
                max_idx = max(max_idx, idx)
            except ValueError:
                # A required dimension (e.g. from filter) is NOT in the hierarchy.
                # We cannot use the rollup because the rollup only contains hierarchy columns.
                return None
        
        # The required level is max_idx + 1 (1-based)
        min_level = max_idx + 1
        
        # We prefer the smallest table, which is the most aggregated one.
        # In a hierarchy, Level 1 is most aggregated, Level N is least.
        # So we want the smallest Level L >= min_level.
        # Wait, Level 1 (Region) has fewer rows than Level 2 (Region, State).
        # We want the *lowest* level number that satisfies the requirement.
        # Because Level 1 < Level 2 in size.
        
        target_level = min_level
        if target_level < 1:
            target_level = 1
            
        # Check if this level or any deeper level exists
        # We iterate from target_level upwards (deeper) until we find one.
        # Actually, we want the *first* one we find because it's the most aggregated one that suffices.
        # But wait, does 'create_materialized_hierarchy' guarantee existence?
        # We check `self.rollup_tables`.
        
        # We don't know the max level, but practical limits apply (e.g. 10).
        for level in range(target_level, 20):
            table = self.rollup_tables.get(f"{spec.table}:{level}")
            if table:
                return table
                
        return None

    def get_rollup_table_name(self, spec: PivotSpec, level: int) -> Optional[str]:
        """Get the name of the rollup table for a given level."""
        return self.rollup_tables.get(f"{spec.table}:{level}")