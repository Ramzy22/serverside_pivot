"""
MaterializedHierarchyManager - Pre-compute and store hierarchical rollups for common drill paths
"""
import asyncio
import json
import os
import time
import threading
from typing import Dict, Any, List, Optional, Union
import ibis
from ibis import BaseBackend as IbisBaseBackend
from pivot_engine.types.pivot_spec import PivotSpec
from pivot_engine.util.sql_builder import quote_ident
from pivot_engine.backends.capabilities import resolve_backend_capabilities


class MaterializedHierarchyManager:
    def __init__(
        self,
        backend: IbisBaseBackend,
        cache,
        registry_path: str = "materialized_registry.json",
        lock: Optional[threading.Lock] = None,
        persist_registry: Optional[bool] = None,
    ):
        self.backend = backend # Expects an Ibis connection
        self.cache = cache
        self.registry_path = registry_path
        if persist_registry is None:
            persist_registry = os.environ.get("PIVOT_PERSIST_MATERIALIZED_REGISTRY", "").strip().lower() in {"1", "true", "yes"}
        self.persist_registry = bool(persist_registry)
        self.rollup_tables = self._load_registry()
        self.jobs = {}  # job_id -> {status, progress, error, result}
        self.lock = lock if lock is not None else threading.Lock()

    def _supports_ddl(self) -> bool:
        caps = resolve_backend_capabilities(self.backend)
        return bool(caps.supports_ddl_materialization)

    def _load_registry(self) -> Dict[str, str]:
        """Load rollup registry from disk when persistence is enabled."""
        if self.persist_registry and self.registry_path and os.path.exists(self.registry_path):
            try:
                with open(self.registry_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Failed to load materialized registry: {e}")
        return {}

    def _save_registry(self):
        """Save rollup registry to disk when persistence is enabled."""
        if not self.persist_registry or not self.registry_path:
            return
        try:
            with open(self.registry_path, 'w') as f:
                json.dump(self.rollup_tables, f)
        except Exception as e:
            print(f"Failed to save materialized registry: {e}")

    async def create_materialized_hierarchy_async(
        self,
        spec: PivotSpec,
        *,
        owner: Optional[str] = None,
    ) -> str:
        """
        Start an asynchronous job to create materialized hierarchy.
        Returns a job_id.
        """
        import uuid
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {
            "status": "pending",
            "progress": 0,
            "table": spec.table,
            "owner": str(owner) if owner else None,
        }

        # DuckDB connections are not safe for this background-thread pattern:
        # the next query can observe a closed pending result on the shared
        # connection. Run the job inline so follow-up hierarchy requests see a
        # clean connection state.
        backend_name = getattr(self.backend, "name", "").lower()
        if backend_name == "duckdb":
            self._create_materialized_hierarchy_sync(spec, job_id)
        else:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, self._create_materialized_hierarchy_sync, spec, job_id)
        
        return job_id

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a materialization job."""
        return self.jobs.get(job_id, {"status": "unknown"})

    def _get_spec_hash(self, spec: PivotSpec) -> str:
        """Generate a hash for the spec configuration relevant to materialization"""
        import hashlib
        import json
        import dataclasses
        
        # Handle Measure objects or dicts
        measures_list = []
        for m in spec.measures:
            if hasattr(m, 'to_dict'):
                measures_list.append(m.to_dict())
            elif dataclasses.is_dataclass(m):
                measures_list.append(dataclasses.asdict(m))
            elif isinstance(m, dict):
                measures_list.append(m)
            else:
                # Fallback
                measures_list.append(str(m))

        key = {
            'table': spec.table,
            'rows': spec.rows,
            'measures': measures_list,
            'filters': str(sorted(spec.filters, key=lambda x: str(x))) if spec.filters else []
        }
        return hashlib.sha256(json.dumps(key, sort_keys=True, default=str).encode()).hexdigest()

    def _create_materialized_hierarchy_sync(self, spec: PivotSpec, job_id: Optional[str] = None):
        """Internal synchronous worker to create materialized hierarchy."""
        with self.lock:
            try:
                if not self._supports_ddl():
                    if job_id:
                        self.jobs[job_id]["status"] = "skipped"
                    return
                if job_id:
                    self.jobs[job_id]["status"] = "running"
                
                spec_hash = self._get_spec_hash(spec)
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
                    self.backend.create_table(rollup_table_name, rollup_expr, overwrite=True)
                    
                    self._create_index_safely(rollup_table_name, level_dims)
                    
                    # Store by spec hash to avoid collisions
                    self.rollup_tables[f"{spec_hash}:{level}"] = rollup_table_name
                    
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

    def create_materialized_hierarchy(self, spec: PivotSpec):
        """Public synchronous method to create materialized hierarchy."""
        return self._create_materialized_hierarchy_sync(spec)

    def cleanup_unused_rollups(self):
        """
        Cleanup all registered rollup tables.
        """
        with self.lock:
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
        try:
            import hashlib
            cols_str = "_".join(columns)
            hash_suffix = hashlib.sha256(cols_str.encode()).hexdigest()[:8]
            index_name = f"idx_{table_name}_{hash_suffix}"[:63]
            
            safe_table = quote_ident(table_name)
            safe_index_name = quote_ident(index_name)
            safe_cols = ", ".join(quote_ident(column) for column in columns)
            sql = f"CREATE INDEX IF NOT EXISTS {safe_index_name} ON {safe_table} ({safe_cols})"
            
            if hasattr(self.backend, 'raw_sql'):
                 res = self.backend.raw_sql(sql)
                 if res and hasattr(res, 'fetchall'):
                     try:
                        res.fetchall()
                     except:  # nosec B110
                        pass
            elif hasattr(self.backend, 'execute'):
                 self.backend.execute(sql)
        except Exception as e:
            print(f"Index creation skipped for {table_name}: {e}")

    def find_best_rollup(self, spec: PivotSpec) -> Optional[str]:
        """
        Find the best materialized rollup table for the given query.
        """
        # Identifying rollups by hash makes this precise
        # If no exact match found, we don't guess (to avoid "column not found" errors)
        return self.get_rollup_table_name(spec, len(spec.rows))

    def get_rollup_table_name(self, spec: PivotSpec, level: int) -> Optional[str]:
        """Get the name of the rollup table for a given level, matching the spec's dimensions."""
        spec_hash = self._get_spec_hash(spec)
        return self.rollup_tables.get(f"{spec_hash}:{level}")

    
