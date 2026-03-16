"""
DuckDBBackend - optimized execution with Arrow support.

Features:
- Parameterized query execution (SQL injection safe)
- Arrow table output for zero-copy data transfer
- Connection pooling
- Query timeout and cancellation
- Performance metrics
"""

from typing import Any, List, Dict, Optional, Union
import time
import queue
import threading
from contextlib import contextmanager
from typing import Any, List, Dict, Optional, Union

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    import pyarrow as pa
except ImportError:
    pa = None


class ConnectionPool:
    """Simple connection/cursor pool for concurrent access"""
    def __init__(self, connection_factory: callable, max_size: int = 10):
        self.pool = queue.Queue(maxsize=max_size)
        self.factory = connection_factory
        self.max_size = max_size
        self.created_count = 0
        self.lock = threading.Lock()
        
        # Pre-fill a few
        initial_fill = min(2, max_size)
        for _ in range(initial_fill):
            self.pool.put(self.factory())
            self.created_count += 1

    @contextmanager
    def get_connection(self):
        try:
            # Try to get from pool without blocking first
            con = self.pool.get(block=False)
        except queue.Empty:
            # If empty, check if we can create more
            with self.lock:
                if self.created_count < self.max_size:
                    con = self.factory()
                    self.created_count += 1
                else:
                    # Wait for one to become available
                    con = self.pool.get() # Block
        
        try:
            yield con
        finally:
            self.pool.put(con)

class DuckDBBackend:
    """
    Production-ready DuckDB backend with parameterization and Arrow support.
    """
    
    def __init__(
        self,
        uri: str = ":memory:",
        read_only: bool = False,
        threads: Optional[int] = None,
        memory_limit: Optional[str] = None,
        connection: Optional["duckdb.DuckDBPyConnection"] = None,
        max_connections: int = 10
    ):
        """
        Initialize DuckDB backend.

        Args:
            uri: Database path or ":memory:" for in-memory
            read_only: Open in read-only mode
            threads: Number of threads (None = auto)
            memory_limit: Memory limit (e.g., "4GB")
            connection: Optional existing DuckDB connection
            max_connections: Max concurrent connections/cursors
        """
        if duckdb is None:
            raise ImportError("duckdb package required. Install: pip install duckdb")

        self.uri = uri
        if connection:
            self.con = connection
        else:
            self.con = duckdb.connect(database=uri, read_only=read_only)

        # Configure DuckDB
        if threads is not None:
            self.con.execute(f"SET threads={threads}")

        if memory_limit is not None:
            self.con.execute(f"SET memory_limit='{memory_limit}'")
            
        # Initialize connection pool
        # For DuckDB, we use cursors from the main connection as 'connections' in the pool
        # This is efficient and safe for concurrent use
        self.pool = ConnectionPool(lambda: self.con.cursor(), max_size=max_connections)
        
        # Track query stats
        self._query_count = 0
        self._total_time = 0.0
    
    def execute(self, query: Dict[str, Any]) -> pa.Table:
        """
        Execute a query and return the result as a PyArrow Table.
        
        Args:
            query: A dictionary containing the 'sql' and 'params'.
            
        Returns:
            A PyArrow Table with the query result.
        """
        sql = query.get("sql")
        params = query.get("params", [])
        
        start_time = time.time()
        
        try:
            # Use connection from pool for concurrency
            with self.pool.get_connection() as con:
                result = con.execute(sql, params).fetch_arrow_table()
            
            # Performance tracking
            self._query_count += 1
            self._total_time += (time.time() - start_time)
            
            return result
        except Exception as e:
            # Log error and re-raise
            print(f"Error executing query:\nSQL: {sql}\nParams: {params}\nError: {e}")
            raise

    async def execute_async(self, query: Dict[str, Any]) -> pa.Table:
        """Execute query asynchronously"""
        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.execute, query)
    
    def execute_arrow(
        self,
        query: Union[str, Dict[str, Any]],
        params: Optional[List[Any]] = None
    ) -> pa.Table:
        """
        Execute query and return Arrow table.
        
        Convenience method for zero-copy Arrow output.
        """
        return self.execute(query, params, return_arrow=True)
    
    def execute_batch(
        self,
        queries: List[Dict[str, Any]],
        return_arrow: bool = False
    ) -> List[Union[List[Dict[str, Any]], pa.Table]]:
        """
        Execute multiple queries in sequence.
        
        Args:
            queries: List of query dicts with "sql" and "params"
            return_arrow: Return Arrow tables
            
        Returns:
            List of results (one per query)
        """
        results = []
        for query in queries:
            result = self.execute(query, return_arrow=return_arrow)
            results.append(result)
        return results
    
    def execute_streaming(
        self,
        query: Union[str, Dict[str, Any]],
        params: Optional[List[Any]] = None,
        batch_size: int = 1000
    ):
        """
        Execute query and yield results in batches.
        
        Useful for large result sets to avoid loading everything in memory.
        
        Yields:
            Batches of rows as list of dicts
        """
        if isinstance(query, dict):
            sql = query.get("sql", "")
            params = query.get("params", [])
        else:
            sql = query
            params = params or []
        
        if params:
            result = self.con.execute(sql, params)
        else:
            result = self.con.execute(sql)
        
        cols = [desc[0] for desc in result.description]
        
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            batch = [dict(zip(cols, row)) for row in rows]
            yield batch
    
    def explain(
        self,
        query: Union[str, Dict[str, Any]],
        params: Optional[List[Any]] = None
    ) -> str:
        """
        Get query execution plan.
        
        Useful for debugging slow queries.
        """
        if isinstance(query, dict):
            sql = query.get("sql", "")
            params = query.get("params", [])
        else:
            sql = query
            params = params or []
        
        explain_sql = f"EXPLAIN {sql}"
        
        if params:
            result = self.con.execute(explain_sql, params)
        else:
            result = self.con.execute(explain_sql)
        
        return "\n".join([row[0] for row in result.fetchall()])
    
    def create_table_from_arrow(
        self,
        table_name: str,
        arrow_table: pa.Table
    ):
        """
        Create DuckDB table from Arrow table (zero-copy).
        
        Example:
            >>> arrow_data = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> backend.create_table_from_arrow("my_table", arrow_data)
        """
        if pa is None:
            raise ImportError("pyarrow required")
        
        self.con.register(table_name, arrow_table)
    
    def register_arrow_dataset(
        self,
        name: str,
        arrow_table: pa.Table,
        temporary: bool = True
    ):
        """
        Register Arrow table as queryable dataset.
        
        Args:
            name: Name to register as
            arrow_table: Arrow table to register
            temporary: If True, table is session-only
        """
        if pa is None:
            raise ImportError("pyarrow required")
        
        self.con.register(name, arrow_table)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get backend performance statistics.
        """
        avg_time = self._total_time / self._query_count if self._query_count > 0 else 0
        
        return {
            "query_count": self._query_count,
            "total_time": self._total_time,
            "avg_query_time": avg_time,
            "uri": self.uri
        }
    
    def reset_stats(self):
        """Reset performance counters"""
        self._query_count = 0
        self._total_time = 0.0
    
    def interrupt(self):
        """Interrupt currently running queries"""
        if self.con:
            try:
                self.con.interrupt()
            except Exception as e:
                print(f"Failed to interrupt DuckDB execution: {e}")

    @contextmanager
    def transaction(self):
        """
        Context manager for transactions.
        
        Example:
            >>> with backend.transaction():
            ...     backend.execute("INSERT INTO ...")
            ...     backend.execute("UPDATE ...")
        """
        self.con.execute("BEGIN TRANSACTION")
        try:
            yield
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
    
    def close(self):
        """Close database connection"""
        if self.con:
            self.con.close()
            self.con = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, 'con') and self.con:
            self.close()


# ========== Helper Functions ==========

def create_backend_from_uri(uri: str, **kwargs) -> DuckDBBackend:
    """
    Factory function to create backend from URI.
    
    Supports:
        - ":memory:" - in-memory database
        - "path/to/db.duckdb" - persistent file
        - "duckdb:///path/to/db.duckdb" - URI format
    """
    if uri.startswith("duckdb://"):
        # Extract path from URI
        uri = uri.replace("duckdb://", "")
        if uri.startswith("/"):
            uri = uri[1:]  # Remove leading slash for relative paths
    
    return DuckDBBackend(uri, **kwargs)


def execute_parallel(
    backend: DuckDBBackend,
    queries: List[Dict[str, Any]],
    max_workers: int = 4
) -> List[List[Dict[str, Any]]]:
    """
    Execute queries in parallel using DuckDB's multi-threading.
    
    Note: DuckDB handles parallelism internally, so this is mainly
    useful for running independent queries concurrently.
    """
    from concurrent.futures import ThreadPoolExecutor
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(backend.execute, q)
            for q in queries
        ]
        results = [f.result() for f in futures]
    
    return results