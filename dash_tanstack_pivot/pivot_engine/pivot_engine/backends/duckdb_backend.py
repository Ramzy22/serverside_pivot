"""
DuckDBBackend - optimized execution with Arrow support.

Features:
- Parameterized query execution (SQL injection safe)
- Arrow table output for zero-copy data transfer
- Connection pooling
- Query timeout and cancellation
- Performance metrics
"""

import asyncio
import time
import queue
import re
import threading
import itertools
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

from pivot_engine.backends.execution import (
    DatabaseExecution,
    DatabaseExecutionError,
    QueryCancelledError,
    QueryTimeoutError,
    resolve_query_timeout_seconds,
)


_MAX_DUCKDB_THREADS = 1024
_CANCELLATION_CLEANUP_TIMEOUT_SECONDS = 5.0
_DUCKDB_MEMORY_LIMIT_RE = re.compile(
    r"^(?P<value>(?:0|[1-9][0-9]*)(?:\.[0-9]+)?)\s*(?P<unit>B|KB|MB|GB|TB|KIB|MIB|GIB|TIB)$",
    re.IGNORECASE,
)
_DUCKDB_MEMORY_UNIT_CANONICAL = {
    "B": "B",
    "KB": "KB",
    "MB": "MB",
    "GB": "GB",
    "TB": "TB",
    "KIB": "KiB",
    "MIB": "MiB",
    "GIB": "GiB",
    "TIB": "TiB",
}


def _fetch_arrow_table(result: Any) -> "pa.Table":
    """Return an Arrow table across supported DuckDB connection APIs.

    DuckDB 0.x connections expose ``fetch_arrow_table`` after ``execute``;
    newer releases also expose ``to_arrow_table``. Prefer the newer spelling
    while retaining the documented legacy fetch API throughout our supported
    DuckDB 0.8--1.x range.
    """
    to_arrow_table = getattr(result, "to_arrow_table", None)
    if callable(to_arrow_table):
        return to_arrow_table()
    fetch_arrow_table = getattr(result, "fetch_arrow_table", None)
    if callable(fetch_arrow_table):
        return fetch_arrow_table()
    raise TypeError("DuckDB query result does not expose an Arrow table conversion API.")


def _validate_duckdb_threads(threads: Any) -> int:
    if isinstance(threads, bool):
        raise ValueError("DuckDB threads must be a positive integer.")
    try:
        normalized_threads = int(threads)
    except (TypeError, ValueError):
        raise ValueError("DuckDB threads must be a positive integer.") from None
    if str(threads).strip() != str(normalized_threads) and not isinstance(threads, int):
        raise ValueError("DuckDB threads must be a positive integer.")
    if normalized_threads < 1 or normalized_threads > _MAX_DUCKDB_THREADS:
        raise ValueError(f"DuckDB threads must be between 1 and {_MAX_DUCKDB_THREADS}.")
    return normalized_threads


def _validate_duckdb_memory_limit(memory_limit: Any) -> str:
    if not isinstance(memory_limit, str):
        raise ValueError("DuckDB memory_limit must be a size string like '4GB'.")
    match = _DUCKDB_MEMORY_LIMIT_RE.fullmatch(memory_limit.strip())
    if not match:
        raise ValueError("DuckDB memory_limit must be a positive size literal such as '512MB' or '4GB'.")
    value = match.group("value")
    try:
        if float(value) <= 0:
            raise ValueError
    except ValueError:
        raise ValueError("DuckDB memory_limit must be greater than zero.") from None
    unit = _DUCKDB_MEMORY_UNIT_CANONICAL[match.group("unit").upper()]
    return f"{value}{unit}"


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

    @staticmethod
    def _is_healthy(con: Any) -> bool:
        try:
            con.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def _new_connection(self):
        con = self.factory()
        self.created_count += 1
        return con

    def _discard_connection(self, con: Any) -> None:
        close = getattr(con, "close", None)
        if callable(close):
            try:
                close()
            except Exception:  # nosec B110
                pass
        with self.lock:
            if self.created_count > 0:
                self.created_count -= 1

    def _borrow_connection(self):
        con = None
        while True:
            with self.lock:
                try:
                    con = self.pool.get(block=False)
                except queue.Empty:
                    if self.created_count < self.max_size:
                        return self._new_connection()
                    # Pool is empty and at max_size — fall through to blocking wait.

            if con is None:
                # Wait for a connection to be returned. Use a timeout so that if a
                # concurrent thread discards an unhealthy connection (decrementing
                # created_count without putting anything back in the queue), we
                # re-enter the lock, see created_count < max_size, and create a new
                # one — otherwise we'd block here forever.
                try:
                    con = self.pool.get(timeout=1.0)
                except queue.Empty:
                    con = None
                    continue  # Re-check created_count under the lock

            if self._is_healthy(con):
                return con

            self._discard_connection(con)
            con = None

    def checkout(self) -> Any:
        """Lease one cursor to a statement owner until it explicitly checks in."""
        return self._borrow_connection()

    def discard(self, con: Any) -> None:
        """Close a lease that a statement owner cannot prove safe to reuse."""
        self._discard_connection(con)

    def checkin(self, con: Any, *, discard: bool = False) -> bool:
        """Return a healthy lease or retire it before another statement can use it."""
        if discard or not self._is_healthy(con):
            self._discard_connection(con)
            return False
        self.pool.put(con)
        return True

    @contextmanager
    def get_connection(self):
        con = self.checkout()
        discard = False
        try:
            yield con
        except Exception:
            discard = True
            raise
        finally:
            self.checkin(con, discard=discard)

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
        max_connections: int = 10,
        query_timeout_seconds: Optional[float] = None,
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
        self._query_timeout_seconds = resolve_query_timeout_seconds(
            query_timeout_seconds
        )
        if connection:
            self.con = connection
        else:
            self.con = duckdb.connect(database=uri, read_only=read_only)

        # Configure DuckDB
        if threads is not None:
            safe_threads = _validate_duckdb_threads(threads)
            self.con.execute(f"SET threads={safe_threads}")

        if memory_limit is not None:
            safe_memory_limit = _validate_duckdb_memory_limit(memory_limit)
            self.con.execute(f"SET memory_limit='{safe_memory_limit}'")
            
        # Initialize connection pool
        # For DuckDB, we use cursors from the main connection as 'connections' in the pool
        # This is efficient and safe for concurrent use
        self.pool = ConnectionPool(lambda: self.con.cursor(), max_size=max_connections)
        
        # Track query stats
        self._query_count = 0
        self._total_time = 0.0
        self._query_id_counter = itertools.count(1)
        self._running_queries_lock = threading.RLock()
        self._running_queries: Dict[str, DatabaseExecution] = {}
        self._execution_metrics = {
            "cancellation_attempts": 0,
            "cancellation_successes": 0,
            "cancellation_failures": 0,
            "timeouts": 0,
            "healthy_checkins": 0,
            "discarded_leases": 0,
        }
    
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
                result = _fetch_arrow_table(con.execute(sql, params))
            
            # Performance tracking
            self._query_count += 1
            self._total_time += (time.time() - start_time)
            
            return result
        except Exception as e:
            # Log error and re-raise
            print(f"Error executing query:\nSQL: {sql}\nParams: {params}\nError: {e}")
            raise

    def _register_execution(self, execution: DatabaseExecution) -> None:
        with self._running_queries_lock:
            self._running_queries[execution.query_id] = execution

    def _unregister_execution(self, execution: DatabaseExecution) -> None:
        with self._running_queries_lock:
            self._running_queries.pop(execution.query_id, None)

    def _record_cancellation_request(self, execution: DatabaseExecution) -> bool:
        requested = execution.request_cancel("client_cancelled")
        if requested:
            with self._running_queries_lock:
                self._execution_metrics["cancellation_attempts"] += 1
        return requested

    def _record_lease_disposition(
        self,
        execution: DatabaseExecution,
        retained: bool,
    ) -> None:
        with self._running_queries_lock:
            if retained:
                self._execution_metrics["healthy_checkins"] += 1
            else:
                self._execution_metrics["discarded_leases"] += 1
            if execution.cancellation_requested:
                key = (
                    "cancellation_successes"
                    if execution.cancellation_succeeded
                    else "cancellation_failures"
                )
                self._execution_metrics[key] += 1

    def _execute_with_lease(
        self,
        query: Dict[str, Any],
        execution: DatabaseExecution,
    ) -> pa.Table:
        """Run one query on an owned cursor and retire the lease before return."""
        lease = None
        discard = False
        try:
            lease = self.pool.checkout()
            execution.attach_lease(lease)
            if execution.cancellation_requested:
                raise QueryCancelledError()

            sql = query.get("sql")
            params = query.get("params", [])
            result = _fetch_arrow_table(lease.execute(sql, params))
            if execution.cancellation_requested:
                raise QueryCancelledError()
            return result
        except BaseException as exc:
            if execution.cancellation_requested:
                discard = execution.cancel_error is not None
                if execution.reason == "query_timeout":
                    raise QueryTimeoutError() from exc
                raise QueryCancelledError() from exc
            if isinstance(exc, DatabaseExecutionError):
                raise
            raise DatabaseExecutionError(str(exc)) from exc
        finally:
            try:
                if lease is not None:
                    retained = self.pool.checkin(lease, discard=discard)
                    self._record_lease_disposition(execution, retained)
            finally:
                execution.finish()

    async def _await_execution_cleanup(self, worker: Any) -> None:
        """Wait for the worker that owns the leased cursor to finish cleanup."""
        try:
            await asyncio.wait_for(
                asyncio.shield(worker),
                timeout=_CANCELLATION_CLEANUP_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            raise DatabaseExecutionError("database cleanup acknowledgement timed out") from exc
        except DatabaseExecutionError:
            pass

    async def execute_async(
        self,
        query: Dict[str, Any],
        *,
        timeout_seconds: Optional[float] = None,
    ) -> pa.Table:
        """Execute one statement asynchronously with native cursor interruption."""
        requested_timeout = (
            resolve_query_timeout_seconds(timeout_seconds)
            if timeout_seconds is not None
            else self._query_timeout_seconds
        )
        loop = asyncio.get_running_loop()
        execution = DatabaseExecution(f"duckdb-query-{next(self._query_id_counter)}")
        self._register_execution(execution)
        worker = loop.run_in_executor(None, self._execute_with_lease, query, execution)
        worker.add_done_callback(lambda _worker: self._unregister_execution(execution))

        try:
            if requested_timeout is None:
                return await asyncio.shield(worker)

            done, _pending = await asyncio.wait({worker}, timeout=requested_timeout)
            if done:
                return worker.result()

            if execution.request_cancel("query_timeout"):
                with self._running_queries_lock:
                    self._execution_metrics["cancellation_attempts"] += 1
                    self._execution_metrics["timeouts"] += 1
            await self._await_execution_cleanup(worker)
            raise QueryTimeoutError()
        except asyncio.CancelledError:
            self._record_cancellation_request(execution)
            await self._await_execution_cleanup(worker)
            raise
        finally:
            if worker.done():
                self._unregister_execution(execution)

    async def cancel_query(self, query_id: Union[int, str]) -> bool:
        """Cancel a registered statement and wait for its lease cleanup."""
        with self._running_queries_lock:
            execution = self._running_queries.get(str(query_id))
        if execution is None:
            return False
        requested = self._record_cancellation_request(execution)
        if requested:
            completed = await asyncio.to_thread(
                execution.wait_for_completion,
                _CANCELLATION_CLEANUP_TIMEOUT_SECONDS,
            )
            if not completed:
                raise DatabaseExecutionError("database cleanup acknowledgement timed out")
            self._unregister_execution(execution)
        return requested

    def execution_metrics(self) -> Dict[str, int]:
        """Return statement-cancellation counters without driver internals."""
        with self._running_queries_lock:
            return {
                "running": len(self._running_queries),
                **self._execution_metrics,
            }
    
    def execute_arrow(
        self,
        query: Dict[str, Any],
    ) -> pa.Table:
        """
        Execute query and return Arrow table.
        
        Convenience method for zero-copy Arrow output.
        """
        return self.execute(query)
    
    def execute_batch(
        self,
        queries: List[Dict[str, Any]],
    ) -> List[pa.Table]:
        """
        Execute multiple queries in sequence.
        
        Args:
            queries: List of query dicts with "sql" and "params"
            
        Returns:
            List of Arrow table results (one per query)
        """
        results = []
        for query in queries:
            result = self.execute(query)
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
        for batch in self.execute_record_batches(
            query,
            params=params,
            max_batch_bytes=32 * 1024 * 1024,
            max_batch_rows=batch_size,
        ):
            yield batch.to_pylist()

    def execute_record_batches(
        self,
        query: Union[str, Dict[str, Any]],
        params: Optional[List[Any]] = None,
        *,
        max_batch_bytes: int = 32 * 1024 * 1024,
        max_batch_rows: int = 10_000,
    ):
        """Yield bounded Arrow record batches without materializing the table."""
        if pa is None:
            raise ImportError("pyarrow is required for bounded export batches")
        if max_batch_bytes <= 0 or max_batch_rows <= 0:
            raise ValueError("record-batch limits must be positive")
        from pivot_engine.export_executor import iter_bounded_record_batches

        if isinstance(query, dict):
            sql = query.get("sql", "")
            params = query.get("params", [])
        else:
            sql = query
            params = params or []

        with self.pool.get_connection() as con:
            result = con.execute(sql, params) if params else con.execute(sql)
            fetch_record_batch = getattr(result, "fetch_record_batch", None)
            if callable(fetch_record_batch):
                reader = fetch_record_batch(int(max_batch_rows))
                for raw_batch in reader:
                    yield from iter_bounded_record_batches(
                        raw_batch,
                        max_batch_bytes=max_batch_bytes,
                        max_batch_rows=max_batch_rows,
                    )
                return

            # Older DuckDB releases expose fetchmany but not Arrow readers.
            # fetchmany is still bounded by rows; convert only that batch and
            # enforce the byte ceiling before it reaches the writer.
            description = getattr(result, "description", None) or []
            columns = [item[0] for item in description]
            while True:
                rows = result.fetchmany(int(max_batch_rows))
                if not rows:
                    return
                batch = pa.RecordBatch.from_pylist(
                    [dict(zip(columns, row)) for row in rows]
                )
                yield from iter_bounded_record_batches(
                    batch,
                    max_batch_bytes=max_batch_bytes,
                    max_batch_rows=max_batch_rows,
                )
    
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
        """Request cancellation of active owned statements without a global interrupt."""
        with self._running_queries_lock:
            executions = list(self._running_queries.values())
        for execution in executions:
            self._record_cancellation_request(execution)

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
