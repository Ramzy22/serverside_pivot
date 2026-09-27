"""
IbisBackend - database-agnostic backend for any Ibis-supported database.

Features:
- Ibis query execution (SQL injection safe)
- Arrow table output for zero-copy data transfer
- Connection pooling
- Query timeout and cancellation
- Performance metrics
- Database-agnostic operations
"""

from typing import Any, List, Dict, Optional, Union
import asyncio
import itertools
import threading
import time
from contextlib import contextmanager

try:
    import ibis
except ImportError:
    ibis = None

try:
    import pyarrow as pa
except ImportError:
    pa = None

from pivot_engine.util.sql_builder import quote_ident
from pivot_engine.backends.duckdb_backend import ConnectionPool, _fetch_arrow_table
from pivot_engine.backends.execution import (
    CancellationUnavailableError,
    DatabaseExecution,
    DatabaseExecutionError,
    QueryCancelledError,
    QueryTimeoutError,
    resolve_query_timeout_seconds,
)


_CANCELLATION_CLEANUP_TIMEOUT_SECONDS = 5.0


class IbisBackend:
    """
    Database-agnostic backend that works with any Ibis-supported database.
    """

    def __init__(
        self,
        connection: Optional[Any] = None,
        connection_uri: Optional[str] = None,
        query_timeout_seconds: Optional[float] = None,
        **connection_kwargs
    ):
        """
        Initialize Ibis backend.

        Args:
            connection: An existing Ibis connection
            connection_uri: URI string for connecting to the database
            **connection_kwargs: Additional connection parameters
        """
        if ibis is None:
            raise ImportError("ibis package required. Install: pip install ibis-framework")
        
        self.con = connection
        
        # Support for different database backends based on URI
        if connection_uri:
            if connection_uri.startswith("postgres://"):
                from urllib.parse import urlparse
                parsed = urlparse(connection_uri)
                self.con = ibis.postgres.connect(
                    host=parsed.hostname,
                    port=parsed.port,
                    user=parsed.username,
                    password=parsed.password,
                    database=parsed.path[1:]  # Remove leading slash
                )
            elif connection_uri.startswith("mysql://"):
                from urllib.parse import urlparse
                parsed = urlparse(connection_uri)
                self.con = ibis.mysql.connect(
                    host=parsed.hostname,
                    port=parsed.port or 3306,
                    user=parsed.username,
                    password=parsed.password,
                    database=parsed.path[1:]
                )
            elif connection_uri.startswith("bigquery://"):
                self.con = ibis.bigquery.connect(**connection_kwargs)
            elif connection_uri.startswith("snowflake://"):
                from urllib.parse import urlparse
                parsed = urlparse(connection_uri)
                self.con = ibis.snowflake.connect(
                    user=parsed.username,
                    password=parsed.password,
                    account=parsed.hostname,
                    **connection_kwargs
                )
            elif connection_uri.startswith("clickhouse://"):
                from urllib.parse import urlparse
                parsed = urlparse(connection_uri)
                self.con = ibis.clickhouse.connect(
                    host=parsed.hostname,
                    port=parsed.port or 8123,
                    user=parsed.username,
                    password=parsed.password,
                    database=parsed.path[1:] if parsed.path else 'default',
                    **connection_kwargs
                )
            elif connection_uri.startswith("sqlite://"):
                db_path = connection_uri.replace("sqlite://", "")
                self.con = ibis.sqlite.connect(db_path)
            elif connection_uri.startswith("duckdb://") or connection_uri == ":memory:":
                self.con = ibis.duckdb.connect(connection_uri.replace("duckdb://", "") if connection_uri.startswith("duckdb://") else connection_uri)
            else:
                # Default to DuckDB
                self.con = ibis.duckdb.connect(connection_uri)

        # Track query stats — lock guards counters updated from executor threads.
        self._query_count = 0
        self._total_time = 0.0
        self._stats_lock = threading.Lock()
        self._query_id_counter = itertools.count(1)
        self._running_queries_lock = threading.RLock()
        self._running_queries: Dict[str, Any] = {}
        self._query_timeout_seconds = resolve_query_timeout_seconds(
            query_timeout_seconds
        )
        self._duckdb_pool: Optional[ConnectionPool] = None
        self._execution_metrics = {
            "cancellation_attempts": 0,
            "cancellation_successes": 0,
            "cancellation_failures": 0,
            "timeouts": 0,
            "healthy_checkins": 0,
            "discarded_leases": 0,
        }
        if self._uses_duckdb_cursor_leases():
            self._duckdb_pool = ConnectionPool(
                lambda: self.con.con.cursor(),
                max_size=10,
            )
        if self._query_timeout_seconds is not None:
            if self._duckdb_pool is None:
                raise CancellationUnavailableError()

    def _backend_name(self) -> str:
        return str(getattr(self.con, "name", "unknown") or "unknown").lower()

    def _uses_duckdb_cursor_leases(self) -> bool:
        raw_connection = getattr(self.con, "con", None)
        return self._backend_name() == "duckdb" and callable(
            getattr(raw_connection, "cursor", None)
        )

    def cancellation_status(self) -> Dict[str, Any]:
        """Describe whether this backend can stop an exact active statement."""
        available = self._duckdb_pool is not None
        return {
            "available": available,
            "backend": self._backend_name(),
            "code": None if available else "cancellation_unavailable",
        }

    def execution_metrics(self) -> Dict[str, int]:
        """Return statement execution counters without exposing driver errors."""
        with self._running_queries_lock:
            return {
                "running": sum(
                    isinstance(item, DatabaseExecution)
                    for item in self._running_queries.values()
                ),
                **self._execution_metrics,
            }

    def execute(self, query: Union[Dict[str, Any], str], params: Optional[List[Any]] = None, return_arrow: bool = True) -> Union[pa.Table, List[Dict[str, Any]]]:
        """
        Execute a query and return the result.

        Args:
            query: A dictionary containing the 'ibis_expr' or 'sql', or a raw SQL string.
            params: Optional list of parameters (mostly for raw SQL).
            return_arrow: If True, returns PyArrow Table. If False, returns list of dicts.

        Returns:
            A PyArrow Table or list of dicts.
        """
        start_time = time.time()

        try:
            result = None
            # Handle dictionary query wrapper
            if isinstance(query, dict):
                if 'ibis_expr' in query:
                    ibis_expr = query['ibis_expr']
                    # Prefer to_pyarrow() for zero-copy efficiency
                    if return_arrow and hasattr(ibis_expr, 'to_pyarrow'):
                        result = ibis_expr.to_pyarrow()
                    else:
                        result = ibis_expr.execute()
                elif 'sql' in query:
                    sql = query['sql']
                    result = self.con.raw_sql(sql)
                else:
                    raise ValueError("Query dict must contain either 'ibis_expr' or 'sql'")
            # Handle raw SQL string
            elif isinstance(query, str):
                result = self.con.raw_sql(query)
            else:
                 raise ValueError(f"Invalid query type: {type(query)}")

            # Convert to Arrow table if requested and not already
            if return_arrow and not isinstance(result, pa.Table):
                if hasattr(result, 'to_arrow_table'):
                    result = result.to_arrow_table()
                elif hasattr(result, 'to_arrow'):
                    result = result.to_arrow()
                elif pa is not None:
                    import pandas as pd
                    if isinstance(result, pd.DataFrame):
                        result = pa.Table.from_pandas(result)
                    else:
                        # Fallback for other types
                        try:
                            df = pd.DataFrame(result)
                            result = pa.Table.from_pandas(df)
                        except Exception:  # nosec B110
                            pass
            elif not return_arrow and isinstance(result, pa.Table):
                 result = result.to_pylist()

            # Performance tracking — guarded because execute() runs in worker threads
            # via run_in_executor and multiple threads can race on these counters.
            elapsed = time.time() - start_time
            with self._stats_lock:
                self._query_count += 1
                self._total_time += elapsed

            return result
        except Exception as e:
            # Log error and re-raise
            print(f"Error executing query:\nQuery: {query}\nError: {e}")
            raise

    def _record_lease_disposition(
        self,
        execution: DatabaseExecution,
        retained: bool,
    ) -> None:
        with self._running_queries_lock:
            key = "healthy_checkins" if retained else "discarded_leases"
            self._execution_metrics[key] += 1
            if execution.cancellation_requested:
                outcome_key = (
                    "cancellation_successes"
                    if execution.cancellation_succeeded
                    else "cancellation_failures"
                )
                self._execution_metrics[outcome_key] += 1

    def _execute_duckdb_ibis_with_lease(
        self,
        query: Union[Dict[str, Any], str],
        params: Optional[List[Any]],
        return_arrow: bool,
        execution: DatabaseExecution,
    ) -> Any:
        """Compile and execute Ibis DuckDB work on one cancellable cursor."""
        assert self._duckdb_pool is not None
        lease = None
        discard = False
        started_at = time.time()
        try:
            ibis_expr = query.get("ibis_expr") if isinstance(query, dict) else None
            if ibis_expr is not None:
                self.con._run_pre_execute_hooks(ibis_expr)
                expression = ibis_expr.as_table()
                sql = self.con.compile(expression)
            elif isinstance(query, dict) and "sql" in query:
                sql = query["sql"]
            elif isinstance(query, str):
                sql = query
            else:
                raise ValueError("Query must contain an ibis_expr or SQL string.")

            lease = self._duckdb_pool.checkout()
            execution.attach_lease(lease)
            if execution.cancellation_requested:
                raise QueryCancelledError()
            if params:
                raw_table = _fetch_arrow_table(lease.execute(sql, params))
            else:
                raw_table = _fetch_arrow_table(lease.execute(sql))
            if execution.cancellation_requested:
                raise QueryCancelledError()

            if ibis_expr is not None:
                from ibis.backends.duckdb.converter import DuckDBPyArrowData

                result = ibis_expr.__pyarrow_result__(
                    raw_table,
                    data_mapper=DuckDBPyArrowData,
                )
            else:
                result = raw_table
            if not return_arrow and isinstance(result, pa.Table):
                result = result.to_pylist()
            with self._stats_lock:
                self._query_count += 1
                self._total_time += time.time() - started_at
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
                    retained = self._duckdb_pool.checkin(lease, discard=discard)
                    self._record_lease_disposition(execution, retained)
            finally:
                execution.finish()

    async def _await_execution_cleanup(self, worker: Any) -> None:
        try:
            await asyncio.wait_for(
                asyncio.shield(worker),
                timeout=_CANCELLATION_CLEANUP_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            raise DatabaseExecutionError("database cleanup acknowledgement timed out") from exc
        except DatabaseExecutionError:
            pass

    async def _execute_async_without_cancellation(
        self,
        query: Union[Dict[str, Any], str],
        params: Optional[List[Any]],
        return_arrow: bool,
    ) -> Any:
        """Preserve normal async Ibis execution when no hard deadline is requested."""
        loop = asyncio.get_running_loop()
        task = loop.run_in_executor(None, self.execute, query, params, return_arrow)
        query_id = f"ibis-query-{next(self._query_id_counter)}"
        with self._running_queries_lock:
            self._running_queries[query_id] = task
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            # The underlying executor future cannot stop the physical query.
            # Do not let an outer asyncio timeout claim database cancellation.
            raise CancellationUnavailableError() from exc
        finally:
            with self._running_queries_lock:
                self._running_queries.pop(query_id, None)

    async def execute_async(
        self,
        query: Union[Dict[str, Any], str],
        params: Optional[List[Any]] = None,
        return_arrow: bool = True,
        *,
        timeout_seconds: Optional[float] = None,
    ) -> Any:
        """Execute with native DuckDB cancellation when an exact lease is available."""
        requested_timeout = (
            resolve_query_timeout_seconds(timeout_seconds)
            if timeout_seconds is not None
            else getattr(self, "_query_timeout_seconds", None)
        )
        if getattr(self, "_duckdb_pool", None) is None:
            if requested_timeout is not None:
                raise CancellationUnavailableError()
            return await self._execute_async_without_cancellation(
                query,
                params,
                return_arrow,
            )

        execution = DatabaseExecution(f"ibis-query-{next(self._query_id_counter)}")
        with self._running_queries_lock:
            self._running_queries[execution.query_id] = execution
        worker = asyncio.get_running_loop().run_in_executor(
            None,
            self._execute_duckdb_ibis_with_lease,
            query,
            params,
            return_arrow,
            execution,
        )
        worker.add_done_callback(
            lambda _worker: self._remove_execution(execution.query_id)
        )
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
            if execution.request_cancel("client_cancelled"):
                with self._running_queries_lock:
                    self._execution_metrics["cancellation_attempts"] += 1
            await self._await_execution_cleanup(worker)
            raise
        finally:
            if worker.done():
                with self._running_queries_lock:
                    self._running_queries.pop(execution.query_id, None)

    async def cancel_query(self, query_id: Union[int, str]) -> bool:
        """Cancel an owned DuckDB statement and wait for its cleanup acknowledgement."""
        normalized_query_id = str(query_id)
        with self._running_queries_lock:
            item = self._running_queries.get(normalized_query_id)
        if not isinstance(item, DatabaseExecution):
            return False
        requested = item.request_cancel("client_cancelled")
        if requested:
            with self._running_queries_lock:
                self._execution_metrics["cancellation_attempts"] += 1
            completed = await asyncio.to_thread(
                item.wait_for_completion,
                _CANCELLATION_CLEANUP_TIMEOUT_SECONDS,
            )
            if not completed:
                raise DatabaseExecutionError("database cleanup acknowledgement timed out")
            with self._running_queries_lock:
                self._running_queries.pop(normalized_query_id, None)
        return requested

    def _remove_execution(self, query_id: str) -> None:
        with self._running_queries_lock:
            self._running_queries.pop(query_id, None)

    def execute_arrow(
        self,
        query: Union[str, Dict[str, Any]],
        params: Optional[List[Any]] = None
    ) -> pa.Table:
        """
        Execute query and return Arrow table.
        """
        return self.execute(query, params, return_arrow=True)

    def execute_batch(
        self,
        queries: List[Dict[str, Any]],
        return_arrow: bool = False
    ) -> List[Union[List[Dict[str, Any]], pa.Table]]:
        """
        Execute multiple queries in sequence.
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
        batch_size: int = 10000
    ):
        """
        Execute query and yield results in batches using efficient streaming if possible.

        Yields:
            Batches of rows as list of dicts (or Arrow RecordBatches if we enhanced the signature)
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
        """Yield native Arrow batches without a full-table execute fallback."""
        if pa is None:
            raise ImportError("pyarrow is required for bounded export batches")
        if max_batch_bytes <= 0 or max_batch_rows <= 0:
            raise ValueError("record-batch limits must be positive")
        from pivot_engine.export_executor import iter_bounded_record_batches

        if isinstance(query, dict) and "ibis_expr" in query:
            expression = query["ibis_expr"]
            to_batches = getattr(expression, "to_pyarrow_batches", None)
            if callable(to_batches):
                for raw_batch in to_batches(limit=None, chunk_size=int(max_batch_rows)):
                    yield from iter_bounded_record_batches(
                        raw_batch,
                        max_batch_bytes=max_batch_bytes,
                        max_batch_rows=max_batch_rows,
                    )
                return

        # A DuckDB Ibis connection can expose the same cursor-level reader for
        # raw SQL. Other Ibis backends must provide their own native batch API;
        # execute()->Table is deliberately not used here.
        if self._duckdb_pool is None:
            from pivot_engine.resource_governor import ResourceExhaustedError, ResourceKind

            raise ResourceExhaustedError(
                ResourceKind.EXPORT,
                reason="bounded_streaming_unavailable",
            )
        if isinstance(query, dict):
            sql = query.get("sql")
            params = query.get("params", [])
        else:
            sql = query
            params = params or []
        if not sql:
            raise ValueError("A SQL query is required for bounded record batches")

        with self._duckdb_pool.get_connection() as lease:
            result = lease.execute(sql, params) if params else lease.execute(sql)
            reader_factory = getattr(result, "fetch_record_batch", None)
            if callable(reader_factory):
                reader = reader_factory(int(max_batch_rows))
                for raw_batch in reader:
                    yield from iter_bounded_record_batches(
                        raw_batch,
                        max_batch_bytes=max_batch_bytes,
                        max_batch_rows=max_batch_rows,
                    )
                return
            description = getattr(result, "description", None) or []
            columns = [item[0] for item in description]
            while True:
                rows = result.fetchmany(int(max_batch_rows))
                if not rows:
                    return
                batch = pa.RecordBatch.from_pylist([dict(zip(columns, row)) for row in rows])
                yield from iter_bounded_record_batches(
                    batch,
                    max_batch_bytes=max_batch_bytes,
                    max_batch_rows=max_batch_rows,
                )

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
        if ibis is None:
            raise ImportError("ibis required")

        # In Ibis, register the table as a temporary table
        if temporary:
            self.con.create_table(name, arrow_table, temp=True)
        else:
            self.con.create_table(name, arrow_table)

    def create_index(self, table_name: str, columns: List[str], index_name: Optional[str] = None):
        """
        Create a database index on the specified table and columns.
        This provides a performance boost for large datasets.
        """
        if not index_name:
            # Generate a safe index name
            import hashlib
            cols_str = "_".join(columns)
            hash_suffix = hashlib.sha256(cols_str.encode()).hexdigest()[:8]
            index_name = f"idx_{table_name}_{hash_suffix}"[:63]

        try:
            # Construct SQL for index creation
            # Most SQL dialects support "CREATE INDEX [IF NOT EXISTS] name ON table (cols)"
            # We use a generic approach and handle exceptions
            
            safe_table = quote_ident(table_name)
            safe_index_name = quote_ident(index_name)
            safe_cols = ", ".join(quote_ident(column) for column in columns)

            sql = f"CREATE INDEX IF NOT EXISTS {safe_index_name} ON {safe_table} ({safe_cols})"
            
            # Execute raw SQL
            # Note: Ibis backends might vary in how they expose raw execution
            if hasattr(self.con, 'raw_sql'):
                self.con.raw_sql(sql)
            elif hasattr(self.con, 'execute'):
                 self.con.execute(sql)
            else:
                print(f"Warning: Backend {type(self.con)} does not support raw SQL for indexing.")
                
        except Exception as e:
            # Log but don't fail the operation - indexing is an optimization
            print(f"Warning: Failed to create index {index_name} on {table_name}: {e}")

    def get_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get schema information for a table.
        Returns a dictionary of column names to types.
        """
        if self.con is None:
            raise ValueError("Backend not connected")
        
        try:
            table = self.con.table(table_name)
            schema = table.schema()
            return {name: str(dtype) for name, dtype in schema.items()}
        except Exception as e:
            print(f"Error getting schema for {table_name}: {e}")
            return {}

    def get_stats(self) -> Dict[str, Any]:
        """
        Get backend performance statistics.
        """
        avg_time = self._total_time / self._query_count if self._query_count > 0 else 0

        return {
            "query_count": self._query_count,
            "total_time": self._total_time,
            "avg_query_time": avg_time,
            "backend_type": getattr(self.con, 'name', 'unknown') if self.con else 'disconnected'
        }

    def reset_stats(self):
        """Reset performance counters"""
        with self._stats_lock:
            self._query_count = 0
            self._total_time = 0.0

    def _execute_transaction_sql(self, sql: str):
        """Execute transaction control SQL or fail before pretending it worked."""
        if self.con is None:
            raise ValueError("Backend not connected")

        raw_sql = getattr(self.con, "raw_sql", None)
        if callable(raw_sql):
            return raw_sql(sql)

        execute = getattr(self.con, "execute", None)
        if callable(execute):
            return execute(sql)

        raise NotImplementedError(
            f"Transactions are not supported by {type(self.con).__name__}: "
            "the connection exposes neither raw_sql() nor execute()."
        )

    @contextmanager
    def transaction(self):
        """
        Context manager for transactions.

        Backends that cannot execute transaction-control SQL must fail explicitly
        instead of silently yielding without BEGIN/COMMIT/ROLLBACK protection.
        """
        self._execute_transaction_sql("BEGIN TRANSACTION")
        try:
            yield
            self._execute_transaction_sql("COMMIT")
        except Exception:
            self._execute_transaction_sql("ROLLBACK")
            raise

    def close(self):
        """Close database connection"""
        if not hasattr(self, "con"):
            return
        if hasattr(self.con, 'close'):
            self.con.close()
        self.con = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, 'close'):
            self.close()
