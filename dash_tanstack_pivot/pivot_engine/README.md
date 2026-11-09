# 🚀 Scalable Pivot Engine

A high-performance, database-agnostic pivot engine designed to handle **millions of rows** with ease. Built on [Ibis](https://ibis-project.org/) and [Apache Arrow](https://arrow.apache.org/), it provides a production-grade backend for hierarchical data exploration, infinite scrolling, and real-time analytical updates.

---

## ✨ Key Capabilities

### 🏎️ Performance at Scale
- **Millions of Rows**: Optimized for large-scale datasets using vectorized operations and zero-copy data transfers via PyArrow.
- **Async Materialization**: Background pre-computation of hierarchical rollups to ensure sub-second UI responsiveness.
- **Memory-Efficient Export**: Stream gigabyte-sized CSV/Parquet files directly from the database to the user without server memory bloat.
- **Intelligent Caching**: Multi-level caching (Memory/Redis) with semantic query diffing.

### 🔐 Enterprise Security
- **API Key Auth**: Built-in authentication via `X-API-Key`.
- **Universal RLS**: Enforce Row-Level Security across all endpoints (Grid, Export, Hierarchical) using user attributes.
- **Safe Expressions**: AST-based parser for calculated measures, preventing code injection.

### 🌐 Backend Agnostic & Cloud Data Warehouses
- **Powered by Ibis**: Zero-copy lazy analytical pushdown across 20+ backends including **Snowflake, Google BigQuery, PostgreSQL, MotherDuck, ClickHouse, Databricks, Trino, SQLite, and DuckDB**.
- **Dynamic Capabilities Detection**: `BackendCapabilities` dynamically introspects backend capabilities (concurrency locking, cancellation, case-insensitive `ILIKE` vs `LOWER() LIKE` fallback, parameter styling `%s` vs `?`, and physical `rowid` independence).
- **Lazy Table Catalog & TTL Caching**: `IbisTableCatalog` manages lazy `ibis.Table` references and qualified `database.schema.table` resolution, caching schemas for `PIVOT_SCHEMA_CACHE_TTL_SECONDS` to eliminate expensive network metadata queries.
- **Bounded Arrow Batch Streaming**: Stream multi-gigabyte exports from cloud warehouses with bounded `.limit(offset=...)` pagination if native batch cursors are unavailable.
- **Fail-Closed Cloud Governance**: Remote cloud warehouses are read-only by default. Mutations (`PIVOT_ALLOW_REMOTE_MUTATIONS=1`) and background DDL rollup creation (`PIVOT_ALLOW_REMOTE_DDL=1`) are strictly opt-in.
- **CDC Support**: Support for both polling-based and push-based (webhook) Change Data Capture.

---

## 🛠️ Configuration (.env)

Create a `.env` file in the root directory:

```env
# Database & Cloud Warehouse
BACKEND_URI=duckdb://data.db  # or snowflake://..., bigquery://..., postgresql://..., md:...
BACKEND_TYPE=duckdb
BACKEND_DATABASE=your_database  # optional default database/catalog
BACKEND_SCHEMA=your_schema      # optional default schema
BACKEND_READ_ONLY=0             # 1 to enforce read-only analytical mode
PIVOT_ALLOW_REMOTE_DDL=0        # 1 to permit background rollup table creation on remote DW
PIVOT_ALLOW_REMOTE_MUTATIONS=0  # 1 to permit SQL write-back mutations on remote DW
PIVOT_SCHEMA_CACHE_TTL_SECONDS=300 # TTL in seconds for table schema metadata cache
PIVOT_ESTIMATE_ROW_COUNT_QUERY=0   # 1 to run live COUNT(*) during planning cost estimation

# Security
PIVOT_API_KEY=your-secret-key-here
JWT_SECRET_KEY=your-jwt-signing-secret
JWT_ISSUER=https://identity.example.com/

# Cache
CACHE_TYPE=memory  # or redis
CACHE_TTL=300

# Performance
TILE_SIZE=100
MAX_HIERARCHY_DEPTH=10

# Exact CDC (requires the distributed Redis controls in docs/operations/exact-cdc.md)
CDC_EXACT_STATE_STORE=memory  # use redis only with validated distributed runtime state
```

---

## 🚀 Quick Start

### Start the Server
```bash
python pivot_engine/main.py
```

### Authenticated Request
All API requests require the `X-API-Key` header.

```bash
curl -X POST http://localhost:8000/pivot \
     -H "X-API-Key: your-secret-key-here" \
     -H "Content-Type: application/json" \
     -d '{"table": "sales", "measures": [{"field": "amount", "agg": "sum", "alias": "total"}]}'
```

---

## 📡 API Endpoints

### 📊 Querying
| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/pivot` | `POST` | Standard pivot aggregation. |
| `/pivot/hierarchical` | `POST` | Tree-based hierarchical result. |
| `/pivot/tanstack` | `POST` | Direct integration with TanStack Table state. |
| `/pivot/virtual-scroll`| `POST` | Visible window for infinite scrolling. |

### 📥 Export & Real-time
| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/pivot/export` | `POST` | Stream CSV/Parquet download. |
| `/pivot/cdc/push-setup`| `POST` | Initialize push-mode CDC for a table. |
| `/pivot/cdc/push/{table}`| `POST` | Ingest external change events. |
| `/ws/pivot/{id}` | `WS` | WebSocket for real-time updates. |

### Exact CDC contract

Polling CDC safely invalidates results but does not provide event-speed
aggregation. For exact inserts, updates, deletes, duplicate extrema, replay,
and restart recovery, use a native/push source that supplies complete row
images plus `source_rows` captured with an atomic source snapshot and
contiguous event watermark. See
[`../../docs/operations/exact-cdc.md`](../../docs/operations/exact-cdc.md) for the
required event envelope, `source_checkpoints`, freshness states, and Redis
deployment controls.

---

## 🛡️ Row-Level Security (RLS)

RLS is enforced automatically based on the user's attributes. When using the API, the system resolves the user from the API Key. In a production deployment, you can extend `security.py` to link keys to specific filter attributes.

**Example Logic:**
If a user is assigned `{"region": "North"}`, the engine automatically appends `WHERE region = 'North'` to **all** queries, including exports and drill-downs.

---

## Dataset authorization

The table name in a browser request is not authority to read a dataset. The
HTTP runtime and table-addressing REST routes are default-deny until the host
installs a `RuntimeDatasetAuthorizer`; the policy is evaluated with the
server-authenticated principal before the controller is called.

```python
from pivot_engine.complete_rest_api import CompletePivotAPI
from pivot_engine.runtime import RuntimeDatasetAuthorizer

api = CompletePivotAPI(
    controller,
    dataset_authorizer=RuntimeDatasetAuthorizer({
        "sales": {
            "roles": {"viewer", "editor", "admin"},
            "operations": {"read", "export", "schema", "subscribe"},
            "rowSecurity": {"mode": "global"},
        },
    }),
)
```

Configure `"*"` only when every authenticated role is deliberately allowed
to reach every dataset. CDC setup and push require both an explicit `"cdc"`
dataset operation and an admin principal. WebSocket connections authenticate
before the handshake, subscriptions require `"subscribe"`, and update notices
are tenant-scoped invalidations rather than raw CDC rows.

Materialization job IDs are also principal-bound. The creating principal is
stored with the job, and `GET /pivot/jobs/{job_id}` returns `404` unless the
caller owns the job (or is an authorized admin) and still has read access to
the job's dataset. This intentionally hides whether a foreign job ID exists.

## Production security contract

Set `ENV=production` only after configuring the following server-owned
controls: HTTPS public origin, trusted hosts and proxy CIDRs, a CSP containing
`frame-ancestors`, an authenticated `rediss://` rate-limit/cache service,
positive request limits, and a rotating JWT key ring. Production JWTs require
an allowed `kid`, `exp`, `iss`, and `aud`; use `PIVOT_JWT_KEYS_JSON` and
`PIVOT_JWT_ACTIVE_KID`, not a shared Flight token. Arrow Flight must receive a
per-principal resolver plus the same `RuntimeDatasetAuthorizer` used by HTTP.

Use [the production runbook](../../docs/operations/production-security-runbook.md)
and [reference Nginx configuration](../../deploy/nginx/serverside-pivot.conf)
as the release checklist. The verifier records edge headers and HTTPS redirect
behaviour:

```bash
python scripts/verify_production_deployment.py --base-url https://pivot.example.com
```

The verifier is intentionally not evidence of certificate issuance, a
penetration test, backup recovery, or incident response; attach those
operator-run records before release.

---

## 🧪 Development & Testing

```bash
# Run the implementation verification suite
pytest tests/test_features_impl.py

# Run the full integration suite
pytest tests/test_complete_implementation.py
```

## 📄 License
MIT © 2025 Pivot Engine Team
