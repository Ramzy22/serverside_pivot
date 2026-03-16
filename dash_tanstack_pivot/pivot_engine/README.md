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

### 🌐 Backend Agnostic
- **Powered by Ibis**: Support for 20+ backends including **DuckDB, Clickhouse, PostgreSQL, BigQuery, Snowflake, and MySQL**.
- **CDC Support**: Support for both polling-based and push-based (webhook) Change Data Capture.

---

## 🛠️ Configuration (.env)

Create a `.env` file in the root directory:

```env
# Database
BACKEND_URI=duckdb://data.db
BACKEND_TYPE=duckdb

# Security
PIVOT_API_KEY=your-secret-key-here

# Cache
CACHE_TYPE=memory  # or redis
CACHE_TTL=300

# Performance
TILE_SIZE=100
MAX_HIERARCHY_DEPTH=10
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

---

## 🛡️ Row-Level Security (RLS)

RLS is enforced automatically based on the user's attributes. When using the API, the system resolves the user from the API Key. In a production deployment, you can extend `security.py` to link keys to specific filter attributes.

**Example Logic:**
If a user is assigned `{"region": "North"}`, the engine automatically appends `WHERE region = 'North'` to **all** queries, including exports and drill-downs.

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