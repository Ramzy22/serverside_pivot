# Run the pivot engine on Oracle Free

From the repository root, with Docker running Linux containers and Docker Compose 2.17 or later, run

```sh
docker compose -f tests/oracle_db/compose.yaml run --build --rm verify
```

Compose starts Oracle Free, waits for its database health check, and runs pytest in a Python container using the checkout and its declared runtime dependencies. The first run downloads the database image and Python packages. Oracle listens on the Compose network. The application user and password in the Compose file are local test credentials.

Each pytest worker connects to `FREEPDB1` through the engine's `IbisBackend` Oracle URI connector, creates a uniquely named ledger table, and commits 192 sales and refunds. The seed includes repeated buyers, missing prices, case variants, blank channels, and fixed-point amounts. The fixture checks the inserted row and non-NULL channel counts before running the pivot core and drops its own table at teardown.

The suite compares ordered row paths, measures, subtotals, pivot cells, page sizes, total row counts, and grand totals against the repository's independent Python reference. The cases include case-insensitive and aggregate filters, Oracle empty-string handling, a page past the end, and a filter matching zero records. Monetary sums must match exactly. Decimal averages allow half a cent because the Arrow result has scale two.

The 17 scenarios also cover minimum and maximum, the leaf-partial strategy, all-data totals, collapsed hierarchy nodes, NULL membership filters, zero-size pages, and subtotals after their children. Mixed-case quoted identifiers and a NOT NULL source dimension exercise Oracle metadata handling and the nullable dimensions introduced by totals.

The pinned image uses Oracle 23.26.4 (26ai). The runner uses four pytest workers at most, with a separate committed table per worker. A successful local run on this image reported `17 passed`.

The test command returns pytest's exit code. Connection, SQL, capability, and parity errors fail the live suite. Ordinary repository test runs skip these cases unless `PIVOT_TEST_ORACLE_URI` is set.

The database stays running for repeated tests. To release this simulation's database, network, and volumes, run

```sh
docker compose -f tests/oracle_db/compose.yaml down -v
```

## The React flagship on Oracle

The `runtime` service (Compose profile `react`) serves the flagship runtime on Oracle: `react_runtime.py` seeds the 12,000-row `revenue_operations` demo table with Oracle types (`NUMBER(14,2)` money, `NUMBER(10)` units) and publishes the Pivot Runtime Protocol V1 on `127.0.0.1:8765`. Backend errors are logged in full in the container output; clients only see a stable code.

```sh
docker compose -f tests/oracle_db/compose.yaml --profile react up --build -d runtime
cd examples/flagship_react && npm run preview    # http://127.0.0.1:5174
docker compose -f tests/oracle_db/compose.yaml --profile react logs -f runtime
```

Stop it with `docker compose -f tests/oracle_db/compose.yaml --profile react stop runtime`.
