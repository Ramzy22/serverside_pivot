"""Serve the flagship runtime (React host) from the Oracle Free container.

Same app as ``examples/flagship_runtime_server.py``; only the adapter differs: it
runs on ``PIVOT_ORACLE_URI`` and seeds the flagship demo table into Oracle at startup.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache

import examples.flagship_demo_data as demo
from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime.router import RuntimeRequestRouter

log = logging.getLogger("oracle_react_runtime")
logging.basicConfig(level=logging.INFO)

# Clients only see a stable code; this local simulation logs the driver error in the container output.
_safe_code = RuntimeRequestRouter._safe_backend_error_code


def _logged_code(exc: BaseException) -> str:
    log.error("backend error: %s", getattr(exc, "detail", None) or exc, exc_info=exc)
    return _safe_code(exc)


RuntimeRequestRouter._safe_backend_error_code = staticmethod(_logged_code)


# Typed like a real Oracle ledger: fixed-point money, integer units. (Letting Ibis infer the
# DDL from Arrow float64 yields FLOAT(126), a decimal NUMBER the driver returns as Decimal while
# Ibis expects float64, so every aggregate over it fails to fetch.)
_TYPES = {
    "region": "VARCHAR2(32)", "segment": "VARCHAR2(32)", "team": "VARCHAR2(32)", "product": "VARCHAR2(32)",
    "channel": "VARCHAR2(32)", "quarter": "VARCHAR2(2)", "owner": "VARCHAR2(64)",
    "revenue": "NUMBER(14,2)", "gross_margin": "NUMBER(14,2)", "budget": "NUMBER(14,2)",
    "units": "NUMBER(10)", "margin_pct": "NUMBER(6,4)", "variance": "NUMBER(14,2)",
}


def _seed(adapter) -> None:
    rows = demo.build_demo_table().to_pylist()
    assert set(rows[0]) == set(_TYPES), sorted(rows[0])
    name, cols = demo.TABLE_NAME, list(_TYPES)
    with adapter.controller.con.begin() as cur:
        cur.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"{name}\" PURGE'; "
                    "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;")
        cur.execute(f'CREATE TABLE "{name}" (' + ", ".join(f'"{c}" {_TYPES[c]}' for c in cols) + ")")
        cur.executemany(f'INSERT INTO "{name}" (' + ",".join(f'"{c}"' for c in cols) + ") VALUES ("
                        + ",".join(f":{i + 1}" for i in range(len(cols))) + ")",
                        [tuple(r[c] for c in cols) for r in rows])
    log.info("seeded %s rows into Oracle table %s", len(rows), name)


@lru_cache(maxsize=1)
def oracle_adapter():
    adapter = create_tanstack_adapter(backend_uri=os.environ["PIVOT_ORACLE_URI"])
    assert adapter.controller.con.name == "oracle", adapter.controller.con.name
    _seed(adapter)
    adapter.controller.catalog.invalidate(demo.TABLE_NAME)
    return adapter


# flagship_runtime_server binds the getter at import, so replace it first.
demo.get_demo_adapter = oracle_adapter
oracle_adapter()  # seed now, so a connection or load failure stops the server at boot

from examples.flagship_runtime_server import app  # noqa: E402

__all__ = ["app"]
