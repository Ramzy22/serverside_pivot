"""Deterministic Arrow facts and an independent, non-SQL aggregate oracle.

Pivot IDs are ``Pxxx|measure``. Missing/all-null sum, average, min and max
cells are None; count counts non-null measures (zero for an absent bucket).
The oracle returns all five statistics per cell, keyed by full group tuples.
"""

from __future__ import annotations

import hashlib
import json
import math
from numbers import Real

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


GROUP_FIELDS = ("region", "account", "product")
MEASURES = ("sales_cents", "units")
TEXT_FIELDS = (*GROUP_FIELDS, "period")
NUMERIC_FIELDS = ("record_id", *MEASURES)
SHAPES = {"hierarchy": (5000, 5, 5, 4), "wide": (8, 16, 8, 256)}
PIVOT_SEPARATOR = "|"


def periods_for_shape(shape: str) -> tuple[str, ...]:
    if not isinstance(shape, str) or shape not in SHAPES:
        raise ValueError("unknown dataset shape")
    return tuple(f"P{i:03d}" for i in range(SHAPES[shape][3]))


def pivot_fields(shape: str, measures=MEASURES) -> list[str]:
    return [f"{period}{PIVOT_SEPARATOR}{measure}" for period in periods_for_shape(shape) for measure in measures]


def build_dataset(rows: int, shape: str) -> pa.Table:
    """Build column vectors, never a list of fact dictionaries.

    Mixed-radix dimensions have independent cycles. In one million hierarchy
    facts each of 5000 roots has every account/product/period combination twice.
    """
    if type(rows) is not int or rows < 0:
        raise ValueError("rows must be a non-negative integer")
    periods_for_shape(shape)
    ids = np.arange(rows, dtype=np.int64)
    arrays = {"record_id": pa.array(ids)}
    stride = 1
    for field, size, prefix in zip(TEXT_FIELDS, SHAPES[shape], ("Region", "Account", "Product", "P")):
        labels = [f"{prefix}{i:04d}" if field != "period" else f"P{i:03d}" for i in range(size)]
        arrays[field] = pc.take(pa.array(labels, type=pa.string()), pa.array((ids // stride) % size))
        stride *= size
    arrays["sales_cents"] = pa.array((ids * 37 % 200_003) - 50_000, type=pa.int64())
    arrays["units"] = pa.array(ids * 13 % 97, type=pa.int64())
    return pa.table(arrays).replace_schema_metadata({b"million.shape": shape.encode()})


def dataset_metadata(table: pa.Table, shape: str) -> dict:
    """JSON-safe source identity and explicit stable pivot-column identities."""
    periods = periods_for_shape(shape)
    digest = hashlib.sha256()
    schema = [{"name": field.name, "type": str(field.type)} for field in table.schema]
    digest.update(json.dumps({"schema": schema, "rows": table.num_rows}, sort_keys=True).encode())
    # take materializes logical rows: Arrow IPC alone may retain bytes outside
    # a sliced string view. It also normalizes redundant all-valid bitmaps.
    canonical = table.take(pa.array(np.arange(table.num_rows, dtype=np.int64))).combine_chunks().replace_schema_metadata(None)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, canonical.schema) as writer:
        writer.write_table(canonical)
    digest.update(memoryview(sink.getvalue()))
    return {
        "shape": shape, "rows": table.num_rows, "source_bytes": table.nbytes,
        "source_sha256": digest.hexdigest(), "schema": schema,
        "dimensions": list(TEXT_FIELDS), "group_fields": list(GROUP_FIELDS),
        "measures": list(MEASURES), "pivot_field": "period", "periods": list(periods),
        "pivot_result_fields": pivot_fields(shape), "pivot_separator": PIVOT_SEPARATOR,
        "cardinalities": {field: len(pc.unique(table[field])) for field in TEXT_FIELDS},
    }


def validate_filters(filters: dict | None) -> dict:
    """Validate the intentionally small explicit AG filter-model surface."""
    if filters is None:
        return {}
    if not isinstance(filters, dict):
        raise ValueError("filterModel must be an object")
    for field, spec in filters.items():
        if field not in (*TEXT_FIELDS, *NUMERIC_FIELDS) or not isinstance(spec, dict):
            raise ValueError("unsupported filter field or model")
        kind = spec.get("filterType")
        if kind == "set" and field in TEXT_FIELDS:
            values = spec.get("values")
            if set(spec) != {"filterType", "values"} or not isinstance(values, list):
                raise ValueError("set filter requires values")
            if len(values) > 5000 or any(value is not None and not isinstance(value, str) for value in values):
                raise ValueError("invalid set values")
        elif kind == "text" and field in TEXT_FIELDS:
            if set(spec) != {"filterType", "type", "filter"} or spec.get("type") != "equals" or not isinstance(spec.get("filter"), str):
                raise ValueError("only exact text equality is supported")
        elif kind == "number" and field in NUMERIC_FIELDS:
            if set(spec) != {"filterType", "type", "filter"} or not isinstance(spec.get("type"), str) or spec.get("type") not in {"equals", "notEqual", "lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual"}:
                raise ValueError("unsupported numeric comparison")
            value = spec.get("filter")
            if isinstance(value, bool) or not isinstance(value, Real) or not -(2**63) <= value < 2**63 or not math.isfinite(value):
                raise ValueError("numeric filter must be finite and fit int64 range")
        else:
            raise ValueError("unsupported filter type")
    return filters


def _matches(values: dict, filters: dict) -> bool:
    # Independent evaluation: no SQL/compiler expressions are reused here.
    for field, spec in filters.items():
        value = values[field]
        if spec["filterType"] == "set":
            if value not in spec["values"]:
                return False
        elif spec["filterType"] == "text":
            if value != spec["filter"]:
                return False
        else:
            if value is None:
                return False
            other = spec["filter"]
            comparison = spec["type"]
            if comparison == "equals" and value != other:
                return False
            if comparison == "notEqual" and value == other:
                return False
            if comparison == "lessThan" and value >= other:
                return False
            if comparison == "lessThanOrEqual" and value > other:
                return False
            if comparison == "greaterThan" and value <= other:
                return False
            if comparison == "greaterThanOrEqual" and value < other:
                return False
    return True


def expected_cells(table: pa.Table, *, group_keys=(), filters=None) -> dict:
    """Aggregate selected next-level cells with Python integer arithmetic.

    ``rows`` maps full group tuples to cell statistics. ``totals`` covers the
    filtered path; ``row_count`` is the number of distinct next-level groups.
    Oracle storage grows with aggregate cells, not fact dictionaries. Null
    group keys are represented as None and sort last, matching the datasource.
    """
    filters = validate_filters(filters)
    if len(group_keys) >= len(GROUP_FIELDS) or any(key is not None and not isinstance(key, str) for key in group_keys):
        raise ValueError("oracle expects a group path before the leaf level")
    shape = (table.schema.metadata or {}).get(b"million.shape", b"hierarchy").decode()
    # External fixtures can select the wide shape by including a distant period.
    if shape == "hierarchy" and any(value is not None and value > "P003" for value in pc.unique(table["period"]).to_pylist()):
        shape = "wide"
    periods = periods_for_shape(shape)
    fields = [*MEASURES, *pivot_fields(shape)]
    groups = {}
    totals = {}

    def add(cells, field, value):
        if value is None:
            return
        stats = cells.setdefault(field, [0, 0, None, None])
        stats[0] += int(value)
        stats[1] += 1
        stats[2] = value if stats[2] is None else min(stats[2], value)
        stats[3] = value if stats[3] is None else max(stats[3], value)

    for batch in table.to_batches(max_chunksize=16_384):
        columns = {name: batch.column(name).to_pylist() for name in table.column_names}
        for index in range(batch.num_rows):
            path = tuple(columns[field][index] for field in GROUP_FIELDS)
            if path[:len(group_keys)] != tuple(group_keys):
                continue
            if filters and not _matches({field: columns[field][index] for field in filters}, filters):
                continue
            key = path[:len(group_keys) + 1]
            cells = groups.setdefault(key, {})
            period = columns["period"][index]
            for measure in MEASURES:
                value = columns[measure][index]
                for target in (cells, totals):
                    add(target, measure, value)
                    if period in periods:
                        add(target, f"{period}{PIVOT_SEPARATOR}{measure}", value)

    def finish(cells):
        result = {}
        for field in fields:
            total, count, minimum, maximum = cells.get(field, (0, 0, None, None))
            result[field] = {"sum": total if count else None, "count": count,
                             "avg": total / count if count else None, "min": minimum, "max": maximum}
        return result

    return {"rows": {key: finish(cells) for key, cells in sorted(groups.items(), key=lambda item: tuple((v is None, v or "") for v in item[0]))},
            "totals": finish(totals), "row_count": len(groups), "group_field": GROUP_FIELDS[len(group_keys)], "periods": list(periods)}
