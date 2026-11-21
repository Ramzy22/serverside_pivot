"""Write tests/conformance/vectors.json from the oracle.

Vectors are language-neutral (JSON): the Python engine (test_conformance_python)
and the TypeScript DuckDB-Wasm emitter (M3) both consume them.  Expected
values come *only* from tests/oracle/oracle.py.

    python -m tests.conformance.generate_vectors
"""

from __future__ import annotations

import json
import pathlib
from typing import Any, Dict, List

from tests.oracle.oracle import Measure, MeasureFilter, Query, pivot

OUT = pathlib.Path(__file__).with_name("vectors.json")

R4 = {  # audit repro r4: NULL dimension group + non-additive parents
    "r": ["N", "N", None, None, "S"],
    "c": ["a", "b", "x", "y", "a"],
    "v": [10, 50, 1, 2, 7],
    "u": ["k", "k", "m", "m", "k"],
}
TYPES = {"r": "string", "c": "string", "u": "string", "v": "int64"}


def _records(columns: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    names = list(columns)
    return [dict(zip(names, values, strict=True)) for values in zip(*columns.values(), strict=True)]


def vec(vid, reqs, columns, rows, measures, measure_filters=(), totals="visible", note=""):
    query = Query(rows=rows, measures=[Measure(*m) for m in measures],
                  measure_filters=[MeasureFilter(*f) for f in measure_filters], totals=totals)
    expected = [{"path": list(r.path), "values": dict(r.values)} for r in pivot(query, _records(columns))]
    return {
        "id": vid,
        "requirements": reqs,
        "note": note,
        "data": {"columns": {k: {"type": TYPES.get(k, "string"), "values": v} for k, v in columns.items()}},
        "query": {
            "rows": rows,
            "measures": [{"alias": a, "agg": g, "field": f} for a, g, f in measures],
            "measureFilters": [{"level": l, "measure": m, "op": o, "value": v} for l, m, o, v in measure_filters],
            "totals": totals,
        },
        "expected": {"rows": expected},
    }


VECTORS = [
    vec("null-group-is-not-grand-total", ["REQ-NULL-002", "REQ-BACK-001"], R4, ["r", "c"],
        [("v_sum", "sum", "v")], note="audit r4"),
    vec("parent-avg-max-distinct-from-records", ["REQ-TOT-003", "REQ-AGG-002", "REQ-AGG-003"],
        {k: R4[k][:2] + R4[k][4:] for k in R4}, ["r", "c"],
        [("v_avg", "avg", "v"), ("v_max", "max", "v"), ("u_cd", "count_distinct", "u")], note="audit r5/r6"),
    vec("case-variants-are-distinct-groups", ["REQ-BACK-002"],
        {"r": ["A", "a", "A"], "v": [1, 10, 100]}, ["r"], [("v_sum", "sum", "v")]),
    vec("delimiter-values-do-not-collide", ["REQ-ID-001"],
        {"r": ["x|||y", "x", "x_y", "x"], "c": ["z", "y|||z", "z", "y_z"], "v": [1, 2, 4, 8]}, ["r", "c"],
        [("v_sum", "sum", "v")]),
    vec("leaf-measure-filter-visible-totals", ["REQ-TOT-001", "REQ-TOT-003", "REQ-TOT-006"],
        {"r": ["A", "A", "B"], "c": ["x", "y", "x"], "v": [1, 10, 2]}, ["r", "c"],
        [("v_sum", "sum", "v")], measure_filters=[(2, "v_sum", ">", 5)]),
    vec("all-null-measure-sums-to-null", ["REQ-AGG-002", "REQ-NULL-001"],
        {"r": ["A", "A"], "v": [None, None]}, ["r"], [("v_sum", "sum", "v"), ("v_cnt", "count", "v")]),
]


def main() -> None:
    OUT.write_text(json.dumps({"version": 1, "vectors": VECTORS}, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(VECTORS)} vectors to {OUT}")


if __name__ == "__main__":
    main()
