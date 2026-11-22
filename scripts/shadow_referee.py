"""Referee shadow diffs with the independent oracle (M2 gate).

Shadow mode classifies each legacy/core difference heuristically.  With
``PIVOT_CORE_SHADOW_CAPTURE=1`` an entry also carries the (small, scoped)
dataset and the core query; this script re-evaluates those with the oracle:

* core == oracle on the whole tree  -> every diff of the entry is a legacy bug
  (its finding id when the heuristic named one, else ``legacy:oracle-confirmed``);
* core != oracle                    -> ``core-bug`` (the gate fails).

Entries without a capture keep their heuristic class.  Usage::

    python scripts/shadow_referee.py artifacts/shadow [--json out.json]

Exit status 1 when any core bug, unclassified or unattributed diff remains.
"""

from __future__ import annotations

import argparse
import base64
import collections
import glob
import json
import math
import os
import sys
from dataclasses import replace
from decimal import Decimal
from typing import Any, Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [ROOT, os.path.join(ROOT, "serverside_pivot", "pivot_engine")]

import ibis  # noqa: E402
import pyarrow as pa  # noqa: E402

from pivot_engine.core.exec import execute  # noqa: E402
from pivot_engine.core.ir import Window, from_wire  # noqa: E402
from tests.oracle.ir_adapter import oracle_rows  # noqa: E402


def _same(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    return bool(a == b)


def referee(capture: Dict[str, Any]) -> Dict[str, Any]:
    table = pa.ipc.open_stream(base64.b64decode(capture["data"])).read_all()
    query = replace(from_wire(capture["query"]), window=Window())
    con = ibis.duckdb.connect()
    con.create_table(query.dataset, table)
    core = [(r["path"], r["values"]) for r in execute(query, con.table).records()]
    oracle = oracle_rows(query, table.to_pylist())
    if [p for p, _ in core] != [p for p, _ in oracle]:
        return {"agrees": False, "why": "paths/order differ",
                "core_only": [list(p) for p, _ in core if p not in {q for q, _ in oracle}][:5],
                "oracle_only": [list(p) for p, _ in oracle if p not in {q for q, _ in core}][:5]}
    for (path, cv), (_, ov) in zip(core, oracle, strict=True):
        bad = {k: [cv.get(k), ov[k]] for k in ov if not _same(cv.get(k), ov[k])}
        if bad:
            return {"agrees": False, "why": "values differ", "path": list(path), "values": bad}
    dims = list(query.rows) + list(query.columns)
    records = table.to_pylist()
    return {"agrees": True, "empty": not records,
            "null_dimension": any(rec.get(d) is None for rec in records for d in dims)}


def attribute(diff: Dict[str, Any], verdict: Dict[str, Any]) -> str:
    """Name the legacy finding for an oracle-confirmed diff."""
    cls = diff.get("class", "unclassified")
    if cls.startswith("legacy:"):
        return cls
    if verdict.get("empty"):
        return "legacy:F-TOT-EMPTY"
    if verdict.get("null_dimension"):
        return "legacy:F-NULL-GT"
    return "legacy:oracle-confirmed"  # confirmed legacy bug still needing a finding id


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("shadow_dir")
    parser.add_argument("--json")
    args = parser.parse_args(argv)
    counts: collections.Counter = collections.Counter()
    core_bugs: List[Dict[str, Any]] = []
    for path in sorted(glob.glob(os.path.join(args.shadow_dir, "*.jsonl"))):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                entry = json.loads(line)
                verdict = referee(entry["capture"]) if entry.get("capture", {}).get("data") else None
                for diff in entry["diffs"]:
                    cls = diff.get("class", "unclassified")
                    if verdict is not None:
                        if verdict["agrees"]:
                            cls = attribute(diff, verdict)
                            if cls == "legacy:oracle-confirmed":
                                core_bugs.append({"spec": entry.get("spec"), "diff": diff, "verdict": verdict,
                                                  "note": "unattributed"})
                        else:
                            cls = "core-bug"
                            core_bugs.append({"spec": entry.get("spec"), "diff": diff, "verdict": verdict})
                    counts[cls] += 1
    report = {"classes": dict(counts.most_common()), "core_bugs": core_bugs[:50]}
    print(json.dumps(report, indent=2, default=str))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
    failing = ("core-bug", "unclassified", "legacy:oracle-confirmed")
    return 1 if any(counts.get(k) for k in failing) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
