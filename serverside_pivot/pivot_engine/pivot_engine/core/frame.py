"""ResultFrame: the one result shape every edge maps from (Arrow + identity)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from .identity import CanonicalScalarCodec

DEPTH = "__depth"
ORD = "__ord"  # a row's 0-based position in the whole ordered tree
TOTAL = "__total"  # the whole tree's row count, on every row of a windowed read


@dataclass(frozen=True)
class ResultFrame:
    table: pa.Table  # dimensions, measures, __depth: the window's rows in order
    rows: Tuple[str, ...]
    measures: Tuple[str, ...]
    fingerprint: str = ""
    revision: Optional[int] = None
    window: Tuple[int, Optional[int]] = (0, None)
    meta: Dict[str, Any] = field(default_factory=dict)
    total: Optional[int] = None  # rows of the whole tree (the window's total), from the same statement
    grand_total: Optional[pa.Table] = None  # the depth-0 row, when the read asked for it

    def records(self) -> List[Dict[str, Any]]:
        """Rows with typed path, canonical identity and depth (REQ-ID-001/002)."""
        return _records(self.table, self.rows, self.measures)

    def grand_total_record(self) -> Optional[Dict[str, Any]]:
        """The grand total as a record, when the read asked for it and the tree has one."""
        if self.grand_total is None or not self.grand_total.num_rows:
            return None
        return _records(self.grand_total, self.rows, self.measures)[0]


def split_window(table: pa.Table, window: Tuple[int, Optional[int]]) -> Tuple[pa.Table, int, pa.Table]:
    """(the window's rows, the tree's row count, the grand-total rows) of one ``windowed`` read."""
    if TOTAL not in table.column_names:  # the whole ordered tree
        body = table.drop_columns([c for c in (ORD,) if c in table.column_names])
        return body, table.num_rows, body.filter(pc.equal(body[DEPTH], 0))
    total = int(table[TOTAL][0].as_py()) if table.num_rows else 0
    offset, limit = window
    position = table[ORD]
    inside = pc.greater_equal(position, offset)
    if limit is not None:
        inside = pc.and_(inside, pc.less(position, offset + limit))
    table = table.drop_columns([ORD, TOTAL])
    return table.filter(inside), total, table.filter(pc.equal(table[DEPTH], 0))


def _records(table: pa.Table, rows: Tuple[str, ...], measures: Tuple[str, ...]) -> List[Dict[str, Any]]:
    """Columnar: each column is read once, and each distinct member value is encoded once per row
    dimension; a key is the JSON array of its members' encodings, so it equals
    ``CanonicalPathCodec.encode(path)`` exactly."""
    data = table.to_pydict()
    depths = data.pop(DEPTH)
    dims = [data[k] for k in rows]
    values = [(m, data[m]) for m in measures]
    tokens: List[Dict[Any, str]] = [{} for _ in rows]
    out = []
    for i, raw_depth in enumerate(depths):
        depth = int(raw_depth)
        path = tuple(dims[j][i] for j in range(depth))
        out.append({
            "path": path,
            "depth": depth,
            "key": "[" + ",".join(_token(tokens[j], v) for j, v in enumerate(path)) + "]",
            "values": {m: column[i] for m, column in values},
            "dims": {rows[j]: path[j] for j in range(depth)},
        })
    return out


def _token(memo: Dict[Any, str], value: Any) -> str:
    """One member's element of a canonical path: the JSON string of its scalar encoding (memoized per column).

    A column holds one Python type, so the value keys the memo, except floats: ``0.0 == -0.0`` encode differently.
    """
    key = ("float", repr(value)) if isinstance(value, float) else value
    token = memo.get(key)
    if token is None:
        token = json.dumps(CanonicalScalarCodec.encode(value), ensure_ascii=False)
        memo[key] = token
    return token
