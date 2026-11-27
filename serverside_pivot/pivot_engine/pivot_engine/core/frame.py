"""ResultFrame: the one result shape every edge maps from (Arrow + identity)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .identity import CanonicalScalarCodec

DEPTH = "__depth"
ORD = "__ord"  # a row's 0-based position in the whole ordered tree
TOTAL = "__total"  # the whole tree's row count, on every row of a windowed read


def _col_to_pylist(chunked: Any) -> List[Any]:
    """Fast Arrow column to Python list conversion preserving exact NULLs and types."""
    n = len(chunked)
    if n == 0:
        return []
    arr = chunked.combine_chunks() if chunked.num_chunks > 1 else chunked.chunk(0)
    t = arr.type
    nc = arr.null_count
    if nc == n:
        return [None] * n
    if nc == 0:
        if (pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_boolean(t)
                or pa.types.is_string(t) or pa.types.is_large_string(t)):
            return list(arr.to_numpy(zero_copy_only=False).tolist())
    elif pa.types.is_string(t) or pa.types.is_large_string(t):
        return list(arr.to_numpy(zero_copy_only=False).tolist())
    elif pa.types.is_floating(t):
        out: List[Any] = list(arr.to_numpy(zero_copy_only=False).tolist())
        valid = arr.is_valid().to_numpy(zero_copy_only=False)
        for idx in np.flatnonzero(~valid):
            out[int(idx)] = None
        return out
    elif pa.types.is_integer(t) or pa.types.is_boolean(t):
        fill_val = pa.scalar(False if pa.types.is_boolean(t) else 0, type=t)
        filled = pc.fill_null(arr, fill_val)
        out = list(filled.to_numpy(zero_copy_only=False).tolist())
        valid = arr.is_valid().to_numpy(zero_copy_only=False)
        for idx in np.flatnonzero(~valid):
            out[int(idx)] = None
        return out
    return list(arr.to_pylist())


def _slice_indices(table: pa.Table, hits: Any) -> pa.Table:
    n = len(hits)
    if n == 0:
        return table.slice(0, 0)
    first = int(hits[0])
    if n == int(hits[-1]) - first + 1:
        return table.slice(first, n)
    return table.take(pa.array(hits, type=pa.int64()))


def _slice_dict(data: Dict[str, List[Any]], hits: Any) -> Dict[str, List[Any]]:
    n = len(hits)
    if n == 0:
        return {k: [] for k in data}
    first = int(hits[0])
    if n == int(hits[-1]) - first + 1:
        end = first + n
        return {k: v[first:end] for k, v in data.items()}
    idx_list = [int(i) for i in hits]
    return {k: [v[i] for i in idx_list] for k, v in data.items()}


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
    _data: Optional[Dict[str, List[Any]]] = field(default=None, repr=False, compare=False)
    _gt_data: Optional[Dict[str, List[Any]]] = field(default=None, repr=False, compare=False)

    def records(self) -> List[Dict[str, Any]]:
        """Rows with typed path, canonical identity and depth (REQ-ID-001/002)."""
        if self._data is not None:
            return _records_from_dict(self._data, self.rows, self.measures)
        return _records(self.table, self.rows, self.measures)

    def grand_total_record(self) -> Optional[Dict[str, Any]]:
        """The grand total as a record, when the read asked for it and the tree has one."""
        if self.grand_total is None or not self.grand_total.num_rows:
            return None
        if self._gt_data is not None:
            return _records_from_dict(self._gt_data, self.rows, self.measures)[0]
        return _records(self.grand_total, self.rows, self.measures)[0]


def split_window(table: pa.Table, window: Tuple[int, Optional[int]]) -> Tuple[pa.Table, int, pa.Table]:
    """(the window's rows, the tree's row count, the grand-total rows) of one ``windowed`` read."""
    body, total, grand, _bd, _gd = split_window_with_data(table, None, window)
    return body, total, grand


def split_window_with_data(
    table: pa.Table,
    col_data: Optional[Dict[str, List[Any]]],
    window: Tuple[int, Optional[int]],
) -> Tuple[pa.Table, int, pa.Table, Optional[Dict[str, List[Any]]], Optional[Dict[str, List[Any]]]]:
    """Fast slice-based window split preserving optional pre-extracted column lists."""
    if TOTAL not in table.column_names:  # the whole ordered tree
        drop = [c for c in (ORD,) if c in table.column_names]
        body = table.drop_columns(drop) if drop else table
        if not body.num_rows:
            empty_d: Optional[Dict[str, List[Any]]] = (
                {k: [] for k in col_data if k != ORD} if col_data is not None else None
            )
            return body, 0, body, empty_d, empty_d
        depths = body[DEPTH].combine_chunks().to_numpy(zero_copy_only=False)
        gt_hits = np.flatnonzero(depths == 0)
        grand = _slice_indices(body, gt_hits)
        if col_data is None:
            return body, body.num_rows, grand, None, None
        clean = {k: v for k, v in col_data.items() if k != ORD}
        return body, body.num_rows, grand, clean, _slice_dict(clean, gt_hits)
    total = int(table[TOTAL][0].as_py()) if table.num_rows else 0
    if not table.num_rows:
        empty = table.drop_columns([ORD, TOTAL])
        empty_d = {k: [] for k in col_data if k not in {ORD, TOTAL}} if col_data is not None else None
        return empty, total, empty, empty_d, empty_d
    ords = table[ORD].combine_chunks().to_numpy(zero_copy_only=False)
    depths = table[DEPTH].combine_chunks().to_numpy(zero_copy_only=False)
    offset, limit = window
    inside = (ords >= offset) & (ords < offset + limit) if limit is not None else (ords >= offset)
    body_hits = np.flatnonzero(inside)
    gt_hits = np.flatnonzero(depths == 0)
    trimmed = table.drop_columns([ORD, TOTAL])
    body = _slice_indices(trimmed, body_hits)
    grand = _slice_indices(trimmed, gt_hits)
    if col_data is None:
        return body, total, grand, None, None
    clean = {k: v for k, v in col_data.items() if k not in {ORD, TOTAL}}
    return body, total, grand, _slice_dict(clean, body_hits), _slice_dict(clean, gt_hits)


def pivot_long(table: pa.Table, query: Any, defaults: Optional[Dict[Any, Any]] = None) -> pa.Table:
    """Widen a page's cells in Arrow; missing groups have the registry's empty value.

    The database returns only a row page and its requested column members.
    Holistic values are copied unchanged from their direct aggregate rows.
    """
    out, _ = pivot_long_with_data(table, query, defaults)
    return out


def pivot_long_with_data(
    table: pa.Table,
    query: Any,
    defaults: Optional[Dict[Any, Any]] = None,
) -> Tuple[pa.Table, Optional[Dict[str, List[Any]]]]:
    """Widen a page's cells and return `(Arrow table, optional column-list dict)` in one pass."""
    if "__is_cell" not in table.column_names:
        return table, None
    from .registry import REGISTRY

    col_names = set(table.column_names)
    base = [*query.rows, *[m.alias for m in query.measures], *[d.alias for d in query.derived], DEPTH]
    base += [c for c in (ORD, TOTAL) if c in col_names]
    num_rows = table.num_rows

    if num_rows == 0:
        selected_arr = pa.array([], type=pa.int64())
        row_indexes: List[int] = []
        n_sel = 0
    elif ORD in col_names:
        ord_np = table[ORD].combine_chunks().to_numpy(zero_copy_only=False)
        if num_rows <= 1 or bool(np.all(ord_np[1:] >= ord_np[:-1])):
            change = np.empty(num_rows, dtype=bool)
            change[0] = True
            if num_rows > 1:
                change[1:] = ord_np[1:] != ord_np[:-1]
            selected_np = np.flatnonzero(change)
            selected_arr = pa.array(selected_np, type=pa.int64())
            row_indexes = list((np.cumsum(change) - 1).tolist())
            n_sel = len(selected_np)
        else:
            ords = list(ord_np.tolist())
            indexes: Dict[Any, int] = {}
            selected_list: List[int] = []
            row_indexes = []
            for i in range(num_rows):
                key = ords[i]
                idx = indexes.get(key)
                if idx is None:
                    idx = len(selected_list)
                    indexes[key] = idx
                    selected_list.append(i)
                row_indexes.append(idx)
            selected_arr = pa.array(selected_list, type=pa.int64())
            n_sel = len(selected_list)
    else:
        depths = _col_to_pylist(table[DEPTH])
        row_cols = [_col_to_pylist(table[k]) for k in query.rows]
        indexes = {}
        selected_list = []
        row_indexes = []
        for i in range(num_rows):
            key = (depths[i], *(col[i] for col in row_cols))
            idx = indexes.get(key)
            if idx is None:
                idx = len(selected_list)
                indexes[key] = idx
                selected_list.append(i)
            row_indexes.append(idx)
        selected_arr = pa.array(selected_list, type=pa.int64())
        n_sel = len(selected_list)

    base_tbl = table.select(base).take(selected_arr)
    members = {tuple(m): j for j, m in enumerate(query.column_members)}
    cell_specs = list(query.cells())
    cells: Dict[str, List[Any]] = {}
    axis = query.unpivot
    axis_index = query.columns.index(axis.label) if axis is not None and axis.label in query.columns else None
    has_defaults = bool(defaults)
    simple_fill = axis is None and not has_defaults

    if simple_fill:
        for _j, _member, m, name in cell_specs:
            empty = REGISTRY.get(m.agg).empty if m.agg not in {"formula", "member"} else None
            cells[name] = [empty] * n_sel
    else:
        axis_in_rows = axis is not None and axis.label in query.rows
        base_axis_vals = _col_to_pylist(base_tbl[axis.label]) if axis_in_rows and axis is not None else None
        base_depth_vals = _col_to_pylist(base_tbl[DEPTH]) if base_axis_vals is not None else None
        axis_level = query.rows.index(axis.label) if axis_in_rows and axis is not None else -1
        for _j, member, m, name in cell_specs:
            empty = REGISTRY.get(m.agg).empty if m.agg not in {"formula", "member"} else None
            if axis is None:
                fill_val = defaults.get((None, m.alias), empty) if defaults is not None else empty
                cells[name] = [fill_val] * n_sel
            else:
                values = []
                for pos in range(n_sel):
                    label = member[axis_index] if axis_index is not None else None
                    if base_axis_vals is not None and base_depth_vals is not None:
                        label = base_axis_vals[pos] if base_depth_vals[pos] > axis_level else None
                    values.append(defaults.get((label, m.alias), defaults.get((None, m.alias), empty))
                                  if defaults is not None else empty)
                cells[name] = values

    # Fast columnar fill of present cell rows (only touch __is_cell, query.columns, __value_*).
    if num_rows > 0:
        is_cells = _col_to_pylist(table["__is_cell"])
        meas_lists = [(m.alias, _col_to_pylist(table[f"__value_{m.alias}"])) for m in query.measures]
        names_by_jm = {(j, m.alias): cells[name] for j, _member, m, name in cell_specs}
        if len(query.columns) == 1:
            col_vals = _col_to_pylist(table[query.columns[0]])
            members_1 = {m[0]: j for m, j in members.items()}
            for i in range(num_rows):
                if is_cells[i] is not True:
                    continue
                j = members_1.get(col_vals[i])
                if j is not None:
                    target = row_indexes[i]
                    for alias, mvals in meas_lists:
                        names_by_jm[(j, alias)][target] = mvals[i]
        else:
            col_lists = [_col_to_pylist(table[k]) for k in query.columns]
            for i in range(num_rows):
                if is_cells[i] is not True:
                    continue
                j = members.get(tuple(cl[i] for cl in col_lists))
                if j is not None:
                    target = row_indexes[i]
                    for alias, mvals in meas_lists:
                        names_by_jm[(j, alias)][target] = mvals[i]

    # Build the widened Arrow table in one O(N) pass matching union_all's exact column order:
    # (*rows, *measures, *derived, *cells, __depth[, __ord, __total]).
    lead_count = len(query.rows) + len(query.measures) + len(query.derived)
    arrays: List[Any] = [base_tbl.column(i) for i in range(lead_count)]
    fields: List[pa.Field] = [base_tbl.schema.field(i) for i in range(lead_count)]
    val_types = {m.alias: table.schema.field(f"__value_{m.alias}").type for m in query.measures}
    for _j, _member, m, name in cell_specs:
        dtype = val_types[m.alias]
        arrays.append(pa.array(cells[name], type=dtype))
        fields.append(pa.field(name, dtype, nullable=True))
    for i in range(lead_count, base_tbl.num_columns):
        arrays.append(base_tbl.column(i))
        fields.append(base_tbl.schema.field(i))
    out_tbl = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    col_dict: Dict[str, List[Any]] = {c: _col_to_pylist(base_tbl[c]) for c in base}
    col_dict.update(cells)
    return out_tbl, col_dict


def _records(table: pa.Table, rows: Tuple[str, ...], measures: Tuple[str, ...]) -> List[Dict[str, Any]]:
    """Columnar: each column is read once, and each distinct member value is encoded once per row
    dimension; a key is the JSON array of its members' encodings, so it equals
    ``CanonicalPathCodec.encode(path)`` exactly."""
    needed = {DEPTH, *rows, *measures}
    data = {c: _col_to_pylist(table[c]) for c in needed if c in table.column_names}
    return _records_from_dict(data, rows, measures)


_ENCODE_STR = json.encoder.encode_basestring
_NONE_JSON = '"{\\"type\\":\\"none\\",\\"value\\":null,\\"version\\":1}"'
_TRUE_JSON = '"{\\"type\\":\\"bool\\",\\"value\\":true,\\"version\\":1}"'
_FALSE_JSON = '"{\\"type\\":\\"bool\\",\\"value\\":false,\\"version\\":1}"'


def _records_from_dict(
    data: Dict[str, List[Any]],
    rows: Tuple[str, ...],
    measures: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    depths = data[DEPTH]
    dims = [data[k] for k in rows]
    values = [(m, data[m]) for m in measures]
    tokens: List[Dict[Any, str]] = [{} for _ in rows]
    tok = _token
    out: List[Dict[str, Any]] = []
    append = out.append
    r0 = rows[0] if rows else ""
    d0 = dims[0] if dims else None
    t0 = tokens[0] if tokens else None
    for i, raw_depth in enumerate(depths):
        depth = int(raw_depth)
        if depth == 1 and d0 is not None and t0 is not None:
            v0 = d0[i]
            append({
                "path": (v0,),
                "depth": 1,
                "key": "[" + tok(t0, v0) + "]",
                "values": {m: column[i] for m, column in values},
                "dims": {r0: v0},
            })
        elif depth == 0:
            append({
                "path": (),
                "depth": 0,
                "key": "[]",
                "values": {m: column[i] for m, column in values},
                "dims": {},
            })
        else:
            path = tuple(dims[j][i] for j in range(depth))
            append({
                "path": path,
                "depth": depth,
                "key": "[" + ",".join(tok(tokens[j], v) for j, v in enumerate(path)) + "]",
                "values": {m: column[i] for m, column in values},
                "dims": {rows[j]: path[j] for j in range(depth)},
            })
    return out


def _token(memo: Dict[Any, str], value: Any) -> str:
    """One member's element of a canonical path: the JSON string of its scalar encoding (memoized per column).

    A column holds one Python type, so the value keys the memo, except floats: ``0.0 == -0.0`` encode differently.
    """
    t = type(value)
    if value is None:
        return _NONE_JSON
    if t is str:
        token = memo.get(value)
        if token is None:
            token = _ENCODE_STR('{"type":"str","value":' + _ENCODE_STR(value) + ',"version":1}')
            memo[value] = token
        return token
    if t is int:
        token = memo.get(value)
        if token is None:
            token = f'"{{\\"type\\":\\"int\\",\\"value\\":\\"{value}\\",\\"version\\":1}}"'
            memo[value] = token
        return token
    if t is bool:
        return _TRUE_JSON if value else _FALSE_JSON
    key = ("float", repr(value)) if isinstance(value, float) else value
    token = memo.get(key)
    if token is None:
        token = _ENCODE_STR(CanonicalScalarCodec.encode(value))
        memo[key] = token
    return token
