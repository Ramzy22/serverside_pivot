"""Typed parent lists bound to native statements, never registered as relations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Sequence

import ibis
import sqlglot as sg


@ibis.udf.scalar.builtin(name="__pivot_bind")  # type: ignore[untyped-decorator]
def _bind(token: str) -> str:  # type: ignore[empty-body]
    """Compiler marker; replaced as an AST function, never by matching user literals."""


def value(name: str, dtype: Any) -> Any:
    return _bind(ibis.literal(name)).cast(dtype)


@dataclass(frozen=True)
class BoundStatement:
    """Native SQL with bound values and the verified expression proving its reads."""

    expr: Any
    sql: str
    params: Dict[str, Any] = field(default_factory=dict)
    kind: str = "read"
    schema: Any = None
    verified_capabilities: bool = False

    def to_pyarrow(self) -> Any:
        from ..authority.verifier import verify_scoped

        if self.kind != "drop":
            verify_scoped(self.expr)
        backend = self.expr._find_backend()
        backend._run_pre_execute_hooks(self.expr)
        cursor = backend.con.cursor()
        try:
            res = cursor.execute(self.sql, self.params or None)
            if hasattr(res, "to_arrow_table"):
                return res.to_arrow_table()
            return res.fetch_arrow_table()
        finally:
            cursor.close()


@dataclass(frozen=True)
class ParentPaths:
    depth: int
    exclude: bool = False

    def restrict(self, relation: Any, keys: Sequence[str], schema: Any) -> Any:
        if len(keys) == 1:
            col = relation[keys[0]]
            dtype = ibis.expr.datatypes.Array(schema[keys[0]].copy(nullable=True))
            parents = value(f"parents_{self.depth}", dtype)
            parents_null = value(f"parents_{self.depth}_null", "boolean")
            keep = (parents.isnull() | col.isin(parents.unnest()).fill_null(False)
                    | (parents_null & col.isnull()))
            if self.exclude:
                excl = value(f"exclude_{self.depth}", dtype)
                excl_null = value(f"exclude_{self.depth}_null", "boolean")
                keep = keep & ~(col.isin(excl.unnest()).fill_null(False) | (excl_null & col.isnull()))
            return relation.filter(keep)
        dtype = ibis.expr.datatypes.Array(ibis.expr.datatypes.Struct(
            {f"k{i}": schema[k].copy(nullable=True) for i, k in enumerate(keys)}))
        parents = value(f"parents_{self.depth}", dtype)
        member = ibis.struct({f"k{i}": relation[k] for i, k in enumerate(keys)})
        # DuckDB struct equality is NULL-safe, including NULLs inside a path.
        keep = parents.isnull() | member.isin(parents.unnest())
        if self.exclude:
            keep = keep & ~member.isin(value(f"exclude_{self.depth}", dtype).unnest())
        return relation.filter(keep)


def parameter_sql(backend: Any, expr: Any) -> str:
    """Compile once, replacing only our reserved literal AST nodes with placeholders."""
    from ibis.expr.operations import InMemoryTable

    from ..cache import key_for
    from ..exec import _cached, _remember, compiled_sql

    op = expr.op()
    key = key_for("plan", "", f"parameters:{backend.name}:{hash(op)}")
    saved = _cached(key, op)
    if saved is not None:
        return str(saved)
    tree = sg.parse_one(compiled_sql(backend, expr), read="duckdb")

    def bind(node: Any) -> Any:
        if isinstance(node, sg.exp.Anonymous) and node.name.lower() == "__pivot_bind":
            return sg.exp.Placeholder(this=node.expressions[0].this)
        return node

    sql = str(tree.transform(bind).sql(dialect="duckdb"))
    if not op.find(InMemoryTable):
        _remember(key, op, sql)
    return sql


def parent_values(paths: Any) -> Any:
    if paths is None:
        return None
    return [{f"k{i}": value for i, value in enumerate(path)} for path in sorted(paths, key=repr)]


def parent_bindings(prefix: str, depth: int, paths: Any, key_count: int) -> Dict[str, Any]:
    """Bind scalar array + null flag for single-key paths, struct array for multi-key paths."""
    if key_count == 1:
        if paths is None:
            return {f"{prefix}_{depth}": None, f"{prefix}_{depth}_null": False}
        non_null = sorted((p[0] for p in paths if p[0] is not None), key=repr)
        return {f"{prefix}_{depth}": non_null, f"{prefix}_{depth}_null": any(p[0] is None for p in paths)}
    return {f"{prefix}_{depth}": parent_values(paths)}


def used_parameters(sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """DuckDB refuses unused named arguments."""
    return {name: value for name, value in params.items() if f"${name}" in sql}


def filters(relation: Any, predicates: Sequence[Any]) -> tuple[Any, Dict[str, Any]]:
    """Bind filter values while keeping the IR's exact NULL and case rules."""
    from ..ir import STRING_OPS, PredicateGroup
    from .union_all import _CMP, _predicate

    params: Dict[str, Any] = {}
    def lower(p: Any) -> Any:
        if isinstance(p, PredicateGroup):
            terms = [lower(i) for i in p.items]
            out = terms[0]
            for term in terms[1:]:
                out = out & term if p.op == "and" else out | term
            return out
        col = relation[p.field]
        name = f"filter_{len(params)}"
        if p.op in {"in", "not_in"}:
            params[name] = [v for v in p.value if v is not None]
            values = value(name, ibis.expr.datatypes.Array(col.type().copy(nullable=True)))
            inside = col.isin(values.unnest()).fill_null(False)
            null_name = name + "_null"
            params[null_name] = any(v is None for v in p.value)
            null = value(null_name, "boolean")
            return ((inside | (col.isnull() & null)) if p.op == "in"
                    else ((~inside & col.notnull()) | (col.isnull() & ~null)))
        if p.op in _CMP and p.value is not None:
            fold = not p.case_sensitive and p.op in {"=", "!="} and isinstance(p.value, str) and col.type().is_string()
            params[name] = p.value.lower() if fold else p.value
            return col.notnull() & _CMP[p.op](col.lower() if fold else col, value(name, col.type()))
        if p.op == "between":
            if any(v is None for v in p.value):
                return ibis.literal(False)
            params[name], params[name + "_hi"] = p.value
            return col.notnull() & (col >= value(name, col.type())) & (col <= value(name + "_hi", col.type()))
        if p.op in STRING_OPS:
            params[name] = p.value if p.case_sensitive else p.value.lower()
            needle = value(name, "string")
            text = col if p.case_sensitive else col.lower()
            if p.op == "contains":
                hit = text.contains(needle)
            elif p.op == "not_contains":
                hit = ~text.contains(needle)
            elif p.op == "starts_with":
                hit = text.substr(0, needle.length()) == needle
            else:
                hit = (needle.length() == 0) | (text.right(needle.length()) == needle)
            return col.notnull() & hit.fill_null(False)
        return _predicate(relation, p)
    return relation.filter([lower(p) for p in predicates]) if predicates else relation, params
