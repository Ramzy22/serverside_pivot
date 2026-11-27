"""Direct long-form aggregates and their page read. Cells widen only at the Arrow edge.

Native engines use GROUPING SETS, including the depth-one grand total in the
same scan. Portable engines use direct aggregates at the same grains. No
measure is ever computed from another aggregate.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional, Sequence, Tuple

import ibis
import sqlglot as sg
from ibis.backends.sql.datatypes import DuckDBType
from ibis.expr.operations import DatabaseTable

from ..authority.verifier import verify_scoped
from ..frame import DEPTH, ORD
from ..ir import CASE_FOLDING_OPS, STRING_OPS, PivotQuery, PredicateGroup, Window
from ..logical import Level
from ..logical import plan as logical_plan
from . import union_all as ua
from .capabilities import check_capabilities, window_functions
from .parameters import BoundStatement, ParentPaths, filters, parameter_sql, parent_bindings, value

CELL = "__is_cell"

_DIRECT_CMP_OPS = frozenset({"=", "!=", ">", ">=", "<", "<="})


def _duck_type(t: Any) -> str:
    return str(DuckDBType.from_ibis(t).sql(dialect="duckdb"))


def direct_filter_sql(predicates: Sequence[Any], schema: Any, params: Dict[str, Any]) -> Optional[str]:
    """Emit DuckDB WHERE SQL with bound filter parameters directly (no Ibis/SQLGlot compile)."""
    if not predicates:
        return ""

    def emit(p: Any) -> Optional[str]:
        if isinstance(p, PredicateGroup):
            parts: List[str] = []
            for item in p.items:
                term = emit(item)
                if term is None:
                    return None
                parts.append(term)
            if not parts:
                return None
            joiner = " AND " if p.op == "and" else " OR "
            return "(" + joiner.join(parts) + ")"
        col = quote(p.field)
        col_t = schema[p.field]
        dt = _duck_type(col_t)
        if p.op in {"in", "not_in"}:
            name = f"filter_{len(params)}"
            params[name] = [v for v in p.value if v is not None]
            null_name = f"{name}_null"
            params[null_name] = any(v is None for v in p.value)
            inside = f'{col} IN (SELECT UNNEST(CAST(${name} AS {dt}[])))'  # noqa: S608
            if p.op == "in":
                return f'({inside} OR ({col} IS NULL AND ${null_name}))'
            return f'(({col} IS NOT NULL AND NOT COALESCE({inside}, FALSE)) OR ({col} IS NULL AND NOT ${null_name}))'
        if p.op in _DIRECT_CMP_OPS:
            if p.value is None:
                return "FALSE"
            fold = (not p.case_sensitive and p.op in CASE_FOLDING_OPS
                    and isinstance(p.value, str) and col_t.is_string())
            name = f"filter_{len(params)}"
            params[name] = p.value.lower() if fold else p.value
            lhs = f'LOWER({col})' if fold else col
            return f'({col} IS NOT NULL AND {lhs} {p.op} CAST(${name} AS {dt}))'
        if p.op == "between":
            if any(v is None for v in p.value):
                return "FALSE"
            name = f"filter_{len(params)}"
            params[name], params[f"{name}_hi"] = p.value
            return f'({col} IS NOT NULL AND {col} >= CAST(${name} AS {dt}) AND {col} <= CAST(${name}_hi AS {dt}))'
        if p.op in STRING_OPS:
            name = f"filter_{len(params)}"
            params[name] = p.value if p.case_sensitive else p.value.lower()
            text = col if p.case_sensitive else f'LOWER({col})'
            needle = f'CAST(${name} AS VARCHAR)'
            if p.op == "contains":
                return f'({col} IS NOT NULL AND CONTAINS({text}, {needle}))'
            if p.op == "not_contains":
                return f'({col} IS NOT NULL AND NOT CONTAINS({text}, {needle}))'
            if p.op == "starts_with":
                return f'({col} IS NOT NULL AND STARTS_WITH({text}, {needle}))'
            return f'({col} IS NOT NULL AND ENDS_WITH({text}, {needle}))'
        if p.op == "is_null":
            return f'({col} IS NULL)'
        if p.op == "is_not_null":
            return f'({col} IS NOT NULL)'
        if p.op == "is_empty":
            return f'(COALESCE(LENGTH(TRIM({col})), 0) = 0)' if col_t.is_string() else f'({col} IS NULL)'
        if p.op == "is_not_empty":
            return f'(COALESCE(LENGTH(TRIM({col})), 0) <> 0)' if col_t.is_string() else f'({col} IS NOT NULL)'
        return None

    clauses: List[str] = []
    for pred in predicates:
        clause = emit(pred)
        if clause is None:
            return None
        clauses.append(clause)
    return " AND ".join(clauses)


def _direct_agg_sql_and_type(m: Any, schema: Any) -> Optional[Tuple[str, Any]]:
    """Direct DuckDB SQL expression and exact Ibis output dtype for standard aggregations."""
    if m.agg == "count":
        sql = "COUNT(*)" if m.field is None else f"COUNT({quote(m.field)})"
        return sql, ibis.dtype("int64")
    if m.field is None:
        return None
    col = quote(m.field)
    col_t = schema[m.field]
    if m.agg == "sum":
        if col_t.is_boolean():
            return None
        if col_t.is_signed_integer():
            return f"SUM({col})", ibis.dtype("int64")
        if col_t.is_unsigned_integer():
            return f"SUM({col})", ibis.dtype("uint64")
        if col_t.is_floating():
            return f"SUM({col})", ibis.dtype("float64")
        if col_t.is_decimal() and col_t.scale is not None:
            return f"SUM({col})", ibis.dtype(f"decimal(38, {col_t.scale})")
        return None
    if m.agg == "avg":
        if col_t.is_boolean():
            return None
        dt = col_t.copy(nullable=True) if col_t.is_decimal() else ibis.dtype("float64")
        return f"AVG({col})", dt
    if m.agg == "min":
        return f"MIN({col})", col_t.copy(nullable=True)
    if m.agg == "max":
        return f"MAX({col})", col_t.copy(nullable=True)
    if m.agg == "count_distinct":
        return f"COUNT(DISTINCT {col}) FILTER (WHERE {col} IS NOT NULL)", ibis.dtype("int64")
    if m.agg == "median":
        return f"QUANTILE_CONT(CAST({col} AS DOUBLE), 0.5)", ibis.dtype("float64")
    if m.agg == "stddev":
        return f"STDDEV_SAMP(CAST({col} AS DOUBLE))", ibis.dtype("float64")
    if m.agg == "variance":
        return f"VARIANCE(CAST({col} AS DOUBLE))", ibis.dtype("float64")
    if m.agg == "percentile" and m.q is not None:
        return f"QUANTILE_CONT(CAST({col} AS DOUBLE), {float(m.q)})", ibis.dtype("float64")
    return None


def _try_direct_grouping_block(
    q: PivotQuery,
    relation: Any,
    depth: int,
    parents: Any,
    include_total: bool,
) -> Optional[LongBlock]:
    """Zero-compile DuckDB GROUPING SETS SQL for plain DatabaseTable queries."""
    op = relation.op()
    if (not isinstance(op, DatabaseTable) or q.categories or q.unpivot is not None
            or q.ragged is not None or q.measure_filters):
        return None
    if parents is not None and not isinstance(parents, ParentPaths):
        return None
    verify_scoped(relation)
    schema = relation.schema()
    agg_specs: List[Tuple[Any, str, Any]] = []
    for m in q.measures:
        spec = _direct_agg_sql_and_type(m, schema)
        if spec is None:
            return None
        agg_specs.append((m, spec[0], spec[1]))

    params: Dict[str, Any] = {}
    where_parts: List[str] = []
    if q.filters:
        f_sql = direct_filter_sql(q.filters, schema, params)
        if f_sql is None:
            return None
        if f_sql:
            where_parts.append(f_sql)
    if depth > 1 and isinstance(parents, ParentPaths):
        pkeys = q.rows[:depth - 1]
        if len(pkeys) == 1:
            pcol = quote(pkeys[0])
            pt = _duck_type(schema[pkeys[0]])
            keep = (f'($parents_{parents.depth} IS NULL OR '
                    f'{pcol} IN (SELECT UNNEST(CAST($parents_{parents.depth} AS {pt}[]))) OR '  # noqa: S608
                    f'($parents_{parents.depth}_null AND {pcol} IS NULL))')
            if parents.exclude:
                excl = (f'COALESCE({pcol} IN (SELECT UNNEST(CAST($exclude_{parents.depth} AS {pt}[]))), FALSE) '  # noqa: S608
                        f'OR ($exclude_{parents.depth}_null AND {pcol} IS NULL)')
                keep = f'({keep} AND NOT ({excl}))'
        else:
            ptyp = 'STRUCT(' + ', '.join(f'k{i} {_duck_type(schema[k])}' for i, k in enumerate(pkeys)) + ')'
            ppath = 'struct_pack(' + ', '.join(f'k{i} := {quote(k)}' for i, k in enumerate(pkeys)) + ')'
            keep = (f'($parents_{parents.depth} IS NULL OR '
                    f'{ppath} IN (SELECT UNNEST(CAST($parents_{parents.depth} AS {ptyp}[]))))')  # noqa: S608
            if parents.exclude:
                excl = f'COALESCE({ppath} IN (SELECT UNNEST(CAST($exclude_{parents.depth} AS {ptyp}[]))), FALSE)'  # noqa: S608
                keep = f'({keep} AND NOT ({excl}))'
        where_parts.append(keep)

    depths = [depth, 0] if depth == 1 and include_total else [depth]
    keys = list(q.rows[:depth])
    group_set = set(keys) | set(q.columns)
    sets = [(*q.rows[:d], *(q.columns if cell else ()))
            for d in depths for cell in ([False, True] if q.columns else [False])]
    gs_sql = ", ".join("(" + ", ".join(quote(k) for k in s) + ")" for s in sets)

    is_cell = f'(GROUPING({quote(q.columns[0])}) = 0)' if q.columns else 'FALSE'
    is_total = f'(GROUPING({quote(keys[0])}) = 1)' if len(depths) > 1 else 'FALSE'

    table_ref = ".".join(quote(n) for n in (op.namespace.catalog, op.namespace.database, op.name) if n is not None)
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    block_fields: Dict[str, Any] = {k: schema[k].copy(nullable=True) for k in (*q.rows, *q.columns)}
    for m, _agg_sql, agg_t in agg_specs:
        block_fields[m.alias] = agg_t.copy(nullable=True)
    block_fields[DEPTH] = ibis.dtype("int16")
    block_fields[CELL] = ibis.dtype("boolean")

    depth_expr = (
        f'CAST(CASE WHEN {is_total} THEN 0 ELSE {depth} END AS SMALLINT) AS {quote(DEPTH)}'
        if len(depths) > 1
        else f'CAST({depth} AS SMALLINT) AS {quote(DEPTH)}'
    )
    dim_selects = [
        quote(k) if k in group_set else f'CAST(NULL AS {_duck_type(schema[k])}) AS {quote(k)}'
        for k in (*q.rows, *q.columns)
    ]

    # For full-table / depth-1 scans with strictly additive measures and multiple grouping sets,
    # compute the finest grain ONCE over facts and roll up all grouping sets from that tiny CTE.
    finest = [*keys, *q.columns]
    additive = all(m.agg in {"sum", "min", "max", "count"} for m in q.measures)
    if depth <= 1 and len(sets) > 1 and finest and additive:
        rollup_fn = {"sum": "SUM", "min": "MIN", "max": "MAX"}
        cte_dims = ", ".join(quote(k) for k in finest)
        cte_aggs = ", ".join(f'{agg_sql} AS {quote(m.alias)}' for m, agg_sql, _ in agg_specs)
        rollup_aggs = [
            f'CAST(COALESCE(SUM({quote(m.alias)}), 0) AS BIGINT) AS {quote(m.alias)}'
            if m.agg == "count"
            else f'{rollup_fn[m.agg]}({quote(m.alias)}) AS {quote(m.alias)}'
            for m, _agg_sql, _ in agg_specs
        ]
        outer_select = ", ".join([*dim_selects, *rollup_aggs, depth_expr, f'{is_cell} AS {quote(CELL)}'])
        sql = (
            f'WITH __g AS (SELECT {cte_dims}, {cte_aggs} FROM {table_ref}{where_sql} GROUP BY {cte_dims}) '  # noqa: S608  # nosec B608
            f'SELECT {outer_select} FROM __g GROUP BY GROUPING SETS ({gs_sql})'
        )
        return LongBlock(relation, sql, depth, params, ibis.schema(block_fields))

    select_parts = [
        *dim_selects,
        *(f'{agg_sql} AS {quote(m.alias)}' for m, agg_sql, _ in agg_specs),
        depth_expr,
        f'{is_cell} AS {quote(CELL)}',
    ]
    sql = (
        f'SELECT {", ".join(select_parts)} FROM {table_ref}{where_sql} '  # noqa: S608  # nosec B608
        f'GROUP BY GROUPING SETS ({gs_sql})'
    )
    return LongBlock(relation, sql, depth, params, ibis.schema(block_fields))


def meaning(query: PivotQuery) -> PivotQuery:
    return replace(query, expanded=None, window=Window(), sort=(), column_members=(), derived=(), subtotals="before")


def plain(query: PivotQuery) -> PivotQuery:
    # Cell sorts retain their member, but the node skeleton never carries wide cells.
    return replace(query, column_members=())


def member_columns(schema: Any, query: PivotQuery) -> Dict[str, str]:
    # Only include member-key columns actually referenced by the query's sort entries.
    needed = {s.by for s in query.sort if s.by}
    if not needed:
        return {}
    return {f"__mk_{i}": c for i, (c, t) in enumerate(schema.items())
            if c in needed and not (t.is_nested() or t.is_binary())}


@dataclass
class LongBlock:
    expr: Any
    sql: str
    depth: int
    params: Dict[str, Any]
    schema: Any = None


def block(query: PivotQuery, relation: Any, depth: int, *, native: bool,
          parents: Any = None, include_total: bool = True) -> LongBlock:
    """One sibling block, plus (at depth 1) the grand-total rows and cells."""
    q = meaning(query)
    if native:
        from ..catalog import store_for

        q = store_for(relation._find_backend()).lower_filters(q, relation, allow_scan=False)
        direct = _try_direct_grouping_block(q, relation, depth, parents, include_total)
        if direct is not None:
            return direct
    plan = logical_plan(q)
    prepared = ua.prepared(relation, q)
    schema = prepared.schema()
    e0, params = filters(prepared, q.filters) if native else (ua.filtered(prepared, q), {})
    eligible = ua._eligible(e0, plan)
    source = eligible if plan.values_from == "E" else e0
    restriction = None if depth <= 1 or parents is None else (tuple(q.rows[:depth - 1]), parents)
    source = ua.restrict(source, restriction, schema)
    depths = [depth, 0] if depth == 1 and include_total else [depth]
    if native:
        verify_scoped(source)
        sql, block_schema = _grouping_sql_and_schema(source, schema, q, depths, eligible)
        return LongBlock(source, sql, depth, params, block_schema)
    extras = member_columns(schema, query)
    parts = []
    for d in depths:
        keys = list(q.rows[:d])
        for cell in ([False, True] if q.columns else [False]):
            group = [*keys, *(q.columns if cell else ())]
            values = {m.alias: ua._lower(m, source, plan, group) for m in q.measures}
            values.update({n: source[c].min() for n, c in extras.items()})
            agg = source.group_by(group).aggregate(**values) if group else source.aggregate(**values)
            if d and q.measure_filters and plan.values_from == "E0":
                agg = ua._semi_join_on_keys(agg, eligible, keys)
            if d and q.ragged is not None:
                agg = agg.filter(agg[keys[-1]].notnull())
            cols = {k: agg[k].cast(schema[k].copy(nullable=True)) if k in group
                    else ibis.null().cast(schema[k].copy(nullable=True)) for k in (*q.rows, *q.columns)}
            parts.append(agg.select(**cols, **{m.alias: agg[m.alias] for m in q.measures},
                                    **{n: agg[n] for n in extras},
                                    **{DEPTH: ibis.literal(d, type="int16"), CELL: ibis.literal(cell)}))
    expr = parts[0]
    for p in parts[1:]:
        expr = expr.union(p, distinct=False)
    verify_scoped(expr)
    return LongBlock(expr, "", depth, params, expr.schema())


def _grouping_sql_and_schema(source: Any, schema: Any, q: PivotQuery, depths: List[int],
                             eligible: Any) -> tuple[str, Any]:
    """Build native GROUPING SETS SQL and exact block Ibis schema in one pass, cached on AST."""
    from ..cache import key_for
    from ..exec import _cached, _remember

    plan = logical_plan(q)
    keys = list(q.rows[:depths[0]])
    group = [*keys, *q.columns]
    values = {}
    for d in depths:
        for cell in ([False, True] if q.columns else [False]):
            grain = [*q.rows[:d], *(q.columns if cell else ())]
            for i, m in enumerate(q.measures):
                values[f"__v_{d}_{int(cell)}_{i}"] = ua._lower(m, source, plan, grain)
    agg = source.group_by(group).aggregate(**values) if group else source.aggregate(**values)
    agg_schema = agg.schema()
    block_fields: Dict[str, Any] = {k: schema[k].copy(nullable=True) for k in (*q.rows, *q.columns)}
    for i, m in enumerate(q.measures):
        block_fields[m.alias] = agg_schema[f"__v_{depths[0]}_0_{i}"].copy(nullable=True)
    block_fields[DEPTH] = ibis.dtype("int16")
    block_fields[CELL] = ibis.dtype("boolean")
    block_schema = ibis.schema(block_fields)

    backend = source._find_backend()
    op = agg.op()
    cache_key = key_for(
        "plan",
        q.dataset,
        f"gsql:{backend.name}:{hash(op)}:{tuple(depths)}:{q.columns}:{bool(q.measure_filters)}:{q.ragged is not None}",
    )
    saved = _cached(cache_key, op)
    if saved is not None:
        return str(saved), block_schema

    check_capabilities(agg)
    tree = sg.parse_one(parameter_sql(backend, agg), read="duckdb")
    projections = {p.alias_or_name: p for p in tree.expressions}
    dims = {k: projections[k].this.copy() for k in group}
    sets = [[dims[k].copy() for k in (*q.rows[:d], *(q.columns if cell else ()))]
            for d in depths for cell in ([False, True] if q.columns else [False])]
    tree.set("group", sg.exp.Group(grouping_sets=[sg.exp.GroupingSets(
        expressions=[sg.exp.Tuple(expressions=g) for g in sets])]))
    is_cell = sg.exp.EQ(this=sg.exp.Grouping(expressions=[dims[q.columns[0]].copy()]),
                        expression=sg.exp.Literal.number(0)) if q.columns else sg.exp.false()
    is_total = sg.exp.EQ(this=sg.exp.Grouping(expressions=[dims[keys[0]].copy()]),
                         expression=sg.exp.Literal.number(1)) if len(depths) > 1 else sg.exp.false()

    def choose(cond: Any, a: Any, b: Any) -> Any:
        return a if a == b else sg.exp.Case(ifs=[sg.exp.If(this=cond.copy(), true=a)], default=b)

    out = []
    for k in (*q.rows, *q.columns):
        term = dims[k].copy() if k in dims else sg.exp.Cast(
            this=sg.exp.Null(), to=sg.exp.DataType.build(str(schema[k]), dialect="duckdb"))
        out.append(sg.exp.alias_(term, k, quoted=True))
    for i, m in enumerate(q.measures):
        def at(d: int, i: int = i) -> Any:
            node = projections[f"__v_{d}_0_{i}"].this.copy()
            return choose(is_cell, projections[f"__v_{d}_1_{i}"].this.copy(), node) if q.columns else node
        term = at(depths[0])
        if len(depths) > 1:
            term = choose(is_total, at(0), term)
        out.append(sg.exp.alias_(term, m.alias, quoted=True))
    out.append(sg.exp.alias_(sg.exp.Cast(this=choose(is_total, sg.exp.Literal.number(0),
                         sg.exp.Literal.number(depths[0])), to=sg.exp.DataType.build("SMALLINT")), DEPTH, quoted=True))
    out.append(sg.exp.alias_(is_cell, CELL, quoted=True))
    tree.set("expressions", out)
    sql = tree.sql(dialect="duckdb")
    # Values are over E0, visibility over E. Keep total rows even when E is empty.
    if q.measure_filters and plan.values_from == "E0":
        visible = parameter_sql(backend, eligible.select(*q.rows).distinct())
        clauses = []
        for d in depths:
            match = " AND ".join(f'b.{quote(k)} IS NOT DISTINCT FROM e.{quote(k)}' for k in q.rows[:d])
            exists = f' AND EXISTS (SELECT 1 FROM ({visible}) e WHERE {match})' if d else ''  # noqa: S608  # nosec B608
            clauses.append(f'(b.{quote(DEPTH)} = {d}{exists})')
        sql = f'SELECT * FROM ({sql}) b WHERE ' + ' OR '.join(clauses)  # noqa: S608  # nosec B608
    if q.ragged is not None:
        clauses = [f'({quote(DEPTH)} = {d}' +
                   (f' AND {quote(q.rows[d-1])} IS NOT NULL' if d else '') + ')' for d in depths]
        sql = f'SELECT * FROM ({sql}) b WHERE ' + ' OR '.join(clauses)  # noqa: S608  # nosec B608
    from ibis.expr.operations import InMemoryTable

    if not op.find(InMemoryTable):
        _remember(cache_key, op, sql)
    return str(sql), block_schema


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def empty_cells(query: PivotQuery, relation: Any) -> Dict[Any, Any]:
    """Exact empty-group values, evaluated once for formulas and per-member axes."""
    if not query.columns or not any(m.agg in {"formula", "member"} for m in query.measures):
        return {}
    from ..catalog import store_for
    from ..exec import canonical, materialize

    def compute() -> Dict[Any, Any]:
        q = meaning(query)
        prepared = ua.prepared(relation, q)
        plan = logical_plan(q)
        axis = q.unpivot
        labels = [None] if axis is None else [None, *[m.label for m in axis.members]]
        out = {}
        for i, label in enumerate(labels):
            source = prepared.filter(False)
            if label is not None and axis is not None:
                one = source.aggregate(__n=source.count())
                source = one.select(**{k: (ibis.literal(label).cast(t) if k == axis.label else ibis.null().cast(t))
                                       for k, t in prepared.schema().items()})
            cell = None if label is None or axis is None else tuple(
                label if k == axis.label else None for k in q.columns)
            values = {}
            names = {}
            for j, m in enumerate(q.measures):
                name = f"e{i}_{j}"
                values[name] = ua._lower(m, source, plan, (*q.rows, *q.columns),
                                        condition=ibis.literal(False), cell=cell)
                names[label, m.alias] = name
            expr = source.aggregate(**values)
            verify_scoped(expr)
            row = materialize(expr).to_pylist()[0]
            out.update({key: row[name] for key, name in names.items()})
        return out
    result: Dict[Any, Any] = store_for(relation._find_backend()).get(
        query, relation, f"empty:{canonical(meaning(query))}", compute)
    return result


def view(query: PivotQuery, relation: Any, blocks: Dict[int, Any], *, parameterized: bool,
         synthetic_total: bool = False) -> Any:
    """Order nodes using their stored values, page them, then attach only the page's cells."""
    q = plain(query)
    plan = logical_plan(q)
    schema = ua.prepared(relation, q).schema()
    extras = member_columns(schema, q)
    def selected(d: int, restriction: Any = None, cell: bool = False) -> Any:
        t = blocks[d]
        if d == 0 and synthetic_total:
            empty = t.filter(False)
            one = empty.aggregate(__n=empty.count())
            return one.select(**{k: ibis.null().cast(schema[k]) for k in q.rows},
                              **{m.alias: ibis.null().cast(t[m.alias].type()) for m in q.measures},
                              **{DEPTH: ibis.literal(0, type="int16"), CELL: ibis.literal(False)})
        t = t.filter((t[DEPTH] == d) & (t[CELL] == cell))
        return ua.restrict(t, restriction, schema)

    def aggregate(keys: Sequence[str], restriction: Any) -> Any:
        t = selected(len(keys), restriction)
        return t.select(*keys, *[m.alias for m in q.measures])

    def member_min(keys: Sequence[str], restriction: Any, by: str, name: str) -> Any:
        d = len(keys)
        if d in blocks:
            t = selected(d, restriction)
            col = next((n for n, c in extras.items() if c == by), by)
            return t.select(*keys, **{name: t[col]})
        # Flat mode can order a dimension by the MIN of a member key.
        # The lookup is an exact MIN; no ancestor measure is built.
        t = selected(max(blocks))
        col = next((n for n, c in extras.items() if c == by), by)
        return t.group_by(list(keys)).aggregate(**{name: t[col].min()})

    def cell_values(keys: Sequence[str], restriction: Any, entries: Sequence[Any]) -> Any:
        nodes = selected(len(keys), restriction)
        result = nodes.select(*keys)
        for n, s, member in entries:
            cells = selected(len(keys), restriction, True)
            cells = cells.filter(ua._member_condition(cells, schema, query.columns, tuple(member)))
            name = f"__cell{len(keys)}_{n}"
            lookup = cells.select(**{f"__lk{j}": cells[k] for j, k in enumerate(keys)},
                                  **{name: cells[s.field], "__found": ibis.literal(True)})
            joined = result.left_join(lookup, ua._lookup_predicates(result, lookup, keys, "__lk"))
            val = joined[name]
            measure = next(m for m in q.measures if m.alias == s.field)
            from ..registry import REGISTRY

            axis = q.unpivot
            label = member[q.columns.index(axis.label)] if axis is not None and axis.label in q.columns else None
            defaults = empty_cells(q, relation) if measure.agg in {"formula", "member"} else {}
            empty = (defaults.get((label, s.field), defaults.get((None, s.field)))
                     if measure.agg in {"formula", "member"}
                     else REGISTRY.get(measure.agg).empty)
            fallback = ibis.literal(empty, type=val.type())
            row_axis = axis is not None and axis.label in keys
            if row_axis:
                if axis is None:
                    raise RuntimeError("row_axis requires an active unpivot axis")
                for m in axis.members:
                    v = defaults.get((m.label, s.field), empty)
                    fallback = ibis.ifelse(joined[axis.label] == m.label, ibis.literal(v, type=val.type()), fallback)
            if empty is not None or row_axis:
                val = ibis.ifelse(joined["__found"].isnull(), fallback, val)
            result = joined.select(*[joined[k] for k in keys],
                                   *[joined[c] for c in result.columns if c not in keys], **{name: val})
        return result

    provider = ua.AggregateProvider(relation, schema, lambda keys, restriction: selected(len(keys), restriction),
                                    aggregate, member_min, cell_values)
    if parameterized:
        plan = replace(plan, levels=tuple(ParameterLevel(level.depth, level.keys, None) for level in plan.levels))
    union, order = ua.tree(plan, relation, None, lambda _source, _plan: provider)
    nodes = ua.ordered_tree(plan, relation, union, order)
    if parameterized:
        nodes = nodes.filter((nodes[ORD] == 0) | (nodes[DEPTH] == 0) |
                             ((nodes[ORD] >= value("offset", "int64")) &
                              (value("limit", "int64").isnull() |
                               (nodes[ORD] < value("offset", "int64") + value("limit", "int64")))))
    else:
        nodes = ua.windowed(nodes, q.window, grand_total=True)
    cells = None
    for d, t in blocks.items():
        if d == 0 and 1 in blocks and blocks[0] is blocks[1]:
            continue
        part = t.filter(t[CELL])
        cells = part if cells is None else cells.union(part, distinct=False)
    if not query.columns:
        return nodes
    if cells is None:
        raise RuntimeError("pivoted view requires cell blocks")
    if parameterized:
        cells = ParentPaths(0).restrict(cells, query.columns, schema)
    else:
        cells = cells.filter(ua._any([ua._member_condition(cells, schema, query.columns, m)
                                    for m in query.column_members]))
    rename = {f"__row{j}": cells[k] for j, k in enumerate(query.rows)}
    rename.update({"__cd": cells[DEPTH], CELL: cells[CELL]})
    rename.update({k: cells[k] for k in query.columns})
    rename.update({f"__value_{m.alias}": cells[m.alias] for m in query.measures})
    cells = cells.select(**rename)
    predicates = [nodes[DEPTH] == cells["__cd"], *ua._lookup_predicates(nodes, cells, query.rows, "__row")]
    joined = nodes.left_join(cells, predicates)
    result = joined.select(*[joined[c] for c in nodes.columns],
                           *[joined[c] for c in cells.columns if not c.startswith("__row") and c != "__cd"])
    return result.order_by(ORD) if ORD in nodes.columns else result


@dataclass(frozen=True)
class ParameterLevel(Level):
    def parent_restriction(self) -> Any:
        return None if self.depth <= 1 else (self.parent_keys, ParentPaths(self.depth))


def read(query: PivotQuery, relation: Any, grand_total: bool, *, prune_total: bool = False) -> Any:
    """The storage-free lowering for engines without native level tables."""
    from ..exec import materialize
    from ..frame import pivot_long
    from .capabilities import level_tables

    native = level_tables(relation._find_backend())
    prune_total = prune_total and bool(query.rows) and not any(d.func == "percent_of_total" for d in query.derived)
    if not window_functions(relation):
        # A non-window engine returns the long tree once. Paging and count
        # are derived from that same materialization at the Arrow edge.
        query = replace(query, window=Window())
    levels = logical_plan(query).levels
    blocks = {}
    for level in levels:
        if level.depth == 0 and (prune_total or any(x.depth == 1 for x in levels)):
            continue
        b = block(query, relation, level.depth, native=False, parents=level.visible_parents,
                  include_total=not prune_total)
        blocks[level.depth] = b.expr
        if level.depth == 1:
            blocks[0] = b.expr
    if 0 not in blocks:
        blocks[0] = next(iter(blocks.values()))
    expr = view(query, relation, blocks, parameterized=False, synthetic_total=prune_total)
    verify_scoped(expr)
    if native:
        result = materialize(BoundStatement(expr, parameter_sql(relation._find_backend(), expr)))
    else:
        result = materialize(expr)
    result = pivot_long(result, query, empty_cells(query, relation))
    if ORD not in result.column_names:
        import pyarrow as pa

        from ..frame import TOTAL

        result = result.append_column(ORD, pa.array(range(result.num_rows), type=pa.int64()))
        result = result.append_column(TOTAL, pa.array([result.num_rows] * result.num_rows, type=pa.int64()))
    return result


def bindings(query: PivotQuery, depths: Sequence[int]) -> Dict[str, Any]:
    visible = {level.depth: level.visible_parents for level in logical_plan(query).levels}
    params: Dict[str, Any] = {}
    for d in depths:
        if d > 1:
            params.update(parent_bindings("parents", d, visible.get(d, frozenset()), d - 1))
    params["offset"] = query.window.offset
    params["limit"] = query.window.limit
    if query.columns:
        params.update(parent_bindings("parents", 0, frozenset(query.column_members), len(query.columns)))
    return params
