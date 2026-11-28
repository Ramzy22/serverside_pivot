"""Specialized, lean DuckDB view SQL over native level tables.

Generates compact, query-shape-specialized SQL (<1.5 KB for default order,
<2.5 KB for sorted views) where visible parents, pagination bounds, column
windows, and cell-sort members are all bound parameters.  Zero unused regex
expressions, zero 29-field sort structs, and zero redundant CTE materializations.
"""
# SQL comes only from compiler types, quoted identifiers and bound parameters.
# ruff: noqa: S608
from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

import ibis
import pyarrow as pa
import sqlglot as sg

from ..frame import DEPTH, ORD, TOTAL
from ..ir import PivotQuery
from ..registry import REGISTRY
from . import long_form as lf
from . import union_all as ua


def dtype(t: Any) -> str:
    return lf.duck_type(t)


def path_type(keys: Sequence[str], schema: Any) -> str:
    return 'STRUCT(' + ', '.join(f'k{i} {dtype(schema[k])}' for i, k in enumerate(keys)) + ')'


def path(keys: Sequence[str], alias: str) -> str:
    return 'struct_pack(' + ', '.join(f'k{i} := {alias}.{lf.quote(k)}' for i, k in enumerate(keys)) + ')'


def literal(s: str) -> str:
    return str(sg.exp.Literal.string(s).sql(dialect="duckdb"))


@lru_cache(maxsize=128)
def tenor(field: str) -> str:
    t = ibis.table({field: "string"}, name="tenor_input")
    parsed = sg.parse_one(str(ibis.to_sql(t.select(v=ua.tenor_days(t[field])), dialect="duckdb")), read="duckdb")
    value = parsed.expressions[0].this.copy()
    for column in value.find_all(sg.exp.Column):
        column.set("table", sg.to_identifier("n"))
    return str(value.sql(dialect="duckdb"))


def sort_signature(query: PivotQuery) -> Tuple[Tuple[Any, ...], ...]:
    """Structural sort identity (cell-sort member values are bound as parameters)."""
    return tuple(
        (s.field, s.desc, s.absolute, s.by or "", s.semantic or "", s.nulls_first, s.member is not None)
        for s in query.sort
    )


def sort_params(query: PivotQuery) -> Dict[str, Any]:
    """Bound parameters for cell-sort member values."""
    params: Dict[str, Any] = {}
    for idx, s in enumerate(query.sort):
        if s.member is not None:
            for j, val in enumerate(s.member):
                params[f"sm{idx}_{j}"] = val
    return params


def _parent_filter_sql(keys: Sequence[str], alias: str, depth: int, schema: Any) -> str:
    if len(keys) == 1:
        col = f'{alias}.{lf.quote(keys[0])}'
        t = dtype(schema[keys[0]])
        return (f'($parents_{depth} IS NULL OR '
                f'list_contains(CAST($parents_{depth} AS {t}[]), {col}) OR '
                f'($parents_{depth}_null AND {col} IS NULL))')
    typ = path_type(keys, schema)
    return (f'($parents_{depth} IS NULL OR '
            f'list_contains(CAST($parents_{depth} AS {typ}[]), {path(keys, alias)}))')


def _column_window_sql(columns: Sequence[str], alias: str, schema: Any) -> str:
    if len(columns) == 1:
        col = f'{alias}.{lf.quote(columns[0])}'
        t = dtype(schema[columns[0]])
        return (f'(list_contains(CAST($parents_0 AS {t}[]), {col}) OR '
                f'($parents_0_null AND {col} IS NULL))')
    typ = path_type(columns, schema)
    return f'list_contains(CAST($parents_0 AS {typ}[]), {path(columns, alias)})'


def _order_term(expr_sql: str, *, desc: bool, absolute: bool, nulls_first: bool, is_numeric: bool) -> str:
    val = f'abs({expr_sql})' if (absolute and is_numeric) else expr_sql
    direction = 'DESC' if desc else 'ASC'
    nulls = 'NULLS FIRST' if nulls_first else 'NULLS LAST'
    return f'{val} {direction} {nulls}'


def _cell_fallback(m: Any, q: PivotQuery, defaults: Dict[Any, Any], idx: int, node_alias: str) -> str:
    if m.agg not in {'formula', 'member'}:
        empty = REGISTRY.get(m.agg).empty
        return 'NULL' if empty is None else str(sg.exp.convert(empty).sql(dialect='duckdb'))
    fallback = 'NULL'
    default = defaults.get((None, m.alias))
    if default is not None:
        fallback = str(sg.exp.convert(default).sql(dialect='duckdb'))
    axis = q.unpivot
    if axis is not None and axis.label in (*q.rows, *q.columns):
        key = (f'$sm{idx}_{q.columns.index(axis.label)}' if axis.label in q.columns
               else f'{node_alias}.{lf.quote(axis.label)}')
        cases = [f'WHEN {key} = {literal(str(label))} THEN {sg.exp.convert(v).sql(dialect="duckdb")}'
                 for (label, alias), v in defaults.items() if alias == m.alias and v is not None]
        if cases:
            fallback = 'CASE ' + ' '.join(cases) + ' ELSE NULL END'
    return fallback


def output_schema(query: PivotQuery, table_schema: Any) -> pa.Schema:
    """Exact PyArrow output schema matching the SELECT list of :func:`sql`."""
    fields: List[pa.Field] = []
    for k in query.rows:
        fields.append(pa.field(k, table_schema[k].to_pyarrow(), nullable=True))
    for m in query.measures:
        fields.append(pa.field(m.alias, table_schema[m.alias].to_pyarrow(), nullable=True))
    for d in query.derived:
        if d.func in {'rank', 'dense_rank'}:
            dt = pa.int64()
        elif d.func == 'cumulative':
            src = table_schema[d.of]
            dt = pa.int64() if src.is_integer() else (pa.float64() if src.is_floating() else src.to_pyarrow())
        else:
            dt = pa.float64()
        fields.append(pa.field(d.alias, dt, nullable=True))
    fields.append(pa.field(DEPTH, pa.int16(), nullable=True))
    fields.append(pa.field(ORD, pa.int64(), nullable=True))
    fields.append(pa.field(TOTAL, pa.int64(), nullable=True))
    if query.columns:
        fields.append(pa.field(lf.CELL, pa.bool_(), nullable=True))
        for k in query.columns:
            fields.append(pa.field(k, table_schema[k].to_pyarrow(), nullable=True))
        for m in query.measures:
            fields.append(pa.field(f'__value_{m.alias}', table_schema[m.alias].to_pyarrow(), nullable=True))
    return pa.schema(fields)


def sql(
    query: PivotQuery,
    tables: Dict[int, Any],
    *,
    synthetic_total: bool = False,
    defaults: Dict[Any, Any],
    member_key_tables: Optional[Dict[Tuple[int, str], str]] = None,
    source_schema: Any = None,
) -> str:
    """Build compact, query-shape-specialized DuckDB SQL with bound interaction parameters."""
    q = query
    schema = next(iter(tables.values())).schema()
    quote = lf.quote
    dims = [quote(k) for k in q.rows]
    aliases = [quote(m.alias) for m in q.measures]
    measures_by_alias = {m.alias: m for m in q.measures}
    mk_tables = member_key_tables or {}

    sources: List[str] = []
    seen = set()
    for depth, table in tables.items():
        if table.op() in seen:
            continue
        seen.add(table.op())
        op = table.op()
        name = '.'.join(quote(n) for n in (op.namespace.catalog, op.namespace.database, op.name) if n is not None)
        restriction = ''
        if depth > 1:
            restriction = ' WHERE ' + _parent_filter_sql(q.rows[:depth - 1], 'b', depth, schema)
        sources.append(f'SELECT * FROM {name} b{restriction}')  # nosec B608

    ctes: List[str] = [
        'b AS (' + ' UNION ALL '.join(sources) + ')'
    ]

    nodes_sql = f'SELECT * FROM b WHERE NOT {quote(lf.CELL)}'  # nosec B608
    if synthetic_total:
        empty = ', '.join(
            f'CAST(NULL AS {dtype(t)}) AS {quote(k)}' if k not in {DEPTH, lf.CELL}
            else f'{"0::SMALLINT" if k == DEPTH else "FALSE"} AS {quote(k)}'
            for k, t in schema.items()
        )
        nodes_sql += ' UNION ALL SELECT ' + empty

    # Analyze which joins are actually needed by query.sort
    has_measure_sort = any(s.field in measures_by_alias and s.member is None for s in q.sort)
    cell_sorts = [(idx, s) for idx, s in enumerate(q.sort) if s.field in measures_by_alias and s.member is not None]
    mk_sorts = [(idx, s) for idx, s in enumerate(q.sort) if s.by and s.field in q.rows]
    has_pct_total = any(d.func == 'percent_of_total' for d in q.derived)

    need_n_mat = (
        synthetic_total
        or (has_measure_sort and q.subtotals != 'none')
        or bool(cell_sorts)
        or bool(mk_sorts)
        or has_pct_total
    )
    if need_n_mat:
        ctes.append('n AS MATERIALIZED (' + nodes_sql + ')')
        node_from = 'n'
    else:
        node_from = f'({nodes_sql}) n'

    # Materialize ONLY the requested cell-sort slices (1 tiny CTE per cell sort, member value bound via $sm{idx}_{j})
    for idx, s in cell_sorts:
        if s.member is None:
            continue
        mem_match = ' AND '.join(
            f'b.{quote(col)} IS NOT DISTINCT FROM CAST($sm{idx}_{j} AS {dtype(schema[col])})'
            for j, col in enumerate(q.columns)
        )
        dim_cols = ', '.join(f'b.{k}' for k in dims)
        dim_prefix = f'{dim_cols}, ' if dim_cols else ''
        sc_cte = (
            f'sc{idx} AS MATERIALIZED (SELECT {dim_prefix}b.{quote(DEPTH)}, '  # nosec B608
            f'b.{quote(s.field)} AS __cv FROM b WHERE b.{quote(lf.CELL)} AND {mem_match})'
        )
        ctes.append(sc_cte)

    joins: List[str] = []
    order: List[str] = []

    if q.subtotals == 'none':
        # Flat layout (REQ-LAYOUT-002): leaves first, grand total last; zero ancestor joins!
        order.append(f'(n.{quote(DEPTH)} = 0) ASC')
        joined_mk: set[Tuple[int, str]] = set()
        for idx, s in enumerate(q.sort):
            if s.field in measures_by_alias and s.member is not None:
                m = measures_by_alias[s.field]
                cond = ' AND '.join([f'n.{quote(DEPTH)} = sc{idx}.{quote(DEPTH)}',
                                     *(f'n.{k} IS NOT DISTINCT FROM sc{idx}.{k}' for k in dims)])
                joins.append(f'LEFT JOIN sc{idx} ON {cond}')
                fb = _cell_fallback(m, q, defaults, idx, 'n')
                val = f'CASE WHEN sc{idx}.{quote(DEPTH)} IS NULL THEN {fb} ELSE sc{idx}.__cv END'
                order.append(_order_term(val, desc=s.desc, absolute=s.absolute,
                                         nulls_first=s.nulls_first, is_numeric=schema[s.field].is_numeric()))
            elif s.field in measures_by_alias:
                order.append(_order_term(f'n.{quote(s.field)}', desc=s.desc, absolute=s.absolute,
                                         nulls_first=s.nulls_first, is_numeric=schema[s.field].is_numeric()))
            elif s.by and s.field in q.rows:
                lvl = q.rows.index(s.field) + 1
                mk_key = (lvl, s.by)
                if mk_key in mk_tables:
                    alias_mk = f'mk_{lvl}_{s.by}'
                    if mk_key not in joined_mk:
                        joined_mk.add(mk_key)
                        cond = ' AND '.join(f'n.{k} IS NOT DISTINCT FROM {quote(alias_mk)}.{k}' for k in dims[:lvl])
                        joins.append(f'LEFT JOIN {mk_tables[mk_key]} {quote(alias_mk)} ON {cond}')
                    mk_num = bool(
                        source_schema is not None and s.by in source_schema and source_schema[s.by].is_numeric()
                    )
                    val = f'CASE WHEN n.{quote(DEPTH)} < {lvl} THEN NULL ELSE {quote(alias_mk)}.__mk END'
                    order.append(_order_term(val, desc=s.desc, absolute=s.absolute,
                                             nulls_first=s.nulls_first, is_numeric=mk_num))
            elif s.semantic == 'tenor' and s.field in q.rows:
                order.append(_order_term(tenor(s.field), desc=s.desc, absolute=False,
                                         nulls_first=s.nulls_first, is_numeric=True))
            elif s.field in q.rows:
                order.append(_order_term(f'n.{quote(s.field)}', desc=s.desc, absolute=s.absolute,
                                         nulls_first=s.nulls_first, is_numeric=schema[s.field].is_numeric()))
        plain_sorted = {s.field for s in q.sort
                        if s.field in q.rows and not s.absolute and not s.by and not s.semantic}
        for k in q.rows:
            if k not in plain_sorted:
                order.append(f'n.{quote(k)} ASC NULLS LAST')
    else:
        # Hierarchical pre-order / post-order (REQ-LAYOUT-001)
        direction_sub = 'DESC' if q.subtotals == 'after' else 'ASC'
        measure_sort_fields = sorted({s.field for s in q.sort if s.field in measures_by_alias and s.member is None})
        for i, k in enumerate(q.rows, start=1):
            order.append(f'(n.{quote(DEPTH)} >= {i}) {direction_sub}')
            am_joined = False
            joined_mk_lvl: set[str] = set()
            dim_sorted = False
            for idx, s in enumerate(q.sort):
                if s.field in measures_by_alias and s.member is not None:
                    m = measures_by_alias[s.field]
                    sc_alias = f'sc{idx}_{i}'
                    sub_cols = ', '.join([*dims[:i], '__cv', 'TRUE AS __hit'])
                    cond = ' AND '.join(f'n.{key} IS NOT DISTINCT FROM {sc_alias}.{key}' for key in dims[:i])
                    sc_join = (
                        f'LEFT JOIN (SELECT {sub_cols} FROM sc{idx} WHERE {quote(DEPTH)} = {i}) '  # nosec B608
                        f'{sc_alias} ON {cond}'
                    )
                    joins.append(sc_join)
                    fb = _cell_fallback(m, q, defaults, idx, 'n')
                    val = (f'CASE WHEN n.{quote(DEPTH)} < {i} THEN NULL '
                           f'WHEN {sc_alias}.__hit IS NULL THEN {fb} ELSE {sc_alias}.__cv END')
                    order.append(_order_term(val, desc=s.desc, absolute=s.absolute,
                                             nulls_first=s.nulls_first, is_numeric=schema[s.field].is_numeric()))
                elif s.field in measures_by_alias:
                    if not am_joined:
                        am_joined = True
                        sub_cols = ', '.join([*dims[:i], *(quote(f) for f in measure_sort_fields)])
                        cond = ' AND '.join(f'n.{key} IS NOT DISTINCT FROM am{i}.{key}' for key in dims[:i])
                        am_join = (
                            f'LEFT JOIN (SELECT {sub_cols} FROM n WHERE {quote(DEPTH)} = {i}) am{i} ON {cond}'  # nosec B608
                        )
                        joins.append(am_join)
                    val = f'CASE WHEN n.{quote(DEPTH)} < {i} THEN NULL ELSE am{i}.{quote(s.field)} END'
                    order.append(_order_term(val, desc=s.desc, absolute=s.absolute,
                                             nulls_first=s.nulls_first, is_numeric=schema[s.field].is_numeric()))
                elif s.field == k and s.by:
                    mk_key = (i, s.by)
                    if mk_key in mk_tables:
                        alias_mk = f'mk_{i}_{s.by}'
                        if s.by not in joined_mk_lvl:
                            joined_mk_lvl.add(s.by)
                            cond = ' AND '.join(
                                f'n.{key} IS NOT DISTINCT FROM {quote(alias_mk)}.{key}' for key in dims[:i]
                            )
                            joins.append(f'LEFT JOIN {mk_tables[mk_key]} {quote(alias_mk)} ON {cond}')
                        mk_num = bool(
                            source_schema is not None and s.by in source_schema and source_schema[s.by].is_numeric()
                        )
                        val = f'CASE WHEN n.{quote(DEPTH)} < {i} THEN NULL ELSE {quote(alias_mk)}.__mk END'
                        order.append(_order_term(val, desc=s.desc, absolute=s.absolute,
                                                 nulls_first=s.nulls_first, is_numeric=mk_num))
                elif s.field == k and s.semantic == 'tenor':
                    order.append(_order_term(tenor(k), desc=s.desc, absolute=False,
                                             nulls_first=s.nulls_first, is_numeric=True))
                elif s.field == k:
                    dim_sorted = True
                    order.append(_order_term(f'n.{quote(k)}', desc=s.desc, absolute=s.absolute,
                                             nulls_first=s.nulls_first, is_numeric=schema[k].is_numeric()))
            if not dim_sorted:
                order.append(f'n.{quote(k)} ASC NULLS LAST')

    join_sql = (' ' + ' '.join(joins)) if joins else ''
    order_sql = ('ORDER BY ' + ', '.join(order)) if order else ''
    node_cols = ', '.join(f'n.{c}' for c in [*dims, *aliases, quote(DEPTH)])

    # Fast Top-N path for pre-order views without sort-joins or derived window functions:
    # avoids full-table window sort + materializing cells before paging.
    if q.subtotals == 'before' and not joins and not q.derived:
        node_cols_b = ', '.join(f'b.{c}' for c in [*dims, *aliases, quote(DEPTH)])
        node_sources: List[str] = []
        seen_n = set()
        for depth, table in tables.items():
            if table.op() in seen_n:
                continue
            seen_n.add(table.op())
            op = table.op()
            name = '.'.join(quote(n) for n in (op.namespace.catalog, op.namespace.database, op.name) if n is not None)
            conds = [f'NOT b.{quote(lf.CELL)}']
            if depth > 1:
                conds.append(_parent_filter_sql(q.rows[:depth - 1], 'b', depth, schema))
            node_src = f'SELECT {node_cols_b} FROM {name} b WHERE ' + ' AND '.join(conds)  # nosec B608
            node_sources.append(node_src)
        if synthetic_total:
            empty_parts = [
                f'CAST(NULL AS {dtype(schema[k])}) AS {quote(k)}'
                for k in (*q.rows, *(m.alias for m in q.measures))
            ] + [f'0::SMALLINT AS {quote(DEPTH)}']
            node_sources.append('SELECT ' + ', '.join(empty_parts))
        tot_cte = f'tot AS (SELECT count(*) AS {quote(TOTAL)} FROM nodes)'  # nosec B608
        page_body_cte = (
            f'page_body AS (SELECT n.*, (row_number() OVER () - 1 + $offset)::BIGINT AS {quote(ORD)} '  # nosec B608
            f'FROM (SELECT * FROM nodes n {order_sql} '
            f'LIMIT COALESCE($limit, 9223372036854775807) OFFSET $offset) n)'
        )
        page_cte = (
            f'page AS ('  # nosec B608
            f'SELECT n.*, 0::BIGINT AS {quote(ORD)}, t.{quote(TOTAL)} FROM nodes n, tot t WHERE n.{quote(DEPTH)} = 0 '
            f'UNION ALL '
            f'SELECT pb.*, t.{quote(TOTAL)} FROM page_body pb, tot t WHERE pb.{quote(DEPTH)} <> 0)'
        )
        topn_ctes: List[str] = [
            'nodes AS MATERIALIZED (' + ' UNION ALL '.join(node_sources) + ')',
            tot_cte,
            page_body_cte,
            page_cte,
        ]
        topn_cols = [f'p.{k}' for k in [*dims, *aliases, quote(DEPTH), quote(ORD), quote(TOTAL)]]
        if q.columns:
            cell_cols_b = ', '.join(
                f'b.{c}' for c in [*dims, quote(DEPTH), quote(lf.CELL), *(quote(k) for k in q.columns), *aliases]
            )
            win = _column_window_sql(q.columns, 'b', schema)
            cell_sources: List[str] = []
            seen_c = set()
            for _depth, table in tables.items():
                if table.op() in seen_c:
                    continue
                seen_c.add(table.op())
                op = table.op()
                parts = (op.namespace.catalog, op.namespace.database, op.name)
                name = '.'.join(quote(n) for n in parts if n is not None)
                # `page` already enforces every parent restriction and LEFT JOIN matches exact (depth, *dims).
                cell_src = f'SELECT {cell_cols_b} FROM {name} b WHERE b.{quote(lf.CELL)} AND {win}'  # nosec B608
                cell_sources.append(cell_src)
            topn_ctes.append('cells AS (' + ' UNION ALL '.join(cell_sources) + ')')
            match = ' AND '.join([f'p.{quote(DEPTH)} = c.{quote(DEPTH)}',
                                  *(f'p.{k} IS NOT DISTINCT FROM c.{k}' for k in dims)])
            topn_cols += [f'c.{quote(lf.CELL)}', *(f'c.{quote(k)}' for k in q.columns),
                          *(f'c.{quote(m.alias)} AS {quote("__value_" + m.alias)}' for m in q.measures)]
            final = 'SELECT ' + ', '.join(topn_cols) + f' FROM page p LEFT JOIN cells c ON {match}'  # nosec B608
        else:
            final = 'SELECT ' + ', '.join(topn_cols) + ' FROM page p'  # nosec B608
        return 'WITH ' + ', '.join(topn_ctes) + ' ' + final + f' ORDER BY p.{quote(ORD)}'

    ordered_cte = (
        f'ordered AS (SELECT {node_cols}, '  # nosec B608
        f'row_number() OVER ({order_sql}) - 1 AS {quote(ORD)}, '
        f'count(*) OVER () AS {quote(TOTAL)} '
        f'FROM {node_from}{join_sql})'
    )
    ctes.append(ordered_cte)
    read_from = 'ordered'
    if q.derived:
        expressions = []
        partition = [f'o.{quote(DEPTH)}', *(f'CASE WHEN o.{quote(DEPTH)} > {i + 1} THEN o.{k} END'
                                            for i, k in enumerate(dims))]
        siblings = 'PARTITION BY ' + ', '.join(partition)

        def number(v: Any) -> str:
            return str(sg.exp.convert(v).sql(dialect='duckdb'))

        for d in q.derived:
            v = f'o.{quote(d.of)}'
            if d.func == 'ratio':
                if d.of2 is None:
                    raise RuntimeError("ratio derived measure requires denominator of2")
                den = f'o.{quote(d.of2)}'
                val = (f'CASE WHEN {den} IS NULL OR {den} = 0 THEN {number(d.null_value)} '
                       f'ELSE {v}::DOUBLE / {den}::DOUBLE END')
            elif d.func == 'percent_of_total':
                den = f'(SELECT {quote(d.of)} FROM n WHERE {quote(DEPTH)} = 0)'  # nosec B608
                val = f'CASE WHEN {den} IS NULL OR {den} = 0 THEN NULL ELSE {v}::DOUBLE / {den}::DOUBLE END'
            elif d.func in {'rank', 'dense_rank'}:
                direction = 'DESC' if d.desc else 'ASC'
                val = (f'CASE WHEN {v} IS NULL THEN NULL ELSE {d.func}() OVER '
                       f'({siblings} ORDER BY {v} {direction} NULLS LAST) END')
            else:
                lo, hi = d.frame if d.frame is not None else (None, 0)

                def bound(bv: Any, lower: bool) -> str:
                    if bv is None:
                        return 'UNBOUNDED PRECEDING' if lower else 'UNBOUNDED FOLLOWING'
                    return 'CURRENT ROW' if bv == 0 else f'{abs(bv)} {"PRECEDING" if bv < 0 else "FOLLOWING"}'

                aggregate = f'sum({v})' if d.func == 'cumulative' else f'avg({v}::DOUBLE)'
                val = (f'{aggregate} OVER ({siblings} ORDER BY o.{quote(ORD)} ROWS BETWEEN '
                       f'{bound(lo, True)} AND {bound(hi, False)})')
            expressions.append(f'{val} AS {quote(d.alias)}')
        derived_cte = 'derived AS (SELECT o.*, ' + ', '.join(expressions) + ' FROM ordered o)'  # nosec B608
        ctes.append(derived_cte)
        read_from = 'derived'

    page_cte_ord = (
        f'page AS (SELECT * FROM {read_from} WHERE {quote(ORD)} = 0 OR {quote(DEPTH)} = 0 OR '  # nosec B608
        f'({quote(ORD)} >= $offset AND ($limit IS NULL OR {quote(ORD)} < $offset + $limit)))'
    )
    ctes.append(page_cte_ord)
    cols = [f'p.{k}' for k in [*dims, *aliases, *(quote(d.alias) for d in q.derived),
                               quote(DEPTH), quote(ORD), quote(TOTAL)]]
    if q.columns:
        match = ' AND '.join([f'p.{quote(DEPTH)} = c.{quote(DEPTH)}',
                              *(f'p.{k} IS NOT DISTINCT FROM c.{k}' for k in dims)])
        window = _column_window_sql(q.columns, 'b', schema)
        cols += [f'c.{quote(lf.CELL)}', *(f'c.{quote(k)}' for k in q.columns),
                 *(f'c.{quote(m.alias)} AS {quote("__value_" + m.alias)}' for m in q.measures)]
        final = (
            'SELECT ' + ', '.join(cols) +
            f' FROM page p LEFT JOIN (SELECT * FROM b WHERE {quote(lf.CELL)} AND {window}) c ON {match}'  # nosec B608
        )
    else:
        final = 'SELECT ' + ', '.join(cols) + ' FROM page p'  # nosec B608
    return 'WITH ' + ', '.join(ctes) + ' ' + final + f' ORDER BY p.{quote(ORD)}'
