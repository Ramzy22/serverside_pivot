"""DuckDB's native, direct-aggregate level tier.

Each table holds complete sibling blocks. Native parameter lists restrict both
missing-block inserts and visible reads; no Arrow relation is registered.
Explicit physical strategies continue to execute the reference paths.
"""

from __future__ import annotations

import hashlib
import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from ..authority.level_binding import SCHEMA_PREFIX, register, unregister
from ..authority.verifier import PlanVerificationError, verify_scoped
from ..cache import CacheKey, CacheManager, key_for
from ..frame import DEPTH
from ..ir import ColumnOrder, PivotQuery, PredicateGroup, Window
from ..logical import plan as logical_plan
from . import long_form as lf
from .parameters import BoundStatement, ParentPaths, parent_bindings, used_parameters

__all__ = ["BoundStatement", "LevelTableStore", "store_for"]

_ADDITIVE_AGGS = frozenset({"sum", "min", "max", "count"})


@dataclass
class Block:
    key: CacheKey
    name: str
    relation: Any
    insert: Optional[BoundStatement]
    source: Any
    meaning_query: PivotQuery
    depth: int
    include_total: bool
    insert_excluding: Optional[BoundStatement] = None
    parents: Set[Tuple[Any, ...]] = field(default_factory=set)
    complete: bool = False
    nbytes: int = 4096


@dataclass
class MemberKeyTable:
    key: CacheKey
    name: str
    relation: Any
    nbytes: int = 4096


def store_for(backend: Any) -> LevelTableStore:
    store = getattr(backend, "_pivot_level_store", None)
    if store is None:
        store = LevelTableStore(backend)
        object.__setattr__(backend, "_pivot_level_store", store)
    return store


def _filter_fields(pred: Any) -> Set[str]:
    if isinstance(pred, PredicateGroup):
        out: Set[str] = set()
        for item in pred.items:
            out |= _filter_fields(item)
        return out
    return {str(pred.field)}


def _proof_expr(tables: Dict[int, Any], mk_relations: Sequence[Any] = ()) -> Any:
    """Tiny Ibis expression referencing every level/member-key table so verify_scoped checks all bindings."""
    unique: List[Any] = []
    seen = set()
    for t in (*tables.values(), *mk_relations):
        if t.op() not in seen:
            seen.add(t.op())
            unique.append(t)
    first_col = unique[0].columns[0]
    out = unique[0].select(**{"__p": unique[0][first_col].cast("string")})
    for t in unique[1:]:
        col = t.columns[0]
        out = out.union(t.select(**{"__p": t[col].cast("string")}), distinct=False)
    return out


class LevelTableStore:
    def __init__(self, backend: Any, budget: int = 256 * 1024 * 1024) -> None:
        from .capabilities import level_tables

        if not level_tables(backend):
            raise ValueError("native level tables require a writable, local DuckDB connection")
        self.backend = backend
        options = getattr(backend, "_con_kwargs", {})
        args = getattr(backend, "_con_args", ())
        database = str(options.get("database", args[0] if args else ":memory:"))
        self.catalog = "memory" if database.startswith(":memory:") or database == ":default:" else Path(database).stem
        self.schema = SCHEMA_PREFIX + uuid.uuid4().hex
        self.lock = threading.RLock()
        self.blocks: Dict[CacheKey, Block] = {}
        self.mk_tables: Dict[CacheKey, MemberKeyTable] = {}
        self.revisions: Dict[str, int] = {}
        self.views: OrderedDict[Any, Tuple[Any, str, Any]] = OrderedDict()
        self.cache = CacheManager(budgets={"level": budget}, on_evict=self._drop)
        self._created = False

    def _drop(self, key: CacheKey, item: Any) -> None:
        from ..exec import materialize

        with self.lock:
            target = f'{lf.quote(self.catalog)}.{lf.quote(self.schema)}.{lf.quote(item.name)}'
            materialize(BoundStatement(item.relation, f'DROP TABLE IF EXISTS {target}',
                                       kind="drop", verified_capabilities=True))
            unregister(item.relation)
            self.blocks.pop(key, None)
            self.mk_tables.pop(key, None)
            self.views.clear()

    def invalidate(self, dataset: Optional[str] = None) -> None:
        with self.lock:
            for name in ([dataset] if dataset is not None else {k.dataset for k in (*self.blocks, *self.mk_tables)}):
                self.revisions[name] = self.revisions.get(name, 0) + 1
            self.cache.invalidate(dataset=dataset, tiers=("level",))

    def _meaning(self, query: PivotQuery) -> PivotQuery:
        return lf.meaning(query)

    def _table_ref(self, name: str) -> str:
        return f'{lf.quote(self.catalog)}.{lf.quote(self.schema)}.{lf.quote(name)}'

    def _ensure_schema(self, expr: Any) -> None:
        from ..exec import materialize

        if not self._created:
            materialize(BoundStatement(expr, f'CREATE SCHEMA {lf.quote(self.catalog)}.{lf.quote(self.schema)}',
                                       kind="create", verified_capabilities=True))
            self._created = True

    def distinct_values_for_field(self, dataset: str, source_op: Any, field: str) -> Optional[List[Dict[str, Any]]]:
        """Fast distinct `(field, LOWER(field))` from an already-complete unfiltered depth-1 block."""
        from ..exec import materialize

        with self.lock:
            rev = str(self.revisions.get(dataset, 0))
            for block in self.blocks.values():
                if (block.depth == 1 and block.complete and block.key.dataset == dataset
                        and block.key.revision == rev and block.source == source_op
                        and block.meaning_query.rows and block.meaning_query.rows[0] == field
                        and not block.meaning_query.filters and not block.meaning_query.measure_filters
                        and not block.meaning_query.categories and block.meaning_query.unpivot is None
                        and block.meaning_query.ragged is None):
                    scoped = True
                    try:
                        verify_scoped(block.relation)
                    except PlanVerificationError:
                        scoped = False
                    if not scoped:
                        continue
                    qfield = lf.quote(field)
                    target = self._table_ref(block.name)
                    sql = (
                        f'SELECT DISTINCT {qfield}, LOWER({qfield}) AS "__fold" '  # noqa: S608  # nosec B608
                        f'FROM {target} WHERE {lf.quote(DEPTH)} = 1 AND NOT {lf.quote(lf.CELL)}'
                    )
                    res = materialize(BoundStatement(block.relation, sql, kind="meta", verified_capabilities=True))
                    return list(res.to_pylist())
        return None

    def _find_unfiltered_lv1(self, query: PivotQuery, source_op: Any) -> Optional[Block]:
        """Locate a complete unfiltered depth-1 block matching `query` except for `filters`."""
        if (not query.filters or not query.rows or query.measure_filters or query.ragged is not None
                or query.categories or query.unpivot is not None):
            return None
        row0 = query.rows[0]
        if any(_filter_fields(p) != {row0} for p in query.filters):
            return None
        from ..exec import canonical

        base_meaning = replace(self._meaning(query), filters=())
        rev = str(self.revisions.get(query.dataset, 0))
        for include_total in (True, False):
            identity = hashlib.sha256(
                f"{canonical(base_meaning)}|{hash(source_op)}|1|{include_total}".encode()).hexdigest()
            key = key_for("level", query.dataset, identity, rev)
            candidate = self.blocks.get(key)
            if candidate is not None and candidate.complete and candidate.source == source_op:
                try:
                    verify_scoped(candidate.relation)
                except PlanVerificationError:
                    candidate = None
                if candidate is not None:
                    return candidate
        return None

    def _try_fill_from_unfiltered_lv1(self, block: Block, query: PivotQuery, relation: Any) -> bool:
        """Populate a filtered depth-1 block directly from an in-memory unfiltered depth-1 block (0 fact scans)."""
        from ..exec import materialize
        from .parameters import filters as build_filters
        from .parameters import parameter_sql

        base = self._find_unfiltered_lv1(query, relation.op())
        if base is None:
            return False
        q_filtered = self._meaning(query)
        params: Dict[str, Any] = {}
        where_sql = lf.direct_filter_sql(q_filtered.filters, base.relation.schema(), params)
        if where_sql is not None:
            base_ref = self._table_ref(base.name)
            extra_where = f' AND ({where_sql})' if where_sql else ''
            select_sql = f'SELECT * FROM {base_ref} WHERE {lf.quote(DEPTH)} = 1{extra_where}'  # noqa: S608  # nosec B608
        else:
            filtered_rel, params = build_filters(base.relation.filter(base.relation[DEPTH] == 1), q_filtered.filters)
            select_sql = parameter_sql(self.backend, filtered_rel)
        target = self._table_ref(block.name)
        proof = _proof_expr({1: base.relation, 2: block.relation})
        # Single-statement depth-1 filter + depth-0 rollup when all measures are strictly additive.
        if block.include_total and all(m.agg in _ADDITIVE_AGGS for m in q_filtered.measures):
            agg_map = {"sum": "SUM", "min": "MIN", "max": "MAX", "count": "SUM"}
            col_exprs = []
            for c in block.relation.columns:
                if c in q_filtered.rows:
                    col_exprs.append('NULL')
                elif c in q_filtered.columns:
                    col_exprs.append(lf.quote(c))
                elif c == DEPTH:
                    col_exprs.append('0::SMALLINT')
                elif c == lf.CELL:
                    col_exprs.append(lf.quote(lf.CELL))
                else:
                    m = next(x for x in q_filtered.measures if x.alias == c)
                    col_exprs.append(f'{agg_map[m.agg]}({lf.quote(c)})')
            gb = ', '.join([*(lf.quote(c) for c in q_filtered.columns), lf.quote(lf.CELL)])
            ins_sql = (
                f'INSERT INTO {target} WITH __m AS MATERIALIZED ({select_sql}) '  # noqa: S608  # nosec B608
                f'SELECT * FROM __m UNION ALL '
                f'SELECT {", ".join(col_exprs)} FROM __m GROUP BY {gb}'
            )
            ins_res = materialize(BoundStatement(
                proof, ins_sql, used_parameters(ins_sql, params), kind="insert", verified_capabilities=True,
            ))
            inserted = int(ins_res.column(0)[0].as_py()) if ins_res.num_rows else 0
            if inserted == 0:
                return False
            block.nbytes += max(64, inserted * max(64, len(block.relation.columns) * 16))
            block.complete = True
            return True

        ins_sql = f'INSERT INTO {target} {select_sql}'  # noqa: S608  # nosec B608
        ins_res = materialize(BoundStatement(
            proof,
            ins_sql,
            used_parameters(ins_sql, params),
            kind="insert",
            verified_capabilities=True,
        ))
        inserted = int(ins_res.column(0)[0].as_py()) if ins_res.num_rows else 0
        if block.include_total:
            if inserted == 0:
                matched_groups = 0
            elif not q_filtered.columns:
                matched_groups = inserted
            else:
                cnt_sql = f'SELECT COUNT(*) FROM {target} WHERE {lf.quote(DEPTH)} = 1 AND NOT {lf.quote(lf.CELL)}'  # noqa: S608  # nosec B608
                cnt_res = materialize(BoundStatement(
                    block.relation,
                    cnt_sql,
                    kind="meta",
                    verified_capabilities=True,
                ))
                matched_groups = int(cnt_res.column(0)[0].as_py()) if cnt_res.num_rows else 0
            del_sql = f'DELETE FROM {target}'  # noqa: S608  # nosec B608
            if matched_groups == 0:
                # Empty filtered result: fall back to facts so empty-group defaults match exactly.
                materialize(BoundStatement(block.relation, del_sql, kind="meta", verified_capabilities=True))
                return False
            if matched_groups == 1:
                # Single matching group: depth-0 matches depth-1 even for holistic measures.
                cols = [
                    'NULL' if c == q_filtered.rows[0] else (
                        '0::SMALLINT' if c == DEPTH else lf.quote(c)
                    )
                    for c in block.relation.columns
                ]
                gt_sql = (
                    f'INSERT INTO {target} SELECT {", ".join(cols)} FROM {target} '  # noqa: S608  # nosec B608
                    f'WHERE {lf.quote(DEPTH)} = 1'
                )
                gt_res = materialize(BoundStatement(block.relation, gt_sql, kind="insert", verified_capabilities=True))
                inserted += int(gt_res.column(0)[0].as_py()) if gt_res.num_rows else 0
            else:
                materialize(BoundStatement(block.relation, del_sql, kind="meta", verified_capabilities=True))
                return False
        block.nbytes += max(64, inserted * max(64, len(block.relation.columns) * 16))
        block.complete = True
        return True

    def _block(self, query: PivotQuery, relation: Any, depth: int, include_total: bool = True) -> Block:
        from ibis.backends.sql.datatypes import DuckDBType
        from ibis.expr.operations import DatabaseTable, Namespace

        from ..exec import canonical, materialize

        meaning = self._meaning(query)
        identity = hashlib.sha256(
            f"{canonical(meaning)}|{hash(relation.op())}|{depth}|{include_total}".encode()).hexdigest()
        key = key_for("level", query.dataset, identity, str(self.revisions.get(query.dataset, 0)))
        block = self.blocks.get(key)
        if block is not None and block.source == relation.op():
            self.cache.get(key)
            return block
        base_identity = hashlib.sha256(
            f"{canonical(meaning)}|{hash(relation.op())}|1|{include_total}".encode()).hexdigest()
        base = self.blocks.get(key_for("level", query.dataset, base_identity, key.revision)) if depth > 1 else None
        lowered = None
        if depth > 1 and base is not None:
            block_schema = base.relation.schema()
            proof = base.relation
        else:
            # Check if depth-1 filtered block can reuse an existing unfiltered lv1 schema without compiling!
            unfiltered_base = self._find_unfiltered_lv1(query, relation.op()) if depth == 1 else None
            if unfiltered_base is not None:
                block_schema = unfiltered_base.relation.schema()
                proof = relation
            else:
                lowered = lf.block(query, relation, depth, native=True, include_total=include_total)
                block_schema = lowered.schema
                proof = lowered.expr
        self._ensure_schema(proof)
        name = "l_" + uuid.uuid4().hex
        columns = ", ".join(f'{lf.quote(k)} {DuckDBType.from_ibis(t).sql(dialect="duckdb")}'
                            for k, t in block_schema.items())
        target = self._table_ref(name)
        materialize(BoundStatement(proof, f'CREATE TABLE {target} ({columns})', kind="create",
                                   verified_capabilities=True))
        table = DatabaseTable(name, block_schema, self.backend,
                              Namespace(catalog=self.catalog, database=self.schema)).to_expr()
        register(table, query.dataset)
        block = Block(key, name, table, None, relation.op(), meaning, depth, include_total)
        if lowered is not None:
            block.insert = self._insert(block, lowered)
        self.blocks[key] = block
        return block

    def _insert(self, block: Block, lowered: lf.LongBlock) -> BoundStatement:
        target = self._table_ref(block.name)
        sql = f'INSERT INTO {target} {lowered.sql}'  # noqa: S608
        return BoundStatement(lowered.expr, sql, lowered.params, kind="insert", verified_capabilities=True)

    def _fill(self, block: Block, parents: Optional[FrozenSet[Tuple[Any, ...]]], depth: int,
              query: PivotQuery, relation: Any) -> None:
        from ..exec import materialize

        if block.complete:
            return
        missing = None if parents is None else parents - block.parents
        if missing == frozenset():
            return
        if depth == 1 and missing is None and self._try_fill_from_unfiltered_lv1(block, query, relation):
            return
        need_exclude = depth > 1 and missing is None and bool(block.parents)
        if need_exclude:
            if block.insert_excluding is None:
                lowered = lf.block(query, relation, depth, native=True, parents=ParentPaths(depth, exclude=True),
                                   include_total=block.include_total)
                block.insert_excluding = self._insert(block, lowered)
            stmt = block.insert_excluding
        else:
            if block.insert is None:
                parent_spec = ParentPaths(depth, exclude=False) if depth > 1 else None
                lowered = lf.block(query, relation, depth, native=True, parents=parent_spec,
                                   include_total=block.include_total)
                block.insert = self._insert(block, lowered)
            stmt = block.insert
        raw_params: Dict[str, Any] = dict(stmt.params)
        if depth > 1:
            raw_params.update(parent_bindings("parents", depth, missing, depth - 1))
            if need_exclude:
                raw_params.update(parent_bindings("exclude", depth, block.parents, depth - 1))
        params = used_parameters(stmt.sql, raw_params)
        result = materialize(replace(stmt, params=params))
        inserted = int(result.column(0)[0].as_py()) if result.num_rows else 0
        block.nbytes += max(64, inserted * max(64, len(block.relation.columns) * 16))
        if missing is None:
            block.complete = True
        else:
            block.parents.update(missing)

    def _member_key_tables(self, query: PivotQuery, relation: Any) -> Tuple[Dict[Tuple[int, str], str], List[Any]]:
        """Lazily build and cache small `(keys..., __mk)` tables ONLY when `SortKey.by` is requested."""
        needed = sorted({(query.rows.index(s.field) + 1, s.by)
                         for s in query.sort if s.by and s.field in query.rows})
        if not needed:
            return {}, []
        from ibis.backends.sql.datatypes import DuckDBType
        from ibis.expr.operations import DatabaseTable, Namespace

        from ..exec import canonical, materialize
        from . import union_all as ua
        from .parameters import filters as build_filters
        from .parameters import parameter_sql

        meaning = self._meaning(query)
        rev = str(self.revisions.get(query.dataset, 0))
        prepared = ua.prepared(relation, meaning)
        e0, fparams = build_filters(prepared, meaning.filters)
        plan = logical_plan(meaning)
        eligible = ua._eligible(e0, plan)
        source = eligible if plan.values_from == "E" else e0

        out_refs: Dict[Tuple[int, str], str] = {}
        out_rels: List[Any] = []
        for depth, by in needed:
            identity = hashlib.sha256(
                f"mk:{canonical(meaning)}|{hash(relation.op())}|{depth}|{by}".encode()).hexdigest()
            key = key_for("level", query.dataset, identity, rev)
            mk = self.mk_tables.get(key)
            if mk is not None:
                self.cache.get(key)
                out_refs[(depth, by)] = self._table_ref(mk.name)
                out_rels.append(mk.relation)
                continue
            keys = list(query.rows[:depth])
            agg = source.group_by(keys).aggregate(**{"__mk": source[by].min()})
            verify_scoped(agg)
            agg_sql = parameter_sql(self.backend, agg)
            mk_schema = agg.schema()
            self._ensure_schema(agg)
            name = "mk_" + uuid.uuid4().hex
            columns = ", ".join(f'{lf.quote(k)} {DuckDBType.from_ibis(t).sql(dialect="duckdb")}'
                                for k, t in mk_schema.items())
            target = self._table_ref(name)
            materialize(BoundStatement(agg, f'CREATE TABLE {target} ({columns})', kind="create",
                                       verified_capabilities=True))
            res = materialize(BoundStatement(
                agg, f'INSERT INTO {target} {agg_sql}',  # noqa: S608
                used_parameters(agg_sql, fparams), kind="insert", verified_capabilities=True,
            ))
            inserted = int(res.column(0)[0].as_py()) if res.num_rows else 0
            table = DatabaseTable(name, mk_schema, self.backend,
                                  Namespace(catalog=self.catalog, database=self.schema)).to_expr()
            register(table, query.dataset)
            mk = MemberKeyTable(key, name, table, max(64, inserted * 64))
            self.mk_tables[key] = mk
            self.cache.put(key, mk)
            out_refs[(depth, by)] = target
            out_rels.append(table)
        return out_refs, out_rels

    def column_members_from_levels(
        self,
        query: PivotQuery,
        relation: Any,
        order: Sequence[ColumnOrder] = (),
    ) -> Optional[Tuple[Tuple[Any, ...], ...]]:
        """Populate (or reuse) depth-1 level block once and read ordered column members directly from it."""
        if (not query.columns or not query.rows or query.measure_filters or query.ragged is not None
                or query.unpivot is not None or query.categories):
            return None
        measure_aliases = {m.alias for m in query.measures}
        by_field = {o.field: o for o in order if o.field in query.columns}
        for o in by_field.values():
            if o.by is not None:
                return None
            if o.total is not None and (len(query.columns) > 1 or o.total not in measure_aliases):
                return None
        from ..exec import materialize
        from . import native_view

        with self.lock:
            block = self._block(query, relation, 1, include_total=True)
            try:
                self._fill(block, None, 1, query, relation)
                schema = block.relation.schema()
                quote = lf.quote
                target = self._table_ref(block.name)
                col_list = ', '.join(quote(c) for c in query.columns)
                order_terms: List[str] = []
                for c in query.columns:
                    col_order: Optional[ColumnOrder] = by_field.get(c)
                    if col_order is None:
                        order_terms.append(f'{quote(c)} ASC NULLS LAST')
                        continue
                    if col_order.total is not None:
                        val = quote(col_order.total)
                        if col_order.absolute and schema[col_order.total].is_numeric():
                            val = f'abs({val})'
                        direction = 'DESC' if col_order.desc else 'ASC'
                        nulls = 'NULLS FIRST' if col_order.nulls_first else 'NULLS LAST'
                        order_terms.append(f'{val} {direction} {nulls}')
                        order_terms.append(f'{quote(c)} ASC NULLS LAST')
                    elif col_order.semantic == 'tenor':
                        t_expr = native_view.tenor(c).replace('n.', '')
                        direction = 'DESC' if col_order.desc else 'ASC'
                        nulls = 'NULLS FIRST' if col_order.nulls_first else 'NULLS LAST'
                        order_terms.append(f'{t_expr} {direction} {nulls}')
                        order_terms.append(f'{quote(c)} ASC NULLS LAST')
                    else:
                        val = quote(c)
                        if col_order.absolute and schema[c].is_numeric():
                            val = f'abs({val})'
                        direction = 'DESC' if col_order.desc else 'ASC'
                        nulls = 'NULLS FIRST' if col_order.nulls_first else 'NULLS LAST'
                        order_terms.append(f'{val} {direction} {nulls}')
                sql = (
                    f'SELECT {col_list} FROM {target} '  # noqa: S608  # nosec B608
                    f'WHERE {quote(DEPTH)} = 0 AND {quote(lf.CELL)} '
                    f'ORDER BY {", ".join(order_terms)}'
                )
                res = materialize(BoundStatement(block.relation, sql, kind="meta", verified_capabilities=True))
                col_lists = [res.column(i).to_pylist() for i in range(len(query.columns))]
                return tuple(zip(*col_lists, strict=True)) if col_lists and col_lists[0] is not None else ()
            finally:
                if block.key in self.blocks:
                    self.cache.put(block.key, block)

    def read(self, query: PivotQuery, relation: Any, grand_total: bool, *, prune_total: bool = False) -> Any:
        from ..exec import canonical, materialize
        from ..frame import pivot_long_with_data
        from . import native_view

        with self.lock:
            prune_total = prune_total and bool(query.rows) and not any(
                d.func == "percent_of_total" for d in query.derived)
            full = logical_plan(replace(query, expanded=None, window=Window()))
            depths = [level.depth for level in full.levels if level.depth != 0 or
                      not (prune_total or any(x.depth == 1 for x in full.levels))]
            blocks = {d: self._block(query, relation, d, include_total=not prune_total) for d in depths}
            visible = {level.depth: level.visible_parents for level in logical_plan(query).levels}
            try:
                for d, block in blocks.items():
                    if d in visible:
                        self._fill(block, visible[d], d, query, relation)
                mk_refs, mk_rels = self._member_key_tables(query, relation)
                view_query = replace(query, expanded=None, window=Window(), column_members=(), sort=())
                sort_sig = native_view.sort_signature(query)
                view_key = (
                    canonical(view_query),
                    sort_sig,
                    tuple(b.name for b in blocks.values()),
                    tuple(sorted(mk_refs.items())),
                )
                cached = self.views.get(view_key)
                if cached is None:
                    tables = {d: b.relation for d, b in blocks.items()}
                    if 1 in tables:
                        tables[0] = tables[1]
                    if 0 not in tables:
                        tables[0] = next(iter(tables.values()))
                    proof = _proof_expr(tables, mk_rels)
                    verify_scoped(proof)
                    source_schema = None
                    if mk_refs:
                        from .union_all import prepared

                        source_schema = prepared(relation, query).schema()
                    table_schema = next(iter(tables.values())).schema()
                    sql = native_view.sql(
                        query,
                        tables,
                        synthetic_total=prune_total,
                        defaults=lf.empty_cells(query, relation),
                        member_key_tables=mk_refs,
                        source_schema=source_schema,
                    )
                    out_schema = native_view.output_schema(query, table_schema)
                    cached = (proof, sql, out_schema)
                    self.views[view_key] = cached
                    if len(self.views) > 256:
                        self.views.popitem(last=False)
                self.views.move_to_end(view_key)
                proof, sql, out_schema = cached
                raw_params = {**lf.bindings(query, depths), **native_view.sort_params(query)}
                params = used_parameters(sql, raw_params)
                result = materialize(BoundStatement(
                    proof, sql, params, kind="read", schema=out_schema, verified_capabilities=True,
                ))
                return pivot_long_with_data(result, query, lf.empty_cells(query, relation))
            finally:
                # Tables stay pinned through the whole read; eviction drops
                # complete tables only after the statement relinquishes them.
                for block in blocks.values():
                    if block.key in self.blocks:
                        self.cache.put(block.key, block)
