/**
 * Client mode on the semantic core (M3).
 *
 * A client `LogicalPivotPlan` (from `ClientSqlPlanner.compile`) is mapped to
 * the same semantic IR the server bridge builds (`transport/core_bridge.py`),
 * emitted as DuckDB SQL (`duckdbSqlEmitter.ts`), executed on the DuckDB-Wasm
 * connection, and shaped into the client row format. Numbers therefore come
 * from DuckDB with the core's semantics, not from JavaScript aggregation.
 *
 * Anything that cannot be mapped exactly throws `EmitterUnsupported`; the
 * caller then serves the request through the legacy client path.
 */

import type { LogicalPivotPlan, NormalizedFieldFilter, NormalizedFilterClause, NormalizedValConfig } from '../client/types';
import {
    DEPTH, EmitterUnsupported, cellName, emitColumnMembersSql, emitPivotSql,
    type ColumnInfo, type TypeFamily,
} from './duckdbSqlEmitter';
import type { Filter, PivotQuery } from './semanticIr.generated';

export { EmitterUnsupported };

type Row = { [key: string]: unknown };

export interface QueryConnection {
    query: (sql: string) => Promise<unknown>;
}

const AGG: Record<string, string> = {
    sum: 'sum', avg: 'avg', mean: 'avg', min: 'min', max: 'max', count: 'count',
    count_distinct: 'count_distinct', distinct_count: 'count_distinct', median: 'median',
    stddev: 'stddev', variance: 'variance', percentile: 'percentile',
    weighted_avg: 'weighted_avg', wavg: 'weighted_avg', weighted_mean: 'weighted_avg',
    first: 'first', last: 'last', // REQ-AGG-008: need an order field
    array_agg: 'array_agg', // REQ-AGG-009: ordered by its order field when given, else by value
};
const WINDOWS: Record<string, string> = {
    rank: 'rank', dense_rank: 'dense_rank', cumulative: 'cumulative', running_avg: 'moving_avg',
    moving_avg: 'moving_avg', percent_of_total: 'percent_of_total', percent_of_grand_total: 'percent_of_total',
};
const CLAUSE_OPS: Record<string, string> = {
    eq: '=', equals: '=', '=': '=', ne: '!=', not_eq: '!=', not_equals: '!=', '!=': '!=',
    gt: '>', '>': '>', gte: '>=', '>=': '>=', lt: '<', '<': '<', lte: '<=', '<=': '<=',
    in: 'in', set: 'in', not_in: 'not_in', notin: 'not_in', between: 'between',
    contains: 'contains', not_contains: 'not_contains', notcontains: 'not_contains',
    starts_with: 'starts_with', startswith: 'starts_with', ends_with: 'ends_with', endswith: 'ends_with',
    is_null: 'is_null', isnull: 'is_null', is_not_null: 'is_not_null', isnotnull: 'is_not_null',
};
const NULL_TOKEN = '__null__';

// ------------------------------------------------------------------ canonical identity (core/identity.py)

/** Python `repr(float)`: shortest round-trip digits, scientific below 1e-4 or from 1e16. */
export function pythonFloatRepr(x: number): string {
    if (!Number.isFinite(x)) throw new EmitterUnsupported('non-finite float in a path');
    if (x === 0) return Object.is(x, -0) ? '-0.0' : '0.0';
    const [mantissa, expText] = x.toExponential().split('e');
    const exp = Number(expText);
    const negative = mantissa.startsWith('-');
    const digits = mantissa.replace('-', '').replace('.', '');
    let body: string;
    if (exp >= -4 && exp < 16) {
        if (exp >= 0) {
            const intPart = digits.slice(0, exp + 1).padEnd(exp + 1, '0');
            const frac = digits.slice(exp + 1);
            body = `${intPart}.${frac || '0'}`;
        } else {
            body = `0.${'0'.repeat(-exp - 1)}${digits}`;
        }
    } else {
        const m = digits.length > 1 ? `${digits[0]}.${digits.slice(1)}` : digits;
        const e = Math.abs(exp) < 10 ? `0${Math.abs(exp)}` : String(Math.abs(exp));
        body = `${m}e${exp < 0 ? '-' : '+'}${e}`;
    }
    return negative ? `-${body}` : body;
}

export function canonicalScalar(value: unknown, family: TypeFamily): string {
    let type: string;
    let encoded: unknown;
    if (value === null || value === undefined) {
        type = 'none'; encoded = null;
    } else if (typeof value === 'boolean') {
        type = 'bool'; encoded = value;
    } else if (typeof value === 'bigint') {
        type = 'int'; encoded = value.toString();
    } else if (typeof value === 'number') {
        if (family === 'integer') { type = 'int'; encoded = String(Math.trunc(value)); } else { type = 'float'; encoded = pythonFloatRepr(value); }
    } else if (typeof value === 'string') {
        type = 'str'; encoded = value;
    } else {
        throw new EmitterUnsupported(`path value of type ${typeof value}`);
    }
    // json.dumps(sort_keys=True, separators=(",", ":"))
    return `{"type":${JSON.stringify(type)},"value":${JSON.stringify(encoded)},"version":1}`;
}

/** `CanonicalPathCodec.encode`: "[]" for the grand total. */
export function canonicalPath(values: unknown[], families: TypeFamily[]): string {
    return `[${values.map((v, i) => JSON.stringify(canonicalScalar(v, families[i]))).join(',')}]`;
}

// ------------------------------------------------------------------ plan -> IR

function typedValue(raw: unknown, column: ColumnInfo, what: string): unknown {
    if (raw === null || raw === undefined || raw === NULL_TOKEN) return null;
    if (column.family === 'integer' || column.family === 'floating' || column.family === 'decimal') {
        if (typeof raw === 'number') return raw;
        const n = typeof raw === 'string' && raw.trim() !== '' ? Number(raw) : NaN;
        if (!Number.isFinite(n)) throw new EmitterUnsupported(`${what}: non-numeric value for a numeric column`);
        if (column.family === 'integer' && !Number.isInteger(n)) throw new EmitterUnsupported(`${what}: fractional value for an integer column`);
        return n;
    }
    if (column.family === 'string') {
        if (typeof raw !== 'string') throw new EmitterUnsupported(`${what}: non-text value for a text column`);
        return raw;
    }
    if (column.family === 'boolean' && typeof raw === 'boolean') return raw;
    throw new EmitterUnsupported(`${what}: ${column.family} column`);
}

function clausePredicate(field: string, clause: NormalizedFilterClause, column: ColumnInfo): Filter {
    const type = String(clause.type || '').trim().toLowerCase();
    const caseSensitive = clause.caseSensitive !== false;
    if (type === 'empty' || type === 'blank' || type === 'not_empty' || type === 'not_blank') {
        if (column.family !== 'string') throw new EmitterUnsupported(`${type} on a ${column.family} column`);
        const blank: Filter = { op: 'or', conditions: [{ field, op: 'is_null' }, { field, op: '=', value: '' }] };
        return type.startsWith('not') ? { op: 'and', conditions: [{ field, op: 'is_not_null' }, { field, op: '!=', value: '' }] } : blank;
    }
    const op = CLAUSE_OPS[type];
    if (!op) throw new EmitterUnsupported(`filter clause ${type}`);
    if (op === 'is_null' || op === 'is_not_null') return { field, op };
    if (op === 'in' || op === 'not_in') {
        const values = (Array.isArray(clause.values) ? clause.values : Array.isArray(clause.value) ? clause.value : [clause.value])
            .map((v) => typedValue(v, column, `filter ${field}`));
        // Membership is exact whatever the case flag: case variants are distinct groups (REQ-NULL-002, F-CASE).
        return { field, op, value: values };
    }
    if (op === 'between') {
        const pair = Array.isArray(clause.values) ? clause.values : Array.isArray(clause.value) ? clause.value : [];
        if (pair.length !== 2) throw new EmitterUnsupported('between needs two values');
        return { field, op, value: pair.map((v) => typedValue(v, column, `filter ${field}`)) };
    }
    if (op === 'contains' || op === 'not_contains' || op === 'starts_with' || op === 'ends_with') {
        if (column.family !== 'string' || typeof clause.value !== 'string') throw new EmitterUnsupported(`${op} on a non-text value`);
        return { field, op, value: clause.value, caseSensitive };
    }
    const value = typedValue(clause.value, column, `filter ${field}`);
    // REQ-NULL-003: a comparison never matches NULL (`= NULL` included); the emitter has no FALSE
    // predicate, so the request is not served on the client rather than answered differently.
    if (value === null) throw new EmitterUnsupported(`comparison ${op} with NULL on ${field}`);
    return { field, op, value, ...(column.family === 'string' ? { caseSensitive } : {}) } as Filter;
}

function fieldFilter(f: NormalizedFieldFilter, columns: Record<string, ColumnInfo>): Filter {
    const column = columns[f.field];
    if (!column) throw new EmitterUnsupported(`filter on unknown column ${f.field}`);
    const conditions = f.clauses.map((c) => clausePredicate(f.field, c, column));
    if (conditions.length === 0) throw new EmitterUnsupported('empty filter');
    return conditions.length === 1 ? conditions[0] : ({ op: f.operator === "OR" ? "or" : "and", conditions } as Filter);
}

function measureRef(vc: NormalizedValConfig, alias: string, columns: Record<string, ColumnInfo>): PivotQuery['measures'][number] {
    const agg = AGG[String(vc.agg || '').toLowerCase()];
    if (!agg) throw new EmitterUnsupported(`aggregation ${vc.agg}`);
    if (!columns[vc.field]) throw new EmitterUnsupported(`measure on unknown column ${vc.field}`);
    const ref: PivotQuery['measures'][number] = { alias, agg, field: vc.field };
    if (agg === 'weighted_avg') {
        if (!vc.weightField || !columns[vc.weightField]) throw new EmitterUnsupported('weighted_avg without a known weight');
        ref.weight = vc.weightField;
    }
    if (agg === 'first' || agg === 'last') {
        if (!vc.orderBy || !columns[vc.orderBy]) throw new EmitterUnsupported(`${agg} without a known order field`);
        ref.orderBy = vc.orderBy;
    }
    if (agg === 'array_agg' && vc.orderBy) {
        if (!columns[vc.orderBy]) throw new EmitterUnsupported('array_agg over an unknown order field');
        ref.orderBy = vc.orderBy;
    }
    if (agg === 'percentile') {
        const q = vc.percentile === undefined ? 0.5 : Number(vc.percentile);
        if (!(q >= 0 && q <= 1)) throw new EmitterUnsupported('percentile outside [0, 1]');
        ref.q = q;
    }
    return ref;
}

function typedPath(parts: string[], rows: string[], columns: Record<string, ColumnInfo>): unknown[] {
    return parts.map((part, i) => {
        const column = columns[rows[i]];
        if (part === 'null') throw new EmitterUnsupported('ambiguous "null" label in an expanded path (send rowKey)');
        return typedValue(part, column, 'expanded path');
    });
}

export interface ClientCoreQuery {
    query: PivotQuery;
    families: TypeFamily[]; // row dimension families, for canonical keys
}

/** Map a client plan to the semantic IR (before column members are known). */
export function planToQuery(plan: LogicalPivotPlan, columns: Record<string, ColumnInfo>): ClientCoreQuery {
    if (plan.kind !== 'data') throw new EmitterUnsupported(`request kind ${plan.kind}`);
    if (plan.viewMode !== 'pivot') throw new EmitterUnsupported(`view mode ${plan.viewMode}`);
    if (plan.layoutMode === 'tabular') throw new EmitterUnsupported('tabular layout');
    if (plan.showSubtotalFooters) throw new EmitterUnsupported('subtotal footers');
    if (plan.groupAllowUnbalanced) throw new EmitterUnsupported('unbalanced grouping');
    if (plan.customDimensions.length) throw new EmitterUnsupported('custom dimensions');
    if (plan.measureAxis.placement === 'rows') throw new EmitterUnsupported('measures on rows');
    const so = plan.sortOptions || {};
    // naturalSort (the client default) is not honoured: ordering is binary and case-sensitive (REQ-BACK-002, F-CLIENT-COLLATION).
    if ((so.nulls && so.nulls !== 'last') || so.columnSortBy
        || Object.values(so.fieldOptions || {}).some((o) => o && (o.sortType || o.sortKeyField || o.absoluteSort))
        || Object.values(so.columnOptions || {}).some((o) => o && (o.sortType || o.sortKeyField || o.absoluteSort))) {
        throw new EmitterUnsupported('sort options');
    }
    plan.rowFields.forEach((f) => { if (!columns[f]) throw new EmitterUnsupported(`row field ${f}`); });
    plan.colFields.forEach((f) => { if (!columns[f]) throw new EmitterUnsupported(`column field ${f}`); });

    const measures: PivotQuery['measures'][number][] = [];
    const derived: NonNullable<PivotQuery['derived']> = [];
    for (const vc of plan.valConfigs) {
        if (vc.agg === 'formula' || vc.formula) throw new EmitterUnsupported('formula measures');
        const id = vc.id || `${vc.field}_${vc.agg}`;
        const windowFn = vc.windowFn ? WINDOWS[String(vc.windowFn).toLowerCase()] : undefined;
        if (vc.windowFn && !windowFn) throw new EmitterUnsupported(`window function ${vc.windowFn}`);
        if (windowFn) {
            if (plan.colFields.length) throw new EmitterUnsupported('window measures with column pivot');
            measures.push(measureRef(vc, `__base_${id}`, columns));
            derived.push({ alias: id, func: windowFn as NonNullable<PivotQuery['derived']>[number]['func'], of: `__base_${id}` });
        } else {
            measures.push(measureRef(vc, id, columns));
        }
    }
    if (!measures.length) throw new EmitterUnsupported('no measures');
    const aliases = new Set(measures.map((m) => m.alias));

    const sort: NonNullable<PivotQuery['sort']> = [];
    for (const rule of plan.sorting) {
        if (rule.sortType || rule.sortKeyField) throw new EmitterUnsupported('sort type');
        if (plan.rowFields.includes(rule.id) || aliases.has(rule.id)) {
            if (aliases.has(rule.id) && plan.colFields.length) throw new EmitterUnsupported('measure sort with column pivot');
            sort.push({ field: rule.id, desc: Boolean(rule.desc), ...(rule.absoluteSort ? { absolute: true } : {}) });
        } else {
            throw new EmitterUnsupported(`sort by ${rule.id}`);
        }
    }

    if (plan.expandedAmbiguous) throw new EmitterUnsupported('expanded path with an empty or NULL member (send rowKey)');
    let expanded: PivotQuery['expanded'];
    if (plan.expandedAll) {
        if (plan.collapsedPaths.length) throw new EmitterUnsupported('collapsed paths under expand-all');
        expanded = null;
    } else {
        expanded = plan.expandedPaths
            .filter((p) => p.length > 0 && p.length < plan.rowFields.length)
            .map((p) => typedPath(p, plan.rowFields, columns)) as PivotQuery['expanded'];
    }

    const query: PivotQuery = {
        dataset: plan.table,
        rows: [...plan.rowFields],
        measures: measures as PivotQuery['measures'],
        filters: plan.filters.map((f) => fieldFilter(f, columns)),
        sort,
        totals: 'visible',
        expanded,
        columns: [...plan.colFields],
        columnMembers: [],
        derived,
    };
    return { query, families: plan.rowFields.map((f) => columns[f].family) };
}

// ------------------------------------------------------------------ results -> client rows

function memberLabel(member: unknown[]): string {
    return member.map((v) => (v === null || v === undefined ? 'null' : String(v))).join('_');
}

function plain(value: unknown): unknown {
    if (typeof value === 'bigint') {
        const n = Number(value);
        if (!Number.isSafeInteger(n)) throw new EmitterUnsupported('integer result beyond 2^53');
        return n;
    }
    return value;
}

export interface ClientCoreResult {
    rows: Row[]; // the visible tree without the grand total, in order
    grandTotalRow: Row | null;
    colCombinations: string[];
}

function pathKey(parts: unknown[]): string {
    return parts.map((v) => (v === null || v === undefined ? '' : String(v))).join('|||');
}

/** Shape core records (rows dims, value columns, __depth) into the client row format. */
export function shapeRows(plan: LogicalPivotPlan, core: ClientCoreQuery, records: Row[]): ClientCoreResult {
    const { query, families } = core;
    const rows = query.rows;
    const members = query.columnMembers || [];
    const labels = members.map(memberLabel);
    if (new Set(labels).size !== labels.length) throw new EmitterUnsupported('ambiguous pivot column labels');
    const visibleIds = plan.valConfigs.map((vc) => vc.id || `${vc.field}_${vc.agg}`);
    const expandedSet = plan.expandedAll ? null : new Set(plan.expandedPaths.map((p) => p.join('|||')));
    const out: Row[] = [];
    let grandTotalRow: Row | null = null;
    for (const rec of records) {
        const depth = Number(plain(rec[DEPTH]));
        const path = rows.slice(0, depth).map((k) => plain(rec[k]));
        const values: Row = {};
        if (members.length) {
            members.forEach((_m, j) => query.measures.forEach((m) => {
                values[`${labels[j]}_${m.alias}`] = plain(rec[cellName(j, m.alias)]);
            }));
            query.measures.forEach((m) => { values[`__RowTotal__${m.alias}`] = plain(rec[m.alias]); });
        } else {
            visibleIds.forEach((id) => { values[id] = plain(rec[id]); });
        }
        if (depth === 0) {
            if (rows.length === 0) {
                out.push({ ...values, _id: 'All', _path: 'All', _rowKey: '[]', depth: 0, _has_children: false, _is_expanded: false });
            }
            grandTotalRow = {
                ...values, _id: 'Grand Total', _path: '__grand_total__', _rowKey: '[]', depth: 0,
                _isTotal: true, __isGrandTotal__: true, _has_children: false, _is_expanded: false,
                ...(rows.length ? { [rows[0]]: 'Grand Total' } : {}),
            };
            continue;
        }
        const display = pathKey(path);
        const hasChildren = depth < rows.length;
        const label = path[depth - 1];
        const row: Row = {
            ...values,
            _id: label === null || label === undefined ? 'null' : String(label),
            _path: display,
            _rowKey: canonicalPath(path, families),
            depth: depth - 1,
            _has_children: hasChildren,
            _is_expanded: hasChildren && (expandedSet === null || expandedSet.has(path.map((v) => String(v)).join('|||'))),
        };
        rows.slice(0, depth).forEach((k, i) => { row[k] = path[i]; });
        out.push(row);
    }
    if (!(plan.includeGrandTotal || plan.showGrandTotal)) grandTotalRow = null; // the core defines it even over an empty E (REQ-TOT-008)
    return { rows: out, grandTotalRow, colCombinations: labels };
}

// ------------------------------------------------------------------ execution

function toRecords(result: unknown): Row[] {
    const table = result as { toArray?: () => unknown[] };
    if (!table || typeof table.toArray !== 'function') throw new EmitterUnsupported('connection returned no Arrow table');
    return table.toArray().map((r) => {
        const row = r as { toJSON?: () => Row };
        return typeof row.toJSON === 'function' ? row.toJSON() : (r as Row);
    });
}

const FAMILY_PATTERNS: Array<[RegExp, TypeFamily]> = [
    [/^(VARCHAR|TEXT|STRING|CHAR|BPCHAR)/, 'string'],
    [/^(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT|INT)/, 'integer'],
    [/^(FLOAT|DOUBLE|REAL)/, 'floating'],
    [/^DECIMAL/, 'decimal'],
    [/^BOOL/, 'boolean'],
    [/^DATE$/, 'date'],
    [/^TIMESTAMP/, 'timestamp'],
];

export function familyOf(sqlType: string): TypeFamily {
    const t = sqlType.toUpperCase();
    const hit = FAMILY_PATTERNS.find(([re]) => re.test(t));
    return hit ? hit[1] : 'other';
}

export async function describeColumns(conn: QueryConnection, table: string): Promise<Record<string, ColumnInfo>> {
    const rows = toRecords(await conn.query(`DESCRIBE ${'"' + table.replace(/"/g, '""') + '"'}`));
    const out: Record<string, ColumnInfo> = {};
    for (const r of rows) {
        const sqlType = String(r.column_type);
        out[String(r.column_name)] = { sqlType, family: familyOf(sqlType) };
    }
    return out;
}

/** Serve a client data request from DuckDB with the core's semantics. */
export async function runClientCore(plan: LogicalPivotPlan, conn: QueryConnection, table: string): Promise<ClientCoreResult & { sql: string }> {
    const columns = await describeColumns(conn, table);
    const core = planToQuery(plan, columns);
    const options = { table, columns };
    if (core.query.columns && core.query.columns.length) {
        const members = toRecords(await conn.query(emitColumnMembersSql(core.query, options)));
        core.query.columnMembers = members.map((m) => (core.query.columns || []).map((c) => plain(m[c])));
    }
    const sql = emitPivotSql(core.query, options);
    return { ...shapeRows(plan, core, toRecords(await conn.query(sql))), sql };
}
