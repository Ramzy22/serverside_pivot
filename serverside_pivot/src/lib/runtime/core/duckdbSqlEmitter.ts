/**
 * DuckDB SQL emitter for the semantic IR (client mode, M3).
 *
 * Emits, for one `PivotQuery` (the wire form in `semanticIr.generated.ts`,
 * generated from `core/spec/semantic-ir.schema.json`), the same flattened,
 * ordered, windowed tree as the Python core's reference strategy
 * (`pivot_engine/core/logical.py` + `core/physical/union_all.py`):
 *
 *   rows dims (NULL where rolled up), measures, derived measures, pivot cells, __depth
 *
 * Semantics are those of docs/semantics/SEMANTICS.md; the conformance test
 * `tests/core/test_ts_emitter_conformance.py` executes this SQL on DuckDB
 * (the engine DuckDB-Wasm runs) and requires it to equal the oracle.
 *
 * Everything the emitter cannot express throws `EmitterUnsupported`; it never
 * approximates.
 */

import type { Filter, PivotQuery } from './semanticIr.generated';

export type TypeFamily = 'string' | 'integer' | 'floating' | 'decimal' | 'boolean' | 'date' | 'timestamp' | 'other';

export interface ColumnInfo {
    family: TypeFamily;
    sqlType: string; // DuckDB type name, used to type rolled-up NULLs
}

export interface EmitOptions {
    table: string; // DuckDB relation holding the (already scoped) dataset
    columns: Record<string, ColumnInfo>;
}

export class EmitterUnsupported extends Error {
    public constructor(message: string) {
        super(`emitter does not support: ${message}`);
        this.name = 'EmitterUnsupported';
    }
}

export const DEPTH = '__depth';
const STRING_OPS = new Set(['contains', 'not_contains', 'starts_with', 'ends_with']);
const CASE_FOLDING_OPS = new Set(['=', '!=', ...STRING_OPS]);
const COMPARISONS = new Set(['=', '!=', '>', '>=', '<', '<=']);
const NUMERIC = new Set<TypeFamily>(['integer', 'floating', 'decimal']);

type Measure = PivotQuery['measures'][number];
type Derived = NonNullable<PivotQuery['derived']>[number];
type SortEntry = NonNullable<PivotQuery['sort']>[number];

export function quoteIdent(name: string): string {
    return `"${String(name).replace(/"/g, '""')}"`;
}

export function cellName(memberIndex: number, alias: string): string {
    return `__c${memberIndex}__${alias}`;
}

/** Output value columns: measures, derived measures, then cells (as `PivotQuery.value_columns`). */
export function valueColumns(query: PivotQuery): string[] {
    const measures = query.measures.map((m) => m.alias);
    const derived = (query.derived || []).map((d) => d.alias);
    const cells: string[] = [];
    (query.columnMembers || []).forEach((_member, j) => query.measures.forEach((m) => cells.push(cellName(j, m.alias))));
    return [...measures, ...derived, ...cells];
}

function aggregateColumns(query: PivotQuery): string[] {
    const cells: string[] = [];
    (query.columnMembers || []).forEach((_member, j) => query.measures.forEach((m) => cells.push(cellName(j, m.alias))));
    return [...query.measures.map((m) => m.alias), ...cells];
}

// ------------------------------------------------------------------ literals

function literal(value: unknown, column: ColumnInfo | undefined): string {
    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    if (typeof value === 'number') {
        if (!Number.isFinite(value)) throw new EmitterUnsupported(`non-finite number ${value}`);
        return column && column.family === 'decimal' ? `CAST('${value}' AS ${column.sqlType})` : String(value);
    }
    if (typeof value === 'bigint') return value.toString();
    if (typeof value === 'string') {
        const text = `'${value.replace(/'/g, "''")}'`;
        if (column && NUMERIC.has(column.family)) return `CAST(${text} AS ${column.sqlType})`;
        return text;
    }
    throw new EmitterUnsupported(`literal ${JSON.stringify(value)}`);
}

function typedLiteral(value: unknown, column: ColumnInfo): string {
    return value === null || value === undefined ? `CAST(NULL AS ${column.sqlType})` : `CAST(${literal(value, column)} AS ${column.sqlType})`;
}

// ------------------------------------------------------------------ predicates (REQ-NULL-003)

function predicate(f: Filter, columns: Record<string, ColumnInfo>): string {
    if ('conditions' in f) {
        const op = String(f.op).toLowerCase();
        if (op !== 'and' && op !== 'or') throw new EmitterUnsupported(`filter group operator ${f.op}`);
        const terms = (f.conditions || []).map((c) => predicate(c as Filter, columns));
        if (terms.length === 0) throw new EmitterUnsupported('empty filter group');
        return `(${terms.join(op === 'and' ? ' AND ' : ' OR ')})`;
    }
    const field = String(f.field);
    const column = columns[field];
    if (!column) throw new EmitterUnsupported(`unknown column ${field}`);
    const col = quoteIdent(field);
    const op = String(f.op);
    const caseSensitive = f.caseSensitive !== false;
    const value = f.value;
    if (STRING_OPS.has(op)) {
        if (typeof value !== 'string') throw new EmitterUnsupported(`${op} needs a string`);
        const text = caseSensitive ? col : `lower(${col})`;
        const needle = literal(caseSensitive ? value : value.toLowerCase(), undefined);
        const hit = {
            contains: `contains(${text}, ${needle})`,
            not_contains: `NOT contains(${text}, ${needle})`,
            starts_with: `starts_with(${text}, ${needle})`,
            ends_with: `ends_with(${text}, ${needle})`,
        }[op as 'contains'];
        return `(${col} IS NOT NULL AND COALESCE(${hit}, FALSE))`;
    }
    if (op === 'between') {
        const [low, high] = Array.isArray(value) ? value : [];
        if (low === null || low === undefined || high === null || high === undefined) return 'FALSE';
        return `(${col} IS NOT NULL AND ${col} >= ${literal(low, column)} AND ${col} <= ${literal(high, column)})`;
    }
    if (COMPARISONS.has(op)) {
        if (value === null || value === undefined) return 'FALSE';
        if (!caseSensitive && CASE_FOLDING_OPS.has(op) && typeof value === 'string' && column.family === 'string') {
            return `(${col} IS NOT NULL AND lower(${col}) ${op} ${literal(value.toLowerCase(), undefined)})`;
        }
        return `(${col} IS NOT NULL AND ${col} ${op} ${literal(value, column)})`;
    }
    if (op === 'in' || op === 'not_in') {
        const list = Array.isArray(value) ? value : [value];
        const values = list.filter((v) => v !== null && v !== undefined);
        const nullListed = values.length !== list.length;
        const sqlList = values.map((v) => literal(v, column)).join(', ');
        if (op === 'in') {
            const term = values.length ? `${col} IN (${sqlList})` : 'FALSE';
            return nullListed ? `(${term} OR ${col} IS NULL)` : `(${term})`;
        }
        if (!values.length) return nullListed ? `(${col} IS NOT NULL)` : 'TRUE';
        const outside = `COALESCE(${col} NOT IN (${sqlList}), FALSE)`;
        return nullListed ? `(${outside} AND ${col} IS NOT NULL)` : `(${outside} OR ${col} IS NULL)`;
    }
    if (op === 'is_empty' || op === 'is_not_empty') { // REQ-NULL-004: NULL or a text of spaces only
        const info = columns[f.field];
        const empty = info && info.family === 'string' ? `(${col} IS NULL OR trim(${col}) = '')` : `(${col} IS NULL)`;
        return op === 'is_empty' ? empty : `(NOT ${empty})`;
    }
    if (op === 'is_null') return `(${col} IS NULL)`;
    if (op === 'is_not_null') return `(${col} IS NOT NULL)`;
    throw new EmitterUnsupported(`operator ${op}`);
}

// ------------------------------------------------------------------ aggregation (registry lowerings)

function aggregate(m: Measure, condition: string | null, columns: Record<string, ColumnInfo>): string {
    const masked = (name: string) => (condition ? `CASE WHEN ${condition} THEN ${quoteIdent(name)} END` : quoteIdent(name));
    if (!m.field) {
        if (m.agg !== 'count') throw new EmitterUnsupported(`${m.agg} without a field`);
        return condition ? `COUNT(CASE WHEN ${condition} THEN 1 END)` : 'COUNT(*)';
    }
    const x = masked(m.field);
    const dbl = `CAST(${x} AS DOUBLE)`;
    switch (m.agg) {
        // Typed as Ibis types it: an integer sum is BIGINT (DuckDB would widen it to HUGEINT).
        case 'sum': return columns[m.field] && columns[m.field].family === 'integer' ? `CAST(SUM(${x}) AS BIGINT)` : `SUM(${x})`;
        case 'count': return `COUNT(${x})`;
        case 'avg': return `AVG(${x})`;
        case 'min': return `MIN(${x})`;
        case 'max': return `MAX(${x})`;
        case 'count_distinct': return `COUNT(DISTINCT ${x})`;
        case 'median': return `QUANTILE_CONT(${dbl}, 0.5)`;
        case 'stddev': return `STDDEV_SAMP(${dbl})`;
        case 'variance': return `VAR_SAMP(${dbl})`;
        case 'percentile':
            if (typeof m.q !== 'number') throw new EmitterUnsupported('percentile without q');
            return `QUANTILE_CONT(${dbl}, ${m.q})`;
        case 'weighted_avg': {
            if (!m.weight) throw new EmitterUnsupported('weighted_avg without a weight');
            const w = masked(m.weight);
            const valid = `(${x}) IS NOT NULL AND (${w}) IS NOT NULL`;
            const num = `SUM(CASE WHEN ${valid} THEN CAST(${x} AS DOUBLE) * CAST(${w} AS DOUBLE) END)`;
            const den = `SUM(CASE WHEN ${valid} THEN CAST(${w} AS DOUBLE) END)`;
            return `CASE WHEN COALESCE(${den}, 0) = 0 THEN NULL ELSE ${num} / ${den} END`;
        }
        case 'first':
        case 'last': { // REQ-AGG-008: the value at the extreme order; ties resolve to the extreme value
            if (!m.orderBy) throw new EmitterUnsupported(`${m.agg} without an order field`);
            const o = masked(m.orderBy);
            return `${m.agg.toUpperCase()}(${x} ORDER BY ${o} ASC, ${x} ASC) FILTER (WHERE (${x}) IS NOT NULL AND (${o}) IS NOT NULL)`;
        }
        case 'array_agg': { // REQ-AGG-009: a series ordered by the order field (ties by value), else by value
            if (!m.orderBy) return `ARRAY_AGG(${x} ORDER BY ${x} ASC) FILTER (WHERE (${x}) IS NOT NULL)`;
            const o = masked(m.orderBy);
            return `ARRAY_AGG(${x} ORDER BY ${o} ASC, ${x} ASC) FILTER (WHERE (${x}) IS NOT NULL AND (${o}) IS NOT NULL)`;
        }
        default:
            throw new EmitterUnsupported(`aggregation ${m.agg}`);
    }
}

function memberCondition(columnsDims: string[], member: unknown[], columns: Record<string, ColumnInfo>): string {
    return columnsDims
        .map((k, i) => (member[i] === null || member[i] === undefined
            ? `${quoteIdent(k)} IS NULL`
            : `${quoteIdent(k)} IS NOT DISTINCT FROM ${typedLiteral(member[i], columns[k])}`))
        .join(' AND ');
}

type FormulaExpr = { op: string; args: unknown[] };

/** REQ-AGG-010: a formula over the node's own aggregates; a NULL input or an undefined result is NULL. */
function formulaSql(query: PivotQuery, e: FormulaExpr, condition: string | null, columns: Record<string, ColumnInfo>,
                    keys: string[]): string {
    const sub = (x: unknown) => formulaSql(query, x as FormulaExpr, condition, columns, keys);
    const a = e.args;
    switch (e.op) {
        case 'null': return 'CAST(NULL AS DOUBLE)';
        case 'num': return `CAST(${Number(a[0])} AS DOUBLE)`;
        case 'ref': {
            const m = query.measures.find((x) => x.alias === a[0]);
            if (!m) throw new EmitterUnsupported(`formula over unknown measure ${String(a[0])}`);
            return `CAST(${levelAggregate(query, m as Measure, condition, columns, keys)} AS DOUBLE)`;
        }
        case 'round': return `ROUND(${sub(a[0])}, ${Number(a[1])})`;
        case 'pow': {
            const exponent = Number(a[1]);
            let x = sub(a[0]);
            if (!Number.isInteger(exponent)) x = `(CASE WHEN ${x} < 0 THEN NULL ELSE ${x} END)`;
            if (exponent < 0) x = `(CASE WHEN ${x} = 0 THEN NULL ELSE ${x} END)`;
            return `POWER(${x}, CAST(${exponent} AS DOUBLE))`;
        }
        case '+': case '-': case '*': return `(${sub(a[0])} ${e.op} ${sub(a[1])})`;
        case '/': return `(CASE WHEN ${sub(a[1])} = 0 THEN NULL ELSE ${sub(a[0])} / ${sub(a[1])} END)`;
        case 'neg': return `(-${sub(a[0])})`;
        case 'abs': return `ABS(${sub(a[0])})`;
        case 'ceil': return `CEIL(${sub(a[0])})`;
        case 'floor': return `FLOOR(${sub(a[0])})`;
        case 'trunc': return `TRUNC(${sub(a[0])})`;
        case 'min': case 'max': {
            const xs = a.map(sub);
            return `(CASE WHEN ${xs.map((x) => `${x} IS NULL`).join(' OR ')} THEN NULL ELSE ${e.op === 'min' ? 'LEAST' : 'GREATEST'}(${xs.join(', ')}) END)`;
        }
        default: throw new EmitterUnsupported(`formula operator ${e.op}`);
    }
}

/** One measure at a level grouped by `keys`; a per-member axis value is level-aware (REQ-DIM-003). */
function levelAggregate(query: PivotQuery, m: Measure, condition: string | null, columns: Record<string, ColumnInfo>,
                        keys: string[]): string {
    if (m.agg === 'formula') {
        const value = formulaSql(query, (m as unknown as { expr: FormulaExpr }).expr, condition, columns, keys);
        return `(CASE WHEN isnan(${value}) OR isinf(${value}) THEN NULL ELSE ${value} END)`;
    }
    if (m.agg !== 'member') return aggregate(m, condition, columns);
    const axis = query.unpivot;
    if (!axis) throw new EmitterUnsupported('member aggregation without a measure axis');
    const series = axis.members.some((member) => member.agg === 'array_agg');
    const type = series ? `${columns[axis.value].sqlType}[]` : columns[axis.value].sqlType;
    // Above the label: no value. An aggregate (not a literal), so a global aggregate keeps its one row.
    if (!keys.includes(axis.label)) return `MIN(CAST(NULL AS ${type}))`;
    const text: ColumnInfo = { family: 'string', sqlType: 'VARCHAR' };
    const whens = axis.members.map((member) => {
        const isMember = `${quoteIdent(axis.label)} = ${typedLiteral(member.label, text)}`;
        const own: Measure = { alias: member.label, agg: String(member.agg), field: member.field, weight: member.weight,
            q: member.q, orderBy: member.orderBy } as Measure;
        const cond = condition ? `(${isMember}) AND (${condition})` : isMember;
        return `WHEN BOOL_OR(${isMember}) THEN CAST(${aggregate(own, cond, columns)} AS ${type})`;
    });
    return `CASE ${whens.join(' ')} ELSE CAST(NULL AS ${type}) END`;
}

// REQ-SORT-005 (as core/physical/union_all.tenor_days): tenor, else ISO date, else month-year; else NULL.
const WS = '[\\t\\n\\f\\r ]*';
const TENOR = `^${WS}([0-9]+(?:\\.[0-9]+)?)${WS}([A-Za-z])${WS}$`;
const ISO = `^${WS}([0-9]{4})-([0-9]{2})-([0-9]{2})${WS}$`;
const MONTH_YEAR = `^${WS}([A-Za-z]+)[\\t\\n\\f\\r ]+([0-9]{4})${WS}$`;
const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];

function tenorDays(x: string): string {
    const g = (pattern: string, i: number) => `NULLIF(regexp_extract(${x}, '${pattern}', ${i}), '')`;
    const num = `CAST(${g(TENOR, 1)} AS DOUBLE)`;
    const unit = `lower(${g(TENOR, 2)})`;
    const factor = `CASE ${unit} WHEN 'd' THEN 1.0 WHEN 'w' THEN 7.0 WHEN 'm' THEN 30.4375 WHEN 'y' THEN 365.25 END`;
    const [y, m, d] = [1, 2, 3].map((i) => `CAST(${g(ISO, i)} AS DOUBLE)`);
    const iso = `((${y} - 1970.0) * 365.25 + (${m} - 1.0) * 30.4375 + ${d})`;
    const myYear = `CAST(${g(MONTH_YEAR, 2)} AS DOUBLE)`;
    const abbr = `substr(lower(${g(MONTH_YEAR, 1)}), 1, 3)`;
    const myMonth = `CASE ${abbr} ${MONTHS.map((name, i) => `WHEN '${name}' THEN ${i + 1}.0`).join(' ')} END`;
    const monthYear = `((${myYear} - 1970.0) * 365.25 + (${myMonth} - 1.0) * 30.4375)`;
    return `CAST(CASE WHEN ${num} IS NOT NULL AND (${factor}) IS NOT NULL THEN ${num} * (${factor}) `
        + `WHEN ${y} IS NOT NULL THEN ${iso} WHEN ${monthYear} IS NOT NULL THEN ${monthYear} END AS DOUBLE)`;
}

function measureSelects(query: PivotQuery, columns: Record<string, ColumnInfo>, aliases: Set<string> | undefined,
                        keys: string[]): string[] {
    const out = query.measures
        .filter((m) => !aliases || aliases.has(m.alias))
        .map((m) => `${levelAggregate(query, m, null, columns, keys)} AS ${quoteIdent(m.alias)}`);
    if (!aliases) {
        (query.columnMembers || []).forEach((member, j) => {
            const cond = memberCondition(query.columns || [], member, columns);
            query.measures.forEach((m) => out.push(
                `${levelAggregate(query, m, cond, columns, keys)} AS ${quoteIdent(cellName(j, m.alias))}`));
        });
    }
    return out;
}

// ------------------------------------------------------------------ logical plan (as core/logical.py)

interface Level { depth: number; keys: string[]; visibleParents: unknown[][] | null }

function pathKey(path: unknown[]): string {
    return JSON.stringify(path);
}

function levels(query: PivotQuery): Level[] {
    if (query.subtotals === 'none') { // REQ-LAYOUT-002: the grand total and the leaves; expansion does not apply
        return [...new Set([0, query.rows.length])].map((d) => ({ depth: d, keys: query.rows.slice(0, d), visibleParents: null }));
    }
    const expanded = query.expanded === undefined || query.expanded === null ? null : query.expanded.map((p) => [...p]);
    const expandedKeys = expanded ? new Set(expanded.map(pathKey)) : null;
    const out: Level[] = [];
    for (let d = 0; d <= query.rows.length; d++) {
        let visible: unknown[][] | null = null;
        if (expanded && d > 1) {
            visible = expanded.filter((p) => p.length === d - 1
                && Array.from({ length: d - 2 }, (_v, i) => i + 1).every((k) => expandedKeys!.has(pathKey(p.slice(0, k)))));
            if (visible.length === 0) continue; // a level with no expanded parent shows no node
        }
        out.push({ depth: d, keys: query.rows.slice(0, d), visibleParents: visible });
    }
    return out;
}

function nullSafeJoin(left: string, right: string, keys: string[], rightKeys?: string[]): string {
    return keys.map((k, i) => `${left}.${quoteIdent(k)} IS NOT DISTINCT FROM ${right}.${quoteIdent((rightKeys || keys)[i])}`).join(' AND ');
}

function frameBound(offset: number | null | undefined, isStart: boolean): string {
    if (offset === null || offset === undefined) return isStart ? 'UNBOUNDED PRECEDING' : 'UNBOUNDED FOLLOWING';
    if (offset === 0) return 'CURRENT ROW';
    return offset < 0 ? `${-offset} PRECEDING` : `${offset} FOLLOWING`;
}

// ------------------------------------------------------------------ the emitter

/** Distinct column members over E0 (scope ∧ row filters), each dimension ordered NULLS LAST (as `exec.column_members`). */
// ------------------------------------------------------------------ categories (REQ-DIM-001), measure axis (REQ-DIM-002)

interface CategorySource {
    ctes: string[];
    relation: string; // the relation holding the base columns plus every category column
    columns: Record<string, ColumnInfo>;
}

/** One CTE per category, in order (a rule may reference an earlier category). */
function withCategories(query: PivotQuery, options: EmitOptions): CategorySource {
    const columns: Record<string, ColumnInfo> = { ...options.columns };
    const ctes: string[] = [];
    let relation = quoteIdent(options.table);
    (query.categories || []).forEach((c, i) => {
        if (columns[c.name]) throw new EmitterUnsupported(`category ${c.name} collides with a column`);
        const labels = [...c.rules.map((r) => r.label), c.fallback];
        let info: ColumnInfo;
        if (labels.every((v) => typeof v === 'string')) info = { family: 'string', sqlType: 'VARCHAR' };
        else if (labels.every((v) => typeof v === 'number' && Number.isInteger(v))) info = { family: 'integer', sqlType: 'BIGINT' };
        else if (labels.every((v) => typeof v === 'number')) info = { family: 'floating', sqlType: 'DOUBLE' };
        else throw new EmitterUnsupported(`category ${c.name} mixes label types`);
        const whens = c.rules.map((r) => `WHEN ${predicate(r.when as Filter, columns)} THEN ${typedLiteral(r.label, info)}`);
        const expr = whens.length ? `CASE ${whens.join(' ')} ELSE ${typedLiteral(c.fallback, info)} END` : typedLiteral(c.fallback, info);
        const name = `__cat${i}`;
        ctes.push(`${name} AS (SELECT *, ${expr} AS ${quoteIdent(c.name)} FROM ${relation})`);
        relation = name;
        columns[c.name] = info;
    });
    const axis = query.unpivot;
    if (axis) { // REQ-DIM-002: the measure axis, one record per member, after the categories
        const names = [axis.label, axis.value, axis.key];
        if (new Set(names).size !== 3 || names.some((n) => !n || columns[n])) {
            throw new EmitterUnsupported('measure axis columns collide with a column');
        }
        axis.members.forEach((m) => { if (!columns[m.field]) throw new EmitterUnsupported(`unknown column ${m.field}`); });
        const families = new Set(axis.members.map((m) => columns[m.field].family));
        const numeric = [...families].every((f) => f === 'integer' || f === 'floating' || f === 'decimal');
        const valueInfo: ColumnInfo = numeric ? { family: 'floating', sqlType: 'DOUBLE' }
            : (families.size === 1 ? columns[axis.members[0].field] : { family: 'string', sqlType: 'VARCHAR' });
        const text: ColumnInfo = { family: 'string', sqlType: 'VARCHAR' };
        const branches = axis.members.map((m) => {
            const value = `CAST(${quoteIdent(m.field)} AS ${valueInfo.sqlType})`;
            const where = axis.dropNulls ? ` WHERE ${quoteIdent(m.field)} IS NOT NULL` : '';
            return `SELECT *, ${typedLiteral(m.label, text)} AS ${quoteIdent(axis.label)}, ${value} AS ${quoteIdent(axis.value)}, `
                + `CAST(${m.order} AS BIGINT) AS ${quoteIdent(axis.key)} FROM ${relation}${where}`;
        });
        ctes.push(`__axis AS (${branches.join(' UNION ALL ')})`);
        relation = '__axis';
        columns[axis.label] = text;
        columns[axis.value] = valueInfo;
        columns[axis.key] = { family: 'integer', sqlType: 'BIGINT' };
    }
    const ragged = query.ragged;
    if (ragged) { // REQ-DIM-005: per position the i-th present value, its field index, and the path length
        ragged.fields.forEach((f) => { if (!columns[f]) throw new EmitterUnsupported(`unknown column ${f}`); });
        const first = columns[ragged.fields[0]];
        const same = ragged.fields.every((f) => columns[f].sqlType === first.sqlType);
        const info: ColumnInfo = same ? first : first.family === 'integer' ? { family: 'integer', sqlType: 'BIGINT' }
            : first.family === 'floating' ? { family: 'floating', sqlType: 'DOUBLE' }
                : first.family === 'string' ? { family: 'string', sqlType: 'VARCHAR' } : (() => {
                    throw new EmitterUnsupported('ragged hierarchy fields of different types');
                })();
        const present = ragged.fields.map((f) => {
            const col = quoteIdent(f);
            const terms = [`${col} IS NOT NULL`];
            if (columns[f].family === 'floating') terms.push(`NOT isnan(${col})`);
            if (!ragged.missing) {
                if (columns[f].family === 'string') terms.push(`${col} <> ''`);
            } else if (ragged.missing.length) {
                terms.push(`${col} NOT IN (${ragged.missing.map((v) => typedLiteral(v, columns[f])).join(', ')})`);
            }
            return `COALESCE(${terms.join(' AND ')}, FALSE)`;
        });
        const counts = present.map((_p, j) => present.slice(0, j + 1).map((p) => `CAST(${p} AS BIGINT)`).join(' + '));
        const extra: string[] = [];
        ragged.fields.forEach((_f, k) => {
            const i = k + 1;
            const hits = ragged.fields.map((f, j) => `WHEN ${present[j]} AND ${counts[j]} = ${i}`);
            extra.push(`CASE ${hits.map((h, j) => `${h} THEN CAST(${quoteIdent(ragged.fields[j])} AS ${info.sqlType})`).join(' ')} END AS ${quoteIdent(`__rag${i}`)}`);
            extra.push(`CASE ${hits.map((h, j) => `${h} THEN CAST(${j} AS BIGINT)`).join(' ')} END AS ${quoteIdent(`__ragf${i}`)}`);
            columns[`__rag${i}`] = info;
            columns[`__ragf${i}`] = { family: 'integer', sqlType: 'BIGINT' };
        });
        extra.push(`${counts[counts.length - 1]} AS ${quoteIdent('__ragn')}`);
        columns.__ragn = { family: 'integer', sqlType: 'BIGINT' };
        ctes.push(`__ragged AS (SELECT *, ${extra.join(', ')} FROM ${relation})`);
        relation = '__ragged';
    }
    return { ctes, relation, columns };
}

export function emitColumnMembersSql(query: PivotQuery, options: EmitOptions): string {
    const dims = query.columns || [];
    if (!dims.length) throw new EmitterUnsupported('column members without column dimensions');
    const source = withCategories(query, options);
    dims.forEach((d) => { if (!source.columns[d]) throw new EmitterUnsupported(`unknown column ${d}`); });
    const where = (query.filters || []).map((f) => predicate(f as Filter, source.columns));
    const cols = dims.map(quoteIdent).join(', ');
    const order = dims.map((d) => `(${quoteIdent(d)} IS NULL) ASC, ${quoteIdent(d)} ASC`).join(', ');
    const prefix = source.ctes.length ? `WITH ${source.ctes.join(', ')} ` : '';
    return `${prefix}SELECT DISTINCT ${cols} FROM ${source.relation}${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY ${order}`;
}

export function emitPivotSql(query: PivotQuery, options: EmitOptions): string {
    const catSource = withCategories(query, options);
    const { columns } = catSource;
    const rows = query.rows;
    const need = (name: string) => {
        if (!columns[name]) throw new EmitterUnsupported(`unknown column ${name}`);
    };
    rows.forEach(need);
    (query.columns || []).forEach(need);
    const ctes: string[] = [...catSource.ctes];
    const where = (query.filters || []).map((f) => predicate(f as Filter, columns));
    ctes.push(`__e0 AS (SELECT * FROM ${catSource.relation}${where.length ? ` WHERE ${where.join(' AND ')}` : ''})`);

    // E: records whose group passes at every filtered level (REQ-TOT-001), groups evaluated on E0.
    const byLevel = new Map<number, NonNullable<PivotQuery['measureFilters']>>();
    for (const mf of query.measureFilters || []) {
        byLevel.set(mf.level, [...(byLevel.get(mf.level) || []), mf]);
    }
    const semi: string[] = [];
    [...byLevel.keys()].sort((a, b) => a - b).forEach((level) => {
        const filters = byLevel.get(level)!;
        const keys = rows.slice(0, level);
        const aliases = new Set(filters.map((f) => f.measure));
        const conds = filters.map((f) => {
            if (!COMPARISONS.has(f.op)) throw new EmitterUnsupported(`measure filter ${f.op}`);
            if (f.value === null || f.value === undefined) return 'FALSE';
            return `${quoteIdent(f.measure)} ${f.op} ${literal(f.value, undefined)}`;
        });
        const name = `__mf${level}`;
        // REQ-DIM-005: in a ragged hierarchy only records with a node at this level are filtered by it.
        const last = quoteIdent(keys[keys.length - 1]);
        const groups = query.ragged ? `(SELECT * FROM __e0 WHERE ${last} IS NOT NULL)` : '__e0';
        ctes.push(`${name} AS (SELECT * FROM (SELECT ${keys.map(quoteIdent).join(', ')}, ${measureSelects(query, columns, aliases, keys).join(', ')} FROM ${groups} GROUP BY ${keys.map(quoteIdent).join(', ')}) WHERE ${conds.join(' AND ')})`);
        const exists = `EXISTS (SELECT 1 FROM ${name} g WHERE ${nullSafeJoin('e', 'g', keys)})`;
        semi.push(query.ragged ? `(e.${last} IS NULL OR ${exists})` : exists);
    });
    ctes.push(`__e AS (SELECT * FROM __e0 e${semi.length ? ` WHERE ${semi.join(' AND ')}` : ''})`);
    const source = query.totals === 'all_data' ? '__e0' : '__e';

    // One aggregate per visible level, typed UNION ALL.
    const value = aggregateColumns(query);
    const levelList = levels(query);
    const partNames: Record<number, string> = {};
    levelList.forEach((level) => {
        const keys = level.keys;
        const restrictions: string[] = [];
        if (query.ragged && keys.length) restrictions.push(`a.${quoteIdent(keys[keys.length - 1])} IS NOT NULL`); // REQ-DIM-005
        if (query.totals === 'all_data' && keys.length) {
            restrictions.push(`EXISTS (SELECT 1 FROM __e v WHERE ${nullSafeJoin('a', 'v', keys)})`);
        }
        if (level.visibleParents) {
            const parentKeys = keys.slice(0, -1);
            const tuples = level.visibleParents.map((p) => `(${p.map((v, i) => typedLiteral(v, columns[parentKeys[i]])).join(', ')})`);
            ctes.push(`__vp${level.depth} (${parentKeys.map((_k, i) => quoteIdent(`k${i}`)).join(', ')}) AS (VALUES ${tuples.join(', ')})`);
            restrictions.push(`EXISTS (SELECT 1 FROM __vp${level.depth} p WHERE ${parentKeys.map((k, i) => `a.${quoteIdent(k)} IS NOT DISTINCT FROM p.${quoteIdent(`k${i}`)}`).join(' AND ')})`);
        }
        const grouped = `SELECT ${[...keys.map(quoteIdent), ...measureSelects(query, columns, undefined, keys)].join(', ')} FROM ${source}${keys.length ? ` GROUP BY ${keys.map(quoteIdent).join(', ')}` : ''}`;
        const select = [
            ...rows.map((k) => (keys.includes(k) ? `a.${quoteIdent(k)}` : `CAST(NULL AS ${columns[k].sqlType}) AS ${quoteIdent(k)}`)),
            ...value.map((v) => `a.${quoteIdent(v)}`),
            `CAST(${level.depth} AS SMALLINT) AS ${DEPTH}`,
        ];
        const name = `__lvl${level.depth}`;
        partNames[level.depth] = name;
        ctes.push(`${name} AS (SELECT ${select.join(', ')} FROM (${grouped}) a${restrictions.length ? ` WHERE ${restrictions.join(' AND ')}` : ''})`);
    });
    const outCols = [...rows, ...value, DEPTH].map(quoteIdent).join(', ');
    ctes.push(`__tree AS (${levelList.map((l) => `SELECT ${outCols} FROM ${partNames[l.depth]}`).join(' UNION ALL ')})`);

    // Measures sorted on: attach each level-i ancestor's value (REQ-SORT-001).
    const sort: SortEntry[] = query.sort || [];
    const aliases = new Set(query.measures.map((m) => m.alias));
    const sortedMeasures = [...new Set(sort.filter((s) => !rows.includes(s.field) && aliases.has(s.field)).map((s) => s.field))];
    let tree = '__tree';
    if (sortedMeasures.length) {
        const joins: string[] = [];
        const extra: string[] = [];
        for (let i = 1; i <= rows.length; i++) {
            const part = partNames[i];
            sortedMeasures.forEach((alias) => {
                const name = quoteIdent(`__sort${i}_${alias}`);
                extra.push(part ? `s${i}.${quoteIdent(alias)} AS ${name}` : `NULL AS ${name}`);
            });
            if (part) joins.push(`LEFT JOIN ${part} s${i} ON ${nullSafeJoin('u', `s${i}`, rows.slice(0, i))}`);
        }
        ctes.push(`__sorted AS (SELECT u.*, ${extra.join(', ')} FROM __tree u ${joins.join(' ')})`);
        tree = '__sorted';
    }
    // REQ-SORT-004: a measure ordered within one column member gets the conditional value of each level-i ancestor.
    const cellSorts = sort.map((s, n) => ({ s, n })).filter(({ s }) => Array.isArray(s.member));
    if (cellSorts.length) {
        const joins: string[] = [];
        const extra: string[] = [];
        for (let i = 1; i <= rows.length; i++) {
            const keys = rows.slice(0, i).map(quoteIdent).join(', ');
            const selects = cellSorts.map(({ s, n }) => {
                const m = query.measures.find((x) => x.alias === s.field);
                if (!m) throw new EmitterUnsupported(`cell sort on unknown measure ${s.field}`);
                const name = quoteIdent(`__cell${i}_${n}`);
                extra.push(`c${i}.${name} AS ${name}`);
                const cond = memberCondition(query.columns || [], s.member as unknown[], columns);
                return `${levelAggregate(query, m as Measure, cond, columns, rows.slice(0, i))} AS ${name}`;
            });
            joins.push(`LEFT JOIN (SELECT ${keys}, ${selects.join(', ')} FROM ${source} GROUP BY ${keys}) c${i} ON ${nullSafeJoin('t', `c${i}`, rows.slice(0, i))}`);
        }
        ctes.push(`__cells AS (SELECT t.*, ${extra.join(', ')} FROM ${tree} t ${joins.join(' ')})`);
        tree = '__cells';
    }
    // REQ-SORT-005: a dimension ordered by a semantic key gets the key of its member value.
    const semanticKeys = sort.filter((s) => rows.includes(s.field) && typeof s.semantic === 'string');
    if (semanticKeys.length) {
        const extra = semanticKeys.map((s) => `${tenorDays(`t.${quoteIdent(s.field)}`)} AS ${quoteIdent(`__key${rows.indexOf(s.field) + 1}`)}`);
        ctes.push(`__semantic AS (SELECT t.*, ${extra.join(', ')} FROM ${tree} t)`);
        tree = '__semantic';
    }
    // REQ-SORT-003: a dimension ordered by a member key gets MIN(key) over each level-i ancestor's records.
    const memberKeys = sort.filter((s) => rows.includes(s.field) && typeof s.by === 'string');
    if (memberKeys.length) {
        const joins: string[] = [];
        const extra: string[] = [];
        memberKeys.forEach((s) => {
            const i = rows.indexOf(s.field) + 1;
            const keys = rows.slice(0, i).map(quoteIdent).join(', ');
            const name = quoteIdent(`__key${i}`);
            extra.push(`k${i}.${name} AS ${name}`);
            joins.push(`LEFT JOIN (SELECT ${keys}, MIN(${quoteIdent(s.by as string)}) AS ${name} FROM ${source} GROUP BY ${keys}) k${i} ON ${nullSafeJoin('t', `k${i}`, rows.slice(0, i))}`);
        });
        ctes.push(`__keyed AS (SELECT t.*, ${extra.join(', ')} FROM ${tree} t ${joins.join(' ')})`);
        tree = '__keyed';
    }
    const order: string[] = [];
    if (query.subtotals === 'none') { // REQ-LAYOUT-002: sort entries in precedence, then the row dimensions; grand total last
        const leaf = rows.length;
        order.push(`(CASE WHEN ${DEPTH} = 0 THEN 1 ELSE 0 END) ASC`);
        const ordered = new Set<string>();
        sort.forEach((s, n) => {
            let expr: string;
            if (!rows.includes(s.field) && Array.isArray(s.member)) expr = quoteIdent(`__cell${leaf}_${n}`);
            else if (!rows.includes(s.field)) expr = quoteIdent(`__sort${leaf}_${s.field}`);
            else if (typeof s.by === 'string' || typeof s.semantic === 'string') expr = quoteIdent(`__key${rows.indexOf(s.field) + 1}`);
            else { expr = quoteIdent(s.field); if (!s.absolute) ordered.add(s.field); }
            if (s.absolute) expr = `abs(${expr})`;
            order.push(`(${expr} IS NULL) ASC`, `${expr} ${s.desc ? 'DESC' : 'ASC'}`);
        });
        rows.forEach((k) => { if (!ordered.has(k)) order.push(`(${quoteIdent(k)} IS NULL) ASC`, `${quoteIdent(k)} ASC`); });
    }
    if (query.subtotals !== 'none') rows.forEach((k, idx) => {
        const i = idx + 1;
        // REQ-LAYOUT-001: the group row before (pre-order) or after (post-order) its descendants.
        order.push(`(CASE WHEN ${DEPTH} >= ${i} THEN 1 ELSE 0 END) ${query.subtotals === 'after' ? 'DESC' : 'ASC'}`);
        let dimensionSorted = false;
        sort.forEach((s, n) => {
            let expr: string;
            if (!rows.includes(s.field) && Array.isArray(s.member)) expr = quoteIdent(`__cell${i}_${n}`);
            else if (!rows.includes(s.field)) expr = quoteIdent(`__sort${i}_${s.field}`);
            else if (s.field === k && (typeof s.by === 'string' || typeof s.semantic === 'string')) expr = quoteIdent(`__key${i}`); // ties: member value
            else if (s.field === k) { expr = quoteIdent(k); dimensionSorted = true; } else return;
            if (s.absolute) expr = `abs(${expr})`;
            order.push(`(${expr} IS NULL) ASC`, `${expr} ${s.desc ? 'DESC' : 'ASC'}`);
        });
        if (!dimensionSorted) order.push(`(${quoteIdent(k)} IS NULL) ASC`, `${quoteIdent(k)} ASC`);
    });

    const derived: Derived[] = query.derived || [];
    const finalCols = [...rows, ...valueColumns(query), DEPTH].map(quoteIdent).join(', ');
    const window = query.window || {};
    const offset = Number(window.offset || 0);
    const limit = window.limit === null || window.limit === undefined ? null : Number(window.limit);
    const page = `${limit !== null ? ` LIMIT ${limit}` : ''}${offset || limit !== null ? ` OFFSET ${offset}` : ''}`;
    const orderSql = order.length ? order.join(', ') : '1';
    if (!derived.length) {
        return `WITH ${ctes.join(',\n')}\nSELECT ${finalCols} FROM ${tree} ORDER BY ${orderSql}${page}`;
    }

    // Derived measures over the visible tree (REQ-AGG-005/007).
    const parents = rows.map((k, j) => `CASE WHEN ${DEPTH} > ${j + 1} THEN ${quoteIdent(k)} ELSE NULL END AS ${quoteIdent(`__p${j}`)}`);
    ctes.push(`__ordered AS (SELECT *, ROW_NUMBER() OVER (ORDER BY ${orderSql}) AS __ord, ${[...parents, '0 AS __one'].join(', ')} FROM ${tree})`);
    const grand = [...new Set(derived.filter((d) => d.func === 'percent_of_total').map((d) => d.of))];
    let base = '__ordered o';
    if (grand.length) {
        ctes.push(`__gt AS (SELECT ${grand.map((a) => `${quoteIdent(a)} AS ${quoteIdent(`__gt_${a}`)}`).join(', ')} FROM __ordered WHERE ${DEPTH} = 0)`);
        base = '__ordered o CROSS JOIN __gt';
    }
    const partition = [DEPTH, ...rows.map((_k, j) => quoteIdent(`__p${j}`))].join(', ');
    const exprs = derived.map((d) => {
        const v = quoteIdent(d.of);
        switch (d.func) {
            case 'ratio': {
                const den = quoteIdent(String(d.of2));
                const fallback = d.nullValue === null || d.nullValue === undefined ? 'CAST(NULL AS DOUBLE)' : `CAST(${d.nullValue} AS DOUBLE)`;
                return `CASE WHEN ${den} IS NULL OR ${den} = 0 THEN ${fallback} ELSE CAST(${v} AS DOUBLE) / CAST(${den} AS DOUBLE) END`;
            }
            case 'percent_of_total': {
                const total = quoteIdent(`__gt_${d.of}`);
                return `CASE WHEN ${total} IS NULL OR ${total} = 0 THEN CAST(NULL AS DOUBLE) ELSE CAST(${v} AS DOUBLE) / CAST(${total} AS DOUBLE) END`;
            }
            case 'rank':
            case 'dense_rank': {
                const fn = d.func === 'rank' ? 'RANK' : 'DENSE_RANK';
                const dir = d.desc === false ? 'ASC' : 'DESC';
                return `CASE WHEN ${v} IS NULL THEN NULL ELSE CAST(${fn}() OVER (PARTITION BY ${partition} ORDER BY (${v} IS NULL) ASC, ${v} ${dir}) AS BIGINT) END`;
            }
            case 'cumulative':
            case 'moving_avg': {
                const [start, end] = d.frame ? [d.frame[0], d.frame[1]] : [null, 0];
                const frame = `ROWS BETWEEN ${frameBound(start as number | null, true)} AND ${frameBound(end as number | null, false)}`;
                const agg = d.func === 'cumulative' ? `SUM(${v})` : `AVG(CAST(${v} AS DOUBLE))`;
                return `${agg} OVER (PARTITION BY ${partition} ORDER BY __ord ${frame})`;
            }
            default:
                throw new EmitterUnsupported(`derived measure ${d.func}`);
        }
    });
    ctes.push(`__derived AS (SELECT o.*, ${derived.map((d, i) => `${exprs[i]} AS ${quoteIdent(d.alias)}`).join(', ')} FROM ${base})`);
    return `WITH ${ctes.join(',\n')}\nSELECT ${finalCols} FROM __derived ORDER BY __ord${page}`;
}
