import type { RuntimeRequestV1 } from '../runtimeContract';
import type {
    LogicalPivotPlan,
    NormalizedCustomDimension,
    NormalizedFieldFilter,
    NormalizedFilterClause,
    NormalizedSortRule,
    NormalizedValConfig,
    PivotLayoutMode,
    PivotViewMode,
} from './types';

function quoteIdent(name: string): string {
    return `"${String(name || '').replace(/"/g, '""')}"`;
}

function quoteLiteral(value: unknown): string {
    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    return `'${String(value).replace(/'/g, "''")}'`;
}

function normalizeViewMode(value: unknown): PivotViewMode {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    if (normalized === 'tree' || normalized === 'report' || normalized === 'table') {
        return normalized;
    }
    return 'pivot';
}

function normalizeLayoutMode(value: unknown): PivotLayoutMode {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    if (normalized === 'outline' || normalized === 'tabular') {
        return normalized;
    }
    return 'hierarchy';
}

function normalizeStringArray(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const seen = new Set<string>();
    const result: string[] = [];
    for (const item of value) {
        if (typeof item === 'string' && item.trim() && !seen.has(item.trim())) {
            seen.add(item.trim());
            result.push(item.trim());
        }
    }
    return result;
}

function normalizeValConfigs(value: unknown): NormalizedValConfig[] {
    if (!Array.isArray(value)) return [];
    const result: NormalizedValConfig[] = [];
    for (const entry of value) {
        if (!entry || typeof entry !== 'object') continue;
        const item = entry as { [key: string]: unknown };
        const agg = typeof item.agg === 'string' && item.agg.trim()
            ? item.agg.trim().toLowerCase()
            : (typeof item.formula === 'string' && item.formula.trim() ? 'formula' : 'sum');
        const field = typeof item.field === 'string' && item.field.trim()
            ? item.field.trim()
            : (typeof item.id === 'string' && item.id.trim() ? item.id.trim() : `formula_${result.length + 1}`);
        const id = typeof item.id === 'string' && item.id.trim()
            ? item.id.trim()
            : `${field}_${agg}`;
        const label = typeof item.label === 'string' && item.label.trim()
            ? item.label.trim()
            : (agg === 'formula' ? field : `${field} (${agg})`);
        const windowFnRaw = typeof item.windowFn === 'string' && item.windowFn.trim()
            ? item.windowFn.trim()
            : (typeof item.showAs === 'string' && item.showAs.trim()
                ? item.showAs.trim()
                : (typeof item.show_as === 'string' && item.show_as.trim() ? item.show_as.trim() : undefined));
        result.push({
            field,
            agg,
            id,
            label,
            weightField: typeof item.weightField === 'string' ? item.weightField : undefined,
            windowFn: windowFnRaw,
            formula: typeof item.formula === 'string' ? item.formula : undefined,
            formulaScope: item.formulaScope === 'columns' ? 'columns' : 'measures',
            percentile: Number.isFinite(Number(item.percentile)) ? Number(item.percentile) : undefined,
            separator: typeof item.separator === 'string' ? item.separator : undefined,
            format: typeof item.format === 'string' ? item.format : undefined,
        });
    }
    return result;
}

function normalizeCustomDimensions(value: unknown): NormalizedCustomDimension[] {
    if (!Array.isArray(value)) return [];
    const result: NormalizedCustomDimension[] = [];
    for (const entry of value) {
        if (!entry || typeof entry !== 'object') continue;
        const raw = entry as { [key: string]: unknown };
        const id = typeof raw.id === 'string' && raw.id.trim()
            ? raw.id.trim()
            : (typeof raw.name === 'string' && raw.name.trim() ? raw.name.trim() : '');
        const field = typeof raw.field === 'string' && raw.field.trim() ? raw.field.trim() : '';
        if (!id || !field) continue;
        const rules: NormalizedCustomDimension['rules'] = [];
        if (Array.isArray(raw.rules)) {
            for (const rule of raw.rules) {
                if (!rule || typeof rule !== 'object') continue;
                const r = rule as { [key: string]: unknown };
                const label = typeof r.label === 'string' && r.label.trim() ? r.label.trim() : 'Bucket';
                const op = typeof r.op === 'string' && r.op.trim()
                    ? r.op.trim().toLowerCase()
                    : (typeof r.operator === 'string' ? r.operator.trim().toLowerCase() : 'eq');
                rules.push({
                    label,
                    op,
                    value: r.value,
                    value2: r.value2 !== undefined ? r.value2 : r.max,
                });
            }
        }
        result.push({
            id,
            field,
            name: typeof raw.name === 'string' && raw.name.trim() ? raw.name.trim() : id,
            defaultBucket: typeof raw.defaultBucket === 'string' && raw.defaultBucket.trim()
                ? raw.defaultBucket.trim()
                : (typeof raw.elseLabel === 'string' && raw.elseLabel.trim() ? raw.elseLabel.trim() : 'Other'),
            rules,
        });
    }
    return result;
}

function normalizeClause(raw: unknown): NormalizedFilterClause | null {
    if (Array.isArray(raw)) {
        return { type: 'in', values: raw };
    }
    if (raw === null || typeof raw === 'string' || typeof raw === 'number' || typeof raw === 'boolean') {
        return { type: 'eq', value: raw };
    }
    if (!raw || typeof raw !== 'object') return null;
    const obj = raw as { [key: string]: unknown };
    const rawType = typeof obj.type === 'string'
        ? obj.type
        : (typeof obj.op === 'string' ? obj.op : (typeof obj.operator === 'string' ? obj.operator : ''));
    const type = rawType.trim().toLowerCase();
    if (!type && Array.isArray(obj.values)) {
        return { type: 'in', values: obj.values, caseSensitive: Boolean(obj.caseSensitive) };
    }
    if (!type && obj.value !== undefined) {
        return {
            type: Array.isArray(obj.value) ? 'in' : 'eq',
            value: obj.value,
            values: Array.isArray(obj.value) ? obj.value : undefined,
            caseSensitive: Boolean(obj.caseSensitive),
        };
    }
    if (!type) return null;
    return {
        type,
        value: obj.value,
        values: Array.isArray(obj.values) ? obj.values : (Array.isArray(obj.value) ? obj.value : undefined),
        caseSensitive: Boolean(obj.caseSensitive),
    };
}

function normalizeFilters(rawFilters: unknown): NormalizedFieldFilter[] {
    if (!rawFilters || typeof rawFilters !== 'object') return [];
    const result: NormalizedFieldFilter[] = [];

    if (Array.isArray(rawFilters)) {
        for (const item of rawFilters) {
            if (!item || typeof item !== 'object') continue;
            const entry = item as { [key: string]: unknown };
            const field = typeof entry.field === 'string' ? entry.field.trim() : '';
            if (!field) continue;
            const clause = normalizeClause(entry);
            if (clause) {
                result.push({ field, operator: 'AND', clauses: [clause] });
            }
        }
        return result;
    }

    for (const [field, spec] of Object.entries(rawFilters as { [key: string]: unknown })) {
        if (!field || spec === undefined) continue;
        if (Array.isArray(spec)) {
            result.push({
                field,
                operator: 'AND',
                clauses: [{ type: 'in', values: spec }],
            });
            continue;
        }
        if (spec && typeof spec === 'object') {
            const obj = spec as { [key: string]: unknown };
            if (Array.isArray(obj.conditions)) {
                const clauses = obj.conditions
                    .map((cond) => normalizeClause(cond))
                    .filter((c): c is NormalizedFilterClause => c !== null);
                if (clauses.length > 0) {
                    const op = typeof obj.operator === 'string' && obj.operator.trim().toUpperCase() === 'OR' ? 'OR' : 'AND';
                    result.push({ field, operator: op, clauses });
                }
                continue;
            }
            const single = normalizeClause(obj);
            if (single) {
                result.push({ field, operator: 'AND', clauses: [single] });
            }
            continue;
        }
        const primitiveClause = normalizeClause(spec);
        if (primitiveClause) {
            result.push({ field, operator: 'AND', clauses: [primitiveClause] });
        }
    }

    return result;
}

function normalizeSorting(rawSorting: unknown, rawSortOptions: unknown): NormalizedSortRule[] {
    if (!Array.isArray(rawSorting)) return [];
    const sortOpts = (rawSortOptions && typeof rawSortOptions === 'object')
        ? (rawSortOptions as { [key: string]: unknown })
        : {};
    const fieldOptions = (sortOpts.fieldOptions && typeof sortOpts.fieldOptions === 'object')
        ? (sortOpts.fieldOptions as { [key: string]: { [key: string]: unknown } })
        : {};
    const result: NormalizedSortRule[] = [];
    for (const item of rawSorting) {
        if (!item || typeof item !== 'object') continue;
        const entry = item as { [key: string]: unknown };
        const id = typeof entry.id === 'string' && entry.id.trim()
            ? entry.id.trim()
            : (typeof entry.colId === 'string' ? entry.colId.trim() : '');
        if (!id) continue;
        const fieldOpt = fieldOptions[id] || {};
        result.push({
            id,
            desc: Boolean(entry.desc),
            sortType: typeof entry.sortType === 'string'
                ? entry.sortType
                : (typeof fieldOpt.sortType === 'string' ? fieldOpt.sortType : undefined),
            sortKeyField: typeof entry.sortKeyField === 'string'
                ? entry.sortKeyField
                : (typeof fieldOpt.sortKeyField === 'string' ? fieldOpt.sortKeyField : undefined),
            absoluteSort: Boolean(entry.absoluteSort ?? fieldOpt.absoluteSort),
        });
    }
    return result;
}

function normalizeExpanded(rawExpanded: unknown): { expandedAll: boolean; expandedPaths: string[][] } {
    if (rawExpanded === true) {
        return { expandedAll: true, expandedPaths: [] };
    }
    if (Array.isArray(rawExpanded)) {
        const paths = rawExpanded
            .map((entry) => (Array.isArray(entry) ? entry.map((v) => String(v)) : String(entry).split('|||').filter(Boolean)))
            .filter((p) => p.length > 0);
        return { expandedAll: false, expandedPaths: paths };
    }
    if (rawExpanded && typeof rawExpanded === 'object') {
        const paths: string[][] = [];
        for (const [key, isExpanded] of Object.entries(rawExpanded as { [key: string]: unknown })) {
            if (!isExpanded) continue;
            if (key === '__ALL__' || key === '*') {
                return { expandedAll: true, expandedPaths: [] };
            }
            const parts = key.split('|||').filter((part) => part.length > 0);
            if (parts.length > 0) {
                paths.push(parts);
            }
        }
        return { expandedAll: false, expandedPaths: paths };
    }
    return { expandedAll: false, expandedPaths: [] };
}

function buildCustomDimensionSql(dim: NormalizedCustomDimension): string {
    const col = quoteIdent(dim.field);
    if (!dim.rules || dim.rules.length === 0) {
        return `${quoteLiteral(dim.defaultBucket)} AS ${quoteIdent(dim.id)}`;
    }
    const branches: string[] = [];
    for (const rule of dim.rules) {
        const op = rule.op;
        const val = rule.value;
        let cond = 'FALSE';
        if (op === 'eq' || op === '=' || op === '==') {
            cond = `${col} = ${quoteLiteral(val)}`;
        } else if (op === 'ne' || op === '!=' || op === 'not_eq') {
            cond = `${col} <> ${quoteLiteral(val)}`;
        } else if (op === 'gt' || op === '>') {
            cond = `TRY_CAST(${col} AS DOUBLE) > ${Number(val) || 0}`;
        } else if (op === 'gte' || op === '>=') {
            cond = `TRY_CAST(${col} AS DOUBLE) >= ${Number(val) || 0}`;
        } else if (op === 'lt' || op === '<') {
            cond = `TRY_CAST(${col} AS DOUBLE) < ${Number(val) || 0}`;
        } else if (op === 'lte' || op === '<=') {
            cond = `TRY_CAST(${col} AS DOUBLE) <= ${Number(val) || 0}`;
        } else if (op === 'between') {
            const pair = Array.isArray(val) ? val : [val, rule.value2];
            cond = `TRY_CAST(${col} AS DOUBLE) BETWEEN ${Number(pair[0]) || 0} AND ${Number(pair[1]) || 0}`;
        } else if (op === 'in') {
            const list = Array.isArray(val)
                ? val
                : String(val ?? '').split(',').map((s) => s.trim()).filter(Boolean);
            cond = list.length > 0 ? `CAST(${col} AS VARCHAR) IN (${list.map((v) => quoteLiteral(String(v))).join(', ')})` : 'FALSE';
        } else if (op === 'contains') {
            cond = `LOWER(CAST(${col} AS VARCHAR)) LIKE ${quoteLiteral(`%${String(val ?? '').toLowerCase()}%`)}`;
        } else if (op === 'starts_with' || op === 'startswith') {
            cond = `LOWER(CAST(${col} AS VARCHAR)) LIKE ${quoteLiteral(`${String(val ?? '').toLowerCase()}%`)}`;
        } else if (op === 'ends_with' || op === 'endswith') {
            cond = `LOWER(CAST(${col} AS VARCHAR)) LIKE ${quoteLiteral(`%${String(val ?? '').toLowerCase()}`)}`;
        }
        branches.push(`WHEN ${cond} THEN ${quoteLiteral(rule.label)}`);
    }
    return `CASE ${branches.join(' ')} ELSE ${quoteLiteral(dim.defaultBucket)} END AS ${quoteIdent(dim.id)}`;
}

function buildClauseSql(field: string, clause: NormalizedFilterClause): string {
    const col = quoteIdent(field);
    const t = clause.type;
    const val = clause.value;
    if (t === 'is_null' || t === 'isnull' || t === 'empty' || t === 'blank') {
        return `(${col} IS NULL OR CAST(${col} AS VARCHAR) = '')`;
    }
    if (t === 'is_not_null' || t === 'isnotnull' || t === 'not_empty' || t === 'not_blank') {
        return `(${col} IS NOT NULL AND CAST(${col} AS VARCHAR) <> '')`;
    }
    if (t === 'in' || t === 'set') {
        const list = clause.values || (Array.isArray(val) ? val : []);
        if (list.length === 0) return '1 = 0';
        const hasNull = list.some((item) => item === null || item === undefined || item === '__null__');
        const nonNull = list.filter((item) => item !== null && item !== undefined && item !== '__null__');
        const parts: string[] = [];
        if (nonNull.length > 0) {
            parts.push(`CAST(${col} AS VARCHAR) IN (${nonNull.map((item) => quoteLiteral(String(item))).join(', ')})`);
        }
        if (hasNull) {
            parts.push(`${col} IS NULL`);
        }
        return parts.length > 1 ? `(${parts.join(' OR ')})` : (parts[0] || '1 = 0');
    }
    if (t === 'not_in' || t === 'notin') {
        const list = clause.values || (Array.isArray(val) ? val : []);
        if (list.length === 0) return '1 = 1';
        return `(CAST(${col} AS VARCHAR) NOT IN (${list.map((item) => quoteLiteral(String(item))).join(', ')}) AND ${col} IS NOT NULL)`;
    }
    if (t === 'eq' || t === 'equals' || t === '=') {
        if (val === null || val === '__null__') return `${col} IS NULL`;
        return `${col} = ${quoteLiteral(val)}`;
    }
    if (t === 'ne' || t === 'not_eq' || t === 'not_equals' || t === '!=') {
        if (val === null || val === '__null__') return `${col} IS NOT NULL`;
        return `${col} <> ${quoteLiteral(val)}`;
    }
    if (t === 'gt' || t === '>') {
        return `TRY_CAST(${col} AS DOUBLE) > ${Number(val) || 0}`;
    }
    if (t === 'gte' || t === '>=') {
        return `TRY_CAST(${col} AS DOUBLE) >= ${Number(val) || 0}`;
    }
    if (t === 'lt' || t === '<') {
        return `TRY_CAST(${col} AS DOUBLE) < ${Number(val) || 0}`;
    }
    if (t === 'lte' || t === '<=') {
        return `TRY_CAST(${col} AS DOUBLE) <= ${Number(val) || 0}`;
    }
    if (t === 'between') {
        const list = Array.isArray(val) ? val : (clause.values || []);
        return `TRY_CAST(${col} AS DOUBLE) BETWEEN ${Number(list[0]) || 0} AND ${Number(list[1]) || 0}`;
    }
    if (t === 'contains') {
        return `LOWER(CAST(${col} AS VARCHAR)) LIKE ${quoteLiteral(`%${String(val ?? '').toLowerCase()}%`)}`;
    }
    if (t === 'not_contains' || t === 'notcontains') {
        return `LOWER(CAST(${col} AS VARCHAR)) NOT LIKE ${quoteLiteral(`%${String(val ?? '').toLowerCase()}%`)}`;
    }
    if (t === 'starts_with' || t === 'startswith') {
        return `LOWER(CAST(${col} AS VARCHAR)) LIKE ${quoteLiteral(`${String(val ?? '').toLowerCase()}%`)}`;
    }
    if (t === 'ends_with' || t === 'endswith') {
        return `LOWER(CAST(${col} AS VARCHAR)) LIKE ${quoteLiteral(`%${String(val ?? '').toLowerCase()}`)}`;
    }
    return '1 = 1';
}

function buildWhereSql(filters: NormalizedFieldFilter[], excludeField?: string): string {
    const active = filters.filter((f) => f.field !== excludeField && f.clauses.length > 0);
    if (active.length === 0) return '';
    const groups = active.map((f) => {
        const joiner = f.operator === 'OR' ? ' OR ' : ' AND ';
        const exprs = f.clauses.map((c) => buildClauseSql(f.field, c));
        return exprs.length > 1 ? `(${exprs.join(joiner)})` : exprs[0];
    });
    return `WHERE ${groups.join(' AND ')}`;
}

function buildMeasureAggSql(vc: NormalizedValConfig, alias?: string): string | null {
    if (vc.agg === 'formula') return null;
    const col = quoteIdent(vc.field);
    const outAlias = quoteIdent(alias || `${vc.field}_${vc.agg}`);
    const agg = vc.agg.toLowerCase();
    if (agg === 'sum') return `SUM(TRY_CAST(${col} AS DOUBLE)) AS ${outAlias}`;
    if (agg === 'avg' || agg === 'mean') return `AVG(TRY_CAST(${col} AS DOUBLE)) AS ${outAlias}`;
    if (agg === 'min') return `MIN(${col}) AS ${outAlias}`;
    if (agg === 'max') return `MAX(${col}) AS ${outAlias}`;
    if (agg === 'count') return `COUNT(${col}) AS ${outAlias}`;
    if (agg === 'count_distinct' || agg === 'distinct_count' || agg === 'distinctcount') {
        return `COUNT(DISTINCT ${col}) AS ${outAlias}`;
    }
    if (agg === 'median') return `MEDIAN(TRY_CAST(${col} AS DOUBLE)) AS ${outAlias}`;
    if (agg === 'stddev' || agg === 'std' || agg === 'stdev') return `STDDEV_SAMP(TRY_CAST(${col} AS DOUBLE)) AS ${outAlias}`;
    if (agg === 'var' || agg === 'variance') return `VAR_SAMP(TRY_CAST(${col} AS DOUBLE)) AS ${outAlias}`;
    if (agg === 'weighted_avg' || agg === 'wavg' || agg === 'weighted_mean') {
        const wCol = vc.weightField ? quoteIdent(vc.weightField) : '1.0';
        return `SUM(TRY_CAST(${col} AS DOUBLE) * TRY_CAST(${wCol} AS DOUBLE)) / NULLIF(SUM(TRY_CAST(${wCol} AS DOUBLE)), 0) AS ${outAlias}`;
    }
    if (agg === 'array_agg') return `LIST(${col}) AS ${outAlias}`;
    if (agg === 'percentile' || agg.startsWith('p')) {
        const q = Number.isFinite(vc.percentile) ? Number(vc.percentile) : 0.5;
        return `QUANTILE_CONT(TRY_CAST(${col} AS DOUBLE), ${Math.max(0, Math.min(1, q))}) AS ${outAlias}`;
    }
    return `SUM(TRY_CAST(${col} AS DOUBLE)) AS ${outAlias}`;
}

export class ClientSqlPlanner {
    public static compile(request: RuntimeRequestV1, defaultTable = 'client_data'): LogicalPivotPlan {
        const payload = (request && typeof request.payload === 'object' && request.payload)
            ? (request.payload as { [key: string]: unknown })
            : {};
        const stateOverride = (payload.state_override && typeof payload.state_override === 'object')
            ? (payload.state_override as { [key: string]: unknown })
            : {};
        const topState = (request && typeof request.state === 'object' && request.state)
            ? (request.state as { [key: string]: unknown })
            : {};
        const mergedState: { [key: string]: unknown } = {
            ...topState,
            ...payload,
            ...stateOverride,
        };

        const table = typeof payload.table === 'string' && payload.table.trim()
            ? payload.table.trim()
            : (typeof mergedState.table === 'string' && mergedState.table.trim() ? mergedState.table.trim() : defaultTable);

        const viewMode = normalizeViewMode(mergedState.viewMode ?? mergedState.view_mode);
        const layoutMode = normalizeLayoutMode(mergedState.layout_mode ?? mergedState.layoutMode);
        const rowFields = normalizeStringArray(mergedState.rowFields ?? mergedState.row_fields ?? mergedState.rows);
        const colFields = normalizeStringArray(mergedState.colFields ?? mergedState.col_fields ?? mergedState.columns);
        const valConfigs = normalizeValConfigs(mergedState.valConfigs ?? mergedState.val_configs ?? mergedState.values);
        const customDimensions = normalizeCustomDimensions(mergedState.customDimensions ?? mergedState.custom_dimensions);
        const filters = normalizeFilters(mergedState.filters);
        const sortOptionsRaw = (mergedState.sortOptions && typeof mergedState.sortOptions === 'object')
            ? (mergedState.sortOptions as { [key: string]: unknown })
            : ((mergedState.sort_options && typeof mergedState.sort_options === 'object')
                ? (mergedState.sort_options as { [key: string]: unknown })
                : {});
        const sorting = normalizeSorting(mergedState.sorting, sortOptionsRaw);
        const { expandedAll, expandedPaths } = normalizeExpanded(mergedState.expanded);

        const rawMeasureAxis = (mergedState.measureAxis && typeof mergedState.measureAxis === 'object')
            ? (mergedState.measureAxis as { [key: string]: unknown })
            : {};
        const measureAxis = {
            placement: rawMeasureAxis.placement === 'rows' ? ('rows' as const) : ('columns' as const),
            position: Number.isFinite(Number(rawMeasureAxis.position)) ? Number(rawMeasureAxis.position) : undefined,
            measureColId: typeof rawMeasureAxis.measureColId === 'string' ? rawMeasureAxis.measureColId : '__measure_label__',
            valueColId: typeof rawMeasureAxis.valueColId === 'string' ? rawMeasureAxis.valueColId : '__measure_value__',
            label: typeof rawMeasureAxis.label === 'string' ? rawMeasureAxis.label : 'Measure',
            valueLabel: typeof rawMeasureAxis.valueLabel === 'string' ? rawMeasureAxis.valueLabel : 'Value',
        };

        const start = Number.isFinite(Number(payload.start)) ? Math.max(0, Number(payload.start)) : 0;
        const end = Number.isFinite(Number(payload.end)) ? Math.max(start, Number(payload.end)) : Math.max(start + 99, 99);
        const colStart = Number.isFinite(Number(payload.col_start)) ? Math.max(0, Number(payload.col_start)) : null;
        const colEnd = Number.isFinite(Number(payload.col_end)) ? Math.max(0, Number(payload.col_end)) : null;

        const baseTableIdent = quoteIdent(table);
        const customSelects = customDimensions.map((dim) => buildCustomDimensionSql(dim));
        const sourceCte = customSelects.length > 0
            ? `WITH __source AS (SELECT *, ${customSelects.join(', ')} FROM ${baseTableIdent})`
            : `WITH __source AS (SELECT * FROM ${baseTableIdent})`;

        const whereClause = buildWhereSql(filters);
        const baseFilteredSql = `${sourceCte} SELECT * FROM __source ${whereClause}`.trim();

        const groupDims = [...rowFields, ...colFields];
        const aggSelects = valConfigs
            .map((vc) => buildMeasureAggSql(vc))
            .filter((expr): expr is string => Boolean(expr));
        const dimSelects = groupDims.map((d) => quoteIdent(d));
        const allSelects = [...dimSelects, ...(aggSelects.length > 0 ? aggSelects : ['COUNT(*) AS "__row_count"'])];
        const groupByClause = groupDims.length > 0
            ? `GROUP BY ${groupDims.map((d) => quoteIdent(d)).join(', ')}`
            : '';
        const groupingSql = `${sourceCte} SELECT ${allSelects.join(', ')} FROM __source ${whereClause} ${groupByClause}`.trim();

        const colValuesSql = colFields.length > 0
            ? `${sourceCte} SELECT DISTINCT ${colFields.map((c) => quoteIdent(c)).join(', ')} FROM __source ${whereClause} ORDER BY ${colFields.map((c) => quoteIdent(c)).join(', ')}`.trim()
            : '';

        const grandTotalSelects = aggSelects.length > 0 ? aggSelects : ['COUNT(*) AS "__row_count"'];
        const grandTotalSql = `${sourceCte} SELECT ${grandTotalSelects.join(', ')} FROM __source ${whereClause}`.trim();

        const targetFilterField = typeof payload.field === 'string' && payload.field.trim()
            ? payload.field.trim()
            : (rowFields[0] || '');
        const filterWhere = targetFilterField ? buildWhereSql(filters, targetFilterField) : whereClause;
        const filterOptionsSql = targetFilterField
            ? `${sourceCte} SELECT DISTINCT ${quoteIdent(targetFilterField)} AS value FROM __source ${filterWhere} ORDER BY 1 LIMIT 500`.trim()
            : '';

        const drillSql = `${sourceCte} SELECT * FROM __source ${whereClause} LIMIT 500`.trim();
        const reqRecord = request as unknown as { [key: string]: unknown };

        return {
            kind: request.kind as any,
            table,
            viewMode,
            layoutMode,
            showSubtotalFooters: Boolean(mergedState.show_subtotal_footers ?? mergedState.showSubtotalFooters),
            includeGrandTotal: Boolean(mergedState.include_grand_total ?? mergedState.showColTotals ?? true),
            showRowTotals: mergedState.showRowTotals !== false,
            showColTotals: mergedState.showColTotals !== false,
            grandTotalPosition: mergedState.grandTotalPosition === 'top' ? 'top' : 'bottom',
            rowFields,
            colFields,
            valConfigs,
            customDimensions,
            filters,
            sorting,
            sortOptions: {
                columnSortBy: typeof sortOptionsRaw.columnSortBy === 'string' ? sortOptionsRaw.columnSortBy : undefined,
                columnSortOrder: typeof sortOptionsRaw.columnSortOrder === 'string' ? sortOptionsRaw.columnSortOrder : undefined,
                nulls: typeof sortOptionsRaw.nulls === 'string' ? sortOptionsRaw.nulls : undefined,
                naturalSort: sortOptionsRaw.naturalSort !== false,
                fieldOptions: (sortOptionsRaw.fieldOptions && typeof sortOptionsRaw.fieldOptions === 'object')
                    ? (sortOptionsRaw.fieldOptions as LogicalPivotPlan['sortOptions']['fieldOptions'])
                    : {},
            },
            expandedAll,
            expandedPaths,
            measureAxis,
            treeConfig: (mergedState.treeConfig && typeof mergedState.treeConfig === 'object')
                ? (mergedState.treeConfig as { [key: string]: unknown })
                : null,
            reportDef: (mergedState.reportDef && typeof mergedState.reportDef === 'object')
                ? (mergedState.reportDef as { [key: string]: unknown })
                : null,
            detailConfig: (mergedState.detailConfig && typeof mergedState.detailConfig === 'object')
                ? (mergedState.detailConfig as { [key: string]: unknown })
                : null,
            layoutManifest: Array.isArray(mergedState.layoutManifest)
                ? (mergedState.layoutManifest as Array<{ id?: string; layoutId?: string }>)
                : null,
            start,
            end,
            colStart,
            colEnd,
            needsColSchema: Boolean(payload.needs_col_schema ?? true),
            version: Number.isFinite(Number(payload.version ?? reqRecord.version)) ? Number(payload.version ?? reqRecord.version) : 0,
            windowSeq: Number.isFinite(Number(payload.window_seq ?? reqRecord.window_seq ?? request.windowSeq)) ? Number(payload.window_seq ?? reqRecord.window_seq ?? request.windowSeq) : 0,
            stateEpoch: Number.isFinite(Number(payload.state_epoch ?? reqRecord.state_epoch ?? request.stateEpoch)) ? Number(payload.state_epoch ?? reqRecord.state_epoch ?? request.stateEpoch) : 0,
            abortGeneration: Number.isFinite(Number(payload.abort_generation ?? reqRecord.abort_generation ?? request.abortGeneration))
                ? Number(payload.abort_generation ?? reqRecord.abort_generation ?? request.abortGeneration)
                : 0,
            sessionId: typeof (payload.session_id ?? reqRecord.session_id ?? request.sessionId) === 'string'
                ? String(payload.session_id ?? reqRecord.session_id ?? request.sessionId)
                : 'client_session',
            clientInstance: typeof (payload.client_instance ?? reqRecord.client_instance ?? request.clientInstance) === 'string'
                ? String(payload.client_instance ?? reqRecord.client_instance ?? request.clientInstance)
                : 'client_instance',
            intent: typeof (payload.intent ?? reqRecord.intent) === 'string'
                ? String(payload.intent ?? reqRecord.intent)
                : 'structural',
            rawRequest: request,
            sql: {
                baseFilteredSql,
                groupingSql,
                colValuesSql,
                grandTotalSql,
                filterOptionsSql,
                drillSql,
            },
        };
    }
}
