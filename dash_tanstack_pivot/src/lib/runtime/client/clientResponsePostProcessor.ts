import { RUNTIME_SCHEMA_VERSION, type JsonObject as WireJsonObject, type RuntimeResponseV1 } from '../runtimeContract';
type JsonObject = { [key: string]: unknown };
import { filterDatasetRows } from './duckdbWasmExecutor';
import type {
    AggregatedCellMetrics,
    ClientExecutedDataset,
    DrillThroughQuery,
    DrillThroughResult,
    LogicalPivotPlan,
    NormalizedSortRule,
    NormalizedValConfig,
} from './types';

function createEmptyCellMetrics(): AggregatedCellMetrics {
    return {
        sum: 0,
        count: 0,
        numericCount: 0,
        min: null,
        max: null,
        weightedNumer: 0,
        weightedDenom: 0,
        values: [],
        firstValue: undefined,
        lastValue: undefined,
    };
}

function accumulateCellMetrics(
    cell: AggregatedCellMetrics,
    row: { [key: string]: unknown },
    vc: NormalizedValConfig
): void {
    const raw = row[vc.field];
    cell.count += 1;
    if (cell.firstValue === undefined && raw !== null && raw !== undefined) {
        cell.firstValue = raw;
    }
    if (raw !== null && raw !== undefined) {
        cell.lastValue = raw;
        cell.values.push(raw);
    }

    const num = typeof raw === 'number' ? raw : Number(raw);
    if (raw !== null && raw !== undefined && raw !== '' && Number.isFinite(num)) {
        cell.sum += num;
        cell.numericCount += 1;
        if (cell.min === null || num < cell.min) cell.min = num;
        if (cell.max === null || num > cell.max) cell.max = num;
        const wRaw = vc.weightField ? row[vc.weightField] : 1;
        const wNum = typeof wRaw === 'number' ? wRaw : Number(wRaw);
        const weight = Number.isFinite(wNum) ? wNum : 1;
        cell.weightedNumer += num * weight;
        cell.weightedDenom += weight;
    }
}

function finalizeCellMetric(cell: AggregatedCellMetrics | undefined, vc: NormalizedValConfig): unknown {
    if (!cell || cell.count === 0) return null;
    const agg = vc.agg.toLowerCase();
    if (agg === 'sum') return cell.numericCount > 0 ? cell.sum : 0;
    if (agg === 'avg' || agg === 'mean') {
        return cell.numericCount > 0 ? cell.sum / cell.numericCount : null;
    }
    if (agg === 'count') return cell.count;
    if (agg === 'min') return cell.min !== null ? cell.min : (cell.firstValue ?? null);
    if (agg === 'max') return cell.max !== null ? cell.max : (cell.lastValue ?? null);
    if (agg === 'weighted_avg' || agg === 'wavg' || agg === 'weighted_mean') {
        return cell.weightedDenom !== 0 ? cell.weightedNumer / cell.weightedDenom : null;
    }
    if (agg === 'array_agg') {
        return cell.values.slice();
    }
    if (agg === 'first') return cell.firstValue ?? null;
    if (agg === 'last') return cell.lastValue ?? null;
    if (agg === 'percentile' || agg.startsWith('p')) {
        const nums = cell.values
            .map((v) => (typeof v === 'number' ? v : Number(v)))
            .filter((n) => Number.isFinite(n))
            .sort((a, b) => a - b);
        if (nums.length === 0) return null;
        const q = Number.isFinite(vc.percentile) ? Math.max(0, Math.min(1, Number(vc.percentile))) : 0.5;
        const pos = (nums.length - 1) * q;
        const base = Math.floor(pos);
        const rest = pos - base;
        return nums[base + 1] !== undefined ? nums[base] + rest * (nums[base + 1] - nums[base]) : nums[base];
    }
    return cell.numericCount > 0 ? cell.sum : cell.count;
}

// Safe recursive-descent formula evaluator supporting +, -, *, /, parentheses, and [identifier] or bare identifiers
export function evaluateSafeFormula(
    expression: string,
    context: { [key: string]: unknown }
): { value: number | null; error: string | null } {
    const src = String(expression || '').trim();
    if (!src) return { value: null, error: null };

    const normalizedLookup = new Map<string, number | null>();
    for (const [k, v] of Object.entries(context)) {
        const num = typeof v === 'number' ? v : Number(v);
        const val = v !== null && v !== undefined && v !== '' && Number.isFinite(num) ? num : null;
        normalizedLookup.set(k, val);
        normalizedLookup.set(k.toLowerCase(), val);
    }

    let pos = 0;
    const skipWs = () => {
        while (pos < src.length && /\s/.test(src[pos])) pos += 1;
    };

    const resolveVar = (rawName: string): number => {
        const clean = rawName.trim();
        if (normalizedLookup.has(clean)) {
            const v = normalizedLookup.get(clean);
            return v === null || v === undefined ? 0 : v;
        }
        if (normalizedLookup.has(clean.toLowerCase())) {
            const v = normalizedLookup.get(clean.toLowerCase());
            return v === null || v === undefined ? 0 : v;
        }
        throw new Error(`Unknown reference: ${clean}`);
    };

    const parseExpression = (): number => {
        let left = parseTerm();
        while (true) {
            skipWs();
            if (pos < src.length && (src[pos] === '+' || src[pos] === '-')) {
                const op = src[pos++];
                const right = parseTerm();
                left = op === '+' ? left + right : left - right;
            } else {
                break;
            }
        }
        return left;
    };

    const parseTerm = (): number => {
        let left = parseFactor();
        while (true) {
            skipWs();
            if (pos < src.length && (src[pos] === '*' || src[pos] === '/')) {
                const op = src[pos++];
                const right = parseFactor();
                if (op === '/') {
                    if (right === 0) return 0;
                    left /= right;
                } else {
                    left *= right;
                }
            } else {
                break;
            }
        }
        return left;
    };

    const parseFactor = (): number => {
        skipWs();
        if (pos < src.length && (src[pos] === '+' || src[pos] === '-')) {
            const sign = src[pos++];
            const val = parseFactor();
            return sign === '-' ? -val : val;
        }
        if (pos < src.length && src[pos] === '(') {
            pos += 1;
            const inner = parseExpression();
            skipWs();
            if (src[pos] === ')') pos += 1;
            return inner;
        }
        if (pos < src.length && src[pos] === '[') {
            pos += 1;
            const start = pos;
            while (pos < src.length && src[pos] !== ']') pos += 1;
            const refName = src.slice(start, pos);
            if (src[pos] === ']') pos += 1;
            return resolveVar(refName);
        }
        const numMatch = src.slice(pos).match(/^\d+(?:\.\d+)?/);
        if (numMatch) {
            pos += numMatch[0].length;
            return Number(numMatch[0]);
        }
        const idMatch = src.slice(pos).match(/^[A-Za-z_][A-Za-z0-9_.:|-]*/);
        if (idMatch) {
            const ident = idMatch[0];
            pos += ident.length;
            skipWs();
            if (src[pos] === '(') {
                pos += 1;
                const args: number[] = [];
                skipWs();
                if (src[pos] !== ')') {
                    while (true) {
                        args.push(parseExpression());
                        skipWs();
                        if (src[pos] === ',') {
                            pos += 1;
                        } else {
                            break;
                        }
                    }
                }
                if (src[pos] === ')') pos += 1;
                const fn = ident.toLowerCase();
                if (fn === 'abs') return Math.abs(args[0] ?? 0);
                if (fn === 'min') return args.length ? Math.min(...args) : 0;
                if (fn === 'max') return args.length ? Math.max(...args) : 0;
                if (fn === 'round') return Math.round(args[0] ?? 0);
                if (fn === 'coalesce' || fn === 'ifnull') return args.find((a) => Number.isFinite(a)) ?? 0;
                return args[0] ?? 0;
            }
            return resolveVar(ident);
        }
        throw new Error(`Unexpected token at position ${pos}`);
    };

    try {
        const result = parseExpression();
        return { value: Number.isFinite(result) ? result : null, error: null };
    } catch (err) {
        return { value: null, error: err instanceof Error ? err.message : 'Formula error' };
    }
}

function parseTenorScore(val: unknown): number | null {
    if (typeof val !== 'string') return null;
    const m = val.trim().toUpperCase().match(/^(\d+(?:\.\d+)?)\s*(ON|TN|SN|D|W|M|Y)$/);
    if (!m) return null;
    const n = Number(m[1]);
    const unit = m[2];
    if (unit === 'ON' || unit === 'TN' || unit === 'SN' || unit === 'D') return n;
    if (unit === 'W') return n * 7;
    if (unit === 'M') return n * 30;
    if (unit === 'Y') return n * 365;
    return null;
}

function compareRowValues(
    aRow: { [key: string]: unknown },
    bRow: { [key: string]: unknown },
    rule: NormalizedSortRule,
    naturalSort = true
): number {
    const keyField = rule.sortKeyField || rule.id;
    const aRaw = aRow[keyField] !== undefined ? aRow[keyField] : aRow._id;
    const bRaw = bRow[keyField] !== undefined ? bRow[keyField] : bRow._id;

    const aNull = aRaw === null || aRaw === undefined || aRaw === '';
    const bNull = bRaw === null || bRaw === undefined || bRaw === '';
    if (aNull && bNull) return 0;
    if (aNull) return 1;
    if (bNull) return -1;

    const dir = rule.desc ? -1 : 1;

    if (rule.sortType === 'tenor') {
        const aScore = parseTenorScore(aRaw);
        const bScore = parseTenorScore(bRaw);
        if (aScore !== null && bScore !== null && aScore !== bScore) {
            return (aScore - bScore) * dir;
        }
    }

    const aNum = typeof aRaw === 'number' ? aRaw : Number(aRaw);
    const bNum = typeof bRaw === 'number' ? bRaw : Number(bRaw);
    if (Number.isFinite(aNum) && Number.isFinite(bNum)) {
        const left = rule.absoluteSort ? Math.abs(aNum) : aNum;
        const right = rule.absoluteSort ? Math.abs(bNum) : bNum;
        if (left !== right) return (left - right) * dir;
    }

    const cmp = String(aRaw).localeCompare(String(bRaw), undefined, {
        numeric: naturalSort,
        sensitivity: 'base',
    });
    return cmp * dir;
}

function isPathExpanded(pathParts: string[], plan: LogicalPivotPlan): boolean {
    const target = pathParts.join('|||');
    if (plan.expandedAll) {
        return !plan.collapsedPaths.some((p) => p.join('|||') === target);
    }
    return plan.expandedPaths.some((p) => p.join('|||') === target);
}

interface HierarchyGroupNode {
    key: string;
    label: string;
    pathParts: string[];
    depth: number;
    field: string;
    rows: Array<{ [key: string]: unknown }>;
    children: HierarchyGroupNode[];
}

function buildGroupTree(
    rows: Array<{ [key: string]: unknown }>,
    rowFields: string[],
    depth: number,
    parentPath: string[]
): HierarchyGroupNode[] {
    if (depth >= rowFields.length) return [];
    const field = rowFields[depth];
    const buckets = new Map<string, Array<{ [key: string]: unknown }>>();
    for (const row of rows) {
        const raw = row[field];
        const label = raw === null || raw === undefined ? 'null' : String(raw);
        const list = buckets.get(label);
        if (list) {
            list.push(row);
        } else {
            buckets.set(label, [row]);
        }
    }

    const nodes: HierarchyGroupNode[] = [];
    for (const [label, groupRows] of Array.from(buckets.entries())) {
        const pathParts = [...parentPath, label];
        const children = depth + 1 < rowFields.length
            ? buildGroupTree(groupRows, rowFields, depth + 1, pathParts)
            : [];
        nodes.push({
            key: pathParts.join('|||'),
            label,
            pathParts,
            depth,
            field,
            rows: groupRows,
            children,
        });
    }
    return nodes;
}

function getMeasureSuffix(vc: NormalizedValConfig): string {
    if (vc.id && vc.id.trim()) return vc.id.trim();
    if (vc.agg === 'formula') return `${vc.field}_formula`;
    return `${vc.field}_${vc.agg}`;
}

function computeAggregatedRecord(
    groupRows: Array<{ [key: string]: unknown }>,
    plan: LogicalPivotPlan,
    colCombinations: string[],
    formulaErrors: { [key: string]: string }
): { [key: string]: unknown } {
    const out: { [key: string]: unknown } = {};
    const nonFormulaConfigs = plan.valConfigs.filter((vc) => vc.agg !== 'formula');
    const formulaConfigs = plan.valConfigs.filter((vc) => vc.agg === 'formula' && vc.formula);

    if (plan.colFields.length === 0) {
        const cells = new Map<string, AggregatedCellMetrics>();
        for (const vc of nonFormulaConfigs) {
            cells.set(getMeasureSuffix(vc), createEmptyCellMetrics());
        }
        for (const row of groupRows) {
            for (const vc of nonFormulaConfigs) {
                const cell = cells.get(getMeasureSuffix(vc))!;
                accumulateCellMetrics(cell, row, vc);
            }
        }
        const formulaContext: { [key: string]: unknown } = {};
        for (const vc of nonFormulaConfigs) {
            const colId = getMeasureSuffix(vc);
            const val = finalizeCellMetric(cells.get(colId), vc);
            out[colId] = val;
            formulaContext[colId] = val;
            formulaContext[vc.field] = val;
            formulaContext[vc.id] = val;
        }
        for (const fc of formulaConfigs) {
            const colId = getMeasureSuffix(fc);
            const evaluated = evaluateSafeFormula(fc.formula || '', formulaContext);
            out[colId] = evaluated.value;
            if (evaluated.error) formulaErrors[colId] = evaluated.error;
        }
        return out;
    }

    // Pivot with column dimensions
    const colCells = new Map<string, AggregatedCellMetrics>();
    const rowTotalCells = new Map<string, AggregatedCellMetrics>();
    for (const vc of nonFormulaConfigs) {
        rowTotalCells.set(`__RowTotal__${getMeasureSuffix(vc)}`, createEmptyCellMetrics());
    }

    for (const row of groupRows) {
        const colPrefix = plan.colFields
            .map((cf) => {
                const v = row[cf];
                return v === null || v === undefined ? 'null' : String(v);
            })
            .join('_');

        for (const vc of nonFormulaConfigs) {
            const cellId = `${colPrefix}_${getMeasureSuffix(vc)}`;
            let cell = colCells.get(cellId);
            if (!cell) {
                cell = createEmptyCellMetrics();
                colCells.set(cellId, cell);
            }
            accumulateCellMetrics(cell, row, vc);

            const totalCell = rowTotalCells.get(`__RowTotal__${getMeasureSuffix(vc)}`)!;
            accumulateCellMetrics(totalCell, row, vc);
        }
    }

    for (const colPrefix of colCombinations) {
        const colFormulaContext: { [key: string]: unknown } = {};
        for (const vc of nonFormulaConfigs) {
            const cellId = `${colPrefix}_${getMeasureSuffix(vc)}`;
            const val = finalizeCellMetric(colCells.get(cellId), vc);
            out[cellId] = val;
            colFormulaContext[vc.field] = val;
            colFormulaContext[`${vc.field}_${vc.agg}`] = val;
            colFormulaContext[vc.id] = val;
        }
        for (const fc of formulaConfigs) {
            if (fc.formulaScope === 'columns') continue;
            const suffix = getMeasureSuffix(fc);
            const cellId = `${colPrefix}_${suffix}`;
            const evaluated = evaluateSafeFormula(fc.formula || '', colFormulaContext);
            out[cellId] = evaluated.value;
            if (evaluated.error) formulaErrors[cellId] = evaluated.error;
        }
    }

    // Column-scope formulas (e.g. [North] - [South] or [North_sales_sum] - [South_sales_sum])
    for (const fc of formulaConfigs) {
        if (fc.formulaScope !== 'columns') continue;
        const baseVc = nonFormulaConfigs[0];
        if (!baseVc) continue;
        const colScopeContext: { [key: string]: unknown } = { ...out };
        for (const colPrefix of colCombinations) {
            colScopeContext[colPrefix] = out[`${colPrefix}_${baseVc.field}_${baseVc.agg}`];
        }
        const suffix = fc.id.endsWith('_formula') ? fc.id : `${fc.field}_formula`;
        const evaluated = evaluateSafeFormula(fc.formula || '', colScopeContext);
        out[suffix] = evaluated.value;
        if (evaluated.error) formulaErrors[suffix] = evaluated.error;
    }

    if (plan.showRowTotals) {
        const totalFormulaContext: { [key: string]: unknown } = {};
        for (const vc of nonFormulaConfigs) {
            const suffix = getMeasureSuffix(vc);
            const totalId = `__RowTotal__${suffix}`;
            const val = finalizeCellMetric(rowTotalCells.get(totalId), vc);
            out[totalId] = val;
            out[`__RowTotal____${suffix}`] = val;
            totalFormulaContext[vc.field] = val;
            totalFormulaContext[`${vc.field}_${vc.agg}`] = val;
            totalFormulaContext[vc.id] = val;
        }
        for (const fc of formulaConfigs) {
            if (fc.formulaScope === 'columns') continue;
            const suffix = getMeasureSuffix(fc);
            const totalId = `__RowTotal__${suffix}`;
            const evaluated = evaluateSafeFormula(fc.formula || '', totalFormulaContext);
            out[totalId] = evaluated.value;
            out[`__RowTotal____${suffix}`] = evaluated.value;
            if (evaluated.error) formulaErrors[totalId] = evaluated.error;
        }
    }

    return out;
}

function applyWindowFunctions(
    rows: Array<{ [key: string]: unknown }>,
    grandTotalRow: { [key: string]: unknown } | null,
    plan: LogicalPivotPlan,
    colCombinations: string[]
): void {
    const windowConfigs = plan.valConfigs.filter((vc) => typeof vc.windowFn === 'string' && vc.windowFn.trim());
    if (windowConfigs.length === 0) return;

    const allTargetRows = grandTotalRow ? [...rows, grandTotalRow] : rows;

    for (const vc of windowConfigs) {
        const rawFn = (vc.windowFn || '').trim().toLowerCase();
        const fn = rawFn === 'percent_of_total' ? 'percent_of_grand_total' : rawFn;
        const suffix = getMeasureSuffix(vc);

        if (plan.colFields.length === 0) {
            const grandDenom = grandTotalRow ? Number(grandTotalRow[suffix]) : 0;
            if (fn === 'percent_of_col' || fn === 'percent_of_grand_total') {
                for (const row of allTargetRows) {
                    const num = Number(row[suffix]);
                    row[suffix] = Number.isFinite(num) && Number.isFinite(grandDenom) && grandDenom !== 0
                        ? num / grandDenom
                        : (num === 0 ? 0 : null);
                }
            }
            continue;
        }

        const totalColKey = `__RowTotal__${suffix}`;
        const grandTotalDenom = grandTotalRow ? Number(grandTotalRow[totalColKey]) : 0;

        for (const row of allTargetRows) {
            const rowDenom = Number(row[totalColKey]);
            for (const colPrefix of colCombinations) {
                const cellKey = `${colPrefix}_${suffix}`;
                const cellNum = Number(row[cellKey]);
                if (!Number.isFinite(cellNum)) continue;

                if (fn === 'percent_of_row') {
                    row[cellKey] = Number.isFinite(rowDenom) && rowDenom !== 0 ? cellNum / rowDenom : 0;
                } else if (fn === 'percent_of_col' && grandTotalRow) {
                    const colDenom = Number(grandTotalRow[cellKey]);
                    row[cellKey] = Number.isFinite(colDenom) && colDenom !== 0 ? cellNum / colDenom : 0;
                } else if (fn === 'percent_of_grand_total') {
                    row[cellKey] = Number.isFinite(grandTotalDenom) && grandTotalDenom !== 0
                        ? cellNum / grandTotalDenom
                        : 0;
                }
            }
            if (fn === 'percent_of_row' && Number.isFinite(rowDenom)) {
                row[totalColKey] = rowDenom !== 0 ? 1 : 0;
            } else if ((fn === 'percent_of_col' || fn === 'percent_of_grand_total') && Number.isFinite(grandTotalDenom) && grandTotalDenom !== 0) {
                if (Number.isFinite(rowDenom)) {
                    row[totalColKey] = rowDenom / grandTotalDenom;
                }
            }
        }
    }
}

function buildPivotVisibleRows(
    executed: ClientExecutedDataset,
    plan: LogicalPivotPlan,
    formulaErrors: { [key: string]: string }
): { rows: Array<{ [key: string]: unknown }>; grandTotalRow: { [key: string]: unknown } | null } {
    const colCombinations = executed.colCombinations;
    const rootNodes = buildGroupTree(executed.filteredRows, plan.rowFields, 0, []);

    const materializeNodeRecord = (node: HierarchyGroupNode, isSubtotal = false): { [key: string]: unknown } => {
        const metrics = computeAggregatedRecord(node.rows, plan, colCombinations, formulaErrors);
        const hasChildren = node.depth + 1 < plan.rowFields.length && node.children.length > 0;
        const tabularForceExpand = plan.layoutMode === 'tabular';
        const expanded = hasChildren && (tabularForceExpand || isPathExpanded(node.pathParts, plan));
        const rowObj: { [key: string]: unknown } = {
            ...metrics,
            _id: isSubtotal ? `${node.label} Total` : node.label,
            _path: isSubtotal ? `${node.key}|||__subtotal__` : node.key,
            depth: node.depth,
            _has_children: (isSubtotal || tabularForceExpand) ? false : hasChildren,
            _is_expanded: (isSubtotal || tabularForceExpand) ? false : expanded,
        };
        for (let i = 0; i <= node.depth && i < plan.rowFields.length; i += 1) {
            rowObj[plan.rowFields[i]] = node.pathParts[i];
        }
        if (isSubtotal) {
            rowObj._isTotal = true;
            rowObj.__isSubtotal__ = true;
            if (plan.layoutMode === 'tabular') {
                rowObj._isTabularSubtotal = true;
            }
            rowObj[node.field] = `${node.label} Total`;
        }
        return rowObj;
    };

    const sortNodeList = (nodes: HierarchyGroupNode[], recordsMap: Map<string, { [key: string]: unknown }>) => {
        nodes.sort((a, b) => {
            const aRec = recordsMap.get(a.key)!;
            const bRec = recordsMap.get(b.key)!;
            for (const rule of plan.sorting) {
                const cmp = compareRowValues(aRec, bRec, rule, plan.sortOptions.naturalSort);
                if (cmp !== 0) return cmp;
            }
            return String(a.label).localeCompare(String(b.label), undefined, {
                numeric: plan.sortOptions.naturalSort,
                sensitivity: 'base',
            });
        });
    };

    const visibleRows: Array<{ [key: string]: unknown }> = [];
    const walkNodes = (nodes: HierarchyGroupNode[]) => {
        const recordMap = new Map<string, { [key: string]: unknown }>();
        for (const node of nodes) {
            recordMap.set(node.key, materializeNodeRecord(node, false));
        }
        sortNodeList(nodes, recordMap);

        for (const node of nodes) {
            const rec = recordMap.get(node.key)!;
            const nodeHasChildren = node.depth + 1 < plan.rowFields.length && node.children.length > 0;
            const expanded = Boolean(rec._is_expanded);

            if (plan.layoutMode === 'tabular') {
                if (nodeHasChildren) {
                    walkNodes(node.children);
                    if (plan.showSubtotalFooters) {
                        visibleRows.push(materializeNodeRecord(node, true));
                    }
                } else {
                    visibleRows.push(rec);
                }
            } else {
                // Hierarchy and outline are pre-order views: the expandable group row
                // already carries the aggregate subtotal (matching Python hierarchy_rows.py).
                visibleRows.push(rec);
                if (nodeHasChildren && expanded) {
                    walkNodes(node.children);
                }
            }
        }
    };

    if (plan.rowFields.length === 0) {
        if (executed.filteredRows.length > 0) {
            const singleMetrics = computeAggregatedRecord(executed.filteredRows, plan, colCombinations, formulaErrors);
            visibleRows.push({
                ...singleMetrics,
                _id: 'All',
                _path: 'All',
                depth: 0,
                _has_children: false,
                _is_expanded: false,
            });
        }
    } else {
        walkNodes(rootNodes);
    }

    let grandTotalRow: { [key: string]: unknown } | null = null;
    if ((plan.includeGrandTotal || plan.showColTotals) && executed.filteredRows.length > 0) {
        const grandMetrics = computeAggregatedRecord(executed.filteredRows, plan, colCombinations, formulaErrors);
        grandTotalRow = {
            ...grandMetrics,
            _id: 'Grand Total',
            _path: '__grand_total__',
            depth: 0,
            _isTotal: true,
            __isGrandTotal__: true,
            _has_children: false,
            _is_expanded: false,
        };
        if (plan.rowFields.length > 0) {
            grandTotalRow[plan.rowFields[0]] = 'Grand Total';
        }
    }

    applyWindowFunctions(visibleRows, grandTotalRow, plan, colCombinations);

    return { rows: visibleRows, grandTotalRow };
}

function buildTreeVisibleRows(
    executed: ClientExecutedDataset,
    plan: LogicalPivotPlan,
    formulaErrors: { [key: string]: string }
): { rows: Array<{ [key: string]: unknown }>; grandTotalRow: { [key: string]: unknown } | null } {
    const tc = plan.treeConfig || {};
    const mode = typeof tc.mode === 'string' ? tc.mode.toLowerCase() : 'adjacency';
    if (mode === 'path' && Array.isArray(tc.pathFields) && tc.pathFields.length > 0) {
        const clonedPlan: LogicalPivotPlan = {
            ...plan,
            rowFields: tc.pathFields.map((f) => String(f)),
        };
        return buildPivotVisibleRows(executed, clonedPlan, formulaErrors);
    }

    const idField = typeof tc.idField === 'string' && tc.idField ? tc.idField : 'id';
    const parentIdField = typeof tc.parentIdField === 'string' && tc.parentIdField ? tc.parentIdField : 'parent_id';
    const labelField = typeof tc.labelField === 'string' && tc.labelField ? tc.labelField : idField;

    const childrenByParent = new Map<string, Array<{ [key: string]: unknown }>>();
    const byId = new Map<string, { [key: string]: unknown }>();

    for (const row of executed.filteredRows) {
        const idVal = row[idField];
        if (idVal === null || idVal === undefined) continue;
        byId.set(String(idVal), row);
    }

    for (const row of executed.filteredRows) {
        const idVal = row[idField];
        if (idVal === null || idVal === undefined) continue;
        const parentVal = row[parentIdField];
        const parentKey = parentVal === null || parentVal === undefined || !byId.has(String(parentVal))
            ? '__ROOT__'
            : String(parentVal);
        const list = childrenByParent.get(parentKey);
        if (list) list.push(row);
        else childrenByParent.set(parentKey, [row]);
    }

    const collectSubtreeRows = (nodeRow: { [key: string]: unknown }): Array<{ [key: string]: unknown }> => {
        const idKey = String(nodeRow[idField]);
        const kids = childrenByParent.get(idKey) || [];
        let acc = [nodeRow];
        for (const kid of kids) {
            acc = acc.concat(collectSubtreeRows(kid));
        }
        return acc;
    };

    const visibleRows: Array<{ [key: string]: unknown }> = [];
    const walkAdjacency = (parentKey: string, parentPath: string[], depth: number) => {
        const kids = childrenByParent.get(parentKey) || [];
        for (const row of kids) {
            const idStr = String(row[idField]);
            const labelStr = String(row[labelField] ?? idStr);
            const pathParts = [...parentPath, idStr];
            const pathKey = pathParts.join('|||');
            const childRows = childrenByParent.get(idStr) || [];
            const hasChildren = childRows.length > 0;
            const expanded = hasChildren && isPathExpanded(pathParts, plan);
            const subtreeRows = collectSubtreeRows(row);
            const metrics = computeAggregatedRecord(subtreeRows, plan, executed.colCombinations, formulaErrors);
            visibleRows.push({
                ...row,
                ...metrics,
                _id: labelStr,
                hierarchy: labelStr,
                ...(hasChildren ? { subRows: [{}] } : {}),
                _path: pathKey,
                depth,
                _has_children: hasChildren,
                _is_expanded: expanded,
            });
            if (hasChildren && expanded) {
                walkAdjacency(idStr, pathParts, depth + 1);
            }
        }
    };

    walkAdjacency('__ROOT__', [], 0);
    return { rows: visibleRows, grandTotalRow: null };
}

function buildReportVisibleRows(
    executed: ClientExecutedDataset,
    plan: LogicalPivotPlan,
    formulaErrors: { [key: string]: string }
): { rows: Array<{ [key: string]: unknown }>; grandTotalRow: { [key: string]: unknown } | null } {
    const reportDef = plan.reportDef || {};
    const rootLevel = (reportDef.root && typeof reportDef.root === 'object')
        ? (reportDef.root as { [key: string]: unknown })
        : (Array.isArray(reportDef.levels) && reportDef.levels[0] && typeof reportDef.levels[0] === 'object'
            ? (reportDef.levels[0] as { [key: string]: unknown })
            : reportDef);
    const field = typeof rootLevel.field === 'string' && rootLevel.field.trim()
        ? rootLevel.field.trim()
        : (plan.rowFields[0] || '');

    if (!field) {
        return buildPivotVisibleRows(executed, plan, formulaErrors);
    }

    const baseResult = buildPivotVisibleRows(executed, { ...plan, rowFields: [field] }, formulaErrors);
    const topN = Number.isFinite(Number(rootLevel.topN)) ? Math.max(1, Number(rootLevel.topN)) : null;
    const rows = topN ? baseResult.rows.slice(0, topN) : baseResult.rows;
    const formatObj = (rootLevel.format && typeof rootLevel.format === 'object')
        ? (rootLevel.format as { [key: string]: unknown })
        : null;
    const labelPrefix = typeof rootLevel.labelPrefix === 'string'
        ? rootLevel.labelPrefix
        : (formatObj && typeof formatObj.labelPrefix === 'string' ? formatObj.labelPrefix : '');

    for (const row of rows) {
        const rawLabel = String(row._id ?? '');
        row._reportDisplayLabel = labelPrefix ? `${labelPrefix}${rawLabel}` : rawLabel;
        if (formatObj) {
            row._reportFormat = formatObj;
        }
    }

    if (Boolean(rootLevel.includeOther ?? rootLevel.include_other) && topN && baseResult.rows.length > topN) {
        const otherRows = baseResult.rows.slice(topN);
        const otherRecord: { [key: string]: unknown } = {
            _id: 'Other',
            hierarchy: 'Other',
            _reportDisplayLabel: 'Other',
            _path: '__other__',
            depth: 0,
            _has_children: false,
            _is_expanded: false,
            __isOther__: true,
        };
        for (const r of otherRows) {
            for (const [k, v] of Object.entries(r)) {
                if (k.startsWith('_') || k === field || k === 'hierarchy') continue;
                if (typeof v === 'number' && Number.isFinite(v)) {
                    otherRecord[k] = (typeof otherRecord[k] === 'number' ? Number(otherRecord[k]) : 0) + v;
                }
            }
        }
        rows.push(otherRecord);
    }
    return { rows, grandTotalRow: baseResult.grandTotalRow };
}

function buildFlatTableVisibleRows(
    executed: ClientExecutedDataset,
    plan: LogicalPivotPlan
): { rows: Array<{ [key: string]: unknown }>; grandTotalRow: { [key: string]: unknown } | null } {
    const sorted = executed.filteredRows.slice();
    if (plan.sorting.length > 0) {
        sorted.sort((a, b) => {
            for (const rule of plan.sorting) {
                const cmp = compareRowValues(a, b, rule, plan.sortOptions.naturalSort);
                if (cmp !== 0) return cmp;
            }
            return 0;
        });
    }
    const rows = sorted.map((row, idx) => ({
        ...row,
        _id: String(row[plan.rowFields[0] || 'id'] ?? `row_${idx + 1}`),
        _path: `row_${idx + 1}`,
        depth: 0,
        _has_children: false,
        _is_expanded: false,
    }));
    return { rows, grandTotalRow: null };
}

function expandRowsMeasurePlacement(
    rows: Array<{ [key: string]: unknown }>,
    grandTotalRow: { [key: string]: unknown } | null,
    plan: LogicalPivotPlan,
    colCombinations: string[]
): { rows: Array<{ [key: string]: unknown }>; grandTotalRow: { [key: string]: unknown } | null } {
    if (plan.measureAxis.placement !== 'rows' || plan.valConfigs.length === 0) {
        return { rows, grandTotalRow };
    }
    const measureColId = plan.measureAxis.measureColId || '__measure_label__';
    const valueColId = plan.measureAxis.valueColId || '__measure_value__';

    const expandOneRow = (sourceRow: { [key: string]: unknown }): Array<{ [key: string]: unknown }> =>
        plan.valConfigs.map((vc) => {
            const suffix = vc.agg === 'formula'
                ? (vc.id.endsWith('_formula') ? vc.id : `${vc.field}_formula`)
                : `${vc.field}_${vc.agg}`;
            const clone: { [key: string]: unknown } = {
                ...sourceRow,
                _path: `${sourceRow._path}|||__measure__${vc.id}`,
                [measureColId]: vc.label || vc.field,
            };
            if (plan.colFields.length === 0) {
                clone[valueColId] = sourceRow[suffix];
            } else {
                for (const colPrefix of colCombinations) {
                    clone[colPrefix] = sourceRow[`${colPrefix}_${suffix}`];
                }
                if (plan.showRowTotals) {
                    clone.__RowTotal__ = sourceRow[`__RowTotal__${suffix}`];
                }
            }
            return clone;
        });

    const expandedRows: Array<{ [key: string]: unknown }> = [];
    for (const r of rows) {
        expandedRows.push(...expandOneRow(r));
    }
    const expandedGrand = grandTotalRow ? expandOneRow(grandTotalRow)[0] || grandTotalRow : null;
    return { rows: expandedRows, grandTotalRow: expandedGrand };
}

function formatColHeader(colId: string): string {
    if (colId.startsWith('__RowTotal__')) {
        const measure = colId.replace('__RowTotal__', '');
        return `Total ${measure.replace(/_/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase())}`;
    }
    return colId.replace(/_/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase());
}

function buildColumnListAndSchema(
    plan: LogicalPivotPlan,
    colCombinations: string[],
    sampleRows: Array<{ [key: string]: unknown }>
): {
    allColumnObjects: Array<{ id: string; header: string; accessorKey: string; isRowTotal?: boolean }>;
    allColumnIds: string[];
    centerColumns: string[];
    colSchema: {
        total_center_cols: number;
        layoutRevision?: string;
        columns: Array<{ id: string; layoutId?: string; index: number; size: number }>;
    };
} {
    if (plan.viewMode === 'table') {
        const sample = sampleRows[0] || {};
        const cols = Object.keys(sample).filter((k) => !k.startsWith('_'));
        return {
            allColumnObjects: cols.map((id) => ({ id, header: formatColHeader(id), accessorKey: id })),
            allColumnIds: cols,
            centerColumns: cols,
            colSchema: {
                total_center_cols: cols.length,
                columns: cols.map((id, index) => ({ id, index, size: 150 })),
            },
        };
    }

    const leftCols = [...plan.rowFields];
    if (plan.measureAxis.placement === 'rows') {
        leftCols.push(plan.measureAxis.measureColId || '__measure_label__');
    }

    let centerCols: string[] = [];
    let rowTotalColIds: string[] = [];

    if (plan.measureAxis.placement === 'rows') {
        if (plan.colFields.length === 0) {
            centerCols = [plan.measureAxis.valueColId || '__measure_value__'];
        } else {
            centerCols = [...colCombinations];
            if (plan.showRowTotals) rowTotalColIds.push('__RowTotal__');
        }
    } else if (plan.colFields.length === 0) {
        centerCols = plan.valConfigs.map((vc) =>
            vc.agg === 'formula'
                ? (vc.id.endsWith('_formula') ? vc.id : `${vc.field}_formula`)
                : `${vc.field}_${vc.agg}`
        );
    } else {
        for (const colPrefix of colCombinations) {
            for (const vc of plan.valConfigs) {
                if (vc.agg === 'formula' && vc.formulaScope === 'columns') continue;
                const suffix = vc.agg === 'formula'
                    ? (vc.id.endsWith('_formula') ? vc.id : `${vc.field}_formula`)
                    : `${vc.field}_${vc.agg}`;
                centerCols.push(`${colPrefix}_${suffix}`);
            }
        }
        for (const vc of plan.valConfigs) {
            if (vc.agg === 'formula' && vc.formulaScope === 'columns') {
                centerCols.push(vc.id.endsWith('_formula') ? vc.id : `${vc.field}_formula`);
            }
        }
        if (plan.showRowTotals) {
            for (const vc of plan.valConfigs) {
                if (vc.agg === 'formula' && vc.formulaScope === 'columns') continue;
                const suffix = vc.agg === 'formula'
                    ? (vc.id.endsWith('_formula') ? vc.id : `${vc.field}_formula`)
                    : `${vc.field}_${vc.agg}`;
                rowTotalColIds.push(`__RowTotal__${suffix}`);
            }
        }
    }

    if (Array.isArray(plan.layoutManifest) && plan.layoutManifest.length > 0) {
        const orderIds = plan.layoutManifest
            .map((entry) => (typeof entry === 'string' ? entry : (entry.id || entry.layoutId || '')))
            .filter(Boolean);
        const orderSet = new Set(orderIds);
        const ordered = orderIds.filter((id) => centerCols.includes(id));
        const remaining = centerCols.filter((id) => !orderSet.has(id));
        centerCols = [...ordered, ...remaining];
    }

    const schemaEntries = centerCols.map((id, index) => ({
        id,
        index,
        size: 145,
    }));

    const slicedSchema = plan.colStart !== null && plan.colEnd !== null
        ? schemaEntries.slice(plan.colStart, plan.colEnd + 1)
        : schemaEntries;

    const allColumnIds = [...leftCols, ...centerCols, ...rowTotalColIds];
    const allColumnObjects = allColumnIds.map((id) => ({
        id,
        header: formatColHeader(id),
        accessorKey: id,
        ...(id.startsWith('__RowTotal__') ? { isRowTotal: true } : {}),
    }));

    return {
        allColumnObjects,
        allColumnIds,
        centerColumns: centerCols,
        colSchema: {
            total_center_cols: centerCols.length,
            columns: slicedSchema,
        },
    };
}

function computeColorScaleStats(
    rows: Array<{ [key: string]: unknown }>,
    centerColumns: string[]
): {
    colMin: { [key: string]: number };
    colMax: { [key: string]: number };
    tableMin: number | null;
    tableMax: number | null;
} {
    const colMin: { [key: string]: number } = {};
    const colMax: { [key: string]: number } = {};
    let tableMin: number | null = null;
    let tableMax: number | null = null;

    for (const row of rows) {
        if (row._isTotal || row.__isGrandTotal__ || row.__isSubtotal__) continue;
        for (const colId of centerColumns) {
            const raw = row[colId];
            const num = typeof raw === 'number' ? raw : Number(raw);
            if (raw === null || raw === undefined || raw === '' || !Number.isFinite(num)) continue;
            if (colMin[colId] === undefined || num < colMin[colId]) colMin[colId] = num;
            if (colMax[colId] === undefined || num > colMax[colId]) colMax[colId] = num;
            if (tableMin === null || num < tableMin) tableMin = num;
            if (tableMax === null || num > tableMax) tableMax = num;
        }
    }

    return { colMin, colMax, tableMin, tableMax };
}

function buildEnvelope(
    plan: LogicalPivotPlan,
    kind: RuntimeResponseV1['kind'],
    status: string,
    payload: JsonObject,
    extraTopLevel: { [key: string]: unknown } = {},
    profile?: JsonObject
): RuntimeResponseV1 {
    const requestId = plan.rawRequest.requestId || `${kind}:${Date.now()}`;
    return {
        schemaVersion: RUNTIME_SCHEMA_VERSION,
        kind,
        status,
        requestId,
        table: plan.table,
        sessionId: plan.sessionId,
        clientInstance: plan.clientInstance,
        stateEpoch: plan.stateEpoch,
        windowSeq: plan.windowSeq,
        payload: payload as unknown as WireJsonObject,
        ...(profile ? { profile: profile as unknown as WireJsonObject } : {}),
        ...extraTopLevel,
    } as RuntimeResponseV1;
}

export class ClientResponsePostProcessor {
    public static buildResponse(
        plan: LogicalPivotPlan,
        executed: ClientExecutedDataset
    ): RuntimeResponseV1 {
        if (plan.kind === 'filter_options') {
            return ClientResponsePostProcessor.buildFilterOptionsResponse(plan, executed);
        }
        if (plan.kind === 'chart') {
            return ClientResponsePostProcessor.buildChartResponse(plan, executed);
        }
        if (plan.kind === 'detail') {
            return ClientResponsePostProcessor.buildDetailResponse(plan, executed);
        }
        if (plan.kind === 'drill') {
            return ClientResponsePostProcessor.buildDrillResponse(plan, executed);
        }
        if (plan.kind === 'export') {
            return ClientResponsePostProcessor.buildExportResponse(plan, executed);
        }
        return ClientResponsePostProcessor.buildDataResponse(plan, executed);
    }

    public static buildDataResponse(
        plan: LogicalPivotPlan,
        executed: ClientExecutedDataset
    ): RuntimeResponseV1 {
        const formulaErrors: { [key: string]: string } = {};

        let built: { rows: Array<{ [key: string]: unknown }>; grandTotalRow: { [key: string]: unknown } | null };
        if (plan.viewMode === 'tree') {
            built = buildTreeVisibleRows(executed, plan, formulaErrors);
        } else if (plan.viewMode === 'report') {
            built = buildReportVisibleRows(executed, plan, formulaErrors);
        } else if (plan.viewMode === 'table') {
            built = buildFlatTableVisibleRows(executed, plan);
        } else {
            built = buildPivotVisibleRows(executed, plan, formulaErrors);
        }

        const measureAdjusted = expandRowsMeasurePlacement(
            built.rows,
            built.grandTotalRow,
            plan,
            executed.colCombinations
        );

        const hasPinnedGrandTotal = Boolean(plan.includeGrandTotal && measureAdjusted.grandTotalRow);
        const totalRows = measureAdjusted.rows.length + (hasPinnedGrandTotal ? 1 : 0);
        const windowedRows = measureAdjusted.rows.slice(plan.start, plan.end + 1);
        const pageRows = [...windowedRows];
        if (hasPinnedGrandTotal && measureAdjusted.grandTotalRow) {
            if (plan.grandTotalPosition === 'top') {
                pageRows.unshift(measureAdjusted.grandTotalRow);
            } else {
                pageRows.push(measureAdjusted.grandTotalRow);
            }
        }

        const { allColumnObjects, allColumnIds, centerColumns, colSchema } = buildColumnListAndSchema(
            plan,
            executed.colCombinations,
            measureAdjusted.rows
        );
        const colorScaleStats = computeColorScaleStats(measureAdjusted.rows, centerColumns);
        const dataWithSentinel = [
            { _path: '__color_scale_stats__', _colorScaleStats: colorScaleStats },
            ...pageRows,
        ];

        const payload: any = {
            data: dataWithSentinel as unknown as JsonObject[],
            rowCount: totalRows,
            total_rows: measureAdjusted.rows.length,
            columns: allColumnObjects as unknown as JsonObject[],
            colSchema: colSchema as unknown as JsonObject,
            col_schema: colSchema as unknown as JsonObject,
            dataOffset: plan.start,
            dataVersion: plan.windowSeq || plan.version || 1,
            version: plan.windowSeq || plan.version || 1,
            windowSeq: plan.windowSeq || plan.version || 1,
            stateEpoch: plan.stateEpoch,
            formulaErrors: Object.keys(formulaErrors).length > 0 ? formulaErrors : null,
            colorScaleStats: colorScaleStats as unknown as JsonObject,
            executionMode: 'client',
            clientEngine: executed.engineUsed,
        };

        return buildEnvelope(
            plan,
            'data',
            'data',
            payload,
            {
                data: pageRows,
                total_rows: measureAdjusted.rows.length,
                rowCount: totalRows,
                columns: allColumnIds,
                col_schema: colSchema,
                dataOffset: plan.start,
                dataVersion: plan.windowSeq || plan.version || 1,
                grandTotalRow: measureAdjusted.grandTotalRow,
                colorScaleStats,
                formulaErrors,
                executionMode: 'client',
                clientEngine: executed.engineUsed,
            },
            {
                totalMs: executed.executionMs,
                sql: executed.lastSql,
                networkRoundtrips: 0,
            }
        );
    }

    public static buildFilterOptionsResponse(
        plan: LogicalPivotPlan,
        executed: ClientExecutedDataset
    ): RuntimeResponseV1 {
        const reqPayload = (plan.rawRequest.payload && typeof plan.rawRequest.payload === 'object')
            ? (plan.rawRequest.payload as { [key: string]: unknown })
            : {};
        const field = typeof (reqPayload.columnId ?? reqPayload.column_id ?? reqPayload.field) === 'string'
            ? String(reqPayload.columnId ?? reqPayload.column_id ?? reqPayload.field).trim()
            : (plan.rowFields[0] || '');
        const search = typeof reqPayload.search === 'string' ? reqPayload.search.trim().toLowerCase() : '';
        const offset = Number.isFinite(Number(reqPayload.offset)) ? Math.max(0, Number(reqPayload.offset)) : 0;
        const limit = Number.isFinite(Number(reqPayload.limit)) ? Math.max(1, Number(reqPayload.limit)) : 250;

        const baseRows = filterDatasetRows(executed.allRows, plan.filters, field);
        const distinctSet = new Set<string>();
        let hasNull = false;
        for (const row of baseRows) {
            const raw = row[field];
            if (raw === null || raw === undefined) {
                hasNull = true;
            } else {
                const str = String(raw);
                if (!search || str.toLowerCase().includes(search)) {
                    distinctSet.add(str);
                }
            }
        }
        const sortedOptions = Array.from(distinctSet).sort((a, b) =>
            a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' })
        );
        if (hasNull && (!search || 'null'.includes(search))) {
            sortedOptions.unshift('__null__');
        }
        const paged = sortedOptions.slice(offset, offset + limit);
        const hasMore = offset + limit < sortedOptions.length;

        const payload: any = {
            columnId: field,
            field,
            options: paged,
            search,
            offset,
            limit,
            total: sortedOptions.length,
            hasMore,
        };

        return buildEnvelope(plan, 'filter_options', 'ok', payload, {
            field,
            columnId: field,
            options: paged,
            total: sortedOptions.length,
            offset,
            limit,
            hasMore,
        });
    }

    public static buildChartResponse(
        plan: LogicalPivotPlan,
        executed: ClientExecutedDataset
    ): RuntimeResponseV1 {
        const dataResp = ClientResponsePostProcessor.buildDataResponse(
            { ...plan, start: 0, end: 99 },
            executed
        );
        const reqPayload = (plan.rawRequest.payload && typeof plan.rawRequest.payload === 'object')
            ? (plan.rawRequest.payload as { [key: string]: unknown })
            : {};
        const requestId = typeof reqPayload.requestId === 'string'
            ? reqPayload.requestId
            : (typeof plan.rawRequest.requestId === 'string' ? plan.rawRequest.requestId : 'chart_req');
        const rawData = Array.isArray((dataResp as any).data)
            ? ((dataResp as any).data)
            : [];
        const rows = rawData.filter(
            (r: any) => !r._isTotal && !r.__isGrandTotal__ && r._path !== '__color_scale_stats__'
        );
        const dataPayload = dataResp.payload || {};

        const chartPayload: any = {
            requestId,
            paneId: typeof reqPayload.pane_id === 'string' ? reqPayload.pane_id : (typeof reqPayload.paneId === 'string' ? reqPayload.paneId : null),
            requestSignature: typeof reqPayload.request_signature === 'string' ? reqPayload.request_signature : null,
            rows: rows as unknown as JsonObject[],
            columns: (dataPayload.columns || []) as JsonObject[],
            colSchema: (dataPayload.colSchema || null) as JsonObject | null,
            totalRows: rows.length,
            dataVersion: plan.windowSeq || plan.version || 1,
            stateEpoch: plan.stateEpoch,
            windowSeq: plan.windowSeq,
        };

        return buildEnvelope(plan, 'chart', 'chart_data', chartPayload, {
            chartData: chartPayload,
        });
    }

    public static executeDrillThroughQuery(
        allRows: Array<{ [key: string]: unknown }>,
        plan: LogicalPivotPlan,
        query: DrillThroughQuery
    ): DrillThroughResult {
        const filtered = filterDatasetRows(allRows, plan.filters);
        const pathParts = query.rowPath ? String(query.rowPath).split('|||').filter(Boolean) : [];
        const matched = filtered.filter((row) => {
            for (let i = 0; i < pathParts.length && i < query.rowFields.length; i += 1) {
                const rf = query.rowFields[i];
                const expected = pathParts[i];
                if (expected === '__grand_total__' || expected === '__subtotal__') continue;
                if (String(row[rf] ?? 'null') !== expected) return false;
            }
            if (query.colField && query.colVal !== undefined && query.colVal !== '') {
                const colFields = query.colField.split('_').filter(Boolean);
                const colVals = String(query.colVal).split('_');
                if (colFields.length === 1) {
                    if (String(row[query.colField] ?? 'null') !== String(query.colVal)) return false;
                } else if (colFields.length === colVals.length) {
                    for (let c = 0; c < colFields.length; c += 1) {
                        if (String(row[colFields[c]] ?? 'null') !== colVals[c]) return false;
                    }
                }
            }
            if (query.filterText && query.filterText.trim()) {
                const needle = query.filterText.trim().toLowerCase();
                const hit = Object.values(row).some(
                    (v) => v !== null && v !== undefined && String(v).toLowerCase().includes(needle)
                );
                if (!hit) return false;
            }
            return true;
        });

        if (query.sortCol) {
            const sortCol = query.sortCol;
            const desc = query.sortDir === 'desc';
            matched.sort((a, b) => compareRowValues(a, b, { id: sortCol, desc }, true));
        }

        const page = Number.isFinite(Number(query.page)) ? Math.max(0, Number(query.page)) : 0;
        const pageSize = Number.isFinite(Number(query.pageSize)) ? Math.max(1, Number(query.pageSize)) : 100;
        const startIdx = page * pageSize;
        const pagedRows = matched.slice(startIdx, startIdx + pageSize);
        const columns = matched[0]
            ? Object.keys(matched[0]).filter((k) => !k.startsWith('_'))
            : [];

        return {
            rows: pagedRows,
            totalRows: matched.length,
            page,
            pageSize,
            columns,
        };
    }

    public static buildDetailResponse(
        plan: LogicalPivotPlan,
        executed: ClientExecutedDataset
    ): RuntimeResponseV1 {
        const reqPayload = (plan.rawRequest.payload && typeof plan.rawRequest.payload === 'object')
            ? (plan.rawRequest.payload as { [key: string]: unknown })
            : {};
        const detailReq = (reqPayload.detailRequest && typeof reqPayload.detailRequest === 'object')
            ? (reqPayload.detailRequest as { [key: string]: unknown })
            : reqPayload;
        const rowPath = typeof detailReq.rowPath === 'string'
            ? detailReq.rowPath
            : (typeof detailReq.row_path === 'string' ? detailReq.row_path : '');
        const page = Number.isFinite(Number(detailReq.page)) ? Number(detailReq.page) : 0;
        const pageSize = Number.isFinite(Number(detailReq.pageSize ?? detailReq.page_size))
            ? Number(detailReq.pageSize ?? detailReq.page_size)
            : 100;
        const sortCol = typeof detailReq.sortCol === 'string' ? detailReq.sortCol : undefined;
        const sortDir = detailReq.sortDir === 'desc' ? 'desc' : 'asc';
        const filterText = typeof detailReq.filterText === 'string' ? detailReq.filterText : '';

        const result = ClientResponsePostProcessor.executeDrillThroughQuery(executed.allRows, plan, {
            rowPath,
            rowFields: plan.rowFields,
            page,
            pageSize,
            sortCol,
            sortDir,
            filterText,
        });

        const detailPayload: any = {
            rowPath,
            rowFields: plan.rowFields,
            rows: result.rows as unknown as JsonObject[],
            totalRows: result.totalRows,
            page: result.page,
            pageSize: result.pageSize,
            columns: result.columns,
            sortCol: sortCol || null,
            sortDir,
            filterText,
        };

        return buildEnvelope(plan, 'detail', 'detail_data', detailPayload, {
            detail: detailPayload,
        });
    }

    public static buildDrillResponse(
        plan: LogicalPivotPlan,
        executed: ClientExecutedDataset
    ): RuntimeResponseV1 {
        const reqPayload = (plan.rawRequest.payload && typeof plan.rawRequest.payload === 'object')
            ? (plan.rawRequest.payload as { [key: string]: unknown })
            : {};
        const rowPath = typeof reqPayload.row_path === 'string'
            ? reqPayload.row_path
            : (typeof reqPayload.rowPath === 'string' ? reqPayload.rowPath : '');
        const result = ClientResponsePostProcessor.executeDrillThroughQuery(executed.allRows, plan, {
            rowPath,
            rowFields: plan.rowFields,
            colField: typeof reqPayload.col_field === 'string' ? reqPayload.col_field : undefined,
            colVal: typeof reqPayload.col_val === 'string' ? reqPayload.col_val : undefined,
            page: Number.isFinite(Number(reqPayload.page)) ? Number(reqPayload.page) : 0,
            pageSize: Number.isFinite(Number(reqPayload.page_size)) ? Number(reqPayload.page_size) : 100,
            sortCol: typeof reqPayload.sort_col === 'string' ? reqPayload.sort_col : undefined,
            sortDir: reqPayload.sort_dir === 'desc' ? 'desc' : 'asc',
            filterText: typeof reqPayload.filter_text === 'string' ? reqPayload.filter_text : undefined,
        });

        const drillPayload: any = {
            rows: result.rows as unknown as JsonObject[],
            totalRows: result.totalRows,
            total_rows: result.totalRows,
            page: result.page,
            pageSize: result.pageSize,
            columns: result.columns,
            rowPath,
        };

        return buildEnvelope(plan, 'drill', 'drillthrough', drillPayload, {
            drill_records: result.rows,
            rows: result.rows,
            total_rows: result.totalRows,
            page: result.page,
            page_size: result.pageSize,
            columns: result.columns,
            row_path: rowPath,
        });
    }

    public static buildExportResponse(
        plan: LogicalPivotPlan,
        executed: ClientExecutedDataset
    ): RuntimeResponseV1 {
        const reqPayload = (plan.rawRequest.payload && typeof plan.rawRequest.payload === 'object')
            ? (plan.rawRequest.payload as { [key: string]: unknown })
            : {};
        const format = typeof reqPayload.format === 'string' ? reqPayload.format.trim().toLowerCase() : 'csv';
        const fullDataPlan: LogicalPivotPlan = {
            ...plan,
            start: 0,
            end: Math.max(100_000, executed.filteredRows.length + 1000),
        };
        const dataResp = ClientResponsePostProcessor.buildDataResponse(fullDataPlan, executed);
        const rows = Array.isArray((dataResp as any).data)
            ? ((dataResp as any).data)
            : [];
        const columns = Array.isArray((dataResp as any).columns)
            ? ((dataResp as any).columns)
            : [];

        let content = '';
        let mimeType = 'text/csv;charset=utf-8;';
        const filename = typeof reqPayload.filename === 'string' && reqPayload.filename.trim()
            ? reqPayload.filename.trim()
            : `pivot-export.${format === 'tsv' ? 'tsv' : (format === 'json' ? 'json' : (format === 'html' ? 'html' : 'csv'))}`;

        if (format === 'json') {
            content = JSON.stringify(rows, null, 2);
            mimeType = 'application/json;charset=utf-8;';
        } else if (format === 'html') {
            const headerHtml = columns.map((c: any) => `<th>${escapeHtml(c)}</th>`).join('');
            const bodyHtml = rows
                .map((r: any) => `<tr>${columns.map((c: any) => `<td>${escapeHtml(String(r[c] ?? ''))}</td>`).join('')}</tr>`)
                .join('\n');
            content = `<table><thead><tr>${headerHtml}</tr></thead><tbody>${bodyHtml}</tbody></table>`;
            mimeType = 'text/html;charset=utf-8;';
        } else {
            const sep = format === 'tsv' ? '\t' : ',';
            mimeType = format === 'tsv' ? 'text/tab-separated-values;charset=utf-8;' : 'text/csv;charset=utf-8;';
            const escapeCell = (val: unknown): string => {
                if (val === null || val === undefined) return '';
                const str = Array.isArray(val) ? val.join(', ') : String(val);
                if (str.includes(sep) || str.includes('"') || str.includes('\n')) {
                    return `"${str.replace(/"/g, '""')}"`;
                }
                return str;
            };
            const lines = [
                columns.map((c: any) => escapeCell(c)).join(sep),
                ...rows.map((r: any) => columns.map((c: any) => escapeCell(r[c])).join(sep)),
            ];
            content = lines.join('\n');
        }

        const base64Data = encodeUtf8Base64(content);
        const exportPayload: any = {
            format,
            filename,
            contentType: mimeType,
            data: base64Data,
            rows: rows.length,
            columns: columns.length,
        };

        return buildEnvelope(plan, 'export', 'ok', exportPayload, {
            format,
            filename,
            mimeType,
            content,
            rowCount: rows.length,
        });
    }
}

function encodeUtf8Base64(str: string): string {
    try {
        if (typeof Buffer !== 'undefined') {
            return Buffer.from(str, 'utf-8').toString('base64');
        }
        if (typeof btoa === 'function') {
            return btoa(unescape(encodeURIComponent(str)));
        }
    } catch (_err) {
        // ignore fallback
    }
    return '';
}

function escapeHtml(value: string): string {
    return value
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}
