/** Small, deterministic SSRM backend over the shared qualification rows. */

export type DataRow = Record<string, unknown>;

type Column = { id: string; field?: string; aggFunc?: string };
type Sort = { colId: string; sort: 'asc' | 'desc' };
type Filter = { filterType?: string; values?: unknown[]; type?: string; filter?: unknown; filterTo?: unknown; operator?: string; conditions?: Filter[] };

export type QueryRequest = {
  startRow: number;
  endRow: number;
  rowGroupCols: Column[];
  groupKeys: string[];
  pivotCols: Column[];
  valueCols: Column[];
  pivotMode: boolean;
  filterModel: Record<string, Filter>;
  sortModel: Sort[];
};

export type QueryResult = {
  rowData: DataRow[];
  rowCount: number;
  pivotResultFields?: string[];
};

const fieldOf = (column: Column): string => column.field || column.id;
const keyOf = (value: unknown): string => value == null ? '' : String(value);

function matchesOne(value: unknown, filter: Filter): boolean {
  if (filter.values) return filter.values.some((candidate) => candidate === value);
  if (filter.conditions?.length) {
    const values = filter.conditions.map((condition) => matchesOne(value, condition));
    return filter.operator === 'OR' ? values.some(Boolean) : values.every(Boolean);
  }
  const source = keyOf(value).toLocaleLowerCase('en-GB');
  const target = keyOf(filter.filter).toLocaleLowerCase('en-GB');
  switch (filter.type) {
    case 'equals': return source === target;
    case 'notEqual': return source !== target;
    case 'contains': return source.includes(target);
    case 'notContains': return !source.includes(target);
    case 'startsWith': return source.startsWith(target);
    case 'endsWith': return source.endsWith(target);
    case 'greaterThan': return Number(value) > Number(filter.filter);
    case 'lessThan': return Number(value) < Number(filter.filter);
    case 'inRange': return Number(value) >= Number(filter.filter) && Number(value) <= Number(filter.filterTo);
    default: return true;
  }
}

function aggregate(rows: DataRow[], column: Column): number | null {
  const values = rows.map((row) => row[fieldOf(column)]).filter((value): value is number => typeof value === 'number');
  if (!values.length) return null;
  switch (column.aggFunc || 'sum') {
    case 'avg': return values.reduce((sum, value) => sum + value, 0) / values.length;
    case 'min': return Math.min(...values);
    case 'max': return Math.max(...values);
    case 'count': return values.length;
    default: return values.reduce((sum, value) => sum + value, 0);
  }
}

function compare(left: unknown, right: unknown): number {
  if (left == null && right == null) return 0;
  if (left == null) return -1;
  if (right == null) return 1;
  if (typeof left === 'number' && typeof right === 'number') return left - right;
  return new Intl.Collator('en-GB', { numeric: true }).compare(String(left), String(right));
}

export function queryRows(sourceRows: DataRow[], request: QueryRequest): QueryResult {
  const filtered = sourceRows.filter((row) =>
    Object.entries(request.filterModel || {}).every(([field, filter]) => matchesOne(row[field], filter)),
  );
  const routed = filtered.filter((row) => request.groupKeys.every((key, depth) =>
    keyOf(row[fieldOf(request.rowGroupCols[depth])]) === key,
  ));
  const grouping = request.rowGroupCols[request.groupKeys.length];
  const pivotFields = request.pivotMode && request.pivotCols.length
    ? Array.from(new Set(filtered.map((row) => request.pivotCols.map((column) => keyOf(row[fieldOf(column)])).join('|'))))
      .sort((left, right) => compare(left, right))
      .flatMap((key) => request.valueCols.map((column) => `${key}|${column.id}`))
    : undefined;
  let result: DataRow[];
  if (grouping) {
    const groups = new Map<string, DataRow[]>();
    for (const row of routed) {
      const key = keyOf(row[fieldOf(grouping)]);
      const group = groups.get(key) || [];
      group.push(row);
      groups.set(key, group);
    }
    result = Array.from(groups, ([key, rows]) => {
      const output: DataRow = { [fieldOf(grouping)]: key };
      if (pivotFields) {
        for (const pivotField of pivotFields) {
          const parts = pivotField.split('|');
          const column = request.valueCols.find((item) => item.id === parts.at(-1))!;
          const matching = rows.filter((row) => request.pivotCols.every((item, index) => keyOf(row[fieldOf(item)]) === parts[index]));
          output[pivotField] = aggregate(matching, column);
        }
      } else {
        for (const column of request.valueCols) output[column.id] = aggregate(rows, column);
      }
      return output;
    });
  } else {
    result = routed.map((row) => {
      const output = { ...row };
      if (pivotFields) {
        for (const pivotField of pivotFields) {
          const parts = pivotField.split('|');
          const column = request.valueCols.find((item) => item.id === parts.at(-1))!;
          output[pivotField] = request.pivotCols.every((item, index) => keyOf(row[fieldOf(item)]) === parts[index])
            ? aggregate([row], column) : null;
        }
      }
      return output;
    });
  }
  if (request.sortModel?.length) {
    result.sort((left, right) => {
      for (const sort of request.sortModel) {
        const order = compare(left[sort.colId], right[sort.colId]);
        if (order) return sort.sort === 'desc' ? -order : order;
      }
      return 0;
    });
  }
  return {
    rowData: result.slice(request.startRow, request.endRow),
    rowCount: result.length,
    ...(pivotFields ? { pivotResultFields: pivotFields } : {}),
  };
}
