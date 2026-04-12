const getRowSpanGroupIndex = (column) => {
    const meta = column && column.columnDef ? column.columnDef.meta : null;
    const index = meta ? Number(meta.rowSpanGroupIndex) : Number.NaN;
    return Number.isFinite(index) ? index : Number.MAX_SAFE_INTEGER;
};

const getRowId = (row) => (row && row.id != null ? String(row.id) : null);

const isTotalRow = (row) => Boolean(
    row
    && row.original
    && (
        row.original._isTotal
        || row.original.__isGrandTotal__
        || row.original._path === '__grand_total__'
        || row.original._id === 'Grand Total'
    )
);

const normalizeComparableValue = (value) => {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') {
        if (!Number.isFinite(value)) return null;
        return `number:${value}`;
    }
    if (typeof value === 'boolean') return `boolean:${value}`;
    if (value instanceof Date) return `date:${value.getTime()}`;
    if (typeof value === 'object') return null;

    const text = String(value);
    if (text.length === 0) return null;
    return `text:${text}`;
};

const getCellComparableValue = (row, column) => {
    if (!row || !column || !column.id) return null;
    const rawValue = typeof row.getValue === 'function'
        ? row.getValue(column.id)
        : (row.original ? row.original[column.id] : undefined);
    return normalizeComparableValue(rawValue);
};

const setCellState = (plan, row, column, state) => {
    const rowId = getRowId(row);
    if (!rowId || !column || !column.id) return;
    if (!plan.has(rowId)) plan.set(rowId, new Map());
    plan.get(rowId).set(column.id, state);
};

const rowsCanSpanForColumn = (previousRow, nextRow, column, rowSpanColumns) => {
    if (!previousRow || !nextRow || !column) return false;
    if (isTotalRow(previousRow) || isTotalRow(nextRow)) return false;

    const currentValue = getCellComparableValue(previousRow, column);
    if (currentValue === null || currentValue !== getCellComparableValue(nextRow, column)) {
        return false;
    }

    const groupIndex = getRowSpanGroupIndex(column);
    for (const parentColumn of rowSpanColumns) {
        if (parentColumn.id === column.id) break;
        if (getRowSpanGroupIndex(parentColumn) >= groupIndex) continue;
        const parentValue = getCellComparableValue(previousRow, parentColumn);
        if (parentValue === null || parentValue !== getCellComparableValue(nextRow, parentColumn)) {
            return false;
        }
    }

    return true;
};

export const collectRowSpanColumns = (columns) => (
    (Array.isArray(columns) ? columns : [])
        .filter((column) => Boolean(column && column.columnDef && column.columnDef.meta && column.columnDef.meta.rowSpan))
        .sort((left, right) => getRowSpanGroupIndex(left) - getRowSpanGroupIndex(right))
);

export const buildRowSpanPlan = ({ rowEntries, rowSpanColumns }) => {
    const plan = new Map();
    if (!Array.isArray(rowEntries) || rowEntries.length < 2 || !Array.isArray(rowSpanColumns) || rowSpanColumns.length === 0) {
        return plan;
    }

    rowSpanColumns.forEach((column) => {
        let runStart = 0;
        let runHeight = Number(rowEntries[0] && rowEntries[0].size) || 0;

        const commitRun = (endIndex) => {
            const runLength = endIndex - runStart;
            if (runLength <= 1) return;
            const firstEntry = rowEntries[runStart];
            setCellState(plan, firstEntry.row, column, {
                rowSpan: runLength,
                height: runHeight,
                hidden: false,
            });
            for (let index = runStart + 1; index < endIndex; index += 1) {
                setCellState(plan, rowEntries[index].row, column, {
                    rowSpan: 0,
                    height: Number(rowEntries[index] && rowEntries[index].size) || 0,
                    hidden: true,
                });
            }
        };

        for (let index = 1; index <= rowEntries.length; index += 1) {
            const previousEntry = rowEntries[index - 1];
            const nextEntry = rowEntries[index];
            if (
                nextEntry
                && rowsCanSpanForColumn(previousEntry.row, nextEntry.row, column, rowSpanColumns)
            ) {
                runHeight += Number(nextEntry.size) || 0;
                continue;
            }

            commitRun(index);
            runStart = index;
            runHeight = nextEntry ? (Number(nextEntry.size) || 0) : 0;
        }
    });

    return plan;
};

export const getRowSpanState = (rowSpanPlan, row, column) => {
    if (!rowSpanPlan || !row || !column) return null;
    const rowId = getRowId(row);
    if (!rowId) return null;
    const rowPlan = rowSpanPlan.get(rowId);
    return rowPlan ? (rowPlan.get(column.id) || null) : null;
};
