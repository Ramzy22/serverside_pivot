/** @param {any} [column] @returns {any} */
const getRowSpanGroupIndex = (column) => {
    /** @type {any} */
    const meta = column && column.columnDef ? column.columnDef.meta : null;
    /** @type {any} */
    const index = meta ? Number(meta.rowSpanGroupIndex) : Number.NaN;
    return Number.isFinite(index) ? index : Number.MAX_SAFE_INTEGER;
};

/** @param {any} [row] @returns {any} */
const getRowId = (row) => (row && row.id != null ? String(row.id) : null);

/** @param {any} [row] @returns {any} */
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

/** @param {any} [value] @returns {any} */
const normalizeComparableValue = (value) => {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') {
        if (!Number.isFinite(value)) return null;
        return `number:${value}`;
    }
    if (typeof value === 'boolean') return `boolean:${value}`;
    if (value instanceof Date) return `date:${value.getTime()}`;
    if (typeof value === 'object') return null;

    /** @type {any} */
    const text = String(value);
    if (text.length === 0) return null;
    return `text:${text}`;
};

/** @param {any} [row] @param {any} [column] @returns {any} */
const getCellComparableValue = (row, column) => {
    if (!row || !column || !column.id) return null;
    /** @type {any} */
    const rawValue = typeof row.getValue === 'function'
        ? row.getValue(column.id)
        : (row.original ? row.original[column.id] : undefined);
    return normalizeComparableValue(rawValue);
};

/** @param {any} [plan] @param {any} [row] @param {any} [column] @param {any} [state] @returns {any} */
const setCellState = (plan, row, column, state) => {
    /** @type {any} */
    const rowId = getRowId(row);
    if (!rowId || !column || !column.id) return;
    if (!plan.has(rowId)) plan.set(rowId, new Map());
    plan.get(rowId).set(column.id, state);
};

/** @param {any} [previousRow] @param {any} [nextRow] @param {any} [column] @param {any} [rowSpanColumns] @returns {any} */
const rowsCanSpanForColumn = (previousRow, nextRow, column, rowSpanColumns) => {
    if (!previousRow || !nextRow || !column) return false;
    if (isTotalRow(previousRow) || isTotalRow(nextRow)) return false;

    /** @type {any} */
    const currentValue = getCellComparableValue(previousRow, column);
    if (currentValue === null || currentValue !== getCellComparableValue(nextRow, column)) {
        return false;
    }

    /** @type {any} */
    const groupIndex = getRowSpanGroupIndex(column);
    for (const parentColumn of rowSpanColumns) {
        if (parentColumn.id === column.id) break;
        if (getRowSpanGroupIndex(parentColumn) >= groupIndex) continue;
        /** @type {any} */
        const parentValue = getCellComparableValue(previousRow, parentColumn);
        if (parentValue === null || parentValue !== getCellComparableValue(nextRow, parentColumn)) {
            return false;
        }
    }

    return true;
};

/** @param {any} [columns] @returns {any} */
export const collectRowSpanColumns = (columns) => (
    (Array.isArray(columns) ? columns : [])
        .filter((column) => Boolean(column && column.columnDef && column.columnDef.meta && column.columnDef.meta.rowSpan))
        .sort((left, right) => getRowSpanGroupIndex(left) - getRowSpanGroupIndex(right))
);

/** @param {any} p0 @returns {any} */
export const buildRowSpanPlan = ({ rowEntries, rowSpanColumns }) => {
    /** @type {any} */
    const plan = new Map();
    if (!Array.isArray(rowEntries) || rowEntries.length < 2 || !Array.isArray(rowSpanColumns) || rowSpanColumns.length === 0) {
        return plan;
    }

    rowSpanColumns.forEach((column) => {
        let runStart = 0;
        /** @type {any} */
        let runHeight = Number(rowEntries[0] && rowEntries[0].size) || 0;

        /** @param {any} [endIndex] @returns {any} */
        const commitRun = (endIndex) => {
            /** @type {any} */
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

/** @param {any} [rowSpanPlan] @param {any} [row] @param {any} [column] @returns {any} */
export const getRowSpanState = (rowSpanPlan, row, column) => {
    if (!rowSpanPlan || !row || !column) return null;
    /** @type {any} */
    const rowId = getRowId(row);
    if (!rowId) return null;
    /** @type {any} */
    const rowPlan = rowSpanPlan.get(rowId);
    return rowPlan ? (rowPlan.get(column.id) || null) : null;
};
