// Build a TSV string directly from server-side raw data objects.
// `visibleColumns` is the TanStack leaf-column list (without __row_number__).
/**
 * @typedef {Record<string, unknown> & {depth?: number, _id?: string}} RawTsvRow
 * @typedef {{id: string, columnDef: {header?: unknown, accessorFn?: (row: RawTsvRow, index: number) => unknown, accessorKey?: string}}} RawTsvColumn
 * @param {RawTsvRow[] | null | undefined} rawRows
 * @param {boolean} withHeaders
 * @param {RawTsvColumn[] | null | undefined} visibleColumns
 * @returns {string | null}
 */
export function buildTsvFromRawRows(rawRows, withHeaders, visibleColumns) {
    if (!Array.isArray(rawRows) || rawRows.length === 0 || !Array.isArray(visibleColumns)) return null;

    let tsv = '';
    if (withHeaders) {
        tsv += visibleColumns
            .map((column) => (typeof column.columnDef.header === 'string' ? column.columnDef.header : column.id))
            .join('\t') + '\n';
    }

    rawRows.forEach((rawRow, rowIndex) => {
        if (!rawRow) return;
        const values = visibleColumns.map((column) => {
            if (column.id === 'hierarchy') {
                const depth = rawRow.depth != null ? rawRow.depth : 0;
                return '  '.repeat(depth) + (rawRow._id || '');
            }
            if (typeof column.columnDef.accessorFn === 'function') {
                const value = column.columnDef.accessorFn(rawRow, rowIndex);
                return value != null ? String(value) : '';
            }
            const value = column.columnDef.accessorKey ? rawRow[column.columnDef.accessorKey] : undefined;
            return value != null ? String(value) : '';
        });
        tsv += values.join('\t') + '\n';
    });

    return tsv;
}
