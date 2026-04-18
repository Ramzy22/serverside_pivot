import * as XLSX from 'xlsx';
import { saveAs } from 'file-saver';

const SKIP_COL_IDS = new Set(['__row_number__']);

/**
 * Build an array-of-arrays (AOA) suitable for XLSX export from the table's
 * row model and header groups.
 */
export function buildExportAoa(allRows, table) {
    const headerGroups = table.getHeaderGroups();

    const lastHeaderGroup = headerGroups[headerGroups.length - 1];
    const leafHeaders = (lastHeaderGroup && lastHeaderGroup.headers != null ? lastHeaderGroup.headers : [])
        .filter(h => !SKIP_COL_IDS.has(h.column.id) && !h.isPlaceholder);

    const leafCount = leafHeaders.length;

    const headerAoaRows = [];
    const allMerges = [];

    headerGroups.forEach((hg, rowIdx) => {
        const aoaRow = new Array(leafCount).fill('');
        let leafPos = 0;
        hg.headers.forEach(h => {
            if (SKIP_COL_IDS.has(h.column.id)) return;
            const span = (h.colSpan != null ? h.colSpan : 1);
            if (!h.isPlaceholder) {
                const colDef = h.column.columnDef;
                let headerText = '';
                if (typeof colDef.header === 'string') {
                    headerText = colDef.header;
                } else if (typeof h.column.id === 'string') {
                    headerText = h.column.id
                        .replace(/^group_/, '')
                        .replace(/\|\|\|/g, ' > ');
                }
                aoaRow[leafPos] = headerText;
                if (span > 1 && rowIdx < headerGroups.length - 1) {
                    allMerges.push({ s: { r: rowIdx, c: leafPos }, e: { r: rowIdx, c: leafPos + span - 1 } });
                }
            }
            leafPos += span;
        });
        headerAoaRows.push(aoaRow);
    });

    const dedupedHeaderRows = headerAoaRows.length > 1
        ? headerAoaRows
        : headerAoaRows;

    const colWidths = leafHeaders.map(h => {
        const colDef = h.column.columnDef;
        return typeof colDef.header === 'string' ? colDef.header.length : (h.column.id != null ? h.column.id : '').length;
    });

    const dataRows = allRows.map((r, index) => {
        // Support both TanStack row objects (have .original) and raw data objects
        const rowData = r.original !== undefined ? r.original : r;
        const rowIndex = r.index !== undefined ? r.index : index;
        return leafHeaders.map((h, ci) => {
            const col = h.column;
            const colId = col.id;
            const colDef = col.columnDef;

            let val;
            if (colId === 'hierarchy') {
                const depth = (rowData && rowData.depth != null ? rowData.depth : (r.depth != null ? r.depth : 0));
                const label = (rowData && rowData._isTotal) ? ((rowData._id != null) ? rowData._id : 'Total') : ((rowData && rowData._id != null) ? rowData._id : '');
                val = '\u00A0\u00A0'.repeat(depth) + label;
            } else if (typeof colDef.accessorFn === 'function') {
                val = colDef.accessorFn(rowData, rowIndex);
            } else if (colDef.accessorKey) {
                val = rowData && rowData[colDef.accessorKey];
            } else {
                val = '';
            }

            if (val === undefined || val === null) val = '';
            const cellLen = String(val).length;
            if (cellLen > colWidths[ci]) colWidths[ci] = cellLen;

            return val;
        });
    });

    const wsCols = colWidths.map(w => ({ wch: Math.min(Math.max(w + 2, 8), 60) }));

    return {
        aoa: [...dedupedHeaderRows, ...dataRows],
        merges: allMerges,
        wsCols,
        headerRowCount: dedupedHeaderRows.length,
    };
}

/**
 * Export the current pivot table to CSV or XLSX.
 */
export function exportPivotTable(table, rowCount, rawRowsOverride = null) {
    const XLSX_LIMIT = 500000;
    // rawRowsOverride: pass loadedRows (raw data objects) in server-side mode to export all cached data
    const allRows = rawRowsOverride || table.getRowModel().rows;

    const isCSV = (rowCount || 0) > XLSX_LIMIT;

    if (isCSV) {
        const SKIP_CSV = new Set(['__row_number__']);
        const leafCols = table.getVisibleLeafColumns().filter(c => !SKIP_CSV.has(c.id));

        const escape = (v) => {
            if (v == null) return '';
            const s = String(v);
            return (s.includes(',') || s.includes('"') || s.includes('\n'))
                ? `"${s.replace(/"/g, '""')}"` : s;
        };
        const header = leafCols.map(c => {
            const h = (c.columnDef && c.columnDef.header);
            return escape(typeof h === 'string' ? h : (c.id != null ? c.id : ''));
        }).join(',');
        const lines = allRows.map((r, index) => {
            const rowData = r.original !== undefined ? r.original : r;
            const rowIndex = r.index !== undefined ? r.index : index;
            return leafCols.map(c => {
                if (c.id === 'hierarchy') {
                    const depth = (rowData && rowData.depth != null ? rowData.depth : (r.depth != null ? r.depth : 0));
                    return escape('  '.repeat(depth) + (rowData && rowData._id != null ? rowData._id : ''));
                }
                const val = typeof (c.columnDef && c.columnDef.accessorFn) === 'function'
                    ? c.columnDef.accessorFn(rowData, rowIndex)
                    : ((c.columnDef && c.columnDef.accessorKey) ? (rowData && rowData[c.columnDef.accessorKey]) : '');
                return escape(val != null ? val : '');
            }).join(',');
        });
        const blob = new Blob([[header, ...lines].join('\n')], { type: 'text/csv;charset=utf-8;' });
        saveAs(blob, 'pivot.csv');
    } else {
        const { aoa, merges, wsCols } = buildExportAoa(allRows, table);
        const ws = XLSX.utils.aoa_to_sheet(aoa);
        if (merges.length > 0) ws['!merges'] = merges;
        if (wsCols && wsCols.length > 0) ws['!cols'] = wsCols;
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, 'Pivot');
        const buf = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
        saveAs(new Blob([buf], { type: 'application/octet-stream' }), 'pivot.xlsx');
    }
}
