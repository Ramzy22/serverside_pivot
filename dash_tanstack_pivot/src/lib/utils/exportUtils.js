import { saveAs } from 'file-saver';

const SKIP_COL_IDS = new Set(['__row_number__']);

const UNITLESS_CSS_PROPS = new Set([
    'fontWeight',
    'lineHeight',
    'opacity',
    'zIndex',
]);

const DEFAULT_HEADER_STYLE = Object.freeze({
    fontWeight: 700,
    textAlign: 'center',
    verticalAlign: 'middle',
    whiteSpace: 'nowrap',
});

const DEFAULT_CELL_STYLE = Object.freeze({
    verticalAlign: 'middle',
    whiteSpace: 'nowrap',
});

const htmlEscapeMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
};

const cssPropertyName = (name) => String(name || '').replace(/[A-Z]/g, (match) => `-${match.toLowerCase()}`);

export function escapeHtml(value) {
    return String(value === undefined || value === null ? '' : value).replace(/[&<>"']/g, (match) => htmlEscapeMap[match]);
}

export function escapeTsv(value) {
    return String(value === undefined || value === null ? '' : value)
        .replace(/\r?\n/g, ' ')
        .replace(/\t/g, ' ');
}

function cssValue(prop, value) {
    if (value === undefined || value === null || value === false || value === '') return null;
    if (typeof value === 'number' && Number.isFinite(value) && !UNITLESS_CSS_PROPS.has(prop)) {
        return `${value}px`;
    }
    return String(value);
}

export function styleObjectToCss(style) {
    if (!style || typeof style !== 'object') return '';
    return Object.entries(style)
        .map(([prop, rawValue]) => {
            const value = cssValue(prop, rawValue);
            return value === null ? null : `${cssPropertyName(prop)}:${value}`;
        })
        .filter(Boolean)
        .join(';');
}

function getColumnSize(column) {
    if (!column) return null;
    if (typeof column.getSize === 'function') {
        const size = Number(column.getSize());
        if (Number.isFinite(size) && size > 0) return Math.round(size);
    }
    const size = Number(column.size || (column.columnDef && column.columnDef.size));
    return Number.isFinite(size) && size > 0 ? Math.round(size) : null;
}

function defaultHeaderLabel(column) {
    if (!column) return '';
    const columnDef = column.columnDef || {};
    if (typeof columnDef.header === 'string' && columnDef.header.trim()) return columnDef.header;
    if (columnDef.headerVal !== undefined && columnDef.headerVal !== null) return String(columnDef.headerVal);
    return column.id !== undefined && column.id !== null ? String(column.id) : '';
}

function defaultCellValue(rowLike, column, rowIndex) {
    if (!column) return '';
    const rowData = rowLike && rowLike.original !== undefined ? rowLike.original : rowLike;
    if (column.id === 'hierarchy') {
        const depth = rowData && rowData.depth != null ? rowData.depth : (rowLike && rowLike.depth != null ? rowLike.depth : 0);
        const label = rowData && rowData._isTotal
            ? (rowData._id != null ? rowData._id : 'Total')
            : (rowData && rowData._id != null ? rowData._id : '');
        return `${'  '.repeat(Math.max(0, Number(depth) || 0))}${label}`;
    }
    if (rowLike && typeof rowLike.getValue === 'function') {
        const value = rowLike.getValue(column.id);
        return value === undefined || value === null ? '' : value;
    }
    const columnDef = column.columnDef || {};
    if (typeof columnDef.accessorFn === 'function') {
        const value = columnDef.accessorFn(rowData, rowIndex);
        return value === undefined || value === null ? '' : value;
    }
    if (columnDef.accessorKey && rowData) {
        const value = rowData[columnDef.accessorKey];
        return value === undefined || value === null ? '' : value;
    }
    if (rowData && column.id && Object.prototype.hasOwnProperty.call(rowData, column.id)) {
        const value = rowData[column.id];
        return value === undefined || value === null ? '' : value;
    }
    return '';
}

function resolveRows(table, rawRowsOverride) {
    if (Array.isArray(rawRowsOverride)) return rawRowsOverride;
    if (table && typeof table.getRowModel === 'function') {
        const rowModel = table.getRowModel();
        return Array.isArray(rowModel && rowModel.rows) ? rowModel.rows : [];
    }
    return [];
}

function resolveColumns(table, options) {
    if (Array.isArray(options.columns)) return options.columns;
    if (table && typeof table.getVisibleLeafColumns === 'function') {
        return table.getVisibleLeafColumns().filter((column) => column && !SKIP_COL_IDS.has(column.id));
    }
    return [];
}

/**
 * Build an array-of-arrays (AOA) suitable for simple spreadsheet export from
 * the table's row model and header groups.
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
                const headerText = defaultHeaderLabel(h.column)
                    || (typeof h.column.id === 'string'
                        ? h.column.id.replace(/^group_/, '').replace(/\|\|\|/g, ' > ')
                        : '');
                aoaRow[leafPos] = headerText;
                if (span > 1 && rowIdx < headerGroups.length - 1) {
                    allMerges.push({ s: { r: rowIdx, c: leafPos }, e: { r: rowIdx, c: leafPos + span - 1 } });
                }
            }
            leafPos += span;
        });
        headerAoaRows.push(aoaRow);
    });

    const colWidths = leafHeaders.map(h => defaultHeaderLabel(h.column).length || (h.column.id != null ? String(h.column.id).length : 0));

    const dataRows = allRows.map((rowLike, index) => leafHeaders.map((h, ci) => {
        const value = defaultCellValue(rowLike, h.column, index);
        const cellLen = String(value).length;
        if (cellLen > colWidths[ci]) colWidths[ci] = cellLen;
        return value;
    }));

    return {
        aoa: [...headerAoaRows, ...dataRows],
        merges: allMerges,
        wsCols: colWidths.map(w => ({ wch: Math.min(Math.max(w + 2, 8), 60) })),
        headerRowCount: headerAoaRows.length,
    };
}

export function buildStyledHtmlTableExport({
    table = null,
    rows = null,
    columns = null,
    includeHeaders = true,
    getHeaderLabel = null,
    getHeaderStyle = null,
    getCellValue = null,
    getCellStyle = null,
    tableStyle = null,
    bodyStyle = null,
} = {}) {
    const exportColumns = Array.isArray(columns)
        ? columns
        : resolveColumns(table, {});
    const exportRows = Array.isArray(rows)
        ? rows
        : resolveRows(table, null);

    const baseTableStyle = {
        borderCollapse: 'collapse',
        borderSpacing: 0,
        fontFamily: 'Inter, Arial, sans-serif',
        fontSize: '12px',
        ...bodyStyle,
        ...tableStyle,
    };
    const tableCss = styleObjectToCss(baseTableStyle);

    const textLines = [];
    const htmlRows = [];

    if (includeHeaders) {
        const textHeader = [];
        const htmlHeaderCells = exportColumns.map((column, columnIndex) => {
            const label = typeof getHeaderLabel === 'function'
                ? getHeaderLabel(column, columnIndex)
                : defaultHeaderLabel(column);
            textHeader.push(escapeTsv(label));
            const width = getColumnSize(column);
            const style = {
                ...DEFAULT_HEADER_STYLE,
                width: width ? `${width}px` : undefined,
                minWidth: width ? `${width}px` : undefined,
                ...(typeof getHeaderStyle === 'function' ? getHeaderStyle(column, columnIndex) : null),
            };
            return `<th style="${escapeHtml(styleObjectToCss(style))}">${escapeHtml(label)}</th>`;
        });
        textLines.push(textHeader.join('\t'));
        htmlRows.push(`<tr>${htmlHeaderCells.join('')}</tr>`);
    }

    exportRows.forEach((rowLike, rowIndex) => {
        const textRow = [];
        const htmlCells = exportColumns.map((column, columnIndex) => {
            const value = typeof getCellValue === 'function'
                ? getCellValue(rowLike, column, rowIndex, columnIndex)
                : defaultCellValue(rowLike, column, rowIndex);
            textRow.push(escapeTsv(value));
            const width = getColumnSize(column);
            const style = {
                ...DEFAULT_CELL_STYLE,
                width: width ? `${width}px` : undefined,
                minWidth: width ? `${width}px` : undefined,
                ...(typeof getCellStyle === 'function' ? getCellStyle(rowLike, column, value, rowIndex, columnIndex) : null),
            };
            return `<td style="${escapeHtml(styleObjectToCss(style))}">${escapeHtml(value)}</td>`;
        });
        textLines.push(textRow.join('\t'));
        htmlRows.push(`<tr>${htmlCells.join('')}</tr>`);
    });

    const html = [
        '<!DOCTYPE html>',
        '<html>',
        '<head>',
        '<meta charset="utf-8" />',
        '<style>table.pivot-export-table{border-collapse:collapse;border-spacing:0}table.pivot-export-table th,table.pivot-export-table td{mso-number-format:"\\@";}</style>',
        '</head>',
        '<body>',
        `<table class="pivot-export-table" style="${escapeHtml(tableCss)}">`,
        htmlRows.join(''),
        '</table>',
        '</body>',
        '</html>',
    ].join('');

    return {
        html,
        text: textLines.join('\n'),
        rowCount: exportRows.length,
        columnCount: exportColumns.length,
    };
}

export function htmlTableToTsv(html) {
    if (!html || typeof DOMParser === 'undefined') return '';
    const doc = new DOMParser().parseFromString(String(html), 'text/html');
    const table = doc.querySelector('table');
    if (!table) return doc.body ? doc.body.textContent || '' : '';
    return Array.from(table.querySelectorAll('tr')).map((row) => (
        Array.from(row.querySelectorAll('th,td')).map((cell) => escapeTsv(cell.textContent || '')).join('\t')
    )).join('\n');
}

export function writeClipboardPayload(payload) {
    const text = payload && payload.text !== undefined && payload.text !== null ? String(payload.text) : '';
    const html = payload && payload.html !== undefined && payload.html !== null ? String(payload.html) : '';
    try {
        if (
            html
            && navigator.clipboard
            && typeof navigator.clipboard.write === 'function'
            && typeof ClipboardItem !== 'undefined'
            && typeof Blob !== 'undefined'
        ) {
            const item = new ClipboardItem({
                'text/html': new Blob([html], { type: 'text/html' }),
                'text/plain': new Blob([text], { type: 'text/plain' }),
            });
            return navigator.clipboard.write([item]);
        }
        if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
            return navigator.clipboard.writeText(text);
        }
        if (typeof document !== 'undefined' && document.body && typeof document.execCommand === 'function') {
            const textarea = document.createElement('textarea');
            textarea.value = text;
            textarea.setAttribute('readonly', '');
            textarea.style.position = 'fixed';
            textarea.style.top = '-1000px';
            textarea.style.opacity = '0';
            document.body.appendChild(textarea);
            textarea.focus();
            textarea.select();
            const copied = document.execCommand('copy');
            document.body.removeChild(textarea);
            if (copied) {
                return Promise.resolve();
            }
        }
    } catch (error) {
        return Promise.reject(error);
    }
    return Promise.reject(new Error('Clipboard API is unavailable.'));
}

export function downloadHtmlTableAsExcel(payload, filename = 'pivot.xls') {
    const html = payload && payload.html ? payload.html : '';
    const blob = new Blob([html], { type: 'application/vnd.ms-excel;charset=utf-8;' });
    saveAs(blob, filename);
}

/**
 * Export the current pivot table as Excel-compatible HTML. This preserves
 * inline styles far better than CSV/TSV or community SheetJS XLSX export.
 */
export function exportPivotTable(table, rowCount, rawRowsOverride = null, options = {}) {
    const allRows = resolveRows(table, rawRowsOverride);
    const payload = buildStyledHtmlTableExport({
        table,
        rows: allRows,
        columns: resolveColumns(table, options),
        includeHeaders: options.includeHeaders !== false,
        getHeaderLabel: options.getHeaderLabel,
        getHeaderStyle: options.getHeaderStyle,
        getCellValue: options.getCellValue,
        getCellStyle: options.getCellStyle,
        tableStyle: options.tableStyle,
        bodyStyle: options.bodyStyle,
    });
    downloadHtmlTableAsExcel(payload, options.filename || 'pivot.xls');
    return {
        rows: payload.rowCount,
        columns: payload.columnCount,
        requestedRows: rowCount || payload.rowCount,
    };
}
