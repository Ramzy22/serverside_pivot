import { saveAs } from 'file-saver';

// XLSX is only needed after a user starts a client-side workbook export. Keep
// it out of the interactive grid bundle: SheetJS is by far the largest export
// dependency and a static import makes every pivot viewer pay that cost.
//
// Normal pivot exports create workbooks from arrays and only need SheetJS's
// writer APIs. Its mini build supplies exactly those APIs. HTML-table import
// remains available through the full build, but pays for the parser only when
// that legacy compatibility path is explicitly called.
/** @type {any} */
let xlsxWriterModulePromise = null;
/** @type {any} */
let xlsxHtmlModulePromise = null;

/** @returns {any} */
function loadXlsxWriter() {
    if (!xlsxWriterModulePromise) {
        xlsxWriterModulePromise = import(/* webpackChunkName: "pivot-xlsx" */ 'xlsx/dist/xlsx.mini.min.js');
    }
    return xlsxWriterModulePromise.then(/** @param {any} [module] @returns {any} */ (module) => module.default || module);
}

/** @returns {any} */
function loadXlsxHtmlParser() {
    if (!xlsxHtmlModulePromise) {
        xlsxHtmlModulePromise = import(/* webpackChunkName: "pivot-xlsx-html" */ 'xlsx');
    }
    return xlsxHtmlModulePromise.then(/** @param {any} [module] @returns {any} */ (module) => module.default || module);
}

/** @type {any} */
const SKIP_COL_IDS = new Set(['__row_number__']);

/** @type {any} */
const UNITLESS_CSS_PROPS = new Set([
    'fontWeight',
    'lineHeight',
    'opacity',
    'zIndex',
]);

/** @type {any} */
const DEFAULT_HEADER_STYLE = Object.freeze({
    fontWeight: 700,
    textAlign: 'center',
    verticalAlign: 'middle',
    whiteSpace: 'nowrap',
});

/** @type {any} */
const DEFAULT_CELL_STYLE = Object.freeze({
    verticalAlign: 'middle',
    whiteSpace: 'nowrap',
});

/** @type {Record<string, any>} */
const htmlEscapeMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
};

/** @param {any} [name] @returns {any} */
const cssPropertyName = (name) => String(name || '').replace(/[A-Z]/g, (match) => `-${match.toLowerCase()}`);

/** @param {any} [value] @returns {any} */
export function escapeHtml(value) {
    return String(value === undefined || value === null ? '' : value).replace(/[&<>"']/g, (match) => htmlEscapeMap[match]);
}

/** @param {any} [value] @returns {any} */
export function escapeTsv(value) {
    /** @type {any} */
    const text = String(value === undefined || value === null ? '' : value);
    return /["\t\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

/** @param {any} [value] @returns {any} */
export function escapeCsv(value) {
    /** @type {any} */
    const text = String(value === undefined || value === null ? '' : value);
    return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

/** @param {any} [prop] @param {any} [value] @returns {any} */
function cssValue(prop, value) {
    if (value === undefined || value === null || value === false || value === '') return null;
    if (typeof value === 'number' && Number.isFinite(value) && !UNITLESS_CSS_PROPS.has(prop)) {
        return `${value}px`;
    }
    return String(value);
}

/** @param {any} [style] @returns {any} */
export function styleObjectToCss(style) {
    if (!style || typeof style !== 'object') return '';
    return Object.entries(style)
        .map(([prop, rawValue]) => {
            /** @type {any} */
            const value = cssValue(prop, rawValue);
            return value === null ? null : `${cssPropertyName(prop)}:${value}`;
        })
        .filter(Boolean)
        .join(';');
}

/** @param {any} [column] @returns {any} */
function getColumnSize(column) {
    if (!column) return null;
    if (typeof column.getSize === 'function') {
        /** @type {any} */
        const size = Number(column.getSize());
        if (Number.isFinite(size) && size > 0) return Math.round(size);
    }
    /** @type {any} */
    const size = Number(column.size || (column.columnDef && column.columnDef.size));
    return Number.isFinite(size) && size > 0 ? Math.round(size) : null;
}

/** @param {any} [column] @returns {any} */
function defaultHeaderLabel(column) {
    if (!column) return '';
    /** @type {any} */
    const columnDef = column.columnDef || {};
    if (typeof columnDef.header === 'string' && columnDef.header.trim()) return columnDef.header;
    if (columnDef.headerVal !== undefined && columnDef.headerVal !== null) return String(columnDef.headerVal);
    return column.id !== undefined && column.id !== null ? String(column.id) : '';
}

/**
 * Convert the visible leaf columns' header ancestry into a compact grid that
 * spreadsheet writers can turn into merged cells. Keeping this derived from
 * the leaf order means it works with virtual columns as well as ordinary
 * TanStack header groups.
  @param {any} [columns] @param {any} [p1] @returns {any} */
export function buildMergedHeaderRows(columns = [], { getHeaderStack = null, getHeaderLabel = null } = {}) {
    /** @type {any} */
    const leaves = (Array.isArray(columns) ? columns : []).filter(Boolean);
    if (leaves.length === 0) return { rows: [], depth: 0 };
    /** @type {any} */
    const labelFor = typeof getHeaderLabel === 'function' ? getHeaderLabel : defaultHeaderLabel;
    /** @type {any} */
    const stacks = leaves.map(/** @param {any} [column] @returns {any} */ (column) => {
        /** @type {any} */
        const rawStack = typeof getHeaderStack === 'function' ? getHeaderStack(column) : null;
        /** @type {any} */
        const stack = (Array.isArray(rawStack) ? rawStack : [])
            .map(value => String(value === undefined || value === null ? '' : value).trim())
            .filter(Boolean);
        return stack.length > 0 ? stack : [String(labelFor(column) || column.id || '')];
    });
    /** @type {any} */
    const depth = Math.max(...stacks.map(/** @param {any} [stack] @returns {any} */ stack => stack.length), 1);
    /** @type {any} */
    const rows = Array.from({ length: depth }, () => ({ cells: [] }));

    for (let level = 0; level < depth; level += 1) {
        let start = 0;
        while (start < leaves.length) {
            const stack = stacks[start];
            if (level >= stack.length) {
                start += 1;
                continue;
            }
            /** @type {any} */
            const prefix = stack.slice(0, level + 1).join('\u0000');
            /** @type {any} */
            let end = start + 1;
            while (end < leaves.length) {
                const candidate = stacks[end];
                if (level >= candidate.length || candidate.slice(0, level + 1).join('\u0000') !== prefix) break;
                end += 1;
            }
            /** @type {any} */
            const columnIds = leaves.slice(start, end).map(/** @param {any} [column] @param {any} [index] @returns {any} */ (column, index) => (
                column && column.id !== undefined && column.id !== null ? String(column.id) : String(start + index)
            ));
            /** @type {any} */
            const rowSpan = level === stack.length - 1 ? depth - level : 1;
            rows[level].cells.push({
                label: stack[level],
                columnIds,
                colSpan: end - start,
                rowSpan,
                startIndex: start,
                endIndex: end - 1,
                styleColumnId: columnIds[0],
            });
            start = end;
        }
    }
    return { rows, depth };
}

/** @param {any} [rowLike] @param {any} [column] @param {any} [rowIndex] @returns {any} */
function defaultCellValue(rowLike, column, rowIndex) {
    if (!column) return '';
    /** @type {any} */
    const rowData = rowLike && rowLike.original !== undefined ? rowLike.original : rowLike;
    if (column.id === 'hierarchy') {
        /** @type {any} */
        const depth = rowData && rowData.depth != null ? rowData.depth : (rowLike && rowLike.depth != null ? rowLike.depth : 0);
        /** @type {any} */
        const label = rowData && rowData._isTotal
            ? (rowData._id != null ? rowData._id : 'Total')
            : (rowData && rowData._id != null ? rowData._id : '');
        return `${'  '.repeat(Math.max(0, Number(depth) || 0))}${label}`;
    }
    if (rowLike && typeof rowLike.getValue === 'function') {
        /** @type {any} */
        const value = rowLike.getValue(column.id);
        return value === undefined || value === null ? '' : value;
    }
    /** @type {any} */
    const columnDef = column.columnDef || {};
    if (typeof columnDef.accessorFn === 'function') {
        /** @type {any} */
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

/** @param {any} [table] @param {any} [rawRowsOverride] @returns {any} */
function resolveRows(table, rawRowsOverride) {
    if (Array.isArray(rawRowsOverride)) return rawRowsOverride;
    if (table && typeof table.getRowModel === 'function') {
        /** @type {any} */
        const rowModel = table.getRowModel();
        return Array.isArray(rowModel && rowModel.rows) ? rowModel.rows : [];
    }
    return [];
}

/** @param {any} [table] @param {any} [options] @returns {any} */
function resolveColumns(table, options) {
    if (Array.isArray(options.columns)) return options.columns;
    if (table && typeof table.getVisibleLeafColumns === 'function') {
        return table.getVisibleLeafColumns().filter(/** @param {any} [column] @returns {any} */ (column) => column && !SKIP_COL_IDS.has(column.id));
    }
    return [];
}

// ── SpreadsheetML 2003 (Excel XML) helpers ──────────────────────────────────

/** @param {any} [v] @returns {any} */
function _xmlEsc(v) {
    return String(v == null ? '' : v)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
}

/** @param {any} [css] @param {any} [fallback] @returns {any} */
function _normColor(css, fallback) {
    if (!css || typeof css !== 'string') return fallback || null;
    /** @type {any} */
    const s = css.trim();
    if (/^#[0-9a-f]{6}$/i.test(s)) return s.toUpperCase();
    if (/^#[0-9a-f]{3}$/i.test(s)) return ('#' + s.slice(1).split('').map(/** @param {any} [c] @returns {any} */ c => c + c).join('')).toUpperCase();
    if (/^#[0-9a-f]{8}$/i.test(s)) return s.slice(0, 7).toUpperCase();
    /** @type {any} */
    const m = s.match(/^rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/i);
    if (m) return ('#' + [m[1], m[2], m[3]].map(n => (+n).toString(16).padStart(2, '0')).join('')).toUpperCase();
    return fallback || null;
}

/** @param {any} [color] @returns {any} */
function _smlBordersXml(color) {
    return ['Bottom', 'Left', 'Right', 'Top'].map(p =>
        `<Border ss:Position="${p}" ss:LineStyle="Continuous" ss:Weight="1" ss:Color="${color}"/>`
    ).join('');
}

/** @param {any} [id] @param {any} [p1] @returns {any} */
function _smlStyle(id, { bg, fg, bold, hAlign, wrap, numFmt, borderColor } = {}) {
    let s = `<Style ss:ID="${id}">`;
    s += `<Alignment ss:Horizontal="${hAlign || 'General'}" ss:Vertical="Center"${wrap ? ' ss:WrapText="1"' : ''}/>`;
    if (borderColor) s += `<Borders>${_smlBordersXml(borderColor)}</Borders>`;
    s += `<Font ss:FontName="Calibri" ss:Size="11"${bold ? ' ss:Bold="1"' : ''}${fg ? ` ss:Color="${fg}"` : ''}/>`;
    if (bg) s += `<Interior ss:Color="${bg}" ss:Pattern="Solid"/>`;
    if (numFmt) s += `<NumberFormat ss:Format="${_xmlEsc(numFmt)}"/>`;
    return s + '</Style>';
}

/** @param {any} [tc] @returns {any} */
function _buildSmlStyles(tc) {
    /** @type {any} */
    const border = _normColor((tc || {}).border, '#CCCCCC');
    /** @type {any} */
    const hdrBg  = _normColor((tc || {}).headerBg || (tc || {}).headerSubtleBg, '#1E3A5F');
    /** @type {any} */
    const hdrFg  = _normColor((tc || {}).headerText, '#FFFFFF');
    /** @type {any} */
    const totBg  = _normColor((tc || {}).totalBgStrong || (tc || {}).totalBg, '#E8EEF8');
    /** @type {any} */
    const totFg  = _normColor((tc || {}).totalText || (tc || {}).text, '#1A2B3C');
    /** @type {any} */
    const dataBg = _normColor((tc || {}).surfaceBg || (tc || {}).background, '#FFFFFF');
    /** @type {any} */
    const dataFg = _normColor((tc || {}).textSec || (tc || {}).text, '#333333');
    /** @type {any} */
    const hierBg = _normColor((tc || {}).hierarchyBg || (tc || {}).surfaceInset, '#F5F7FA');
    const numFmt = '#,##0.##';
    return [
        '<Style ss:ID="Default"><Alignment ss:Vertical="Center"/><Font ss:FontName="Calibri" ss:Size="11"/></Style>',
        _smlStyle('sHdr',  { bg: hdrBg,  fg: hdrFg,  bold: true, hAlign: 'Center', wrap: true, borderColor: border }),
        _smlStyle('sHdrT', { bg: totBg,  fg: totFg,  bold: true, hAlign: 'Center', wrap: true, borderColor: border }),
        _smlStyle('sDtxt', { bg: dataBg, fg: dataFg, hAlign: 'Left',  borderColor: border }),
        _smlStyle('sDnum', { bg: dataBg, fg: dataFg, hAlign: 'Right', borderColor: border, numFmt }),
        _smlStyle('sDhir', { bg: hierBg, fg: dataFg, hAlign: 'Left',  borderColor: border }),
        _smlStyle('sTtxt', { bg: totBg,  fg: totFg,  bold: true, hAlign: 'Left',  borderColor: border }),
        _smlStyle('sTnum', { bg: totBg,  fg: totFg,  bold: true, hAlign: 'Right', borderColor: border, numFmt }),
    ].join('');
}

/** @param {any} [headerGroups] @param {any} [getHeaderLabel] @param {any} [isHeaderTotalCol] @returns {any} */
function _buildSmlHeaderRows(headerGroups, getHeaderLabel, isHeaderTotalCol) {
    if (!headerGroups || !headerGroups.length) return '';
    return headerGroups.map(/** @param {any} [hg] @returns {any} */ (hg) => {
        let cells = '';
        let colCursor = 1;       // leaf column position (1-based)
        let xmlAutoIdx = 1;      // where SpreadsheetML will auto-place next cell
        hg.headers.forEach(/** @param {any} [h] @returns {any} */ (h) => {
            if (!h.column || SKIP_COL_IDS.has(h.column.id)) return;
            /** @type {any} */
            const span = h.colSpan || 1;
            if (h.isPlaceholder) { colCursor += span; return; }
            /** @type {any} */
            const rowSpan = h.rowSpan || 1;
            /** @type {any} */
            const label = typeof getHeaderLabel === 'function'
                ? getHeaderLabel(h.column, colCursor - 1)
                : defaultHeaderLabel(h.column);
            /** @type {any} */
            const isTotH = typeof isHeaderTotalCol === 'function' ? isHeaderTotalCol(h.column) : false;
            /** @type {any} */
            const sid = isTotH ? 'sHdrT' : 'sHdr';
            let attrs = `ss:StyleID="${sid}"`;
            if (colCursor !== xmlAutoIdx) attrs = `ss:Index="${colCursor}" ` + attrs;
            if (span > 1) attrs += ` ss:MergeAcross="${span - 1}"`;
            if (rowSpan > 1) attrs += ` ss:MergeDown="${rowSpan - 1}"`;
            cells += `<Cell ${attrs}><Data ss:Type="String">${_xmlEsc(label)}</Data></Cell>`;
            colCursor += span;
            xmlAutoIdx = colCursor;
        });
        return `<Row>${cells}</Row>`;
    }).join('');
}

/** @param {any} [rows] @param {any} [exportColumns] @param {any} [getCellValue] @param {any} [getCellRawValue] @param {any} [isHierarchyCol] @param {any} [isTotalRow] @param {any} [isTotalCol] @returns {any} */
function _buildSmlDataRows(rows, exportColumns, getCellValue, getCellRawValue, isHierarchyCol, isTotalRow, isTotalCol) {
    let xml = '';
    rows.forEach(/** @param {any} [rowLike] @param {any} [rowIndex] @returns {any} */ (rowLike, rowIndex) => {
        /** @type {any} */
        const isTotal = typeof isTotalRow === 'function' && isTotalRow(rowLike);
        let cells = '';
        exportColumns.forEach(/** @param {any} [col] @returns {any} */ (col) => {
            /** @type {any} */
            const isHier = typeof isHierarchyCol === 'function' && isHierarchyCol(col);
            /** @type {any} */
            const isTotC = !isHier && typeof isTotalCol === 'function' && isTotalCol(col);
            /** @type {any} */
            const displayVal = typeof getCellValue === 'function'
                ? getCellValue(rowLike, col, rowIndex)
                : defaultCellValue(rowLike, col, rowIndex);
            /** @type {any} */
            const rawVal = typeof getCellRawValue === 'function'
                ? getCellRawValue(rowLike, col, rowIndex)
                : null;
            /** @type {any} */
            const isNum = !isHier && typeof rawVal === 'number' && Number.isFinite(rawVal);
            /** @type {any} */
            let sid;
            if (isHier)                   sid = 'sDhir';
            else if (isTotal || isTotC)   sid = isNum ? 'sTnum' : 'sTtxt';
            else                          sid = isNum ? 'sDnum' : 'sDtxt';
            if (isNum) {
                cells += `<Cell ss:StyleID="${sid}"><Data ss:Type="Number">${rawVal}</Data></Cell>`;
            } else {
                cells += `<Cell ss:StyleID="${sid}"><Data ss:Type="String">${_xmlEsc(String(displayVal == null ? '' : displayVal))}</Data></Cell>`;
            }
        });
        xml += `<Row>${cells}</Row>`;
    });
    return xml;
}

/**
 * Export the pivot table as SpreadsheetML 2003 XML (.xls).
 * Produces a real Excel file with merged headers, typed cells, and cell styles —
 * no format-mismatch warning, numbers sort/sum correctly in Excel.
  @param {any} [p0] @returns {any} */
export function buildSpreadsheetMlExport({
    table = null,
    rows = null,
    columns = null,
    getHeaderLabel = null,
    isHeaderTotalCol = null,
    getCellValue = null,
    getCellRawValue = null,
    isHierarchyCol = null,
    isTotalRow = null,
    isTotalCol = null,
    themeColors = {},
    filename = 'pivot.xls',
    sheetName = 'Pivot',
} = {}) {
    /** @type {any} */
    const exportCols = resolveColumns(table, { columns: Array.isArray(columns) ? columns : undefined });
    /** @type {any} */
    const exportRows = Array.isArray(rows) ? rows : resolveRows(table, null);
    /** @type {any} */
    const headerGroups = table && typeof table.getHeaderGroups === 'function' ? table.getHeaderGroups() : [];

    /** @type {any} */
    const styles = _buildSmlStyles(themeColors);

    /** @type {any} */
    const colDefs = exportCols.map(/** @param {any} [col] @returns {any} */ (col) => {
        /** @type {any} */
        const sz = getColumnSize(col);
        return sz && sz > 0 ? `<Column ss:Width="${Math.round(sz * 0.75)}"/>` : '<Column ss:AutoFitWidth="1" ss:Width="60"/>';
    }).join('');

    /** @type {any} */
    const headerRowsXml = _buildSmlHeaderRows(headerGroups, getHeaderLabel, isHeaderTotalCol);
    /** @type {any} */
    const dataRowsXml   = _buildSmlDataRows(exportRows, exportCols, getCellValue, getCellRawValue, isHierarchyCol, isTotalRow, isTotalCol);

    const hdrCount = headerGroups.length;
    /** @type {any} */
    const freezeXml = hdrCount > 0
        ? `<WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel"><FreezePanes/><FrozenNoSplit/><SplitHorizontal>${hdrCount}</SplitHorizontal><TopRowBottomPane>${hdrCount}</TopRowBottomPane></WorksheetOptions>`
        : '';

    const xml = `<?xml version="1.0"?>\n<?mso-application progid="Excel.Sheet"?>\n<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" xmlns:x="urn:schemas-microsoft-com:office:excel"><Styles>${styles}</Styles><Worksheet ss:Name="${_xmlEsc(sheetName)}"><Table ss:DefaultRowHeight="18">${colDefs}${headerRowsXml}${dataRowsXml}</Table>${freezeXml}</Worksheet></Workbook>`;

    /** @type {any} */
    const blob = new Blob([xml], { type: 'application/vnd.ms-excel;charset=utf-8' });
    /** @type {any} */
    const safeName = String(filename || 'pivot.xls');
    saveAs(blob, /\.(xls|xlsx)$/i.test(safeName) ? safeName : safeName + '.xls');

    return { rows: exportRows.length, columns: exportCols.length };
}

/**
 * Export the pivot table as a real Office Open XML workbook (.xlsx).
  @param {any} [p0] @returns {Promise<any>} */
export async function buildXlsxExport({
    table = null,
    rows = null,
    columns = null,
    getHeaderLabel = null,
    getHeaderStack = null,
    getCellValue = null,
    getCellRawValue = null,
    filename = 'pivot.xlsx',
    sheetName = 'Pivot',
} = {}) {
    const XLSX = await loadXlsxWriter();
    /** @type {any} */
    const exportCols = resolveColumns(table, { columns: Array.isArray(columns) ? columns : undefined });
    /** @type {any} */
    const exportRows = Array.isArray(rows) ? rows : resolveRows(table, null);
    /** @type {any} */
    const colWidths = exportCols.map(/** @param {any} [column] @param {any} [columnIndex] @returns {any} */ (column, columnIndex) => {
        /** @type {any} */
        const label = typeof getHeaderLabel === 'function'
            ? getHeaderLabel(column, columnIndex)
            : defaultHeaderLabel(column);
        return Math.max(String(label || '').length, 8);
    });
    /** @type {any} */
    const headerRow = exportCols.map(/** @param {any} [column] @param {any} [columnIndex] @returns {any} */ (column, columnIndex) => (
        typeof getHeaderLabel === 'function'
            ? getHeaderLabel(column, columnIndex)
            : defaultHeaderLabel(column)
    ));
    /** @type {any} */
    const mergedHeaders = buildMergedHeaderRows(exportCols, { getHeaderStack, getHeaderLabel });
    /** @type {any} */
    const headerRows = mergedHeaders.rows.length > 0
        ? mergedHeaders.rows.map(/** @param {any} p0 @returns {any} */ ({ cells }) => {
            /** @type {any} */
            const row = Array(exportCols.length).fill('');
            cells.forEach(/** @param {any} [cell] @returns {any} */ cell => { row[cell.startIndex] = cell.label; });
            return row;
        })
        : [headerRow];
    /** @type {any} */
    const dataRows = exportRows.map(/** @param {any} [rowLike] @param {any} [rowIndex] @returns {any} */ (rowLike, rowIndex) => (
        exportCols.map(/** @param {any} [column] @param {any} [columnIndex] @returns {any} */ (column, columnIndex) => {
            /** @type {any} */
            const rawValue = typeof getCellRawValue === 'function'
                ? getCellRawValue(rowLike, column, rowIndex)
                : undefined;
            /** @type {any} */
            const value = rawValue !== undefined
                ? rawValue
                : (typeof getCellValue === 'function'
                    ? getCellValue(rowLike, column, rowIndex)
                    : defaultCellValue(rowLike, column, rowIndex));
            const width = String(value === undefined || value === null ? '' : value).length;
            if (width > colWidths[columnIndex]) colWidths[columnIndex] = width;
            return value === undefined || value === null ? '' : value;
        })
    ));
    /** @type {any} */
    const worksheet = XLSX.utils.aoa_to_sheet([...headerRows, ...dataRows]);
    /** @type {any[]} */
    const merges = [];
    mergedHeaders.rows.forEach(/** @param {any} p0 @param {any} [rowIndex] @returns {any} */ ({ cells }, rowIndex) => {
        cells.forEach(/** @param {any} [cell] @returns {any} */ (cell) => {
            if (cell.colSpan > 1 || cell.rowSpan > 1) {
                merges.push({
                    s: { r: rowIndex, c: cell.startIndex },
                    e: { r: rowIndex + cell.rowSpan - 1, c: cell.endIndex },
                });
            }
        });
    });
    if (merges.length > 0) worksheet['!merges'] = merges;
    worksheet['!cols'] = colWidths.map(/** @param {any} [width] @returns {any} */ width => ({ wch: Math.min(Math.max(width + 2, 8), 60) }));
    /** @type {any} */
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);
    /** @type {any} */
    const bytes = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    /** @type {any} */
    const blob = new Blob([bytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    /** @type {any} */
    const safeName = String(filename || 'pivot.xlsx');
    /** @type {any} */
    const downloadName = /\.xlsx$/i.test(safeName)
        ? safeName
        : (/\.xls$/i.test(safeName) ? safeName.replace(/\.xls$/i, '.xlsx') : `${safeName}.xlsx`);
    saveAs(blob, downloadName || 'pivot.xlsx');

    return { rows: exportRows.length, columns: exportCols.length };
}

/**
 * Export the pivot table as UTF-8 CSV without loading the XLSX dependency.
  @param {any} [p0] @returns {any} */
export function buildCsvExport({
    table = null,
    rows = null,
    columns = null,
    getHeaderLabel = null,
    getCellValue = null,
    getCellRawValue = null,
    filename = 'pivot.csv',
} = {}) {
    /** @type {any} */
    const exportCols = resolveColumns(table, { columns: Array.isArray(columns) ? columns : undefined });
    /** @type {any} */
    const exportRows = Array.isArray(rows) ? rows : resolveRows(table, null);
    /** @type {any} */
    const headerRow = exportCols.map(/** @param {any} [column] @param {any} [columnIndex] @returns {any} */ (column, columnIndex) => escapeCsv(
        typeof getHeaderLabel === 'function'
            ? getHeaderLabel(column, columnIndex)
            : defaultHeaderLabel(column)
    ));
    /** @type {any} */
    const dataRows = exportRows.map(/** @param {any} [rowLike] @param {any} [rowIndex] @returns {any} */ (rowLike, rowIndex) => exportCols.map(/** @param {any} [column] @param {any} [columnIndex] @returns {any} */ (column, columnIndex) => {
        /** @type {any} */
        const rawValue = typeof getCellRawValue === 'function'
            ? getCellRawValue(rowLike, column, rowIndex)
            : undefined;
        /** @type {any} */
        const value = rawValue !== undefined
            ? rawValue
            : (typeof getCellValue === 'function'
                ? getCellValue(rowLike, column, rowIndex)
                : defaultCellValue(rowLike, column, rowIndex));
        return escapeCsv(value);
    }));
    /** @type {any} */
    const csv = [headerRow, ...dataRows].map(row => row.join(',')).join('\r\n');
    /** @type {any} */
    const blob = new Blob([`\ufeff${csv}`], { type: 'text/csv;charset=utf-8' });
    /** @type {any} */
    const safeName = String(filename || 'pivot.csv');
    saveAs(blob, /\.csv$/i.test(safeName) ? safeName : `${safeName}.csv`);

    return { rows: exportRows.length, columns: exportCols.length };
}

// ── AOA helper (used by legacy paths) ───────────────────────────────────────

/**
 * Build an array-of-arrays (AOA) suitable for simple spreadsheet export from
 * the table's row model and header groups.
  @param {any} [allRows] @param {any} [table] @returns {any} */
export function buildExportAoa(allRows, table) {
    /** @type {any} */
    const headerGroups = table.getHeaderGroups();

    const lastHeaderGroup = headerGroups[headerGroups.length - 1];
    /** @type {any} */
    const leafHeaders = (lastHeaderGroup && lastHeaderGroup.headers != null ? lastHeaderGroup.headers : [])
        .filter(/** @param {any} [h] @returns {any} */ h => !SKIP_COL_IDS.has(h.column.id) && !h.isPlaceholder);

    const leafCount = leafHeaders.length;

    /** @type {any[]} */
    const headerAoaRows = [];
    /** @type {any[]} */
    const allMerges = [];

    headerGroups.forEach(/** @param {any} [hg] @param {any} [rowIdx] @returns {any} */ (hg, rowIdx) => {
        /** @type {any} */
        const aoaRow = new Array(leafCount).fill('');
        let leafPos = 0;
        hg.headers.forEach(/** @param {any} [h] @returns {any} */ h => {
            if (SKIP_COL_IDS.has(h.column.id)) return;
            const span = (h.colSpan != null ? h.colSpan : 1);
            if (!h.isPlaceholder) {
                /** @type {any} */
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

    /** @type {any} */
    const colWidths = leafHeaders.map(/** @param {any} [h] @returns {any} */ h => defaultHeaderLabel(h.column).length || (h.column.id != null ? String(h.column.id).length : 0));

    /** @type {any} */
    const dataRows = allRows.map(/** @param {any} [rowLike] @param {any} [index] @returns {any} */ (rowLike, index) => leafHeaders.map(/** @param {any} [h] @param {any} [ci] @returns {any} */ (h, ci) => {
        /** @type {any} */
        const value = defaultCellValue(rowLike, h.column, index);
        const cellLen = String(value).length;
        if (cellLen > colWidths[ci]) colWidths[ci] = cellLen;
        return value;
    }));

    return {
        aoa: [...headerAoaRows, ...dataRows],
        merges: allMerges,
        wsCols: colWidths.map(/** @param {any} [w] @returns {any} */ w => ({ wch: Math.min(Math.max(w + 2, 8), 60) })),
        headerRowCount: headerAoaRows.length,
    };
}

/** @param {any} [p0] @returns {any} */
export function buildStyledHtmlTableExport({
    table = null,
    rows = null,
    columns = null,
    includeHeaders = true,
    getHeaderLabel = null,
    getHeaderStack = null,
    getHeaderStyle = null,
    getCellValue = null,
    getCellStyle = null,
    tableStyle = null,
    bodyStyle = null,
} = {}) {
    /** @type {any} */
    const exportColumns = Array.isArray(columns)
        ? columns
        : resolveColumns(table, {});
    /** @type {any} */
    const exportRows = Array.isArray(rows)
        ? rows
        : resolveRows(table, null);

    /** @type {Record<string, any>} */
    const baseTableStyle = {
        borderCollapse: 'collapse',
        borderSpacing: 0,
        fontFamily: 'Inter, Arial, sans-serif',
        fontSize: '12px',
        ...bodyStyle,
        ...tableStyle,
    };
    /** @type {any} */
    const tableCss = styleObjectToCss(baseTableStyle);

    /** @type {any[]} */
    const textLines = [];
    /** @type {any[]} */
    const htmlRows = [];

    if (includeHeaders) {
        // TSV text header is always flat (leaf columns)
        /** @type {any} */
        const textHeader = exportColumns.map(/** @param {any} [column] @param {any} [columnIndex] @returns {any} */ (column, columnIndex) => {
            /** @type {any} */
            const label = typeof getHeaderLabel === 'function' ? getHeaderLabel(column, columnIndex) : defaultHeaderLabel(column);
            return escapeTsv(label);
        });
        textLines.push(textHeader.join('\t'));

        // HTML header: multi-level when table has multiple header groups
        /** @type {any} */
        const headerGroups = table && typeof table.getHeaderGroups === 'function' ? table.getHeaderGroups() : [];
        if (typeof getHeaderStack === 'function') {
            /** @type {any} */
            const merged = buildMergedHeaderRows(exportColumns, { getHeaderStack, getHeaderLabel });
            merged.rows.forEach(/** @param {any} p0 @returns {any} */ ({ cells }) => {
                /** @type {any} */
                const rowCells = cells.map(/** @param {any} [cell] @returns {any} */ (cell) => {
                    const column = exportColumns[cell.startIndex];
                    /** @type {any} */
                    const width = cell.colSpan === 1 ? getColumnSize(column) : null;
                    /** @type {Record<string, any>} */
                    const style = {
                        ...DEFAULT_HEADER_STYLE,
                        width: width ? `${width}px` : undefined,
                        minWidth: width ? `${width}px` : undefined,
                        ...(typeof getHeaderStyle === 'function' ? getHeaderStyle(column, cell.startIndex) : null),
                    };
                    let attrs = `style="${escapeHtml(styleObjectToCss(style))}"`;
                    if (cell.colSpan > 1) attrs += ` colspan="${cell.colSpan}"`;
                    if (cell.rowSpan > 1) attrs += ` rowspan="${cell.rowSpan}"`;
                    return `<th ${attrs}>${escapeHtml(cell.label)}</th>`;
                }).join('');
                htmlRows.push(`<tr>${rowCells}</tr>`);
            });
        } else if (headerGroups.length > 1) {
            headerGroups.forEach(/** @param {any} [hg] @returns {any} */ (hg) => {
                let rowCells = '';
                hg.headers.forEach(/** @param {any} [h] @returns {any} */ (h) => {
                    if (!h.column || SKIP_COL_IDS.has(h.column.id)) return;
                    if (h.isPlaceholder) return; // covered by parent rowspan
                    /** @type {any} */
                    const span = h.colSpan || 1;
                    /** @type {any} */
                    const rowSpan = h.rowSpan || 1;
                    /** @type {any} */
                    const label = typeof getHeaderLabel === 'function' ? getHeaderLabel(h.column, 0) : defaultHeaderLabel(h.column);
                    /** @type {any} */
                    const width = getColumnSize(h.column);
                    /** @type {Record<string, any>} */
                    const style = {
                        ...DEFAULT_HEADER_STYLE,
                        width: width ? `${width}px` : undefined,
                        minWidth: width ? `${width}px` : undefined,
                        ...(typeof getHeaderStyle === 'function' ? getHeaderStyle(h.column, 0) : null),
                    };
                    let attrs = `style="${escapeHtml(styleObjectToCss(style))}"`;
                    if (span > 1) attrs += ` colspan="${span}"`;
                    if (rowSpan > 1) attrs += ` rowspan="${rowSpan}"`;
                    rowCells += `<th ${attrs}>${escapeHtml(label)}</th>`;
                });
                htmlRows.push(`<tr>${rowCells}</tr>`);
            });
        } else {
            // Single-level: flat header row
            /** @type {any} */
            const htmlHeaderCells = exportColumns.map(/** @param {any} [column] @param {any} [columnIndex] @returns {any} */ (column, columnIndex) => {
                /** @type {any} */
                const label = typeof getHeaderLabel === 'function' ? getHeaderLabel(column, columnIndex) : defaultHeaderLabel(column);
                /** @type {any} */
                const width = getColumnSize(column);
                /** @type {Record<string, any>} */
                const style = {
                    ...DEFAULT_HEADER_STYLE,
                    width: width ? `${width}px` : undefined,
                    minWidth: width ? `${width}px` : undefined,
                    ...(typeof getHeaderStyle === 'function' ? getHeaderStyle(column, columnIndex) : null),
                };
                return `<th style="${escapeHtml(styleObjectToCss(style))}">${escapeHtml(label)}</th>`;
            });
            htmlRows.push(`<tr>${htmlHeaderCells.join('')}</tr>`);
        }
    }

    exportRows.forEach(/** @param {any} [rowLike] @param {any} [rowIndex] @returns {any} */ (rowLike, rowIndex) => {
        /** @type {any[]} */
        const textRow = [];
        /** @type {any} */
        const htmlCells = exportColumns.map(/** @param {any} [column] @param {any} [columnIndex] @returns {any} */ (column, columnIndex) => {
            /** @type {any} */
            const value = typeof getCellValue === 'function'
                ? getCellValue(rowLike, column, rowIndex, columnIndex)
                : defaultCellValue(rowLike, column, rowIndex);
            textRow.push(escapeTsv(value));
            /** @type {any} */
            const width = getColumnSize(column);
            /** @type {Record<string, any>} */
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

    /** @type {any} */
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

/** @param {any} [html] @returns {any} */
export function htmlTableToTsv(html) {
    if (!html || typeof DOMParser === 'undefined') return '';
    // HTML parsing normalizes raw CR/CRLF to LF, but character references
    // retain CR. Protect text without changing tag/attribute whitespace.
    /** @type {any} */
    const source = String(html).replace(/<!--[\s\S]*?-->|<(?:"[^"]*"|'[^']*'|[^'">])*>|[^<]+/g, (part) => (
        part.startsWith('<') ? part : part.replace(/\r/g, '&#13;')
    ));
    /** @type {any} */
    const doc = new DOMParser().parseFromString(source, 'text/html');
    /** @type {any} */
    const table = doc.querySelector('table');
    if (!table) return doc.body ? doc.body.textContent || '' : '';
    return Array.from(table.querySelectorAll('tr')).map((row) => (
        Array.from(row.querySelectorAll('th,td')).map((cell) => escapeTsv(cell.textContent || '')).join('\t')
    )).join('\n');
}

/** @param {any} [payload] @returns {any} */
export function writeClipboardPayload(payload) {
    /** @type {any} */
    const text = payload && payload.text !== undefined && payload.text !== null ? String(payload.text) : '';
    /** @type {any} */
    const html = payload && payload.html !== undefined && payload.html !== null ? String(payload.html) : '';
    try {
        if (
            html
            && navigator.clipboard
            && typeof navigator.clipboard.write === 'function'
            && typeof ClipboardItem !== 'undefined'
            && typeof Blob !== 'undefined'
        ) {
            /** @type {any} */
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
            /** @type {any} */
            const textarea = document.createElement('textarea');
            textarea.value = text;
            textarea.setAttribute('readonly', '');
            textarea.style.position = 'fixed';
            textarea.style.top = '-1000px';
            textarea.style.opacity = '0';
            document.body.appendChild(textarea);
            textarea.focus();
            textarea.select();
            /** @type {any} */
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

/** @param {any} [payload] @param {any} [filename] @returns {Promise<any>} */
export async function downloadHtmlTableAsExcel(payload, filename = 'pivot.xlsx') {
    const XLSX = await loadXlsxHtmlParser();
    /** @type {any} */
    const html = payload && payload.html ? payload.html : '';
    /** @type {any} */
    const workbook = XLSX.read(html || '<table></table>', { type: 'string' });
    /** @type {any} */
    const bytes = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    /** @type {any} */
    const blob = new Blob([bytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    /** @type {any} */
    const safeName = String(filename || 'pivot.xlsx');
    saveAs(blob, /\.xlsx$/i.test(safeName) ? safeName : `${safeName.replace(/\.xls$/i, '')}.xlsx`);
}

/**
 * Export the pivot table as a real .xlsx file.
  @param {any} [table] @param {any} [rowCount] @param {any} [rawRowsOverride] @param {any} [options] @returns {Promise<any>} */
export async function exportPivotTable(table, rowCount, rawRowsOverride = null, options = {}) {
    /** @type {any} */
    const allRows = resolveRows(table, rawRowsOverride);
    /** @type {any} */
    const exportCols = resolveColumns(table, options);
    await buildXlsxExport({
        table,
        rows: allRows,
        columns: exportCols,
        getHeaderLabel: options.getHeaderLabel,
        getHeaderStack: options.getHeaderStack,
        getCellValue: options.getCellValue,
        getCellRawValue: options.getCellRawValue,
        filename: options.filename || 'pivot.xlsx',
    });
    return { rows: allRows.length, columns: exportCols.length, requestedRows: rowCount || allRows.length };
}

/**
 * Export the pivot table as a .csv file.
  @param {any} [table] @param {any} [rowCount] @param {any} [rawRowsOverride] @param {any} [options] @returns {Promise<any>} */
export async function exportPivotTableCsv(table, rowCount, rawRowsOverride = null, options = {}) {
    /** @type {any} */
    const allRows = resolveRows(table, rawRowsOverride);
    /** @type {any} */
    const exportCols = resolveColumns(table, options);
    buildCsvExport({
        table,
        rows: allRows,
        columns: exportCols,
        getHeaderLabel: options.getHeaderLabel,
        getCellValue: options.getCellValue,
        getCellRawValue: options.getCellRawValue,
        filename: options.filename || 'pivot.csv',
    });
    return { rows: allRows.length, columns: exportCols.length, requestedRows: rowCount || allRows.length };
}
