import { saveAs } from 'file-saver';
import * as XLSX from 'xlsx';

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

// ── SpreadsheetML 2003 (Excel XML) helpers ──────────────────────────────────

function _xmlEsc(v) {
    return String(v == null ? '' : v)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
}

function _normColor(css, fallback) {
    if (!css || typeof css !== 'string') return fallback || null;
    const s = css.trim();
    if (/^#[0-9a-f]{6}$/i.test(s)) return s.toUpperCase();
    if (/^#[0-9a-f]{3}$/i.test(s)) return ('#' + s.slice(1).split('').map(c => c + c).join('')).toUpperCase();
    if (/^#[0-9a-f]{8}$/i.test(s)) return s.slice(0, 7).toUpperCase();
    const m = s.match(/^rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/i);
    if (m) return ('#' + [m[1], m[2], m[3]].map(n => (+n).toString(16).padStart(2, '0')).join('')).toUpperCase();
    return fallback || null;
}

function _smlBordersXml(color) {
    return ['Bottom', 'Left', 'Right', 'Top'].map(p =>
        `<Border ss:Position="${p}" ss:LineStyle="Continuous" ss:Weight="1" ss:Color="${color}"/>`
    ).join('');
}

function _smlStyle(id, { bg, fg, bold, hAlign, wrap, numFmt, borderColor } = {}) {
    let s = `<Style ss:ID="${id}">`;
    s += `<Alignment ss:Horizontal="${hAlign || 'General'}" ss:Vertical="Center"${wrap ? ' ss:WrapText="1"' : ''}/>`;
    if (borderColor) s += `<Borders>${_smlBordersXml(borderColor)}</Borders>`;
    s += `<Font ss:FontName="Calibri" ss:Size="11"${bold ? ' ss:Bold="1"' : ''}${fg ? ` ss:Color="${fg}"` : ''}/>`;
    if (bg) s += `<Interior ss:Color="${bg}" ss:Pattern="Solid"/>`;
    if (numFmt) s += `<NumberFormat ss:Format="${_xmlEsc(numFmt)}"/>`;
    return s + '</Style>';
}

function _buildSmlStyles(tc) {
    const border = _normColor((tc || {}).border, '#CCCCCC');
    const hdrBg  = _normColor((tc || {}).headerBg || (tc || {}).headerSubtleBg, '#1E3A5F');
    const hdrFg  = _normColor((tc || {}).headerText, '#FFFFFF');
    const totBg  = _normColor((tc || {}).totalBgStrong || (tc || {}).totalBg, '#E8EEF8');
    const totFg  = _normColor((tc || {}).totalText || (tc || {}).text, '#1A2B3C');
    const dataBg = _normColor((tc || {}).surfaceBg || (tc || {}).background, '#FFFFFF');
    const dataFg = _normColor((tc || {}).textSec || (tc || {}).text, '#333333');
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

function _buildSmlHeaderRows(headerGroups, getHeaderLabel, isHeaderTotalCol) {
    if (!headerGroups || !headerGroups.length) return '';
    return headerGroups.map((hg) => {
        let cells = '';
        let colCursor = 1;       // leaf column position (1-based)
        let xmlAutoIdx = 1;      // where SpreadsheetML will auto-place next cell
        hg.headers.forEach((h) => {
            if (!h.column || SKIP_COL_IDS.has(h.column.id)) return;
            const span = h.colSpan || 1;
            if (h.isPlaceholder) { colCursor += span; return; }
            const rowSpan = h.rowSpan || 1;
            const label = typeof getHeaderLabel === 'function'
                ? getHeaderLabel(h.column, colCursor - 1)
                : defaultHeaderLabel(h.column);
            const isTotH = typeof isHeaderTotalCol === 'function' ? isHeaderTotalCol(h.column) : false;
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

function _buildSmlDataRows(rows, exportColumns, getCellValue, getCellRawValue, isHierarchyCol, isTotalRow, isTotalCol) {
    let xml = '';
    rows.forEach((rowLike, rowIndex) => {
        const isTotal = typeof isTotalRow === 'function' && isTotalRow(rowLike);
        let cells = '';
        exportColumns.forEach((col) => {
            const isHier = typeof isHierarchyCol === 'function' && isHierarchyCol(col);
            const isTotC = !isHier && typeof isTotalCol === 'function' && isTotalCol(col);
            const displayVal = typeof getCellValue === 'function'
                ? getCellValue(rowLike, col, rowIndex)
                : defaultCellValue(rowLike, col, rowIndex);
            const rawVal = typeof getCellRawValue === 'function'
                ? getCellRawValue(rowLike, col, rowIndex)
                : null;
            const isNum = !isHier && typeof rawVal === 'number' && Number.isFinite(rawVal);
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
 */
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
    const exportCols = resolveColumns(table, { columns: Array.isArray(columns) ? columns : undefined });
    const exportRows = Array.isArray(rows) ? rows : resolveRows(table, null);
    const headerGroups = table && typeof table.getHeaderGroups === 'function' ? table.getHeaderGroups() : [];

    const styles = _buildSmlStyles(themeColors);

    const colDefs = exportCols.map((col) => {
        const sz = getColumnSize(col);
        return sz && sz > 0 ? `<Column ss:Width="${Math.round(sz * 0.75)}"/>` : '<Column ss:AutoFitWidth="1" ss:Width="60"/>';
    }).join('');

    const headerRowsXml = _buildSmlHeaderRows(headerGroups, getHeaderLabel, isHeaderTotalCol);
    const dataRowsXml   = _buildSmlDataRows(exportRows, exportCols, getCellValue, getCellRawValue, isHierarchyCol, isTotalRow, isTotalCol);

    const hdrCount = headerGroups.length;
    const freezeXml = hdrCount > 0
        ? `<WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel"><FreezePanes/><FrozenNoSplit/><SplitHorizontal>${hdrCount}</SplitHorizontal><TopRowBottomPane>${hdrCount}</TopRowBottomPane></WorksheetOptions>`
        : '';

    const xml = `<?xml version="1.0"?>\n<?mso-application progid="Excel.Sheet"?>\n<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" xmlns:x="urn:schemas-microsoft-com:office:excel"><Styles>${styles}</Styles><Worksheet ss:Name="${_xmlEsc(sheetName)}"><Table ss:DefaultRowHeight="18">${colDefs}${headerRowsXml}${dataRowsXml}</Table>${freezeXml}</Worksheet></Workbook>`;

    const blob = new Blob([xml], { type: 'application/vnd.ms-excel;charset=utf-8' });
    const safeName = String(filename || 'pivot.xls');
    saveAs(blob, /\.(xls|xlsx)$/i.test(safeName) ? safeName : safeName + '.xls');

    return { rows: exportRows.length, columns: exportCols.length };
}

/**
 * Export the pivot table as a real Office Open XML workbook (.xlsx).
 */
export function buildXlsxExport({
    table = null,
    rows = null,
    columns = null,
    getHeaderLabel = null,
    getCellValue = null,
    getCellRawValue = null,
    filename = 'pivot.xlsx',
    sheetName = 'Pivot',
} = {}) {
    const exportCols = resolveColumns(table, { columns: Array.isArray(columns) ? columns : undefined });
    const exportRows = Array.isArray(rows) ? rows : resolveRows(table, null);
    const colWidths = exportCols.map((column, columnIndex) => {
        const label = typeof getHeaderLabel === 'function'
            ? getHeaderLabel(column, columnIndex)
            : defaultHeaderLabel(column);
        return Math.max(String(label || '').length, 8);
    });
    const headerRow = exportCols.map((column, columnIndex) => (
        typeof getHeaderLabel === 'function'
            ? getHeaderLabel(column, columnIndex)
            : defaultHeaderLabel(column)
    ));
    const dataRows = exportRows.map((rowLike, rowIndex) => (
        exportCols.map((column, columnIndex) => {
            const rawValue = typeof getCellRawValue === 'function'
                ? getCellRawValue(rowLike, column, rowIndex)
                : undefined;
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
    const worksheet = XLSX.utils.aoa_to_sheet([headerRow, ...dataRows]);
    worksheet['!cols'] = colWidths.map(width => ({ wch: Math.min(Math.max(width + 2, 8), 60) }));
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);
    const bytes = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([bytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const safeName = String(filename || 'pivot.xlsx');
    const downloadName = /\.xlsx$/i.test(safeName)
        ? safeName
        : (/\.xls$/i.test(safeName) ? safeName.replace(/\.xls$/i, '.xlsx') : `${safeName}.xlsx`);
    saveAs(blob, downloadName || 'pivot.xlsx');

    return { rows: exportRows.length, columns: exportCols.length };
}

// ── AOA helper (used by legacy paths) ───────────────────────────────────────

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
        // TSV text header is always flat (leaf columns)
        const textHeader = exportColumns.map((column, columnIndex) => {
            const label = typeof getHeaderLabel === 'function' ? getHeaderLabel(column, columnIndex) : defaultHeaderLabel(column);
            return escapeTsv(label);
        });
        textLines.push(textHeader.join('\t'));

        // HTML header: multi-level when table has multiple header groups
        const headerGroups = table && typeof table.getHeaderGroups === 'function' ? table.getHeaderGroups() : [];
        if (headerGroups.length > 1) {
            headerGroups.forEach((hg) => {
                let rowCells = '';
                hg.headers.forEach((h) => {
                    if (!h.column || SKIP_COL_IDS.has(h.column.id)) return;
                    if (h.isPlaceholder) return; // covered by parent rowspan
                    const span = h.colSpan || 1;
                    const rowSpan = h.rowSpan || 1;
                    const label = typeof getHeaderLabel === 'function' ? getHeaderLabel(h.column, 0) : defaultHeaderLabel(h.column);
                    const width = getColumnSize(h.column);
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
            const htmlHeaderCells = exportColumns.map((column, columnIndex) => {
                const label = typeof getHeaderLabel === 'function' ? getHeaderLabel(column, columnIndex) : defaultHeaderLabel(column);
                const width = getColumnSize(column);
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

export function downloadHtmlTableAsExcel(payload, filename = 'pivot.xlsx') {
    const html = payload && payload.html ? payload.html : '';
    const workbook = XLSX.read(html || '<table></table>', { type: 'string' });
    const bytes = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([bytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const safeName = String(filename || 'pivot.xlsx');
    saveAs(blob, /\.xlsx$/i.test(safeName) ? safeName : `${safeName.replace(/\.xls$/i, '')}.xlsx`);
}

/**
 * Export the pivot table as a real .xlsx file.
 */
export function exportPivotTable(table, rowCount, rawRowsOverride = null, options = {}) {
    const allRows = resolveRows(table, rawRowsOverride);
    const exportCols = resolveColumns(table, options);
    buildXlsxExport({
        table,
        rows: allRows,
        columns: exportCols,
        getHeaderLabel: options.getHeaderLabel,
        getCellValue: options.getCellValue,
        getCellRawValue: options.getCellRawValue,
        filename: options.filename || 'pivot.xlsx',
    });
    return { rows: allRows.length, columns: exportCols.length, requestedRows: rowCount || allRows.length };
}
