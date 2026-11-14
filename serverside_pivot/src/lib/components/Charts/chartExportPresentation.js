/** Presentation metadata and download helpers shared by every chart renderer.  @param {any} [blob] @param {any} [filename] @returns {any} */

export const downloadBlob = (blob, filename) => {
    if (typeof window === 'undefined' || !blob) return;
    /** @type {any} */
    const url = window.URL.createObjectURL(blob);
    /** @type {any} */
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    window.URL.revokeObjectURL(url);
};

/** @param {any} [presentation] @returns {any} */
export const normalizeChartPresentation = (presentation = {}) => ({
    title: String(presentation.title || '').trim(),
    legendPosition: presentation.legendPosition === 'top' ? 'top' : 'bottom',
    legendItems: Array.isArray(presentation.legendItems) ? presentation.legendItems.filter(/** @param {any} [item] @returns {any} */ item => item && item.label) : [],
    background: presentation.background || '#ffffff',
    textColor: presentation.textColor || '#111827',
    secondaryTextColor: presentation.secondaryTextColor || '#4b5563',
});

/** @param {any} [container] @param {any} [title] @param {any} [legendPosition] @param {any} [theme] @returns {any} */
export const readChartPresentation = (container, title, legendPosition, theme) => {
    /** @type {any} */
    const legendItems = container
        ? Array.from(container.querySelectorAll('[data-chart-export-legend-item="true"]')).map((element) => ({
            label: element.getAttribute('data-chart-export-label') || element.textContent || '',
            color: element.getAttribute('data-chart-export-color') || theme.primary || '#2563eb',
            hidden: element.getAttribute('data-chart-export-hidden') === 'true',
        })).filter(item => item.label)
        : [];
    return normalizeChartPresentation({
        title,
        legendPosition,
        legendItems,
        background: theme.chartBg || theme.surfaceBg || theme.background || '#ffffff',
        textColor: theme.text || '#111827',
        secondaryTextColor: theme.textSec || theme.text || '#4b5563',
    });
};

/** @param {any} [items] @param {any} [width] @param {any} [estimateWidth] @returns {any} */
export const layoutLegendItems = (items, width, estimateWidth) => {
    /** @type {any[]} */
    const rows = [];
    /** @type {any[]} */
    let currentRow = [];
    let usedWidth = 0;
    items.forEach(/** @param {any} [item] @returns {any} */ (item) => {
        /** @type {any} */
        const itemWidth = Math.max(48, estimateWidth(item.label) + 30);
        if (currentRow.length > 0 && usedWidth + itemWidth > Math.max(80, width - 24)) {
            rows.push(currentRow);
            currentRow = [];
            usedWidth = 0;
        }
        currentRow.push({ ...item, width: itemWidth });
        usedWidth += itemWidth;
    });
    if (currentRow.length > 0) rows.push(currentRow);
    return rows;
};

/** @param {any} [value] @returns {any} */
const escapeSpreadsheetCsvValue = (value) => {
    /** @type {any} */
    const text = String(value ?? '');
    /** @type {any} */
    const safeText = typeof value === 'string' && /^[=+\-@]/.test(text) ? `'${text}` : text;
    return safeText.includes(',') || safeText.includes('"') || safeText.includes('\n')
        ? `"${safeText.replace(/"/g, '""')}"`
        : safeText;
};

/** @param {any} [model] @param {any} [fileStem] @param {any} [rawPresentation] @returns {any} */
export const exportChartCsv = (model, fileStem, rawPresentation) => {
    if (!model) return;
    /** @type {any} */
    const categories = Array.isArray(model.categories) ? model.categories : [];
    /** @type {any} */
    const series = Array.isArray(model.series) ? model.series : [];
    if (categories.length === 0 || series.length === 0) return;
    /** @type {any} */
    const header = ['Category', ...series.map(/** @param {any} [seriesItem] @returns {any} */ (seriesItem) => seriesItem.name)].map(escapeSpreadsheetCsvValue).join(',');
    /** @type {any} */
    const rows = categories.map(/** @param {any} [category] @param {any} [index] @returns {any} */ (category, index) => [category, ...series.map(/** @param {any} [seriesItem] @returns {any} */ (seriesItem) => seriesItem.values[index] ?? '')].map(escapeSpreadsheetCsvValue).join(','));
    /** @type {any} */
    const presentation = normalizeChartPresentation(rawPresentation);
    /** @type {any} */
    const legendLabels = presentation.legendItems.length > 0
        ? presentation.legendItems.map(/** @param {any} [item] @returns {any} */ item => item.label)
        : series.map(/** @param {any} [item] @returns {any} */ item => item.name).filter(Boolean);
    /** @type {any[]} */
    const metadata = [];
    if (presentation.title) metadata.push(['Title', presentation.title].map(escapeSpreadsheetCsvValue).join(','));
    if (legendLabels.length > 0) metadata.push(['Legend', ...legendLabels].map(escapeSpreadsheetCsvValue).join(','));
    if (metadata.length > 0) metadata.push('');
    downloadBlob(new Blob([`\ufeff${[...metadata, header, ...rows].join('\n')}`], { type: 'text/csv;charset=utf-8' }), `${fileStem}.csv`);
};
