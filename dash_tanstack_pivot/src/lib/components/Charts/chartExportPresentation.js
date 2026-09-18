/** Presentation metadata and download helpers shared by every chart renderer. */

export const downloadBlob = (blob, filename) => {
    if (typeof window === 'undefined' || !blob) return;
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    window.URL.revokeObjectURL(url);
};

export const normalizeChartPresentation = (presentation = {}) => ({
    title: String(presentation.title || '').trim(),
    legendPosition: presentation.legendPosition === 'top' ? 'top' : 'bottom',
    legendItems: Array.isArray(presentation.legendItems) ? presentation.legendItems.filter(item => item && item.label) : [],
    background: presentation.background || '#ffffff',
    textColor: presentation.textColor || '#111827',
    secondaryTextColor: presentation.secondaryTextColor || '#4b5563',
});

export const readChartPresentation = (container, title, legendPosition, theme) => {
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

export const layoutLegendItems = (items, width, estimateWidth) => {
    const rows = [];
    let currentRow = [];
    let usedWidth = 0;
    items.forEach((item) => {
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

const escapeSpreadsheetCsvValue = (value) => {
    const text = String(value ?? '');
    const safeText = typeof value === 'string' && /^[=+\-@]/.test(text) ? `'${text}` : text;
    return safeText.includes(',') || safeText.includes('"') || safeText.includes('\n')
        ? `"${safeText.replace(/"/g, '""')}"`
        : safeText;
};

export const exportChartCsv = (model, fileStem, rawPresentation) => {
    if (!model) return;
    const categories = Array.isArray(model.categories) ? model.categories : [];
    const series = Array.isArray(model.series) ? model.series : [];
    if (categories.length === 0 || series.length === 0) return;
    const header = ['Category', ...series.map((seriesItem) => seriesItem.name)].map(escapeSpreadsheetCsvValue).join(',');
    const rows = categories.map((category, index) => [category, ...series.map((seriesItem) => seriesItem.values[index] ?? '')].map(escapeSpreadsheetCsvValue).join(','));
    const presentation = normalizeChartPresentation(rawPresentation);
    const legendLabels = presentation.legendItems.length > 0
        ? presentation.legendItems.map(item => item.label)
        : series.map(item => item.name).filter(Boolean);
    const metadata = [];
    if (presentation.title) metadata.push(['Title', presentation.title].map(escapeSpreadsheetCsvValue).join(','));
    if (legendLabels.length > 0) metadata.push(['Legend', ...legendLabels].map(escapeSpreadsheetCsvValue).join(','));
    if (metadata.length > 0) metadata.push('');
    downloadBlob(new Blob([`\ufeff${[...metadata, header, ...rows].join('\n')}`], { type: 'text/csv;charset=utf-8' }), `${fileStem}.csv`);
};
