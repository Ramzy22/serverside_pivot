/** Pure layout and localized-text helpers used by the pivot composition layer.  @param {any} [headerGroupCount] @param {any} [rowHeight] @param {any} [showFloatingFilters] @returns {any} */

export const getStickyHeaderHeight = (headerGroupCount, rowHeight, showFloatingFilters) => (
    (headerGroupCount * rowHeight) + (showFloatingFilters ? rowHeight : 0)
);

/** @param {any} [template] @param {any} [values] @returns {any} */
export const formatLocaleTextTemplate = (template, values = {}) => {
    /** @type {any} */
    const text = template === null || template === undefined ? '' : String(template);
    if (!values || typeof values !== 'object') return text;
    return text.replace(/\{([A-Za-z0-9_]+)\}/g, /** @param {any} [match] @param {any} [key] @returns {any} */ (match, key) => (
        Object.prototype.hasOwnProperty.call(values, key) ? String(values[key]) : match
    ));
};

/** @type {any} */
const FIT_FIXED_COL_IDS = new Set(['__row_number__', '__row_checkbox__', '__report_config__']);

/** @param {any} [p0] @returns {any} */
export const computeFitToWidthLayout = ({
    columns = [],
    containerWidth = 0,
    baseFontSize = 13,
    minFontSize = 10,
} = {}) => {
    /** @type {any} */
    const numericContainerWidth = Number(containerWidth);
    /** @type {any} */
    const parsedFontSize = typeof baseFontSize === 'number'
        ? baseFontSize
        : (parseFloat(String(baseFontSize || '13')) || 13);
    if (!Number.isFinite(numericContainerWidth) || numericContainerWidth <= 0 || !Array.isArray(columns) || columns.length === 0) {
        return { columnSizing: {}, fontSize: parsedFontSize };
    }
    /** @type {any[]} */
    const fixedCols = [];
    /** @type {any[]} */
    const scalableCols = [];
    columns.forEach((col) => {
        if (!col || !col.id) return;
        /** @type {any} */
        const base = Math.max(24, Number(col.baseSize) || 100);
        if (FIT_FIXED_COL_IDS.has(col.id)) {
            fixedCols.push({ id: col.id, base });
        } else {
            scalableCols.push({ id: col.id, base });
        }
    });
    if (scalableCols.length === 0) {
        return { columnSizing: {}, fontSize: parsedFontSize };
    }
    /** @type {any} */
    const fixedWidth = fixedCols.reduce((sum, col) => sum + col.base, 0);
    /** @type {any} */
    const availableWidth = Math.max(60, Math.floor(numericContainerWidth) - fixedWidth - 2);
    /** @type {any} */
    const totalBaseline = scalableCols.reduce((sum, col) => sum + col.base, 0);
    if (totalBaseline <= 0) {
        return { columnSizing: {}, fontSize: parsedFontSize };
    }

    /** @type {any} */
    const ratio = availableWidth / totalBaseline;
    /** @type {any} */
    const perColFloor = Math.min(28, Math.max(16, Math.floor(availableWidth / scalableCols.length)));
    /** @type {any} */
    const widths = scalableCols.map((col) => {
        /** @type {any} */
        const minAllowed = col.id === 'hierarchy'
            ? Math.min(Math.max(perColFloor, 56), Math.max(perColFloor, Math.floor(availableWidth * 0.36)))
            : perColFloor;
        return Math.max(minAllowed, Math.floor(col.base * ratio));
    });
    /** @type {any} */
    let currentSum = widths.reduce(/** @param {any} [sum] @param {any} [w] @returns {any} */ (sum, w) => sum + w, 0);
    if (currentSum !== availableWidth && widths.length > 0) {
        /** @type {any} */
        const diff = availableWidth - currentSum;
        if (diff > 0) {
            widths[0] += diff;
        } else {
            // Trim proportionally from columns above floor
            let remaining = -diff;
            for (let i = widths.length - 1; i >= 0 && remaining > 0; i -= 1) {
                const col = scalableCols[i];
                /** @type {any} */
                const minAllowed = col.id === 'hierarchy' ? Math.max(perColFloor, 48) : perColFloor;
                /** @type {any} */
                const spare = Math.max(0, widths[i] - minAllowed);
                /** @type {any} */
                const take = Math.min(spare, remaining);
                widths[i] -= take;
                remaining -= take;
            }
        }
    }
    /** @type {Record<string, any>} */
    const columnSizing = {};
    scalableCols.forEach((col, idx) => {
        columnSizing[col.id] = widths[idx];
    });
    /** @type {any} */
    const fittedFontSize = ratio < 1 && (numericContainerWidth <= 640 || ratio < 0.85)
        ? Math.max(minFontSize, Math.round(parsedFontSize * Math.max(0.7, ratio)))
        : parsedFontSize;
    return { columnSizing, fontSize: fittedFontSize };
};
