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

/** @param {any} [value] @param {any} [fallback] @returns {any} */
const parseNumericPx = (value, fallback) => {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim()) {
        /** @type {any} */
        const parsed = Number.parseFloat(value.trim());
        if (Number.isFinite(parsed)) return parsed;
    }
    return fallback;
};

/** @param {any} [value] @returns {any} */
const floorQuarterPx = (value) => {
    if (!Number.isFinite(value) || value <= 0) return value;
    return Math.floor(value * 4 + 1e-6) / 4;
};

/** @param {any} [font] @param {any} [fallbackCharPx] @returns {any} */
export const createTextWidthMeasurer = (font, fallbackCharPx = 7.5) => {
    /** @type {any} */
    let context = null;
    if (typeof document !== 'undefined' && typeof document.createElement === 'function') {
        try {
            const canvas = document.createElement('canvas');
            context = canvas && typeof canvas.getContext === 'function' ? canvas.getContext('2d') : null;
            if (context && font) context.font = font;
        } catch (_err) {
            context = null;
        }
    }
    /** @param {any} [text] @returns {any} */
    return (text) => {
        const str = text === null || text === undefined ? '' : String(text);
        if (!str) return 0;
        if (context && typeof context.measureText === 'function') {
            if (font && context.font !== font) context.font = font;
            const measured = context.measureText(str);
            if (measured && Number.isFinite(measured.width)) return measured.width;
        }
        return str.length * fallbackCharPx;
    };
};

/** @param {any} [text] @param {any} [measureFn] @returns {any} */
export const measureLongestHeaderWord = (text, measureFn) => {
    const words = String(text === null || text === undefined ? '' : text)
        .trim()
        .split(/\s+/)
        .filter(Boolean);
    if (words.length === 0) return 0;
    let maxWidth = 0;
    words.forEach((word) => {
        maxWidth = Math.max(maxWidth, measureFn(word));
    });
    return maxWidth;
};

/** @param {any} [p0] @returns {any} */
export const measureColumnFitMetric = ({
    columnId,
    baseSize = 100,
    rows = [],
    getCellRawValue,
    formatCellText,
    formatHierarchyLabel,
    headerLines = [],
    shortLabel = null,
    headerWrap = false,
    headerControlsMode = 'always',
    canSort = true,
    sortIndicator = 'arrow',
    measureBodyText,
    measureHeaderText,
    cellPaddingX = 12,
    headerPaddingX = 12,
    numericPaddingRight = 8,
    hierarchyIndentPx = 24,
    hierarchyExpandPx = 22,
    cellOverscan = 4,
    headerOverscan = 4,
} = {}) => {
    const isHierarchy = columnId === 'hierarchy';
    const base = Math.max(24, Number(baseSize) || 100);
    const cellHorizontalPad = (cellPaddingX * 2) + (isHierarchy ? 0 : numericPaddingRight) + cellOverscan;
    const minCellPad = isHierarchy
        ? Math.max(18, Math.round(cellPaddingX * 0.7) + hierarchyExpandPx)
        : Math.max(4, Math.round(((cellPaddingX * 2) + numericPaddingRight) * 0.35));

    let maxCellTextWidth = 0;
    let maxHierarchyChrome = isHierarchy ? hierarchyExpandPx : 0;
    (Array.isArray(rows) ? rows : []).forEach((row) => {
        if (!row) return;
        const original = row.original || row;
        if (isHierarchy) {
            const depth = typeof original?.depth === 'number'
                ? original.depth
                : (typeof original?._depth === 'number' ? original._depth : (row.depth || 0));
            const rawHierarchy = original?._label != null
                ? String(original._label)
                : (original?._id != null ? String(original._id) : '');
            const label = typeof formatHierarchyLabel === 'function' ? formatHierarchyLabel(rawHierarchy) : rawHierarchy;
            maxHierarchyChrome = Math.max(maxHierarchyChrome, (depth * hierarchyIndentPx) + hierarchyExpandPx);
            maxCellTextWidth = Math.max(maxCellTextWidth, measureBodyText(label));
        } else {
            const rawVal = typeof getCellRawValue === 'function' ? getCellRawValue(row, columnId) : original?.[columnId];
            const formatted = typeof formatCellText === 'function' ? formatCellText(rawVal, row) : String(rawVal ?? '');
            maxCellTextWidth = Math.max(maxCellTextWidth, measureBodyText(formatted));
        }
    });

    const sampleRowCount = Array.isArray(rows) ? rows.length : 0;
    const totalCellChrome = cellHorizontalPad + (isHierarchy ? maxHierarchyChrome : 0);
    const cellContentWidth = sampleRowCount > 0
        ? (maxCellTextWidth + totalCellChrome)
        : Math.max(maxCellTextWidth + totalCellChrome, base);

    let fullHeaderTextWidth = 0;
    let widestWordTextWidth = 0;
    (Array.isArray(headerLines) ? headerLines : []).forEach((line) => {
        fullHeaderTextWidth = Math.max(fullHeaderTextWidth, measureHeaderText(line));
        widestWordTextWidth = Math.max(widestWordTextWidth, measureLongestHeaderWord(line, measureHeaderText));
    });
    const shortTextWidth = shortLabel ? measureHeaderText(String(shortLabel)) : null;

    const sortReserve = canSort && sortIndicator !== 'none' ? 14 : 0;
    const controlsReserve = headerControlsMode === 'always' ? 28 : 4;
    const totalHeaderPad = (headerPaddingX * 2) + sortReserve + controlsReserve + headerOverscan;
    const compactHeaderPad = Math.max(4, Math.round(headerPaddingX * 2 * 0.35)) + (sortReserve ? 10 : 0) + 2;

    const requiredHeaderText = shortTextWidth !== null
        ? Math.min(headerWrap ? widestWordTextWidth : fullHeaderTextWidth, shortTextWidth)
        : (headerWrap ? widestWordTextWidth : fullHeaderTextWidth);

    const headerMinWidth = requiredHeaderText + totalHeaderPad;
    const headerFullWidth = fullHeaderTextWidth + totalHeaderPad;
    const contentSize = Math.ceil(Math.max(cellContentWidth, headerMinWidth));
    const preferredSize = Math.ceil(Math.max(contentSize, headerFullWidth));
    const textSize = Math.max(maxCellTextWidth, requiredHeaderText);
    const paddingSize = Math.max(0, contentSize - textSize);
    const minPaddingSize = Math.ceil(
        cellContentWidth >= headerMinWidth
            ? Math.min(totalCellChrome, minCellPad)
            : Math.min(totalHeaderPad, compactHeaderPad)
    );

    return {
        id: columnId,
        baseSize: base,
        contentSize,
        preferredSize,
        textSize,
        paddingSize,
        minPaddingSize,
    };
};

/** @param {any} [theme] @param {any} [fitScale] @param {any} [fittedFontSize] @param {any} [minFontSize] @returns {any} */
export const computeCompactFitThemeOverrides = (theme, fitScale, fittedFontSize, minFontSize = 10) => {
    const themeObj = theme && typeof theme === 'object' ? theme : {};
    const safeScale = Number.isFinite(fitScale) && fitScale > 0 ? Math.min(1, fitScale) : 1;
    const parsedFloor = parseNumericPx(minFontSize, 10);
    const baseHeaderSize = parseNumericPx(
        themeObj.headerFontSize !== undefined && themeObj.headerFontSize !== null ? themeObj.headerFontSize : themeObj.fontSize,
        13
    );
    const baseGroupHeaderSize = parseNumericPx(
        themeObj.headerGroupFontSize !== undefined && themeObj.headerGroupFontSize !== null
            ? themeObj.headerGroupFontSize
            : baseHeaderSize,
        baseHeaderSize
    );
    const baseCellPad = parseNumericPx(themeObj.cellPaddingX, 12);
    const baseHeaderPad = parseNumericPx(
        themeObj.headerPaddingX !== undefined && themeObj.headerPaddingX !== null ? themeObj.headerPaddingX : baseCellPad,
        baseCellPad
    );
    const baseNumPad = parseNumericPx(themeObj.numericPaddingRight, 8);
    const padScale = Math.max(0.28, safeScale);

    return {
        fontSize: fittedFontSize,
        headerFontSize: Math.max(parsedFloor, floorQuarterPx(baseHeaderSize * safeScale)),
        headerGroupFontSize: Math.max(parsedFloor, floorQuarterPx(baseGroupHeaderSize * safeScale)),
        cellPaddingX: Math.max(2, Math.round(baseCellPad * padScale)),
        headerPaddingX: Math.max(2, Math.round(baseHeaderPad * padScale)),
        numericPaddingRight: Math.max(1, Math.round(baseNumPad * padScale)),
    };
};

/** @param {any} [widths] @param {any} [targetSum] @param {any} [weights] @returns {any} */
const distributeSurplusWidth = (widths, targetSum, weights) => {
    const total = widths.reduce(/** @param {any} [sum] @param {any} [width] @returns {any} */ (sum, width) => sum + width, 0);
    let diff = Math.round(targetSum) - total;
    if (diff <= 0 || widths.length === 0) return widths;
    const next = widths.slice();
    const safeWeights = weights.map(/** @param {any} [w] @returns {any} */ (w) => Math.max(1, Number(w) || 1));
    const totalWeight = safeWeights.reduce(/** @param {any} [sum] @param {any} [w] @returns {any} */ (sum, w) => sum + w, 0);
    if (totalWeight > 0) {
        for (let i = 0; i < next.length && diff > 0; i += 1) {
            const share = Math.min(diff, Math.floor((diff * safeWeights[i]) / totalWeight));
            if (share > 0) {
                next[i] += share;
                diff -= share;
            }
        }
    }
    let idx = 0;
    while (diff > 0) {
        next[idx % next.length] += 1;
        diff -= 1;
        idx += 1;
    }
    return next;
};

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
    const parsedFontSize = parseNumericPx(baseFontSize, 13);
    /** @type {any} */
    const rawMinFontSize = parseNumericPx(minFontSize, 10);
    /** @type {any} */
    const effectiveMinFontSize = Math.min(
        parsedFontSize,
        Number.isFinite(rawMinFontSize) && rawMinFontSize > 0 ? rawMinFontSize : 10
    );
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
        /** @type {Record<string, any>} */
        const entry = {
            id: col.id,
            base,
            contentSize: Number.isFinite(Number(col.contentSize)) && Number(col.contentSize) > 0 ? Number(col.contentSize) : null,
            preferredSize: Number.isFinite(Number(col.preferredSize)) && Number(col.preferredSize) > 0 ? Number(col.preferredSize) : null,
            textSize: Number.isFinite(Number(col.textSize)) && Number(col.textSize) >= 0 ? Number(col.textSize) : null,
            paddingSize: Number.isFinite(Number(col.paddingSize)) && Number(col.paddingSize) >= 0 ? Number(col.paddingSize) : null,
            minPaddingSize: Number.isFinite(Number(col.minPaddingSize)) && Number(col.minPaddingSize) >= 0 ? Number(col.minPaddingSize) : null,
        };
        if (FIT_FIXED_COL_IDS.has(col.id)) {
            fixedCols.push(entry);
        } else {
            scalableCols.push(entry);
        }
    });
    if (scalableCols.length === 0) {
        return { columnSizing: {}, fontSize: parsedFontSize };
    }
    /** @type {any} */
    const fixedWidth = fixedCols.reduce((sum, col) => sum + col.base, 0);
    /** @type {any} */
    const availableWidth = Math.max(60, Math.floor(numericContainerWidth) - fixedWidth - 2);
    const hasContentMeasurements = scalableCols.some((col) => col.contentSize !== null);

    if (hasContentMeasurements) {
        const contentCols = scalableCols.map((col) => {
            const content = col.contentSize !== null ? col.contentSize : col.base;
            const preferred = col.preferredSize !== null ? Math.max(content, col.preferredSize) : Math.max(content, col.base);
            const textWidth = col.textSize !== null ? col.textSize : Math.max(8, content - 16);
            const padWidth = col.paddingSize !== null ? col.paddingSize : Math.max(0, content - textWidth);
            const minPad = col.minPaddingSize !== null ? Math.min(padWidth, col.minPaddingSize) : Math.min(padWidth, 6);
            return { id: col.id, base: col.base, content, preferred, textWidth, padWidth, minPad };
        });
        const totalContentAtBase = contentCols.reduce((sum, col) => sum + col.content, 0);

        if (totalContentAtBase <= availableWidth) {
            let widths = contentCols.map((col) => Math.max(16, Math.ceil(col.content)));
            let currentSum = widths.reduce((sum, w) => sum + w, 0);
            if (currentSum <= availableWidth) {
                const headerDeficits = contentCols.map((col, idx) => Math.max(0, Math.ceil(col.preferred) - widths[idx]));
                const totalDeficit = headerDeficits.reduce((sum, d) => sum + d, 0);
                if (totalDeficit > 0) {
                    const room = availableWidth - currentSum;
                    const takeRatio = Math.min(1, room / totalDeficit);
                    widths = widths.map((w, idx) => w + Math.floor(headerDeficits[idx] * takeRatio));
                    currentSum = widths.reduce((sum, w) => sum + w, 0);
                }
                widths = distributeSurplusWidth(
                    widths,
                    availableWidth,
                    contentCols.map((col) => col.preferred)
                );
            } else {
                const trimOrder = widths.map((w, idx) => ({ idx, spare: Math.max(0, w - 16) }));
                let excess = currentSum - availableWidth;
                for (let i = trimOrder.length - 1; i >= 0 && excess > 0; i -= 1) {
                    const take = Math.min(trimOrder[i].spare, excess);
                    widths[i] -= take;
                    excess -= take;
                }
            }
            /** @type {Record<string, any>} */
            const columnSizing = {};
            contentCols.forEach((col, idx) => {
                columnSizing[col.id] = widths[idx];
            });
            return { columnSizing, fontSize: parsedFontSize };
        }

        const totalMinPad = contentCols.reduce((sum, col) => sum + col.minPad, 0);
        const totalLinear = contentCols.reduce(
            (sum, col) => sum + col.textWidth + Math.max(0, col.padWidth - col.minPad),
            0
        );
        const targetScale = totalLinear > 0
            ? Math.max(0.05, Math.min(1, (availableWidth - totalMinPad) / totalLinear))
            : 1;
        const candidateFont = floorQuarterPx(parsedFontSize * targetScale);
        const fittedFontSize = Math.max(
            effectiveMinFontSize,
            Math.min(parsedFontSize, candidateFont > 0 ? candidateFont : effectiveMinFontSize)
        );
        const effectiveScale = parsedFontSize > 0 ? Math.min(1, fittedFontSize / parsedFontSize) : targetScale;

        const targetWidths = contentCols.map(
            (col) => col.minPad + (col.textWidth + Math.max(0, col.padWidth - col.minPad)) * effectiveScale
        );
        const totalTarget = targetWidths.reduce((sum, w) => sum + w, 0);
        const colFloor = Math.min(20, Math.max(12, Math.floor((availableWidth / contentCols.length) * 0.55)));
        let widths;
        if (totalTarget <= availableWidth) {
            widths = targetWidths.map((w) => Math.max(colFloor, Math.floor(w)));
            const sumFloor = widths.reduce((sum, w) => sum + w, 0);
            if (sumFloor < availableWidth) {
                widths = distributeSurplusWidth(widths, availableWidth, targetWidths);
            }
        } else {
            const fitRatio = availableWidth / totalTarget;
            widths = targetWidths.map((w) => Math.max(colFloor, Math.floor(w * fitRatio)));
        }
        let finalSum = widths.reduce(/** @param {any} [sum] @param {any} [w] @returns {any} */ (sum, w) => sum + w, 0);
        if (finalSum > availableWidth) {
            let remaining = finalSum - availableWidth;
            for (let i = widths.length - 1; i >= 0 && remaining > 0; i -= 1) {
                const spare = Math.max(0, widths[i] - colFloor);
                const take = Math.min(spare, remaining);
                widths[i] -= take;
                remaining -= take;
            }
        } else if (finalSum < availableWidth) {
            widths = distributeSurplusWidth(widths, availableWidth, targetWidths);
        }
        /** @type {Record<string, any>} */
        const columnSizing = {};
        contentCols.forEach((col, idx) => {
            columnSizing[col.id] = widths[idx];
        });
        return { columnSizing, fontSize: fittedFontSize };
    }

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
    const allowDeepScale = effectiveMinFontSize < parsedFontSize * 0.7;
    const scaleFactor = allowDeepScale ? ratio : Math.max(0.7, ratio);
    /** @type {any} */
    const fittedFontSize = ratio < 1 && (numericContainerWidth <= 640 || ratio < 0.85)
        ? Math.max(
            effectiveMinFontSize,
            Math.min(
                parsedFontSize,
                allowDeepScale ? floorQuarterPx(parsedFontSize * scaleFactor) : Math.round(parsedFontSize * scaleFactor)
            )
        )
        : parsedFontSize;
    return { columnSizing, fontSize: fittedFontSize };
};
