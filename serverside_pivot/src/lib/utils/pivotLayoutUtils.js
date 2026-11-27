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

/** Measure the label as rendered, including CSS tracking and optional sort chrome.
 * @param {any} text @param {any} options @returns {number}
 */
export const measureHeaderLabel = (text, {
    measureText,
    fontSize = 14,
    theme = {},
    measureSubHeader = true,
    wrap = false,
    canSort = false,
    sortIndicator = theme.sortIndicator || 'arrow',
} = {}) => {
    const px = parseNumericPx(fontSize, 14);
    const measure = measureText || createTextWidthMeasurer(`${theme.headerFontWeight ?? 600} ${px}px ${theme.fontFamily || 'Inter, system-ui, sans-serif'}`, px * .62);
    const transform = measureSubHeader ? (theme.headerTextTransform ?? 'uppercase') : 'none';
    const tracking = measureSubHeader ? (theme.headerLetterSpacing ?? '0.04em') : 0;
    const spacing = String(tracking).endsWith('em') ? parseNumericPx(tracking, 0) * px : parseNumericPx(tracking, 0);
    let label = String(text ?? '');
    if (transform === 'uppercase') label = label.toUpperCase();
    else if (transform === 'lowercase') label = label.toLowerCase();
    else if (transform === 'capitalize') label = label.replace(/\b\w/g, char => char.toUpperCase());
    const width = /** @param {string} word */ word => measure(word) + [...word].length * spacing;
    return (wrap ? measureLongestHeaderWord(label, width) : width(label)) + (canSort && sortIndicator !== 'none' ? 14 : 0);
};

/** @param {any} [p0] @returns {any} */
export const measureColumnFitMetric = ({
    columnId,
    baseSize = 100,
    minSize = 20,
    rows = [],
    getCellRawValue,
    formatCellText,
    formatHierarchyLabel,
    getHierarchyLabel,
    headerLines = [],
    shortLabel = null,
    headerWrap = false,
    headerControlsMode = 'hover',
    canSort = true,
    sortIndicator = 'arrow',
    measureBodyText,
    measureHeaderText,
    measureGroupHeaderText = measureHeaderText,
    cellPaddingX = 12,
    headerPaddingX = 12,
    numericPaddingRight = 8,
    hierarchyIndentPx = 24,
    hierarchyExpandPx = 22,
    hierarchyLeafPx = 18,
    cellOverscan = 4,
    headerOverscan = 4,
} = {}) => {
    const isHierarchy = columnId === 'hierarchy';
    const base = Math.max(24, Number(baseSize) || 100);
    const cellHorizontalPad = (cellPaddingX * 2) + (isHierarchy ? 0 : numericPaddingRight) + cellOverscan;
    // Keep text and fixed indentation coupled to the row they came from.
    /** @type {Map<number, number>} */
    const bodyWidthsByIndent = new Map();
    (Array.isArray(rows) ? rows : []).forEach((row) => {
        if (!row) return;
        const original = row.original || row;
        let indent = 0;
        let textWidth;
        if (isHierarchy) {
            const depth = typeof original?.depth === 'number'
                ? original.depth
                : (typeof original?._depth === 'number' ? original._depth : (row.depth || 0));
            const rawHierarchy = typeof getHierarchyLabel === 'function'
                ? getHierarchyLabel(row)
                : (original?._label ?? original?._id ?? '');
            const label = typeof formatHierarchyLabel === 'function' ? formatHierarchyLabel(rawHierarchy) : rawHierarchy;
            const canExpand = !original?._isTotal && (row.getCanExpand?.() ?? Boolean(original?._has_children));
            const reportIndent = Math.max(0, Number(original?._reportFormat?.indent) || 0);
            indent = Math.max(0, depth) * hierarchyIndentPx + reportIndent + (canExpand ? hierarchyExpandPx : hierarchyLeafPx);
            textWidth = measureBodyText(label, row);
        } else {
            const rawVal = typeof getCellRawValue === 'function' ? getCellRawValue(row, columnId) : original?.[columnId];
            const formatted = typeof formatCellText === 'function' ? formatCellText(rawVal, row) : String(rawVal ?? '');
            textWidth = measureBodyText(formatted, row);
        }
        bodyWidthsByIndent.set(indent, Math.max(bodyWidthsByIndent.get(indent) || 0, textWidth));
    });

    let fullHeaderTextWidth = 0;
    let minLeafText = 0;
    let minGroupText = 0;
    (Array.isArray(headerLines) ? headerLines : []).forEach((line, index) => {
        const isLeaf = index === headerLines.length - 1;
        const measure = isLeaf ? measureHeaderText : measureGroupHeaderText;
        const fullWidth = measure(line);
        const minWidth = headerWrap ? measureLongestHeaderWord(line, measure) : fullWidth;
        fullHeaderTextWidth = Math.max(fullHeaderTextWidth, fullWidth);
        if (isLeaf) minLeafText = minWidth;
        else minGroupText = Math.max(minGroupText, minWidth);
    });
    if (shortLabel) minLeafText = Math.min(minLeafText, measureHeaderText(String(shortLabel)));
    const sortReserve = canSort && sortIndicator !== 'none' ? 14 : 0;
    const controlsReserve = headerControlsMode === 'always' ? 28 : 0;
    const totalHeaderPad = (headerPaddingX * 2) + sortReserve + controlsReserve + headerOverscan;
    /** @param {any} compactTheme @param {any} bodyScale @param {any} headerScale @param {any} groupScale */
    const getRequiredWidth = (compactTheme, bodyScale, headerScale, groupScale) => {
        let bodyWidth = 0;
        bodyWidthsByIndent.forEach((width, indent) => { bodyWidth = Math.max(bodyWidth, width * bodyScale + indent); });
        const bodyPad = compactTheme
            ? (compactTheme.cellPaddingX * 2) + (isHierarchy ? 0 : compactTheme.numericPaddingRight) + cellOverscan
            : cellHorizontalPad;
        const headerPad = (compactTheme?.headerPaddingX ?? headerPaddingX) * 2 + headerOverscan;
        return Math.ceil(Math.max(
            minSize,
            bodyWidthsByIndent.size === 0 ? base * bodyScale : bodyWidth + bodyPad,
            minLeafText * headerScale + headerPad + sortReserve + controlsReserve,
            minGroupText * groupScale + headerPad
        ));
    };
    const contentSize = getRequiredWidth(null, 1, 1, 1);

    return {
        id: columnId,
        baseSize: base,
        contentSize,
        preferredSize: Math.ceil(Math.max(contentSize, fullHeaderTextWidth + totalHeaderPad)),
        getRequiredWidth,
    };
};

/** @param {any} [theme] @param {any} [fitScale] @param {any} [fittedFontSize] @param {any} [minFontSize] @param {number} [paddingScale] @returns {any} */
export const computeCompactFitThemeOverrides = (theme, fitScale, fittedFontSize, minFontSize = 10, paddingScale) => {
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
    const padScale = paddingScale ?? Math.max(0.28, safeScale);

    return {
        fontSize: fittedFontSize,
        headerFontSize: Math.min(baseHeaderSize, Math.max(parsedFloor, floorQuarterPx(baseHeaderSize * safeScale))),
        headerGroupFontSize: Math.min(baseGroupHeaderSize, Math.max(parsedFloor, floorQuarterPx(baseGroupHeaderSize * safeScale))),
        cellPaddingX: Math.min(baseCellPad, Math.max(2, Math.round(baseCellPad * padScale))),
        headerPaddingX: Math.min(baseHeaderPad, Math.max(2, Math.round(baseHeaderPad * padScale))),
        numericPaddingRight: Math.min(baseNumPad, Math.max(1, Math.round(baseNumPad * padScale))),
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
    theme = null,
    getFixedColumnSize = null,
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
            getRequiredWidth: col.getRequiredWidth,
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
    const fixedWidth = fixedCols.reduce((sum, col) => sum + (getFixedColumnSize?.(col.id) ?? col.base), 0);
    /** @type {any} */
    const availableWidth = Math.max(0, Math.floor(numericContainerWidth) - fixedWidth - 2);
    const hasContentMeasurements = scalableCols.some((col) => col.contentSize !== null);

    if (hasContentMeasurements) {
        const contentCols = scalableCols.map((col) => ({
            ...col,
            content: col.contentSize ?? col.base,
            preferred: col.preferredSize ?? col.contentSize ?? col.base,
        }));
        /** @param {number[]} widths */
        const sum = (widths) => widths.reduce((total, width) => total + width, 0);
        /** @param {number[]} widths */
        const sizingFor = (widths) => Object.fromEntries(contentCols.map((col, index) => [col.id, widths[index]]));
        const baseWidths = contentCols.map((col) => Math.ceil(col.content));

        if (sum(baseWidths) <= availableWidth) {
            const deficits = contentCols.map((col, index) => Math.max(0, Math.ceil(col.preferred) - baseWidths[index]));
            const deficit = sum(deficits);
            const share = deficit ? Math.min(1, (availableWidth - sum(baseWidths)) / deficit) : 0;
            const widths = baseWidths.map((width, index) => width + Math.floor(deficits[index] * share));
            return {
                columnSizing: sizingFor(distributeSurplusWidth(widths, availableWidth, contentCols.map((col) => col.preferred))),
                fontSize: parsedFontSize,
            };
        }

        const baseTheme = { ...theme, fontSize: parsedFontSize };
        const baseHeaderSize = parseNumericPx(baseTheme.headerFontSize, parsedFontSize);
        const baseGroupSize = parseNumericPx(baseTheme.headerGroupFontSize, baseHeaderSize);
        /** @param {number} fontSize @param {number} [paddingScale] */
        const layoutAt = (fontSize, paddingScale) => {
            const scale = fontSize / parsedFontSize;
            const compactTheme = computeCompactFitThemeOverrides(baseTheme, scale, fontSize, effectiveMinFontSize, paddingScale);
            const headerScale = compactTheme.headerFontSize / baseHeaderSize;
            const groupScale = compactTheme.headerGroupFontSize / baseGroupSize;
            const widths = contentCols.map((col) => col.getRequiredWidth
                ? col.getRequiredWidth(compactTheme, scale, headerScale, groupScale)
                : Math.max(20, Math.ceil(col.content * scale)));
            return { fontSize, widths, compactTheme };
        };

        let layout = layoutAt(effectiveMinFontSize);
        if (sum(layout.widths) <= availableWidth) {
            // Search the quarter-pixel sizes above the floor for the largest fit.
            let low = Math.floor(effectiveMinFontSize * 4) + 1;
            let high = Math.ceil(parsedFontSize * 4) - 1;
            while (low <= high) {
                const midpoint = Math.floor((low + high) / 2);
                const candidate = layoutAt(midpoint / 4);
                if (sum(candidate.widths) <= availableWidth) {
                    layout = candidate;
                    low = midpoint + 1;
                } else {
                    high = midpoint - 1;
                }
            }
        } else {
            // Give padding back at the floor. If the text still needs more space,
            // keep its measured width and let the viewport scroll.
            layout = layoutAt(effectiveMinFontSize, 0);
        }
        const { widths, fontSize, compactTheme } = layout;
        return {
            columnSizing: sizingFor(distributeSurplusWidth(widths, availableWidth, widths)),
            fontSize,
            compactTheme,
        };
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
