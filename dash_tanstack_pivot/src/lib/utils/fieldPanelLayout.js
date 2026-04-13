const DEFAULT_PANEL_LIMITS = Object.freeze({
    minWidth: 120,
    maxWidth: 520,
    minHeight: 44,
    maxHeight: 560,
});

export const FIELD_PANEL_SIZE_LIMITS = Object.freeze({
    availableFields: Object.freeze({
        ...DEFAULT_PANEL_LIMITS,
        minHeight: 56,
    }),
    rows: Object.freeze({
        ...DEFAULT_PANEL_LIMITS,
        minHeight: 40,
    }),
    cols: Object.freeze({
        ...DEFAULT_PANEL_LIMITS,
        minHeight: 40,
    }),
    vals: Object.freeze({
        ...DEFAULT_PANEL_LIMITS,
        minHeight: 56,
    }),
    filter: Object.freeze({
        ...DEFAULT_PANEL_LIMITS,
        minHeight: 40,
    }),
});

export const DEFAULT_FIELD_PANEL_SIZES = Object.freeze({
    availableFields: Object.freeze({ width: null, height: 164 }),
    rows: Object.freeze({ width: null, height: 140 }),
    cols: Object.freeze({ width: null, height: 140 }),
    vals: Object.freeze({ width: null, height: 188 }),
    filter: Object.freeze({ width: null, height: 140 }),
});

function clampNumber(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

export function getFieldPanelLimits(panelId) {
    return FIELD_PANEL_SIZE_LIMITS[panelId] || DEFAULT_PANEL_LIMITS;
}

export function sanitizeFieldPanelSizeEntry(panelId, value) {
    const limits = getFieldPanelLimits(panelId);
    const defaults = DEFAULT_FIELD_PANEL_SIZES[panelId] || { width: null, height: limits.minHeight };
    const source = value && typeof value === 'object' ? value : {};
    const rawWidth = source.width === null || source.width === undefined || source.width === ''
        ? NaN
        : Number(source.width);
    const rawHeight = source.height === null || source.height === undefined || source.height === ''
        ? NaN
        : Number(source.height);

    return {
        width: Number.isFinite(rawWidth)
            ? clampNumber(Math.round(rawWidth), limits.minWidth, limits.maxWidth)
            : defaults.width,
        height: Number.isFinite(rawHeight)
            ? clampNumber(Math.round(rawHeight), limits.minHeight, limits.maxHeight)
            : defaults.height,
    };
}

export function sanitizeFieldPanelSizes(value) {
    const source = value && typeof value === 'object' ? value : {};
    return Object.keys(DEFAULT_FIELD_PANEL_SIZES).reduce((acc, panelId) => {
        acc[panelId] = sanitizeFieldPanelSizeEntry(panelId, source[panelId]);
        return acc;
    }, {});
}

export function mergeFieldPanelSize(currentSizes, panelId, nextPartialSize) {
    const base = sanitizeFieldPanelSizes(currentSizes);
    if (!Object.prototype.hasOwnProperty.call(DEFAULT_FIELD_PANEL_SIZES, panelId)) {
        return base;
    }

    return {
        ...base,
        [panelId]: sanitizeFieldPanelSizeEntry(panelId, {
            ...base[panelId],
            ...(nextPartialSize && typeof nextPartialSize === 'object' ? nextPartialSize : {}),
        }),
    };
}
