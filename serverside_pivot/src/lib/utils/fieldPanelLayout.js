/** @type {any} */
const DEFAULT_PANEL_LIMITS = Object.freeze({
    minWidth: 120,
    maxWidth: 520,
    minHeight: 44,
    maxHeight: 560,
});

/** @type {any} */
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

/** @type {any} */
export const DEFAULT_FIELD_PANEL_SIZES = Object.freeze({
    availableFields: Object.freeze({ width: null, height: 164 }),
    rows: Object.freeze({ width: null, height: 140 }),
    cols: Object.freeze({ width: null, height: 140 }),
    vals: Object.freeze({ width: null, height: 188 }),
    filter: Object.freeze({ width: null, height: 140 }),
});

/** @param {any} [value] @param {any} [min] @param {any} [max] @returns {any} */
function clampNumber(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

/** @param {any} [panelId] @returns {any} */
export function getFieldPanelLimits(panelId) {
    return FIELD_PANEL_SIZE_LIMITS[panelId] || DEFAULT_PANEL_LIMITS;
}

/** @param {any} [panelId] @param {any} [value] @returns {any} */
export function sanitizeFieldPanelSizeEntry(panelId, value) {
    /** @type {any} */
    const limits = getFieldPanelLimits(panelId);
    /** @type {any} */
    const defaults = DEFAULT_FIELD_PANEL_SIZES[panelId] || { width: null, height: limits.minHeight };
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    /** @type {any} */
    const rawWidth = source.width === null || source.width === undefined || source.width === ''
        ? NaN
        : Number(source.width);
    /** @type {any} */
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

/** @param {any} [value] @returns {any} */
export function sanitizeFieldPanelSizes(value) {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    return Object.keys(DEFAULT_FIELD_PANEL_SIZES).reduce((acc, panelId) => {
        acc[panelId] = sanitizeFieldPanelSizeEntry(panelId, source[panelId]);
        return acc;
    }, /** @type {Record<string, any>} */ ({}));
}

/** @param {any} [currentSizes] @param {any} [panelId] @param {any} [nextPartialSize] @returns {any} */
export function mergeFieldPanelSize(currentSizes, panelId, nextPartialSize) {
    /** @type {any} */
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
