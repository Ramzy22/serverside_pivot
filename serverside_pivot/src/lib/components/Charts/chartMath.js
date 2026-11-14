/** Pure chart calculations shared by SVG and ECharts presentation paths.  @param {any} [value] @param {any} [fallback] @param {any} [minWidth] @param {any} [maxWidth] @returns {any} */

export const clampChartPanelBaseWidth = (value, fallback = 430, minWidth, maxWidth) => {
    /** @type {any} */
    const numericValue = Number(value);
    /** @type {any} */
    const numericFallback = Number(fallback);
    /** @type {any} */
    const candidate = Number.isFinite(numericValue)
        ? numericValue
        : (Number.isFinite(numericFallback) ? numericFallback : 430);
    return Math.max(minWidth, Math.min(maxWidth, Math.floor(candidate)));
};

/** @param {any} [values] @returns {any} */
export const computeLinearTrendline = (values) => {
    /** @type {any} */
    const points = values.map(/** @param {any} [value] @param {any} [index] @returns {any} */ (value, index) => [index, Number(value)]).filter(/** @param {any} p0 @returns {any} */ ([, value]) => Number.isFinite(value));
    if (points.length < 2) return null;
    const count = points.length;
    /** @type {any} */
    const sumX = points.reduce(/** @param {any} sum @param {any} p1 @returns {any} */ (sum, [value]) => sum + value, 0);
    /** @type {any} */
    const sumY = points.reduce(/** @param {any} sum @param {any} p1 @returns {any} */ (sum, [, value]) => sum + value, 0);
    /** @type {any} */
    const sumXY = points.reduce(/** @param {any} sum @param {any} p1 @returns {any} */ (sum, [x, y]) => sum + x * y, 0);
    /** @type {any} */
    const sumX2 = points.reduce(/** @param {any} sum @param {any} p1 @returns {any} */ (sum, [value]) => sum + value * value, 0);
    /** @type {any} */
    const denominator = count * sumX2 - sumX * sumX;
    if (denominator === 0) return null;
    /** @type {any} */
    const slope = (count * sumXY - sumX * sumY) / denominator;
    /** @type {any} */
    const intercept = (sumY - slope * sumX) / count;
    return values.map(/** @param {any} [_] @param {any} [index] @returns {any} */ (_, index) => slope * index + intercept);
};

/** @param {any} [values] @param {any} [period] @returns {any} */
export const computeMovingAverage = (values, period) => {
    /** @type {any} */
    const normalizedPeriod = Math.max(2, Math.floor(Number(period) || 3));
    return values.map(/** @param {any} [_] @param {any} [index] @returns {any} */ (_, index) => {
        /** @type {any} */
        const window = values.slice(Math.max(0, index - normalizedPeriod + 1), index + 1).map(Number).filter(Number.isFinite);
        return window.length > 0 ? window.reduce(/** @param {any} [sum] @param {any} [value] @returns {any} */ (sum, value) => sum + value, 0) / window.length : null;
    });
};

/** @param {any} [model] @returns {any} */
export const canStackAreaSeries = (model) => Array.isArray(model && model.series) && model.series.length > 1;
