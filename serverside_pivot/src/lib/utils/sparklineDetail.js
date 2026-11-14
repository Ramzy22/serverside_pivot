/** Pure formatting and coordinate helpers for the sparkline-detail surface.  @param {any} [value] @param {any} [maximumFractionDigits] @returns {any} */

export const formatSparklineDetailValue = (value, maximumFractionDigits = 4) => {
    /** @type {any} */
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric.toLocaleString(undefined, { maximumFractionDigits });
    return value === null || value === undefined ? '' : String(value);
};

/** @param {any} [points] @returns {any} */
export const getSparklineDetailNumericValues = (points) => (
    (Array.isArray(points) ? points : [])
        .map((point) => Number(point && point.value))
        .filter((value) => Number.isFinite(value))
);

/** @param {any} [points] @param {any} [mode] @returns {any} */
export const formatSparklineDetailExtremum = (points, mode) => {
    /** @type {any} */
    const values = getSparklineDetailNumericValues(points);
    if (values.length === 0) return '';
    return formatSparklineDetailValue(mode === 'min' ? Math.min(...values) : Math.max(...values));
};

/** @param {any} [values] @param {any} [type] @returns {any} */
export const resolveSparklineDetailRange = (values, type) => {
    if (!Array.isArray(values) || values.length === 0) return { min: 0, max: 1 };
    /** @type {any} */
    let minValue = Math.min(...values);
    /** @type {any} */
    let maxValue = Math.max(...values);
    if (type === 'column' || type === 'bar') {
        minValue = Math.min(minValue, 0);
        maxValue = Math.max(maxValue, 0);
    }
    if (minValue === maxValue) {
        /** @type {any} */
        const spread = Math.max(1, Math.abs(minValue) * 0.08);
        return { min: minValue - spread, max: maxValue + spread };
    }
    return { min: minValue, max: maxValue };
};

/** @param {any} [value] @param {any} [minValue] @param {any} [maxValue] @param {any} [minAxis] @param {any} [maxAxis] @returns {any} */
export const scaleSparklineDetailValue = (value, minValue, maxValue, minAxis, maxAxis) => {
    if (maxValue === minValue) return (minAxis + maxAxis) / 2;
    /** @type {any} */
    const normalized = (Number(value) - minValue) / (maxValue - minValue);
    return maxAxis - ((maxAxis - minAxis) * normalized);
};
