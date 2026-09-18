/** Pure formatting and coordinate helpers for the sparkline-detail surface. */

export const formatSparklineDetailValue = (value, maximumFractionDigits = 4) => {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric.toLocaleString(undefined, { maximumFractionDigits });
    return value === null || value === undefined ? '' : String(value);
};

export const getSparklineDetailNumericValues = (points) => (
    (Array.isArray(points) ? points : [])
        .map((point) => Number(point && point.value))
        .filter((value) => Number.isFinite(value))
);

export const formatSparklineDetailExtremum = (points, mode) => {
    const values = getSparklineDetailNumericValues(points);
    if (values.length === 0) return '';
    return formatSparklineDetailValue(mode === 'min' ? Math.min(...values) : Math.max(...values));
};

export const resolveSparklineDetailRange = (values, type) => {
    if (!Array.isArray(values) || values.length === 0) return { min: 0, max: 1 };
    let minValue = Math.min(...values);
    let maxValue = Math.max(...values);
    if (type === 'column' || type === 'bar') {
        minValue = Math.min(minValue, 0);
        maxValue = Math.max(maxValue, 0);
    }
    if (minValue === maxValue) {
        const spread = Math.max(1, Math.abs(minValue) * 0.08);
        return { min: minValue - spread, max: maxValue + spread };
    }
    return { min: minValue, max: maxValue };
};

export const scaleSparklineDetailValue = (value, minValue, maxValue, minAxis, maxAxis) => {
    if (maxValue === minValue) return (minAxis + maxAxis) / 2;
    const normalized = (Number(value) - minValue) / (maxValue - minValue);
    return maxAxis - ((maxAxis - minAxis) * normalized);
};
