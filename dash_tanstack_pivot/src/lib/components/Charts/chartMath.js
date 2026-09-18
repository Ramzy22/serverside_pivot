/** Pure chart calculations shared by SVG and ECharts presentation paths. */

export const clampChartPanelBaseWidth = (value, fallback = 430, minWidth, maxWidth) => {
    const numericValue = Number(value);
    const numericFallback = Number(fallback);
    const candidate = Number.isFinite(numericValue)
        ? numericValue
        : (Number.isFinite(numericFallback) ? numericFallback : 430);
    return Math.max(minWidth, Math.min(maxWidth, Math.floor(candidate)));
};

export const computeLinearTrendline = (values) => {
    const points = values.map((value, index) => [index, Number(value)]).filter(([, value]) => Number.isFinite(value));
    if (points.length < 2) return null;
    const count = points.length;
    const sumX = points.reduce((sum, [value]) => sum + value, 0);
    const sumY = points.reduce((sum, [, value]) => sum + value, 0);
    const sumXY = points.reduce((sum, [x, y]) => sum + x * y, 0);
    const sumX2 = points.reduce((sum, [value]) => sum + value * value, 0);
    const denominator = count * sumX2 - sumX * sumX;
    if (denominator === 0) return null;
    const slope = (count * sumXY - sumX * sumY) / denominator;
    const intercept = (sumY - slope * sumX) / count;
    return values.map((_, index) => slope * index + intercept);
};

export const computeMovingAverage = (values, period) => {
    const normalizedPeriod = Math.max(2, Math.floor(Number(period) || 3));
    return values.map((_, index) => {
        const window = values.slice(Math.max(0, index - normalizedPeriod + 1), index + 1).map(Number).filter(Number.isFinite);
        return window.length > 0 ? window.reduce((sum, value) => sum + value, 0) / window.length : null;
    });
};

export const canStackAreaSeries = (model) => Array.isArray(model && model.series) && model.series.length > 1;
