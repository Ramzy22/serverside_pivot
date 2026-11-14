import { formatDisplayLabel } from './helpers';

/** @type {any} */
const VALID_SPARKLINE_TYPES = new Set(['line', 'area', 'column', 'bar']);
const GEOMETRY_MAX_POINTS_FLOOR = 24;
const GEOMETRY_MAX_POINTS_CEIL = 160;
/** @type {any} */
const normalizedPointCache = typeof WeakMap !== 'undefined' ? new WeakMap() : null;
/** @param {any} [value] @param {any} [min] @param {any} [max] @returns {any} */
const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

/** @param {any} [value] @returns {any} */
const toFiniteNumber = (value) => {
    /** @type {any} */
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
};

/** @param {any} [entry] @param {any} [index] @returns {any} */
const normalizeSparklinePointEntry = (entry, index) => {
    if (entry === null || entry === undefined) return null;
    if (typeof entry === 'number') {
        return Number.isFinite(entry)
            ? { index, value: entry, label: String(index + 1) }
            : null;
    }
    if (Array.isArray(entry)) {
        if (entry.length === 0) return null;
        /** @type {any} */
        const tupleValue = entry.length > 1 ? toFiniteNumber(entry[1]) : toFiniteNumber(entry[0]);
        if (!Number.isFinite(tupleValue)) return null;
        return {
            index,
            value: tupleValue,
            label: entry[0] === undefined || entry[0] === null ? String(index + 1) : String(entry[0]),
        };
    }
    if (typeof entry === 'object') {
        /** @type {any} */
        const candidateValue = toFiniteNumber(
            entry.value !== undefined ? entry.value
                : (entry.y !== undefined ? entry.y
                    : (entry.amount !== undefined ? entry.amount
                        : (entry.total !== undefined ? entry.total : null)))
        );
        if (!Number.isFinite(candidateValue)) return null;
        /** @type {any} */
        const labelSource = entry.label !== undefined ? entry.label
            : (entry.x !== undefined ? entry.x
                : (entry.name !== undefined ? entry.name
                    : (entry.category !== undefined ? entry.category
                        : (entry.date !== undefined ? entry.date : index + 1))));
        return {
            index,
            value: candidateValue,
            label: labelSource === undefined || labelSource === null ? String(index + 1) : String(labelSource),
        };
    }
    return null;
};

/** @param {any} [value] @param {any} [fallbackType] @returns {any} */
export const normalizeSparklineConfig = (value, fallbackType = 'line') => {
    if (value === false || value === null || value === undefined) return null;
    /** @type {any} */
    const source = value === true ? {} : (typeof value === 'object' ? value : {});
    if (source.enabled === false) return null;
    /** @type {any} */
    const requestedType = typeof source.type === 'string' ? source.type.trim().toLowerCase() : '';
    /** @type {any} */
    const type = VALID_SPARKLINE_TYPES.has(requestedType) ? requestedType : fallbackType;
    return {
        enabled: true,
        type,
        metric: typeof source.metric === 'string' && source.metric.trim()
            ? source.metric.trim().toLowerCase()
            : 'last',
        header: typeof source.header === 'string' && source.header.trim() ? source.header.trim() : null,
        color: typeof source.color === 'string' && source.color.trim() ? source.color.trim() : null,
        positiveColor: typeof source.positiveColor === 'string' && source.positiveColor.trim() ? source.positiveColor.trim() : null,
        negativeColor: typeof source.negativeColor === 'string' && source.negativeColor.trim() ? source.negativeColor.trim() : null,
        showCurrentValue: source.showCurrentValue !== false,
        showDelta: source.showDelta !== false,
        areaOpacity: Number.isFinite(Number(source.areaOpacity))
            ? clamp(Number(source.areaOpacity), 0.02, 0.45)
            : 0.14,
        compact: Boolean(source.compact),
        hideColumns: Boolean(source.hideColumns),
        source: (typeof source.source === 'string' && source.source.trim().toLowerCase() === 'field')
            ? 'field'
            : 'pivot',
        displayMode: (typeof source.displayMode === 'string' && source.displayMode.trim().toLowerCase() === 'value')
            ? 'value'
            : 'trend',
        placement: 'before',
    };
};

/** @param {any} [rawValue] @returns {any} */
export const normalizeSparklinePoints = (rawValue) => {
    const candidateSeries = (
        rawValue && typeof rawValue === 'object' && !Array.isArray(rawValue)
            ? (Array.isArray(rawValue.data) ? rawValue.data
                : (Array.isArray(rawValue.values) ? rawValue.values
                    : (Array.isArray(rawValue.points) ? rawValue.points : null)))
            : rawValue
    );
    if (!Array.isArray(candidateSeries)) return [];
    if (normalizedPointCache && normalizedPointCache.has(candidateSeries)) {
        return normalizedPointCache.get(candidateSeries);
    }
    /** @type {any} */
    const normalized = candidateSeries
        .map((entry, index) => normalizeSparklinePointEntry(entry, index))
        .filter(Boolean);
    if (normalizedPointCache) {
        normalizedPointCache.set(candidateSeries, normalized);
    }
    return normalized;
};

/** @param {any} p0 @returns {any} */
export const buildPivotSparklinePoints = ({
    rowData,
    columnIds,
    resolveValue,
    resolveLabel,
}) => {
    if (!rowData || !Array.isArray(columnIds) || columnIds.length === 0) return [];
    return columnIds
        .map((columnId, index) => {
            /** @type {any} */
            const rawValue = typeof resolveValue === 'function'
                ? resolveValue(columnId, rowData[columnId], rowData)
                : rowData[columnId];
            /** @type {any} */
            const numericValue = toFiniteNumber(rawValue);
            if (!Number.isFinite(numericValue)) return null;
            /** @type {any} */
            const rawLabel = typeof resolveLabel === 'function' ? resolveLabel(columnId, index, rowData) : columnId;
            return {
                index,
                value: numericValue,
                label: rawLabel ? String(rawLabel) : String(index + 1),
                columnId,
            };
        })
        .filter(Boolean);
};

/** @param {any} [points] @param {any} [metric] @returns {any} */
export const resolveSparklineMetricValue = (points, metric = 'last') => {
    if (!Array.isArray(points) || points.length === 0) return null;
    /** @type {any} */
    let first = null;
    /** @type {any} */
    let last = null;
    let min = Infinity;
    let max = -Infinity;
    let sum = 0;
    let count = 0;

    for (const point of points) {
        /** @type {any} */
        const value = toFiniteNumber(point && point.value);
        if (!Number.isFinite(value)) continue;
        if (first === null) first = value;
        last = value;
        if (value < min) min = value;
        if (value > max) max = value;
        sum += value;
        count += 1;
    }
    if (count === 0) return null;

    switch (metric) {
        case 'first':
            return first;
        case 'min':
            return min;
        case 'max':
            return max;
        case 'avg':
        case 'average':
            return sum / count;
        case 'sum':
            return sum;
        case 'delta':
            return count > 1 ? last - first : first;
        case 'last':
        default:
            return last;
    }
};

/** @param {any} [points] @returns {any} */
export const resolveSparklineDeltaValue = (points) => {
    if (!Array.isArray(points) || points.length < 2) return null;
    /** @type {any} */
    const previous = toFiniteNumber(points[points.length - 2] && points[points.length - 2].value);
    /** @type {any} */
    const current = toFiniteNumber(points[points.length - 1] && points[points.length - 1].value);
    if (!Number.isFinite(previous) || !Number.isFinite(current)) return null;
    return current - previous;
};

/** @param {any} [value] @param {any} [minValue] @param {any} [maxValue] @param {any} [minAxis] @param {any} [maxAxis] @returns {any} */
const scaleValueToAxis = (value, minValue, maxValue, minAxis, maxAxis) => {
    if (maxValue === minValue) return (minAxis + maxAxis) / 2;
    /** @type {any} */
    const normalized = (value - minValue) / (maxValue - minValue);
    return minAxis + ((maxAxis - minAxis) * normalized);
};

/** @param {any} [width] @returns {any} */
const resolveMaxGeometryPoints = (width) => (
    Math.max(
        GEOMETRY_MAX_POINTS_FLOOR,
        Math.min(GEOMETRY_MAX_POINTS_CEIL, Math.floor(Number(width) || GEOMETRY_MAX_POINTS_CEIL))
    )
);

/** @param {any} [points] @param {any} [start] @param {any} [end] @returns {any} */
const averageBucketPoint = (points, start, end) => {
    let sum = 0;
    let count = 0;
    for (let index = start; index < end; index += 1) {
        sum += Number(points[index].value);
        count += 1;
    }
    const source = points[start];
    return {
        ...source,
        value: count > 0 ? sum / count : Number(source.value),
        label: start + 1 === end ? source.label : `${source.label || start + 1}-${points[end - 1].label || end}`,
    };
};

/** @param {any} [points] @param {any} [maxPoints] @param {any} [type] @returns {any} */
const downsampleSparklinePoints = (points, maxPoints, type) => {
    if (!Array.isArray(points) || points.length <= maxPoints) return points;
    if (type === 'column' || type === 'bar') {
        /** @type {any[]} */
        const sampled = [];
        /** @type {any} */
        const bucketSize = points.length / maxPoints;
        for (let bucket = 0; bucket < maxPoints; bucket += 1) {
            /** @type {any} */
            const start = Math.floor(bucket * bucketSize);
            /** @type {any} */
            const end = Math.min(points.length, Math.max(start + 1, Math.floor((bucket + 1) * bucketSize)));
            sampled.push(averageBucketPoint(points, start, end));
        }
        return sampled;
    }

    /** @type {any} */
    const bucketCount = Math.max(1, Math.floor((maxPoints - 2) / 2));
    /** @type {any} */
    const bucketSize = Math.max(1, (points.length - 2) / bucketCount);
    /** @type {any[]} */
    const sampled = [points[0]];

    for (let bucket = 0; bucket < bucketCount; bucket += 1) {
        /** @type {any} */
        const start = Math.max(1, Math.floor(1 + (bucket * bucketSize)));
        /** @type {any} */
        const end = Math.min(points.length - 1, Math.floor(1 + ((bucket + 1) * bucketSize)));
        if (start >= end) continue;

        let minIndex = start;
        let maxIndex = start;
        for (let index = start + 1; index < end; index += 1) {
            /** @type {any} */
            const value = Number(points[index].value);
            if (value < Number(points[minIndex].value)) minIndex = index;
            if (value > Number(points[maxIndex].value)) maxIndex = index;
        }

        if (minIndex === maxIndex) {
            sampled.push(points[minIndex]);
        } else if (minIndex < maxIndex) {
            sampled.push(points[minIndex], points[maxIndex]);
        } else {
            sampled.push(points[maxIndex], points[minIndex]);
        }
    }

    sampled.push(points[points.length - 1]);
    if (sampled.length <= maxPoints) return sampled;

    /** @type {any} */
    const stride = Math.ceil(sampled.length / maxPoints);
    /** @type {any} */
    const thinned = sampled.filter((_, index) => index % stride === 0).slice(0, Math.max(1, maxPoints - 1));
    thinned.push(points[points.length - 1]);
    return thinned;
};

/** @param {any} p0 @returns {any} */
export const buildSparklineGeometry = ({
    points,
    width = 120,
    height = 30,
    padding = 4,
    type = 'line',
}) => {
    /** @type {any[]} */
    const validPoints = [];
    let minValue = Infinity;
    let maxValue = -Infinity;
    if (Array.isArray(points)) {
        for (const point of points) {
            /** @type {any} */
            const numericValue = toFiniteNumber(point && point.value);
            if (!Number.isFinite(numericValue)) continue;
            /** @type {any} */
            const normalizedPoint = point && typeof point === 'object'
                ? (point.value === numericValue ? point : { ...point, value: numericValue })
                : { index: validPoints.length, value: numericValue, label: String(validPoints.length + 1) };
            validPoints.push(normalizedPoint);
            if (numericValue < minValue) minValue = numericValue;
            if (numericValue > maxValue) maxValue = numericValue;
        }
    }
    if (validPoints.length === 0) {
        return {
            points: [],
            bars: [],
            linePath: '',
            areaPath: '',
            minValue: null,
            maxValue: null,
            baselineY: null,
            baselineX: null,
        };
    }

    /** @type {any} */
    const displayPoints = downsampleSparklinePoints(validPoints, resolveMaxGeometryPoints(width), type);
    /** @type {any} */
    const valueFloor = type === 'column' || type === 'bar' ? Math.min(minValue, 0) : minValue;
    /** @type {any} */
    const valueCeil = type === 'column' || type === 'bar' ? Math.max(maxValue, 0) : maxValue;
    /** @type {any} */
    const safeWidth = Math.max(width, padding * 2 + 8);
    /** @type {any} */
    const safeHeight = Math.max(height, padding * 2 + 8);
    /** @type {any} */
    const innerWidth = Math.max(1, safeWidth - (padding * 2));
    /** @type {any} */
    const innerHeight = Math.max(1, safeHeight - (padding * 2));
    /** @type {any} */
    const baselineY = scaleValueToAxis(0, valueFloor, valueCeil, safeHeight - padding, padding);
    /** @type {any} */
    const baselineX = scaleValueToAxis(0, valueFloor, valueCeil, padding, safeWidth - padding);

    if (type === 'bar') {
        /** @type {any} */
        const bandHeight = innerHeight / Math.max(displayPoints.length, 1);
        /** @type {any} */
        const barHeight = Math.max(2, bandHeight * 0.62);
        /** @type {any} */
        const bars = displayPoints.map(/** @param {any} [point] @param {any} [index] @returns {any} */ (point, index) => {
            /** @type {any} */
            const scaledX = scaleValueToAxis(Number(point.value), valueFloor, valueCeil, padding, safeWidth - padding);
            /** @type {any} */
            const y = padding + (index * bandHeight) + ((bandHeight - barHeight) / 2);
            return {
                ...point,
                x: Math.min(baselineX, scaledX),
                y,
                width: Math.max(1, Math.abs(scaledX - baselineX)),
                height: barHeight,
                positive: Number(point.value) >= 0,
            };
        });
        return {
            points: [],
            bars,
            linePath: '',
            areaPath: '',
            minValue,
            maxValue,
            baselineY: null,
            baselineX,
        };
    }

    /** @type {any} */
    const stepX = displayPoints.length > 1 ? innerWidth / (displayPoints.length - 1) : 0;
    /** @type {any} */
    const laidOutPoints = displayPoints.map(/** @param {any} [point] @param {any} [index] @returns {any} */ (point, index) => ({
        ...point,
        x: padding + (index * stepX),
        y: scaleValueToAxis(Number(point.value), valueFloor, valueCeil, safeHeight - padding, padding),
        baseY: baselineY,
    }));

    if (type === 'column') {
        /** @type {any} */
        const barWidth = Math.max(2, Math.min(18, (displayPoints.length > 0 ? innerWidth / displayPoints.length : innerWidth) * 0.62));
        /** @type {any} */
        const bars = laidOutPoints.map(/** @param {any} [point] @returns {any} */ (point) => ({
            ...point,
            x: point.x - (barWidth / 2),
            y: Number(point.value) >= 0 ? point.y : baselineY,
            width: barWidth,
            height: Math.max(1, Math.abs(point.y - baselineY)),
            positive: Number(point.value) >= 0,
        }));
        return {
            points: laidOutPoints,
            bars,
            linePath: '',
            areaPath: '',
            minValue,
            maxValue,
            baselineY,
            baselineX: null,
        };
    }

    /** @type {any} */
    const linePath = laidOutPoints
        .map(/** @param {any} [point] @param {any} [index] @returns {any} */ (point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
        .join(' ');
    /** @type {any} */
    const areaPath = type === 'area' && laidOutPoints.length > 1
        ? `${linePath} L ${laidOutPoints[laidOutPoints.length - 1].x.toFixed(2)} ${baselineY.toFixed(2)} L ${laidOutPoints[0].x.toFixed(2)} ${baselineY.toFixed(2)} Z`
        : '';

    return {
        points: laidOutPoints,
        bars: [],
        linePath,
        areaPath,
        minValue,
        maxValue,
        baselineY,
        baselineX: null,
    };
};

/** @param {any} [config] @param {any} [fallbackField] @returns {any} */
export const buildSparklineHeader = (config, fallbackField) => {
    /** @type {any} */
    const sparklineConfig = normalizeSparklineConfig(config && config.sparkline);
    if (sparklineConfig && sparklineConfig.header) return sparklineConfig.header;
    /** @type {any} */
    const label = config && config.label ? config.label : fallbackField;
    return `${formatDisplayLabel(label || 'Value')} Trend`;
};

/** @param {any} [config] @returns {any} */
export const getSparklineValueConfigIdentity = (config) => {
    if (!config || typeof config !== 'object') return '';
    /** @type {any} */
    const field = config.field === undefined || config.field === null ? '' : String(config.field);
    /** @type {any} */
    const agg = config.agg === undefined || config.agg === null ? '' : String(config.agg);
    return `${field}::${agg}`;
};

/** @param {any} [fixedConfigs] @returns {any} */
export const getFixedSparklineConfigMap = (fixedConfigs) => {
    /** @type {any} */
    const fixedMap = new Map();
    if (!Array.isArray(fixedConfigs)) return fixedMap;
    fixedConfigs.forEach((config) => {
        if (!config || typeof config !== 'object' || !config.sparkline || config.sparkline === false) return;
        /** @type {any} */
        const identity = getSparklineValueConfigIdentity(config);
        if (!identity) return;
        /** @type {any} */
        const sparkline = config.sparkline === true
            ? {}
            : (typeof config.sparkline === 'object' ? config.sparkline : {});
        fixedMap.set(identity, { ...sparkline, placement: 'before' });
    });
    return fixedMap;
};

/** @param {any} [configs] @param {any} [fixedConfigs] @returns {any} */
export const normalizeSparklineValConfigsForView = (configs, fixedConfigs = []) => {
    /** @type {any} */
    const fixedMap = getFixedSparklineConfigMap(fixedConfigs);
    return Array.isArray(configs)
        ? configs.map((config) => {
            if (!config || typeof config !== 'object') return config;
            /** @type {any} */
            const fixedSparkline = fixedMap.get(getSparklineValueConfigIdentity(config));
            /** @type {any} */
            const removesFixedSparkline = Boolean(
                fixedSparkline
                && config.sparkline
                && typeof config.sparkline === 'object'
                && config.sparkline.enabled === false
            );
            /** @type {any} */
            const sourceSparkline = (
                removesFixedSparkline
                || config.sparkline === false
                || config.sparkline === null
                || config.sparkline === undefined
            ) ? fixedSparkline : config.sparkline;
            if (!sourceSparkline) return config;
            /** @type {any} */
            const sparkline = sourceSparkline === true
                ? {}
                : (typeof sourceSparkline === 'object' ? sourceSparkline : {});
            return {
                ...config,
                sparkline: {
                    ...sparkline,
                    placement: 'before',
                },
            };
        })
        : [];
};
