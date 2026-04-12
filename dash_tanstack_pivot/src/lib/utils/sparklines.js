import { formatDisplayLabel } from './helpers';

const VALID_SPARKLINE_TYPES = new Set(['line', 'area', 'column', 'bar']);
const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

const toFiniteNumber = (value) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
};

const normalizeSparklinePointEntry = (entry, index) => {
    if (entry === null || entry === undefined) return null;
    if (typeof entry === 'number') {
        return Number.isFinite(entry)
            ? { index, value: entry, label: String(index + 1) }
            : null;
    }
    if (Array.isArray(entry)) {
        if (entry.length === 0) return null;
        const tupleValue = entry.length > 1 ? toFiniteNumber(entry[1]) : toFiniteNumber(entry[0]);
        if (!Number.isFinite(tupleValue)) return null;
        return {
            index,
            value: tupleValue,
            label: entry[0] === undefined || entry[0] === null ? String(index + 1) : String(entry[0]),
        };
    }
    if (typeof entry === 'object') {
        const candidateValue = toFiniteNumber(
            entry.value !== undefined ? entry.value
                : (entry.y !== undefined ? entry.y
                    : (entry.amount !== undefined ? entry.amount
                        : (entry.total !== undefined ? entry.total : null)))
        );
        if (!Number.isFinite(candidateValue)) return null;
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

export const normalizeSparklineConfig = (value, fallbackType = 'line') => {
    if (value === false || value === null || value === undefined) return null;
    const source = value === true ? {} : (typeof value === 'object' ? value : {});
    if (source.enabled === false) return null;
    const requestedType = typeof source.type === 'string' ? source.type.trim().toLowerCase() : '';
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
        placement: (typeof source.placement === 'string' && ['after', 'before', 'end'].includes(source.placement.trim().toLowerCase()))
            ? source.placement.trim().toLowerCase()
            : 'after',
    };
};

export const normalizeSparklinePoints = (rawValue) => {
    const candidateSeries = (
        rawValue && typeof rawValue === 'object' && !Array.isArray(rawValue)
            ? (Array.isArray(rawValue.data) ? rawValue.data
                : (Array.isArray(rawValue.values) ? rawValue.values
                    : (Array.isArray(rawValue.points) ? rawValue.points : null)))
            : rawValue
    );
    if (!Array.isArray(candidateSeries)) return [];
    return candidateSeries
        .map((entry, index) => normalizeSparklinePointEntry(entry, index))
        .filter(Boolean);
};

export const buildPivotSparklinePoints = ({
    rowData,
    columnIds,
    resolveValue,
    resolveLabel,
}) => {
    if (!rowData || !Array.isArray(columnIds) || columnIds.length === 0) return [];
    return columnIds
        .map((columnId, index) => {
            const rawValue = typeof resolveValue === 'function'
                ? resolveValue(columnId, rowData[columnId], rowData)
                : rowData[columnId];
            const numericValue = toFiniteNumber(rawValue);
            if (!Number.isFinite(numericValue)) return null;
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

export const resolveSparklineMetricValue = (points, metric = 'last') => {
    if (!Array.isArray(points) || points.length === 0) return null;
    const values = points
        .map((point) => toFiniteNumber(point && point.value))
        .filter((value) => Number.isFinite(value));
    if (values.length === 0) return null;
    switch (metric) {
        case 'first':
            return values[0];
        case 'min':
            return Math.min(...values);
        case 'max':
            return Math.max(...values);
        case 'avg':
        case 'average':
            return values.reduce((sum, value) => sum + value, 0) / values.length;
        case 'sum':
            return values.reduce((sum, value) => sum + value, 0);
        case 'delta':
            return values.length > 1 ? values[values.length - 1] - values[0] : values[0];
        case 'last':
        default:
            return values[values.length - 1];
    }
};

export const resolveSparklineDeltaValue = (points) => {
    if (!Array.isArray(points) || points.length < 2) return null;
    const previous = toFiniteNumber(points[points.length - 2] && points[points.length - 2].value);
    const current = toFiniteNumber(points[points.length - 1] && points[points.length - 1].value);
    if (!Number.isFinite(previous) || !Number.isFinite(current)) return null;
    return current - previous;
};

const scaleValueToAxis = (value, minValue, maxValue, minAxis, maxAxis) => {
    if (maxValue === minValue) return (minAxis + maxAxis) / 2;
    const normalized = (value - minValue) / (maxValue - minValue);
    return minAxis + ((maxAxis - minAxis) * normalized);
};

export const buildSparklineGeometry = ({
    points,
    width = 120,
    height = 30,
    padding = 4,
    type = 'line',
}) => {
    const validPoints = Array.isArray(points) ? points.filter((point) => Number.isFinite(toFiniteNumber(point && point.value))) : [];
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

    const values = validPoints.map((point) => Number(point.value));
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const valueFloor = type === 'column' || type === 'bar' ? Math.min(minValue, 0) : minValue;
    const valueCeil = type === 'column' || type === 'bar' ? Math.max(maxValue, 0) : maxValue;
    const safeWidth = Math.max(width, padding * 2 + 8);
    const safeHeight = Math.max(height, padding * 2 + 8);
    const innerWidth = Math.max(1, safeWidth - (padding * 2));
    const innerHeight = Math.max(1, safeHeight - (padding * 2));
    const baselineY = scaleValueToAxis(0, valueFloor, valueCeil, safeHeight - padding, padding);
    const baselineX = scaleValueToAxis(0, valueFloor, valueCeil, padding, safeWidth - padding);

    if (type === 'bar') {
        const bandHeight = innerHeight / Math.max(validPoints.length, 1);
        const barHeight = Math.max(2, bandHeight * 0.62);
        const bars = validPoints.map((point, index) => {
            const scaledX = scaleValueToAxis(Number(point.value), valueFloor, valueCeil, padding, safeWidth - padding);
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

    const stepX = validPoints.length > 1 ? innerWidth / (validPoints.length - 1) : 0;
    const laidOutPoints = validPoints.map((point, index) => ({
        ...point,
        x: padding + (index * stepX),
        y: scaleValueToAxis(Number(point.value), valueFloor, valueCeil, safeHeight - padding, padding),
        baseY: baselineY,
    }));

    if (type === 'column') {
        const barWidth = Math.max(2, Math.min(18, (validPoints.length > 0 ? innerWidth / validPoints.length : innerWidth) * 0.62));
        const bars = laidOutPoints.map((point) => ({
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

    const linePath = laidOutPoints
        .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
        .join(' ');
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

export const buildSparklineHeader = (config, fallbackField) => {
    const sparklineConfig = normalizeSparklineConfig(config && config.sparkline);
    if (sparklineConfig && sparklineConfig.header) return sparklineConfig.header;
    const label = config && config.label ? config.label : fallbackField;
    return `${formatDisplayLabel(label || 'Value')} Trend`;
};
