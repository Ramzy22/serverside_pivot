import React from 'react';
import { buildSparklineGeometry } from '../../utils/sparklines';
import {
    formatSparklineDetailValue,
    getSparklineDetailNumericValues,
    resolveSparklineDetailRange,
    scaleSparklineDetailValue,
} from '../../utils/sparklineDetail';

/** @param {any} p0 @returns {any} */
export function SparklineDetailGraph({ modal, theme }) {
    /** @type {any} */
    const points = Array.isArray(modal && modal.points) ? modal.points : [];
    /** @type {any} */
    const sparklineConfig = modal && modal.sparklineConfig ? modal.sparklineConfig : {};
    /** @type {any} */
    const type = ['line', 'area', 'column', 'bar'].includes(sparklineConfig.type) ? sparklineConfig.type : 'line';
    const width = 640;
    const height = 300;
    const padding = 46;
    /** @type {[any, (...args: any[]) => any]} */
    const [hoverIndex, setHoverIndex] = React.useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [zoomWindow, setZoomWindow] = React.useState(null);
    React.useEffect(() => {
        setHoverIndex(null);
        setZoomWindow(null);
    }, [points.length, type]);
    /** @type {any} */
    const normalizedZoomWindow = React.useMemo(() => {
        /** @type {any} */
        const lastIndex = Math.max(0, points.length - 1);
        if (!zoomWindow || points.length === 0) return { start: 0, end: lastIndex };
        /** @type {any} */
        const start = Math.max(0, Math.min(lastIndex, Math.floor(Number(zoomWindow.start) || 0)));
        /** @type {any} */
        const end = Math.max(start, Math.min(lastIndex, Math.floor(Number(zoomWindow.end) || lastIndex)));
        return { start, end };
    }, [points.length, zoomWindow]);
    /** @type {any} */
    const visiblePoints = React.useMemo(
        () => points.slice(normalizedZoomWindow.start, normalizedZoomWindow.end + 1),
        [normalizedZoomWindow, points]
    );
    const visiblePointCount = visiblePoints.length;
    /** @type {any} */
    const canZoom = points.length > 4;
    /** @type {any} */
    const canZoomIn = canZoom && visiblePointCount > 4;
    /** @type {any} */
    const isZoomed = canZoom && (normalizedZoomWindow.start > 0 || normalizedZoomWindow.end < points.length - 1);
    /** @type {any} */
    const geometry = React.useMemo(
        () => buildSparklineGeometry({ points: visiblePoints, type, width, height, padding }),
        [visiblePoints, type]
    );
    /** @type {any} */
    const values = getSparklineDetailNumericValues(visiblePoints);
    /** @type {any} */
    const valueRange = React.useMemo(
        () => resolveSparklineDetailRange(values, type),
        [values, type]
    );
    /** @type {any} */
    const chartMarks = React.useMemo(() => {
        if (type === 'bar') {
            return geometry.bars.map(/** @param {any} [bar] @param {any} [index] @returns {any} */ (bar, index) => ({
                ...bar,
                pointIndex: index,
                fullIndex: normalizedZoomWindow.start + index,
                markerX: Number(bar.value) >= 0 ? bar.x + bar.width : bar.x,
                markerY: bar.y + (bar.height / 2),
            }));
        }
        if (type === 'column') {
            return geometry.bars.map(/** @param {any} [bar] @param {any} [index] @returns {any} */ (bar, index) => ({
                ...bar,
                pointIndex: index,
                fullIndex: normalizedZoomWindow.start + index,
                markerX: bar.x + (bar.width / 2),
                markerY: Number(bar.value) >= 0 ? bar.y : bar.y + bar.height,
            }));
        }
        return geometry.points.map(/** @param {any} [point] @param {any} [index] @returns {any} */ (point, index) => ({
            ...point,
            pointIndex: index,
            fullIndex: normalizedZoomWindow.start + index,
            markerX: point.x,
            markerY: point.y,
        }));
    }, [geometry.bars, geometry.points, normalizedZoomWindow.start, type]);
    /** @type {any} */
    const yTicks = React.useMemo(() => {
        /** @type {number} */ const tickCount = 5;
        return Array.from({ length: tickCount }, (_, index) => {
            /** @type {any} */
            const ratio = tickCount === 1 ? 0 : index / (tickCount - 1);
            /** @type {any} */
            const value = valueRange.max - ((valueRange.max - valueRange.min) * ratio);
            return {
                value,
                y: padding + ((height - (padding * 2)) * ratio),
            };
        });
    }, [height, padding, valueRange]);
    /** @type {any} */
    const xTicks = React.useMemo(() => {
        if (!Array.isArray(visiblePoints) || visiblePoints.length === 0) return [];
        /** @type {any} */
        const lastIndex = visiblePoints.length - 1;
        /** @type {any} */
        const indexes = Array.from(new Set([
            0,
            Math.floor(lastIndex * 0.25),
            Math.floor(lastIndex * 0.5),
            Math.floor(lastIndex * 0.75),
            lastIndex,
        ]));
        return indexes
            .map(/** @param {any} [index] @returns {any} */ (index) => {
                const point = visiblePoints[index];
                /** @type {any} */
                const mark = chartMarks.find(/** @param {any} [candidate] @returns {any} */ (candidate) => candidate.pointIndex === index);
                if (!point || !mark) return null;
                return { index, label: point.label, x: mark.markerX };
            })
            .filter(Boolean);
    }, [chartMarks, visiblePoints]);
    /** @type {any} */
    const strokeColor = sparklineConfig.color || theme.primary || '#2563EB';
    /** @type {any} */
    const positiveColor = sparklineConfig.positiveColor || (theme.isDark ? '#86EFAC' : '#2F855A');
    /** @type {any} */
    const negativeColor = sparklineConfig.negativeColor || (theme.isDark ? '#FDA4AF' : '#B91C1C');
    /** @type {any} */
    const minLabel = values.length ? formatSparklineDetailValue(Math.min(...values)) : '';
    /** @type {any} */
    const maxLabel = values.length ? formatSparklineDetailValue(Math.max(...values)) : '';
    const firstPoint = visiblePoints[0];
    const lastPoint = visiblePoints[visiblePoints.length - 1];
    /** @type {any} */
    const zeroY = valueRange.min < 0 && valueRange.max > 0
        ? scaleSparklineDetailValue(0, valueRange.min, valueRange.max, padding, height - padding)
        : null;
    /** @type {any} */
    const activeMark = hoverIndex !== null && chartMarks[hoverIndex] ? chartMarks[hoverIndex] : null;
    const tooltipWidth = 172;
    const tooltipHeight = 54;
    /** @type {any} */
    const tooltipX = activeMark
        ? Math.max(10, Math.min(width - tooltipWidth - 10, activeMark.markerX + (activeMark.markerX > width * 0.66 ? -tooltipWidth - 14 : 14)))
        : 0;
    /** @type {any} */
    const tooltipY = activeMark
        ? Math.max(10, Math.min(height - tooltipHeight - 10, activeMark.markerY + (activeMark.markerY > height * 0.55 ? -tooltipHeight - 14 : 14)))
        : 0;
    /** @type {(...args: any[]) => any} */
    const handleGraphPointerMove = React.useCallback((event) => {
        if (!chartMarks.length) return;
        /** @type {any} */
        const rect = event.currentTarget.getBoundingClientRect();
        /** @type {any} */
        const svgX = ((event.clientX - rect.left) / Math.max(rect.width, 1)) * width;
        /** @type {any} */
        const svgY = ((event.clientY - rect.top) / Math.max(rect.height, 1)) * height;
        let nearestIndex = 0;
        let nearestScore = Number.POSITIVE_INFINITY;
        chartMarks.forEach(/** @param {any} [mark] @param {any} [index] @returns {any} */ (mark, index) => {
            /** @type {any} */
            const dx = svgX - mark.markerX;
            /** @type {any} */
            const dy = svgY - mark.markerY;
            /** @type {any} */
            const score = type === 'bar' ? (dx * dx) + (dy * dy) : Math.abs(dx) + (Math.abs(dy) * 0.08);
            if (score < nearestScore) {
                nearestScore = score;
                nearestIndex = index;
            }
        });
        setHoverIndex(nearestIndex);
    }, [chartMarks, height, type, width]);
    /** @type {(...args: any[]) => any} */
    const setZoomRange = React.useCallback((start, end) => {
        if (points.length <= 0) {
            setZoomWindow(null);
            return;
        }
        /** @type {any} */
        const lastIndex = points.length - 1;
        /** @type {any} */
        const minWindowSize = Math.min(4, points.length);
        /** @type {any} */
        const boundedStart = Math.max(0, Math.min(lastIndex, Math.floor(start)));
        /** @type {any} */
        const boundedEnd = Math.max(boundedStart, Math.min(lastIndex, Math.floor(end)));
        if (boundedEnd - boundedStart + 1 >= points.length) {
            setZoomWindow(null);
            return;
        }
        if (boundedEnd - boundedStart + 1 < minWindowSize) {
            /** @type {any} */
            const center = Math.round((boundedStart + boundedEnd) / 2);
            /** @type {any} */
            const half = Math.floor(minWindowSize / 2);
            /** @type {any} */
            const nextStart = Math.max(0, Math.min(lastIndex - minWindowSize + 1, center - half));
            setZoomWindow({ start: nextStart, end: nextStart + minWindowSize - 1 });
            return;
        }
        setZoomWindow({ start: boundedStart, end: boundedEnd });
    }, [points.length]);
    /** @type {(...args: any[]) => any} */
    const zoomByFactor = React.useCallback((factor) => {
        if (!canZoom) return;
        const currentStart = normalizedZoomWindow.start;
        const currentEnd = normalizedZoomWindow.end;
        /** @type {any} */
        const currentLength = currentEnd - currentStart + 1;
        /** @type {any} */
        const minLength = Math.min(4, points.length);
        /** @type {any} */
        const nextLength = Math.max(minLength, Math.min(points.length, Math.round(currentLength * factor)));
        /** @type {any} */
        const center = activeMark
            ? activeMark.fullIndex
            : Math.round((currentStart + currentEnd) / 2);
        /** @type {any} */
        const nextStart = Math.max(0, Math.min(points.length - nextLength, Math.round(center - (nextLength / 2))));
        setZoomRange(nextStart, nextStart + nextLength - 1);
    }, [activeMark, canZoom, normalizedZoomWindow, points.length, setZoomRange]);
    /** @type {(...args: any[]) => any} */
    const resetZoom = React.useCallback(() => {
        setHoverIndex(null);
        setZoomWindow(null);
    }, []);
    /** @type {(...args: any[]) => any} */
    const handleGraphWheel = React.useCallback((event) => {
        if (!canZoom) return;
        event.preventDefault();
        zoomByFactor(event.deltaY < 0 ? 0.68 : 1.42);
    }, [canZoom, zoomByFactor]);

    return (
        <div
            data-pivot-sparkline-detail-graph="true"
            style={{
                flex: '1 1 430px',
                minWidth: '300px',
                padding: '14px',
                borderRight: `1px solid ${theme.border}`,
                background: theme.headerSubtleBg || theme.background,
            }}
        >
            {canZoom && (
                <div
                    data-pivot-sparkline-zoom-controls="true"
                    style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '10px', marginBottom: '8px' }}
                >
                    <span style={{ fontSize: '11px', fontWeight: 700, color: theme.textSec }}>
                        {isZoomed
                            ? `${normalizedZoomWindow.start + 1}-${normalizedZoomWindow.end + 1} of ${points.length}`
                            : `${points.length} points`}
                    </span>
                    <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                        <button
                            type="button"
                            data-pivot-sparkline-zoom-in="true"
                            disabled={!canZoomIn}
                            onClick={() => zoomByFactor(0.62)}
                            style={{ border: `1px solid ${theme.border}`, background: canZoomIn ? theme.background : theme.headerSubtleBg, color: canZoomIn ? theme.text : theme.textSec, borderRadius: '6px', padding: '4px 8px', fontSize: '11px', fontWeight: 700, cursor: canZoomIn ? 'pointer' : 'default', opacity: canZoomIn ? 1 : 0.55 }}
                        >
                            Zoom In
                        </button>
                        <button
                            type="button"
                            data-pivot-sparkline-zoom-out="true"
                            disabled={!isZoomed}
                            onClick={() => zoomByFactor(1.62)}
                            style={{ border: `1px solid ${theme.border}`, background: isZoomed ? theme.background : theme.headerSubtleBg, color: isZoomed ? theme.text : theme.textSec, borderRadius: '6px', padding: '4px 8px', fontSize: '11px', fontWeight: 700, cursor: isZoomed ? 'pointer' : 'default', opacity: isZoomed ? 1 : 0.55 }}
                        >
                            Zoom Out
                        </button>
                        <button
                            type="button"
                            data-pivot-sparkline-zoom-reset="true"
                            disabled={!isZoomed}
                            onClick={resetZoom}
                            style={{ border: `1px solid ${theme.border}`, background: isZoomed ? theme.background : theme.headerSubtleBg, color: isZoomed ? theme.text : theme.textSec, borderRadius: '6px', padding: '4px 8px', fontSize: '11px', fontWeight: 700, cursor: isZoomed ? 'pointer' : 'default', opacity: isZoomed ? 1 : 0.55 }}
                        >
                            Reset
                        </button>
                    </div>
                </div>
            )}
            <svg
                data-pivot-sparkline-detail-svg="true"
                viewBox={`0 0 ${width} ${height}`}
                aria-label="Sparkline detail chart"
                onMouseMove={handleGraphPointerMove}
                onMouseLeave={() => setHoverIndex(null)}
                onWheel={handleGraphWheel}
                style={{ display: 'block', width: '100%', height: '320px', cursor: chartMarks.length ? 'crosshair' : 'default' }}
            >
                <rect x="0" y="0" width={width} height={height} fill={theme.background || '#fff'} opacity="0.72" />
                {yTicks.map(/** @param {any} [tick] @param {any} [index] @returns {any} */ (tick, index) => (
                    <g key={`y-tick-${index}`}>
                        <line x1={padding} y1={tick.y} x2={width - padding} y2={tick.y} stroke={theme.border} strokeWidth="1" opacity={index === yTicks.length - 1 ? 0.9 : 0.45} strokeDasharray={index === yTicks.length - 1 ? '' : '4 6'} />
                        <text x={padding - 8} y={tick.y + 4} fill={theme.textSec} fontSize="10" textAnchor="end">{formatSparklineDetailValue(tick.value)}</text>
                    </g>
                ))}
                {xTicks.map(/** @param {any} [tick] @returns {any} */ (tick) => (
                    <g key={`x-tick-${tick.index}`}>
                        <line x1={tick.x} y1={padding} x2={tick.x} y2={height - padding} stroke={theme.border} strokeWidth="1" opacity="0.28" strokeDasharray="3 8" />
                        <text x={tick.x} y={height - 12} fill={theme.textSec} fontSize="10" textAnchor="middle">{String(tick.label || '').slice(0, 18)}</text>
                    </g>
                ))}
                {zeroY !== null ? (
                    <line x1={padding} y1={zeroY} x2={width - padding} y2={zeroY} stroke={theme.textSec} strokeWidth="1.2" opacity="0.5" />
                ) : null}
                <line x1={padding} y1={height - padding} x2={width - padding} y2={height - padding} stroke={theme.border} strokeWidth="1.2" />
                <line x1={padding} y1={padding} x2={padding} y2={height - padding} stroke={theme.border} strokeWidth="1.2" />
                {type === 'area' && geometry.areaPath ? (
                    <path d={geometry.areaPath} fill={strokeColor} opacity={sparklineConfig.areaOpacity || 0.14} />
                ) : null}
                {type === 'line' || type === 'area' ? (
                    <>
                        {geometry.linePath ? (
                            <path d={geometry.linePath} fill="none" stroke={strokeColor} strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
                        ) : null}
                        {chartMarks.map(/** @param {any} [point] @param {any} [index] @returns {any} */ (point, index) => (
                            <circle
                                key={`${point.label || 'point'}-${index}`}
                                data-pivot-sparkline-detail-point="true"
                                cx={point.markerX}
                                cy={point.markerY}
                                r={activeMark && activeMark.pointIndex === point.pointIndex ? '5.4' : '3.6'}
                                fill={strokeColor}
                                stroke={theme.background}
                                strokeWidth="1.4"
                            >
                                <title>{`${point.label}: ${formatSparklineDetailValue(point.value, 6)}`}</title>
                            </circle>
                        ))}
                    </>
                ) : null}
                {type === 'column' || type === 'bar' ? (
                    chartMarks.map(/** @param {any} [bar] @param {any} [index] @returns {any} */ (bar, index) => (
                        <rect
                            key={`${bar.label || 'bar'}-${index}`}
                            x={bar.x}
                            y={bar.y}
                            width={bar.width}
                            height={bar.height}
                            rx="2"
                            ry="2"
                            fill={bar.positive ? positiveColor : negativeColor}
                            opacity={activeMark && activeMark.fullIndex === bar.fullIndex ? 1 : 0.88}
                        >
                            <title>{`${bar.label}: ${formatSparklineDetailValue(bar.value, 6)}`}</title>
                        </rect>
                    ))
                ) : null}
                {activeMark ? (
                    <g data-pivot-sparkline-tooltip="true" pointerEvents="none">
                        {type !== 'bar' ? (
                            <line x1={activeMark.markerX} y1={padding} x2={activeMark.markerX} y2={height - padding} stroke={strokeColor} strokeWidth="1" opacity="0.55" strokeDasharray="5 5" />
                        ) : null}
                        <circle cx={activeMark.markerX} cy={activeMark.markerY} r="6.4" fill={strokeColor} stroke={theme.background} strokeWidth="2" />
                        <rect x={tooltipX} y={tooltipY} width={tooltipWidth} height={tooltipHeight} rx="7" fill={theme.background || '#fff'} stroke={theme.border} strokeWidth="1.2" opacity="0.98" />
                        <text x={tooltipX + 10} y={tooltipY + 18} fill={theme.textSec} fontSize="10" fontWeight="700">{String(activeMark.label || activeMark.pointIndex + 1).slice(0, 28)}</text>
                        <text x={tooltipX + 10} y={tooltipY + 38} fill={theme.text} fontSize="15" fontWeight="800">{formatSparklineDetailValue(activeMark.value, 6)}</text>
                    </g>
                ) : null}
                {geometry.maxValue !== null ? (
                    <text x={width - padding} y="22" fill={theme.textSec} fontSize="11" fontWeight="700" textAnchor="end">Max {maxLabel}</text>
                ) : null}
                {geometry.minValue !== null ? (
                    <text x={width - padding} y={height - 28} fill={theme.textSec} fontSize="11" fontWeight="700" textAnchor="end">Min {minLabel}</text>
                ) : null}
                <rect data-pivot-sparkline-hover-target="true" x="0" y="0" width={width} height={height} fill="transparent" />
            </svg>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '10px', marginTop: '6px', fontSize: '11px', color: theme.textSec }}>
                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{firstPoint ? firstPoint.label : ''}</span>
                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', textAlign: 'right' }}>{lastPoint ? lastPoint.label : ''}</span>
            </div>
        </div>
    );
}

