import React, { useMemo } from 'react';
import { buildSparklineGeometry, resolveSparklineDeltaValue } from '../../utils/sparklines';

const DEFAULT_WIDTH = 132;
const DEFAULT_HEIGHT = 34;

export default function SparklineCell({
    points = [],
    type = 'line',
    theme,
    color = null,
    positiveColor = null,
    negativeColor = null,
    areaOpacity = 0.14,
    showCurrentValue = true,
    showDelta = true,
    currentLabel = '',
    deltaLabel = '',
    title = '',
    compact = false,
}) {
    const geometry = useMemo(
        () => buildSparklineGeometry({
            points,
            type,
            width: DEFAULT_WIDTH,
            height: DEFAULT_HEIGHT,
            padding: compact ? 3 : 4,
        }),
        [compact, points, type]
    );

    const currentPoint = Array.isArray(geometry.points) && geometry.points.length > 0
        ? geometry.points[geometry.points.length - 1]
        : null;
    const resolvedDeltaValue = resolveSparklineDeltaValue(points);
    const strokeColor = color || theme.primary || '#2563EB';
    const resolvedPositiveColor = positiveColor || (theme.isDark ? '#86EFAC' : '#2F855A');
    const resolvedNegativeColor = negativeColor || (theme.isDark ? '#FDA4AF' : '#B91C1C');
    const deltaTone = resolvedDeltaValue === null
        ? (theme.textSec || theme.text || '#64748B')
        : (resolvedDeltaValue >= 0 ? resolvedPositiveColor : resolvedNegativeColor);
    const resolvedDeltaLabel = deltaLabel || (
        resolvedDeltaValue === null ? '' : `${resolvedDeltaValue >= 0 ? '+' : ''}${resolvedDeltaValue}`
    );
    const hasMetaLabels = (showCurrentValue && currentLabel) || (showDelta && resolvedDeltaLabel && !compact);
    const positiveBarColor = positiveColor || strokeColor;
    const negativeBarColor = negativeColor || resolvedNegativeColor;

    if (!Array.isArray(points) || points.length === 0) {
        return (
            <div
                data-pivot-sparkline-cell="true"
                data-pivot-sparkline-type={type}
                data-pivot-sparkline-points="0"
                style={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'flex-end', color: theme.textSec }}
            >
                —
            </div>
        );
    }

    return (
        <div
            data-pivot-sparkline-cell="true"
            data-pivot-sparkline-type={type}
            data-pivot-sparkline-points={points.length}
            title={title || undefined}
            style={{
                width: '100%',
                minWidth: 0,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: compact ? '4px' : '8px',
                paddingRight: compact ? '2px' : '6px',
            }}
        >
            <svg
                data-pivot-sparkline-svg="true"
                viewBox={`0 0 ${DEFAULT_WIDTH} ${DEFAULT_HEIGHT}`}
                aria-label="Cell sparkline"
                style={{
                    display: 'block',
                    flex: '1 1 auto',
                    minWidth: 0,
                    width: '100%',
                    height: compact ? '28px' : '34px',
                    overflow: 'visible',
                }}
            >
                {type === 'line' || type === 'area' ? (
                    <>
                        {type === 'area' && geometry.areaPath ? (
                            <path
                                d={geometry.areaPath}
                                fill={strokeColor}
                                opacity={areaOpacity}
                            />
                        ) : null}
                        {geometry.linePath ? (
                            <path
                                d={geometry.linePath}
                                fill="none"
                                stroke={strokeColor}
                                strokeWidth={compact ? '1.8' : '2.2'}
                                strokeLinecap="round"
                                strokeLinejoin="round"
                            />
                        ) : null}
                        {currentPoint ? (
                            <circle
                                cx={currentPoint.x}
                                cy={currentPoint.y}
                                r={compact ? '2.4' : '2.8'}
                                fill={strokeColor}
                            />
                        ) : null}
                    </>
                ) : null}
                {type === 'column' || type === 'bar' ? (
                    geometry.bars.map((bar, index) => (
                        <rect
                            key={`${bar.label || 'bar'}-${index}`}
                            x={bar.x}
                            y={bar.y}
                            width={bar.width}
                            height={bar.height}
                            rx={compact ? '1.5' : '2.5'}
                            ry={compact ? '1.5' : '2.5'}
                            fill={bar.positive ? positiveBarColor : negativeBarColor}
                            opacity={bar.positive ? 0.92 : 0.88}
                        />
                    ))
                ) : null}
            </svg>
            {hasMetaLabels ? (
                <div
                    style={{
                        flexShrink: 0,
                        display: 'flex',
                        flexDirection: 'column',
                        alignItems: 'flex-end',
                        gap: '1px',
                        minWidth: compact ? '0' : '46px',
                    }}
                >
                    {showCurrentValue && currentLabel ? (
                        <span
                            data-pivot-sparkline-current="true"
                            style={{
                                fontSize: compact ? '10px' : '11px',
                                lineHeight: 1.1,
                                fontWeight: 700,
                                color: theme.text,
                                whiteSpace: 'nowrap',
                            }}
                        >
                            {currentLabel}
                        </span>
                    ) : null}
                    {showDelta && resolvedDeltaLabel && !compact ? (
                        <span
                            data-pivot-sparkline-delta="true"
                            style={{
                                fontSize: '10px',
                                lineHeight: 1.1,
                                fontWeight: 700,
                                color: deltaTone,
                                whiteSpace: 'nowrap',
                            }}
                        >
                            {resolvedDeltaLabel}
                        </span>
                    ) : null}
                </div>
            ) : null}
        </div>
    );
}
