import React from 'react';

/**
 * Lightweight Apple-style shimmer skeleton used as Suspense fallback for lazy UI panels.
 * @param {{ variant?: string, height?: any, width?: any, style?: any }} [props]
 * @returns {any}
 */
export default function ShimmerFallback({ variant = 'panel', height = undefined, width = undefined, style = undefined } = {}) {
    const shimmerBg = 'linear-gradient(90deg, var(--sp-hover, rgba(148,163,184,0.08)) 20%, var(--sp-hover-strong, rgba(148,163,184,0.18)) 50%, var(--sp-hover, rgba(148,163,184,0.08)) 80%)';
    const shimmerStyle = {
        background: shimmerBg,
        backgroundSize: '200% 100%',
        animation: 'pivot-skeleton-shimmer 1.4s ease-in-out infinite',
        borderRadius: 6,
    };

    if (variant === 'inline') {
        return (
            <div
                aria-busy="true"
                aria-label="Loading panel"
                style={{
                    width: width || '100%',
                    height: height || 180,
                    padding: '12px 16px',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: 10,
                    background: 'var(--sp-surface, transparent)',
                    borderTop: '1px solid var(--sp-border, rgba(148,163,184,0.16))',
                    ...style,
                }}
            >
                <div style={{ ...shimmerStyle, height: 14, width: '28%' }} />
                <div style={{ ...shimmerStyle, height: 12, width: '92%' }} />
                <div style={{ ...shimmerStyle, height: 12, width: '84%' }} />
                <div style={{ ...shimmerStyle, height: 12, width: '68%' }} />
            </div>
        );
    }

    if (variant === 'chart') {
        return (
            <div
                aria-busy="true"
                aria-label="Loading chart"
                style={{
                    width: width || '100%',
                    height: height || 220,
                    padding: 16,
                    display: 'flex',
                    flexDirection: 'column',
                    gap: 12,
                    borderRadius: 12,
                    background: 'var(--sp-surface, transparent)',
                    border: '1px solid var(--sp-border, rgba(148,163,184,0.16))',
                    ...style,
                }}
            >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ ...shimmerStyle, height: 14, width: '34%' }} />
                    <div style={{ ...shimmerStyle, height: 22, width: 72 }} />
                </div>
                <div style={{ display: 'flex', alignItems: 'flex-end', gap: 10, flex: 1, paddingTop: 8 }}>
                    {[52, 78, 42, 88, 64, 72].map((pct, index) => (
                        <div key={index} style={{ ...shimmerStyle, flex: 1, height: `${pct}%`, borderRadius: 4 }} />
                    ))}
                </div>
            </div>
        );
    }

    const isSidebar = variant === 'sidebar';

    return (
        <div
            aria-busy="true"
            aria-label="Loading panel"
            style={{
                width: width || 300,
                height: height || '100%',
                minHeight: 160,
                padding: 14,
                display: 'flex',
                flexDirection: 'column',
                gap: 12,
                background: 'var(--sp-sidebar-bg, var(--sp-surface, transparent))',
                borderLeft: isSidebar ? undefined : '1px solid var(--sp-border, rgba(148,163,184,0.16))',
                borderRight: isSidebar ? '1px solid var(--sp-border, rgba(148,163,184,0.16))' : undefined,
                ...style,
            }}
        >
            <div style={{ ...shimmerStyle, height: 28, width: '100%', borderRadius: 8 }} />
            <div style={{ ...shimmerStyle, height: 76, width: '100%', borderRadius: 8 }} />
            <div style={{ ...shimmerStyle, height: 76, width: '100%', borderRadius: 8 }} />
            <div style={{ ...shimmerStyle, height: 120, width: '100%', borderRadius: 8 }} />
        </div>
    );
}
