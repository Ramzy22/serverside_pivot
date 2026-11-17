import React from 'react';

const SKELETON_SEGMENT_WIDTHS = ['22%', '16%', '18%', '14%'];

/** @param {any} p0 @returns {any} */
const SkeletonRow = ({ style, rowHeight }) => {
    const barHeight = Math.max(10, Math.floor((rowHeight || 32) * 0.52));
    return (
        <div style={{ ...style, display: 'flex', alignItems: 'center', gap: '12px', padding: '0 10px', boxSizing: 'border-box' }} className="pivot-skeleton-row">
            {SKELETON_SEGMENT_WIDTHS.map((width, idx) => (
                <div
                    key={idx}
                    style={{
                        width,
                        flex: idx === SKELETON_SEGMENT_WIDTHS.length - 1 ? '1 1 14%' : `0 0 ${width}`,
                        height: `${barHeight}px`,
                        background: 'var(--pivot-loading-cell-gradient, linear-gradient(90deg, rgba(148,163,184,0.14) 0%, rgba(148,163,184,0.28) 45%, rgba(148,163,184,0.14) 100%))',
                        backgroundSize: '220% 100%',
                        borderRadius: '6px',
                        animation: 'pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 1.6s) linear infinite',
                    }}
                />
            ))}
        </div>
    );
};

export default SkeletonRow;
