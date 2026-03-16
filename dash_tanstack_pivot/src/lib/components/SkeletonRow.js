import React from 'react';

const SkeletonRow = ({ style, rowHeight }) => {
    return (
        <div style={{ ...style, display: 'flex', alignItems: 'center' }} className="pivot-skeleton-row">
            <div 
                style={{ 
                    width: '100%', 
                    height: `${Math.floor(rowHeight * 0.6)}px`, 
                    background: 'var(--pivot-loading-cell-gradient, linear-gradient(90deg, rgba(232,242,255,0.7) 0%, rgba(190,218,255,0.94) 45%, rgba(232,242,255,0.7) 100%))',
                    backgroundSize: '220% 100%',
                    borderRadius: '4px',
                    margin: '0 8px',
                    animation: 'pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite'
                }} 
            />
        </div>
    );
};

export default SkeletonRow;
