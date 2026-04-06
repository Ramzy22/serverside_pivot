import React from 'react';
import DetailSurfaceContent from './DetailSurfaceContent';

export function InlineDetailPanel({
    detailState,
    onClose,
    onPageChange,
    onSort,
    onFilter,
    theme,
    width,
    top,
    height = 280,
}) {
    if (!detailState) return null;
    return (
        <div style={{
            position: 'absolute',
            top: `${top}px`,
            left: 0,
            width: `${width}px`,
            height: `${Math.max(220, Number(height) || 280)}px`,
            zIndex: 95,
            border: `1px solid ${theme.border}`,
            borderRadius: '14px',
            overflow: 'hidden',
            background: theme.surfaceBg || theme.background,
            boxShadow: '0 18px 40px rgba(15,23,42,0.16)',
        }}>
            <DetailSurfaceContent
                detailState={detailState}
                onClose={onClose}
                onPageChange={onPageChange}
                onSort={onSort}
                onFilter={onFilter}
                theme={theme}
                compact
            />
        </div>
    );
}

export default InlineDetailPanel;
