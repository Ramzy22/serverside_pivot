import React from 'react';
import DetailSurfaceContent from './DetailSurfaceContent';

export function DetailDrawer({
    detailState,
    onClose,
    onPageChange,
    onSort,
    onFilter,
    theme,
    height = 320,
}) {
    if (!detailState) return null;
    return (
        <div style={{
            position: 'absolute',
            left: 0,
            right: 0,
            bottom: 0,
            height: `${Math.max(220, Number(height) || 320)}px`,
            borderTop: `1px solid ${theme.border}`,
            background: theme.surfaceBg || theme.background,
            zIndex: 85,
            boxShadow: '0 -10px 24px rgba(15,23,42,0.12)',
            overflow: 'hidden',
        }}>
            <DetailSurfaceContent
                detailState={detailState}
                onClose={onClose}
                onPageChange={onPageChange}
                onSort={onSort}
                onFilter={onFilter}
                theme={theme}
            />
        </div>
    );
}

export default DetailDrawer;
