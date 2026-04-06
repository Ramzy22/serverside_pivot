import React from 'react';
import DetailSurfaceContent from './DetailSurfaceContent';

export function DetailSidePanel({
    detailState,
    onClose,
    onPageChange,
    onSort,
    onFilter,
    theme,
    width = 480,
}) {
    if (!detailState) return null;
    return (
        <div style={{
            width: `${Math.max(320, Number(width) || 480)}px`,
            minWidth: `${Math.max(320, Number(width) || 480)}px`,
            maxWidth: '52vw',
            borderLeft: `1px solid ${theme.border}`,
            background: theme.surfaceBg || theme.background,
            display: 'flex',
            flexDirection: 'column',
            minHeight: 0,
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

export default DetailSidePanel;
