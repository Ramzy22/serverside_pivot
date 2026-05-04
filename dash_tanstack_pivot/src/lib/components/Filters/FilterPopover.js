import React, { useState, useRef, useEffect } from 'react';
import ColumnFilter from './ColumnFilter';

const FALLBACK_WIDTH = 300;
const FALLBACK_HEIGHT = 400;
const VIEWPORT_PADDING = 16;

const FilterPopover = ({ column, anchorEl, onClose, onFilter, currentFilter, options = [], optionMeta = null, onSearchOptions, onLoadMoreOptions, theme }) => {
    const resolvedTheme = theme || { background: '#fff', surfaceBg: '#fff', border: '#ccc', text: '#333' };
    const [position, setPosition] = useState(null);
    const popoverRef = useRef(null);
    const columnAnchorTarget = typeof Element !== 'undefined' && column instanceof Element ? column : null;
    const columnPositionKey = columnAnchorTarget || (column && (column.id || column.header)) || null;

    useEffect(() => {
        const target = anchorEl || columnAnchorTarget;
        if (!target || typeof target.getBoundingClientRect !== 'function' || typeof window === 'undefined') {
            setPosition(prevPosition => (prevPosition === null ? prevPosition : null));
            return;
        }

        const updatePosition = () => {
            const rect = target.getBoundingClientRect();
            const viewportHeight = window.innerHeight;
            const viewportWidth = window.innerWidth;
            const popoverWidth = (popoverRef.current && popoverRef.current.offsetWidth) || FALLBACK_WIDTH;
            const popoverHeight = (popoverRef.current && popoverRef.current.offsetHeight) || FALLBACK_HEIGHT;

            let top = rect.bottom;
            let left = rect.left;

            if (top + popoverHeight + VIEWPORT_PADDING > viewportHeight) {
                top = rect.top - popoverHeight;
            }

            top = Math.max(VIEWPORT_PADDING, Math.min(top, viewportHeight - popoverHeight - VIEWPORT_PADDING));
            left = Math.max(VIEWPORT_PADDING, Math.min(left, viewportWidth - popoverWidth - VIEWPORT_PADDING));

            setPosition((prevPosition) => {
                if (prevPosition && prevPosition.top === top && prevPosition.left === left) {
                    return prevPosition;
                }
                return { top, left };
            });
        };

        updatePosition();
        const rafId = window.requestAnimationFrame ? window.requestAnimationFrame(updatePosition) : null;
        window.addEventListener('resize', updatePosition);
        window.addEventListener('scroll', updatePosition, true);

        return () => {
            if (rafId !== null && window.cancelAnimationFrame) {
                window.cancelAnimationFrame(rafId);
            }
            window.removeEventListener('resize', updatePosition);
            window.removeEventListener('scroll', updatePosition, true);
        };
    }, [anchorEl, columnAnchorTarget, columnPositionKey]);

    if (!position) {
        return null;
    }

    return (
        <div ref={popoverRef}
            style={{
                position: 'fixed', // Changed from absolute
                top: `${position.top}px`,
                left: `${position.left}px`,
                background: resolvedTheme.surfaceBg || resolvedTheme.background || '#fff',
                border: `1px solid ${resolvedTheme.border || '#ccc'}`,
                boxShadow: resolvedTheme.shadowMd || '0 4px 12px rgba(0,0,0,0.15)',
                zIndex: 1000,
                padding: '12px',
                borderRadius: resolvedTheme.radiusSm || '4px',
                width: '300px',
                display: 'flex',
                flexDirection: 'column',
                gap: '8px',
                color: resolvedTheme.text || '#333'
            }} onClick={e => e.stopPropagation()}>
            <ColumnFilter 
                column={column} 
                onFilter={onFilter} 
                currentFilter={currentFilter} 
                options={options} 
                optionMeta={optionMeta}
                onSearchOptions={onSearchOptions}
                onLoadMoreOptions={onLoadMoreOptions}
                theme={resolvedTheme}
                onClose={onClose} 
            />
        </div>
    );
};

export default FilterPopover;
