import React, { useState, useRef, useEffect } from 'react';
import ColumnFilter from './ColumnFilter';

const FALLBACK_WIDTH = 300;
const FALLBACK_HEIGHT = 400;
const VIEWPORT_PADDING = 16;

const FilterPopover = ({ column, anchorEl, onClose, onFilter, currentFilter, options = [], theme }) => {
    const [position, setPosition] = useState(null);
    const popoverRef = useRef(null);

    useEffect(() => {
        const target = anchorEl || (typeof Element !== 'undefined' && column instanceof Element ? column : null);
        if (!target) {
            setPosition(null);
            return;
        }

        const rect = target.getBoundingClientRect();
        const viewportHeight = window.innerHeight;
        const viewportWidth = window.innerWidth;
        const popoverWidth = popoverRef.current?.offsetWidth || FALLBACK_WIDTH;
        const popoverHeight = popoverRef.current?.offsetHeight || FALLBACK_HEIGHT;

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
    }, [anchorEl, column]);

    if (!position) {
        return null;
    }

    return (
        <div ref={popoverRef}
            style={{
                position: 'fixed', // Changed from absolute
                top: `${position.top}px`,
                left: `${position.left}px`,
                background: '#fff',
                border: '1px solid #ccc',
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                zIndex: 1000,
                padding: '12px',
                borderRadius: '4px',
                width: '300px',
                display: 'flex',
                flexDirection: 'column',
                gap: '8px',
                color: '#333'
            }} onClick={e => e.stopPropagation()}>
            <ColumnFilter 
                column={column} 
                onFilter={onFilter} 
                currentFilter={currentFilter} 
                options={options} 
                theme={theme} 
                onClose={onClose} 
            />
        </div>
    );
};

export default FilterPopover;
