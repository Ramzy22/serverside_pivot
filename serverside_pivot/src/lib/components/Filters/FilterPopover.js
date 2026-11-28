import React, { useState, useRef, useEffect } from 'react';
import ColumnFilter from './ColumnFilter';
import { useDismiss, OverlayPortal } from '../../hooks/useDismiss';
import { computeAnchoredPlacement } from '../../utils/anchoredPlacement';

const POPOVER_WIDTH = 320;
const VIEWPORT_PADDING = 12;

/** @param {any} p0 @returns {any} */
const FilterPopover = ({ column, anchorEl, onClose, onFilter, currentFilter, options = [], optionMeta = null, onSearchOptions, onLoadMoreOptions, theme }) => {
    /** @type {any} */
    const resolvedTheme = theme || { background: '#fff', surfaceBg: '#fff', border: '#ccc', text: '#333' };
    /** @type {[any, (...args: any[]) => any]} */
    const [position, setPosition] = useState(null);
    /** @type {any} */
    const popoverRef = useRef(null);
    /** @type {any} */
    const columnAnchorTarget = typeof Element !== 'undefined' && column instanceof Element ? column : null;
    /** @type {any} */
    const resolvedAnchorTarget = anchorEl || columnAnchorTarget;
    /** @type {any} */
    const columnPositionKey = columnAnchorTarget || (column && (column.id || column.header)) || null;

    useDismiss({
        open: Boolean(position),
        onClose,
        refs: [popoverRef, resolvedAnchorTarget],
        restoreFocus: true,
    });

    useEffect(() => {
        /** @type {any} */
        const target = anchorEl || columnAnchorTarget;
        if (!target || typeof target.getBoundingClientRect !== 'function' || typeof window === 'undefined') {
            setPosition(/** @param {any} [prevPosition] @returns {any} */ prevPosition => (prevPosition === null ? prevPosition : null));
            return;
        }

        /** @returns {any} */
        const updatePosition = () => {
            /** @type {any} */
            const anchorRect = target.getBoundingClientRect();
            // Open below the whole column header (not the icon inside it), aligned to the icon.
            /** @type {any} */
            const header = typeof target.closest === 'function' ? target.closest('[role="columnheader"]') : null;
            /** @type {any} */
            const headerRect = header && typeof header.getBoundingClientRect === 'function' ? header.getBoundingClientRect() : null;
            /** @type {any} */
            const rect = headerRect
                ? { top: headerRect.top, bottom: headerRect.bottom, left: anchorRect.left }
                : anchorRect;
            /** @type {any} */
            const popover = popoverRef.current;
            /** @type {any} */
            const next = computeAnchoredPlacement(rect, {
                width: (popover && popover.offsetWidth) || Math.min(POPOVER_WIDTH, window.innerWidth - (VIEWPORT_PADDING * 2)),
                // scrollHeight is the content's natural height even while maxHeight clips it.
                contentHeight: popover ? popover.scrollHeight : undefined,
                viewportWidth: window.innerWidth,
                viewportHeight: window.innerHeight,
                margin: VIEWPORT_PADDING,
                comfortableHeight: 240,
            });

            setPosition(/** @param {any} [prevPosition] @returns {any} */ (prevPosition) => {
                if (prevPosition && prevPosition.top === next.top && prevPosition.left === next.left && prevPosition.maxHeight === next.maxHeight) {
                    return prevPosition;
                }
                return { top: next.top, left: next.left, maxHeight: next.maxHeight };
            });
        };

        updatePosition();
        /** @type {any} */
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
        <OverlayPortal theme={resolvedTheme} anchorEl={resolvedAnchorTarget} zIndex={10002}>
            <div
                ref={popoverRef}
                role="dialog"
                aria-label="Filter column"
                className="sp-menu-surface"
                style={{
                    position: 'fixed',
                    top: `${position.top}px`,
                    left: `${position.left}px`,
                    background: resolvedTheme.surfaceBg || resolvedTheme.background || '#fff',
                    backdropFilter: 'blur(18px) saturate(180%)',
                    WebkitBackdropFilter: 'blur(18px) saturate(180%)',
                    border: `1px solid ${resolvedTheme.border || 'var(--sp-border, #cbd5e1)'}`,
                    boxShadow: resolvedTheme.shadow || resolvedTheme.shadowMd || '0 14px 36px rgba(0,0,0,0.18)',
                    zIndex: 10002,
                    padding: '12px',
                    borderRadius: 'var(--sp-radius-lg, 12px)',
                    width: `${POPOVER_WIDTH}px`,
                    maxWidth: `calc(100vw - ${VIEWPORT_PADDING * 2}px)`,
                    maxHeight: `${position.maxHeight}px`,
                    overflow: 'hidden',
                    boxSizing: 'border-box',
                    pointerEvents: 'auto',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '8px',
                    color: resolvedTheme.text || 'var(--sp-text, #0f172a)'
                }}
                onClick={e => e.stopPropagation()}
            >
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
        </OverlayPortal>
    );
};

export default FilterPopover;
