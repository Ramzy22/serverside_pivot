import React, { useState, useRef, useEffect } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import Icons from '../../utils/Icons';

/** @param {any} p0 @returns {any} */
const ToolPanelSection = ({ title, children, items, renderItem, theme, styles, initialExpanded = true, count, onDrop, sectionId }) => {
    /** @type {[any, (...args: any[]) => any]} */
    const [expanded, setExpanded] = useState(initialExpanded);
    const [renderItemsWhileClosing, setRenderItemsWhileClosing] = useState(initialExpanded);
    const [scrollSettled, setScrollSettled] = useState(initialExpanded);
    const [isOver, setIsOver] = useState(false);
    const contentRef = useRef(null);

    useEffect(() => {
        if (expanded) {
            setRenderItemsWhileClosing(true);
            const openTimer = setTimeout(() => setScrollSettled(true), 220);
            return () => clearTimeout(openTimer);
        }
        setScrollSettled(false);
        const closeTimer = setTimeout(() => setRenderItemsWhileClosing(false), 220);
        return () => clearTimeout(closeTimer);
    }, [expanded]);

    /** @type {any} */
    const rowVirtualizer = useVirtualizer({
        count: items ? items.length : 0,
        getScrollElement: () => contentRef.current,
        estimateSize: () => 32,
        overscan: 10,
        enabled: !!items && (expanded || renderItemsWhileClosing)
    });

    /** @param {any} [e] @returns {any} */
    const handleDragOver = (e) => {
        e.preventDefault();
        setIsOver(true);
    };

    /** @returns {any} */
    const handleDragLeave = () => {
        setIsOver(false);
    };

    /** @param {any} [e] @returns {any} */
    const handleDrop = (e) => {
        e.preventDefault();
        setIsOver(false);
        /** @type {any} */
        const columnId = e.dataTransfer.getData('text/plain');
        if (onDrop && columnId) {
            onDrop(columnId, sectionId);
        }
    };

    /** @param {any} [e] @returns {any} */
    const handleHeaderKeyDown = (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            setExpanded(/** @param {any} [prev] @returns {any} */ (prev) => !prev);
        }
    };

    return (
        <div
            data-is-over={isOver ? 'true' : undefined}
            style={{
                ...styles.toolPanelSection,
                flex: expanded ? '0 1 auto' : '0 0 auto',
                minHeight: expanded ? '40px' : '32px',
                overflow: 'hidden', // Prevent overlap
                borderRadius: isOver ? '8px' : undefined,
                background: isOver ? (theme.select || 'rgba(37,99,235,0.08)') : undefined,
                boxShadow: isOver ? `inset 0 0 0 1.5px ${theme.primary}` : undefined,
                transition: 'background-color 140ms ease, box-shadow 140ms ease',
            }}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
        >
            <div
                role="button"
                tabIndex={0}
                aria-expanded={expanded}
                className="sp-btn sp-btn-ghost"
                style={{ ...styles.toolPanelSectionHeader, borderRadius: '6px' }}
                onClick={() => setExpanded(!expanded)}
                onKeyDown={handleHeaderKeyDown}
            >
                <span style={{
                    marginRight: '8px',
                    opacity: 0.72,
                    display: 'inline-flex',
                    alignItems: 'center',
                    transform: expanded ? 'rotate(90deg)' : 'rotate(0deg)',
                    transition: 'transform 160ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1))',
                }}>
                    <Icons.ChevronRight />
                </span>
                <span style={{ flex: 1 }}>{title}</span>
                {count !== undefined && <span style={{ fontSize: '10px', opacity: 0.7, background: theme.hover, padding: '2px 7px', borderRadius: '10px', fontVariantNumeric: 'tabular-nums' }}>{count}</span>}
            </div>

            <div
                className="sp-disclosure-grid"
                data-expanded={expanded ? 'true' : 'false'}
                style={{ pointerEvents: expanded ? 'auto' : 'none' }}
            >
            <div
                ref={contentRef}
                className="sp-disclosure-inner"
                style={{
                    ...styles.toolPanelList,
                    maxHeight: '300px',
                    overflowY: expanded && scrollSettled ? 'auto' : 'hidden',
                }}
            >
                {items ? (
                    <div style={{
                        height: `${rowVirtualizer.getTotalSize()}px`,
                        width: '100%',
                        position: 'relative'
                    }}>
                        {rowVirtualizer.getVirtualItems().map(/** @param {any} [virtualRow] @returns {any} */ virtualRow => (
                            <div
                                key={virtualRow.index}
                                style={{
                                    position: 'absolute',
                                    top: 0,
                                    left: 0,
                                    width: '100%',
                                    height: `${virtualRow.size}px`,
                                    transform: `translateY(${virtualRow.start}px)`
                                }}
                            >
                                {renderItem(items[virtualRow.index], virtualRow.index)}
                            </div>
                        ))}
                    </div>
                ) : children}
            </div>
            </div>
        </div>
    );
};

export default ToolPanelSection;
