import React, { useState, useRef, useEffect } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import Icons from '../Icons';

const ToolPanelSection = ({ title, children, items, renderItem, theme, styles, initialExpanded = true, count, onDrop, sectionId }) => {
    const [expanded, setExpanded] = useState(initialExpanded);
    const [height, setHeight] = useState(initialExpanded ? 200 : 0);
    const [isOver, setIsOver] = useState(false);
    const contentRef = useRef(null);
    const parentRef = useRef(null);

    useEffect(() => {
        if (expanded && contentRef.current) {
            const contentHeight = contentRef.current.scrollHeight;
            setHeight(Math.min(contentHeight, 300)); // Cap at 300px
        } else {
            setHeight(0);
        }
    }, [expanded, items, count]);

    const rowVirtualizer = useVirtualizer({
        count: items ? items.length : 0,
        getScrollElement: () => contentRef.current,
        estimateSize: () => 32,
        overscan: 10,
        enabled: !!items && expanded
    });

    const handleDragOver = (e) => {
        e.preventDefault();
        setIsOver(true);
    };

    const handleDragLeave = () => {
        setIsOver(false);
    };

    const handleDrop = (e) => {
        e.preventDefault();
        setIsOver(false);
        const columnId = e.dataTransfer.getData('text/plain');
        if (onDrop && columnId) {
            onDrop(columnId, sectionId);
        }
    };

    return (
        <div
            style={{
                ...styles.toolPanelSection,
                flex: expanded ? '0 1 auto' : '0 0 auto',
                minHeight: expanded ? '40px' : '32px',
                overflow: 'hidden' // Prevent overlap
            }}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
        >
            <div style={styles.toolPanelSectionHeader} onClick={() => setExpanded(!expanded)}>
                <span style={{ marginRight: '8px', opacity: 0.7, display: 'flex' }}>
                    {expanded ? <Icons.ChevronDown /> : <Icons.ChevronRight />}
                </span>
                <span style={{ flex: 1 }}>{title}</span>
                {count !== undefined && <span style={{ fontSize: '10px', opacity: 0.5, background: theme.hover, padding: '2px 6px', borderRadius: '10px' }}>{count}</span>}
            </div>

            <div
                ref={contentRef}
                style={{
                    ...styles.toolPanelList,
                    height: `${height}px`,
                    overflowY: expanded ? 'auto' : 'hidden',
                    transition: 'height 0.3s ease',
                    opacity: expanded ? 1 : 0
                }}
            >
                {items ? (
                    <div style={{
                        height: `${rowVirtualizer.getTotalSize()}px`,
                        width: '100%',
                        position: 'relative'
                    }}>
                        {rowVirtualizer.getVirtualItems().map(virtualRow => (
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
    );
};

export default ToolPanelSection;
