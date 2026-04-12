import React, { useState } from 'react';
import Icons from '../../utils/Icons';
import { getAllLeafColumns, getAllLeafIdsFromColumn, hasChildrenInZone } from '../../utils/helpers';
import { formatDisplayLabel } from '../../utils/helpers';

const ColumnTreeItem = ({ column, level, theme, styles, handlePinColumn, colSearch, selectedCols, setSelectedCols, onDrop, sectionId }) => {
    const [expanded, setExpanded] = useState(level < 1); // Only expand root level by default
    const [isHovered, setIsHovered] = useState(false);
    const [isDragging, setIsDragging] = useState(false);
    
    const isGroup = column.columns && column.columns.length > 0;
    
    const header = column.columnDef.header;
    // Extract clean label, removing group prefix
    let label = typeof header === 'string' ? header : column.id;
    if (typeof label === 'string' && label.startsWith('group_')) {
        label = column.id.replace('group_', '').split('|||').pop() || label;
    }
    // For React elements (like the collapse button), extract the headerVal
    if (column.headerVal) {
        label = column.headerVal;
    }
    label = formatDisplayLabel(label);
    
    const pin = column.getIsPinned();
    const isVisible = column.getIsVisible();
    const isSelected = selectedCols.has(column.id);

    if (colSearch && !label.toLowerCase().includes(colSearch.toLowerCase())) {
        if (isGroup) {
            const anyChildMatches = (col) => {
                const childHeader = col.columnDef.header;
                const childLabel = typeof childHeader === 'string' ? childHeader : col.id;
                if (childLabel.toLowerCase().includes(colSearch.toLowerCase())) return true;
                if (col.columns) return col.columns.some(anyChildMatches);
                return false;
            };
            if (!anyChildMatches(column)) return null;
        } else {
            return null;
        }
    }

    const toggleSelection = (e) => {
        e.stopPropagation();
        const newSet = new Set(selectedCols);
        // User fix: When toggling a group selection, it now only selects the leaf columns (removed the parent group ID from the selection array).
        const ids = isGroup ? getAllLeafIdsFromColumn(column) : [column.id];
        
        const allSelected = ids.every(id => newSet.has(id));
        if (allSelected) {
            ids.forEach(id => newSet.delete(id));
        } else {
            ids.forEach(id => newSet.add(id));
        }
        setSelectedCols(newSet);
    };

    const toggleVisibility = (e) => {
        e.stopPropagation();
        
        if (isGroup) {
            const leafCols = getAllLeafColumns(column);
            const shouldShow = leafCols.some(c => !c.getIsVisible());
            leafCols.forEach(c => c.toggleVisibility(shouldShow));
        } else {
            column.toggleVisibility();
        }
    };

    const handlePin = (e, side) => {
        e.stopPropagation();
        handlePinColumn(column.id, side);
    };

    const onColDragStart = (e) => {
        setIsDragging(true);
        e.dataTransfer.setData('text/plain', column.id);
        e.dataTransfer.effectAllowed = 'move';
    };

    const onColDragEnd = () => {
        setIsDragging(false);
    };

    const handleItemDrop = (e) => {
        e.preventDefault();
        e.stopPropagation();
        const droppedColId = e.dataTransfer.getData('text/plain');
        if (droppedColId && onDrop && droppedColId !== column.id) {
             onDrop(droppedColId, sectionId, column.id);
        }
    };

    const handleKeyDown = (e) => {
        if (e.key === 'Enter') {
            toggleVisibility(e);
        } else if (e.key === ' ') {
            e.preventDefault();
            toggleSelection(e);
        } else if (e.altKey && e.key === 'ArrowLeft') {
            e.preventDefault();
            handlePinColumn(column.id, 'left');
        } else if (e.altKey && e.key === 'ArrowRight') {
            e.preventDefault();
            handlePinColumn(column.id, 'right');
        } else if (e.altKey && e.key === 'ArrowDown') {
            e.preventDefault();
            handlePinColumn(column.id, false);
        }
    };

    const getPinBtnStyle = (active) => ({
        padding: '4px', 
        background: active ? theme.primary : 'transparent',
        border: 'none', 
        cursor: 'pointer', 
        borderRadius: '4px', 
        color: active ? '#fff' : theme.textSec,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        opacity: active ? 1 : 0.6,
        transition: 'all 0.2s'
    });

    return (
        <div 
            style={{ display: 'flex', flexDirection: 'column', opacity: isDragging ? 0.5 : 1 }}
            draggable={!isGroup}
            onDragStart={onColDragStart}
            onDragEnd={onColDragEnd}
            onDragOver={e => e.preventDefault()}
            onDrop={handleItemDrop}
            role="treeitem"
            aria-selected={isSelected}
            aria-expanded={expanded}
        >
            <div 
                style={{
                    ...styles.columnItem,
                    paddingLeft: `${level * 12 + 8}px`, // Reduced indentation step
                    background: isSelected ? theme.select : (isHovered ? theme.hover : 'transparent'),
                    borderLeft: pin ? `3px solid ${theme.primary}` : '3px solid transparent'
                }} 
                onMouseEnter={e => !isSelected && setIsHovered(true)} 
                onMouseLeave={e => !isSelected && setIsHovered(false)}
                tabIndex={0}
                onKeyDown={handleKeyDown}
            >
                <input 
                    type="checkbox" 
                    checked={isSelected} 
                    onChange={toggleSelection}
                    onClick={(e) => { e.stopPropagation(); toggleSelection(e); }}
                    style={{ margin: 0, cursor: 'pointer', pointerEvents: 'auto' }}
                    tabIndex={-1} 
                />

                {!isGroup && (
                    <span style={{ cursor: 'grab', display: 'flex', opacity: 0.7, marginRight: '4px' }}>
                        <Icons.DragIndicator />
                    </span>
                )}
                
                {isGroup ? (
                    <span onClick={() => setExpanded(!expanded)} style={{ cursor: 'pointer', display: 'flex', opacity: 0.7, marginRight: '4px' }}>
                        {expanded ? <Icons.ChevronDown /> : <Icons.ChevronRight />}
                    </span>
                ) : <span style={{ width: '20px' }} />}
                
                <span 
                    onClick={toggleVisibility}
                    style={{ 
                        cursor: 'pointer', 
                        display: 'flex', 
                        color: isVisible ? theme.primary : theme.textSec,
                        opacity: isVisible ? 1 : 0.5,
                        marginRight: '4px'
                    }}
                    title={isVisible ? "Hide Column" : "Show Column"}
                >
                    {isVisible ? <Icons.Visibility /> : <Icons.VisibilityOff />}
                </span>
                
                <span 
                    style={{ 
                        flex: 1, 
                        overflow: 'hidden', 
                        textOverflow: 'ellipsis', 
                        whiteSpace: 'nowrap',
                        fontWeight: isGroup ? 600 : 400,
                        cursor: isGroup ? 'pointer' : 'default',
                        color: isVisible ? theme.text : theme.textSec,
                        opacity: isVisible ? 1 : 0.6,
                        display: 'flex', alignItems: 'center'
                    }} 
                    onClick={() => isGroup && setExpanded(!expanded)}
                    title={label}
                >
                    {isGroup && <Icons.Group style={{ marginRight: '6px', fontSize: '14px', opacity: 0.8 }} />}
                    {label}
                </span>

                {!isGroup && (
                    <div className="pin-controls">
                        <button onClick={(e) => handlePin(e, 'left')}>
                            <Icons.PinLeft />
                        </button>
                        <button onClick={(e) => handlePin(e, false)}>
                            <Icons.Unpin />
                        </button>
                        <button onClick={(e) => handlePin(e, 'right')}>
                            <Icons.PinRight />
                        </button>
                    </div>
                )}
            </div>
            {isGroup && expanded && (
                <div style={{ display: 'flex', flexDirection: 'column' }}>
                    {column.columns
                        .filter(child => hasChildrenInZone(child, sectionId))
                        .map(child => (
                        <ColumnTreeItem 
                            key={child.id} 
                            column={child} 
                            level={level + 1} 
                            theme={theme} 
                            styles={styles} 
                            handlePinColumn={handlePinColumn}
                            colSearch={colSearch}
                            selectedCols={selectedCols}
                            setSelectedCols={setSelectedCols}
                            onDrop={onDrop}
                            sectionId={sectionId}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

export default ColumnTreeItem;
