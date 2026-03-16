import React, { useState } from 'react';
import Icons from '../Icons';
import ColumnFilter from '../Filters/ColumnFilter';

const SidebarFilterItem = ({ column, theme, styles, onFilter, currentFilter, options }) => {
    const [expanded, setExpanded] = useState(false);
    const hasFilter = currentFilter && (currentFilter.conditions || currentFilter.value);

    return (
        <div style={{display: 'flex', flexDirection: 'column'}}>
            <div 
                style={{
                    ...styles.columnItem,
                    cursor: 'pointer',
                    background: expanded ? theme.select : 'transparent',
                    borderLeft: hasFilter ? `3px solid ${theme.primary}` : '3px solid transparent'
                }}
                onClick={() => setExpanded(!expanded)}
            >
                <span style={{marginRight: '8px', opacity: 0.7, display: 'flex'}}>
                    {expanded ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                </span>
                <span style={{flex: 1, fontWeight: 500, display: 'flex', alignItems: 'center', gap: '6px'}}>
                    {hasFilter && <Icons.Filter style={{fontSize: '12px', color: theme.primary}}/>}
                    {typeof column.header === 'string' ? column.header : (column.columnDef && typeof column.columnDef.header === 'string' ? column.columnDef.header : column.id)}
                </span>
            </div>
            {expanded && (
                <div style={{padding: '8px', borderBottom: `1px solid ${theme.border}44`}}>
                    <ColumnFilter
                        column={column}
                        onFilter={onFilter}
                        currentFilter={currentFilter}
                        options={options}
                        theme={theme}
                    />
                </div>
            )}
        </div>
    );
};

export default SidebarFilterItem;
