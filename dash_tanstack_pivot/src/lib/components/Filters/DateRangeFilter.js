import React, { useEffect, useState } from 'react';
import Icons from '../../utils/Icons';
import { getStyles } from '../../utils/styles';

const createDefaultDateCondition = () => ({ type: 'between', value: '', value2: '' });

const cloneDateConditions = (filter) => {
    if (filter && Array.isArray(filter.conditions)) {
        return filter.conditions.map(condition => ({ ...condition }));
    }
    if (filter && filter.value && !filter.conditions) {
        // Handle legacy single value or specialized filter structure
        return [{ type: 'eq', value: filter.value }];
    }
    return [createDefaultDateCondition()];
};

const formatLocalDate = (date) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
};

const DateRangeFilter = ({ onFilter, currentFilter, theme, onClose }) => {
    const resolvedTheme = theme || { primary: '#1976d2', border: '#e0e0e0', headerBg: '#f5f5f5', text: '#212121' };
    const styles = getStyles(resolvedTheme);
    
    const isMulti = currentFilter && currentFilter.conditions;
    const [operator, setOperator] = useState(isMulti ? (currentFilter.operator || 'AND') : 'AND');
    const [conditions, setConditions] = useState(() => cloneDateConditions(currentFilter));

    useEffect(() => {
        const nextIsMulti = currentFilter && currentFilter.conditions;
        setOperator(nextIsMulti ? (currentFilter.operator || 'AND') : 'AND');
        setConditions(cloneDateConditions(currentFilter));
    }, [currentFilter]);

    const updateCondition = (index, key, value) => {
        setConditions(previousConditions => previousConditions.map((condition, conditionIndex) => (
            conditionIndex === index ? { ...condition, [key]: value } : condition
        )));
    };

    const addCondition = () => {
        setConditions(previousConditions => [...previousConditions, createDefaultDateCondition()]);
    };

    const removeCondition = (index) => {
        setConditions(previousConditions => previousConditions.filter((_, i) => i !== index));
    };

    const apply = () => {
        const validConditions = conditions.filter(c => {
            if (c.type === 'between') return c.value && c.value2;
            return c.value;
        }).map(condition => ({ ...condition }));
        
        if (validConditions.length > 0) {
            onFilter({ operator, conditions: validConditions });
        } else {
            onFilter(null);
        }
        if (onClose) onClose();
    };

    const setPreset = (days) => {
        const e = new Date();
        const s = new Date();
        s.setDate(e.getDate() - days);
        // For presets, we reset to a single condition
        setConditions([{ type: 'between', value: formatLocalDate(s), value2: formatLocalDate(e) }]);
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
            <div style={{display: 'flex', justifyContent: 'flex-end', gap: '4px'}}>
                <button type="button" onClick={() => setOperator('AND')} style={{...styles.btn, padding: '2px 6px', fontSize: '10px', background: operator === 'AND' ? resolvedTheme.primary : (resolvedTheme.headerSubtleBg || '#eee'), color: operator === 'AND' ? '#fff' : resolvedTheme.text}}>AND</button>
                <button type="button" onClick={() => setOperator('OR')} style={{...styles.btn, padding: '2px 6px', fontSize: '10px', background: operator === 'OR' ? resolvedTheme.primary : (resolvedTheme.headerSubtleBg || '#eee'), color: operator === 'OR' ? '#fff' : resolvedTheme.text}}>OR</button>
            </div>

            <div style={{display: 'flex', gap: '6px', flexWrap: 'wrap'}}>
                <button type="button" onClick={() => setPreset(0)} style={{...styles.btn, fontSize:'11px', padding:'4px 8px'}}>Today</button>
                <button type="button" onClick={() => setPreset(7)} style={{...styles.btn, fontSize:'11px', padding:'4px 8px'}}>Last 7d</button>
                <button type="button" onClick={() => setPreset(30)} style={{...styles.btn, fontSize:'11px', padding:'4px 8px'}}>Last 30d</button>
            </div>

            <div style={{display: 'flex', flexDirection: 'column', gap: '8px', maxHeight: '200px', overflowY: 'auto'}}>
                {conditions.map((cond, index) => (
                    <div key={index} style={{display: 'flex', flexDirection: 'column', gap: '4px', border: '1px solid #f0f0f0', padding: '8px', borderRadius: '4px'}}>
                        <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                            <select value={cond.type} onChange={e => updateCondition(index, 'type', e.target.value)} style={{padding:'2px', borderRadius:'2px', border:'1px solid #ddd', fontSize: '11px', width: '100px'}}>
                                <option value="between">Between</option>
                                <option value="eq">Equals</option>
                                <option value="gt">After</option>
                                <option value="lt">Before</option>
                            </select>
                            {conditions.length > 1 && <span onClick={() => removeCondition(index)} style={{cursor: 'pointer'}}><Icons.Close/></span>}
                        </div>
                        <div style={{display: 'flex', gap: '4px', alignItems: 'center'}}>
                            <input type="date" value={cond.value} onChange={e => updateCondition(index, 'value', e.target.value)} style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', flex: 1, minWidth: 0, fontSize: '12px'}} />
                            {cond.type === 'between' && (
                                <>
                                    <span>-</span>
                                    <input type="date" value={cond.value2 || ''} onChange={e => updateCondition(index, 'value2', e.target.value)} style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', flex: 1, minWidth: 0, fontSize: '12px'}} />
                                </>
                            )}
                        </div>
                    </div>
                ))}
            </div>
            
            <div style={{display: 'flex', gap: '8px'}}>
                <button type="button" onClick={addCondition} style={{...styles.btn, flex: 1, justifyContent: 'center', background: resolvedTheme.headerSubtleBg || '#f5f5f5', fontSize: '11px'}}>Add</button>
                <button type="button" onClick={apply} style={{...styles.btn, flex: 1, justifyContent: 'center', background: resolvedTheme.primary, color: '#fff', fontSize: '11px'}}>Apply</button>
            </div>
        </div>
    );
};

export default DateRangeFilter;
