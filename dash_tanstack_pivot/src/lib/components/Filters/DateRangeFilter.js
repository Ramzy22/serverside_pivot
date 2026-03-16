import React, { useState } from 'react';
import Icons from '../Icons';
import { getStyles } from '../../utils/styles';

const DateRangeFilter = ({ onFilter, currentFilter, theme }) => {
    const styles = getStyles(theme || { primary: '#1976d2', border: '#e0e0e0', headerBg: '#f5f5f5', text: '#212121' });
    
    const isMulti = currentFilter && currentFilter.conditions;
    const [operator, setOperator] = useState(isMulti ? currentFilter.operator : 'AND');
    const [conditions, setConditions] = useState(() => {
        if (isMulti) return currentFilter.conditions;
        if (currentFilter && currentFilter.value && !currentFilter.conditions) {
             // Handle legacy single value or specialized filter structure
             return [{ type: 'eq', value: currentFilter.value }];
        }
        return [{ type: 'between', value: '', value2: '' }];
    });

    const updateCondition = (index, key, value) => {
        const newConditions = [...conditions];
        newConditions[index][key] = value;
        setConditions(newConditions);
    };

    const addCondition = () => {
        setConditions([...conditions, { type: 'between', value: '', value2: '' }]);
    };

    const removeCondition = (index) => {
        const newConditions = conditions.filter((_, i) => i !== index);
        setConditions(newConditions);
    };

    const apply = () => {
        const validConditions = conditions.filter(c => {
            if (c.type === 'between') return c.value && c.value2;
            return c.value;
        });
        
        if (validConditions.length > 0) {
            onFilter({ operator, conditions: validConditions });
        } else {
            onFilter(null);
        }
    };

    const setPreset = (days) => {
        const e = new Date();
        const s = new Date();
        s.setDate(e.getDate() - days);
        const fmt = d => d.toISOString().split('T')[0];
        // For presets, we reset to a single condition
        setConditions([{ type: 'between', value: fmt(s), value2: fmt(e) }]);
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
            <div style={{display: 'flex', justifyContent: 'flex-end', gap: '4px'}}>
                <button onClick={() => setOperator('AND')} style={{...styles.btn, padding: '2px 6px', fontSize: '10px', background: operator === 'AND' ? theme.primary : '#eee', color: operator === 'AND' ? '#fff' : '#333'}}>AND</button>
                <button onClick={() => setOperator('OR')} style={{...styles.btn, padding: '2px 6px', fontSize: '10px', background: operator === 'OR' ? theme.primary : '#eee', color: operator === 'OR' ? '#fff' : '#333'}}>OR</button>
            </div>

            <div style={{display: 'flex', gap: '6px', flexWrap: 'wrap'}}>
                <button onClick={() => setPreset(0)} style={{...styles.btn, fontSize:'11px', padding:'4px 8px'}}>Today</button>
                <button onClick={() => setPreset(7)} style={{...styles.btn, fontSize:'11px', padding:'4px 8px'}}>Last 7d</button>
                <button onClick={() => setPreset(30)} style={{...styles.btn, fontSize:'11px', padding:'4px 8px'}}>Last 30d</button>
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
                <button onClick={addCondition} style={{...styles.btn, flex: 1, justifyContent: 'center', background: '#f5f5f5', fontSize: '11px'}}>Add</button>
                <button onClick={apply} style={{...styles.btn, flex: 1, justifyContent: 'center', background: theme.primary, color: '#fff', fontSize: '11px'}}>Apply</button>
            </div>
        </div>
    );
};

export default DateRangeFilter;
