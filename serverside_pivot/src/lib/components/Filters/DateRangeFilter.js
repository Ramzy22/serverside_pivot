import React, { useEffect, useState } from 'react';
import Icons from '../../utils/Icons';
import { getStyles } from '../../utils/styles';

/** @returns {any} */
const createDefaultDateCondition = () => ({ type: 'between', value: '', value2: '' });

/** @param {any} [filter] @returns {any} */
const cloneDateConditions = (filter) => {
    if (filter && Array.isArray(filter.conditions)) {
        return filter.conditions.map(/** @param {any} [condition] @returns {any} */ condition => ({ ...condition }));
    }
    if (filter && filter.value && !filter.conditions) {
        // Handle legacy single value or specialized filter structure
        return [{ type: 'eq', value: filter.value }];
    }
    return [createDefaultDateCondition()];
};

/** @param {any} [date] @returns {any} */
const formatLocalDate = (date) => {
    /** @type {any} */
    const year = date.getFullYear();
    /** @type {any} */
    const month = String(date.getMonth() + 1).padStart(2, '0');
    /** @type {any} */
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
};

/** @param {any} p0 @returns {any} */
const DateRangeFilter = ({ onFilter, currentFilter, theme, onClose }) => {
    /** @type {any} */
    const resolvedTheme = theme || { primary: '#1976d2', border: '#e0e0e0', headerBg: '#f5f5f5', text: '#212121' };
    /** @type {any} */
    const styles = getStyles(resolvedTheme);
    
    /** @type {any} */
    const isMulti = currentFilter && currentFilter.conditions;
    /** @type {[any, (...args: any[]) => any]} */
    const [operator, setOperator] = useState(isMulti ? (currentFilter.operator || 'AND') : 'AND');
    /** @type {[any, (...args: any[]) => any]} */
    const [conditions, setConditions] = useState(() => cloneDateConditions(currentFilter));

    useEffect(() => {
        /** @type {any} */
        const nextIsMulti = currentFilter && currentFilter.conditions;
        setOperator(nextIsMulti ? (currentFilter.operator || 'AND') : 'AND');
        setConditions(cloneDateConditions(currentFilter));
    }, [currentFilter]);

    /** @param {any} [index] @param {any} [key] @param {any} [value] @returns {any} */
    const updateCondition = (index, key, value) => {
        setConditions(/** @param {any} [previousConditions] @returns {any} */ previousConditions => previousConditions.map(/** @param {any} [condition] @param {any} [conditionIndex] @returns {any} */ (condition, conditionIndex) => (
            conditionIndex === index ? { ...condition, [key]: value } : condition
        )));
    };

    /** @returns {any} */
    const addCondition = () => {
        setConditions(/** @param {any} [previousConditions] @returns {any} */ previousConditions => [...previousConditions, createDefaultDateCondition()]);
    };

    /** @param {any} [index] @returns {any} */
    const removeCondition = (index) => {
        setConditions(/** @param {any} [previousConditions] @returns {any} */ previousConditions => previousConditions.filter(/** @param {any} [_] @param {any} [i] @returns {any} */ (_, i) => i !== index));
    };

    /** @returns {any} */
    const apply = () => {
        /** @type {any} */
        const validConditions = conditions.filter(/** @param {any} [c] @returns {any} */ c => {
            if (c.type === 'between') return c.value && c.value2;
            return c.value;
        }).map(/** @param {any} [condition] @returns {any} */ condition => ({ ...condition }));
        
        if (validConditions.length > 0) {
            onFilter({ operator, conditions: validConditions });
        } else {
            onFilter(null);
        }
        if (onClose) onClose();
    };

    /** @param {any} [days] @returns {any} */
    const setPreset = (days) => {
        /** @type {any} */
        const e = new Date();
        /** @type {any} */
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
                {conditions.map(/** @param {any} [cond] @param {any} [index] @returns {any} */ (cond, index) => (
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
