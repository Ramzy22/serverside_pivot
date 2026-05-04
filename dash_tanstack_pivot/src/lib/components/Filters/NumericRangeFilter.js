import React, { useEffect, useState } from 'react';

const getRangeDraft = (filter) => {
    const condition = filter && filter.conditions && filter.conditions[0];
    if (!condition) return { min: '', max: '' };

    if (condition.type === 'between') {
        if (Array.isArray(condition.value)) {
            return {
                min: condition.value[0] !== undefined && condition.value[0] !== null ? String(condition.value[0]) : '',
                max: condition.value[1] !== undefined && condition.value[1] !== null ? String(condition.value[1]) : '',
            };
        }
        return {
            min: condition.value !== undefined && condition.value !== null ? String(condition.value) : '',
            max: condition.value2 !== undefined && condition.value2 !== null ? String(condition.value2) : '',
        };
    }

    if (condition.type === 'gte' || condition.type === 'gt') {
        return { min: condition.value !== undefined && condition.value !== null ? String(condition.value) : '', max: '' };
    }
    if (condition.type === 'lte' || condition.type === 'lt') {
        return { min: '', max: condition.value !== undefined && condition.value !== null ? String(condition.value) : '' };
    }

    return {
        min: condition.value !== undefined && condition.value !== null ? String(condition.value) : '',
        max: condition.value2 !== undefined && condition.value2 !== null ? String(condition.value2) : '',
    };
};

const parseNumericDraft = (value) => {
    const trimmed = value === null || value === undefined ? '' : String(value).trim();
    if (!trimmed) return { empty: true, value: null };
    const numericValue = Number(trimmed);
    if (!Number.isFinite(numericValue)) return null;
    return { empty: false, value: numericValue };
};

const NumericRangeFilter = ({ onFilter, currentFilter, theme, onClose }) => {
    const resolvedTheme = theme || { primary: '#1976d2', danger: '#d32f2f', textSec: '#666' };
    const initialDraft = getRangeDraft(currentFilter);
    const [min, setMin] = useState(initialDraft.min);
    const [max, setMax] = useState(initialDraft.max);
    const [error, setError] = useState(null);

    useEffect(() => {
        const nextDraft = getRangeDraft(currentFilter);
        setMin(nextDraft.min);
        setMax(nextDraft.max);
        setError(null);
    }, [currentFilter]);

    const apply = () => {
        const parsedMin = parseNumericDraft(min);
        const parsedMax = parseNumericDraft(max);
        if (!parsedMin || !parsedMax) {
            setError('Enter valid numeric values.');
            return;
        }
        if (!parsedMin.empty && !parsedMax.empty && parsedMin.value > parsedMax.value) {
            setError('Min must be less than or equal to max.');
            return;
        }

        setError(null);
        if (!parsedMin.empty && !parsedMax.empty) {
            onFilter({ operator: 'AND', conditions: [{ type: 'between', value: parsedMin.value, value2: parsedMax.value }] });
        } else if (!parsedMin.empty) {
            onFilter({ operator: 'AND', conditions: [{ type: 'gte', value: parsedMin.value }] });
        } else if (!parsedMax.empty) {
            onFilter({ operator: 'AND', conditions: [{ type: 'lte', value: parsedMax.value }] });
        } else {
            onFilter(null);
        }
        if (onClose) onClose();
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
             <div style={{display: 'flex', gap: '8px', alignItems: 'center'}}>
                <input type="number" placeholder="Min" value={min} onChange={e => { setMin(e.target.value); setError(null); }} onKeyDown={e => { if (e.key === 'Enter') apply(); }} style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', width: '80px'}} />
                <div style={{flex:1, height:'2px', background:'#eee', position:'relative'}}>
                     <div style={{position:'absolute', left:'0', right:'0', top:'-1px', height:'4px', background: resolvedTheme.primary, opacity: 0.3}} />
                </div>
                <input type="number" placeholder="Max" value={max} onChange={e => { setMax(e.target.value); setError(null); }} onKeyDown={e => { if (e.key === 'Enter') apply(); }} style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', width: '80px'}} />
            </div>
            {error && <div style={{fontSize: '11px', color: resolvedTheme.danger || '#d32f2f'}}>{error}</div>}
            <button type="button" onClick={apply} style={{border:'none', borderRadius:'4px', background: resolvedTheme.primary, color:'#fff', cursor:'pointer', padding:'5px 10px', fontSize:'11px', alignSelf:'flex-end'}}>Apply</button>
        </div>
    );
};

export default NumericRangeFilter;
