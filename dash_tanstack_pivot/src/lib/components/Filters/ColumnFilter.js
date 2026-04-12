import React, { useState, useEffect } from 'react';
import Icons from '../../utils/Icons';
import { getStyles } from '../../utils/styles';
import DateRangeFilter from './DateRangeFilter';
import NumericRangeFilter from './NumericRangeFilter';
import MultiSelectFilter from './MultiSelectFilter';

const ColumnFilter = ({ column, onFilter, currentFilter, options = [], theme, onClose }) => {
    const styles = getStyles(theme || { primary: '#1976d2', border: '#e0e0e0', headerBg: '#f5f5f5', text: '#212121' });
    const isLeaf = column.getLeafColumns
        ? column.getLeafColumns().length === 1
        : !column.columns;

    if (!isLeaf) {
        return (
            <div style={{ padding: '12px', fontSize: '12px', background: theme.background, color: theme.text }}>
                Filters are available only for value columns.
            </div>
        );
    }

    const leaf = column.getLeafColumns ? column.getLeafColumns()[0] : column;
    const colId = leaf.id.toLowerCase();
    const isDate = (leaf.columnDef && leaf.columnDef.meta && leaf.columnDef.meta.type === 'date') || colId.includes('date') || colId.includes('time');
    const isNumeric = (leaf.columnDef && leaf.columnDef.meta && leaf.columnDef.meta.type === 'number') || colId.includes('sales') || colId.includes('cost') || colId.includes('amount') || colId.includes('price');

    const [tab, setTab] = useState('condition');

    // Auto-select tab based on available options
    useEffect(() => {
        if (tab === 'condition' && options && options.length > 0) {
            setTab('values');
        } else if (isDate && tab === 'condition') {
             setTab('date');
        }
    }, [options, isDate]);

    // --- Existing Condition Logic ---
    const isMulti = currentFilter && currentFilter.conditions;
    const [operator, setOperator] = useState(isMulti ? currentFilter.operator : 'AND');
    const [conditions, setConditions] = useState(
        isMulti ? currentFilter.conditions : [{type: 'contains', value: '', caseSensitive: false}]
    );

    const updateCondition = (index, key, value) => {
        const newConditions = [...conditions];
        newConditions[index][key] = value;
        setConditions(newConditions);
    };
    
    const addCondition = () => {
        setConditions([...conditions, {type: 'contains', value: '', caseSensitive: false}]);
    };

    const removeCondition = (index) => {
        const newConditions = conditions.filter((_, i) => i !== index);
        setConditions(newConditions);
    };

    const handleApply = () => {
        const validConditions = conditions.filter(c => {
             if (c.type === 'between') return c.value && c.value2;
             return String(c.value).trim() !== '';
        });
        
        const newFilter = {
            operator: operator,
            conditions: validConditions.map(c => {
                 let finalVal = c.value;
                 let finalVal2 = c.value2;
                 
                 if (isNumeric) {
                     if (finalVal !== '' && !isNaN(Number(finalVal))) finalVal = Number(finalVal);
                     if (finalVal2 !== '' && !isNaN(Number(finalVal2))) finalVal2 = Number(finalVal2);
                 }

                 if (c.type === 'between') {
                      return { ...c, value: [finalVal, finalVal2] };
                 }
                 return { ...c, value: finalVal };
            })
        };
        
        if (newFilter.conditions.length > 0) {
            onFilter(newFilter);
        } else {
            onFilter(null);
        }
        if (onClose) onClose();
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px', color: '#333'}}>
            <div style={{fontWeight: 600, fontSize: '12px', borderBottom: '1px solid #eee', paddingBottom: '8px', display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                <span>Filter: {column.header}</span>
                <div style={{display: 'flex', background: '#f5f5f5', borderRadius: '4px', padding: '2px'}}>
                    <div onClick={() => setTab('condition')} style={{padding:'2px 8px', fontSize:'10px', cursor:'pointer', borderRadius:'3px', background: tab==='condition'?'#fff':'transparent', boxShadow: tab==='condition'?'0 1px 2px rgba(0,0,0,0.1)':'none'}}>Rules</div>
                    <div onClick={() => setTab('values')} style={{padding:'2px 8px', fontSize:'10px', cursor:'pointer', borderRadius:'3px', background: tab==='values'?'#fff':'transparent', boxShadow: tab==='values'?'0 1px 2px rgba(0,0,0,0.1)':'none'}}>List</div>
                    {isDate && <div onClick={() => setTab('date')} style={{padding:'2px 8px', fontSize:'10px', cursor:'pointer', borderRadius:'3px', background: tab==='date'?'#fff':'transparent', boxShadow: tab==='date'?'0 1px 2px rgba(0,0,0,0.1)':'none'}}>Date</div>}
                    {isNumeric && <div onClick={() => setTab('numeric')} style={{padding:'2px 8px', fontSize:'10px', cursor:'pointer', borderRadius:'3px', background: tab==='numeric'?'#fff':'transparent', boxShadow: tab==='numeric'?'0 1px 2px rgba(0,0,0,0.1)':'none'}}>Range</div>}
                </div>
            </div>
            
            <div style={{maxHeight: '300px', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '8px'}}>
                {tab === 'values' && (
                    <MultiSelectFilter options={options} onFilter={onFilter} currentFilter={currentFilter} onClose={onClose} theme={theme} />
                )}

                {tab === 'date' && (
                    <DateRangeFilter onFilter={onFilter} currentFilter={currentFilter} theme={theme} />
                )}

                {tab === 'numeric' && (
                    <NumericRangeFilter onFilter={onFilter} currentFilter={currentFilter} theme={theme} />
                )}

                {tab === 'condition' && (
                    <>
                        <div style={{display: 'flex', justifyContent: 'flex-end', gap: '4px', marginBottom: '4px'}}>
                            <button onClick={() => setOperator('AND')} style={{padding: '2px 6px', fontSize: '10px', background: operator === 'AND' ? theme.primary: '#eee', color: operator === 'AND' ? '#fff' : '#333', border: 'none', borderRadius: '2px'}}>AND</button>
                            <button onClick={() => setOperator('OR')} style={{padding: '2px 6px', fontSize: '10px', background: operator === 'OR' ? theme.primary: '#eee', color: operator === 'OR' ? '#fff' : '#333', border: 'none', borderRadius: '2px'}}>OR</button>
                        </div>
                        <div style={{display: 'flex', flexDirection: 'column', gap: '12px', paddingRight: '8px'}}>
                        {conditions.map((cond, index) => (
                            <div key={index} style={{display: 'flex', flexDirection: 'column', gap: '4px', border: '1px solid #f0f0f0', padding: '8px', borderRadius: '4px'}}>
                                <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                                    <label style={{fontSize:'11px', color: '#666'}}>Condition {index + 1}</label>
                                    {conditions.length > 1 && <span onClick={() => removeCondition(index)} style={{cursor: 'pointer'}}><Icons.Close/></span>}
                                </div>
                                <select value={cond.type} onChange={e => updateCondition(index, 'type', e.target.value)} style={{padding:'4px', borderRadius:'2px', border:'1px solid #ddd'}}>
                                    <option value="eq">Equals</option>
                                    <option value="ne">Not Equals</option>
                                    <option value="contains">Contains</option>
                                    <option value="startsWith">Starts With</option>
                                    <option value="endsWith">Ends With</option>
                                    <option value="gt">Greater Than</option>
                                    <option value="lt">Less Than</option>
                                    <option value="between">Between (Range)</option>
                                    <option value="in">In List</option>
                                </select>
                                
                                {cond.type === 'between' ? (
                                    <div style={{display: 'flex', gap: '4px'}}>
                                        <input 
                                            placeholder="Start" 
                                            value={cond.value} 
                                            onChange={e => updateCondition(index, 'value', e.target.value)} 
                                            style={{padding:'6px', borderRadius:'2px', border:'1px solid #ddd', fontSize: '13px', width: '50%'}}
                                        />
                                        <input 
                                            placeholder="End" 
                                            value={cond.value2 || ''} 
                                            onChange={e => updateCondition(index, 'value2', e.target.value)} 
                                            style={{padding:'6px', borderRadius:'2px', border:'1px solid #ddd', fontSize: '13px', width: '50%'}}
                                        />
                                    </div>
                                ) : (
                                    <input 
                                        placeholder="Value..." 
                                        value={cond.value} 
                                        onChange={e => updateCondition(index, 'value', e.target.value)} 
                                        style={{padding:'6px', borderRadius:'2px', border:'1px solid #ddd', fontSize: '13px'}}
                                    />
                                )}
                                
                                <div style={{display:'flex', alignItems:'center', gap:'4px', marginTop:'2px'}}>
                                    <input 
                                        type="checkbox" 
                                        checked={cond.caseSensitive || false} 
                                        onChange={e => updateCondition(index, 'caseSensitive', e.target.checked)}
                                        id={`cs-${index}`}
                                    />
                                    <label htmlFor={`cs-${index}`} style={{fontSize:'11px', color:'#555', cursor:'pointer'}}>Match Case</label>
                                </div>
                            </div>
                        ))}
                        </div>
                        <button onClick={addCondition} style={{...styles.btn, justifyContent: 'center', background: '#f5f5f5'}}>Add Condition</button>
                    </>
                )}
            </div>

            <div style={{display:'flex', justifyContent: 'space-between', gap: '8px', marginTop: '8px', borderTop: '1px solid #eee', paddingTop: '8px'}}>
                <button onClick={() => { onFilter(null); if(onClose) onClose(); }} style={{padding: '4px 8px', border:'none', background:'none', cursor:'pointer', color: '#d32f2f', fontSize: '11px'}}>Clear & Close</button>
                <div style={{display: 'flex', gap: '8px'}}>
                    {onClose && <button onClick={onClose} style={{padding: '4px 8px', border:'none', background:'none', cursor:'pointer', fontSize: '11px'}}>Close</button>}
                    {tab === 'condition' && (
                        <button onClick={handleApply} style={{padding: '4px 12px', background: theme.primary, color: '#fff', border:'none', borderRadius: '2px', cursor:'pointer', fontSize: '11px'}}>
                            Apply
                        </button>
                    )}
                </div>
            </div>
        </div>
    );
};

export default ColumnFilter;
