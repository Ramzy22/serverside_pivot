import React, { useState, useEffect } from 'react';

const MultiSelectFilter = ({ options = [], onFilter, currentFilter, onClose, theme }) => {
    const [search, setSearch] = useState('');
    const [selected, setSelected] = useState(new Set());
    const [manualValue, setManualValue] = useState('');

    useEffect(() => {
        // Initialize from current filter if it's an 'in' type
        if (currentFilter && currentFilter.conditions && currentFilter.conditions[0] && currentFilter.conditions[0].type === 'in') {
            setSelected(new Set(currentFilter.conditions[0].value));
        }
    }, [currentFilter]);

    const filteredOptions = options.filter(o => String(o).toLowerCase().includes(search.toLowerCase()));

    const toggle = (val) => {
        const newSet = new Set(selected);
        if (newSet.has(val)) newSet.delete(val);
        else newSet.add(val);
        setSelected(newSet);
    };

    const addManualValue = () => {
        const value = manualValue.trim();
        if (!value) return;
        const newSet = new Set(selected);
        newSet.add(value);
        setSelected(newSet);
        setManualValue('');
    };

    const apply = () => {
        if (selected.size > 0) {
            onFilter({ operator: 'AND', conditions: [{ type: 'in', value: Array.from(selected) }] });
        } else {
            onFilter(null);
        }
        if (onClose) onClose();
    };

    const clear = () => {
        setSelected(new Set());
        onFilter(null);
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px', maxHeight: '200px'}}>
            <input 
                placeholder="Search..." 
                value={search} 
                onChange={e => setSearch(e.target.value)} 
                style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', fontSize:'11px'}} 
            />
            <div style={{display:'flex', gap:'6px'}}>
                <input
                    placeholder="Add value..."
                    value={manualValue}
                    onChange={e => setManualValue(e.target.value)}
                    onKeyDown={e => {
                        if (e.key === 'Enter') {
                            e.preventDefault();
                            addManualValue();
                        }
                    }}
                    style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', fontSize:'11px', flex:1}}
                />
                <button
                    onClick={addManualValue}
                    style={{padding:'4px 8px', fontSize:'11px', border:'1px solid #ddd', background:'#f5f5f5', borderRadius:'4px', cursor:'pointer'}}
                >
                    Add
                </button>
            </div>
            <div style={{overflowY: 'auto', flex: 1, border: '1px solid #f0f0f0', borderRadius:'4px'}}>
                {filteredOptions.length === 0 ? <div style={{padding:'8px', color:'#999', fontSize:'11px'}}>No options...</div> :
                filteredOptions.map((opt, i) => (
                    <div key={i} onClick={() => toggle(opt)} style={{display:'flex', gap:'6px', padding:'4px 8px', cursor:'pointer', alignItems:'center', background: selected.has(opt) ? '#e3f2fd' : 'transparent'}}>
                        <input type="checkbox" checked={selected.has(opt)} readOnly style={{margin:0}} />
                        <span style={{fontSize:'11px'}}>{opt}</span>
                    </div>
                ))}
            </div>
            <div style={{display:'flex', justifyContent:'space-between', gap:'8px'}}>
                <button
                    onClick={clear}
                    style={{padding:'4px 8px', border:'none', background:'none', cursor:'pointer', color:'#d32f2f', fontSize:'11px'}}
                >
                    Clear
                </button>
                <div style={{display:'flex', gap:'8px'}}>
                    {onClose && (
                        <button
                            onClick={onClose}
                            style={{padding:'4px 8px', border:'none', background:'none', cursor:'pointer', fontSize:'11px'}}
                        >
                            Close
                        </button>
                    )}
                    <button
                        onClick={apply}
                        style={{padding:'4px 12px', background:(theme && theme.primary) || '#1976d2', color:'#fff', border:'none', borderRadius:'2px', cursor:'pointer', fontSize:'11px'}}
                    >
                        Apply
                    </button>
                </div>
            </div>
        </div>
    );
};

export default MultiSelectFilter;
