import React, { useState, useEffect } from 'react';

const MultiSelectFilter = ({ options = [], onFilter, currentFilter }) => {
    const [search, setSearch] = useState('');
    const [selected, setSelected] = useState(new Set());

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
        
        if (newSet.size > 0) {
            onFilter({ operator: 'AND', conditions: [{ type: 'in', value: Array.from(newSet) }] });
        } else {
            onFilter(null);
        }
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px', maxHeight: '200px'}}>
            <input 
                placeholder="Search..." 
                value={search} 
                onChange={e => setSearch(e.target.value)} 
                style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', fontSize:'11px'}} 
            />
            <div style={{overflowY: 'auto', flex: 1, border: '1px solid #f0f0f0', borderRadius:'4px'}}>
                {filteredOptions.length === 0 ? <div style={{padding:'8px', color:'#999', fontSize:'11px'}}>No options...</div> :
                filteredOptions.map((opt, i) => (
                    <div key={i} onClick={() => toggle(opt)} style={{display:'flex', gap:'6px', padding:'4px 8px', cursor:'pointer', alignItems:'center', background: selected.has(opt) ? '#e3f2fd' : 'transparent'}}>
                        <input type="checkbox" checked={selected.has(opt)} readOnly style={{margin:0}} />
                        <span style={{fontSize:'11px'}}>{opt}</span>
                    </div>
                ))}
            </div>
        </div>
    );
};

export default MultiSelectFilter;
