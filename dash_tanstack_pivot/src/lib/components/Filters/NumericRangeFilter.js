import React, { useState } from 'react';

const NumericRangeFilter = ({ onFilter, currentFilter, theme }) => {
    const [min, setMin] = useState((currentFilter && currentFilter.conditions && currentFilter.conditions[0]) ? currentFilter.conditions[0].value : '');
    const [max, setMax] = useState((currentFilter && currentFilter.conditions && currentFilter.conditions[0]) ? currentFilter.conditions[0].value2 : '');

    const apply = (mn, mx) => {
        if (mn !== '' && mx !== '') {
            onFilter({ operator: 'AND', conditions: [{ type: 'between', value: Number(mn), value2: Number(mx) }] });
        } else if (mn !== '') {
            onFilter({ operator: 'AND', conditions: [{ type: 'gte', value: Number(mn) }] });
        } else if (mx !== '') {
            onFilter({ operator: 'AND', conditions: [{ type: 'lte', value: Number(mx) }] });
        } else {
            onFilter(null);
        }
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
             <div style={{display: 'flex', gap: '8px', alignItems: 'center'}}>
                <input type="number" placeholder="Min" value={min} onChange={e => { setMin(e.target.value); apply(e.target.value, max); }} style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', width: '80px'}} />
                <div style={{flex:1, height:'2px', background:'#eee', position:'relative'}}>
                     <div style={{position:'absolute', left:'0', right:'0', top:'-1px', height:'4px', background: theme.primary, opacity: 0.3}} />
                </div>
                <input type="number" placeholder="Max" value={max} onChange={e => { setMax(e.target.value); apply(min, e.target.value); }} style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', width: '80px'}} />
            </div>
        </div>
    );
};

export default NumericRangeFilter;
