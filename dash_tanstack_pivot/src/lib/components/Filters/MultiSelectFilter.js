import React, { useState, useEffect, useMemo } from 'react';

const LOCAL_RENDER_LIMIT = 250;

const MultiSelectFilter = ({ options = [], optionMeta = null, onSearchOptions, onLoadMoreOptions, onFilter, currentFilter, onClose, theme }) => {
    const [search, setSearch] = useState('');
    const [selected, setSelected] = useState(new Set());
    const [manualValue, setManualValue] = useState('');
    const remoteOptions = typeof onSearchOptions === 'function';

    useEffect(() => {
        // Initialize from current filter if it's an 'in' type
        if (currentFilter && currentFilter.conditions && currentFilter.conditions[0] && currentFilter.conditions[0].type === 'in') {
            setSelected(new Set(currentFilter.conditions[0].value));
        } else {
            setSelected(new Set());
        }
    }, [currentFilter]);

    useEffect(() => {
        if (!remoteOptions) return undefined;
        const timeout = setTimeout(() => {
            onSearchOptions(search);
        }, 250);
        return () => clearTimeout(timeout);
    }, [onSearchOptions, remoteOptions, search]);

    const filteredOptions = useMemo(() => {
        const baseOptions = Array.isArray(options) ? options : [];
        const searched = remoteOptions
            ? baseOptions
            : baseOptions.filter(o => String(o).toLowerCase().includes(search.toLowerCase()));
        const merged = [];
        const seen = new Set();
        selected.forEach((value) => {
            const key = `${typeof value}:${String(value)}`;
            if (seen.has(key)) return;
            seen.add(key);
            merged.push(value);
        });
        searched.forEach((value) => {
            const key = `${typeof value}:${String(value)}`;
            if (seen.has(key)) return;
            seen.add(key);
            merged.push(value);
        });
        return merged;
    }, [options, remoteOptions, search, selected]);

    const visibleOptions = filteredOptions.slice(0, LOCAL_RENDER_LIMIT);
    const localHasMore = !remoteOptions && filteredOptions.length > visibleOptions.length;
    const remoteHasMore = remoteOptions && optionMeta && optionMeta.hasMore === true;
    const loading = remoteOptions && optionMeta && optionMeta.loading === true;
    const loadedCount = Array.isArray(options) ? options.length : 0;
    const totalCount = optionMeta && Number.isFinite(Number(optionMeta.total)) ? Number(optionMeta.total) : filteredOptions.length;

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
                placeholder={remoteOptions ? "Search values..." : "Search loaded values..."} 
                value={search} 
                onChange={e => setSearch(e.target.value)} 
                style={{border:'1px solid #ddd', borderRadius:'4px', padding:'4px', fontSize:'11px'}} 
            />
            <div style={{fontSize:'10px', color:'#777'}}>
                {remoteOptions
                    ? `${loading ? 'Loading' : 'Loaded'} ${loadedCount}${Number.isFinite(totalCount) ? ` of ${totalCount}` : ''} values`
                    : `${filteredOptions.length} matching value${filteredOptions.length === 1 ? '' : 's'}`}
            </div>
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
                {visibleOptions.length === 0 ? <div style={{padding:'8px', color:'#999', fontSize:'11px'}}>{loading ? 'Loading options...' : 'No options...'}</div> :
                visibleOptions.map((opt, i) => (
                    <div key={i} onClick={() => toggle(opt)} style={{display:'flex', gap:'6px', padding:'4px 8px', cursor:'pointer', alignItems:'center', background: selected.has(opt) ? '#e3f2fd' : 'transparent'}}>
                        <input type="checkbox" checked={selected.has(opt)} readOnly style={{margin:0}} />
                        <span style={{fontSize:'11px'}}>{opt}</span>
                    </div>
                ))}
                {localHasMore && (
                    <div style={{padding:'8px', color:'#777', fontSize:'11px', borderTop:'1px solid #f0f0f0'}}>
                        Showing first {LOCAL_RENDER_LIMIT}. Search to narrow the list.
                    </div>
                )}
                {remoteHasMore && (
                    <button
                        type="button"
                        disabled={loading}
                        onClick={() => {
                            if (typeof onLoadMoreOptions === 'function') {
                                onLoadMoreOptions(search, loadedCount);
                            }
                        }}
                        style={{width:'100%', padding:'7px 8px', border:'none', borderTop:'1px solid #f0f0f0', background:'#fafafa', cursor:loading?'default':'pointer', fontSize:'11px', color:(theme && theme.primary) || '#1976d2'}}
                    >
                        {loading ? 'Loading...' : 'Load more'}
                    </button>
                )}
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
