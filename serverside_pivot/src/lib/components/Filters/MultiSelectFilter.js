import React, { useState, useEffect, useMemo } from 'react';
import { filterButtonStyle, filterCardStyle, filterHintStyle, filterInputStyle } from './filterStyles';

const LOCAL_RENDER_LIMIT = 250;

/** @param {any} [text] @returns {any} */
const parseFilterListText = (text) => String(text || '')
    .split(/[\r\n\t,;]+/)
    .map(value => value.trim())
    .filter(Boolean);

/**
 * The List tab of a column filter: pick values from the column's members.
 * The owning ColumnFilter renders the footer; this editor registers how to
 * build its filter in `applyRef` and calls `onSubmit` on Enter in the search.
 * @param {any} p0 @returns {any}
 */
const MultiSelectFilter = ({ options = [], optionMeta = null, onSearchOptions, onLoadMoreOptions, currentFilter, applyRef, onSubmit, theme }) => {
    /** @type {[any, (...args: any[]) => any]} */
    const [search, setSearch] = useState('');
    /** @type {[any, (...args: any[]) => any]} */
    const [selected, setSelected] = useState(new Set());
    /** @type {[any, (...args: any[]) => any]} */
    const [manualValue, setManualValue] = useState('');
    /** @type {any} */
    const remoteOptions = typeof onSearchOptions === 'function';

    useEffect(() => {
        // Initialize from current filter if it's an 'in' type
        if (currentFilter && currentFilter.conditions && currentFilter.conditions[0] && currentFilter.conditions[0].type === 'in') {
            const currentValue = currentFilter.conditions[0].value;
            setSelected(new Set(Array.isArray(currentValue) ? currentValue : parseFilterListText(currentValue)));
        } else {
            setSelected(new Set());
        }
    }, [currentFilter]);

    useEffect(() => {
        if (!remoteOptions) return undefined;
        /** @type {any} */
        const timeout = setTimeout(() => {
            onSearchOptions(search);
        }, 250);
        return () => clearTimeout(timeout);
    }, [onSearchOptions, remoteOptions, search]);

    /** @type {any} */
    const filteredOptions = useMemo(() => {
        /** @type {any} */
        const baseOptions = Array.isArray(options) ? options : [];
        /** @type {any} */
        const searched = remoteOptions
            ? baseOptions
            : baseOptions.filter(/** @param {any} [o] @returns {any} */ o => String(o).toLowerCase().includes(search.toLowerCase()));
        /** @type {any[]} */
        const merged = [];
        /** @type {any} */
        const seen = new Set();
        selected.forEach(/** @param {any} [value] @returns {any} */ (value) => {
            const key = `${typeof value}:${String(value)}`;
            if (seen.has(key)) return;
            seen.add(key);
            merged.push(value);
        });
        searched.forEach(/** @param {any} [value] @returns {any} */ (value) => {
            const key = `${typeof value}:${String(value)}`;
            if (seen.has(key)) return;
            seen.add(key);
            merged.push(value);
        });
        return merged;
    }, [options, remoteOptions, search, selected]);

    /** @type {any} */
    const visibleOptions = filteredOptions.slice(0, LOCAL_RENDER_LIMIT);
    /** @type {any} */
    const localHasMore = !remoteOptions && filteredOptions.length > visibleOptions.length;
    /** @type {any} */
    const remoteHasMore = remoteOptions && optionMeta && optionMeta.hasMore === true;
    /** @type {any} */
    const loading = remoteOptions && optionMeta && optionMeta.loading === true;
    /** @type {any} */
    const loadedCount = Array.isArray(options) ? options.length : 0;
    /** @type {any} */
    const totalCount = optionMeta && Number.isFinite(Number(optionMeta.total)) ? Number(optionMeta.total) : filteredOptions.length;

    if (applyRef) {
        applyRef.current = () => ({
            filter: selected.size > 0
                ? { operator: 'AND', conditions: [{ type: 'in', value: Array.from(selected) }] }
                : null,
        });
    }

    /** @param {any} [val] @returns {any} */
    const toggle = (val) => {
        /** @type {any} */
        const newSet = new Set(selected);
        if (newSet.has(val)) newSet.delete(val);
        else newSet.add(val);
        setSelected(newSet);
    };

    /** @param {any} [values] @returns {any} */
    const addManualValues = (values) => {
        /** @type {any} */
        const parsedValues = Array.isArray(values)
            ? values.map(value => String(value || '').trim()).filter(Boolean)
            : parseFilterListText(values);
        if (parsedValues.length === 0) return;
        /** @type {any} */
        const newSet = new Set(selected);
        parsedValues.forEach(/** @param {any} [value] @returns {any} */ value => newSet.add(value));
        setSelected(newSet);
        setManualValue('');
    };

    /** @returns {any} */
    const addManualValue = () => {
        addManualValues(manualValue);
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px', minHeight: 0}}>
            <input
                placeholder={remoteOptions ? "Search values..." : "Search loaded values..."}
                aria-label="Search values"
                value={search}
                onChange={e => setSearch(e.target.value)}
                onKeyDown={e => {
                    if (e.key === 'Enter' && typeof onSubmit === 'function') {
                        e.preventDefault();
                        onSubmit();
                    }
                }}
                style={filterInputStyle(theme)}
            />
            <div style={filterHintStyle(theme)}>
                {remoteOptions
                    ? `${loading ? 'Loading' : 'Loaded'} ${loadedCount}${Number.isFinite(totalCount) ? ` of ${totalCount}` : ''} values`
                    : `${filteredOptions.length} matching value${filteredOptions.length === 1 ? '' : 's'}`}
                {selected.size > 0 ? ` · ${selected.size} selected` : ''}
            </div>
            <div style={{display:'flex', gap:'6px'}}>
                <input
                    placeholder="Add or paste values..."
                    aria-label="Add or paste values"
                    value={manualValue}
                    onChange={e => setManualValue(e.target.value)}
                    onPaste={e => {
                        /** @type {any} */
                        const pastedText = e.clipboardData && e.clipboardData.getData('text');
                        /** @type {any} */
                        const pastedValues = parseFilterListText(pastedText);
                        if (pastedValues.length <= 1) return;
                        e.preventDefault();
                        addManualValues(pastedValues);
                    }}
                    onKeyDown={e => {
                        if (e.key === 'Enter') {
                            e.preventDefault();
                            addManualValue();
                        }
                    }}
                    style={{ ...filterInputStyle(theme), flex: 1 }}
                />
                <button
                    type="button"
                    className="sp-btn sp-btn-subtle"
                    onClick={addManualValue}
                    disabled={!manualValue.trim()}
                    style={{ ...filterButtonStyle(theme, 'subtle'), opacity: manualValue.trim() ? 1 : 0.55, cursor: manualValue.trim() ? 'pointer' : 'default' }}
                >
                    Add
                </button>
            </div>
            <div role="listbox" aria-multiselectable="true" aria-label="Values" style={{ ...filterCardStyle(theme), overflowY: 'auto', flex: 1, minHeight: '96px', maxHeight: '220px' }}>
                {visibleOptions.length === 0 ? <div style={{ ...filterHintStyle(theme), padding:'8px' }}>{loading ? 'Loading options...' : 'No options...'}</div> :
                visibleOptions.map(/** @param {any} [opt] @param {any} [i] @returns {any} */ (opt, i) => (
                    <label
                        key={i}
                        role="option"
                        aria-selected={selected.has(opt)}
                        style={{display:'flex', gap:'8px', padding:'5px 8px', cursor:'pointer', alignItems:'center', fontSize:'12px', color: theme.text, background: selected.has(opt) ? (theme.select || theme.hover) : 'transparent'}}
                    >
                        <input type="checkbox" checked={selected.has(opt)} onChange={() => toggle(opt)} style={{margin:0, accentColor: theme.primary}} />
                        <span>{opt}</span>
                    </label>
                ))}
                {localHasMore && (
                    <div style={{ ...filterHintStyle(theme), padding:'8px', borderTop:`1px solid ${theme.border}` }}>
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
                        style={{ ...filterButtonStyle(theme, 'ghost'), width:'100%', borderRadius: 0, borderTop:`1px solid ${theme.border}`, color: theme.primary, cursor: loading ? 'default' : 'pointer' }}
                    >
                        {loading ? 'Loading...' : 'Load more'}
                    </button>
                )}
            </div>
        </div>
    );
};

export default MultiSelectFilter;
