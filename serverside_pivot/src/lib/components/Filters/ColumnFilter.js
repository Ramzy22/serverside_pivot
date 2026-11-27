import React, { useState, useEffect, useRef } from 'react';
import Icons from '../../utils/Icons';
import ThemedSelect from '../ThemedSelect';
import DateRangeFilter from './DateRangeFilter';
import NumericRangeFilter from './NumericRangeFilter';
import MultiSelectFilter from './MultiSelectFilter';
import { filterButtonStyle, filterCardStyle, filterHintStyle, filterInputStyle } from './filterStyles';

/** @returns {any} */
const createDefaultCondition = () => ({type: 'contains', value: '', caseSensitive: false});

/** @param {any} [value] @returns {any} */
const parseFilterListText = (value) => {
    /** @type {any} */
    const text = Array.isArray(value) ? value.join('\n') : String(value || '');
    return text
        .split(/[\r\n\t,;]+/)
        .map(/** @param {any} [item] @returns {any} */ item => item.trim())
        .filter(Boolean);
};

/** @param {{ conditions?: any[], [key: string]: any }} [filter] @returns {any} */
const cloneFilterConditions = (filter) => (
    filter && Array.isArray(filter.conditions)
        ? filter.conditions.map(condition => ({ ...condition }))
        : [createDefaultCondition()]
);

/**
 * Column filter editor: Rules / List / Date / Range tabs over one footer
 * (Clear · Cancel · Apply). Each tab body registers how to build its filter
 * in `applyRef`; Apply builds it, sends it and closes.
 * @param {any} p0 @returns {any}
 */
const ColumnFilter = ({ column, onFilter, currentFilter, options = [], optionMeta = null, onSearchOptions, onLoadMoreOptions, theme, onClose }) => {
    /** @type {any} */
    const resolvedTheme = theme || { primary: '#4F46E5', border: '#e2e8f0', headerBg: '#f8fafc', text: '#0f172a', textSec: '#64748b', background: '#fff' };
    /** @type {any} */
    const isLeaf = column.getLeafColumns
        ? column.getLeafColumns().length === 1
        : !column.columns;

    /** @type {any} */
    const leaf = isLeaf
        ? (column.getLeafColumns ? column.getLeafColumns()[0] : column)
        : null;
    /** @type {any} */
    const colId = String((leaf && leaf.id) || (column && column.id) || '').toLowerCase();
    const columnLabel = (
        (typeof column.header === 'string' && column.header)
        || (column.columnDef && typeof column.columnDef.header === 'string' && column.columnDef.header)
        || (leaf && leaf.columnDef && typeof leaf.columnDef.header === 'string' && leaf.columnDef.header)
        || (leaf && leaf.id)
        || (column && column.id)
        || ''
    );
    /** @type {any} */
    const isDate = (leaf && leaf.columnDef && leaf.columnDef.meta && leaf.columnDef.meta.type === 'date') || colId.includes('date') || colId.includes('time');
    /** @type {any} */
    const isNumeric = (leaf && leaf.columnDef && leaf.columnDef.meta && leaf.columnDef.meta.type === 'number') || colId.includes('sales') || colId.includes('cost') || colId.includes('amount') || colId.includes('price');

    /** @type {[any, (...args: any[]) => any]} */
    const [tab, setTab] = useState('condition');
    /** @type {any} */
    const tabAutoInitialized = useRef(false);
    /** The active tab's filter builder: () => ({ filter } | { error }). */
    /** @type {{ current: null | (() => any) }} */
    const applyRef = useRef(null);
    /** @param {any} [nextTab] @returns {any} */
    const selectTab = (nextTab) => {
        tabAutoInitialized.current = true;
        setTab(nextTab);
    };

    // Auto-select tab on first mount only; never override a user-driven tab change
    useEffect(() => {
        if (tabAutoInitialized.current) return;
        if (options && options.length > 0) {
            tabAutoInitialized.current = true;
            setTab('values');
        } else if (isDate) {
            tabAutoInitialized.current = true;
            setTab('date');
        }
    }, [options && options.length, isDate]); // eslint-disable-line react-hooks/exhaustive-deps

    /** @type {any} */
    const isMulti = currentFilter && currentFilter.conditions;
    /** @type {[any, (...args: any[]) => any]} */
    const [operator, setOperator] = useState(isMulti ? (currentFilter.operator || 'AND') : 'AND');
    /** @type {[any, (...args: any[]) => any]} */
    const [conditions, setConditions] = useState(() => cloneFilterConditions(currentFilter));

    useEffect(() => {
        /** @type {any} */
        const nextIsMulti = currentFilter && currentFilter.conditions;
        setOperator(nextIsMulti ? (currentFilter.operator || 'AND') : 'AND');
        setConditions(cloneFilterConditions(currentFilter));
    }, [currentFilter]);

    if (!isLeaf) {
        return (
            <div style={{ ...filterHintStyle(resolvedTheme), padding: '12px', fontSize: '12px' }}>
                Filters are available only for value columns.
            </div>
        );
    }

    /** @param {any} [index] @param {any} [key] @param {any} [value] @returns {any} */
    const updateCondition = (index, key, value) => {
        setConditions(/** @param {any} [previousConditions] @returns {any} */ previousConditions => previousConditions.map(/** @param {any} [condition] @param {any} [conditionIndex] @returns {any} */ (condition, conditionIndex) => (
            conditionIndex === index ? { ...condition, [key]: value } : condition
        )));
    };

    /** @returns {any} */
    const addCondition = () => {
        setConditions(/** @param {any} [previousConditions] @returns {any} */ previousConditions => [...previousConditions, createDefaultCondition()]);
    };

    /** @param {any} [index] @returns {any} */
    const removeCondition = (index) => {
        setConditions(/** @param {any} [previousConditions] @returns {any} */ previousConditions => previousConditions.filter(/** @param {any} [_] @param {any} [i] @returns {any} */ (_, i) => i !== index));
    };

    /** @returns {any} */
    const buildRulesFilter = () => {
        /** @type {any} */
        const validConditions = conditions.filter(/** @param {any} [c] @returns {any} */ c => {
             if (c.type === 'between') return c.value && c.value2;
             if (c.type === 'in') return parseFilterListText(c.value).length > 0;
             return String(c.value).trim() !== '';
        });

        /** @type {any[]} */
        const builtConditions = validConditions.map(/** @param {any} [c] @returns {any} */ c => {
             let finalVal = c.value;
             let finalVal2 = c.value2;

             if (c.type === 'in') {
                 /** @type {any} */
                 const listValues = parseFilterListText(finalVal).map(/** @param {any} [value] @returns {any} */ value => (
                     isNumeric && value !== '' && !isNaN(Number(value)) ? Number(value) : value
                 ));
                 return { ...c, value: listValues };
             }

             if (isNumeric) {
                 if (finalVal !== '' && !isNaN(Number(finalVal))) finalVal = Number(finalVal);
                 if (finalVal2 !== '' && !isNaN(Number(finalVal2))) finalVal2 = Number(finalVal2);
             }

             if (c.type === 'between') {
                  return { ...c, value: [finalVal, finalVal2] };
             }
             return { ...c, value: finalVal };
        });
        return { filter: builtConditions.length > 0 ? { operator, conditions: builtConditions } : null };
    };

    if (tab === 'condition') applyRef.current = buildRulesFilter;

    /** @returns {any} */
    const handleApply = () => {
        /** @type {any} */
        const result = typeof applyRef.current === 'function' ? applyRef.current() : { filter: null };
        if (!result || result.error) return;
        onFilter(result.filter || null);
        if (onClose) onClose();
    };

    /** @returns {any} */
    const handleClear = () => {
        onFilter(null);
        if (onClose) onClose();
    };

    /** @type {{ id: string, label: string }[]} */
    const tabs = [
        { id: 'condition', label: 'Rules' },
        { id: 'values', label: 'List' },
        ...(isDate ? [{ id: 'date', label: 'Date' }] : []),
        ...(isNumeric ? [{ id: 'numeric', label: 'Range' }] : []),
    ];
    /** @param {any} [event] @returns {any} */
    const handleTabKeyDown = (event) => {
        if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
        event.preventDefault();
        const index = tabs.findIndex((entry) => entry.id === tab);
        const nextIndex = (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
        selectTab(tabs[nextIndex].id);
        /** @type {any} */
        const list = event.currentTarget;
        const nextButton = list && list.querySelectorAll ? list.querySelectorAll('[role="tab"]')[nextIndex] : null;
        if (nextButton && typeof nextButton.focus === 'function') nextButton.focus();
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '10px', color: resolvedTheme.text, fontSize: '12px', minHeight: 0, flex: 1}}>
            <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '8px'}}>
                <span style={{fontWeight: 700, fontSize: '13px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'}} title={String(columnLabel)}>Filter: {columnLabel}</span>
                <div
                    role="tablist"
                    aria-label="Filter mode"
                    onKeyDown={handleTabKeyDown}
                    style={{display: 'flex', gap: '2px', padding: '2px', borderRadius: resolvedTheme.radiusSm || '8px', background: resolvedTheme.headerSubtleBg || resolvedTheme.hover, flexShrink: 0}}
                >
                    {tabs.map((entry) => (
                        <button
                            key={entry.id}
                            type="button"
                            role="tab"
                            aria-selected={tab === entry.id}
                            tabIndex={tab === entry.id ? 0 : -1}
                            onClick={() => selectTab(entry.id)}
                            style={{
                                border: 'none',
                                padding: '4px 10px',
                                fontSize: '11px',
                                fontWeight: 600,
                                cursor: 'pointer',
                                borderRadius: '6px',
                                color: tab === entry.id ? resolvedTheme.text : resolvedTheme.textSec,
                                background: tab === entry.id ? (resolvedTheme.surfaceBg || resolvedTheme.background) : 'transparent',
                                boxShadow: tab === entry.id ? '0 1px 2px rgba(0,0,0,0.12)' : 'none',
                            }}
                        >
                            {entry.label}
                        </button>
                    ))}
                </div>
            </div>

            <div role="tabpanel" style={{overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '8px', minHeight: 0, flex: 1}}>
                {tab === 'values' && (
                    <MultiSelectFilter
                        options={options}
                        optionMeta={optionMeta}
                        onSearchOptions={onSearchOptions}
                        onLoadMoreOptions={onLoadMoreOptions}
                        currentFilter={currentFilter}
                        applyRef={applyRef}
                        onSubmit={handleApply}
                        theme={resolvedTheme}
                    />
                )}

                {tab === 'date' && (
                    <DateRangeFilter currentFilter={currentFilter} theme={resolvedTheme} applyRef={applyRef} />
                )}

                {tab === 'numeric' && (
                    <NumericRangeFilter currentFilter={currentFilter} theme={resolvedTheme} applyRef={applyRef} onSubmit={handleApply} />
                )}

                {tab === 'condition' && (
                    <>
                        {conditions.length > 1 ? (
                            <div role="group" aria-label="Combine conditions" style={{display: 'flex', justifyContent: 'flex-end', gap: '4px'}}>
                                <button type="button" aria-pressed={operator === 'AND'} onClick={() => setOperator('AND')} style={filterButtonStyle(resolvedTheme, 'toggle', operator === 'AND')}>AND</button>
                                <button type="button" aria-pressed={operator === 'OR'} onClick={() => setOperator('OR')} style={filterButtonStyle(resolvedTheme, 'toggle', operator === 'OR')}>OR</button>
                            </div>
                        ) : null}
                        <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
                        {conditions.map(/** @param {any} [cond] @param {any} [index] @returns {any} */ (cond, index) => (
                            <div key={index} style={{ ...filterCardStyle(resolvedTheme), display: 'flex', flexDirection: 'column', gap: '6px', padding: '8px' }}>
                                <div style={{display: 'flex', gap: '6px', alignItems: 'center'}}>
                                    <ThemedSelect size="sm" aria-label={`Condition ${index + 1} operator`} value={cond.type} onChange={(/** @type {any} */ e) => updateCondition(index, 'type', e.target.value)} style={{flex: 1, minWidth: 0}}>
                                        <option value="eq">Equals</option>
                                        <option value="ne">Not Equals</option>
                                        <option value="contains">Contains</option>
                                        <option value="startsWith">Starts With</option>
                                        <option value="endsWith">Ends With</option>
                                        <option value="gt">Greater Than</option>
                                        <option value="lt">Less Than</option>
                                        <option value="between">Between (Range)</option>
                                        <option value="in">In List</option>
                                    </ThemedSelect>
                                    {conditions.length > 1 && (
                                        <button type="button" aria-label={`Remove condition ${index + 1}`} onClick={() => removeCondition(index)} style={{ ...filterButtonStyle(resolvedTheme, 'ghost'), padding: '2px 4px' }}><Icons.Close/></button>
                                    )}
                                </div>

                                {cond.type === 'between' ? (
                                    <div style={{display: 'flex', gap: '6px'}}>
                                        <input
                                            placeholder="Start"
                                            aria-label={`Condition ${index + 1} start`}
                                            value={cond.value}
                                            onChange={e => updateCondition(index, 'value', e.target.value)}
                                            onKeyDown={e => { if (e.key === 'Enter') handleApply(); }}
                                            style={{ ...filterInputStyle(resolvedTheme), width: '50%' }}
                                        />
                                        <input
                                            placeholder="End"
                                            aria-label={`Condition ${index + 1} end`}
                                            value={cond.value2 || ''}
                                            onChange={e => updateCondition(index, 'value2', e.target.value)}
                                            onKeyDown={e => { if (e.key === 'Enter') handleApply(); }}
                                            style={{ ...filterInputStyle(resolvedTheme), width: '50%' }}
                                        />
                                    </div>
                                ) : (
                                    <input
                                        placeholder={cond.type === 'in' ? "Paste or enter values..." : "Value..."}
                                        aria-label={`Condition ${index + 1} value`}
                                        value={Array.isArray(cond.value) ? cond.value.join(', ') : cond.value}
                                        onChange={e => updateCondition(index, 'value', e.target.value)}
                                        onKeyDown={e => { if (e.key === 'Enter') handleApply(); }}
                                        onPaste={e => {
                                            if (cond.type !== 'in') return;
                                            /** @type {any} */
                                            const pastedText = e.clipboardData && e.clipboardData.getData('text');
                                            /** @type {any} */
                                            const pastedValues = parseFilterListText(pastedText);
                                            if (pastedValues.length <= 1) return;
                                            e.preventDefault();
                                            updateCondition(index, 'value', pastedValues.join(', '));
                                        }}
                                        style={filterInputStyle(resolvedTheme)}
                                    />
                                )}

                                <label style={{ ...filterHintStyle(resolvedTheme), display:'flex', alignItems:'center', gap:'6px', cursor: 'pointer' }}>
                                    <input
                                        type="checkbox"
                                        checked={cond.caseSensitive || false}
                                        onChange={e => updateCondition(index, 'caseSensitive', e.target.checked)}
                                        style={{margin: 0, accentColor: resolvedTheme.primary}}
                                    />
                                    Match case
                                </label>
                            </div>
                        ))}
                        </div>
                        <button type="button" className="sp-btn sp-btn-subtle" onClick={addCondition} style={filterButtonStyle(resolvedTheme, 'subtle')}>Add condition</button>
                    </>
                )}
            </div>

            <div style={{display:'flex', justifyContent: 'space-between', alignItems: 'center', gap: '8px', borderTop: `1px solid ${resolvedTheme.border}`, paddingTop: '10px'}}>
                <button type="button" className="sp-btn sp-btn-ghost" onClick={handleClear} style={filterButtonStyle(resolvedTheme, 'danger')}>Clear</button>
                <div style={{display: 'flex', gap: '8px'}}>
                    {onClose && <button type="button" className="sp-btn sp-btn-subtle" onClick={onClose} style={filterButtonStyle(resolvedTheme, 'subtle')}>Cancel</button>}
                    <button type="button" className="sp-btn sp-btn-primary" onClick={handleApply} style={filterButtonStyle(resolvedTheme, 'primary')}>Apply</button>
                </div>
            </div>
        </div>
    );
};

export default ColumnFilter;
