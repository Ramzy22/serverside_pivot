import React, { useEffect, useState } from 'react';
import Icons from '../../utils/Icons';
import ThemedSelect from '../ThemedSelect';
import { filterButtonStyle, filterCardStyle, filterInputStyle } from './filterStyles';

/** @returns {any} */
const createDefaultDateCondition = () => ({ type: 'between', value: '', value2: '' });

/** @param {any} [filter] @returns {any} */
const cloneDateConditions = (filter) => {
    if (filter && Array.isArray(filter.conditions)) {
        return filter.conditions.map(/** @param {any} [condition] @returns {any} */ condition => ({ ...condition }));
    }
    if (filter && filter.value && !filter.conditions) {
        // A single value is an equality condition.
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

/**
 * The Date tab of a column filter. The owning ColumnFilter renders the footer;
 * this editor registers how to build its filter in `applyRef`.
 * @param {any} p0 @returns {any}
 */
const DateRangeFilter = ({ currentFilter, theme, applyRef }) => {
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

    if (applyRef) {
        applyRef.current = () => {
            /** @type {any} */
            const validConditions = conditions.filter(/** @param {any} [c] @returns {any} */ c => {
                if (c.type === 'between') return c.value && c.value2;
                return c.value;
            }).map(/** @param {any} [condition] @returns {any} */ condition => ({ ...condition }));
            return { filter: validConditions.length > 0 ? { operator, conditions: validConditions } : null };
        };
    }

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

    /** @param {any} [days] @returns {any} */
    const setPreset = (days) => {
        /** @type {any} */
        const e = new Date();
        /** @type {any} */
        const s = new Date();
        s.setDate(e.getDate() - days);
        // A preset replaces the conditions with one range.
        setConditions([{ type: 'between', value: formatLocalDate(s), value2: formatLocalDate(e) }]);
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
            <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '6px', flexWrap: 'wrap'}}>
                <div style={{display: 'flex', gap: '6px', flexWrap: 'wrap'}}>
                    <button type="button" onClick={() => setPreset(0)} style={filterButtonStyle(theme, 'toggle')}>Today</button>
                    <button type="button" onClick={() => setPreset(7)} style={filterButtonStyle(theme, 'toggle')}>Last 7d</button>
                    <button type="button" onClick={() => setPreset(30)} style={filterButtonStyle(theme, 'toggle')}>Last 30d</button>
                </div>
                {conditions.length > 1 ? (
                    <div role="group" aria-label="Combine conditions" style={{display: 'flex', gap: '4px'}}>
                        <button type="button" aria-pressed={operator === 'AND'} onClick={() => setOperator('AND')} style={filterButtonStyle(theme, 'toggle', operator === 'AND')}>AND</button>
                        <button type="button" aria-pressed={operator === 'OR'} onClick={() => setOperator('OR')} style={filterButtonStyle(theme, 'toggle', operator === 'OR')}>OR</button>
                    </div>
                ) : null}
            </div>

            <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
                {conditions.map(/** @param {any} [cond] @param {any} [index] @returns {any} */ (cond, index) => (
                    <div key={index} style={{ ...filterCardStyle(theme), display: 'flex', flexDirection: 'column', gap: '6px', padding: '8px' }}>
                        <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                            <ThemedSelect size="sm" aria-label={`Date condition ${index + 1} operator`} value={cond.type} onChange={(/** @type {any} */ e) => updateCondition(index, 'type', e.target.value)} style={{fontSize: '12px', width: '120px'}}>
                                <option value="between">Between</option>
                                <option value="eq">Equals</option>
                                <option value="gt">After</option>
                                <option value="lt">Before</option>
                            </ThemedSelect>
                            {conditions.length > 1 && (
                                <button type="button" aria-label={`Remove date condition ${index + 1}`} onClick={() => removeCondition(index)} style={{ ...filterButtonStyle(theme, 'ghost'), padding: '2px 4px' }}><Icons.Close/></button>
                            )}
                        </div>
                        <div style={{display: 'flex', gap: '6px', alignItems: 'center'}}>
                            <input type="date" aria-label={cond.type === 'between' ? 'From date' : 'Date'} value={cond.value} onChange={e => updateCondition(index, 'value', e.target.value)} style={{ ...filterInputStyle(theme), flex: 1 }} />
                            {cond.type === 'between' && (
                                <>
                                    <span style={{color: theme.textSec}}>–</span>
                                    <input type="date" aria-label="To date" value={cond.value2 || ''} onChange={e => updateCondition(index, 'value2', e.target.value)} style={{ ...filterInputStyle(theme), flex: 1 }} />
                                </>
                            )}
                        </div>
                    </div>
                ))}
            </div>

            <button type="button" className="sp-btn sp-btn-subtle" onClick={addCondition} style={filterButtonStyle(theme, 'subtle')}>Add condition</button>
        </div>
    );
};

export default DateRangeFilter;
