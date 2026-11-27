import React, { useEffect, useState } from 'react';
import { filterInputStyle } from './filterStyles';

/** @param {any} [filter] @returns {any} */
const getRangeDraft = (filter) => {
    /** @type {any} */
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

/** @param {any} [value] @returns {any} */
const parseNumericDraft = (value) => {
    /** @type {any} */
    const trimmed = value === null || value === undefined ? '' : String(value).trim();
    if (!trimmed) return { empty: true, value: null };
    /** @type {any} */
    const numericValue = Number(trimmed);
    if (!Number.isFinite(numericValue)) return null;
    return { empty: false, value: numericValue };
};

/**
 * The Range tab of a column filter. The owning ColumnFilter renders the footer;
 * this editor registers how to build its filter in `applyRef` (returning an
 * `error` when the draft is invalid) and calls `onSubmit` on Enter.
 * @param {any} p0 @returns {any}
 */
const NumericRangeFilter = ({ currentFilter, theme, applyRef, onSubmit }) => {
    /** @type {any} */
    const initialDraft = getRangeDraft(currentFilter);
    /** @type {[any, (...args: any[]) => any]} */
    const [min, setMin] = useState(initialDraft.min);
    /** @type {[any, (...args: any[]) => any]} */
    const [max, setMax] = useState(initialDraft.max);
    /** @type {[any, (...args: any[]) => any]} */
    const [error, setError] = useState(null);

    useEffect(() => {
        /** @type {any} */
        const nextDraft = getRangeDraft(currentFilter);
        setMin(nextDraft.min);
        setMax(nextDraft.max);
        setError(null);
    }, [currentFilter]);

    if (applyRef) {
        applyRef.current = () => {
            /** @type {any} */
            const parsedMin = parseNumericDraft(min);
            /** @type {any} */
            const parsedMax = parseNumericDraft(max);
            if (!parsedMin || !parsedMax) {
                setError('Enter valid numeric values.');
                return { error: true };
            }
            if (!parsedMin.empty && !parsedMax.empty && parsedMin.value > parsedMax.value) {
                setError('Min must be less than or equal to max.');
                return { error: true };
            }
            setError(null);
            if (!parsedMin.empty && !parsedMax.empty) {
                return { filter: { operator: 'AND', conditions: [{ type: 'between', value: parsedMin.value, value2: parsedMax.value }] } };
            }
            if (!parsedMin.empty) return { filter: { operator: 'AND', conditions: [{ type: 'gte', value: parsedMin.value }] } };
            if (!parsedMax.empty) return { filter: { operator: 'AND', conditions: [{ type: 'lte', value: parsedMax.value }] } };
            return { filter: null };
        };
    }

    /** @param {any} [event] */
    const submitOnEnter = (event) => {
        if (event.key === 'Enter' && typeof onSubmit === 'function') {
            event.preventDefault();
            onSubmit();
        }
    };

    return (
        <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
            <div style={{display: 'flex', gap: '8px', alignItems: 'center'}}>
                <input type="number" placeholder="Min" aria-label="Minimum" value={min} onChange={e => { setMin(e.target.value); setError(null); }} onKeyDown={submitOnEnter} style={{ ...filterInputStyle(theme), flex: 1 }} />
                <span style={{color: theme.textSec}}>–</span>
                <input type="number" placeholder="Max" aria-label="Maximum" value={max} onChange={e => { setMax(e.target.value); setError(null); }} onKeyDown={submitOnEnter} style={{ ...filterInputStyle(theme), flex: 1 }} />
            </div>
            {error && <div role="alert" style={{fontSize: '11px', color: theme.danger || '#dc2626'}}>{error}</div>}
        </div>
    );
};

export default NumericRangeFilter;
