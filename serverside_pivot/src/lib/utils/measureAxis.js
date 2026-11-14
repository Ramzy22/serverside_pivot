import { formatDisplayLabel } from './helpers';

/** @type {any} */
const MEASURE_AXIS_PLACEMENTS = new Set(['none', 'rows', 'columns']);

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizeMeasureAxisValue = (value, fallback = {}) => {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    /** @type {any} */
    const fallbackSource = fallback && typeof fallback === 'object' ? fallback : {};
    /** @type {any} */
    const rawPlacement = source.placement || source.axis || source.measureAxis || source.measurePlacement || fallbackSource.placement || 'none';
    /** @type {any} */
    const normalizedPlacement = String(rawPlacement || 'none').trim().toLowerCase();
    /** @type {any} */
    const placement = MEASURE_AXIS_PLACEMENTS.has(normalizedPlacement)
        ? normalizedPlacement
        : ((/** @type {Record<string, string>} */ ({ row: 'rows', column: 'columns', value: 'none', values: 'none', off: 'none' }))[normalizedPlacement] || 'none');
    /** @type {any} */
    const labelField = String(
        source.labelField
        || source.label_field
        || source.measureNameField
        || source.measureLabelField
        || fallbackSource.labelField
        || 'Measure Name'
    ).trim() || 'Measure Name';
    /** @type {any} */
    const valueField = String(
        source.valueField
        || source.value_field
        || source.measureValueField
        || source.measureAmountField
        || fallbackSource.valueField
        || 'Amount'
    ).trim() || 'Amount';
    /** @type {any} */
    const members = Array.isArray(source.members)
        ? source.members.filter(/** @param {any} [member] @returns {any} */ (member) => member && typeof member === 'object').map(/** @param {any} [member] @returns {any} */ (member) => ({ ...member }))
        : [];
    return {
        placement,
        labelField,
        valueField,
        members,
        suppressEmptyMembers: Boolean(source.suppressEmptyMembers || source.suppress_empty_members),
        suppressZeroMembers: Boolean(source.suppressZeroMembers || source.suppress_zero_members),
        totalsPolicy: String(source.totalsPolicy || source.totals_policy || fallbackSource.totalsPolicy || 'per_member'),
    };
};

/** @param {any} [config] @returns {any} */
export const getMeasureAxisAggForValueConfig = (config) => {
    /** @type {any} */
    const agg = String((config && config.agg) || 'sum').trim();
    return agg || 'sum';
};

/** @param {any} [config] @returns {any} */
export const getMeasureAxisAliasForValueConfig = (config) => {
    if (!config || typeof config !== 'object' || !config.field) return '';
    /** @type {any} */
    const agg = getMeasureAxisAggForValueConfig(config);
    return String(
        config.alias
        || config.id
        || (agg === 'formula' ? config.field : `${config.field}_${agg}`)
    ).trim();
};

/** @param {any} [member] @returns {any} */
export const getMeasureAxisAliasForMember = (member) => (
    String(
        (member && (
            member.measureAlias
            || member.measure_alias
            || member.alias
            || member.id
        )) || ''
    ).trim()
);

/** @param {any} [member] @returns {any} */
export const getMeasureAxisSourceFieldForMember = (member) => (
    String(
        (member && (
            member.sourceField
            || member.source_field
            || member.field
            || member.aggregationField
            || member.aggregation_field
        )) || ''
    ).trim()
);

/** @param {any} [measureAxis] @param {any} [valConfigs] @returns {any} */
export const buildMeasureAxisRuntimeConfig = (measureAxis, valConfigs = []) => {
    /** @type {any} */
    const normalized = normalizeMeasureAxisValue(measureAxis);
    if (normalized.placement === 'none') return { ...normalized, members: [] };
    /** @type {any} */
    const valueConfigs = (Array.isArray(valConfigs) ? valConfigs : [])
        .filter((config) => config && config.field && config.agg);
    /** @type {any} */
    const explicitMembers = Array.isArray(normalized.members) ? normalized.members : [];
    /** @type {any} */
    const explicitByAlias = new Map();
    /** @type {any} */
    const explicitBySourceAgg = new Map();
    explicitMembers.forEach(/** @param {any} [member] @returns {any} */ (member) => {
        if (!member || typeof member !== 'object') return;
        /** @type {any} */
        const alias = getMeasureAxisAliasForMember(member);
        if (alias && !explicitByAlias.has(alias)) {
            explicitByAlias.set(alias, member);
        }
        /** @type {any} */
        const sourceField = getMeasureAxisSourceFieldForMember(member);
        /** @type {any} */
        const agg = String(member.agg || member.aggregation || 'sum').trim().toLowerCase();
        if (sourceField) {
            const key = `${sourceField}|||${agg}`;
            if (!explicitBySourceAgg.has(key)) explicitBySourceAgg.set(key, member);
        }
    });

    /** @type {any} */
    const seenAliases = new Set();
    /** @type {any} */
    const runtimeMembers = valueConfigs.reduce(/** @param {any} [members] @param {any} [config] @returns {any} */ (members, config) => {
        /** @type {any} */
        const agg = getMeasureAxisAggForValueConfig(config);
        /** @type {any} */
        const normalizedAgg = String(agg || 'sum').trim().toLowerCase();
        /** @type {any} */
        const measureAlias = getMeasureAxisAliasForValueConfig(config);
        if (!measureAlias || seenAliases.has(measureAlias)) return members;
        /** @type {any} */
        const sourceField = String(config.field || '').trim();
        /** @type {any} */
        const explicitMember = explicitByAlias.get(measureAlias)
            || explicitBySourceAgg.get(`${sourceField}|||${normalizedAgg}`)
            || {};
        seenAliases.add(measureAlias);
        members.push({
            ...explicitMember,
            id: measureAlias,
            sourceField,
            measureAlias,
            agg,
            label: config.label || explicitMember.label || formatDisplayLabel(sourceField),
            order: members.length,
            format: config.format !== undefined ? config.format : explicitMember.format,
            editable: config.editable !== undefined ? Boolean(config.editable) : Boolean(explicitMember.editable),
            weightField: config.weightField || explicitMember.weightField,
        });
        return members;
    }, []);
    return { ...normalized, members: runtimeMembers };
};
