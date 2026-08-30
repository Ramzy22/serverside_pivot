import { formatDisplayLabel } from './helpers';

const MEASURE_AXIS_PLACEMENTS = new Set(['none', 'rows', 'columns']);

export const normalizeMeasureAxisValue = (value, fallback = {}) => {
    const source = value && typeof value === 'object' ? value : {};
    const fallbackSource = fallback && typeof fallback === 'object' ? fallback : {};
    const rawPlacement = source.placement || source.axis || source.measureAxis || source.measurePlacement || fallbackSource.placement || 'none';
    const normalizedPlacement = String(rawPlacement || 'none').trim().toLowerCase();
    const placement = MEASURE_AXIS_PLACEMENTS.has(normalizedPlacement)
        ? normalizedPlacement
        : ({ row: 'rows', column: 'columns', value: 'none', values: 'none', off: 'none' }[normalizedPlacement] || 'none');
    const labelField = String(
        source.labelField
        || source.label_field
        || source.measureNameField
        || source.measureLabelField
        || fallbackSource.labelField
        || 'Measure Name'
    ).trim() || 'Measure Name';
    const valueField = String(
        source.valueField
        || source.value_field
        || source.measureValueField
        || source.measureAmountField
        || fallbackSource.valueField
        || 'Amount'
    ).trim() || 'Amount';
    const members = Array.isArray(source.members)
        ? source.members.filter((member) => member && typeof member === 'object').map((member) => ({ ...member }))
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

export const getMeasureAxisAggForValueConfig = (config) => {
    const agg = String((config && config.agg) || 'sum').trim();
    return agg || 'sum';
};

export const getMeasureAxisAliasForValueConfig = (config) => {
    if (!config || typeof config !== 'object' || !config.field) return '';
    const agg = getMeasureAxisAggForValueConfig(config);
    return String(
        config.alias
        || config.id
        || (agg === 'formula' ? config.field : `${config.field}_${agg}`)
    ).trim();
};

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

export const buildMeasureAxisRuntimeConfig = (measureAxis, valConfigs = []) => {
    const normalized = normalizeMeasureAxisValue(measureAxis);
    if (normalized.placement === 'none') return { ...normalized, members: [] };
    const valueConfigs = (Array.isArray(valConfigs) ? valConfigs : [])
        .filter((config) => config && config.field && config.agg);
    const explicitMembers = Array.isArray(normalized.members) ? normalized.members : [];
    const explicitByAlias = new Map();
    const explicitBySourceAgg = new Map();
    explicitMembers.forEach((member) => {
        if (!member || typeof member !== 'object') return;
        const alias = getMeasureAxisAliasForMember(member);
        if (alias && !explicitByAlias.has(alias)) {
            explicitByAlias.set(alias, member);
        }
        const sourceField = getMeasureAxisSourceFieldForMember(member);
        const agg = String(member.agg || member.aggregation || 'sum').trim().toLowerCase();
        if (sourceField) {
            const key = `${sourceField}|||${agg}`;
            if (!explicitBySourceAgg.has(key)) explicitBySourceAgg.set(key, member);
        }
    });

    const seenAliases = new Set();
    const runtimeMembers = valueConfigs.reduce((members, config) => {
        const agg = getMeasureAxisAggForValueConfig(config);
        const normalizedAgg = String(agg || 'sum').trim().toLowerCase();
        const measureAlias = getMeasureAxisAliasForValueConfig(config);
        if (!measureAlias || seenAliases.has(measureAlias)) return members;
        const sourceField = String(config.field || '').trim();
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
