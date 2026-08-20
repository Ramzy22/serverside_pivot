const VIRTUAL_AXIS_PLACEMENTS = new Set(['none', 'rows', 'columns']);

const isPlainObject = (value) => Boolean(value && typeof value === 'object' && !Array.isArray(value));

export const normalizeVirtualAxisId = (value, fallback = 'axis') => {
    const raw = String(value === null || value === undefined ? fallback : value).trim();
    const normalized = raw
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^a-z0-9_-]/g, '');
    return normalized || fallback;
};

const normalizeVirtualAxisPlacement = (value, fallback = 'none') => {
    const raw = String(value || fallback || 'none').trim().toLowerCase();
    if (VIRTUAL_AXIS_PLACEMENTS.has(raw)) return raw;
    return {
        row: 'rows',
        column: 'columns',
        col: 'columns',
        cols: 'columns',
        off: 'none',
        disabled: 'none',
        false: 'none',
    }[raw] || 'none';
};

const getObjectValue = (source, keys, fallback = undefined) => {
    if (!isPlainObject(source)) return fallback;
    for (const key of keys) {
        if (Object.prototype.hasOwnProperty.call(source, key)) return source[key];
    }
    return fallback;
};

export const normalizeVirtualAxisMember = (member, index = 0) => {
    const source = isPlainObject(member) ? member : { label: member, value: member };
    const hasValue = Object.prototype.hasOwnProperty.call(source, 'value');
    const value = hasValue
        ? source.value
        : getObjectValue(source, ['memberValue', 'member_value', 'scope'], null);
    const rawId = getObjectValue(
        source,
        ['id', 'memberId', 'member_id', 'key'],
        value !== null && value !== undefined ? value : getObjectValue(source, ['label', 'name'], `member_${index}`)
    );
    const id = normalizeVirtualAxisId(rawId, `member_${index}`);
    const labelSource = getObjectValue(
        source,
        ['label', 'memberLabel', 'member_label', 'name'],
        value !== null && value !== undefined ? value : id
    );
    const label = String(labelSource === null || labelSource === undefined ? id : labelSource).trim() || id;
    return {
        ...source,
        id,
        label,
        value,
        enabled: source.enabled !== false,
        order: Number.isFinite(Number(source.order)) ? Number(source.order) : index,
    };
};

export const normalizeVirtualAxis = (axis, fallback = {}) => {
    const source = isPlainObject(axis) ? axis : {};
    const fallbackSource = isPlainObject(fallback) ? fallback : {};
    const rawId = getObjectValue(
        source,
        ['id', 'axisId', 'axis_id', 'key'],
        getObjectValue(fallbackSource, ['id', 'axisId', 'axis_id', 'key'], getObjectValue(source, ['labelField', 'label_field', 'label', 'name'], 'axis'))
    );
    const id = normalizeVirtualAxisId(rawId, 'axis');
    const labelSource = getObjectValue(
        source,
        ['labelField', 'label_field', 'label', 'name'],
        getObjectValue(fallbackSource, ['labelField', 'label_field', 'label', 'name'], id)
    );
    const labelField = String(labelSource || id).trim() || id;
    const placement = normalizeVirtualAxisPlacement(
        getObjectValue(source, ['placement', 'axis', 'location'], getObjectValue(fallbackSource, ['placement'], 'none')),
        'none'
    );
    const memberSource = Array.isArray(source.members)
        ? source.members
        : (Array.isArray(fallbackSource.members) ? fallbackSource.members : []);
    const seenMembers = new Set();
    const members = memberSource
        .map(normalizeVirtualAxisMember)
        .filter((member) => {
            if (!member || !member.id || seenMembers.has(member.id)) return false;
            seenMembers.add(member.id);
            return true;
        })
        .sort((a, b) => a.order - b.order);
    const enabled = Object.prototype.hasOwnProperty.call(source, 'enabled')
        ? Boolean(source.enabled)
        : (Object.prototype.hasOwnProperty.call(fallbackSource, 'enabled')
            ? Boolean(fallbackSource.enabled)
            : placement !== 'none');
    return {
        ...source,
        id,
        labelField,
        placement,
        enabled,
        members,
    };
};

export const normalizeVirtualAxes = (value, fallback = []) => {
    const rawAxes = Array.isArray(value)
        ? value
        : (isPlainObject(value) && Array.isArray(value.virtualAxes)
            ? value.virtualAxes
            : (isPlainObject(value) && Array.isArray(value.axes) ? value.axes : []));
    const fallbackAxes = Array.isArray(fallback) ? fallback : [];
    const fallbackById = new Map(
        fallbackAxes
            .map((axis) => normalizeVirtualAxis(axis))
            .filter((axis) => axis && axis.id)
            .map((axis) => [axis.id, axis])
    );
    const seen = new Set();
    return rawAxes
        .map((axis) => {
            const normalized = normalizeVirtualAxis(axis);
            return normalizeVirtualAxis(axis, fallbackById.get(normalized.id));
        })
        .filter((axis) => {
            if (!axis || !axis.id || seen.has(axis.id)) return false;
            seen.add(axis.id);
            return true;
        });
};

export const isVirtualAxisActive = (axis, placement = null) => {
    if (!axis || axis.enabled === false || axis.placement === 'none') return false;
    if (placement && axis.placement !== placement) return false;
    return Array.isArray(axis.members) && axis.members.some((member) => member && member.enabled !== false);
};

export const findVirtualAxisMember = (axis, value) => {
    if (!axis || !Array.isArray(axis.members)) return null;
    const id = normalizeVirtualAxisId(value, '');
    const text = String(value === null || value === undefined ? '' : value).trim();
    return axis.members.find((member) => (
        member
        && (
            member.id === id
            || String(member.label || '') === text
            || String(member.value === null || member.value === undefined ? '' : member.value) === text
        )
    )) || null;
};

const getVirtualAxisValuesMap = (config) => {
    if (!isPlainObject(config)) return {};
    const source = getObjectValue(config, ['virtualAxisValues', 'virtual_axis_values'], {});
    if (Array.isArray(source)) {
        return source.reduce((acc, entry) => {
            if (!isPlainObject(entry)) return acc;
            const axisId = normalizeVirtualAxisId(getObjectValue(entry, ['axisId', 'axis_id', 'id'], ''));
            if (axisId) acc[axisId] = entry;
            return acc;
        }, {});
    }
    return isPlainObject(source) ? source : {};
};

export const getVirtualAxisEntriesForConfig = (config, axes, placement = 'columns') => {
    const normalizedAxes = normalizeVirtualAxes(axes);
    const valuesByAxis = getVirtualAxisValuesMap(config);
    return normalizedAxes.reduce((entries, axis) => {
        if (!isVirtualAxisActive(axis, placement)) return entries;
        const rawEntry = valuesByAxis[axis.id];
        if (!isPlainObject(rawEntry)) return entries;
        const rawMemberId = getObjectValue(rawEntry, ['memberId', 'member_id', 'id', 'value', 'label'], '');
        const member = findVirtualAxisMember(axis, rawMemberId);
        const memberId = member ? member.id : normalizeVirtualAxisId(rawMemberId, '');
        const memberLabel = String(
            getObjectValue(rawEntry, ['memberLabel', 'member_label', 'label'], member ? member.label : rawMemberId)
        ).trim() || memberId;
        entries.push({
            axis,
            axisId: axis.id,
            axisLabel: axis.labelField,
            member,
            memberId,
            memberLabel,
            value: getObjectValue(rawEntry, ['value'], member ? member.value : rawMemberId),
            enabled: rawEntry.enabled !== false && (!member || member.enabled !== false),
        });
        return entries;
    }, []);
};

export const updateVirtualAxisPlacement = (axes, axisId, placement) => {
    const normalizedAxes = normalizeVirtualAxes(axes);
    const normalizedAxisId = normalizeVirtualAxisId(axisId, '');
    const normalizedPlacement = normalizeVirtualAxisPlacement(placement, 'none');
    return normalizedAxes.map((axis) => (
        axis.id === normalizedAxisId
            ? {
                ...axis,
                placement: normalizedPlacement,
                enabled: normalizedPlacement !== 'none',
            }
            : axis
    ));
};

export const updateVirtualAxisMemberEnabled = (axes, axisId, memberId, enabled) => {
    const normalizedAxes = normalizeVirtualAxes(axes);
    const normalizedAxisId = normalizeVirtualAxisId(axisId, '');
    const normalizedMemberId = normalizeVirtualAxisId(memberId, '');
    return normalizedAxes.map((axis) => {
        if (axis.id !== normalizedAxisId) return axis;
        return {
            ...axis,
            members: (axis.members || []).map((member) => (
                member.id === normalizedMemberId
                    ? { ...member, enabled: Boolean(enabled) }
                    : member
            )),
        };
    });
};

export const getVirtualAxisDisplayLabel = (axis) => (
    String((axis && (axis.labelField || axis.label || axis.name || axis.id)) || 'Virtual Axis').trim() || 'Virtual Axis'
);
