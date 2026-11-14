/** @type {any} */
const VIRTUAL_AXIS_PLACEMENTS = new Set(['none', 'rows', 'columns']);

/** @param {any} [value] @returns {any} */
const isPlainObject = (value) => Boolean(value && typeof value === 'object' && !Array.isArray(value));

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizeVirtualAxisId = (value, fallback = 'axis') => {
    /** @type {any} */
    const raw = String(value === null || value === undefined ? fallback : value).trim();
    /** @type {any} */
    const normalized = raw
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^a-z0-9_-]/g, '');
    return normalized || fallback;
};

/** @param {any} [value] @param {any} [fallback] @returns {any} */
const normalizeVirtualAxisPlacement = (value, fallback = 'none') => {
    /** @type {any} */
    const raw = String(value || fallback || 'none').trim().toLowerCase();
    if (VIRTUAL_AXIS_PLACEMENTS.has(raw)) return raw;
    return (/** @type {Record<string, string>} */ ({
        row: 'rows',
        column: 'columns',
        col: 'columns',
        cols: 'columns',
        off: 'none',
        disabled: 'none',
        false: 'none',
    }))[raw] || 'none';
};

/** @param {any} [source] @param {any} [keys] @param {any} [fallback] @returns {any} */
const getObjectValue = (source, keys, fallback = undefined) => {
    if (!isPlainObject(source)) return fallback;
    for (const key of keys) {
        if (Object.prototype.hasOwnProperty.call(source, key)) return source[key];
    }
    return fallback;
};

/** @param {any} [member] @param {any} [index] @returns {any} */
export const normalizeVirtualAxisMember = (member, index = 0) => {
    /** @type {any} */
    const source = isPlainObject(member) ? member : { label: member, value: member };
    /** @type {any} */
    const hasValue = Object.prototype.hasOwnProperty.call(source, 'value');
    /** @type {any} */
    const value = hasValue
        ? source.value
        : getObjectValue(source, ['memberValue', 'member_value', 'scope'], null);
    /** @type {any} */
    const rawId = getObjectValue(
        source,
        ['id', 'memberId', 'member_id', 'key'],
        value !== null && value !== undefined ? value : getObjectValue(source, ['label', 'name'], `member_${index}`)
    );
    /** @type {any} */
    const id = normalizeVirtualAxisId(rawId, `member_${index}`);
    /** @type {any} */
    const labelSource = getObjectValue(
        source,
        ['label', 'memberLabel', 'member_label', 'name'],
        value !== null && value !== undefined ? value : id
    );
    /** @type {any} */
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

/** @param {any} [axis] @param {any} [fallback] @returns {any} */
export const normalizeVirtualAxis = (axis, fallback = {}) => {
    /** @type {any} */
    const source = isPlainObject(axis) ? axis : {};
    /** @type {any} */
    const fallbackSource = isPlainObject(fallback) ? fallback : {};
    /** @type {any} */
    const rawId = getObjectValue(
        source,
        ['id', 'axisId', 'axis_id', 'key'],
        getObjectValue(fallbackSource, ['id', 'axisId', 'axis_id', 'key'], getObjectValue(source, ['labelField', 'label_field', 'label', 'name'], 'axis'))
    );
    /** @type {any} */
    const id = normalizeVirtualAxisId(rawId, 'axis');
    /** @type {any} */
    const labelSource = getObjectValue(
        source,
        ['labelField', 'label_field', 'label', 'name'],
        getObjectValue(fallbackSource, ['labelField', 'label_field', 'label', 'name'], id)
    );
    /** @type {any} */
    const labelField = String(labelSource || id).trim() || id;
    /** @type {any} */
    const placement = normalizeVirtualAxisPlacement(
        getObjectValue(source, ['placement', 'axis', 'location'], getObjectValue(fallbackSource, ['placement'], 'none')),
        'none'
    );
    /** @type {any} */
    const memberSource = Array.isArray(source.members)
        ? source.members
        : (Array.isArray(fallbackSource.members) ? fallbackSource.members : []);
    /** @type {any} */
    const seenMembers = new Set();
    /** @type {any} */
    const members = memberSource
        .map(normalizeVirtualAxisMember)
        .filter(/** @param {any} [member] @returns {any} */ (member) => {
            if (!member || !member.id || seenMembers.has(member.id)) return false;
            seenMembers.add(member.id);
            return true;
        })
        .sort(/** @param {any} [a] @param {any} [b] @returns {any} */ (a, b) => a.order - b.order);
    /** @type {any} */
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

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizeVirtualAxes = (value, fallback = []) => {
    /** @type {any} */
    const rawAxes = Array.isArray(value)
        ? value
        : (isPlainObject(value) && Array.isArray(value.virtualAxes)
            ? value.virtualAxes
            : (isPlainObject(value) && Array.isArray(value.axes) ? value.axes : []));
    /** @type {any} */
    const fallbackAxes = Array.isArray(fallback) ? fallback : [];
    /** @type {any} */
    const fallbackById = new Map(
        fallbackAxes
            .map(/** @param {any} [axis] @returns {any} */ (axis) => normalizeVirtualAxis(axis))
            .filter(/** @param {any} [axis] @returns {any} */ (axis) => axis && axis.id)
            .map(/** @param {any} [axis] @returns {any} */ (axis) => [axis.id, axis])
    );
    /** @type {any} */
    const seen = new Set();
    return rawAxes
        .map(/** @param {any} [axis] @returns {any} */ (axis) => {
            /** @type {any} */
            const normalized = normalizeVirtualAxis(axis);
            return normalizeVirtualAxis(axis, fallbackById.get(normalized.id));
        })
        .filter(/** @param {any} [axis] @returns {any} */ (axis) => {
            if (!axis || !axis.id || seen.has(axis.id)) return false;
            seen.add(axis.id);
            return true;
        });
};

/** @param {any} [axis] @param {any} [placement] @returns {any} */
export const isVirtualAxisActive = (axis, placement = null) => {
    if (!axis || axis.enabled === false || axis.placement === 'none') return false;
    if (placement && axis.placement !== placement) return false;
    return Array.isArray(axis.members) && axis.members.some(/** @param {any} [member] @returns {any} */ (member) => member && member.enabled !== false);
};

/** @param {any} [axis] @param {any} [value] @returns {any} */
export const findVirtualAxisMember = (axis, value) => {
    if (!axis || !Array.isArray(axis.members)) return null;
    /** @type {any} */
    const id = normalizeVirtualAxisId(value, '');
    /** @type {any} */
    const text = String(value === null || value === undefined ? '' : value).trim();
    return axis.members.find(/** @param {any} [member] @returns {any} */ (member) => (
        member
        && (
            member.id === id
            || String(member.label || '') === text
            || String(member.value === null || member.value === undefined ? '' : member.value) === text
        )
    )) || null;
};

/** @param {any} [config] @returns {any} */
const getVirtualAxisValuesMap = (config) => {
    if (!isPlainObject(config)) return {};
    /** @type {any} */
    const source = getObjectValue(config, ['virtualAxisValues', 'virtual_axis_values'], {});
    if (Array.isArray(source)) {
        return source.reduce((acc, entry) => {
            if (!isPlainObject(entry)) return acc;
            /** @type {any} */
            const axisId = normalizeVirtualAxisId(getObjectValue(entry, ['axisId', 'axis_id', 'id'], ''));
            if (axisId) acc[axisId] = entry;
            return acc;
        }, {});
    }
    return isPlainObject(source) ? source : {};
};

/** @param {any} [config] @param {any} [axes] @param {any} [placement] @returns {any} */
export const getVirtualAxisEntriesForConfig = (config, axes, placement = 'columns') => {
    /** @type {any} */
    const normalizedAxes = normalizeVirtualAxes(axes);
    /** @type {any} */
    const valuesByAxis = getVirtualAxisValuesMap(config);
    return normalizedAxes.reduce(/** @param {any} [entries] @param {any} [axis] @returns {any} */ (entries, axis) => {
        if (!isVirtualAxisActive(axis, placement)) return entries;
        const rawEntry = valuesByAxis[axis.id];
        if (!isPlainObject(rawEntry)) return entries;
        /** @type {any} */
        const rawMemberId = getObjectValue(rawEntry, ['memberId', 'member_id', 'id', 'value', 'label'], '');
        /** @type {any} */
        const member = findVirtualAxisMember(axis, rawMemberId);
        /** @type {any} */
        const memberId = member ? member.id : normalizeVirtualAxisId(rawMemberId, '');
        /** @type {any} */
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

/** @param {any} [axes] @param {any} [axisId] @param {any} [placement] @returns {any} */
export const updateVirtualAxisPlacement = (axes, axisId, placement) => {
    /** @type {any} */
    const normalizedAxes = normalizeVirtualAxes(axes);
    /** @type {any} */
    const normalizedAxisId = normalizeVirtualAxisId(axisId, '');
    /** @type {any} */
    const normalizedPlacement = normalizeVirtualAxisPlacement(placement, 'none');
    return normalizedAxes.map(/** @param {any} [axis] @returns {any} */ (axis) => (
        axis.id === normalizedAxisId
            ? {
                ...axis,
                placement: normalizedPlacement,
                enabled: normalizedPlacement !== 'none',
            }
            : axis
    ));
};

/** @param {any} [axes] @param {any} [axisId] @param {any} [memberId] @param {any} [enabled] @returns {any} */
export const updateVirtualAxisMemberEnabled = (axes, axisId, memberId, enabled) => {
    /** @type {any} */
    const normalizedAxes = normalizeVirtualAxes(axes);
    /** @type {any} */
    const normalizedAxisId = normalizeVirtualAxisId(axisId, '');
    /** @type {any} */
    const normalizedMemberId = normalizeVirtualAxisId(memberId, '');
    return normalizedAxes.map(/** @param {any} [axis] @returns {any} */ (axis) => {
        if (axis.id !== normalizedAxisId) return axis;
        return {
            ...axis,
            members: (axis.members || []).map(/** @param {any} [member] @returns {any} */ (member) => (
                member.id === normalizedMemberId
                    ? { ...member, enabled: Boolean(enabled) }
                    : member
            )),
        };
    });
};

/** @param {any} [axis] @returns {any} */
export const getVirtualAxisDisplayLabel = (axis) => (
    String((axis && (axis.labelField || axis.label || axis.name || axis.id)) || 'Virtual Axis').trim() || 'Virtual Axis'
);
