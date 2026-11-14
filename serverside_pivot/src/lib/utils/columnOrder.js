// Column-order normalization and movement helpers shared by the pivot root.

/** @param {any} [value] @returns {any} */
export const normalizeColumnOrderValue = (value) => {
    if (!Array.isArray(value)) return [];
    /** @type {any} */
    const seen = new Set();
    /** @type {any[]} */
    const result = [];
    value.forEach((item) => {
        if (item === null || item === undefined) return;
        /** @type {any} */
        const id = String(item).trim();
        if (!id || seen.has(id)) return;
        seen.add(id);
        result.push(id);
    });
    return result;
};

/** @param {any} [columnDefs] @returns {any} */
export const collectColumnDefLeafIds = (columnDefs) => {
    /** @type {any[]} */
    const ids = [];
    /** @type {any} */
    const seen = new Set();
    /** @param {any} [defs] @returns {any} */
    const visit = (defs) => {
        if (!Array.isArray(defs)) return;
        defs.forEach((columnDef) => {
            if (!columnDef || typeof columnDef !== 'object') return;
            if (Array.isArray(columnDef.columns) && columnDef.columns.length > 0) {
                visit(columnDef.columns);
                return;
            }
            /** @type {any} */
            const rawId = columnDef.id || columnDef.accessorKey;
            if (rawId === null || rawId === undefined) return;
            /** @type {any} */
            const id = String(rawId).trim();
            if (!id || seen.has(id)) return;
            seen.add(id);
            ids.push(id);
        });
    };
    visit(columnDefs);
    return ids;
};

/** @param {any} [order] @param {any} [availableIds] @returns {any} */
export const mergeColumnOrderWithIds = (order, availableIds) => {
    /** @type {any} */
    const normalizedOrder = normalizeColumnOrderValue(order);
    /** @type {any} */
    const normalizedAvailable = normalizeColumnOrderValue(availableIds);
    if (normalizedAvailable.length === 0) return normalizedOrder;
    /** @type {any} */
    const availableSet = new Set(normalizedAvailable);
    /** @type {any} */
    const ordered = normalizedOrder.filter(/** @param {any} [id] @returns {any} */ id => availableSet.has(id));
    /** @type {any} */
    const orderedSet = new Set(ordered);
    normalizedAvailable.forEach(/** @param {any} [id] @returns {any} */ (id) => {
        if (!orderedSet.has(id)) ordered.push(id);
    });
    return ordered;
};

/** Retain identities absent from the current sparse schema in their saved slots.  @param {any} [savedIds] @param {any} [cellOrder] @param {any} [schema] @returns {any} */
export const reconcilePivotLayoutOrder = (savedIds, cellOrder, schema) => {
    /** @type {any} */
    const entries = Array.isArray(schema?.columns) ? schema.columns.filter(Boolean) : [];
    /** @type {any} */
    const byCell = new Map(entries.filter(/** @param {any} [entry] @returns {any} */ entry => entry.layoutId).map(/** @param {any} [entry] @returns {any} */ entry => [entry.id, entry.layoutId]));
    /** @type {any} */
    const visible = normalizeColumnOrderValue(cellOrder).map(/** @param {any} [id] @returns {any} */ id => byCell.get(id)).filter(Boolean);
    /** @type {any} */
    const uniqueVisible = normalizeColumnOrderValue(visible);
    /** @type {any} */
    const current = normalizeColumnOrderValue(savedIds);
    /** @type {any} */
    const visibleSet = new Set(uniqueVisible);
    let cursor = 0;
    /** @type {any} */
    const next = current.map(/** @param {any} [id] @returns {any} */ id => visibleSet.has(id) ? uniqueVisible[cursor++] : id);
    while (cursor < uniqueVisible.length) next.push(uniqueVisible[cursor++]);
    return normalizeColumnOrderValue(next);
};

/** @param {any} [manifest] @param {any} [revision] @param {any} [acknowledgedManifest] @param {any} [pinnedCellIds] @param {any} [schema] @param {any} [forceManifest] @param {any} [savedPinMap] @param {any} [scopeChanged] @returns {any} */
export const pivotLayoutRequestFields = (manifest, revision, acknowledgedManifest, pinnedCellIds, schema, forceManifest = false, savedPinMap = {}, scopeChanged = false) => {
    /** @type {any} */
    const normalized = normalizeColumnOrderValue(manifest);
    /** @type {any} */
    const entries = Array.isArray(schema?.columns) ? schema.columns.filter(Boolean) : [];
    /** @type {any} */
    const byCell = new Map(entries.filter(/** @param {any} [entry] @returns {any} */ entry => entry.layoutId).map(/** @param {any} [entry] @returns {any} */ entry => [entry.id, entry.layoutId]));
    /** @type {any} */
    const pinnedColumnIds = normalizeColumnOrderValue(pinnedCellIds.map(/** @param {any} [id] @returns {any} */ id => byCell.get(id) || savedPinMap[id])).slice(0, 64);
    return {
        layoutRevision: scopeChanged ? undefined : revision || undefined,
        layoutManifest: scopeChanged || forceManifest || !revision || acknowledgedManifest !== normalized.join('\u001f') ? normalized : undefined,
        pinnedColumnIds,
    };
};

/** @param {any} [left] @param {any} [right] @returns {any} */
export const areStringArraysEqual = (left, right) => (
    Array.isArray(left)
    && Array.isArray(right)
    && left.length === right.length
    && left.every((value, index) => value === right[index])
);

/** @param {any} [order] @param {any} [columnId] @param {any} [targetColumnId] @param {any} [position] @returns {any} */
export const moveColumnIdInOrder = (order, columnId, targetColumnId, position = 'before') => {
    /** @type {any} */
    const normalizedOrder = normalizeColumnOrderValue(order);
    /** @type {any} */
    const movingId = columnId === null || columnId === undefined ? '' : String(columnId);
    /** @type {any} */
    const targetId = targetColumnId === null || targetColumnId === undefined ? '' : String(targetColumnId);
    if (!movingId || !targetId || movingId === targetId) return normalizedOrder;
    /** @type {any} */
    const sourceIndex = normalizedOrder.indexOf(movingId);
    /** @type {any} */
    const originalTargetIndex = normalizedOrder.indexOf(targetId);
    if (sourceIndex < 0 || originalTargetIndex < 0) return normalizedOrder;
    /** @type {any[]} */
    const next = [...normalizedOrder];
    /** @type {any} */
    const [moved] = next.splice(sourceIndex, 1);
    /** @type {any} */
    let targetIndex = next.indexOf(targetId);
    if (targetIndex < 0) targetIndex = originalTargetIndex;
    if (position === 'after') targetIndex += 1;
    next.splice(Math.max(0, Math.min(targetIndex, next.length)), 0, moved);
    return next;
};

/** @param {any} [order] @param {any} [columnId] @param {any} [delta] @returns {any} */
export const moveColumnIdByDelta = (order, columnId, delta) => {
    /** @type {any} */
    const normalizedOrder = normalizeColumnOrderValue(order);
    /** @type {any} */
    const movingId = columnId === null || columnId === undefined ? '' : String(columnId);
    /** @type {any} */
    const sourceIndex = normalizedOrder.indexOf(movingId);
    if (sourceIndex < 0) return normalizedOrder;
    /** @type {any} */
    const targetIndex = Math.max(0, Math.min(normalizedOrder.length - 1, sourceIndex + delta));
    if (targetIndex === sourceIndex) return normalizedOrder;
    /** @type {any[]} */
    const next = [...normalizedOrder];
    /** @type {any} */
    const [moved] = next.splice(sourceIndex, 1);
    next.splice(targetIndex, 0, moved);
    return next;
};
