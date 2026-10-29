// Column-order normalization and movement helpers shared by the pivot root.

export const normalizeColumnOrderValue = (value) => {
    if (!Array.isArray(value)) return [];
    const seen = new Set();
    const result = [];
    value.forEach((item) => {
        if (item === null || item === undefined) return;
        const id = String(item).trim();
        if (!id || seen.has(id)) return;
        seen.add(id);
        result.push(id);
    });
    return result;
};

export const collectColumnDefLeafIds = (columnDefs) => {
    const ids = [];
    const seen = new Set();
    const visit = (defs) => {
        if (!Array.isArray(defs)) return;
        defs.forEach((columnDef) => {
            if (!columnDef || typeof columnDef !== 'object') return;
            if (Array.isArray(columnDef.columns) && columnDef.columns.length > 0) {
                visit(columnDef.columns);
                return;
            }
            const rawId = columnDef.id || columnDef.accessorKey;
            if (rawId === null || rawId === undefined) return;
            const id = String(rawId).trim();
            if (!id || seen.has(id)) return;
            seen.add(id);
            ids.push(id);
        });
    };
    visit(columnDefs);
    return ids;
};

export const mergeColumnOrderWithIds = (order, availableIds) => {
    const normalizedOrder = normalizeColumnOrderValue(order);
    const normalizedAvailable = normalizeColumnOrderValue(availableIds);
    if (normalizedAvailable.length === 0) return normalizedOrder;
    const availableSet = new Set(normalizedAvailable);
    const ordered = normalizedOrder.filter(id => availableSet.has(id));
    const orderedSet = new Set(ordered);
    normalizedAvailable.forEach((id) => {
        if (!orderedSet.has(id)) ordered.push(id);
    });
    return ordered;
};

/** Retain identities absent from the current sparse schema in their saved slots. */
export const reconcilePivotLayoutOrder = (savedIds, cellOrder, schema) => {
    const entries = Array.isArray(schema?.columns) ? schema.columns.filter(Boolean) : [];
    const byCell = new Map(entries.filter(entry => entry.layoutId).map(entry => [entry.id, entry.layoutId]));
    const visible = normalizeColumnOrderValue(cellOrder).map(id => byCell.get(id)).filter(Boolean);
    const uniqueVisible = normalizeColumnOrderValue(visible);
    const current = normalizeColumnOrderValue(savedIds);
    const visibleSet = new Set(uniqueVisible);
    let cursor = 0;
    const next = current.map(id => visibleSet.has(id) ? uniqueVisible[cursor++] : id);
    while (cursor < uniqueVisible.length) next.push(uniqueVisible[cursor++]);
    return normalizeColumnOrderValue(next);
};

export const pivotLayoutRequestFields = (manifest, revision, acknowledgedManifest, pinnedCellIds, schema, forceManifest = false, savedPinMap = {}, scopeChanged = false) => {
    const normalized = normalizeColumnOrderValue(manifest);
    const entries = Array.isArray(schema?.columns) ? schema.columns.filter(Boolean) : [];
    const byCell = new Map(entries.filter(entry => entry.layoutId).map(entry => [entry.id, entry.layoutId]));
    const pinnedColumnIds = normalizeColumnOrderValue(pinnedCellIds.map(id => byCell.get(id) || savedPinMap[id])).slice(0, 64);
    return {
        layoutRevision: scopeChanged ? undefined : revision || undefined,
        layoutManifest: scopeChanged || forceManifest || !revision || acknowledgedManifest !== normalized.join('\u001f') ? normalized : undefined,
        pinnedColumnIds,
    };
};

export const areStringArraysEqual = (left, right) => (
    Array.isArray(left)
    && Array.isArray(right)
    && left.length === right.length
    && left.every((value, index) => value === right[index])
);

export const moveColumnIdInOrder = (order, columnId, targetColumnId, position = 'before') => {
    const normalizedOrder = normalizeColumnOrderValue(order);
    const movingId = columnId === null || columnId === undefined ? '' : String(columnId);
    const targetId = targetColumnId === null || targetColumnId === undefined ? '' : String(targetColumnId);
    if (!movingId || !targetId || movingId === targetId) return normalizedOrder;
    const sourceIndex = normalizedOrder.indexOf(movingId);
    const originalTargetIndex = normalizedOrder.indexOf(targetId);
    if (sourceIndex < 0 || originalTargetIndex < 0) return normalizedOrder;
    const next = [...normalizedOrder];
    const [moved] = next.splice(sourceIndex, 1);
    let targetIndex = next.indexOf(targetId);
    if (targetIndex < 0) targetIndex = originalTargetIndex;
    if (position === 'after') targetIndex += 1;
    next.splice(Math.max(0, Math.min(targetIndex, next.length)), 0, moved);
    return next;
};

export const moveColumnIdByDelta = (order, columnId, delta) => {
    const normalizedOrder = normalizeColumnOrderValue(order);
    const movingId = columnId === null || columnId === undefined ? '' : String(columnId);
    const sourceIndex = normalizedOrder.indexOf(movingId);
    if (sourceIndex < 0) return normalizedOrder;
    const targetIndex = Math.max(0, Math.min(normalizedOrder.length - 1, sourceIndex + delta));
    if (targetIndex === sourceIndex) return normalizedOrder;
    const next = [...normalizedOrder];
    const [moved] = next.splice(sourceIndex, 1);
    next.splice(targetIndex, 0, moved);
    return next;
};
