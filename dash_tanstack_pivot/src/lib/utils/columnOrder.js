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
