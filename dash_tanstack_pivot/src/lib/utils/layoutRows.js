export const isBlankHierarchyValue = (value) => (
    value === undefined
    || value === null
    || value === ''
    || (typeof value === 'number' && Number.isNaN(value))
);

export const resolveTabularPathFieldValue = (rowData, field, index, depth) => {
    if (!rowData || typeof rowData !== 'object') return undefined;
    if (rowData._isTotal || rowData._path === '__grand_total__') return undefined;

    const pathParts = typeof rowData._path === 'string' && rowData._path !== ''
        ? rowData._path.split('|||')
        : [];
    const pathFields = Array.isArray(rowData._pathFields) ? rowData._pathFields : [];
    const pathFieldIndex = pathFields.indexOf(field);
    const fallbackPathIndex = pathFieldIndex >= 0 ? pathFieldIndex : index;

    if (index > depth && pathFieldIndex < 0) return undefined;
    return pathParts[fallbackPathIndex];
};

export const resolveTabularRowFieldValue = (
    rowData,
    field,
    index,
    layoutMode,
    fallbackValue = undefined,
) => {
    const value = fallbackValue !== undefined
        ? fallbackValue
        : (rowData && typeof rowData === 'object' ? rowData[field] : undefined);
    if (layoutMode !== 'tabular' || !isBlankHierarchyValue(value)) {
        return value;
    }

    const depth = rowData && rowData.depth !== undefined ? rowData.depth : 0;
    const pathValue = resolveTabularPathFieldValue(rowData, field, index, depth);
    return isBlankHierarchyValue(pathValue) ? value : pathValue;
};
