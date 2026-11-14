/** @param {any} [value] @returns {any} */
export const isBlankHierarchyValue = (value) => (
    value === undefined
    || value === null
    || value === ''
    || (typeof value === 'number' && Number.isNaN(value))
);

/** @param {any} [rowData] @param {any} [field] @param {any} [index] @param {any} [depth] @returns {any} */
export const resolveTabularPathFieldValue = (rowData, field, index, depth) => {
    if (!rowData || typeof rowData !== 'object') return undefined;
    if (rowData._isTotal || rowData._path === '__grand_total__') return undefined;

    /** @type {any} */
    const pathParts = typeof rowData._path === 'string' && rowData._path !== ''
        ? rowData._path.split('|||')
        : [];
    /** @type {any} */
    const pathFields = Array.isArray(rowData._pathFields) ? rowData._pathFields : [];
    /** @type {any} */
    const pathFieldIndex = pathFields.indexOf(field);
    /** @type {any} */
    const fallbackPathIndex = pathFieldIndex >= 0 ? pathFieldIndex : index;

    if (index > depth && pathFieldIndex < 0) return undefined;
    return pathParts[fallbackPathIndex];
};

/** @param {any} [rowData] @param {any} [field] @param {any} [index] @param {any} [layoutMode] @param {any} [fallbackValue] @returns {any} */
export const resolveTabularRowFieldValue = (
    rowData,
    field,
    index,
    layoutMode,
    fallbackValue = undefined,
) => {
    /** @type {any} */
    const value = fallbackValue !== undefined
        ? fallbackValue
        : (rowData && typeof rowData === 'object' ? rowData[field] : undefined);
    if (layoutMode !== 'tabular' || !isBlankHierarchyValue(value)) {
        return value;
    }

    /** @type {any} */
    const depth = rowData && rowData.depth !== undefined ? rowData.depth : 0;
    /** @type {any} */
    const pathValue = resolveTabularPathFieldValue(rowData, field, index, depth);
    return isBlankHierarchyValue(pathValue) ? value : pathValue;
};
