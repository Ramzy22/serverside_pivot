/** @param {any} [field] @returns {any} */
export const isAutoDataFieldId = (field) => (
    field !== undefined
    && field !== null
    && String(field).trim() !== ''
    && field !== '__col_schema'
);

/** @param {any} [rows] @returns {any} */
export const collectDataFieldIds = (rows) => {
    /** @type {any[]} */
    const fields = [];
    /** @type {any} */
    const seen = new Set();
    if (!Array.isArray(rows)) return fields;
    rows.forEach((row) => {
        if (!row || typeof row !== 'object') return;
        Object.keys(row).forEach((field) => {
            if (!isAutoDataFieldId(field) || seen.has(field)) return;
            seen.add(field);
            fields.push(field);
        });
    });
    return fields;
};

/** @param {any} [left] @param {any} [right] @returns {any} */
export const compareFilterOptionValues = (left, right) => {
    /** @type {any} */
    const leftNumber = Number(left);
    /** @type {any} */
    const rightNumber = Number(right);
    if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
        return leftNumber - rightNumber;
    }
    return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: 'base' });
};

/** @param {any} [rows] @param {any} [fields] @returns {any} */
export const buildClientFilterOptionMap = (rows, fields) => {
    /** @type {Record<string, any>} */
    const result = {};
    /** @type {any} */
    const sourceRows = Array.isArray(rows) ? rows : [];
    /** @type {any} */
    const fieldList = Array.isArray(fields) && fields.length > 0 ? fields : collectDataFieldIds(sourceRows);
    fieldList.forEach(/** @param {any} [field] @returns {any} */ (field) => {
        if (!isAutoDataFieldId(field)) return;
        /** @type {any[]} */
        const values = [];
        /** @type {any} */
        const seen = new Set();
        sourceRows.forEach(/** @param {any} [row] @returns {any} */ (row) => {
            if (!row || typeof row !== 'object' || !Object.prototype.hasOwnProperty.call(row, field)) return;
            const value = row[field];
            if (value === null || value === undefined || value === '') return;
            const key = `${typeof value}:${String(value)}`;
            if (seen.has(key)) return;
            seen.add(key);
            values.push(value);
        });
        result[field] = values.sort(compareFilterOptionValues);
    });
    return result;
};
