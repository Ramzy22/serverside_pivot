export const isAutoDataFieldId = (field) => (
    field !== undefined
    && field !== null
    && String(field).trim() !== ''
    && field !== '__col_schema'
);

export const collectDataFieldIds = (rows) => {
    const fields = [];
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

export const compareFilterOptionValues = (left, right) => {
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
        return leftNumber - rightNumber;
    }
    return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: 'base' });
};

export const buildClientFilterOptionMap = (rows, fields) => {
    const result = {};
    const sourceRows = Array.isArray(rows) ? rows : [];
    const fieldList = Array.isArray(fields) && fields.length > 0 ? fields : collectDataFieldIds(sourceRows);
    fieldList.forEach((field) => {
        if (!isAutoDataFieldId(field)) return;
        const values = [];
        const seen = new Set();
        sourceRows.forEach((row) => {
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
