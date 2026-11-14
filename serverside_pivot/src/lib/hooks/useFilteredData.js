import { useMemo } from 'react';

/** @param {any} [rowVal] @param {any} [filterGroup] @returns {any} */
export function evaluateFilterGroup(rowVal, filterGroup) {
  if (!filterGroup) return true;
  if (typeof filterGroup === 'string') {
    return String(rowVal).toLowerCase().includes(filterGroup.toLowerCase());
  }
  if (!filterGroup.conditions || filterGroup.conditions.length === 0) return true;

  /** @type {any} */
  const passes = filterGroup.conditions.map(/** @param {any} [cond] @returns {any} */ cond => {
    const val = cond.value;
    if (cond.type === 'in') return Array.isArray(val) && val.includes(rowVal);
    /** @type {any} */
    const rStr = String(rowVal).toLowerCase();
    /** @type {any} */
    const vStr = String(val).toLowerCase();
    if (cond.type === 'contains') return rStr.includes(vStr);
    if (cond.type === 'startsWith') return rStr.startsWith(vStr);
    if (cond.type === 'endsWith') return rStr.endsWith(vStr);
    if (cond.type === 'eq' || cond.type === 'equals')
      return cond.caseSensitive ? String(rowVal) === String(val) : rStr === vStr;
    if (cond.type === 'ne' || cond.type === 'notEquals')
      return cond.caseSensitive ? String(rowVal) !== String(val) : rStr !== vStr;
    /** @type {any} */
    const rNum = Number(rowVal);
    /** @type {any} */
    const vNum = Number(val);
    if (!isNaN(rNum) && !isNaN(vNum)) {
      if (cond.type === 'gt') return rNum > vNum;
      if (cond.type === 'lt') return rNum < vNum;
      if (cond.type === 'gte') return rNum >= vNum;
      if (cond.type === 'lte') return rNum <= vNum;
      if (cond.type === 'between') {
        /** @type {any} */
        const vNum2 = Number(cond.value2);
        return rNum >= vNum && rNum <= vNum2;
      }
    }
    return true;
  });

  return filterGroup.operator === 'OR' ? passes.some(/** @param {any} [p] @returns {any} */ p => p) : passes.every(/** @param {any} [p] @returns {any} */ p => p);
}

/** @param {any} [data] @param {any} [filters] @param {any} [serverSide] @returns {any} */
export function useFilteredData(data, filters, serverSide) {
  return useMemo(() => {
    if (serverSide) return data || [];
    if (!data || !data.length) return [];
    return data.filter(/** @param {any} [row] @returns {any} */ row =>
      Object.entries(filters).every(([colId, filterGroup]) =>
        evaluateFilterGroup(row[colId], filterGroup)
      )
    );
  }, [data, filters, serverSide]);
}
