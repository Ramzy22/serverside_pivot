import { useMemo } from 'react';

export function evaluateFilterGroup(rowVal, filterGroup) {
  if (!filterGroup) return true;
  if (typeof filterGroup === 'string') {
    return String(rowVal).toLowerCase().includes(filterGroup.toLowerCase());
  }
  if (!filterGroup.conditions || filterGroup.conditions.length === 0) return true;

  const passes = filterGroup.conditions.map(cond => {
    const val = cond.value;
    if (cond.type === 'in') return Array.isArray(val) && val.includes(rowVal);
    const rStr = String(rowVal).toLowerCase();
    const vStr = String(val).toLowerCase();
    if (cond.type === 'contains') return rStr.includes(vStr);
    if (cond.type === 'startsWith') return rStr.startsWith(vStr);
    if (cond.type === 'endsWith') return rStr.endsWith(vStr);
    if (cond.type === 'eq' || cond.type === 'equals')
      return cond.caseSensitive ? String(rowVal) === String(val) : rStr === vStr;
    if (cond.type === 'ne' || cond.type === 'notEquals')
      return cond.caseSensitive ? String(rowVal) !== String(val) : rStr !== vStr;
    const rNum = Number(rowVal);
    const vNum = Number(val);
    if (!isNaN(rNum) && !isNaN(vNum)) {
      if (cond.type === 'gt') return rNum > vNum;
      if (cond.type === 'lt') return rNum < vNum;
      if (cond.type === 'gte') return rNum >= vNum;
      if (cond.type === 'lte') return rNum <= vNum;
      if (cond.type === 'between') {
        const vNum2 = Number(cond.value2);
        return rNum >= vNum && rNum <= vNum2;
      }
    }
    return true;
  });

  return filterGroup.operator === 'OR' ? passes.some(p => p) : passes.every(p => p);
}

export function useFilteredData(data, filters, serverSide) {
  return useMemo(() => {
    if (serverSide) return data || [];
    if (!data || !data.length) return [];
    return data.filter(row =>
      Object.entries(filters).every(([colId, filterGroup]) =>
        evaluateFilterGroup(row[colId], filterGroup)
      )
    );
  }, [data, filters, serverSide]);
}
