import { useCallback, useMemo } from 'react';

const COLOR_PALETTES = {
  redGreen: { low: [248, 105, 107], high: [99, 190, 123], darkLow: '#5c0001', darkHigh: '#1a4d2a' },
  greenRed: { low: [99, 190, 123], high: [248, 105, 107], darkLow: '#1a4d2a', darkHigh: '#5c0001' },
  blueWhite: { low: [65, 105, 225], high: [255, 255, 255], darkLow: '#0d2b6e', darkHigh: '#333' },
  yellowBlue: { low: [255, 220, 0], high: [30, 90, 200], darkLow: '#5a4700', darkHigh: '#0a2a6e' },
  orangePurp: { low: [255, 140, 0], high: [120, 50, 200], darkLow: '#5c3000', darkHigh: '#2a0060' },
};

/** Owns client/server color-scale statistics and cell presentation. */
export function useRuntimeColorScalePresentation({
  colorPalette,
  colorScaleMode,
  colorScaleColumns = null,
  colFields,
  filteredData,
  getRuleBasedStyle,
  nodes,
  rowFields,
  serverColorScaleStats,
}) {
  const scopedColumns = useMemo(() => Array.isArray(colorScaleColumns) ? new Set(colorScaleColumns) : null, [colorScaleColumns]);
  const clientColorScaleStats = useMemo(() => {
    const metaKeys = new Set(['_id', '_path', '_isTotal', 'depth', '_depth', '_level', '_expanded', '_parentPath', '_has_children', '_is_expanded', 'subRows', 'uuid', '__virtualIndex', '__colPending', '__isGrandTotal__']);
    rowFields.forEach(field => metaKeys.add(field));
    colFields.forEach(field => metaKeys.add(field));
    const byCol = {};
    const byRow = {};
    let tableMin = Number.POSITIVE_INFINITY;
    let tableMax = Number.NEGATIVE_INFINITY;
    const sourceData = filteredData.length > 0 ? filteredData : nodes;
    sourceData.forEach((row, index) => {
      if (!row || typeof row !== 'object' || row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total') return;
      const rowKey = String(row._path || row._id || index);
      let rowMin = Number.POSITIVE_INFINITY;
      let rowMax = Number.NEGATIVE_INFINITY;
      Object.entries(row).forEach(([key, raw]) => {
        if (metaKeys.has(key) || typeof raw !== 'number' || Number.isNaN(raw)) return;
        if (!byCol[key]) byCol[key] = { min: raw, max: raw };
        else {
          byCol[key].min = Math.min(byCol[key].min, raw);
          byCol[key].max = Math.max(byCol[key].max, raw);
        }
        rowMin = Math.min(rowMin, raw);
        rowMax = Math.max(rowMax, raw);
        tableMin = Math.min(tableMin, raw);
        tableMax = Math.max(tableMax, raw);
      });
      if (Number.isFinite(rowMin) && Number.isFinite(rowMax)) byRow[rowKey] = { min: rowMin, max: rowMax };
    });
    return {
      byCol,
      byRow,
      table: Number.isFinite(tableMin) && Number.isFinite(tableMax) ? { min: tableMin, max: tableMax } : null,
    };
  }, [colFields, filteredData, nodes, rowFields]);

  const colorScaleStats = useMemo(() => {
    if (!serverColorScaleStats || !clientColorScaleStats) return serverColorScaleStats || clientColorScaleStats;
    return {
      byCol: { ...clientColorScaleStats.byCol, ...serverColorScaleStats.byCol },
      byRow: { ...clientColorScaleStats.byRow, ...(serverColorScaleStats.byRow || {}) },
      table: serverColorScaleStats.table || clientColorScaleStats.table,
    };
  }, [clientColorScaleStats, serverColorScaleStats]);

  const getConditionalStyle = useCallback((colId, value, rowData, rowId) => {
    const ruleStyle = getRuleBasedStyle(colId, value);
    if (colorScaleMode === 'off' || typeof value !== 'number' || Number.isNaN(value)) return ruleStyle;
    if (scopedColumns && !scopedColumns.has(colId)) return ruleStyle;
    if (rowData && (rowData._isTotal || rowData._path === '__grand_total__' || rowData._id === 'Grand Total')) return ruleStyle;
    let stats = null;
    if (colorScaleMode === 'col') stats = colorScaleStats.byCol && colorScaleStats.byCol[colId] || null;
    else if (colorScaleMode === 'row') stats = colorScaleStats.byRow && colorScaleStats.byRow[String(rowData && rowData._path || rowId || '')] || null;
    else if (colorScaleMode === 'table') stats = colorScaleStats.table || null;
    if (!stats || !Number.isFinite(stats.min) || !Number.isFinite(stats.max) || stats.max === stats.min) return ruleStyle;
    const palette = COLOR_PALETTES[colorPalette] || COLOR_PALETTES.redGreen;
    const { low, high } = palette;
    const hasZeroCrossing = stats.min < 0 && stats.max > 0;
    const position = hasZeroCrossing ? value <= 0 ? 0.5 * (value - stats.min) / (0 - stats.min) : 0.5 + 0.5 * value / stats.max : (value - stats.min) / (stats.max - stats.min);
    const clamped = Math.max(0, Math.min(1, position));
    const alpha = 0.06 + Math.abs(clamped - 0.5) * 2 * 0.76;
    const [red, green, blue] = clamped <= 0.5 ? low : high;
    const luminance = (0.299 * red + 0.587 * green + 0.114 * blue) / 255;
    return {
      background: `rgba(${red},${green},${blue},${alpha.toFixed(3)})`,
      color: alpha > 0.5 ? luminance > 0.45 ? '#111827' : '#ffffff' : undefined,
      ...ruleStyle,
    };
  }, [colorPalette, colorScaleMode, colorScaleStats, getRuleBasedStyle, scopedColumns]);

  return { COLOR_PALETTES, clientColorScaleStats, colorScaleStats, getConditionalStyle };
}
