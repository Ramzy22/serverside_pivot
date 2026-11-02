// @ts-check
/** @typedef {{rowIndex:number, colIndex:number, rowId?:string, colId?:string, virtualIndex?:number, scopeKey?:string}} CellSelectionAnchor */
/** @typedef {{id:string, original?:{__virtualIndex?:number}, getValue:(columnId:string)=>unknown}} SelectionRow */
/** @typedef {{id:string}} SelectionColumn */

/**
 * Positions are rendering hints only. IDs survive a server cache-window change;
 * virtualIndex orders an absent endpoint without allocating missing rows.
 * @param {SelectionRow[]} rows
 * @param {SelectionColumn[]} columns
 * @param {number} rowIndex
 * @param {number} colIndex
 * @param {string} scopeKey
 * @returns {CellSelectionAnchor}
 */
export function captureCellAnchor(rows, columns, rowIndex, colIndex, scopeKey) {
  const row = rows[rowIndex];
  return {rowIndex, colIndex, rowId:row?.id, colId:columns[colIndex]?.id,
    virtualIndex:row?.original?.__virtualIndex, scopeKey};
}

/** @param {CellSelectionAnchor} anchor @param {SelectionRow[]} rows */
export function resolveAnchorRow(anchor, rows) {
  return anchor.rowId === undefined ? anchor.rowIndex : rows.findIndex(row => row.id === anchor.rowId);
}
/** @param {CellSelectionAnchor} anchor @param {SelectionColumn[]} columns */
export function resolveAnchorColumn(anchor, columns) {
  return anchor.colId === undefined ? anchor.colIndex : columns.findIndex(column => column.id === anchor.colId);
}
/**
 * Semantic scope only: callers must not pass viewport windows or response versions.
 * @param {boolean} serverSide
 * @param {string|null} dataset
 * @param {string} rowStructureKey
 * @param {unknown} expanded
 * @param {unknown[]} viewDefinition
 */
export function cellSelectionScopeKey(serverSide, dataset, rowStructureKey, expanded, viewDefinition) {
  return JSON.stringify([serverSide, dataset, rowStructureKey, expanded, viewDefinition]);
}
