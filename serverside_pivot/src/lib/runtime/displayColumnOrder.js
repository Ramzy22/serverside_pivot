/** Rendered positional axis, including non-data gutters: left, center, right.  @param {any} [table] @returns {any} */
export function getDisplayOrderedLeafColumns(table) {
  if (!table) return [];
  /** @type {any} */
  const hasPinnedRegions = ['getLeftVisibleLeafColumns', 'getCenterVisibleLeafColumns', 'getRightVisibleLeafColumns']
    .every(method => typeof table[method] === 'function');
  return hasPinnedRegions ? [
    ...table.getLeftVisibleLeafColumns(),
    ...table.getCenterVisibleLeafColumns(),
    ...table.getRightVisibleLeafColumns(),
  ] : typeof table.getVisibleLeafColumns === 'function' ? table.getVisibleLeafColumns() : [];
}

/** A gutter occupies display space but cannot become a selected data cell.  @param {any} [column] @returns {any} */
export function isSelectableDisplayColumn(column) {
  return !!column && column.id !== '__report_config__';
}
