import { useCallback } from 'react';

import { GRAND_TOTAL_ROW_ID } from '../hooks/usePivotNormalization';

/** Supplies editable-column metadata and editor presentation from grid state. */
export function useRuntimeEditPresentation({
  editorOptionsLoadingRef,
  filterOptions,
  getKey,
  normalizedEditingConfig,
  resolveCellEditOwnershipState,
  resolveColumnEditSpec,
  resolveEditorOptionsSource,
  resolveVisibleDataRowId,
  serverSide,
  tableDataRef,
  valConfigs,
  validationRules,
}) {
  const getDirectVisibleChildRowIds = useCallback(ancestorRowId => {
    const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
    if (ancestorRowId === GRAND_TOTAL_ROW_ID) {
      return visibleRows
        .map(resolveVisibleDataRowId)
        .filter(rowId => typeof rowId === 'string' && rowId !== GRAND_TOTAL_ROW_ID && !rowId.includes('|||'));
    }
    const normalizedAncestorId = ancestorRowId === null || ancestorRowId === undefined ? '' : String(ancestorRowId);
    if (!normalizedAncestorId) return [];
    const ancestorDepth = normalizedAncestorId.split('|||').length;
    const directChildPrefix = `${normalizedAncestorId}|||`;
    return visibleRows
      .map(resolveVisibleDataRowId)
      .filter(rowId => typeof rowId === 'string' && rowId.startsWith(directChildPrefix) && rowId.split('|||').length === ancestorDepth + 1);
  }, [resolveVisibleDataRowId, tableDataRef]);

  const resolveAggregationConfigForColumnId = useCallback(columnId => {
    if (!Array.isArray(valConfigs) || !columnId) return null;
    const normalizedId = String(columnId).trim().toLowerCase();
    if (!normalizedId) return null;
    const matchedConfig = valConfigs.find(config => {
      if (!config || typeof config !== 'object' || !config.field) return false;
      if (config.agg === 'formula') {
        const formulaId = String(config.field).trim().toLowerCase();
        return normalizedId === formulaId || normalizedId.endsWith(`_${formulaId}`);
      }
      const measureId = String(config.alias || config.id || getKey('', config.field, config.agg)).trim().toLowerCase();
      const measureSuffix = `_${config.field}_${config.agg}`.toLowerCase();
      return normalizedId === measureId || normalizedId.endsWith(`_${measureId}`) || normalizedId.endsWith(measureSuffix);
    }) || null;
    if (!matchedConfig) return null;
    return {
      field: matchedConfig.field || null,
      agg: matchedConfig.agg || null,
      weightField: matchedConfig.weightField || null,
      windowFn: matchedConfig.windowFn || null,
    };
  }, [getKey, valConfigs]);

  const isEditableAggregationColumn = useCallback((config, columnId) => {
    if (!config || typeof config !== 'object') return false;
    if (typeof columnId === 'string' && columnId.startsWith('__RowTotal__')) return false;
    const normalizedAgg = String(config.agg || '').trim().toLowerCase();
    if (!normalizedAgg || normalizedAgg === 'formula' || config.windowFn) return false;
    return !['count', 'count_distinct', 'distinct_count'].includes(normalizedAgg);
  }, []);

  const resolveEditorPresentation = useCallback((rowId, columnId, columnConfig = null, currentValue = undefined, defaultEditable = false) => {
    const editorConfig = resolveColumnEditSpec({
      editingConfig: normalizedEditingConfig,
      validationRules,
      columnId,
      columnConfig,
      currentValue,
      defaultEditable,
    });
    if (!editorConfig || editorConfig.editable === false) return null;
    const ownershipState = serverSide && rowId !== null && rowId !== undefined && columnId !== null && columnId !== undefined
      ? resolveCellEditOwnershipState(String(rowId), String(columnId))
      : { mode: 'apply', targetEventIds: [], reason: null };
    const optionsSource = editorConfig.optionsSource || resolveEditorOptionsSource(editorConfig, columnId, columnConfig);
    const optionsKey = optionsSource && optionsSource.columnId ? optionsSource.columnId : null;
    const options = optionsKey && Array.isArray(filterOptions[optionsKey])
      ? filterOptions[optionsKey]
      : Array.isArray(editorConfig.options) ? editorConfig.options : [];
    const loading = Boolean(optionsKey && editorOptionsLoadingRef.current[optionsKey]);
    return {
      editorConfig: { ...editorConfig, optionsSource, options, editOwnershipMode: ownershipState.mode, replaceEventIds: ownershipState.targetEventIds },
      options,
      loading,
      optionsKey,
      editingDisabled: ownershipState.mode === 'blocked',
      editingDisabledReason: ownershipState.reason,
    };
  }, [editorOptionsLoadingRef, filterOptions, normalizedEditingConfig, resolveCellEditOwnershipState, resolveColumnEditSpec, resolveEditorOptionsSource, serverSide, validationRules]);

  return { getDirectVisibleChildRowIds, isEditableAggregationColumn, resolveAggregationConfigForColumnId, resolveEditorPresentation };
}
