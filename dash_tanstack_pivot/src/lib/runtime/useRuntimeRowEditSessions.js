import { useCallback } from 'react';

import { cellValuesMatch } from '../hooks/usePivotNormalization';
import { validateEditorValue } from '../utils/editing';

/** Owns local row-edit session creation, validation, and cancellation. */
export function useRuntimeRowEditSessions({
  emitEditLifecycleEvent,
  isEditableAggregationColumn,
  resolveAggregationConfigForColumnId,
  resolveCurrentCellValue,
  resolveEditorPresentation,
  showNotification,
  tableRef,
  updateRowEditSessions,
  rowEditSessionsRef,
}) {
  const getRowEditSession = useCallback(rowId => {
    if (rowId === null || rowId === undefined) return null;
    return rowEditSessionsRef.current[String(rowId)] || null;
  }, [rowEditSessionsRef]);

  const closeRowEditSession = useCallback(rowId => {
    if (rowId === null || rowId === undefined) return;
    const normalizedRowId = String(rowId);
    updateRowEditSessions(previousSessions => {
      if (!previousSessions[normalizedRowId]) return previousSessions;
      const nextSessions = {
        ...previousSessions
      };
      delete nextSessions[normalizedRowId];
      return nextSessions;
    });
  }, [updateRowEditSessions]);

  const resolveRowEditableColumns = useCallback(row => {
    if (!row || !tableRef.current || typeof tableRef.current.getVisibleLeafColumns !== 'function') return [];
    const rowId = row && row.original && row.original._path ? row.original._path : row.id;
    return tableRef.current.getVisibleLeafColumns().reduce((acc, column) => {
      if (!column || !column.id) return acc;
      if (column.id === 'hierarchy' || column.id === '__row_number__' || column.id === '__report_config__' || String(column.id).startsWith('__tree_level__')) return acc;
      if (column.columnDef && column.columnDef.meta && column.columnDef.meta.isSchemaPlaceholder) return acc;
      const columnConfig = resolveAggregationConfigForColumnId(column.id);
      const currentValue = typeof row.getValue === 'function' ? row.getValue(column.id) : row.original && Object.prototype.hasOwnProperty.call(row.original, column.id) ? row.original[column.id] : undefined;
      const editorState = resolveEditorPresentation(rowId, column.id, columnConfig, currentValue, isEditableAggregationColumn(columnConfig, column.id));
      if (!editorState || !editorState.editorConfig || editorState.editorConfig.editable === false) {
        return acc;
      }
      if (editorState.editingDisabled) {
        return acc;
      }
      acc.push({
        colId: column.id,
        columnConfig,
        editorConfig: editorState.editorConfig,
        currentValue: resolveCurrentCellValue(rowId, column.id, currentValue)
      });
      return acc;
    }, []);
  }, [isEditableAggregationColumn, resolveAggregationConfigForColumnId, resolveCurrentCellValue, resolveEditorPresentation, tableRef]);

  const startRowEditSession = useCallback((row, options = {}) => {
    if (!row) return;
    const rowId = row && row.original && row.original._path ? row.original._path : row.id;
    if (!rowId) return;
    const normalizedRowId = String(rowId);
    const editableColumns = resolveRowEditableColumns(row);
    if (editableColumns.length === 0) {
      showNotification('This row does not expose editable visible values.', 'warning');
      return;
    }
    const normalizedFocusColumnId = options && options.focusColumnId !== undefined && options.focusColumnId !== null ? String(options.focusColumnId) : null;
    const editableColumnMap = editableColumns.reduce((acc, entry) => {
      acc[entry.colId] = {
        columnConfig: entry.columnConfig,
        editorConfig: entry.editorConfig
      };
      return acc;
    }, {});
    const originalValues = editableColumns.reduce((acc, entry) => {
      acc[entry.colId] = entry.currentValue;
      return acc;
    }, {});
    let createdSession = false;
    updateRowEditSessions(previousSessions => {
      const previousSession = previousSessions[normalizedRowId] || null;
      if (!previousSession) createdSession = true;
      return {
        ...previousSessions,
        [normalizedRowId]: {
          active: true,
          status: previousSession && previousSession.status === 'saving' ? 'saving' : 'editing',
          drafts: previousSession && previousSession.drafts ? {
            ...previousSession.drafts
          } : {},
          errors: previousSession && previousSession.errors ? {
            ...previousSession.errors
          } : {},
          touched: previousSession && previousSession.touched ? {
            ...previousSession.touched
          } : {},
          originalValues: {
            ...originalValues,
            ...(previousSession && previousSession.originalValues ? previousSession.originalValues : {})
          },
          editableColumns: {
            ...(previousSession && previousSession.editableColumns ? previousSession.editableColumns : {}),
            ...editableColumnMap
          },
          autoSave: Boolean(options && options.autoSave),
          focusColumnId: normalizedFocusColumnId,
          trigger: options && options.trigger ? String(options.trigger) : previousSession && previousSession.trigger ? previousSession.trigger : 'manual'
        }
      };
    });
    if (createdSession) {
      emitEditLifecycleEvent({
        kind: 'row_edit_start',
        rowId: normalizedRowId,
        columnIds: editableColumns.map(entry => entry.colId)
      });
    }
  }, [emitEditLifecycleEvent, resolveRowEditableColumns, showNotification, updateRowEditSessions]);

  const handleBlockedCellEdit = useCallback(payload => {
    if (!payload || typeof payload !== 'object') return;
    showNotification(payload.reason || 'This cell cannot be edited while an overlapping edit is active.', 'warning');
  }, [showNotification]);

  const updateRowDraftValue = useCallback((rowId, colId, nextValue, meta = {}) => {
    if (rowId === null || rowId === undefined || !colId) return;
    const normalizedRowId = String(rowId);
    const normalizedColId = String(colId);
    let emittedValidationError = null;
    updateRowEditSessions(previousSessions => {
      const session = previousSessions[normalizedRowId];
      if (!session) return previousSessions;
      const nextDrafts = {
        ...(session.drafts || {})
      };
      const nextErrors = {
        ...(session.errors || {})
      };
      const nextTouched = {
        ...(session.touched || {}),
        [normalizedColId]: true
      };
      const originalValue = session.originalValues ? session.originalValues[normalizedColId] : undefined;
      if (cellValuesMatch(nextValue, originalValue)) {
        delete nextDrafts[normalizedColId];
      } else {
        nextDrafts[normalizedColId] = nextValue;
      }
      const editorConfig = (meta && meta.editorConfig ? meta.editorConfig : session.editableColumns && session.editableColumns[normalizedColId] && session.editableColumns[normalizedColId].editorConfig) || {};
      const nextRowValues = {
        ...(session.originalValues || {}),
        ...nextDrafts,
        [normalizedColId]: Object.prototype.hasOwnProperty.call(nextDrafts, normalizedColId) ? nextDrafts[normalizedColId] : originalValue
      };
      const validation = validateEditorValue(nextRowValues[normalizedColId], editorConfig.validationRules || [], {
        columnId: normalizedColId,
        columnLabel: normalizedColId,
        rowId: normalizedRowId,
        rowValues: nextRowValues
      });
      if (!validation.valid) {
        nextErrors[normalizedColId] = validation.error || 'Invalid value';
        emittedValidationError = nextErrors[normalizedColId];
      } else {
        delete nextErrors[normalizedColId];
      }
      return {
        ...previousSessions,
        [normalizedRowId]: {
          ...session,
          status: 'editing',
          drafts: nextDrafts,
          errors: nextErrors,
          touched: nextTouched
        }
      };
    });
    if (emittedValidationError) {
      emitEditLifecycleEvent({
        kind: 'row_edit_validation_error',
        rowId: normalizedRowId,
        colId: normalizedColId,
        message: emittedValidationError
      });
    }
  }, [emitEditLifecycleEvent, updateRowEditSessions]);

  const cancelRowEditSession = useCallback(rowId => {
    if (rowId === null || rowId === undefined) return;
    closeRowEditSession(rowId);
    emitEditLifecycleEvent({
      kind: 'row_edit_cancel',
      rowId: String(rowId)
    });
  }, [closeRowEditSession, emitEditLifecycleEvent]);

  const getRowEditMeta = useCallback(row => {
    if (!row) return {
      rowId: null,
      session: null,
      canEdit: false,
      dirtyCount: 0,
      errorCount: 0,
      saving: false
    };
    const rowId = row && row.original && row.original._path ? row.original._path : row.id;
    const session = rowId ? getRowEditSession(rowId) : null;
    const editableColumns = resolveRowEditableColumns(row);
    return {
      rowId: rowId ? String(rowId) : null,
      session,
      canEdit: editableColumns.length > 0,
      dirtyCount: session ? Object.keys(session.drafts || {}).length : 0,
      errorCount: session ? Object.values(session.errors || {}).filter(Boolean).length : 0,
      saving: Boolean(session && session.status === 'saving')
    };
  }, [getRowEditSession, resolveRowEditableColumns]);

  return {
    cancelRowEditSession,
    closeRowEditSession,
    getRowEditMeta,
    getRowEditSession,
    handleBlockedCellEdit,
    resolveRowEditableColumns,
    startRowEditSession,
    updateRowDraftValue,
  };
}
