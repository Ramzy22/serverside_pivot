import React, { useCallback } from 'react';

/** Owns row-edit session validation, dispatch, and row-action presentation.  @param {any} p0 @returns {any} */
export function useRuntimeRowEditing({
  Icons,
  cancelRowEditSession,
  closeRowEditSession,
  dispatchBatchUpdateRequest,
  emitEditLifecycleEvent,
  getRowEditMeta,
  normalizedEditingConfig,
  rowEditSessionsRef,
  serverSide,
  theme,
  updateRowEditSessions,
  validateEditorValue,
}) {
  /** @type {(...args: any[]) => any} */
  const saveRowEditSession = useCallback(rowId => {
    if (rowId === null || rowId === undefined) return;
    /** @type {any} */
    const normalizedRowId = String(rowId);
    const session = rowEditSessionsRef.current[normalizedRowId];
    if (!session || session.status === 'saving') return;
    /** @type {any} */
    const dirtyColumns = Object.keys(session.drafts || {});
    if (dirtyColumns.length === 0) {
      closeRowEditSession(normalizedRowId);
      emitEditLifecycleEvent({ kind: 'row_edit_cancel', rowId: normalizedRowId, empty: true });
      return;
    }
    /** @type {Record<string, any>} */
    const nextErrors = {};
    /** @type {Record<string, any>} */
    const rowValues = { ...(session.originalValues || {}), ...(session.drafts || {}) };
    dirtyColumns.forEach(/** @param {any} [colId] @returns {any} */ colId => {
      /** @type {any} */
      const editorConfig = session.editableColumns && session.editableColumns[colId] ? session.editableColumns[colId].editorConfig : {};
      /** @type {any} */
      const validation = validateEditorValue(rowValues[colId], editorConfig.validationRules || [], {
        columnId: colId, columnLabel: colId, rowId: normalizedRowId, rowValues,
      });
      if (!validation.valid) nextErrors[colId] = validation.error || 'Invalid value';
    });
    if (Object.keys(nextErrors).length > 0) {
      updateRowEditSessions(/** @param {any} [previousSessions] @returns {any} */ previousSessions => {
        const previousSession = previousSessions[normalizedRowId];
        if (!previousSession) return previousSessions;
        return {
          ...previousSessions,
          [normalizedRowId]: {
            ...previousSession,
            status: 'editing',
            errors: { ...(previousSession.errors || {}), ...nextErrors },
          },
        };
      });
      emitEditLifecycleEvent({ kind: 'row_edit_validation_error', rowId: normalizedRowId, errors: nextErrors });
      return;
    }
    /** @type {any} */
    const updates = dirtyColumns.map(/** @param {any} [colId] @returns {any} */ colId => {
      /** @type {any} */
      const editableEntry = session.editableColumns && session.editableColumns[colId] ? session.editableColumns[colId] : null;
      /** @type {Record<string, any>} */
      const nextUpdate = {
        rowId: normalizedRowId,
        colId,
        value: rowValues[colId],
        oldValue: session.originalValues ? session.originalValues[colId] : undefined,
        rowPath: normalizedRowId,
        source: 'row-edit',
        timestamp: Date.now(),
      };
      if (editableEntry && editableEntry.columnConfig && editableEntry.columnConfig.field) {
        nextUpdate.aggregation = {
          field: editableEntry.columnConfig.field,
          agg: editableEntry.columnConfig.agg,
          weightField: editableEntry.columnConfig.weightField || null,
          windowFn: editableEntry.columnConfig.windowFn || null,
        };
      }
      return nextUpdate;
    });
    updateRowEditSessions(/** @param {any} [previousSessions] @returns {any} */ previousSessions => {
      const previousSession = previousSessions[normalizedRowId];
      if (!previousSession) return previousSessions;
      return { ...previousSessions, [normalizedRowId]: { ...previousSession, status: 'saving' } };
    });
    emitEditLifecycleEvent({ kind: 'row_edit_commit_start', rowId: normalizedRowId, dirtyColumns });
    /** @type {any} */
    const dispatched = dispatchBatchUpdateRequest(updates, 'row-edit', { rowEditRowId: normalizedRowId, dirtyColumns });
    if (!dispatched) {
      updateRowEditSessions(/** @param {any} [previousSessions] @returns {any} */ previousSessions => {
        const previousSession = previousSessions[normalizedRowId];
        if (!previousSession) return previousSessions;
        return { ...previousSessions, [normalizedRowId]: { ...previousSession, status: 'editing' } };
      });
      emitEditLifecycleEvent({
        kind: 'row_edit_commit_error', rowId: normalizedRowId,
        message: 'Unable to dispatch the row edit transaction.',
      });
      return;
    }
    if (!serverSide || !dispatched.requestId) {
      closeRowEditSession(normalizedRowId);
      emitEditLifecycleEvent({ kind: 'row_edit_commit_success', rowId: normalizedRowId, dirtyColumns, local: true });
    }
  }, [closeRowEditSession, dispatchBatchUpdateRequest, emitEditLifecycleEvent, rowEditSessionsRef, serverSide, updateRowEditSessions, validateEditorValue]);

  /** @type {(...args: any[]) => any} */
  const renderRowEditActions = useCallback(row => {
    if (!row || !normalizedEditingConfig.rowActions || !['row', 'hybrid'].includes(normalizedEditingConfig.mode)) return null;
    /** @type {any} */
    const meta = getRowEditMeta(row);
    if (!meta.canEdit) return null;
    /** @type {any} */
    const hasSession = Boolean(meta.session);
    /** @type {any} */
    const hasErrors = meta.errorCount > 0;
    /** @type {any} */
    const showSessionControls = hasSession && !meta.session.autoSave;
    /** @type {any} */
    const showSessionStatus = hasSession && (meta.dirtyCount > 0 || meta.saving);
    if (!showSessionControls && !showSessionStatus) return null;
    return <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px', marginLeft: '8px', flexShrink: 0 }}>
      {showSessionControls ? <>
        <button type="button" data-row-edit-action="save" data-row-edit-rowid={meta.rowId} onClick={event => {
          event.stopPropagation();
          saveRowEditSession(meta.rowId);
        }} onMouseDown={event => event.stopPropagation()} disabled={meta.saving} style={{
          border: `1px solid ${hasErrors ? '#dc2626' : theme.primary || '#2563EB'}`,
          background: hasErrors ? '#fef2f2' : theme.primary || '#2563EB',
          color: hasErrors ? '#991b1b' : '#fff', cursor: meta.saving ? 'progress' : 'pointer',
          opacity: meta.saving ? 0.7 : 1, padding: '2px 4px', borderRadius: '6px',
          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        }} title={hasErrors ? 'Fix validation errors before saving this row' : 'Save this row'}>
          <Icons.Save />
        </button>
        <button type="button" data-row-edit-action="cancel" data-row-edit-rowid={meta.rowId} onClick={event => {
          event.stopPropagation();
          cancelRowEditSession(meta.rowId);
        }} onMouseDown={event => event.stopPropagation()} style={{
          border: `1px solid ${theme.border}`, background: theme.surfaceBg, color: theme.textSec,
          cursor: 'pointer', padding: '2px 4px', borderRadius: '6px', display: 'inline-flex',
          alignItems: 'center', justifyContent: 'center',
        }} title="Cancel row editing">
          <Icons.Close />
        </button>
      </> : null}
      {showSessionStatus ? <span style={{
        fontSize: '10px', fontWeight: 700, color: hasErrors ? '#991b1b' : theme.textSec,
        background: hasErrors ? '#fee2e2' : theme.headerSubtleBg, borderRadius: '999px', padding: '1px 6px',
      }}>
        {meta.saving ? 'Saving…' : `${meta.dirtyCount} dirty${meta.errorCount ? ` • ${meta.errorCount} invalid` : ''}`}
      </span> : null}
    </span>;
  }, [Icons, cancelRowEditSession, getRowEditMeta, normalizedEditingConfig.mode, normalizedEditingConfig.rowActions, saveRowEditSession, theme.border, theme.headerSubtleBg, theme.primary, theme.surfaceBg, theme.textSec]);

  return { renderRowEditActions, saveRowEditSession };
}
