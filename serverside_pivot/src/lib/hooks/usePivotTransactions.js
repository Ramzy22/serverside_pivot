import { useCallback, useRef, useState } from 'react';

/**
 * Owns transaction history and the server-side transaction lifecycle.
 *
 * The composition root supplies the runtime transport and editing-specific
 * operations through `configure`. Keeping those dependencies explicit avoids
 * a second owner for row caches, selection, or edit overlays.
  @returns {any} */
export function usePivotTransactions() {
    /** @type {any} */
    const contextRef = useRef({});
    /** @type {any} */
    const undoStackRef = useRef([]);
    /** @type {any} */
    const redoStackRef = useRef([]);
    /** @type {any} */
    const pendingHistoryRef = useRef(new Map());
    /** @type {any} */
    const pendingHistoryRequestRef = useRef(null);
    /** @type {any} */
    const editSessionStateRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [transactionHistoryState, setTransactionHistoryState] = useState({
        undoCount: 0,
        redoCount: 0,
        pending: false,
    });

    /** @type {(...args: any[]) => any} */
    const configure = useCallback((context) => {
        contextRef.current = context && typeof context === 'object' ? context : {};
    }, []);

    /** @type {(...args: any[]) => any} */
    const syncTransactionHistoryState = useCallback(() => {
        setTransactionHistoryState({
            undoCount: undoStackRef.current.length,
            redoCount: redoStackRef.current.length,
            pending: Boolean(pendingHistoryRequestRef.current),
        });
    }, []);

    /** @type {(...args: any[]) => any} */
    const normalizeEventIds = useCallback((eventIds) => {
        const { normalizeTrackedEventIds } = contextRef.current;
        return typeof normalizeTrackedEventIds === 'function'
            ? normalizeTrackedEventIds(eventIds)
            : [];
    }, []);

    /** @type {(...args: any[]) => any} */
    const findTransactionHistoryEntryByEventId = useCallback((eventId) => {
        /** @type {any} */
        const normalizedEventId = String(eventId || '').trim();
        if (!normalizedEventId) return null;
        /** @param {any} [entries] @returns {any} */
        const findInEntries = (entries) => (
            Array.isArray(entries)
                ? entries.find((entry) => entry && entry.kind === 'transaction' && entry.eventId === normalizedEventId) || null
                : null
        );
        return findInEntries(undoStackRef.current) || findInEntries(redoStackRef.current);
    }, []);

    /** @type {(...args: any[]) => any} */
    const sortEventIdsByActiveHistory = useCallback((eventIds) => {
        /** @type {any} */
        const normalizedEventIds = normalizeEventIds(eventIds);
        if (normalizedEventIds.length <= 1) return normalizedEventIds;
        /** @type {any} */
        const orderMap = new Map();
        undoStackRef.current.forEach(/** @param {any} [entry] @param {any} [index] @returns {any} */ (entry, index) => {
            if (!entry || entry.kind !== 'transaction' || !entry.eventId) return;
            orderMap.set(entry.eventId, index);
        });
        return normalizedEventIds.sort(/** @param {any} [left] @param {any} [right] @returns {any} */ (left, right) => {
            /** @type {any} */
            const leftIndex = orderMap.has(left) ? orderMap.get(left) : -1;
            /** @type {any} */
            const rightIndex = orderMap.has(right) ? orderMap.get(right) : -1;
            return rightIndex - leftIndex;
        });
    }, [normalizeEventIds]);

    /** @type {(...args: any[]) => any} */
    const pruneTransactionHistoryEntries = useCallback((eventIds) => {
        /** @type {any} */
        const normalizedEventIds = new Set(normalizeEventIds(eventIds));
        if (normalizedEventIds.size === 0) return;
        undoStackRef.current = undoStackRef.current.filter(/** @param {any} [entry] @returns {any} */ (entry) => (
            !entry || entry.kind !== 'transaction' || !normalizedEventIds.has(entry.eventId)
        ));
        redoStackRef.current = redoStackRef.current.filter(/** @param {any} [entry] @returns {any} */ (entry) => (
            !entry || entry.kind !== 'transaction' || !normalizedEventIds.has(entry.eventId)
        ));
        syncTransactionHistoryState();
    }, [normalizeEventIds, syncTransactionHistoryState]);

    /** @type {(...args: any[]) => any} */
    const pushUnifiedHistoryEntry = useCallback((entry) => {
        if (!entry || typeof entry !== 'object') return;
        /** @type {any} */
        const limit = Number(contextRef.current.maxHistoryEntries) || 100;
        /** @type {any[]} */
        const nextUndoStack = [...undoStackRef.current, entry];
        if (nextUndoStack.length > limit) {
            nextUndoStack.splice(0, nextUndoStack.length - limit);
        }
        undoStackRef.current = nextUndoStack;
        redoStackRef.current = [];
        syncTransactionHistoryState();
    }, [syncTransactionHistoryState]);

    /** @type {(...args: any[]) => any} */
    const queueTransactionHistoryRequest = useCallback((requestId, entry, options = {}) => {
        if (!requestId || !entry || typeof entry !== 'object') return false;
        pendingHistoryRef.current.set(requestId, entry);
        if (options.blocksHistory === true) {
            pendingHistoryRequestRef.current = requestId;
        }
        syncTransactionHistoryState();
        return true;
    }, [syncTransactionHistoryState]);

    /** @type {(...args: any[]) => any} */
    const clearTransactionHistory = useCallback(() => {
        const {
            sessionIdRef,
            clientInstanceRef,
            clearAllOptimisticCellValues,
            clearAllEditedCellMarkers,
            clearAllComparisonValueState,
            clearPendingLayoutHistoryCapture,
        } = contextRef.current;
        undoStackRef.current = [];
        redoStackRef.current = [];
        pendingHistoryRef.current.clear();
        pendingHistoryRequestRef.current = null;
        editSessionStateRef.current = {
            sessionId: sessionIdRef && sessionIdRef.current,
            clientInstance: clientInstanceRef && clientInstanceRef.current,
            sessionKey: null,
            sessionVersion: 0,
        };
        if (typeof clearAllOptimisticCellValues === 'function') clearAllOptimisticCellValues();
        if (typeof clearAllEditedCellMarkers === 'function') clearAllEditedCellMarkers();
        if (typeof clearAllComparisonValueState === 'function') clearAllComparisonValueState();
        if (typeof clearPendingLayoutHistoryCapture === 'function') clearPendingLayoutHistoryCapture();
        syncTransactionHistoryState();
    }, [syncTransactionHistoryState]);

    /** @type {(...args: any[]) => any} */
    const dispatchTransactionRequest = useCallback((transactionPayload, source = 'transaction') => {
        const {
            normalizeTransactionHistoryPayload,
            normalizeTransactionRefreshModeValue,
            shouldShowTransactionLoading,
            setPropsRef,
            serverSide,
            showNotification,
            visiblePatchRowPathsRef,
            visiblePatchCenterIdsRef,
            colFields,
            latestRequestedViewportRef,
            latestViewportRef,
            defaultServerSideBlockSize,
            resolveStableRequestedColumnWindow,
            beginExpansionRequest,
            markRequestPending,
            emitRuntimeRequest,
            tableName,
            sessionIdRef,
            clientInstanceRef,
            serverSidePinsGrandTotal,
            immersiveMode,
        } = contextRef.current;
        /** @type {any} */
        let normalizedTransaction = typeof normalizeTransactionHistoryPayload === 'function'
            ? normalizeTransactionHistoryPayload(transactionPayload, { source })
            : null;
        if (!normalizedTransaction || !setPropsRef || !setPropsRef.current || !serverSide) return null;
        if (pendingHistoryRequestRef.current) {
            if (typeof showNotification === 'function') {
                showNotification('Wait for the current edit-history action to finish before applying another edit.', 'warning');
            }
            return null;
        }
        /** @type {any} */
        const requestedRefreshMode = typeof normalizeTransactionRefreshModeValue === 'function'
            ? normalizeTransactionRefreshModeValue(normalizedTransaction.refreshMode, 'smart')
            : normalizedTransaction.refreshMode;
        /** @type {any} */
        const visibleRowPaths = visiblePatchRowPathsRef && Array.isArray(visiblePatchRowPathsRef.current)
            ? visiblePatchRowPathsRef.current
            : [];
        /** @type {any} */
        const visibleCenterIds = visiblePatchCenterIdsRef && Array.isArray(visiblePatchCenterIdsRef.current)
            ? visiblePatchCenterIdsRef.current
            : [];
        const canUsePatchRefresh = (
            requestedRefreshMode === 'patch'
            && visibleRowPaths.length > 0
            && (!(Array.isArray(colFields) ? colFields.length : 0) || visibleCenterIds.length > 0)
        );
        if (requestedRefreshMode === 'patch' && !canUsePatchRefresh) {
            normalizedTransaction = {
                ...normalizedTransaction,
                refreshMode: 'viewport',
            };
        }
        /** @type {any} */
        const showGlobalLoading = typeof shouldShowTransactionLoading === 'function'
            ? shouldShowTransactionLoading(normalizedTransaction)
            : true;
        /** @type {any} */
        const blockSize = Number(defaultServerSideBlockSize) || 48;
        /** @type {any} */
        const viewportSnapshot = (latestRequestedViewportRef && latestRequestedViewportRef.current)
            || (latestViewportRef && latestViewportRef.current)
            || { start: 0, end: blockSize - 1, count: blockSize };
        /** @type {any} */
        const nextStart = Math.max(0, Number.isFinite(Number(viewportSnapshot.start)) ? Number(viewportSnapshot.start) : 0);
        /** @type {any} */
        const nextEndCandidate = Number.isFinite(Number(viewportSnapshot.end))
            ? Number(viewportSnapshot.end)
            : (nextStart + Math.max(1, Number(viewportSnapshot.count) || blockSize) - 1);
        /** @type {any} */
        const nextEnd = Math.max(nextStart, nextEndCandidate);
        /** @type {any} */
        const nextCount = Math.max(1, nextEnd - nextStart + 1);
        /** @type {any} */
        const updateColumnWindow = typeof resolveStableRequestedColumnWindow === 'function'
            ? resolveStableRequestedColumnWindow()
            : { start: null, end: null };
        /** @type {any} */
        const tx = typeof beginExpansionRequest === 'function' ? beginExpansionRequest() : null;
        if (!tx || typeof emitRuntimeRequest !== 'function' || typeof markRequestPending !== 'function') return null;
        const requestId = `${source}:${tx.version}`;
        markRequestPending({
            version: tx.version,
            reqStart: nextStart,
            reqEnd: nextEnd,
            colStart: updateColumnWindow.start,
            colEnd: updateColumnWindow.end,
            silent: !showGlobalLoading,
        });
        emitRuntimeRequest('transaction', {
            table: tableName || undefined,
            start: nextStart,
            end: nextEnd,
            count: nextCount,
            window_seq: tx.version,
            version: tx.version,
            requestId,
            state_epoch: tx.stateEpoch,
            session_id: sessionIdRef && sessionIdRef.current,
            client_instance: clientInstanceRef && clientInstanceRef.current,
            abort_generation: tx.abortGeneration,
            intent: 'viewport',
            col_start: updateColumnWindow.start !== null ? updateColumnWindow.start : undefined,
            col_end: updateColumnWindow.end !== null ? updateColumnWindow.end : undefined,
            include_grand_total: serverSidePinsGrandTotal || undefined,
            immersive_mode: immersiveMode || undefined,
            ...normalizedTransaction,
            ...(normalizedTransaction.refreshMode === 'patch' ? {
                visibleRowPaths,
                visibleCenterColumnIds: visibleCenterIds,
            } : {}),
        });
        return { requestId };
    }, []);

    /** @type {(...args: any[]) => any} */
    const finalizeTransactionHistoryResponse = useCallback((response, payload) => {
        if (!response || typeof response !== 'object') return;
        /** @type {any} */
        const requestId = response.requestId || null;
        if (!requestId) return;
        /** @type {any} */
        const pendingEntry = pendingHistoryRef.current.get(requestId);
        if (!pendingEntry) return;
        pendingHistoryRef.current.delete(requestId);

        const context = contextRef.current;
        /** @type {any} */
        const transactionResult = payload && typeof payload === 'object' && payload.transaction && typeof payload.transaction === 'object'
            ? payload.transaction
            : {};
        /** @type {any} */
        const didApplyWork = typeof context.hasAppliedTransactionWork === 'function'
            ? context.hasAppliedTransactionWork(transactionResult)
            : false;
        /** @type {any} */
        const transactionWarnings = Array.isArray(transactionResult.warnings) ? transactionResult.warnings : [];
        /** @type {any} */
        const propagationEntries = Array.isArray(transactionResult.propagation) ? transactionResult.propagation : [];
        /** @type {any} */
        const responseAffectedCells = transactionResult.affectedCells && typeof transactionResult.affectedCells === 'object'
            ? transactionResult.affectedCells
            : null;
        /** @type {any} */
        const responseEditSession = transactionResult.editSession && typeof transactionResult.editSession === 'object'
            ? transactionResult.editSession
            : null;
        if (responseEditSession) {
            editSessionStateRef.current = {
                sessionId: responseEditSession.sessionId || (context.sessionIdRef && context.sessionIdRef.current),
                clientInstance: responseEditSession.clientInstance || (context.clientInstanceRef && context.clientInstanceRef.current),
                sessionKey: responseEditSession.sessionKey || null,
                sessionVersion: Number(responseEditSession.sessionVersion) || 0,
            };
        }

        /** @returns {any} */
        const resolveTargetHistoryEntries = () => {
            /** @type {any} */
            const explicitEntries = Array.isArray(pendingEntry.historyEntries)
                ? pendingEntry.historyEntries.filter(Boolean)
                : [];
            if (explicitEntries.length > 0) return explicitEntries;
            /** @type {any} */
            const targetEventIds = Array.isArray(pendingEntry.targetEventIds) ? pendingEntry.targetEventIds : [];
            return targetEventIds.map(findTransactionHistoryEntryByEventId).filter(Boolean);
        };
        /** @param {any} [historyEntries] @returns {any} */
        const rollbackHistoryEntries = (historyEntries) => {
            (Array.isArray(historyEntries) ? historyEntries : []).forEach((entry) => {
                if (!entry || typeof entry !== 'object') return;
                if (entry.editedMarkerPlan && typeof context.applyEditedCellMarkerPlan === 'function') {
                    context.applyEditedCellMarkerPlan(entry.editedMarkerPlan, 'backward');
                }
                if (entry.comparisonPlan && typeof context.applyComparisonValuePlan === 'function') {
                    context.applyComparisonValuePlan(entry.comparisonPlan, 'backward');
                }
                if (entry.eventId && typeof context.detachTrackedEventOwnership === 'function') {
                    context.detachTrackedEventOwnership([entry.eventId]);
                }
            });
        };
        /** @param {any} [entry] @returns {any} */
        const pushCapturedHistoryEntry = (entry) => {
            if (!entry) return;
            pushUnifiedHistoryEntry(entry);
            if (entry.eventId && entry.affectedCells && typeof context.appendTrackedEventOwnership === 'function') {
                context.appendTrackedEventOwnership(entry.eventId, entry.affectedCells);
            }
        };
        /** @param {any} [message] @returns {any} */
        const restoreRowEditSession = (message) => {
            if (!pendingEntry.rowEditRowId || typeof context.updateRowEditSessions !== 'function') return;
            context.updateRowEditSessions(/** @param {any} [previousSessions] @returns {any} */ (previousSessions) => {
                const previousSession = previousSessions[pendingEntry.rowEditRowId];
                if (!previousSession) return previousSessions;
                return {
                    ...previousSessions,
                    [pendingEntry.rowEditRowId]: { ...previousSession, status: 'editing' },
                };
            });
            if (typeof context.emitEditLifecycleEvent === 'function') {
                context.emitEditLifecycleEvent({
                    kind: 'row_edit_commit_error',
                    rowId: pendingEntry.rowEditRowId,
                    message,
                });
            }
        };
        /** @returns {any} */
        const releaseRequest = () => {
            if (response.status === 'data' && typeof context.releaseOptimisticCellRequest === 'function') {
                context.releaseOptimisticCellRequest(requestId);
            }
        };
        /** @param {any} [message] @param {any} [level] @returns {any} */
        const notify = (message, level) => {
            if (typeof context.showNotification === 'function') context.showNotification(message, level);
        };

        if (pendingEntry.action === 'apply') {
            /** @type {any} */
            const captureable = Boolean(transactionResult.history && transactionResult.history.captureable);
            /** @type {any} */
            const undoTransaction = typeof context.normalizeTransactionHistoryPayload === 'function'
                ? context.normalizeTransactionHistoryPayload(transactionResult.inverseTransaction)
                : null;
            /** @type {any} */
            const redoTransaction = typeof context.normalizeTransactionHistoryPayload === 'function'
                ? context.normalizeTransactionHistoryPayload(transactionResult.redoTransaction)
                : null;
            releaseRequest();
            if (!didApplyWork && pendingEntry.editedMarkerPlan && typeof context.applyEditedCellMarkerPlan === 'function') {
                context.applyEditedCellMarkerPlan(pendingEntry.editedMarkerPlan, 'backward');
            }
            if (!didApplyWork && pendingEntry.comparisonPlan && typeof context.applyComparisonValuePlan === 'function') {
                context.applyComparisonValuePlan(pendingEntry.comparisonPlan, 'backward');
            }
            if (didApplyWork && captureable && undoTransaction && redoTransaction) {
                pushCapturedHistoryEntry({
                    kind: 'transaction', undoTransaction, redoTransaction,
                    editedMarkerPlan: pendingEntry.editedMarkerPlan || null,
                    comparisonPlan: pendingEntry.comparisonPlan || null,
                    source: pendingEntry.source || transactionResult.source || 'transaction',
                    createdAt: Date.now(), eventId: transactionResult.eventId || null,
                    sessionVersion: Number(transactionResult.sessionVersion) || 0,
                    affectedCells: responseAffectedCells,
                    impactedScopeIds: Array.isArray(transactionResult.impactedScopeIds) ? transactionResult.impactedScopeIds : [],
                });
            } else if (didApplyWork && transactionResult.history && Array.isArray(transactionResult.history.warnings) && transactionResult.history.warnings[0]) {
                notify(transactionResult.history.warnings[0], 'warning');
            } else if (!didApplyWork && transactionWarnings[0]) {
                notify(transactionWarnings[0], 'warning');
            }
            if (didApplyWork && propagationEntries.length > 0 && typeof context.recordPropagationEntries === 'function') {
                context.recordPropagationEntries(propagationEntries);
            }
            if (pendingEntry.rowEditRowId) {
                if (didApplyWork) {
                    if (typeof context.closeRowEditSession === 'function') context.closeRowEditSession(pendingEntry.rowEditRowId);
                    if (typeof context.emitEditLifecycleEvent === 'function') {
                        context.emitEditLifecycleEvent({ kind: 'row_edit_commit_success', rowId: pendingEntry.rowEditRowId, dirtyColumns: pendingEntry.dirtyColumns || [] });
                    }
                } else {
                    restoreRowEditSession(transactionWarnings[0] || 'The row edit did not apply.');
                }
            }
            return;
        }

        if (pendingEntry.action === 'undo' || pendingEntry.action === 'redo') {
            releaseRequest();
            /** @type {any} */
            const isUndo = pendingEntry.action === 'undo';
            if (didApplyWork && pendingEntry.entry && pendingEntry.entry.editedMarkerPlan && typeof context.applyEditedCellMarkerPlan === 'function') {
                context.applyEditedCellMarkerPlan(pendingEntry.entry.editedMarkerPlan, isUndo ? 'backward' : 'forward');
            }
            if (((isUndo && didApplyWork) || (!isUndo && !didApplyWork)) && pendingEntry.entry && pendingEntry.entry.comparisonPlan && typeof context.applyComparisonValuePlan === 'function') {
                context.applyComparisonValuePlan(pendingEntry.entry.comparisonPlan, isUndo ? 'backward' : 'backward');
            }
            if (isUndo && didApplyWork && pendingEntry.entry && pendingEntry.entry.eventId && typeof context.detachTrackedEventOwnership === 'function') {
                context.detachTrackedEventOwnership([pendingEntry.entry.eventId]);
            }
            if (!isUndo && didApplyWork && pendingEntry.entry && pendingEntry.entry.eventId && pendingEntry.entry.affectedCells && typeof context.appendTrackedEventOwnership === 'function') {
                context.appendTrackedEventOwnership(pendingEntry.entry.eventId, pendingEntry.entry.affectedCells);
            }
            if (didApplyWork) {
                if (isUndo) {
                    undoStackRef.current = undoStackRef.current.slice(0, -1);
                    redoStackRef.current = [...redoStackRef.current, pendingEntry.entry];
                } else {
                    redoStackRef.current = redoStackRef.current.slice(0, -1);
                    undoStackRef.current = [...undoStackRef.current, pendingEntry.entry];
                }
            }
            pendingHistoryRequestRef.current = null;
            syncTransactionHistoryState();
            if (didApplyWork) notify(isUndo ? 'Undid the last transaction.' : 'Reapplied the last transaction.', 'success');
            else if (transactionWarnings[0]) notify(transactionWarnings[0], 'warning');
            return;
        }

        if (pendingEntry.action === 'revert' || pendingEntry.action === 'replace') {
            releaseRequest();
            if (didApplyWork) {
                /** @type {any} */
                const targetEntries = resolveTargetHistoryEntries();
                if (targetEntries.length > 0) rollbackHistoryEntries(targetEntries);
                else if (Array.isArray(pendingEntry.targetEventIds) && pendingEntry.targetEventIds.length > 0 && typeof context.detachTrackedEventOwnership === 'function') {
                    context.detachTrackedEventOwnership(pendingEntry.targetEventIds, { adjustCounts: true });
                }
                if (Array.isArray(pendingEntry.targetEventIds) && pendingEntry.targetEventIds.length > 0) {
                    pruneTransactionHistoryEntries(pendingEntry.targetEventIds);
                }
                if (pendingEntry.action === 'replace') {
                    if (pendingEntry.comparisonPlan && typeof context.applyComparisonValuePlan === 'function') context.applyComparisonValuePlan(pendingEntry.comparisonPlan, 'forward');
                    if (pendingEntry.editedMarkerPlan && typeof context.applyEditedCellMarkerPlan === 'function') context.applyEditedCellMarkerPlan(pendingEntry.editedMarkerPlan, 'forward');
                    /** @type {any} */
                    const captureable = Boolean(transactionResult.history && transactionResult.history.captureable);
                    /** @type {any} */
                    const undoTransaction = typeof context.normalizeTransactionHistoryPayload === 'function' ? context.normalizeTransactionHistoryPayload(transactionResult.inverseTransaction) : null;
                    /** @type {any} */
                    const redoTransaction = typeof context.normalizeTransactionHistoryPayload === 'function' ? context.normalizeTransactionHistoryPayload(transactionResult.redoTransaction) : null;
                    if (captureable && undoTransaction && redoTransaction) {
                        pushCapturedHistoryEntry({
                            kind: 'transaction', undoTransaction, redoTransaction,
                            editedMarkerPlan: pendingEntry.editedMarkerPlan || null,
                            comparisonPlan: pendingEntry.comparisonPlan || null,
                            source: pendingEntry.source || transactionResult.source || 'replace',
                            createdAt: Date.now(), eventId: transactionResult.eventId || null,
                            sessionVersion: Number(transactionResult.sessionVersion) || 0,
                            affectedCells: responseAffectedCells,
                            impactedScopeIds: Array.isArray(transactionResult.impactedScopeIds) ? transactionResult.impactedScopeIds : [],
                        });
                    }
                    if (pendingEntry.rowEditRowId) {
                        if (typeof context.closeRowEditSession === 'function') context.closeRowEditSession(pendingEntry.rowEditRowId);
                        if (typeof context.emitEditLifecycleEvent === 'function') {
                            context.emitEditLifecycleEvent({ kind: 'row_edit_commit_success', rowId: pendingEntry.rowEditRowId, dirtyColumns: pendingEntry.dirtyColumns || [] });
                        }
                    }
                }
                if (pendingEntry.clearAllOnSuccess) {
                    if (typeof context.clearAllOptimisticCellValues === 'function') context.clearAllOptimisticCellValues();
                    if (typeof context.clearAllEditedCellMarkers === 'function') context.clearAllEditedCellMarkers();
                    if (typeof context.clearAllComparisonValueState === 'function') context.clearAllComparisonValueState();
                    if (typeof context.clearPropagationLog === 'function') context.clearPropagationLog();
                }
                notify(pendingEntry.successMessage || (pendingEntry.action === 'replace' ? 'Updated the selected edit.' : 'Reverted the selected edits.'), 'success');
            } else if (transactionWarnings[0]) {
                restoreRowEditSession(transactionWarnings[0]);
                notify(transactionWarnings[0], 'warning');
            } else if (pendingEntry.rowEditRowId) {
                restoreRowEditSession('The row edit did not apply.');
            }
            pendingHistoryRequestRef.current = null;
            syncTransactionHistoryState();
        }
    }, [findTransactionHistoryEntryByEventId, pruneTransactionHistoryEntries, pushUnifiedHistoryEntry, syncTransactionHistoryState]);

    /** @type {(...args: any[]) => any} */
    const clearPendingTransactionHistoryRequest = useCallback((response, notifyOnError = false) => {
        if (!response || typeof response !== 'object') return;
        /** @type {any} */
        const requestId = response.requestId || null;
        if (!requestId) return;
        /** @type {any} */
        const pendingEntry = pendingHistoryRef.current.get(requestId);
        if (!pendingEntry) return;
        pendingHistoryRef.current.delete(requestId);
        const context = contextRef.current;
        if (typeof context.clearOptimisticCellValuesForRequest === 'function') context.clearOptimisticCellValuesForRequest(requestId);
        if (pendingEntry.action === 'apply' && pendingEntry.editedMarkerPlan && typeof context.applyEditedCellMarkerPlan === 'function') {
            context.applyEditedCellMarkerPlan(pendingEntry.editedMarkerPlan, 'backward');
        }
        if ((pendingEntry.action === 'apply' || pendingEntry.action === 'redo') && pendingEntry.comparisonPlan && typeof context.applyComparisonValuePlan === 'function') {
            context.applyComparisonValuePlan(pendingEntry.comparisonPlan, 'backward');
        }
        if ((pendingEntry.action === 'apply' || pendingEntry.action === 'replace') && pendingEntry.rowEditRowId && typeof context.updateRowEditSessions === 'function') {
            context.updateRowEditSessions(/** @param {any} [previousSessions] @returns {any} */ (previousSessions) => {
                const previousSession = previousSessions[pendingEntry.rowEditRowId];
                if (!previousSession) return previousSessions;
                return { ...previousSessions, [pendingEntry.rowEditRowId]: { ...previousSession, status: 'editing' } };
            });
            if (typeof context.emitEditLifecycleEvent === 'function') {
                context.emitEditLifecycleEvent({ kind: 'row_edit_commit_error', rowId: pendingEntry.rowEditRowId, message: response.message || 'Unable to commit the row edit.' });
            }
        }
        if (['undo', 'redo', 'revert', 'replace'].includes(pendingEntry.action)) {
            pendingHistoryRequestRef.current = null;
            syncTransactionHistoryState();
            if (notifyOnError && response.status === 'error' && typeof context.showNotification === 'function') {
                context.showNotification(`Unable to ${pendingEntry.action} the transaction.`, 'error');
            }
        }
    }, [syncTransactionHistoryState]);

    /** @type {(...args: any[]) => any} */
    const executeUndoTransaction = useCallback(() => {
        const context = contextRef.current;
        if (pendingHistoryRequestRef.current) return;
        const entry = undoStackRef.current[undoStackRef.current.length - 1];
        if (!entry || typeof entry !== 'object') return;
        if (entry.kind === 'layout') {
            undoStackRef.current = undoStackRef.current.slice(0, -1);
            redoStackRef.current = [...redoStackRef.current, entry];
            if (typeof context.clearPendingLayoutHistoryCapture === 'function') context.clearPendingLayoutHistoryCapture();
            if (typeof context.applyLayoutHistorySnapshot === 'function') context.applyLayoutHistorySnapshot(entry.before);
            syncTransactionHistoryState();
            if (typeof context.showNotification === 'function') context.showNotification('Undid the last layout change.', 'success');
            return;
        }
        if (!context.serverSide) return;
        /** @type {any} */
        const undoPayload = entry.eventId
            ? { eventAction: 'undo', eventId: entry.eventId, refreshMode: context.supportsPatchTransactionRefresh ? 'patch' : 'smart' }
            : entry.undoTransaction;
        if (!undoPayload) return;
        /** @type {any} */
        const dispatched = dispatchTransactionRequest(undoPayload, 'undo');
        if (!dispatched || !dispatched.requestId) return;
        queueTransactionHistoryRequest(dispatched.requestId, {
            action: 'undo', entry, targetEventIds: entry.eventId ? [entry.eventId] : [],
        }, { blocksHistory: true });
    }, [dispatchTransactionRequest, queueTransactionHistoryRequest, syncTransactionHistoryState]);

    /** @type {(...args: any[]) => any} */
    const executeRedoTransaction = useCallback(() => {
        const context = contextRef.current;
        if (pendingHistoryRequestRef.current) return;
        const entry = redoStackRef.current[redoStackRef.current.length - 1];
        if (!entry || typeof entry !== 'object') return;
        if (entry.kind === 'layout') {
            redoStackRef.current = redoStackRef.current.slice(0, -1);
            undoStackRef.current = [...undoStackRef.current, entry];
            if (typeof context.clearPendingLayoutHistoryCapture === 'function') context.clearPendingLayoutHistoryCapture();
            if (typeof context.applyLayoutHistorySnapshot === 'function') context.applyLayoutHistorySnapshot(entry.after);
            syncTransactionHistoryState();
            if (typeof context.showNotification === 'function') context.showNotification('Reapplied the last layout change.', 'success');
            return;
        }
        if (!context.serverSide) return;
        if (entry.comparisonPlan && typeof context.applyComparisonValuePlan === 'function') context.applyComparisonValuePlan(entry.comparisonPlan, 'forward');
        if (typeof context.setEditValueDisplayMode === 'function') context.setEditValueDisplayMode('edited');
        /** @type {any} */
        const redoPayload = entry.eventId
            ? { eventAction: 'redo', eventId: entry.eventId, refreshMode: context.supportsPatchTransactionRefresh ? 'patch' : 'smart' }
            : entry.redoTransaction;
        if (!redoPayload) {
            if (entry.comparisonPlan && typeof context.applyComparisonValuePlan === 'function') context.applyComparisonValuePlan(entry.comparisonPlan, 'backward');
            if (context.comparisonValueState && context.comparisonValueState.activeCount === 0 && typeof context.setEditValueDisplayMode === 'function') context.setEditValueDisplayMode('original');
            return;
        }
        /** @type {any} */
        const dispatched = dispatchTransactionRequest(redoPayload, 'redo');
        if (!dispatched || !dispatched.requestId) {
            if (entry.comparisonPlan && typeof context.applyComparisonValuePlan === 'function') context.applyComparisonValuePlan(entry.comparisonPlan, 'backward');
            if (context.comparisonValueState && context.comparisonValueState.activeCount === 0 && typeof context.setEditValueDisplayMode === 'function') context.setEditValueDisplayMode('original');
            return;
        }
        queueTransactionHistoryRequest(dispatched.requestId, {
            action: 'redo', entry, comparisonPlan: entry.comparisonPlan || null,
            targetEventIds: entry.eventId ? [entry.eventId] : [],
        }, { blocksHistory: true });
    }, [dispatchTransactionRequest, queueTransactionHistoryRequest, syncTransactionHistoryState]);

    return {
        configure,
        transactionHistoryState,
        syncTransactionHistoryState,
        findTransactionHistoryEntryByEventId,
        sortEventIdsByActiveHistory,
        pruneTransactionHistoryEntries,
        pushUnifiedHistoryEntry,
        queueTransactionHistoryRequest,
        clearTransactionHistory,
        dispatchTransactionRequest,
        finalizeTransactionHistoryResponse,
        clearPendingTransactionHistoryRequest,
        executeUndoTransaction,
        executeRedoTransaction,
    };
}
