import { useVirtualizer } from '@tanstack/react-virtual';
import { useEffect, useLayoutEffect, useRef, useMemo, useState, useCallback } from 'react';
import { useRowCache } from './useRowCache';

const isGrandTotalRow = (row) => (
    !!row && (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total')
);

const columnWindowCovers = (availableStart, availableEnd, requiredStart, requiredEnd) => {
    if (requiredEnd === null || requiredEnd === undefined) return true;
    if (availableEnd === null || availableEnd === undefined) return true;
    return availableStart <= requiredStart && availableEnd >= requiredEnd;
};

const blockMatchesColumnWindow = (block, colStart, colEnd) => {
    if (!block || colEnd === null || colEnd === undefined) return true;
    const blockHasColMeta = block.colEnd !== null && block.colEnd !== undefined;
    // Blocks loaded without column metadata come from an unwindowed/full-data
    // response. That payload is a superset of any later narrower column window,
    // so it should stay valid instead of triggering an immediate shrink refetch
    // that blanks cells after the first structural paint.
    if (!blockHasColMeta) return true;
    // A wider loaded window should satisfy any narrower visible window. Exact
    // equality is too strict for a virtualized table because tiny layout
    // re-measures can shift the visible leaf range by one column and trigger a
    // self-canceling cascade of viewport requests.
    return columnWindowCovers(block.colStart, block.colEnd, colStart, colEnd);
};

const resolveBufferedColumnWindow = (colStart, colEnd, prefetchColumnCountOverride = null) => {
    if (colStart === null || colStart === undefined || colEnd === null || colEnd === undefined) {
        return {
            requestedColStart: colStart ?? null,
            requestedColEnd: colEnd ?? null,
            visibleColumnCount: 0,
            requestedColumnCount: 0,
            prefetchColumnCount: 0,
        };
    }

    const visibleColumnCount = Math.max(0, colEnd - colStart + 1);
    const prefetchColumnCount = Number.isFinite(Number(prefetchColumnCountOverride))
        ? Math.max(0, Math.floor(Number(prefetchColumnCountOverride)))
        : Math.max(2, Math.min(4, Math.ceil(Math.max(visibleColumnCount, 1) / 2)));
    const requestedColStart = Math.max(0, colStart - prefetchColumnCount);
    const requestedColEnd = Math.max(requestedColStart, colEnd + prefetchColumnCount);

    return {
        requestedColStart,
        requestedColEnd,
        visibleColumnCount,
        requestedColumnCount: Math.max(0, requestedColEnd - requestedColStart + 1),
        prefetchColumnCount,
    };
};

const debugLog = (...args) => {
    const buildDebugEnabled = process.env.NODE_ENV !== 'production';
    let runtimeDebugEnabled = false;

    if (typeof window !== 'undefined') {
        try {
            runtimeDebugEnabled = window.__PIVOT_DEBUG__ === true || window.localStorage.getItem('pivot-debug') === '1';
        } catch (error) {
            runtimeDebugEnabled = window.__PIVOT_DEBUG__ === true;
        }
    }

    if (!buildDebugEnabled && !runtimeDebugEnabled) return;
    console.log('[pivot-client]', ...args);
};

const getServerSideRowOverscan = (rowCount) => {
    const numericRowCount = Number.isFinite(Number(rowCount)) ? Number(rowCount) : 0;
    if (numericRowCount >= 100000) return 4;
    if (numericRowCount >= 25000) return 5;
    return 6;
};

const getUrgentJumpRowOverscan = (visibleRows) => {
    const numericVisibleRows = Number.isFinite(Number(visibleRows)) ? Number(visibleRows) : 0;
    return Math.max(8, Math.min(32, Math.ceil(Math.max(numericVisibleRows, 1) / 2)));
};

/**
 * Hook to manage server-side virtualization with caching.
 */
export const useServerSideRowModel = ({
    parentRef,
    serverSide,
    rowCount,
    rowHeight,
    data, // The window of data received from backend
    dataOffset = 0, // The starting index of that window
    dataVersion = 0, // Data version from backend to prevent stale updates
    setProps,
    blockSize = 100,
    maxBlocksInCache = 500,
    blockLoadDebounceMs = null,
    rowOverscan = null,
    prefetchColumns = null,
    estimateRowHeight,
    keyMapper, // function to get a key from a row, if needed
    cacheKey,
    excludeGrandTotal = false,
    cinemaMode = false,
    stateEpoch = 0,
    sessionId = 'anonymous',
    clientInstance = 'default',
    tableName = null,
    abortGeneration = 0,
    structuralInFlight = false,
    requestVersionRef: externalRequestVersionRef = null,
    colStart = null,
    colEnd = null,
    responseColStart = null,
    responseColEnd = null,
    needsColSchema = false,
    columnRangeUrgencyToken = 0,
    onViewportRequest = null,
}) => {
    // 1. Initialize Cache
    const {
        setCurrentEpoch,
        getRow,
        setBlockLoading,
        setBlockLoaded,
        getBlock,
        clearCache,
        invalidateFromBlock,
        softInvalidateFromBlock,
        softInvalidateRange,
        pruneToRange,
        setPinnedRange,
        cacheVersion,
        getLoadedBlocks
    } = useRowCache({ blockSize, maxBlocks: maxBlocksInCache });

    // Request Version Tracking
    const internalRequestVersionRef = useRef(0);
    const requestVersionRef = externalRequestVersionRef || internalRequestVersionRef;
    const inflightRequestRef = useRef(null);
    const pendingViewportRef = useRef(null);
    const viewportAbortControllerRef = useRef(
        typeof AbortController !== 'undefined' ? new AbortController() : null
    );
    const [grandTotalRow, setGrandTotalRow] = useState(null);
    // Incremented whenever inflight is cleared so the viewport effect re-runs
    // even when virtualRows hasn't changed (e.g. after a stale/dropped response).
    const [inflightClearedAt, setInflightClearedAt] = useState(0);
    // Track what col range was requested so the data-sync effect can stamp blocks correctly
    // without needing dataColStart/dataColEnd as props (which would cascade on every response).
    const lastRequestedColStartRef = useRef(null);
    const lastRequestedColEndRef = useRef(null);
    const queuedViewportRef = useRef(null);
    const viewportFlushTimerRef = useRef(null);
    const lastViewportDispatchAtRef = useRef(0);
    const lastObservedScrollTopRef = useRef(0);
    const lastFastScrollDispatchRef = useRef({ startBlock: -1, endBlock: -1, dispatchedAt: 0 });
    const lastImmediateViewportRef = useRef({
        startBlock: -1,
        endBlock: -1,
        colStart: null,
        colEnd: null,
        dispatchedAt: 0,
    });
    const lastHandledColumnRangeUrgencyRef = useRef(0);
    // Throttle pruneToRange: only run when the viewport block range shifts by ≥1 block.
    const lastPrunedRangeRef = useRef({ startBlock: -1, endBlock: -1 });

    // 2. Initialize Virtualizer
    // We use the full rowCount for serverSide
    const rowVirtualizer = useVirtualizer({
        count: serverSide ? (rowCount || 0) : (data ? data.length : 0),
        getScrollElement: () => parentRef.current,
        estimateSize: estimateRowHeight || (() => rowHeight),
        overscan: serverSide
            ? (
                Number.isFinite(Number(rowOverscan))
                    ? Math.max(0, Math.floor(Number(rowOverscan)))
                    : getServerSideRowOverscan(rowCount)
            )
            : 12
    });
    
    const virtualRows = rowVirtualizer.getVirtualItems();

    // Force a synchronous re-measure after mount so the virtualizer gets the real
    // container height before the first paint. Without this, getScrollElement() returns
    // null on the first render pass (refs commit after render), leaving the virtualizer
    // with height=0 and rendering only overscan rows — causing the top and last rows
    // to be invisible until the user first interacts.
    useLayoutEffect(() => {
        rowVirtualizer.measure();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // Re-measure when server-side row count changes from bootstrap/initial load.
    // This prevents an initial 0-count measurement from leaving the first/last
    // rows out of the visible range until user interaction triggers another pass.
    useLayoutEffect(() => {
        if (!serverSide) return;
        rowVirtualizer.measure();
    }, [serverSide, rowCount, rowHeight, rowVirtualizer]);

    useEffect(() => {
        if (!serverSide) return;
        setCurrentEpoch(stateEpoch);
        inflightRequestRef.current = null;
        queuedViewportRef.current = null;
        lastRequestedColStartRef.current = null;
        lastRequestedColEndRef.current = null;
        lastViewportDispatchAtRef.current = 0;
        lastObservedScrollTopRef.current = parentRef.current ? parentRef.current.scrollTop : 0;
        lastFastScrollDispatchRef.current = { startBlock: -1, endBlock: -1, dispatchedAt: 0 };
        lastImmediateViewportRef.current = {
            startBlock: -1,
            endBlock: -1,
            colStart: null,
            colEnd: null,
            dispatchedAt: 0,
        };
        lastHandledColumnRangeUrgencyRef.current = columnRangeUrgencyToken;
        if (viewportFlushTimerRef.current) {
            clearTimeout(viewportFlushTimerRef.current);
            viewportFlushTimerRef.current = null;
        }
    }, [serverSide, stateEpoch, setCurrentEpoch]);

    useEffect(() => {
        if (!serverSide || !clearCache) return;
        clearCache();
        inflightRequestRef.current = null;
        pendingViewportRef.current = null;
        queuedViewportRef.current = null;
        lastRequestedColStartRef.current = null;
        lastRequestedColEndRef.current = null;
        lastViewportDispatchAtRef.current = 0;
        lastObservedScrollTopRef.current = parentRef.current ? parentRef.current.scrollTop : 0;
        lastFastScrollDispatchRef.current = { startBlock: -1, endBlock: -1, dispatchedAt: 0 };
        lastImmediateViewportRef.current = {
            startBlock: -1,
            endBlock: -1,
            colStart: null,
            colEnd: null,
            dispatchedAt: 0,
        };
        lastHandledColumnRangeUrgencyRef.current = columnRangeUrgencyToken;
        if (viewportFlushTimerRef.current) {
            clearTimeout(viewportFlushTimerRef.current);
            viewportFlushTimerRef.current = null;
        }
        if (!externalRequestVersionRef) {
            requestVersionRef.current = 0;
        }
        // Reset the grand total when filters/fields change (cacheKey change) so that a
        // pre-filter grand total value is never shown alongside post-filter row data.
        // The grand total is re-populated once the first response containing it arrives.
        setGrandTotalRow(null);
    }, [serverSide, cacheKey, clearCache, externalRequestVersionRef, requestVersionRef]);

    useEffect(() => {
        if (!serverSide) return;
        // Cooperative cancel point: invalidate inflight viewport intents from prior abort generation.
        inflightRequestRef.current = null;
        pendingViewportRef.current = null;
        queuedViewportRef.current = null;
        lastObservedScrollTopRef.current = parentRef.current ? parentRef.current.scrollTop : 0;
        lastFastScrollDispatchRef.current = { startBlock: -1, endBlock: -1, dispatchedAt: 0 };
        lastImmediateViewportRef.current = {
            startBlock: -1,
            endBlock: -1,
            colStart: null,
            colEnd: null,
            dispatchedAt: 0,
        };
        lastHandledColumnRangeUrgencyRef.current = columnRangeUrgencyToken;
        if (viewportFlushTimerRef.current) {
            clearTimeout(viewportFlushTimerRef.current);
            viewportFlushTimerRef.current = null;
        }
        if (viewportAbortControllerRef.current) {
            viewportAbortControllerRef.current.abort();
            viewportAbortControllerRef.current = new AbortController();
        }
    }, [serverSide, abortGeneration, stateEpoch]);

    useEffect(() => {
        if (!serverSide || !parentRef.current) return;
        // Clamp scroll when row count shrinks after collapse to avoid out-of-range virtual windows.
        const maxScrollTop = Math.max(
            0,
            (rowCount || 0) * rowHeight - parentRef.current.clientHeight
        );
        if (parentRef.current.scrollTop > maxScrollTop) {
            parentRef.current.scrollTop = maxScrollTop;
            if (rowVirtualizer.scrollToOffset) {
                rowVirtualizer.scrollToOffset(maxScrollTop);
            }
        }
    }, [serverSide, rowCount, rowHeight, rowVirtualizer, parentRef]);

    // 3. Sync incoming data to Cache
    useEffect(() => {
        if (!serverSide || !data) return;

        let normalizedData = data;
        let normalizedOffset = dataOffset;

        if (excludeGrandTotal) {
            const foundGrandTotal = data.find(isGrandTotalRow) || null;
            if (foundGrandTotal) {
                // Responses may include only a windowed column slice.
                // Merge with previous values so pinned grand total cells don't
                // transiently blank when horizontal windows shift.
                setGrandTotalRow(prev => (prev ? { ...prev, ...foundGrandTotal } : foundGrandTotal));
            }

            normalizedData = data.filter(row => !isGrandTotalRow(row));
        }

        if (normalizedData.length === 0) {
            debugLog('sync-empty', {
                dataOffset,
                dataVersion,
                excludeGrandTotal
            });
            return;
        }

        // Calculate which blocks this data covers
        // We assume data is a contiguous chunk starting at dataOffset
        const startBlock = Math.floor(normalizedOffset / blockSize);
        const endBlock = Math.floor((normalizedOffset + normalizedData.length - 1) / blockSize);
        
        for (let b = startBlock; b <= endBlock; b++) {
            const blockStart = b * blockSize;
            
            // Calculate slice indices relative to the 'data' array
            // Overlap start: Max(block start, data start)
            const absStart = Math.max(blockStart, normalizedOffset);
            const relStart = absStart - normalizedOffset;
            
            // Overlap end: Min(block end, data end)
            // Block end (exclusive) = (b+1)*blockSize
            const absEnd = Math.min((b + 1) * blockSize, normalizedOffset + normalizedData.length);
            const relEnd = absEnd - normalizedOffset;
            
            if (relStart < relEnd) {
                const blockRows = normalizedData
                    .slice(relStart, relEnd)
                    .map((row, index) => ({
                        ...row,
                        __virtualIndex: absStart + index
                    }));
                const effectiveRowCount = rowCount || (normalizedOffset + normalizedData.length);
                const expectedRowsForBlock = Math.max(
                    0,
                    Math.min((b + 1) * blockSize, effectiveRowCount) - blockStart
                );
                const isCompleteBlock = absStart === blockStart && blockRows.length >= expectedRowsForBlock;

                // Use dataVersion for stale response protection
                const effectiveResponseColStart = responseColStart !== null && responseColStart !== undefined
                    ? responseColStart
                    : lastRequestedColStartRef.current;
                const effectiveResponseColEnd = responseColEnd !== null && responseColEnd !== undefined
                    ? responseColEnd
                    : lastRequestedColEndRef.current;
                setBlockLoaded(
                    b,
                    blockRows,
                    dataVersion,
                    isCompleteBlock,
                    stateEpoch,
                    effectiveResponseColStart,
                    effectiveResponseColEnd
                );
                debugLog('sync-block', {
                    blockIndex: b,
                    stateEpoch,
                    normalizedOffset,
                    dataOffset,
                    dataVersion,
                    blockRows: blockRows.length,
                    expectedRowsForBlock,
                    isCompleteBlock
                });
            }
        }

        const inflight = inflightRequestRef.current;
        if (inflight) {
            // Use the original (pre-grand-total-filter) offset and length so that
            // a response containing a filtered-out grand total row still fully
            // covers the inflight range and clears it.  Using normalizedData.length
            // would give responseEnd = inflight.end - 1 whenever a grand total was
            // removed, leaving the inflight stuck and blocking the next request for
            // the same block range.
            const responseStart = dataOffset;
            const responseEnd = dataOffset + data.length - 1;
            // dataVersion is the client's window_seq echoed back by the server, so
            // it equals the inflight.version that was sent with the request.
            // Only clear the inflight when the response version is >= the inflight
            // version — this prevents a stale response (old window_seq) from
            // prematurely clearing the inflight for a newer in-progress request,
            // which would cause vertical scroll blocks to appear "orphaned" and
            // trigger an infinite re-request cascade.
            if (
                responseStart <= inflight.start &&
                responseEnd >= inflight.end &&
                dataVersion >= inflight.version
            ) {
                inflightRequestRef.current = null;
                // Signal to the viewport effect that it should re-run and look for
                // orphaned loading blocks (blocks whose inflight was superseded by
                // a faster request and whose stale backend response was dropped).
                setInflightClearedAt(t => t + 1);
                debugLog('clear-inflight', {
                    requestStart: inflight.start,
                    requestEnd: inflight.end,
                    requestVersion: inflight.version,
                    responseStart,
                    responseEnd,
                    dataVersion
                });
            }
        }
    }, [data, dataOffset, dataVersion, excludeGrandTotal, serverSide, blockSize, rowCount, setBlockLoaded, stateEpoch, responseColStart, responseColEnd]);

    const collectBlocksNeeded = useCallback((
        firstRow,
        lastRow,
        currentInflight = inflightRequestRef.current,
        overrideColStart = colStart,
        overrideColEnd = colEnd
    ) => {
        const startBlock = Math.floor(firstRow / blockSize);
        const endBlock = Math.floor(lastRow / blockSize);
        const blocksNeeded = [];

        for (let b = startBlock; b <= endBlock; b++) {
            const block = getBlock(b, stateEpoch);
            const isStale = block && block.status === 'loading' && (Date.now() - block.timestamp > 1500);
            const isPartial = block && block.status === 'partial';
            const blockStart = b * blockSize;
            const blockEnd = (b + 1) * blockSize - 1;
            const isOrphaned = block && block.status === 'loading' && !isStale && !(
                currentInflight &&
                currentInflight.abortGeneration === abortGeneration &&
                currentInflight.stateEpoch === stateEpoch &&
                currentInflight.start <= blockStart &&
                currentInflight.end >= blockEnd
            );
            const isColMismatch = block && block.status === 'loaded' && !blockMatchesColumnWindow(block, overrideColStart, overrideColEnd);
            const inflightColMismatch = block && block.status === 'loading' && currentInflight && !columnWindowCovers(
                currentInflight.colStart,
                currentInflight.colEnd,
                overrideColStart,
                overrideColEnd
            );
            if (!block || block.status === 'error' || isStale || isPartial || isOrphaned || isColMismatch || inflightColMismatch) {
                blocksNeeded.push(b);
            }
        }

        if (blocksNeeded.length > 0) {
            const newInflightMinBlock = Math.min(...blocksNeeded);
            const newInflightMaxBlock = Math.max(...blocksNeeded);
            for (let b = startBlock; b <= endBlock; b++) {
                if (b >= newInflightMinBlock && b <= newInflightMaxBlock) continue;
                const bBlock = getBlock(b, stateEpoch);
                if (bBlock && bBlock.status === 'loading') {
                    blocksNeeded.push(b);
                }
            }
        }

        return {
            startBlock,
            endBlock,
            blocksNeeded: [...new Set(blocksNeeded)].sort((left, right) => left - right),
        };
    }, [abortGeneration, blockSize, colEnd, colStart, getBlock, stateEpoch]);

    const requestViewport = useCallback((
        firstRow,
        lastRow,
        blocksNeeded,
        overrideColStart = colStart,
        overrideColEnd = colEnd,
        queuedAt = Date.now(),
        options = {}
    ) => {
        if (!setProps || blocksNeeded.length === 0) return false;
        const abortSignal = viewportAbortControllerRef.current ? viewportAbortControllerRef.current.signal : null;
        if (abortSignal && abortSignal.aborted) return false;

        const minBlock = Math.min(...blocksNeeded);
        const maxBlock = Math.max(...blocksNeeded);
        const reqStart = minBlock * blockSize;
        const reqEnd = (maxBlock + 1) * blockSize - 1;

        const inflight = inflightRequestRef.current;
        const inflightIsFresh = inflight && (Date.now() - inflight.timestamp <= 5000);
        const {
            requestedColStart,
            requestedColEnd,
            visibleColumnCount,
            requestedColumnCount,
            prefetchColumnCount,
        } = resolveBufferedColumnWindow(overrideColStart, overrideColEnd, prefetchColumns);

        if (
            inflightIsFresh &&
            inflight.start === reqStart &&
            inflight.end === reqEnd &&
            columnWindowCovers(inflight.colStart, inflight.colEnd, overrideColStart, overrideColEnd) &&
            inflight.abortGeneration === abortGeneration &&
            inflight.stateEpoch === stateEpoch
        ) {
            debugLog('skip-duplicate-viewport', {
                firstRow,
                lastRow,
                reqStart,
                reqEnd,
                requestedColStart,
                requestedColEnd,
                visibleColStart: overrideColStart,
                visibleColEnd: overrideColEnd,
                inflightVersion: inflight.version,
                stateEpoch,
                abortGeneration
            });
            return false;
        }

        const newVersion = requestVersionRef.current + 1;
        const requestId = `viewport:${sessionId}:${clientInstance}:${newVersion}`;
        requestVersionRef.current = newVersion;
        const previousColStart = lastRequestedColStartRef.current;
        const previousColEnd = lastRequestedColEndRef.current;
        const hasColumnWindow = requestedColStart !== null && requestedColEnd !== null;
        const columnRangeChanged = hasColumnWindow && (
            previousColStart !== requestedColStart ||
            previousColEnd !== requestedColEnd
        );
        const overlapStart = hasColumnWindow && previousColStart !== null && previousColEnd !== null
            ? Math.max(previousColStart, requestedColStart)
            : null;
        const overlapEnd = hasColumnWindow && previousColStart !== null && previousColEnd !== null
            ? Math.min(previousColEnd, requestedColEnd)
            : null;
        const overlapCount = overlapStart !== null && overlapEnd !== null && overlapEnd >= overlapStart
            ? overlapEnd - overlapStart + 1
            : 0;
        const columnDeltaCount = columnRangeChanged
            ? Math.max(1, requestedColumnCount - overlapCount)
            : 0;

        // Always update the block version, even when it is already loading.
        // If we skip setBlockLoading for loading blocks, the block retains its
        // OLD request version.  A stale response for that old version then passes
        // the `block.version > requestVersion` guard (equal, not strictly greater)
        // and is accepted, clearing the inflight and triggering yet another cascade
        // of orphan-detected re-requests.  setBlockLoading preserves existing rows
        // (stale-while-revalidate) so calling it again is safe.
        for (let b = minBlock; b <= maxBlock; b++) {
            setBlockLoading(b, newVersion, stateEpoch);
        }
        setPinnedRange(minBlock, maxBlock, 2, stateEpoch);

        const reqCount = reqEnd - reqStart + 1;
        inflightRequestRef.current = {
            start: reqStart,
            end: reqEnd,
            colStart: requestedColStart,
            colEnd: requestedColEnd,
            version: newVersion,
            timestamp: Date.now(),
            abortGeneration,
            stateEpoch
        };
        if (typeof onViewportRequest === 'function') {
            onViewportRequest({
                requestId,
                version: newVersion,
                reqStart,
                reqEnd,
                colStart: requestedColStart,
                colEnd: requestedColEnd,
                hasColumnWindow,
                columnRangeChanged,
                columnDeltaCount,
                visibleColumnCount,
                requestedColumnCount,
                prefetchColumnCount,
                queuedAt,
                emittedAt: Date.now(),
                stateEpoch,
                abortGeneration,
                silent: Boolean(options && options.silent),
            });
        }

        // Stamp the col range into refs BEFORE setProps so the data-sync effect
        // always reads the correct range even if the response arrives synchronously.
        lastRequestedColStartRef.current = requestedColStart;
        lastRequestedColEndRef.current = requestedColEnd;

        setProps({
            runtimeRequest: {
                kind: 'data',
                requestId,
                payload: {
                    requestId,
                    table: tableName || undefined,
                    start: reqStart,
                    end: reqEnd,
                    count: reqCount,
                    version: newVersion,
                    window_seq: newVersion,
                    state_epoch: stateEpoch,
                    session_id: sessionId,
                    client_instance: clientInstance,
                    abort_generation: abortGeneration,
                    intent: 'viewport',
                    col_start: requestedColStart !== null ? requestedColStart : undefined,
                    col_end: requestedColEnd !== null ? requestedColEnd : undefined,
                    needs_col_schema: needsColSchema || undefined,
                    include_grand_total: excludeGrandTotal || undefined,
                    cinema_mode: cinemaMode || undefined,
                },
            },
        });

        debugLog('request-viewport', {
            firstRow,
            lastRow,
            blocksNeeded,
            reqStart,
            reqEnd,
            previousColStart,
            previousColEnd,
            visibleColStart: overrideColStart,
            visibleColEnd: overrideColEnd,
            requestedColStart,
            requestedColEnd,
            columnRangeChanged,
            columnDeltaCount,
            visibleColumnCount,
            requestedColumnCount,
            prefetchColumnCount,
            needsColSchema,
            version: newVersion,
            stateEpoch,
            abortGeneration
        });
        return true;
    }, [
        setProps,
        blockSize,
        abortGeneration,
        stateEpoch,
        requestVersionRef,
        getBlock,
        setBlockLoading,
        sessionId,
        clientInstance,
        tableName,
        colStart,
        colEnd,
        needsColSchema,
        excludeGrandTotal,
        onViewportRequest,
        prefetchColumns,
        setPinnedRange,
        cinemaMode,
    ]);

    const flushQueuedViewport = useCallback((forceImmediate = false) => {
        if (viewportFlushTimerRef.current) {
            clearTimeout(viewportFlushTimerRef.current);
            viewportFlushTimerRef.current = null;
        }
        const queued = queuedViewportRef.current;
        if (!queued || queued.blocksNeeded.length === 0) return false;
        queuedViewportRef.current = null;
        const sent = requestViewport(
            queued.firstRow,
            queued.lastRow,
            queued.blocksNeeded,
            queued.overrideColStart,
            queued.overrideColEnd,
            queued.enqueuedAt
        );
        if (sent || forceImmediate) {
            lastViewportDispatchAtRef.current = Date.now();
        }
        return sent;
    }, [requestViewport]);

    const enqueueViewportRequest = useCallback((
        firstRow,
        lastRow,
        blocksNeeded,
        {
            immediate = false,
            overrideColStart = colStart,
            overrideColEnd = colEnd,
        } = {}
    ) => {
        if (!blocksNeeded || blocksNeeded.length === 0) return false;
        const normalizedBlocks = [...new Set(blocksNeeded)].sort((left, right) => left - right);
        const existing = queuedViewportRef.current;

        if (
            existing &&
            existing.stateEpoch === stateEpoch &&
            existing.abortGeneration === abortGeneration
        ) {
            queuedViewportRef.current = {
                ...existing,
                firstRow,
                lastRow,
                overrideColStart,
                overrideColEnd,
                blocksNeeded: [...new Set([...existing.blocksNeeded, ...normalizedBlocks])].sort((left, right) => left - right),
                enqueuedAt: existing.enqueuedAt || Date.now(),
                updatedAt: Date.now(),
            };
        } else {
            queuedViewportRef.current = {
                firstRow,
                lastRow,
                overrideColStart,
                overrideColEnd,
                blocksNeeded: normalizedBlocks,
                stateEpoch,
                abortGeneration,
                enqueuedAt: Date.now(),
                updatedAt: Date.now(),
            };
        }

        const queued = queuedViewportRef.current;
        if (queued && queued.blocksNeeded.length > 0) {
            setPinnedRange(queued.blocksNeeded[0], queued.blocksNeeded[queued.blocksNeeded.length - 1], 2, stateEpoch);
        }

        if (viewportFlushTimerRef.current) {
            clearTimeout(viewportFlushTimerRef.current);
            viewportFlushTimerRef.current = null;
        }

        if (immediate) {
            lastImmediateViewportRef.current = {
                startBlock: normalizedBlocks[0],
                endBlock: normalizedBlocks[normalizedBlocks.length - 1],
                colStart: overrideColStart,
                colEnd: overrideColEnd,
                dispatchedAt: Date.now(),
            };
            lastHandledColumnRangeUrgencyRef.current = columnRangeUrgencyToken;
            lastViewportDispatchAtRef.current = Date.now();
            return flushQueuedViewport(true);
        }

        const elapsed = Date.now() - lastViewportDispatchAtRef.current;
        const debounceMs = immediate
            ? 0
            : (
                Number.isFinite(Number(blockLoadDebounceMs))
                    ? Math.max(0, Math.floor(Number(blockLoadDebounceMs)))
                    : (elapsed >= 80 ? 16 : 48)
            );
        viewportFlushTimerRef.current = setTimeout(() => {
            viewportFlushTimerRef.current = null;
            flushQueuedViewport(immediate);
        }, debounceMs);
        return true;
    }, [abortGeneration, blockLoadDebounceMs, colEnd, colStart, columnRangeUrgencyToken, flushQueuedViewport, prefetchColumns, setPinnedRange, stateEpoch]);

    const requestUrgentColumnViewport = useCallback((overrideColStart, overrideColEnd) => {
        if (!serverSide || !parentRef.current || structuralInFlight) return false;

        const scrollEl = parentRef.current;
        const estimatedFirstRow = Math.max(0, Math.floor(scrollEl.scrollTop / Math.max(rowHeight, 1)));
        const estimatedVisibleRows = Math.max(1, Math.ceil(scrollEl.clientHeight / Math.max(rowHeight, 1)));
        const urgentJumpOverscanRows = getUrgentJumpRowOverscan(estimatedVisibleRows);
        const estimatedLastRow = Math.max(
            estimatedFirstRow,
            Math.min(
                Math.max((rowCount || 0) - 1, estimatedFirstRow),
                estimatedFirstRow + estimatedVisibleRows + urgentJumpOverscanRows
            )
        );

        const { blocksNeeded } = collectBlocksNeeded(
            estimatedFirstRow,
            estimatedLastRow,
            inflightRequestRef.current,
            overrideColStart,
            overrideColEnd
        );
        if (blocksNeeded.length === 0) {
            lastHandledColumnRangeUrgencyRef.current = columnRangeUrgencyToken;
            return false;
        }

        return enqueueViewportRequest(estimatedFirstRow, estimatedLastRow, blocksNeeded, {
            immediate: true,
            overrideColStart,
            overrideColEnd,
        });
    }, [
        blockSize,
        collectBlocksNeeded,
        columnRangeUrgencyToken,
        enqueueViewportRequest,
        parentRef,
        rowCount,
        rowHeight,
        serverSide,
        structuralInFlight,
    ]);

    const requestVisibleViewportRefresh = useCallback((overrideColStart = colStart, overrideColEnd = colEnd) => {
        if (!serverSide || !parentRef.current || structuralInFlight) return false;

        const scrollEl = parentRef.current;
        const estimatedFirstRow = virtualRows.length > 0
            ? virtualRows[0].index
            : Math.max(0, Math.floor(scrollEl.scrollTop / Math.max(rowHeight, 1)));
        const estimatedLastRow = virtualRows.length > 0
            ? virtualRows[virtualRows.length - 1].index
            : Math.max(
                estimatedFirstRow,
                Math.min(
                    Math.max((rowCount || 0) - 1, estimatedFirstRow),
                    estimatedFirstRow + Math.max(1, Math.ceil(scrollEl.clientHeight / Math.max(rowHeight, 1)))
                )
            );

        const startBlock = Math.floor(estimatedFirstRow / blockSize);
        const endBlock = Math.floor(estimatedLastRow / blockSize);
        softInvalidateRange(startBlock, endBlock, stateEpoch);

        const { blocksNeeded } = collectBlocksNeeded(
            estimatedFirstRow,
            estimatedLastRow,
            inflightRequestRef.current,
            overrideColStart,
            overrideColEnd
        );
        if (blocksNeeded.length === 0) return false;

        return requestViewport(
            estimatedFirstRow,
            estimatedLastRow,
            blocksNeeded,
            overrideColStart,
            overrideColEnd,
            Date.now(),
            { silent: true }
        );
    }, [
        blockSize,
        colEnd,
        colStart,
        collectBlocksNeeded,
        parentRef,
        requestViewport,
        rowCount,
        rowHeight,
        serverSide,
        softInvalidateRange,
        stateEpoch,
        structuralInFlight,
        virtualRows,
    ]);

    const viewportSchedulingEnabled = true;

    useEffect(() => {
        if (!viewportSchedulingEnabled || !serverSide || !parentRef.current) return;

        const scrollEl = parentRef.current;
        lastObservedScrollTopRef.current = scrollEl.scrollTop;

        const handleScroll = () => {
            if (structuralInFlight) {
                lastObservedScrollTopRef.current = scrollEl.scrollTop;
                return;
            }

            const nextScrollTop = scrollEl.scrollTop;
            const scrollDelta = Math.abs(nextScrollTop - lastObservedScrollTopRef.current);
            lastObservedScrollTopRef.current = nextScrollTop;

            const bigJumpThreshold = rowHeight * blockSize * 4;
            if (scrollDelta < bigJumpThreshold) return;

            const estimatedFirstRow = Math.max(0, Math.floor(nextScrollTop / Math.max(rowHeight, 1)));
            const estimatedVisibleRows = Math.max(1, Math.ceil(scrollEl.clientHeight / Math.max(rowHeight, 1)));
            const urgentJumpOverscanRows = getUrgentJumpRowOverscan(estimatedVisibleRows);
            const estimatedLastRow = Math.max(
                estimatedFirstRow,
                Math.min(
                    Math.max((rowCount || 0) - 1, estimatedFirstRow),
                    estimatedFirstRow + estimatedVisibleRows + urgentJumpOverscanRows
                )
            );

            const { startBlock, endBlock, blocksNeeded } = collectBlocksNeeded(estimatedFirstRow, estimatedLastRow);
            if (blocksNeeded.length === 0) return;

            const previousFastDispatch = lastFastScrollDispatchRef.current;
            const now = Date.now();
            if (
                previousFastDispatch.startBlock === startBlock
                && previousFastDispatch.endBlock === endBlock
                && (now - previousFastDispatch.dispatchedAt) < 180
            ) {
                return;
            }

            lastFastScrollDispatchRef.current = {
                startBlock,
                endBlock,
                dispatchedAt: now,
            };

            enqueueViewportRequest(estimatedFirstRow, estimatedLastRow, blocksNeeded, { immediate: true });
        };

        scrollEl.addEventListener('scroll', handleScroll, { passive: true });
        return () => {
            scrollEl.removeEventListener('scroll', handleScroll);
        };
    }, [
        viewportSchedulingEnabled,
        serverSide,
        parentRef,
        structuralInFlight,
        rowHeight,
        blockSize,
        rowCount,
        collectBlocksNeeded,
        enqueueViewportRequest,
    ]);

    useEffect(() => {
        if (!viewportSchedulingEnabled || !serverSide || !setProps || virtualRows.length === 0) return;

        const firstRow = virtualRows[0].index;
        const lastRow = virtualRows[virtualRows.length - 1].index;
        const retentionBufferBlocks = 2;
        const { startBlock, endBlock, blocksNeeded } = collectBlocksNeeded(firstRow, lastRow);
        const previousRange = lastPrunedRangeRef.current;
        const blockJumpDistance = previousRange.startBlock >= 0
            ? Math.max(
                Math.abs(startBlock - previousRange.startBlock),
                Math.abs(endBlock - previousRange.endBlock)
            )
            : 0;
        const columnJumpDistance = (
            colStart !== null
            && colStart !== undefined
            && colEnd !== null
            && colEnd !== undefined
            && lastRequestedColStartRef.current !== null
            && lastRequestedColStartRef.current !== undefined
            && lastRequestedColEndRef.current !== null
            && lastRequestedColEndRef.current !== undefined
        )
            ? Math.max(
                Math.abs(colStart - lastRequestedColStartRef.current),
                Math.abs(colEnd - lastRequestedColEndRef.current)
            )
            : 0;
        const columnRangeUrgent = columnRangeUrgencyToken > lastHandledColumnRangeUrgencyRef.current;
        const shouldDispatchImmediately = blockJumpDistance >= 4 || columnJumpDistance >= 12 || columnRangeUrgent;
        const recentImmediateViewport = lastImmediateViewportRef.current;
        const matchesRecentImmediateViewport = (
            recentImmediateViewport.startBlock === startBlock
            && recentImmediateViewport.endBlock === endBlock
            && recentImmediateViewport.colStart === colStart
            && recentImmediateViewport.colEnd === colEnd
            && (Date.now() - recentImmediateViewport.dispatchedAt) < 180
        );

        setPinnedRange(startBlock, endBlock, retentionBufferBlocks + 1, stateEpoch);

        const lpr = lastPrunedRangeRef.current;
        if (startBlock !== lpr.startBlock || endBlock !== lpr.endBlock) {
            pruneToRange(startBlock, endBlock, retentionBufferBlocks, stateEpoch);
            lastPrunedRangeRef.current = { startBlock, endBlock };
        }

        if (structuralInFlight) {
            const existingPending = pendingViewportRef.current;
            pendingViewportRef.current = existingPending
                ? {
                    firstRow,
                    lastRow,
                    blocksNeeded: [...new Set([...(existingPending.blocksNeeded || []), ...blocksNeeded])].sort((left, right) => left - right),
                }
                : { firstRow, lastRow, blocksNeeded };
            return;
        }

        if (blocksNeeded.length > 0) {
            if (matchesRecentImmediateViewport) {
                if (columnRangeUrgent) {
                    lastHandledColumnRangeUrgencyRef.current = columnRangeUrgencyToken;
                }
                return;
            }
            enqueueViewportRequest(firstRow, lastRow, blocksNeeded, { immediate: shouldDispatchImmediately });
        } else if (columnRangeUrgent) {
            lastHandledColumnRangeUrgencyRef.current = columnRangeUrgencyToken;
        }
    }, [
        virtualRows,
        serverSide,
        setProps,
        pruneToRange,
        stateEpoch,
        structuralInFlight,
        inflightClearedAt,
        cacheVersion,
        collectBlocksNeeded,
        enqueueViewportRequest,
        setPinnedRange,
        colStart,
        colEnd,
        columnRangeUrgencyToken,
    ]);

    useEffect(() => {
        if (!viewportSchedulingEnabled || !serverSide || structuralInFlight || !pendingViewportRef.current || virtualRows.length === 0) return;

        const pending = pendingViewportRef.current;
        pendingViewportRef.current = null;
        const retentionBufferBlocks = 2;

        const { startBlock, endBlock, blocksNeeded } = pending.blocksNeeded && pending.blocksNeeded.length > 0
            ? {
                startBlock: Math.floor(pending.firstRow / blockSize),
                endBlock: Math.floor(pending.lastRow / blockSize),
                blocksNeeded: pending.blocksNeeded,
            }
            : collectBlocksNeeded(pending.firstRow, pending.lastRow);

        setPinnedRange(startBlock, endBlock, retentionBufferBlocks + 1, stateEpoch);
        pruneToRange(startBlock, endBlock, retentionBufferBlocks, stateEpoch);

        if (blocksNeeded.length > 0) {
            enqueueViewportRequest(pending.firstRow, pending.lastRow, blocksNeeded, { immediate: true });
        }
    }, [serverSide, structuralInFlight, virtualRows, blockSize, pruneToRange, stateEpoch, collectBlocksNeeded, enqueueViewportRequest, setPinnedRange]);

    // 4. Check Viewport & Trigger Fetch (with Debounce)
    useEffect(() => {
        if (!serverSide || !setProps || virtualRows.length === 0 || viewportSchedulingEnabled) return;

        // Use a small timeout to debounce rapid scrolling/viewport changes
        const timer = setTimeout(() => {
            const firstRow = virtualRows[0].index;
            const lastRow = virtualRows[virtualRows.length - 1].index;
            
            // Identify missing blocks
            const blocksNeeded = [];
            const startBlock = Math.floor(firstRow / blockSize);
            const endBlock = Math.floor(lastRow / blockSize);
            const retentionBufferBlocks = 2;

            // Only prune when the visible block range actually shifts — avoids redundant
            // cache churn on every debounced pass while the viewport hasn't moved a full block.
            const lpr = lastPrunedRangeRef.current;
            if (startBlock !== lpr.startBlock || endBlock !== lpr.endBlock) {
                pruneToRange(startBlock, endBlock, retentionBufferBlocks, stateEpoch);
                lastPrunedRangeRef.current = { startBlock, endBlock };
            }

            const currentInflight = inflightRequestRef.current;
            for (let b = startBlock; b <= endBlock; b++) {
                const block = getBlock(b, stateEpoch);
                const isStale = block && block.status === 'loading' && (Date.now() - block.timestamp > 1500);
                const isPartial = block && block.status === 'partial';
                // A loading block is "orphaned" when the current inflight no longer covers
                // it (e.g. user scrolled away to a different range). Re-request it so it
                // doesn't stay stuck for the full stale timeout.
                const blockStart = b * blockSize;
                const blockEnd = (b + 1) * blockSize - 1;
                const isOrphaned = block && block.status === 'loading' && !isStale && !(
                    currentInflight &&
                    currentInflight.abortGeneration === abortGeneration &&
                    currentInflight.stateEpoch === stateEpoch &&
                    currentInflight.start <= blockStart &&
                    currentInflight.end >= blockEnd
                );
                // Column window mismatch only matters for blocks that were already loaded
                // as a specific window. Full-data blocks remain valid for any narrower
                // window in the same epoch.
                const isColMismatch = block && block.status === 'loaded' && !blockMatchesColumnWindow(block, colStart, colEnd);
                const inflightColMismatch = block && block.status === 'loading' && currentInflight && !columnWindowCovers(
                    currentInflight.colStart,
                    currentInflight.colEnd,
                    colStart,
                    colEnd
                );
                if (!block || block.status === 'error' || isStale || isPartial || isOrphaned || isColMismatch || inflightColMismatch) {
                    blocksNeeded.push(b);
                }
            }

            if (structuralInFlight) {
                pendingViewportRef.current = { firstRow, lastRow };
                return;
            }

            // Prevent orphan ping-pong: when we're about to send a new request
            // covering only a subset of the viewport blocks, any loading block
            // outside that new inflight range would immediately become "orphaned"
            // on the very next viewport-effect tick, triggering another request
            // that orphans the first block, and so on forever.
            // Fix: extend blocksNeeded to absorb any loading blocks in the
            // viewport that the new inflight would leave uncovered.
            if (blocksNeeded.length > 0) {
                const newInflightMinBlock = Math.min(...blocksNeeded);
                const newInflightMaxBlock = Math.max(...blocksNeeded);
                for (let b = startBlock; b <= endBlock; b++) {
                    if (b >= newInflightMinBlock && b <= newInflightMaxBlock) continue;
                    const bBlock = getBlock(b, stateEpoch);
                    if (bBlock && bBlock.status === 'loading') {
                        blocksNeeded.push(b);
                    }
                }
                requestViewport(firstRow, lastRow, blocksNeeded);
            }
        }, 50);

        return () => clearTimeout(timer);

    }, [virtualRows, serverSide, getBlock, setProps, blockSize, pruneToRange, stateEpoch, structuralInFlight, requestViewport, inflightClearedAt, cacheVersion, colStart, colEnd]);

    useEffect(() => {
        if (!serverSide || structuralInFlight || !pendingViewportRef.current || virtualRows.length === 0 || viewportSchedulingEnabled) return;

        const pending = pendingViewportRef.current;
        pendingViewportRef.current = null;

        const startBlock = Math.floor(pending.firstRow / blockSize);
        const endBlock = Math.floor(pending.lastRow / blockSize);
        const retentionBufferBlocks = 2;

        pruneToRange(startBlock, endBlock, retentionBufferBlocks, stateEpoch);

        const blocksNeeded = [];
        const currentInflight = inflightRequestRef.current;
        for (let b = startBlock; b <= endBlock; b++) {
            const block = getBlock(b, stateEpoch);
            const isStale = block && block.status === 'loading' && (Date.now() - block.timestamp > 1500);
            const isPartial = block && block.status === 'partial';
            const blockStart = b * blockSize;
            const blockEnd = (b + 1) * blockSize - 1;
            const isOrphaned = block && block.status === 'loading' && !isStale && !(
                currentInflight &&
                currentInflight.abortGeneration === abortGeneration &&
                currentInflight.stateEpoch === stateEpoch &&
                currentInflight.start <= blockStart &&
                currentInflight.end >= blockEnd
            );
            const isColMismatch = block && block.status === 'loaded' && !blockMatchesColumnWindow(block, colStart, colEnd);
            const inflightColMismatch = block && block.status === 'loading' && currentInflight && !columnWindowCovers(
                currentInflight.colStart,
                currentInflight.colEnd,
                colStart,
                colEnd
            );
            if (!block || block.status === 'error' || isStale || isPartial || isOrphaned || isColMismatch || inflightColMismatch) {
                blocksNeeded.push(b);
            }
        }

        // Same orphan-extension as the main viewport effect.
        if (blocksNeeded.length > 0) {
            const newInflightMinBlock = Math.min(...blocksNeeded);
            const newInflightMaxBlock = Math.max(...blocksNeeded);
            for (let b = startBlock; b <= endBlock; b++) {
                if (b >= newInflightMinBlock && b <= newInflightMaxBlock) continue;
                const bBlock = getBlock(b, stateEpoch);
                if (bBlock && bBlock.status === 'loading') {
                    blocksNeeded.push(b);
                }
            }
            requestViewport(pending.firstRow, pending.lastRow, blocksNeeded);
        }
    }, [serverSide, structuralInFlight, virtualRows, blockSize, getBlock, pruneToRange, requestViewport, stateEpoch, colStart, colEnd, abortGeneration]);

    // 5. Cleanup Effect
    useEffect(() => {
        return () => {
            // Clear cache when hook unmounts to prevent memory leaks
            if (clearCache) clearCache();
            inflightRequestRef.current = null;
            pendingViewportRef.current = null;
            queuedViewportRef.current = null;
            if (viewportFlushTimerRef.current) {
                clearTimeout(viewportFlushTimerRef.current);
                viewportFlushTimerRef.current = null;
            }
            if (viewportAbortControllerRef.current) {
                viewportAbortControllerRef.current.abort();
            }
        };
    }, [clearCache]);

    // 6. Construct Data for Render (The "Window")
    const renderedDataInfo = useMemo(() => {
        if (!serverSide) return { data: data || [], offset: 0 };
        if (virtualRows.length === 0) return { data: [], offset: 0 };
        
        const firstIndex = virtualRows[0].index;
        const lastIndex = virtualRows[virtualRows.length - 1].index;
        
        const startBlock = Math.floor(firstIndex / blockSize);
        const endBlock = Math.floor(lastIndex / blockSize);
        
        const startOffset = startBlock * blockSize;
        // Total length needed to cover from start of startBlock to end of endBlock
        const totalLen = (endBlock - startBlock + 1) * blockSize;
        
        // Fill with empty objects to prevent accessors from crashing on undefined
        // We use a factory function or map to ensure distinct objects if needed, 
        // but for read-only accessors {} is fine. 
        // However, fill({}) reuses the SAME object reference. 
        // Safer to use map or just loop.
        const merged = new Array(totalLen).fill(null);
        
        for (let b = startBlock; b <= endBlock; b++) {
            const block = getBlock(b, stateEpoch);
            if (block && block.rows) {
                const offsetInMerged = (b - startBlock) * blockSize;
                const blockColMismatch = !blockMatchesColumnWindow(block, colStart, colEnd);
                const blockPendingColumns = (
                    block.status === 'loading' ||
                    block.status === 'partial' ||
                    blockColMismatch
                );
                for (let i = 0; i < block.rows.length; i++) {
                    const row = block.rows[i];
                    if (blockPendingColumns && row && typeof row === 'object') {
                        merged[offsetInMerged + i] = { ...row, __colPending: true };
                    } else {
                        merged[offsetInMerged + i] = row;
                    }
                }
            }
        }

        debugLog('render-window', {
            firstIndex,
            lastIndex,
            startBlock,
            endBlock,
            startOffset,
            totalLen,
            blockStates: Array.from({ length: endBlock - startBlock + 1 }, (_, idx) => {
                const blockIndex = startBlock + idx;
                const block = getBlock(blockIndex, stateEpoch);
                return {
                    blockIndex,
                    status: block ? block.status : 'missing',
                    rows: block && block.rows ? block.rows.length : 0,
                    version: block ? block.version : null
                };
            })
        });
        
        return { data: merged, offset: startOffset };
    }, [virtualRows, getBlock, serverSide, blockSize, data, cacheVersion, stateEpoch, colStart, colEnd]);

    const loadedRows = useMemo(() => {
        if (!serverSide) return Array.isArray(data) ? data : [];

        const blocks = getLoadedBlocks(stateEpoch);
        if (blocks.length === 0) return [];

        const mergedRows = [];
        const seenVirtualIndexes = new Set();

        blocks.forEach((block) => {
            const blockColMismatch = !blockMatchesColumnWindow(block, colStart, colEnd);
            if (blockColMismatch) return;

            block.rows.forEach((row, index) => {
                const virtualIndex = row && typeof row.__virtualIndex === 'number'
                    ? row.__virtualIndex
                    : (block.blockIndex * blockSize) + index;
                if (seenVirtualIndexes.has(virtualIndex)) return;
                seenVirtualIndexes.add(virtualIndex);
                mergedRows.push(row);
            });
        });

        mergedRows.sort((left, right) => {
            const leftIndex = left && typeof left.__virtualIndex === 'number' ? left.__virtualIndex : 0;
            const rightIndex = right && typeof right.__virtualIndex === 'number' ? right.__virtualIndex : 0;
            return leftIndex - rightIndex;
        });

        return mergedRows;
    }, [serverSide, data, getLoadedBlocks, stateEpoch, cacheVersion, colStart, colEnd, blockSize]);

    const getRowInEpoch = useCallback((rowIndex) => getRow(rowIndex, stateEpoch), [getRow, stateEpoch]);
    const invalidateFromCurrentEpoch = useCallback(
        (blockIndex) => invalidateFromBlock(blockIndex, stateEpoch),
        [invalidateFromBlock, stateEpoch]
    );
    const softInvalidateFromCurrentEpoch = useCallback(
        (blockIndex) => softInvalidateFromBlock(blockIndex, stateEpoch),
        [softInvalidateFromBlock, stateEpoch]
    );

    return {
        rowVirtualizer,
        getRow: getRowInEpoch,
        clearCache,
        invalidateFromBlock: invalidateFromCurrentEpoch,
        softInvalidateFromBlock: softInvalidateFromCurrentEpoch,
        grandTotalRow,
        loadedRows,
        renderedData: renderedDataInfo.data,
        renderedOffset: renderedDataInfo.offset,
        requestUrgentColumnViewport,
        requestVisibleViewportRefresh,
    };
};
