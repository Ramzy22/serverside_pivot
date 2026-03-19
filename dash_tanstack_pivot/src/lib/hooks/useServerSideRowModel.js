import { useVirtualizer } from '@tanstack/react-virtual';
import { useEffect, useLayoutEffect, useRef, useMemo, useState, useCallback } from 'react';
import { useRowCache } from './useRowCache';

const isGrandTotalRow = (row) => (
    !!row && (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total')
);

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
    estimateRowHeight,
    keyMapper, // function to get a key from a row, if needed
    cacheKey,
    excludeGrandTotal = false,
    stateEpoch = 0,
    sessionId = 'anonymous',
    clientInstance = 'default',
    tableName = null,
    abortGeneration = 0,
    structuralInFlight = false,
    requestVersionRef: externalRequestVersionRef = null,
    colStart = null,
    colEnd = null,
    needsColSchema = false,
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
        pruneToRange,
        cacheVersion,
        getLoadedBlocks
    } = useRowCache({ blockSize });

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
    // Throttle pruneToRange: only run when the viewport block range shifts by ≥1 block.
    const lastPrunedRangeRef = useRef({ startBlock: -1, endBlock: -1 });

    // 2. Initialize Virtualizer
    // We use the full rowCount for serverSide
    const rowVirtualizer = useVirtualizer({
        count: serverSide ? (rowCount || 0) : (data ? data.length : 0),
        getScrollElement: () => parentRef.current,
        estimateSize: estimateRowHeight || (() => rowHeight),
        overscan: 12 // Keep DOM footprint small; block-based prefetch covers further rows
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
    }, [serverSide, stateEpoch, setCurrentEpoch]);

    useEffect(() => {
        if (!serverSide || !clearCache) return;
        clearCache();
        inflightRequestRef.current = null;
        pendingViewportRef.current = null;
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
                const isCompleteBlock = relStart === 0 && blockRows.length >= expectedRowsForBlock;

                // Use dataVersion for stale response protection
                setBlockLoaded(b, blockRows, dataVersion, isCompleteBlock, stateEpoch, lastRequestedColStartRef.current, lastRequestedColEndRef.current);
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
    }, [data, dataOffset, dataVersion, excludeGrandTotal, serverSide, blockSize, rowCount, setBlockLoaded, stateEpoch]);

    const requestViewport = useCallback((firstRow, lastRow, blocksNeeded) => {
        if (!setProps || blocksNeeded.length === 0) return false;
        const abortSignal = viewportAbortControllerRef.current ? viewportAbortControllerRef.current.signal : null;
        if (abortSignal && abortSignal.aborted) return false;

        const minBlock = Math.min(...blocksNeeded);
        const maxBlock = Math.max(...blocksNeeded);
        const reqStart = minBlock * blockSize;
        const reqEnd = (maxBlock + 1) * blockSize - 1;

        const inflight = inflightRequestRef.current;
        const inflightIsFresh = inflight && (Date.now() - inflight.timestamp <= 5000);

        if (
            inflightIsFresh &&
            inflight.start === reqStart &&
            inflight.end === reqEnd &&
            inflight.colStart === colStart &&
            inflight.colEnd === colEnd &&
            inflight.abortGeneration === abortGeneration &&
            inflight.stateEpoch === stateEpoch
        ) {
            debugLog('skip-duplicate-viewport', {
                firstRow,
                lastRow,
                reqStart,
                reqEnd,
                inflightVersion: inflight.version,
                stateEpoch,
                abortGeneration
            });
            return false;
        }

        const newVersion = requestVersionRef.current + 1;
        requestVersionRef.current = newVersion;
        const previousColStart = lastRequestedColStartRef.current;
        const previousColEnd = lastRequestedColEndRef.current;
        const hasColumnWindow = colStart !== null && colEnd !== null;
        const columnRangeChanged = hasColumnWindow && (
            previousColStart !== colStart ||
            previousColEnd !== colEnd
        );
        const visibleColumnCount = hasColumnWindow ? Math.max(0, colEnd - colStart + 1) : 0;
        const overlapStart = hasColumnWindow && previousColStart !== null && previousColEnd !== null
            ? Math.max(previousColStart, colStart)
            : null;
        const overlapEnd = hasColumnWindow && previousColStart !== null && previousColEnd !== null
            ? Math.min(previousColEnd, colEnd)
            : null;
        const overlapCount = overlapStart !== null && overlapEnd !== null && overlapEnd >= overlapStart
            ? overlapEnd - overlapStart + 1
            : 0;
        const columnDeltaCount = columnRangeChanged
            ? Math.max(1, visibleColumnCount - overlapCount)
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

        const reqCount = reqEnd - reqStart + 1;
        inflightRequestRef.current = {
            start: reqStart,
            end: reqEnd,
            colStart,
            colEnd,
            version: newVersion,
            timestamp: Date.now(),
            abortGeneration,
            stateEpoch
        };
        if (typeof onViewportRequest === 'function') {
            onViewportRequest({
                version: newVersion,
                reqStart,
                reqEnd,
                colStart,
                colEnd,
                hasColumnWindow,
                columnRangeChanged,
                columnDeltaCount,
                visibleColumnCount,
            });
        }

        // Stamp the col range into refs BEFORE setProps so the data-sync effect
        // always reads the correct range even if the response arrives synchronously.
        lastRequestedColStartRef.current = colStart;
        lastRequestedColEndRef.current = colEnd;

        setProps({
            viewport: {
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
                col_start: colStart !== null ? colStart : undefined,
                col_end: colEnd !== null ? colEnd : undefined,
                needs_col_schema: needsColSchema || undefined,
                include_grand_total: excludeGrandTotal || undefined,
            }
        });

        debugLog('request-viewport', {
            firstRow,
            lastRow,
            blocksNeeded,
            reqStart,
            reqEnd,
            previousColStart,
            previousColEnd,
            colStart,
            colEnd,
            columnRangeChanged,
            columnDeltaCount,
            visibleColumnCount,
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
    ]);

    // 4. Check Viewport & Trigger Fetch (with Debounce)
    useEffect(() => {
        if (!serverSide || !setProps || virtualRows.length === 0) return;

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
                // Column window mismatch: block needs a fresh fetch because its col range
                // doesn't match the current window.  Two cases:
                // 1. Block was loaded in full-data mode (colEnd=null/undefined) but the
                //    client is now in windowed mode — needs re-fetch with windowed cols.
                // 2. Block was loaded in windowed mode with a different col range.
                const blockHasColMeta = block && block.colEnd !== null && block.colEnd !== undefined;
                const isColMismatch = block && block.status === 'loaded' && colEnd !== null && (
                    !blockHasColMeta ||
                    block.colStart !== colStart ||
                    block.colEnd !== colEnd
                );
                const inflightColMismatch = block && block.status === 'loading' && currentInflight && (
                    currentInflight.colStart !== colStart ||
                    currentInflight.colEnd !== colEnd
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
        if (!serverSide || structuralInFlight || !pendingViewportRef.current || virtualRows.length === 0) return;

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
            // Column window mismatch: block was fetched with a different col range.
            // Also fires when a block has no col metadata at all (fetched before windowing
            // was active) but we're now in windowed mode — treat as mismatch so it re-fetches.
            const blockHasColMetaReplay = block && block.colEnd !== null && block.colEnd !== undefined;
            const isColMismatch = block && block.status === 'loaded' && colEnd !== null && (
                !blockHasColMetaReplay ||
                block.colStart !== colStart ||
                block.colEnd !== colEnd
            );
            const inflightColMismatch = block && block.status === 'loading' && currentInflight && (
                currentInflight.colStart !== colStart ||
                currentInflight.colEnd !== colEnd
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
                const blockHasColMeta = block.colEnd !== null && block.colEnd !== undefined;
                const blockColMismatch = colEnd !== null && (
                    !blockHasColMeta ||
                    block.colStart !== colStart ||
                    block.colEnd !== colEnd
                );
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
            const blockHasColMeta = block.colEnd !== null && block.colEnd !== undefined;
            const blockColMismatch = colEnd !== null && (
                !blockHasColMeta ||
                block.colStart !== colStart ||
                block.colEnd !== colEnd
            );
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
        renderedOffset: renderedDataInfo.offset
    };
};
