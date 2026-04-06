import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

const DEFAULT_VISIBLE_RANGE = { start: 0, end: 0 };

const rangesEqual = (left, right) =>
    left?.start === right?.start && left?.end === right?.end;

const normalizeVisibleRange = (value) => {
    if (!value || typeof value !== 'object') return DEFAULT_VISIBLE_RANGE;
    const start = Number(value.start);
    const end = Number(value.end);
    if (!Number.isFinite(start) || !Number.isFinite(end)) return DEFAULT_VISIBLE_RANGE;
    const safeStart = Math.max(0, Math.floor(start));
    const safeEnd = Math.max(safeStart, Math.floor(end));
    return { start: safeStart, end: safeEnd };
};

export const useServerSideViewportController = ({
    serverSide,
    effectiveRowCount,
    responseColumns,
    dataVersion,
    stateEpoch,
    structuralInFlight,
    structuralPendingVersionRef,
    filteredData,
    rowFields,
    colFields,
    schemaFallbackWidth = 140,
    requestVersionRef,
    latestDataVersionRef,
    requestUrgentColumnViewportRef = null,
    coerceTransportNumber,
    mergeSparseColSchema,
    isSparseSchemaRangeLoaded,
    performanceConfig = null,
}) => {
    const latestRequestedViewportRef = useRef(null);
    const latestRequestedColumnWindowRef = useRef({ start: null, end: null });
    const [visibleColRange, setVisibleColRange] = useState(DEFAULT_VISIBLE_RANGE);
    const visibleColRangeRef = useRef(DEFAULT_VISIBLE_RANGE);
    const colRequestStartRef = useRef(null);
    const colRequestEndRef = useRef(null);
    const needsColSchemaRef = useRef(true);
    const pendingRequestVersionsRef = useRef(new Set());
    const visiblePendingRequestVersionsRef = useRef(new Set());
    const pendingHorizontalRequestVersionsRef = useRef(new Set());
    const loadingDelayTimerRef = useRef(null);
    const [cachedColSchema, setCachedColSchema] = useState(null);
    const colSchemaEpochRef = useRef(-1);
    const [pendingHorizontalColumnCount, setPendingHorizontalColumnCount] = useState(0);
    const [isHorizontalColumnRequestPending, setIsHorizontalColumnRequestPending] = useState(false);
    const [isRequestPending, setIsRequestPending] = useState(false);
    const [columnRangeUrgencyToken, setColumnRangeUrgencyToken] = useState(0);
    const lastObservedHorizontalScrollLeftRef = useRef(0);
    const lastFastHorizontalRangeRef = useRef({ start: -1, end: -1, dispatchedAt: 0 });

    const updateVisibleColRange = useCallback((updater) => {
        setVisibleColRange((previousRange) => {
            const nextRange = normalizeVisibleRange(
                typeof updater === 'function' ? updater(previousRange) : updater
            );
            if (rangesEqual(previousRange, nextRange)) return previousRange;
            visibleColRangeRef.current = nextRange;
            return nextRange;
        });
    }, []);

    useEffect(() => {
        visibleColRangeRef.current = visibleColRange;
    }, [visibleColRange]);

    const responseSchemaWindow = useMemo(() => {
        if (!serverSide || !Array.isArray(responseColumns)) {
            return { start: null, end: null };
        }
        const schemaEntry = responseColumns.find((column) => column && column.id === '__col_schema');
        const schemaColumns = schemaEntry && schemaEntry.col_schema && Array.isArray(schemaEntry.col_schema.columns)
            ? schemaEntry.col_schema.columns
            : [];
        if (schemaColumns.length === 0) {
            return { start: null, end: null };
        }
        const firstIndex = coerceTransportNumber(schemaColumns[0] && schemaColumns[0].index, null);
        const lastIndex = coerceTransportNumber(schemaColumns[schemaColumns.length - 1] && schemaColumns[schemaColumns.length - 1].index, null);
        if (!Number.isFinite(firstIndex) || !Number.isFinite(lastIndex)) {
            return { start: null, end: null };
        }
        return {
            start: Math.max(0, Math.floor(firstIndex)),
            end: Math.max(Math.floor(firstIndex), Math.floor(lastIndex)),
        };
    }, [coerceTransportNumber, responseColumns, serverSide]);

    const markRequestPending = useCallback((requestMeta) => {
        const normalizedMeta = requestMeta && typeof requestMeta === 'object'
            ? requestMeta
            : { version: requestMeta };
        if (
            Number.isFinite(Number(normalizedMeta.reqStart))
            && Number.isFinite(Number(normalizedMeta.reqEnd))
        ) {
            const reqStart = Number(normalizedMeta.reqStart);
            const reqEnd = Number(normalizedMeta.reqEnd);
            latestRequestedViewportRef.current = {
                start: reqStart,
                end: reqEnd,
                count: Math.max(1, reqEnd - reqStart + 1),
            };
        }
        if (
            Number.isFinite(Number(normalizedMeta.colStart))
            && Number.isFinite(Number(normalizedMeta.colEnd))
        ) {
            latestRequestedColumnWindowRef.current = {
                start: Number(normalizedMeta.colStart),
                end: Number(normalizedMeta.colEnd),
            };
        }
        const numericVersion = Number(normalizedMeta.version);
        const isVisiblePendingRequest = normalizedMeta.silent !== true;
        if (Number.isFinite(numericVersion)) {
            pendingRequestVersionsRef.current.add(numericVersion);
            if (isVisiblePendingRequest) {
                visiblePendingRequestVersionsRef.current.add(numericVersion);
            }
        }
        if (
            Number.isFinite(numericVersion) &&
            normalizedMeta.columnRangeChanged &&
            normalizedMeta.hasColumnWindow
        ) {
            pendingHorizontalRequestVersionsRef.current.add(numericVersion);
            setIsHorizontalColumnRequestPending(true);
            setPendingHorizontalColumnCount(Math.max(
                1,
                Math.min(
                    normalizedMeta.columnDeltaCount || normalizedMeta.visibleColumnCount || 1,
                    Math.max(normalizedMeta.visibleColumnCount || 1, 6)
                )
            ));
        }
        if (!isVisiblePendingRequest || isRequestPending || loadingDelayTimerRef.current !== null) return;
        loadingDelayTimerRef.current = setTimeout(() => {
            loadingDelayTimerRef.current = null;
            if (visiblePendingRequestVersionsRef.current.size > 0) {
                setIsRequestPending(true);
            }
        }, 200);
    }, [isRequestPending]);

    useEffect(() => {
        const numericVersion = Number(dataVersion);
        if (!Number.isFinite(numericVersion)) return;
        latestDataVersionRef.current = numericVersion;
        if (numericVersion > requestVersionRef.current) {
            requestVersionRef.current = numericVersion;
        }
        for (const pendingVersion of Array.from(pendingRequestVersionsRef.current)) {
            if (pendingVersion <= numericVersion) {
                pendingRequestVersionsRef.current.delete(pendingVersion);
            }
        }
        for (const pendingVersion of Array.from(visiblePendingRequestVersionsRef.current)) {
            if (pendingVersion <= numericVersion) {
                visiblePendingRequestVersionsRef.current.delete(pendingVersion);
            }
        }
        for (const pendingVersion of Array.from(pendingHorizontalRequestVersionsRef.current)) {
            if (pendingVersion <= numericVersion) {
                pendingHorizontalRequestVersionsRef.current.delete(pendingVersion);
            }
        }
        if (visiblePendingRequestVersionsRef.current.size === 0) {
            if (loadingDelayTimerRef.current !== null) {
                clearTimeout(loadingDelayTimerRef.current);
                loadingDelayTimerRef.current = null;
            }
            setIsRequestPending(false);
        }
        if (pendingHorizontalRequestVersionsRef.current.size === 0) {
            setIsHorizontalColumnRequestPending(false);
            setPendingHorizontalColumnCount(0);
        }
    }, [dataVersion, latestDataVersionRef, requestVersionRef]);

    useEffect(() => () => {
        if (loadingDelayTimerRef.current !== null) {
            clearTimeout(loadingDelayTimerRef.current);
            loadingDelayTimerRef.current = null;
        }
    }, []);

    useEffect(() => {
        if (!isRequestPending) return;
        const timeoutId = setTimeout(() => {
            pendingRequestVersionsRef.current.clear();
            visiblePendingRequestVersionsRef.current.clear();
            pendingHorizontalRequestVersionsRef.current.clear();
            setIsRequestPending(false);
            setIsHorizontalColumnRequestPending(false);
            setPendingHorizontalColumnCount(0);
        }, 15000);
        return () => clearTimeout(timeoutId);
    }, [isRequestPending]);

    useEffect(() => {
        if (!serverSide) return;
        pendingRequestVersionsRef.current.clear();
        pendingHorizontalRequestVersionsRef.current.clear();
        visiblePendingRequestVersionsRef.current.clear();
        if (loadingDelayTimerRef.current !== null) {
            clearTimeout(loadingDelayTimerRef.current);
            loadingDelayTimerRef.current = null;
        }
        setIsRequestPending(false);
        setIsHorizontalColumnRequestPending(false);
        setPendingHorizontalColumnCount(0);
        latestRequestedViewportRef.current = null;
        latestRequestedColumnWindowRef.current = { start: null, end: null };
        lastObservedHorizontalScrollLeftRef.current = 0;
        lastFastHorizontalRangeRef.current = { start: -1, end: -1, dispatchedAt: 0 };
        visibleColRangeRef.current = DEFAULT_VISIBLE_RANGE;
        setVisibleColRange(DEFAULT_VISIBLE_RANGE);
        setColumnRangeUrgencyToken((previousToken) => previousToken + 1);
    }, [serverSide, stateEpoch]);

    useEffect(() => {
        if (!serverSide) return;
        setCachedColSchema(null);
    }, [serverSide, stateEpoch]);

    useEffect(() => {
        if (!serverSide || !responseColumns) return;
        const schemaEntry = responseColumns.find((column) => column && column.id === '__col_schema');
        if (!schemaEntry || !schemaEntry.col_schema) return;

        const pendingStructural = structuralPendingVersionRef.current;
        const numericVersion = Number(dataVersion);
        const schemaIsFreshForCurrentEpoch = !structuralInFlight
            || !pendingStructural
            || (Number.isFinite(numericVersion) && numericVersion >= pendingStructural.version);

        if (!schemaIsFreshForCurrentEpoch) return;

        setCachedColSchema((previousSchema) => (
            mergeSparseColSchema(previousSchema, schemaEntry.col_schema, schemaFallbackWidth)
        ));
        colSchemaEpochRef.current = stateEpoch;
    }, [
        dataVersion,
        mergeSparseColSchema,
        responseColumns,
        schemaFallbackWidth,
        serverSide,
        stateEpoch,
        structuralInFlight,
        structuralPendingVersionRef,
    ]);

    useEffect(() => {
        if (serverSide || cachedColSchema) return;
        if (!filteredData || filteredData.length === 0) return;
        const rowMetaKeys = new Set(['_id', '_path', '_isTotal', '_level', '_expanded',
            '_parentPath', '_has_children', '_is_expanded', 'depth', 'uuid', 'subRows', '__virtualIndex']);
        const ignoredIds = new Set([...rowFields, ...colFields, '_isTotal']);
        const colIds = [];
        const colIdSet = new Set();
        for (const row of filteredData) {
            if (!row) continue;
            for (const key of Object.keys(row)) {
                if (!colIdSet.has(key) && !rowMetaKeys.has(key) && !ignoredIds.has(key)) {
                    colIds.push(key);
                    colIdSet.add(key);
                }
            }
        }
        if (colIds.length > 0) {
            setCachedColSchema({
                total_center_cols: colIds.length,
                columns: colIds.map((id, index) => ({ index, id, size: schemaFallbackWidth })),
            });
            colSchemaEpochRef.current = stateEpoch;
        }
    }, [cachedColSchema, colFields, filteredData, rowFields, schemaFallbackWidth, serverSide, stateEpoch]);

    const totalCenterCols = cachedColSchema
        ? coerceTransportNumber(
            cachedColSchema.total_center_cols,
            Array.isArray(cachedColSchema.columns) ? cachedColSchema.columns.length : null
        )
        : null;

    const maxCenterIndex = totalCenterCols !== null ? Math.max(totalCenterCols - 1, 0) : null;
    const normalizedVisibleColStart = (serverSide && totalCenterCols !== null && totalCenterCols > 0)
        ? Math.max(0, Math.min(Math.floor(visibleColRange.start || 0), maxCenterIndex))
        : null;
    const normalizedVisibleColEnd = (serverSide && totalCenterCols !== null && totalCenterCols > 0)
        ? Math.max(
            normalizedVisibleColStart !== null ? normalizedVisibleColStart : 0,
            Math.min(Math.floor(visibleColRange.end || 0), maxCenterIndex)
        )
        : null;

    const schemaMissingForVisibleRange = Boolean(
        serverSide
        && cachedColSchema
        && colSchemaEpochRef.current === stateEpoch
        && totalCenterCols !== null
        && totalCenterCols > 0
        && !isSparseSchemaRangeLoaded(cachedColSchema, normalizedVisibleColStart, normalizedVisibleColEnd)
    );

    const needsColSchema = !cachedColSchema
        || colSchemaEpochRef.current !== stateEpoch
        || schemaMissingForVisibleRange;

    const colRequestStart = (serverSide && totalCenterCols !== null && totalCenterCols > 0)
        ? normalizedVisibleColStart
        : null;

    const colRequestEnd = (serverSide && totalCenterCols !== null && totalCenterCols > 0)
        ? normalizedVisibleColEnd
        : null;

    colRequestStartRef.current = colRequestStart;
    colRequestEndRef.current = colRequestEnd;
    needsColSchemaRef.current = needsColSchema;

    const resolveStableRequestedColumnWindow = useCallback(() => {
        const currentStart = Number.isFinite(Number(colRequestStartRef.current))
            ? Number(colRequestStartRef.current)
            : null;
        const currentEnd = Number.isFinite(Number(colRequestEndRef.current))
            ? Number(colRequestEndRef.current)
            : null;
        const latestStart = Number.isFinite(Number(latestRequestedColumnWindowRef.current?.start))
            ? Number(latestRequestedColumnWindowRef.current.start)
            : null;
        const latestEnd = Number.isFinite(Number(latestRequestedColumnWindowRef.current?.end))
            ? Number(latestRequestedColumnWindowRef.current.end)
            : null;

        const candidateStarts = [currentStart, latestStart].filter((value) => value !== null);
        const candidateEnds = [currentEnd, latestEnd].filter((value) => value !== null);
        if (candidateStarts.length === 0 || candidateEnds.length === 0) {
            return { start: null, end: null };
        }
        return {
            start: Math.min(...candidateStarts),
            end: Math.max(...candidateEnds),
        };
    }, []);

    const syncPreciseVisibleColRange = useCallback((nextPreciseRange, options = {}) => {
        const nextRange = normalizeVisibleRange(nextPreciseRange);
        const preserveWiderRange = Boolean(options.preserveWiderRange);
        updateVisibleColRange((previousRange) => {
            const recentFastRange = lastFastHorizontalRangeRef.current;
            const preserveRecentUrgentRange = recentFastRange.start >= 0
                && recentFastRange.end >= recentFastRange.start
                && (Date.now() - recentFastRange.dispatchedAt) < 240
                && previousRange.start <= nextRange.start
                && previousRange.end >= nextRange.end;
            const preserveRecentRightEdgeUrgentRange = recentFastRange.start >= 0
                && recentFastRange.end === previousRange.end
                && previousRange.end === nextRange.end
                && (Date.now() - recentFastRange.dispatchedAt) < 320
                && nextRange.start >= Math.max(0, previousRange.start - 2);
            if (preserveRecentUrgentRange) {
                return previousRange;
            }
            if (preserveRecentRightEdgeUrgentRange) {
                return previousRange;
            }
            if (
                preserveWiderRange
                && nextRange.start === previousRange.start
                && nextRange.end < previousRange.end
            ) {
                return previousRange;
            }
            return nextRange;
        });
    }, [updateVisibleColRange]);

    const handleHorizontalScrollMetrics = useCallback((metrics) => {
        const scrollLeft = Number(metrics && metrics.scrollLeft);
        const clientWidth = Number(metrics && metrics.clientWidth);
        const scrollWidth = Number(metrics && metrics.scrollWidth);
        if (!Number.isFinite(scrollLeft) || !Number.isFinite(clientWidth) || !Number.isFinite(scrollWidth)) {
            return;
        }

        if (structuralInFlight || !serverSide) {
            lastObservedHorizontalScrollLeftRef.current = scrollLeft;
            return;
        }

        const delta = Math.abs(scrollLeft - lastObservedHorizontalScrollLeftRef.current);
        lastObservedHorizontalScrollLeftRef.current = scrollLeft;

        const averageColumnWidth = Math.max(
            48,
            Number.isFinite(Number(metrics && metrics.averageCenterColumnWidth))
                ? Number(metrics.averageCenterColumnWidth)
                : schemaFallbackWidth
        );
        const bigJumpThreshold = Math.max(
            clientWidth * 0.6,
            averageColumnWidth * 10
        );
        if (delta < bigJumpThreshold) return;

        const fallbackCenterCount = Number.isFinite(Number(metrics && metrics.centerColumnCount))
            ? Math.max(0, Math.floor(Number(metrics.centerColumnCount)))
            : 0;
        const resolvedCenterCount = Number.isFinite(Number(totalCenterCols))
            ? Math.max(0, Math.floor(Number(totalCenterCols)))
            : fallbackCenterCount;
        if (resolvedCenterCount <= 0) return;
        const largeColumnMode = resolvedCenterCount >= 5000;
        const extremeColumnMode = resolvedCenterCount >= 20000;

        const maxIndex = Math.max(resolvedCenterCount - 1, 0);
        const leftPinnedWidth = Number.isFinite(Number(metrics && metrics.leftPinnedWidth))
            ? Math.max(0, Number(metrics.leftPinnedWidth))
            : 0;
        const atRightEdge = (scrollLeft + clientWidth) >= (scrollWidth - 1);
        const estimatedCenterOffset = Math.max(0, scrollLeft - leftPinnedWidth);
        let estimatedStart = Math.max(
            0,
            Math.min(maxIndex, Math.floor(estimatedCenterOffset / Math.max(averageColumnWidth, 1)))
        );
        const estimatedVisibleCount = Math.max(
            1,
            Math.ceil(clientWidth / Math.max(averageColumnWidth, 1))
        );
        const bufferCount = extremeColumnMode
            ? 1
            : (largeColumnMode
                ? Math.max(1, Math.min(3, Math.ceil(estimatedVisibleCount / 5)))
                : Math.max(2, Math.min(8, Math.ceil(estimatedVisibleCount / 3))));
        const edgeSafetyCount = atRightEdge
            ? (extremeColumnMode
                ? Math.max(bufferCount, Math.min(8, estimatedVisibleCount))
                : (largeColumnMode
                    ? Math.max(bufferCount, Math.min(12, Math.ceil(estimatedVisibleCount * 1.5)))
                    : Math.max(bufferCount, Math.min(24, estimatedVisibleCount * 2))))
            : bufferCount;
        let estimatedEnd = Math.min(maxIndex, estimatedStart + estimatedVisibleCount + bufferCount);
        estimatedStart = Math.max(0, estimatedStart - bufferCount);

        if (atRightEdge) {
            estimatedEnd = maxIndex;
            estimatedStart = Math.max(0, estimatedEnd - estimatedVisibleCount - edgeSafetyCount);
        }

        const previousFastRange = lastFastHorizontalRangeRef.current;
        const now = Date.now();
        if (
            atRightEdge
            && previousFastRange.end === maxIndex
            && previousFastRange.start >= 0
            && (now - previousFastRange.dispatchedAt) < 320
            && previousFastRange.start <= estimatedStart
        ) {
            return;
        }
        if (
            previousFastRange.start === estimatedStart
            && previousFastRange.end === estimatedEnd
            && (now - previousFastRange.dispatchedAt) < 180
        ) {
            return;
        }

        lastFastHorizontalRangeRef.current = {
            start: estimatedStart,
            end: estimatedEnd,
            dispatchedAt: now,
        };

        setColumnRangeUrgencyToken((previousToken) => previousToken + 1);
        updateVisibleColRange({
            start: estimatedStart,
            end: estimatedEnd,
        });

        const urgentViewportRequester = requestUrgentColumnViewportRef && requestUrgentColumnViewportRef.current;
        if (typeof urgentViewportRequester === 'function') {
            urgentViewportRequester(estimatedStart, estimatedEnd);
        }
    }, [
        requestUrgentColumnViewportRef,
        schemaFallbackWidth,
        serverSide,
        structuralInFlight,
        totalCenterCols,
        updateVisibleColRange,
    ]);

    const resetVisibleColRange = useCallback((visibleCountHint = null) => {
        const currentRange = visibleColRangeRef.current || DEFAULT_VISIBLE_RANGE;
        const currentCount = Math.max(1, currentRange.end - currentRange.start + 1);
        const nextVisibleCount = Math.max(1, Number(visibleCountHint) || currentCount);
        const nextRange = {
            start: 0,
            end: Math.max(0, nextVisibleCount - 1),
        };
        lastFastHorizontalRangeRef.current = { start: -1, end: -1, dispatchedAt: 0 };
        setColumnRangeUrgencyToken((previousToken) => previousToken + 1);
        updateVisibleColRange(nextRange);
    }, [updateVisibleColRange]);

    const serverSideBlockSize = useMemo(() => {
        if (!serverSide) return 100;
        const configuredBlockSize = Number.isFinite(Number(performanceConfig && performanceConfig.cacheBlockSize))
            ? Math.max(16, Math.min(1024, Math.floor(Number(performanceConfig.cacheBlockSize))))
            : null;
        if (configuredBlockSize !== null) return configuredBlockSize;
        const numericRowCount = Number.isFinite(Number(effectiveRowCount)) ? Number(effectiveRowCount) : 0;
        const numericCenterCols = Number.isFinite(Number(totalCenterCols)) ? Number(totalCenterCols) : 0;
        if (numericCenterCols >= 20000) return 48;
        if (numericRowCount >= 100000 || numericCenterCols >= 500) return 64;
        if (numericRowCount >= 25000 || numericCenterCols >= 250) return 80;
        return 100;
    }, [effectiveRowCount, performanceConfig, serverSide, totalCenterCols]);

    return {
        cachedColSchema,
        colRequestEnd,
        colRequestStart,
        colRequestEndRef,
        colRequestStartRef,
        colSchemaEpochRef,
        columnRangeUrgencyToken,
        handleHorizontalScrollMetrics,
        isHorizontalColumnRequestPending,
        isRequestPending,
        latestRequestedColumnWindowRef,
        latestRequestedViewportRef,
        markRequestPending,
        needsColSchema,
        needsColSchemaRef,
        pendingHorizontalColumnCount,
        resetVisibleColRange,
        responseSchemaWindow,
        resolveStableRequestedColumnWindow,
        serverSideBlockSize,
        syncPreciseVisibleColRange,
        totalCenterCols,
        visibleColRange,
    };
};
