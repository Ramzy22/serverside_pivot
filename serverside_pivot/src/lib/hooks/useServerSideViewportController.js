import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { estimateUrgentHorizontalColumnRange } from '../utils/horizontalViewport';

/** @type {Record<string, any>} */
const DEFAULT_VISIBLE_RANGE = { start: 0, end: 0 };

/** @param {any} [left] @param {any} [right] @returns {any} */
const rangesEqual = (left, right) =>
    left?.start === right?.start && left?.end === right?.end;

/** @param {any} [value] @returns {any} */
const normalizeVisibleRange = (value) => {
    if (!value || typeof value !== 'object') return DEFAULT_VISIBLE_RANGE;
    /** @type {any} */
    const start = Number(value.start);
    /** @type {any} */
    const end = Number(value.end);
    if (!Number.isFinite(start) || !Number.isFinite(end)) return DEFAULT_VISIBLE_RANGE;
    /** @type {any} */
    const safeStart = Math.max(0, Math.floor(start));
    /** @type {any} */
    const safeEnd = Math.max(safeStart, Math.floor(end));
    return { start: safeStart, end: safeEnd };
};

/** @param {any} p0 @returns {any} */
export const useServerSideViewportController = ({
    serverSide,
    effectiveRowCount,
    responseColumns,
    responseColSchema = null,
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
    /** @type {any} */
    const latestRequestedViewportRef = useRef(null);
    /** @type {any} */
    const latestRequestedColumnWindowRef = useRef({ start: null, end: null });
    /** @type {[any, (...args: any[]) => any]} */
    const [visibleColRange, setVisibleColRange] = useState(DEFAULT_VISIBLE_RANGE);
    /** @type {any} */
    const visibleColRangeRef = useRef(DEFAULT_VISIBLE_RANGE);
    /** @type {any} */
    const colRequestStartRef = useRef(null);
    /** @type {any} */
    const colRequestEndRef = useRef(null);
    /** @type {any} */
    const needsColSchemaRef = useRef(true);
    /** @type {any} */
    const pendingRequestVersionsRef = useRef(new Set());
    /** @type {any} */
    const visiblePendingRequestVersionsRef = useRef(new Set());
    /** @type {any} */
    const pendingHorizontalRequestVersionsRef = useRef(new Set());
    /** @type {any} */
    const loadingDelayTimerRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [cachedColSchema, setCachedColSchema] = useState(null);
    /** @type {any} */
    const colSchemaEpochRef = useRef(-1);
    /** @type {[any, (...args: any[]) => any]} */
    const [pendingHorizontalColumnCount, setPendingHorizontalColumnCount] = useState(0);
    /** @type {[any, (...args: any[]) => any]} */
    const [isHorizontalColumnRequestPending, setIsHorizontalColumnRequestPending] = useState(false);
    /** @type {[any, (...args: any[]) => any]} */
    const [isRequestPending, setIsRequestPending] = useState(false);
    /** @type {[any, (...args: any[]) => any]} */
    const [columnRangeUrgencyToken, setColumnRangeUrgencyToken] = useState(0);
    /** @type {any} */
    const lastObservedHorizontalScrollLeftRef = useRef(0);
    /** @type {any} */
    const lastFastHorizontalRangeRef = useRef({ start: -1, end: -1, dispatchedAt: 0 });
    /** @type {any} */
    const deferredHorizontalMetricsRef = useRef(null);

    /** @type {(...args: any[]) => any} */
    const updateVisibleColRange = useCallback((updater) => {
        setVisibleColRange(/** @param {any} [previousRange] @returns {any} */ (previousRange) => {
            /** @type {any} */
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

    /** @type {any} */
    const responseSchema = useMemo(() => {
        if (responseColSchema && typeof responseColSchema === 'object') {
            return responseColSchema;
        }
        if (!Array.isArray(responseColumns)) {
            return null;
        }
        /** @type {any} */
        const schemaEntry = responseColumns.find((column) => (
            column
            && typeof column === 'object'
            && column.id === '__col_schema'
            && column.col_schema
            && typeof column.col_schema === 'object'
        ));
        return schemaEntry ? schemaEntry.col_schema : null;
    }, [responseColSchema, responseColumns]);

    /** @type {any} */
    const responseSchemaWindow = useMemo(() => {
        if (!serverSide || !responseSchema) {
            return { start: null, end: null };
        }
        /** @type {any} */
        const schemaColumns = Array.isArray(responseSchema.columns)
            ? responseSchema.columns
            : [];
        if (schemaColumns.length === 0) {
            return { start: null, end: null };
        }
        /** @type {any} */
        const firstIndex = coerceTransportNumber(schemaColumns[0] && schemaColumns[0].index, null);
        /** @type {any} */
        const lastIndex = coerceTransportNumber(schemaColumns[schemaColumns.length - 1] && schemaColumns[schemaColumns.length - 1].index, null);
        if (!Number.isFinite(firstIndex) || !Number.isFinite(lastIndex)) {
            return { start: null, end: null };
        }
        return {
            start: Math.max(0, Math.floor(firstIndex)),
            end: Math.max(Math.floor(firstIndex), Math.floor(lastIndex)),
        };
    }, [coerceTransportNumber, responseSchema, serverSide]);

    /** @type {(...args: any[]) => any} */
    const markRequestPending = useCallback((requestMeta) => {
        /** @type {any} */
        const normalizedMeta = requestMeta && typeof requestMeta === 'object'
            ? requestMeta
            : { version: requestMeta };
        if (
            Number.isFinite(Number(normalizedMeta.reqStart))
            && Number.isFinite(Number(normalizedMeta.reqEnd))
        ) {
            /** @type {any} */
            const reqStart = Number(normalizedMeta.reqStart);
            /** @type {any} */
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
        /** @type {any} */
        const numericVersion = Number(normalizedMeta.version);
        /** @type {any} */
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

    /** @type {(...args: any[]) => any} */
    const resolvePendingRequest = useCallback((requestMeta) => {
        /** @type {any} */
        const normalizedMeta = requestMeta && typeof requestMeta === 'object'
            ? requestMeta
            : { version: requestMeta };
        /** @type {any} */
        const numericVersion = Number(
            normalizedMeta.version !== undefined
                ? normalizedMeta.version
                : (normalizedMeta.windowSeq ?? normalizedMeta.window_seq)
        );
        if (!Number.isFinite(numericVersion)) return false;

        let changed = false;
        if (pendingRequestVersionsRef.current.delete(numericVersion)) {
            changed = true;
        }
        if (visiblePendingRequestVersionsRef.current.delete(numericVersion)) {
            changed = true;
        }
        if (pendingHorizontalRequestVersionsRef.current.delete(numericVersion)) {
            changed = true;
        }
        if (!changed) return false;

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
        return true;
    }, []);

    useEffect(() => {
        /** @type {any} */
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
        /** @type {any} */
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
        setColumnRangeUrgencyToken(/** @param {any} [previousToken] @returns {any} */ (previousToken) => previousToken + 1);
    }, [serverSide, stateEpoch]);

    useEffect(() => {
        if (!serverSide) return;
        setCachedColSchema(null);
    }, [serverSide, stateEpoch]);

    useEffect(() => {
        if (!serverSide || !responseSchema) return;

        const pendingStructural = structuralPendingVersionRef.current;
        /** @type {any} */
        const numericVersion = Number(dataVersion);
        /** @type {any} */
        const schemaIsFreshForCurrentEpoch = !structuralInFlight
            || !pendingStructural
            || (Number.isFinite(numericVersion) && numericVersion >= pendingStructural.version);

        if (!schemaIsFreshForCurrentEpoch) return;

        setCachedColSchema(/** @param {any} [previousSchema] @returns {any} */ (previousSchema) => (
            mergeSparseColSchema(previousSchema, responseSchema, schemaFallbackWidth)
        ));
        colSchemaEpochRef.current = stateEpoch;
    }, [
        dataVersion,
        mergeSparseColSchema,
        responseSchema,
        schemaFallbackWidth,
        serverSide,
        stateEpoch,
        structuralInFlight,
        structuralPendingVersionRef,
    ]);

    useEffect(() => {
        if (serverSide || cachedColSchema) return;
        if (!filteredData || filteredData.length === 0) return;
        /** @type {any} */
        const rowMetaKeys = new Set(['_id', '_path', '_isTotal', '_level', '_expanded',
            '_parentPath', '_has_children', '_is_expanded', 'depth', 'uuid', 'subRows', '__virtualIndex']);
        /** @type {any} */
        const ignoredIds = new Set([...rowFields, ...colFields, '_isTotal']);
        /** @type {any[]} */
        const colIds = [];
        /** @type {any} */
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

    /** @type {any} */
    const totalCenterCols = cachedColSchema
        ? coerceTransportNumber(
            cachedColSchema.total_center_cols,
            Array.isArray(cachedColSchema.columns) ? cachedColSchema.columns.length : null
        )
        : null;

    /** @type {any} */
    const maxCenterIndex = totalCenterCols !== null ? Math.max(totalCenterCols - 1, 0) : null;
    /** @type {any} */
    const normalizedVisibleColStart = (serverSide && totalCenterCols !== null && totalCenterCols > 0)
        ? Math.max(0, Math.min(Math.floor(visibleColRange.start || 0), maxCenterIndex))
        : null;
    /** @type {any} */
    const normalizedVisibleColEnd = (serverSide && totalCenterCols !== null && totalCenterCols > 0)
        ? Math.max(
            normalizedVisibleColStart !== null ? normalizedVisibleColStart : 0,
            Math.min(Math.floor(visibleColRange.end || 0), maxCenterIndex)
        )
        : null;

    /** @type {any} */
    const schemaMissingForVisibleRange = Boolean(
        serverSide
        && cachedColSchema
        && colSchemaEpochRef.current === stateEpoch
        && totalCenterCols !== null
        && totalCenterCols > 0
        && !isSparseSchemaRangeLoaded(cachedColSchema, normalizedVisibleColStart, normalizedVisibleColEnd)
    );

    /** @type {any} */
    const needsColSchema = !cachedColSchema
        || colSchemaEpochRef.current !== stateEpoch
        || schemaMissingForVisibleRange;

    /** @type {any} */
    const colRequestStart = (serverSide && totalCenterCols !== null && totalCenterCols > 0)
        ? normalizedVisibleColStart
        : null;

    /** @type {any} */
    const colRequestEnd = (serverSide && totalCenterCols !== null && totalCenterCols > 0)
        ? normalizedVisibleColEnd
        : null;

    colRequestStartRef.current = colRequestStart;
    colRequestEndRef.current = colRequestEnd;
    needsColSchemaRef.current = needsColSchema;

    /** @type {(...args: any[]) => any} */
    const resolveStableRequestedColumnWindow = useCallback(() => {
        /** @type {any} */
        const currentStart = Number.isFinite(Number(colRequestStartRef.current))
            ? Number(colRequestStartRef.current)
            : null;
        /** @type {any} */
        const currentEnd = Number.isFinite(Number(colRequestEndRef.current))
            ? Number(colRequestEndRef.current)
            : null;
        /** @type {any} */
        const latestStart = Number.isFinite(Number(latestRequestedColumnWindowRef.current?.start))
            ? Number(latestRequestedColumnWindowRef.current.start)
            : null;
        /** @type {any} */
        const latestEnd = Number.isFinite(Number(latestRequestedColumnWindowRef.current?.end))
            ? Number(latestRequestedColumnWindowRef.current.end)
            : null;

        /** @type {any} */
        const candidateStarts = [currentStart, latestStart].filter((value) => value !== null);
        /** @type {any} */
        const candidateEnds = [currentEnd, latestEnd].filter((value) => value !== null);
        if (candidateStarts.length === 0 || candidateEnds.length === 0) {
            return { start: null, end: null };
        }
        return {
            start: Math.min(...candidateStarts),
            end: Math.max(...candidateEnds),
        };
    }, []);

    /** @type {(...args: any[]) => any} */
    const syncPreciseVisibleColRange = useCallback((nextPreciseRange, options = {}) => {
        /** @type {any} */
        const nextRange = normalizeVisibleRange(nextPreciseRange);
        /** @type {any} */
        const preserveWiderRange = Boolean(options.preserveWiderRange);
        updateVisibleColRange(/** @param {any} [previousRange] @returns {any} */ (previousRange) => {
            const recentFastRange = lastFastHorizontalRangeRef.current;
            /** @type {any} */
            const preserveRecentUrgentRange = recentFastRange.start >= 0
                && recentFastRange.end >= recentFastRange.start
                && (Date.now() - recentFastRange.dispatchedAt) < 240
                && previousRange.start <= nextRange.start
                && previousRange.end >= nextRange.end;
            /** @type {any} */
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

    /** @type {(...args: any[]) => any} */
    const handleHorizontalScrollMetrics = useCallback((metrics) => {
        /** @type {any} */
        const scrollLeft = Number(metrics && metrics.scrollLeft);
        /** @type {any} */
        const clientWidth = Number(metrics && metrics.clientWidth);
        /** @type {any} */
        const scrollWidth = Number(metrics && metrics.scrollWidth);
        if (!Number.isFinite(scrollLeft) || !Number.isFinite(clientWidth) || !Number.isFinite(scrollWidth)) {
            return;
        }

        if (!serverSide) {
            lastObservedHorizontalScrollLeftRef.current = scrollLeft;
            return;
        }
        if (structuralInFlight || isRequestPending || isHorizontalColumnRequestPending) {
            deferredHorizontalMetricsRef.current = metrics;
            return;
        }

        /** @type {any} */
        const delta = Math.abs(scrollLeft - lastObservedHorizontalScrollLeftRef.current);
        lastObservedHorizontalScrollLeftRef.current = scrollLeft;

        /** @type {any} */
        const averageColumnWidth = Math.max(
            48,
            Number.isFinite(Number(metrics && metrics.averageCenterColumnWidth))
                ? Number(metrics.averageCenterColumnWidth)
                : schemaFallbackWidth
        );
        /** @type {any} */
        const bigJumpThreshold = Math.max(
            clientWidth * 0.6,
            averageColumnWidth * 10
        );
        if (delta < bigJumpThreshold) return;

        /** @type {any} */
        const fallbackCenterCount = Number.isFinite(Number(metrics && metrics.centerColumnCount))
            ? Math.max(0, Math.floor(Number(metrics.centerColumnCount)))
            : 0;
        /** @type {any} */
        const resolvedCenterCount = Number.isFinite(Number(totalCenterCols))
            ? Math.max(0, Math.floor(Number(totalCenterCols)))
            : fallbackCenterCount;
        /** @type {any} */
        const estimatedRange = estimateUrgentHorizontalColumnRange({
            scrollLeft,
            clientWidth,
            scrollWidth,
            averageColumnWidth,
            centerColumnCount: resolvedCenterCount,
        });
        if (!estimatedRange) return;
        const { start: estimatedStart, end: estimatedEnd, atRightEdge } = estimatedRange;
        /** @type {any} */
        const maxIndex = Math.max(resolvedCenterCount - 1, 0);

        const previousFastRange = lastFastHorizontalRangeRef.current;
        /** @type {any} */
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

        setColumnRangeUrgencyToken(/** @param {any} [previousToken] @returns {any} */ (previousToken) => previousToken + 1);
        updateVisibleColRange({
            start: estimatedStart,
            end: estimatedEnd,
        });

        /** @type {any} */
        const urgentViewportRequester = requestUrgentColumnViewportRef && requestUrgentColumnViewportRef.current;
        if (typeof urgentViewportRequester === 'function') {
            urgentViewportRequester(estimatedStart, estimatedEnd);
        }
    }, [
        isHorizontalColumnRequestPending,
        isRequestPending,
        requestUrgentColumnViewportRef,
        schemaFallbackWidth,
        serverSide,
        structuralInFlight,
        totalCenterCols,
        updateVisibleColRange,
    ]);

    useEffect(() => {
        if (!serverSide || structuralInFlight || isRequestPending || isHorizontalColumnRequestPending) {
            return undefined;
        }
        const deferredMetrics = deferredHorizontalMetricsRef.current;
        if (!deferredMetrics) return undefined;
        deferredHorizontalMetricsRef.current = null;
        /** @type {any} */
        const timer = setTimeout(() => {
            handleHorizontalScrollMetrics(deferredMetrics);
        }, 0);
        return () => {
            clearTimeout(timer);
        };
    }, [
        handleHorizontalScrollMetrics,
        isHorizontalColumnRequestPending,
        isRequestPending,
        serverSide,
        structuralInFlight,
    ]);

    /** @type {(...args: any[]) => any} */
    const resetVisibleColRange = useCallback((visibleCountHint = null) => {
        /** @type {any} */
        const currentRange = visibleColRangeRef.current || DEFAULT_VISIBLE_RANGE;
        /** @type {any} */
        const currentCount = Math.max(1, currentRange.end - currentRange.start + 1);
        /** @type {any} */
        const nextVisibleCount = Math.max(1, Number(visibleCountHint) || currentCount);
        /** @type {Record<string, any>} */
        const nextRange = {
            start: 0,
            end: Math.max(0, nextVisibleCount - 1),
        };
        lastFastHorizontalRangeRef.current = { start: -1, end: -1, dispatchedAt: 0 };
        setColumnRangeUrgencyToken(/** @param {any} [previousToken] @returns {any} */ (previousToken) => previousToken + 1);
        updateVisibleColRange(nextRange);
    }, [updateVisibleColRange]);

    /** @type {any} */
    const serverSideBlockSize = useMemo(() => {
        if (!serverSide) return 48;
        /** @type {any} */
        const configuredCacheBlockSize = performanceConfig && performanceConfig.cacheBlockSize;
        /** @type {any} */
        const configuredBlockSize = (
            configuredCacheBlockSize !== null
            && configuredCacheBlockSize !== undefined
            && Number.isFinite(Number(configuredCacheBlockSize))
        )
            ? Math.max(16, Math.min(1024, Math.floor(Number(performanceConfig.cacheBlockSize))))
            : null;
        if (configuredBlockSize !== null) return configuredBlockSize;
        /** @type {any} */
        const numericRowCount = Number.isFinite(Number(effectiveRowCount)) ? Number(effectiveRowCount) : 0;
        /** @type {any} */
        const numericCenterCols = Number.isFinite(Number(totalCenterCols)) ? Number(totalCenterCols) : 0;
        if (numericCenterCols >= 20000) return 32;
        if (numericRowCount >= 100000 || numericCenterCols >= 500) return 40;
        return 48;
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
        resolvePendingRequest,
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
