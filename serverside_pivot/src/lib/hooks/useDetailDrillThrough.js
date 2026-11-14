import { useState, useCallback, useEffect, useRef } from 'react';

/**
 * Manages detail/drill-through panel state, requests, and caching.
  @param {any} p0 @returns {any} */
export function useDetailDrillThrough({
    detailConfig,
    detailMode,
    viewMode,
    reportDef,
    treeConfig,
    rowFields,
    colFields,
    valConfigs,
    tableName,
    runtimeResponse,
    emitRuntimeRequest,
    stateEpoch,
    abortGeneration,
    sessionIdRef,
    clientInstanceRef,
    requestVersionRef,
    setPropsRef,
    profilingEnabledRef,
    pivotProfilerRef,
    profilerComponentIdRef,
}) {
    /** @type {[any, (...args: any[]) => any]} */
    const [detailSurface, setDetailSurface] = useState(null);
    /** @type {any} */
    const detailSurfaceCacheRef = useRef(new Map());
    /** @type {any} */
    const pendingDetailRequestRef = useRef(null);
    /** @type {any} */
    const toggleDetailForRowRef = useRef(null);

    /** @type {(...args: any[]) => any} */
    const getCachedDetailSurface = useCallback((rowPath) => {
        if (!detailConfig.keepDetailRows || !rowPath) return null;
        /** @type {any} */
        const cachedSurface = detailSurfaceCacheRef.current.get(rowPath);
        if (!cachedSurface) return null;
        detailSurfaceCacheRef.current.delete(rowPath);
        detailSurfaceCacheRef.current.set(rowPath, cachedSurface);
        return { ...cachedSurface };
    }, [detailConfig.keepDetailRows]);

    /** @type {(...args: any[]) => any} */
    const cacheDetailSurface = useCallback((surface) => {
        if (!detailConfig.keepDetailRows || !surface || !surface.rowPath) return;
        /** @type {Record<string, any>} */
        const nextSurface = {
            ...surface,
            loading: false,
            requestId: null,
        };
        detailSurfaceCacheRef.current.delete(nextSurface.rowPath);
        detailSurfaceCacheRef.current.set(nextSurface.rowPath, nextSurface);
        /** @type {any} */
        const maxEntries = Math.max(1, Number(detailConfig.keepDetailRowsCount) || 10);
        while (detailSurfaceCacheRef.current.size > maxEntries) {
            const oldestKey = detailSurfaceCacheRef.current.keys().next().value;
            if (!oldestKey) break;
            detailSurfaceCacheRef.current.delete(oldestKey);
        }
    }, [detailConfig.keepDetailRows, detailConfig.keepDetailRowsCount]);

    // Keep cache size in sync with config
    useEffect(() => {
        if (!detailConfig.keepDetailRows) {
            detailSurfaceCacheRef.current.clear();
            return;
        }
        /** @type {any} */
        const maxEntries = Math.max(1, Number(detailConfig.keepDetailRowsCount) || 10);
        while (detailSurfaceCacheRef.current.size > maxEntries) {
            const oldestKey = detailSurfaceCacheRef.current.keys().next().value;
            if (!oldestKey) break;
            detailSurfaceCacheRef.current.delete(oldestKey);
        }
    }, [detailConfig.keepDetailRows, detailConfig.keepDetailRowsCount]);

    // Cache completed detail surfaces
    useEffect(() => {
        if (!detailSurface || detailSurface.loading || !detailSurface.rowPath) return;
        cacheDetailSurface(detailSurface);
    }, [cacheDetailSurface, detailSurface]);

    // Sync detail mode changes
    useEffect(() => {
        setDetailSurface(/** @param {any} [previousSurface] @returns {any} */ (previousSurface) => {
            if (!previousSurface) return previousSurface;
            if (previousSurface.source === 'drill') return previousSurface;
            if (detailMode === 'none') return null;
            if (previousSurface.mode === detailMode) return previousSurface;
            return {
                ...previousSurface,
                mode: detailMode,
            };
        });
    }, [detailMode]);

    // Reset detail surface on structural changes
    useEffect(() => {
        setDetailSurface(null);
        pendingDetailRequestRef.current = null;
        detailSurfaceCacheRef.current.clear();
    }, [viewMode, reportDef, treeConfig, rowFields, colFields, valConfigs, tableName]);

    // Handle detail runtime responses
    useEffect(() => {
        if (!runtimeResponse || typeof runtimeResponse !== 'object') return;
        if (runtimeResponse.kind !== 'detail') return;
        if (
            pendingDetailRequestRef.current
            && runtimeResponse.requestId
            && runtimeResponse.requestId !== pendingDetailRequestRef.current
        ) {
            return;
        }

        /** @type {any} */
        const payload = runtimeResponse.payload && typeof runtimeResponse.payload === 'object'
            ? runtimeResponse.payload
            : {};

        if (runtimeResponse.status === 'detail_data') {
            setDetailSurface(/** @param {any} [previousSurface] @returns {any} */ (previousSurface) => ({
                ...(previousSurface || {}),
                loading: false,
                rowPath: payload.rowPath || payload.row_path || '',
                rowFields: Array.isArray(payload.rowFields) ? payload.rowFields : [],
                rowKey: payload.rowKey || payload.row_key || payload.rowPath || payload.row_path || '',
                title: payload.title || (payload.rowPath ? payload.rowPath.split('|||').slice(-1)[0] : 'Detail'),
                rows: Array.isArray(payload.rows) ? payload.rows : [],
                columns: Array.isArray(payload.columns) ? payload.columns : [],
                page: Number.isFinite(Number(payload.page)) ? Number(payload.page) : 0,
                pageSize: Number.isFinite(Number(payload.pageSize || payload.page_size)) ? Number(payload.pageSize || payload.page_size) : 100,
                totalRows: Number.isFinite(Number(payload.totalRows || payload.total_rows))
                    ? Number(payload.totalRows || payload.total_rows)
                    : (Array.isArray(payload.rows) ? payload.rows.length : 0),
                sortCol: payload.sortCol || payload.sort_col || null,
                sortDir: payload.sortDir || payload.sort_dir || 'asc',
                filterText: payload.filterText || payload.filter || '',
                detailKind: payload.detailKind || payload.detail_kind || 'records',
                mode: payload.mode || (previousSurface && previousSurface.mode) || detailMode,
                source: payload.source || (previousSurface && previousSurface.source) || 'detail',
            }));
            if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
                pivotProfilerRef.current.resolve({
                    requestId: runtimeResponse.requestId,
                    componentId: profilerComponentIdRef.current,
                    kind: 'detail',
                    status: runtimeResponse.status,
                });
            }
        } else if (runtimeResponse.status === 'error') {
            console.error('Detail request failed:', runtimeResponse.message || runtimeResponse);
            setDetailSurface(null);
            if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
                pivotProfilerRef.current.resolve({
                    requestId: runtimeResponse.requestId,
                    componentId: profilerComponentIdRef.current,
                    kind: 'detail',
                    status: runtimeResponse.status,
                });
            }
        }

        if (
            pendingDetailRequestRef.current
            && runtimeResponse.requestId
            && runtimeResponse.requestId === pendingDetailRequestRef.current
        ) {
            pendingDetailRequestRef.current = null;
        }
    }, [detailMode, runtimeResponse]);

    /** @type {(...args: any[]) => any} */
    const fetchDetailData = useCallback((detailRequest) => {
        if (!detailRequest || typeof detailRequest !== 'object') return;
        /** @type {any} */
        const rowPath = detailRequest.rowPath || detailRequest.row_path || '';
        /** @type {any} */
        const rowKey = detailRequest.rowKey || detailRequest.row_key || rowPath;
        /** @type {any} */
        const cachedSurface = getCachedDetailSurface(rowPath);
        /** @type {any} */
        const refreshStrategy = detailConfig.refreshStrategy || 'rows';
        /** @type {any} */
        const shouldReuseCachedSurface = Boolean(
            cachedSurface
            && detailConfig.keepDetailRows
            && refreshStrategy !== 'everything'
        );
        /** @type {any} */
        const shouldSkipRequest = Boolean(shouldReuseCachedSurface && refreshStrategy === 'nothing');
        /** @type {any} */
        const requestId = shouldSkipRequest
            ? null
            : String(
                detailRequest.requestId
                || `detail:${detailRequest.rowPath || detailRequest.row_path || detailRequest.rowKey || Date.now()}`
            );
        pendingDetailRequestRef.current = requestId;

        /** @type {any} */
        const resolvedTitle = detailRequest.title
            || (shouldReuseCachedSurface && cachedSurface.title)
            || (rowPath ? rowPath.split('|||').slice(-1)[0] : 'Detail');
        /** @type {any} */
        const resolvedPage = Number.isFinite(Number(detailRequest.page))
            ? Number(detailRequest.page)
            : (shouldReuseCachedSurface && Number.isFinite(Number(cachedSurface.page)) ? Number(cachedSurface.page) : 0);
        /** @type {any} */
        const resolvedPageSize = Number.isFinite(Number(detailRequest.pageSize || detailRequest.page_size))
            ? Number(detailRequest.pageSize || detailRequest.page_size)
            : (shouldReuseCachedSurface && Number.isFinite(Number(cachedSurface.pageSize)) ? Number(cachedSurface.pageSize) : 100);
        /** @type {any} */
        const resolvedSortCol = Object.prototype.hasOwnProperty.call(detailRequest, 'sortCol') || Object.prototype.hasOwnProperty.call(detailRequest, 'sort_col')
            ? (detailRequest.sortCol || detailRequest.sort_col || null)
            : (shouldReuseCachedSurface ? (cachedSurface.sortCol || null) : null);
        /** @type {any} */
        const resolvedSortDir = detailRequest.sortDir || detailRequest.sort_dir
            || (shouldReuseCachedSurface && cachedSurface.sortDir)
            || 'asc';
        /** @type {any} */
        const resolvedFilterText = Object.prototype.hasOwnProperty.call(detailRequest, 'filterText') || Object.prototype.hasOwnProperty.call(detailRequest, 'filter')
            ? (detailRequest.filterText || detailRequest.filter || '')
            : (shouldReuseCachedSurface ? (cachedSurface.filterText || '') : '');
        /** @type {any} */
        const resolvedRowFields = Array.isArray(detailRequest.rowFields || detailRequest.row_fields)
            ? (detailRequest.rowFields || detailRequest.row_fields)
            : (shouldReuseCachedSurface && Array.isArray(cachedSurface.rowFields) ? cachedSurface.rowFields : []);
        /** @type {any} */
        const resolvedAnchorRowId = detailRequest.anchorRowId || detailRequest.anchor_row_id || rowPath;
        /** @type {any} */
        const resolvedAnchorDepth = detailRequest.anchorDepth || detailRequest.anchor_depth || 0;
        /** @type {any} */
        const resolvedDetailKind = detailRequest.detailKind
            || detailRequest.detail_kind
            || (shouldReuseCachedSurface && cachedSurface.detailKind)
            || detailConfig.defaultKind
            || 'records';
        /** @type {any} */
        const resolvedMode = detailRequest.mode || detailMode;
        /** @type {any} */
        const resolvedSource = detailRequest.source
            || (shouldReuseCachedSurface && cachedSurface.source)
            || 'detail';

        if (refreshStrategy === 'everything' && rowPath) {
            detailSurfaceCacheRef.current.delete(rowPath);
        }

        setDetailSurface(/** @param {any} [previousSurface] @returns {any} */ (previousSurface) => ({
            ...(previousSurface || {}),
            ...(shouldReuseCachedSurface ? cachedSurface : {}),
            loading: !shouldSkipRequest,
            rowPath,
            rowKey,
            title: resolvedTitle,
            page: resolvedPage,
            pageSize: resolvedPageSize,
            sortCol: resolvedSortCol,
            sortDir: resolvedSortDir,
            filterText: resolvedFilterText,
            rowFields: resolvedRowFields,
            rows: shouldReuseCachedSurface
                ? (Array.isArray(cachedSurface.rows) ? cachedSurface.rows : [])
                : (previousSurface && previousSurface.rowPath === rowPath ? previousSurface.rows : []),
            columns: shouldReuseCachedSurface
                ? (Array.isArray(cachedSurface.columns) ? cachedSurface.columns : [])
                : (previousSurface && previousSurface.rowPath === rowPath ? previousSurface.columns : []),
            anchorRowId: resolvedAnchorRowId,
            anchorDepth: resolvedAnchorDepth,
            detailKind: resolvedDetailKind,
            mode: resolvedMode,
            source: resolvedSource,
            requestId,
        }));

        if (shouldSkipRequest || !setPropsRef.current) return;
        emitRuntimeRequest('detail', {
            ...detailRequest,
            requestId,
            rowPath,
            rowKey,
            title: resolvedTitle,
            page: resolvedPage,
            pageSize: resolvedPageSize,
            sortCol: resolvedSortCol,
            sortDir: resolvedSortDir,
            filterText: resolvedFilterText,
            rowFields: resolvedRowFields,
            table: tableName || undefined,
            session_id: sessionIdRef.current,
            client_instance: clientInstanceRef.current,
            state_epoch: stateEpoch,
            abort_generation: abortGeneration,
            window_seq: requestVersionRef.current,
            detailKind: resolvedDetailKind,
            mode: resolvedMode,
            source: resolvedSource,
        });
    }, [abortGeneration, detailConfig.defaultKind, detailConfig.keepDetailRows, detailConfig.refreshStrategy, detailMode, emitRuntimeRequest, getCachedDetailSurface, stateEpoch, tableName]);

    /** @type {(...args: any[]) => any} */
    const handleCellDrillThrough = useCallback((row, colId) => {
        if (!row || !row.original) return;
        const rowPath = row.original._path;
        if (!rowPath || rowPath === '__grand_total__') return;
        fetchDetailData({
            rowPath,
            rowFields: Array.isArray(row.original._pathFields) && row.original._pathFields.length > 0
                ? row.original._pathFields
                : rowFields,
            rowKey: row.original._rowKey || rowPath,
            page: 0,
            pageSize: 100,
            sortCol: null,
            sortDir: 'asc',
            filterText: '',
            title: row.original._label || row.original._id || rowPath,
            detailKind: 'records',
            anchorRowId: row.id,
            anchorDepth: typeof row.original.depth === 'number' ? row.original.depth : 0,
            mode: detailMode !== 'none' ? detailMode : 'drawer',
            source: 'drill',
            columnId: colId || null,
        });
    }, [detailMode, fetchDetailData, rowFields]);

    /** @type {(...args: any[]) => any} */
    const toggleDetailForRow = useCallback((row) => {
        if (!row || !row.original || detailMode === 'none') return;
        /** @type {any} */
        const rowPath = row.original._path || row.id;
        if (!rowPath || rowPath === '__grand_total__') return;
        if (detailSurface && detailSurface.rowPath === rowPath) {
            setDetailSurface(null);
            pendingDetailRequestRef.current = null;
            return;
        }
        // Pass only the identity/structural fields — fetchDetailData owns all cache
        // resolution internally via getCachedDetailSurface, so pre-reading here would
        // cause a double LRU touch and duplicate cache logic.
        fetchDetailData({
            rowPath,
            rowFields: Array.isArray(row.original._pathFields) && row.original._pathFields.length > 0
                ? row.original._pathFields
                : rowFields,
            rowKey: row.original._rowKey || rowPath,
            title: row.original._label || row.original._id || rowPath,
            detailKind: row.original._detail_kind || detailConfig.defaultKind || 'records',
            anchorRowId: row.id,
            anchorDepth: typeof row.original.depth === 'number' ? row.original.depth : 0,
        });
    }, [detailConfig.defaultKind, detailMode, detailSurface, fetchDetailData, rowFields]);

    /** @type {(...args: any[]) => any} */
    const handleDetailPageChange = useCallback((nextPage) => {
        if (!detailSurface) return;
        fetchDetailData({
            rowPath: detailSurface.rowPath,
            rowFields: detailSurface.rowFields || rowFields,
            rowKey: detailSurface.rowKey,
            page: nextPage,
            pageSize: detailSurface.pageSize || 100,
            sortCol: detailSurface.sortCol,
            sortDir: detailSurface.sortDir,
            filterText: detailSurface.filterText,
            title: detailSurface.title,
            detailKind: detailSurface.detailKind,
            anchorRowId: detailSurface.anchorRowId,
            anchorDepth: detailSurface.anchorDepth,
            mode: detailSurface.mode,
            source: detailSurface.source,
        });
    }, [detailSurface, fetchDetailData, rowFields]);

    /** @type {(...args: any[]) => any} */
    const handleDetailSort = useCallback((columnId, direction) => {
        if (!detailSurface) return;
        fetchDetailData({
            rowPath: detailSurface.rowPath,
            rowFields: detailSurface.rowFields || rowFields,
            rowKey: detailSurface.rowKey,
            page: 0,
            pageSize: detailSurface.pageSize || 100,
            sortCol: columnId,
            sortDir: direction,
            filterText: detailSurface.filterText,
            title: detailSurface.title,
            detailKind: detailSurface.detailKind,
            anchorRowId: detailSurface.anchorRowId,
            anchorDepth: detailSurface.anchorDepth,
            mode: detailSurface.mode,
            source: detailSurface.source,
        });
    }, [detailSurface, fetchDetailData, rowFields]);

    /** @type {(...args: any[]) => any} */
    const handleDetailFilter = useCallback((text) => {
        if (!detailSurface) return;
        fetchDetailData({
            rowPath: detailSurface.rowPath,
            rowFields: detailSurface.rowFields || rowFields,
            rowKey: detailSurface.rowKey,
            page: 0,
            pageSize: detailSurface.pageSize || 100,
            sortCol: detailSurface.sortCol,
            sortDir: detailSurface.sortDir,
            filterText: text,
            title: detailSurface.title,
            detailKind: detailSurface.detailKind,
            anchorRowId: detailSurface.anchorRowId,
            anchorDepth: detailSurface.anchorDepth,
            mode: detailSurface.mode,
            source: detailSurface.source,
        });
    }, [detailSurface, fetchDetailData, rowFields]);

    /** @type {(...args: any[]) => any} */
    const handleDetailClose = useCallback(() => setDetailSurface(null), []);

    // Keep ref in sync for external callers
    useEffect(() => {
        toggleDetailForRowRef.current = toggleDetailForRow;
    }, [toggleDetailForRow]);

    /** @type {any} */
    const activeDetailDisplayMode = detailSurface && detailSurface.mode
        ? detailSurface.mode
        : detailMode;

    return {
        detailSurface,
        setDetailSurface,
        toggleDetailForRowRef,
        pendingDetailRequestRef,
        activeDetailDisplayMode,
        fetchDetailData,
        handleCellDrillThrough,
        toggleDetailForRow,
        handleDetailPageChange,
        handleDetailSort,
        handleDetailFilter,
        handleDetailClose,
    };
}
