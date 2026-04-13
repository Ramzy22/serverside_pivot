/**
 * usePivotData Hook
 * 
 * Manages server-side data transport state including:
 * - Transport data envelope (data, rowCount, columns, schema)
 * - Chart data state
 * - Filter options state
 * - Editor options loading state
 * - Runtime response handling
 * - Patch envelope application
 * 
 * Extracted from DashTanstackPivot.react.js (Phase 2 of refactoring)
 */

import { useState, useEffect, useCallback } from 'react';
import {
    normalizeRuntimeDataEnvelope,
    applyRuntimePatchEnvelope,
} from './usePivotNormalization';

export function usePivotData({
    serverSide,
    inputData = [],
    runtimeResponse = null,
}) {
    // Transport data state
    const [transportDataState, setTransportDataState] = useState(() => normalizeRuntimeDataEnvelope({
        data: Array.isArray(inputData) ? inputData : [],
        rowCount: serverSide ? null : (Array.isArray(inputData) ? inputData.length : 0),
        columns: [],
        dataOffset: 0,
        dataVersion: 0,
    }));

    // Transport filter options state
    const [transportFilterOptionsState, setTransportFilterOptionsState] = useState(() => ({}));

    // Editor options loading state
    const [editorOptionsLoadingState, setEditorOptionsLoadingState] = useState(() => ({}));

    // Transport chart data state
    const [transportChartDataState, setTransportChartDataState] = useState(() => (null));

    // Extract frequently used values
    const data = transportDataState.data;
    const rowCount = transportDataState.rowCount;
    const responseColumns = transportDataState.columns;
    const responseColSchema = transportDataState.colSchema;
    const dataOffset = transportDataState.dataOffset;
    const dataVersion = transportDataState.dataVersion;
    const filterOptions = transportFilterOptionsState;
    const chartData = transportChartDataState;

    // Update transport data when input data changes (client-side mode)
    useEffect(() => {
        if (serverSide) return;
        setTransportDataState((previousState) => normalizeRuntimeDataEnvelope({
            data: Array.isArray(inputData) ? inputData : [],
            rowCount: Array.isArray(inputData) ? inputData.length : 0,
            columns: previousState.columns,
            dataOffset: 0,
            dataVersion: previousState.dataVersion,
        }, previousState));
    }, [inputData, serverSide]);

    // Apply runtime response (server-side mode)
    const applyRuntimeResponse = useCallback((payload) => {
        if (!payload) return;
        setTransportDataState((previousState) => normalizeRuntimeDataEnvelope(payload, previousState));
    }, []);

    // Apply runtime patch envelope (server-side partial updates)
    const applyRuntimePatch = useCallback((patch) => {
        if (!patch) return;
        setTransportDataState((previousState) => applyRuntimePatchEnvelope({
            rows: patch.rows,
            rowCount: patch.rowCount,
            columns: patch.columns,
            colSchema: patch.colSchema || patch.col_schema,
            dataOffset: patch.dataOffset,
            dataVersion: patch.dataVersion,
        }, previousState));
    }, []);

    // Set filter options from transport
    const setFilterOptions = useCallback((options) => {
        setTransportFilterOptionsState(options || {});
    }, []);

    // Set editor options loading state
    const setEditorOptionsLoading = useCallback((state) => {
        setEditorOptionsLoadingState(state || {});
    }, []);

    // Set chart data from transport
    const setChartData = useCallback((data) => {
        setTransportChartDataState(data);
    }, []);

    return {
        // State
        transportDataState,
        data,
        rowCount,
        responseColumns,
        responseColSchema,
        dataOffset,
        dataVersion,
        filterOptions,
        chartData,
        editorOptionsLoadingState,

        // Actions
        setTransportDataState,
        applyRuntimeResponse,
        applyRuntimePatch,
        setFilterOptions,
        setEditorOptionsLoading,
        setChartData,
    };
}
