import { useViewportRuntimeStage } from './useViewportRuntimeStage';

import { useRuntimeStructuralSync } from './useRuntimeStructuralSync';
import type {
    CommandRuntimeDomain,
    RuntimeCommand,
    TransportRuntimeDomain,
    ViewportRuntimeDomain,
} from './runtimeDomains';

const command = (value: unknown): RuntimeCommand => typeof value === 'function'
    ? value as RuntimeCommand
    : () => undefined;
const items = (value: unknown): readonly unknown[] => Array.isArray(value) ? value : [];
const count = (value: unknown): number | null => Number.isFinite(value) ? Number(value) : null;
const freeze = <T extends object>(value: T): Readonly<T> => Object.freeze(value);

/** Table construction, virtualization, structural sync, and scroll restoration. */
export function useViewportRuntime(
    commands: CommandRuntimeDomain,
    transport: TransportRuntimeDomain,
): ViewportRuntimeDomain {
    // Commands and transport cross this boundary only as named event-time
    // ports. The viewport stage receives a frozen input snapshot and cannot
    // write back into the command domain.
    const stageInput = freeze({
        ...commands,
        applySelectionValueFormat: commands.selection.applyValueFormat,
        exportPivot: commands.export.exportPivot,
        cancelActiveExportRequest: commands.export.cancel,
        emitRuntimeRequest: transport.port.emit,
        recordProfilerQueuedRequest: transport.profiling.recordQueued,
    });
    const composed = useViewportRuntimeStage(stageInput, transport);
    useRuntimeStructuralSync({
        actions: {
            beginExpansionRequest: composed.beginExpansionRequest,
            beginStructuralTransaction: composed.beginStructuralTransaction,
            buildRuntimeRequestStateOverride: composed.buildRuntimeRequestStateOverride,
            markRequestPending: composed.markRequestPending,
            resolveStableRequestedColumnWindow: composed.resolveStableRequestedColumnWindow,
            setPendingColumnSkeletonCount: composed.setPendingColumnSkeletonCount,
        },
        refs: {
            abortGenerationRef: composed.abortGenerationRef,
            clientInstanceRef: composed.clientInstanceRef,
            expansionScrollRestoreRafRef: composed.expansionScrollRestoreRafRef,
            expansionScrollRestoreRef: composed.expansionScrollRestoreRef,
            latestDataVersionRef: composed.latestDataVersionRef,
            latestRequestedViewportRef: composed.latestRequestedViewportRef,
            latestViewportRef: composed.latestViewportRef,
            needsColSchemaRef: composed.needsColSchemaRef,
            parentRef: composed.parentRef,
            pendingExpansionRef: composed.pendingExpansionRef,
            sessionIdRef: composed.sessionIdRef,
            setPropsRef: composed.setPropsRef,
        },
        state: {
            colFields: composed.colFields,
            columnOrder: composed.columnOrder,
            columnPinning: composed.columnPinning,
            columnSizing: composed.columnSizing,
            columnVisibility: composed.columnVisibility,
            customDimensions: composed.customDimensions,
            detailConfig: composed.detailConfig,
            detailMode: composed.detailMode,
            effectiveSortOptions: composed.effectiveSortOptions,
            expanded: composed.expanded,
            filters: composed.filters,
            grandTotalPinOverride: composed.grandTotalPinOverride,
            immersiveMode: composed.immersiveMode,
            layoutMode: composed.layoutMode,
            reportDef: composed.reportDef,
            rowFields: composed.rowFields,
            rowPinning: composed.rowPinning,
            runtimeMeasureAxis: composed.runtimeMeasureAxis,
            serverSide: composed.serverSide,
            serverSideBlockSize: composed.serverSideBlockSize,
            serverSidePinsGrandTotal: composed.serverSidePinsGrandTotal,
            showColTotals: composed.showColTotals,
            showRowTotals: composed.showRowTotals,
            showSubtotals: composed.showSubtotals,
            sorting: composed.sorting,
            tableName: composed.tableName,
            treeConfig: composed.treeConfig,
            valConfigs: composed.valConfigs,
            viewMode: composed.viewMode,
        },
        initialState: {
            viewMode: composed.normalizedInitialViewMode,
            detailMode: composed.normalizedInitialDetailMode,
            treeConfig: composed.normalizedInitialTreeConfig,
            detailConfig: composed.normalizedInitialDetailConfig,
            reportDef: composed.reportDef,
            customDimensions: composed.normalizedInitialCustomDimensions,
            measureAxis: composed.normalizedInitialMeasureAxis,
            rowFields: composed.initialRowFields,
            colFields: composed.initialColFields,
            valConfigs: composed.normalizedInitialValConfigs,
            filters: {},
            sorting: [],
            sortOptions: composed.effectiveSortOptions,
            expanded: {},
            showRowTotals: composed.initialShowRowTotals,
            showColTotals: composed.initialShowColTotals,
            showSubtotals: composed.initialShowSubtotals,
            columnPinning: composed.initialColumnPinning,
            rowPinning: composed.initialRowPinning,
            grandTotalPinOverride: composed.grandTotalPinOverride,
            serverSidePinsGrandTotal: composed.serverSidePinsGrandTotal,
            columnVisibility: {},
            columnOrder: [],
            columnSizing: {},
        },
    });

    return freeze<ViewportRuntimeDomain>({
        ...composed,
        table: freeze({
            instance: composed.table,
            renderCell: command(composed.renderCell),
            renderHeaderCell: command(composed.renderHeaderCell),
        }),
        rows: freeze({
            count: Number.isFinite(composed.rowCount) ? Number(composed.rowCount) : 0,
            height: Number.isFinite(composed.rowHeight) ? Number(composed.rowHeight) : 32,
            visible: items(composed.rows),
            virtual: items(composed.virtualRows),
        }),
        columns: freeze({
            visible: items(composed.visibleLeafColumns),
            left: items(composed.leftCols),
            center: items(composed.centerCols),
            right: items(composed.rightCols),
            totalCenter: count(composed.totalCenterCols),
        }),
        virtualization: freeze({
            parent: composed.parentRef,
            navigation: freeze({
                scrollToRow: typeof composed.scrollToDisplayRow === 'function'
                    ? composed.scrollToDisplayRow
                    : () => undefined,
                scrollToColumn: typeof composed.scrollToDisplayColumn === 'function'
                    ? composed.scrollToDisplayColumn
                    : () => undefined,
                refresh: typeof composed.requestVisibleViewportRefresh === 'function'
                    ? composed.requestVisibleViewportRefresh
                    : () => undefined,
            }),
        }),
        // The response lifecycle cannot receive the viewport's mutable stage
        // object. Export/copy are the only two event-time browser effects it
        // needs, so expose them as a narrow typed port instead.
        delivery: freeze({
            copy: command(composed.copyRichToClipboard),
            download: command(composed.triggerExportDownload),
        }),
    });
}
