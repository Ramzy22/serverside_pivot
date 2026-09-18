import { useWorkspaceRuntimeStage } from './useWorkspaceRuntimeStage';

import { createWorkspacePresentation } from './createWorkspacePresentation';
import type {
    RuntimeCommand,
    ViewportRuntimeDomain,
    WorkspacePresentation,
    WorkspaceRuntimeDomain,
} from './runtimeDomains';

const command = (value: unknown): RuntimeCommand => typeof value === 'function'
    ? value as RuntimeCommand
    : () => undefined;
const items = (value: unknown): readonly unknown[] => Array.isArray(value) ? value : [];
const freeze = <T extends object>(value: T): Readonly<T> => Object.freeze(value);

/** Chart/detail coordination and the renderer-ready workspace output. */
export function useWorkspaceRuntime(viewport: ViewportRuntimeDomain): WorkspaceRuntimeDomain {
    // The workspace sees the viewport's public table and navigation values,
    // plus command functions already materialized by the preceding domain.
    // Its stage receives a frozen snapshot and cannot extend the viewport
    // domain.
    const stageInput = freeze({
        ...viewport,
        parentRef: viewport.virtualization.parent,
        table: viewport.table.instance,
        renderCell: viewport.table.renderCell,
        renderHeaderCell: viewport.table.renderHeaderCell,
    });
    const composed = useWorkspaceRuntimeStage(stageInput);
    const presentation = createWorkspacePresentation(composed as WorkspacePresentation);

    return freeze<WorkspaceRuntimeDomain>({
        presentation,
        toolbar: presentation,
        sidebar: presentation,
        canvas: presentation,
        charts: freeze({
            panes: items(composed.chartCanvasPanes),
            renderPane: command(composed.renderChartCanvasPane),
            renderWorkspaceModal: command(composed.renderChartWorkspaceModal),
        }),
        detail: freeze({
            surface: composed.detailSurface,
            close: command(composed.handleDetailClose),
        }),
        chartRequest: freeze({
            abortGeneration: Number(composed.abortGeneration) || 0,
            activeRequest: composed.activeChartRequestRef,
            candidates: Array.isArray(composed.chartRequestCandidates)
                ? composed.chartRequestCandidates
                : [],
            requestSequence: composed.chartRequestSeqRef,
            clientInstance: composed.clientInstanceRef,
            completedSignatures: composed.completedChartRequestSignaturesRef,
            needsColumnSchema: composed.needsColSchema === true,
            requestVersion: composed.requestVersionRef,
            serverSide: composed.serverSide === true,
            session: composed.sessionIdRef,
            setProps: composed.setPropsRef,
            stateEpoch: Number(composed.stateEpoch) || 0,
            structuralInFlight: composed.structuralInFlight === true,
            totalCenterColumns: Number.isFinite(composed.totalCenterCols)
                ? Number(composed.totalCenterCols)
                : null,
        }),
    });
}
