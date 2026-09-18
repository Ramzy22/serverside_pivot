import { useCommandRuntimeStage } from './useCommandRuntimeStage';

import type { CommandRuntimeDomain, CoreRuntimeDomain, RuntimeCommand, TransportRuntimeDomain } from './runtimeDomains';

const command = (value: unknown): RuntimeCommand => typeof value === 'function'
    ? value as RuntimeCommand
    : () => undefined;
const freeze = <T extends object>(value: T): Readonly<T> => Object.freeze(value);

/** User actions: fields, layout, editing, selection, export, and charts. */
export function useCommandRuntime(
    core: CoreRuntimeDomain,
    transport: TransportRuntimeDomain,
): CommandRuntimeDomain {
    // The command stage receives an immutable snapshot. It cannot extend or
    // mutate the core domain that supplied these values.
    const stageInput = freeze({
        ...core,
        tableRef: core.refs.table,
        setPropsRef: core.refs.setProps,
        clientInstanceRef: core.refs.clientInstance,
        sessionIdRef: core.refs.session,
        savePersistedState: core.persistence.save,
        loadPersistedState: core.persistence.load,
    });
    const composed = useCommandRuntimeStage(stageInput, transport);

    return freeze<CommandRuntimeDomain>({
        ...composed,
        fields: freeze({
            move: command(composed.applyFieldZoneMove),
            expandAll: command(composed.handleExpandAllRows),
            pinColumn: command(composed.handlePinColumn),
            pinRow: command(composed.handlePinRow),
        }),
        layout: freeze({
            undo: command(composed.handleUndo),
            redo: command(composed.handleRedo),
            setRowFields: command(composed.setRowFieldsWithHistory),
            setColFields: command(composed.setColFieldsWithHistory),
            setValues: command(composed.setValConfigsWithHistory),
        }),
        editing: freeze({
            dispatchCellEdit: command(composed.dispatchInlineCellEdit),
            revertCell: command(composed.handleRevertCell),
            revertAll: command(composed.handleRevertAll),
        }),
        selection: freeze({
            applyValueFormat: command(composed.applySelectionValueFormat),
            copy: command(composed.copyRichToClipboard),
        }),
        export: freeze({
            exportPivot: command(composed.exportPivot),
            cancel: command(composed.cancelActiveExportRequest),
        }),
        charts: freeze({
            addPane: command(composed.handleAddChartCanvasPane),
            updatePane: command(composed.updateChartCanvasPane),
            removePane: command(composed.handleRemoveChartCanvasPane),
            openSelectionChart: command(composed.openSelectionChart),
        }),
    });
}
