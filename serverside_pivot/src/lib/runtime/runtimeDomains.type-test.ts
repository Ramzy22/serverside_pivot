import type {
    CoreRuntimeDomain,
    PivotRuntimeProps,
    TransportRuntimeDomain,
    ViewportRuntimeDomain,
    WorkspacePresentation,
} from './runtimeDomains';
import type { RuntimeResponseV1 } from './runtimeContract';

/*
 * These compile-only assertions deliberately live in the TypeScript program.
 * They protect public props, versioned envelopes, and the narrow imperative
 * ports without introducing a runtime test dependency.
 */
const props: PivotRuntimeProps = {
    id: 'orders-pivot',
    rowFields: ['region'],
    viewMode: 'pivot',
    setProps: () => undefined,
};
void props;

// @ts-expect-error Dash view modes are a closed public-prop union.
const invalidProps: PivotRuntimeProps = { viewMode: 'invalid-mode' };
void invalidProps;

const response: RuntimeResponseV1 = {
    schemaVersion: 1,
    kind: 'data',
    status: 'ok',
    payload: {},
};
void response;

declare const core: CoreRuntimeDomain;
declare const transport: TransportRuntimeDomain;
declare const viewport: ViewportRuntimeDomain;
declare const workspace: WorkspacePresentation;

transport.port.emit('data', { offset: 0 });
transport.host.publish({ rowFields: ['country'] });
viewport.virtualization.navigation.scrollToRow(0);
workspace.handleUndo();
workspace.renderCell();
const presentationCount: number = workspace.rowCount;
const presentationFields: readonly string[] = workspace.rowFields;
const presentationServerSide: boolean = workspace.serverSide;
const presentationTitle: string = workspace.pivotTitle;
void presentationCount;
void presentationFields;
void presentationServerSide;
void presentationTitle;
void core.configuration;

// @ts-expect-error Core exposes named groups, never the legacy state bag.
core.runtime;
// @ts-expect-error Imperative ports are navigation-only and carry no row data.
viewport.virtualization.navigation.rows;
// @ts-expect-error Runtime command kinds are closed by the wire contract.
transport.port.emit('not-a-runtime-command', {});
