import { useCoreRuntimeStage } from './useCoreRuntimeStage';

import type React from 'react';

import type { RuntimePayloadTransport } from './httpTransport';
import type {
    CoreRuntimeDomain,
    CoreRuntimeRefs,
    CoreRuntimeState,
    PivotObject,
    PivotRuntimeConfiguration,
    PivotRuntimeProps,
    PivotValueConfig,
} from './runtimeDomains';

const asArray = <T>(value: unknown): readonly T[] => Array.isArray(value) ? value as readonly T[] : [];
const asObject = (value: unknown): PivotObject => value && typeof value === 'object' && !Array.isArray(value)
    ? value as PivotObject
    : {};
const freeze = <T extends object>(value: T): Readonly<T> => Object.freeze(value);

/**
 * The core stage has a deliberately narrow output boundary. This is not a
 * shared cross-domain binding: subsequent domains receive an immutable core
 * domain and create their own local implementation state.
 */
interface SetupRuntimeValues {
    readonly id?: string;
    readonly tableName?: string;
    readonly serverSide?: boolean;
    readonly viewMode?: PivotRuntimeProps['viewMode'];
    readonly detailMode?: PivotRuntimeProps['detailMode'];
    readonly persistence?: boolean | string | number;
    readonly rowFields?: readonly string[];
    readonly colFields?: readonly string[];
    readonly valConfigs?: readonly PivotValueConfig[];
    readonly filters?: PivotObject;
    readonly sorting?: readonly PivotObject[];
    readonly expanded?: boolean | PivotObject;
    readonly columnOrder?: readonly string[];
    readonly columnVisibility?: PivotObject;
    readonly columnSizing?: PivotObject;
    readonly columnPinning?: PivotObject;
    readonly rowPinning?: PivotObject;
    readonly tableRef: React.MutableRefObject<unknown>;
    readonly setPropsRef: React.MutableRefObject<PivotRuntimeProps['setProps']>;
    readonly clientInstanceRef: React.MutableRefObject<string | null>;
    readonly sessionIdRef: React.MutableRefObject<string | null>;
    readonly savePersistedState?: (state: PivotObject) => void;
    readonly loadPersistedState?: () => PivotObject | null;
}

/** Owns normalized configuration, React state, persistence, and stable refs. */
export function useCoreRuntime(
    props: PivotRuntimeProps & { readonly payloadTransport?: RuntimePayloadTransport },
    payloadTransport?: RuntimePayloadTransport,
): CoreRuntimeDomain {
    // The composed stage is intentionally wider internally; the core domain
    // consumes and exposes only this declared setup-owned boundary.
    const resolvedTransport = payloadTransport ?? props.payloadTransport;
    const setup = useCoreRuntimeStage(props, resolvedTransport) as unknown as SetupRuntimeValues;
    const configuration = freeze<PivotRuntimeConfiguration>({
        componentId: typeof setup.id === 'string' ? setup.id : null,
        tableName: typeof setup.tableName === 'string' ? setup.tableName : null,
        serverSide: setup.serverSide === true,
        viewMode: typeof setup.viewMode === 'string' ? setup.viewMode : 'pivot',
        detailMode: typeof setup.detailMode === 'string' ? setup.detailMode : 'none',
        persistence: setup.persistence,
        props,
    });
    const state = freeze<CoreRuntimeState>({
        rowFields: asArray<string>(setup.rowFields),
        colFields: asArray<string>(setup.colFields),
        valConfigs: asArray(setup.valConfigs),
        filters: asObject(setup.filters),
        sorting: asArray(setup.sorting),
        expanded: setup.expanded === true || setup.expanded === false ? setup.expanded : asObject(setup.expanded),
        columnOrder: asArray<string>(setup.columnOrder),
        columnVisibility: asObject(setup.columnVisibility),
        columnSizing: asObject(setup.columnSizing),
        columnPinning: asObject(setup.columnPinning),
        rowPinning: asObject(setup.rowPinning),
    });
    const refs = freeze<CoreRuntimeRefs>({
        table: setup.tableRef,
        setProps: setup.setPropsRef,
        clientInstance: setup.clientInstanceRef,
        session: setup.sessionIdRef,
    });

    // Setup values are copied once. No later domain can mutate setup state or
    // publish a shared mutable record back into it.
    return freeze<CoreRuntimeDomain>({
        ...setup,
        configuration,
        state,
        refs,
        persistence: freeze({
            save: typeof setup.savePersistedState === 'function' ? setup.savePersistedState : () => undefined,
            load: typeof setup.loadPersistedState === 'function' ? setup.loadPersistedState : () => null,
        }),
    });
}
