import type { FrontendRenderer, FrontendState } from '@streamlit/component-v2-lib';
import { createRoot, type Root } from 'react-dom/client';
import { createElement } from 'react';

import {
    HttpPivotTransport,
    LatestViewportScheduler,
    PivotWorkspace,
    type PivotWorkspaceProps,
} from '@pivot/react';

// Keep host-neutral transport traces visible when this V2 component runs in
// Streamlit's iframe. They are intentionally opt-in in the shared workspace.
(window as Window & { __PIVOT_DEBUG__?: boolean }).__PIVOT_DEBUG__ = true;

interface StreamlitPivotState extends FrontendState {
    view_state: Record<string, unknown>;
}

interface StreamlitPivotData {
    runtimeEndpoint: string;
    transportHeaders?: Record<string, string>;
    // State output is opt-in because setStateValue is a Streamlit rerun
    // boundary. Rendering, scrolling, and resizing always stay client-side.
    viewStateEnabled?: boolean;
    configuration: Omit<PivotWorkspaceProps, 'transport' | 'onStateChange'>;
}

interface MountedWorkspace {
    root: Root;
    transport: LatestViewportScheduler;
    transportKey: string;
}

const mounted = new WeakMap<HTMLElement | ShadowRoot, MountedWorkspace>();

const transportFor = (
    parentElement: HTMLElement | ShadowRoot,
    data: StreamlitPivotData,
    existing?: MountedWorkspace,
): MountedWorkspace => {
    const transportKey = JSON.stringify([data.runtimeEndpoint, data.transportHeaders || {}]);
    if (existing && existing.transportKey === transportKey) return existing;
    const root = existing?.root || createRoot(parentElement);
    return {
        root,
        transport: new LatestViewportScheduler(new HttpPivotTransport({
            endpoint: data.runtimeEndpoint,
            headers: data.transportHeaders,
        })),
        transportKey,
    };
};

const render: FrontendRenderer<StreamlitPivotState, StreamlitPivotData> = (args) => {
    const { parentElement, data, setStateValue } = args;
    const entry = transportFor(parentElement, data, mounted.get(parentElement));
    // If endpoint/header configuration changed, retain the React root but
    // replace only the direct transport. No request is routed via Streamlit.
    if (!mounted.has(parentElement)) mounted.set(parentElement, entry);
    else if (mounted.get(parentElement) !== entry) {
        mounted.set(parentElement, entry);
    }
    const configuration = data.configuration as Omit<PivotWorkspaceProps, 'transport' | 'onStateChange'>;
    entry.root.render(createElement(PivotWorkspace, {
        ...configuration,
        transport: entry.transport,
        onStateChange: data.viewStateEnabled
            ? (viewState) => setStateValue('view_state', viewState)
            : undefined,
    }));
    return () => {
        const current = mounted.get(parentElement);
        if (current) {
            current.root.unmount();
            mounted.delete(parentElement);
        }
    };
};

export default render;
