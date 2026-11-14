import React from 'react';

import { PivotWorkspaceShell } from '../components/PivotWorkspaceShell';
import { useCommandRuntime } from './useCommandRuntime';
import { useCoreRuntime } from './useCoreRuntime';
import { useTransportChartRequestDispatch, useTransportResponseRuntime, useTransportRuntime } from './useTransportRuntime';
import { useViewportRuntime } from './useViewportRuntime';
import { useWorkspaceRuntime } from './useWorkspaceRuntime';
import { createWorkspaceViewModel } from './workspaceViewModel';
import type { PivotRuntimeProps } from './runtimeDomains';
import type { RuntimePayloadTransport } from './httpTransport';

/** Typed runtime composition boundary for the public Dash component. */
export function PivotRuntimeRoot(props: PivotRuntimeProps & {
    readonly payloadTransport?: RuntimePayloadTransport;
}) {
    const { payloadTransport } = props;
    const core = useCoreRuntime(props);
    const transport = useTransportRuntime(core);
    const commands = useCommandRuntime(core, transport);
    const viewport = useViewportRuntime(commands, transport);
    const workspaceDomain = useWorkspaceRuntime(viewport);
    // Response processing is transport/core work. Chart dispatch runs after
    // workspace composition because chart candidates belong to presentation.
    useTransportResponseRuntime(core, transport, viewport, payloadTransport);
    useTransportChartRequestDispatch(workspaceDomain, transport);
    const domains = Object.freeze({ core, transport, commands, viewport, workspace: workspaceDomain });
    const workspace = createWorkspaceViewModel(domains);
    return <PivotWorkspaceShell workspace={workspace} />;
}
