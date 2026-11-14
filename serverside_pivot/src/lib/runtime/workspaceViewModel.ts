import type { PivotRuntimeDomains, WorkspaceViewModel } from './runtimeDomains';

/**
 * A renderer receives only the named output of the workspace domain. Earlier
 * versions searched a merged runtime record and inferred ownership from key
 * lists; composition is now fixed by the runtime root's dependency graph.
 */
export function createWorkspaceViewModel(domains: PivotRuntimeDomains): WorkspaceViewModel {
    return Object.freeze({
        presentation: domains.workspace.presentation,
        controls: Object.freeze({
            toolbar: domains.workspace.toolbar,
            sidebar: domains.workspace.sidebar,
        }),
        canvas: domains.workspace.canvas,
    });
}
