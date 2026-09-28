import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';

import {
    HttpPivotTransport,
    LatestViewportScheduler,
    PivotWorkspace,
} from 'dash_tanstack_pivot/react';

import './styles.css';

type WorkspaceConfig = Omit<React.ComponentProps<typeof PivotWorkspace>, 'transport'>;

// Flagship examples deliberately surface transport diagnostics in DevTools.
// Applications opt in with the same flag when investigating a host integration.
(window as Window & { __PIVOT_DEBUG__?: boolean }).__PIVOT_DEBUG__ = true;

const transport = new LatestViewportScheduler(new HttpPivotTransport({
    endpoint: '/v1/runtime',
}));

const capabilities = [
    {
        number: '01 / LAYOUT',
        title: 'Compose the model',
        copy: 'Drag fields across rows, columns, filters, and measures. Add formulas, custom dimensions, formats, and report definitions in the same workspace.',
    },
    {
        number: '02 / SCALE',
        title: 'Keep the viewport lean',
        copy: 'Virtual rows and columns move through a latest-wins HTTP scheduler. The service can cache, profile, prefetch, and externalize large payloads.',
    },
    {
        number: '03 / ANALYZE',
        title: 'Make the signal visible',
        copy: 'Use hierarchy, totals, filters, selections, number formats, sparklines, and an on-demand chart workspace to interrogate the result.',
    },
    {
        number: '04 / DELIVER',
        title: 'Carry work forward',
        copy: 'Save views and reports, copy rich selections, or export XLSX. Optional chart and spreadsheet code is loaded only when a user needs it.',
    },
];

function CapabilityCard({ number, title, copy }: (typeof capabilities)[number]) {
    return <article className="capability-card">
        <span className="capability-number">{number}</span>
        <h2>{title}</h2>
        <p>{copy}</p>
    </article>;
}

function App() {
    const [workspaceConfig, setWorkspaceConfig] = useState<WorkspaceConfig | null>(null);
    const [workspaceConfigError, setWorkspaceConfigError] = useState<string | null>(null);

    useEffect(() => {
        let active = true;
        fetch('/v1/workspace-config')
            .then(response => {
                if (!response.ok) throw new Error(`workspace configuration request failed (${response.status})`);
                return response.json() as Promise<WorkspaceConfig>;
            })
            .then(config => {
                if (active) setWorkspaceConfig(config);
            })
            .catch(error => {
                if (active) setWorkspaceConfigError(error instanceof Error ? error.message : 'workspace configuration request failed');
            });
        return () => { active = false; };
    }, []);

    return <main className="page-shell">
        <header className="hero">
            <div className="hero-copy">
                <span className="eyebrow"><i /> serverside-pivot / React</span>
                <h1>A host-neutral pivot workspace for the analytical web.</h1>
                <p>
                    This standalone page consumes the published React API directly. It uses a compact,
                    typed V1 request envelope—not a Dash callback bridge—and the grid below is the exact
                    shared workspace used by the Dash and Streamlit showcases.
                </p>
            </div>
            <aside className="hero-stats" aria-label="Demo facts">
                <div><strong>V1</strong><span>runtime protocol</span></div>
                <div><strong>12k</strong><span>source rows</span></div>
                <div><strong>HTTP</strong><span>direct transport</span></div>
                <div><strong>lazy</strong><span>charts + XLSX</span></div>
            </aside>
        </header>

        <section className="capability-grid" aria-label="Core capabilities">
            {capabilities.map((capability) => <CapabilityCard key={capability.number} {...capability} />)}
        </section>

        <section className="workspace-card" aria-label="Interactive pivot workspace">
            <div className="workspace-intro">
                <div>
                    <span className="eyebrow"><i /> live workspace</span>
                    <p>Try the field panel, toolbar, totals, themes, chart launcher, and XLSX export.</p>
                </div>
                <code>POST /v1/runtime</code>
            </div>
            {workspaceConfig ? <PivotWorkspace {...workspaceConfig} transport={transport} /> : (
                <p role="status">{workspaceConfigError || 'Loading workspace configuration…'}</p>
            )}
        </section>

        <section className="integration-grid">
            <article>
                <span className="eyebrow"><i /> public API</span>
                <h2>React owns the shell.</h2>
                <p>Provide <code>PivotWorkspace</code> with <code>HttpPivotTransport</code>; React keeps the presentation and direct browser transport while your application owns authentication, routes, and surrounding UI.</p>
            </article>
            <article>
                <span className="eyebrow"><i /> one protocol</span>
                <h2>One workspace, three hosts.</h2>
                <p>The same runtime domains render in React and Streamlit. Dash retains its Python callback experience while mapping its events to the same server-side semantics.</p>
            </article>
        </section>
    </main>;
}

createRoot(document.getElementById('root')!).render(<React.StrictMode><App /></React.StrictMode>);
