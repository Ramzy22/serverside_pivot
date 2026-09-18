import React from 'react';

const CELL_ERROR_FALLBACK_STYLE = {
    color: 'var(--pivot-danger-color, #b42318)',
    fontSize: '11px',
    fontWeight: 600,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
};

const isPlainRenderableObject = (value) => value
    && typeof value === 'object'
    && !Array.isArray(value)
    && !React.isValidElement(value);

/** Renderer safety boundary, independent from cell selection/runtime state. */
export class CellErrorBoundary extends React.Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false };
    }

    static getDerivedStateFromError() {
        return { hasError: true };
    }

    componentDidUpdate(prevProps) {
        if (prevProps.resetKey !== this.props.resetKey && this.state.hasError) {
            this.setState({ hasError: false });
        }
    }

    render() {
        if (this.state.hasError) {
            return <span data-pivot-cell-error="true" role="status" aria-label="Cell render failed" style={CELL_ERROR_FALLBACK_STYLE}>Cell error</span>;
        }
        return this.props.children;
    }
}

export function SafeCellContent({ render }) {
    const rendered = render();
    if (isPlainRenderableObject(rendered) || (Array.isArray(rendered) && rendered.some(isPlainRenderableObject))) {
        try {
            return JSON.stringify(rendered);
        } catch {
            return String(rendered);
        }
    }
    return rendered;
}
