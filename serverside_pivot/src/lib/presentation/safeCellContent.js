import React from 'react';

/** @type {React.CSSProperties} */
const CELL_ERROR_FALLBACK_STYLE = {
    color: 'var(--pivot-danger-color, #b42318)',
    fontSize: '11px',
    fontWeight: 600,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
};

/** @param {any} value */
const isPlainRenderableObject = (value) => value
    && typeof value === 'object'
    && !Array.isArray(value)
    && !React.isValidElement(value);

/**
 * Renderer safety boundary, independent from cell selection/runtime state.
 * @extends {React.Component<{ resetKey?: any, children?: any }, { hasError: boolean }>}
 */
export class CellErrorBoundary extends React.Component {
    /** @param {{ resetKey?: any, children?: any }} props */
    constructor(props) {
        super(props);
        this.state = { hasError: false };
    }

    static getDerivedStateFromError() {
        return { hasError: true };
    }

    /** @param {{ resetKey?: any, children?: any }} prevProps */
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

/** @param {{ render: () => any }} props */
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
