import React from 'react';

/** @extends {React.Component<any, { hasError: boolean, error: any }>} */
class PivotErrorBoundary extends React.Component {
  /** @param {any} props */
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: /** @type {any} */ (null) };
  }

  /** @param {any} [error] @returns {any} */
  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  /** @param {any} [error] @param {any} [info] @returns {any} */
  componentDidCatch(error, info) {
    if (process.env.NODE_ENV !== 'production') {
      console.error('[PivotErrorBoundary]', error, info.componentStack);
    }
  }

  render() {
    if (this.state.hasError) {
      /** @type {any} */
      const msg = (this.state.error ? this.state.error.message : null) || 'An unexpected error occurred.';
      return (
        <div style={{
          padding: '20px',
          color: 'var(--sp-text, #111827)',
          border: '1px solid rgba(239, 68, 68, 0.35)',
          borderRadius: '12px',
          background: 'var(--sp-surface-bg, #ffffff)',
          boxShadow: 'var(--sp-shadow-sm, 0 4px 16px rgba(0,0,0,0.08))',
          fontFamily: 'var(--sp-font-sans, system-ui, -apple-system, sans-serif)',
          margin: '12px',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#ef4444', fontWeight: 700, fontSize: '14px' }}>
            <span aria-hidden="true" style={{ width: '8px', height: '8px', borderRadius: '999px', background: '#ef4444' }} />
            <span>Pivot table error</span>
          </div>
          <p style={{ marginTop: '8px', marginBottom: 0, fontSize: '13px', color: 'var(--sp-text-sec, #4b5563)', lineHeight: 1.5 }}>{msg}</p>
          <button
            type="button"
            className="sp-btn sp-btn-primary"
            onClick={() => this.setState({ hasError: false, error: null })}
            style={{
              marginTop: '14px',
              padding: '7px 16px',
              borderRadius: '999px',
              border: 'none',
              background: 'var(--sp-primary, #2563eb)',
              color: '#ffffff',
              fontSize: '12px',
              fontWeight: 600,
              cursor: 'pointer',
            }}
          >
            Retry
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

export default PivotErrorBoundary;
