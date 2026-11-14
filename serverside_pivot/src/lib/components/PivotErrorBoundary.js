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
          padding: '16px',
          color: '#d32f2f',
          border: '1px solid #ffcdd2',
          borderRadius: '4px',
          background: '#fff8f8',
          fontFamily: 'sans-serif'
        }}>
          <strong>Pivot table error</strong>
          <p style={{ marginTop: '8px', fontSize: '13px' }}>{msg}</p>
          <button
            onClick={() => this.setState({ hasError: false, error: null })}
            style={{ marginTop: '8px', padding: '4px 12px', cursor: 'pointer' }}
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
