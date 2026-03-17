import React from 'react';

class PivotErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    if (process.env.NODE_ENV !== 'production') {
      console.error('[PivotErrorBoundary]', error, info.componentStack);
    }
  }

  render() {
    if (this.state.hasError) {
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
