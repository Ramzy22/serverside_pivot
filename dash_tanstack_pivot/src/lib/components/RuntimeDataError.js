import React from 'react';

/** Data failures have fixed safe copy; backend messages never render here. */
export function RuntimeDataError({ error, onRetry, theme }) {
  if (!error) return null;
  return <div role="alert" style={{
    display: 'flex', alignItems: 'center', gap: 12, padding: '10px 14px',
    flexShrink: 0, color: theme.text, background: theme.background,
    borderBottom: `1px solid ${theme.border}`,
  }}>
    <span>We could not load the grid data. Try again.</span>
    <button type="button" onClick={onRetry} disabled={error.retrying}
      style={{ color: theme.text, background: theme.background,
        border: `1px solid ${theme.border}`, borderRadius: 4, padding: '5px 12px',
        cursor: error.retrying ? 'wait' : 'pointer', font: 'inherit' }}>
      {error.retrying ? 'Retrying…' : 'Retry'}
    </button>
  </div>;
}
