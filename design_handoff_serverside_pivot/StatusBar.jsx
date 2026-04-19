// StatusBar.jsx — Bottom status bar

function StatusBar({ theme, rowCount, selectedCount, density }) {
  const densityLabel = ['Compact', 'Normal', 'Loose'][density] || 'Normal';
  return (
    <div style={{
      height: '32px', borderTop: `1px solid ${theme.border}`,
      display: 'flex', alignItems: 'center', padding: '0 14px',
      background: theme.headerSubtleBg || theme.headerBg || '#F9FAFB',
      gap: '16px', fontSize: '11px', color: theme.textSec || '#64748B',
      flexShrink: 0, justifyContent: 'space-between',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
        <span><strong style={{ color: theme.text }}>{rowCount.toLocaleString()}</strong> rows</span>
        {selectedCount > 0 && (
          <span style={{ color: theme.primary }}><strong>{selectedCount}</strong> selected</span>
        )}
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
        <span>serverside-pivot</span>
        <span style={{
          background: theme.isDark ? 'rgba(255,255,255,0.08)' : 'rgba(79,70,229,0.08)',
          color: theme.primary, borderRadius: '999px', padding: '2px 8px', fontWeight: 600, fontSize: '10px',
        }}>flash</span>
        <span>{densityLabel}</span>
      </div>
    </div>
  );
}

Object.assign(window, { StatusBar });
