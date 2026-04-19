// SidebarPanel.jsx — Field picker sidebar component

const SIDEBAR_ICONS = {
  Rows: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M4 6h16v2H4zm0 5h16v2H4zm0 5h16v2H4z"/></svg>,
  Cols: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M10 18h5V5h-5v13zm-6 0h5V5H4v13zM16 5v13h4V5h-4z"/></svg>,
  Vals: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M18 6h-8.74l5.26 6-5.26 6H18v2H5v-2l6-6.87L5 4V2h13v4z"/></svg>,
  Field: () => <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M2 10h14v2H2zm0-4h14v2H2zm0 8h14v2H2zm16 2l4-4-4-4v3h-2v2h2z"/></svg>,
  Drag: () => <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M11 18c0 1.1-.9 2-2 2s-2-.9-2-2 .9-2 2-2 2 .9 2 2zm-2-8c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0-6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm6 4c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"/></svg>,
  Close: () => <svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg>,
};

function FieldChip({ theme, label, onRemove, isValue }) {
  const rs = theme.radiusSm || '10px';
  const chipStyle = {
    background: isValue
      ? (theme.isDark ? 'rgba(255,255,255,0.05)' : 'rgba(79,70,229,0.06)')
      : (theme.isDark ? 'rgba(255,255,255,0.07)' : '#ffffff'),
    border: `1px solid ${isValue ? (theme.isDark ? 'rgba(255,255,255,0.14)' : 'rgba(79,70,229,0.18)') : theme.border}`,
    borderRadius: rs,
    padding: '5px 8px',
    marginBottom: '5px',
    display: 'flex', alignItems: 'center', gap: '6px',
    cursor: 'grab', color: isValue ? theme.primary : theme.text,
    fontSize: '12px', fontWeight: isValue ? 500 : 400,
    boxShadow: theme.shadowSm || '0 1px 2px rgba(0,0,0,0.05)',
    transition: 'border-color 120ms ease, background 120ms ease',
  };
  return (
    <div style={chipStyle}>
      <span style={{ color: '#9CA3AF' }}><SIDEBAR_ICONS.Drag /></span>
      {isValue && <span style={{ color: theme.primary }}><SIDEBAR_ICONS.Vals /></span>}
      <span style={{ flex: 1 }}>{label}</span>
      <button onClick={onRemove} style={{ background: 'none', border: 'none', cursor: 'pointer', color: theme.textSec || '#9CA3AF', display: 'flex', padding: 0 }}>
        <SIDEBAR_ICONS.Close />
      </button>
    </div>
  );
}

function DropZone({ theme, children, placeholder }) {
  const rs = theme.radiusSm || '10px';
  return (
    <div style={{
      minHeight: '44px', border: theme.isDark ? `1px solid ${theme.border}` : '1px solid #F3F4F6',
      borderRadius: rs, padding: '6px',
      background: theme.isDark ? 'rgba(255,255,255,0.02)' : (theme.surfaceInset || 'rgba(249,250,251,0.5)'),
      boxShadow: theme.isDark ? 'none' : (theme.shadowInset || 'none'),
    }}>
      {children || (
        <div style={{ height: '32px', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '11px', color: theme.isDark ? 'rgba(255,255,255,0.15)' : '#D1D5DB' }}>
          {placeholder || 'Drop fields here'}
        </div>
      )}
    </div>
  );
}

function SectionLabel({ theme, icon, label }) {
  return (
    <div style={{
      fontSize: '11px', fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase',
      color: theme.textSec || '#94A3B8', marginBottom: '6px',
      display: 'flex', alignItems: 'center', gap: '6px',
    }}>
      {icon}
      {label}
    </div>
  );
}

function FieldList({ theme, fields, rowFields, colFields, valFields, onAdd, onRemoveRow, onRemoveCol, onRemoveVal }) {
  const available = fields.filter(f => !rowFields.includes(f) && !colFields.includes(f) && !valFields.map(v => v.field).includes(f));
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
      {available.map(f => (
        <div key={f} onClick={() => onAdd(f)} style={{
          display: 'flex', alignItems: 'center', gap: '8px', padding: '5px 8px',
          fontSize: '12px', color: theme.textSec || '#475569', borderRadius: '6px',
          cursor: 'pointer', transition: 'background 100ms',
        }}
          onMouseEnter={e => e.currentTarget.style.background = theme.hover || '#F8FAFC'}
          onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
        >
          <span style={{ color: theme.isDark ? 'rgba(255,255,255,0.25)' : '#CBD5E1' }}><SIDEBAR_ICONS.Field /></span>
          {f}
        </div>
      ))}
    </div>
  );
}

function SidebarPanel({ theme, fields, rowFields, setRowFields, colFields, setColFields, valFields, setValFields }) {
  const rs = theme.radiusSm || '10px';
  const removeFrom = (arr, setArr, item) => setArr(arr.filter(x => x !== item));
  const addField = (f) => {
    if (rowFields.length === 0) setRowFields([...rowFields, f]);
    else setValFields([...valFields, { field: f, agg: 'sum', label: f }]);
  };

  return (
    <div style={{
      width: '260px', minWidth: '260px',
      borderRight: `1px solid ${theme.border}`,
      background: theme.sidebarBg, display: 'flex', flexDirection: 'column',
      padding: '16px 12px', gap: '16px', overflowY: 'auto',
    }}>
      {/* Rows */}
      <div>
        <SectionLabel theme={theme} icon={<SIDEBAR_ICONS.Rows />} label="Rows" />
        <DropZone theme={theme}>
          {rowFields.map(f => <FieldChip key={f} theme={theme} label={f} onRemove={() => removeFrom(rowFields, setRowFields, f)} />)}
          {rowFields.length === 0 && null}
        </DropZone>
      </div>
      {/* Columns */}
      <div>
        <SectionLabel theme={theme} icon={<SIDEBAR_ICONS.Cols />} label="Columns" />
        <DropZone theme={theme} placeholder="Drop column fields">
          {colFields.map(f => <FieldChip key={f} theme={theme} label={f} onRemove={() => removeFrom(colFields, setColFields, f)} />)}
        </DropZone>
      </div>
      {/* Values */}
      <div>
        <SectionLabel theme={theme} icon={<SIDEBAR_ICONS.Vals />} label="Values" />
        <DropZone theme={theme}>
          {valFields.map(v => <FieldChip key={v.field} theme={theme} label={`${v.label || v.field} · ${v.agg}`} isValue onRemove={() => setValFields(valFields.filter(x => x.field !== v.field))} />)}
          {valFields.length === 0 && null}
        </DropZone>
      </div>
      {/* Available fields */}
      <div style={{ flex: 1 }}>
        <SectionLabel theme={theme} icon={null} label="Available Fields" />
        <FieldList theme={theme} fields={fields} rowFields={rowFields} colFields={colFields} valFields={valFields} onAdd={addField} onRemoveRow={f => removeFrom(rowFields, setRowFields, f)} onRemoveCol={f => removeFrom(colFields, setColFields, f)} onRemoveVal={f => setValFields(valFields.filter(x => x.field !== f))} />
      </div>
    </div>
  );
}

Object.assign(window, { SidebarPanel, FieldChip, DropZone });
