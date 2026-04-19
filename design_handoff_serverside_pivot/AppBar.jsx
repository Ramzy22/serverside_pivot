// AppBar.jsx — serverside-pivot AppBar / Toolbar component

const AppBarStyles = {
  appbar: (theme) => ({
    minHeight: '60px',
    borderBottom: `1px solid ${theme.border}`,
    display: 'flex',
    alignItems: 'center',
    padding: '0 18px',
    justifyContent: 'space-between',
    background: theme.headerBg,
    color: theme.text,
    boxShadow: theme.shadowInset || 'none',
    gap: '12px',
    flexShrink: 0,
  }),
  left: { display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0, flex: 1 },
  right: { display: 'flex', alignItems: 'center', gap: '6px', flexShrink: 0 },
  sep: (theme) => ({ width: '1px', height: '20px', background: theme.border, margin: '0 2px', flexShrink: 0 }),
  title: (theme) => ({ fontSize: '15px', fontWeight: 700, color: theme.text, whiteSpace: 'nowrap' }),
  searchBox: (theme) => ({
    display: 'flex', alignItems: 'center', gap: '6px',
    background: theme.isDark ? 'rgba(255,255,255,0.08)' : (theme.headerSubtleBg || '#F3F4F6'),
    borderRadius: theme.radiusSm || '10px',
    padding: '4px 8px', width: '180px',
  }),
  btn: (theme) => ({
    padding: '4px 6px', borderRadius: theme.radiusSm || '10px',
    border: `1px solid transparent`, background: 'transparent',
    cursor: 'pointer', fontSize: '12px', display: 'inline-flex', alignItems: 'center', gap: '5px',
    fontWeight: 500, color: theme.text, whiteSpace: 'nowrap', fontFamily: 'inherit',
    transition: 'background 120ms ease, border-color 120ms ease',
  }),
  btnBorder: (theme) => ({ border: `1px solid ${theme.border}`, padding: '4px 10px' }),
  themePill: (theme) => ({
    display: 'flex', alignItems: 'center', gap: '6px',
    padding: '5px 10px', borderRadius: theme.radiusSm || '10px',
    border: `1px solid ${theme.border}`, background: theme.headerSubtleBg || theme.hover,
    cursor: 'pointer', fontSize: '12px', fontWeight: 500, color: theme.text, fontFamily: 'inherit',
    position: 'relative',
  }),
};

const ICON = {
  Menu: () => <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z"/></svg>,
  Search: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M15.5 14h-.79l-.28-.27C15.41 12.59 16 11.11 16 9.5 16 5.91 13.09 3 9.5 3S3 5.91 3 9.5 5.91 16 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg>,
  Export: () => <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/></svg>,
  ChevronDown: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M16.59 8.59L12 13.17 7.41 8.59 6 10l6 6 6-6z"/></svg>,
  Fullscreen: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M5 5h5v2H7v3H5V5zm9 0h5v5h-2V7h-3V5zM5 14h2v3h3v2H5v-5zm12 3v-3h2v5h-5v-2h3z"/></svg>,
  Undo: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M12.5 8c-2.65 0-5.05.99-6.9 2.6L2 7v9h9l-3.62-3.62c1.39-1.16 3.16-1.88 5.12-1.88 3.54 0 6.55 2.31 7.6 5.5l2.37-.78C21.08 11.03 17.15 8 12.5 8z"/></svg>,
  Redo: () => <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M18.4 10.6C16.55 8.99 14.15 8 11.5 8c-4.65 0-8.58 3.03-9.96 7.22L3.9 15.7c1.05-3.19 4.05-5.5 7.6-5.5 1.95 0 3.73.72 5.12 1.88L13 15.5h9v-9l-3.6 3.1z"/></svg>,
  Settings: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M19.14 12.94c.04-.31.06-.63.06-.94s-.02-.63-.06-.94l2.03-1.58a.5.5 0 0 0 .12-.64l-1.92-3.32a.5.5 0 0 0-.6-.22l-2.39.96a7.23 7.23 0 0 0-1.63-.94l-.36-2.54a.5.5 0 0 0-.5-.42H10.1a.5.5 0 0 0-.5.42l-.36 2.54c-.58.23-1.13.54-1.63.94l-2.39-.96a.5.5 0 0 0-.6.22L2.7 8.84a.5.5 0 0 0 .12.64l2.03 1.58c-.04.31-.06.63-.06.94s.02.63.06.94L2.82 14.52a.5.5 0 0 0-.12.64l1.92 3.32a.5.5 0 0 0 .6.22l2.39-.96c.5.4 1.05.72 1.63.94l.36 2.54a.5.5 0 0 0 .5.42h3.8a.5.5 0 0 0 .5-.42l.36-2.54c.58-.23 1.13-.54 1.63-.94l2.39.96a.5.5 0 0 0 .6-.22l1.92-3.32a.5.5 0 0 0-.12-.64l-2.03-1.58zM12 15.5A3.5 3.5 0 1 1 12 8.5a3.5 3.5 0 0 1 0 7z"/></svg>,
  Spacing: () => <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 13h2v-2H3v2zm0 4h2v-2H3v2zm0-8h2V7H3v2zm4 4h14v-2H7v2zm0 4h14v-2H7v2zM7 7v2h14V7H7z"/></svg>,
};

const THEME_ORDER = ['flash','dark','bloomblerg_black','blooomberg','strata','crystal','light','material','balham','alabaster','satin'];
const THEME_LABELS = { flash:'Flash', dark:'Dark', bloomblerg_black:'Bloomblerg Black', blooomberg:'Blooomberg', strata:'Strata', crystal:'Crystal', light:'Light', material:'Material', balham:'Balham', alabaster:'Alabaster', satin:'Satin' };
const DENSITY_LABELS = ['Compact','Normal','Loose'];

function ThemeDropdown({ theme, themeName, setThemeName, themes }) {
  const [open, setOpen] = React.useState(false);
  const ref = React.useRef(null);
  React.useEffect(() => {
    const h = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', h);
    return () => document.removeEventListener('mousedown', h);
  }, []);
  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button style={AppBarStyles.themePill(theme)} onClick={() => setOpen(o => !o)}>
        <div style={{ width: 10, height: 10, borderRadius: '50%', background: theme.primary, flexShrink: 0 }} />
        {THEME_LABELS[themeName] || themeName}
        <ICON.ChevronDown />
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: '100%', right: 0, zIndex: 9999, marginTop: '6px',
          background: theme.surfaceBg || theme.background || '#fff',
          border: `1px solid ${theme.border}`, borderRadius: theme.radiusSm || '10px',
          boxShadow: theme.shadowMd || '0 8px 24px rgba(0,0,0,0.12)',
          padding: '6px', minWidth: '180px',
        }}>
          {THEME_ORDER.map(name => (
            <button key={name} onClick={() => { setThemeName(name); setOpen(false); }} style={{
              width: '100%', textAlign: 'left', padding: '8px 10px', borderRadius: '8px',
              border: 'none', background: name === themeName ? (theme.select || 'rgba(79,70,229,0.08)') : 'transparent',
              color: name === themeName ? theme.primary : theme.text,
              fontSize: '12px', fontWeight: name === themeName ? 700 : 500,
              cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px', fontFamily: 'inherit',
            }}>
              <div style={{ width: 8, height: 8, borderRadius: '50%', background: themes[name]?.primary || '#ccc', flexShrink: 0 }} />
              {THEME_LABELS[name] || name}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function PivotAppBar({ theme, themes, themeName, setThemeName, sidebarOpen, setSidebarOpen, title, rowCount, density, setDensity }) {
  const s = AppBarStyles;
  return (
    <div style={s.appbar(theme)}>
      <div style={s.left}>
        <button style={s.btn(theme)} onClick={() => setSidebarOpen(o => !o)} title="Toggle sidebar">
          <ICON.Menu />
        </button>
        <span style={s.title(theme)}>{title}</span>
        <div style={s.sep(theme)} />
        <div style={s.searchBox(theme)}>
          <ICON.Search />
          <input placeholder="Search columns…" style={{ border: 'none', background: 'transparent', outline: 'none', fontSize: '12px', color: theme.text, fontFamily: 'inherit', width: '100%' }} readOnly />
        </div>
      </div>
      <div style={s.right}>
        <button style={s.btn(theme)} title="Undo"><ICON.Undo /></button>
        <button style={s.btn(theme)} title="Redo"><ICON.Redo /></button>
        <div style={s.sep(theme)} />
        {/* Density toggle */}
        <div style={{ display: 'flex', alignItems: 'center', border: `1px solid ${theme.border}`, borderRadius: theme.radiusSm || '10px', overflow: 'hidden' }}>
          {DENSITY_LABELS.map((label, i) => (
            <button key={label} onClick={() => setDensity(i)} style={{
              padding: '4px 8px', border: 'none',
              background: density === i ? theme.primary : 'transparent',
              color: density === i ? '#fff' : theme.textSec,
              fontSize: '11px', fontWeight: 500, cursor: 'pointer', fontFamily: 'inherit',
              borderRight: i < 2 ? `1px solid ${theme.border}` : 'none',
            }}>{label}</button>
          ))}
        </div>
        <div style={s.sep(theme)} />
        <button style={{ ...s.btn(theme), ...s.btnBorder(theme) }}><ICON.Export /> Export</button>
        <button style={{ ...s.btn(theme), padding: '4px 6px' }}><ICON.Settings /></button>
        <button style={{ ...s.btn(theme), padding: '4px 6px' }}><ICON.Fullscreen /></button>
        <div style={s.sep(theme)} />
        <ThemeDropdown theme={theme} themeName={themeName} setThemeName={setThemeName} themes={themes} />
      </div>
    </div>
  );
}

Object.assign(window, { PivotAppBar, ICON, AppBarStyles, THEME_ORDER, THEME_LABELS, DENSITY_LABELS });
