// PivotTable.jsx — Table body with header, rows, totals

const ROW_HEIGHTS = [36, 44, 56];

function Sparkline({ color, negative }) {
  const pts = negative
    ? [8,22, 18,18, 28,20, 38,14, 48,16, 58,10, 68,12, 78,6]
    : [8,20, 18,16, 28,18, 38,10, 48,14, 58,8, 68,12, 78,4];
  const poly = pts.reduce((s,v,i) => s + (i%2===0?`${v},`:`${v} `),'');
  return (
    <svg width="80" height="28" viewBox="0 0 86 28" style={{ flexShrink: 0 }}>
      <polyline points={poly} fill="none" stroke={negative ? '#EF4444' : '#22C55E'} strokeWidth="1.5" strokeLinejoin="round" />
      <circle cx={pts[pts.length-2]} cy={pts[pts.length-1]} r="2.5" fill={negative ? '#EF4444' : '#22C55E'} />
    </svg>
  );
}

function SortIcon({ dir, color }) {
  if (!dir) return null;
  return dir === 'asc'
    ? <svg width="12" height="12" viewBox="0 0 24 24" fill={color}><path d="M4 12l1.41 1.41L11 7.83V20h2V7.83l5.58 5.59L20 12l-8-8-8 8z"/></svg>
    : <svg width="12" height="12" viewBox="0 0 24 24" fill={color}><path d="M20 12l-1.41-1.41L13 16.17V4h-2v12.17l-5.58-5.59L4 12l8 8 8-8z"/></svg>;
}

function FilterIcon({ active, color }) {
  return (
    <svg width="11" height="11" viewBox="0 0 24 24" fill={active ? color : 'rgba(150,150,150,0.4)'}>
      <path d="M10 18h4v-2h-4v2zM3 6v2h18V6H3zm3 7h12v-2H6v2z"/>
    </svg>
  );
}

function HeaderCell({ theme, label, width, sorted, onSort, pinned, isHierarchy }) {
  const r = theme.radius || '4px';
  const sortColor = theme.sortedHeaderBorder || theme.primary || '#4F46E5';
  const bg = sorted ? (theme.sortedHeaderBg || 'rgba(79,70,229,0.06)') : (theme.headerBg || '#fff');
  const color = sorted ? (theme.sortedHeaderText || theme.primary) : theme.text;
  const [hover, setHover] = React.useState(false);

  return (
    <div
      onClick={onSort}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        width, minWidth: width, maxWidth: width,
        height: '38px', display: 'flex', alignItems: 'center',
        padding: '0 12px', gap: '5px',
        borderRight: `1px solid ${theme.border}`,
        borderBottom: sorted ? `2px solid ${sortColor}` : `1px solid ${theme.border}`,
        fontWeight: 600, color,
        background: hover ? (theme.hoverStrong || theme.hover || '#F3F4F6') : bg,
        cursor: 'pointer', userSelect: 'none', flexShrink: 0, whiteSpace: 'nowrap',
        overflow: 'hidden', textOverflow: 'ellipsis', fontSize: '13px',
        boxShadow: theme.shadowInset || 'none',
        boxSizing: 'border-box',
        ...(pinned ? { position: 'sticky', left: 0, zIndex: 3 } : {}),
      }}
    >
      <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' }}>{label}</span>
      <SortIcon dir={sorted} color={sortColor} />
      <FilterIcon active={false} color={sortColor} />
    </div>
  );
}

function DataCell({ theme, value, width, isNeg, isPos, isMono, isHierarchy, isSelected, depth, expanded, onExpand, rowHeight }) {
  const bg = isHierarchy ? (theme.hierarchyBg || '#EDF4FF') : (theme.surfaceBg || theme.background || '#fff');
  const color = isNeg ? '#EF4444' : isPos ? '#22C55E' : theme.text;
  return (
    <div style={{
      width, minWidth: width, maxWidth: width, height: rowHeight,
      display: 'flex', alignItems: 'center', padding: '0 12px', gap: '6px',
      borderRight: `1px solid ${theme.border}`, borderBottom: `1px solid ${theme.border}`,
      background: isSelected ? (theme.select || 'rgba(79,70,229,0.08)') : bg,
      boxShadow: isSelected ? `inset 0 0 0 2px ${theme.primary || '#4F46E5'}` : 'none',
      color, fontFamily: isMono ? "'JetBrains Mono', monospace" : 'inherit',
      fontSize: '13px', flexShrink: 0, whiteSpace: 'nowrap',
      overflow: 'hidden', textOverflow: 'ellipsis', boxSizing: 'border-box',
    }}>
      {isHierarchy && depth > 0 && <div style={{ width: depth * 16, flexShrink: 0 }} />}
      {isHierarchy && onExpand && (
        <button onClick={onExpand} style={{
          background: 'none', border: 'none', cursor: 'pointer', padding: '2px',
          display: 'flex', color: theme.textSec || '#64748B', flexShrink: 0,
        }}>
          {expanded
            ? <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M16.59 8.59L12 13.17 7.41 8.59 6 10l6 6 6-6z"/></svg>
            : <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"/></svg>
          }
        </button>
      )}
      <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' }}>{value}</span>
    </div>
  );
}

function SparklineCell({ theme, width, negative, rowHeight }) {
  return (
    <div style={{
      width, minWidth: width, height: rowHeight, display: 'flex', alignItems: 'center',
      padding: '0 8px', borderRight: `1px solid ${theme.border}`,
      borderBottom: `1px solid ${theme.border}`, flexShrink: 0, boxSizing: 'border-box',
      background: theme.surfaceBg || theme.background || '#fff',
    }}>
      <Sparkline color={theme.primary} negative={negative} />
      <span style={{ fontSize: '11px', color: negative ? '#EF4444' : '#22C55E', marginLeft: '6px', fontFamily: "'JetBrains Mono', monospace", fontWeight: 600 }}>
        {negative ? '−2.4%' : '+3.8%'}
      </span>
    </div>
  );
}

function TotalRow({ theme, cols, height, isGrand }) {
  const bg = isGrand ? (theme.totalBgStrong || 'rgba(224,231,255,0.95)') : (theme.totalBg || 'rgba(238,242,255,0.82)');
  const color = isGrand ? (theme.totalTextStrong || '#312E81') : (theme.totalText || '#3730A3');
  return (
    <div style={{ display: 'flex', borderBottom: `1px solid ${theme.border}` }}>
      {cols.map((col, i) => (
        <div key={i} style={{
          width: col.width, minWidth: col.width, height,
          display: 'flex', alignItems: 'center', padding: '0 12px',
          borderRight: `1px solid ${theme.border}`,
          background: bg, color, fontWeight: isGrand ? 700 : 600,
          fontFamily: i > 0 ? "'JetBrains Mono', monospace" : 'inherit',
          fontSize: '13px', flexShrink: 0, boxSizing: 'border-box',
        }}>
          {col.value}
        </div>
      ))}
    </div>
  );
}

function PivotTable({ theme, data, cols, showSparkline, density, sortCol, setSortCol }) {
  const rowHeight = ROW_HEIGHTS[density] || 44;
  const [expanded, setExpanded] = React.useState({});
  const [selectedCell, setSelectedCell] = React.useState(null);
  const [hoveredRow, setHoveredRow] = React.useState(null);
  const r = theme.radius || '4px';

  const toggleExpand = (key) => setExpanded(prev => ({ ...prev, [key]: !prev[key] }));
  const handleSort = (colKey) => {
    setSortCol(prev => prev?.key === colKey
      ? { key: colKey, dir: prev.dir === 'asc' ? 'desc' : null }
      : { key: colKey, dir: 'asc' });
  };

  const hierWidth = 200;
  const colWidth = 150;

  return (
    <div style={{ flex: 1, overflow: 'auto', display: 'flex', flexDirection: 'column', position: 'relative', background: theme.pageBg || theme.background || '#fff' }}>
      {/* Sticky header */}
      <div style={{ position: 'sticky', top: 0, zIndex: 10, display: 'flex', background: theme.headerBg, boxShadow: theme.shadowInset || 'none' }}>
        <HeaderCell theme={theme} label={cols[0]?.label || 'Instrument'} width={hierWidth} sorted={sortCol?.key === 'hier' ? sortCol.dir : null} onSort={() => handleSort('hier')} isHierarchy />
        {cols.slice(1).map((col, i) => (
          <HeaderCell key={i} theme={theme} label={col.label} width={colWidth} sorted={sortCol?.key === col.key ? sortCol.dir : null} onSort={() => handleSort(col.key)} />
        ))}
        {showSparkline && <HeaderCell theme={theme} label="Price 20D" width={160} sorted={null} onSort={() => {}} />}
      </div>
      {/* Data rows */}
      {data.map((row, ri) => (
        <div key={ri}
          style={{ display: 'flex', background: hoveredRow === ri ? (theme.hover || '#F8FAFC') : 'transparent' }}
          onMouseEnter={() => setHoveredRow(ri)}
          onMouseLeave={() => setHoveredRow(null)}
        >
          <DataCell theme={theme} value={row.label} width={hierWidth} isHierarchy depth={row.depth || 0}
            expanded={expanded[row.key]} onExpand={row.hasChildren ? () => toggleExpand(row.key) : null}
            rowHeight={rowHeight} />
          {cols.slice(1).map((col, ci) => {
            const val = row[col.key];
            const isSelected = selectedCell === `${ri}-${ci}`;
            return (
              <DataCell key={ci} theme={theme} value={val}
                width={colWidth} isMono isNeg={typeof val === 'string' && val.startsWith('−')}
                isPos={typeof val === 'string' && val.startsWith('+')}
                isSelected={isSelected}
                rowHeight={rowHeight}
                onClick={() => setSelectedCell(isSelected ? null : `${ri}-${ci}`)}
              />
            );
          })}
          {showSparkline && <SparklineCell theme={theme} width={160} negative={ri % 3 === 2} rowHeight={rowHeight} />}
        </div>
      ))}
      {/* Totals */}
      <TotalRow theme={theme} height={rowHeight} cols={[
        { value: 'Total', width: hierWidth },
        ...cols.slice(1).map(c => ({ value: c.total || '—', width: colWidth })),
        ...(showSparkline ? [{ value: '', width: 160 }] : []),
      ]} />
      <TotalRow theme={theme} height={rowHeight} isGrand cols={[
        { value: 'Grand Total', width: hierWidth },
        ...cols.slice(1).map(c => ({ value: c.grandTotal || '—', width: colWidth })),
        ...(showSparkline ? [{ value: '', width: 160 }] : []),
      ]} />
    </div>
  );
}

Object.assign(window, { PivotTable, Sparkline });
