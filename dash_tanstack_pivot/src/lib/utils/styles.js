export const themes = {
    light: {
        name: 'light',
        primary: '#1976d2',
        border: '#e0e0e0',
        headerBg: '#f5f5f5',
        sortedHeaderBg: '#eaf3ff',
        sortedHeaderBorder: '#1976d2',
        sortedHeaderText: '#0d47a1',
        text: '#212121',
        textSec: '#757575',
        hover: '#eeeeee',
        select: '#e3f2fd',
        background: '#fff',
        sidebarBg: '#fafafa',
        pinnedBoundaryShadow: 'rgba(15,23,42,0.22)'
    },
    dark: {
        name: 'dark',
        isDark: true,
        primary: '#90caf9',
        border: '#424242',
        headerBg: '#333',
        sortedHeaderBg: '#1c2f44',
        sortedHeaderBorder: '#90caf9',
        sortedHeaderText: '#e3f2fd',
        text: '#fff',
        textSec: '#b0b0b0',
        hover: '#424242',
        select: '#1e3a5f',
        background: '#212121',
        sidebarBg: '#2c2c2c',
        pinnedBoundaryShadow: 'rgba(0,0,0,0.5)'
    },
    material: {
        name: 'material',
        primary: '#6200ee',
        border: '#e0e0e0',
        headerBg: '#fff',
        sortedHeaderBg: '#f3edff',
        sortedHeaderBorder: '#6200ee',
        sortedHeaderText: '#3b1f80',
        text: '#000',
        textSec: '#666',
        hover: '#f5f5f5',
        select: '#e8eaf6',
        background: '#fff',
        sidebarBg: '#fafafa',
        pinnedBoundaryShadow: 'rgba(17,24,39,0.22)'
    },
    balham: {
        name: 'balham',
        primary: '#0091ea',
        border: '#BDC3C7',
        headerBg: '#F5F7F7',
        sortedHeaderBg: '#e8f4fd',
        sortedHeaderBorder: '#0091ea',
        sortedHeaderText: '#0b4f75',
        text: '#2c3e50',
        textSec: '#7f8c8d',
        hover: '#ecf0f1',
        select: '#d6eaf8',
        background: '#fff',
        sidebarBg: '#fafafa',
        pinnedBoundaryShadow: 'rgba(15,23,42,0.22)'
    },
    flash: {
        name: 'flash',
        primary: '#18181b',
        border: '#e4e4e7',
        headerBg: '#fafafa',
        sortedHeaderBg: '#f4f4f5',
        sortedHeaderBorder: '#18181b',
        sortedHeaderText: '#09090b',
        text: '#09090b',
        textSec: '#71717a',
        hover: '#f4f4f5',
        select: '#e4e4e7',
        background: '#ffffff',
        sidebarBg: '#fafafa',
        pinnedBoundaryShadow: 'rgba(9,9,11,0.08)',
        radius: '6px',
    },
    strata: {
        name: 'strata',
        isDark: true,
        primary: '#00f2ff',
        border: 'rgba(255,255,255,0.08)',
        headerBg: 'rgba(12,12,18,0.98)',
        sortedHeaderBg: 'rgba(0,242,255,0.08)',
        sortedHeaderBorder: '#00f2ff',
        sortedHeaderText: '#00f2ff',
        text: '#e0e0e6',
        textSec: '#8a8a98',
        hover: 'rgba(255,255,255,0.04)',
        select: 'rgba(0,242,255,0.12)',
        background: '#0d0d12',
        sidebarBg: '#0a0a10',
        pinnedBoundaryShadow: 'rgba(0,0,0,0.7)',
        radius: '8px',
    },
    crystal: {
        name: 'crystal',
        isDark: true,
        primary: '#00f2ff',
        border: 'rgba(255,255,255,0.08)',
        headerBg: 'rgba(3,7,18,0.92)',
        sortedHeaderBg: 'rgba(0,242,255,0.08)',
        sortedHeaderBorder: '#00f2ff',
        sortedHeaderText: '#00f2ff',
        text: '#f9fafb',
        textSec: '#9ca3af',
        hover: 'rgba(255,255,255,0.03)',
        select: 'rgba(0,242,255,0.1)',
        background: '#030712',
        sidebarBg: '#050b1a',
        pinnedBoundaryShadow: 'rgba(0,0,0,0.8)',
        radius: '8px',
    },
    alabaster: {
        name: 'alabaster',
        primary: '#2563eb',
        border: 'rgba(0,0,0,0.06)',
        headerBg: '#fcfcfc',
        sortedHeaderBg: 'rgba(37,99,235,0.05)',
        sortedHeaderBorder: '#2563eb',
        sortedHeaderText: '#1e40af',
        text: '#0a0a0a',
        textSec: '#666666',
        hover: 'rgba(37,99,235,0.04)',
        select: 'rgba(37,99,235,0.08)',
        background: '#ffffff',
        sidebarBg: '#fcfcfc',
        pinnedBoundaryShadow: 'rgba(0,0,0,0.06)',
        radius: '8px',
    },
    satin: {
        name: 'satin',
        primary: '#0f172a',
        border: 'rgba(0,0,0,0.04)',
        headerBg: 'rgba(255,255,255,0.8)',
        sortedHeaderBg: '#f1f5f9',
        sortedHeaderBorder: '#0f172a',
        sortedHeaderText: '#0f172a',
        text: '#0f172a',
        textSec: '#64748b',
        hover: 'rgba(248,250,252,0.8)',
        select: '#f1f5f9',
        background: '#ffffff',
        sidebarBg: '#fdfdfd',
        pinnedBoundaryShadow: 'rgba(0,0,0,0.04)',
        radius: '12px',
    }
};

export const gridDimensionTokens = Object.freeze({
    density: Object.freeze({
        spacingLabels: Object.freeze(['Compact', 'Normal', 'Loose']),
        rowHeights: Object.freeze([32, 40, 56])
    }),
    columnWidths: Object.freeze({
        schemaFallback: 140,
        rowNumber: 50,
        hierarchy: 250,
        dimension: 150,
        measure: 130,
        subtotal: 130,
        collapsedPlaceholder: 60
    }),
    autoSize: Object.freeze({
        headerPadding: 40,
        cellPadding: 24,
        minWidth: 60,
        maxWidth: 600
    })
});

export const mergeStateStyles = (...layers) =>
    layers.reduce((merged, layer) => {
        if (!layer || typeof layer !== 'object') return merged;
        Object.entries(layer).forEach(([key, value]) => {
            if (value === undefined || value === null) return;
            if (key === 'boxShadow' && merged.boxShadow) {
                merged.boxShadow = `${merged.boxShadow}, ${value}`;
                return;
            }
            merged[key] = value;
        });
        return merged;
    }, {});

export const isDarkTheme = (theme) => theme && (theme.name === 'dark' || theme.isDark === true);

export const getStyles = (theme) => {
    const r = theme.radius || '4px';
    return ({
    root: {
        display: 'flex',
        flexDirection: 'column',
        fontFamily: 'Roboto, Helvetica, Arial, sans-serif',
        height: '100%',
        background: theme.background,
        border: `1px solid ${theme.border}`,
        borderRadius: r,
        overflow: 'hidden',
        fontSize: '13px',
        color: theme.text
    },
    appBar: {
        height: '48px',
        borderBottom: `1px solid ${theme.border}`,
        display: 'flex',
        alignItems: 'center',
        padding: '0 16px',
        justifyContent: 'space-between',
        background: theme.headerBg,
        color: theme.text
    },
    searchBox: {
        display: 'flex',
        alignItems: 'center',
        background: theme.isDark ? 'rgba(255,255,255,0.08)' : '#f5f5f5',
        borderRadius: r,
        padding: '4px 8px',
        width: '200px'
    },
    sidebar: {
        width: '320px',
        minWidth: '320px',
        borderRight: `1px solid ${theme.border}`,
        background: theme.sidebarBg,
        display: 'flex',
        flexDirection: 'column',
        padding: '16px',
        gap: '16px',
        overflowY: 'auto'
    },
    sectionTitle: {
        fontSize: '11px',
        fontWeight: 700,
        textTransform: 'uppercase',
        color: theme.textSec,
        marginBottom: '8px'
    },
    chip: {
        background: theme.isDark ? 'rgba(255,255,255,0.07)' : '#fff',
        border: `1px solid ${theme.border}`,
        borderRadius: r,
        padding: '6px 8px',
        marginBottom: '6px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        cursor: 'grab',
        boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
        position: 'relative',
        color: theme.text
    },
    dropZone: {
        minHeight: '40px',
        maxHeight: '140px',
        overflowY: 'auto',
        border: `1px dashed ${theme.border}`,
        borderRadius: r,
        padding: '8px',
        background: 'rgba(0,0,0,0.02)'
    },
    main: {
        flex: 1,
        overflow: 'hidden',
        position: 'relative',
        display: 'flex',
        flexDirection: 'column'
    },
    scrollContainer: {
        flex: 1,
        overflow: 'auto',
        position: 'relative'
    },
    headerSticky: {
        position: 'sticky',
        top: 0,
        zIndex: 100,
        background: theme.headerBg,
        width: 'fit-content',
        minWidth: '100%'
    },
    headerRow: {
        display: 'flex',
        width: '100%'
    },
    headerCell: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '0 8px',
        borderRight: `1px solid ${theme.border}`,
        borderBottom: `1px solid ${theme.border}`,
        fontWeight: 600,
        color: theme.text,
        position: 'relative',
        boxSizing: 'border-box',
        flexShrink: 0,
        minWidth: 0,
        overflow: 'hidden',
        whiteSpace: 'nowrap',
        textOverflow: 'ellipsis'
    },
    pinned: {
        position: 'sticky',
        zIndex: 3,
        background: theme.background
    },
    pinnedLeft: {
        left: 0,
        borderRight: `1px solid ${theme.border}`
    },
    pinnedRight: {
        right: 0,
        borderLeft: `1px solid ${theme.border}`
    },
    row: {
        display: 'flex',
        position: 'absolute',
        left: 0,
        width: '100%',
        boxSizing: 'border-box'
    },
    cell: {
        display: 'flex',
        alignItems: 'center',
        padding: '0 8px',
        borderRight: `1px solid ${theme.border}`,
        borderBottom: `1px solid ${theme.border}`,
        background: theme.background,
        color: theme.text,
        overflow: 'hidden',
        whiteSpace: 'nowrap',
        boxSizing: 'border-box',
        flexShrink: 0
    },
    cellSelected: {
        background: `${theme.select} !important`,
        boxShadow: `inset 0 0 0 2px ${theme.primary}`,
        zIndex: 2,
        position: 'relative'
    },
    btn: {
        padding: '4px 10px',
        borderRadius: '6px',
        border: `1px solid transparent`,
        background: 'transparent',
        cursor: 'pointer',
        fontSize: '12px',
        display: 'inline-flex',
        alignItems: 'center',
        gap: '5px',
        fontWeight: 500,
        color: theme.text,
        whiteSpace: 'nowrap',
        lineHeight: 1.5,
        transition: 'background 120ms ease, border-color 120ms ease, box-shadow 120ms ease',
        userSelect: 'none',
    },
    dropLine: {
        position: 'absolute',
        height: '2px',
        background: theme.primary,
        left: 0, right: 0, zIndex: 10, pointerEvents: 'none'
    },
    expandedSeparator: {
        borderBottom: `2px solid ${theme.primary}`
    },
    toolPanelSection: {
        display: 'flex',
        flexDirection: 'column',
        borderBottom: `1px solid ${theme.border}44`,
        marginBottom: '4px',
        transition: 'all 0.3s ease-in-out'
    },
    toolPanelSectionHeader: {
        display: 'flex',
        alignItems: 'center',
        padding: '8px 12px',
        background: theme.headerBg,
        cursor: 'pointer',
        fontSize: '11px',
        fontWeight: 600,
        textTransform: 'uppercase',
        color: theme.textSec,
        userSelect: 'none',
        transition: 'background 0.2s'
    },
    toolPanelList: {
        display: 'flex',
        flexDirection: 'column',
        padding: '4px 0',
        transition: 'height 0.3s ease'
    },
    columnItem: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        padding: '6px 12px',
        fontSize: '12px',
        cursor: 'default',
        transition: 'background 0.2s, transform 0.2s',
        position: 'relative',
        userSelect: 'none'
    }
}); };
