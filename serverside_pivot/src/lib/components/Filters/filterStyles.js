/** Shared, theme-token-only styles for the column filter editors. */

/** @param {any} theme @returns {Record<string, any>} */
export const filterInputStyle = (theme) => ({
    border: `1px solid ${theme.border}`,
    borderRadius: theme.radiusSm || '8px',
    padding: '6px 8px',
    fontSize: '12px',
    background: theme.background || theme.surfaceBg,
    color: theme.text,
    minWidth: 0,
    boxSizing: 'border-box',
    outline: 'none',
});

/**
 * @param {any} theme
 * @param {'primary' | 'subtle' | 'ghost' | 'danger' | 'toggle'} [variant]
 * @param {boolean} [active] for 'toggle'
 * @returns {Record<string, any>}
 */
export const filterButtonStyle = (theme, variant = 'subtle', active = false) => {
    const base = {
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '6px',
        padding: '6px 12px',
        fontSize: '12px',
        fontWeight: 600,
        lineHeight: 1.2,
        borderRadius: theme.radiusSm || '8px',
        cursor: 'pointer',
        whiteSpace: 'nowrap',
    };
    if (variant === 'primary') {
        return { ...base, border: `1px solid ${theme.primary}`, background: theme.primary, color: '#fff' };
    }
    if (variant === 'ghost') {
        return { ...base, border: '1px solid transparent', background: 'transparent', color: theme.text };
    }
    if (variant === 'danger') {
        return { ...base, border: '1px solid transparent', background: 'transparent', color: theme.danger || '#dc2626' };
    }
    if (variant === 'toggle') {
        return {
            ...base,
            padding: '4px 10px',
            fontSize: '11px',
            border: `1px solid ${active ? theme.primary : theme.border}`,
            background: active ? (theme.select || theme.hover) : (theme.background || theme.surfaceBg),
            color: active ? theme.primary : theme.text,
        };
    }
    return { ...base, border: `1px solid ${theme.border}`, background: theme.background || theme.surfaceBg, color: theme.text };
};

/** A bordered group (one condition, the option list). @param {any} theme @returns {Record<string, any>} */
export const filterCardStyle = (theme) => ({
    border: `1px solid ${theme.border}`,
    borderRadius: theme.radiusSm || '8px',
    background: theme.background || theme.surfaceBg,
});

/** @param {any} theme @returns {Record<string, any>} */
export const filterHintStyle = (theme) => ({
    fontSize: '11px',
    color: theme.textSec,
});
