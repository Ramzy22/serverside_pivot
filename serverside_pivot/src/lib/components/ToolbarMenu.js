import React, { useRef, useState } from 'react';
import Icons from '../utils/Icons';
import { useDismiss } from '../hooks/useDismiss';

/**
 * A toolbar button that opens a small menu (the same <details> pattern and
 * menu surface as the Export menu). `children` is a function receiving
 * `close`, so items can close the menu after acting.
 *
 * @param {{
 *   label: React.ReactNode,
 *   ariaLabel: string,
 *   title?: string,
 *   theme: any,
 *   buttonStyle?: Record<string, any>,
 *   active?: boolean,
 *   align?: 'left' | 'right',
 *   minWidth?: number,
 *   chevron?: boolean,
 *   placement?: 'above' | 'below',
 *   summaryClassName?: string,
 *   testId?: string,
 *   children: (close: () => void) => React.ReactNode,
 * }} props
 * @returns {any}
 */
export function ToolbarMenu({
    label,
    ariaLabel,
    title,
    theme,
    buttonStyle = {},
    active = false,
    align = 'left',
    minWidth = 180,
    chevron = true,
    placement = 'below',
    summaryClassName,
    testId,
    children,
}) {
    /** @type {any} */
    const menuRef = useRef(null);
    const [open, setOpen] = useState(false);
    const close = () => {
        setOpen(false);
        if (menuRef.current) menuRef.current.removeAttribute('open');
    };
    useDismiss({ open, onClose: close, refs: [menuRef], restoreFocus: true });

    return (
        <details
            ref={menuRef}
            open={open}
            onToggle={(event) => setOpen(Boolean(event.currentTarget && event.currentTarget.open))}
            style={{ position: 'relative', display: 'inline-flex' }}
        >
            <summary
                className={summaryClassName || `sp-btn ${active ? 'sp-btn-active' : 'sp-btn-subtle'}`}
                aria-label={ariaLabel}
                aria-haspopup="menu"
                aria-expanded={open}
                title={title}
                data-testid={testId}
                style={{
                    ...buttonStyle,
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '6px',
                    listStyle: 'none',
                    cursor: 'pointer',
                    boxSizing: 'border-box',
                }}
            >
                {label}
                {chevron ? <span aria-hidden="true" style={{ display: 'inline-flex', alignItems: 'center' }}><Icons.ChevronDown /></span> : null}
            </summary>
            {open ? (
                <div
                    role="menu"
                    aria-label={ariaLabel}
                    className="sp-menu-surface"
                    style={{
                        position: 'absolute',
                        [align === 'right' ? 'right' : 'left']: 0,
                        [placement === 'above' ? 'bottom' : 'top']: 'calc(100% + 6px)',
                        zIndex: 60,
                        minWidth: `${minWidth}px`,
                        padding: '5px',
                        border: `1px solid ${theme.border}`,
                        borderRadius: '10px',
                        background: `var(--sp-glass-bg, ${theme.surfaceBg || theme.headerBg})`,
                        backdropFilter: 'blur(18px) saturate(180%)',
                        WebkitBackdropFilter: 'blur(18px) saturate(180%)',
                        boxShadow: theme.shadow || '0 14px 36px rgba(15, 23, 42, 0.18)',
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '1px',
                    }}
                >
                    {children(close)}
                </div>
            ) : null}
        </details>
    );
}

/**
 * @param {{ theme: any, onSelect: () => void, checked?: boolean, radio?: boolean, disabled?: boolean, title?: string, children: React.ReactNode, [key: string]: any }} props
 * @returns {any}
 */
export function ToolbarMenuItem({ theme, onSelect, checked, radio = false, disabled = false, title, children, ...rest }) {
    const role = checked === undefined ? 'menuitem' : (radio ? 'menuitemradio' : 'menuitemcheckbox');
    return (
        <button
            type="button"
            role={role}
            aria-checked={checked === undefined ? undefined : Boolean(checked)}
            className="sp-menu-item"
            disabled={disabled}
            title={title}
            onClick={onSelect}
            {...rest}
            style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                width: '100%',
                border: 'none',
                background: checked ? (theme.select || theme.hover) : 'transparent',
                color: checked ? theme.primary : theme.text,
                cursor: disabled ? 'not-allowed' : 'pointer',
                opacity: disabled ? 0.5 : 1,
                padding: '7px 10px',
                borderRadius: '6px',
                textAlign: 'left',
                fontSize: '12px',
                fontWeight: checked ? 700 : 500,
                whiteSpace: 'nowrap',
            }}
        >
            <span aria-hidden="true" style={{ width: '12px', display: 'inline-flex', justifyContent: 'center' }}>{checked ? '✓' : ''}</span>
            {children}
        </button>
    );
}

/** @param {{ theme: any, children: React.ReactNode }} props @returns {any} */
export function ToolbarMenuSection({ theme, children }) {
    return (
        <div
            role="presentation"
            style={{
                padding: '8px 10px 4px',
                fontSize: '10px',
                fontWeight: 800,
                letterSpacing: '0.08em',
                textTransform: 'uppercase',
                color: theme.textSec,
            }}
        >
            {children}
        </div>
    );
}
