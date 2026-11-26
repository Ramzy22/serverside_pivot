import React, { useEffect, useState, useLayoutEffect, useRef, useCallback } from 'react';
import Icons from '../../utils/Icons';
import { useDismiss, OverlayPortal } from '../../hooks/useDismiss';

const OPEN_DELAY = 80;
const CLOSE_DELAY = 220;

const ICON_GUTTER_PX = 16;

// Renders the items for either the root menu or a sub-menu. When any item in
// the menu carries an icon, every item reserves the same icon gutter so all
// labels start on one vertical line.
/** @param {any} p0 @returns {any} */
const MenuItems = ({ actions, theme, onClose, itemRefs, onItemMouseEnter, onKeyDown }) => {
    const reserveIconGutter = actions.some(/** @param {any} [action] @returns {boolean} */ (action) => Boolean(action && action !== 'separator' && action.icon));
    return (
    <>
        {actions.map(/** @param {any} [action] @param {any} [i] @returns {any} */ (action, i) => {
            if (action === 'separator') {
                return (
                    <div
                        key={i}
                        role="presentation"
                        style={{ height: '1px', background: theme?.border || 'var(--sp-border, #e2e8f0)', margin: '4px 0' }}
                    />
                );
            }
            /** @type {any} */
            const disabled = Boolean(action.disabled);
            /** @type {any} */
            const hasChildren = Array.isArray(action.children) && action.children.length > 0;
            return (
                <div
                    key={i}
                    ref={(el) => { if (itemRefs) itemRefs.current[i] = el; }}
                    className="sp-menu-item"
                    role="menuitem"
                    aria-haspopup={hasChildren ? 'menu' : undefined}
                    aria-disabled={disabled || undefined}
                    tabIndex={disabled ? -1 : 0}
                    title={action.title || (typeof action.label === 'string' ? action.label : undefined)}
                    onMouseEnter={() => {
                        if (onItemMouseEnter) onItemMouseEnter(i, hasChildren, disabled);
                    }}
                    onClick={(e) => {
                        if (disabled || hasChildren) { e.stopPropagation(); return; }
                        if (typeof action.onClick === 'function') action.onClick();
                        onClose();
                    }}
                    onKeyDown={(e) => {
                        if (disabled) return;
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            e.stopPropagation();
                            if (!hasChildren && typeof action.onClick === 'function') action.onClick();
                            if (!hasChildren) onClose();
                            else onItemMouseEnter && onItemMouseEnter(i, true, false);
                        } else {
                            onKeyDown && onKeyDown(e, i);
                        }
                    }}
                    style={{
                        padding: '7px 10px',
                        cursor: disabled ? 'not-allowed' : 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px',
                        color: disabled ? (theme?.textSec || 'var(--sp-text-sec, #64748b)') : (theme?.text || 'var(--sp-text, #0f172a)'),
                        opacity: disabled ? 0.48 : 1,
                        userSelect: 'none',
                    }}
                >
                    {(action.icon || reserveIconGutter) && (
                        <span aria-hidden="true" style={{ color: theme?.textSec || 'var(--sp-text-sec, #64748b)', display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: ICON_GUTTER_PX, flexShrink: 0 }}>
                            {action.icon || null}
                        </span>
                    )}
                    <span style={{ flex: 1, minWidth: 0, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{action.label}</span>
                    {hasChildren && (
                        <span aria-hidden="true" style={{ color: theme?.textSec || 'var(--sp-text-sec, #64748b)', display: 'inline-flex', alignItems: 'center', marginLeft: '4px' }}>
                            {Icons && Icons.ChevronRight ? <Icons.ChevronRight /> : '>'}
                        </span>
                    )}
                </div>
            );
        })}
    </>
    );
};

/** @param {any} [theme] @returns {any} */
const menuStyle = (theme) => ({
    background: `var(--sp-glass-bg, ${theme?.surfaceBg || theme?.background || '#fff'})`,
    backdropFilter: 'blur(18px) saturate(180%)',
    WebkitBackdropFilter: 'blur(18px) saturate(180%)',
    border: `1px solid ${theme?.border || 'var(--sp-border, #cbd5e1)'}`,
    boxShadow: theme?.shadow || theme?.shadowMd || '0 14px 36px rgba(0,0,0,0.18)',
    padding: '5px',
    borderRadius: 'var(--sp-radius-lg, 10px)',
    fontSize: 'var(--sp-fs-base, 13px)',
    minWidth: '200px',
    // Labels stay on one line; the menu widens to fit them, up to the viewport.
    maxWidth: 'min(380px, calc(100vw - 16px))',
    maxHeight: 'calc(100vh - 16px)',
    overflowY: 'auto',
    pointerEvents: 'auto',
    color: theme?.text || 'var(--sp-text, #0f172a)',
});

/** @param {any} p0 @returns {any} */
const ContextMenu = ({ x, y, onClose, actions, theme }) => {
    /** @type {[any, (...args: any[]) => any]} */
    const [pos, setPos] = useState({ x, y });
    /** @type {[any, (...args: any[]) => any]} */
    const [openIdx, setOpenIdx] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [anchorRect, setAnchorRect] = useState(null);
    /** @type {any} */
    const menuRef = useRef(null);
    /** @type {any} */
    const submenuRef = useRef(null);
    /** @type {any} */
    const itemRefs = useRef([]);
    /** @type {any} */
    const timerRef = useRef(null);

    // Focus helpers
    /** @type {(...args: any[]) => any} */
    const focusAt = useCallback((start, dir = 1) => {
        const n = actions.length;
        for (let s = 0; s < n; s++) {
            /** @type {any} */
            const i = (start + s * dir + n * 2) % n;
            const a = actions[i];
            if (a && a !== 'separator' && !a.disabled && itemRefs.current[i]) {
                itemRefs.current[i].focus();
                return;
            }
        }
    }, [actions]);

    useEffect(() => { focusAt(0, 1); }, [focusAt]);
    useEffect(() => () => clearTimeout(timerRef.current), []);

    // Shared timer helpers — used by both root menu and sub-menu mouse events
    /** @type {(...args: any[]) => any} */
    const scheduleClose = useCallback(() => {
        clearTimeout(timerRef.current);
        timerRef.current = setTimeout(() => {
            setOpenIdx(null);
            setAnchorRect(null);
        }, CLOSE_DELAY);
    }, []);

    /** @type {(...args: any[]) => any} */
    const cancelClose = useCallback(() => {
        clearTimeout(timerRef.current);
    }, []);

    /** @type {(...args: any[]) => any} */
    const openSubmenu = useCallback((i) => {
        clearTimeout(timerRef.current);
        timerRef.current = setTimeout(() => {
            setOpenIdx(i);
            const el = itemRefs.current[i];
            if (el) setAnchorRect(el.getBoundingClientRect());
        }, openIdx !== null ? 0 : OPEN_DELAY);
    }, [openIdx]);

    /** @type {(...args: any[]) => any} */
    const handleItemMouseEnter = useCallback((i, hasChildren, disabled) => {
        if (disabled) return;
        if (hasChildren) {
            openSubmenu(i);
        } else {
            cancelClose();
            setOpenIdx(null);
            setAnchorRect(null);
        }
    }, [openSubmenu, cancelClose]);

    /** @type {(...args: any[]) => any} */
    const handleRootKeyDown = useCallback((e, currentIndex) => {
        if (e.key === 'Escape') { e.preventDefault(); onClose(); }
        else if (e.key === 'ArrowDown') { e.preventDefault(); focusAt(currentIndex + 1, 1); }
        else if (e.key === 'ArrowUp') { e.preventDefault(); focusAt(currentIndex - 1, -1); }
        else if (e.key === 'Home') { e.preventDefault(); focusAt(0, 1); }
        else if (e.key === 'End') { e.preventDefault(); focusAt(actions.length - 1, -1); }
        else if (e.key === 'ArrowRight' && openIdx === null) {
            if (Number.isFinite(currentIndex) && currentIndex >= 0) {
                const a = actions[currentIndex];
                if (a && a !== 'separator' && Array.isArray(a.children)) {
                    e.preventDefault();
                    openSubmenu(currentIndex);
                }
            }
        }
    }, [onClose, focusAt, actions, openIdx, openSubmenu]);

    // Clamp position strictly to the visible viewport
    useLayoutEffect(() => {
        /** @returns {any} */
        const clamp = () => {
            // clientWidth/Height exclude the page scrollbars, so the menu is
            // clamped to the area it can actually be seen in.
            const vw = document.documentElement.clientWidth || window.innerWidth;
            const vh = document.documentElement.clientHeight || window.innerHeight;
            /** @type {any} */
            const menuEl = menuRef.current;
            // offsetWidth/Height are layout sizes, unaffected by the enter
            // animation's scale transform (getBoundingClientRect is not).
            /** @type {any} */
            const mw = (menuEl && menuEl.offsetWidth) || 210;
            /** @type {any} */
            const mh = Math.min((menuEl && menuEl.offsetHeight) || actions.length * 32 + 20, vh - 16);
            const pad = 8;
            /** @type {any} */
            const ax = Math.max(pad, Math.min(x, vw - mw - pad));
            /** @type {any} */
            const ay = Math.max(pad, Math.min(y, vh - mh - pad));
            setPos({ x: ax, y: ay });
        };
        clamp();
        window.addEventListener('resize', clamp);
        window.addEventListener('scroll', clamp, true);
        return () => {
            window.removeEventListener('resize', clamp);
            window.removeEventListener('scroll', clamp, true);
        };
    }, [x, y, actions.length]);

    // Sub-menu position
    /** @type {[any, (...args: any[]) => any]} */
    const [submenuPos, setSubmenuPos] = useState({ left: 0, top: 0 });
    useLayoutEffect(() => {
        if (!anchorRect || openIdx === null) return;
        const el = submenuRef.current;
        if (!el) return;
        const vw = document.documentElement.clientWidth || window.innerWidth;
        const vh = document.documentElement.clientHeight || window.innerHeight;
        /** @type {any} */
        const mw = el.offsetWidth || 200;
        /** @type {any} */
        const mh = Math.min(el.offsetHeight || 200, vh - 16);
        const pad = 8;
        /** @type {any} */
        let left = anchorRect.right + 2;
        if (left + mw > vw - pad) left = anchorRect.left - mw - 2;
        /** @type {any} */
        let top = anchorRect.top - 6;
        if (top + mh > vh - pad) top = vh - mh - pad;
        setSubmenuPos({ left: Math.max(pad, left), top: Math.max(pad, top) });
    }, [anchorRect, openIdx]);

    /** @type {any} */
    const openAction = openIdx !== null ? actions[openIdx] : null;
    /** @type {any} */
    const subActions = openAction && Array.isArray(openAction.children) ? openAction.children : null;

    // Sub-menu item focus
    /** @type {any} */
    const subItemRefs = useRef([]);
    /** @type {(...args: any[]) => any} */
    const focusSubAt = useCallback((start, dir = 1) => {
        if (!subActions) return;
        const n = subActions.length;
        for (let s = 0; s < n; s++) {
            /** @type {any} */
            const i = (start + s * dir + n * 2) % n;
            const a = subActions[i];
            if (a && a !== 'separator' && !a.disabled && subItemRefs.current[i]) {
                subItemRefs.current[i].focus();
                return;
            }
        }
    }, [subActions]);

    useEffect(() => {
        if (subActions) subItemRefs.current = subItemRefs.current.slice(0, subActions.length);
    }, [subActions]);

    /** @type {(...args: any[]) => any} */
    const handleSubKeyDown = useCallback((e, currentIndex) => {
        if (e.key === 'Escape' || e.key === 'ArrowLeft') {
            e.preventDefault();
            e.stopPropagation();
            setOpenIdx(null);
            setAnchorRect(null);
            if (openIdx !== null && itemRefs.current[openIdx]) itemRefs.current[openIdx].focus();
        } else if (e.key === 'ArrowDown') { e.preventDefault(); focusSubAt(currentIndex + 1, 1); }
        else if (e.key === 'ArrowUp') { e.preventDefault(); focusSubAt(currentIndex - 1, -1); }
    }, [focusSubAt, openIdx]);

    useDismiss({
        open: true,
        onClose,
        refs: [menuRef, submenuRef],
        restoreFocus: true,
    });

    return (
        <OverlayPortal theme={theme} zIndex={10001}>
            {/* Root menu */}
            <div
                ref={menuRef}
                className="sp-menu-surface"
                role="menu"
                style={{ position: 'fixed', top: pos.y, left: pos.x, zIndex: 10001, ...menuStyle(theme) }}
                onMouseLeave={scheduleClose}
                onMouseEnter={cancelClose}
            >
                <MenuItems
                    actions={actions}
                    theme={theme}
                    onClose={onClose}
                    itemRefs={itemRefs}
                    onItemMouseEnter={handleItemMouseEnter}
                    onKeyDown={handleRootKeyDown}
                />
            </div>

            {/* Sub-menu flyout */}
            {subActions && (
                <div
                    ref={submenuRef}
                    className="sp-menu-surface"
                    role="menu"
                    style={{ position: 'fixed', top: submenuPos.top, left: submenuPos.left, zIndex: 10002, ...menuStyle(theme) }}
                    onMouseEnter={cancelClose}
                    onMouseLeave={scheduleClose}
                >
                    <MenuItems
                        actions={subActions}
                        theme={theme}
                        onClose={onClose}
                        itemRefs={subItemRefs}
                        onKeyDown={handleSubKeyDown}
                    />
                </div>
            )}
        </OverlayPortal>
    );
};

export default ContextMenu;
