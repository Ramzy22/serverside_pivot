import React, { useEffect, useState, useLayoutEffect, useRef, useCallback } from 'react';

const OPEN_DELAY = 80;
const CLOSE_DELAY = 220;

// Renders the items for either the root menu or a sub-menu
const MenuItems = ({ actions, theme, onClose, itemRefs, onItemMouseEnter, onKeyDown }) => (
    <>
        {actions.map((action, i) => {
            if (action === 'separator') {
                return (
                    <div
                        key={i}
                        role="presentation"
                        style={{ height: '1px', background: theme?.border || '#e0e0e0', margin: '4px 0' }}
                    />
                );
            }
            const disabled = Boolean(action.disabled);
            const hasChildren = Array.isArray(action.children) && action.children.length > 0;
            return (
                <div
                    key={i}
                    ref={(el) => { if (itemRefs) itemRefs.current[i] = el; }}
                    role="menuitem"
                    aria-haspopup={hasChildren ? 'menu' : undefined}
                    aria-disabled={disabled || undefined}
                    tabIndex={disabled ? -1 : 0}
                    title={action.title || (typeof action.label === 'string' ? action.label : undefined)}
                    onMouseEnter={() => onItemMouseEnter && onItemMouseEnter(i, hasChildren, disabled)}
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
                        padding: '7px 12px 7px 14px',
                        cursor: disabled ? 'not-allowed' : 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px',
                        color: disabled ? (theme?.textSec || '#757575') : (theme?.text || '#111'),
                        opacity: disabled ? 0.48 : 1,
                        userSelect: 'none',
                    }}
                    onMouseEnterCapture={(e) => {
                        e.currentTarget.style.backgroundColor = disabled ? '' : (theme?.hover || '#f5f5f5');
                    }}
                    onMouseLeaveCapture={(e) => {
                        e.currentTarget.style.backgroundColor = '';
                    }}
                >
                    {action.icon && (
                        <span style={{ color: theme?.textSec || '#757575', display: 'flex', flexShrink: 0 }}>
                            {action.icon}
                        </span>
                    )}
                    <span style={{ flex: 1 }}>{action.label}</span>
                    {hasChildren && (
                        <span style={{ color: theme?.textSec || '#9ca3af', fontSize: '10px', marginLeft: '4px' }}>▶</span>
                    )}
                </div>
            );
        })}
    </>
);

const menuStyle = (theme) => ({
    background: theme?.surfaceBg || theme?.background || '#fff',
    border: `1px solid ${theme?.border || '#ccc'}`,
    boxShadow: theme?.shadowMd || '0 4px 20px rgba(0,0,0,0.15)',
    padding: '6px 0',
    borderRadius: theme?.radiusSm || '8px',
    fontSize: '13px',
    minWidth: '190px',
    maxWidth: '300px',
    color: theme?.text || '#111',
});

const ContextMenu = ({ x, y, onClose, actions, theme }) => {
    const [pos, setPos] = useState({ x, y });
    const [openIdx, setOpenIdx] = useState(null);
    const [anchorRect, setAnchorRect] = useState(null);
    const menuRef = useRef(null);
    const submenuRef = useRef(null);
    const itemRefs = useRef([]);
    const timerRef = useRef(null);

    // Focus helpers
    const focusAt = useCallback((start, dir = 1) => {
        const n = actions.length;
        for (let s = 0; s < n; s++) {
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
    const scheduleClose = useCallback(() => {
        clearTimeout(timerRef.current);
        timerRef.current = setTimeout(() => {
            setOpenIdx(null);
            setAnchorRect(null);
        }, CLOSE_DELAY);
    }, []);

    const cancelClose = useCallback(() => {
        clearTimeout(timerRef.current);
    }, []);

    const openSubmenu = useCallback((i) => {
        clearTimeout(timerRef.current);
        timerRef.current = setTimeout(() => {
            setOpenIdx(i);
            const el = itemRefs.current[i];
            if (el) setAnchorRect(el.getBoundingClientRect());
        }, openIdx !== null ? 0 : OPEN_DELAY);
    }, [openIdx]);

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

    const handleRootKeyDown = useCallback((e, currentIndex) => {
        if (e.key === 'Escape') { e.preventDefault(); onClose(); }
        else if (e.key === 'ArrowDown') { e.preventDefault(); focusAt(currentIndex + 1, 1); }
        else if (e.key === 'ArrowUp') { e.preventDefault(); focusAt(currentIndex - 1, -1); }
        else if (e.key === 'Home') { e.preventDefault(); focusAt(0, 1); }
        else if (e.key === 'End') { e.preventDefault(); focusAt(actions.length - 1, -1); }
        else if (e.key === 'ArrowRight' && openIdx === null) {
            // open sub-menu if hovered item has children
            if (Number.isFinite(currentIndex) && currentIndex >= 0) {
                const a = actions[currentIndex];
                if (a && a !== 'separator' && Array.isArray(a.children)) {
                    e.preventDefault();
                    openSubmenu(currentIndex);
                }
            }
        }
    }, [onClose, focusAt, actions, openIdx, openSubmenu]);

    // Clamp position to viewport
    useLayoutEffect(() => {
        const clamp = () => {
            const vw = window.innerWidth;
            const vh = window.innerHeight;
            const rect = menuRef.current?.getBoundingClientRect();
            const mw = rect?.width || 200;
            const mh = rect?.height || actions.length * 32 + 20;
            const pad = 8;
            let ax = Math.max(pad, Math.min(x, vw - mw - pad));
            let ay = Math.max(pad, Math.min(y, vh - mh - pad));
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
    const [submenuPos, setSubmenuPos] = useState({ left: 0, top: 0 });
    useLayoutEffect(() => {
        if (!anchorRect || openIdx === null) return;
        const el = submenuRef.current;
        if (!el) return;
        const vw = window.innerWidth;
        const vh = window.innerHeight;
        const mw = el.offsetWidth || 200;
        const mh = el.offsetHeight || 200;
        const pad = 6;
        let left = anchorRect.right + 2;
        if (left + mw > vw - pad) left = anchorRect.left - mw - 2;
        let top = anchorRect.top - 6;
        if (top + mh > vh - pad) top = vh - mh - pad;
        setSubmenuPos({ left: Math.max(pad, left), top: Math.max(pad, top) });
    }, [anchorRect, openIdx]);

    const openAction = openIdx !== null ? actions[openIdx] : null;
    const subActions = openAction && Array.isArray(openAction.children) ? openAction.children : null;

    // Sub-menu item focus
    const subItemRefs = useRef([]);
    const focusSubAt = useCallback((start, dir = 1) => {
        if (!subActions) return;
        const n = subActions.length;
        for (let s = 0; s < n; s++) {
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

    return (
        <>
            {/* Root menu */}
            <div
                ref={menuRef}
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
        </>
    );
};

export default ContextMenu;
